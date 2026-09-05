// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// THE AVX GATE ContextSwitch.asm reads on every switch.
//
// This lives beside ContextSwitch.asm ON PURPOSE. The two are one contract: Init writes a frame
// that ContextSwitch's restore sequence reads back, register for register and slot for slot. If
// they ever disagree the failure is a wild jump, not a compile error, so keeping them in the same
// directory means you cannot edit one without seeing the other. The Linux pair is src/posix/.
#include "../../include/Fiber.h"
#include "../../include/Thread.h"
#include "../../include/TaskScheduler.h"
#include <intrin.h>   // __cpuid / _xgetbv, for the AVX gate below

using namespace JLib;

// Defined in ContextSwitch.asm. The restore lands on this at a 16-aligned RSP; it
// 'call's the entry point we stash in RBX, which re-establishes the ABI 8-mod-16 entry.
extern "C" void FiberTrampoline();

// ---- the AVX gate ----------------------------------------------------------------------------
// READ BY ASSEMBLY on every single context switch. ContextSwitch.asm declares this EXTERN and
// tests it, so the name, the type and the zero-means-no are a contract with that file rather than
// an implementation detail of this one. It lives here for the same reason Fiber::Init does.
//
// WHAT IT BUYS. ContextSwitch saves XMM6-15 with legacy-SSE `movdqa`. Executing legacy SSE while
// the upper halves of YMM are live costs an SSE/AVX transition, and a fiber parking straight out
// of an AVX kernel is in precisely that state -- so a vectorised workload pays it on every switch.
// Measured by bench/context_switch.cpp on a 13900K, all arms in one process with a same-vs-same
// control reading 1.000x:
//
//     movdqa, dirty upper state    85.8 ns/switch
//     vzeroupper + movdqa           9.2 ns/switch      <-- this
//     movdqa, clean upper state     9.2 ns/switch
//
// One vzeroupper at the top of the switch recovers all of it, and covers BOTH halves of the
// routine -- the saves for the outgoing fiber and the loads for the incoming one -- because
// nothing between them re-dirties the upper state.
//
// WHY IT IS GATED RATHER THAN UNCONDITIONAL. `vzeroupper` is itself an AVX instruction. Emitting
// it unconditionally would raise the entire library's floor from baseline x86-64 to AVX (2011+) to
// buy an optimisation that, on a CPU without AVX, has nothing to optimise: no AVX means no dirty
// upper state means no transition to avoid. The gate keeps the floor and skips the instruction in
// the one case where it would be both illegal and pointless.
//
// ZERO IS THE SAFE VALUE, and that is why this is constant-initialised to 0 and only ever raised.
// If the initialiser below has not run yet -- another translation unit's static initialiser
// switching a fiber before ours runs -- the switch behaves exactly as it did before this existed:
// correct, and slower. There is no ordering in which the gate is wrong in the dangerous direction.
extern "C" unsigned char JLibCtxHasAvx = 0;

namespace {

bool DetectAvx() {
	int r[4]{};
	__cpuid(r, 0);
	if (r[0] < 1) return false;

	__cpuid(r, 1);
	const bool osxsave = (r[2] & (1 << 27)) != 0;
	const bool avx     = (r[2] & (1 << 28)) != 0;
	if (!osxsave || !avx) return false;

	// CPUID.AVX ALONE IS NOT ENOUGH, and getting this wrong is a #UD rather than a slow path. The
	// CPU may implement AVX while the OS has not enabled the register state for it, in which case
	// the AVX registers are not preserved across an OS context switch and the instructions fault.
	// OSXSAVE is what makes XGETBV itself legal to execute, so it must be checked FIRST; bits 1
	// and 2 of XCR0 are the XMM and YMM state, and the OS must have enabled both.
	const unsigned long long xcr0 = _xgetbv(0);
	return (xcr0 & 0x6) == 0x6;
}

struct AvxGateInit { AvxGateInit() { JLibCtxHasAvx = DetectAvx() ? 1u : 0u; } };
const AvxGateInit g_avxGateInit;

} // namespace

// Fiber::Init MOVED TO include/Fiber.h (three #if-guarded arms: AAPCS64, Win64 x64, SysV x64).
// It left because keeping ONE definition of it reachable was costing three separate build
// safeguards -- a CMake REMOVE_ITEM, a cross-directory list(APPEND), and a configure-time
// FATAL_ERROR on a stale file -- all guarding the same hazard: two definitions both land in the
// archive and static linking picks whichever comes first, with no duplicate-symbol error.
//
// THE GATE COULD NOT FOLLOW IT. JLibCtxHasAvx is an `extern "C"` DEFINITION that ContextSwitch.asm
// resolves by name, and the detector below is a static initialiser that must run exactly once. A
// header would make both of those a question about inline variables and static-init order, on the
// path where a wrong answer is a wild jump on every context switch. One TU is the right answer.
