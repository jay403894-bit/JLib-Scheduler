// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Fiber::Init -- Windows x64 frame layout.
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

void Fiber::Init(void(*entryPoint)())
{
	// 16-byte-align the very top of this fiber's stack.
	uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
	uintptr_t* sp = (uintptr_t*)top;

	// Windows x64 ABI: a called function gets 32 bytes of shadow space ABOVE its return
	// address for its callees to spill register params. The trampoline 'call's the C++
	// entry, so reserve that shadow at the very top, inside this fiber's own stack --
	// otherwise the entry function writes past stackTop (next fiber's base => silent
	// corruption, or unmapped memory => write AV at the stack-region boundary).
	// SysV has no shadow space, which is why the POSIX version does not do this.
	sp -= 4;                                 // 32 bytes shadow space

	// Return address consumed by ContextSwitch's final 'ret': the trampoline. It runs at
	// a 16-aligned RSP and 'call's the real entry (in RBX) to land it at ABI 8-mod-16.
	*(--sp) = (uintptr_t)&FiberTrampoline;

	// 8 callee-saved GPR slots. ContextSwitch pops them r15..rbx, so rbx (popped last)
	// is the highest slot -- we seed it with the entry point for the trampoline's
	// `call rbx`. The rest are zero; a fresh fiber has no meaningful GPR state.
	// RDI and RSI are callee-saved on Windows and NOT on SysV -- that difference is why
	// the POSIX frame has six GPR slots here rather than eight.
	*(--sp) = (uintptr_t)entryPoint; // rbx
	*(--sp) = 0;                     // rbp
	*(--sp) = 0;                     // rdi
	*(--sp) = 0;                     // rsi
	*(--sp) = 0;                     // r12
	*(--sp) = 0;                     // r13
	*(--sp) = 0;                     // r14
	*(--sp) = 0;                     // r15

	// 8-byte slot that realigns the XMM block to 16 -- mirrors ContextSwitch's
	// `sub rsp, 168` (= 160 XMM + 8). Without it ctx.rsp would be 8 mod 16 and the
	// restore's movdqa would #GP. ContextSwitch also stashes the FP control state here:
	// MXCSR at [base+160] (low 4 bytes), x87 FCW at [base+164] (next 2). Seed the ABI
	// defaults so the first switch-in's ldmxcsr/fldcw load a sane masked state instead
	// of garbage: MXCSR 0x1F80 (all FP exceptions masked, round-to-nearest), FCW 0x037F.
	*(--sp) = 0x0000037F00001F80ULL;

	// 160 bytes for non-volatile XMM6-15 (10 * 16). Restored with movdqa, so this block
	// -- and ctx.rsp -- must be 16-aligned. Zero-initialized; no incoming XMM state.
	// Every XMM register is CALLER-saved under SysV, so this block does not exist there.
	for (int k = 0; k < 20; ++k) *(--sp) = 0; // 20 * 8 = 160 bytes

	ctx.rsp = (void*)sp; // 16-aligned base of the XMM block; ContextSwitch loads RSP here
}
