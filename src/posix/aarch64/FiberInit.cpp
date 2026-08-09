// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Fiber::Init -- AAPCS64 (Linux/Android AArch64, and Apple arm64) frame layout.
//
// Beside ContextSwitch.s for the same reason the other two ports sit together: this function
// writes the frame that assembly reads back, slot for slot.
//
// The frame is 176 bytes, the largest of the three ports, and every difference from SysV's 64 is
// an ABI difference:
//   - twelve callee-saved GPRs (x19-x28, x29, x30), not six
//   - the low 64 bits of v8-v15 ARE callee-saved, so 64 bytes of FP state must be seeded; under
//     SysV every XMM register is caller-saved and the block does not exist
//   - the return address is NOT a stack slot. AArch64 keeps it in x30 and 'ret' branches to the
//     register, so the trampoline address goes in the x30 SLOT rather than above the GPRs.
//
// Layout at ctx.rsp, low to high -- must match ContextSwitch.s exactly:
//   +0  d8..d15 (64 bytes)   +64 FPCR   +72 pad
//   +80 x19  +88 x20  +96 x21  +104 x22  +112 x23  +120 x24
//   +128 x25  +136 x26  +144 x27  +152 x28  +160 x29 (FP)  +168 x30 (LR)
#include "../../../include/Fiber.h"
#include "../../../include/Thread.h"
#include "../../../include/TaskScheduler.h"

// Fail LOUDLY if the wrong arch's frame builder reaches the compiler. This is not hypothetical: a
// stale src/posix/FiberInit.cpp (the 64-byte SysV frame) left behind by an rsync without --delete
// was globbed into an AArch64 build, and BOTH definitions of Fiber::Init went into libScheduler.a.
// Static-archive linking does not diagnose that -- it takes the first member resolving the symbol
// and silently drops the other. The SysV version compiles fine on ARM (it is just eight pointer
// stores), so the only symptom was ContextSwitch restoring 176 bytes from a 64-byte frame and
// faulting one instruction past the stack top. An #error turns that entire failure mode into a
// build break.
#if !defined(__aarch64__) && !defined(_M_ARM64)
#error "aarch64/FiberInit.cpp built for a non-AArch64 target: the build picked the wrong arch directory."
#endif

using namespace JLib;

// Defined in ContextSwitch.s. The restore lands on this via 'ret' branching to the seeded x30; it
// 'blr's the entry point stashed in x19.
extern "C" void FiberTrampoline();

void Fiber::Init(void(*entryPoint)())
{
	// 16-byte-align the very top of this fiber's stack. On AArch64 this is not a convention that
	// only matters at call boundaries: a misaligned SP faults on any stack access.
	uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
	uintptr_t* sp = (uintptr_t*)top;

	// Written high address to low, i.e. the reverse of the order ContextSwitch pops them.

	// x30 (LR). ContextSwitch's final 'ret' branches HERE rather than popping a return address,
	// which is the one structural difference from the x86 ports. The trampoline arrives with SP
	// already 16-aligned -- there is no 'call' to push 8 and no compensation to undo.
	*(--sp) = (uintptr_t)&FiberTrampoline;
	*(--sp) = 0;                     // x29 (FP): zero terminates a backtrace cleanly at the fiber base

	*(--sp) = 0;                     // x28
	*(--sp) = 0;                     // x27
	*(--sp) = 0;                     // x26
	*(--sp) = 0;                     // x25
	*(--sp) = 0;                     // x24
	*(--sp) = 0;                     // x23
	*(--sp) = 0;                     // x22
	*(--sp) = 0;                     // x21
	*(--sp) = 0;                     // x20
	*(--sp) = (uintptr_t)entryPoint; // x19: the trampoline's 'blr x19' target

	// The upper half of FPCR's 16-byte push. ContextSwitch never reads it; zeroed so a stack dump
	// of a fresh fiber has no garbage in it.
	*(--sp) = 0;

	// FPCR. Zero IS the AAPCS64 default -- round-to-nearest (RMode=0), flush-to-zero off (FZ=0),
	// and every FP exception untrapped (the enable bits at 8-12 clear). Seeded for the same reason
	// the x86 ports seed MXCSR 0x1F80: the first switch-in's 'msr fpcr' must load a sane state, not
	// whatever happened to be on this stack. Deliberately a constant rather than a read of the
	// current FPCR, so a fiber's rounding mode never depends on which thread created it.
	*(--sp) = 0;

	// d8-d15, low 64 bits each. Callee-saved here, unlike on x86-64.
	for (int i = 0; i < 8; ++i)
		*(--sp) = 0;

	// 22 slots * 8 bytes = 176, so a 16-aligned top leaves ctx.rsp 16-aligned too.
	ctx.rsp = (void*)sp;
}
