// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Fiber::Init -- System V AMD64 (Linux) frame layout.
//
// Beside ContextSwitch.s for the same reason the Windows pair sit together: this function writes
// the frame that assembly reads back, slot for slot. Verified against it by a standalone harness
// before either was wired into the scheduler -- round trips, callee-saved GPR preservation and
// MXCSR isolation all checked.
//
// The frame is 64 bytes versus Windows' 272, and every difference is an ABI difference:
//   - no 32-byte shadow space (a Win64 requirement with no SysV equivalent)
//   - six callee-saved GPRs, not eight: RDI and RSI are ARGUMENT registers here
//   - no XMM block at all: every XMM register is caller-saved under SysV
//
// Layout at ctx.rsp, low to high -- must match ContextSwitch.s exactly:
//   +0  MXCSR (4) + x87 CW (2) + 2 pad     +8  r15   +16 r14   +24 r13
//   +32 r12                                +40 rbx   +48 rbp   +56 return address
#include "../../../include/Fiber.h"
#include "../../../include/Thread.h"
#include "../../../include/TaskScheduler.h"

// See the matching guard in aarch64/FiberInit.cpp. A stale copy of THIS file, globbed into an
// AArch64 build, is what silently seeded a 64-byte SysV frame for a 176-byte AAPCS64 restore.
#if !defined(__x86_64__) && !defined(_M_X64)
#error "x86_64/FiberInit.cpp built for a non-x86-64 target: the build picked the wrong arch directory."
#endif

using namespace JLib;

// Defined in ContextSwitch.s. The restore lands on this at a 16-aligned RSP; it 'call's the entry
// point stashed in RBX, which re-establishes the ABI 8-mod-16 entry.
extern "C" void FiberTrampoline();

void Fiber::Init(void(*entryPoint)())
{
	// 16-byte-align the very top of this fiber's stack.
	uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
	uintptr_t* sp = (uintptr_t*)top;

	// Return address consumed by ContextSwitch's final 'ret': the trampoline. It runs at a
	// 16-aligned RSP and 'call's the real entry (in RBX) to land it at ABI 8-mod-16.
	*(--sp) = (uintptr_t)&FiberTrampoline;

	// Six callee-saved GPR slots, in the order ContextSwitch pops them (r15 first, rbp last).
	// rbp is therefore the highest slot and rbx the second highest -- rbx carries the entry
	// point for the trampoline's `call rbx`.
	*(--sp) = 0;                     // rbp
	*(--sp) = (uintptr_t)entryPoint; // rbx
	*(--sp) = 0;                     // r12
	*(--sp) = 0;                     // r13
	*(--sp) = 0;                     // r14
	*(--sp) = 0;                     // r15

	// FP control state, and the 8 bytes that bring the frame to 16-byte alignment: MXCSR in the
	// low 4, x87 FCW in the next 2. Seeded with the ABI defaults so the first switch-in's
	// ldmxcsr/fldcw load a sane masked state rather than garbage -- MXCSR 0x1F80 (all FP
	// exceptions masked, round-to-nearest), FCW 0x037F. Identical encoding to the Windows slot.
	*(--sp) = 0x0000037F00001F80ULL;

	// 8 slots * 8 bytes = 64, so a 16-aligned top leaves ctx.rsp 16-aligned too.
	ctx.rsp = (void*)sp;
}
