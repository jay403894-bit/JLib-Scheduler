// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

/*  ContextSwitch / FiberTrampoline -- System V AMD64 (Linux).
 *  Port of ContextSwitch.asm (Win64/MASM). Intel syntax to stay readable next to the original.
 *
 *  ucontext was rejected on purpose: swapcontext() saves and restores the SIGNAL MASK, which is a
 *  sigprocmask SYSCALL on every switch. For a scheduler whose headline figure is single-digit-
 *  microsecond task latency, a syscall per switch is disqualifying regardless of the POSIX
 *  deprecation. Boost.Context avoids it the same way this does: hand-written assembly.
 *
 *  WHAT DIFFERS FROM THE WIN64 VERSION, and why this file is SHORTER:
 *    - Arguments: RDI = 'from', RSI = 'to'  (Win64 used RCX/RDX).
 *    - Callee-saved set is smaller: rbx, rbp, r12-r15. RDI and RSI are ARGUMENT registers here,
 *      not callee-saved, so they are not preserved -- unlike Win64 where both are.
 *    - NO XMM SAVES AT ALL. Every XMM register is caller-saved in SysV, so the entire 160-byte
 *      xmm6-15 block from the Win64 version disappears. This is the bulk of the size difference.
 *    - No shadow space: the 32-byte spill area Win64 requires above the return address does not
 *      exist in this ABI, so Fiber::Init reserves nothing equivalent.
 *    - No TEB fixup: Windows fiber switches must swap the thread environment block's stack bounds
 *      so stack probes and SEH behave. Linux has no equivalent; moving RSP is the whole job.
 *
 *  WHAT DOES NOT CHANGE: MXCSR and the x87 control word are callee-saved in SysV too, and they are
 *  ONE PHYSICAL REGISTER PAIR SHARED BY EVERY FIBER ON A WORKER. Without saving them, a fiber that
 *  sets a rounding mode or flush-to-zero and then yields silently leaks that state into whatever
 *  fiber resumes next -- a bug that shows up as wrong arithmetic in unrelated code.
 *
 *  ALIGNMENT: 'call' pushes 8 onto a 16-aligned stack, so entry RSP is 8 mod 16. Six pushes (48
 *  bytes) leave it 8 mod 16. Reserving 8 more brings it to 0 mod 16, so the saved RSP is always
 *  16-aligned -- the same invariant the Win64 version maintains, reached with less padding.
 *
 *  FRAME LAYOUT at the saved RSP (low to high):
 *      +0   MXCSR (4) + x87 CW (2) + 2 pad
 *      +8   r15
 *      +16  r14
 *      +24  r13
 *      +32  r12
 *      +40  rbx        <-- Fiber::Init seeds the entry point here, for FiberTrampoline
 *      +48  rbp
 *      +56  return address
 */

    .intel_syntax noprefix
    .text

    .globl  ContextSwitch
    .type   ContextSwitch, @function
ContextSwitch:
    .cfi_startproc
    /* Without this, gdb tries to unwind past the switch using the CALLER's frame and produces
       garbage backtraces inside every fiber. Marking rip undefined stops the unwinder cleanly. */
    .cfi_undefined rip

    /* 1. Save callee-saved GPRs (SysV set: rbx, rbp, r12-r15). */
    push    rbp
    push    rbx
    push    r12
    push    r13
    push    r14
    push    r15

    /* 2. Save nonvolatile FP control state; the 8 bytes also realign RSP to 16. */
    sub     rsp, 8
    stmxcsr dword ptr [rsp]
    fnstcw  word  ptr [rsp + 4]

    /* 3. Swap stack pointers. */
    mov     [rdi], rsp          /* save old RSP (16-aligned) */
    mov     rsp, [rsi]          /* load new RSP (16-aligned) */

    /* 4. Restore FP control state. */
    ldmxcsr dword ptr [rsp]
    fldcw   word  ptr [rsp + 4]
    add     rsp, 8

    /* 5. Restore callee-saved GPRs, reverse order. */
    pop     r15
    pop     r14
    pop     r13
    pop     r12
    pop     rbx
    pop     rbp

    ret
    .cfi_endproc
    .size   ContextSwitch, .-ContextSwitch


/*  Entry trampoline for freshly-initialized fibers. The restore above lands here with RSP 16-aligned
 *  (0 mod 16); Fiber::Init seeded RBX with the C++ entry point. Entering via 'call' pushes an 8-byte
 *  return address, so the entry function starts at 8 mod 16 -- exactly the alignment the ABI and the
 *  compiler assume of a normally-called function. The entry never returns (it switches away); ud2
 *  traps loudly if it ever does.
 */
    .globl  FiberTrampoline
    .type   FiberTrampoline, @function
FiberTrampoline:
    .cfi_startproc
    .cfi_undefined rip
    call    rbx
    ud2
    .cfi_endproc
    .size   FiberTrampoline, .-FiberTrampoline

/* Non-executable stack, or the linker warns and marks the whole binary's stack executable. */
    .section .note.GNU-stack,"",@progbits
