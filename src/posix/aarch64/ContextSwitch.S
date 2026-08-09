// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

/*  ContextSwitch / FiberTrampoline -- AAPCS64 (Linux/Android AArch64, and Apple arm64).
 *  Sibling of src/posix/x86_64/ContextSwitch.s. Same contract, different ABI.
 *
 *  CONTRACT (identical to every other port -- Fiber::Init depends on it):
 *      void ContextSwitch(void** fromSp, void** toSp);
 *      x0 = &from->sp   (where to store the outgoing stack pointer)
 *      x1 = &to->sp     (where to read the incoming one)
 *
 *  WHAT DIFFERS FROM THE x86-64 VERSIONS:
 *    - Arguments are x0/x1 (SysV used rdi/rsi, Win64 rcx/rdx).
 *    - Callee-saved GPRs are x19-x28 plus x29 (FP) and x30 (LR): twelve registers, more than
 *      SysV's six, so the GPR block is larger.
 *    - FP saves are SMALL. Only the LOW 64 BITS of v8-v15 are callee-saved, so d8-d15 is 64 bytes
 *      -- against Win64's 160-byte xmm6-15 block, and SysV's none at all.
 *    - There is no return address ON the stack. AArch64 puts it in x30 (LR), so 'ret' branches to
 *      x30 rather than popping. That is why x30 is saved with the GPRs here and why the trampoline
 *      below sets it up differently from the x86 versions.
 *    - No shadow space, no TEB fixup -- as with SysV, moving SP is the whole job.
 *
 *  WHAT DOES NOT CHANGE: the FP control register is per-thread state shared by every fiber on a
 *  worker, exactly like MXCSR on x86. A fiber that changes rounding mode or flush-to-zero and then
 *  yields would silently leak it into whatever fiber resumes next. On AArch64 that register is
 *  FPCR (control); FPSR is status/sticky-flags and is caller-saved, so it is deliberately not saved.
 *
 *  ALIGNMENT: AAPCS64 requires SP 16-byte aligned at ALL times, not merely at call boundaries --
 *  a misaligned SP faults on any stack access, it does not merely violate convention. Every stp
 *  below moves SP by a multiple of 16, so the invariant holds throughout.
 *
 *  FRAME LAYOUT at the saved SP (low to high) -- 176 bytes total
 *  (64 FP + 16 FPCR-and-pad + 96 GPR):
 *      +0    d8,  d9
 *      +16   d10, d11
 *      +32   d12, d13
 *      +48   d14, d15
 *      +64   FPCR (only the low 32 bits are meaningful)
 *      +72   pad -- the other half of FPCR's 16-byte push, never read
 *      +80   x19, x20
 *      +96   x21, x22
 *      +112  x23, x24
 *      +128  x25, x26
 *      +144  x27, x28
 *      +160  x29 (FP), x30 (LR)
 *  Fiber::Init seeds the ENTRY POINT in the x19 slot (+80) and FiberTrampoline in the x30 slot
 *  (+168), mirroring how the SysV port seeds rbx and the return address. Those two offsets are
 *  the whole interface between this file and aarch64/FiberInit.cpp -- keep them in step.
 */

    .text

#if defined(__APPLE__)
    .globl  _ContextSwitch
    .p2align 2
_ContextSwitch:
#else
    .globl  ContextSwitch
    .type   ContextSwitch, @function
    .p2align 2
ContextSwitch:
#endif
    .cfi_startproc
    /* AArch64's return address lives in x30, so it is x30 -- not a stack slot -- that the unwinder
       follows. Without this, gdb walks past the switch using the CALLER's frame and prints garbage
       backtraces inside every fiber. Same defect the SysV port hit with .cfi_undefined rip. */
    .cfi_undefined x30

    /* 1. Callee-saved GPRs. Descending pre-index so they land in ascending address order. */
    stp     x29, x30, [sp, #-16]!
    stp     x27, x28, [sp, #-16]!
    stp     x25, x26, [sp, #-16]!
    stp     x23, x24, [sp, #-16]!
    stp     x21, x22, [sp, #-16]!
    stp     x19, x20, [sp, #-16]!

    /* 2. FP control register. 16 bytes keeps SP 16-aligned; only 4 are meaningful. */
    mrs     x9, fpcr
    str     x9, [sp, #-16]!

    /* 3. Callee-saved FP: the LOW 64 bits of v8-v15 only. */
    stp     d14, d15, [sp, #-16]!
    stp     d12, d13, [sp, #-16]!
    stp     d10, d11, [sp, #-16]!
    stp     d8,  d9,  [sp, #-16]!

    /* 4. Swap stack pointers. mov to/from sp needs a scratch: sp is not a general operand here. */
    mov     x9, sp
    str     x9, [x0]            /* save outgoing SP (16-aligned) */
    ldr     x9, [x1]            /* load incoming SP (16-aligned) */
    mov     sp, x9

    /* 5. Restore, exact reverse. */
    ldp     d8,  d9,  [sp], #16
    ldp     d10, d11, [sp], #16
    ldp     d12, d13, [sp], #16
    ldp     d14, d15, [sp], #16

    ldr     x9, [sp], #16
    msr     fpcr, x9
    /* Changing FPCR requires a context-synchronisation event before dependent FP instructions are
       guaranteed to observe it. Returning through 'ret' provides one, so no explicit isb is needed
       here -- but an isb would be required if FP work were done inline after this point. */

    ldp     x19, x20, [sp], #16
    ldp     x21, x22, [sp], #16
    ldp     x23, x24, [sp], #16
    ldp     x25, x26, [sp], #16
    ldp     x27, x28, [sp], #16
    ldp     x29, x30, [sp], #16

    ret                          /* branches to x30, restored above */
    .cfi_endproc
#if !defined(__APPLE__)
    .size   ContextSwitch, .-ContextSwitch
#endif


/*  Entry trampoline for freshly-initialized fibers.
 *
 *  Reached because Fiber::Init seeded the x30 slot with this address, so the 'ret' above branches
 *  here instead of returning to a real caller. SP is 16-aligned on arrival, which is what the ABI
 *  requires at a function boundary -- note this differs from x86, where 'call' pushes 8 and the
 *  callee starts at 8 mod 16. AArch64 has no such push, so no compensation is needed.
 *
 *  The C++ entry point was seeded in the x19 slot. It never returns (it switches away); brk traps
 *  loudly if it ever does.
 */
#if defined(__APPLE__)
    .globl  _FiberTrampoline
    .p2align 2
_FiberTrampoline:
#else
    .globl  FiberTrampoline
    .type   FiberTrampoline, @function
    .p2align 2
FiberTrampoline:
#endif
    .cfi_startproc
    .cfi_undefined x30
    blr     x19                  /* call the seeded entry point */
    brk     #0                   /* unreachable: the entry must never return */
    .cfi_endproc
#if !defined(__APPLE__)
    .size   FiberTrampoline, .-FiberTrampoline

/* Non-executable stack, or the linker marks the whole binary's stack executable.
   Apple's linker has no equivalent directive and defaults to non-executable. */
    .section .note.GNU-stack,"",@progbits
#endif
