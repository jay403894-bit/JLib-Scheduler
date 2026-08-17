; SPDX-License-Identifier: BSD-3-Clause
; Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
;
; ContextSwitch / FiberTrampoline -- Windows on ARM64 (Snapdragon X and friends).
; Third sibling of src/win32/ContextSwitch.asm (x64 MASM) and src/posix/aarch64/ContextSwitch.S
; (AAPCS64, GAS syntax). Same contract as every other port.
;
; CONTRACT (identical everywhere -- Fiber::Init depends on it):
;     void ContextSwitch(void** fromSp, void** toSp);
;     x0 = &from->sp, x1 = &to->sp
;
; WHY THIS FILE EXISTS AT ALL, i.e. why the Linux AArch64 switch could not just be reused.
; The scheduler's design claims the OS axis and the arch axis are orthogonal, and for every syscall
; in platform.h that is true. The context switch is the one exception, because a calling convention
; belongs to the (OS, ARCH) PAIR, not to the arch -- which is why the tree grew a src/win32/aarch64/
; beside src/posix/aarch64/ rather than an #ifdef. That is a deliberate, documented caveat.
;
; What actually differs from src/posix/aarch64/ContextSwitch.S is NARROWER than it looks:
;   - ASSEMBLER SYNTAX. MSVC's armasm64 (AREA/PROC/ENDP/EXPORT/END) is unrelated to GAS's
;     .globl/.type/.cfi_*. This is the whole of the real difference.
;   - UNWIND DATA. Windows uses .pdata/.xdata rather than DWARF CFI, so the .cfi_undefined x30 the
;     GAS port uses to stop the unwinder has no direct spelling here. See the note below.
; What does NOT differ: the register set, the frame layout, and Fiber::Init. Windows ARM64 and
; AAPCS64 agree on the callee-saved set (x19-x28, x29, x30, and the LOW 64 bits of v8-v15), so
; src/posix/aarch64/FiberInit.cpp is reused verbatim by this build. Keep the frame layout below in
; step with that file -- it seeds the entry point in the x19 slot and FiberTrampoline in the x30
; slot, and those two offsets are the entire interface.
;
; x18 IS NEVER TOUCHED, AND THAT IS LOAD-BEARING HERE.
; On Windows ARM64 x18 is strictly reserved as the TEB pointer; on Linux it is merely
; platform-reserved. Either way it is absent from the save set below -- but on Windows the reason is
; stronger than convention, and it is specifically about THIS scheduler: fibers MIGRATE BETWEEN
; WORKER THREADS. x18 describes the THREAD, not the fiber. Saving it into a fiber's frame and
; restoring it after that fiber resumes on a different worker would install a stale TEB pointer on a
; thread that did nothing wrong, corrupting thread-local state and surfacing later as
; nondeterministic damage in unrelated code. So x18 must be left exactly as the running thread left
; it: not saved, not restored, not read. Do not "complete" the register set by adding it.
;
; NO TEB StackBase/StackLimit FIXUP, deliberately, matching the shipped x64 port.
; An earlier CMake comment asserted the x64 MASM performs this fixup. It does not -- there is no TEB
; access anywhere in src/win32/. This port matches that behaviour rather than inventing a third one.
; The consequence is the same on both: because the TEB still describes the worker's ORIGINAL stack,
; an overflow off a fiber stack arrives as an access violation on the arena's guard page rather than
; as a proper stack-overflow exception, and SEH across a fiber boundary is correspondingly limited.
; That is the normal trade for lightweight fibers (boost.context makes it too) and it has shipped on
; x64 for the life of this library. If it is ever fixed, fix BOTH ports together.
;
; ALIGNMENT: AArch64 requires SP 16-byte aligned at ALL times, not just at call boundaries -- a
; misaligned SP faults on any stack access. Every stp/str below moves SP by a multiple of 16.
;
; FRAME LAYOUT at the saved SP (low to high) -- 176 bytes (64 FP + 16 FPCR+pad + 96 GPR):
;     +0    d8,  d9        +80   x19, x20
;     +16   d10, d11       +96   x21, x22
;     +32   d12, d13       +112  x23, x24
;     +48   d14, d15       +128  x25, x26
;     +64   FPCR (low 32)  +144  x27, x28
;     +72   pad            +160  x29 (FP), x30 (LR)

    AREA    |.text|, CODE, READONLY

    EXPORT  ContextSwitch
    EXPORT  FiberTrampoline

; Plain PROC/ENDP, no PROLOG_SAVE_REG_PAIR macros, matching src/win32/ContextSwitch.asm's plain
; MASM PROC. The macros exist to describe a frame to the unwinder, and this function deliberately
; does not HAVE a describable frame: it returns onto a different stack than it was called on, so any
; unwind description would be a lie the moment the SP swap lands. The GAS port says the same thing
; with .cfi_undefined x30 -- stop, do not attempt to walk out of here.
ContextSwitch PROC

    ; 1. Callee-saved GPRs. Descending pre-index, so they land in ascending address order.
    stp     x29, x30, [sp, #-16]!
    stp     x27, x28, [sp, #-16]!
    stp     x25, x26, [sp, #-16]!
    stp     x23, x24, [sp, #-16]!
    stp     x21, x22, [sp, #-16]!
    stp     x19, x20, [sp, #-16]!

    ; 2. FP CONTROL register. FPCR is per-thread state shared by every fiber on a worker, exactly
    ; like MXCSR on x86 -- a fiber that changes rounding mode or flush-to-zero and then yields would
    ; leak it into whoever resumes next. FPSR is status/sticky and caller-saved, so it is not saved.
    ; 16 bytes to keep SP aligned; only the low 4 are meaningful.
    mrs     x9, fpcr
    str     x9, [sp, #-16]!

    ; 3. Callee-saved FP: the LOW 64 bits of v8-v15 only, 64 bytes total.
    stp     d14, d15, [sp, #-16]!
    stp     d12, d13, [sp, #-16]!
    stp     d10, d11, [sp, #-16]!
    stp     d8,  d9,  [sp, #-16]!

    ; 4. Swap stack pointers. sp is not a general operand for ldr/str, hence the scratch.
    ; x9 is caller-saved (IP0/IP1 territory is x16/x17; x9-x15 are ordinary temporaries), so
    ; clobbering it here is free.
    mov     x9, sp
    str     x9, [x0]            ; save outgoing SP (16-aligned)
    ldr     x9, [x1]            ; load incoming SP (16-aligned)
    mov     sp, x9

    ; 5. Restore, exact reverse.
    ldp     d8,  d9,  [sp], #16
    ldp     d10, d11, [sp], #16
    ldp     d12, d13, [sp], #16
    ldp     d14, d15, [sp], #16

    ldr     x9, [sp], #16
    msr     fpcr, x9
    ; Writing FPCR needs a context-synchronisation event before dependent FP instructions are
    ; guaranteed to see it. The 'ret' below provides one, so no explicit isb is needed HERE -- but
    ; one would be required if any FP work were ever added between this point and the return.

    ldp     x19, x20, [sp], #16
    ldp     x21, x22, [sp], #16
    ldp     x23, x24, [sp], #16
    ldp     x25, x26, [sp], #16
    ldp     x27, x28, [sp], #16
    ldp     x29, x30, [sp], #16

    ret                         ; branches to x30, restored above

    ENDP

; Entry trampoline for freshly-initialized fibers.
;
; Reached because Fiber::Init seeded the x30 slot with this address, so the 'ret' above branches here
; instead of returning to a real caller. SP is 16-aligned on arrival -- note this differs from the
; x86 ports, where 'call' pushes an 8-byte return address and the callee starts at 8 mod 16.
; AArch64 has no such push (the return address is in x30), so there is nothing to compensate for.
;
; The C++ entry point was seeded in the x19 slot. It never returns -- it switches away -- and brk
; traps loudly if it ever does.
FiberTrampoline PROC

    blr     x19                 ; call the seeded entry point
    brk     #0                  ; unreachable: the entry must never return

    ENDP

    END
