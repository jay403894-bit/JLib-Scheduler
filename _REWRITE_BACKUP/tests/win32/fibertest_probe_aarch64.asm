; SPDX-License-Identifier: BSD-3-Clause
; Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
;
; Assembly half of the Windows/ARM64 ABI harness. Companion to tests/fibertest_win_aarch64.cpp.
;
; WHY THIS FILE EXISTS AT ALL: MSVC SUPPORTS NO INLINE ASSEMBLY ON ARM64.
; Not "a different syntax" -- none. No __asm blocks, no GCC-style asm volatile, and no way to pin a
; C++ variable to a named register the way `register uint64_t v asm("x19")` does in the POSIX
; harness (tests/fibertest_aarch64.cpp). Since the entire point of an ABI harness is to put known
; values in SPECIFIC registers, drive a switch, and read them back, that work cannot live in the C++
; file on this toolchain. It lives here instead, and the C++ side is reduced to a driver.
;
; NOTHING HERE REFERENCES A GLOBAL SYMBOL, deliberately. Both routines take pointers as arguments
; and keep everything they need on the fiber's own stack or in caller-saved registers. That avoids
; armasm64's adrp/add relocation spelling entirely -- one less toolchain-specific thing to get
; subtly wrong in a file whose whole job is detecting subtle wrongness.
;
; The struct both routines share, matching FiberCtx in the .cpp (three 8-byte fields):
;     +0   fibSp    -- saved SP of the fiber
;     +8   mainSp   -- saved SP of the "thread" context
;     +16  hits     -- round-trip counter

    AREA    |.text|, CODE, READONLY

    EXPORT  FiberEntryAsm
    EXPORT  AbiProbe

    IMPORT  ContextSwitch

; -------------------------------------------------------------------------------------------
; void FiberEntryAsm(void)   -- entered via FiberTrampoline's 'blr x19'
;
; MakeFiber seeds x19 with this address and x20 with the FiberCtx pointer, so the context arrives
; in a register rather than through a global. x20 is itself in the set this routine deliberately
; destroys, so the pointer is parked on the fiber's own stack immediately and reloaded each pass --
; the fiber's SP is preserved across a switch, which is precisely what makes that safe.
;
; Each pass: bump hits, stamp 0xDEAD into every callee-saved GPR and FP register, set flush-to-zero
; in FPCR, and switch back. If ContextSwitch does not restore properly, the caller's sentinels come
; back holding 0xDEAD -- a recognisable value, far easier to diagnose than an arbitrary clobber.
FiberEntryAsm PROC

    str     x20, [sp, #-16]!        ; park the ctx pointer; 16 keeps SP 16-aligned

fe_loop
    ldr     x9,  [sp]               ; ctx
    ldr     x10, [x9, #16]          ; ctx->hits
    add     x10, x10, #1
    str     x10, [x9, #16]

    ; Trash every callee-saved GPR. x19-x28 is the whole set.
    mov     x19, #0xDEAD
    mov     x20, #0xDEAD
    mov     x21, #0xDEAD
    mov     x22, #0xDEAD
    mov     x23, #0xDEAD
    mov     x24, #0xDEAD
    mov     x25, #0xDEAD
    mov     x26, #0xDEAD
    mov     x27, #0xDEAD
    mov     x28, #0xDEAD

    ; Trash the callee-saved FP set: the LOW 64 bits of v8-v15.
    mov     x11, #0xDEAD
    fmov    d8,  x11
    fmov    d9,  x11
    fmov    d10, x11
    fmov    d11, x11
    fmov    d12, x11
    fmov    d13, x11
    fmov    d14, x11
    fmov    d15, x11

    ; Set flush-to-zero (FPCR bit 24). A mode the caller must NOT inherit -- this is what proves
    ; the switch saves and restores FPCR rather than letting it leak between contexts.
    ; isb: a write to FPCR is not guaranteed to be seen by later FP instructions without a
    ; context-synchronisation event.
    mrs     x11, fpcr
    orr     x11, x11, #0x1000000
    msr     fpcr, x11
    isb

    ; ContextSwitch(&ctx->fibSp, &ctx->mainSp). Reloaded from the stack because x9 is caller-saved
    ; and ContextSwitch uses it as its own scratch.
    ldr     x9, [sp]
    add     x0, x9, #0              ; &fibSp
    add     x1, x9, #8              ; &mainSp
    bl      ContextSwitch

    b       fe_loop                 ; resumed: go round again. Never returns; returning would hit
                                    ; FiberTrampoline's brk.
    ENDP

; -------------------------------------------------------------------------------------------
; uint64_t AbiProbe(void** from, void** to)
;   x0 = from, x1 = to
;
; Loads a distinct sentinel into each callee-saved register, performs one ContextSwitch (which lands
; in FiberEntryAsm above, guaranteeing every one of those registers is stamped with 0xDEAD before
; control comes back), then verifies each against its sentinel.
;
; RETURNS A BITMASK, one bit per register, 0 meaning everything survived:
;   bits 0-9   x19..x28
;   bits 10-17 d8..d15
; A mask rather than a bool so a partial failure names exactly which registers moved -- a switch
; with an off-by-one in its frame typically loses one PAIR, and that pattern is the diagnosis.
;
; Branchless comparison (cmp + cset + orr) rather than conditional jumps: no branches means no
; chance of the comparison sequence itself perturbing what is being measured.
AbiProbe PROC

    ; This routine is itself an AAPCS64 function, so it must preserve x19-x28 and d8-d15 for ITS
    ; caller -- and it is about to overwrite all of them. Save first, restore at the end.
    stp     x29, x30, [sp, #-16]!
    mov     x29, sp
    stp     x19, x20, [sp, #-16]!
    stp     x21, x22, [sp, #-16]!
    stp     x23, x24, [sp, #-16]!
    stp     x25, x26, [sp, #-16]!
    stp     x27, x28, [sp, #-16]!
    stp     d8,  d9,  [sp, #-16]!
    stp     d10, d11, [sp, #-16]!
    stp     d12, d13, [sp, #-16]!
    stp     d14, d15, [sp, #-16]!

    ; Sentinels. Distinct per register so a mask bit and a dumped value agree about which one moved.
    ; x0/x1 hold the switch arguments and are NOT in the callee-saved set, so they survive this.
    mov     x19, #0x1919
    mov     x20, #0x2020
    mov     x21, #0x2121
    mov     x22, #0x2222
    mov     x23, #0x2323
    mov     x24, #0x2424
    mov     x25, #0x2525
    mov     x26, #0x2626
    mov     x27, #0x2727
    mov     x28, #0x2828

    mov     x9,  #0xD808
    fmov    d8,  x9
    mov     x9,  #0xD909
    fmov    d9,  x9
    mov     x9,  #0xD010
    fmov    d10, x9
    mov     x9,  #0xD011
    fmov    d11, x9
    mov     x9,  #0xD012
    fmov    d12, x9
    mov     x9,  #0xD013
    fmov    d13, x9
    mov     x9,  #0xD014
    fmov    d14, x9
    mov     x9,  #0xD015
    fmov    d15, x9

    bl      ContextSwitch           ; x0/x1 still hold from/to

    ; Verify. x11 accumulates the failure mask.
    mov     x11, #0

    mov     x9, #0x1919
    cmp     x19, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #0
    mov     x9, #0x2020
    cmp     x20, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #1
    mov     x9, #0x2121
    cmp     x21, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #2
    mov     x9, #0x2222
    cmp     x22, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #3
    mov     x9, #0x2323
    cmp     x23, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #4
    mov     x9, #0x2424
    cmp     x24, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #5
    mov     x9, #0x2525
    cmp     x25, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #6
    mov     x9, #0x2626
    cmp     x26, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #7
    mov     x9, #0x2727
    cmp     x27, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #8
    mov     x9, #0x2828
    cmp     x28, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #9

    fmov    x12, d8
    mov     x9,  #0xD808
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #10
    fmov    x12, d9
    mov     x9,  #0xD909
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #11
    fmov    x12, d10
    mov     x9,  #0xD010
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #12
    fmov    x12, d11
    mov     x9,  #0xD011
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #13
    fmov    x12, d12
    mov     x9,  #0xD012
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #14
    fmov    x12, d13
    mov     x9,  #0xD013
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #15
    fmov    x12, d14
    mov     x9,  #0xD014
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #16
    fmov    x12, d15
    mov     x9,  #0xD015
    cmp     x12, x9
    cset    x10, ne
    orr     x11, x11, x10, lsl #17

    mov     x0, x11                 ; return the mask

    ldp     d14, d15, [sp], #16
    ldp     d12, d13, [sp], #16
    ldp     d10, d11, [sp], #16
    ldp     d8,  d9,  [sp], #16
    ldp     x27, x28, [sp], #16
    ldp     x25, x26, [sp], #16
    ldp     x23, x24, [sp], #16
    ldp     x21, x22, [sp], #16
    ldp     x19, x20, [sp], #16
    ldp     x29, x30, [sp], #16
    ret

    ENDP

    END
