; SPDX-License-Identifier: BSD-3-Clause
; Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
;
; BENCH-ONLY context-switch variants. NOTHING HERE SHIPS.
;
; WHY THIS FILE EXISTS. src/win32/ContextSwitch.asm saves XMM6-15 with legacy-SSE `movdqa`. On an
; AVX machine, a legacy-SSE instruction executed while the upper halves of YMM are DIRTY costs an
; SSE/AVX transition -- a real, documented, microarchitecture-dependent penalty. A fiber that parks
; immediately after an AVX kernel (which is exactly what a game engine does) would pay it on every
; switch. The two candidate fixes are one `vzeroupper` at entry, or VEX-encoded `vmovdqa`, and the
; only question worth answering is whether either is worth taking.
;
; THE FRAME IS NOT BEING CHANGED. Same 168 bytes, same offsets, same ten registers, same MXCSR/FCW
; slots as the shipped routine -- these are drop-in interchangeable with it and with each other, on
; the same fiber stacks, mid-run. Only the ENCODING of the ten stores and ten loads differs. That is
; also what makes an honest A/B possible: every arm runs in ONE process against ONE pair of stacks,
; so there is no cross-build comparison to be fooled by.
;
; THE MACRO IS THE POINT. Writing the three variants out by hand would leave three sequences that
; have to be proven identical by reading them. Expanding all three from CS_BODY makes "identical
; except for the mnemonic" a property of the source instead of a claim in a comment.

.const
        align 16
; 1.0f x8. BenchDirtyUpper multiplies this by itself, so the value stays 1.0 forever: no denormals,
; no infinities, no input-dependent slow path that could land on one arm and not another.
;
; Loaded with vmovUPS, not vmovAPS: ml64's .const segment aligns to 16, not 32, so a 32-byte aligned
; load is not expressible here without a custom segment. It does not matter -- the load is outside
; the switch and identical in both stubs, so whatever it costs it costs both arms equally.
kOnesPs dd  8 dup(3F800000h)

.code

; ---- the shipped sequence, parameterised by the 16-byte move ---------------------------------
CS_BODY MACRO mov16:REQ
    ; 1. Callee-saved GPRs.
    push rbx
    push rbp
    push rdi
    push rsi
    push r12
    push r13
    push r14
    push r15

    ; 2. Non-volatile XMM6-15. 168 = 160 (10*16) + 8 realignment; the alignment argument is spelled
    ;    out in src/win32/ContextSwitch.asm and is unchanged here.
    sub rsp, 168
    mov16 xmmword ptr [rsp + 0],   xmm6
    mov16 xmmword ptr [rsp + 16],  xmm7
    mov16 xmmword ptr [rsp + 32],  xmm8
    mov16 xmmword ptr [rsp + 48],  xmm9
    mov16 xmmword ptr [rsp + 64],  xmm10
    mov16 xmmword ptr [rsp + 80],  xmm11
    mov16 xmmword ptr [rsp + 96],  xmm12
    mov16 xmmword ptr [rsp + 112], xmm13
    mov16 xmmword ptr [rsp + 128], xmm14
    mov16 xmmword ptr [rsp + 144], xmm15

    ; 2b. Nonvolatile FP control state, in the alignment slack.
    stmxcsr dword ptr [rsp + 160]
    fnstcw  word  ptr [rsp + 164]

    ; 3. Swap stacks.
    mov [rcx], rsp
    mov rsp, [rdx]

    ; 4. Restore FP control state, then XMM.
    ldmxcsr dword ptr [rsp + 160]
    fldcw   word  ptr [rsp + 164]
    mov16 xmm6,  xmmword ptr [rsp + 0]
    mov16 xmm7,  xmmword ptr [rsp + 16]
    mov16 xmm8,  xmmword ptr [rsp + 32]
    mov16 xmm9,  xmmword ptr [rsp + 48]
    mov16 xmm10, xmmword ptr [rsp + 64]
    mov16 xmm11, xmmword ptr [rsp + 80]
    mov16 xmm12, xmmword ptr [rsp + 96]
    mov16 xmm13, xmmword ptr [rsp + 112]
    mov16 xmm14, xmmword ptr [rsp + 128]
    mov16 xmm15, xmmword ptr [rsp + 144]
    add rsp, 168

    ; 5. Callee-saved GPRs back.
    pop r15
    pop r14
    pop r13
    pop r12
    pop rsi
    pop rdi
    pop rbp
    pop rbx
    ret
ENDM

; ARM A -- byte-for-byte the shipped sequence. Duplicated rather than measured through the library
; symbol so that it sits in the same module, under the same alignment pressure, as the arms it is
; compared against. The library symbol is ALSO measured, as a cross-module control: if those two
; rows disagree, the difference is code placement and no other row means anything.
CsSse PROC
    CS_BODY movdqa
CsSse ENDP

; ARM B -- one vzeroupper on entry, then the identical legacy-SSE body.
; Legal because the upper halves of YMM are VOLATILE across a call under the Win64 ABI: a context
; switch is an opaque call, so anything the compiler had live up there is already spilled. One
; vzeroupper at the top covers BOTH halves of the routine -- the saves for the outgoing fiber and
; the loads for the incoming one -- because nothing between them re-dirties the upper state.
CsSseVzu PROC
    vzeroupper
    CS_BODY movdqa
CsSseVzu ENDP

; ARM C -- VEX encodings, no vzeroupper. vmovdqa xmm still writes 128 bits and still saves exactly
; the ten registers the ABI requires; it does not touch, need, or preserve any YMM state. It simply
; never enters the legacy-SSE domain, so there is no transition to pay for.
CsAvx PROC
    CS_BODY vmovdqa
CsAvx ENDP


; ---- upper-YMM state stubs -------------------------------------------------------------------
; Both arms of the dirty/clean comparison do the SAME 256-bit arithmetic and differ by exactly one
; instruction, so the measured gap is the transition rather than the kernel.
;
; These are assembly rather than intrinsics on purpose: MSVC inserts vzeroupper of its own accord at
; function boundaries when it decides the upper state wants clearing, which would quietly make the
; "dirty" arm clean and the entire comparison vacuous.

; Leaves the upper 128 bits of YMM0 live. This is the state a fiber parks in when it suspends out of
; an AVX kernel without a vzeroupper -- the case this whole bench is about.
BenchDirtyUpper PROC
    vmovups ymm0, ymmword ptr [kOnesPs]
    vmulps  ymm0, ymm0, ymm0
    ret
BenchDirtyUpper ENDP

; The control: identical arithmetic, upper halves retired before returning.
BenchCleanUpper PROC
    vmovups ymm0, ymmword ptr [kOnesPs]
    vmulps  ymm0, ymm0, ymm0
    vzeroupper
    ret
BenchCleanUpper ENDP


; ---- correctness gate ------------------------------------------------------------------------
; DELIBERATELY ABI-VIOLATING, and only ever called from the bench's fiber loop during verification.
; Fills XMM6-15 -- its CALLER's non-volatile registers -- with a pattern and does not put them back.
; That is the entire purpose: it is what makes the round-trip check non-vacuous. With no clobber in
; between, a variant that saved nothing at all would still appear to "preserve" the pattern, because
; nothing would have disturbed it.
BenchClobberXmm PROC
    mov eax, 05A5A5A5Ah
    movd xmm6, eax
    pshufd xmm6, xmm6, 0
    movdqa xmm7,  xmm6
    movdqa xmm8,  xmm6
    movdqa xmm9,  xmm6
    movdqa xmm10, xmm6
    movdqa xmm11, xmm6
    movdqa xmm12, xmm6
    movdqa xmm13, xmm6
    movdqa xmm14, xmm6
    movdqa xmm15, xmm6
    ret
BenchClobberXmm ENDP

; void BenchXmmRoundTrip(Context* from, Context* to, CsFn fn, void* out160)
;   rcx = from, rdx = to, r8 = fn, r9 = out (160 bytes)
;
; Writes a distinct pattern into each of XMM6-15, performs ONE round trip through `fn` (out to the
; fiber, which clobbers all ten, and back), then stores the ten registers to `out`. If the variant
; saves and restores correctly, `out` holds the pattern; if it uses a wrong offset, drops a
; register, or saves nothing, `out` holds the fiber's clobber value and the caller reports it.
; Preserves its own caller's XMM6-15 around all of that, since it destroys them.
BenchXmmRoundTrip PROC
    push rbx                      ; entry RSP is 8 mod 16 -> 0 mod 16, which is what `call` wants
    sub  rsp, 192                 ; [0,32) shadow space for the call, [32,192) our caller's XMM6-15

    movdqa [rsp + 32],  xmm6
    movdqa [rsp + 48],  xmm7
    movdqa [rsp + 64],  xmm8
    movdqa [rsp + 80],  xmm9
    movdqa [rsp + 96],  xmm10
    movdqa [rsp + 112], xmm11
    movdqa [rsp + 128], xmm12
    movdqa [rsp + 144], xmm13
    movdqa [rsp + 160], xmm14
    movdqa [rsp + 176], xmm15

    ; `out` has to survive the round trip, so it goes in RBX -- non-volatile, and therefore saved
    ; and restored by the very routine under test. R8/R9 would not survive; they are volatile.
    mov rbx, r9
    mov r10, r8                   ; fn, called below with RCX/RDX still holding from/to

    mov eax, 0A6A6A6A6h
    movd xmm6, eax
    pshufd xmm6, xmm6, 0
    mov eax, 0A7A7A7A7h
    movd xmm7, eax
    pshufd xmm7, xmm7, 0
    mov eax, 0A8A8A8A8h
    movd xmm8, eax
    pshufd xmm8, xmm8, 0
    mov eax, 0A9A9A9A9h
    movd xmm9, eax
    pshufd xmm9, xmm9, 0
    mov eax, 0AAAAAAAAh
    movd xmm10, eax
    pshufd xmm10, xmm10, 0
    mov eax, 0ABABABABh
    movd xmm11, eax
    pshufd xmm11, xmm11, 0
    mov eax, 0ACACACACh
    movd xmm12, eax
    pshufd xmm12, xmm12, 0
    mov eax, 0ADADADADh
    movd xmm13, eax
    pshufd xmm13, xmm13, 0
    mov eax, 0AEAEAEAEh
    movd xmm14, eax
    pshufd xmm14, xmm14, 0
    mov eax, 0AFAFAFAFh
    movd xmm15, eax
    pshufd xmm15, xmm15, 0

    call r10                      ; out to the fiber and back

    movdqu [rbx + 0],   xmm6
    movdqu [rbx + 16],  xmm7
    movdqu [rbx + 32],  xmm8
    movdqu [rbx + 48],  xmm9
    movdqu [rbx + 64],  xmm10
    movdqu [rbx + 80],  xmm11
    movdqu [rbx + 96],  xmm12
    movdqu [rbx + 112], xmm13
    movdqu [rbx + 128], xmm14
    movdqu [rbx + 144], xmm15

    movdqa xmm6,  [rsp + 32]
    movdqa xmm7,  [rsp + 48]
    movdqa xmm8,  [rsp + 64]
    movdqa xmm9,  [rsp + 80]
    movdqa xmm10, [rsp + 96]
    movdqa xmm11, [rsp + 112]
    movdqa xmm12, [rsp + 128]
    movdqa xmm13, [rsp + 144]
    movdqa xmm14, [rsp + 160]
    movdqa xmm15, [rsp + 176]

    add rsp, 192
    pop rbx
    ret
BenchXmmRoundTrip ENDP

END
