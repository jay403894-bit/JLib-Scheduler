.code
ContextSwitch PROC
    ; RCX = 'from' pointer (ptr to rsp), RDX = 'to' pointer (ptr to rsp)

    ; 1. Save Callee-Saved GPRs
    push rbx
    push rbp
    push rdi
    push rsi
    push r12
    push r13
    push r14
    push r15

    ; 2. Save Non-Volatile XMM Registers (6 through 15).
    ; After 8 pushes RSP is 8 mod 16 -- the 'call' into here pushed an 8-byte return
    ; address onto a 16-aligned stack, and 8*8 bytes preserve that offset. Reserve
    ; 168 = 160 (10 * 16 for xmm6-15) + 8 dummy: the extra 8 realigns RSP back to 16,
    ; so the XMM block is 16-aligned and movdqa (aligned) is legal. The dummy 8 bytes
    ; sit at [rsp+160 .. rsp+168), between the XMM block and the GPR pushes.
    sub rsp, 168
    movdqa [rsp + 0], xmm6
    movdqa [rsp + 16], xmm7
    movdqa [rsp + 32], xmm8
    movdqa [rsp + 48], xmm9
    movdqa [rsp + 64], xmm10
    movdqa [rsp + 80], xmm11
    movdqa [rsp + 96], xmm12
    movdqa [rsp + 112], xmm13
    movdqa [rsp + 128], xmm14
    movdqa [rsp + 144], xmm15

    ; 2b. Save nonvolatile FP control state into the formerly-dummy 8 bytes at [rsp+160].
    ; MXCSR control bits (rounding, FTZ/DAZ, exception masks) and the x87 control word are
    ; ABI-nonvolatile, and MXCSR is one physical register shared by all fibers on a worker.
    ; Without this, a fiber that sets a rounding/FTZ mode and yields silently leaks it into
    ; whatever fiber resumes next. 4 bytes MXCSR + 2 bytes FCW fit in the alignment slack.
    stmxcsr dword ptr [rsp + 160]
    fnstcw  word  ptr [rsp + 164]

    ; 3. Swap Stack Pointers
    mov [rcx], rsp    ; Save old RSP (16-aligned)
    mov rsp, [rdx]    ; Load new RSP (16-aligned)

    ; 4. Restore nonvolatile FP control state (Fiber::Init seeds sane defaults), then XMM.
    ldmxcsr dword ptr [rsp + 160]
    fldcw   word  ptr [rsp + 164]
    movdqa xmm6,  [rsp + 0]
    movdqa xmm7,  [rsp + 16]
    movdqa xmm8,  [rsp + 32]
    movdqa xmm9,  [rsp + 48]
    movdqa xmm10, [rsp + 64]
    movdqa xmm11, [rsp + 80]
    movdqa xmm12, [rsp + 96]
    movdqa xmm13, [rsp + 112]
    movdqa xmm14, [rsp + 128]
    movdqa xmm15, [rsp + 144]
    add rsp, 168      ; drop XMM block + dummy (back to the GPR pushes)

    ; 5. Restore Callee-Saved GPRs
    pop r15
    pop r14
    pop r13
    pop r12
    pop rsi
    pop rdi
    pop rbp
    pop rbx

    ret
ContextSwitch ENDP

; Entry trampoline for freshly-initialized fibers. The restore in ContextSwitch lands
; here at a 16-aligned RSP (0 mod 16). Fiber::Init seeded RBX with the C++ entry point.
; Entering via 'call' pushes an 8-byte return address, so the C++ wrapper is entered at
; 8 mod 16 -- the alignment the ABI/compiler expects of a normally-called function.
; The wrapper never returns (it ContextSwitches away); ud2 traps if it ever does.
FiberTrampoline PROC
    call rbx
    ud2
FiberTrampoline ENDP
END
