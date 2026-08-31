// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Standalone test for the AAPCS64 context switch, with NO scheduler attached.
// Links against exactly one other file -- src/posix/aarch64/ContextSwitch.S -- so a failure here
// is the assembly's fault and nothing else's.
//
//   clang++ -std=c++17 -O1 -g -o fibertest \
//       tests/fibertest_aarch64.cpp src/posix/aarch64/ContextSwitch.S
//   ./fibertest ; echo "exit $?"
//
// Run it at -O0 AND -O2 before believing it. Register allocation differs enough between them that
// a switch can pass at one and fail at the other.
//
// What it proves, in the order the failures matter:
//   1. a round trip happens at all (switch in, switch back)
//   2. repeated switches are stable -- a frame-size error usually survives one and dies on two
//   3. every callee-saved GPR (x19-x28) survives a switch
//   4. every callee-saved FP register (d8-d15) survives a switch
//   5. FPCR does not leak between contexts (the AArch64 analogue of MXCSR)
//   6. the fiber stack's guard page still faults
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#include <csignal>

extern "C" void ContextSwitch(void** from, void** to);
extern "C" void FiberTrampoline();

// ---------------------------------------------------------------------------------------------
// The frame, as ContextSwitch.S saves it. 176 bytes, 22 slots.
// THIS IS THE REFERENCE for src/posix/aarch64/FiberInit.cpp -- if the two ever disagree, this one
// is the one that has been run.
//   [0..7]  d8..d15      [8] FPCR      [9] pad
//   [10] x19  [11] x20  [12] x21  [13] x22  [14] x23
//   [15] x24  [16] x25  [17] x26  [18] x27  [19] x28
//   [20] x29 (FP)        [21] x30 (LR)
static const size_t kFrameBytes = 176;
static const int    kSlotX19    = 10;   // byte +80
static const int    kSlotX30    = 21;   // byte +168

static void* MakeFiber(void* stackBase, size_t stackSize, void (*entry)()) {
    uintptr_t top = (uintptr_t)((char*)stackBase + stackSize) & ~(uintptr_t)0xF;
    uint64_t* f = (uint64_t*)(top - kFrameBytes);
    memset(f, 0, kFrameBytes);
    // FPCR slot stays 0: that IS the AAPCS64 default (round-to-nearest, FZ off, no trapped
    // exceptions), so no explicit seed value is needed the way MXCSR's 0x1F80 was on x86.
    f[kSlotX19] = (uint64_t)entry;              // the trampoline's 'blr x19' target
    f[kSlotX30] = (uint64_t)&FiberTrampoline;   // ContextSwitch's 'ret' branches here, not to a
                                                // popped return address -- the structural
                                                // difference from both x86 ports
    return (void*)f;
}

// ---------------------------------------------------------------------------------------------
static inline uint64_t GetFpcr() {
    uint64_t v; asm volatile("mrs %0, fpcr" : "=r"(v)); return v;
}
static inline void SetFpcr(uint64_t v) {
    // isb: a write to FPCR is not guaranteed to be observed by later FP instructions without a
    // context-synchronisation event.
    asm volatile("msr fpcr, %0\n\tisb" :: "r"(v) : "memory");
}
static const uint64_t kFpcrFZ = (uint64_t)1 << 24;   // flush-to-zero: a mode main must not inherit

static void* g_main = nullptr;   // saved SP of the "thread" context
static void* g_fib  = nullptr;   // saved SP of the fiber
static int   g_hits = 0;

static void FiberEntry() {
    for (;;) {
        ++g_hits;
        // Stamp recognisable garbage into every callee-saved register, then hand control back.
        // If ContextSwitch does not restore properly, main's sentinels come back holding THESE
        // values -- which is far easier to diagnose than an arbitrary clobber.
        asm volatile(
            "mov  x19, #0xDEAD\n\t"  "mov  x20, #0xDEAD\n\t"
            "mov  x21, #0xDEAD\n\t"  "mov  x22, #0xDEAD\n\t"
            "mov  x23, #0xDEAD\n\t"  "mov  x24, #0xDEAD\n\t"
            "mov  x25, #0xDEAD\n\t"  "mov  x26, #0xDEAD\n\t"
            "mov  x27, #0xDEAD\n\t"  "mov  x28, #0xDEAD\n\t"
            "fmov d8,  #-1.0\n\t"    "fmov d9,  #-1.0\n\t"
            "fmov d10, #-1.0\n\t"    "fmov d11, #-1.0\n\t"
            "fmov d12, #-1.0\n\t"    "fmov d13, #-1.0\n\t"
            "fmov d14, #-1.0\n\t"    "fmov d15, #-1.0"
            ::: "x19","x20","x21","x22","x23","x24","x25","x26","x27","x28",
                "d8","d9","d10","d11","d12","d13","d14","d15");
        SetFpcr(GetFpcr() | kFpcrFZ);
        ContextSwitch(&g_fib, &g_main);
    }
    // Deliberately unreachable: returning would hit the trampoline's brk.
}

// ---------------------------------------------------------------------------------------------
// x19-x28 are ALL the callee-saved GPRs, so pinning all ten at once leaves the allocator nothing
// to work with. Two passes of five keeps the pressure sane and still covers every register.
#define PIN5(P, A,B,C,D,E)                                                              \
    register uint64_t P##a asm(A) = 0x1111111111111111ull ^ (uintptr_t)A[1];            \
    register uint64_t P##b asm(B) = 0x2222222222222222ull ^ (uintptr_t)B[1];            \
    register uint64_t P##c asm(C) = 0x3333333333333333ull ^ (uintptr_t)C[1];            \
    register uint64_t P##d asm(D) = 0x4444444444444444ull ^ (uintptr_t)D[1];            \
    register uint64_t P##e asm(E) = 0x5555555555555555ull ^ (uintptr_t)E[1]
#define TIE5(P)  asm volatile("" : "+r"(P##a), "+r"(P##b), "+r"(P##c), "+r"(P##d), "+r"(P##e))
#define CHK1(NAME, VAR, WANT)                                                           \
    if ((VAR) != (WANT)) {                                                              \
        printf("  FAIL %-4s got %016llx want %016llx\n", NAME,                          \
               (unsigned long long)(VAR), (unsigned long long)(WANT)); ok = false; }

static bool CheckGprsLow() {
    bool ok = true;
    PIN5(p, "x19","x20","x21","x22","x23");
    const uint64_t w0=pa, w1=pb, w2=pc, w3=pd, w4=pe;
    TIE5(p);
    ContextSwitch(&g_main, &g_fib);
    TIE5(p);
    CHK1("x19", pa, w0) CHK1("x20", pb, w1) CHK1("x21", pc, w2)
    CHK1("x22", pd, w3) CHK1("x23", pe, w4)
    return ok;
}
static bool CheckGprsHigh() {
    bool ok = true;
    PIN5(q, "x24","x25","x26","x27","x28");
    const uint64_t w0=qa, w1=qb, w2=qc, w3=qd, w4=qe;
    TIE5(q);
    ContextSwitch(&g_main, &g_fib);
    TIE5(q);
    CHK1("x24", qa, w0) CHK1("x25", qb, w1) CHK1("x26", qc, w2)
    CHK1("x27", qd, w3) CHK1("x28", qe, w4)
    return ok;
}

static bool CheckFp() {
    bool ok = true;
    register double f8  asm("d8")  = 8.25;   register double f9  asm("d9")  = 9.25;
    register double f10 asm("d10") = 10.25;  register double f11 asm("d11") = 11.25;
    register double f12 asm("d12") = 12.25;  register double f13 asm("d13") = 13.25;
    register double f14 asm("d14") = 14.25;  register double f15 asm("d15") = 15.25;
    asm volatile("" : "+w"(f8),"+w"(f9),"+w"(f10),"+w"(f11),"+w"(f12),"+w"(f13),"+w"(f14),"+w"(f15));
    ContextSwitch(&g_main, &g_fib);
    asm volatile("" : "+w"(f8),"+w"(f9),"+w"(f10),"+w"(f11),"+w"(f12),"+w"(f13),"+w"(f14),"+w"(f15));
    const double want[8] = { 8.25, 9.25, 10.25, 11.25, 12.25, 13.25, 14.25, 15.25 };
    const double got [8] = { f8, f9, f10, f11, f12, f13, f14, f15 };
    for (int i = 0; i < 8; ++i)
        if (got[i] != want[i]) {
            printf("  FAIL d%-3d got %f want %f\n", i + 8, got[i], want[i]);
            ok = false;
        }
    return ok;
}

// ---------------------------------------------------------------------------------------------
int main() {
    const size_t kStack = 64 * 1024;
    const size_t kPage  = (size_t)sysconf(_SC_PAGESIZE);

    // mmap equivalent of the Windows VirtualAlloc arena: reserve the region unreadable, then make
    // everything ABOVE the lowest page usable. The lowest page stays PROT_NONE so an overflow
    // faults immediately instead of silently eating the next fiber's stack.
    void* region = mmap(nullptr, kStack, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (region == MAP_FAILED) { perror("mmap"); return 1; }
    if (mprotect((char*)region + kPage, kStack - kPage, PROT_READ | PROT_WRITE) != 0) {
        perror("mprotect"); return 1;
    }

    g_fib = MakeFiber(region, kStack, FiberEntry);
    // Page size is worth printing: Apple Silicon uses 16K pages where Linux/ARM typically uses 4K,
    // so the guard costs 16K of a 64K fiber stack there rather than 4K.
    printf("frame %zu bytes, x19 slot +%d, x30 slot +%d, page %zu\n",
           kFrameBytes, kSlotX19 * 8, kSlotX30 * 8, kPage);

    const uint64_t mainFpcr = GetFpcr();

    // 1+2. The first switch is the one that proves the seeded frame is shaped right; do a couple
    // of bare round trips before anything more delicate, so a total failure reports as a crash
    // HERE rather than inside a register check.
    ContextSwitch(&g_main, &g_fib);
    ContextSwitch(&g_main, &g_fib);
    printf("round trips        : %s (%d so far)\n", g_hits >= 2 ? "ok" : "NO RETURN", g_hits);

    bool gprOk = CheckGprsLow();
    gprOk = CheckGprsHigh() && gprOk;
    printf("callee-saved GPRs  : %s\n", gprOk ? "preserved" : "CLOBBERED (x19-x28)");

    bool fpOk = CheckFp();
    printf("callee-saved d8-d15: %s\n", fpOk ? "preserved" : "CLOBBERED");

    // 5. The fiber set FZ on every pass. If the switch does not save/restore FPCR, it is set here.
    const uint64_t nowFpcr = GetFpcr();
    bool fpcrOk = (nowFpcr == mainFpcr);
    printf("FPCR isolation     : %s (main %016llx, now %016llx)\n",
           fpcrOk ? "preserved" : "LEAKED",
           (unsigned long long)mainFpcr, (unsigned long long)nowFpcr);
    if (!fpcrOk && ((nowFpcr ^ mainFpcr) & kFpcrFZ))
        printf("  -> FZ leaked out of the fiber: the FPCR slot is not being restored\n");

    // 6. Guard page must still fault. Forked so the fault kills a child, not the test.
    //
    // ACCEPT SIGSEGV *OR* SIGBUS. Linux raises SIGSEGV for a protection violation; macOS/Mach
    // raises SIGBUS when the page is MAPPED but protected, and reserves SIGSEGV for addresses that
    // are not mapped at all. Checking only SIGSEGV made this report "*** WRITABLE ***" on Apple
    // Silicon for a guard page that was working perfectly -- a false alarm about the one safety
    // property the arena exists to provide, which is the worst kind of test bug to leave in.
    printf("guard page         : ");
    fflush(stdout);
    pid_t pid = fork();
    if (pid == 0) { *(volatile char*)region = 1; _exit(0); }   // must not survive this
    int st = 0; waitpid(pid, &st, 0);
    const int sig = WIFSIGNALED(st) ? WTERMSIG(st) : 0;
    const bool faulted = (sig == SIGSEGV || sig == SIGBUS);
    printf("%s\n", faulted
           ? (sig == SIGBUS ? "faults as expected (SIGBUS)" : "faults as expected (SIGSEGV)")
           : "*** WRITABLE ***");

    bool pass = g_hits >= 2 && gprOk && fpOk && fpcrOk && faulted;
    printf("\n%s\n", pass ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return pass ? 0 : 1;
}
