// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Standalone test for the WINDOWS/ARM64 context switch, with NO scheduler attached.
// Links against exactly two other files -- src/win32/aarch64/ContextSwitch.asm and
// tests/win32/fibertest_probe_aarch64.asm -- so a failure here is the assembly's fault and nothing
// else's. Windows counterpart of tests/fibertest_aarch64.cpp; same six checks, same order.
//
//   cmake --build build --config Release --target SchedulerFiberAbiTest
//   ./build/bin/Release/SchedulerFiberAbiTest.exe ; echo "exit $?"
//
// WHY THIS IS A SEPARATE FILE rather than #ifdefs in the POSIX harness. Two reasons, and the first
// one is decisive:
//
//   1. MSVC SUPPORTS NO INLINE ASSEMBLY ON ARM64 -- none at all. The POSIX harness is built almost
//      entirely out of `register uint64_t v asm("x19")` pins and asm volatile blocks, and there is
//      no MSVC spelling for any of it. The register work had to move into a companion .asm file
//      (tests/win32/fibertest_probe_aarch64.asm), which changes the SHAPE of the test, not just its
//      syntax. #ifdefs over that would leave two unrelated programs in one file.
//   2. The guard-page check is structurally different: fork() does not exist, and SEH catches the
//      fault in-process instead. That is simpler than the POSIX version, not harder -- no child
//      process, no signal decoding, no SIGSEGV-vs-SIGBUS ambiguity.
//
// What it proves, in the order the failures matter:
//   1. a round trip happens at all (switch in, switch back)
//   2. repeated switches are stable -- a frame-size error usually survives one and dies on two
//   3. every callee-saved GPR (x19-x28) survives a switch
//   4. every callee-saved FP register (d8-d15) survives a switch
//   5. FPCR does not leak between contexts (the AArch64 analogue of MXCSR)
//   6. the fiber stack's guard page still faults
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <intrin.h>
#include <cstdio>
#include <cstdint>
#include <cstring>

extern "C" void     ContextSwitch(void** from, void** to);
extern "C" void     FiberTrampoline();
extern "C" void     FiberEntryAsm();
extern "C" uint64_t AbiProbe(void** from, void** to);

// Shared with fibertest_probe_aarch64.asm, which hard-codes these offsets (+0, +8, +16).
// Keep the two in step.
struct FiberCtx {
    void*    fibSp;    // +0
    void*    mainSp;   // +8
    uint64_t hits;     // +16
};
static FiberCtx g_ctx{};

// ---------------------------------------------------------------------------------------------
// The frame, as src/win32/aarch64/ContextSwitch.asm saves it. 176 bytes, 22 slots -- IDENTICAL to
// the POSIX AAPCS64 layout, which is the whole reason src/posix/aarch64/FiberInit.cpp is reused by
// the Windows ARM64 build rather than duplicated.
// THIS IS THE REFERENCE for that file -- if the two ever disagree, this one is the one that has run.
//   [0..7]  d8..d15      [8] FPCR      [9] pad
//   [10] x19  [11] x20  [12] x21  [13] x22  [14] x23
//   [15] x24  [16] x25  [17] x26  [18] x27  [19] x28
//   [20] x29 (FP)        [21] x30 (LR)
static const size_t kFrameBytes = 176;
static const int    kSlotX19    = 10;   // byte +80
static const int    kSlotX20    = 11;   // byte +88
static const int    kSlotX30    = 21;   // byte +168

static void* MakeFiber(void* stackBase, size_t stackSize, void (*entry)(), void* ctx) {
    uintptr_t top = (uintptr_t)((char*)stackBase + stackSize) & ~(uintptr_t)0xF;
    uint64_t* f = (uint64_t*)(top - kFrameBytes);
    memset(f, 0, kFrameBytes);
    // FPCR slot stays 0: that IS the AAPCS64 default (round-to-nearest, FZ off, no trapped
    // exceptions), so no explicit seed is needed the way MXCSR's 0x1F80 was on x86.
    f[kSlotX19] = (uint64_t)entry;              // the trampoline's 'blr x19' target
    f[kSlotX20] = (uint64_t)ctx;                // FiberEntryAsm reads its context from x20, because
                                                // the probe assembly references no globals
    f[kSlotX30] = (uint64_t)&FiberTrampoline;   // ContextSwitch's 'ret' branches HERE, not to a
                                                // popped return address -- the structural
                                                // difference from both x86 ports
    return (void*)f;
}

// ---------------------------------------------------------------------------------------------
// _ReadStatusReg is the MSVC intrinsic standing in for `mrs x0, fpcr`. ARM64_FPCR comes from
// <arm64intr.h>, pulled in by <intrin.h> on ARM64 targets.
static inline uint64_t GetFpcr() { return (uint64_t)_ReadStatusReg(ARM64_FPCR); }
static const uint64_t kFpcrFZ = (uint64_t)1 << 24;   // flush-to-zero: a mode main must not inherit

// Guard-page probe. SEH instead of fork(): the fault is caught in-process, so there is no child to
// reap and no signal to decode. Its own function with no C++ objects, because __try/__except cannot
// coexist with anything requiring unwinding in the same frame.
static bool GuardPageFaults(void* addr) {
    __try {
        *(volatile char*)addr = 1;
        return false;                 // wrote successfully -- the guard is NOT protecting anything
    }
    __except (GetExceptionCode() == EXCEPTION_ACCESS_VIOLATION
                  ? EXCEPTION_EXECUTE_HANDLER : EXCEPTION_CONTINUE_SEARCH) {
        return true;
    }
}

// ---------------------------------------------------------------------------------------------
static const char* kRegNames[18] = {
    "x19","x20","x21","x22","x23","x24","x25","x26","x27","x28",
    "d8","d9","d10","d11","d12","d13","d14","d15"
};

int main() {
    const size_t kStack = 64 * 1024;
    SYSTEM_INFO si{};
    GetSystemInfo(&si);
    const size_t kPage = si.dwPageSize;

    // The VirtualAlloc equivalent of the POSIX mmap arena: reserve the whole region, then commit
    // everything ABOVE the lowest page. The lowest page stays RESERVED-BUT-UNCOMMITTED, so an
    // overflow faults immediately instead of silently eating whatever is below it.
    void* region = VirtualAlloc(nullptr, kStack, MEM_RESERVE, PAGE_NOACCESS);
    if (!region) { printf("VirtualAlloc reserve failed: %lu\n", GetLastError()); return 1; }
    if (!VirtualAlloc((char*)region + kPage, kStack - kPage, MEM_COMMIT, PAGE_READWRITE)) {
        printf("VirtualAlloc commit failed: %lu\n", GetLastError()); return 1;
    }

    g_ctx.fibSp = MakeFiber(region, kStack, FiberEntryAsm, &g_ctx);

    printf("frame %zu bytes, x19 slot +%d, x20 slot +%d, x30 slot +%d, page %zu\n",
           kFrameBytes, kSlotX19 * 8, kSlotX20 * 8, kSlotX30 * 8, kPage);

    const uint64_t mainFpcr = GetFpcr();

    // 1+2. The first switch proves the seeded frame is shaped right; do a couple of bare round
    // trips before anything more delicate, so a total failure reports as a crash HERE rather than
    // inside a register check.
    ContextSwitch(&g_ctx.mainSp, &g_ctx.fibSp);
    ContextSwitch(&g_ctx.mainSp, &g_ctx.fibSp);
    const bool tripsOk = g_ctx.hits >= 2;
    printf("round trips        : %s (%llu so far)\n",
           tripsOk ? "ok" : "NO RETURN", (unsigned long long)g_ctx.hits);

    // 3+4. One call covers every callee-saved GPR and FP register: the probe seeds them all, drives
    // one switch (which lands in FiberEntryAsm and stamps 0xDEAD over every one of them), and
    // verifies on return. The POSIX harness needs two passes of five because pinning all ten GPRs
    // at once leaves the register allocator nothing to work with -- an inline-asm constraint that
    // simply does not arise when the whole probe IS assembly.
    const uint64_t mask = AbiProbe(&g_ctx.mainSp, &g_ctx.fibSp);
    const bool gprOk = (mask & 0x3FF) == 0;          // bits 0-9
    const bool fpOk  = (mask & 0x3FC00) == 0;        // bits 10-17
    if (mask) {
        printf("  clobber mask %05llx ->", (unsigned long long)mask);
        for (int i = 0; i < 18; ++i) if (mask & (1ull << i)) printf(" %s", kRegNames[i]);
        printf("\n");
    }
    printf("callee-saved GPRs  : %s\n", gprOk ? "preserved" : "CLOBBERED (x19-x28)");
    printf("callee-saved d8-d15: %s\n", fpOk ? "preserved" : "CLOBBERED");

    // 5. The fiber set FZ on every pass. If the switch does not save/restore FPCR, it is set here.
    const uint64_t nowFpcr = GetFpcr();
    const bool fpcrOk = (nowFpcr == mainFpcr);
    printf("FPCR isolation     : %s (main %016llx, now %016llx)\n",
           fpcrOk ? "preserved" : "LEAKED",
           (unsigned long long)mainFpcr, (unsigned long long)nowFpcr);
    if (!fpcrOk && ((nowFpcr ^ mainFpcr) & kFpcrFZ))
        printf("  -> FZ leaked out of the fiber: the FPCR slot is not being restored\n");

    // 6. Guard page must still fault.
    const bool faulted = GuardPageFaults(region);
    printf("guard page         : %s\n",
           faulted ? "faults as expected (ACCESS_VIOLATION)" : "*** WRITABLE ***");

    const bool pass = tripsOk && gprOk && fpOk && fpcrOk && faulted;
    printf("\n%s\n", pass ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return pass ? 0 : 1;
}
