// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
// ============================ THE ONE PLACE THAT DECIDES THE PLATFORM ============================
//
// Everything else in the scheduler tests JLIB_PLATFORM_WINDOWS / JLIB_PLATFORM_POSIX, never _WIN32
// or __linux__ directly. That is not ceremony: it means adding a third platform is an edit to THIS
// FILE plus new files under src/, rather than a hunt through ten translation units for every
// compiler macro someone happened to reach for.
//
// It also carries the small platform primitives -- the virtual-memory calls the fiber stack arena
// needs. Those are wrapped rather than #ifdef'd at their call sites, because the arena's actual
// LOGIC (page alignment, bounds checking, where the guard page goes and why) is identical on both
// platforms and deserves to exist exactly once. Only the three syscalls differ.
//
// Where an implementation diverges WHOLESALE rather than by a syscall -- the topology scan reads a
// Win32 API on Windows and parses sysfs text on Linux, sharing nothing -- the split is by FILE, in
// src/win32/ and src/posix/, not by #ifdef. Splitting by DIRECTORY rather than by filename suffix
// is deliberate: the CMake build globs src/*.cpp, so two same-named platform variants sitting side
// by side would both compile and collide.

#include <cstddef>   // std::size_t, used by the primitives below on BOTH platforms
#include <cstdint>

// TWO AXES, decided independently. The OS picks the syscalls; the ARCH picks the context switch and
// the spin hint. They are genuinely orthogonal -- Linux/x86-64 and Linux/AArch64 share every syscall
// in this file and differ only in one instruction -- so collapsing them into a single "platform"
// enum would force a fresh copy of the POSIX layer for each new chip. CMake makes the same split:
// src/posix/ is the OS layer, src/posix/<arch>/ is the ABI layer.
#if defined(_WIN32)
    #define JLIB_PLATFORM_WINDOWS 1
#elif defined(__linux__)      // also true on Android: bionic differs in details, not in kind
    #define JLIB_PLATFORM_POSIX 1
#else
    #error "JLib::Scheduler supports Windows, Linux and Android. See platform.h."
#endif

#if defined(__x86_64__) || defined(_M_X64)
    #define JLIB_ARCH_X86_64 1
#elif defined(__aarch64__) || defined(_M_ARM64)
    #define JLIB_ARCH_AARCH64 1
#else
    // Not a compiler flag away: adding an arch means writing src/posix/<arch>/ContextSwitch.S and a
    // matching Fiber::Init, then teaching CpuRelax() below the local spin hint.
    #error "JLib::Scheduler supports x86-64 and AArch64: the fiber context switch is hand-written assembly."
#endif

// ------------------------------------------------------------------------------------------------
#if JLIB_PLATFORM_WINDOWS

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#define NOGDI              // Suppress GDI macros like Rectangle and DrawText
#define NOUSER             // Suppress User macros like CloseWindow, ShowCursor
#include <windows.h>
#undef NOGDI
#undef NOUSER

using affinity_mask_t = DWORD_PTR;
using native_handle_t = HANDLE;

#else  // ------------------------------------------------------------------- POSIX

#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <unistd.h>
#if JLIB_ARCH_X86_64
#include <xmmintrin.h>   // _mm_pause -- on Windows this arrives via windows.h/intrin.h
#endif

// cpu_set_t is the direct analogue of Windows' affinity mask, and pthread_t of a HANDLE. Naming
// them the same way keeps Thread.h and the scheduler's members platform-free.
using affinity_mask_t = cpu_set_t;
using native_handle_t = pthread_t;

#endif

// ================================ VIRTUAL MEMORY PRIMITIVES ====================================
// Three operations, wrapped so FiberStackArena can be written once. The arena reserves a large
// region with NO access, then commits the parts it hands out and leaves the lowest page of each
// stack unbacked as a guard -- an overflow faults at the instruction that caused it instead of
// silently corrupting the next fiber's stack.
namespace JLib { namespace platform {

// Reserve address space without backing it. Returns nullptr on failure.
inline void* ReserveNoAccess(std::size_t bytes) {
#if JLIB_PLATFORM_WINDOWS
    return ::VirtualAlloc(nullptr, bytes, MEM_RESERVE, PAGE_NOACCESS);
#else
    // PROT_NONE + MAP_NORESERVE is the mmap equivalent of a Windows RESERVE: address space is
    // taken, nothing is committed, and touching it faults. MAP_NORESERVE matters on Linux because
    // without it a large mapping can be charged against the overcommit limit up front.
    void* p = ::mmap(nullptr, bytes, PROT_NONE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    return (p == MAP_FAILED) ? nullptr : p;
#endif
}

// Make an already-reserved range readable/writable. Returns false on failure.
inline bool CommitReadWrite(void* addr, std::size_t bytes) {
#if JLIB_PLATFORM_WINDOWS
    return ::VirtualAlloc(addr, bytes, MEM_COMMIT, PAGE_READWRITE) != nullptr;
#else
    return ::mprotect(addr, bytes, PROT_READ | PROT_WRITE) == 0;
#endif
}

// Release an entire reservation obtained from ReserveNoAccess.
inline void ReleaseReservation(void* addr, std::size_t bytes) {
#if JLIB_PLATFORM_WINDOWS
    (void)bytes;                       // MEM_RELEASE requires a size of 0
    ::VirtualFree(addr, 0, MEM_RELEASE);
#else
    ::munmap(addr, bytes);             // munmap, unlike VirtualFree, DOES want the length
#endif
}

// ==================================== SPIN-WAIT HINT ===========================================
// Tell the core "I am in a spin loop, this is not useful work". Every one of the scheduler's
// test_and_set loops goes through here. It is not a yield to the OS and never blocks -- it is a
// hint to the pipeline, so the cost of getting it wrong is throughput, not correctness.
//
// This is the ONLY thing in the POSIX layer that is genuinely per-arch rather than per-OS, which is
// why it is a wrapped primitive and not seventeen #ifdefs at the call sites.
//   x86-64: PAUSE -- de-pipelines the loop, avoids the memory-order-violation flush on exit, and on
//           SMT hardware hands the physical core's issue slots to the sibling thread.
//   AArch64: YIELD -- the architectural equivalent hint. On many implementations it retires as a
//           NOP, but on SMT-capable ARM cores it does the same sibling handoff, and it is the
//           instruction the architecture designates for this. (Some spin implementations prefer
//           'isb' for a longer stall; that is a tuning choice to MEASURE on real hardware, not to
//           assume -- and it is a heavier hammer than PAUSE, so it is not the like-for-like port.)
inline void CpuRelax() {
#if JLIB_ARCH_X86_64
    _mm_pause();
#else
    __asm__ __volatile__("yield" ::: "memory");
#endif
}

// Which logical CPU the calling thread is on RIGHT NOW. Inherently a hint: the answer can be stale
// before it is returned unless the thread is pinned. Used only to index the P/E class table when
// choosing work, where being occasionally wrong costs a slightly worse routing decision and
// nothing else.
inline unsigned CurrentCpu() {
#if JLIB_PLATFORM_WINDOWS
    return ::GetCurrentProcessorNumber();
#else
    const int c = ::sched_getcpu();      // -1 only if the syscall is unavailable
    return (c < 0) ? 0u : (unsigned)c;
#endif
}

inline std::size_t PageSize() {
#if JLIB_PLATFORM_WINDOWS
    return 4096;                       // x64 Windows normal page; large pages are not used here
#else
    return (std::size_t)::sysconf(_SC_PAGESIZE);
#endif
}

}} // namespace JLib::platform
