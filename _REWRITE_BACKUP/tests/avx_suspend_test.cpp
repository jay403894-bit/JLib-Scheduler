// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES 256-BIT VECTOR STATE SURVIVE A FIBER SUSPEND?
//
// THE QUESTION. ContextSwitch.asm saves XMM6-XMM15 -- the LOW 128 bits -- and nothing above them.
// The Win64 ABI says that is complete: the low halves of XMM6-15 are non-volatile, and the UPPER
// halves of YMM0-15 are volatile, meaning any callee may destroy them. The same is true of every
// AVX-512 register and every mask register.
//
// THE ARGUMENT FOR WHY THAT IS SAFE, which is what this file exists to stop trusting on its own:
// the context switch is an opaque function call (it lives in a .asm file, so no compiler can see
// through it), and a call clobbers volatile registers. So a compiler MUST spill anything live in a
// YMM upper half before the call and reload it after -- and the spill lands on the FIBER'S OWN
// STACK, which travels with the fiber. Resuming on a different thread reloads from the same stack.
//
// THE FAILURE MODE IF THAT IS WRONG IS SILENT. Not a crash: the upper 128 bits of a vector come back
// as whatever the resuming thread last had in them, so the arithmetic simply produces wrong numbers,
// in a game engine, in code nobody would think to suspect. That is worth a test rather than a
// paragraph of reasoning.
//
// HOW IT IS MADE ADVERSARIAL. A suspend that nothing interferes with proves little -- the register
// might survive by luck because nothing else touched it. So while the subject fibers are parked,
// OTHER fibers spin doing 256-bit arithmetic with a DIFFERENT pattern, on every worker. If the upper
// halves were not preserved across the suspend, they come back holding the interference pattern.
//
// WHAT THIS DOES NOT COVER, stated so the pass is not read as broader than it is: hand-written
// assembly or inline asm that deliberately keeps a YMM register live ACROSS a call, without telling
// the compiler. That violates the ABI on its own terms and would break against any function call,
// fiber or not -- it is not something a context switch can rescue.

#include "TaskScheduler.h"
#include "Thread.h"
#include "platform.h"

#include <atomic>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

// AVX intrinsics. MSVC exposes everything through <intrin.h>; GCC/Clang want <immintrin.h>.
#if defined(_MSC_VER)
  #include <intrin.h>
#else
  #include <immintrin.h>
#endif

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {

constexpr int kSubjects   = 8;      // fibers that hold a pattern across a suspend
constexpr int kInterferes = 8;      // fibers that stomp YMM while the subjects are parked

std::atomic<int> g_armed{ 0 };      // subjects that have loaded their pattern and are about to park
std::atomic<int> g_verified{ 0 };   // subjects whose pattern came back intact
std::atomic<int> g_corrupted{ 0 };
std::atomic<bool> g_release{ false };
std::atomic<bool> g_interfere{ true };

// Distinct per subject so a mix-up between two subjects is also caught, not just a stomp.
// The values live in the UPPER 128 bits as well as the lower, which is the whole point: a switch
// that saved only XMM would return the low half correct and the high half garbage.
alignas(32) double g_pattern[kSubjects][4];
alignas(32) double g_readback[kSubjects][4];

void SubjectBody(void* p) {
    const int id = static_cast<int>(reinterpret_cast<std::uintptr_t>(p));

    // Load a 256-bit value. From here to the verify below, this variable is what must survive.
    __m256d v = _mm256_load_pd(g_pattern[id]);

    // Touch it so the compiler cannot fold the load into the verify and skip the interval.
    v = _mm256_add_pd(v, _mm256_set1_pd(0.0));

    g_armed.fetch_add(1, std::memory_order_release);

    // THE SUSPEND. A fiber-aware wait, so this genuinely parks the fiber and frees the worker --
    // the resume may land on a different thread entirely, which is the case that matters.
    while (!g_release.load(std::memory_order_acquire))
        JLib::Thread::CoYield();

    // And read it back. If the upper 128 bits were not preserved, they now hold whatever the
    // resuming thread's interference left there.
    _mm256_store_pd(g_readback[id], v);

    const bool intact = std::memcmp(g_readback[id], g_pattern[id], sizeof g_pattern[id]) == 0;
    if (intact) g_verified.fetch_add(1, std::memory_order_relaxed);
    else        g_corrupted.fetch_add(1, std::memory_order_relaxed);
}

// Runs 256-bit arithmetic continuously with a pattern that is NOT any subject's, so a subject whose
// upper half was clobbered comes back holding something recognisably foreign.
void InterferenceBody(void*) {
    __m256d junk = _mm256_set_pd(-1.5, -2.5, -3.5, -4.5);
    const __m256d step = _mm256_set1_pd(1.0);
    while (g_interfere.load(std::memory_order_acquire)) {
        for (int i = 0; i < 64; ++i) junk = _mm256_add_pd(junk, step);
        JLib::Thread::CoYield();
    }
    // Keep it observable so none of the above is optimised away.
    alignas(32) double sink[4];
    _mm256_store_pd(sink, junk);
    if (sink[0] == 12345.678) std::printf("");
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("AVX state across a fiber suspend -- workers=%zu\n\n", sched.GetWorkerCount());

    for (int i = 0; i < kSubjects; ++i)
        for (int j = 0; j < 4; ++j)
            g_pattern[i][j] = 1000.0 + i * 10.0 + j;   // distinct in every lane, including the upper two

    JLib::WaitGroup subjects, noise;
    subjects.n.store(kSubjects, std::memory_order_relaxed);
    noise.n.store(kInterferes, std::memory_order_relaxed);

    for (int i = 0; i < kInterferes; ++i) {
        JLib::Task* t = sched.CreateTask(InterferenceBody, nullptr, false,
                                         JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { noise.n.fetch_sub(1, std::memory_order_release); continue; }
        t->waitGroup = &noise;
        sched.Push(t);
    }

    for (int i = 0; i < kSubjects; ++i) {
        JLib::Task* t = sched.CreateTask(SubjectBody,
                                         reinterpret_cast<void*>(static_cast<std::uintptr_t>(i)),
                                         false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { subjects.n.fetch_sub(1, std::memory_order_release); continue; }
        t->waitGroup = &subjects;
        sched.Push(t);
    }

    // Let every subject park WITH its pattern loaded, and let the interference run for a while in
    // that window -- the parked interval is the only place the corruption could happen.
    while (g_armed.load(std::memory_order_acquire) < kSubjects)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    g_release.store(true, std::memory_order_release);
    sched.WaitFor(subjects);
    g_interfere.store(false, std::memory_order_release);
    sched.WaitFor(noise);

    std::printf("      %d subjects: %d intact, %d corrupted\n",
                kSubjects, g_verified.load(), g_corrupted.load());

    Check(g_verified.load() == kSubjects,
          "256-bit vector state survived a suspend, a migration and AVX interference");
    Check(g_corrupted.load() == 0, "no subject came back holding another pattern's bits");

    if (g_corrupted.load() != 0) {
        std::printf("\n  THE SWITCH MUST SAVE YMM UPPER HALVES. The Win64 ABI says they are volatile,\n");
        std::printf("  so the compiler should have spilled them before the call -- if they did not\n");
        std::printf("  survive, that reasoning is wrong here and ContextSwitch.asm needs vmovdqu of\n");
        std::printf("  ymm6-ymm15 (plus a CPU feature check, since it cannot be assumed).\n");
    }

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    sched.Join();
    return g_fail == 0 ? 0 : 1;
}
