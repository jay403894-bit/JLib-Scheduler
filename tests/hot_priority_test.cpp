// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE PRIORITY RATCHET: does an elevated worker stand back down when K sheds?
//
// HotThreadPolicy::Elevated puts THREAD_PRIORITY_TIME_CRITICAL on hot workers. The raise used to be
// permanent, which was correct while the hot set was STATIC -- only ever K workers could be raised
// and K never moved. Dynamic K breaks that: K ramps to maxK, every worker that was ever hot keeps
// the priority, K sheds back to minK, and the priority does not shed with it.
//
// WHY IT MATTERS AND IS NOT COSMETIC. Under the default Sleep policy a demoted worker PARKS, so
// sitting at 15 costs nothing. Under NoSleep it SPINS at 15 -- which is precisely the "N spinning
// threads preempt the completion thread feeding them" configuration the scheduler records as 5x
// worse. NoSleep is not a corner case; it is what a server build would pick.
//
// COUNTED, NOT TIMED. How many threads sit at TIME_CRITICAL after a ramp-and-shed is a FACT: it has
// no variance and needs no noise floor, unlike the latency question it would otherwise be inferred
// from. That is the whole reason this test is cheap enough to be worth having.

#define NOMINMAX
#include <TaskScheduler.h>
#include <windows.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-68s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

// Every worker reports its OWN priority, which is the only way to read it: GetThreadPriority needs a
// handle, and a worker's handle is not exposed. A task per worker, pinned by affinity so each lands
// on a distinct one.
static std::atomic<int> g_prio[64];

static int CountCritical(JLib::TaskScheduler& s, size_t n) {
    for (size_t i = 0; i < 64; ++i) g_prio[i].store(-9999, std::memory_order_relaxed);
    JLib::WaitGroup wg;
    wg.n.store((int)n, std::memory_order_relaxed);
    for (size_t i = 0; i < n; ++i) {
        // hiPri PROBES, and that is the whole trick. An ORDINARY task cannot observe this: the
        // per-task path drops a hot worker to NORMAL before running non-lane work, so every probe
        // would read NORMAL whether or not the ratchet exists. A LANE task asks the real question --
        // "would this worker take a completion at elevated priority right now" -- which is exactly
        // what the capability flag gates.
        auto* t = s.CreateTask([i]() {
            g_prio[i].store(::GetThreadPriority(::GetCurrentThread()), std::memory_order_relaxed);
        }, /*hipri*/ 1);
        if (t) { t->waitGroup = &wg; JLib::Task* arr[1] = { t }; s.PushBatch(arr, 1, (uint8_t)(i + 1), 1024, true); }
        else   { wg.n.fetch_sub(1, std::memory_order_relaxed); }
    }
    s.WaitFor(wg);
    int c = 0;
    for (size_t i = 0; i < n; ++i)
        if (g_prio[i].load() == THREAD_PRIORITY_TIME_CRITICAL) ++c;
    return c;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("hot-thread priority ratchet\n");

    JLib::TaskScheduler::SetHotThreadPolicy(JLib::TaskScheduler::HotThreadPolicy::Elevated);
    JLib::TaskScheduler::Init(0);
    auto& s = JLib::TaskScheduler::Instance();
    const size_t n = s.GetWorkerCount();
    if (n < 8) { std::printf("  pool too small; skipping\n"); return 0; }
    const size_t probe = (n < 16) ? n : 16;

    // Baseline: nothing hot, so nothing elevated.
    JLib::TaskScheduler::SetHotWorkers(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    Check(CountCritical(s, probe) == 0, "with K=0, no worker is at TIME_CRITICAL");

    // Ramp by hand to the same place dynamic K would reach, then shed. Using SetHotWorkers directly
    // rather than driving the controller keeps this about the PRIORITY, not about whether the
    // controller happens to ramp on this machine -- one property per test.
    JLib::TaskScheduler::SetHotWorkers(4);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const int atFour = CountCritical(s, probe);
    std::printf("      at K=4: %d workers at TIME_CRITICAL\n", atFour);
    Check(atFour > 0, "raising K elevates the hot workers");

    JLib::TaskScheduler::SetHotWorkers(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const int afterShed = CountCritical(s, probe);
    std::printf("      after shedding to K=1: %d still at TIME_CRITICAL\n", afterShed);
    // THE ASSERTION. Before the fix this stayed at whatever the peak was: the raise was one-way.
    Check(afterShed < atFour, "workers STAND DOWN when K sheds -- the priority is not a ratchet");

    JLib::TaskScheduler::SetHotWorkers(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const int atZero = CountCritical(s, probe);
    std::printf("      back at K=0: %d still at TIME_CRITICAL\n", atZero);
    Check(atZero == 0, "and all the way back down at K=0");

    s.Join();
    std::printf("\n%s -- %d failure(s)\n", failures ? "FAILED" : "ALL CHECKS PASSED", failures);
    return failures ? 1 : 0;
}
