// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// EPOCH MECHANISM COSTS: the guard, and the reclamation scan.
//
// THIS WAS A SLOTS-vs-COUNTED COMPARISON and counted epochs are gone, so the comparison is gone with
// them. What survives is the pair of absolute numbers, which is what any change to reclamation has
// to be read against:
//
//   1. GUARDS PER SECOND under contention -- the hot path. Two UNCONTENDED stores per guard,
//      because the reader owns its slot.
//   2. RECLAIM SCAN COST -- MinActiveEpoch is O(participants): every fiber slot AND every thread
//      slot, and the fiber pool is sized per core, so this grows with the machine.
//
// READ THEM AS A RATIO, NOT AS TWO FACTS. Guards fire on every traversal; reclamation fires on a
// Tick. A hot-path regression is paid orders of magnitude more often than a scan saving is earned,
// and the same arithmetic decides whether moving the RETIRE path off its global queue is a win or a
// wash -- retire fires on a REMOVE, which is rarer still.
//
// REPETITIONS INTERLEAVE. Measuring one arm to completion and then another is how thermal drift
// becomes a result, which this project has already been caught by once.

#include "TaskScheduler.h"
#include "Epochs.h"
#include "Thread.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <vector>

namespace {

std::atomic<long long> g_guards{ 0 };
std::atomic<bool>      g_stop{ false };

// One worker's inner loop: take a guard, drop it, repeat. Deliberately empty inside -- this is
// measuring the guard, not a traversal, and anything in the body would dilute exactly the
// difference under test.
void HammerSlots() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        for (int i = 0; i < 64; ++i) { SlotEpochGuard g(JLib::CurrentEpochSlot()); }
        n += 64;
    }
    g_guards.fetch_add(n, std::memory_order_relaxed);
}


double GuardsPerSec(void (*fn)(), int workers, int ms) {
    g_guards.store(0); g_stop.store(false);
    auto& sched = JLib::TaskScheduler::Instance();
    for (int i = 0; i < workers; ++i) {
        if (auto* t = sched.CreateTask([fn] { fn(); })) sched.Push(t);
    }
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    g_stop.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));   // let them drain and add in
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    return g_guards.load() / secs;
}

double Median(std::vector<double> v) { std::sort(v.begin(), v.end()); return v[v.size() / 2]; }

} // namespace

int main(int argc, char** argv) {
    const int reps    = (argc > 1) ? std::atoi(argv[1]) : 5;
    const int ms      = (argc > 2) ? std::atoi(argv[2]) : 200;

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& em    = JLib::EpochManager::Instance();
    const int workers = (int)sched.GetWorkerCount();

    std::printf("epoch mechanisms -- workers=%d, %d ms per arm, %d interleaved reps\n\n",
                workers, ms, reps);

    // ---- 1. the hot path ------------------------------------------------------------------------
    std::vector<double> slots;
    for (int r = 0; r < reps; ++r) slots.push_back(GuardsPerSec(&HammerSlots, workers, ms));
    const double s = Median(slots);
    std::printf("  guards/sec, %d threads hammering: %12.0f\n", workers, s);

    // ---- 2. the cold path -----------------------------------------------------------------------
    //
    // THE REASON THIS FILE SURVIVED THE COUNTED-EPOCH REMOVAL. The comparison it was built for is
    // gone, but this number is not: MinActiveEpoch is O(participants) -- every fiber slot and every
    // thread slot -- and the fiber pool is sized per core, so the scan grows with the machine. It is
    // the cost that any change to the retire path has to be read against.
    {
        constexpr int kCalls = 20000;
        const auto t0 = std::chrono::steady_clock::now();
        volatile size_t sink = 0;
        for (int i = 0; i < kCalls; ++i) sink += em.MinActiveEpoch();
        (void)sink;
        const double us = std::chrono::duration<double, std::micro>(
            std::chrono::steady_clock::now() - t0).count() / kCalls;
        std::printf("\n  MinActiveEpoch: %.3f us per call over %d participant slots\n",
                    us, em.ParticipantCount());
    }

    std::printf("\n  READ THEM TOGETHER. Guards outnumber reclaims by a wide margin in every\n"
                "  workload here -- TaskDAG takes a guard per node walk and reclaims on Tick --\n"
                "  so a hot-path regression is paid far more often than a scan saving is.\n"
                "  That ratio is also what decides whether moving the RETIRE path off the global\n"
                "  queue is a win or a wash: retire fires on a remove, guards fire on a traversal.\n");

    JLib::detail::TeardownForTesting(sched);
    return 0;
}
