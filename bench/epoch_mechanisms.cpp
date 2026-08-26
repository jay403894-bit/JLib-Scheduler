// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// SLOTS vs COUNTED EPOCHS: the measurement that decides whether counted epochs should REPLACE the
// participant scan or merely sit beside it for coroutines.
//
// THE TRADE IS NOT OBVIOUS IN EITHER DIRECTION, which is the whole reason this exists:
//
//   slots     guard: two UNCONTENDED stores (the reader owns its slot)
//             reclaim: O(participants) -- every fiber slot and every thread slot, and the fiber
//             pool is sized per core, so this grows with the machine
//
//   counted   guard: two RMWs on a line EVERY reader in that epoch shares
//             reclaim: O(kEpochSlots) -- eight loads, regardless of machine size
//
// So replacing slots with counters moves cost from a cold, amortised, batched path onto EVERY
// lock-free operation. Which way that nets out depends entirely on the guard-to-reclaim ratio, and
// that is an empirical question about the workload rather than a design argument.
//
// TWO NUMBERS, because they can point opposite ways and the conclusion needs both:
//
//   1. GUARDS PER SECOND under contention -- the hot path, and the one counted epochs can lose
//      badly, since a shared counter is exactly the cache line a per-reader slot exists to avoid.
//   2. RECLAIM SCAN COST -- the cold path, and the one slots lose, since the participant list is
//      thousands of entries on a real pool.
//
// ARMS INTERLEAVE per repetition. Measuring one mechanism to completion and then the other is how a
// thermal drift becomes a result, which this project has already been caught by once.

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

void HammerCounted() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        for (int i = 0; i < 64; ++i) { CountedEpochGuard g; }
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
    std::vector<double> slots, counted;
    for (int r = 0; r < reps; ++r) {
        slots.push_back(GuardsPerSec(&HammerSlots, workers, ms));
        counted.push_back(GuardsPerSec(&HammerCounted, workers, ms));
    }
    const double s = Median(slots), c = Median(counted);
    std::printf("  guards/sec, %d threads hammering:\n", workers);
    std::printf("    slots    %12.0f\n", s);
    std::printf("    counted  %12.0f   (%.2fx %s)\n", c, (c > s ? c / s : s / c),
                c > s ? "faster" : "SLOWER");

    // ---- 2. the cold path -----------------------------------------------------------------------
    //
    // MinActiveEpoch scans participants AND the ring, so this times both together -- the point is
    // the absolute cost against how often it runs, not a clean split. The participant count is
    // printed because it is what a counted-only scheme would delete.
    {
        constexpr int kCalls = 20000;
        const auto t0 = std::chrono::steady_clock::now();
        volatile size_t sink = 0;
        for (int i = 0; i < kCalls; ++i) sink += em.MinActiveEpoch();
        (void)sink;
        const double us = std::chrono::duration<double, std::micro>(
            std::chrono::steady_clock::now() - t0).count() / kCalls;
        std::printf("\n  MinActiveEpoch: %.3f us per call (%d participant slots + %zu ring slots)\n",
                    us, em.ParticipantCount(), JLib::EpochManager::kEpochSlots);
        std::printf("    a counted-only scheme would scan %zu instead of %d.\n",
                    JLib::EpochManager::kEpochSlots, em.ParticipantCount());
    }

    std::printf("\n  READ THEM TOGETHER. Guards outnumber reclaims by a wide margin in every\n"
                "  workload here -- TaskDAG takes a guard per node walk and reclaims on Tick --\n"
                "  so a hot-path regression is paid far more often than the scan saving is.\n");

    sched.Join();
    return 0;
}
