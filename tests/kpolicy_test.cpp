// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// KPolicy: the controller reads different evidence for a deadline lane than for a throughput one.
//
// THE WHOLE TEST IS A PAIRED COMPARISON, and it has to be. "WaitTime promoted" on its own proves
// nothing -- K could have moved for any reason, or the workload could be one that QueueLoad handles
// perfectly well. What is being asserted is a DIFFERENCE: the same workload, the same range, the
// same duration, promotes under one policy and provably does not under the other. If QueueLoad ever
// starts promoting here, this test fails, and it should -- the premise would be gone.
//
// The workload is the one measured to defeat QueueLoad: a low arrival rate where each arrival waits
// behind ORDINARY work rather than behind another lane task. That is the case every volume-based
// signal is blind to -- the lane queue never gets deep, occupancy stays near zero, and two arrivals
// never coincide, yet arrivals are late. See the KPolicy comment in TaskScheduler.h.

#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int  g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-64s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

namespace {

std::atomic<bool> g_stop{ false };

// Saturating ordinary work. This is what a lane arrival ends up queued behind: the hot worker is
// not reserved, so between completions it runs pool work like any other worker.
void LoadThread(TaskScheduler& sched) {
    while (!g_stop.load(std::memory_order_relaxed)) {
        sched.ParallelFor(0, 64, 1, [](int lo, int hi) {
            for (int i = lo; i < hi; ++i) {
                volatile double acc = 0;
                for (int k = 0; k < 200000; ++k) acc += (double)k * 1.000001;
            }
        });
    }
}

// Returns the highest K observed while pushing lane tasks at `rateHz` for `ms`.
size_t RunAndWatchK(TaskScheduler& sched, int ms, int rateHz) {
    size_t peak = TaskScheduler::GetHotWorkers();

    const auto  start    = std::chrono::steady_clock::now();
    const auto  deadline = start + std::chrono::milliseconds(ms);
    const auto  period   = std::chrono::microseconds(1000000 / rateHz);
    auto        next     = start + period;

    while (std::chrono::steady_clock::now() < deadline) {
        // hiPri = 1: this is a lane task. Tiny body -- microseconds -- which is exactly the shape
        // that makes occupancy useless as a signal.
        auto* t = sched.CreateTask([]() {
            volatile double acc = 0;
            for (int k = 0; k < 200; ++k) acc += (double)k;
        }, /*hipri*/ 1);
        if (t) sched.Push(t);

        std::this_thread::sleep_until(next);
        next += period;

        const size_t k = TaskScheduler::GetHotWorkers();
        if (k > peak) peak = k;
    }
    return peak;
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    TaskScheduler::SetHotWorkerRange(1, 3);
    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();

    std::printf("KPolicy -- workers=%zu\n\n", sched.GetWorkerCount());

    Check(TaskScheduler::GetHotWorkerPolicy() == TaskScheduler::KPolicy::QueueLoad,
          "QueueLoad is the default, so existing users are unaffected");

    // A target far above anything this workload produces, then far below: the setter must be the
    // thing that decides, not some hidden constant.
    TaskScheduler::SetLaneWaitTargetNs(50000);
    Check(TaskScheduler::GetLaneWaitTargetNs() == 50000, "the wait target round-trips");
    TaskScheduler::SetLaneWaitTargetNs(0);
    Check(TaskScheduler::GetLaneWaitTargetNs() >= 1000,
          "a zero target is clamped -- it would promote forever and never shed");

    std::thread load(LoadThread, std::ref(sched));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));   // let the pool saturate

    // ---- the control: QueueLoad on a latency-bound lane -----------------------------------------
    //
    // MUST NOT PROMOTE. Every signal it has measures volume, and at this rate there is none: the
    // lane queue never gets deep and occupancy is a rounding error. If this assertion ever passes
    // by promoting, the paired assertion below stops meaning anything.
    TaskScheduler::SetHotWorkerPolicy(TaskScheduler::KPolicy::QueueLoad);
    TaskScheduler::SetHotWorkersEffective(1);
    const size_t peakQueueLoad = RunAndWatchK(sched, 1500, 60);
    Check(peakQueueLoad == 1,
          "QueueLoad does NOT promote on a latency-bound lane (the negative control)");

    // ---- the subject: WaitTime on the identical workload -----------------------------------------
    TaskScheduler::SetHotWorkerPolicy(TaskScheduler::KPolicy::WaitTime);
    // RESTORED EXPLICITLY -- the clamp check above left it at the 1 us floor, and inheriting that
    // here would be a test configuring itself wrong by accident.
    //
    // THE TARGET MUST SIT ABOVE THE POOL'S WAKE LATENCY, measured at a ~90 us floor. Set it below
    // that and every arrival on an otherwise idle pool looks late, so K climbs to max and stays --
    // observed directly: at a 50 us target this trace read "3 3 3 3 3 ..." with NO load running at
    // all. That is a mis-set budget, not a controller fault, and it is the first thing to check if
    // WaitTime ever appears to ratchet.
    TaskScheduler::SetLaneWaitTargetNs(250000);   // the shipped default
    TaskScheduler::SetHotWorkersEffective(1);
    const size_t peakWaitTime = RunAndWatchK(sched, 1500, 60);
    Check(peakWaitTime > 1,
          "WaitTime DOES promote on the same workload (the paired result)");

    std::printf("\n  peak K: QueueLoad=%zu  WaitTime=%zu\n", peakQueueLoad, peakWaitTime);

    // ---- and it must come back down ---------------------------------------------------------------
    //
    // A promote path with no matching shed is the K ratchet, which this codebase has grown three
    // separate times. Stopping the load removes the thing arrivals were queueing behind, so waits
    // collapse and the core is surplus.
    g_stop.store(true, std::memory_order_relaxed);
    load.join();

    // Traced rather than sampled once at the end: "it did not shed" and "it shed and was promoted
    // straight back" are different failures with different fixes, and a single final reading cannot
    // tell them apart.
    std::printf("\n  K trace after the load stops (lane still ticking at 60 Hz):\n   ");
    size_t minSeen = TaskScheduler::GetHotWorkers();
    for (int i = 0; i < 24; ++i) {
        RunAndWatchK(sched, 100, 60);
        const size_t k = TaskScheduler::GetHotWorkers();
        if (k < minSeen) minSeen = k;
        std::printf(" %zu", k);
    }
    std::printf("\n");

    Check(minSeen < peakWaitTime || peakWaitTime == 1,
          "WaitTime sheds once arrivals stop waiting (no ratchet)");

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
