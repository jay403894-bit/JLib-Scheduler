// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES THE WORKER LOOP RUN AT ALL?
//
// SCOPE, AND IT IS DELIBERATELY TINY. This asks one question and refuses to ask any other: with the
// blocking parks deleted, fibers pinned to their workers, and a notify that is a direct fiber
// resume, does the pool still pick tasks up and run them.
//
// WHAT IT WILL NOT TOUCH, because the refactor has not reached those parts yet and a test that
// fails on them tells you nothing about the question above:
//
//   Join / teardown  -- Join() stops workers ONE AT A TIME and predates all of this. A frame
//                       resumed into a departing worker's queue is stranded, and the resumed queue
//                       has no rescue path by design. That is a real problem and it is NOT this
//                       file's problem: the probe calls _Exit and never tears down.
//   WaitFor          -- a wait is a different mechanism from a suspend. Main polls an atomic here
//                       rather than waiting on anything, so nothing in the result can be blamed on
//                       the waiting path.
//   correctness of pinning -- tests/fiber_pinning_test.cpp already measures that separately.
//
// SO A PASS MEANS EXACTLY ONE THING: tasks submitted to the pool got dispatched to workers, ran to
// completion, and the pool kept going for several rounds. A failure means the loop itself is broken
// and every other test's result is noise.
//
// ROUNDS RATHER THAN ONE BATCH, because the interesting failure is not "nothing ever ran" -- it is
// a pool that runs the first batch while every worker is still awake and then stalls once workers
// have gone through their idle path at least once. One batch cannot see that; several can.

#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

namespace {

constexpr int kRounds        = 8;
constexpr int kTasksPerRound = 200;

std::atomic<int> g_ran{ 0 };

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("worker loop probe -- workers=%zu, %d rounds x %d tasks\n\n",
                sched.GetWorkerCount(), kRounds, kTasksPerRound);

    int failed = 0;

    for (int r = 0; r < kRounds; ++r) {
        const int before = g_ran.load(std::memory_order_acquire);

        for (int i = 0; i < kTasksPerRound; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {
                g_ran.fetch_add(1, std::memory_order_release);
            }, nullptr, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            if (!t) { std::printf("  round %d: CreateTask returned null\n", r); ++failed; break; }
            sched.Push(t);
        }

        // POLL, do not wait. Using WaitFor here would put the waiting path inside the measurement
        // and a hang could not be attributed. A deadline turns a stall into a report instead of a
        // hung process.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        const int target = before + kTasksPerRound;
        while (g_ran.load(std::memory_order_acquire) < target
               && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }

        const int now = g_ran.load(std::memory_order_acquire);
        if (now < target) {
            std::printf("  round %-2d STALLED: %d of %d ran\n", r, now - before, kTasksPerRound);
            sched.DumpPoolState("worker_loop_probe stall");
            ++failed;
            break;
        }
        std::printf("  round %-2d ok: %d ran (total %d)\n", r, now - before, now);

        // Let every worker go through its idle path before the next round, so round r+1 is
        // dispatched into a pool that has already been idle once. That is the state a broken
        // idle/notify path shows up in, and the first round cannot reach it.
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }

    std::printf("\n%s -- %d of %d tasks ran\n",
                failed == 0 ? "WORKER LOOP RUNS" : "WORKER LOOP IS BROKEN",
                g_ran.load(std::memory_order_acquire), kRounds * kTasksPerRound);

    // _Exit, NOT Join and not a normal return. Teardown is unbuilt for this refactor and would
    // report its own unrelated failure on top of this one -- which is exactly the confusion this
    // file exists to avoid. Nothing here owns a resource the OS will not reclaim.
    std::fflush(stdout);
    std::_Exit(failed == 0 ? 0 : 1);
}
