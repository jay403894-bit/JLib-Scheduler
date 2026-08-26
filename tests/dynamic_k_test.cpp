// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DYNAMIC K, and specifically the one property the design rests on:
//
//     LOWERING K MUST NOT STRAND LANE WORK.
//
// The argument for why it cannot is short and it is convincing, which is exactly the problem. It
// goes: the hiPri deque's only writer is the owner's own inbox drain; that drain is gated on
// `servesHiPri || hiPriStray`; hiPriStray is true whenever either lane structure is non-empty; and
// the sleep predicate independently covers the inbox. So a demoted worker either finds its leftover
// or is forbidden from parking. Airtight.
//
// THE SAME KIND OF ARGUMENT ABOUT THE SAME CODE WAS WRONG THREE TIMES IN ONE DAY. The K=0 hiPri
// deadlock was reasoned about across three sittings -- PickNextWorker's fallback, the yield path,
// stranded hiPri deques, all named as the cause and all disproven -- while the actual fault was a
// brace putting the loPri drain inside the hiPri gate. Static analysis of this exact block has a
// losing record. So the property gets asserted, not argued.
//
// WHAT WOULD FAIL HERE IF IT WERE WRONG: tasks pushed at a worker that is then demoted simply never
// run, and the test times out rather than reporting a wrong number. That is the honest failure shape
// for a stranding bug -- there is nothing to read, which is precisely why it needs a deadline.

#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-72s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

template <typename F>
static bool WaitUntil(F pred, int budgetMs = 5000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

static std::atomic<int> g_ran{ 0 };

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(0);
    auto& s = JLib::TaskScheduler::Instance();
    const size_t n = s.GetWorkerCount();
    std::printf("dynamic K -- workers=%zu\n\n", n);

    if (n < 4) {
        std::printf("  pool too small to demote meaningfully; skipping\n");
        s.Join();
        return 0;
    }

    // ============================================================================================
    std::printf("demotion does not strand lane work\n");
    {
        // Aim the work at workers that are about to STOP being hot. Explicit affinity, so this does
        // not depend on how steering happens to spread -- the task is placed where the test needs it.
        constexpr int kPer = 64;
        JLib::TaskScheduler::SetHotWorkers(4);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));   // let the pool notice
        g_ran.store(0);

        int pushed = 0;
        for (uint8_t w = 2; w <= 4; ++w) {          // affinity is 1-based: workers 1, 2, 3
            std::vector<JLib::Task*> batch;
            for (int i = 0; i < kPer; ++i)
                if (auto* t = s.CreateTask([](void*) { g_ran.fetch_add(1, std::memory_order_relaxed); },
                                           nullptr, /*hipri*/ 1))
                    batch.push_back(t);
            if (!batch.empty()) {
                s.PushBatch(batch.data(), batch.size(), w, /*minPerSegment*/ 1024, /*hiPri*/ true);
                pushed += (int)batch.size();
            }
        }

        // DEMOTE IMMEDIATELY, with the work still in flight. No sleep first: the interesting window
        // is the one where the batch is sitting in an inbox that is about to belong to a worker
        // which no longer serves the lane.
        JLib::TaskScheduler::SetHotWorkers(1);

        const bool all = WaitUntil([&] { return g_ran.load() == pushed; }, 10000);
        std::printf("      pushed=%d ran=%d\n", pushed, g_ran.load());
        Check(all, "every lane task runs after its worker is demoted mid-flight");
    }

    // ============================================================================================
    // REPEATED transitions under continuous load. One demotion exercises one interleaving; dynamic K
    // produces a stream of them, and the failure being hunted is a race, so it needs many tries.
    std::printf("\nrepeated promote/demote under continuous load\n");
    {
        constexpr int kRounds = 40;
        constexpr int kPer    = 24;
        g_ran.store(0);
        int pushed = 0;

        for (int r = 0; r < kRounds; ++r) {
            const size_t k = (r % 2) ? 1u : 4u;
            JLib::TaskScheduler::SetHotWorkers(k);
            for (uint8_t w = 2; w <= 4; ++w) {
                std::vector<JLib::Task*> batch;
                for (int i = 0; i < kPer; ++i)
                    if (auto* t = s.CreateTask([](void*) { g_ran.fetch_add(1, std::memory_order_relaxed); },
                                               nullptr, /*hipri*/ 1))
                        batch.push_back(t);
                if (!batch.empty()) {
                    s.PushBatch(batch.data(), batch.size(), w, 1024, true);
                    pushed += (int)batch.size();
                }
            }
        }
        const bool all = WaitUntil([&] { return g_ran.load() == pushed; }, 15000);
        std::printf("      %d transitions, pushed=%d ran=%d\n", kRounds, pushed, g_ran.load());
        Check(all, "no task is lost across 40 K transitions under load");
    }

    // ============================================================================================
    // THE HAND-OFF HINT. A demoted worker holding leftover lane work must ADVERTISE it, or mode-4
    // helpers are not permitted to touch it and it is served only by the worker that just stopped
    // prioritising it. Asserted on the bitmap directly rather than inferred from latency.
    std::printf("\nlane work arriving at an ALREADY-demoted worker is advertised\n");
    {
        // THE ORDER IS THE WHOLE TEST: demote FIRST, push SECOND.
        //
        // The obvious arrangement -- push while the worker is hot, then demote -- proves nothing,
        // and it took two negative-control runs to see why. A hot worker with 512 queued tasks has
        // ALREADY set its own bit through the normal threshold path, and nothing clears it
        // afterwards (its threshold path stops running the moment it stops being hot). So the
        // leftover looks advertised whether or not the stray path exists, and that version of this
        // test passed with ForceLaneHint commented out. Twice.
        //
        // The real hole is a push that LANDS AFTER the demotion: the producer read hotN=4 and aimed
        // at worker 3, K dropped, worker 3 saw empty queues and set nothing, and only then did the
        // work arrive. Reproduced here deterministically by simply doing it in that order.
        JLib::TaskScheduler::SetHotWorkers(1);
        // The bit must be CLEAR before we start, or the assertion below is meaningless again.
        const bool clean = WaitUntil([&] {
            return (JLib::TaskScheduler::LaneBacklogMask() & (1ull << 3)) == 0;
        }, 3000);
        Check(clean, "precondition: worker 3's lane bit is clear before the push");
        g_ran.store(0);

        // NO "OR IT ALREADY FINISHED" ESCAPE HATCH. The first version of this check had one, and it
        // PASSED WITH ForceLaneHint COMMENTED OUT -- the work drained inside the observation window
        // and the hatch carried the test. That is the third vacuous test in this project's history
        // and the reason every new assertion here gets run against its own negative control before
        // it is believed.
        //
        // Enough work that one worker cannot possibly drain it while we look. 512 x 500us is
        // ~256 ms of serial work, against a 3 s observation budget. And helpers cannot shorten it --
        // after the demotion K=1 under the default Sleep policy, where mode 4 is inert because the
        // ordinary workers are parked.
        int pushed = 0;
        std::vector<JLib::Task*> batch;
        for (int i = 0; i < 512; ++i)
            if (auto* t = s.CreateTask([](void*) {
                    std::this_thread::sleep_for(std::chrono::microseconds(500));
                    g_ran.fetch_add(1, std::memory_order_relaxed);
                }, nullptr, 1))
                batch.push_back(t);
        if (!batch.empty()) { s.PushBatch(batch.data(), batch.size(), 4, 4096, true); pushed = (int)batch.size(); }

        // Worker 3 (affinity 4) is the demoted one. Require its bit, and require it while work is
        // genuinely still outstanding -- so the assertion cannot be satisfied by an empty queue.
        bool seen = false, sawItBusy = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
        while (std::chrono::steady_clock::now() < deadline && !seen) {
            if (JLib::TaskScheduler::LaneBacklogMask() & (1ull << 3)) {
                seen = true;
                sawItBusy = g_ran.load() < pushed;
            }
        }
        std::printf("      advertised=%d whileOutstanding=%d ran=%d/%d\n",
                    (int)seen, (int)sawItBusy, g_ran.load(), pushed);
        Check(seen,      "the demoted worker ADVERTISES its leftover lane queue");
        Check(sawItBusy, "and it did so while the work was still outstanding");
        Check(WaitUntil([&] { return g_ran.load() == pushed; }, 30000), "and all of it still runs");
    }

    // ============================================================================================
    std::printf("\nthe controller respects its bounds\n");
    {
        JLib::TaskScheduler::SetHotWorkers(1);
        JLib::TaskScheduler::SetHotWorkerRange(1, 3);
        size_t lo = 0, hi = 0;
        JLib::TaskScheduler::GetHotWorkerRange(lo, hi);
        Check(lo == 1 && hi == 3, "range round-trips");

        // minK is forced to 1 only when the range CAN MOVE: at K=0 the lane does not exist, so a
        // controller starting there has nothing to observe and could never ramp up.
        JLib::TaskScheduler::SetHotWorkerRange(0, 3);
        JLib::TaskScheduler::GetHotWorkerRange(lo, hi);
        Check(lo == 1, "a MOVABLE range is clamped up to minK=1 -- K=0 is absorbing under scaling");

        // ... but (0,0) is a legal FIXED point, and it is the pre-existing default.
        JLib::TaskScheduler::SetHotWorkerRange(0, 0);
        JLib::TaskScheduler::GetHotWorkerRange(lo, hi);
        Check(lo == 0 && hi == 0, "(0,0) stays (0,0) -- a fixed K=0 is legal, only a moving one is not");

        JLib::TaskScheduler::SetHotWorkerRange(5, 2);
        JLib::TaskScheduler::GetHotWorkerRange(lo, hi);
        Check(hi >= lo, "an inverted range is normalised rather than accepted");

        // STATIC MUST BE INERT. This is the default, so a controller that moved K under Static
        // would be a behaviour change for every existing user of SetHotWorkers.
        JLib::TaskScheduler::SetHotWorkers(2);
        const size_t before = JLib::TaskScheduler::GetHotWorkers();
        for (int i = 0; i < 200; ++i) JLib::TaskScheduler::MaybeAdjustHotWorkers();
        Check(JLib::TaskScheduler::GetHotWorkers() == before,
              "min==max never moves K, however hard the controller is driven");

        // And the pre-existing invariant the whole lane design rests on.
        JLib::TaskScheduler::SetHotWorkers(n + 10);
        Check(JLib::TaskScheduler::GetHotWorkers() < n,
              "at least one ORDINARY worker always survives (K is clamped to pool-1)");
        JLib::TaskScheduler::SetHotWorkers(0);
    }

    std::printf("\n%s -- %d failure(s)\n", g_fail ? "FAILED" : "ALL CHECKS PASSED", g_fail);
    s.Join();
    return g_fail ? 1 : 0;
}
