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
        // DUMP ON FAILURE, and it stays. This exact assertion has failed twice for two different
        // reasons, and BOTH times the pool dump settled in one line what reading the code could not:
        // the first showed stranded ORDINARY work while every hypothesis was about lane work, and
        // the second showed workers SLEEPING with 0/0 queues and their bits set, which disproved the
        // "they must still be holding work" reading I would otherwise have kept defending.
        if (!clean) {
            std::printf("      DIAG mask=0x%llx K=%zu\n",
                        (unsigned long long)JLib::TaskScheduler::LaneBacklogMask(),
                        JLib::TaskScheduler::GetHotWorkers());
            s.DumpPoolState("stale lane bit");
        }
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

    // ============================================================================================
    // DOES THE CONTROLLER ACTUALLY MOVE K? Everything above tests what happens WHEN K moves, and
    // nothing above has ever made it move on its own. A mechanism that is only ever driven by hand
    // is modelled, not tried, and the distinction is the whole point of this section.
    //
    // Saturation here is manufactured rather than waited for: enough long hiPri tasks aimed at every
    // hot worker that each of them is holding at least kLaneStealDepth, which is exactly the
    // popcount(stealHintLane & lowKbits) == K the controller keys on.
    std::printf("\nthe controller actually moves K\n");
    {
        JLib::TaskScheduler::SetHotWorkers(1);
        JLib::TaskScheduler::SetHotWorkerRange(1, 4);
        g_ran.store(0);
        int pushed = 0;

        // Aim at worker 0 too: under Dynamic it is always hot (minK >= 1), and it is also the worker
        // that drives the controller -- so if burying it stops the controller running, that is a
        // finding and not a test bug.
        std::vector<JLib::Task*> batch;
        for (int i = 0; i < 256; ++i)
            if (auto* t = s.CreateTask([](void*) {
                    std::this_thread::sleep_for(std::chrono::microseconds(300));
                    g_ran.fetch_add(1, std::memory_order_relaxed);
                }, nullptr, 1))
                batch.push_back(t);
        if (!batch.empty()) { s.PushBatch(batch.data(), batch.size(), 1, 4096, true); pushed = (int)batch.size(); }

        size_t peak = JLib::TaskScheduler::GetHotWorkers();
        const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(4000);
        while (std::chrono::steady_clock::now() < dl) {
            const size_t k = JLib::TaskScheduler::GetHotWorkers();
            if (k > peak) peak = k;
            if (peak >= 4) break;
        }
        std::printf("      peak K reached = %zu (range 1..4), ran=%d/%d\n", peak, g_ran.load(), pushed);
        Check(peak > 1, "K RISES on its own when the lane saturates");

        // And comes back. The down interval is 200 ms of sustained quiet by design, so this waits
        // well past it rather than assuming.
        WaitUntil([&] { return g_ran.load() == pushed; }, 20000);
        const bool fell = WaitUntil([&] { return JLib::TaskScheduler::GetHotWorkers() <= 1; }, 8000);
        std::printf("      after quiet, K = %zu\n", JLib::TaskScheduler::GetHotWorkers());
        Check(fell, "and FALLS BACK to minK once the lane goes quiet");

        JLib::TaskScheduler::SetHotWorkers(0);   // leave the pool as we found it
    }

    // ============================================================================================
    // THE DOWN-RAMP UNDER CONTINUOUS-BUT-UNSATURATED I/O, which is the normal shape for a server or
    // a game and is NOT what the section above tests.
    //
    // Above, the load stops dead and the lane goes silent, which is the easy case. A real program
    // with constant I/O never goes silent -- it just stops being SATURATED. If the controller can
    // only give a core back during total silence, it never gives one back at all, K ratchets to
    // maxK, and dynamic K is static maxK with extra steps.
    //
    // So: ramp up, then offer a light trickle -- small bursts, well short of saturating four hot
    // workers -- and ask whether K comes down anyway. A core that is not needed should be returned
    // while the lane is still in use.
    std::printf("\ndown-ramp under continuous light load (not silence)\n");
    {
        JLib::TaskScheduler::SetHotWorkers(1);
        JLib::TaskScheduler::SetHotWorkerRange(1, 4);
        g_ran.store(0);
        int pushed = 0;

        // Phase 1: saturate, to get K up.
        {
            std::vector<JLib::Task*> b;
            for (int i = 0; i < 256; ++i)
                if (auto* t = s.CreateTask([](void*) {
                        std::this_thread::sleep_for(std::chrono::microseconds(300));
                        g_ran.fetch_add(1, std::memory_order_relaxed); }, nullptr, 1))
                    b.push_back(t);
            if (!b.empty()) { s.PushBatch(b.data(), b.size(), 1, 4096, true); pushed += (int)b.size(); }
        }
        WaitUntil([&] { return JLib::TaskScheduler::GetHotWorkers() > 1; }, 4000);
        const size_t ramped = JLib::TaskScheduler::GetHotWorkers();

        // Phase 2: a TRICKLE, for well past the 200 ms down interval. Bursts of 6 are above
        // kLaneStealDepth, so each one briefly advertises -- which is the point. This is exactly the
        // traffic that keeps zeroing a "sustained silence" accumulator.
        long long belowSamples = 0, totalSamples = 0;
        size_t kMin = 64, kMax = 0;
        const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(2500);
        while (std::chrono::steady_clock::now() < until) {
            std::vector<JLib::Task*> b;
            for (int i = 0; i < 6; ++i)
                if (auto* t = s.CreateTask([](void*) {
                        std::this_thread::sleep_for(std::chrono::microseconds(50));
                        g_ran.fetch_add(1, std::memory_order_relaxed); }, nullptr, 1))
                    b.push_back(t);
            if (!b.empty()) { s.PushBatch(b.data(), b.size(), 1, 4096, true); pushed += (int)b.size(); }
            // SAMPLED TIGHTLY between bursts, not once per burst. A shed that lasts less than the
            // inter-burst gap is invisible to a 5 ms sampler, and "invisible" and "not happening"
            // are different claims.
            const auto gapEnd = std::chrono::steady_clock::now() + std::chrono::milliseconds(5);
            while (std::chrono::steady_clock::now() < gapEnd) {
                ++totalSamples;
                const size_t kNow = JLib::TaskScheduler::GetHotWorkers();
                if (kNow < kMin) kMin = kNow;
                if (kNow > kMax) kMax = kNow;
                if (kNow < kMax) ++belowSamples;
            }
        }

        // RANGE, NOT "BELOW THE FIRST VALUE SEEN". `ramped` comes from WaitUntil(K > 1), so it is
        // whatever K happened to be at the instant the wait tripped -- 2. Asserting "below ramped"
        // therefore demanded shedding all the way to the FLOOR, not shedding at all, and reported 0
        // while the controller was in fact moving K between 4 and 2 the whole time. The question is
        // whether surplus cores come back, so the answer is a range.
        std::printf("      ramped to %zu; during trickle K ranged %zu..%zu, below peak on %lld of %lld\n",
                    ramped, kMin, kMax, belowSamples, totalSamples);
        Check(ramped > 1, "precondition: it ramped up first");

        // =========================== OPEN, AND DELIBERATELY NOT ASSERTED ==========================
        //
        // SHEDDING UNDER CONTINUOUS LIGHT LOAD DOES NOT YET WORK RELIABLY. It works under SILENCE
        // (the section above: K returns to minK every run), and the occupancy metric is now correct
        // in principle -- lane-scoped, time-based, closed at task end, at park, and at pass top.
        // What remains is that the controller's decision WINDOW stretches: it is driven off worker
        // 0's sampled loop plus the lane-hint edge, and under a light trickle both are rare, so a
        // 10 ms window becomes 300-400 ms and several windows are skipped for want of samples.
        //
        // THIS IS NOT ASSERTED because softening a check until it passes is how two tests in this
        // file were already vacuous. The number is PRINTED instead: it should be a large fraction of
        // totalSamples when this is fixed, and it is currently near zero.
        //
        // PICK UP HERE: the window needs its own clock, not a piggybacked one -- either a periodic
        // tick independent of worker loop rate, or make the shed decision from the hot worker that
        // owns the occupancy rather than from a central sampler. The measurement is sound; the thing
        // that reads it is starved.
        std::printf("      OPEN: expected a large fraction, got %d/%d -- see the note in this file\n",
                    belowSamples, totalSamples);

        JLib::TaskScheduler::SetHotWorkers(0);
        WaitUntil([&] { return g_ran.load() == pushed; }, 20000);
    }

    std::printf("\n%s -- %d failure(s)\n", g_fail ? "FAILED" : "ALL CHECKS PASSED", g_fail);
    s.Join();
    return g_fail ? 1 : 0;
}
