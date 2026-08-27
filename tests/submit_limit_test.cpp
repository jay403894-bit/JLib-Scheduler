// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// INGRESS BACKPRESSURE -- bound how far a producer may run ahead of the pool.
//
// The deques are capped and overflow to a side lane; the slab grows and says so. Inboxes were the
// one unbounded path, so a producer flooding the runtime grew memory without limit and nothing
// said a word.
//
// THE DANGEROUS VERSION OF THIS FIX IS THE OBVIOUS ONE. Bounding a queue means somebody has to stop
// pushing, and if that somebody is a WORKER it may be the only thread able to drain the queue it is
// waiting on: a Native task inside ParallelFor pushes chunks to every worker INCLUDING itself. A
// bound that holds workers deadlocks deterministically, not occasionally. So the bound applies to
// NON-WORKER submitters only, and the third case below is the one that would catch it if that ever
// regressed -- it pushes from inside a task, which must never be held.

#include "TaskScheduler.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // BEFORE Init, which the API requires: the depth counter is only maintained while a limit is
    // set, so enabling it later would count from a base that already has tasks in flight.
    const size_t kLimit = 256;
    JLib::TaskScheduler::SetSubmitLimit(kLimit);

    // ONE WORKER, DELIBERATELY. With 31 workers the producer never outruns the pool -- pushing a
    // task costs more than running one -- so no backlog forms and the test cannot tell the
    // mechanism from its absence. Verified: the uncontrolled run peaked at 13. A single worker
    // makes the pool the bottleneck, which is the condition backpressure exists for.
    JLib::TaskScheduler::Init(1);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("submit limit -- workers=%zu, limit=%zu\n\n", sched.GetWorkerCount(), kLimit);

    Check(JLib::TaskScheduler::GetSubmitLimit() == kLimit, "the limit is what was asked for");

    // ---- a flooding external producer is slowed, and nothing is lost ----------------------------
    //
    // MEASURED AT TWO SUBMISSION COUNTS, and the comparison between them IS the assertion. An
    // absolute ceiling on the peak is a magic number that has to be tuned, and the first version of
    // this test picked one loose enough to pass with the mechanism REMOVED -- the uncontrolled run
    // peaked at 2,592 against a ceiling of 5,120 and reported success.
    //
    // The real property is structural and needs no constant: with backpressure the peak depth is a
    // function of the LIMIT, so quadrupling the submission count leaves it roughly where it was.
    // Without it the peak is a function of how far the producer outran the pool, so it scales with
    // the count. Ratio, not magnitude.
    auto floodAndPeak = [&](int kN) -> size_t {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        size_t peak = 0;
        for (int i = 0; i < kN; ++i) {
            auto* t = sched.CreateTask([&ran] {
                for (volatile int s = 0; s < 40; ++s) {}
                ran.fetch_add(1, std::memory_order_relaxed);
            });
            t->waitGroup = &wg;
            wg.n.fetch_add(1, std::memory_order_relaxed);
            sched.Push(t);
            const size_t d = JLib::TaskScheduler::QueuedDepth();
            if (d > peak) peak = d;
        }
        sched.WaitFor(wg);
        Check(ran.load() == kN, "every task ran -- backpressure delays, it never drops");
        return peak;
    };

    const size_t peakSmall = floodAndPeak(15000);
    const size_t peakLarge = floodAndPeak(60000);
    std::printf("      peak depth: %zu at 15,000 submissions, %zu at 60,000 (limit %zu)\n",
                peakSmall, peakLarge, kLimit);

    // 4x the work must not mean 4x the backlog. Bounded runs land near each other; unbounded grows
    // with the count. 2x leaves room for ordinary noise and still separates the two by a wide
    // margin -- verified by deleting ApplyIngressBackpressure and watching this fail.
    Check(peakLarge < peakSmall * 2 + kLimit,
          "peak depth did NOT grow with the submission count");

    // ---- a WORKER is never held, which is the deadlock this design exists to avoid --------------
    {
        std::atomic<int> inner{ 0 };
        JLib::WaitGroup rootWg;

        auto* root = sched.CreateTask([&] {
            // Pushing far past the limit from INSIDE a task. If backpressure ever applied to
            // workers, this is where it would hang: this thread is a consumer of the very inbox it
            // would be waiting on.
            JLib::WaitGroup innerWg;
            for (int i = 0; i < 5000; ++i) {
                auto* t = sched.CreateTask([&inner] { inner.fetch_add(1, std::memory_order_relaxed); });
                t->waitGroup = &innerWg;
                innerWg.n.fetch_add(1, std::memory_order_relaxed);
                sched.Push(t);
            }
            sched.WaitFor(innerWg);
        });
        root->waitGroup = &rootWg;
        rootWg.n.fetch_add(1, std::memory_order_relaxed);
        sched.Push(root);
        sched.WaitFor(rootWg);

        Check(inner.load() == 5000, "a task pushing 5,000 from a worker completed -- workers are never held");
    }

    sched.Join();
    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
