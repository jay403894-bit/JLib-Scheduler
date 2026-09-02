// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// SetFiberBudget's whole job is to change what StartPool computes for the fiber pool's capacity --
// a plumbing question, not an algorithm, but the kind of plumbing that fails silently: get the
// wiring wrong and the setter still "works" (compiles, round-trips through its own getters) while
// StartPool quietly keeps using the old hardcoded 64/8. Checking the getters alone would not catch
// that; this checks the pool GlobalFiberPool actually built.
//
// A standalone binary rather than a case folded into primitives_test.cpp, because
// TestFiberCapOversubscribed there hardcodes 4*64=256 directly into its own logic (see that test's
// comment). Changing the process-wide fiber budget anywhere in that shared binary would silently
// invalidate it -- exactly the kind of cross-test contamination a separate process avoids for free.

#define NOMINMAX
#include <TaskScheduler.h>
#include <cstdio>
#include <cstdlib>

static int failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-64s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("SetFiberBudget\n");

    // ONE STACK CLASS. This asserted a second, 512 KB "heavy" budget as well; that class was
    // deleted because nothing in the library, the tests or the benches ever requested it while it
    // committed ~127 MB up front on a 31-worker pool. The assertions went with it rather than being
    // relaxed -- a test that checks a number nothing consumes is the same trap the class was.
    Check(JLib::TaskScheduler::StandardFibersPerWorker() == 64, "default is 64 fibers/worker");

    // Not the default, so a test that silently exercised the untouched default could not happen to
    // pass for the wrong reason.
    const size_t kPerWorker = 17;
    JLib::TaskScheduler::SetFiberBudget(kPerWorker);
    Check(JLib::TaskScheduler::StandardFibersPerWorker() == kPerWorker, "getter reflects the new value");

    // The real question: did StartPool actually CONSUME these, not just store them. An explicit
    // small pool size keeps the expected total exact rather than depending on this runner's core
    // count.
    JLib::TaskScheduler::Init(4);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    const size_t workers = sched.GetWorkerCount();

    // ---- ALL THREE CLASSES, NOT JUST STANDARD. ---------------------------------------------
    //
    // This was `workers * kPerWorker` and it was right only while the other two budgets were ZERO.
    // The moment deepPerComputeWorker defaulted to 1 the pool built 72 against an expected 68, and
    // the four extra were exactly the deep fibers -- a correct pool failing an incomplete
    // assertion. It caught the change, which is the good outcome; re-baselining the constant would
    // have thrown that away and left the same blind spot for tiny.
    //
    // Computed from the ACCESSORS rather than from literals, so this tracks the defaults instead of
    // pinning them. The test is about whether StartPool CONSUMES the budget it was given -- not
    // about what the budget happens to be.
    const size_t stdPer  = JLib::TaskScheduler::StandardFibersPerWorker();
    const size_t deepPer = JLib::TaskScheduler::DeepFibersPerComputeWorker();
    const size_t tinyPer = JLib::TaskScheduler::TinyFibersPerKWorker();
    const size_t K       = JLib::TaskScheduler::GetHotWorkers();
    const size_t expected = workers * stdPer + workers * deepPer + K * tinyPer;
    // TotalCount, NOT AvailableCount, and the difference is the whole point of the assertion.
    // The question here is "did StartPool consume the budget it was given" -- a CAPACITY question.
    // AvailableCount answers a different one: how many are unclaimed right now. Those two used to
    // be the same number only because no worker had ever touched the pool before going to sleep;
    // once workers stopped blocking they each take a park fiber and prime a thread-local cache
    // batch at startup, so `available` is legitimately smaller and the old check failed with
    // 32 against an expected 80. The instrument was wrong, not the pool.
    const size_t actual = sched.GetGlobalPool().TotalCount();
    char what[128];
    std::snprintf(what, sizeof(what),
                  "capacity = %zu*std(%zu) + %zu*deep(%zu) + %zu*tiny(%zu) = %zu, got %zu",
                  workers, stdPer, workers, deepPer, K, tinyPer, expected, actual);
    Check(actual == expected, what);

    JLib::detail::TeardownForTesting(sched);
    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
