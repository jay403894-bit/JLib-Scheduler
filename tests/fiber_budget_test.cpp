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

    // Defaults, before Init() and before this process has ever called the setter -- must match
    // what StartPool always computed inline (coreCount * 64, coreCount * 8) before this API existed.
    Check(JLib::TaskScheduler::StandardFibersPerWorker() == 64, "default standard/worker is 64");
    Check(JLib::TaskScheduler::HeavyFibersPerWorker() == 8, "default heavy/worker is 8");

    // Neither value is the default in either direction, so a test that silently exercised the
    // untouched default could not happen to pass for the wrong reason.
    const size_t kStandard = 17, kHeavy = 3;
    JLib::TaskScheduler::SetFiberBudget(kStandard, kHeavy);
    Check(JLib::TaskScheduler::StandardFibersPerWorker() == kStandard, "getter reflects the new standard value");
    Check(JLib::TaskScheduler::HeavyFibersPerWorker() == kHeavy, "getter reflects the new heavy value");

    // The real question: did StartPool actually CONSUME these, not just store them. An explicit
    // small pool size keeps the expected total exact rather than depending on this runner's core
    // count.
    JLib::TaskScheduler::Init(4);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    const size_t workers = sched.GetWorkerCount();
    const size_t expected = workers * (kStandard + kHeavy);
    const size_t actual = sched.GetGlobalPool().AvailableCount();
    char what[128];
    std::snprintf(what, sizeof(what),
                  "pool capacity is workers(%zu) * (standard+heavy) = %zu, got %zu",
                  workers, expected, actual);
    Check(actual == expected, what);

    sched.Join();
    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
