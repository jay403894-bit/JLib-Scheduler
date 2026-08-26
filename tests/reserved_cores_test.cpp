// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// SetReservedCores has to change what the pool is BUILT with, not just what a getter echoes back --
// the same standard SetSlabSizes and SetFiberBudget are held to, and for the same reason: a setter
// that stores a value and is never consulted passes every test that only reads it back.
//
// A STANDALONE BINARY, because this can only be observed through the AUTO pool size. Init() runs
// once per process and an explicit poolSize bypasses GetSafeTC entirely, so there is no way to fold
// this into a suite that has already initialised.
//
// WHY THIS EXISTS AT ALL: it is the replacement for PushImmediate. That API pinned a pool worker to
// a blocking subsystem -- taking a worker out of a WORK-STEALING pool and spilling its queue to
// everyone else. Reserving a core and running a plain std::thread buys the same census accounting
// (workers + main + yours = hardware_concurrency) with none of the invariants, because the
// scheduler never owns the thread.

#define NOMINMAX
#include <TaskScheduler.h>
#include <cstdio>
#include <thread>

static int failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-66s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("SetReservedCores\n");

    const unsigned hw = std::thread::hardware_concurrency();
    std::printf("  hardware_concurrency = %u\n", hw);
    if (hw <= 4) {
        // The arithmetic below would clamp to 1 and stop distinguishing anything.
        std::printf("  too few cores to observe the reservation; skipping\n");
        return 0;
    }

    Check(JLib::TaskScheduler::GetReservedCores() == 0, "nothing is reserved by default");

    constexpr unsigned kReserve = 2;
    JLib::TaskScheduler::SetReservedCores(kReserve);
    Check(JLib::TaskScheduler::GetReservedCores() == kReserve, "the getter reflects the new value");

    // BEFORE Init, which is the whole contract. The pool is sized once, so a value set afterwards
    // could only be a no-op, and a test that set it late would pass while proving nothing.
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();

    // main + kReserve are the app's; the rest are workers. Timer and reactor are both off in a
    // fresh process, so they contribute nothing here -- which is deliberate: this asserts the term
    // THIS setter adds, not the sum of every reservation.
    const size_t expected = (size_t)hw - 1 - kReserve;
    char what[160];
    std::snprintf(what, sizeof(what),
                  "the pool is actually SMALLER by %u: expected %zu workers, got %zu",
                  kReserve, expected, sched.GetWorkerCount());
    Check(sched.GetWorkerCount() == expected, what);

    // The census closes. This is the number the whole feature exists to keep honest: a thread the
    // scheduler does not know about is a core that quietly went missing, and PushImmediate's
    // justification was exactly that a pinned task stayed visible to placement.
    std::snprintf(what, sizeof(what),
                  "workers + main + reserved == hardware_concurrency (%zu + 1 + %u == %u)",
                  sched.GetWorkerCount(), kReserve, hw);
    Check(sched.GetWorkerCount() + 1 + kReserve == (size_t)hw, what);

    // And the pool still works with the smaller count -- an arithmetic change that produced a pool
    // which cannot run anything would pass every check above.
    std::atomic<int> ran{ 0 };
    JLib::WaitGroup wg;
    wg.n.store(64, std::memory_order_relaxed);
    for (int i = 0; i < 64; ++i) {
        auto* t = sched.CreateTask([&ran]() { ran.fetch_add(1, std::memory_order_relaxed); });
        if (t) { t->waitGroup = &wg; sched.Push(t); }
        else   { wg.n.fetch_sub(1, std::memory_order_relaxed); }
    }
    sched.WaitFor(wg);
    Check(ran.load() == 64, "the reduced pool still runs work");

    // A DEFAULT-SIZED pool that was never starved, so these high-water marks are a real profile
    // rather than "every class hit its ceiling" -- which is all the slab-size test can show, since
    // that one deliberately exhausts every class in turn.
    //
    // PRINTED, NOT ASSERTED. The numbers depend on what this binary happened to run; pinning them
    // would be asserting the test rather than the code.
    JLib::TaskScheduler::ReportSlabUsage("slab usage, default pool");

    sched.Join();
    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
