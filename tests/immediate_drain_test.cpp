// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Correctness test for Worker()'s immediate/fork inbox drain (Thread.cpp, "2. Immediate task
// execution"), which moved from a per-task Requeue() loop to a single PushBatch() call in 2.2.0
// (SchedulerBench's "requeue vs pushbatch" sweep measured 7.5-8.2x less dispatch time for the
// swap). Nothing exercised this path before -- PushImmediate() had zero test coverage of any kind
// -- so this is new coverage, not a regression guard for a previously-known bug.
//
// What specifically has to keep working after the swap: PushBatch's segmenting can run MULTIPLE
// rounds when a worker's inbox backlog exceeds BATCH_SIZE (64); the null-compaction added because
// PushBatch links tasks[i]->next contiguously and cannot tolerate a hole (the old Requeue loop's
// per-task `if (!t) continue` could) must not drop or duplicate a real task; and the drain must
// still terminate even though the pinning worker's own core is excluded from PushBatch's
// placement -- the same immediateCoresInUse invariant the old Requeue-based drain relied on.
//
// Only the loPri inbox is exercised deterministically here: PushLocal's explicit-cpuaffinity
// branch (TaskScheduler.cpp) routes into loPriInboxes[idx] regardless of a task's own hiPri flag,
// which is the one public entry point that can land a backlog in a SPECIFIC worker's inbox at
// all -- there is no equally deterministic way to force a large hiPri backlog onto one worker
// through public API alone. The hiPri call site runs the identical drainInbox lambda body with
// only the literal bool passed to PushBatch differing, and PushBatch's hiPri branch itself
// (hiPriInboxes vs loPriInboxes) is already exercised elsewhere (e.g. ParallelFor's hiPri
// batches), so the residual risk from that asymmetry is low but not zero -- noted rather than
// silently assumed away.

#define NOMINMAX
#include <TaskScheduler.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>

using namespace JLib;

static int failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-72s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("Immediate-task inbox drain (PushBatch-based, since 2.2.0)\n");

    // 2 workers: worker 0 (core 1) gets pinned by PushImmediate below. Worker 1 is the ONLY place
    // PushBatch's segment loop can route the drained backlog to -- immediateCoresInUse excludes
    // worker 0 the instant it pins -- so 2 is the minimum pool size that can prove the drain
    // doesn't strand its own backlog behind the pin it is trying to get out from under.
    TaskScheduler::Init(2);
    TaskScheduler& sched = TaskScheduler::Instance();

    static std::atomic<int> completed{ 0 };

    // 150 > BATCH_SIZE (64): forces drainInbox's for(;;) loop through more than one round, so
    // PushBatch is called more than once for this single backlog, not just the trivial case.
    constexpr int kBacklog = 150;
    WaitGroup wg;
    wg.n.store(kBacklog, std::memory_order_relaxed);
    for (int i = 0; i < kBacklog; ++i) {
        Task* t = sched.CreateTask(+[](void*) { completed.fetch_add(1, std::memory_order_relaxed); }, nullptr);
        t->waitGroup = &wg;
        sched.Push(1, t);   // explicit cpu_affinity=1 (1-indexed) -> worker 0's loPriInbox, deterministically
    }

    static std::atomic<bool> immediateRan{ false };
    WaitGroup immWg;
    immWg.n.store(1, std::memory_order_relaxed);
    Task* immediateTask = sched.CreateTask(+[](void*) {
        immediateRan.store(true, std::memory_order_relaxed);
        }, nullptr);
    immediateTask->waitGroup = &immWg;

    // Pins worker 0. Its Worker() loop must drain its OWN inbox -- the 150 tasks just pushed --
    // via the new PushBatch-based drainInbox BEFORE it starts running immediateTask at all.
    bool pushed = sched.PushImmediate(1, immediateTask);
    Check(pushed, "PushImmediate accepted (pool active)");

    // Relies on the CI job-level timeout as the hang backstop, same convention as
    // fiber_budget_test.cpp / task_slab_size_test.cpp -- a real hang here (the backlog stranded
    // behind the pin, or PushBatch routing a task back INTO the inbox being drained) fails the job
    // instead of this test adding its own bespoke watchdog.
    sched.WaitFor(wg);
    sched.WaitFor(immWg);

    Check(completed.load(std::memory_order_acquire) == kBacklog,
          "every backlogged task ran exactly once (no drop/duplicate from PushBatch compaction)");
    Check(immediateRan.load(std::memory_order_relaxed), "immediate task itself ran after the drain");

    sched.Join();
    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
