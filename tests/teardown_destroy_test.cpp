// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DO THE DESTRUCTORS ACTUALLY WORK? NOBODY HAS EVER FOUND OUT.
//
// `Init()` does `instance = new TaskScheduler(...)` and nothing deletes it. At exit AtExitDestroyer
// calls Join() and then leaks deliberately -- correct for a process that is about to stop existing,
// and it means `~TaskScheduler` and every member destructor beneath it HAVE NEVER EXECUTED. Not
// rarely: never, in any program, since the project began.
//
// THAT IS NOT A HYPOTHETICAL RISK AND THIS PROJECT HAS THE RECEIPT. `~TaskMPSCQueue` freed its stub
// with `::delete stub_` -- the `::` forcing the global deallocator, handing a TaskAllocator slab slot
// to the CRT heap, which is instant STATUS_HEAP_CORRUPTION for anyone who destroyed one. It survived
// for the life of the project because nothing ever destroyed one. Its own comment says so.
//
// So this file exists to run that code once. Production keeps the shipping behaviour (drain, then
// leak); the destructors get exercised here, where a fault is a failing test instead of somebody's
// shutdown.
//
// ---- WHAT IT IS EXPECTED TO FIND ------------------------------------------------------------
//
// Members destruct in REVERSE declaration order, and in TaskScheduler that order is:
//
//     deques           (2287)   vector<unique_ptr<TaskDeque>>
//     normalInboxes     (2376)   \
//     laneInboxes     (2377)    > vector<unique_ptr<TaskMPSCQueue>>
//     resumedInboxes   (2401)   /
//     taskAllocator    (2519)   owns the slab pools
//     mainQ            (2935)   TaskMPSCQueue
//
// so taskAllocator is destroyed BEFORE the three inbox vectors. And `~TaskMPSCQueue` ends with
// `alloc_->Free(stub_)`. Three queues per worker, ~90 on a 31-worker pool, every one of them
// freeing into an allocator that is already gone.
//
// The CHANGELOG records that `delete instance` "crashes, reproducibly, access violation" and places
// it "in TaskScheduler's own member destruction". This predicts exactly where and why. It is the
// same shape as the TimerQueue hang fixed the same week -- one object reaching into another after
// that other is gone -- except within a single class instead of between two statics.
//
// A PASS IS THEREFORE THE INTERESTING OUTCOME, and a crash is a diagnosis rather than a surprise.
#include "TaskScheduler.h"

#include <atomic>
#include <cstdio>

static std::atomic<int> g_ran{ 0 };

int main() {
    std::printf("teardown: destructors run for the first time\n");

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("  pool up, %zu workers\n", sched.GetWorkerCount());

    // REAL WORK FIRST, so the structures being destroyed are not pristine. A queue that never held
    // anything, a deque that never grew and a slab that never handed out a slot would exercise the
    // easy path through every destructor and prove very little.
    {
        constexpr int kN = 4000;
        JLib::WaitGroup wg;
        wg.n.store(kN, std::memory_order_relaxed);
        for (int i = 0; i < kN; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {
                g_ran.fetch_add(1, std::memory_order_relaxed);
            }, nullptr);
            if (!t) { std::printf("  CreateTask returned null\n"); return 1; }
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        std::printf("  %d tasks ran; inboxes, deques and the slab have all been used\n",
                    g_ran.load());
    }

    std::printf("  calling DestroyForTesting() -- Join, then the delete production skips\n");
    std::fflush(nullptr);

    JLib::detail::DestroyForTesting();

    // Reaching this line at all is the result. Anything that faults inside the destructors takes
    // the process with it and never gets here, which the exit code reports.
    std::printf("  returned from delete -- every member destructor executed\n");
    std::printf("\nALL CHECKS PASSED\n");
    return 0;
}
