// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A FULL LANE MUST NOT LOSE A TASK -- it GROWS, and overflow is the backstop past that.
//
// A TaskDeque is a FIXED 32,768 slots and push_bottom returns false when it is full. That return
// was honoured at exactly one of its three call sites. The other two ignored it, and neither
// failure looked like a dropped task:
//
//   the inbox drain    the task had already been popped, so a refusal LOST it. Its WaitGroup never
//                      decremented, so the failure surfaced as a HANG in whoever waited.
//   the yield requeue  a yielded fiber that is never requeued is never RESUMED. Its stack never
//                      unwinds, so nothing it holds is released -- RAII, its WaitGroup slot, a
//                      hazard record. Also a hang, further from the cause.
//
// HOW THIS FORCES THE CONDITION, since a running worker drains its own lane faster than a producer
// can fill it. Pushes from an external thread land in the worker's INBOX, which is unbounded; the
// deque only fills when the worker moves the inbox across. So: one worker, held on a blocking task
// so it drains nothing, flood the inbox well past a lane's capacity, then release it. The drain
// then hits a full deque for real, on the exact path that used to drop.
//
// WHAT IS ASSERTED is only what the fix promises -- every task runs exactly once. Not the order,
// and not that overflow was used: this must keep passing if the deque is ever grown or the
// overflow lane reshaped. The counter check below is the one that would catch a silent regression
// to "capacity is now large enough", which would make this test vacuous without failing it.

#include "TaskScheduler.h"
#include "TaskDeque.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(1);            // ONE worker: one lane to overflow, no stealing to hide it
    auto& sched = JLib::TaskScheduler::Instance();

    std::printf("deque overflow -- workers=%zu, lane capacity=32768\n\n", sched.GetWorkerCount());

    // REACHING THE PATH IS THE WHOLE TRICK, and the first version of this test did not.
    //
    // The main worker loop drains its inbox BATCH_SIZE (64) at a time via push_bottom_batch, takes
    // one to run, and already requeues on refusal -- so a lane never accumulates and no amount of
    // flooding from outside will fill one. Flooding from an external thread therefore proves
    // nothing; that version passed with the fix removed.
    //
    // DrainOwnInboxesToDeques is the unguarded one, and it is the SPIN-HELP path: a worker spinning
    // inside WaitFor moves its ENTIRE inbox across in one go, one push_bottom at a time. So the
    // flood has to come from a task running ON the worker, which then waits -- that is what puts
    // 45,000 items in an inbox that is about to be drained in a single pass.
    const int kN = 45000;

    const size_t growsBefore = JLib::TaskDeque::GrowCount();

    std::atomic<int> ran{ 0 };
    JLib::WaitGroup  rootWg;

    auto* root = sched.CreateTask([&] {
        JLib::WaitGroup inner;
        for (int i = 0; i < kN; ++i) {
            auto* t = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
            t->waitGroup = &inner;
            inner.n.fetch_add(1, std::memory_order_relaxed);
            sched.Push(t);
        }
        // Spinning here is what calls DrainOwnInboxesToDeques with a full inbox.
        sched.WaitFor(inner);
    });
    root->waitGroup = &rootWg;
    rootWg.n.fetch_add(1, std::memory_order_relaxed);
    sched.Push(root);

    sched.WaitFor(rootWg);

    Check(ran.load() == kN, "every task ran -- none lost to a full lane");
    if (ran.load() != kN) std::printf("      expected %d, got %d\n", kN, ran.load());

    // THE CHECK THAT MAKES THE ONE ABOVE MEAN SOMETHING. "Nothing was lost" is equally true of a run
    // where the deque never filled at all, so without this the test passes with the fix REMOVED --
    // and the first version of this file did exactly that, confirmed by deleting the overflow call
    // and watching it still report ALL CHECKS PASSED. Assert the path was ENTERED, not merely that
    // the outcome looked right.
    const size_t overflowed = sched.OverflowTotal(false) + sched.OverflowTotal(true);
    const size_t grew       = JLib::TaskDeque::GrowCount() - growsBefore;
    std::printf("      lane grew %zu time(s); overflow lane took %zu task(s)\n", grew, overflowed);

    // THE CHECK THAT MAKES "nothing was lost" MEAN SOMETHING. That is equally true of a run where
    // the lane never filled, and the first version of this file passed with its mechanism removed
    // for exactly that reason. Assert the path was ENTERED.
    //
    // IT ASSERTS GROWTH, NOT OVERFLOW, AND THAT CHANGED WHEN grow() LANDED. Before it, a full lane
    // pushed to the overflow lane and this asked for overflowed > 0. Now the lane DOUBLES instead,
    // so overflow is unreachable until kMaxCapacity -- 4M slots, far past what a test should
    // allocate -- and this same run reports 0 overflowed. Overflow is not dead code: it is what a
    // deque that cannot grow any further does instead of dropping a task.
    Check(grew > 0, "the lane actually GREW (else this test proves nothing)");

    if (kN <= 32768)
        std::printf("  WARNING: kN no longer exceeds a lane; this test is vacuous\n");

    sched.Join();
    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
