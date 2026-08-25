// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE CANCELLABLE WAIT: WaitFor(wg, token) ends THIS WAIT, not the work.
//
// The distinction this file exists to defend is that cancelling a wait is "I stopped waiting", NOT
// "the group is finished". The obvious-looking implementation -- have the cancel drive the count to
// zero so every waiter falls out -- is wrong in a way that is invisible until it corrupts something:
// the outstanding tasks are still outstanding, they still complete, and they still decrement. A
// count zeroed by a cancel is a lie that a second waiter believes, and it strands every task still
// in flight with nothing left for it to release.
//
// So the checks here are mostly about what must NOT change:
//   - n is untouched by the cancel, and the tasks still drive it to zero themselves
//   - an UNCANCELLABLE WaitFor on the same group is not woken by somebody else's cancel
//   - a group that genuinely completed reports Ok even if the token also fired
//
// Both waiting paths are covered, because they are different code: a fiber PARKS on a DirectEvent
// and has to be unparked, while a bare thread spin-helps and observes the flag between passes.

#include "TaskScheduler.h"
#include "CancelToken.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

namespace {

int g_fail = 0;

void Check(bool ok, const char* what) {
    std::printf("  %-64s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) ++g_fail;
}

// Bounded, so a broken build fails instead of hanging CI.
template <typename Fn>
bool WaitUntil(Fn&& pred, int ms = 5000) {
    for (int i = 0; i < ms; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();

    // ---- BARE THREAD: cancel observed between helping passes -------------------------------
    std::printf("bare-thread wait, cancelled while work is still outstanding\n");
    {
        std::atomic<bool> release{ false };
        std::atomic<int>  ran{ 0 };
        JLib::WaitGroup wg;
        JLib::CancelScope scope;

        // One task that will not finish until we say so, so the wait is genuinely outstanding
        // at the moment we cancel.
        auto* t = sched.CreateTask([&] {
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
            ran.fetch_add(1, std::memory_order_relaxed);
            });
        t->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        sched.Push(t);

        // Cancel from another thread while this one is inside the wait.
        std::thread canceller([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            scope.Cancel();
            });

        const JLib::WaitResult r = sched.WaitFor(wg, scope.Token());
        canceller.join();

        Check(r == JLib::WaitResult::Cancelled, "the wait returned Cancelled");
        Check((wg.n.load() & JLib::WaitGroup::COUNT_MASK) == 1,
              "n is UNTOUCHED -- the task is still outstanding");
        Check(ran.load() == 0, "and it genuinely has not run yet");

        // THE POINT: the abandoned work still completes and still decrements. If the cancel had
        // zeroed the count, this decrement would underflow it instead.
        release.store(true, std::memory_order_release);
        Check(WaitUntil([&] { return (wg.n.load() & JLib::WaitGroup::COUNT_MASK) == 0; }),
              "the remaining task still ran and still decremented to zero");
        Check(ran.load() == 1, "exactly once");
    }

    // ---- BARE THREAD: uncancelled group completes normally ---------------------------------
    std::printf("bare-thread wait, group completes\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        JLib::CancelScope scope;

        for (int i = 0; i < 8; ++i) {
            auto* t = sched.CreateTask([&] { ran.fetch_add(1, std::memory_order_relaxed); });
            t->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
            sched.Push(t);
        }

        const JLib::WaitResult r = sched.WaitFor(wg, scope.Token());
        Check(r == JLib::WaitResult::Ok, "an uncancelled wait returns Ok");
        Check(ran.load() == 8, "every task ran");
    }

    // ---- COMPLETION BEATS CANCELLATION when both are true -----------------------------------
    std::printf("a group that finished reports Ok even with the token already fired\n");
    {
        JLib::WaitGroup wg;
        JLib::CancelScope scope;
        scope.Cancel();                       // fired BEFORE the wait even starts

        // Nothing outstanding: the group is complete, so the honest answer is Ok. Reporting
        // Cancelled here would tell the caller to throw away results that exist.
        const JLib::WaitResult r = sched.WaitFor(wg, scope.Token());
        Check(r == JLib::WaitResult::Ok, "complete group reports Ok, not Cancelled");
    }

    // ---- FIBER PATH: the waiter PARKS, so it has to be unparked ------------------------------
    std::printf("fiber wait, released early by CancelWaiters\n");
    {
        std::atomic<bool> release{ false };
        std::atomic<int>  result{ -1 };
        std::atomic<bool> waiterParked{ false };
        JLib::WaitGroup inner;                // what the fiber waits on
        JLib::WaitGroup outer;                // how we wait for the fiber itself
        JLib::CancelScope scope;

        auto* blocker = sched.CreateTask([&] {
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
            });
        blocker->waitGroup = &inner; inner.n.fetch_add(1, std::memory_order_relaxed);
        sched.Push(blocker);

        auto* waiter = sched.CreateTask([&] {
            waiterParked.store(true, std::memory_order_release);
            const JLib::WaitResult r = sched.WaitFor(inner, scope.Token());
            result.store(int(r), std::memory_order_release);
            });
        waiter->type = JLib::TaskType::Fiber;   // must be a fiber: this one actually parks
        waiter->waitGroup = &outer; outer.n.fetch_add(1, std::memory_order_relaxed);
        sched.Push(waiter);

        Check(WaitUntil([&] { return waiterParked.load(std::memory_order_acquire); }),
              "the waiting fiber reached the wait");

        // Give it a moment to actually park, then release it EAGERLY -- the whole reason
        // WaitGroup::CancelWaiters exists. A parked fiber cannot poll a flag.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        scope.Cancel();
        const std::size_t woken = inner.CancelWaiters(scope.Token());

        Check(WaitUntil([&] { return result.load(std::memory_order_acquire) >= 0; }),
              "the parked fiber was released");
        Check(woken == 1, "CancelWaiters reported waking exactly one waiter");
        Check(result.load() == int(JLib::WaitResult::Cancelled), "and it returned Cancelled");
        Check((inner.n.load() & JLib::WaitGroup::COUNT_MASK) == 1,
              "n is still UNTOUCHED after an eager release");

        release.store(true, std::memory_order_release);
        Check(WaitUntil([&] { return (inner.n.load() & JLib::WaitGroup::COUNT_MASK) == 0; }),
              "the abandoned task still completed on its own");
        sched.WaitFor(outer);
    }

    // ---- AN UNCANCELLABLE WAITER IS NOT COLLATERAL ------------------------------------------
    std::printf("a plain WaitFor is not woken by somebody else's cancel\n");
    {
        std::atomic<bool> release{ false };
        std::atomic<bool> plainReturned{ false };
        JLib::WaitGroup inner, outer;
        JLib::CancelScope scope;

        auto* blocker = sched.CreateTask([&] {
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
            });
        blocker->waitGroup = &inner; inner.n.fetch_add(1, std::memory_order_relaxed);
        sched.Push(blocker);

        auto* plain = sched.CreateTask([&] {
            sched.WaitFor(inner);                       // uncancellable by contract
            plainReturned.store(true, std::memory_order_release);
            });
        plain->type = JLib::TaskType::Fiber;
        plain->waitGroup = &outer; outer.n.fetch_add(1, std::memory_order_relaxed);
        sched.Push(plain);

        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        scope.Cancel();
        const std::size_t woken = inner.CancelWaiters(scope.Token());

        Check(woken == 0, "CancelWaiters woke nobody -- the plain waiter is not in that list");
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        Check(!plainReturned.load(std::memory_order_acquire),
              "and the plain waiter is still waiting, as its contract says");

        release.store(true, std::memory_order_release);
        Check(WaitUntil([&] { return plainReturned.load(std::memory_order_acquire); }),
              "it returns only when the group actually completes");
        sched.WaitFor(outer);
    }

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
