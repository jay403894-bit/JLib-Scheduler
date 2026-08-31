// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// EAGER MUTEX CANCELLATION -- the waiter leaves WITHOUT anyone releasing the lock.
//
// This is the whole point, and it is what skip-at-release could not do. Every case below holds the
// lock for the duration and NEVER unlocks before the assertion: if the waiter comes back, it came
// back because it was ejected, not because the lock became available.
//
// WHY IT MATTERED ENOUGH TO CHANGE. A frame parked on a mutex whose holder is itself abandoned could
// not be woken by anything -- so its stack never unwound, and nothing it held was released: RAII,
// its WaitGroup slot, a hazard record. The mutex was the only blocking primitive with no way out,
// which made it the structural reason parked work leaks at teardown.

#include "TaskScheduler.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int  g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-68s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();
    std::printf("eager mutex cancellation -- workers=%zu\n\n", sched.GetWorkerCount());

    // ---- a cancelled fiber waiter is ejected while the lock is STILL HELD --------------------
    {
        SchedulerMutex m;
        m.Lock();                                     // held by main for the whole case

        CancelScope scope;
        std::atomic<int> result{ -1 };
        std::atomic<bool> parked{ false }, done{ false };

        WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        Task* t = sched.CreateTask([&]() {
            parked.store(true, std::memory_order_release);
            const WaitResult r = m.LockCancellable();
            result.store((int)r, std::memory_order_release);
            if (r == WaitResult::Ok) m.Unlock();      // only unlock if we actually got it
            done.store(true, std::memory_order_release);
        }, false, FiberSize::Standard, TaskType::Fiber);
        t->cancelToken = scope.Token().Raw();      // LockCancellable reads the TASK's token
        t->waitGroup = &wg;
        sched.Push(t);

        for (int i = 0; i < 1000 && !parked.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        std::this_thread::sleep_for(std::chrono::milliseconds(30));   // reach the park

        Check(!done.load(std::memory_order_acquire),
              "the fiber is genuinely parked on a held lock (otherwise this proves nothing)");

        // THE POINT: cancel and eject, with main still holding the lock.
        scope.Cancel();
        m.CancelWaiters(scope.Token());

        for (int i = 0; i < 1000 && !done.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        Check(done.load(std::memory_order_acquire),
              "it came back WITHOUT the lock ever being released -- skip-at-release could not");
        Check(result.load(std::memory_order_acquire) == (int)WaitResult::Cancelled,
              "and it reports Cancelled, so it knows it does not hold the lock");

        sched.WaitFor(wg);
        m.Unlock();
    }

    // ---- an UNCANCELLED waiter is left alone --------------------------------------------------
    // The negative control for the filter: ejecting everything would pass the case above too.
    {
        SchedulerMutex m;
        m.Lock();

        CancelScope victimScope, bystanderScope;
        std::atomic<bool> bystanderDone{ false };

        WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        Task* t = sched.CreateTask([&]() {
            const WaitResult r = m.LockCancellable();
            if (r == WaitResult::Ok) m.Unlock();
            bystanderDone.store(true, std::memory_order_release);
        }, false, FiberSize::Standard, TaskType::Fiber);
        t->cancelToken = bystanderScope.Token().Raw();
        t->waitGroup = &wg;
        sched.Push(t);

        std::this_thread::sleep_for(std::chrono::milliseconds(40));

        // Cancel a DIFFERENT scope. The bystander must stay parked.
        victimScope.Cancel();
        m.CancelWaiters(victimScope.Token());
        std::this_thread::sleep_for(std::chrono::milliseconds(40));

        Check(!bystanderDone.load(std::memory_order_acquire),
              "NEGATIVE CONTROL: a waiter under a DIFFERENT scope is not ejected");

        m.Unlock();                                   // now it may have the lock
        sched.WaitFor(wg);
        Check(bystanderDone.load(std::memory_order_acquire),
              "and it still acquires normally once the lock is released");
    }

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
