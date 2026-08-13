// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Tests for SchedulerMutex, SchedulerSemaphore, SchedulerConditionVariable and ScopedPermit.
//
// These were the least-tested code in the scheduler and the most recently changed: 1.2.3 added three
// behavioural guards to them and 1.2.4 touched them again, both verified by little more than "the
// benchmark still completes", which barely exercises a lock at all.
//
// EVERY BLOCKING TEST RUNS UNDER A WATCHDOG. A regression here does not produce a wrong answer, it
// produces a hang -- and a hang in CI burns the entire job timeout before reporting anything, which
// is exactly what the 1.2.0 macOS bug did for thirty minutes a run. The watchdog turns that into a
// failed assertion in seconds.

#define NOMINMAX
#include <TaskScheduler.h>
#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <thread>
#include <chrono>

static int failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-58s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

// ---- watchdog ---------------------------------------------------------------------------------
// _Exit rather than abort or a thrown exception: the point is to escape a deadlock, and a deadlocked
// process cannot be relied on to unwind anything. A nonzero exit is all CI needs.
static std::atomic<bool> g_done{ false };
static void StartWatchdog(int seconds, const char* what) {
    std::thread([seconds, what] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
        while (!g_done.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() > deadline) {
                std::printf("\n  WATCHDOG: '%s' did not finish in %ds -- treating as a DEADLOCK.\n",
                            what, seconds);
                std::fflush(stdout);
                std::_Exit(1);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
    }).detach();
}

// ---- 1. SchedulerMutex basics -------------------------------------------------------------------
static void TestMutexBasics() {
    std::printf("SchedulerMutex\n");
    JLib::SchedulerMutex m;

    Check(m.Try_Lock(),  "Try_Lock succeeds on an uncontended mutex");
    Check(!m.Try_Lock(), "Try_Lock fails while held");
    m.Unlock();
    Check(m.Try_Lock(),  "Try_Lock succeeds again after Unlock");
    m.Unlock();

    // Lock/Unlock uncontended must not spin, help, or otherwise take a scenic route.
    m.Lock();
    Check(!m.Try_Lock(), "Lock() actually holds it");
    m.Unlock();
    Check(m.Try_Lock(),  "Unlock() actually releases it");
    m.Unlock();
}

// ---- 2. Contention between two bare threads -----------------------------------------------------
static void TestMutexContention() {
    std::printf("SchedulerMutex under contention\n");
    JLib::SchedulerMutex m;
    std::atomic<int> counter{ 0 };
    std::atomic<int> maxSeen{ 0 };

    // If the mutex ever admits two holders, `inside` exceeds 1 and maxSeen records it.
    std::atomic<int> inside{ 0 };
    auto body = [&] {
        for (int i = 0; i < 2000; ++i) {
            m.Lock();
            const int n = inside.fetch_add(1, std::memory_order_acq_rel) + 1;
            int prev = maxSeen.load(std::memory_order_relaxed);
            while (n > prev && !maxSeen.compare_exchange_weak(prev, n)) {}
            counter.fetch_add(1, std::memory_order_relaxed);
            inside.fetch_sub(1, std::memory_order_acq_rel);
            m.Unlock();
        }
    };

    std::thread a(body), b(body);
    a.join(); b.join();

    Check(counter.load() == 4000, "both threads completed every iteration");
    Check(maxSeen.load() == 1,    "never two holders at once");
}

// ---- 3. SchedulerSemaphore ----------------------------------------------------------------------
static void TestSemaphore() {
    std::printf("SchedulerSemaphore\n");
    JLib::SchedulerSemaphore sem(1);

    Check(sem.Try_Wait(),  "Try_Wait takes the only permit");
    Check(!sem.Try_Wait(), "Try_Wait fails when exhausted");
    sem.Signal();
    Check(sem.Try_Wait(),  "permit available again after Signal");
    sem.Signal();

    // Released by a DIFFERENT thread than took it. This is the case that makes permits unownable and
    // therefore untrackable, which is why ScopedPermit exists as an opt-in rather than Wait() doing
    // it -- so prove the untracked path still works.
    JLib::SchedulerSemaphore ps(0);
    std::atomic<bool> consumed{ false };
    std::thread consumer([&] { ps.Wait(); consumed.store(true, std::memory_order_release); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    Check(!consumed.load(std::memory_order_acquire), "consumer blocks while no permit exists");
    ps.Signal();
    consumer.join();
    Check(consumed.load(std::memory_order_acquire), "producer's Signal released the consumer");
}

// ---- 4. ScopedPermit ----------------------------------------------------------------------------
static void TestScopedPermit(JLib::TaskScheduler& sched) {
    std::printf("SchedulerSemaphore::ScopedPermit\n");
    static JLib::SchedulerSemaphore sem(1);

    {
        JLib::SchedulerSemaphore::ScopedPermit p(sem);
        Check(!sem.Try_Wait(), "holds the permit for its scope");
    }
    Check(sem.Try_Wait(), "released the permit on destruction");
    sem.Signal();

    // A leak or a double-release shows up here rather than as a mystery later.
    for (int i = 0; i < 1000; ++i) { JLib::SchedulerSemaphore::ScopedPermit p(sem); }
    Check(sem.Try_Wait(), "balanced across 1000 acquire/release cycles");
    sem.Signal();

    // From a FIBER. Ownership is counted for bare threads only, because a fiber can acquire on one
    // worker and resume on another; a fiber must therefore leave the count alone entirely.
    JLib::WaitGroup wg; wg.n.store(1, std::memory_order_relaxed);
    JLib::Task* t = sched.CreateTask(+[](void*) {
        JLib::SchedulerSemaphore::ScopedPermit p(sem);
    }, nullptr);
    t->waitGroup = &wg; sched.Push(t);
    sched.WaitFor(wg);
    Check(sem.Try_Wait(), "permit intact after a task used ScopedPermit");
    sem.Signal();
}

// ---- 5. THE REGRESSION TEST: self-deadlock by inversion -----------------------------------------
//
// This is the bug 1.2.3 fixed, and the reason this file exists. It FAILS on 1.2.2.
//
// A bare thread that blocks on a SchedulerMutex runs stolen noFiber tasks while it waits. So:
// hold A, queue tasks that want A, then contend on B. The waiting thread helps, runs a task that
// asks for A, and A is held by that same thread -- which is stuck inside the task. Nothing can
// release it. No fiber involved, and no lock-ordering discipline in the caller prevents it, because
// the scheduler chooses the interleaving.
//
// 1.2.3 stops a thread that OWNS a SchedulerMutex from helping at all, so it never runs the task and
// simply waits for B.
//
// Note the asymmetry: passing is deterministic, failing is probabilistic -- the broken version only
// hangs if the helper happens to steal one of the A-wanting tasks. Hence many of them and a long
// enough hold on B. A test that reproduces a real hang most of the time is worth far more than one
// that reproduces it never.
static JLib::SchedulerMutex g_A;
static std::atomic<int> g_tasksRun{ 0 };

static void WantsA(void*) {
    g_A.Lock();
    g_tasksRun.fetch_add(1, std::memory_order_relaxed);
    g_A.Unlock();
}

static void TestNoSelfDeadlock(JLib::TaskScheduler& sched) {
    std::printf("no self-deadlock when helping while holding a lock (1.2.3 regression)\n");

    JLib::SchedulerMutex B;
    constexpr int kTasks = 256;
    g_tasksRun.store(0, std::memory_order_relaxed);

    JLib::WaitGroup wg;
    wg.n.store(kTasks, std::memory_order_relaxed);

    // Someone else owns B for long enough that the main thread does a lot of waiting on it.
    std::atomic<bool> holderReady{ false };
    std::thread holder([&] {
        B.Lock();
        holderReady.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        B.Unlock();
    });
    while (!holderReady.load(std::memory_order_acquire)) std::this_thread::yield();

    g_A.Lock();                       // main now OWNS A
    for (int i = 0; i < kTasks; ++i) {
        JLib::Task* t = sched.CreateTask(WantsA, nullptr);
        if (!t) { Check(false, "CreateTask returned null"); g_A.Unlock(); holder.join(); return; }
        t->waitGroup = &wg;
        sched.Push(t);
    }

    B.Lock();                         // contended: pre-1.2.3 this helps, runs WantsA, and hangs
    Check(true, "acquired the second lock while holding the first");
    B.Unlock();
    g_A.Unlock();                     // release A so the queued tasks can proceed

    sched.WaitFor(wg);
    holder.join();

    Check(g_tasksRun.load(std::memory_order_relaxed) == kTasks,
          "every queued task that wanted the held lock completed");
}

int main() {
    StartWatchdog(30, "primitives test");

    JLib::TaskScheduler::Init(4);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    TestMutexBasics();
    TestMutexContention();
    TestSemaphore();
    TestScopedPermit(sched);
    TestNoSelfDeadlock(sched);

    sched.Join();
    g_done.store(true, std::memory_order_release);

    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
