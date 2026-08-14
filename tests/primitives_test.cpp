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
#include <cstring>
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

// PushArray: every index runs EXACTLY once, the chunk count is what was asked for, and the
// WaitGroup covers all of it. "Exactly once" is the property worth guarding -- the failure mode of
// a chunked submit API is an off-by-one at a chunk boundary that either skips the last item or runs
// it twice, and neither shows up in a timing benchmark. A per-index counter catches both directions
// at once, which a plain total would not.
static void TestPushArray(JLib::TaskScheduler& sched) {
    constexpr size_t kN = 1000;
    static std::atomic<int> visits[kN];
    for (size_t i = 0; i < kN; ++i) visits[i].store(0, std::memory_order_relaxed);

    JLib::WaitGroup wg;
    const size_t made = sched.PushArray(0, kN, 32, [](size_t i) {
        visits[i].fetch_add(1, std::memory_order_relaxed);
    }, &wg);
    sched.WaitFor(wg);

    Check(made == (kN + 31) / 32, "PushArray created ceil(n/chunk) tasks");
    bool once = true;
    for (size_t i = 0; i < kN; ++i)
        if (visits[i].load(std::memory_order_relaxed) != 1) { once = false; break; }
    Check(once, "every index ran exactly once (no gap, no duplicate)");

    // A chunk larger than the range must collapse to one task, not overrun the end.
    for (size_t i = 0; i < kN; ++i) visits[i].store(0, std::memory_order_relaxed);
    JLib::WaitGroup wg2;
    const size_t big = sched.PushArray(0, 10, 4096, [](size_t i) {
        visits[i].fetch_add(1, std::memory_order_relaxed);
    }, &wg2);
    sched.WaitFor(wg2);
    bool clipped = (big == 1) && visits[9].load(std::memory_order_relaxed) == 1
                              && visits[10].load(std::memory_order_relaxed) == 0;
    Check(clipped, "chunk larger than the range clips to one task and stops at end");

    // Degenerate inputs return 0 and submit nothing, rather than dividing by zero or looping.
    JLib::WaitGroup wg3;
    Check(sched.PushArray(5, 5, 8, [](size_t) {}, &wg3) == 0, "empty range creates no tasks");
    Check(sched.PushArray(9, 4, 8, [](size_t) {}, &wg3) == 0, "inverted range creates no tasks");

    // chunkSize 0 is treated as 1 rather than dividing by zero.
    for (size_t i = 0; i < kN; ++i) visits[i].store(0, std::memory_order_relaxed);
    JLib::WaitGroup wg4;
    const size_t z = sched.PushArray(0, 16, 0, [](size_t i) {
        visits[i].fetch_add(1, std::memory_order_relaxed);
    }, &wg4);
    sched.WaitFor(wg4);
    bool zok = (z == 16);
    for (size_t i = 0; i < 16 && zok; ++i)
        if (visits[i].load(std::memory_order_relaxed) != 1) zok = false;
    Check(zok, "chunkSize 0 behaves as 1");
}

// PushBatch spreads a large batch across workers instead of stacking it on one. The property that
// must hold regardless of how it segments: every task arrives exactly once. Segmenting relinks the
// `next` chain per run, so a bug here loses or duplicates whole segments -- the exact failure the
// old single-target version could not have.
static void TestPushBatchSpread(JLib::TaskScheduler& sched) {
    constexpr int kTasks = 5000;
    static std::atomic<int> ran{ 0 };
    ran.store(0, std::memory_order_relaxed);

    JLib::WaitGroup wg;
    wg.n.store(kTasks, std::memory_order_relaxed);
    std::vector<JLib::Task*> ts(kTasks);
    for (int i = 0; i < kTasks; ++i) {
        ts[i] = sched.CreateTask(+[](void*) { ran.fetch_add(1, std::memory_order_relaxed); }, nullptr);
        ts[i]->waitGroup = &wg;
    }
    sched.PushBatch(ts.data(), ts.size());
    sched.WaitFor(wg);

    Check(ran.load(std::memory_order_relaxed) == kTasks,
          "every task in a spread PushBatch ran exactly once");
}

// Pass "nosleep" to run the ENTIRE suite under IdlePolicy::NoSleep. Reusing every existing case
// under both policies beats writing one bespoke NoSleep test: the policy changes the worker's park
// path, which is exactly where the 1.2.0 lost wakeup lived, and the cases that would expose a
// regression there are the blocking ones already written -- contention, the semaphore handoff, and
// the spin-help deadlock guard. CI runs this binary twice, once per policy.
int main(int argc, char** argv) {
    const bool noSleep = (argc > 1) && std::strcmp(argv[1], "nosleep") == 0;
    std::printf("idle policy: %s\n\n", noSleep ? "nosleep" : "sleep");
    StartWatchdog(30, noSleep ? "primitives test (nosleep)" : "primitives test");

    if (noSleep) JLib::TaskScheduler::SetIdlePolicy(JLib::TaskScheduler::IdlePolicy::NoSleep);
    JLib::TaskScheduler::Init(4);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    TestMutexBasics();
    TestMutexContention();
    TestSemaphore();
    TestScopedPermit(sched);
    TestNoSelfDeadlock(sched);
    TestPushArray(sched);
    TestPushBatchSpread(sched);

    sched.Join();
    g_done.store(true, std::memory_order_release);

    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
