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
#include "LockFreeHashMap.h"
#include <TaskScheduler.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <thread>
#include <chrono>
#include <utility>   // std::pair -- the contention probe returns two counters
#include <functional>   // RunCursorRange takes std::function<void(int,int)>&

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
// A bare thread that blocks on a SchedulerMutex runs stolen Native tasks while it waits. So:
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

// ---- 8. THE FIBER CAP: more tasks blocked at once than there are fibers -----------------------
//
// A SUSPENDED task holds its fiber, so the number of tasks that may block SIMULTANEOUSLY is capped
// at the pool size (64 standard per worker). Past that, AcquireFiber returns null and the worker
// re-queues the task and yields rather than running it. That still makes progress -- a resumable
// task takes the existing-fiber path and needs no free fiber -- but it looks like a stall, and it
// cost eight minutes of a benchmark run to discover by accident.
//
// THE WATCHDOG IS THE ASSERTION. There is no clean threshold for "too slow" here, and inventing one
// would make the test flaky on a loaded machine. "Finishes at all, within longer than a person
// would wait" is the property that matters: over-subscribing the pool must degrade, not deadlock.
static void TestFiberCapOversubscribed(JLib::TaskScheduler& sched) {
    std::printf("fiber cap: more blocked tasks than fibers\n");

    // Init(4) below -> 4 * 64 = 256 standard fibers. Deliberately past it.
    constexpr int kBlocked = 320;
    static std::atomic<int> finished{ 0 };
    static std::atomic<bool> released{ false };
    finished.store(0, std::memory_order_relaxed);
    released.store(false, std::memory_order_relaxed);

    // One lookup, then the Event& overload -- no registry lock on the hot path.
    JLib::Event& ev = sched.GetEvent("captest");
    static JLib::Event* evp = &ev;

    JLib::WaitGroup wg;
    wg.n.store(kBlocked, std::memory_order_relaxed);
    for (int i = 0; i < kBlocked; ++i) {
        JLib::Task* t = sched.CreateTask(+[](void*) {
            JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
            // Armed, so a release that already landed is caught by self-signalling rather than
            // stranding this fiber -- the same shape the comparison harness uses.
            s.WaitOnEventArmed(*evp, [] {
                if (released.load(std::memory_order_acquire)) evp->SignalAll();
            });
            finished.fetch_add(1, std::memory_order_relaxed);
        }, nullptr, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { Check(false, "CreateTask returned null under fiber pressure"); return; }
        t->waitGroup = &wg;
        sched.Push(t);
    }

    // Let the pool saturate before releasing, so the over-subscribed state is actually entered.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    released.store(true, std::memory_order_release);
    ev.SignalAll();

    sched.WaitFor(wg);
    Check(finished.load(std::memory_order_relaxed) == kBlocked,
          "every task completed with more blocked than fibers");
}
// ---- 9. SchedulerMutex under FIBER contention ---------------------------------------------------
// Section 2 covers two BARE THREADS, which take a completely different path: they spin and help.
// A fiber SUSPENDS on contention instead, so none of section 2's coverage says anything about this
// one. Given the whole reason these primitives exist is to be fiber-aware, the suspend path being
// untested was the larger half of the gap.
// PROVES ITS OWN PREMISE, and it has to. The first version of this ran the locked loop and asserted
// "max one holder", which was measured to pass even with NO mutex at all in 1 of 3 runs -- with only
// two fibers on the pool they frequently just never overlap, and then the assertion is vacuous. A
// mutual-exclusion test that can pass while the mutex is broken is worse than no test, because it
// reports "ok".
//
// So it runs twice: an UNLOCKED probe that must observe overlap (otherwise the run is reported
// inconclusive rather than green), then the locked loop. Both start behind a rendezvous so the two
// fibers are actually resident at the same time instead of running back to back.
// File scope, not a local: it is used inside two nested lambdas, and MSVC (correctly, and more
// strictly than GCC) requires a constexpr local to be captured explicitly to be usable there.
// 200 by default, overridable via JLIB_TEST_CONTENTION_ITERS.
//
// When this test relied on LUCK to observe overlap, a big count was how it bought enough chances.
// Overlap is now FORCED on the first iteration, so the count does no statistical work -- it only
// multiplies contended Lock/Unlock round trips, each of which SUSPENDS and resumes a fiber.
//
// WHY IT IS A KNOB AND NOT JUST 200: at 2000 this blew the 30s watchdog on macOS arm64 -- but the
// SAME test at the SAME count had passed one commit earlier, so the failure is INTERMITTENT, and
// dropping to 200 may have reduced the odds of hitting a race by ~10x rather than fixing anything.
// The high count is the reproducer. CI hammers macOS with it (see .github/workflows/ci.yml) at a
// raised watchdog, which is what separates the two hypotheses:
//   completes every time, just slowly -> genuinely slow suspend/resume, nothing to fix
//   hangs intermittently              -> a race in the park/wake or mutex-waiter handshake, and
//                                        this project has shipped one of those before (1.2.0,
//                                        macOS arm64, ~1 run in 3, invisible on x86's TSO)
static int FiberContentionIters() {
    if (const char* e = std::getenv("JLIB_TEST_CONTENTION_ITERS")) {
        const int v = std::atoi(e);
        if (v > 0) return v;
    }
    return 200;
}
static const int kFiberContentionIters = FiberContentionIters();

// Changing IdlePolicy on a RUNNING pool -- allowed as of 1.3.6, and previously a data race.
//
// The real risk being tested is a lost wakeup at the transition. Flipping to NoSleep while workers
// are parked, and to Sleep while they are spinning, moves the whole pool across the park/wake
// handshake repeatedly and under load. If any of those transitions could strand a task, this hangs
// and the watchdog reports it -- which is the correct outcome, and the reason this does not simply
// assert on a counter and return.
//
// Deliberately NOT asserting that workers actually stopped parking: that is unobservable from
// outside without instrumentation that would perturb the thing being measured. What is asserted is
// the property that matters -- every task completes across every transition -- plus that the scoped
// guard restores exactly, including nested.
// ParallelFor: EXACTLY-ONCE coverage, which is the only property slice-stealing can plausibly get
// wrong. A cursor hands out [lo, lo+grain) to whichever worker asks next, so the failure modes are
// an element visited twice (two workers got overlapping slices) or never (the tail was dropped by
// an off-by-one). Both are silent in a benchmark -- a loop that skips 3 of 4 million elements just
// looks fast.
//
// Sizes are chosen so the last slice does NOT divide evenly, because that ragged tail is exactly
// where an off-by-one lives. A range that divides cleanly would pass with a broken clamp.
static void TestRangeCoverage(JLib::TaskScheduler& sched) {
    std::printf("ParallelFor exactly-once coverage\n");

    struct Case { int n; int grain; };
    const Case cases[] = {
        { 1000000, 64 },      // grain divides badly into the tail
        {  999983, 97 },      // prime-ish n, grain not a factor of anything
        {     100,  1 },      // grain below the internal floor of 64 -- must still cover exactly
        {       1, 64 },      // single element, grain far larger than the range
        {       0, 64 },      // empty range must call nothing and must not hang
    };

    for (const Case& c : cases) {
        std::vector<std::atomic<int>> visits(c.n > 0 ? c.n : 1);
        for (auto& v : visits) v.store(0, std::memory_order_relaxed);

        sched.ParallelFor(0, c.n, c.grain, [&visits](int lo, int hi) {
            for (int i = lo; i < hi; ++i)
                visits[i].fetch_add(1, std::memory_order_relaxed);
            });

        int missed = 0, doubled = 0;
        for (int i = 0; i < c.n; ++i) {
            const int v = visits[i].load(std::memory_order_relaxed);
            if (v == 0) ++missed;
            else if (v > 1) ++doubled;
        }
        char what[96];
        std::snprintf(what, sizeof(what), "n=%d grain=%d: every element exactly once", c.n, c.grain);
        Check(missed == 0 && doubled == 0, what);
        if (missed || doubled)
            std::printf("      missed %d, visited-twice %d\n", missed, doubled);
    }
}

// RunCursorRange is PUBLIC -- it is the shared-cursor mechanism ParallelFor falls back to when a
// second non-worker thread is already splitting, and the README documents calling it directly for
// very large uniform ranges (it measurably beats the recursive splitter above ~N=200,000). Despite
// that it had ZERO test coverage until this: nothing caught a regression here except a human
// running it by hand once, off to the side, to answer a question about whether it still worked
// after ParallelRange was removed. It did, that time. This makes sure the next time is automatic.
static void TestCursorRangeDirect(JLib::TaskScheduler& sched) {
    std::printf("RunCursorRange (direct call) exactly-once coverage\n");

    // Same shape of cases as TestRangeCoverage, so a reader who trusts one trusts the other:
    // ragged division, a prime-ish size, a grain below the internal floor, and both range-API
    // edge cases (single element, empty).
    struct Case { int n; int grain; };
    const Case cases[] = {
        { 1000000, 64 },
        {  999983, 97 },
        {     100,  1 },
        {       1, 64 },
        {       0, 64 },
    };

    for (const Case& c : cases) {
        std::vector<std::atomic<int>> visits(c.n > 0 ? c.n : 1);
        for (auto& v : visits) v.store(0, std::memory_order_relaxed);

        // BY REFERENCE, non-const, and a named lvalue -- RunCursorRange's signature does not accept
        // a temporary. That is a real usability wart (see the header comment on the declaration)
        // and this is also, incidentally, the test that would catch it changing silently.
        std::function<void(int, int)> body = [&visits](int lo, int hi) {
            for (int i = lo; i < hi; ++i)
                visits[i].fetch_add(1, std::memory_order_relaxed);
        };
        sched.RunCursorRange(0, c.n, c.grain, body);

        int missed = 0, doubled = 0;
        for (int i = 0; i < c.n; ++i) {
            const int v = visits[i].load(std::memory_order_relaxed);
            if (v == 0) ++missed;
            else if (v > 1) ++doubled;
        }
        char what[96];
        std::snprintf(what, sizeof(what), "n=%d grain=%d: every element exactly once", c.n, c.grain);
        Check(missed == 0 && doubled == 0, what);
        if (missed || doubled)
            std::printf("      missed %d, visited-twice %d\n", missed, doubled);
    }

    // Inverted range (end < start): TestRangeCoverage does not have this case because ParallelFor
    // is documented to treat it as empty via `end - begin <= 0`. RunCursorRange takes the same
    // contract but nothing had ever exercised the inverted direction specifically.
    {
        std::atomic<int> touched{ 0 };
        std::function<void(int, int)> body = [&touched](int, int) { touched.fetch_add(1, std::memory_order_relaxed); };
        sched.RunCursorRange(5, 3, 64, body);
        Check(touched.load() == 0, "inverted range (end < start): calls nothing");
    }
}

// The path production code actually takes: RunCursorRange is reached from ParallelFor only when a
// SECOND non-worker thread loses the race for the single shared lane (see NonWorkerLaneClaim).
// TestCursorRangeDirect proves the algorithm is correct in isolation; this proves it is correct
// under the real contention that puts it on the path at all.
static void TestCursorRangeFallback(JLib::TaskScheduler& sched) {
    std::printf("RunCursorRange fallback under real non-worker contention\n");

    const int n = 50000;
    std::vector<std::atomic<int>> visits(n);
    for (auto& v : visits) v.store(0, std::memory_order_relaxed);

    auto work = [&] {
        sched.ParallelFor(0, n, 64, [&visits](int lo, int hi) {
            for (int i = lo; i < hi; ++i)
                visits[i].fetch_add(1, std::memory_order_relaxed);
        });
    };
    // Two bare threads calling ParallelFor at once: exactly one holds the non-worker lane and
    // splits, the other is routed into RunCursorRange. Both must still cover their own full range.
    std::thread a(work), b(work);
    a.join();
    b.join();

    int missed = 0, wrong = 0;
    for (int i = 0; i < n; ++i) {
        const int v = visits[i].load(std::memory_order_relaxed);
        if (v == 0) ++missed;
        else if (v != 2) ++wrong;   // each of the two callers must touch every element once
    }
    Check(missed == 0 && wrong == 0, "two contending ParallelFor callers: every element touched by both, exactly once each");
    if (missed || wrong)
        std::printf("      missed %d, wrong-count %d\n", missed, wrong);
}

static void TestIdlePolicySwitchUnderLoad(JLib::TaskScheduler& sched) {
    std::printf("IdlePolicy change on a live pool\n");

    using IP = JLib::TaskScheduler::IdlePolicy;
    const IP entry = JLib::TaskScheduler::GetIdlePolicy();

    Check(JLib::TaskScheduler::GetIdlePolicy() == entry, "GetIdlePolicy round-trips");

    // Hammer the transition with work in flight. Each round parks or unparks the pool while tasks
    // are being pushed, so the flip lands in the middle of the handshake rather than between quiet
    // periods.
    constexpr int kRounds = 40;
    constexpr int kPerRound = 64;
    std::atomic<int> ran{ 0 };
    for (int r = 0; r < kRounds; ++r) {
        JLib::TaskScheduler::SetIdlePolicy((r & 1) ? IP::NoSleep : IP::Sleep);
        JLib::WaitGroup wg;
        wg.n.store(kPerRound, std::memory_order_relaxed);
        for (int i = 0; i < kPerRound; ++i) {
            JLib::Task* t = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); },
                                             false, JLib::FiberSize::Standard, JLib::TaskType::Native);
            if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);    // hangs here if a transition ever stranded one
    }
    // Restore explicitly. There is no RAII guard for this by design (see SetIdlePolicy's comment),
    // so the discipline it used to enforce is now the caller's -- including this test's, and a run
    // that left NoSleep set would silently tax every case after it.
    JLib::TaskScheduler::SetIdlePolicy(entry);

    Check(ran.load(std::memory_order_relaxed) <= kRounds * kPerRound, "no task ran twice");
    std::printf("  %d tasks across %d policy flips on a live pool          %s\n",
                ran.load(std::memory_order_relaxed), kRounds,
                ran.load(std::memory_order_relaxed) > 0 ? "ok" : "NOTHING RAN");
    Check(JLib::TaskScheduler::GetIdlePolicy() == entry, "policy restored after the loop");
}

static void TestMutexFiberContention(JLib::TaskScheduler& sched) {
    std::printf("mutex contention between two FIBERS (suspend path)\n");

    // useLock=false -> probe: how much overlap is there when nothing excludes them?
    // useLock=true  -> the real check.
    auto run = [&sched](bool useLock, JLib::SchedulerMutex* m) {
        std::atomic<int> arrived{ 0 }, inside{ 0 }, maxSeen{ 0 }, counter{ 0 };
        JLib::WaitGroup wg;
        wg.n.store(2, std::memory_order_relaxed);
        for (int t = 0; t < 2; ++t) {
            JLib::Task* task = sched.CreateTask([&arrived, &inside, &maxSeen, &counter, useLock, m] {
                // Rendezvous: don't start until BOTH fibers are running, so the loops overlap
                // instead of the first finishing before the second is scheduled. Bounded so a
                // starved pool degrades to a weaker test rather than a hang.
                // Bounded IN TIME, not in yields. A yield-count budget is not portable: a yield
                // costs wildly different amounts across Windows, Linux and macOS QoS scheduling, so
                // "1,000,000 yields" is a different timeout on every platform and unbounded on the
                // slowest. A deadline is the same everywhere.
                arrived.fetch_add(1, std::memory_order_acq_rel);
                {
                    const auto rvDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
                    while (arrived.load(std::memory_order_acquire) < 2 &&
                           std::chrono::steady_clock::now() < rvDeadline)
                        std::this_thread::yield();
                }

                for (int i = 0; i < kFiberContentionIters; ++i) {
                    if (useLock) m->Lock();
                    const int now = inside.fetch_add(1, std::memory_order_acq_rel) + 1;

                    // PROBE ONLY: hold the section open until the other fiber is also inside, so
                    // overlap is FORCED rather than hoped for. Without this the window is a few
                    // nanoseconds wide and whether anyone observes it depends on how many cores the
                    // machine has -- which is why this passed 5/5 locally on 32 threads and failed
                    // on a 2-vCPU CI runner. Bounded, so a machine that genuinely cannot run them
                    // concurrently falls through instead of hanging.
                    // FIRST ITERATION ONLY. The rendezvous above already proved both fibers are
                    // running, so one held section is enough to observe the overlap -- and bounding
                    // it here matters: forcing on every iteration would make whichever fiber
                    // finishes second spin the full budget on each of its remaining iterations,
                    // waiting for a partner that has already exited.
                    if (!useLock && i == 0) {
                        const auto ovDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
                        while (inside.load(std::memory_order_acquire) < 2 &&
                               std::chrono::steady_clock::now() < ovDeadline)
                            std::this_thread::yield();
                    }

                    int prev = maxSeen.load(std::memory_order_relaxed);
                    while (now > prev && !maxSeen.compare_exchange_weak(prev, now)) {}
                    counter.fetch_add(1, std::memory_order_relaxed);
                    inside.fetch_sub(1, std::memory_order_acq_rel);
                    if (useLock) m->Unlock();
                }
                }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            if (!task) return std::pair<int, int>{ -1, -1 };
            task->waitGroup = &wg;
            sched.Push(task);
        }
        sched.WaitFor(wg);
        return std::pair<int, int>{ maxSeen.load(), counter.load() };
    };

    const auto probe = run(false, nullptr);
    if (probe.first < 0) { Check(false, "CreateTask returned null"); return; }

    // NOT a Check(). A premise about achievable concurrency is a property of the MACHINE, not of the
    // code under test, so it must never fail the build: a 2-vCPU CI runner that cannot get two
    // fibers running at once has found nothing wrong with the mutex. It reports which of two things
    // the run below is worth -- proof, or a weaker "did not misbehave". This started life as a
    // Check() and failed exactly that way on CI while passing 5/5 on a 32-thread desktop.
    const bool overlapProven = probe.first > 1;
    std::printf("  %-58s %s\n",
        overlapProven ? "PREMISE: the two fibers genuinely overlap when unlocked"
                      : "PREMISE: could not force overlap on this machine",
        overlapProven ? "ok" : "INCONCLUSIVE (next check is weaker, not failed)");

    JLib::SchedulerMutex m;
    const auto locked = run(true, &m);
    if (locked.first < 0) { Check(false, "CreateTask returned null"); return; }

    Check(locked.second == 2 * kFiberContentionIters, "both fibers completed every iteration");
    Check(locked.first == 1, overlapProven ? "never two fiber holders at once (overlap proven)"
                                           : "never two fiber holders at once (overlap unproven)");
}

// File scope for the same reason as kFiberContentionIters above: used inside a lambda with an
// explicit capture list, which MSVC requires to be captured if it is a local.
static constexpr int kMixedContentionIters = 300;

// ---- 10. Mixed FIBER vs BARE-THREAD contention --------------------------------------------------
// The two paths have to interoperate on the SAME mutex: a suspending fiber and a spin-helping bare
// thread, each of which must see the other's ownership. Neither single-path test covers the handoff
// between them.
static void TestMutexMixedContention(JLib::TaskScheduler& sched) {
    std::printf("mutex contention between a fiber and a bare thread\n");

    JLib::SchedulerMutex m;
    std::atomic<int> inside{ 0 };
    std::atomic<int> maxSeen{ 0 };
    std::atomic<int> done{ 0 };

    auto body = [&m, &inside, &maxSeen, &done]() {
        for (int i = 0; i < kMixedContentionIters; ++i) {
            m.Lock();
            const int now = inside.fetch_add(1, std::memory_order_acq_rel) + 1;
            int prev = maxSeen.load(std::memory_order_relaxed);
            while (now > prev && !maxSeen.compare_exchange_weak(prev, now)) {}
            inside.fetch_sub(1, std::memory_order_acq_rel);
            m.Unlock();
        }
        done.fetch_add(1, std::memory_order_release);
    };

    JLib::WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);
    // `body` passed directly, as an lvalue. This did not compile until LambdaTask gained a const&
    // constructor -- see TestCreateTaskAcceptsNamedCallable below, which guards it.
    JLib::Task* t = sched.CreateTask(body, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
    if (!t) { Check(false, "CreateTask returned null"); return; }
    t->waitGroup = &wg;
    sched.Push(t);

    std::thread bare(body);          // same body, bare-thread path
    bare.join();
    sched.WaitFor(wg);

    Check(done.load(std::memory_order_acquire) == 2, "fiber and bare thread both finished");
    Check(maxSeen.load() == 1,                       "never two holders across the two paths");
}

// ---- 11. SchedulerConditionVariable on a fiber --------------------------------------------------
// Had NO tests at all, and it has the subtlest contract of the three: Wait must release the mutex
// while parked and re-acquire it before returning.
//
// The release is proved BY CONSTRUCTION rather than by an assert: main cannot take the mutex at all
// unless the waiting fiber gave it up inside Wait. If that ever regresses, main blocks forever and
// the watchdog reports it. The re-acquire is asserted directly, from the waiter.
static void TestConditionVariableFiber(JLib::TaskScheduler& sched) {
    std::printf("condition variable: fiber waits, mutex released while parked, Notify_One wakes it\n");

    JLib::SchedulerMutex m;
    JLib::SchedulerConditionVariable cv;
    bool ready = false;                       // guarded by m
    std::atomic<bool> inWait{ false };
    std::atomic<bool> heldAfterWake{ false };
    std::atomic<bool> finished{ false };

    JLib::WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);
    JLib::Task* waiter = sched.CreateTask([&] {
        m.Lock();
        inWait.store(true, std::memory_order_release);
        while (!ready) cv.Wait(m);            // predicate loop: the only correct way to use Wait
        // We are back and must own the mutex again. Try_Lock is non-recursive, so it failing here
        // means it is held -- and main released it before notifying, so the holder can only be us.
        heldAfterWake.store(!m.Try_Lock(), std::memory_order_release);
        m.Unlock();
        finished.store(true, std::memory_order_release);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
    if (!waiter) { Check(false, "CreateTask returned null"); return; }
    waiter->waitGroup = &wg;
    sched.Push(waiter);

    while (!inWait.load(std::memory_order_acquire)) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));   // let it actually park

    m.Lock();                                 // blocks unless Wait released it
    Check(true, "main acquired the mutex while the fiber was parked in Wait");
    ready = true;
    m.Unlock();
    cv.Notify_One();

    sched.WaitFor(wg);
    Check(finished.load(std::memory_order_acquire),      "Notify_One woke the parked fiber");
    Check(heldAfterWake.load(std::memory_order_acquire), "Wait re-acquired the mutex before returning");
}

// ---- 12. Notify_All wakes EVERY waiter ----------------------------------------------------------
// Notify_One draining the queue one entry at a time and Notify_All draining it fully are easy to
// confuse in an implementation; a single-waiter test passes under either.
static void TestConditionVariableNotifyAll(JLib::TaskScheduler& sched) {
    std::printf("condition variable: Notify_All releases every waiter\n");

    JLib::SchedulerMutex m;
    JLib::SchedulerConditionVariable cv;
    bool ready = false;                       // guarded by m
    constexpr int kWaiters = 8;
    std::atomic<int> parked{ 0 };
    std::atomic<int> woke{ 0 };

    JLib::WaitGroup wg;
    wg.n.store(kWaiters, std::memory_order_relaxed);
    for (int i = 0; i < kWaiters; ++i) {
        JLib::Task* t = sched.CreateTask([&] {
            m.Lock();
            parked.fetch_add(1, std::memory_order_release);
            while (!ready) cv.Wait(m);
            m.Unlock();
            woke.fetch_add(1, std::memory_order_release);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { Check(false, "CreateTask returned null"); return; }
        t->waitGroup = &wg;
        sched.Push(t);
    }

    while (parked.load(std::memory_order_acquire) < kWaiters) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    m.Lock();
    ready = true;
    m.Unlock();
    cv.Notify_All();

    sched.WaitFor(wg);
    Check(woke.load(std::memory_order_acquire) == kWaiters, "all 8 waiters resumed after one Notify_All");
}

// ---- 13. The bare-thread Wait() CONTRACT --------------------------------------------------------
// Worth pinning down because it surprises people: on a bare thread Wait() does NOT wait for a
// notification. It unlocks, takes one spin-help step, and re-locks -- so it returns promptly whether
// or not anyone signalled, and only a caller that re-checks its predicate in a loop is correct.
// (Fibers park properly; see test 11. The asymmetry is the point.) A caller that wrote
// `if (!ready) cv.Wait(m);` would be broken on a bare thread and fine on a fiber, which is exactly
// the kind of bug that survives review.
static void TestConditionVariableBareThreadContract() {
    std::printf("condition variable: bare-thread Wait() returns without a notification\n");

    JLib::SchedulerMutex m;
    JLib::SchedulerConditionVariable cv;

    m.Lock();
    const auto t0 = std::chrono::steady_clock::now();
    cv.Wait(m);                               // nothing will ever notify this
    const double ms = std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - t0).count();
    m.Unlock();

    Check(ms < 1000.0, "returns promptly with no notification (predicate loop REQUIRED)");

    // And the loop form still makes progress once another thread sets the predicate.
    std::atomic<bool> ready{ false };
    std::atomic<bool> got{ false };
    std::thread setter([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        ready.store(true, std::memory_order_release);
        cv.Notify_All();
        });
    m.Lock();
    while (!ready.load(std::memory_order_acquire)) cv.Wait(m);
    got.store(true, std::memory_order_release);
    m.Unlock();
    setter.join();
    Check(got.load(std::memory_order_acquire), "predicate loop makes progress once the predicate holds");
}

// ---- 14. A permit returned by a DIFFERENT thread than took it -----------------------------------
// A semaphore is not a lock and deliberately tracks no ownership, which is what lets it be used as a
// producer/consumer signal. That is a design decision worth a test, because "helpfully" adding an
// ownership check later would look like a hardening improvement and would break this.
static void TestSemaphoreCrossThreadRelease() {
    std::printf("semaphore: a permit may be returned by a thread that never took it\n");

    JLib::SchedulerSemaphore sem(1, 4);

    std::atomic<bool> took{ false };
    std::thread taker([&] { sem.Wait(); took.store(true, std::memory_order_release); });
    taker.join();
    Check(took.load(std::memory_order_acquire), "one thread took the only permit");
    Check(!sem.Try_Wait(),                      "the permit is genuinely gone");

    std::thread giver([&] { sem.Signal(); });   // never held it
    giver.join();
    Check(sem.Try_Wait(), "a permit released by a different thread is usable");
}

// ---- 15. CreateTask accepts a NAMED callable, not just a temporary -------------------------------
// Regression guard. LambdaTask's F is already decayed by CreateTask, so its `F&&` constructor is a
// plain rvalue reference; before the const& overload was added, passing a named lambda was a compile
// error. This test is mostly a COMPILE-TIME assertion -- if the overload is ever removed, this file
// stops building, which is the loudest possible failure and exactly what is wanted for an API shape.
// The runtime checks confirm the copy actually carried the captures.
static void TestCreateTaskAcceptsNamedCallable(JLib::TaskScheduler& sched) {
    std::printf("CreateTask accepts a named (lvalue) callable\n");

    std::atomic<int> ran{ 0 };
    const int captured = 41;
    auto named = [&ran, captured] { ran.fetch_add(captured + 1, std::memory_order_relaxed); };

    JLib::WaitGroup wg;
    wg.n.store(2, std::memory_order_relaxed);

    JLib::Task* a = sched.CreateTask(named);              // lvalue -> copies
    JLib::Task* b = sched.CreateTask(std::move(named));   // rvalue -> still moves, as before
    if (!a || !b) { Check(false, "CreateTask returned null"); return; }
    a->waitGroup = &wg; b->waitGroup = &wg;
    sched.Push(a); sched.Push(b);
    sched.WaitFor(wg);

    Check(ran.load(std::memory_order_relaxed) == 84, "both the copied and the moved callable ran with captures intact");
}

// Event::SignalOne -- wake exactly one waiter. The operation the intrusive Treiber stack could not
// support at all: its links were the tasks, so no single waiter could be removed. The fiber-indexed
// table has no such constraint, and this is what the change bought.
static void TestEventSignalOne(JLib::TaskScheduler& sched) {
    std::printf("Event::SignalOne\n");

    JLib::Event& ev = sched.GetEvent("signalone-probe");
    static JLib::Event* evp = &ev;
    static std::atomic<int> woke{ 0 };
    static std::atomic<int> parked{ 0 };
    woke.store(0, std::memory_order_relaxed);
    parked.store(0, std::memory_order_relaxed);

    Check(!ev.SignalOne(), "SignalOne on an event with no waiters reports nothing woken");

    constexpr int kN = 8;
    JLib::WaitGroup wg;
    wg.n.store(kN, std::memory_order_relaxed);
    for (int i = 0; i < kN; ++i) {
        JLib::Task* t = sched.CreateTask(+[](void*) {
            JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
            parked.fetch_add(1, std::memory_order_relaxed);
            s.WaitOnEvent(*evp);
            woke.fetch_add(1, std::memory_order_relaxed);
        }, nullptr, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { Check(false, "CreateTask returned null"); return; }
        t->waitGroup = &wg;
        sched.Push(t);
    }

    // Wait for all of them to be genuinely parked, not merely started.
    for (int spin = 0; spin < 400 && parked.load(std::memory_order_acquire) < kN; ++spin)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    Check(parked.load() == kN, "all waiters parked");

    // ONE waiter, not all of them. This is the check the whole redesign exists for.
    Check(ev.SignalOne(), "SignalOne found a waiter");
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    Check(woke.load() == 1, "exactly one waiter woke -- the rest are still parked");

    // Drain the remainder one at a time; each call must take exactly one.
    for (int i = 1; i < kN; ++i) {
        const bool got = ev.SignalOne();
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        if (!got) { Check(false, "SignalOne failed to find a remaining waiter"); break; }
    }
    sched.WaitFor(wg);
    Check(woke.load() == kN, "repeated SignalOne drained every waiter exactly once");
    Check(!ev.SignalOne(), "the event is empty again afterwards");
}

// Concurrent SignalOne from several threads at once. The model says the slot exchange is what makes
// a wake exclusive, not the bit claim, so this is the behavioural counterpart to that: N signallers
// racing over N waiters must wake each waiter exactly once and never twice.
static void TestEventSignalOneConcurrent(JLib::TaskScheduler& sched) {
    std::printf("Event::SignalOne under concurrent signallers\n");

    JLib::Event& ev = sched.GetEvent("signalone-race");
    static JLib::Event* evp2 = &ev;
    static std::atomic<int> woke2{ 0 };
    static std::atomic<int> parked2{ 0 };
    woke2.store(0, std::memory_order_relaxed);
    parked2.store(0, std::memory_order_relaxed);

    constexpr int kN = 16;
    JLib::WaitGroup wg;
    wg.n.store(kN, std::memory_order_relaxed);
    for (int i = 0; i < kN; ++i) {
        JLib::Task* t = sched.CreateTask(+[](void*) {
            JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
            parked2.fetch_add(1, std::memory_order_relaxed);
            s.WaitOnEvent(*evp2);
            woke2.fetch_add(1, std::memory_order_relaxed);
        }, nullptr, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { Check(false, "CreateTask returned null"); return; }
        t->waitGroup = &wg;
        sched.Push(t);
    }
    for (int spin = 0; spin < 400 && parked2.load(std::memory_order_acquire) < kN; ++spin)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    std::this_thread::sleep_for(std::chrono::milliseconds(120));

    // More signallers than waiters, all at once: the surplus must come back empty rather than
    // double-waking anyone.
    std::atomic<int> succeeded{ 0 };
    std::vector<std::thread> signallers;
    for (int i = 0; i < 4; ++i) {
        signallers.emplace_back([&] {
            for (int k = 0; k < kN; ++k)
                if (ev.SignalOne()) succeeded.fetch_add(1, std::memory_order_relaxed);
        });
    }
    for (auto& th : signallers) th.join();

    sched.WaitFor(wg);
    Check(succeeded.load() == kN, "successful SignalOne calls exactly equal the waiter count");
    Check(woke2.load() == kN, "every waiter woke exactly once under concurrent signallers");
}

// LockFreeHashMap -- a general-purpose lock-free map, no longer used by the scheduler itself.
// These checks lived in the DAG cancellation suite while Event indexed its waiters with this
// map; Event now uses a flat fiber-indexed table, so they belong here with the other
// standalone container and primitive tests instead of in a suite about DAG cancellation.
static void TestLockFreeHashMap(JLib::TaskScheduler& sched) {
    (void)sched;
    // Keys here are Task* values from a slab of 64-byte slots, so they are 64-BYTE ALIGNED AND
    // CONSECUTIVE -- exactly the input a hash that does not mix will collapse onto one bucket.
    // does not mix will collapse onto a single bucket. Nothing about the scheduler's behaviour
    // would change if that happened, which is why it gets a direct check rather than being left
    // to the sections below to notice.
    std::printf("hash spreads slab-aligned keys\n");
    {
        JLib::LockFreeHashMap<int> m(*JLib::TaskScheduler::Instance().GetAllocator(), 16);
        std::vector<int> hits(m.buckets_count(), 0);
        const uintptr_t base = 0x7ff600004000ull;      // a plausible slab base
        for (int i = 0; i < 512; ++i) ++hits[m.bucket_index(base + uintptr_t(i) * 64)];

        size_t used = 0, worst = 0;
        for (int h : hits) { if (h) ++used; if (size_t(h) > worst) worst = size_t(h); }
        Check(used == m.buckets_count(), "every bucket receives slab-aligned keys");
        // Even would be 32 per bucket. Unmixed would be all 512 in one.
        Check(worst <= 64, "no bucket takes a disproportionate share");
    }

    // Sizing. The bound on waiters is the TOTAL fiber budget, so these are the numbers that decide
    // whether chains stay short on a big machine -- getting this wrong is invisible at runtime.
    std::printf("index sizing follows the total fiber budget\n");
    {
        using Map = JLib::LockFreeHashMap<int>;
        Check(Map::SuggestBuckets(64 * 4)  == 32,  "4 workers x 64 fibers -> 32 buckets (chain ~8)");
        Check(Map::SuggestBuckets(64 * 16) == 128, "16 workers x 64 fibers -> 128 buckets (chain ~8)");
        Check(Map::SuggestBuckets(8)       == 16,  "tiny bound still gets the 16-bucket floor");
        Check(Map::SuggestBuckets(1u << 20) == 512, "huge bound is capped at the 512 ceiling");
    }

    // Lazy bucket creation is the new concurrent code: many threads racing to publish the FIRST
    // bucket, where every loser must free the list it built. A leak here is silent, and a
    // double-publish would lose entries outright.
    std::printf("concurrent first-insert into one bucket\n");
    {
        JLib::LockFreeHashMap<int> m(*JLib::TaskScheduler::Instance().GetAllocator(), 64);
        const int kThreads = 8, kPer = 64;

        // All keys chosen to land in ONE bucket, so every thread races on the same slot.
        const size_t target = m.bucket_index(0x1000);
        std::vector<uint64_t> keys;
        for (uint64_t k = 0x1000; keys.size() < size_t(kThreads * kPer); k += 8)
            if (m.bucket_index(k) == target) keys.push_back(k);

        std::vector<std::thread> ts;
        std::atomic<int> added{ 0 };
        for (int t = 0; t < kThreads; ++t) {
            ts.emplace_back([&, t] {
                for (int i = 0; i < kPer; ++i)
                    if (m.add(keys[size_t(t * kPer + i)], t * kPer + i))
                        added.fetch_add(1, std::memory_order_relaxed);
            });
        }
        for (auto& th : ts) th.join();

        Check(added.load() == kThreads * kPer, "every racing insert landed");
        int seen = 0;
        m.for_each([&](int) { ++seen; });
        Check(seen == kThreads * kPer, "and every one is reachable afterwards");

        int stillThere = 0;
        for (uint64_t k : keys) if (m.contains(k)) ++stillThere;
        Check(stillThere == kThreads * kPer, "all keys found by lookup");
    }
}

int main(int argc, char** argv) {
    const bool noSleep = (argc > 1) && std::strcmp(argv[1], "nosleep") == 0;
    // "noreclaim" runs the WHOLE suite with worker self-triggered reclamation disabled, the way an
    // application that ticks EpochManager from its own idle path would run.
    //
    // Set here, BEFORE Init, rather than flipped inside a test -- because SetSelfReclaim's contract
    // is init-only (it is a plain bool read by every worker). A test that flipped it on a live pool
    // would be racing the very readers it is meant to exercise, and would be testing something the
    // API does not promise. Reusing the whole suite is also better coverage than one bespoke case:
    // every retire path in every test runs with the counter disabled.
    const bool noReclaim = (argc > 1) && std::strcmp(argv[1], "noreclaim") == 0;
    if (noReclaim) JLib::EpochManager::Instance().SetSelfReclaim(false);
    std::printf("idle policy: %s\n\n", noSleep ? "nosleep" : "sleep");
    // 30s default, overridable via JLIB_TEST_WATCHDOG_SECS. Raising it is what lets a high-iteration
    // reproducer distinguish SLOW from STUCK: if the run completes in 60s it was merely slow, and if
    // it never completes it is a hang no matter how long you wait.
    int watchdogSecs = 30;
    if (const char* w = std::getenv("JLIB_TEST_WATCHDOG_SECS")) {
        const int v = std::atoi(w);
        if (v > 0) watchdogSecs = v;
    }
    StartWatchdog(watchdogSecs, noSleep ? "primitives test (nosleep)" : "primitives test");

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
    TestFiberCapOversubscribed(sched);
    // Only meaningful in the "noreclaim" run; a no-op otherwise. Placed AFTER the tests above so
    // real retire traffic has already happened -- asserting the counter is zero before anything has
    // retired would pass for the wrong reason.
    if (noReclaim) {
        auto& em = JLib::EpochManager::Instance();
        std::printf("self-reclaim disabled (noreclaim)\n");
        Check(!em.SelfReclaimEnabled(), "flag actually took");
        Check(!em.ShouldSelfReclaim(), "workers will not self-trigger");
        // The point of the flag: RetirePtr skips its fetch_add entirely, so the counter must still
        // read zero after a suite's worth of retirements.
        Check(em.RetiredCount() == 0, "no counter traffic while disabled");
        // And a MANUAL tick must be safe. The decrement in TryReclaim is guarded symmetrically with
        // the increment; unguarded it would wrap size_t here, since nothing ever incremented.
        em.Tick();
        Check(em.RetiredCount() == 0, "manual Tick did not underflow the counter");
    }

    TestRangeCoverage(sched);
    TestCursorRangeDirect(sched);
    TestCursorRangeFallback(sched);
    TestIdlePolicySwitchUnderLoad(sched);
    TestMutexFiberContention(sched);
    TestMutexMixedContention(sched);
    TestConditionVariableFiber(sched);
    TestConditionVariableNotifyAll(sched);
    TestConditionVariableBareThreadContract();
    TestSemaphoreCrossThreadRelease();
    TestCreateTaskAcceptsNamedCallable(sched);
    TestEventSignalOne(sched);
    TestEventSignalOneConcurrent(sched);
    TestLockFreeHashMap(sched);

    sched.Join();
    g_done.store(true, std::memory_order_release);

    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
