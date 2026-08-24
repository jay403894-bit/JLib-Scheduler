// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// The third execution mode. Proves a C++20 coroutine can be scheduled on the pool, suspend, resume
// on a different worker, and complete a WaitGroup -- with the slab balanced afterwards.
//
// THE SLAB CHECK IS THE POINT, not a formality. Coroutine tasks are the one kind the worker does not
// free (see the ownership note in Coroutine.h), so every leak or double-free this design could have
// shows up as a slot imbalance and as nothing else. A test that only checked "did the body run"
// would pass just as happily while leaking a slot per coroutine or corrupting the free list.

#include "TaskScheduler.h"
#include "Coroutine.h"
#include "TaskDAG.h"   // only THIS test needs both; Coroutine.h does not include TaskDAG.h

#include <atomic>
#include <cstdio>
#include <string>
#include <thread>
#include <unordered_set>
#include <stdexcept>
#include <vector>

static int g_failures = 0;

static void Check(bool cond, const char* what) {
    std::printf("  %-64s %s\n", what, cond ? "ok" : "FAILED");
    if (!cond) ++g_failures;
}

static std::atomic<int> g_ran{ 0 };
static std::atomic<int> g_resumedElsewhere{ 0 };
static std::atomic<int> g_bodyOrder{ 0 };

// Suspends once in the middle, so the halves can land on different workers.
static JLib::Coro Halves(std::atomic<int>* firstHalf, std::atomic<int>* secondHalf) {
    const auto before = std::this_thread::get_id();
    firstHalf->fetch_add(1, std::memory_order_relaxed);

    co_await JLib::Reschedule{};

    if (std::this_thread::get_id() != before)
        g_resumedElsewhere.fetch_add(1, std::memory_order_relaxed);
    secondHalf->fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

// Suspends many times -- the re-push path is where a double free would surface.
static JLib::Coro Yielding(int times, std::atomic<int>* counter) {
    for (int i = 0; i < times; ++i) {
        counter->fetch_add(1, std::memory_order_relaxed);
        co_await JLib::Reschedule{};
    }
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

// Never suspends at all: completes inside its very first resume, which is the path where the
// worker's "task may already be dangling" rule actually bites.
static JLib::Coro Immediate(std::atomic<int>* counter) {
    counter->fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

static std::atomic<int> g_lazyStarted{ 0 };
static std::atomic<int> g_voidRan{ 0 };

static JLib::Lazy<int> Doubled(int n) {
    g_lazyStarted.fetch_add(1, std::memory_order_relaxed);
    co_await JLib::Reschedule{};
    co_return n * 2;
}

static JLib::Lazy<int> Nested(int n) {
    const int once = co_await Doubled(n);
    co_return co_await Doubled(once);
}

static JLib::Lazy<void> VoidLazy() {
    co_await JLib::Reschedule{};
    g_voidRan.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

static JLib::Lazy<int> Thrower() {
    co_await JLib::Reschedule{};
    throw std::runtime_error("from inside a Lazy");
    co_return 0;   // unreachable
}

// The catch lives at the co_await, which is the whole point of storing the exception_ptr in the
// promise rather than letting it escape through resume().
static JLib::Lazy<int> CatchesThrower(bool* caught) {
    try {
        co_return co_await Thrower();
    }
    catch (const std::runtime_error&) {
        *caught = true;
        co_return -1;
    }
}

// Recurses through N awaits. With symmetric transfer this is O(1) machine stack regardless of depth.
static JLib::Lazy<long long> Chain(int n) {
    if (n == 0) co_return 0;
    co_return 1 + co_await Chain(n - 1);
}

// Suspends once, so the node is completed from a body that genuinely migrated workers rather than
// one that ran straight through.
static JLib::Coro NodeWork(std::atomic<int>* counter) {
    counter->fetch_add(1, std::memory_order_relaxed);
    co_await JLib::Reschedule{};
    co_return;
}

// Shared state for the superset-primitive tests. A struct rather than captures because the
// coroutine bodies below MUST be named functions -- see the note on them.
struct SupersetFixture {
    JLib::SchedulerMutex m;
    JLib::SchedulerSemaphore sem{ 3 };
    std::atomic<int> overlap{ 0 }, maxOverlap{ 0 };
    std::atomic<int> coroHits{ 0 }, fiberHits{ 0 }, threadHits{ 0 };
    std::atomic<int> inside{ 0 }, maxInside{ 0 }, semDone{ 0 };

    void Enter(std::atomic<int>& cur, std::atomic<int>& peak) {
        const int now = cur.fetch_add(1, std::memory_order_acq_rel) + 1;
        int prev = peak.load(std::memory_order_relaxed);
        while (now > prev && !peak.compare_exchange_weak(prev, now)) {}
    }
};

// NAMED FUNCTIONS, NOT LAMBDAS, and this is not style. A lambda-coroutine's frame does not own its
// closure -- it stores a `this` pointer to it -- so a temporary `[&]{...}()` lambda passed to Spawn
// is destroyed at the end of that full expression while the coroutine is still suspended, and every
// capture it reads afterwards is a dangling reference. An earlier version of this test did exactly
// that and passed by luck until unrelated timing changed. Parameters of a named coroutine ARE copied
// into the frame, so a pointer parameter is safe.
static JLib::Coro MutexCoro(SupersetFixture* f, int iters) {
    for (int k = 0; k < iters; ++k) {
        co_await JLib::LockAsync(f->m);
        f->Enter(f->overlap, f->maxOverlap);
        f->coroHits.fetch_add(1, std::memory_order_relaxed);
        f->overlap.fetch_sub(1, std::memory_order_acq_rel);
        f->m.Unlock();
    }
}

static JLib::Coro SemaphoreCoro(SupersetFixture* f, int iters) {
    for (int k = 0; k < iters; ++k) {
        co_await JLib::AcquireAsync(f->sem);
        f->Enter(f->inside, f->maxInside);
        f->inside.fetch_sub(1, std::memory_order_acq_rel);
        f->sem.Signal();
        f->semDone.fetch_add(1, std::memory_order_relaxed);
    }
}

// Ordering within one coroutine must hold even though it migrates between workers.
static JLib::Coro Ordered(std::atomic<bool>* wrongOrder) {
    const int a = g_bodyOrder.fetch_add(1, std::memory_order_relaxed);
    co_await JLib::Reschedule{};
    const int b = g_bodyOrder.fetch_add(1, std::memory_order_relaxed);
    if (b <= a) wrongOrder->store(true, std::memory_order_relaxed);
    co_return;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    // Small on purpose -- see the slab-accounting section. The default 1M slots would swallow any
    // leak this test could produce, so the test would pass whether or not the design is correct.
    JLib::TaskScheduler::SetTaskSlabSize(4096);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("coroutine mode -- workers=%zu\n\n", sched.GetWorkerCount());

    // ---- a coroutine runs at all, and does not run before it is scheduled ------------------
    std::printf("scheduling\n");
    {
        std::atomic<int> first{ 0 }, second{ 0 };
        g_ran.store(0);

        JLib::Coro c = Halves(&first, &second);
        // initial_suspend is suspend_always, so constructing the Coro must not have run any of it.
        Check(first.load() == 0 && second.load() == 0,
              "constructing a Coro runs none of the body (initial_suspend)");

        JLib::WaitGroup wg;
        const bool spawned = JLib::Spawn(std::move(c), &wg);
        Check(spawned, "Spawn accepted the coroutine");
        sched.WaitFor(wg);

        Check(first.load() == 1, "the half before co_await ran exactly once");
        Check(second.load() == 1, "the half after co_await ran exactly once");
        Check(g_ran.load() == 1, "the coroutine reached its end");
    }

    // ---- a coroutine that never suspends ---------------------------------------------------
    std::printf("completes inside the first resume\n");
    {
        std::atomic<int> n{ 0 };
        g_ran.store(0);
        JLib::WaitGroup wg;
        for (int i = 0; i < 64; ++i) JLib::Spawn(Immediate(&n), &wg);
        sched.WaitFor(wg);
        Check(n.load() == 64, "all 64 non-suspending coroutines ran");
        Check(g_ran.load() == 64, "all 64 reached their end");
    }

    // ---- many suspensions, many coroutines --------------------------------------------------
    std::printf("repeated suspension\n");
    {
        std::atomic<int> hits{ 0 };
        g_ran.store(0);
        JLib::WaitGroup wg;
        const int kCoros = 128, kYields = 16;
        for (int i = 0; i < kCoros; ++i) JLib::Spawn(Yielding(kYields, &hits), &wg);
        sched.WaitFor(wg);
        Check(hits.load() == kCoros * kYields, "every suspension resumed exactly once");
        Check(g_ran.load() == kCoros, "every coroutine reached its end");
    }

    // ---- WaitFor really waits ----------------------------------------------------------------
    std::printf("WaitGroup completion\n");
    {
        std::atomic<int> hits{ 0 };
        JLib::WaitGroup wg;
        for (int i = 0; i < 32; ++i) JLib::Spawn(Yielding(8, &hits), &wg);
        sched.WaitFor(wg);
        // If the group were signalled before the bodies finished, this would be short.
        Check(hits.load() == 32 * 8, "WaitFor returned only after every body completed");
        Check((wg.n.load() & JLib::WaitGroup::COUNT_MASK) == 0, "the group drained to zero");
    }

    // ---- per-coroutine ordering survives migration -------------------------------------------
    std::printf("ordering across a suspension\n");
    {
        std::atomic<bool> wrong{ false };
        g_bodyOrder.store(0);
        JLib::WaitGroup wg;
        for (int i = 0; i < 64; ++i) JLib::Spawn(Ordered(&wrong), &wg);
        sched.WaitFor(wg);
        Check(!wrong.load(), "the second half of each body ran after its own first half");
    }

    // ---- THE ONE THAT MATTERS: slab accounting -----------------------------------------------
    // Coroutine tasks are freed by the coroutine, never by the worker, so a leak or a double free
    // is invisible in every check above. The allocator exposes no live count, so this measures it
    // the way the slab itself would notice: run far MORE coroutines through it than it has slots.
    // With kSlabSlots slots and kTotal >> kSlabSlots, a single leaked slot per coroutine exhausts
    // the slab long before the end and CreateTask starts returning nullptr, which Spawn reports.
    // A double free corrupts the free list instead, and hands the same slot out twice -- which the
    // exactly-once counters catch.
    std::printf("slab accounting (leaks and double frees live here)\n");
    {
        std::atomic<int> hits{ 0 };
        int spawnFailures = 0;
        const int kBatches = 200, kPerBatch = 64;          // 12,800 coroutines through 4,096 slots
        for (int b = 0; b < kBatches; ++b) {
            JLib::WaitGroup wg;
            for (int i = 0; i < kPerBatch; ++i)
                if (!JLib::Spawn(Yielding(3, &hits), &wg)) ++spawnFailures;
            sched.WaitFor(wg);
        }
        Check(spawnFailures == 0,
              "12,800 coroutines through a 4,096-slot slab: no allocation ever failed");
        Check(hits.load() == kBatches * kPerBatch * 3,
              "every suspension of every coroutine resumed exactly once");
        if (spawnFailures) std::printf("      %d Spawn calls failed -- slab exhausted\n", spawnFailures);
    }

    // ---- an un-spawned Coro must not leak its frame ------------------------------------------
    std::printf("un-spawned coroutine\n");
    {
        std::atomic<int> n{ 0 };
        {
            JLib::Coro c = Immediate(&n);   // created, never spawned, destroyed by ~Coro
        }
        Check(n.load() == 0, "an un-spawned coroutine never runs its body");
    }

    // ---- superset mutex: all three contexts on ONE lock --------------------------------------
    // The point is not that each kind works alone -- it is that they contend with each other on the
    // same object. `overlap` proves the critical section is genuinely exclusive: it is incremented
    // on entry and decremented on exit, so any value above 1 observed inside means two contexts held
    // the lock simultaneously.
    std::printf("superset mutex (coroutines + fibers + bare threads, same lock)\n");
    SupersetFixture fx;
    {
        const int kIters = 200;
        JLib::WaitGroup coroWg;
        for (int i = 0; i < 8; ++i) JLib::Spawn(MutexCoro(&fx, kIters), &coroWg);

        // fibers -- suspend inside Lock() on the very same mutex. These CAN be lambdas: CreateTask
        // copies the closure into the task's own storage, so it is not a dangling temporary.
        JLib::WaitGroup fiberWg;
        fiberWg.n.store(4, std::memory_order_relaxed);
        for (int i = 0; i < 4; ++i) {
            auto* t = sched.CreateTask([&fx, kIters] {
                for (int k = 0; k < kIters; ++k) {
                    fx.m.Lock();
                    fx.Enter(fx.overlap, fx.maxOverlap);
                    fx.fiberHits.fetch_add(1, std::memory_order_relaxed);
                    fx.overlap.fetch_sub(1, std::memory_order_acq_rel);
                    fx.m.Unlock();
                }
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->waitGroup = &fiberWg;
            sched.Push(t);
        }

        // bare threads -- these never queue; they spin-and-help until the lock frees
        std::vector<std::thread> threads;
        for (int i = 0; i < 3; ++i) {
            threads.emplace_back([&fx, kIters] {
                for (int k = 0; k < kIters; ++k) {
                    fx.m.Lock();
                    fx.Enter(fx.overlap, fx.maxOverlap);
                    fx.threadHits.fetch_add(1, std::memory_order_relaxed);
                    fx.overlap.fetch_sub(1, std::memory_order_acq_rel);
                    fx.m.Unlock();
                }
            });
        }

        sched.WaitFor(coroWg);
        sched.WaitFor(fiberWg);
        for (auto& th : threads) th.join();

        Check(fx.coroHits.load()   == 8 * kIters, "every coroutine acquisition completed");
        Check(fx.fiberHits.load()  == 4 * kIters, "every fiber acquisition completed");
        Check(fx.threadHits.load() == 3 * kIters, "every bare-thread acquisition completed");
        Check(fx.maxOverlap.load() == 1, "never two holders at once across all three context kinds");
        if (fx.maxOverlap.load() != 1)
            std::printf("      max simultaneous holders observed: %d\n", fx.maxOverlap.load());
    }

    // ---- superset semaphore ------------------------------------------------------------------
    std::printf("superset semaphore (permit count is the invariant)\n");
    {
        const int kPermits = 3;   // matches SupersetFixture::sem
        const int kIters = 100;
        JLib::WaitGroup coroWg;
        for (int i = 0; i < 8; ++i) JLib::Spawn(SemaphoreCoro(&fx, kIters), &coroWg);

        JLib::WaitGroup fiberWg;
        fiberWg.n.store(4, std::memory_order_relaxed);
        for (int i = 0; i < 4; ++i) {
            auto* t = sched.CreateTask([&fx, kIters] {
                for (int k = 0; k < kIters; ++k) {
                    fx.sem.Wait();
                    fx.Enter(fx.inside, fx.maxInside);
                    fx.inside.fetch_sub(1, std::memory_order_acq_rel);
                    fx.sem.Signal();
                    fx.semDone.fetch_add(1, std::memory_order_relaxed);
                }
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->waitGroup = &fiberWg;
            sched.Push(t);
        }

        sched.WaitFor(coroWg);
        sched.WaitFor(fiberWg);

        Check(fx.semDone.load() == 12 * kIters, "every coroutine and fiber acquisition completed");
        Check(fx.maxInside.load() <= kPermits, "never more than the permit count inside at once");
        if (fx.maxInside.load() > kPermits)
            std::printf("      max concurrent permit holders: %d (limit %d)\n", fx.maxInside.load(), kPermits);
    }

    // ---- Lazy<T>: values, exceptions, and the stack -------------------------------------------
    std::printf("Lazy<T> values\n");
    {
        Check(JLib::SyncWait(Doubled(21)) == 42, "SyncWait returns the coroutine's value");
        Check(JLib::SyncWait(Nested(10)) == 40, "a Lazy awaiting a Lazy composes (2x then 2x)");

        // void specialization
        g_voidRan.store(0);
        JLib::SyncWait(VoidLazy());
        Check(g_voidRan.load() == 1, "Lazy<void> runs and SyncWait returns");
    }

    std::printf("Lazy<T> laziness and lifetime\n");
    {
        g_lazyStarted.store(0);
        {
            JLib::Lazy<int> l = Doubled(1);       // created, never awaited
            Check(g_lazyStarted.load() == 0, "a Lazy does not start until it is awaited");
        }                                          // destroyed un-started; must not leak or crash
        Check(true, "destroying an un-awaited Lazy is well defined");

        Check(JLib::SyncWait(Doubled(3)) == 6, "the pool still works after an abandoned Lazy");
    }

    std::printf("Lazy<T> exceptions\n");
    {
        bool caught = false;
        int observed = 0;
        // The throw happens inside the inner Lazy; the catch is at the co_await in the outer one.
        observed = JLib::SyncWait(CatchesThrower(&caught));
        Check(caught, "an exception thrown in a Lazy surfaces at the awaiting co_await");
        Check(observed == -1, "the awaiting coroutine resumed normally after catching");
    }

    // ---- THE ONE THAT JUSTIFIES SYMMETRIC TRANSFER --------------------------------------------
    // A chain of N awaits must cost O(1) stack. If FinalAwaiter::await_suspend resumed the
    // continuation instead of RETURNING its handle, every completed frame would sit on the machine
    // stack while resuming the next, and this overflows. 100k deep is far past any worker stack.
    std::printf("deep await chain (O(1) stack, or this crashes)\n");
    {
        const int kDepth = 100000;
        const long long got = JLib::SyncWait(Chain(kDepth));
        Check(got == kDepth, "100,000-deep await chain returned the right value");
        if (got != kDepth) std::printf("      expected %d, got %lld\n", kDepth, got);
    }

    // ---- coroutine completes a DAG external node ---------------------------------------------
    // Coroutine work as a dependency EDGE rather than something a fiber node blocks on. The DAG
    // never learns what a coroutine is -- it just gets an external node signalled, exactly as an
    // IOCP completion would signal one.
    std::printf("coroutine completes a DAG external node\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::TaskDAG dag(sched);
        auto* n    = dag.CreateExternalNode();
        auto* tail = dag.CreateNode(sched.CreateTask([&ran] { ran.fetch_add(10, std::memory_order_relaxed); }));
        dag.AddDependency(tail, n);
        Check(dag.Submit(), "DAG submitted");

        Check(JLib::Spawn(NodeWork(&ran), n), "coroutine spawned onto the external node");

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (ran.load(std::memory_order_acquire) != 11 &&
               std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
        Check(ran.load() == 11, "coroutine ran, signalled the node, and the dependent fired");
    }

    // Many at once -- the reason this exists. As fiber nodes these would be 2,000 x 64KB of stacks
    // against a 64-per-worker budget; as coroutines it is two slab slots each.
    // DELIBERATELY MODEST, and the arithmetic is the reason. This file runs on a 4,096-slot slab so
    // the leak check above can bite, and every participant here draws from it:
    //
    //   external node   2 slots  (TaskNode + its dependents list -- TaskNode's ctor allocates twice)
    //   tail node       2 slots
    //   tail task       1 slot
    //   coroutine       2 slots  (Task + frame; frames come from this slab as of 2.12.0)
    //   ------------------------------------------------------------------------------
    //                   7 slots per iteration, plus whatever EBR has not reclaimed yet
    //
    // 400 needed ~2,800 and died. The scale demonstration lives in dag_external_test (4,000 pending
    // nodes on a default slab); this one only has to prove the coroutine->node wiring works.
    //
    // The failure mode is worth knowing: TaskNode's constructor THROWS on exhaustion rather than
    // returning null, so an uncaught one terminates. Caught below so a future sizing drift reports
    // itself instead of crashing.
    std::printf("100 coroutines, one external node each\n");
    {
        const int kN = 100;
        std::atomic<int> ran{ 0 };
        JLib::TaskDAG dag(sched);
        std::vector<JLib::TaskNode*> ext;
        ext.reserve(kN);
        bool allocOk = true;
        try {
            for (int i = 0; i < kN; ++i) {
                auto* n = dag.CreateExternalNode();
                auto* task = sched.CreateTask([&ran] { ran.fetch_add(10, std::memory_order_relaxed); });
                auto* t = task ? dag.CreateNode(task) : nullptr;
                if (!n || !t) { allocOk = false; break; }
                dag.AddDependency(t, n);
                ext.push_back(n);
            }
        }
        catch (const std::exception& e) {
            allocOk = false;
            std::printf("      slab exhausted: %s\n", e.what());
        }
        Check(allocOk, "every node allocated (slab not exhausted)");
        Check(dag.Submit(), "DAG submitted");
        int spawnFailures = 0;
        for (auto* n : ext) if (!JLib::Spawn(NodeWork(&ran), n)) ++spawnFailures;
        Check(spawnFailures == 0, "all 100 coroutines spawned");

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (ran.load(std::memory_order_acquire) != kN * 11 &&
               std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
        Check(ran.load() == kN * 11, "every coroutine completed its node exactly once");
        if (ran.load() != kN * 11) std::printf("      expected %d, got %d\n", kN * 11, ran.load());
    }

    std::printf("\n%s\n", g_failures == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_failures == 0 ? 0 : 1;
}

