// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// TaskDAG::Cancel -- abort a graph. Cancellation is an OUTCOME, not an unwind: a cancelled node
// never runs its payload, it transitions and propagates, so the ordinary dependent walk does all the
// work and nothing traverses backwards.
//
// THE TWO THINGS THAT WOULD SILENTLY RUIN THIS, and which each get a check below:
//
//   1. A cancelled task must still DECREMENT ITS WAITGROUP. Nothing else ever will -- the task does
//      not run -- so a caller in WaitFor would block forever on abandoned work. Cancelling would
//      deadlock whoever asked to wait, which is the exact opposite of the point.
//   2. A cancelled task must be DESTROYED AND FREED. The worker frees tasks it ran; a task that is
//      never dispatched has no such owner, and leaks a slab slot per cancelled node. That is
//      invisible in every behavioural check, so it is measured against a small slab instead.

#include "TaskScheduler.h"
#include "TaskDAG.h"

#include <new>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <algorithm>
#include <vector>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

static bool WaitUntil(std::atomic<int>& v, int want, int ms = 4000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (v.load(std::memory_order_acquire) != want) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::yield();
    }
    return true;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    // Small on purpose: a leaked task per cancelled node has to run the slab out, not hide in it.
    // Raised from 8,192: the sections below churn 400 DAGs whose nodes are EBR-RETIRED, so
    // reclamation lags allocation and 8,192 ran genuinely dry partway through. Still small enough
    // that a leaked task per cancelled node exhausts it, which is what the slab check needs.
    JLib::TaskScheduler::SetSlabSizes({ 32768, 4096, 4096 });
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("TaskDAG cancellation -- workers=%zu\n\n", sched.GetWorkerCount());

    // ---- cancel before Submit: nothing runs at all -------------------------------------------
    std::printf("cancel before Submit\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);

        JLib::TaskNode* prev = nullptr;
        for (int i = 0; i < 5; ++i) {
            auto* t = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
            t->waitGroup = &wg;
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* n = dag.CreateNode(t);
            if (prev) dag.AddDependency(n, prev);   // a chain
            prev = n;
        }

        dag.Cancel();
        Check(dag.Cancelled(), "Cancel() is observable");
        Check(dag.Submit(), "Submit still succeeds on a cancelled graph");

        // THE DEADLOCK CHECK. If a cancelled task failed to decrement, this never returns.
        sched.WaitFor(wg);
        Check(true, "WaitFor returned -- cancelled tasks released their WaitGroup");
        Check(ran.load() == 0, "no payload ran");
    }

    // ---- cancel partway: what already ran stays run, the rest does not ------------------------
    std::printf("cancel partway through a chain\n");
    {
        std::atomic<int> headRan{ 0 }, tailRan{ 0 };
        std::atomic<bool> release{ false };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);

        auto* h = sched.CreateTask([&] {
            headRan.fetch_add(1, std::memory_order_relaxed);
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
        });
        h->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* head = dag.CreateNode(h);

        auto* t = sched.CreateTask([&] { tailRan.fetch_add(1, std::memory_order_relaxed); });
        t->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* tail = dag.CreateNode(t);
        dag.AddDependency(tail, head);

        Check(dag.Submit(), "DAG submitted");
        Check(WaitUntil(headRan, 1), "head started");

        dag.Cancel();                       // head is mid-body and keeps going; tail is not dispatched
        release.store(true, std::memory_order_release);

        sched.WaitFor(wg);
        Check(headRan.load() == 1, "the already-running node completed normally");
        Check(tailRan.load() == 0, "the not-yet-dispatched dependent never ran");
    }

    // ---- AND: one cancelled input cancels the node, without waiting for the others ------------
    std::printf("AND gate with a cancelled input\n");
    {
        std::atomic<int> ran{ 0 }, sinkRan{ 0 };
        std::atomic<bool> release{ false };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);

        // a completes normally; b is held until after the cancel, so the AND is still waiting.
        auto* ta = sched.CreateTask([&] { ran.fetch_add(1, std::memory_order_relaxed); });
        ta->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* a = dag.CreateNode(ta);

        auto* tb = sched.CreateTask([&] {
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
            ran.fetch_add(1, std::memory_order_relaxed);
        });
        tb->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* b = dag.CreateNode(tb);

        auto* ts = sched.CreateTask([&] { sinkRan.fetch_add(1, std::memory_order_relaxed); });
        ts->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* sink = dag.CreateNode(ts);      // AND is the default gate type
        dag.AddDependency(sink, a);
        dag.AddDependency(sink, b);

        Check(dag.Submit(), "DAG submitted");
        Check(WaitUntil(ran, 1), "the first input completed");

        dag.Cancel();
        release.store(true, std::memory_order_release);
        sched.WaitFor(wg);

        Check(sinkRan.load() == 0, "AND with a cancelled input did not run");
    }

    // ---- OR: first result wins, even when it is a cancellation --------------------------------
    std::printf("OR gate: first result wins\n");
    {
        std::atomic<int> sinkRan{ 0 };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);

        auto* ta = sched.CreateTask([] {});
        ta->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* a = dag.CreateNode(ta);

        auto* gate = dag.CreateGate(JLib::TaskNode::OR);
        dag.AddDependency(gate, a);

        auto* ts = sched.CreateTask([&] { sinkRan.fetch_add(1, std::memory_order_relaxed); });
        ts->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* sink = dag.CreateNode(ts);
        dag.AddDependency(sink, gate);

        dag.Cancel();                        // a is cancelled, so the OR's first result is CANCELLED
        Check(dag.Submit(), "DAG submitted");
        sched.WaitFor(wg);
        Check(sinkRan.load() == 0, "OR whose first result was CANCELLED stayed cancelled");
    }

    // ---- THE LEAK CHECK: a cancelled task must return its slot -------------------------------
    // 8,192 slots; each iteration is a node (2 slots: TaskNode + its dependents list) plus a task.
    // 400 graphs of 4 cancelled nodes is 4,800 tasks and 9,600 node slots CHURNED -- far past the
    // slab -- so a single leaked task per cancelled node exhausts it and CreateTask starts failing.
    // What the chunked-edge refactor was for. A TaskNode used to cost FOUR slab slots before a
    // single edge existed -- the node, its LockFreeList object, and that list's two sentinels --
    // plus one slot per edge. Now it is one slot for the node, and edges come from DAG-owned chunks
    // that touch the slab not at all. Measured, because nothing behavioural would notice a
    // regression here.
    std::printf("DAG node and edge slab cost\n");
    {
        auto* alloc = JLib::TaskScheduler::Instance().GetAllocator();
        const int kNodes = 64;

        const long long base = alloc->LiveCount();
        const long long smallBase = alloc->SmallLiveCount();
        {
            JLib::TaskDAG dag(sched);
            JLib::WaitGroup wg;
            JLib::TaskNode* prev = nullptr;
            for (int i = 0; i < kNodes; ++i) {
                auto* t = sched.CreateTask([] {});
                t->waitGroup = &wg;
                wg.n.fetch_add(1, std::memory_order_relaxed);
                auto* n = dag.CreateNode(t);
                if (!n) { Check(false, "CreateNode returned null"); break; }
                if (prev) dag.AddDependency(n, prev);     // a chain: kNodes-1 edges
                prev = n;
            }

            // BOTH pools: the node lives in the 64-byte class and its task in the 256-byte one,
            // so a single-pool number would silently under-report and the check would pass for
            // the wrong reason.
            const long long held = (alloc->LiveCount() - base)
                                 + (alloc->SmallLiveCount() - smallBase);
            std::printf("    slab slots for %d nodes + %d edges: %lld\n", kNodes, kNodes - 1, held);
            // One slot per node, one per task, plus one CHUNK slot per 16 edges -- chunks are
            // cut from the slab, not the heap, so the per-edge cost stays inside the single
            // allocator. The old layout was 4 slots per node plus one per edge: 5*64 + 63 = 383.
            const long long chunks = (kNodes - 1 + 15) / 16;   // ceil(edges / edges-per-slot)
            Check(held <= 2 * kNodes + chunks,
                  "a node costs one slab slot and 16 edges share one");
            dag.Cancel();
            dag.Submit();
            sched.WaitFor(wg);
        }
    }

    std::printf("slab accounting across many cancelled graphs\n");
    {
        const int kGraphs = 400, kNodes = 4;
        int allocFailures = 0;
        for (int g = 0; g < kGraphs; ++g) {
            JLib::WaitGroup wg;
            JLib::TaskDAG dag(sched);
            JLib::TaskNode* prev = nullptr;
            for (int i = 0; i < kNodes; ++i) {
                auto* t = sched.CreateTask([] {});
                if (!t) { ++allocFailures; break; }
                auto* n = dag.CreateNode(t);
                if (!n) {
                    // ORDER MATTERS HERE, and getting it wrong turns a clean failure into a HANG.
                    // The WaitGroup is joined only AFTER the node exists: a task that never reaches
                    // a node is owned by nobody, so nothing will ever decrement for it and the
                    // WaitFor below would block forever on it. An earlier version incremented first
                    // and stranded the group the moment the slab ran out -- which is precisely the
                    // condition this section exists to produce, so the leak control froze instead of
                    // reporting. Release the orphan and stop.
                    ++allocFailures;
                    DestroyTask(t);
                    sched.GetAllocator()->Free(t);
                    break;
                }
                t->waitGroup = &wg;
                wg.n.fetch_add(1, std::memory_order_relaxed);
                if (prev) dag.AddDependency(n, prev);
                prev = n;
            }
            dag.Cancel();
            dag.Submit();
            sched.WaitFor(wg);     // also re-checks the decrement, 400 more times
        }
        Check(allocFailures == 0,
              "1,600 cancelled tasks through an 8,192-slot slab: no allocation ever failed");
        if (allocFailures) std::printf("      %d allocations failed -- cancelled tasks are leaking\n",
                                       allocFailures);
    }

    // ---- the token reaches a task that is ALREADY RUNNING -------------------------------------
    // Fire()'s flag only governs what has not been dispatched. This is the other half: a node that
    // is mid-body hears about the abort through its scope and can give up.
    std::printf("cancel reaches a running task through its token\n");
    {
        std::atomic<int> iterations{ 0 };
        std::atomic<bool> sawCancel{ false }, started{ false };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);

        auto* t = sched.CreateTask([&] {
            started.store(true, std::memory_order_release);
            // A long body that polls, which is all a Native task can do.
            for (int i = 0; i < 100000000; ++i) {
                if (JLib::CurrentTaskCancelled()) { sawCancel.store(true, std::memory_order_release); return; }
                iterations.fetch_add(1, std::memory_order_relaxed);
            }
        });
        auto* n = dag.CreateNode(t);
        Check(t && n, "task and node allocated");
        t->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);

        Check(dag.Submit(), "DAG submitted");
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!started.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
        Check(started.load(), "the task started");

        dag.Cancel();
        sched.WaitFor(wg);
        Check(sawCancel.load(), "the running task observed cancellation and returned early");
        Check(iterations.load() < 100000000, "it stopped short of finishing the loop");
    }

    // A task with no scope must never report cancelled -- otherwise every unscoped task in the
    // process would start aborting the moment anything anywhere was cancelled.
    std::printf("unscoped work is not cancelled work\n");
    {
        std::atomic<bool> saw{ false };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* t = sched.CreateTask([&] { saw.store(JLib::CurrentTaskCancelled(), std::memory_order_release); });
        t->waitGroup = &wg;
        sched.Push(t);
        sched.WaitFor(wg);
        Check(!saw.load(), "a task outside any scope reports not-cancelled");
        Check(!JLib::CurrentTaskCancelled(), "off a task entirely reports not-cancelled");
    }

    // ---- skip-at-release: a cancelled fiber waiting on a lock is passed over -------------------
    // The delivery mechanism, and the one that could silently do the wrong thing: a cancelled
    // waiter must come back WITHOUT the lock, and the lock must still reach someone who wants it.
    // SchedulerSemaphore::WaitCancellable. The release side of this shipped in 2.14.0 and could not
    // be reached until 3.0.3: Signal() walked past cancelled waiters, but Wait() pushed a null
    // result slot, which by Waiter's contract means "never skipped". So the interesting checks are
    // not "does the flag arrive" but the two things that would be silently wrong.
    std::printf("SchedulerSemaphore::WaitCancellable\n");
    {
        JLib::SchedulerSemaphore sem(0);                  // no permits: every waiter parks
        JLib::CancelScope scope;
        std::atomic<int> cancelled{ 0 }, acquired{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;

        constexpr int kN = 4;
        for (int i = 0; i < kN; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                parked.fetch_add(1, std::memory_order_relaxed);
                if (sem.WaitCancellable() == JLib::WaitResult::Cancelled)
                    cancelled.fetch_add(1, std::memory_order_relaxed);
                else
                    acquired.fetch_add(1, std::memory_order_relaxed);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->cancelToken = scope.Token().Raw();
            t->waitGroup = &wg;
            sched.Push(t);
        }
        Check(WaitUntil(parked, kN), "all waiters parked on the semaphore");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));

        scope.Cancel();
        // Marking does not wake anyone: skip-at-release means the waiter learns at the release, so
        // the permits below are what actually drive them out.
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        Check(cancelled.load() == 0 && acquired.load() == 0,
              "cancelling alone does not wake a semaphore waiter");

        // ONE Signal releases ALL FOUR, because each cancelled waiter is skipped and the walk
        // continues. If skip-at-release were broken this would wake exactly one and the WaitFor
        // below would hang -- which is the failure this section exists to catch.
        sem.Signal();
        sched.WaitFor(wg);
        Check(cancelled.load() == kN, "every cancelled waiter returned Cancelled");
        Check(acquired.load() == 0, "none of them believed it held a permit");

        // THE PERMIT MUST STILL BE THERE. A cancelled waiter takes nothing, so the Signal above is
        // still owed to somebody. If Cancelled consumed a permit this reads 0 and a later Wait()
        // would park forever -- invisible in every check above.
        Check(sem.Try_Wait(), "the permit survived: cancellation consumed nothing");
    }

    // SchedulerSemaphore::CancelWaiters -- EAGER, for a semaphore used as an I/O throttle. The
    // proof that it is eager is that NO Signal is issued anywhere below and WaitFor still returns.
    // SchedulerConditionVariable::WaitCancellable. THE INVARIANT under test is not "does the flag
    // arrive" -- it is that a CANCELLED return still holds the mutex. If it did not, every caller
    // would need a conditional unlock, and the first to forget would unlock a mutex it does not
    // hold, corrupting it for everyone from a path that only runs when something is already wrong.
    //
    // Proven by having each waiter Unlock() unconditionally on the cancelled path, exactly as a real
    // caller would, and then checking the mutex is actually free afterwards. If a cancelled return
    // came back WITHOUT the lock, those unlocks would be releasing a mutex nobody held.
    std::printf("SchedulerConditionVariable::WaitCancellable holds the mutex on cancel\n");
    {
        JLib::SchedulerMutex m;
        JLib::SchedulerConditionVariable cv;
        JLib::CancelScope scope;
        std::atomic<int> cancelled{ 0 }, woke{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;

        constexpr int kN = 4;
        for (int i = 0; i < kN; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                m.Lock();
                parked.fetch_add(1, std::memory_order_relaxed);
                const JLib::WaitResult r = cv.WaitCancellable(m);
                if (r == JLib::WaitResult::Cancelled) cancelled.fetch_add(1, std::memory_order_relaxed);
                else                                  woke.fetch_add(1, std::memory_order_relaxed);
                m.Unlock();          // UNCONDITIONAL -- the whole point of the invariant
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->cancelToken = scope.Token().Raw();
            t->waitGroup = &wg;
            sched.Push(t);
        }
        Check(WaitUntil(parked, kN), "all waiters reached the condition variable");
        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        // EAGER: no Notify anywhere. The condition simply never becomes true, which is the case a
        // condition variable most needs cancelling for.
        scope.Cancel();
        cv.CancelWaiters(scope.Token());
        sched.WaitFor(wg);

        Check(cancelled.load() == kN, "every waiter returned Cancelled with no notify at all");
        Check(woke.load() == 0, "none reported a normal wake");

        // THE INVARIANT. Every waiter unlocked unconditionally above; if a cancelled return had not
        // held the lock, the mutex is now in a corrupt state and this cannot be taken.
        Check(m.Try_Lock(), "the mutex is free: every cancelled return really did hold it");
        m.Unlock();
    }

    // A plain Wait() must be untouched by cancellation -- it has nowhere to report Cancelled, so
    // waking it would return it into its critical section believing the condition became true.
    std::printf("CancelWaiters never wakes a plain condition-variable Wait\n");
    {
        JLib::SchedulerMutex m;
        JLib::SchedulerConditionVariable cv;
        JLib::CancelScope scope;
        std::atomic<int> woke{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            m.Lock();
            parked.fetch_add(1, std::memory_order_relaxed);
            cv.Wait(m);                                   // NOT the cancellable spelling
            woke.fetch_add(1, std::memory_order_relaxed);
            m.Unlock();
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->cancelToken = scope.Token().Raw();             // cancelled scope, uncancellable wait
        t->waitGroup = &wg;
        sched.Push(t);

        Check(WaitUntil(parked, 1), "the plain waiter parked");
        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        scope.Cancel();
        cv.CancelWaiters(scope.Token());
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        Check(woke.load() == 0, "a plain Wait is not woken by cancellation");

        cv.Notify_All();                                  // only a real notify releases it
        sched.WaitFor(wg);
        Check(woke.load() == 1, "and it wakes normally when the condition is notified");
    }

    // Uncancelled behaviour must be untouched: notify still works through the cancellable spelling.
    std::printf("uncancelled WaitCancellable is an ordinary condition wait\n");
    {
        JLib::SchedulerMutex m;
        JLib::SchedulerConditionVariable cv;
        std::atomic<int> ok{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            m.Lock();
            parked.fetch_add(1, std::memory_order_relaxed);
            if (cv.WaitCancellable(m) == JLib::WaitResult::Ok)
                ok.fetch_add(1, std::memory_order_relaxed);
            m.Unlock();
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->waitGroup = &wg;                                // no scope at all
        sched.Push(t);

        Check(WaitUntil(parked, 1), "the waiter parked");
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        cv.Notify_One();
        sched.WaitFor(wg);
        Check(ok.load() == 1, "a waiter with no scope is notified normally");
        Check(m.Try_Lock(), "and the mutex came back to it and was released");
        m.Unlock();
    }

    std::printf("SchedulerSemaphore::CancelWaiters wakes without a signal\n");
    {
        JLib::SchedulerSemaphore sem(0);
        JLib::CancelScope scope;
        std::atomic<int> cancelled{ 0 }, acquired{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;

        constexpr int kN = 4;
        for (int i = 0; i < kN; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                parked.fetch_add(1, std::memory_order_relaxed);
                if (sem.WaitCancellable() == JLib::WaitResult::Cancelled)
                    cancelled.fetch_add(1, std::memory_order_relaxed);
                else
                    acquired.fetch_add(1, std::memory_order_relaxed);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->cancelToken = scope.Token().Raw();
            t->waitGroup = &wg;
            sched.Push(t);
        }
        Check(WaitUntil(parked, kN), "all waiters parked on the throttle");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));

        scope.Cancel();
        sem.CancelWaiters(scope.Token());     // no Signal anywhere -- this is the whole point
        sched.WaitFor(wg);

        Check(cancelled.load() == kN, "every waiter woke Cancelled with no signal at all");
        Check(acquired.load() == 0, "none of them believed it held a permit");
        Check(!sem.Try_Wait(), "no permit was invented: the counter is untouched");
    }

    // THE ONE THAT WOULD BE A REAL BUG. A plain Wait() has nowhere to report Cancelled, so ejecting
    // it would return its caller into a critical section holding a permit it does not have. Those
    // waiters must stay queued no matter what token they carry.
    std::printf("CancelWaiters never ejects a plain Wait()\n");
    {
        JLib::SchedulerSemaphore sem(0);
        JLib::CancelScope scope;
        std::atomic<int> acquired{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            parked.fetch_add(1, std::memory_order_relaxed);
            sem.Wait();                                   // NOT the cancellable spelling
            acquired.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->cancelToken = scope.Token().Raw();             // cancelled scope, uncancellable wait
        t->waitGroup = &wg;
        sched.Push(t);

        Check(WaitUntil(parked, 1), "the plain waiter parked");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));

        scope.Cancel();
        sem.CancelWaiters(scope.Token());
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(acquired.load() == 0, "a plain Wait() is not woken by cancellation");

        sem.Signal();                                     // only a real permit releases it
        sched.WaitFor(wg);
        Check(acquired.load() == 1, "and it acquires normally when one arrives");
    }

    // Scope-selective: cancelling one scope must not disturb a waiter under another.
    std::printf("CancelWaiters only ejects the scope it was given\n");
    {
        JLib::SchedulerSemaphore sem(0);
        JLib::CancelScope doomed, kept;
        std::atomic<int> doomedCancelled{ 0 }, keptAcquired{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(2, std::memory_order_relaxed);

        auto* a = sched.CreateTask([&] {
            parked.fetch_add(1, std::memory_order_relaxed);
            if (sem.WaitCancellable() == JLib::WaitResult::Cancelled)
                doomedCancelled.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        a->cancelToken = doomed.Token().Raw();
        a->waitGroup = &wg;
        sched.Push(a);

        auto* b = sched.CreateTask([&] {
            parked.fetch_add(1, std::memory_order_relaxed);
            if (sem.WaitCancellable() == JLib::WaitResult::Ok)
                keptAcquired.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        b->cancelToken = kept.Token().Raw();
        b->waitGroup = &wg;
        sched.Push(b);

        Check(WaitUntil(parked, 2), "both waiters parked");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));

        doomed.Cancel();
        sem.CancelWaiters(doomed.Token());
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(doomedCancelled.load() == 1, "the cancelled scope's waiter woke Cancelled");
        Check(keptAcquired.load() == 0, "the other scope's waiter is still parked");

        sem.Signal();
        sched.WaitFor(wg);
        Check(keptAcquired.load() == 1, "and it got the permit when one arrived");
    }

    // Cancelled BEFORE the wait, but reached from INSIDE a running task. That distinction is the
    // whole point of this section: a task cancelled before dispatch never runs at all -- the worker
    // discards it at pickup -- so cancelling the scope and then pushing the task tests the pickup
    // path, not this one. The first version of this check did exactly that, and "failed" because
    // the body never executed rather than because the early-out was wrong.
    std::printf("WaitCancellable declines a free permit when already cancelled\n");
    {
        JLib::SchedulerSemaphore sem(1);                  // a permit IS available
        JLib::CancelScope scope;
        std::atomic<int> result{ -1 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            scope.Cancel();                               // now running, and now cancelled
            result.store(sem.WaitCancellable() == JLib::WaitResult::Cancelled ? 1 : 0,
                         std::memory_order_release);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->cancelToken = scope.Token().Raw();
        t->waitGroup = &wg;
        sched.Push(t);
        sched.WaitFor(wg);

        Check(result.load() == 1, "returned Cancelled even though a permit was free");
        Check(sem.Try_Wait(), "and left the permit for someone who can use it");
    }

    // The uncancelled path must be untouched -- a cancellable API that quietly changed ordinary
    // behaviour would be worse than not having it.
    std::printf("uncancelled WaitCancellable is an ordinary Wait\n");
    {
        JLib::SchedulerSemaphore sem(0);
        std::atomic<int> ok{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* t = sched.CreateTask([&] {
            if (sem.WaitCancellable() == JLib::WaitResult::Ok)
                ok.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->waitGroup = &wg;                                // NO cancel token at all
        sched.Push(t);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sem.Signal();
        sched.WaitFor(wg);
        Check(ok.load() == 1, "a waiter with no scope acquires normally");
    }

    std::printf("skip-at-release on a contended mutex\n");
    {
        JLib::SchedulerMutex m;
        JLib::CancelScope scope;
        std::atomic<int> cancelledWaits{ 0 }, okWaits{ 0 };
        std::atomic<bool> holderIn{ false }, holderRelease{ false };
        JLib::WaitGroup wg;

        // A holder that keeps the lock until told, so the waiters below genuinely park.
        wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* ht = sched.CreateTask([&] {
            m.Lock();
            holderIn.store(true, std::memory_order_release);
            while (!holderRelease.load(std::memory_order_acquire)) std::this_thread::yield();
            m.Unlock();
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        ht->waitGroup = &wg;
        sched.Push(ht);

        const auto d0 = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!holderIn.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < d0) std::this_thread::yield();
        Check(holderIn.load(), "holder acquired the lock");

        // Four scoped waiters that will be cancelled, and one unscoped that must still get the lock.
        for (int i = 0; i < 4; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                if (m.LockCancellable() == JLib::WaitResult::Cancelled) {
                    cancelledWaits.fetch_add(1, std::memory_order_relaxed);
                    return;                       // NOT holding the lock -- must not Unlock
                }
                okWaits.fetch_add(1, std::memory_order_relaxed);
                m.Unlock();
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->cancelToken = scope.Token().Raw();
            t->waitGroup = &wg;
            sched.Push(t);
        }

        wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* keep = sched.CreateTask([&] {
            m.Lock();                              // unscoped, not cancellable, must be served
            okWaits.fetch_add(1, std::memory_order_relaxed);
            m.Unlock();
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        keep->waitGroup = &wg;
        sched.Push(keep);

        std::this_thread::sleep_for(std::chrono::milliseconds(150));   // let them all park
        scope.Cancel();
        holderRelease.store(true, std::memory_order_release);
        sched.WaitFor(wg);

        Check(cancelledWaits.load() == 4, "all four cancelled waiters returned Cancelled");
        Check(okWaits.load() == 1, "the uncancellable waiter still got the lock");

        // If a skipped waiter had wrongly been handed the lock, or the lock had been dropped
        // without reaching anyone, this would fail or hang.
        Check(m.Try_Lock(), "the mutex ended up free and consistent");
        m.Unlock();
    }

    // ---- a cancellable wait that is NOT cancelled behaves exactly like Lock() -----------------
    std::printf("uncancelled LockCancellable is an ordinary lock\n");
    {
        JLib::SchedulerMutex m;
        JLib::WaitGroup wg;
        std::atomic<int> got{ 0 };
        for (int i = 0; i < 8; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                if (m.LockCancellable() == JLib::WaitResult::Ok) {
                    got.fetch_add(1, std::memory_order_relaxed);
                    m.Unlock();
                }
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        Check(got.load() == 8, "all eight acquired normally");
    }

    // ---- pickup-time cancellation, outside any DAG -------------------------------------------
    // The DAG catches cancellation at Fire, BEFORE a task reaches a queue, so nothing above
    // exercises the worker's own check. This does: the scope is cancelled first, then the tasks are
    // pushed, so every one of them is picked up already-cancelled and must be discarded rather than
    // run -- with its WaitGroup released so the wait below returns.
    std::printf("cancelled tasks are discarded at pickup\n");
    {
        JLib::CancelScope scope;
        scope.Cancel();

        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        const int kN = 500;
        int pushed = 0, allocFail = 0;
        for (int i = 0; i < kN; ++i) {
            // ALWAYS NULL-CHECK CreateTask HERE. By this point the suite has churned 400 DAGs whose
            // nodes are EBR-RETIRED rather than freed immediately, so reclamation lags the
            // allocations and the slab can genuinely be out. Dereferencing the nullptr crashes with
            // the section header as the last thing printed, which reads exactly like a hang.
            auto* t = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
            if (!t) { ++allocFail; continue; }
            t->cancelToken = scope.Token().Raw();
            t->waitGroup = &wg;
            wg.n.fetch_add(1, std::memory_order_relaxed);
            sched.Push(t);
            ++pushed;
        }
        Check(allocFail == 0, "all 500 tasks allocated");
        if (allocFail) std::printf("      %d allocations failed -- slab pressure, raise it\n", allocFail);
        sched.WaitFor(wg);          // hangs if a discarded task failed to release the group
        Check(ran.load() == 0, "none of the pushed cancelled tasks ran");
        Check((wg.n.load() & JLib::WaitGroup::COUNT_MASK) == 0, "the group drained to zero");
    }

    // A STARTED task must never be discarded -- the queued entry may be a resume, and dropping one
    // abandons a live fiber stack. Here the task suspends, is cancelled while parked, and its
    // resume goes back through the same queues the check inspects: it must be let through so the
    // fiber unwinds, not thrown away.
    std::printf("a suspended task's resume survives the pickup check\n");
    {
        JLib::SchedulerMutex m;
        JLib::CancelScope scope;
        std::atomic<int> reachedEnd{ 0 };
        std::atomic<bool> parked{ false }, release{ false };
        JLib::WaitGroup wg;

        // Holder keeps the lock so the task below genuinely parks in the waiter queue.
        wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* ht = sched.CreateTask([&] {
            m.Lock();
            parked.store(true, std::memory_order_release);
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
            m.Unlock();
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        ht->waitGroup = &wg;
        sched.Push(ht);

        const auto d = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!parked.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < d) std::this_thread::yield();

        wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* t = sched.CreateTask([&] {
            if (m.LockCancellable() == JLib::WaitResult::Ok) m.Unlock();
            // Reached either way. If the resume had been discarded, this never runs and the
            // WaitFor below hangs -- which is the failure this check exists to catch.
            reachedEnd.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->cancelToken = scope.Token().Raw();
        t->waitGroup = &wg;
        sched.Push(t);

        std::this_thread::sleep_for(std::chrono::milliseconds(120));   // let it park
        scope.Cancel();
        release.store(true, std::memory_order_release);
        sched.WaitFor(wg);

        Check(reachedEnd.load() == 1, "the suspended task was resumed and unwound, not discarded");
    }


    // ---- direct cancellation now reaches a waiter queue ----------------------------------------
    // Task.h says a task is cancelled if EITHER its scope was cancelled OR it was cancelled
    // individually, and TaskScheduler.h calls IsTaskCancelled "THE one place cancellation is
    // decided". Until 3.2.1 the mutex and semaphore release paths did not go through it -- they read
    // the SCOPE TOKEN copied into the Waiter, so cancelledDirect was invisible and a directly
    // cancelled waiter parked on a semaphore could not be cancelled at all.
    //
    // This waiter has NO SCOPE (token kNone), so the old predicate had nothing to look at. If the
    // release path ever goes back to reading the token, this waiter takes the permit and the first
    // check below fails.
    std::printf("a DIRECTLY cancelled waiter is skipped at release\n");
    {
        JLib::SchedulerSemaphore sem(0);
        std::atomic<int> cancelled{ 0 }, acquired{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            parked.fetch_add(1, std::memory_order_relaxed);
            if (sem.WaitCancellable() == JLib::WaitResult::Cancelled)
                cancelled.fetch_add(1, std::memory_order_relaxed);
            else
                acquired.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->waitGroup = &wg;                       // deliberately no cancelToken
        sched.Push(t);

        Check(WaitUntil(parked, 1), "the unscoped waiter parked");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));

        // Exactly what Event::CancelWaiters does to a waiter it has claimed.
        t->cancelledDirect = 1;
        sem.Signal();
        sched.WaitFor(wg);

        Check(cancelled.load() == 1, "it woke Cancelled from cancelledDirect alone, with no scope");
        Check(acquired.load() == 0, "it did not take the permit");
        Check(sem.Try_Wait(), "and the permit went back to the counter for someone who wants it");
    }

    // THE ONE THAT MATTERS: a parked TASK, cancelled by an enclosing scope it never heard of.
    // The task carries the innermost token only -- one uint32_t, exactly as before -- and the walk
    // in CancelToken::Cancelled does the rest through IsTaskCancelled at a real suspension point.
    // This is the shape a timeout will use: the timer cancels the operation, the peer disconnecting
    // cancels the connection, and the same parked wait wakes for either.
    std::printf("a parked task is cancelled by an ENCLOSING scope\n");
    {
        JLib::SchedulerSemaphore sem(0);
        JLib::CancelScope conn;
        JLib::CancelScope op(conn.Token());
        std::atomic<int> cancelled{ 0 }, acquired{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            parked.fetch_add(1, std::memory_order_relaxed);
            if (sem.WaitCancellable() == JLib::WaitResult::Cancelled)
                cancelled.fetch_add(1, std::memory_order_relaxed);
            else
                acquired.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->cancelToken = op.Token().Raw();          // the INNER scope only
        t->waitGroup = &wg;
        sched.Push(t);

        Check(WaitUntil(parked, 1), "the waiter parked under the inner scope");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));

        conn.Cancel();                              // cancel the OUTER scope; nothing knows about t
        sem.Signal();                               // skip-at-release has to see it through the walk
        sched.WaitFor(wg);

        Check(cancelled.load() == 1, "it woke Cancelled from a scope it does not carry");
        Check(acquired.load() == 0, "it did not take the permit");
        Check(sem.Try_Wait(), "and the permit went back to the counter");
    }

    // ---- THE CLEANUP TRAP ----------------------------------------------------------------------
    // Pins the behaviour documented at SchedulerMutex::LockCancellable, because a rule with no test
    // is a rule somebody "fixes" later.
    //
    // An unwind path runs BECAUSE the scope was cancelled, so its token is already cancelled when it
    // gets there. LockCancellable pre-checks the token before touching the mutex, so on that path it
    // returns Cancelled WITHOUT EVEN LOOKING AT THE LOCK -- here the mutex is provably free and it
    // still refuses. Not a race that leaks sometimes: it is every time. Cleanup uses plain Lock().
    std::printf("LockCancellable refuses a FREE lock once the scope is cancelled\n");
    {
        JLib::SchedulerMutex m;
        JLib::CancelScope scope;
        // NOT cancelled before the push: a task whose scope is already cancelled is DISCARDED AT
        // PICKUP and never runs, which would test the pickup path instead of this one. The task
        // cancels itself below, exactly as work does when it discovers it has been abandoned.
        std::atomic<int> refused{ 0 }, gotIt{ 0 }, cleanedUp{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            scope.Cancel();          // now we are on an unwind path, and the token is cancelled

            // Nobody holds this mutex. Try_Lock would succeed right now.
            if (m.LockCancellable() == JLib::WaitResult::Cancelled) refused.fetch_add(1, std::memory_order_relaxed);
            else { gotIt.fetch_add(1, std::memory_order_relaxed); m.Unlock(); }

            // What the unwind must actually use. Plain Lock() cannot fail, so the release happens.
            m.Lock();
            cleanedUp.fetch_add(1, std::memory_order_relaxed);
            m.Unlock();
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->cancelToken = scope.Token().Raw();
        t->waitGroup = &wg;
        sched.Push(t);
        sched.WaitFor(wg);

        Check(refused.load() == 1, "an uncontended lock was still refused: the pre-check fires first");
        Check(gotIt.load() == 0, "it never acquired, so there is nothing to Unlock");
        Check(cleanedUp.load() == 1, "plain Lock() on the same free mutex succeeded: cleanup belongs there");
        Check(m.Try_Lock(), "and the mutex is undamaged afterwards");
        m.Unlock();
    }

    // ---- the WHOLE table, including its last slot ------------------------------------------------
    // Exercises the two things that had to change to raise kCancelSlots from 4,096 to 65,535.
    //
    // 1. The free-list head packs a ONE-BASED index, so a full table has to represent 65,535. That
    //    did not fit alongside the ABA tag in 32 bits; the head is 64-bit now. If it were not, the
    //    high slots would alias low ones and this loop would hand out duplicates or run short.
    // 2. kCancelSlots stops at 65,535 rather than 65,536 so that index 0xFFFF is never valid --
    //    a token at index 0xFFFF, generation 0xFFFF is bit-identical to kNone, and would read as
    //    UNSCOPED. Every scope here is made to report its own cancellation, which is exactly what a
    //    token silently equal to kNone could not do.
    std::printf("the cancel table can be filled to its last slot\n");
    {
        std::vector<JLib::CancelScope> all(JLib::detail::kCancelSlots);
        size_t invalid = 0, unresolvable = 0, aliased = 0;
        std::vector<uint32_t> seen;
        seen.reserve(JLib::detail::kCancelSlots);

        for (auto& s : all) {
            if (!s.Valid()) { ++invalid; continue; }
            s.Cancel();
            if (!s.Token().Cancelled()) ++unresolvable;
            seen.push_back(s.Token().Raw() & 0xFFFFu);
        }
        std::sort(seen.begin(), seen.end());
        for (size_t i = 1; i < seen.size(); ++i) if (seen[i] == seen[i - 1]) ++aliased;

        char m[144];
        std::snprintf(m, sizeof m, "all %u slots handed out (invalid: %zu)",
                      JLib::detail::kCancelSlots, invalid);
        Check(invalid == 0, m);
        Check(unresolvable == 0, "every one of them resolved and reported its own cancellation");
        Check(aliased == 0, "no two live scopes were given the same slot index");
        Check(seen.empty() || seen.back() < 0xFFFFu, "no scope was issued index 0xFFFF, which is kNone");

        // Full table. One more must fail OPEN -- invalid, cancelling nothing, never throwing.
        {
            JLib::CancelScope overflow;
            Check(!overflow.Valid(), "a scope past the end of a full table is invalid");
            Check(!overflow.Cancelled(), "and reports not-cancelled rather than cancelling everything");
            overflow.Cancel();
            Check(!overflow.Cancelled(), "cancelling an invalid scope is a no-op, not a crash");
        }
    }

    // ...and the table comes back. If ReleaseSlot lost entries at the 64-bit boundary this is where
    // it shows: the second fill would come up short.
    std::printf("and the whole table is recovered afterwards\n");
    {
        std::vector<JLib::CancelScope> all(JLib::detail::kCancelSlots);
        size_t invalid = 0;
        for (auto& s : all) if (!s.Valid()) ++invalid;
        Check(invalid == 0, "the table refilled completely after being emptied");
    }

    // ---- NESTED SCOPES -------------------------------------------------------------------------
    // The parent link lives in CancelSlot, not in Task: a Task is capped at 64 bytes and there are
    // millions of them, a slot is uncapped and there are 4,096. So nesting costs Task nothing -- it
    // still carries exactly one token, its innermost scope -- and Cancelled() walks the chain.
    //
    // This is what lets ONE token express several reasons to stop, which is the whole point for I/O:
    // an operation nested in a connection is cancelled by a timeout firing on the operation OR by
    // the peer going away and the connection being cancelled.
    std::printf("cancellation is inherited from an enclosing scope\n");
    {
        JLib::CancelScope conn;
        JLib::CancelScope req(conn.Token());
        JLib::CancelScope op(req.Token());

        Check(!op.Cancelled(), "a fresh three-deep chain is not cancelled");

        conn.Cancel();                                   // the OUTERMOST scope
        Check(op.Cancelled(), "the innermost scope sees a cancel three levels up");
        Check(req.Cancelled(), "and so does the one in between");
        Check(conn.Cancelled(), "and the scope that was actually cancelled");
    }

    // One-way. A child aborting must not take its parent or its siblings with it -- one request
    // timing out does not close the connection.
    std::printf("a child's cancellation does not escape upward\n");
    {
        JLib::CancelScope conn;
        JLib::CancelScope a(conn.Token()), b(conn.Token());

        a.Cancel();
        Check(a.Cancelled(), "the cancelled child is cancelled");
        Check(!conn.Cancelled(), "its parent is not");
        Check(!b.Cancelled(), "and neither is its sibling");
    }

    // A child MAY outlive its parent. The link is a token, not a pointer, so this is a resolve
    // failure and not a dangle -- and critically the child must not start inheriting from whatever
    // scope claims the parent's slot next.
    std::printf("a child that outlives its parent stops inheriting, safely\n");
    {
        JLib::CancelToken childTok;
        alignas(JLib::CancelScope) unsigned char storage[sizeof(JLib::CancelScope)];
        JLib::CancelScope* child = nullptr;
        {
            JLib::CancelScope parent;
            child = new (storage) JLib::CancelScope(parent.Token());
            childTok = child->Token();
            Check(!childTok.Cancelled(), "not cancelled while the parent is alive and uncancelled");
        }   // parent destroyed; its slot goes back on the free list

        Check(!childTok.Cancelled(), "still not cancelled once the parent is gone");

        // The very next scope takes the parent's freed slot (LIFO). Cancelling it must not reach
        // the orphan -- if the generation check were missing, this is where it would show up.
        {
            JLib::CancelScope squatter;
            squatter.Cancel();
            Check(!childTok.Cancelled(), "and a NEW scope on the parent's old slot does not adopt it");
        }

        child->Cancel();
        Check(childTok.Cancelled(), "the orphan can still be cancelled in its own right");
        child->~CancelScope();
    }

    // CancelVia: cancel through a bare token. This is what a timer will use -- it cannot own the
    // scope, because the operation owns it and usually finishes first with the timer still queued.
    std::printf("CancelVia cancels through a token, and is inert once the scope is gone\n");
    {
        JLib::CancelToken stale;
        {
            JLib::CancelScope s;
            stale = s.Token();
            Check(JLib::CancelVia(s.Token()), "a live token reports that it cancelled something");
            Check(s.Cancelled(), "and the scope really is cancelled");
            Check(JLib::CancelVia(s.Token()), "cancelling twice is idempotent, not an error");
        }

        // The scope is gone: exactly the state a timer finds when its operation completed first.
        Check(!JLib::CancelVia(stale), "a stale token cancels nothing and says so");
        Check(!stale.Cancelled(), "and nothing was set behind it");

        // And it must not cancel whoever inherits the slot.
        JLib::CancelScope next;
        Check(!JLib::CancelVia(stale), "still inert once the slot has been re-issued");
        Check(!next.Cancelled(), "the new tenant is untouched");
    }

    // ---- scope slots recycle -------------------------------------------------------------------
    // There are 4,096 slots. Creating far more scopes than that proves ReleaseSlot returns them:
    // if it did not, everything past the 4,096th would fail open (Valid() == false) and cancellation
    // would silently stop working -- which is exactly the failure mode that would never show up in
    // a behavioural test, because failing open looks like "nothing was cancelled".
    //
    // Valid() IS NOT ENOUGH ON ITS OWN, and this test used to stop there. Valid() only says a slot
    // was obtained; it says nothing about whether the token still RESOLVES to it. So each scope is
    // also made to report its OWN cancellation -- the least it can possibly be asked to do, and the
    // thing that goes silently false if the generation check ever stops matching.
    //
    // The count is past 65,536 ON PURPOSE. The free list is LIFO, so create/destroy in a loop reuses
    // the SAME slot every time and drives one generation counter as fast as it can go. 65,536 is
    // where a 16-bit generation field wraps, and a per-frame scope reaches it in about 18 minutes at
    // 60fps -- well inside one play session, and invisible unless something checks resolution.
    std::printf("cancel scope slots recycle\n");
    {
        int invalid = 0;
        long long firstUnresolvable = -1;
        for (long long i = 0; i < 70000; ++i) {
            JLib::CancelScope s;
            if (!s.Valid()) { ++invalid; break; }
            s.Cancel();
            if (!s.Token().Cancelled()) { firstUnresolvable = i; break; }
        }
        Check(invalid == 0, "70,000 sequential scopes through a 4,096-slot table, all valid");

        char msg[160];
        std::snprintf(msg, sizeof msg,
                      "every one of them could still report its own cancellation%s",
                      firstUnresolvable < 0 ? ""
                                            : " -- FAILED OPEN, see iteration in the count below");
        Check(firstUnresolvable < 0, msg);
        if (firstUnresolvable >= 0)
            std::printf("      first scope that could not resolve its own token: %lld\n", firstUnresolvable);
    }

    // Same under contention: the free list is a Treiber stack with an ABA tag, and a lost or
    // duplicated slot shows up as either an invalid scope or two live scopes sharing one slot.
    std::printf("scope slots recycle under contention\n");
    {
        std::atomic<int> invalid{ 0 }, aliased{ 0 };
        std::vector<std::thread> ts;
        for (int t = 0; t < 8; ++t) {
            ts.emplace_back([&] {
                for (int i = 0; i < 5000; ++i) {
                    JLib::CancelScope a, b;
                    if (!a.Valid() || !b.Valid()) { invalid.fetch_add(1); return; }
                    // Two live scopes must never share a slot -- the low 16 bits are the index.
                    if ((a.Token().Raw() & 0xFFFF) == (b.Token().Raw() & 0xFFFF))
                        aliased.fetch_add(1);
                    // And they must be independently cancellable.
                    a.Cancel();
                    if (b.Cancelled()) aliased.fetch_add(1);
                }
            });
        }
        for (auto& th : ts) th.join();
        Check(invalid.load() == 0, "80,000 concurrent scope acquisitions all succeeded");
        Check(aliased.load() == 0, "no two live scopes ever shared a slot");
    }

    // ---- Event waiters can be cancelled, with the Treiber stack untouched ---------------------
    // The mechanism: Event::CancelWaiters walks a SIDE INDEX of the same waiters and marks them.
    // Nothing is removed from the stack, no node is disturbed, and the wake path is unchanged --
    // the cancellation is delivered when SignalAll resumes the task, exactly like a normal signal.

    // THE POINT OF THE FLAT WAITER INDEX. Event advertises itself as allocation-free on the wait
    // path, and the LockFreeHashMap version quietly broke that -- one slab cell per suspend, freed
    // per signal. Nothing behavioural would ever notice, so it gets measured directly: park N tasks
    // and confirm the slab grew by N (the tasks themselves) and not by 2N.
    std::printf("parking on an event allocates nothing beyond the tasks\n");
    {
        auto& ev = sched.GetEvent("alloc-probe");
        auto* alloc = JLib::TaskScheduler::Instance().GetAllocator();
        std::atomic<int> parked{ 0 };
        JLib::WaitGroup wg;

        const int kN = 64;
        // Materialise the slot array BEFORE the baseline: it is one lazy array per event, not a
        // per-waiter cost, and charging a one-time allocation to the per-wait path would be a
        // misleading measurement either way it came out.
        {
            JLib::WaitGroup warm;
            warm.n.fetch_add(1, std::memory_order_relaxed);
            auto* w = sched.CreateTask([&] { sched.WaitOnEvent(ev); },
                                       false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            w->waitGroup = &warm;
            sched.Push(w);
            std::this_thread::sleep_for(std::chrono::milliseconds(80));
            ev.SignalAll();
            sched.WaitFor(warm);
        }

        const long long base = alloc->LiveCount();
        const long long smallBase = alloc->SmallLiveCount();

        for (int i = 0; i < kN; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                parked.fetch_add(1, std::memory_order_relaxed);
                sched.WaitOnEvent(ev);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        Check(WaitUntil(parked, kN), "all probes reached the event");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));   // let them park

        const long long held = alloc->LiveCount() - base;
        std::printf("    slab slots held while %d tasks are parked: %lld\n", kN, held);
        // kN tasks are live and nothing else should be. The hash-map index made this 2*kN.
        Check(held <= kN, "no per-waiter slab allocation on the wait path");

        ev.SignalAll();
        sched.WaitFor(wg);
    }

    std::printf("Event waiters cancelled through the side index\n");
    {
        auto& ev = sched.GetEvent("cancel-waiters-probe");
        std::atomic<int> cancelled{ 0 }, normal{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;

        // Large enough that 16 buckets must hold several entries each -- at kN=6 no two keys would
        // share a bucket and the collision path would go untested.
        const int kN = 128;
        for (int i = 0; i < kN; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                parked.fetch_add(1, std::memory_order_relaxed);
                if (sched.WaitOnEventCancellable(ev) == JLib::WaitResult::Cancelled)
                    cancelled.fetch_add(1, std::memory_order_relaxed);
                else
                    normal.fetch_add(1, std::memory_order_relaxed);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->waitGroup = &wg;
            sched.Push(t);
        }

        Check(WaitUntil(parked, kN), "all waiters reached the event");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));   // let them actually park

        // EAGER. No SignalAll is issued here and none is coming -- WaitFor returning at all is the
        // proof. That is the whole point of eager wake on an Event: the condition it waits for may
        // never occur, so a skip-at-release waiter would sit here forever.
        ev.CancelWaiters();
        sched.WaitFor(wg);

        Check(cancelled.load() == kN, "every waiter woke Cancelled with no signal at all");
        Check(normal.load() == 0, "none reported a normal wake");

        // And they were REMOVED, not merely marked: a later signal must find nobody, or a waiter
        // could be resumed a second time after its task has already completed and been recycled.
        Check(!ev.SignalOne(), "cancelled waiters were taken out of the event");
    }

    // An uncancelled event wait must still behave exactly as before.
    std::printf("uncancelled Event wait is unchanged\n");
    {
        auto& ev = sched.GetEvent("normal-wait-probe");
        std::atomic<int> ok{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        for (int i = 0; i < 4; ++i) {
            wg.n.fetch_add(1, std::memory_order_relaxed);
            auto* t = sched.CreateTask([&] {
                parked.fetch_add(1, std::memory_order_relaxed);
                if (sched.WaitOnEventCancellable(ev) == JLib::WaitResult::Ok)
                    ok.fetch_add(1, std::memory_order_relaxed);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        Check(WaitUntil(parked, 4), "all waiters parked");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ev.SignalAll();
        sched.WaitFor(wg);
        Check(ok.load() == 4, "all four woke normally with Ok");
    }

    // ---- the pool still works afterwards ------------------------------------------------------
    std::printf("pool health after cancellation\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);
        // Same join-after-the-node ordering as above, and for the same reason: if the slab is
        // exhausted -- which it will be if the disposal path above ever regresses -- an eager
        // increment strands this group and the suite hangs instead of reporting.
        auto* t = sched.CreateTask([&] { ran.fetch_add(1, std::memory_order_relaxed); });
        JLib::TaskNode* n = t ? dag.CreateNode(t) : nullptr;
        Check(t && n, "a fresh task and node could still be allocated");
        if (t && !n) { DestroyTask(t); sched.GetAllocator()->Free(t); }
        if (t && n) {
            t->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
            Check(dag.Submit(), "a fresh DAG submitted");
            sched.WaitFor(wg);
            Check(ran.load() == 1, "an uncancelled graph still runs normally");
        }
    }

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
