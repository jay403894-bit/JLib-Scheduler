// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// TSAN probe -- NOT a benchmark. Exercises each lock-free structure enough times for TSAN to
// observe conflicting accesses, then exits. Deliberately small: a race is reported the FIRST time
// two conflicting accesses occur without intervening synchronisation, so volume buys nothing and
// costs 5-15x runtime under instrumentation. The full bench is the wrong target for this.
//
//   g++ -std=c++20 -O1 -g -fno-omit-frame-pointer -fsanitize=thread -I include \
//       src/*.cpp src/posix/*.cpp src/posix/ContextSwitch.s bench/tsan_probe.cpp \
//       -o /tmp/tsan_probe -lpthread
//   setarch $(uname -m) -R ./tsan_probe        # -R works around TSAN's ASLR mapping error
//
// READING THE OUTPUT -- two known blind spots, do not chase either:
//   * TaskDeque uses std::atomic_thread_fence, which TSAN CANNOT model (gcc warns -Wtsan). Reports
//     naming pop_bottom/steal are almost certainly that blind spot, not a real race.
//   * Fibers move stacks between threads without calling __tsan_switch_to_fiber, so TSAN may
//     attribute one thread's history to another. Reports straddling a fiber task are suspect.
// What IS trustworthy: reports where both stacks sit in Epochs, LockFreeList, WaitGroup, the fiber
// STATUS transitions, or TaskAllocator. Those are the structures an ARM port would stress.
//
// EXPECTED BASELINE (2026-08-17, x86-64 / gcc 13): exactly ONE report, and treat any other count as
// a finding. The known one is `Task::Task` racing `StealClassCompatible` inside TaskDeque::steal_if
// -- pred dereferences a task the thief has not claimed yet, so it can read a slab slot the owner
// concurrently recycled. It is real and it is KNOWN-SAFE for the reason written above steal_if in
// TaskDeque.h (CAS success proves the read was of the live task; every other outcome discards it).
// Do NOT suppress it and do not raise this number to make a run green -- a suppression here would
// also hide a genuine future bug in the same predicate.
//
// Build note: the command line above is INCOMPLETE. `src/posix/*.cpp` picks up Topology.cpp only;
// Fiber::Init lives in src/posix/<arch>/FiberInit.cpp and omitting it is a LINK error, not a
// compile one. Add `src/posix/x86_64/FiberInit.cpp` (or aarch64/). Build somewhere persistent --
// /tmp did not survive between wsl.exe invocations. And do not read `$?` after a pipeline: it
// reports the LAST command's status, so `g++ ... | head; echo $?` cheerfully reports a failed
// build as successful.
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <atomic>
#include <thread>
#include <cstdio>

static std::atomic<int> g_counter{0};

int main() {
    JLib::TaskScheduler::Init();
    auto& sched = JLib::TaskScheduler::Instance();

    // 1. noFiber fan-out: worker deques, the task allocator, and WaitGroup completion. noFiber runs
    //    on the worker's own OS stack, so these paths involve NO fiber switch -- anything reported
    //    here is real, not a fiber-attribution artifact.
    {
        JLib::WaitGroup wg;
        constexpr int kN = 400;
        wg.n.store(kN, std::memory_order_relaxed);
        for (int i = 0; i < kN; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {
                g_counter.fetch_add(1, std::memory_order_relaxed);
            }, nullptr);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        printf("fan-out done      : counter=%d (expect 400)\n", g_counter.load());
    }

    // 2. DAGs with real dependencies: TaskDAG's node graph and the LockFreeList behind
    //    AddDependency, plus the epoch manager that guards its reclamation. Several small graphs
    //    rather than one big one, so nodes are added AND retired repeatedly.
    for (int r = 0; r < 8; ++r) {
        JLib::TaskDAG dag(sched);
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        auto* a = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
        auto* b = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
        auto* c = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
        JLib::Task* last = sched.CreateTask(+[](void*) {}, nullptr);
        last->waitGroup = &wg;
        auto* d = dag.CreateNode(last);
        dag.AddDependency(b, a);
        dag.AddDependency(c, a);
        dag.AddDependency(d, b);
        dag.AddDependency(d, c);
        dag.Submit();
        sched.WaitFor(wg);
    }
    printf("dag rounds done   : 8\n");

    // 3. Fiber-backed tasks that actually suspend: the fiber status state machine, the global fiber
    //    pool's acquire/release, and the context switch itself. This is the section most likely to
    //    produce fiber-attribution noise -- and also the only one that touches those paths at all.
    {
        JLib::WaitGroup wg;
        constexpr int kF = 32;
        wg.n.store(kF, std::memory_order_relaxed);
        for (int i = 0; i < kF; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {
                g_counter.fetch_add(1, std::memory_order_relaxed);
            }, nullptr, /*hipri*/0, JLib::FiberSize::Standard, /*noFiber*/0);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        printf("fiber tasks done  : counter=%d (expect 432)\n", g_counter.load());
    }

    // 4. ParallelFor (demand-driven since 1.4): the NON-WORKER LANE, the speculative publish/reclaim path, and the
    //    relaxed live-slot counter in TaskAllocator.
    //
    //    Worth being specific about what is being asked here, because two of the three are exactly
    //    the categories this file's header calls suspect and trustworthy respectively.
    //
    //    * The lane is a TaskDeque, so push_bottom/pop_bottom/steal reports against it fall under
    //      the atomic_thread_fence blind spot -- expected, not news. The NEW question is whether
    //      the lane is ever driven by two threads at once, which the claim is supposed to prevent;
    //      that would show up as two OWNERS in the report rather than an owner and a thief.
    //    * TaskAllocator IS trustworthy per the header, and it is what matters most here:
    //      Alloc/Free now take a plain relaxed load+store on a shard instead of an RMW, justified
    //      by the claim that only the owning thread writes its own shard. If that reasoning is
    //      wrong, this is where it surfaces -- and it is a genuine data race, not a fence artifact.
    //    * The 3 concurrent non-worker callers are the case the single-owner claim exists for. Two
    //      lose the claim and fall through to RunCursorRange, which is itself worth exercising
    //      alongside a live splitter.
    {
        std::atomic<long long> sum{0};
        auto body = [&sum](int lo, int hi) {
            long long s = 0;
            for (int i = lo; i < hi; ++i) s += i;
            sum.fetch_add(s, std::memory_order_relaxed);
        };

        // From MAIN: claims the non-worker lane.
        for (int r = 0; r < 4; ++r) sched.ParallelFor(0, 20000, 32, body);

        // From INSIDE a worker task: uses that worker's own deque instead. Fiber-backed, because it
        // blocks in WaitFor.
        {
            JLib::WaitGroup wg;
            wg.n.store(1, std::memory_order_relaxed);
            JLib::Task* t = sched.CreateTask([&] {
                sched.ParallelFor(0, 20000, 32, body);
            }, /*hipri*/0, JLib::FiberSize::Standard, /*noFiber*/0);
            t->waitGroup = &wg;
            sched.Push(t);
            sched.WaitFor(wg);
        }

        // THREE non-worker threads at once: one wins the lane, the others must degrade cleanly.
        {
            std::thread a([&]{ for (int r = 0; r < 3; ++r) sched.ParallelFor(0, 20000, 32, body); });
            std::thread b([&]{ for (int r = 0; r < 3; ++r) sched.ParallelFor(0, 20000, 32, body); });
            std::thread c([&]{ for (int r = 0; r < 3; ++r) sched.ParallelFor(0, 20000, 32, body); });
            a.join(); b.join(); c.join();
        }

        // Every run covers [0,20000) exactly once, so the total is fixed: 14 runs of 199990000.
        const long long expect = 199990000LL * 14;
        printf("lazy splitting    : sum=%lld (expect %lld)%s\n",
               sum.load(), expect, sum.load() == expect ? "" : "   <-- COVERAGE WRONG");
    }

    printf("probe complete\n");
    return 0;
}
