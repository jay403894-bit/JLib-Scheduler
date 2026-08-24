// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// TaskDAG::CreateExternalNode -- a node completed by something outside the pool instead of by
// running. The point is to make an I/O completion or a GPU fence a dependency EDGE rather than
// something a task blocks on, so a thousand pending waits cost a few hundred bytes each instead of
// a 64KB fiber stack each.
//
// THE CASE THAT MATTERS IS ORDERING. An external completion may land BEFORE the node's own
// dependencies are satisfied -- an I/O can finish while the nodes ahead of it are still running.
// Fire() and SignalExternal can therefore arrive in either order, and exactly one of them must
// propagate to the dependents. Both orders are tested here; testing only the natural one would pass
// against an implementation that drops the early signal entirely, which is the likely bug.

#include "TaskScheduler.h"
#include "TaskDAG.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-62s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

static bool WaitFor(std::atomic<int>& v, int want, int ms = 4000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (v.load(std::memory_order_acquire) != want) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::yield();
    }
    return true;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("TaskDAG external nodes -- workers=%zu\n\n", sched.GetWorkerCount());

    // ---- signal AFTER the node is armed (the natural order) ---------------------------------
    std::printf("signal arrives after the dependencies are satisfied\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::TaskDAG dag(sched);

        auto* head = dag.CreateNode(sched.CreateTask([&] { ran.fetch_add(1); }));
        auto* ext  = dag.CreateExternalNode();
        auto* tail = dag.CreateNode(sched.CreateTask([&] { ran.fetch_add(10); }));

        dag.AddDependency(ext, head);    // ext waits on head
        dag.AddDependency(tail, ext);    // tail waits on ext
        Check(dag.Submit(), "DAG submitted");

        Check(WaitFor(ran, 1), "head ran");
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(ran.load() == 1, "tail did NOT run while the external node was pending");

        dag.SignalExternal(ext);
        Check(WaitFor(ran, 11), "tail ran once the external node was signalled");
    }

    // ---- signal BEFORE the node is armed (the race the rendezvous exists for) ---------------
    std::printf("signal arrives before the dependencies are satisfied\n");
    {
        std::atomic<int> ran{ 0 };
        std::atomic<bool> release{ false };
        JLib::TaskDAG dag(sched);

        // head spins until we let it go, so the external node is still un-armed when we signal.
        auto* head = dag.CreateNode(sched.CreateTask([&] {
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
            ran.fetch_add(1);
        }));
        auto* ext  = dag.CreateExternalNode();
        auto* tail = dag.CreateNode(sched.CreateTask([&] { ran.fetch_add(10); }));

        dag.AddDependency(ext, head);
        dag.AddDependency(tail, ext);
        Check(dag.Submit(), "DAG submitted");

        // Signal while head is still running: ext cannot have been fired yet.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        dag.SignalExternal(ext);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        Check(ran.load() == 0, "an early signal did not fire the dependents on its own");

        release.store(true, std::memory_order_release);
        Check(WaitFor(ran, 11), "the early signal was remembered: tail ran after head finished");
    }

    // ---- an external node with no dependencies is a root ------------------------------------
    std::printf("external node as a root\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::TaskDAG dag(sched);
        auto* ext  = dag.CreateExternalNode();
        auto* tail = dag.CreateNode(sched.CreateTask([&] { ran.fetch_add(1); }));
        dag.AddDependency(tail, ext);
        Check(dag.Submit(), "DAG submitted");

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        Check(ran.load() == 0, "nothing ran while the root external node was pending");
        dag.SignalExternal(ext);
        Check(WaitFor(ran, 1), "dependents ran on signal");
    }

    // ---- many pending at once: the reason this exists at all --------------------------------
    // 4,000 concurrently-pending nodes. As suspended fiber nodes that would be 4,000 x 64KB of
    // stacks against a budget that defaults to 64 per worker -- exhaustion, and inside a DAG
    // potentially a deadlock rather than a stall. As external nodes it is neither.
    std::printf("4,000 external nodes pending simultaneously\n");
    {
        const int kN = 4000;
        std::atomic<int> ran{ 0 };
        JLib::TaskDAG dag(sched);
        std::vector<JLib::TaskNode*> exts;
        exts.reserve(kN);

        for (int i = 0; i < kN; ++i) {
            auto* e = dag.CreateExternalNode();
            auto* t = dag.CreateNode(sched.CreateTask([&] { ran.fetch_add(1); }));
            dag.AddDependency(t, e);
            exts.push_back(e);
        }
        Check(dag.Submit(), "DAG submitted");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        Check(ran.load() == 0, "none fired while pending");

        // Signal from several threads at once -- the signal side must be thread-safe.
        std::vector<std::thread> signallers;
        for (int t = 0; t < 4; ++t) {
            signallers.emplace_back([&, t] {
                for (int i = t; i < kN; i += 4) dag.SignalExternal(exts[(size_t)i]);
            });
        }
        for (auto& th : signallers) th.join();

        Check(WaitFor(ran, kN), "all 4,000 dependents ran exactly once");
        if (ran.load() != kN) std::printf("      expected %d, got %d\n", kN, ran.load());
    }

    // ---- duplicate edges (regression) ---------------------------------------------------------
    // AddDependency used to key each entry on the dependent's pointer so a repeated edge was
    // dropped, while dependencies_left was incremented unconditionally. The counter and the edge
    // count then disagreed: the countdown could only reach 1, and Kahn's -- which uses the counter
    // as the in-degree -- never drained the node, so HasCycle reported a cycle in an acyclic graph
    // and Submit silently rejected it. This hung with nothing having run at all.
    std::printf("duplicate edges (regression)\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);

        auto* ta = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
        ta->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* a = dag.CreateNode(ta);

        auto* tb = sched.CreateTask([&ran] { ran.fetch_add(10, std::memory_order_relaxed); });
        tb->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* b = dag.CreateNode(tb);

        dag.AddDependency(b, a);
        dag.AddDependency(b, a);      // the same edge, twice
        Check(b->dependencies_left.load() == 2, "both edges were counted");
        Check(dag.Submit(), "an acyclic graph with a duplicate edge is NOT rejected");

        sched.WaitFor(wg);            // hung here before the fix
        Check(ran.load() == 11, "both nodes ran exactly once");
    }

    // ---- a rejected cyclic graph must release its WaitGroups (regression) ---------------------
    // Submit's cycle path destroyed the tasks without decrementing, so rejecting a bad graph hung
    // whoever was waiting on it -- a worse outcome than the cycle itself.
    std::printf("rejected cyclic graph releases its WaitGroups (regression)\n");
    {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        JLib::TaskDAG dag(sched);

        auto* ta = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
        ta->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* a = dag.CreateNode(ta);

        auto* tb = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
        tb->waitGroup = &wg; wg.n.fetch_add(1, std::memory_order_relaxed);
        auto* b = dag.CreateNode(tb);

        dag.AddDependency(b, a);
        dag.AddDependency(a, b);      // a genuine cycle
        Check(!dag.Submit(), "the cycle was detected and the graph rejected");

        sched.WaitFor(wg);            // hung here before the fix
        Check(true, "WaitFor returned rather than blocking on abandoned tasks");
        Check(ran.load() == 0, "nothing from a rejected graph ran");
    }

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
