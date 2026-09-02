// A DAG NODE CREATED WITH AN EXPLICIT PRIORITY MUST ACTUALLY RUN.
//
// TaskDAG::Fire dispatched on three flags -- isMain, isFork, isLocal -- and had no else. CreateNode
// sets `isLocal = (priority == NONE)`, so passing ANY priority cleared it and the node fell off the
// end of the chain: marked `submitted`, never pushed, dependents never counted down, WaitFor never
// returns. A permanent hang reachable through ordinary public API.
//
// WHY IT SAT UNDETECTED: CreateNode's default priority IS NONE, so every existing caller and every
// existing test takes the isLocal branch. The bug is not on the common path, it is in the parameter
// -- which is exactly the shape a suite full of default-argument callers cannot see.
//
// BOUNDED WAIT, NOT WaitFor. The failure being tested for is a HANG, and a test that hangs does not
// fail -- it spins every worker until something kills it. Twice today that cost more than a
// thousand CPU-seconds and made unrelated tests look flaky. A deadline turns the hang into a
// reported failure, which is the entire difference between a diagnosis and a mystery.

#include "../include/TaskScheduler.h"
#include "../include/TaskDAG.h"
#include <cstdio>
#include <atomic>
#include <chrono>
#include <thread>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-68s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

static bool AwaitCount(std::atomic<int>& v, int want, int ms = 3000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (v.load(std::memory_order_acquire) < want) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== DAG: a node with an explicit priority still runs ===\n");

    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();

    // ---- THE CONTROL FIRST: the default path, which always worked. -------------------------
    //
    // Runs before the interesting case so a build where the DAG is broken outright is caught here
    // rather than misattributed to the priority parameter.
    {
        std::atomic<int> ran{ 0 };
        TaskDAG dag(sched);
        auto* n = dag.CreateNode(sched.CreateInternalTask([&ran] {
            ran.fetch_add(1, std::memory_order_release);
        }));
        Check(n != nullptr, "control: a default-priority node was created");
        Check(dag.Submit(), "control: the graph submitted");
        Check(AwaitCount(ran, 1), "control: a DEFAULT-priority node runs (else the DAG is broken)");
    }

    // ---- THE BUG: an explicit priority. -----------------------------------------------------
    {
        std::atomic<int> ran{ 0 };
        TaskDAG dag(sched);
        auto* n = dag.CreateNode(sched.CreateInternalTask([&ran] {
            ran.fetch_add(1, std::memory_order_release);
        }), /*priority*/ 3);
        Check(n != nullptr, "a priority-3 node was created");
        Check(dag.Submit(), "the graph submitted");
        Check(AwaitCount(ran, 1),
              "a node with an EXPLICIT PRIORITY runs (it was marked submitted and never pushed)");
    }

    // ---- AND ITS DEPENDENTS STILL FIRE ------------------------------------------------------
    //
    // The hang was not only that the node never ran -- it is that `submitted` was already true, so
    // nothing could ever fire it again and every downstream countdown stalled behind it. Checking
    // the node alone would miss the half that makes it a graph-wide deadlock rather than one lost
    // task.
    {
        std::atomic<int> order{ 0 };
        std::atomic<int> upstreamAt{ -1 }, downstreamAt{ -1 };
        TaskDAG dag(sched);
        auto* up = dag.CreateNode(sched.CreateInternalTask([&] {
            upstreamAt.store(order.fetch_add(1, std::memory_order_acq_rel));
        }), /*priority*/ 7);
        auto* down = dag.CreateNode(sched.CreateInternalTask([&] {
            downstreamAt.store(order.fetch_add(1, std::memory_order_acq_rel));
        }));
        dag.AddDependency(down, up);   // down waits on up
        Check(dag.Submit(), "a two-node graph behind a prioritised node submitted");
        const bool done = AwaitCount(order, 2);
        std::printf("    upstream ran at %d, downstream at %d\n",
                    upstreamAt.load(), downstreamAt.load());
        Check(done, "BOTH nodes ran -- a stranded node deadlocks everything downstream of it");
        Check(!done || (upstreamAt.load() == 0 && downstreamAt.load() == 1),
              "and the edge was still honoured (upstream before downstream)");
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
