// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// HOW DOES A TaskDAG SCALE, and is the edge structure ever visible?
//
// The frame-DAG case in bench.cpp is a SIX-NODE graph. It measures dispatch latency and says
// nothing about adjacency: no node there has more than four dependents, so edge iteration is
// nanoseconds inside a ~22 us graph. That left a real question unanswered when TaskNode::dependents
// moved from a LockFreeList to pointer-linked chunk cells -- whether edge layout matters at all.
//
// THIS SEPARATES BUILD FROM EXECUTE, which is the whole point. They stress different things and
// mixing them is how you end up unable to attribute a number:
//
//   BUILD   -- CreateNode + AddDependency. This is where nodes and edge chunks are allocated, so
//              it is the phase the storage change actually touched.
//   EXECUTE -- Submit through the last node finishing. Dominated by dispatch and wake latency;
//              edge walking is one pass over each node's dependents.
//
// WHAT TO LOOK FOR. Read build ns/edge across the fan-out column: if edge allocation and linking
// cost anything, it shows there and it shows flat (it is O(1) per edge). Read execute us/node
// across the same row: if that is flat in fan-out too, then walking dependents is invisible next to
// dispatch, and CSR-vs-linked is not a question worth reopening. If it climbs with fan-out, it is.
//
// METHOD -- see the traps this project has already fallen into:
//   - ONE PROCESS. Every shape is measured here, not compared across binaries.
//   - Best-of-N per shape, not an average, so a scheduling hiccup cannot inflate a result.
//   - A warm-up graph before timing, so first-touch of the slab is not charged to shape #1.
//   - Per-EDGE and per-NODE normalisation, because absolute times across shapes are meaningless.
//   - The payload is empty on purpose: this measures the DAG, not the work. Anything else would
//       bury the signal under the thing being scheduled.

#include <TaskScheduler.h>
#include <TaskDAG.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <vector>

using Clock = std::chrono::steady_clock;
static double MsSince(Clock::time_point t0) {
    return std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
}

struct Shape {
    int nodes;
    int fanout;    // dependents per node; 1 == a chain
};

struct Result {
    double buildMs = 1e300;
    double execMs = 1e300;
    long long edges = 0;
};

// Builds a layered graph: each node depends on up to `fanout` nodes from the previous layer, which
// is the shape that actually varies dependents-per-node. A chain (fanout 1) is the degenerate case.
static Result RunShape(JLib::TaskScheduler& sched, const Shape& s, int reps) {
    Result best;
    for (int r = 0; r < reps; ++r) {
        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        long long edges = 0;

        const auto tBuild = Clock::now();
        JLib::TaskDAG dag(sched);
        std::vector<JLib::TaskNode*> prevLayer, curLayer;
        int made = 0;

        while (made < s.nodes) {
            const int layerSize = (s.fanout <= 1)
                ? 1
                : ((s.nodes - made) < s.fanout ? (s.nodes - made) : s.fanout);
            curLayer.clear();
            for (int i = 0; i < layerSize && made < s.nodes; ++i, ++made) {
                auto* t = sched.CreateTask([&ran] { ran.fetch_add(1, std::memory_order_relaxed); });
                if (!t) { std::printf("  CreateTask returned null -- slab too small\n"); return best; }
                t->waitGroup = &wg;
                wg.n.fetch_add(1, std::memory_order_relaxed);
                auto* n = dag.CreateNode(t);
                if (!n) { std::printf("  CreateNode returned null -- slab too small\n"); return best; }
                for (auto* p : prevLayer) { dag.AddDependency(n, p); ++edges; }
                curLayer.push_back(n);
            }
            prevLayer = curLayer;
        }
        const double buildMs = MsSince(tBuild);

        const auto tExec = Clock::now();
        if (!dag.Submit()) { std::printf("  Submit rejected the graph\n"); return best; }
        sched.WaitFor(wg);
        const double execMs = MsSince(tExec);

        if (ran.load() != s.nodes) std::printf("  WRONG: ran=%d expected=%d\n", ran.load(), s.nodes);
        if (buildMs < best.buildMs) best.buildMs = buildMs;
        if (execMs < best.execMs)  best.execMs = execMs;
        best.edges = edges;
    }
    return best;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    // Big enough for the widest shape below: nodes + tasks + edge chunks, with headroom for the
    // EBR lag that retires completed nodes rather than freeing them immediately.
    JLib::TaskScheduler::SetSlabSizes({ 1 << 20, 1 << 17, 1 << 17 });
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();

    std::printf("TaskDAG scaling -- workers=%zu\n", sched.GetWorkerCount());
    std::printf("empty payloads: this measures the DAG, not the work\n\n");

    // Warm-up: first touch of the slab and the worker wake path should not land on shape #1.
    RunShape(sched, Shape{ 64, 4 }, 1);

    const Shape shapes[] = {
        { 256, 1 }, { 256, 4 }, { 256, 16 }, { 256, 64 },
        { 2048, 1 }, { 2048, 4 }, { 2048, 16 }, { 2048, 64 },
        { 8192, 4 }, { 8192, 64 },
    };

    std::printf("%7s %7s %9s %12s %12s %11s %12s\n",
                "nodes", "fanout", "edges", "build ms", "build ns/edge", "exec ms", "exec us/node");
    std::printf("------------------------------------------------------------------------------------\n");
    for (const Shape& s : shapes) {
        const Result r = RunShape(sched, s, 3);
        if (r.buildMs > 1e200) continue;
        const double nsPerEdge = r.edges ? (r.buildMs * 1e6) / double(r.edges) : 0.0;
        std::printf("%7d %7d %9lld %12.3f %12.1f %11.3f %12.3f\n",
                    s.nodes, s.fanout, r.edges, r.buildMs, nsPerEdge,
                    r.execMs, (r.execMs * 1000.0) / double(s.nodes));
    }

    std::printf("\nslab shared-tier contention (needs -DJLIBSCHED_ALLOC_STATS=ON):\n");
    JLib::TaskAllocator::ReportStats();
    std::printf("\ntask sizes (needs -DJLIBSCHED_TASK_STATS=ON):\n");
    JLib::detail::ReportTaskSizes();
    // Fixed-size slab consumers, printed because the histogram above only covers CreateTask.
    // The allocator counter reports far more allocations than tasks created: the rest are these,
    // and they take a full 256-byte slot each just as a bare Task does.
    std::printf("  fixed-size slab users: Task %zu B, TaskNode %zu B, DagEdge %zu B  (slot is %zu B)\n",
                sizeof(JLib::Task), sizeof(JLib::TaskNode), sizeof(JLib::DagEdge),
                JLib::TaskAllocator::SLOT);
    // LambdaTask sizes for representative captures. Printed rather than asserted because the
    // answer is COMPILER-DEPENDENT: F is a member after the Task base, and whether a small capture
    // fits depends on reuse of the base class tail padding -- which MSVC and GCC do not treat
    // identically. Task has 8 free bytes at 56-63 since the Treiber waiter stack was retired, so a
    // single 8-byte capture may cost nothing at all. That is worth knowing per toolchain, not
    // assuming.
    {
        int cap0 = 0; double cap1 = 0; char capBig[64]{};
        auto lEmpty = []{};
        auto lOne   = [&cap0]{ (void)cap0; };
        auto lTwo   = [&cap0, &cap1]{ (void)cap0; (void)cap1; };
        auto lBig   = [capBig]{ (void)capBig[0]; };
        std::printf("  LambdaTask by capture: empty %zu B, 1 ref %zu B, 2 refs %zu B, 64 B capture %zu B\n",
                    sizeof(JLib::LambdaTask<decltype(lEmpty)>), sizeof(JLib::LambdaTask<decltype(lOne)>),
                    sizeof(JLib::LambdaTask<decltype(lTwo)>),   sizeof(JLib::LambdaTask<decltype(lBig)>));
    }

    std::printf("\nread DOWN each block: if exec us/node is flat as fanout grows, walking dependents\n");
    std::printf("is invisible next to dispatch and edge layout is not worth reopening.\n");

    sched.Join();
    return 0;
}
