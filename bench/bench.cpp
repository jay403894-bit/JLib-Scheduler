// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Scheduler microbenchmark -- console app built by the umbrella solution (Bench.vcxproj takes a
// ProjectReference on Scheduler), so it measures exactly the library every game links, not a
// special build. Build JLib.slnx, run build\x64\<Config>\SchedulerBench.exe.
//
// Four benches, chosen to mirror how the games actually use the scheduler:
//   1. throughput   -- how many no-op tasks/sec the pool can drain (create+push+steal+run+free)
//   2. latency      -- round-trip for ONE task: push -> worker runs it -> WaitFor returns
//   3. ParallelFor  -- real math over a big array, serial vs parallel, reported as speedup
//   4. frame DAG    -- a Game01-shaped graph (start -> {update,sprites,text,particles} -> present)
//                      built+submitted+drained per iteration, reported as graphs/sec and us/frame
#define NOMINMAX
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <chrono>
#include <cstdio>

// Case-insensitive compare for the affinity-policy argument. The two platforms spell it
// differently and neither name is standard C++: MSVC has _stricmp, POSIX has strcasecmp.
#if defined(_WIN32)
  #include <cstring>
  #define JLIB_STRICMP _stricmp
#else
  #include <strings.h>
  #define JLIB_STRICMP strcasecmp
#endif
#include <cstdint>
#include <vector>
#include <thread>
#include <algorithm>

using Clock = std::chrono::steady_clock;
static double MsBetween(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ---------------------------------------------------------------- 1. throughput
static void BenchThroughput(JLib::TaskScheduler& sched) {
    constexpr int kTasks = 200'000;
    constexpr int kRuns = 5;
    double best = 1e300;
    for (int run = 0; run < kRuns; ++run) {
        JLib::WaitGroup wg;
        wg.n.store(kTasks, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < kTasks; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        best = std::min(best, MsBetween(t0, Clock::now()));
    }
    printf("throughput   : %d no-op tasks in %.2f ms best-of-%d  ->  %.2f M tasks/sec\n",
        kTasks, best, kRuns, kTasks / best / 1000.0);
}

// ---------------------------------------------------------------- 2. latency
static void BenchLatency(JLib::TaskScheduler& sched) {
    constexpr int kIters = 20'000;
    auto t0 = Clock::now();
    for (int i = 0; i < kIters; ++i) {
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr);
        t->waitGroup = &wg;
        sched.Push(t);
        sched.WaitFor(wg);
    }
    double totalMs = MsBetween(t0, Clock::now());
    printf("latency      : %d serial round-trips in %.2f ms  ->  %.2f us per push->run->wait\n",
        kIters, totalMs, totalMs * 1000.0 / kIters);
}

// ---------------------------------------------------------------- 3. ParallelFor
static void BenchParallelFor(JLib::TaskScheduler& sched) {
    constexpr int kN = 1 << 24;            // 16M floats = 64 MB, well past every cache
    constexpr int kChunk = 1 << 16;        // 64K elements per task = 256 tasks
    constexpr int kRuns = 5;
    std::vector<float> data(kN, 1.0f);

    auto kernel = [&](int s, int e) {
        for (int i = s; i < e; ++i)
            data[i] = data[i] * 1.000001f + 0.5f; // enough math to not be pure bandwidth
    };

    double serialBest = 1e300, parallelBest = 1e300;
    for (int run = 0; run < kRuns; ++run) {
        auto t0 = Clock::now();
        kernel(0, kN);
        serialBest = std::min(serialBest, MsBetween(t0, Clock::now()));
    }
    for (int run = 0; run < kRuns; ++run) {
        auto t0 = Clock::now();
        sched.ParallelFor(0, kN, kChunk, kernel);
        parallelBest = std::min(parallelBest, MsBetween(t0, Clock::now()));
    }
    printf("ParallelFor  : 16M floats  serial %.2f ms | parallel %.2f ms (%d-elem chunks)  ->  %.2fx speedup\n",
        serialBest, parallelBest, kChunk, serialBest / parallelBest);
}

// ---------------------------------------------------------------- 4. frame-shaped DAG
static void BenchFrameDag(JLib::TaskScheduler& sched) {
    constexpr int kFrames = 20'000;
    constexpr int kRuns = 3;
    double best = 1e300;
    for (int run = 0; run < kRuns; ++run) {
        auto t0 = Clock::now();
        for (int f = 0; f < kFrames; ++f) {
            JLib::TaskDAG dag(sched);
            JLib::WaitGroup wg;
            wg.n.store(1, std::memory_order_relaxed);

            auto* start = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
            auto* update = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
            auto* sprites = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
            auto* text = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
            auto* parts = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
            JLib::Task* presentTask = sched.CreateTask(+[](void*) {}, nullptr);
            presentTask->waitGroup = &wg;
            auto* present = dag.CreateNode(presentTask);

            dag.AddDependency(update, start);
            dag.AddDependency(sprites, start);
            dag.AddDependency(sprites, update);
            dag.AddDependency(text, start);
            dag.AddDependency(parts, start);
            dag.AddDependency(present, sprites);
            dag.AddDependency(present, text);
            dag.AddDependency(present, parts);

            dag.Submit();
            sched.WaitFor(wg);
        }
        best = std::min(best, MsBetween(t0, Clock::now()));
    }
    printf("frame DAG    : %d 6-node graphs in %.2f ms best-of-%d  ->  %.0f graphs/sec, %.2f us/graph\n",
        kFrames, best, kRuns, kFrames / best * 1000.0, best * 1000.0 / kFrames);
}

static void BenchRecursiveForkJoin(JLib::TaskScheduler& sched);

// ------------------------------------------------- 5. ParallelFor CROSSOVER SWEEP
// The question this answers: ParallelFor's serial fallback triggers below 10,000 items, a constant
// picked BEFORE fork-join existed and never re-measured. Fork-join made dispatch cheaper, so the true
// crossover should have moved DOWN.
//
// The deeper problem the sweep exposes: element COUNT cannot express the crossover at all. What races
// dispatch overhead is TOTAL WORK = count x cost-per-element, and those differ by orders of magnitude
// between callers. So this sweeps both axes -- N and per-element cost -- and reports where parallel
// starts winning for each. One number cannot be right for all four rows; that is the finding.
//
// Calls ParallelForFJ DIRECTLY, deliberately: going through ParallelFor would hit the very 10k gate
// being measured and silently report serial-vs-serial below it.

// Per-element bodies of increasing cost. volatile-ish accumulation so the optimizer can't delete them;
// the results are summed into a global that gets printed.
static std::atomic<double> g_sink{ 0.0 };

// std::atomic<floating-point>::fetch_add is C++20 (P0020R6), and AppleClang 15's libc++ does not
// ship it -- which is what broke the macOS build while the LIBRARY itself compiled fine. A
// compare-exchange loop is the portable equivalent, works in C++17, and costs nothing here: no
// mainstream CPU has a native atomic FP add, so fetch_add lowers to this loop anyway. Removing it
// leaves the bench with no C++20 dependency at all.
static void SinkAdd(double v) {
    double cur = g_sink.load(std::memory_order_relaxed);
    while (!g_sink.compare_exchange_weak(cur, cur + v,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed)) {
        // cur was refreshed with the current value by the failed exchange; retry.
    }
}

template <int kFlops>
static double BodyCost(int i) {
    double x = (double)i * 0.5 + 1.0;
    for (int f = 0; f < kFlops; ++f) x = x * 1.000001 + 0.000001;   // dependent chain: no vectorizing away
    return x;
}

template <int kFlops>
static void SweepOne(JLib::TaskScheduler& sched, const char* label, const std::vector<int>& sizes) {
    printf("  %-10s |", label);
    int crossover = -1;
    double crossoverSerialUs = 0.0;

    for (int n : sizes) {
        constexpr int kRuns = 7;
        double bestSerial = 1e300, bestPar = 1e300;

        // Grain: aim for ~4 chunks per worker, which is the usual load-balancing sweet spot (enough
        // pieces to even out, few enough to keep per-chunk overhead down). Never below 1.
        const int workers = (int)std::max(1u, std::thread::hardware_concurrency() - 1u);
        const int grain   = std::max(1, n / (workers * 4));

        for (int r = 0; r < kRuns; ++r) {
            // --- serial: the exact call ParallelFor's else-branch makes ---
            auto t0 = Clock::now();
            double acc = 0.0;
            for (int i = 0; i < n; ++i) acc += BodyCost<kFlops>(i);
            bestSerial = std::min(bestSerial, MsBetween(t0, Clock::now()));
            SinkAdd(acc);

            // --- parallel ---
            std::vector<double> partials((size_t)workers * 8, 0.0);   // padded, but correctness only
            auto t1 = Clock::now();
            std::atomic<double> pacc{ 0.0 };
            // ParallelFor, not ParallelForFJ: now that the gate is probe-based rather than a fixed
            // element count, this measures the DECISION as well as the dispatch. Every cell should
            // read >= ~1.00x -- anything well below means the probe chose parallel when it shouldn't.
            sched.ParallelFor(0, n, grain, [&](int a, int b) {
                double local = 0.0;
                for (int i = a; i < b; ++i) local += BodyCost<kFlops>(i);
                // fetch_add on a double isn't available pre-C++20 atomics ops, so accumulate via CAS.
                double cur = pacc.load(std::memory_order_relaxed);
                while (!pacc.compare_exchange_weak(cur, cur + local, std::memory_order_relaxed)) {}
                });
            bestPar = std::min(bestPar, MsBetween(t1, Clock::now()));
            SinkAdd(pacc.load());
        }

        const double speedup = bestSerial / std::max(bestPar, 1e-9);
        if (crossover < 0 && speedup > 1.0) { crossover = n; crossoverSerialUs = bestSerial * 1000.0; }
        printf(" %5.2fx", speedup);
    }

    // The number that actually generalizes: how much SERIAL WORK (in microseconds) the loop has to
    // represent before parallelising it pays. If that figure is roughly constant across body costs,
    // it is the threshold the scheduler should be using instead of an element count.
    if (crossover > 0) printf("   | wins from N=%-7d (serial work there: %7.1f us)\n",
                              crossover, crossoverSerialUs);
    else               printf("   | serial wins throughout\n");
}

static void BenchParallelForCrossover(JLib::TaskScheduler& sched) {
    // Finer low end than the first pass: the interesting crossovers for expensive bodies sit between
    // 500 and 4000, and the original grid stepped straight over them.
    const std::vector<int> sizes = { 256, 512, 1000, 2000, 4000, 10000, 40000, 200000 };

    printf("\nParallelFor crossover sweep (speedup = serial/parallel; >1.00 means parallel wins)\n");
    printf("  gate: probe a prefix, extrapolate, parallelize when est. work >= 75us (was: N > 10000)\n");
    printf("  %-10s |", "body");
    for (int n : sizes) printf(" %5d", n);
    printf("   |\n");
    printf("  -----------+------------------------------------------------------+--------------------------------------\n");

    SweepOne<1>   (sched, "trivial",  sizes);   // ~1 flop   -- memory-bound-ish, worst case for parallel
    SweepOne<8>   (sched, "light",    sizes);   // ~8 flops
    SweepOne<64>  (sched, "medium",   sizes);   // ~64 flops -- a transform / integration step
    SweepOne<512> (sched, "heavy",    sizes);   // ~512 flops -- a solver iteration

    printf("  (sink %.1f -- printed only so the bodies can't be optimized away)\n", g_sink.load());
}

int main(int argc, char** argv) {
    // Worker binding policy, chosen on the command line. It has to be set BEFORE Init() (binding
    // happens at thread creation), and the scheduler is a process-wide singleton -- so one process
    // can only measure ONE policy. Compare by running the exe three times:
    //     bench.exe hard    (default -- SetThreadAffinityMask)
    //     bench.exe ideal   (SetThreadIdealProcessor: a hint Windows may override)
    //     bench.exe none    (placement entirely up to Windows)
    // What to look for: 'hard' should win on a quiet machine (cache locality, no migration). 'ideal'
    // should be close, and should degrade far more gracefully when something ELSE is loading the
    // machine -- which is the case hard affinity handles worst. Run each under load as well as idle;
    // measuring only on an idle box is what makes hard affinity look strictly better than it is.
    const char* policyName = "hard";
    auto policy = JLib::TaskScheduler::AffinityPolicy::Hard;
    if (argc > 1) {
        if (JLIB_STRICMP(argv[1], "ideal") == 0) { policy = JLib::TaskScheduler::AffinityPolicy::Ideal; policyName = "ideal"; }
        else if (JLIB_STRICMP(argv[1], "none") == 0) { policy = JLib::TaskScheduler::AffinityPolicy::None; policyName = "none"; }
        else if (JLIB_STRICMP(argv[1], "physical") == 0) { policy = JLib::TaskScheduler::AffinityPolicy::PhysicalOnly; policyName = "physical"; }
    }
    JLib::TaskScheduler::SetAffinityPolicy(policy);

    printf("JLib::Scheduler bench  (sizeof(Task)=%zu, hw threads=%u, affinity=%s)\n",
        sizeof(JLib::Task), std::thread::hardware_concurrency(), policyName);
    printf("----------------------------------------------------------------\n");

    JLib::TaskScheduler::Init();
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    // Warmup: get every worker spun up and fibers touched before measuring anything.
    {
        JLib::WaitGroup wg;
        wg.n.store(10'000, std::memory_order_relaxed);
        for (int i = 0; i < 10'000; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
    }

    BenchThroughput(sched);
    BenchLatency(sched);
    BenchParallelFor(sched);
    BenchRecursiveForkJoin(sched);
    BenchFrameDag(sched);
    BenchParallelForCrossover(sched);

    printf("----------------------------------------------------------------\n");
    printf("done.\n");
    return 0;
}

// ---------------------------------------------------------------- 5. recursive fork-join
static std::atomic<int> fj_pushed{0}, fj_failed{0}, fj_executed{0};

static void RecursiveForkJoinImpl(JLib::TaskScheduler& sched, int start, int end, int BASE_CASE) {
    if (end - start <= BASE_CASE) {
        // Leaf: simple compute work
        fj_executed.fetch_add(1);
        for (int i = start; i < end; ++i) {
            volatile float dummy = i * 1.000001f + 0.5f;
            (void)dummy;
        }
        return;
    }

    int mid = start + (end - start) / 2;
    // noFiber=false is the load-bearing argument: these tasks call WaitFor below, which SUSPENDS,
    // and only a fiber-backed task can suspend. It also makes this the only section of the bench
    // that exercises a fiber at all -- every other one uses the fn-pointer overload, which
    // defaults to noFiber=true.
    JLib::Task* left = sched.CreateTask([&sched, start, mid, BASE_CASE] {
        RecursiveForkJoinImpl(sched, start, mid, BASE_CASE);
    }, false, JLib::FiberSize::Standard, false);  // hipri=false, size=Standard, noFiber=false
    JLib::Task* right = sched.CreateTask([&sched, mid, end, BASE_CASE] {
        RecursiveForkJoinImpl(sched, mid, end, BASE_CASE);
    }, false, JLib::FiberSize::Standard, false);

    if (!left || !right) {
        printf("ERROR: CreateTask failed\n");
        return;
    }

    JLib::WaitGroup wg;
    left->waitGroup = &wg;
    right->waitGroup = &wg;
    wg.n.fetch_add(2, std::memory_order_relaxed);

    if (!sched.PushFork(left)) {
        fj_failed.fetch_add(1);
        printf("ERROR: PushFork(left) failed\n");
    } else {
        fj_pushed.fetch_add(1);
    }

    if (!sched.PushFork(right)) {
        fj_failed.fetch_add(1);
        printf("ERROR: PushFork(right) failed\n");
    } else {
        fj_pushed.fetch_add(1);
    }

    sched.WaitFor(wg);
}

static void BenchRecursiveForkJoin(JLib::TaskScheduler& sched) {
    constexpr int kN = 1 << 20;            // 1M elements
    constexpr int kBaseCase = 10'000;      // 10k-elem leaf
    constexpr int kRuns = 1;
    double best = 1e300;

    for (int run = 0; run < kRuns; ++run) {
        fj_pushed.store(0);
        fj_failed.store(0);
        fj_executed.store(0);
        auto t0 = Clock::now();

        // Wrap recursion in a task so it never blocks a main/worker thread
        JLib::WaitGroup wg;
        JLib::Task* task = sched.CreateTask([&sched, kN, kBaseCase] {
            RecursiveForkJoinImpl(sched, 0, kN, kBaseCase);
        }, false, JLib::FiberSize::Standard, false);  // noFiber=false: this task suspends, so it needs a fiber

        // RecursiveForkJoinImpl checks its two CreateTask results; this one never did, so an
        // exhausted allocator surfaced as a write through nullptr instead of a message.
        if (!task) {
            printf("ERROR: CreateTask(outer) returned null -- allocator exhausted "
                   "(live=%lld / capacity=%zu)\n",
                   sched.GetAllocator()->LiveCount(), sched.GetAllocator()->Capacity());
            fflush(stdout);
            return;
        }

        task->waitGroup = &wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);
        sched.Push(task);  // Use Push (load-balanced), not PushFork (main thread)
        sched.WaitFor(wg);

        double ms = MsBetween(t0, Clock::now());
        best = std::min(best, ms);

        printf("fork-join    : completed run %d: %.2f ms (pushed=%d, executed=%d)\n",
            run, ms, fj_pushed.load(), fj_executed.load());
        fflush(stdout);
    }

    printf("fork-join    : 1M recursive (10k-elem leaf) best-of-%d  ->  %.2f ms\n",
        kRuns, best);
}
