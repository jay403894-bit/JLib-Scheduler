// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// JLib::Scheduler vs Taskflow. Read compare_enkits.cpp first -- it carries the fairness rules and,
// next to each number, the harness faults that corrupted it before those rules existed.
//
// SEPARATE BINARY FROM THE enkiTS HARNESS, for two reasons that are not style:
//   1. Taskflow 4.x sets CMAKE_CXX_STANDARD 20 REQUIRED. JLib is C++17 and stays C++17; only this
//      target moves, and keeping it out of the enkiTS translation unit means that comparison is
//      not silently rebuilt under a different standard than it was measured under.
//   2. ONE PROCESS PER LIBRARY. Measuring two schedulers in one process is only valid while every
//      pool present parks when idle -- the enkiTS harness proved that the hard way, where a
//      spinning JLib moved enkiTS's own numbers ~40%. A separate binary makes isolation structural
//      instead of a flag someone has to remember.
//
// WORKER ACCOUNTING, checked in the source rather than assumed, because all three libraries count
// differently and getting this wrong silently hands someone an extra core:
//   JLib     -- Init(N) spawns N workers; main is not a worker but DOES help while waiting, via
//               TryRunStolenNoFiberTask (noFiber tasks only).
//   enkiTS   -- Initialize(N) spawns N-1 and uses the CALLER as thread 0, a full worker.
//   Taskflow -- Executor(N) spawns N; the caller does NOT participate. corun_until is documented as
//               callable only "from a worker of the calling executor", so run(tf).wait() blocks.
// Equalised on THREADS THAT CAN EXECUTE TASKS, which is what bounds throughput: 31 + a helping main
// for JLib, 31 + main for enkiTS, and 32 spawned for Taskflow. Taskflow therefore has one more OS
// thread in existence, but the same execution width, and its extra thread is blocked rather than
// spinning so it costs nothing.

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <TaskScheduler.h>
#include <taskflow/taskflow.hpp>
// The umbrella header DECLARES the algorithms but does not define them -- for_each_index links only
// with this. A declaration-only use compiles fine and fails at link, which is a slow way to find out.
#include <taskflow/algorithm/for_each.hpp>

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <vector>
#include <atomic>
#include <thread>
#include <algorithm>

using Clock = std::chrono::steady_clock;
static double Ms(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

static inline uint64_t Spin(uint64_t seed, int iters) {
    uint64_t x = seed | 1;
    for (int i = 0; i < iters; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
    return x;
}
static std::atomic<uint64_t> g_sink{ 0 };

static double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}
static double MedOr(const std::vector<double>& v) { return v.empty() ? -1.0 : Median(v); }
static void Report(const char* who, const std::vector<double>& runs, const char* unit) {
    if (runs.empty()) { printf("    %-16s %9s\n", who, "--"); return; }
    const double med = Median(runs);
    const double lo = *std::min_element(runs.begin(), runs.end());
    const double hi = *std::max_element(runs.begin(), runs.end());
    printf("    %-16s %9.3f %-2s  (%.3f to %.3f, %3.0f%% spread)\n",
           who, med, unit, lo, hi, med > 0 ? 100.0 * (hi - lo) / med : 0.0);
}

static bool g_doJ = true;
static bool g_doT = true;

static void Cell(double v, int width, int prec = 1) {
    if (v < 0) printf(" %*s", width, "--");
    else       printf(" %*.*f", width, prec, v);
}

// ---------------------------------------------------------------- round-trip latency
// Submit one task, wait for it, repeat. Pure wake path. This is the workload where JLib's margin
// over enkiTS was largest (4.6 us vs 21.7), and the cause there was the WAIT path rather than the
// queue -- enkiTS suspends on a shared completion semaphore. Taskflow's is a different design
// again, so this row is the one worth having first.
static void BenchLatency(JLib::TaskScheduler& jl, tf::Executor& ex) {
    constexpr int kPings = 20000;
    printf("  round-trip latency (%d serial submit-and-wait round-trips)\n", kPings);

    std::vector<double> a, b;
    if (g_doJ) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                JLib::WaitGroup wg;
                wg.n.store(1, std::memory_order_relaxed);
                JLib::Task* t = jl.CreateTask(+[](void*) {
                    g_sink.fetch_add(1, std::memory_order_relaxed);
                }, nullptr);
                if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
                t->waitGroup = &wg;
                jl.Push(t);
                jl.WaitFor(wg);
            }
            a.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    if (g_doT) {
        // async() is Taskflow's single fire-and-forget task, and its future is the round-trip wait.
        // A Taskflow graph would be the wrong shape here: building a one-node graph per ping would
        // measure graph construction, which is the same category of error as feeding enkiTS N
        // single-item task sets.
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                auto fu = ex.async([] { g_sink.fetch_add(1, std::memory_order_relaxed); });
                fu.get();
            }
            b.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    Report("JLib", a, "us");
    Report("Taskflow", b, "us");
    printf("\n");
}

static constexpr int kRuns      = 7;
static constexpr int kWorkIters = 200;

// ---------------------------------------------------------------- independent-task throughput
// Taskflow's fire-and-forget shape is silent_async + one wait_for_all, which matches JLib's N
// CreateTask/Push against one WaitFor(WaitGroup). Both are then "N independent callables, one
// wait", which is the frame-graph shape and a fair fight.
static void BenchThroughput(JLib::TaskScheduler& jl, tf::Executor& ex) {
    printf("  independent-task throughput -- ns per task, lower is better\n\n");
    printf("     %8s %11s %11s %12s %13s\n",
           "tasks", "JLib Push", "JLib Batch", "JLib arr/32", "TF silent_async");

    const int counts[] = { 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> a, ab, pa, tfv;

        if (g_doJ) {
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg; wg.n.store(n, std::memory_order_relaxed);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    JLib::Task* t = jl.CreateTask(+[](void* p) {
                        g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                    }, (void*)(intptr_t)i);
                    if (!t) return;
                    t->waitGroup = &wg; jl.Push(t);
                }
                jl.WaitFor(wg);
                a.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            batch.resize(n);
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg; wg.n.store(n, std::memory_order_relaxed);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    batch[i] = jl.CreateTask(+[](void* p) {
                        g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                    }, (void*)(intptr_t)i);
                    if (!batch[i]) return;
                    batch[i]->waitGroup = &wg;
                }
                jl.PushBatch(batch.data(), (size_t)n);
                jl.WaitFor(wg);
                ab.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg;
                auto t0 = Clock::now();
                jl.PushArray(0, (size_t)n, 32, [](size_t i) {
                    g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
                }, &wg);
                jl.WaitFor(wg);
                pa.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
        }
        if (g_doT) {
            for (int r = 0; r < kRuns; ++r) {
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i)
                    ex.silent_async([i] {
                        g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
                    });
                ex.wait_for_all();
                tfv.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
        }

        printf("     %8d", n);
        Cell(MedOr(a), 11); Cell(MedOr(ab), 11); Cell(MedOr(pa), 12); Cell(MedOr(tfv), 13);
        printf("\n");
    }
    printf("\n");
}

// ---------------------------------------------------------------- bulk data-parallel
// Each library's native bulk shape. The Taskflow GRAPH IS BUILT OUTSIDE THE TIMER and re-run,
// because a Taskflow is explicitly a reusable object -- rebuilding it per iteration would measure
// graph construction, the same error as charging enkiTS for an allocation it does not make.
static void BenchBulk(JLib::TaskScheduler& jl, tf::Executor& ex) {
    constexpr int kItems = 20000;
    printf("  bulk parallel-for (%d items, native shape on both sides)\n", kItems);

    std::vector<double> a, b;
    if (g_doJ) {
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            jl.ParallelFor(0, kItems, 256, [](int lo, int hi) {
                uint64_t acc = 0;
                for (int i = lo; i < hi; ++i) acc ^= Spin((uint64_t)i, kWorkIters);
                g_sink.fetch_add(acc, std::memory_order_relaxed);
            });
            a.push_back(Ms(t0, Clock::now()));
        }
    }
    if (g_doT) {
        tf::Taskflow flow;
        flow.for_each_index(0, kItems, 1, [](int i) {
            g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
        });
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            ex.run(flow).wait();
            b.push_back(Ms(t0, Clock::now()));
        }
    }
    Report("JLib", a, "ms");
    Report("Taskflow", b, "ms");
    printf("\n");
}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);   // a crash must not swallow the trail; MSVC ignores _IOLBF

    size_t pool = 0;
    bool noSleep = false;
    for (int a = 1; a < argc; ++a) {
        if (strcmp(argv[a], "nosleep") == 0)   { noSleep = true; continue; }
        if (strcmp(argv[a], "--only=jlib") == 0) { g_doT = false; continue; }
        if (strcmp(argv[a], "--only=tf") == 0)   { g_doJ = false; continue; }
        pool = (size_t)strtoul(argv[a], nullptr, 10);
    }
    if (noSleep && g_doT) {
        g_doT = false;
        printf("note: nosleep implies --only=jlib; a spinning pool cannot share a process with\n"
               "      another scheduler being timed. Run --only=tf separately for its column.\n\n");
    }
    if (noSleep)
        JLib::TaskScheduler::SetIdlePolicy(JLib::TaskScheduler::IdlePolicy::NoSleep);

    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    JLib::TaskScheduler::Init(pool);
    JLib::TaskScheduler& jl = JLib::TaskScheduler::Instance();
    if (!g_doJ) jl.Join();   // real teardown: nothing of JLib runs while Taskflow is timed

    const uint32_t workers = (uint32_t)(pool ? pool : std::thread::hardware_concurrency() - 1);
    tf::Executor ex(workers + 1);   // see the worker-accounting note at the top

    printf("\nJLib::Scheduler vs Taskflow %d.%d.%d  (workers=%u, affinity=none, JLib idle=%s, measuring=%s)\n",
           TF_VERSION / 100000, TF_VERSION / 100 % 1000, TF_VERSION % 100,
           workers, noSleep ? "nosleep" : "sleep",
           (g_doJ && g_doT) ? "both" : (g_doJ ? "JLib only" : "Taskflow only"));
    printf("================================================================\n\n");

    if (g_doJ) {
        JLib::WaitGroup wg; wg.n.store(4096, std::memory_order_relaxed);
        for (int i = 0; i < 4096; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
    }
    if (g_doT) {
        tf::Taskflow warm;
        warm.for_each_index(0, 4096, 1, [](int) {});
        ex.run(warm).wait();
    }

    BenchThroughput(jl, ex);
    BenchBulk(jl, ex);
    BenchLatency(jl, ex);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doJ) jl.Join();
    return 0;
}
