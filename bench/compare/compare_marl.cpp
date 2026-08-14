// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// JLib::Scheduler vs marl. Read compare_enkits.cpp first -- it carries the fairness rules and,
// next to each number, the harness faults that corrupted it before those rules existed.
//
// WHY marl IS THE INTERESTING ONE. enkiTS and Taskflow are architectural CONTRASTS: no fibers, so a
// task that blocks holds its thread, and the comparison is hybrid-vs-threadpool. marl runs EVERY
// task on a fiber, which makes it the only true PEER -- the one library where a gap is about
// implementation of the same idea rather than a difference of ideas. It is also where the hybrid's
// central claim gets tested from the other side: marl pays fiber cost on every task, this pays it
// only on tasks that opt in, and the blocking workload is where that trade should be visible.
//
// marl IS ARCHIVED (last commit 2026-04-27). These numbers calibrate the fiber path; they are not a
// recommendation to adopt it, and the README says so.
//
// WORKER ACCOUNTING, checked in each library's source rather than assumed:
//   JLib     -- Init(N) spawns N workers; main is not a worker but helps while waiting, via
//               TryRunStolenNoFiberTask (noFiber tasks only).
//   marl     -- Scheduler::Config::setWorkerThreadCount(N) spawns N; the BOUND thread runs tasks
//               too when it blocks on a WaitGroup, so main participates much as it does here.
// Both therefore get N = hw-1 spawned plus a participating main.
//
// ONE PROCESS PER LIBRARY, same as the other two harnesses: --only=jlib / --only=marl, and nosleep
// forces --only=jlib because a spinning pool perturbs anything timed beside it.

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <TaskScheduler.h>
#include <marl/scheduler.h>
#include <marl/waitgroup.h>

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
static void Cell(double v, int width, int prec = 1) {
    if (v < 0) printf(" %*s", width, "--");
    else       printf(" %*.*f", width, prec, v);
}
static void Report(const char* who, const std::vector<double>& runs, const char* unit) {
    if (runs.empty()) { printf("    %-16s %9s\n", who, "--"); return; }
    const double med = Median(runs);
    const double lo = *std::min_element(runs.begin(), runs.end());
    const double hi = *std::max_element(runs.begin(), runs.end());
    printf("    %-16s %9.3f %-2s  (%.3f to %.3f, %3.0f%% spread)\n",
           who, med, unit, lo, hi, med > 0 ? 100.0 * (hi - lo) / med : 0.0);
}

static bool g_doJ = true;
static bool g_doM = true;
static constexpr int kRuns      = 7;
static constexpr int kWorkIters = 200;

// ---------------------------------------------------------------- independent-task throughput
// marl::schedule + WaitGroup is a direct structural match for JLib's CreateTask/Push + WaitGroup --
// closer than either of the other two libraries gets. The difference measured here is therefore
// close to a like-for-like cost of the two implementations of the same idea, except that every marl
// task lands on a fiber while JLib's default noFiber tasks do not.
static void BenchThroughput(JLib::TaskScheduler& jl) {
    printf("  independent-task throughput -- ns per task, lower is better\n\n");
    printf("     %8s %11s %11s %12s %11s\n",
           "tasks", "JLib Push", "JLib Batch", "JLib arr/32", "marl");

    const int counts[] = { 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> a, ab, pa, m;

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
        if (g_doM) {
            for (int r = 0; r < kRuns; ++r) {
                marl::WaitGroup wg(n);
                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    marl::schedule([wg, i] {
                        g_sink.fetch_add(Spin((uint64_t)i, kWorkIters), std::memory_order_relaxed);
                        wg.done();
                    });
                }
                wg.wait();
                m.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
        }

        printf("     %8d", n);
        Cell(MedOr(a), 11); Cell(MedOr(ab), 11); Cell(MedOr(pa), 12); Cell(MedOr(m), 11);
        printf("\n");
    }
    printf("\n");
}

// ---------------------------------------------------------------- round-trip latency
static void BenchLatency(JLib::TaskScheduler& jl) {
    constexpr int kPings = 20000;
    printf("  round-trip latency (%d serial submit-and-wait round-trips)\n", kPings);

    std::vector<double> a, b;
    if (g_doJ) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                JLib::WaitGroup wg; wg.n.store(1, std::memory_order_relaxed);
                JLib::Task* t = jl.CreateTask(+[](void*) {
                    g_sink.fetch_add(1, std::memory_order_relaxed);
                }, nullptr);
                if (!t) return;
                t->waitGroup = &wg; jl.Push(t); jl.WaitFor(wg);
            }
            a.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    if (g_doM) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < kPings; ++i) {
                marl::WaitGroup wg(1);
                marl::schedule([wg] {
                    g_sink.fetch_add(1, std::memory_order_relaxed);
                    wg.done();
                });
                wg.wait();
            }
            b.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
        }
    }
    Report("JLib", a, "us");
    Report("marl", b, "us");
    printf("\n");
}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);

    size_t pool = 0;
    bool noSleep = false;
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "nosleep") == 0)      { noSleep = true; continue; }
        if (strcmp(argv[i], "--only=jlib") == 0)  { g_doM = false; continue; }
        if (strcmp(argv[i], "--only=marl") == 0)  { g_doJ = false; continue; }
        pool = (size_t)strtoul(argv[i], nullptr, 10);
    }
    if (noSleep && g_doM) {
        g_doM = false;
        printf("note: nosleep implies --only=jlib; a spinning pool cannot share a process with\n"
               "      another scheduler being timed. Run --only=marl separately.\n\n");
    }
    if (noSleep)
        JLib::TaskScheduler::SetIdlePolicy(JLib::TaskScheduler::IdlePolicy::NoSleep);

    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    JLib::TaskScheduler::Init(pool);
    JLib::TaskScheduler& jl = JLib::TaskScheduler::Instance();
    if (!g_doJ) jl.Join();   // real teardown: nothing of JLib runs while marl is timed

    const uint32_t workers = (uint32_t)(pool ? pool : std::thread::hardware_concurrency() - 1);

    marl::Scheduler::Config cfg;
    cfg.setWorkerThreadCount((int)workers);
    marl::Scheduler marlSched(cfg);
    if (g_doM) marlSched.bind();

    printf("\nJLib::Scheduler vs marl  (workers=%u, affinity=none, JLib idle=%s, measuring=%s)\n",
           workers, noSleep ? "nosleep" : "sleep",
           (g_doJ && g_doM) ? "both" : (g_doJ ? "JLib only" : "marl only"));
    printf("================================================================\n");
    printf("marl is ARCHIVED (last commit 2026-04-27) -- calibration, not a recommendation.\n\n");

    if (g_doJ) {
        JLib::WaitGroup wg; wg.n.store(4096, std::memory_order_relaxed);
        for (int i = 0; i < 4096; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
    }
    if (g_doM) {
        marl::WaitGroup wg(4096);
        for (int i = 0; i < 4096; ++i) marl::schedule([wg] { wg.done(); });
        wg.wait();
    }

    BenchThroughput(jl);
    BenchLatency(jl);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doM) marl::Scheduler::unbind();
    if (g_doJ) jl.Join();
    return 0;
}
