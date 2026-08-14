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
#include <marl/event.h>

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

// ---------------------------------------------------------------- blocking crossover
// THE ONLY LIKE-FOR-LIKE ROW IN THE WHOLE COMPARISON, and the reason marl is worth measuring at
// all. Against enkiTS and Taskflow this workload is barely a contest: their blocking task holds a
// THREAD while a fiber here suspends and frees the worker, so the result is close to arithmetic.
// marl suspends a fiber too -- marl::Event::wait() from inside a marl task yields the fiber and
// hands the thread back, exactly as WaitOnEvent does. So this measures two implementations of the
// same idea with no structural advantage on either side.
//
// PREDICTION, WRITTEN BEFORE THE FIRST RUN. It is recorded here rather than in a commit message
// because the whole point is that it cannot be quietly revised after seeing the output:
//
//   1. PURE SUSPEND/RESUME: CLOSE. Both are hand-written per-architecture assembly doing the same
//      register save/restore, and BOTH POOL FIBERS -- marl reuses from Worker::idleFibers, this
//      reuses from ThreadLocalCache over GlobalFiberPool. Neither pays fresh fiber creation in
//      steady state, so there is no basis to predict either is faster.
//
//   2. MIXED WORKLOAD: THIS SHOULD WIN, by roughly the fraction of tasks that never touch a fiber.
//      marl fiber-backs EVERY task; here only noFiber=0 tasks do, and this benchmark is 25%
//      blocking, so 75% of its tasks skip the pool round-trip entirely. That -- not a faster fiber
//      -- is the hybrid's actual claim, and it is what this row exists to test.
//
//   3. AT D=0 the gap should be at its widest in this scheduler's favour, and it should NARROW as D
//      grows, because once the block dominates, what the other 75% cost stops mattering.
//
// Prediction 2 is the one worth being suspicious of: an earlier version of this comment argued the
// OPPOSITE, on the grounds that opt-in fibers must be the less-tuned path and that marl gets its
// fibers for free. Both premises were wrong -- "not the default" is not "less optimised", and marl
// pools fibers exactly as this does. Four other predictions during this comparison work were also
// refuted by measurement (pendingTasks' cost, SpinBriefly twice, warm-worker placement). Treat the
// numbers below as the finding and everything above as a hypothesis that was cheap to write down.
//
// RESULT, FIRST RUN -- PREDICTION 3 REFUTED, AND INVERTED:
//
//     block us     JLib     marl    ratio
//            0    11.62     4.73    0.41x
//           50     6.95     5.24    0.75x
//          150     6.75     6.46    0.96x
//          300     6.79     7.68    1.13x
//          600     8.30    10.72    1.29x
//         2000    22.14    24.49    1.11x
//
// The gap is widest in MARL's favour at D=0 (2.4x) and this scheduler only overtakes past ~200 us
// -- exactly opposite to the predicted shape. Prediction 1 (close) holds only around the crossover.
//
// AND THE LIKELY CAUSE IS NOT FIBERS. GetEvent(name) takes a GLOBAL registryMtx and hashes a
// std::string, and each blocking task here hits it TWICE: once inside WaitOnEventArmed and once in
// the arm callback's SignalAll. That is ~1280 acquisitions of one mutex per run, spread over 31
// workers. marl::Event is a bare handle with no registry, no lock and no string. So this row as
// written compares JLib's NAMED-EVENT REGISTRY against marl's DIRECT HANDLE, and the fiber cost --
// the thing it was built to isolate -- is buried underneath that.
//
// It is still a real finding about this library rather than only a harness fault: the registry lock
// is a genuine serialisation point on the shared-wait path, and it is the same registryMtx convoy
// the GetEvent declaration warns about. But it means the number above must NOT be quoted as a fiber
// comparison.
//
// TO ISOLATE: rerun with WaitOnEventDirectArmed, which takes a pooled DirectEvent and touches no map
// and no global lock -- the true structural analogue of marl::Event. If the D=0 gap collapses, the
// registry was the whole story and the fiber paths are close as Prediction 1 said. If it does not,
// the difference is real and belongs to fiber acquisition. That test is not written yet.
static constexpr int kBatch      = 256;
static constexpr int kBatches    = 10;
static constexpr int kBlockEvery = 4;       // 25% of tasks block
static constexpr int kHeavyIters = 50000;   // ~50 us per non-blocking task

static std::atomic<bool> g_released{ false };

// Spun, not slept: sleep_for(20us) is a full ~15.6 ms quantum on Windows, which is how an earlier
// version of the enkiTS harness "measured" a 50 us block at 13 ms per batch.
static void ReleaseAfter(int durationUs, JLib::TaskScheduler* jl, marl::Event* ev) {
    const auto deadline = Clock::now() + std::chrono::microseconds(durationUs);
    while (Clock::now() < deadline) std::this_thread::yield();
    g_released.store(true, std::memory_order_release);
    if (jl) jl->GetEvent("compare_io").SignalAll();
    if (ev) ev->signal();
}

static void BenchBlocking(JLib::TaskScheduler& jl) {
    printf("  blocking crossover -- 25%% of tasks wait on an external signal\n");
    printf("     %d batches of %d; ms for all %d tasks, lower is better\n\n",
           kBatches, kBatch, kBatches * kBatch);
    printf("     %8s %11s %11s %9s\n", "block us", "JLib", "marl", "ratio");

    const int durations[] = { 0, 50, 150, 300, 600, 2000 };
    for (int d : durations) {
        std::vector<double> a, b;

        if (g_doJ) {
            for (int r = 0; r < 3; ++r) {
                auto t0 = Clock::now();
                for (int batch = 0; batch < kBatches; ++batch) {
                    g_released.store(d == 0, std::memory_order_release);
                    std::thread rel;
                    if (d) rel = std::thread(ReleaseAfter, d, &jl, nullptr);

                    JLib::WaitGroup wg;
                    wg.n.store(kBatch, std::memory_order_relaxed);
                    for (int i = 0; i < kBatch; ++i) {
                        // NOT gated on d: the D=0 baseline must run the SAME workload, or it is 256
                        // heavy tasks against the other rows' 192 and every comparison is garbage.
                        const bool blocks = (i % kBlockEvery) == 0;
                        JLib::Task* t = blocks
                            // Armed, so a release that already happened is caught by self-signalling
                            // rather than missed -- the same race-freedom marl gets from Mode::Manual.
                            ? jl.CreateTask(+[](void*) {
                                  JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                                  s.WaitOnEventArmed("compare_io", [&s] {
                                      if (g_released.load(std::memory_order_acquire))
                                          s.GetEvent("compare_io").SignalAll();
                                  });
                              }, nullptr, false, JLib::FiberSize::Standard, /*noFiber*/0)
                            : jl.CreateTask(+[](void* p) {
                                  g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kHeavyIters),
                                                   std::memory_order_relaxed);
                              }, (void*)(intptr_t)i);
                        if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
                        t->waitGroup = &wg;
                        jl.Push(t);
                    }
                    jl.WaitFor(wg);
                    if (rel.joinable()) rel.join();
                }
                a.push_back(Ms(t0, Clock::now()));
            }
        }
        if (g_doM) {
            for (int r = 0; r < 3; ++r) {
                auto t0 = Clock::now();
                for (int batch = 0; batch < kBatches; ++batch) {
                    // Manual: stays signalled, so all 64 waiters unblock and a task that reaches
                    // wait() after the signal returns immediately instead of stranding.
                    marl::Event ev(marl::Event::Mode::Manual, d == 0);
                    g_released.store(d == 0, std::memory_order_release);
                    std::thread rel;
                    if (d) rel = std::thread(ReleaseAfter, d, nullptr, &ev);

                    marl::WaitGroup wg(kBatch);
                    for (int i = 0; i < kBatch; ++i) {
                        const bool blocks = (i % kBlockEvery) == 0;
                        if (blocks) {
                            marl::schedule([wg, ev] { ev.wait(); wg.done(); });
                        } else {
                            marl::schedule([wg, i] {
                                g_sink.fetch_add(Spin((uint64_t)i, kHeavyIters),
                                                 std::memory_order_relaxed);
                                wg.done();
                            });
                        }
                    }
                    wg.wait();
                    if (rel.joinable()) rel.join();
                }
                b.push_back(Ms(t0, Clock::now()));
            }
        }

        const double ja = MedOr(a), mb = MedOr(b);
        printf("     %8d", d);
        Cell(ja, 11, 2); Cell(mb, 11, 2);
        if (ja > 0 && mb > 0) printf(" %8.2fx\n", mb / ja); else printf("        -\n");
    }
    printf("\n     ratio > 1.00 means JLib is faster.\n\n");
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
    BenchBlocking(jl);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doM) marl::Scheduler::unbind();
    if (g_doJ) jl.Join();
    return 0;
}
