// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// JLib::Scheduler vs enkiTS, on one machine with one harness.
//
// WHY enkiTS SPECIFICALLY. It is the closest architectural peer: work-stealing, per-worker queues,
// no fibers. A gap against it localises to IMPLEMENTATION rather than design, which is what makes it
// actionable. A gap against oneTBB would confound the two -- you could not tell whether the deque or
// the partitioner was responsible.
//
// THIS IS CALIBRATION, NOT A CONTEST. The point is to find where the overhead is, which means
// running the cases that might lose. Predictions written down BEFORE measuring, so a surprise stays
// surprising instead of being rationalised afterwards:
//
//   1. THROUGHPUT -- expect enkiTS to win per task. It does not allocate: the caller owns the
//      ITaskSet and keeps it alive. JLib allocates a slab slot per submission because CreateTask
//      takes fire-and-forget lambdas with no lifetime burden on the caller. A gap here is a
//      deliberate API trade, not something left on the floor.
//   2. LATENCY -- genuinely unknown, which is why it is here. It exercises the wake path, where 26%
//      was found hiding by accident this week. A reference number makes that visible on purpose.
//   3. BLOCKING -- expect JLib to win, and the DIRECTION is structural rather than empirical: a
//      suspended fiber frees its worker by construction, a blocked enkiTS task holds one. What is
//      actually being measured is the CROSSOVER. Fibers cost something per task, so below some
//      block DURATION the simpler scheduler wins on overhead. "Above X us of blocking, this wins"
//      is more useful and more credible than "we win on blocking workloads". The axis is duration,
//      not blocked fraction -- benchmark 4 records why the fraction version measured nothing.
//
// FAIRNESS RULES, because every easy mistake here flatters one side. The first draft of this file
// broke two of them and reported enkiTS as 15x slower; both faults were the harness:
//   - EACH LIBRARY EXPRESSES EACH WORKLOAD THE WAY ITS AUTHOR INTENDED. enkiTS's bulk shape is ONE
//     ITaskSet with a range, not N single-item sets. Feeding it N sets measures enkiTS being used
//     wrong. Benchmark 1 reports BOTH shapes precisely so that difference is visible instead of
//     being silently folded into a headline number.
//   - ONE WAIT PER BATCH ON BOTH SIDES. Calling WaitforTask N times against JLib's single
//     WaitFor(WaitGroup) measures N wake-ups versus one, which is a latency test wearing a
//     throughput test's clothes. That was the entire 15x.
//   - enkiTS's per-thread pipe is 2^8 = 256 entries (gc_PipeSizeLog2, TaskScheduler.cpp). Past that,
//     AddTaskSetToPipe runs the task INLINE ON THE SUBMITTER as backpressure. That is a real
//     property a real user hits, so benchmark 1 sweeps across the boundary rather than sitting on
//     one side of it. Benchmarks that are not about the pipe stay under 256 per batch.
//   - Identical work per item, identical counts, identical warmup, median-of-N with spread shown.
//   - Same worker count from the same source.
//   - Neither library's numbers get quoted without the other's.

#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <TaskScheduler.h>
#include <TaskScheduler_enki.h>     // enkiTS, aliased by CMake to dodge the header name collision

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <algorithm>

using Clock = std::chrono::steady_clock;
static double Ms(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// Dependency-chained so the compiler cannot fold it, and so both libraries pay the same cost per
// item. Same lesson as the burst bench: `acc += i * k` has a closed form GCC finds and MSVC does
// not, which silently makes a benchmark measure different things on different platforms.
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
static void Report(const char* who, const std::vector<double>& runs, const char* unit = "ms") {
    const double med = Median(runs);
    const double lo = *std::min_element(runs.begin(), runs.end());
    const double hi = *std::max_element(runs.begin(), runs.end());
    printf("    %-16s %9.3f %-2s  (%.3f to %.3f, %3.0f%% spread)\n",
           who, med, unit, lo, hi, med > 0 ? 100.0 * (hi - lo) / med : 0.0);
}

static constexpr int kRuns      = 7;    // odd, so the median is a real sample
static constexpr int kWorkIters = 200;  // ~0.2 us of work per item

// WHICH LIBRARY THIS RUN MEASURES. Both default to true, which is correct ONLY because an idle JLib
// pool is parked and therefore harmless to whatever is being timed next to it. That stops being
// true the moment JLib is set to NoSleep: its workers then spin through enkiTS's benchmarks, 63
// threads on 32 CPUs, and enkiTS's own numbers move ~40% purely because a JLib setting changed.
//
// So a run that configures either library away from "gets out of the way when idle" must measure
// ONE library and nothing else. `--only=jlib` / `--only=enki` do that; the absent library's cells
// print as "--" rather than as a number nobody can attribute.
static bool g_doJ = true;
static bool g_doE = true;

// Median of a possibly-unmeasured series. Negative means "not run in this process", which the
// formatter turns into "--" -- deliberately not 0, because a zero in a latency column reads as a
// spectacular result rather than as missing data.
static double MedOr(const std::vector<double>& v) { return v.empty() ? -1.0 : Median(v); }
static void Cell(double v, int width, int prec = 1) {
    if (v < 0) printf(" %*s", width, "--");
    else       printf(" %*.*f", width, prec, v);
}

// ============================================================ 1. independent-task throughput
// Reported in BOTH enkiTS shapes:
//   "enkiTS N sets" -- the literal analogue of JLib's N tasks.
//   "enkiTS 1 set"  -- the author-intended shape for bulk homogeneous work.
//
// WHAT THE N-SETS COLUMN ACTUALLY MEASURES, since two plausible explanations were wrong: it is FLAT
// at ~20 us/task from n=64 to n=20000, which rules out the 256-entry pipe (that would degrade WITH
// n, and n=64 never reaches it), and it survived replacing WaitforAll with a dependency sentinel,
// which rules out the wait. It tracks WORKER COUNT instead -- 133 ns/task at 3 workers, ~340 at 7,
// ~20,000 at 31, for identical work. That is wake amplification: every AddTaskSetToPipe calls
// WakeThreadsForNewTasks, and with an idle pool each add wakes every waiting thread so that one of
// them can claim a single task. It is a real property of submitting many tiny sets from an external
// thread, and it is also precisely the usage enkiTS tells you to avoid. Read this column as "the
// cost of using enkiTS against its grain", not as enkiTS's throughput -- that is the next column.
struct EnkiOne : enki::ITaskSet {
    int id = 0;
    void ExecuteRange(enki::TaskSetPartition, uint32_t) override {
        g_sink.fetch_add(Spin((uint64_t)id, kWorkIters), std::memory_order_relaxed);
    }
};
struct EnkiSentinel : enki::ITaskSet {
    EnkiSentinel() { m_SetSize = 1; }
    void ExecuteRange(enki::TaskSetPartition, uint32_t) override {}
};
struct EnkiRange : enki::ITaskSet {
    void ExecuteRange(enki::TaskSetPartition r, uint32_t) override {
        uint64_t acc = 0;
        for (uint32_t i = r.start; i < r.end; ++i) acc ^= Spin(i, kWorkIters);
        g_sink.fetch_add(acc, std::memory_order_relaxed);
    }
};

static void BenchThroughput(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    printf("  1. independent-task throughput -- ns per task, lower is better\n");
    printf("     (N-sets column is wake amplification, not pipe backpressure -- see comment above)\n\n");
    printf("     %8s %10s %10s %10s %13s %11s\n",
           "tasks", "JLib Push", "JLib Batch", "JLib arr/32", "enkiTS N sets", "enkiTS 1 set");

    const int counts[] = { 64, 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> a, ab, b, c;

      if (g_doJ) {
        for (int r = 0; r < kRuns; ++r) {
            JLib::WaitGroup wg;
            wg.n.store(n, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < n; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void* p) {
                    g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                }, (void*)(intptr_t)i);
                if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
                t->waitGroup = &wg;
                jl.Push(t);
            }
            jl.WaitFor(wg);                                   // ONE wait, matching enkiTS's one
            a.push_back(Ms(t0, Clock::now()) * 1e6 / n);      // ns per task
        }
      }

        // JLib's OWN bulk submission shape. The rule that enkiTS gets expressed the way its author
        // intended cuts both ways, and the column above breaks it against JLib: per-task Push does
        // an O(n) serial CreateTask+Push+NotifyWorker on one thread, and each notify takes a worker
        // mutex (the lost-wakeup fix). PushBatch amortises that over the whole batch. Quoting only
        // the Push column against enkiTS's ranged set would be the same category of error as
        // feeding enkiTS N single-item sets -- just pointed the other way.
        batch.resize(n);
      if (g_doJ) {
        for (int r = 0; r < kRuns; ++r) {
            JLib::WaitGroup wg;
            wg.n.store(n, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < n; ++i) {
                batch[i] = jl.CreateTask(+[](void* p) {
                    g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kWorkIters), std::memory_order_relaxed);
                }, (void*)(intptr_t)i);
                if (!batch[i]) { printf("     JLib: CreateTask returned null\n"); return; }
                batch[i]->waitGroup = &wg;
            }
            jl.PushBatch(batch.data(), (size_t)n);
            jl.WaitFor(wg);
            ab.push_back(Ms(t0, Clock::now()) * 1e6 / n);
        }
      }

        // PushArray at chunk 32 -- JLib's amortised shape, and the RIGHT counterpart to enkiTS's
        // ranged set: both turn n items into a few dozen scheduled entities. The two per-task
        // columns above are the per-ENTITY cost, which is a different quantity, and comparing one
        // to the other in either direction is the category error this file keeps warning about.
        //
        // ONE ASYMMETRY LEFT, and it runs against JLib: fn is per-item, so this column does n
        // atomic fetch_adds on g_sink while EnkiRange accumulates into a local and does one per
        // PARTITION. Benchmark 5 prices that contention at ~3-9 ns/item, which at these magnitudes
        // is a meaningful part of the column. It is left in rather than tuned away because a
        // per-item callable is the point of the API -- but the number is a ceiling, not a floor.
        std::vector<double> pa;
      if (g_doJ) {
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

        // Sets live outside the timer: the caller owns them by design, so charging enkiTS an
        // allocation it does not make would be inventing a loss.
        //
        // The wait is a SENTINEL WITH N DEPENDENCIES, which is enkiTS's actual analogue of a
        // WaitGroup. Two wrong ways to do this, both tried, both measured, both discarded:
        //   - WaitforTask on each of the N sets: N wake-ups against JLib's one. A latency test.
        //   - WaitforAll(): not a per-batch wait at all. It latches m_bWaitforAllCalled true
        //     permanently and INJECTS A DUMMY PINNED TASK into another running thread, scanning up
        //     to m_NumThreads to find one. Its cost tracks worker count, not task count -- 133 ns
        //     per task at 3 workers, 20,000 ns at 31, for identical work. That is the WaitforAll
        //     implementation being measured, not enkiTS.
      if (g_doE) {
        std::vector<EnkiOne> sets(n);
        for (int i = 0; i < n; ++i) { sets[i].id = i; sets[i].m_SetSize = 1; }
        EnkiSentinel sentinel;
        std::vector<enki::Dependency> deps(n);
        for (int i = 0; i < n; ++i) sentinel.SetDependency(deps[i], &sets[i]);
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            for (int i = 0; i < n; ++i) enki_.AddTaskSetToPipe(&sets[i]);
            enki_.WaitforTask(&sentinel);     // runs automatically once all N complete
            b.push_back(Ms(t0, Clock::now()) * 1e6 / n);
        }
      }

      if (g_doE) {
        EnkiRange one;
        one.m_SetSize = (uint32_t)n;
        one.m_MinRange = 1;
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            enki_.AddTaskSetToPipe(&one);
            enki_.WaitforTask(&one);
            c.push_back(Ms(t0, Clock::now()) * 1e6 / n);
        }
      }

        // MedOr, never Median: an unmeasured library leaves its vector EMPTY, and Median indexes
        // v[size/2] unconditionally, so a bare Median() on it reads out of bounds. That crash is
        // what --only= surfaced the first time it ran.
        printf("     %8d", n);
        Cell(MedOr(a), 10); Cell(MedOr(ab), 10); Cell(MedOr(pa), 10);
        Cell(MedOr(b), 13); Cell(MedOr(c), 11);
        printf("\n");
    }
    printf("\n");
}

// ============================================================ 2. bulk data-parallel
// Each library's native bulk shape. This compares PARTITIONING as much as scheduling -- JLib splits
// on a measured work probe, enkiTS on SetSize/MinRange -- which is the honest framing of it.
static void BenchBulk(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    constexpr int kItems = 20000;
    printf("  2. bulk parallel-for (%d items, native shape on both sides)\n", kItems);

    std::vector<double> a, b, c;
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

    // ParallelRange is the MECHANISM-MATCHED counterpart to enkiTS here: both hand workers slices
    // off a shared cursor rather than materialising a task per chunk, and both take a range
    // callable. ParallelFor stays in the table because it is what a caller who does not know their
    // workload should reach for -- and on THIS body, which costs the same per item, it should win:
    // finer slicing buys nothing when there is nothing to balance. The interesting comparison is
    // ParallelRange against enkiTS, since that is like against like.
    for (int r = 0; r < kRuns; ++r) {
        auto t0 = Clock::now();
        jl.ParallelRange(0, kItems, 256, [](int lo, int hi) {
            uint64_t acc = 0;
            for (int i = lo; i < hi; ++i) acc ^= Spin((uint64_t)i, kWorkIters);
            g_sink.fetch_add(acc, std::memory_order_relaxed);
        });
        c.push_back(Ms(t0, Clock::now()));
    }
  }

  if (g_doE) {
    EnkiRange bulk;
    bulk.m_SetSize = kItems;
    bulk.m_MinRange = 256;
    for (int r = 0; r < kRuns; ++r) {
        auto t0 = Clock::now();
        enki_.AddTaskSetToPipe(&bulk);
        enki_.WaitforTask(&bulk);
        b.push_back(Ms(t0, Clock::now()));
    }
  }

    if (g_doJ) Report("JLib ParallelFor",   a);
    if (g_doJ) Report("JLib ParallelRange", c);
    if (g_doE) Report("enkiTS",             b);
    printf("\n");
}

// ============================================================ 3. round-trip latency
// Submit one task, wait for it, repeat. Pure wake path on both sides. enkiTS spins gc_SpinCount=10
// times before suspending on a SHARED completion semaphore, so a wake can land on the wrong waiter
// and get forwarded -- worth knowing when reading this row.
struct EnkiPing : enki::ITaskSet {
    void ExecuteRange(enki::TaskSetPartition, uint32_t) override {
        g_sink.fetch_add(1, std::memory_order_relaxed);
    }
};

static void BenchLatency(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    constexpr int kPings = 20000;
    printf("  3. round-trip latency (%d serial submit-and-wait round-trips)\n", kPings);

    std::vector<double> a, b;
  if (g_doJ) {
    for (int r = 0; r < 3; ++r) {
        auto t0 = Clock::now();
        for (int i = 0; i < kPings; ++i) {
            JLib::WaitGroup wg;
            wg.n.store(1, std::memory_order_relaxed);
            JLib::Task* t = jl.CreateTask(+[](void*) { g_sink.fetch_add(1, std::memory_order_relaxed); }, nullptr);
            if (!t) { printf("     JLib: CreateTask returned null\n"); return; }
            t->waitGroup = &wg;
            jl.Push(t);
            jl.WaitFor(wg);
        }
        a.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
    }
  }

  if (g_doE) {
    EnkiPing ping;
    ping.m_SetSize = 1;
    for (int r = 0; r < 3; ++r) {
        auto t0 = Clock::now();
        for (int i = 0; i < kPings; ++i) {
            enki_.AddTaskSetToPipe(&ping);
            enki_.WaitforTask(&ping);
        }
        b.push_back(Ms(t0, Clock::now()) * 1000.0 / kPings);
    }
  }

    if (g_doJ) Report("JLib", a, "us");
    if (g_doE) Report("enkiTS", b, "us");
    printf("\n");
}

// ============================================================ 4. blocking crossover
// THE ONE THAT MATTERS, and the only one whose result is a recommendation rather than a number.
//
// The mechanism: a JLib task that waits on an Event SUSPENDS ITS FIBER and hands the worker back to
// the pool; an enkiTS task that waits on a condition variable HOLDS ITS THREAD. std::condition_variable
// is the fair enkiTS expression -- it has no fiber-aware wait, so that is what its user writes.
//
// THE AXIS IS BLOCK DURATION, NOT BLOCKED FRACTION. A first version swept the fraction of tasks
// contending a mutex and JLib lost every row, which looked like a refutation and was actually a
// design error: with a ~1 us critical section the wait is shorter than a fiber suspend/resume, so
// parking costs more than it saves, AND the freed worker has nothing to do because the only thing
// keeping the other tasks from running was the same lock. Parking pays exactly when the block is
// long AND there is unrelated work to absorb the gap. So: fixed 25% blocked, sweep how long they
// block, model the block as waiting on something EXTERNAL (I/O, a fence) rather than on each other.
//
// Expected shapes, on the record before running. W = the non-blocking work, D = the block:
//   JLib   ~ max(D, W)   -- parked fibers overlap with the remaining work
//   enkiTS ~ D + W       -- blocked threads cannot run the remaining work, so the two serialise
// which predicts JLib winning by up to 2x, peaking where D == W, and tapering at both ends. If the
// measurement does not show a taper at both ends, the harness is wrong, not the theory.
//
// A FAIRNESS DEBT, stated rather than hidden: JLib submits 256 individual tasks per batch while
// enkiTS submits one ranged set, because JLib's blocking tasks need noFiber=0 individually. From
// benchmark 1 that is ~1.2 us/task of submission overhead JLib pays and enkiTS does not, worth
// ~300 us per batch -- comparable to D itself. The D=0 row measures exactly that gap, so read the
// DELTA COLUMNS (each library against its own D=0 baseline), which cancel it. The absolute columns
// are printed too, and JLib loses those outright at every D. Both are the truth; the delta is the
// one that isolates the mechanism being tested.
static constexpr int kBatch      = 256;
static constexpr int kBatches    = 10;
static constexpr int kBlockEvery = 4;         // 25% of tasks block
static constexpr int kHeavyIters = 50000;     // ~50 us per non-blocking task

// Batch handshake, shared by both libraries so neither gets a cheaper release path.
static std::atomic<int>  g_blockersLeft{ 0 };
static std::atomic<bool> g_released{ false };
static std::mutex              g_cvMutex;
static std::condition_variable g_cv;

// Releases after D microseconds. ONE signal on each side -- the lost-wakeup race (a fiber that
// registers after SignalAll has already fired would wait forever) is closed by WaitOnEventArmed at
// the waiter, not by re-signalling here. An earlier version looped `SignalAll(); sleep_for(20us);`
// until every blocker came through, which measured Windows' 15.6 ms timer granularity instead of
// the scheduler: sleep_for(20us) is a full quantum, and JLib's D=50 row read 13 ms per batch, 260x
// the block it was supposed to be modelling. Deadlines are spun, not slept, for the same reason.
static void Releaser(JLib::TaskScheduler* jl, int durationUs, bool jlibSide) {
    const auto deadline = Clock::now() + std::chrono::microseconds(durationUs);
    while (Clock::now() < deadline) std::this_thread::yield();
    if (jlibSide) {
        g_released.store(true, std::memory_order_release);
        jl->GetEvent("compare_io").SignalAll();
    } else {
        { std::lock_guard<std::mutex> g(g_cvMutex); g_released.store(true, std::memory_order_release); }
        g_cv.notify_all();
    }
}

struct EnkiBlock : enki::ITaskSet {
    void ExecuteRange(enki::TaskSetPartition r, uint32_t) override {
        for (uint32_t i = r.start; i < r.end; ++i) {
            if (i % kBlockEvery == 0) {
                std::unique_lock<std::mutex> lk(g_cvMutex);
                g_cv.wait(lk, [] { return g_released.load(std::memory_order_acquire); });
            } else {
                g_sink.fetch_add(Spin(i, kHeavyIters), std::memory_order_relaxed);
            }
        }
    }
};

static void BenchBlocking(JLib::TaskScheduler& jl, enki::TaskScheduler& enki_) {
    printf("  4. blocking crossover -- 25%% of tasks wait on an external signal\n");
    printf("     %d batches of %d; ms for all %d tasks, lower is better\n\n",
           kBatches, kBatch, kBatches * kBatch);
    printf("     %8s %10s %10s %8s %10s %10s %8s\n",
           "block us", "JLib", "enkiTS", "ratio", "JLib d", "enkiTS d", "d ratio");

    const int durations[] = { 0, 50, 150, 300, 600, 2000 };
    double jlBase = 0, enBase = 0;

    for (int d : durations) {
        std::vector<double> a, b;

      if (g_doJ) {
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int batch = 0; batch < kBatches; ++batch) {
                g_released.store(d == 0, std::memory_order_release);
                g_blockersLeft.store(d ? kBatch / kBlockEvery : 0, std::memory_order_release);
                std::thread rel;
                if (d) rel = std::thread(Releaser, &jl, d, true);

                JLib::WaitGroup wg;
                wg.n.store(kBatch, std::memory_order_relaxed);
                for (int i = 0; i < kBatch; ++i) {
                    // NOT `d && ...`: the D=0 baseline must run the SAME workload, otherwise it is
                    // 256 heavy tasks against the other rows' 192 and every delta is garbage. At
                    // D=0, g_released is already true, so these tasks still pay full fiber setup
                    // and event registration and then return immediately -- which is exactly what
                    // the baseline should charge. enkiTS's EnkiBlock always did this; JLib's side
                    // did not, so JLib's baseline was inflated by 33% more work than enkiTS's.
                    const bool blocks = (i % kBlockEvery) == 0;
                    // Blocking tasks MUST be fiber-backed (noFiber=0): WaitOnEvent needs a fiber to
                    // suspend. Non-blocking tasks keep the default noFiber=1, so the fiber cost is
                    // paid only where it buys something -- the whole point of the hybrid, and the
                    // reason the D=0 row is a fair baseline rather than a rigged one.
                    JLib::Task* t = blocks
                        // WaitOnEventArmed, not WaitOnEvent: 'arm' runs AFTER this fiber is
                        // registered as a waiter and marked parkable, so a release that already
                        // happened is caught by self-signalling instead of being missed. That is
                        // the same lost-wakeup shape as the cv's predicate on the enkiTS side, so
                        // both libraries are race-free by their own intended mechanism.
                        ? jl.CreateTask(+[](void*) {
                              JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                              s.WaitOnEventArmed("compare_io", [&s] {
                                  if (g_released.load(std::memory_order_acquire))
                                      s.GetEvent("compare_io").SignalAll();
                              });
                              g_blockersLeft.fetch_sub(1, std::memory_order_acq_rel);
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

      if (g_doE) {
        EnkiBlock set;
        set.m_SetSize = kBatch;
        set.m_MinRange = 8;
        for (int r = 0; r < 3; ++r) {
            auto t0 = Clock::now();
            for (int batch = 0; batch < kBatches; ++batch) {
                g_released.store(d == 0, std::memory_order_release);
                std::thread rel;
                if (d) rel = std::thread(Releaser, &jl, d, false);
                enki_.AddTaskSetToPipe(&set);
                enki_.WaitforTask(&set);
                if (rel.joinable()) rel.join();
            }
            b.push_back(Ms(t0, Clock::now()));
        }
      }

        // MedOr, never Median -- an unmeasured library's vector is empty and Median indexes it
        // unconditionally. Same trap as benchmark 1.
        const double ja = MedOr(a), eb = MedOr(b);
        if (d == 0) { jlBase = (ja < 0 ? 0 : ja); enBase = (eb < 0 ? 0 : eb); }
        const double jd = (ja < 0) ? -1.0 : ja - jlBase;
        const double ed = (eb < 0) ? -1.0 : eb - enBase;
        // A ratio is meaningless once either side is missing or JLib's own delta is near zero --
        // dividing by 0.02 ms of noise printed a "321x" that says nothing. Print a dash rather than
        // a number that invites being quoted.
        printf("     %8d", d);
        Cell(ja, 10, 2); Cell(eb, 10, 2);
        if (ja > 0 && eb > 0) printf(" %6.2fx", eb / ja); else printf("       -");
        Cell(jd, 10, 2); Cell(ed, 10, 2);
        if (jd > 0.5 && ed >= 0) printf(" %7.2fx\n", ed / jd); else printf("       -\n");
    }
    printf("\n     ratio > 1.00 means JLib is faster. The delta columns subtract each library's own\n");
    printf("     D=0 baseline, cancelling JLib's per-task submission overhead (see the header).\n\n");
}

// ============================================================ 5. JLib per-task cost breakdown
// JLib-only, no enkiTS. Benchmark 1 says PushBatch costs ~210 ns/task and that the figure is FLAT
// in worker count (186 ns at 3 workers, 218 at 31), which rules out contention -- not the WaitGroup
// line, not the deques. So it is a fixed per-task cost, and the question is which half owns it:
// CREATION on the submitting thread, or DISPATCH+EXECUTE+RECLAIM across the pool.
//
// Splitting the timer answers that directly, and it decides whether a CreateTaskBatch API is worth
// building. If creation is most of the cost, a batch allocator handing out CONTIGUOUS slots turns
// the free-list pointer chase into a linear prefetchable walk and should cut it hard. If dispatch
// dominates, batching creation cannot help much no matter how it is written, because the cost is on
// the consumer side: claim, run, decrement the WaitGroup, destruct, return the slot.
//
// Bodies are EMPTY here on purpose -- this measures scheduler overhead, not work.
static void BenchBreakdown(JLib::TaskScheduler& jl) {
    if (!g_doJ) return;   // JLib-only benchmark
    printf("  5. JLib per-task cost breakdown -- ns per task, empty bodies\n\n");
    printf("     %8s %10s %10s %10s %14s %8s %8s %8s\n",
           "tasks", "create", "dispatch", "total", "+shared sink", "arr/8", "arr/32", "arr/128");

    const int counts[] = { 256, 1024, 8192, 20000 };
    std::vector<JLib::Task*> batch;
    for (int n : counts) {
        std::vector<double> cr, di, sk;
        for (int pass = 0; pass < 2; ++pass) {
            // pass 1 repeats the whole thing with a body that does ONE fetch_add on a shared
            // atomic. That is the only difference, and it prices benchmark 1's g_sink: a per-task
            // shape does n contended RMWs on one cache line, while enkiTS's ranged shape does one
            // per PARTITION. That asymmetry is the harness's, not the scheduler's, so it has to be
            // quantified before benchmark 1's JLib column can be read as scheduler overhead.
            for (int r = 0; r < kRuns; ++r) {
                batch.resize(n);
                JLib::WaitGroup wg;
                wg.n.store(n, std::memory_order_relaxed);

                auto t0 = Clock::now();
                for (int i = 0; i < n; ++i) {
                    batch[i] = pass == 0
                        ? (JLib::Task*)jl.CreateTask(+[](void*) {}, nullptr)
                        : (JLib::Task*)jl.CreateTask(+[](void*) {
                              g_sink.fetch_add(1, std::memory_order_relaxed);
                          }, nullptr);
                    if (!batch[i]) { printf("     JLib: CreateTask returned null\n"); return; }
                    batch[i]->waitGroup = &wg;
                }
                auto t1 = Clock::now();
                jl.PushBatch(batch.data(), (size_t)n);
                jl.WaitFor(wg);
                auto t2 = Clock::now();

                if (pass == 0) {
                    cr.push_back(Ms(t0, t1) * 1e6 / n);
                    di.push_back(Ms(t1, t2) * 1e6 / n);
                } else {
                    sk.push_back(Ms(t0, t2) * 1e6 / n);
                }
            }
        }
        // PushArray at a few chunk sizes: same n items, ceil(n/chunk) tasks. Per-ITEM cost, so it
        // is directly comparable to the total column above -- that is chunk=1 by another name.
        double pa[3] = { 0, 0, 0 };
        const size_t chunks[3] = { 8, 32, 128 };
        for (int ci = 0; ci < 3; ++ci) {
            std::vector<double> v;
            for (int r = 0; r < kRuns; ++r) {
                JLib::WaitGroup wg;
                auto t0 = Clock::now();
                jl.PushArray(0, (size_t)n, chunks[ci], [](size_t) {}, &wg);
                jl.WaitFor(wg);
                v.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            pa[ci] = Median(v);
        }

        const double c = Median(cr), d = Median(di);
        printf("     %8d %10.1f %10.1f %10.1f %14.1f %8.1f %8.1f %8.1f\n",
               n, c, d, c + d, Median(sk), pa[0], pa[1], pa[2]);
    }
    printf("\n");
}

int main(int argc, char** argv) {
    // Line-buffered: this harness can crash or be killed by a watchdog, and fully-buffered stdout
    // loses every line up to that point -- which turns "it segfaulted somewhere" into a bisect.
    setvbuf(stdout, nullptr, _IONBF, 0);   // MSVC maps _IOLBF to _IOFBF, so only _IONBF really flushes
    // usage: CompareEnkiTS [poolSize] [nosleep]
    //
    // *** DO NOT QUOTE THE nosleep RUN AS A COMPARISON. *** It is here for JLib-side investigation
    // only. Both schedulers live in THIS process -- JLib takes hw-1 workers and enkiTS takes hw --
    // and under Sleep that is harmless because the pool not being benchmarked is parked. Under
    // NoSleep, JLib's workers spin through enkiTS's benchmarks as well: 63 threads on 32 CPUs.
    //
    // The proof it is confounded is in enkiTS's own numbers, which cannot legitimately move when a
    // JLib setting changes, and do: ranged per-item 15.3 -> 8.7 ns, latency 21.4 -> 18.2 us, bulk
    // 0.331 -> 0.220 ms. enkiTS got ~40% FASTER because JLib stopped sleeping -- almost certainly
    // core parking, since the spinners keep every core unparked and boosted, which outweighs the
    // contention. Both columns are then measuring the machine's power state as much as either
    // scheduler.
    //
    // The head-to-head numbers therefore come from the DEFAULT (Sleep) run. NoSleep figures belong
    // to SchedulerBench, which runs JLib alone in its own process with nothing else to perturb.
    //
    // The idle policy is a JLib-side knob only; enkiTS runs at ITS default in both cases, which is
    // the honest comparison -- enkiTS has its own spin-then-park (gc_SpinCount) and reconfiguring
    // one library to match the other's tuning is the same category of error as the harness faults
    // documented at the top of this file. Run it twice and report two JLib rows.
    size_t pool = 0;
    bool noSleep = false;
    for (int a = 1; a < argc; ++a) {
        if (strcmp(argv[a], "nosleep") == 0)    { noSleep = true; continue; }
        if (strcmp(argv[a], "--only=jlib") == 0) { g_doE = false; continue; }
        if (strcmp(argv[a], "--only=enki") == 0) { g_doJ = false; continue; }
        pool = (size_t)strtoul(argv[a], nullptr, 10);
    }
    // nosleep implies --only=jlib. A spinning JLib pool perturbs anything measured beside it, and
    // silently producing a confounded enkiTS column would be worse than refusing: the numbers look
    // perfectly reasonable and are wrong by ~40%.
    if (noSleep && g_doE) {
        g_doE = false;
        printf("note: nosleep implies --only=jlib; a spinning pool cannot share a process with\n"
               "      another scheduler being timed. Run --only=enki separately for its column.\n\n");
    }
    if (noSleep)
        JLib::TaskScheduler::SetIdlePolicy(JLib::TaskScheduler::IdlePolicy::NoSleep);

    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    // ONLY THE LIBRARY BEING MEASURED KEEPS RUNNING THREADS. That is the whole point of --only:
    // threads that do not exist cannot contend, cannot hold cores unparked, and cannot flatter or
    // penalise whatever is being timed.
    //
    // JLib is Init()ed either way, then immediately Join()ed when it is not the subject. Join is a
    // real teardown -- it sets stopFlag, notifies every worker, joins each thread and clears the
    // pool -- so nothing of JLib is left running while enkiTS is timed. Initialising and tearing
    // down is cheaper than the alternative, which is threading a nullable scheduler through every
    // benchmark: the singleton must exist for Instance() to be valid, and the benchmarks that do
    // not touch it are already gated.
    JLib::TaskScheduler::Init(pool);
    JLib::TaskScheduler& jl = JLib::TaskScheduler::Instance();
    if (!g_doJ) jl.Join();

    // Same worker count from the same source, or none of this means anything. JLib reserves the
    // submitting thread and spawns N workers; enkiTS counts the calling thread INSIDE its total, so
    // numThreadsTotal = workers + 1 leaves both with the same number of workers. Verified against
    // enkiTS's source rather than assumed: Initialize(n) sets numTaskThreadsToCreate = n - 1, and
    // its own default Initialize() passes hardware_concurrency() -- so enkiTS's default and JLib's
    // hw-1 are THE SAME CHOICE, counted from opposite ends.
    const uint32_t workers = (uint32_t)(pool ? pool : std::thread::hardware_concurrency() - 1);
    enki::TaskScheduler enki_;
    if (g_doE) enki_.Initialize(workers + 1);

    printf("\nJLib::Scheduler vs enkiTS   (workers=%u, affinity=none, JLib idle=%s, measuring=%s)\n",
           workers, noSleep ? "nosleep" : "sleep",
           (g_doJ && g_doE) ? "both" : (g_doJ ? "JLib only" : "enkiTS only"));
    printf("================================================================\n");
    printf("Calibration, not a contest. Predictions are in the file header.\n\n");

    // Warm whichever pools are live: spin threads up and touch the fiber cache before timing.
    if (g_doJ) {
        JLib::WaitGroup wg; wg.n.store(4096, std::memory_order_relaxed);
        for (int i = 0; i < 4096; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
    }
    if (g_doE) {
        EnkiRange w; w.m_SetSize = 4096; w.m_MinRange = 64;
        enki_.AddTaskSetToPipe(&w); enki_.WaitforTask(&w);
    }

    BenchThroughput(jl, enki_);
    BenchBulk(jl, enki_);
    BenchLatency(jl, enki_);
    BenchBlocking(jl, enki_);
    BenchBreakdown(jl);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doE) enki_.WaitforAllAndShutdown();
    if (g_doJ) jl.Join();
    return 0;
}
