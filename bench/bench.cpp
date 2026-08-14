// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Scheduler microbenchmark -- console app built by the umbrella solution (Bench.vcxproj takes a
// ProjectReference on Scheduler), so it measures exactly the library every game links, not a
// special build. Build JLib.slnx, run build\x64\<Config>\SchedulerBench.exe.
//
// Five benches, chosen to mirror how the games actually use the scheduler:
//   1a. throughput/1p -- no-op tasks/sec with ONE thread submitting (create+push+steal+run+free)
//   1b. throughput/mp -- the same total work, submitted from several tasks already on the pool
//   2.  latency       -- round-trip for ONE task: push -> worker runs it -> WaitFor returns
//   3.  ParallelFor   -- real math over a big array, serial vs parallel, reported as speedup
//   4.  frame DAG     -- a Game01-shaped graph (start -> {update,sprites,text,particles} -> present)
//                        built+submitted+drained per iteration, reported as graphs/sec and us/frame
//
// 1a and 1b exist as a PAIR because measuring only the first one is actively misleading, and this
// was learned the hard way. Sweeping pool size on 1a shows throughput collapsing from 3.4 M/s at 8
// workers to 0.8 M/s at 14 and staying there, which reads as the scheduler failing to scale. It is
// not: 1b, fork-join and the frame DAG are all flat across the same sweep on the same machine.
//
// What 1a actually measures past that point is a single producer being outrun. One thread creates
// and pushes roughly 3.4 M tasks/sec; once the pool can drain faster than that, the surplus workers
// find empty deques and spend their time steal-probing, and that traffic slows the producer down.
// Hence a CLIFF at the crossover rather than a gradual slope. That is a real and useful number --
// a main thread submitting a frame's work is a genuine pattern -- but it is a submission-rate
// measurement, not a capacity one, and the label now says so.
#define NOMINMAX
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <Thread.h>    // StealStatsRead/Reset -- no-ops unless built with JLIBSCHED_STEAL_STATS
#include <chrono>
#include <cstdio>

// Case-insensitive compare for the affinity-policy argument. The two platforms spell it
// differently and neither name is standard C++: MSVC has _stricmp, POSIX has strcasecmp.
#if defined(_WIN32)
  #include <cstring>
  #define JLIB_STRICMP  _stricmp
  #define JLIB_STRNICMP _strnicmp
#else
  #include <strings.h>
  #define JLIB_STRICMP  strcasecmp
  #define JLIB_STRNICMP strncasecmp
#endif
#include <cstdint>
#include <vector>
#include <thread>
#include <atomic>     // the section watchdog
#include <cstdlib>    // std::_Exit, used by the watchdog
#include <algorithm>
#include <string>      // std::to_string, for the pool-size label
#include <cstdlib>     // strtoul, for the pool-size argument
#include <cmath>       // std::sqrt, the compute-bound ParallelFor body
#include <utility>     // std::pair, returned by the measure helper

using Clock = std::chrono::steady_clock;
static double MsBetween(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ---- section watchdog ---------------------------------------------------------------------------
// This benchmark is the scheduler's smoke test in CI, which means a HANG here is a real signal and
// not just an inconvenience. Twice now the macOS arm64 job has sat at 30 minutes and been killed by
// the job timeout, reporting nothing except which STEP it died in -- so an intermittent lost wakeup
// cost half an hour of runner time and produced no information about where it happened.
//
// A per-section deadline fixes that. Each bench announces itself, and if one overruns, the watchdog
// names it and exits nonzero within a few minutes instead of thirty. That turns "the bench hung"
// into "the bench hung in fork-join", which is the difference between a guess and a lead.
//
// _Exit rather than abort: a deadlocked process cannot be relied on to unwind, and a nonzero exit is
// all CI needs. The limit is deliberately generous -- the crossover sweep is legitimately the
// slowest section and CI runners are shared VMs -- because a false positive here would be worse
// than the thing it is catching.
static std::atomic<const char*> g_section{ "startup" };
static std::atomic<bool> g_benchDone{ false };

static void Section(const char* name) { g_section.store(name, std::memory_order_release); }

static void StartSectionWatchdog(int perSectionSeconds) {
    std::thread([perSectionSeconds] {
        const char* last = g_section.load(std::memory_order_acquire);
        auto since = Clock::now();
        while (!g_benchDone.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            const char* now = g_section.load(std::memory_order_acquire);
            if (now != last) { last = now; since = Clock::now(); continue; }
            if (Clock::now() - since > std::chrono::seconds(perSectionSeconds)) {
                printf("\n*** WATCHDOG: section '%s' exceeded %ds -- treating as a HANG.\n",
                       last, perSectionSeconds);
                printf("*** This is the scheduler failing to drain, not a slow machine.\n");
                fflush(stdout);
                std::_Exit(1);
            }
        }
    }).detach();
}

// ------------------------------------------------- 1a. throughput, ONE producer (the main thread)
static constexpr int kThroughputTasks = 200'000;
static constexpr int kThroughputRuns  = 5;

// Prints nothing unless the library was built with -DJLIBSCHED_STEAL_STATS=ON. Probes per task is
// the number to watch: it says how many victim deques a worker had to touch, on average, to move
// one task through the system. Cheap work-stealing is a fraction of a probe per task. A large
// number means workers are spending their time asking empty deques for work.
static void ReportStealStats(const char* label) {
    if (!JLib::kStealStatsEnabled) return;
    long long probes = 0, hits = 0;
    JLib::StealStatsRead(probes, hits);
    printf("  steal/%s   : %lld probes, %lld hits (%.1f%% hit rate, %.2f probes per task)\n",
        label, probes, hits,
        probes ? 100.0 * (double)hits / (double)probes : 0.0,
        (double)probes / (double)(kThroughputTasks * kThroughputRuns));
    JLib::StealStatsReset();
}

static void BenchThroughputSingleProducer(JLib::TaskScheduler& sched) {
    double best = 1e300, bestPush = 0.0, bestDrain = 0.0;
    // Reset HERE, not at construction: workers steal-probe continuously while idle, so anything
    // accumulated before the measured region is dead time rather than work being done.
    JLib::StealStatsReset();
    for (int run = 0; run < kThroughputRuns; ++run) {
        JLib::WaitGroup wg;
        wg.n.store(kThroughputTasks, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < kThroughputTasks; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        // Split the measurement: everything up to here is SUBMISSION by one thread, everything
        // after is the pool finishing whatever is left. Without the split, "throughput" conflates a
        // serial producer with a parallel consumer and a change in either looks the same.
        auto t1 = Clock::now();
        sched.WaitFor(wg);
        auto t2 = Clock::now();
        const double total = MsBetween(t0, t2);
        if (total < best) { best = total; bestPush = MsBetween(t0, t1); bestDrain = MsBetween(t1, t2); }
    }
    printf("throughput/1p: %d no-op tasks in %.2f ms best-of-%d  ->  %.2f M tasks/sec  (1 producer)\n",
        kThroughputTasks, best, kThroughputRuns, kThroughputTasks / best / 1000.0);
    printf("               submit %.2f ms (%.2f M/s), drain-after-submit %.2f ms\n",
        bestPush, kThroughputTasks / bestPush / 1000.0, bestDrain);
    ReportStealStats("1p");
}

// ------------------------------------------------- 1b. throughput, SEVERAL producers on the pool
// Producers are TASKS rather than extra std::threads, deliberately. Spawning four more OS threads
// on a machine whose pool already owns every core would measure oversubscription as much as
// submission, and it would change what is being compared as the pool grows. Producing from inside
// the pool is also how a job system is actually driven once work is nested: fork-join already does
// exactly this, which is why it stays flat where 1a falls over.
//
// Four rather than one-per-worker so the count stays FIXED across a pool-size sweep. The whole
// point is comparing 8 workers against 31, and that comparison is meaningless if the producer count
// moves at the same time.
static constexpr int kProducers = 4;

namespace {
struct ProducerArg {
    JLib::TaskScheduler* sched;
    JLib::WaitGroup*     wg;
    int                  count;
    int                  failed;   // CreateTask returning nullptr means the slab ran dry
};
ProducerArg g_producerArgs[kProducers];
}

static void ProducerBody(void* p) {
    auto* a = static_cast<ProducerArg*>(p);
    for (int i = 0; i < a->count; ++i) {
        JLib::Task* t = a->sched->CreateTask(+[](void*) {}, nullptr);
        if (!t) { ++a->failed; a->wg->n.fetch_sub(1, std::memory_order_release); continue; }
        t->waitGroup = a->wg;
        a->sched->Push(t);
    }
}

// ------------------------------------------------- 1c. throughput via PushBatch
// PushBatch links a run of tasks locally and hands the whole chain to ONE inbox with a single
// notify, instead of paying a worker selection, an inbox push and a condition-variable signal per
// task. Given that the per-push wake turned out to dominate single-producer submission, this is the
// API-level answer to the same problem the fan-out cap addresses by policy, and it has been in the
// scheduler the whole time. It was never benchmarked, which is part of why the cost stayed hidden.
static void BenchThroughputBatched(JLib::TaskScheduler& sched) {
    constexpr int kChunk = 64;               // 200,000 divides evenly by this
    double best = 1e300, bestPush = 0.0, bestDrain = 0.0;
    JLib::Task* chunk[kChunk];
    JLib::StealStatsReset();
    for (int run = 0; run < kThroughputRuns; ++run) {
        JLib::WaitGroup wg;
        wg.n.store(kThroughputTasks, std::memory_order_relaxed);
        auto t0 = Clock::now();
        int made = 0;
        for (int i = 0; i < kThroughputTasks; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr);
            if (!t) { printf("throughput/batch: ERROR -- CreateTask returned null\n"); return; }
            t->waitGroup = &wg;
            chunk[made++] = t;
            if (made == kChunk) { sched.PushBatch(chunk, (size_t)made, 0); made = 0; }
        }
        if (made) sched.PushBatch(chunk, (size_t)made, 0);
        auto t1 = Clock::now();
        sched.WaitFor(wg);
        auto t2 = Clock::now();
        const double total = MsBetween(t0, t2);
        if (total < best) { best = total; bestPush = MsBetween(t0, t1); bestDrain = MsBetween(t1, t2); }
    }
    printf("throughput/bt: %d no-op tasks in %.2f ms best-of-%d  ->  %.2f M tasks/sec  (1 producer, PushBatch x%d)\n",
        kThroughputTasks, best, kThroughputRuns, kThroughputTasks / best / 1000.0, kChunk);
    printf("               submit %.2f ms (%.2f M/s), drain-after-submit %.2f ms\n",
        bestPush, kThroughputTasks / bestPush / 1000.0, bestDrain);
    ReportStealStats("bt");
}

// ------------------------------------------------- 1d. HEAVY burst from an idle pool
// The case none of the other benches covers, and the one that decides whether narrowing external
// fan-out is safe as a default. A frame loop is idle, then submits a handful of EXPENSIVE tasks.
//
// The worry is specific: a push notifies only its target worker. With wide fan-out each of the N
// tasks wakes its own worker and they all start at once. With a narrow cap they all land on one
// inbox, and the rest of the pool has to be recruited by STEALING -- which requires those workers to
// be awake to steal. If they slept through it, parallelism collapses to roughly one worker and the
// speedup below drops toward 1.0. Reported as speedup against a measured single-task time so it is
// self-calibrating rather than depending on a hand-tuned duration.
static std::atomic<unsigned long long> g_burstSink{ 0 };
static void BurstBody(void*) {
    // An xorshift CHAIN, not a summation. The obvious `acc += i * k` loop has a closed form and GCC
    // finds it: the same body measured 0.66 ms under MSVC and 0.02 ms under GCC, which silently
    // turned this into a task-overhead benchmark on one platform and a parallelism benchmark on the
    // other. Every iteration here depends on the previous one, so there is nothing to fold.
    unsigned long long x = 88172645463325252ull;
    for (int i = 0; i < 3'000'000; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
    g_burstSink.fetch_add(x, std::memory_order_relaxed);
}

static void BenchIdleBurst(JLib::TaskScheduler& sched) {
    constexpr int kBurst = 16;
    constexpr int kRuns  = 5;

    // Reference: one task, alone, on an otherwise idle pool.
    double solo = 1e300;
    for (int r = 0; r < kRuns; ++r) {
        JLib::WaitGroup wg; wg.n.store(1, std::memory_order_relaxed);
        auto t0 = Clock::now();
        JLib::Task* t = sched.CreateTask(BurstBody, nullptr);
        if (!t) { printf("burst        : ERROR -- CreateTask returned null\n"); return; }
        t->waitGroup = &wg; sched.Push(t);
        sched.WaitFor(wg);
        solo = std::min(solo, MsBetween(t0, Clock::now()));
    }

    double burst = 1e300;
    for (int r = 0; r < kRuns; ++r) {
        // Let the pool actually PARK before the burst; measuring a pool that is still spinning would
        // quietly answer a different question.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        JLib::WaitGroup wg; wg.n.store(kBurst, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < kBurst; ++i) {
            JLib::Task* t = sched.CreateTask(BurstBody, nullptr);
            if (!t) { printf("burst        : ERROR -- CreateTask returned null\n"); return; }
            t->waitGroup = &wg; sched.Push(t);
        }
        sched.WaitFor(wg);
        burst = std::min(burst, MsBetween(t0, Clock::now()));
    }

    printf("burst        : %d heavy tasks from an idle pool -> %.2f ms (1 task = %.2f ms, speedup %.1fx of %d)\n",
        kBurst, burst, solo, (solo * kBurst) / burst, kBurst);
}

static void BenchThroughputMultiProducer(JLib::TaskScheduler& sched) {
    double best = 1e300;
    int    totalFailed = 0;
    JLib::StealStatsReset();
    for (int run = 0; run < kThroughputRuns; ++run) {
        JLib::WaitGroup wg;
        wg.n.store(kThroughputTasks, std::memory_order_relaxed);

        // Split as evenly as possible; the first producer absorbs the remainder.
        const int per = kThroughputTasks / kProducers;
        auto t0 = Clock::now();
        for (int p = 0; p < kProducers; ++p) {
            g_producerArgs[p] = { &sched, &wg,
                                  (p == 0) ? (kThroughputTasks - per * (kProducers - 1)) : per, 0 };
            JLib::Task* t = sched.CreateTask(ProducerBody, &g_producerArgs[p]);
            if (!t) { printf("throughput/mp: ERROR -- CreateTask returned null for a producer\n"); return; }
            sched.Push(t);
        }
        sched.WaitFor(wg);
        best = std::min(best, MsBetween(t0, Clock::now()));
        for (int p = 0; p < kProducers; ++p) totalFailed += g_producerArgs[p].failed;
    }
    if (totalFailed)
        printf("throughput/mp: ERROR -- %d task allocations failed\n", totalFailed);
    printf("throughput/mp: %d no-op tasks in %.2f ms best-of-%d  ->  %.2f M tasks/sec  (%d producers)\n",
        kThroughputTasks, best, kThroughputRuns, kThroughputTasks / best / 1000.0, kProducers);
    ReportStealStats("mp");
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
//
// TWO cases, reported separately, because one number here is actively misleading.
//
// The memory-bound case used to be the only one printed, and it reports BELOW 1.00x on every
// machine measured so far (0.75x on a Ryzen laptop APU, 0.82x on a phone, 1.09x on an M1 Air, whose
// unified memory has the best bandwidth-per-core of the three). A reader sees that near the top of
// the output and concludes ParallelFor does not work. What it actually shows is that a DRAM-bound
// loop does not parallelise -- 64 MB at roughly two flops per element read and written, so every
// worker queues on the same memory controller. No scheduler fixes that, and this one already takes
// its better dispatch path here: 256 chunks is well past the fork-join crossover on any pool size.
//
// So the pair is the honest report. Same call, same scheduler, two workloads: one the memory system
// caps, one it does not.
static void BenchParallelFor(JLib::TaskScheduler& sched) {
    constexpr int kRuns = 5;
    const int workers = (int)std::max(1u, std::thread::hardware_concurrency() - 1u);

    // Best-of-kRuns for each of a serial and a parallel pass over the same data.
    auto measure = [&](int n, int chunk, auto&& body) {
        double serialBest = 1e300, parallelBest = 1e300;
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            body(0, n);
            serialBest = std::min(serialBest, MsBetween(t0, Clock::now()));
        }
        for (int r = 0; r < kRuns; ++r) {
            auto t0 = Clock::now();
            sched.ParallelFor(0, n, chunk, body);
            parallelBest = std::min(parallelBest, MsBetween(t0, Clock::now()));
        }
        return std::pair<double, double>{ serialBest, parallelBest };
    };

    // --- memory-bound: 64 MB, ~2 flops per element. Bandwidth decides this, not the scheduler. ---
    {
        constexpr int kN = 1 << 24;          // 16M floats = 64 MB, past every cache
        constexpr int kChunk = 1 << 16;      // 256 chunks
        std::vector<float> data(kN, 1.0f);
        auto body = [&](int s, int e) {
            for (int i = s; i < e; ++i) data[i] = data[i] * 1.000001f + 0.5f;
        };
        auto [ser, par] = measure(kN, kChunk, body);
        printf("ParallelFor  : memory-bound  16M floats, ~2 flop/elem  serial %6.2f ms | parallel %6.2f ms  ->  %5.2fx\n",
            ser, par, ser / par);
        printf("               (this one is capped by the memory system, not the scheduler: it scales on a\n"
               "                large L3 that holds much of the 64 MB, and lands BELOW 1.00x where it does\n"
               "                not -- 0.75x measured on a Ryzen laptop APU, 1.09x on an M1 Air)\n");
    }

    // --- compute-bound: cache-resident, ~200 cycles per element. Now the cores decide. ---
    // 1 MB rather than 256 KB: at 64K elements the whole parallel pass was ~150 us, which is about
    // what dispatching 124 tasks costs on a large pool -- so it measured push throughput, not
    // speedup. Four times the work moves the ratio back to where the cores dominate.
    {
        constexpr int kN = 1 << 18;          // 256K floats = 1 MB, cache-resident
        const int chunk = std::max(1, kN / (workers * 4));
        std::vector<float> data(kN, 1.0f);
        auto body = [&](int s, int e) {
            for (int i = s; i < e; ++i) {
                float v = data[i];
                for (int k = 0; k < 10; ++k) v = v * 1.000001f + std::sqrt(v + 1.0f);
                data[i] = v;
            }
        };
        auto [ser, par] = measure(kN, chunk, body);
        printf("ParallelFor  : compute-bound 256K floats, ~200 cyc/elem serial %6.2f ms | parallel %6.2f ms  ->  %5.2fx\n",
            ser, par, ser / par);
    }
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

// How far above 1.00x a cell has to be before it counts as a win.
//
// Not 1.00x. Run-to-run noise on a quiet machine reaches ~1.09x even for a body doing a microsecond
// of total work, and the old rule -- first cell above 1.00x -- happily reported that as the
// crossover. An M1 Air run produced "trivial: wins from N=1000 (serial work there: 1.0 us)", which
// is nonsense: parallel does not beat serial on one microsecond of work, the row was just
// 1.00/1.00/1.09/1.07/1.06 and the first sample over the line got picked.
//
// A real crossover also STAYS won as N grows, so a lone spike is required to be confirmed by the
// next size up. Between the margin and the persistence check, the reported number means something.
static constexpr double kWinMargin = 1.15;

template <int kFlops>
static void SweepOne(JLib::TaskScheduler& sched, const char* label, const std::vector<int>& sizes) {
    printf("  %-10s |", label);
    std::vector<double> speedups, serialUs;
    speedups.reserve(sizes.size());
    serialUs.reserve(sizes.size());

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
        speedups.push_back(speedup);
        serialUs.push_back(bestSerial * 1000.0);
        printf(" %5.2fx", speedup);
    }

    // First size that clears the margin AND is confirmed by the next size up (the last column has
    // nothing after it, so it stands alone).
    int    crossover = -1;
    double crossoverSerialUs = 0.0;
    for (size_t i = 0; i < speedups.size(); ++i) {
        if (speedups[i] < kWinMargin) continue;
        if (i + 1 < speedups.size() && speedups[i + 1] < kWinMargin) continue;   // lone spike, not a crossover
        crossover = sizes[i];
        crossoverSerialUs = serialUs[i];
        break;
    }

    // The number that actually generalizes: how much SERIAL WORK (in microseconds) the loop has to
    // represent before parallelising it pays. If that figure is roughly constant across body costs,
    // it is the threshold the scheduler should be using instead of an element count.
    if (crossover > 0) printf("   | wins from N=%-7d (serial work there: %7.1f us)\n",
                              crossover, crossoverSerialUs);
    else               printf("   | never clears %.2fx\n", kWinMargin);
}

static void BenchParallelForCrossover(JLib::TaskScheduler& sched) {
    // Finer low end than the first pass: the interesting crossovers for expensive bodies sit between
    // 500 and 4000, and the original grid stepped straight over them.
    const std::vector<int> sizes = { 256, 512, 1000, 2000, 4000, 10000, 40000, 200000 };

    printf("\nParallelFor crossover sweep (speedup = serial/parallel; >1.00 means parallel wins)\n");
    printf("  a crossover is only reported at >=%.2fx confirmed by the next size -- below that is noise\n",
           kWinMargin);
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
    // Worker binding policy, chosen on the command line. It must be set BEFORE Init() (binding
    // happens at thread creation), and the scheduler is a process-wide singleton -- so one process
    // measures ONE policy. Compare by running the exe once per policy.
    //
    // DEFAULTS TO 'ideal' BECAUSE THE LIBRARY DOES. A benchmark whose default differs from the
    // library's default reports numbers nobody actually gets, and this one used to: it defaulted to
    // 'hard' on the old assumption that pinning wins on a quiet machine. Measured 2026-08-05, that
    // was wrong -- hard cost ~45% on wake latency and ~2x on the frame DAG, because a wake has to
    // wait for one specific, possibly parked core instead of landing on any awake core in the
    // domain. The library's default changed to Ideal then; this had not caught up.
    //
    // So do NOT expect 'hard' to win. It is kept because it is the only mode where you DECIDE where
    // oversubscription lands rather than discovering it, which is worth measuring on a machine that
    // genuinely owns itself -- and because a policy nobody can reproduce is a claim, not a result.
    const char* policyName = "ideal";
    auto policy = JLib::TaskScheduler::AffinityPolicy::Ideal;
    if (argc > 1) {
        if (JLIB_STRICMP(argv[1], "ideal") == 0) { /* the default, but accept it explicitly */ }
        else if (JLIB_STRICMP(argv[1], "hard") == 0) { policy = JLib::TaskScheduler::AffinityPolicy::Hard; policyName = "hard"; }
        else if (JLIB_STRICMP(argv[1], "none") == 0) { policy = JLib::TaskScheduler::AffinityPolicy::None; policyName = "none"; }
        else if (JLIB_STRICMP(argv[1], "physical") == 0) { policy = JLib::TaskScheduler::AffinityPolicy::PhysicalOnly; policyName = "physical"; }
        else {
            // Anything else, --help included, prints usage and exits. It used to fall through to
            // the default and silently run the whole suite, so asking for help started a multi-
            // minute benchmark under a policy you did not choose.
            printf("usage: SchedulerBench [ideal|hard|none|physical] [poolSize] [nosweep]\n"
                   "  ideal     (default, and the library's default) Windows: SetThreadIdealProcessor.\n"
                   "            Linux: bind to the whole LLC domain\n"
                   "  hard      bind each worker to one logical CPU. Measured ~45%% worse on wake\n"
                   "            latency and ~2x worse on the frame DAG -- do not assume it wins\n"
                   "  none      leave placement to the OS\n"
                   "  physical  one worker per physical core, SMT siblings left empty\n"
                   "\n"
                   "  poolSize  worker count passed to Init(). 0 or omitted = auto (hw-1).\n"
                   "            For sweeping pool size against latency and the frame DAG, which is\n"
                   "            a DIAGNOSTIC -- do not ship a small pool, it starves everything that\n"
                   "            is not a tiny graph.\n"
                   "  nosweep   skip the ParallelFor crossover sweep (much the slowest section),\n"
                   "            so a pool-size sweep is a few seconds per point instead of minutes.\n"
                   "\n"
                   "The scheduler is a process-wide singleton and both policy and pool size are\n"
                   "fixed at Init(), so one run measures ONE configuration -- run the exe once per\n"
                   "point to compare. On macOS and Android every policy is a no-op (no usable\n"
                   "affinity API there); prefer 'none' on those so the label matches reality.\n");
            return (JLIB_STRICMP(argv[1], "--help") == 0 || JLIB_STRICMP(argv[1], "-h") == 0) ? 0 : 2;
        }
    }

    size_t poolSize = 0;                 // 0 = auto (hw-1)
    bool   runSweep = true;
    for (int a = 2; a < argc; ++a) {
        if (JLIB_STRICMP(argv[a], "nosweep") == 0) { runSweep = false; continue; }
        poolSize = (size_t)strtoul(argv[a], nullptr, 10);
    }

    JLib::TaskScheduler::SetAffinityPolicy(policy);

    // The version is stamped here on purpose. Results get pasted into issues and threads, and the
    // suite changes: the ParallelFor case was split in two, the default affinity policy moved from
    // hard to ideal, and the crossover sweep stopped reporting noise as a win. Third-party numbers
    // from before those changes had to be thrown away because nothing in the output identified the
    // build. Anything pasted from here says which scheduler produced it.
#ifndef JLIBSCHED_VERSION
#define JLIBSCHED_VERSION "unknown"   // hand-built outside CMake
#endif
    printf("JLib::Scheduler %s bench  (sizeof(Task)=%zu, hw threads=%u, affinity=%s, pool=%s)\n",
        JLIBSCHED_VERSION,
        sizeof(JLib::Task), std::thread::hardware_concurrency(), policyName,
        poolSize ? std::to_string(poolSize).c_str() : "auto");
    printf("----------------------------------------------------------------\n");

    // 180s per section. Generous on purpose: the crossover sweep is legitimately the slowest part
    // and CI runners are shared VMs, so a false positive would be worse than what it catches. Even
    // so it reports in three minutes instead of the thirty a job timeout takes.
    StartSectionWatchdog(180);

    JLib::TaskScheduler::Init(poolSize);
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

    // Every section announces itself so the watchdog can name the one that hangs. Twice the macOS
    // job has been killed by the 30-minute job timeout knowing only that "the benchmark step" hung.
    Section("throughput/1p");  BenchThroughputSingleProducer(sched);
    Section("throughput/bt");  BenchThroughputBatched(sched);
    Section("throughput/mp");  BenchThroughputMultiProducer(sched);
    Section("latency");        BenchLatency(sched);
    Section("ParallelFor");    BenchParallelFor(sched);
    Section("fork-join");      BenchRecursiveForkJoin(sched);
    Section("frame DAG");      BenchFrameDag(sched);
    // LAST, and deliberately. This one sleeps 50 ms before each of its five runs so the pool is
    // genuinely parked, which is the whole point of it. Run earlier, it leaves every worker cold and
    // the next section pays the wake-up: with it sitting before the frame DAG, that section read
    // 30.88 us/graph against 22.7 to 24.7 measured on its own. Anything that deliberately idles the
    // pool has to go after everything it would otherwise contaminate.
    Section("burst");          BenchIdleBurst(sched);
    if (runSweep) { Section("ParallelFor crossover sweep"); BenchParallelForCrossover(sched); }

    g_benchDone.store(true, std::memory_order_release);
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
    // Was 1, which made this the only unreportable number in the suite: measured across five runs
    // it swung 0.34-0.49 ms (37%) while latency held to 3% and the frame DAG to 7%. A single run of
    // a sub-millisecond section is mostly scheduler warm-up and whatever else the OS was doing.
    // Matches the other sections at 3 now.
    constexpr int kRuns = 3;
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
