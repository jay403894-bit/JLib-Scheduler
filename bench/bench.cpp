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
// workers to 0.8 M/s at 14 and staying there, which reads as the scheduler failing to scale.
//
// What 1a actually measures past that point is a single producer being outrun. One thread creates
// and pushes roughly 3.4 M tasks/sec; once the pool can drain faster than that, the surplus workers
// find empty deques and spend their time steal-probing, and that traffic slows the producer down.
// Hence a CLIFF at the crossover rather than a gradual slope. That is a real and useful number --
// a main thread submitting a frame's work is a genuine pattern -- but it is a submission-rate
// measurement, not a capacity one, and the label says so.
//
// 1b HAS THE SAME CLIFF. This comment used to say "1b, fork-join and the frame DAG are all flat
// across the same sweep", and for 1b that is FALSE -- corrected 2026-08-17 after the row was
// mistaken for a regression and bisected. Measured, 31-worker machine, default Sleep:
//
//     pool     4      8     16     18     20  |    22     24     31
//     mp   11.08  11.39  10.13  10.93  10.50  |  2.37   5.36   3.60
//
// Same shape as 1a, just further right: four producers can feed more consumers than one, so the
// crossover lands at ~21 workers instead of ~14. Past it the consumers drain faster than the
// producers submit, workers run dry and PARK, and every subsequent Push buys a kernel wake instead
// of the ~1 ns an awake worker costs -- so the metric is bistable, ~10-11 M/s while the pool stays
// saturated and ~3 M/s once it starts parking.
//
// TWO CONSEQUENCES, both bit us. First, `best-of-N` on a bistable metric publishes whichever regime
// got lucky: this row was recorded as 8.4 M/s and then 9.8 M/s sixteen minutes later on identical
// code, and reads ~3 M/s today, which was mistaken for a regression until v1.3.0 was rebuilt and
// measured the same as HEAD. Second, the cliff means 1b is NOT the capacity control 1a needed --
// it is a second submission-rate measurement with a higher crossover. fork-join and the frame DAG
// are still the flat ones; lean on those.
#define NOMINMAX
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <Event.h>        // event/resume section
#include <DirectEvent.h>  // event/resume section
#include <Thread.h>    // StealStatsRead/Reset -- no-ops unless built with JLIBSCHED_STEAL_STATS
#include <platform.h>  // SpinHintName -- stamped into the banner, see CpuRelax
#include <chrono>
#include <functional>  // RunCursorRange takes std::function<void(int,int)>&
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

// A best-of-N result that also remembers what the other N-1 runs said.
//
// WHY: best-of-N collapses a distribution to one number, and on a BIMODAL row that number is
// whichever regime got lucky -- which reads as a confident measurement and is not one. The
// multi-producer row is exactly that (see the header): it has a pool-size cliff at ~21 workers, and
// a machine sitting near the edge lands in either the ~11 M/s saturated regime or the ~3 M/s parked
// one. It was published as 8.4 M/s, then 9.8 M/s sixteen minutes later on identical code, then read
// ~3 M/s months later -- and that gap was mistaken for a regression and chased through a bisect
// before anyone noticed the row simply has two answers.
//
// Best-of-N is still the headline: it is the right summary for "how fast can this go", and changing
// it would break comparison with every number already published. The spread is reported ALONGSIDE
// it so a bimodal row announces itself instead of hiding.
struct Spread {
    double best  = 1e300;
    double worst = 0.0;
    int    n     = 0;
    void add(double ms) {
        if (ms < best)  best  = ms;
        if (ms > worst) worst = ms;
        ++n;
    }
    // Slowest / fastest. 1.00 is perfectly repeatable.
    double ratio() const { return (best > 0.0 && best < 1e299) ? worst / best : 1.0; }
};

// Past this, the run-to-run gap is too wide to be ordinary noise on these benches (which sit around
// 1.2x) and the reader is told not to trust the headline alone. Deliberately well clear of noise
// AND well under the ~3x the multi-producer cliff produces, so it catches the real case without
// crying wolf on a normal one.
static constexpr double kSuspectSpreadRatio = 1.50;

// `tasks / ms / 1000` is M tasks/sec, so the FASTEST time is the highest rate.
static void PrintSpread(const char* label, const Spread& s, double tasks) {
    if (s.n < 2) return;
    const double fastRate = tasks / s.best  / 1000.0;
    const double slowRate = tasks / s.worst / 1000.0;
    printf("               %d runs spanned %.2f .. %.2f M/s (%.2fx)%s\n",
        s.n, fastRate, slowRate, s.ratio(),
        s.ratio() >= kSuspectSpreadRatio
            ? "   <-- BIMODAL: read the range, not the headline"
            : "");
    (void)label;
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
// ---- THE BANNER HAS TO BE A CHECK, NOT AN ECHO ------------------------------------------------
//
// Printing K and F straight from the atomics the scheduler steers by proves nothing: it agrees with
// itself no matter how the wiring is broken. The bands are only REAL if the workers that actually
// park are exactly the ones the layout says may park.
//
//   [0, K)      reserved   -- parks only when ReservedNeverParks() is off
//   [K, K+Fbase) floor     -- MUST NEVER PARK. This is the definition, not a tendency.
//   [K+Fbase, N) parkable  -- should be where every park came from
//
// BASE, NOT LIVE F, for the must-never-park band: growth is transient, so a worker promoted during a
// burst and shed afterwards is legitimately parkable by the time this runs. Only the configured base
// floor is never-park for the whole run, and asserting against the live value would flag the
// controller doing its job.
//
// A violation makes the WHOLE RUN JUNK rather than merely odd -- every row above was then measured
// on a pool with a different shape than the one named at the top.
static void PrintBandVerdict() {
    // ---- SETTLE BEFORE READING THE BANDS, OR THE SHED TEST IS A FALSE ALARM ------------------
    //
    // This read F the instant the last section returned, and the last section is a SWEEP that ends
    // mid-wave: it grows the floor and stops. The collapse cannot have fired yet -- it refuses
    // while the 6 ms grow-hold from the final completion is still live -- so the banner printed
    // `FLOOR DID NOT SHED: F=28` on a floor that was working exactly as designed.
    //
    // That is why the failure only appeared on FULL runs and never with `nosweep`: without the
    // sweeps the last section is the burst, which already settles 25 ms internally and hands the
    // banner a shed floor. The scheduler behaved identically in both; only the observation moved.
    //
    // 25 ms, matching the burst row's settle and comfortably past the 6 ms hold. After it, F above
    // base IS the real thing the message describes -- growth fired and the collapse did not -- and
    // the advice it gives about the collapse gate is worth following.
    std::this_thread::sleep_for(std::chrono::milliseconds(25));

    const size_t n = JLib::TaskScheduler::Instance().GetWorkerCount();
    const size_t K = JLib::TaskScheduler::GetHotWorkers();
    const size_t F = JLib::TaskScheduler::GetAwakeFloor();
    const size_t Fb = JLib::TaskScheduler::GetAwakeFloorBase();
    // BANDS CANNOT MOVE: K is static, so no verdict below can be invalidated by a mid-run
    // band change. This read GetHotWorkerRange and compared kmax > kmin.
    const bool bandsMoved = false;

    const size_t bandF = Fb;

    // ---- OBSERVED K, NOT DECLARED K ---------------------------------------------------------
    //
    // A RESERVED CORE THAT PARKED IS NOT RESERVED. The band's whole promise is a thread that is
    // awake and holding no bulk work when a completion lands; a worker that slept is a parkable
    // core wearing a reserved name, and printing `reserved [0,1)` for it is how you spend a night
    // debugging the wrong bug. ReservedNeverParks() DEFAULTS FALSE, so by default that is exactly
    // what [0,K) is.
    //
    // So the banner reports what the pool DID: a reserved worker counts only if it never parked.
    // If none qualify, the reserved band prints as EMPTY and K reads 0, whatever was configured.
    //
    // This measures the never-park half only. The other half -- that [0,K) refuses loPri -- is
    // covered by SchedulerReservedBandLiveKTest, which is where an assertion belongs; a banner
    // should report, not assert.
    size_t observedK = 0;
    for (size_t q = 0; q < K; ++q)
        if (JLib::TaskScheduler::GetWorkerParkCount(q) == 0) ++observedK;

    // ONE K ACROSS THE WHOLE LINE. This printed the reserved band and the K= field from OBSERVED K
    // while the floor band came from LIVE K, so a run where K moved showed `reserved [0,1)` and
    // `K=1` next to `floor [7,9)` -- three numbers on one line that cannot all be true. Every band
    // here is now spelled from the same K, and the observed count is reported as its own fact below.
    printf("bands (OBSERVED): reserved [0,%zu)  floor [%zu,%zu)  parkable [%zu,%zu)   K=%zu %s  F=%zu (base %zu)\n",
           K, K, K + bandF, K + bandF, n, K,
           "pinned", F, Fb);   // K is always pinned now -- the controller is gone
    if (K && observedK != K)
        printf("        of the %zu workers in [0,%zu), %zu never parked\n", K, K, observedK);
    if (K + F > n)
        printf("        *** INVALID LAYOUT: K+F=%zu > N=%zu, so the parkable band is EMPTY ***\n",
               K + F, n);

    unsigned resvP = 0, floorP = 0, parkP = 0;
    size_t   floorWho = 0, parkWho = 0;
    for (size_t q = 0; q < n; ++q) {
        const unsigned p = JLib::TaskScheduler::GetWorkerParkCount(q);
        if (q < K)            resvP  += p;
        else if (q < K + Fb) { floorP += p; if (p) ++floorWho; }
        else                 { parkP  += p; if (p) ++parkWho;  }
    }
    printf("        observed parks: reserved %u | floor %u | parkable %u (on %zu of %zu parkable workers)\n",
           resvP, floorP, parkP, parkWho, (n > K + Fb) ? n - (K + Fb) : 0);
    // ATTRIBUTION BY BAND IS ONLY MEANINGFUL IF THE BANDS HELD STILL. Parks are counted per WORKER
    // and binned here by the FINAL layout, so when K moved during the run a worker that was reserved
    // at the moment it parked gets filed under whatever band its index sits in now. A run with
    // K 1 -> 10 -> 8 reported `reserved 211 | floor 144` that way and NONE of them were violations.
    // Say so, rather than printing numbers that read as findings.
    if (bandsMoved)
        printf("        ^ ATTRIBUTED BY THE FINAL LAYOUT, and K MOVED this run -- a worker is filed\n"
               "          under the band its index sits in NOW, not the one it was in when it parked.\n"
               "          These three numbers are NOT violations. Use the live-floor count below,\n"
               "          which is evaluated AT PARK TIME.\n");

    if (K && observedK != K && !bandsMoved)
        printf("        *** K IS NOMINAL: %zu of %zu workers in [0,%zu) PARKED, so they are parkable\n"
               "            cores with a reserved name -- read K as %zu. Either call\n"
               "            SetReservedNeverParks(true) so the band is real, or stop calling it\n"
               "            reserved. hiPri still routes and runs; it just is not RESERVED. ***\n",
               K - observedK, K, K, observedK);
    if (floorP && !bandsMoved)
        printf("        *** JUNK RUN: %u parks on %zu FLOOR worker(s) -- the floor is defined as\n"
               "            never-parking, so the rows above were measured on a different pool than\n"
               "            the banner claims ***\n", floorP, floorWho);
    // MEANING CHANGED WITH THE LAST-GATE GUARD in the worker's park path. This used to count
    // COMPLETED parks by live-floor members -- a genuine violation, and the precursor to a stuck F.
    // The worker now re-reads the band word AFTER the sleep commit and turns around if it has
    // become floor, so a floor member can no longer complete a park at all; what this counts is
    // how often that last gate CAUGHT one mid-commit.
    //
    // THE RACE IS NOT ADAPTIVE K'S -- that is gone, and K is static. What still moves the band
    // under a worker committing to sleep is FLOOR GROWTH AND COLLAPSE: [K, K+F) widens on a burst
    // and returns to base when the wave drains, so a parkable worker can become a floor member
    // between the predicate and the sleep. Small nonzero numbers are that window being absorbed,
    // not a defect -- the defect signal is the SHED test on the burst row, and the FLOOR DID NOT
    // SHED banner below. `nogrow` pins F, so this should read 0 there; a nonzero count under
    // `nogrow` means something OTHER than growth is moving the band, and that is worth chasing.
    printf("        floor-member parks CAUGHT at the last gate: %llu  <- the worker re-checks the\n"
           "        band word after committing to sleep and recovers if the floor moved over it;\n"
           "        small counts are the floor GROWING over a worker mid-commit being absorbed\n"
           "        (expect 0 under `nogrow`). A floor member COMPLETING a park is structurally\n"
           "        impossible now -- if the SHED test fails anyway, suspect the collapse gate,\n"
           "        not this\n",
           JLib::TaskScheduler::GetFloorParkCount());
    if (F > Fb)
        printf("        *** FLOOR DID NOT SHED: F=%zu still above base %zu at the end of the run.\n"
               "            Growth fired and the collapse did not, so every row after the one that\n"
               "            grew it was measured on a wider pool than it asked for. ***\n", F, Fb);
    if (!parkP && n > K + Fb)
        printf("        NOTE: nothing parked at all. Either the pool never went idle, or the\n"
               "        parkable band is not reachable -- check before trusting any idle-path row.\n");
}

static std::atomic<const char*> g_section{ "startup" };
static std::atomic<bool> g_benchDone{ false };

static void Section(const char* name) {
    g_section.store(name, std::memory_order_release);
    // EVERY ROW STARTS AT THE CONFIGURED FLOOR. Push-side growth is transient by design, but the
    // shed is gated on a hold, and the gap between two bench rows is shorter than that hold -- so a
    // flood row handed its grown floor to the next row and every number there measured the flood's
    // pool instead of the configured one. It showed up as a latency row opening at 16 with p99 1.70
    // us against 0.60: not a latency regression, leftover spinners.
    //
    // Harness hygiene, not a scheduler behaviour: rows are supposed to be independent measurements
    // and nothing outside a benchmark should ever call this.
    // And the peak with it, so each row.s peak describes THAT row. Without this the DAG row would
    // inherit a 16 from the no-op flood above it and a genuine streak leak into DAG would be
    // invisible -- the number would already read 16 for an unrelated reason.
    JLib::TaskScheduler::ResetAwakeFloorPeak();
}

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
                if (JLib::TaskScheduler::IsInitialized())
                    JLib::TaskScheduler::Instance().DumpPoolState(last);
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
    Spread spread;
    // Reset HERE, not at construction: workers steal-probe continuously while idle, so anything
    // accumulated before the measured region is dead time rather than work being done.
    JLib::StealStatsReset();
    // IS THE PARK EVEN ON THIS ROW? Without this number an A/B between park primitives on this row
    // cannot be read: "no difference" and "the primitive was never called" print identically.
    JLib::TaskScheduler::ResetWakeCount();
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
        spread.add(total);
        if (total < best) { best = total; bestPush = MsBetween(t0, t1); bestDrain = MsBetween(t1, t2); }
    }
    printf("throughput/1p: %d no-op tasks in %.2f ms best-of-%d  ->  %.2f M tasks/sec  (1 producer)\n",
        kThroughputTasks, best, kThroughputRuns, kThroughputTasks / best / 1000.0);
    printf("               submit %.2f ms (%.2f M/s), drain-after-submit %.2f ms\n",
        bestPush, kThroughputTasks / bestPush / 1000.0, bestDrain);
    printf("               kernel wakes: %llu over %d runs (%.2f per 1k tasks)  <- how much of this\n"
           "               row is the park primitive at all\n",
           (unsigned long long)JLib::TaskScheduler::GetWakeCount(), kThroughputRuns,
           JLib::TaskScheduler::GetWakeCount() * 1000.0 / (double)kThroughputTasks / kThroughputRuns);
    PrintSpread(nullptr, spread, kThroughputTasks);
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
    Spread spread;
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
        spread.add(total);
        if (total < best) { best = total; bestPush = MsBetween(t0, t1); bestDrain = MsBetween(t1, t2); }
    }
    printf("throughput/bt: %d no-op tasks in %.2f ms best-of-%d  ->  %.2f M tasks/sec  (1 producer, PushBatch x%d)\n",
        kThroughputTasks, best, kThroughputRuns, kThroughputTasks / best / 1000.0, kChunk);
    printf("               submit %.2f ms (%.2f M/s), drain-after-submit %.2f ms\n",
        bestPush, kThroughputTasks / bestPush / 1000.0, bestDrain);
    PrintSpread(nullptr, spread, kThroughputTasks);
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
// Workers that ran a leaf, per sweep cell. Answers the only question left when a wake fix does not
// move a row: did the thieves arrive at all?
static std::atomic<unsigned char> g_sweepSeen[64];
static size_t SweepParticipants() {
    size_t n = 0;
    for (auto& c : g_sweepSeen) if (c.load(std::memory_order_relaxed)) ++n;
    return n;
}

static std::atomic<unsigned long long> g_burstSink{ 0 };

// HOW MANY WORKERS ACTUALLY RAN A BURST TASK. The peak floor says how many were WOKEN; this says how
// many were USED, and the two came apart badly: a wave that promoted to 16 still finished in six
// waves, which is three workers' worth. Waking a worker, placing work where it can reach it, and it
// actually running that work are three separate things, and only the last one shows up in the wall
// time. Without this the next step is guesswork about which of the three failed.
static std::atomic<unsigned> g_burstLandedOn[64];
static void BurstBody(void*) {
    // An xorshift CHAIN, not a summation. The obvious `acc += i * k` loop has a closed form and GCC
    // finds it: the same body measured 0.66 ms under MSVC and 0.02 ms under GCC, which silently
    // turned this into a task-overhead benchmark on one platform and a parallelism benchmark on the
    // other. Every iteration here depends on the previous one, so there is nothing to fold.
    if (JLib::Thread::Current()) {
        const int q = JLib::Thread::Current()->qIndex;
        if (q >= 0 && q < 64) g_burstLandedOn[q].fetch_add(1, std::memory_order_relaxed);
    }
    unsigned long long x = 88172645463325252ull;
    for (int i = 0; i < 3'000'000; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
    g_burstSink.fetch_add(x, std::memory_order_relaxed);
}

// ------------------------------------------------- lane pressure (ROW REMOVED WITH ADAPTIVE K)
//
// THE ROW THIS BODY FED IS GONE, and so is the question it asked. It existed to answer "does
// adaptive K ever actually move", by holding the lane NON-EMPTY -- small bodies, depth more than K
// -- rather than by burying the pool, which would have promoted for the wrong reason.
//
// The answer turned out to be no, and not for want of pressure: the controller promoted on
// `missed || adv == mask`, and BOTH terms were unreachable under load -- NoteLaneMiss had no
// callers at all, and the other term is edge-triggered, so a busier lane fired it LESS. 2.5M hiPri
// tasks moved K by zero. The controller was removed rather than fixed; K is static now.
//
// The body below is left in place, unreferenced, because it is the only calibrated
// keep-the-lane-warm workload in the file and the next lane experiment will want it. It is not
// dead code that nobody noticed -- it is a fixture waiting for its row.
static std::atomic<unsigned long long> g_laneSink{ 0 };
static void LaneBody(void*) {
    unsigned long long x = 88172645463325252ull;
    for (int i = 0; i < 2000; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
    g_laneSink.fetch_add(x, std::memory_order_relaxed);
}


static void BenchIdleBurst(JLib::TaskScheduler& sched, JLib::CorePref pref) {
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

    // HOW WIDE DID THE POOL ACTUALLY GET? The speedup alone cannot separate "growth never fired"
    // from "growth fired and the wave still cost this much", and those two want opposite fixes. The
    // awake floor either side of the burst answers it structurally, immune to the timing noise the
    // millisecond figure carries.
    const size_t floorBeforeBurst = JLib::TaskScheduler::GetAwakeFloor();
    JLib::TaskScheduler::ResetAwakeFloorPeak();
    for (auto& c : g_burstLandedOn) c.store(0, std::memory_order_relaxed);

    double burst = 1e300;
    for (int r = 0; r < kRuns; ++r) {
        // Let the pool actually PARK before the burst; measuring a pool that is still spinning would
        // quietly answer a different question.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        JLib::WaitGroup wg; wg.n.store(kBurst, std::memory_order_relaxed);
        // AFTER the sleep, so the parks the pool did on its way down are not counted as burst wakes.
        // This row is 16 tasks of ~3.3 ms; a kernel wake is ~3 us. If the count here is small, the
        // park primitive is a rounding error on this row and the row CANNOT decide between two of
        // them -- which is a statement about the instrument, not about the primitives.
        JLib::TaskScheduler::ResetWakeCount();
        auto t0 = Clock::now();
        for (int i = 0; i < kBurst; ++i) {
            JLib::Task* t = sched.CreateTask(BurstBody, nullptr, 0, JLib::TaskType::Native, pref);
            if (!t) { printf("burst        : ERROR -- CreateTask returned null\n"); return; }
            t->waitGroup = &wg; sched.Push(t);
        }
        sched.WaitFor(wg);
        burst = std::min(burst, MsBetween(t0, Clock::now()));
    }

    // PEAK, NOT THE VALUE AT THIS LINE. The floor collapses to base the instant the wave drains and
    // an overflow worker notices, which happens long before this printf -- so reading the live floor
    // here measures the SHED and reports it as the growth. It printed "1 -> 1" next to a 7.02 ms
    // wall time, and those cannot both be true: 16 tasks of 3.28 ms on one worker is ~50 ms, and
    // 7.02 ms is two waves. The controller was working; the instrument was pointed at the wrong
    // instant.
    // ---- DID IT SHED, OR HAD IT SIMPLY NOT SHED YET? -----------------------------------------
    //
    // The floor reading taken here is worthless on its own: the last heavy body to finish calls
    // GrowFloorIfLongBody on its way out, which refreshes the grow timestamp, and the collapse
    // refuses for kFloorHoldNs (6 ms) after any growth. So a floor read microseconds after the
    // burst ALWAYS looks grown, whether or not it is stuck. Reading it once and calling it a
    // ratchet is the same mistake as reading it once and calling it a shed.
    //
    // A settle longer than the hold separates them: still grown after this, and the collapse is
    // genuinely not firing.
    const size_t floorImmediatelyAfter = JLib::TaskScheduler::GetAwakeFloor();
    // TEMP DIAG -- THE DELTA ACROSS THE SETTLE IS THE WHOLE QUESTION. Nothing is submitted during
    // this sleep, so the 6 ms hold must expire and heldByHold must stop growing. If it keeps
    // climbing through 25 ms of idle, the grow timestamp is being refreshed with no tasks in
    // flight -- something in the IDLE path is stamping it -- and no amount of waiting will ever
    // let the collapse through. Totals cannot show this; only the delta can.
    unsigned long long c0=0,ng0=0,hd0=0,cs0=0,dn0=0;
    JLib::TaskScheduler::GetFloorCollapseStats(c0,ng0,hd0,cs0,dn0);
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    const size_t floorAfterSettle = JLib::TaskScheduler::GetAwakeFloor();
    {
        unsigned long long c=0,ng=0,hd=0,cs=0,dn=0;
        JLib::TaskScheduler::GetFloorCollapseStats(c,ng,hd,cs,dn);
        printf("               collapse TOTAL:  calls=%llu notGrown=%llu heldByHold=%llu casLost=%llu SHED=%llu\n",
               c,ng,hd,cs,dn);
        printf("               collapse DURING THE 25ms IDLE SETTLE: calls=%llu notGrown=%llu heldByHold=%llu casLost=%llu SHED=%llu\n",
               c-c0, ng-ng0, hd-hd0, cs-cs0, dn-dn0);
        printf("               ^ heldByHold decays over the FIRST ~6 ms of the settle and must be\n"
               "                 ~0 after: the last completion legitimately refreshes the grow-hold\n"
               "                 on its way out (see the burst note above), so calls made before it\n"
               "                 expires are correctly refused. Tens of thousands here is that 6 ms\n"
               "                 tail at spin rate. The failure signature is heldByHold ~= calls --\n"
               "                 the stamp being refreshed from the IDLE path for the whole settle,\n"
               "                 with no tasks in flight -- and SHED=0 next to it.\n");

        // TEMP DIAG -- A STALE STEAL HINT BLOCKS BOTH GATES AT ONCE.
        //
        // The collapse CALL SITE is `qIndex >= K+Fbase && advertisedCount == 0`, and the PARK gate
        // is `advertisedCount == 0 && !onAwakeFloor`. advertisedCount is the popcount of the global
        // steal hints, so ONE bit left set with an empty queue behind it stops the shed AND stops
        // parking, pool-wide, forever. `calls=0` through a 25 ms idle can only mean that gate was
        // false -- nothing else stands between a spinning floor worker and the call.
        //
        // So print the bits and check each one against the queue it claims work for. A set bit with
        // an empty deque is a hint nobody will ever clear, because hints are publisher-set and
        // owner-cleared and the owner has nothing to drain.
        {
            // TEMP DIAG -- DUMP THE POOL WHEN THE SHED FAILED. `calls=0` through 25 ms of idle with
            // no stale hint means the eligible workers are not LOOPING, and the only way that is
            // true is that they are asleep. The floor claims [K, K+F); if those indices show
            // SLEEPING here, they were promoted into the floor while parked and never woken, which
            // is a growth that raised a number without waking anybody -- and then nobody is left to
            // call the collapse. This prints the answer instead of another hypothesis.
            if (floorAfterSettle > JLib::TaskScheduler::GetAwakeFloorBase())
                JLib::TaskScheduler::Instance().DumpPoolState(
                    "bench: FLOOR STUCK after a 25 ms idle settle");
            unsigned advertised = 0, stale = 0;
            JLib::TaskScheduler::GetStaleHintReport(advertised, stale);
            printf("               steal hints after the settle: advertised=%u  of which STALE=%u\n",
                   advertised, stale);
            if (stale)
                printf("               *** %u STALE HINT(S) over EMPTY queues: advertisedCount can never\n"
                       "                   reach 0, so the collapse call site and the park gate are both\n"
                       "                   dead pool-wide. ***\n", stale);
        }
    }
    const unsigned long long burstWakes = (unsigned long long)JLib::TaskScheduler::GetWakeCount();
    const size_t floorPeakBurst  = JLib::TaskScheduler::GetAwakeFloorPeak();
    const size_t floorAfterBurst = JLib::TaskScheduler::GetAwakeFloor();
    // TWO ARMS, dflt and wide, and the pair is the point. `dflt` is steered at the awake floor --
    // the cheap push, no kernel wake -- and this row is where that is the wrong trade: 16 bodies of
    // ~3.3 ms each, against a wake of ~3 us. `wide` skips the narrowing and takes the full-pool
    // rotation, paying a wake per push to have every worker running immediately instead of waiting
    // for owners to drain their inboxes into something stealable.
    //
    // READ THE PARTICIPANT LINE, NOT THE MILLISECONDS. The speedup is capped by ceil(16/participants)
    // waves, so "workers that actually RAN a burst task" is the number that says whether placement
    // reached the pool at all.
    printf("burst/%-5s: %d heavy tasks from an idle pool -> %.2f ms (1 task = %.2f ms, speedup %.1fx of %d)\n",
        (pref == JLib::CorePref::Wide) ? "wide" : "dflt",
        kBurst, burst, solo, (solo * kBurst) / burst, kBurst);
    printf("               floor after the burst: %zu immediately, %zu after a 25 ms settle (base %zu)\n"
           "               ^ the first is expected to be grown -- the last completion refreshes the\n"
           "                 6 ms grow-hold on its way out. THE SECOND IS THE SHED TEST.\n",
           floorImmediatelyAfter, floorAfterSettle, JLib::TaskScheduler::GetAwakeFloorBase());
    printf("               kernel wakes in the LAST burst: %llu  <- a wake is ~3 us and one task here\n"
           "               is ~3.3 ms, so unless this is in the thousands the park primitive is\n"
           "               noise on this row and cannot be A/B'd with it\n", burstWakes);
    printf("               awake floor: %zu before, PEAK %zu during, %zu after (base %zu).\n"
           "               The peak is the number that matters -- the floor sheds as soon as the\n"
           "               wave drains, so 'after' is the collapse, not the growth. %d tasks over\n"
           "               PEAK workers is ceil(%d/peak) waves, and that caps this speedup.\n"
           "               peak == base means push-side growth never fired.\n",
           floorBeforeBurst, floorPeakBurst, floorAfterBurst,
           JLib::TaskScheduler::GetAwakeFloorBase(), kBurst, kBurst);
    {
        // WOKEN is not USED. Peak says how many workers were promoted; this says how many ran a
        // task. A wave that promotes to 16 and still finishes in six waves used about three of
        // them, and no wall-clock number can tell you which of wake / place / run failed.
        unsigned participants = 0, total = 0;
        for (size_t q = 0; q < 64; ++q)
            if (unsigned c = g_burstLandedOn[q].load(std::memory_order_relaxed)) {
                ++participants; total += c;
            }
        printf("               workers that actually RAN a burst task: %u (of %d tasks over %d runs)\n",
               participants, kBurst * kRuns, kRuns);
        printf("               spread:");
        for (size_t q = 0; q < 64; ++q)
            if (unsigned c = g_burstLandedOn[q].load(std::memory_order_relaxed))
                printf(" q%zu=%u", q, c);
        printf("\n               (participants far below the peak means the wave was not REACHABLE\n"
               "                to the workers growth woke -- a busy worker's inbox has one legal\n"
               "                consumer, so work only becomes stealable when that worker drains it)\n");
        (void)total;
    }
}

static void BenchThroughputMultiProducer(JLib::TaskScheduler& sched) {
    double best = 1e300;
    Spread spread;
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
        const double total = MsBetween(t0, Clock::now());
        spread.add(total);
        best = std::min(best, total);
        for (int p = 0; p < kProducers; ++p) totalFailed += g_producerArgs[p].failed;
    }
    if (totalFailed)
        printf("throughput/mp: ERROR -- %d task allocations failed\n", totalFailed);
    printf("throughput/mp: %d no-op tasks in %.2f ms best-of-%d  ->  %.2f M tasks/sec  (%d producers)\n",
        kThroughputTasks, best, kThroughputRuns, kThroughputTasks / best / 1000.0, kProducers);
    // This row is the reason Spread exists -- it is bimodal near the pool-size cliff. See the file
    // header, and do not quote the headline from a run whose spread flagged.
    PrintSpread("mp", spread, kThroughputTasks);
    ReportStealStats("mp");
}

// ---------------------------------------------------------------- 2. latency
// WHERE THE PUSH ACTUALLY LANDED, indexed by the qIndex of the worker that RAN the task. This
// reports what placement DID, not what the placement code is believed to do -- the distinction that
// matters, because the awake floor is only doing anything if the serial round trip lands inside
// [0, floor). A push routed to a parked worker buys a WaitOnAddress round trip through the kernel,
// which is the old 4-8 us row and occasionally the 100 us one. Read this next to the kernel-wake
// counter: landing off the floor and a nonzero wake count are one fact seen from two sides.
static std::atomic<unsigned> g_landedOn[64];

// Body-entry and body-exit stamps for the round-trip split -- see the task body below. Plain
// relaxed atomics: exactly one worker writes them per iteration and the serial waiter reads them
// only after WaitFor has returned, so there is nothing to order against.
static std::atomic<long long> g_bodyStartNs{ 0 };
static std::atomic<long long> g_bodyEndNs{ 0 };

// Round-trip microseconds above which an iteration is reported as a stall. `stall=N` overrides.
static double g_stallUs = 50.0;

// Which worker actually RAN the task, recorded by the body itself. -1 means a non-worker ran it,
// which happens when the waiting thread helps -- see `helped` in the stall report.
static std::atomic<int> g_bodyQ{ -1 };

static inline long long NowNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch()).count();
}

static void BenchLatency(JLib::TaskScheduler& sched) {
    constexpr int kIters = 20'000;

    // Only populated (and only costs anything) when built with -DJLIBSCHED_LATENCY_STATS=ON --
    // see Thread.h. Serial, one task in flight at a time, same setup as the round trip itself, so
    // a global "most recent" timestamp per transition is unambiguous: nothing else touches these
    // between one iteration's Push and its WaitFor returning.
    std::vector<double> wakeUs, loopEntryUs, stealScanUs;
    if (JLib::kLatencyStatsEnabled) {
        wakeUs.reserve(kIters); loopEntryUs.reserve(kIters); stealScanUs.reserve(kIters);
    }

    // ---- PER-ITERATION SAMPLES, NOT A MEAN ---------------------------------------------------
    //
    // This row used to report totalMs/kIters and nothing else, and that number cannot answer the
    // question the row exists for. A run whose p50 is 0.53 us and whose max is 105.7 us prints as
    // "0.53 us" -- the mean absorbs a hundred-microsecond outlier spread over 20,000 samples and
    // the pathology is invisible. p50 is what a lock-free push into an already-running worker costs
    // and it will look good either way; p99 and max are where an OS wake shows up, so THOSE are the
    // floor test. Read them, not the headline.
    std::vector<double> rtUs;
    rtUs.reserve(kIters);

    // ---- CATCH THE PATHOLOGICAL STATE IN THE ACT ---------------------------------------------
    //
    // WHY THIS EXISTS. Six runs of this binary, identical arguments, produced round trips of
    // 2.21, 2.24, 2.48, 2.69, 4.49 -- and 664.64 us. Three hundred times, and the frame DAG row in
    // that same run went 11 -> 486 us with it. Two independent rows going pathological together is
    // a pool-wide STATE, not a fluke in one measurement.
    //
    // A summary printed at the END cannot diagnose it: by then the pool has recovered and the
    // percentiles only say "slow". The state has to be photographed WHILE IT IS HAPPENING, which
    // is what this does -- the first round trip to blow the threshold dumps every worker's state
    // and all three queue classes, then the run continues so the row still reports its numbers.
    //
    // ONCE PER RUN. In the bad state every iteration is slow, so the first sample is as good as any
    // and 20,000 dumps would be unreadable.
    const double kPathologicalUs = g_stallUs;   // `stall=N` on the command line; 50 us default
    bool dumped = false;
    double worstRtUs = 0.0;   // report on each new worst -- see the report site
    // Stall tally by dominant segment. The comparable output of this row; see the classify site.
    unsigned long long stallCount = 0, stallDispatch = 0, stallExecution = 0, stallCompletion = 0;
    unsigned long long stallWithWake = 0;
    double maxDispatchUs = 0.0, maxExecutionUs = 0.0, maxCompletionUs = 0.0;
    // Healthy-iteration poll rate, the yardstick the stall report is read against. See the
    // accumulate site for why it excludes the pathological iterations.
    unsigned long long healthyPollSum = 0, healthyPollN = 0;

    // Both counters are per-row, so they describe THIS row and not the warmup that preceded it.
    JLib::TaskScheduler::ResetWakeCount();

    // THE FLOOR THIS ROW ACTUALLY RAN UNDER. The banner prints the CONFIGURED floor, which is not
    // the same number: push-side growth can raise it, and the throughput rows above this one push
    // 200,000 tasks. A previous run measured this row at floor=8 while the banner said 2 -- the
    // burst was blamed, and the burst runs AFTER this row. Every conclusion about serial latency
    // is conditional on this number, so it is printed with the row rather than inferred from the
    // banner or reconstructed from the landing spread.
    const size_t floorAtLatencyStart = JLib::TaskScheduler::GetAwakeFloor();
    for (auto& c : g_landedOn) c.store(0, std::memory_order_relaxed);

    // ---- THE STALL HAS TO BE SAMPLED WHILE IT IS HAPPENING ----------------------------------
    //
    // THIS DUMP USED TO RUN AFTER WaitFor RETURNED, and it therefore could not work. By then the
    // task has completed, the serial loop has nothing else in flight, and the pool is idle BY
    // CONSTRUCTION -- so the only state it could ever print was "two floor workers awake, everyone
    // else parked, nothing queued, nothing busy". Every pathological dump ever produced by this
    // bench showed that, and it was read as evidence rather than as an artefact of when it was
    // taken. It also carried a header saying "Pool state AT THE MOMENT it happened", which was
    // false, and four reading hints describing states that cannot exist at that point.
    //
    // A blocked thread cannot dump the pool it is blocked on, so the sample has to come from
    // somewhere else. This thread publishes the start of each round trip and a watcher reads it.
    //
    // IT SPINS, DELIBERATELY. The event is ~50 us and rare (1 in 20,000), so a sleeping poller
    // would miss it every time -- there is no sleep granularity on Windows that resolves 50 us.
    // The cost is one busy core for the ~15 ms this row takes, which is worth knowing about but is
    // not measured by anything here: the row times a serial round trip, and a spinning watcher does
    // not queue work, steal, or wake anybody.
    //
    // FIRES ONCE. After it dumps it stops looking, so a pathological run cannot turn into a
    // thousand dumps and a perturbed row.
    std::atomic<long long> rtInFlightNs{ 0 };   // 0 = between iterations
    std::atomic<bool>      watcherStop{ false };
    std::atomic<bool>      watcherDumped{ false };
    std::thread watcher([&] {
        for (;;) {
            if (watcherStop.load(std::memory_order_acquire)) return;
            const long long started = rtInFlightNs.load(std::memory_order_acquire);
            if (started != 0 && !watcherDumped.load(std::memory_order_relaxed)) {
                const long long now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          Clock::now().time_since_epoch()).count();
                if ((double)(now - started) / 1000.0 > kPathologicalUs) {
                    watcherDumped.store(true, std::memory_order_relaxed);
                    printf("\n  *** STALL IN PROGRESS: this round trip has been outstanding for\n"
                           "      %.1f us and has NOT completed. The pool state below is being read\n"
                           "      WHILE it is stuck, which is the state the old dump could never\n"
                           "      show -- it sampled after WaitFor returned, when the pool is idle\n"
                           "      by construction. Read it as:\n"
                           "        a worker AWAKE with an empty queue -> searching, finding nothing\n"
                           "        a worker with a NON-EMPTY queue    -> queued and not being run\n"
                           "        'rs' non-empty                     -> a pinned resume nobody may take\n"
                           "        every worker busy                  -> saturation, not a stall\n"
                           "        ALL asleep with nothing queued     -> the wake was LOST\n",
                           (double)(now - started) / 1000.0);
                    JLib::TaskScheduler::Instance().DumpPoolState("bench: STALL IN PROGRESS");
                    printf("  (the row below still reports its own numbers)\n\n");
                }
            }
            JLib::platform::CpuRelax();
        }
    });

    auto t0 = Clock::now();
    for (int i = 0; i < kIters; ++i) {
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        // ---- SPLIT THE ROUND TRIP AT THE TASK ITSELF -------------------------------------------
        //
        // The round trip is push -> WaitFor returns, and a single number for it cannot say WHERE a
        // 50 us outlier lives. Two stamps taken inside the task body cut it into three segments
        // that have different owners:
        //
        //     push  -> body start   DISPATCH. Placement, the notify, and the worker's wake.
        //     body start -> end     EXECUTION. Nothing here but the (empty) body; a large value
        //                           means the worker was PREEMPTED mid-task.
        //     body end -> returned  COMPLETION + OBSERVATION. The waitGroup decrement, and this
        //                           thread noticing it.
        //
        // The third segment is the one the pool dumps kept pointing at: an empty pool with a round
        // trip still outstanding means the body finished long ago and the delay is entirely after
        // it. Splitting it here rather than in the library keeps the instrument in the bench --
        // the decrement path has ten call sites and none of them should grow a clock read.
        //
        // Stamped in the body, so no library change buys this. The gap between the body returning
        // and the decrement is a handful of instructions, which is below what this can resolve
        // and is not where 50 us hides.
        g_bodyStartNs.store(0, std::memory_order_relaxed);
        g_bodyEndNs.store(0, std::memory_order_relaxed);
        JLib::Task* t = sched.CreateTask(+[](void*) {
            g_bodyStartNs.store(NowNs(), std::memory_order_relaxed);
            // Which worker picked this up? Thread::instance is the running worker's own TLS, so
            // this is ground truth for placement, and it costs one relaxed increment.
            g_bodyQ.store(JLib::Thread::Current() ? JLib::Thread::Current()->qIndex : -1,
                          std::memory_order_relaxed);
            if (JLib::Thread::Current()) {
                const int q = JLib::Thread::Current()->qIndex;
                if (q >= 0 && q < 64) g_landedOn[q].fetch_add(1, std::memory_order_relaxed);
            }
            g_bodyEndNs.store(NowNs(), std::memory_order_relaxed);
        }, nullptr);
        t->waitGroup = &wg;
        const int64_t pushNs = JLib::kLatencyStatsEnabled
            ? std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count()
            : 0;
        // ---- DID THIS PUSH BUY A KERNEL WAKE? --------------------------------------------------
        //
        // The one question the marks cannot answer on this path, and it splits the remaining
        // possibilities cleanly:
        //
        //   delta 0  no WakeByAddress was issued, so the target was AWAKE when the push chose it.
        //            Nothing kernel-side is in the number: the delay is an awake worker not getting
        //            round to draining its own inbox, which is a scheduling or a drain problem.
        //   delta 1  a wake WAS issued -- the target had advertised intent to park -- so the delay
        //            is the OS putting that thread back on a core, and no scheduler change to the
        //            dispatch path touches it.
        //
        // Global, but the row is strictly serial with one task in flight, so the delta belongs to
        // this push and nothing else.
        const unsigned long long wakesBefore = JLib::TaskScheduler::GetWakeCount();
        const auto rtStart = Clock::now();
        // PUBLISHED BEFORE THE PUSH and cleared after the wait, so the watcher above sees a
        // non-zero start exactly for the window this thread is blocked in.
        rtInFlightNs.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                               rtStart.time_since_epoch()).count(),
                           std::memory_order_release);
        sched.Push(t);
        const long long pushDoneNs = NowNs();
        sched.WaitFor(wg);
        // STAMPED HERE, NOT AT PRINT TIME, and the first version got this wrong in a way the
        // output caught immediately: the completion segment was computed inside the printf that
        // reports it, so it also contained the two printfs above it. The three segments then summed
        // to 10.2 us on a 7.4 us round trip -- an instrument reporting more time than elapsed,
        // which is the one arithmetic a split like this can be checked against for free.
        const long long returnedNs = NowNs();
        const unsigned long long wakesAfter = JLib::TaskScheduler::GetWakeCount();
        const bool wakeDelta0 = (wakesAfter != wakesBefore);   // did this push buy a kernel wake?
        // READ IMMEDIATELY, BEFORE ANYTHING ELSE ON THIS THREAD WAITS. They are thread_local and
        // reset by the next bare wait, so a single intervening WaitFor would overwrite them with
        // that wait's numbers and the report would describe the wrong iteration.
        const unsigned wPolls  = JLib::TaskScheduler::LastBareWaitPolls();
        const unsigned wYields = JLib::TaskScheduler::LastBareWaitYields();
        const unsigned wHelped = JLib::TaskScheduler::LastBareWaitHelped();
        rtInFlightNs.store(0, std::memory_order_release);
        const double rt = std::chrono::duration<double, std::micro>(Clock::now() - rtStart).count();
        rtUs.push_back(rt);

        // ---- CLASSIFY EVERY STALL, NOT JUST THE ONE THAT GETS PRINTED --------------------------
        //
        // THE MAX IS NOISE, MEASURED: five identical runs of this row produced maxima of 27.5, 88.1,
        // 49.7, 197.9 and 46.9 us -- a 7.2x spread with p50 and p99 stable to a rounding digit
        // across all five. So a single exemplar report says nothing about a CONFIGURATION, however
        // detailed it is, and two arms compared on one run each is not a comparison. That mistake
        // was made twice here before this tally existed, in both directions.
        //
        // What survives the noise is the ATTRIBUTION. Which segment holds a stall is a property of
        // the mechanism; how big the worst one happened to get is a property of the afternoon.
        // Counting stalls by their dominant segment turns a rare tail event into something with a
        // denominator, and it is the only number in this block worth comparing between arms.
        if (rt > kPathologicalUs) {
            ++stallCount;
            const long long sbs = g_bodyStartNs.load(std::memory_order_relaxed);
            const long long sbe = g_bodyEndNs.load(std::memory_order_relaxed);
            if (sbs != 0 && sbe != 0) {
                const double dUs = (double)(sbs - pushDoneNs) / 1000.0;
                const double eUs = (double)(sbe - sbs)        / 1000.0;
                const double cUs = (double)(returnedNs - sbe) / 1000.0;
                if      (dUs >= eUs && dUs >= cUs) { ++stallDispatch;   if (dUs > maxDispatchUs)   maxDispatchUs   = dUs; }
                else if (cUs >= dUs && cUs >= eUs) { ++stallCompletion; if (cUs > maxCompletionUs) maxCompletionUs = cUs; }
                else                               { ++stallExecution;  if (eUs > maxExecutionUs)  maxExecutionUs  = eUs; }
                if (wakeDelta0) ++stallWithWake;
            }
        }

        // ON EACH NEW WORST, NOT ON THE FIRST OVER THE LINE. The original fired once, on the first
        // iteration to exceed the threshold, and then stopped looking -- so the block described
        // whichever outlier happened to come first, while the row's headline reported the MAX. Those
        // are different iterations, and comparing one arm's first outlier against another arm's max
        // is not a comparison at all. It made a spinyield A/B read as a 70 vs 89 us difference that
        // the underlying samples did not support.
        //
        // Printing on every new worst means the LAST block printed is the worst one, which is the
        // one to read and the one that matches `max` in the row below. Costs a few extra blocks on a
        // bad run, which is cheaper than a wrong comparison.
        if (rt > kPathologicalUs && rt > worstRtUs) {
            worstRtUs = rt;
            dumped = true;
            printf("\n  *** PATHOLOGICAL ROUND TRIP: iteration %d took %.1f us "
                   "(healthy is ~2 us) ***\n", i, rt);

            // ---- WHERE THE OUTLIER ACTUALLY LIVES ----------------------------------------
            const long long bs = g_bodyStartNs.load(std::memory_order_relaxed);
            const long long be = g_bodyEndNs.load(std::memory_order_relaxed);
            if (bs != 0 && be != 0) {
                printf("      dispatch   (push -> body start)  %8.1f us\n",
                       (double)(bs - pushDoneNs) / 1000.0);
                printf("      execution  (body start -> end)   %8.1f us   <- large = worker PREEMPTED\n",
                       (double)(be - bs) / 1000.0);
                printf("      completion (body end -> return)  %8.1f us   <- large = the WAITER\n",
                       (double)(returnedNs - be) / 1000.0);
                printf("      (segments should sum to about the round trip; a large excess means\n"
                       "       the instrument is measuring itself)\n");

                // ---- AND SPLIT DISPATCH ITSELF, when the build can ---------------------------
                //
                // "It is dispatch" is an answer that contains four more. These marks cut push ->
                // body start at the three transitions that have different causes and different
                // fixes:
                //
                //   push -> Wake      THE OS. Notify, and the target thread being scheduled again.
                //                     A parked worker on a core in a deep C-state lives here, and
                //                     nothing in this library can shorten it.
                //   Wake -> PreSteal  the worker is running but has not started looking.
                //   PreSteal -> Found the SEARCH. Large means the task was queued somewhere this
                //                     worker had to hunt for -- or could not take.
                //   Found -> body     the tail after the task is in hand.
                //
                // Requires -DJLIBSCHED_LATENCY_STATS=ON; a normal build reports zeros and this is
                // skipped. Do not compare a stats build's absolute numbers against a normal one.
                if (JLib::kLatencyStatsEnabled) {
                    int64_t wNs = 0, psNs = 0, fNs = 0;
                    JLib::LatencyStatsRead(wNs, psNs, fNs);
                    // EACH MARK IS JUDGED SEPARATELY, and the first version's failure to do that
                    // threw away the interesting case. Requiring all three to be current discards
                    // the whole split whenever `Wake` is stale -- but a stale Wake is not a broken
                    // measurement, it is a RESULT: no park exit happened, so the task was taken by a
                    // worker that was ALREADY AWAKE and no OS wake is in this number at all. On a
                    // grown floor that is the common case, and it is precisely the case worth
                    // seeing, because it rules the kernel out and leaves placement and the search.
                    const bool wakeCurrent   = (wNs  > pushDoneNs);
                    const bool searchCurrent = (psNs > pushDoneNs && fNs >= psNs && bs >= fNs);
                    if (searchCurrent && wakeCurrent && psNs >= wNs) {
                        printf("        push  -> Wake      %8.1f us   <- the OS wake\n",
                               (double)(wNs - pushDoneNs) / 1000.0);
                        printf("        Wake  -> PreSteal  %8.1f us\n", (double)(psNs - wNs) / 1000.0);
                        printf("        PreSteal -> Found  %8.1f us   <- the search\n",
                               (double)(fNs - psNs) / 1000.0);
                        printf("        Found -> body      %8.1f us\n", (double)(bs - fNs) / 1000.0);
                    } else if (searchCurrent) {
                        printf("        NO PARK EXIT this iteration -- the worker was already awake,\n"
                               "        so none of this delay is an OS wake.\n");
                        printf("        push  -> PreSteal  %8.1f us   <- placement + the worker\n"
                               "                                          getting round to looking\n",
                               (double)(psNs - pushDoneNs) / 1000.0);
                        printf("        PreSteal -> Found  %8.1f us   <- the search\n",
                               (double)(fNs - psNs) / 1000.0);
                        printf("        Found -> body      %8.1f us\n", (double)(bs - fNs) / 1000.0);
                    } else {
                        // The marks are process-wide globals, so a set that predates this push
                        // belongs to a DIFFERENT iteration and must be discarded rather than printed
                        // as a negative or a wild number.
                        printf("        (search marks predate this push -- discarded, they belong to\n"
                               "         another round trip)\n");
                    }
                }
                // WHICH WORKER, and it is the question the band dump cannot answer. The dump shows
                // who was awake at DUMP time; this shows who actually ran the task. A stall that
                // lands on one of the few SLEEPING indices while a floor of 13 spins is a placement
                // result -- steering chose a parked worker and bought a wake it did not need to.
                printf("      landed on worker q%d\n", g_bodyQ.load(std::memory_order_relaxed));
                const unsigned long long wakeDelta = wakesAfter - wakesBefore;
                printf("      kernel wakes bought by this push: %llu\n", wakeDelta);
                // SAY WHAT THE COUNTER MEANS, NOT WHAT IT SUGGESTS. This line used to read "an
                // awake worker did not get round to draining its own inbox", which describes a
                // DELAY in the vocabulary of a HANG and was read that way. The task completed --
                // that is the only reason there is a number here at all. A strand does not produce
                // a slow round trip, it produces no round trip: the serial row waits forever and
                // the run never finishes. Nothing in this block can report one.
                printf(wakeDelta == 0
                       ? "        0 -> NO NOTIFY WAS SENT. The producer read the target as WS_AWAKE\n"
                         "        and skipped the wake, which is correct and is what avoids the\n"
                         "        kernel cost -- but it means nothing hurries this task along. Its\n"
                         "        latency is entirely that one worker's next poll of its own inbox,\n"
                         "        and if the OS is not running that thread, nothing will chase it.\n"
                         "        The task still completes; it is late, not lost.\n"
                       : "        >0 -> a wake WAS issued: the target had advertised intent to park,\n"
                         "        so this delay is the OS putting that thread back on a core.\n");
            } else {
                printf("      (no body stamps: the task had not started when the wait ended --\n"
                       "       impossible for a WaitGroup of 1, so treat this line as a bug)\n");
            }

            // ---- AND WHETHER THIS THREAD WAS EVEN RUNNING --------------------------------
            //
            // THE POINT OF THE WHOLE EXERCISE. A long completion segment has two causes that look
            // identical in every timing: the waiter spun and kept seeing the count above zero, or
            // the waiter was not on a CPU at all. polls tells them apart without a clock.
            //
            // CALIBRATE AGAINST THE HEALTHY RATE PRINTED WITH THE ROW, and do not guess it. A
            // poll is a load, a steal attempt and a CpuRelax, and the first measurement of it came
            // out at ~125 ns -- so a HEALTHY p50 round trip spins about 4 times, not the thousand
            // that seemed obvious before it was measured. The scale that matters is therefore:
            //
            //     healthy                      ~4 polls
            //     50 us STALL, spun through    ~400 polls      (50 us / 125 ns)
            //     50 us STALL, off-CPU         still ~4        the thread never looked
            //
            // Two orders of magnitude apart, which is what makes this decisive -- but the ns/poll
            // figure is machine-specific, so read the ratio against the row's own printed rate
            // rather than against the numbers above.
            printf("      waiter: %u polls, %u yields, %u helped\n", wPolls, wYields, wHelped);
            printf("        polls HIGH -> waiter ran and kept seeing n>0: the pool was late.\n"
                   "        polls LOW  -> waiter was NOT SCHEDULED: nothing was missed.\n");
            if (wHelped)
                printf("        NOTE: helped>0 -- this thread RAN a stolen task during the wait,\n"
                       "        so this round trip is not a dispatch measurement.\n");
            // NO POOL DUMP HERE, AND ITS ABSENCE IS THE FIX. One used to print at this point under
            // the heading "Pool state AT THE MOMENT it happened", which was not true: the task has
            // completed, WaitFor has returned, and a serial loop has nothing else outstanding -- so
            // the pool is idle BY CONSTRUCTION and the dump could only ever show two floor workers
            // awake and everything else parked. That output was read as evidence more than once,
            // including to build and then retract a theory about the park path.
            //
            // The watcher started before this loop dumps DURING the stall instead, which is the
            // only time the state means anything. If it did not fire, the stall was shorter than
            // its threshold, and that is worth knowing too.
            printf("  (see the STALL IN PROGRESS dump above if the watcher caught one -- this\n"
                   "   line is only the measurement. The pool is idle by now, so a dump taken\n"
                   "   here would show nothing but an idle pool and has been removed.)\n\n");
        }

        // THE CALIBRATION FOR THE POLL COUNT, and it is why the pathological block can say
        // "HIGH" and "LOW" and mean something. A poll is a load plus a steal attempt plus a
        // CpuRelax, and how many of those fit in a microsecond is a property of THIS machine --
        // so the healthy rate has to be measured here rather than assumed. Collected only from
        // iterations that did not blow the threshold, so the outlier cannot skew its own baseline.
        if (rt <= kPathologicalUs) { healthyPollSum += wPolls; healthyPollN += 1; }

        if (JLib::kLatencyStatsEnabled) {
            int64_t wakeNs = 0, preStealNs = 0, foundNs = 0;
            JLib::LatencyStatsRead(wakeNs, preStealNs, foundNs);
            // Skip a sample where a mark didn't fire this iteration (shouldn't happen in the
            // serial/idle-pool steady state, but a control that can't go negative is worth more
            // than one that silently accepts garbage).
            if (wakeNs > pushNs && preStealNs >= wakeNs && foundNs >= preStealNs) {
                wakeUs.push_back((double)(wakeNs - pushNs) / 1000.0);
                loopEntryUs.push_back((double)(preStealNs - wakeNs) / 1000.0);
                stealScanUs.push_back((double)(foundNs - preStealNs) / 1000.0);
            }
        }
    }
    double totalMs = MsBetween(t0, Clock::now());

    // Stopped before the row is printed, so the spinning core is released and nothing below this
    // point is measured with an extra thread running.
    watcherStop.store(true, std::memory_order_release);
    watcher.join();

    std::sort(rtUs.begin(), rtUs.end());
    auto pct = [&](double p) { return rtUs[(size_t)((double)(rtUs.size() - 1) * p)]; };
    printf("latency      : %d serial round-trips in %.2f ms  ->  mean %.2f us per push->run->wait\n",
        kIters, totalMs, totalMs * 1000.0 / kIters);
    printf("               p50 %.2f us | p99 %.2f us | max %.2f us   <- p99/max are the floor test,\n"
           "               because an OS wake cannot hide there the way it hides in the mean\n",
        pct(0.50), pct(0.99), rtUs.back());
    // ---- THE COMPARABLE NUMBER, and the only one in this row that survives a rerun -------------
    printf("               stalls >%.0f us: %llu of %d", kPathologicalUs, stallCount, kIters);
    if (stallCount) {
        printf("   dispatch %llu (max %.1f) | execution %llu (max %.1f) | completion %llu (max %.1f)\n",
               stallDispatch, maxDispatchUs, stallExecution, maxExecutionUs,
               stallCompletion, maxCompletionUs);
        printf("               of those, %llu bought a kernel wake -- the rest hit a target that was\n"
               "               ALREADY AWAKE, so no OS wake is in them at all\n", stallWithWake);
        printf("               ^ COMPARE THESE COUNTS BETWEEN ARMS, NOT max. Five identical runs of\n"
               "                 this row spanned 27.5..197.9 us of max (7.2x) with p50/p99 stable,\n"
               "                 so a max read from one run measures the afternoon, not the config.\n");
    } else {
        printf("   (none)\n");
    }
    if (healthyPollN) {
        const unsigned long long meanPolls = healthyPollSum / healthyPollN;
        // ns per poll, from the p50 round trip. Guarded because a p50 fast enough to complete
        // inside the first load -- zero polls -- is a legitimate outcome on a live floor, and it
        // must print as "0 polls", not divide by it.
        const double perPoll = meanPolls ? (pct(0.50) * 1000.0 / (double)meanPolls) : 0.0;
        printf("               waiter spun %llu polls on a healthy p50 round trip (~%.0f ns/poll)\n"
               "               ^ THE YARDSTICK for any STALL report above: a stall the waiter spun\n"
               "                 THROUGH scales its poll count with its duration. One that reports a\n"
               "                 healthy-looking count was not spinning -- it was off-CPU.\n",
            meanPolls, perPoll);
    }

    // ---- DID THE FLOOR RECEIVE THE WORK? -----------------------------------------------------
    //
    // Two independent views of one question. A serial round trip into a pool with a live floor
    // should wake NOBODY: placement steers at a worker that is already spinning, which picks the
    // inbox up on its next pass. Thousands of wakes, or a low on-floor percentage, both mean
    // placement is still routing to sleepers and the floor is decoration.
    {
        // THE FLOOR IS [K, K+F), NOT [0, F). This counted [0, floorN) -- which is the RESERVED band
        // that ordinary work is deliberately masked OUT of -- and so reported "0.0% landed on the
        // floor" on the same row as "kernel wakes: 0". Those cannot both be true, and the wake count
        // is the one that was right: work was landing on the floor exactly as intended and the
        // instrument was looking at the wrong band. A diagnostic left behind by a layout change does
        // not go quiet, it lies with a plausible number.
        const size_t floorN = JLib::TaskScheduler::GetAwakeFloor();
        const size_t kResv  = JLib::TaskScheduler::GetHotWorkers();
        unsigned onFloor = 0, onResv = 0, total = 0;
        for (size_t q = 0; q < 64; ++q) {
            const unsigned c = g_landedOn[q].load(std::memory_order_relaxed);
            total += c;
            if (q >= kResv && q < kResv + floorN) onFloor += c;
            else if (q < kResv)                   onResv  += c;
        }
        printf("               floor during this row: %zu -> %zu (base %zu)  <- if this is above the\n"
               "               base, an earlier row grew it and did not shed; every number in this\n"
               "               row is then a measurement of THAT floor, not of the configured one\n",
            floorAtLatencyStart, floorN, JLib::TaskScheduler::GetAwakeFloorBase());
        printf("               kernel wakes this row: %llu  (a live floor should need ~0)\n",
            (unsigned long long)JLib::TaskScheduler::GetWakeCount());
        printf("               landed on the floor [%zu,%zu): %u of %u (%.1f%%)  -- the rest paid a wake\n",
            kResv, kResv + floorN, onFloor, total,
            total ? 100.0 * (double)onFloor / (double)total : 0.0);
        if (onResv)
            printf("               landed in the RESERVED band [0,%zu): %u  <- ordinary work is masked\n"
                   "               out of it, so anything here is a placement leak\n", kResv, onResv);
        if (total) {
            printf("               landing spread:");
            for (size_t q = 0; q < 64; ++q) {
                const unsigned c = g_landedOn[q].load(std::memory_order_relaxed);
                if (c) printf(" q%zu=%u", q, c);
            }
            printf("\n");
        }
    }

    if (!JLib::kLatencyStatsEnabled) return;
    auto med = [](std::vector<double> v) -> double {
        if (v.empty()) return 0.0;
        std::sort(v.begin(), v.end());
        return v[v.size() / 2];
        };
    printf("  breakdown (median of %zu clean samples): OS wake %.2f us, loop entry (sections 1-3)"
        " %.2f us, steal scan (section 4) %.2f us\n",
        wakeUs.size(), med(wakeUs), med(loopEntryUs), med(stealScanUs));
    printf("  (loop entry + steal scan = the cost paid between waking and finding the task that woke\n"
           "   you, which sits in the INBOX, checked only in section 5 -- after the local deque and\n"
           "   the full steal scan both fail against an otherwise-idle pool)\n");
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
    // THE FLOOR THIS ROW RAN UNDER, for the same reason the latency row prints it. This row moved
    // 3.51 -> 6.6 us when push-side spilling was added and did NOT come back when the collapse was
    // fixed, so the cause is still open: a floor stuck at 16 (spinner contention) and a floor at 2
    // with the spill re-routing graph nodes are different diagnoses wanting opposite fixes, and the
    // us/graph number alone cannot tell them apart.
    printf("               floor during this row: %zu (base %zu, peak %zu)\n",
           JLib::TaskScheduler::GetAwakeFloor(), JLib::TaskScheduler::GetAwakeFloorBase(),
           JLib::TaskScheduler::GetAwakeFloorPeak());
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
// (This used to call the removed ParallelForFJ DIRECTLY, to dodge the fixed 10k gate it was
// measuring -- going through ParallelFor would have reported serial-vs-serial below it. There is no
// gate at all now, so the sweep calls ParallelFor normally.)


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
    std::vector<size_t> participants;    speedups.reserve(sizes.size());
    serialUs.reserve(sizes.size());

    for (int n : sizes) {
        constexpr int kRuns = 7;
        double bestSerial = 1e300, bestPar = 1e300;        for (auto& c : g_sweepSeen) c.store(0, std::memory_order_relaxed);

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
            // ParallelFor, not the removed fork-join entry point. There is no gate to dodge any more:
            // the range is split speculatively and steals decide, so this measures dispatch plus
            // whatever the pool chose to take. Cells below ~1.00x mean the splitter parallelized a
            // body too cheap to be worth it -- which nothing probe-free can refuse to do.
            sched.ParallelFor(0, n, grain, [&](int a, int b) {
                // WHO RAN THIS LEAF. The one number that separates "the wake did not fire" from
                // "the wake fired and the thief found nothing": if a row is slow with 2-3
                // participants the steal scan is not reaching the non-worker lane; if it is slow
                // with 20, the thieves arrived and the cost is per-leaf.
                if (JLib::Thread* w = JLib::Thread::Current()) {
                    const int q = w->qIndex;
                    if (q >= 0 && q < 64) g_sweepSeen[q].store(1, std::memory_order_relaxed);
                }
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
        participants.push_back(SweepParticipants());
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
    // WORKERS THAT RAN A LEAF, per cell. A slow row with 2-3 participants means the thieves never
    // arrived and the steal scan is the bug; a slow row with 20 means they did arrive and the cost
    // is per-leaf. From the ratio alone those two are indistinguishable, and they want opposite
    // fixes -- which is how an afternoon goes into fixing the wrong one.
    printf("             distinct workers that ran a leaf (count, not an index; pool has %zu):",
           JLib::TaskScheduler::Instance().GetWorkerCount());
    for (size_t i = 0; i < participants.size(); ++i) printf(" %zu", participants[i]);
    printf("\n");
}

static void BenchParallelForCrossover(JLib::TaskScheduler& sched) {
    // Finer low end than the first pass: the interesting crossovers for expensive bodies sit between
    // 500 and 4000, and the original grid stepped straight over them.
    const std::vector<int> sizes = { 256, 512, 1000, 2000, 4000, 10000, 40000, 200000 };

    printf("\nParallelFor crossover sweep (speedup = serial/parallel; >1.00 means parallel wins)\n");
    printf("  a crossover is only reported at >=%.2fx confirmed by the next size -- below that is noise\n",
           kWinMargin);
    printf("  no gate: the range is split speculatively and STEALS decide -- an untaken split is\n");
    printf("           taken back and run inline (~11 ns). Nothing probe-free can decline a body\n");
    printf("           too cheap to parallelize, so cells below 1.00x are that, not a bug.\n");
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

// ---------------- splitter vs cursor: the comparison the README's crossover table depended on ----
// RunCursorRange is PUBLIC and is what ParallelFor falls back to when a second non-worker thread is
// already splitting -- and the README documents it as measurably beating the recursive splitter
// above roughly N=200,000 on a uniform body (22.6x vs 18.5x, one machine). That table came from a
// one-off scratchpad harness that was never committed to this repo. Nothing -- not CI, not a second
// machine, not a future session -- could reproduce it. This is that comparison, done properly, kept
// where it can be rebuilt.
//
// INTERLEAVED WITH A CONTROL, on purpose, and this is not decoration: a BLOCK-MEASURED comparison
// between two range APIs is exactly what produced a fictional 15-47% gap the first time this
// library compared ParallelFor against ParallelRange (see the 1.4.0 CHANGELOG entry on it). Every
// rep re-runs the splitter a second time as `ctl`; if ctl disagrees with the first splitter run by
// more than the printed threshold, the cell is marked suspect rather than trusted. A `?` on a cell
// means the machine was not quiet enough for THAT reading, not that the cursor tied the splitter.
template <int kFlops>
static void SweepSplitterVsCursor(JLib::TaskScheduler& sched, const char* label, const std::vector<int>& sizes) {
    printf("  %-10s |", label);
    std::vector<double> ratios;
    std::vector<bool>   suspects;   // per cell: did its own same-vs-same control move >5%?
    ratios.reserve(sizes.size());

    for (int n : sizes) {
        constexpr int kReps = 7;   // matches SweepOne's kRuns; this has three arms per rep instead
                                    // of two, so it is already the slower of the two sweeps.
        const int workers = (int)std::max(1u, std::thread::hardware_concurrency() - 1u);
        const int grain   = std::max(1, n / (workers * 4));

        // Both arms run the IDENTICAL body -- same accumulation, same CAS-loop reduction -- so the
        // only thing that can differ between them is the range mechanism itself.
        auto runSplitter = [&]() -> double {
            std::atomic<double> pacc{ 0.0 };
            auto t0 = Clock::now();
            sched.ParallelFor(0, n, grain, [&](int a, int b) {
                double local = 0.0;
                for (int i = a; i < b; ++i) local += BodyCost<kFlops>(i);
                double cur = pacc.load(std::memory_order_relaxed);
                while (!pacc.compare_exchange_weak(cur, cur + local, std::memory_order_relaxed)) {}
                });
            const double ms = MsBetween(t0, Clock::now());
            SinkAdd(pacc.load());
            return ms;
        };
        auto runCursor = [&]() -> double {
            std::atomic<double> pacc{ 0.0 };
            // Named lvalue, non-const -- RunCursorRange's signature does not accept a temporary.
            std::function<void(int, int)> body = [&](int a, int b) {
                double local = 0.0;
                for (int i = a; i < b; ++i) local += BodyCost<kFlops>(i);
                double cur = pacc.load(std::memory_order_relaxed);
                while (!pacc.compare_exchange_weak(cur, cur + local, std::memory_order_relaxed)) {}
                };
            auto t0 = Clock::now();
            sched.RunCursorRange(0, n, grain, body);
            const double ms = MsBetween(t0, Clock::now());
            SinkAdd(pacc.load());
            return ms;
        };

        // Parallel to `ratios`: whether each cell's own same-vs-same control held. The verdict
        // below consults it; printing a '?' the reader must apply by hand is not a control.
        std::vector<double> splitMs, cursorMs, ctlMs;
        splitMs.reserve(kReps); cursorMs.reserve(kReps); ctlMs.reserve(kReps);
        for (int r = 0; r < kReps; ++r) {
            splitMs.push_back(runSplitter());    // A
            cursorMs.push_back(runCursor());     // B
            ctlMs.push_back(runSplitter());      // A again -- isolates run-to-run noise from A vs B
        }

        auto med = [](std::vector<double> v) { std::sort(v.begin(), v.end()); return v[v.size() / 2]; };
        const double splitMed  = med(splitMs);
        const double cursorMed = med(cursorMs);
        const double ctlMed    = med(ctlMs);
        // >1.00 means the cursor took LESS wall time than the splitter at this N -- the cursor wins.
        const double cursorWins = splitMed / std::max(cursorMed, 1e-9);
        const double noiseFloor = splitMed / std::max(ctlMed, 1e-9);
        ratios.push_back(cursorWins);

        const bool suspect = std::fabs(noiseFloor - 1.0) > 0.05;   // control moved >5% on its own
        suspects.push_back(suspect);
        printf(" %5.2fx%s", cursorWins, suspect ? "?" : " ");
    }

    // Same persistence rule SweepOne uses: the first N where the cursor clears the margin AND is
    // still ahead at the next size, so a lone spike is not reported as a real crossover.
    // SKIP THE CELLS THIS FUNCTION JUST FLAGGED. `suspect` was computed, printed as '?', and then
    // discarded -- so the verdict was derived from data the same code told the reader not to
    // believe. On a run where 19 of 24 cells were flagged it still announced "cursor ahead from
    // N=1000" and "from N=100000", both resting on flagged cells, while the only two UNFLAGGED
    // cells in the table disagreed with each other about the direction.
    //
    // A control that is displayed but not obeyed is worse than no control: it looks like rigour
    // and changes nothing. If every candidate is suspect there is no verdict to report, and saying
    // so is the honest output.
    int crossover = -1;
    bool anyTrustworthy = false;
    for (size_t i = 0; i < ratios.size(); ++i) {
        if (suspects[i]) continue;                 // the control moved on its own here
        anyTrustworthy = true;
        if (ratios[i] < kWinMargin) continue;
        // The persistence rule still applies, but a suspect NEIGHBOUR cannot refute a good cell --
        // treat it as absent rather than as evidence against.
        if (i + 1 < ratios.size() && !suspects[i + 1] && ratios[i + 1] < kWinMargin) continue;
        crossover = sizes[i];
        break;
    }
    if (crossover > 0)          printf("   | cursor ahead from N=%d\n", crossover);
    else if (!anyTrustworthy)   printf("   | NO VERDICT -- every cell's own control moved >5%%\n");
    else                        printf("   | splitter ahead (or tied) across this whole range\n");
}

static void BenchSplitterVsCursorCrossover(JLib::TaskScheduler& sched) {
    // Extends past the README's largest point (200,000) with two further sizes, so a crossover
    // found there gets the SAME confirmation-by-next-size discipline as everything else in this
    // file, rather than standing alone as the last, unconfirmed column the way it did before.
    const std::vector<int> sizes = { 1000, 4000, 20000, 100000, 200000, 400000 };

    printf("\nSplitter vs cursor (RunCursorRange, direct) -- ratio = splitter_ms/cursor_ms; >1.00 means "
           "the cursor wins\n");
    printf("  '?' marks a cell whose same-vs-same control moved >5%% on its own -- distrust that cell,\n");
    printf("  not the cursor. See the ParallelFor doc table in README.md for when to reach for this.\n");
    printf("  %-10s |", "body");
    for (int n : sizes) printf(" %6d", n);
    printf("    |\n");
    printf("  -----------+--------------------------------------------------------+------------------\n");

    SweepSplitterVsCursor<1>   (sched, "trivial", sizes);
    SweepSplitterVsCursor<8>   (sched, "light",   sizes);
    SweepSplitterVsCursor<64>  (sched, "medium",  sizes);
    SweepSplitterVsCursor<512>(sched, "heavy",   sizes);

    printf("  (sink %.1f -- printed only so the bodies can't be optimized away)\n", g_sink.load());
}

// ------------------------------------------------------ splitpref: does the SPLITTER want Wide?
//
// THE ONE QUESTION THIS ROW EXISTS FOR. RunLazyRange places each split half, and whether that push
// should be steered at the awake floor (Default) or spread across the pool (Wide) is unsettled. The
// case for Default is that the split is SPECULATIVE -- an untaken one is taken straight back and run
// inline for ~11 ns -- so a kernel wake per split, recursively, is the wrong currency. That is a
// prediction.
//
// IT CANNOT BE ANSWERED ACROSS RUNS, and that is why this is a row rather than a flag you set twice.
// The crossover's serial baselines have been seen to move 2x between runs of the same binary, and
// the splitter-vs-cursor table reads 1.46-1.76 on a throttled process against 1.02-1.07 on a quiet
// one -- same code. Anything compared across two invocations is measuring the machine.
//
// SO THE ARMS ALTERNATE INSIDE ONE LOOP, A/B/A/B, and the reported number is the median of each.
// Interleaving is what makes thermal drift, a background process and clock behaviour common-mode.
//
// AND IT MUST BE READ AT A FIXED FLOOR. At floor=31 nothing parks, the steer set IS the whole pool,
// and Default and Wide place almost identically -- the row would honestly report ~1.00x and mean
// nothing. Run it at the default floor=2, where the two differ.
static void BenchSplitPref(JLib::TaskScheduler& sched) {
    printf("\nsplitpref    : does the LAZY SPLITTER want Wide? ratio = default_ms/wide_ms; >1.00 means Wide wins\n");
    printf("               arms ALTERNATE inside one loop -- a cross-run comparison of this cannot\n"
           "               work, see the note above. Read at floor=2; at a wide floor both arms\n"
           "               place identically and this is ~1.00x by construction.\n");

    struct Case { const char* name; int n; int work; };
    const Case cases[] = {
        { "medium N=20000",  20000,  64 },
        { "heavy  N=20000",  20000, 512 },
        { "heavy  N=200000", 200000, 512 },
    };

    for (const Case& c : cases) {
        std::vector<double> dflt, wide;
        int seenD = 0, seenW = 0;
        constexpr int kReps = 7;
        for (int r = 0; r < kReps; ++r) {
            for (int arm = 0; arm < 2; ++arm) {
                JLib::TaskScheduler::SetParallelSplitWide(arm != 0);
                for (auto& s : g_sweepSeen) s.store(0, std::memory_order_relaxed);
                const auto t0 = Clock::now();
                sched.ParallelFor(0, c.n, 1, [&](int lo, int hi) {
                    // WHICH WORKERS ACTUALLY GOT A SPLIT. This is the number worth trusting here:
                    // a participant count is a COUNT, and it survives the thermal drift and
                    // background load that make the millisecond ratio argue with itself run to run.
                    // It also separates the two things the ratio conflates -- "Wide spread the work
                    // further and still lost" is a complete answer; "Wide was slower" is not.
                    if (JLib::Thread* w = JLib::Thread::Current()) {
                        const int q = w->qIndex;
                        if (q >= 0 && q < 64) g_sweepSeen[q].store(1, std::memory_order_relaxed);
                    }
                    double acc = 0;
                    for (int i = lo; i < hi; ++i)
                        for (int k = 0; k < c.work; ++k) acc += (double)(i ^ k) * 1.000001;
                    // store, not fetch_add: std::atomic<double> has no fetch_add before C++20 and
                    // this only needs to defeat the optimiser, not accumulate correctly.
                    g_sink.store(g_sink.load(std::memory_order_relaxed) + acc,
                                 std::memory_order_relaxed);
                });
                const double ms = MsBetween(t0, Clock::now());
                int seen = 0;
                for (auto& s : g_sweepSeen) if (s.load(std::memory_order_relaxed)) ++seen;
                if (arm) { wide.push_back(ms); seenW = (seen > seenW) ? seen : seenW; }
                else     { dflt.push_back(ms); seenD = (seen > seenD) ? seen : seenD; }
            }
        }
        JLib::TaskScheduler::SetParallelSplitWide(false);   // leave the process on the shipped default

        // ---- THE SPREAD, NOT JUST THE MEDIAN -------------------------------------------------
        //
        // PAIRED PER REP, because the rep is the interleaved unit: arm A and arm B ran back to back
        // under the same machine state, so their ratio is the only comparison that means anything.
        // Taking the median of each arm separately and dividing throws that pairing away.
        //
        // AND THE RANGE IS PRINTED because this row was read three times as three different
        // answers -- one cell measured 0.91x, 1.06x and 1.21x across runs while participation never
        // moved. A single number invited "Wide loses" and then "Wide wins" from the same binary.
        // Same discipline as the throughput rows: read the range, not the headline.
        std::vector<double> ratio;
        for (size_t i = 0; i < dflt.size() && i < wide.size(); ++i)
            if (wide[i] > 0.0) ratio.push_back(dflt[i] / wide[i]);
        std::sort(ratio.begin(), ratio.end());
        const double rMid = ratio.empty() ? 0.0 : ratio[ratio.size() / 2];
        const double rLo  = ratio.empty() ? 0.0 : ratio.front();
        const double rHi  = ratio.empty() ? 0.0 : ratio.back();

        std::sort(dflt.begin(), dflt.end());
        std::sort(wide.begin(), wide.end());
        const double d = dflt[dflt.size() / 2];
        const double w = wide[wide.size() / 2];
        printf("               %-16s default %7.3f ms (%2d workers) | wide %7.3f ms (%2d workers)"
               "  ->  %.2fx  [%.2f .. %.2f]\n",
               c.name, d, seenD, w, seenW, rMid, rLo, rHi);
    }
}

// ------------------------------------------------- inbox-drain dispatch: Requeue loop vs PushBatch
// Worker()'s immediate/fork inbox drain (Thread.cpp, "2. Immediate task execution") empties a
// worker's own inbox one task at a time via Requeue() before that worker pins to a persistent
// service task -- each call pays its own PickNextWorker + spin-check + single-item push +
// NotifyWorker (a mutex lock). PushBatch exists specifically to amortize that per-task notify cost
// across a segmented, spread submission, and the drained tasks already sit in a contiguous array at
// the point Requeue is called today, so swapping the loop for one PushBatch call costs nothing to
// wire up IF it is actually faster at the sizes that drain sees (a round is capped at BATCH_SIZE=64).
//
// This isolates exactly the two production functions being compared (Requeue, PushBatch), not a
// reimplementation of either, so the numbers here transfer directly to the real drainInbox change.
// Same interleaved-with-control discipline as SweepSplitterVsCursor above, for the same reason: a
// block-measured A-then-B comparison is exactly what produced a fictional splitter-vs-cursor gap
// once already (1.4.0 CHANGELOG). Timing covers DISPATCH only (the loop/call itself) -- completion
// is waited for outside the timed region so a rep can't contaminate the next one, but is not part of
// the cost being compared: the pinning worker only cares how fast it clears its hands of the batch.
static void SweepRequeueVsPushBatch(JLib::TaskScheduler& sched, const std::vector<int>& sizes) {
    constexpr int kReps = 9;

    for (int n : sizes) {
        auto runRequeue = [&]() -> double {
            JLib::WaitGroup wg;
            wg.n.store(n, std::memory_order_relaxed);
            std::vector<JLib::Task*> tasks((size_t)n);
            for (int i = 0; i < n; ++i) {
                tasks[i] = sched.CreateTask(+[](void*) {}, nullptr);
                tasks[i]->waitGroup = &wg;
            }
            auto t0 = Clock::now();
            for (int i = 0; i < n; ++i) sched.Requeue(tasks[i]);
            const double us = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
            sched.WaitFor(wg);   // drain before the next arm reuses the pool -- excluded from the timing
            return us;
        };
        auto runPushBatch = [&]() -> double {
            JLib::WaitGroup wg;
            wg.n.store(n, std::memory_order_relaxed);
            std::vector<JLib::Task*> tasks((size_t)n);
            for (int i = 0; i < n; ++i) {
                tasks[i] = sched.CreateTask(+[](void*) {}, nullptr);
                tasks[i]->waitGroup = &wg;
            }
            // minPerSegment=8: sized for THIS caller's batch sizes (<=256), not inherited from
            // ParallelFor's default -- PushBatch's own header warns that splitting a small batch too
            // finely pays more notifies than the parallelism is worth.
            auto t0 = Clock::now();
            sched.PushBatch(tasks.data(), (size_t)n, /*cpuaffinity*/0, /*minPerSegment*/8, /*hiPri*/false);
            const double us = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
            sched.WaitFor(wg);
            return us;
        };

        std::vector<double> reqUs, pbUs, ctlUs;
        reqUs.reserve(kReps); pbUs.reserve(kReps); ctlUs.reserve(kReps);
        for (int r = 0; r < kReps; ++r) {
            reqUs.push_back(runRequeue());     // A
            pbUs.push_back(runPushBatch());    // B
            ctlUs.push_back(runRequeue());     // A again -- isolates run-to-run noise from A vs B
        }

        auto med = [](std::vector<double> v) { std::sort(v.begin(), v.end()); return v[v.size() / 2]; };
        const double reqMed = med(reqUs);
        const double pbMed  = med(pbUs);
        const double ctlMed = med(ctlUs);
        // >1.00 means PushBatch took LESS wall time than the Requeue loop at this N -- PushBatch wins.
        const double pbWins     = reqMed / std::max(pbMed, 1e-9);
        const double noiseFloor = reqMed / std::max(ctlMed, 1e-9);
        const bool   suspect    = std::fabs(noiseFloor - 1.0) > 0.05;   // control moved >5% on its own

        printf("  N=%-4d  %6.2fx%s   (requeue=%7.2f us, pushbatch=%7.2f us)\n",
            n, pbWins, suspect ? "?" : " ", reqMed, pbMed);
    }
}

static void BenchRequeueVsPushBatch(JLib::TaskScheduler& sched) {
    // 64 is the real BATCH_SIZE a single drain round processes; 256 stands in for a busier inbox
    // (multiple rounds) the header comment on the real drain loop says it has to handle.
    const std::vector<int> sizes = { 8, 32, 64, 128, 256 };

    printf("\nInbox-drain dispatch (Worker()'s immediate/fork drain): per-task Requeue() loop vs one\n");
    printf("PushBatch() call -- ratio = requeue_us/pushbatch_us; >1.00 means PushBatch wins.\n");
    printf("  '?' marks a cell whose same-vs-same control moved >5%% on its own -- distrust that cell.\n");

    SweepRequeueVsPushBatch(sched, sizes);
}

// ================================================================================================
// WHAT hot=N ACTUALLY BUYS, and what an Event resume costs.
//
// THE GAP THESE CLOSE. Everything above measures the POOL: submission cliffs, fork-join, the DAG.
// None of it measures the product story -- I/O completion lands on the hot lane, a parked fiber is
// resumed, and a cold OS wake is what you pay when neither applies. Worse, `hot=N` has until now
// mostly just SHRUNK the compute pool, because every section reached it through a generic Push,
// which never routes to the lane. A paste with hot=2 therefore looked like a regression in
// throughput and showed nothing in return. These sections are the "in return".
//
// THE HOT API IS A hiPri PUSH. There is no PushHot(): PickNextWorker rotates the HOT set for hiPri
// and the ordinary set for everything else, so priority IS the routing. That also means the two
// arms below are only different when K > 0 -- at K = 0 the lane is collapsed and PushLocal's
// `useHi = hiPri && HiPriLaneActive()` sends a hiPri task to loPri anyway, deliberately, because
// nobody probes hiPri at K=0 and a task routed there would never run. So at K=0 this prints ONE row
// and says why, rather than printing two identical numbers side by side and inviting someone to
// read the difference between them as signal.
// ================================================================================================

static void BenchLatencyHotCold(JLib::TaskScheduler& sched) {
    constexpr int kIters = 20'000;
    const size_t K = JLib::TaskScheduler::GetHotWorkers();

    auto runArm = [&](bool hiPri) -> double {
        const auto t0 = Clock::now();
        for (int i = 0; i < kIters; ++i) {
            JLib::WaitGroup wg;
            wg.n.store(1, std::memory_order_relaxed);
            JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr, hiPri ? 1 : 0);
            if (!t) return -1.0;
            t->waitGroup = &wg;
            sched.Push(t);
            sched.WaitFor(wg);
        }
        return MsBetween(t0, Clock::now()) * 1000.0 / kIters;
    };

    // SKIP ONLY IF hiPri HAS NOWHERE TO BE STEERED. This tested K == 0, which stopped being the
    // right question in 5.0.0: hiPri no longer needs a reserved lane. A hiPri push now goes to the
    // hiPri INBOX of an awake-floor worker, and every worker drains hiPri before its own deque, so
    // the row means something whenever there is a floor to aim at -- with or without K.
    if (!JLib::TaskScheduler::HiPriLaneActive()
        || JLib::TaskScheduler::GetAwakeFloorBase() == 0) {
        const double us = runArm(false);
        printf("latency/hot  : SKIPPED -- hiPri has no steer target (floor=0 and K=0), so it lands\n");
        printf("               wherever ordinary placement sends it and this row would just be\n");
        printf("               latency/cold twice. Re-run with floor>=1 or hot>=1.\n");
        printf("latency/cold : %.2f us per push->run->wait\n", us);
        return;
    }

    // COLD FIRST, then hot. Order matters and not for fairness: the hot workers spin, so running
    // hot first leaves them warm and the cold arm measures a pool that has just been busy. Cold
    // first gives the cold arm the genuinely idle pool it is supposed to describe.
    const double coldUs = runArm(false);
    const double hotUs  = runArm(true);

    printf("latency/cold : %.2f us per push->run->wait  (ordinary push -> any worker, may be parked)\n", coldUs);
    // SAY THAT THIS ROW IS UNSTEERED, because next to the serial `latency` row it reads as a
    // contradiction: 3.97 us here against 0.54 us there, same pool, same floor. They are not
    // measuring the same thing. The serial row lands on a floor worker that is already scheduled;
    // this one takes the ordinary placement path to whichever worker comes up, parked or not, and
    // an OS wake is most of what it reports. That is the OLD path, kept deliberately as the
    // before-picture -- without it there is nothing to compare the floor against.
    printf("               ^ UNSTEERED, on purpose: this is the pre-floor round trip. The gap to\n"
           "                 the `latency` row above IS the floor's effect, not an inconsistency.\n");
    printf("latency/hot  : %.2f us per push->run->wait  (hiPri push -> hiPri inbox of an\n"
           "               awake-floor worker, drained before that worker's own deque)\n", hotUs);
    if (coldUs > 0.0)
        printf("               hot is %.2fx of cold%s\n", hotUs / coldUs,
               (hotUs < coldUs) ? " (lower is better -- the lane is doing its job)"
                                : " -- NOT faster; that is a finding, not a bad run");
}

// ================================================================================================
// EVENT / DIRECTEVENT RESUME. The first section in this file that exercises either primitive --
// fork-join is the only other fiber section, and it never parks on an event.
//
// THREE ARMS, and the third is NOT a variant of the first:
//
//   A  DirectEvent   1 waiter          hold-off 0 and 1 ms
//   B  Event         1 and 8 waiters   hold-off 0 and 1 ms
//   C  DirectEvent, waiter on a worker that has been allowed to PARK
//
// C IS THE PARKED-WAITER ROW, and it is only "the OS row" WHEN THE POOL CAN PARK. It is printed
// separately from A because it prices a different thing -- getting a stopped worker running again,
// rather than the primitive itself -- and averaging it into A hides what A exists to isolate.
//
// ITS MEANING NOW DEPENDS ON floor=N, so the caption is chosen at run time rather than asserted.
// At floor=0 this is a genuine OS wake: WaitOnAddress, a kernel round trip, a cold core. With a
// live floor the waiter's worker may never have parked at all, so the same number is no longer
// evidence about the OS -- calling it "the OS row" there would be a caption describing the old
// configuration. Measured at floor=2 it lands ~2.2 us, which is NOT a wake.
//
// THE HOLD-OFF IS THE POINT of running each arm twice. At 0 the waiter has only just parked and
// the resume may land while the fiber is still warm; at 1 ms it has certainly settled. The gap
// between those two numbers is the part that belongs to the machine rather than to the primitive.
//
// A AND B ARE NOT RANKED AGAINST EACH OTHER, AND THIS SECTION PRINTS NO RATIO BETWEEN THEM.
//
// TWO INDEPENDENT REASONS, either of which is sufficient.
//
// The sampling cannot support a ranking. Four consecutive runs on an idle machine gave 0.33x,
// 0.79x, 1.09x and 1.17x -- Direct ahead twice, Event ahead twice, across a factor of three and a
// half. And it is not simply a busy machine: in the 0.33x run the Event arm's MEDIAN OF 25 was
// 32.4 us where the other three put it at 6.6-10.0, so one arm sometimes collects a batch that
// twenty-five samples do not average away. Each arm's own number is a measurement; a quotient of
// two numbers that unstable is not, and printing it -- even hedged -- is what invites somebody to
// quote it.
//
// And they answer different questions, so a ranking would be the wrong output even with perfect
// sampling. Event is for REPEATED waits on a known event, where the slot index is the waiting
// fiber's own poolIndex and registering is two stores that allocate nothing. Direct is for a
// PER-OPERATION wait with a fresh identity -- a fence value, an I/O completion -- where the cost
// that matters is not the resume at all but the registry name you did not have to mint, because
// GetEvent holds a global mutex across its lookup and never evicts.
//
// Closing the small structural gap would mean preallocating one DirectEvent per fiber, which is
// Event's design plus a per-fiber object idle almost always -- memory spent chasing a difference
// this bench cannot resolve, to make one primitive better at a job the other already has.
//
// THE CONCLUSION THIS ROW MUST NOT PRODUCE is "Direct is slower, use Event." Direct exists so a
// PER-OPERATION wait -- a fence, an I/O completion, anything with a fresh identity each time --
// never mints a registry name. GetEvent holds a global mutex across its lookup and never evicts, so
// a name per operation is an unbounded map and a lock convoy that shows up an hour into a run
// looking exactly like a deadlock. That is what the pool round trip buys, and no latency number
// here can show it.
static void BenchEventResume(JLib::TaskScheduler& sched, bool includeSleepingArm) {
    // Signal -> resume, measured inside the resumed task so no cross-thread clock comparison is
    // needed beyond one steady_clock, which is monotonic across threads on every target here.
    static std::atomic<long long> s_signalNs{ 0 };
    static std::atomic<long long> s_resumeNs{ 0 };
    static std::atomic<JLib::DirectEvent*> s_direct{ nullptr };
    static std::atomic<int> s_armed{ 0 };
    static std::atomic<int> s_done{ 0 };

    auto nowNs = [] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   Clock::now().time_since_epoch()).count();
    };

    // ---- arm A / C: DirectEvent -------------------------------------------------------------
    auto directRound = [&](int holdOffMs) -> double {
        s_direct.store(nullptr, std::memory_order_relaxed);
        s_armed.store(0, std::memory_order_relaxed);
        s_done.store(0, std::memory_order_relaxed);

        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        JLib::Task* t = sched.CreateTask(+[](void*) {
            auto& s = JLib::TaskScheduler::Instance();
            s.WaitOnEventDirectArmed([](JLib::DirectEvent* e) {
                s_direct.store(e, std::memory_order_release);
                s_armed.store(1, std::memory_order_release);
            });
            s_resumeNs.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 Clock::now().time_since_epoch()).count(),
                             std::memory_order_release);
            s_done.store(1, std::memory_order_release);
        }, nullptr, false, JLib::TaskType::Fiber);
        if (!t) return -1.0;
        t->waitGroup = &wg;
        sched.Push(t);

        while (!s_armed.load(std::memory_order_acquire)) std::this_thread::yield();
        if (holdOffMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(holdOffMs));

        JLib::DirectEvent* e = s_direct.load(std::memory_order_acquire);
        s_signalNs.store(nowNs(), std::memory_order_release);
        e->Signal();
        sched.WaitFor(wg);

        const long long a = s_signalNs.load(std::memory_order_acquire);
        const long long b = s_resumeNs.load(std::memory_order_acquire);
        return (b > a) ? (double)(b - a) / 1000.0 : -1.0;
    };

    // ---- arm B: named Event, N waiters -------------------------------------------------------
    auto eventRound = [&](int waiters, int holdOffMs) -> double {
        // HOISTED, not looked up per wait. GetEvent takes a global mutex and a string hash; doing
        // that inside the measured region would time the registry rather than the resume, which is
        // the mistake its own header warns about.
        JLib::Event& ev = sched.GetEvent("bench/event-resume");
        s_armed.store(0, std::memory_order_relaxed);
        s_resumeNs.store(0, std::memory_order_relaxed);

        JLib::WaitGroup wg;
        wg.n.store(waiters, std::memory_order_relaxed);
        for (int i = 0; i < waiters; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void* p) {
                auto& s = JLib::TaskScheduler::Instance();
                JLib::Event* e = (JLib::Event*)p;
                s.WaitOnEventArmed(*e, [] { s_armed.fetch_add(1, std::memory_order_release); });
                // FIRST WAITER TO WAKE WINS THE TIMESTAMP. With N waiters the interesting number is
                // signal -> first resume; the tail of a SignalAll is a different question and is not
                // what this row claims to measure.
                long long expect = 0;
                s_resumeNs.compare_exchange_strong(
                    expect,
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        Clock::now().time_since_epoch()).count(),
                    std::memory_order_acq_rel);
            }, &ev, false, JLib::TaskType::Fiber);
            if (!t) { wg.n.fetch_sub(1, std::memory_order_release); continue; }
            t->waitGroup = &wg;
            sched.Push(t);
        }

        while (s_armed.load(std::memory_order_acquire) < waiters) std::this_thread::yield();
        if (holdOffMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(holdOffMs));

        s_signalNs.store(nowNs(), std::memory_order_release);
        ev.SignalAll();
        sched.WaitFor(wg);

        const long long a = s_signalNs.load(std::memory_order_acquire);
        const long long b = s_resumeNs.load(std::memory_order_acquire);
        return (b > a) ? (double)(b - a) / 1000.0 : -1.0;
    };

    auto medianOf = [&](const std::function<double()>& f, int reps) -> double {
        std::vector<double> v;
        v.reserve(reps);
        for (int i = 0; i < reps; ++i) { const double d = f(); if (d > 0.0) v.push_back(d); }
        if (v.empty()) return -1.0;
        std::sort(v.begin(), v.end());
        return v[v.size() / 2];
    };

    constexpr int kReps = 25;
    printf("event/resume : signal -> resumed, median of %d\n", kReps);

    const double a0 = medianOf([&] { return directRound(0); }, kReps);
    const double a1 = medianOf([&] { return directRound(1); }, kReps);
    printf("  A DirectEvent  1 waiter    hold-off 0ms: %7.2f us   1ms: %7.2f us\n", a0, a1);

    const double b0 = medianOf([&] { return eventRound(1, 0); }, kReps);
    const double b1 = medianOf([&] { return eventRound(1, 1); }, kReps);
    printf("  B Event        1 waiter    hold-off 0ms: %7.2f us   1ms: %7.2f us\n", b0, b1);
    const double b8 = medianOf([&] { return eventRound(8, 1); }, kReps);
    printf("  B Event        8 waiters   hold-off 1ms: %7.2f us  (signal -> FIRST resume)\n", b8);

    // NO VERDICT, DELIBERATELY, and this replaced code that declared one.
    //
    // Four consecutive runs on one idle machine put A/B at 0.33x, 0.79x, 1.09x and 1.17x -- Direct
    // twice ahead, Event twice ahead, spanning a factor of three and a half. An earlier version of
    // this section printed "Direct is faster" or "Event is faster, and that is EXPECTED" depending
    // on where a single run landed, which would have written whichever direction that run happened
    // to take into somebody's notes as a property of the primitives.
    //
    // The dispersion is not the machine being busy. In the 0.33x run the Event arm's MEDIAN OF 25
    // was 32.4 us against 6.6-10.0 us in the other three, so one arm occasionally collects a
    // pathologically slow batch that twenty-five samples do not average away. Until that is
    // understood, this row reports two numbers and no comparison.
    //
    // WHAT DOES REPRODUCE, run to run, is latency/hot at ~0.10x of latency/cold. Trust that row.
    // NO RATIO IS PRINTED. Not a labelled one, not a hedged one -- printing the number at all
    // invites the ranking, and 25 samples cannot support one: four consecutive runs on an idle
    // machine gave 0.33x, 0.79x, 1.09x and 1.17x, both directions, and in the first of those the
    // Event arm's MEDIAN was 32.4 us against 6.6-10.0 in the rest. Each arm's own number is a
    // measurement; the quotient of two numbers this unstable is not.
    //
    // They also answer different questions, so a ranking would be the wrong output even if the
    // sampling were good enough to produce one.
    printf("  These are NOT ranked against each other. Same job, different constraints:\n"
           "    Event  -- repeated waits on a KNOWN event. Slot index is the waiting fiber's own\n"
           "              poolIndex, so registering is two stores and allocates nothing. Hoist the\n"
           "              Event& once at startup; GetEvent by name takes a global mutex.\n"
           "    Direct -- one PER-OPERATION wait with a fresh identity (a fence value, an I/O\n"
           "              completion). Costs a pooled object, and in exchange never mints a\n"
           "              registry name -- GetEvent's map never evicts, so a name per operation is\n"
           "              an unbounded map and a lock convoy that looks like a deadlock an hour in.\n");

    if (includeSleepingArm) {
        // C: let the pool genuinely park before signalling. 50 ms is what BenchIdleBurst uses for
        // the same purpose, and it is the reason this arm is last -- it leaves the pool cold.
        auto sleeper = [&]() -> double {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            return directRound(0);
        };
        const double c = medianOf(sleeper, 10);
        printf("  C DirectEvent  waiter on a PARKED pool          : %7.2f us\n", c);
        if (JLib::TaskScheduler::GetAwakeFloor() == 0) {
            printf("     ^ THE OS ROW. This is the wake, not the primitive. Do not average it into A.\n");
        } else {
            printf("     ^ NOT the OS row at floor=%zu -- the waiter's worker need never have parked,\n"
                   "       so this is the resume, not a kernel wake. It IS the OS row at floor=0.\n"
                   "       Still not averaged into A: different question either way.\n",
                   JLib::TaskScheduler::GetAwakeFloor());
        }
    }
}

// ================================================================================================
// IO-PIPE: the number that belongs next to "I/O completions target the hot lane".
//
// There is no reactor in this executable, so one is faked in the only way that keeps the
// measurement honest: a SIDE std::thread -- explicitly not a pool worker -- plays the completion
// thread and pushes hiPri work, exactly as IoReactor's completion threads do. Routing this through
// a generic Push would measure the ordinary pool and label it I/O, which is the mislabelling this
// whole section exists to stop.
//
// MEASURED: completion -> job START, not job end. The body is trivial on purpose; what is being
// timed is how long a completion waits for a worker to pick it up.
static void BenchIoPipe(JLib::TaskScheduler& sched) {
    const size_t K = JLib::TaskScheduler::GetHotWorkers();
    // Same rule as latency/hot: what matters is whether a hiPri push has somewhere to be steered,
    // not whether K reserved anybody for it.
    if (!JLib::TaskScheduler::HiPriLaneActive()
        || JLib::TaskScheduler::GetAwakeFloorBase() == 0) {
        printf("io-pipe      : SKIPPED -- hiPri has no steer target (floor=0 and K=0), so this would\n");
        printf("               measure the ordinary pool and call it I/O.\n");
        return;
    }

    constexpr int kOps = 4'000;
    static std::atomic<long long> s_startNs{ 0 };
    std::vector<double> lat;
    lat.reserve(kOps);

    // Lane-reachability counter, sampled around THIS row only. It is a process-wide total, so the
    // delta is the only readable form -- latency/hot ran 20000 hiPri pushes before this.
    const unsigned long long spill0 = JLib::TaskScheduler::GetHiPriSpillCount();

    std::thread reactor([&] {
        for (int i = 0; i < kOps; ++i) {
            JLib::WaitGroup wg;
            wg.n.store(1, std::memory_order_relaxed);
            JLib::Task* t = sched.CreateTask(+[](void*) {
                s_startNs.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    Clock::now().time_since_epoch()).count(),
                                std::memory_order_release);
            }, nullptr, /*hiPri*/ 1);
            if (!t) { continue; }
            t->waitGroup = &wg;
            s_startNs.store(0, std::memory_order_relaxed);
            const long long post = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                       Clock::now().time_since_epoch()).count();
            sched.Push(t);
            sched.WaitFor(wg);
            const long long start = s_startNs.load(std::memory_order_acquire);
            if (start > post) lat.push_back((double)(start - post) / 1000.0);
        }
    });
    reactor.join();

    if (lat.empty()) { printf("io-pipe      : no samples\n"); return; }
    std::sort(lat.begin(), lat.end());
    printf("io-pipe      : completion -> job start, %zu samples (hiPri -> awake floor, K=%zu)\n",
           lat.size(), K);
    printf("               p50 %.2f us   p90 %.2f us   p99 %.2f us   max %.2f us\n",
           lat[lat.size() / 2], lat[(size_t)(lat.size() * 0.90)],
           lat[(size_t)(lat.size() * 0.99)], lat.back());

    // ---- DID THE LANE STAY REACHABLE, AND WHICH MECHANISM HAD TO ACT ------------------------
    //
    // Two different rescues, and they answer different halves of the same question:
    //
    //   spilled -- the PRODUCER found the reserved owner already inside a body and redirected the
    //              push to another worker in [0, K). Zero on a healthy lane. Rising means
    //              completions arrive faster than K retires them, which is K being too small for
    //              the completion rate -- a configuration answer, not a fault.
    //   staged  -- the OWNER unloaded its remaining lane tasks into its own hiPri deque on its way
    //              into a body, so a thief in the band could take them. Only reserved workers probe
    //              hiPri, so a staged task that never runs means the band had no spare consumer.
    //
    // THE SPILL CANNOT LEAVE [0, K). It walked the awake floor and then the whole awake pool until
    // 5.0.0, which put completions behind bulk bodies on the reasoning that an awake worker costs no
    // wake -- true, and not the question this row asks. At K == 1 there is no second reserved worker
    // and `spilled` is structurally 0; that is the mechanism being inert, not healthy.
    //
    // IT IS THE ONLY LANE RESCUE NOW. `staged` was printed beside it and is gone with the lane
    // deque -- there is no unload-at-dispatch to count, because there is nowhere to unload to.
    const unsigned long long spilled = JLib::TaskScheduler::GetHiPriSpillCount() - spill0;
    printf("               lane reachability this row: spilled=%llu  of %zu pushes\n",
           spilled, (size_t)kOps);
    printf("               ^ THIS ROW CANNOT SPILL, and 0 here is a negative control rather than a\n"
           "                 pass: it pushes one completion and waits for it, so the reserved owner\n"
           "                 is never inside a body when the next one arrives and the scan is never\n"
           "                 reached. io-pipe/ovl below is the arm that can move these.\n");
}

// Sink for the overlapped row's body, so the spin cannot be optimised away.
static std::atomic<unsigned long long> g_ioOverlapSink{ 0 };

// In-flight completions in the overlapped row. FILE SCOPE, not a local constexpr: MSVC will not
// take an enclosing function's constexpr as an array bound inside a lambda (C2131), and the array
// has to live in the lambda because that is where the window is built.
static constexpr size_t kIoOvlWindow = 8;   // > K on every configuration we ship

// ================================================================================================
// IO-PIPE/OVL: the same question with the lane actually under pressure.
//
// WHY A SECOND ROW RATHER THAN A CHANGE TO THE FIRST. The serial row measures a completion arriving
// at an IDLE lane -- the best case, and the one that says what the lane costs when nothing is in the
// way. It is a real number and it is not this one. Overlapping changes what p99 MEANS: it stops
// being "how long to be picked up" and becomes "how long to be picked up given that k others are
// already ahead of me", which is the head-of-line question. Reporting one number for both is how a
// bench quietly answers a question nobody asked.
//
// TWO THINGS ARE DIFFERENT AND BOTH ARE LOAD-BEARING:
//
//   1. A WINDOW OF IN-FLIGHT COMPLETIONS. kWindow are pushed back to back before anything is
//      waited on, so pushes past the K'th arrive at a reserved worker that is already running one.
//      That is the only way HiPriSpillTarget's scan is reached at all -- its fast path is a single
//      `busy` load, and on the serial row that load is always false.
//
//   2. A BODY WITH A DURATION. The serial row's body is a timestamp store, ~50 ns, so even pushed
//      back to back the owner would be free again before the next push landed and the window would
//      not overlap in practice. ~2 us is chosen to be longer than a push round trip and far shorter
//      than the 400 us bulk bodies in io_overlap_test: long enough that the lane genuinely backs up,
//      short enough that this stays a LANE measurement rather than a throughput one.
//
// STILL A SIDE THREAD, not a pool worker, for the same reason as the serial row: a completion
// pushed from inside the pool is a different code path and would be mislabelled I/O.
//
// WHAT TO READ. p50 here is a queueing number and is expected to be worse than the serial row --
// that is the window, not a regression. The row's actual subject is the pair on the last line:
// `spilled` counts completions the PRODUCER redirected to another worker in [0, K) because the
// owner was mid-body, and `staged` counts lane tasks the OWNER unloaded into its own hiPri deque on
// its way into a body. Zero on BOTH under a window this wide would mean neither reachability
// mechanism ever fired, which is the interesting failure -- it would say the lane is being drained
// by luck.
static void BenchIoPipeOverlap(JLib::TaskScheduler& sched, bool variable) {
    const size_t K = JLib::TaskScheduler::GetHotWorkers();
    if (!JLib::TaskScheduler::HiPriLaneActive()
        || JLib::TaskScheduler::GetAwakeFloorBase() == 0) {
        printf("io-pipe/ovl  : SKIPPED -- same reason as io-pipe above.\n");
        return;
    }

    constexpr int kOps = 4'000;

    // ONE SLOT PER IN-FLIGHT COMPLETION. The serial row could use a single static because exactly
    // one task existed at a time; with a window that would have every task racing to write the same
    // cell and the latencies would be nonsense. postNs is written by the producer just before its
    // own Push, startNs by the task itself, so the difference is per-completion.
    struct Slot {
        std::atomic<long long> startNs;
        long long              postNs;
        unsigned long long     iters;    // this completion's own duration -- see the mix below
    };

    auto nowNs = [] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   Clock::now().time_since_epoch()).count();
    };

    // The duration is PER TASK now, not a constant in the body. See the mix below.
    auto body = +[](void* p) {
        Slot* s = (Slot*)p;
        s->startNs.store(std::chrono::duration_cast<std::chrono::nanoseconds>(
                             Clock::now().time_since_epoch()).count(),
                         std::memory_order_release);
        unsigned long long x = 88172645463325252ull;
        const unsigned long long n = s->iters;
        for (unsigned long long i = 0; i < n; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
        g_ioOverlapSink.fetch_add(x, std::memory_order_relaxed);
    };

    // ---- WHY THE DURATION VARIES, AND WHY THE FIXED ARM IS STILL RUN ------------------------
    //
    // ROUND-ROBIN PLACEMENT IS OPTIMAL FOR EQUAL SERVICE TIMES and blind to unequal ones. With a
    // constant 2.2 us body, alternating between two reserved workers IS perfect balance, so the
    // fixed arm measures a lane whose placement cannot be improved -- and the strand counters were
    // reading 20-28% against exactly that best case.
    //
    // Real completions are not uniform. A movement packet, a state sync and a chat message are
    // different work arriving on the same socket, and a burst of them lands together: at K=2 three
    // packets go to K0, K1, K0, so the third waits on the FIRST finishing however long that takes,
    // even once K1 is free. That is the case the producer-side spill structurally cannot reach --
    // at the instant the third is pushed both workers are busy, so the search finds nobody.
    //
    // THE MIX is heavy-tailed on purpose and its mean is held near the fixed arm's 2.2 us, so the
    // two rows differ in VARIANCE and not in offered load. Anything else would confound "variance
    // hurts placement" with "more work is slower".
    //
    //     70%  x 1 us      20%  x 3 us      8%  x 8 us      2%  x 30 us     -> mean ~2.5 us
    //
    // DETERMINISTIC, not random: the duration is a hash of the completion's index, so a rerun is
    // the same workload and two configurations can be compared. A repeating table would risk
    // aligning with the window (8) or the rotation (K), which would be an artifact rather than a
    // finding; splitmix64 has no such period.
    const double itersPerUs = 2000.0 / 2.2;
    auto durationFor = [&](int i) -> unsigned long long {
        if (!variable) return 2000ull;
        unsigned long long z = (unsigned long long)i + 0x9E3779B97F4A7C15ull;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        z ^= (z >> 31);
        const unsigned bucket = (unsigned)(z % 100ull);
        const double us = (bucket < 70) ? 1.0 : (bucket < 90) ? 3.0 : (bucket < 98) ? 8.0 : 30.0;
        return (unsigned long long)(us * itersPerUs);
    };

    std::vector<double> lat;
    lat.reserve(kOps);
    bool ok = true;

    const unsigned long long spill0    = JLib::TaskScheduler::GetHiPriSpillCount();
    const unsigned long long strand0   = JLib::TaskScheduler::GetLaneStrandCount();
    const unsigned long long idlePeer0 = JLib::TaskScheduler::GetLaneStrandIdlePeerCount();
    const unsigned long long idleWide0 = JLib::TaskScheduler::GetLaneStrandIdleWideCount();

    std::thread reactor([&] {
        std::vector<Slot> slots(kIoOvlWindow);
        for (int base = 0; base < kOps; base += (int)kIoOvlWindow) {
            const size_t n = (kOps - base < (int)kIoOvlWindow) ? (size_t)(kOps - base) : kIoOvlWindow;

            // CREATED BEFORE ANY OF THEM IS TIMED. Allocation is not what this row is measuring,
            // and doing it inside the window would put a slab pop between two pushes that are
            // supposed to be back to back.
            JLib::Task* ts[kIoOvlWindow] = {};
            size_t made = 0;
            for (size_t j = 0; j < n; ++j) {
                slots[j].startNs.store(0, std::memory_order_relaxed);
                slots[j].iters = durationFor(base + (int)j);
                ts[j] = sched.CreateTask(body, &slots[j], /*hiPri*/ 1);
                if (!ts[j]) break;
                ++made;
            }
            if (made == 0) { ok = false; break; }

            JLib::WaitGroup wg;
            wg.n.store((int)made, std::memory_order_relaxed);
            for (size_t j = 0; j < made; ++j) {
                ts[j]->waitGroup = &wg;
                slots[j].postNs  = nowNs();
                sched.Push(ts[j]);
            }
            sched.WaitFor(wg);

            for (size_t j = 0; j < made; ++j) {
                const long long s = slots[j].startNs.load(std::memory_order_acquire);
                if (s > slots[j].postNs) lat.push_back((double)(s - slots[j].postNs) / 1000.0);
            }
        }
    });
    reactor.join();

    if (!ok)         { printf("io-pipe/ovl  : ERROR -- CreateTask returned null\n"); return; }
    if (lat.empty()) { printf("io-pipe/ovl  : no samples\n"); return; }

    std::sort(lat.begin(), lat.end());
    printf("io-pipe/%-4s : completion -> job start, %zu samples, %zu in flight (hiPri, K=%zu)\n",
           variable ? "ovlv" : "ovl", lat.size(), kIoOvlWindow, K);
    if (variable)
        printf("               handler duration VARIES: 70%% 1us, 20%% 3us, 8%% 8us, 2%% 30us\n"
               "               (mean held near the fixed row's 2.2us, so the two differ in\n"
               "                VARIANCE and not in offered load)\n");
    printf("               p50 %.2f us   p90 %.2f us   p99 %.2f us   max %.2f us\n",
           lat[lat.size() / 2], lat[(size_t)(lat.size() * 0.90)],
           lat[(size_t)(lat.size() * 0.99)], lat.back());

    const unsigned long long spilled = JLib::TaskScheduler::GetHiPriSpillCount() - spill0;
    printf("               lane reachability this row: spilled=%llu  of %zu pushes\n",
           spilled, (size_t)kOps);
    printf("               ^ spilled = the producer found the owner mid-body and moved the push to\n"
           "                 another idle worker in [0,K). It is now the ONLY lane rescue: the\n"
           "                 staging half is gone with the lane deque, so anything not spilled at\n"
           "                 push time waits for its owner's current body. At K=1 `spilled` is\n"
           "                 structurally 0; there is no second reserved worker.\n");

    // ---- WOULD A SHARED MPMC LANE WIN ANYTHING? THIS ROW IS WHERE IT WOULD SHOW ----------------
    const unsigned long long strands  = JLib::TaskScheduler::GetLaneStrandCount()          - strand0;
    const unsigned long long idlePeer = JLib::TaskScheduler::GetLaneStrandIdlePeerCount()  - idlePeer0;
    const unsigned long long idleWide = JLib::TaskScheduler::GetLaneStrandIdleWideCount()  - idleWide0;
    const double pctK  = strands ? (100.0 * (double)idlePeer / (double)strands) : 0.0;
    const double pctKF = strands ? (100.0 * (double)idleWide / (double)strands) : 0.0;
    printf("               MPMC prize: %llu dispatches stranded a backlog\n"
           "                 %llu (%.1f%%) had an idle worker in [0,K)    <- what the spill can reach\n"
           "                 %llu (%.1f%%) had an idle worker in [0,K+F)  <- what a SHARED lane could\n",
           strands, idlePeer, pctK, idleWide, pctKF);
    printf("               ^ a strand is a dispatch that left a non-empty lane inbox behind, i.e.\n"
           "                 work that just became unreachable -- one MPSC, one legal consumer, and\n"
           "                 that consumer is entering a body. THE GAP BETWEEN THE TWO LINES is what\n"
           "                 changing the STRUCTURE buys that improving the spill never could: the\n"
           "                 first is bounded by K by definition, the second is the consumer set a\n"
           "                 pull queue would have. Neither counts the parkable band -- a parked\n"
           "                 worker is never busy, so including it would read ~100%% forever while\n"
           "                 costing the ~3 us wake the lane exists to avoid.\n"
           "                 An MPMC removes MISALLOCATION, not queueing,\n"
           "                 so when every reserved worker is busy it waits exactly as long as this\n"
           "                 does. Near 0%% means the lane saturates cleanly, the p99 gap above is\n"
           "                 dispatch cost rather than bad placement, and a shared queue would add\n"
           "                 contention to buy nothing. Near 100%% means work sits behind busy\n"
           "                 owners while peers idle -- the one case the producer-side spill cannot\n"
           "                 reach, because it decides at PUSH time and the owner can enter a body\n"
           "                 immediately after. Biased HIGH on purpose: a sleeping peer counts as\n"
           "                 idle, so this is an upper bound on what an MPMC could win.\n");
    (void)g_ioOverlapSink.load(std::memory_order_relaxed);
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

    // Anything unrecognised, --help included, prints this and exits. It used to fall through to
    // the default and silently run the whole suite, so asking for help started a multi-minute
    // benchmark under a policy you did not choose.
    auto printUsage = []() {
            printf("usage: SchedulerBench [ideal|hard|none|physical] [poolSize] [nosweep]\n"
                   "                     [sleep|nosleep|both] [hot=N] [floor=N] [ev|noev]\n"
                   "  sleep     (default) idle workers park on a condition variable.\n"
                   "  nosleep   never park. Holds every worker core; lowest dispatch latency.\n"
                   "            Measured 4.1x on latency, 2.9x on the frame DAG, 3.1x on fork-join.\n"
                   "  both      run the whole suite once per policy, for a side-by-side paste.\n"
                   "            Re-runs this binary twice: the scheduler is a one-shot singleton,\n"
                   "            so one process cannot host both policies. Each therefore gets a\n"
                   "            genuinely cold pool rather than inheriting the other's state.\n"
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
                   "  nosweep   skip the ParallelFor crossover sweep AND the splitter-vs-cursor\n"
                   "            sweep (the two slowest sections), so a pool-size sweep is a few\n"
                   "            seconds per point instead of minutes.\n"
                   "  hot=N     reserve N workers for the LATENCY LANE (SetHotWorkers). DEFAULT 2,\n"
                   "            and PINNED: SetHotWorkers fixes the range to [N,N] so the dynamic-K\n"
                   "            controller cannot move it mid-run. That matters because the\n"
                   "            controller reacts to lane occupancy that 1p/mp/burst create as a\n"
                   "            side effect -- left to itself, K would differ between runs of the\n"
                   "            same command and the banner (sampled once at startup) would not\n"
                   "            even show it. hot=0 is also pinned: a lane-less pool, deliberately.\n"
                   "            K=0 collapses the lane: a hiPri push then routes to loPri by design,\n"
                   "            because nobody probes hiPri at K=0 and a task sent there would never\n"
                   "            run -- so latency/hot and io-pipe SKIP themselves and say why rather\n"
                   "            than printing a number that is really latency/cold twice.\n"
                   "            N is taken from the compute pool, so raising it makes the throughput\n"
                   "            rows slightly worse; latency/hot and io-pipe are what you get back,\n"
                   "            and they are the only rows that can show it. Start with hot=1.\n"
                   "  floor=N   the AWAKE FLOOR: workers 0..N-1 never park (SetAwakeFloor). DEFAULT 0,\n"
                   "            which is the historical behaviour -- the whole pool may go to sleep.\n"
                   "            THIS IS NOT hot=N, and the two were briefly the same argument, which\n"
                   "            made every paste ambiguous. hot=K picks WHICH QUEUE a hiPri push\n"
                   "            lands in; floor=N decides whether landing anywhere costs an OS wake.\n"
                   "            They are independent: floor=2 hot=0 is a live floor with no lane,\n"
                   "            hot=2 floor=0 is a lane whose workers can all be asleep.\n"
                   "            The floor only pays off if PLACEMENT steers at it, so the latency row\n"
                   "            prints where each push actually landed and how many kernel wakes the\n"
                   "            row cost. A high floor with a low on-floor percentage means the\n"
                   "            feature is decoration and the wake count will say so.\n"
                   "  noev      skip latency/hot, event/resume and io-pipe. Those are the only\n"
                   "            sections that exercise the LANE and the Event primitives, and the\n"
                   "            only reason hot=N changes anything but the compute pool size --\n"
                   "            every other section reaches the pool through a generic Push, which\n"
                   "            never routes to the lane. On by default; `ev` re-enables.\n"
                   "\n"
                   "The scheduler is a process-wide singleton and both policy and pool size are\n"
                   "fixed at Init(), so one run measures ONE configuration -- run the exe once per\n"
                   "point to compare. On macOS and Android every policy is a no-op (no usable\n"
                   "affinity API there); prefer 'none' on those so the label matches reality.\n");
    };

    // ARGV[1] IS THE AFFINITY POLICY ONLY IF IT NAMES ONE, and is optional otherwise.
    //
    // It used to be positional-and-mandatory: any first token that was not a policy printed usage
    // and exited. Since every OTHER argument is order-independent, that one positional slot was a
    // trap -- `SchedulerBench floor=2 hot=2` did not run the bench under the default policy, it
    // printed the help text, and the help text's own `[ideal|hard|none|physical]` brackets said the
    // argument was optional. Now the policy is consumed only when recognised and the option loop
    // starts wherever it left off, so every documented invocation actually runs.
    int firstOpt = 1;
    if (argc > 1) {
        if      (JLIB_STRICMP(argv[1], "ideal") == 0)    { firstOpt = 2; }  // the default, accepted explicitly
        else if (JLIB_STRICMP(argv[1], "hard") == 0)     { policy = JLib::TaskScheduler::AffinityPolicy::Hard;         policyName = "hard";     firstOpt = 2; }
        else if (JLIB_STRICMP(argv[1], "none") == 0)     { policy = JLib::TaskScheduler::AffinityPolicy::None;         policyName = "none";     firstOpt = 2; }
        else if (JLIB_STRICMP(argv[1], "physical") == 0) { policy = JLib::TaskScheduler::AffinityPolicy::PhysicalOnly; policyName = "physical"; firstOpt = 2; }
    }

    size_t poolSize = 0;                 // 0 = auto (hw-1)
    bool   runSweep = true;
    // The Event/lane sections. ON by default because they are the point of hot=N and they cost a
    // couple of seconds; `noev` exists so a CI job that only wants the pool numbers can skip them.
    bool   runEvents = true;
    // Idle policy is a trailing token so existing invocations keep working unchanged.
    auto   idle = JLib::TaskScheduler::IdlePolicy::Sleep;
    const char* idleName = "sleep";
    bool   runBoth = false;
    // hot=N: workers dedicated to the latency lane. DEFAULT 2, AND PINNED -- see the SetHotWorkers
    // call below for why a benchmark must not leave this to the controller.
    size_t hotWorkers = 2;
    size_t awakeFloor  = JLib::TaskScheduler::GetAwakeFloor();
    bool   floorGrowth = true;   // `nogrow` turns the growth controller off
    bool   neverPark   = false;  // `neverpark` makes [0,K) spin instead of sleeping -- see the flag
    size_t splitCap = 0; bool haveSplitCap = false;   // splitcap=N  // floor=N; default = the shipped one
    for (int a = firstOpt; a < argc; ++a) {
        if (JLIB_STRICMP(argv[a], "nosweep") == 0) { runSweep = false; continue; }
        if (JLIB_STRICMP(argv[a], "noev") == 0)    { runEvents = false; continue; }
        // nogrow: pin the awake floor at its base -- no push-side spill, no completion-side growth,
        // no redistribute. The A/B for the whole growth controller, in ONE binary, so the machine
        // that holds the baseline can run both arms in one session instead of across two builds.
        if (JLIB_STRICMP(argv[a], "nogrow") == 0) { floorGrowth = false; continue; }
        // norecruit -- the A/B arm for range recruitment. ON by default, because what it replaces
        //   is a single wake per published range plus discovery-by-luck. The ceiling for it is the
        //   floor=31 run, where every worker is already awake: heavy N=2000 was 7.05x at the
        //   default floor against 22.07x there, so ~3.1x is the whole prize and an arm that gets
        //   part of it should be read against that, not against the old number alone.
        // mwidth -- derive fan-out WIDTH (and the fan-out decision) from a measured first chunk
        //   instead of an iteration count. Opt-in: it changes what every ParallelFor call does.
        //   Sweep it against the trivial and heavy rows TOGETHER with minfan=0 -- the claim is that
        //   one number handles both ends, so a run that only improves one end has not shown it.
        if (JLIB_STRICMP(argv[a], "mwidth") == 0) {
            JLib::TaskScheduler::SetMeasuredWidth(true);
            continue;
        }
        // memory -- remember measured body cost per call site so the NEXT range skips the probe.
        //   OFF by default because it MEASURED WORSE here, and the reason is this harness: one
        //   lambda serves all four grain rows, so target_type() keys them all to the same slot and
        //   the average of a trivial and a heavy body describes neither. trivial N=256 went
        //   0.33x -> 0.01x. The flag exists to reproduce that, and to be useful in a program whose
        //   call sites have genuinely distinct callable types. Only meaningful with `mwidth`.
        if (JLIB_STRICMP(argv[a], "memory") == 0) {
            JLib::TaskScheduler::SetRememberedCost(true);
            continue;
        }
        if (JLIB_STRICMP(argv[a], "norecruit") == 0) {
            JLib::TaskScheduler::SetRangeRecruit(false);
            continue;
        }
        // wakecost=N -- nanoseconds, the `c` every recruitment decision is a ratio against. Default
        //   3000 (the measured OS wake here). Lower recruits sooner and wider; sweep it if the
        //   cascade over- or under-shoots, since it is the mechanism's only tuned number.
        if (JLIB_STRNICMP(argv[a], "wakecost=", 9) == 0) {
            JLib::TaskScheduler::SetWakeCostNs((unsigned)strtoul(argv[a] + 9, nullptr, 10));
            continue;
        }
        // floorhold=N -- milliseconds a grown floor is held before it may shed. Default 6. The
        //   "for how long" half of release-the-herd; sweep it against a row that produces work on a
        //   cadence, since that is the case the 6 ms constant was never sized for.
        if (JLIB_STRNICMP(argv[a], "floorhold=", 10) == 0) {
            JLib::TaskScheduler::SetFloorHoldMs((unsigned)strtoul(argv[a] + 10, nullptr, 10));
            continue;
        }
        // splitcap=N: how many unclaimed splits the lazy splitter tolerates before it stops
        // publishing and runs the rest inline. 0 = split unconditionally (the old behaviour), which
        // is the control arm for the demand cap. Default is the library.s.
        // ---- THE K + M EXPERIMENT: BOTH BANDS ADAPTIVE AT ONCE (RETIRED) ---------------------
        //
        // `hotrange=MIN,MAX` ARMED ADAPTIVE K AND NO LONGER EXISTS -- neither the flag nor the
        // controller behind it. K is STATIC: whatever `hot=N` says, for the whole run. Kept as a
        // note rather than deleted because the shape of the experiment is the part worth
        // remembering: the controller was provably inert (2.5M hiPri tasks moved K by zero), its
        // promote signal was edge-triggered so load made it fire LESS, and it could not be run
        // alongside an adaptive floor because F was defined as an offset OF K. Whoever wants both
        // bands adaptive again needs an identity-based reserved set first, not a second controller.
        //
        // The floor band is [K, K+F) recomputed from LIVE atomics at every site -- see GetFloorBase.
        // hotprio -- arm the OS thread-priority promote/demote. DEFAULT OFF (HotThreadPolicy::Normal),
        //   which makes the whole mechanism a no-op by construction rather than by a branch. Reserved
        //   workers run CRITICAL, ACTIVE floor workers HIGH, everyone else NORMAL, and an individual
        //   hiPri task raises to CRITICAL for its duration wherever it lands. Elevating the whole pool
        //   measured 5x WORSE, so this is per-band on purpose -- do not read a win here as "elevate
        //   more".
        if (JLIB_STRICMP(argv[a], "hotprio") == 0) {
            JLib::TaskScheduler::SetHotThreadPolicy(JLib::TaskScheduler::HotThreadPolicy::Elevated);
            continue;
        }
        // spinyield=N -- yield the core every N+1 idle passes. Default 7. COUPLED TO THE FLOOR CAP:
        //   raising it while the floor can grow wide reproduces the permanent-floor=31 pathology from
        //   a default config. Sweep it against the SERIAL row, not just a burst row.
        if (JLIB_STRNICMP(argv[a], "spinyield=", 10) == 0) {
            JLib::TaskScheduler::SetSpinYieldMask((unsigned)strtoul(argv[a] + 10, nullptr, 10));
            continue;
        }
        // narrowsteer -- ordinary pushes aim at the BASE floor instead of the live one (pre-5.0.0).
        //   The default is now the wide zone; this is the A/B arm, because the narrow steer was
        //   bought with real numbers on the serial row (p50 0.40 -> 0.90 us, 1p 10.0 -> 5.74 M/s
        //   when spread) and giving them back has to be visible.
        if (JLIB_STRICMP(argv[a], "narrowsteer") == 0) {
            JLib::TaskScheduler::SetPlacementFollowsGrownFloor(false);
            continue;
        }
        // park=cv | park=wait: which primitive a worker blocks on when it parks.
        //
        // wait (default) -- WaitOnAddress on Windows, FUTEX_WAIT on Linux.
        // cv             -- a per-worker condition variable; puts a mutex back on the notify path.
        //
        // RUN THIS AGAINST throughput/1p AND THE LATENCY ROW, not against bench/futex_variance.cpp.
        // The isolated ping-pong already says condvar has the tighter wake variance and it has said
        // so twice; what it cannot see is the push path, which is the only place the two arms
        // actually differ in cost. Interleave the arms -- do not run all of one then all of the other.
        if (JLIB_STRICMP(argv[a], "park=cv") == 0) {
            JLib::TaskScheduler::SetParkPrimitive(JLib::TaskScheduler::ParkPrimitive::CondVar);
            continue;
        }
        if (JLIB_STRICMP(argv[a], "park=wait") == 0) {
            JLib::TaskScheduler::SetParkPrimitive(JLib::TaskScheduler::ParkPrimitive::WaitAddress);
            continue;
        }
        // resv=N: workers reserved for hiPri only. 0 disables reservation entirely (ordinary work may
        // use every worker). The A/B for "what does a reserved core cost when no I/O is arriving".
        if (JLIB_STRNICMP(argv[a], "resv=", 5) == 0) {
            JLib::TaskScheduler::SetReservedHiPri((size_t)strtoul(argv[a] + 5, nullptr, 10));
            continue;
        }
        // neverpark -- the reserved band [0,K) SPINS instead of sleeping when its lane is empty.
        //
        // THIS USED TO BE IMPLIED BY hot=/resv= AND NO LONGER IS. SetHotWorkers(k) sets K and
        // nothing else now, because reservation ("do not run bulk work here") and spin ("never
        // sleep") are separate purchases and folding them together charged every caller the ~35%
        // ordinary-latency tax for a property most did not want. So `hot=2` alone gives a reserved
        // band that PARKS, and the banner will correctly call that K nominal.
        //
        // That makes the pair the real A/B, and it is the one the io numbers came from:
        //     hot=2              reserved, parks when idle   -- what a game wants
        //     hot=2 neverpark    reserved, always spinning   -- what the reactor wants
        // Measured on a busy pool, completion latency: p50 5.90 -> 2.00 us, p99 43.00 -> 6.30.
        //
        // `hotrange=` IS NOT PART OF THAT PAIR ANY MORE. Arming the controller now turns never-park
        // on by itself (see SetHotWorkerRange), because the clamp that gives an armed range a
        // minimum K of 1 exists precisely to guarantee a worker that is ALREADY SPINNING -- a
        // reserved band that sleeps pays a kernel wake on the first hiPri arrival after every idle
        // gap, which for bursty traffic is every burst. So this flag is about `hot=N`, the STATIC
        // case, where reservation without the spin is a real thing to want.
        // Applied HERE and not folded into hot= for the same reason the library stopped folding it.
        // DEFERRED, NOT APPLIED HERE, for the reason spelled out at the SetHotWorkers call below:
        // a setting made during the parse can be silently reverted by a setter applied after it.
        // That already happened once with hotrange=. SetHotWorkers no longer touches this flag, so
        // parse-time would work today -- and would break the day it does, silently.
        if (JLIB_STRICMP(argv[a], "neverpark") == 0) { neverPark = true; continue; }
        // leaves=N: leaves per worker the splitter mints before raising the grain itself. Default 64,
        // which never binds. leaves=1 makes the splitter mint about as many tasks as the cursor does,
        // which is the comparison that matters for a cheap body.
        // minfan=N: iterations per worker below which ParallelFor refuses to fan out. 0 disables the
        // gate entirely (pre-5.0.0 behaviour). Protects cheap bodies at small N and costs expensive
        // ones at the same N -- sweep it against the trivial and heavy rows together.
        if (JLIB_STRNICMP(argv[a], "minfan=", 7) == 0) {
            JLib::TaskScheduler::SetMinItersPerWorker((size_t)strtoul(argv[a] + 7, nullptr, 10));
            continue;
        }
        if (JLIB_STRNICMP(argv[a], "leaves=", 7) == 0) {
            JLib::TaskScheduler::SetLeavesPerWorker((size_t)strtoul(argv[a] + 7, nullptr, 10));
            continue;
        }
        if (JLIB_STRNICMP(argv[a], "splitcap=", 9) == 0) {
            splitCap = (size_t)strtoul(argv[a] + 9, nullptr, 10);
            haveSplitCap = true;
            continue;
        }
        if (JLIB_STRICMP(argv[a], "ev") == 0)      { runEvents = true;  continue; }
        if (JLIB_STRICMP(argv[a], "sleep") == 0) { continue; }
        if (JLIB_STRICMP(argv[a], "nosleep") == 0)      { idle = JLib::TaskScheduler::IdlePolicy::NoSleep;       idleName = "nosleep";   continue; }
        if (JLIB_STRICMP(argv[a], "both") == 0) { runBoth = true; continue; }
        // hot=N: dedicate N workers to the low-latency lane. EXISTS TO PRICE THEM.
        //
        // A hot worker is removed from general placement, so worker-bound work loses K/N of the
        // pool -- 3.2% per worker on a 32-thread box. That cost is arithmetic, but it is invisible
        // in a DISPATCH benchmark: at ~60ns/task the producer is the bottleneck and the workers are
        // not saturated, so removing one changes nothing. The `heavy` ParallelFor row is genuinely
        // worker-bound and is where the ratio should actually show up.
        if (JLIB_STRNICMP(argv[a], "hot=", 4) == 0) {
            hotWorkers = (size_t)strtoul(argv[a] + 4, nullptr, 10);
            continue;
        }
        // floor=N -- THE AWAKE FLOOR, AND IT IS NOT hot=N. Two different knobs, briefly wired to the
        // same argument, which made every paste ambiguous:
        //   hot=N   -> K, the latency LANE. Changes WHICH QUEUE a hiPri push routes to.
        //   floor=N -> N workers that never park. Changes whether a push has to buy an OS wake.
        // A pool can have a live floor and no lane (floor=2 hot=0), or a lane whose workers all
        // park (hot=2 floor=0). Tying them means a row that moved cannot be attributed to either.
        if (JLIB_STRNICMP(argv[a], "floor=", 6) == 0) {
            awakeFloor = (size_t)strtoul(argv[a] + 6, nullptr, 10);
            continue;
        }
        // ---- WHAT COUNTS AS A STALL, in microseconds ------------------------------------------
        //
        // 50 is the default because that is the size of the outlier being chased, but it is not a
        // property of anything -- and a fixed trigger cannot be aimed. A row whose max lands just
        // UNDER it (43.5 us on the run that motivated this flag) reports nothing at all, so the
        // diagnostic is silent exactly when it nearly had something to say.
        //
        // Lower it to catch smaller stalls, and to exercise the report on a healthy machine.
        if (JLIB_STRNICMP(argv[a], "stall=", 6) == 0) {
            // VALIDATED, because the obvious use of this flag is to set it high enough to silence
            // the report -- and strtod answers a value too large for a double with HUGE_VAL, and a
            // non-numeric token with 0. Neither is rejected by anything downstream: 0 makes every
            // one of 20,000 iterations a "stall", and inf silently disables the tally while the row
            // still prints a threshold, so the output looks like a clean run rather than a disabled
            // check. Both are answers to a question the user did not ask.
            const double v = strtod(argv[a] + 6, nullptr);
            if (!(v > 0.0) || v > 1.0e9) {
                printf("bad stall= value '%s' (want microseconds, 0 < N <= 1e9); keeping %.0f\n",
                       argv[a] + 6, g_stallUs);
            } else {
                g_stallUs = v;
            }
            continue;
        }
        // A BARE NUMBER IS THE POOL SIZE; ANYTHING ELSE IS A TYPO. This used to be an unguarded
        // strtoul, which returns 0 for a non-numeric token -- so a misspelled flag silently
        // selected the auto pool size and ran the full suite under a configuration nobody asked
        // for. A mistyped knob has to be louder than a working one, not quieter.
        if (argv[a][0] >= '0' && argv[a][0] <= '9') {
            poolSize = (size_t)strtoul(argv[a], nullptr, 10);
            continue;
        }
        printUsage();
        return (JLIB_STRICMP(argv[a], "--help") == 0 || JLIB_STRICMP(argv[a], "-h") == 0) ? 0 : 2;
    }

    // "both" runs the suite once per idle policy so the two are directly comparable in one paste.
    //
    // It RE-RUNS THIS BINARY rather than looping in-process, and that is forced rather than lazy:
    // TaskScheduler::Init throws if an instance already exists and nothing ever clears it, so a
    // process hosts exactly one scheduler for its lifetime. Making that resettable to serve a
    // benchmark convenience would mean tearing down worker threads, the fiber pool and the epoch
    // manager's retired lists and trusting all of it to come back clean -- a real lifecycle change
    // with real risk, for a dev tool. Two processes cost a few seconds and cannot be subtly wrong.
    //
    // A side benefit worth having: each policy gets a genuinely cold pool, so neither inherits the
    // other's cache or clock state.
    if (runBoth) {
        std::string self = argv[0];
        int rc = 0;
        for (const char* pol : { "sleep", "nosleep" }) {
            std::string cmd = "\"" + self + "\"";
            for (int a = 1; a < argc; ++a) {
                if (JLIB_STRICMP(argv[a], "both") == 0) continue;   // don't recurse
                cmd += " "; cmd += argv[a];
            }
            cmd += " "; cmd += pol;
#if defined(_WIN32)
            // cmd.exe strips the outermost quote pair, so a path containing spaces needs the WHOLE
            // command wrapped again or it splits at the first space and runs the wrong thing.
            cmd = "\"" + cmd + "\"";
#endif
            printf("\n########## idle policy: %s ##########\n", pol);
            fflush(stdout);
            const int r = std::system(cmd.c_str());
            if (r != 0) rc = r;
        }
        return rc;
    }

    JLib::TaskScheduler::SetAffinityPolicy(policy);
    JLib::TaskScheduler::SetIdlePolicy(idle);

    // The version is stamped here on purpose. Results get pasted into issues and threads, and the
    // suite changes: the ParallelFor case was split in two, the default affinity policy moved from
    // hard to ideal, and the crossover sweep stopped reporting noise as a win. Third-party numbers
    // from before those changes had to be thrown away because nothing in the output identified the
    // build. Anything pasted from here says which scheduler produced it.
#ifndef JLIBSCHED_VERSION
#define JLIBSCHED_VERSION "unknown"   // hand-built outside CMake
#endif
    printf("JLib::Scheduler %s bench  (sizeof(Task)=%zu, hw threads=%u, affinity=%s, idle=%s, pool=%s, spin=%s)\n",
        JLIBSCHED_VERSION,
        sizeof(JLib::Task), std::thread::hardware_concurrency(), policyName, idleName,
        poolSize ? std::to_string(poolSize).c_str() : "auto",
        JLib::platform::SpinHintName());
    printf("----------------------------------------------------------------\n");

    // 180s per section. Generous on purpose: the crossover sweep is legitimately the slowest part
    // and CI runners are shared VMs, so a false positive would be worse than what it catches. Even
    // so it reports in three minutes instead of the thirty a job timeout takes.
    StartSectionWatchdog(180);

    // BEFORE Init: StartPool clamps K against the real pool size and publishes the hot CPU set as
    // the workers come up, so the count has to be known by then.
    //
    // ALWAYS CALLED, INCLUDING AT ZERO, AND THAT IS THE POINT. SetHotWorkers(k) pins the range to
    // [k,k], which the dynamic-K controller cannot move. Skipping the call -- which is what
    // `if (hotWorkers)` used to do at the default -- leaves K to the controller, and the controller
    // reacts to lane occupancy that 1p, mp and burst create as a side effect. K would then differ
    // between runs of the same command, and every row would be describing a configuration the
    // reader cannot see, because the banner samples GetHotWorkers() once at startup and a promotion
    // after that never appears in the paste.
    //
    // That is exactly the throughput/bt lesson -- a bench that quietly reports two configurations
    // under one heading -- and it is worse here, because K changes what the OTHER rows mean, not
    // just its own. A benchmark measures one configuration and says which one.
    //
    // DEFAULT 2 rather than 0: at K=0 the lane is collapsed, latency/hot and io-pipe skip
    // themselves, and the suite silently stops covering the thing those sections exist for. Pass
    // hot=0 deliberately to measure a lane-less pool -- that is still PINNED at zero, not dynamic.
    // ...AND `hotrange=` MUST WIN, because this call is what un-arms it. SetHotWorkers pins the
    // range to [k,k], so applying the default hot=2 here after the parser had already called
    // SetHotWorkerRange silently reverted adaptive K to pinned -- the run looked identical to a
    // plain one and the banner said "pinned" next to a command line that asked for a range. The
    // range is therefore applied HERE, in the same place and after the same parse, rather than
    // inside the argument handler where ordering decides the outcome.
    // hotrange= is gone with the controller; K is whatever hot=N says.
    JLib::TaskScheduler::SetHotWorkers(hotWorkers);

    // AFTER the K setters, deliberately. Neither of them touches this flag any more, but this is
    // the ordering that stays correct if one ever does again -- the same lesson hotrange= taught.
    if (neverPark) JLib::TaskScheduler::SetReservedNeverParks(true);

    // THE AWAKE FLOOR, applied separately from K on purpose (see the floor= argument). Workers
    // 0..N-1 never park, so a push steered at one of them is picked up by a thread that is already
    // scheduled -- no WaitOnAddress, no kernel round trip. The floor buys nothing unless placement
    // actually steers there, which is what the latency row's landing histogram exists to check.
    JLib::TaskScheduler::SetAwakeFloor(awakeFloor);
    JLib::TaskScheduler::SetFloorGrowthEnabled(floorGrowth);
    if (haveSplitCap) JLib::TaskScheduler::SetLazySplitCap(splitCap);

    // PRESIZE THE SLAB, because a growth allocation is MEASUREMENT NOISE HERE and nowhere else.
    //
    // The library default is slots64 = 24K, and throughput/1p pushes 200,000 capture-free no-op
    // tasks from one producer -- which outruns the drain, exceeds that class, and grows mid-run.
    // The result is an allocation inside a timed section plus a warning line landing in the middle
    // of the output. Growth is the right DEFAULT behaviour (a game should stutter once, not die),
    // but a benchmark exists to describe the scheduler, and an allocator hitch inside the number is
    // the scheduler being blamed for the allocator.
    //
    // NOT A SUGGESTION FOR APPLICATIONS. The shipped defaults target the 80% who never call this
    // and are sized against PEAK LIVE, not against total submitted -- the number that matters is
    // how many tasks exist at once, which for a real frame loop is small. These are bench numbers:
    // deliberately far above anything an app should reserve, because this process is allowed to
    // spend 30 MB to keep a measurement clean and an app is not.
    JLib::TaskScheduler::SlabSizes slab;
    slab.slots64  = 256 * 1024;   // 16 MB -- throughput/1p's burst, TaskNodes, PushArray chunks
    slab.slots80  =  64 * 1024;   // 5 MB  -- capturing lambdas across the sweeps
    slab.slots128 =   8 * 1024;   // 1 MB  -- larger coroutine frames
    slab.slots256 =  32 * 1024;   // 8 MB  -- DAG edge chunks; the frame DAG runs 20,000 graphs
    JLib::TaskScheduler::SetSlabSizes(slab);

    JLib::TaskScheduler::Init(poolSize);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    // Say what was actually configured -- a run pasted without this is unattributable, and K
    // changes what the numbers mean.
    // SAY WHICH API THE LATENCY ROWS USED, not just what K is. A paste showing hot=2 next to a
    // latency number taken through a generic Push is how a false regression hunt starts: the lane
    // was never on the path, so K only shrank the compute pool and the row looks worse for free.
    // The plain `latency` row below is ALWAYS a generic push; latency/hot is the hiPri one.
    const size_t bannerK = JLib::TaskScheduler::GetHotWorkers();
    // "PINNED" is not decoration: it says the dynamic-K controller cannot move this mid-run, so the
    // number in the banner is the number every row below was measured under.
    printf("config: workers=%zu  affinity=%s  idle=%s  events=%s  park=%s\n",
           sched.GetWorkerCount(), policyName, idleName, runEvents ? "on" : "off",
           JLib::TaskScheduler::GetParkPrimitive() == JLib::TaskScheduler::ParkPrimitive::CondVar
               ? "cv" : "wait");
    // NO RESET HERE. It used to zero the park counters so the verdict "covered the measured run"
    // -- and it hid the fact that reserved workers park ONCE at startup and then sleep through
    // everything, reporting them as never parking at all. Lifetime totals are the honest number:
    // a floor worker parking is junk whenever it happened, and a reserved worker parking early and
    // staying asleep is the behaviour you want to be able to SEE.
    // ---- INTENT AT THE TOP, OBSERVED BAND AT THE BOTTOM --------------------------------------
    //
    // NO LIVE BAND HERE. Printing `reserved [0,2) floor [2,4)` before anything has run states a
    // layout as fact when all that exists is a request -- and the run then goes on to show that
    // [0,K) parks and F does not stay where it was put. A banner that announces a layout the pool
    // does not have is how the wrong bug gets debugged. What is true at this point is only what was
    // ASKED FOR, so that is all this line claims.
    printf("requested: hot=%zu  floorBase=%zu\n",
           hotWorkers, JLib::TaskScheduler::GetAwakeFloorBase());
    // SAY WHEN THE KNOB IS INERT. GetHotWorkers() returns 0 unconditionally -- the RESERVED LANE is
    // deleted, and the early return is the "provably unreachable" step. `hot=N` is accepted and does
    // nothing, and a banner reading `hot=0` next to a command line reading `hot=5` looks like a parse
    // failure rather than a stubbed feature. An inert knob has to announce itself.
    //
    // WHAT K NO LONGER GATES, since 5.0.0: hiPri itself. A hiPri push routes to the hiPri INBOX of an
    // awake-floor worker and every worker drains hiPri before its own deque, so latency/hot and
    // io-pipe run WITHOUT K. What K would still add is RESERVATION -- keeping ordinary work off those
    // workers so a completion finds an IDLE core rather than merely an awake one.
    // STALE TEXT REMOVED. This used to say "the RESERVED LANE is deleted -- K stays 0", which was
    // true while GetHotWorkers() returned 0 unconditionally and is not now. It also fired whenever
    // `hotrange=` was used, because that leaves the `hot=` default untouched -- so the banner said
    // ADAPTIVE K=1 on one line and "K stays 0" on the next. A banner that contradicts itself is
    // worse than one that says nothing.
    // ITS OWN LINE. K and the floor answer different questions, and a row that moved has to be
    // attributable to one of them; printing them together invites reading a lane change as a
    // parking change. floor=0 means the whole pool may park -- the historical behaviour.
    printf("        awake-floor=%zu  -- workers that never park (floor=N). It is what makes a\n"
           "                        hiPri push cheap: the push is steered at a floor worker, which is\n"
           "                        already running, so it costs no OS wake. hot=N adds RESERVATION\n"
           "                        on top -- keeping ordinary work off those workers -- and is LIVE\n"
           "                        again (K, [0,K)). Reservation does NOT imply never-park: add\n"
           "                        `neverpark`, or arm a range, for that.\n",
           JLib::TaskScheduler::GetAwakeFloor());
    printf("        latency row = generic Push (ordinary placement);  latency/hot = hiPri push%s\n",
           JLib::TaskScheduler::GetAwakeFloorBase() ? "" :
               "  [floor=0 and K=0: hiPri has no steer target, so hot rows skip]");

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

    // THE SECTIONS THAT MAKE hot= MEAN SOMETHING, and they sit HERE for a reason: after everything
    // that wants a warm pool, before burst. Burst deliberately parks the pool for 50 ms per run, so
    // anything measuring a hot lane after it would be timing an OS wake and calling it lane latency
    // -- the same contamination that once read the frame DAG at 30.88 us against its true 22-24.
    if (runEvents) { Section("latency/hot");  BenchLatencyHotCold(sched); }
    if (runEvents) { Section("event/resume"); BenchEventResume(sched, /*sleeping arm*/ true); }
    if (runEvents) { Section("io-pipe");      BenchIoPipe(sched);
                                             BenchIoPipeOverlap(sched, /*variable*/ false);
                                             BenchIoPipeOverlap(sched, /*variable*/ true); }
    // LAST, and deliberately. This one sleeps 50 ms before each of its five runs so the pool is
    // genuinely parked, which is the whole point of it. Run earlier, it leaves every worker cold and
    // the next section pays the wake-up: with it sitting before the frame DAG, that section read
    // 30.88 us/graph against 22.7 to 24.7 measured on its own. Anything that deliberately idles the
    // pool has to go after everything it would otherwise contaminate.
    // The "lane pressure" row is gone with adaptive K: it existed to ask whether the
    // controller ramps under sustained hiPri load, and there is no controller.
    Section("burst");          BenchIdleBurst(sched, JLib::CorePref::Default);
    Section("burst");          BenchIdleBurst(sched, JLib::CorePref::Wide);
    if (runSweep) { Section("ParallelFor crossover sweep"); BenchParallelForCrossover(sched); }
    if (runSweep) { Section("splitter vs cursor sweep");    BenchSplitterVsCursorCrossover(sched); }
    if (runSweep) { Section("splitpref");                   BenchSplitPref(sched); }
    if (runSweep) { Section("requeue vs pushbatch");        BenchRequeueVsPushBatch(sched); }

    g_benchDone.store(true, std::memory_order_release);
    printf("----------------------------------------------------------------\n");
    // VERIFIED HERE, NOT AT THE TOP. At the top nothing has parked yet, so the check would pass
    // vacuously -- which is the failure mode it exists to prevent. This is the run's verdict on
    // whether the layout it announced is the layout it actually ran.
    PrintBandVerdict();
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
    // TaskType::Fiber is the load-bearing argument: these tasks call WaitFor below, which SUSPENDS,
    // and only a fiber-backed task can suspend. It also makes this the only section of the bench
    // that exercises a fiber at all -- every other one uses the fn-pointer overload, which
    // defaults to TaskType::Native.
    JLib::Task* left = sched.CreateTask([&sched, start, mid, BASE_CASE] {
        RecursiveForkJoinImpl(sched, start, mid, BASE_CASE);
    }, false, JLib::TaskType::Fiber);
    JLib::Task* right = sched.CreateTask([&sched, mid, end, BASE_CASE] {
        RecursiveForkJoinImpl(sched, mid, end, BASE_CASE);
    }, false, JLib::TaskType::Fiber);

    if (!left || !right) {
        printf("ERROR: CreateTask failed\n");
        return;
    }

    JLib::WaitGroup wg;
    left->waitGroup = &wg;
    right->waitGroup = &wg;
    wg.n.fetch_add(2, std::memory_order_relaxed);

    // Push. This used PushFork until 1.3.4, which put both children on the CALLING worker and left
    // stealing as the only way a 128-leaf tree could spread: 0.32 ms against 0.25 ms here, and up
    // to 4.6x slower on smaller trees. That measurement is what retired PushFork.
    if (!sched.Push(left)) {
        fj_failed.fetch_add(1);
        printf("ERROR: Push(left) failed\n");
    } else {
        fj_pushed.fetch_add(1);
    }

    if (!sched.Push(right)) {
        fj_failed.fetch_add(1);
        printf("ERROR: Push(right) failed\n");
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
        }, false, JLib::TaskType::Fiber);  // suspends, so it needs a fiber

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
        sched.Push(task);  // load-balanced; submitted from the main thread
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
