// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// JLib::Scheduler 5.0 -- the fibers-only runtime, measured fresh.
//
// bench.cpp still exists and still works, but it grew around a library that had two idle policies,
// three task types and an optional C++20 layer. None of those are true now, and a benchmark whose
// arms describe a product you no longer ship is worse than no benchmark: it invites comparisons
// between configurations that cannot both exist. This one measures what 5.0 actually is.
//
// ============================ HOW TO READ THIS, AND HOW NOT TO ============================
//
// EVERY ARM IS INTERLEAVED AND ITS ORDER ROTATES PER REP. Running arm A ten times and then arm B ten
// times measures the machine's thermal ramp as much as the code -- a monotonic drift lands entirely
// on whichever arm went second and reads as a clean result. This file alternates, and rotates which
// arm leads. That is not caution: a fixed order once moved a control configuration by 2x here.
//
// MEDIANS, WITH THE RANGE PRINTED. The last digit is not meaningful on several of these rows. Read
// the order of magnitude and the ratio between arms, never the number alone. Where an arm's range
// overlaps its neighbour's, the row says INDISTINGUISHABLE rather than letting you infer a winner
// from two medians a nanosecond apart.
//
// THE CONFIG IS STAMPED ON EVERY SECTION. Workers, K, floor and fiber mode all change what these
// rows mean, and a pasted result with no configuration attached has cost this project real time.
//
// A QUIET MACHINE MEANS QUIET. Two rounds of numbers were once thrown away because a VM that had
// been shut down seconds earlier was still tearing down in the background. Give the machine real
// idle time first, and take any result you care about twice.
//
// WHAT THIS DELIBERATELY DOES NOT DO: IT DOES NOT COMPARE AGAINST OTHER LIBRARIES, and that is a
// position rather than an omission. A head-to-head implies a shared objective function, and
// schedulers are not built for the same things -- this one trades throughput for call-graph
// transparency and a bounded I/O tail, and a fork-join benchmark will say so as if it were a defect.
// Publishing the table invites precisely the comparison the design does not make.
//
// The bench/compare/ tools still exist for sanity-checking a change against a known implementation,
// which is a different use: a private control, not a claim. Their isolation rules matter for the
// same reason -- two schedulers in one process measure each other's spinning, not their own work.
//
// EVERY ARM HERE COMPARES THIS RUNTIME AGAINST ITSELF: lane against floor, batch against single,
// parallel against its own serial baseline, floor against parked. Those are comparisons a reader can
// act on, because both sides are configurations they actually have.

#include "TaskScheduler.h"
#include "../tests/fiber_body.h"
#include "Thread.h"
#include "platform.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------------------------
// Sample collection. Everything here reports a median and a range; nothing reports a mean, because
// one descheduled sample moves a mean and cannot move a median.
// ---------------------------------------------------------------------------------------------
struct Samples {
    std::vector<double> v;
    void add(double d) { v.push_back(d); }
    double median() { std::sort(v.begin(), v.end()); return v.empty() ? 0.0 : v[v.size() / 2]; }
    double pct(double p) {
        if (v.empty()) return 0.0;
        std::sort(v.begin(), v.end());
        size_t i = (size_t)(p * (double)(v.size() - 1) + 0.5);
        return v[i];
    }
    double lo() { std::sort(v.begin(), v.end()); return v.empty() ? 0.0 : v.front(); }
    double hi() { std::sort(v.begin(), v.end()); return v.empty() ? 0.0 : v.back(); }
};

// The park-on-a-gate fiber body: a named function plus an explicit context. Not a lambda -- a
// fiber's stack is a place, while a closure is a value the worker loop frees when the body returns,
// so a fiber given one that then parks resumes into a dead frame or never dies.
struct GateCtx { JLib::Event* gate; std::atomic<int>* parked; std::atomic<int>* done; };
static void GateWaitBody(void* p) {
    auto& c = *static_cast<GateCtx*>(p);
    c.parked->fetch_add(1, std::memory_order_release);
    JLib::TaskScheduler::Instance().WaitOnEvent(*c.gate);
    c.done->fetch_add(1, std::memory_order_release);
}

static double UsSince(Clock::time_point t0) {
    return std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
}

// Two arms OVERLAP when neither median sits outside the other's observed range. Printed rather than
// hidden, because "A beat B by 3%" across overlapping ranges is the single most common way a
// benchmark lies.
static bool Overlaps(Samples& a, Samples& b) {
    return !(a.median() > b.hi() || b.median() > a.hi());
}

static void Row(const char* name, Samples& s, const char* unit) {
    std::printf("  %-34s %9.2f %-3s   [%.2f - %.2f]\n", name, s.median(), unit, s.lo(), s.hi());
}

static void Section(const char* title) {
    std::printf("\n%s\n", title);
    for (const char* p = title; *p; ++p) std::printf("-");
    std::printf("\n");
}

// ---------------------------------------------------------------------------------------------
// 1. DISPATCH LATENCY -- push to first instruction of the task.
//
// THE SINGLE MOST IMPORTANT NUMBER IN THIS FILE, and the one the reserved lane exists to move. It
// is measured as a ROUND TRIP from the pushing thread, because that is what an application can
// actually observe: a one-sided timestamp taken inside the task measures the clock's agreement
// between two cores as much as the scheduler.
//
// BOTH LANES, INTERLEAVED. Lane::Normal goes to the floor; Lane::LowLatency goes to the reserved
// band if K > 0. Running them alternately in one process is the only way the comparison is honest --
// separate runs would compare two thermal states.
// ---------------------------------------------------------------------------------------------
static void BenchDispatch(JLib::TaskScheduler& sched, int reps) {
    Section("1. DISPATCH LATENCY -- push -> task runs (round trip, from the pusher)");

    const size_t K = JLib::TaskScheduler::GetHotWorkers();
    Samples normal, lane;

    auto one = [&](JLib::Lane ln, Samples& out) {
        std::atomic<bool> ran{ false };
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        const auto t0 = Clock::now();
        JLib::Task* t = sched.CreateTask([&ran] { ran.store(true, std::memory_order_release); },
                                         ln);
        if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); return; }
        t->waitGroup = &wg;
        sched.Push(t);
        sched.WaitFor(wg);
        out.add(UsSince(t0));
    };

    // WARM-UP IS NOT OPTIONAL and is discarded. The first push into a cold pool wakes workers from
    // a park, which is a kernel round trip that has nothing to do with steady-state dispatch.
    for (int i = 0; i < 200; ++i) { Samples junk; one(JLib::Lane::Normal, junk); one(JLib::Lane::LowLatency, junk); }

    // ---- SAMPLE COUNT IS DECOUPLED FROM `reps` HERE, AND ONLY HERE. --------------------------
    //
    // A TAIL NEEDS SAMPLES. Every other section aggregates internally -- 20k tasks, 64 fibers -- so
    // fifteen of them is fifteen real measurements. This one times ONE dispatch per sample, so
    // fifteen samples is fifteen numbers, and pct(0.99) over fifteen resolves to index 14: it IS
    // the maximum, wearing a percentile's name. The first run of this file duly reported a "p99" of
    // 53.70 us from a single descheduling hiccup on a path the same run had just proved was
    // identical to its neighbour.
    //
    // So the latency arms take thousands of samples regardless of `reps`, and the percentile label
    // is withheld below unless there are enough to mean anything.
    const int kLatSamples = std::max(2000, reps * 200);
    std::printf("  %d samples per arm: ", kLatSamples);
    const int dot = std::max(1, kLatSamples / 15);
    for (int r = 0; r < kLatSamples; ++r) {
        if (r & 1) { one(JLib::Lane::Normal, normal);  one(JLib::Lane::LowLatency, lane); }
        else       { one(JLib::Lane::LowLatency, lane); one(JLib::Lane::Normal, normal); }
        if ((r % dot) == 0) { std::printf("."); std::fflush(stdout); }
    }
    std::printf("\n");

    std::printf("  (K=%zu reserved worker(s); with K=0 the two arms are the same code path)\n\n", K);
    Row("Lane::Normal   (floor)", normal, "us");
    Row("Lane::LowLatency (K)", lane, "us");
    // p99 IS ONLY PRINTED WHEN IT IS ONE. Below ~100 samples the 99th percentile and the maximum
    // are the same element, and calling the maximum a percentile invites a reader to treat one
    // descheduling event as a property of the scheduler.
    if (normal.v.size() >= 100) {
        std::printf("  %-34s %9.2f us\n", "Lane::Normal   p99", normal.pct(0.99));
        std::printf("  %-34s %9.2f us\n", "Lane::LowLatency p99", lane.pct(0.99));
        std::printf("  %-34s %9.2f us   <- one sample; environment, not the lane\n",
                    "worst single dispatch seen", std::max(normal.hi(), lane.hi()));
    } else {
        std::printf("  (too few samples for a percentile -- the range above is all this run supports)\n");
    }

    if (K == 0)
        std::printf("\n  NOTE: K=0, so LowLatency has no reserved band to go to and both arms are the\n"
                    "        SAME PATH. The rows above are an A/A control -- they should match, and a\n"
                    "        difference here is measurement noise, not a lane.\n");
    else if (Overlaps(normal, lane))
        std::printf("\n  INDISTINGUISHABLE: the ranges overlap, so this run does not show the lane\n"
                    "        beating the floor. That is a real outcome on a quiet machine -- the lane\n"
                    "        buys a bounded TAIL, and a p50 comparison is not where it shows.\n");
}

// ---------------------------------------------------------------------------------------------
// 2. THROUGHPUT -- how fast one producer can hand work over.
//
// SUBMISSION, NOT EXECUTION. The task body is empty on purpose: this measures the cost of the push
// path and the drain, and anything in the body would measure the body. An empty task is not
// unrealistic here -- it is the isolation that makes the number mean "overhead".
//
// IN-FLIGHT CAP. Pushing a million tasks as fast as possible measures the ALLOCATOR hitting its
// ceiling, not the scheduler; an earlier version of this row queued 118k deep and the teardown dump
// was the only interesting output. The cap keeps the pool in a steady state.
// ---------------------------------------------------------------------------------------------
static void BenchThroughput(JLib::TaskScheduler& sched, int reps) {
    Section("2. THROUGHPUT -- single producer, empty tasks (submission overhead)");

    constexpr int kPerRep = 20000;
    Samples single, batched;

    auto runSingle = [&] {
        JLib::WaitGroup wg;
        wg.n.store(kPerRep, std::memory_order_relaxed);
        const auto t0 = Clock::now();
        for (int i = 0; i < kPerRep; ++i) {
            JLib::Task* t = sched.CreateTask([] {}, JLib::Lane::Normal);
            if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        single.add((double)kPerRep / UsSince(t0));   // tasks per microsecond
    };

    auto runBatch = [&] {
        constexpr size_t kChunk = 256;
        JLib::WaitGroup wg;
        wg.n.store(kPerRep, std::memory_order_relaxed);
        std::vector<JLib::Task*> buf;
        buf.reserve(kChunk);
        const auto t0 = Clock::now();
        int made = 0;
        while (made < kPerRep) {
            buf.clear();
            const int want = std::min<int>(kChunk, kPerRep - made);
            for (int i = 0; i < want; ++i) {
                JLib::Task* t = sched.CreateTask([] {}, JLib::Lane::Normal);
                if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
                t->waitGroup = &wg;
                buf.push_back(t);
            }
            made += want;
            if (!buf.empty()) sched.PushBatch(buf.data(), buf.size());
        }
        sched.WaitFor(wg);
        batched.add((double)kPerRep / UsSince(t0));
    };

    runSingle(); runBatch();   // warm-up, discarded
    single.v.clear(); batched.v.clear();

    std::printf("  %d rounds x 20k tasks: ", reps);
    for (int r = 0; r < reps; ++r) {
        if (r & 1) { runSingle(); runBatch(); }
        else       { runBatch();  runSingle(); }
        std::printf("."); std::fflush(stdout);
    }
    std::printf("\n\n");

    Row("Push, one at a time", single, "M/s");
    Row("PushBatch (chunk 256)", batched, "M/s");
    if (Overlaps(single, batched))
        std::printf("\n  INDISTINGUISHABLE this run -- batching wins where the per-push notify is the\n"
                    "        cost, so a pool that is already awake narrows the gap.\n");
}

// ---------------------------------------------------------------------------------------------
// 3. FIBER SUSPEND / RESUME -- the defining cost of a fibers-only runtime.
//
// THIS IS THE ROW 5.0 LIVES OR DIES ON. Every wait in the library -- an Event, a WaitGroup, an I/O
// completion -- is a fiber suspending and later resuming somewhere else, and the coroutine layer
// that used to offer an alternative is gone. If this number is bad, nothing else here matters.
//
// MEASURED AS A FULL ROUND TRIP: park on an event, be signalled, run again. That includes the
// context switch BOTH ways plus whatever it costs to get the resumption scheduled -- which since
// 5.0 goes through PushTarget like any other placement. A bare ContextSwitch microbenchmark lives
// in bench/context_switch.cpp and answers a different, smaller question.
// ---------------------------------------------------------------------------------------------
static void BenchFiberSuspend(JLib::TaskScheduler& sched, int reps) {
    Section("3. FIBER SUSPEND / RESUME -- park on an event, be woken, run again");

    constexpr int kFibers = 64;
    Samples s;
    bool stalled = false;

    // NEVER yield() IN A WAIT LOOP HERE, and this is measured rather than stylistic. On Windows
    // std::this_thread::yield() is SwitchToThread(), which hands the core to another READY thread --
    // and with a pool of spinning workers there are always plenty, so this thread goes to the back
    // of the run queue waiting for something that has usually already happened. That is the 664us
    // round trip this project already diagnosed once. A short sleep gives the core back to the OS
    // instead of volunteering it to the pool.
    auto waitUntil = [](auto pred, int budgetMs) {
        const auto dl = Clock::now() + std::chrono::milliseconds(budgetMs);
        while (!pred() && Clock::now() < dl)
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        return pred();
    };

    auto round = [&](bool record) -> bool {
        JLib::Event& gate = sched.GetEvent("rtbench_gate");
        std::atomic<int> parked{ 0 }, done{ 0 };
        JLib::WaitGroup wg;
        int made = 0;
        wg.n.store(kFibers, std::memory_order_relaxed);

        // One context for all kFibers -- nothing varies per fiber -- declared outside the loop so
        // it outlives every task pointing at it.
        GateCtx gctx{ &gate, &parked, &done };
        for (int i = 0; i < kFibers; ++i) {
            JLib::Task* t = JLibTest::MakeCtxTask(sched, &GateWaitBody, &gctx);
            if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
            t->waitGroup = &wg;
            sched.Push(t);
            ++made;
        }

        // EVERYONE PARKS BEFORE ANYONE IS RELEASED. Timing from the signal is what isolates the
        // resume: if fibers were still arriving, this would time the spawn as well.
        //
        // BOUNDED, BECAUSE A BENCHMARK MUST NOT ANSWER FAILURE WITH SILENCE. The first version of
        // this section spun here forever, so a fiber that never parked was indistinguishable from a
        // section that was merely slow -- and "is it hung or is it working?" is not a question a
        // benchmark should make its reader ask.
        if (!waitUntil([&] { return parked.load(std::memory_order_acquire) >= made; }, 10000)) {
            std::printf("  STALLED: only %d of %d fibers parked. The pool could not supply fibers --\n"
                        "           see SetFiberBudget (normalPerComputeWorker).\n",
                        parked.load(), made);
            return false;
        }

        // ---- THE TIMED WINDOW SPINS. IT MUST NOT SLEEP, AND THAT IS MEASURED. --------------
        //
        // The first version of this polled with sleep_for(200us) and reported 250 us per resume
        // against 0.78 us for the same code timed properly -- a 320x error, entirely instrument.
        // std::this_thread::sleep_for honours the SYSTEM TIMER GRANULARITY on Windows, which is
        // ~1-15 ms unless someone has called timeBeginPeriod, so a "200 microsecond" poll is
        // nothing of the kind and the sleep dominates the thing being measured.
        //
        // Nor may it yield(): that is SwitchToThread, which hands the core to the pool and puts
        // this thread behind every spinning worker (the 664 us round trip this project already
        // diagnosed). So the timed wait is a tight spin with a deadline -- accurate at this
        // duration, ~50 us of one core, and still incapable of hanging.
        const auto t0 = Clock::now();
        gate.SignalAll();

        const auto hardDeadline = t0 + std::chrono::seconds(10);
        while (done.load(std::memory_order_acquire) < made) {
            JLib::platform::CpuRelax();
            if (Clock::now() > hardDeadline) {
                std::printf("  STALLED: %d of %d fibers resumed after SignalAll. This is a LOST WAKE,\n"
                            "           not a slow one -- please report it with the config banner above.\n",
                            done.load(), made);
                return false;
            }
        }
        const double us = UsSince(t0);
        sched.WaitFor(wg);   // safe now: every body has already finished
        if (record) s.add(us / (double)made);
        return true;
    };

    if (!round(false)) { stalled = true; }        // warm-up, discarded
    // PROGRESS, so "slow" and "stuck" look different from outside. Without it a long section and a
    // hung one produce the same thing on a terminal: nothing.
    std::printf("  %d fibers x %d rounds: ", kFibers, reps);
    for (int r = 0; r < reps && !stalled; ++r) {
        if (!round(true)) { stalled = true; break; }
        std::printf("."); std::fflush(stdout);
    }
    std::printf("\n\n");

    if (stalled) {
        std::printf("  SECTION ABANDONED -- the rows below would be a statement about a stall.\n");
        return;
    }

    Row("resume, per fiber", s, "us");
    // SAME RULE AS SECTION 1: one sample per ROUND here, so `reps` samples, and a percentile over
    // fifteen of them is the maximum with a better name. The per-fiber figure above already
    // averages 64 resumes, which is where this section's statistics actually come from.
    if (s.v.size() >= 100) std::printf("  %-34s %9.2f us\n", "resume p99", s.pct(0.99));
    else                   std::printf("  %-34s %9.2f us   <- worst ROUND, not a percentile\n",
                                       "slowest round seen", s.hi());
    std::printf("\n  %d fibers released by one SignalAll, so this includes the wake storm -- which is\n"
                "  the realistic shape (an I/O burst or a frame boundary), not a single sleeper.\n"
                "  A round is bounded at 10s; anything slower is reported as a stall rather than\n"
                "  waited on, so this section cannot hang the run.\n", kFibers);
}

// ---------------------------------------------------------------------------------------------
// 4. PARALLELFOR -- the range API, at grains either side of the crossover.
//
// THE CROSSOVER IS A TIME, NOT AN N. Where parallel beats serial depends on how long the body takes,
// not how many items there are -- measured, the crossover N spans 781x across bodies while the
// serial time at crossover moves only 4.3x (~75us). So this reports the SERIAL BASELINE beside each
// arm rather than a speedup in isolation: a speedup with no baseline cannot be checked.
// ---------------------------------------------------------------------------------------------
static constexpr int kPfN   = 1 << 16;   // items
static constexpr int kPfOps = 24;         // dependent FP ops per item -- see the note below

static void BenchParallelFor(JLib::TaskScheduler& sched, int reps) {
    Section("4. PARALLELFOR -- against its own serial baseline");

    // ---- THE BODY IS COMPUTE-BOUND, AND THE PREVIOUS ONE WAS THE PROBLEM ---------------------
    //
    // This used to stream 4 MB of floats: `data[i] = data[i] * 1.000001f + 0.5f`. That is a MEMORY
    // benchmark wearing a scheduler's clothes. One core can already saturate a good fraction of DRAM
    // bandwidth, so adding thirty more buys almost nothing and the row reported ~3x on a 31-worker
    // pool -- a number about the memory subsystem that a reader would naturally read as a statement
    // about ParallelFor. It also made the row wildly unstable: a 10x spread on the parallel arm and
    // a speedup that wandered 3.86 -> 3.57 -> 2.96 across three runs of unchanged code.
    //
    // WHAT THE ROW IS SUPPOSED TO ANSWER: how well does the SPLITTER divide work and keep workers
    // fed. To see that, the body has to be limited by the thing being scaled -- cores -- not by a
    // resource that is already saturated at one core.
    //
    // SO: A DEPENDENT FLOATING-POINT CHAIN PER INDEX, and no array at all.
    //   * NO MEMORY TRAFFIC. Each item's work is computed FROM ITS INDEX, so the working set is a
    //     register. There is no array to stream, no cache to miss, and nothing shared to contend on.
    //   * DEPENDENT, so it cannot be vectorised into something the CPU finishes early. Each step
    //     needs the previous one's result, which makes the cost per item stable and real.
    //   * NOT ELIMINABLE. The accumulator escapes into an atomic once per SLICE -- not per item, so
    //     it is nowhere near the hot loop, and the optimiser cannot delete work whose result leaves
    //     the translation unit.
    //
    // FEWER ITEMS, MORE WORK EACH. 64K items at ~24 dependent ops rather than 1M at ~2, which keeps
    // the serial baseline in the same few-milliseconds range while making each item's cost dominate
    // the loop overhead around it.

    // The sink. Written once per slice; its only job is to stop the compiler proving the loop dead.
    static std::atomic<std::uint64_t> sink{ 0 };

    auto body = [](int b, int e) {
        double acc = 0.0;
        for (int i = b; i < e; ++i) {
            double x = 1.0 + (double)i * 1e-6;
            // A DEPENDENT CHAIN: every line needs the line above it.
            for (int k = 0; k < kPfOps; ++k)
                x = 1.0 / (1.0 + x * x) + x * 0.5;
            acc += x;
        }
        sink.fetch_add((std::uint64_t)(acc * 1024.0), std::memory_order_relaxed);
    };

    Samples serial, par, parNoProbe;

    auto runSerial = [&] { const auto t0 = Clock::now(); body(0, kPfN); serial.add(UsSince(t0)); };

    // ---- THE PROBE, A/B'd IN ONE PROCESS ----------------------------------------------------
    //
    // SetMeasuredWidth is ON by default: ParallelFor times the first chunk on the CALLER and picks
    // fan-out width as sqrt(W/c). Turning it off falls back to the older rule -- serial or the whole
    // pool, chosen by an iteration count that never looks at the body.
    //
    // INTERLEAVED, NOT TWO RUNS. The width is a runtime setter, so both arms are the same binary on
    // the same thermal state, alternating. Comparing two separate invocations would compare two
    // machines' worth of drift and call it a design decision.
    //
    // THE PROBE IS ALSO WHERE THIS ROW'S VARIANCE COMES FROM: it measures on a caller running at
    // single-core boost, before the pool spins up and the all-core clock drops, so it under-estimates
    // the width and recruitment widens afterwards. That means the honest comparison is not just the
    // medians -- it is the medians AND the spread, printed for both.
    auto runPar = [&](bool measured, Samples& out) {
        JLib::TaskScheduler::SetMeasuredWidth(measured);
        const auto t0 = Clock::now();
        std::function<void(int,int)> f = body;
        sched.ParallelFor(0, kPfN, 512, f);
        out.add(UsSince(t0));
    };

    runSerial(); runPar(true, par); runPar(false, parNoProbe);
    serial.v.clear(); par.v.clear(); parNoProbe.v.clear();   // warm-up

    for (int r = 0; r < reps; ++r) {
        switch (r % 3) {
            case 0: runSerial(); runPar(true, par);  runPar(false, parNoProbe); break;
            case 1: runPar(true, par);  runPar(false, parNoProbe); runSerial(); break;
            default: runPar(false, parNoProbe); runSerial(); runPar(true, par); break;
        }
    }
    JLib::TaskScheduler::SetMeasuredWidth(true);   // restore the default, always

    const size_t W = sched.GetWorkerCount();
    Row("serial baseline", serial, "us");
    Row("ParallelFor, probe ON  (ships)", par, "us");
    Row("ParallelFor, probe OFF", parNoProbe, "us");

    const double sp   = par.median()        > 0.0 ? serial.median() / par.median()        : 0.0;
    const double spNP = parNoProbe.median() > 0.0 ? serial.median() / parNoProbe.median() : 0.0;
    std::printf("  %-34s %9.2f x   of %zu workers (%.0f%% efficiency)\n",
                "speedup, probe ON", sp, W, W ? 100.0 * sp / (double)W : 0.0);
    std::printf("  %-34s %9.2f x   of %zu workers (%.0f%% efficiency)\n",
                "speedup, probe OFF", spNP, W, W ? 100.0 * spNP / (double)W : 0.0);

    // SPREAD, NOT JUST THE MEDIAN. The probe's known weakness is variance -- it measures on a caller
    // at single-core boost and recruitment corrects afterwards -- so an honest A/B has to show
    // whether turning it off trades a worse median for a steadier one, or is simply worse.
    auto spread = [](Samples& s) { return s.lo() > 0.0 ? s.hi() / s.lo() : 0.0; };
    std::printf("  %-34s %9.2f x / %.2f x   (hi/lo: ON, OFF)\n",
                "run-to-run spread", spread(par), spread(parNoProbe));

    if (sp > spNP * 1.05)
        std::printf("\n  THE PROBE EARNS ITS PLACE: it is faster than the fixed rule, not merely\n"
                    "  cheaper to justify. Turning it off falls back to serial-or-whole-pool chosen\n"
                    "  by an iteration count that never looks at the body.\n");
    else if (spNP > sp * 1.05)
        std::printf("\n  THE PROBE IS LOSING on this shape -- the fixed rule is faster here. Worth\n"
                    "  knowing which body reverses it before treating measured width as settled.\n");
    else
        std::printf("\n  INDISTINGUISHABLE on this shape. The probe is defended by the cases where the\n"
                    "  fixed rule is badly wrong (0.02x for trivial work at N=256, 6.9x for heavy work\n"
                    "  at the same N), not by this one -- a uniform body is where it matters least.\n");

    std::printf("\n  COMPUTE-BOUND BODY, so this row is about the SPLITTER rather than about memory\n"
                "  bandwidth -- see the note in the source for what it used to measure and why that\n"
                "  was the wrong question.\n");

    // ---- WORKER COUNT IS NOT THE CEILING, AND SAYING SO IS NOT AN EXCUSE ---------------------
    //
    // An earlier version of this line warned that efficiency under 50% was "worth looking at" and
    // pointed at the grain and the pool ramp. On this class of hardware that accuses the wrong
    // component. Two effects cap the achievable speedup before the scheduler is involved at all:
    //
    //   FREQUENCY. The serial arm is ONE core at single-core boost. The parallel arm is every core
    //   at the all-core bin, which is materially lower. The baseline is measured at a clock the
    //   parallel arm can never run at, so some of the "missing" speedup is arithmetic.
    //
    //   HETEROGENEOUS CORES. A hybrid part has performance and efficiency cores with different
    //   IPC and different clocks. Thirty-one workers is not thirty-one of whatever ran the serial
    //   arm, so "31x" was never the target.
    //
    // The number to compare across RUNS is this one; the number to compare against the worker count
    // is not meaningful on a hybrid, boosting machine. A regression shows up as this figure moving,
    // not as its distance from W.
    std::printf("  NOTE ON THE CEILING: %zu is not the target. The serial arm runs on ONE core at\n"
                "  single-core boost; the parallel arm runs on all of them at the all-core bin, and\n"
                "  on a hybrid part not all of those cores are the same speed. Compare this figure\n"
                "  against ITSELF across runs -- a regression moves it; its distance from %zu does\n"
                "  not mean what it looks like.\n", W, W);
}

// ---------------------------------------------------------------------------------------------
// 5. THE IDLE TAX -- what a pool costs the rest of your process while it is doing NOTHING.
//
// THE ROW MOST BENCHMARKS OMIT, AND THE ONE THAT DECIDES DEFAULTS. A scheduler benchmark measures a
// pool that IS the workload, so an always-awake worker looks free -- there is no render thread, no
// audio thread, no main thread for it to tax. Measured against a memory-bound main thread instead,
// an idle spinning pool cost 3.5% synthetically and 23% inside a real 2D game. That gap is why
// IdlePolicy::NoSleep was removed in 5.0 and why the awake floor is BOUNDED.
//
// SO THIS MEASURES THE FLOOR, which is the library's resting cost: F workers stay unparked by
// default. The arms are floor=0 (everyone parks) against the shipped floor, with the pool otherwise
// completely idle.
// ---------------------------------------------------------------------------------------------
static void BenchIdleTax(JLib::TaskScheduler& sched, int reps) {
    Section("5. IDLE TAX -- what an idle pool costs a memory-bound main thread");

    constexpr size_t kWorking = 1u << 20;   // 4 MB of floats: larger than L2, smaller than most L3
    static std::vector<float> buf;
    buf.assign(kWorking, 1.0f);

    // ---- THE WORKLOAD IS SWEPT BY LENGTH, AND THAT IS THE POINT OF THIS SECTION ------------
    //
    // A SINGLE LENGTH CANNOT ANSWER THE QUESTION IT WAS BEING ASKED. This row measured ~+20% for a
    // pool-wide floor against a historical +3.5% for the same worker count -- six times apart, same
    // machine class, same configuration. The likeliest explanation is not the scheduler: it is that
    // the historical measurement used a ~14.65 ms frame and this one used ~400 us.
    //
    // WHY LENGTH WOULD MATTER THAT MUCH. A short pass on an otherwise idle machine runs at full
    // boost; the instant N cores start spinning, all-core limits bite and the clock drops. Over a
    // full frame both arms settle toward the same sustained clock and the gap compresses. So a
    // 400 us sample sits inside the boost TRANSIENT, where spinning hurts most, and a 15 ms one
    // mostly does not.
    //
    // If the tax falls toward 3.5% as the workload lengthens, the two figures are ONE PHENOMENON AT
    // TWO TIMESCALES and both are correct about different things. If it stays near 20%, they are
    // different measurements and the historical number is the one to re-examine. Either answer is
    // worth having; a single length can produce neither.
    auto workload = [&](int passes) {
        const auto t0 = Clock::now();
        float acc = 0.0f;
        for (int p = 0; p < passes; ++p)
            for (size_t i = 0; i < kWorking; ++i) { buf[i] = buf[i] * 1.000001f + 0.5f; acc += buf[i]; }
        if (acc == 12345.678f) std::printf("");   // keep it observable
        return UsSince(t0);
    };

    // THE BASE, NOT THE LIVE VALUE, AND THE DIFFERENCE IS NOT ACADEMIC. GetAwakeFloor() returns
    // what the floor IS RIGHT NOW, and by the time this section runs the growth controller has
    // reacted to four sections of load -- the first version of this row read 29 on a 31-worker pool
    // and printed it as "as shipped" while the banner said 2. That is a mislabelled arm, which is
    // the exact failure this file's header warns about, produced by this file.
    const size_t baseFloor = JLib::TaskScheduler::GetAwakeFloorBase();
    const size_t wideFloor = sched.GetWorkerCount();
    Samples parked, shipped, wide;

    // COLLAPSE THE GROWN FLOOR FIRST, then let it settle. Measuring immediately after a floor change
    // measures the transition, not the state.
    JLib::TaskScheduler::SetAwakeFloor(baseFloor);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    (void)workload(1);   // warm-up

    // ---- THREE LENGTHS, SO THE TAX CAN BE READ AS A FUNCTION OF ONE ------------------------
    //
    // ~400 us is a burst, ~4 ms is a heavy chunk of a frame, ~15 ms is a whole 60 Hz frame and is
    // chosen to match the historical measurement this row is being reconciled against.
    struct Len { int passes; const char* name; };
    const Len lengths[] = { { 1, "~0.4 ms (burst)" }, { 10, "~4 ms" }, { 37, "~15 ms (a frame)" } };

    std::printf("  workload length is SWEPT -- see the note above for why one length cannot answer\n"
                "  the question. Each row is floor=0 against a pool-wide floor at that length.\n\n");
    std::printf("  %-18s %9s %9s %8s %9s %8s %9s %8s\n",
                "length", "parked", "wide 1/8", "tax", "wide none", "tax", "wide every", "tax");
    std::printf("  (yield cadence per idle pass: 1/8 = the default, none, every)\n");

    double taxAtShortest = 0.0, taxAtLongest = 0.0, taxNoYieldLong = 0.0, taxAllYieldLong = 0.0;

    // ---- THE THIRD ARM: A WIDE FLOOR THAT DOES NOT YIELD --------------------------------------
    //
    // THIS IS THE ONE THAT SETTLES WHETHER A POOL-WIDE FLOOR IS WHAT NoSleep WAS. Read the two idle
    // paths side by side:
    //
    //   NoSleep          search -> the park block skipped by policy -> loop. That is all.
    //   floor, F > 4     search -> park block skipped by band -> FloorSpin -> GetAwakeFloor()
    //                    -> yieldWithHandshake(): CAS to WS_YIELD, std::this_thread::yield(),
    //                       CAS back
    //   floor, F <= 4    search -> ... -> CpuRelax only, NO yield
    //
    // std::this_thread::yield() IS SwitchToThread ON WINDOWS -- it hands the core to another READY
    // thread. At F=31 that is thirty-one threads volunteering their cores to the OS scheduler on
    // every idle pass, plus two CAS operations each. NoSleep never did any of it. So a wide floor
    // and NoSleep are NOT the same mechanism, and comparing their taxes was never like for like.
    //
    // kYieldFloorMinDefault is 4, which is why the SHIPPED floor of 2 stays on the CpuRelax path and
    // measures ~0%. Raising the threshold above the pool size forces a WIDE floor down that same
    // path -- which is exactly what NoSleep did -- so this arm is the NoSleep-equivalent.
    //
    // FALSIFIABLE, WHICH IS THE POINT: if the tax collapses toward the historical +3.5% here, the
    // gap was the yield and there was never a discrepancy. If it does NOT collapse, this whole
    // explanation is wrong and the search reopens. The row is printed either way.
    // ---- AND A FIFTH ARM, BRACKETING THE YIELD FROM THE OTHER SIDE ---------------------------
    //
    // Arm 4 removed the yield and nothing happened, which is evidence in ONE direction. This arm
    // takes it the other way: SetSpinYieldMask(0) makes a floor worker yield on EVERY idle pass
    // instead of one in eight (the default mask is 7).
    //
    // THE PREDICTION BEING TESTED: that the tax is CORE OCCUPANCY and yielding cannot relieve it,
    // because std::this_thread::yield() with nothing else ready returns immediately -- with 31
    // spinning workers and one busy main thread on 32 logical cores, everybody already has a core
    // and there is nothing to yield TO. The thread keeps its core, keeps it clocked, and the
    // package stays in a low all-core boost bin.
    //
    // If that is right, yielding eight times as often changes nothing either, and the tax is
    // bracketed from both sides. If the tax FALLS, the occupancy story is wrong and the yield
    // cadence is a real lever that was left at the wrong setting -- which would be worth knowing.
    const size_t   yieldMinEntry = JLib::TaskScheduler::GetYieldFloorMin();
    const unsigned maskEntry     = JLib::TaskScheduler::GetSpinYieldMask();
    const size_t   kNoYield      = wideFloor + 1;   // above the pool: nobody is "big enough to yield"

    for (const Len& L : lengths) {
        Samples p, w, wq, wy;
        auto sample = [&](size_t f, size_t yieldMin, unsigned mask, Samples& out) {
            JLib::TaskScheduler::SetSpinYieldMask(mask);
            // THE TRANSITION IS NOT THE TAX. SetAwakeFloor(N) promotes and WAKES N parked workers,
            // which at ~5 us a wake is real work landing inside the sample -- and asymmetrically,
            // since floor=0 wakes nobody. A discarded pass absorbs it, leaving the recorded one
            // measuring steady-state OCCUPANCY, which is what the row claims.
            JLib::TaskScheduler::SetYieldFloorMin(yieldMin);
            JLib::TaskScheduler::SetAwakeFloor(f);
            (void)workload(L.passes);
            out.add(workload(L.passes));
        };
        // FOUR ARMS, ROTATED, so no arm is always the one that follows a drift. A fixed order over
        // four is worse than over two: the last arm inherits three arms' worth of ramp.
        //
        //   p  = parked          floor 0
        //   w  = wide + yield    floor N, default cadence (mask 7 -- one pass in eight)
        //   wq = wide, NO yield  floor N, threshold above the pool -> CpuRelax only
        //   wy = wide, yield ALL floor N, mask 0 -> a yield on every idle pass
        auto arm = [&](int which) {
            switch (which) {
                case 0: sample(0,         yieldMinEntry, maskEntry, p);  break;
                case 1: sample(wideFloor, yieldMinEntry, maskEntry, w);  break;
                case 2: sample(wideFloor, kNoYield,      maskEntry, wq); break;
                default: sample(wideFloor, yieldMinEntry, 0u,       wy); break;
            }
        };
        for (int r = 0; r < reps; ++r)
            for (int i = 0; i < 4; ++i) arm((r + i) % 4);
        const double tax   = p.median() > 0.0 ? 100.0 * (w.median()  - p.median()) / p.median() : 0.0;
        const double taxNY = p.median() > 0.0 ? 100.0 * (wq.median() - p.median()) / p.median() : 0.0;
        const double taxAY = p.median() > 0.0 ? 100.0 * (wy.median() - p.median()) / p.median() : 0.0;
        std::printf("  %-18s %9.1f %9.1f %+7.2f%% %9.1f %+7.2f%% %9.1f %+7.2f%%\n",
                    L.name, p.median(), w.median(), tax, wq.median(), taxNY, wy.median(), taxAY);
        if (&L == &lengths[0]) taxAtShortest = tax;
        taxAtLongest    = tax;
        taxNoYieldLong  = taxNY;
        taxAllYieldLong = taxAY;
    }
    JLib::TaskScheduler::SetYieldFloorMin(yieldMinEntry);
    JLib::TaskScheduler::SetSpinYieldMask(maskEntry);        // restore BOTH, always

    // ---- AND THE SHIPPED FLOOR, WHICH IS THE ONLY ROW THAT IS A CLAIM ABOUT THE DEFAULT ----
    {
        Samples p, s;
        auto sample = [&](size_t f, Samples& out) {
            JLib::TaskScheduler::SetAwakeFloor(f);
            (void)workload(1);
            out.add(workload(1));
        };
        for (int r = 0; r < reps; ++r) {
            if (r & 1) { sample(0, p); sample(baseFloor, s); }
            else       { sample(baseFloor, s); sample(0, p); }
        }
        const double tax = p.median() > 0.0
                         ? 100.0 * (s.median() - p.median()) / p.median() : 0.0;
        std::printf("\n  %-20s %10.2f us %10.2f us %+9.2f %%   <- floor=%zu, the DEFAULT\n",
                    "~0.4 ms, shipped", p.median(), s.median(), tax, baseFloor);
        if (Overlaps(p, s))
            std::printf("  INDISTINGUISHABLE, which is the expected answer for a small floor on a\n"
                        "  machine with cores to spare -- and it is the resting cost of the library.\n");
    }
    JLib::TaskScheduler::SetAwakeFloor(baseFloor);   // restore, always

    // ---- THE RECONCILIATION, STATED RATHER THAN LEFT TO THE READER -------------------------
    std::printf("\n  RECONCILIATION against the historical +3.5%% for IdlePolicy::NoSleep on a"
                " ~14.65 ms frame:\n"
                "      wide + yield : burst %+.2f%%   frame %+.2f%%\n"
                "      wide, NO yield (the NoSleep-equivalent path) : frame %+.2f%%\n",
                taxAtShortest, taxAtLongest, taxNoYieldLong);

    // LENGTH WAS THE FIRST HYPOTHESIS AND IT IS DEAD. Kept as a line rather than deleted, because a
    // future machine could behave differently and the reader should see the test, not just its
    // verdict on one box.
    if (taxAtLongest < taxAtShortest * 0.5)
        std::printf("  The tax FALLS with workload length -- the boost-transient explanation holds\n"
                    "  here, and the two figures are one phenomenon at two timescales.\n");
    else
        std::printf("  Length is NOT the explanation: the tax barely moves across 0.4 ms to 15 ms.\n");

    // ---- THE YIELD, BRACKETED FROM BOTH SIDES ------------------------------------------------
    //
    // Three cadences at the frame length: none, the default one-in-eight, and every pass. If the
    // tax is CORE OCCUPANCY then none of them should matter, because a yield with nothing else
    // ready returns immediately -- the thread keeps its core either way and the package stays in
    // the same all-core boost bin. If any of them moves the number, the cadence is a real lever.
    std::printf("\n  YIELD CADENCE, at the frame length: none %+.2f%%   1/8 %+.2f%%   every %+.2f%%\n",
                taxNoYieldLong, taxAtLongest, taxAllYieldLong);

    const double loCad = std::min(std::min(taxNoYieldLong, taxAtLongest), taxAllYieldLong);
    const double hiCad = std::max(std::max(taxNoYieldLong, taxAtLongest), taxAllYieldLong);
    const bool   flat  = (taxAtLongest > 0.0) && ((hiCad - loCad) < taxAtLongest * 0.25);

    if (flat)
        std::printf("  FLAT ACROSS ALL THREE, which brackets it: the yield cadence is not the lever.\n"
                    "  That is what core occupancy predicts -- a yield with nothing else ready gives\n"
                    "  nothing away, since every worker already has a core and the main thread is the\n"
                    "  only other runnable. So the cost is the cores being HELD and clocked, not the\n"
                    "  scheduling around them, and no cheaper spin recovers it.\n"
                    "  It also means a wide floor is NOT distinguishable from NoSleep by its yield,\n"
                    "  so the historical +3.5%% remains unexplained and unreproduced. Do not quote it.\n");
    else
        std::printf("  THE CADENCE MOVES THE TAX (%.2f%% spread), so it IS a lever and the occupancy\n"
                    "  explanation is incomplete. Whichever end is cheapest is worth understanding\n"
                    "  before the default of one-in-eight is treated as settled.\n", hiCad - loCad);
}

// ---------------------------------------------------------------------------------------------
int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    int reps = 15;
    size_t pool = 0;              // 0 = let the library size it
    bool wantReactor = false;

    for (int a = 1; a < argc; ++a) {
        const std::string s = argv[a];
        if (s == "--help" || s == "-h") {
            std::printf("usage: runtime_bench [reps=N] [pool=N] [io] [pin]\n"
                        "  reps=N  samples per arm (default 15)\n"
                        "  pool=N  worker count    (default: library's choice)\n"
                        "  io      enable the I/O reactor, which reserves K=2\n"
                        "  pin     FiberMode::Pin instead of the default Migrate\n");
            return 0;
        }
        if (s.rfind("reps=", 0) == 0) { reps = std::atoi(s.c_str() + 5); continue; }
        if (s.rfind("pool=", 0) == 0) { pool = (size_t)std::atoi(s.c_str() + 5); continue; }
        if (s == "io")  { wantReactor = true; continue; }
        if (s == "pin") { JLib::TaskScheduler::SetFiberMode(JLib::FiberMode::Pin); continue; }
        std::printf("unknown argument '%s' -- try --help\n", s.c_str());
        return 2;
    }
    if (reps < 3) reps = 3;

    if (wantReactor) JLib::TaskScheduler::EnableIoReactor(true);
    JLib::TaskScheduler::Init(pool);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

#ifndef JLIBSCHED_VERSION
#define JLIBSCHED_VERSION "unknown"
#endif
    // THE BANNER IS PART OF THE RESULT. Every row below moves with these, and a pasted number with
    // no configuration attached cannot be compared against anything.
    std::printf("JLib::Scheduler %s -- runtime bench\n", JLIBSCHED_VERSION);
    std::printf("  workers=%zu  K=%zu  floor=%zu  fibers=%s  reactor=%s  hw=%u  page=%zu  reps=%d\n",
                sched.GetWorkerCount(),
                JLib::TaskScheduler::GetHotWorkers(),
                JLib::TaskScheduler::GetAwakeFloor(),
                JLib::TaskScheduler::FibersMigrate() ? "Migrate" : "Pin",
                wantReactor ? "on" : "off",
                std::thread::hardware_concurrency(),
                JLib::platform::PageSize(),
                reps);
    std::printf("  NOTE: floor above is the LIVE value and the growth controller moves it under load;\n"
                "  section 5 reports the BASE it settles back to, which is the shipped resting cost.\n"
                "  This runtime is FIBERS ONLY and pure C++17 -- there is no coroutine arm to compare\n"
                "  against, and no idle policy to select. See the header for how to read these rows.\n");

    BenchDispatch(sched, reps);
    BenchThroughput(sched, reps);
    BenchFiberSuspend(sched, reps);
    BenchParallelFor(sched, reps);
    BenchIdleTax(sched, reps);

    std::printf("\ndone. Take anything you intend to quote twice, on an idle machine.\n");
    return 0;
}
