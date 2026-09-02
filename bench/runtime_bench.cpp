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
// WHAT THIS DELIBERATELY DOES NOT DO: it does not compare against other libraries. That belongs in
// bench/compare/, where the isolation rules are enforced -- two schedulers in one process measure
// each other's spinning, not their own throughput.

#include "TaskScheduler.h"
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
                                         ln, JLib::TaskType::Native);
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
            JLib::Task* t = sched.CreateTask([] {}, JLib::Lane::Normal, JLib::TaskType::Native);
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
                JLib::Task* t = sched.CreateTask([] {}, JLib::Lane::Normal, JLib::TaskType::Native);
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

        for (int i = 0; i < kFibers; ++i) {
            JLib::Task* t = sched.CreateTask([&] {
                parked.fetch_add(1, std::memory_order_release);
                JLib::TaskScheduler::Instance().WaitOnEvent(gate);
                done.fetch_add(1, std::memory_order_release);
            }, JLib::Lane::Normal, JLib::TaskType::Fiber);
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
static void BenchParallelFor(JLib::TaskScheduler& sched, int reps) {
    Section("4. PARALLELFOR -- against its own serial baseline");

    constexpr int kN = 1 << 20;
    static std::vector<float> data;
    data.assign(kN, 1.0f);

    auto body = [](int b, int e) {
        for (int i = b; i < e; ++i) data[i] = data[i] * 1.000001f + 0.5f;
    };

    Samples serial, par;

    auto runSerial = [&] { const auto t0 = Clock::now(); body(0, kN); serial.add(UsSince(t0)); };
    auto runPar    = [&] {
        const auto t0 = Clock::now();
        std::function<void(int,int)> f = body;
        sched.ParallelFor(0, kN, 4096, f);
        par.add(UsSince(t0));
    };

    runSerial(); runPar(); serial.v.clear(); par.v.clear();   // warm-up

    for (int r = 0; r < reps; ++r) {
        if (r & 1) { runSerial(); runPar(); }
        else       { runPar();    runSerial(); }
    }

    Row("serial baseline", serial, "us");
    Row("ParallelFor (grain 4096)", par, "us");
    const double sp = par.median() > 0.0 ? serial.median() / par.median() : 0.0;
    std::printf("  %-34s %9.2f x\n", "speedup", sp);
    std::printf("\n  %zu workers. A speedup well under the worker count is NOT automatically a defect --\n"
                "  this body is memory-bound, so it saturates bandwidth long before it saturates cores.\n",
                sched.GetWorkerCount());
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

    auto workload = [&] {
        const auto t0 = Clock::now();
        float acc = 0.0f;
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
    workload();   // warm-up

    for (int r = 0; r < reps; ++r) {
        // ARMS ALTERNATE AND THE FLOOR IS SET IMMEDIATELY BEFORE EACH, so a drift in the machine
        // lands on all three. Setting it once and running ten of each is how this row lies.
        auto sample = [&](size_t f, Samples& out) {
            JLib::TaskScheduler::SetAwakeFloor(f);
            out.add(workload());
        };
        if (r % 3 == 0)      { sample(0, parked); sample(baseFloor, shipped); sample(wideFloor, wide); }
        else if (r % 3 == 1) { sample(baseFloor, shipped); sample(wideFloor, wide); sample(0, parked); }
        else                 { sample(wideFloor, wide); sample(0, parked); sample(baseFloor, shipped); }
    }
    JLib::TaskScheduler::SetAwakeFloor(baseFloor);   // restore, always

    char l1[64], l2[64];
    std::snprintf(l1, sizeof l1, "floor=%zu   (as shipped)", baseFloor);
    std::snprintf(l2, sizeof l2, "floor=%zu  (whole pool awake)", wideFloor);
    Row("floor=0    (pool fully parked)", parked, "us");
    Row(l1, shipped, "us");
    Row(l2, wide, "us");

    if (parked.median() > 0.0) {
        std::printf("  %-34s %+9.2f %%\n", "tax of the shipped floor",
                    100.0 * (shipped.median() - parked.median()) / parked.median());
        std::printf("  %-34s %+9.2f %%\n", "tax of a pool-wide floor",
                    100.0 * (wide.median()    - parked.median()) / parked.median());
    }

    std::printf("\n  THE THIRD ROW IS WHAT NoSleep USED TO COST -- every worker held awake for the whole\n"
                "  run. It is here as the reference the shipped floor is cheap AGAINST, not as a\n"
                "  configuration to choose. Read the two taxes together: the second is the price of\n"
                "  the wake path the floor buys, the third is the price of buying it for everyone.\n");
    if (Overlaps(parked, shipped))
        std::printf("\n  Rows 1 and 2 are INDISTINGUISHABLE, which is the expected answer for a small\n"
                    "  floor on a machine with cores to spare. Row 3 is where the cost becomes visible.\n");
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
