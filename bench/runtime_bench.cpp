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
    // ZEROED HERE so the count below belongs to THIS section and not to whatever ran before it.
    JLib::TaskScheduler::ResetReservedYieldsSuppressed();

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

        // ---- WAS K YIELDING DURING THIS MEASUREMENT? ----------------------------------------
        //
        // A reserved worker no longer yields at all. It used to borrow the FLOOR's rule -- the gate
        // reads GetAwakeFloor() -- so K's policy was set by a quantity unrelated to K, and looked
        // deliberate only because the shipped floor of 2 sits below the threshold of 4. The growth
        // controller raises F under load, and past 4 every reserved worker began yielding.
        //
        // WHY IT BELONGS IN THIS SECTION SPECIFICALLY. A push aimed at a worker that has just
        // published WS_YIELD either re-aims or waits a quantum -- which is a TAIL, and the tail is
        // the only thing the lane is bought for. If the LowLatency p99 above sits worse than the
        // floor's, this line says whether the old coupling could have been the cause.
        //
        // ZERO IS A RESULT, NOT AN ABSENCE. It means F stayed under the threshold for this whole
        // run, the coupling never fired, and any K-latency figure here has to be explained by
        // something else. Non-zero is the number of yields the decoupling removed.
        const std::uint64_t kSup = JLib::TaskScheduler::GetReservedYieldsSuppressed();
        std::printf("  %-34s %9llu     %s\n",
                    "K yields suppressed (was: F-gated)", (unsigned long long)kSup,
                    kSup == 0 ? "<- F stayed under the threshold; the old coupling never fired here"
                              : "<- the old rule WAS yielding K; suspect this for the lane p99");
    } else {
        std::printf("  (too few samples for a percentile -- the range above is all this run supports)\n");
    }

    // ================= 1b. SHOULD K YIELD AT ALL? ==========================================
    //
    // TWO ARGUMENTS, OPPOSITE DIRECTIONS, AND UNTIL THIS ROW ONLY ONE HAD EVER BEEN ASSERTED.
    //
    //   AGAINST: K is reserved so a latency-critical completion meets a RUNNING thread. A yield is
    //   a mini-preempt of exactly that thread, and a push aimed at a worker which just published
    //   WS_YIELD re-aims or eats a quantum -- the tail K exists to remove.
    //
    //   FOR: K NEVER PARKS, so it is the most permanent core hog in the pool. Section 5b measured a
    //   never-yielding spinner costing 1.9x on the measuring thread's own frame and 28% of
    //   competitor throughput. The thread that holds a core forever is precisely the one that
    //   starves others at quantum granularity.
    //
    // THE ONE PIECE OF EVIDENCE BEFORE THIS ROW EXISTED cut against the first argument: in a run
    // where the suppression counter read ZERO -- K did not yield at all -- LowLatency p99 was
    // 1.70 us against the floor's 0.90. Not yielding did not buy K a better tail than the floor
    // already had. So "K must never yield" was a mechanism story with no number under it, and it
    // was briefly hard-coded as policy on the strength of sounding right.
    //
    // WHAT THIS ROW CANNOT DO ALONE: it runs on a quiet machine, which is the regime where yielding
    // has nothing to yield to and can only cost. If K's tail is unchanged here, that is a licence to
    // yield, not a reason to -- the reason lives in 5b, under oversubscription. Read the two rows
    // together or neither.
    if (K > 0 && normal.v.size() >= 100) {
        std::printf("\n1b. SHOULD K YIELD? (K's tail against its own cadence, quiet machine)\n");
        std::printf("----------------------------------------------------------------------\n");

        const unsigned rmEntry = JLib::TaskScheduler::GetReservedYieldMask();
        struct KArm { const char* name; unsigned mask; };
        const KArm karms[] = {
            { "K never yields (ships)", JLib::TaskScheduler::kReservedYieldNever },
            { "K yields 1 pass in 8",   7u },
            { "K yields every pass",    0u },
        };
        Samples ks[3];

        // ROTATED, and a third of the section-1 sample count per arm so this does not triple the
        // run time to answer a subsidiary question.
        // FULL SAMPLE COUNT PER ARM, NOT A THIRD OF IT, and the reason is the statistic below.
        // p99.9 over 1,666 samples is the SECOND-worst element -- barely more robust than the
        // maximum it was brought in to replace. Over 5,000 it is the fifth-worst, which survives a
        // single descheduling event. This arm costs three times what section 1 costs; that is the
        // price of the tail being the thing actually in question.
        const int kPer = std::max(2000, kLatSamples);
        for (int r = 0; r < kPer; ++r) {
            for (int i = 0; i < 3; ++i) {
                const int a = (r + i) % 3;
                JLib::TaskScheduler::SetReservedYieldMask(karms[a].mask);
                one(JLib::Lane::LowLatency, ks[a]);
            }
        }
        JLib::TaskScheduler::SetReservedYieldMask(rmEntry);       // restore, always

        std::printf("  %-26s %10s %10s %10s %10s\n",
                    "cadence", "p50 (us)", "p99 (us)", "p99.9 (us)", "max (us)");
        for (int a = 0; a < 3; ++a)
            std::printf("  %-26s %10.2f %10.2f %10.2f %10.2f\n",
                        karms[a].name, ks[a].median(), ks[a].pct(0.99), ks[a].pct(0.999), ks[a].hi());

        // ---- THE VERDICT READS p99.9, AND THE FIRST VERSION READ THE WRONG THING ------------
        //
        // It gated on p99 and printed "K's tail does not care about its own cadence" from a run
        // whose p99 was 1.60 us in all three arms -- while the MAX column read 324.40 / 39.90 /
        // 59.40. The one statistic that moved was the one the verdict ignored, on a row whose whole
        // subject is the bounded tail.
        //
        // AND THE MAX IS STILL NOT THE ANSWER. A single 324 us sample is one descheduling event
        // until it happens twice; section 1 says exactly that about its own worst-sample row, and
        // it would be inconsistent to treat the same statistic as evidence here because it points
        // somewhere interesting. p99.9 over 5,000 samples is the fifth-worst -- far enough out to
        // be the tail, far enough in that one stall cannot make it.
        //
        // MAX IS PRINTED ANYWAY, because a reader comparing three rotated arms should see it. It is
        // context, not the claim.
        const double pNever = ks[0].pct(0.999), pEighth = ks[1].pct(0.999), pEvery = ks[2].pct(0.999);
        const double worstYield = std::max(pEighth, pEvery);
        if (pNever <= 0.0 || worstYield <= 0.0) {
            std::printf("\n  NO USABLE PERCENTILE from this run -- nothing to conclude.\n");
        } else if (worstYield > pNever * 1.25) {
            std::printf("\n  YIELDING COSTS K ITS TAIL: p99.9 rises %.0f%% when K yields. That is the\n"
                        "  against-argument earning its place, and the default should stand.\n",
                        100.0 * (worstYield / pNever - 1.0));
        } else if (pNever > worstYield * 1.25) {
            std::printf("\n  K'S TAIL IS BETTER WHEN IT YIELDS: p99.9 %.2f us never vs %.2f us worst\n"
                        "  yielding arm, a %.0f%% improvement. That inverts the current default, and\n"
                        "  it is the ONE claim this row is built to make -- so repeat it before acting.\n"
                        "  It agrees with 5b, where not yielding cost 1.9x on an oversubscribed box.\n",
                        pNever, worstYield, 100.0 * (pNever / worstYield - 1.0));
        } else {
            std::printf("\n  K'S TAIL DOES NOT CARE about its own cadence on a quiet machine (%.2f vs\n"
                        "  %.2f us p99.9). The against-argument is UNSUPPORTED here -- which does not\n"
                        "  make yielding right, it removes the reason not to. Section 5b is where the\n"
                        "  reason FOR it lives: a permanently spinning K is the pool's most durable\n"
                        "  core hog, and 5b priced that at 1.9x on an oversubscribed box.\n"
                        "  CHECK THE MAX COLUMN BEFORE MOVING ON. If it separates while p99.9 does\n"
                        "  not, that is a single stall in one arm, not a finding -- but it is worth\n"
                        "  a repeat, because it is also what a quantum-length wait looks like.\n",
                        pNever, worstYield);
        }
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

    // ================= 2b. THE GUEST YIELD CADENCE ==========================================
    //
    // THE IDLE LOOP HAS TWO ARMS AND ONLY ONE WAS EVER MEASURABLE.
    //
    //   FLOOR (onAwakeFloor)  always-on, forbidden to sleep. Yields at SetSpinYieldMask -- one
    //                         pass in eight by default, staggered by qIndex, and not at all below
    //                         YieldFloorMin. Section 5 sweeps this one.
    //   GUEST (everyone else) awake because something was ADVERTISED, and on its way to a park.
    //                         Yielded EVERY pass, unconditionally, with no knob at all.
    //
    // That asymmetry is backwards on its face. yield() is politeness owed by whoever holds a core
    // INDEFINITELY, which is the floor by construction. A guest is heading for a park, and parking
    // is the yield -- a more complete one. Yet the guest was the arm paying on every pass.
    //
    // WHY THIS ROW IS HERE AND NOT IN SECTION 5. The idle-tax sweep sets a POOL-WIDE floor, so
    // every worker is onAwakeFloor and there are NO GUESTS in it -- it structurally cannot measure
    // this arm, and adding a column there would have produced three identical numbers and a
    // conclusion. A guest exists under LOAD: a burst arrives, more workers wake than there is work
    // for, and the losers scan, find nothing, and spin briefly before parking. That is exactly what
    // 20k empty tasks through one producer manufactures, which is why it belongs in this section.
    //
    // WHAT WOULD MAKE THE YIELD WORTH KEEPING: the floor GROWS, so a worker that entered a pass as
    // a guest can become a legal aimed target mid-pass, and a push aimed at a spinner that never
    // publishes its state buys nothing. But that is the HANDSHAKE's job. A guest that skips the
    // yield stays on the core at WS_EMPTY -- "on core, scanning, no syscall owed" -- which is the
    // truthful advertisement. The frequency is a separate question from the handshake, and this row
    // is the first time it has been asked.
    {
        std::printf("\n2b. GUEST-ARM YIELD CADENCE (the burst path, not the floor)\n");
        std::printf("------------------------------------------------------------\n");

        const unsigned guestEntry = JLib::TaskScheduler::GetGuestYieldMask();
        Samples every, eighth, never;

        auto runWith = [&](unsigned mask, Samples& out) {
            JLib::TaskScheduler::SetGuestYieldMask(mask);
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
            out.add((double)kPerRep / UsSince(t0));
        };

        runWith(0u, every); runWith(7u, eighth);
        runWith(JLib::TaskScheduler::kGuestYieldNever, never);
        every.v.clear(); eighth.v.clear(); never.v.clear();          // warm-up discarded

        // ROTATED, so no arm is always the one that follows a drift.
        for (int r = 0; r < reps; ++r) {
            switch (r % 3) {
                case 0:  runWith(0u, every); runWith(7u, eighth); runWith(JLib::TaskScheduler::kGuestYieldNever, never); break;
                case 1:  runWith(7u, eighth); runWith(JLib::TaskScheduler::kGuestYieldNever, never); runWith(0u, every); break;
                default: runWith(JLib::TaskScheduler::kGuestYieldNever, never); runWith(0u, every); runWith(7u, eighth); break;
            }
        }
        JLib::TaskScheduler::SetGuestYieldMask(guestEntry);          // restore, always

        Row("guest yields EVERY pass (ships)", every,  "M/s");
        Row("guest yields 1 pass in 8", eighth, "M/s");
        Row("guest never yields", never,  "M/s");

        // THE VERDICT IS GATED ON OVERLAP, like every other verdict in this file. Three arms means
        // the honest question is whether the BEST separates from the SHIPPED one, not whether some
        // pair of medians differ.
        Samples& best = (never.median() >= eighth.median())
                        ? (never.median() >= every.median() ? never : every)
                        : (eighth.median() >= every.median() ? eighth : every);
        const char* bestName = (&best == &never) ? "never" : (&best == &eighth ? "1-in-8" : "every");

        if (&best == &every || Overlaps(best, every)) {
            std::printf("\n  NO CASE TO ANSWER on this shape: the ranges overlap, so relaxing the guest\n"
                        "  yield does not measurably buy throughput here. The unconditional yield stays\n"
                        "  the default on the evidence, which is the right outcome for a knob added to\n"
                        "  test a hypothesis rather than to be turned.\n");
        } else {
            std::printf("\n  THE GUEST YIELD IS COSTING THROUGHPUT: '%s' beats the shipped 'every pass'\n"
                        "  by %.1f%% with NO range overlap. That is the arm with the least reason to be\n"
                        "  polite -- it is on its way to a park -- so this is worth following.\n"
                        "  BEFORE CHANGING THE DEFAULT, measure the VICTIM: a guest that stops yielding\n"
                        "  keeps its core, and this row cannot see what that costs a co-running thread.\n"
                        "  Section 5's shape (a busy main thread beside the pool) is where that shows.\n",
                        bestName,
                        100.0 * (best.median() / std::max(1e-9, every.median()) - 1.0));
        }
    }
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

    // ---- THE VERDICT IS GATED ON OVERLAP, NOT ON A FIXED 5% -------------------------------
    //
    // THIS SECTION USED TO COMPARE MEDIANS AGAINST A 1.05 THRESHOLD, and that is only meaningful
    // when the noise is well under 5%. Here it is not: a real run reported
    //
    //     probe ON 252.2 us, probe OFF 235.8 us   -- a 7% gap
    //     run-to-run spread 2.44x and 2.27x       -- a +130% range
    //
    // and printed "THE PROBE IS LOSING" on the strength of 13.02 > 12.17 * 1.05, a margin of
    // 0.24x. The same binary on the same machine had said the opposite the day before. A test that
    // flips sign between runs and prints a confident label either way is worse than no test: it
    // manufactures a directional claim out of noise and puts it in front of whoever reads the
    // output next.
    //
    // Overlaps() has been in this file the whole time, used by sections 1, 2 and 5 for exactly
    // this. Section 4 simply never called it. The fix is not a bigger threshold -- it is refusing
    // to make a directional claim when the ranges do not separate.
    if (Overlaps(par, parNoProbe)) {
        std::printf("\n  INDISTINGUISHABLE on this shape, and the RANGES OVERLAP -- so this run makes\n"
                    "  no claim about which is faster. The medians differ by %.1f%%, which is inside\n"
                    "  the %.2fx / %.2fx run-to-run spread; a difference smaller than the noise is\n"
                    "  not a result. A uniform compute-bound body is where the two rules agree by\n"
                    "  construction: see the trivial and heavy shapes below, which is where an\n"
                    "  iteration-count rule and a body-timing probe are supposed to diverge.\n",
                    100.0 * (std::max(sp, spNP) / std::max(1e-9, std::min(sp, spNP)) - 1.0),
                    spread(par), spread(parNoProbe));
    } else if (sp > spNP) {
        std::printf("\n  THE PROBE EARNS ITS PLACE: faster than the fixed rule, and the ranges do NOT\n"
                    "  overlap, so this is a result rather than a coin flip. Turning it off falls\n"
                    "  back to serial-or-whole-pool chosen by an iteration count that never looks\n"
                    "  at the body.\n");
    } else {
        std::printf("\n  THE PROBE IS LOSING on this shape, and the ranges do NOT overlap -- so this\n"
                    "  one is real. Worth knowing which body reverses it before treating measured\n"
                    "  width as settled.\n");
    }

    // ================= 4b. THE TWO SHAPES WHERE THE RULES MUST DISAGREE ======================
    //
    // The arm above cannot settle the probe, and that is not a flaw in it -- a uniform
    // compute-bound body at 64K items is where an iteration-count rule and a body-timing probe
    // agree BY CONSTRUCTION. Both say "this is big, go wide", and both are right.
    //
    // This section is the part that was missing. It used to be a sentence CITING two numbers
    // measured somewhere else -- "0.02x for trivial work at N=256, 6.9x for heavy work at the same
    // N" -- which is a benchmark asserting its conclusion instead of producing it. Those two cases
    // are cheap to run, so they are run.
    //
    // SAME N, OPPOSITE CORRECT ANSWERS. That is the whole design:
    //
    //   TRIVIAL body at small N  -> the right answer is SERIAL. Fanning 256 near-empty items across
    //                               29 workers costs more in dispatch than the work is worth.
    //   HEAVY body at small N    -> the right answer is WIDE. The same 256 items now carry real
    //                               work, and serial leaves the whole machine idle.
    //
    // An iteration count cannot tell these apart -- N is 256 in both. A probe that TIMES THE FIRST
    // CHUNK can, because it is measuring the body rather than counting it. So if the probe is worth
    // anything, this is where it shows, and if it is not, this is where that shows too.
    {
        std::printf("\n4b. THE TWO SHAPES THAT SEPARATE THE RULES (same N, opposite right answers)\n");
        std::printf("---------------------------------------------------------------------------\n");

        constexpr int kSmallN = 256;

        // Trivial: one multiply-add per item. Escapes to the sink once per slice, so it cannot be
        // deleted, but it is nowhere near enough work to pay for fan-out.
        auto trivial = [](int b, int e) {
            double acc = 0.0;
            for (int i = b; i < e; ++i) acc += (double)i * 1.000001;
            sink.fetch_add((std::uint64_t)acc, std::memory_order_relaxed);
        };

        // Heavy: the SAME dependent chain as the main arm, ~40x the ops per item. Same item count,
        // so the only thing that changed is the cost of a single index.
        auto heavy = [](int b, int e) {
            double acc = 0.0;
            for (int i = b; i < e; ++i) {
                double x = 1.0 + (double)i * 1e-6;
                for (int k = 0; k < kPfOps * 40; ++k)
                    x = 1.0 / (1.0 + x * x) + x * 0.5;
                acc += x;
            }
            sink.fetch_add((std::uint64_t)(acc * 1024.0), std::memory_order_relaxed);
        };

        // One shape at a time, both rules interleaved -- same binary, same thermal state, so the
        // comparison is between the RULES and not between two moments.
        // ---- INNER REPEATS, BECAUSE THE TRIVIAL SHAPE IS BELOW THE CLOCK -----------------------
        //
        // 256 items at one flop each is ~256 flops. The first version of this timed ONE call and
        // reported 0.10 us against 0.20 us -- a "2.00x penalty" that is one tick of the clock, not
        // a measurement. The heavy row carried the whole verdict and the trivial row was decoration
        // pretending to be evidence.
        //
        // THE REPEAT GOES AROUND THE MEASUREMENT, NOT INTO THE RANGE. Raising kSmallN would change
        // the question -- the entire point is what each rule does at a SMALL N, and the fixed rule
        // is being asked whether 256 is big. So N stays 256, the call is made `inner` times, and
        // the total is divided. Every call still sees 256 items and each rule still makes the same
        // decision it would make once; only the clock gets enough to resolve.
        //
        // PER-SHAPE, NOT A CONSTANT. The heavy row is already ~1000 us and needs no help; giving it
        // the trivial row's repeat count would multiply this section's runtime by 2000 to sharpen a
        // number that is already sharp.
        auto runShape = [&](const char* name, int inner, auto&& fn) {
            Samples ser, on, off;
            auto doSerial = [&] {
                const auto t0 = Clock::now();
                for (int j = 0; j < inner; ++j) fn(0, kSmallN);
                ser.add(UsSince(t0) / (double)inner);
            };
            auto doPar = [&](bool measured, Samples& out) {
                JLib::TaskScheduler::SetMeasuredWidth(measured);
                std::function<void(int,int)> f = fn;
                const auto t0 = Clock::now();
                for (int j = 0; j < inner; ++j)
                    sched.ParallelFor(0, kSmallN, 1, f);  // grain 1: let the rule choose the width
                out.add(UsSince(t0) / (double)inner);
            };

            doSerial(); doPar(true, on); doPar(false, off);
            ser.v.clear(); on.v.clear(); off.v.clear();                 // warm-up discarded

            for (int r = 0; r < reps; ++r) {
                switch (r % 3) {
                    case 0:  doSerial();      doPar(true, on); doPar(false, off); break;
                    case 1:  doPar(true, on); doPar(false, off); doSerial();      break;
                    default: doPar(false, off); doSerial();      doPar(true, on); break;
                }
            }
            JLib::TaskScheduler::SetMeasuredWidth(true);

            std::printf("\n  %s body, N=%d\n", name, kSmallN);
            Row("  serial", ser, "us");
            Row("  ParallelFor, probe ON", on, "us");
            Row("  ParallelFor, probe OFF", off, "us");

            // THE MEASURE IS DISTANCE FROM THE RIGHT ANSWER, not raw speed. For each shape the best
            // achievable is min(serial, wide); a rule is judged by how close it gets to that, which
            // is what makes one number comparable across two shapes with opposite right answers.
            const double best = std::min(ser.median(), std::min(on.median(), off.median()));
            if (best > 0.0) {
                std::printf("  %-30s %8.2fx / %8.2fx   (cost vs the best of the three: ON, OFF)\n",
                            "  penalty for the rule", on.median() / best, off.median() / best);
            }
            return std::pair<double,double>{ on.median(), off.median() };
        };

        // 2000 inner repeats on the trivial shape (~256 flops a call, far below the clock); 1 on
        // the heavy shape, which is already ~1000 us and resolves fine.
        const auto triv = runShape("TRIVIAL (1 flop/item; serial should win)", 2000, trivial);
        const auto hvy  = runShape("HEAVY   (960 flops/item; wide should win)", 1, heavy);

        // ---- THE JOINT CLAIM, which neither shape can make alone -----------------------------
        //
        // A rule that is merely fast on one shape proves nothing -- serial-always wins the trivial
        // row and loses the heavy one; whole-pool-always does the reverse. The question is whether
        // ONE rule is close to right on BOTH, at the same N, and that is a claim only the pair can
        // support.
        const double onWorst  = std::max(triv.first  / std::min(triv.first,  triv.second),
                                         hvy.first   / std::min(hvy.first,   hvy.second));
        const double offWorst = std::max(triv.second / std::min(triv.first,  triv.second),
                                         hvy.second  / std::min(hvy.first,   hvy.second));
        std::printf("\n  WORST CASE ACROSS BOTH SHAPES:  probe ON %.2fx   probe OFF %.2fx\n",
                    onWorst, offWorst);
        if (onWorst < offWorst * 0.9)
            std::printf("  THE PROBE EARNS ITS PLACE HERE. It is not faster on either shape by much;\n"
                        "  it is close to right on BOTH, at one N, which is the thing an iteration\n"
                        "  count structurally cannot do.\n");
        else if (offWorst < onWorst * 0.9)
            std::printf("  THE FIXED RULE WINS THE PAIR, which would be a real finding -- it would mean\n"
                        "  the probe mis-times these shapes badly enough to lose its own best case.\n");
        else
            std::printf("  NEITHER SEPARATES on this machine. Before concluding anything, check that\n"
                        "  the shapes really did diverge: if serial and wide are close on BOTH rows,\n"
                        "  kSmallN or the op counts are not extreme enough to pose the question.\n");
    }

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
    // ================= 5b. OVERSUBSCRIBED -- WHERE THE YIELD IS SUPPOSED TO EARN IT ==========
    //
    // EVERY YIELD ROW ABOVE IS RIGGED IN THE YIELD'S DISFAVOUR, and saying so is the point. With
    // 29 workers and one busy main thread on 32 logical cores, SwitchToThread() has nothing to
    // switch TO: it enters the kernel, finds no other ready thread on that processor, and returns.
    // Pure cost, no benefit -- which is exactly what the rows show, and it would be dishonest to
    // conclude "the yield is worthless" from a configuration where it structurally cannot help.
    //
    // YIELD IS POLITENESS, AND POLITENESS ONLY PAYS WHEN SOMEBODY IS WAITING. So this arm puts
    // somebody there: COMPETITOR THREADS, enough that runnable threads exceed cores. Now a yielding
    // worker hands its core to a thread that can use it, and refusing to yield takes it away.
    //
    // TWO NUMBERS, AND THE SECOND IS THE ONE THE OTHER ROWS CANNOT SEE:
    //
    //   OURS    the same main-thread workload as above -- what the pool costs us.
    //   THEIRS  total competitor iterations -- what the pool costs EVERYBODY ELSE.
    //
    // A change that improves OURS while destroying THEIRS is not an optimisation, it is a transfer,
    // and it is precisely the shape that a scheduler-only benchmark reports as a win. That is the
    // same trap as raising thread priority to fix a dispatch stall: the pool's numbers improve by
    // taking from the thing the pool exists to serve.
    {
        std::printf("\n5b. OVERSUBSCRIBED -- the yield's actual job (competitors > free cores)\n");
        std::printf("-----------------------------------------------------------------------\n");

        const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
        // ENOUGH TO GUARANTEE CONTENTION rather than merely hint at it: the pool is already sized
        // near hw, so one competitor per hardware thread means every yield has a taker.
        const unsigned kCompetitors = hw;

        std::atomic<bool> stop{ false };
        std::atomic<unsigned long long> spins{ 0 };
        std::vector<std::thread> comp;
        comp.reserve(kCompetitors);
        for (unsigned i = 0; i < kCompetitors; ++i) {
            comp.emplace_back([&stop, &spins] {
                unsigned long long local = 0;
                double x = 1.0;
                // PUBLISHED WHILE RUNNING, NOT AT EXIT. The first version accumulated into `local`
                // and did ONE fetch_add when the thread ended -- which is after every arm has been
                // measured. Every per-arm delta was therefore exactly zero, and the section printed
                // "THE YIELD BUYS THE COMPETITORS NOTHING" from an instrument that had published
                // nothing at all. A verdict computed from 0/0 is the failure this whole file exists
                // to avoid, produced by this file.
                //
                // BATCHED AT 64, not per iteration: the point is to be visible to the reader
                // sampling between arms, not to add a contended atomic to the competitor's inner
                // loop and measure THAT.
                while (!stop.load(std::memory_order_relaxed)) {
                    // Real work, not a spin on the flag: a competitor that only polls an atomic is
                    // not competing for anything a yield could hand it.
                    for (int k = 0; k < 512; ++k) x = x * 1.0000001 + 0.5;
                    if ((++local & 63u) == 0) spins.fetch_add(64, std::memory_order_relaxed);
                }
                if (x == 4321.1234) std::printf("");            // keep it observable
                spins.fetch_add(local & 63u, std::memory_order_relaxed);   // the remainder
            });
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));   // let them get scheduled

        const Len& L = lengths[2];   // the frame length -- where the yield rows diverged most
        struct Arm { const char* name; size_t yieldMin; unsigned mask; };
        const Arm arms[] = {
            { "wide, no yield", kNoYield,      maskEntry },
            { "wide, 1-in-8",   yieldMinEntry, maskEntry },
            { "wide, every",    yieldMinEntry, 0u        },
        };

        double oursMed[3] = { 0, 0, 0 };
        unsigned long long theirs[3] = { 0, 0, 0 };

        // INTERLEAVED AND ROTATED, like every other multi-arm row in this file. The first version
        // ran 25 reps of arm 0, then 25 of arm 1, then 25 of arm 2 -- exactly the fixed order this
        // file's own header warns about ("a fixed order once moved a control configuration by 2x
        // here"). It put `no yield` first and reported it 1.9x worse than 1-in-8, which is equally
        // consistent with a machine warming up under 32 competitor threads.
        Samples oursS[3];
        unsigned long long theirsAcc[3] = { 0, 0, 0 };

        auto oneArm = [&](int a) {
            JLib::TaskScheduler::SetSpinYieldMask(arms[a].mask);
            JLib::TaskScheduler::SetYieldFloorMin(arms[a].yieldMin);
            JLib::TaskScheduler::SetAwakeFloor(wideFloor);
            const unsigned long long before = spins.load(std::memory_order_relaxed);
            oursS[a].add(workload(L.passes));
            theirsAcc[a] += spins.load(std::memory_order_relaxed) - before;
        };

        for (int a = 0; a < 3; ++a) { oneArm(a); oursS[a].v.clear(); theirsAcc[a] = 0; }  // warm-up

        for (int r = 0; r < reps; ++r)
            for (int i = 0; i < 3; ++i) oneArm((r + i) % 3);

        for (int a = 0; a < 3; ++a) { oursMed[a] = oursS[a].median(); theirs[a] = theirsAcc[a]; }

        stop.store(true, std::memory_order_relaxed);
        for (auto& t : comp) t.join();

        std::printf("  %u competitor threads on %u hardware threads, floor=%zu, frame length\n",
                    kCompetitors, hw, wideFloor);
        std::printf("  %-18s %12s %14s\n", "cadence", "ours (us)", "theirs (iters)");
        for (int a = 0; a < 3; ++a)
            std::printf("  %-18s %12.1f %14llu\n", arms[a].name, oursMed[a], theirs[a]);

        // ---- THE READING, STATED SO IT CANNOT BE TAKEN THE CONVENIENT WAY -------------------
        //
        // The claim under test is NOT "which cadence is fastest for us". It is whether the yield
        // buys the competitors anything at a price we can name. If THEIRS barely moves across the
        // three cadences, the yield is not doing its job even where its job exists, and the
        // unconditional guest yield is cost with no beneficiary anywhere.
        // ZERO IS NOT A RESULT, IT IS A BROKEN INSTRUMENT -- and saying so out loud is the whole
        // lesson of the first version, which printed a confident verdict from 0/0. If the
        // competitors reported nothing, the only honest output is that this row measured nothing.
        const unsigned long long theirsMin = std::min(std::min(theirs[0], theirs[1]), theirs[2]);
        const unsigned long long theirsMax = std::max(std::max(theirs[0], theirs[1]), theirs[2]);
        // NO EARLY RETURN ON FAILURE. An exit here would skip the SHIPPED-floor row further down,
        // which is the only row in this section that is a claim about the DEFAULT. A broken
        // sub-experiment must not take the product's own number with it.
        if (theirsMin == 0) {
            std::printf("\n  NO COMPETITOR PROGRESS RECORDED (min=0). This row measured NOTHING about\n"
                        "  the yield -- an instrument failure, not a finding, and no conclusion about\n"
                        "  cadence may be drawn from it. The first version of this section did exactly\n"
                        "  that: the competitor threads published their count at EXIT, so every\n"
                        "  per-arm delta was zero and the spread came out 0.00x, which was then\n"
                        "  printed as 'the yield buys the competitors nothing'.\n");
        } else {
            const double theirsSpread = (double)theirsMax / (double)theirsMin;
            std::printf("\n  competitor throughput spread across cadences: %.2fx\n", theirsSpread);
            if (theirsSpread < 1.05)
                std::printf("  THE YIELD BUYS THE COMPETITORS NOTHING even here, where there is something\n"
                            "  to yield to -- which is what a preemptive OS predicts: SwitchToThread\n"
                            "  gives up the rest of one quantum and returns the thread to the ready\n"
                            "  queue immediately, so the SHARE each thread gets is unchanged. If that\n"
                            "  holds on repeat, the cadence is cost with no beneficiary, and only\n"
                            "  PARKING actually gives a core back.\n");
            else
                std::printf("  THE YIELD IS DOING ITS JOB: competitor throughput moves with the cadence, so\n"
                            "  the tax in the rows above is buying something real for threads outside the\n"
                            "  pool. Read those rows as a PRICE, not as waste.\n");
        }
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
