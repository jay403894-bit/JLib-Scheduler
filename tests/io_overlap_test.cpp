// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES A COMPLETION STILL LOSE WHEN IT LANDS ON A WORKER THAT IS RUNNING BULK?
//
// WHY THIS FILE EXISTS AGAIN. PickNextWorker carries a recorded result -- "lane was briefly
// steered at the unreserved floor, and it lost" -- with numbers from a test of this name:
//
//     loPri/lane at p99:  0.12x, 0.91x, 0.62x     (>1 would mean lane wins)
//
// The comment outlived the harness. tests/io_overlap_test.cpp was not in the tree when the
// question came up again, so the one measurement standing between the reserved band and its
// deletion could not be reproduced, checked for what it actually configured, or re-run at a
// different grain. A result nobody can re-run is a story.
//
// AND THE GRAIN IS THE WHOLE QUESTION. That measurement saturated the pool with 400 us bulk
// bodies. The argument against removing K rests entirely on head-of-line blocking -- a completion
// queued behind a body that has not returned -- so it is an argument about HOW LONG THE BODY IS,
// and 400 us is not what a frame looks like. The ParallelFor crossover table puts the interesting
// grain at ~75 us of total serial work split across ~29 workers, i.e. single-digit-microsecond
// leaves. Waiting behind one of those is nothing; waiting behind 400 us is the entire finding.
//
// So this sweeps the body length and reports the ratio at each. If lane stops losing at game
// grain, the reserved band is dead weight for that workload and the deletion is clean. If it only
// loses at 400 us, K is an option for applications that admit long bodies rather than a default.
//
// ---- WHAT IS ASSERTED, AND WHAT IS ONLY REPORTED -------------------------------------------
//
// ASSERTED: every probe completes. That is not a formality -- an inbox has one legal consumer, so
// a completion placed on a worker that stops consuming is not late, it is unreachable, and the
// symptom is a hang rather than a bad number. The watchdog turns that hang into a failure.
//
// REPORTED: the latencies and the ratio. Making a perf number a pass/fail assertion produces a
// test that goes red on a busy machine and teaches everyone to ignore it. The decision this file
// informs is a human one; its job is to produce the table honestly and to fail only when
// something is actually broken.
//
// THE CONTROL IS THE INTERLEAVE. lane and loPri probes alternate within the same loop against the
// same bulk load, so both arms see the same machine state, the same thermal condition and the same
// background noise. Measuring one arm and then the other would compare two different machines.
#include "TaskScheduler.h"
#include "Thread.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-68s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static long long NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch()).count();
}

// ---- THE BULK BODY, CALIBRATED AT RUNTIME ---------------------------------------------------
//
// NOT A FIXED ITERATION COUNT. The whole point is to compare grains -- 5 us against 400 us -- so
// the body has to be that long on THIS machine, not on the one the constant was tuned on. A
// hardcoded count makes the sweep measure the developer's old CPU.
static std::atomic<unsigned long long> g_bulkIters{ 1000 };
static std::atomic<unsigned long long> g_sink{ 0 };

static unsigned long long Burn(unsigned long long iters) {
    unsigned long long x = 88172645463325252ull;
    for (unsigned long long i = 0; i < iters; ++i) { x ^= x << 13; x ^= x >> 7; x ^= x << 17; }
    return x;
}

// IN-FLIGHT COUNTED IN THE BODY, NOT BY A WAITGROUP. See the producer: the point is to keep the
// pool continuously full, and a WaitGroup per batch forces the producer to stop pushing until the
// batch drains. Only globals are touched, so nothing here outlives its storage.
static std::atomic<int> g_bulkInFlight{ 0 };

static void BulkBody(void*) {
    g_sink.fetch_add(Burn(g_bulkIters.load(std::memory_order_relaxed)), std::memory_order_relaxed);
    g_bulkInFlight.fetch_sub(1, std::memory_order_release);
}

// Iterations per microsecond, measured once. Warmed first so the first timing does not pay for
// cold branch predictors and a cold clock.
static double CalibrateItersPerUs() {
    Burn(200000);
    const long long t0 = NowNs();
    const unsigned long long r = Burn(2000000);
    const long long t1 = NowNs();
    g_sink.fetch_add(r, std::memory_order_relaxed);
    const double ns = (double)(t1 - t0);
    if (ns <= 0.0) return 1000.0;
    return 2000000.0 / (ns / 1000.0);
}

// ---- THE PROBE ------------------------------------------------------------------------------
//
// One slot, because probes are strictly serial: push one, wait for it, record, push the next. That
// is the right shape for "how long does A completion wait when the pool is saturated" -- adding a
// window would fold queueing against OTHER PROBES into a number that is supposed to be about
// queueing against BULK.
struct ProbeSlot {
    std::atomic<long long> startNs{ 0 };
    long long              postNs = 0;
};

static void ProbeBody(void* p) {
    static_cast<ProbeSlot*>(p)->startNs.store(NowNs(), std::memory_order_release);
}

static double Pct(std::vector<double>& v, double q) {
    if (v.empty()) return 0.0;
    size_t i = (size_t)(v.size() * q);
    if (i >= v.size()) i = v.size() - 1;
    return v[i];
}

struct ArmResult {
    std::vector<double> lat;
    double p50 = 0.0, p99 = 0.0, max = 0.0;
};

int main(int argc, char** argv) {
    // K from the command line so the same binary answers "with the reserved band" and "without".
    // Default 2: the configuration the recorded result was arguing FOR, so the default run is the
    // one that has something to defend.
    size_t K = 2;
    if (argc > 1) {
        const long v = std::strtol(argv[1], nullptr, 10);
        if (v >= 0 && v < 32) K = (size_t)v;
    }

    // ARGV[2] = NEVER-PARK, and it is not optional for a fair reading. Without it the reserved band
    // SLEEPS between completions, so every lane probe pays a ~3 us kernel wake and the lane's p50
    // measures the park primitive rather than the placement. That is a real configuration -- it is
    // the default -- but comparing a parking lane against an awake pool answers a question nobody
    // asked.
    bool neverPark = false;
    if (argc > 2) neverPark = std::strtol(argv[2], nullptr, 10) != 0;

    // ARGV[3] = FLOOR LANE. The third configuration, and the one the recorded result was actually
    // about: lane routing ARMED but aimed at the awake floor instead of a reserved band. Only
    // meaningful at K = 0 -- with K > 0 the reserved branch wins in PickNextWorker and this changes
    // nothing, which the banner below says out loud rather than letting a run look like a result.
    bool floorLane = false;
    if (argc > 3) floorLane = std::strtol(argv[3], nullptr, 10) != 0;

    if (K > 0) JLib::TaskScheduler::SetHotWorkers(K);
    if (neverPark) JLib::TaskScheduler::SetReservedNeverParks(true);
    if (floorLane) JLib::TaskScheduler::SetHiPriFloorLane(true);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();

    const size_t workers = sched.GetWorkerCount();
    std::printf("io overlap -- completion latency against a saturated pool\n");
    std::printf("workers=%zu  K=%zu  floor=%zu  neverpark=%s  floorlane=%s\n",
                workers, JLib::TaskScheduler::GetHotWorkers(),
                JLib::TaskScheduler::GetAwakeFloorBase(),
                neverPark ? "on" : "off", floorLane ? "on" : "off");

    // SAY WHICH CONFIGURATION THIS ACTUALLY IS. Three flags produce combinations that silently
    // collapse onto each other, and a run whose banner does not distinguish them is how one
    // configuration gets reported under another's name.
    if (floorLane && JLib::TaskScheduler::GetHotWorkers() > 0)
        std::printf("  NOTE: floorlane is INERT at K>0 -- the reserved branch wins in\n"
                    "        PickNextWorker. This run is the plain reserved lane.\n");
    else if (floorLane)
        std::printf("  arms: lane -> awake floor [0,%zu), NOT reserved -- lands on a worker that\n"
                    "        is awake but may be inside a bulk body.\n",
                    JLib::TaskScheduler::GetAwakeFloorBase());
    else if (JLib::TaskScheduler::GetHotWorkers() == 0)
        std::printf("  arms: NEGATIVE CONTROL -- K=0 and no floor lane, so HiPriLaneActive() is\n"
                    "        false and both arms are the same code path. Expect 1.00x.\n");
    std::printf("\n");

    if (workers < 4) {
        std::printf("  SKIPPED: needs at least 4 workers to saturate and still probe.\n");
        JLib::detail::TeardownForTesting(sched);
        return 0;
    }

    const double itersPerUs = CalibrateItersPerUs();
    std::printf("calibration: %.0f iterations per microsecond\n\n", itersPerUs);

    // ---- THE WATCHDOG IS THE REAL ASSERTION -------------------------------------------------
    //
    // A stranded completion does not return a bad number, it never returns -- WaitFor parks
    // forever on a task in an inbox whose only legal consumer has stopped consuming. Without this
    // the failure mode of the thing being tested is an indefinitely hung CI job, which reads as
    // infrastructure trouble rather than as this test failing.
    std::atomic<bool> finished{ false };
    std::thread watchdog([&] {
        for (int i = 0; i < 1200 && !finished.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!finished.load(std::memory_order_acquire)) {
            std::printf("\n  *** HUNG: a probe never completed. That is a STRANDED completion --\n"
                        "      an inbox has one legal consumer and it stopped consuming. ***\n");
            std::fflush(nullptr);
            std::_Exit(2);
        }
    });

    const int bodyUs[] = { 5, 20, 400 };
    constexpr int kProbesPerArm = 500;
    constexpr int kBulkBatch    = 256;

    std::printf("%-10s %-28s %-28s %s\n", "body", "lane p50/p99/max (us)",
                "loPri p50/p99/max (us)", "loPri/lane  (>1 = lane wins)");
    std::printf("---------------------------------------------------------------------"
                "------------------------------------\n");

    for (int b = 0; b < 3; ++b) {
        const int us = bodyUs[b];
        g_bulkIters.store((unsigned long long)(itersPerUs * (double)us),
                          std::memory_order_relaxed);

        // ---- SATURATE, CONTINUOUSLY -------------------------------------------------------
        //
        // BOUNDED IN-FLIGHT, NOT BATCH-AND-WAIT, and the difference is a defect this test had. The
        // producer used to push a WaitGroup batch and block on it before pushing the next, which
        // leaves the pool EMPTY at every batch boundary. That hole is negligible next to a 5 us
        // body and is most of the window next to a 400 us one, so the coarse row silently measured
        // an idle pool: two consecutive runs read 3584 and 1792 bulk bodies, and the second scored
        // loPri p99 at 0.90 us -- a saturation row with nothing to be saturated by.
        //
        // Counting in-flight in the body instead lets the producer stay ahead of the pool with no
        // synchronisation point at all. The pool is full for the whole probe window at every grain,
        // which is the one thing this file needs to be true.
        //
        // NO WAITGROUP ON BULK. Nothing waits for these; the drain below is what makes them safe to
        // leave running, and the body touches only globals.
        //
        // Pushed with PushBatch and lane=false: this is bulk, it must spread, and it must never
        // take the lane. Routing it lane would make the probe compete with itself.
        std::atomic<bool> stop{ false };
        std::atomic<long long> bulkDone{ 0 };
        const int kInFlightCap = (int)workers * 8;
        std::thread bulk([&] {
            std::vector<JLib::Task*> arr(kBulkBatch);
            while (!stop.load(std::memory_order_relaxed)) {
                if (g_bulkInFlight.load(std::memory_order_acquire) >= kInFlightCap) {
                    std::this_thread::yield();
                    continue;
                }
                int made = 0;
                for (int i = 0; i < kBulkBatch; ++i) {
                    JLib::Task* t = sched.CreateTask(BulkBody, nullptr, /*lane*/ JLib::Lane::Normal);
                    if (!t) break;
                    arr[i] = t;
                    ++made;
                }
                if (made == 0) { std::this_thread::yield(); continue; }
                // INCREMENT BEFORE THE PUSH. A task can run and decrement the instant it is
                // visible, so counting up afterwards can drive the counter negative and uncap the
                // producer -- which is the bug this cap exists to prevent, wearing a disguise.
                g_bulkInFlight.fetch_add(made, std::memory_order_release);
                sched.PushBatch(arr.data(), (size_t)made, 0, 64, /*lane*/ JLib::Lane::Normal);
                bulkDone.fetch_add(made, std::memory_order_relaxed);
            }
        });

        // Let the pool actually fill before probing. Measuring the ramp is measuring the ramp.
        std::this_thread::sleep_for(std::chrono::milliseconds(30));

        const unsigned long long strand0 = JLib::TaskScheduler::GetLaneStrandCount();
        const unsigned long long idleK0  = JLib::TaskScheduler::GetLaneStrandIdlePeerCount();
        const unsigned long long idleW0  = JLib::TaskScheduler::GetLaneStrandIdleWideCount();

        ArmResult hi, lo;
        hi.lat.reserve(kProbesPerArm);
        lo.lat.reserve(kProbesPerArm);
        int completed = 0;

        // ---- PROBE FROM A SIDE THREAD, NOT A WORKER ---------------------------------------
        //
        // A completion pushed from inside the pool is a different code path -- it can be placed by
        // a thread that is itself a legal consumer, and Push behaves differently for workers. The
        // real reactor is an OS completion thread, so this is one.
        std::thread probe([&] {
            for (int i = 0; i < kProbesPerArm * 2; ++i) {
                const bool useHi = (i & 1) == 0;
                ProbeSlot slot;
                JLib::WaitGroup wg;
                wg.n.store(1, std::memory_order_relaxed);
                JLib::Task* t = sched.CreateTask(ProbeBody, &slot, useHi ? JLib::Lane::LowLatency : JLib::Lane::Normal);
                if (!t) continue;
                t->waitGroup = &wg;
                slot.postNs = NowNs();
                sched.Push(t);
                sched.WaitFor(wg);
                ++completed;
                const long long s = slot.startNs.load(std::memory_order_acquire);
                if (s > slot.postNs) {
                    const double d = (double)(s - slot.postNs) / 1000.0;
                    (useHi ? hi.lat : lo.lat).push_back(d);
                }
            }
        });
        probe.join();

        stop.store(true, std::memory_order_relaxed);
        bulk.join();

        // DRAIN BEFORE MOVING TO THE NEXT BODY LENGTH. Bulk carries no WaitGroup, so joining the
        // producer only stops new pushes -- leftovers would still be running while the next row
        // changes g_bulkIters underneath them, and that row's "5 us" bodies would be however long
        // the previous row's were. The watchdog covers the pathological case of this never settling.
        while (g_bulkInFlight.load(std::memory_order_acquire) > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        const unsigned long long strands = JLib::TaskScheduler::GetLaneStrandCount()         - strand0;
        const unsigned long long idleK   = JLib::TaskScheduler::GetLaneStrandIdlePeerCount() - idleK0;
        const unsigned long long idleW   = JLib::TaskScheduler::GetLaneStrandIdleWideCount() - idleW0;

        for (ArmResult* a : { &hi, &lo }) {
            std::sort(a->lat.begin(), a->lat.end());
            a->p50 = Pct(a->lat, 0.50);
            a->p99 = Pct(a->lat, 0.99);
            a->max = a->lat.empty() ? 0.0 : a->lat.back();
        }
        const double r50 = (hi.p50 > 0.0) ? lo.p50 / hi.p50 : 0.0;
        const double r99 = (hi.p99 > 0.0) ? lo.p99 / hi.p99 : 0.0;

        char hiCol[64], loCol[64];
        std::snprintf(hiCol, sizeof(hiCol), "%6.2f /%7.2f /%8.2f", hi.p50, hi.p99, hi.max);
        std::snprintf(loCol, sizeof(loCol), "%6.2f /%7.2f /%8.2f", lo.p50, lo.p99, lo.max);
        std::printf("%-4d us   %-28s %-28s p50 %.2fx  p99 %.2fx\n",
                    us, hiCol, loCol, r50, r99);
        std::printf("           bulk bodies run: %lld   strands: %llu (idle in K: %llu, in K+F: %llu)\n",
                    (long long)bulkDone.load(std::memory_order_relaxed), strands, idleK, idleW);

        // THE ONLY HARD ASSERTION. Both arms must have produced every probe; a missing one means a
        // task was created and never ran, which the watchdog would have caught as a hang unless it
        // was lost silently.
        char what[96];
        std::snprintf(what, sizeof(what), "body %d us: all %d probes completed",
                      us, kProbesPerArm * 2);
        Check(completed == kProbesPerArm * 2, what);
    }

    finished.store(true, std::memory_order_release);
    watchdog.join();

    std::printf("\nHOW TO READ THE RATIO. >1 means the lane arm was faster, i.e. the lane earned\n"
                "its place. <1 means an unsteered ordinary push beat it -- which is what the\n"
                "recorded result found at 400 us, because steering concentrates every completion on\n"
                "the few floor workers while an ordinary push spreads across the whole pool. Compare\n"
                "the 5 and 20 us rows against the 400 us row: if the ratio crosses 1.0 as the body\n"
                "shrinks, head-of-line blocking is a property of the GRAIN and not of the design.\n"
                "\nRun with an argument to set K (0 = no reserved band at all): io_overlap_test 0\n");

    std::printf("\n%s\n", g_fail ? "FAILED" : "PASSED");
    JLib::detail::TeardownForTesting(sched);
    return g_fail ? 1 : 0;
}
