// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// WHY THIS EXISTS
//
// 2.7.0 put a bounded plain spin in front of the bare-thread contended paths of SchedulerMutex::Lock
// and SchedulerSemaphore::Wait: try, CpuRelax a bounded number of times, and only then escalate to
// ContendedSpinStep (which attempts a real steal via TryRunStolenNativeTask before backing off).
// The spin bound -- kFastSpinTries in src/TaskScheduler.cpp -- was picked as "well under
// kIdleSpinsBeforeYield", which is not a measurement. This decides it.
//
// THE THING THAT MAKES THIS NON-OBVIOUS. A larger spin bound can improve lock acquisition LATENCY
// while making whole-system THROUGHPUT worse, because every spin iteration is a cycle the waiting
// thread did not spend running a stolen task. Measuring only the lock and declaring victory would
// be exactly the wrong call. Every scenario reports both, and reports the pool's completed
// background work as a first-class number rather than a footnote.
//
// There was no SchedulerMutex/SchedulerSemaphore benchmark in this repo before this file --
// bench/bench.cpp covers push/steal/ParallelFor and never takes a lock -- so the constant had
// nothing to be tuned against even in principle.
//
// WHY THE ARMS ROTATE INSIDE ONE PROCESS (and why the first version of this file was thrown away)
//
// kFastSpinTries ships as a compile-time constant, so the obvious harness builds one binary per
// candidate value and runs them in turn. That was built first and it DID NOT WORK. Measured against
// an A/A control -- the same source and flags built twice under two labels -- process-to-process
// drift on this machine reached a p90 of 52% and a max of 118% on lock throughput, and the fiber
// control arm (which never touches the spin path and must be flat by construction) moved up to
// 246%. Nothing the constant could plausibly do survives a floor like that, and more repetitions do
// not help when the variance is between processes rather than within one.
//
// So the value is made settable at runtime -- ONLY in a build configured with
// -DJLIBSCHED_TUNABLE_FAST_SPIN=ON -- and every arm runs inside a single process, rotating round by
// round. Whatever the machine is doing during a round is then shared by all six arms instead of
// being charged to whichever one happened to launch into it. The rotation's starting offset shifts
// each round so no arm keeps the same position in the order.
//
// WHAT IS SWEPT (the axes, per the review that prompted this)
//   critical section  none / tiny(0ns) / short(200ns) / moderate(2us) / long(50us)
//   contenders        1 (uncontended) / 2 / 8 / 16
//   caller kind       bare thread (uses the spin path) / fiber (suspends -- NEGATIVE CONTROL)
//   background load   off (nothing to steal) / on (a saturated pool, so helping is productive)
//   pool size         command line; run the binary twice to cover two worker counts
//
// THE TWO CONTROLS, without which this measures nothing:
//   '64' and '64ctl' are the SAME VALUE run twice per round. Their gap is pure noise and is printed
//   per scenario as that scenario's floor. An arm that does not move further than 64ctl did has not
//   shown an effect, however good its number looks.
//
//   The FIBER rows contend from fibers, which suspend and never enter SpinThenHelp at all. They
//   must stay flat across arms. If they do not, the harness is reading the machine and no
//   bare-thread delta in this file can be believed.
//
// The background-load axis separates two genuinely different regimes. With nothing to steal,
// ContendedSpinStep's steal attempt is pure waste and the fast spin can only help. With a saturated
// pool that attempt is productive work, and the fast spin delays it. An arm that wins one and loses
// the other is the expected outcome, not a failed experiment. The load is started ONCE PER SCENARIO
// and left running across every arm and round, so all arms see the same pool state rather than each
// paying its own spin-up.
//
// BUILD
//   cmake -B build-lockbench -DJLIBSCHED_TUNABLE_FAST_SPIN=ON
//   cmake --build build-lockbench --config Release --target SchedulerLockBench
// RUN
//   SchedulerLockBench.exe [poolSize] [windowMs] [rounds]
//     poolSize  0 = auto (hardware_concurrency-1). Default 0.
//     windowMs  measurement window per pass. Default 120.
//     rounds    rotations through every arm. Default 7.
//
// RESULTS (2026-08-23, Windows, 9 rounds, 150 ms window, run at 8 and 31 workers)
//
// THE FAST SPIN IS A REGRESSION AND WAS REVERTED TO 0. It is monotonic in the spin count -- every
// increase makes things worse -- and 0 wins lock latency, lock throughput and POOL throughput at the
// same time, so this never became the latency-versus-throughput trade it was built to arbitrate.
//
//   tiny critical section, 8 bare contenders, saturated pool, 8 workers
//     arm        p50        p99         acq/s     pool tasks/s
//     0            0 ns   38,900 ns   17,789,590      304,750
//     16         400 ns   80,100 ns    6,360,572      242,896
//     64       4,400 ns   61,900 ns      715,976      195,384
//     256      4,600 ns   67,200 ns      672,775      190,288
//     1024     5,000 ns   68,200 ns      625,812      171,135
//     [A/A noise floor, 64ctl vs 64: acq -2.9%, bg -8.1%]
//
//   the same shape at 31 workers: spin=0 is 4.44x the lock throughput of spin=64, p50 100 ns vs
//   3,400 ns, p99 75,600 ns vs 257,400 ns, pool throughput +2.2%.
//
// WHERE IT DOES NOT MATTER, which is as informative as where it does: the uncontended rows are flat
// across every arm (Try_Lock succeeds on the first attempt and the spin is never reached) and so are
// the long-critical-section rows (the spin always exhausts, so every arm ends up in
// ContendedSpinStep anyway). The effect is confined to short critical sections under real
// contention, which is exactly the regime the spin was supposed to win.
//
// WHY SPINNING LOSES, most likely: ContendedSpinStep is not just a slower retry, it is BACKOFF. A
// tight CpuRelax loop re-runs Try_Lock's spinLock.test_and_set at full rate, and each of those is a
// write to the very cache line the HOLDER must acquire to finish and release. Spinning harder
// starves the thread you are waiting for. That also explains the otherwise puzzling bg=off rows,
// where 0 still wins by ~2x even though its steal attempt always fails and runs nothing.
//
// CONTROLS, both clean at 8 workers (which is what makes the above believable):
//   A/A     '64' vs '64ctl', same value twice: +0.3% acq, +0.4% bg on the fiber row; worst -8.1%.
//   fiber   every arm within +-1.3% of the baseline, as required -- fibers suspend and never enter
//           the spin path, so any movement there would have meant the harness was reading the
//           machine rather than the code.
//
// CAVEAT: one machine, one OS. The backoff explanation is inferred from the shape of the result, not
// separately instrumented -- nothing here counts cache-line transfers. What IS directly measured is
// that raising the bound is never better on this hardware, in any of the regimes swept.

#include "TaskScheduler.h"

#if !defined(JLIBSCHED_TUNABLE_FAST_SPIN)
#error "SchedulerLockBench requires -DJLIBSCHED_TUNABLE_FAST_SPIN=ON. Without it the spin bound is a \
compile-time constant, the arms cannot be interleaved in one process, and the measurement is \
swamped by process-to-process drift -- see the header comment."
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static inline int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        Clock::now().time_since_epoch()).count();
}

// ---- calibrated busy work ---------------------------------------------------------------------
// A real dependent-op chain, not a sleep and not a clock poll. Sleeping would park the thread and
// measure the OS scheduler instead of the lock; polling a clock inside the critical section would
// put a serialising instruction in the middle of the thing being measured. The multiply chain is
// data-dependent so it cannot be vectorised or hoisted, and the result escapes to a volatile sink
// so it cannot be eliminated outright.
static volatile uint64_t g_sink = 0;

static inline void BusySpin(uint64_t iters) {
    uint64_t x = 0x9E3779B97F4A7C15ull ^ iters;
    for (uint64_t i = 0; i < iters; ++i) {
        x = x * 6364136223846793005ull + 1442695040888963407ull;
    }
    g_sink = x;
}

// Iterations per nanosecond, resolved once at startup. Absolute accuracy barely matters -- a fixed
// iteration count already means the same work in every arm. The calibration exists so the scenario
// names are honest about their magnitudes instead of being raw loop counts.
static double g_itersPerNs = 1.0;

static void Calibrate() {
    // Warm up first: the first pass pays page faults and frequency ramp, and folding that into the
    // divisor would make every subsequent "microsecond" systematically short.
    BusySpin(2'000'000);
    const int64_t t0 = NowNs();
    BusySpin(20'000'000);
    const int64_t t1 = NowNs();
    const double ns = static_cast<double>(t1 - t0);
    g_itersPerNs = ns > 0.0 ? (20'000'000.0 / ns) : 1.0;
}

static inline uint64_t ItersForNs(int ns) {
    if (ns <= 0) return 0;
    const double it = g_itersPerNs * static_cast<double>(ns);
    return it < 1.0 ? 1u : static_cast<uint64_t>(it);
}

// ---- small stats helpers -----------------------------------------------------------------------
static double Median(std::vector<double> v) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return (n % 2) ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

static uint32_t Pct(const std::vector<uint32_t>& sorted, double p) {
    if (sorted.empty()) return 0;
    size_t idx = static_cast<size_t>(p * static_cast<double>(sorted.size() - 1) + 0.5);
    if (idx >= sorted.size()) idx = sorted.size() - 1;
    return sorted[idx];
}

// ---- background load ---------------------------------------------------------------------------
// A feeder thread keeping the pool saturated with ~10us Native tasks. It maintains a bounded number
// in flight rather than pushing a fixed batch up front: a fixed batch that drained mid-scenario
// would silently turn the "loaded" arm into the "idle" arm partway through, and nothing in the
// output would say so.
struct BackgroundLoad {
    std::atomic<uint64_t> done{ 0 };
    std::atomic<uint64_t> pushed{ 0 };
    std::atomic<bool>     stop{ false };
    std::atomic<uint64_t> failedAllocs{ 0 };
    std::thread           feeder;
    bool                  active = false;

    void Start(JLib::TaskScheduler& sched, size_t inFlightTarget) {
        active = true;
        const uint64_t work = ItersForNs(10'000);
        feeder = std::thread([this, &sched, inFlightTarget, work] {
            while (!stop.load(std::memory_order_relaxed)) {
                const uint64_t out = pushed.load(std::memory_order_relaxed)
                                   - done.load(std::memory_order_relaxed);
                if (out >= inFlightTarget) {
                    std::this_thread::yield();
                    continue;
                }
                auto* t = sched.CreateTask([this, work] {
                    BusySpin(work);
                    done.fetch_add(1, std::memory_order_release);
                });
                if (!t) {                                   // slab exhausted -- record, don't spin hot
                    failedAllocs.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::yield();
                    continue;
                }
                pushed.fetch_add(1, std::memory_order_relaxed);
                sched.Push(t);
            }
        });
    }

    // Drains before returning: an in-flight task's lambda writes to `done`, which lives in this
    // object, so returning while any is still queued would be a use-after-free.
    void Stop(JLib::TaskScheduler& sched) {
        if (!active) return;
        stop.store(true, std::memory_order_relaxed);
        if (feeder.joinable()) feeder.join();
        const int64_t deadline = NowNs() + 60LL * 1000 * 1000 * 1000;
        while (done.load(std::memory_order_acquire) < pushed.load(std::memory_order_acquire)) {
            if (NowNs() > deadline) {
                std::fprintf(stderr, "\nFATAL: background load failed to drain (%llu pushed, %llu done)\n",
                             (unsigned long long)pushed.load(), (unsigned long long)done.load());
                std::abort();
            }
            sched.TryRunStolenNativeTask();
        }
        active = false;
    }
};

// ---- scenarios ---------------------------------------------------------------------------------
struct Scenario {
    const char* name;
    int  csNs;          // critical section length
    int  contenders;
    bool fiberCallers;  // true = contend from fiber tasks (control: never uses the spin path)
    bool bgLoad;
};

struct ArmSpec { const char* label; int spin; };

// '64' and '64ctl' are deliberately identical -- the A/A control. Everything else brackets it,
// including 0, which restores the pre-2.7.0 behaviour of attempting a steal on the first failed try.
static const ArmSpec kArms[] = {
    { "0",     0 },
    { "16",    16 },
    { "64",    64 },
    { "64ctl", 64 },
    { "256",   256 },
    { "1024",  1024 },
};
static constexpr size_t kNumArms = sizeof(kArms) / sizeof(kArms[0]);

struct RunResult {
    uint64_t acquisitions = 0;
    uint64_t bgDone       = 0;
    double   seconds      = 0.0;
    uint32_t p50 = 0, p99 = 0;
};

struct RunState {
    JLib::SchedulerMutex  mtx;
    std::atomic<bool>     stop{ false };
    std::atomic<uint64_t> acquisitions{ 0 };
    std::atomic<bool>     go{ false };
    uint64_t              csIters = 0;
    bool                  measureLatency = false;
};

// Identical body for both caller kinds, which is the point: the only difference between the arms is
// the CONTEXT it runs in (bare thread vs fiber), because that is what selects the suspend path over
// the spin path inside SchedulerMutex::Lock.
static void ContendLoop(RunState& st, std::vector<uint32_t>& latOut) {
    while (!st.go.load(std::memory_order_acquire)) std::this_thread::yield();

    uint64_t local = 0;
    if (st.measureLatency) {
        while (!st.stop.load(std::memory_order_relaxed)) {
            const int64_t t0 = NowNs();
            st.mtx.Lock();
            const int64_t t1 = NowNs();
            BusySpin(st.csIters);
            st.mtx.Unlock();
            ++local;
            // Bounded so a long run cannot balloon into gigabytes. Past the cap the loop keeps
            // running -- so the load on the lock does not change halfway through -- but stops
            // recording.
            if (latOut.size() < (1u << 19)) {
                const int64_t d = t1 - t0;
                latOut.push_back(static_cast<uint32_t>(d < 0 ? 0 : d));
            }
        }
    }
    else {
        while (!st.stop.load(std::memory_order_relaxed)) {
            st.mtx.Lock();
            BusySpin(st.csIters);
            st.mtx.Unlock();
            ++local;
        }
    }
    st.acquisitions.fetch_add(local, std::memory_order_relaxed);
}

// `bg` is started and stopped by the CALLER, once per scenario, so every arm and round sees the
// same steady-state pool rather than paying its own spin-up.
static RunResult RunOne(JLib::TaskScheduler& sched, const Scenario& sc,
                        int windowMs, bool measureLatency, BackgroundLoad& bg) {
    RunState st;
    st.csIters = ItersForNs(sc.csNs);
    st.measureLatency = measureLatency;

    std::vector<std::vector<uint32_t>> lat(static_cast<size_t>(sc.contenders));
    if (measureLatency) for (auto& v : lat) v.reserve(1 << 14);

    std::vector<std::thread> threads;
    JLib::WaitGroup wg;
    if (sc.fiberCallers) {
        wg.n.store(sc.contenders, std::memory_order_relaxed);
        for (int i = 0; i < sc.contenders; ++i) {
            auto* t = sched.CreateTask(
                [&st, &lat, i] { ContendLoop(st, lat[static_cast<size_t>(i)]); },
                /*hipri*/ false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            if (!t) {
                std::fprintf(stderr, "FATAL: could not allocate fiber contender task\n");
                std::abort();
            }
            t->waitGroup = &wg;
            sched.Push(t);
        }
    }
    else {
        threads.reserve(static_cast<size_t>(sc.contenders));
        for (int i = 0; i < sc.contenders; ++i) {
            threads.emplace_back([&st, &lat, i] { ContendLoop(st, lat[static_cast<size_t>(i)]); });
        }
    }

    // `go` is released only after every contender exists, so the window covers steady-state
    // contention rather than a ragged start where the first thread runs uncontended.
    std::this_thread::sleep_for(std::chrono::milliseconds(3));
    const uint64_t bgAtStart = bg.done.load(std::memory_order_acquire);
    const int64_t  t0 = NowNs();
    st.go.store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(windowMs));
    st.stop.store(true, std::memory_order_relaxed);
    const int64_t  t1 = NowNs();
    const uint64_t bgAtEnd = bg.done.load(std::memory_order_acquire);

    if (sc.fiberCallers) sched.WaitFor(wg);
    else                 for (auto& th : threads) th.join();

    RunResult r;
    r.acquisitions = st.acquisitions.load(std::memory_order_relaxed);
    r.bgDone  = bgAtEnd - bgAtStart;
    r.seconds = static_cast<double>(t1 - t0) / 1e9;

    if (measureLatency) {
        std::vector<uint32_t> all;
        for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
        std::sort(all.begin(), all.end());
        r.p50 = Pct(all, 0.50);
        r.p99 = Pct(all, 0.99);
    }
    return r;
}

// Per-arm samples for one scenario, one value per round.
struct ArmSamples {
    std::vector<double> acqPerSec, bgPerSec, p50, p99;
};

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    const size_t poolSize = (argc > 1) ? static_cast<size_t>(std::atoi(argv[1])) : 0;
    const int    windowMs = (argc > 2) ? std::atoi(argv[2]) : 120;
    const int    rounds   = (argc > 3) ? std::atoi(argv[3]) : 7;

    Calibrate();
    JLib::TaskScheduler::Init(poolSize);
    auto& sched = JLib::TaskScheduler::Instance();
    const size_t workers = sched.GetWorkerCount();

    std::printf("JLib::Scheduler lock contention -- workers=%zu  window=%dms  rounds=%d  "
                "calib=%.2f iters/ns\n", workers, windowMs, rounds, g_itersPerNs);
    std::printf("arms rotate INSIDE this process; '64' and '64ctl' are the same value "
                "(their gap is the noise floor)\n\n");

    // Deliberately a short, targeted list rather than a full cross product. Every axis the review
    // asked for is represented, but contention regimes that cannot distinguish the arms -- a 50us
    // critical section at every contender count, say, where the spin always exhausts and escalates
    // regardless -- get one row instead of six. Rounds bought with the saved time are worth more
    // than rows, because the limiting factor here is variance, not coverage.
    const Scenario scenarios[] = {
        // uncontended: Try_Lock succeeds first attempt, so the spin bound is unreachable. Any
        // movement across arms here is noise by construction -- a second control, for free.
        { "uncontended",      200,   1, false, false },
        { "uncontended",      200,   1, false, true  },
        // tiny/short critical sections: the fast spin's best case, because the holder releases
        // within a few cycles and a plain retry can catch it.
        { "tiny-cs(0ns)",     0,     2, false, false },
        { "tiny-cs(0ns)",     0,     2, false, true  },
        { "tiny-cs(0ns)",     0,     8, false, true  },
        { "tiny-cs(0ns)",     0,    16, false, true  },
        { "short-cs(200ns)",  200,   2, false, false },
        { "short-cs(200ns)",  200,   2, false, true  },
        { "short-cs(200ns)",  200,   8, false, true  },
        { "short-cs(200ns)",  200,  16, false, true  },
        // moderate and long: the spin should exhaust and escalate, so these exist to show the cost
        // of spinning where it cannot pay off.
        { "moderate-cs(2us)", 2000,  8, false, false },
        { "moderate-cs(2us)", 2000,  8, false, true  },
        { "long-cs(50us)",    50000, 8, false, false },
        { "long-cs(50us)",    50000, 8, false, true  },
        // fiber control -- suspends instead of spinning, must be flat.
        { "tiny-cs(0ns)",     0,     8, true,  true  },
    };
    const size_t kNumScenarios = sizeof(scenarios) / sizeof(scenarios[0]);

    for (size_t si = 0; si < kNumScenarios; ++si) {
        const Scenario& sc = scenarios[si];

        BackgroundLoad bg;
        if (sc.bgLoad) {
            bg.Start(sched, workers * 4);
            // Let the pool reach steady state before the first arm is measured, so arm 0 of round 0
            // is not charged the ramp.
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        ArmSamples samples[kNumArms];

        for (int round = 0; round < rounds; ++round) {
            for (size_t k = 0; k < kNumArms; ++k) {
                // Rotate the starting offset each round so no arm keeps the same position in the
                // order -- otherwise whichever arm always runs first inherits whatever the previous
                // round left behind.
                const size_t ai = (k + static_cast<size_t>(round)) % kNumArms;
                JLib::detail::SetFastSpinTries(kArms[ai].spin);

                const RunResult lat = RunOne(sched, sc, windowMs, /*measureLatency*/ true,  bg);
                const RunResult thr = RunOne(sched, sc, windowMs, /*measureLatency*/ false, bg);

                samples[ai].acqPerSec.push_back(thr.seconds > 0 ? thr.acquisitions / thr.seconds : 0.0);
                samples[ai].bgPerSec .push_back(thr.seconds > 0 ? thr.bgDone / thr.seconds : 0.0);
                samples[ai].p50      .push_back(lat.p50);
                samples[ai].p99      .push_back(lat.p99);
            }
        }

        if (sc.bgLoad) bg.Stop(sched);
        JLib::detail::SetFastSpinTries(64);   // leave the default in place between scenarios

        // ---- report ----
        std::printf("%s  contenders=%d  caller=%s  bg=%s\n",
                    sc.name, sc.contenders, sc.fiberCallers ? "fiber" : "bare",
                    sc.bgLoad ? "on" : "off");
        std::printf("  %-7s %10s %10s %12s %12s   %8s %8s\n",
                    "arm", "p50ns", "p99ns", "acq/s", "bgtask/s", "acqR", "bgR");

        double base[4] = { 0, 0, 0, 0 };   // medians of the '64' arm: acq, bg, p50, p99
        for (size_t ai = 0; ai < kNumArms; ++ai) {
            if (std::string(kArms[ai].label) != "64") continue;
            base[0] = Median(samples[ai].acqPerSec);
            base[1] = Median(samples[ai].bgPerSec);
            base[2] = Median(samples[ai].p50);
            base[3] = Median(samples[ai].p99);
        }

        double ctlAcqRatio = 1.0, ctlBgRatio = 1.0;
        for (size_t ai = 0; ai < kNumArms; ++ai) {
            const double a = Median(samples[ai].acqPerSec);
            const double b = Median(samples[ai].bgPerSec);
            const double m50 = Median(samples[ai].p50);
            const double m99 = Median(samples[ai].p99);
            const double ar = base[0] > 0 ? a / base[0] : 0.0;
            const double br = base[1] > 0 ? b / base[1] : 0.0;

            if (std::string(kArms[ai].label) == "64ctl") { ctlAcqRatio = ar; ctlBgRatio = br; }

            // bg ratios are meaningless with no background load, so they print as '-' rather than
            // as 0.000 -- a zero there reads like a catastrophic regression instead of "not measured".
            char brBuf[16];
            if (sc.bgLoad) std::snprintf(brBuf, sizeof brBuf, "%8.3f", br);
            else           std::snprintf(brBuf, sizeof brBuf, "%8s", "-");
            std::printf("  %-7s %10.0f %10.0f %12.0f %12.0f   %8.3f %s\n",
                        kArms[ai].label, m50, m99, a, b, ar, brBuf);
            // Archival CSV: scenario,csNs,contenders,caller,bg,arm,p50,p99,acqPerSec,bgPerSec
            std::printf("CSV,%zu,%s,%d,%d,%s,%s,%s,%.0f,%.0f,%.0f,%.0f\n",
                        workers, sc.name, sc.csNs, sc.contenders,
                        sc.fiberCallers ? "fiber" : "bare", sc.bgLoad ? "on" : "off",
                        kArms[ai].label, m50, m99, a, b);
        }

        // The floor, restated per scenario. Reading any arm's ratio without this number next to it
        // is how a 2% "win" gets shipped out of a 10% spread.
        if (sc.bgLoad) {
            std::printf("  [noise floor, 64ctl vs 64:  acq %+.1f%%   bg %+.1f%%]\n\n",
                        (ctlAcqRatio - 1.0) * 100.0, (ctlBgRatio - 1.0) * 100.0);
        }
        else {
            std::printf("  [noise floor, 64ctl vs 64:  acq %+.1f%%]\n\n",
                        (ctlAcqRatio - 1.0) * 100.0);
        }
    }

    return 0;
}
