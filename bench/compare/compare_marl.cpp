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
//               TryRunStolenNativeTask (Native tasks only).
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

// Set in main after construction. BenchIdleTax ticks from a thread it spawns, and marl requires a
// scheduler bound on whatever thread calls schedule(); main.s bind does not carry to it.
static marl::Scheduler* g_marlSched = nullptr;
static bool g_doJ = true;
static bool g_doM = true;

// THE TASK TYPE EVERY UNTAGGED JLib ROW SUBMITS, switched by the `fiberonly` flag.
//
// WHY THIS MAKES THE COMPARISON FAIRER RATHER THAN JUST DIFFERENT. Read the mixed-workload note
// further down: marl fiber-backs EVERY task, and in the default configuration here only
// TaskType::Fiber tasks do, so three quarters of this harness skips the fiber pool entirely. That
// asymmetry is not an accident -- it IS the hybrid's claim, and those rows exist to measure it.
//
// But it means those rows have never compared the two schedulers' FIBER paths against each other,
// only JLib's Native fast path against marl's fiber path. `fiberonly` is the like-for-like read:
// every task fiber-backed on both sides. Expect it to be SLOWER than the default rows, and expect
// that to mean nothing bad -- it is a different question, not a worse score.
//
// (Historically also forced by Mode::FiberOnly, which rejected Native outright -- removed in 4.0.2)
// and says so loudly. Submitting one would abort the run.
static JLib::TaskType g_jlType = JLib::TaskType::Native;
static constexpr int kRuns      = 7;
static constexpr int kWorkIters = 200;

// ---------------------------------------------------------------- independent-task throughput
// marl::schedule + WaitGroup is a direct structural match for JLib's CreateTask/Push + WaitGroup --
// closer than either of the other two libraries gets. The difference measured here is therefore
// close to a like-for-like cost of the two implementations of the same idea, except that every marl
// task lands on a fiber while JLib's default Native tasks do not.
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
                    }, (void*)(intptr_t)i, false, g_jlType);
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
                    }, (void*)(intptr_t)i, false, g_jlType);
                    if (!batch[i]) return;
                    batch[i]->waitGroup = &wg;
                }
                jl.PushBatch(batch.data(), (size_t)n);
                jl.WaitFor(wg);
                ab.push_back(Ms(t0, Clock::now()) * 1e6 / n);
            }
            // PushArray BUILDS ITS OWN TASKS and does not take a type, so they are Native -- which
            // (Mode::FiberOnly used to reject it outright and fatally.) The row sits out rather than aborting
            // the run, and `pa` staying empty prints `--` instead of a number that was never taken.
            // Not a gap worth closing here: the row measures a bulk-submission API, and the
            // fiberonly question is about the per-task fiber path.
            if (g_jlType != JLib::TaskType::Fiber) {
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
                }, nullptr, false, g_jlType);
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
//      marl fiber-backs EVERY task; here only TaskType::Fiber tasks do, and this benchmark is 25%
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
// RESULT. Two runs, two refuted hypotheses, and the answer is in neither of them.
//
//     block us    JLib   marl  ratio        (second run, Event& overload -- no registry lookup)
//            0   13.54   4.58  0.34x
//           50    6.49   4.91  0.76x
//          150    6.70   5.22  0.78x
//          300    6.32   5.50  0.87x
//          600    8.08   8.57  1.06x
//         2000   22.00  23.72  1.08x
//
// PREDICTION 3 (widest in this scheduler's favour at D=0, narrowing) -- REFUTED AND INVERTED. The
// gap is widest in MARL's favour at D=0 and closes as D grows.
//
// PREDICTION 2 (this wins the mixed workload by the fraction of tasks that skip fibers) -- NOT
// SUPPORTED. The best case anywhere is 1.08x, which is not the effect that was predicted.
//
// PREDICTION 1 (close on suspend/resume) -- CONFIRMED, but only where both libraries actually
// suspend. At D >= 600 the ratio is 1.06-1.08x, which is as close as this harness can resolve.
//
// FIRST HYPOTHESIS FOR THE D=0 GAP, REFUTED: the registry. GetEvent(name) takes a global mutex and
// hashes a string, and the first version paid that twice per blocking task. Adding Event& overloads
// and hoisting the lookup to startup changed D=0 from 11.62 to 13.54 -- i.e. nothing. The overloads
// are worth keeping on their own merits, but they were not the story.
//
// WHAT IT ACTUALLY IS: the two Events are not the same primitive. marl::Event::Shared::wait() is
// cv.wait(lock, []{ return signalled; }) -- a PREDICATE wait over sticky state, so on an already
// signalled Manual event it returns without suspending anything. JLib::Event has no signalled state
// at all; it is a pure rendezvous (SignalAll takes the waiter list), so "already signalled" is not
// representable and WaitOnEventArmed always pays WANTS_SUSPEND + AddWaiter + ContextSwitch out +
// requeue + reschedule + ContextSwitch in.
//
// So D=0 is not a fiber measurement and must not be quoted as one -- it is stateful-event versus
// stateless-rendezvous, and marl skips the work entirely rather than doing it faster. The rows where
// both genuinely suspend are D >= 600, and there they are level.
//
// THE ACTIONABLE PART, for a later release: an already-satisfied wait costing a full suspend/resume
// round trip is a real cost on a real pattern -- a GPU fence that has already completed by the time
// the task looks is the obvious one. Giving Event an optional signalled state, or a predicate
// overload that checks before suspending, would remove it. That changes Event's semantics, so it is
// not a 1.3.x change.
static constexpr int kBatch      = 256;
static constexpr int kBatches    = 10;
static constexpr int kBlockEvery = 4;       // 25% of tasks block
static constexpr int kHeavyIters = 50000;   // ~50 us per non-blocking task

static std::atomic<bool> g_released{ false };

// Looked up ONCE. The point of the Event& overloads: GetEvent takes a global registryMtx and hashes
// a string, and the first version of this benchmark paid that twice per blocking task -- once in
// WaitOnEventArmed and once in the arm callback -- which is what it actually measured.
static JLib::Event* g_ioEvent = nullptr;

// Spun, not slept: sleep_for(20us) is a full ~15.6 ms quantum on Windows, which is how an earlier
// version of the enkiTS harness "measured" a 50 us block at 13 ms per batch.
static void ReleaseAfter(int durationUs, JLib::TaskScheduler* jl, marl::Event* ev) {
    const auto deadline = Clock::now() + std::chrono::microseconds(durationUs);
    while (Clock::now() < deadline) std::this_thread::yield();
    g_released.store(true, std::memory_order_release);
    if (jl) g_ioEvent->SignalAll();
    if (ev) ev->signal();
}

// ---------------------------------------------------------------- idle-policy tax
//
// WHO ACTUALLY PAYS FOR A SPINNING POOL. Every other row here is blind to it by construction: the
// harness is the only thing on the machine, so cores held by idle workers are free and a library
// that never parks looks strictly better. It is not free in the place this library is FOR. A game
// has a render thread, a GPU submit thread and an audio thread that want those cores, and the
// scheduler's idle policy is a tax levied on them.
//
// That blind spot has bitten this project before and in exactly this shape: the synthetic frame DAG
// says NoSleep is 2.9x BETTER (7.8 vs 22.5 us/graph) and the real 2D game says it is 23% WORSE
// (462.0 vs 383.3 ms). Same policy, opposite sign, and the difference is entirely whether anything
// else wanted the cores. Quoting the blocking row without this one repeats that mistake.
//
// THE MEASUREMENT. A victim thread does a FIXED amount of arithmetic and is timed. Alongside it, a
// producer submits a trivial batch every 2 ms -- a frame tick -- so the pool cycles idle -> woken ->
// idle about a hundred times while the victim runs. The submitted work is deliberately negligible
// (~0.2 us a task); anything the victim loses is the IDLE POLICY, not competition for real work.
//
// Read it as the tax: victim-with-pool minus victim-alone. marl spins a full millisecond after every
// one of those ticks, so a 2 ms tick means its workers are hot essentially always.
static constexpr int kVictimIters  = 8000;    // ~20 us each -> ~160 ms of victim work
static constexpr int kVictimSpin   = 20000;
static constexpr int kTickUs       = 2000;
static constexpr int kTasksPerTick = 8;

// MORE THAN ONE VICTIM, BECAUSE ONE CANNOT BE STARVED ON THIS MACHINE. The first version ran a
// single victim thread and measured marl's tax at 1.0% -- real, but far below the 23% the same idle
// policy cost in the actual 2D game. The reason is that 31 spinners on 32 logical cores still leave
// one free, so a lone victim never has to fight for it. A game does not have one other thread; it
// has render, GPU submit, audio and main, and they are all trying to run WHILE the pool is idle.
//
// Four is chosen to make workers + victims exceed the core count, which is the condition under which
// an idle policy costs anything at all. Below that this row measures nothing and says so.
static constexpr int kVictimThreads = 4;

static double RunVictim() {
    auto t0 = Clock::now();
    std::vector<std::thread> v;
    v.reserve(kVictimThreads);
    for (int t = 0; t < kVictimThreads; ++t)
        v.emplace_back([t] {
            uint64_t x = (uint64_t)t + 1;
            for (int i = 0; i < kVictimIters; ++i) x = Spin(x, kVictimSpin);
            g_sink.fetch_add(x, std::memory_order_relaxed);
        });
    for (auto& th : v) th.join();
    return Ms(t0, Clock::now());
}

static void BenchIdleTax(JLib::TaskScheduler& jl) {
    printf("  idle-policy tax -- what the pool costs a CO-RESIDENT thread, ms (lower is better)\n\n");

    // Baseline FIRST and with nothing submitted, so it measures the victim alone on this machine.
    const double base = std::min(RunVictim(), RunVictim());

    JLib::TaskScheduler::ResetAwakeFloorPeak();
    size_t floorLiveAtEnd = 0;
    std::atomic<bool> stop{ false };
    double withPool = -1.0;
    if (g_doJ) {
        std::thread ticker([&] {
            while (!stop.load(std::memory_order_relaxed)) {
                JLib::WaitGroup wg; wg.n.store(kTasksPerTick, std::memory_order_relaxed);
                for (int i = 0; i < kTasksPerTick; ++i) {
                    JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
                    if (!t) break;
                    t->waitGroup = &wg; jl.Push(t);
                }
                jl.WaitFor(wg);
                std::this_thread::sleep_for(std::chrono::microseconds(kTickUs));
            }
        });
        withPool = RunVictim();
        floorLiveAtEnd = JLib::TaskScheduler::GetAwakeFloor();   // BEFORE stopping the ticker
        stop.store(true, std::memory_order_relaxed);
        ticker.join();
    }
    else if (g_doM) {
        std::thread ticker([&] {
            // marl::schedule REQUIRES A BOUND SCHEDULER ON THE CALLING THREAD, and main's bind does
            // not carry to a thread we spawned -- the first version of this row exited 1 on that.
            marl::Scheduler* s = g_marlSched;
            if (!s) return;                 // nothing to bind: leave withPool at -1 and print "--"
            s->bind();
            while (!stop.load(std::memory_order_relaxed)) {
                marl::WaitGroup wg(kTasksPerTick);
                for (int i = 0; i < kTasksPerTick; ++i) marl::schedule([wg] { wg.done(); });
                wg.wait();
                std::this_thread::sleep_for(std::chrono::microseconds(kTickUs));
            }
            marl::Scheduler::unbind();
        });
        withPool = RunVictim();
        stop.store(true, std::memory_order_relaxed);
        ticker.join();
    }

    // THE STRUCTURAL NUMBER, and the one to trust when the timing is noisy. A tick faster than
    // kFloorHoldNs (6 ms) refreshes the grow-hold before the collapse can ever fire, so the floor
    // ratchets to the cap and STAYS there -- the pool is then permanently spinning against the
    // victims, which is precisely the configuration the permanent floor=31 arm measures. `live at
    // end` is read while the ticker is still running, so a value at the cap is the ratchet caught in
    // the act rather than a shed that had not happened yet.
    if (g_doJ)
        printf("     JLib floor during the row: peak %zu, live at end %zu (base %zu, tick %d us)\n",
               JLib::TaskScheduler::GetAwakeFloorPeak(), floorLiveAtEnd,
               JLib::TaskScheduler::GetAwakeFloorBase(), kTickUs);
    printf("     victim alone                       %8.1f ms\n", base);
    printf("     victim with a ticking pool         %8.1f ms\n", withPool);
    if (base > 0 && withPool > 0)
        printf("     TAX                                %8.1f ms  (%.1f%%)\n",
               withPool - base, 100.0 * (withPool - base) / base);
    printf("\n     A pool that parks should read near zero here. A pool that spins after every tick\n"
           "     charges the victim for cores it is not using. This row is why the blocking\n"
           "     crossover must not be quoted on its own.\n\n");
}

static void BenchBlocking(JLib::TaskScheduler& jl) {
    printf("  blocking crossover -- 25%% of tasks wait on an external signal\n");
    printf("     %d batches of %d; ms for all %d tasks, lower is better\n\n",
           kBatches, kBatch, kBatches * kBatch);
    printf("     %8s %11s %11s %9s\n", "block us", "JLib", "marl", "ratio");

    const int durations[] = { 0, 50, 150, 300, 600, 2000 };
    // HOW MUCH OF EACH ROW IS KERNEL WAKES, printed per duration.
    //
    // The note above attributes the D=0 gap to stateful-vs-stateless events, and that IS a real
    // asymmetry -- but it cannot be the whole story, because raising the awake floor to 31 moved
    // D=0 from 10.45 ms to 5.32 ms and event semantics do not change with the floor. The other
    // candidate is the push path: 256 pushes per batch against a pool with floor=2 is up to 254
    // wakes at ~3.5 us each, which is most of a batch on its own.
    //
    // "Slower here" and "woke a parked worker 2560 times" print identically without this number.
    unsigned long long blkWakes = 0;
    for (int d : durations) {
        std::vector<double> a, b;

        if (g_doJ) {
            JLib::TaskScheduler::ResetWakeCount();
            JLib::TaskScheduler::ResetAwakeFloorPeak();
            for (int r = 0; r < 3; ++r) {
                auto t0 = Clock::now();
                for (int batch = 0; batch < kBatches; ++batch) {
                    g_released.store(d == 0, std::memory_order_release);
                    std::thread rel;
                    if (d) rel = std::thread(ReleaseAfter, d, &jl, nullptr);

                    static JLib::Task* blk[kBatch];
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
                                  s.WaitOnEventArmed(*g_ioEvent, [] {
                                      if (g_released.load(std::memory_order_acquire))
                                          g_ioEvent->SignalAll();
                                  });
                              }, nullptr, false, JLib::TaskType::Fiber)
                            : jl.CreateTask(+[](void* p) {
                                  g_sink.fetch_add(Spin((uint64_t)(intptr_t)p, kHeavyIters),
                                                   std::memory_order_relaxed);
                              }, (void*)(intptr_t)i, false, g_jlType);
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

        if (g_doJ) blkWakes = (unsigned long long)JLib::TaskScheduler::GetWakeCount();
        const size_t blkPeakF = g_doJ ? JLib::TaskScheduler::GetAwakeFloorPeak() : 0;
        const double ja = MedOr(a), mb = MedOr(b);
        printf("     %8d", d);
        Cell(ja, 11, 2); Cell(mb, 11, 2);
        if (ja > 0 && mb > 0) printf(" %8.2fx", mb / ja); else printf("        -");
        // Per rep: 3 reps x 10 batches x 256 tasks = 7680 pushes. A number near that means the row
        // is a wake benchmark; a number near zero means the pool stayed hot and the row is measuring
        // whatever else it claims to measure.
        if (g_doJ) printf("   wakes %llu / 7680   peakF %zu", blkWakes, blkPeakF);
        printf("\n");
    }
    printf("\n     ratio > 1.00 means JLib is faster.\n\n");
}


// ---------------------------------------------------------------- where the blocking cost goes
// JLib-only. The blocking row above works out to roughly 5 us per blocking task, and three guesses
// at which stage owns it have now been wrong (the event registry, the serial requeue, and before
// that fiber tuning). So measure the stages instead of arguing about them.
//
// Three variants, each differing from the previous by exactly ONE stage:
//   A  TaskType::Native, empty body -- baseline: create, place, claim, run, destroy. No fiber at all.
//   B  TaskType::Fiber, empty body  -- adds fiber acquire from the pool, ContextSwitch in,
//                                      ContextSwitch out, fiber release. The task never suspends.
//   C  TaskType::Fiber, waits on an ALREADY-SIGNALLED event -- adds AddWaiter, SignalAll, the
//                                      SUSPENDED->READY CAS, the re-queue, a second trip through
//                                      placement/inbox/deque, and a second ContextSwitch pair.
//
// So B-A is what a fiber costs to attach and run on, and C-B is what a full suspend/resume round
// trip through the scheduler costs. Together they should account for the 5 us; if they do not, the
// cost is in the wait itself and not in any of this.
static void BenchFiberBreakdown(JLib::TaskScheduler& jl) {
    if (!g_doJ) return;
    // N MUST STAY WELL UNDER THE FIBER POOL. Variant C suspends every task, and a suspended task
    // HOLDS its fiber, so concurrent suspensions are bounded by standardFiberCount (64 per core,
    // ~1984 here). At 20000 this livelocked: AcquireFiber returns null, the worker re-queues and
    // yields, and prints to cerr on every failed acquire -- minutes of console flooding rather than
    // a measurement. 1000 keeps every variant inside the pool with room to spare.
    constexpr int N = 1000;
    printf("  fiber cost breakdown -- ns per task, empty bodies, JLib only\n\n");

    std::vector<double> a, b, c;

    // ---- ROTATE THE ARMS, AND WARM UP FIRST ---------------------------------------------------
    //
    // THIS ROW PRINTED AN IMPOSSIBLE NUMBER: A (no fiber) 686 ns against B (fiber, never suspends)
    // 381 ns -- attaching a fiber measuring 305 ns CHEAPER than not attaching one. The format
    // string reads "(+%.0f for the fiber)" and printed a negative, which is the tell.
    //
    // CAUSE: the arms ran in a FIXED order A, B, C, five times, never rotated. Each rep ends in
    // WaitFor, the pool goes idle and the floor sheds -- so the NEXT rep's A is the arm that
    // re-wakes the pool and pays the ramp back up (this program grows the floor to 30), while B and
    // C then run into a hot pool. The bias is per-rep and systematic, so a median over five reps
    // PRESERVES it rather than removing it. Whichever arm goes first eats the ramp, and here that
    // was always A.
    //
    // FIX IS THE ONE lock_contention.cpp ALREADY LEARNED, after its A/A control showed 52% p90
    // drift: rotate the arms inside one process with a shifting start offset so each takes its turn
    // paying, plus an untimed warm-up so no measured rep is the one that first touches the slab and
    // populates the fiber pool.
    auto runA = [&]() -> double {
        if (g_jlType == JLib::TaskType::Fiber) return -1.0;   // no Native path in the fiberonly arm
        JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
        return Ms(t0, Clock::now()) * 1e6 / N;
    };
    auto runB = [&]() -> double {
        JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr, false, JLib::TaskType::Fiber);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
        return Ms(t0, Clock::now()) * 1e6 / N;
    };
    auto runC = [&]() -> double {
        g_released.store(true, std::memory_order_release);
        JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
        auto t0 = Clock::now();
        for (int i = 0; i < N; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {
                JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                s.WaitOnEventArmed(*g_ioEvent, [] { g_ioEvent->SignalAll(); });
            }, nullptr, false, JLib::TaskType::Fiber);
            t->waitGroup = &wg; jl.Push(t);
        }
        jl.WaitFor(wg);
        return Ms(t0, Clock::now()) * 1e6 / N;
    };

    (void)runA(); (void)runB(); (void)runC();   // untimed: reach steady state before counting

    for (int r = 0; r < 6; ++r) {               // 6, so over the rotation each arm leads twice
        for (int s = 0; s < 3; ++s) {
            switch ((r + s) % 3) {
                case 0: { const double v = runA(); if (v >= 0) a.push_back(v); } break;
                case 1:   b.push_back(runB()); break;
                default:  c.push_back(runC()); break;
            }
        }
    }

    // The original fixed-order loop, kept compiled out so the defect stays reproducible rather than
    // only described. Set to 1 and A and B swap places again.
#if 0
    for (int r = 0; r < 5; ++r) {
        // A: no fiber. SKIPPED UNDER fiberonly, and not because of a limitation -- this row IS the
        // Native baseline, and "no fiber" is the one thing a Fiber-task arm does not have. Forcing it
        // to TaskType::Fiber would not rescue the row, it would silently duplicate row B and report
        // the A->B delta as zero. `a` stays empty and prints `--`.
        if (g_jlType != JLib::TaskType::Fiber) {
            JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr);
                t->waitGroup = &wg; jl.Push(t);
            }
            jl.WaitFor(wg);
            a.push_back(Ms(t0, Clock::now()) * 1e6 / N);
        }
        {   // B: fiber-backed, never suspends
            JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr,
                                              false, JLib::TaskType::Fiber);
                t->waitGroup = &wg; jl.Push(t);
            }
            jl.WaitFor(wg);
            b.push_back(Ms(t0, Clock::now()) * 1e6 / N);
        }
        {   // C: fiber-backed, full suspend/resume on an already-signalled event
            g_released.store(true, std::memory_order_release);
            JLib::WaitGroup wg; wg.n.store(N, std::memory_order_relaxed);
            auto t0 = Clock::now();
            for (int i = 0; i < N; ++i) {
                JLib::Task* t = jl.CreateTask(+[](void*) {
                    JLib::TaskScheduler& s = JLib::TaskScheduler::Instance();
                    s.WaitOnEventArmed(*g_ioEvent, [] { g_ioEvent->SignalAll(); });
                }, nullptr, false, JLib::TaskType::Fiber);
                t->waitGroup = &wg; jl.Push(t);
            }
            jl.WaitFor(wg);
            c.push_back(Ms(t0, Clock::now()) * 1e6 / N);
        }
    }

#endif
    // MedOr, not Median: under fiberonly row A does not run, and Median indexes an empty vector.
    // That segfaulted, after printing this section's header and nothing else -- which read as the
    // bench simply stopping. Guarding a row and not guarding its reader is half a change.
    const double ma = MedOr(a), mb = MedOr(b), mc = MedOr(c);

    if (ma >= 0) {
        printf("     %-34s %8.0f ns\n", "A  no fiber (baseline)", ma);
        printf("     %-34s %8.0f ns   (+%.0f for the fiber)\n",
               "B  fiber, never suspends", mb, mb - ma);
    } else {
        printf("     %-34s %8s      (no Native path in the Fiber-task arm)\n",
               "A  no fiber (baseline)", "--");
        printf("     %-34s %8.0f ns\n", "B  fiber, never suspends", mb);
    }

    // C - B IS THE ISOLATED COST OF THE EVENT, and it is the only line here that survives losing
    // row A. Same task type, same body, same pool: the single difference is that C registers on an
    // event, is signalled, parks and is resumed. Whatever separates these two numbers is what a
    // direct fiber suspend/resume would be replacing -- and it is the number to argue from, not a
    // round-trip row, which measures a path where main is a bare thread that spin-helps and never
    // touches an Event at all.
    printf("     %-34s %8.0f ns   (+%.0f for the EVENT park/resume)\n",
           "C  fiber, suspend + resume", mc, mc - mb);
    printf("\n");
}
int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);

    size_t pool = 0;
    bool noSleep = false;
    // fiberonly: run JLib's rows with EVERY task fiber-backed, the way marl always does.
    //
    // ONE CHANGE NOW, NOT TWO. This used to also flip Mode::FiberOnly, so the row confounded the
    // task type with the park mechanism. The mode is gone (4.0.2) and its park is unconditional, so
    // what is left is a clean Fiber-vs-Native comparison against the same scheduler.
    bool fiberOnly = false;
    // hot=N: dedicate N workers to the low-latency lane. Included here to answer a specific
    // question -- whether K-hot changes FIBER WAKE latency. It should not: a wake preserves the
    // task's own hiPri tag (Event::WakeOne batches by t->hiPri), and every fiber in this harness is
    // untagged, so they wake on the ordinary lane no matter what K is. Measuring it is how that
    // claim stops being an assertion.
    size_t hot = 0;
    for (int i = 1; i < argc; ++i) {
        if (strncmp(argv[i], "hot=", 4) == 0)     { hot = (size_t)strtoul(argv[i] + 4, nullptr, 10); continue; }
        // floor=N -- THE LAST ASYMMETRY WITH marl THAT IS NOT WORKER COUNT.
        //
        // K (hot=) reserves workers, so hot=2 runs JLib on 29 compute workers against marl's 31 and
        // is NOT a like-for-like config. The awake floor does not reserve anything -- floor workers
        // are ordinary compute workers -- but they never PARK, and every marl worker does. Default
        // is 2. `floor=0` makes both pools park identically, which is the strict-parity run;
        // anything else measures the shipped policy, which is also a legitimate question.
        // AND THE GROWTH CONTROLLER GOES WITH IT. SetAwakeFloor sets base and current, but the
        // push-side controller can still raise the live floor above base during a burst -- so
        // `floor=0` alone would park identically to marl right up until the first burst and then
        // quietly stop being the parity run it was asked to be.
        // spinyield=N -- yield the core every N+1 idle passes (default 1023). Lower is politer.
        // The knob exists because marl's spin yields every few microseconds and this one yielded
        // every few hundred, which is why floor=31/nosleep LOST to parking on the blocking row.
        // widesteer -- ordinary pushes aim at the LIVE floor instead of the base floor, so a burst
        // lands wide instead of on two workers that the other 29 then have to steal from.
        if (strcmp(argv[i], "widesteer") == 0) {
            JLib::TaskScheduler::SetPlacementFollowsGrownFloor(true);
            continue;
        }
        if (strncmp(argv[i], "spinyield=", 10) == 0) {
            JLib::TaskScheduler::SetSpinYieldMask((unsigned)strtoul(argv[i] + 10, nullptr, 10));
            continue;
        }
        if (strncmp(argv[i], "floor=", 6) == 0) {
            const size_t f = (size_t)strtoul(argv[i] + 6, nullptr, 10);
            JLib::TaskScheduler::SetAwakeFloor(f);
            if (f == 0) JLib::TaskScheduler::SetFloorGrowthEnabled(false);
            continue;
        }
        if (strcmp(argv[i], "nosleep") == 0)      { noSleep = true; continue; }
        if (strcmp(argv[i], "fiberonly") == 0)    { fiberOnly = true; continue; }
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

    if (hot) {
        JLib::TaskScheduler::SetHotWorkers(hot);
        JLib::TaskScheduler::SetHotThreadPolicy(JLib::TaskScheduler::HotThreadPolicy::Elevated);
    }

    if (fiberOnly && g_doM) {
        g_doM = false;
        printf("note: fiberonly implies --only=jlib -- its park never sleeps, and a spinning pool\n"
               "      cannot share a process with another scheduler being timed. Run --only=marl\n"
               "      separately and compare the two pastes.\n\n");
    }

    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    JLib::TaskScheduler::Init(pool);
    // `fiberOnly` now selects the TASK TYPE only. Mode::FiberOnly was removed in 4.0.2 once it
    // became behaviourally identical to Default -- the pinning, the direct resume and the futex park
    // it was built for are unconditional now, and admitting Native was the last difference. The
    // Fiber-vs-Native axis this flag selects is still a real one and is what the flag measures.
    if (fiberOnly) g_jlType = JLib::TaskType::Fiber;
    JLib::TaskScheduler& jl = JLib::TaskScheduler::Instance();
    if (g_doJ) g_ioEvent = &jl.GetEvent("compare_io");   // the one and only registry lookup
    if (!g_doJ) JLib::detail::TeardownForTesting(jl);   // real teardown: nothing of JLib runs while marl is timed

    const uint32_t workers = (uint32_t)(pool ? pool : std::thread::hardware_concurrency() - 1);

    marl::Scheduler::Config cfg;
    cfg.setWorkerThreadCount((int)workers);
    marl::Scheduler marlSched(cfg);
    g_marlSched = &marlSched;
    if (g_doM) marlSched.bind();

    printf("\nJLib::Scheduler vs marl  (workers=%u, affinity=none, JLib idle=%s, measuring=%s)\n",
           workers, noSleep ? "nosleep" : "sleep",
           (g_doJ && g_doM) ? "both" : (g_doJ ? "JLib only" : "marl only"));
    printf("================================================================\n");
    printf("marl is ARCHIVED (last commit 2026-04-27) -- calibration, not a recommendation.\n\n");

    if (g_doJ) {
        JLib::WaitGroup wg; wg.n.store(4096, std::memory_order_relaxed);
        for (int i = 0; i < 4096; ++i) {
            JLib::Task* t = jl.CreateTask(+[](void*) {}, nullptr, false,
                                          g_jlType);
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
    BenchIdleTax(jl);
    BenchFiberBreakdown(jl);

    printf("(sink %llu -- printed only so none of the work can be optimised away)\n",
           (unsigned long long)g_sink.load());

    if (g_doM) marl::Scheduler::unbind();

    // WATCHDOG AROUND Join(). This harness has been observed to hang HERE -- after every result has
    // printed, so all work is finished and nothing is racing. That makes the pool STATIC at hang
    // time, and a dump readable rather than a smear: it prints per-worker queue sizes, sleep states
    // and the non-worker lane, which is precisely what a stack trace cannot tell you.
    //
    // Lives in the harness rather than the library because it is a diagnostic for one known bug, and
    // because a scheduler that watchdogs its own shutdown would be hiding the failure it should be
    // reporting.
    if (g_doJ) {
        std::atomic<bool> joined{ false };
        std::thread watchdog([&] {
            for (int i = 0; i < 100 && !joined.load(std::memory_order_acquire); ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (!joined.load(std::memory_order_acquire)) {
                printf("\n!!! Join() has not returned after 10s -- pool state follows !!!\n");
                jl.DumpPoolState("Join watchdog");
                fflush(stdout);
            }
            });
        JLib::detail::TeardownForTesting(jl);
        joined.store(true, std::memory_order_release);
        watchdog.join();
    }
    return 0;
}
