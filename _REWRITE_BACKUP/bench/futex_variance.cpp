// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Standalone, Windows-only -- NOT part of SchedulerBench. Answers a question the 2026-08-16 futex
// investigation (see memory/scheduler-push-path-atomics.md, "FUTEX: BUILT, BENCHMARKED, REVERTED")
// never actually measured: that investigation rejected WaitOnAddress/WakeByAddressSingle on
// throughput/1p MEDIAN (13% slower than std::condition_variable), but its 3 recorded runs per arm
// happened to look like this --
//
//     condvar : 0.94 1.17 0.96 M/s   (noisy)
//     futex   : 0.84 0.84 0.84 M/s   (flat)
//
// -- which is suggestive of futex being lower-variance, but N=3 per arm cannot tell "genuinely
// tighter variance" apart from "three numbers that happened to round the same way". This harness
// runs the ONE thing that comparison never isolated: wake-latency VARIANCE (p50/p90/p99/max/stddev)
// for a genuinely parked waiter, not throughput. That is also the more honest question for THIS
// project's own stated priorities -- the epoch self-reclaim change (1.x) was shipped explicitly
// because "throughput does not change... what changes is the tail... a frame-time CONSISTENCY
// feature", so if futex trades average speed for tighter variance, that trade might be the right
// one even though it lost the throughput benchmark that originally killed it.
//
// PROTOCOL: two threads ping-pong. The waiter parks, publishes "I am about to wait", then genuinely
// blocks (condvar wait or WaitOnAddress). The signaler waits for that publish, sleeps briefly to be
// sure the waiter is truly in the OS wait (not just about to call it -- for WaitOnAddress this is
// belt-and-suspenders, not required for correctness: WaitOnAddress re-checks *Address against the
// compare value immediately before blocking, so a signal landing in that window returns instantly
// rather than being lost, the same guarantee a real futex gives), records t0, signals, then waits
// for the waiter to publish its own wake timestamp t1. Runs many rounds, ALTERNATING arms per
// round (not block-measured: condvar round, futex round, condvar round, ...) so neither arm gets
// an unfair share of whatever the machine is doing at any given time.
//
// BUILD (MSVC, from a "Developer Command Prompt" / vcvars-initialized shell):
//   cl /std:c++20 /O2 /EHsc bench\futex_variance.cpp /Fe:futex_variance.exe
// RUN:
//   futex_variance.exe [reps-per-round] [rounds] [park-delay-us]
//
// RESULT (2026-08-21, Windows, 5000 reps/arm, two independent full runs): NO -- the repeatability
// does not hold up at real sample size, and if anything the opposite is true.
//
//     run 1  stddev ratio (condvar/futex, >1.0 = futex tighter): 0.26x
//     run 2  stddev ratio (condvar/futex, >1.0 = futex tighter): 0.55x
//
// Both times condvar's stddev is WELL BELOW futex's -- futex is consistently the LESS repeatable
// arm, not the more repeatable one. Median/p90 are close to a wash both runs (~1.0-1.05x), so
// there is no speed edge to trade for it either. Futex also produced the two worst single outliers
// across both runs (6.98ms and 4.42ms max, vs condvar's worst of 2.83ms). A smaller trial run
// (N=40/arm) before this one showed the OPPOSITE -- futex both faster and tighter -- which is
// itself the finding worth remembering: that smaller sample was exactly as misleading as the
// original N=3 that started this whole question, just in the other direction. Trust neither.
//
// CAVEAT: this is the ISOLATED PRIMITIVE in a 2-thread ping-pong, not the full 31-worker scheduler
// round trip the original 2026-08-16 investigation measured (create+push+steal+run+free under real
// contention). It answers "is WaitOnAddress itself more repeatable" (no), not "would swapping it
// into Worker() change the scheduler's real frame-time behavior" -- though this result gives that
// question one LESS reason to be worth re-opening, not one more.

#include <windows.h>
#include <synchapi.h>
#pragma comment(lib, "Synchronization.lib")   // WaitOnAddress/WakeByAddressSingle -- not auto-linked
#pragma comment(lib, "winmm.lib")             // timeBeginPeriod/timeEndPeriod
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <condition_variable>
#include <mutex>

using Clock = std::chrono::steady_clock;
static int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
}

// ---- Arm 1: std::condition_variable, the shipped default -----------------------------------
struct CondvarWaiter {
    std::mutex m;
    std::condition_variable cv;
    bool ready = false;

    void Wait() {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [this] { return ready; });
        ready = false;
    }
    void Signal() {
        { std::lock_guard<std::mutex> lock(m); ready = true; }
        cv.notify_one();
    }
    static const char* Name() { return "condvar"; }
};

// ---- Arm 2: WaitOnAddress/WakeByAddressSingle, the reverted 2026-08-16 attempt --------------
struct FutexWaiter {
    std::atomic<LONG> flag{ 0 };

    void Wait() {
        LONG expected = 0;
        // Re-checks *Address == *CompareAddress atomically right before blocking -- a Signal()
        // landing between a caller publishing "about to wait" and actually reaching this call
        // cannot be lost, same guarantee a real futex gives.
        while (flag.load(std::memory_order_acquire) == 0) {
            WaitOnAddress(&flag, &expected, sizeof(LONG), INFINITE);
        }
        flag.store(0, std::memory_order_relaxed);
    }
    void Signal() {
        flag.store(1, std::memory_order_release);
        WakeByAddressSingle(&flag);
    }
    static const char* Name() { return "futex"; }
};

template <typename WaiterT>
static std::vector<double> RunRound(int reps, int parkDelayUs) {
    WaiterT waiter;
    std::atomic<bool> aboutToWait{ false };
    std::atomic<int64_t> wakeTimeNs{ 0 };

    std::thread waiterThread([&] {
        for (int i = 0; i < reps; ++i) {
            aboutToWait.store(true, std::memory_order_release);
            waiter.Wait();
            wakeTimeNs.store(NowNs(), std::memory_order_release);
        }
        });

    std::vector<double> latenciesUs;
    latenciesUs.reserve(reps);
    for (int i = 0; i < reps; ++i) {
        while (!aboutToWait.load(std::memory_order_acquire)) std::this_thread::yield();
        aboutToWait.store(false, std::memory_order_relaxed);
        if (parkDelayUs > 0) std::this_thread::sleep_for(std::chrono::microseconds(parkDelayUs));

        const int64_t t0 = NowNs();
        waiter.Signal();

        int64_t t1;
        while ((t1 = wakeTimeNs.exchange(0, std::memory_order_acq_rel)) == 0) std::this_thread::yield();
        latenciesUs.push_back((double)(t1 - t0) / 1000.0);
    }

    waiterThread.join();
    return latenciesUs;
}

struct Stats {
    double median = 0, p90 = 0, p99 = 0, max = 0, mean = 0, stddev = 0;
};
static Stats Summarize(std::vector<double> v) {
    Stats s;
    if (v.empty()) return s;
    std::sort(v.begin(), v.end());
    auto pct = [&](double p) { size_t i = (size_t)(p * (v.size() - 1)); return v[i]; };
    s.median = pct(0.50);
    s.p90 = pct(0.90);
    s.p99 = pct(0.99);
    s.max = v.back();
    double sum = 0; for (double x : v) sum += x;
    s.mean = sum / v.size();
    double sq = 0; for (double x : v) sq += (x - s.mean) * (x - s.mean);
    s.stddev = std::sqrt(sq / v.size());
    return s;
}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IONBF, 0);   // unbuffered -- otherwise nothing appears until exit
    // Windows' default timer resolution is coarse (often ~15.6ms) -- without raising it, a
    // "200us" sleep_for can actually take 15ms+, turning a few-second run into minutes. 1ms
    // resolution is standard practice for exactly this and is restored automatically on exit.
    timeBeginPeriod(1);

    const int reps = argc > 1 ? std::atoi(argv[1]) : 500;
    const int rounds = argc > 2 ? std::atoi(argv[2]) : 10;
    const int parkDelayUs = argc > 3 ? std::atoi(argv[3]) : 200;

    printf("futex_variance: %d reps/round x %d rounds, %d us park delay, arms alternated per round\n",
        reps, rounds, parkDelayUs);
    printf("(park delay is realism, not correctness -- WaitOnAddress cannot lose a signal to this race)\n\n");

    std::vector<double> condvarAll, futexAll;
    for (int r = 0; r < rounds; ++r) {
        auto c = RunRound<CondvarWaiter>(reps, parkDelayUs);
        auto f = RunRound<FutexWaiter>(reps, parkDelayUs);
        condvarAll.insert(condvarAll.end(), c.begin(), c.end());
        futexAll.insert(futexAll.end(), f.begin(), f.end());

        Stats cs = Summarize(c), fs = Summarize(f);
        printf("round %2d  condvar: median=%6.2f p90=%6.2f p99=%6.2f max=%7.2f stddev=%6.2f us"
            "   futex: median=%6.2f p90=%6.2f p99=%6.2f max=%7.2f stddev=%6.2f us\n",
            r, cs.median, cs.p90, cs.p99, cs.max, cs.stddev,
            fs.median, fs.p90, fs.p99, fs.max, fs.stddev);
    }

    Stats cAll = Summarize(condvarAll), fAll = Summarize(futexAll);
    printf("\n=== combined (%zu reps each) ===\n", condvarAll.size());
    printf("condvar: median=%.2f us  p90=%.2f  p99=%.2f  max=%.2f  mean=%.2f  stddev=%.2f (%.1f%% of mean)\n",
        cAll.median, cAll.p90, cAll.p99, cAll.max, cAll.mean, cAll.stddev, 100.0 * cAll.stddev / cAll.mean);
    printf("futex  : median=%.2f us  p90=%.2f  p99=%.2f  max=%.2f  mean=%.2f  stddev=%.2f (%.1f%% of mean)\n",
        fAll.median, fAll.p90, fAll.p99, fAll.max, fAll.mean, fAll.stddev, 100.0 * fAll.stddev / fAll.mean);
    printf("\nratio (condvar/futex, >1.0 means futex is TIGHTER): stddev %.2fx   p99 %.2fx   median %.2fx\n",
        cAll.stddev / fAll.stddev, cAll.p99 / fAll.p99, cAll.median / fAll.median);

    timeEndPeriod(1);
    return 0;
}
