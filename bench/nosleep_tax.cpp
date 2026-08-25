// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// WHAT AN IDLE NoSleep POOL COSTS EVERYONE ELSE.
//
// NoSleep trades CPU for wake latency: workers never park, so a push reaches a running thread
// instead of paying an OS wake (~85% of a measured 4.3us round trip). The bill arrives as a TAX ON
// OTHER WORK -- in a real app, the main thread. Game01 measured that tax at ~3.5%.
//
// The tax has two components and they are not the same thing:
//
//   SPINNING   an idle worker occupies a core. Unavoidable; it is what the caller bought.
//   SEARCHING  an idle worker repeatedly probes every other worker's deque endpoint. Avoidable,
//              and this is what the steal hint removes -- the probe becomes one read of a word
//              that is only written on state changes, so it sits shared-clean in every cache.
//
// "Spinning is fine; spinning AND searching is the problem." This bench separates them by measuring
// a fixed main-thread workload against three arms:
//
//   no pool      the floor -- what the work costs with the machine to itself
//   hint off     idle NoSleep pool, workers spinning and probing (the old behaviour)
//   hint on      idle NoSleep pool, workers spinning and reading one shared word
//
// tax = (arm - floor) / floor. The difference between the two pool arms is what the hint bought.
//
// ARMS INTERLEAVE inside one process and one pool. Measuring them as separate runs once moved a
// control configuration by 2x, which was machine drift being read as a result.

#include "TaskScheduler.h"
#include "Thread.h"
#include "platform.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <vector>

namespace {

// A main-thread workload with a real memory footprint, so the measurement is sensitive to cache
// traffic from other cores rather than being pure register work that no amount of remote probing
// could disturb. That sensitivity is the entire point: the tax IS cache interference.
constexpr size_t kWorking = 1u << 20;   // 4 MB of floats -- larger than L2, smaller than most L3
std::vector<float> g_buf;

double RunWorkload(int reps) {
    const auto t0 = std::chrono::steady_clock::now();
    for (int r = 0; r < reps; ++r) {
        float acc = 0.0f;
        for (size_t i = 0; i < kWorking; ++i) {
            g_buf[i] = g_buf[i] * 1.000001f + 0.5f;
            acc += g_buf[i];
        }
        // Keep acc observable so the loop survives optimisation.
        if (acc == 12345.678f) std::printf("");
    }
    return std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now() - t0).count();
}

double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

} // namespace

int main(int argc, char** argv) {
    const int reps  = (argc > 1) ? std::atoi(argv[1]) : 15;
    const int inner = (argc > 2) ? std::atoi(argv[2]) : 12;

    setvbuf(stdout, nullptr, _IONBF, 0);   // unbuffered: a fastfail must not swallow the trace
    g_buf.assign(kWorking, 1.0f);

    std::printf("NoSleep tax -- what an IDLE spinning pool costs the main thread\n");
    std::printf("  workload: %zu floats, %d passes per sample, %d interleaved samples per arm\n\n",
                kWorking, inner, reps);

    // FLOOR FIRST, with no pool at all. Taken before Init so the measurement is not standing on a
    // pool that merely claims to be idle.
    std::vector<double> floorS;
    for (int r = 0; r < reps; ++r) floorS.push_back(RunWorkload(inner));
    const double floorMs = Median(floorS);

    JLib::TaskScheduler::SetIdlePolicy(JLib::TaskScheduler::IdlePolicy::NoSleep);
    // Init is STATIC and must precede any Instance() call -- going through Instance() to reach it
    // fastfails before the pool exists, with no output if stdout is still buffered.
    JLib::TaskScheduler::Init();

    // Nothing is ever submitted. Every worker is idle for the whole run, which is the configuration
    // under test -- an app whose pool is provisioned for bursts and quiet between them.
    // PROBES ARE ATTRIBUTED PER ARM, by reading the counters either side of each sample. The whole
    // point of the pairing is that the probe count and the tax are the same fact measured two ways:
    // one is the cause, the other is what it costs. A total summed over both arms would show the
    // cause without ever attributing it.
    std::vector<double> offS, onS;
    long long offProbes = 0, onProbes = 0, offHits = 0, onHits = 0;
    for (int r = 0; r < reps; ++r) {
        long long p0 = 0, h0 = 0, p1 = 0, h1 = 0;

        JLib::TaskScheduler::SetStealHint(false);
        JLib::StealStatsRead(p0, h0);
        offS.push_back(RunWorkload(inner));
        JLib::StealStatsRead(p1, h1);
        offProbes += p1 - p0; offHits += h1 - h0;

        JLib::TaskScheduler::SetStealHint(true);
        JLib::StealStatsRead(p0, h0);
        onS.push_back(RunWorkload(inner));
        JLib::StealStatsRead(p1, h1);
        onProbes += p1 - p0; onHits += h1 - h0;
    }

    const double offMs = Median(offS), onMs = Median(onS);
    const double taxOff = 100.0 * (offMs - floorMs) / floorMs;
    const double taxOn  = 100.0 * (onMs  - floorMs) / floorMs;

    std::printf("  %-22s %8.2f ms\n", "no pool (floor)", floorMs);
    std::printf("  %-22s %8.2f ms   tax %+6.2f%%\n", "NoSleep, hint OFF", offMs, taxOff);
    std::printf("  %-22s %8.2f ms   tax %+6.2f%%\n", "NoSleep, hint ON",  onMs,  taxOn);
    if (taxOff > 0.0)
        std::printf("\n  the hint removes %.0f%% of the NoSleep tax (%.2f%% -> %.2f%%)\n",
                    100.0 * (taxOff - taxOn) / taxOff, taxOff, taxOn);

    if (JLib::kStealStatsEnabled) {
        std::printf("\n  remote deque touches by an idle pool with nothing to steal:\n");
        std::printf("    hint OFF  %12lld probes, %lld hits\n", offProbes, offHits);
        std::printf("    hint ON   %12lld probes, %lld hits", onProbes, onHits);
        if (onProbes > 0) std::printf("   (%.0fx fewer)", (double)offProbes / (double)onProbes);
        else              std::printf("   (all of them)");
        std::printf("\n");
    }
    else {
        std::printf("  (probe counts need -DJLIBSCHED_STEAL_STATS=ON)\n");
    }

    JLib::TaskScheduler::Instance().Join();
    return 0;
}
