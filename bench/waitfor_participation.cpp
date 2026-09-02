// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// WHERE DOES THE 4.5 us IN A SERIAL SUBMIT-AND-WAIT ROUND TRIP ACTUALLY GO?
//
// compare_marl.cpp reports JLib at ~4.55 us against marl's ~0.82 us on that row. Reading marl's
// source, two structural differences stand out and neither is the parking PRIMITIVE (marl parks
// workers on a std::condition_variable, same class of thing this does):
//
//   1. marl's enqueue steers each task to a worker that is ALREADY SPINNING (scheduler.cpp:176-192,
//      the spinningWorkers[] exchange), so in a serial ping-pong no worker is ever woken.
//   2. marl's BOUND thread runs tasks while blocked on a WaitGroup, so the submitting thread is not
//      woken either.
//
// This bench exists to size (2) -- the SUBMITTER's park -- because it is the half nothing in the
// scheduler currently addresses, and because it can be measured without touching the library.
//
// WHY NOT JUST COMPARE IDLE POLICIES. NoSleep is a BENCHMARKING TOOL, not a configuration anyone
// ships; the shipping answer to (1) is K hot workers. So idle policy is held at Sleep here and K is
// the axis, which is what a real deployment would vary.
//
// READ THE K ROWS WITH CARE, and check the conditions line first. SetHotWorkers also applies
// HotThreadPolicy::Elevated, and a process that has been power-throttled does not get the priority
// it asks for -- the hot workers then are not meaningfully hot and the K rows understate what they
// do on a normally-launched process. The MAIN-PARK rows have no such dependency: a spin is a spin.
//
// THE ARMS DIFFER IN EXACTLY ONE THING EACH, and all five run in one process, interleaved, because
// the awake floor and SetHotWorkers are both runtime settings. No cross-build comparison anywhere.

// A named task body -- one spelling for every task body in the tree.
static void EmptyBody(void*);

#include <windows.h>

#include "TaskScheduler.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#if defined(_MSC_VER)
  #include <intrin.h>
#endif

namespace {

double g_qpcFreq = 0.0;

double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return n & 1 ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

struct Arm {
    const char* label;
    size_t      k;         // hot workers
    bool        spin;      // true: main spins on the count. false: main parks in WaitFor.
    bool        lane;     // true: push to the LANE, which is what K hot workers serve
    bool        fiber;     // Fiber task rather than Native
    const char* note;
};

// Nanoseconds per serial submit-and-wait round trip.
double TimeArm(const Arm& arm, int pings) {
    auto& sched = JLib::TaskScheduler::Instance();
    JLib::TaskScheduler::SetHotWorkers(arm.k);
    // Let the pool reach the requested shape before timing. Outside the timed region on purpose:
    // a K change has to propagate to workers that may currently be parked.
    std::this_thread::sleep_for(std::chrono::milliseconds(15));

    LARGE_INTEGER a, b;
    ::QueryPerformanceCounter(&a);
    for (int i = 0; i < pings; ++i) {
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);

        // lane is what routes to the LANE. Setting K without this was the mistake the first
        // version of this bench made: K hot workers serve the lane, so a generic Push cannot
        // reach them and K reads as doing nothing.
        JLib::Task* t = sched.CreateTask(&EmptyBody, nullptr, arm.lane ? JLib::Lane::LowLatency : JLib::Lane::Normal,
                                         arm.fiber ? JLib::TaskType::Fiber : JLib::TaskType::Native);
        t->waitGroup = &wg;
        sched.Push(t);

        if (arm.spin) {
            // MAIN NEVER PARKS. Mask with COUNT_MASK -- the top bit of n is WAITER_BIT, and reading
            // n raw would compare a count against a flag.
            //
            // NOT REGISTERING IS WHAT MAKES THIS SAFE TO DESTROY BELOW. WakeAll is called only when
            // the completing worker sees WAITER_BIT set (Thread.cpp:950), and only WaitFor sets it.
            // So with nobody registered the worker's LAST touch of this WaitGroup is its fetch_sub,
            // which the acquire load below synchronises with. Spin-and-destroy would be a
            // use-after-free on wg.mtx if that guard were not there.
            while ((wg.n.load(std::memory_order_acquire) & JLib::WaitGroup::COUNT_MASK) > 0)
                _mm_pause();
        } else {
            sched.WaitFor(wg);
        }
    }
    ::QueryPerformanceCounter(&b);

    const double secs = double(b.QuadPart - a.QuadPart) / g_qpcFreq;
    return secs * 1e9 / double(pings);
}

} // namespace


static void EmptyBody(void*) {}

int main(int argc, char** argv) {
    int pings = 4000;
    int reps  = 31;
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--pings") && i + 1 < argc) pings = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--reps") && i + 1 < argc) reps = std::atoi(argv[++i]);
        else { std::printf("usage: %s [--pings N] [--reps N]\n", argv[0]); return 1; }
    }

    {
        PROCESS_POWER_THROTTLING_STATE pt{};
        pt.Version = PROCESS_POWER_THROTTLING_CURRENT_VERSION;
        const BOOL  gotQos = ::GetProcessInformation(::GetCurrentProcess(), ProcessPowerThrottling,
                                                     &pt, sizeof pt);
        const DWORD pc  = ::GetPriorityClass(::GetCurrentProcess());
        const bool  eco = gotQos && (pt.ControlMask & PROCESS_POWER_THROTTLING_EXECUTION_SPEED)
                                 && (pt.StateMask   & PROCESS_POWER_THROTTLING_EXECUTION_SPEED);
        std::printf("conditions: priority class 0x%lX%s, EcoQoS %s\n",
                    (unsigned long)pc,
                    pc == NORMAL_PRIORITY_CLASS ? " (NORMAL)"
                      : pc == IDLE_PRIORITY_CLASS ? " (IDLE -- THROTTLED, the K rows are meaningless)"
                      : pc == HIGH_PRIORITY_CLASS ? " (HIGH)" : "",
                    eco ? "ON -- THROTTLED, the K rows are meaningless" : (gotQos ? "off" : "unknown"));
    }

    // (This used to pin IdlePolicy::Sleep explicitly. That enum is gone in 5.0 and the library
    // default is an awake floor of 2, which is the same configuration these rows were measured
    // under -- Sleep never overrode the floor. Nothing to state, so nothing is stated.)
    JLib::TaskScheduler::SetHotThreadPolicy(JLib::TaskScheduler::HotThreadPolicy::Elevated);
    JLib::TaskScheduler::Init(0);

    LARGE_INTEGER f;
    ::QueryPerformanceFrequency(&f);
    g_qpcFreq = double(f.QuadPart);

    // THE MODE COLUMN IS GONE, AND SO ARE TWO ROWS. This table existed to price Mode::FiberOnly's
    // event park against Default's condvar park -- rows 2-vs-3 and 4-vs-5 were that comparison. The
    // mode was removed in 4.0.2 because it had become behaviourally identical to Default: the park
    // it introduced is now what every pool uses, so those rows would measure the same configuration
    // twice and print a difference that could only be noise.
    //
    // What is still worth measuring here is the axis the mode was confounded with -- Native vs Fiber
    // task type, and generic vs lane -- so those rows stay.
    const Arm arms[] = {
        { "Native generic K=0", 0, false, false, false, "what compare_marl measures today" },
        { "Fiber  generic K=0", 0, false, false, true,  "task type is the only change from row 1" },
        { "Fiber  lane   K=2", 2, false, true,  true,  "the lane" },
        { "Native generic K=0", 0, false, false, false, "SAME-VS-SAME CONTROL -- must read 1.00x" },
    };
    const int kArms = int(sizeof arms / sizeof arms[0]);

    std::printf("%u workers, idle=Sleep, %d round trips/rep, %d reps, interleaved\n",
                (unsigned)JLib::TaskScheduler::Instance().GetWorkerCount(), pings, reps);
    std::printf("marl on this row, for reference: 0.823 us\n\n");

    for (int a = 0; a < kArms; ++a) (void)TimeArm(arms[a], pings / 8 + 1);

    std::vector<std::vector<double>> s(kArms);
    for (int rep = 0; rep < reps; ++rep)
        for (int a = 0; a < kArms; ++a)
            s[a].push_back(TimeArm(arms[a], pings));

    const double base = Median(s[0]);
    std::printf("%-30s %10s %10s %8s   %s\n", "", "median us", "min us", "ratio", "");
    for (int a = 0; a < kArms; ++a) {
        const double med = Median(s[a]);
        const double mn  = *std::min_element(s[a].begin(), s[a].end());
        std::printf("%-30s %10.3f %10.3f %7.3fx   %s\n",
                    arms[a].label, med / 1000.0, mn / 1000.0, med / base, arms[a].note);
    }

    std::printf("\n    PARK MECHANISM ALONE   %+.3f us   (row 2 - row 3): condvar minus event,\n"
                "                                        same task type, same K, same push path\n"
                "    on the lane            %+.3f us   (row 4 - row 5)\n"
                "    the lane itself         %.3f us   against marl's 0.823 us\n",
                (Median(s[1]) - Median(s[2])) / 1000.0,
                (Median(s[3]) - Median(s[4])) / 1000.0,
                Median(s[3]) / 1000.0);

    const double ctrl = Median(s[5]) / base;
    if (ctrl < 0.97 || ctrl > 1.03)
        std::printf("\n    SAME-VS-SAME CONTROL READS %.3fx -- not resolving. Ignore the rows.\n", ctrl);

    JLib::TaskScheduler::SetHotWorkers(0);   // the header's rule: restore what you flipped
    return 0;
}
