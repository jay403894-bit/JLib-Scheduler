// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Exercises the NAMED event path -- WaitOnEvent / GetEvent().SignalAll() -- which the benchmark
// suite never touches. Written when Event went from a std::mutex around an unordered_set to a
// lock-free intrusive stack, because that change had no coverage otherwise.
//
//   clang++ -std=c++17 -O2 -g -I include tests/event_smoke.cpp build/libScheduler.a -o eventsmoke
//   ./eventsmoke ; echo "exit $?"
//
// What it actually tries to break, in order:
//   1. Many fibers pushing onto the waiter stack CONCURRENTLY -- the CAS loop in AddWaiter is the
//      only place two threads race, so the test wants real contention, not one waiter at a time.
//   2. A signal that arrives while registrations are still in flight -- the interesting ordering,
//      since a waiter that registers after the exchange must not be lost. Repeated rounds with no
//      settling delay give that window a chance to open.
//   3. Signalling an event nobody is waiting on, and one that has already been drained -- both
//      must be no-ops rather than faults.
#include <TaskScheduler.h>   // brings WaitGroup, Event and DirectEvent with it
#include <cstdio>
#include <atomic>
#include <thread>
#include <chrono>
#include <cstdlib>   // atoi
#include <string>    // std::to_string

static std::atomic<int> g_resumed{ 0 };
static std::atomic<int> g_entered{ 0 };

// usage: event_smoke [rounds] [waiters] [poolSize]
//
// Sizeable because ThreadSanitizer needs a much smaller run than a plain one: it slows execution
// 5-15x, and the default configuration (31 workers, main hammering SignalAll in a spin loop) turns
// into hours on a 2-4 core CI runner. Small numbers lose nothing under TSAN -- it reasons about the
// happens-before graph and reports a race on FIRST observation, so repetitions buy nothing there.
// They do buy something on real hardware, where a race only shows up if the interleaving actually
// occurs, which is why the default stays large.
int main(int argc, char** argv) {
    const int  kRoundsArg  = (argc > 1) ? atoi(argv[1]) : 200;
    const int  kWaitersArg = (argc > 2) ? atoi(argv[2]) : 24;
    const size_t poolSize  = (argc > 3) ? (size_t)atoi(argv[3]) : 0;   // 0 = auto (hw-1)

    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    JLib::TaskScheduler::Init(poolSize);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();
    printf("config              : %d rounds x %d waiters, pool=%s\n",
        kRoundsArg, kWaitersArg, poolSize ? std::to_string(poolSize).c_str() : "auto");

    bool ok = true;

    // ---- empty and double signal: must not fault ----
    sched.GetEvent("never_waited").SignalAll();
    sched.GetEvent("never_waited").SignalAll();
    printf("empty signal        : ok\n");

    // ---- the real test: N waiters per round, many rounds ----

    for (int round = 0; round < kRoundsArg; ++round) {
        char name[32];
        snprintf(name, sizeof(name), "round_%d", round % 8);   // bounded name set, as documented
        auto& ev = sched.GetEvent(name);

        g_entered.store(0, std::memory_order_relaxed);
        const int before = g_resumed.load(std::memory_order_relaxed);

        JLib::WaitGroup wg;
        wg.n.fetch_add(kWaitersArg, std::memory_order_relaxed);
        for (int i = 0; i < kWaitersArg; ++i) {
            // These suspend, so they need a fiber under them.
            JLib::Task* t = sched.CreateTask([&sched, name] {
                g_entered.fetch_add(1, std::memory_order_relaxed);
                sched.WaitOnEvent(name);
                g_resumed.fetch_add(1, std::memory_order_relaxed);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            if (!t) { printf("FAIL: CreateTask returned null (round %d)\n", round); return 1; }
            t->waitGroup = &wg;
            sched.Push(t);
        }

        // Signal repeatedly rather than waiting for all to register. A waiter that pushes AFTER an
        // exchange has to be caught by a later one -- that is exactly the race worth hammering, and
        // sleeping until everyone registered would hide it.
        while (wg.n.load(std::memory_order_acquire) > 0) {
            ev.SignalAll();
            std::this_thread::yield();
        }
        sched.WaitFor(wg);

        const int gained = g_resumed.load(std::memory_order_relaxed) - before;
        if (gained != kWaitersArg) {
            printf("FAIL round %d: %d of %d waiters resumed\n", round, gained, kWaitersArg);
            ok = false;
            break;
        }
    }

    printf("concurrent waiters  : %s (%d rounds x %d waiters = %d resumes)\n",
        ok ? "ok" : "FAILED", kRoundsArg, kWaitersArg, g_resumed.load());

    // ---- signal a drained event again ----
    sched.GetEvent("round_0").SignalAll();
    printf("drained re-signal   : ok\n");

    sched.Join();
    printf("\n%s\n", ok ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return ok ? 0 : 1;
}
