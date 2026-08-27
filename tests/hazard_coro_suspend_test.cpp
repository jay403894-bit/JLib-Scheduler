// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE SUSPENSION CHECK MUST ACTUALLY FIRE -- death test for HazardDomain::FatalSuspendWithGuard.
//
// A coroutine MAY hold a HazardGuard; it MAY NOT carry one across a suspension point. That rule is
// enforced in detail::ArmResume, which every awaiter routes through. This file exists because the
// rest of the suite passes without ever exercising it: nothing else holds a guard across a
// co_await, so "the suite is green" said nothing whatsoever about the mechanism.
//
// THREE CHILDREN, and the two that must NOT abort matter as much as the one that must. A check that
// fires on every coroutine would satisfy the fatal case and be worthless:
//
//   suspend-with-guard   coroutine takes a guard, then co_awaits.       MUST ABORT.
//   drop-then-await      the standard pattern -- guard in an inner
//                        scope, released BEFORE the co_await.           MUST NOT abort.
//   fiber-park           a FIBER holds a guard across a park.           MUST NOT abort.
//
// THE THIRD IS THE FALSE-POSITIVE CHECK AND THE EASIEST THING TO GET WRONG. A parked fiber holding
// a guard is legal -- cells are indexed by the fiber, so protection survives the park, which is the
// entire point of this design. If the depth counter were a plain per-thread count, that fiber's
// guard would still be counted on the worker, and the next coroutine to suspend there would abort
// for something it did not do. Fiber rows are excluded at guard construction; this proves it.

#include "TaskScheduler.h"
#include "Coroutine.h"
#include "Hazard.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#if defined(_WIN32)
  #include <crtdbg.h>
  #include <windows.h>
#endif

using namespace JLib;

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {

struct Node { int magic = 0xA11E; };
std::atomic<Node*> g_head{ nullptr };
SchedulerMutex     g_gate;
std::atomic<bool>  g_reached{ false };

void SuppressCrashDialogs() {
#if defined(_WIN32)
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
    _CrtSetReportMode(_CRT_ASSERT, 0);
    ::SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
#endif
}

int RunChild(const char* self, const char* mode, std::string& err) {
    char cmd[1024];
    std::snprintf(cmd, sizeof(cmd), "\"%s\" %s 2>&1", self, mode);
    err.clear();
#if defined(_WIN32)
    FILE* p = _popen(cmd, "r");
#else
    FILE* p = popen(cmd, "r");
#endif
    if (!p) return -1;
    char buf[512];
    while (std::fgets(buf, sizeof(buf), p)) err += buf;
#if defined(_WIN32)
    return _pclose(p);
#else
    return pclose(p);
#endif
}

// THE VIOLATION. The guard is alive across the co_await, so ArmResume must stop this before the
// frame can be re-pushed and resumed somewhere its cells do not live.
Coro CarriesGuardAcrossAwait() {
    HazardGuard g;
    g.Protect(0, g_head);
    g_reached.store(true, std::memory_order_release);
    co_await LockAsync(g_gate);
    g_gate.Unlock();
    co_return;
}

// THE STANDARD PATTERN. Guard scoped to the synchronous lookup and released before suspending, so
// the check must let this through untouched.
Coro DropsGuardBeforeAwait() {
    {
        HazardGuard g;
        g.Protect(0, g_head);
    }
    g_reached.store(true, std::memory_order_release);
    co_await LockAsync(g_gate);
    g_gate.Unlock();
    co_return;
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- children ---------------------------------------------------------------------------
    if (argc > 1) {
        SuppressCrashDialogs();
        const char* mode = argv[1];

        TaskScheduler::Init(0);
        auto& sched = TaskScheduler::Instance();
        g_head.store(new Node{}, std::memory_order_release);

        if (std::strcmp(mode, "suspend-with-guard") == 0) {
            g_gate.Lock();                       // force the await to actually suspend
            WaitGroup wg;
            Spawn(CarriesGuardAcrossAwait(), &wg);
            sched.WaitFor(wg);                   // unreachable: the child aborts inside ArmResume
            std::printf("NO ABORT -- the check did not fire\n");
            return 0;
        }

        if (std::strcmp(mode, "drop-then-await") == 0) {
            g_gate.Lock();
            WaitGroup wg;
            Spawn(DropsGuardBeforeAwait(), &wg);
            // Let it park on the gate, then release so it can finish.
            while (!g_reached.load(std::memory_order_acquire)) std::this_thread::yield();
            g_gate.Unlock();
            sched.WaitFor(wg);
            std::printf("completed with the guard dropped before the await\n");
            sched.Join();
            return 0;
        }

        // A FIBER holding a guard across a park. Legal, and the case a naive per-thread depth would
        // have broken: the fiber's guard must not be counted against a coroutine suspending later
        // on the same worker.
        if (std::strcmp(mode, "fiber-park") == 0) {
            g_gate.Lock();
            WaitGroup wg;
            auto* t = sched.CreateTask([] {
                HazardGuard g;                    // fiber row -- does NOT count toward the depth
                g.Protect(0, g_head);
                g_reached.store(true, std::memory_order_release);
                g_gate.Lock();                    // PARKS the fiber while the guard is held
                g_gate.Unlock();
            }, false, FiberSize::Standard, TaskType::Fiber);
            t->waitGroup = &wg;
            wg.n.fetch_add(1, std::memory_order_relaxed);
            sched.Push(t);

            while (!g_reached.load(std::memory_order_acquire)) std::this_thread::yield();
            g_gate.Unlock();
            sched.WaitFor(wg);
            std::printf("fiber parked and resumed while holding a guard\n");
            sched.Join();
            return 0;
        }

        return 9;
    }

    // ---- parent -----------------------------------------------------------------------------
    std::printf("coroutine guard across a suspension (death test)\n\n");
    std::string err;

    int rc = RunChild(argv[0], "suspend-with-guard", err);
    const bool named = err.find("suspended while holding") != std::string::npos;
    const bool hasFix = err.find("THE FIX IS THE STANDARD PATTERN") != std::string::npos
                        && err.find("co_await safe->AsyncWork()") != std::string::npos;
    std::printf("      guard carried across await: exit=%d\n", rc);
    Check(rc != 0, "carrying a guard across a co_await ABORTS at the suspension point");
    Check(named,   "and it is the SUSPENSION handler that fired, named in the message");
    Check(hasFix,  "and the message carries the fix, not just the diagnosis");

    rc = RunChild(argv[0], "drop-then-await", err);
    std::printf("      guard dropped before await:  exit=%d\n", rc);
    Check(rc == 0, "the standard pattern is NOT blocked (else the check is worthless)");

    rc = RunChild(argv[0], "fiber-park", err);
    std::printf("      fiber parked holding guard:  exit=%d\n", rc);
    Check(rc == 0, "a FIBER may hold a guard across a park -- fiber rows do not count");

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
