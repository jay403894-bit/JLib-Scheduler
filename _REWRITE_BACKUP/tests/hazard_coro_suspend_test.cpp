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
#include <thread>

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
std::atomic<bool>  g_mainDone{ false };
std::atomic<bool>  g_sawAlive{ false };
std::atomic<int>   g_freed{ 0 };

// NEGATIVE CONTROL SWITCH. With this set the coroutine takes NO guard, so the node it names is
// unprotected and main's scan must free it. Without this the "heldOff" assertion could not be shown
// to fail, and an assertion that cannot fail is not an assertion.
std::atomic<bool>  g_skipProtect{ false };

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

// PROTECTS WITHOUT EVER SUSPENDING. The guard is held across a SPIN, not a co_await, so this is
// the supported path -- and while it spins, main retires the node it named and scans. If the
// announcement in the worker's cells is doing its job, the node survives.
Coro ProtectsWhileSpinning() {
    Node* n = g_head.load(std::memory_order_acquire);
    HazardGuard g;
    if (!g_skipProtect.load(std::memory_order_acquire)) n = g.Protect(0, g_head);
    g_reached.store(true, std::memory_order_release);

    // SPIN, DO NOT co_await. Suspending here is the violation the death test covers; this child is
    // about whether the protection WORKS on the path that is allowed.
    while (!g_mainDone.load(std::memory_order_acquire)) std::this_thread::yield();

    // Read through the pointer AFTER main retired and scanned it. Reachable only because the guard
    // held it off; without protection this is a use-after-free.
    g_sawAlive.store(n && n->magic == 0xA11E, std::memory_order_release);
    co_return;
}   // ~HazardGuard here -- the node becomes freeable

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

        // DOES A COROUTINE'S GUARD ACTUALLY PROTECT ANYTHING? The death test above proves the CHECK
        // fires. It says nothing about whether the protection works, which is the claim that
        // matters -- and those are different things, so this is the one that tests the feature.
        //
        // NO SUSPENSION ANYWHERE IN THE GUARDED SPAN, which is what makes this the supported path
        // rather than the forbidden one. The coroutine SPINS while main retires and scans, so it
        // stays on its worker and its worker-cell announcement stays valid the whole time.
        //
        // BOTH DIRECTIONS, because "not freed" alone passes on a domain that never frees anything:
        //   protected + retire + scan  ->  MUST NOT be freed
        //   guard dropped + scan       ->  MUST be freed
        if (std::strcmp(mode, "coro-protects") == 0 || std::strcmp(mode, "coro-control") == 0) {
            if (std::strcmp(mode, "coro-control") == 0)
                g_skipProtect.store(true, std::memory_order_release);
            WaitGroup wg;
            Spawn(ProtectsWhileSpinning(), &wg);

            while (!g_reached.load(std::memory_order_acquire)) std::this_thread::yield();

            // RETIRE FROM MAIN, i.e. a different thread from the one announcing it -- which is the
            // only arrangement that tests anything. The coroutine is inside its guarded span.
            Node* victim = g_head.exchange(nullptr, std::memory_order_acq_rel);
            HazardDomain::Instance().Retire(victim, [](void* p) {
                g_freed.fetch_add(1, std::memory_order_relaxed);
                delete static_cast<Node*>(p);
            });
            HazardDomain::Instance().Scan();

            const bool heldOff = (g_freed.load(std::memory_order_acquire) == 0);
            g_mainDone.store(true, std::memory_order_release);
            sched.WaitFor(wg);                    // the coroutine drops its guard as it returns

            HazardDomain::Instance().Scan();
            HazardDomain::Instance().Scan();
            const bool freedAfter = (g_freed.load(std::memory_order_acquire) == 1);
            const bool sawAlive   = g_sawAlive.load(std::memory_order_acquire);

            std::printf("heldOff=%d sawAlive=%d freedAfter=%d\n",
                        (int)heldOff, (int)sawAlive, (int)freedAfter);
            sched.Join();
            return (heldOff && sawAlive && freedAfter) ? 0 : 1;
        }

        // NESTED GUARDS ON ONE WORKER ROW must ABORT. Not "does Scan free" -- a different bug, and
        // the one this file did not cover until it was found.
        //
        // Every non-fiber guard on a worker resolves to the SAME cells: Cells(fiberCount + qIndex).
        // Two live guards there share one row, and ~HazardGuard nulls the WHOLE row rather than the
        // cells it personally set -- so the inner guard's destructor erases the outer guard's
        // announcement while the outer is still using it. A Scan may then free a node the outer
        // guard still names.
        //
        // FIBERS DO NOT COLLIDE ACROSS TASKS, since each fiber has its own poolIndex. Worker rows do,
        // and they are what native tasks and coroutines get.
        //
        // REACHED HERE BY PLAIN NESTING because that is the smallest reproduction. The path that
        // makes it more than a style rule is the spin-help one: a native task holding a guard calls
        // WaitFor, TryRunStolenNativeTask runs ANOTHER task on the same worker, and that task's
        // guard lands on the same row without anything looking nested at all.
        if (std::strcmp(mode, "nested-worker-row") == 0) {
            WaitGroup wg;
            auto* t = sched.CreateTask([] {
                HazardGuard outer;
                Node* n = outer.Protect(0, g_head);

                {
                    // THE VIOLATION. Before the fix this destructed and nulled the whole row,
                    // silently erasing `outer`. Now constructing it aborts.
                    HazardGuard inner;                 // SAME worker row as `outer`
                    inner.Protect(1, g_head);
                }

                g_reached.store(true, std::memory_order_release);
                while (!g_mainDone.load(std::memory_order_acquire)) std::this_thread::yield();

                // Still holding `outer`. If its announcement survived, this node is intact.
                g_sawAlive.store(n && n->magic == 0xA11E, std::memory_order_release);
            });
            t->waitGroup = &wg;
            wg.n.fetch_add(1, std::memory_order_relaxed);
            sched.Push(t);

            while (!g_reached.load(std::memory_order_acquire)) std::this_thread::yield();

            Node* victim = g_head.exchange(nullptr, std::memory_order_acq_rel);
            HazardDomain::Instance().Retire(victim, [](void* p) {
                g_freed.fetch_add(1, std::memory_order_relaxed);
                delete static_cast<Node*>(p);
            });
            HazardDomain::Instance().Scan();

            const bool heldOff = (g_freed.load(std::memory_order_acquire) == 0);
            g_mainDone.store(true, std::memory_order_release);
            sched.WaitFor(wg);

            std::printf("heldOff=%d (outer guard still live when main scanned)\n", (int)heldOff);
            sched.Join();
            return heldOff ? 0 : 1;
        }

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

    rc = RunChild(argv[0], "coro-protects", err);
    std::printf("      coroutine guard protects:    exit=%d  %s", rc, err.c_str());
    Check(rc == 0, "a node named by a coroutine survived a retire+scan, and freed once dropped");

    rc = RunChild(argv[0], "nested-worker-row", err);
    std::printf("      second guard on one worker row: exit=%d\n", rc);
    const bool nestedNamed = err.find("second HazardGuard on a worker row") != std::string::npos;
    const bool nestedFix   = err.find("USE ONE GUARD WITH SEVERAL CELLS") != std::string::npos;
    Check(rc != 0,     "a second worker-row guard ABORTS instead of erasing the first");
    Check(nestedNamed, "and it is the NESTED handler that fired, not another one");
    Check(nestedFix,   "and the message says to use one guard with several cells");

    rc = RunChild(argv[0], "coro-control", err);
    std::printf("      NEGATIVE CONTROL, no guard:  exit=%d  %s", rc, err.c_str());
    Check(rc != 0, "without the guard the node IS freed -- the check above can fail");

    rc = RunChild(argv[0], "drop-then-await", err);
    std::printf("      guard dropped before await:  exit=%d\n", rc);
    Check(rc == 0, "the standard pattern is NOT blocked (else the check is worthless)");

    rc = RunChild(argv[0], "fiber-park", err);
    std::printf("      fiber parked holding guard:  exit=%d\n", rc);
    Check(rc == 0, "a FIBER may hold a guard across a park -- fiber rows do not count");

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
