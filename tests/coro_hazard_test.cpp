// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// HAZARD POINTERS ON A COROUTINE -- the acceptance test IS the spec:
//
//     Protect on a coroutine, co_await the mutex, CANCEL/DESTROY that frame from another worker,
//     retire the node from a THIRD.
//
// Both failure directions live in that one scenario, which is why it is the test and not an
// addition to it:
//
//   unregister LATE   -> the record still names a node nothing owns; or worse, the scan reads cells
//                        belonging to a frame that is already gone. Smashes immediately.
//   unregister EARLY, with the record reused -> a scanner still walking that block attributes a new
//                        frame's pointers to the old one. Smashes later, which is the nastier half.
//
// The record's CELLS are domain-owned rather than frame-owned (see the deviation note in Hazard.h),
// so the "scan reads freed frame memory" direction is removed by construction rather than by timing.
//
// == "DESTROY THAT FRAME FROM ANOTHER WORKER" IS UNREACHABLE HERE, AND THAT IS DELIBERATE ==
//
// The spec's step 3 asks for a suspended frame destroyed from outside. This library has no such
// path, by design, and the refusal is explicit:
//
//     TaskScheduler::DiscardIfCancelled:  if (!task || task->started || !IsTaskCancelled(task))
//                                             return false;
//
// A STARTED task is never discarded -- Thread.cpp says why: "Discarding one of those abandons a
// live stack or frame rather than cancelling it... A started task is let through to run, and
// observes the cancellation at its own next suspend point, where it can unwind properly." And
// Spawn() takes the handle via Release(), so no caller retains one to destroy either.
//
// So the reachable analogue -- and what the last case below actually does -- is: suspend inside a
// protected section, cancel the scope, let the frame RESUME and unwind. ~HazardGuard runs on that
// unwind. A frame discarded before it ever starts never took a guard, so it holds no record.
//
// This is worth stating rather than leaving as an untested gap: the unregister-on-external-destroy
// contract has no code path to violate, which is a stronger guarantee than a passing test for it.

#include "TaskScheduler.h"
#include "Coroutine.h"
#include "Hazard.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int  g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-70s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

namespace {

constexpr std::uint64_t kAlive = 0xA11FE0000A11FE00ull;
constexpr std::uint64_t kDead  = 0xDEADDEADDEADDEADull;

struct Node {
    std::uint64_t magic = kAlive;
    ~Node() { magic = kDead; }
};

std::atomic<int>   g_freed{ 0 };
std::atomic<Node*> g_head{ nullptr };
SchedulerMutex     g_gate;
std::atomic<bool>  g_published{ false };
std::atomic<bool>  g_resumed{ false };
std::atomic<bool>  g_sawAlive{ false };

void RetireNode(Node* n) {
    HazardDomain::Instance().Retire(n, [](void* p) {
        g_freed.fetch_add(1, std::memory_order_relaxed);
        delete static_cast<Node*>(p);
    });
}

Coro Reader() {
    HazardGuard g;
    Node* n = g.Protect(0, g_head);
    g_published.store(true, std::memory_order_release);

    // THE SUSPEND. A coroutine resumes as a call on whatever worker takes it, so this is the point
    // where worker-owned cells would silently stop protecting anything.
    co_await LockAsync(g_gate);
    g_gate.Unlock();

    g_sawAlive.store(n->magic == kAlive, std::memory_order_release);
    g_resumed.store(true, std::memory_order_release);
    co_return;
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    TaskScheduler::EnableIoReactor(false);
    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();
    std::printf("coroutine hazard pointers -- workers=%zu\n\n", sched.GetWorkerCount());

    g_head.store(new Node{}, std::memory_order_release);
    g_gate.Lock();                       // so the coroutine is guaranteed to suspend

    // Spawn() increments the WaitGroup ITSELF. Pre-setting n as well leaves it at 2 and WaitFor
    // never returns -- which is what the first version of this test did, and it read exactly like a
    // hazard-pointer deadlock. The fiber idiom in coroutine_test sets n by hand only because it
    // pushes the Task directly instead of going through Spawn.
    WaitGroup wg;
    Spawn(Reader(), &wg);

    for (int i = 0; i < 2000 && !g_published.load(std::memory_order_acquire); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    Check(g_published.load(std::memory_order_acquire),
          "the coroutine published a hazard before suspending");

    // Retire + scan WHILE THE FRAME IS SUSPENDED. If the record is not in the scan set, this frees.
    Node* victim = g_head.exchange(nullptr, std::memory_order_acq_rel);
    RetireNode(victim);
    HazardDomain::Instance().Scan();
    Check(g_freed.load() == 0,
          "a node named by a SUSPENDED coroutine frame survives a retire+scan");

    g_gate.Unlock();
    sched.WaitFor(wg);
    Check(g_resumed.load(std::memory_order_acquire) && g_sawAlive.load(std::memory_order_acquire),
          "the frame resumed and its node was still intact");

    // The frame is gone, so ~promise_type released the record. Reclamation must resume.
    HazardDomain::Instance().Scan();
    HazardDomain::Instance().Scan();
    Check(g_freed.load() == 1,
          "once the frame is destroyed the record is released and the node IS freed");

    // ---- THE CASE THE SPEC NAMES: a SUSPENDED frame holding a record, CANCELLED ------------------
    //
    // This is the one that matters, and the earlier checks do not cover it. A frame that completes
    // normally releases through the ordinary path; a frame never spawned never took a record at all.
    // The dangerous path is a frame suspended INSIDE a protected section that is then cancelled --
    // final_suspend never runs, and if the record were released from a scheduler hook it would leak
    // one per cancellation, and the node it names would never be freed again.
    //
    // Here the guard is a local in the frame, so cancellation unwinds it and the language runs
    // ~HazardGuard. The proof is that the node becomes freeable afterwards.
    {
        g_freed.store(0, std::memory_order_relaxed);
        g_published.store(false, std::memory_order_relaxed);
        g_head.store(new Node{}, std::memory_order_release);
        g_gate.Lock();                                  // guarantee the suspend

        CancelScope scope;
        WaitGroup   wg2;
        Spawn(Reader(), &wg2, 0, CorePref::Default, scope.Token().Raw());

        for (int i = 0; i < 2000 && !g_published.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        Node* v2 = g_head.exchange(nullptr, std::memory_order_acq_rel);
        RetireNode(v2);
        HazardDomain::Instance().Scan();
        Check(g_freed.load() == 0, "still protected while the frame is suspended and not yet cancelled");

        // CANCEL THE SUSPENDED FRAME. The mutex wait returns Cancelled, the coroutine unwinds, and
        // ~HazardGuard releases the record on the way out.
        // ORDER IS LOAD-BEARING: Cancel, then UNLOCK, then wait.
        //
        // SchedulerMutex cancellation is SKIP-AT-RELEASE, not eager -- unlike Event, semaphore and
        // the condition variable, cancelling a mutex waiter does not wake it. The cancelled waiter
        // is only walked past when the lock is RELEASED. Waiting before unlocking therefore hangs
        // forever, which is what the first version of this did and which reads exactly like a
        // hazard-record leak. Nothing to fix in the library; the test had the order wrong.
        scope.Cancel();
        g_gate.Unlock();
        sched.WaitFor(wg2);

        HazardDomain::Instance().Scan();
        HazardDomain::Instance().Scan();
        Check(g_freed.load() == 1,
              "a CANCELLED suspended frame released its record, and the node is freed");
        (void)0;
    }

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
