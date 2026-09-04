// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES AN AFFINE DEBT RUN ON THE WORKER THAT OWES IT?
//
// This is the half of the old fiber_debt_test that still has a subject. That file tested
// ReleaseOnFiberDeath and DeleteOnFiberDeath -- a memory-only ledger, removed because the moment it
// fired was a moment when freeing was already safe. What is left is the case those could not serve
// and epochs and hazards cannot serve for themselves: state only ONE THREAD may retract.
//
// WHY THAT DISTINCTION IS THE WHOLE TEST. Releasing memory on the wrong thread costs a cache
// migration. Clearing an epoch slot on a thread that never set it un-announces a slot that was
// never there and frees nodes under a live traversal -- not slow, WRONG. So "it was released" is
// not the claim. The claim is "it was released BY THE HOLDER", and a test that only counts releases
// would pass while the mechanism did the one thing it exists to prevent.
//
// So every debt carries the worker it named, the release records the worker it actually ran on, and
// the assertion is that those agree for all of them.
//
// ---- WHY THIS FILE EXISTS AT ALL ----------------------------------------------------------
//
// TaskScheduler.h says of ReleaseOnWorker: "NOTHING IN THE LIBRARY CALLS THIS YET, and that is
// deliberate rather than incomplete... The wiring is here so that a debt which DOES need it is one
// call away, and so THE PATH IS TESTED rather than discovered later on the death path."
//
// Archiving the old test made that last clause false -- the affine path had no coverage at all,
// which is exactly the state the sentence forbids. Untested wiring on a death path is worse than no
// wiring, because the comment promises otherwise.

#include "TaskScheduler.h"
#include "Thread.h"
#include "Fiber.h"
#include "fiber_body.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-70s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

// Which worker is running this code, or SIZE_MAX off a worker entirely.
static size_t CurrentWorker() {
    Thread* t = Thread::GetCurrent();
    return t ? (size_t)t->qIndex : SIZE_MAX;
}

// ---- THE OBJECT CARRIES ITS OWN DEBT NODE -----------------------------------------------------
//
// Forced rather than chosen: FiberRegistry's dispatch path must not allocate, so there is nowhere to
// put a node the caller did not already own. Putting it inside the object costs two stores.
struct AffineOwned {
    FiberDebt debt;
    size_t    owedTo = SIZE_MAX;   // the holder this debt named
};

static std::atomic<int>    g_released{ 0 };
static std::atomic<int>    g_wrongThread{ 0 };
static std::atomic<size_t> g_lastReleaseWorker{ SIZE_MAX };

static void AffineRelease(void* p) noexcept {
    auto* o = static_cast<AffineOwned*>(p);
    const size_t here = CurrentWorker();
    g_lastReleaseWorker.store(here, std::memory_order_relaxed);
    // THE ASSERTION LIVES HERE, not in main, because only the release knows where it ran. A
    // mismatch is counted rather than asserted so one bad discharge does not abort the run and hide
    // how many others were fine.
    if (here != o->owedTo) g_wrongThread.fetch_add(1, std::memory_order_relaxed);
    g_released.fetch_add(1, std::memory_order_relaxed);
    delete o;
}

struct RegCtx {
    std::atomic<int>* ran;
    std::atomic<int>* registered;
    bool              registerDebt;   // arm 3 turns this off -- see there
};

static void RegisterBody(void* p) {
    RegCtx& c = *static_cast<RegCtx*>(p);
    if (c.registerDebt) {
        const size_t me = CurrentWorker();
        auto* o   = new AffineOwned();
        o->owedTo = me;
        // kOwesEpoch: any non-zero kind routes the death down AdvanceCleanup. Which kind it is does
        // not change the mechanism -- the kind decides THAT a chain happens, the creditor bit
        // decides WHO it visits.
        const bool ok = TaskScheduler::ReleaseOnWorker(o->debt, o, &AffineRelease,
                                                       me, Fiber::kOwesEpoch);
        if (ok) c.registered->fetch_add(1, std::memory_order_relaxed);
        else    delete o;              // refused: this frame still owns it
    }
    c.ran->fetch_add(1, std::memory_order_relaxed);
}

static bool WaitFor(std::atomic<int>& v, int target, int ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (v.load(std::memory_order_acquire) < target
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return v.load(std::memory_order_acquire) >= target;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== an affine debt is released BY THE WORKER THAT OWES IT ===\n");

    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();

    // ---- ARM 1: THE TWO REFUSALS, FIRST -------------------------------------------------------
    //
    // Both run on MAIN, before any fiber exists, so a mechanism that registered debts
    // unconditionally fails here rather than later where it would look like a leak.
    {
        AffineOwned a; a.owedTo = 0;
        const bool offFiber = TaskScheduler::ReleaseOnWorker(a.debt, &a, &AffineRelease,
                                                            0, Fiber::kOwesEpoch);
        Check(!offFiber, "registering an affine debt OFF a fiber is REFUSED");

        // kOwesNothing is the gate that stops a caller tagging a fiber with no kind -- which would
        // put a debt on the list and never route the death down the chain that collects it.
        AffineOwned b; b.owedTo = 0;
        const bool noKind = TaskScheduler::ReleaseOnWorker(b.debt, &b, &AffineRelease,
                                                          0, Fiber::kOwesNothing);
        Check(!noKind, "and a debt with kOwesNothing is REFUSED (a kind is what runs the chain)");
        Check(g_released.load() == 0, "neither refusal released anything behind the caller's back");
    }

    // ---- ARM 2: THE REAL PATH -----------------------------------------------------------------
    constexpr int kRounds = 300;
    {
        std::atomic<int> ran{ 0 }, registered{ 0 };
        RegCtx ctx{ &ran, &registered, true };

        for (int i = 0; i < kRounds; ++i) {
            Task* t = JLibTest::MakeCtxTask(sched, &RegisterBody, &ctx);
            if (t) sched.Push(t);
        }
        Check(WaitFor(ran, kRounds, 15000), "every task ran");
        Check(registered.load() == kRounds, "every fiber registered its affine debt");

        // Releases happen on the death path, which is behind the recycle and the reclaim task, so
        // this waits rather than sampling.
        const bool all = WaitFor(g_released, kRounds, 15000);
        std::printf("  released %d of %d, wrong-thread %d, last release on worker %zu\n",
                    g_released.load(), kRounds, g_wrongThread.load(),
                    g_lastReleaseWorker.load());

        Check(all, "EVERY affine debt was released (a stranded one means the chain never ran)");
        Check(g_released.load() == kRounds,
              "and released EXACTLY once each -- no double discharge");

        // THE CLAIM THIS FILE EXISTS FOR.
        Check(g_wrongThread.load() == 0,
              "every release ran ON THE WORKER THE DEBT NAMED (not merely somewhere)");
    }

    // ---- ARM 3: THE CONTROL THAT MAKES ARM 2 MEAN SOMETHING -----------------------------------
    //
    // Same tasks, same count, same everything -- except no debt is registered. If the counter still
    // climbs, arm 2's numbers were measuring something other than the debts it registered, and its
    // "exactly once" would have been arithmetic rather than evidence.
    {
        const int before = g_released.load(std::memory_order_relaxed);
        std::atomic<int> ran{ 0 }, registered{ 0 };
        RegCtx ctx{ &ran, &registered, false };

        for (int i = 0; i < kRounds; ++i) {
            Task* t = JLibTest::MakeCtxTask(sched, &RegisterBody, &ctx);
            if (t) sched.Push(t);
        }
        Check(WaitFor(ran, kRounds, 15000), "the no-debt arm ran too");
        std::this_thread::sleep_for(std::chrono::milliseconds(200));   // give a stray release time

        Check(registered.load() == 0, "it registered nothing, by construction");
        Check(g_released.load(std::memory_order_relaxed) == before,
              "CONTROL: fibers that owe nothing release nothing (the hook is not always-on)");
    }

    std::printf("\n%s -- %d failure(s)\n", g_fail ? "FAILED" : "PASSED", g_fail);
    return g_fail ? 1 : 0;
}
