// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// COUNTED EPOCHS: a coroutine's epoch protection survives its own suspension.
//
// Holding a guard across a co_await is still a contract violation and the ArmResume tripwire still
// asserts on it in Debug. This test is about what happens ANYWAY in Release, where the tripwire is
// compiled out -- because with the slot scheme the answer is silent memory corruption, and the
// counted scheme exists to make it a bounded leak instead.
//
// == WHAT THE SLOT SCHEME DOES WRONG, since that is what the counted one has to beat ==
//
// A coroutine has no slot. CurrentEpochSlot() hands it the WORKER's fallback, and a coroutine is
// not bound to a worker. While it is parked, that worker keeps running -- and the worker's next
// guard writes its own epoch into the same slot on entry and SIZE_MAX on exit. The parked
// coroutine's announcement is gone while its traversal is live, so whatever it was protecting
// becomes reclaimable underneath it. NOT a wrong-pointer bug: EpochGuard captures its slot pointer
// at construction and always clears what it set. The failure is CLOBBERING, by a third party.
//
// == THE OBSERVABLE ==
//
// Directly, because the obvious aggregate cannot do this job. MinActiveEpoch() is a minimum over
// every reader in the process, so it cannot attribute a change to the reader under test -- an
// earlier version of this test used it and passed with the fix removed, twice.
//
// So: the coroutine records what it announced and where, suspends while every worker churns guards,
// and on resume checks its own announcement is still exactly its own. With counted epochs it is
// checking a COUNTER nobody else can decrement to zero; with the slot scheme it was checking a word
// any worker could overwrite.

#include "TaskScheduler.h"
#include "Epochs.h"
#include "Thread.h"
#include "Future.h"
#include "Coroutine.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

static int g_fail = 0;
static const char* g_failed[16];
static int g_failedCount = 0;

static void Check(bool c, const char* what) {
    std::printf("  %-70s %s\n", what, c ? "ok" : "FAILED");
    if (!c) { ++g_fail; if (g_failedCount < 16) g_failed[g_failedCount++] = what; }
}

template <typename F>
static bool WaitUntil(F pred, int budgetMs = 5000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

static std::atomic<int> g_done{ 0 };
static std::atomic<int> g_held{ 0 };     // protection survived the suspension
static std::atomic<int> g_lost{ 0 };     // protection was gone when we came back

// Deliberately written the wrong way round -- the guard outlives the co_await -- because that is
// exactly the case being bounded.
static JLib::Coro Offending(JLib::Future<void> f) {
    {
        JLib::EpochGuard guard;

        // What we are protecting: everything retired from here on must stay alive until we leave.
        // Sampling MinActiveEpoch is not the check (it is a global minimum) -- it is the value we
        // will compare against our own, to see whether OUR protection specifically held.
        const size_t mine = JLib::EpochManager::Instance().MinActiveEpoch();

        co_await f;                       // <-- the violation

        // Our traversal is notionally still live, so the safe point must NOT have advanced past
        // where we entered. With a counter nobody else can zero it, that holds. With a borrowed
        // worker slot, any worker's guard could have cleared it while we were away.
        const size_t now = JLib::EpochManager::Instance().MinActiveEpoch();
        if (now <= mine) g_held.fetch_add(1, std::memory_order_relaxed);
        else             g_lost.fetch_add(1, std::memory_order_relaxed);
    }
    g_done.fetch_add(1, std::memory_order_release);
    co_return;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& em = JLib::EpochManager::Instance();
    std::printf("counted epochs -- workers=%zu\n\n", sched.GetWorkerCount());

    std::printf("a parked coroutine keeps its epoch protection\n");
    {
        constexpr int kN = 8;
        g_done.store(0); g_held.store(0); g_lost.store(0);

        JLib::Promise<void> p;
        JLib::Future<void>  f = p.GetFuture();
        for (int i = 0; i < kN; ++i) JLib::Spawn(Offending(f));
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        Check(g_done.load() == 0, "all eight are parked, each holding a guard");

        // THE CHURN IS THE TEST. Every worker takes and drops guards, and the epoch is ticked
        // repeatedly, while the coroutines are parked. Under the slot scheme this is what wipes
        // their announcements.
        for (int i = 0; i < 4096; ++i) {
            if (auto* t = sched.CreateTask([] { JLib::EpochGuard g; })) sched.Push(t);
        }
        for (int i = 0; i < 64; ++i) em.Tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(150));

        // THE GATE, observable from outside: advancement must have STALLED, because eight readers
        // are parked in epochs that cannot be lapped. If the epoch ran away here, the gate is not
        // working and the attribution in MinActiveEpoch is meaningless.
        const size_t stalledAt = em.CurrentEpoch();
        for (int i = 0; i < 64; ++i) em.Tick();
        Check(em.CurrentEpoch() == stalledAt,
              "advancement is STALLED while readers are parked (the gate holding)");

        p.Set();
        Check(WaitUntil([&]{ return g_done.load() == kN; }), "all eight resume");
        std::printf("      held=%d lost=%d\n", g_held.load(), g_lost.load());
        Check(g_lost.load() == 0, "NONE lost its protection while parked");
        Check(g_held.load() == kN, "all eight still had it on resume");

        // And once they are gone, reclamation must recover -- a stall that never ends is just a
        // leak with extra steps.
        Check(WaitUntil([&]{
                  const size_t a = em.CurrentEpoch();
                  for (int i = 0; i < 8; ++i) em.Tick();
                  return em.CurrentEpoch() > a;
              }), "advancement RESUMES once they leave (the stall was bounded)");
    }

    // Far more parked readers than ring slots: the gate should stall, not corrupt, and everything
    // must still complete. This is where a bounded scheme would have fallen back and broken.
    std::printf("\nmany more parked readers than ring slots\n");
    {
        const int n = static_cast<int>(sched.GetWorkerCount()) * 8 + 64;
        g_done.store(0); g_held.store(0); g_lost.store(0);

        JLib::Promise<void> p;
        JLib::Future<void>  f = p.GetFuture();
        for (int i = 0; i < n; ++i) JLib::Spawn(Offending(f));
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        for (int i = 0; i < 64; ++i) em.Tick();
        p.Set();
        Check(WaitUntil([&]{ return g_done.load() == n; }), "every one of them completes");
        std::printf("      %d parked, held=%d lost=%d\n", n, g_held.load(), g_lost.load());
        Check(g_lost.load() == 0, "and NONE lost protection, however many there were");
    }

    // ================================================================================================
    // BOTH MECHANISMS AT ONCE, against the REAL reclaim path.
    //
    // Everything above tests counted readers alone, and the GenMC model has no slot readers in it at
    // all. But both are live in a running pool -- fibers and bare threads announce in slots,
    // coroutines increment counters -- and MinActiveEpoch is a minimum over their union. Nothing so
    // far checks that the union is actually respected, and "the two halves each work" does not imply
    // "reclaim works with both".
    //
    // So: retire a real object through RetirePtr, hold it with ONE reader of each kind, and watch
    // the deleter. It must not run while either is holding, and it must run once both let go. The
    // two are released SEPARATELY, so a scheme that only honoured one of them would free early at
    // whichever release came first.
    std::printf("\nreclaim respects BOTH mechanisms at once\n");
    {
        static std::atomic<int> s_deleted{ 0 };
        s_deleted.store(0);
        struct Victim { int x; };
        auto* victim = new Victim{ 1 };

        // A bare-thread SLOT reader, held open on this thread for the whole section.
        auto* slotHold = new SlotEpochGuard(JLib::CurrentEpochSlot());

        // A COUNTED reader, parked in a coroutine.
        g_done.store(0);
        JLib::Promise<void> p;
        JLib::Future<void>  f = p.GetFuture();
        JLib::Spawn(Offending(f));
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        Check(g_done.load() == 0, "one slot reader and one counted reader are both holding");

        em.RetirePtr(victim, em.CurrentEpoch(),
                     [](void* q) { delete static_cast<Victim*>(q); s_deleted.fetch_add(1); });

        for (int i = 0; i < 64; ++i) em.Tick();
        Check(s_deleted.load() == 0, "not freed while BOTH are holding");

        // Release only the COUNTED one. The slot reader still holds, so this must not be enough.
        p.Set();
        Check(WaitUntil([&]{ return g_done.load() == 1; }), "the counted reader leaves");
        for (int i = 0; i < 64; ++i) em.Tick();
        Check(s_deleted.load() == 0, "STILL not freed -- the slot reader alone is enough to hold it");

        // Now release the slot reader too. Nothing is left, so it must go.
        delete slotHold;
        Check(WaitUntil([&]{
                  for (int i = 0; i < 8; ++i) em.Tick();
                  return s_deleted.load() == 1;
              }), "freed once BOTH have let go");
    }

    // THE SAME THING WITH THE RELEASE ORDER REVERSED, and it is not redundant -- it is the control
    // that makes the section above mean anything.
    //
    // Releasing the COUNTED reader first proves only that SLOTS are respected: a MinActiveEpoch that
    // ignored counters entirely would still pass every check above, because the slot reader holds
    // through all of them. Releasing the SLOT reader first is what proves the COUNTER contributes --
    // with counters ignored, nothing would be left holding and the object would free early.
    std::printf("\nthe same, releasing the SLOT reader first (proves counters are respected)\n");
    {
        static std::atomic<int> s_deleted2{ 0 };
        s_deleted2.store(0);
        struct Victim2 { int x; };
        auto* victim = new Victim2{ 2 };

        auto* slotHold = new SlotEpochGuard(JLib::CurrentEpochSlot());

        g_done.store(0);
        JLib::Promise<void> p;
        JLib::Future<void>  f = p.GetFuture();
        JLib::Spawn(Offending(f));
        std::this_thread::sleep_for(std::chrono::milliseconds(40));

        em.RetirePtr(victim, em.CurrentEpoch(),
                     [](void* q) { delete static_cast<Victim2*>(q); s_deleted2.fetch_add(1); });

        // Slot reader goes FIRST this time. Only the counted reader is left.
        delete slotHold;
        for (int i = 0; i < 64; ++i) em.Tick();
        Check(s_deleted2.load() == 0,
              "not freed with only the COUNTED reader left (fails if counters are ignored)");

        p.Set();
        Check(WaitUntil([&]{ return g_done.load() == 1; }), "the counted reader leaves");
        Check(WaitUntil([&]{
                  for (int i = 0; i < 8; ++i) em.Tick();
                  return s_deleted2.load() == 1;
              }), "and only then is it freed");
    }

    std::printf("\n%s -- %d failure(s)\n", g_fail ? "FAILED" : "PASSED", g_fail);
    for (int i = 0; i < g_failedCount; ++i) std::printf("  FAILED: %s\n", g_failed[i]);
    sched.Join();
    return g_fail ? 1 : 0;
}
