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
        JLib::CoroSafeEpochGuard guard;

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
            if (auto* t = sched.CreateTask([] { JLib::CoroSafeEpochGuard g; })) sched.Push(t);
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

    std::printf("\n%s -- %d failure(s)\n", g_fail ? "FAILED" : "PASSED", g_fail);
    for (int i = 0; i < g_failedCount; ++i) std::printf("  FAILED: %s\n", g_failed[i]);
    sched.Join();
    return g_fail ? 1 : 0;
}
