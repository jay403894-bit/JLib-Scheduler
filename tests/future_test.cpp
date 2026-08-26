// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Future<T>/Promise<T>: the multi-consumer half of the pair whose single-consumer half is Lazy<T>.
//
// EVERY SECTION HERE HAS A NEGATIVE CONTROL, because a test that only checks "the value arrived"
// passes with most of the interesting behaviour removed. This project has already shipped a
// primitives test that passed one run in three with the mutex deleted, so:
//
//   * multi-consumer checks that ALL of N awaiters ran, not that some did
//   * the cancel section checks that the OTHER waiters still completed -- cancelling one wait must
//     not cancel the work or anyone else's interest in it
//   * the broken section checks waiters are RELEASED rather than merely that nothing crashed; a
//     hang is the failure mode and only a bounded wait can catch it
//   * Take() is checked for the value, and UseCount for the precondition it asserts on

#include "TaskScheduler.h"
#include "Future.h"
#include "Coroutine.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <mutex>

static int g_fail = 0;
static const char* g_failed[32];
static int g_failedCount = 0;

static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) { ++g_fail; if (g_failedCount < 32) g_failed[g_failedCount++] = what; }
}

template <typename F>
static bool WaitUntil(F pred, int budgetMs = 3000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

static std::atomic<int> g_ran{ 0 }, g_sawValue{ 0 }, g_cancelled{ 0 }, g_broken{ 0 };

// Uncancellable consumer: co_await yields const T& directly.
static JLib::Coro Consumer(JLib::Future<std::string> f, const char* expect) {
    const std::string& v = co_await f;
    if (v == expect) g_sawValue.fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

// Cancellable consumer: reports which outcome it got.
static JLib::Coro ScopedConsumer(JLib::Future<std::string> f, JLib::CancelToken tok) {
    const JLib::FutureResult<std::string> r = co_await JLib::WaitFuture(f, tok);
    if (r.status == JLib::FutureStatus::Cancelled)      g_cancelled.fetch_add(1, std::memory_order_relaxed);
    else if (r.status == JLib::FutureStatus::Broken)    g_broken.fetch_add(1, std::memory_order_relaxed);
    else if (r.Ok() && *r == "shared")                  g_sawValue.fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

// Future<void> consumers: the signal form.
static JLib::Coro Signalled(JLib::Future<void> f) {
    co_await f;
    g_ran.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

static JLib::Coro ScopedSignalled(JLib::Future<void> f, JLib::CancelToken tok) {
    const JLib::FutureResult<void> r = co_await JLib::WaitFuture(f, tok);
    if (r.status == JLib::FutureStatus::Broken)         g_broken.fetch_add(1, std::memory_order_relaxed);
    else if (r.status == JLib::FutureStatus::Cancelled) g_cancelled.fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- BEFORE Init(): a Promise works with no scheduler ---------------------------------------
    //
    // The producer side is documented as plain C++17, usable from a thread that has never heard of
    // this scheduler -- so a Promise built before Init() has to work. It is also the property that
    // decided where the shared state lives: slab allocation was built and reverted (see Future.h),
    // because a Future's lifetime belongs to whoever holds a copy, and an allocator sized for the
    // scheduler's own churn is the wrong home for an object the user decides how long to keep.
    //
    // RUN FIRST, DELIBERATELY: after Init() this section says nothing.
    std::printf("before Init(): a Promise works with no scheduler at all\n");
    {
        JLib::Promise<std::string> p;
        JLib::Future<std::string>  f = p.GetFuture();
        Check(f.Valid() && !f.Ready(), "a Promise works with no scheduler at all");
        p.Set(std::string("preinit"));
        Check(f.Ready() && f.Get() == "preinit", "and carries its value");
    }
    Check(true, "and destructs cleanly with no scheduler ever started");

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("Future<T> -- workers=%zu\n", sched.GetWorkerCount());
    // Printed because the shared state's size decides whether it can come off the task slab, and
    // std::mutex on MSVC is far larger than people expect.
    std::printf("  sizeof: mutex=%zu State<void>=%zu State<int>=%zu State<string>=%zu Waiter=%zu\n\n",
                sizeof(std::mutex), sizeof(JLib::detail::FutureState<void>),
                sizeof(JLib::detail::FutureState<int>),
                sizeof(JLib::detail::FutureState<std::string>),
                sizeof(JLib::detail::FutureWaiter));

    // ---- the whole point: N consumers, one result ---------------------------------------------
    std::printf("N consumers all receive one result\n");
    {
        constexpr int kN = 8;
        g_ran.store(0); g_sawValue.store(0);
        JLib::Promise<std::string> p;
        JLib::Future<std::string> f = p.GetFuture();

        Check(f.Valid() && !f.Ready(), "a fresh Future is valid and not ready");
        Check(f.UseCount() == 2, "Promise and Future share one state (refcount 2)");

        for (int i = 0; i < kN; ++i) JLib::Spawn(Consumer(f, "shared"));
        // Let them all park BEFORE the value lands -- otherwise this tests the ready path only.
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        Check(g_ran.load() == 0, "consumers park while the value is unset (nothing completed early)");

        const std::size_t woke = p.Set(std::string("shared"));
        Check(woke == kN, "Set() woke every parked waiter, not just one");
        Check(WaitUntil([&]{ return g_ran.load() == kN; }), "all N consumers resumed");
        Check(g_sawValue.load() == kN, "all N saw the SAME correct value (this is the whole point)");
        Check(f.Ready() && f.Get() == "shared", "the value stays readable after everyone has read it");
    }

    // ---- awaiting a result that is ALREADY set must not suspend --------------------------------
    std::printf("\nawaiting an already-set Future takes the ready path\n");
    {
        g_ran.store(0); g_sawValue.store(0);
        JLib::Promise<std::string> p;
        JLib::Future<std::string> f = p.GetFuture();
        p.Set(std::string("early"));
        for (int i = 0; i < 4; ++i) JLib::Spawn(Consumer(f, "early"));
        Check(WaitUntil([&]{ return g_ran.load() == 4; }), "consumers of an already-set Future complete");
        Check(g_sawValue.load() == 4, "and all four read the value");
    }

    // ---- cancellation cancels the WAIT, not the work or anyone else ----------------------------
    std::printf("\ncancelling one waiter leaves the work and the other waiters alone\n");
    {
        g_ran.store(0); g_sawValue.store(0); g_cancelled.store(0); g_broken.store(0);
        JLib::Promise<std::string> p;
        JLib::Future<std::string> f = p.GetFuture();

        JLib::CancelScope doomed;
        JLib::CancelScope survivor;
        for (int i = 0; i < 3; ++i) JLib::Spawn(ScopedConsumer(f, doomed.Token()));
        for (int i = 0; i < 3; ++i) JLib::Spawn(ScopedConsumer(f, survivor.Token()));
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        Check(g_ran.load() == 0, "all six parked before anything was cancelled");

        doomed.Cancel();
        const std::size_t ejected = f.CancelWaiters(doomed.Token());
        Check(WaitUntil([&]{ return g_cancelled.load() == 3; }),
              "the three waiters in the cancelled scope returned Cancelled");
        Check(ejected == 3, "exactly three were ejected -- the other scope was untouched");

        // THE NEGATIVE CONTROL. If cancellation leaked into the state, these three would never
        // finish and the value below would never reach anyone.
        Check(!f.Broken(), "the Future did NOT become broken because one consumer left");
        const std::size_t woke = p.Set(std::string("shared"));
        Check(woke == 3, "Set() still woke the three survivors");
        Check(WaitUntil([&]{ return g_ran.load() == 6; }), "all six coroutines finished");
        Check(g_sawValue.load() == 3, "the survivors got the value; the cancelled ones did not");
    }

    // ---- a Promise destroyed unset must RELEASE its waiters, not hang them ---------------------
    std::printf("\na Promise dropped without Set() releases its waiters\n");
    {
        g_ran.store(0); g_broken.store(0);
        JLib::Future<std::string> f;
        {
            JLib::Promise<std::string> p;
            f = p.GetFuture();
            JLib::CancelScope sc;
            for (int i = 0; i < 4; ++i) JLib::Spawn(ScopedConsumer(f, sc.Token()));
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            Check(g_ran.load() == 0, "waiters parked on a Promise nobody has set");
        }   // p dies here, unset
        Check(WaitUntil([&]{ return g_ran.load() == 4; }),
              "the waiters were RELEASED rather than left parked forever");
        Check(g_broken.load() == 4, "and each was told Broken");
        Check(f.Broken() && !f.Ready(), "the Future reports Broken");
    }

    // ---- copy semantics and Take() -------------------------------------------------------------
    std::printf("\ncopying is another consumer; Take() moves out\n");
    {
        JLib::Promise<std::string> p;
        JLib::Future<std::string> a = p.GetFuture();
        {
            JLib::Future<std::string> b = a;              // copy = another consumer
            Check(a.UseCount() == 3, "copying a Future adds a reference");
            (void)b;
        }
        Check(a.UseCount() == 2, "and dropping the copy removes it");

        p.Set(std::string("moved"));
        Check(a.Ready() && a.Get() == "moved", "Get() yields the value without copying it out");
    }
    {
        // Take() asserts UseCount()==1, so the Promise has to be gone first. It publishes on
        // destruction, which is a no-op once a value has landed.
        JLib::Future<std::string> only;
        {
            JLib::Promise<std::string> p;
            only = p.GetFuture();
            p.Set(std::string("taken"));
        }
        Check(only.UseCount() == 1, "the last Future is the sole owner once the Promise is gone");
        Check(only.Take() == "taken", "Take() moves the value out for a sole owner");
    }

    // ---- Set() twice is a no-op rather than an error --------------------------------------------
    std::printf("\nsetting twice is ignored, not an error\n");
    {
        JLib::Promise<std::string> p;
        JLib::Future<std::string> f = p.GetFuture();
        Check(p.Set(std::string("first")) == 0, "Set with no waiters wakes nobody");
        p.Set(std::string("second"));
        Check(f.Get() == "first", "the second Set was ignored; the first value stands");
    }

    // ---- Future<void>: the coroutine-awaitable signal Event cannot be --------------------------
    std::printf("\nFuture<void> is a signal N coroutines can await\n");
    {
        g_ran.store(0); g_broken.store(0);
        JLib::Promise<void> p;
        JLib::Future<void>  f = p.GetFuture();
        for (int i = 0; i < 5; ++i) JLib::Spawn(Signalled(f));
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        Check(g_ran.load() == 0, "five coroutines parked on an unsignalled Future<void>");
        Check(p.Set() == 5, "Set() woke all five");
        Check(WaitUntil([&]{ return g_ran.load() == 5; }), "all five resumed");
        Check(f.Ready(), "the signal stays set for anyone who asks later");
    }
    {
        // Same broken-producer rule as the value form -- a dropped Promise<void> must release.
        g_ran.store(0); g_broken.store(0);
        {
            JLib::Promise<void> p;
            JLib::Future<void>  f = p.GetFuture();
            JLib::CancelScope sc;
            for (int i = 0; i < 3; ++i) JLib::Spawn(ScopedSignalled(f, sc.Token()));
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            Check(g_ran.load() == 0, "and park on a Promise<void> nobody sets");
        }
        Check(WaitUntil([&]{ return g_broken.load() == 3; }),
              "a dropped Promise<void> releases its waiters as Broken too");
    }

    std::printf("\n%s -- %d failure(s)\n", g_fail ? "FAILED" : "PASSED", g_fail);
    for (int i = 0; i < g_failedCount; ++i) std::printf("  FAILED: %s\n", g_failed[i]);
    sched.Join();
    return g_fail ? 1 : 0;
}
