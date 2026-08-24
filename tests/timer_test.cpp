// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// TimerQueue -- the hierarchical wheel.
//
// A timer's whole job is to cancel a scope and then wake whoever is parked, so most of what is
// worth testing is not "did it fire at the right millisecond" but the things that go wrong QUIETLY:
// a disarmed timer firing anyway, a timer outliving its scope and cancelling whoever inherited the
// slot, a far-future entry lost while cascading between levels, or a deadline that fires EARLY and
// cancels work that still had time left.
//
// Timing checks are deliberately loose. This runs on a general-purpose OS with a 1ms tick, and a
// test that asserts a tight upper bound on a sleep is a test that fails on a loaded machine for no
// reason. What is asserted tightly is the one direction that is a real bug: a timer must never fire
// EARLY.

#include "TaskScheduler.h"
#include "Timer.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int g_failures = 0;

static void Check(bool ok, const char* what) {
    std::printf("  %-64s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

static constexpr int64_t ms(int64_t n) { return n * 1'000'000; }

// Spin-wait on a predicate with a generous ceiling, so a hang fails the test instead of hanging it.
template <typename F>
static bool WaitUntil(F pred, int budgetMs = 5000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

int main() {
    // Unbuffered: a crash mid-suite must not swallow the lines that say where it got to.
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // Init is STATIC and has to run before anything touches Instance(); the scheduler fast-fails
    // rather than hand out a half-built singleton.
    // The timer runs a thread of its own, so the pool is sized to leave it a core.
    JLib::TaskScheduler::SetReserveTimerCore(true);
    JLib::TaskScheduler::Init(0);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    std::printf("a deadline cancels its scope\n");
    {
        JLib::CancelScope s;
        JLib::TimerQueue::Instance().Arm(ms(20), s.Token());
        Check(!s.Cancelled(), "not cancelled the moment it is armed");
        Check(WaitUntil([&] { return s.Cancelled(); }), "cancelled once the deadline passes");
    }

    // THE DIRECTION THAT IS A REAL BUG. Late is a scheduling artifact; EARLY cancels work that still
    // had time left, and no caller can defend against it.
    std::printf("a deadline never fires early\n");
    {
        JLib::CancelScope s;
        const auto armed = std::chrono::steady_clock::now();
        JLib::TimerQueue::Instance().Arm(ms(120), s.Token());

        Check(WaitUntil([&] { return s.Cancelled(); }), "it fired");
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - armed).count();

        char msg[128];
        std::snprintf(msg, sizeof msg, "it fired at %lldms, not before its 120ms deadline",
                      (long long)elapsed);
        Check(elapsed >= 118, msg);   // 2ms of slack for tick rounding, no more
    }

    std::printf("Disarm stops it\n");
    {
        JLib::CancelScope s;
        auto h = JLib::TimerQueue::Instance().Arm(ms(30), s.Token());
        Check(JLib::TimerQueue::Instance().Disarm(h), "Disarm reports that it removed the timer");
        Check(!JLib::TimerQueue::Instance().Disarm(h), "disarming twice removes nothing the second time");

        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(!s.Cancelled(), "and the scope was never cancelled");
    }

    // The RAII spelling, which is what a lexical wait should use: the operation finishing early takes
    // its own timer out of the wheel on the way past.
    std::printf("Deadline disarms itself when the operation finishes first\n");
    {
        JLib::CancelScope s;
        {
            JLib::Deadline d(ms(30), s.Token());
            Check(d.Armed(), "armed on construction");
        }   // operation completed; destructor disarms
        std::this_thread::sleep_for(std::chrono::milliseconds(120));
        Check(!s.Cancelled(), "the scope outlived its deadline uncancelled");
    }

    // THE ONE THAT WOULD BE SILENT AND AWFUL. An operation that completes early destroys its scope
    // while the timer is still queued. When that timer fires it must cancel NOTHING -- not the scope
    // that inherited the freed slot, which in a server is an unrelated live connection.
    std::printf("a timer that outlives its scope cancels nobody\n");
    {
        {
            JLib::CancelScope doomed;
            JLib::TimerQueue::Instance().Arm(ms(40), doomed.Token());
        }   // scope gone, timer still armed

        // The next scope takes the freed slot (the cancel free list is LIFO).
        JLib::CancelScope squatter;
        std::this_thread::sleep_for(std::chrono::milliseconds(160));
        Check(!squatter.Cancelled(), "the scope that inherited the slot was not cancelled");
    }

    // The integration that makes a timeout useful: setting the cancel flag does NOT wake anyone --
    // a parked task has already passed its last suspension point -- so the timer runs an EJECT that
    // wakes it. Without that, this test hangs at WaitFor.
    std::printf("a deadline wakes a task already parked on a semaphore\n");
    {
        JLib::SchedulerSemaphore sem(0);
        JLib::CancelScope conn;
        JLib::CancelScope op(conn.Token());      // nested: a timeout OR the connection dying
        std::atomic<int> cancelled{ 0 }, acquired{ 0 }, parked{ 0 };
        JLib::WaitGroup wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);

        auto* t = sched.CreateTask([&] {
            parked.fetch_add(1, std::memory_order_relaxed);
            if (sem.WaitCancellable() == JLib::WaitResult::Cancelled)
                cancelled.fetch_add(1, std::memory_order_relaxed);
            else
                acquired.fetch_add(1, std::memory_order_relaxed);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        t->cancelToken = op.Token().Raw();
        t->waitGroup = &wg;
        sched.Push(t);

        Check(WaitUntil([&] { return parked.load() == 1; }), "the waiter parked on the semaphore");

        // Nobody will ever Signal this semaphore. Only the deadline can end the wait.
        JLib::Deadline d(ms(60), op.Token(), JLib::EjectSemaphore, &sem);
        sched.WaitFor(wg);

        Check(cancelled.load() == 1, "the deadline woke it with Cancelled");
        Check(acquired.load() == 0, "it did not believe it held a permit");
        Check(!sem.Try_Wait(), "no permit was invented");
        Check(op.Cancelled(), "and the flag is set, so any later check agrees");
        Check(!conn.Cancelled(), "the timeout did not escape upward to the connection");


    }

    // Cascading. Entries beyond level 0's 256-tick window start in a coarser level and have to be
    // moved down as time approaches; an entry lost in that move would simply never fire, which no
    // short test would notice. These deadlines straddle the level-0/level-1 boundary.
    std::printf("timers cascade down from the coarser levels\n");
    {
        constexpr int kN = 6;
        std::vector<JLib::CancelScope> scopes(kN);
        // 200ms is inside level 0 (256 ticks); 300 and 400 are not, so they start at level 1.
        const int64_t delays[kN] = { ms(200), ms(260), ms(300), ms(350), ms(400), ms(500) };
        for (int i = 0; i < kN; ++i)
            JLib::TimerQueue::Instance().Arm(delays[i], scopes[i].Token());

        int fired = 0;
        WaitUntil([&] {
            fired = 0;
            for (auto& s : scopes) if (s.Cancelled()) ++fired;
            return fired == kN;
        }, 6000);

        char msg[128];
        std::snprintf(msg, sizeof msg, "all %d cascaded timers fired (got %d)", kN, fired);
        Check(fired == kN, msg);
    }

    // Ordering. A wheel buckets by the deadline's bits rather than sorting, so an entry landing in
    // the wrong slot shows up as an out-of-order firing rather than as a lost one.
    std::printf("deadlines fire in order\n");
    {
        constexpr int kN = 8;
        std::vector<JLib::CancelScope> scopes(kN);
        std::atomic<int> seq{ 0 };
        int order[kN];
        for (int i = 0; i < kN; ++i) order[i] = -1;

        for (int i = 0; i < kN; ++i)
            JLib::TimerQueue::Instance().Arm(ms(40 + 30 * i), scopes[i].Token());

        WaitUntil([&] {
            for (int i = 0; i < kN; ++i)
                if (order[i] < 0 && scopes[i].Cancelled())
                    order[i] = seq.fetch_add(1, std::memory_order_relaxed);
            for (int i = 0; i < kN; ++i) if (order[i] < 0) return false;
            return true;
        }, 6000);

        bool ordered = true;
        for (int i = 1; i < kN; ++i) if (order[i] < order[i - 1]) ordered = false;
        Check(ordered, "each deadline was observed cancelled after the one before it");
    }

    // Bulk arm-and-cancel, which is the shape an I/O server actually produces: a timeout per
    // request, nearly all of them removed before firing. Exercises the free list and the occupancy
    // bitmap far more than the expiry path does.
    std::printf("mass arm and disarm leaves the wheel empty\n");
    {
        constexpr int kN = 4000;
        std::vector<JLib::CancelScope> scopes(kN);
        std::vector<JLib::TimerHandle> handles;
        handles.reserve(kN);

        const size_t before = JLib::TimerQueue::Instance().PendingCount();
        for (int i = 0; i < kN; ++i)
            handles.push_back(JLib::TimerQueue::Instance().Arm(ms(5000 + i), scopes[i].Token()));

        Check(JLib::TimerQueue::Instance().PendingCount() == before + kN,
              "every timer was accounted for while armed");

        size_t removed = 0;
        for (auto h : handles) if (JLib::TimerQueue::Instance().Disarm(h)) ++removed;

        char msg[128];
        std::snprintf(msg, sizeof msg, "all %d were removed before firing (got %zu)", kN, removed);
        Check(removed == kN, msg);
        Check(JLib::TimerQueue::Instance().PendingCount() == before,
              "and the pending count came back to where it started");

        int anyCancelled = 0;
        for (auto& s : scopes) if (s.Cancelled()) ++anyCancelled;
        Check(anyCancelled == 0, "none of them cancelled anything on the way out");
    }

    // Armed from several threads at once. The wheel is one mutex today; this is what would have to
    // stay green if that ever becomes per-slot locks.
    std::printf("arming concurrently from many threads\n");
    {
        constexpr int kThreads = 8, kPer = 200;
        std::vector<std::thread> ts;
        std::atomic<int> armed{ 0 };
        std::vector<std::vector<JLib::CancelScope>> scopes(kThreads);

        for (int t = 0; t < kThreads; ++t) {
            scopes[t] = std::vector<JLib::CancelScope>(kPer);
            ts.emplace_back([&, t] {
                for (int i = 0; i < kPer; ++i) {
                    auto h = JLib::TimerQueue::Instance().Arm(ms(30), scopes[t][i].Token());
                    if (h.Valid()) armed.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
        for (auto& th : ts) th.join();
        Check(armed.load() == kThreads * kPer, "every concurrent Arm returned a valid handle");

        int fired = 0;
        WaitUntil([&] {
            fired = 0;
            for (auto& v : scopes) for (auto& s : v) if (s.Cancelled()) ++fired;
            return fired == kThreads * kPer;
        }, 8000);

        char msg[128];
        std::snprintf(msg, sizeof msg, "all %d of them fired (got %d)", kThreads * kPer, fired);
        Check(fired == kThreads * kPer, msg);
    }

    // An invalid scope has nothing to cancel, so arming against it should be refused outright rather
    // than occupying a slot and waking the thread for a token that can never resolve.
    std::printf("arming against a token with no scope is refused\n");
    {
        const size_t before = JLib::TimerQueue::Instance().PendingCount();
        Check(!JLib::TimerQueue::Instance().Arm(ms(10), JLib::CancelToken{}).Valid(),
              "an absent token gets an invalid handle");
        Check(JLib::TimerQueue::Instance().PendingCount() == before,
              "and nothing was queued");
    }

    JLib::TimerQueue::Instance().Stop();

    std::printf("\n%s\n", g_failures == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_failures == 0 ? 0 : 1;
}
