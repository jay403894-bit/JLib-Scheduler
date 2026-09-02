// THE CONTRACT THAT REPLACED COUNTED EPOCHS.
//
// Counted epochs existed so a coroutine could hold an EpochGuard across a suspension: the counter
// was sharded so enter and leave need not use the same shard, which survived the migration. That is
// gone, and what pays for it is a check that FIRES IN RELEASE.
//
// So this is not a nice-to-have. The entire safety argument for deleting the ring is "the rule was
// already forbidden and is now actually enforced", and an enforcement nothing tests is a claim
// rather than an enforcement. tests/coro_epoch_test.cpp was deleted with the mechanism it covered --
// its first case was titled "a parked coroutine keeps its epoch protection", which is exactly the
// capability that no longer exists.
//
// TWO ARMS, AND THE CLEAN ONE IS THE POINT. A test that only checked "violating fires the handler"
// would pass if the handler fired on EVERY suspension -- which would make every correct coroutine
// abort in Release, a far worse bug than the one being prevented. The clean arm separates "detects
// the violation" from "detects a co_await".
//
// CLEAN RUNS FIRST, deliberately. If the violating arm ran first it would leave the counter
// nonzero, and a clean arm that wrongly fired would be invisible underneath it.
//
// The handler seam replaces what happens AFTER detection, never whether detection runs -- a
// regression test cannot assert on a process that has already aborted.

#include "../include/TaskScheduler.h"
#include "../include/Coroutine.h"
#include "../include/Thread.h"
#include <cstdio>
#include <atomic>
#include <chrono>
#include <thread>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-68s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

static std::atomic<int> g_violations{ 0 };
static void RecordViolation() { g_violations.fetch_add(1, std::memory_order_release); }

// BOUNDED, NOT TaskScheduler::WaitFor. A test that hangs does not fail -- it spins every worker
// until something kills it, and the earlier version of this file burned 1,235 CPU-seconds doing
// exactly that while the rest of the suite ran alongside it and went flaky. A deadline turns a hang
// into a reported failure, which is the difference between a diagnosis and a mystery.
static bool AwaitWaitGroup(WaitGroup& wg, int ms = 3000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while ((wg.n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK) != 0) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

// CLEAN: the guard is dropped before the suspension, which is the documented fix.
static JLib::Coro DropsGuardBeforeAwait() {
    { EpochGuard guard; (void)guard; }        // the traversal ends here
    co_await JLib::Reschedule{};              // ...and only then does it suspend
}

// VIOLATING: the guard is still alive across the suspension. Announced on THIS worker, and its
// destructor will run on whichever worker resumes it.
static JLib::Coro HoldsGuardAcrossAwait() {
    EpochGuard guard;
    co_await JLib::Reschedule{};
    (void)guard;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== coroutine epoch contract: a guard may not span a suspension ===\n");

    SetCoroSuspendViolationHandlerForTest(&RecordViolation);
    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();

    {
        g_violations.store(0);
        // NOT pre-set to 1: Spawn does its own fetch_add on the WaitGroup. Setting it here as well
        // made the count 2 against one decrement, and WaitFor spun forever -- 1,235 CPU-seconds
        // before it was killed. The count belongs to whoever creates the work.
        WaitGroup wg;
        const bool ok = Spawn(DropsGuardBeforeAwait(), &wg);
        Check(ok, "the clean coroutine spawned (else nothing below is tested)");
        Check(!ok || AwaitWaitGroup(wg), "the clean coroutine finished (not a hang)");
        std::printf("    clean arm: violations=%d\n", g_violations.load());
        Check(g_violations.load() == 0,
              "a coroutine that DROPS its guard before suspending is NOT flagged");
    }

    {
        g_violations.store(0);
        WaitGroup wg;
        const bool ok = Spawn(HoldsGuardAcrossAwait(), &wg);
        Check(ok, "the violating coroutine spawned");
        Check(!ok || AwaitWaitGroup(wg), "the violating coroutine finished (not a hang)");
        std::printf("    violating arm: violations=%d\n", g_violations.load());
        Check(g_violations.load() > 0,
              "holding an EpochGuard across a suspension IS caught -- in RELEASE");
    }

    SetCoroSuspendViolationHandlerForTest(nullptr);   // restore the real abort
    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
