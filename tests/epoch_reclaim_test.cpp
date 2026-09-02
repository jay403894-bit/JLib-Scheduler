// DOES EPOCH RECLAMATION STILL ACTUALLY FREE ANYTHING?
//
// Nothing in the suite asserted this before, which was survivable while a global queue meant ANY
// reclaimer drained EVERY retirement. Per-thread bags change that: only the owner drains its own,
// so "the suite is green" no longer implies memory comes back. This test exists because the change
// that made it necessary is the same change that made it possible for it to fail.
//
// FOUR CLAIMS, and the middle two are the ones the rework introduced:
//
//   1. RECLAIM WORKS         retire, tick, the deleters ran.
//   2. NEGATIVE CONTROL      without a tick, NOTHING is freed. If this fails, claim 1 is vacuous --
//                            it would pass on a build that freed eagerly, or never protected at all.
//   3. BAGS ARE PRIVATE      another thread's tick does NOT free my garbage. This is the documented
//                            behaviour change: the old global queue would have. Asserting it stops
//                            it being rediscovered as a bug.
//   4. THREAD EXIT HANDS OFF a thread that retires and then EXITS must not strand its bag. This is
//                            the safety net for claim 3 -- without it, private bags would turn a
//                            short-lived thread into an unconditional leak.

#include "../include/TaskScheduler.h"
#include "../include/Epochs.h"
#include <cstdio>
#include <atomic>
#include <thread>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-70s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

static std::atomic<int> g_freed{ 0 };
struct Victim { int magic = 0xBEEF; };
static void FreeVictim(void* p) {
    g_freed.fetch_add(1, std::memory_order_release);
    delete static_cast<Victim*>(p);
}

// Retire `n` pointers at the CURRENT epoch from whatever thread calls this.
static void RetireSome(EpochManager& em, int n) {
    const size_t e = em.CurrentEpoch();
    for (int i = 0; i < n; ++i) em.RetirePtr(new Victim{}, e, &FreeVictim);
}

// Advancing twice guarantees the retired epoch is strictly below the minimum any reader can be
// announced at -- one advance is not enough, since a retirement AT epoch E is only safe once the
// minimum has passed E.
static void AdvanceAndReclaim(EpochManager& em) {
    em.AdvanceEpoch();
    em.AdvanceEpoch();
    em.TryReclaim();
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== epoch reclamation: per-thread bags still free memory ===\n");

    // A pool, because EpochManager::Init sizes the thread slots and StartPool does that.
    TaskScheduler::Init(4);
    auto& em = EpochManager::Instance();

    // ---- 2 FIRST: the negative control, so a build that frees eagerly is caught before claim 1 --
    {
        g_freed.store(0);
        RetireSome(em, 32);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        std::printf("    retired 32, no tick: freed=%d\n", g_freed.load());
        Check(g_freed.load() == 0,
              "NEGATIVE CONTROL: without a reclaim, nothing is freed (else claim 1 is vacuous)");
    }

    // ---- 1: and it does free once reclaimed --------------------------------------------------
    {
        AdvanceAndReclaim(em);
        std::printf("    after tick: freed=%d\n", g_freed.load());
        Check(g_freed.load() == 32, "reclaim frees everything this thread retired");
    }

    // ---- 3: BAGS ARE PRIVATE -- the documented behaviour change ------------------------------
    //
    // A raw std::thread retires, then MAIN ticks. Under the old global `incoming` queue main would
    // have freed them. It must not now: the bag belongs to the thread that filled it.
    {
        g_freed.store(0);
        std::thread other([&] { RetireSome(em, 16); });
        other.join();     // the thread is GONE -- but see claim 4, its bag was handed off at exit

        // Main ticks. Main's own bag is empty, so this must free nothing OF ITS OWN -- but the
        // orphan handoff from the joined thread is legitimately sweepable, which is claim 4.
        AdvanceAndReclaim(em);
        std::printf("    other thread retired 16, then exited; main ticked: freed=%d\n",
                    g_freed.load());
        Check(g_freed.load() == 16,
              "a thread that retires and EXITS does not strand its bag (orphan handoff)");
    }

    // ---- 3 proper: a LIVE thread's bag is not drained by someone else -------------------------
    {
        g_freed.store(0);
        std::atomic<bool> retired{ false }, mayExit{ false };
        std::thread holder([&] {
            RetireSome(em, 8);
            retired.store(true, std::memory_order_release);
            while (!mayExit.load(std::memory_order_acquire)) std::this_thread::yield();
        });
        while (!retired.load(std::memory_order_acquire)) std::this_thread::yield();

        AdvanceAndReclaim(em);                       // main ticks while the owner is STILL ALIVE
        const int freedByOther = g_freed.load();
        std::printf("    owner alive, main ticked: freed=%d (expect 0)\n", freedByOther);
        Check(freedByOther == 0,
              "another thread's tick does NOT drain a live thread's bag (bags are private)");

        mayExit.store(true, std::memory_order_release);
        holder.join();                               // now it hands off at exit
        AdvanceAndReclaim(em);
        std::printf("    owner exited, main ticked: freed=%d\n", g_freed.load());
        Check(g_freed.load() == 8, "and once the owner is gone the handoff makes them reclaimable");
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
