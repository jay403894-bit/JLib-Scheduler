// ONE FIBER, SEVERAL KINDS OF DEBT, EACH RELEASED EXACTLY ONCE.
//
// A fiber can owe several things at once, allocated several different ways -- one from `new`, one
// from a custom arena, one from a pool. `owedKinds` already records WHICH kinds are outstanding;
// FiberDebt is where the payloads live, and this file is about the two ways that goes wrong.
//
// WHY A LIST AND A LOOP, rather than one pointer and one deleter: THE REAPER REACHES EACH CREDITOR
// EXACTLY ONCE. `creditors` is a bitmask and TakeCreditor clears the bit before dispatching, which
// is what makes a double release impossible -- and it means that single visit has to discharge
// everything that worker is owed, of every kind. One pointer could not.
//
// THE TWO FAILURES, and both produce a plausible-looking run:
//
//   MISSED   a debt that is never released leaks. Nothing crashes, nothing reports it, and the
//            process is simply larger than it should be an hour later.
//   DOUBLE   a debt released twice frees memory somebody else now owns. Usually it does not fault
//            either -- it corrupts, and the fault arrives somewhere unrelated.
//
// So the assertions are on EXACT COUNTS, per object. "Everything was released" and "nothing was
// released twice" are different claims and a test that only sums them cannot tell one failure from
// the other cancelling it out.

#include "TaskScheduler.h"
#include "fiber_body.h"
#include "Fiber.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-72s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

// ---- three DIFFERENT allocation kinds, so the combination is real and not three of one ---------

static std::atomic<int> g_deletedNew{ 0 };
static std::atomic<int> g_freedArena{ 0 };
static std::atomic<int> g_freedPool{ 0 };

struct NewOwned {                 // released with `delete`
    FiberDebt debt;
    int magic = 0xA1;
    ~NewOwned() { g_deletedNew.fetch_add(1, std::memory_order_relaxed); }
};

struct ArenaOwned {               // released with a custom function, NOT delete
    FiberDebt debt;
    int magic = 0xB2;
};
static void ArenaFree(void* p) noexcept {
    g_freedArena.fetch_add(1, std::memory_order_relaxed);
    std::free(p);
}

struct PoolOwned {                // a third kind, released a third way
    FiberDebt debt;
    int magic = 0xC3;
};
static void PoolFree(void* p) noexcept {
    g_freedPool.fetch_add(1, std::memory_order_relaxed);
    std::free(p);
}

// ---- FIBER BODIES: NAMED FUNCTIONS, EXPLICIT CONTEXTS ---------------------------------------
//
// One spelling for every task body -- not a lambda, and not a captureless one either, so there is
// never a per-site judgement about which form is safe. A fiber's stack is a PLACE: register state
// mapped to memory, kept intact across a suspension. A closure is a VALUE the worker loop frees
// the moment the body returns. A fiber handed a closure that then parks either resumes into a dead
// frame or never dies and never returns its row, and neither surfaces where it was caused because
// SlabPool is append-only and a released slot stays mapped holding its old bytes.
struct DebtCtx  { std::atomic<int>* ran; std::atomic<int>* registered; };
struct DirtyCtx { std::atomic<int>* ran; std::atomic<int>* dirty; };

static void ThreeDebtsBody(void* p) {
    auto& c = *static_cast<DebtCtx*>(p);
    // Three objects, three allocators, one fiber. This is the combination the list exists for --
    // a single deleter slot could hold one of these.
    auto* a = new NewOwned();
    auto* b = (ArenaOwned*)std::malloc(sizeof(ArenaOwned));
    auto* d = (PoolOwned*)std::malloc(sizeof(PoolOwned));
    if (!b || !d) return;
    b->debt = FiberDebt{}; b->magic = 0xB2;
    d->debt = FiberDebt{}; d->magic = 0xC3;

    int n = 0;
    n += TaskScheduler::DeleteOnFiberDeath(a->debt, a)                     ? 1 : 0;
    n += TaskScheduler::ReleaseOnFiberDeath(b->debt, b, &ArenaFree)        ? 1 : 0;
    n += TaskScheduler::ReleaseOnFiberDeath(d->debt, d, &PoolFree)         ? 1 : 0;
    c.registered->fetch_add(n, std::memory_order_relaxed);
    c.ran->fetch_add(1, std::memory_order_release);
}

// Pure churn, to turn the pool over so the fibers above are RECYCLED and not merely finished.
static void ChurnBody(void*) {}

static void CleanSlotBody(void* p) {
    auto& c = *static_cast<DirtyCtx*>(p);
    Thread* th = Thread::GetCurrent();
    if (th && th->currentFiber && th->currentFiber->debts != nullptr)
        c.dirty->fetch_add(1, std::memory_order_relaxed);
    c.ran->fetch_add(1, std::memory_order_release);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== fiber debts: every kind released, each exactly once ===\n");

    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();

    // ---- OFF A FIBER IT MUST REFUSE, and say so ------------------------------------------------
    //
    // Returning false rather than doing nothing is the whole contract: there is no fiber to attach
    // to, so the CALLER still owns the object. A caller that assumed otherwise has just leaked it,
    // and silence is what would let that ship.
    {
        FiberDebt node;
        NewOwned* p = new NewOwned();
        const bool ok = TaskScheduler::DeleteOnFiberDeath(node, p);
        Check(!ok, "registering a debt OFF a fiber is REFUSED (main still owns the object)");
        Check(g_deletedNew.load() == 0, "and nothing was released behind the caller's back");
        delete p;                                   // caller still owns it, exactly as told
        g_deletedNew.store(0, std::memory_order_relaxed);
    }

    // ---- THE COMBINATION: three kinds on ONE fiber ---------------------------------------------
    const int kRounds = 200;
    {
        std::atomic<int> ran{ 0 };
        std::atomic<int> registered{ 0 };

        // THE CONTEXT IS DECLARED OUTSIDE THE LOOP because it must outlive every task that points
        // at it, and nothing here varies per iteration.
        DebtCtx dctx{ &ran, &registered };
        for (int i = 0; i < kRounds; ++i) {
            auto* t = JLibTest::MakeCtxTask(sched, &ThreeDebtsBody, &dctx);
            if (t) sched.Push(t);
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (ran.load(std::memory_order_acquire) < kRounds
               && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        // The fibers have to be RECYCLED, not merely finished -- release runs in ResetForReuse. A
        // fiber sitting in a worker's cache has not been recycled yet, so push enough churn to turn
        // the pool over rather than sleeping and hoping.
        for (int i = 0; i < kRounds * 2; ++i) {
            auto* t = JLibTest::MakeCtxTask(sched, &ChurnBody, nullptr);
            if (t) sched.Push(t);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(400));

        const int reg = registered.load(std::memory_order_relaxed);
        const int dn = g_deletedNew.load(), fa = g_freedArena.load(), fp = g_freedPool.load();
        std::printf("  %d tasks ran, %d debts registered\n", ran.load(), reg);
        std::printf("  released: delete=%d arenaFree=%d poolFree=%d  (expected %d each)\n",
                    dn, fa, fp, kRounds);

        Check(ran.load(std::memory_order_acquire) == kRounds, "every task ran");
        Check(reg == kRounds * 3, "all three debts registered on every fiber (the combination)");

        // COUNTED SEPARATELY, NOT SUMMED. A total of 600 is also what one-missed-plus-one-doubled
        // looks like, and those are opposite bugs.
        Check(dn == kRounds, "the `new` debt released EXACTLY once per fiber -- no leak, no double");
        Check(fa == kRounds, "the arena debt released EXACTLY once, through ITS OWN deleter");
        Check(fp == kRounds, "the pool debt released EXACTLY once, through ITS OWN deleter");

        // THE DELETERS MUST NOT BE INTERCHANGEABLE. Three separate counters rather than one is what
        // proves each object went to the function registered FOR IT: a single counter would be
        // satisfied by all three going through `delete`, which for the malloc'd two is undefined.
        Check(dn + fa + fp == kRounds * 3, "and no debt was routed to another kind's deleter");
    }

    // ---- A RECYCLED FIBER MUST NOT CARRY A DEBT FORWARD ---------------------------------------
    //
    // The quiet one, same shape as the slot scrub: a leftover debt on a reused fiber releases an
    // object the NEXT occupant never registered, at a time it does not expect.
    {
        std::atomic<int> ran{ 0 }, dirty{ 0 };
        DirtyCtx cctx{ &ran, &dirty };
        for (int i = 0; i < 200; ++i) {
            auto* t = JLibTest::MakeCtxTask(sched, &CleanSlotBody, &cctx);
            if (t) sched.Push(t);
        }
        const auto d = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (ran.load(std::memory_order_acquire) < 200 && std::chrono::steady_clock::now() < d)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        std::printf("  recycle: %d/200 tasks ran, %d saw an inherited debt list\n",
                    ran.load(), dirty.load());
        Check(ran.load(std::memory_order_acquire) == 200, "every recycle-probe task ran");
        Check(dirty.load(std::memory_order_relaxed) == 0,
              "a recycled fiber NEVER carries the previous occupant's debts");
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_fail ? "FAILED" : "PASSED", g_fail, g_fail == 1 ? "" : "s");
    return g_fail ? 1 : 0;
}
