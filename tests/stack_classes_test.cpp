// THREE STACK CLASSES, AND THEY DEFAULT TO OFF -- SO ONLY THIS TEST PROVES THEY WORK.
//
// Tiny and Deep are 0 per worker by default, deliberately: the heavy class was deleted once for
// committing ~127 MB while nothing asked for it, and a nonzero default is a bill every program pays
// at startup. The consequence is that a green suite says NOTHING about them -- this file is the only
// thing that turns them on.
//
// FOUR CLAIMS:
//
//   1. A task gets the class it ASKED FOR. Asserted from INSIDE the task, on the fiber actually
//      bound to it, not from the pool's bookkeeping -- the bookkeeping being right is not the same
//      as the routing being right, and only one of those is what a caller experiences.
//
//   2. poolIndex STAYS DENSE ACROSS CLASSES. This is load-bearing far outside the pool: Event's
//      waiter table is a PERFECT HASH keyed on it, FiberRegistry's table indexes by it, and
//      HazardDomain's fiber rows do too. A gap or an overlap corrupts all three silently.
//
//   3. A RELEASED FIBER GOES HOME TO ITS OWN CLASS. Run many tiny tasks in sequence: if release
//      misfiled even one into the Standard cache, a later tiny task would be handed a 64 KB stack
//      and the class would quietly stop being tiny. One task cannot show this; a run of them can.
//
//   4. THE USABLE FIGURE IS THE PROMISE. Sizes are derived from PageSize() at runtime precisely so
//      "8 KiB usable" is not a cute number that becomes zero on a 16 KiB-page platform. Checking
//      usable >= the promise is the assertion; checking an exact byte count would fail on Apple
//      Silicon for being MORE generous than asked.

#include "../include/TaskScheduler.h"
#include "../include/Thread.h"
#include <cstdio>
#include <atomic>
#include <set>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-70s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

// K WORKERS ARE WHERE TINY FIBERS LIVE, so the test must set K or the tiny count is zero
// and every assertion below it is vacuous. That is the band budget working as designed, and it is
// exactly the kind of thing a test can fail to notice: with K=0 the pool builds no tiny stacks at
// all and a Tiny task would spin on acquisition rather than report anything.
static constexpr size_t kHotWorkers    = 2;
static constexpr size_t kTinyPerK      = 8;
static constexpr size_t kNormalPerCompute = 4;
static constexpr size_t kDeepPerCompute   = 2;

// What the fiber bound to this task actually is. Read from inside the body, which is the only place
// the answer reflects routing rather than intent.
struct Seen { size_t stackSize; int cls; size_t poolIndex; };
static std::atomic<size_t> g_seenSize{ 0 };
static std::atomic<int>    g_seenClass{ -1 };

static void RecordSelf() {
    Thread* w = Thread::Current();
    Fiber*  f = w ? w->currentFiber : nullptr;
    if (!f) { g_seenClass.store(-2, std::memory_order_release); return; }
    g_seenSize.store(f->stackSize, std::memory_order_relaxed);
    g_seenClass.store((int)f->stackClass, std::memory_order_release);
}

static Seen RunOne(TaskScheduler& sched, StackClass want) {
    g_seenSize.store(0); g_seenClass.store(-1);
    WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);
    Task* t = sched.CreateTask([] { RecordSelf(); });
    t->stackClass = want;
    t->waitGroup = &wg;
    sched.Push(t);
    sched.WaitFor(wg);
    return Seen{ g_seenSize.load(), g_seenClass.load(), 0 };
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== stack classes: tiny / standard / deep ===\n");

    // MUST precede Init -- the pool builds every fiber up front.
    // SetHotWorkers, NOT SetReservedCores -- two different things, and confusing them is what made
    // the first run of this test build ZERO tiny fibers and then HANG waiting for one.
    // SetReservedCores holds cores back FROM the pool; K is a band WITHIN it, the workers that
    // serve the I/O lane. Only K sizes the tiny class.
    TaskScheduler::SetHotWorkers(kHotWorkers);
    TaskScheduler::SetFiberBudget(kNormalPerCompute, kTinyPerK, kDeepPerCompute);

    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();
    auto& pool  = sched.GetGlobalPool();

    const size_t page = platform::PageSize();
    std::printf("  page=%zu  tiny=%zu standard=%zu deep=%zu  total=%zu\n",
                page, pool.CountOf(StackClass::Tiny), pool.CountOf(StackClass::Standard),
                pool.CountOf(StackClass::Deep), pool.TotalCount());

    const bool provisioned = pool.CountOf(StackClass::Tiny) > 0 && pool.CountOf(StackClass::Deep) > 0;
    Check(provisioned,
          "the pool actually built tiny and deep fibers (else everything below is vacuous)");
    // BAIL, DO NOT CONTINUE. A task asking for a class the pool has none of does not fail -- it
    // SPINS: AcquireFiber returns null, the task is requeued, and the worker tries again forever.
    // The first run of this file did exactly that and had to be killed. A precondition that cannot
    // be met is a reason to stop with a message, not to proceed into a hang.
    if (!provisioned) {
        std::printf("  REFUSING to continue: a Tiny or Deep task would SPIN on acquisition rather\n"
                    "  than fail, so this test would hang instead of reporting.\n");
        std::printf("=== FAILED (%d failures) ===\n", g_failures);
        return 1;
    }

    // ---- 1 + 4: the class asked for is the class bound, and usable meets the promise ----------
    {
        const Seen s = RunOne(sched, StackClass::Standard);
        std::printf("    standard: class=%d stackSize=%zu usable=%zu\n",
                    s.cls, s.stackSize, s.stackSize - page);
        Check(s.cls == (int)StackClass::Standard, "a Standard task binds a Standard fiber");
        Check(s.stackSize > page && s.stackSize - page >= 60 * 1024 - page,
              "and its usable depth meets the Standard promise");
    }
    {
        const Seen s = RunOne(sched, StackClass::Tiny);
        std::printf("    tiny:     class=%d stackSize=%zu usable=%zu\n",
                    s.cls, s.stackSize, s.stackSize - page);
        Check(s.cls == (int)StackClass::Tiny, "a Tiny task binds a TINY fiber, not a Standard one");
        // >= 2 pages, not == 8 KiB. On a 16 KiB-page platform 2 pages is 32 KiB, which is MORE than
        // promised -- an exact-bytes assertion would fail there for the wrong reason.
        Check(s.stackSize - page >= 2 * page, "and it has at least the promised 2 pages of stack");
        Check(s.stackSize < 60 * 1024, "and it really is smaller than Standard (else 'tiny' is a lie)");
    }
    {
        const Seen s = RunOne(sched, StackClass::Deep);
        std::printf("    deep:     class=%d stackSize=%zu usable=%zu\n",
                    s.cls, s.stackSize, s.stackSize - page);
        Check(s.cls == (int)StackClass::Deep, "a Deep task binds a DEEP fiber");
        Check(s.stackSize - page >= 508 * 1024, "and its usable depth meets the Deep promise");
    }

    // ---- 2: poolIndex is dense and unique across all three classes ---------------------------
    {
        const size_t total = pool.TotalCount();
        std::set<size_t> seen;
        size_t nulls = 0, dupes = 0;
        for (size_t i = 0; i < total; ++i) {
            Fiber* f = pool.At(i);
            if (!f) { ++nulls; continue; }
            if (!seen.insert(f->poolIndex).second) ++dupes;
        }
        std::printf("    poolIndex: total=%zu distinct=%zu nulls=%zu dupes=%zu  At(total)=%p\n",
                    total, seen.size(), nulls, dupes, (void*)pool.At(total));
        Check(nulls == 0,             "At() resolves every index below TotalCount (no gap)");
        Check(dupes == 0,             "and no two fibers share a poolIndex (no overlap)");
        Check(seen.size() == total,   "so the index space is DENSE across all three classes");
        Check(pool.At(total) == nullptr, "and one past the end is null, not a stray class");
    }

    // ---- 3: a released fiber goes home to its OWN class --------------------------------------
    //
    // ONE TASK CANNOT SHOW THIS. Misfiling only surfaces once a fiber has been released and handed
    // out again, so this runs a sequence longer than the tiny class is deep.
    {
        const size_t rounds = kTinyPerK * 4 + 8;
        size_t wrong = 0;
        for (size_t i = 0; i < rounds; ++i) {
            const Seen s = RunOne(sched, StackClass::Tiny);
            if (s.cls != (int)StackClass::Tiny) ++wrong;
        }
        std::printf("    %zu sequential tiny tasks: wrong-class=%zu\n", rounds, wrong);
        Check(wrong == 0,
              "release routes a fiber back to its OWN class's cache (reuse stays tiny)");
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
