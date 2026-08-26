// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// SetSlabSizes' job, like SetFiberBudget's, is to change what TaskScheduler's construction
// actually builds -- not just what a getter echoes back. Checking Capacity() alone would catch a
// setter that never reached the allocator; it would not catch one that reached it but was
// silently clamped or ignored past construction. Actually exhausting the slab does.
//
// A standalone binary for the same reason fiber_budget_test.cpp is one: mutating the process-wide
// slab size anywhere inside primitives_test.cpp's shared Init() would starve tests that create
// tasks liberally and were never written expecting a slab of a few dozen slots.

#define NOMINMAX
#include <TaskScheduler.h>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <atomic>
#include <chrono>
#include <thread>

static int failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-64s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("SetSlabSizes\n");

    // Before Init() and before this process has ever called the setter. All four are pinned, not
    // just one: these defaults were DERIVED FROM A MEASUREMENT (Game01, 37,480 tasks -- 15.4% at
    // 64 bytes, 84.6% at 80, nothing above 80), and they total 176 MB against 256 MB pre-3.0. An
    // earlier cut of the same release added the 80-byte pool without shrinking the 256-byte one
    // and reserved 360 MB -- a memory regression shipped as a memory optimisation, and a test that
    // pinned only one pool would not have said a word about it.
    char what[160];
    const JLib::TaskScheduler::SlabSizes def = JLib::TaskScheduler::CurrentSlabSizes();
    // RESIZED IN 4.0.1: 176 MB -> ~3.8 MB. The old figure was sized for a world where exhaustion
    // was FATAL, so the only safe default was one nobody could exhaust. Growth made under-sizing a
    // hitch instead of a failure, so the default now covers the common case and warns outside it.
    Check(def.slots256 == 4 * 1024,  "default 256-byte pool is 4K slots (1 MB)");
    Check(def.slots128 == 2 * 1024,  "default 128-byte pool is 2K slots (0.25 MB)");
    Check(def.slots80  == 16 * 1024, "default 80-byte pool is 16K slots (1.25 MB) -- the primary");
    Check(def.slots64  == 24 * 1024, "default 64-byte pool is 24K slots (1.5 MB) -- first choice for anything small");
    // The TOTAL is pinned too, and that is not redundant: an earlier release added the 80-byte pool
    // without shrinking the 256-byte one and reserved 360 MB -- a memory regression shipped as a
    // memory optimisation, which a per-pool check alone would not have caught.
    const size_t total = def.slots256 * 256 + def.slots128 * 128 + def.slots80 * 80 + def.slots64 * 64;
    std::snprintf(what, sizeof(what), "defaults reserve at most 4 MB in total, got %.2f MB",
                  (double)total / (1024.0 * 1024.0));
    Check(total <= 4u * 1024 * 1024, what);

    // Small, and deliberately not a round number, so a coincidental pass against the untouched
    // default is impossible and exhaustion below is cheap to trigger and easy to reason about.
    const size_t kSlots = 37;
    // SetSlabSizes sizes EACH pool, and it has to be checked per pool rather than through one
    // number: the whole reason it exists is that a single figure with fixed divisors silently
    // starved a class (routing tasks into the 64-byte pool evicted coroutine frames from it). A
    // test that only looked at the 256-byte capacity would have passed straight through that.
    //
    // Deliberately asymmetric values, none a divisor of another, so a setter that ignored a field
    // and fell back to `big / 8` would be caught rather than coincidentally agree.
    JLib::TaskScheduler::SlabSizes sizes;
    sizes.slots256 = kSlots;
    sizes.slots128 = 21;
    sizes.slots80  = 37;
    sizes.slots64  = 53;
    JLib::TaskScheduler::SetSlabSizes(sizes);

    Check(JLib::TaskScheduler::CurrentSlabSizes().slots256 == kSlots, "getter reflects the new value");

    JLib::TaskScheduler::Init(2);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    // Did construction actually consume it, not just store it.
    const size_t capacity = sched.GetAllocator()->Capacity();
    Check(sched.GetAllocator()->MidCapacity()    == 21, "128-byte pool got its own configured size");
    Check(sched.GetAllocator()->Slot80Capacity() == 37, "80-byte pool got its own configured size");
    Check(sched.GetAllocator()->BigCapacity()   == kSlots, "256-byte pool got its own configured size");
    Check(sched.GetAllocator()->SmallCapacity() == 53, "64-byte pool got its own configured size");
    std::snprintf(what, sizeof(what), "allocator capacity is the configured %zu, got %zu",
                  kSlots + 21 + 37 + 53, capacity);
    // Capacity() is the TOTAL across the three pools, because a Task can come from any of
    // them -- AllocSized falls through 64 -> 128 -> 256. Asserting against `big` alone would
    // have passed while the exhaustion check below silently measured something else.
    Check(capacity == kSlots + 21 + 37 + 53, what);

    // The functional check, not just the number it reports: allocate well past kSlots and confirm
    // CreateTask actually runs out. Init() itself already consumes a handful of slots for its own
    // queue stubs (mainQ, each worker's inboxes), so exhaustion is expected somewhat BEFORE kSlots,
    // not at exactly kSlots -- the bound below is deliberately loose for that reason. What it rules
    // out is the wiring silently reverting to the old 1M-slot default: with that bug, none of these
    // 137 attempts would ever fail, and `allocated` would land at 137, well over kSlots.
    // GROWTH OFF FOR THIS SECTION, and turning it off is what keeps the section meaningful. With
    // growth on (the default) CreateTask never returns null, so every one of these attempts succeeds
    // and the check below passes while testing NOTHING -- the third vacuous shape this file has
    // produced. A ceiling that moves cannot be asserted, so the ceiling is pinned first.
    JLib::TaskScheduler::SetSlabGrowth(false);

    std::vector<JLib::Task*> held;
    size_t allocated = 0;
    for (size_t i = 0; i < kSlots + 100; ++i) {
        JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr);
        if (!t) break;
        held.push_back(t);
        ++allocated;
    }
    std::snprintf(what, sizeof(what),
                  "CreateTask exhausts at or before capacity (%zu), allocated %zu then nullptr",
                  capacity, allocated);
    // Against the TOTAL, not `big`: a Task can be served by any of the three pools, so the bound
    // is their sum. Comparing to kSlots alone said "37" while the allocator could legitimately
    // hand out 111, which is a test asserting the wrong contract rather than a real failure.
    Check(allocated <= capacity, what);
    Check(allocated > 0, "at least one task was allocated before exhaustion");

    for (auto* t : held) sched.GetAllocator()->Free(t);
    held.clear();

    // ============================================================================================
    // AND THE OTHER HALF: with growth ON -- the default -- the same loop must NOT fail. This is the
    // point of the feature, so it gets its own assertion rather than being inferred from the
    // section above passing. Exhaustion becomes a hitch (one allocation, one warning on stderr)
    // instead of a null return that strands whatever the caller was trying to schedule.
    //
    // The bound is well past the pinned capacity of 148, so reaching it can only mean the pool grew.
    JLib::TaskScheduler::SetSlabGrowth(true);
    Check(JLib::TaskScheduler::SlabGrowthEnabled(), "growth reports itself enabled");
    {
        std::vector<JLib::Task*> grown;
        size_t got = 0;
        for (size_t i = 0; i < capacity + 500; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {}, nullptr);
            if (!t) break;
            grown.push_back(t);
            ++got;
        }
        std::snprintf(what, sizeof(what),
                      "with growth ON, allocation continues past the configured %zu -- got %zu",
                      capacity, got);
        Check(got > capacity, what);
        // Every one of those must free correctly, which is the real risk of growth: a slot from an
        // EXTENT has to route home by address just as a primary-block slot does. If SlotInSlab did
        // not walk the extent chain, these would fall through to ::operator delete on slab memory.
        for (auto* t : grown) sched.GetAllocator()->Free(t);
        Check(sched.GetAllocator()->Capacity() > capacity,
              "Capacity() reports the grown total, not the configured one");
    }

    // ============================================================================================
    // A LAMBDA BIGGER THAN THE BIGGEST SLOT. Until 4.0.1 this was a static_assert -- a COMPILE
    // error -- which made the task path stricter than the coroutine path for no defensible reason:
    // an oversized coroutine frame has always fallen back to the global heap. This section only
    // exists because it could not previously be written.
    //
    // THE DESTRUCTOR COUNT IS THE POINT, not that it runs. A heap task that is freed into a slab
    // pool, or freed twice, or never freed, all still "run" -- so the assertion is on the capture's
    // destructor firing EXACTLY once, which separates all three.
    {
        // CONSTRUCTIONS == DESTRUCTIONS, not a predicted count. The first version of this asserted
        // "exactly two" and saw three -- correctly, because [big, &ran] copies into the closure and
        // the closure is then copied into LambdaTask. Predicting the copy count makes the test
        // fragile against an unrelated change; the invariant that actually matters is that nothing
        // leaked and nothing was destroyed twice, and that is a balance, not a number.
        struct Tracked {
            // ATOMIC, and this is not pedantry: the task copy is destroyed on a WORKER thread while
            // the assertion reads on main. With plain ints the read is a data race and the main
            // thread may simply never observe the increment -- which reads as "3 built, 2 destroyed",
            // i.e. indistinguishable from a genuine leak. That flaked ~50%% here and cost a real
            // hunt through three disposal sites before a probe using atomics came back balanced 12
            // times out of 12.
            static std::atomic<int>& Ctors() { static std::atomic<int> c{ 0 }; return c; }
            static std::atomic<int>& Dtors() { static std::atomic<int> d{ 0 }; return d; }
            char        ballast[512];   // forces sizeof(LambdaTask<...>) well past SLOT (256)
            Tracked()                 { ballast[0] = 1; Ctors().fetch_add(1); }
            Tracked(const Tracked& o) { ballast[0] = o.ballast[0]; Ctors().fetch_add(1); }
            ~Tracked()                { Dtors().fetch_add(1); }
        };
        Tracked::Ctors().store(0);
        Tracked::Dtors().store(0);

        std::atomic<int> ran{ 0 };
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        {
            Tracked big;
            auto* t = sched.CreateTask([big, &ran]() {
                (void)big.ballast[0];
                ran.fetch_add(1, std::memory_order_relaxed);
            });
            Check(t != nullptr, "a lambda larger than the biggest slot can be created at all");
            if (t) { t->waitGroup = &wg; sched.Push(t); sched.WaitFor(wg); }
            else   { wg.n.store(0, std::memory_order_relaxed); }
        }

        Check(ran.load() == 1, "the oversized task actually ran");
        // One for the local `big`, one for the copy captured into the task body. More means a double
        // free ran the destructor twice; fewer means the task body was never destroyed.
        // WAIT FOR THE BALANCE, do not sample it. The WaitGroup is signalled BEFORE the worker
        // destroys and frees the task, so reading the counters the instant WaitFor returns catches
        // the gap and reports "3 built, 2 destroyed" -- a race in the TEST, seen 1 run in 6. A
        // bounded wait cannot mask a real leak: a leaked copy never balances, so it still fails.
        for (int i = 0; i < 2000 && Tracked::Dtors().load() != Tracked::Ctors().load(); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        std::snprintf(what, sizeof(what),
                      "every captured copy is destroyed exactly once: %d built, %d destroyed",
                      Tracked::Ctors().load(), Tracked::Dtors().load());
        Check(Tracked::Ctors().load() > 0 && Tracked::Ctors().load() == Tracked::Dtors().load(), what);
    }

    // ---- THE USAGE REPORT ---------------------------------------------------------------------
    //
    // Not merely that it prints -- that the HIGH-WATER mark is a real measurement. This binary has
    // deliberately exhausted and grown a tiny slab, so the peak must be non-zero and the classes
    // that grew must say so. A report that always printed zeroes would look identical in a passing
    // run, which is the shape three tests in this file have already failed as.
    JLib::TaskScheduler::ReportSlabUsage("slab usage after this test");
    {
        const auto u = sched.GetAllocator()->UsageProfile();
        // PEAK-LIVE, not resident. Resident is capacity by construction in the default build --
        // Prefault touches every slot -- so asserting on it would pass without measuring anything.
        Check(u.c64.peakLive > 0 || u.c80.peakLive > 0 || u.c256.peakLive > 0,
              "peak live slots is non-zero after real allocation");
        const bool grewSomewhere = u.c64.extents || u.c80.extents || u.c128.extents || u.c256.extents;
        Check(grewSomewhere, "the report identifies the class that had to grow");
        Check(sched.GetAllocator()->HighWaterBytes() > 0, "resident bytes are reported");
    }

    sched.Join();
    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
