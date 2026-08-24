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
    const JLib::TaskScheduler::SlabSizes def = JLib::TaskScheduler::CurrentSlabSizes();
    Check(def.slots256 == 1024 * 1024 / 4, "default 256-byte pool is 256K slots (64 MB)");
    Check(def.slots128 == 1024 * 1024 / 8, "default 128-byte pool is 128K slots (16 MB)");
    Check(def.slots80  == 1024 * 1024,     "default 80-byte pool is 1M slots (80 MB) -- the primary");
    Check(def.slots64  == 1024 * 1024 / 4, "default 64-byte pool is 256K slots (16 MB)");
    Check(def.slots256 * 256 + def.slots128 * 128 + def.slots80 * 80 + def.slots64 * 64
              == 176u * 1024 * 1024,       "defaults reserve 176 MB in total, down from 256 MB");

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
    char what[128];
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

    // No manual cleanup of `held` -- these were never pushed, so nothing but memory is owned, and
    // the process exits immediately after. DestroyTask() is not public API for a standalone task
    // like this, so nothing here relies on finding it.
    sched.Join();
    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
