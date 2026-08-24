// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "SlabPool.h"
#include <cstddef>

namespace JLib {

    // The scheduler's memory: one place every Task, TaskNode, DAG edge chunk and coroutine frame
    // comes from. That single-source property is deliberate and worth defending -- it is what makes
    // "no heap traffic at runtime" checkable rather than aspirational, and what lets a slab-size
    // number mean something.
    //
    // SIZE CLASSES, and why there are three. Everything used to be one 256-byte slot, sized for the
    // biggest Task. Then coroutine frames were measured (bench/coroutine_bench.cpp with
    // -DJLIBSCHED_CORO_STATS=ON, 640,008 allocations):
    //
    //     <=64 bytes   75.0%      <=128 bytes   25.0%      <=256 bytes   ~0%      largest 224
    //
    // Three quarters of frames were sitting in a 256-byte slot using a quarter of it -- and, worse,
    // occupying a slot a Task could not have, since frames and Tasks share this slab by design. So
    // the waste was contention, not just footprint.
    //
    // THE CLASSES WERE CHOSEN BY ARITHMETIC, not by taste. Average bytes saved per frame:
    //
    //     64 + 256          0.75 x 192                 = 144 B
    //     128 + 256         1.00 x 128                 = 128 B     <-- worse than 64 alone
    //     64 + 128 + 256    0.75 x 192 + 0.25 x 128    = 176 B
    //
    // 64 is the single best addition and 128 adds ~22% on top of it. A 128-only split would have
    // been WORSE than 64-only, which is the kind of thing that is obvious afterwards and invisible
    // beforehand.
    //
    // NOT A SPEED CHANGE, though it turned out to be one: the A/B (bench/frame_class.cpp, arms
    // interleaved in one process against an A/A control) put the 64-byte class 17.2% ahead on frame
    // alloc+free. That was not the goal and should not be quoted as the justification; the memory
    // and slab-pressure argument above is.
    class TaskAllocator {
    public:
        // Kept as TaskAllocator::SLOT rather than SlabPool<256>::SLOT because callers use it to size
        // things against the slab -- TaskDAG cuts edge chunks to exactly one slot, Coroutine.h picks
        // a class by it.
        static constexpr std::size_t SLOT       = 256;
        static constexpr std::size_t MID_SLOT   = 128;
        static constexpr std::size_t SMALL_SLOT = 64;
        static constexpr std::size_t BATCH      = SlabPool<SLOT>::BATCH;

    private:
        // Distinct template arguments, so each of these has its own per-thread cache and its own
        // sharded live counters. That is the entire reason SlabPool is a template -- see its header
        // for the two bugs that hand-duplicating a pool produced.
        SlabPool<SLOT>       big;
        SlabPool<MID_SLOT>   mid;
        SlabPool<SMALL_SLOT> small;

    public:
        // EXPLICIT PER-CLASS SIZES rather than divisors off one number. Memory in one pool
        // cannot serve a request from another, so a single figure cannot size three pools
        // without either wasting memory or starving a consumer -- see TaskScheduler::SlabSizes
        // for the case that proved it (tasks routed into the 64-byte class silently evicted
        // coroutine frames from a pool sized `slots / 8` for frames alone).
        explicit TaskAllocator(std::size_t bigSlots, std::size_t midSlots,
                               std::size_t smallSlots, bool lazy = false)
            : big(bigSlots, lazy)
            , mid(midSlots, lazy)
            , small(smallSlots, lazy) {
        }

        // Convenience for callers with one number, preserving the historical derivation.
        explicit TaskAllocator(std::size_t slots, bool lazy = false)
            : TaskAllocator(slots, slots / 8, slots / 8, lazy) {
        }
        TaskAllocator& operator=(const TaskAllocator&) = delete;

        // ---- the Task path, unchanged --------------------------------------------------------
        // Always the 256-byte class. Tasks are a fixed size and there is nothing to choose.
        void* Alloc()                     { return big.Alloc(); }
        // ROUTES BY ADDRESS, and this is a safety requirement rather than a convenience.
        // Tasks and TaskNodes come from AllocSized, so they may live in ANY of the three
        // pools, and returning one to the wrong pool's free list is immediate heap
        // corruption -- the failure class this allocator already has history with.
        //
        // Doing it here rather than at each call site is deliberate. There are nine task-free
        // sites across four files (Thread.cpp x3, TaskScheduler.cpp x4, TaskDAG.cpp, and
        // Coroutine.h), an audit found several that a first pass missed, and any Free(task)
        // added later would be a silent corruption waiting to happen. Correct by construction
        // beats correct by vigilance on a path that crashes days away from its cause.
        //
        // Small pool is checked first: Tasks and TaskNodes are the common case and both fit
        // the 64-byte class, so the common path is a single range check.
        void  Free(void* slot)            { if (!FreeSized(slot)) big.Free(slot); }
        bool  SlotInSlab(const void* p) const { return big.SlotInSlab(p); }
        long long LiveCount() const       { return big.LiveCount(); }
        // TOTAL across all three pools, because that is what bounds how many Tasks can exist:
        // AllocSized falls through 64 -> 128 -> 256, so a task can come from any of them.
        // Reporting `big` alone here would be the same trap SetTaskSlabSize was -- a name that
        // says "the allocator's capacity" while answering for a third of it. Per-class figures
        // are available separately for anyone who needs the split.
        std::size_t Capacity() const      { return big.Capacity() + mid.Capacity() + small.Capacity(); }
        std::size_t BigCapacity() const   { return big.Capacity(); }
        void Prefault(std::size_t slots)  { big.Prefault(slots); }

        // ---- size-classed path, for callers whose objects vary ---------------------------------
        //
        // Smallest class that fits, falling through on exhaustion so a dry small pool degrades to a
        // bigger slot rather than to a failure. Returns nullptr only when nothing fits at all --
        // callers handle that (Coroutine.h falls back to global new).
        void* AllocSized(std::size_t n) {
            if (n <= SMALL_SLOT) if (void* p = small.Alloc()) return p;
            if (n <= MID_SLOT)   if (void* p = mid.Alloc())   return p;
            if (n <= SLOT)       if (void* p = big.Alloc())   return p;
            return nullptr;
        }

        // ADDRESS decides the class, not a stored size or the caller's memory of one. The three
        // backing allocations cannot overlap, so at most one pool claims any given pointer -- which
        // is also what makes the runtime frame-pool toggle safe to flip mid-run: a slot allocated
        // under one setting still frees correctly afterwards.
        //
        // Returns false if the pointer belongs to no pool, so a caller mixing slab and heap pointers
        // can route the rest itself.
        bool FreeSized(void* p) {
            if (small.SlotInSlab(p)) { small.Free(p); return true; }
            if (mid.SlotInSlab(p))   { mid.Free(p);   return true; }
            if (big.SlotInSlab(p))   { big.Free(p);   return true; }
            return false;
        }

        bool OwnsSized(const void* p) const {
            return small.SlotInSlab(p) || mid.SlotInSlab(p) || big.SlotInSlab(p);
        }

        // Per-class diagnostics. Kept separate rather than summed: the tests that assert exact slab
        // accounting care which class a thing came out of, and a combined number would hide exactly
        // the regression they exist to catch (frames quietly going back to 256-byte slots).
        long long   SmallLiveCount() const { return small.LiveCount(); }
        std::size_t SmallCapacity()  const { return small.Capacity(); }
        long long   MidLiveCount()   const { return mid.LiveCount(); }

        // Prints per-class shared-tier contention when built with -DJLIBSCHED_ALLOC_STATS=ON.
        // The number that matters is refill-blocked as a fraction of refills -- see SlabPool.
        static void ReportStats() {
            SlabPool<SLOT>::ReportStats("256B (tasks)");
            SlabPool<MID_SLOT>::ReportStats("128B");
            SlabPool<SMALL_SLOT>::ReportStats("64B");
        }
        std::size_t MidCapacity()    const { return mid.Capacity(); }
    };
}
