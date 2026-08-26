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
        // 80 BYTES, and this one was chosen from a MEASURED application rather than reasoned
        // about. Game01, instrumented with JLIBSCHED_TASK_STATS over 28,619 real tasks:
        //
        //     largest 80 bytes, mean 77.2 -- 17.7% at 64, 82.3% at 80, NOTHING above 80.
        //
        // The distribution is bimodal at exactly 64 and 80 because a capture-free or
        // single-8-byte-capture task is 64 (it fits Task's tail padding) and a two-capture
        // lambda is 80. Real frame-loop code is overwhelmingly the latter -- [&flag, dt],
        // [fn, lo, hi], and so on -- so 82% of tasks were taking a 128-byte slot and using
        // 62% of it.
        //
        // WHAT THIS CORRECTS. The 64-byte class was added first on the strength of the
        // COROUTINE FRAME distribution (75% <=64), and that turned out not to generalise: on
        // tasks, 64+256 alone is worth only 0.87x because barely a sixth of tasks fit it.
        // Measured bytes per task for Game01: 256-only 256.0, 64+128+256 116.7 (what 3.0
        // shipped), and 64+80+128+256 77.2 -- which is the MEAN exactly, because with only
        // two sizes present there is no spread left to average over.
        static constexpr std::size_t SLOT80     = 80;
        static constexpr std::size_t SMALL_SLOT = 64;
        static constexpr std::size_t BATCH      = SlabPool<SLOT>::BATCH;

    private:
        // NAMED pool256/pool128/pool64, NOT big/mid/small, and the reason is not style.
        //
        // `small` IS A WINDOWS MACRO. rpcndr.h does `#define small char`, and rpcndr.h arrives
        // through windows.h in essentially every real Windows application. `SlabPool<64> small;`
        // expands to `SlabPool<64> char;` and the error is C2628 "followed by char is illegal",
        // followed by a hundred cascade errors in TaskScheduler.h that point nowhere near here.
        //
        // This shipped in 3.0.0 and 3.0.1 and broke every Windows consumer, while this repo's
        // own build stayed green -- nothing here includes windows.h in a translation unit that
        // also includes TaskAllocator.h. Found by building Game01, which is the only thing that
        // could find it. Naming a member after a common Windows macro is the trap; the names
        // below cannot collide and say their slot size, which is what the reader wants anyway.
        // Distinct template arguments, so each of these has its own per-thread cache and its own
        // sharded live counters. That is the entire reason SlabPool is a template -- see its header
        // for the two bugs that hand-duplicating a pool produced.
        SlabPool<SLOT>       pool256;
        SlabPool<MID_SLOT>   pool128;
        SlabPool<SLOT80>     pool80;
        SlabPool<SMALL_SLOT> pool64;

    public:
        // EXPLICIT PER-CLASS SIZES rather than divisors off one number. Memory in one pool
        // cannot serve a request from another, so a single figure cannot size three pools
        // without either wasting memory or starving a consumer -- see TaskScheduler::SlabSizes
        // for the case that proved it (tasks routed into the 64-byte class silently evicted
        // coroutine frames from a pool sized `slots / 8` for frames alone).
        explicit TaskAllocator(std::size_t bigSlots, std::size_t midSlots,
                               std::size_t slots80, std::size_t smallSlots, bool lazy = false)
            : pool256(bigSlots, lazy)
            , pool128(midSlots, lazy)
            , pool80(slots80, lazy)
            , pool64(smallSlots, lazy) {
        }

        // Convenience for callers with one number, preserving the historical derivation.
        explicit TaskAllocator(std::size_t slots, bool lazy = false)
            : TaskAllocator(slots, slots / 8, slots / 8, slots / 8, lazy) {
        }
        TaskAllocator& operator=(const TaskAllocator&) = delete;

        // ---- the Task path, unchanged --------------------------------------------------------
        // Always the 256-byte class. Tasks are a fixed size and there is nothing to choose.
        void* Alloc()                     { return pool256.Alloc(); }
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
        // THE ONE DISPOSAL FUNNEL. FreeSized routes by ADDRESS and returns false for a pointer no
        // pool owns -- which since 4.0.1 is a real case, not an impossible one: a task whose body is
        // larger than the biggest slot, or one allocated while the slab was exhausted, comes from
        // the global heap exactly as an oversized coroutine frame does (see detail::FrameAlloc).
        //
        // THE OLD FALLBACK WAS pool256.Free(slot), and it was a corruption waiting for a caller.
        // FreeSized already tests pool256, so reaching the fallback means the pointer belongs to NO
        // pool -- and handing that to pool256's free list would have spliced foreign memory into it.
        // Unreachable while every task fit in a slot; reachable the moment one did not.
        //
        // ::operator delete, NOT `delete task`: Task::operator delete exists only because a virtual
        // destructor requires it and asserts if ever called. The destructor is run by the caller
        // (DestroyTask); this releases the storage.
        void  Free(void* slot)            { if (!FreeSized(slot)) ::operator delete(slot); }
        bool  SlotInSlab(const void* p) const { return pool256.SlotInSlab(p); }
        long long LiveCount() const       { return pool256.LiveCount(); }
        // TOTAL across all three pools, because that is what bounds how many Tasks can exist:
        // AllocSized falls through 64 -> 128 -> 256, so a task can come from any of them.
        // Reporting `big` alone here would be the same trap SetTaskSlabSize was -- a name that
        // says "the allocator's capacity" while answering for a third of it. Per-class figures
        // are available separately for anyone who needs the split.
        std::size_t Capacity() const      { return pool256.Capacity() + pool128.Capacity()
                                                 + pool80.Capacity()  + pool64.Capacity(); }
        std::size_t BigCapacity() const   { return pool256.Capacity(); }
        void Prefault(std::size_t slots)  { pool256.Prefault(slots); }

        // ---- size-classed path, for callers whose objects vary ---------------------------------
        //
        // Smallest class that fits, falling through on exhaustion so a dry small pool degrades to a
        // bigger slot rather than to a failure. Returns nullptr only when nothing fits at all --
        // callers handle that (Coroutine.h falls back to global new).
        void* AllocSized(std::size_t n) {
            if (n <= SMALL_SLOT) if (void* p = pool64.Alloc())  return p;
            if (n <= SLOT80)     if (void* p = pool80.Alloc())  return p;
            if (n <= MID_SLOT)   if (void* p = pool128.Alloc())   return p;
            if (n <= SLOT)       if (void* p = pool256.Alloc())   return p;
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
            if (pool64.SlotInSlab(p)) { pool64.Free(p); return true; }
            if (pool80.SlotInSlab(p)) { pool80.Free(p); return true; }
            if (pool128.SlotInSlab(p))   { pool128.Free(p);   return true; }
            if (pool256.SlotInSlab(p))   { pool256.Free(p);   return true; }
            return false;
        }

        bool OwnsSized(const void* p) const {
            return pool64.SlotInSlab(p) || pool80.SlotInSlab(p)
                || pool128.SlotInSlab(p) || pool256.SlotInSlab(p);
        }

        // Per-class diagnostics. Kept separate rather than summed: the tests that assert exact slab
        // accounting care which class a thing came out of, and a combined number would hide exactly
        // the regression they exist to catch (frames quietly going back to 256-byte slots).
        long long   SmallLiveCount() const { return pool64.LiveCount(); }
        std::size_t SmallCapacity()  const { return pool64.Capacity(); }
        long long   MidLiveCount()   const { return pool128.LiveCount(); }
        long long   Slot80LiveCount() const { return pool80.LiveCount(); }
        std::size_t Slot80Capacity()  const { return pool80.Capacity(); }

        // Prints per-class shared-tier contention when built with -DJLIBSCHED_ALLOC_STATS=ON.
        // The number that matters is refill-blocked as a fraction of refills -- see SlabPool.
        static void ReportStats() {
            SlabPool<SLOT>::ReportStats("256B (tasks)");
            SlabPool<MID_SLOT>::ReportStats("128B");
            SlabPool<SLOT80>::ReportStats("80B");
            SlabPool<SMALL_SLOT>::ReportStats("64B");
        }
        std::size_t MidCapacity()    const { return pool128.Capacity(); }
    };
}
