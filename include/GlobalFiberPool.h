// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <mutex>
#include <vector>
#include "Fiber.h"
#include "FiberStackArena.h"
#include "platform.h"   // PageSize -- the guard page comes out of every region
#include "Context.h"
#include "concurrentqueue.h"

namespace JLib {
    class Thread;
    class GlobalFiberPool {
        // Per-fiber stack sizes. Each is used three times below (arena capacity, the
        // AllocateStack request, and Fiber::stackSize) -- keep them here so tuning one
        // cannot leave the arena sized for a stack the fibers no longer use. The lowest
        // 4KB page of each stack is the guard page, so usable depth is 4KB less.
        // ---- ONE STACK CLASS, AND THE SECOND ONE WAS DELETED --------------------------------
        //
        // A 512 KB "heavy" class sat beside this one, sized for deep recursive call stacks.
        // NOTHING EVER ASKED FOR IT: Task::requiredSize was written by CreateTask and read by
        // nobody, and no call site in the library, the tests or the benches ever passed Heavy. It
        // came from another scheduler's design, against a workload that never turned up.
        //
        // AND IT WAS NOT FREE WHILE IT WAITED. FiberStackArena reserves lazily but AllocateStack
        // COMMITS, and the pool builds every fiber up front -- so on a 31-worker pool the heavy
        // class committed 31 x 8 x 512 KB, ~127 MB of real memory and roughly half this library's
        // startup footprint, plus the time to commit it.
        //
        // Deleted rather than wired up: a second class is a short job to add back the day a
        // workload needs one, and until then it is 127 MB of speculation.
        //
        // ---- AND NOW THERE ARE THREE, WITH THE 127 MB LESSON BUILT IN ------------------------
        //
        // Tiny and Deep default to ZERO PER WORKER. The heavy class was not deleted for being a bad
        // idea; it was deleted for costing 31 x 8 x 512 KB of COMMITTED memory while nothing asked
        // for it. AllocateStack commits, and the pool builds every fiber up front, so a nonzero
        // default is a bill paid at startup by every program whether or not it ever binds one.
        //
        // So the classes exist, are addressable, and cost nothing until somebody sets a count. That
        // is the difference between adding a class and adding speculation.
        //
        // ---- THE BUDGET IS **USABLE** DEPTH, AND THE REGION IS DERIVED FROM IT ----------------
        //
        // STATED THIS WAY ON PURPOSE. The arena leaves the lowest page of every region unbacked as a
        // guard, so a region figure and a stack figure differ by a page -- and an "8 KB tiny stack"
        // written as an 8 KB REGION is 4 KiB of actual stack on x64. 4 KiB is one page: the return
        // addresses of ReadFile/WSARecv plus the reactor's own wrappers, and nothing else. That is a
        // claim that the I/O body is a few dozen instructions and never calls out, not a budget.
        //
        // 8 KiB usable is where to start unless the I/O path has been measured to the byte.
        //
        // NOT constexpr, and that is a portability bug avoided rather than a style choice.
        // **APPLE SILICON AND MANY ARM64 CONFIGURATIONS USE 16 KiB PAGES**, where a hardcoded 12 KiB
        // region rounds up to 16 KiB and leaves ZERO usable stack -- a guard page and nothing else.
        // Deriving the region from PageSize() at runtime makes the USABLE figure the guarantee, and
        // rounding can then only ever hand out MORE than promised.
        //
        // AND THE TINY CLASS IS MUCH WEAKER THERE, which is worth knowing before leaning on it:
        //
        //   4 KiB pages   tiny region 12 KiB  ->  8 KiB usable   ~5x cheaper than Standard
        //   16 KiB pages  tiny region 32 KiB  -> 16 KiB usable   ~2x cheaper than Standard
        //
        // The guard page is a fixed tax per fiber, so a platform with big pages taxes small stacks
        // hardest. "A fiber per pending I/O operation" is a very different proposition on Apple
        // Silicon than on x64, and any footprint claim has to name which.
        // ---- TINY IS DENOMINATED IN **PAGES**. THE OTHER TWO ARE BYTE BUDGETS. ---------------
        //
        // Not an inconsistency -- they are answering different questions.
        //
        // TINY asks "what is the smallest stack that is still a valid mapping?", and the answer is
        // a whole number of pages by definition. Writing it as 8 KiB is a cute number that is not a
        // mapping at all on Apple Silicon:
        //
        //   Windows / Linux x64        4 KiB page   -> 2 pages =  8 KiB usable
        //   Linux aarch64              4 or 16 KiB  -> query it; both work
        //   macOS / iOS Apple Silicon  16 KiB page  -> 2 pages = 32 KiB usable
        //
        // AND THAT MAKES TINY MUCH LESS TINY ON BIG-PAGE PLATFORMS, which is a fact about the
        // platform rather than a tuning failure: the guard page is a fixed tax per fiber, so it
        // falls hardest on the smallest class. ~5x cheaper than Standard on x64, ~2x on Apple
        // Silicon. "A fiber per pending I/O operation" is a different proposition on each, and any
        // footprint claim has to name which.
        //
        // STANDARD AND DEEP stay byte budgets because they are budgets -- 60 KiB of stack is a
        // statement about how deep a task may recurse, and it must not quadruple because the
        // platform's pages got bigger. Page-denominating them would take Standard from a 64 KiB
        // region to 256 KiB on Apple Silicon and change the main pool's footprint 4x under a change
        // that was supposed to be about adding classes.
        static constexpr size_t kTinyUsablePages = 2;
        static constexpr size_t kStandardUsable  = 60  * 1024;   // 64 KB region, as it always was
        static constexpr size_t kDeepUsable      = 508 * 1024;   // 512 KB region, as heavy was

        static size_t UsableFor(StackClass c) {
            switch (c) {
                case StackClass::Tiny: return kTinyUsablePages * JLib::platform::PageSize();
                case StackClass::Deep: return kDeepUsable;
                default:               return kStandardUsable;
            }
        }
        // Region = usable + one guard page. AllocateStack rounds up to whole pages anyway; adding
        // the page here is what makes the USABLE figure the promise.
        static size_t RegionFor(StackClass c) {
            return UsableFor(c) + JLib::platform::PageSize();
        }

        static constexpr size_t kClassCount = 3;   // indexed by (size_t)StackClass

        mutable std::mutex poolMutex;
        // ONE FREE QUEUE PER CLASS. A single queue cannot serve them: a caller asking for Deep must
        // not be handed a 64 KB stack, and a caller asking for Tiny must not consume a 512 KB one.
        moodycamel::ConcurrentQueue<Fiber*> availableFibers[kClassCount];
        unsigned int size = 0;                     // total across all classes
        unsigned int classCount[kClassCount] = {}; // how many of each
        // Ownership: one arena and one fiber vector per class. Separate arenas rather than one --
        // the arena is a bump allocator with a uniform stride per region, and mixing sizes in it
        // would make the address->index arithmetic non-uniform for no benefit.
        FiberStackArena* arenas[kClassCount] = {};
        std::vector<Fiber> fibers[kClassCount];

        // Private constructor - use Create() factory
        GlobalFiberPool(size_t tinyCount, size_t standardCount, size_t deepCount);

    public:
        ~GlobalFiberPool() = default;

        // Factory. Tiny and Deep default to 0 -- see the note above on why a nonzero default is a
        // bill every program pays at startup.
        static GlobalFiberPool* Create(size_t standardCount, size_t tinyCount = 0,
                                       size_t deepCount = 0);

        size_t CountOf(StackClass c) const { return classCount[(size_t)c]; }

        // Bulk acquire: Take up to 'count' fibers
        std::vector<Fiber*> StealBatch(size_t count, StackClass c = StackClass::Standard);

        // Class-aware. The no-class overload means Standard, so every existing caller is unchanged.
        size_t StealInto(Fiber** dest, size_t maxCount, StackClass c = StackClass::Standard);

        // Bulk release: Return a batch of fibers
        void ReturnBatch(Fiber** fibers, size_t count);
        static void FiberEntryWrapper();

        // Query available fiber count (for diagnostics)
        size_t AvailableCount() const;

        // TOTAL fibers. The exact upper bound on tasks that can be parked anywhere at once, since a
        // parked task holds a fiber -- Event sizes its waiter index by it.
        size_t TotalCount() const { return size; }

        // ---- WHICH FIBER OWNS THIS STACK ADDRESS? Null if the address is not on a fiber stack.
        //
        // THE READ PATH FOR FIBER-LOCAL STORAGE, and it deliberately does not consult the thread.
        // A fiber is what survives a suspend; the worker running it is not, so any identity taken
        // from `thread_local Thread* instance` before a wait names the worker the fiber LEFT --
        // Thread.cpp says exactly that where it writes homeWorker, and FiberRegistry::FlsGet used
        // to do exactly that.
        //
        // WHY IT IS ANSWERABLE: each class's stacks come out of ONE contiguous reservation at a
        // CONSTANT stride, so an address on a stack determines its index by arithmetic. Callers
        // pass the address of one of their own locals, which lives on the stack of whichever fiber
        // is running -- nothing to thread through, and nothing to invalidate on migration, because
        // the answer is a property of the memory the caller stands on rather than of the thread
        // standing there.
        const Fiber* FiberForStack(const void* addr) const noexcept;
        Fiber*       FiberForStack(const void* addr) noexcept;

        // THE FIBER AT A GIVEN poolIndex, or null if out of range.
        //
        // SUPPLY, NOT ACCOUNTING. This exists so FiberRegistry can build its address table; the
        // pool deliberately does NOT grow an ownership or cleanup role to go with it. If the pool
        // drove cleanup it would have to push tasks, which means this header would need
        // TaskScheduler -- and TaskScheduler.h already includes THIS header. That is a cycle, and
        // it is why the registry is a third party rather than a method here.
        //
        // AND IT IS THE ONE PLACE THE STACK CLASSES TOUCH -- exactly as predicted. poolIndex was
        // designed for a second class's return ("standard fibers occupy [0, standardCount), heavy
        // fibers follow"), and that is what happened: this function gained the walk and NOT ONE
        // caller indexing by poolIndex changed. FiberRegistry's table, Event's waiter index and
        // HazardDomain's fiber rows all still see one dense range.
        //
        // ORDER IS Standard, Tiny, Deep -- standard first so the overwhelmingly common class keeps
        // the low indices it has always had, which keeps any dump or log comparable across the
        // change.
        Fiber* At(size_t poolIndex) {
            static constexpr StackClass kOrder[kClassCount] =
                { StackClass::Standard, StackClass::Tiny, StackClass::Deep };
            for (size_t k = 0; k < kClassCount; ++k) {
                auto& v = fibers[(size_t)kOrder[k]];
                if (poolIndex < v.size()) return &v[poolIndex];
                poolIndex -= v.size();
            }
            return nullptr;
        }
    };
}
