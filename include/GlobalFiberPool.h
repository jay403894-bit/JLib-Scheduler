// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <mutex>
#include <vector>
#include "Fiber.h"
#include "FiberStackArena.h"
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
        static constexpr size_t kStandardStackSize = 64 * 1024;

        mutable std::mutex poolMutex;
        moodycamel::ConcurrentQueue<Fiber*> availableFibers;
        unsigned int size = 0;
        // Ownership: arena and fiber storage
        FiberStackArena standardArena;
        std::vector<Fiber> standardFibers;

        // Private constructor - use Create() factory
        explicit GlobalFiberPool(size_t standardCount);

    public:
        ~GlobalFiberPool() = default;

        // Factory method: creates the global pool with the requested fiber count
        static GlobalFiberPool* Create(size_t standardCount);

        // Bulk acquire: Take up to 'count' fibers
        std::vector<Fiber*> StealBatch(size_t count);

        size_t StealInto(Fiber** dest, size_t maxCount);

        // Bulk release: Return a batch of fibers
        void ReturnBatch(Fiber** fibers, size_t count);
        static void FiberEntryWrapper();

        // Query available fiber count (for diagnostics)
        size_t AvailableCount() const;

        // TOTAL fibers. The exact upper bound on tasks that can be parked anywhere at once, since a
        // parked task holds a fiber -- Event sizes its waiter index by it.
        size_t TotalCount() const { return size; }
    };
}
