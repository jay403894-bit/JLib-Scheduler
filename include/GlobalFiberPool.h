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
        static constexpr size_t kStandardStackSize = 64 * 1024;
        static constexpr size_t kHeavyStackSize = 512 * 1024;

        mutable std::mutex poolMutex;
        moodycamel::ConcurrentQueue<Fiber*> availableFibers;
        unsigned int size = 0;
        // Ownership: arenas and fiber storage
        FiberStackArena standardArena;
        FiberStackArena heavyArena;
        std::vector<Fiber> standardFibers;
        std::vector<Fiber> heavyFibers;

        // Private constructor - use Create() factory
        GlobalFiberPool(size_t standardCount, size_t heavyCount);
            

    public:
        ~GlobalFiberPool() = default;

        // Factory method: creates the global pool with specified fiber counts
        static GlobalFiberPool* Create(size_t standardCount, size_t heavyCount);

        // Bulk acquire: Take up to 'count' fibers
        std::vector<Fiber*> StealBatch(size_t count);

        size_t StealInto(Fiber** dest, size_t maxCount);

        // Bulk release: Return a batch of fibers
        void ReturnBatch(Fiber** fibers, size_t count);
        static void FiberEntryWrapper();

        // Query available fiber count (for diagnostics)
        size_t AvailableCount() const;
    };
}
