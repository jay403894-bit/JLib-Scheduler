#pragma once
#include <mutex>
#include <vector>
#include "Fiber.h"
#include "FiberStackArena.h"
#include "Context.h"
#include "concurrentqueue.h"

namespace T_Threads {
    class T_Thread;
    class GlobalFiberPool {
        mutable std::mutex poolMutex;
        moodycamel::ConcurrentQueue<Fiber*> availableFibers;
        unsigned int size = 0;
        // Ownership: arenas and fiber storage
        FiberStackArena standardArena;
        FiberStackArena heavyArena;
        std::vector<Fiber> standardFibers;
        std::vector<Fiber> heavyFibers;

        // Private constructor — use Create() factory
        GlobalFiberPool(size_t standardCount, size_t heavyCount);
            

    public:
        ~GlobalFiberPool() = default;

        // Factory method: creates the global pool with specified fiber counts
        static GlobalFiberPool* Create(size_t standardCount, size_t heavyCount);

        // Bulk acquire: Take up to 'count' fibers
        std::vector<Fiber*> StealBatch(size_t count);

        // Bulk release: Return a batch of fibers
        void ReturnBatch(std::vector<Fiber*>& batch);
        static void FiberEntryWrapper();

        // Query available fiber count (for diagnostics)
        size_t AvailableCount() const;
    };
}
