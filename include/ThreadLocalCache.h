#pragma once
#include <vector>
#include "Fiber.h"
#include "GlobalFiberPool.h"

namespace T_Threads {
    // Per-worker fiber free-list. Single-threaded: only the owning worker touches it.
    // Pure LIFO stack over localFibers[0..count); no wrapping, no separate head/tail.
    // (The old head/tail+count ring mixed LIFO Push/Pop with a head-based overflow drain;
    //  the indices drifted out of sync and it handed the global pool fibers that were
    //  still live in the cache -> the same fiber got acquired by two workers -> two
    //  workers ran one stack -> corruption. It also dropped the fiber being pushed.)
    template<size_t MaxCapacity = 256>
    struct alignas(64) ThreadLocalCache {
        Fiber* localFibers[MaxCapacity];
        size_t activeCapacity = 0;
        size_t count = 0;
        GlobalFiberPool* globalPool = nullptr;

        void Initialize(GlobalFiberPool* pool, size_t runtimeCapacity) {
            activeCapacity = (runtimeCapacity <= MaxCapacity) ? runtimeCapacity : MaxCapacity;
            if (activeCapacity < 2) activeCapacity = 2; // need room to return a half + keep f
            globalPool = pool;
        }

        void Push(Fiber* f) {
            if (count >= activeCapacity) {
                // Full: spill the TOP half back to the global pool. The top half is the
                // contiguous range [count-half, count); returning it needs no shift and
                // leaves the bottom half [0, count-half) untouched. Disjoint regions =>
                // no fiber is ever both spilled and kept.
                size_t half = activeCapacity / 2;
                globalPool->ReturnBatch(&localFibers[count - half], half);
                count -= half;
            }
            localFibers[count++] = f; // always store f (the old code forgot this on overflow)
        }

        Fiber* Pop() {
            if (count == 0 && globalPool) {
                // Refill from the global pool, bounded by our capacity so we never
                // overrun localFibers.
                size_t got = globalPool->StealInto(localFibers, activeCapacity);
                count = got;
            }
            if (count == 0) return nullptr;
            return localFibers[--count];
        }
    };
};
