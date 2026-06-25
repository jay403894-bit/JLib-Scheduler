#pragma once
#include <vector>
#include "Fiber.h"
#include "GlobalFiberPool.h"

namespace T_Threads {
    template<size_t MaxCapacity = 256>
    struct alignas(64) ThreadLocalCache {
        Fiber* localFibers[MaxCapacity]; // Fixed array, stack-allocated or embedded in struct
        size_t activeCapacity;            // Your calculated size
        size_t count = 0;             // Track the number of items
        GlobalFiberPool* globalPool = nullptr;
        // Set the global pool this cache refills from
        void Initialize(GlobalFiberPool* pool,size_t runtimeCapacity) {
            activeCapacity = (runtimeCapacity <= MaxCapacity) ? runtimeCapacity : MaxCapacity;
            globalPool = pool;
        }
        void Push(Fiber* f) {
            if (count < activeCapacity) {
                localFibers[count++] = f;
            }
            else if (globalPool) {
                // 1. Batch return: Move half to a temporary vector
                size_t half = activeCapacity / 2;
                std::vector<Fiber*> batchToReturn(localFibers, localFibers + half);

                // 2. Call your existing pool method
                globalPool->ReturnBatch(batchToReturn);

                // 3. Shift remaining items to front using memmove (fast and safe)
                memmove(&localFibers[0], &localFibers[half], (activeCapacity - half) * sizeof(Fiber*));

                // 4. Reset count and add the new fiber
                count = (activeCapacity - half);
                localFibers[count++] = f;
            }
        }

        Fiber* Pop() {
            if (count == 0 && globalPool) {
                // Option: Keep your existing StealBatch which returns a vector
                std::vector<Fiber*> batch = globalPool->StealBatch(activeCapacity / 2);
                for (Fiber* f : batch) {
                    localFibers[count++] = f;
                }
            }
            return (count > 0) ? localFibers[--count] : nullptr;
        }
    };
};