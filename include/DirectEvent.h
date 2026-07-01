#pragma once
#include <atomic>
#include <vector>
#include "Task.h"
#include "Fiber.h"
#include "concurrentqueue.h"

namespace T_Threads {
    // Single-waiter, lock-free rendezvous. One per OUTSTANDING wait, taken from a pool.
    // The signaler holds the pointer directly (handed to it by the arm callback), so there
    // is no name, no map, and no global lock -- unlike the string-keyed Event registry, which
    // grew unbounded and turned GetEvent into a lock convoy under long-running fence traffic.
    struct DirectEvent {
        std::atomic<Task*> waiter{ nullptr };

        // Called by the external signaler (e.g. a GPU-fence Win32 callback). exchange() makes
        // it one-shot/idempotent: only ONE caller wins the waiter, a second Signal sees null.
        void Signal() {
            if (Task* t = waiter.exchange(nullptr, std::memory_order_acq_rel))
                t->assignedFiber->Resume();   // handles the WANTS_SUSPEND/SUSPENDED race
        }
    };

    // Fixed pool of reused DirectEvents (never destroyed -> no lifetime/UAF hazard). Sized
    // once; addresses are stable. Size it above the max CONCURRENT waits across all callers.
    class EventPool {
        std::vector<DirectEvent> storage;
        moodycamel::ConcurrentQueue<DirectEvent*> freeq;
    public:
        explicit EventPool(size_t n) : storage(n), freeq(n) {
            for (auto& e : storage) freeq.enqueue(&e);
        }
        // Returns nullptr if exhausted -- caller must size the pool so this cannot happen.
        DirectEvent* Acquire() {
            DirectEvent* e = nullptr;
            if (!freeq.try_dequeue(e)) return nullptr;
            e->waiter.store(nullptr, std::memory_order_relaxed);
            return e;
        }
        void Release(DirectEvent* e) { freeq.enqueue(e); }
    };
}
