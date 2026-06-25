#pragma once
#include <mutex>
#include <vector>
#include <unordered_set>
#include "TaskScheduler.h"
namespace T_Threads {
    class Event {
    private:
        std::mutex mtx;
        std::unordered_set<Task*> waiters;

    public:
        // Register a waiter. Does NOT touch fiber status -- WaitOnEvent already put the
        // fiber in WANTS_SUSPEND before calling this. The mutex serializes registration
        // against Signal/SignalAll, so a signal landing after we register will find us.
        void AddWaiter(Task* t) {
            std::lock_guard<std::mutex> lock(mtx);
            waiters.insert(t);
        }
		// Remove a waiter. Does NOT touch fiber status -- the fiber is already resumed or
		// canceled. The mutex serializes removal against Signal/SignalAll, so a signal landing
		// after we remove will not find us.
        void RemoveWaiter(Task* t) {
            std::lock_guard<std::mutex> lock(mtx);
            waiters.erase(t);
        }
        // Wake up one specific task via the fiber suspend/resume primitive.
        void Signal(Task* t) {
            {
                std::lock_guard<std::mutex> lock(mtx);
                if (!waiters.erase(t)) return; // wasn't waiting
            }
            // Resume() handles the SUSPENDED/WANTS_SUSPEND race and re-queues via Requeue
            // (no pendingTasks double-count). Done outside the lock.
            t->assignedFiber->Resume();
        }

        // Wake up everyone waiting on this event.
        void SignalAll() {
            std::vector<Task*> to_wake;
            {
                std::lock_guard<std::mutex> lock(mtx);
                to_wake.assign(waiters.begin(), waiters.end());
                waiters.clear();
            }
            for (auto* t : to_wake) {
                t->assignedFiber->Resume();
            }
        }
    };
};