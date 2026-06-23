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
        void AddWaiter(Task* t) {
            std::lock_guard<std::mutex> lock(mtx);
            t->assignedFiber->status.store(FiberStatus::SUSPENDED, std::memory_order_release);
            waiters.insert(t);
        }

        // Wake up one specific task
        void Signal(Task* t) {
            std::lock_guard<std::mutex> lock(mtx);
            if (waiters.erase(t)) {
                // Leave status as SUSPENDED — Worker checks for SUSPENDED 
                TaskScheduler::Instance().Push(t);
            }
        }

        // Wake up everyone waiting on this specific event
        void SignalAll() {
            std::lock_guard<std::mutex> lock(mtx);
            for (auto* t : waiters) {
                TaskScheduler::Instance().Push(t);
            }
            waiters.clear();
        }
    };
};