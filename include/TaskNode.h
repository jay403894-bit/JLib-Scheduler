#pragma once
#include "Task.h"
#include "TaskScheduler.h"
#include "LockFreeList.h"
namespace T_Threads {
    struct TaskNode {
        Task* task;
        LockFreeList<TaskNode*>* dependents;
        std::atomic<int> dependencies_left;
        std::atomic<bool> submitted{ false };
        uint8_t cpuID = 0;
        uint8_t priority = 0;
        bool isLocal = true;
        bool isFork = false;
        void* mem;
        TaskNode(Task* t) : task(t), dependencies_left(0) {
			void* mem =TaskScheduler::Instance().GetAllocator()->Alloc();
            dependents = new (mem)LockFreeList<TaskNode*>();
        }
        ~TaskNode() {
            if (dependents) {
                dependents->~LockFreeList<TaskNode*>();

                TaskScheduler::Instance().GetAllocator()->Free(dependents);
            }
         }
    };
};