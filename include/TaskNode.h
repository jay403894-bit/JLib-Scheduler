#pragma once
#include "Task.h"
#include "TaskAllocator.h"   // <- instead of TaskScheduler.h; the node only needs the allocator
#include "LockFreeList.h"
namespace T_Threads {
    struct TaskNode {
        TaskAllocator& alloc;                // injected; node's list allocates from this
        Task* task;
        LockFreeList<TaskNode*>* dependents;
        std::atomic<int> dependencies_left;
        std::atomic<bool> submitted{ false };
        uint8_t cpuID = 0;
        uint8_t priority = 0;
        bool isLocal = true;
        bool isFork = false;

        TaskNode(Task* t, TaskAllocator& allocator)
            : alloc(allocator), task(t), dependencies_left(0)   // <- reference bound here
        {
            void* m = alloc.Alloc();
            dependents = new (m) LockFreeList<TaskNode*>(alloc);  // pass the ref straight through
        }

        ~TaskNode() {
            if (dependents) {
                dependents->~LockFreeList<TaskNode*>();
                alloc.Free(dependents);
            }
        }
    };
}