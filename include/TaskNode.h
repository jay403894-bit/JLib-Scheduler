#pragma once
#include "Task.h"
#include "TaskAllocator.h"   // the node only needs the allocator, not the whole scheduler
#include "LockFreeList.h"

namespace T_Threads {
    struct TaskNode {
        TaskAllocator& alloc;                 // injected; the node's list allocates from this
        Task* task;                           // nullptr for a gate (see isGate)

        // How this node decides it's ready, given its direct predecessors:
        //   AND -> fire once ALL predecessors finish (dependencies_left counts down to 0)
        //   OR  -> fire on the FIRST predecessor (the `submitted` exchange dedups the rest)
        enum LogicType { AND, OR };
        LogicType gateType = AND;

        // A gate has no task: when its trigger fires it propagates INSTANTLY (runs its own
        // OnTaskFinished) instead of scheduling work. Compose gates to build arbitrary
        // boolean expressions, e.g. (A && B) || C.
        bool isGate = false;

        LockFreeList<TaskNode*>* dependents;
        std::atomic<int> dependencies_left;
        std::atomic<bool> submitted{ false };
        uint8_t cpuID = 0;
        uint8_t priority = 0;
        bool isLocal = true;
        bool isFork = false;
        // Runs via TaskScheduler::PushMain (drained by ProcessMainThread) instead of the
        // worker pool. Whoever waits on a WaitGroup covering this node's completion MUST use
        // WaitForMain, not WaitFor -- see WaitForMain's declaration comment.
        bool isMain = false;

        TaskNode(Task* t, TaskAllocator& allocator)
            : alloc(allocator), task(t), dependencies_left(0)   // reference bound here
        {
            void* m = alloc.Alloc();
            dependents = new (m) LockFreeList<TaskNode*>(alloc);
        }

        ~TaskNode() {
            if (dependents) {
                dependents->~LockFreeList<TaskNode*>();
                alloc.Free(dependents);
            }
        }
    };
}
