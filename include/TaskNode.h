// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Task.h"
#include "TaskAllocator.h"   // the node only needs the allocator, not the whole scheduler
#include "LockFreeList.h"
#include <stdexcept>
#include <string>

namespace JLib {
    class TaskDAG; // owner backpointer only -- no TaskDAG.h include (it includes this header)

    struct TaskNode {
        TaskAllocator& alloc;                 // injected; the node's list allocates from this
        Task* task;                           // nullptr for a gate (see isGate)

        // Embedded completion context -- stamped by TaskDAG::Fire() when it repoints the
        // task's fn/data at OnTaskFinishedWrapper, so the node's REAL work survives the
        // overwrite. This used to be a separate heap-allocated TaskFinishedContext (a new+
        // delete per node fire, the DAG runtime's only heap traffic); the node itself always
        // outlives the trampoline (it's EBR-retired at the END of OnTaskFinished, and the
        // actual free is deferred past any reader), so the fields live here instead and the
        // DAG runtime is genuinely zero-allocation. Unused (defaults) on gates -- they never
        // schedule a task, so Fire() never stamps them.
        TaskDAG* owner = nullptr;
        Task::Func origFn = nullptr;
        void* origData = nullptr;

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
            // CreateNode/CreateGate already checked THEIR OWN slot (the TaskNode itself), but
            // this is a SECOND, separate allocation for the dependents list -- unchecked, this
            // used to placement-new a LockFreeList at address nullptr when the pool was
            // exhausted, writing through a null `this` inside LockFreeList's constructor
            // (manifested as "Access violation writing location 0x0"). Throwing here at least
            // turns silent memory corruption into a diagnosable, catchable failure with the
            // live/capacity counts attached.
            if (!m) {
                throw std::runtime_error(
                    "TaskAllocator exhausted while constructing a TaskNode's dependents list "
                    "(live=" + std::to_string(allocator.LiveCount()) +
                    ", capacity=" + std::to_string(allocator.Capacity()) + ")");
            }
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
