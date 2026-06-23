# T_Threads

A high-performance, lightweight C++17 fibers-based task scheduler / job system with thread affinity, and work-stealing queues

---

## Features

- **Thread Pool** — Configurable worker threads (defaults to one per core)
- **Local Queues + Work-Stealing** — Excellent cache locality with automatic stealing when idle
- **Fibers** — Efficient user-mode context switching
- **Task DAG** — Full dependency graph support
- **ParallelFor** — Blocking and non-blocking variants
- **Forked Tasks** — Run long-running tasks outside the pool
- **Main Thread Queue** — Safe tasks for the main thread
- **Automated Memory Management** — Arena allocation + epoch-based garbage collection (no manual `CollectGarbage()` needed)
- **Lambda + Function Pointer** support

---

## Memory Management (Updated)

Memory is **fully automated** using arena allocators combined with an **epoch-based garbage collection** system.

You only need to call `Tick()` periodically from the **main thread**:

```cpp
#include "Epochs.h"

// At the end of your main loop / frame (main thread only)
T_Threads::EpochManager::Instance().Tick();
This safely reclaims retired tasks and nodes when they are no longer referenced.

Motivation
T_Threads was built as a hobby project to explore advanced parallelism in C++. It is currently used in my personal game/simulation engine.

Usage
Starting the Scheduler
C++// Initialize (optional: specify number of workers)
T_Threads::TaskScheduler::Init(/* optional worker count */);

T_Threads::TaskScheduler& scheduler = T_Threads::TaskScheduler::Instance();
The pool cleans up automatically on program exit.
Creating & Submitting Tasks
C++// Lambda style
auto* task = scheduler.CreateTask([]() {
    printf("Hello from task!\n");
});

// Or function pointer style
void MyTask(void* data) { ... }
Task* task = scheduler.CreateTask(MyTask, data);
Scheduling
C++scheduler.Push(task);           // Load-balanced (round-robin)
scheduler.Push(1, task);        // Hint to specific core (work-stealing allowed)

ParallelFor
C++scheduler.ParallelFor(0, 10000, 128, [&](int start, int end) {
    for (int i = start; i < end; ++i) {
        UpdateEntity(i);
    }
});
There is also a non-blocking version: ParallelForNB(...)
Task DAG
C++TaskDAG dag(scheduler);

TaskNode* nodeA = dag.createNode(...);
TaskNode* nodeB = dag.createNode(...);

dag.addDependency(nodeB, nodeA);
dag.submitIfReady(nodeA);
Forked Tasks (Long-running)
C++scheduler.PushFork(coreID, task);
// Later...
scheduler.Stop(task);
Main Thread Tasks
C++scheduler.EnqueueToMain(task);
scheduler.ProcessMainThread();   // Call from main thread

Suspending Tasks
Fibers allow tasks to be suspended using the included Event system. Documentation will be expanded in the future.

Limitations / Known Issues

Windows Only (uses Windows Fibers)
Task limits: ~32M global, ~32k per local queue
Hobby / experimental quality — not production hardened
Singleton design (global scheduler state)
Be mindful of raw pointers and task lifetimes


Notes

Local queues execute pinned tasks before stealing work.
Feel free to experiment and submit pull requests!
