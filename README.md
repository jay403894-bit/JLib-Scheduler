# T_Threads

A high-performance, lightweight C++17 fibers-based task scheduler / job system with thread affinity, and work-stealing queues

---

## Features

- **Thread Pool** — Configurable worker threads (defaults to one per core)
- **Local Queues + Work-Stealing** — Excellent cache locality with automatic stealing when idle
- **Fibers** — Efficient user-mode context switching with suspend/resume/yield
- **Task DAG** — Full dependency graph support
- **ParallelFor** — Blocking and non-blocking variants
- **Forked Tasks** — Run long-running tasks outside the pool
- **Main Thread Queue** — Safe tasks for the main thread
- **Automated Memory Management** — Arena allocation + epoch-based garbage collection (no manual `CollectGarbage()` needed)
- **Lambda + Function Pointer** support
- **roughly 8 microseconds of latency**
---

## Memory Management (Updated)

Memory is **fully automated** using slab allocator combined with an **epoch-based garbage collection** system.


----------
Motivation
----------

T_Threads was built as a hobby project to explore advanced parallelism in C++. It is currently used in my personal game/simulation engine.

-----
Usage
-----

Starting the Scheduler
C++// Initialize (optional: specify number of workers)
```cpp

T_Threads::TaskScheduler::Init(/* optional worker count */);

T_Threads::TaskScheduler& scheduler = T_Threads::TaskScheduler::Instance();
The pool cleans up automatically on program exit.
Creating & Submitting Tasks
C++// Lambda style
auto* task = scheduler.CreateTask([]() {
    printf("Hello from task!\n");
});

-- if you want a task to be hi priority set HiPri to true when creating the task!
its one of the CreateTask Parameters, autoset to false
-- if you have a deep callstack set FiberSize to FiberSize::Heavy -- there are less
heavy fibers than light fibers but heavy fibers get 512k stack vs 64

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
```
There is also a non-blocking version: ParallelForNB(...)
Task DAG
```cpp

C++TaskDAG dag(scheduler);

TaskNode* nodeA = dag.CreateNode(...);
TaskNode* nodeB = dag.CreateNode(...);

dag.AddDependency(nodeB, nodeA);
dag.Submit();
Forked Tasks (Long-running)
C++scheduler.PushFork(coreID, task);
// Later...
scheduler.Stop(task);
Main Thread Tasks
C++scheduler.EnqueueToMain(task);
scheduler.ProcessMainThread();   // Call from main thread
```
Suspending Tasks
Fibers allow tasks to be suspended using the included Event system or through T_Threads which operates through the fiber objects.
```cpp
T_Thread::GetCurrent()->currentFiber->Suspend(); (or CoYield, etc) 

    T_Threads::T_Thread::Resume(suspendedTask->assignedFiber); // Resume always takes an assigned fiber
    T_Threads::T_Thread::Suspend(suspendedTask->assignedFiber); //if suspending another thread
    T_Threads::T_Thread::CoYield(suspendedTask->assignedFiber); // if yielding another thread
    T_Threads::T_Thread::Suspend(); // if suspending THIS thread
    T_Threads::T_Thread::Yield(); // if yielding THIS thread

    //Event system
    #include "TaskScheduler.h"
	#include "Event.h"
	#include <cstdio>
	using namespace T_Threads;

	int main() {
   		 TaskScheduler::Init();           // spin up the worker pool
    		auto& sched = TaskScheduler::Instance();

   		 // ---- Waiter task: parks its fiber until the event fires ----
   		 sched.Push([&] {
      		 std::printf("[waiter] waiting on \"ready\"...\n");
        	sched.WaitOnEvent("ready");  // suspends THIS fiber, frees the worker
        	std::printf("[waiter] resumed!\n");
    	});

    	// ---- Signaler task: wakes everyone parked on "ready" ----
    	sched.Push([&] {
        	std::printf("[signaler] firing \"ready\"\n");
        	sched.GetEvent("ready").SignalAll();   // resume all waiters
    	});

    	sched.WaitAll();                 // let both tasks finish
    	sched.Join();
    return 0;
}

```

Limitations / Known Issues

Windows Only (uses Windows Fibers)
Task limits: ~32M global, ~32k per local queue
Hobby / experimental quality — not production hardened
Singleton design (global scheduler state)
Be mindful of raw pointers and task lifetimes


Notes

Local queues execute pinned tasks before stealing work.
Feel free to experiment and submit pull requests!
