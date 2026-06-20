# T_Threads



A high-performance, lightweight lockfree C++ task scheduler with thread affinity and work-stealing queues.



## Contribution



Feel free to explore and experiment with the scheduler. Pull requests or ideas welcome for improvements.



## Features



- **Thread Pool:** Efficient management of worker threads.

- **Local Queues:** Tasks can be scheduled to specific threads for cache locality.

- **Heap/periodic and delayed tasks:** Tasks can be scheduled to run periodically or delayed.

- **Manual task Garbage collection:** deferred deletion, scheduler.CollectGarbage() at the end of a frame or whenever tasks are completed and no longer being used/saved.

- **Work-Stealing:** Threads can steal tasks from other queues when idle.

- **Priority Queues:** Supports multiple levels of priority.

- **Lambda Support:** Tasks can be submitted as function pointers or lambda expressions.

- **Lightweight:** lockfree, atomic operations for performance.

- **DAG:** Supports task dependency graphing with optional TaskDAG



## Motivation



T_Threads was built as a hobby project to explore advanced parallelism in C++. It provides a flexible task scheduler with:



- Local queues for thread affinity.

- Work-stealing across threads.

- Priority-based execution.

- Minimal overhead for fast task dispatch.



It’s designed for hobby projects, experiments, or as a foundation for building custom concurrent systems. I am currently using it in my game/simulation engine project 

-----
Usage
-----

----------------------
Starting the Scheduler
----------------------
	TaskScheduler& scheduler = TaskScheduler::Instance();

	scheduler.StartPool(uint8_t clock_worker);  // Launches worker threads

you must choose a core to run the clock and heap to notify workers and run periodic and delayed tasks

The pool runs automatically and can optionally be joined:

	scheduler.Join();  // Stops all workers and joins threads

the pool will automatically rejoin on program exit -- HOWEVER any forked workers must be manually stopped by holding a reference to the task they run with the stop() function in the task->stop() otherwise it will hang on exit, once stopped they will rejoin the pool

----------------
Submitting Tasks
----------------

-----------
Local Tasks
-----------
Assign tasks to a thread-local queue:

	scheduler.SubmitLocal(task);         // Round-robin load balanced
	scheduler.SubmitLocal(1, task);      // Assign to CPU core 1 specifically

you can also directly type in a lambda instead of a task, but it will not be tracked unless you specifically

save it as a task or lambdatask object 

-------------
Priority Tasks
-------------

Submit tasks to a global priority queue (5 levels):

	scheduler.SubmitPQ(task);             // Default priority (3)
	scheduler.SubmitPQ(0, task);          // Specific priority (0–4)

------------
Forked Tasks
------------

Fork a task outside the pool:

	scheduler.SubmitFork(coreID, task);

Forked tasks temporarily remove the thread from the pool. Any local work is redistributed before forking. Stop the worker to rejoin the pool.

Lambda Tasks

All submit methods accept lambda expressions, they do not track tasks and will simply be added to the garbage graveyard upon completion. you can store a new LambdaTask and track it that way.

	scheduler.SubmitLocal([](){ std::cout << "Lambda task running!" << std::endl; });

	scheduler.SubmitLocal(1, [](){ std::cout << "Pinned lambda!" << std::endl; });

to stop a forked task 

	scheduler.Stop(Task*); 
	
granted you have to setup stop conditions in the task this sets the flag though so main or another thread can request stop


-----------------
Main Thread Tasks
-----------------
You can enqueue tasks from any thread to run on the main thread:

	scheduler.EnqueueToMain(task);
	scheduler.ProcessMainThread();  // Run all main-thread tasks

Tasks are usually heap-allocated.

To track completion safely, keep tasks in a vector and delete them after all tasks finish using waitall:

    std::vector<Task*> tasks;
    for (int it = 0; it < iterations; ++it) {
        for (int t = 0; t < tasksPerIteration; ++t) {
            // allocate id on heap to keep it alive for the function
            int* id = new int(t);

            Task* task = new Task(simpleTaskFn, id);

            scheduler.SubmitLocal(task);
            tasks.push_back(task);
            std::this_thread::yield();
        }
		scheduler.WaitAll(tasks);

-----------------
Memory Management
-----------------
delete old tasks/garbage collection
do this at the end of a frame or whenever a large amount of tasks are presumably 
as deferred cleanup
		
		scheduler.CollectGarbage();

____________________
:::TO USE THE DAG:::
--------------------

The TaskDAG module provides a high-performance, dependency-aware task scheduling system. It allows you to build complex task graphs where tasks execute only after their dependencies have completed, ensuring optimal utilization of the TaskScheduler.



Getting Started

1. Creating the Graph

Use the TaskDAG as a factory for your nodes. The graph owns the nodes, simplifying memory management.

	TaskScheduler& sched = TaskScheduler::instance();
	TaskDAG dag(sched);



// Create nodes with routing options

	TaskNode* nodeA = dag.createNode(new LambdaTask([](){ /* Work */ }), 		Queue::Default, ANY_CORE);

	TaskNode* nodeB = dag.createNode(new LambdaTask([](){ /* Work */ }), Queue::Priority, 0);



// Define dependencies (B runs after A)

	dag.addDependency(nodeB, nodeA);

2. Executing the DAG

Submit your entry-point node(s). The scheduler handles the propagation through the graph automatically.

	dag.submitIfReady(nodeA);

3. Cleanup

To maintain performance, the system utilizes a Main-Thread-Only garbage collection strategy.

Call this periodically (usually once per frame on the main thread)
		
		sched.CollectGarbage(); //normal garbage collection call
		dag.Clear(); //clear the dag nodes 

---------------------------
Limitations / Known Issues
---------------------------

Lambdas must be be saved for tracking if you need to and are one shot unless declared as new LambdaTask. if you just submit([](){}) without saving the task for tracking

Windows Only: No pthreads/Linux support yet.

Task Limits: ~32 million tasks in global queues, ~32k per work-stealing queue. overflow not really handled

if you need bigger sizes you have to modify the source or if you had to save memory and needed less and knew in advance

Memory Safety: Be cautious with raw pointers and task lifetimes.


-----
Notes
-----

The scheduler is a singleton (TaskScheduler::instance()), introducing global state.

Local queues execute pinned tasks before work-stealing tasks.

Priority queues run after local queues finish work.

This is a hobby project, not production-grade, but very flexible for experimentation. 
