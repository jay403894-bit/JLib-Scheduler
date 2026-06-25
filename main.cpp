#include <iostream>        
#include <thread>         
#include <functional>
#include <algorithm> 
#include <utility>
#include "include/T_Thread.h"
#include "include/TaskScheduler.h"  
#include "include/TaskDAG.h"
#include "include/Task.h"
using T_Threads::Task;
using T_Threads::TaskScheduler;

void forkedTask(void* data) {
    int ctr=0;
    while (true)
    {
        if (T_Threads::current_task->stopFlag.load(std::memory_order_acquire))
            break;

        ctr = (ctr + 1) % 5;
        int capturedCtr = ctr; // new variable per iteration
        TaskScheduler::Instance().Push(ctr, [ctr]() {
            std::cout << "ctr: " << ctr << std::endl;
            });
  
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

// Example function for tasks
void simpleTaskFn(void* data) {
    int* id = static_cast<int*>(data);
    std::this_thread::yield();
    std::cout << "Task " << *id
        << " executed on thread " << std::this_thread::get_id()
        << std::endl << std::flush;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
}

void YieldTest(void* data) {
    int ctr = 0;
    while (true) {
        if (ctr % 3 == 0) {                   // fires every 3 iterations, not just once
            std::cout << "YieldTest: yielding at ctr=" << ctr << std::endl;
            T_Threads::T_Thread::CoYield();
        }
        ctr++;
    }
    
}
Task* suspendedTask;
std::atomic<bool> suspendDone{ false };
std::atomic<bool> isReadyToResume{ false };
void SuspendTest(void* data) {
    int ctr = 0;
    while (true) {
        if (ctr == 3) {
            std::cout << "SuspendTest: Suspending control after 3 iterations." << std::endl;
            isReadyToResume.store(true, std::memory_order_release); // Signal main!
            T_Threads::T_Thread::Suspend();

        }
		if (ctr == 6) {
			std::cout << "Task Resumed Exiting." << std::endl;
            suspendDone.store(true, std::memory_order_release);
            break;
		}
        ctr++;
    }
}
void ResumeTest(void* data) {
	std::cout << "ResumeTest: Resuming the suspended task." << std::endl;
	T_Threads::T_Thread::Resume(suspendedTask->assignedFiber);
}

void fork() {
    TaskScheduler& scheduler = TaskScheduler::Instance();

    Task* forkt = new Task(forkedTask, nullptr);


    scheduler.PushFork(3,forkt);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    forkt->Stop();
    while (!forkt->complete.load()) {
        std::this_thread::yield();
    }
}
void RunDAGTest() {
    using namespace T_Threads;
    TaskScheduler& sched = TaskScheduler::Instance();
    TaskDAG dag(sched);
    std::atomic<int> counter{ 0 };

    // 1. Create Nodes using the DAG factory
    TaskNode* nodeA = dag.CreateNode(new LambdaTask([&]() { std::cout << "Task A finished\n"; counter++; }));
    TaskNode* nodeB = dag.CreateNode(new LambdaTask([&]() { std::cout << "Task B finished\n"; counter++; }));
    TaskNode* nodeC = dag.CreateNode(new LambdaTask([&]() { std::cout << "Task C finished\n"; counter++; }));
    TaskNode* nodeD = dag.CreateNode(new LambdaTask([&]() { std::cout << "Task D finished (Target reached!)\n"; counter++; }));
   
    // 2. Setup Dependencies
    dag.AddDependency(nodeB, nodeA);
    dag.AddDependency(nodeC, nodeA);
    dag.AddDependency(nodeD, nodeB);
    dag.AddDependency(nodeD, nodeC);

    // 3. Start DAG
    std::cout << "Starting DAG...\n";
    dag.SubmitIfReady(nodeA);

    // 4. Wait
    while (counter < 4) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // IMPORTANT: Collect garbage so the LambdaTasks created by the nodes are deleted!
    //dag.Clear();
    std::cout << "All tasks complete.\n";
}

void MyWorkFunction(int start, int end) {
    // This runs on your worker threads. 
    // Set a breakpoint here to verify execution.
    int sum = 0;
    for (int i = start; i < end; ++i) {
        sum += i;
    }
}
int main() {
    T_Threads::TaskScheduler::Init();
    auto& scheduler = T_Threads::TaskScheduler::Instance();

    int start = 0;
    int end = 100000; 
    int chunkSize = 1000;


    const int iterations = 10;
    const int tasksPerIteration = 100;
    int ctr = 0;

    std::vector<Task*> tasks;
    T_Threads::WaitGroup wg;
    for (int it = 0; it < iterations; ++it) {
        for (int t = 0; t < tasksPerIteration; ++t) {

            int* id = new int(t);

            Task* task = scheduler.CreateTask(simpleTaskFn, id);
            task->waitGroup = &wg;
            scheduler.Push(task);
			wg.n.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::yield();
        }
        std::this_thread::yield();
    }    
    std::cout << "Stress test completed." << std::endl;
    fork();


    scheduler.WaitFor(wg);
    RunDAGTest(); 
    scheduler.ParallelFor(start, end, chunkSize, MyWorkFunction);

    Task* yieldTask = scheduler.CreateTask(YieldTest, nullptr);
    scheduler.Push(yieldTask);

    suspendedTask = scheduler.CreateTask(SuspendTest, nullptr);
    scheduler.Push(suspendedTask);

    T_Threads::Fiber* sf = nullptr;
    while (!suspendDone.load(std::memory_order_acquire)) {
        if (!sf) sf = suspendedTask->assignedFiber;
        if (sf) T_Threads::T_Thread::Resume(sf);
        std::this_thread::yield();
    }

    
   return 0;
}
