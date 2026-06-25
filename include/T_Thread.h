#pragma once
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <random>
#include <thread>
#include "Task.h"
#include "Fiber.h"
#include "Epochs.h"
#include "LockFreeList.h"
#include "ThreadLocalCache.h"
#include "GlobalFiberPool.h"
namespace T_Threads {
	class TaskScheduler;
    inline thread_local Task* current_task = nullptr;
    struct WaitHandle {
        Fiber* fiber;
        std::atomic<bool> signaled{ false };
    };
    struct AcquireWorkRes {
        Task* task;
        bool isImmediate;
    };
    class T_Thread {
    public:
        static thread_local T_Thread* self;

        Context schedulerCtx; 
        Fiber* currentFiber = nullptr; 
        Task* currentRunningTask = nullptr;
        int qIndex = 0;

        T_Thread(TaskScheduler& scheduler);
        T_Thread(const T_Thread& other) = delete;
        T_Thread& operator=(const T_Thread& other) = delete;
        ~T_Thread();
        void StartWorker(size_t cpu_affinity);
        std::thread::id GetID();
        bool SetImmediateTask(Task* task_);

        int GetQueueLoad();
        void SetQueueIndex(size_t index);
        void Join();
        static T_Thread* GetCurrent();
        static void CoYield(Fiber* targetFiber);
        static void Suspend(Fiber* targetFiber);
        static void Resume(Fiber* targetFiber);
        static void CoYield();
        static void Suspend();
        static void Resume();
        void NotifyWorker();
        bool Ready();
    private:
        uint64_t GenerateID();
        Fiber* AcquireFiber(Task* task);
        void ReleaseFiber(Fiber* f);
        uint32_t FastRand();
        void WaitBackoff(int& spin_count);
        void ExecuteTask(Task* task);
        Task* AcquireWork(bool& isFork);   // inbox drain + immediate + localQ + pop_bottom + steal
        void  RunTask(Task* task, bool isFork);  // acquire/resume fiber, switch, handle DEAD/YIELD/SUSPEND
        void Worker();

        TaskScheduler* scheduler;
        std::vector<Fiber*> localFiberCache;
        static LockFreeList<Fiber*> suspendedFibers;
        ThreadLocalCache<> localCache;
        static thread_local T_Thread* instance;

        std::atomic<bool> immediate{ false };
        std::atomic<bool> running{ false };
        std::atomic<bool> ready{ false };
        std::atomic<bool> joining{ false };
        std::mutex workerMutex;
        std::mutex joinMutex;
        std::condition_variable cvWorkerDone;
        std::condition_variable cv;
        std::condition_variable cvAffinity;
        Task* task = nullptr;
        Task* immediateTask = nullptr;
        std::thread thread;
        std::thread::native_handle_type nativeHandle;
        std::vector<Task*> localQ;

    };
};