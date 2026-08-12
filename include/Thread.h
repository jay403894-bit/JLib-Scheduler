// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <random>
#include <thread>
#include "Task.h"
#include "Fiber.h"
#include "Epochs.h"
#include "ThreadLocalCache.h"
#include "GlobalFiberPool.h"
namespace JLib {
	class TaskScheduler;
    struct WaitHandle {
        Fiber* fiber;
        std::atomic<bool> signaled{ false };
    };
    struct AcquireWorkRes {
        Task* task;
        bool isImmediate;
    };
    class Thread {
    public:
        static thread_local Thread* self;

        Context schedulerCtx;
        Fiber* currentFiber = nullptr;
        Task* currentRunningTask = nullptr;
        int qIndex = 0;
        // True while this worker is actively executing a task (fast path OR on a fiber) --
        // a cheap heuristic hint for OTHER workers deciding whether to steal from an SMT
        // sibling (see TaskScheduler::siblingQIndex). Relaxed: a stale read just makes the
        // heuristic slightly worse for one steal attempt, never incorrect/unsafe.
        std::atomic<bool> busy{ false };

        Thread(TaskScheduler& scheduler);
        Thread(const Thread& other) = delete;
        Thread& operator=(const Thread& other) = delete;
        ~Thread();
        void StartWorker(size_t cpu_affinity);
        std::thread::id GetID();
        bool SetImmediateTask(Task* task_);

        int GetQueueLoad();
        void SetQueueIndex(size_t index);
        void Join();
        static Thread* GetCurrent();
        static void CoYield(Fiber* targetFiber);
        static void Suspend(Fiber* targetFiber);
        static void Resume(Fiber* targetFiber);
        static void CoYield();
        static void Suspend();
        static void Resume();
        void NotifyWorker();
        // Called by TaskScheduler (PushLocal/PushBatch/ParallelFor/Requeue) whenever a task is
        // pushed specifically to THIS worker's inbox, right alongside the matching
        // NotifyWorker() call -- lets Worker()'s sleep predicate depend only on "did MY OWN
        // queue change," not a pool-wide counter (see hasQueuedWork's comment).
        void MarkQueuedWork() { hasQueuedWork.store(true, std::memory_order_release); }
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
        ThreadLocalCache<> localCache;
        static thread_local Thread* instance;

        std::atomic<bool> immediate{ false };
        // Set by MarkQueuedWork() (see its comment) whenever THIS worker's own inbox/deque
        // receives a task; cleared once per Worker() loop iteration right before that
        // iteration's local-queue/steal/inbox-drain search, so a push that lands between one
        // clear and the next always either (a) gets found directly by that same iteration's
        // search, or (b) re-arms this flag for the next predicate check. Deliberately NOT a
        // pool-wide counter (that was the old `queuedTasks` design) -- a worker that's
        // genuinely asleep only needs to know about work landing on ITSELF; stealable work on
        // OTHER workers' deques is already found for free by the unconditional steal-attempt
        // phase every awake worker runs each loop pass, with no predicate involved at all.
        std::atomic<bool> hasQueuedWork{ false };
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

    };
};