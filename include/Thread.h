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

    // ================================ STEAL INSTRUMENTATION (opt-in) ============================
    // Counts steal PROBES (one call against one victim, hiPri then loPri) and HITS (probes that
    // claimed a task). Built to answer one question: past the point where the pool can drain faster
    // than a single producer can submit, do the surplus workers actually burn themselves on empty
    // deques? A probe/hit ratio that explodes across that crossover says yes.
    //
    // OFF unless JLIBSCHED_STEAL_STATS is defined, because this sits in the steal loop and the whole
    // point is measuring contention there. Instrumentation that perturbs the thing it measures is
    // worse than none: a shared counter here would manufacture exactly the cache-line traffic the
    // experiment is trying to detect. Hence one counter per worker, each on its own line, summed
    // only when someone asks.
    //
    // Enable with -DJLIBSCHED_STEAL_STATS=ON at configure time.
#ifdef JLIBSCHED_STEAL_STATS
    struct alignas(platform::kCacheLine) StealCounters { std::atomic<long long> probes{ 0 }; std::atomic<long long> hits{ 0 }; };
    inline constexpr size_t kStealStatSlots = 256;
    inline StealCounters g_stealStats[kStealStatSlots];
    #define JLIBSCHED_STEAL_STAT(q, field)                                                   \
        do { const size_t _qi = (size_t)(q);                                                 \
             if (_qi < ::JLib::kStealStatSlots)                                              \
                 ::JLib::g_stealStats[_qi].field.fetch_add(1, std::memory_order_relaxed);    \
        } while (0)

    inline void StealStatsReset() {
        for (size_t i = 0; i < kStealStatSlots; ++i) {
            g_stealStats[i].probes.store(0, std::memory_order_relaxed);
            g_stealStats[i].hits.store(0, std::memory_order_relaxed);
        }
    }
    inline void StealStatsRead(long long& probes, long long& hits) {
        probes = 0; hits = 0;
        for (size_t i = 0; i < kStealStatSlots; ++i) {
            probes += g_stealStats[i].probes.load(std::memory_order_relaxed);
            hits   += g_stealStats[i].hits.load(std::memory_order_relaxed);
        }
    }
    inline constexpr bool kStealStatsEnabled = true;
#else
    #define JLIBSCHED_STEAL_STAT(q, field) ((void)0)
    inline void StealStatsReset() {}
    inline void StealStatsRead(long long& probes, long long& hits) { probes = 0; hits = 0; }
    inline constexpr bool kStealStatsEnabled = false;
#endif

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
        // fiberCacheCapacity: how many fibers this worker may hold in its local cache. Passed in
        // rather than derived here, because the only place that knows both the global fiber budget
        // and the worker count is StartPool. The old inline formula tried to compute it from
        // workers.size() alone and could not, which is how it ended up as a constant by accident.
        void StartWorker(size_t cpu_affinity, size_t fiberCacheCapacity);
        std::thread::id GetID();
        bool SetImmediateTask(Task* task_);

        int GetQueueLoad();
        void SetQueueIndex(size_t index);

        // Move everything in THIS worker's inboxes onto its own deques. Returns true if anything
        // moved. Only ever called on the calling thread's own Thread object -- both queues are
        // owner-only on the drain side, and this is that owner.
        //
        // Exists for one situation: a worker BLOCKED INSIDE A TASK (a noFiber task spinning in
        // WaitFor/SchedulerMutex/SchedulerConditionVariable). It will not return to Worker()'s loop
        // while it spins, and nothing else may drain its inboxes, so anything sitting there is
        // invisible to the ENTIRE pool -- including, potentially, the very task it is waiting for.
        // Deques are stealable, so moving the work across makes it reachable again.
        bool DrainOwnInboxesToDeques();
        void Join();
        static Thread* GetCurrent();
        static void CoYield(Fiber* targetFiber);
        static void Suspend(Fiber* targetFiber);
        static void Resume(Fiber* targetFiber);
        static void CoYield();
        static void Suspend();
        static void Resume();
        // force=true skips the awake-check below and always signals. Used by Join(): shutdown flips
        // `running` rather than pushing work, and the awake-check is only PROVEN correct for the
        // hasQueuedWork handshake. Extending it to a second variable would be the same Dekker race
        // on a pair the model never covered, so shutdown does not get the optimisation.
        void NotifyWorker(bool force = false);

        // Called by TaskScheduler (PushLocal/PushBatch/ParallelFor/Requeue) whenever a task is
        // pushed specifically to THIS worker's inbox, right alongside the matching
        // NotifyWorker() call -- lets Worker()'s sleep predicate depend only on "did MY OWN
        // queue change," not a pool-wide counter (see hasQueuedWork's comment).
        //
        // seq_cst, NOT release, and it is load bearing. This store and NotifyWorker's load of
        // workerState are a StoreLoad pair on DIFFERENT objects, which is exactly the ordering
        // release/acquire does not provide. With release here the pusher can observe a stale AWAKE
        // while the worker observes a stale empty queue, and the worker parks on a task only it can
        // drain. tests/verify/sleepwake_model.c reports that as a safety violation; it passes with
        // seq_cst. Note it would still run correctly on x86 either way, because `lock cmpxchg`
        // incidentally drains the store buffer -- AArch64 is where the weaker version breaks.
        void MarkQueuedWork() { hasQueuedWork.store(true, std::memory_order_seq_cst); }
        bool Ready();

        // DIAGNOSTIC ONLY, for TaskScheduler::DumpPoolState(). Reads are relaxed and unsynchronised
        // because this runs from a watchdog on a pool that is already presumed wedged -- a torn or
        // slightly stale view is fine, and taking any lock here could hang the diagnostic itself.
        // The combination worth looking for is state=SLEEPING with queued=1: a worker parked while
        // holding work only it can drain, which is the lost-wakeup signature.
        struct DebugState {
            int  qIndex;
            int  workerState;      // 0 AWAKE, 1 GOING_TO_SLEEP, 2 SLEEPING
            bool hasQueuedWork;
            bool busy;
            bool immediate;
            bool running;
        };
        DebugState GetDebugState() const {
            return DebugState{
                qIndex,
                workerState.load(std::memory_order_relaxed),
                hasQueuedWork.load(std::memory_order_relaxed),
                busy.load(std::memory_order_relaxed),
                immediate.load(std::memory_order_relaxed),
                running.load(std::memory_order_relaxed)
            };
        }
    private:
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

        // Worker sleep state, so a push can skip the wake entirely when this worker is already
        // running. Three states rather than a bool because the interesting window is between
        // "decided to park" and "actually inside cv.wait": GOING_TO_SLEEP publishes the INTENT, so
        // a pusher arriving mid-decision still signals instead of assuming it will be seen.
        //
        // Protocol and its proof: tests/verify/sleepwake_model.c. Every transition here and the
        // load in NotifyWorker are seq_cst, and that is not defensive -- the model's -DACQ_REL_ONLY
        // negative control fails with a lost wakeup.
        enum WorkerState : int { WS_AWAKE = 0, WS_GOING_TO_SLEEP = 1, WS_SLEEPING = 2 };
        std::atomic<int> workerState{ WS_AWAKE };

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