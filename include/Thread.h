// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <optional>
#include <atomic>
#include <chrono>
#include <cstdint>
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

    // ================================ LANE WAKE RATE (opt-in) ===================================
    // Counts what TaskScheduler::WakeForLane actually does:
    //     edges     0->1 transitions of a lane hint bit -- how often a worker gets buried
    //     notifies  wakes actually SENT, which is what costs ~90us of core time each
    //
    // THIS IS THE COST SIDE, and it is measured by COUNTING rather than by an A/B, deliberately.
    // The benefit was measured as latency and needed a 5-9% noise floor and two replications to
    // read. The cost does not need any of that: a wake's price is already known, so the only open
    // variable is the rate, and a count has no variance. It is also the only form of this question
    // answerable in an application whose frame timing is unstable -- which is the normal case
    // without vsync, and clamped to the refresh interval with it.
    //
    // Two counters and not one, because they answer different questions. `edges` says how often the
    // pool reaches the buried state at all -- a property of the WORKLOAD, true even at wake=0.
    // `notifies` says what this mechanism spent. Their ratio is the wake budget actually being used:
    // far below the configured n means most burials found no parked worker to pull up, which is
    // itself the finding that the mechanism is inert in that configuration.
    //
    // Enable with -DJLIBSCHED_LANE_WAKE_STATS=ON at configure time.
#ifdef JLIBSCHED_LANE_WAKE_STATS
    struct alignas(platform::kCacheLine) LaneWakeCounters {
        std::atomic<long long> edges{ 0 };
        std::atomic<long long> notifies{ 0 };
    };
    inline LaneWakeCounters g_laneWake;
    #define JLIBSCHED_LANE_WAKE_STAT(field) \
        ::JLib::g_laneWake.field.fetch_add(1, std::memory_order_relaxed)
    inline void LaneWakeStatsReset() {
        g_laneWake.edges.store(0, std::memory_order_relaxed);
        g_laneWake.notifies.store(0, std::memory_order_relaxed);
    }
    inline void LaneWakeStatsRead(long long& edges, long long& notifies) {
        edges    = g_laneWake.edges.load(std::memory_order_relaxed);
        notifies = g_laneWake.notifies.load(std::memory_order_relaxed);
    }
    inline constexpr bool kLaneWakeStatsEnabled = true;
#else
    #define JLIBSCHED_LANE_WAKE_STAT(field) ((void)0)
    inline void LaneWakeStatsReset() {}
    inline void LaneWakeStatsRead(long long& e, long long& n) { e = n = 0; }
    inline constexpr bool kLaneWakeStatsEnabled = false;
#endif

    // ============================ HOT-WORKER OCCUPANCY WITNESS (opt-in) =========================
    // Answers exactly one question, and it is the question that decides whether hot->hot stealing
    // has a job at all: WHEN A HOT WORKER IS LATE, IS A SIBLING HOT WORKER IDLE AT THAT MOMENT?
    //
    // Latency numbers alone cannot answer it. A bad p99 on the lane is equally consistent with
    // "every hot worker is saturated" (a capacity problem -- raise K, stealing changes nothing) and
    // with "one hot worker is buried behind a long task while its sibling spins on an empty deque"
    // (a balance problem -- exactly what stealing fixes). Those two demand opposite responses, so
    // measuring the tail and guessing which one produced it is how a mechanism gets built on a
    // coin flip.
    //
    // So each hot worker, on each pass where IT has nothing to run, looks at its siblings:
    //     idlePasses     -- passes where this hot worker had an empty lane and empty inbox
    //     idleWithSib    -- ... of those, passes where SOME sibling hot worker had a backlog
    //     sibDepthSum    -- summed sibling depth over those passes, for a mean queue length
    //
    //     idleWithSib / idlePasses  ~ 0   ->  steering already balances; hot->hot is dead weight
    //     idleWithSib / idlePasses  >> 0  ->  work is sitting still next to an idle core
    //
    // This scans the hot deques from the idle side, which is precisely the traffic the design
    // avoids -- that is why it is opt-in and never compiled into a shipping build. It perturbs what
    // it measures in BOTH directions, so neither result is free: the scan slows the idle worker
    // (making it less likely to look idle) but it also reads endpoints the busy worker is writing,
    // costing that worker exclusive state and making it more likely to look backlogged. Hence the
    // 1-in-64 sampling, and hence the rule that this instrument decides a DIRECTION, not a
    // magnitude -- a ratio near zero and a ratio near one mean different things; 0.04 versus 0.07
    // does not.
    //
    // Enable with -DJLIBSCHED_HOT_OCCUPANCY_STATS=ON at configure time.
#ifdef JLIBSCHED_HOT_OCCUPANCY_STATS
    struct alignas(platform::kCacheLine) HotOccCounters {
        std::atomic<long long> idlePasses{ 0 };
        std::atomic<long long> idleWithSib{ 0 };
        std::atomic<long long> sibDepthSum{ 0 };
    };
    inline constexpr size_t kHotOccSlots = 256;
    inline HotOccCounters g_hotOcc[kHotOccSlots];
    inline void HotOccStatsReset() {
        for (size_t i = 0; i < kHotOccSlots; ++i) {
            g_hotOcc[i].idlePasses.store(0, std::memory_order_relaxed);
            g_hotOcc[i].idleWithSib.store(0, std::memory_order_relaxed);
            g_hotOcc[i].sibDepthSum.store(0, std::memory_order_relaxed);
        }
    }
    inline void HotOccStatsRead(long long& idlePasses, long long& idleWithSib, long long& sibDepthSum) {
        idlePasses = idleWithSib = sibDepthSum = 0;
        for (size_t i = 0; i < kHotOccSlots; ++i) {
            idlePasses  += g_hotOcc[i].idlePasses.load(std::memory_order_relaxed);
            idleWithSib += g_hotOcc[i].idleWithSib.load(std::memory_order_relaxed);
            sibDepthSum += g_hotOcc[i].sibDepthSum.load(std::memory_order_relaxed);
        }
    }
    inline constexpr bool kHotOccStatsEnabled = true;
#else
    inline void HotOccStatsReset() {}
    inline void HotOccStatsRead(long long& a, long long& b, long long& c) { a = b = c = 0; }
    inline constexpr bool kHotOccStatsEnabled = false;
#endif

    // ============================== LATENCY BREAKDOWN INSTRUMENTATION (opt-in) ===================
    // Built to answer one question: where does the 4.3 us push->run->wait round-trip (README's
    // Sleep-mode figure) actually go -- the OS kernel wake, or Worker()'s own loop order? A parked
    // worker wakes because a Push() landed in ITS INBOX, but the loop checks its local deque (3)
    // and runs a full steal scan (4) BEFORE it ever looks at that inbox (5) -- so a cold wake pays
    // for a steal sweep across an otherwise-idle pool that is guaranteed to fail, before it finds
    // the very task that woke it. Three global timestamps (not per-worker: this is a SERIAL,
    // one-task-in-flight investigation, same setup as BenchLatency, so there is only ever one
    // worker actually doing anything to record) mark the three transitions that split that gap:
    // Wake (cv.wait returns / the recheck-abort escape, whichever fires), PreSteal (right before
    // section 4 begins), and Found (section 5 actually retrieves the task).
    //
    // OFF unless JLIBSCHED_LATENCY_STATS is defined, for the same reason as JLIBSCHED_STEAL_STATS:
    // a clock read on every wake is not free, and instrumentation that perturbs a ~us-scale
    // measurement is worse than none. Turn on only for this experiment; do not compare its numbers
    // against a normal build's.
    //
    // Enable with -DJLIBSCHED_LATENCY_STATS=ON at configure time.
#ifdef JLIBSCHED_LATENCY_STATS
    inline std::atomic<int64_t> g_lastWakeNs{ 0 };
    inline std::atomic<int64_t> g_lastPreStealNs{ 0 };
    inline std::atomic<int64_t> g_lastFoundNs{ 0 };
    inline int64_t LatencyNowNs() {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    }
    #define JLIBSCHED_LATENCY_MARK(which) \
        (::JLib::g_last##which##Ns.store(::JLib::LatencyNowNs(), std::memory_order_relaxed))
    inline void LatencyStatsRead(int64_t& wakeNs, int64_t& preStealNs, int64_t& foundNs) {
        wakeNs     = g_lastWakeNs.load(std::memory_order_relaxed);
        preStealNs = g_lastPreStealNs.load(std::memory_order_relaxed);
        foundNs    = g_lastFoundNs.load(std::memory_order_relaxed);
    }
    inline constexpr bool kLatencyStatsEnabled = true;
#else
    #define JLIBSCHED_LATENCY_MARK(which) ((void)0)
    inline void LatencyStatsRead(int64_t& wakeNs, int64_t& preStealNs, int64_t& foundNs) {
        wakeNs = preStealNs = foundNs = 0;
    }
    inline constexpr bool kLatencyStatsEnabled = false;
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
        // Exists for one situation: a worker BLOCKED INSIDE A TASK (a Native task spinning in
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

        // THE FOURTH PREDICATE INPUT. Set by TaskScheduler::WakeForLane when a HOT worker publishes
        // a lane backlog, to pull a parked ordinary worker up to come and steal it.
        //
        // seq_cst for exactly the reason MarkQueuedWork is, and the reason is not defensive: this
        // store and NotifyWorker's load of workerState are a StoreLoad pair on DIFFERENT objects,
        // and NotifyWorker's awake-skip is only sound for flags that sit in the single total order.
        // The 1.2.0 hang was a predicate input left at release/acquire while its neighbour was
        // promoted. tests/verify/sleepwake_model.c carries this flag and a -DWEAK_LANEWAKE negative
        // control that MUST fail.
        void MarkLaneWake() { laneWake.store(true, std::memory_order_seq_cst); }

        // Is this worker parked or on its way there? Used to aim a lane wake at a worker that will
        // actually pay for one -- NotifyWorker already skips an awake worker for free, but a wake
        // budget of N should be spent on N SLEEPING workers, not burned on awake ones.
        bool Parked() const { return workerState.load(std::memory_order_seq_cst) != WS_AWAKE; }

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

        // EDGE-TRIGGERED, and that is the whole safety argument for it. Cleared once per Worker()
        // loop iteration in the same place as hasQueuedWork, so ONE wake buys ONE search pass and
        // the worker parks again unless it actually found something.
        //
        // The level-triggered version -- reading TaskScheduler::stealHintLane straight from the
        // sleep predicate -- is the obvious implementation and it is wrong twice over. It would put
        // a line shared with every hot worker inside every parked worker's predicate, and worse, a
        // worker that woke and lost the steal race would find the predicate STILL true and never
        // park: N-K workers spinning for as long as one hot worker stays buried, which is NoSleep
        // arrived at by accident and with none of NoSleep's bounds. A flag the worker consumes
        // cannot do that.
        std::atomic<bool> laneWake{ false };

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

    // Epoch slot for the current execution context: the running fiber's slot if we are on one,
    // else this (bare) thread's fallback slot. The fiber branch is migration-proof -- the slot
    // travels with the fiber across a context switch. Bare threads (e.g. the main thread building
    // a DAG) do not migrate, so their per-thread fallback is correct.
    //
    // Lives HERE rather than in LockFreeList.h, where it was originally written, because it is an
    // epoch concept and not a list one -- and TaskDAG needs it now that its dependent walk guards
    // itself instead of borrowing LockFreeList::for_each's guard. Epochs.h cannot host it: it
    // would have to include Fiber.h, which already includes Epochs.h. Thread.h already sees both.
    inline std::atomic<size_t>* CurrentEpochSlot() {
        if (Thread* w = Thread::GetCurrent())
            if (Fiber* f = w->currentFiber)
                return &f->localEpoch;
        return EpochManager::Instance().ThreadSlot(thread_id);
    }

    // TRUE when the caller is a coroutine task: on a worker, but with no fiber, which is what a
    // Coroutine-type task looks like (they ride the Native path and never take one).
    //
    // A bare thread also has no fiber and deliberately answers FALSE -- its thread slot is genuinely
    // its own and stable for the guard's whole life, so the slot mechanism is correct and cheaper
    // for it. The case this separates is the coroutine's, where the slot would be BORROWED from
    // whichever worker happens to be running it and stops being the right place the instant it
    // suspends.
    inline bool OnCoroutineTask() {
        Thread* w = Thread::GetCurrent();
        if (!w || w->currentFiber) return false;
        Task* t = w->currentRunningTask;
        return t && t->type == TaskType::Coroutine;
    }

    // ================================================================================================
    // THE GUARD EVERY COROUTINE-REACHABLE CALL SITE SHOULD TAKE.
    //
    // Picks the mechanism by who is asking. A fiber or bare thread gets the slot guard, unchanged
    // and uncontended. A COROUTINE gets the counted guard, because a slot would be borrowed from a
    // worker it is not bound to -- and a guard held across a co_await would then be un-announced by
    // that worker's next guard while the traversal is still live. For a fiber that is a leak; for a
    // coroutine it is a use-after-free.
    //
    // BOTH MECHANISMS ARE LIVE AT ONCE and MinActiveEpoch takes the minimum over their union, so
    // reclamation respects whichever readers exist. See EpochManager's counted-epoch block and
    // tests/verify/counted_epoch_model.c.
    //
    // ------------------------------------------------------------------------------------------
    // WHY THIS LIVES IN Thread.h AND NOT NEXT TO THE OTHER TWO GUARDS IN Epochs.h
    //
    // It cannot live there. The picking decision needs OnCoroutineTask(), which needs
    // Thread::GetCurrent() and Task::type -- and Thread.h ALREADY INCLUDES Epochs.h. Moving this
    // class down would invert that edge: Epochs.h would have to see Thread, which sees Epochs.
    //
    // A forward declaration would compile, and it is worse than the split, because it fails in
    // exactly the wrong direction: the guard would pick correctly only in translation units that
    // also happened to pull in Thread.h. Every other one would fail to link -- or, if someone
    // "fixed" that with a fallback definition, would quietly hand a coroutine a borrowed slot,
    // which is the use-after-free this class exists to prevent. The value of this type is that
    // there is exactly ONE decision site and it can never be the wrong one.
    //
    // SO THE SPLIT IS: Epochs.h owns the two MECHANISMS (SlotEpochGuard, CountedEpochGuard) and
    // knows nothing about who is running. Thread.h owns the CHOICE, because only Thread.h can see
    // the caller. Both mechanism definitions in Epochs.h point back here.
    //
    // And this one has the plain name deliberately. Callers should essentially never name
    // SlotEpochGuard or CountedEpochGuard directly; this spelling is the correct one everywhere,
    // so it gets the obvious name and the two mechanisms get the qualified ones.
    // ------------------------------------------------------------------------------------------
    class EpochGuard {
    public:
        EpochGuard() {
            if (OnCoroutineTask() || EpochManager::ForceCountedEpochs()) counted_.emplace();
            else                                                        slotted_.emplace(CurrentEpochSlot());
        }
        EpochGuard(const EpochGuard&) = delete;
        EpochGuard& operator=(const EpochGuard&) = delete;
    private:
        std::optional<CountedEpochGuard> counted_;
        std::optional<SlotEpochGuard>    slotted_;
    };
};
