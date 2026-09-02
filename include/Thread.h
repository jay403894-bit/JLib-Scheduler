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
#include "TsanFiber.h"   // fiber annotations for ThreadSanitizer; no-ops without it
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
    // ---- WHERE DID EACH TASK COME FROM? -------------------------------------------------------
    //
    // probes/hits answer "is stealing working". These answer a different question the search ORDER
    // turns on: of the work a worker ran, how much came from a queue only IT could drain, versus
    // one anybody could have taken?
    //
    // That distinction is the whole argument about inbox-first versus steal-first. An inbox has
    // exactly one legal consumer, so delaying it is unrecoverable; a deque item is stealable, so
    // delaying it costs the pool nothing. A reordering experiment has to show what it did to that
    // mix, not just to wall time -- and wall time on this row has been unreliable all night.
    //
    // ALSO GIVES BALANCE FOR FREE. Summing a worker's five counters is how many tasks it ran, so
    // the spread across workers is the load-balance number without a separate instrument.
    //
    // Per worker and cache-line aligned for the same reason as probes/hits: a shared counter would
    // manufacture the contention the experiment is trying to observe.
    struct alignas(platform::kCacheLine) StealCounters {
        std::atomic<long long> probes{ 0 };
        std::atomic<long long> hits{ 0 };
        std::atomic<long long> fromHiInbox{ 0 };    // own reserved-lane inbox   -- unstealable
        std::atomic<long long> fromLoInbox{ 0 };    // own ordinary inbox        -- unstealable
        std::atomic<long long> fromResumed{ 0 };    // own pinned-resume inbox   -- unstealable
        std::atomic<long long> fromDeque{ 0 };      // own deque                 -- was stealable
        std::atomic<long long> fromSteal{ 0 };      // somebody else's deque
        // FLOW, NOT A SOURCE, so it does not double-count against the five above: how many tasks a
        // worker moved from its loPri INBOX into its own DEQUE in bulk. This is the number that
        // showed the inbox is a STAGING queue rather than an execution queue -- work arrives there
        // unstealable and is republished stealable in batches, so "an inbox item can only be run by
        // its owner" is true for a much shorter window than the structure suggests.
        std::atomic<long long> stagedFromInbox{ 0 };
        // THE KEPT TASK, run straight out of the drain batch without ever touching the deque. This
        // is the path that made the other counters undercount: with one producer the drain almost
        // always finds a single task, which is now kept and run directly, so it staged nothing and
        // passed no acquisition site. 1p reported 2,700 tasks on a row that pushes a MILLION.
        // Counting it closes the accounting -- every task is now ranDirect, or staged and then
        // taken from a deque, or arrived through one of the inboxes.
        std::atomic<long long> ranDirect{ 0 };
    };
    inline constexpr size_t kStealStatSlots = 256;
    inline StealCounters g_stealStats[kStealStatSlots];

    // ---- WHERE IS EACH WORKER STANDING RIGHT NOW? --------------------------------------------
    //
    // A BREADCRUMB, NOT A TIMER, and the distinction is what makes it usable. Timing each phase
    // would cost a MonotonicNs() per phase against a loop pass that runs in ~100 ns -- the
    // instrument would dominate the thing it measures, which is how the fiber breakdown ended up
    // reporting the floor ramp. This is one relaxed byte store per transition and no clock at all.
    //
    // WHAT IT ANSWERS. The stall watcher already dumps every worker's state mid-stall, and that
    // dump could say AWAKE with empty queues for thirty workers and leave you no wiser -- "awake"
    // is a protocol word, not a location. This says which line of Worker() the thread was last at.
    // For a 2.9 ms dispatch that is the difference between "the pool was scanning" and "the pool
    // was not executing at all", which is the fork the whole diagnosis turns on.
    //
    // Relaxed is right: nothing steers on it, a stale read is a breadcrumb one phase old, and any
    // stronger ordering would put a barrier in the hot loop to serve a diagnostic.
    enum class WorkerPhase : unsigned char {
        Start = 0,      // top of a pass, before any queue is consulted
        HiPri,          // own reserved-lane inbox
        Resumed,        // own pinned-resume inbox
        OwnDeque,       // own deque
        InboxDrain,     // own loPri inbox, the bulk drain
        StealScan,      // probing other workers' deques
        ParkGate,       // inside the park block, deciding whether to sleep
        // A FLOOR WORKER WENT ROUND, AND IT IS NOT A PARK. Without this the idle floor paints
        // ParkGate: it enters the park block, finds nothing, reads the band word, sees it is on the
        // floor and `continue`s -- so the breadcrumb says "deciding whether to sleep" for a thread
        // that is structurally forbidden to sleep.
        //
        // AND THAT MADE EVERY IDLE DUMP LOOK LIKE A HANDSHAKE FAILURE. The floor's resting pose is
        // NOTIFIED + parkGate: every aimed push swaps the word to NOTIFIED, and at F <= the yield
        // threshold the worker never takes the handshake's consume, so the word STAYS NOTIFIED and
        // the phase STAYS parkGate for the whole row. Read as "a permit was latched on a worker at
        // the park gate", that is a stall; read correctly it is an idle floor doing its job -- 0
        // floor parks completed, 0 yield aims, 20000/20000 landings.
        //
        // NOT FIXED BY CONSUMING THE PERMIT ON THIS PATH. That would turn NOTIFIED into EMPTY at
        // the cost of a CAS on every idle pass, and buys nothing: the next Wake still correctly
        // performs no syscall either way. The word is right. The NAME was wrong.
        FloorSpin,      // on the floor, went round rather than parking -- NOT a park attempt
        Parked,         // blocked in WaitOnAddress / futex / condvar
        Running,        // executing a task body
        Count
    };
    inline const char* WorkerPhaseName(unsigned char p) {
        static const char* k[] = { "start", "hiPri", "resumed", "ownDeque", "drain",
                                   "stealScan", "parkGate", "floorSpin", "PARKED", "RUNNING", "?" };
        return p < (unsigned char)WorkerPhase::Count ? k[p] : "?";
    }
    struct alignas(platform::kCacheLine) PhaseSlot { std::atomic<unsigned char> phase{ 0 }; };
    inline PhaseSlot g_workerPhase[kStealStatSlots];
    #define JLIBSCHED_PHASE(q, ph)                                                            \
        do { const size_t _pi = (size_t)(q);                                                  \
             if (_pi < ::JLib::kStealStatSlots)                                               \
                 ::JLib::g_workerPhase[_pi].phase.store(                                      \
                     (unsigned char)::JLib::WorkerPhase::ph, std::memory_order_relaxed);      \
        } while (0)
    inline const char* WorkerPhaseOf(size_t q) {
        return q < kStealStatSlots
             ? WorkerPhaseName(g_workerPhase[q].phase.load(std::memory_order_relaxed)) : "-";
    }
    #define JLIBSCHED_STEAL_STAT(q, field)                                                   \
        do { const size_t _qi = (size_t)(q);                                                 \
             if (_qi < ::JLib::kStealStatSlots)                                              \
                 ::JLib::g_stealStats[_qi].field.fetch_add(1, std::memory_order_relaxed);    \
        } while (0)

    inline void StealStatsReset() {
        for (size_t i = 0; i < kStealStatSlots; ++i) {
            g_stealStats[i].probes.store(0, std::memory_order_relaxed);
            g_stealStats[i].hits.store(0, std::memory_order_relaxed);
            g_stealStats[i].fromHiInbox.store(0, std::memory_order_relaxed);
            g_stealStats[i].fromLoInbox.store(0, std::memory_order_relaxed);
            g_stealStats[i].fromResumed.store(0, std::memory_order_relaxed);
            g_stealStats[i].fromDeque.store(0, std::memory_order_relaxed);
            g_stealStats[i].fromSteal.store(0, std::memory_order_relaxed);
            g_stealStats[i].stagedFromInbox.store(0, std::memory_order_relaxed);
            g_stealStats[i].ranDirect.store(0, std::memory_order_relaxed);
        }
    }

    // Per-worker task sources, plus the balance figures that fall out of them. `spread` is
    // max/median tasks-per-active-worker: 1.0 is perfectly even, and it is the number a search-order
    // change should move if it is doing what its advocates claim.
    struct SourceReport {
        long long hiInbox = 0, loInbox = 0, resumed = 0, deque = 0, stolen = 0, total = 0;
        long long staged = 0;         // inbox -> own deque, a FLOW not a source
        long long direct = 0;         // kept from the drain batch and run without touching the deque
        long long unstealable = 0;   // hiInbox + loInbox + resumed -- work only its owner could run
        double    spread = 0.0;      // max / median over workers that ran anything
        size_t    active = 0;
    };
    inline SourceReport StealStatsSources(size_t workerCount) {
        SourceReport r;
        std::vector<long long> per;
        if (workerCount > kStealStatSlots) workerCount = kStealStatSlots;
        for (size_t i = 0; i < workerCount; ++i) {
            const long long hi = g_stealStats[i].fromHiInbox.load(std::memory_order_relaxed);
            const long long lo = g_stealStats[i].fromLoInbox.load(std::memory_order_relaxed);
            const long long rs = g_stealStats[i].fromResumed.load(std::memory_order_relaxed);
            const long long dq = g_stealStats[i].fromDeque.load(std::memory_order_relaxed);
            const long long st = g_stealStats[i].fromSteal.load(std::memory_order_relaxed);
            r.hiInbox += hi; r.loInbox += lo; r.resumed += rs; r.deque += dq; r.stolen += st;
            r.staged += g_stealStats[i].stagedFromInbox.load(std::memory_order_relaxed);
            const long long di = g_stealStats[i].ranDirect.load(std::memory_order_relaxed);
            r.direct += di;
            // ranDirect is a SOURCE and belongs in the per-worker total -- it is a task that ran.
            // stagedFromInbox is a FLOW and does not, or it would double-count against the deque
            // pop that follows it.
            const long long t = hi + lo + rs + dq + st + di;
            if (t > 0) per.push_back(t);
        }
        r.total = r.hiInbox + r.loInbox + r.resumed + r.deque + r.stolen + r.direct;
        r.unstealable = r.hiInbox + r.loInbox + r.resumed;
        r.active = per.size();
        if (!per.empty()) {
            std::sort(per.begin(), per.end());
            const long long med = per[per.size() / 2];
            r.spread = med > 0 ? (double)per.back() / (double)med : 0.0;
        }
        return r;
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
    struct SourceReport {
        long long hiInbox = 0, loInbox = 0, resumed = 0, deque = 0, stolen = 0, total = 0;
        long long staged = 0;         // inbox -> own deque, a FLOW not a source
        long long direct = 0;         // kept from the drain batch and run without touching the deque
        long long unstealable = 0;
        double    spread = 0.0;
        size_t    active = 0;
    };
    inline SourceReport StealStatsSources(size_t) { return SourceReport{}; }
    #define JLIBSCHED_PHASE(q, ph) ((void)0)
    inline const char* WorkerPhaseOf(size_t) { return "-"; }
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
        // MUTUAL FRIENDSHIP, and both directions are load-bearing.
        //
        // TaskScheduler already declares `friend class Thread`, which is how Worker() reaches the
        // queues -- scheduler->loPriInboxes[qIndex], scheduler->resumedInboxes[qIndex] and the rest.
        // This is the other half: the scheduler owns the thread table and drives shutdown across all
        // of it at once, so it needs this worker's state (running, workerState) without a named
        // accessor minted for every field it touches.
        //
        // The named accessors that DO exist -- GetFiber, GetThread, RequestStop -- stay, because
        // they say what the operation MEANS at the call site. Friendship is the escape hatch for
        // everything that has not earned a name.
        friend class TaskScheduler;

    public:
        // ---- WHAT ONE PUSH TOUCHES ON THIS OBJECT ------------------------------------------
        //
        // PushLocal writes three fields on the worker it selected -- inboxDepth, hasQueuedWork and
        // workerState -- while that worker is concurrently reading or RMW-ing all three. Every
        // DISTINCT CACHE LINE among them is a coherence transfer the producer pays PER TASK, and
        // PushBatch pays once per batch. That difference is most of why Push measures ~422 ns/task
        // against PushBatch's ~61.
        //
        // REPORTED FROM INSIDE THE CLASS because the fields are private and because the layout is
        // the library's own business. Reasoning about it from declaration order does not work --
        // the compiler decides, and these three are declared ~160 lines apart with no alignas
        // between them, which SUGGESTS separate lines. Suggests is not measures.
        static void PushPathFieldOffsets(size_t& inboxDepthOff,
                                         size_t& hasQueuedWorkOff,
                                         size_t& workerStateOff) noexcept;

        static thread_local Thread* self;

        // THE WORKER RUNNING ON THIS THREAD, or nullptr on a non-worker (the app's main thread, an
        // I/O thread). The backing pointer is private because nothing outside the scheduler may
        // reassign it; reading it is safe and is how a task answers "which worker picked me up?",
        // which is what the bench's placement histogram needs and what makes a claim about steering
        // checkable rather than assumed.
        //
        // NOT the same as `self` above, which is declared but never assigned -- read this one.
        static Thread* Current() noexcept { return instance; }

        Context schedulerCtx;

        // TSAN'S HANDLE FOR schedulerCtx, or null without the sanitizer. The worker's own context is
        // a FIBER from TSan's point of view even though nothing here calls it one, so switching back
        // to it has to be announced exactly like switching into a task fiber.
        //
        // Obtained with __tsan_get_current_fiber() at the top of Worker() rather than created: this
        // context already exists and TSan is already on it, so asking which one it is on is right
        // and creating a second handle for the same stack would be wrong.
        void* tsanSchedulerFiber = nullptr;

        // TSAN: "I am switching back to my worker's own context." Called before every
        // ContextSwitch(&X->ctx, X->homeCtx) -- eleven sites across Fiber.cpp, GlobalFiberPool.cpp
        // and TaskScheduler.cpp, all the same direction, so they share one call rather than eleven
        // spellings that could drift.
        //
        // NULL-TOLERANT AT BOTH LEVELS: no current Thread (a fiber running somewhere that is not a
        // worker) and no handle yet both skip silently. Losing one edge costs fidelity; passing
        // null into the runtime is undefined.
        static void TsanSwitchToScheduler() noexcept {
            if (Thread* t = GetCurrent()) tsan::SwitchTo(t->tsanSchedulerFiber);
        }
        Fiber* currentFiber = nullptr;
        Task* currentRunningTask = nullptr;
        int qIndex = 0;
        // WS_AWAKE(0) / WS_GOING_TO_SLEEP(1) / WS_SLEEPING(2). NO LONGER READ BY THE PLACEMENT PATH:
        // that read existed only to decide whether to bump NoteWakeMiss, a counter nothing loaded,
        // and it touched this line on every push while the owning worker was RMW-ing it. NotifyWorker
        // still reaches this word, for a reason.
        // seq_cst, NOT relaxed, and it was relaxed. Every caller compares this against WS_PARKED to
        // decide whether somebody needs waking or may be targeted -- which makes it one half of a
        // Dekker pair with MarkQueuedWork, and a Dekker pair is only sound when BOTH sides are
        // seq_cst. Parked() was already seq_cst; this was the same question asked weakly, and the
        // five call sites reading it could act on a stale answer. The loads are in wake-selection
        // scans, not a hot loop -- each is followed by a syscall or a placement decision -- so the
        // strengthening costs nothing measurable and removes a class of doubt entirely.
        int GetWorkerState() const noexcept { return workerState.load(std::memory_order_seq_cst); }

        // ---- DID THIS WORKER EXECUTE AT ALL DURING SOME INTERVAL? ----------------------------
        //
        // THE ONE NUMBER THAT SEPARATES "the scheduler did not dispatch" FROM "the OS did not run
        // the thread", which every stall analysis so far has had to infer from poll counts and core
        // numbers. It counts IDLE PASSES of Worker(): a worker that is spinning increments it
        // thousands of times a millisecond, and a worker that is not on a CPU cannot increment it
        // at all.
        //
        // Sample it either side of an interval and read the delta:
        //     delta  > 0   the thread WAS running and scanning, and still did not take the work.
        //                  That is a scheduler defect -- the search missed a reachable queue.
        //     delta == 0   the thread did not execute. No scheduler change can help; the OS did
        //                  not schedule it.
        //
        // Relaxed, and only meaningful on an IDLE worker -- a worker inside a task body is not
        // taking idle passes either, so a zero delta there means "busy", not "not running". Read it
        // next to `busy`.
        unsigned SpinTick() const noexcept { return dbgSpinTick.load(std::memory_order_relaxed); }

        // The OS thread itself, so a caller can join it directly -- `threads[i]->GetThread().join()`
        // -- rather than going through Thread::Join().
        //
        // WHY THAT IS THE BETTER SHUTDOWN. Thread::Join() flipped `running`, notified, then waited
        // on a condition variable for a predicate it had just satisfied itself, then joined the
        // thread anyway. Every step but the last was ceremony around the one call that actually
        // waits. std::thread::join() returns when the OS thread has genuinely exited, which is a
        // stronger statement than any flag handshake was making.
        //
        // THE CALLER OWNS THE ORDER, and that is the point of exposing it. Shutdown is: stop the
        // producers, cancel every parked frame so it can unwind, ResumeAll() so workers are awake to
        // run the unwinding, THEN join. Splitting "make it stop" from "wait for it to stop" is what
        // lets those happen in that order across the whole pool instead of one worker at a time --
        // which is precisely what strands a fiber pinned to a worker that has already left.
        std::thread& GetThread() { return thread; }

        // Tell this worker to leave its loop. Does NOT wait -- that is GetThread().join(), and the
        // separation is the whole point.
        //
        // Thread::Join() fused the two, which forced shutdown to be per-worker: stop one, wait for
        // it, stop the next. Worker 0 was therefore already gone while 1..N were still draining, and
        // a fiber pinned to worker 0 resumed into a queue nobody would ever pop again -- stranded
        // mid-unwind, so its destructors never ran. Split, the caller can stop EVERY worker, wake
        // them all, and only then wait for them, which has no such window.
        void RequestStop() {
            running.store(false, std::memory_order_release);
            // AND WAKE IT. Harmless while the pool only spins -- a spinning worker sees `running`
            // on its next pass either way -- and REQUIRED the moment a blocking park returns: a
            // parked worker is off the run queue and never observes the flag on its own, which is
            // a Join that waits forever on a thread told to stop that cannot hear it.
            Wake();
        }

        // Bring this worker back to its search loop. Publishes a state CHANGE and then wakes the
        // address, in that order, and both halves are required.
        //
        // TODAY THIS IS JUST THE STATE STORE, because the pool spins and a spinning worker observes
        // it on its next pass. There is no OS call left in here.
        //
        // KEPT AS A NAMED OPERATION RATHER THAN INLINED, because the moment a blocking park returns
        // this is where its signal goes, and the ORDER is the part that is easy to get wrong:
        // publish the state change, THEN signal. A wake delivered between the waiter's last
        // predicate check and the point it actually blocks reaches nobody and is gone -- the waiter
        // then sleeps on a value nobody will change again. Changing the word first means a late
        // waiter never blocks at all. That is the futex rule, and skipping it is not a theoretical
        // lost wakeup: it hung the pool intermittently and hung Join every single time.
        void Wake() noexcept;

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

        // Pops ONE task from this worker's own lane inbox, for a blocked owner to run while it
        // spin-helps. Returns null if the lane is empty, or if the next lane task was fiber-backed
        // -- which a fiberless helper cannot run, so it is relocated to the loPri deque and
        // `relocated` is set, meaning a notify is owed. See the definition for why the owner
        // looking at its own inbox is the only legal way that work can move.
        Task* TryTakeLaneTask(bool& relocated);
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
        // == WS_PARKED, not "!= awake". Under the permit machine WS_NOTIFIED means a wake is
        // latched for a worker that is NOT parked -- it is running, or about to consume the permit
        // and go round again. Treating that as parked would spend a wake budget on somebody who
        // needs no syscall, which is the opposite of what this predicate is for.
        bool Parked() const { return workerState.load(std::memory_order_seq_cst) == WS_PARKED; }

        bool Ready();

        // DIAGNOSTIC ONLY, for TaskScheduler::DumpPoolState(). Reads are relaxed and unsynchronised
        // because this runs from a watchdog on a pool that is already presumed wedged -- a torn or
        // slightly stale view is fine, and taking any lock here could hang the diagnostic itself.
        // The combination worth looking for is state=SLEEPING with queued=1: a worker parked while
        // holding work only it can drain, which is the lost-wakeup signature.
        struct DebugState {
            int  qIndex;
            // RAW, NEVER COLLAPSED TO "awake". EMPTY and YIELD are both "not parked" and they
            // mean opposite things to a pusher -- EMPTY says "on core, I will find it myself",
            // YIELD says "I am off the core, aim elsewhere". A dump that folds them together
            // cannot show the difference between a healthy idle pool and one whose cores are all
            // in a yield window.
            int  workerState;      // WS_EMPTY / WS_NOTIFIED / WS_PARKED / WS_YIELD
            bool hasQueuedWork;
            bool laneWake;         // the OTHER edge-triggered hint; cleared at the top of a pass
            bool onAwakeFloor;     // as THIS PASS saw it -- a snapshot, and the floor grows
            unsigned spinTick;     // did this pass take the yield arm? (tick & mask) == 0
            bool busy;
            bool running;
        };
        DebugState GetDebugState() const {
            return DebugState{
                qIndex,
                workerState.load(std::memory_order_relaxed),
                hasQueuedWork.load(std::memory_order_relaxed),
                laneWake.load(std::memory_order_relaxed),
                dbgOnAwakeFloor.load(std::memory_order_relaxed),
                dbgSpinTick.load(std::memory_order_relaxed),
                busy.load(std::memory_order_relaxed),
                running.load(std::memory_order_relaxed)
            };
        }
    private:
        Fiber* AcquireFiber(Task* task);
        void ReleaseFiber(Fiber* f);

        // What happens to a fiber and its task once it has switched back out. Extracted from
        // Worker() so a bound main thread can run the identical state machine rather than a second
        // copy of it. See the definition in Thread.cpp for the three outcomes and their traps.
        void OnFiberReturned(Fiber* f, Task* task) noexcept;
        uint32_t FastRand();
        void WaitBackoff(int& spin_count);
        void ExecuteTask(Task* task);
        Task* AcquireWork(bool& isFork);   // inbox drain + localQ + pop_bottom + steal
        void  RunTask(Task* task, bool isFork);  // acquire/resume fiber, switch, handle DEAD/YIELD/SUSPEND
        void Worker();

        // Called from BOTH completion arms after a task ends. Grows the awake floor and hands this
        // worker.s backlog to overflow workers when the body that just ended was long enough that
        // queueing behind it is expensive. Factored out because it lived in the two arms
        // separately and immediately drifted: the Native arm redistributed and the fiber arm only
        // grew, so a wave of FIBER tasks reproduced the original bug -- floor at 16, nobody fed.
        void GrowFloorIfLongBody(long long bodyNs);

        // Wake sleepers for a still-live published range, sized by the leaf this worker just ran.
        // Evidence-driven, not predicted -- see the definition.
        void RecruitForLiveRange(long long bodyNs);

        TaskScheduler* scheduler;
        ThreadLocalCache<> localCache;
        static thread_local Thread* instance;

        // Set by MarkQueuedWork() (see its comment) whenever THIS worker's own inbox/deque
        // receives a task; cleared once per Worker() loop iteration right before that
        // iteration's local-queue/steal/inbox-drain search, so a push that lands between one
        // clear and the next always either (a) gets found directly by that same iteration's
        // search, or (b) re-arms this flag for the next predicate check. Deliberately NOT a
        // pool-wide counter (that was the old `queuedTasks` design) -- a worker that's
        // genuinely asleep only needs to know about work landing on ITSELF; stealable work on
        // OTHER workers' deques is already found for free by the unconditional steal-attempt
        // phase every awake worker runs each loop pass, with no predicate involved at all.
        // Tasks pushed into THIS worker.s inbox and not yet drained. Per-worker rather than
        // pool-wide so producers aiming at different workers do not share a cache line, and
        // maintained unconditionally -- the pool-wide g_inboxDepth only counts when a submit limit
        // is set, so it reads zero in every default configuration and cannot steer anything.
        //
        // EXISTS TO ANSWER "HOW MUCH", NOT "ANY". hasQueuedWork is a bool, and a bool cannot tell a
        // 16-task wave queued behind two workers from a 6-node graph doing the same -- which is
        // exactly the distinction the floor growth rule got wrong.
        // ---- CONDVAR PARK ARM (A/B against WaitOnAddress/futex) ---------------------------
        //
        // Per-worker, so the sleep is one-waiter-one-address either way and there is no herd for
        // either primitive to avoid. Only one of the two arms is ever used in a run; see
        // TaskScheduler::ParkPrimitive.
        //
        // WHY THIS IS WORTH MEASURING IN THE SCHEDULER rather than in isolation: the isolated
        // ping-pong says condvar has tighter wake variance, but it cannot see the two things that
        // actually differ here -- the mutex condvar puts back on the NOTIFY path, and the fact
        // that the WaitOnAddress arm re-reads three inboxes and four seq_cst flags before it ever
        // blocks, work the condvar arm does under its predicate lock instead.
        std::mutex              parkMx;
        std::condition_variable parkCv;

        // HOW MANY TIMES THIS WORKER ACTUALLY BLOCKED. Diagnostic only -- nothing steers on it.
        // Exists so a bench can check its DECLARED bands against OBSERVED behaviour instead of
        // printing the same atomics the scheduler already steers by, which agrees with itself
        // whatever the wiring does. See the park site in Worker().
        std::atomic<unsigned> parkCount{ 0 };

        std::atomic<int> inboxDepth{ 0 };

        // MonotonicNs at which this worker entered its CURRENT task, or 0 when it is not in one.
        // Published so a PUSHER can ask "has this worker been busy longer than a trivial body?",
        // which is the only signal that separates a wave of 3 ms tasks from a flood of 60 ns ones.
        // Both look identical by queue depth, and depth alone grew the floor to 16 on a no-op flood.
        //
        // COSTS NOTHING NEW: the clock read already happened here for the floor-utilisation window,
        // and only floor workers pay it -- one or two threads, not thirty-one.
        std::atomic<long long> taskStartNs{ 0 };

        std::atomic<bool> hasQueuedWork{ false };

        // ---- THE THIRD FIELD EVERY PUSH WRITES, MOVED HERE FOR THE CACHE LINE ----------------
        //
        // Declared next to inboxDepth and hasQueuedWork because PushLocal writes all three on the
        // target worker, once per task. Measured at 64-byte granularity they used to straddle two
        // coherence lines (2460/2472 on line 38, 2520 on line 39), so a producer paid two transfers
        // per push against a worker concurrently RMW-ing the same words. Now one.
        //
        // NOT FREE IN BOTH DIRECTIONS, which is why this is a measurement and not a rule: sharing a
        // line means the worker's RMW on workerState now invalidates the line holding the two
        // fields the producer writes, and vice versa. It wins if producer writes outnumber worker
        // RMWs on this object, and the throughput row is exactly that shape. The semantics and the
        // WS_* enum are documented at the enum's declaration further down; this is storage only.
        std::atomic<int> workerState{ 0 /* WS_EMPTY -- enum is declared below, see there */ };

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

        // PUBLISHED FOR THE DUMP ONLY. onAwakeFloor and spinTick are pass locals, and without them
        // the snapshot cannot answer "was this worker in the yield arm when the push landed" --
        // which is the question a row of NOTIFIED-with-empty-queues actually raises. Relaxed, and
        // written on the IDLE path only, so they cost nothing on a pass that found work.
        std::atomic<bool>     dbgOnAwakeFloor{ false };
        std::atomic<unsigned> dbgSpinTick{ 0 };

        // "The last thing I re-queued was a YIELD, not a genuine resume."
        //
        // NOT ATOMIC, and that is the point: it is written by this worker in OnFiberReturned and
        // read by this worker in its own search, both on its own thread. A yielded fiber is
        // re-queued only by the worker that ran it (single-producer, stated at the push site), so
        // there is no second writer and no ordering to establish.
        //
        // It exists because the resumed inbox is checked before the loPri inbox drain -- correct
        // for a pinned resume, which nobody else may drain, and starvation for a yield loop, which
        // re-arms that check forever. See the push site in Thread::OnFiberReturned and
        // tests/yield_starvation_test.cpp.
        bool yieldedLastPass = false;

        // ---- LANE DUTY CYCLE, sampled BY THE WORKER ITSELF ------------------------------------
        // How often this worker actually has lane work, counted on its own loop at its own rate.
        //
        // THE CENTRAL-SAMPLER VERSION FAILED, and the failure is the reason this lives here. The
        // controller first polled the top worker from ONE driver worker`s pass counter -- and under
        // a light trickle that driver barely loops, so a 200 ms window gathered ONE OR TWO samples
        // where it needed dozens. Measured: s=1, s=2, against s=39348 in a window where the pool
        // happened to be spinning. A measurement whose density depends on a third party`s loop rate
        // is not a measurement.
        //
        // Counted by the subject instead, so density scales with the thing being measured. Both are
        // relaxed and live on lines this worker owns; the controller reads them rarely.
        //
        // Public because the controller in TaskScheduler.cpp reads and resets them; nothing else
        // should touch them.
    public:
        std::atomic<unsigned> laneCyclesTotal{ 0 };
        std::atomic<unsigned> laneCyclesBusy{ 0 };
        // Nanoseconds spent executing LANE tasks. Time, not passes: the loop alternates an execute
        // pass with a search pass, so a fully occupied worker tops out at exactly 50%% of passes --
        // measured as 32/64, 18/36, 14/28, structurally rather than statistically. A pass is not a
        // unit of time, so the ratio it produces is not an occupancy.
        std::atomic<long long> laneBusyNs{ 0 };
        // LANE TASKS RUN in the window. For a DEADLINE lane this is the honest "is this core earning
        // its keep" signal, and occupancy is not: a hot worker resuming an I/O completion does a few
        // microseconds of work and suspends again, so a fully-loaded lane worker can sit at single-
        // digit occupancy. It is paid to be AVAILABLE, not busy. Counting tasks separates "this core
        // takes a share of the arrival rate" from "this core is surplus", which is the actual
        // question, and it does not punish a lane for being fast.
        std::atomic<unsigned> laneTasksRun{ 0 };

        // Tasks this worker has picked up since the awake-floor controller last read it. Exchanged
        // to zero per window, so it answers exactly one question: did the MARGINAL floor worker do
        // anything this window? Zero for several windows running is the demote signal.
        //
        // Relaxed and per-worker: an exact count is not needed, only zero versus non-zero, and the
        // line is this worker's own.
        std::atomic<unsigned> tasksRun{ 0 };

        // Nanoseconds this worker spent EXECUTING since the controller last read it. The saturation
        // signal: a floor worker at ~100% is the case the wake-miss counter cannot see, because
        // work piling onto an already-awake worker wakes nobody. That is exactly `burst` -- sixteen
        // tasks onto one worker, no misses, and without this the floor would never grow.
        //
        // ACCUMULATED ONLY BY WORKERS INSIDE THE FLOOR, which is what makes the two clock reads per
        // task affordable: there are one or two such workers, not thirty-one. The old K-hot version
        // charged this to every lane task and its own comment called that out as a cost paid by
        // users who never asked for scaling.
        std::atomic<long long> busyNs{ 0 };

    private:

        // Worker sleep state, so a push can skip the wake entirely when this worker is already
        // running. Three states rather than a bool because the interesting window is between
        // "decided to park" and "actually inside cv.wait": GOING_TO_SLEEP publishes the INTENT, so
        // a pusher arriving mid-decision still signals instead of assuming it will be seen.
        //
        // Protocol and its proof: tests/verify/sleepwake_model.c. Every transition here and the
        // load in NotifyWorker are seq_cst, and that is not defensive -- the model's -DACQ_REL_ONLY
        // negative control fails with a lost wakeup.
        // ---- THE PARK PERMIT WORD. THREE STATES, EVERY WRITE AN RMW. ------------------------
        //
        // Sleep IS suspend and wake IS resume, so this mirrors the fiber machine rather than
        // approximating it. The fiber side has carried the needed window all along:
        //
        //     fiber                 thread
        //     WANTS_SUSPEND         about to block, not published yet
        //     SUSPENDED             WS_PARKED    -- the thread is actually waiting
        //     SUSPEND_SIGNALED      WS_NOTIFIED  -- the wake won the race, permit latched
        //     READY + requeue       consume the permit, run again
        //     Resume                Wake
        //
        // WS_GOING_TO_SLEEP IS GONE and its absence is the point. It was an "intent, uncommitted"
        // state that nothing could act on: a waker seeing it had to guess, and a wake landing
        // between "I saw no work" and "I blocked" was dropped -- the worker then slept with work in
        // its local deque, its inbox, or a just-resumed fiber. Here the CAS to WS_PARKED IS the
        // commitment, exactly as the fiber publishes SUSPENDED only after its context is saved,
        // and a wake that arrives before it is LATCHED as WS_NOTIFIED rather than lost.
        //
        // WS_PARKED KEEPS THE VALUE 2 ON PURPOSE. Several call sites in TaskScheduler.cpp test
        // `GetWorkerState() == 2 /* WS_SLEEPING */` to mean "is this worker asleep". Renumbering
        // would leave those compiling and silently meaning something else, which is the quietest
        // possible way to break placement.
        //
        // NOT THE FIBER'S WORD, and never share it: Fiber Resume enqueues a task, thread Wake only
        // unparks a core. Mixing them gives "fiber is READY, worker is PARKED, nobody runs".
        //
        // Modelled in tests/verify/sleepwake_permit_model.c. The proposed machine verifies clean;
        // the control that removes BOTH the swap-wake and the post-commit recheck is a safety
        // violation. Each mechanism alone is sufficient -- do not "simplify" either without
        // reproducing that control.
        // WS_YIELD IS THE FOURTH STATE AND IT IS NOT A KIND OF PARKED. A floor worker never parks,
        // so its word reads EMPTY for its whole life -- and EMPTY is a CLAIM: "on the core,
        // scanning, a push needs no syscall because I will find it myself". The floor yields every
        // 8th idle pass (Worker(), the onAwakeFloor arm) to avoid burning a core, and
        // std::this_thread::yield() is not a small pause -- the thread LEAVES THE CORE. For that
        // window EMPTY is a lie, the push skips its wake, and the dump says AWAKE while the task
        // waits for the OS to reschedule the thread: a quantum under oversubscription.
        //
        //   EMPTY     on core, scanning. No syscall needed; the scan will find it.
        //   NOTIFIED  a permit is latched.
        //   PARKED    committed sleep. A push owes a syscall.
        //   YIELD     about to leave, or off, the core. Do not aim here. NO SYSCALL IS OWED --
        //             the thread is RUNNABLE and comes back on its own, so a futex wake would be
        //             aimed at a thread that is not waiting.
        //
        // WHY 3, AND WHY PARKED KEEPS 2. Every reader outside the park machine asks "is it asleep"
        // and spells it `== 2`, so a fourth value is invisible to all of them -- which is the whole
        // reason it is safe to add. The shapes that WOULD break are `!= 0` meaning "needs a
        // syscall" (would futex a runnable thread) and `== 0` meaning "aim here" (would aim at a
        // core that is gone). Neither exists today; both compile, so do not introduce one.
        // Parked() stays `== WS_PARKED`. DO NOT FOLD YIELD INTO IT.
        //
        // Modelled in tests/verify/yieldstate_model.c, which also says which half is load-bearing:
        // the HANDSHAKE is mandatory (-DYIELD_STORE_BACK is a genuine lost task), while placement
        // skipping YIELD is a LATENCY win only -- -DTARGET_YIELDED is green, because the swap
        // still latches and the return CAS sees NOTIFIED and rescans.
        enum WorkerState : int { WS_EMPTY = 0, WS_NOTIFIED = 1, WS_PARKED = 2, WS_YIELD = 3 };
        // THE MEMBER ITSELF MOVED UP, next to inboxDepth and hasQueuedWork. The enum stays here
        // with the reasoning above; only the storage was relocated, and only for layout.
        //
        // WHY: PushLocal writes all three of those fields on the target worker, and measured at
        // 64-byte granularity they occupied TWO coherence lines -- inboxDepth 2460 and
        // hasQueuedWork 2472 on line 38, workerState 2520 on line 39, with ~48 bytes of lane and
        // debug counters between them. Two lines is two transfers per push from a producer that
        // the owning worker is concurrently RMW-ing. See tests/thread_layout_test.cpp, which
        // reports the offsets and fails if a future field splits them again.

        std::atomic<bool> running{ false };
        std::atomic<bool> ready{ false };
        std::atomic<bool> joining{ false };

        Task* task = nullptr;
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
        // ONE MECHANISM, SO NO CHOICE LEFT TO MAKE. This used to branch on OnCoroutineTask() and
        // route coroutines through a counted guard -- a TLS read, a null check and a task-type load
        // on EVERY guard, on the path measured at 2.52 billion guards/sec. The counted ring is gone
        // (see Epochs.h), so this is one slot guard and the branch with it.
        EpochGuard() : slotted_(CurrentEpochSlot()) {}
        EpochGuard(const EpochGuard&) = delete;
        EpochGuard& operator=(const EpochGuard&) = delete;
    private:
        SlotEpochGuard slotted_;
    };
};
