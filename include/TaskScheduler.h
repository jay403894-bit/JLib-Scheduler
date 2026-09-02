// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// == CREDIT / PRIOR ART ==
//
// THE SHAPE OF THIS SCHEDULER IS NOT ORIGINAL, and the honest history is neither "copied" nor
// "invented". It was started independently, before the talks were known to exist. They were found
// partway in, and from that point the design was informed by them -- so what began as convergence
// became, deliberately, the same design.
//
// The reference is Christian Gyrling, "Parallelizing the Naughty Dog Engine Using Fibers" (GDC
// 2015): a small pool of worker threads, fibers as the unit that blocks so a wait costs a context
// switch instead of a core, and counters as the sync primitive. It was published as a TALK, with no
// paper and no reference implementation, so nothing here is a port -- but nothing here is unaware of
// it either, and claiming pure independence would be as inaccurate as claiming to have followed a
// spec.
//
// CONCRETELY, WHAT WAS TAKEN: the fiber stack split -- 64 KB standard, 512 KB heavy -- is
// FiberTaskingLib's numbers, the open implementation of that design. Where the two differ
// elsewhere, the difference came from a measurement, and CHANGELOG.md says which one.
//
// The pieces underneath it are equally other people's, and are named where they are used rather
// than only here:
//
//   Chase-Lev work-stealing deque    David Chase and Yossi Lev (SPAA 2005). Memory ordering from
//                                    Le, Pop, Cohen and Zappa Nardelli (PPoPP 2013) -- see
//                                    TaskDeque.h, and README.md for what our own model checking
//                                    settled about the steal CAS.
//   Intrusive MPSC queue             Dmitry Vyukov. See TaskMPSCQueue.h.
//   Epoch-based reclamation          Keir Fraser (2004). The COUNTED variant used for coroutines
//                                    is SRCU-shaped -- see Epochs.h for why a coroutine cannot
//                                    have a slot and what that costs.
//   Hazard pointers                  Maged M. Michael (IEEE TPDS, 2004). See Hazard.h, which also
//                                    documents the two bugs a textbook port has in a fiber runtime.
//   Work stealing as a discipline    Blumofe and Leiserson (Cilk). ParallelFor is Cilk-style, with
//                                    no cost model -- steals decide how work is divided.
//   Fiber stack sizes               FiberTaskingLib (RichieSams) -- 64 KB / 512 KB, see
//                                    GlobalFiberPool.h.
//   Treiber stack                    R. K. Treiber (1986). Used for Event's waiter list until
//                                    2.15.0, when a flat fiber-indexed table replaced it.
//   ConcurrentQueue, LightweightSemaphore
//                                    Cameron Desrochers (moodycamel), vendored under their own
//                                    licenses -- see include/LICENSE.md.
//
// WHAT IS ACTUALLY OURS is the integration and the failure modes: three execution modes sharing one
// pool, the K-hot lane and its controller, counted epochs so a coroutine reader is safe rather than
// forbidden, hazard cells indexed by the thing that MIGRATES instead of by thread, the cancellation
// model, and the tripwires. Those are the parts with no prior art to copy, which is also why they
// are the parts that have been wrong most often.
//
// == THE DECISION THAT DECIDES THE OTHERS: FIBERS MIGRATE ==
//
// A resumed fiber may continue on ANY worker, not the one that suspended it. Everything below is
// downstream of that, so changing it is not a local edit -- it re-opens four subsystems at once.
//
// WHY MIGRATE, given that pinning is the conservative default and what middleware does (marl:
// "the fiber must belong to this worker"). Two reasons, and the first is the load-bearing one:
//
//   1. WE OWN EVERY JOB IN THE PROCESS. Pinning exists to protect a library from `thread_local`
//      state it cannot audit -- the host's allocator caches, profilers, loggers -- read from a
//      different thread than the one that wrote them. A game engine has no such foreign code on
//      its fibers, so the hazard is a rule we can enforce ("do not cache a TLS-derived value
//      across a suspension point") rather than a structure we must build.
//   2. STACK WARMTH DECAYS; WAITING DOES NOT. Pinning preserves a 64 KB stack in its home core's
//      cache -- real, but only for very short suspends. Park on an I/O completion for a
//      millisecond and the stack is evicted several times over, so pinning then buys nothing and
//      still costs a wait for one specific worker. Under a frame deadline, resuming NOW on a cold
//      stack beats resuming later on a warm one.
//
// WHAT MIGRATION COSTS, all of it deliberate and all of it paid elsewhere:
//
//   * SlabPool::Free routes by ADDRESS, not by "the freeing thread owns this". A pinned design
//      could always push to the local free list and skip the ownership question entirely.
//   * Epochs use a GLOBAL participant list with CAS rather than a per-shard scheme. A sharded
//      design decides reclamation locally, which is cheaper and scales better with core count --
//      and is UNSOUND under migration, because a read that starts on worker A and finishes on B
//      belongs to no single shard. Seastar gets the cheap version by being shared-nothing; we
//      cannot, and the global list is the price of resume-anywhere.
//   * Hazard cells are indexed by the FIBER, not the thread, for the same reason.
//
// SO IF A REWRITE EVER PINS, three things change together and none of them are optional: the
// allocator can drop address routing, epochs/hazards can sharded per worker, and the mailbox stops
// needing a drain-to-deque escape. Pick one branch and follow it; the expensive mistake is taking
// the cost of migration and the constraints of pinning at the same time.
//
// THE MAILBOX IS THE MIDDLE GROUND, and it is why the inbox exists at all. An inbox has exactly one
// legal consumer, so a task put back in its owner's inbox is resumed by that same worker with the
// stack still warm -- pinning's benefit as a PLACEMENT rather than a rule. The drain to the deque is
// the escape hatch for when the owner does not come back promptly. Too eager and short suspends
// lose their warmth; too late and a busy owner strands work nobody else can reach.
//
// AND THE TRADE THAT NEVER GOES AWAY: mailboxes are faster round-trip, deques are faster parallel.
// A mailbox is one hop and no contention; a deque is stealable the moment it is published. Latency
// wants the first, a frame's parallel phase wants the second, and no single structure is both.

#pragma once
#define NOMINMAX
#include "Task.h"
#include "CancelToken.h"
#include "TaskMPSCQueue.h"
#include "concurrentqueue.h"   // the shared lane intake -- see laneIntake
#include "Epochs.h"
#include "TaskDeque.h"
#include "TaskAllocator.h"
#include "Topology.h"   // topology::CpuMask -- llcMaskOfWorker is one per worker
#include <cstdio>   // the stale-library guard reports through stderr
#include <cstdlib>  // ...and aborts rather than corrupting the heap
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>   // SchedulerMutex bare-thread fallback
#include <functional>
#include <string>
#include <unordered_map>
#include <thread>
// <immintrin.h> was here and is x86-only. Removed rather than guarded, for the same reason
// LockFreeList.h lost its <intrin.h>: nothing in this header uses an intrinsic. The spin hint the
// .cpp needs is platform::CpuRelax() in platform.h, which is arch-correct by construction.
#include <queue>
#include "GlobalFiberPool.h"
// The three synchronisation primitives, all pulled in here so a caller needs only this header --
// WaitFor takes a WaitGroup&, GetEvent returns an Event&, and WaitOnEventDirectArmed hands out a
// DirectEvent*. Event.h used to be excluded because it depended on this header; it now depends on
// Fiber.h instead, which is all it ever needed.
#include "DirectEvent.h"
#include "WaitGroup.h"
#include "WaitPrimitive.h"
#include "Event.h"
namespace JLib {
	class Thread;

	// How a cancellable wait ended.
	namespace detail {
		// TASK SIZE HISTOGRAM -- opt-in, -DJLIBSCHED_TASK_STATS=ON.
		//
		// THE QUESTION. TaskAllocator::SLOT is 256 because it must fit the LARGEST LambdaTask<F>
		// (see the static_assert in CreateTask). But sizeof(Task) is 64, and a capture-free task is
		// exactly that -- so it occupies a quarter of the slot it is given. Coroutine frames had the
		// same shape and a 64-byte class turned out to be worth 4x the memory on 75% of them; the
		// task path is the same question on a far higher volume (292,565 tasks vs 0 frames in the
		// DAG scaling bench).
		//
		// WHY THIS CANNOT BE ANSWERED FROM THE BENCHES. Every task in bench/ is a capture-free
		// function pointer or an empty lambda, so a bench-derived histogram is maximally flattering
		// and would argue for a 64-byte class on evidence that says nothing about real code. The
		// distribution that matters comes from an APPLICATION -- Game01, the physics step, the
		// renderer's per-cell submits -- where lambdas actually capture. Run it there before
		// choosing anything.
		//
		// 32-byte buckets rather than the coroutine histogram's octaves, because the decision here
		// is exactly WHERE to put a class boundary, and 64/128/256 would beg that question.
		//
		// Sharded per thread for the same reason every other counter in this library is: a shared
		// atomic on the creation path would manufacture contention and perturb the thing being
		// measured. This one is on the HOT path -- every CreateTask -- so it matters more here than
		// anywhere else.
#if defined(JLIBSCHED_TASK_STATS)
		inline constexpr size_t kTaskSizeBuckets = 8;
		inline constexpr size_t kTaskSizeShards = 64;

		struct alignas(platform::kCacheLine) TaskSizeShard {
			std::atomic<uint64_t> bucket[kTaskSizeBuckets]{};
			std::atomic<uint64_t> maxSize{ 0 };
			std::atomic<uint64_t> totalBytes{ 0 };
		};
		inline TaskSizeShard g_taskSizeShards[kTaskSizeShards];
		inline std::atomic<size_t> g_taskSizeNext{ 0 };

		inline TaskSizeShard& TaskSizeSlot() {
			static thread_local TaskSizeShard* s = [] {
				const size_t n = g_taskSizeNext.fetch_add(1, std::memory_order_relaxed);
				return &g_taskSizeShards[n % kTaskSizeShards];
			}();
			return *s;
		}

		// Upper bound of each bucket, so bucket i covers (kTaskSizeEdge[i-1], kTaskSizeEdge[i]].
		// Edges at 80 and 96 because a REAL application turned out to be bimodal at exactly 64 and
		// 80 (Game01, 28,619 tasks), and 32-byte buckets hid that -- everything landed in one "<=96"
		// row that could have meant anything from 65 to 96. A histogram whose resolution is coarser
		// than the decision it informs is a histogram that has to be re-run.
		inline constexpr size_t kTaskSizeEdge[kTaskSizeBuckets] = { 64, 80, 96, 128, 160, 192, 256, 0 };

		inline void RecordTaskSize(size_t n) {
			TaskSizeShard& s = TaskSizeSlot();
			size_t b = kTaskSizeBuckets - 1;                       // last bucket is "> 256"
			for (size_t i = 0; i + 1 < kTaskSizeBuckets; ++i)
				if (n <= kTaskSizeEdge[i]) { b = i; break; }
			// Sole writer per shard in the common case, so load/store rather than fetch_add -- the
			// same trick TaskAllocator::liveAdd uses, and for the same measured reason.
			s.bucket[b].store(s.bucket[b].load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
			s.totalBytes.store(s.totalBytes.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
			uint64_t prev = s.maxSize.load(std::memory_order_relaxed);
			while (n > prev && !s.maxSize.compare_exchange_weak(prev, n, std::memory_order_relaxed)) {}
		}

		void ReportTaskSizes();
		// Explicit stream, so the shutdown auto-report can reach a file. A GUI app has no console.
		void ReportTaskSizesTo(std::FILE* out);
#else
		inline void RecordTaskSize(size_t) {}
		inline void ReportTaskSizes() {}
		inline void ReportTaskSizesTo(std::FILE*) {}
#endif
	}

	enum class WaitResult : uint8_t {
		Ok,          // acquired the lock / took the permit
		Cancelled,   // the scope was cancelled; NOTHING WAS ACQUIRED -- do not Unlock or Signal
	};

	class TaskScheduler;

	namespace detail {
		// NOT PUBLIC API, AND NOT A RESTART. Tears the pool down mid-process: stops the service
		// threads, drains every registered primitive so parked frames unwind, joins the workers.
		// The pool does NOT come back -- Init() throws on a non-null instance and always has.
		//
		// This exists because taking Join() off the public surface left two callers that are not
		// applications and whose need is real:
		//
		//   tests/teardown_drain_test.cpp -- the drain is the thing under test, so it has to be
		//   invoked deliberately rather than waited for at exit.
		//
		//   bench/compare/compare_{marl,taskflow,enkits}.cpp -- these tear JLib down BEFORE timing
		//   the competitor, so an idle JLib pool is not taxing someone else's numbers. That is a
		//   fairness property of the comparison, not a convenience; without it every cross-library
		//   row in the README is measured against a machine that is also running this scheduler.
		//
		// Deliberately a free function in `detail` rather than a public member: an application that
		// finds this has been told what it is, and nothing in the supported surface implies a pool
		// can be stopped and started again.
		void TeardownForTesting(TaskScheduler& scheduler);

		// ---- THE ONE PLACE THE DESTRUCTORS ACTUALLY RUN --------------------------------------
		//
		// `Init()` does `instance = new TaskScheduler(...)` and nothing ever deletes it; at exit
		// AtExitDestroyer calls Join() and then LEAKS on purpose, because a process about to stop
		// existing gains nothing from freeing its address space. That is the right shipping choice
		// and it has a cost nobody was paying attention to: **~TaskScheduler and every member
		// destructor under it have never executed, in any program, ever.**
		//
		// Unexecuted code is where bugs live, and this project has already paid for that once --
		// `~TaskMPSCQueue` used `::delete stub_`, handing a slab slot to the CRT heap, and it
		// survived for the life of the project purely because nothing destroyed one.
		//
		// So the destructors get exercised HERE, in a test, instead of at exit. Production keeps the
		// safe path (drain, then leak); the destructors stay executed somewhere, so they cannot rot
		// back into dead code; and an ordering bug in them surfaces in a test run rather than in
		// somebody's shutdown.
		//
		// Returns after the delete. If it crashes, that IS the finding.
		void DestroyForTesting();
	}

	class TaskScheduler {
		friend void detail::TeardownForTesting(TaskScheduler&);
		friend void detail::DestroyForTesting();
		friend class Thread;
		// Registers and unlinks itself in the chain below. Only these two pointers are touched.
		friend class WaitPrimitive;
		friend class GlobalFiberPool;


	public:
		// Priority inheritance methods (public for SchedulerMutex access)
		Task* GetCurrentTask() const;

		// The execution mode of the task running on this thread. Static and straight to the
		// thread-local -- no Instance(), which throws when uninitialised and would put a branch plus
		// a global load in front of every caller. Reads currentRunningTask, not currentFiber,
		// because a coroutine HAS no fiber and that is the case worth distinguishing.
		//
		// Returns TaskType::Native when no task is running -- a bare thread. Correct rather than a
		// fallback: a bare thread does not change stack mid-section either.
		//
		// WHAT IT IS FOR: choosing a reclamation scheme. That choice is PERFORMANCE, not safety --
		// epochs are safe for all three modes, which is what counted epochs were built to give
		// coroutines. The rule:
		//
		//     If coroutines will use the structure, hazards are likely faster. Otherwise epochs.
		//     ONE SCHEME PER STRUCTURE -- do not mix.
		//
		// Coroutines are the deciding factor because a coroutine cannot have an epoch SLOT (slots
		// need a stable identity; a frame has none), so it takes the COUNTED path: 1.82 B guards/sec
		// against slots' 2.52 B. Fibers and native tasks get slots and pay no such penalty.
		//
		// DO NOT MIX: neither scheme sees the other's readers, so a hazard scan frees a node an
		// epoch reader is traversing, and epoch advance frees a node a parked hazard reader names.
		// Sound mixing needs a dual-condition retire, which is NOT BUILT.
		//
		// So the intended use is choosing a structure's scheme WHEN YOU WRITE IT, not branching per
		// traversal. See design/NOTES.md.
		static TaskType CurrentTaskType() noexcept;
		void CleanupTaskMetadata(Task* task);

		// Cancellation, observed wherever a task is about to RUN. True means the task was cancelled
		// and has already been disposed of -- released its WaitGroup, destroyed, slot returned --
		// so the caller must not touch it again and must not execute it.
		//
		// THIS EXISTS BECAUSE THE CHECK WAS IN ONE PLACE AND A TASK CAN START IN FOUR. Worker() had
		// it; ProcessMainThread, WaitFor's own drain and TryRunStolenNativeTask did not, so a
		// cancelled task picked up by a HELPER ran its payload -- reproducibly, a handful out of 500
		// in about 7% of runs, because the helper is a race for the same queue the workers are
		// discarding from. Same shape as the 3.2.1 bug where the waiter queues bypassed
		// IsTaskCancelled: "one place cancellation is decided" only holds if every site calls it.
		//
		// ONLY AN UNSTARTED TASK IS DISCARDED, for the reason Worker() documents at length: a queued
		// entry may be a RESUME, and discarding one of those abandons a live stack or coroutine
		// frame instead of cancelling it.
		bool DiscardIfCancelled(Task* task);

		static TaskScheduler& Instance() {
			if (!instance)
				throw std::runtime_error("Call TaskScheduler::Init() before Instance()!");
			return *instance;
		}
		static void Init(size_t poolSize = 0); // 0 = auto-detect

		~TaskScheduler();
		bool PushMain(Task* task);
		void ProcessMainThread();
		// Waits on wg like WaitFor(), but ALSO drains mainQ (ProcessMainThread) each spin.
		// REQUIRED if the WaitGroup covers any TaskDAG main-affinity node (see TaskNode::isMain)
		// -- those tasks only ever run when something calls ProcessMainThread, so a plain
		// WaitFor() would spin forever waiting on a task nothing is servicing. Caller must BE
		// the main thread (only it should call ProcessMainThread).
		void WaitForMain(WaitGroup& wg);
		void NotifyAll();

		// Resume every worker's pinned park fiber -- the broadcast wake, with no kernel object on
		// the path. See the definition for what it does NOT wake: frames parked on primitives.
		void ResumeAll();

		// Print every worker's sleep/queue state to stdout. For a watchdog that has decided the pool
		// is wedged: a stack trace only shows workers sitting in cv.wait, which is already known.
		// What identifies a lost wakeup is a worker with state=SLEEPING and queued=1 -- parked while
		// holding work that only it can drain, because inboxes are not stealable.
		// Unsynchronised by design; see Thread::GetDebugState.
		void DumpPoolState(const char* why) const;
		// Parallel range loop. Decides serial-vs-parallel by MEASURING, not by element count: it runs a
		// small prefix inline, times it, and parallelizes the rest only if the extrapolated work clears
		// ~75us. That constant is the fork-join dispatch overhead and is the only thing that
		// generalizes -- the crossover ELEMENT COUNT moves 400x with per-element cost (see
		// bench.exe's crossover sweep), which is why the old fixed `> 10000` gate simultaneously
		// parallelized cheap loops that then ran 11x slower and serialized expensive ones that would
		// have run 12x faster. `chunkSize` is the grain; it is floored so the range can't be split into
		// more than ~4 chunks per worker no matter how small a value is passed.
		// How workers are bound to logical CPUs. Set BEFORE Init(); changing it afterwards does
		// nothing, since binding happens at thread creation.
		//
		//   Hard  (default) -- SetThreadAffinityMask. A worker runs on its own core or nowhere. Best
		//                      cache locality, and the only mode where you DECIDE where
		//                      oversubscription lands instead of discovering it. This is what shipping
		//                      engines do, because a game largely owns the machine.
		//   Ideal           -- SetThreadIdealProcessor: a strong hint. Keeps locality and keeps the
		//                      topology map meaningful, but Windows may migrate a worker under
		//                      contention from ANOTHER PROCESS, or for thermal/core-parking reasons --
		//                      the cases where hard affinity's worst case bites. A good default if the
		//                      host application does not own the machine.
		//   None            -- placement entirely up to Windows. For embedding inside an application
		//                      that manages thread placement itself.
		//
		// Locality-aware stealing (SMT sibling, then cache cluster) only means anything under Hard or
		// Ideal: if threads migrate freely, "my sibling" stops describing where data is. Pinning and
		// topology-aware stealing are one decision, not two.
		//   PhysicalOnly    -- one worker per PHYSICAL core, pinned to that core's first logical CPU with
//                      every SMT sibling left empty. HALVES the pool on an SMT machine, so it varies
//                      placement AND worker count together -- that is unavoidable, and sizing a pool
//                      to physical cores is a normal configuration in its own right.
        enum class AffinityPolicy : uint8_t { Hard = 0, Ideal, None, PhysicalOnly };
		static void           SetAffinityPolicy(AffinityPolicy p);
		static AffinityPolicy GetAffinityPolicy();

		// IdlePolicy WAS HERE, AND IT IS GONE. Sleep vs NoSleep -- park on the condition variable,
		// or never park at all -- was the pool's only idle control from 1.2 through 5.0. It was
		// removed because the awake floor is strictly better at the job NoSleep was doing, and
		// keeping both meant shipping a public setting whose good configuration was "do not use it".
		//
		// WHAT NoSleep BOUGHT, so nobody re-derives it: it never entered the kernel, so a notify was
		// a store a spinning worker observed rather than a ~5.5us futex round trip (6.7us parked
		// against 1.2us spinning, p50, reserved band). That is real, and it is why the setting
		// existed at all.
		//
		// WHAT IT COST, which is why it loses. An idle spinning pool taxes every OTHER thread in the
		// process, because the cost is core OCCUPANCY rather than cache traffic -- a control pool of
		// pure CpuRelax threads touching no shared memory reproduced almost all of it. Synthetic
		// (31 workers, idle pool, memory-bound main thread): 14.65 ms/frame parked against 15.17 ms
		// spinning, +3.5%. Inside a real 2D game, where a render thread, GPU driver threads and
		// audio are all competing for the cores the spinners hold: 383us against 462us, 23% WORSE.
		// The synthetic figure understated the embedded cost by ~7x.
		//
		// AND THE AWAKE FLOOR IS THE SAME BENEFIT WITHOUT THAT BILL. F workers stay unparked and
		// absorb the wake, so the notify-is-a-store path is still there for the work that needs it,
		// but the count is BOUNDED and it SHEDS when the burst ends -- which is exactly the half
		// NoSleep could not do. NoSleep held every core for the whole run whether or not any work
		// was coming; the floor holds a few, and only while they are earning it. K is the same
		// bargain for the latency lane.
		//
		// A SpinBriefly middle setting was also measured and was worse than BOTH neighbours,
		// monotonically worse as the budget grew (frame DAG, us/graph: Sleep 22.5 | brief@2 23.6 |
		// brief@20 27.3 | brief@100 34.4). The 2-competitive spin-then-block argument assumes
		// spinning is free for everyone else, and at 31 workers it is not. So there is no middle to
		// restore either -- the floor IS the middle, and it is a controller rather than a constant.
		// Runtime kill switch for the bulk steal hint, so its value can be measured against an A/A
		// control inside ONE process. Diagnostic: shipping code should leave it on.
		static inline std::atomic<bool> stealHintOn{ true };
		static void SetStealHint(bool on) noexcept { stealHintOn.store(on, std::memory_order_relaxed); }

		// K-HOT: keep the first K workers from ever parking, while the rest park normally.
		//
		// The bounded version of what IdlePolicy::NoSleep used to do pool-wide (see the note above
		// for why that setting is gone). The spin penalty scales with how many cores spin, so a
		// handful of hot workers is a different trade from spinning all thirty-one of them. A hot
		// worker also costs a PUSHER nothing: it never advertises
		// WS_GOING_TO_SLEEP, so pushes to it take the awake-preference skip instead of a notify.
		//
		// DEFAULT 0 -- off. A spinning core taxes every other thread in the process, so this is
		// opt-in exactly like the I/O layer is; a job-system-only user must not pay for it.
		//
		// WHETHER IT HELPS DEPENDS ON WHERE WORK LANDS. If pushes are spread round-robin, K hot
		// workers only catch K/N of them, and the answer is to STEER work at them rather than to
		// raise K. Measure before choosing a number.
		// ---- MODE::FIBERONLY WAS REMOVED IN 4.0.2 ----------------------------------------------
		//
		// It existed to answer "can the pool be pure-fiber?", and the answer turned out to be that
		// the question was wrong. Everything the mode was built to enable -- fibers pinned to their
		// home worker, notify as a direct resume, the per-worker resumed inbox, WaitGroup direct
		// waiters, and the futex park behind the awake floor -- is now UNCONDITIONAL and shipped for
		// every pool. None of it ever needed a mode; it needed the pinning invariant, and that holds
		// in both.
		//
		// What the mode actually did in the end was REJECT TaskType::Native, and that was a mistake
		// that cost a working ParallelFor: its split leaves are Native by design, because an untaken
		// split is taken back and run inline for ~11 ns and a leaf that cheap cannot pay for a
		// ContextSwitch plus a slab fiber. "Fiber-only" is a statement about which tasks may SUSPEND,
		// not about what every grain costs to start -- and once Native was admitted again the mode
		// was behaviourally identical to Default, an enum with no effect.
		//
		// THE RULE IT WAS TRYING TO EXPRESS SURVIVES, unchanged and unconditional:
		//   Fiber/Coroutine -- may suspend. WaitOnEvent, WaitFor, a mutex that blocks.
		//   Native          -- must not suspend. Enforced where it always was: assignedFiber stays
		//                      null, so a mismarked Native task fails loudly in WaitOnEvent's guard
		//                      rather than corrupting the worker's OS stack.
		//
		// A worker's own park being a fiber and a task needing a fiber were always separate
		// questions; conflating them is what the mode encoded.


		// Sets K, the reserved band [0, K). K ONLY -- it does not touch the never-park flag, so a
		// reserved worker still parks when its lane is empty unless you ask otherwise. Reservation
		// ("do not run bulk work on q0") and spin ("never sleep") are separate purchases; folding
		// them together charged every caller a ~35% ordinary-latency tax for a property most of
		// them never wanted.
		static void   SetHotWorkers(size_t k);

		// K + never-park together, the combination the I/O reactor wants: a completion landing on a
		// PARKED reserved worker pays the OS wake that reserving the core was meant to remove.
		// Measured on a busy pool: p50 5.90 -> 2.00 us, p99 43.00 -> 6.30. That win belongs to the
		// lane, not to everyone who calls SetHotWorkers.
		// THE FROZEN CEILING ON K. Two reserved workers, and the third is not withheld because the
		// mechanism cannot do it -- it is withheld because every reserved thread comes off the floor,
		// which is where throughput lives, and because main plus the timer plus one core per I/O
		// completion thread are ALREADY reserved before the pool is sized. K=2 on an 8-core box
		// leaves three floor workers. Requests above this are CLAMPED, not refused; ask
		// GetHotWorkers() for what you actually got. Revisit only with a measurement that says two
		// was short, and see design/NOTES.md for the K=1 vs K=2 numbers that set it here.
		static constexpr size_t kMaxReservedWorkers = 2;

		static void   SetIoHotLane(size_t k);
		// Re-applies the K clamp once workers exist; called by StartPool. See SetHotWorkers.
		void ClampHotWorkersToPool();
		static size_t GetHotWorkers();

		// ---- THE AWAKE FLOOR: how many workers are never allowed to park ----------------------
		//
		// THE ONE LOAD-BEARING IDEA LEFT FROM K-HOT, with everything incidental stripped off: no
		// lane, no lane queues, no priority tag on Task, no controller. Just a count.
		//
		// WHY IT IS NEEDED, measured rather than assumed. Letting every worker park fixed the idle
		// tax and every bimodal row in the bench -- throughput/mp had been 1.7-2.0x bimodal in every
		// 31-worker run and became 1.14x, and frame DAG went from 7-486 us to 6.4. But it moved the
		// entire cost onto the WAKE: a serial round trip pays one every time (latency 4.4 us), the
		// event/resume hold-off rows went to 9-12 us, and `burst` collapsed from 12.5x to 2.0x
		// because waking sixteen parked workers at once is sixteen OS round trips.
		//
		// A floor of K resolves that without giving the idle tax back. K workers stay spinning, so
		// the common case finds somebody already running and pays nothing; the other N-K stay parked
		// and stop competing for cores. PickNextWorker's awake-preferred placement then has a set to
		// steer at -- it was built for this and has had nothing to aim at, because until now either
		// everyone spun or everyone parked.
		//
		// WORKERS 0..K-1 ARE THE FLOOR, by index. Not a rotating set: a stable one keeps those
		// workers' caches warm and makes the behaviour reproducible run to run, which matters more
		// than fairness for a set this small.
		//
		// DEFAULT 1. Zero is legal and means "the pool may go fully idle" -- correct for a batch
		// process that would rather give the cores back. Anything above ~2 starts paying the idle
		// tax again in proportion, so raise it only against a measurement.
		// ---- DOES THE FLOOR'S RAMP WIDEN THE LANDING ZONE, OR ONLY THE AWAKE SET? -------------
		//
		// false -- ordinary pushes aim at [K, K+base). Workers the growth controller woke earn their
		//          tasks by STEALING. The pre-4.0.2 behaviour.
		// true (default since 4.0.2) -- pushes aim at [K, K+live), so a burst lands wide immediately
		//          and a promoted worker actually receives work instead of only being awake.
		//
		// This is the one unbalanced tradeoff PickNextWorker documents and does not resolve: a narrow
		// landing zone is what makes a single producer fast (spreading measured p50 0.40 -> 0.90 us,
		// 1p 10.0 -> 5.74 M/s) and it is what makes a 256-task burst slow (the marl blocking row runs
		// 10.0 ms with two receivers against 5.1 ms with a wide one -- 70 kernel wakes in 7680 pushes,
		// so it is the landing zone and not the park).
		//
		// SWEEP IT AGAINST THE SERIAL ROW, not just the burst; the narrow steer was bought with real
		// numbers and this gives them back if it is wrong.
		static void   SetPlacementFollowsGrownFloor(bool on) noexcept;
		static bool   GetPlacementFollowsGrownFloor() noexcept;
		static void   SetAwakeFloor(size_t k) noexcept;
		// ---- IDLE-SPIN POLITENESS ------------------------------------------------------------
		//
		// A worker that stays awake with nothing to do yields the core every (mask+1) search passes
		// and CpuRelax()es in between. Default 7 (every 8 passes). LOWER IS MORE POLITE.
		//
		// COUPLED TO THE FLOOR GROWTH CAP. Raising this while the floor can grow wide reproduces the
		// permanent-floor=31 pathology from a default config: thirty never-parking workers spinning
		// rudely starve every other runnable thread on the machine.
		//
		// This is a knob because the right value depends on how many workers are spinning at once,
		// which the library does not get to choose: two floor workers can afford to be rude, and
		// thirty-one cannot. A hard spin across a full pool starves unrelated runnable threads --
		// measured at 10 ms -> 24-40 ms on the marl blocking row, worse than simply parking. marl
		// spins a full millisecond in that same row without the problem because it yields every few
		// microseconds rather than every few hundred.
		//
		// It does NOT decide whether to park. See Thread::Worker.
		// ---- THE FLOOR IS A THIRD BAND (M), NOT A VARIANT OF K --------------------------------
		//
		// GetFloorBase() is where the floor starts. USE IT FOR EVERY FLOOR BOUND; keep using
		// GetHotWorkers() for "is this worker serving the lane right now". The two are the same
		// number while K is static, which is exactly why the distinction has to be explicit before
		// K is allowed to move -- otherwise a K promotion slides the floor band under running
		// workers and every K+M site becomes a race instead of a static off-by-one.
		//
		// ONE FORMULA, READ FROM LIVE ATOMICS, EVERYWHERE. The bands are [0,K) reserved,
		// [K,K+F) floor, [K+F,N) parkable, and K+F <= N is enforced by refusing a grow that would
		// break it (see NoteFloorCrowding). A moving band is made safe by every site spelling it the
		// same way and by the two controllers never moving in the same window -- not by freezing one
		// of them, which is what an earlier fixed-base-at-Kmax attempt did.
		// ---- THE WHOLE BAND LAYOUT IN ONE LOAD ------------------------------------------------
		//
		// USE THIS FOR ANY BAND-MEMBERSHIP DECISION. K and F live in one 64-bit word precisely so a
		// worker can ask "which band am I in" and get an answer that was true at a single instant.
		// Calling GetHotWorkers() and GetAwakeFloor() separately reintroduces the torn pair: the
		// answer can mix a K from before a promotion with an F from after it, and that produced
		// 30+ confirmed floor-worker parks per run the moment K actually started moving.
		//
		// The single getters remain for callers that genuinely want one number (a banner, a test,
		// a controller adjusting its own field). They are not wrong -- they are just not a BAND.
		// EVERY BAND FIELD, FROM ONE LOAD. kmin/kmax are the SCALING RANGE, and they are here rather
		// than behind GetHotWorkerRange() because the worker needs them on the same pass it needs k
		// and f: `kmax > kmin` is the "may the controller move K" predicate, and the observer that
		// feeds that controller has to gate on the identical answer. Asking it from a second load is
		// how the observer ends up off while the controller runs -- reading zeros, shedding K every
		// window. Under static K, SetHotWorkers pins kmin == kmax and the whole question is false.
		struct Bands { size_t k; size_t f; size_t fbase; size_t kmin; size_t kmax; };
		static Bands  GetBands() noexcept;
		static size_t GetFloorBase() noexcept;

		// ---- OBSERVED PARKING, FOR CHECKING A BANNER AGAINST REALITY --------------------------
		//
		// How many times worker q actually blocked. A banner that prints K and F read from the same
		// atomics the scheduler steers by is an ECHO, not a check -- it agrees with itself however
		// the wiring is broken. These let a bench assert the DECLARED bands against what the pool
		// did: nobody in [0,K) or [K,K+F) may appear here, and [K+F,N) should.
		static unsigned GetWorkerParkCount(size_t q) noexcept;
		static void     ResetWorkerParkCounts() noexcept;
		static void   SetSpinYieldMask(unsigned mask) noexcept;
		static unsigned GetSpinYieldMask() noexcept;

		// BELOW THIS FLOOR SIZE, A FLOOR WORKER DOES NOT YIELD AT ALL -- it stays in CpuRelax.
		//
		// yield() is there so a LARGE never-parking set does not pin the machine (thirty spinners
		// starving everything else runnable). A handful of cores in CpuRelax do not do that, and
		// below the threshold the yield costs more than it saves: with F=2 the steer set has two
		// bits, so a yield on either is a coin flip that a push lands on the core that just stepped
		// off. Measured on a latency row: 708 pushes in 20,000 aimed at a WS_YIELD worker, 683
		// re-aimed to the sibling, 25 with both floor cores in a yield window at once. The re-aim
		// cannot invent a third floor worker, so the only fix at F=2 is not to yield.
		//
		// 4 is a judgement, not a measurement. It is above the F=2 and F=3 configurations the
		// latency rows actually use and well below the grown floors the politeness was written for.
		//
		// A DEFAULT, NOT A CONSTANT. This is policy, and policy an application can be wrong about
		// belongs behind a setter with a defensible default rather than baked into a header -- the
		// same shape as SetSpinYieldMask beside it. An app that runs a permanently wide floor may
		// want it lower; one that pins two cores for latency and nothing else may want it higher.
		static constexpr size_t kYieldFloorMinDefault = 4;
		static void   SetYieldFloorMin(size_t f) noexcept;
		static size_t GetYieldFloorMin() noexcept;

		// ---- HOW LONG AN IDLE WORKER PAUSES BEFORE IT LOOKS AGAIN ----------------------------
		//
		// CpuRelax iterations per idle pass. This is the POLITE half of the idle loop: a pause hint
		// keeps the core, costs power rather than a context switch, and lets a hyperthread sibling
		// make progress. Only the yield gives the core away.
		//
		// SHORT BY DEFAULT AND IT SHOULD STAY SHORT. This runs between two consecutive looks at the
		// queues, so it is dispatch latency for anything that arrives during it -- a long relax is
		// a cheap way to make a worker look asleep while it is burning a core. 32 is a pause, not a
		// backoff; anything that wants a real backoff should be parking instead.
		static constexpr unsigned kWorkerRelaxDefault = 32;
		static void     SetWorkerRelax(unsigned iterations) noexcept;
		static unsigned GetWorkerRelax() noexcept;

		static size_t GetAwakeFloor() noexcept;

		// ---- THE CONTROLLER: the floor moves itself between 1 and the pool size ---------------
		//
		// ALWAYS ON. A static floor cannot be right for both a serial round trip and a 16-task
		// burst: measured at floor 1, latency was 0.35 us (12x better than parked) while `burst`
		// ran 1.0x of 16 -- no parallelism at all, because every task queued behind the one awake
		// worker. Floor 2 halved the latency win and only reached 1.4x. There is no constant that
		// serves both, which is what makes this a controller rather than a tuning knob.
		//
		// TWO SIGNALS, BOTH DIRECTLY MEASURED -- no proxy, no occupancy inference:
		//
		//   PROMOTE  a push had to WAKE a parked worker. That is the floor being too small, stated
		//            as the exact cost it causes, counted where it happens (NotifyWorker). One miss
		//            in a 1 ms window is enough: a wake costs microseconds and a spare spinning
		//            worker costs a fraction of a core, so this is deliberately eager.
		//
		//   DEMOTE   the MARGINAL floor worker -- index K-1, the one that would be given up -- ran
		//            zero tasks for three consecutive windows. Asking about the marginal worker
		//            rather than the average is what makes shedding possible at all: an average
		//            over a busy floor never falls, so K would ratchet up and stay.
		//
		// THE RATCHET RULE, carried over from the K-hot controller and the reason it is stated here:
		// anything keyed off "became idle" rather than "IS idle" never sheds. The demote test is
		// re-evaluated from live counters every window, and the low-window counter RESETS on any
		// window where the marginal worker did work.
		//
		// ASYMMETRIC RATES ON PURPOSE. Promotion is cheap to get wrong (one extra spinner) and
		// expensive to delay (every dispatch pays a wake), so it fires on one miss. Demotion is
		// expensive to get wrong (the next dispatch pays a wake) and cheap to delay, so it needs
		// three quiet windows and its own slower clock.
		//
		// A CONTROLLER DECLINING TO ACT IS NOT A DEFECT. Sitting at 1 under a light serial load is
		// the correct answer, not blindness -- one awake worker is all that workload can use.
		static void MaybeAdjustAwakeFloor() noexcept;

		// NoteWakeMiss REMOVED. It was the promote signal for a miss-ratio promotion that no longer
		// exists -- growth is the push path's job (NoteFloorCrowding) -- and after that went, both
		// it and its denominator were incremented on every push and loaded by nothing.

		// ---- DID THE YIELD RE-AIM EVER FIRE? -------------------------------------------------
		//
		// WITHOUT THESE, AN A/B ON THE FOURTH STATE IS UNANSWERABLE. "No difference" and "the
		// mechanism never ran" produce identical output, and this repository has already published
		// one park-primitive A/B that turned out to be measuring a path taken by under 1% of the
		// row. So the mechanism has to report its own rate before any number about it is worth
		// reading.
		//
		//   YieldAimCount()    pushes whose chosen candidate read WS_YIELD -- i.e. how often the
		//                      floor's yield window is actually in the way of a push
		//   YieldReaimCount()  of those, how many found another awake worker in the same bitmap
		//                      word and moved. The difference is the pushes that kept a yielding
		//                      target because there was no alternative -- safe, just late.
		//
		// A zero AIM count means the window is never hit and the whole fourth state is costing a
		// load per push for nothing. A high AIM with a low REAIM means the alternative search is
		// too narrow (one word, one step) rather than the state being useless. Read both.
		static unsigned YieldAimCount() noexcept;
		static unsigned YieldReaimCount() noexcept;
		static void     ResetYieldCounters() noexcept;
		static void     NoteYieldAim(bool reaimed) noexcept;

		// ---- LANE OVERFLOW: KEEP lane WORK REACHABLE WHEN ITS OWNER CANNOT REACH IT ----------
		//
		// Returns the worker a lane task should ACTUALLY go to, given the one placement picked.
		// Normally that is `chosen` unchanged; it differs only when `chosen` already has a lane
		// task queued AND is inside a task body, which is the one state in which the queued task
		// is reachable by nobody.
		//
		// WHY THE PRODUCER DECIDES. The lane is an MPSC with exactly one legal consumer, so the
		// only thread that can drain it is its owner -- and an owner busy inside a task is not in
		// Worker(), so it reaches neither the lane pop nor any spill of its own. Every consumer-side
		// fix has that same hole; the push is the last point at which a thread that is definitely
		// running gets to decide. Same reason TryTakeLaneTask exists for the BLOCKED owner, which is
		// the other half of this and does not cover the busy one.
		//
		// NOT A DEQUE, AND THAT IS THE POINT. The lane was deliberately reduced to an inbox because
		// staging latency work into a bulk structure is an O(depth) unload -- exactly the cost the
		// lane exists to avoid, measured at roughly half the io p99. This adds no structure: the
		// overflow goes to another worker's lane INBOX, which every worker drains before its own
		// deque, so it keeps lane priority instead of being demoted into bulk work.
		static size_t HiPriSpillTarget(size_t chosen) noexcept;

		// How many lane pushes were redirected by HiPriSpillTarget. Zero on a healthy lane; a
		// rising count means completions are arriving faster than the reserved band retires them,
		// which is the K controller's promote case, not an error.
		static unsigned long long GetHiPriSpillCount() noexcept;

		// ---- IS A SHARED MPMC LANE WORTH BUILDING? --------------------------------------------
		//
		// GetLaneStrandCount()         -- dispatches that left a non-empty lane inbox behind, i.e.
		//                                 a backlog that just became unreachable to everyone.
		// GetLaneStrandIdlePeerCount() -- of those, how many had ANOTHER reserved worker not in a
		//                                 body at that instant.
		//
		// THE SECOND NUMBER IS THE ENTIRE CASE FOR A PULL QUEUE. An MPMC removes misallocation, not
		// queueing: it adds no service capacity, so when every reserved worker is inside a body it
		// waits exactly as long as an inbox does. Its win is confined to backlogs that had somebody
		// idle to go to -- and this counts those directly instead of inferring them from a latency
		// gap that dispatch cost also lives in.
		//
		// Read as a ratio. Near 0 means the lane saturates cleanly and a shared queue buys nothing
		// but contention. Near 1 means work is sitting behind busy owners while peers idle, which
		// is the one thing the producer-side spill structurally cannot fix -- it decides at PUSH
		// time and the owner can enter a body immediately afterwards.
		//
		// Deliberately an UPPER bound: a sleeping peer counts as idle (see the definition).
		// TWO PEER SETS. GetLaneStrandIdlePeerCount() scans [0, K) -- what the current lane can
		// reach, and the ceiling on any producer-side fix. GetLaneStrandIdleWideCount() scans
		// [0, K+F) -- reserved plus the awake floor, which is the consumer set a SHARED pull lane
		// would actually have. The gap between them is what changing the structure would buy that
		// improving the spill never could.
		//
		// Neither includes the parkable band, and that exclusion is what keeps the wide number
		// meaningful: a scan over the whole pool reports ~100% forever, because a parked worker is
		// never busy -- and waking one costs the ~3 us that the lane exists to avoid.
		static void NoteLaneStrand(size_t ownerIndex) noexcept;
		static unsigned long long GetLaneStrandCount() noexcept;
		static unsigned long long GetLaneStrandIdlePeerCount() noexcept;
		static unsigned long long GetLaneStrandIdleWideCount() noexcept;

		// NoteHiPriStaged / GetHiPriStagedCount WERE HERE and are gone with the lane deque. They
		// counted lane tasks a worker unloaded into lane[qIndex] at dispatch. With no deque there
		// is no unload, so the counter had no writer -- and a diagnostic that can only ever report
		// zero is worse than an absent one, because a reader takes the zero for a measurement.
		// Lane reachability is now one number, the spill above.

		// Lane-deque probes and successful steals. The pair localises a rescue failure to one of
		// three places without a debugger: no probes means thieves never reached the lane (victim
		// selection or the hint), probes without hits means they reached it and could not take
		// (steal_if losing, or classOK declining), hits without progress means something after the
		// steal is losing the task.
		static void NoteLaneProbe(bool hit) noexcept;
		static unsigned long long GetLaneProbeCount() noexcept;
		static unsigned long long GetLaneStealCount() noexcept;

		// Wake one sleeping worker to come and steal freshly staged lane work. `excludeQ` is the
		// worker that just staged it, which is about to enter a task body and is the one thread
		// that certainly cannot help.
		//
		// THE OLD LANE WAKE WAS REMOVED FOR A REASON THAT NO LONGER HOLDS. UpdateLaneHint records
		// it: "with the lane deque gone there is nothing for a woken worker to steal" -- waking a
		// worker to look at an MPSC it may not touch is pure cost, and the measurements that killed
		// it were taken in exactly that world. With the deque back the woken worker has something
		// it is allowed to take, which is the entire difference.
		//
		// AND WITHOUT IT THE STAGING IS INERT. advertisedCount is read by workers that are AWAKE
		// and deciding whether to park; it says nothing to one already asleep. A worker stages its
		// remainder precisely because it is about to disappear, which is usually when the rest of
		// the pool has just gone quiet -- so the likeliest state is that every potential thief is
		// parked and no advertisement reaches any of them. Measured that way: 8 staged, 0 stolen.
		void NotifyLaneHelper(size_t excludeQ) noexcept;

		// GROW THE FLOOR FROM THE PUSH PATH. `submitted` is how many tasks this submit is adding --
		// 1 for a Push, N for a PushBatch, so a batch may promote in one step and a single push may
		// not.
		//
		// WHY THE PUSH PATH AND NOT TASK COMPLETION. The completion-driven controller CANNOT grow
		// the floor during the exact workload that needs it. Measured: 16 tasks of 3.28 ms each,
		// pushed at an idle pool with F=2. Both floor workers are inside a task body for
		// milliseconds so they reach no completion, and the other 29 are blocked in the kernel
		// running no loop at all -- so nothing executes the controller, the floor stays at 2, and
		// 16 tasks serialise into 8 waves. 36.20 ms, 1.5x of 16.
		//
		// The pusher is the only party awake during a burst, so it is the only one that can see the
		// queue building. That is not a tuning problem -- an earlier attempt moved the completion
		// subsample from 1-in-64 to 1-in-8 for this exact row, which is the right diagnosis aimed
		// one level too shallow: a burst is precisely the workload with no completions until it is
		// already over.
		static void NoteFloorCrowding(size_t submitted) noexcept;

		// Master switch for the floor GROWTH controller -- push-side spill, completion-side growth and
		// the redistribute that follows it. Off pins the floor at its base. Exists so one binary can
		// A/B the controller on the machine that holds the baseline, instead of across two builds.
		// Demand cap for the lazy splitter: how many unclaimed splits a range tolerates on its own lane
		// before it stops publishing and runs the rest inline. 0 disables the cap (split unconditionally,
		// the pre-4.0.2 behaviour). See RunLazyRange for why the lane depth is the demand signal.
		// How many leaves per worker the splitter mints before it raises the grain itself. The splitter
		// costs one Task per leaf where the cursor costs one atomic, so this is the dispatch-cost knob.
		// Iterations per worker below which ParallelFor runs the range SERIALLY. N-only, no body probe.
		// Higher protects cheap bodies at small N; lower protects expensive ones. They are the same N,
		// so no value serves both -- see the gate in ParallelFor.
		static void   SetMinItersPerWorker(size_t n) noexcept;
		static size_t GetMinItersPerWorker() noexcept;

		static void   SetLeavesPerWorker(size_t n) noexcept;
		static size_t GetLeavesPerWorker() noexcept;

		static void   SetLazySplitCap(size_t n) noexcept;
		static size_t GetLazySplitCap() noexcept;

		// ---- RANGE RECRUITMENT: a completed leaf is EVIDENCE, not a prediction ------------------
		//
		// THE PROBLEM IT SOLVES. A published range wakes exactly ONE worker, on the parallel hint's
		// 0->1 edge, and everyone else has to discover the work by happening to steal. Measured, same
		// binary, floor as the only variable: heavy N=2000 runs 7.05x at the default floor and 22.07x
		// at floor=31, where all 31 are already awake and looking. That 3.1x is pure ramp -- the
		// range is limited by how many free workers SEE the work, not by the work.
		//
		// WHY NOT JUST SIZE THE RAMP. Because that needs the total work W at range entry, and the
		// range does not know W -- measuring a leaf to extrapolate is a guess that is wrong exactly
		// when the body is non-uniform. Any scheme phrased as "grow by f(W)" is playing oracle.
		//
		// SO RECRUIT ON EVIDENCE INSTEAD. A worker that has just RUN a leaf knows two facts, both
		// observed and neither predicted: a range is still live (its hint bit is set), and a leaf of
		// it cost `bodyNs`. If that exceeded the price of a wake, waking more workers pays. The
		// worker that measured it does the recruiting.
		//
		// THIS IS SELF-LIMITING IN BOTH DIRECTIONS, which is the property a timer-based spin cannot
		// have. Cheap bodies never recruit -- a trivial leaf costs nanoseconds, never clears the
		// gate, and the range stays narrow without any iteration-count heuristic deciding for it.
		// And recruitment stops when the range drains, because ClearParallelHintIfEmpty clears the
		// bit and there is nothing left to recruit for. No work, no evidence, no wake.
		//
		// HOW MANY, and this is why it is not one-per-leaf. A leaf costing bodyNs buys bodyNs/c
		// wakes in the time it took to run -- so waking one is absurdly conservative for a 200 us
		// leaf against a 3 us wake, and would make full width take log2(P) LEAF DURATIONS. Waking
		// bodyNs/c reaches full width after roughly one leaf when leaves are expensive, and never
		// when they are cheap. Same ratio as the crossover, from the same measured c.
		static void   SetRangeRecruit(bool on) noexcept;
		static bool   RangeRecruitEnabled() noexcept;

		// ---- MEASURED FAN-OUT WIDTH (opt-in) ---------------------------------------------------
		//
		// Runs the first kMinGrain items of a range on the calling thread, times them, and uses the
		// result for BOTH decisions ParallelFor has to make: whether to fan out, and HOW WIDE.
		// Width is sqrt(W/c) -- the k that minimises W/k + k*c, where c is the wake cost.
		//
		// WHAT IT REPLACES IS A MISSING MIDDLE, not a mis-tuned constant. Fan-out has two states
		// today, serial or workers.size() wide, chosen by an iteration count that never looks at
		// the body -- so trivial work at N=256 recruits 23 of 31 workers for ~2 us of work, while
		// heavy work at the same N is refused outright (measured: 0.02x and 6.9x respectively, cap
		// off). No value of SetMinItersPerWorker serves both, because the difference is not in N.
		//
		// The probe is not a cost: the first chunk is work the range must do regardless, so this is
		// two clock reads. That is what makes it different from the body probe removed in 1.4.
		//
		// It is a LOWER BOUND by design -- a back-loaded body makes the first chunk unrepresentative
		// and W too small. Range recruitment then widens on evidence as expensive leaves complete,
		// so the probe picks a defensible start and recruitment corrects upward.
		//
		// ON BY DEFAULT since 2026-08-31. Two consequences worth knowing before you turn it off:
		//
		//   ParallelFor now runs a few items ON THE CALLER before deciding anything, so a range
		//   that ends up serial has paid a small probe (~0.06 us at N<=512) it did not before. That
		//   buys not refusing heavy ranges the old gate declined -- 1.00x to 6-15x at N<=1000.
		//
		//   SetMinItersPerWorker no longer gates anything unless this is OFF. It remains the gate
		//   for the disabled path and is otherwise inert.
		//
		// SetMeasuredWidth(false) restores the previous behaviour exactly.
		static void   SetMeasuredWidth(bool on) noexcept;
		static bool   GetMeasuredWidth() noexcept;

		// Remember what each call site's body cost, so the NEXT range of it starts at the right
		// width with no probe at all -- the probe is serial time on the critical path, and paying
		// it again for a body already measured is the cold-start cost repeated forever.
		//
		// Keyed by the callable's type (every lambda has a distinct one), NOT globally: a single
		// average across call sites mixes a 0.5 ns/element body with a 600 ns/element one and
		// describes neither. Collisions are harmless -- a wrong estimate costs a wrong initial
		// width, and width is a lower bound that recruitment corrects upward.
		//
		// Re-probes 1 call in 64 so a body whose cost drifts is not remembered wrong forever.
		//
		// OFF BY DEFAULT, ON A MEASURED FAILURE. target_type() assumes one callable type per call
		// site, and a dispatcher that forwards every body through ONE wrapper lambda -- an ordinary
		// pattern, and what the scheduler's own grain sweep does -- collapses every body to a single
		// key. Measured: trivial at N=256 went 0.33x -> 0.01x with memory on, because the averaged
		// "body" looked expensive enough to fan out work costing nanoseconds. Needs a caller-supplied
		// key to be correct; enable only if your call sites have genuinely distinct callable types.
		static void   SetRememberedCost(bool on) noexcept;
		static bool   GetRememberedCost() noexcept;

		// The price of one kernel wake, in nanoseconds -- the `c` above. MEASURED, not guessed:
		// event/resume with a 1 ms hold-off costs 8.0 us against 5.0 us with none, and that ~3 us
		// delta is the OS wake and nothing else. It is a property of the MACHINE, so it is a knob
		// rather than a constant; a box with a cheaper wake should recruit sooner and parallelize
		// smaller ranges, and both fall out of this one number.
		// ---- INSTRUMENT: force every notify, making the WS_AWAKE skip unreachable --------------
		//
		// NotifyWorker skips the wake when it reads the target as WS_AWAKE. That skip is the
		// StoreLoad half of the wake protocol and the reason a push to a running worker costs no
		// syscall. Its safety rests on an ORDERING ARGUMENT -- the producer stores the task before
		// loading the state, so the worker's CAS to GOING_TO_SLEEP and its seq_cst re-check both
		// follow that load in the single total order, and it cannot park on work already pushed.
		//
		// This flag tests that argument instead of restating it. Force every notify: if the tail
		// is unchanged, the skip is not implicated and the stalls live elsewhere. If it vanishes,
		// the ordering argument is wrong somewhere and this is a correctness bug rather than a
		// tuning question.
		//
		// NOT A CANDIDATE DEFAULT. Every push would pay a WakeByAddress syscall, including the
		// overwhelming majority aimed at workers that are awake and need nothing -- exactly the
		// cost the skip exists to remove.
		// SetAlwaysNotify/GetAlwaysNotify were HERE and are gone with the permit machine. They
		// forced every notify so the WS_AWAKE skip could be A/B'd; the skip no longer exists,
		// because Wake() now decides from the swap's previous value instead of a separate load.
		// The A/B answered its question first -- forcing every notify changed nothing, 10 stalls
		// per 120,000 either way -- which is what pointed at the state machine rather than the skip.

		static void   SetWakeCostNs(unsigned ns) noexcept;
		static unsigned GetWakeCostNs() noexcept;

		// How long a GROWN floor is held before it may shed back to base, in milliseconds. The
		// timer behind "release the herd for a while": recruitment wakes workers, this decides how
		// long they stay. Refreshed by every growth, so continued work extends it and silence ends
		// it -- there is no fixed expiry to outlive the workload.
		//
		// Default 6 ms, the historical constant. Size it slightly longer than the gap you want to
		// hold through: ~20 ms for a 60 Hz frame loop, seconds for a batch job. Do NOT set it far
		// beyond your work's cadence -- a hold longer than the interval between waves is refreshed
		// before it can expire, the floor never sheds, and the pool is permanently NoSleep.
		static void   SetFloorHoldMs(unsigned ms) noexcept;
		static unsigned GetFloorHoldMs() noexcept;

		static void SetFloorGrowthEnabled(bool on) noexcept;
		static bool GetFloorGrowthEnabled() noexcept;

		// ---- HOW WIDE MAY A BURST TAKE THE FLOOR? --------------------------------------------
		//
		// 0 = UNLIMITED, and that is the default and the shipped behaviour: growth may take the
		// live floor to the whole pool and CollapseAwakeFloorToBase returns it to base when the
		// wave drains. A fork-join row is routinely observed at `peak 30` on a 31-worker pool and
		// back to 2 after. That IS "burst to a temporary NoSleep and shed the same way" -- it
		// already works, and this knob does not add it.
		//
		// WHAT THIS ADDS IS A CEILING, which is the part an application could not express. Growth
		// is transient, but transient is not free: every promoted worker is a core that stops
		// yielding to whatever else the machine is doing, and an app that shares the box with an
		// audio thread, a render thread or another process may want the burst bounded well below
		// the pool. Setting it to 4 says "grow up to four extra cores, then queue instead".
		//
		// THE OLD OBJECTION TO A WIDE FLOOR NO LONGER HOLDS, and it is worth writing down because
		// the note beside SetSpinYieldMask still records it: lifting the cap made the marl blocking
		// row WORSE (15.5/17.6/13.5 against 10.3/10.5/9.9) because thirty never-parking workers
		// with a RUDE spin starved everything else runnable. That was the spin, not the width --
		// "the honest fix is the spin, not the cap" -- and the spin has since been fixed twice
		// over: the yield interval went 1024 -> 8 idle passes, and the yield is now phase-staggered
		// by qIndex so a wide floor cannot step off the core in lockstep.
		//
		// A CEILING, NOT A BASE. SetAwakeFloor(N) is a PERMANENT floor and is a different and much
		// more expensive thing: `floor=31` as a base measures a 3.3-17 us round trip against 0.6 at
		// base=2, because 31 never-parking workers starve the submitter. The danger was never a
		// floor that grows -- it is a floor that does not SHED.
		//
		// THE ARGUMENT IS THE MAXIMUM LIVE FLOOR WIDTH, NOT A NUMBER OF EXTRA WORKERS. It is
		// compared against live F directly, so with a base of 2, SetAwakeFloorMax(2) forbids
		// growth entirely (F is already at the ceiling) and SetAwakeFloorMax(6) allows four more.
		// A cap at or below the base is therefore equivalent to SetFloorGrowthEnabled(false), and
		// `growcap=2 floor=2` duly measured `peak 2` on a fork-join row that reaches `peak 30`
		// uncapped. Naming it maxExtraWorkers, as the first draft did, would have had every caller
		// off by the base.
		//
		// Clamped to the structural limit, so a value larger than the pool gets the pool rather
		// than wrapping. Settable before Init and honoured live; lowering it does not retroactively
		// shed a floor that is already wider, it only refuses further growth -- the collapse still
		// returns the floor to base when the wave drains.
		// 0 = USE THE DEFAULT POLICY: Fmax = max(n - 2, Fbase), then clamped to n - K - 2.
		//
		// AN ABSOLUTE 16 WAS PART OF THAT AND WAS REMOVED 2026-09-01 after a sweep. It was pinning a
		// 31-worker pool at 16 participants -- fifteen workers that never ran a task -- and buying
		// nothing: at a ceiling of 29 the blocking row went 9.50 -> 6.27 ms while round-trip and
		// idle tax did not move (0.533 -> 0.577 us, 0.5% -> -0.0%). What bounds a grown floor is the
		// COLLAPSE back to base, not a constant; the constant was bounding the burst instead.
		// See the ceiling note in NoteFloorCrowding for the table.
		//
		// THE FLOOR BASE SCALES TOO, and is set at pool start unless the app stated one:
		// Fbase = n <= 8 ? 1 : 2. On an 8-thread box a base of 2 is a QUARTER of the machine held
		// permanently off the park path, often two SMT siblings; on a 4-core it is half. Wide --
		// wake the crowd once, everyone parks after -- is the cheaper way for a small pool to use
		// everyone. The F=1 tax is a one-bit steer set: a long body on q0 sends the next aimed push
		// off the floor to buy a wake. Accepted at 8; the fix if it hurts is a placement fallback
		// when the single floor worker is busy, NOT a base of 2.
		//
		// QUOTE THIS FROM THE POOL SIZE YOU SHIP. A small pool makes a ceiling look like a
		// regression by construction -- an n/2 rule gave 4 on eight workers and burst/dflt fell to
		// 2.3x, while on 31 it gave 15, above an observed peak of 11-13, and never bound at all.
		// Use a small pool to catch index bugs, never to tune this.
		static void   SetAwakeFloorMax(size_t maxLiveFloorWidth) noexcept;
		static size_t GetAwakeFloorMax() noexcept;

		// The floor the process ASKED for, as opposed to GetAwakeFloor() which is what it is right
		// now -- growth may hold it above the base for the length of a wave.
		// Workers [0, R) take lane work only; ordinary placement skips them. R must be <= the awake
		// floor, or a reserved worker parks and a completion pays the wake this exists to avoid.
		// Does a reserved worker refuse to park? Default FALSE: reservation already guarantees it is
		// not stuck inside a compute leaf, which is the property I/O needs. Never sleeping is a
		// stronger promise that costs a spinning core, so it is opt-in and should be turned on only if
		// the wake on the resume path is measured to matter.
		// ---- WHICH PRIMITIVE A WORKER PARKS ON ------------------------------------------------
		//
		// WaitAddress -- WaitOnAddress (Windows) / FUTEX_WAIT (Linux). Default on both.
		//                On Darwin it is NOT A PARK AT ALL: there is no address wait, so the idle
		//                path spins. Selectable there only as a negative control.
		// CondVar     -- a per-worker std::condition_variable. DEFAULT AND ONLY PARK ON DARWIN,
		//                where __ulock_wait is private API. Selectable elsewhere for A/B.
		//
		// MEASURED, 2026-08-29, and the isolated bench got it wrong. bench/futex_variance.cpp says
		// condvar has the tighter wake variance (stddev 0.71x, twice) -- that is a two-thread
		// ping-pong with no push path and IT DOES NOT TRANSFER. In the scheduler, on the only row
		// where the park is measurable (floor=0, ~19,400 wakes / 20,000 round-trips), WaitAddress
		// won mean and p50 with no overlap across 7 interleaved reps per arm, and the p99 advantage
		// condvar was supposed to have did not appear at all.
		//
		// The cost is mostly on the WAITER, not the notify mutex: a condvar wake must reacquire the
		// mutex inside the wait before it can re-check its predicate, so the woken thread takes a
		// lock before it can run. WaitOnAddress returns straight to the work loop.
		//
		// THAT IS A WINDOWS RESULT AND IT DOES NOT DEMOTE CondVar ON DARWIN, where the alternative
		// is not FUTEX_WAIT but a busy spin. Do not "fix" the Darwin default by citing it.
		//
		// Re-run bench park=cv|park=wait with floor=0 before reopening any of this; a default-config
		// row cannot see the park at all (<1% of every row) and will report a tie no matter what.
		enum class ParkPrimitive : uint8_t { WaitAddress = 0, CondVar = 1 };
		static void          SetParkPrimitive(ParkPrimitive p) noexcept;
		static ParkPrimitive GetParkPrimitive() noexcept;

		static bool ReservedNeverParks() noexcept;
		static void SetReservedNeverParks(bool on) noexcept;

		static size_t ReservedHiPri() noexcept;
		static void   SetReservedHiPri(size_t r) noexcept;

		static size_t GetAwakeFloorBase() noexcept;

		// Shed a grown floor back to the base in one step. Called from the idle path; returns true
		// if it actually shed. See the definition for why one step and not a ramp.
		static bool CollapseAwakeFloorToBase() noexcept;
		// TEMP DIAG -- remove with the counters in the .cpp
		static void GetFloorCollapseStats(unsigned long long&, unsigned long long&, unsigned long long&, unsigned long long&, unsigned long long&) noexcept;

		// High-water mark of the floor since the last reset. The floor sheds the instant a wave
		// drains, so anything that measures a wave and THEN reads GetAwakeFloor() reads the base --
		// it has already collapsed. Reset before the region of interest, read after.
		static size_t GetAwakeFloorPeak() noexcept;
		static void   ResetAwakeFloorPeak() noexcept;

		// Drop a grown floor back to the base UNCONDITIONALLY -- no hold, no emptiness check.
		//
		// FOR MEASUREMENT HARNESSES, NOT FOR THE SCHEDULER'S OWN USE. A benchmark that runs a flood
		// row and then a latency row will otherwise open the second one at whatever floor the first
		// left behind, and every number in it is then a measurement of that floor rather than of
		// the configured one -- a latency row opened at 16 reported p99 1.70 us against 0.60, which
		// reads as a latency regression and is really leftover spinners.
		//
		// Do NOT call this to shed a live wave: it does not check whether anything is still queued.
		// The scheduler's own shedding is CollapseAwakeFloorToBase, which does.
		static void ForceAwakeFloorToBase() noexcept;

		// Hand `count` tasks from worker `ownerIdx`'s OWN deque to overflow workers' inboxes.
		//
		// CALLED ONLY BY THE OWNER, from its own completion path. That restriction is what makes it
		// legal at all: a Chase-Lev deque has a single-producer bottom, so nobody else may write
		// this worker's ring -- but the owner popping its own bottom and pushing into somebody
		// else's MPSC inbox uses only operations each side is allowed to perform.
		//
		// This is how a wave spreads without putting anything on the push path. See the call site
		// for why the completion is the only place that knows a spread is warranted.
		// Wake ONE parked worker to come and steal. Called by the splitter on the hint.s 0 -> 1 edge --
		// once per range, never per split. See the call site.
		void WakeOneForSteal() noexcept;

		// Wake up to `n` sleepers for a live range. Deliberately NOT spaced like WakeOneForSteal --
		// see the definition for why throttling would discard the evidence recruitment runs on.
		void WakeForSteal(size_t n) noexcept;

		void RedistributeToOverflow(size_t ownerIdx, size_t count);

		// NotePush REMOVED with NoteWakeMiss -- it was the denominator of the same retired ratio.

		// Counts actual kernel wakes (WakeByAddress). The diagnostic for "is the floor receiving the
		// work, or is placement still routing to sleepers?" -- see NoteWakeCall.
		static void   NoteWakeCall() noexcept;
		static void   NoteFloorPark() noexcept;            // TEMP DIAG
		static unsigned long long GetFloorParkCount() noexcept;  // TEMP DIAG
		// TEMP DIAG: how many steal-hint bits are set, and how many of those point at an EMPTY
		// queue. A stale bit pins advertisedCount above zero, which kills the collapse call site
		// AND the park gate pool-wide. REMOVE with the rest of the shed instrumentation.
		static void   GetStaleHintReport(unsigned& advertised, unsigned& stale) noexcept;
		static unsigned long long GetWakeCount() noexcept;
		static void   ResetWakeCount() noexcept;

		// ---- DID THE WAITER RUN? (bare, non-fiber WaitFor only) -------------------------------
		//
		// Describe the calling thread's MOST RECENT bare wait -- the spin loop a non-worker takes
		// through WaitFor. thread_local and reset at the start of each such wait, so they are read
		// after WaitFor returns and mean "that wait", not "all waits since startup".
		//
		// WHAT THEY DECIDE. A long wait has two causes that no timing can tell apart: the waiter
		// spun and kept seeing the count above zero (the completion was genuinely outstanding), or
		// the waiter was not scheduled at all (nothing was late; it was not there to look). polls
		// separates them -- high means it looked and kept finding work outstanding, near-zero means
		// it never got the chance. See the comment on BareWaitBackoff.
		//
		// helped is the disclaimer on the other two: a bare waiter that runs a stolen task WAS the
		// pool for that stretch, so a round trip with helped > 0 is not a dispatch measurement.
		//
		// Zero for a fiber wait, which suspends instead of spinning and never enters this loop.
		static unsigned LastBareWaitPolls()  noexcept;
		static unsigned LastBareWaitYields() noexcept;
		static unsigned LastBareWaitHelped() noexcept;

		// Moves K WITHOUT touching the bounds. SetHotWorkers pins [k,k]; this is the move on its own,
		// used by the range clamp and by the controller -- which must not rewrite the bounds it is
		// being steered by. Public only so the controller can reach it; apps want SetHotWorkers.
		static void SetHotWorkersEffective(size_t k);

		// ============================== DYNAMIC K (opt-in, off by default) ==========================
		//
		// STATIC IS THE DEFAULT AND THAT IS NOT CAUTION. An app with a handful of I/O tasks wants
		// deterministic core assignment, and a K that moves underneath it also perturbs the
		// reactor's ExcludeCurrentThreadFromHotCpus placement and anything the app pinned itself.
		// Predictability is the feature; set the policy once, before Init, and forget it.
		//
		// WHAT THE APP DECIDES IS THE CEILING, NOT THE SET POINT. That distinction is the whole
		// design. The scheduler cannot know whether a core is better spent on the lane or on frame
		// work -- but it does not need to, because maxK already encodes how much diversion the app
		// will tolerate. Inside that bound, saturation is a fact about the lane alone and the
		// scheduler can read it directly.
		//
		// THE SIGNAL: every hot worker advertising a backlog at once, i.e.
		//     popcount(stealHintLane & lowKbits) == K
		// which means each of them is holding at least kLaneStealDepth. That is the one condition a
		// larger K actually fixes -- capacity. It is deliberately NOT "the lane is busy": a busy lane
		// that is keeping up needs nothing.
		//
		// THE MASK IS LOAD-BEARING. Bits above K-1 can linger from a previous, higher K, so a bare
		// popcount would read a stale worker as evidence and ramp on it.
		//
		// ASYMMETRIC ON PURPOSE. Up is fast (kHotUpIntervalNs) because a saturated lane is losing
		// latency every microsecond. Down is slow and requires a SUSTAINED quiet period, because
		// down is the direction that can thrash, and because an idle hot worker costs only spin --
		// the bounded cost K-hot already accepts by design. Cheap to be late, expensive to be wrong.
		//
		// DOWN IS SAFE WHILE WORK IS QUEUED, which is the part that sounds like it should not be.
		// The demotion is applied by the worker itself AFTER an execute pass, when it holds nothing
		// in flight -- only its queues, which the drain logic already sees. So there is no window in
		// which work can be dropped. A demoted worker keeps serving its lane until its inbox and
		// deque are both empty (Thread.cpp's hiPriStray), then parks. And it cannot be held open by
		// new arrivals: steering routes over 1 + (w % hotN) and the completion thread re-reads hotN
		// every flush, so the instant K drops the producer stops aiming there and the queue only
		// drains. THAT ORDERING IS LOAD-BEARING -- the producer's view of K must fall before the
		// worker stops serving. It is automatic today because both read the same atomic; caching K
		// producer-side would break it.
		// THERE IS NO POLICY FLAG, and that is the better design rather than a shortcut: min == max
		// IS static, by construction. SetHotWorkers(k) pins the range to [k, k], the default range
		// is (0, 0) which is exactly the pre-existing K=0 default, and scaling is opt-in purely by
		// WIDENING the range. So "I want exactly K cores and complete control over their lifetime"
		// is expressible, it is still the default, and there is no second knob that can fall out of
		// step with the bounds.
		//
		// minK is forced to >= 1 only when the range can actually move. At K=0 the lane does not
		// exist at all -- lane routes to the ordinary lane and no worker serves it -- so a
		// controller starting there has nothing to observe and could never ramp up. Absorbing under
		// scaling; perfectly fine as a fixed point, which is what (0,0) means.
		//
		// maxK inherits SetHotWorkers' pool clamp, so at least one ORDINARY worker always survives:
		// a pool that is entirely hot has no legal destination for ordinary work and hangs.

		// Evaluated by ONE worker, sampled rather than every pass -- a clock read per pass per
		// worker would cost more than the mechanism saves. Public only so Thread.cpp can call it.
		// THE "IS SCALING ON?" PREDICATE IS max > min, AND IT HAS NO ACCESSOR ON PURPOSE. There was
		// one (HotScalingActive) and it was dead -- hard-wired to false, read by nobody, while the
		// controller gated on max > min directly. Two spellings of one question is how the observer
		// and the controller end up disagreeing about whether to run. Ask GetHotWorkerRange and
		// compare; under static K, min == max (the default, and what SetHotWorkers pins), so none of
		// the adaptive machinery runs: no clock stamps, no per-pass counters, no controller call.

		// Reported by a worker whose inbox drain moved more than one lane task -- i.e. a completion had
		// to queue behind another. Detected at depth ONE, where the saturation edge needs
		// kLaneStealDepth, and computed for free from a count the drain already has.

		// WHO MAY DRAIN A BACKLOGGED LANE.
		//
		//   0  nobody -- the lane is strictly private to its hot worker
		//   1  hot workers only, from a backlogged hot sibling
		//   4  (default) hot workers, plus any ORDINARY worker that is already awake and searching
		//   2, 3  diagnostic arms the dispatch bench interleaves; see Thread.cpp`s steal predicate
		//
		// Mode 4 NEVER WAKES ANYONE. It is purely opportunistic: it spends capacity that happens to
		// be spinning already. That is exactly why it is safe to default on -- under the default Sleep
		// policy an ordinary worker is PARKED when the lane backs up (it has no bulk work keeping it
		// awake), so the arm is inert and measured identical to mode 1 in every row. Under NoSleep it
		// is a capacity valve, and the valve matters most where K is smallest:
		//
		//     NoSleep, p99            off    hot-only   + ordinary
		//       K=1  200us uniform    229       173          53
		//       K=1  200us skewed    1190      1165         554
		//       K=2  200us skewed    1196       806         464
		//       K=4   20us            162       108          92
		//
		// At K=4 the hot set already absorbs the backlog and mode 4 is a wash. It is a safety valve
		// for an under-provisioned lane, NOT a substitute for setting K: under Sleep it cannot help
		// at all, and that is the default.
		// laneHintMode IS GONE, with the mechanism it selected. Mode 4 granted an ORDINARY worker
		// permission to steal a buried hot worker.s lane -- a permission that means nothing now the
		// lane is an MPSC inbox with exactly one legal consumer. Its last reader went with
		// WakeForLane; what remained was a knob three benches set and nothing read, which is the
		// same failure as HotScalingActive: a flag that reads live and gates nothing.

		// THE SAME HINT, READ BY A PRODUCER INSTEAD OF A THIEF.
		//
		// stealHintLane exists so an idle hot worker can find a buried sibling. But the I/O
		// completion thread is choosing WHERE TO PUT work at exactly the moment that bit is set, and
		// it was choosing blind: pushSteered rotates `w = steer++` across the hot set with no idea
		// that worker w is 200us into a handler. Stealing then has to undo a placement the producer
		// could simply not have made -- and stealing is the expensive repair (a probe, a contended
		// CAS against the owner, a lost line) where not-aiming-there is free.
		//
		// One atomic load per flush, of a line the producer already shares with the hot set. The
		// mask, not a per-worker query, so a flush pays for one load rather than K.
		//
		// Bit w == worker w (queue index, so PushBatch affinity w+1) is advertising a lane backlog
		// at or past kLaneStealDepth. Only the first 64 workers can ever be represented; past that
		// the bit is absent and the worker reads as available, which is the safe direction.
		static unsigned long long LaneBacklogMask() noexcept {
			return Instance().stealHintLane.load(std::memory_order_acquire);
		}

		// Runtime arm for the above, so the dispatch bench interleaves skip-on against skip-off
		// INSIDE ONE PROCESS. Three separately-built binaries once moved the K=1 rows -- a
		// configuration where the mechanism provably cannot act -- by 2x, which was machine drift
		// presented as a result. Never compare arms across builds again.
		static inline std::atomic<bool> steerSkipOn{ false };
		static void SetSteerSkip(bool on) noexcept { steerSkipOn.store(on, std::memory_order_relaxed); }
		static bool GetSteerSkip() noexcept { return steerSkipOn.load(std::memory_order_relaxed); }

		// ============================ WAKING SLEEPERS FOR THE LANE ==================================
		//
		// How many PARKED ordinary workers to pull up when a hot worker publishes a lane backlog.
		// 0 disables it, which is the behaviour before this existed.
		//
		// THE GAP THIS CLOSES. laneHintMode 4 lets an ordinary worker drain a backlogged lane, and
		// under the default Sleep policy it is INERT -- an ordinary worker with no bulk work is
		// parked exactly when the lane backs up, so the valve is welded shut in the configuration
		// that ships. Everything mode 4 measured came from NoSleep runs. This is the missing half:
		// the hint is already the one signal in the system that says "lane work is sitting still,"
		// so let it also be a reason to wake somebody.
		//
		// WHY A BARE NOTIFY CANNOT WORK, since that is the obvious version. cv.wait re-evaluates the
		// sleep predicate on every wake, and the predicate knows nothing about the lane: the worker
		// would wake, re-check, still see an empty own-inbox and no hasQueuedWork, and park again
		// WITHOUT EVER REACHING THE STEAL LOOP. The predicate change is the mechanism; the notify is
		// only what delivers it. That is why this costs a fourth flag and a re-proof rather than one
		// line at the publisher.
		//
		// AND WHY IT IS A FLAG AND NOT A READ OF stealHintLane -- see Thread::laneWake. Level
		// triggering would leave a worker that lost the steal race unable to park at all.
		//
		// COST MODEL, which is what decides the default. A wake is ~90us of wall clock before the
		// woken worker runs anything (measured 8-24). So this can only pay where the thing it is
		// racing is LONGER than that -- a buried hot worker with a 200us handler and a queue behind
		// it. Against a 20us handler the wake arrives after the backlog is already gone, and all it
		// bought was a woken core. kLaneStealDepth (4) is the throttle: the hint only sets when a
		// worker is genuinely buried, so this is a far narrower trigger than NoSleep's "never park".
		// laneWakeCount IS GONE. It was WakeForLane.s budget -- how many ordinary workers to pull
		// up for a buried lane -- and WakeForLane no longer exists, so this had no reader at all.

		// THE LOWER EDGE OF THE SCHMITT TRIGGER in UpdateLaneHint: once a worker is advertising a
		// lane backlog, it keeps advertising until its depth drops to THIS, rather than until it
		// drops below kLaneStealDepth.
		//
		// Default 3 == kLaneStealDepth - 1, which is exactly a single threshold and therefore the
		// behaviour before hysteresis existed. Lower it to open the gap. It is a runtime knob and
		// not a constant so the arms interleave INSIDE one process -- comparing them as two builds is
		// how a 1.8x drift on a control arm got read as a result twice in two days.
		//
		// SET IT WITH SetLaneWake, NOT INDEPENDENTLY. The two are coupled, and measurement 8-26 says
		// the wrong pairing is strictly worse than either default:
		//
		//     wake = 0  ->  clear = 3   widening doubles steal probes and buys nothing, because
		//                               there is no woken helper for the wider window to serve
		//     wake > 0  ->  clear = 0   probes rise 1.2-1.5x and p50 falls by up to 3.5x
		//
		// WHY THE GAP ONLY PAYS ALONGSIDE WAKES. The bit is a PERMISSION: an ordinary worker may
		// touch the lane only while LaneStealable holds. A wake takes ~90us to land, and at clear=3
		// the owner has usually drained below kLaneStealDepth by then -- so the helper that was
		// summoned arrives to find itself no longer allowed to help, parks again, and delivered
		// nothing for the price of a wake. The gap keeps the permission alive long enough for the
		// helper to arrive. With no wakes there is nobody arriving, so all the wider window does is
		// keep thieves probing.
		//
		// The recommended pair for a latency-sensitive lane on one hot core:
		//     SetHotWorkers(1); SetLaneWake(2); SetLaneClearDepth(0);
		// DEFAULT 0, NOT 3, SINCE THE LANE BECAME AN INBOX. The signal feeding UpdateLaneHint is
		// PRESENCE (0/1) now -- an MPSC has no size() and the hint only ever answered "is there
		// lane work", never "how much". With a binary input the Schmitt trigger degenerates to
		// plain tracking: set at 1, clear at 0. The old 3 would clear the bit while work existed.
		static inline std::atomic<int> laneClearDepth{ 0 };
		static void SetLaneClearDepth(int d) noexcept { laneClearDepth.store(d, std::memory_order_relaxed); }
		static int  GetLaneClearDepth() noexcept { return laneClearDepth.load(std::memory_order_relaxed); }

		// THE UPPER EDGE. Default kLaneStealDepth (4), i.e. unchanged.
		//
		// WHY IT IS A KNOB NOW. 4 was chosen when set and clear were THE SAME NUMBER, to stop a
		// thief taking the owner's warm task -- the v1 steal-hint failure, 22,000 probes against a
		// 9,164 baseline. But the occupancy witness then measured, at K=4 with stealing on:
		// idle% 73.19 with meanDepth 2.45. A hot worker idle 73% of its idle passes beside a sibling
		// holding ~2.45 tasks -- BELOW the threshold, so never advertised and never stolen. That is
		// measured, unharvested imbalance, and a lower upper edge is what would reach it.
		//
		// The v1 objection has not gone away; it has become testable. With hysteresis, set and clear
		// are independent, so set=2/clear=0 is a shape that could not be expressed when 4 was picked.
		// DEFAULT 1, NOT 4, for the same reason as laneClearDepth above -- against a 0/1 presence
		// signal a threshold of 4 is NEVER reached, which silently killed both consumers of the
		// bit: the K controller.s `adv == mask` promote and the edge-triggered call in
		// UpdateLaneHint. The depth-shaped knobs survive for the bench, but the depths they were
		// tuned against (deque sizes) no longer exist.
		static inline std::atomic<int> laneSetDepth{ 1 };
		static void SetLaneSetDepth(int d) noexcept { laneSetDepth.store(d, std::memory_order_relaxed); }
		static int  GetLaneSetDepth() noexcept { return laneSetDepth.load(std::memory_order_relaxed); }

		// THE TWO PREDICATES THAT DEFINE THE LOW-LATENCY LANE. Everything -- push routing, inbox
		// draining, deque popping, steal probes, the sleep predicate -- asks one of these rather than
		// open-coding the condition. Nine sites read the lane lane; a copy of the rule at each is
		// how the pickup-discard invariant drifted three times, so there are no copies.
		//
		// THE LANE ONLY EXISTS WHEN SOMEONE SERVES IT. At K=0 a lane task routes to the ordinary
		// lane and NOBODY probes lane -- which makes the default pool's worker loop CHEAPER than it
		// was before any of this: one inbox, one deque, one steal probe per victim.
		//
		// lane was never a real priority queue -- per-worker queues plus stealing gave no global
		// ordering, so "picked up first" cost every worker a second inbox, a second deque and a
		// second probe per victim to buy something close to a coin flip. It has a job now, and only
		// where it has one.
		// ---- HIPRI IS A ROUTE, NOT A RESERVED LANE (4.0.2) ------------------------------------
		//
		// This used to be `GetHotWorkers() > 0`, so with K stubbed to 0 every lane push collapsed to
		// loPri and the reactor.s steering became a no-op. Priority now works WITHOUT K: a lane push
		// goes to a worker.s lane INBOX, every worker drains lane before its own deque, and the
		// AWAKE FLOOR is what makes that fast -- the push is steered at a worker that never parks, so
		// an I/O completion lands on a running thread without buying a wake.
		//
		// WHAT THIS DOES NOT GIVE YOU is reservation. K kept ordinary work OFF the hot workers so a
		// completion found an IDLE core; the floor only guarantees an AWAKE one, and placement aims
		// bulk work at those same indices. A completion can therefore queue behind a bulk task on the
		// worker it was steered to -- ordering wins the race, not isolation.
		// ---- THE FLOOR-LANE VARIANT: A LANE WITHOUT A RESERVATION -----------------------------
		//
		// EXPERIMENT, DEFAULT OFF. Routes lane at the awake floor [K, K+F) instead of the reserved
		// band, so completions land on a worker that is guaranteed AWAKE but is also running bulk.
		// It is the configuration behind PickNextWorker's recorded "lane was briefly steered at the
		// unreserved floor, and it lost" -- whose harness no longer existed when the question came
		// back, so the claim could neither be reproduced nor re-run at a different grain.
		//
		// NOTHING ON THE CONSUMER SIDE CHANGES, and that is why this is a routing flag rather than a
		// design. Every worker already pops its own lane inbox before its own deque, reserved or
		// not (see Worker()'s lane block, gated only on `!task_to_run`). The inbox is per worker, it
		// is FIFO, and ordering within it is preserved -- which is the property a strand needs and
		// the reason a shared MPMC cannot serve this.
		//
		// WHAT IT GIVES UP, stated so the measurement is not read as a free win: reservation. A
		// floor worker is awake, which buys the absence of a kernel wake, and it is NOT free, which
		// is a different question -- a completion behind its current body waits for that body. The
		// spill cannot help either: it searches [0, K), and this variant is for K = 0.
		// ---- WHICH BREADTH DOES THE LAZY SPLITTER ASK FOR? ------------------------------------
		//
		// Default false = CorePref::Default, the shipped behaviour. True = Wide, so each split half
		// is placed across the full pool instead of steered at the awake floor.
		//
		// A SWITCH RATHER THAN A DECISION, because the decision is not made. The case for Default is
		// that a lazy split is SPECULATIVE -- halved on the guess a thief takes it, and an untaken
		// split is taken straight back and run inline for ~11 ns -- so paying a kernel wake per
		// split, recursively, is the wrong currency. That is reasoning. The one attempt to measure
		// it read the splitter-vs-cursor table inverting, which turned out to be a property of the
		// measuring machine: the same code reads 1.46-1.76 (cursor ahead) under a throttled process
		// and 1.02-1.07 (tied) on a quiet one.
		//
		// AND IT CANNOT BE SETTLED ACROSS RUNS. The crossover's serial baselines have been observed
		// to move 2x between runs, which makes any before/after comparison of those cells worthless.
		// The bench's `splitpref` row exists to alternate the two settings within one measurement.
		static void SetParallelSplitWide(bool on) noexcept;
		static bool ParallelSplitWide() noexcept;

		static void SetHiPriFloorLane(bool on) noexcept;
		static bool HiPriFloorLane() noexcept;

		static bool HiPriLaneActive() { return GetHotWorkers() > 0 || HiPriFloorLane(); }

		// Does THIS worker serve the lane?
		//
		// K > 0: only the hot ones, so N-K workers halve their search -- half the inbox checks, half
		// the deque checks, half the steal probes. That is the structural win, and it is why
		// ordinary workers must NOT steal lane: letting them means they have to probe it.
		//
		// K == 0: EVERY worker serves it, exactly as before any of this existed. Without that clause
		// nobody drains a lane lane at the default setting, and anything that reaches one -- a task
		// queued before K changed, the shutdown drain, any push site the collapse missed -- strands
		// forever while the pool spins looking for work it refuses to take. Making correctness
		// depend on catching every push site was the fragile half of this design; this makes the
		// safe case the DEFAULT case and leaves the optimisation to the configuration that asked
		// for it.
		// K == 0 RETURNS FALSE FOR EVERYONE, and that is the dead-lane elimination: PickNextWorker
		// routes lane to the hot set only, so at K=0 nothing can enter a lane lane and scanning
		// one is provably wasted work at the DEFAULT setting.
		//
		// ================================================================================================
		// PRIORITY BELONGS TO THE TASK, AND NOTHING MAY OVERRIDE IT ON RESUME.
		//
		// A task's lane is set once, at CreateTask, and defaults to 0. Push, Requeue and PushTarget
		// all route on it; IoReactor splits its completion batch by it precisely so PushBatch taking
		// priority as a PARAMETER cannot quietly drop it. There is no path that promotes a task to
		// the lane behind the caller's back, and there must not be one.
		//
		// THE TEMPTING CHANGE IS "RESUMES SHOULD BE lane" -- a woken fiber or coroutine is latency
		// sensitive, the argument goes, so put it on the fast lane. It is wrong for a reason that has
		// nothing to do with how long the resumed body runs:
		//
		//     THE LANE'S CAPACITY IS K. Anything routed there competes for K workers.
		//
		// K is 0 or 1 by default. Routing every resume there does not make resumes fast -- it
		// SERIALISES a 31-worker pool through one worker. The lane is valuable because it is sparse;
		// fill it with everything and it is just a smaller pool with a longer queue. That argument
		// holds even if every resumed body were provably short.
		//
		// The body length is the SECOND problem, and it is measured: a resumed coroutine is arbitrary
		// code, a running task cannot be preempted, and the skewed soak found that one-in-eight
		// resumes doing 200us of work takes the lane to a 1.1ms p99 with hot workers spending 62-65%
		// of their idle passes beside a buried sibling. Lane stealing drains the queue behind such a
		// task but cannot touch the task itself.
		//
		// So lane stays OPT-IN, per task, defaulting off -- and setting it is a claim about the work
		// ("every resume of this is short, and it is worth one of my K slots"), not a request for
		// speed. Only the caller can make that claim; the scheduler cannot infer it.
		// ================================================================================================
		//
		// It is not the whole safety story, though -- lowering K while lane work is already queued
		// would strand it. The worker sites pair this with a cheap non-empty check on their OWN
		// queues (a local cache line, one load) so anything stranded is still drained. Remote probes
		// get no such fallback: those are the ping-pong, and there is nothing to rescue there.
		// EVERY worker serves lane now. It used to be the K hot workers only, which at K=0 meant
		// nobody -- so a task in a lane inbox was drained by the stray path or not at all. With
		// priority expressed as ORDER rather than as a reserved subset, every worker checks its lane
		// inbox before its own deque and before stealing.
		// WorkerServesHiPri(q) IS GONE. It was `q < GetHotWorkers()` -- a third name for the question
		// the worker already answers as `q < bandsNow.k`, and a second load to answer it with. Ask
		// GetBands() once per pass and compare.

		// HOW HARD the I/O critical path preempts. Applies to the K hot workers AND the reactor's
		// completion threads -- NEVER to the process, and never to the other workers.
		//
		// THE SCOPE IS IN THE CALL, NOT THE ENUM, and that is deliberate. Process-wide elevation
		// measured 5x WORSE alongside K-hot: it raises all N workers, and N spinning threads then
		// preempt the completion thread feeding them. An enum offering a process-wide tier as a peer
		// of a per-thread one would invite exactly that mistake, so it does not offer one.
		//
		//   Normal    leave scheduling alone. The default.
		//   Elevated  no privilege required, and IMPLEMENTED ON EVERY PLATFORM:
		//               Windows  THREAD_PRIORITY_TIME_CRITICAL -- top of the non-realtime range (15)
		//                        inside a normal-priority process. Measured best: HOT p99 4.5-10us
		//                        against a 144-2416us baseline.
		//               macOS    QOS_CLASS_USER_INTERACTIVE. QoS, not affinity -- Apple Silicon has
		//                        no thread affinity at all, and QoS is what steers P vs E there.
		//               Linux    per-thread nice -10, via syscall(SYS_setpriority, PRIO_PROCESS,
		//                        gettid). The RAW SYSCALL is load-bearing: glibc's setpriority
		//                        follows POSIX and applies to the whole PROCESS, which would elevate
		//                        all N workers -- the configuration measured 5x WORSE.
		//   Realtime  reserved. BEHAVES AS Elevated on every platform today.
		//
		// WHY Realtime ESCALATES NOWHERE. On Windows the only step above TIME_CRITICAL is
		// REALTIME_PRIORITY_CLASS: PROCESS-wide, privileged, and able to let a spin loop starve the
		// OS. On Linux it would be SCHED_FIFO, which is NOT the analogue of TIME_CRITICAL -- FIFO
		// sits above nearly everything and will not yield, far closer to the process-wide elevation
		// recorded above as a regression. A FIFO tier is a deliberate future opt-in, default off and
		// off the hot path, not a mapping for Realtime. Refusing to escalate is the honest answer;
		// silently handing the caller a privilege tier they did not ask for is not.
		//
		// ELEVATION CAN BE REFUSED ON POSIX, AND THAT IS NOT AN ERROR. Lowering nice needs
		// CAP_SYS_NICE or a raised RLIMIT_NICE; EPERM is swallowed rather than reported, because the
		// unprivileged answer is still an answer. ANDROID IGNORES IT REGARDLESS -- cgroups arbitrate
		// there -- so never quote an Android measurement as evidence this works. Affinity is not a
		// substitute either: exclusive pinning was measured and is worse than doing nothing (see
		// SetHotWorkerExclusive).
		enum class HotThreadPolicy : uint8_t { Normal = 0, Elevated, Realtime };
		static void            SetHotThreadPolicy(HotThreadPolicy p);
		static HotThreadPolicy GetHotThreadPolicy();

		// OS POWER THROTTLING (EcoQoS) for worker threads. Windows only; see ApplyPowerThrottling
		// in Thread.cpp for why every other platform is a deliberate no-op.
		//
		// SEPARATE FROM HotThreadPolicy BECAUSE IT IS A DIFFERENT KNOB. Priority decides who wins a
		// timeslice contest; EcoQoS decides whether the thread runs at reduced FREQUENCY and gets
		// parked on an efficiency core. A thread can be TIME_CRITICAL and still be throttled. So
		// this applies to EVERY worker, not just hot ones, whatever HotThreadPolicy says.
		//
		// THE DEFAULT FOLLOWS TOPOLOGY RATHER THAN BLANKET-DISABLING, and that is the whole design:
		// this scheduler already knows which workers sit on performance cores and which sit on
		// efficiency cores, and the throttling decision should agree with that placement instead of
		// fighting it.
		//
		//   NEVER ECOQoS A THREAD YOU THEN SET AS IDEAL P-CORE. That combination asks the OS for two
		//   opposite things -- put this work on your fastest core, and also run it slowly -- and
		//   what you get is the P-core sitting at a clamped frequency. The blanket OptOut this
		//   started as would not have done that, but it also throws away information already in
		//   hand: an E-core worker draining background bulk work has no reason to burn turbo budget.
		//
		//   Topology       P-core workers opt OUT of throttling; E-core workers are throttled.
		//                  DEFAULT. On a non-hybrid CPU every core reports as P, so this is
		//                  "nothing is throttled" there -- which is also the right answer.
		//   OptOut         every worker asks for full execution speed, whatever its core.
		//   SystemManaged  stop overriding and let the OS decide -- exactly what a process that
		//                  never called this used to get. NOT the same as OptOut: it clears the
		//                  override rather than requesting full speed.
		//   Force          every worker asks to BE throttled. For a pool that should run cheap -- a
		//                  background asset importer, a nightly bake -- where finishing on E-cores
		//                  is the point.
		//
		// WHY IT MATTERS AT ALL. Windows applies EcoQoS by inheritance and by heuristic: a process
		// launched from a background context starts throttled, and one that loses foreground can be
		// demoted. Measured on the development machine, those two levers together moved dispatch
		// latency by ~173x -- enough to invalidate an entire benchmarking session before the cause
		// was found. A pool the application explicitly created and is actively feeding is not the
		// workload EcoQoS exists for.
		//
		// SET BEFORE Init. Applied once per worker as that worker starts, which is also why it costs
		// nothing at runtime: one syscall at thread entry, on no per-task path, no steal path, and
		// nothing near the deque CAS. Per-task QoS would be a syscall per task and is not offered.
		//
		// LINUX IS DELIBERATELY A NO-OP. The nearest equivalents are SCHED_IDLE and nice, and both
		// mean "background" far more strongly than EcoQoS does -- most game loops should not ask for
		// that, and cgroup cpu.uclamp is an administrative setting a library has no business
		// writing. On Android cgroups arbitrate regardless. DARWIN IS ALSO NOT WIRED HERE: QoS is
		// the lever (USER_INTERACTIVE vs UTILITY) and HotThreadPolicy already sets it for hot
		// workers, but a class-following version is untestable in CI -- flagged rather than guessed
		// at, the same way the AArch64 PAC/BTI gap is.
		//
		// A REQUEST, NOT A GUARANTEE. The OS may still throttle for thermal or battery reasons, and
		// on anything older than Windows 10 1809 the call simply fails. Failure is ignored, the same
		// way HotThreadPolicy ignores EPERM: refusing is the system's answer, not a caller error.
		enum class PowerThrottling : uint8_t { Topology = 0, OptOut, SystemManaged, Force };
		static void            SetWorkerPowerThrottling(PowerThrottling p);
		static PowerThrottling GetWorkerPowerThrottling();

		// INGRESS BACKPRESSURE -- bound how far a producer may run ahead of the pool.
		//
		// The deques are capped and overflow to a side lane; the slab grows and says so. The inbox
		// was the one unbounded path, so a producer thread flooding the runtime grew memory without
		// limit and nothing said a word. This is the bound.
		//
		// ZERO IS UNLIMITED AND IS THE DEFAULT. Nothing changes until an application asks. When it
		// is off, both the submit check and the drain accounting are one relaxed load of a
		// read-shared line -- the same gate the overflow lanes use.
		//
		// MUST BE SET BEFORE Init. The depth counter is only maintained while a limit is set, so
		// turning it on mid-run starts counting from a base that already has tasks in flight, and
		// the number stays wrong for the life of the process.
		//
		// NON-WORKER SUBMITTERS ONLY, and that restriction is the entire safety argument. Bounding a
		// queue means somebody stops pushing, and if that somebody is a WORKER it may be the only
		// thread able to drain the queue it is waiting on -- a Native task inside ParallelFor pushes
		// chunks to every worker including itself, so a bound there deadlocks deterministically
		// rather than occasionally. External threads are never consumers of a worker inbox.
		//
		// A HELD PRODUCER HELPS, IT DOES NOT SLEEP. Over the limit, the submitting thread runs one
		// task itself before continuing -- the same mechanism WaitFor already uses on non-workers.
		// And it is BOUNDED, not blocking: one task, then the push proceeds regardless. Slowing a
		// producer to the rate the pool drains at is the goal; giving the runtime a veto on
		// submission is not, because a submit that never returns is worse than a deep queue.
		static void   SetSubmitLimit(size_t maxQueued);
		static size_t GetSubmitLimit();

		// Tasks sitting in inboxes, queued but not yet started. ZERO unless a submit limit is set --
		// the counter is not maintained otherwise, and reporting a stale zero would be worse than
		// reporting nothing.
		static size_t QueuedDepth();

		// Hard-pin ONLY the hot workers to their cores, leaving the rest on the global policy.
		// MUST be set before Init -- placement happens as each worker starts. OFF BY DEFAULT.
		//
		// Hard-pinning the whole pool measured ~45%% worse wake latency and ~2x on a frame DAG,
		// which is why the default policy is Ideal. A hot worker is the opposite case: it never
		// parks, so it has no wake latency to lose, and there are one or two rather than N. This is
		// also the PORTABLE half of the priority story -- it reduces preemption without needing
		// TIME_CRITICAL or CAP_SYS_NICE.
		static void SetHotWorkerPin(bool on);
		static bool GetHotWorkerPin();

		// EXCLUSIVE AFFINITY: the hot workers own their cores and EVERY OTHER THREAD masks those
		// bits off. Pinning ALONE measured worse than doing nothing, because it confines the hot
		// worker without excluding anyone else from its core -- so when another thread lands there
		// the hot worker cannot migrate away and waits. This is the other half. Set before Init,
		// implies pinning for the hot workers, OFF BY DEFAULT.
		//
		// The userspace approximation of isolcpus -- and it cannot exclude OTHER PROCESSES, which is
		// exactly where it stops being equivalent to the real thing.
		static void SetHotWorkerExclusive(bool on);
		static bool GetHotWorkerExclusive();
		static void SetHotCpuMask(unsigned long long m);
		static unsigned long long GetHotCpuMask();

		// Called BY a thread ON ITSELF to stay off the hot cores. Ordinary workers and the reactor's
		// completion threads do this automatically; an application thread that wants the same can
		// call it directly. No-op unless exclusive mode is on.
		static void ExcludeCurrentThreadFromHotCpus();

		// How much estimated SERIAL WORK (microseconds) a loop must represent before ParallelFor splits
		// it. Defaults to 75us in Release and 750us in Debug -- the constant is the fork-join
		// dispatch+join overhead, and an unoptimized build pays roughly an order of magnitude more of it.
		// Exposed because it is a property of the machine and build, not a universal truth; an app that
		// has profiled its own workload knows better. Set it enormous (1e12) to force every ParallelFor
		// serial, which is the fastest way to answer "is ParallelFor causing this?" without a rebuild.
		// Set once at startup; read-only thereafter.

		// Force every ParallelFor to run its whole range inline, on the calling thread.
		//
		// Replaces `SetParallelForThresholdUs`, which was removed in 1.4 along with the probe it
		// tuned. That setter's genuinely useful job was not tuning: it was answering "is ParallelFor
		// responsible for this?" in one run, without a rebuild, by being set enormous. This keeps
		// that affordance and drops the pretence that the number meant anything portable.
		static void SetParallelForSerial(bool on);
		static bool ParallelForSerial();

		// Parallel range loop. Calls func(lo, hi) over disjoint subranges covering [begin, end),
		// blocks until every one has run, and the calling thread participates rather than idling.
		//
		// IT NEVER PREDICTS -- this is the 1.4 change, and it replaced the probe outright.
		//
		// Until 1.4 this ran a serial prefix, timed it, extrapolated, and parallelized only if the
		// estimate cleared a ~75us gate. That works on a uniform body and CANNOT work on a
		// data-dependent one, which is what this library is for. A prefix over items 0..311 says
		// nothing about item 50,000 when the early ones early-out at 2 ns and the later ones do full
		// mesh collision at 4,000 ns. Worse, the prefix WARMS THE CACHE while the remainder streams
		// from DRAM, so the estimate is biased low SYSTEMATICALLY rather than noisily; and on a
		// hybrid part the probe runs on the caller's core class and the work runs on another.
		//
		// THE ROOT OF IT: ITERATION COUNT IS A USELESS PROXY FOR EXECUTION TIME. Everything below is
		// a consequence of trying to recover time from a number that does not carry it.
		//
		// Static probing hits four walls. They are independent -- fixing one leaves the rest:
		//
		//   1. PROBE OVERHEAD. A timer read is 15-30 ns plus pipeline serialisation, against a
		//      ~69 ns task overhead, so a probe fine-grained enough to see divergence costs more
		//      than the thing it is deciding about.
		//      *** THIS ONE DID NOT BITE US, and the distinction is worth keeping: our probe called
		//      the clock exactly TWICE per ParallelFor, outside the loop, so ~30-60 ns amortised
		//      over the whole range. Do not cite it as a reason ours failed. It matters the other
		//      way -- ours was the CHEAPEST POSSIBLE probe and the other three walls still took it.
		//   2. CACHE PERTURBATION. The prefix warms L1/L2; the remaining 99% streams from DRAM. The
		//      estimate is therefore biased LOW systematically rather than noisily -- always the
		//      same direction, so it never averages out and more sampling cannot help.
		//   3. DATA-DEPENDENT WORK. In the workloads this library targets -- frustum culling, ray
		//      tracing, narrow-phase -- iteration 1 early-outs at 2 ns and iteration 50 does full
		//      mesh collision at 4,000 ns. No sample predicts divergence.
		//   4. ASYMMETRIC TOPOLOGY. Measuring on a P-core describes an execution profile that means
		//      nothing once an E-core steals the remainder. The normal case on the Android/ARM
		//      targets, not an edge case.
		//
		// And a fifth that is about people rather than hardware: the gate LOOKED DETERMINISTIC while
		// being wrong. Same input, same answer, every run -- so it read as a measurement rather than
		// a guess, and nothing ever prompted a check. A noisy predictor advertises its own
		// unreliability; this one did not.
		//
		// NOT A NOVEL POSITION. No mainstream work-stealing runtime gates parallelism on a timed
		// serial prefix: Cilk-5 let spawn/steal decide, oneTBB's auto_partitioner uses a depth
		// budget with steal feedback, and Rayon resets a split budget on observed steals. All of
		// them infer demand from STEALS -- something that actually happened -- rather than from a
		// cost model. This is converging with a settled answer, not inventing against it.
		//
		// WHAT REPLACED IT: RECURSIVE LAZY SPLITTING, and STEALS DECIDE. This publishes the right
		// half of the range onto the calling thread's own deque and carries on with the left. Nobody
		// took it -> the splitter takes it straight back and runs it inline for ~11 ns, no dispatch
		// and no notify. Somebody took it -> the pool was hungry and the split was right.
		//
		// Everything it needs lives on THIS stack frame, which is sound only because ParallelFor
		// BLOCKS: the wait group and callable outlive every task by construction. That is exactly
		// why PushArray, which does NOT block, cannot use it.
		//
		// A SHARED-CURSOR ALTERNATIVE EXISTS AND IS USED AS A FALLBACK (RunCursorRange): one task per
		// worker, each pulling [lo, lo+grain) off one atomic. ParallelFor was switched to it for a
		// single commit and switched back. The bench crossover sweep -- 32 points over four body
		// costs, which is the instrument for this question -- shows the two CROSS OVER rather than
		// tie, medians of 3 and non-overlapping below 200k:
		//
		//     heavy body   N=1000   2000    4000    10000   200000
		//       splitter   10.3x   12.6x   13.6x   13.9x    18.5x
		//       cursor      6.6x    7.8x    9.4x    9.3x    22.6x
		//
		// The splitter is 1.4-1.6x better across the whole mid-range -- hundreds to thousands of
		// items with real per-item work, i.e. the frame-graph shape this library is for -- and gives
		// up ~1.2x only at very large N. A "tied" verdict was published briefly on the strength of
		// three samples that were all large-N, which is the one region where they agree. Do not
		// re-run that comparison without the sweep.
		// GRAIN (was `chunkSize`) is the smallest subrange worth handing to another thread. It is
		// floored at 64 slices per worker with an absolute floor of 64 items -- a statement about
		// the POOL, whose size is known exactly, never about the BODY.
		//
		// WHAT YOU GIVE UP with the probe gone, stated plainly: nothing here can decline to
		// parallelize a body too cheap to be worth it. Pass a grain far smaller than the body
		// justifies and it will faithfully slice that fine and lose to a serial loop -- ~0.19x on a
		// 20k range of a ~0.4 ns/element body, where the old probe held 1.00x. Give it a grain worth
		// a few microseconds of work and the same case is ~1.00x. If you cannot say, use the
		// overload below; if you need to know whether a loop should be parallel at all,
		// SetParallelForSerial(true) answers it in one run without a rebuild.
		void ParallelFor(int begin, int end, int grain, std::function<void(int, int)> func);

		// The same thing WITHOUT having to name a grain, and this is the one most callers want.
		//
		// "How do I know what grain to pass?" is the fair objection to the overload above, and for
		// most callers the honest answer is that they cannot know: they know their body is "a
		// collision check" or "a matrix multiply", not what it costs in nanoseconds per element.
		// Requiring a number nobody has is how you get 32 passed in because it looked reasonable.
		//
		// So this derives one from the two things that ARE known exactly -- the range and the pool
		// size -- and never consults the body: `range / (workers * 8)`, the rule Cilk's `cilk_for`
		// uses for its own default.
		//
		// A TUNING CONSTANT IS PART OF THE ALGORITHM IT WAS TUNED FOR, and this number has now been
		// wrong in BOTH directions inside a single release. 8 per worker suits the recursive
		// splitter, which pays per split and has a serial spine and so wants fewer, larger leaves.
		// While ParallelFor was briefly pointed at the shared cursor -- which publishes every lane at
		// once and pays one fetch_add per slice, so it wants the finer division -- keeping 8
		// measured 6.23x against 16.55x at an explicit grain on a 64 MB memory-bound body. Changing
		// it to defer to the cursor's floor then measured 4.36x against 11.33x once ParallelFor went
		// back to splitting. If the algorithm underneath changes again, this is a measurement to
		// redo, not a constant to carry across.
		//
		// PASS A GRAIN ONLY IF YOU HAVE MEASURED. The number that matters is wall-clock per leaf,
		// not elements: aim for a few microseconds of work in each. If you have timed the loop
		// serially once -- which is a two-line experiment in a dev build -- then
		// `grain = range * (target_leaf_time / total_serial_time)` gets you there without knowing
		// anything per-element.
		//
		// This does NOT rescue a body too cheap to parallelize; nothing probe-free can, and the
		// explicit overload's comment says why. What it does is bound the damage: leaves are capped
		// at `workers * 64`, so the worst case is that many dispatches rather than one per element.
		void ParallelFor(int begin, int end, std::function<void(int, int)> func);
		// `ParallelForFJ` was REMOVED in 1.4. It was the fork-join variant: split in half, spawn the
		// right half, recurse left. ParallelFor stopped dispatching to it when the slice-stealing
		// cursor path replaced per-chunk tasks, which left it public with no caller anywhere.
		//
		// USE `ParallelFor`. It is the drop-in -- same shape, same blocking behaviour -- and it still
		// decides serial-vs-parallel for you.



		// Shared slice-stealing core behind ParallelFor. Takes func
		// by REFERENCE: both callers block, so the object outlives every task, and copying a
		// std::function per worker would reintroduce an allocation this exists to remove.
		void RunCursorRange(int start, int end, int grain, std::function<void(int, int)>& func);

		// Blocking range loop with ATOMIC SLICE-STEALING, and no probe.
		// `ParallelRange` was REMOVED in 1.4, before it ever shipped. It was added earlier in the
		// same release as the probe-free entry point, and once ParallelFor lost its probe and moved
		// to the same slice-stealing cursor the two were literally the same function. Two names for
		// one behaviour is worse than the problem either was solving. Use ParallelFor.

		bool Push(Task* task);
		void WaitFor(WaitGroup& wg);

		// ---- THE CANCELLABLE WaitFor WAS REMOVED (9-02). WaitGroup IS NOT A WAIT PRIMITIVE. -----
		//
		// `WaitFor(wg, token)` and `WaitGroup::CancelWaiters` are gone. The header carried its own
		// obituary for weeks: "INCOMPLETE. A FIBER THAT PARKS HERE CAN WAIT FOREVER."
		//
		// THE IDEA WAS WRONG, NOT JUST THE DELIVERY. A WaitGroup is a CONCURRENCY COUNTER -- N
		// outstanding, and a join that ends at zero. Event, the semaphore and the condition variable
		// own a QUEUE OF WAITERS, which is the thing cancellation ejects from; a counter owns none.
		// That is why the old CancelWaiters had to promise in capitals that it did not touch `n`:
		// it could not, because cancelling a wait while the counted work keeps running leaves the
		// count outstanding and the tasks decrementing into a group nobody is joined to.
		//
		// So there was nothing coherent to deliver, and an EjectWaitGroup would have made an
		// incoherent operation reliable rather than correct.
		//
		// WHAT IT COST: waitgroup_cancel_test deadlocked 2-13% of runs, and a deadlocked test does
		// not fail -- it spins every worker until something kills it. Its only user was itself, and
		// it stayed green by calling `inner.CancelWaiters(...)` by hand on the line after
		// `scope.Cancel()`, working around the very gap it existed to cover.
		//
		// IF YOU NEED A JOIN YOU CAN ABANDON, compose it from a primitive that HAS waiters: wait on
		// an Event the last task signals, and cancel the Event. The counter stays a counter.
		//
		// Plain WaitFor(wg) below is untouched and was always the uncancellable one by contract.
		bool Push(uint8_t cpu_affinity, Task* task);

		// ---- PUSH TO THE LANE, OR SAY YOU COULD NOT. ------------------------------------------
		//
		// Every other Push places the task SOMEWHERE and returns. This one is allowed to REFUSE, and
		// that is its entire reason to exist: the I/O reactor needs to know whether a reserved
		// worker actually took the completion, because if none did it keeps the task in its own
		// backlog and retries rather than dumping it somewhere it will sit.
		//
		// The bool is NOT a queue-level failure -- the inbox is a Vyukov append and cannot fail once
		// the item is committed. It is the result of TARGET SELECTION: "was there an available K
		// worker". That distinction is why this lives here and not on TaskMPSCQueue, where a
		// try-push would be a check that can never fail.
		//
		// REQUIRES A lane TASK, and returns false for anything else rather than placing it. K reads
		// only its lane inbox, so a loPri task sent to a reserved worker is not deprioritised, it is
		// UNREACHABLE -- inbox work is unstealable and K never looks. That exact routing hung the
		// whole reactor until 9-02. Refusing here means a caller that gets it wrong sees a false and
		// sends the task to the floor, instead of silently losing it.
		//
		// AVAILABILITY IS LaneBacklogMask(), not `busy`. A K worker mid-handler is fine to queue
		// behind; one already advertising a lane backlog past kLaneStealDepth is not. Reading the
		// mask rather than K per-worker queries keeps this at one atomic load per call.
		//
		// Returns false when: K == 0, the task is not lane, or every reserved worker is buried.
		bool PushIO(Task* task) noexcept;

		// ---- THE SHARED LANE INTAKE: producer, consumer, and the park probe -------------------
		//
		// PushLaneIntake  N reactor producers, one bulk enqueue, then ONE notify so a reserved
		//                 worker that is allowed to park cannot sleep on a non-empty intake.
		//                 Returns false when K == 0 -- there are no consumers, so the caller must
		//                 send the work somewhere that has one rather than let it sit.
		// TakeLaneIntake  called only by a reserved worker, on its own pass.
		// LaneIntakeIdle  the park probe. MUST be named by every predicate that can park a reserved
		//                 worker: this queue has K legal consumers rather than one, but zero of them
		//                 are looking while they are all asleep, and the intake is not attached to
		//                 any worker for a drain to find.
		//
		// APPROXIMATE BY CONSTRUCTION, and that is why it is spelled "Idle" rather than "empty".
		// moodycamel's size_approx can lag a concurrent enqueue, so this may say idle while an item
		// is in flight -- exactly the window TaskMPSCQueue::quiescent() exists to close for the
		// inboxes. The notify in PushLaneIntake is what makes that safe: the producer stores, then
		// wakes, so a worker that parks on a stale idle reading is woken by the push that raced it.
		// A predicate that trusted this ALONE would be the lost wake again.
		// ---- WHEN MAY A RESERVED WORKER GO EARN ITS CORE BACK? --------------------------------
		//
		// A reserved worker that never steals costs the floor a worker's worth of throughput: the
		// pool is sized at N-K whatever K is doing, so parking returns the core to the OS and not to
		// the pool. At K=2 on a 29-worker pool that is ~7% of the pool idle whenever I/O is quiet.
		//
		// So they steal -- but only after the lane has been silent for this long. The risk is
		// measured and it is not small: tryStealFrom records a 1202 us max from a reserved worker
		// holding a bulk task when a completion arrived.
		//
		// TIME SINCE THE LAST PUSH, NOT "IS THE INTAKE EMPTY". The gap between two completions in
		// one burst is empty too, and stealing into that gap is exactly how that 1202 us happened.
		//
		// THE DEFAULT IS A GUESS AND IS MEANT TO BE TUNED. 500 us is long enough that an ordinary
		// inter-arrival gap does not read as quiet and short enough to reclaim a core promptly, but
		// no measurement here chose it. Raise it if the lane's tail regresses; lower it if the floor
		// is visibly short of workers while I/O idles.
		// The on/off arm for the whole behaviour, and the A/B against a purely reserved band. Off
		// means a reserved worker takes nothing but lane work, however long the lane stays quiet --
		// which is what the placement invariant has to be measured against, since with stealing on
		// "ordinary work ran on [0,K)" no longer distinguishes a placement bug from K earning its
		// core back.
		static void     SetReservedStealing(bool on) noexcept;
		static bool     ReservedStealing() noexcept;

		static void     SetIoQuietWindowUs(unsigned us) noexcept;
		static unsigned IoQuietWindowUs() noexcept;
		static bool     IoLaneQuiet() noexcept;

		static bool  PushLaneIntake(Task** tasks, size_t n) noexcept;
		static Task* TakeLaneIntake() noexcept;
		static bool  LaneIntakeIdle() noexcept;

		// THE A/B ARM. Off restores per-worker steering at push time -- the control the intake has
		// to beat. A RUNTIME flag rather than a build one on purpose: the two arms must be
		// comparable inside one process, because separately built binaries once moved a dispatch
		// bench's K=1 rows by 2x and that was machine drift reported as a result.
		//
		// Turning it off does NOT stop reserved workers reading the intake -- it stops the reactor
		// filling it, so anything already queued still drains. Flipping it mid-run is therefore
		// safe in both directions.
		static inline std::atomic<bool> laneIntakeOn{ true };
		static void SetLaneIntake(bool on) noexcept { laneIntakeOn.store(on, std::memory_order_relaxed); }
		static bool LaneIntakeEnabled() noexcept { return laneIntakeOn.load(std::memory_order_relaxed); }
		// ---- WHERE A RESUMED TASK WENT, AND WHO MAY TAKE IT ------------------------------------
		//
		// NOT A bool, AND THE REASON IS THAT `false` WOULD MEAN TWO THINGS. Today false means "this
		// task is queued NOWHERE" -- a lost task, and the yield site calls dropping it "the worse of
		// the two lost-task sites". Under migratable fibers the caller also wants to know whether
		// the resume became STEALABLE, and answering that with the same bool makes `!Requeue(t)`
		// unreadable: a leak and a correctly-pinned resume look identical.
		//
		// THE AXIS IS HOW MANY WORKERS MAY RUN IT, which is the thing that decides resume latency:
		//   Failed     queued nowhere. The task is lost unless the caller acts. Only a null task
		//              today, but it stays distinct so it cannot be confused with the two below.
		//   Pinned     queued where exactly ONE worker may take it -- a resume inbox, or an
		//              ordinary inbox before its owner drains it. Resume waits for that worker.
		//   Stealable  queued on a DEQUE. The owner pops it LIFO (still cache-warm) and any thief
		//              may take it from the other end, so the resume happens on whoever is free.
		//
		// That last one is the whole point of migratable mode: pinned makes a resume wait for one
		// specific worker, stealable lets the pool answer it.
		enum class RequeueResult { Failed, Pinned, Stealable };
		RequeueResult Requeue(Task* task);
		// minPerSegment: the smallest run this is willing to hand to a single worker. The default of
		// 64 suits a big fire-and-forget batch, where the alternative is ONE push and the notifies
		// are pure added cost. A caller replacing N individual Push() calls -- which already notify
		// N times -- wants 1, because for it any segmenting strictly REDUCES notifies. Getting this
		// backwards is a real regression in both directions, so it is a parameter rather than a
		// constant: see ParallelFor's flat path.
		// pref: the batch is still assumed HOMOGENEOUS -- every task in one call is placed as this
		// ONE class, not read per-task. A genuinely mixed-corePref set of tasks is the CALLER's job
		// to split into homogeneous runs and call PushBatch once per run -- see Thread.cpp's
		// drainInbox for the pattern. Deliberately not scanned here: most callers (ParallelFor, any
		// already-homogeneous submission) already know their batch is one class, and a scan-and-
		// partition on every call would tax that common case to serve the one caller that needs it.
		void PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity=0, size_t minPerSegment=64,
		               Lane lane = Lane::Normal, CorePref pref=CorePref::Default);

		// Submit [begin, end) as ceil(n/chunkSize) TASKS rather than n of them, each task looping
		// over its own chunk and calling fn(i) per index. Returns the number of tasks created.
		//
		// WHY THIS EXISTS. Per-task scheduler overhead is ~85-105 ns measured (create + dispatch +
		// execute + reclaim), and it is a per-TASK cost, not a per-item one: it buys queueing,
		// stealing, completion accounting and reclamation, none of which n independent items each
		// need separately when they are known up front. Chunking divides that whole figure by
		// chunkSize -- at 32 it is ~3 ns/item -- which is the same amortisation a range-based API
		// gets, without giving up a per-item callable.
		//
		// NOT MADE OBSOLETE BY ParallelFor, and the reason is structural rather than a
		// performance argument. ParallelFor BLOCKS: its cursor, wait group and
		// callable all live on the caller's stack frame, which is the only thing that makes a
		// zero-allocation slice-stealing loop possible. PushArray does not block -- it hands you a
		// WaitGroup and returns, so you can submit range work and go do something else, or never
		// wait at all. A non-blocking cursor would need heap state and a refcount to keep it alive
		// past the caller's frame, which is a different design with a different cost.
		//
		// So the split is: PushArray when you want the range submitted and control back, the other
		// two when you want it done before the next line runs. It is also the only one of the three
		// taking a PER-ITEM callable rather than fn(lo, hi).
		//
		// ONE SHARP EDGE, since nothing here floors chunkSize for you: this creates a task per
		// chunk, so a tiny chunkSize over a large range creates a task per few items and will
		// exhaust the slab (measured: chunk 1 over 4M items exhausted a 1M-slot arena and fell back
		// to running inline, at 32 ns/item against 0.06 at chunk 4096). Pick a chunk size.
		//
		// NOT A REPLACEMENT FOR ParallelFor, which picks its own split, runs a chunk on the calling
		// thread and blocks until done. (It no longer probes -- 1.4 removed the gate; splits are
		// published speculatively and steals decide.) PushArray is the fire-and-forget
		// sibling: it returns as soon as the work is queued, so use it when the caller has other
		// things to do, wants to submit several arrays before waiting on them together, or already
		// knows its own chunk size. Pass a WaitGroup to be able to wait; pass nullptr not to.
		//
		// fn is COPIED into every chunk task (so it must be copyable, and its captures must outlive
		// the work). If the arena is exhausted the remaining chunks run INLINE on the caller, which
		// is the same graceful degradation ParallelFor does rather than dropping work on the floor.
		template<typename F>
		// lane routes every chunk task to the high-priority queue; default is low, matching
		// CreateTask and PushBatch. Priority here is QUEUE ORDER only -- it never implies placement,
		// which is CorePref's job (see Task.h).
		size_t PushArray(size_t begin, size_t end, size_t chunkSize, F&& fn, WaitGroup* wg = nullptr,
		                 Lane lane = Lane::Normal) {
			if (end <= begin) return 0;
			if (chunkSize == 0) chunkSize = 1;
			const size_t total  = end - begin;
			const size_t chunks = (total + chunkSize - 1) / chunkSize;

			std::vector<Task*> ts;
			ts.reserve(chunks);
			for (size_t c = 0; c < chunks; ++c) {
				const size_t lo = begin + c * chunkSize;
				const size_t hi = (lo + chunkSize > end) ? end : lo + chunkSize;
				// Wide: these are chunks of one range, split precisely so other workers run them.
				// See the splitter and cursor paths -- same argument, same currency.
				Task* t = CreateInternalTask([fn, lo, hi]() { for (size_t i = lo; i < hi; ++i) fn(i); },
				                             lane, CorePref::Wide);
				if (!t) {                                   // arena exhausted: run it here
					for (size_t i = lo; i < hi; ++i) fn(i);
					continue;
				}
				t->waitGroup = wg;
				ts.push_back(t);
			}

			// Count BEFORE the push, never after: the instant PushBatch publishes these, a worker
			// can run one and decrement. Adding to n afterwards races a WaitFor that already saw
			// zero and returned on half-submitted work.
			if (wg && !ts.empty())
				wg->n.fetch_add((int)ts.size(), std::memory_order_relaxed);
			if (!ts.empty())
				PushBatch(ts.data(), ts.size(), 0, /*minPerSegment*/64, lane);
			return ts.size();
		}
		// PushImmediate was REMOVED in 4.0.1 -- use TaskScheduler::SetReservedCores and run a plain
		// std::thread. It handed a task to a SPECIFIC worker`s immediate slot and marked that core
		// in-use until the task finished, which for a persistent subsystem meant forever: a worker
		// taken out of a WORK-STEALING pool, its queue spilled to everyone else.
		//
		// Its justification was that a pinned task stays VISIBLE TO PLACEMENT where an outside thread
		// is a core that quietly went missing. Dynamic K ended that: a pinned worker is neither
		// ordinary nor hot in anything the scheduler tracks, so it became invisible in a second way --
		// and the hot set could grow OVER it, steering completions into a queue that never drains.
		//
		// SetReservedCores buys the same census accounting with none of the invariants, because the
		// scheduler never owns the thread: nothing can promote it, steal from it, or wait on it.

		// PushFork was REMOVED in 1.3.4 -- use Push. It placed a child on the CALLING worker, on the
		// theory that the parent was about to WaitFor and free that core, so the child would run
		// warm on the parent's data. Measurement killed both halves of that: the win was one
		// avoided worker WAKE (~5us, and absent entirely on an unparked worker) and not locality at
		// all -- a control child sharing NO data with its parent saved just as much. Against that,
		// it was 1.5x-4.6x SLOWER for any fork-join-shaped spawn, worst on small trees. To place a
		// task on a known worker, Push(cpu_affinity, task) -- one-based, 0 meaning round-robin.

		static bool IsInitialized() {
			return instance != nullptr;
		}

		// How many worker threads this pool ACTUALLY has. Use this to size per-worker resources --
		// command allocators, context pools, a third-party job system's concurrency hint -- rather
		// than recomputing it from std::thread::hardware_concurrency(), which is a guess about a
		// number this object already knows. The auto pool is hw-1, but Init(poolSize) accepts an
		// explicit size and an app with a persistent audio thread is told to pass hw-2, so any
		// hardware_concurrency-derived count is wrong on exactly the machines that were configured
		// deliberately.
		//
		// NOT the same question as GetSafeTC(), which answers "what size SHOULD the pool be" and is
		// only meaningful before Init(). This answers "what size is it".
		//
		// Fixed after Init() and safe to call from anywhere; the vector is built once in StartPool
		// and never resized while the pool is running.
		size_t GetWorkerCount() const;

		// THE THREAD TABLE, indexed by worker id. Exposed so a caller can reach a specific worker's
		// pinned fiber and suspend or resume it DIRECTLY -- `GetThreads()[w]->GetFiber()->Resume()`
		// -- instead of going through an Event, which costs a waiter table, an arbitration and a
		// pooled object to say the one thing this says in a pointer dereference.
		//
		// A REFERENCE TO THE LIVE CONTAINER, NOT A COPY, and it cannot be a copy: Thread owns a
		// std::thread, atomics and its own park fiber, so a vector<Thread> by value would have to
		// duplicate a running thread. Callers get the real table or nothing useful at all.
		//
		// AND NOT std::vector<Thread> EITHER, YET. Thread still holds joinMutex, cvWorkerDone and
		// cvAffinity -- all non-movable -- so std::vector<Thread> does not instantiate. Those are
		// Join's and the affinity path's, not the park's, and the park's are already gone. When
		// Join is rebuilt and they go with it, Thread becomes movable and this can become a
		// contiguous vector<Thread> without touching a single call site, because the subscript and
		// the -> both still read the same.
		//
		// INDEX IS THE WORKER ID, so entry w is worker w's Thread and the fiber it holds is pinned
		// to w (Fiber::homeWorker == w). That correspondence is what makes the table addressable at
		// all; without pinning, entry w's fiber could resume anywhere.
		const std::vector<Thread*>& GetThreads() const { return workers; }

		// Pay the task slab's page faults NOW rather than during the run.
		//
		// As of 1.4 the slab is LAZY: `Init()` touches almost nothing, and resident memory grows
		// with peak live tasks instead of with capacity (measured 277 MB -> 21 MB, 57 ms -> 5.6 ms
		// at 31 workers). The trade is that first-touching a page moves from startup into whenever
		// that page is first needed, so a workload whose live-task count GROWS faults pages
		// mid-run -- and a fault mid-frame is a latency spike.
		//
		// Call this during load if you would rather eat the cost up front. Prefaulting the full
		// capacity reproduces the pre-1.4 behaviour exactly. Latency-sensitive applications with a
		// known task ceiling probably want it; a phone, or anything that never approaches the
		// default million slots, should leave it alone and keep the memory.
		void PrefaultTaskSlots(size_t slots);

		// Build the task slab LAZILY instead of up front. Set BEFORE Init(); the slab is constructed
		// with the scheduler.
		//
		// DEFAULT IS OFF, and the reason is a property rather than a number. Eager construction
		// faults every page in before the first task runs, so the steady state has no memory events
		// at all -- allocation is a pointer pop off a free list. That is the zero-allocation runtime
		// this library advertises. Lazy keeps heap allocation out too, but moves first-touch page
		// faults INTO the run, and a fault mid-frame is an unpredictable kernel transition on the
		// critical path -- the same class of problem as worker-side epoch reclamation, which was
		// worth 3x on p99 to move off it.
		//
		// What lazy buys, measured at the default 1M slots x 256 bytes:
		//
		//     eager   277 MB resident, Init() 57 ms      lazy   21 MB resident, Init() 5.6 ms
		//
		// So: leave it off on desktop, where 256 MB is invisible and the guarantee is worth more.
		// Turn it on for Android and iOS, where a whole app may have a few hundred MB and a quarter
		// of a gigabyte of never-touched task slots is disqualifying. Resident cost then tracks peak
		// live tasks rather than capacity.
		//
		// PrefaultTaskSlots() is the middle ground: lazy on, then prefault the ceiling you actually
		// expect during load, and pay nothing for the rest.
		static void SetLazyTaskSlab(bool on);
		static bool LazyTaskSlabEnabled();

		// Total task slab capacity. Defaults to 1024*1024 (1M), same as always; call this before
		// Init() to change it. No structural reason it was fixed -- not a power of two anything else
		// depends on, not coupled to the ABI guard, which hashes type layout rather than a runtime
		// count -- it just went unexposed until something wanted it. Combine with SetLazyTaskSlab to
		// size AND choose the commit strategy: a small eager slab for a memory budget you want to
		// hold for certain, or a large lazy one for a ceiling you do not expect to reach.
		//
		// PRE-INIT ONLY, same contract as the setters above: taskAllocator is a member of
		// TaskScheduler, constructed once when Init() calls `new TaskScheduler(...)`, so this has to
		// land before that to be read by it.
		//
		// Exhaustion past whatever this is set to is not an error on its own -- CreateTask returns
		// nullptr and every caller in this codebase already falls back to running inline (see
		// RunLazyRange and ParallelFor's grain-floor comment) -- but a slab too small for a real
		// workload turns that fallback from an occasional safety net into the common case, which
		// costs the dispatch benefit this scheduler exists for. Measure before shrinking it.
		// WHAT ELSE DRAWS ON THIS SLAB, because "slots" is not the same as "tasks" and sizing it
		// from the task count alone will come up short:
		//
		//   a task                1 slot
		//   a TaskDAG node        2 slots  -- the TaskNode AND its dependents list; TaskNode's
		//                                    constructor allocates twice. Applies to gates and
		//                                    external nodes too, which have no task at all.
		//   a spawned coroutine   2 slots  -- its Task and its frame (frames come from here as of
		//                                    2.12.0, for one arena to size and observe rather than
		//                                    two, and so coroutines do not punch a hole in the
		//                                    zero-allocation steady state described above)
		//
		// THE FAILURE MODES DIFFER, which matters more than the counts. A coroutine frame FALLS BACK
		// to global new when the slab is full or the frame exceeds TaskAllocator::SLOT, so it never
		// fails -- frames borrow opportunistically and stop competing under pressure. A task returns
		// nullptr; a TaskNode's constructor THROWS. Size for the tasks and nodes; the frames will
		// look after themselves.
		//
		// JLib::SetCoroFramePooling(false) opts frames back out to global new entirely.

		// ---- per-class slab sizing ---------------------------------------------------------------
		//
		// The slab is THREE pools -- 256, 128 and 64 bytes -- and their sizes used to be derived from
		// the one task-slab number by fixed divisors (`slots / 8` each). That was defensible while
		// coroutine frames were the only user of the small classes, and it stops being defensible the
		// moment anything else uses them: a divisor chosen for one consumer silently starves the next.
		//
		// It is not hypothetical. Routing Tasks and TaskNodes into the 64-byte class was tried and
		// immediately evicted coroutine frames from it -- the pool sized `slots / 8` for frames could
		// not also hold the tasks, so frames quietly fell back to 256-byte slots and the class stopped
		// doing the job it was built for. Nothing failed; it just silently undid itself.
		//
		// So the sizes are EXPLICIT. One number cannot size three pools, because memory in one pool
		// cannot serve a request from another -- that is the price of size classes, and the right
		// response is to let the caller state the split rather than to guess a divisor.
		struct SlabSizes {
			// NAMED FOR THEIR SLOT SIZE, not big/mid/small. `small` is a Windows macro (rpcndr.h:
			// `#define small char`), so a member of that name breaks every consumer that includes
			// windows.h -- which shipped in 3.0.0. Caught by tests/windows_header_guard.cpp on its
			// first run, in the PUBLIC struct callers have to write, which is the worse place for it.
			// DEFAULTS DERIVED FROM MEASUREMENT, not from a divisor. Game01 instrumented with
			// JLIBSCHED_TASK_STATS over 37,480 real tasks: largest 80 bytes, mean 77.5, 15.4% at
			// exactly 64 and 84.6% at exactly 80, NOTHING above 80. So the 80-byte pool is the
			// primary one and the 256-byte pool is no longer where tasks live -- it now serves DAG
			// edge chunks (which want a full slot on purpose) and coroutine frames up to 224 bytes.
			//
			//     256K x 256 =  64 MB      1M x 80 = 80 MB
			//     128K x 128 =  16 MB    256K x 64 = 16 MB      total 176 MB
			//
			// That is DOWN from 256 MB pre-3.0, with 1.63M task capacity against 1M. An earlier cut
			// of this release added the 80-byte pool WITHOUT shrinking the 256-byte one and reserved
			// 360 MB -- a memory regression dressed as a memory optimisation.
			//
			// THE TRADE, stated because a default decides for everyone who never calls SetSlabSizes:
			// an application whose lambdas capture more than 128 bytes gets 256K task slots here
			// rather than 1M, because such a task goes straight to the 256-byte class and cannot
			// fall back. No measured workload looks like that -- but none has been measured, either,
			// and the honest version of "we have evidence for this shape and none for the other" is
			// to say so rather than to imply the case is closed.
			// ================= RESIZED IN 4.0.1: 176 MB -> 4 MB, and why that is safe ==============
			//
			// THE OLD NUMBERS WERE SIZED FOR A WORLD WHERE RUNNING OUT WAS FATAL. Exhaustion meant a
			// null Task and a dropped unit of work, so the only defensible default was one nobody
			// could plausibly exhaust -- and 176 MB of reserved address space, prefaulted at Init,
			// is what "nobody" costs. That is a backend-server footprint arrived at defensively.
			//
			// Growth (4.0.1) changed the trade. Exhaustion is now one allocation and a one-line
			// warning naming the class, so under-sizing costs a hitch instead of a failure. The
			// default no longer has to cover the worst case; it has to cover the COMMON case and
			// degrade honestly outside it.
			//
			// THE DISTRIBUTION IS UNCHANGED and still says where to spend: Game01, 37,480 real
			// tasks, 15.4% at exactly 64 bytes and 84.6% at exactly 80, nothing above 80. What
			// changed is the QUANTITY, because the figure that matters is PEAK CONCURRENT live
			// slots, not tasks ever created -- and 37,480 tasks over a run is nothing like 37,480
			// at once. TaskScheduler::ReportSlabUsage() now measures that peak directly, which is
			// the tool the old numbers did not have.
			//
			//     16K x 80 = 1.25 MB     24K x 64 = 1.5 MB
			//      2K x 128 = 0.25 MB     4K x 256 = 1.0 MB       total ~4.0 MB
			//
			// A MIDDLE, DELIBERATELY. IoT wants kilobytes and a cloud service wants gigabytes, and
			// no single number serves both -- so this targets the 80% that never call SetSlabSizes,
			// and everyone else profiles with ReportSlabUsage and sets their own. Being wrong low
			// now prints a warning telling them exactly that; being wrong high silently taxed every
			// user who never noticed.
			//
			// PREFAULT COST FALLS WITH IT. lazy=false means Init() walks and links every slot, so
			// the old default paid that on 176 MB of pages at startup. This is ~3.8 MB.
			// 64B GETS THE LARGEST SHARE despite being 15.4%% of the task distribution, and that is not
			// a contradiction. AllocSized falls through 64 -> 80 -> 128 -> 256, so the 64-byte class is
			// FIRST CHOICE for everything that fits it -- capture-free tasks, single-capture tasks, and
			// every TaskNode (56 bytes). The distribution describes what tasks ARE; the fall-through
			// decides what the class is ASKED FOR, and it is asked for more. Both benches exhaust this
			// class and no other, which is the measurement that set these numbers.
			size_t slots256 = 4 * 1024;          // DAG edge chunks, frames >128 B, oversized lambdas
			size_t slots128 = 2 * 1024;          // larger coroutine frames
			size_t slots80  = 16 * 1024;         // two-capture lambdas: the common frame-loop task
			size_t slots64  = 24 * 1024;         // capture-free / single-capture tasks, TaskNodes
		};

		// PRE-INIT ONLY: the allocator is constructed with these
		// when the scheduler is, so a call after Init() does nothing.
		//
		// THE FAILURE MODES STILL DIFFER PER CLASS, and that is what to size against. A coroutine
		// frame FALLS BACK -- to a larger class, then to global new -- so an undersized `small`
		// costs memory and never fails. A Task returns nullptr and a TaskNode's constructor THROWS,
		// so `big` is the one whose exhaustion is a real error. Size `big` for tasks and nodes; size
		// `small` and `mid` for the peak number of coroutine frames live at once, which is bounded by
		// how many you actually spawn, not by anything the library controls.
		static void SetSlabSizes(const SlabSizes& sizes);

		// SLAB GROWTH -- process-wide, ON by default, safe to call at any time.
		//
		// When a size class runs out, the pool allocates another extent instead of returning null.
		// That is a CORRECTNESS choice rather than a performance one: the alternative is a task that
		// never runs. "No allocations at runtime" was always a PERFORMANCE rule, and a performance
		// rule that turns into a crash at its own boundary is the wrong rule -- a game that exhausts
		// the slab should stutter once, not die.
		//
		// The cost is one allocation plus a one-time warning on stderr naming the class and its
		// size, so a developer profiling a build can size it properly at Init. Users never see it.
		//
		// Turn it OFF for a fixed-footprint build that would rather fail loudly, or for a test that
		// needs to assert a ceiling -- a ceiling that moves cannot be asserted.
		static void SetSlabGrowth(bool on) noexcept;
		static bool SlabGrowthEnabled() noexcept;
		static SlabSizes CurrentSlabSizes();

		// ---- OPTIONAL SERVICE LAYERS ------------------------------------------------------------
		//
		// This library is a JOB SYSTEM first. Deadlines and asynchronous I/O are layers on top, and
		// an app that wants neither should pay for neither -- not a thread, not a core, not a
		// surprise. So they are OFF until asked for, and asking has to happen before Init because
		// the pool is sized once and each enabled service takes a core.
		//
		//     TaskScheduler::EnableIoReactor(true);   // implies timers
		//     TaskScheduler::Init(0);                 // hw-3 workers: main, timer, completion
		//
		// ENFORCED, NOT ADVISED. Using a disabled layer fails at the first call, loudly and
		// deterministically, rather than working while quietly running the machine one thread over.
		// A warning was the first design and it was wrong: the failure it guards against is a few
		// percent of throughput, forever, which nobody notices and nobody attributes correctly. A
		// hard stop at the first Arm or the first Register is a two-minute fix.
		//
		// Enforcement only applies once a POOL EXISTS. Using the timer or the reactor without ever
		// calling Init is legitimate -- there are no workers to oversubscribe.
		//
		// Ignored when an EXPLICIT poolSize is given: that is the app's own arithmetic and this must
		// not silently second-guess it. The layers still have to be enabled to be used.
		static void EnableTimers(bool on) noexcept;
		static bool TimersEnabled() noexcept;

		// Implies EnableTimers: an I/O timeout is a deadline, and every non-trivial reactor user
		// wants one. Enabling I/O and then discovering timers are off would be a papercut with no
		// upside.
		//
		// `completionThreads` is HOW MANY THREADS DRAIN THE COMPLETION PORT, and it is a parameter of
		// this call rather than a knob on IoReactor for one reason: the pool reserves one core PER
		// completion thread, so the two numbers must agree. As separate settings they would drift,
		// and drifting means silent oversubscription -- the exact failure this opt-in exists to stop.
		//
		//     EnableIoReactor(true, 4);  Init(0);   // hw-6: main, timer, 4 completion threads
		//
		// DEFAULT 1, AND NOT MEASURED. IOCP is built for many threads on one port and a busy server
		// will want more than one -- but the crossover has not been benchmarked here, and the honest
		// default is the one whose cost is known. Raise it against a profile, not a hunch; the port's
		// concurrency limit is set to match, so the kernel runs at most this many at once regardless.
		static void EnableIoReactor(bool on, unsigned completionThreads = 1) noexcept;
		static bool IoReactorEnabled() noexcept;
		static unsigned IoCompletionThreads() noexcept;

		// Give the timer thread a core of its own, so an app that uses deadlines does not run one
		// thread over the machine.
		//
		// WHY THIS IS NOT AUTOMATIC. The auto pool size is hw-1, and GetSafeTC's census calls that an
		// EXACT FIT: workers + main, nothing spare. TimerQueue adds a thread, so an app using
		// deadlines is oversubscribed by exactly one -- the deficit that cost a measured 3-4% the
		// last time it happened (see GetSafeTC's history note on GameInput). But the timer thread
		// only exists once something arms a deadline, and that is long after Init has sized the pool,
		// so the scheduler cannot detect it -- and reserving unconditionally would take a worker away
		// from every app that never arms one.
		//
		// So it is the app's declaration, like an explicit poolSize. Call it before Init:
		//
		//     TaskScheduler::SetReserveTimerCore(true);
		//     TaskScheduler::Init(0);          // hw-2 workers; main and the timer take the rest
		//
		// Ignored when an EXPLICIT poolSize is given -- an explicit size is the app's own arithmetic
		// and this must not silently second-guess it.
		//
		// A Development build complains the first time a deadline is armed without this set, because
		// the alternative is the failure mode that is hardest to notice: everything works, slightly
		// worse, forever.
		static void SetReserveTimerCore(bool reserve) noexcept;
		static bool ReserveTimerCore() noexcept;

		// The same, for IoReactor's completion thread. Both may be set; the pool loses one core per
		// service thread in use, so an app doing timed async I/O runs hw-3 workers.
		//
		// TWO FLAGS RATHER THAN A COUNT, because the question an app can answer is "do I use
		// deadlines" and "do I use async I/O" -- not "how many service threads does the library run",
		// which is the library's business and has changed twice already.
		static void SetReserveIoCore(bool reserve) noexcept;

		// RESERVE CORES FOR THREADS THE APP RUNS ITSELF. Set BEFORE Init; read only when the pool
		// is sized, so setting it afterwards does nothing.
		//
		// The general case of SetReserveTimerCore and SetReserveIoCore, which reserve one core for
		// the timer thread and one per I/O completion thread. Same argument, count from the caller:
		// a thread the scheduler does not own still occupies a core, and one it does not KNOW about
		// is a core that quietly went missing. Declaring it keeps workers + main + timer + io + yours
		// an exact fit instead of oversubscribing by however many the app started.
		//
		//     TaskScheduler::SetReservedCores(1);       // I will run an audio mixer myself
		//     TaskScheduler::Init(0);                   // pool sizes itself around it
		//     std::thread mixer(RunMixer);              // ...and here it is
		//
		// THIS IS THE REPLACEMENT FOR PushImmediate, which pinned a pool worker to a blocking
		// subsystem: that took a worker out of a WORK-STEALING pool and spilled its queue to
		// everyone else, and under a hot set that moves it could not even be kept consistent.
		// Reserving a core and running a plain std::thread buys the same census accounting with
		// none of the invariants -- the scheduler never owns the thread, so nothing can promote it,
		// steal from it, or wait on it.
		//
		// Only affects the AUTO size (Init(0) / Init with poolSize == 0). An explicit poolSize is
		// taken as the caller's own arithmetic and is not adjusted.
		// PRINT WHAT THE SLAB ACTUALLY USED, and what to configure because of it.
		//
		// Call at shutdown, or any time. Per size class it reports the configured capacity, the
		// HIGH-WATER slots the pool genuinely had to make resident, how many are live right now, and
		// whether the class had to grow past its configuration -- then prints a ready-to-paste
		// SetSlabSizes() call sized to what this run measured.
		//
		// HIGH-WATER, NOT LIVE, is the number to size against. Live answers "how many slots are
		// checked out RIGHT NOW", and by the time you read it the peak has been and gone.
		//
		// FREE IN EVERY BUILD, because it adds no counter: refill() prefers recycled slots and only
		// advances the bump cursor when the free list could not fill a batch, so that cursor already
		// IS the high-water mark. Build with -DJLIBSCHED_ALLOC_STATS=ON to get per-class alloc/free
		// RATES underneath it -- which class is hot, and whether the size-class split matches the
		// workload. Those counters are sharded per thread and compiled out otherwise.
		// THE SAME REPORT AS TEXT, because printf is useless in the application that most needs it.
		//
		// A windowed game has no console, so stdout goes nowhere -- and even with one, the process
		// exits before anyone can read the last frame's numbers, which would mean pausing the main
		// thread just to look. Returning the string removes both problems and lets the caller decide:
		//
		//     ImGui::TextUnformatted(TaskScheduler::SlabUsageString().c_str());   // live, in-engine
		//     std::ofstream("slab.txt") << TaskScheduler::SlabUsageString();      // survives exit
		//
		// ReportSlabUsage() below is this plus delivery, and on Windows that includes
		// OutputDebugStringA -- which needs no console AND persists in the debugger's Output window
		// after the process is gone. That is the path a GUI app actually wants, and it is the same
		// one the free-list canary already uses.
		static std::string SlabUsageString(const char* label = "slab usage");

		static void ReportSlabUsage(const char* label = "slab usage");

		static void     SetReservedCores(unsigned n) noexcept;
		static unsigned GetReservedCores() noexcept;
		static bool ReserveIoCore() noexcept;


		// Fibers PER WORKER available to be held by a SUSPENDED task at once -- not a cap on tasks
		// in flight, since only a task that actually suspends holds one. Defaults to 64 standard +
		// 8 heavy per worker, same as always; call this before Init() to change the multiplier.
		// Total capacity is this times the pool's actual worker count, so a multiplier is what gets
		// configured here rather than an absolute total the caller would have to keep in sync with
		// an "auto" pool size it may not know yet.
		//
		// PRE-INIT ONLY, same contract as SetLazyTaskSlab and SetParallelForSerial above. Not
		// resizable after Init(): each fiber registers a permanent slot with the epoch manager and
		// the stack arena is one fixed allocation made at StartPool, so there is no live pool to
		// grow into even if this were called later -- it would just be ignored.
		//
		// This is the actual lever the exhaustion warning in Thread.cpp means when it says "raise
		// standardFiberCount" -- that used to name a local variable inside StartPool with no way to
		// reach it from outside the library. Call this before Init() instead.
		// ONE STACK CLASS: the 512 KB "heavy" class was deleted (nothing ever requested it, and it
		// committed ~127 MB up front). Add a second class back when a workload needs one.
		// PER BAND, because the bands do different jobs: compute workers run ordinary tasks and want
		// normal stacks, K (reserved) workers serve the I/O lane and want TINY ones. POOL TOTALS and
		// cache prefill, NOT a partition -- a K worker that steals ordinary work still draws a normal
		// fiber from the global queue, and exhaustion is global and is a SPIN rather than a failure.
		//
		// DEEP DEFAULTS TO 0: opt-in. At 512 KB apiece, 1 per worker is ~15.5 MB committed on a
		// 31-worker box for a class that may never be bound -- the heavy class's original mistake in
		// miniature. Set before Init().
		static void SetFiberBudget(size_t normalPerComputeWorker = 64,
		                           size_t tinyPerKWorker        = 64,   // 128 at the K=2 default
		                           size_t deepPerComputeWorker  = 0);
		static size_t NormalFibersPerComputeWorker();
		static size_t TinyFibersPerKWorker();
		static size_t DeepFibersPerComputeWorker();
		static size_t StandardFibersPerWorker();
		// Tiny (8 KiB usable, for I/O continuations) and Deep (508 KiB, for deep recursion).
		// BOTH DEFAULT TO 0 -- see the note at their definition; a nonzero default commits memory
		// at startup for every program whether or not it binds one. Set before Init().

		// ---- MIGRATABLE FIBERS: the mode switch, and it is ONE PREDICATE, not two schedulers -----
		//
		// true (THE DEFAULT since 5.0): a resumed fiber may continue on ANY worker. This is what a
		// fiber-task library is FOR -- work that suspends goes back to whichever worker is free,
		// which is the whole reason to have a pool rather than a thread per job. It is also the
		// position the architecture header at the top of this file argues for, and the one the
		// library already PAID for: address-routed frees, a global epoch participant list,
		// fiber-indexed hazard cells. Defaulting to pinned meant every program bought that
		// machinery and then declined to use it.
		//
		// WHAT MIGRATION COSTS: thread-affine cleanup can no longer just happen wherever the fiber
		// ends. It is settled through Fiber's creditor set, one hop per creditor -- and that costs
		// nothing for a fiber that never touched affine state, which today is all of them.
		//
		// false: a fiber resumes only on the worker it was bound to. marl's contract -- "the fiber
		// must belong to this worker" -- and the conservative one. THE REASON TO PICK IT IS
		// thread_local: pinned is what makes a `thread_local` read before a suspension point still
		// valid after it. Migratable mode gives that up, and gives it up SILENTLY -- the value is
		// simply the other worker's, with nothing to catch it.
		//
		// WHY A SETTING RATHER THAN A CHOICE. The two are not equally right for every host. A game
		// that owns every job in the process can enforce "do not cache a TLS-derived value across a
		// suspension point" and take the rebalancing; middleware embedded in a host it cannot audit
		// cannot. Shipping both is more code. It is not two sets of invariants: PINNED IS THE
		// MIGRATABLE PATH WITH THE CREDITOR SET FORCED TO ONE MEMBER, so the mechanisms are shared
		// and this flag is read at the resume-routing decision rather than branched on throughout.
		//
		// MUST BE SET BEFORE Init(). Flipping it under a live pool would strand fibers under the
		// rule they were bound with while new ones follow the other.
		// FiberMode::Migrate (default) or FiberMode::Pin -- see the enum in Task.h for the contract
		// and for why this is not a bool. MUST BE SET BEFORE Init(): flipping it under a live pool
		// would strand fibers under the rule they were bound with while new ones follow the other.
		static void      SetFiberMode(FiberMode m);
		static FiberMode GetFiberMode();

		// The predicate the routing actually asks. Spelled out rather than left as a bool setter, so
		// a call site reads as a question about the mode instead of about a flag.
		static bool FibersMigrate() { return GetFiberMode() == FiberMode::Migrate; }

		// ---- FIBER-LOCAL STORAGE: use this where you would have used thread_local -------------
		//
		// Migratable fibers resume on whichever worker is free, so TLS read before a suspension
		// point is not the same object after it. This is attached to the FIBER, which is the thing
		// that actually survives the wait, so it is correct in BOTH modes and needs no #if.
		//
		//     enum class Fls : uint16_t { Scratch = 0, LastError = 1, COUNT };
		//     static_assert((size_t)Fls::COUNT <= JLib::Fiber::kLocalSlots, "too many FLS slots");
		//
		//     auto* s = TaskScheduler::FiberLocalAs<Scratch>((size_t)Fls::Scratch);
		//     TaskScheduler::FiberLocal((size_t)Fls::LastError) = err;      // raw void*&
		//
		// ONE LOAD AND AN INDEX. No map, no hash, no lock, no allocation -- it has to be cheap
		// enough to use on the path TLS would have been used on, or nobody will.
		//
		// RETURNS nullptr OFF A FIBER, rather than asserting. A Native task and a bare thread have
		// no fiber, and that is a legitimate state rather than a bug -- library code that may run in
		// either context needs to be able to ASK. FiberLocal() returns a reference to a per-thread
		// scratch void* in that case, so a write is harmless and a read gives nullptr; HasFiberLocal()
		// is the explicit check for code that must distinguish.
		//
		// THE SLOT'S MEANING IS YOURS. The library never reads one. It clears them on recycle -- so
		// a fiber never hands its successor stale state -- but it cannot free what they point at,
		// having no type: a slot holding an owning pointer must be released by the task that set it,
		// before that task ends.
		static bool   HasFiberLocal() noexcept;
		static void*& FiberLocal(size_t slot) noexcept;

		template <typename T>
		static T* FiberLocalAs(size_t slot) noexcept {
			return static_cast<T*>(FiberLocal(slot));
		}

		// ---- OWE A RELEASE UNTIL THIS FIBER DIES ----------------------------------------------
		//
		// The slots above hold a FIXED, SMALL set of well-known values. This holds an arbitrary
		// number of objects, each allocated a DIFFERENT way -- one from `new`, one from an arena,
		// one from a pool -- all owed by the same fiber and all released when it is recycled. That
		// combination is the point: `owedKinds` already records which KINDS are outstanding, and
		// this is where the payloads live.
		//
		// THE CALLER SUPPLIES THE NODE, and that is forced rather than chosen. FiberRegistry's
		// dispatch path must not allocate -- a malloc on a death path is the allocation most likely
		// to fail and a dropped cleanup is a resource never given back -- so there is nowhere to put
		// a node the caller did not already own. Put the FiberDebt inside the object being
		// registered and it costs two stores and no memory at all.
		//
		//     struct Buf { JLib::FiberDebt debt; char data[4096]; };
		//     auto* b = new Buf();
		//     TaskScheduler::DeleteOnFiberDeath(b->debt, b);      // `delete b` when the fiber dies
		//
		//     auto* p = MyArena::Alloc(n);                        // any allocator, via the raw form
		//     TaskScheduler::ReleaseOnFiberDeath(node, p, &MyArena::Free);
		//
		// MEMORY ONLY. NOT EPOCHS, NOT HAZARDS. These run on whichever thread recycles the fiber,
		// which is safe for memory -- SlabPool.h documents why freeing on another thread costs cache
		// migration and nothing else -- and WRONG for anything thread-affine. Clearing an epoch slot
		// from the wrong thread un-announces a slot that was never set there and frees nodes under a
		// live traversal. Affine debts belong on FiberRegistry's creditor chain, which runs the
		// release on the owing worker; that is the whole reason that chain exists.
		//
		// OFF A FIBER IT IS REFUSED, and returns false rather than leaking silently: there is no
		// fiber to attach the debt to, so the caller still owns the object and needs to know.
		static bool ReleaseOnFiberDeath(FiberDebt& node, void* obj,
		                                void (*release)(void*) noexcept) noexcept;

		// ---- AN AFFINE DEBT: released ON `holder`, not on whoever recycles --------------------
		//
		// The form above is for MEMORY, which is fungible -- it runs wherever the fiber is recycled
		// because that is harmless. This one is for state only ONE WORKER may retract: a slot in
		// participants[q], a worker-owned hazard cell, a thread-owned handle. Releasing those from
		// the wrong thread is not slow, it is wrong -- clearing an epoch slot on a thread that never
		// set it un-announces a live traversal and frees nodes underneath it.
		//
		// So this tags the fiber with `kind`, which is what routes its death down the creditor chain
		// instead of straight back to the pool, and the chain visits `holder` exactly once. That one
		// visit discharges everything this worker is owed -- which is why the debts are a list.
		//
		// NOTHING IN THE LIBRARY CALLS THIS YET, and that is deliberate rather than incomplete. The
		// two obvious candidates do not need it: epochs are thread-keyed and their guard cannot span
		// a suspend, and the retire bags are thread_local with their own orphan stores. The wiring
		// is here so that a debt which DOES need it is one call away, and so the path is tested
		// rather than discovered later on the death path.
		static bool ReleaseOnWorker(FiberDebt& node, void* obj,
		                            void (*release)(void*) noexcept,
		                            size_t holder, uint32_t kind) noexcept;

		// Release every debt `f` owes to THIS holder, and leave the rest linked for theirs. The
		// creditor chain reaches each worker exactly once, so that visit must discharge all of the
		// kinds it owes -- which is what the list is for. Returns how many ran, so "nothing owed"
		// is distinguishable from "nothing happened".
		static size_t DischargeFiberDebts(Fiber* f, size_t holder) noexcept;

		template <typename T>
		static bool DeleteOnFiberDeath(FiberDebt& node, T* p) noexcept {
			if (!p) return false;
			return ReleaseOnFiberDeath(node, p,
				[](void* q) noexcept { delete static_cast<T*>(q); });
		}

		// ---- WHICH BRANCH DID A RESUME TAKE? (diagnostic, OFF unless JLIBSCHED_REQUEUE_TRACE) ---
		//
		// Requeue has three exits and they are indistinguishable from outside, which is exactly the
		// blindness that let migratable_fiber_test report "0 migrations" for a reason that had
		// nothing to do with the routing: the resume did not come from where the test assumed.
		// A caller ON A WORKER lands in the lane branch and pushes to ITS OWN deque bottom, which
		// is stealable but is also the owner's LIFO end -- so it is re-popped locally and looks
		// exactly like pinning. A caller on a BARE THREAD has no lane and falls to placement.
		//
		// Counting the exits separates those. Compiled out entirely by default: this sits on the
		// resume path and the push path is measured.
		static void RequeueTraceReset();
		static void RequeueTraceReport(const char* label);

		// ---- PushBatch PLACES Wide: EXPERIMENT, DEFAULT OFF ------------------------------------
		//
		// A CALLER HANDING OVER N TASKS AT ONCE HAS DECLARED A BURST, which is the one honest burst
		// signal the scheduler has. It cannot infer one from the push path: the only push-time
		// signal is floor crowding, and that fires all through throughput/1p -- 200,000 no-op tasks
		// where Wide would pay a kernel wake per push against ~900 today.
		//
		// WHAT Wide BUYS, already measured on the burst row: `burst/dflt` grew the floor to 13,
		// got 12 participants and took 9.92 ms; `burst/wide` kept the floor at 2, got 31
		// participants and took 5.07 ms. Twice as fast with NO growth at all, because Wide skips
		// the steer-at-the-awake-floor block and pays the wakes to have every worker running
		// immediately -- see the `Wide DELIBERATELY DOES NOT ENTER HERE` note in PickNextWorker.
		//
		// SO THE BENEFIT IS NOT IN QUESTION; THE COST IS. This flag exists to price it on
		// throughput/bt (16.47 M/s at 64-task chunks) and throughput/mp, where the same wakes buy
		// nothing because the work is short and already stealable.
		//
		// UPGRADES CorePref::Default ONLY. A caller that asked for something specific keeps it --
		// a flag that overrides an explicit choice is a different and worse thing.
		//
		// NOTE it will NOT move the burst rows: those push tasks individually, not as a batch.
		static void SetPushBatchWide(bool on) noexcept;
		static bool PushBatchWide() noexcept;

		// ---- SPIN-HELP ON A BARE THREAD'S WaitFor: DIAGNOSTIC OFF SWITCH -----------------------
		//
		// Default TRUE, which is the shipped behaviour: a bare thread inside WaitFor runs one
		// stolen NATIVE task per poll instead of only waiting. That is work-conserving and it is
		// why `main` counts as a participant rather than a spectator.
		//
		// WHAT TURNING IT OFF IS FOR. It answers "how much of this pool's throughput is actually
		// the CALLING thread?" -- which no timing row can separate on its own, because main's
		// contribution is indistinguishable from the pool being fast. LastBareWaitHelped() counts
		// it; this removes it.
		//
		// IT IS NOT A FAIRNESS KNOB, AND MUST NOT BE USED AS ONE. marl's bound thread also runs
		// tasks when it blocks on a WaitGroup, so both libraries are N workers plus a participating
		// main. Disabling this here makes the comparison N against N+1 -- a different question, and
		// a worse one if it gets quoted as a like-for-like.
		//
		// SCOPE: WaitFor only. The SchedulerMutex acquisition path helps too and is deliberately
		// NOT covered -- helping there is entangled with the reentrancy guards (t_spinHelpDepth,
		// t_heldMutexes) that exist to stop it self-deadlocking, and switching it off is a
		// different experiment with a different blast radius.
		static void SetBareWaitHelp(bool on) noexcept;
		static bool BareWaitHelp() noexcept;

		// ---- PUSH TO ONE WORKER'S RESUME INBOX -------------------------------------------------
		//
		// The resume inbox is the only per-worker queue with the contract this needs: exactly one
		// legal consumer and NO path into a deque, so what is pushed here runs on `worker` and
		// cannot be stolen onto another thread. That is not a preference for cleanup work, it is
		// the entire requirement -- a cleanup job that gets stolen releases thread-affine state on
		// the wrong thread, which is the bug the whole mechanism exists to prevent.
		//
		// EXISTS BECAUSE THE REGISTRY IS A SEPARATE TRANSLATION UNIT. Thread.cpp reaches
		// `resumedInboxes[q]->push(t)` as a field; FiberRegistry cannot, and should not become a
		// friend of everything to do it. One named method is the smaller surface.
		//
		// Returns false if the pool is down or `worker` is out of range. CHECK IT: a dropped
		// cleanup is not a dropped task, it is a resource that is never given back, and the caller
		// is the only one positioned to retry or report.
		static bool PushResume(size_t worker, Task* task);

		// WAKE A WORKER THAT HAS BEEN HANDED CLEANUP, with nothing to push.
		//
		// FiberRegistry::Deliver links the fiber onto the holder's own chain -- there is no task and
		// no queue push -- but the WAKE is still required, and for the same reason PushResume does
		// it: that chain has exactly one legal consumer, so a parked worker with tokens on it never
		// drains them. The cost is not just delayed cleanup. A token cannot be recycled until its
		// chain drains, and a token is embedded in the fiber, so a parked creditor holds a fiber out
		// of the pool. Under pressure that is starvation, not latency.
		//
		// PUSH FIRST, NOTIFY SECOND at every call site -- the reverse loses the wake outright, since
		// the target can observe an empty chain, eat its permit and park with the token arriving
		// just behind it. Same lost-wake shape as the band-skip fix.
		//
		// Returns false if the pool is down or `worker` is out of range; external holders (main, an
		// app's own threads) are not workers and are never notified -- they poll ProcessMainThread.
		static bool NotifyHolder(size_t worker);

		GlobalFiberPool& GetGlobalPool();
		// NAMED events are for a BOUNDED, STATIC set of rendezvous points -- "physics_done",
		// "level_loaded", the handful of names your app knows at compile time. The registry is
		// find-or-insert and never evicts, which is correct for that use: it reaches N entries and
		// stays there.
		//
		// DO NOT MINT A NAME PER OPERATION. A key like "fence_" + counter grows the map without
		// bound, and since every GetEvent holds registryMtx across the lookup (and occasionally a
		// rehash of a huge map), the result is a lock convoy that PRESENTS EXACTLY LIKE A DEADLOCK
		// in a debugger -- several workers piled on one mutex with no visible owner. It takes an
		// hour or so of uptime to show up, so it will not appear in any short test.
		//
		// For a per-operation wait -- a GPU fence value, an IO completion, anything with a fresh
		// identity each time -- use WaitOnEventDirectArmed below instead. It takes a pooled
		// DirectEvent, touches no map and no global lock, and is the reason no eviction policy is
		// needed here: the unbounded case has its own API.
		Event& GetEvent(const std::string& name);
		// Event& overloads: no registry lookup, no global mutex, no string hash. Prefer these on any
		// path that waits on the SAME event repeatedly -- hoist GetEvent() to startup and keep the
		// reference. It stays valid for the process lifetime (the registry owns unique_ptr<Event>, so
		// a rehash moves the pointer not the object, and nothing erases entries).
		void WaitOnEvent(Event& ev);
		void WaitOnEvent(const std::string& eventName);

		// WaitOnEvent, but reports whether this task was cancelled while parked.
		//
		// DELIVERY IS AT THE WAKE, not at the cancel. Event::CancelWaiters marks the task through
		// the waiter index and WAKES it -- an Event may be signalled by a condition that never occurs,
		// so nothing is removed from it and its no-pop invariant holds. The task therefore stays
		// parked until the event actually fires; SignalAll resumes it as usual, and this returns
		// Cancelled instead of Ok.
		//
		// So a cancelled waiter is NOT woken early. If the event never fires it never returns --
		// but an uncancelled waiter on that event does not return either, so cancellation was never
		// what would have rescued it. Waking early would mean removing one waiter from a lock-free
		// stack whose links ARE the tasks, which is the thing Event's header comment forbids.
		//
		// Returns Ok for a task with no cancellation of any kind, which is every existing caller.
		[[nodiscard]] WaitResult WaitOnEventCancellable(Event& ev);
		// Like WaitOnEvent, but runs 'arm' AFTER this fiber is registered as a waiter and
		// marked parkable (WANTS_SUSPEND), and BEFORE it actually suspends. Use it to arm an
		// external wakeup (e.g. a GPU-fence completion callback that will SignalAll this
		// event) with no lost-wakeup race: any signal that fires once 'arm' has run is
		// guaranteed to find a registered, resumable waiter. Must be called from a fiber.
		void WaitOnEventArmed(Event& ev, const std::function<void()>& arm);
		void WaitOnEventArmed(const std::string& eventName, const std::function<void()>& arm);
		// Direct/handle variant of WaitOnEventArmed: no name, no registry, no global lock. Takes
		// a pooled DirectEvent and hands its pointer to 'arm' so the external signaler can wake
		// this fiber via DirectEvent::Signal() with a direct pointer. Preferred for the common
		// "signaler shares context with the waiter" case (fences, IO). Must be called on a fiber.
		void WaitOnEventDirectArmed(const std::function<void(DirectEvent*)>& arm);
		// True if the caller is currently a worker running a task on a fiber (so it is safe
		// to WaitOnEvent / WaitOnEventArmed). False on the main thread or any non-worker.
		bool IsOnFiber();
		void Pause();
		void Resume();
		void Stop(Task* worker_task);
		TaskAllocator* GetAllocator();

		// WaitAll() was REMOVED in 1.3.0. It spun on a global `pendingTasks` atomic that every push
		// and every completion had to maintain -- 24 ns per task in contention, 27% of the whole
		// per-task cost, to serve one method with no callers anywhere. Wait on a WaitGroup instead,
		// which is scoped to the work you actually submitted; "all work everywhere" is not a
		// question a caller can ask meaningfully once more than one system shares the pool.

		// Lets a non-worker caller (e.g. main, while spinning on a WaitGroup/counter) safely
		// help drain the pool instead of pure-spinning. Steals ONE Native task via GetTask(), which
		// vets the task's TaskType AT THE DEQUE (TaskDeque::steal_if) -- a fiber-backed task is never claimed
		// by this fiberless caller at all (it could suspend, and there's no fiber to switch away
		// to), so it stays queued for a real worker. This replaced the old steal-then-Requeue
		// relocation, which was pure contention churn (claim CAS + re-push + notify, task moved
		// nowhere). On a successful steal: runs Execute() inline, then frees with the EXACT SAME
		// sequence Worker()'s fast path uses (DestroyTask(), Free(), EBR tick check) -- required so
		// the slab stays correct; skipping either of these leaks a slab slot.
		// Returns true if it ran a task, false if nothing stealable -- callers should yield()
		// on false to avoid a hot spin.
		bool TryRunStolenNativeTask();

		// Steal-time class compatibility (used with TaskDeque::steal_if -- vet BEFORE claiming, never
		// steal-then-Requeue, which is pure deque contention + worker thrash). Placement policy at steal
		// mirrors push: Default/Any/Wide tasks are stealable by EVERY worker (work-conserving for the
		// common case); explicit P/E tasks are only stolen by a matching-class thief -- corePref is the
		// sole placement authority, including across steals. degenerateTopology (non-hybrid CPU: one
		// class set empty) disables class checks entirely so nothing is ever unstealable.
		// Takes the PREFERENCE, not the task. Steal vetting runs before the thief has claimed
		// anything, so it must not dereference the candidate -- the value arrives in the deque's
		// pointer tag instead (see TaskDeque's StealBits).
		// ---- ALWAYS TRUE NOW THAT P/E ARE GONE ----------------------------------------------
		//
		// It used to make an explicitly-P task stealable only by a P-class thief, which was the
		// steal-side half of class placement. With no classes to match there is nothing to veto:
		// every task is stealable by every worker, which is the work-conserving answer and was
		// already the behaviour for Default and Wide -- i.e. for every task anyone actually pushed.
		//
		// KEPT AS A FUNCTION rather than deleted at the call sites, on purpose. The steal path
		// threads a predicate through `steal_if` and the deque's pointer tag; collapsing that
		// plumbing is a separate change to TaskDeque::StealBits, and doing it in the same commit
		// would mix "P/E is gone" with "the steal predicate changed shape". One of those is easy to
		// review and the other is not.
		static bool StealClassCompatible(CorePref, bool, bool) {
			return true;
		}
		// Convenience for callers that legitimately HOLD the task (owner-side placement checks),
		// where dereferencing is fine. Steal paths must use the CorePref overload.
		static bool StealClassCompatible(const Task* t, bool thiefIsP, bool degenerateTopology) {
			return StealClassCompatible(t->corePref, thiefIsP, degenerateTopology);
		}

		// A C++20 GATE USED TO LIVE HERE, and 5.0 removed the thing it was gating. It asserted that
		// a C++17 translation unit could not pass TaskType::Coroutine -- an enumerator a C++17 TU
		// could NAME (it was declared in Task.h) but never legally produce, since the only thing
		// that made one lived in a header that #errored below C++20. The worker read that type as
		// an OWNERSHIP signal ("the frame frees itself, do not free the Task"), so a counterfeit
		// one ran, leaked, and never signalled its WaitGroup.
		//
		// With the runtime fibers-only there is no third type, no split language mode, and nothing
		// to counterfeit. The whole class of bug is gone rather than guarded.
		// `stack` picks which fiber stack the task binds if it ever binds one -- Standard (60 KB),
		// Tiny (2 pages, for I/O continuations) or Deep (508 KB). It is the LAST parameter and
		// defaults, so every existing call site is unchanged.
		Task* CreateTaskImpl(void(*fn)(void*), void* data, Lane lane, TaskType type,
		                     CorePref corePref, StackClass stack = StackClass::Standard);

		// ---- THE PUBLIC JOB IS A FIBER --------------------------------------------------------
		//
		// DEFAULT CHANGED FROM Native TO Fiber. A public job may suspend -- that is what a job system
		// in this family is FOR -- and a Native task may not, so defaulting to Native handed every
		// caller the one type that cannot wait on anything. The old default was a performance choice
		// that quietly narrowed the API.
		//
		// AND IT IS WHAT MAKES MEMORY RECLAMATION ONE STORY INSTEAD OF THREE. Only a context that can
		// suspend and RESUME ON ANOTHER THREAD needs to be tracked for cleanup, because only it can
		// leave thread-affine state behind on a worker it has since left. A fiber can; a Native task
		// cannot (it runs to completion where it started). So if every public job is a fiber, the
		// fiber registry covers the whole public surface and nothing else needs a death hook.
		//
		// THE COST IS A CONTEXT SWITCH, and it is 9.2 ns since the AVX-transition fix (85.8 ns
		// before it). A fiber that never suspends still runs to completion exactly like a Native
		// task; it just paid for the option.
		//
		// Native has NOT gone away -- see CreateInternalTask. It is no longer the thing a caller
		// gets by accident.
		Task* CreateTask(void(*fn)(void*), void* data, Lane lane = Lane::Normal, TaskType type = TaskType::Fiber,
		                 CorePref corePref = CorePref::Default, StackClass stack = StackClass::Standard) {
			return CreateTaskImpl(fn, data, lane, type, corePref, stack);
		}

		// ---- INTERNAL JOBS: NATIVE ON PURPOSE, AND THE REASONS ARE NOT INTERCHANGEABLE ----------
		//
		// Two kinds of caller belong here and they want Native for different reasons:
		//
		//   PARALLELFOR LEAVES -- a grain is a slice of one range with no wait in it. Giving each a
		//   fiber would bound grain concurrency by the fiber pool (coreCount * StandardFibersPerWorker,
		//   64 KB of stack apiece) and put grain acquisition on the path that already deadlocked once
		//   under nested ParallelFor: AcquireFiber fails, Requeue, spin. Grains are the one workload
		//   where the count is unbounded and the body provably never waits.
		//
		//   CLEANUP HOPS -- these must run to completion ON THE WORKER THEY WERE SENT TO. A fiber
		//   could suspend mid-release and resume elsewhere, which is precisely the migration the
		//   cleanup exists to repair. Here "cannot suspend" is the REQUIREMENT, not a concession.
		//
		//   MAIN-AFFINITY DAG NODES -- and this one is PUBLIC, which is why Native stays selectable
		//   rather than becoming private. ProcessMainThread runs a task with `t->Execute()` on the
		//   main thread's own stack: no fiber is bound, so there is nothing to switch away to and a
		//   suspension inside one fail-fasts with no message. TaskDAG::CreateMainNode rejects a
		//   Fiber task outright for exactly this reason, and that guard got much easier to trip the
		//   moment Fiber became the default.
		//
		// SO NATIVE IS NOT PRIVATE -- it is no longer the thing a caller gets by ACCIDENT. Asking
		// for it explicitly is asking for "this may never wait on anything", which is a real and
		// occasionally correct thing to want. This overload exists so the library's own internal
		// jobs say so at the call site instead of passing an enum whose meaning is a footnote.
		// STACKCLASS MATTERS HERE EVEN THOUGH THIS IS NATIVE, and that is not obvious. A Native task
		// is fiberless on the floor -- but a RESERVED worker binds a fiber to one anyway, which is
		// exactly the path an I/O continuation takes. So `StackClass::Tiny` on an internal task is
		// the difference between a completion parking on 8 KB and parking on 60 KB.
		Task* CreateInternalTask(void(*fn)(void*), void* data, Lane lane = Lane::Normal,
		                         CorePref corePref = CorePref::Default,
		                         StackClass stack = StackClass::Standard) {
			return CreateTaskImpl(fn, data, lane, TaskType::Native, corePref, stack);
		}


		// Fiber by default, for the reasons on the raw overload above.
		template<typename F>
		auto CreateTask(F&& f, Lane lane = Lane::Normal, TaskType type = TaskType::Fiber,
		                CorePref corePref = CorePref::Default, StackClass stack = StackClass::Standard) {
			using L = LambdaTask<std::decay_t<F>>;
			// NO SIZE CEILING, as of 4.0.1. A capture larger than the biggest slot used to be a
			// COMPILE ERROR, which made the task path stricter than the coroutine path for no reason
			// anyone could defend: a coroutine frame over 256 bytes has always fallen back to the
			// global heap (detail::FrameAlloc), and a lambda body is the same kind of object with
			// the same lifetime -- allocated once, destroyed exactly once, through a handle the
			// scheduler owns. The only difference was that one of them was allowed to be big.
			//
			// The slab stays the fast path and the overwhelmingly common one; this only removes a
			// wall. Disposal needs no size flag because TaskAllocator::Free routes by ADDRESS.
			// Concrete size is a compile-time constant here, which is also why size-classing the
			// task path would be nearly free: the class could be picked at compile time.
			detail::RecordTaskSize(sizeof(L));

			static_assert(alignof(L) <= 16, "lambda over-aligned for the slot");

			// sizeof(L) is a compile-time constant, so the class is chosen at compile time and is correct
			// for ANY capture size -- a big lambda still lands in the 256-byte class. Nothing here
			// depends on knowing the size distribution in advance.
			// AllocSized returns null for two different reasons and both end up here: the body does
			// not fit any class, or every class that would fit is exhausted. Neither is a reason to
			// fail a task the caller has already written -- the heap is slower, not wrong, and a
			// task that runs late beats one that never runs.
			void* mem = taskAllocator.AllocSized(sizeof(L));
			if (!mem) mem = ::operator new(sizeof(L));
			if (!mem) return static_cast<L*>(nullptr);
			L* t = ::new (mem) L(std::forward<F>(f));
 			t->lane = lane;

			t->type = type;
			t->corePref = corePref;
			t->stackClass = stack;
			// ~LambdaTask is empty and its only member is the functor, so the destructor has work
			// to do only when the CAPTURES do. Non-capturing lambdas and captures of scalars or
			// raw pointers -- the overwhelming majority of task bodies -- skip the virtual call.
			t->trivialDtor = std::is_trivially_destructible_v<std::decay_t<F>> ? 1 : 0;
			return t;
		}

		// Lambda form of CreateInternalTask -- see the raw overload for why these stay Native.
		// ParallelFor's three leaf paths (flat chunks, cursor lanes, the lazy splitter) are the
		// callers, and their bodies provably never wait.
		template<typename F>
		auto CreateInternalTask(F&& f, Lane lane = Lane::Normal, CorePref corePref = CorePref::Default,
		                        StackClass stack = StackClass::Standard) {
			return CreateTask(std::forward<F>(f), lane, TaskType::Native, corePref, stack);
		}

		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushTarget(t);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(uint8_t cpu_affinity, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushTarget(t, cpu_affinity);
		}
	private:
		// ---- CONSTRUCTION NO LONGER STARTS THE POOL, AND THAT IS A RACE FIX -------------------
		//
		// It used to be `TaskScheduler(size_t) { StartPool(poolSize); }`, so `Init`'s
		// `instance = new TaskScheduler(poolSize)` started worker threads INSIDE the constructor --
		// and `instance` is not assigned until the constructor returns. Workers reached GetBands(),
		// read `instance`, and raced main's write of it.
		//
		// FOUND BY TSAN, first run:
		//   Write  main   TaskScheduler::Init()      TaskScheduler.cpp:460
		//   Read   T1     TaskScheduler::GetBands()  TaskScheduler.cpp:2574 <- Thread::Worker()
		//
		// It was not only a data race on a plain pointer. GetBands does
		// `const size_t nw = instance ? instance->workers.size() : 0;` and then clamps K and F
		// against nw -- so for the whole startup window nw read ZERO and THE CLAMPS DID NOT APPLY,
		// while workers were already making band decisions with the result.
		//
		// Init now assigns `instance` and THEN calls StartPool, so the pointer is published before
		// any worker thread exists and the publication happens-before every worker's first read by
		// thread creation. Nothing else constructed a TaskScheduler or called StartPool -- one
		// caller each -- so this splits cleanly.
		TaskScheduler() = default;

		// ---- TEARDOWN IS NOT PUBLIC API. It happens once, at process exit. -------------------
		//
		// Join() WAS public through 4.0.2, and what it advertised was not true. It reads as
		// "tear the pool down, and by implication bring it back" -- but there has never been a way
		// back: Init() throws on a non-null `instance`, Join() does not null it, and StartPool is
		// private with the constructor as its only caller. So the cycle Join()'s own comments
		// describe could not be reached from outside the class. Nothing was broken by that, because
		// nothing outside the test suite ever called it -- Game01 does not, and the task-size
		// reporter at the bottom of this library exists in its at-exit form precisely because
		// hooking Join produced a run that looked successful and wrote nothing.
		//
		// Private and destructor-only makes the real lifetime the stated one: the pool is
		// process-lifetime, it drains exactly once, and there is no half-supported restart to
		// reason about. That is a BREAKING CHANGE and it is why this release is 5.0.0.
		void Join();

		// WHY THIS TYPE EXISTS AT ALL. Making Join private is only half of it: `instance` is a raw
		// pointer that was new'd and never deleted, so ~TaskScheduler() -- the one caller of Join()
		// that remains -- never ran in any program. Private without this would not have made
		// teardown destructor-only, it would have made teardown NEVER HAPPEN, and the drain would
		// have become unreachable code that still looked load-bearing.
		//
		// WHAT IT COSTS, stated plainly because it is a real hazard and not a theoretical one. The
		// drain resumes parked frames so they can unwind, and those frames run USER destructors --
		// during static destruction, when statics in other translation units may already be gone.
		// A frame that touches one of those on the way out is a use-after-destruction that will
		// look like a crash at exit with no obvious cause. The alternative was leaving every parked
		// frame abandoned, which is the thing Join() exists to prevent, so this is the lesser of
		// two bad endings rather than a clean one.
		//
		// A nested class because `instance` and Join() are both private; a file-static struct in
		// the .cpp would need friendship to reach either.
		struct AtExitDestroyer { ~AtExitDestroyer(); };
		static AtExitDestroyer atExitDestroyer;

		// ---------- former SharedQueues state ----------
		// (`nextId` removed in 1.3.4: a process-wide atomic counter whose only reader,
		//  Thread::GenerateID(), had no callers anywhere. Never executed, so it cost no time -- but
		//  it sat in the shared struct implying task IDs existed. Nothing assigns or reads one.)
		std::atomic<bool> paused{ false };

		// THE WORK-STEALING DEQUES, one per worker plus one for the non-worker lane at the end.
		// Named `loPri` until 5.0.1, which was a contrast with a `lane` array that no longer
		// exists -- there is one deque per worker now, so the priority half of the name described
		// nothing. Priority survives where it is still real: laneInboxes vs normalInboxes.
		//
		// COMMENTS THROUGHOUT STILL SAY "loPri", and roughly a third of those mean the INBOX, which
		// legitimately keeps that name. Sorting the two apart is a reading job rather than a rename,
		// so it has not been done in the same pass that could not be checked by the compiler.
		std::vector<std::unique_ptr<TaskDeque>> deques;

		// ---- THE lane DEQUE IS THE BACKUP QUEUE, AND IT IS BACK (5.0.0) ---------------------
		//
		// It was deleted on the argument that "a Chase-Lev deque exists to be stolen from, nobody
		// steals lane work, and a reserved worker wants an inbox and nothing else on the path that
		// exists for latency." The first half of that is right and is preserved below. The second
		// half assumed something that did not survive: that nothing would ever need to reach lane
		// work except its owner.
		//
		// AN INBOX HAS EXACTLY ONE LEGAL CONSUMER. That is not a tuning choice, it is what an MPSC
		// is -- so lane work is precisely as reachable as its owner, and an owner inside a task body
		// is reachable by nobody. Every fix that keeps the lane inbox-only has the same shape: move
		// the work to a DIFFERENT single consumer, which relocates the failure rather than removing
		// it. Spill it to worker 3's inbox and worker 3 enters a long body, and it is stranded
		// again. A deque has no such state: anyone may steal from it, at any time.
		//
		// THE ESCAPE QUEUE WAS THE OTHER ANSWER AND IT WAS REVERTED (it cost throughput on the
		// bench and bought nothing measurable back). Something has to serve that role, and this is
		// the structure that already does it for ordinary work.
		//
		// THE LATENCY PROPERTY IS KEPT, because the deque is not on the fast path. A worker pops
		// its lane INBOX first and runs that task with no staging at all -- which is the whole of
		// what "no unloading step" bought, and it is what the io p50/p99 numbers came from. Only the
		// REMAINDER is staged here, at dispatch, in one batch, and only when there is a remainder.
		// The common case -- one completion arrives, one worker takes it -- never touches this.
		// THE LANE DEQUE IS GONE (5.0.1). `std::vector<std::unique_ptr<TaskDeque>> lane` sat here:
		// one Chase-Lev ring per worker, parallel to loPri, holding lane work that a reserved
		// worker had unloaded from its inbox on its way into a task body.
		//
		// A Chase-Lev deque exists so OTHER threads can steal from it. Nothing may take lane work
		// from a reserved worker -- the lane is an MPSC inbox with exactly one legal consumer -- so
		// the deque was serving a use it structurally could not have. What it bought was a rescue
		// for work queued behind a LONG body on a reserved core, and the answer to that is the
		// contract (do not run long bodies on K) plus HiPriSpillTarget, which stops the backlog
		// forming at push time instead of unloading it afterwards.
		//
		// What went with it: the lane steal sweep in the steal loop, the staging unload at
		// dispatch, the per-thief lane probe, and 32,768 ring slots per worker.

		// Ingress backpressure bookkeeping. All three are no-ops unless a submit limit is set.
		static void NoteInboxPush(size_t n);
		static void NoteInboxDrain(size_t n);
		static void ApplyIngressBackpressure();

		// ---------- the NON-WORKER LANE ----------
		// loPri/lane carry ONE EXTRA deque pair past the workers, at index `nonWorkerLane`
		// (== workers.size(), fixed by StartPool). Everything else -- inboxes,
		// the P/E sets and PickNextWorker -- stays worker-indexed.
		//
		// WHY IT EXISTS. Demand-driven splitting (ParallelFor since 1.4) works by publishing a split onto
		// the SPLITTER'S OWN deque and taking it straight back if nobody stole it -- measured at
		// 17.8 ns, versus ~85-105 ns for a real cross-thread dispatch. A worker has a deque to do
		// that with. Main did not, so a ParallelFor called from the main thread -- which is the
		// normal case -- had nowhere to publish, and the alternative was to hand the whole range to
		// one worker and let the tree fan out over log2(N) STEAL HOPS before the pool filled up.
		// One deque removes that ramp entirely: every worker can steal directly from main's lane,
		// in parallel, starting from the first split.
		//
		// SINGLE OWNER, ENFORCED. A Chase-Lev deque has exactly one pusher/popper by construction,
		// so two non-worker threads splitting at once would corrupt it. `nonWorkerLaneClaimed` is
		// the claim; a caller that loses it falls back to the cursor path rather than waiting, so
		// a second non-worker thread degrades to the 1.4 behaviour instead of blocking.
		//
		// Thieves treat it as one more victim and nothing else: it is probed after the topology
		// phases (it has no cache locality to anybody) and before the global random fallback.
		size_t nonWorkerLane = 0;
		std::atomic<bool> nonWorkerLaneClaimed{ false };

		// Everything a lazy split needs that does NOT change as the recursion descends. Held on the
		// root caller's stack and passed down by pointer, which is sound for exactly the reason
		// the cursor path is: ParallelFor BLOCKS, so the frame outlives every task
		// spawned under it. A non-blocking variant would need this refcounted on the heap.
		struct LazyRangeState {
			std::function<void(int, int)>* func;
			WaitGroup* wg;
			int grain;

			// Has this RANGE already woken a thief? Shared by every split of the range, because the
			// wake we care about is one per range, not one per split -- see RunLazyRange's wake
			// block for why the FIRST publish is the one that most needs it and was the one skipped.
			std::atomic<bool> wokeForRange{ false };
		};
		// The deque the CALLING thread may publish onto, or nullptr if it has none. Resolved per
		// invocation and never cached across one: a split that gets stolen resumes on a different
		// thread, and it must then publish onto THAT thread's deque, not the one it came from.
		TaskDeque* LaneForCurrentThread();
		size_t LaneIndexForCurrentThread();
		void RunLazyRange(int lo, int hi, LazyRangeState* st);
		std::vector<std::unique_ptr<TaskMPSCQueue>> normalInboxes;
		// ---- THE SHARED LANE INTAKE. One queue, K consumers. ----------------------------------
		//
		// THE PROBLEM IT SOLVES IS REACHABILITY, NOT CAPACITY. A per-worker lane inbox is MPSC with
		// exactly ONE legal consumer, so a completion queued behind a worker that just entered a
		// body cannot be taken by an idle sibling -- and the MPMC-prize counters measured that
		// directly: ~100% of stranded backlogs had an idle worker somewhere in [0,K+F) while only
		// ~42% had one in [0,K). The threads to run that work already existed. Adding reserved
		// workers adds capacity to a reachability problem, which is why K=2 and K=4 measured the
		// same to within noise.
		//
		// THIS IS NOT THE CHASE-LEV THAT WAS REMOVED ON 8-30, and the distinction matters because
		// the rule reads like it forbids this. That was a PER-WORKER deque, and a Chase-Lev exists
		// entirely to be stolen from -- useless on workers that neither steal nor are stolen from,
		// and it cost a push-then-pop hop on the one path that exists for latency. This is a SHARED
		// intake: one queue for the whole band, which is the property the inbox structurally cannot
		// have. "Reserved workers use the MPSC inbox directly" stays true for everything else.
		//
		// NOT STRICT MPMC, AND IT DOES NOT NEED TO BE. Strict global FIFO across all completions is
		// the wrong property anyway: two sockets do not owe each other an ordering. Per-socket order
		// is the coroutine that owns the socket -- IoStream keeps exactly one transfer per direction
		// in the kernel -- so ordering is a property of the CHAIN, not of the runqueue, and no
		// scheduling decision here can reorder a stream against itself.
		moodycamel::ConcurrentQueue<Task*> laneIntake;

		std::vector<std::unique_ptr<TaskMPSCQueue>> laneInboxes;
		// RESUMED FIBERS, ONE QUEUE PER WORKER, DRAINED ONLY BY ITS OWNER AND NEVER INTO A DEQUE.
		//
		// THE POINT IS THE "NEVER INTO A DEQUE" HALF. The other two inboxes drain into the owner's
		// deque, which is the correct thing for a task -- it becomes stealable and the pool
		// rebalances. But a resumed fiber must NOT become stealable: it is pinned to the worker it
		// was bound on (Fiber::homeWorker), and a thief taking it would migrate it, which is the
		// thread_local hazard that pinning exists to remove. So resumed fibers get a queue that has
		// no path into the deque at all, and the worker runs them straight out of it.
		//
		// A PREDICATE COULD NOT DO THIS JOB, which is why it is a separate structure rather than a
		// filter on the existing drain. A thief must decide BEFORE it claims a task, and the only
		// thing it may read before claiming is the deque's tag -- TaskDeque::StealBits -- which
		// carries corePref and type and is immutable by contract ("written exclusively by
		// CreateTask... nothing mutates them afterwards"). "Already holds a fiber" is mutable state
		// acquired long after CreateTask, so it cannot ride in the tag, and dereferencing an
		// unclaimed task to ask is the exact lifetime bug StealBits was introduced to fix.
		// Separating by QUEUE is the only place the distinction can live.
		//
		// EVERY PARK PREDICATE MUST CONSULT THIS. There are four (the pre-park recheck, the Event
		// sleep predicate, the condvar wait predicate, and the pre-sleep drain) plus the hiPriStray
		// work-presence check. A worker that parks without checking this queue sleeps on runnable
		// work that no other worker is permitted to take -- an unrecoverable hang, not a stall,
		// and the same signature as the loPri-inbox hang already recorded in Worker().
		std::vector<std::unique_ptr<TaskMPSCQueue>> resumedInboxes;

		// NO PER-HOME FIBER LIST. An earlier pass in this design built one -- a padded Treiber stack
		// per worker, of the fibers homed to it -- to answer "what does worker H still owe?". It was
		// removed on the same day it was written, and the reason is worth keeping: THE POOL IS
		// ALREADY THAT CENSUS. GlobalFiberPool holds one contiguous `std::vector<Fiber>` indexed by
		// Fiber::poolIndex, reserved and leaked so it never reallocates, so enumerating fibers is a
		// scan over TotalCount() and filtering by creditor is a mask test. A maintained list would
		// have been a second copy of that, kept in step by hand, with a CAS on the fiber acquire
		// path paying for it.
		//
		// The remaining question a list could have answered -- who owes what -- is answered on the
		// fiber instead, by Fiber::creditors. See there for why it is a SET and not one home.
		// The width tie-in (Fiber::kCreditorWords vs kMaxHintQueues) is asserted in
		// TaskScheduler.cpp, not here: kMaxHintQueues is declared further down this class, and a
		// class-scope static_assert is not a complete-class context, so it cannot see it yet.

		static GlobalFiberPool* globalPool;
		// -----------------------------------------------

		// ---- loPri starvation prevention: steal fairness ----
		// After kStealFairnessWindow consecutive lane steals, GetTask() forces a loPri scan so a
		// steady stream of lane work can't starve loPri tasks. (There used to also be age-based
		// promotion -- boost old loPri tasks to lane -- but it's redundant now that stealing is
		// single-item: a stolen task runs immediately, so the steal itself un-starves it.)
		//
		// PER THREAD, NOT PER SCHEDULER -- see the definition in TaskScheduler.cpp. This was a plain
		// `int` member on the singleton, read-modify-written by GetTask() from every thread that
		// spin-helps, which is a genuine data race and ThreadSanitizer says so (2026-08-17). It is
		// also the wrong shape for what the counter means: the window is a property of ONE
		// stealer's recent history, and sharing it made N concurrent helpers trip it N times faster
		// than intended. Thread.cpp's `consecutiveMisses` backoff counter is thread_local for
		// exactly these two reasons and this now matches it.
		static thread_local int consecutiveHiPriSteals;
		static constexpr int kStealFairnessWindow = 8; // after 8 lane steals, force a loPri scan
		uint64_t GetCurrentTimeMs() const;
		// ----

		// ---- Priority inheritance for locks (prevent inversion deadlock) ----
		// Priority boosts now stored directly on Task.priorityBoost (no lock needed)
		// ----


		void RunCounted(WaitGroup& wg, Task* t);
		static size_t GetSafeTC();
		// Steals ONE task (lane-then-loPri, with steal fairness) for a non-worker helper. nullptr
		// if nothing stealable. See definition.
		Task* GetTask();
		void StartPool(size_t poolSize);
		bool PushTarget(Task* task, uint8_t cpuaffinity = 0);
		// `lane` selects WHICH SET is rotated, and that is what makes the lane invariant structural
		// rather than a convention every call site has to remember. A lane task rotates the hot
		// workers only; everything else rotates the ordinary ones only. One branch, one place, and
		// no caller can route a lane task somewhere nothing serves it.
		int PickNextWorker(CorePref pref = CorePref::Default, Lane lane = Lane::Normal);
		// Picks a worker from the requested class set (P/E), SPILLING to the other class if unavailable;
		// Default/Any/Wide (and non-hybrid / all-pinned) use the original full-pool round-robin. Placement
		// is governed SOLELY by CorePref -- lane is queue order only, never consulted for placement.
		// Preference is a hint -- never a constraint.

		// NOTE: an external-submitter fan-out cap was tried here and REMOVED. See CHANGELOG 1.1.1.
		// It made single-producer submission much faster and burst parallelism much worse, and it
		// could livelock against the pinned-core retry loop PushTarget used to carry, which was safe
		// with thirty-one candidates and not with four (that loop went with PushImmediate). If you are
		// tempted to reintroduce it, the useful version is preferring workers that are already
		// AWAKE, not capping how many exist.

		// ---------- topology-aware steal biasing ----------
		// Queried ONCE at StartPool() time via GetLogicalProcessorInformationEx -- real
		// hardware topology, not an assumption from the sequential affinity scheme (worker
		// qIndex i is pinned to logical CPU i+1, main sits on logical CPU 0; that mapping alone
		// doesn't tell you which logical CPUs actually share a core or an LLC on THIS machine).
		void BuildTopology(unsigned int num_workers);
		// clusterMates[qIndex] -- other worker qIndexes sharing this worker's last-level cache
		// domain, EXCLUDING its direct SMT sibling (that's handled separately below, since it
		// needs the extra "only if idle" check). Tried first, in random order, before falling
		// back to the existing global-random steal.
		std::vector<std::vector<int>> clusterMates;
		// Class-split views of clusterMates (built in BuildTopology once isPCore is known): a thief
		// probes victims IT CAN STEAL FROM first -- same-class mates, then the idle SMT sibling (same
		// physical core = same class by construction), then foreign-class mates as the last local
		// phase, then a global random fallback (also same-class-first). With every task at Default
		// this only reorders identical coverage; with class-pinned work it kills futile decline
		// probes at the source (scan order matches steal legality) and raises steal hit rate.
		// Non-hybrid: matesOtherClass is empty everywhere -> behavior identical to the classic order.
		std::vector<std::vector<int>> matesSameClass, matesOtherClass;
		// siblingQIndex[qIndex] -- the OTHER worker qIndex sharing this worker's physical core
		// (SMT sibling), or -1 if none (no SMT, or the sibling logical CPU isn't a pool worker
		// -- e.g. it's main's). Only stolen from if idle (see Thread::busy) -- a busy SMT
		// sibling shares this worker's execution ports, so stealing its work doesn't recruit
		// any additional throughput, just adds queued work to an already-contended core.
		std::vector<int> siblingQIndex;

		// llcMaskOfWorker[qIndex] -- the set of logical CPUs sharing this worker's last-level cache,
		// as a bitmask. 0 if topology was unavailable. POSIX ONLY in effect: it exists so the Ideal
		// policy can bind a worker to its whole LLC DOMAIN rather than to one core.
		//
		// Windows expresses "keep locality without rigidity" through SetThreadIdealProcessor, a hint.
		// Linux has no hint -- affinity is all or nothing per CPU -- but sched_setaffinity takes a
		// MASK, so the same intent is expressed by binding to the domain instead of the core. The
		// kernel may then place the thread on any core in that domain, so a wake lands on one that
		// is already awake instead of waiting for a specific parked core, which is where hard
		// pinning's ~45% wake-latency cost came from.
		//
		// The mask is exactly as tight as the hardware warrants. On MULTI-L3 parts (Ryzen CCDs,
		// Threadripper, EPYC) it genuinely binds, which is where it matters -- migrating across
		// cache domains costs inter-die latency per steal and makes clusterMates describe a machine
		// that does not exist. On SINGLE-L3 parts the domain is every CPU, so it binds nothing:
		// correct rather than missing, since there is no domain to protect and binding would only
		// add the rigidity that measured ~45% worse. UNMEASURED on real Linux hardware -- WSL
		// virtualises topology and has no real core parking.
		std::vector<topology::CpuMask> llcMaskOfWorker;
		// isPCore[qIndex] -- 1 if this worker is pinned to a PERFORMANCE core, 0 for an EFFICIENCY core
		// (Intel hybrid, e.g. i9-13900K = 8 P + 16 E). Derived in BuildTopology from each core's
		// PROCESSOR_RELATIONSHIP.EfficiencyClass (highest class present = P). Non-hybrid CPU -> all 1
		// (harmless). DATA ONLY for now: nothing schedules on it yet. Foundation for P/E-aware routing
		// (lane->P, loPri/bulk->E) + a P/E-aware core reserve -- needed manually because the pool is
		// HARD-PINNED (Thread::StartWorker SetThreadAffinityMask), so the OS can't place work P/E for us.
		std::vector<char> isPCore;
		// isPCpu[logical CPU] -- P/E class of every logical processor, same EfficiencyClass
		// derivation as isPCore but indexed by CPU, not worker. Needed because TryRunStolenNativeTask's
		// callers include NON-worker, possibly UNPINNED threads (main, or any app thread hitting a
		// SchedulerMutex/SchedulerConditionVariable spin): their class can't be assumed -- it's looked
		// up via GetCurrentProcessorNumber() at steal time ("this Native task would run HERE, right now").
		// Workers keep the cheaper static isPCore[qIndex] lookup (hard-pinned, class never changes).
		std::vector<char> isPCpu;
		// -----------------------------------------------

		static TaskScheduler* instance;
		// 1M tasks by default; see SetSlabSizes to change it. Eager unless SetLazyTaskSlab(true)
		// was called before Init -- see that setter for why the default is the expensive one.
		// Built from the per-class sizes -- see SlabSizes.
		TaskAllocator taskAllocator{ CurrentSlabSizes().slots256, CurrentSlabSizes().slots128, CurrentSlabSizes().slots80,
		                             CurrentSlabSizes().slots64, LazyTaskSlabEnabled() };

	public:
		// ABI LAYOUT CANARY. Not used by anything at runtime; its OFFSET is the payload.
		//
		// It sits immediately after taskAllocator, so it moves whenever any member above it is
		// added, removed, resized or reordered -- which is exactly the change class that the
		// stale-library guard needs to see and that sizeof() cannot report. 1.4 shifted
		// taskAllocator by 8 bytes with sizeof(TaskScheduler) pinned at 1664, and the guard matched
		// straight through it.
		//
		// It is PUBLIC so the guard can take offsetof() from a free function with no access to this
		// class. The alternative -- a member function that reports its own offsets -- cannot work:
		// it would have external linkage, the linker would fold the copies from the library and the
		// application into one, and both sides would then read the SAME layout and always agree.
		// A guard that compares a value against itself is worse than no guard, because it reports
		// success.
		struct AbiCanary { char unused; };
		AbiCanary abiCanary{};

	private:
		// Head of the intrusive primitive chain -- see WaitPrimitive. Guarded by its own mutex
		// because construction and destruction of locks happen on any thread at any time, and this
		// must not contend with anything on the task path.
		WaitPrimitive* primitivesHead = nullptr;
		std::mutex     primitivesMtx;

		std::unordered_map<std::string, std::unique_ptr<Event>> eventRegistry;
		std::mutex registryMtx;
		EventPool eventPool{ 1024 };   // pooled DirectEvents for WaitOnEventDirectArmed
		std::atomic<bool> poolActive{ false };
		// TWO STEAL HINTS, because the pool holds two kinds of queue content with OPPOSITE
		// requirements, and one bit cannot mean both.
		//
		//   BACKLOG      "this worker has substantial excess work." Stealing it is an OPTIMISATION
		//                and only pays when the owner is demonstrably behind, since migration costs
		//                a cache miss on the task's working set. Threshold-maintained by the owner,
		//                so it is written only on crossings -- almost never for a queue that stays
		//                deep or stays shallow.
		//   PARALLELISM  "this work was deliberately produced for parallel execution." Stealing it
		//                is the LOAD-BALANCING MECHANISM, not an optimisation -- ParallelFor has no
		//                cost model and steals are what divide the range. A single split must be
		//                visible the instant it exists.
		//
		//   stealable = backlog || parallelism
		//
		// WHY BOTH ARE NEEDED, measured: a backlog-only hint cut remote probes 46x (9,164 -> 200,
		// every probe productive) and simultaneously took ParallelFor from 7.49x to 0.93x -- SLOWER
		// THAN SERIAL -- because shallow splits never reach any threshold. Thresholds of 2, 4, 8 and
		// 16 all did it. Threshold 1 is not a fix either: that is "any non-empty queue", which
		// summons every idle thief onto one deque for ONE task and measured 22,000 probes against a
		// 9,164 baseline, with the owner losing races for work it was about to run warm.
		//
		// The herd is correct exactly when there is work for a herd. PARALLELISM says so; BACKLOG
		// says so; a bare non-empty queue does not.
		static constexpr size_t kStealHintDepth = 8;

		// ---- THE HINT BITMAPS ARE MULTI-WORD, and that is a correctness property, not a size ----
		//
		// One bit per queue index. A SINGLE 64-bit word used to be the whole story, and the failure
		// it produced was not "large pools lose an optimisation" -- it was a CLIFF, in the direction
		// that hurts most:
		//
		//   deques.size() is num_workers + 1 (the non-worker lane), so the edge sat at 64 WORKERS,
		//   not 64 CPUs. Init(0) takes hardware_concurrency - 1, so a 64-thread machine landed on
		//   exactly 64 and kept its hints, while a 128-thread Threadripper got 127 workers and
		//   MaybeStealable disabled the hints WHOLESALE -- every idle worker back to probing every
		//   victim, at the 0.2-0.9% hit rate the hints were built to fix. Probe traffic goes as N^2,
		//   so the machine where the hint matters most was the one that switched it off.
		//
		// The wholesale disable was itself correct given one word: covering only 0..63 would starve
		// every queue above it, since those could never advertise and so would never be stolen from.
		// The fix is more words, never a narrower guarantee.
		//
		// FOUR WORDS = 256 QUEUES, matching topology::CpuMask::kMaxCpus so the two ceilings cannot
		// drift apart. Cost is bounded by the POOL, not by the constant: readers walk
		// ceil(deques.size()/64) words, so a 32-worker pool touches exactly one and pays what it
		// always did. Above 256 the guards below fall back to "always a candidate", which is the old
		// probe-everything behaviour -- correct, just unoptimised, and on hardware nobody has yet.
		static constexpr size_t kHintWords     = 4;
		static constexpr size_t kMaxHintQueues = kHintWords * 64;   // 256

		// ---- WHICH WORKERS ARE AWAKE. One bit per worker, maintained by the worker itself. -------
		//
		// THE PROBLEM IT SOLVES, and the measurement that demanded it: single-producer throughput
		// SCALES BACKWARDS -- 10.97 M/s at 2 workers, 4.46 at 4, 2.20 at 31, for no-op tasks. More
		// workers is strictly less throughput. That is not contention; it is a per-push cost that
		// grows as the pool gets IDLER. Placement was pure round-robin, so with 31 workers a single
		// producer lands on a different worker nearly every push, most of them parked, and each of
		// those pushes buys an OS wake. With 2 workers the same pushes hit two saturated workers
		// that are always AWAKE, NotifyWorker's skip fires, and the push is nearly free.
		//
		// So: pick a worker that is ALREADY RUNNING and the wake never happens. This is what marl
		// does with its spinningWorkers[] exchange -- steering each enqueue at a worker already
		// spinning is why a serial ping-pong there wakes nobody -- and it is the one structural
		// difference that survived every other comparison.
		//
		// WRITTEN ONLY ON A TRANSITION, which is what makes it affordable. A worker sets its bit
		// when it resumes searching and clears it when it parks; both are already expensive moments.
		// Pushers only ever READ it, and under a steady workload the words sit shared-clean.
		//
		// A STALE BIT IS SAFE IN BOTH DIRECTIONS, which is why these are relaxed. Stale-set means a
		// push chooses a worker that just parked -- NotifyWorker wakes it, exactly the old
		// behaviour. Stale-clear means a push skips a worker that just woke -- it goes to another
		// awake one. Neither loses work; both are the cost this is trying to reduce, not a
		// correctness question.
		//
		// ALL BITS CLEAR MEANS THE WHOLE POOL IS PARKED, and then there is no awake worker to
		// prefer -- placement falls back to round-robin and the wake is genuinely necessary.
		std::atomic<unsigned long long> awakeHint[kHintWords]{};
		// ---- AWAY: THIS WORKER CANNOT SERVICE ITS OWN INBOX RIGHT NOW ------------------------
		//
		// Set around every dispatch -- native function, fiber switch-in, coroutine resume -- and
		// cleared when the worker is back in its search loop.
		//
		// WHY IT HAS TO EXIST. An inbox has exactly ONE legal consumer: its owner. Nobody steals
		// from it. So a task placed on a worker that is inside a task body is not merely late, it
		// is STRANDED until that body returns -- and if that body is itself waiting on the task it
		// just pushed there, it never returns. That is a deadlock built out of two correct pieces.
		//
		// Same shape as the awake bitmap and read the same way: placement PREFERS to avoid these
		// workers, and falls back to the full set rather than refusing to place at all.
		// awayHint IS GONE. It marked workers inside a task body so placement could avoid them --
		// correct in intent, and two atomic RMWs per task on ONE shared word in practice, which
		// measured throughput/1p 5.37 -> 2.93 M/s and frame DAG 8.45 -> 35.82 us/graph. See
		// UpdateBacklogHint below. The reachability it defended is now kept by publishing a
		// BACKED-UP inbox into the stealable deque at dispatch, gated on a counter the worker
		// already owns.

		std::atomic<unsigned long long> stealHintBacklog[kHintWords]{};
		std::atomic<unsigned long long> stealHintParallel[kHintWords]{};

		// Owner-maintained, on push and pop. Writes only on a threshold crossing.
		// ADVERTISING REGARDLESS OF DEPTH WAS TRIED AND REVERTED -- do not re-add it without
		// running the full bench, because no unit test can see what it costs.
		//
		// The argument was good: UpdateBacklogHint below only sets the bit at kStealHintDepth (8)
		// or more, which is right while the owner keeps taking passes and wrong at the one moment
		// it stops -- a worker entering a long body will not get to a shallow backlog shortly, and
		// below the threshold nothing else may learn it exists.
		//
		// WHAT IT ACTUALLY COST: `advertisedCount != 0` is POOL-WIDE and gates both parking and the
		// collapse call site. A worker takes its next task with a non-empty deque on most
		// dispatches, so "advertise when a worker departs" meant "advertise nearly always", and one
		// shallow queue kept the whole pool awake. Measured over three affinity policies:
		// throughput/1p 3.12 -> 1.24 M/s, kernel wakes 169k -> 318k, frame DAG 4.66 -> 7.82
		// us/graph, and the awake floor ended at F=28 having never shed -- `FLOOR DID NOT SHED` on
		// two runs of three.
		//
		// The case it was written for is handled at PUSH time now: work whose target is away goes
		// to the escape queue instead of that worker's inbox, so it never needed advertising.
		void UpdateBacklogHint(size_t q, size_t depth) noexcept {
			if (q >= kMaxHintQueues) return;
			auto& word = stealHintBacklog[q >> 6];
			const unsigned long long bit = 1ull << (q & 63);
			const bool want = depth >= kStealHintDepth;
			if (want == ((word.load(std::memory_order_relaxed) & bit) != 0)) return;
			if (want) word.fetch_or(bit, std::memory_order_release);
			else      word.fetch_and(~bit, std::memory_order_relaxed);
		}
		// Set by the splitter when it publishes; cleared by the owner when its lane drains. Held
		// conservatively -- while ANY work remains the lane stays advertised, which costs a probe
		// and can never hide a split.
		// Returns TRUE only on the 0 -> 1 EDGE, i.e. when this call is the one that made the lane
		// advertise. Callers use that to wake a thief exactly once per range instead of once per
		// split -- see the wake site in the splitter for what per-split cost looked like.
		bool SetParallelHint(size_t q) noexcept {
			if (q >= kMaxHintQueues) return false;
			const unsigned long long m = 1ull << (q & 63);
			const unsigned long long old =
				stealHintParallel[q >> 6].fetch_or(m, std::memory_order_release);
			return (old & m) == 0;
		}
		// ---- HIPRI PRESENCE, ONE BIT PER WORKER ----------------------------------------------
		//
		// Set when anything lands in worker q's lane inbox or deque; cleared by q itself when both
		// are empty. A thief probes a remote lane deque ONLY when the bit says there is something
		// there, so the common case -- no I/O in flight -- costs a register test rather than a
		// steal_if against another worker's cache line.
		//
		// REUSES stealHintLane, which is the bitmap the K lane advertised through and has been
		// unused since K was stubbed. Same shape (one word, one bit per worker, workers 0..63),
		// same publisher-sets/owner-clears discipline, so nothing new has to be reasoned about --
		// only the meaning of the bit changed, from "this hot worker has lane work buried" to
		// "this worker has lane work at all".
		void SetHiPriHint(size_t q) noexcept {
			if (q >= 64) return;
			stealHintLane.fetch_or(1ull << q, std::memory_order_release);
		}
		void ClearHiPriHint(size_t q) noexcept {
			if (q >= 64) return;
			stealHintLane.fetch_and(~(1ull << q), std::memory_order_relaxed);
		}
		bool HiPriHintSet(size_t q) const noexcept {
			if (q >= 64) return true;   // past the bitmap: probe rather than miss work
			return (stealHintLane.load(std::memory_order_acquire) & (1ull << q)) != 0;
		}
		// The whole word, for a worker that needs to ask "is there lane work ANYWHERE" rather than
		// "at q". A reserved worker's idle decision needs exactly that: it may not take loPri, so
		// loPri advertisements must not keep it awake. See Thread::Worker's advertisedCount.
		unsigned long long HiPriHintWord() const noexcept {
			return stealHintLane.load(std::memory_order_acquire);
		}

		void ClearParallelHintIfEmpty(size_t q, size_t depth) noexcept {
			if (q >= kMaxHintQueues || depth != 0) return;
			stealHintParallel[q >> 6].fetch_and(~(1ull << (q & 63)), std::memory_order_relaxed);
		}

		// Runtime kill switch, so the hint`s value can be measured against an A/A control inside one
		// process. Three separately-built binaries measured in three sessions is how a 2x machine
		// drift once got read as a result.
		// THE POOL-WIDE DISABLE IS GONE, and it is the whole point of the multi-word change. It read
		// `deques.size() > 64 -> return true`, which turned the hint off for EVERY queue the moment
		// the pool outgrew one word -- necessarily, because covering only 0..63 would have starved
		// everything above it. With 256 bits available the guard is now per-queue, so a pool larger
		// than one word keeps its hints; only a pool larger than 256 falls back, and then uniformly.

		// ---- PER-WORD ACCESSORS FOR THE STEAL BLOCK ------------------------------------------
		//
		// The sweep snapshots ceil(queues/64) words ONCE and then tests bits in a register, rather
		// than re-deriving a candidate's bit through MaybeStealable per victim. On a one-word pool
		// that is two acquire loads for the whole sweep instead of two per mate.
		//
		// MEMBERS, NOT THE STATICS, and this is not a style preference. LaneBacklogMask() reaches
		// the same word through Instance(), which THROWS when the pool is not up -- from a noexcept
		// function, so it is std::terminate, not an exception. Workers start INSIDE StartPool,
		// before `instance` is assigned, so one unconditional static read in the worker loop took
		// every pool-starting test down at 0xC0000409: silent, no message, nothing for ASan to find.
		// A worker already holds `scheduler`; inside that loop, use these and never the statics.
		// Set/clear this worker's awake bit. Called by the worker itself, on transitions only.
		void SetAwake(size_t q, bool awake) noexcept {
			if (q >= kMaxHintQueues) return;   // past the bitmap: no bit, and placement falls back
			auto& word = awakeHint[q >> 6];
			const unsigned long long m = 1ull << (q & 63);
			if (awake) word.fetch_or(m, std::memory_order_relaxed);
			else       word.fetch_and(~m, std::memory_order_relaxed);
		}
		unsigned long long AwakeHintWord(size_t wi) const noexcept {
			return awakeHint[wi].load(std::memory_order_relaxed);
		}

		// AN ESCAPE QUEUE WAS BUILT HERE AND REVERTED. A moodycamel MPMC queue held work whose
		// chosen worker was inside a task body, so any worker could take it -- the right answer to
		// "an inbox has one legal consumer and its consumer is busy". It was reverted for its
		// TRIGGER, not its design: diverting requires knowing the owner will be busy for a LONG
		// time, and no cheap signal says that. See UpdateBacklogHint below for the measurements.

		// IS ANY PUBLISHED RANGE STILL LIVE? The second half of the recruitment gate -- see
		// SetRangeRecruit. PARALLEL BITS ONLY, deliberately: a backlog bit means somebody has a
		// deque worth stealing from, which is an ordinary condition and not a reason to spend
		// wakes. Recruitment is for a range that is still handing out leaves.
		bool AnyParallelHint() const noexcept {
			for (size_t w = 0; w < kHintWords; ++w)
				if (stealHintParallel[w].load(std::memory_order_acquire)) return true;
			return false;
		}
		unsigned long long StealHintWord(size_t wi) const noexcept {
			return stealHintBacklog[wi].load(std::memory_order_acquire)
			     | stealHintParallel[wi].load(std::memory_order_acquire);
		}

		// Is ANY queue advertising stealable work right now? The same question the park condition
		// asks, and for the floor collapse it is the difference between "the wave is over" and "this
		// one worker lost a race for it" -- shedding on the second would shear a live wave.
		// TEMP DIAG -- is the queue a hint bit claims work for actually empty? A set bit over an
		// empty queue is a hint nobody will clear (publisher-sets, owner-clears, owner has nothing
		// to drain) and it pins advertisedCount above zero pool-wide.
		bool WorkerQueuesEmpty(size_t q) const noexcept {
			if (q >= deques.size()) return true;
			// laneInboxes is sized to the WORKERS only -- loPri has one extra for the non-worker
			// lane, so guarding on deques.size() lets q index one past the end of the inboxes.
			if (q >= laneInboxes.size()) return deques[q]->empty();
			return deques[q]->empty() && laneInboxes[q]->empty();
		}
		bool AnyStealAdvertised() const noexcept {
			for (size_t w = 0; w < kHintWords; ++w)
				if (StealHintWord(w)) return true;
			return false;
		}
		// The lane map is deliberately ONE word (hot workers are always the lowest indices, and K is
		// clamped to 64 for exactly that reason), so every higher word is empty by construction
		// rather than by accident -- returning 0 keeps the caller's loop uniform across both maps.
		unsigned long long LaneHintWord(size_t wi) const noexcept {
			return (wi == 0) ? stealHintLane.load(std::memory_order_acquire) : 0ull;
		}

		bool MaybeStealable(size_t q) const noexcept {
			if (q >= kMaxHintQueues || deques.size() > kMaxHintQueues) return true;
			if (!stealHintOn.load(std::memory_order_relaxed)) return true;
			const size_t wi = q >> 6;
			const unsigned long long bit = 1ull << (q & 63);
			return ((stealHintBacklog[wi].load(std::memory_order_acquire)
			       | stealHintParallel[wi].load(std::memory_order_acquire)) & bit) != 0;
		}

		// ---- THE LANE'S OWN BACKLOG SIGNAL, and why it is not the one above -------------------
		//
		// The two hints above are OWNER-maintained: the worker updates them once per pass through
		// its loop. That works for bulk work and fails for the lane, in exactly the case the lane
		// cares about. MEASURED: with one in eight completions running a 200us handler, a hot worker
		// spends 62-65% of its idle passes next to a SIBLING sitting on a mean depth of 8-13 -- and
		// during those 200us the buried worker is not looping, so an owner-maintained bit would stay
		// unset precisely when it is needed. Whoever advertises the backlog must be running, and the
		// only party guaranteed to be running is the one doing the pushing.
		//
		// So: an EXACT count of lane tasks outstanding at each worker, incremented by the publisher
		// and decremented by whoever removes one (the owner popping, or a thief stealing). The
		// counter is per-worker and on its own line; the BIT derived from it is what thieves read,
		// so a spinning hot worker touches one shared word rather than K contended counters.
		//
		// THRESHOLD, not "non-empty". Steering placed that task on that worker deliberately, and a
		// hot worker about to run its own warm task must not lose it to a sibling -- that is the
		// v1 steal-hint failure (owner loses the race, 22,000 probes against a 9,164 baseline) with
		// higher stakes, because here it also defeats the placement. Four is above the depth a
		// keeping-up worker reaches and well below the 8-13 measured when one is buried.
		// kLaneStealDepth IS GONE with the steal it throttled. It was the depth above which a
		// SIBLING was permitted to steal from a hot worker.s lane deque; no deque, no steal, no
		// threshold. The hint bit it once gated survives as a pure presence flag for the K
		// controller, set/cleared against the inbox (see laneSetDepth/laneClearDepth defaults).

		std::atomic<unsigned long long> stealHintLane{ 0 };

		// PushIO's round-robin start, so a steady completion stream does not pile on worker 0 while
		// the rest of the reserved band idles. Relaxed everywhere: the only requirement is that
		// successive calls differ, never that two threads agree on a value.
		std::atomic<size_t> ioSteer_{ 0 };

		// MAINTAINED BY THE OWNER AT ITS DRAIN, and the distinction from "owner maintained per pass"
		// is the whole reason this works. The buried worker is not looping during its 200us handler
		// -- but it drained its inbox into its deque IMMEDIATELY BEFORE starting that handler, and
		// at that moment it knew the depth. So the backlog is advertised before the worker goes
		// dark, and stays advertised for exactly as long as it is buried.
		//
		// The publisher-side exact counter this replaces worked too, and cost p50 ~30% ACROSS THE
		// BOARD -- including the uniform control where stealing almost never fires, which is what
		// identified it as accounting rather than mechanism. One atomic RMW per lane push, on a line
		// shared between the completion thread and the worker it steers to, on the hot path. Same
		// shape as the unsharded coroutine counter that once cost 62%. This version writes only on
		// threshold crossings, and only from a thread that already owns the line.
		//
		// The gap it accepts: tasks arriving DURING the handler sit in the inbox unadvertised. Those
		// are the ones a publisher-side counter would have caught, and giving them up is what buys
		// back the p50.
		// DIAGNOSTIC ARM SELECTOR, so the three candidate designs run from ONE binary and can be
		// interleaved. Comparing them as three separately-built executables measured in three
		// sessions produced a 2x swing in the K=1 rows -- a configuration with no sibling, where
		// hot->hot stealing provably cannot act -- which is machine drift, not a result. The branch
		// costs the same in every arm, which is the property that matters here.
		//
		//   0  no lane hint, no hot->hot stealing (the shipped behaviour before this)
		//   1  hint maintained at the DRAIN only, from a local count
		//   2  hint maintained per pickup, from lane->size()  (touches the thief-written line)

		// A SCHMITT TRIGGER, not a threshold. Sets at kLaneStealDepth, clears at laneClearDepth, and
		// the gap between them is the whole point.
		//
		// WHY, MEASURED 8-26. With one threshold the bit chatters: a helper drains the lane below the
		// line, it refills, the line is crossed again -- and the crossing RATE rose from 6,934/s to
		// 17,731/s once lane wakes were switched on, because the wakes are what does the draining.
		// Positive feedback on the edge count. Each edge fires up to n wakes, so a mechanism designed
		// as "one wake per burial" measured 3,600-92,000 wakes/s.
		//
		// The bit is read by THIEVES as well as by the wake path, so widening it changes both. A
		// worker that was never buried never sets the bit and is untouched either way; a worker that
		// WAS buried now keeps advertising down to laneClearDepth, so a thief may take its
		// second-to-last task. That is the trade the gap buys, and it is measured rather than
		// assumed -- see the bench's wake=0 rows, which vary the gap with the wake path switched off
		// and therefore isolate the effect on stealing alone.
		void UpdateLaneHint(size_t w, size_t depth) noexcept {
			if (w >= 64) return;
			const unsigned long long bit = 1ull << w;
			const bool isSet = (stealHintLane.load(std::memory_order_relaxed) & bit) != 0;
			const bool want  = isSet ? (depth > (size_t)laneClearDepth.load(std::memory_order_relaxed))
			                         : (depth >= (size_t)laneSetDepth.load(std::memory_order_relaxed));
			if (want == isSet) return;
			if (want) {
				stealHintLane.fetch_or(bit, std::memory_order_release);
				// THE 0->1 EDGE IS THE EVENT, not the level, and only the owner writes this bit --
				// so this fires once per burial rather than once per push. That rarity is what makes
				// it affordable to do here, on the hot worker itself, microseconds before it
				// disappears into a long handler.
				// NO LANE WAKE. The bit is a PRESENCE flag for the K controller, not a reason to
				// wake anybody: with the lane deque gone there is nothing for a woken worker to
				// steal, and the lane.s owner pops its own inbox on its next pass. Waking a
				// worker that can only look at a queue it may not touch is the pure-cost shape
				// this file already records for the removed lane steal probe.
				//
				// A MaybeAdjustHotWorkers() EDGE FIRED HERE AND IS GONE WITH THE CONTROLLER. The
				// argument for it was good -- this is the moment saturation BECOMES true, on the
				// worker that just got buried, rather than whenever somebody next samples. It did
				// not matter, because the controller it woke never moved K in 2.5M lane tasks. K
				// is static now; this bit is a steal hint and nothing else.
			}
			else      stealHintLane.fetch_and(~bit, std::memory_order_relaxed);
		}

		// Defined out of line in TaskScheduler.cpp: it touches Thread, which is only forward
		// declared at this point.
		// WakeForLane(depth) IS GONE. It woke ORDINARY workers to come and drain a buried hot
		// worker's lane, which the lane being an MPSC inbox makes impossible: an inbox has exactly
		// one legal consumer, so a woken worker cannot touch that lane at all. It also had ZERO
		// callers by the time it was removed -- a mechanism that could not fire, guarded by a mode
		// flag (laneHintMode 4) that gated nothing else.
		bool LaneStealable(size_t w) const noexcept {
			if (w >= 64) return false;
			return (stealHintLane.load(std::memory_order_acquire) & (1ull << w)) != 0;
		}

		std::atomic<int> nextWorker{ 0 };
		// Separate cursor for the hot set, so lane traffic and ordinary traffic do not perturb each
		// other's rotation -- and so the hot rotation stays over 0..K-1 rather than the whole pool.
		std::atomic<size_t> nextHotWorker{ 0 };
		// Cursor for lane wakes, separate from both of the above so a burst of burials spreads over the
		// ordinary workers instead of repeatedly waking the same one.
		std::atomic<size_t> nextLaneWake{ 0 };
		// P/E routing (see PickNextWorker): worker qIndices split by efficiency class (from isPCore),
		// each with its own round-robin cursor. Built in StartPool. Preference is a HINT -- PickNextWorker
		// spills to the other class if the preferred one has no available worker, and an empty set (non-
		// hybrid CPU) just falls through to the full pool. Nothing here is a hard constraint.
		std::vector<int> pWorkers, eWorkers;
		std::atomic<size_t> nextPWorker{ 0 }, nextEWorker{ 0 };
		std::atomic<bool> stopFlag{ false };
		// RAW Thread*, NOT shared_ptr. One owner, process-length lifetime, and the table is read on the
		// wake path -- a refcount and a control-block hop bought nothing and cost both. Also what makes
		// the table a plain contiguous array of pointers: Thread itself holds atomics and a std::thread
		// and so is not movable, which rules out vector<Thread>, but a pointer always is.
		// Deleted at the two clear sites in TaskScheduler.cpp; see those for why it is safe there.
		std::vector<Thread*> workers;
		TaskMPSCQueue mainQ;
		std::mutex poolMutex;
	};
#if defined(JLIBSCHED_TUNABLE_FAST_SPIN)
	// PRESENT ONLY IN A DIAGNOSTIC BUILD (-DJLIBSCHED_TUNABLE_FAST_SPIN=ON). Makes the bare-thread
	// fast-spin bound settable at runtime so bench/lock_contention.cpp can rotate through candidate
	// values inside ONE process. It has to be one process: as a pure compile-time constant each
	// value is a separate binary, and an A/A control showed process-to-process drift far larger than
	// any effect the constant could have. Not a scheduler setting and not available in a normal
	// build -- see kFastSpinTries in src/TaskScheduler.cpp.
	namespace detail {
		void SetFastSpinTries(int n);
		int  GetFastSpinTries();
	}
#endif

	// Fiber-aware mutex. A blocked fiber SUSPENDS (freeing its worker); a blocked bare thread runs
	// stolen Native work instead of spinning.
	//
	// IT NO LONGER INHERITS PRIORITY, and the comment here claimed it did for a month. Boosting the
	// holder on contention was added in 8555cbd ("implemented starvation prevention", 2026-07-15)
	// and its single call site was removed five days later in 21719ac, the rewrite that turned this
	// from a spinlock into the suspend-or-help lock described above. Both functions are now DELETED
	// (see the note where they lived in TaskScheduler.cpp): Boost had no callers, which made
	// `priorityBoost` permanently 0 and Unboost a permanent no-op -- and once lane became the
	// low-latency lane, a mechanism that promotes an aged ORDINARY task into it would have pushed
	// bulk work onto the hot workers, which is precisely what the lane excludes. The packed
	// `priorityBoost` bit is left in place so Task's layout fingerprint is unchanged.
	//
	// That removal was correct, and it is worth knowing WHY so nobody re-adds the boost as a fix for
	// a hang it cannot cause. Classic priority inversion needs a high-priority waiter to starve the
	// holder of CPU. Nothing here can: `lane` is QUEUE ORDER ONLY -- never OS thread priority,
	// never placement -- every worker runs at the same OS priority, and a task that has already
	// STARTED owns its worker until it yields or finishes, so no amount of lane work can deschedule
	// a running lock holder. What made inversion real in the old design was that waiters SPUN, so
	// they genuinely competed with the holder for a core; suspending and helping both removed that.
	// The one residual case -- the holder is a suspended fiber whose resume sits in a loPri queue
	// while lane work floods in -- is bounded by kStealFairnessWindow, which forces a loPri scan
	// every 8 consecutive lane steals. Same shape as the age-based promotion that was removed once
	// single-item stealing made it redundant: a mitigation outliving its premise.
	// One queued waiter on a SchedulerMutex or SchedulerSemaphore. EXACTLY ONE of the two pointers is
	// non-null, and which one decides how the waiter is released:
	//
	//   fiber != nullptr   a suspended fiber        -> Thread::Resume(fiber)
	//   coro  != nullptr   a suspended coroutine    -> TaskScheduler::Push(coro), which re-runs its
	//                                                  resume trampoline on some worker
	//
	// BARE THREADS ARE NOT HERE, deliberately. A blocked bare thread cannot suspend, so it never
	// queues at all -- it spins on Try_Lock/Try_Wait running stolen work (see ContendedSpinStep) and
	// finds the lock free on its own. Only contexts that can actually park need waking.
	//
	// THIS IS THE WHOLE COST OF SUPERSETTING. Everything else about these primitives is unchanged:
	// the release path pops a waiter as it always did and branches on which kind it is. Note that
	// nothing here needs C++20 -- a coroutine waiter is just a Task* and re-arming it is a Push --
	// so this stays in the C++17 core, and only the `co_await` spelling lives in Coroutine.h.
	//
	// LIFETIME, same invariant the condition variable already relies on: a Waiter is held by VALUE in
	// the queue, so it does not point into a suspended stack. The Task*/Fiber* it carries outlive the
	// wait because a waiter cannot leave before it is released, and every release path removes the
	// entry from the queue BEFORE resuming or pushing it.
	// THE one place cancellation is decided. A task is cancelled if EITHER its scope was cancelled
	// or it was cancelled individually (Event::CancelWaiters). Every observation point -- worker
	// pickup, CurrentTaskCancelled, anything added later -- must go through here, so a new way of
	// being cancelled reaches all of them rather than the one its author happened to be editing.
	inline bool IsTaskCancelled(const Task* t) {
		if (!t) return false;
		if (t->cancelledDirect) return true;
		return CancelToken(t->cancelToken).Cancelled();
	}

	// Has the currently-running task's scope been cancelled?
	//
	// THE ONLY CANCELLATION CHECK A NATIVE TASK GETS, and that is by necessity rather than design: a
	// Native task has no suspension points, so there is nowhere for the runtime to deliver the news.
	// Call it wherever a long body can usefully give up:
	//
	//     for (auto& chunk : work) {
	//         if (JLib::CurrentTaskCancelled()) return;   // RAII unwinds as usual
	//         Process(chunk);
	//     }
	//
	// Fibers and coroutines will also observe cancellation at their suspension points -- that half is
	// not built yet, see CancelToken.h -- but polling stays available to all three and is the right
	// tool for a compute loop that never suspends.
	//
	// Returns false off a task entirely, and false for a task whose scope has been destroyed:
	// unscoped work is not cancelled work. One load through a table; no lock.
	bool CurrentTaskCancelled();

	struct Waiter {
		Fiber* fiber = nullptr;
		Task*  coro  = nullptr;

		// CANCELLABLE WAITERS, both paths. A cancellable waiter carries a pointer to a result slot it
		// owns -- a local on the suspended fiber's stack, or a member of the awaiter inside a coroutine
		// frame. Both are stable precisely because a waiter cannot leave before it is resumed, and
		// resuming it is what lets it leave.
		//
		// A NULL SLOT MEANS "NOT CANCELLABLE", and that is the safety property, not an oversight.
		// Plain Lock()/Wait() pass null and are never skipped, so they cannot come back without the
		// thing they asked for. If cancellation applied to them, a caller who ignored the result
		// would proceed believing it held a lock it does not hold -- silent, and far worse than
		// waiting. Cancellation is opt-in per call site for exactly that reason.
		WaitResult* result = nullptr;
		uint32_t    token  = 0xFFFFFFFFu;   // CancelToken::kNone
	};

	class SchedulerMutex : public WaitPrimitive {
	private:
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;
		bool locked = false;
		Task* lockHolder = nullptr;
		std::queue<Waiter> waiters;   // fibers AND coroutines; see Waiter
		std::atomic_flag holderLock = ATOMIC_FLAG_INIT;

		// ---- THE BARE-THREAD FALLBACK: BLOCK, DO NOT SPIN ------------------------------------
		//
		// A bare thread (main, an app's own thread) has no context to switch away from, so it
		// cannot take the fiber path. It used to spin on Try_Lock and, between spins, try to HELP by
		// stealing Native work -- and that help cannot succeed on a fiber-backed pool, because
		// GetTask vets candidates with `fiberlessRunnable` which rejects TaskType::Fiber outright.
		// So it burned CPU walking steal candidates for work it was never permitted to claim. That
		// is the 664 us round trip in TryRunStolenNativeTask's own note.
		//
		// A REAL BLOCK IS STRICTLY BETTER **FOR A BARE THREAD** AND STRICTLY WORSE FOR A WORKER, and
		// the difference is why this is not simply "block everywhere". A bare thread is not in the
		// pool, so taking it off CPU costs nothing. A WORKER put to sleep here would leave its inbox
		// holding work only it may drain -- unstealable -- which is the busy+inbox deadlock, just
		// asleep instead of hot. Hence the Native guard in Lock(): a task on a worker never reaches
		// this path at all.
		//
		// LOCK ORDER IS bareMtx -> spinLock AND NEVER THE REVERSE. The waiter holds bareMtx and
		// takes spinLock inside the Try_Lock predicate; Unlock releases spinLock BEFORE it touches
		// bareMtx. No call site holds both in the other order, so there is no cycle.
		//
		// bareWaiters GATES THE NOTIFY so an uncontended Unlock -- the overwhelmingly common case --
		// pays one relaxed load and never touches the mutex or the condition variable.
		std::mutex              bareMtx;
		std::condition_variable bareCv;
		std::atomic<int>        bareWaiters{ 0 };

	public:
		// TEST SEAM for the may-not-block refusal below, modelled on g_epochSuspendViolation. The
		// violation is a std::abort() in a shipped build, and a test cannot assert on an abort from
		// inside the same process, so installing a handler substitutes for it.
		//
		// CONTROL DOES NOT RETURN HOLDING THE LOCK when a handler is installed -- there is no lock
		// to return with, that is the whole point. A handler must not be installed outside a test.
		static std::atomic<void(*)()> s_blockViolationHook;
	private:

	public:
		SchedulerMutex() = default;
		~SchedulerMutex() { LeaveRegistry(); }   // FIRST -- see WaitPrimitive

		// EAGER CANCELLATION, matching Event / SchedulerSemaphore / SchedulerConditionVariable.
		//
		// This lock used to be the ONE primitive without it: cancellation was skip-at-release only,
		// so a cancelled waiter stayed parked until the holder happened to release. That is the odd
		// one out in two ways, and both have now cost something.
		//
		//   CONSISTENCY. Every other blocking primitive can eject a waiter on demand. A caller that
		//   cancels a scope and expects its waits to end has no way to know one of them is a mutex.
		//
		//   TEARDOWN. A frame parked on a mutex whose holder is itself abandoned cannot be woken by
		//   ANYTHING. Its stack never unwinds, so nothing it holds is released -- RAII, its
		//   WaitGroup slot, a hazard record. That is the structural reason parked work leaks at
		//   shutdown, and the mutex was the only primitive with no way out of it.
		//
		// Ejected waiters are resumed with WaitResult::Cancelled and DO NOT hold the lock -- the
		// same rule as everywhere else here: a cancelled acquire took no permit, a cancelled wait
		// holds no lock.
		//
		// An invalid token means "everything", which is what a teardown drain wants.
		void CancelWaiters(CancelToken tok = CancelToken{});

	protected:
		// Teardown only: release everyone, unconditionally, so their frames can unwind.
		void DrainForShutdown() override { CancelWaiters(); }
	public:

		// Acquires the lock. (No priority boost happens on contention despite what this line used to
		// say -- see the class comment above for when that stopped being true and why it is fine.)
		//
		// Callable from EITHER context, and the two behave differently on contention:
		//   on a fiber      -- the fiber is queued and SUSPENDED, freeing the worker for other work.
		//   on a bare thread -- there is no fiber to suspend, so it spins, running one stolen
		//                       Native task per iteration (TryRunStolenNativeTask) instead of
		//                       burning the cycles.
		//
		// That second path is why this is NOT the right lock everywhere, and the reason is stronger
		// than latency. Running stolen tasks inside the acquisition loop makes locking REENTRANT:
		// user code executes while you are blocked, and it can take locks of its own. Three guards
		// bound that (see ContendedSpinStep in TaskScheduler.cpp), the important one being that a
		// bare thread ALREADY HOLDING a SchedulerMutex stops helping entirely -- otherwise a helped
		// task asking for the lock you hold deadlocks you against yourself, with no fiber involved.
		//
		// What is still true: a caller that must return promptly, or that is not one of ours at all
		// (a driver thread-pool callback, say), will run tasks from the graph while it waits. For a
		// short critical section reached from foreign threads, a plain std::mutex is both cheaper
		// and safer; see the note in Event.h.
		//
		// Also unguarded by design: holding a SEMAPHORE permit. Permits have no owner -- the thread
		// that takes one is often not the one that returns it -- so there is nothing sound to count.
		void Lock();

		void Unlock();

		// Non-blocking try_lock
		bool Try_Lock();

		// Lock(), but observes the calling task's cancellation scope. Returns Cancelled if the
		// scope was cancelled while waiting -- IN WHICH CASE THE LOCK IS NOT HELD and Unlock() must
		// not be called.
		//
		// DELIVERY IS SKIP-AT-RELEASE, and the delay is the deliberate part of the design. A
		// cancelled waiter is not plucked out of the queue -- removing one entry would need the
		// queue to support arbitrary removal and the canceller to know which lock to look in. It is
		// skipped when the lock is NEXT RELEASED: Unlock walks past it, resumes it with Cancelled so
		// it can unwind, and hands ownership to the first waiter that is still interested.
		//
		// So cancellation here is bounded by how long the current holder keeps the lock, not
		// immediate. That is acceptable because a lock nobody releases hangs an uncancelled waiter
		// just as thoroughly; it is not a case cancellation was going to rescue. Waking a parked
		// waiter the instant Cancel() is called needs a scope->waiter registry, which costs more
		// than it currently buys.
		//
		// NEVER PUT CLEANUP BEHIND THIS SPELLING. A mutex is the primitive most likely to guard the
		// very state an unwind has to touch -- a pool to return a block to, a registry to remove
		// yourself from -- and an unwind path runs BECAUSE the scope was cancelled. So the token is
		// already cancelled by the time it gets there, the pre-check above fires, and this returns
		// Cancelled WITHOUT EVEN LOOKING AT THE LOCK. Not a race that leaks sometimes: the lock can
		// be sitting free and it still returns Cancelled, every single time, and the release never
		// happens.
		//
		// The rule that follows: cancellable spellings are for the PRODUCTIVE work you are
		// abandoning. The unwind that abandoning it requires uses plain Lock(), which cannot fail --
		// cancellation is what put you on that path, so it must not also be able to block your exit
		// from it.
		//
		// AND IF YOU DO WANT AN EAGERLY CANCELLABLE LOCK, YOU ALREADY HAVE ONE: SchedulerSemaphore
		// sem(1, 1). WaitCancellable() acquires, Signal() releases, CancelWaiters(tok) ejects a
		// waiter immediately, and maxPermits=1 clamps a double release instead of inventing a second
		// permit. That is why there is no eager cancel here and no second lock type -- it would be a
		// worse copy of a primitive that already exists.
		//
		// The trade is OWNERSHIP, and it is the whole difference: a permit has no owner (see Lock's
		// note on why holding one is unguarded), so a binary semaphore gives up the holder record,
		// the deadlock diagnostics built on it, and the guarantee that whoever releases is whoever
		// acquired. Take the semaphore when the wait may be unbounded and abandonment is a real
		// outcome; take the mutex when you want the lock to be owned.
		//
		// A BARE THREAD checks the token between spins instead, since it never enqueues.
		[[nodiscard]] WaitResult LockCancellable();

		// COROUTINE PATH. Do not call this directly -- use `co_await JLib::LockAsync(m)` from
		// Coroutine.h, which is the only correct way to reach it.
		//
		// Acquires the lock if it is free, and otherwise queues `coroTask` to be re-pushed when the
		// lock is released. Returns TRUE if the lock was taken (the caller must NOT suspend) and
		// FALSE if the task was queued (the caller must stay suspended).
		//
		// The test and the enqueue happen under the SAME spinLock acquisition, and that is the entire
		// reason this is one call rather than a Try_Lock followed by an enqueue. Split them and a
		// release landing in the gap finds an empty queue, sets locked = false and wakes nobody --
		// after which this task queues onto a free lock and waits for an Unlock that will never come.
		// Same lost-wakeup shape as the 1.3.5 bug, reached a different way.
		//
		// Once this returns false the task is visible to Unlock, so the caller must touch neither the
		// task nor its coroutine handle again: it may already be running on another worker.
		bool LockAsyncEnqueue(Task* coroTask);

		// Cancellable form of the above, for `co_await LockAsyncCancellable(m)`. `result` is a slot
		// inside the awaiter (and therefore inside the coroutine frame), which survives the
		// suspension for the same reason a fiber's stack local does.
		bool LockAsyncEnqueue(Task* coroTask, WaitResult* result);
	};

	class SchedulerSemaphore : public WaitPrimitive {
	private:
		std::mutex mtx;
		std::queue<Waiter> waiters;        // suspended fibers AND coroutines; see Waiter
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT; // Must be here!
		int permits;
		const int maxPermits;

	public:

		explicit SchedulerSemaphore(int initialPermits, int maxPermits = INT_MAX)
			: permits(initialPermits), maxPermits(maxPermits) {}

		~SchedulerSemaphore() { LeaveRegistry(); }   // FIRST -- see WaitPrimitive

		void Wait();

		// Cancellable Wait. Returns Cancelled if this task's scope was cancelled while parked, and
		// in that case NO PERMIT WAS TAKEN -- do not Signal() to "give it back", there is nothing to
		// give back. Separate from Wait() so a caller who ignores the result cannot proceed believing
		// it holds a permit it does not hold; same rule as SchedulerMutex::LockCancellable.
		[[nodiscard]] WaitResult WaitCancellable();

		// EAGER cancellation, for a semaphore used as an I/O THROTTLE rather than a critical section.
		// Ejects waiters and wakes them with Cancelled WITHOUT waiting for a Signal -- the point being
		// that the release you would wait for is the work you are trying to abandon.
		//
		// tok selects whose waits to abort (the usual case: one player, one zone, one request). A
		// default-constructed token means EVERYONE, for teardown.
		//
		// Waiters that used plain Wait() are never ejected: they have nowhere to report Cancelled, so
		// waking one would hand its caller a permit it does not hold.
		void CancelWaiters(CancelToken tok = CancelToken{});

	protected:
		// Teardown only: release everyone, unconditionally, so their frames can unwind.
		void DrainForShutdown() override { CancelWaiters(); }
	public:

		bool Try_Wait();

		void Signal();

		// COROUTINE PATH. Use `co_await JLib::AcquireAsync(sem)` from Coroutine.h rather than calling
		// this directly. Takes a permit if one is available (returns true, caller must NOT suspend),
		// otherwise queues the task to be re-pushed by Signal() (returns false, caller stays
		// suspended). Test and enqueue share one spinLock acquisition for the same lost-wakeup reason
		// spelled out on SchedulerMutex::LockAsyncEnqueue.
		//
		// The ownership asymmetry documented on ScopedPermit applies here too and more sharply: a
		// permit taken by a coroutine has no owner the scheduler can track, and a coroutine can be
		// resumed on any worker, so nothing counts this as an acquisition for the helping guard.
		// result: optional slot the releaser writes before resuming, so a coroutine can learn it was
		// cancelled. Null means not cancellable and never skipped -- see the note on Waiter. The slot
		// must outlive the suspension, which is why the awaiter holds it as a MEMBER: an awaiter lives
		// in the coroutine frame, so a member of it is stable for exactly as long as the wait is.
		bool WaitAsyncEnqueue(Task* coroTask, WaitResult* result = nullptr);

		// RAII permit, and the ONLY way to hold one safely across a blocking call on a bare thread.
		//
		// Why this exists rather than tracking it in Wait(). A bare thread that blocks on a
		// scheduler primitive runs stolen tasks while it waits, so anything it holds can be demanded
		// by the code it runs. SchedulerMutex closes that by counting ownership and refusing to help
		// while it owns something. A raw permit cannot be counted the same way: the thread that
		// takes one is frequently not the thread that returns it, which is the entire point of a
		// producer/consumer semaphore, so counting a Wait() as an acquisition would make a
		// consumer's tally climb forever and silently stop it ever helping again.
		//
		// This resolves that by making the USAGE PATTERN declare itself. Take a permit lock-like --
		// acquire, work, release on the same thread -- and you get the same guard the mutex has.
		// Keep using Wait()/Signal() directly for producer/consumer and you pay nothing, correctly,
		// because there the permit genuinely has no owner.
		//
		// A fiber deliberately gets no counting: it can acquire on one worker and resume on another,
		// so a per-thread count would be corrupted by migration. It also does not need it, since a
		// fiber SUSPENDS on contention and never enters the helping path at all.
		//
		// What this does NOT do: make deadlock impossible. A helped task can still block on
		// something the scheduler cannot see -- a std::mutex, a file read, a GPU fence -- and no
		// ownership tracking reaches those. The rule in DESIGN.md is the real protection; this makes
		// the safe spelling the convenient one.
		class ScopedPermit {
		public:
			explicit ScopedPermit(SchedulerSemaphore& s);
			~ScopedPermit();
			ScopedPermit(const ScopedPermit&) = delete;
			ScopedPermit& operator=(const ScopedPermit&) = delete;
		private:
			SchedulerSemaphore& sem;
		};
	};

	class SchedulerConditionVariable : public WaitPrimitive {
	private:
		// User-space spinlock protecting the internal CV queue
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;

		// One entry per waiting fiber: the transient semaphore it is parked on, plus the scope it
		// waits under so CancelWaiters can pick out whose waits to abort.
		//
		// `sem` POINTS INTO THE WAITING FIBER'S STACK FRAME -- see the invariant note in Wait().
		// Anything that lets a waiter return must remove its entry from this queue FIRST, under the
		// lock, exactly as the notifiers do. That is the entire reason CancelWaiters below removes
		// before it wakes rather than the other way round.
		struct CvWaiter {
			SchedulerSemaphore* sem   = nullptr;
			uint32_t            token = 0xFFFFFFFFu;   // CancelToken::kNone
		};
		std::queue<CvWaiter> waitingQueue;

		void LockQueue();
		void UnlockQueue();

	public:
		SchedulerConditionVariable() = default;
		~SchedulerConditionVariable() { LeaveRegistry(); }   // FIRST -- see WaitPrimitive

		// Fibers suspend here; Native tasks spin and steal work
		void Wait(SchedulerMutex& mutex);

		// Cancellable Wait.
		//
		// THE INVARIANT: this RE-ACQUIRES THE MUTEX BEFORE RETURNING, cancelled or not. A condition
		// variable's contract is that Wait returns holding the lock, and cancellation does not get
		// to break it -- otherwise every caller would need a conditional unlock and the first one to
		// forget would unlock a mutex it does not hold. So the caller's Unlock stays unconditional
		// and only the RESULT differs.
		//
		// That is why this is safe where a timed wait would not be: re-acquiring is a wait, and for
		// a fiber or a coroutine a wait is a suspension rather than a blocked thread.
		[[nodiscard]] WaitResult WaitCancellable(SchedulerMutex& mutex);

		// EAGER cancellation: wakes matching waiters without any Notify. A condition variable is the
		// clearest case for it -- the condition may simply never become true.
		//
		// tok selects whose waits to abort; a default-constructed token means everyone, for
		// teardown. Waiters that used plain Wait() are never woken: they have nowhere to report
		// Cancelled.
		void CancelWaiters(CancelToken tok = CancelToken{});

	protected:
		// Teardown only: release everyone, unconditionally, so their frames can unwind.
		void DrainForShutdown() override { CancelWaiters(); }
	public:

		// Unblocks one waiting fiber context
		void Notify_One();

		// Unblocks all waiting fiber contexts
		void Notify_All();
	};

	// ================================ STALE-LIBRARY GUARD ==========================================
	// Aborts at startup if the Scheduler library was compiled against a DIFFERENT version of these
	// headers than the code including them now.
	//
	// WHY THIS EARNS ITS KEEP. Several classes here are header-only with real data members --
	// EpochManager, Task, TaskAllocator -- so adding a single member changes their layout in the
	// consumer's translation unit while the already-built .lib still uses the old offsets. Nothing
	// diagnoses that. The symptoms are heap corruption at an unrelated address, and they are
	// spectacularly misleading: this has produced an access violation inside std::vector::resize
	// (iterator-debug machinery walking a garbage proxy), a garbage epoch-slot pointer surfacing
	// inside LockFreeList::add, and a corrupted free list surfacing inside TaskAllocator::refill --
	// none of which are anywhere near the actual mistake, which was simply "you did not rebuild".
	//
	// This project has now lost time to it TWICE. A one-time comparison at static init is a much
	// better trade than the diagnosis.
	//
	// _ITERATOR_DEBUG_LEVEL is in the signature deliberately: on MSVC it changes std::vector's
	// layout, so mixing /MDd (level 2) with /MD (level 0) breaks exactly the same way and is the
	// other half of this failure mode.
	// Declared here rather than by including <windows.h>, which this header deliberately does not
	// pull in. The declaration matches the one in winbase.h exactly, so including both is fine.
#if defined(_WIN32)
}
extern "C" __declspec(dllimport) void __stdcall OutputDebugStringA(const char*);
namespace JLib {
#endif

	namespace detail {
		// WHAT THIS MUST COVER: every type whose LAYOUT a header-inline function depends on.
		//
		// Until 1.4 this was sizeof() only, and it did not cover TaskScheduler at all. Both of those
		// were wrong, and the second hid the first: 1.4 added nonWorkerLane and nonWorkerLaneClaimed
		// and made consecutiveHiPriSteals thread_local, which moved `taskAllocator` from offset 304
		// to 312 -- while sizeof(TaskScheduler) stayed at 1664, because the new members landed in
		// padding that was already there. sizeof(Task), sizeof(TaskAllocator) and
		// sizeof(EpochManager) did not move either. So the signature MATCHED across a genuine ABI
		// break, and the guard stayed silent through precisely the failure it exists to catch.
		//
		// THE RULE, corrected: sizeof is not a proxy for layout. Anything header-inline code reaches
		// into contributes an OFFSET (TaskScheduler::abiCanary carries that for the scheduler
		// itself), not just a size. Sizes stay for the types inline code allocates or copies whole.
		//
		// WHY THE ANONYMOUS NAMESPACE, WHICH IS NORMALLY WRONG IN A HEADER: internal linkage is the
		// entire mechanism. This has to evaluate SEPARATELY in the library's translation unit and in
		// each application translation unit, because comparing those two evaluations is the test.
		// An `inline` function -- what this was first written as -- has external linkage, so the
		// linker folds the copies into one and both sides call the SAME body. Tested: with the
		// library built at offset 296 and the application at 312, the folded version compared equal
		// and the program ran into the corruption unchecked. Per-TU copies are the point, not a
		// smell, and the ODR is satisfied precisely because these are distinct entities.
		#if defined(__GNUC__) || defined(__clang__)
		#	pragma GCC diagnostic push
		#	pragma GCC diagnostic ignored "-Winvalid-offsetof"
		#endif
		// Carried FIELD BY FIELD rather than hashed down to one number, because "signature
		// 4417359680416 != 4417354841872" tells you that something is stale and nothing about what.
		// Naming the field that moved tells you which header changed, which usually tells you which
		// library was not rebuilt.
		struct AbiComponents {
			// taskLayout is a FINGERPRINT, not a size -- see TaskLayoutFingerprint below. The field
			// COUNT and their types are this function's ABI, so nothing may be added here without
			// breaking every already-built library: an older one would return fewer bytes than the
			// caller allocated and the guard would compare against uninitialized memory to decide
			// whether the guard should fire. Widening the MEANING of an existing field costs
			// nothing, which is why that is what was done when Task's layout needed covering.
			uint32_t sizeEpochManager, taskLayout, sizeTaskAllocator, sizeTaskDeque;
			uint32_t sizeTaskMPSCQueue, sizeWaitGroup, sizeTaskScheduler;
			uint32_t offsetAbiCanary, iteratorDebugLevel;
		};

		namespace {
			// A fingerprint of Task's LAYOUT rather than merely its size, because size alone is blind
			// to the change class that matters most here.
			//
			// 2.8.0 packed six one-byte flags into a single byte. Every one of them moved, and
			// sizeof(Task) stayed 64 -- they had been followed by padding either way. A translation
			// unit compiled against the new header and linked to the older library would have read
			// `trivialDtor` from the wrong bit: silently skipping ~Task and leaking every lambda
			// capture, or running a destructor it should not have. The guard would have reported
			// everything in agreement, which is worse than having no guard, because it reports
			// success.
			//
			// Hashed rather than reported directly because the facts that matter do not fit in one
			// number: size, alignment, the packed flag block's width, and the offset of every named
			// member. Any reorder, resize or repack changes it. The cost is that a mismatch prints
			// two opaque values instead of "64 vs 72" -- acceptable, since the only useful response
			// to ANY mismatch here is the one the message already gives: rebuild.
			//
			// offsetof on Task is conditionally-supported (a vtable makes it non-standard-layout) and
			// is used on exactly the same footing as the existing offsetof(TaskScheduler, abiCanary)
			// below. Every compiler this library targets supports it.
			inline uint32_t TaskLayoutFingerprint() {
				uint32_t h = 2166136261u;                       // FNV-1a
				auto mix = [&h](uint32_t v) { h ^= v; h *= 16777619u; };
				mix((uint32_t)sizeof(Task));
				mix((uint32_t)alignof(Task));
				mix((uint32_t)sizeof(TaskFlagPacking));         // width of the packed flag block
				mix(TaskFlagBitLayout());                       // and every flag's BIT POSITION in it
				mix((uint32_t)offsetof(Task, fn));
				mix((uint32_t)offsetof(Task, data));
				mix((uint32_t)offsetof(Task, assignedFiber));
				mix((uint32_t)offsetof(Task, next));
				mix((uint32_t)offsetof(Task, waitGroup));
				return h;
			}

			inline AbiComponents LocalAbiComponents() {
				AbiComponents c{};
				c.sizeEpochManager    = (uint32_t)sizeof(EpochManager);
				c.taskLayout          = TaskLayoutFingerprint();
				c.sizeTaskAllocator   = (uint32_t)sizeof(TaskAllocator);
				c.sizeTaskDeque       = (uint32_t)sizeof(TaskDeque);
				c.sizeTaskMPSCQueue   = (uint32_t)sizeof(TaskMPSCQueue);
				c.sizeWaitGroup       = (uint32_t)sizeof(WaitGroup);
				c.sizeTaskScheduler   = (uint32_t)sizeof(TaskScheduler);
				c.offsetAbiCanary     = (uint32_t)offsetof(TaskScheduler, abiCanary);
#if defined(_ITERATOR_DEBUG_LEVEL)
				c.iteratorDebugLevel  = (uint32_t)_ITERATOR_DEBUG_LEVEL;
#endif
				return c;
			}
		}
		#if defined(__GNUC__) || defined(__clang__)
		#	pragma GCC diagnostic pop
		#endif

		// Defined in TaskScheduler.cpp, so it carries the value as the LIBRARY saw it.
		//
		// NAMED THIS WAY ON PURPOSE. There are two distinct staleness failures and this covers both:
		//   - The library predates the guard entirely (or was not rebuilt at all). Then this symbol
		//     is MISSING and you get a LINK error -- and a linker cannot print an explanation, so
		//     the only place to put one is the symbol name itself. "unresolved external symbol
		//     LibraryAbiSignature" tells you nothing; this tells you what to do.
		//   - The library was rebuilt but from different headers. Then it links, and the runtime
		//     comparison below fires with a real message.
		AbiComponents JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration();

		// Runs once per TRANSLATION UNIT, not once per program -- and that distinction is the whole
		// guard. This was `inline const bool`, which has external linkage and therefore gets folded
		// to a single copy, so exactly one TU's view was ever checked and every other TU went
		// unexamined. Internal linkage means each TU tests ITS OWN layout against the library's.
		// (See LocalAbiComponents above for the same mistake and the measurement that caught it.)
		namespace {
		[[maybe_unused]] const bool g_abiChecked = [] {
			const AbiComponents lib = JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration();
			const AbiComponents hdr = LocalAbiComponents();

			char msg[1600];
			int n = std::snprintf(msg, sizeof msg,
				"[JLib::Scheduler] FATAL: this translation unit was compiled against DIFFERENT "
				"Scheduler headers than the Scheduler library it is linked to.\n"
				"  Fields that disagree (library vs this TU):\n");

			const struct { const char* name; uint32_t l, h; } fields[] = {
				{ "sizeof(EpochManager)",              lib.sizeEpochManager,   hdr.sizeEpochManager   },
				{ "Task layout (size/align/offsets)",  lib.taskLayout,         hdr.taskLayout         },
				{ "sizeof(TaskAllocator)",             lib.sizeTaskAllocator,  hdr.sizeTaskAllocator  },
				{ "sizeof(TaskDeque)",                 lib.sizeTaskDeque,      hdr.sizeTaskDeque      },
				{ "sizeof(TaskMPSCQueue)",             lib.sizeTaskMPSCQueue,  hdr.sizeTaskMPSCQueue  },
				{ "sizeof(WaitGroup)",                 lib.sizeWaitGroup,      hdr.sizeWaitGroup      },
				{ "sizeof(TaskScheduler)",             lib.sizeTaskScheduler,  hdr.sizeTaskScheduler  },
				{ "offsetof(TaskScheduler,abiCanary)", lib.offsetAbiCanary,    hdr.offsetAbiCanary    },
				{ "_ITERATOR_DEBUG_LEVEL",             lib.iteratorDebugLevel, hdr.iteratorDebugLevel },
			};
			bool differs = false;
			for (const auto& f : fields) {
				if (f.l == f.h) continue;
				differs = true;
				n += std::snprintf(msg + n, (n < (int)sizeof msg) ? sizeof msg - n : 0,
					"    %-36s %u vs %u\n", f.name, f.l, f.h);
			}
			if (!differs) return true;

			std::snprintf(msg + n, (n < (int)sizeof msg) ? sizeof msg - n : 0,
				"  Rebuild EVERY library that includes TaskScheduler.h, not just the Scheduler --\n"
				"  a stale Sound/Renderer/Physics library carries its own inlined copy of CreateTask\n"
				"  and reaches the wrong offset in a correctly-built scheduler object.\n"
				"  Note the Scheduler ships Debug, Development and Release; rebuilding only some of\n"
				"  them causes exactly this. _ITERATOR_DEBUG_LEVEL differing means /MDd vs /MD.\n"
				"  Continuing would fault at an unrelated address -- refusing instead.\n");

			std::fputs(msg, stderr);
			std::fflush(stderr);
			// A GUI-subsystem process has no console, so the text above goes nowhere and all the
			// user sees is "Fatal program exit requested" at __scrt_common_main_seh. This is the
			// one channel that works there, and it lands in the debugger's Output window.
#if defined(_WIN32)
			OutputDebugStringA(msg);
#endif
			std::abort();
			return false;
		}();
		}
	}
}

