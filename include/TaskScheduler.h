// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#define NOMINMAX
#include "Task.h"
#include "TaskMPSCQueue.h"
#include "Epochs.h"
#include "TaskDeque.h"
#include "TaskAllocator.h"
#include "Topology.h"   // topology::CpuMask -- llcMaskOfWorker is one per worker
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <mutex>
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
#include "Event.h"
namespace JLib {
	class Thread;

	class TaskScheduler {
		friend class Thread;
		friend class GlobalFiberPool;


	public:
		// Priority inheritance methods (public for SchedulerMutex access)
		Task* GetCurrentTask() const;
		void BoostTaskPriority(Task* task);
		void UnboostTaskPriority(Task* task);
		void CleanupTaskMetadata(Task* task);

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
		void Join();
		void NotifyAll();

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

		// What an idle worker does when it has no local work and stole nothing. Set BEFORE Init().
		//
		//   Sleep (default) -- park on the condition variable. Costs a kernel transition to wake,
		//                      which is the single largest item in dispatch latency, and gives the
		//                      core back to everything else on the machine.
		//   NoSleep         -- never park while running. Lowest possible dispatch latency, and it
		//                      holds every worker core permanently. Measured on the reference machine:
		//                      4.1x on round-trip latency, 2.9x on the frame DAG, 3.1x on fork-join,
		//                      5.2x on single-producer throughput. That is what a core each buys.
		//
		// THE DEFAULT IS Sleep AND SHOULD STAY THAT WAY for a library rather than an application.
		// Spinning workers are a battery and thermal problem on the ARM64/Android target, where the
		// throttling costs more clock than the wake latency ever costs in dispatch; they starve
		// whatever else the host runs (an audio device thread, a render thread, Jolt through the
		// JobSystem adapter); and they make the oversubscription policy incoherent, since that
		// policy reserves a core per persistent busy thread and this would make EVERY worker one.
		// NoSleep is for an application that genuinely owns the machine -- a fullscreen game is the
		// obvious one, and the gains above are large enough to be worth the cores there.
		//
		// Pause() parks regardless of policy -- pausing means "stop using the CPU", and a policy
		// that kept spinning through it would be ignoring the only thing Pause is for.
		//
		// NOT A TIMED WAIT, and the distinction matters: this changes how long a worker searches
		// BEFORE parking, and the park itself stays an unconditional cv.wait. A lost wakeup still
		// hangs, unconditionally and visibly, instead of being silently papered over by a timeout.
		// It does make such a race rarer, which is a real debugging cost -- the 1.2.0 lost wakeup
		// was only findable because it reproduced at ~25% on a slow runner.
		// THERE IS DELIBERATELY NO MIDDLE SETTING. A SpinBriefly mode existed here, searching for a
		// configurable number of microseconds before parking, on the classic spin-then-block
		// reasoning that spinning for the cost of a block is 2-competitive. It was measured and it
		// was WORSE THAN BOTH NEIGHBOURS, monotonically worse as the budget grew (frame DAG, µs per
		// graph: Sleep 22.5 | brief@2 23.6 | brief@5 23.6 | brief@20 27.3 | brief@100 34.4 |
		// NoSleep 7.8). The 2-competitive argument assumes spinning is free for everyone else, and
		// with 31 workers it is not: spinners burn memory bandwidth and contend on steal CASes
		// against the workers that actually have work, and then park anyway, paying the wake cost
		// plus continuous park/unpark cv churn on top. Both extremes avoid one half of that; the
		// middle gets both. Removed rather than kept as a trap for anyone expecting a compromise.
		enum class IdlePolicy : uint8_t { Sleep = 0, NoSleep };
		static void       SetIdlePolicy(IdlePolicy p);
		static IdlePolicy GetIdlePolicy();

		// How much estimated SERIAL WORK (microseconds) a loop must represent before ParallelFor splits
		// it. Defaults to 75us in Release and 750us in Debug -- the constant is the fork-join
		// dispatch+join overhead, and an unoptimized build pays roughly an order of magnitude more of it.
		// Exposed because it is a property of the machine and build, not a universal truth; an app that
		// has profiled its own workload knows better. Set it enormous (1e12) to force every ParallelFor
		// serial, which is the fastest way to answer "is ParallelFor causing this?" without a rebuild.
		// Set once at startup; read-only thereafter.
		static void   SetParallelForThresholdUs(double us);
		static double GetParallelForThresholdUs();

		void ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func);
		// Fork-join (recursive-split) variant of ParallelFor. Splits the range in half, spawns the
		// right half as a task and recurses on the left inline; `grain` is the base-case size. It
		// parallelizes task CREATION -- the tree is built by the whole pool -- instead of the caller
		// spawning every chunk serially.
		//
		// NOT experimental, and usually not the one to call: ParallelFor DISPATCHES HERE AUTOMATICALLY
		// once a range needs more than ~2 tasks per worker, which is where the measurements put the
		// crossover. Below that the flat path is ~14% faster (no tree to build); above it, flat's
		// O(#tasks) serial CreateTask+Push+NotifyWorker on one thread collapses -- ~8x slower at
		// ~15k tasks. Call this directly only to bypass ParallelFor's serial-vs-parallel probe, which
		// is what the crossover benchmark does deliberately.
		void ParallelForFJ(int start, int end, int grain, std::function<void(int, int)> func);
		void ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func);
		bool Push(Task* task);
		void WaitFor(WaitGroup& wg);
		bool Push(uint8_t cpu_affinity, Task* task);
		bool Requeue(Task* task);
		// minPerSegment: the smallest run this is willing to hand to a single worker. The default of
		// 64 suits a big fire-and-forget batch, where the alternative is ONE push and the notifies
		// are pure added cost. A caller replacing N individual Push() calls -- which already notify
		// N times -- wants 1, because for it any segmenting strictly REDUCES notifies. Getting this
		// backwards is a real regression in both directions, so it is a parameter rather than a
		// constant: see ParallelFor's flat path.
		void PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity=0, size_t minPerSegment=64,
		               bool hiPri=false);

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
		// NOT A REPLACEMENT FOR ParallelFor, which probes the work, picks its own split, runs a
		// chunk on the calling thread and blocks until done. PushArray is the fire-and-forget
		// sibling: it returns as soon as the work is queued, so use it when the caller has other
		// things to do, wants to submit several arrays before waiting on them together, or already
		// knows its own chunk size. Pass a WaitGroup to be able to wait; pass nullptr not to.
		//
		// fn is COPIED into every chunk task (so it must be copyable, and its captures must outlive
		// the work). If the arena is exhausted the remaining chunks run INLINE on the caller, which
		// is the same graceful degradation ParallelFor does rather than dropping work on the floor.
		template<typename F>
		// hiPri routes every chunk task to the high-priority queue; default is low, matching
		// CreateTask and PushBatch. Priority here is QUEUE ORDER only -- it never implies placement,
		// which is CorePref's job (see Task.h).
		size_t PushArray(size_t begin, size_t end, size_t chunkSize, F&& fn, WaitGroup* wg = nullptr,
		                 bool hiPri = false) {
			if (end <= begin) return 0;
			if (chunkSize == 0) chunkSize = 1;
			const size_t total  = end - begin;
			const size_t chunks = (total + chunkSize - 1) / chunkSize;

			std::vector<Task*> ts;
			ts.reserve(chunks);
			for (size_t c = 0; c < chunks; ++c) {
				const size_t lo = begin + c * chunkSize;
				const size_t hi = (lo + chunkSize > end) ? end : lo + chunkSize;
				Task* t = CreateTask([fn, lo, hi]() { for (size_t i = lo; i < hi; ++i) fn(i); }, hiPri);
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
				PushBatch(ts.data(), ts.size(), 0, /*minPerSegment*/64, hiPri);
			return ts.size();
		}
		// Hand a task to a SPECIFIC core via that worker's immediate slot, bypassing the queues
		// entirely. For a persistent service running inside the pool rather than beside it -- an
		// audio mixer, a network poll loop. Running such a thing as a plain std::thread would work,
		// but the scheduler could not see it and the pool would silently oversubscribe the machine;
		// a pinned task is visible to placement, an outside thread is a core that quietly went
		// missing. The core is marked in immediateCoresInUse and excluded from normal placement
		// until the task finishes.
		bool PushImmediate(uint8_t cpu_affinity, Task* task);

		// PushFork was REMOVED in 1.3.4 -- use Push. It placed a child on the CALLING worker, on the
		// theory that the parent was about to WaitFor and free that core, so the child would run
		// warm on the parent's data. Measurement killed both halves of that: the win was one
		// avoided worker WAKE (~5us, gone entirely under IdlePolicy::NoSleep) and not locality at
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
		// help drain the pool instead of pure-spinning. Steals ONE noFiber task via GetTask(), which
		// vets the noFiber flag AT THE DEQUE (TaskDeque::steal_if) -- a fiber-backed task is never claimed
		// by this fiberless caller at all (it could suspend, and there's no fiber to switch away
		// to), so it stays queued for a real worker. This replaced the old steal-then-Requeue
		// relocation, which was pure contention churn (claim CAS + re-push + notify, task moved
		// nowhere). On a successful steal: runs Execute() inline, then frees with the EXACT SAME
		// sequence Worker()'s fast path uses (DestroyTask(), Free(), EBR tick check) -- required so
		// the slab stays correct; skipping either of these leaks a slab slot.
		// Returns true if it ran a task, false if nothing stealable -- callers should yield()
		// on false to avoid a hot spin.
		bool TryRunStolenNoFiberTask();

		// Steal-time class compatibility (used with TaskDeque::steal_if -- vet BEFORE claiming, never
		// steal-then-Requeue, which is pure deque contention + worker thrash). Placement policy at steal
		// mirrors push: Default/Any/Wide tasks are stealable by EVERY worker (work-conserving for the
		// common case); explicit P/E tasks are only stolen by a matching-class thief -- corePref is the
		// sole placement authority, including across steals. degenerateTopology (non-hybrid CPU: one
		// class set empty) disables class checks entirely so nothing is ever unstealable.
		static bool StealClassCompatible(const Task* t, bool thiefIsP, bool degenerateTopology) {
			const CorePref p = t->corePref;
			if (p == CorePref::Default || p == CorePref::Wide) return true;   // Any aliases Wide
			if (degenerateTopology) return true;
			return (p == CorePref::P) ? thiefIsP : !thiefIsP;
		}

		Task* CreateTask(void(*fn)(void*), void* data, uint8_t hipri = false, FiberSize size = FiberSize::Standard, uint8_t noFiber = true, CorePref corePref = CorePref::Default);

		template<typename F>
		auto CreateTask(F&& f, uint8_t hipri = false, FiberSize size = FiberSize::Standard, uint8_t noFiber = true, CorePref corePref = CorePref::Default) {
			using L = LambdaTask<std::decay_t<F>>;
			static_assert(sizeof(L) <= TaskAllocator::SLOT, "lambda too big for a slot");
			static_assert(alignof(L) <= 16, "lambda over-aligned for the slot");
			void* mem = taskAllocator.Alloc();
			if (!mem) return static_cast<L*>(nullptr);
			L* t = ::new (mem) L(std::forward<F>(f));
 			t->hiPri = hipri;
			t->requiredSize = size;
			t->noFiber = noFiber;
			t->corePref = corePref;
			// ~LambdaTask is empty and its only member is the functor, so the destructor has work
			// to do only when the CAPTURES do. Non-capturing lambdas and captures of scalars or
			// raw pointers -- the overwhelming majority of task bodies -- skip the virtual call.
			t->trivialDtor = std::is_trivially_destructible_v<std::decay_t<F>> ? 1 : 0;
			return t;
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(uint8_t cpu_affinity, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t, cpu_affinity);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void PushImmediate(size_t coreID, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushToCore(coreID, t);
		}

	private:
		explicit TaskScheduler(size_t poolSize);

		// ---------- former SharedQueues state ----------
		std::atomic<uint64_t> nextId{ 0 };
		std::vector<std::unique_ptr<std::atomic<bool>>> immediateCoresInUse;
		std::atomic<bool> paused{ false };
		std::vector<std::unique_ptr<TaskDeque>> loPri;
		std::vector<std::unique_ptr<TaskDeque>> hiPri;
		std::vector<std::unique_ptr<TaskMPSCQueue>> loPriInboxes;
		std::vector<std::unique_ptr<TaskMPSCQueue>> hiPriInboxes;
		static GlobalFiberPool* globalPool;
		// -----------------------------------------------

		// ---- loPri starvation prevention: steal fairness ----
		// After kStealFairnessWindow consecutive hiPri steals, GetTask() forces a loPri scan so a
		// steady stream of hiPri work can't starve loPri tasks. (There used to also be age-based
		// promotion -- boost old loPri tasks to hiPri -- but it's redundant now that stealing is
		// single-item: a stolen task runs immediately, so the steal itself un-starves it.)
		int consecutiveHiPriSteals = 0;
		static constexpr int kStealFairnessWindow = 8; // after 8 hiPri steals, force a loPri scan
		uint64_t GetCurrentTimeMs() const;
		// ----

		// ---- Priority inheritance for locks (prevent inversion deadlock) ----
		// Priority boosts now stored directly on Task.priorityBoost (no lock needed)
		// ----


		void RunCounted(WaitGroup& wg, Task* t);
		static size_t GetSafeTC();
		// Steals ONE task (hiPri-then-loPri, with steal fairness) for a non-worker helper. nullptr
		// if nothing stealable. See definition.
		Task* GetTask();
		void StartPool(size_t poolSize);
		bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
		int PickNextWorker(CorePref pref = CorePref::Default);
		bool PushToCore(size_t core_id, Task* task);
		// Picks a worker from the requested class set (P/E), SPILLING to the other class if unavailable;
		// Default/Any/Wide (and non-hybrid / all-pinned) use the original full-pool round-robin. Placement
		// is governed SOLELY by CorePref -- hiPri is queue order only, never consulted for placement.
		// Preference is a hint -- never a constraint.

		// NOTE: an external-submitter fan-out cap was tried here and REMOVED. See CHANGELOG 1.1.1.
		// It made single-producer submission much faster and burst parallelism much worse, and it
		// could livelock: PushLocal spins on `while (immediateCoresInUse[chosen])` with no yield and
		// no widening, which is safe with thirty-one candidates and is not with four. If you are
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
		// (hiPri->P, loPri/bulk->E) + a P/E-aware core reserve -- needed manually because the pool is
		// HARD-PINNED (Thread::StartWorker SetThreadAffinityMask), so the OS can't place work P/E for us.
		std::vector<char> isPCore;
		// isPCpu[logical CPU] -- P/E class of every logical processor, same EfficiencyClass
		// derivation as isPCore but indexed by CPU, not worker. Needed because TryRunStolenNoFiberTask's
		// callers include NON-worker, possibly UNPINNED threads (main, or any app thread hitting a
		// SchedulerMutex/SchedulerConditionVariable spin): their class can't be assumed -- it's looked
		// up via GetCurrentProcessorNumber() at steal time ("this noFiber task would run HERE, right now").
		// Workers keep the cheaper static isPCore[qIndex] lookup (hard-pinned, class never changes).
		std::vector<char> isPCpu;
		// -----------------------------------------------

		static TaskScheduler* instance;
		TaskAllocator taskAllocator{ 1024 * 1024 }; // 1M tasks
		std::unordered_map<std::string, std::unique_ptr<Event>> eventRegistry;
		std::mutex registryMtx;
		EventPool eventPool{ 1024 };   // pooled DirectEvents for WaitOnEventDirectArmed
		std::atomic<bool> poolActive{ false };
		std::atomic<int> nextWorker{ 0 };
		// P/E routing (see PickNextWorker): worker qIndices split by efficiency class (from isPCore),
		// each with its own round-robin cursor. Built in StartPool. Preference is a HINT -- PickNextWorker
		// spills to the other class if the preferred one has no available worker, and an empty set (non-
		// hybrid CPU) just falls through to the full pool. Nothing here is a hard constraint.
		std::vector<int> pWorkers, eWorkers;
		std::atomic<size_t> nextPWorker{ 0 }, nextEWorker{ 0 };
		std::atomic<bool> stopFlag{ false };
		std::vector<std::shared_ptr<Thread>> workers;
		TaskMPSCQueue mainQ;
		std::mutex poolMutex;
	};
	// Priority-inheritance-aware mutex wrapper. When a task tries to lock and blocks, the
	// lock holder's priority is temporarily boosted to prevent priority inversion deadlock.
	class SchedulerMutex {
	private:
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;
		bool locked = false;
		Task* lockHolder = nullptr;
		std::queue<Fiber*> waitingFibers; // fibers waiting for the lock
		std::atomic_flag holderLock = ATOMIC_FLAG_INIT;

	public:
		SchedulerMutex() = default;
		~SchedulerMutex() = default;

		// Acquires the lock, boosting the holder's priority on contention to prevent inversion.
		//
		// Callable from EITHER context, and the two behave differently on contention:
		//   on a fiber      -- the fiber is queued and SUSPENDED, freeing the worker for other work.
		//   on a bare thread -- there is no fiber to suspend, so it spins, running one stolen
		//                       noFiber task per iteration (TryRunStolenNoFiberTask) instead of
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
	};

	class SchedulerSemaphore {
	private:
		std::mutex mtx;
		std::queue<Fiber*> waitingFibers;  // suspended fibers waiting for a permit
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT; // Must be here!
		int permits;
		const int maxPermits;

	public:

		explicit SchedulerSemaphore(int initialPermits, int maxPermits = INT_MAX)
			: permits(initialPermits), maxPermits(maxPermits) {}

		void Wait();

		bool Try_Wait();

		void Signal();

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

	class SchedulerConditionVariable {
	private:
		// User-space spinlock protecting the internal CV queue
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;

		// A queue of semaphores, each representing a waiting fiber context
		std::queue<SchedulerSemaphore*> waitingQueue;

		void LockQueue();
		void UnlockQueue();

	public:
		SchedulerConditionVariable() = default;
		~SchedulerConditionVariable() = default;

		// Fibers suspend here; noFiber tasks spin and steal work
		void Wait(SchedulerMutex& mutex);

		// Unblocks one waiting fiber context
		void Notify_One();

		// Unblocks all waiting fiber contexts
		void Notify_All();
	};
}

