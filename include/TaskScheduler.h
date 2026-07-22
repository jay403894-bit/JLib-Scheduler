#pragma once
#define NOMINMAX
#include "Task.h"
#include "TaskMPSCQueue.h"
#include "Epochs.h"
#include "TaskDeque.h"
#include "TaskAllocator.h"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <unordered_map>
#include <thread>
#include <immintrin.h>
#include <queue>
#include "GlobalFiberPool.h"
#include "DirectEvent.h"
namespace JLib {
	class Thread;
	class Event;	

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
		void ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func);
		void ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func);
		bool Push(Task* task);
		void WaitFor(WaitGroup& wg);
		bool Push(uint8_t cpu_affinity, Task* task);
		bool Requeue(Task* task);
		void PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity=0);
		bool PushImmediate(uint8_t cpu_affinity, Task* task);
		bool PushFork(Task* task);
		static bool IsInitialized() {
			return instance != nullptr;
		}
		GlobalFiberPool& GetGlobalPool();
		Event& GetEvent(const std::string& name);
		void WaitOnEvent(const std::string& eventName);
		// Like WaitOnEvent, but runs 'arm' AFTER this fiber is registered as a waiter and
		// marked parkable (WANTS_SUSPEND), and BEFORE it actually suspends. Use it to arm an
		// external wakeup (e.g. a GPU-fence completion callback that will SignalAll this
		// event) with no lost-wakeup race: any signal that fires once 'arm' has run is
		// guaranteed to find a registered, resumable waiter. Must be called from a fiber.
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
		void WaitAll();

		// Lets a non-worker caller (e.g. main, while spinning on a WaitGroup/counter) safely
		// help drain the pool instead of pure-spinning. Steals ONE task via GetTask() (plain
		// steal() across every deque -- safe for any number of concurrent thieves, worker or
		// not) and either runs it or hands it back:
		//  - fastJob (the common case, see Task::fastJob): runs Execute() inline right here,
		//    then frees it with the EXACT SAME sequence Worker()'s fast path uses (~Task(),
		//    Free(), pendingTasks decrement, EBR tick check) -- required so the slab and
		//    pendingTasks/WaitAll() stay correct; skipping any one of these either leaks a
		//    slab slot or hangs a WaitAll().
		//  - NOT fastJob: this caller has no fiber to suspend it on if it ever calls
		//    WaitOnEvent*, so it's handed back via Requeue() (does NOT touch pendingTasks --
		//    the task was already counted, it's only relocating) for a real worker to run.
		// Returns true if it did anything (ran or requeued a task), false if there was nothing
		// to steal -- callers should yield() on false to avoid a hot spin.
		bool TryRunStolenFastJob();

		Task* CreateTask(void(*fn)(void*), void* data, uint8_t hipri = false, FiberSize size = FiberSize::Standard, uint8_t fastJob = true);

		template<typename F>
		auto CreateTask(F&& f, uint8_t hipri = false, FiberSize size = FiberSize::Standard, uint8_t fastJob = true) {
			using L = LambdaTask<std::decay_t<F>>;
			static_assert(sizeof(L) <= TaskAllocator::SLOT, "lambda too big for a slot");
			static_assert(alignof(L) <= 16, "lambda over-aligned for the slot");
			void* mem = taskAllocator.Alloc();
			if (!mem) return static_cast<L*>(nullptr);
			L* t = ::new (mem) L(std::forward<F>(f));
 			t->hiPri = hipri;
			t->requiredSize = size;
			t->fastJob = fastJob;
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
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void PushFork(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushFork(t);
		}

	private:
		explicit TaskScheduler(size_t poolSize);

		// ---------- former SharedQueues state ----------
		std::atomic<uint64_t> nextId{ 0 };
		std::atomic<int> pendingTasks{ 0 };
		std::vector<std::unique_ptr<std::atomic<bool>>> immediateCoresInUse;
		std::atomic<bool> paused{ false };
		std::vector<std::unique_ptr<TaskDeque>> loPri;
		std::vector<std::unique_ptr<TaskDeque>> hiPri;
		std::vector<std::unique_ptr<TaskMPSCQueue>> loPriInboxes;
		std::vector<std::unique_ptr<TaskMPSCQueue>> hiPriInboxes;
		static GlobalFiberPool* globalPool;
		// -----------------------------------------------

		// ---- Starvation prevention (age-based promotion + fairness) ----
		// Queued timestamps now stored directly on Task.queuedTimeMs (no lock needed)
		static constexpr uint64_t kAgePromotionThresholdMs = 50; // promote loPri if waiting > 50ms
		int consecutiveHiPriSteals = 0;
		static constexpr int kStealFairnessWindow = 8; // after 8 hiPri steals, force a loPri scan
		uint64_t GetCurrentTimeMs() const;
		// ----

		// ---- Priority inheritance for locks (prevent inversion deadlock) ----
		// Priority boosts now stored directly on Task.priorityBoost (no lock needed)
		// ----


		void RunCounted(WaitGroup& wg, Task* t);
		static size_t GetSafeTC();
		// Batch version of GetTask() -- see definition for details.
		size_t GetTaskBatch(Task** out, size_t maxCount);
		// Age-based promotion applied to a batch the caller has just EXCLUSIVELY acquired via a
		// steal_batch CAS (post-steal ownership = race-free; see definition). Shared by BOTH
		// steal sites -- GetTaskBatch (main's spin helper) AND Thread::Worker's own steal path --
		// so promotion isn't limited to the rare main-thread-spinning case. Call only with tasks
		// stolen from a loPri deque (a hiPri steal is already top priority).
		void PromoteAgedStolen(Task** batch, size_t n);
		void StartPool(size_t poolSize);
		bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
		bool PushToCore(size_t core_id, Task* task);
		int PickNextWorker();

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
		// siblingQIndex[qIndex] -- the OTHER worker qIndex sharing this worker's physical core
		// (SMT sibling), or -1 if none (no SMT, or the sibling logical CPU isn't a pool worker
		// -- e.g. it's main's). Only stolen from if idle (see Thread::busy) -- a busy SMT
		// sibling shares this worker's execution ports, so stealing its work doesn't recruit
		// any additional throughput, just adds queued work to an already-contended core.
		std::vector<int> siblingQIndex;
		// -----------------------------------------------

		static TaskScheduler* instance;
		TaskAllocator taskAllocator{ 1024 * 1024 }; // 1M tasks
		std::unordered_map<std::string, std::unique_ptr<Event>> eventRegistry;
		std::mutex registryMtx;
		EventPool eventPool{ 1024 };   // pooled DirectEvents for WaitOnEventDirectArmed
		std::atomic<bool> poolActive{ false };
		std::atomic<int> nextWorker{ 0 };
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

		// Acquires the lock. If the caller blocks on contention, boosts the lock holder's
		// priority to prevent priority inversion. Must be called from a fiber (task context).
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

		// Fibers suspend here; FastJobs spin/steal work
		void Wait(SchedulerMutex& mutex);

		// Unblocks one waiting fiber context
		void Notify_One();

		// Unblocks all waiting fiber contexts
		void Notify_All();
	};
}
