#include "../include/Thread.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include "../include/platform.h"
#include <stdexcept>
#include <vector>
#include <chrono>
using namespace JLib;

static_assert(sizeof(Task) <= TaskAllocator::SLOT, "Task doesn't fit a slot");
static_assert(alignof(Task) <= 16, "Task over-aligned for a slot");

TaskScheduler* TaskScheduler::instance = nullptr;
GlobalFiberPool* TaskScheduler::globalPool = nullptr;

TaskScheduler::TaskScheduler(size_t poolSize) {
	StartPool(poolSize);
}
size_t TaskScheduler::GetSafeTC() {
	unsigned int cores = std::thread::hardware_concurrency();
	if (cores == 0) return 1;
	if (cores == 1) return 1;
	return static_cast<size_t>(cores - 1);
}
void TaskScheduler::Init(size_t poolSize) {
	if (instance != nullptr)
		throw std::runtime_error("TaskScheduler already initialized!");
	instance = new TaskScheduler(poolSize);

}
GlobalFiberPool& JLib::TaskScheduler::GetGlobalPool()
{
	if (!instance->globalPool)
		throw std::runtime_error("GlobalFiberPool not initialized!");
	return *instance->globalPool;

}
TaskScheduler::~TaskScheduler() {
	if (!stopFlag)
		Join();
}
bool TaskScheduler::PushMain(Task* task) {
	if (!poolActive) return false;
	if (!task) return false;
	mainQ.push(task);
	return true;
}
void TaskScheduler::ProcessMainThread() {
	if (!poolActive) return;
	Task* t;
	while (mainQ.pop(t)) {
		if (!t) continue;
		t->Execute();
		if (t->waitGroup) {
			if (t->waitGroup) {
				int old = t->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
				if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
					t->waitGroup->WakeAll();   // only touches wg if someone registered
			}
		}
		// Worker() frees a DEAD task after running it (see its FiberStatus::DEAD branch) --
		// this path was missing the equivalent, so every PushMain'd task leaked its slab.
		// Latent/unnoticed while PushMain was barely used; would leak fast once frame tasks
		// (StartFrame/Submit-producers/PresentFrame) route through here every frame.
		t->~Task();
		taskAllocator.Free(t);
	}
}
void TaskScheduler::WaitForMain(WaitGroup& wg) {
	while (wg.n.load(std::memory_order_acquire) > 0) {
		ProcessMainThread();  // drain any ready main-affinity DAG nodes
		std::this_thread::yield();
	}
}
void TaskScheduler::Join() {
	if (!poolActive) return;
	
	stopFlag.store(true, std::memory_order_release);

	{
		registryMtx.lock();
		for (auto& pair : eventRegistry)
			pair.second->SignalAll();

		registryMtx.unlock();
	}
	NotifyAll();

	for (auto& worker : workers)
		worker->Join();

	{
		poolMutex.lock();
		workers.clear();
		mainQ.clear();
		immediateCoresInUse.clear();
		poolMutex.unlock();
	}

	poolActive.store(false, std::memory_order_release);
}
void TaskScheduler::NotifyAll() {
	for (auto& w : workers)
		w->NotifyWorker();
}
void TaskScheduler::ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;
	if (totalItems > 10000) {
		int numTasks = (totalItems + chunkSize - 1) / chunkSize;

		// Chunk 0 is MAIN'S OWN LANE: the calling thread computes it as a plain inline call
		// (NOT a scheduled task), so it can never suspend/resume and never touches a fiber or
		// task slab. Chunks 1..numTasks-1 go to workers. This keeps all hw lanes busy without
		// making the caller a task-runner -- the caller stays a pure waiter for scheduled work.
		// See WaitFor for why the caller must not run scheduled tasks.
		const int mainChunkStart = start;
		const int mainChunkEnd = std::min(start + chunkSize, end);

		// Capturing `&func` (a by-value parameter) is safe ONLY because WaitFor(wg) below
		// blocks until every task tied to `wg` has completed -- the tasks never outlive this
		// frame. The completion decrement is done exclusively by the worker's waitGroup path
		// (Thread::Worker / TryRunStolenFastJob); the task body must NOT also decrement, or
		// each task counts down twice and the wait wakes early on half-finished work.
		WaitGroup wg;
		for (int i = 1; i < numTasks; ++i) {
			int chunkStart = start + i * chunkSize;
			int chunkEnd = std::min(chunkStart + chunkSize, end);

			Task* t = CreateTask([&func, chunkStart, chunkEnd]() {
				func(chunkStart, chunkEnd);
				});
			if (!t) {
				func(chunkStart, chunkEnd); // arena exhausted: graceful degradation, run it here
				continue;
			}
			t->waitGroup = &wg;
			wg.n.fetch_add(1, std::memory_order_relaxed);
			// MUST route through Push()/PushLocal, NOT a blind `loPriInboxes[i % n]->push()`
			// round-robin: PushLocal's PickNextWorker skips any core whose immediateCoresInUse
			// is set (a worker PINNED by a persistent PushImmediate/PushFork task -- e.g. the
			// audio subsystem's forever-running mixer). A pinned worker never returns to its
			// loop to drain its inbox, and inboxes are owner-drain-only (never stealable, so
			// TryRunStolenFastJob can't rescue them) -- so a chunk shoved into a pinned worker's
			// inbox is stranded forever and WaitFor(wg) spins until the heat death of the app.
			// This was the particle-demo deadlock: it only bit once the sound thread was pinned.
			// Push() also handles pendingTasks++/MarkQueuedWork/NotifyWorker.
			Push(t);
		}

		// Main computes its own lane while the workers churn -- no wasted thread.
		func(mainChunkStart, mainChunkEnd);

		// Block until every dispatched chunk is done (fiber callers park; non-fiber callers
		// spin-and-help via TryRunStolenFastJob inside WaitFor).
		WaitFor(wg);
	}
	else
	{
		for (int i = start; i < end; i++)
		{
			func(i, i + 1);
		}
	}
}
void TaskScheduler::ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);
		Push([=]() { func(chunkStart, chunkEnd); });
	}
}
// Queries real Windows topology (GetLogicalProcessorInformationEx) and fills siblingQIndex/
// clusterMates from it -- NOT from the sequential affinity scheme assumption (worker qIndex i
// is pinned to logical CPU i+1, main sits on 0). That mapping tells you what you ASKED the OS
// for, not what the hardware actually looks like (adjacent logical CPU numbers being SMT/cache
// neighbors is a common convention, never a guarantee). Limitation: only considers processor
// GROUP 0 (fine for the vast majority of desktop/workstation hardware, i.e. <=64 logical CPUs;
// a true >64-logical-CPU multi-group machine would need GROUP_AFFINITY.Group handled too).
// On ANY failure (API unsupported, nothing returned, etc.) falls back to safe defaults: no SMT
// sibling for anyone, and everyone in one big cluster -- equivalent to the old plain-random
// behavior, just never wrong.
static bool GetGroupMasksForRelation(LOGICAL_PROCESSOR_RELATIONSHIP relation, std::vector<ULONG_PTR>& outMasks) {
	DWORD len = 0;
	GetLogicalProcessorInformationEx(relation, nullptr, &len);
	if (len == 0) return false;

	std::vector<std::byte> buffer(len);
	auto* base = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data());
	if (!GetLogicalProcessorInformationEx(relation, base, &len)) return false;

	DWORD offset = 0;
	while (offset < len) {
		auto* info = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data() + offset);
		if (info->Relationship == relation) {
			const PROCESSOR_RELATIONSHIP& proc = info->Processor;
			for (WORD g = 0; g < proc.GroupCount; ++g) {
				if (proc.GroupMask[g].Group == 0) {
					outMasks.push_back(proc.GroupMask[g].Mask);
				}
			}
		}
		offset += info->Size;
	}
	return true;
}

void TaskScheduler::BuildTopology(unsigned int num_workers) {
	siblingQIndex.assign(num_workers, -1);
	clusterMates.assign(num_workers, {});

	// logical CPU -> qIndex, per the known affinity scheme (0 = main, i+1 = worker i).
	auto qIndexOf = [num_workers](int logicalCpu) -> int {
		if (logicalCpu < 1) return -1;
		unsigned int q = (unsigned int)(logicalCpu - 1);
		return (q < num_workers) ? (int)q : -1;
	};

	std::vector<ULONG_PTR> coreMasks;    // one entry per physical core -- SMT sibling groups
	std::vector<ULONG_PTR> cacheMasks;   // one entry per LLC instance -- cluster groups
	bool haveCores = GetGroupMasksForRelation(RelationProcessorCore, coreMasks);
	bool haveCache = GetGroupMasksForRelation(RelationCache, cacheMasks);

	if (haveCores) {
		for (ULONG_PTR mask : coreMasks) {
			std::vector<int> qsInGroup;
			for (int cpu = 0; cpu < 64; ++cpu) {
				if (mask & (ULONG_PTR(1) << cpu)) {
					int q = qIndexOf(cpu);
					if (q >= 0) qsInGroup.push_back(q);
				}
			}
			// Only meaningful if exactly two POOL WORKERS share this physical core -- if the
			// group is size 1 (its SMT sibling is main or an unused logical CPU) there's no
			// worker sibling to record.
			if (qsInGroup.size() == 2) {
				siblingQIndex[qsInGroup[0]] = qsInGroup[1];
				siblingQIndex[qsInGroup[1]] = qsInGroup[0];
			}
		}
	}

	if (haveCache) {
		for (ULONG_PTR mask : cacheMasks) {
			// RelationCache returns one record PER cache instance PER level (L1/L2/L3 all
			// come back through this same query) -- we only want the last-level one(s), which
			// in practice are the masks covering the MOST logical CPUs. Filtering by "biggest
			// masks seen" below (after the loop) is simpler than threading the cache Level
			// field through GetGroupMasksForRelation, so just collect qIndex groups for every
			// mask here and pick the highest-CPU-count group per worker afterward.
			std::vector<int> qsInGroup;
			for (int cpu = 0; cpu < 64; ++cpu) {
				if (mask & (ULONG_PTR(1) << cpu)) {
					int q = qIndexOf(cpu);
					if (q >= 0) qsInGroup.push_back(q);
				}
			}
			if (qsInGroup.size() < 2) continue;
			for (int q : qsInGroup) {
				// Keep the LARGEST group seen for this worker -- that's the last-level
				// (widest-sharing) cache instance rather than a narrower L1/L2 one.
				if (qsInGroup.size() > clusterMates[q].size() + 1) {
					clusterMates[q].clear();
					for (int other : qsInGroup) {
						if (other != q && other != siblingQIndex[q]) clusterMates[q].push_back(other);
					}
				}
			}
		}
	}
	else {
		// Fallback: no cache topology available -- treat the whole pool as one cluster so
		// locality-first-random-fallback degrades to exactly the old plain-random behavior.
		for (unsigned int q = 0; q < num_workers; ++q) {
			for (unsigned int other = 0; other < num_workers; ++other) {
				if (other != q && (int)other != siblingQIndex[q]) clusterMates[q].push_back((int)other);
			}
		}
	}
}

void TaskScheduler::StartPool(size_t poolSize) {
	poolMutex.lock();
	thread_counter.store(0, std::memory_order_release);
	if (poolSize == 0)
		poolSize = GetSafeTC();
	if (poolSize > GetSafeTC())
		poolSize = GetSafeTC();

	unsigned int num_workers = static_cast<unsigned int>(poolSize);
	// +1 for the main/submitting thread: it takes thread_id == num_workers at the end
	// of StartPool and uses epochs too (e.g. DAG AddDependency -> EnterEpoch). Sizing to
	// just num_workers leaves that slot out of bounds -> AV in Enter/LeaveEpoch.
	EpochManager::Instance().Init(num_workers + 1);
	BuildTopology(num_workers);
	stopFlag.store(false, std::memory_order_release);
	nextWorker = 0;
	unsigned int coreCount = std::thread::hardware_concurrency()-1;
	if (coreCount == 0) coreCount = 4; // Fallback

	size_t standardFiberCount = coreCount * 64;
	size_t heavyFiberCount = coreCount * 8;

	// 3. Ensure a minimum to avoid "thrashing"
	
	// GlobalFiberPool now owns all fibers and stack allocation
	globalPool = GlobalFiberPool::Create(standardFiberCount, heavyFiberCount);
	workers.clear();
	loPri.clear();
	immediateCoresInUse.clear();
	loPriInboxes.clear();
	hiPri.clear();
	hiPriInboxes.clear();
	workers.reserve(num_workers);
	loPri.reserve(num_workers);
	hiPri.reserve(num_workers);
	immediateCoresInUse.reserve(num_workers);
	loPriInboxes.reserve(num_workers);
	hiPriInboxes.reserve(num_workers);

	// mainQ (used by PushMain/ProcessMainThread, e.g. TaskDAG main-affinity nodes) was NEVER
	// init()'d -- its default ctor leaves head_/tail_/stub_ uninitialized. Harmless as long as
	// nothing actually called PushMain, which nothing did until real work started routing
	// through it (TaskDAG::Fire's isMain branch): TaskMPSCQueue::append() then wrote through a
	// garbage head_/prev pointer -> write access violation. One-time init, same as every other
	// TaskMPSCQueue (see loPriInboxes/hiPriInboxes below).
	mainQ.init(&taskAllocator);

	for (unsigned int i = 0; i < num_workers; ++i) {
		immediateCoresInUse.push_back(std::make_unique<std::atomic<bool>>(false));
		loPri.push_back(std::make_unique<TaskDeque>());
		hiPri.push_back(std::make_unique<TaskDeque>());
		loPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		hiPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		loPriInboxes[i]->init(&taskAllocator);
		hiPriInboxes[i]->init(&taskAllocator);
	}
	for (unsigned int i = 0; i < num_workers; ++i) {
		auto worker = std::make_shared<Thread>(*this);
		worker->SetQueueIndex(i);
		workers.push_back(worker);
		worker->StartWorker(i + 1);
	}
	for (auto& w : workers) {
		while (!w->Ready())
			std::this_thread::yield();
	}
	thread_id = thread_counter.fetch_add(1);
	poolActive.store(true, std::memory_order_release);
	poolMutex.unlock();
}

void TaskScheduler::WaitOnEvent(const std::string& eventName) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		// A fastJob task (see Task::fastJob) runs with no fiber underneath it -- there's
		// nothing to switch away to. This is a contract violation, not a transient failure.
		throw std::runtime_error("WaitOnEvent called from a task with no assigned fiber -- "
			"fastJob tasks must never suspend.");
	}

	auto& event = GetEvent(eventName);

	// Order matters. Become parkable (WANTS_SUSPEND) BEFORE registering, so any signal
	// that races in sees a resumable state (Resume() flips WANTS_SUSPEND->SUSPEND_SIGNALED
	// and the worker wakes us after the switch). AddWaiter only inserts -- it no longer
	// touches status. The event mutex serializes AddWaiter against Signal/SignalAll, so a
	// signal that lands after we register is guaranteed to find and wake us.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	event.AddWaiter(myTask);

	// Return via the fiber's homeCtx (the worker stamps it before each switch-in),
	// not thread_local schedulerCtx -- the waiter resumes on whatever worker the
	// event signal lands on, which may differ from this one.
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}
bool TaskScheduler::Push(Task* task) {
	return PushLocal(task);
}


void TaskScheduler::RunCounted(WaitGroup& wg, Task* t) {
	wg.n.fetch_add(1, std::memory_order_relaxed);
	t->waitGroup = &wg;
	Push(t);
}

void TaskScheduler::WaitFor(WaitGroup& wg) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		WaitOnEventDirectArmed([&wg](DirectEvent* ev) {
			std::lock_guard<std::mutex> lock(wg.mtx);
			wg.waiters.insert(ev);
			int old = wg.n.fetch_or(WaitGroup::WAITER_BIT, std::memory_order_acq_rel);
			if ((old & WaitGroup::COUNT_MASK) == 0) {
				wg.waiters.erase(ev);
				ev->Signal();   // already done -- wake ourselves so we don't park forever
			}
			});
		return;
	}
	else {
		while (wg.n.load(std::memory_order_acquire) > 0) {
			if (!TryRunStolenFastJob())
				std::this_thread::yield();
		}
	}
}
void JLib::TaskScheduler::PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity)
{
	// 1. Manually link them locally: Task A -> Task B -> Task C
	for (size_t i = 0; i < count - 1; ++i) {
		tasks[i]->next.store(tasks[i + 1], std::memory_order_relaxed);
	}
	// The last task's next is already handled by the queue's exchange logic
	pendingTasks.fetch_add(count, std::memory_order_relaxed);
	// 2. Submit the pointers directly - NO wrappers, NO heap allocation
	if (cpuaffinity == 0)
	{
		int chosen = PickNextWorker();
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			std::this_thread::yield();
			chosen = PickNextWorker();
		}
		loPriInboxes[chosen]->push_batch(tasks[0], tasks[count - 1]);
		// FIX: this whole function previously never notified ANYONE after the push -- if
		// `chosen` happened to be genuinely asleep, the entire batch would sit undiscovered
		// until that worker was woken for some unrelated reason (a different push landing on
		// it, etc.). A worker's own cv is private; nothing wakes it without an explicit notify
		// targeting it specifically.
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();
	}
	else
	{
		int chosen = cpuaffinity - 1;
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			std::this_thread::yield();
			chosen = PickNextWorker();
		}
		loPriInboxes[chosen]->push_batch(tasks[0], tasks[count - 1]);
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();
	}
}

bool TaskScheduler::Push(uint8_t cpu_affinity, Task* task) {
	return PushLocal(task, cpu_affinity);
}

bool TaskScheduler::PushImmediate(uint8_t cpu_affinity, Task* task) {
	if (!task) return false;
	return PushToCore(cpu_affinity, task);
}

bool TaskScheduler::PushFork(Task* task) {
	if (!task) return false;
	if (!poolActive) return false;

	int worker_id;
	bool is_local_push = false;

	if (IsOnFiber()) {
		worker_id = Thread::GetCurrent()->qIndex;
		is_local_push = true;  // Pushing to current worker, skip busy check
	}
	else {
		worker_id = PickNextWorker();
		if (worker_id < 0) return false;
	}

	// Only check busy flag if NOT pushing to current worker
	if (!is_local_push && immediateCoresInUse[worker_id]->load(std::memory_order_acquire))
		return false;

	if (!is_local_push) {
		immediateCoresInUse[worker_id]->store(true, std::memory_order_release);
	}

	task->isForked = 1;
	pendingTasks.fetch_add(1, std::memory_order_relaxed);

	return PushLocal(task, worker_id);
}
void TaskScheduler::WaitOnEventArmed(const std::string& eventName, const std::function<void()>& arm) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventArmed called from a task with no assigned fiber -- "
			"fastJob tasks must never suspend.");
	}

	auto& event = GetEvent(eventName);

	// Same ordering as WaitOnEvent: become parkable, then register as a waiter, so a signal
	// that races in is not lost (Resume flips WANTS_SUSPEND->SUSPEND_SIGNALED and the worker
	// wakes us after the switch). Crucially, run 'arm' only AFTER both -- the arm callback
	// hooks the external wakeup (e.g. a GPU fence), and must not be able to fire SignalAll
	// before this fiber is a discoverable, resumable waiter.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	event.AddWaiter(myTask);

	if (arm) arm();

	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}

void TaskScheduler::WaitOnEventDirectArmed(const std::function<void(DirectEvent*)>& arm) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventDirectArmed called from a task with no assigned "
			"fiber -- fastJob tasks must never suspend.");
	}

	DirectEvent* e = eventPool.Acquire();   // pool sized for max concurrent waits (never null in practice)

	// Ordering is load-bearing and identical in spirit to WaitOnEventArmed:
	//  1. become parkable FIRST -- if we published the waiter first, a signal could Resume() us
	//     while still RUNNING, and Resume() is a no-op for unrecognized states -> LOST wakeup.
	//  2. publish the waiter, so a signal that lands now finds a resumable target.
	//  3. arm the external wakeup only AFTER both.
	//  4. suspend.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	e->waiter.store(myTask, std::memory_order_release);

	if (arm) arm(e);

	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);

	// Resumed: WE own release. Signal() already exchanged waiter->null and will not touch e again.
	eventPool.Release(e);
}

bool TaskScheduler::IsOnFiber() {
	auto* t = Thread::GetCurrent();
	// currentRunningTask alone isn't enough -- a fastJob task sets it too (see Worker()'s fast
	// path) but deliberately never gets a fiber. Callers use this to decide whether
	// WaitOnEvent*-style suspension is safe, so it must be false for a fastJob task.
	return t != nullptr && t->currentRunningTask != nullptr && t->currentFiber != nullptr;
}

Event& TaskScheduler::GetEvent(const std::string& name) {
	registryMtx.lock();
	if (eventRegistry.find(name) == eventRegistry.end())
		eventRegistry[name] = std::make_unique<Event>();
	Event& event = *eventRegistry[name];
	registryMtx.unlock();
	return event;
}
void TaskScheduler::Pause() {
	paused.store(true, std::memory_order_release);
}
void TaskScheduler::Resume() {
	paused.store(false, std::memory_order_release);
	NotifyAll();
}
void TaskScheduler::Stop(Task* worker_task) {
	// Only the SCHEDULER's own stopFlag matters -- Task::stopFlag was removed (it never had a
	// single reader; workers only ever check this scheduler-level flag). The parameter is kept
	// for API compatibility with existing callers.
	(void)worker_task;
	stopFlag.store(true, std::memory_order_release);
}


// Batch version of GetTask() -- same random-start, hiPri-then-loPri scan, but pulls up to
// maxCount items from whichever deque's steal_batch() actually hits, amortizing the CAS cost
// across the batch instead of paying one CAS per stolen task. Caller (there's no per-caller
// "own deque" assumption here -- this is used by non-workers too, e.g. main) is responsible
// for what happens to items beyond out[0].
// NOW INCLUDES: fairness (alternate hiPri/loPri scans) and age-based promotion (boost old loPri tasks).
size_t TaskScheduler::GetTaskBatch(Task** out, size_t maxCount) {
	uint64_t now = GetCurrentTimeMs();

	// Fairness: after kStealFairnessWindow consecutive hiPri steals, force a loPri scan
	bool forceLoPri = (consecutiveHiPriSteals >= kStealFairnessWindow);

	if (!forceLoPri) {
		// Normal priority: try hiPri first
		size_t numThreads = hiPri.size();
		size_t start = rand() % numThreads;
		for (size_t i = 0; i < numThreads; ++i) {
			size_t target = (start + i) % numThreads;
			size_t n = hiPri[target]->steal_batch(out, maxCount);
			if (n > 0) {
				consecutiveHiPriSteals++;
				return n;
			}
		}
	}

	// Try loPri (either forced or as fallback)
	consecutiveHiPriSteals = 0; // reset fairness counter when we actually steal from loPri
	size_t numThreads = loPri.size();
	size_t start = rand() % numThreads;
	for (size_t i = 0; i < numThreads; ++i) {
		size_t target = (start + i) % numThreads;
		size_t n = loPri[target]->steal_batch(out, maxCount);
		if (n > 0) {
			PromoteAgedStolen(out, n);
			return n;
		}
	}

	return 0;
}

void TaskScheduler::PromoteAgedStolen(Task** batch, size_t n) {
	// RACE-FREE by construction: the caller reached here only after a steal_batch CAS removed
	// batch[0..n) from a deque, so this thread now EXCLUSIVELY owns them -- no other worker or
	// thief can read/write these tasks until we hand them back, and that handoff (Requeue /
	// push_bottom) publishes this plain write via the queue's own release/acquire atomics.
	// (The old disabled-code fears were both wrong: no mid-steal race exists post-CAS, and the
	// "task loss in ParallelFor" it was blamed for was the pinned-core inbox-stranding deadlock.)
	//
	// Setting hiPri only MATTERS at a priority-aware enqueue -- a task's DEQUE is its priority;
	// steal/pop/push_bottom never read the flag. So callers MUST route a promoted task to the
	// hiPri deque/inbox afterward (GetTaskBatch's non-fastJob leftovers go via Requeue, which is
	// hiPri-aware; Thread::Worker routes its leftovers by hiPri explicitly) or the boost is inert.
	//
	// uint32 wall-clock diff: queuedTimeMs is a truncated uint32 stamp, so compare in uint32 too
	// (wraparound-safe for the small ages involved).
	uint32_t nowMs = (uint32_t)GetCurrentTimeMs();
	for (size_t k = 0; k < n; ++k) {
		if (!batch[k]->hiPri &&
			(nowMs - batch[k]->queuedTimeMs) > kAgePromotionThresholdMs) {
			batch[k]->hiPri = 1;
		}
	}
}

bool TaskScheduler::TryRunStolenFastJob() {
	// Steal up to 4 in one CAS instead of one steal() per task -- everything beyond the first
	// is bonus work found "for free" alongside it. Each item still gets the SAME per-item
	// contract as before: fastJob runs inline with full completion bookkeeping, non-fastJob
	// goes back via Requeue() (can't safely run here -- no fiber).
	constexpr size_t kBatchCap = 4;
	Task* batch[kBatchCap];
	size_t n = GetTaskBatch(batch, kBatchCap);
	if (n == 0)
		return false;

	for (size_t i = 0; i < n; ++i) {
		Task* task = batch[i];
		if (!task->fastJob) {
			// Requeue does NOT touch pendingTasks -- it's only relocating an already-counted task.
			Requeue(task);
			continue;
		}
		task->Execute();
		if (task->waitGroup) {
			int old = task->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
			if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
				task->waitGroup->WakeAll();   // only touches wg if someone registered
		}
		task->~Task();
		taskAllocator.Free(task);
		pendingTasks.fetch_sub(1, std::memory_order_acq_rel);
	}

	if (EpochManager::Instance().RetiredCount() > 512) {
		EpochManager::Instance().Tick();
	}
	return true;
}

TaskAllocator* TaskScheduler::GetAllocator() {
	return &taskAllocator;
}
void TaskScheduler::WaitAll() {
	while (pendingTasks.load(std::memory_order_acquire) > 0)
		std::this_thread::yield();
}

Task* TaskScheduler::CreateTask(void(*fn)(void*), void* data, uint8_t hipri, FiberSize size, uint8_t fastJob) {
	void* mem = taskAllocator.Alloc();
	if (!mem) return nullptr;
	Task* t = ::new (mem) Task(fn, data, hipri, size);
	t->fastJob = fastJob;
	return t;
}

bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task) return false;

	// Record queuedTime for age-based promotion (no lock needed: only this thread touches this task)
	if (!task->hiPri) {
		task->queuedTimeMs = (uint32_t)GetCurrentTimeMs();
	}

	size_t num_workers = workers.size();
	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			loPriInboxes[idx]->push(task);
			pendingTasks.fetch_add(1, std::memory_order_release);
			// Targeted at worker idx specifically, not NotifyAll() -- only that one worker's
			// inbox actually changed. MarkQueuedWork() (release-ordered, matching
			// Thread.h's hasQueuedWork comment) pairs with the worker's own acquire-load in its
			// sleep predicate, closing the same notify-loss race the old blanket approach
			// happened to also close, just without waking every other worker for nothing.
			workers[idx]->MarkQueuedWork();
			workers[idx]->NotifyWorker();
		}
		else
			return false;
	}
	else {
		uint8_t chosen = PickNextWorker();
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			chosen = PickNextWorker();
		}
		if(task->hiPri)
			hiPriInboxes[chosen]->push(task);
		else
			loPriInboxes[chosen]->push(task);
		pendingTasks.fetch_add(1, std::memory_order_release);
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();

	}
	return true;
}
bool TaskScheduler::Requeue(Task* task) {
	if (!task) return false;
	// Re-queue a paused task (resumed after Suspend). Unlike PushLocal this does NOT
	// bump pendingTasks -- the task was already counted at its original submission and
	// is only resuming, not newly created. (The yield path does the same, via the
	// worker's push_bottom.) Otherwise every suspend->resume cycle leaks +1.
	uint8_t chosen = PickNextWorker();
	while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
		chosen = PickNextWorker();
	}
	if(task->hiPri)
		hiPriInboxes[chosen]->push(task);
	else
		loPriInboxes[chosen]->push(task);
	workers[chosen]->MarkQueuedWork();
	workers[chosen]->NotifyWorker();
	return true;
}
bool TaskScheduler::PushToCore(size_t core_id, Task* task) {
	if (core_id < 1) return false;
	if (!poolActive) return false;
	if (!task) return false;

	size_t idx = (core_id - 1) % workers.size();
	if (immediateCoresInUse[idx]->load(std::memory_order_acquire)) return false;

	// Marks this core busy-with-a-fork until Thread::Worker() clears it on completion (see
	// the is_handling_fork cleanup in both the fastJob and fiber-DEAD paths). If the forked
	// task never returns (a long-running subsystem pinned here for the program's lifetime),
	// this correctly STAYS true forever -- which is what makes PickNextWorker()'s existing
	// skip-if-busy check actually mean something: without setting this, a never-returning fork
	// would leave this worker's INBOX (not its deque -- that's still fully stealable) silently
	// accepting new round-robin-dispatched work that nothing would ever drain again.
	immediateCoresInUse[idx]->store(true, std::memory_order_release);

	// Deliberately does NOT call MarkQueuedWork(): a forked/immediate task bypasses the shared
	// deques/inboxes entirely (goes straight into workers[idx]->immediateTask below) and wakes
	// ONLY that one targeted worker via SetImmediateTask's own `immediate` flag + notify --
	// hasQueuedWork is specifically for the deque/inbox case, which this isn't.
	pendingTasks.fetch_add(1, std::memory_order_relaxed);
	workers[idx]->SetImmediateTask(task);
	workers[idx]->NotifyWorker();
	return true;
}
int TaskScheduler::PickNextWorker() {
	size_t n = workers.size();
	for (size_t i = 0; i < n; ++i) {
		size_t idx = (nextWorker + i) % n;
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			nextWorker = (idx + 1) % n;
			return static_cast<int>(idx);
		}
	}
	int fallback = static_cast<int>(nextWorker);
	nextWorker = (fallback + 1) % n;
	return fallback;
}

// ---- Starvation prevention implementation ----
uint64_t TaskScheduler::GetCurrentTimeMs() const {
	using namespace std::chrono;
	return duration_cast<milliseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

// ---- Priority inheritance implementation ----
Task* TaskScheduler::GetCurrentTask() const {
	// Access the current worker thread via thread-local storage (Thread::GetCurrent() returns
	// the thread_local Thread::instance if this is a worker thread, nullptr otherwise).
	// If we're on a worker thread with a running task, return it; otherwise nullptr.
	Thread* currentThread = Thread::GetCurrent();
	if (currentThread && currentThread->currentRunningTask) {
		return currentThread->currentRunningTask;
	}
	return nullptr; // Not on a worker thread, or no task currently running
}

void TaskScheduler::BoostTaskPriority(Task* task) {
	if (!task) return;

	// Only boost if not already boosted (no lock needed: only one thread modifies this task)
	if (task->priorityBoost == 0) {
		task->priorityBoost = task->hiPri;
		task->hiPri = 1; // boost to high priority
	}
}

void TaskScheduler::UnboostTaskPriority(Task* task) {
	if (!task) return;
	// Restore original priority if boosted (no lock needed: only one thread modifies this task)
	if (task->priorityBoost != 0) {
		task->hiPri = task->priorityBoost; // restore original priority
		task->priorityBoost = 0;
	}
}

void TaskScheduler::CleanupTaskMetadata(Task* task) {
	if (!task) return;
	// Metadata is stored directly on task, no cleanup needed (task is about to be freed anyway)
	// Just restore priority if it was boosted
	UnboostTaskPriority(task);
}

void SchedulerMutex::Lock() {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		// Fiber context: suspend on contention instead of blocking thread
		Task* callerTask = (TaskScheduler::IsInitialized()) ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
			if (!locked) {
				locked = true;
				spinLock.clear(std::memory_order_release);
				{
					while (holderLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
					lockHolder = callerTask;
					holderLock.clear(std::memory_order_release);
				}
				return;
			}
			waitingFibers.push(current);
			spinLock.clear(std::memory_order_release);
		}
		Thread::Suspend(current);
		// Resumed: we have the lock
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
	}
	else {
		// Fast job: try to run stolen work while spinning on lock
		while (!Try_Lock()) {
			if (TaskScheduler::IsInitialized()) {
				if (!TaskScheduler::Instance().TryRunStolenFastJob()) {
					_mm_pause();
				}
			}
			else {
				_mm_pause();
			}
		}
	}
}

void SchedulerMutex::Unlock()
{
	Task* wasHolder;
	Fiber* nextFiber = nullptr;
	{
		while (holderLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
		wasHolder = lockHolder;
		lockHolder = nullptr;
		holderLock.clear(std::memory_order_release);
	}

	{
		while (spinLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
		if (!waitingFibers.empty()) {
			nextFiber = waitingFibers.front();
			waitingFibers.pop();
		}
		else {
			locked = false;
		}
		spinLock.clear(std::memory_order_release);
	}

	// Only unboos if scheduler is initialized AND it's not a recursive/shutdown path
	if (wasHolder && TaskScheduler::IsInitialized()) {
		TaskScheduler::Instance().UnboostTaskPriority(wasHolder);
	}

	if (nextFiber) {
		Thread::Resume(nextFiber);
	}
}

bool SchedulerMutex::Try_Lock()
{
	while (spinLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
	if (!locked) {
		locked = true;
		spinLock.clear(std::memory_order_release);
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
			Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
		return true;
	}
	spinLock.clear(std::memory_order_release);
	return false;
}

void SchedulerSemaphore::Wait() {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;
	if (current != nullptr) {
		{
			// Tight spin-lock to protect inner state variables in user-space
			while (spinLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }

			if (permits > 0) {
				--permits;
				spinLock.clear(std::memory_order_release);
				return;
			}
			waitingFibers.push(current);
			spinLock.clear(std::memory_order_release);
		}
		Thread::Suspend(current);
	}
	else {
		// Fast job: continuous loop until permit is successfully acquired
		while (!Try_Wait()) {
			if (TaskScheduler::IsInitialized()) {
				if (!TaskScheduler::Instance().TryRunStolenFastJob()) {
					_mm_pause();
				}
			}
			else
				_mm_pause();
		}
	}
}

bool SchedulerSemaphore::Try_Wait() {
	while (spinLock.test_and_set(std::memory_order_acquire)) { _mm_pause(); }
	if (permits > 0) {
		--permits;
		spinLock.clear(std::memory_order_release);
		return true;
	}
	spinLock.clear(std::memory_order_release);
	return false;
}

void SchedulerSemaphore::Signal()
{
	// 1. Acquire the user-space spinlock
	while (spinLock.test_and_set(std::memory_order_acquire)) {
		_mm_pause();
	}

	// 2. Safely manipulate the queue and permits
	if (!waitingFibers.empty()) {
		Fiber* fiber = waitingFibers.front();
		waitingFibers.pop();

		// 3. Release lock BEFORE resuming the fiber to minimize contention overhead
		spinLock.clear(std::memory_order_release);

		Thread::Resume(fiber);
	}
	else {
		if (permits < maxPermits) {
			++permits;  // no one waiting, just increment
		}
		// 3. Release lock on this execution path
		spinLock.clear(std::memory_order_release);
	}
}

void SchedulerConditionVariable::LockQueue() {
	while (spinLock.test_and_set(std::memory_order_acquire)) {
		_mm_pause();
	}
}

void SchedulerConditionVariable::UnlockQueue() {
	spinLock.clear(std::memory_order_release);
}

void SchedulerConditionVariable::Wait(SchedulerMutex& mutex) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		// 1. Create a transient, local semaphore initialized to 0 permits
		SchedulerSemaphore localWaitSemaphore(0, 1);

		// 2. Lock the CV internal queue and push our wait handle
		LockQueue();
		waitingQueue.push(&localWaitSemaphore);
		UnlockQueue();

		// 3. Release the outer engine mutex so other threads/fibers can work
		mutex.Unlock();

		// 4. Suspend the fiber by waiting on our local semaphore.
		// Your existing semaphore code handles fiber suspension seamlessly here!
		localWaitSemaphore.Wait();

		// 5. Re-acquire the outer engine lock before returning control to the task
		mutex.Lock();
	}
	else {
		// Fast Job fallback
		mutex.Unlock();
		if (TaskScheduler::IsInitialized()) {
			if (!TaskScheduler::Instance().TryRunStolenFastJob())
				_mm_pause();
		}
		else {
			_mm_pause();
		}
		mutex.Lock();
	}
}

void SchedulerConditionVariable::Notify_One() {
	SchedulerSemaphore* nextSemaphore = nullptr;

	LockQueue();
	if (!waitingQueue.empty()) {
		nextSemaphore = waitingQueue.front();
		waitingQueue.pop();
	}
	UnlockQueue();

	// Signal the semaphore out-of-lock to maximize throughput
	if (nextSemaphore) {
		nextSemaphore->Signal();
	}
}

void SchedulerConditionVariable::Notify_All() {
	std::queue<SchedulerSemaphore*> localQueue;

	// Flush the global wait list into a local thread-isolated stack instantly
	LockQueue();
	std::swap(waitingQueue, localQueue);
	UnlockQueue();

	// Signal all waiting contexts sequentially
	while (!localQueue.empty()) {
		SchedulerSemaphore* sem = localQueue.front();
		localQueue.pop();
		sem->Signal();
	}
}