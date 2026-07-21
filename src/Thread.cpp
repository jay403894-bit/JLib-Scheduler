#include "../include/Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include <chrono>
#include <iostream>
using namespace JLib;
thread_local Thread* Thread::instance = nullptr;

Thread::Thread(TaskScheduler& scheduler) : scheduler(&scheduler) {
	std::memset(&schedulerCtx, 0, sizeof(Context));
}
Thread::~Thread() {
}
void Thread::StartWorker(size_t cpu_affinity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		instance = this;
		thread_id = thread_counter.fetch_add(1);
		unsigned int workerThreads = (scheduler->workers.size() > 1) ? scheduler->workers.size() : 1;
		size_t idealCapacity = ((workerThreads * 72) / workerThreads) * 0.5;
		if (idealCapacity < 16) idealCapacity = 16;
		localCache.Initialize(&scheduler->GetGlobalPool(),idealCapacity);
		this->Worker();
		});
	nativeHandle = thread.native_handle();
#ifdef _WIN32
	SetThreadAffinityMask(nativeHandle, 1ULL << cpu_affinity);
#endif
	ready->store(true, std::memory_order_release);
};
std::thread::id Thread::GetID() {
	return thread.get_id();
}
bool Thread::SetImmediateTask(Task* new_task) {
	if (!new_task) return false;
	{
		immediateTask = new_task;
		immediate.store(true, std::memory_order_release);
	}
	cv.notify_one();
	return true;
}
void Thread::SetQueueIndex(size_t index)
{
	qIndex = index;
};
void Thread::Join() {
	bool expected = false;
	if (!joining.compare_exchange_strong(expected, true)) return;

	running.store(false, std::memory_order_release);
	NotifyWorker();

	std::unique_lock<std::mutex> lock(joinMutex);
	cvWorkerDone.wait(lock, [this] {
		return !running.load(std::memory_order_acquire);
		});

	if (thread.joinable())
		thread.join();

	joining.store(false, std::memory_order_release);
}
Thread* Thread::GetCurrent() { 
	return instance; 
}

void Thread::CoYield(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->CoYield();
	}
}
void Thread::Suspend(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->Suspend();
	}
}
 void Thread::Resume(Fiber* targetFiber) {
	 if (targetFiber) {
		 targetFiber->Resume(); 
	 }
}

 void JLib::Thread::CoYield()
 {
	 GetCurrent()->currentFiber->CoYield();
 }

 void JLib::Thread::Suspend()
 {
	 GetCurrent()->currentFiber->Suspend();
 }

 uint64_t Thread::GenerateID() {
	 return scheduler->nextId.fetch_add(1, std::memory_order_relaxed);
 }
void Thread::NotifyWorker(){
	// The empty lock is load-bearing: cv.notify_one() without synchronizing on workerMutex
	// can land in the window AFTER Worker()'s sleep predicate evaluated false but BEFORE it
	// actually blocks -- the notify is dropped, the flag store is never re-checked, and the
	// worker sleeps on a non-empty inbox forever. Inboxes are only drainable by their owner
	// (steals never scan them), so one lost wakeup = that task stranded permanently (this was
	// the ParallelFor heisen-deadlock). Acquiring the mutex forces the notify to land either
	// before the predicate runs (it sees the flag) or after the worker is blocked (it wakes).
	{ std::lock_guard<std::mutex> g(workerMutex); }
	cv.notify_one();
}

bool Thread::Ready(){
	return ready.load(std::memory_order_acquire);
}
       
Fiber* Thread::AcquireFiber(Task* task) {
	Fiber* f = localCache.Pop();
	if (f) return f;

	f = localCache.Pop();

	if (!f) {
		std::cerr << "CRITICAL: Global pool exhausted!" << std::endl;
	}
	return f;
}

void Thread::ReleaseFiber(Fiber* f) {
	localCache.Push(f);
}

uint32_t Thread::FastRand() {
	static thread_local uint32_t x = []() {
		auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
		uint32_t seed = static_cast<uint32_t>(now);
		seed ^= (std::hash<std::thread::id>{}(std::this_thread::get_id()) << 1);
		return seed == 0 ? 1 : seed;
		}();	
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	return x;
}
Task* JLib::Thread::AcquireWork(bool& isFork)
{
	return nullptr;
}
void JLib::Thread::RunTask(Task* task, bool isFork)
{}
void Thread::Worker() {
	running.store(true, std::memory_order_release);
	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	static thread_local Task* task_to_run = nullptr;
	static thread_local bool is_handling_fork = false;
	while (running.load(std::memory_order_acquire)) {

		ready.store(true, std::memory_order_release);
		// Cleared once per iteration, unconditionally, BEFORE this iteration's own-queue/steal/
		// inbox-drain search (steps 3-5 below) -- so any push that landed before this clear gets
		// found directly by that same search (turning task_to_run non-null, never reaching the
		// sleep predicate at all this iteration), and any push landing AFTER the clear re-arms
		// this flag (via MarkQueuedWork(), called by whichever push targeted this worker) for
		// the sleep predicate to see fresh, later in this same iteration. See hasQueuedWork's
		// declaration comment in Thread.h for the full reasoning.
		hasQueuedWork.store(false, std::memory_order_relaxed);
		// --- 1. Execute task if found ---
		if (task_to_run) {
			// Fast path: run directly on THIS worker's own OS-thread stack, no fiber acquired
			// or ContextSwitch paid at all. Only safe because fastJob tasks are a CONTRACT --
			// they must never call WaitOnEvent*/anything that suspends (there's no fiber here
			// to switch away to). assignedFiber is deliberately left nullptr for these tasks,
			// which is exactly what WaitOnEvent*'s guards check for -- a mismarked fastJob
			// task that tries to suspend anyway fails loudly there instead of corrupting the
			// worker's real call stack.
			if (task_to_run->fastJob) {
				currentRunningTask = task_to_run;
				busy.store(true, std::memory_order_relaxed);
				task_to_run->Execute();
				if (task_to_run->waitGroup) {
					int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						task_to_run->waitGroup->WakeAll();   // only touches wg if someone registered
				}
				busy.store(false, std::memory_order_relaxed);
				currentRunningTask = nullptr;

				bool was_forked = task_to_run->isForked;  // Save before destruction
				scheduler->CleanupTaskMetadata(task_to_run);
				task_to_run->~Task();
				scheduler->GetAllocator()->Free(task_to_run);
				scheduler->pendingTasks.fetch_sub(1, std::memory_order_acq_rel);

				// Clear busy flag for both immediate (is_handling_fork) and load-balanced forks (was_forked)
				if (is_handling_fork || was_forked) {
					if (qIndex < (int)scheduler->immediateCoresInUse.size()) {
						scheduler->immediateCoresInUse[qIndex]->store(false, std::memory_order_release);
					}
					is_handling_fork = false;
				}
				if (EpochManager::Instance().RetiredCount() > 512) {
					EpochManager::Instance().Tick();
				}
				task_to_run = nullptr;
				ready.store(true, std::memory_order_release);
				continue;
			}

			Fiber* existingFiber = task_to_run->assignedFiber;

			Fiber* f;
			if (existingFiber) {
				f = existingFiber;      // resume existing context
			}
			else {
				f = AcquireFiber(task_to_run);
				if (!f) {
					// No fiber available right now (transient -- fibers are in use and will
					// free up). Re-queue WITHOUT losing the task's origin:
					//  - A forked/immediate task is pinned to THIS core, so restore it as the
					//    immediate task and leave immediateCoresInUse[qIndex] set (it's still
					//    pending here, and that flag also stops a new fork from clobbering it).
					//  - A regular task goes back on the local deque to be retried or stolen.
					if (is_handling_fork) {
						immediateTask = task_to_run;
						immediate.store(true, std::memory_order_release);
					}
					else {
						scheduler->loPri[qIndex]->push_bottom(task_to_run);
					}
					std::this_thread::yield();
					continue;
				}
				task_to_run->assignedFiber = f;
				f->owningTask = task_to_run;
				f->Init(GlobalFiberPool::FiberEntryWrapper);
			}

			f->status.store(FiberStatus::RUNNING, std::memory_order_release);
			f->homeCtx = &this->schedulerCtx;   // where the fiber returns to: THIS worker
			currentRunningTask = task_to_run;
			currentFiber = f;
			busy.store(true, std::memory_order_relaxed);
			{
				ContextSwitch(&this->schedulerCtx, &f->ctx);
	
			}
			busy.store(false, std::memory_order_relaxed);

			FiberStatus fs = f->status.load(std::memory_order_acquire);
			if (fs == FiberStatus::DEAD) {
				// Completed for good
				if (task_to_run->waitGroup) {
					int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						task_to_run->waitGroup->WakeAll();   // only touches wg if someone registered
				}
				bool was_forked = task_to_run->isForked;  // Save before destruction
				task_to_run->assignedFiber = nullptr;
				ReleaseFiber(f);

				scheduler->CleanupTaskMetadata(task_to_run);
				task_to_run->~Task();
				scheduler->GetAllocator()->Free(task_to_run);
				scheduler->pendingTasks.fetch_sub(1, std::memory_order_acq_rel);

				// Clear busy flag if this was a forked task
				if (was_forked) {
					if (qIndex < (int)scheduler->immediateCoresInUse.size()) {
						scheduler->immediateCoresInUse[qIndex]->store(false, std::memory_order_release);
					}
				}

				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}
			else if (fs == FiberStatus::WANTS_YIELD) {
				// Yielded. The switch above already saved the fiber's context
				f->status.store(FiberStatus::READY, std::memory_order_release);
				scheduler->loPri[qIndex]->push_bottom(task_to_run);
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}
			else {
				// WANTS_SUSPEND: the context is now safely saved, so publish SUSPENDED.
				// But a Signal/Resume may have raced in during WANTS_SUSPEND and flipped us
				// to SUSPEND_SIGNALED -- in that case DON'T park (the wakeup would be lost);
				// resume immediately instead. The CAS makes the park-vs-signal decision atomic.
				FiberStatus exp = FiberStatus::WANTS_SUSPEND;
				if (f->status.compare_exchange_strong(exp, FiberStatus::SUSPENDED, std::memory_order_acq_rel)) {
					// parked; a later Signal/Resume re-queues it (SUSPENDED -> READY)
				}
				else if (exp == FiberStatus::SUSPEND_SIGNALED) {
					// signal beat us here: wake now instead of parking
					f->status.store(FiberStatus::READY, std::memory_order_release);
					scheduler->Requeue(task_to_run);
				}
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}

			if (is_handling_fork) {
				if (qIndex < (int)scheduler->immediateCoresInUse.size()) {
					scheduler->immediateCoresInUse[qIndex]->store(false, std::memory_order_release);
				}
				is_handling_fork = false;
			}

			if (EpochManager::Instance().RetiredCount() > 512) {
				EpochManager::Instance().Tick();
			}

			task_to_run = nullptr;
		}
		// --- 2. Immediate task execution ---
		{
			if (immediateTask != nullptr) {
				// Finish this worker's own queued inbox work FIRST, rather than dumping it for
				// others to steal -- forking isn't urgent, and relying on a peer happening to
				// steal it loses efficiency and risks unbounded latency if the pool is busy
				// elsewhere. fastJob tasks (the common case, defaulted true) are GUARANTEED to
				// never suspend, so they're executed to completion right here, no fiber needed
				// -- genuinely finished, not just relocated. A non-fastJob task COULD
				// legitimately suspend on an external event; forcing it to finish inline isn't
				// possible without duplicating the whole fiber/status state machine, so THOSE
				// still fall back to the deque for stealing (the old behavior) as the safety
				// net for that rarer case. Only the INBOX needs this treatment -- anything
				// already sitting in this worker's own deque was already directly stealable via
				// steal(), it was never actually at risk.
				auto drainInbox = [&](TaskMPSCQueue* inbox, TaskDeque* deque) {
					size_t count = 0;
					while (count < BATCH_SIZE && inbox->pop(batch[count])) count++;
					for (size_t i = 0; i < count; ++i) {
						Task* t = batch[i];
						if (!t) continue;
						scheduler->Requeue(t);
					}
				};
				drainInbox(scheduler->hiPriInboxes[qIndex].get(), scheduler->hiPri[qIndex].get());
				drainInbox(scheduler->loPriInboxes[qIndex].get(), scheduler->loPri[qIndex].get());

				if (EpochManager::Instance().RetiredCount() > 512) {
					EpochManager::Instance().Tick();
				}

				task_to_run = immediateTask;
				current_task = immediateTask;
				immediateTask = nullptr;
				immediate.store(false, std::memory_order_release);
				is_handling_fork = true;
				continue;
			}
		}
		{
			// --- 3. Local  queues ---
			if (!task_to_run) {
				auto opt = scheduler->hiPri[qIndex]->pop_bottom();
				if (opt) {
					Task* task = *opt;
					if (!task) {
						std::cerr << "[worker " << qIndex << "] Null task from pop_bottom!" << std::endl;
					}
					else {
						task_to_run = task;
						current_task = task;
						continue;
					}
				}
			}
			if (!task_to_run) {
				auto opt = scheduler->loPri[qIndex]->pop_bottom();
				if (opt) {
					Task* task = *opt;
					if (!task) {
						std::cerr << "[worker " << qIndex << "] Null task from pop_bottom!" << std::endl;
					}
					else {
						task_to_run = task;
						current_task = task;
						continue;
					}
				}
			}
		}
		{
			// --- 4. Work stealing ---
			if (!task_to_run) {
				// Non-blocking backoff: a per-worker (thread_local, no shared/contended state --
				// can't itself become a new source of contention) count of consecutive whole-
				// steal-block misses. Above the threshold, shrink how many clusterMates get
				// probed each iteration (down to 1) instead of the full set -- reduces redundant
				// CAS/cache-line traffic pool-wide during a steal storm (many idle workers all
				// hammering the same handful of targets) without ever sleeping, so this worker
				// stays fully responsive to a fresh immediate/forked task or inbox item. Resets
				// to 0 the instant ANY steal succeeds -- "the storm cleared" observed via a real
				// outcome, since TaskDeque::steal() is lock-free (CAS-based, no actual lock to
				// wait on).
				static thread_local int consecutiveMisses = 0;
				constexpr int kBackoffMissThreshold = 8;

				// Locality-first, random fallback -- built from REAL queried topology
				// (TaskScheduler::BuildTopology), not an assumption about the affinity scheme:
				//  1. Try same-last-level-cache mates (clusterMates), random order, EXCLUDING
				//     the direct SMT sibling (handled separately below).
				//  2. If the SMT sibling is currently IDLE, try it -- a BUSY sibling shares
				//     this core's execution ports, so stealing its work wouldn't recruit any
				//     new throughput, just pile more work onto an already-contended core.
				//  3. Fall back to the old global-random steal across everyone.
				// Steal up to 4 in one CAS instead of one steal() per task -- amortizes the
				// atomic RMW cost across the batch. Only out[0] becomes task_to_run; anything
				// else stolen alongside it lands on THIS worker's own deque (matching priority
				// tier) via push_bottom_batch, so it's immediately available locally without
				// needing more remote CAS traffic -- or, in the near-impossible case that this
				// worker's own deque is somehow full, Requeue() so it's never simply lost.
				constexpr size_t kStealBatchCap = 4;
				auto tryStealBatchFrom = [&](int target) -> bool {
					Task* stolen[kStealBatchCap];
					TaskDeque* ownDeque = scheduler->hiPri[qIndex].get();
					size_t n = scheduler->hiPri[target]->steal_batch(stolen, kStealBatchCap);
					if (n == 0) {
						n = scheduler->loPri[target]->steal_batch(stolen, kStealBatchCap);
						ownDeque = scheduler->loPri[qIndex].get();
					}
					if (n == 0) return false;

					task_to_run = stolen[0];
					if (n > 1 && !ownDeque->push_bottom_batch(&stolen[1], n - 1)) {
						for (size_t i = 1; i < n; ++i) {
							if (!ownDeque->push_bottom(stolen[i])) scheduler->Requeue(stolen[i]);
						}
					}
					return true;
				};

				const auto& mates = scheduler->clusterMates[qIndex];
				if (!mates.empty()) {
					size_t probeLimit = (consecutiveMisses < kBackoffMissThreshold)
						? mates.size() : (size_t)1;
					size_t mstart = FastRand() % mates.size();
					for (size_t i = 0; i < probeLimit; ++i) {
						int target = mates[(mstart + i) % mates.size()];
						if (tryStealBatchFrom(target)) break;
					}
				}

				if (!task_to_run) {
					int sibling = scheduler->siblingQIndex[qIndex];
					if (sibling >= 0 && !scheduler->workers[sibling]->busy.load(std::memory_order_relaxed)) {
						tryStealBatchFrom(sibling);
					}
				}

				if (!task_to_run) {
					int res = FastRand() % (scheduler->workers.size() - 1);
					if (res == qIndex) res++;
					tryStealBatchFrom(res);
				}

				if (task_to_run) {
					consecutiveMisses = 0;
					current_task = task_to_run;
					continue;
				}
				else {
					++consecutiveMisses;
				}
			}
		}
		

		// 5. --- Pull from inboxes before sleep (drain them so nothing gets stuck) ---
		if (!task_to_run) {
			size_t count = 0;
			while (count < BATCH_SIZE && scheduler->hiPriInboxes[qIndex]->pop(batch[count])) {
				count++;
			}
			if (count > 0) {
				if (scheduler->hiPri[qIndex]->push_bottom_batch(batch, count)) {
					auto opt = scheduler->hiPri[qIndex]->pop_bottom();
					if (opt) {
						task_to_run = *opt;
						continue;
					}
				}
			}

			if (!task_to_run) {
				count = 0;
				while (count < BATCH_SIZE && scheduler->loPriInboxes[qIndex]->pop(batch[count])) {
					count++;
				}
				if (count > 0) {
					if (scheduler->loPri[qIndex]->push_bottom_batch(batch, count)) {
						auto opt = scheduler->loPri[qIndex]->pop_bottom();
						if (opt) {
							task_to_run = *opt;
							continue;
						}
					}
				}
			}
		}

		if (task_to_run) {
			continue;
		}
		else {
			// Per-worker signal, not a pool-wide counter: hasQueuedWork means "a task was pushed
			// specifically to ME since my last search," which is the only thing that should
			// keep THIS worker from actually sleeping. Stealable work on OTHER workers' deques
			// doesn't belong here at all -- it's found by the unconditional steal-attempt phase
			// every AWAKE worker already runs each loop pass (steps 3-4 above), with no
			// predicate gating it. A worker that's genuinely out of local work and found nothing
			// to steal has no reason to keep spinning just because some unrelated queue has a
			// backlog it structurally can't help with anyway (e.g. another worker's own inbox,
			// which nobody but that worker can ever drain).
			std::unique_lock<std::mutex> lock(workerMutex);
			cv.wait(lock, [this]() {
				return !running.load(std::memory_order_acquire)
					|| immediate.load(std::memory_order_acquire)
					|| (!scheduler->paused.load(std::memory_order_acquire) && hasQueuedWork.load(std::memory_order_acquire));
				});

			if (!running.load(std::memory_order_acquire)) break;
		}
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}
