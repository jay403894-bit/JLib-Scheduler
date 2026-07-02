#include "../include/T_Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include <chrono>
#include <iostream>
using namespace T_Threads;
thread_local T_Thread* T_Thread::instance = nullptr;

T_Thread::T_Thread(TaskScheduler& scheduler) : scheduler(&scheduler) {
	std::memset(&schedulerCtx, 0, sizeof(Context));
}
T_Thread::~T_Thread() {
}
void T_Thread::StartWorker(size_t cpu_affinity)
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
std::thread::id T_Thread::GetID() {
	return thread.get_id();
}
bool T_Thread::SetImmediateTask(Task* new_task) {
	if (!new_task) return false;
	{
		immediateTask = new_task;
		immediate.store(true, std::memory_order_release);
	}
	cv.notify_one();
	return true;
}
void T_Thread::SetQueueIndex(size_t index)
{
	qIndex = index;
};
void T_Thread::Join() {
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
T_Thread* T_Thread::GetCurrent() { 
	return instance; 
}

void T_Thread::CoYield(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->CoYield();
	}
}
void T_Thread::Suspend(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->Suspend();
	}
}
 void T_Thread::Resume(Fiber* targetFiber) {
	 if (targetFiber) {
		 targetFiber->Resume(); 
	 }
}

 void T_Threads::T_Thread::CoYield()
 {
	 GetCurrent()->currentFiber->CoYield();
 }

 void T_Threads::T_Thread::Suspend()
 {
	 GetCurrent()->currentFiber->Suspend();
 }

 uint64_t T_Thread::GenerateID() {
	 return scheduler->nextId.fetch_add(1, std::memory_order_relaxed);
 }
void T_Thread::NotifyWorker(){
	cv.notify_one();
}

bool T_Thread::Ready(){
	return ready.load(std::memory_order_acquire);
}
       
Fiber* T_Thread::AcquireFiber(Task* task) {
	Fiber* f = localCache.Pop();
	if (f) return f;

	f = localCache.Pop();

	if (!f) {
		std::cerr << "CRITICAL: Global pool exhausted!" << std::endl;
	}
	return f;
}

void T_Thread::ReleaseFiber(Fiber* f) {
	localCache.Push(f);
}

uint32_t T_Thread::FastRand() {
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
Task* T_Threads::T_Thread::AcquireWork(bool& isFork)
{
	return nullptr;
}
void T_Threads::T_Thread::RunTask(Task* task, bool isFork)
{}
void T_Thread::Worker() {
	running.store(true, std::memory_order_release);
	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	static thread_local Task* task_to_run = nullptr;
	static thread_local bool is_handling_fork = false;
	while (running.load(std::memory_order_acquire)) {

		ready.store(true, std::memory_order_release);
		// --- 1. Execute task if found ---
		if (task_to_run) {
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
			{
				ContextSwitch(&this->schedulerCtx, &f->ctx);
			}
			
			FiberStatus fs = f->status.load(std::memory_order_acquire);
			if (fs == FiberStatus::DEAD) {
				// Completed for good 
				task_to_run->assignedFiber = nullptr;
				ReleaseFiber(f);
				task_to_run->~Task();
				scheduler->GetAllocator()->Free(task_to_run);
				scheduler->pendingTasks.fetch_sub(1, std::memory_order_acq_rel);
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
				//dump inboxes to be stolen from the immediate task, so it doesn't get stuck
				size_t count = 0;
				while (count < BATCH_SIZE && scheduler->hiPriInboxes[qIndex]->pop(batch[count])) {
					count++;
				}
				if (count > 0) {
					scheduler->hiPri[qIndex]->push_bottom_batch(batch, count);
				}

				count = 0;
				while (count < BATCH_SIZE && scheduler->loPriInboxes[qIndex]->pop(batch[count])) {
					count++;
				}
				if (count > 0) {
					scheduler->loPri[qIndex]->push_bottom_batch(batch, count);
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
				size_t stride = 1;
				size_t neighbor = (qIndex + stride) % scheduler->workers.size();

				// 1. Try to steal High Priority from the immediate neighbor first
				if (auto stolen = scheduler->hiPri[neighbor]->steal()) {
					task_to_run = *stolen;
				}
				// 2. If no HiPri, try to steal Low Priority from the neighbor
				else if (auto stolen = scheduler->loPri[neighbor]->steal()) {
					task_to_run = *stolen;
				}
				else {
					// 3. Try to steal from a random worker
					int res = FastRand() % (scheduler->workers.size() - 1);
					if (res == qIndex) res++;

					// Again: HiPri first, then LoPri
					if (auto stolen = scheduler->hiPri[res]->steal()) {
						task_to_run = *stolen;
					}
					else if (auto stolen = scheduler->loPri[res]->steal()) {
						task_to_run = *stolen;
					}
				}

				if (task_to_run) {
					current_task = task_to_run;
					continue;
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

		// --- Sleep only if no task is ready ---
		// BOUNDED wait (1ms), NOT a plain cv.wait: pendingTasks is bumped OUTSIDE workerMutex
		// in the push path, so a notify can fire between this predicate check and the actual
		// block -> LOST WAKEUP. With plain cv.wait the worker then sleeps forever holding the
		// last task in its queue, every other worker idle-sleeps too (nobody steals it), and
		// main yields in WaitFor forever -> full hang (observed ~53min in). The 1ms timeout
		// makes every worker re-check pendingTasks and drain its inbox, self-healing the missed
		// wakeup at ~0% CPU. Do NOT remove this timeout -- "inbox was drained" does not hold
		// under the lost-wakeup race. (Regression of the 2026-06-30 fix.)
		if (task_to_run) {
			continue;
		}
		else {
			std::unique_lock<std::mutex> lock(workerMutex);
			cv.wait(lock, [this]() {
				return !running.load(std::memory_order_acquire)
					|| immediate.load(std::memory_order_acquire)
					|| (!scheduler->paused.load(std::memory_order_acquire) && scheduler->pendingTasks.load(std::memory_order_acquire) > 0);
				});

			if (!running.load(std::memory_order_acquire)) break;
		}
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}
