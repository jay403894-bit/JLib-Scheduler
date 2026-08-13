// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include <chrono>
#include <iostream>
#include <cstring>   // std::memset -- MSVC pulls this in transitively, libstdc++ does not
using namespace JLib;
thread_local Thread* Thread::instance = nullptr;

#if !JLIB_PLATFORM_WINDOWS
#include <sched.h>
#include <pthread.h>

// Bind another thread to a CPU set.
//
// glibc's pthread_setaffinity_np did not reach bionic until API 36, and Termux/NDK builds target
// far lower than that, so on Android this goes through the syscall wrapper directly. That takes a
// TID rather than a pthread_t -- and the caller here is the PARENT thread, which cannot pass 0
// ("me") because that would pin itself instead of the worker. pthread_gettid_np is the bionic
// extension that closes the gap; it has been there since API 21.
//
// Failure is deliberately swallowed on both paths, matching the Windows behaviour: a container or
// cgroup may legitimately forbid the CPU, and an unpinned worker is a performance question, not a
// correctness one. On Android that is not an edge case -- the platform's cgroups own thread
// placement, so these calls routinely fail (or succeed and are then overridden) for an
// unprivileged app. Treat Android placement as UNENFORCEABLE and never quote a number from it.
// Takes a plain 64-bit mask rather than a cpu_set_t so the CALL SITES stay portable: macOS has no
// cpu_set_t at all, and mentioning it in the policy switch would need the whole block #ifdef'd.
static inline void BindThreadToMask(pthread_t handle, const topology::CpuMask& mask)
{
#if JLIB_PLATFORM_DARWIN
	// macOS has NO thread-affinity API on arm64. THREAD_AFFINITY_POLICY still compiles but has been
	// a no-op since Apple Silicon -- the kernel owns placement and expresses intent through QoS
	// classes instead. So this is an honest no-op, not a stub awaiting an implementation: the
	// policy is unenforceable, which is why the docs say placement is Windows/Linux only.
	(void)handle; (void)mask;
#else
	// cpu_set_t is 1024 bits by default, so Linux never had the 64-CPU problem Windows has -- the
	// old ceiling here was purely the uint64_t this function used to take.
	cpu_set_t set;
	CPU_ZERO(&set);
	for (unsigned c = 0; c < topology::CpuMask::kMaxCpus; ++c)
		if (mask.Test(c) && c < CPU_SETSIZE) CPU_SET(c, &set);
  #if defined(__BIONIC__)
	(void)sched_setaffinity(pthread_gettid_np(handle), sizeof(cpu_set_t), &set);
  #else
	(void)pthread_setaffinity_np(handle, sizeof(cpu_set_t), &set);
  #endif
#endif
}
#endif

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

	// ---- THE REMAINING CEILING: CpuMask::kMaxCpus, not 64 ----
	// The 64-CPU limit this guard originally enforced is gone. Windows binding now goes through
	// SetThreadGroupAffinity / SetThreadIdealProcessorEx, which take the processor group as DATA
	// rather than inheriting it from the calling thread, and every mask in the scheduler is a
	// CpuMask rather than a uint64_t. What is left is the CpuMask width itself.
	//
	// This is still worth guarding rather than trusting, because the failure it prevents is silent.
	// A CpuId past the mask width would index off the end of efficiencyClass and set no bit at all,
	// so the worker would appear bound while being bound to nothing, which reads as unexplained
	// slowness. Refusing and saying so costs one branch per worker at startup.
	//
	// Degrading to None is not a capacity loss: the worker is still created, still stealing, still
	// running tasks. Only its PLACEMENT is dropped, which is exactly AffinityPolicy::None, and its
	// locality becomes approximate. Raising the cap is CpuMask::kWords in Topology.h.
	auto affinityPolicy = scheduler->GetAffinityPolicy();
	if (cpu_affinity >= topology::CpuMask::kMaxCpus &&
	    affinityPolicy != TaskScheduler::AffinityPolicy::None) {
		affinityPolicy = TaskScheduler::AffinityPolicy::None;
		static std::atomic<bool> warned{ false };
		if (!warned.exchange(true, std::memory_order_relaxed)) {
			std::cerr << "[JLib::Scheduler] logical CPU " << cpu_affinity
			          << " is past CpuMask::kMaxCpus (" << topology::CpuMask::kMaxCpus
			          << ") -- that worker runs unbound. Raise CpuMask::kWords in Topology.h.\n";
		}
	}

#if JLIB_PLATFORM_WINDOWS
	// HOW this worker is bound to its core, per TaskScheduler::SetAffinityPolicy.
	//
	// Hard  -- SetThreadAffinityMask: the thread runs on that logical CPU or nowhere. Best cache
	//          locality and the only mode where "where does oversubscription land" is a decision
	//          rather than a discovery. Worst case: another process pins something to the same core
	//          and this worker is stuck behind it with no escape.
	// Ideal -- SetThreadIdealProcessor: a strong HINT. Windows keeps the thread there when it can,
	//          so cache locality and the topology map still hold, but it may migrate under contention
	//          or thermal pressure. Notably it also leaves core parking and Thread Director able to
	//          do their jobs.
	// None  -- leave placement entirely to Windows. Only sensible if this library is embedded in an
	//          application that owns thread placement itself, which is a real consideration for a
	//          LIBRARY that would otherwise hard-pin N threads inside someone else's process.
	//
	// NOTE the topology-aware steal ordering (SMT sibling first, then cache cluster) is only
	// meaningful under Hard or Ideal: if threads migrate freely, "my sibling" no longer describes
	// where any data actually is. Pinning and locality-aware stealing are ONE decision, not two.
	// Both calls are the GROUP-AWARE variants, and that is the whole reason this scheduler can
	// address a machine wider than 64 logical CPUs. SetThreadAffinityMask takes a bare 64-bit mask
	// interpreted in the CALLING thread's group, so it cannot name a CPU in another group at all --
	// the group is not a parameter, it is ambient. SetThreadGroupAffinity takes the group as data.
	// Same story for SetThreadIdealProcessor versus its Ex form, which swaps a bare DWORD for a
	// PROCESSOR_NUMBER carrying {Group, Number}.
	//
	// On a machine with one group these are exactly equivalent to what they replaced, so nothing
	// changes for the overwhelming majority of hardware. The flat CpuId splits into its two halves
	// by construction (see Topology.h): group is the high bits, processor number is the low six.
	const WORD  cpuGroup  = (WORD)topology::CpuMask::GroupOf((topology::CpuId)cpu_affinity);
	const BYTE  cpuNumber = (BYTE)topology::CpuMask::BitOf((topology::CpuId)cpu_affinity);

	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:   // one worker per physical core -- still a hard bind
	case TaskScheduler::AffinityPolicy::Hard: {
		GROUP_AFFINITY ga{};
		ga.Mask  = (KAFFINITY)(1ULL << cpuNumber);
		ga.Group = cpuGroup;
		SetThreadGroupAffinity(nativeHandle, &ga, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		PROCESSOR_NUMBER pn{};
		pn.Group  = cpuGroup;
		pn.Number = cpuNumber;
		SetThreadIdealProcessorEx(nativeHandle, &pn, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::None:
		break;
	}
#else
	// Linux. Same four policies, but only ONE of them has anything to do.
	//
	// Hard/PhysicalOnly -> pthread_setaffinity_np with a single-CPU set: the direct analogue of
	//                      SetThreadAffinityMask, with the same tradeoff (best locality, no escape
	//                      if something else lands on that core).
	//
	// Ideal -> bind to this worker's WHOLE LLC DOMAIN, not to one core. Linux has no equivalent of
	//          SetThreadIdealProcessor: affinity here is all-or-nothing per CPU, with no "hint".
	//          But sched_setaffinity takes a MASK, so the same INTENT -- keep locality true with
	//          minimum rigidity -- is expressed at a coarser granularity than Windows can. The
	//          kernel may place the thread on any core in the domain, so a wake lands on one that
	//          is already awake rather than waiting for a specific parked core, which is where
	//          hard pinning's ~45% wake-latency cost came from.
	//
	//          The mask ends up EXACTLY AS TIGHT AS THE HARDWARE WARRANTS, which is the point:
	//            - Multi-L3 (Ryzen CCDs, Threadripper, EPYC, multi-socket): it genuinely binds, and
	//              that is where it matters. An unbound worker migrating across cache domains pays
	//              inter-die latency on every steal AND makes clusterMates describe a machine state
	//              that does not exist.
	//            - Single-L3 (most Intel desktop): the domain is every CPU, so the mask binds
	//              nothing. Correct, not a gap -- there is no cache domain to protect, and binding
	//              anyway would only add the rigidity that measured ~45% worse on wake latency.
	//
	//          What this does NOT protect is siblingQIndex: the kernel can still migrate between
	//          physical cores inside the LLC, so "my SMT sibling" stays approximate on Linux.
	//          Deliberate. Tightening to a 2-CPU SMT-pair mask would buy a steal-ORDERING
	//          preference at close to hard pinning's cost -- the wrong side of that trade.
	//
	//          So the POLICY means the same thing on both platforms and only the MECHANISM differs.
	//          UNMEASURED on real Linux hardware: WSL virtualises topology and has no real core
	//          parking, so the placement effect cannot be benchmarked there. Falls back to no
	//          binding if topology was unavailable (mask 0), which is the old behaviour.
	//
	// None -> nothing, same as Windows.
	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:
	case TaskScheduler::AffinityPolicy::Hard: {
		topology::CpuMask one;
		one.Set((topology::CpuId)cpu_affinity);
		BindThreadToMask(nativeHandle, one);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		const size_t qi = (size_t)qIndex;   // set by SetQueueIndex before StartWorker
		if (qi < scheduler->llcMaskOfWorker.size()) {
			const topology::CpuMask& llc = scheduler->llcMaskOfWorker[qi];
			if (llc.Any()) BindThreadToMask(nativeHandle, llc);
		}
		break;
	}
	case TaskScheduler::AffinityPolicy::None:
		break;
	}
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
	NotifyWorker(/*force*/ true);   // shutdown flips `running`, not hasQueuedWork -- see NotifyWorker

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
void Thread::NotifyWorker(bool force){
	// THE OPTIMISATION: if this worker is running, say nothing. It clears hasQueuedWork and
	// re-searches every loop pass, so it will find the task on its own, and MarkQueuedWork has
	// already been stored by every caller that reaches here. Skipping saves a mutex acquire/release
	// plus a condition-variable signal, and when the worker is actually parked the signal is a
	// kernel thread wake costing microseconds.
	//
	// That cost is not theoretical. While the pool is saturated nobody is waiting on the condvar and
	// the signal is nearly free, but once the pool drains faster than one thread can submit, every
	// push buys a real wake -- which is why single-producer submission used to collapse from 3.4 M/s
	// at 8 workers to 0.8 at 14+, a threshold rather than a gradient.
	//
	// The load is seq_cst and pairs with MarkQueuedWork's seq_cst store; see that function and
	// tests/verify/sleepwake_model.c for why nothing weaker is sound. GOING_TO_SLEEP counts as
	// "needs a signal": the worker has published intent but may not be inside cv.wait yet.
	if (!force && workerState.load(std::memory_order_seq_cst) == WS_AWAKE) return;

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
			// or ContextSwitch paid at all. Only safe because noFiber tasks are a CONTRACT --
			// they must never call WaitOnEvent*/anything that suspends (there's no fiber here
			// to switch away to). assignedFiber is deliberately left nullptr for these tasks,
			// which is exactly what WaitOnEvent*'s guards check for -- a mismarked noFiber
			// task that tries to suspend anyway fails loudly there instead of corrupting the
			// worker's real call stack.
			if (task_to_run->noFiber) {
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
				// elsewhere. noFiber tasks (the common case, defaulted true) are GUARANTEED to
				// never suspend, so they're executed to completion right here, no fiber needed
				// -- genuinely finished, not just relocated. A fiber-backed task COULD
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
				// Steal ONE task from `target`: try its hiPri deque first, then loPri. Single-item
				// steal is the ONLY correct steal in this lock-free deque -- a batched range steal
				// double-claims tasks the owner concurrently pops (use-after-free; see TaskDeque.h).
				// So there is no batch and no leftovers to re-home: whatever we steal becomes
				// task_to_run and runs on THIS worker immediately. (No age-promotion here either --
				// promoting an aged loPri task only helps if it gets REQUEUED into the hiPri lane,
				// but a task stolen for immediate execution is already un-starved by the steal, so
				// flipping its priority would be a no-op.)
				// CLASS-AWARE via steal_if: vet corePref BEFORE claiming (see StealClassCompatible) --
				// an explicit P/E task is left in place for a matching-class thief instead of being
				// stolen-then-Requeued (requeue on mismatch = deque contention + thrash, by design
				// rejected). Default/Any/Wide tasks (the common case) remain stealable by everyone.
				const bool iAmP  = scheduler->isPCore[qIndex] != 0;
				const bool degen = scheduler->pWorkers.empty() || scheduler->eWorkers.empty();
				bool sawDecline  = false;   // saw a task this sweep that exists but isn't ours (class mismatch)
				auto classOK = [&](Task* t) {
					if (TaskScheduler::StealClassCompatible(t, iAmP, degen)) return true;
					sawDecline = true;
					return false;
				};
				auto tryStealFrom = [&](int target) -> bool {
					JLIBSCHED_STEAL_STAT(qIndex, probes);
					auto s = scheduler->hiPri[target]->steal_if(classOK);
					if (!s) s = scheduler->loPri[target]->steal_if(classOK);
					if (!s) return false;
					JLIBSCHED_STEAL_STAT(qIndex, hits);
					task_to_run = *s;
					return true;
				};

				// CLASS-FIRST victim ordering: probe the victims this worker can actually steal
				// class-pinned work from BEFORE the ones it might have to decline -- scan order
				// matches steal legality. Phases: same-class LLC mates -> idle SMT sibling (same
				// physical core = same class by construction) -> foreign-class mates -> global
				// random, same-class set first. Same total coverage as the old order (the two mate
				// lists ARE clusterMates, split); on non-hybrid CPUs matesOtherClass is empty and
				// this degrades to exactly the classic scan.
				auto probeList = [&](const std::vector<int>& list) {
					if (list.empty() || task_to_run) return;
					size_t probeLimit = (consecutiveMisses < kBackoffMissThreshold)
						? list.size() : (size_t)1;
					size_t s = FastRand() % list.size();
					for (size_t i = 0; i < probeLimit; ++i) {
						if (tryStealFrom(list[(s + i) % list.size()])) break;
					}
				};

				probeList(scheduler->matesSameClass[qIndex]);

				if (!task_to_run) {
					int sibling = scheduler->siblingQIndex[qIndex];
					if (sibling >= 0 && !scheduler->workers[sibling]->busy.load(std::memory_order_relaxed)) {
						tryStealFrom(sibling);
					}
				}

				probeList(scheduler->matesOtherClass[qIndex]);

				if (!task_to_run) {
					// Global fallback: one random probe from the same-class set, then one from the
					// other class -- covers workers outside this LLC cluster (multi-CCD parts).
					auto tryRandomFrom = [&](const std::vector<int>& set) {
						if (set.empty() || task_to_run) return;
						int t = set[FastRand() % set.size()];
						if (t != qIndex) tryStealFrom(t);
					};
					tryRandomFrom(iAmP ? scheduler->pWorkers : scheduler->eWorkers);
					tryRandomFrom(iAmP ? scheduler->eWorkers : scheduler->pWorkers);
				}

				if (task_to_run) {
					consecutiveMisses = 0;
					continue;
				}
				else {
					// A class-DECLINED sweep is not emptiness: work exists, it just isn't ours. The
					// backoff counter exists to damp CAS storms from probing EMPTY deques -- and a
					// steal_if decline performs NO CAS -- so declines must not shrink future probe
					// width (that would blind this worker to compatible stragglers next sweep).
					// Neither is it success, so no reset. Parking is unaffected either way: sleep
					// was always per-sweep (cv.wait below), and class-targeted push notifies cover
					// new compatible work while we're parked.
					if (!sawDecline)
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
			// Publish the INTENT to park before the final check. From here until this worker is
			// awake again, a pusher observing the state will signal rather than assume this worker
			// will notice on its own. Model checked: tests/verify/sleepwake_model.c.
			int expected = WS_AWAKE;
			workerState.compare_exchange_strong(expected, WS_GOING_TO_SLEEP,
				std::memory_order_seq_cst, std::memory_order_relaxed);

			// RECHECK after advertising, and mirror the wait predicate exactly so the two cannot
			// disagree about what counts as work. The hasQueuedWork load is seq_cst deliberately:
			// it and MarkQueuedWork's store are the StoreLoad pair the whole protocol turns on, and
			// acquire here is precisely the bug the model's negative control reproduces.
			if (!running.load(std::memory_order_acquire)
				|| immediate.load(std::memory_order_acquire)
				|| (!scheduler->paused.load(std::memory_order_acquire)
					&& hasQueuedWork.load(std::memory_order_seq_cst))) {
				workerState.store(WS_AWAKE, std::memory_order_seq_cst);
				if (!running.load(std::memory_order_acquire)) break;
				continue;   // work landed while deciding: go search for it instead of parking
			}

			std::unique_lock<std::mutex> lock(workerMutex);
			int expectedGoing = WS_GOING_TO_SLEEP;
			workerState.compare_exchange_strong(expectedGoing, WS_SLEEPING,
				std::memory_order_seq_cst, std::memory_order_relaxed);

			cv.wait(lock, [this]() {
				return !running.load(std::memory_order_acquire)
					|| immediate.load(std::memory_order_acquire)
					|| (!scheduler->paused.load(std::memory_order_acquire) && hasQueuedWork.load(std::memory_order_acquire));
				});

			// Back to AWAKE before releasing the mutex, so the very next push skips the signal.
			workerState.store(WS_AWAKE, std::memory_order_seq_cst);

			if (!running.load(std::memory_order_acquire)) break;
		}
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}

