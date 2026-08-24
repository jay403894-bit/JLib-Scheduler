// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include <chrono>
#include <iostream>
#include <cstring>   // std::memset -- MSVC pulls this in transitively, libstdc++ does not
#include <utility>   // std::swap, for drainInbox's in-place corePref partition
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
void Thread::StartWorker(size_t cpu_affinity, size_t fiberCacheCapacity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready, fiberCacheCapacity]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		instance = this;
		thread_id = thread_counter.fetch_add(1);
		// ThreadLocalCache::Initialize clamps this to its MaxCapacity and floors it at 2.
		localCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity);
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
		immediate.store(true, std::memory_order_seq_cst);
	}
	cv.notify_one();
	return true;
}
void Thread::SetQueueIndex(size_t index)
{
	qIndex = index;
};

bool Thread::DrainOwnInboxesToDeques() {
	// Pushes to our OWN deque, and NOT via Requeue the way Worker()'s pre-immediate drain does. That
	// difference is about THIS caller, not about Requeue -- the other drain is correct:
	// PushToCore stores immediateCoresInUse BEFORE SetImmediateTask, so by the time that worker sees
	// an immediate task its own core is already flagged, and PickNextWorker skips flagged workers.
	// Requeue there provably cannot hand a task back into the inbox being emptied.
	//
	// A worker spinning in WaitFor carries NO such flag -- it is running an ordinary task -- so
	// PickNextWorker can and will select it, and Requeue would put the task straight back where we
	// found it. Taking the flag to borrow that guarantee is worse than it looks: it marks the core
	// pinned for ALL placement, makes targeted Push(cpu, task) start returning false against us, and
	// has to be cleared on every exit from the spin. push_bottom needs none of that and is monotone
	// progress -- once on the deque the task is stealable by every other worker AND visible to our
	// own GetTask, which scans every deque including this one.
	const size_t BATCH = 64;
	Task* batch[BATCH];
	bool moved = false;

	auto drain = [&](TaskMPSCQueue* inbox, TaskDeque* deque) {
		for (;;) {
			size_t count = 0;
			while (count < BATCH && inbox->pop(batch[count])) count++;
			if (count == 0) break;
			for (size_t i = 0; i < count; ++i) {
				if (!batch[i]) continue;
				deque->push_bottom(batch[i]);
				moved = true;
			}
		}
	};

	drain(scheduler->hiPriInboxes[qIndex].get(), scheduler->hiPri[qIndex].get());
	drain(scheduler->loPriInboxes[qIndex].get(), scheduler->loPri[qIndex].get());
	return moved;
}
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
	// The load is seq_cst and pairs with the seq_cst STORES of every flag the sleep decision reads:
	// hasQueuedWork (MarkQueuedWork), immediate (SetImmediateTask and the fork path) and paused
	// (Pause/Resume). All four operations of each pair have to be in one total order or the skip is
	// unsound -- see tests/verify/sleepwake_model.c.
	//
	// THIS SHIPPED BROKEN ONCE, in 1.2.0, and the reason is worth keeping. The first model had ONE
	// flag and proved the handshake for it. The predicate has three. `immediate` and `paused` were
	// left as release stores read with acquire, so for those the total-order argument did not exist:
	// the setter stores its flag, loads workerState, sees AWAKE, skips -- while the worker stores
	// GOING_TO_SLEEP, loads the flag, sees the stale value, and parks forever. It hung macOS arm64
	// in CI about one run in three and never once reproduced on x86, because TSO hides it.
	//
	// The model now carries `-DIMMEDIATE_ONLY -DWEAK_IMMEDIATE` as a permanent negative control that
	// reproduces exactly that. If a FOURTH input is ever added to the sleep predicate, it must be
	// seq_cst on both sides and it must go into that model. A proof covers what it modelled.
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
		// ONCE PER PROCESS, not once per failure. Exhaustion is not a single event: the caller
		// re-queues the task and yields, so a pool that is genuinely short spins through this path
		// millions of times. Printing each one turned an eight-minute stall into an eight-minute
		// stall that also floods the console with synchronised writes, which is slower AND hides
		// the one line that explains it. The condition is a capacity problem, so one line naming
		// the cap and the cause is all a reader can act on.
		static std::atomic<bool> warned{ false };
		if (!warned.exchange(true, std::memory_order_relaxed)) {
			// Report the ACTUAL configured budget, not the default. This line used to say
			// "(64 per core by default)" unconditionally, so a reader who had already raised the
			// budget was told a number that was not theirs and could not tell whether their call
			// had taken effect -- the one thing the message exists to help them decide.
			const size_t perWorker = TaskScheduler::StandardFibersPerWorker();
			const size_t workers   = scheduler ? scheduler->GetWorkerCount() : 0;
			std::cerr << "[JLib::Scheduler] fiber pool exhausted. A SUSPENDED task holds its fiber, "
			             "so the number of tasks that may be blocked AT ONCE is capped by the pool: "
			          << perWorker << " standard per worker";
			if (workers) std::cerr << " x " << workers << " workers = " << (perWorker * workers);
			std::cerr << " total. Past that, workers re-queue and retry instead of running.\n"
			             "  Usually that is a STALL that clears as blocked tasks finish. INSIDE A "
			             "TaskDAG IT MAY NEVER CLEAR: if the tasks holding the fibers are waiting on "
			             "work that cannot get a fiber because they are holding them all, nothing "
			             "progresses again. A DAG whose concurrently-suspended nodes outnumber the "
			             "budget above is the shape to look for.\n"
			             "  Either block fewer tasks concurrently, or call "
			             "TaskScheduler::SetFiberBudget(standardPerWorker, heavyPerWorker) BEFORE "
			             "Init() to raise it. This warning prints once.\n";
		}
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
	// IdlePolicy spin state for THIS worker. Both are reset the moment work is found, not only on
	// the fall-through to park -- otherwise a worker that spun most of its budget and then got a
	// task would carry that count into its next idle episode and park early ever after.
	unsigned idleSpins = 0;
	while (running.load(std::memory_order_acquire)) {

		ready.store(true, std::memory_order_release);
		// Cleared once per iteration, unconditionally, BEFORE this iteration's own-queue/steal/
		// inbox-drain search (steps 3-5 below) -- so any push that landed before this clear gets
		// found directly by that same search (turning task_to_run non-null, never reaching the
		// sleep predicate at all this iteration), and any push landing AFTER the clear re-arms
		// this flag (via MarkQueuedWork(), called by whichever push targeted this worker) for
		// the sleep predicate to see fresh, later in this same iteration. See hasQueuedWork's
		// declaration comment in Thread.h for the full reasoning.
		// seq_cst, NOT relaxed, and the whole comment above depends on it. A relaxed store is not
		// ordered against the loads that follow, so it may SINK PAST the drain below: the worker
		// searches and finds nothing, a push then lands and sets this flag seq_cst, and only then
		// does the stale clear land and wipe it. The worker parks with a task in its own inbox and
		// the only signal for it destroyed. That is the lost wakeup, and it is why the reasoning
		// above ("a push landing AFTER the clear re-arms the flag") did not hold in practice: the
		// clear had no defined position relative to the search it is supposed to precede.
		//
		// This is a tightening, not the guarantee. Ordering the clear cannot make the flag alone
		// sufficient, because the flag and the queue are separate objects and no single operation
		// observes both -- a push whose queue write is not yet visible to the drain can still be
		// missed. That is why the park decision consults the inboxes directly; this just removes the
		// reordering that made the window easy to hit.
		hasQueuedWork.store(false, std::memory_order_seq_cst);
		// --- 1. Execute task if found ---
		if (task_to_run) {
			// CANCELLATION, OBSERVED AT PICKUP. One flag in the scope; every task carrying a token
			// to it reads the new value the next time it is touched, and this is one of those
			// points. Nothing was removed from any queue to make this work -- which is exactly why
			// it reaches a task parked anywhere, including an Event's lock-free waiter stack, with
			// no change to Event at all.
			//
			// ONLY AN UNSTARTED TASK MAY BE DISCARDED. A queued entry may be a RESUME: Thread::Resume
			// ends in Requeue(owningTask), so a suspended fiber returns through these same queues,
			// and a coroutine awaiter re-pushes its Task the same way. Discarding one of those
			// abandons a live stack or frame rather than cancelling it -- see Task::started. A
			// started task is let through to run, and observes the cancellation at its own next
			// suspend point or poll, where it can unwind properly.
			if (!task_to_run->started && IsTaskCancelled(task_to_run)) {
				// Same disposal the DAG uses for a node that never runs: release the WaitGroup first
				// so nothing waiting on this work blocks forever on something abandoned.
				if (task_to_run->waitGroup) {
					const int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						task_to_run->waitGroup->WakeAll();
				}
				scheduler->CleanupTaskMetadata(task_to_run);
				DestroyTask(task_to_run);
				scheduler->GetAllocator()->FreeSized(task_to_run);
				task_to_run = nullptr;
				if (EpochManager::Instance().ShouldSelfReclaim()) EpochManager::Instance().Tick();
				ready.store(true, std::memory_order_release);
				continue;
			}
			task_to_run->started = 1;

			// Fast path: run directly on THIS worker's own OS-thread stack, no fiber acquired
			// or ContextSwitch paid at all. Only safe because Native tasks are a CONTRACT --
			// they must never call WaitOnEvent*/anything that suspends (there's no fiber here
			// to switch away to). assignedFiber is deliberately left nullptr for these tasks,
			// which is exactly what WaitOnEvent*'s guards check for -- a mismarked Native
			// task that tries to suspend anyway fails loudly there instead of corrupting the
			// worker's real call stack.
			// Coroutines ride this path too: resuming one is a plain fn(data) call -- fn is a
			// trampoline, data is the handle address -- that runs on this worker's stack and
			// returns, exactly like a Native task. That type erasure is what keeps <coroutine> and
			// C++20 out of the core entirely. They differ only in who completes them (below).
			if (task_to_run->type == TaskType::Native || task_to_run->type == TaskType::Coroutine) {
				// READ BEFORE Execute(), and this is load-bearing rather than tidy. A coroutine's
				// resume can run the body to completion, and completion frees both the frame and
				// this Task -- so `task_to_run` may be DANGLING the instant Execute() returns.
				// Touching ->type afterwards to decide what to do about it is a use-after-free.
				const bool isCoroutine = (task_to_run->type == TaskType::Coroutine);

				currentRunningTask = task_to_run;
				busy.store(true, std::memory_order_relaxed);
				task_to_run->Execute();

				// OWNERSHIP: the worker completes Native tasks and NEVER completes coroutine ones.
				//
				// The tempting design -- a "did it finish" flag the worker checks after resuming --
				// is racy and was written and discarded here. A coroutine that suspends is re-pushed
				// by whatever armed its resume, so a SECOND worker can pick it up, run it to
				// completion and free it while the first worker is still deciding; both then observe
				// "finished" and both free. There is no flag read that closes that, because the
				// window opens the moment the task becomes re-pushable, which is inside resume().
				//
				// Giving the C++20 side sole ownership removes the race instead of racing better:
				// the coroutine signals its own WaitGroup and frees its own Task exactly once, from
				// inside the coroutine, before its frame goes away. See JLib/Coroutine.h.
				if (!isCoroutine) {
					if (task_to_run->waitGroup) {
						int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
						if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
							task_to_run->waitGroup->WakeAll();   // only touches wg if someone registered
					}
					busy.store(false, std::memory_order_relaxed);
					currentRunningTask = nullptr;

					scheduler->CleanupTaskMetadata(task_to_run);
					DestroyTask(task_to_run);
					scheduler->GetAllocator()->FreeSized(task_to_run);
				}
				else {
					// Coroutine: the task may already be freed, so nothing below may touch it.
					// Clearing these two is still required and still safe -- neither reads the task.
					busy.store(false, std::memory_order_relaxed);
					currentRunningTask = nullptr;
					task_to_run = nullptr;
				}

				// Release the core claimed by PushImmediate/PushToCore. Sound because an immediate
				// task goes into THIS worker's immediate slot and cannot be stolen, so the worker
				// that releases the claim is always the one it was made against. Task::isForked was
				// a second, unsound way in -- a queued, stealable task releasing whatever core the
				// worker that happened to run it had claimed -- and went with PushFork in 1.3.4.
				if (is_handling_fork) {
					if (qIndex < (int)scheduler->immediateCoresInUse.size()) {
						scheduler->immediateCoresInUse[qIndex]->store(false, std::memory_order_release);
					}
					is_handling_fork = false;
				}
				if (EpochManager::Instance().ShouldSelfReclaim()) {
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
						immediate.store(true, std::memory_order_seq_cst);
					}
					else {
						// NOT push_bottom on our own deque. That end is LIFO for the owner (see
						// TaskDeque's header), so the next pop_bottom takes THE SAME TASK back and
						// this worker spins pop -> no fiber -> push -> pop forever on one task. It
						// therefore never reaches its inbox drain -- which is exactly where a
						// SignalAll deposits the resumed tasks whose completion would FREE the
						// fibers this one is waiting for. Every worker doing that at once is a
						// deadlock, not the "transient, fibers will free up" this comment used to
						// claim: over-subscribing the pool hung the primitives suite until its
						// watchdog fired.
						//
						// Requeue routes it through PickNextWorker to a worker inbox, so it leaves
						// this worker's hands and this pass continues to the steal and inbox-drain
						// steps that can actually make progress.
						scheduler->Requeue(task_to_run);
					}
					// MUST clear it. task_to_run is a thread_local that survives `continue`, and the
					// loop top runs `if (task_to_run)` before searching -- so leaving it set makes
					// this worker keep re-processing a task it has already handed to a queue, adding
					// one more copy each pass. With the old push_bottom that was hidden, because the
					// worker re-popped the very same task and the duplicate was itself; routing the
					// task anywhere else turns it into the same Task* live in two queues and run by
					// two workers, which is a use-after-free.
					task_to_run = nullptr;
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
				task_to_run->assignedFiber = nullptr;
				ReleaseFiber(f);

				scheduler->CleanupTaskMetadata(task_to_run);
				DestroyTask(task_to_run);
				scheduler->GetAllocator()->FreeSized(task_to_run);

				// No core release here: the is_handling_fork clear below this whole block covers the
				// fiber path for PushImmediate. The isForked clear that used to sit here went with
				// PushFork in 1.3.4.

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

			if (EpochManager::Instance().ShouldSelfReclaim()) {
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
				// elsewhere. Native tasks (the common case, and the default) are GUARANTEED to
				// never suspend, so they're executed to completion right here, no fiber needed
				// -- genuinely finished, not just relocated. A fiber-backed task COULD
				// legitimately suspend on an external event; forcing it to finish inline isn't
				// possible without duplicating the whole fiber/status state machine, so THOSE
				// still fall back to the deque for stealing (the old behavior) as the safety
				// net for that rarer case. Only the INBOX needs this treatment -- anything
				// already sitting in this worker's own deque was already directly stealable via
				// steal(), it was never actually at risk.
				// DRAINS UNTIL EMPTY, not one BATCH_SIZE pass. It used to take at most 64 and stop,
				// which was fine while an inbox rarely held more: PushLocal round-robins one task at
				// a time. PushBatch now deliberately hands a large contiguous RUN to a single inbox
				// (~645 tasks for a 20k batch at the default minPerSegment), so the 64 cap would leave
				// hundreds behind -- in the inbox of a worker that is about to pin, where nothing can
				// steal them. For a short fork that is latency; for a PERSISTENT pinned service, which
				// is what this mechanism exists for (an audio mixer, a network poll loop), the pin
				// never ends and those tasks are stranded for the life of the process. That is the
				// particle-demo deadlock again, needing >64 queued instead of >0.
				//
				// TERMINATION rests on an invariant, since this no longer has a count bound: PushBatch's
				// segment loop routes through PickNextWorker, which SKIPS any core whose
				// immediateCoresInUse is set -- and PushToCore stores that flag BEFORE SetImmediateTask,
				// so by the time this worker observes immediateTask its own core is already marked.
				// PushBatch therefore cannot hand a task back to the inbox being drained. The one
				// exception is every worker being pinned at once, where PushBatch's segment loop already
				// spins on its own while(immediateCoresInUse) loop regardless of this drain -- a
				// pre-existing hazard, not one introduced here. Do NOT "fix" this loop by re-adding a
				// cap: the cap is what stranded the overflow in the first place.
				//
				// ONE PushBatch CALL PER ROUND, not a per-task Requeue loop (that was this function
				// until 2.2.0). Each Requeue call pays its own PickNextWorker + spin-check + single-item
				// push + NotifyWorker (a mutex lock); PushBatch amortizes that cost across a segmented,
				// spread submission -- measured 7.5-8.2x faster at these sizes (SchedulerBench's
				// "requeue vs pushbatch" sweep, interleaved with a same-vs-same control; BATCH_SIZE=64 is
				// the size a single round actually sees). minPerSegment=8 is sized for a round this
				// small -- ParallelFor's default would over-segment it and pay more notifies than the
				// spread is worth (see PushBatch's own header on that regression).
				//
				// PushBatch places a whole call as ONE class (see its own corePref note) -- so a
				// drained inbox that actually mixes P/E/Default tasks (2.1.0+ allows this; class
				// routing is opt-in but no longer hypothetical once one caller uses it) needs
				// splitting into homogeneous runs FIRST, one PushBatch call per run. Done in place on
				// batch[] itself (Dutch-flag 3-way partition), not via separate scratch arrays or
				// heap vectors -- this is a hot path and the common case (everything Default, which
				// is 100% of it today) must still cost exactly one PushBatch call, not three.
				//
				// batch[] can hold null entries (see the pop loops in section 5 below, which guard the
				// same way) -- PushBatch links tasks[i]->next contiguously and cannot tolerate a hole,
				// so nulls are compacted out before the call, unlike the old loop's per-task `if (!t)`.
				auto drainInbox = [&](TaskMPSCQueue* inbox, bool inboxIsHiPri) {
					for (;;) {
						size_t count = 0;
						while (count < BATCH_SIZE && inbox->pop(batch[count])) count++;
						if (count == 0) break;
						size_t live = 0;
						for (size_t i = 0; i < count; ++i)
							if (batch[i]) batch[live++] = batch[i];
						if (live == 0) continue;

						// [0, pEnd) = P, [pEnd, eStart) = Default/Wide/Any (all route identically --
						// see CorePref's own comment), [eStart, live) = E.
						size_t pEnd = 0, mid = 0, eStart = live;
						while (mid < eStart) {
							CorePref pr = batch[mid]->corePref;
							if (pr == CorePref::P) {
								std::swap(batch[pEnd], batch[mid]);
								++pEnd; ++mid;
							}
							else if (pr == CorePref::E) {
								--eStart;
								std::swap(batch[mid], batch[eStart]);
							}
							else {
								++mid;
							}
						}
						if (pEnd > 0)
							scheduler->PushBatch(batch, pEnd, /*cpuaffinity*/0, /*minPerSegment*/8,
								inboxIsHiPri, CorePref::P);
						if (eStart > pEnd)
							scheduler->PushBatch(batch + pEnd, eStart - pEnd, /*cpuaffinity*/0,
								/*minPerSegment*/8, inboxIsHiPri, CorePref::Default);
						if (live > eStart)
							scheduler->PushBatch(batch + eStart, live - eStart, /*cpuaffinity*/0,
								/*minPerSegment*/8, inboxIsHiPri, CorePref::E);
					}
				};
				drainInbox(scheduler->hiPriInboxes[qIndex].get(), /*inboxIsHiPri*/true);
				drainInbox(scheduler->loPriInboxes[qIndex].get(), /*inboxIsHiPri*/false);

				if (EpochManager::Instance().ShouldSelfReclaim()) {
					EpochManager::Instance().Tick();
				}

				task_to_run = immediateTask;
				immediateTask = nullptr;
				immediate.store(false, std::memory_order_seq_cst);
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
				JLIBSCHED_LATENCY_MARK(PreSteal);
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
				// Takes the deque's tag bits rather than the Task*, so no candidate is
				// dereferenced before it is claimed (see TaskDeque::StealBits).
				auto classOK = [&](StealBits sb) {
					if (TaskScheduler::StealClassCompatible(sb.corePref, iAmP, degen)) return true;
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

				// THE NON-WORKER LANE (see TaskScheduler::nonWorkerLane). Probed HERE -- after both
				// topology phases, before the global random fallback -- and the position is a
				// judgement, not an accident. It has no cache locality to any worker, so it does
				// not belong among the LLC mates; but it is the one victim guaranteed to be a
				// SINGLE known index rather than a random draw out of a set, so putting it after
				// the random fallback would make a lazy split's fan-out depend on a dice roll.
				// Unlike the mate lists this is one deque pair, so the probe is unconditional and
				// costs two loads when it is empty, which is the overwhelmingly common case.
				if (!task_to_run) {
					tryStealFrom((int)scheduler->nonWorkerLane);
				}

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
						JLIBSCHED_LATENCY_MARK(Found);
						continue;
					}
				}
				else {
					// THE DEQUE WAS FULL AND THESE ARE ALREADY OUT OF THE INBOX. Dropping them here
					// used to be silent: pop() had consumed them, batch[] is reused next iteration,
					// so the tasks never ran, their slab slots leaked, and every WaitFor on their
					// WaitGroup waited forever with nothing to show in a pool dump. Losing work is a
					// worse failure than stalling on it -- a stall is at least diagnosable.
					// Requeue instead: inboxes are unbounded MPSC queues, so it cannot fail the way
					// a fixed-capacity deque can. Reachable only past 32768 queued on ONE worker's
					// deque, which needs a deliberate single-core bulk push (PushBatch with an
					// explicit cpuaffinity does not spread), but the cost of handling it is one loop.
					for (size_t i = 0; i < count; ++i)
						if (batch[i]) scheduler->Requeue(batch[i]);
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
							JLIBSCHED_LATENCY_MARK(Found);
							continue;
						}
					}
					else {
						// Same as the hiPri branch above: these are already out of the inbox, so
						// dropping them loses them silently. Requeue rather than discard.
						for (size_t i = 0; i < count; ++i)
							if (batch[i]) scheduler->Requeue(batch[i]);
					}
				}
			}
		}

		if (task_to_run) {
			idleSpins = 0;   // work found: this idle episode is over, next one starts fresh
			continue;
		}
		else {
			// IDLE POLICY. Found nothing this pass -- decide whether to search again or park.
			//
			// This sits BEFORE the WS_GOING_TO_SLEEP publish on purpose. A worker that spins here
			// never advertises an intent to park, so it stays WS_AWAKE and pushers take the
			// awake-preference skip instead of paying a notify for it: spinning does not merely
			// avoid the wake, it removes the wake cost from the PRODUCER too.
			//
			// Deliberately NOT a timed wait. The park below stays an unconditional cv.wait, so a
			// lost wakeup still hangs visibly rather than being masked by a timeout -- see the
			// IdlePolicy comment in TaskScheduler.h for why that distinction is the whole point.
			{
				// Shutdown and Pause both override the policy. Pausing means "stop using the CPU",
				// and a policy that spun through it would defeat the only thing Pause exists for.
				const bool mayspin = TaskScheduler::GetIdlePolicy() == TaskScheduler::IdlePolicy::NoSleep
					&& running.load(std::memory_order_acquire)
					&& !scheduler->paused.load(std::memory_order_seq_cst);

				if (mayspin) {
					++idleSpins;
					// Yield periodically even under NoSleep. A pure CpuRelax loop is pathological
					// if the pool is oversubscribed or shares cores with the host's own threads,
					// and "I own the machine" is a claim the caller makes, not one this can verify.
					if ((idleSpins & 0xFF) == 0) std::this_thread::yield();
					else platform::CpuRelax();
					continue;   // search again rather than parking
				}
				idleSpins = 0;   // policy is Sleep, or shutting down/paused: fall through and park
			}

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
			// ALL of these are seq_cst, not just hasQueuedWork. Each one pairs with a seq_cst store
			// on the setter's side, and each pair needs its own place in the single total order --
			// promoting one flag and leaving the others at acquire is precisely the bug that hung
			// macOS arm64 in 1.2.0. `running` is the exception and does not need it, because Join()
			// passes force=true and never takes the skip.
			// The inbox checks are NOT redundant with hasQueuedWork, and the evidence says so. A
			// captured hang shows a worker SLEEPING with a non-empty loPri inbox and the flag at 0:
			// the flag is cleared blind at the top of each loop, so a push whose queue write is not
			// yet visible to this worker's drain, but whose MarkQueuedWork landed before the clear,
			// leaves the item present and the only signal wiped. Inboxes are not stealable, so that
			// task strands and every waiter on it hangs.
			// The flag stays as the cheap common case; the queue is the truth.
			if (!running.load(std::memory_order_acquire)
				|| immediate.load(std::memory_order_seq_cst)
				|| (!scheduler->paused.load(std::memory_order_seq_cst)
					&& (hasQueuedWork.load(std::memory_order_seq_cst)
						|| !scheduler->hiPriInboxes[qIndex]->empty()
						|| !scheduler->loPriInboxes[qIndex]->empty()))) {
				workerState.store(WS_AWAKE, std::memory_order_seq_cst);
				JLIBSCHED_LATENCY_MARK(Wake);
				if (!running.load(std::memory_order_acquire)) break;
				continue;   // work landed while deciding: go search for it instead of parking
			}

			std::unique_lock<std::mutex> lock(workerMutex);
			int expectedGoing = WS_GOING_TO_SLEEP;
			workerState.compare_exchange_strong(expectedGoing, WS_SLEEPING,
				std::memory_order_seq_cst, std::memory_order_relaxed);

			// Same inbox checks as the recheck above, and for the same reason: the flag can be wiped
			// while an item is present, so a predicate that trusts only the flag can decide to sleep
			// on a non-empty inbox. Being evaluated under the mutex does not help -- the mutex
			// orders this against a NOTIFIER, and the failure is a push that never notifies because
			// its flag write was already lost.
			cv.wait(lock, [this]() {
				return !running.load(std::memory_order_acquire)
					|| immediate.load(std::memory_order_acquire)
					|| (!scheduler->paused.load(std::memory_order_acquire)
						&& (hasQueuedWork.load(std::memory_order_acquire)
							|| !scheduler->hiPriInboxes[qIndex]->empty()
							|| !scheduler->loPriInboxes[qIndex]->empty()));
				});

			// Back to AWAKE before releasing the mutex, so the very next push skips the signal.
			workerState.store(WS_AWAKE, std::memory_order_seq_cst);
			JLIBSCHED_LATENCY_MARK(Wake);

			if (!running.load(std::memory_order_acquire)) break;
		}
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}

