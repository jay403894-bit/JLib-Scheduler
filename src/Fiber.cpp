// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Fiber.h"
#include "../include/Thread.h"
#include "../include/TaskScheduler.h"
using namespace JLib;
std::atomic<uint64_t> JLib::Fiber::idGenerator{ 0 };

// Fiber::Init lives in src/win32/FiberInit.cpp and src/posix/FiberInit.cpp -- it writes the exact
// frame its platform's ContextSwitch restore reads back, so it belongs next to that assembly
// rather than here. Everything below is platform-independent scheduler logic.

void Fiber::CoYield() {
	JLIB_EPOCH_CHECK_NO_GUARD("Fiber::CoYield");
	// Record intent and switch out.
	this->status.store(FiberStatus::WANTS_YIELD, std::memory_order_release);
	Thread::TsanSwitchToScheduler();
	ContextSwitch(&this->ctx, this->homeCtx);
}

void Fiber::Suspend() {
	JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Suspend");
	// Record intent and switch out.
	this->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	Thread::TsanSwitchToScheduler();
	ContextSwitch(&this->ctx, this->homeCtx);
}
// The CAS half only -- see the header. Returns true when THIS call performed the SUSPENDED -> READY
// transition, so the caller owns re-queueing owningTask.
bool Fiber::ResumeQueueless() {
	// Robust wake that closes the lost-wakeup window. Two parkable states:
	//  - SUSPENDED: the worker already saved the context and parked us -> CAS to READY
	//    and re-queue (Requeue: nothing to re-count, since we were never completed).
	//  - WANTS_SUSPEND: we asked to suspend but the worker hasn't published SUSPENDED
	//    yet (context maybe not saved) -> we must NOT resume now. Flip to SUSPEND_SIGNALED
	//    so the worker's park step wakes us once the context is safely saved.
	// Idempotent: a second Resume (or a state we don't recognize) is a no-op.
	while (true) {
		FiberStatus s = status.load(std::memory_order_acquire);
		if (s == FiberStatus::SUSPENDED) {
			FiberStatus exp = FiberStatus::SUSPENDED;
			return status.compare_exchange_strong(exp, FiberStatus::READY, std::memory_order_acq_rel);
		}
		else if (s == FiberStatus::WANTS_SUSPEND) {
			FiberStatus exp = FiberStatus::WANTS_SUSPEND;
			if (status.compare_exchange_strong(exp, FiberStatus::SUSPEND_SIGNALED, std::memory_order_acq_rel))
				return false;           // worker will wake it when it parks
			// CAS lost to the worker parking us (now SUSPENDED) -> loop and take that path
		}
		else {
			// RUNNING / READY / SUSPEND_SIGNALED / DEAD: not resumable right now (already
			// signaled, not waiting, or running). Nothing to do.
			return false;
		}
	}
}
// Unchanged behaviour for every caller that wakes ONE fiber: CAS, then re-queue if we won it.
void Fiber::Resume() {
	if (ResumeQueueless())
		TaskScheduler::Instance().Requeue(this->owningTask);
}

void JLib::RequeueResumedBatch(Task** tasks, size_t n, bool hiPri) {
	if (n == 0) return;
	// PINNED: THE BATCH CANNOT BE ONE BATCH. Every task here holds a fiber, and each of those fibers
	// is pinned to whichever worker bound it, so a wake of N fibers is a wake of up to N DIFFERENT
	// worker queues. PushBatch exists to hand a contiguous run to one worker and cannot express
	// that, so this falls back to routing each through Requeue.
	//
	// The batching win is smaller than it looks here anyway. It was measured on PUSH (7.5-8.2x) and
	// it comes from amortising one placement, one queue push and one notify across many tasks --
	// all three of which are per-destination costs, and pinning has already fixed the destinations.
	// What is genuinely given up is the single notify for a SignalAll that wakes many fibers on one
	// worker; if that shows up in a profile, the fix is to group by homeWorker here, not to migrate.
	//
	// `hiPri` is now unused. It is kept in the signature rather than removed because the routing it
	// used to select is decided by Requeue from the task's own tag, and changing the signature would
	// churn every Event call site to no effect.
	(void)hiPri;
	auto& sched = TaskScheduler::Instance();
	for (size_t i = 0; i < n; ++i) sched.Requeue(tasks[i]);
}
