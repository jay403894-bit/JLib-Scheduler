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
	ContextSwitch(&this->ctx, this->homeCtx);
}

void Fiber::Suspend() {
	JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Suspend");
	// Record intent and switch out.
	this->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
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
	// minPerSegment = 1, not the default 64: this batch is a WAKE, and every fiber in it became
	// runnable at the same instant. Handing all of them to one worker would leave the other 30
	// stealing them back one at a time, which is most of the cost this exists to remove.
	TaskScheduler::Instance().PushBatch(tasks, n, 0, /*minPerSegment*/1, hiPri);
}
