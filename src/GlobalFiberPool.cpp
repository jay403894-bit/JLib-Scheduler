// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/GlobalFiberPool.h"
#include "../include/Thread.h"

using namespace JLib;

GlobalFiberPool::GlobalFiberPool(size_t standardCount)
	: standardArena(standardCount * kStandardStackSize),
	availableFibers(standardCount),
	size(standardCount) // Initialize queue with total capacity
{
	standardFibers.reserve(standardCount);
	for (size_t i = 0; i < standardCount; ++i) {
		void* stackMem = standardArena.AllocateStack(kStandardStackSize);
		if (!stackMem) throw std::runtime_error("Failed to allocate stack");

		standardFibers.emplace_back();
		Fiber& f = standardFibers.back();
		f.stackBase = stackMem;
		f.stackSize = kStandardStackSize;
		f.poolIndex = i;                        // one dense range, [0, standardCount)

		// TSAN: ONE HANDLE PER FIBER, MADE HERE AND NEVER DESTROYED -- which matches the fibers
		// themselves, since the pool reserves and leaks them. Without this every switch into this
		// stack is invisible to the sanitizer and it attributes two fibers on one worker to a single
		// thread, which misses real races and invents false ones. No-op without the sanitizer.
		f.tsanFiber = tsan::CreateFiber();

		// NO PER-FIBER EPOCH SLOT. This registered &f.localEpoch as an EBR participant, which meant
		// MinActiveEpoch scanned one slot PER FIBER on every reclaim attempt -- 2016 slots and
		// 0.629 us per call on a 31-worker box, against 32 once only threads participate.
		//
		// The fiber slot existed so protection could survive a suspension. It never needed to:
		// suspending inside an EpochGuard is forbidden and is now enforced in Release, so a guard's
		// enter and leave are always on the same thread and the THREAD's slot is correct. See
		// CurrentEpochSlot in Thread.h.

		// Push into the lock-free queue instead of a vector
		availableFibers.enqueue(&f);
	}

	// A SECOND LOOP BUILT A 512 KB "HEAVY" CLASS HERE AND IS GONE. Nothing requested it -- see the
	// note in the header -- and it committed ~127 MB on a 31-worker pool while it waited.
}
GlobalFiberPool* GlobalFiberPool::Create(size_t standardCount)
{
	return new GlobalFiberPool(standardCount);
}


// ---- REFILL: ONE BULK DEQUEUE, NO ALLOCATION -------------------------------------------------
//
// StealInto is the REFILL path -- ThreadLocalCache::Pop calls it whenever a worker's cache runs
// dry -- so it runs whenever a worker needs a fiber and has none. It used to go through the
// vector-returning StealBatch and paid for it three times over:
//
//   * A HEAP ALLOCATION PER REFILL. StealBatch built a std::vector, StealInto copied it into the
//     caller's array and destroyed it. A malloc and a copy to move pointers we already had.
//   * size_approx() AS THE LOOP CONDITION. It is a sum across producer blocks, not a load, and it
//     was re-evaluated every iteration.
//   * ONE try_dequeue PER FIBER, each its own trip through the queue's block machinery.
//
// try_dequeue_bulk does the whole thing in one call with no allocation. The queue always had the
// entry point; we were just not using it.
size_t GlobalFiberPool::StealInto(Fiber** dest, size_t maxCount) {
	if (maxCount == 0) return 0;
	return availableFibers.try_dequeue_bulk(dest, maxCount);
}

// KEPT, AND NOW A WRAPPER RATHER THAN THE IMPLEMENTATION. It is public on GlobalFiberPool, so it
// stays; but having the allocating version be the one that does the work meant the hot path
// inherited the allocation. One implementation now, and it is the one without the vector.
std::vector<Fiber*> GlobalFiberPool::StealBatch(size_t count)
{
	std::vector<Fiber*> batch(count);
	const size_t got = StealInto(batch.data(), count);
	batch.resize(got);
	return batch;
}


void GlobalFiberPool::ReturnBatch(Fiber** fibers, size_t count) {
	if (count == 0) return;

	// A RECYCLED FIBER IS A NEW FIBER. This used to scrub localEpoch here and nothing else -- and
	// that scrub exists because a fiber once went back announced at a dead epoch. A list of inline
	// field clears is correct until someone adds a field, and whoever adds it is not reading this
	// loop. Fiber::ResetForReuse keeps the scrub next to the members instead; the full list, and
	// what is deliberately NOT reset, is documented there.
	//
	// SCRUB ALL, THEN PUBLISH ONCE. The scrub must finish before a fiber is visible to another
	// worker, so the two cannot be interleaved into one enqueue-per-fiber loop and still be one
	// bulk call -- but they do not need to be: every fiber here is already ours. Same reasoning as
	// the refill above, and the same saving.
	for (size_t i = 0; i < count; ++i) fibers[i]->ResetForReuse();
	availableFibers.enqueue_bulk(fibers, count);
}

void GlobalFiberPool::FiberEntryWrapper()
{
	Fiber* self = Thread::GetCurrent()->currentFiber;
	Task*  task = Thread::GetCurrent()->currentRunningTask;

	if (self && task) {
		task->Execute();
	}

	self->status.store(FiberStatus::DEAD, std::memory_order_release);

	// A task that RETURNS while still announced would leave this fiber's slot pinned at an old
	// epoch, and the fiber then goes back to the pool carrying it. ReturnBatch() already scrubs
	// localEpoch to SIZE_MAX for exactly that reason -- this catches the CAUSE instead, since a
	// guard that outlives its traversal is a bug wherever it happens.
	JLIB_EPOCH_CHECK_NO_GUARD_AT_EXIT("fiber exit (task returned)");

	Thread::TsanSwitchToScheduler();
	ContextSwitch(&self->ctx, self->homeCtx);
}

size_t GlobalFiberPool::AvailableCount() const
{

	std::lock_guard<std::mutex> lock(poolMutex);
	return availableFibers.size_approx();

}
