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

		// Register this fiber's EBR slot ONCE. Fibers live for the whole program (the
		// pool is leaked, the vector is reserve()'d so it never reallocates), so the
		// slot address is stable and never needs unregistering -- no lifetime trap.
		EpochManager::Instance().RegisterParticipant(&f.localEpoch);

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


std::vector<Fiber*> GlobalFiberPool::StealBatch(size_t count)
{
	std::vector<Fiber*> batch;
	batch.reserve(count); // Optional: reserve space for efficiency

	Fiber* fiber = nullptr;
	// Loop until we have filled the requested batch or the queue is empty
	while (batch.size() < count && availableFibers.size_approx() > 0) {
		if(availableFibers.try_dequeue(fiber))
			batch.push_back(fiber);
	}

	return batch;
}
size_t GlobalFiberPool::StealInto(Fiber** dest, size_t maxCount) {
	// 1. Call your existing vector-based logic
	std::vector<Fiber*> stolen = StealBatch(maxCount);

	// 2. Copy it into the provided array (stack/buffer)
	size_t count = stolen.size();
	for (size_t i = 0; i < count; ++i) {
		dest[i] = stolen[i];
	}

	return count;
}


void GlobalFiberPool::ReturnBatch(Fiber** fibers, size_t count) {
	if (count == 0) return;

	// Direct enqueueing of the pointer batch
	for (size_t i = 0; i < count; ++i) {
		// A RECYCLED FIBER IS A NEW FIBER. This used to scrub localEpoch here and nothing else --
		// and that scrub exists because a fiber once went back announced at a dead epoch. A list of
		// inline field clears is correct until someone adds a field, and whoever adds it is not
		// reading this loop. Fiber::ResetForReuse keeps the scrub next to the members instead; the
		// full list, and what is deliberately NOT reset, is documented there.
		fibers[i]->ResetForReuse();
		availableFibers.enqueue(fibers[i]);
	}
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
	JLIB_EPOCH_CHECK_NO_GUARD("fiber exit (task returned)");

	ContextSwitch(&self->ctx, self->homeCtx);
}

size_t GlobalFiberPool::AvailableCount() const
{

	std::lock_guard<std::mutex> lock(poolMutex);
	return availableFibers.size_approx();

}
