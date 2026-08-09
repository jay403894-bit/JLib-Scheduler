// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/GlobalFiberPool.h"
#include "../include/Thread.h"

using namespace JLib;

GlobalFiberPool::GlobalFiberPool(size_t standardCount, size_t heavyCount)
	: standardArena(standardCount * kStandardStackSize),
	heavyArena(heavyCount * kHeavyStackSize),
	availableFibers(standardCount + heavyCount),
	size(standardCount + heavyCount) // Initialize queue with total capacity
{
	// 1. Initialize standard fibers
	standardFibers.reserve(standardCount);
	for (size_t i = 0; i < standardCount; ++i) {
		void* stackMem = standardArena.AllocateStack(kStandardStackSize);
		if (!stackMem) throw std::runtime_error("Failed to allocate stack");

		standardFibers.emplace_back();
		Fiber& f = standardFibers.back();
		f.stackBase = stackMem;
		f.stackSize = kStandardStackSize;

		// Register this fiber's EBR slot ONCE. Fibers live for the whole program (the
		// pool is leaked, the vector is reserve()'d so it never reallocates), so the
		// slot address is stable and never needs unregistering -- no lifetime trap.
		EpochManager::Instance().RegisterParticipant(&f.localEpoch);

		// Push into the lock-free queue instead of a vector
		availableFibers.enqueue(&f);
	}

	// 2. Initialize heavy fibers
	heavyFibers.reserve(heavyCount);
	for (size_t i = 0; i < heavyCount; ++i) {
		void* stackMem = heavyArena.AllocateStack(kHeavyStackSize);
		if (!stackMem) throw std::runtime_error("Failed to allocate stack");

		heavyFibers.emplace_back();
		Fiber& f = heavyFibers.back();
		f.stackBase = stackMem;
		f.stackSize = kHeavyStackSize;

		EpochManager::Instance().RegisterParticipant(&f.localEpoch);  // see standard loop

		// Push into the lock-free queue
		availableFibers.enqueue(&f);
	}
}
GlobalFiberPool* GlobalFiberPool::Create(size_t standardCount, size_t heavyCount)
{
	return new GlobalFiberPool(standardCount, heavyCount);
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
		fibers[i]->localEpoch.store(SIZE_MAX, std::memory_order_release);  // Defensive: clear epoch state
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


	ContextSwitch(&self->ctx, self->homeCtx);
}

size_t GlobalFiberPool::AvailableCount() const
{

	std::lock_guard<std::mutex> lock(poolMutex);
	return availableFibers.size_approx();

}
