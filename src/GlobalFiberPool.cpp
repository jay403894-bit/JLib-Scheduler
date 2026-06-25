#include "../include/GlobalFiberPool.h"
#include "../include/T_Thread.h"

using namespace T_Threads;

GlobalFiberPool::GlobalFiberPool(size_t standardCount, size_t heavyCount)
	: standardArena(standardCount * 64 * 1024),
	heavyArena(heavyCount * 512 * 1024),
	availableFibers(standardCount + heavyCount),
	size(standardCount + heavyCount) // Initialize queue with total capacity
{
	// 1. Initialize standard fibers
	standardFibers.reserve(standardCount);
	for (size_t i = 0; i < standardCount; ++i) {
		void* stackMem = standardArena.AllocateStack(64 * 1024);
		if (!stackMem) throw std::runtime_error("Failed to allocate stack");

		standardFibers.emplace_back();
		Fiber& f = standardFibers.back();
		f.stackBase = stackMem;
		f.stackSize = 64 * 1024;

		// Push into the lock-free queue instead of a vector
		availableFibers.enqueue(&f);
	}

	// 2. Initialize heavy fibers
	heavyFibers.reserve(heavyCount);
	for (size_t i = 0; i < heavyCount; ++i) {
		void* stackMem = heavyArena.AllocateStack(512 * 1024);
		if (!stackMem) throw std::runtime_error("Failed to allocate stack");

		heavyFibers.emplace_back();
		Fiber& f = heavyFibers.back();
		f.stackBase = stackMem;
		f.stackSize = 512 * 1024;

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

void GlobalFiberPool::ReturnBatch(std::vector<Fiber*>&batch)
{
	if (batch.empty()) return;

	// No lock needed: MoodyCamel ConcurrentQueue handles 
	// multiple producers enqueueing simultaneously.
	for (Fiber* f : batch) {
		availableFibers.enqueue(f);
	}

	// Clear the local vector so the caller knows it's been emptied
	batch.clear();
}

void GlobalFiberPool::FiberEntryWrapper()
{
	Fiber* self = T_Thread::GetCurrent()->currentFiber;
	Task*  task = T_Thread::GetCurrent()->currentRunningTask;

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
