// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/GlobalFiberPool.h"
#include "../include/Thread.h"

using namespace JLib;

GlobalFiberPool::GlobalFiberPool(size_t tinyCount, size_t standardCount, size_t deepCount)
{
	// ORDER IS Standard, Tiny, Deep, and poolIndex runs DENSE ACROSS ALL THREE. Standard first so
	// the common class keeps the low indices it has always had -- every consumer that indexes by
	// poolIndex (FiberRegistry's table, Event's waiter index, HazardDomain's fiber rows) sees one
	// unbroken range and needed no change at all.
	const StackClass order[kClassCount] =
		{ StackClass::Standard, StackClass::Tiny, StackClass::Deep };
	const size_t counts[kClassCount] = { standardCount, tinyCount, deepCount };

	size = 0;
	size_t nextIndex = 0;

	for (size_t k = 0; k < kClassCount; ++k) {
		const StackClass cls = order[k];
		const size_t n       = counts[k];
		const size_t ci      = (size_t)cls;
		classCount[ci] = (unsigned int)n;
		size += (unsigned int)n;
		if (n == 0) continue;                   // NO ARENA AT ALL for an empty class -- a zero-count
		                                        // class must not reserve address space either.

		const size_t region = RegionFor(cls);
		arenas[ci] = new FiberStackArena(n * region);
		fibers[ci].reserve(n);

		for (size_t i = 0; i < n; ++i) {
			void* stackMem = arenas[ci]->AllocateStack(region);
			if (!stackMem) throw std::runtime_error("Failed to allocate stack");

			fibers[ci].emplace_back();
			Fiber& f = fibers[ci].back();
			f.stackBase  = stackMem;
			// STACK SIZE IS THE REGION, NOT THE USABLE FIGURE. Fiber::Init computes the stack TOP as
			// stackBase + stackSize, and the region base is what AllocateStack returned -- the guard
			// page is at the bottom, so the top is the far end of the whole region. Subtracting the
			// guard here would move the top down a page and waste it; the guard protects by being
			// unbacked at the LOW end, which is where a downward-growing stack runs into it.
			f.stackSize  = region;
			f.stackClass = cls;
			f.poolIndex  = nextIndex++;         // dense across classes

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

			// Into ITS OWN class's free queue. One shared queue would hand a Deep asker a 64 KB
			// stack, which is the failure the classes exist to prevent.
			availableFibers[ci].enqueue(&f);
		}
	}
}
GlobalFiberPool* GlobalFiberPool::Create(size_t standardCount, size_t tinyCount, size_t deepCount)
{
	return new GlobalFiberPool(tinyCount, standardCount, deepCount);
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
size_t GlobalFiberPool::StealInto(Fiber** dest, size_t maxCount, StackClass c) {
	if (maxCount == 0) return 0;
	return availableFibers[(size_t)c].try_dequeue_bulk(dest, maxCount);
}

// KEPT, AND NOW A WRAPPER RATHER THAN THE IMPLEMENTATION. It is public on GlobalFiberPool, so it
// stays; but having the allocating version be the one that does the work meant the hot path
// inherited the allocation. One implementation now, and it is the one without the vector.
std::vector<Fiber*> GlobalFiberPool::StealBatch(size_t count, StackClass c)
{
	std::vector<Fiber*> batch(count);
	const size_t got = StealInto(batch.data(), count, c);
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
	// ROUTED BY THE FIBER'S OWN CLASS, one bulk call per class present in the batch. A batch is
	// almost always single-class -- a worker's cache holds one class -- so the common case is one
	// scrub loop and one enqueue_bulk, exactly as before. The grouping exists so a mixed batch
	// cannot file a 512 KB stack into the Tiny queue, which would hand it to an I/O continuation
	// and quietly consume the class the whole design is trying to keep cheap.
	Fiber* grouped[kClassCount][64];
	size_t n[kClassCount] = {};
	for (size_t i = 0; i < count; ++i) {
		Fiber* f = fibers[i];
		f->ResetForReuse();
		const size_t ci = (size_t)f->stackClass;
		grouped[ci][n[ci]++] = f;
		if (n[ci] == 64) { availableFibers[ci].enqueue_bulk(grouped[ci], 64); n[ci] = 0; }
	}
	for (size_t ci = 0; ci < kClassCount; ++ci)
		if (n[ci]) availableFibers[ci].enqueue_bulk(grouped[ci], n[ci]);
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
	size_t n = 0;
	for (size_t ci = 0; ci < kClassCount; ++ci) n += availableFibers[ci].size_approx();
	return n;
}

// ---- WHICH FIBER OWNS THIS STACK ADDRESS? See the declaration for why this exists. -------------
//
// THE STRIDE IS THE PAGE-ROUNDED REGION, NOT RegionFor(cls). AllocateStack rounds its request up to
// whole pages before advancing the arena offset -- it has to, or two stacks could share a page and
// a guard page would land on the previous fiber's live stack top. Recomputing the raw region here
// would drift from the real spacing on any platform whose page size does not divide it, and the
// drift is SILENT: the index comes out one too low near the end of a large arena, so a fiber reads
// ANOTHER fiber's slot rather than failing. Round the same way the allocator did.
//
// BOUNDED BY WHAT WAS ACTUALLY BUILT, not by the reservation. The arena reserves address space for
// `n` stacks up front but a class may hold fewer live fibers than it reserved for; an address past
// the last one is not a fiber stack, and answering with an index into unbuilt storage would be
// worse than answering null.
//
// THE GUARD PAGE IS INSIDE THE REGION and is deliberately NOT special-cased: an address there still
// belongs to that fiber's region, and treating it as "no fiber" would answer null exactly when a
// stack is about to overflow -- the moment the answer matters most.
const JLib::Fiber* JLib::GlobalFiberPool::FiberForStack(const void* addr) const noexcept {
	if (!addr) return nullptr;
	const auto a = reinterpret_cast<uintptr_t>(addr);
	for (size_t ci = 0; ci < kClassCount; ++ci) {
		const FiberStackArena* ar = arenas[ci];
		if (!ar || fibers[ci].empty()) continue;
		const auto base = reinterpret_cast<uintptr_t>(ar->Base());
		if (a < base || a >= base + ar->TotalSize()) continue;

		const size_t page   = ar->PageSize();
		const size_t stride = (RegionFor((StackClass)ci) + page - 1) & ~(page - 1);
		if (stride == 0) continue;

		const size_t idx = (size_t)((a - base) / stride);
		if (idx >= fibers[ci].size()) return nullptr;   // reserved but never built
		return &fibers[ci][idx];
	}
	return nullptr;   // not on any fiber stack -- a Native task, main, or an app's own thread
}

JLib::Fiber* JLib::GlobalFiberPool::FiberForStack(const void* addr) noexcept {
	return const_cast<Fiber*>(static_cast<const GlobalFiberPool*>(this)->FiberForStack(addr));
}
