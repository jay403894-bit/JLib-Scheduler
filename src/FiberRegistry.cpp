// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// See include/FiberRegistry.h for why this is not part of GlobalFiberPool, and for the shape of
// the cleanup chain. This file is where the registry's two dependencies meet -- the pool it reads
// the fiber table from, and the scheduler it pushes cleanup hops through.

#include "../include/FiberRegistry.h"
#include "../include/GlobalFiberPool.h"
#include "../include/TaskScheduler.h"
#include "../include/Thread.h"

namespace JLib {

	FiberRegistry& FiberRegistry::Instance() {
		// LEAKED ON PURPOSE, and this is not tidiness -- a plain `static FiberRegistry r;` CRASHES
		// now that workers poll this every pass.
		//
		// A function-local static is destroyed at exit in reverse construction order, and
		// AtExitDestroyer calls Join() from ITS destructor -- so the registry is destroyed while the
		// workers reading it are still spinning, and each pass then reads `inbound` through freed
		// storage. Measured, not theorised: ACCESS_VIOLATION and heap corruption inside ntdll, in
		// five tests that never touch this registry (DagCancelTest 3/3, plus MutexCancel, IoOptIn,
		// ReservedLoPriPlacement and WaitGroupCancel intermittently), every one of which passed when
		// run alone. What localised it was compiling the worker-side drain out for one build.
		//
		// SAME CHOICE THE FIBER POOL MAKES, for the same reason: a process about to stop existing
		// gains nothing from freeing its address space, and everything from not pulling a structure
		// out from under threads that are still running.
		static FiberRegistry* r = new FiberRegistry();
		return *r;
	}

	void FiberRegistry::Build(GlobalFiberPool* p, size_t workerCount) {
		// THE CREDITOR MASK MUST COVER EVERY ADDRESSABLE HOLDER -- workers AND the external ids main
		// and an app's own threads claim. If it does not, NoteCreditor refuses a high-numbered
		// holder (correctly, since wrapping would bill the wrong thread) and that holder's cleanup
		// is dropped instead. Silent, and only on machines wide enough to reach the gap.
		//
		// ASSERTED HERE rather than in Fiber.h, which cannot see kMaxHintQueues without a cycle.
		static_assert(Fiber::kCreditorWords * 64 >= 256 + kExternalReaders,
			"Fiber::kCreditorWords is too narrow for the worker ceiling plus the external ids.");

		table.clear();
		pool = p;
		workers = workerCount;
		externalNext.store(0, std::memory_order_release);

		// RESIZED BY CONSTRUCTION, NOT assign(): std::atomic is neither copyable nor movable, so a
		// vector of them can only be built fresh and swapped in. This is also why Build is an
		// Init-time call and not something that may run while a holder reads its own head.
		std::vector<std::atomic<Fiber*>> fresh(HolderCount());
		for (auto& h : fresh) h.store(nullptr, std::memory_order_relaxed);
		inbound.swap(fresh);

		if (!pool) return;
		const size_t n = pool->TotalCount();
		table.reserve(n);
		// ONE LOOP TODAY, TWO WHEN HEAVY RETURNS -- and the second one goes in
		// GlobalFiberPool::At, not here. Everything on this side indexes by table position and does
		// not learn that a second backing array exists.
		for (size_t i = 0; i < n; ++i) table.push_back(pool->At(i));
	}

	void FiberRegistry::Reset() {
		table.clear();
		inbound.clear();
		pool = nullptr;
		workers = 0;
		externalNext.store(0, std::memory_order_release);
	}

	// ---- THE HOLDER SIDE ----------------------------------------------------------------------

	size_t FiberRegistry::ClaimExternal() {
		// FETCH_ADD THEN BOUNDS-CHECK THE **PRE-INCREMENT** VALUE, because the counter keeps rising
		// past the end: every caller after the 64th still increments it. Testing the post-increment
		// counter would re-admit a claimer the moment it wrapped, and a reused holder id turns a
		// leak into a use-after-free.
		const size_t slot = externalNext.fetch_add(1, std::memory_order_acq_rel);
#if defined(JLIB_FIBERHOLDER_CTL_NO_EXHAUST_GUARD)
		return workers + (slot % kExternalReaders);   // CONTROL: wrap instead of refusing
#else
		if (slot >= kExternalReaders) return kNoHolder;
		return workers + slot;
#endif
	}

	size_t FiberRegistry::CurrentHolder() {
		// See the header for why one branch caches and the other must not.
		if (Thread* w = Thread::Current()) {
			const size_t h = HolderOfWorker((size_t)w->qIndex);
			if (h < workers) return h;
			// A Thread that is not one of our workers (a bound main thread) falls through to the
			// external path rather than indexing past the worker range.
		}
		thread_local size_t mine = kNoHolder;
		if (mine == kNoHolder) mine = ClaimExternal();
		return mine;
	}

	bool FiberRegistry::Deliver(size_t holder, Fiber* f) {
		if (!f || holder >= inbound.size()) return false;
		// TREIBER PUSH. Many producers (any thread finishing a hop), exactly one consumer (the
		// holder, via TakeAll) -- the same producer/consumer split the resume inboxes have, for the
		// same reason.
		Fiber* head = inbound[holder].load(std::memory_order_relaxed);
		do {
			f->cleanupNext = head;
		} while (!inbound[holder].compare_exchange_weak(head, f,
					std::memory_order_release, std::memory_order_relaxed));

#if !defined(JLIB_FIBERHOLDER_CTL_NO_NOTIFY)
		// PUSH FIRST, NOTIFY SECOND. The reverse loses the wake outright: the target can observe an
		// empty chain, eat its permit and park with the fiber arriving just behind it.
		//
		// WORKERS ONLY. External holders are not parked by the scheduler and have no permit to hand
		// -- they poll ProcessMainThread, which is the contract.
		if (holder < workers) TaskScheduler::NotifyHolder(holder);
#endif
		return true;
	}

	Fiber* FiberRegistry::TakeAll(size_t holder) {
		if (holder >= inbound.size()) return nullptr;
		// ONE EXCHANGE TAKES THE WHOLE CHAIN. Popping one at a time would race a concurrent push
		// against the head for every element; this touches the head once however many are waiting.
		return inbound[holder].exchange(nullptr, std::memory_order_acq_rel);
	}

	bool FiberRegistry::HolderHasWork(size_t holder) const {
		if (holder >= inbound.size()) return false;
		// ACQUIRE LOAD, NOT AN RMW. A worker polls this every pass; taking the line exclusive each
		// time would dirty a cache line per worker per pass whether or not anything is there.
		// Event::SignalAll learned this the expensive way -- it exchanged every word of its occupied
		// table unconditionally, so waking ONE waiter cost 35 RMWs at 31 workers.
		return inbound[holder].load(std::memory_order_acquire) != nullptr;
	}

	size_t FiberRegistry::DrainHolder(size_t holder) {
		Fiber* f = TakeAll(holder);
		size_t n = 0;
		while (f) {
			// READ cleanupNext BEFORE ADVANCING. AdvanceCleanup may link this same fiber onto
			// another holder's chain, which overwrites it -- so the walk must capture its successor
			// first or it follows the fiber into someone else's list.
			Fiber* nxt = f->cleanupNext;
			f->cleanupNext = nullptr;
			// RELEASE WHAT THIS HOLDER OWES, then hand on. Running the release BEFORE advancing is
			// load-bearing: advance may recycle the fiber, and a recycled fiber is a new fiber.
			if (release) release(holder, f);
			AdvanceCleanup(f);
			++n;
			f = nxt;
		}
		return n;
	}

	size_t FiberRegistry::DrainAllForTeardown() {
		size_t n = 0;
		for (size_t h = 0; h < inbound.size(); ++h) n += DrainHolder(h);
		return n;
	}

	void FiberRegistry::SetRelease(ReleaseFn fn) { release = fn; }

	bool FiberRegistry::ReturnToPool(Fiber* f) {
		if (!f || !pool) return false;
		// CLAIM IT. DEAD is precisely "finished, pending cleanup/reclamation", so it is the state a
		// fiber is in for exactly the window this runs in, and CAS-ing out of it is a one-shot that
		// costs nothing on the winning path. Anything not in DEAD is either still running or was
		// already claimed, and both mean: not mine to return.
		FiberStatus expected = FiberStatus::DEAD;
		if (!f->status.compare_exchange_strong(expected, FiberStatus::READY,
				std::memory_order_acq_rel, std::memory_order_acquire))
			return false;
		// ReturnBatch calls Fiber::ResetForReuse, so the scrub happens on exactly one path whether
		// a fiber comes back from here or from a worker's batch return. Doing it here as well
		// would be a second place to forget a field.
		pool->ReturnBatch(&f, 1);
		return true;
	}

	void FiberRegistry::SetDispatch(DispatchFn fn) { dispatch = fn; }
	void FiberRegistry::SetRecycle(RecycleFn fn)   { recycle  = fn; }

	// ---- the default dispatch: THE FIBER ITSELF, onto the creditor's chain -----------------------
	//
	// This used to be CreateTask + PushResume -- a 64-byte slab allocation, a task lifecycle, an
	// inbox hop and a DestroyTask, all to carry two words (which fiber, and what it owes) that were
	// already ON the fiber. Linking the fiber is one CAS and cannot fail for want of memory, which
	// matters on a death path: an allocation here is least welcome and most likely to be the thing
	// that fails, and a dropped cleanup is a resource never given back.
	static bool DefaultDispatch(size_t holder, Fiber* f) {
		return FiberRegistry::Instance().Deliver(holder, f);
	}

	static void DefaultRecycle(Fiber* f) {
		// THE CHAIN HAS DRAINED, SO THE FIBER OWES NOBODY AND GOES BACK. Everything a recycled
		// fiber must not carry -- creditors included -- is scrubbed by Fiber::ResetForReuse, which
		// ReturnBatch calls; this path deliberately does not scrub anything itself, so there is one
		// list of fields to keep current rather than two.
		//
		// A LOST CLAIM IS NOT AN ERROR. ReturnToPool returns false when the fiber was not in DEAD --
		// already returned, or never finished -- and both mean somebody else owns it. Nothing to
		// report and nothing to retry.
		FiberRegistry::Instance().ReturnToPool(f);
	}

	bool FiberRegistry::AdvanceCleanup(Fiber* f) {
		if (!f) return false;

#if defined(JLIB_FIBERHOLDER_CTL_FANOUT)
		// CONTROL: take every creditor, deliver to all of them, then recycle. Reads as equivalent
		// and is not -- those deliveries are QUEUED, not RUN, so the fiber is recycled while the
		// chains still reference it. The test asserts a hop actually RAN before the first recycle,
		// and that the fiber is recycled exactly once.
		{
			bool any = false;
			for (;;) {
				const size_t h = f->TakeCreditor();
				if (h == SIZE_MAX) break;
				(dispatch ? dispatch : &DefaultDispatch)(h, f);
				any = true;
			}
			(recycle ? recycle : &DefaultRecycle)(f);
			return any;
		}
#endif
		const size_t worker = f->TakeCreditor();
		if (worker == SIZE_MAX) {
			// NOBODY IS OWED. The chain ends here, and whoever is running this hop is the last
			// worker that owed anything -- which is what makes a completion count unnecessary.
			(recycle ? recycle : &DefaultRecycle)(f);
			return false;
		}

		if ((dispatch ? dispatch : &DefaultDispatch)(worker, f))
			return true;

		// DISPATCH FAILED, AND THE CREDITOR IS ALREADY OFF THE SET -- TakeCreditor removed it. Put
		// it back rather than losing it: a dropped cleanup is not a dropped task, it is an
		// apartment or a handle that is never given back, and nothing downstream would report it.
		// NoteCreditor is idempotent, so restoring costs one OR and cannot double-count.
		f->NoteCreditor(worker);
		return false;
	}

	void FiberRegistry::CleanupHop(void* fiber) {
		Fiber* f = static_cast<Fiber*>(fiber);
		if (!f) return;
		// THE ACTUAL RELEASE OF THIS WORKER'S AFFINE STATE GOES HERE, and it is an application
		// concern: COM apartments, thread-owned handles, a magazine that refuses to be remote-freed.
		// The library itself owes nothing at this point -- slab frees route by address, epochs use a
		// global participant list, hazard cells are indexed by the fiber. So this is an extension
		// point with no default body rather than a list of library steps.
		//
		// Then take the next hop. Running the release BEFORE advancing is load-bearing: advance may
		// recycle the fiber, and a recycled fiber is a new fiber.
		Instance().AdvanceCleanup(f);
	}

}
