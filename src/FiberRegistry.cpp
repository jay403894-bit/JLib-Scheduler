// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// See include/FiberRegistry.h for why this is not part of GlobalFiberPool, and for the shape of
// the cleanup chain. This file is where the registry's two dependencies meet -- the pool it reads
// the fiber table from, and the scheduler it pushes cleanup hops through.

#include "../include/FiberRegistry.h"
#include "../include/GlobalFiberPool.h"
#include "../include/TaskScheduler.h"

namespace JLib {

	FiberRegistry& FiberRegistry::Instance() {
		// Function-local static: constructed on first use, after main has started, so it cannot
		// race the pool's own construction order the way a file-scope object could.
		static FiberRegistry r;
		return r;
	}

	void FiberRegistry::Build(GlobalFiberPool* p) {
		table.clear();
		pool = p;
		if (!pool) return;
		const size_t n = pool->TotalCount();
		table.reserve(n);
		// ONE LOOP TODAY, TWO WHEN HEAVY RETURNS -- and the second one goes in
		// GlobalFiberPool::At, not here. Everything on this side indexes by table position and does
		// not learn that a second backing array exists.
		for (size_t i = 0; i < n; ++i) table.push_back(pool->At(i));
	}

	void FiberRegistry::Reset() { table.clear(); pool = nullptr; }

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

	// ---- the default dispatch: a real Task, into the creditor's resume inbox --------------------
	static bool DefaultDispatch(size_t worker, Fiber* f) {
		if (!TaskScheduler::IsInitialized()) return false;
		// CreateTask + PushResume, which is exactly the pair the decoupling buys: the pool could
		// reach neither without a cycle, and this file reaches both because it is downstream of
		// both.
		//
		// TaskType::Native, deliberately. A cleanup hop must run to completion on the worker it was
		// sent to; giving it a fiber would let it suspend, and a suspended cleanup on a migratable
		// pool can resume somewhere else -- which is precisely the thing being cleaned up after.
		Task* t = TaskScheduler::Instance().CreateInternalTask(&FiberRegistry::CleanupHop, f,
			/*hipri*/ 0);
		if (!t) return false;
		return TaskScheduler::PushResume(worker, t);
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
