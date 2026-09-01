// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// FIBER REGISTRY -- WHO OWES CLEANUP FOR WHICH FIBER, AND THE LOOP THAT SETTLES IT.
//
// == WHY THIS IS NOT PART OF GlobalFiberPool ==
//
// The pool's job is SUPPLY: storage, stack arenas, lending fibers out and taking them back,
// assigning poolIndex. This is ACCOUNTING: who owes thread-affine cleanup, and driving the hops
// that discharge it. Different reason to change, different consumers.
//
// AND THE SPLIT IS FORCED, NOT MERELY TIDY. Settling a debt means pushing a task to a worker, so
// whoever drives cleanup needs TaskScheduler -- and TaskScheduler.h already includes
// GlobalFiberPool.h. Putting this on the pool is a cycle. A third party that includes both, and
// that neither of them includes, is the only direction that compiles.
//
// == THE ADDRESS TABLE ==
//
// A flat vector<Fiber*> indexed by poolIndex. Today that is 1:1 with the pool's single array, so
// it looks redundant -- it is not, and the reason is the heavy stack class. Heavy was deleted but
// poolIndex was designed for its return ("standard fibers occupy [0, standardCount), heavy fibers
// follow"). With a table, a second backing array is one more append at build time and NOTHING that
// indexes by table position changes. Without one, every such site learns about two arrays.
//
// == THE CLEANUP CHAIN ==
//
// A fiber that ENDS (as opposed to suspending) may owe cleanup to several workers -- see
// Fiber::creditors for why it is a set and not one home. Discharging it is a CHAIN, one hop at a
// time:
//
//     AdvanceCleanup(f):  take ONE creditor
//                         no creditor left?  -> recycle the fiber, done
//                         otherwise          -> make a cleanup task, push it to that worker's
//                                               resume inbox, return
//     the cleanup task:   release that worker's affine state, then call AdvanceCleanup(f) again
//
// SO THE LAST WORKER TO OWE ANYTHING IS THE ONE THAT RECYCLES, and no separate completion count is
// needed. The obvious alternative -- take every creditor, push N tasks, recycle -- is WRONG in a way
// that reads as equivalent: those tasks are QUEUED, not run, so the fiber returns to the pool while
// they still reference it. Pushing to the resume inbox makes them run soon; it does not make them
// run before the next statement.
//
// NOTHING CALLS AdvanceCleanup YET. The death-path hook is the next piece of work.

#pragma once
#include "Fiber.h"
#include <cstddef>
#include <vector>

namespace JLib {

	class GlobalFiberPool;
	class Task;

	class FiberRegistry {
	public:
		static FiberRegistry& Instance();

		// Fill the address table from `pool`. Idempotent; safe to call again after a Join/Init
		// cycle, which is why it clears first rather than appending.
		void Build(GlobalFiberPool* pool);
		void Reset();

		size_t Count() const { return table.size(); }
		Fiber* Get(size_t poolIndex) const {
			return poolIndex < table.size() ? table[poolIndex] : nullptr;
		}

		// ONE HOP OF THE CHAIN. Returns true if a cleanup task was dispatched to a creditor and the
		// chain continues; false if the fiber owed nobody and was handed to the recycler.
		//
		// Returns false ALSO when a dispatch fails, and the two are deliberately not distinguished
		// here -- see the .cpp. A caller that needs to know asks Fiber::HasCreditors.
		bool AdvanceCleanup(Fiber* f);

		// ---- SEAMS, so the chain can be tested without a live pool or scheduler ----------------
		//
		// Not a plugin system. These exist because the alternative is testing the chain THROUGH the
		// scheduler, and the last structure tested that way (deque_grow_test) deadlocked in the
		// plumbing and never reached its assertion. Both default to the real thing.
		//
		// Dispatch: hand `f`'s cleanup for `worker` to that worker. Return false if it could not be
		// delivered -- a dropped cleanup is a resource never given back, so this is checked.
		using DispatchFn = bool (*)(size_t worker, Fiber* f);
		// Recycle: the chain has drained; the fiber owes nobody and may return to the pool.
		using RecycleFn  = void (*)(Fiber* f);
		void SetDispatch(DispatchFn fn);   // null restores the default
		void SetRecycle(RecycleFn fn);     // null restores the default

		// The body a dispatched cleanup task runs: release this worker's affine state, then take
		// the next hop. Public because the default dispatch installs it as a Task function.
		static void CleanupHop(void* fiber);

		// Return `f` to the pool it was built from. ONE-SHOT: claims the fiber by CAS-ing its status
		// out of DEAD, so exactly one caller can hand it back however many reach here. A fiber
		// returned twice sits twice in the pool's available queue and is handed to two tasks --
		// two stacks that are the same stack, which is not a stall.
		//
		// Returns false if the claim was lost or there is no pool to return to.
		bool ReturnToPool(Fiber* f);

	private:
		FiberRegistry() = default;
		std::vector<Fiber*> table;
		// Kept from Build. The registry returns fibers to the pool it enumerated, rather than
		// reaching TaskScheduler::globalPool -- one source, and it cannot go stale against the
		// table it is indexing.
		GlobalFiberPool* pool = nullptr;
		DispatchFn dispatch = nullptr;
		RecycleFn  recycle  = nullptr;
	};

}
