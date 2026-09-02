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
//                         otherwise          -> LINK THE FIBER onto that holder's chain, return
//     the holder's drain: release its affine state, then call AdvanceCleanup(f) again
//
// SO THE LAST HOLDER TO OWE ANYTHING IS THE ONE THAT RECYCLES, and no separate completion count is
// needed. The obvious alternative -- take every creditor, deliver N times, recycle -- is WRONG in a
// way that reads as equivalent: those deliveries are QUEUED, not run, so the fiber returns to the
// pool while the chains still reference it. Delivery makes them run soon; it does not make them run
// before the next statement.
//
// THE FIBER IS THE MESSAGE. Each hop is one CAS onto Fiber::cleanupNext plus a wake. This replaced
// a real Task per hop -- a 64-byte slab allocation, a task lifecycle, an inbox hop and a
// DestroyTask -- to carry two words that were already on the fiber.
//
// THE DEATH HOOK IS LIVE: Thread::OnFiberReturned routes a DEAD fiber here when it OwesCleanup(),
// and the chain's last hop is what returns it to the pool. A fiber therefore cannot be handed out
// again with debts outstanding. Nothing SETS a debt kind yet, so the gate is false for every fiber
// today and the common path is one relaxed load.

#pragma once
#include "Fiber.h"
#include <atomic>
#include <cstddef>
#include <vector>

namespace JLib {

	class GlobalFiberPool;
	class Task;

	class FiberRegistry {
	public:
		static FiberRegistry& Instance();

		// Fill the fiber table from `pool` and size the holder space to `workerCount`. Idempotent;
		// safe to call again after a Join/Init cycle, which is why it clears first rather than
		// appending.
		//
		// MUST RUN BEFORE ANY WORKER EXISTS -- it swaps a vector of atomics, which cannot be resized
		// while a holder is reading its own chain head. StartPool calls it once the pool is built
		// and num_workers is fixed, which is the first point both bounds are known.
		void Build(GlobalFiberPool* pool, size_t workerCount);
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

		// ==== THE HOLDER SIDE ======================================================================
		//
		// TWO SPACES, TWO ARRAYS, AND THEY MUST NEVER BE INDEXED INTO EACH OTHER.
		//
		//     table[i]    -- fiber i's registration      (DEBTOR space, by Fiber::poolIndex)
		//     inbound[h]  -- holder h's cleanup chain    (HOLDER space, threads only)
		//
		// Thread 3 is not fiber 3. Nothing here ever puts a thread into `table`, and the accessors
		// are named for their space so writing that bug requires typing a word that contradicts it.
		//
		// WHY ONLY FIBERS ARE DEBTORS. A debtor is something that can borrow thread-affine state on
		// one thread and die on another -- so its debt is a SET of threads and it needs a record
		// that outlives the borrowing. A thread never travels, so its creditor set could only ever
		// name itself. Threads do not get registrations; they get a chain and a drain point. This
		// is why the reader-indexed registry that briefly existed alongside this one was deleted:
		// it held entries for threads whose creditor sets are empty by construction.
		//
		// WHY EXTERNAL IDS ARE CLAIMED AND NEVER RELEASED: inherited from HazardDomain rather than
		// re-decided. A program churning thousands of short-lived threads exhausts them and that
		// ASSERTS rather than corrupting. Releasing would mean reusing an id whose debt may not be
		// discharged, and a reused holder id turns a leak into a use-after-free.
		static constexpr size_t kExternalReaders = 64;
		static constexpr size_t kNoHolder        = ~size_t(0);

		size_t HolderCount()             const { return workers + kExternalReaders; }
		size_t HolderOfWorker(size_t w)  const { return w; }          // workers are the prefix
		bool   IsWorkerHolder(size_t h)  const { return h < workers; }

		// Claim the next external holder id. Lazy, one-shot, never released; kNoHolder when the 64
		// are gone. Safe from any thread.
		size_t ClaimExternal();

		// This thread's holder id: its worker's if it is on one, else a lazily-claimed external id.
		//
		// THE WORKER BRANCH IS NOT CACHED AND THE BARE-THREAD BRANCH IS. A bare thread never
		// migrates, so its id is stable for its whole life -- the same argument CurrentEpochSlot's
		// thread fallback rests on. A worker's answer is only valid until the caller suspends.
		// So this carries the contract the epoch guard already carries: DO NOT HOLD IT ACROSS A
		// SUSPEND. It is a point read, not a value to keep.
		size_t CurrentHolder();

		// Link `f` onto `holder`'s chain. One CAS, then a WAKE -- see TaskScheduler::NotifyHolder
		// for why the wake is mandatory rather than a latency choice.
		bool   Deliver(size_t holder, Fiber* f);
		// Take the WHOLE chain in one exchange. Caller owns every fiber in it and walks cleanupNext.
		Fiber* TakeAll(size_t holder);
		// ACQUIRE LOAD, NOT AN RMW -- this is polled every worker pass.
		bool   HolderHasWork(size_t holder) const;

		// Drain `holder`'s chain: release what it owes on each fiber, then advance that fiber's
		// chain. MUST BE CALLED BY THE HOLDER ITSELF; calling it for someone else runs their
		// thread-affine release on the wrong thread, which is the bug the routing exists to prevent.
		size_t DrainHolder(size_t holder);

		// Drain every holder. TEARDOWN ONLY -- it deliberately breaks "the holder drains its own",
		// which is safe exactly once: after Join() has stopped the workers there is no holder left
		// to run its own release, and the alternative is exiting still owing.
		size_t DrainAllForTeardown();

		// Where a resource wrapper hooks in. Nothing sets a debt kind yet, so this is unset and the
		// chain is exercised end to end without pretending a debt exists.
		using ReleaseFn = void (*)(size_t holder, Fiber* f);
		void SetRelease(ReleaseFn fn);

	private:
		FiberRegistry() = default;
		// Resolve a seam to the installed hook or the real default, with ONE load each.
		DispatchFn Dispatcher();
		RecycleFn  Recycler();

		std::vector<Fiber*> table;
		// One chain head per HOLDER. A vector of atomics cannot be resized while anyone reads it,
		// which is why Build() is an Init-time call made before any worker exists.
		std::vector<std::atomic<Fiber*>> inbound;
		size_t workers = 0;
		std::atomic<size_t> externalNext{ 0 };

		// ---- THE SEAMS ARE ATOMIC, AND THAT IS NOT PEDANTRY ------------------------------------
		//
		// These are read by WORKERS -- AdvanceCleanup and DrainHolder run on whichever thread owes
		// something -- and they are written by whoever installs a seam. fiber_drain_live_test
		// installs its release hook AFTER TaskScheduler::Init, so the pool is already up and the
		// write genuinely races the reads. As plain pointers that is UB that happens to work on
		// x86, which is the worst combination: it survives every run and TSan is right about it.
		//
		// Relaxed is enough. The pointer is the whole payload -- there is no data published
		// alongside it that a reader must see -- so there is nothing to acquire.
		std::atomic<DispatchFn> dispatchFn{ nullptr };
		std::atomic<RecycleFn>  recycleFn{ nullptr };
		std::atomic<ReleaseFn>  releaseFn{ nullptr };
		// Kept from Build. The registry returns fibers to the pool it enumerated, rather than
		// reaching TaskScheduler::globalPool -- one source, and it cannot go stale against the
		// table it is indexing.
		GlobalFiberPool* pool = nullptr;
		DispatchFn dispatch = nullptr;
		RecycleFn  recycle  = nullptr;
	};

}
