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

		// The pool this registry was built over, or null before Build. Read by the fiber-local
		// path to answer "which fiber am I" from a stack address -- see SelfFiber in the .cpp.
		GlobalFiberPool* Pool() const noexcept { return pool; }
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
		//
		// ---- THE CONTRACT, AND BOTH HALVES ARE LOAD-BEARING ------------------------------------
		//
		// IT MUST BE IDEMPOTENT, AND IT MUST TOLERATE NOT OWNING ANYTHING.
		//
		// The registry guarantees AT MOST ONE dispatch per worker per fiber life -- `creditors` is a
		// bitmask, so a worker that picked the fiber up ten times is one bit, and TakeCreditor
		// clears that bit before dispatching. What the registry does NOT and cannot guarantee is
		// that the worker still holds what it owed. By the time a hop lands, that worker has run
		// arbitrary other work; the thing being released may already be gone, released by teardown,
		// or released by the resource's own destructor.
		//
		// So the hook checks its own state and returns when there is nothing to do. Concretely: if
		// the epoch slot it would clear already reads SIZE_MAX, that is the answer -- return, do not
		// clear it again. A hook written as "I am being called, therefore I still own this" produces
		// a double retire, or worse, touches a slot a LATER life is already using.
		//
		// IT RUNS BEFORE THE HOP ADVANCES, deliberately (see DrainHolder). Advancing can recycle the
		// fiber, and a recycled fiber is a different life -- so a hook that ran after would be
		// inspecting the next occupant's state while believing it was cleaning up the last one.
		//
		// IT IS NOT WHERE THE FIBER IS FREED. The registry owns that: the chain recycles when the
		// creditor set drains, which is after the last hop has RUN rather than merely been queued.
		using ReleaseFn = void (*)(size_t holder, Fiber* f);
		void SetRelease(ReleaseFn fn);

		// ---- QUEUE A RECLAMATION SWEEP AS A TASK ----------------------------------------------
		//
		// Called on every fiber death, rate-limited to one sweep in flight. The reaper SENDS a task
		// rather than sweeping inline, and rather than asking the application to call Tick() --
		// see the definition for why both of those were tried and rejected. Static because it needs
		// no registry state: it reaches the scheduler and the two reclamation domains directly.
		static void QueueReclaim();

		// ---- FIBER-LOCAL STORAGE: the replacement for thread_local -----------------------------
		//
		// TWO DIFFERENT INDICES, and keeping them apart is the whole reason this reads clearly:
		//
		//   THE FIBER ID   Fiber::poolIndex -- "the identity every table names". One per fiber,
		//                  stable for the fiber's life in the pool, and what GetID returns.
		//   AN FLS SLOT    a KIND of value, the same index on every fiber. FlsAlloc hands these out.
		//
		// Together they address fiber[id].local[slot]. Calling both "the id" would make
		// `FlsGet(slot)` look like it takes a fiber, which it does not -- it always means the
		// CURRENT fiber, because that is the only one the caller is inside.
		//
		// WHY THIS EXISTS AT ALL: migratable fibers resume on whichever worker is free, so a
		// thread_local read before a suspension point is not the same object after it -- silently,
		// because you get the resuming worker's copy. This is attached to the fiber, which is the
		// thing that actually survives the wait.
		//
		// FlsAlloc IS PROCESS-WIDE AND ONE-SHOT. Call it once at startup and keep the result; it
		// never releases a slot, because releasing one while a fiber still holds a value in it
		// would hand that value to the next caller who allocated the same index. There are
		// Fiber::kLocalSlots of them and kNoSlot comes back when they are gone -- checked, not
		// asserted, so a library that runs out degrades instead of taking the process down.
		static constexpr uint16_t kNoSlot = 0xFFFF;

		static uint16_t FlsAlloc() noexcept;
		static void*    FlsGet(uint16_t slot) noexcept;
		static void     FlsSet(uint16_t slot, void* p) noexcept;

		// The registry's identity for a fiber. SIZE_MAX when there is none -- off a fiber, or a
		// fiber the pool never registered.
		static size_t GetID(const Fiber* f) noexcept;
		static size_t GetID() noexcept;          // the fiber this call is running on

		// ---- WHO FREES A FIBER-LOCAL SLOT: NOBODY. SLOTS ARE BORROWED --------------------------
		//
		// A slot is a `void*` and the library has no type for it, so on recycle it is CLEARED and
		// never freed. Whoever put a value in a slot owns that value and frees it themselves.
		//
		// `SetSlotDeleter(slot, fn)` USED TO MAKE A SLOT OWNING and was removed, along with
		// TaskScheduler::ReleaseOnFiberDeath -- the library's other user-deletion ledger. Two
		// schemes, keyed differently, firing at the same moment, and that moment is the problem:
		// when a fiber dies its slots are unreachable, so freeing was already safe and needed no
		// ledger. If a value CAN still be reached, a fiber's death is not what makes freeing it
		// safe -- epochs and hazards are, and both already exist for that. A third deferral bought
		// no case the first two do not cover.
		//
		// The feature's own test recorded the cost of getting it wrong in both directions:
		// installing a deleter late freed a slot that earlier arms had used to park integer
		// sentinels, and withdrawing one early leaked (150 allocations, 144 frees, 6 fibers still
		// in a worker's cache when it came out). Both are what "this slot is owning" means arriving
		// or leaving while the pool disagrees -- which is a hazard the borrowed-only rule does not
		// have.
		//
		// THE CREDITOR CHAIN IS UNAFFECTED and is a different mechanism for a different problem:
		// not "free this later" but "only worker q may retract this", which no epoch or hazard can
		// express. See Fiber::creditors and TaskScheduler::ReleaseOnWorker.

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

	// ---- FiberLocal<T>: the typed face of the slots -------------------------------------------
	//
	// Use this where you would have used `thread_local` for anything that must survive a suspend.
	//
	//     static JLib::FiberLocal<Scratch> g_scratch = JLib::MakeFiberLocal<Scratch>();
	//     ...
	//     g_scratch.set(scratch);          // inside a fiber
	//     g_scratch->Reset();              // later, possibly on a DIFFERENT worker -- still yours
	//
	// A POINTER, NOT A VALUE, and deliberately: the slot is one `void*`, so this hands out what you
	// put in it and never constructs or destroys anything. OWNERSHIP STAYS YOURS, and there is no
	// longer any way to hand it over: release the object before the task ends. The library frees
	// nothing a slot points at, and both ledgers that used to offer otherwise -- a slot deleter here
	// and TaskScheduler::ReleaseOnFiberDeath -- were removed as a third reclamation scheme that
	// bought no case epochs and hazards do not already cover.
	//
	// NULL IS THE HONEST ANSWER OFF A FIBER, not a crash. A Native task and a bare thread have no
	// fiber-local storage at all, and library code that may run in either context has to be able to
	// ask rather than assume. `operator->` on a null get() is the caller's bug, exactly as it is
	// for any other pointer -- this does not paper over it with a dummy object.
	template <typename T>
	struct FiberLocal {
		uint16_t slot = FiberRegistry::kNoSlot;

		T*   get() const noexcept       { return static_cast<T*>(FiberRegistry::FlsGet(slot)); }
		void set(T* p) const noexcept   { FiberRegistry::FlsSet(slot, p); }
		T*   operator->() const noexcept { return get(); }
		explicit operator bool() const noexcept { return get() != nullptr; }
	};

	// Allocates the slot. Call ONCE, at startup, and keep the result -- see FlsAlloc for why slots
	// are never released. Returning the wrapper by value keeps the declaration a one-liner.
	template <typename T>
	inline FiberLocal<T> MakeFiberLocal() noexcept {
		return FiberLocal<T>{ FiberRegistry::FlsAlloc() };
	}

}
