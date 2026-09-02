// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// TOKEN REGISTRY -- the single index over every execution context that can owe thread-affine debt.
//
// == THE READER SPACE ==
//
// Three kinds, one contiguous index, fibers first:
//
//     [0,            fiberCount)                          fibers      -- by Fiber::poolIndex
//     [fiberCount,   fiberCount+workerCount)              workers     -- Native tasks read here
//     [.. +workerCount, .. +workerCount+kExternalReaders) external    -- main, an app's own threads
//
// THIS LAYOUT IS COPIED FROM HazardDomain ON PURPOSE, not reinvented. That file already resolves
// the same three cases (CurrentReader / IsFiberReader / kExternalReaders) and already paid for
// getting it wrong once -- resolving a fiber to its WORKER's cells is a real bug it keeps a
// deliberate test switch for. Two answers to one question is how they drift.
//
// FIBERS ARE THE PREFIX so that a fiber's poolIndex IS its reader id with no arithmetic, which
// keeps the hottest lookup a subscript.
//
// == WHY EXTERNAL IDS ARE CLAIMED AND NEVER RELEASED ==
//
// Same policy as HazardDomain, and inherited rather than re-decided: a program churning thousands
// of short-lived threads exhausts them, and that ASSERTS rather than corrupting. Releasing would
// mean reusing an id whose debt may not be discharged yet, and a reused reader id is the one thing
// that turns a leak into a use-after-free.
//
// == DELIVERY: THE TOKEN IS THE MESSAGE ==
//
// Each holder has an inbound chain -- a Treiber stack of tokens that owe it something. Delivering
// is one CAS; draining is one exchange. No Task, no slab allocation, no closure: the payload of a
// cleanup message is (token, kinds), and both live in the token.
//
// A SINGLE HOLDER IS A CHAIN OF ONE, which is the point. Pinned and migratable are not two
// mechanisms and not two code paths -- pinned is the creditor set with exactly one member, and its
// chain has exactly one link.
//
// == WHAT THIS IS NOT ==
//
// Not a supply mechanism. It does not hand out fibers, stacks or slots; GlobalFiberPool does that.
// This is ACCOUNTING -- who owes what, and driving the hops that settle it. The split is forced,
// not merely tidy: settling a debt means reaching the scheduler, and TaskScheduler.h already
// includes GlobalFiberPool.h.

#pragma once
#include "DebtToken.h"
#include <atomic>
#include <cstddef>
#include <vector>

namespace JLib {

	class TokenRegistry {
	public:
		static constexpr std::size_t kExternalReaders = 64;
		static constexpr std::size_t kNoReader        = ~std::size_t(0);

		static TokenRegistry& Instance();

		// Size the reader space. Idempotent; clears first rather than appending, so an Init/Join
		// cycle does not leave a stale tail behind.
		//
		// MUST RUN AFTER the fiber pool and the workers exist, for the same reason HazardDomain's
		// table does: both bounds are read from them.
		void Build(std::size_t fiberCount, std::size_t workerCount);
		void Reset();

		std::size_t FiberBase()    const { return 0; }
		std::size_t WorkerBase()   const {
#if defined(JLIB_TOKENREG_CTL_OVERLAP)
			return 0;      // CONTROL: worker ids collide with fiber ids. See token_registry_test.
#else
			return fibers;
#endif
		}
		std::size_t ExternalBase() const { return fibers + workers; }
		std::size_t Capacity()     const { return fibers + workers + kExternalReaders; }

		bool IsFiberReader(std::size_t r)    const { return r < fibers; }
		bool IsWorkerReader(std::size_t r)   const { return r >= WorkerBase()   && r < ExternalBase(); }
		bool IsExternalReader(std::size_t r) const { return r >= ExternalBase() && r < Capacity(); }

		// HOLDER ids -- the creditor space -- are threads only: workers then externals, rebased to
		// zero so the mask stays small. A fiber is never a creditor because a fiber cannot hold
		// thread-affine state.
		std::size_t HolderOfWorker(std::size_t w)   const { return w; }
		std::size_t HolderOfReader(std::size_t r)   const {
			if (IsWorkerReader(r))   return r - WorkerBase();
			if (IsExternalReader(r)) return workers + (r - ExternalBase());
			return kMaxHolders;                    // fibers are not holders
		}
		std::size_t HolderCount() const { return workers + kExternalReaders; }

		// Claim the next external reader id. Lazy, one-shot, never released; kNoReader when the 64
		// are gone. Safe from any thread.
		std::size_t ClaimExternal();
		std::size_t ExternalClaimed() const {
			return externalNext.load(std::memory_order_acquire);
		}

		void       Register(std::size_t readerId, DebtToken* t);
		DebtToken* Get(std::size_t readerId) const {
			return readerId < table.size() ? table[readerId] : nullptr;
		}

		// ---- DELIVERY --------------------------------------------------------------------------
		//
		// Push `t` onto `holder`'s inbound chain. One CAS. False only if the holder is out of range
		// or the registry is not built -- both of which mean the caller must not drop the debt.
		bool Deliver(std::size_t holder, DebtToken* t);
		// Take the WHOLE chain in one exchange. The caller owns every token in it and must walk
		// `next` itself; returns null when nothing was owed.
		DebtToken* TakeAll(std::size_t holder);
		bool       HolderHasWork(std::size_t holder) const;

		// Drain `holder`'s whole chain: release what it owes on each token, then advance that
		// token's chain. Returns how many tokens were handled. MUST BE CALLED BY THE HOLDER ITSELF
		// -- that is the entire point of routing by holder, and calling it for someone else runs
		// their thread-affine release on the wrong thread.
		std::size_t DrainHolder(std::size_t holder);

		// This thread's holder id: its worker's if it is on one, otherwise a lazily-claimed external
		// id. kMaxHolders when the 64 external slots are gone.
		//
		// THE WORKER BRANCH IS NOT CACHED AND THE BARE-THREAD BRANCH IS, and the asymmetry is the
		// point. A bare thread never migrates, so its id is stable for its whole life -- the same
		// argument CurrentEpochSlot's thread fallback rests on. A worker's answer is only valid
		// until the caller suspends, because a fiber resumed elsewhere is on a different thread.
		//
		// SO THE CONTRACT IS THE ONE THE EPOCH GUARD ALREADY CARRIES: do not hold this across a
		// suspend. It is a point read, not a value to keep.
		std::size_t CurrentHolder();

		// Drain every holder's chain. TEARDOWN ONLY -- it deliberately violates "the holder drains
		// its own", which is safe exactly once: after Join() has stopped the workers there is no
		// other thread left to run their releases, and the alternative is exiting still owing.
		std::size_t DrainAllForTeardown();

		// Where a resource wrapper hooks in. Nothing sets a debt kind yet, so this is unset and the
		// chain is exercised end to end without pretending a debt exists.
		using ReleaseFn = void (*)(std::size_t holder, DebtToken* t);
		void SetRelease(ReleaseFn fn);

		// ---- THE CHAIN -------------------------------------------------------------------------
		//
		// ONE HOP. True if the token was delivered to a creditor and the chain continues; false if
		// it owed nobody and was handed to the recycler.
		//
		// False ALSO on a failed delivery, and the two are deliberately not distinguished: a caller
		// that needs to know asks HasCreditors. The creditor is restored on failure, so a dropped
		// delivery is retried rather than lost -- a lost cleanup is not a lost task, it is a handle
		// that is never given back and nothing downstream would report it.
		bool AdvanceCleanup(DebtToken* t);

		// ---- SEAMS, so the chain can be tested with no pool and no scheduler --------------------
		//
		// Not a plugin system. The alternative is testing the chain THROUGH live plumbing, and the
		// last structure tested that way deadlocked before it ever reached its assertion.
		using DispatchFn = bool (*)(std::size_t holder, DebtToken* t);
		using RecycleFn  = void (*)(DebtToken* t);
		void SetDispatch(DispatchFn fn);
		void SetRecycle(RecycleFn fn);

	private:
		TokenRegistry() = default;

		std::vector<DebtToken*> table;                      // reader id -> token
		// One inbound chain head per HOLDER. unique_ptr-free: a vector of atomics cannot be resized
		// while anyone reads it, which is exactly why Build() is documented as an Init-time call.
		std::vector<std::atomic<DebtToken*>> inbound;

		std::size_t fibers  = 0;
		std::size_t workers = 0;
		std::atomic<std::size_t> externalNext{ 0 };

		DispatchFn dispatch = nullptr;
		RecycleFn  recycle  = nullptr;
		ReleaseFn  release  = nullptr;
	};

}  // namespace JLib
