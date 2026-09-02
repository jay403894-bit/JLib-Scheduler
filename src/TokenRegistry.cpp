// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// See include/TokenRegistry.h for the reader-space layout and why the token is the message.

#include "../include/TokenRegistry.h"
#include "../include/TaskScheduler.h"

namespace JLib {

	TokenRegistry& TokenRegistry::Instance() {
		// Function-local static: constructed on first use, after main has started, so it cannot
		// race the pool's construction order the way a file-scope object could.
		static TokenRegistry r;
		return r;
	}

	void TokenRegistry::Build(std::size_t fiberCount, std::size_t workerCount) {
		// THE HOLDER MASK MUST COVER EVERY ADDRESSABLE THREAD. If it does not, NoteCreditor refuses
		// a high-numbered holder -- correctly, since wrapping would bill the wrong one -- and that
		// holder's cleanup is dropped instead. Silent, and only on machines wide enough to reach
		// the gap, which is the worst combination.
		//
		// ASSERTED HERE rather than in DebtToken.h because kMaxHintQueues lives on TaskScheduler and
		// that header cannot be reached from there without a cycle.
		static_assert(kMaxHolders >= 256 + kExternalReaders,
			"kHolderWords is too narrow for the scheduler's worker ceiling plus the external ids.");

		fibers  = fiberCount;
		workers = workerCount;
		externalNext.store(0, std::memory_order_release);

		table.assign(Capacity(), nullptr);
		// RESIZED, NOT ASSIGNED: std::atomic is neither copyable nor movable, so the vector can only
		// be built by construction. This is also why Build() is an Init-time call and not something
		// that may run while a holder is reading its own head.
		std::vector<std::atomic<DebtToken*>> fresh(HolderCount());
		for (auto& h : fresh) h.store(nullptr, std::memory_order_relaxed);
		inbound.swap(fresh);
	}

	void TokenRegistry::Reset() {
		table.clear();
		inbound.clear();
		fibers = workers = 0;
		externalNext.store(0, std::memory_order_release);
	}

	std::size_t TokenRegistry::ClaimExternal() {
		// FETCH_ADD, THEN BOUNDS-CHECK -- and the check must survive the counter running past the
		// end, because it does: every caller after the 64th still increments. Comparing the
		// PRE-INCREMENT value is what makes that safe; testing the post-increment counter would
		// re-admit a claimer the moment it wrapped.
		const std::size_t slot = externalNext.fetch_add(1, std::memory_order_acq_rel);
#if defined(JLIB_TOKENREG_CTL_NO_EXHAUST_GUARD)
		// CONTROL: wrap instead of refusing. Two threads then get the same id, which is the exact
		// failure a reused reader id causes -- a leak becomes a use-after-free.
		return ExternalBase() + (slot % kExternalReaders);
#else
		if (slot >= kExternalReaders) return kNoReader;
		return ExternalBase() + slot;
#endif
	}

	void TokenRegistry::Register(std::size_t readerId, DebtToken* t) {
		if (readerId >= table.size()) return;
		if (t) t->readerId = readerId;
		table[readerId] = t;
	}

	bool TokenRegistry::Deliver(std::size_t holder, DebtToken* t) {
		if (!t || holder >= inbound.size()) return false;
		// TREIBER PUSH. Many producers (any thread that finishes a hop), exactly one consumer (the
		// holder, via TakeAll), which is the same producer/consumer split the resume inboxes have
		// and for the same reason.
		DebtToken* head = inbound[holder].load(std::memory_order_relaxed);
		do {
			t->next = head;
		} while (!inbound[holder].compare_exchange_weak(head, t,
					std::memory_order_release, std::memory_order_relaxed));
		return true;
	}

	DebtToken* TokenRegistry::TakeAll(std::size_t holder) {
		if (holder >= inbound.size()) return nullptr;
		// ONE EXCHANGE TAKES THE WHOLE CHAIN. Popping one at a time would race a concurrent push
		// against the head for every element; this touches the head exactly once no matter how many
		// tokens are waiting.
		return inbound[holder].exchange(nullptr, std::memory_order_acq_rel);
	}

	bool TokenRegistry::HolderHasWork(std::size_t holder) const {
		if (holder >= inbound.size()) return false;
		// ACQUIRE LOAD, NOT AN RMW. A sweeper polls this across every holder; taking the line
		// exclusive on each one would dirty a cache line per holder per poll whether or not anything
		// is there. Event::SignalAll learned this the expensive way -- it exchanged every word of
		// its occupied table unconditionally, so waking ONE waiter cost 35 RMWs at 31 workers.
		return inbound[holder].load(std::memory_order_acquire) != nullptr;
	}

	// ---- the default dispatch: the token itself, onto the creditor's inbound chain --------------
	static bool DefaultDispatch(std::size_t holder, DebtToken* t) {
		return TokenRegistry::Instance().Deliver(holder, t);
	}

	static void DefaultRecycle(DebtToken* t) {
		// THE CHAIN HAS DRAINED, so the token owes nobody. The registry does not own the storage --
		// tokens are embedded in a Fiber, a Thread or the external array -- so there is nothing here
		// to free. Scrubbing is the owner's job at ITS reuse point, which is the one place that
		// knows whether the slot is being reused at all.
		if (t) t->owedKinds.store(kOwesNothing, std::memory_order_release);
	}

	void TokenRegistry::SetDispatch(DispatchFn fn) { dispatch = fn; }
	void TokenRegistry::SetRecycle(RecycleFn fn)   { recycle  = fn; }

	bool TokenRegistry::AdvanceCleanup(DebtToken* t) {
		if (!t) return false;

#if defined(JLIB_TOKENREG_CTL_FANOUT)
		// CONTROL: take every creditor and deliver to all of them, then recycle. Reads as
		// equivalent and is not -- those tokens are QUEUED, not RUN, so the token is recycled while
		// the chain still references it. The test asserts a hop actually RAN before the recycle.
		{
			bool any = false;
			for (;;) {
				const std::size_t h = t->TakeCreditor();
				if (h == kMaxHolders) break;
				(dispatch ? dispatch : &DefaultDispatch)(h, t);
				any = true;
			}
			(recycle ? recycle : &DefaultRecycle)(t);
			return any;
		}
#else
		const std::size_t holder = t->TakeCreditor();
		if (holder == kMaxHolders) {
			// NOBODY IS OWED. The chain ends here, and whoever is running this hop is the last
			// holder that owed anything -- which is what makes a separate completion count
			// unnecessary.
			(recycle ? recycle : &DefaultRecycle)(t);
			return false;
		}

		if ((dispatch ? dispatch : &DefaultDispatch)(holder, t))
			return true;

		// DELIVERY FAILED AND THE CREDITOR IS ALREADY OFF THE SET -- TakeCreditor removed it. Put it
		// back rather than losing it. NoteCreditor is idempotent, so restoring costs one OR and
		// cannot double-count.
		t->NoteCreditor(holder);
		return false;
#endif
	}

}  // namespace JLib
