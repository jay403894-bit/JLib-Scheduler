// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE DEBT TOKEN -- one generic holder of thread-affine debt, for ANY execution context.
//
// == WHY THIS IS NOT ON Fiber ==
//
// The registry that drives cleanup used to index fibers, and that is the one thing it could not
// keep doing. Garbage is produced by three kinds of reader, not one:
//
//   * FIBERS        -- migrate, so their debt can name several holders
//   * WORKERS       -- run Native tasks, which cannot suspend, so a native task's reader identity
//                      IS its worker's. n slots, not one per task.
//   * BARE THREADS  -- main building a DAG retires through EpochManager::ThreadSlot. This is not a
//                      hypothetical: the library itself does it.
//
// A `std::vector<Fiber*>` can name the first and nothing else, which is why "just forbid epochs off
// a fiber" is not available -- it would forbid what main already does.
//
// THE ALTERNATIVE THAT WAS REJECTED was to make every context a fiber (a "degenerate fiber" for
// native tasks) so one table could hold them all. That inverts the cost: a native reader needs
// WORKER-many slots and a fiber-per-task gives it TASK-many, and fibers come from a fixed pool
// (`coreCount * StandardFibersPerWorker()`, 64 KB stacks from one reservation) that native tasks
// currently consume none of. A ParallelFor grain per fiber makes grain count bounded by pool size.
// The pure-fiber fork already found this the hard way: rejecting Native ABORTED ParallelFor, and
// the rule that came out of it was "waits are fibers, not every grain."
//
// So the fix goes the other way: the registry stops caring what a fiber is. What it indexes is a
// TOKEN, and a fiber merely has one.
//
// == EMBEDDED, NEVER ALLOCATED ==
//
// A token lives inside storage that already outlives the debt -- in Fiber (leaked and reserve()d),
// in Thread (lives for the pool), in a static array for external threads. The registry holds
// pointers to them. This is not a micro-optimisation: it keeps the DEATH PATH allocation-free,
// which is where an allocation is least welcome and most likely to be the thing that fails.
//
// == TWO FIELDS, TWO DIFFERENT QUESTIONS ==
//
// `next` is a SEQUENCE  -- the intrusive delivery chain. One holder is a chain of length one, so
//                          there is one code path and no branch for the common case.
// `creditors` is MEMBERSHIP -- which holders owe. Deliberately a bitmask and NOT a list, because a
//                          list gives up exactly the properties that matter here: nothing to
//                          allocate on the death path, dedup by construction (a holder that touches
//                          this token fifty times sets one bit; a list queues fifty jobs and needs
//                          its own dedup pass), and one fetch_or with no CAS loop on the
//                          debt-incurring path.
//
// Collapsing the two because "a single holder is just a list of one" is right for `next` and wrong
// for `creditors`. Kept apart on purpose.

#pragma once
#include "platform.h"
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace JLib {

	// WHO CAN BE A CREDITOR: only a THREAD can hold thread-affine state, so the creditor space is
	// workers + external threads -- NOT the whole reader space. A fiber cannot owe another fiber.
	// That is what keeps this mask small enough to sit in every token.
	//
	// The bound is checked against the scheduler's real worker ceiling in TokenRegistry.cpp, where
	// TaskScheduler.h is visible; it cannot be checked here without a cycle.
	static constexpr std::size_t kHolderWords = 6;              // 384 holders
	static constexpr std::size_t kMaxHolders  = kHolderWords * 64;

	// The kinds of thread-affine debt a token can carry. A SMALL FIXED ENUM, deliberately: the
	// payload of a cleanup message is (token, kinds), which is why the token can BE the message and
	// no Task is needed to carry a closure.
	enum OwedKind : std::uint32_t {
		kOwesNothing = 0,
		kOwesSlab    = 1,   // reserved; SlabPool::Free routes by ADDRESS, so this is not owed today
		kOwesEpoch   = 2,   // reserved; epoch garbage is not affine -- a dying reader splices instead
		kOwesHazard  = 4,   // REAL: HazardDomain's retire bag is `thread_local RetireBatch`
	};

	struct alignas(platform::kCacheLine) DebtToken {
		// ---- SEQUENCE: the intrusive delivery chain ----------------------------------------------
		//
		// THE TOKEN IS THE MESSAGE. Linking it onto a holder's inbound chain is one CAS and carries
		// everything the holder needs -- which token, and what it owes. The alternative that was
		// costed and dropped was a real Task per hop: a 64-byte slab allocation, a task lifecycle,
		// an inbox hop and a DestroyTask, all to carry two words.
		//
		// Owned by whichever chain this token is currently on, and null when it is on none.
		DebtToken* next = nullptr;

		// ---- MEMBERSHIP: which holders owe cleanup for this token --------------------------------
		std::atomic<std::uint64_t> creditors[kHolderWords] = {};
		std::atomic<std::uint32_t> owedKinds{ kOwesNothing };

		// Its own index in the reader space. kNoReader until registered.
		std::size_t readerId = ~std::size_t(0);

		// Record that `holder` now owes cleanup for this token. Idempotent BY CONSTRUCTION rather
		// than by checking -- setting a set bit is a no-op -- so a hot path that fires on every
		// affine allocation costs one atomic OR with no branch and no retry.
		void NoteCreditor(std::size_t holder) {
			// REFUSE, DO NOT WRAP. A wrapped index would silently bill the wrong holder, and the
			// symptom would be cleanup running on a thread that owes nothing while the thread that
			// does never hears about it.
			if (holder >= kMaxHolders) return;
			creditors[holder >> 6].fetch_or(std::uint64_t(1) << (holder & 63),
			                                std::memory_order_release);
		}

		bool HasCreditors() const {
			for (std::size_t w = 0; w < kHolderWords; ++w)
				if (creditors[w].load(std::memory_order_acquire)) return true;
			return false;
		}

		// Remove and return the lowest-numbered creditor, or kMaxHolders when the set is empty.
		//
		// THIS IS THE CHAIN STEP. Cleanup takes ONE creditor and delivers to it; that holder does
		// its work and takes the next; whoever finds the set empty recycles. The fan-out shape --
		// take them all, deliver N, then recycle -- reads as equivalent and is not: those messages
		// are QUEUED, not run, so the token would be recycled while they still reference it.
		std::size_t TakeCreditor() {
			for (std::size_t w = 0; w < kHolderWords; ++w) {
				std::uint64_t cur = creditors[w].load(std::memory_order_acquire);
				while (cur) {
					const unsigned b = platform::CountTrailingZeros64(cur);
					const std::uint64_t bit = std::uint64_t(1) << b;
					if (creditors[w].compare_exchange_weak(cur, cur & ~bit,
							std::memory_order_acq_rel, std::memory_order_acquire))
						return (w << 6) + b;
					// CAS failed: `cur` was reloaded with someone else's change. Re-examine this
					// word rather than moving on -- another bit in it may still be ours to take.
				}
			}
			return kMaxHolders;
		}

		void MarkOwed(std::uint32_t kinds) {
			owedKinds.fetch_or(kinds, std::memory_order_release);
		}
		std::uint32_t Owed() const { return owedKinds.load(std::memory_order_acquire); }

		// GATED ON WHAT IS OWED, NOT ON HAVING CREDITORS. Being picked up by a worker is not a debt.
		// The fiber version of this said HasCreditors() once, and because registration happens on
		// every pickup, every death then dispatched cleanup for nothing.
		bool OwesCleanup() const { return Owed() != kOwesNothing; }

		// A RECYCLED TOKEN IS A NEW TOKEN. readerId is deliberately NOT cleared: it describes the
		// SLOT, not the occupant, exactly like Fiber::poolIndex.
		void ResetForReuse() {
			next = nullptr;
			for (std::size_t w = 0; w < kHolderWords; ++w)
				creditors[w].store(0, std::memory_order_relaxed);
			owedKinds.store(kOwesNothing, std::memory_order_release);
		}
	};

}  // namespace JLib
