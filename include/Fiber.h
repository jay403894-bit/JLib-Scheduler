// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Context.h"
#include "TsanFiber.h"   // tsanFiber below; no-ops without the sanitizer
#include "platform.h"
#include "Task.h"
#include <atomic>
#include <cstdint>
namespace JLib {
	enum class FiberStatus {
		READY,         // In a work queue, waiting to be run/stolen
		RUNNING,       // Currently executing on a worker
		WANTS_YIELD,   // Fiber asked to yield; worker re-queues it AFTER its ctx is saved
		WANTS_SUSPEND, // Fiber asked to suspend; worker marks SUSPENDED after its ctx is saved
		SUSPEND_SIGNALED, // A signal/Resume raced in during WANTS_SUSPEND; worker wakes it instead of parking
		SUSPENDED,     // Parked, not queued; only now may Resume() make it READY + re-queue
		DEAD           // Finished, pending cleanup/reclamation
	};
	struct alignas(16) Fiber {
		Context ctx;
		uint64_t id;
		void* stackBase;
		size_t stackSize;
		// WHICH CLASS THIS FIBER BELONGS TO. Recorded rather than inferred from stackSize so
		// ReturnBatch can route it home in one load, and so a future class with a coincidentally
		// equal size cannot be misfiled.
		StackClass stackClass = StackClass::Standard;
		// DENSE, STABLE index into the global fiber pool: standard fibers occupy [0, standardCount),
		// heavy fibers follow. The pool is leaked and its vectors are reserve()d so they never
		// reallocate, so this is fixed for the life of the program.
		//
		// It is what makes Event's waiter index a PERFECT HASH rather than a hash table: a parked
		// task always holds a fiber, and a fiber can be parked on at most one event at a time
		// (parking is what the fiber is doing), so fiber index -> waiter slot has no collisions
		// by construction. See Event::AddWaiter.
		size_t poolIndex = SIZE_MAX;
		Task* owningTask = nullptr; // The task currently running on this fiber
		Context* homeCtx = nullptr; // Scheduler ctx to return to; the worker sets this before each switch-in
		// THE WORKER THIS FIBER IS PINNED TO. Unconditional since 4.0.2 (was Mode::FiberOnly). Set once, where the fiber is
		// bound to a task, and read by every resume path to decide where the fiber goes back.
		//
		// WHY PINNING EXISTS. Nothing about a fiber's saved context is thread-specific -- the switch
		// is a pure register save/restore and the stack travels with the fiber -- but everything
		// reached through `thread_local` IS. TLS follows the THREAD; a migrating fiber does not. So
		// a resumed fiber reads `thread_id`, `Thread::instance` and every per-thread cache belonging
		// to whichever worker happened to pick it up, and any value derived from those BEFORE the
		// suspend is silently wrong AFTER it. Pinning removes the class of bug rather than asking
		// every future call site to remember the rule.
		//
		// marl takes the same position by construction -- `switchToFiber()` is documented "the fiber
		// must belong to this worker", and its steal() moves TASKS, never fibers. What it gives up,
		// and what this gives up with it: a worker holding many blocked fibers cannot shed them, so
		// resumed work is not rebalanced. TASK stealing is untouched; a Fiber task that has not run
		// yet holds no fiber and stays freely stealable.
		//
		// SIZE_MAX means "not bound" -- a fiber sitting in a pool cache. It is not a valid target.
		size_t homeWorker = SIZE_MAX;

#if defined(JLIBSCHED_REQUEUE_TRACE)
		// WHERE Requeue SENT THIS FIBER on its last resume, stamped by the router and read back by
		// the fiber once it is running again. Diagnostic only, compiled out by default.
		//
		// IT EXISTS BECAUSE TWO SEPARATE INSTRUMENTS DISAGREED and neither could settle it: the
		// router reported 178 of 256 resumes routed AWAY from home, while the test reported every
		// fiber waking on the worker it left. Both counted honestly; they just never counted the
		// SAME task, so nothing could join them. This joins them -- one task, both ends.
		size_t lastPlacedOn = SIZE_MAX;
#endif

		// INTRUSIVE LINK for a WaitGroup's direct waiter stack. Owned by whichever
		// primitive this fiber is parked on, and null whenever it is not parked on one.
		//
		// WHY IT LIVES ON THE FIBER AND NOT THE TASK. Task is on a hard 64-byte budget; Fiber is
		// not. It is also the correct owner: a fiber is parked on at most one thing at a time --
		// parking is what the fiber is DOING -- so one link per fiber can never be contended between
		// two primitives. That is the same argument that makes Event's fiber-indexed waiter table a
		// perfect hash.
		//
		// AN EARLIER DESIGN THREADED THE WAITER LIST THROUGH Task AND WAS RETIRED FOR A REAL BUG:
		// the links WERE the tasks, so a task freed back to the slab while a list still held its
		// address meant the next drain walked a recycled slot. Fibers are never freed -- the global
		// pool reserves and leaks them -- so a link through a fiber cannot dangle that way.
		Fiber* nextWaiter = nullptr;

		// ---- THE CLEANUP CHAIN LINK -- SEPARATE FROM nextWaiter ON PURPOSE ----------------------
		//
		// THE FIBER IS THE MESSAGE. A dying fiber that owes a worker is linked onto that worker's
		// inbound chain with one CAS, carrying everything the worker needs -- which fiber, and what
		// it owes. The alternative, and what this replaced, was a real Task per hop: a 64-byte slab
		// allocation, a task lifecycle, an inbox hop and a DestroyTask, to carry two words.
		//
		// NOT REUSING nextWaiter, even though a dead fiber is provably not parked on a primitive.
		// That argument is true today and is exactly the kind that stops being true quietly: the
		// two links have different owners (a primitive owns one, the registry the other) and
        // different lifetimes, and sharing them would make "is this fiber parked or owing?" a
		// question the pointer cannot answer. Fiber is not on a size budget; Task is.
		Fiber* cleanupNext = nullptr;

		// TSan's handle for this fiber, or null in any build without the sanitizer. Made once with
		// the pool and never destroyed, because fibers are never destroyed -- the pool reserves and
		// leaks them. See TsanFiber.h for why an unannotated fiber scheduler produces meaningless
		// TSan output in BOTH directions.
		//
		// NOT CLEARED BY ResetForReuse: like poolIndex, it describes the SLOT, not the occupant. A
		// recycled fiber is the same fiber to TSan, which is correct -- the stack is the same stack.
		void* tsanFiber = nullptr;

		// ---- THE CREDITOR SET: every worker this fiber owes thread-affine cleanup to ------------
		//
		// A SET, NOT ONE HOME, AND THAT IS THE WHOLE POINT. `homeWorker` above can name one worker.
		// Under migration that is not enough: a fiber that allocates on A, migrates, and allocates
		// again on B owes BOTH, and no single index can say so. An earlier draft of this design
		// tried one `uint8_t home` and the second creditor is exactly what broke it.
		//
		// A SET, NOT A SEQUENCE. Cleanup order does not matter -- each worker releases its own
		// resources and no creditor's work depends on another's -- so this needs membership, not
		// ordering. Which makes a bitmask strictly better than the linked list of ids it replaces:
		//
		//   * NOTHING TO ALLOCATE on the death path, where an allocation is least welcome.
		//   * DEDUPLICATED BY CONSTRUCTION. A worker that touches this fiber fifty times sets one
		//     bit. A list would queue fifty cleanup jobs and need its own dedup pass to avoid it.
		//   * ONE `fetch_or` to record, no CAS loop, so the debt-incurring path stays cheap.
		//   * The iteration is CountTrailingZeros64, the same idiom the scheduler already uses for
		//     the awake bitmap.
		//
		// AND IT IS ON THE FIBER, NOT THE TASK, for the reason the nextWaiter comment above gives:
		// Task is on a hard 64-byte budget and Fiber is not. 32 bytes here is free; on Task it
		// would push every single-capture lambda out of the 64-byte slab class.
		//
		// PINNED MODE IS THIS SET WITH EXACTLY ONE MEMBER. It is not a second mechanism and not a
		// second code path -- the binding worker is added at bind, nothing else ever is, and the
		// cleanup chain degenerates to one hop. See TaskScheduler::MigratableFibers.
		// WIDE ENOUGH FOR HOLDERS, NOT JUST WORKERS. A creditor is any THREAD that can own
		// thread-affine state, and that is workers PLUS the external ids main and an app's own
		// threads claim. At 4 words this covered 256 workers and silently refused every external
		// holder above it -- NoteCreditor drops an out-of-range holder rather than wrapping, which
		// is right, but the debt is then dropped with it. See the static_assert in
		// FiberRegistry.cpp that ties this to kMaxHintQueues + kExternalReaders.
		static constexpr size_t kCreditorWords = 6;   // 384 holders
		std::atomic<uint64_t> creditors[kCreditorWords] = {};

		// Record that `worker` now owes cleanup for this fiber. Idempotent BY CONSTRUCTION rather
		// than by checking -- setting a set bit is a no-op, so the caller never has to ask whether
		// it already registered, and a hot path that fires on every affine allocation costs one
		// atomic OR with no branch and no retry.
		void NoteCreditor(size_t worker) {
			if (worker >= kCreditorWords * 64) return;   // refuse, do not wrap: a wrapped index
			                                             // would silently bill the wrong worker
			creditors[worker >> 6].fetch_or(1ull << (worker & 63), std::memory_order_release);
		}

		// Remove and return the lowest-numbered creditor, or SIZE_MAX when the set is empty.
		//
		// THIS IS THE CHAIN STEP. Cleanup pops ONE creditor and queues one job to it; that job does
		// its work and pops the next; whoever gets SIZE_MAX recycles the fiber. The fan-out shape --
		// pop them all, queue N jobs, then recycle -- looks equivalent and is not: the jobs would be
		// QUEUED, not run, so the fiber returns to the pool while cleanup still references it.
		// Priority changes when those jobs run; it does not change that ordering.
		size_t TakeCreditor() {
			for (size_t w = 0; w < kCreditorWords; ++w) {
				uint64_t cur = creditors[w].load(std::memory_order_acquire);
				while (cur) {
					const unsigned b = platform::CountTrailingZeros64(cur);
					// CAS rather than fetch_and: two threads may drain concurrently during a
					// teardown sweep, and both must not be handed the same creditor.
					if (creditors[w].compare_exchange_weak(cur, cur & (cur - 1),
							std::memory_order_acq_rel, std::memory_order_acquire))
						return w * 64 + b;
					// cur was reloaded by the failed CAS; re-examine it rather than restarting.
				}
			}
			return SIZE_MAX;
		}

		// ---- WHAT IS OWED, AS OPPOSED TO WHO IS OWED IT ----------------------------------------
		//
		// `creditors` records WHO ran this fiber; this records WHAT it incurred. They are separate
		// because they are set at different times by different parties: registration happens on
		// EVERY pickup (cheap, unconditional, one fetch_or), while a kind is set only when a
		// resource is actually acquired.
		//
		// AND THE SPLIT IS WHAT MAKES REGISTRATION AFFORDABLE. Without it, wiring registration made
		// every fiber death dispatch a cleanup task per worker it had run on -- to run an empty
		// routine, and to recycle through the GLOBAL pool instead of the thread-local cache. Real
		// work for nothing. With it, a fiber that never touched affine state has creditors, no
		// kinds, and takes the same path it always did.
		//
		// KINDS ARE THE THREE RECLAMATION SYSTEMS, and that is not a coincidence: they are exactly
		// the three costs the architecture header says migration already paid (address-routed slab
		// frees, a global epoch participant list, fiber-indexed hazard cells). Recording the debt is
		// what would let those go back to their cheap per-thread forms.
		enum OwedKind : uint32_t {
			kOwesNothing = 0,
			kOwesSlab    = 1u << 0,
			kOwesEpoch   = 1u << 1,
			kOwesHazard  = 1u << 2,
		};
		std::atomic<uint32_t> owedKinds{ kOwesNothing };

		void NoteOwed(uint32_t kinds) { owedKinds.fetch_or(kinds, std::memory_order_release); }
		uint32_t Owed() const { return owedKinds.load(std::memory_order_acquire); }

		// THE GATE ON THE WHOLE CLEANUP CHAIN. Creditors alone are not a reason to run it -- being
		// picked up is not a debt.
		bool OwesCleanup() const { return Owed() != kOwesNothing; }

		bool HasCreditors() const {
			for (size_t w = 0; w < kCreditorWords; ++w)
				if (creditors[w].load(std::memory_order_acquire)) return true;
			return false;
		}

		// Drop every creditor without running cleanup. FOR RECYCLE ONLY, and only once the chain has
		// drained -- a fiber returning to the pool is a NEW fiber and must owe nobody. Calling this
		// with debts outstanding does not lose a task, it loses a RELEASE: the COM apartment or
		// handle those creditors were holding is never given back, and nothing reports it.
		void ClearCreditors() {
			for (size_t w = 0; w < kCreditorWords; ++w)
				creditors[w].store(0, std::memory_order_release);
		}

		// ---- EVERYTHING A RECYCLED FIBER MUST NOT CARRY, IN ONE PLACE --------------------------
		//
		// A recycled fiber IS a new fiber. Anything left over from its last life is state the next
		// task inherits without asking for it.
		//
		// ONE FUNCTION, NEXT TO THE FIELDS, RATHER THAN SCRUBS AT THE RETURN SITE. GlobalFiberPool::
		// ReturnBatch used to clear localEpoch inline and nothing else -- and that line exists
		// BECAUSE a fiber once went back to the pool still announced at an old epoch, which is an
		// ABA on the reclaimer. A per-field list at the call site is correct exactly until someone
		// adds a field, and the person adding the field is not looking at the return path. Here,
		// adding a member and forgetting to reset it means the two are adjacent on screen.
		//
		// WHAT IS DELIBERATELY *NOT* RESET: poolIndex (dense, stable, and the identity every table
		// in the system is keyed by -- see Event's perfect hash), id, stackBase, stackSize, ctx and
		// the arena pointers. Those describe the SLOT, not the occupant, and clearing them would
		// unmake the fiber rather than free it.
		void ResetForReuse() {
			ClearCreditors();
			// AND THE KINDS WITH THEM. A recycled fiber carrying a stale kind would send its next
			// occupant's death down the cleanup chain to release something it never acquired. This
			// is the exact field the "one reset list, next to the members" note below exists for --
			// it was added after that rule, and the rule is why it is here rather than forgotten.
			owedKinds.store(kOwesNothing, std::memory_order_release);
			// SIZE_MAX is "not in an epoch". The original inline scrub, kept for its original
			// reason: a slot still announced pins the reclaimer at a dead epoch.
			localEpoch.store(SIZE_MAX, std::memory_order_release);
			// SIZE_MAX is "not bound". A stale value here names a worker the next occupant never
			// ran on, which under pinning is a resume aimed at the wrong thread.
			homeWorker = SIZE_MAX;
			owningTask = nullptr;
			homeCtx    = nullptr;
			// Owned by whichever primitive the fiber was parked on. A survivor means the next
			// occupant appears to be queued on a wait list it never joined.
			nextWaiter = nullptr;
			status.store(FiberStatus::READY, std::memory_order_release);
		}

		std::atomic<FiberStatus>  status;
		// EBR participation slot. SIZE_MAX == "not in an epoch". The fiber is the unit
		// that migrates across workers, so the slot lives here (not on the thread).
		// Default member init covers the move ctor too (a moved/fresh fiber is not in an
		// epoch), so the move ctor doesn't need to mention it.
		std::atomic<size_t> localEpoch{ SIZE_MAX };
		static std::atomic<uint64_t> idGenerator;
		void (*taskFunction)();
		Fiber() : stackBase(nullptr), stackSize(0), taskFunction(nullptr), status(FiberStatus::READY), id(idGenerator.fetch_add(1, std::memory_order_relaxed)) {

		}
		Fiber(Fiber&& other) noexcept
			: ctx(other.ctx), stackBase(other.stackBase), stackSize(other.stackSize),
			  taskFunction(other.taskFunction), status(other.status.load(std::memory_order_relaxed)), id(idGenerator.fetch_add(1, std::memory_order_relaxed)) {}
		Fiber& operator=(Fiber&&) = delete;
		Fiber(const Fiber&) = delete;
		Fiber& operator=(const Fiber&) = delete;
		void Init(void (*entryPoint)());
		void CoYield();    // Swaps back to scheduler                            
		void Suspend();  // Moves from RUNNING -> SUSPENDED
		// The CAS half of Resume, WITHOUT the re-queue. Returns true when this call is the one that
		// moved SUSPENDED -> READY, meaning the CALLER now owns re-queueing owningTask. Lets a waker
		// with many fibers to wake (Event::SignalAll) collect them and submit one batch instead of
		// paying a placement, an inbox push and a condition-variable signal per fiber.
		bool ResumeQueueless();
		void Resume();   // Moves from SUSPENDED -> READY

		// Safety check for the work-stealer
		bool IsReady() const { return status == FiberStatus::READY; }
	};
} // namespace JLib

namespace JLib {
	// Re-queue fibers already transitioned to READY by ResumeQueueless. Lives here rather than in
	// Event.h because Event.h only knows Fiber.h -- reaching TaskScheduler from a header it already
	// includes would be circular.
	void RequeueResumedBatch(Task** tasks, size_t n, Lane lane);
}
