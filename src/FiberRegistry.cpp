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
#include "../include/Epochs.h"    // EpochManager::Tick -- the reclaim task's body
#include "../include/Hazard.h"    // HazardDomain::Scan -- the other half of it

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
			if (ReleaseFn r = releaseFn.load(std::memory_order_relaxed)) r(holder, f);
			AdvanceCleanup(f);
			++n;
			f = nxt;
		}
		return n;
	}

	size_t FiberRegistry::DrainAllForTeardown() {
		// ---- SWEEP UNTIL QUIESCENT. DEFENCE IN DEPTH, NOT A BUG FIX. ---------------------------
		//
		// A single forward pass over [0, size) is CORRECT TODAY, and only for a reason that lives in
		// another file: TakeCreditor pops the LOWEST set bit first, so every hop hands the fiber to
		// a HIGHER worker index than the one it is on, and a forward sweep can never be handed work
		// behind itself.
		//
		// That coupling is invisible from here. Change TakeCreditor to pop the highest bit, or to
		// rotate its start for fairness across creditors, and this becomes a silent leak: draining
		// holder 9 hands the fiber to holder 1, which this sweep already walked past, so its chain
		// is never drained again -- the fiber is not recycled, the resource is never given back, and
		// every local step still looks correct.
		//
		// One extra pass at shutdown buys independence from an ordering guarantee nothing states or
		// tests. That is worth it HERE specifically because this is the last look anyone takes: the
		// steady-state drain is forward-only and stays that way, since its holder is a live worker
		// that will look at its own chain again next pass.
		//
		// BOUNDED, because the chain is finite by construction and a runaway here would hang
		// shutdown rather than leak. Each fiber has at most kCreditorWords*64 creditors and every
		// hop CLEARS one bit before dispatching, so the number of hops a fiber can still take is
		// strictly decreasing -- one sweep per possible creditor is a ceiling nothing can reach.
		// Stopping early on a clean sweep is what makes the common case one pass, not 384.
		size_t n = 0;
		for (size_t sweep = 0; sweep < Fiber::kCreditorWords * 64; ++sweep) {
			size_t moved = 0;
			for (size_t h = 0; h < inbound.size(); ++h) moved += DrainHolder(h);
			if (moved == 0) break;      // nothing left anywhere: done
			n += moved;
		}
		return n;
	}

	void FiberRegistry::SetRelease(ReleaseFn fn) { releaseFn.store(fn, std::memory_order_relaxed); }

	// ---- CLEARING FIBER-LOCAL SLOTS --------------------------------------------------------------
	//
	// A SLOT-DELETER LEDGER WAS REMOVED HERE. `SetSlotDeleter(slot, fn)` made a slot OWNING, and
	// this function freed those values when a fiber was recycled -- a per-slot deleter table, a
	// fast-path mask, and a publication order between them.
	//
	// It was the second user-deletion ledger in the library (the other was
	// TaskScheduler::ReleaseOnFiberDeath, removed with it), and neither earned its keep. Both fired
	// at the same moment, and that moment is one where freeing was ALREADY safe: the fiber is dead,
	// nothing else can reach its slot. If something else COULD still reach the value, a fiber's
	// death is not what makes the free safe -- epochs and hazards are, and both already exist. So
	// the ledger either replaced a free the caller could have written inline, or stood in for a
	// reclamation scheme it was not.
	//
	// WHAT SURVIVES IS THE CLEAR. Slots are borrowed pointers now, in every case: whoever put a
	// value in one owns it, and this zeroes the array so a recycled fiber never observes the
	// previous occupant's value. No table, no mask, no atomics on the recycle path.
	//
	// File scope rather than a member, because this is reached from Fiber::ResetForReuse -- which
	// has no registry pointer and must not acquire one. Instance() there would be a singleton touch
	// on the recycle path of every fiber, and would order Fiber's teardown behind the registry's.
	namespace detail {
		void ReleaseFiberSlots(void** slots, size_t n) noexcept {
			if (!slots) return;
			for (size_t i = 0; i < n && i < Fiber::kLocalSlots; ++i) slots[i] = nullptr;
		}
	}

	// EVERY FIBER DEATH IS A RECLAIM OPPORTUNITY, and this is the one path both routes converge
	// on -- the creditor chain ends here and so does the no-debt fast path. Rate-limited by the CAS
	// inside, so this costs one relaxed load when a sweep is already pending, which under any burst
	// is almost every call.
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
		QueueReclaim();
		pool->ReturnBatch(&f, 1);
		return true;
	}

	void FiberRegistry::SetDispatch(DispatchFn fn) { dispatchFn.store(fn, std::memory_order_relaxed); }
	void FiberRegistry::SetRecycle(RecycleFn fn)   { recycleFn.store(fn, std::memory_order_relaxed); }

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

	// ONE LOAD, USED ONCE. Reading the atomic twice -- test-then-call -- would let a seam be
	// installed between the two, so the call would go to a pointer the test never checked.
	// Defined here rather than with the other members because it names the two defaults above.
	FiberRegistry::DispatchFn FiberRegistry::Dispatcher() {
		DispatchFn d = dispatchFn.load(std::memory_order_relaxed);
		return d ? d : &DefaultDispatch;
	}
	FiberRegistry::RecycleFn FiberRegistry::Recycler() {
		RecycleFn r = recycleFn.load(std::memory_order_relaxed);
		return r ? r : &DefaultRecycle;
	}

	// ---- THE REAPER SENDS A TASK. IT DOES NOT SWEEP, AND IT DOES NOT ASK THE APP. --------------
	//
	// THREE DESIGNS, AND THE FIRST TWO WERE BOTH WRONG:
	//
	//   WORKER-INLINE   whichever worker crossed the threshold stopped and swept. That is a p99
	//                   killer: it walks every epoch participant on a thread that was supposed to be
	//                   running a frame, at a moment nobody chose. Measured 331 us p99 against
	//                   113 us without, with p50 and p90 unchanged.
	//   APP-DRIVEN      the embedder calls Tick() at a frame boundary. Better tail, but it makes a
	//                   LIBRARY depend on a loop its embedder may not have, and forgetting leaks
	//                   silently in release. Not an obligation this can place.
	//   A TASK          this. The sweep is queued like any other work, so it displaces nothing, and
	//                   it goes out as Lane::Normal so it can never land on the reserved band.
	//
	// The difference from worker-inline is not WHO runs it -- a task runs on a worker too -- it is
	// WHEN. Inline, a worker abandons its pass mid-flight; queued, the sweep waits its turn behind
	// work that was already there.
	//
	// HERE RATHER THAN IN Epochs.h, because this is the reaper and reclamation policy is its job.
	// Epochs.h stays a data structure that knows nothing about scheduling, which is also what keeps
	// it includable from Task.h without a cycle.
	//
	// ONE SWEEP IN FLIGHT, enforced by the CAS. Fiber deaths come in bursts; without it a burst
	// queues one sweep per death, each walking every participant to free what the first already
	// freed. The task clears the flag on its way out, so the next death after a sweep completes may
	// queue another.
	// ---- FIBER-LOCAL SLOT ALLOCATION ---------------------------------------------------------
	//
	// ONE-SHOT AND NEVER RELEASED, which is a decision rather than an omission. Releasing a slot
	// while any fiber still holds a value in it would hand that value to whoever allocated the same
	// index next -- a use-after-free with the shape of a working program, since the pointer is live
	// and the type is wrong. Slots are cheap (kLocalSlots of them, allocated at startup) and the
	// pattern they exist for is a fixed set of well-known values, so nothing needs to give one back.
	static std::atomic<uint16_t> g_nextFlsSlot{ 0 };

	uint16_t FiberRegistry::FlsAlloc() noexcept {
		const uint16_t s = g_nextFlsSlot.fetch_add(1, std::memory_order_relaxed);
		// CHECKED, NOT ASSERTED. A library that runs out of slots should degrade -- every get()
		// returns null and the caller's own null handling takes over -- rather than take down a
		// process for a resource it could have done without.
		if (s >= (uint16_t)Fiber::kLocalSlots) return kNoSlot;
		return s;
	}

	void* FiberRegistry::FlsGet(uint16_t slot) noexcept {
		if (slot >= (uint16_t)Fiber::kLocalSlots) return nullptr;   // kNoSlot included
		Thread* t = Thread::GetCurrent();
		Fiber*  f = t ? t->currentFiber : nullptr;
		// NULL OFF A FIBER. A Native task and a bare thread have none, which is a legitimate state
		// for library code that may run in either context -- so it answers rather than asserting.
		return f ? f->local[slot] : nullptr;
	}

	void FiberRegistry::FlsSet(uint16_t slot, void* p) noexcept {
		if (slot >= (uint16_t)Fiber::kLocalSlots) return;
		Thread* t = Thread::GetCurrent();
		if (Fiber* f = (t ? t->currentFiber : nullptr)) f->local[slot] = p;
		// SILENTLY DROPPED off a fiber, and that asymmetry with get() is deliberate: a get can
		// report "nothing here" honestly, but there is nowhere to PUT a value, and the caller
		// storing one has already decided it is running on a fiber. Making this loud would fire on
		// legitimate library code that stores opportunistically.
	}

	size_t FiberRegistry::GetID(const Fiber* f) noexcept {
		return f ? f->poolIndex : SIZE_MAX;
	}

	size_t FiberRegistry::GetID() noexcept {
		Thread* t = Thread::GetCurrent();
		return GetID(t ? t->currentFiber : nullptr);
	}

	static std::atomic<bool> g_reclaimQueued{ false };

	// ---- THE THIRD RECLAMATION DOMAIN: user memory owed by dead fibers ------------------------
	//
	// A Treiber stack, many producers (any thread recycling a fiber), one consumer (the reclaim
	// task). Spliced onto here by Fiber::ResetForReuse rather than released there, for the same
	// reason the sweeps are not done there: a recycle is a worker finishing a fiber and about to
	// look for work, and running arbitrary user deleters at that moment is unbounded work of the
	// caller's choosing on a thread that was doing something.
	//
	// LEAKED-BY-DESIGN IS NOT NEEDED HERE, unlike the epoch and hazard orphan stores: this is a
	// plain atomic with constant initialisation and no destructor, so it stays readable however
	// late a thread recycles a fiber during teardown.
	static std::atomic<FiberDebt*> g_pendingDebts{ nullptr };

	namespace detail {
		void HandOffFiberDebts(FiberDebt* head) noexcept {
			if (!head) return;
			// FIND THE TAIL, then splice the whole chain in one CAS. Pushing node-by-node would be
			// N CAS operations on a line every recycling thread shares; the walk is local to a list
			// this thread exclusively owns and is typically one or two nodes.
			FiberDebt* tail = head;
			while (tail->next) tail = tail->next;

			FiberDebt* old = g_pendingDebts.load(std::memory_order_relaxed);
			do {
				tail->next = old;
			} while (!g_pendingDebts.compare_exchange_weak(old, head,
						std::memory_order_release, std::memory_order_relaxed));

			// QUEUE THE SWEEP HERE, because this is where the need actually arises.
			//
			// Triggering only from ReturnToPool was not enough and the debt test caught it at zero
			// released: that is the REGISTRY's return path, while the common one is the worker's
			// own ReleaseFiber into its local cache. Most recycles never touch ReturnToPool, so
			// most handed-off debts would have waited for a fiber that happened to die the other
			// way -- which in a pool doing no cleanup work is never.
			//
			// Tying it to the handoff means the trigger and the garbage are the same event.
			FiberRegistry::QueueReclaim();
		}
	}

	// Drain and discharge. Returns how many ran, for the caller's diagnostics.
	static size_t ReleasePendingDebts() {
		// ONE EXCHANGE TAKES THE WHOLE STACK, so a producer racing this pushes onto a fresh chain
		// and is swept by the next pass rather than contending for every node.
		FiberDebt* d = g_pendingDebts.exchange(nullptr, std::memory_order_acq_rel);
		size_t n = 0;
		while (d) {
			// READ next BEFORE releasing. The node usually lives INSIDE the object being released,
			// so the deleter is free to destroy it -- reading the link afterwards is a
			// use-after-free, and an intrusive list is where that mistake is easiest to make.
			FiberDebt* nxt = d->next;
			d->next = nullptr;
			if (d->release && d->obj) d->release(d->obj);
			++n;
			d = nxt;
		}
		return n;
	}

	void FiberRegistry::QueueReclaim() {
		bool expected = false;
		if (!g_reclaimQueued.compare_exchange_strong(expected, true,
				std::memory_order_acq_rel, std::memory_order_relaxed))
			return;                                   // one is already pending

		TaskScheduler* s = TaskScheduler::IsInitialized() ? &TaskScheduler::Instance() : nullptr;
		// NO POOL IS A LEGITIMATE STATE, not an error -- a fiber can die during teardown, and a
		// retire can happen before Init. Release the flag so a later death tries again.
		if (!s) { g_reclaimQueued.store(false, std::memory_order_release); return; }

		// ---- ONE PASS, ALL THREE DOMAINS. THIS IS THE SMR SCHEME. ----------------------------
		//
		// Safe Memory Reclamation here is not one mechanism but three, and they are discharged
		// together on purpose:
		//
		//   EPOCHS    Tick() -- advance, then free what no reader can still reach
		//   HAZARDS   Scan() -- free what no reader has named
		//   DEBTS     user memory a dead fiber owed, through the deleter its owner registered
		//
		// ORDER MATTERS, and this is the reason the debts go LAST. A user deleter may itself retire
		// something -- freeing a node whose children are epoch-protected is the ordinary case -- and
		// running the deletes before the sweeps would leave those retirements for the NEXT pass.
		// Running them after means the retirement is already in a bag when the following pass
		// sweeps, which is one pass of latency rather than two.
		//
		// It also means a single reclaim task is the whole story: an app watching for leaks has one
		// thing to look at, not three that fire on different triggers.
		Task* t = s->CreateInternalTask([] {
			EpochManager::Instance().Tick();
			HazardDomain::Instance().Scan();
			ReleasePendingDebts();
			// CLEARED LAST, after all three: clearing earlier would let a second sweep queue while
			// this one is still walking, which is the burst the flag exists to collapse.
			//
			// SEQ_CST, PAIRED WITH THE LOAD BELOW, and both are load-bearing -- see the re-check.
			g_reclaimQueued.store(false, std::memory_order_seq_cst);

			// ---- THE LAST FIBER'S DEBTS HAVE NO SECOND TRIGGER ---------------------------
			//
			// ReleasePendingDebts says a racing producer "is swept by the next pass", and that is
			// true exactly when a next pass happens. FIBER DEATH IS THE ONLY TRIGGER, so the final
			// death in a run has no successor, and this is a lost wake in the classic shape:
			//
			//   1. this task drains the stack with its exchange(nullptr)
			//   2. the last dying fiber pushes its debts
			//   3. it calls QueueReclaim, finds the flag STILL SET, and queues nothing
			//   4. we clear the flag -- and nothing will ever set it again
			//
			// Its debts are then never released. MEASURED at 199 of 200 fibers released, once in
			// roughly eight runs of fiber_debt_test; rare because the window is one exchange wide,
			// and permanent when it lands.
			//
			// THE RE-CHECK CLOSES IT, and the ordering is the whole fix. Producer does
			// push-then-CAS; we do clear-then-load. With both sides sequentially consistent every
			// interleaving is covered: a producer that pushed before our drain was swept; one that
			// pushed after the drain but before the clear fails its CAS and is caught by the load
			// here; one that pushes after the clear wins its CAS and queues its own sweep. Release
			// on the store alone is NOT enough -- StoreLoad is the one reordering acquire/release
			// does not forbid, so the load could be hoisted above the clear and see an empty stack
			// that the producer is about to fill.
			if (g_pendingDebts.load(std::memory_order_seq_cst) != nullptr)
				QueueReclaim();
		}, Lane::Normal);

		// ALLOCATION CAN FAIL ON A DEATH PATH, which is exactly where it is least welcome. Dropping
		// the sweep is safe -- the bag simply stays full and the next fiber death queues another --
		// so this releases the flag rather than leaving it latched forever.
		if (!t) { g_reclaimQueued.store(false, std::memory_order_release); return; }

		// ---- A DROPPED PUSH MUST NOT LATCH THE FLAG FOREVER --------------------------------
		//
		// Push's bool is checked rather than discarded, and this is the caller that most needs to:
		// the flag says "a sweep is coming", so believing a push that did not land stops
		// reclamation for the REST OF THE RUN -- the same permanent-loss shape as the lost wake
		// below, reached a different way.
		//
		// RELEASING THE FLAG IS THE RETRY. There is no useful immediate retry here: this runs on a
		// fiber death path where the reason a push failed would still be true a nanosecond later.
		// Clearing the flag hands the retry to the next fiber death, which is the trigger the whole
		// scheme is built on, and the debts stay on the stack meanwhile rather than being lost.
		//
		// TODAY THIS IS DEFENSIVE, and deliberately so. PushTarget returns false only for a null
		// task -- the inboxes are intrusive Vyukov MPSC queues, unbounded, whose push() cannot fail
		// -- so the branch is currently unreachable through a non-null task. It costs one predicted
		// branch on a cold path, and it means the day Push acquires a real failure mode (a bounded
		// queue, a refusal during teardown) this essential retries instead of silently stopping.
		if (!s->Push(t)) {
			g_reclaimQueued.store(false, std::memory_order_release);
			return;
		}
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
				Dispatcher()(h, f);
				any = true;
			}
			Recycler()(f);
			return any;
		}
#endif
		const size_t worker = f->TakeCreditor();
		if (worker == SIZE_MAX) {
			// NOBODY IS OWED. The chain ends here, and whoever is running this hop is the last
			// worker that owed anything -- which is what makes a completion count unnecessary.
			Recycler()(f);
			return false;
		}

		if (Dispatcher()(worker, f))
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
