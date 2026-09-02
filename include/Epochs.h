// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
#include <cstdio>    // fprintf -- the debug-only EpochGuard suspend tripwire below
#include <cassert>   // assert   -- ditto
#include <cstdlib>   // _dupenv_s / getenv / free -- the JLIB_FORCE_COUNTED diagnostic
#include "Task.h"
#include "platform.h"   // kCacheLine, for the counted-epoch ring below
#include "concurrentqueue.h"

namespace JLib {
	struct EpochParticipant {
		std::atomic<size_t> localEpoch{ SIZE_MAX };
	};
	using DeleterFunc = void(*)(void*);
	struct LNodeBase;
	struct LMarkableReference;
	struct SNMarkableReference;
	struct SNodeBase;
	struct DelayedTask;
	struct PeriodicTask;
	extern thread_local size_t thread_id;
	inline std::atomic<size_t>  thread_counter;

	// ---- PER-THREAD RETIRE BAGS -----------------------------------------------------------------
	//
	// Retiring is a POINT OPERATION that cannot suspend, so a thread-affine bag is sound where a
	// thread-affine PROTECT cell would not be -- the same argument HazardDomain writes down for its
	// own bag, and this shape is copied from there rather than invented.
	//
	// ONLY THE OWNER PUSHES AND ONLY THE OWNER DRAINS, so the vector needs no synchronisation at all.
	// That is the entire win: retire was a moodycamel enqueue and is now a push_back.
	//
	// The garbage itself is NOT thread-affine -- a retired pointer's deleter is `delete` or a slab
	// free, and SlabPool::Free routes by ADDRESS. The bag is affine because the CONTAINER is, which
	// is why thread exit needs a handoff and nothing else does.
	namespace detail {
		struct EpochRetired {
			void*  ptr;
			size_t epoch;
			void (*deleter)(void*);
		};

		// LEAKED ON PURPOSE. A thread_local destructor may run during process teardown, and a
		// function-local static with a destructor could already be gone by then -- handing entries
		// to a destroyed vector is a worse bug than the one being fixed. Same choice, same reason, as
		// HazardDomain's orphan store and the registries.
		struct EpochOrphanStore {
			std::mutex mtx;
			std::vector<EpochRetired> items;
		};
		inline EpochOrphanStore* const g_epochOrphans = new EpochOrphanStore();

		// THE GATE, and why TryReclaim does not pay a mutex. Reclaim runs on a threshold and on every
		// Tick, so it is warm; the orphan sweep is for a case that never happens in a healthy run.
		// One relaxed load of a line that stays 0 keeps the cost at zero until something is stranded.
		inline std::atomic<size_t> g_epochOrphanCount{ 0 };

		struct EpochRetireBatch {
			std::vector<EpochRetired> items;
			~EpochRetireBatch();
		};
		inline thread_local EpochRetireBatch t_epochBag;

		// SET BY ~EpochRetireBatch, and it must OUTLIVE the batch. A bool with constant
		// initialization has no destructor, so it is still readable while other thread_locals are
		// being destroyed -- and one of those may itself retire, which would otherwise touch a dead
		// vector. After the flag is up, retire goes straight to the orphan store.
		inline thread_local bool t_epochBagDead = false;

		inline EpochRetireBatch& EpochBag() { return t_epochBag; }

		inline void OrphanEpochEntries(std::vector<EpochRetired>& v) {
			if (v.empty()) return;
			std::lock_guard<std::mutex> lk(g_epochOrphans->mtx);
			for (auto& e : v) g_epochOrphans->items.push_back(e);
			g_epochOrphanCount.store(g_epochOrphans->items.size(), std::memory_order_release);
			v.clear();
		}

		// A BAG ABANDONED AT THREAD EXIT IS NOT DELAYED RECLAMATION, IT IS A LEAK -- the deleter
		// simply never runs. Handing the leftovers to a global store any later reclaim sweeps is
		// what makes a short-lived thread safe to retire from.
		inline EpochRetireBatch::~EpochRetireBatch() {
			t_epochBagDead = true;
			OrphanEpochEntries(items);
		}

		// Returns how many were freed. Takes the mutex, which is why it is gated.
		inline size_t SweepEpochOrphans(size_t safeEpoch) {
			std::lock_guard<std::mutex> lk(g_epochOrphans->mtx);
			auto& v = g_epochOrphans->items;
			size_t kept = 0, freed = 0;
			for (size_t i = 0; i < v.size(); ++i) {
				if (v[i].epoch < safeEpoch) { v[i].deleter(v[i].ptr); ++freed; }
				else                        { v[kept++] = v[i]; }
			}
			v.resize(kept);
			g_epochOrphanCount.store(v.size(), std::memory_order_release);
			return freed;
		}
	}

	class EpochManager {
	private:
		std::vector<std::atomic<size_t>*> participants;
	//	std::mutex participantMutex;
		struct GlobalRetired {
			void* ptr;        // node/arena pointer to free once safe
			size_t epoch;     // epoch at which it was retired
			void (*deleter)(void*);
		};
		// ---- THE GLOBAL RETIRE QUEUE IS GONE. THREE STRUCTURES BECAME ONE. ----------------------
		//
		// It was: a moodycamel MPMC `incoming` that every retire enqueued into, a `pending` staging
		// vector holding drained-but-not-yet-safe entries, and a `reclaiming` CAS so exactly one
		// thread could own `pending`.
		//
		// THE GATE EXISTED **BECAUSE** `pending` WAS SHARED. One global staging vector means exactly
		// one thread may be draining into it. Per-thread bags give every reclaimer a private slice,
		// so there is nothing left to arbitrate -- the gate is not optimised away, it is
		// unnecessary. All three go together; it is one shape, not three fixes.
		//
		// AND THE RETIRE PATH LOSES ITS ATOMIC ENTIRELY. It was a moodycamel implicit-producer
		// enqueue -- a thread-hash lookup, possibly a block allocation, and a 24-byte copy. It is
		// now a push_back into a vector only this thread ever touches. The carry-over of entries
		// that are not yet safe IS `pending`, except private instead of contended.
		//
		// See EpochRetireBatch below. The shape is copied from HazardDomain's thread_local
		// RetireBatch, orphan store and all, rather than invented -- that one already handles thread
		// exit correctly and has been in service.
		std::atomic<size_t> retiredCount{ 0 };     // approx live retired count; DIAGNOSTIC ONLY now

		// Bookkeeping for the "disabled and never ticked" warning in RetirePtr. Only the CODE that
		// touches these is conditional; the MEMBERS are unconditional and deliberately so.
		//
		// EpochManager is header-only, so an #if around a member makes sizeof() depend on the
		// including translation unit's build flags -- a Release library with a Development consumer
		// would then disagree about this class's layout. That is precisely the failure this file's
		// own stale-library guard exists to catch, and manufacturing more of it to save 16 bytes in
		// a singleton would be a bad trade. Unused in Release; costs nothing there.
		std::atomic<size_t> devRetiredNeverSwept{ 0 };
		std::atomic<bool>   devNoTickWarned{ false };

		std::atomic<size_t> globalEpoch{ 0 };

		struct ThreadEpoch {
			std::atomic<size_t> localEpoch{ SIZE_MAX };   // SIZE_MAX == not in an epoch
		};
		std::vector<ThreadEpoch*> threadEpochs;

		// ---- THE COUNTED-EPOCH RING WAS HERE, AND IS GONE ---------------------------------------
		//
		// It existed for ONE caller: a coroutine, which has no epoch slot of its own. The counter
		// was sharded so enter and leave need not use the same shard, which is what let a guard
		// survive a coroutine migrating mid-traversal.
		//
		// REMOVED BECAUSE IT INSURED AGAINST A RULE THE CODE ALREADY FORBIDS. Suspending inside an
		// EpochGuard is an invariant violation for coroutines exactly as it is for fibers -- see
		// CoroEpochGuardSuspendCheck, which asserts on it. The ring was the net under a rule that is
		// checked, and it was not free:
		//
		//   * EVERY guard paid a branch plus OnCoroutineTask() -- a TLS read, a null check and a
		//     task-type load -- to decide which mechanism to use. On the path measured at 2.52
		//     BILLION guards/sec, that is the whole cost.
		//   * MinActiveEpoch summed 8 ring slots x 32 shards = 256 seq_cst loads per reclaim.
		//   * AdvanceEpoch could REFUSE, because the gate would not enter an occupied slot. Slot
		//     readers never needed that gate: they announce their epoch and the min over them is
		//     exact. So advancement is now an unconditional CAS and reclamation cannot stall on it.
		//
		// WHAT PAYS FOR IT: the coroutine suspend check moved OUT of the dev-only block into
		// Release. The rare event (a co_await) carries the cost instead of the hot one (a guard),
		// and it now fires in the configuration where the bug would actually ship. Coroutines that
		// genuinely need reclamation across a suspend use HAZARD POINTERS, which support it by
		// design -- that is why both schemes exist.
		//
		// The 61x figure that justified sharding was about counted-vs-counted (unsharded 40.9M
		// guards/sec against slots' 2.52 billion). It was never an argument for having the ring.

		EpochManager() = default;
	public:
		EpochManager(const EpochManager&) = delete;
		EpochManager& operator=(const EpochManager&) = delete;
		~EpochManager() {
			for (auto* te : threadEpochs) {
				delete te;
			}
			threadEpochs.clear();
		}
		// Simply pass the address of the member that already exists in your Task
		// For the mechanism benchmark: how many slots MinActiveEpoch has to walk.
		int ParticipantCount() const { return (int)participants.size(); }

		void RegisterParticipant(std::atomic<size_t>* slot) {
		//	std::lock_guard<std::mutex> lock(participantMutex);
			participants.push_back(slot);
		}

		
		static EpochManager& Instance() {
			// Intentionally leaked (never destructed). Worker threads can outlive main
			// in this design (the scheduler instance is heap-allocated and not deleted),
			// so a Meyers-singleton destructor would run at static teardown while a
			// worker still calls Enter/LeaveEpoch -> threadEpochs[tid] freed -> read AV.
			// Leaking the manager lets the OS reclaim it at process exit instead.
			//
			// THE JLIB_FORCE_COUNTED ENV READ WAS HERE and went with counted epochs. It existed to
			// route every reader through the counted path for the mechanism comparison; with one
			// mechanism there is nothing to compare against and nothing to force.
			static EpochManager* mgr = new EpochManager();
			return *mgr;
		}
		void Tick()
		{
			AdvanceEpoch();
			TryReclaim();
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			// Reset the dev warning counter. It measures retirements SINCE THE LAST TICK, not
			// cumulative -- a cumulative count would fire on CORRECT usage (ticking every frame)
			// as soon as enough frames had gone by, and a warning that fires when you did the right
			// thing is worse than no warning at all.
			devRetiredNeverSwept.store(0, std::memory_order_relaxed);
#endif
		}
	
		void Init(size_t maxThreads)
		{
			threadEpochs.resize(maxThreads);
			for (size_t i = 0; i < maxThreads; i++) {
				threadEpochs[i] = new ThreadEpoch();
				// Register the bare-thread fallback slots (used by non-fiber callers,
				// e.g. the main thread building a DAG). Fiber slots are registered in
				// GlobalFiberPool. MinActiveEpoch scans the union of both.
				RegisterParticipant(&threadEpochs[i]->localEpoch);
			}
		}
		// Fallback epoch slot for a bare thread (one not running on a fiber), by thread_id.
		std::atomic<size_t>* ThreadSlot(size_t tid) {
			// BOUNDS TRIPWIRE. This had no check, and the failure mode was terrible: an out-of-range
			// tid indexes past the vector, returns whatever garbage pointer was there, and the
			// EpochGuard constructor writes through it -- surfacing as an access violation inside
			// LockFreeList::add with nothing pointing at the real cause (the thread_id).
			//
			// tid comes from the thread_local `thread_id`, which is assigned in exactly two places:
			// Thread::StartWorker for each worker, and the tail of StartPool for the MAIN thread.
			// The vector is sized num_workers + 1 for precisely that reason. A tid past the end
			// therefore means a thread that took an id without a slot being reserved for it, or a
			// thread that never got one and is still carrying the default 0 while ALIASING worker 0.
			// Both are real bugs, and both are silent today.
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			if (tid >= threadEpochs.size()) {
				std::fprintf(stderr,
					"[JLib::Scheduler] FATAL: epoch slot %zu requested but only %zu exist. "
					"A thread is using an epoch guard without a reserved slot -- see thread_id's "
					"assignment in Thread::StartWorker and TaskScheduler::StartPool.\n",
					tid, threadEpochs.size());
				std::fflush(stderr);
				assert(false && "epoch slot index out of range -- see stderr");
			}
#endif
			return &threadEpochs[tid]->localEpoch;
		}
		// EACH THREAD RECLAIMS ITS OWN BAG. No gate, because there is nothing shared to arbitrate --
		// see the note where `incoming`/`pending`/`reclaiming` used to be.
		//
		// ONE MinActiveEpoch CALL FOR THE WHOLE PASS, not one per entry: it is a scan over every
		// participant and the answer cannot change in a way that makes MORE things unsafe partway
		// through (the minimum only rises). Hoisting it is correct, not merely faster.
		void TryReclaim() {
			auto& bag = detail::EpochBag();
			if (bag.items.empty() && detail::g_epochOrphanCount.load(std::memory_order_relaxed) == 0)
				return;

			const size_t safeEpoch = MinActiveEpoch();
			size_t freed = 0;

			// Free what is now safe; compact survivors to the front. EBR keeps entries whose epoch
			// >= safeEpoch -- a reader may still be able to reach them.
			size_t kept = 0;
			for (size_t i = 0; i < bag.items.size(); ++i) {
				if (bag.items[i].epoch < safeEpoch) {
					bag.items[i].deleter(bag.items[i].ptr);
					++freed;
				} else {
					bag.items[kept++] = bag.items[i];
				}
			}
			bag.items.resize(kept);

			// AND SWEEP WHAT ABANDONED THREADS LEFT. Gated on a relaxed counter that stays 0 in a
			// healthy process, so this costs one load on the common path -- the same arrangement
			// HazardDomain uses, and for the same reason: the orphan case never happens until it
			// does, and then nothing else would ever free those entries.
			if (detail::g_epochOrphanCount.load(std::memory_order_acquire) != 0)
				freed += detail::SweepEpochOrphans(safeEpoch);
			// Guarded symmetrically with the increment in RetirePtr. With self-reclaim off nothing
			// ever increments this, so an unguarded subtract would wrap size_t to a huge value on
			// the first manual Tick(). Harmless while disabled -- Tick() is now the app's call
			// on the flag before ever reading the count -- but it would poison any diagnostic read
			// of RetiredCount(), which is exactly the sort of quietly-wrong number that wastes an
			// afternoon later.
			if (freed) retiredCount.fetch_sub(freed, std::memory_order_relaxed);
		}
		// Approximate count of pointers awaiting reclamation. Workers poll this to
		// self-trigger Tick() under load, so reclamation no longer depends solely on an
		// external (engine) Tick() call.
		size_t RetiredCount() const { return retiredCount.load(std::memory_order_relaxed); }

		// THE ONE PLACE that decides whether a worker should stop and reclaim. The four call sites
		// (three in Worker(), one in TaskScheduler) previously each spelled the comparison out, so
		// this also removes four copies of a predicate that must agree.
		// ASKS **THIS THREAD'S** BAG, not the global count. With per-thread bags a worker that has
		// retired nothing has nothing to reclaim, however busy the rest of the pool has been -- the
		// global counter would send it scanning every participant to free none of its own.
		//
		// A plain .size() on a vector this thread owns: no atomic, and no shared line to touch on a
		// predicate that runs on every task completion.

		// How many retirements should accumulate before a self-triggered Tick() is worth paying for.
		//
		// SCALES WITH THE PARTICIPANT SET, because that is what a reclaim actually costs:
		// MinActiveEpoch() scans EVERY participant -- one atomic load per worker slot AND per fiber
		// slot, and the fiber pool is sized per core, so the scan grows with the machine while a
		// fixed constant does not. At a hardcoded 512 the amortised cost per retirement was
		// scan_length/512, i.e. it got WORSE the bigger the pool: a 4-worker box scanned a handful of
		// slots, a 31-worker box with a full fiber pool scans on the order of two thousand.
		//
		// One retirement per participant makes that ratio exactly O(1) per retirement at any pool
		// size, which is the property worth holding fixed. The floor keeps small pools from
		// reclaiming so eagerly that the scan dominates a nearly-empty retire list.
		//
		// Safe to read on the hot path: `participants` is built once in StartPool and frozen before
		// any worker runs (see MinActiveEpoch's comment), so this is a plain size read with no
		// synchronisation -- and it sits behind a relaxed atomic load that already happened.
		size_t ReclaimThreshold() const {
			constexpr size_t kFloor = 512;
			const size_t p = participants.size();
			return p > kFloor ? p : kFloor;
		}
		size_t CurrentEpoch() { return globalEpoch.load(std::memory_order_acquire); }
		size_t MinActiveEpoch() {
			size_t minEpoch = globalEpoch.load(std::memory_order_acquire);
			// Scan the participants union (all fiber slots + all thread fallback slots).
			// Registration only happens at setup, so this never races a freed slot.
		
			// participants is built once during StartPool (single-threaded) and frozen before any
			// worker runs; thread-creation publishes it. Hence MinActiveEpoch reads it WITHOUT a lock.
			// If you ever add runtime (un)registration, this read becomes a data race -- re-add a lock.
			
			//	std::lock_guard<std::mutex> lock(participantMutex);
			
			for (auto* slot : participants) {
				size_t e = slot->load(std::memory_order_acquire);
				if (e != SIZE_MAX && e < minEpoch) minEpoch = e;
			}

			// ONE MECHANISM, SO ONE SCAN. A second loop over the counted-epoch ring lived here and
			// cost 8 slots x 32 shards = 256 seq_cst loads on every reclaim attempt. With counted
			// epochs gone there is exactly one class of reader and the minimum over the participant
			// slots is exact -- a slot reader ANNOUNCES the epoch it entered, so nothing has to be
			// inferred from a ring position.
			return minEpoch;
		}
	
		template<typename T>
		void RetirePtr(T* p, size_t epoch, DeleterFunc d) {
			// A PUSH INTO A VECTOR ONLY THIS THREAD TOUCHES. No queue, no atomic, no producer
			// lookup -- this was a moodycamel implicit-producer enqueue, which hashes the thread,
			// may allocate a block, and copies 24 bytes.
			//
			// THE DEAD-BAG CHECK IS NOT PARANOIA. thread_locals are destroyed in an order nobody
			// controls, and one destroyed later than this bag may itself retire -- pushing into a
			// vector whose storage is already gone. After the flag is up, entries go straight to the
			// orphan store, which is leaked and therefore always valid.
			if (detail::t_epochBagDead) {
				std::vector<detail::EpochRetired> one{ detail::EpochRetired{ (void*)p, epoch, d } };
				detail::OrphanEpochEntries(one);
			} else {
				detail::EpochBag().items.push_back(detail::EpochRetired{ (void*)p, epoch, d });
			}
			// DIAGNOSTIC ONLY NOW. The self-reclaim DECISION is thread-local (see
			// ShouldSelfReclaim); this keeps RetiredCount() meaning what it used to mean for anyone
			// reading it, and stays gated so the disabled path pays nothing.
			retiredCount.fetch_add(1, std::memory_order_relaxed);
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			// DEV-ONLY: make "disabled and never ticked" loud instead of silent.
			//
			// Turning self-reclaim off is a legitimate choice -- but the failure mode when you forget
			// the matching Tick() is unbounded memory growth with nothing to point at, which is the
			// one genuinely bad property of the OFF path. Counting here costs a relaxed increment in
			// builds where that is irrelevant, and buys a single sentence naming the exact mistake.
			// Release keeps paying nothing: it does not compile this block at all.
			//
			// UNCONDITIONAL, AND IT USED TO BE AN `else`. It was the else-arm of an
			// `if (ShouldSelfReclaim())` -- when self-reclaim was removed the `if` went and this
			// arm was left behind, an `else` with nothing to attach to. RELEASE NEVER NOTICED,
			// because the whole block is inside the !NDEBUG guard, so every local Release build was
			// green while Debug did not compile at all. Retiring is now always worth counting: with
			// the sweep queued on fiber death, a retire that never gets swept means the reaper is
			// not running, which is a scheduler bug rather than a missing call.
			{
				const size_t n = devRetiredNeverSwept.fetch_add(1, std::memory_order_relaxed) + 1;
				if (n > 100000 && !devNoTickWarned.exchange(true, std::memory_order_relaxed)) {
					std::fprintf(stderr,
						"[JLib::Scheduler] %zu pointers retired and Tick() has NEVER RUN. Memory is\n"
						"  growing without bound, and this is a BUG IN THE SCHEDULER rather than\n"
						"  something you forgot.\n"
						"  Reclamation is queued as a TASK by FiberRegistry::QueueReclaim on every\n"
						"  fiber death, so a healthy pool sweeps without you doing anything. Seeing\n"
						"  this means those tasks are not running: either no fiber has died (a pool\n"
						"  that only runs Native tasks never recycles one), or the pool was never\n"
						"  started, or the sweep task is being dropped.\n"
						"  You CAN call EpochManager::Instance().Tick() yourself as a workaround, but\n"
						"  please report it -- the reaper is supposed to make that unnecessary.\n"
						"  This warning prints once.\n", n);
					std::fflush(stderr);
				}
			}
#endif
		}

	
	private:
		// ================================================================================================
		// COUNTED EPOCHS -- the second reader mechanism, for readers with no stable identity.
		//
		// WHY THIS IS WORTH TWO MECHANISMS, and it is not performance -- measured, the two are
		// indistinguishable on the frame DAG (22.20 vs 21.78 us/graph, overlapping). It is that this
		// library's whole claim is THREE EXECUTION MODES ON ONE POOL with Task as the common
		// denominator, and reclamation was the one system that served only two of them. A coroutine
		// could use the DAG, the reactor, the primitives and the slab -- but could not safely hold
		// epoch protection, because it has no slot and borrowing a worker's corrupts rather than
		// leaks. Closing that makes "universal" true rather than nearly true.
		//
		// ============================================================================================
		// WHICH MECHANISM A NEW KIND OF READER SHOULD USE, because this will come up again.
		//
		// The rule is NOT "does it have identity". It is: IS THE READER DRAWN FROM A BOUNDED SET
		// ALLOCATED UP FRONT?
		//
		//   YES -> SLOTS. A slot can be reserved for each one at startup, registered once, and never
		//          unregistered. Threads and fibers both qualify: the fiber pool is preallocated and
		//          recycled, so a fiber's slot is stable for the life of the process. Slots are
		//          faster and lock-free -- two uncontended stores against two RMWs -- and the
		//          participant vector stays frozen, which is what lets MinActiveEpoch scan it with
		//          no lock at all.
		//
		//   NO  -> THE COUNTED RING. Transients. Nothing can be reserved for a reader that does not
		//          exist yet and may never exist again, and any fixed pool you invent reintroduces
		//          the ceiling that made them transient in the first place.
		//
		// THE COUNTERFACTUAL IS THE CLEAN TEST: if fibers were created on demand rather than drawn
		// from a pool built at launch, they would be transients too and belong on the ring. Nothing
		// about a fiber makes slots right -- the POOL does. Coroutines are on the ring because they
		// are allocated per operation from the slab, not because they are coroutines.
		//
		// SRCU reached the same structure from a different direction: its readers may SLEEP, so a
		// critical section outlives the CPU it started on. Ours SUSPEND, so it outlives the worker.
		// Both reduce to the same thing -- the reader is not bound to whatever you would index by --
		// and both answer it by counting readers per epoch instead of locating them.
		//
		// JLib::EpochGuard -- DEFINED IN Thread.h, not here -- is the single place this decision is
		// made. Keep it that way. It lives there because picking needs OnCoroutineTask(), which needs
		// Thread::GetCurrent(), and Thread.h includes THIS header; the reverse edge cannot exist. The
		// full reasoning is in the block above that class.
		// ============================================================================================
		//
		// The slot scheme above asks WHERE EVERY READER IS and needs one stable slot per reader.
		// Fibers have that (fixed pool, registered once); coroutines cannot, and any fixed pool for
		// them reintroduces the exact ceiling coroutines exist to escape. So this asks a different
		// question -- HOW MANY READERS ARE IN EACH EPOCH -- and identity disappears:
		//
		//     enter:  e = globalEpoch;  counters[e % N]++     the token IS `e`
		//     leave:  counters[token % N]--
		//
		// A coroutine holds `e` in its own frame, migrates freely, and shares nothing with any
		// worker. Unbounded readers, no registration, nothing to clobber. SRCU in shape.
		//
		// MODEL-CHECKED: tests/verify/counted_epoch_model.c, GenMC 0.17.0. Read its RESULTS block
		// before changing anything here -- two of the decisions below are load-bearing and one of
		// them is proven so.
		//
		// NO RE-VALIDATION, and that is deliberate rather than an omission. The obvious hazard is a
		// reader loading the epoch, the reclaimer advancing and freeing, then the reader incrementing
		// a counter nobody will look at again. GenMC says it cannot happen HERE, for a structural
		// reason: the reclaimer unlinks BEFORE it checks counters, and a reader announces BEFORE it
		// traverses -- so a reader whose increment lands after the check has necessarily loaded the
		// pointer after the unlink and finds nothing. The window closes itself.
		//
		//   THAT MAKES announce-then-traverse A REQUIREMENT, not a convention. Any path that reads a
		//   protected pointer BEFORE entering an epoch breaks this silently. The RAII guard is what
		//   enforces it: construct, then traverse. If a call site ever cannot, restore the
		//   re-validation (load, increment, re-load, retry) rather than reasoning about the case.
	public:
		// ---- THE ADVANCE GATE WENT WITH THE RING, AND THAT IS NOT A WEAKENING -------------------
		//
		// This used to refuse to advance into an OCCUPIED ring slot, and GenMC proved the gate
		// necessary -- removing it was the one control that FAILED. But what it protected was the
		// ring's ARITHMETIC: a counted reader's slot position had to map to exactly one epoch, and
		// advancing into an occupied slot made a reader parked kEpochSlots epochs back
		// indistinguishable from one entering now.
		//
		// A SLOT READER ANNOUNCES ITS EPOCH DIRECTLY. Nothing is inferred from a position, so there
		// is nothing for an ambiguous slot to corrupt, and the minimum over the participants is
		// exact however far the global epoch has run ahead. The gate was never protecting them.
		//
		// AND IT COST SOMETHING REAL: advancement could DECLINE, which stalls reclamation for as
		// long as anybody is parked -- a bounded leak, but one that existed only to keep the ring
		// honest. Now it is an unconditional CAS and the only way it returns false is losing a race
		// to another advancer, which is not a stall.
		bool AdvanceEpoch() {
			size_t e = globalEpoch.load(std::memory_order_acquire);
			return globalEpoch.compare_exchange_strong(e, e + 1, std::memory_order_seq_cst,
			                                           std::memory_order_relaxed);
		}


	};
};

// THE INVARIANT: a fiber must not SUSPEND OR YIELD while inside an EpochGuard.
//
// Why it matters. Reclamation frees only up to MinActiveEpoch(), the minimum announced epoch across
// the whole (static) participant set. A fiber that parks inside a guard keeps its slot announced at
// the epoch it entered for as long as it stays parked -- a GPU fence, an event, indefinitely. While
// it sits there NOTHING retired afterwards can ever be reclaimed, and the retired list grows without
// bound.
//
// This is a LIVENESS bug, not a safety one, and knowing which matters for diagnosing it.
// CurrentEpochSlot() hands a fiber its OWN slot (see LockFreeList.h), and that slot travels across a
// context switch, so the destructor always writes the right place no matter which worker resumes it.
// Nothing is corrupted. Instead memory creeps, and it surfaces as an unrelated allocator dying an
// hour into a session -- precisely the profile of the LockFreeList destructor leak this project
// already paid for once. That is why this is a tripwire and not a comment.
//
// AND FOR A COROUTINE IT IS WORSE THAN A LEAK -- it is memory corruption. A coroutine has no epoch
// slot: it runs as a Native task with no fiber, so CurrentEpochSlot() hands it the WORKER's fallback
// -- and a coroutine is not bound to a worker. Park inside a guard and that worker keeps running;
// its very next EpochGuard writes its own epoch into the same slot on entry and SIZE_MAX on exit.
// The parked coroutine's announcement is gone while its traversal is still live, and whatever it was
// protecting becomes reclaimable underneath it. See ~EpochGuard's note about the shared fallback --
// this is that, arriving.
//
// GIVING COROUTINES SLOTS WAS TRIED (2026-08-25) AND REVERTED. Not because it was hard: because it
// cannot be made to work, for a reason that is worth writing down so nobody rebuilds it.
//
// Per-coroutine registration is out immediately -- MinActiveEpoch() scans `participants` WITHOUT a
// lock precisely because registration happens once at setup, and coroutines are unbounded and
// dynamic. So the attempt was a FIXED pool of slots claimed on guard entry and released on exit,
// sized at 2x threads on the reasoning that concurrent guards cannot exceed threads-executing-
// guarded-code.
//
// That reasoning is circular. It holds only while no guard spans a suspension, which is the one
// case the slot exists for. Measured: 132 coroutines parked inside guards against a 62-slot pool
// left 37 of them falling back to the shared worker slot. Corruption 28% of the time rather than
// 100% is not a fix -- it is a worse bug, because it passes every test and fails in production.
//
// AND ANY BOUND IS WRONG IN PRINCIPLE, which is the deciding argument. Coroutines exist in this
// library precisely BECAUSE fibers are bounded: IoReactor.h's own note says a reactor's steady state
// is thousands of parked operations, so a bounded park budget "would not degrade, it would stop".
// A pool of parking slots -- of any size -- reintroduces exactly the ceiling coroutines were adopted
// to escape. Sizing was never the problem. Bounding was.
//
// SO THE INVARIANT IS THE ANSWER, not a slot scheme. EBR cannot express "protected across an
// unbounded wait" -- that is what the technique is, not a gap in this implementation. Hazard
// pointers can, by protecting per-object and paying on every read. If something ever genuinely needs
// protection across a suspension, that is the mechanism to reach for; do not try to make epochs do
// it. The tripwire below is enforcement, and ArmResume carries the coroutine half.
//
// Not a defect peculiar to this design: bounded critical sections are what EBR IS, in the paper too.
// A stalled participant stalls reclamation everywhere; hazard pointers dodge it by protecting
// per-object and pay on every read. What fibers change is only how EASY the violation is to write,
// because suspending here is cheap and idiomatic. Today it holds by construction rather than by
// discipline -- guards live only inside LockFreeList's operations, which never wait. The fix at any
// future call site is always the same: finish the traversal, drop the guard, then wait.
//
// The counter is thread_local even though a guard is per-FIBER. That is sound precisely BECAUSE the
// thing being detected is the violation: if no yield happens inside a guard, the guard never spans a
// migration and thread-local and fiber-local agree. They diverge only in the case this exists to
// catch, and they diverge in the direction that fires.
//
// ---- ENFORCED IN RELEASE, AND READ FROM THE SLOT ----------------------------------------------
//
// This was DEBUG/DEVELOPMENT ONLY, and that was affordable for exactly one reason: a fiber carried
// its OWN epoch slot, so a fiber that suspended inside a guard was merely SLOW -- the slot stayed
// announced, reclamation stalled, and it showed up much later as allocator exhaustion. A leak.
//
// ONCE EVERY READER USES THE THREAD SLOT, THAT SAME VIOLATION IS A USE-AFTER-FREE. Enter announces
// thread A's slot; the fiber resumes on B and its destructor clears B's slot -- which was never set,
// so a live traversal on B is un-announced and its nodes are freed underneath it. The fiber slot was
// never protection for the fiber; it was insurance against this rule going unchecked.
//
// That is the identical trade counted epochs made for coroutines, one level up, and it gets the
// identical answer: DELETE THE INSURANCE, ENFORCE THE RULE.
//
// IT READS THE SLOT, NOT A DEPTH COUNTER, and that is what makes it affordable to ship. The counter
// cost two thread_local operations on EVERY guard -- the path measured at 2.52 billion guards/sec.
// The slot is already there, and asking it costs ONE load AT A SUSPENSION, which is rare and
// already expensive. So `t_epochGuardDepth` and its ENTER/LEAVE macros are gone entirely: dev builds
// stop paying for them too.
//
// WHY THE READ IS SOUND. With one slot per thread, the only announcer of this thread's slot is
// whatever is running on this thread. At a suspend point that is the caller. So "announced" means
// "the caller holds a guard", with no false-positive path -- and fibers and coroutines collapse into
// ONE check, because the thing being detected is the same for both.
//
// Not a defect peculiar to this design: bounded critical sections are what EBR IS, in the paper too.
// A stalled participant stalls reclamation everywhere; hazard pointers dodge it by protecting
// per-object and pay on every read. What fibers change is only how EASY the violation is to write,
// because suspending is cheap and idiomatic here. The fix at any call site is always the same:
// finish the traversal, drop the guard, then wait.
//
// IF THE TRAVERSAL GENUINELY MUST SPAN THE SUSPENSION, use a HazardGuard. Hazard cells are indexed
// by the reader and survive a park by design; that is why both schemes exist.
namespace JLib {
	// TEST SEAM, narrow on purpose: it replaces what happens AFTER detection, never whether
	// detection runs. A regression test cannot assert on a process that has already aborted.
	using EpochSuspendViolationFn = void (*)();
	inline std::atomic<EpochSuspendViolationFn> g_epochSuspendViolation{ nullptr };
	inline void SetEpochSuspendViolationHandlerForTest(EpochSuspendViolationFn fn) {
		g_epochSuspendViolation.store(fn, std::memory_order_relaxed);
	}

	// `where` names the suspend point, so the message says which call has to drop its guard first
	// rather than leaving you to find it.
	inline void EpochGuardSuspendCheck(const char* where) {
		std::atomic<size_t>* slot = EpochManager::Instance().ThreadSlot(thread_id);
		if (!slot) return;
		if (slot->load(std::memory_order_acquire) == SIZE_MAX) return;   // not announced: fine
		if (EpochSuspendViolationFn h = g_epochSuspendViolation.load(std::memory_order_relaxed)) {
			h();
			return;
		}
		fprintf(stderr,
			"[JLib::Scheduler] INVARIANT VIOLATED: suspended inside an EpochGuard at %s.\n"
			"  Every reader uses its THREAD's epoch slot. This guard announced on the thread it\n"
			"  started on, and will be destructed on whichever thread resumes it -- clearing a slot\n"
			"  that was never set there, which un-announces a live traversal and frees nodes\n"
			"  underneath it.\n"
			"  Fix: end the guarded traversal and let the EpochGuard destruct BEFORE waiting.\n"
			"  If it genuinely must span the suspension, use a HazardGuard -- hazard cells are\n"
			"  indexed by the reader and survive a park; epochs do not, and that is why both\n"
			"  schemes exist.\n",
			where);
		fflush(stderr);
		std::abort();
	}
}
// ENTER/LEAVE ARE NO-OPS EVERYWHERE NOW. The depth counter they maintained is gone; the check reads
// the slot instead. Kept as macros so the guard bodies do not churn.
#define JLIB_EPOCH_GUARD_ENTER()        ((void)0)
#define JLIB_EPOCH_GUARD_LEAVE()        ((void)0)
#define JLIB_EPOCH_CHECK_NO_GUARD(where) JLib::EpochGuardSuspendCheck(where)
namespace JLib {
	// ---- THE COROUTINE CHECK IS NOW THE SAME CHECK ------------------------------------------
	//
	// It had its own implementation while fibers carried per-fiber slots and coroutines borrowed the
	// worker's: the two readers announced in different places, so "am I inside a guard" was a
	// different question for each. With ONE SLOT PER THREAD it is the same question, and keeping two
	// answers to it is how they drift.
	//
	// The separate name survives only so the call site in Coroutine.h still reads as the coroutine
	// rule; the seam and the message are shared.
	inline void CoroEpochGuardSuspendCheck() { EpochGuardSuspendCheck("co_await"); }

	// ---- THE EXIT SITE IS AMBIGUOUS, SO IT WARNS RATHER THAN ABORTS ---------------------------
	//
	// A SUSPENSION inside a guard is unambiguously wrong: the guard announced on this thread and
	// will be dropped on whichever thread resumes, which is a use-after-free. Abort.
	//
	// A TASK RETURNING with the slot announced is NOT the same thing, and a single slot read cannot
	// tell the two cases apart:
	//
	//   LEAKED   -- this task took a guard and never dropped it. The thread stays announced,
	//               MinActiveEpoch cannot advance, and nothing retired afterwards is ever
	//               reclaimed. A leak, which is what this check has always said it was.
	//   NESTED   -- an OUTER traversal on this same thread is still live and legitimately
	//               announced. TaskDAG::ForEachDependent does exactly this: it holds a guard while
	//               firing dependents, so a dependent that runs inline reaches fiber exit inside
	//               someone else's guard, having done nothing wrong.
	//
	// dag_external_test hits the second case deterministically. Aborting on it would make a correct
	// program die, which is worse than the leak being reported late -- so this reports once and
	// keeps going, and the abort stays where the answer is unambiguous.
	//
	// THE UNDERLYING QUESTION IS NOT SETTLED: whether running a task inside an EpochGuard is legal
	// at all. If that dependent SUSPENDS, the outer guard spans a migration and the ambiguity
	// becomes the real bug. Recorded in design/NOTES.md rather than decided here.
	inline std::atomic<bool> g_exitGuardWarned{ false };
	inline void EpochGuardExitCheck(const char* where) {
		std::atomic<size_t>* slot = EpochManager::Instance().ThreadSlot(thread_id);
		if (!slot) return;
		if (slot->load(std::memory_order_acquire) == SIZE_MAX) return;
		bool expected = false;
		if (!g_exitGuardWarned.compare_exchange_strong(expected, true,
				std::memory_order_acq_rel, std::memory_order_relaxed))
			return;                                  // once per process, not once per task
		fprintf(stderr,
			"[JLib::Scheduler] NOTE: a task returned at %s with this thread's epoch slot still\n"
			"  announced. Either the task leaked an EpochGuard -- in which case reclamation stalls\n"
			"  from here on -- or an OUTER guard on this thread is legitimately still live\n"
			"  (TaskDAG::ForEachDependent holds one while it fires dependents). This warns once and\n"
			"  does not abort, because those two cases are indistinguishable from one slot read.\n",
			where);
		fflush(stderr);
	}

	// Back-compat alias for the seam. One handler, because there is one check.
	inline void SetCoroSuspendViolationHandlerForTest(EpochSuspendViolationFn fn) {
		SetEpochSuspendViolationHandlerForTest(fn);
	}
}
#define JLIB_EPOCH_CHECK_NO_GUARD_CORO() JLib::CoroEpochGuardSuspendCheck()
// The ambiguous site -- see EpochGuardExitCheck. Warns once, never aborts.
#define JLIB_EPOCH_CHECK_NO_GUARD_AT_EXIT(where) JLib::EpochGuardExitCheck(where)

// ==================================================================================================
// THE TWO MECHANISMS. NEITHER OF THESE IS THE ONE YOU WANT AT A CALL SITE.
//
// >>> USE JLib::EpochGuard, WHICH IS DEFINED IN Thread.h, NOT IN THIS FILE. <<<
//
// It is a tiny RAII wrapper that picks between the two below by asking WHO IS RUNNING, and it is
// the only place that decision is allowed to be made. It lives in Thread.h and not next to these
// definitions for one hard reason: picking requires OnCoroutineTask() -> Thread::GetCurrent(), and
// Thread.h already includes Epochs.h. Epochs.h cannot see Thread without a cycle. Forward-declaring
// it here would compile and then fail silently in any TU that did not also include Thread.h, which
// is precisely the mistake -- handing a coroutine a borrowed slot -- that the class exists to make
// impossible. The long version of this argument sits above the class in Thread.h.
//
// So: this file owns the mechanisms and knows nothing about the caller. Thread.h owns the choice.
// Name these two directly only in code that is provably one kind of reader forever (the benchmark
// that measures one against the other, for instance).
// ==================================================================================================

// THE COUNTED GUARD WAS HERE. It announced by incrementing an epoch's counter rather than writing a
// slot, so protection travelled in a token in the coroutine's own frame and survived a migration.
// Removed with the ring: it insured against suspending inside a guard, which
// CoroEpochGuardSuspendCheck already forbids and now asserts in RELEASE as well as dev builds. A
// coroutine that genuinely needs reclamation across a suspend uses hazard pointers, which support
// it by design.
//
// THERE IS NOW ONE MECHANISM, so this file no longer owns a choice and Thread.h's EpochGuard no
// longer makes one -- it is an alias for the slot guard over CurrentEpochSlot().

// THE SLOT GUARD -- for a reader that owns a stable slot for the guard's whole life: a fiber, or a
// bare thread. Two uncontended stores, which is why it stays the default for those. Named
// SlotEpochGuard, not EpochGuard, because it is a mechanism and not a call-site choice; see the
// block above, and use JLib::EpochGuard from Thread.h.
struct SlotEpochGuard {
	std::atomic<size_t>* slot;

	SlotEpochGuard(std::atomic<size_t>* s) : slot(s) {
		// Enter: store global epoch
		slot->store(JLib::EpochManager::Instance().CurrentEpoch(),
			std::memory_order_release);
		JLIB_EPOCH_GUARD_ENTER();
	}

	~SlotEpochGuard() {
		// Leave: mark as SIZE_MAX.
		//
		// NOTE, unstated until now: this stores SIZE_MAX unconditionally rather than restoring the
		// previous value, so NESTED guards sharing one slot break the outer one -- the inner exit
		// un-announces a traversal that is still running. Not reachable from today's call sites
		// (LockFreeList's operations never wait, so a guarded region cannot re-enter), but the
		// bare-thread fallback shares a single slot per thread, which is where it would surface.
		slot->store(SIZE_MAX, std::memory_order_release);
		JLIB_EPOCH_GUARD_LEAVE();
	}
};
