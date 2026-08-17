// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
#include <cstdio>    // fprintf -- the debug-only EpochGuard suspend tripwire below
#include <cassert>   // assert   -- ditto
#include "Task.h"
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
	class EpochManager {
	private:
		std::vector<std::atomic<size_t>*> participants;
	//	std::mutex participantMutex;
		struct GlobalRetired {
			void* ptr;        // node/arena pointer to free once safe
			size_t epoch;     // epoch at which it was retired
			void (*deleter)(void*);
		};
		// Producers (RetirePtr, i.e. a list remove) enqueue here LOCK-FREE.
		moodycamel::ConcurrentQueue<GlobalRetired> incoming;
		// Owned exclusively by the single active reclaimer (gated by `reclaiming`): holds
		// drained entries not yet safe to free. No lock needed -- only one thread touches it.
		std::vector<GlobalRetired> pending;
		std::atomic<bool>   reclaiming{ false };   // only one reclaimer at a time
		std::atomic<size_t> retiredCount{ 0 };     // approx live retired count; drives self-reclaim

		// Whether workers self-trigger reclamation. See SetSelfReclaim() for the contract.
		//
		// A PLAIN bool, NOT atomic, and that is a deliberate difference from
		// TaskScheduler::SetIdlePolicy -- which is atomic precisely BECAUSE it is documented as
		// changeable on a running pool. This one is documented as settable only before StartPool,
		// so a worker hoisting the read out of its loop is not a bug here, it is the point: the
		// branch folds away and the disabled build pays nothing at all. Make this atomic only if
		// the contract ever changes to allow runtime flips, and change the contract first.
		bool selfReclaim = true;

		// Bookkeeping for the "disabled and never ticked" warning in RetirePtr. Only the CODE that
		// touches these is conditional; the MEMBERS are unconditional and deliberately so.
		//
		// EpochManager is header-only, so an #if around a member makes sizeof() depend on the
		// including translation unit's build flags -- a Release library with a Development consumer
		// would then disagree about this class's layout. That is precisely the failure this file's
		// own stale-library guard exists to catch, and manufacturing more of it to save 16 bytes in
		// a singleton would be a bad trade. Unused in Release; costs nothing there.
		std::atomic<size_t> devRetiredWhileDisabled{ 0 };
		std::atomic<bool>   devNoTickWarned{ false };

		std::atomic<size_t> globalEpoch{ 0 };

		struct ThreadEpoch {
			std::atomic<size_t> localEpoch{ SIZE_MAX };   // SIZE_MAX == not in an epoch
		};
		std::vector<ThreadEpoch*> threadEpochs;
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
			devRetiredWhileDisabled.store(0, std::memory_order_relaxed);
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
		void TryReclaim() {
			// One reclaimer at a time. Others bail -- reclaim is cold and idempotent. This
			// makes the reclaimer effectively single-threaded, so `pending` needs no lock
			// even though RetirePtr runs concurrently (it only touches `incoming`).
			bool expected = false;
			if (!reclaiming.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
				return;

			// 1. Drain what producers enqueued into our private list.
			GlobalRetired item;
			while (incoming.try_dequeue(item))
				pending.push_back(item);

			// 2. Free what's now safe; compact survivors to the front. EBR keeps entries
			//    whose epoch >= safeEpoch -- a reader may still be able to reach them.
			size_t safeEpoch = MinActiveEpoch();
			size_t kept = 0, freed = 0;
			for (size_t i = 0; i < pending.size(); ++i) {
				if (pending[i].epoch < safeEpoch) {
					pending[i].deleter(pending[i].ptr);
					++freed;
				} else {
					pending[kept++] = pending[i];
				}
			}
			pending.resize(kept);
			// Guarded symmetrically with the increment in RetirePtr. With self-reclaim off nothing
			// ever increments this, so an unguarded subtract would wrap size_t to a huge value on
			// the first manual Tick(). Harmless while disabled -- ShouldSelfReclaim() short-circuits
			// on the flag before ever reading the count -- but it would poison any diagnostic read
			// of RetiredCount(), which is exactly the sort of quietly-wrong number that wastes an
			// afternoon later.
			if (selfReclaim && freed) retiredCount.fetch_sub(freed, std::memory_order_relaxed);

			reclaiming.store(false, std::memory_order_release);
		}
		// Approximate count of pointers awaiting reclamation. Workers poll this to
		// self-trigger Tick() under load, so reclamation no longer depends solely on an
		// external (engine) Tick() call.
		size_t RetiredCount() const { return retiredCount.load(std::memory_order_relaxed); }

		// THE ONE PLACE that decides whether a worker should stop and reclaim. The four call sites
		// (three in Worker(), one in TaskScheduler) previously each spelled the comparison out, so
		// this also removes four copies of a predicate that must agree.
		bool ShouldSelfReclaim() const {
			return selfReclaim && RetiredCount() > ReclaimThreshold();
		}

		// Turn OFF worker self-triggered reclamation, making Tick() the caller's job.
		//
		// MUST be called before StartPool, and never again. It is a plain bool read from every
		// worker; flipping it on a live pool is a data race, and a worker may have hoisted the read
		// out of its loop and never see it anyway. That is not a defect -- see the member's comment.
		//
		// WHY YOU MIGHT -- and it is NOT the atomic, which was the original guess and measured
		// nothing. The real win is TAIL LATENCY, and it is large. Frame-shaped DAG loop, 32 nodes
		// per frame, 4000 frames after 400 warm-up, three interleaved rounds, per-frame us:
		//
		//     self-reclaim  p50 58.1/58.9/60.2   p90 67.7/66.8/69.0   p99 331/331/336
		//     tick on main  p50 60.5/58.7/58.5   p90 69.3/67.8/66.3   p99 125/111/104
		//
		// Median and p90 are a wash -- the fetch_add really is free, exactly as the comparable
		// nextWorker experiment predicted. But p99 improves ~3x, and the reason is WHERE the work
		// happens, not how much of it there is: TryReclaim calls MinActiveEpoch(), which scans EVERY
		// participant slot (~2280 in that run, and it scales with the pool -- see ReclaimThreshold).
		// Under self-reclaim a WORKER stops to do that scan in the middle of a frame, stalling work
		// on the critical path at an unpredictable moment. Ticking from an idle main thread moves
		// the identical scan into the gap between frames.
		//
		// So this is for an application that cares about frame-time CONSISTENCY and has a natural
		// idle point. It buys nothing for throughput, and a batch job with no idle point should
		// leave it alone.
		//
		// WHY THE DEFAULT IS ON. Not because a library may not require an explicit pump -- plenty do,
		// legitimately, and "tick me from your loop" is a perfectly reasonable contract. It is on
		// because it is the SAFE FAILURE MODE: an embedder with no loop to tick from (a headless
		// server, a batch job, a plugin inside someone else's engine, a Join()-and-exit tool) gets
		// working reclamation without having read this comment, whereas the reverse default gets
		// unbounded growth without having read this comment.
		//
		// Both modes are supported and neither is a fallback. If your application has an idle point,
		// turning this off is a real choice with a measured benefit (below), not a workaround.
		//
		// The genuine hazard in the OFF mode is that forgetting Tick() used to fail SILENTLY. It no
		// longer does: Development and debug builds keep counting retirements even while disabled,
		// purely so the "disabled and never ticked" case can say so once. See the warning in
		// RetirePtr. Release pays nothing for that.
		//
		// AND MEASURE BEFORE ASSUMING IT BUYS ANYTHING. The `fetch_add` sits immediately after a
		// lock-free MPSC enqueue that is itself at least one atomic RMW plus a node link, so it is
		// the cheaper half of an operation you cannot remove. The directly comparable experiment --
		// making PickNextWorker's `nextWorker` relaxed, a locked xchg removed from the hotter PUSH
		// path, codegen-verified -- measured EXACTLY ZERO, because the neighbouring notify mutex
		// dominated. Expect the same here unless a profile says otherwise.
		//
		// If you turn this off, call EpochManager::Instance().Tick() from your idle path. It is
		// already public and needs no other change.
		void SetSelfReclaim(bool on) { selfReclaim = on; }
		bool SelfReclaimEnabled() const { return selfReclaim; }

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
			return minEpoch;
		}
	
		template<typename T>
		void RetirePtr(T* p, size_t epoch, DeleterFunc d) {
			incoming.enqueue(GlobalRetired{ (void*)p, epoch, d });   // lock-free
			// The ONLY atomic in this path, and the only reason it exists is to let workers
			// self-trigger. With self-reclaim off the counter has no reader, so skip it entirely --
			// that is the whole saving SetSelfReclaim(false) offers.
			if (selfReclaim) retiredCount.fetch_add(1, std::memory_order_relaxed);
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
			// DEV-ONLY: make "disabled and never ticked" loud instead of silent.
			//
			// Turning self-reclaim off is a legitimate choice -- but the failure mode when you forget
			// the matching Tick() is unbounded memory growth with nothing to point at, which is the
			// one genuinely bad property of the OFF path. Counting here costs a relaxed increment in
			// builds where that is irrelevant, and buys a single sentence naming the exact mistake.
			// Release keeps paying nothing: the branch above is all it sees.
			else {
				const size_t n = devRetiredWhileDisabled.fetch_add(1, std::memory_order_relaxed) + 1;
				if (n > 100000 && !devNoTickWarned.exchange(true, std::memory_order_relaxed)) {
					std::fprintf(stderr,
						"[JLib::Scheduler] %zu pointers retired with self-reclaim DISABLED and Tick() "
						"never called. Memory will grow without bound. Call "
						"EpochManager::Instance().Tick() from your idle path, or re-enable "
						"SetSelfReclaim(true). This warning prints once.\n", n);
					std::fflush(stderr);
				}
			}
#endif
		}

	
	private:
		void AdvanceEpoch() { globalEpoch.fetch_add(1, std::memory_order_acq_rel); }


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
// DEBUG/DEVELOPMENT ONLY. Release builds carry neither the counter nor the checks, so this costs a
// shipped binary nothing -- same policy as GetEvent's registry tripwire.
#if defined(_DEBUG) || defined(JLIB_DEVELOPMENT)
namespace JLib {
	inline thread_local int t_epochGuardDepth = 0;

	// `where` names the suspend point, so the message says which call has to drop its guard first
	// rather than leaving you to find it.
	inline void EpochGuardSuspendCheck(const char* where) {
		if (t_epochGuardDepth > 0) {
			fprintf(stderr,
				"[JLib::Scheduler] INVARIANT VIOLATED: fiber suspended inside an EpochGuard at %s "
				"(depth %d). The fiber's epoch slot stays announced for the whole suspension, so "
				"MinActiveEpoch() cannot advance and NOTHING retired from now on will ever be "
				"reclaimed. This does not crash -- it leaks, and shows up much later as allocator "
				"exhaustion. Fix: end the guarded traversal and let the EpochGuard destruct BEFORE "
				"waiting.\n",
				where, t_epochGuardDepth);
			assert(false && "fiber suspended while holding an EpochGuard -- see stderr");
		}
	}
}
#define JLIB_EPOCH_GUARD_ENTER()        (++JLib::t_epochGuardDepth)
#define JLIB_EPOCH_GUARD_LEAVE()        (--JLib::t_epochGuardDepth)
#define JLIB_EPOCH_CHECK_NO_GUARD(where) JLib::EpochGuardSuspendCheck(where)
#else
#define JLIB_EPOCH_GUARD_ENTER()        ((void)0)
#define JLIB_EPOCH_GUARD_LEAVE()        ((void)0)
#define JLIB_EPOCH_CHECK_NO_GUARD(where) ((void)0)
#endif

struct EpochGuard {
	std::atomic<size_t>* slot;

	EpochGuard(std::atomic<size_t>* s) : slot(s) {
		// Enter: store global epoch
		slot->store(JLib::EpochManager::Instance().CurrentEpoch(),
			std::memory_order_release);
		JLIB_EPOCH_GUARD_ENTER();
	}

	~EpochGuard() {
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