// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Context.h"
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
	void RequeueResumedBatch(Task** tasks, size_t n, bool hiPri);
}
