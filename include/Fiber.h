#pragma once
#include "Context.h"
#include "platform.h"
#include "Task.h"
#include <atomic>
namespace T_Threads {
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
		Task* owningTask = nullptr; // The task currently running on this fiber
		Context* homeCtx = nullptr; // Scheduler ctx to return to; the worker sets this before each switch-in
		std::atomic<FiberStatus>  status;
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
		void Resume();   // Moves from SUSPENDED -> READY

		// Safety check for the work-stealer
		bool IsReady() const { return status == FiberStatus::READY; }
	};
} // namespace T_Threads
