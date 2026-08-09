// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <thread>
#include <vector>
#include <mutex>
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
			if (freed) retiredCount.fetch_sub(freed, std::memory_order_relaxed);

			reclaiming.store(false, std::memory_order_release);
		}
		// Approximate count of pointers awaiting reclamation. Workers poll this to
		// self-trigger Tick() under load, so reclamation no longer depends solely on an
		// external (engine) Tick() call.
		size_t RetiredCount() const { return retiredCount.load(std::memory_order_relaxed); }
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
			retiredCount.fetch_add(1, std::memory_order_relaxed);
		}

	
	private:
		void AdvanceEpoch() { globalEpoch.fetch_add(1, std::memory_order_acq_rel); }


	};
};

struct EpochGuard {
	std::atomic<size_t>* slot;

	EpochGuard(std::atomic<size_t>* s) : slot(s) {
		// Enter: store global epoch
		slot->store(JLib::EpochManager::Instance().CurrentEpoch(),
			std::memory_order_release);
	}

	~EpochGuard() {
		// Leave: mark as SIZE_MAX
		slot->store(SIZE_MAX, std::memory_order_release);
	}
};