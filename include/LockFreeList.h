// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#define NOMINMAX
#include <vector>
#include <atomic>
#include <iostream>
// <intrin.h> was here and is MSVC-only. Removed rather than guarded: nothing in this header uses
// an intrinsic from it -- it was a leftover include that only ever cost a Linux build.
#include <limits>
#include <cstdint>
#include "Epochs.h"
#include "TaskAllocator.h"
#include "Thread.h"
#include "Fiber.h"

namespace JLib {
	// Epoch slot for the current execution context: the running fiber's slot if we're on
	// one, else this (bare) thread's fallback slot. The fiber branch is migration-proof --
	// the slot travels with the fiber across a context switch. Bare threads (e.g. the main
	// thread building a DAG) don't migrate, so their per-thread fallback is correct.
	inline std::atomic<size_t>* CurrentEpochSlot() {
		if (Thread* w = Thread::GetCurrent())
			if (Fiber* f = w->currentFiber)
				return &f->localEpoch;
		return EpochManager::Instance().ThreadSlot(thread_id);
	}

	struct LNodeBase; // forward declaration

	// MarkableReference stores a Node* and a bool mark
	struct LMarkableReference {
		LNodeBase* val_;
		bool marked_;

		LMarkableReference(LNodeBase* val = nullptr, bool mark = false)
			: val_(val), marked_(mark) {}
	};

	// Hardcoded MarkablePointer for Node*
	struct LMarkablePointer {
		std::atomic<uintptr_t> ref_{ 0 };

		// Packing: Use the last bit for the mark
		static uintptr_t pack(LNodeBase* ptr, bool mark) {
			return reinterpret_cast<uintptr_t>(ptr) | (mark ? 1ULL : 0ULL);
		}

		static LNodeBase* unpackPtr(uintptr_t val) {
			return reinterpret_cast<LNodeBase*>(val & ~1ULL);
		}

		static bool unpackMark(uintptr_t val) {
			return (val & 1ULL) != 0;
		}

		// Default: empty/null
		LMarkablePointer(LNodeBase* val = nullptr, bool mark = false) {
			ref_.store(pack(val, mark), std::memory_order_release);
		}
		bool getMark() const {
			return (ref_.load(std::memory_order_acquire) & 1ULL) != 0;
		}
		void set(LNodeBase* val, bool mark) {
			// pack() uses the bitwise OR logic to combine the pointer and the mark
			ref_.store(pack(val, mark), std::memory_order_release);
		}
		// 2. Flip the bit without any allocations
		bool attemptMark(LNodeBase* expectedPtr, bool newMark) {
			uintptr_t curr = ref_.load(std::memory_order_acquire);
			while (true) {
				LNodeBase* ptr = reinterpret_cast<LNodeBase*>(curr & ~1ULL);
				bool mark = (curr & 1ULL) != 0;

				if (ptr != expectedPtr) return false;
				if (mark == newMark) return true;

				uintptr_t desired = reinterpret_cast<uintptr_t>(ptr) | (newMark ? 1ULL : 0ULL);
				if (ref_.compare_exchange_weak(curr, desired, std::memory_order_acq_rel))
					return true;
			}
		}
		// Atomic Get
		LNodeBase* get(bool& mark) const {
			uintptr_t val = ref_.load(std::memory_order_acquire);
			mark = unpackMark(val);
			return unpackPtr(val);
		}
		LNodeBase* getReference() const {
			// Load the atomic value
			uintptr_t val = ref_.load(std::memory_order_acquire);
			// Mask out the LSB (Least Significant Bit) and cast back to pointer
			return reinterpret_cast<LNodeBase*>(val & ~1ULL);
		}
		// Atomic CAS
		bool compareAndSet(LNodeBase* expectedPtr, LNodeBase* newPtr, bool expectedMark, bool newMark) {
			uintptr_t expected = pack(expectedPtr, expectedMark);
			uintptr_t desired = pack(newPtr, newMark);
			return ref_.compare_exchange_strong(expected, desired, std::memory_order_acq_rel);
		}
	};
	struct LNodeBase {
		LMarkablePointer next;   // always points to NodeBase*
		uint64_t key;           // keep the key here for traversal/comparison
		// The allocator this node came from, so it can be returned to the RIGHT one.
		//
		// Needed because reclamation goes through EpochManager::RetirePtr, whose callback is a
		// plain void(*)(void*) with no context -- so the deleter is necessarily a STATIC function
		// and cannot see the owning list's `allocator` member. Carrying the pointer per node is
		// the cheapest way to give the callback that context.
		//
		// Previously slabDeleter simply referenced the instance member from a static function.
		// MSVC never caught it because nothing instantiated remove() on a slab-backed list; GCC
		// rejects it outright. It was a landmine rather than dead code -- the first caller of
		// remove() would have broken the build on both compilers.
		TaskAllocator* owner = nullptr;
	};
	template<typename T>
	struct LNode : LNodeBase {
		T data;  // actual payload (the list's element type, e.g. TaskNode* or Fiber*)
		LNode(uint64_t k, T d) {  // accept the payload by value
			key = k;
			data = d;
		}
	};

	template <typename T>
	class LockFreeList {
		struct Window {
			LNodeBase* pred;
			LNodeBase* curr;
			Window(LNodeBase* myPred, LNodeBase* myCurr) {
				pred = myPred, curr = myCurr;
			}
			static Window find(LNodeBase* head, uint64_t key) {
				LNodeBase* pred = nullptr;
				LNodeBase* curr = nullptr;
				LNodeBase* succ = nullptr;
				bool marked = false;
				bool snip = false;
			RETRY:
				while (true) {
					pred = head;
					curr = pred->next.getReference();
					while (true) {
						succ = curr->next.get(marked);
						while (marked) {
							snip = pred->next.compareAndSet(curr, succ, false, false);
							if (!snip) goto RETRY;
							curr = succ;
							succ = curr->next.get(marked);
						}
						if (curr->key >= key)
							return Window(pred, curr);
						pred = curr;
						curr = succ;
					}
				}
			}
		};
		TaskAllocator& allocator;

		static void slabDeleter(void* ptr) {
			auto* node = static_cast<LNode<T>*>(ptr);

			// 1. If you used placement new, explicitly destroy
			node->data.~T();

			// 2. Return the raw memory block to the EXACT allocator that gave it to you -- read
			// back off the node, since a static callback has no instance to ask.
			if (node->owner) node->owner->Free(node);
		}
		static void heapDeleter(void* ptr) {
			auto* node = static_cast<LNode<T>*>(ptr);
			// 1. Manually call the destructor of the data if Task*is a complex object
			node->data.~T();
			// 2. Use 'delete' since you used 'new'
			delete node;
		}
		LNodeBase* head;
		LNodeBase* tail;
	public:
		LockFreeList(TaskAllocator& alloc) : allocator(alloc) {
			void* mem = allocator.Alloc();
			void* mem2 = allocator.Alloc();
			head = new (mem) LNode<T>(0, T());
			tail = new (mem2) LNode<T>(UINT64_MAX, T());
			head->owner = &allocator;
			tail->owner = &allocator;
			head->next.set(tail, false);
		}
		~LockFreeList() {
			// Free every LIVE entry between the sentinels too, not just head/tail -- entries
			// added via add() (e.g. TaskDAG::AddDependency's edges) were being leaked forever,
			// since nothing ever called remove() on them and the destructor only reclaimed the
			// two sentinels. Confirmed root cause of a slow TaskAllocator exhaustion: 6
			// AddDependency() calls/frame in one particular app, each permanently leaking one
			// slab slot the moment this list's owning TaskNode got destroyed.
			// Safe to walk WITHOUT an EpochGuard: by the time this destructor runs (via
			// NodeDeleter, after EBR retirement), no reader can still be traversing this list --
			// that's the whole point of retiring the OWNING node through EBR first.
			LNodeBase* curr = head->next.getReference();
			while (curr != tail) {
				LNodeBase* next = curr->next.getReference();
				LNode<T>* typed = static_cast<LNode<T>*>(curr);
				typed->data.~T();
				allocator.Free(curr);
				curr = next;
			}
			allocator.Free(head);
			allocator.Free(tail);
		}
		bool add(uint64_t key, T item) {
			EpochGuard guard(CurrentEpochSlot());   // RAII: leaves on every return path
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);

				if (curr->key == key) {
					return false;
				}
				void* mem = allocator.Alloc();
				LNode<T>* node = new (mem) LNode<T>(key, item);
				node->owner = &allocator;   // so slabDeleter can return it on retire
				node->next.set(curr, false);

				if (pred->next.compareAndSet(curr, node, false, false)) {
					return true;
				}
			}
		}
		bool remove(uint64_t key) {
			EpochGuard guard(CurrentEpochSlot());   // RAII: leaves on every return path
			bool snip = false;
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);
				if (curr->key != key) {
					return false;
				}
				else {
					LNode<T>* succ = static_cast<LNode<T>*>(curr->next.getReference());
					snip = curr->next.attemptMark(succ, true);
					if (!snip)
						continue;
					pred->next.compareAndSet(curr, succ, false, false);
					EpochManager::Instance().RetirePtr(
						curr,
						EpochManager::Instance().CurrentEpoch(),
						&LockFreeList<T>::slabDeleter
					);
					return true;
				}
			}
		}
		template <typename F>
		void for_each(F func) {

			EpochGuard guard(CurrentEpochSlot());  // Ensure we are in an epoch for safe traversal
			// Start after the sentinel head
			LNodeBase* curr = head->next.getReference();

			while (curr != tail) {
				// Use your new bit-packed methods
				bool marked = curr->next.getMark();
				LNodeBase* succ = curr->next.getReference();

				if (!marked) {
					// Cast to the internal node type to access the data
					LNode<T>* typedNode = static_cast<LNode<T>*>(curr);
					func(typedNode->data);
				}
				curr = succ;
			}
		}
		bool contains(uint64_t key) {
			EpochGuard guard(CurrentEpochSlot());  // Ensure we are in an epoch for safe traversal
			LNodeBase* curr = head;

			while (curr != nullptr) {
				LNodeBase* succ = curr->next.getReference();
				bool marked = curr->next.getMark();

				if (curr->key >= key) {
					return (curr->key == key && !marked);
				}

				curr = succ;
			}
			return false;
		}
		T* get(uint64_t key) {
			EpochGuard guard(CurrentEpochSlot());  // Ensure we are in an epoch for safe traversal
			bool marked = false;
			LNodeBase* curr = head;

			while (curr->key < key) {
				curr = curr->next.get(marked);
			}

			if (curr->key == key && !marked) {
				LNode<T>* typedNode = static_cast<LNode<T>*>(curr);
				return &typedNode->data;  // return pointer to T
			}

			return nullptr;  // not found
		}
	};



};