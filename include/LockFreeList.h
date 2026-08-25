// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

//
// STATUS AS OF 2.15.0: NO PRODUCTION CALLER. The scheduler itself does not use this any more --
// its last two consumers both moved to structures that fit their access patterns better, and the
// reasons are worth knowing before reaching for it again:
//
//   - Event's waiter index became a flat array indexed by Fiber::poolIndex. Its keys were never
//     arbitrary: a parked task always holds a fiber, fibers have dense stable indices, and a fiber
//     parks on one event at a time, so a perfect hash already existed. A general map was strictly
//     worse -- it allocated a cell per suspend on a path documented as allocation-free.
//   - TaskNode::dependents became pointer-linked edges in DAG-owned chunk storage. Graph
//     construction is single-threaded by contract, so the lock-freedom here was unreachable in
//     practice, and the list cost four slab slots per node before a single edge existed.
//
// It is KEPT, not deleted: it is correct, TSan-probed (bench/tsan_probe.cpp), and it is the
// substrate for LockFreeHashMap. Reach for it when the key space is genuinely arbitrary and
// concurrent. When the keys are already a dense bounded index, they are a better index than any
// hash of them.
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
			// SLAB EXHAUSTION. Placement-new onto null is the "access violation writing 0x0" this
			// header already warns about twice, and it became genuinely reachable when
			// LockFreeHashMap made bucket construction LAZY: the sentinels are no longer taken at
			// startup on a fresh slab, but at some later FIRST INSERT, which can land on a dry one.
			// Leave the list empty and report it through ok() instead of crashing -- a caller that
			// cannot get a bucket degrades (see Event::AddWaiter), it does not die.
			if (!mem || !mem2) {
				if (mem)  allocator.Free(mem);
				if (mem2) allocator.Free(mem2);
				head = tail = nullptr;
				return;
			}
			head = new (mem) LNode<T>(0, T());
			tail = new (mem2) LNode<T>(UINT64_MAX, T());
			head->owner = &allocator;
			tail->owner = &allocator;
			head->next.set(tail, false);
		}

		// False only if the slab was dry when this list was built -- see the constructor. Such a
		// list is inert: every operation below returns as if it were empty, so a caller that fails
		// to check gets nothing done rather than undefined behaviour.
		bool ok() const { return head != nullptr; }
		~LockFreeList() {
			if (!head) return;   // never constructed -- see ok()
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
		// APPEND, no key, no search. A Treiber-style push directly after the sentinel head: one CAS,
		// O(1), and it never walks the list.
		//
		// WHY THIS EXISTS RATHER THAN add(). The one user of this container is TaskNode::dependents,
		// which only ever appends and iterates -- it never looks anything up. It used add(), whose
		// key was the dependent's own pointer, purely to DEDUPLICATE edges; that cost a
		// Window::find walk per edge (so wiring a node with k dependents was O(k^2)) and, worse, it
		// was incorrect: AddDependency incremented dependencies_left unconditionally while a
		// duplicate add returned false, so the countdown could never reach zero and Kahn's saw an
		// in-degree that did not match the edges, reporting a false cycle. Duplicate edges are
		// self-consistent WITHOUT dedup -- two entries, two decrements, reaching zero -- so removing
		// it fixes the bug and deletes the search at the same time.
		//
		// A list built with push() must not be used with add/remove/contains: those assume keys are
		// sorted and every node here carries key 0. Nothing mixes them.
		//
		// Returns false only if the slab is exhausted. The caller must check -- add() historically
		// did not, and placement-new over a null slot is the "access violation writing 0x0" that
		// TaskNode's constructor comment already describes.
		bool push(T item) {
			if (!head) return false;   // inert list -- see ok()
			CoroSafeEpochGuard guard;
			void* mem = allocator.Alloc();
			if (!mem) return false;
			LNode<T>* node = new (mem) LNode<T>(0, item);
			node->owner = &allocator;
			while (true) {
				LNodeBase* first = head->next.getReference();
				node->next.set(first, false);
				if (head->next.compareAndSet(first, node, false, false)) return true;
			}
		}

		bool add(uint64_t key, T item) {
			if (!head) return false;   // inert list -- see ok()
			CoroSafeEpochGuard guard;   // RAII: leaves on every return path
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);

				if (curr->key == key) {
					return false;
				}
				void* mem = allocator.Alloc();
				// Slab exhaustion is a NULL here, and placement-new over it is the "access
				// violation writing 0x0" that push()'s comment describes. add() went without this
				// check for its whole life because its only caller could not run the slab dry;
				// Event's waiter index can, since it allocates one cell per parked task.
				if (!mem) return false;
				LNode<T>* node = new (mem) LNode<T>(key, item);
				node->owner = &allocator;   // so slabDeleter can return it on retire
				node->next.set(curr, false);

				if (pred->next.compareAndSet(curr, node, false, false)) {
					return true;
				}
			}
		}
		bool remove(uint64_t key) {
			if (!head) return false;   // inert list -- see ok()
			CoroSafeEpochGuard guard;   // RAII: leaves on every return path
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
			if (!head) return;         // inert list -- see ok()

			CoroSafeEpochGuard guard;  // Ensure we are in an epoch for safe traversal
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
			if (!head) return false;   // inert list -- see ok()
			CoroSafeEpochGuard guard;  // Ensure we are in an epoch for safe traversal
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
			CoroSafeEpochGuard guard;  // Ensure we are in an epoch for safe traversal
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
