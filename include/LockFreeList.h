#pragma once
#define NOMINMAX
#include <vector>
#include <atomic>
#include <iostream>
#include <intrin.h>
#include <limits>
#include <cstdint>
#include "Epochs.h"
#include "TaskScheduler.h"
namespace T_Threads {
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

		static void slabDeleter(void* ptr) {
			auto* node = static_cast<LNode<T>*>(ptr);

			// 1. If you used placement new, explicitly destroy
			node->data.~T();

			// 2. Return the raw memory block to the EXACT allocator that gave it to you
			TaskScheduler::Instance().GetAllocator()->Free(node);
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
		LockFreeList() {
			void* mem = TaskScheduler::Instance().GetAllocator()->Alloc();
			void* mem2 = TaskScheduler::Instance().GetAllocator()->Alloc();
			head = new (mem) LNode<T>(0, T());
			tail = new (mem2) LNode<T>(UINT64_MAX, T());
			head->next.set(tail, false);
		}
		~LockFreeList() {
			TaskScheduler::Instance().GetAllocator()->Free(head);
			TaskScheduler::Instance().GetAllocator()->Free(tail);
		}
		bool add(uint64_t key, T item) {
			EpochManager::Instance().EnterEpoch(thread_id);
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);

				if (curr->key == key) {
					EpochManager::Instance().LeaveEpoch(thread_id); // leave epoch
					return false;
				}
				void* mem = TaskScheduler::Instance().GetAllocator()->Alloc();
				LNode<T>* node = new (mem) LNode<T>(key, item);
				node->next.set(curr, false);

				if (pred->next.compareAndSet(curr, node, false, false)) {
					EpochManager::Instance().LeaveEpoch(thread_id); // leave epoch
					return true;
				}
			}
		}
		bool remove(uint64_t key) {
			EpochManager::Instance().EnterEpoch(thread_id); // enter epoch
			bool snip = false;
			while (true) {
				Window window = Window::find(head, key);
				LNode<T>* pred = static_cast<LNode<T>*>(window.pred);
				LNode<T>* curr = static_cast<LNode<T>*>(window.curr);
				if (curr->key != key) {
					EpochManager::Instance().LeaveEpoch(thread_id); // leave epoch
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
					EpochManager::Instance().LeaveEpoch(thread_id); // leave epoch
					return true;
				}
			}
		}
		template <typename F>
		void for_each(F func) {
			EpochGuard guard(thread_id);  // Ensure we are in an epoch for safe traversal
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
			EpochGuard guard(thread_id);  // Ensure we are in an epoch for safe traversal
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
			EpochGuard guard(thread_id);  // Ensure we are in an epoch for safe traversal
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