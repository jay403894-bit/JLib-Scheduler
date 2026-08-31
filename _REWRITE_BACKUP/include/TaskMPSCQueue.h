// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <type_traits>
#include "Task.h"
#include "platform.h"   // platform::kCacheLine
#include "TaskAllocator.h"

namespace JLib {
	//vyokov-style intrusive MPSCqueue for Task pointers
    class alignas(platform::kCacheLine)TaskMPSCQueue {
        static_assert(std::is_pointer<Task*>::value, "MPSCQueue<T> expects a pointer type");

        std::atomic<Task*> head_;
        Task*              tail_;
        Task*              stub_;
        // Remembered so the destructor can return `stub_` to the arena it actually came from.
        // See the destructor for why that is not optional.
        TaskAllocator*     alloc_;

        void append(Task* n) {
            n->next.store(nullptr, std::memory_order_relaxed);
            Task* prev = head_.exchange(n, std::memory_order_acq_rel);
            prev->next.store(n, std::memory_order_release);
        }

    public:
        // Zero-initialized rather than left indeterminate. The default ctor used to leave head_,
        // tail_ and stub_ as garbage, which has already cost one access violation (mainQ was
        // constructed but never init()'d, so append() wrote through a garbage head_ -- see the
        // mainQ.init() note in TaskScheduler::StartPool). A null stub_ is also what lets the
        // destructor below tell an initialized queue from one that never was.
        TaskMPSCQueue() : head_(nullptr), tail_(nullptr), stub_(nullptr), alloc_(nullptr) {}

        // `stub_` COMES FROM THE SLAB, so it goes back to the slab.
        //
        // This was `::delete stub_;`, and the `::` is the whole bug: it forces the GLOBAL
        // deallocation function, stepping past Task's own operator delete (which exists only to
        // assert). So a TaskAllocator slot was handed to the CRT heap -- immediate
        // STATUS_HEAP_CORRUPTION (0xC0000374) for anyone who ever destroyed one of these.
        //
        // It went unnoticed because nothing in the library destroys a TaskMPSCQueue: Init() does
        // `instance = new TaskScheduler(...)` with no matching delete, so ~TaskScheduler -- and
        // with it the inbox vectors -- never runs. A test harness that stack-allocates one hits it
        // on the first run.
        ~TaskMPSCQueue() {
            if (!stub_) return;          // constructed but never init()'d: nothing to unwind
            clear();
            stub_->~Task();
            if (alloc_) alloc_->Free(stub_);
            stub_ = nullptr;
        }

        void init(TaskAllocator* allocator) {
            void* mem = allocator->Alloc();
            stub_ = ::new (mem) Task();
            stub_->next.store(nullptr, std::memory_order_relaxed);
            head_.store(stub_, std::memory_order_relaxed);
            tail_ = stub_;
            alloc_ = allocator;
        }
        void push(Task* task) { append(task); }

        void push_batch(Task*head_batch, Task*tail_batch) {
            tail_batch->next.store(nullptr, std::memory_order_relaxed);
            Task* prev = head_.exchange(tail_batch, std::memory_order_acq_rel);
            prev->next.store(head_batch, std::memory_order_release);
        }

        bool pop(Task*& out) {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);

            if (tail == stub_) {
                if (!next) return false;            
                tail_ = next;                      
                tail = next;
                next = next->next.load(std::memory_order_acquire);
            }

            if (next) {                              
                tail_ = next;
                out = static_cast<Task*>(tail);
                return true;
            }

        
            if (tail != head_.load(std::memory_order_acquire)) {
                return false;
            }

            append(stub_);
            next = tail->next.load(std::memory_order_acquire);
            if (next) {
                tail_ = next;
                out = static_cast<Task*>(tail);
                return true;
            }
            return false;
        }

        void clear() {
            Task*tmp;
            while (pop(tmp)) { }
        }

        bool empty() const {
            Task* tail = tail_;
            Task* next = tail->next.load(std::memory_order_acquire);
            return (tail == stub_ && next == nullptr);
        }
    };
}
