// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <optional>
#include <iostream>
#include <stdexcept>
#include <algorithm>

#include "Task.h"

namespace JLib {

    // Lock-free Chase-Lev work-stealing deque. Owner uses push_bottom/pop_bottom (LIFO, one end);
    // thieves use steal() (FIFO, other end). This is the classic, correct single-item protocol.
    //
    // NO BATCHED STEAL (decided 2026-07-22): a lock-free batch steal is NOT possible here. A batch
    // would claim a range [t, t+n) guarded by one top_ CAS, but the owner's pop_bottom takes from
    // the bottom and does NOT touch top_ for non-last items -- so a batch could double-claim a task
    // the owner also popped (-> use-after-free / double-free: this was a real heisenbug). Making it
    // correct needs either a lock (hot-path cost) or a block-based deque (complex + unverifiable
    // without race testing). Not worth it -- single-item stealing is standard and fast, so there is
    // no steal_batch: callers steal one task at a time via steal().
    class alignas(64) TaskDeque {
    public:
        explicit TaskDeque(size_t capacity = 32768)
            : capacity_(capacity),
            mask_(capacity - 1),
            buffer_(new Task* [capacity])
        {
            if ((capacity & (capacity - 1)) != 0)
                throw std::runtime_error("Capacity must be a power of 2");

            for (size_t i = 0; i < capacity; i++)
                buffer_[i] = nullptr;

            top_.store(0, std::memory_order_relaxed);
            bottom_.store(0, std::memory_order_relaxed);
        }

        ~TaskDeque() {
            delete[] buffer_;
        }

        // Owner-only push.
        bool push_bottom(Task* item) {
            if (!item) {
                std::cerr << "[TaskDeque::push_bottom] ERROR: pushing null item!\n";
                return false;
            }
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);
            if (b - t >= capacity_) {
                return false;  // Full
            }
            buffer_[b & mask_] = item;
            std::atomic_thread_fence(std::memory_order_release);
            bottom_.store(b + 1, std::memory_order_release);
            return true;
        }

        // Owner-only bulk push (owner is the sole producer at the bottom, so this is safe: no
        // stealer ever writes the buffer, only advances top_).
        bool push_bottom_batch(Task** items, size_t count) {
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);

            if ((b + count) - t > capacity_) {
                return false;
            }

            for (size_t i = 0; i < count; ++i) {
                buffer_[(b + i) & mask_] = items[i];
            }

            std::atomic_thread_fence(std::memory_order_release);
            bottom_.store(b + count, std::memory_order_release);
            return true;
        }

        // Owner-only pop (LIFO). Standard Chase-Lev: the last-item race with a stealer is resolved
        // by both sides CASing top_.
        std::optional<Task*> pop_bottom() {
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);

            if (t >= b) {
                return std::nullopt;  // Empty
            }

            b -= 1;
            bottom_.store(b, std::memory_order_release);

            // DO NOT REMOVE. This orders the store to bottom_ above against the load of top_ below
            // -- a StoreLoad pair, the one reordering x86-TSO permits and the one release/acquire
            // does NOT prevent on any architecture. Advice to drop it "because the surrounding
            // operations are already release/acquire" has been offered more than once and is wrong.
            //
            // PROVEN, not argued: deleting it in tests/verify/deque_model.c (-DNO_POP_FENCE) makes
            // GenMC produce an execution in under a second where one item is claimed by both the
            // owner and a thief -- the double-claim use-after-free. x86 cannot show you this.
            std::atomic_thread_fence(std::memory_order_seq_cst);

            t = top_.load(std::memory_order_acquire);

            if (t <= b) {
                Task* item = buffer_[b & mask_];
                if (t == b) {
                    // Last item: race the stealer for it.
                    // acq_rel, not seq_cst. Le/Pop/Cohen/Zappa Nardelli's verified Chase-Lev
                    // (PPoPP 2013) uses seq_cst for the steal-side CAS; MODEL CHECKED with GenMC
                    // v0.17.0 on 2026-08-11 (tests/verify/deque_model.c, one owner + two thieves,
                    // 174 complete executions) and acq_rel is sufficient -- the seq_cst FENCES on
                    // both sides carry the store-load ordering, so the CAS strength is not what
                    // makes this correct. seq_cst here would cost a stronger barrier on ARM64
                    // (casal vs casa) for nothing.
                    if (!top_.compare_exchange_strong(
                        t, t + 1,
                        std::memory_order_acq_rel,
                        std::memory_order_relaxed))
                    {
                        // Stealer won.
                        bottom_.store(b + 1, std::memory_order_relaxed);
                        return std::nullopt;
                    }
                    // Owner won.
                    bottom_.store(b + 1, std::memory_order_relaxed);
                }
                return item;
            }
            else {
                // Empty.
                bottom_.store(t, std::memory_order_relaxed);
                return std::nullopt;
            }
        }

        // Thief: takes the OLDEST task. Claims exactly ONE item and resolves the sole collision with
        // pop_bottom (the last item) via the top_ CAS -- the only correct steal in this deque.
        std::optional<Task*> steal() {
            size_t t = top_.load(std::memory_order_acquire);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            size_t b = bottom_.load(std::memory_order_acquire);

            if (t < b) {
                Task* item = buffer_[t & mask_];
                if (top_.compare_exchange_strong(
                    t, t + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed))
                {
                    return item;
                }
            }
            return std::nullopt;
        }

        // PREDICATED steal: vet the candidate BEFORE claiming it, in one operation. Exists because
        // separate peek-then-steal is a TOCTOU race (between the peek and the steal-CAS another thief
        // can advance top_, and the steal would then claim a DIFFERENT, unvetted task) -- and because
        // the alternative, steal-then-Requeue on a mismatch, is pure contention churn (a CAS claim +
        // a full re-push + a notify, to move a task nowhere). Protocol is IDENTICAL to steal() with
        // the predicate evaluated between the buffer read and the CAS: pred false -> return nullopt
        // WITHOUT CASing (task stays put for a compatible thief); pred true -> CAS claims exactly the
        // slot that was vetted.
        //
        // SAFETY of the pre-CAS dereference (pred reads fields of a task we don't own yet): (1) Tasks
        // live in TaskAllocator slab slots that are NEVER unmapped, so the loads can't fault -- worst
        // case they read a recycled slot's bytes. (2) If the CAS then SUCCEEDS, top_ never moved, so
        // no consumer claimed slot t in between (the owner only touches slot t when it's the LAST item,
        // and resolves that via this same top_ CAS) -- i.e. CAS success PROVES the vetted read was of
        // the live, still-queued task. (3) If pred declines or the CAS fails, no claim occurred and
        // the (possibly stale) read influenced nothing but a skipped steal -- benign by construction.
        template <class Pred>
        std::optional<Task*> steal_if(Pred&& pred) {
            size_t t = top_.load(std::memory_order_acquire);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            size_t b = bottom_.load(std::memory_order_acquire);

            if (t < b) {
                Task* item = buffer_[t & mask_];
                if (!pred(item))
                    return std::nullopt;   // incompatible: leave it for a matching thief, NO claim
                if (top_.compare_exchange_strong(
                    t, t + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed))
                {
                    return item;
                }
            }
            return std::nullopt;
        }

        size_t size() const {
            size_t t = top_.load(std::memory_order_acquire);
            size_t b = bottom_.load(std::memory_order_acquire);
            return (b > t) ? (b - t) : 0;
        }

        size_t capacity() const {
            return capacity_;
        }
        bool empty() const {
            size_t t = top_.load(std::memory_order_acquire);
            size_t b = bottom_.load(std::memory_order_acquire);
            return t >= b;
        }
    private:
        Task** buffer_;
        const size_t capacity_;
        const size_t mask_;

        alignas(64) std::atomic<size_t> top_;
        alignas(64) std::atomic<size_t> bottom_;
    };

}
