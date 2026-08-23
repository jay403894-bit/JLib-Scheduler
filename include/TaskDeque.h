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
#include "platform.h"   // platform::kCacheLine

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
    // The only fields a thief is allowed to vet BEFORE it has claimed a task, carried in the spare
    // low bits of the stored pointer rather than read out of the Task itself.
    //
    // WHY THIS EXISTS. steal_if used to hand the predicate the Task* and let it dereference a task
    // the thief had not claimed yet. That read was safe in OUTCOME -- see the argument above
    // steal_if -- but it was a genuine read of memory whose object lifetime had ended: the owner
    // can pop that task, run it, free it, and have the slab hand the very same slot straight back
    // (the free list is thread-local LIFO, so that recycle is the COMMON case), all while the thief
    // is still reading through its stale pointer. ThreadSanitizer reported it, correctly.
    //
    // Note what does NOT fix it: making Task::corePref/type atomic. The racing write is the
    // CONSTRUCTOR of the next task in that slot, and initialization is not an atomic operation
    // whatever the member's type is -- and the underlying problem is the ended lifetime, which no
    // member type addresses. The only real fix is to stop dereferencing an unclaimed task, which is
    // what this does: the thief now reads bits it got from the DEQUE, published by the same
    // release/acquire protocol that already publishes the pointer.
    //
    // Free, rather than a parallel array: a second array would add a cache line to every push and
    // every steal. These bits ride in the pointer that is already being loaded.
    //
    // SAFE ONLY BECAUSE THE TAG CANNOT GO STALE: both fields are written exclusively by CreateTask,
    // before the task is ever pushed. Nothing mutates them afterwards (checked). If that ever
    // changes, a task's tag and its fields could disagree and stealing would vet against the wrong
    // value -- so a new mutator of either field must re-tag or this scheme breaks quietly.
    struct StealBits {
        CorePref corePref;
        TaskType type;
    };

    class alignas(platform::kCacheLine) TaskDeque {
    public:
        explicit TaskDeque(size_t capacity = 32768)
            : capacity_(capacity),
            mask_(capacity - 1),
            buffer_(new uintptr_t[capacity])
        {
            if ((capacity & (capacity - 1)) != 0)
                throw std::runtime_error("Capacity must be a power of 2");

            for (size_t i = 0; i < capacity; i++)
                buffer_[i] = 0;

            top_.store(0, std::memory_order_relaxed);
            bottom_.store(0, std::memory_order_relaxed);
        }

        ~TaskDeque() {
            delete[] buffer_;
        }

        // ---- tag encoding -------------------------------------------------------------------
        // bits 0-1: CorePref (Default/P/E/Wide are 0..3)   bits 2-3: TaskType
        //
        // TaskType USED TO BE ONE BIT -- `type == Native ? 0x4 : 0` -- back when the enum had two
        // values. 2.8.0 added Coroutine and that encoding silently started LYING: a coroutine task
        // round-tripped as TaskType::Fiber, because "not Native" was the only other thing the bit
        // could say. It was harmless purely by luck (the one predicate reading it asked for
        // == Native, so the lie produced a decline rather than a wrong steal) and would have become
        // a real bug the moment any predicate asked for == Fiber. Two bits, and a static_assert so
        // a fourth TaskType cannot reintroduce it quietly.
        static constexpr uintptr_t kTagMask = 0xF;
        static_assert(alignof(Task) > kTagMask,
            "TaskDeque packs steal-vetting bits into the low bits of a Task*; Task's alignment "
            "must leave them free. Shrinking alignas(Task) breaks this silently.");
        static_assert(static_cast<unsigned>(TaskType::Coroutine) <= 3,
            "TaskType must fit in the deque's two tag bits (2-3); adding a fifth value needs a "
            "wider tag, and alignof(Task) is what limits how wide it can get.");

        static uintptr_t tag(Task* item) {
            const uintptr_t p = reinterpret_cast<uintptr_t>(item);
            return p | (static_cast<uintptr_t>(item->corePref) & 0x3)
                     | ((static_cast<uintptr_t>(item->type) & 0x3) << 2);
        }
        static Task* untag(uintptr_t v) {
            return reinterpret_cast<Task*>(v & ~kTagMask);
        }
        static StealBits bits(uintptr_t v) {
            return StealBits{ static_cast<CorePref>(v & 0x3),
                              static_cast<TaskType>((v >> 2) & 0x3) };
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
            buffer_[b & mask_] = tag(item);
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
                buffer_[(b + i) & mask_] = tag(items[i]);
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
                Task* item = untag(buffer_[b & mask_]);
                if (t == b) {
                    // Last item: race the stealer for it.
                    // seq_cst, matching Le/Pop/Cohen/Zappa Nardelli's verified Chase-Lev (PPoPP
                    // 2013). This was acq_rel from 2026-08-11 to 2026-08-15, on the strength of a
                    // GenMC v0.17.0 run (tests/verify/deque_model.c, ONE OWNER + TWO THIEVES, 174
                    // complete executions) that found acq_rel sufficient. Restored to the paper's
                    // ordering deliberately -- read this before weakening it again.
                    //
                    // The GenMC result is probably CORRECT: the last-element race is a Dekker
                    // pattern, resolved by the two seq_cst FENCES, so this CAS only has to publish
                    // and observe. But note the asymmetry in what that tool proves. Its OTHER
                    // finding -- that deleting the fence above double-claims in under a second -- is
                    // a concrete counterexample, i.e. proof. "No counterexample found" is only
                    // bounded evidence, and the bound here is literally two thieves; this pool runs
                    // 31. Same tool, very different confidence, easy to file both under "verified".
                    //
                    // What decides it is that the weakening buys NOTHING where this runs: on x86-64
                    // acq_rel and seq_cst emit the identical `lock cmpxchg` (checked). It differs
                    // only on AArch64 -- the newest, least-exercised port -- and what it would buy
                    // there is unmeasured, while what it risks is two workers claiming one task:
                    // a double-free that would never be reproducible. If you want it back, measure
                    // the barrier on ARM first and widen the model to more thieves.
                    if (!top_.compare_exchange_strong(
                        t, t + 1,
                        std::memory_order_seq_cst,
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
                Task* item = untag(buffer_[t & mask_]);
                if (top_.compare_exchange_strong(
                    t, t + 1,
                    std::memory_order_seq_cst,   // see pop_bottom: paper ordering, not GenMC's weaker acq_rel
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
        // NO DEREFERENCE OF AN UNCLAIMED TASK. `pred` is handed StealBits decoded from the stored
        // pointer's tag, never the Task itself, so nothing here reads memory the thief does not own.
        //
        // This replaced a version that passed pred the Task* and let it read corePref/type
        // directly. That was safe in OUTCOME and the argument was sound -- CAS success proves top_
        // never moved, so the vetted read was of the live task; any other outcome discards it -- but
        // it was still a read through a pointer whose object lifetime could already have ended. The
        // owner may pop that task, run it, free it, and get the same slab slot straight back (the
        // free list is thread-local LIFO, so the recycle is the COMMON case), all while a thief is
        // mid-read. ThreadSanitizer reported it and was RIGHT to: the racing address was a slab
        // slot, not one of this deque's atomics, so the atomic_thread_fence blind spot did not
        // explain it away.
        //
        // Two dead ends worth recording, because both sound right:
        //   * Making Task::corePref/type ATOMIC does not fix it. The racing write is the
        //     CONSTRUCTOR of the next task in that slot, and initialization is not an atomic
        //     operation whatever the member's type is. The real problem is the ended lifetime,
        //     which no member type addresses.
        //   * "Those fields never change, so the read is harmless" is a non-argument. By the time
        //     of the racing write they belong to a DIFFERENT object; their immutability on the old
        //     one is irrelevant.
        //
        // THE PROTOCOL IS UNCHANGED by tagging -- same indices, same atomics, same fences, same CAS.
        // Only the payload's spare low bits are new, so tests/verify/deque_model.c still describes
        // this algorithm exactly.
        template <class Pred>
        std::optional<Task*> steal_if(Pred&& pred) {
            size_t t = top_.load(std::memory_order_acquire);
            std::atomic_thread_fence(std::memory_order_seq_cst);
            size_t b = bottom_.load(std::memory_order_acquire);

            if (t < b) {
                const uintptr_t slot = buffer_[t & mask_];
                // Vetted from the TAG, never by dereferencing `slot` -- that is the entire point.
                // Everything the predicate needs travels in bits the deque itself owns.
                if (!pred(bits(slot)))
                    return std::nullopt;   // incompatible: leave it for a matching thief, NO claim
                Task* item = untag(slot);
                if (top_.compare_exchange_strong(
                    t, t + 1,
                    std::memory_order_seq_cst,   // see pop_bottom: paper ordering, not GenMC's acq_rel
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
        // TAGGED POINTERS, not plain Task*. See StealBits above for what the low bits carry and
        // why. Task is `alignas(16)`, so bits 0-3 of every Task* in here are guaranteed zero and
        // free to use; kTagMask is asserted against alignof(Task) below so shrinking that alignment
        // cannot silently start corrupting pointers.
        uintptr_t* buffer_;
        const size_t capacity_;
        const size_t mask_;

        alignas(platform::kCacheLine) std::atomic<size_t> top_;
        alignas(platform::kCacheLine) std::atomic<size_t> bottom_;
    };

}
