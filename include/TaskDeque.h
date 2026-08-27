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
    private:
        // ONE IMMUTABLE OBJECT PER GENERATION -- slots and mask together, written once before it is
        // published and never mutated after, so reading r->mask after loading r is automatically
        // consistent with r->slots. Declared FIRST because grow() and MakeRing() name it. See the
        // note further down for why two independent atomics would be a bug rather than a style choice.
        struct Ring {
            size_t                  mask;
            size_t                  capacity;
            std::atomic<uintptr_t>* slots;
        };

    public:
        explicit TaskDeque(size_t capacity = 32768) {
            if ((capacity & (capacity - 1)) != 0)
                throw std::runtime_error("Capacity must be a power of 2");
            ring_.store(MakeRing(capacity), std::memory_order_relaxed);
            top_.store(0, std::memory_order_relaxed);
            bottom_.store(0, std::memory_order_relaxed);
        }

        ~TaskDeque() {
            for (Ring* r : retired_) { delete[] r->slots; delete r; }
            Ring* r = ring_.load(std::memory_order_relaxed);
            delete[] r->slots;
            delete r;
        }


        // ---- growth -------------------------------------------------------------------------
        //
        // MODEL-CHECKED BEFORE IT WAS WRITTEN. tests/verify/deque_grow_model.c, GenMC v0.17.0:
        // 210 complete executions, no errors, with three negative controls that all fail as they
        // must. Read that file before changing anything below -- each line here corresponds to one
        // of its claims.
        static constexpr size_t kMaxCapacity = 1u << 22;   // 4M slots; see grow()

        // HOW MANY TIMES ANY DEQUE HAS GROWN. Process-wide and incremented only inside grow(), so it
        // is free on every path that matters. It exists because a test asserting "no task was lost"
        // cannot tell growth working from a deque that never filled -- the same vacuity that made
        // the first version of tests/deque_overflow_test.cpp pass with its mechanism removed.
        // A C++17 INLINE VARIABLE, not a function-local static. The function-local version compiled
        // and linked, and the test read a DIFFERENT object from the one the library incremented --
        // it reported 0 grows on a run that had grown. An inline data member is one object across
        // every translation unit by the standard, which is the property actually needed here.
        static inline std::atomic<size_t> g_growCount{ 0 };
        static size_t GrowCount() { return g_growCount.load(std::memory_order_relaxed); }

        static Ring* MakeRing(size_t capacity) {
            Ring* r = new Ring{ capacity - 1, capacity, new std::atomic<uintptr_t>[capacity] };
            for (size_t i = 0; i < capacity; ++i)
                r->slots[i].store(0, std::memory_order_relaxed);
            return r;
        }

        // OWNER ONLY, called from push_bottom having already observed the deque full with the same
        // b and t the push will use. Returns false if it cannot grow, and the caller then falls
        // back exactly as it did before.
        //
        // top_ AND bottom_ ARE NOT TOUCHED. That is what leaves the existing Chase-Lev proof
        // intact: the CAS on top_ stays the sole arbiter of who owns a slot, and this only changes
        // WHERE that slot lives. Copying by LOGICAL index is what makes the two rings agree -- a
        // thief reading old->slots[t & oldMask] and one reading new->slots[t & newMask] read the
        // SAME VALUE -- so a grow racing a steal cannot change WHICH task is claimed.
        bool grow(Ring* old, size_t t, size_t b) {
            const size_t newCap = old->capacity * 2;

            // A CEILING, DELIBERATELY, and the reason the overflow lane still exists. Doubling
            // without a bound turns an infinitely self-spawning task from a diagnosable failure
            // into an OOM with no message. 4M slots is 32MB of pointers on one lane, which no real
            // workload reaches; past it the caller overflows and the assert there names the cause.
            if (newCap > kMaxCapacity) return false;

            Ring* r = nullptr;
            try {
                r = MakeRing(newCap);
            }
            catch (const std::bad_alloc&) {
                // NOTHING HAS BEEN PUBLISHED, so the deque is still entirely on `old` and the
                // caller's fallback is correct. Refusing to grow is not an error state.
                return false;
            }

            // COPY BY LOGICAL INDEX. i is the absolute index, so the same i lands at a different
            // physical slot under the new mask while holding the same value.
            for (size_t i = t; i != b; ++i)
                r->slots[i & r->mask].store(old->slots[i & old->mask].load(std::memory_order_relaxed),
                                            std::memory_order_relaxed);

            // RELEASE. A thief that acquires this pointer must see the copy above; -DNO_PUBLISH_RELEASE
            // in the model is exactly this store weakened, and it is a safety violation.
            ring_.store(r, std::memory_order_release);

            // RETIRE, NOT DELETE. A thief may still be reading through `old`. Kept until this deque
            // dies -- see the note on retired_ for why that beats reclaiming it properly here.
            retired_.push_back(old);
            g_growCount.fetch_add(1, std::memory_order_relaxed);
            return true;
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

            // RELAXED IS RIGHT FOR THE OWNER: it is the only writer of ring_, so it cannot read a
            // stale one. Thieves load it acquire, pairing with grow's releasing store.
            Ring* r = ring_.load(std::memory_order_relaxed);
            if (b - t >= r->capacity) {
                // GROW INSTEAD OF REFUSING. Only when it declines -- past kMaxCapacity, or the
                // allocation failed -- does the caller still see false and take its fallback.
                if (!grow(r, t, b)) return false;   // Full and cannot grow
                r = ring_.load(std::memory_order_relaxed);
            }
            r->slots[b & r->mask].store(tag(item), std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_release);
            bottom_.store(b + 1, std::memory_order_release);
            return true;
        }

        // Owner-only bulk push (owner is the sole producer at the bottom, so this is safe: no
        // stealer ever writes the buffer, only advances top_).
        bool push_bottom_batch(Task** items, size_t count) {
            size_t b = bottom_.load(std::memory_order_relaxed);
            size_t t = top_.load(std::memory_order_acquire);

            Ring* r = ring_.load(std::memory_order_relaxed);
            while ((b + count) - t > r->capacity) {
                // A LOOP, NOT AN IF: one doubling may not cover a whole batch, where a single push
                // only ever needs one. grow() returning false terminates it, so this cannot spin.
                if (!grow(r, t, b)) return false;
                r = ring_.load(std::memory_order_relaxed);
            }

            for (size_t i = 0; i < count; ++i) {
                r->slots[(b + i) & r->mask].store(tag(items[i]), std::memory_order_relaxed);
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
                Ring* r = ring_.load(std::memory_order_relaxed);
                Task* item = untag(r->slots[b & r->mask].load(std::memory_order_relaxed));
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
                // ACQUIRE, AND RELOADED EVERY ATTEMPT: ring_ is no longer loop-invariant once the
                // deque can grow, and this pairs with grow's releasing store.
                Ring* r = ring_.load(std::memory_order_acquire);
                Task* item = untag(r->slots[t & r->mask].load(std::memory_order_relaxed));
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
                // Same reload rule as steal() -- see there.
                Ring* r = ring_.load(std::memory_order_acquire);
                const uintptr_t slot = r->slots[t & r->mask].load(std::memory_order_relaxed);
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

        // THE CURRENT capacity, which now changes: the deque doubles rather than refusing a push.
        // Any caller caching this is wrong -- there are none, and there should stay none.
        size_t capacity() const {
            return ring_.load(std::memory_order_acquire)->capacity;
        }
        bool empty() const {
            size_t t = top_.load(std::memory_order_acquire);
            size_t b = bottom_.load(std::memory_order_acquire);
            return t >= b;
        }
    private:
        // ONE IMMUTABLE OBJECT PER GENERATION, holding the slots and the mask together. This shape
        // is not tidiness -- it is what tests/verify/deque_grow_model.c's -DSPLIT_PTR_MASK control
        // proves is required. Store the pointer and the mask as two independent atomics and a thief
        // can load one from each generation and index off the end of the older array; GenMC reports
        // it immediately. A Ring is written once, before it is published, and never mutated after,
        // so reading r->mask after loading r is automatically consistent with r->slots.
        //
        // TAGGED POINTERS, not plain Task*. See StealBits above for what the low bits carry and
        // why. Task is `alignas(16)`, so bits 0-3 of every Task* in here are guaranteed zero and
        // free to use; kTagMask is asserted against alignof(Task) below so shrinking that alignment
        // cannot silently start corrupting pointers.
        //
        // SLOTS ARE ATOMIC AND ACCESSED RELAXED, matching the verified Chase-Lev of Le, Pop, Cohen
        // and Zappa Nardelli, and it is NOT ceremony. The owner pushing at b writes the same
        // PHYSICAL slot a thief reads at a stale t whenever their logical indices are congruent mod
        // capacity. The thief discards the value when its CAS fails, so it is benign in OUTCOME --
        // but with a plain uintptr_t it is a data race on a non-atomic object, which is UB, and
        // GenMC reports it as one the moment a model lets the owner push concurrently with a thief.
        // deque_model.c never did (its owner thread only pops), which is why this stood so long.
        // Relaxed atomics generate identical code to plain loads and stores on x86-64 and AArch64.

        std::atomic<Ring*> ring_;

        // RETIRED RINGS ARE KEPT, NOT FREED, until this deque is destroyed. That is a deliberate
        // choice over epoch or hazard reclamation, and it is cheaper in the place that matters:
        //
        //   FREEING AT GROW IS UNSAFE. A thief that loaded the old Ring before the publish is still
        //   reading through it. deque_grow_model.c's -DNO_RETIRE control is exactly this, and it is
        //   a safety violation.
        //
        //   RECLAIMING IT PROPERLY WOULD PUT A GUARD ON THE STEAL PATH. An EpochGuard per steal
        //   attempt is real cost on the hottest loop in the library, paid on every attempt to make
        //   a once-in-a-process event safe.
        //
        //   KEEPING THEM IS BOUNDED AND TINY. Capacity doubles, so every retired ring together is
        //   smaller than the live one -- the waste is under 2x, and only for a deque that actually
        //   grew. The original Chase-Lev paper does the same.
        //
        // OWNER-ONLY, and never read by anyone else, so it needs no synchronisation of its own.
        std::vector<Ring*> retired_;

        alignas(platform::kCacheLine) std::atomic<size_t> top_;
        alignas(platform::kCacheLine) std::atomic<size_t> bottom_;
    };

}
