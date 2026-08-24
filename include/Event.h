// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
// Fiber.h, not TaskScheduler.h: all this needs is Task and Fiber, and Fiber.h brings Task.h with
// it. Depending on TaskScheduler.h created a cycle that forced TaskScheduler.h to forward-declare
// Event -- so GetEvent() returned a reference callers could not use without separately including
// this file. Breaking the cycle lets TaskScheduler.h include Event.h like it already includes
// DirectEvent.h, and the papercut goes away.
#include "Fiber.h"
#include "platform.h"   // CountTrailingZeros64 for the occupancy scan
namespace JLib {
    // Named rendezvous point: any number of fibers park on it; a signal wakes them all, or one.
    //
    // LOCK-FREE AND ALLOCATION-FREE. It has not always been either: this began as a std::mutex
    // around an unordered_set, so every suspend allocated a hash node and every signal took a lock
    // an external thread (a GPU-fence callback, say) might contend. Both are long gone, and keeping
    // them gone is a standing constraint on anything added here.
    //
    // MEMBERSHIP IS AN OPEN TABLE INDEXED BY FIBER, because the keys are not arbitrary and a
    // perfect hash already exists:
    //
    //   1. Events are FIBER-ONLY. A waiter is resumed through t->assignedFiber, so every waiter has
    //      one. Coroutine tasks never park on an Event, and Native tasks cannot suspend at all.
    //   2. Fibers live in the global pool's std::vector<Fiber>, reserve()d and leaked, so each has
    //      a dense index stable for the life of the program -- Fiber::poolIndex.
    //   3. A fiber is parked on AT MOST ONE event at a time, because parking is what the fiber is
    //      doing.
    //
    // Together those make fiber index -> slot collision-free BY CONSTRUCTION, not merely rarely
    // colliding. Registering and taking a waiter are each a single store to a known slot: no
    // hashing, no probing, no chain, no tombstone, and NO EBR AT ALL, since nothing is ever freed.
    //
    // THIS REPLACED A TREIBER STACK threaded through Task::nextWaiter, which was correct (and
    // model-checked -- tests/verify/event_model.c is kept for that argument) but INTRUSIVE, and
    // that was the ceiling. Its links WERE the tasks, so no waiter could ever leave early: a task
    // freed back to the slab while the stack still held its address as a link meant the next drain
    // walked a recycled slot. That forbade removing one specific waiter, which forbade SignalOne,
    // and made cancellation mark-only. A table indexed by fiber has no such constraint -- nothing
    // threads through the element -- so a waiter can simply be cleared from a slot.
    //
    // Retiring it also collapsed TWO structures into one. While both existed, every operation had
    // to keep them in step in a specific order (slot before bit, bit before stack push, bit cleared
    // before slot, slot before resume), and that was a live source of subtle ordering bugs. And it
    // returned 8 bytes to Task, which sits on a hard 64-byte budget.
    //
    // THE OCCUPANCY BITMAP is what keeps enumeration from paying for the table's sparseness.
    // Direct indexing makes register and take O(1), but SignalAll and CancelWaiters must ENUMERATE,
    // and a bare slot array does not know which slots are live -- three waiters in a 1,024-slot
    // table would mean touching 1,024 pointers to find them. One bit per slot turns that into 16
    // words: scan, and pop set bits with CountTrailingZeros64.
    //
    // MODEL CHECKED, GenMC v0.17.0, 2026-08-24 (tests/verify/event_table_model.c, two pushers + a
    // concurrent SignalAll + a concurrent SignalOne, 36 complete executions): publication is
    // correctly ordered and a waiter is claimed exactly once. Three negative controls all fail.
    //
    //   THE ONE THING TO KNOW BEFORE EDITING SignalAll OR SignalOne. The model found that the two
    //   arbiters here are not independent, and the SLOT EXCHANGE IS THE STRONGER ONE: only one
    //   thread can exchange a non-null Task* out of a slot, so every double claim degenerates into
    //   the loser reading null. All three negative controls -- including one written specifically
    //   to force a double count -- land on that null instead. So the `if (t)` guard after the
    //   exchange is NOT defensive padding, it IS the arbitration that guarantees wake-once. The bit
    //   claim keeps the bitmap itself honest; it is not what makes the wake exclusive. Do not
    //   "simplify" that null check away.
    //
    // THE PROPERTY THIS GAVE UP relative to the stack, stated because it is invisible until
    // something depends on it: the stack took every waiter in ONE exchange and so had a single
    // linearization point. A word scan does not, so a waiter registering into an already-scanned
    // word mid-scan is not woken by that signal, while one registering into a later word is. The
    // guarantee callers actually rely on is weaker and still holds -- REGISTERED BEFORE THE SIGNAL
    // BEGAN IMPLIES WOKEN, since every word is scanned. WaitOnEventArmed depends on exactly that
    // and no more: it registers, then arms whatever can signal, so the signal cannot begin first.
    class Event {
    private:
        // The cost is paid per EVENT rather than per WAITER: one pointer plus one bit per fiber in
        // the pool, so ~8 KB at a 1,024-fiber budget. Allocated LAZILY, so an event that is only
        // ever signalled -- or never used at all -- costs one pointer.
        //
        // THE SCAN IS O(f/64) REGARDLESS OF WAITER COUNT, which is only acceptable because f is
        // bounded and small -- structurally, not by convention. Every fiber carries a reserved
        // stack (64 KB standard, 512 KB heavy), and the default budget is 64 + 8 per worker, so a
        // worker costs 8 MB of stack. That puts the bitmap at 8 bytes per 4 MB of stack:
        //
        //     4 workers ->   288 fibers ->  5 words (40 B)  vs  32 MB of stacks
        //    16 workers -> 1,152 fibers -> 18 words (144 B) vs 128 MB of stacks
        //    64 workers -> 4,608 fibers -> 72 words (576 B) vs 512 MB of stacks
        //
        // Reaching even a single 4 KB page of bitmap takes 32,768 fibers, or 4 GB of reserved fiber
        // stacks. SetFiberBudget cannot realistically get there.
        //
        // If that ever stops holding, the fix is a summary word (one bit per nonzero word), taking
        // the scan to O(waiters + f/4096). It is NOT done here because maintaining it costs the
        // TAKE path -- which runs per waiter on every SignalAll -- a word-empty test, a summary
        // clear, and a re-test to close the race against a concurrent register. Three atomics added
        // to a hot path to speed up a rare one is the wrong trade at any f the table above covers.
        //
        // A sparse set (dense waiter array + sparse[fiber] -> dense position) would give true
        // O(waiters) enumeration and was rejected: removal is swap-with-last, a multi-word
        // compaction that moves an entry another thread may be indexing through concurrently.
        struct WaiterTable {
            std::size_t fiberCount = 0;      // slots; also the exact cap on concurrent waiters
            std::size_t words = 0;           // ceil(fiberCount / 64)
            std::atomic<Task*>* slots = nullptr;
            std::atomic<std::uint64_t>* occupied = nullptr;

            ~WaiterTable() { delete[] slots; delete[] occupied; }
        };

        // ONE pointer publishes the whole table, which is why there is no publish-order hazard
        // here. An earlier shape stored the array and its length in two separate atomics, and then
        // the order mattered in a way that was easy to get backwards: a reader that loaded the
        // array first could see it live with a length of zero and silently skip indexing its
        // waiter. A single acquire load of a fully-built table cannot be torn like that.
        std::atomic<WaiterTable*> table{ nullptr };

        WaiterTable* EnsureTable();

        // Take the waiter in slot i, whose bit this thread has just won. Returns null if another
        // claimer got there first -- see the arbitration note in the class comment.
        static Task* TakeSlot(WaiterTable* tb, std::size_t i) {
            // exchange rather than load-then-store: once taken, the fiber can be resumed, complete,
            // and re-park at this same index, so there must be no window between the two.
            return tb->slots[i].exchange(nullptr, std::memory_order_acq_rel);
        }

        static void WakeOne(Task* t, Task** hi, std::size_t& nhi, Task** lo, std::size_t& nlo,
                            std::size_t cap) {
            // Handles the WANTS_SUSPEND/SUSPENDED race exactly as Resume() does; true means this
            // call won the SUSPENDED -> READY transition and now owns re-queueing the task.
            if (!t->assignedFiber->ResumeQueueless()) return;
            if (t->hiPri) {
                hi[nhi++] = t;
                if (nhi == cap) { RequeueResumedBatch(hi, nhi, true); nhi = 0; }
            } else {
                lo[nlo++] = t;
                if (nlo == cap) { RequeueResumedBatch(lo, nlo, false); nlo = 0; }
            }
        }

    public:
        Event() = default;
        ~Event() { delete table.load(std::memory_order_acquire); }

        Event(const Event&) = delete;
        Event& operator=(const Event&) = delete;

        // Build the waiter table if it does not exist yet. MUST SUCCEED BEFORE A CALLER PARKS.
        //
        // This is separate from AddWaiter because of an ordering the caller cannot reorder: a fiber
        // must become parkable (WANTS_SUSPEND) BEFORE it registers, so a racing signal sees a
        // resumable state. That leaves no safe point at which AddWaiter could fail -- the fiber is
        // already committed to suspending, and a waiter that cannot be registered is not merely
        // uncancellable (as it was when the Treiber stack was the real membership), it is LOST, and
        // the task hangs forever. So allocation happens here, first, where failing is still just an
        // error return. See TaskScheduler::WaitOnEvent.
        bool Reserve() { return EnsureTable() != nullptr; }

        // Register a waiter. Does NOT touch fiber status -- WaitOnEvent has already put the fiber
        // in WANTS_SUSPEND before calling this. Infallible by contract: Reserve() must have
        // returned true first, and poolIndex is always < fiberCount because the table is sized from
        // the same pool the fiber came out of.
        //
        // SLOT FIRST, THEN BIT. The bit is what a scan trusts, so it must not become visible before
        // the pointer it advertises. The release on the fetch_or is what publishes the slot store to
        // a claimer's acquiring RMW; dropping it is a modelled negative control that fails.
        void AddWaiter(Task* t) {
            WaiterTable* tb = table.load(std::memory_order_acquire);
            if (!tb || !t->assignedFiber) return;
            const std::size_t i = t->assignedFiber->poolIndex;
            if (i >= tb->fiberCount) return;

            tb->slots[i].store(t, std::memory_order_release);
            tb->occupied[i >> 6].fetch_or(std::uint64_t(1) << (i & 63),
                                          std::memory_order_release);
        }

        // Marks every task currently waiting here as cancelled. DOES NOT WAKE THEM. Cancellation is
        // an outcome observed where the task is next touched, not an unwind: the task stays parked,
        // a later signal resumes it, and the worker sees the flag at pickup.
        //
        // Reads the slot WITHOUT clearing it and WITHOUT clearing the bit -- this is the one walker
        // that is not a claimer. The null check still matters: a concurrent take clears the bit and
        // then the slot, so the two disagree for an instant.
        void CancelWaiters() {
            WaiterTable* tb = table.load(std::memory_order_acquire);
            if (!tb) return;
            for (std::size_t w = 0; w < tb->words; ++w) {
                std::uint64_t bits = tb->occupied[w].load(std::memory_order_acquire);
                while (bits) {
                    const unsigned b = platform::CountTrailingZeros64(bits);
                    bits &= bits - 1;
                    if (Task* t = tb->slots[(w << 6) + b].load(std::memory_order_acquire))
                        t->cancelledDirect = 1;
                }
            }
        }

        // Wake everyone waiting on this event.
        //
        // One exchange per WORD, which claims every waiter in that word at once. Resumable fibers
        // are COLLECTED and re-queued in a batch rather than one at a time: each individual
        // re-queue is a placement + inbox push + seq_cst flag + condition-variable signal, and a
        // broadcast that wakes 64 fibers paid all of that 64 times, serially, on whichever thread
        // called SignalAll. Measured elsewhere: single Push ~1 M/s, PushBatch ~12 M/s.
        //
        // Split by priority because PushBatch takes ONE priority for the whole run -- merging them
        // would silently demote a hiPri fiber. Buffers are fixed and flushed when full, so this
        // allocates nothing on a path that may run from any thread, including a signaller that is
        // not a worker.
        void SignalAll() {
            WaiterTable* tb = table.load(std::memory_order_acquire);
            if (!tb) return;

            constexpr std::size_t kBuf = 64;
            Task* lo[kBuf]; std::size_t nlo = 0;
            Task* hi[kBuf]; std::size_t nhi = 0;

            for (std::size_t w = 0; w < tb->words; ++w) {
                std::uint64_t bits = tb->occupied[w].exchange(0, std::memory_order_acq_rel);
                while (bits) {
                    const unsigned b = platform::CountTrailingZeros64(bits);
                    bits &= bits - 1;
                    // The null check IS the arbitration -- see the class comment. Do not remove it.
                    if (Task* t = TakeSlot(tb, (w << 6) + b))
                        WakeOne(t, hi, nhi, lo, nlo, kBuf);
                }
            }
            RequeueResumedBatch(hi, nhi, true);
            RequeueResumedBatch(lo, nlo, false);
        }

        // Wake AT MOST ONE waiter. Returns whether one was woken.
        //
        // Losing a claim is not failure -- it means another signaller took that waiter -- so this
        // moves to the next candidate bit rather than giving up. Both the bit claim and the slot
        // exchange can be lost independently, and both are retried past.
        //
        // The word is re-read each pass rather than trusting the scan snapshot: bits set after the
        // load are legitimate candidates, and a stale snapshot would report "nobody waiting" while
        // a waiter sat in the table.
        bool SignalOne() {
            WaiterTable* tb = table.load(std::memory_order_acquire);
            if (!tb) return false;

            for (std::size_t w = 0; w < tb->words; ++w) {
                std::uint64_t bits = tb->occupied[w].load(std::memory_order_acquire);
                while (bits) {
                    const unsigned b = platform::CountTrailingZeros64(bits);
                    const std::uint64_t m = std::uint64_t(1) << b;

                    const std::uint64_t old =
                        tb->occupied[w].fetch_and(~m, std::memory_order_acq_rel);
                    if (old & m) {
                        if (Task* t = TakeSlot(tb, (w << 6) + b)) {
                            if (t->assignedFiber->ResumeQueueless())
                                RequeueResumedBatch(&t, 1, t->hiPri);
                            return true;
                        }
                    }
                    bits &= ~m;                       // someone else took it; try the next
                }
            }
            return false;
        }
    };
};
