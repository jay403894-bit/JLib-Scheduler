// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// Cooperative cancellation. C++17, core -- all three execution modes share it.
//
// THE ONE RULE: cancellation is observed at SUSPENSION POINTS, plus voluntarily by POLLING. What
// differs between the modes is only which of those they have:
//
//   Native      no suspension points exist, so polling is the only option:
//                   if (JLib::CurrentTaskCancelled()) return;
//   Fiber       every suspend point is a cancellation point. The wait returns Cancelled and the
//               body unwinds normally -- destructors run, RAII is unaffected.
//   Coroutine   the same, spelled through the awaiter: await_ready short-circuits when already
//               cancelled, await_resume reports it at the co_await.
//
// Go, C#, and Rust all land on cooperative cancellation for the same reason: the alternative is
// destroying a stack asynchronously, which no language can make safe. Nothing here can stop a task
// that is mid-body and not looking.
//
// WAKING AN ALREADY-PARKED WAITER IS IMPLEMENTED as of 3.1.0/3.2.0, and this note used to say it was
// not. Event, SchedulerSemaphore, SchedulerConditionVariable and SchedulerMutex each have an eager
// CancelWaiters that wakes matching waiters with no signal at all. Every one of those paths removes
// a waiter from its structure BEFORE waking it, because the queue entries point into suspended
// stack frames.
//
// SchedulerMutex WAS the exception, on the argument that a binary SchedulerSemaphore(1, 1) already
// is an eagerly cancellable lock and a second lock type would be a worse copy of it. That was
// wrong, and for a reason the argument did not consider: a frame parked on a mutex whose holder is
// itself abandoned cannot be woken by ANYTHING, so its stack never unwinds and nothing it holds is
// released -- RAII, its WaitGroup slot, a hazard record. Teardown, not consistency, is what settled
// it. See SchedulerMutex::CancelWaiters.
//
// EAGER IS A CALL ON THE PRIMITIVE, NOT SOMETHING A SCOPE PERFORMS. Cancelling a scope sets a flag
// that every observation point walks; it does NOT go and find the primitives that scope's tasks are
// parked on, because nothing indexes waiters by scope. A caller that needs a parked waiter out
// right now names the primitive: mutex.CancelWaiters(tok). Same two-phase shape the I/O reactor
// needs, and for the same reason -- see RequestCancel there.
//
// STILL MISSING: timers and timeouts, which is the only real gap. See the CHANGELOG for 3.2.0.
//
// SCOPES NEST. A scope constructed with a parent token is cancelled when it is cancelled OR when
// anything enclosing it is, so ONE token on a task can express several reasons to stop: a timeout
// firing on the operation, or the connection it belongs to going away. That is why a timeout needs
// no deadline field anywhere -- a timer just holds a token and calls CancelVia. Cancellation only
// ever travels DOWN: a request timing out does not close its connection.
//
// TWO BOUNDS WORTH KNOWING. There are kCancelSlots (65,535) LIVE scopes -- one short of what the
// token's 16-bit index could address, see the note there -- and exhaustion FAILS OPEN. And a
// generation is 16 bits, so a handle stale by exactly 65,536 reuses of its slot aliases whatever
// holds it now; the free list is LIFO, so a create/destroy loop drives ONE slot's counter and
// reaches that bound far sooner than a spread-out workload would.

#include <atomic>
#include <cstdint>

namespace JLib {

    class CancelToken;

    namespace detail {
        // A cancellation SCOPE's state. Deliberately not one flag per task: scopes are what get
        // cancelled -- every task in a graph, every operation for a connection -- and tasks
        // reference one.
        struct CancelSlot {
            // GENERATION AND CANCELLED FLAG IN ONE WORD, so they can be read and written together.
            // High 32 bits generation, low bit cancelled.
            //
            // They were two atomics until the parent chain went in, and that was safe only while
            // the sole writer of `cancelled` was the CancelScope that owned the slot. CancelVia
            // breaks that: a timer holds a bare TOKEN, so it must check the generation and then set
            // the flag -- and between those two steps the scope can be destroyed and the slot handed
            // to somebody else, at which point the flag lands on an unrelated scope. Cancelling a
            // random live connection is precisely what the generation exists to prevent, so the
            // check and the write have to be one atomic step. One word makes that a CAS.
            std::atomic<uint64_t> state{ 0 };

            static constexpr uint64_t kCancelledBit = 1;
            static uint32_t GenOf(uint64_t s) noexcept { return uint32_t(s >> 32) & 0xFFFFu; }
            static bool     CancelledIn(uint64_t s) noexcept { return (s & kCancelledBit) != 0; }

            // Enclosing scope, or 0xFFFFFFFF for a root. THE PARENT LINK LIVES HERE, NOT IN Task,
            // and that is the whole design decision: a Task is capped at 64 bytes by static_assert
            // and there are millions of them, while a slot is uncapped and there are at most 65,535.
            // Putting it here also means nesting costs Task nothing at all -- a task still carries
            // exactly one token, its innermost scope, and the chain is walked on the read side.
            //
            // Written once before the owning scope's token is published, then read from other
            // threads; atomic for that, not for contention.
            std::atomic<uint32_t> parent{ 0xFFFFFFFFu };
            // Intrusive free-list link, as a 1-BASED index (0 means end of list). Only touched while
            // this slot is free, so it needs no atomicity of its own -- the publish is the CAS on
            // the list head.
            uint32_t nextFree{ 0 };
        };

        // 16 bytes: 65,535 of them is a ~1 MiB static array. Stated so a change that quietly
        // doubles it -- another atomic, a debug field -- has to argue with this line first.
        static_assert(sizeof(CancelSlot) == 16, "CancelSlot pads the table; keep it 16 bytes");

        // Fixed table, taken from an O(1) intrusive free list -- not the linear CAS scan this
        // once used, which every TaskDAG construction paid for. No second allocator either way.
        // 65,535 AND NOT 65,536, which looks like an off-by-one and is not. The token packs a 16-bit
        // index, and CancelToken::kNone is 0xFFFFFFFF -- index 0xFFFF, generation 0xFFFF. A slot at
        // index 0xFFFF would therefore produce a token IDENTICAL to kNone once every 65,536
        // generations, and that token reads as "unscoped": the task silently stops being
        // cancellable. Stopping one short means 0xFFFF is never a valid index and the collision is
        // unconstructible.
        //
        // WHY THIS BIG. It was 4,096, sized for one scope per frame or per graph. Async I/O wants a
        // scope per connection AND one per in-flight operation, and that is NOT bounded by the fiber
        // pool: coroutines are the vehicle for I/O precisely because they are not fibers, their
        // frames come from the task slab, and there can be hundreds of thousands in flight. 4,096
        // would have been reached by an ordinary server, and exhaustion FAILS OPEN -- the quietest
        // possible failure.
        //
        // The cost is a ~1 MiB zero-initialised static array, which lives in BSS and stays untouched
        // until scopes are actually created.
        inline constexpr uint32_t kCancelSlots = 65535;
        CancelSlot* CancelSlotTable();

        // Resolves a packed handle, returning null if the slot has been recycled since it was
        // issued. That generation check is the whole reason the handle is not a bare pointer.
        CancelSlot* ResolveCancelSlot(uint32_t raw);

        // How far Cancelled() will walk before giving up. Real nesting is shallow -- connection,
        // request, operation is three -- so this is a backstop against a corrupted chain, not a
        // limit anyone should design against. A bounded walk cannot hang a suspend-point check.
        //
        // A CYCLE IS UNCONSTRUCTIBLE ANYWAY: a parent token must already exist when the child takes
        // its slot, so a scope can never become its own ancestor. If a parent is destroyed and its
        // slot recycled, the child's stored handle carries the OLD generation and stops resolving --
        // it does not silently re-point at whatever moved in.
        inline constexpr int kMaxScopeDepth = 8;
    }

    // A 4-byte handle: 16-bit slot index, 16-bit generation. Four bytes because that is exactly
    // what fits the space reclaimed in Task by packing its flags in 2.9.0 -- so carrying a token on
    // every task costs nothing and Task stays one cache line.
    //
    // THE GENERATION IS NOT DECORATION. Slots are recycled; without it a token held by a finished
    // task would read whichever scope inherited the slot, and report someone else's cancellation.
    // 16/16 gives 65,536 live scopes and 65,536 reuses before wrap. If either bound is too small
    // for a workload, re-split the bits -- 20/12 buys a million scopes at 4,096 reuses.
    class CancelToken {
    public:
        static constexpr uint32_t kNone = 0xFFFFFFFFu;

        CancelToken() noexcept = default;
        explicit CancelToken(uint32_t raw) noexcept : raw_(raw) {}

        bool Valid() const noexcept { return raw_ != kNone; }
        uint32_t Raw() const noexcept { return raw_; }

        // False for an absent OR stale token. A task holding a handle to a scope that has since
        // been destroyed is not cancelled -- it is unscoped, which is the safe reading: the
        // alternative would cancel work at random as slots recycle.
        //
        // WALKS TO THE ROOT, so a scope is cancelled when IT was cancelled or when anything
        // enclosing it was. That is what lets one token express several reasons to stop: an
        // operation nested in a connection is cancelled by cancelling the operation (a timeout
        // firing) OR by cancelling the connection (the peer went away), and the task carrying the
        // operation's token sees both. Without the walk a Task would need one token per reason, and
        // it has room for exactly one.
        //
        // Cost on the common path -- an unparented scope -- is one extra relaxed-ish load that
        // reads kNone and stops. This is called at every suspend point, so that matters.
        bool Cancelled() const noexcept {
            uint32_t raw = raw_;
            for (int depth = 0; depth < detail::kMaxScopeDepth; ++depth) {
                if (raw == kNone) return false;
                const uint32_t index = raw & 0xFFFFu;
                if (index >= detail::kCancelSlots) return false;

                detail::CancelSlot* s = &detail::CancelSlotTable()[index];
                // ONE load gets both halves, so the generation this answer is based on is the same
                // generation the flag was read from. Two loads could straddle a recycle and report
                // a new tenant's cancellation against an old handle.
                const uint64_t st = s->state.load(std::memory_order_acquire);

                // Stale: the scope this link named is gone. Stop rather than guess -- the same
                // reading as a stale token itself, and the reason a child may outlive its parent.
                if (detail::CancelSlot::GenOf(st) != (raw >> 16)) return false;
                if (detail::CancelSlot::CancelledIn(st)) return true;
                raw = s->parent.load(std::memory_order_acquire);
            }
            return false;
        }

        // Is this scope `ancestor`, or nested anywhere inside it?
        //
        // WHAT THIS IS FOR, and why Cancelled() is not enough. Cancelled() answers "should this stop"
        // by walking UP for a set flag. This answers "does this belong to that scope" by walking up
        // for an IDENTITY, and the eager cancel paths need the second: CancelWaiters(tok) has to
        // pick out which waiters a scope owns, and a waiter parked under a nested scope is owned by
        // the parent even though its token differs.
        //
        // Matching tokens with == instead is the bug this replaced. It is invisible in testing
        // because cancellation still WORKS -- Cancelled() walks, so pre-checks and skip-at-release
        // are right -- it is only the EAGER wake that silently degrades to lazy. For a wait that may
        // never be released on its own, which is the only reason eager exists, lazy means never.
        //
        // An invalid `ancestor` is false, not "everyone": callers spell "everyone" by not calling
        // this at all, and a stale handle quietly matching everything is the opposite of safe.
        bool IsWithin(CancelToken ancestor) const noexcept {
            if (!ancestor.Valid()) return false;
            uint32_t raw = raw_;
            for (int depth = 0; depth < detail::kMaxScopeDepth; ++depth) {
                if (raw == kNone) return false;
                if (raw == ancestor.raw_) return true;

                const uint32_t index = raw & 0xFFFFu;
                if (index >= detail::kCancelSlots) return false;
                detail::CancelSlot* s = &detail::CancelSlotTable()[index];
                const uint64_t st = s->state.load(std::memory_order_acquire);
                // Stale link: the scope this named is gone, so the chain stops here. Same reading as
                // a stale token itself.
                if (detail::CancelSlot::GenOf(st) != (raw >> 16)) return false;
                raw = s->parent.load(std::memory_order_acquire);
            }
            return false;
        }

    private:
        uint32_t raw_ = kNone;
    };

    // Owns a slot for its lifetime and hands out tokens against it. Cancelling is one-way and
    // idempotent; there is no un-cancel, because a task that already observed the cancellation
    // cannot be un-told.
    //
    // TOKENS MUST NOT OUTLIVE THEIR SCOPE IN USE -- they stay memory-safe (the generation check
    // catches a recycled slot) but they stop reporting. Give the scope a lifetime that covers the
    // work it governs: a frame, a connection, a request.
    class CancelScope {
    public:
        CancelScope() noexcept;

        // NESTED scope. Cancelling `parent` cancels this one and everything under it, without the
        // canceller knowing what those are -- which is the point: a connection cancels its requests,
        // a frame cancels the work it spawned, and nothing has to keep a registry of children.
        //
        // The parent is captured as a TOKEN, not a pointer, so a child outliving its parent is
        // memory-safe: the generation check fails and the child simply stops inheriting. It does
        // not dangle and it does not inherit from whatever takes the slot next.
        //
        // Passing an invalid or stale token gives an ordinary root scope. That is deliberate --
        // failing open matches what an exhausted table already does, and the alternative would be
        // to abort live work because a parent was already gone.
        explicit CancelScope(CancelToken parent) noexcept;

        ~CancelScope();

        CancelScope(const CancelScope&) = delete;
        CancelScope& operator=(const CancelScope&) = delete;

        void Cancel() noexcept;
        bool Cancelled() const noexcept;

        // Invalid if the table was full at construction. A scope that could not get a slot never
        // cancels anything rather than silently cancelling everything -- failing open, because the
        // opposite would abort live work on an unrelated resource shortage.
        CancelToken Token() const noexcept { return CancelToken(raw_); }
        bool Valid() const noexcept { return raw_ != CancelToken::kNone; }

    private:
        uint32_t raw_ = CancelToken::kNone;
    };

    // Cancel a scope you hold only a TOKEN for, rather than the CancelScope object.
    //
    // WHAT THIS IS FOR. A timer cannot own the scope it fires against -- the scope belongs to the
    // operation, and the operation usually finishes FIRST, with the timer still queued. So the timer
    // holds a token, and the generation check makes the late firing a no-op instead of a hazard:
    // the scope is gone, the slot has moved on, the handle stops resolving, and nothing happens.
    // That is what makes "cancel the operation's scope when the deadline expires" implementable
    // without a deadline field in Task and without unregistering timers on the fast path.
    //
    // Returns whether anything was cancelled, so a caller that cares can tell a live scope from a
    // stale handle. Cancelling is one-way and idempotent, so calling twice is harmless.
    bool CancelVia(CancelToken token) noexcept;

} // namespace JLib
