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
// not. Event, SchedulerSemaphore and SchedulerConditionVariable each have an eager CancelWaiters
// that wakes matching waiters with no signal at all. SchedulerMutex deliberately does not: a binary
// SchedulerSemaphore(1, 1) already is an eagerly cancellable lock, and a second lock type would be a
// worse copy of it. Every one of those paths removes a waiter from its structure BEFORE waking it,
// because the queue entries point into suspended stack frames.
//
// STILL MISSING: timers and timeouts, which is the only real gap. See the CHANGELOG for 3.2.0.
//
// TWO BOUNDS WORTH KNOWING. There are kCancelSlots (4,096) LIVE scopes, not the 65,536 the token's
// 16-bit index could address -- and exhaustion fails open, so a workload that keeps a scope per
// connection must size this deliberately. And a generation is 16 bits, so a handle stale by exactly
// 65,536 reuses of its slot aliases whatever holds it now; the free list is LIFO, so a create/destroy
// loop drives ONE slot's counter and reaches that bound far sooner than a spread-out workload would.

#include <atomic>
#include <cstdint>

namespace JLib {

    class CancelToken;

    namespace detail {
        // A cancellation SCOPE's state. Deliberately not one flag per task: scopes are what get
        // cancelled -- every task in a graph, every operation for a connection -- and tasks
        // reference one.
        struct CancelSlot {
            std::atomic<uint32_t> cancelled{ 0 };
            std::atomic<uint32_t> generation{ 0 };   // bumped on release; defeats stale handles
            // Intrusive free-list link, as a 1-BASED index (0 means end of list). Only touched while
            // this slot is free, so it needs no atomicity of its own -- the publish is the CAS on
            // the list head.
            uint32_t nextFree{ 0 };
        };

        // Fixed table. Scope creation is not a hot path (one per graph, per connection, per
        // request), so a linear CAS scan is fine and avoids another allocator.
        inline constexpr uint32_t kCancelSlots = 4096;
        CancelSlot* CancelSlotTable();

        // Resolves a packed handle, returning null if the slot has been recycled since it was
        // issued. That generation check is the whole reason the handle is not a bare pointer.
        CancelSlot* ResolveCancelSlot(uint32_t raw);
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
        bool Cancelled() const noexcept {
            if (raw_ == kNone) return false;
            detail::CancelSlot* s = detail::ResolveCancelSlot(raw_);
            return s && s->cancelled.load(std::memory_order_acquire) != 0;
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

} // namespace JLib
