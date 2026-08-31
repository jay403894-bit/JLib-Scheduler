// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "CancelToken.h"

#include <atomic>
#include <cstddef>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace JLib {
    struct DirectEvent;   // pointer-only below; the definition is needed in WaitGroup.cpp
    struct Fiber;         // same: the direct waiter stack stores Fiber*, and only WaitGroup.cpp
                          // dereferences one. Including Fiber.h here would drag Task.h and Epochs.h
                          // into every TU that touches a WaitGroup, which is the exact bloat that
                          // moved this out of Task.h in 1.0.

    // Completion counter with a parked-waiter list: the primitive behind WaitFor.
    //
    // Lived in Task.h until 1.0, alongside Task itself, which put its <mutex> and <unordered_set>
    // into every translation unit that touched a Task -- and only two of the nine headers including
    // Task.h ever named a WaitGroup. It is a synchronisation primitive, sibling to Event and
    // DirectEvent, and now has a header like they do. Task.h forward-declares it, since a Task only
    // ever holds a WaitGroup*.
    //
    // Nothing needs to include this directly: TaskScheduler.h pulls it in, exactly as it already
    // does for DirectEvent.h.
    struct WaitGroup {
        // n packs two things. The low bits are the outstanding-task COUNT; the top bit records
        // that at least one waiter has parked, so a completing task knows whether anyone needs
        // waking. Mask with COUNT_MASK before comparing a count -- reading n raw and testing
        // against 0 sees the bit and concludes work is outstanding when none is.
        static constexpr int WAITER_BIT = 0x40000000;
        static constexpr int COUNT_MASK = WAITER_BIT - 1;
        std::atomic<int> n{ 0 };
        std::mutex mtx;
        std::unordered_set<DirectEvent*> waiters;  // fibers parked on this group

        // Waiters that asked to be released early if their scope is cancelled. Kept apart from
        // `waiters` because they are the only ones CancelWaiters may touch -- a plain WaitFor is
        // uncancellable by contract and must never be woken by somebody else's cancel.
        struct CancelWaiter {
            DirectEvent* ev = nullptr;
            uint32_t     token = 0xFFFFFFFFu;   // CancelToken::kNone
        };
        std::vector<CancelWaiter> cancellable;

        // ---- DIRECT WAITERS: no mutex, no allocation, no DirectEvent ---------------------------
        //
        // WHAT THE OTHER PATH COSTS, per wait: a pooled DirectEvent acquire, a std::mutex, and an
        // unordered_set node -- a heap allocation. And per completion, WakeAll takes the mutex again
        // and builds a std::vector to wake from. On the most-used wait in the library.
        //
        // This is the same information as an intrusive lock-free stack threaded through the parked
        // fibers themselves. A push is one CAS, a drain is one exchange, and nothing allocates.
        //
        // ONLY SOUND BECAUSE FIBERS ARE PINNED AND NEVER FREED. The link lives on the Fiber, and the
        // global pool reserves and leaks its fibers, so a stack node cannot be recycled underneath a
        // walker -- which is exactly the bug that retired the older Task-threaded waiter list.
        //
        // ORDERING IS LOAD-BEARING AND IS COPIED FROM WaitOnEventDirectArmed, which documents it:
        // become parkable FIRST (status = WANTS_SUSPEND), publish the waiter SECOND, arm THIRD,
        // suspend LAST -- and suspend by ContextSwitch, never Fiber::Suspend(), because Suspend()
        // re-stores WANTS_SUSPEND and would overwrite a SUSPEND_SIGNALED that raced in. Publishing
        // before becoming parkable lets a signal Resume() a RUNNING fiber, which is a no-op, which
        // is a lost wakeup and a permanent hang.
        std::atomic<Fiber*> directWaiters{ nullptr };

        // Drain the direct stack and resume everyone on it. Safe to call with an empty stack, which
        // is the Default-mode case -- nothing ever pushes there.
        void WakeAllDirect();

        void WakeAll();

        // Release the waiters whose scope is `tok` (or inside it) and tell them they were cancelled.
        // Returns how many were woken.
        //
        // DOES NOT TOUCH n, AND THAT IS THE WHOLE POINT. Cancelling a wait means "I stopped
        // waiting", not "this group is finished": the outstanding tasks are still outstanding and
        // still decrement when they complete. Zeroing the count here would be a lie that a second
        // waiter, or a later WaitFor on the same group, would believe -- and it would strand every
        // task still in flight with nothing left to release.
        std::size_t CancelWaiters(CancelToken tok) noexcept;
    };
}
