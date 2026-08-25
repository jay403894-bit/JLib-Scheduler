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
