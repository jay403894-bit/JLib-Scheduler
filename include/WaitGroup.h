// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
#include <mutex>
#include <unordered_set>

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

        void WakeAll();
    };
}
