// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/WaitGroup.h"
#include "../include/Fiber.h"
#include "../include/DirectEvent.h"   // full definition needed here for Signal()

using namespace JLib;


void JLib::WaitGroup::WakeAll()
{
    std::vector<DirectEvent*> to_wake;
    {
        std::lock_guard<std::mutex> lock(mtx);
        to_wake.assign(waiters.begin(), waiters.end());
        waiters.clear();
        // The cancellable waiters are waiting for the SAME thing and are released by the same
        // event. Being cancellable only changes what else can end their wait, not what completion
        // means -- so they are woken here too, and they report Ok because the group really did
        // finish. Missing them here would park a cancellable waiter forever on a completed group.
        for (const auto& w : cancellable) to_wake.push_back(w.ev);
        cancellable.clear();
        n.fetch_and(~WAITER_BIT, std::memory_order_release);   // clear bit for reuse
    }
    for (auto* ev : to_wake)
        ev->Signal();
}

std::size_t JLib::WaitGroup::CancelWaiters(CancelToken tok) noexcept
{
    std::vector<DirectEvent*> to_wake;
    {
        std::lock_guard<std::mutex> lock(mtx);

        // REMOVE BEFORE WAKE, never the reverse -- the same rule the CV documents. Every entry
        // points at a DirectEvent living in a parked fiber's frame; waking a waiter lets that frame
        // continue and the pointer die, so a waiter left in the list after being woken is a
        // dangling pointer the next WakeAll would signal.
        std::vector<CancelWaiter> keep;
        keep.reserve(cancellable.size());
        for (const auto& w : cancellable) {
            const bool matches = !tok.Valid() || CancelToken(w.token).IsWithin(tok);
            if (matches) to_wake.push_back(w.ev);
            else         keep.push_back(w);
        }
        cancellable.swap(keep);

        // n IS DELIBERATELY UNTOUCHED. See the declaration: the tasks are still outstanding and
        // still Done() on their own. WAITER_BIT is left alone too -- it is a hint, and clearing it
        // while `waiters` still holds someone would lose their wake.
    }
    for (auto* ev : to_wake)
        ev->Signal();
    return to_wake.size();
}