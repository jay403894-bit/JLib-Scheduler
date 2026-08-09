// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Task.h"
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
        n.fetch_and(~WAITER_BIT, std::memory_order_release);   // clear bit for reuse
    }
    for (auto* ev : to_wake)
        ev->Signal();
}