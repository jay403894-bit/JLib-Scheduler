// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/WaitGroup.h"
#include "../include/Fiber.h"
#include "../include/DirectEvent.h"   // full definition needed here for Signal()
#include "../include/TaskScheduler.h"   // Fiber::Resume is defined there (needs Thread + TaskScheduler)

using namespace JLib;


void JLib::WaitGroup::WakeAllDirect()
{
    // ONE EXCHANGE TAKES THE WHOLE LIST, which also makes this safe to call concurrently with
    // itself: exactly one caller can win a given set of waiters, and the loser gets null.
    Fiber* head = directWaiters.exchange(nullptr, std::memory_order_acq_rel);
    if (!head) return;

    n.fetch_and(~WAITER_BIT, std::memory_order_release);   // clear the bit for reuse

    while (head) {
        // READ THE LINK BEFORE THE RESUME, and this is not a style preference. Resume() makes the
        // fiber runnable immediately -- it can be picked up, run, finish its wait and park on
        // something ELSE before this loop's next instruction, overwriting nextWaiter. Reading it
        // afterwards walks whatever list that fiber joined next, which is somebody else's.
        Fiber* next = head->nextWaiter;
        head->nextWaiter = nullptr;
        head->Resume();       // handles the WANTS_SUSPEND/SUSPENDED race, exactly as Signal did
        head = next;
    }
}

void JLib::WaitGroup::WakeAll()
{
    // EVERY READ AND WRITE OF THIS GROUP HAPPENS BEFORE ANY WAITER IS RESUMED. Not a preference --
    // it is the rule CancelWaiters below states outright, and violating it is a use-after-free that
    // reproduces on nearly every run.
    //
    // A WaitGroup is very often a STACK LOCAL in the frame that is waiting on it -- fork-join is
    // literally `WaitGroup wg; ...push...; WaitFor(wg);`. Resuming a waiter lets that frame run to
    // the closing brace and destroy the group, concurrently, while this function is still inside
    // it. Draining the direct stack first and then reaching for `mtx` touches a destroyed mutex.
    //
    // So: collect everything, clear everything, publish the bit -- then wake, and touch nothing
    // afterwards. `directHead` and `to_wake` are locals and outlive the group deliberately.
    std::vector<DirectEvent*> to_wake;
    Fiber* directHead = nullptr;
    {
        std::lock_guard<std::mutex> lock(mtx);
        to_wake.assign(waiters.begin(), waiters.end());
        waiters.clear();
        // The cancellable-waiter sweep was here and is gone with the feature. Completion still
        // wakes everybody it ever woke: `waiters` above and `directWaiters` below were always the
        // two real memberships.
        // Taken under the same lock as the rest, so one pass collects the whole membership of both
        // mechanisms. They coexist because a group can hold waiters that parked either way.
        directHead = directWaiters.exchange(nullptr, std::memory_order_acq_rel);
        n.fetch_and(~WAITER_BIT, std::memory_order_release);   // clear bit for reuse
    }

    // ---- NOTHING BELOW MAY TOUCH `this` ----
    while (directHead) {
        // Read the link before the resume: a resumed fiber can run, finish and park on something
        // else before the next instruction here, overwriting nextWaiter.
        Fiber* next = directHead->nextWaiter;
        directHead->nextWaiter = nullptr;
        directHead->Resume();
        directHead = next;
    }
    for (auto* ev : to_wake)
        ev->Signal();
}
