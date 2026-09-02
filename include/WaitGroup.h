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

        // ---- THE CANCELLABLE WAIT WAS REMOVED (9-02). ------------------------------------------
        //
        // `WaitFor(wg, token)` and `WaitGroup::CancelWaiters` are gone, along with the `cancellable`
        // vector that fed them. They did not work, and the header said so for weeks in its own
        // words: "INCOMPLETE. A FIBER THAT PARKS HERE CAN WAIT FOREVER."
        //
        // THE DEFECT WAS STRUCTURAL, not a bug to chase. The token a waiter stored here was PASSIVE
        // -- a filter applied by whoever walked this vector, never a wake. Cancellation dispatches
        // ejection BY TYPE (Event, SchedulerSemaphore, SchedulerConditionVariable, IoAcceptor) and
        // there was no EjectWaitGroup, so `scope.Cancel()` could not reach a waiter parked here.
        // Cancelling a scope on a group that will never complete -- which is what cancelling usually
        // MEANS -- woke nothing at all.
        //
        // WHAT IT COST: waitgroup_cancel_test deadlocked ~2-13% of runs, and a deadlocked test does
        // not fail, it spins every worker until something kills it. The only thing keeping it green
        // the rest of the time was the test calling `inner.CancelWaiters(...)` by hand on the line
        // after `scope.Cancel()` -- so the feature's own test was working around it.
        //
        // ONE USER, AND IT WAS THAT TEST. Nothing in the library, the benches or the samples ever
        // called either function. A cancellable wait that the cancellation machinery cannot reach is
        // not a feature with a flaky test; it is a promise the API could not keep.
        //
        // AND IT SHOULD NOT COME BACK, because the defect was not the delivery path -- it was the
        // idea. A WaitGroup IS NOT A WAIT PRIMITIVE. It is a concurrency counter: N outstanding, and
        // a join that ends when N reaches zero. Event, the semaphore and the condition variable own
        // a QUEUE OF WAITERS, which is the thing cancellation ejects from; a counter owns none.
        //
        // That is why the old CancelWaiters had to promise, in capitals, that it did not touch `n`.
        // It could not: cancelling a wait while the counted work keeps running leaves the count
        // outstanding and the tasks still decrementing into a group nobody is joined to. So the
        // operation had no coherent meaning to deliver, and building a delivery path for it -- an
        // EjectWaitGroup, or deriving from WaitPrimitive -- would have made an incoherent operation
        // reliable rather than making it correct.
        //
        // If you need a join you can abandon, compose it from a primitive that HAS waiters: wait on
        // an Event that the last task signals, and cancel the Event. The counter stays a counter.
        //
        // Plain `WaitFor(wg)` is untouched and was always the uncancellable one by contract.

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
        // CancelWaiters WAS HERE and is gone with the cancellable wait -- see the note above the
        // removed `cancellable` vector. Its own comment used to insist, in capitals, that it did not
        // touch `n`, which was the tell: an operation that cannot affect the thing the group counts
        // is not cancelling the group, and the group has no waiter queue to eject from either.
    };
}
