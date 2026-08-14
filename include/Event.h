// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <atomic>
// Fiber.h, not TaskScheduler.h: all this needs is Task (for Task*/nextWaiter) and Fiber (for
// Resume), and Fiber.h brings Task.h with it. Depending on TaskScheduler.h created a cycle that
// forced TaskScheduler.h to forward-declare Event -- so GetEvent() returned a reference callers
// could not use without separately including this file. Breaking the cycle lets TaskScheduler.h
// include Event.h like it already includes DirectEvent.h, and the papercut goes away.
#include "Fiber.h"
namespace JLib {
    // Named rendezvous point: any number of fibers park on it, one signal wakes them all.
    //
    // LOCK-FREE AND ALLOCATION-FREE, which it was not: this used to be a std::mutex around an
    // unordered_set, so every suspend allocated a hash node and every signal took a lock that an
    // external thread (a GPU-fence callback, say) might contend. Both are gone -- an intrusive
    // push-only stack threaded through Task::nextWaiter.
    //
    // WHY IT NEEDS NO EBR, NO HAZARD POINTERS AND NO TAGGED POINTER, which is the usual price of a
    // lock-free stack: ABA bites on POP -- you read head, read head->next, then CAS, and in that
    // window the node can be freed and a recycled address put back, so your CAS succeeds against a
    // stale link. NOTHING HERE POPS. SignalAll takes the entire list in one exchange, so there is
    // no read-then-CAS window at all and no node is ever inspected while another thread could be
    // detaching it.
    //
    // That property is load-bearing, so guard it: this stays correct only while there is no
    // "remove one specific waiter" operation. An earlier Signal(Task*) and RemoveWaiter pair
    // existed and had zero callers; adding either back reintroduces remove-from-middle and with it
    // the whole hazard-pointer problem. Wake everyone and let the waiters re-check their own
    // condition instead -- which is what every current caller already does.
    class Event {
    private:
        std::atomic<Task*> head{ nullptr };

    public:
        // Register a waiter. Does NOT touch fiber status -- WaitOnEvent has already put the fiber
        // in WANTS_SUSPEND before calling this. The release on success publishes nextWaiter to
        // whoever later takes the list, so a signal landing after we push will find us.
        // MODEL CHECKED, GenMC v0.17.0, 2026-08-11 (tests/verify/event_model.c, two concurrent
        // pushers + one drainer, 24 complete executions): no waiter lost, none woken twice, and no
        // race on the plain nextWaiter field. Dropping the release below reports a non-atomic race
        // immediately, so the release is load-bearing and not decoration.
        //
        // The subtle half is the CHAINED case -- A pushes, B pushes, the drainer takes B. B's CAS
        // is an RMW reading A's value, which puts it in A's release sequence, so the acquiring
        // exchange synchronises with A's release too and A's nextWaiter write is visible. That rule
        // is why a Treiber push is correct with release/acquire instead of seq_cst.
        void AddWaiter(Task* t) {
            Task* h = head.load(std::memory_order_relaxed);
            do {
                t->nextWaiter = h;
            } while (!head.compare_exchange_weak(h, t,
                         std::memory_order_release, std::memory_order_relaxed));
        }

        // Wake everyone waiting on this event.
        //
        // The exchange is the whole synchronisation: it detaches the list atomically, so this
        // caller privately owns every node afterwards and no other thread can touch them. Read
        // nextWaiter BEFORE resuming -- Resume() can put the task straight back on a worker, which
        // may finish it and recycle its slab slot before the loop advances.
        // Wakes every registered waiter. The list is taken in ONE exchange, so a signal never
        // partially drains it.
        //
        // Resumable fibers are COLLECTED and re-queued in a batch rather than one at a time. Each
        // individual re-queue is a placement + inbox push + seq_cst flag + condition-variable
        // signal, and a broadcast that wakes 64 fibers paid all of that 64 times, serially, on
        // whichever thread called SignalAll. Measured elsewhere: single Push ~1 M/s, PushBatch
        // ~12 M/s.
        //
        // Split by priority because PushBatch takes ONE priority for the whole run -- merging them
        // would silently demote a hiPri fiber. Buffers are fixed and flushed when full, so this
        // allocates nothing on a path that may run from any thread, including a signaller that is
        // not a worker.
        void SignalAll() {
            Task* t = head.exchange(nullptr, std::memory_order_acq_rel);
            constexpr size_t kBuf = 64;
            Task* lo[kBuf]; size_t nlo = 0;
            Task* hi[kBuf]; size_t nhi = 0;
            while (t) {
                Task* next = t->nextWaiter;
                t->nextWaiter = nullptr;
                // Handles the WANTS_SUSPEND/SUSPENDED race exactly as Resume() did; true means this
                // call won the SUSPENDED -> READY transition and now owns re-queueing the task.
                if (t->assignedFiber->ResumeQueueless()) {
                    if (t->hiPri) {
                        hi[nhi++] = t;
                        if (nhi == kBuf) { RequeueResumedBatch(hi, nhi, true); nhi = 0; }
                    } else {
                        lo[nlo++] = t;
                        if (nlo == kBuf) { RequeueResumedBatch(lo, nlo, false); nlo = 0; }
                    }
                }
                t = next;
            }
            RequeueResumedBatch(hi, nhi, true);
            RequeueResumedBatch(lo, nlo, false);
        }
    };
};
