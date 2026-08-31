// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// Deadlines. C++17, core -- all three execution modes use it through the same call.
//
// WHAT THIS IS NOT: it is not a timed wait, and no worker ever sleeps on a deadline. The scheduler's
// standing rule is that a worker's park path takes no timeout, because a wait that can return
// unsignalled hides a lost wakeup -- the bug then looks like "occasionally slow" instead of "hung",
// which is far worse to find. So deadlines live on ONE dedicated thread that does nothing else, and
// the workers' idle path is exactly as it was.
//
// WHAT A TIMER ACTUALLY DOES: it cancels a SCOPE. That is the whole mechanism, and it is why this
// file is small. Since 3.3.0 scopes nest, so a timeout is not a new kind of cancellation -- it is
// the same kind, arriving from a second party:
//
//     CancelScope op(connection.Token());          // cancelled if the peer goes away...
//     Deadline d(50ms, op.Token(), Eject(sem));    // ...or if this fires first
//     WaitResult r = sem.WaitCancellable();        // one wait, both outcomes
//
// Nothing about Task changed to make that work. A task still carries one 4-byte token; the walk in
// CancelToken::Cancelled composes the reasons.
//
// TWO STEPS, IN THIS ORDER, and both are needed. Setting the cancel flag does NOT wake anybody --
// cancellation is observed at suspension points, and a task already parked has passed its last one.
// So an expiring timer first calls CancelVia (so every LATER check agrees it is cancelled) and then
// runs an EJECT callback that wakes whoever is parked right now. The eject is supplied by the
// arming code because only it knows which primitive the wait is on; the helpers below cover the
// built-in ones, and an I/O request supplies its own -- see the note on TimerEject.
//
// REMOVAL IS AN OPTIMISATION HERE, NOT A CORRECTNESS REQUIREMENT, which is the payoff from building
// this after the scope work rather than before. An operation that completes before its deadline
// destroys its scope; the timer then fires on a stale token, the generation check in CancelVia
// fails, and nothing happens. Disarm exists so a queue full of dead entries does not accumulate, not
// to prevent a wrong cancellation. Compare a timeout on a condition variable, which could NOT have
// been retrofitted safely: it would pop its own frame while its address was still queued.

#include "CancelToken.h"

#include <cstddef>
#include <cstdint>

namespace JLib {

    class Event;
    class SchedulerSemaphore;
    class SchedulerConditionVariable;

    // Monotonic nanoseconds. Never the wall clock: a deadline must not move because somebody
    // adjusted the system time or a DST boundary went past.
    int64_t MonotonicNs() noexcept;

    // What an expired timer calls to WAKE whatever is parked, after the cancel flag is set.
    //
    // Type-erased on purpose. The timer must not know about Event, or about semaphores, or about an
    // I/O request -- those all have different ideas of what waking means, and an I/O request's is the
    // most different: asking the OS to cancel a transfer does NOT end it, so its eject starts a
    // cancellation and the operation still has to be awaited to a completion. Keeping that behind a
    // function pointer is what lets the reactor supply its own without this file growing.
    //
    // CALLED WITHOUT ANY TIMER LOCK HELD, and it may run on the timer thread. It must not block,
    // must not arm or disarm timers, and must tolerate its target having already been woken by
    // somebody else -- a deadline and a real completion can race, and every built-in wake path
    // already arbitrates that.
    using TimerEject = void (*)(void* ctx, CancelToken token);

    // Identifies one armed timer. Index plus generation, so a handle for a timer that has already
    // fired or been disarmed is inert rather than pointing at whoever reused the slot -- the same
    // construction as CancelToken, for the same reason.
    struct TimerHandle {
        uint64_t raw = 0;
        bool Valid() const noexcept { return raw != 0; }
    };

    class TimerQueue {
    public:
        static TimerQueue& Instance();

        // Cancel `token` after `delayNs`, then eject(ctx, token) if one was given.
        //
        // A delay of zero or less fires at the next tick rather than inline, so arming never runs a
        // callback on the caller's stack -- a caller that armed a timer while holding a lock would
        // otherwise re-enter with it held.
        //
        // Returns an invalid handle if the token is already stale or absent: there would be nothing
        // to cancel, and queueing it would just be a wake-up for no reason.
        TimerHandle Arm(int64_t delayNs, CancelToken token,
                        TimerEject eject = nullptr, void* ctx = nullptr);

        // Remove before it fires. True if this call removed it; false if it had already fired, was
        // already disarmed, or the handle is stale. NOT required for correctness -- see the file
        // header -- but a server that arms a timeout per request and completes most of them early
        // needs it, or the queue fills with entries nobody will ever want.
        //
        // Does NOT wait for a concurrently-firing timer to finish. If Disarm returns false the
        // callback may be running right now; a caller that needs to know cancellation did not happen
        // should ask the scope, which is the authority.
        bool Disarm(TimerHandle h) noexcept;

        // Armed and not yet fired. Diagnostics and tests; racy by nature.
        std::size_t PendingCount() const noexcept;

        // The thread starts on the first Arm and is stopped at exit. Stop is idempotent, and safe to
        // call explicitly by a process that wants the thread gone before its own teardown.
        void Stop() noexcept;

        // Undo Stop so the queue can serve a NEW pool after Join. The wheel survives Stop and the
        // worker respawns on the next Arm, so this only clears the stop latch.
        void Start() noexcept;

    private:
        TimerQueue();
        ~TimerQueue();
        TimerQueue(const TimerQueue&) = delete;
        TimerQueue& operator=(const TimerQueue&) = delete;

        struct Impl;
        Impl* impl;
    };

    // RAII deadline. Arms on construction, disarms on destruction -- so an operation that finishes
    // early takes its timer out of the queue on the way past, at the same point the scope it would
    // have cancelled is going out of scope anyway.
    //
    // USE THIS RATHER THAN Arm/Disarm. The bare calls are for a reactor holding many in-flight
    // deadlines in its own table; anything with a lexical wait wants the destructor.
    class Deadline {
    public:
        Deadline(int64_t delayNs, CancelToken token,
                 TimerEject eject = nullptr, void* ctx = nullptr)
            : h_(TimerQueue::Instance().Arm(delayNs, token, eject, ctx)) {}

        ~Deadline() { TimerQueue::Instance().Disarm(h_); }

        Deadline(const Deadline&) = delete;
        Deadline& operator=(const Deadline&) = delete;

        // False once it has fired or been disarmed. NOT "did the operation time out" -- ask the
        // scope for that; this only says whether a timer is still pending.
        bool Armed() const noexcept { return h_.Valid(); }

        // Take it out early, before the destructor. Returns what Disarm returned.
        bool Cancel() noexcept { return TimerQueue::Instance().Disarm(h_); }

    private:
        TimerHandle h_;
    };

    // Ejects for the built-in primitives. Each is just "wake the waiters in this scope", spelled the
    // way that primitive spells it, so a caller writes Deadline(ns, tok, EjectSemaphore, &sem)
    // instead of writing the cast itself.
    //
    // ALL THREE ARE EAGER, which is the point of a timeout: skip-at-release would make the deadline
    // land only when somebody else got round to releasing, which for a wait that has already run too
    // long is exactly the thing that is not happening.
    void EjectEvent(void* ctx, CancelToken token);          // ctx: Event*
    void EjectSemaphore(void* ctx, CancelToken token);      // ctx: SchedulerSemaphore*
    void EjectConditionVariable(void* ctx, CancelToken token);  // ctx: SchedulerConditionVariable*

} // namespace JLib
