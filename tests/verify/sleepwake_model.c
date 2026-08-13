// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the PROPOSED worker sleep/wake handshake -- the one that lets a push SKIP the mutex and
// condition-variable signal entirely when the target worker is already running. Companion to
// deque_model.c and event_model.c; see the former for what a model checker is.
//
//   WHY THIS EXISTS BEFORE THE CODE DOES
//   Today every push pays Thread::NotifyWorker() unconditionally: an empty mutex acquire/release
//   plus cv.notify_one(), whether or not the worker is awake. While the pool is saturated that is
//   nearly free. Once the pool drains faster than one thread can submit, workers genuinely park and
//   every push buys a real thread wake. Measured cost of that: single-producer submission collapses
//   from 3.4 M/s at 8 workers to 0.8 M/s at 14+, and round-trip latency is ~6x higher than it needs
//   to be.
//
//   Skipping the notify when the worker is awake is worth roughly 6x on latency. It is also the
//   single most dangerous edit available in this codebase, because getting it wrong reintroduces the
//   ParallelFor lost-wakeup deadlock: a worker asleep on a non-empty inbox, and inboxes are drainable
//   only by their owner, so the task is stranded permanently rather than merely delayed.
//
//   THE QUESTION THIS FILE ANSWERS
//   Is the three-state handshake below free of lost wakeups, and does it actually need seq_cst?
//   The pusher does store-then-load (publish work, observe state). The worker does store-then-load
//   (publish state, observe work). Those are StoreLoad pairs on DIFFERENT locations, which is the
//   one ordering acquire/release does not give you. If both loads may go stale simultaneously, the
//   pusher decides "it is awake, no signal needed" while the worker decides "no work, I will sleep",
//   and nobody is left to wake it.
//
//   Being an RMW does not rescue the CAS: the race is across two objects, so the worker's write of
//   GOING_TO_SLEEP still has to be ordered against its RELOAD of the work counter.
//
//   SAFETY, NOT LIVENESS -- the trick that makes this checkable here
//   "Every parked worker eventually runs" is a liveness property, and a stateless model checker
//   cannot express it. So the sleep is modelled as a TERMINAL STATE rather than a blocking call:
//   the worker sets SLEEPING and returns. The bad outcome then becomes an ordinary assertion on the
//   final state -- work queued, worker sleeping, no signal sent -- which is exactly reachable-state
//   checking and precisely what this tool is for. That is why TLA+ is not needed for THIS question,
//   though it still would be for anything about eventual progress.
//
//   Build variants:
//     genmc -- sleepwake_model.c                     # as proposed (seq_cst)
//     genmc -- -DACQ_REL_ONLY sleepwake_model.c      # negative control, MUST fail
//
//   RESULTS: recorded at the bottom of this file.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

/* Thread::workerState */
#define ST_AWAKE     0
#define ST_GOING     1     /* intent published: about to sleep, not yet in cv.wait */
#define ST_SLEEPING  2

static _Atomic int g_state;      /* one worker's state */
static _Atomic int g_work;       /* items in that worker's inbox */
static _Atomic int g_notified;   /* did the pusher take the mutex and signal? */

#ifdef ACQ_REL_ONLY
  /* Negative control. This is the version that looks correct and IS correct on x86, because
     `lock cmpxchg` is incidentally a full barrier that drains the store buffer. On AArch64,
     ldaxr/stlxr give exactly acquire-release and nothing more, so the hole reopens. The scheduler
     ships on Apple Silicon and Android, so "works on my desktop" is not an argument here. */
  #define PUBLISH      memory_order_release
  #define OBSERVE      memory_order_acquire
  #define TRANSITION   memory_order_acq_rel
#else
  #define PUBLISH      memory_order_seq_cst
  #define OBSERVE      memory_order_seq_cst
  #define TRANSITION   memory_order_seq_cst
#endif

// ---- the worker, deciding whether to park ----------------------------------------------------
// Mirrors Worker(): the local-queue / steal / inbox search has just come up empty, so it considers
// sleeping. The RECHECK after advertising intent is the entire point of the ST_GOING state.
static void *worker(void *arg) {
    (void)arg;

    int expected = ST_AWAKE;
    atomic_compare_exchange_strong_explicit(&g_state, &expected, ST_GOING,
                                            TRANSITION, memory_order_relaxed);

    if (atomic_load_explicit(&g_work, OBSERVE) == 0) {
        int e2 = ST_GOING;
        if (atomic_compare_exchange_strong_explicit(&g_state, &e2, ST_SLEEPING,
                                                    TRANSITION, memory_order_relaxed)) {
            /* cv.wait() would go here. Modelled as termination: see the header note. */
        }
    } else {
        /* Found work after advertising intent: abandon the park. This is the benign path. */
        atomic_store_explicit(&g_state, ST_AWAKE, memory_order_relaxed);
    }
    return NULL;
}

// ---- the pusher ------------------------------------------------------------------------------
// Mirrors PushLocal(): publish the task, then decide whether the worker needs waking. The whole
// optimisation is the `else` -- an AWAKE worker will find this on its next loop iteration, so the
// mutex and the condvar are skipped entirely.
static void *pusher(void *arg) {
    (void)arg;

    atomic_fetch_add_explicit(&g_work, 1, PUBLISH);

    int s = atomic_load_explicit(&g_state, OBSERVE);
    if (s == ST_GOING || s == ST_SLEEPING) {
        /* take workerMutex, cv.notify_one(). COUNTED with an RMW rather than stored: two pushers
           storing the same value to one location is an unordered write-write pair, which GenMC's
           in-place revisiting rejects outright. RMWs are totally ordered per location, so this
           sidesteps a modelling artifact -- and counting signals is the more faithful thing anyway. */
        atomic_fetch_add_explicit(&g_notified, 1, memory_order_relaxed);
    }
    return NULL;
}

int main(void) {
    atomic_init(&g_state, ST_AWAKE);
    atomic_init(&g_work, 0);
    atomic_init(&g_notified, 0);

    /* TWO pushers, not one. The Dekker race needs only one, but two is the realistic shape -- the
       main thread and a worker can both target the same inbox -- and it also covers the case where
       one pusher observes AWAKE and skips while the other observes GOING and signals. */
    pthread_t w, p0, p1;
    pthread_create(&w,  NULL, worker, NULL);
    pthread_create(&p0, NULL, pusher, NULL);
    pthread_create(&p1, NULL, pusher, NULL);
    pthread_join(w,  NULL);
    pthread_join(p0, NULL);
    pthread_join(p1, NULL);

    const int work     = atomic_load(&g_work);
    const int state    = atomic_load(&g_state);
    const int notified = atomic_load(&g_notified);

    /* THE PROPERTY: no lost wakeup. A worker that reached SLEEPING with work queued must have been
       signalled. Anything else is a thread parked forever on a task only it can drain.

       Note what is NOT asserted: signalling a worker that then decided to stay awake is FINE. That
       is a notify landing on a condvar with no waiter, which is a no-op -- a wasted syscall, not a
       correctness problem. Asserting against it would reject a correct protocol. */
    assert(!(work > 0 && state == ST_SLEEPING && notified == 0));

    return 0;
}

// RESULTS -- GenMC v0.17.0, 2026-08-12, one worker + two pushers:
//
//   as proposed (seq_cst)   no errors, 25 complete executions
//   -DACQ_REL_ONLY          Error: Safety violation!  (the assert below)
//
// So the three-state handshake is free of lost wakeups at this bound, AND the seq_cst is load
// bearing rather than defensive. The negative control is what makes the first statement worth
// anything, and it fails in exactly the predicted way. From its trace, with one pusher:
//
//   worker: (1,3): Racq (g_work,  0) [(0,2)]   <- reads the INITIAL value, misses the push
//   pusher: (2,3): Racq (g_state, 0) [(0,1)]   <- reads the INITIAL value, misses GOING_TO_SLEEP
//
// Both loads stale simultaneously: the pusher concludes "awake, no signal needed" while the worker
// concludes "no work, safe to sleep". Neither thread is reordered against itself. Acquire/release
// simply never promised that at least one of them would observe the other, because StoreLoad is the
// one pair it leaves free. seq_cst gives a single total order in which one store necessarily
// precedes the other's load, so at most one side can be stale.
//
// WHY THIS MATTERS BEYOND THE MODEL: the failing version is correct on x86 anyway, because
// `lock cmpxchg` is incidentally a full barrier and drains the store buffer. It is AArch64 where
// ldaxr/stlxr give exactly acquire-release and the hole is real. This scheduler ships on Apple
// Silicon and Android. Testing the acq_rel version on a desktop would have produced a clean run and
// a shipped deadlock.
//
// One modelling note worth keeping: g_notified is incremented with an RMW rather than stored,
// because two pushers storing the same value to one location is an unordered write-write pair that
// GenMC's in-place revisiting rejects outright. That was a harness artifact, not a protocol defect.
//
// Keep -DACQ_REL_ONLY as a permanent negative control and re-run it whenever this model changes.
