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
//     genmc -- sleepwake_model.c                                        # as shipped
//     genmc -- -DIMMEDIATE_ONLY sleepwake_model.c                       # one setter, strong
//     genmc -- -DIMMEDIATE_ONLY -DWEAK_IMMEDIATE sleepwake_model.c      # the 1.2.0 bug, MUST fail
//     genmc -- -DACQ_REL_ONLY sleepwake_model.c                         # MUST fail
//     genmc -- -DLANE_ONLY sleepwake_model.c                            # one setter, strong
//     genmc -- -DLANE_ONLY -DWEAK_LANEWAKE sleepwake_model.c            # MUST fail
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

static _Atomic int g_state;      /* Thread::workerState */
static _Atomic int g_work;       /* Thread::hasQueuedWork */
static _Atomic int g_immediate;  /* Thread::immediate -- the SECOND predicate input */
static _Atomic int g_lanewake;   /* Thread::laneWake -- the FOURTH predicate input */
static _Atomic int g_notified;   /* did a setter take the mutex and signal? */

#ifdef ACQ_REL_ONLY
  /* Negative control 1: the whole handshake weakened. This is the version that looks correct and IS
     correct on x86, because `lock cmpxchg` is incidentally a full barrier that drains the store
     buffer. On AArch64, ldaxr/stlxr give exactly acquire-release and nothing more, so the hole
     reopens. The scheduler ships on Apple Silicon and Android, so "works on my desktop" is not an
     argument here. */
  #define PUBLISH      memory_order_release
  #define OBSERVE      memory_order_acquire
  #define TRANSITION   memory_order_acq_rel
#else
  #define PUBLISH      memory_order_seq_cst
  #define OBSERVE      memory_order_seq_cst
  #define TRANSITION   memory_order_seq_cst
#endif

/* Negative control 3: the same weakening applied to the FOURTH input. Added when lane wakes gave
   the sleep predicate its fourth flag -- because the rule at the bottom of this file says a new
   input arrives with its own control, and a rule nobody enforces is a comment. */
#ifdef WEAK_LANEWAKE
  #define LANE_PUBLISH  memory_order_release
  #define LANE_OBSERVE  memory_order_acquire
#else
  #define LANE_PUBLISH  PUBLISH
  #define LANE_OBSERVE  OBSERVE
#endif

/* Negative control 2, and the one that matters: THE BUG ACTUALLY SHIPPED IN 1.2.0.
   The first version of this model had a single flag. The real sleep predicate has three inputs, and
   `immediate` and `paused` were left as plain release stores read with acquire while only
   `hasQueuedWork` was promoted to seq_cst. The total-order argument covers the flags that are IN
   the total order and no others, so the second flag reopened the identical hole -- and hung macOS
   arm64 in CI roughly one run in three.
   -DWEAK_IMMEDIATE reproduces exactly that: flag one strong, flag two weak. It MUST fail. */
#ifdef WEAK_IMMEDIATE
  #define IMM_PUBLISH  memory_order_release
  #define IMM_OBSERVE  memory_order_acquire
#else
  #define IMM_PUBLISH  PUBLISH
  #define IMM_OBSERVE  OBSERVE
#endif

// ---- the worker, deciding whether to park ----------------------------------------------------
// Mirrors Worker(): the local-queue / steal / inbox search has just come up empty, so it considers
// sleeping. The RECHECK after advertising intent is the entire point of the ST_GOING state.
static void *worker(void *arg) {
    (void)arg;

    int expected = ST_AWAKE;
    atomic_compare_exchange_strong_explicit(&g_state, &expected, ST_GOING,
                                            TRANSITION, memory_order_relaxed);

    /* EVERY input to the real sleep predicate must be here, not just the interesting one. Modelling
       a subset is what shipped the 1.2.0 hang: the proof covered `hasQueuedWork` and the code
       applied it to a decision that also reads `immediate` and `paused`. */
    const int work = atomic_load_explicit(&g_work,      OBSERVE);
    const int imm  = atomic_load_explicit(&g_immediate, IMM_OBSERVE);
    const int lane = atomic_load_explicit(&g_lanewake,  LANE_OBSERVE);

    if (work == 0 && imm == 0 && lane == 0) {
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

// ---- the OTHER setter --------------------------------------------------------------------------
// Mirrors SetImmediateTask (and, identically in shape, Resume clearing `paused`). Same handshake,
// different flag. This thread is the whole reason the model was rewritten: it did not exist in the
// version that "proved" the protocol, so nothing checked whether a second predicate input was also
// in the total order. It was not, and macOS arm64 found out.
static void *immediate_setter(void *arg) {
    (void)arg;

    atomic_store_explicit(&g_immediate, 1, IMM_PUBLISH);

    int s = atomic_load_explicit(&g_state, OBSERVE);
    if (s == ST_GOING || s == ST_SLEEPING) {
        atomic_fetch_add_explicit(&g_notified, 1, memory_order_relaxed);
    }
    return NULL;
}

// ---- the lane-wake setter ----------------------------------------------------------------------
// Mirrors TaskScheduler::WakeForLane: a HOT worker has just published a lane backlog and is pulling
// a parked ordinary worker up to come and steal it. Same handshake, third flag.
//
// A SEPARATE THREAD FROM THE PUSHER ON PURPOSE, and not for coverage. The pusher's flag means "a
// task landed in YOUR inbox"; this one means "somebody ELSE is holding work you may take." They are
// set by different threads at unrelated moments, so folding this into the pusher would assume away
// the only interleaving that matters -- the one where this is the sole setter racing the park.
static void *lane_setter(void *arg) {
    (void)arg;

    atomic_store_explicit(&g_lanewake, 1, LANE_PUBLISH);

    int s = atomic_load_explicit(&g_state, OBSERVE);
    if (s == ST_GOING || s == ST_SLEEPING) {
        atomic_fetch_add_explicit(&g_notified, 1, memory_order_relaxed);
    }
    return NULL;
}

int main(void) {
    atomic_init(&g_state, ST_AWAKE);
    atomic_init(&g_work, 0);
    atomic_init(&g_immediate, 0);
    atomic_init(&g_lanewake, 0);
    atomic_init(&g_notified, 0);

    /* One worker, one pusher, one immediate-setter. Two DIFFERENT setters rather than two of the
       same, because the failure that shipped needed a second FLAG, not a second thread -- the
       earlier two-pusher version explored more interleavings of the same variable and proved
       nothing about the other inputs. */
    pthread_t w;
    pthread_create(&w,  NULL, worker,           NULL);

    /* LANE_ONLY isolates the fourth flag for exactly the reason IMMEDIATE_ONLY isolates the second:
       a correctly-ordered setter MASKS a broken one, so a weak flag only shows itself when it is the
       sole thing racing the park. Note IMMEDIATE_ONLY deliberately excludes the lane setter as well,
       so the 1.2.0 control keeps testing precisely what it tested before this flag existed -- adding
       a fourth correctly-ordered setter to it would have quietly made that control pass. */
#ifndef LANE_ONLY
    pthread_t im;
    pthread_create(&im, NULL, immediate_setter, NULL);
#endif
#ifndef IMMEDIATE_ONLY
    pthread_t ln;
    pthread_create(&ln, NULL, lane_setter, NULL);
#endif

    /* A CORRECTLY ordered flag MASKS a broken one, which is why this can be turned off.
       Any single notify wakes the worker, whichever setter sent it. So while the seq_cst pusher is
       running, the worker can only reach SLEEPING in executions where that pusher already
       signalled -- if the pusher skipped, the total-order argument forces the worker to observe the
       work and stay awake. The lost wakeup on the WEAK flag is therefore unreachable here, and the
       first version of this model reported -DWEAK_IMMEDIATE as clean.
       That is not the real system: a worker can park with only an immediate-setter racing it.
       -DIMMEDIATE_ONLY removes the pusher so the weak flag has to stand on its own. */
#if !defined(IMMEDIATE_ONLY) && !defined(LANE_ONLY)
    pthread_t p;
    pthread_create(&p, NULL, pusher, NULL);
#endif

    pthread_join(w,  NULL);
#ifndef LANE_ONLY
    pthread_join(im, NULL);
#endif
#ifndef IMMEDIATE_ONLY
    pthread_join(ln, NULL);
#endif
#if !defined(IMMEDIATE_ONLY) && !defined(LANE_ONLY)
    pthread_join(p, NULL);
#endif

    const int work      = atomic_load(&g_work);
    const int immediate = atomic_load(&g_immediate);
    const int lanewake  = atomic_load(&g_lanewake);
    const int state     = atomic_load(&g_state);
    const int notified  = atomic_load(&g_notified);

    /* THE PROPERTY: no lost wakeup, from ANY input the sleep decision reads. A worker that reached
       SLEEPING while any of them says it has something to do must have been signalled. Anything
       else is a thread parked forever on work only it can drain.

       Note what is NOT asserted: signalling a worker that then decided to stay awake is FINE. That
       is a notify landing on a condvar with no waiter, which is a no-op -- a wasted syscall, not a
       correctness problem. Asserting against it would reject a correct protocol. */
    assert(!((work > 0 || immediate > 0 || lanewake > 0) && state == ST_SLEEPING && notified == 0));

    return 0;
}

// RESULTS -- GenMC v0.17.0, 2026-08-13, one worker plus one or two setters:
//
//   as shipped now, every flag seq_cst        no errors, 32 complete executions
//   -DIMMEDIATE_ONLY (strong, one setter)     no errors, 5 complete executions
//   -DIMMEDIATE_ONLY -DWEAK_IMMEDIATE         Error: Safety violation!   <- the 1.2.0 bug
//   -DACQ_REL_ONLY                            Error: Safety violation!
//
// RE-RUN 2026-08-25 with laneWake added as the FOURTH input (GenMC, same machine):
//
//   as shipped, four flags seq_cst            no errors, 233 complete executions
//   -DIMMEDIATE_ONLY                          no errors, 5 complete executions
//   -DIMMEDIATE_ONLY -DWEAK_IMMEDIATE         Error: Safety violation!   <- still reproduces
//   -DACQ_REL_ONLY                            Error: Safety violation!
//   -DLANE_ONLY                               no errors, 5 complete executions
//   -DLANE_ONLY -DWEAK_LANEWAKE               Error: Safety violation!   <- the new control
//
// Read the last line first: it is the only evidence that this model actually COVERS the new flag.
// A fourth input that passes is worth nothing on its own -- the model would report exactly the same
// thing if the flag were absent from the predicate entirely. WEAK_LANEWAKE failing is what proves
// the seq_cst on MarkLaneWake is load-bearing rather than decoration.
//
// Read the second line next: still 5 executions, unchanged. IMMEDIATE_ONLY deliberately excludes the
// lane setter, so the 1.2.0 control tests precisely what it tested before. Had it been left in, a
// fourth correctly-ordered setter would have MASKED the weak flag and quietly turned that control
// green -- the exact trap this file already documents one paragraph down, re-encountered while
// adding to it.
//
// The third line is the important one, and it is why this file was rewritten. The FIRST version of
// this model had a single flag. It passed, the protocol was implemented from it, and 1.2.0 shipped
// a lost wakeup that hung macOS arm64 in CI roughly one run in three. The model was not wrong about
// what it modelled; it was applied to a decision with two more inputs than it contained.
//
// Note also why -DIMMEDIATE_ONLY has to exist at all, because it is a trap worth naming: with the
// correctly-ordered pusher present, -DWEAK_IMMEDIATE PASSES. Any single notify wakes the worker
// whoever sent it, so while a seq_cst pusher is racing, the worker can only reach SLEEPING in
// executions where that pusher already signalled -- if it skipped, the total-order argument forces
// the worker to see the work and stay awake. A correctly handled flag MASKS a broken one. The real
// system has no such guarantee, since a worker can park with only an immediate-setter racing it, so
// the weak flag has to be checked standing on its own.
//
// RULE FOR ANYONE ADDING A FOURTH INPUT to Worker()'s sleep predicate: it must be seq_cst on both
// the store and the load, and it must appear in this model with its own negative control. A proof
// covers what it modelled and nothing else. That sentence cost a release.
