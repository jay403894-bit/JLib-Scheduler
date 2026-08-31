// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the PROPOSED thread park/unpark machine: a three-state PERMIT word, CAS/swap only.
// Companion to sleepwake_model.c (the handshake that ships today) and deque_model.c; see the latter
// for what a model checker is, and the former for why sleep is modelled as a terminal state.
//
//   THE DIAGNOSIS THIS MODELS -- Jay's, and it is a structural one rather than a measurement.
//
//   The FIBER machine is the verified one. The THREAD park/unpark machine does not implement the
//   same handshake, and that is the gap. Sleep IS suspend; wake IS resume. The fiber side already
//   carries the window the thread side lacks:
//
//       fiber                 thread
//       WANTS_SUSPEND         about to block, not published yet
//       SUSPENDED             PARKED     -- the thread is actually waiting
//       SUSPEND_SIGNALED      NOTIFIED   -- the wake won the race
//       READY + requeue       consume the permit, run again
//       Resume                Wake
//
//   If the thread side is only "set sleeping / clear sleeping", a wake landing between "I saw no
//   work" and "I blocked" is DROPPED. The worker then sleeps with work in its local deque, its
//   inbox, or a just-resumed fiber. That is the stall.
//
//   THE MACHINE. One word, three states, and every write to it is an RMW:
//     EMPTY     not parked, no permit
//     PARKED    committed sleep
//     NOTIFIED  a resume permit -- the wake already happened
//
//   SLEEP:
//     1. Fast path: CAS NOTIFIED -> EMPTY. Succeeds => a resume arrived before the suspend
//        committed; do not sleep.
//     2. Publish intent: CAS EMPTY -> PARKED.
//     3. If that fails because the word is NOTIFIED, consume it (NOTIFIED -> EMPTY) and return.
//        Same shape as the fiber parker handling WANTS_SUSPEND -> SUSPEND_SIGNALED itself.
//     4. ONLY once PARKED is visible: the last look at local + inbox, then block.
//     5. Returning from the OS wait the word must be NOTIFIED; swap it back to EMPTY. A spurious
//        wake with PARKED still set means wait again.
//
//   WAKE:
//     swap(NOTIFIED) -- NOT "CAS if PARKED".
//       previous EMPTY or NOTIFIED -> the permit is latched; do not touch the OS thread.
//       previous PARKED            -> now, and only now, futex_wake / notify_one.
//
//   SWAP IS LOAD-BEARING, and -DWAKE_CAS_ONLY below is the control that shows it. A CAS which bails
//   when it already sees NOTIFIED loses the release edge the sleeper's park path has to synchronise
//   with, and drops the permit when the sleeper has not yet reached PARKED.
//
//   THE CAS TO PARKED IS THE LINEARIZATION POINT with wake -- exactly as the fiber publishes
//   SUSPENDED after its context is saved. The final local/inbox/steal check must come AFTER it, or
//   the "empty your own late" loop is recreated on the thread instead of the fiber.
//
//   NOT THE FIBER'S WORD. Fiber Resume enqueues a task; thread Wake only unparks a core. Sharing
//   one word is how "fiber is READY, worker is PARKED, nobody runs" happens.
//
//   THE TWO MECHANISMS ARE INDEPENDENTLY SUFFICIENT, which the controls established rather than
//   assumed, and it is the most useful thing this file has to say about the design:
//
//     WAKE_CAS_ONLY alone  -- the CAS fails while the word is EMPTY so no permit is latched, BUT
//                             the work was published first, so the recheck-after-PARKED sees it
//                             and cancels the sleep. No bug.
//     NO_RECHECK alone     -- the worker commits to PARKED blind, BUT swap(NOTIFIED) then observes
//                             prev == PARKED, owes the OS wake, and performs it. No bug.
//     BOTH removed         -- SAFETY VIOLATION. Work published, thread committed PARKED, no wake.
//
//   So this is belt-and-braces on purpose: either the wake catches a committed sleeper, or the
//   sleeper catches work published before it committed. Removing one is survivable and removing
//   both is the stall. Anyone tempted to simplify either half should reproduce the combined control
//   first.
//
//   Build variants -- RUN THE CONTROLS FIRST:
//     genmc -- sleepwake_permit_model.c                                  # as proposed
//     genmc -- -DWAKE_CAS_ONLY -DNO_RECHECK sleepwake_permit_model.c     # MUST fail
//     genmc -- -DWAKE_CAS_ONLY sleepwake_permit_model.c                  # passes: recheck covers
//     genmc -- -DNO_RECHECK sleepwake_permit_model.c                     # passes: swap covers
//     genmc -- -DACQ_REL_ONLY sleepwake_permit_model.c                   # ordering probe
//
//   A NEGATIVE CONTROL THAT PASSES MEANS THE HARNESS IS BROKEN, NOT THAT THE CODE IS GOOD. The
//   first draft of this file asserted its interesting property in main() AFTER THE JOINS -- where
//   the window it described no longer existed -- and duly reported "no errors" for a variant built
//   to fail. It also carried a second assertion ending in `&& 0`. Both are gone. Confirm the
//   combined control fails before believing the green run.
//
//   RESULTS: recorded at the bottom.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define ST_EMPTY     0
#define ST_PARKED    1
#define ST_NOTIFIED  2

static _Atomic int g_permit;   /* the park word */
static _Atomic int g_work;     /* the task the producer published */
static _Atomic int g_oswake;   /* futex_wake / notify_one actually performed */

#ifdef ACQ_REL_ONLY
  /* The StoreLoad pairs are on DIFFERENT locations -- the producer publishes work then observes the
     permit; the worker publishes PARKED then observes work. That is the one ordering acquire/release
     does not give. Incidentally fine on x86 (lock-prefixed RMWs drain the store buffer); a genuine
     hole on AArch64, which this ships on. */
  #define PUBLISH     memory_order_release
  #define OBSERVE     memory_order_acquire
  #define TRANSITION  memory_order_acq_rel
#else
  #define PUBLISH     memory_order_seq_cst
  #define OBSERVE     memory_order_seq_cst
  #define TRANSITION  memory_order_seq_cst
#endif

/* ---- the worker deciding to park -------------------------------------------------------------
   Reached only after local, inbox and steal have all come up empty. */
static void *worker(void *arg) {
    (void)arg;

    /* 1. FAST PATH: a permit is already latched, so a resume arrived before this suspend could
          commit. Consume it and go round again -- never sleep on an outstanding permit. */
    int e = ST_NOTIFIED;
    if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_EMPTY,
                                                TRANSITION, memory_order_relaxed))
        return NULL;                       /* continue the search loop */

    /* 2. PUBLISH INTENT. This CAS is the LINEARIZATION POINT with Wake: after it succeeds, a waker
          that swaps sees PARKED and knows it owns the OS wake. */
    e = ST_EMPTY;
    if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_PARKED,
                                                 TRANSITION, memory_order_relaxed)) {
        /* 3. Failed, and the reason is readable: a waker latched a permit while we were deciding.
              Consume it ourselves rather than sleeping -- the parker handles SUSPEND_SIGNALED. */
        if (e == ST_NOTIFIED) {
            int e2 = ST_NOTIFIED;
            atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed);
        }
        return NULL;                       /* continue the search loop */
    }

    /* PARKED is now visible to any waker. */

#ifndef NO_RECHECK
    /* 4. THE LAST LOOK, and it must be here -- after PARKED is published, not before. Checking
          earlier recreates the dropped-wake window this machine exists to close: work published
          between the check and the commit would find a waker that latches a permit we never see,
          because we would already be inside block().

          CONTROL -DNO_RECHECK removes it and must produce a lost wakeup. */
    if (atomic_load_explicit(&g_work, OBSERVE) != 0) {
        int e3 = ST_PARKED;
        if (atomic_compare_exchange_strong_explicit(&g_permit, &e3, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed))
            return NULL;                   /* cancelled the sleep; run the work */
        /* Lost the cancel to a waker: the word is NOTIFIED, so a permit is latched and the block
           below would return immediately. Consume and continue. */
        int e4 = ST_NOTIFIED;
        atomic_compare_exchange_strong_explicit(&g_permit, &e4, ST_EMPTY,
                                                TRANSITION, memory_order_relaxed);
        return NULL;
    }
#endif

    /* 5. block(). Modelled as TERMINATION, per sleepwake_model.c: "every parked worker eventually
          runs" is liveness and a stateless checker cannot express it, so the bad outcome is made a
          reachable-state question instead -- work queued, thread committed PARKED, no OS wake. */
    return NULL;
}

/* ---- the waker -------------------------------------------------------------------------------
   Publishes the task, then latches a permit. */
static void *waker(void *arg) {
    (void)arg;
    atomic_fetch_add_explicit(&g_work, 1, PUBLISH);

#ifdef WAKE_CAS_ONLY
    /* CONTROL. "CAS if PARKED" -- wake only when the sleeper is already committed. Looks
       sufficient and is not: between the worker's fast-path check and its CAS to PARKED the word
       is EMPTY, so this CAS fails, no permit is latched, and the worker then commits to PARKED and
       blocks on work that was already published. MUST produce a lost wakeup. */
    int e = ST_PARKED;
    if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_NOTIFIED,
                                                TRANSITION, memory_order_relaxed))
        atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
#else
    /* THE PROPOSED WAKE: swap unconditionally. The permit is latched no matter which state the
       sleeper was in, so it cannot be dropped; only the PREVIOUS value decides whether an OS wake
       is owed. Swap rather than CAS also keeps the release edge the sleeper synchronises with. */
    const int prev = atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    if (prev == ST_PARKED)
        atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
    /* prev EMPTY or NOTIFIED: latched, and the OS thread is not touched. */
#endif
    return NULL;
}

int main(void) {
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_work, 0);
    atomic_init(&g_oswake, 0);

    pthread_t w, p;
    pthread_create(&w, NULL, worker, NULL);
    pthread_create(&p, NULL, waker, NULL);
    pthread_join(w, NULL);
    pthread_join(p, NULL);

    const int work   = atomic_load(&g_work);
    const int permit = atomic_load(&g_permit);
    const int oswake = atomic_load(&g_oswake);

    /* NO LOST WAKEUP. Work was published, the thread is committed PARKED, and no OS wake was
       performed -- so nothing will ever run it. Inboxes are owner-drain-only, so this is a
       permanent strand rather than a delay: the stall, in its terminal form. */
    assert(!(work > 0 && permit == ST_PARKED && oswake == 0));

    /* A SECOND ASSERTION WAS HERE AND IT WAS VACUOUS -- it ended in `&& 0`, so it could never fire.
       Deleted rather than repaired: a latched permit with the worker back in its search loop is the
       BENIGN case (it is consumed on the next pass, or becomes one spurious wake), and the harmful
       version is already covered by the assertion above. An assertion that cannot fail is worse than
       no assertion, because it reads like coverage. */

    return 0;
}

/* =================================================================================================
   RESULTS -- fill in from an actual run before citing this file.

     ~/genmc/RelWithDebInfo/bin/genmc -- tests/verify/sleepwake_permit_model.c
     ~/genmc/RelWithDebInfo/bin/genmc -- -DWAKE_CAS_ONLY tests/verify/sleepwake_permit_model.c
     ~/genmc/RelWithDebInfo/bin/genmc -- -DNO_RECHECK    tests/verify/sleepwake_permit_model.c
     ~/genmc/RelWithDebInfo/bin/genmc -- -DACQ_REL_ONLY  tests/verify/sleepwake_permit_model.c

   GenMC is not on PATH; invoke by full path. Built inside WSL (~/genmc), not under /mnt/c --
   reading the model across /mnt/c at run time is fine.

   RUN 2026-08-31, GenMC v0.17.0, WSL:

   | variant                      | outcome                          |
   |------------------------------|----------------------------------|
   | (as proposed)                | no errors, 5 complete executions |
   | -DWAKE_CAS_ONLY -DNO_RECHECK | **SAFETY VIOLATION**             |
   | -DWAKE_CAS_ONLY              | no errors, 4 executions          |
   | -DNO_RECHECK                 | no errors, 3 executions          |
   | -DACQ_REL_ONLY               | no errors, 5 executions          |

   THE PROPOSED MACHINE HOLDS, and the combined control proves the harness can express the bug --
   without that line the green result above would be worthless.

   -DACQ_REL_ONLY PASSING IS NOT A LICENCE TO WEAKEN THE ORDERING. It says this harness does not
   distinguish them, which is a statement about the harness: every write to the permit word here is
   an RMW (CAS or swap), and RMWs are totally ordered per location regardless of the memory order
   requested, so the StoreLoad hazard acq_rel would expose has no plain store to expose it through.
   sleepwake_model.c's ACQ_REL_ONLY DOES fail, because that protocol stores. Read this cell as
   "not tested here", not as "acq_rel is sufficient".

   EXECUTION COUNTS ARE SMALL (3-5) because the harness is one worker and one waker with sleep
   modelled as termination. That is enough to reach the bug -- the combined control finds it -- but
   it does not cover multiple concurrent wakers, a second worker, or the return-from-wait path where
   the word must read NOTIFIED and be swapped back to EMPTY. Those are the obvious extensions if the
   machine is built.
   ============================================================================================== */
   ============================================================================================== */
