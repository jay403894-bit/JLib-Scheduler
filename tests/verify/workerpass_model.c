// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the WORKER'S PASS over its publish sites -- the edge-triggered hint, the search that
// can miss a queue, and the SECOND park after a consume. Companion to sleepwake_permit_model.c,
// which models the permit word itself and deliberately stops at that word's boundary.
//
//   WHY A SECOND FILE AND NOT A FIFTH CONTROL IN THE FIRST ONE. Jay's call, and it is right:
//   sleepwake_permit_model.c has become a table of the PERMIT WORD, and everything it still cannot
//   see is OUTSIDE the word --
//       * a hint flag CLEARED at the top of a pass, so it can be eaten before it is read
//       * a SEARCH that does not cover every publish site
//       * a SECOND PARK after a consume
//   None of those are properties of the handshake. One worker with ONE park attempt and a STICKY
//   work flag cannot express "the pass that ate the permit then missed the inbox", which is the
//   shape the leftover dispatch stalls still have room to live in.
//
//   THE TWO SITES, AND THEY ARE NOT THE SAME THING. This is the whole point of the harness:
//       g_flag    the EDGE-TRIGGERED hint. hasQueuedWork / laneWake. Set by the producer, and
//                 CLEARED BY THE WORKER AT THE TOP OF EACH PASS. It is a hint, not the work.
//       g_inbox   the WORK. Owner-drain-only, so if the owner parks with this non-empty and
//                 nothing wakes it, the task is stranded permanently rather than late.
//   A search that consults the hint instead of the queue is correct exactly until the hint is
//   consumed by the pass that then fails to look. sleepwake_permit_model.c models a STICKY flag
//   with no separate payload, which is why -DWAKE_CAS_ONLY is green over there: the recheck always
//   finds work that can never be consumed. Here it can be consumed.
//
//   THE PARK MACHINE IS NOT UNDER TEST HERE. It is the one verified in sleepwake_permit_model.c,
//   copied deliberately rather than shared, so that this file's reds cannot be blamed on it:
//   swap-wake, CAS to PARKED as the linearization point, recheck AFTER the commit, registration on
//   the wait object, pre-wait re-read, CAS (never store) to consume, spurious returns re-block.
//   COALESCING IS ALSO NOT UNDER TEST -- one waker here, so the oswake <= 1 property belongs to the
//   other file.
//
//   PASSES = 2 IS THE BOUND, AND IT IS THE POINT. One pass cannot fail this way.
//
//   Build variants -- RUN THE CONTROLS FIRST:
//     genmc -- workerpass_model.c                                  # as proposed
//     genmc -- -DSEARCH_MISS workerpass_model.c                    # yield-skip / steal-before-inbox
//     genmc -- -DSEARCH_MISS -DWAKE_CAS_ONLY workerpass_model.c    # the cell that was green wrongly
//     genmc -- -DRECHECK_FLAG_ONLY workerpass_model.c              # recheck trusts the hint
//     genmc -- -DCLEAR_AFTER_SEARCH workerpass_model.c             # clear at the BOTTOM of the pass
//     genmc -- -DSEARCH_MISS -DRECHECK_FULL workerpass_model.c      # separates the two halves
//     genmc -- -DPROBE_BLOCKED workerpass_model.c                  # reachability, must go red
//
//   -DPROBE_BLOCKED IS NOT A CONTROL, IT IS A REACHABILITY CHECK. Every green in this file is
//   worthless if the worker never actually reaches the blocked terminal state, and that has now
//   bitten this repository five times in one file. It asserts(0) where the strand would be counted.
//   Run it FIRST; if it does not go red, no other result here means anything.
//
//   RESULTS: recorded at the bottom.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

/* VALUES MATCH Thread::WorkerState EXACTLY -- see the note in sleepwake_permit_model.c. */
#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2

/* THE WORKER GETS TWO PASSES. One is what the other model has, and one cannot express a strand
   that requires a consume followed by a miss. */
#define PASSES 2

static _Atomic int g_permit;    /* the park word */
static _Atomic int g_flag;      /* EDGE-TRIGGERED hint: hasQueuedWork / laneWake */
static _Atomic int g_inbox;     /* THE WORK. Owner-drain-only. */
static _Atomic int g_ran;       /* tasks the worker actually consumed */
static _Atomic int g_blocked;   /* the worker terminated INSIDE a wait */

static _Atomic int g_waiting;   /* registered on the wait object */
static _Atomic int g_delivered; /* a notification REACHED a registered waiter */
static _Atomic int g_oswake;    /* notifications issued */
static _Atomic int g_spurious;  /* the OS returned a wait for no reason */

/* EVERY WRITE TO A CONTENDED WORD IS AN RMW, and that is not a stylistic preference -- GenMC
   REFUSES a graph with a plain store racing an RMW on the same location ("Unordered writes"), which
   is how -DPOSTWAIT_STORE fails in the companion file. g_flag and g_inbox each have two writers
   (producer sets, worker clears/drains), so both use exchange. */

#ifdef ACQ_REL_ONLY
  #define PUBLISH     memory_order_release
  #define OBSERVE     memory_order_acquire
  #define TRANSITION  memory_order_acq_rel
#else
  #define PUBLISH     memory_order_seq_cst
  #define OBSERVE     memory_order_seq_cst
  #define TRANSITION  memory_order_seq_cst
#endif

/* -DSEARCH_MISS IS THE PAIR Jay SPECIFIED: the pass does not look at the inbox, AND the park
   recheck consults only the hint. Either half alone is a different bug; the named control is both,
   because that is the shape the real yield-skip has -- it skips the queue and trusts the flag. */
#if defined(SEARCH_MISS) && !defined(RECHECK_FLAG_ONLY) && !defined(RECHECK_FULL)
  #define RECHECK_FLAG_ONLY 1
#endif

/* -DRECHECK_FULL FORCES THE QUEUE-CHECKING RECHECK BACK ON, so the two halves of SEARCH_MISS can be
   separated. Without it the named control is the pair Jay specified and there is no way to ask
   which half does the damage -- and "which half" is a design question with an answer worth having:
   it says whether a lossy search is survivable when the recheck covers the queue. */
#ifdef RECHECK_FULL
  #undef RECHECK_FLAG_ONLY
#endif

/* ---- the OS, as an agent ---------------------------------------------------------------------
   A spurious wakeup has no corresponding write in the program, so there is nothing for a checker to
   interleave and the branch is dead code without this. (__VERIFIER_nondet_int() does not help --
   GenMC 0.17 returns a fixed value and explores one execution.) */
static void *os_noise(void *arg) {
    (void)arg;
    atomic_store_explicit(&g_spurious, 1, PUBLISH);
    return NULL;
}

/* ---- leaving the wait for another pass -------------------------------------------------------
   Asserted AT THE TRANSITION, not in main(): a producer overwrites PARKED before the joins, so a
   final-state form of this check cannot fire. A thread going back to search must not leave PARKED
   published -- the next Wake would pay a syscall aimed at nobody, and its own next park could not
   commit, because CAS EMPTY -> PARKED fails against a word that already reads PARKED. */
static void leave_wait_for_search(void) {
    assert(atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED);
    atomic_store_explicit(&g_waiting, 0, PUBLISH);
}

/* ---- the worker ------------------------------------------------------------------------------ */
static void *worker(void *arg) {
    (void)arg;

    for (int pass = 0; pass < PASSES; ++pass) {

#ifndef CLEAR_AFTER_SEARCH
        /* TOP OF THE PASS: consume the hint. This is the line Worker() actually has, and the reason
           a sticky-flag model cannot see past it. Harmless on its own -- the hint is not the work,
           and the search below looks at the WORK. It stops being harmless the moment something
           downstream trusts the hint instead. */
        atomic_exchange_explicit(&g_flag, 0, TRANSITION);
#endif

        /* THE SEARCH. */
        int got = 0;
#ifndef SEARCH_MISS
        got = atomic_exchange_explicit(&g_inbox, 0, TRANSITION) != 0;
#else
        /* CONTROL: this pass does not look at the inbox at all -- steal-before-inbox, or the
           yield-skip on the resume inbox. It can only be told about the work by the hint, and the
           top of this pass just ate the hint. */
#endif

#ifdef CLEAR_AFTER_SEARCH
        /* CONTROL: clear at the BOTTOM of the pass instead of the top. Work published DURING the
           search sets the hint; this clear then eats it, and the recheck below finds nothing. */
        atomic_exchange_explicit(&g_flag, 0, TRANSITION);
#endif

        if (got) {
            atomic_fetch_add_explicit(&g_ran, 1, PUBLISH);
            return NULL;                    /* ran the task; nothing is stranded */
        }

        /* ---- PARK PROTOCOL. Verified in sleepwake_permit_model.c; not under test here. ---- */

        int e = ST_NOTIFIED;
        if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed))
            continue;                       /* a permit was latched: go round again */

        e = ST_EMPTY;
        if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_PARKED,
                                                     TRANSITION, memory_order_relaxed)) {
            if (e == ST_NOTIFIED) {
                int e2 = ST_NOTIFIED;
                atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                        TRANSITION, memory_order_relaxed);
            }
            continue;
        }

        /* PARKED is published. THE LAST LOOK, and WHAT IT LOOKS AT IS THE QUESTION THIS FILE ASKS. */
        {
#ifdef RECHECK_FLAG_ONLY
            /* CONTROL: trust the hint. Correct exactly until the hint is consumed by a pass that
               then fails to look at the queue behind it. */
            const int live = atomic_load_explicit(&g_flag, OBSERVE) != 0;
#else
            /* The hint is a hint; the QUEUE is the truth. Checking both means a hint eaten at the
               top of the pass costs a wasted wake, never a strand. */
            const int live = (atomic_load_explicit(&g_flag,  OBSERVE) != 0)
                          || (atomic_load_explicit(&g_inbox, OBSERVE) != 0);
#endif
            if (live) {
                int e3 = ST_PARKED;
                if (atomic_compare_exchange_strong_explicit(&g_permit, &e3, ST_EMPTY,
                                                            TRANSITION, memory_order_relaxed))
                    continue;               /* cancelled the sleep */
                int e4 = ST_NOTIFIED;
                atomic_compare_exchange_strong_explicit(&g_permit, &e4, ST_EMPTY,
                                                        TRANSITION, memory_order_relaxed);
                continue;
            }
        }

        /* REGISTER ON THE WAIT OBJECT, then the pre-wait re-read that makes an address wait correct. */
        atomic_store_explicit(&g_waiting, 1, PUBLISH);
        if (atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED) {
            leave_wait_for_search();
            continue;
        }

        /* RETURN FROM THE WAIT. The gate must NOT be a read of the permit word -- that is the
           pre-wait re-read handed out for free, and it silently repairs the control that removes
           it. A wait returns because something REACHED a registered waiter, or because the OS felt
           like it. */
        {
            const int delivered_to_me = atomic_load_explicit(&g_delivered, OBSERVE) != 0;
            const int spurious        = atomic_load_explicit(&g_spurious,  OBSERVE) != 0;

            if (!delivered_to_me && !spurious) {
#ifdef PROBE_BLOCKED
                assert(0);                  /* REACHABILITY PROBE -- must fire */
#endif
                atomic_store_explicit(&g_blocked, 1, PUBLISH);
                return NULL;                /* still inside the wait */
            }

            if (atomic_load_explicit(&g_permit, OBSERVE) == ST_PARKED) {
                /* SPURIOUS: no permit. Re-block rather than fall through -- falling through would
                   leave PARKED published with nobody behind it. */
#ifdef PROBE_BLOCKED
                assert(0);
#endif
                atomic_store_explicit(&g_blocked, 1, PUBLISH);
                return NULL;
            }

            /* Real delivery. CAS, never store: the word is only cleared if it is still the permit
               we were told about. */
            int e5 = ST_NOTIFIED;
            atomic_compare_exchange_strong_explicit(&g_permit, &e5, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed);
            leave_wait_for_search();
            /* AND ROUND AGAIN -- this is the second park the other model cannot reach. */
        }
    }

    return NULL;                            /* ran out of passes: not blocked, not a strand */
}

/* ---- the producer ----------------------------------------------------------------------------
   Publishes the WORK first, then the HINT, then wakes. That order is not optional: a hint visible
   before the work it announces sends a worker to look at an empty queue. */
static void *producer(void *arg) {
    (void)arg;
    atomic_exchange_explicit(&g_inbox, 1, TRANSITION);
    atomic_exchange_explicit(&g_flag,  1, TRANSITION);

#ifdef WAKE_CAS_ONLY
    /* CONTROL. "CAS if PARKED". In the companion file this is GREEN, because a sticky work flag
       lets the post-commit recheck cover for the permit the failed CAS never latched. Here the flag
       can be consumed, so pair it with -DSEARCH_MISS and that cover is gone. */
    {
        int e = ST_PARKED;
        if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_NOTIFIED,
                                                    TRANSITION, memory_order_relaxed)) {
            atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
            if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
                atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
        }
    }
#else
    {
        const int prev = atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
        if (prev == ST_PARKED) {
            atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
            /* DELIVERY, not issuance: a notify aimed at a thread that has not registered yet
               reaches nobody. Bumped in EVERY arm -- wiring this to one arm only is what made the
               companion file's WAKE_CAS_ONLY cell fail on its own success case. */
            if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
                atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
        }
    }
#endif
    return NULL;
}

int main(void) {
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_flag, 0);
    atomic_init(&g_inbox, 0);
    atomic_init(&g_ran, 0);
    atomic_init(&g_blocked, 0);
    atomic_init(&g_waiting, 0);
    atomic_init(&g_delivered, 0);
    atomic_init(&g_oswake, 0);
    atomic_init(&g_spurious, 0);

    pthread_t w, p, os;
    pthread_create(&w,  NULL, worker,   NULL);
    pthread_create(&p,  NULL, producer, NULL);
    pthread_create(&os, NULL, os_noise, NULL);
    pthread_join(w,  NULL);
    pthread_join(p,  NULL);
    pthread_join(os, NULL);

    const int inbox   = atomic_load(&g_inbox);
    const int blocked = atomic_load(&g_blocked);
    const int permit  = atomic_load(&g_permit);

    /* THE STRAND. The worker is asleep, the inbox still holds a task, and no permit is outstanding.
       The producer has finished -- it was joined -- so nothing will ever issue another wake, and the
       inbox is owner-drain-only, so no other thread can take the task either. Permanent.

       `permit != ST_NOTIFIED` IS LOAD-BEARING AND IS NOT A GET-OUT. A latched permit means the
       address wait returns by value-compare and the worker gets another pass; the model simply ran
       out of passes at PASSES = 2. Dropping that term would report the model's own bound as a bug --
       the same over-strong-assertion mistake the companion file made and GenMC caught. */
    assert(!(blocked == 1 && inbox > 0 && permit != ST_NOTIFIED));

    return 0;
}

/* =================================================================================================
   RESULTS -- fill in from an actual run before citing this file.

     ~/genmc/RelWithDebInfo/bin/genmc -- tests/verify/workerpass_model.c

   GenMC is not on PATH; invoke by full path. Built inside WSL (~/genmc), not under /mnt/c.
   RUN 2026-08-31, GenMC v0.17.0, WSL. One worker (TWO passes), one producer, one OS agent.

   | variant                                    | outcome              | complete execs |
   |--------------------------------------------|----------------------|----------------|
   | -DPROBE_BLOCKED (reachability, not a control)| **fires** -- good   | --             |
   | (as proposed)                              | no errors            | 20             |
   | -DSEARCH_MISS                              | **SAFETY VIOLATION** | 2              |
   | -DSEARCH_MISS -DWAKE_CAS_ONLY              | **SAFETY VIOLATION** | 1              |
   | -DSEARCH_MISS -DRECHECK_FULL               | no errors            | 36             |
   | -DRECHECK_FLAG_ONLY                        | no errors            | 17             |
   | -DCLEAR_AFTER_SEARCH                       | no errors            | 24             |
   | -DCLEAR_AFTER_SEARCH -DRECHECK_FLAG_ONLY   | no errors            | 30             |
   | -DWAKE_CAS_ONLY                            | no errors            | 19             |

   THE STRAND IS REAL AND IT NEEDS BOTH PASSES. The -DSEARCH_MISS counterexample, in order:

     producer   inbox = 1, flag = 1, swap(permit) -> NOTIFIED
     pass 1     exchange(flag, 0) READS 1          <-- the hint is EATEN, unread
     pass 1     search does not look at the inbox
     pass 1     fast-path CAS NOTIFIED -> EMPTY succeeds  <-- the PERMIT is EATEN
     pass 2     clear (already 0), search misses, CAS EMPTY -> PARKED commits
     pass 2     recheck reads flag == 0, parks
     terminal   inbox = 1, blocked, permit = PARKED

   The consume is in PASS 1 and the strand is in PASS 2. A one-pass harness -- which is what
   sleepwake_permit_model.c is -- cannot produce this shape at all. Neither can a harness whose work
   flag is sticky, because the hint can never be eaten.

   THE INVARIANT THIS ACTUALLY ESTABLISHES, and it is narrower and more useful than "do not skip the
   inbox": EVERY PARK ATTEMPT MUST READ THE ACTUAL QUEUE AT LEAST ONCE. Not the hint -- the queue.
   Where it reads it does not matter:
     - search reads the queue, recheck trusts the hint   (-DRECHECK_FLAG_ONLY)      -> GREEN
     - search misses, recheck reads the queue            (-DSEARCH_MISS -DRECHECK_FULL) -> GREEN
     - hint cleared late, search reads the queue         (-DCLEAR_AFTER_SEARCH)      -> GREEN
     - hint cleared late AND recheck trusts the hint     (both)                      -> GREEN
     - NEITHER reads the queue                           (-DSEARCH_MISS)             -> **RED**
   The hint is never load-bearing on its own, and the clear-at-the-top is harmless BY ITSELF. What
   is load-bearing is that the queue is consulted somewhere in the pass. That is a cheaper rule to
   hold than "never skip the inbox", and it is the one to check the yield-skip against.

   -DWAKE_CAS_ONLY IS GREEN HERE TOO, AND FOR THE SAME REASON AS IN THE COMPANION FILE -- the search
   reads the queue, so the permit the failed CAS never latched does not matter. It goes RED the
   moment it is paired with -DSEARCH_MISS, in ONE execution. So the corrected reading stands: this
   harness does not show swap-wake is necessary on its own, but it shows the swap is what remains
   when the search stops covering for it. KEEP THE SWAP.

   -DACQ_REL_ONLY IS NOT A CONTROL IN THIS FILE and is not listed above. It reports no errors, and
   that means "not tested here", exactly as the first version of the companion file's cell did
   before a wait object made it meaningful. The ordering question is settled in
   sleepwake_permit_model.c, where -DACQ_REL_ONLY is RED. Do not cite this file on ordering.

   STILL NOT MODELLED:
     - MORE THAN TWO PASSES, and more than one queue. A worker with an inbox, a deque and a steal
       list can miss a different site on each pass; this models one site and two passes.
     - A SECOND WORKER, hence nothing about which of two sleepers a wake reaches.
     - THE DRAIN OBLIGATION that makes wake coalescing sound -- one producer here on purpose.
   ============================================================================================== */
