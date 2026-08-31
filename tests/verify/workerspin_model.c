// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the SPIN at src/Thread.cpp:3023 -- the pre-park recheck sending a worker back to
// search when the search cannot find anything. Third in the set:
//
//     sleepwake_permit_model.c   the PERMIT WORD. Handshake, ordering, consume path.
//     workerpass_model.c         the STRAND. work published, owner parked, no wake.
//     workerspin_model.c         the SPIN.   hint said work, search finds nothing, never parks.
//
//   THE SPIN IS A SAFETY PROPERTY, and that is the whole reason this file can exist. Jay's point:
//   the dump at 3023 is a REACHABLE BAD STATE, not a fairness question, so it does not need a
//   liveness checker -- which is fortunate, because GenMC 0.17 has no fairness, no branching
//   nondeterminism, and a bounded loop always terminates. Assert THE LIE instead of the progress:
//
//       STRAND   work published AND owner parked AND no wake            -> workerpass_model.c
//       SPIN     the recheck fired AND the search cannot find anything  -> here
//
//   Neither is "the task eventually runs". THERE IS NO THIRD ASSERTION THAT MEANS PROGRESS, and
//   `assert(g_found)` at the end of main() is liveness in a false moustache: it is wrong when the
//   worker parked correctly on a wake the OS agent has not delivered, and right when the worker
//   span twice and the pass bound ran out.
//
//   WHY IT IS NOT `!(hint && !found)`. That form fires on the BENIGN case -- the search looked, the
//   queue was empty, a producer published, the recheck saw it and sent the worker round to collect
//   it. One wasted lap is the mechanism working. The LIE is narrower: the recheck fired and THE
//   SOURCES THE SEARCH ACTUALLY DRAINS ARE EMPTY, so the next lap must come back empty too, and the
//   one after that. That is the difference between a wasted lap and thirty-one workers pinned AWAKE
//   with busy = 0.
//
//   THE DISJUNCTION IS COPIED, NOT INVENTED. An invented hint formula makes every result here
//   fiction. src/Thread.cpp:3003 reads, in full:
//
//       hasQueuedWork || laneWake
//           || !hiPriInboxes[qIndex]->quiescent()
//           || !loPriInboxes[qIndex]->quiescent()
//           || !resumedInboxes[qIndex]->quiescent()
//
//   and the search drains all three queues -- hiPri at 2252, resumed at 2375, loPri via
//   drainOwnInbox at 1318. SO THE SETS MATCH, WITH ONE EXCEPTION, AND THE EXCEPTION IS THE MODEL:
//
//       drainOwnInbox() opens `if (task_to_run || reservedForHiPri) return false;`
//
//   A RESERVED worker does not drain its loPri inbox. The recheck's loPri term carries no such
//   guard. So for a reserved worker the recheck names a queue the search will not touch, which is
//   the -DSEARCH_MISS build below. Whether a reserved worker can ever HOLD a loPri task is a
//   placement question this file does not answer -- it answers what happens if it does.
//
//   g_flag IS hasQueuedWork AND laneWake COLLAPSED. Both are edge-triggered, both are cleared at
//   the top of the pass, and nothing here distinguishes them. g_lopri stands for the one queue whose
//   drain is guarded; hiPri and resumed are not modelled because their drains are unguarded and
//   therefore cannot produce the mismatch.
//
//   THE PARK MACHINE IS NOT UNDER TEST and the STRAND IS NOT ASSERTED HERE. Both belong to the
//   other two files. A worker that parks with hint == 0 and the queue non-empty is a strand, and
//   asserting it here as well would mean two files disagreeing about which one owns that cell.
//
//   Build variants -- RUN THE PROBE FIRST:
//     genmc -- -DPROBE_HINT workerspin_model.c            # reachability: MUST go red
//     genmc -- workerspin_model.c                         # as shipped (non-reserved worker)
//     genmc -- -DSEARCH_MISS workerspin_model.c           # reserved worker: drain guarded, recheck not
//     genmc -- -DRECHECK_HINTS_ONLY workerspin_model.c    # recheck drops the queue terms
//     genmc -- -DSEARCH_MISS -DRECHECK_HINTS_ONLY workerspin_model.c
//
//   RESULTS: recorded at the bottom.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2

static _Atomic int g_lopri;    /* the loPri inbox. Owner-drain-only. */
static _Atomic int g_flag;     /* hasQueuedWork / laneWake, collapsed. EDGE-TRIGGERED. */
static _Atomic int g_permit;   /* EMPTY / NOTIFIED / PARKED */

static _Atomic int g_hint;     /* the recheck fired and sent the worker round again */
static _Atomic int g_found;    /* the search took a task */
static _Atomic int g_parked;   /* the worker committed to sleep */

#define PUBLISH     memory_order_seq_cst
#define OBSERVE     memory_order_seq_cst
#define TRANSITION  memory_order_seq_cst

/* ---- THE SEARCH'S SOURCE SET, DEFINED ONCE -----------------------------------------------------
 *
 * ONE SWITCH FEEDS BOTH THE DRAIN AND THE "WOULD IT FIND ANYTHING" PROBE. If those two could
 * disagree the assertion below would be testing a formula I made up rather than the search, which
 * is the failure mode this whole set of files keeps hitting. They cannot drift: there is one
 * #define and both functions read it.
 *
 * SEARCH_MISS models drainOwnInbox()'s `if (task_to_run || reservedForHiPri) return false;` -- a
 * reserved worker whose loPri inbox the recheck still names. */
/* -DPLACE_ON_RESERVED NAMES THE CONFIGURATION -- this worker is RESERVED and an ordinary push
 * reached its loPri inbox. That was reachable until 2026-08-31: PickNextWorker's reserved-band mask
 * lived inside `if (const size_t baseF = GetAwakeFloorBase())`, so with the floor base at 0 the
 * awake bitmap still carried bits below K and a CorePref::Default task could be handed to a
 * reserved index. The mask is now unconditional, above the bitmap pick.
 *
 * IT IMPLIES SEARCH_MISS RATHER THAN ADDING A SECOND CONDITION, and that is not a shortcut: being
 * reserved IS the reason drainOwnInbox returns early. One property, not two.
 *
 * WHAT THIS BUILD DOES AND DOES NOT SHOW. It is mechanically identical to -DSEARCH_MISS, so it
 * proves nothing extra about the model. It exists to keep the configuration NAMED and red, so the
 * cost of the mask is written down somewhere that runs. THIS FILE CANNOT VERIFY THE C++ FIX -- no
 * model here reads PickNextWorker. Whether an ordinary push can still reach loPriInboxes[q < K] is
 * a question for the source and for a runtime test, not for GenMC. */
#ifdef PLACE_ON_RESERVED
  #ifndef SEARCH_MISS
    #define SEARCH_MISS 1
  #endif
#endif

#ifdef SEARCH_MISS
  #define SEARCH_SEES_LOPRI 0
#else
  #define SEARCH_SEES_LOPRI 1
#endif

/* Takes the task. */
static int search_drain(void) {
#if SEARCH_SEES_LOPRI
    return atomic_exchange_explicit(&g_lopri, 0, TRANSITION) != 0;
#else
    return 0;
#endif
}

/* Does NOT take it: "if this worker searched again right now, would it come back with anything?"
   Exactly the sources search_drain() reads, by construction. */
static int search_would_find(void) {
#if SEARCH_SEES_LOPRI
    return atomic_load_explicit(&g_lopri, OBSERVE) != 0;
#else
    return 0;
#endif
}

/* ---- THE PRE-PARK RECHECK, copied from src/Thread.cpp:3003 -------------------------------------
   `running` and `paused` are omitted deliberately: neither is a work source, and a shutdown or a
   pause exits the loop rather than sending it round again. */
static int recheck_hint(void) {
    int h = atomic_load_explicit(&g_flag, OBSERVE) != 0;          /* hasQueuedWork || laneWake */
#ifndef RECHECK_HINTS_ONLY
    h = h || (atomic_load_explicit(&g_lopri, OBSERVE) != 0);      /* !loPriInboxes->quiescent() */
#endif
    return h;
}

/* ---- the worker: ONE pass ---------------------------------------------------------------------
   One is enough. The spin is a property of a SINGLE pass -- it is "this pass will go round again
   and the next one cannot do better" -- so a second pass would add state space and no reach. The
   strand needed two passes; this does not. */
static void *worker(void *arg) {
    (void)arg;

    /* LOOP TOP: consume the edge-triggered hint. src/Thread.cpp:1624. */
    atomic_exchange_explicit(&g_flag, 0, TRANSITION);

    if (search_drain()) {
        atomic_store_explicit(&g_found, 1, PUBLISH);
        return NULL;                                  /* ran a task: progress, nothing to assert */
    }

    if (recheck_hint()) {
        atomic_store_explicit(&g_hint, 1, PUBLISH);

        /* THIS IS src/Thread.cpp:3023. The recheck fired, so the worker `continue`s to the top of
           the loop -- skipping the backoff at the bottom, which is what makes the spin hot rather
           than merely wasteful. The lie is that there is something to go back for.

           ASSERTED HERE, NOT IN main(). Same lesson as the companion file's assertion 3: a producer
           publishing, or this worker's own loop-top clear on the next pass, overwrites the evidence
           long before the joins. A final-state form of this check cannot fire. */
#ifdef PROBE_HINT
        assert(0);                                    /* REACHABILITY -- must fire */
#endif
        assert(search_would_find());
        return NULL;
    }

    /* Otherwise the park protocol runs -- verified in sleepwake_permit_model.c, and the strand it
       can produce is asserted in workerpass_model.c. Neither is re-litigated here. */
    /* EXCHANGE, NOT STORE -- and this file got it wrong on the first run. A plain store here races
       the producer's swap and GenMC refuses the graph outright with "Unordered writes", exactly as
       -DPOSTWAIT_STORE does in sleepwake_permit_model.c. Every write to the permit word is an RMW;
       writing the rule down in a header comment does not exempt the file that wrote it. */
    atomic_exchange_explicit(&g_permit, ST_PARKED, TRANSITION);
    atomic_store_explicit(&g_parked, 1, PUBLISH);
    return NULL;
}

/* ---- the producer -----------------------------------------------------------------------------
   WORK FIRST, THEN THE HINT. A hint visible before the work it announces sends a worker to look at
   an empty queue -- which is this file's bug, arrived at from the producer side. */
static void *producer(void *arg) {
    (void)arg;
    atomic_exchange_explicit(&g_lopri, 1, TRANSITION);
    atomic_exchange_explicit(&g_flag,  1, TRANSITION);
    atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    return NULL;
}

int main(void) {
    atomic_init(&g_lopri, 0);
    atomic_init(&g_flag, 0);
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_hint, 0);
    atomic_init(&g_found, 0);
    atomic_init(&g_parked, 0);

    pthread_t w, p;
    pthread_create(&w, NULL, worker,   NULL);
    pthread_create(&p, NULL, producer, NULL);
    pthread_join(w, NULL);
    pthread_join(p, NULL);

    /* NOTHING IS ASSERTED HERE, AND THAT IS THE POINT. The spin is transient by construction and
       the strand belongs to workerpass_model.c. `assert(g_found)` would be liveness wearing
       safety's clothes -- false whenever the worker parked correctly on an undelivered wake. */
    (void)atomic_load(&g_hint);
    (void)atomic_load(&g_found);
    (void)atomic_load(&g_parked);
    return 0;
}

/* =================================================================================================
   RESULTS -- fill in from an actual run before citing this file.

     ~/genmc/RelWithDebInfo/bin/genmc -- tests/verify/workerspin_model.c

   GenMC is not on PATH; invoke by full path. Built inside WSL (~/genmc), not under /mnt/c.
   RUN 2026-08-31, GenMC v0.17.0, WSL. One worker (ONE pass), one producer.

   | variant                            | outcome              | complete execs |
   |------------------------------------|----------------------|----------------|
   | -DPROBE_HINT (reachability)        | **fires** -- good    | 4              |
   | (as shipped, non-reserved worker)  | no errors            | 6              |
   | -DSEARCH_MISS                      | **SAFETY VIOLATION** | 2              |
   | -DPLACE_ON_RESERVED                | **SAFETY VIOLATION** | 2              |
   | -DPLACE_ON_RESERVED -DSEARCH_MISS  | **SAFETY VIOLATION** | 2              |
   | -DRECHECK_HINTS_ONLY               | no errors            | 5              |
   | -DSEARCH_MISS -DRECHECK_HINTS_ONLY | **SAFETY VIOLATION** | 4              |

   THE SHIPPED NON-RESERVED PATH IS CLEAN. The recheck names loPri and the search drains loPri, so
   the recheck cannot lie: the only way to reach it with the flag set is with the queue still full,
   and the next lap collects it. One wasted lap, which is the mechanism working.

   -DRECHECK_HINTS_ONLY IS GREEN, AND THE REASON IS THE PRODUCER'S ORDER, not luck. Work is
   published BEFORE the hint, so a recheck that trusts only the hint still cannot fire on an empty
   queue. Reverse those two stores in the producer and this cell goes red. It is green because of a
   property of the PUSH path, not of the recheck -- do not read it as "the queue terms in the
   recheck are optional". They are what makes the STRAND impossible (workerpass_model.c), which is a
   different assertion in a different file.

   -DSEARCH_MISS IS THE CELL THAT MATTERS, AND IT HAS A NAMED SITE IN THE SOURCE. It models a
   recheck that names a queue the search will not drain. That is not hypothetical:

       src/Thread.cpp:1319   drainOwnInbox: `if (task_to_run || reservedForHiPri) return false;`
       src/Thread.cpp:3008   recheck:       `|| !loPriInboxes[qIndex]->quiescent()`   -- NO GUARD

   A RESERVED worker does not drain its loPri inbox; the recheck still asks about it. If such a
   worker ever holds a loPri task, the recheck fires, the `continue` skips the backoff, the search
   declines to look, and it fires again. Hot, forever, until something else changes.

   THE ITEM COULD EXIST, AND IT WAS A PLACEMENT BUG -- FIXED 2026-08-31. This file originally
   stopped at "what happens IF it exists" and pointed at K movement as the likely route. The actual
   route was simpler and needed no movement at all:

     PickNextWorker (TaskScheduler.cpp) masked the reserved band out of the awake bitmap INSIDE
     `if (const size_t baseF = GetAwakeFloorBase())`. The bitmap pick that consumes the mask sits
     AFTER that block and returns first. With the floor base at 0 the mask never ran, the pick
     returned an index in [0, K), and an ordinary CorePref::Default task was pushed to
     loPriInboxes[q < K].

   The mask is now unconditional, immediately after the bitmap is built. `j < hotN` in the function's
   tail fallback always had the right test and was simply unreachable whenever the bitmap path
   returned; the two now implement the same sentence. Reachable by configuration rather than by a
   race: EnableIoReactor() calls SetIoHotLane(1) -> SetHotWorkers(1), so every reactor app runs
   K >= 1, and any app that also set the awake floor to 0 was in it. The library default is Fbase 2,
   which is what kept it hidden.

   THE FIX WENT IN THE WRITER, NOT THE RECHECK, and the reason is in this file's own terms. Teaching
   the recheck the reservedForHiPri gate would silence it on an OWNER-DRAIN-ONLY queue, which turns
   this spin into a permanent strand -- workerpass_model.c's assertion, not this one's. Fix what
   puts the task there.

   WHAT THIS FILE STILL CANNOT SAY: whether the fix holds. No model here reads PickNextWorker.
   -DPLACE_ON_RESERVED keeps the broken configuration named and red so the cost of the mask is
   written down, but "can an ordinary push still reach loPriInboxes[q < K]" is a question for the
   source and for a runtime test.

   NOT MODELLED: hiPri and resumed inboxes (their drains are unguarded, so they cannot produce the
   mismatch); `running` and `paused` (not work sources -- they exit the loop rather than restart it);
   more than one pass (the spin is a property of a single pass); the backoff itself.
   ============================================================================================== */
