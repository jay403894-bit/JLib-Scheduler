// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the FOURTH STATE: YIELD. Fourth in the set --
//
//     sleepwake_permit_model.c   the permit word: EMPTY / NOTIFIED / PARKED
//     workerpass_model.c         the STRAND. work published, owner parked, no wake
//     workerspin_model.c         the SPIN.   recheck fired, search cannot find it
//     yieldstate_model.c         the LIE.    advertised AWAKE, thread is off the core
//
//   THE PROBLEM, and it is Jay's. A floor worker never parks, so its permit word reads EMPTY
//   forever and the bitmap advertises it as AWAKE. To avoid burning a core it does not spin flat:
//
//       if ((++spinTick & GetSpinYieldMask()) == 0) std::this_thread::yield();
//       else                                        platform::CpuRelax();
//
//   CpuRelax() is a pause. It stays on the core and costs nothing but power -- that arm is fine.
//   std::this_thread::yield() IS NOT A SMALL PAUSE. It is a request to the OS to run somebody else,
//   and the thread leaves the core. One idle pass in eight, today.
//
//   WHAT THAT COSTS, spelled out because the permit machine makes it invisible. A push aimed at
//   that worker swaps the permit, reads prev == EMPTY, and CORRECTLY issues no syscall -- EMPTY
//   means "on core, scanning, it will find this itself". Except it is not on the core. Nothing runs
//   the task until the OS reschedules the thread, which under oversubscription is a scheduling
//   quantum, not a cache miss. AND THE DUMP SAYS AWAKE, so the whole permit machine that was just
//   modelled to death reports the healthy state while the stall happens.
//
//   THE FIX IS A FOURTH STATE IN THE SAME WORD, not a second source of truth. Clearing the awake
//   BITMAP around the yield would work too and is worse: two things to keep in agreement, and the
//   history in this repository is that two predicates describing one question drift (see the term-
//   for-term note on the three park predicates in Thread.cpp).
//
//       EMPTY     on core, scanning. A push needs no syscall; the scan will find it.
//       NOTIFIED  a permit is latched.
//       PARKED    committed sleep. A push owes a syscall.
//       YIELD     ABOUT TO LEAVE, OR OFF, THE CORE. DO NOT TARGET THIS THREAD.
//
//   YIELD IS NOT PARKED AND MUST NOT BUY A SYSCALL. The thread is RUNNABLE -- it comes back on its
//   own, with no help from anybody. A futex wake aimed at a thread that is not waiting is a wasted
//   syscall, and on the floor's own hot path. So a waker that reads prev == YIELD latches the permit
//   and does nothing else, exactly as for EMPTY. What YIELD changes is PLACEMENT: it tells the
//   pusher to aim somewhere else, which is the entire point.
//
//   A HANDSHAKE BEFORE AND AFTER, and both halves are load-bearing:
//     BEFORE  CAS EMPTY -> YIELD, then leave the core. If the CAS fails the word is NOTIFIED, a
//             permit arrived while we were deciding: consume it and do not yield at all.
//     AFTER   CAS YIELD -> EMPTY. If THAT fails the word is NOTIFIED, because a producer swapped
//             while we were off the core: consume it and rescan. THE `AFTER` HALF MUST BE A CAS.
//             A plain store(EMPTY) drops that permit -- the producer already decided it owed no
//             syscall, so nothing will ever re-announce the work.
//
//   Build variants -- RUN THE CONTROLS FIRST:
//     genmc -- yieldstate_model.c                          # as proposed
//     genmc -- -DNO_YIELD_HANDSHAKE yieldstate_model.c     # MUST fail: today's code
//     genmc -- -DYIELD_STORE_BACK yieldstate_model.c       # MUST fail: store instead of CAS
//     genmc -- -DTARGET_YIELDED yieldstate_model.c         # PASSES -- and that is the finding
//     genmc -- -DPROBE_OFFCORE yieldstate_model.c          # reachability, MUST fail
//
//   THE TWO HALVES OF THE FOURTH STATE ARE NOT WORTH THE SAME, and this file separates them:
//
//     THE HANDSHAKE IS MANDATORY. Publishing YIELD before leaving and CAS-ing back on return is
//     what keeps the word honest and keeps a permit from being dropped. Both controls that remove
//     it are red, and -DYIELD_STORE_BACK is a genuine LOST TASK.
//
//     "DO NOT TARGET" IS AN OPTIMISATION, NOT A CORRECTNESS RULE. -DTARGET_YIELDED aims at a
//     yielded worker anyway and is GREEN, because the swap latches the permit, the worker's
//     after-handshake CAS fails against NOTIFIED, and it consumes and rescans. Nothing is lost.
//     What it costs is the wait for the OS to reschedule that thread -- a quantum, not a cache
//     miss, and under oversubscription that is the whole stall. So placement SHOULD skip YIELD,
//     but if it ever cannot, the machine still works. Do not cite this cell as proof the placement
//     rule is optional; cite it as the reason it can be tuned rather than proven.
//
//   RESULTS: recorded at the bottom.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

/* VALUES MATCH Thread::WorkerState. PARKED keeps the value 2 it has always had, because readers
   elsewhere compare against it numerically; YIELD takes the next free value rather than renumbering
   anything. */
#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2
#define ST_YIELD     3

static _Atomic int g_permit;
static _Atomic int g_oncore;    /* is the worker thread actually executing right now? */
static _Atomic int g_work;      /* the task a producer published */
static _Atomic int g_oswake;    /* syscalls performed */
static _Atomic int g_aimed;     /* a producer aimed at this worker */

/* THE WORKER TOOK A PERMIT AND WENT BACK ROUND. Without this the final assertion cannot tell
 * "consumed, will rescan, correct" from "wiped, nobody was told, lost" -- both leave the word at
 * EMPTY. The first version of this file asserted on the word alone and duly failed its own
 * as-proposed build on the ordinary success case. */
static _Atomic int g_consumed;

#define PUBLISH     memory_order_seq_cst
#define OBSERVE     memory_order_seq_cst
#define TRANSITION  memory_order_seq_cst

/* ---- the floor worker: scan, and every so often give the core back ---------------------------- */
static void *worker(void *arg) {
    (void)arg;

    /* On core and scanning. A floor worker never parks, so this is its whole life. */
    atomic_store_explicit(&g_oncore, 1, PUBLISH);

    /* Found nothing this pass. This is the yield pass -- one in eight in the real loop. */

#ifndef NO_YIELD_HANDSHAKE
    /* BEFORE. Publish "do not target me" and only then leave the core. */
    {
        int e = ST_EMPTY;
        if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_YIELD,
                                                     TRANSITION, memory_order_relaxed)) {
            /* A permit landed while we were deciding. Consume it; do not yield. */
            if (e == ST_NOTIFIED) {
                int e2 = ST_NOTIFIED;
                if (atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                            TRANSITION, memory_order_relaxed))
                    atomic_store_explicit(&g_consumed, 1, PUBLISH);
            }
            return NULL;                     /* back round the scan with work to find */
        }
    }
#else
    /* CONTROL -- TODAY'S CODE. Leave the core with the word still reading EMPTY, which says
       "on core, scanning, no syscall needed". MUST fail. */
#endif

    /* OFF THE CORE. Everything between here and the return is time this thread does not exist. */
    atomic_store_explicit(&g_oncore, 0, PUBLISH);
#ifdef PROBE_OFFCORE
    assert(0);                               /* REACHABILITY: is the off-core window reached? */
#endif
    atomic_store_explicit(&g_oncore, 1, PUBLISH);

#ifndef NO_YIELD_HANDSHAKE
    /* AFTER. Back on the core. */
# ifdef YIELD_STORE_BACK
    /* CONTROL. A plain store back to EMPTY. If a producer swapped to NOTIFIED while we were off the
       core, this WIPES the permit -- and that producer already decided it owed no syscall, so the
       work is never re-announced. MUST fail. */
    atomic_store_explicit(&g_permit, ST_EMPTY, PUBLISH);
# else
    {
        int e = ST_YIELD;
        if (!atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_EMPTY,
                                                     TRANSITION, memory_order_relaxed)) {
            /* Not YIELD any more: a producer latched a permit while we were away. Consume it. */
            int e2 = ST_NOTIFIED;
            if (atomic_compare_exchange_strong_explicit(&g_permit, &e2, ST_EMPTY,
                                                        TRANSITION, memory_order_relaxed))
                atomic_store_explicit(&g_consumed, 1, PUBLISH);
        }
    }
# endif
#endif
    return NULL;
}

/* ---- the producer: consult the word, then aim ------------------------------------------------- */
static void *producer(void *arg) {
    (void)arg;

    /* PLACEMENT READS THE STATE. YIELD means "do not target this thread" -- the one thing the
       fourth state exists to say. */
    const int st = atomic_load_explicit(&g_permit, OBSERVE);
#ifndef TARGET_YIELDED
    if (st == ST_YIELD)
        return NULL;                         /* aim somewhere else; this worker is not there */
#else
    /* CONTROL. Placement ignores YIELD and aims here anyway. MUST fail. */
    (void)st;
#endif

    atomic_store_explicit(&g_aimed, 1, PUBLISH);
    atomic_store_explicit(&g_work, 1, PUBLISH);

    const int prev = atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    if (prev == ST_PARKED) {
        atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
        return NULL;
    }

    /* prev was EMPTY, YIELD or NOTIFIED: no syscall. For EMPTY that is correct -- the worker is on
       the core and its own scan will find this. THE ASSERTION IS THAT THE CLAIM IS TRUE.

       Asserted HERE, at the moment the decision is made, not in main(): the worker comes back on
       core a moment later and the evidence is gone. Same lesson as assertion 3 in
       sleepwake_permit_model.c, which sat in main() and could never fire. */
    if (prev == ST_EMPTY)
        assert(atomic_load_explicit(&g_oncore, OBSERVE) == 1);

    return NULL;
}

int main(void) {
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_oncore, 1);
    atomic_init(&g_work, 0);
    atomic_init(&g_oswake, 0);
    atomic_init(&g_aimed, 0);
    atomic_init(&g_consumed, 0);

    pthread_t w, p;
    pthread_create(&w, NULL, worker,   NULL);
    pthread_create(&p, NULL, producer, NULL);
    pthread_join(w, NULL);
    pthread_join(p, NULL);

    const int work   = atomic_load(&g_work);
    const int permit = atomic_load(&g_permit);
    const int oswake = atomic_load(&g_oswake);
    const int consumed = atomic_load(&g_consumed);

    /* NO DROPPED PERMIT. Work was published, no syscall was paid, the worker never consumed a
       permit, and the word does not hold one either -- so nothing is latched, nothing is parked,
       and nobody was ever told. -DYIELD_STORE_BACK is the control: it wipes a permit that arrived
       while the thread was off the core.

       THE `consumed` AND `!= ST_NOTIFIED` TERMS ARE BOTH LOAD-BEARING, and the first version of
       this file had neither. It asserted on the word alone, so it fired on the ORDINARY SUCCESS
       case -- the worker's before-handshake CAS fails against a NOTIFIED word, it consumes the
       permit, returns to scan, and leaves the word at EMPTY exactly as a dropped permit would.
       Same shape as every other over-strong assertion in this directory: the final state cannot
       tell two different histories apart unless something records which one happened. */
    assert(!(work > 0 && oswake == 0 && consumed == 0 && permit != ST_NOTIFIED));

    return 0;
}

/* =================================================================================================
   RESULTS -- fill in from an actual run before citing this file.

     ~/genmc/RelWithDebInfo/bin/genmc -- tests/verify/yieldstate_model.c

   GenMC is not on PATH; invoke by full path. Built inside WSL (~/genmc), not under /mnt/c.
   RUN 2026-08-31, GenMC v0.17.0, WSL. One floor worker (one yield pass), one producer.

   | variant                | outcome              | complete execs |
   |------------------------|----------------------|----------------|
   | -DPROBE_OFFCORE        | **fires** -- good    | --             |
   | (as proposed)          | no errors            | 6              |
   | -DNO_YIELD_HANDSHAKE   | **SAFETY VIOLATION** | 2              |
   | -DYIELD_STORE_BACK     | **SAFETY VIOLATION** | 2              |
   | -DTARGET_YIELDED       | no errors            | 7              |

   -DNO_YIELD_HANDSHAKE IS TODAY'S CODE and it is red. The worker leaves the core with the word at
   EMPTY, a producer swaps, reads prev == EMPTY, correctly concludes "on core, scanning, no syscall
   needed" -- and the assertion that the conclusion is TRUE fails. That is a latency bug written as
   a safety property on purpose, the same way workerspin_model.c writes the 3023 spin: the state is
   reachable and bad, so it does not need a liveness checker.

   -DYIELD_STORE_BACK IS THE ONE GENUINE LOST TASK in this file. A plain store back to EMPTY on
   return wipes a permit that landed while the thread was off the core, and the producer that
   latched it already decided it owed no syscall, so nothing re-announces the work. The after-half
   of the handshake MUST be a CAS. Every write to this word is an RMW; this is that rule again.

   -DTARGET_YIELDED IS GREEN AND THE GREEN IS THE RESULT. See the note in the header: the placement
   half of the fourth state buys LATENCY, not correctness.

   WHAT IS NOT MODELLED:
     - ONE yield pass. The real loop yields every 8th idle pass forever, so a worker can be off the
       core repeatedly; nothing here says anything about the distribution.
     - The awake BITMAP. This models placement reading the state word directly. If the bitmap stays
       a second source of truth, it must be kept in agreement with the word, and this file does not
       check that -- which is the argument for the word being the only source.
     - K. A reserved worker never parks either, so it has the same yield window; whether it should
       yield at all is a policy question this file does not touch.
   ============================================================================================== */
