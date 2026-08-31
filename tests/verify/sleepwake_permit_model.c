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
//   A CLAIM THIS FILE MADE AND HAS NOW RETRACTED. Before the wait object was modelled, three of
//   these controls PASSED, and that was read as "the swap-wake and the post-commit recheck are
//   independently sufficient -- belt and braces". THAT WAS AN ARTEFACT OF THE MISSING WAIT OBJECT,
//   not a property of the design. With delivery modelled, WAKE_CAS_ONLY fails on its own, and so
//   does ACQ_REL_ONLY. Only NO_RECHECK still passes alone.
//
//   The lesson is the one this repository keeps re-learning: a control that passes tells you about
//   the HARNESS first and the code second. Three passing controls looked like a robust design and
//   were actually a harness that could not see the window the design exists to close.
//
//   Build variants -- RUN THE CONTROLS FIRST:
//     genmc -- sleepwake_permit_model.c                                  # as proposed
//     genmc -- -DNO_PREWAIT_REREAD sleepwake_permit_model.c              # MUST fail
//     genmc -- -DWAKE_CAS_ONLY sleepwake_permit_model.c                  # MUST fail
//     genmc -- -DACQ_REL_ONLY sleepwake_permit_model.c                   # MUST fail
//     genmc -- -DWAKE_CAS_ONLY -DNO_RECHECK sleepwake_permit_model.c     # MUST fail
//     genmc -- -DWAKE_ALWAYS_SYSCALL sleepwake_permit_model.c            # MUST fail (coalescing)
//     genmc -- -DNO_RECHECK sleepwake_permit_model.c                     # passes: swap covers it
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

/* VALUES MATCH Thread::WorkerState EXACTLY. They did not -- this file had PARKED=1 and
   NOTIFIED=2 while the code has the reverse -- which changes nothing about what the model
   PROVES but is a trap for anyone reading the two side by side, which is the entire reason
   this file exists. */
#define ST_EMPTY     0
#define ST_NOTIFIED  1
#define ST_PARKED    2

static _Atomic int g_permit;   /* the park word */
static _Atomic int g_work;     /* the task the producer published */
static _Atomic int g_oswake;   /* futex_wake / notify_one actually performed */

/* ---- THE WAIT OBJECT, WHICH THE FIRST VERSION OF THIS FILE DID NOT HAVE -----------------------
 *
 * The first draft ended the worker at `return` and treated g_oswake as "the worker will run". In a
 * real process that is false, and it hid the window that actually matters:
 *
 *     CAS EMPTY -> PARKED            permit published
 *     recheck queues + flags
 *                                <-- wake: swap(NOTIFIED), prev==PARKED, notify_one()
 *     block on the wait object       the waiter is NOT REGISTERED YET
 *
 * The old harness counted that notify as a successful unpark -- oswake==1, assertion satisfied --
 * while the real thread then enters the wait AFTER the signal and never returns. The next producer
 * swaps, sees prev != PARKED, correctly issues no OS wake, and the core is asleep with a latched
 * permit and owner-only inbox work behind it. A permanent strand, invisible to a harness with no
 * wait object in it.
 *
 * This is why the address-wait arms are safe and it is not luck: WaitOnAddress/futex wait ON THE
 * PERMIT WORD, so swap(NOTIFIED) makes the wait return by value-compare. The condvar arm is safe
 * for a different reason -- its predicate reads the permit under the same mutex Wake takes. NEITHER
 * REASON WAS STATED ANYWHERE, so a future change of wait object would keep a green model.
 *
 * g_waiting encodes "registered on the wait object". The pre-wait re-read of the permit is the
 * thing being tested: remove it (-DNO_PREWAIT_REREAD) and this must go red.
 */
static _Atomic int g_waiting;

/* ISSUED IS NOT DELIVERED, and conflating them is what let the first extended version of this file
 * pass -DNO_PREWAIT_REREAD. g_oswake counts notifications SENT. A notify_one() that fires before the
 * waiter has registered on the wait object reaches nobody and is gone -- the counter still reads 1.
 * That is precisely the stall being modelled, so the witness has to be delivery:
 *
 *     g_delivered  incremented only when the wake found g_waiting already set
 *
 * On an ADDRESS wait this distinction collapses -- the kernel compares the word under its own lock,
 * so a swap that lands before registration makes the wait return immediately and delivery is
 * guaranteed. That collapse is modelled by the pre-wait re-read. On a CONDVAR over a SEPARATE object
 * with no predicate on the permit, it does not collapse, and this is the counter that shows it. */
static _Atomic int g_delivered;

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

    /* 5. REGISTER ON THE WAIT OBJECT, THEN BLOCK. The registration is a separate step from the
          permit commit and is on a DIFFERENT object -- that separation is the whole point, and the
          first version of this file collapsed it into `return`. */
    atomic_store_explicit(&g_waiting, 1, PUBLISH);

#ifndef NO_PREWAIT_REREAD
    /* THE PRE-WAIT RE-READ, and it is what makes an address-wait arm correct. WaitOnAddress and
       FUTEX_WAIT both compare the word against the expected value under the kernel's own lock and
       return immediately if it differs -- this models that compare. A wake that landed between the
       recheck above and this point already swapped the permit to NOTIFIED, so the wait must abort
       rather than block on a signal that has already been delivered.

       CONTROL -DNO_PREWAIT_REREAD removes it, which is what a condvar on a SEPARATE object with no
       predicate on the permit word would look like. That must go red. */
    if (atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED) {
        atomic_store_explicit(&g_waiting, 0, PUBLISH);
        return NULL;                       /* wait aborted; consume and go round */
    }
#endif

    /* Blocked: g_waiting == 1 and the permit is PARKED. Termination from here models a thread that
       is inside the wait -- and whether that is a stall is now an assertable question, because the
       final state records that it was registered. */
    return NULL;
}

/* ---- the waker -------------------------------------------------------------------------------
   Publishes the task, then latches a permit. TWO of these run concurrently: see the note in main()
   for why one is not enough. */
static void *waker(void *arg) {
    (void)arg;
    atomic_fetch_add_explicit(&g_work, 1, PUBLISH);

#ifdef WAKE_ALWAYS_SYSCALL
    /* CONTROL, AND IT IS THE ARM THAT ACTUALLY RAN ON THE MACHINE. "Notify all the time" was the
       first thing tried against the stalls, before the state machine existed -- latch the permit and
       then unconditionally pay the syscall, ignoring the previous state. It is SAFE: it fails neither
       lost-wakeup assertion. It fails the COALESCING assertion, one syscall per producer instead of
       one per park, which is exactly why it was reverted. Without this control the coalescing
       assertion would be an assertion no variant can break, i.e. worthless. */
    atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
    if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
        atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
#elif defined(WAKE_CAS_ONLY)
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
    if (prev == ST_PARKED) {
        atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
        /* DELIVERY, not issuance. A notify_one() aimed at a thread that has not yet registered on
           the wait object reaches nobody -- the signal is not queued, it is dropped. Only count it
           as delivered if the waiter was already there. This is the line that makes the
           NO_PREWAIT_REREAD control able to fail. */
        if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
            atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
    }
    /* prev EMPTY or NOTIFIED: latched, and the OS thread is not touched. */
#endif
    return NULL;
}

int main(void) {
    atomic_init(&g_permit, ST_EMPTY);
    atomic_init(&g_work, 0);
    atomic_init(&g_oswake, 0);
    atomic_init(&g_waiting, 0);
    atomic_init(&g_delivered, 0);

    /* TWO WAKERS, ONE WORKER. One waker cannot express the property the permit word exists to buy:
       that N producers hitting a parked worker cost ONE syscall, not N. It also cannot express the
       failure Jay named -- "one permit coalesces; the second must not be the only OS wake" -- where
       the first producer's work is latched but silently, and the sleeper is only rescued by a second
       producer that may never arrive. With swap-wake the FIRST swap is the one that sees prev ==
       PARKED and owns the syscall; the second sees NOTIFIED and correctly stays out of the kernel.

       This is also the interleaving in which the two wakers can be ordered either way with respect
       to the worker's fast-path CAS, its commit CAS, its recheck and its registration -- which is
       where the state space actually comes from. */
    pthread_t w, p1, p2;
    pthread_create(&w,  NULL, worker, NULL);
    pthread_create(&p1, NULL, waker,  NULL);
    pthread_create(&p2, NULL, waker,  NULL);
    pthread_join(w,  NULL);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);

    const int work    = atomic_load(&g_work);
    const int permit  = atomic_load(&g_permit);
    const int oswake  = atomic_load(&g_oswake);
    const int waiting   = atomic_load(&g_waiting);
    const int delivered = atomic_load(&g_delivered);

    /* NO LOST WAKEUP. Work was published, the thread is committed PARKED, and no OS wake was
       performed -- so nothing will ever run it. Inboxes are owner-drain-only, so this is a
       permanent strand rather than a delay: the stall, in its terminal form. */
    assert(!(work > 0 && permit == ST_PARKED && oswake == 0));

    /* BLOCKED ON A SIGNAL THAT ALREADY FIRED -- the window the first version of this file could not
       express. The thread registered on the wait object (waiting==1) and the permit reads NOTIFIED,
       meaning a waker already swapped and, seeing prev==PARKED, already sent its one notification.
       That notification arrived BEFORE the registration, so it reached nobody; the next producer
       will swap, see prev != PARKED, and correctly send nothing. The thread is asleep with a latched
       permit and owner-only work behind it. Permanent, not late.

       This is the assertion the pre-wait re-read exists to satisfy, and -DNO_PREWAIT_REREAD is the
       control that must make it fire. */
    /* THE `oswake == 0` TERM IS LOAD-BEARING and I dropped it on the first attempt, which made this
       fire on the ordinary success case: worker registers, waker swaps seeing prev == PARKED, sends
       the wake, and the final state is legitimately {waiting, NOTIFIED, oswake==1}. The bug is the
       state with NO wake ever performed -- a registered waiter and a latched permit that nobody
       paid a syscall for, so nothing will arrive. GenMC caught my over-strong assertion in the
       as-proposed variant, which is the harness doing its job against the harness. */
    assert(!(work > 0 && waiting == 1 && permit == ST_NOTIFIED && delivered == 0));

    /* COALESCING. Two producers, at most one syscall -- the permit word absorbs the second. This is
       the whole reason the word carries three states instead of a bool: NOTIFIED is a latched permit
       that a later waker can see and stand down on. -DWAKE_ALWAYS_SYSCALL is the control and reaches
       oswake == 2.

       WHAT THIS ASSERTION ASSUMES AND DOES NOT CHECK: coalescing two wakes into one is only sound
       because the woken worker DRAINS, rather than taking a single task and parking again. That
       property lives in the search loop, not in the permit word, and sleep is modelled here as
       termination -- so this harness cannot see it. It is a real obligation on Worker(), stated
       rather than proven. Say so before citing this file as "coalescing verified". */
    assert(oswake <= 1);

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

   RUN 2026-08-31, GenMC v0.17.0, WSL -- WAIT OBJECT MODELLED, ONE WORKER, TWO WAKERS:

   | variant                      | outcome              | complete execs |
   |------------------------------|----------------------|----------------|
   | (as proposed)                | no errors            | 40             |
   | -DNO_PREWAIT_REREAD          | **SAFETY VIOLATION** | --             |
   | -DWAKE_CAS_ONLY              | **SAFETY VIOLATION** | --             |
   | -DACQ_REL_ONLY               | **SAFETY VIOLATION** | --             |
   | -DWAKE_CAS_ONLY -DNO_RECHECK | **SAFETY VIOLATION** | --             |
   | -DWAKE_ALWAYS_SYSCALL        | **SAFETY VIOLATION** | --  (oswake==2)|
   | -DNO_RECHECK                 | no errors            | 24             |

   FIVE OF SIX CONTROLS FAIL. Only NO_RECHECK still passes alone, and the reason is stated at that
   #ifdef: the unconditional swap already latches the permit the recheck would have found. Keep the
   recheck anyway -- it is what turns a latched permit into work done without a syscall.

   -DACQ_REL_ONLY FAILS, AND THAT IS THE IMPORTANT CELL. It PASSED in the version of this file that
   had no wait object, and that pass was an artefact: with sleep encoded as bare termination every
   write to the permit was an RMW, and RMWs are totally ordered per location whatever order is
   requested, so acq_rel was indistinguishable from seq_cst and the cell honestly read "not tested
   here". Registering on a SEPARATE object introduces the StoreLoad pair ACROSS TWO LOCATIONS that
   acquire/release does not give, and the hole opens. The seq_cst requirement on this word is now
   proven by the harness rather than argued from x86-vs-AArch64.

   -DWAKE_ALWAYS_SYSCALL IS A DIFFERENT KIND OF RED and should be read as such. It violates neither
   lost-wakeup assertion -- it is a CORRECT machine. It fails only `oswake <= 1`: two producers, two
   syscalls. That is the arm that ran on the machine first ("notify all the time") and it is why the
   permit word carries three states rather than a bool.

   THE TWO-WAKER CASE IS WHY THE EXECUTION COUNT WENT 8 -> 40. One waker cannot order two swaps
   against each other, so it cannot express coalescing at all, and it cannot express "the first
   producer's wake was dropped and only the second rescued the sleeper".

   WHAT IS STILL NOT MODELLED, so do not claim it:
     - THE RETURN-FROM-WAIT PATH. Sleep is still termination. The rule that a wait returning with
       PARKED still set is spurious and must re-block, and that the word must read NOTIFIED and be
       swapped back to EMPTY, is unverified here.
     - A SECOND WORKER, hence nothing about which of two sleepers a wake reaches, or about the
       targeting the state word is also used for.
     - THE DRAIN OBLIGATION coalescing rests on -- see the note at the `oswake <= 1` assertion.
     - g_work IS STICKY. In Worker() the real flags (hasQueuedWork, laneWake) are EDGE-TRIGGERED and
       cleared at the top of the loop. A model whose work flag can never be consumed cannot express
       "the flag was cleared by the pass that then failed to find the task".
   ============================================================================================== */
