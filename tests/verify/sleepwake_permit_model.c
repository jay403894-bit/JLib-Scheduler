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
//   WHAT THIS FILE HAS BEEN WRONG ABOUT, IN ORDER. Every one of these was a GREEN that meant
//   nothing, and none of them was found by staring at the design:
//
//     1. THE ASSERTION WAS AFTER THE JOINS, where the window it described no longer existed. A
//        variant built to fail reported "no errors". A second assertion ended in `&& 0`.
//     2. NO WAIT OBJECT. `return` stood for "blocked", and g_oswake -- notifications ISSUED -- was
//        read as "the worker will run". Three controls passed, and this file concluded the
//        swap-wake and the post-commit recheck were "independently sufficient -- belt and braces".
//     3. THE WITNESS WAS WIRED TO ONE ARM ONLY. g_delivered was incremented in the swap-wake path
//        and not in the WAKE_CAS_ONLY path, so that control went red ON ITS OWN SUCCESS CASE. Its
//        red was reported in a results table as evidence that swap-wake is necessary. IT IS NOT
//        EVIDENCE OF THAT; see the RESULTS note, because the corrected cell is GREEN.
//     4. THE RETURN GATE READ THE PERMIT WORD. Deciding to leave the wait because the word looks
//        NOTIFIED is the pre-wait re-read granted unconditionally to every arm -- it turned the
//        NO_PREWAIT_REREAD control from red to green.
//     5. ASSERTION 3 WAS A FINAL-STATE CHECK ON A TRANSIENT. A waker always overwrites PARKED
//        before the joins, so it could not fire; an assert(0) probe proved the branch reachable
//        while the control reported "no errors" over 72 executions.
//
//   The pattern is one thing said five ways: A GREEN IS A CLAIM ABOUT THE HARNESS UNTIL PROVEN
//   OTHERWISE. Run the controls, and when one passes, find out whether it CAN fail before writing
//   down what its pass means.
//
//   Build variants -- RUN THE CONTROLS FIRST:
//     genmc -- sleepwake_permit_model.c                                  # as proposed
//     genmc -- -DNO_PREWAIT_REREAD sleepwake_permit_model.c              # MUST fail
//     genmc -- -DACQ_REL_ONLY sleepwake_permit_model.c                   # MUST fail
//     genmc -- -DWAKE_CAS_ONLY -DNO_RECHECK sleepwake_permit_model.c     # MUST fail
//     genmc -- -DWAKE_ALWAYS_SYSCALL sleepwake_permit_model.c            # MUST fail (coalescing)
//     genmc -- -DNO_SPURIOUS_REBLOCK sleepwake_permit_model.c            # MUST fail (assertion 3)
//     genmc -- -DPOSTWAIT_STORE sleepwake_permit_model.c                 # MUST be refused
//     genmc -- -DWAKE_CAS_ONLY sleepwake_permit_model.c                  # passes -- see RESULTS
//     genmc -- -DNO_RECHECK sleepwake_permit_model.c                     # passes -- see RESULTS
//
//   RESULTS: recorded at the bottom. THE TWO PASSING CONTROLS ARE NOT A CLEAN BILL OF HEALTH --
//   they pass because g_work is STICKY, and the real flags are edge-triggered. Read that note
//   before quoting this file.

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

/* LEFT THE WAIT FOR THE SEARCH LOOP. Distinguishes the two terminal states a blocked thread can
 * reach, which the earlier versions could not tell apart because both were spelled `return`:
 *
 *     g_left_wait == 0   still inside the wait -- assertions 1 and 2 judge this
 *     g_left_wait == 1   returned and went back to searching
 *
 * The second one carries an obligation the first does not: THE PERMIT WORD MUST NOT STILL SAY
 * PARKED. PARKED means "a thread is committed asleep and a waker owes it a syscall". If the thread
 * has left, that syscall is aimed at nobody -- and the next park attempt cannot commit, because its
 * CAS EMPTY -> PARKED fails against a word that is already PARKED. */
static _Atomic int g_left_wait;

/* SPURIOUS RETURN, MODELLED AS AN AGENT. A spurious wakeup is an OS event with no corresponding
 * write in the program, so there is nothing for a model checker to interleave -- and a branch with
 * no interleaving behind it is dead code, which is what the first attempt at the re-block control
 * produced: -DNO_SPURIOUS_REBLOCK reported "no errors" over 54 executions while never once reaching
 * the line it exists to test. (`__VERIFIER_nondet_int()` does not help; GenMC 0.17 returns a fixed
 * value from it and explores a single execution.)
 *
 * A separate thread writing this flag gives GenMC a real pair of events to order both ways, so the
 * spurious branch is genuinely reached. The OS is an agent in this system; modelling it as one is
 * more honest than pretending its returns are caused by the program. */
static _Atomic int g_spurious;


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
/* ---- LEAVING THE WAIT, AND WHY ASSERTION 3 IS NOT IN main() ----------------------------------
 *
 * ASSERTION 3 IS A CHECK ON A TRANSIENT, so a final-state check cannot see it. Written in main()
 * as `!(left_wait && permit == ST_PARKED)` it reported "no errors" over 72 executions -- not
 * because the branch was unreachable (an assert(0) probe in it fires immediately) but because BOTH
 * wakers always run before the joins, and a waker always swaps the word to NOTIFIED. By the time
 * main() looks, the evidence has been overwritten. The window is real and it is gone.
 *
 * The rule generalises past this file: end-of-execution assertions can only catch states that
 * PERSIST. Anything another thread will overwrite has to be asserted where it happens.
 *
 * What is being asserted: a thread that goes back to the search loop must not leave PARKED
 * published. PARKED means "a thread is committed asleep and a waker owes it a syscall" -- if the
 * thread has left, that syscall is aimed at nobody, and the thread's own next park attempt cannot
 * commit, because CAS EMPTY -> PARKED fails against a word that already reads PARKED. */
static void leave_wait_for_search(void) {
    assert(atomic_load_explicit(&g_permit, OBSERVE) != ST_PARKED);
    atomic_store_explicit(&g_waiting,   0, PUBLISH);
    atomic_store_explicit(&g_left_wait, 1, PUBLISH);
}

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
        leave_wait_for_search();
        return NULL;                       /* wait aborted; go round and take the fast path */
    }
#endif

    /* ---- RETURN FROM THE WAIT ----------------------------------------------------------------
     *
     * The previous version stopped here, and `return` stood for "blocked forever". That deleted the
     * whole consume path: the rules that a spurious return must RE-BLOCK, and that leaving the wait
     * must not leave PARKED published, had nowhere to be checked.
     *
     * Modelled as an ADDRESS WAIT, which is what the Windows and Linux arms use: the wait returns
     * and the thread reads the word to find out why. Reading PARKED means nothing was delivered --
     * a spurious return -- and the thread must go back to waiting rather than fall through. That
     * branch is also what preserves the terminal blocked state assertions 1 and 2 judge, so the
     * earlier results are not quietly discarded by adding this.
     */
    {
        /* THE GATE MUST NOT BE A READ OF THE PERMIT WORD, and the first version of this block made
           exactly that mistake: it went straight to `load(g_permit)` and returned from the wait
           because the word looked NOTIFIED. That IS the pre-wait re-read wearing a different hat --
           it is the address-wait value-compare, granted unconditionally to every arm -- and it
           turned -DNO_PREWAIT_REREAD from red to GREEN. A thread that registered after the signal
           was handed the signal back by the harness.

           A real wait returns for exactly two reasons, and neither is "the word changed":
             - a notification REACHED a registered waiter          -> g_delivered
             - the OS returned for no reason at all                -> g_spurious, written by a
                                                                      separate agent so that GenMC
                                                                      explores both orders
           Neither exists in this execution => the thread is still inside the wait, and that is the
           terminal state assertions 1 and 2 judge. */
        const int delivered_to_me = atomic_load_explicit(&g_delivered, OBSERVE) != 0;
        const int spurious        = atomic_load_explicit(&g_spurious,  OBSERVE) != 0;
        if (!delivered_to_me && !spurious)
            return NULL;                   /* still blocked */

        const int p = atomic_load_explicit(&g_permit, OBSERVE);

        if (p == ST_PARKED) {
#ifdef NO_SPURIOUS_REBLOCK
            /* CONTROL. Treat ANY return as a wake: clear the registration and go search. The permit
               word is left reading PARKED with nobody parked behind it. MUST go red on assertion 3. */
            leave_wait_for_search();
            return NULL;
#else
            /* SPURIOUS. Still registered, still PARKED: back into the wait. Terminating here is the
               blocked state, exactly as before. */
            return NULL;
#endif
        }

        /* REAL DELIVERY -- the word reads NOTIFIED. Consume it. */
#ifdef POSTWAIT_STORE
        /* CONTROL, and it is the shape the OLD code had: a plain store back to the awake state.
           See the RESULTS note before trusting this cell either way -- with a sticky work flag this
           harness cannot currently tell it apart from the CAS. */
        atomic_store_explicit(&g_permit, ST_EMPTY, PUBLISH);
#else
        {
            /* CAS, NOT A STORE. The word is only cleared if it is still the permit we were told
               about; anything else is somebody else's business and not ours to overwrite. */
            int e5 = ST_NOTIFIED;
            atomic_compare_exchange_strong_explicit(&g_permit, &e5, ST_EMPTY,
                                                    TRANSITION, memory_order_relaxed);
        }
#endif
        leave_wait_for_search();
        return NULL;
    }
}

/* THE WITNESS IS NOT PART OF THE MECHANISM UNDER TEST, and keeping it in one function is not
 * tidiness -- it is the fix for a bug this file already had. The first two-waker version wrote the
 * g_oswake/g_delivered pair inline in each arm and the WAKE_CAS_ONLY arm bumped only g_oswake. That
 * made the control go red on its OWN SUCCESS CASE: worker registers, CAS PARKED -> NOTIFIED
 * succeeds, a real wake is performed, and the final state reads {waiting, NOTIFIED, delivered == 0}
 * -- which assertion 2 calls a stall. A control that fails for a reason unrelated to the thing it
 * controls for is worth exactly as much as a control that passes.
 *
 * Every arm calls this. A new arm cannot forget it. */
static void note_wake_performed(void) {
    atomic_fetch_add_explicit(&g_oswake, 1, memory_order_relaxed);
    /* DELIVERY, not issuance. A notify_one() aimed at a thread that has not yet registered on the
       wait object reaches nobody -- the signal is not queued, it is dropped. */
    if (atomic_load_explicit(&g_waiting, OBSERVE) == 1)
        atomic_fetch_add_explicit(&g_delivered, 1, memory_order_relaxed);
}

/* ---- the OS, as an agent ----------------------------------------------------------------------
   Its whole job is to return a blocked thread from its wait for no reason. */
static void *os_noise(void *arg) {
    (void)arg;
    atomic_store_explicit(&g_spurious, 1, PUBLISH);
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
    note_wake_performed();
#elif defined(WAKE_CAS_ONLY)
    /* CONTROL. "CAS if PARKED" -- wake only when the sleeper is already committed. Looks
       sufficient and is not: between the worker's fast-path check and its CAS to PARKED the word
       is EMPTY, so this CAS fails, no permit is latched, and the worker then commits to PARKED and
       blocks on work that was already published. MUST produce a lost wakeup -- and it must produce
       it on ASSERTION 1 (PARKED with no wake ever performed), because assertion 2 is about delivery
       and this arm performs a perfectly real wake whenever its CAS succeeds. */
    int e = ST_PARKED;
    if (atomic_compare_exchange_strong_explicit(&g_permit, &e, ST_NOTIFIED,
                                                TRANSITION, memory_order_relaxed))
        note_wake_performed();
#else
    /* THE PROPOSED WAKE: swap unconditionally. The permit is latched no matter which state the
       sleeper was in, so it cannot be dropped; only the PREVIOUS value decides whether an OS wake
       is owed. Swap rather than CAS also keeps the release edge the sleeper synchronises with. */
    const int prev = atomic_exchange_explicit(&g_permit, ST_NOTIFIED, TRANSITION);
    if (prev == ST_PARKED)
        note_wake_performed();
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
    atomic_init(&g_left_wait, 0);
    atomic_init(&g_spurious, 0);

    /* TWO WAKERS, ONE WORKER. One waker cannot express the property the permit word exists to buy:
       that N producers hitting a parked worker cost ONE syscall, not N. It also cannot express the
       failure Jay named -- "one permit coalesces; the second must not be the only OS wake" -- where
       the first producer's work is latched but silently, and the sleeper is only rescued by a second
       producer that may never arrive. With swap-wake the FIRST swap is the one that sees prev ==
       PARKED and owns the syscall; the second sees NOTIFIED and correctly stays out of the kernel.

       This is also the interleaving in which the two wakers can be ordered either way with respect
       to the worker's fast-path CAS, its commit CAS, its recheck and its registration -- which is
       where the state space actually comes from. */
    pthread_t w, p1, p2, os;
    pthread_create(&w,  NULL, worker, NULL);
    pthread_create(&p1, NULL, waker,  NULL);
    pthread_create(&p2, NULL, waker,  NULL);
    pthread_create(&os, NULL, os_noise, NULL);
    pthread_join(w,  NULL);
    pthread_join(p1, NULL);
    pthread_join(p2, NULL);
    pthread_join(os, NULL);

    const int work    = atomic_load(&g_work);
    const int permit  = atomic_load(&g_permit);
    const int oswake  = atomic_load(&g_oswake);
    const int waiting   = atomic_load(&g_waiting);
    const int delivered = atomic_load(&g_delivered);
    const int left_wait = atomic_load(&g_left_wait);

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

    /* ASSERTION 3 IS NOT HERE. It lives in leave_wait_for_search(), because it checks a TRANSIENT:
       a waker always overwrites PARKED with NOTIFIED before the joins, so this exact assertion,
       written here, reported "no errors" over 72 executions on a control whose branch an assert(0)
       probe proved reachable. left_wait is still read below only to keep the state visible. */
    (void)left_wait;

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

   RUN 2026-08-31, GenMC v0.17.0, WSL. One worker, TWO wakers, an OS agent for spurious returns,
   the wait object and the return-from-wait consume path all modelled:

   | variant                      | outcome                     | complete execs |
   |------------------------------|-----------------------------|----------------|
   | (as proposed)                | no errors                   | 84             |
   | -DNO_PREWAIT_REREAD          | **SAFETY VIOLATION**        | --             |
   | -DACQ_REL_ONLY               | **SAFETY VIOLATION**        | --             |
   | -DWAKE_CAS_ONLY -DNO_RECHECK | **SAFETY VIOLATION**        | --             |
   | -DWAKE_ALWAYS_SYSCALL        | **SAFETY VIOLATION** (coalescing) | --      |
   | -DNO_SPURIOUS_REBLOCK        | **SAFETY VIOLATION**        | --             |
   | -DPOSTWAIT_STORE             | **REFUSED: unordered writes**| --            |
   | -DWAKE_CAS_ONLY              | no errors -- SEE BELOW      | 47             |
   | -DNO_RECHECK                 | no errors -- SEE BELOW      | 68             |

   -DWAKE_CAS_ONLY WAS PUBLISHED AS A RED AND IT IS GREEN. The earlier table listed it as a safety
   violation and cited it as proof that the unconditional swap is necessary. That red was the
   witness being wired to one arm: g_delivered was bumped in the swap path and not in the CAS path,
   so the control failed on its own SUCCESS case -- worker registers, CAS PARKED -> NOTIFIED
   succeeds, a real wake is performed, and the final state {waiting, NOTIFIED, delivered == 0} was
   read as a stall. Every arm now goes through note_wake_performed(). CORRECTED VERDICT: THIS
   HARNESS DOES NOT SHOW THAT SWAP-WAKE IS NECESSARY.

   BOTH PASSING CONTROLS PASS FOR THE SAME REASON, AND IT IS A LIMIT OF THE MODEL. g_work is
   STICKY -- once published it stays published. So:
     - WAKE_CAS_ONLY survives because the post-commit RECHECK finds the sticky work and cancels
       the sleep, covering for the permit the failed CAS never latched.
     - NO_RECHECK survives because the unconditional SWAP latches the permit the recheck would
       have found.
   Together they fail. Each alone is rescued by the other, which is the "independently sufficient"
   shape this file already retracted once. DO NOT RE-DERIVE THAT CONCLUSION FROM THIS TABLE. In
   Worker() the flags this models -- hasQueuedWork, laneWake -- are EDGE-TRIGGERED and cleared at
   the top of the pass. A recheck that runs after the flag was consumed finds nothing, and the
   WAKE_CAS_ONLY rescue disappears. Modelling that is the next job; until it is done, KEEP BOTH
   MECHANISMS and treat neither green as permission to drop one.

   -DPOSTWAIT_STORE IS A REFUSAL, NOT AN ASSERTION FAILURE, and that is the stronger result. GenMC
   stops with "Unordered writes": the worker's plain `Wsc (g_permit, 0)` is unordered against a
   waker's `UWsc (g_permit, 1)`. No witness of mine is involved -- the checker is reporting that
   the design invariant "EVERY write to the permit word is an RMW" has been broken, and that the
   store can land either side of the concurrent swap in the modification order. Either it wipes a
   permit nobody will re-issue (the waker saw prev == NOTIFIED, so it paid no syscall), or it is
   itself wiped. CAS, never store.

   -DNO_SPURIOUS_REBLOCK needed TWO fixes before it meant anything. Its branch was unreachable
   until the OS became an agent (`__VERIFIER_nondet_int()` does not branch in GenMC 0.17 -- it
   returns a fixed value and explores one execution), and then its assertion was in main(), where a
   waker always overwrote PARKED before the joins. It now lives in leave_wait_for_search() and
   fires in 2 executions.

   STILL NOT MODELLED, so do not claim it:
     - EDGE-TRIGGERED WORK FLAGS. The single biggest gap, and the one that makes two cells above
       green. This is where a stall can still live.
     - A SECOND WORKER: nothing about which of two sleepers a wake reaches, or about the targeting
       the state word is also used for.
     - THE DRAIN OBLIGATION coalescing rests on -- see the note at the `oswake <= 1` assertion.
     - The worker still executes ONE park attempt. A thread that leaves the wait and goes round
       again is modelled as termination, so nothing checks the SECOND park.
   ============================================================================================== */
