// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the FIBER WAIT/RESUME handshake shared by SchedulerMutex::Lock, SchedulerSemaphore::Wait
// and (transitively) SchedulerConditionVariable::Wait. Companion to deque_model.c, event_model.c and
// sleepwake_model.c; see deque_model.c for what a model checker is.
//
//   WHY THIS EXISTS
//   It is the handshake that shipped a deadlock. Until 1.3.5 both wait paths published the fiber to
//   the waiter queue BEFORE marking it parkable:
//
//       waitingFibers.push(current);       // discoverable...
//       spinLock.clear(release);           // ...but status is still RUNNING
//       Thread::Suspend(current);          // only NOW -> WANTS_SUSPEND
//
//   An Unlock()/Signal() landing in that window pops the fiber and calls Resume(). ResumeQueueless
//   does not treat RUNNING as resumable, so it takes its "nothing to do" branch and DISCARDS the
//   wake. The fiber then parks SUSPENDED with no reference to it left anywhere, while the unlocker
//   has already handed ownership over -- Unlock deliberately leaves `locked` true whenever it pops a
//   waiter, because that is a direct handoff. Result: a mutex locked forever with no holder.
//
//   It was the last unmodelled synchronisation in the scheduler, and it was the one that broke. The
//   deque and the sleep predicate both had models. This did not.
//
//   THE INTERESTING RESULT: this is an ORDERING bug, not a MEMORY-ORDERING bug
//   Unlike sleepwake_model.c, nothing here needs seq_cst, and the fix was not a barrier. The waiter
//   stores its status and pushes itself under the SAME spinlock the unlocker must acquire to pop it,
//   so there is a genuine release/acquire chain:
//
//       status store -> spinLock.clear(release) -> spinLock acquire -> pop -> status load
//
//   Any unlocker that pops the waiter therefore observes everything the waiter published before
//   releasing the lock. That is why plain release/acquire is sufficient and why -DSEQ_CST below
//   changes nothing. The bug was that the status store sat on the WRONG SIDE of the lock release --
//   a program-order defect that no amount of fencing would have fixed. Worth stating plainly,
//   because the reflex on seeing a lost wakeup is to reach for stronger orderings.
//
//   SAFETY, NOT LIVENESS -- same trick as sleepwake_model.c
//   The park is modelled as a TERMINAL STATE rather than a blocking call, so "this fiber is stranded"
//   becomes an assertion on the final state: ownership was handed over, the fiber ended SUSPENDED,
//   and nobody re-queued it. That is reachable-state checking, which is what this tool does.
//
//   Build variants:
//     genmc -- fiberwait_model.c                      # as shipped in 1.3.5
//     genmc -- -DOLD_ORDERING fiberwait_model.c       # the 1.3.4 bug, MUST fail
//     genmc -- -DCLOBBER_SUSPEND fiberwait_model.c    # the Thread::Suspend trap, MUST fail
//     genmc -- -DSEQ_CST fiberwait_model.c            # expected identical to shipped
//
//   RESULTS: recorded at the bottom of this file.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

/* Fiber::status, the subset this handshake uses. */
#define FS_RUNNING          0
#define FS_WANTS_SUSPEND    1
#define FS_SUSPEND_SIGNALED 2
#define FS_SUSPENDED        3
#define FS_READY            4

#ifdef SEQ_CST
  #define PUB  memory_order_seq_cst
  #define OBS  memory_order_seq_cst
  #define TRAN memory_order_seq_cst
#else
  /* What ships. The spinlock supplies the happens-before edge; see the header note. */
  #define PUB  memory_order_release
  #define OBS  memory_order_acquire
  #define TRAN memory_order_acq_rel
#endif

static _Atomic int g_status;    /* Fiber::status of the waiting fiber                */
static _Atomic int g_queued;    /* 1 == present in waitingFibers (one waiter modelled) */
static _Atomic int g_requeued;  /* times the waiter was made runnable -- must end at 1 */
static _Atomic int g_owner;     /* 1 == Unlock popped us, so the lock is OURS now     */

/* SchedulerMutex::spinLock. A real lock, because the release/acquire edge it provides is precisely
   what makes the fixed ordering sound -- modelling it away would prove a different protocol. */
static pthread_mutex_t g_spin;

// ---- the waiting fiber: SchedulerMutex::Lock's contended path --------------------------------
// Finds the mutex held, enqueues itself, and switches out. The worker's park step runs on the far
// side of the context switch and is inlined here, since it is the same logical thread of control.
static void *waiter(void *arg) {
    (void)arg;

    pthread_mutex_lock(&g_spin);
#ifdef OLD_ORDERING
    /* THE 1.3.4 BUG: discoverable while still RUNNING. The status store has slipped past the lock
       release below, into Fiber::Suspend, so a popper can observe RUNNING and drop the wake. */
    atomic_store_explicit(&g_queued, 1, PUB);
#else
    /* THE FIX: parkable BEFORE discoverable, and both INSIDE the critical section, so anyone who
       pops us has necessarily synchronised with the status store. Mirrors WaitOnEvent. */
    atomic_store_explicit(&g_status, FS_WANTS_SUSPEND, PUB);
    atomic_store_explicit(&g_queued, 1, PUB);
#endif
    pthread_mutex_unlock(&g_spin);

#ifdef OLD_ORDERING
    atomic_store_explicit(&g_status, FS_WANTS_SUSPEND, PUB);   /* Thread::Suspend, too late */
#endif

#ifdef CLOBBER_SUSPEND
    /* THE TRAP, and the reason the fix does not simply reorder two lines and keep calling
       Thread::Suspend(). Fiber::Suspend() stores WANTS_SUSPEND *unconditionally*. Run it after the
       reorder and it overwrites a SUSPEND_SIGNALED that a racing Resume just wrote -- the signal is
       erased, the park step below then CASes cleanly to SUSPENDED, and the wake is lost exactly as
       before. This is why both call sites ContextSwitch directly. */
    atomic_store_explicit(&g_status, FS_WANTS_SUSPEND, PUB);
#endif

    /* ---- the worker's park step (Worker(), after ContextSwitch returns) ---- */
    int exp = FS_WANTS_SUSPEND;
    if (atomic_compare_exchange_strong_explicit(&g_status, &exp, FS_SUSPENDED,
                                                TRAN, memory_order_relaxed)) {
        /* Parked. cv-less: a later Resume must find SUSPENDED and re-queue us. Terminal here. */
    } else if (exp == FS_SUSPEND_SIGNALED) {
        /* A Resume beat us to it: wake now instead of parking. */
        atomic_store_explicit(&g_status, FS_READY, PUB);
        atomic_fetch_add_explicit(&g_requeued, 1, memory_order_relaxed);
    }
    return NULL;
}

// ---- the unlocking fiber: SchedulerMutex::Unlock + Fiber::ResumeQueueless --------------------
// Pops one waiter and hands the lock to it. Note it does NOT clear `locked` on this path: popping a
// waiter IS the ownership transfer, which is what makes a dropped wake unrecoverable rather than
// merely slow.
static void *unlocker(void *arg) {
    (void)arg;

    int popped = 0;
    pthread_mutex_lock(&g_spin);
    if (atomic_load_explicit(&g_queued, OBS) == 1) {
        atomic_store_explicit(&g_queued, 0, PUB);
        popped = 1;
        atomic_store_explicit(&g_owner, 1, PUB);   /* `locked` stays true: handoff */
    }
    pthread_mutex_unlock(&g_spin);

    if (!popped) return NULL;

    /* Fiber::ResumeQueueless, verbatim in shape. */
    for (;;) {
        int s = atomic_load_explicit(&g_status, OBS);
        if (s == FS_SUSPENDED) {
            int e = FS_SUSPENDED;
            if (atomic_compare_exchange_strong_explicit(&g_status, &e, FS_READY,
                                                        TRAN, memory_order_relaxed))
                atomic_fetch_add_explicit(&g_requeued, 1, memory_order_relaxed);
            return NULL;
        } else if (s == FS_WANTS_SUSPEND) {
            int e = FS_WANTS_SUSPEND;
            if (atomic_compare_exchange_strong_explicit(&g_status, &e, FS_SUSPEND_SIGNALED,
                                                        TRAN, memory_order_relaxed))
                return NULL;               /* the park step will wake it */
            /* lost to the worker parking it -> retry, now on the SUSPENDED path */
        } else {
            /* RUNNING / READY / SUSPEND_SIGNALED / DEAD. Under OLD_ORDERING this is reachable with
               s == RUNNING, and THIS RETURN IS THE BUG: the wake is silently discarded. */
            return NULL;
        }
    }
}

int main(void) {
    atomic_init(&g_status, FS_RUNNING);
    atomic_init(&g_queued, 0);
    atomic_init(&g_requeued, 0);
    atomic_init(&g_owner, 0);
    pthread_mutex_init(&g_spin, NULL);

    pthread_t w, u;
    pthread_create(&w, NULL, waiter,   NULL);
    pthread_create(&u, NULL, unlocker, NULL);
    pthread_join(w, NULL);
    pthread_join(u, NULL);

    const int owner    = atomic_load(&g_owner);
    const int status   = atomic_load(&g_status);
    const int requeued = atomic_load(&g_requeued);

    /* THE PROPERTY: no stranded owner. If Unlock popped this fiber it also handed it the lock, so
       the fiber MUST end up runnable. Ending SUSPENDED with nobody having re-queued it is a fiber
       parked forever holding a mutex nobody can release -- the 1.3.4 deadlock. */
    assert(!(owner == 1 && status == FS_SUSPENDED && requeued == 0));

    /* AND no double wake. Re-queueing twice would run one task on two workers and, worse, hand the
       same fiber out of the pool twice. The WANTS_SUSPEND -> SUSPEND_SIGNALED CAS exists to make
       park-vs-signal a single atomic decision; this asserts it actually is one. */
    assert(requeued <= 1);

    /* A waiter that was never popped must NOT have been woken by this unlocker. Guards against
       "fix" attempts that resume speculatively to dodge the race. */
    assert(!(owner == 0 && requeued > 0));

    return 0;
}

// RESULTS -- GenMC v0.17.0 (commit #29b03a6), 2026-08-17, one waiter plus one unlocker:
//
//   as shipped in 1.3.5        no errors, 3 complete executions, 1 blocked
//   -DOLD_ORDERING             Error: Safety violation!   <- the 1.3.4 deadlock
//   -DCLOBBER_SUSPEND          Error: Safety violation!   <- the Thread::Suspend clobber
//   -DSEQ_CST                  no errors, 3 complete executions, 1 blocked
//
// THE OLD_ORDERING COUNTEREXAMPLE, which is the shipped bug in four events:
//     (1,3) Wrel g_queued  = 1     waiter publishes itself, still RUNNING
//     (2,3) Racq g_queued  = 1     unlocker sees it and pops
//     (2,5) Wrel g_owner   = 1     ownership handed over; `locked` stays true
//     (2,7) Racq g_status  = 0     reads RUNNING -> ResumeQueueless returns, WAKE DISCARDED
//   then (1,7) CASes g_status 1->3, parking the waiter as SUSPENDED. Final state: owner=1,
//   status=SUSPENDED, requeued=0. A fiber parked forever holding a mutex nobody can release.
//
// -DSEQ_CST MATCHING THE SHIPPED RESULT EXACTLY -- same 3 executions -- is the point of that
// variant, not a formality. It is the evidence for the claim in the header: this was never a memory
// ordering bug. The spinlock already provides the release/acquire edge, so strengthening every
// access buys literally nothing, and no barrier anywhere would have prevented the deadlock. The
// only fix was moving the status store to the correct side of the lock release. When a lost wakeup
// turns up, check program order BEFORE reaching for seq_cst.
//
// STATE SPACE IS SMALL (3 executions) and that is worth naming rather than glossing. The spinlock
// serialises the queue operations and there is one waiter, so there is not much left to interleave.
// A small state space is not evidence of a weak model, but it is not evidence of a strong one
// either -- what makes this model worth keeping is that BOTH negative controls fire. A model with
// no discriminating power passes everything, which is precisely how the first sleepwake model
// "proved" a protocol that then shipped a hang.
//
// WHAT THIS MODEL DOES NOT COVER, stated because sleepwake_model.c cost a release by being applied
// past its own boundary:
//   - ONE waiter. Multiple fibers queued on one mutex are not explored, so nothing here says
//     anything about waiter FAIRNESS or about two unlockers racing to pop different waiters.
//   - The spinlock is a real lock, so this proves the protocol GIVEN mutual exclusion on the queue.
//     It says nothing about the atomic_flag spin implementation itself.
//   - The semaphore's permit COUNT is not modelled, only its fiber wait path, which is structurally
//     identical to the mutex's. The CV's own safety rests on the permit count absorbing an early
//     signal (see the comment at SchedulerConditionVariable::Wait) and is NOT verified here.
//   - Liveness is out of scope by construction: the park is terminal. This proves no fiber is
//     STRANDED, not that every fiber eventually runs.
