// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the lock-free waiter STACK that Event.h used until 2026-08-24, for a stateless
// model checker.
//
//   THE STACK IS RETIRED. Event now keeps waiters in a fiber-indexed slot table with an
//   occupancy bitmap (modelled in event_table_model.c), and Task::nextWaiter is gone. This
//   file is KEPT rather than deleted because the argument it verifies is the reason the
//   replacement exists: the stack was correct, and still could not support SignalOne. Its
//   links WERE the tasks, so a waiter could never leave early without the next drain walking
//   a recycled slab slot. Retiring it was not a bug fix -- it was lifting that ceiling.
//
//   Do not resurrect this design without re-reading why it was replaced. What follows is the
//   original header, unchanged.
//
// Companion to deque_model.c; see that file's header for what a model checker is and why running
// the real thing is not a substitute.
//
//   THE QUESTION THIS FILE EXISTS TO ANSWER
//   Event::AddWaiter writes `t->nextWaiter` with a PLAIN, NON-ATOMIC store and then publishes the
//   node with a release CAS on `head`. SignalAll takes the whole list with an acquiring exchange
//   and then READS `nextWaiter` from another thread. Whether that is a data race or correctly
//   ordered rests on an argument, not an observation:
//
//     - direct case: the plain write is sequenced before the release CAS, which synchronises-with
//       the acquire exchange. Fine.
//     - CHAINED case: A pushes, then B pushes, and the drainer takes B. Does the drainer see A's
//       nextWaiter write? Only via the RELEASE SEQUENCE rule -- B's CAS is an RMW that reads A's
//       value, which places it in A's release sequence, so the acquire on head synchronises with
//       A's release too.
//
//   That second bullet is the whole reason a Treiber push works with release/acquire rather than
//   seq_cst, and it is exactly the kind of reasoning that sounds right and is worth checking.
//
//   Also checked: NO NODE IS WOKEN TWICE. Waking a fiber twice resumes a task that may already
//   have completed and had its slab slot recycled -- the same failure class as the deque's
//   double-claim.
//
//   ABA is deliberately absent by construction: nothing here POPS. SignalAll takes the entire list
//   in one exchange, so there is no read-head-then-CAS window for a recycled address to slip into.
//   That property is why Event needs no EBR or hazard pointers, and it holds ONLY while there is no
//   remove-one-waiter operation (Signal(Task*) and RemoveWaiter were deleted for this reason).
//
//   Build variants:
//     genmc -- event_model.c                  # as shipped
//     genmc -- -DNO_RELEASE event_model.c     # negative control, MUST fail
//
//   RESULTS: recorded at the bottom of this file.

#include <stdatomic.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define NNODES 2      /* two concurrent pushers is enough to exercise the release SEQUENCE */

/* Mirrors Task: nextWaiter is a PLAIN field, not an atomic. That is the point -- if the ordering
   below is wrong, the checker reports a data race on this member rather than a logic error. */
typedef struct Node {
    int          id;
    struct Node *nextWaiter;
} Node;

static Node              g_nodes[NNODES];
static _Atomic(Node *)   g_head;
static _Atomic int       g_drained[NNODES + 1];

#ifdef NO_RELEASE
  /* Negative control: drop the release on the publishing CAS, which is the claim "the CAS itself
     is enough because it is an RMW". It is not -- without release, the plain nextWaiter write is
     not published to the acquiring reader. */
  #define PUBLISH_ORDER memory_order_relaxed
#else
  #define PUBLISH_ORDER memory_order_release
#endif

// ---- Event::AddWaiter -------------------------------------------------------------------------
static void add_waiter(Node *t) {
    Node *h = atomic_load_explicit(&g_head, memory_order_relaxed);
    do {
        t->nextWaiter = h;                       /* plain store, published by the CAS below */
    } while (!atomic_compare_exchange_weak_explicit(
                 &g_head, &h, t, PUBLISH_ORDER, memory_order_relaxed));
}

// ---- Event::SignalAll -------------------------------------------------------------------------
static void signal_all(void) {
    Node *t = atomic_exchange_explicit(&g_head, NULL, memory_order_acq_rel);
    while (t) {
        Node *next = t->nextWaiter;              /* read BEFORE waking: waking can recycle t */
        t->nextWaiter = NULL;
        atomic_fetch_add_explicit(&g_drained[t->id], 1, memory_order_relaxed);
        t = next;
    }
}

// ---------------------------------------------------------------------------------------------
static void *pusher0(void *arg) { (void)arg; add_waiter(&g_nodes[0]); return NULL; }
static void *pusher1(void *arg) { (void)arg; add_waiter(&g_nodes[1]); return NULL; }
static void *drainer(void *arg) { (void)arg; signal_all();            return NULL; }

int main(void) {
    atomic_init(&g_head, NULL);
    for (int i = 0; i < NNODES; ++i) {
        g_nodes[i].id = i + 1;
        g_nodes[i].nextWaiter = NULL;
        atomic_init(&g_drained[i + 1], 0);
    }

    pthread_t p0, p1, d;
    pthread_create(&p0, NULL, pusher0, NULL);
    pthread_create(&p1, NULL, pusher1, NULL);
    pthread_create(&d,  NULL, drainer, NULL);
    pthread_join(p0, NULL);
    pthread_join(p1, NULL);
    pthread_join(d,  NULL);

    /* The concurrent drain may have run before either push, or between them. Drain again now that
       everything is quiescent, so every node must have been taken exactly once overall. */
    signal_all();

    /* PROPERTY 1 -- NO WAITER LOST. A fiber that parks and is never resumed hangs forever. */
    /* PROPERTY 2 -- NO WAITER WOKEN TWICE. Resuming a completed task touches a recycled slab slot. */
    for (int i = 1; i <= NNODES; ++i)
        assert(atomic_load(&g_drained[i]) == 1);

    return 0;
}

// RESULTS -- GenMC v0.17.0 (LLVM 15), 2026-08-11, two pushers + one concurrent drainer:
//
//   as shipped (release CAS)   no errors, 24 complete executions
//   -DNO_RELEASE               Error: Non-atomic race!
//
// So Event.h's ordering is correct, INCLUDING the chained case the release-sequence rule covers.
// No waiter lost, none woken twice, and no race on the plain nextWaiter field.
//
// The negative control is what makes that worth stating. Dropping the release reports a
// "Non-atomic race" -- precisely the failure predicted: the plain nextWaiter write stops being
// published to the acquiring reader. It is not an assertion failure but a RACE, which is the
// checker naming the exact defect rather than a downstream symptom of it.
//
// Keep -DNO_RELEASE as a permanent negative control and re-run it whenever this model changes.
