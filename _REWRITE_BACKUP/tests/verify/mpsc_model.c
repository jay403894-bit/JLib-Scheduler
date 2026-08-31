// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of include/TaskMPSCQueue.h -- Dmitry Vyukov's intrusive MPSC queue, which every worker
// inbox is. Many producers append; exactly one consumer (the owning worker) pops.
//
// THIS IS THE LAST UNMODELLED LOCK-FREE STRUCTURE IN THE LIBRARY. The deque, its resize, the event
// table, the sleep/wake predicate, the fiber wait/resume handshake and the counted epochs all have
// models; the inboxes did not, which meant the path EVERY task travels was the one path taken on
// trust. That is the reason this file exists, not a suspicion about the algorithm.
//
//   THE ALGORITHM, and the one thing about it that surprises people:
//
//     append(n):  n->next = NULL
//                 prev = XCHG(head, n)      // atomic, so producers linearize here
//                 prev->next = n            // NOT atomic with the exchange
//
//   THE WINDOW BETWEEN THOSE TWO LINES IS THE WHOLE DESIGN. A producer that has swapped head_ but
//   not yet stored prev->next has left the list temporarily BROKEN: head_ names its node, but no
//   reachable `next` chain leads there. A consumer arriving inside that window sees a queue that
//   looks empty, and pop() correctly returns false even though a push has already linearized.
//
//   THAT IS A LIVENESS PROPERTY, NOT A SAFETY ONE, and this model is careful about the difference:
//
//     SAFETY  -- what is asserted. No item is popped twice, and nothing is handed out whose
//                payload has not been published. These must hold in EVERY execution.
//     LIVENESS -- NOT asserted, deliberately. "Everything pushed is eventually popped" is false for
//                a bounded run: a consumer can finish while a producer sits in that window. The
//                real consumer is a worker loop that calls pop() again, and a model that demanded
//                a drain here would be asserting something the code does not promise.
//
//   Asserting the second as if it were the first is the obvious mistake with this queue, and it
//   would produce a "bug report" against correct code.
//
//   THE STUB is what keeps the queue non-empty so `prev` is always a real node -- there is no
//   special case for pushing into an empty queue, which is where hand-rolled MPSC queues usually
//   go wrong. pop() re-appends the stub when it drains the last item, and the awkward-looking
//   `tail != head_` check is how it distinguishes "genuinely empty" from "a producer is mid-append".
//
//   NEGATIVE CONTROLS -- all three, and the third one is the reason this file was almost wrong:
//
//     -DNO_APPEND_RELEASE   the `prev->next = n` store drops to relaxed. The consumer can then
//                           follow the link and read a node whose payload is not yet visible.
//     -DNO_POP_ACQUIRE      the consumer's `next` loads drop to relaxed, losing the other half of
//                           that same pair.
//     -DRELAXED_XCHG        the head_ exchange drops from acq_rel to relaxed. An item is LOST.
//
//   READ THIS BEFORE TRUSTING A CLEAN RESULT ANYWHERE. The first version of this model had a
//   BOUNDED consumer -- pop a fixed number of times and stop -- and under it RELAXED_XCHG reported
//   NO ERRORS over 14,840 executions. It was written up as "a comparison, not a control": the
//   exchange is an RMW, RMWs on one location share a total modification order whatever ordering
//   they carry, so producers linearize regardless and acq_rel looked stronger than necessary.
//
//   That reasoning is correct about LINEARIZATION and irrelevant to what actually breaks. With a
//   bounded consumer a `false` from pop() is AMBIGUOUS -- it may mean "a producer is mid-append",
//   which is correct and expected, or "this item is never coming out", which is the bug. The model
//   could not tell them apart, so it could not see the bug, so the ordering looked free.
//
//   Adding the post-join drain (see main) makes "nothing is lost" a decidable safety property, and
//   RELAXED_XCHG immediately fails with a SAFETY VIOLATION in 3 executions. acq_rel on the exchange
//   IS load-bearing. The consumer's `tail != head_` test -- the one that distinguishes a genuinely
//   empty queue from a producer mid-append -- is what needs it.
//
//   THE LESSON IS ABOUT HARNESSES, NOT ABOUT THIS QUEUE: an assertion too weak to fail makes the
//   thing it was supposed to test look unnecessary. "No errors" from a model is worth exactly as
//   much as the strongest property it asserts.
//
//   RESULT, GenMC v0.17.0 (LLVM 15.0.7), 2026-08-27, two producers + one consumer, 2 items each:
//
//     default (as shipped)   no errors, 2478 complete executions
//     -DNO_APPEND_RELEASE    NON-ATOMIC RACE -- the consumer reads a payload racing its write
//     -DNO_POP_ACQUIRE       NON-ATOMIC RACE -- same pair, other half
//     -DRELAXED_XCHG         SAFETY VIOLATION, 3 executions -- an item is lost
//                            (reported NO ERRORS before the drain existed; see above)
//
//   RUN THEM -- -unroll is a GenMC option and goes BEFORE the --, the -D defines after:
//
//     genmc -unroll=8 -- tests/verify/mpsc_model.c
//     genmc -unroll=8 -- -DNO_APPEND_RELEASE tests/verify/mpsc_model.c   # MUST fail
//     genmc -unroll=8 -- -DNO_POP_ACQUIRE    tests/verify/mpsc_model.c   # MUST fail
//     genmc -unroll=8 -- -DRELAXED_XCHG      tests/verify/mpsc_model.c   # MUST fail
//
// SCOPE: two producers, one consumer, two items each. Two producers is the minimum that makes the
// exchange contended -- with one, head_ is never raced and the linearization point is untested.

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define NPROD      2
#define PER_PROD   2
#define NITEMS     (NPROD * PER_PROD)

#ifdef NO_APPEND_RELEASE
  #define LINK_ORDER memory_order_relaxed
#else
  #define LINK_ORDER memory_order_release   /* what TaskMPSCQueue.h ships */
#endif

#ifdef NO_POP_ACQUIRE
  #define NEXT_ORDER memory_order_relaxed
#else
  #define NEXT_ORDER memory_order_acquire
#endif

#ifdef RELAXED_XCHG
  #define XCHG_ORDER memory_order_relaxed
#else
  #define XCHG_ORDER memory_order_acq_rel
#endif

// A NODE CARRIES A PAYLOAD WRITTEN BEFORE IT IS LINKED. That payload is the point of the
// release/acquire pair: without it the consumer can reach the node and read a stale `value`, which
// is what NO_APPEND_RELEASE and NO_POP_ACQUIRE each break one half of.
struct Node {
    _Atomic(struct Node *) next;
    int                    value;      /* deliberately NON-atomic -- publication is the whole test */
};

static struct Node  g_stub;
static struct Node  g_nodes[NITEMS];

static _Atomic(struct Node *) g_head;
static struct Node           *g_tail;   /* CONSUMER-PRIVATE: single consumer, so no atomic needed */

static _Atomic int g_claims[NITEMS + 1];

/* ---- producer side --------------------------------------------------------------------------- */
static void append(struct Node *n) {
    atomic_store_explicit(&n->next, NULL, memory_order_relaxed);

    /* THE LINEARIZATION POINT. Producers order against each other here and nowhere else. */
    struct Node *prev = atomic_exchange_explicit(&g_head, n, XCHG_ORDER);

    /* THE WINDOW. Between the exchange above and this store the list is BROKEN: head_ names n, but
       nothing reachable from tail_ leads to it. A consumer arriving here sees an empty queue. */
    atomic_store_explicit(&prev->next, n, LINK_ORDER);
}

/* ---- consumer side, mirroring TaskMPSCQueue::pop --------------------------------------------- */
static struct Node *pop(void) {
    struct Node *tail = g_tail;
    struct Node *next = atomic_load_explicit(&tail->next, NEXT_ORDER);

    if (tail == &g_stub) {
        if (!next) return NULL;                  /* genuinely empty, or a producer is mid-append */
        g_tail = next;
        tail   = next;
        next   = atomic_load_explicit(&next->next, NEXT_ORDER);
    }

    if (next) {
        g_tail = next;
        return tail;
    }

    /* NOT EMPTY, ONE ITEM LEFT: re-append the stub so the last item gains a successor and can be
       handed out. The head_ check is how "genuinely empty" is told from "mid-append" -- if tail is
       not head_, some producer has already swapped and its link store is still in flight. */
    if (tail != atomic_load_explicit(&g_head, memory_order_acquire))
        return NULL;

    append(&g_stub);

    next = atomic_load_explicit(&tail->next, NEXT_ORDER);
    if (next) {
        g_tail = next;
        return tail;
    }
    return NULL;
}

/* --------------------------------------------------------------------------------------------- */
static void *producer(void *arg) {
    const long id = (long)arg;                   /* 0 or 1 */
    for (int i = 0; i < PER_PROD; ++i) {
        struct Node *n = &g_nodes[id * PER_PROD + i];
        /* WRITTEN BEFORE THE LINK, and read after it by the consumer. This is the data the
           release/acquire pair publishes; a plain int makes a broken pair a visible bug rather
           than a silent one. */
        n->value = (int)(id * PER_PROD + i) + 1;
        append(n);
    }
    return NULL;
}

static void *consumer(void *arg) {
    (void)arg;
    /* BOUNDED, AND DELIBERATELY NOT A DRAIN LOOP. Popping until NITEMS have come out would be
       asserting LIVENESS -- and would hang the checker on the legitimate execution where a producer
       is still inside the append window when the consumer gives up. The real worker loop simply
       calls pop() again later. */
    for (int i = 0; i < NITEMS; ++i) {
        struct Node *n = pop();
        if (!n) continue;

        /* A NODE HANDED OUT MUST HAVE ITS PAYLOAD VISIBLE. This is what the ordering controls
           break: with a relaxed link store or a relaxed next load, the consumer can reach the node
           before its value was published and read 0. */
        assert(n->value >= 1 && n->value <= NITEMS);
        atomic_fetch_add_explicit(&g_claims[n->value], 1, memory_order_relaxed);
    }
    return NULL;
}

int main(void) {
    atomic_init(&g_stub.next, NULL);
    g_stub.value = -1;
    for (int i = 0; i < NITEMS; ++i) {
        atomic_init(&g_nodes[i].next, NULL);
        g_nodes[i].value = 0;                    /* 0 is the "not yet published" value */
    }
    for (int i = 0; i <= NITEMS; ++i) atomic_init(&g_claims[i], 0);

    atomic_init(&g_head, &g_stub);
    g_tail = &g_stub;

    pthread_t p0, p1, c;
    pthread_create(&p0, NULL, producer, (void *)0L);
    pthread_create(&p1, NULL, producer, (void *)1L);
    pthread_create(&c,  NULL, consumer, NULL);
    pthread_join(p0, NULL);
    pthread_join(p1, NULL);
    pthread_join(c,  NULL);

    // ---- THE POST-JOIN DRAIN, and it is not a liveness assertion --------------------------------
    //
    // WHAT THE BOUNDED CONSUMER ABOVE CANNOT TELL YOU. It pops a fixed number of times and stops, so
    // a `false` from pop() is ambiguous: it may mean "a producer is mid-append", which is correct
    // and expected, or it may mean "this queue is now permanently wedged" -- a stranded tail_ from a
    // mishandled stub re-append, which is exactly what the `tail != head_` branch and append(&g_stub)
    // exist to get right. Without this the model tests the ordering and not the structure.
    //
    // WHY IT IS SAFETY AND NOT LIVENESS. Every producer has been JOINED, so every exchange AND every
    // link store has happened -- the window that makes "empty" ambiguous is closed by construction.
    // At that point "pop until it returns false" is a terminating, deterministic operation, and
    // "everything that went in came out" is a plain safety property over the final state. No spin
    // loop, no retry bound, no claim about what happens at any earlier instant.
    //
    // THIS IS THE ASSERTION THAT WOULD CATCH A WEDGED QUEUE. If the stub logic can strand tail_,
    // some item never comes out here and the count is short.
    for (;;) {
        struct Node *n = pop();
        if (!n) break;
        assert(n->value >= 1 && n->value <= NITEMS);
        atomic_fetch_add_explicit(&g_claims[n->value], 1, memory_order_relaxed);
    }

    /* PROPERTY 1 -- NO DUPLICATION. Two consumers running one task is the failure that matters;
       here there is one consumer, so this catches the queue handing the same node out twice, which
       a mishandled stub re-append does. */
    for (int i = 1; i <= NITEMS; ++i)
        assert(atomic_load(&g_claims[i]) <= 1);

    /* PROPERTY 2 -- NOTHING IS LOST, now that the drain above makes that a safety claim. Every item
       every producer pushed must have come out exactly once, counting the concurrent consumer and
       the post-join drain together.

       THIS IS NOT THE LIVENESS CLAIM THE HEADER WARNS ABOUT. That one is "everything pushed is
       eventually popped", asserted at an arbitrary instant, and it is FALSE -- a consumer can
       legitimately see an empty queue while a producer sits between its exchange and its link
       store. This one is evaluated after every producer has been JOINED, so that window is closed
       by construction and the queue's final state is fully determined.

       IT IS ALSO THE ONLY ASSERTION HERE THAT CAN CATCH A WEDGED QUEUE -- a stranded tail_ from a
       mishandled stub re-append shows up as a missing item and nothing else in this file would
       notice. Combined with PROPERTY 1 it says: exactly once, no more and no fewer. */
    int total = 0;
    for (int i = 1; i <= NITEMS; ++i) total += atomic_load(&g_claims[i]);
    assert(total == NITEMS);

    return 0;
}
