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
//   NEGATIVE CONTROLS, because a model that cannot fail proves nothing:
//
//     -DNO_APPEND_RELEASE   the `prev->next = n` store drops to relaxed. The consumer can then
//                           follow the link and read a node whose payload is not yet visible.
//     -DNO_POP_ACQUIRE      the consumer's `next` loads drop to relaxed, losing the other half of
//                           that same pair.
//
//   AND ONE COMPARISON, WHICH IS NOT A CONTROL AND IS LABELLED THAT WAY DELIBERATELY:
//
//     -DRELAXED_XCHG        the head_ exchange drops from acq_rel to relaxed. It checks CLEAN, and
//                           that is the correct answer rather than a hole in the harness. An
//                           exchange is an RMW, and every RMW on one location participates in that
//                           location's total modification order whatever ordering it carries -- so
//                           producers still linearize. What acq_rel would additionally buy is
//                           ordering against OTHER memory, and the payload does not need it: it is
//                           published by the release store on prev->next and read by the acquire
//                           load, which are the two controls above.
//
//                           SO acq_rel HERE IS STRONGER THAN THESE PROPERTIES REQUIRE, exactly as
//                           deque_model.c found for the steal CAS. IT STAYS ANYWAY, for the reason
//                           recorded there: on x86-64 the weaker form emits identical code, so the
//                           weakening buys nothing where this runs, and it differs only on AArch64
//                           -- the least-exercised port -- where the gain is unmeasured and the
//                           risk is a queue that silently drops a task. Measure the barrier on ARM
//                           before touching it.
//
//   RESULT, GenMC v0.17.0 (LLVM 15.0.7), 2026-08-27, two producers + one consumer, 2 items each:
//
//     default (as shipped)   no errors, 2478 complete executions
//     -DNO_APPEND_RELEASE    NON-ATOMIC RACE -- the consumer reads a payload racing its write
//     -DNO_POP_ACQUIRE       NON-ATOMIC RACE -- same pair, other half
//     -DRELAXED_XCHG         no errors, 14840 complete executions (see above: expected)
//
//   RUN THEM -- -unroll is a GenMC option and goes BEFORE the --, the -D defines after:
//
//     genmc -unroll=8 -- tests/verify/mpsc_model.c
//     genmc -unroll=8 -- -DNO_APPEND_RELEASE tests/verify/mpsc_model.c   # MUST fail
//     genmc -unroll=8 -- -DNO_POP_ACQUIRE    tests/verify/mpsc_model.c   # MUST fail
//     genmc -unroll=8 -- -DRELAXED_XCHG      tests/verify/mpsc_model.c   # comparison: passes
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

    /* PROPERTY 1 -- NO DUPLICATION. Two consumers running one task is the failure that matters;
       here there is one consumer, so this catches the queue handing the same node out twice, which
       a mishandled stub re-append does. */
    for (int i = 1; i <= NITEMS; ++i)
        assert(atomic_load(&g_claims[i]) <= 1);

    /* PROPERTY 2 -- NOTHING FABRICATED. Every claim was a node a producer actually published; the
       payload assert in the consumer is the real check and this is its aggregate form.

       NOT ASSERTED: that everything pushed came out. See the header -- that is liveness, it is
       false for a bounded run, and demanding it would be a bug report against correct code. */
    int total = 0;
    for (int i = 1; i <= NITEMS; ++i) total += atomic_load(&g_claims[i]);
    assert(total <= NITEMS);

    return 0;
}
