// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of Event's FIBER-INDEXED WAITER TABLE -- the slot array plus its occupancy bitmap -- for
// a stateless model checker. Companion to event_model.c (which models the Treiber waiter stack) and
// deque_model.c; see that file's header for what a model checker is and why running the real thing
// is not a substitute.
//
//   THIS IS NOT A REPLACEMENT FOR event_model.c YET. The stack is still the wake path in the
//   shipping header; this models the table that sits beside it, and the protocol that WOULD let the
//   table become the wake path (see scratchpad/event-table-wake-path-scope.md). Half of what is
//   below ships today and is unverified: AddWaiter already does the slot-then-bit publish, and
//   CancelWaiters already reads through it.
//
//   THE QUESTIONS THIS FILE EXISTS TO ANSWER
//
//   1. PUBLICATION. AddWaiter stores the Task* into slots[i] and THEN sets bit i. A reader that
//      sees the bit set must see the task, never a null slot. That rests on the release on the
//      fetch_or synchronising with the acquire on the claiming RMW -- an argument, not an
//      observation, and the same release-sequence reasoning event_model.c exists to check, applied
//      to a different pair of operations.
//
//   2. EXCLUSIVE CLAIM. A waiter must be taken by exactly one claimer even when SignalAll and
//      SignalOne run concurrently. THE PROTOCOL HAS TWO ARBITERS AND THIS FILE MODELS BOTH:
//      the bitmap RMW (fetch_and returning the bit set) and, behind it, the slot exchange -- only
//      one thread can exchange a non-null Task* out of slots[i]; anyone else gets NULL.
//
//      Fiber::ResumeQueueless's SUSPENDED -> READY CAS would be a THIRD arbiter in the real code
//      and is deliberately NOT modelled. Including it would let this file pass with a completely
//      broken claim protocol and tell us nothing -- the vacuous-test trap the primitives suite hit
//      once already (a suite that passed 1-in-3 with the mutex removed).
//
//      A FINDING, recorded because it was not obvious before running this: the two arbiters are not
//      independent, and the slot exchange is the stronger one. Break the bit claim (either control
//      below) and the failure surfaces at the PUBLICATION assert -- the losing claimer reads NULL --
//      never as a double count. That is why assert(prev == 0) in take() cannot fire in this
//      configuration; it is kept as a tripwire for a future model that re-parks a fiber on the same
//      slot, which is the only way two claimers could both see a non-null task. It is honest to say
//      it is currently unreachable rather than to leave it looking like a property being proved.
//
//   WHAT IS DELIBERATELY *NOT* ASSERTED: that a waiter arriving DURING a SignalAll is woken by it.
//   The stack takes every waiter in a single exchange and so has one linearization point; a word
//   scan does not, and a waiter landing in an already-scanned word is legitimately missed. The
//   guarantee callers rely on is weaker and still holds: registered-before-the-signal-began implies
//   woken. WaitOnEventArmed depends on exactly that and no more, since it registers before arming
//   whatever can signal. So, like event_model.c, this drains again at quiescence and asserts each
//   waiter was taken exactly once OVERALL.
//
//   Build variants:
//     genmc -- event_table_model.c                      # as shipped
//     genmc -- -DNO_RELEASE event_table_model.c         # negative control, MUST fail (Q1)
//     genmc -- -DCLAIM_NOT_RMW event_table_model.c      # negative control, MUST fail (Q2)
//     genmc -- -DCLAIM_ALWAYS_WINS event_table_model.c  # negative control, MUST fail (Q2)
//
//   RESULTS: recorded at the bottom of this file.

#include <stdatomic.h>
#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define NFIBERS 2     /* one slot per fiber; two is enough for two concurrent claimers to collide */
#define NWORDS  1     /* ceil(NFIBERS/64) */

/* Mirrors Task only as far as this protocol cares: identity, so a double claim is detectable. */
typedef struct Node { int id; } Node;

static Node               g_nodes[NFIBERS];
static _Atomic(Node *)    g_slots[NFIBERS];
static _Atomic(uint64_t)  g_occupied[NWORDS];
static _Atomic int        g_claimed[NFIBERS + 1];

#ifdef NO_RELEASE
  /* Negative control for Q1: publish the occupancy bit with relaxed ordering, i.e. the claim that
     "the slot store is already a release, so the bit does not need one". It is not enough -- a
     release on a DIFFERENT location does not order this one, so the claimer can win the bit and
     then read a slot that is still null. */
  #define PUBLISH_ORDER memory_order_relaxed
#else
  #define PUBLISH_ORDER memory_order_release
#endif

// ---- Event::AddWaiter (the indexing half) ------------------------------------------------------
// SLOT FIRST, THEN BIT. The bit is what a scan trusts, so it must not become visible before the
// pointer it advertises.
static void add_waiter(int i) {
    atomic_store_explicit(&g_slots[i], &g_nodes[i], memory_order_release);
    atomic_fetch_or_explicit(&g_occupied[i >> 6], (uint64_t)1 << (i & 63), PUBLISH_ORDER);
}

// ---- taking a waiter whose bit this thread just won --------------------------------------------
static void take(int i) {
    /* exchange rather than load+store: after a claim the fiber can be reused and re-parked at this
       same index, so there must be no window between reading the slot and clearing it. */
    Node *t = atomic_exchange_explicit(&g_slots[i], NULL, memory_order_acq_rel);

    /* PROPERTY 3 -- PUBLICATION, and in practice the load-bearing safety check of the whole
       protocol. Winning the bit must mean the task is visible; a NULL here means either the
       slot store was not published in time (Q1) or another claimer already took this waiter
       (Q2). Both negative controls land on this line, which is the finding in the header. */
    assert(t != NULL);

    const int prev = atomic_fetch_add_explicit(&g_claimed[t->id], 1, memory_order_relaxed);

    /* PROPERTY 2 -- NO WAITER CLAIMED TWICE. UNREACHABLE IN THIS CONFIGURATION and kept
       deliberately: the slot exchange above turns every double claim into a NULL read, so a
       second claimer never gets here with a task. This becomes live only in a model that
       re-parks a fiber on the same slot. Do not read a passing run as evidence for it. */
    assert(prev == 0);

    /* Fiber::ResumeQueueless() would go here. Deliberately absent -- see header. */
}

// ---- Event::SignalAll ---------------------------------------------------------------------------
// One exchange per WORD. That is the analogue of the stack's take-everything-in-one-exchange, but
// per word rather than globally -- which is precisely the property that weakens, see the header.
static void signal_all(void) {
    for (int w = 0; w < NWORDS; ++w) {
        uint64_t bits = atomic_exchange_explicit(&g_occupied[w], 0, memory_order_acq_rel);
        while (bits) {
            const int b = __builtin_ctzll(bits);
            bits &= bits - 1;                       /* clear the lowest set bit */
            take(w * 64 + b);
        }
    }
}

// ---- Event::SignalOne ---------------------------------------------------------------------------
// Scan for a candidate, then try to CLAIM it. Losing the claim is not failure -- another signaller
// took that waiter -- so move on to the next candidate bit rather than giving up.
static int signal_one(void) {
    for (int w = 0; w < NWORDS; ++w) {
        uint64_t bits = atomic_load_explicit(&g_occupied[w], memory_order_acquire);
        while (bits) {
            const int b = __builtin_ctzll(bits);
            const uint64_t m = (uint64_t)1 << b;
            int won;
#ifdef CLAIM_ALWAYS_WINS
            /* Negative control for Q2, aimed at the DOUBLE CLAIM specifically: keep the RMW
               (so no lost update) but ignore what it returns, i.e. the reasoning "the scan
               already told me this bit was set, so the waiter is mine". It is not -- another
               claimer can have taken it between the scan and the RMW, and then both wake it.
               This is the control that makes assert(prev == 0) in take() non-vacuous. */
            atomic_fetch_and_explicit(&g_occupied[w], ~m, memory_order_acq_rel);
            won = 1;
#elif defined(CLAIM_NOT_RMW)
            /* Negative control for Q2: read-modify-write split into a load and a store, i.e. the
               claim that "we already know the bit is set, so just clear it". Two claimers can both
               observe it set and both proceed, and the same waiter is woken twice. */
            const uint64_t cur = atomic_load_explicit(&g_occupied[w], memory_order_acquire);
            won = (cur & m) != 0;
            if (won)
                atomic_store_explicit(&g_occupied[w], cur & ~m, memory_order_release);
#else
            const uint64_t old = atomic_fetch_and_explicit(&g_occupied[w], ~m,
                                                           memory_order_acq_rel);
            won = (old & m) != 0;
#endif
            if (won) { take(w * 64 + b); return 1; }
            bits &= ~m;                             /* someone else took it; try the next */
        }
    }
    return 0;
}

// ---------------------------------------------------------------------------------------------
static void *pusher0(void *arg) { (void)arg; add_waiter(0); return NULL; }
static void *pusher1(void *arg) { (void)arg; add_waiter(1); return NULL; }
static void *waker_all(void *arg) { (void)arg; signal_all();  return NULL; }
static void *waker_one(void *arg) { (void)arg; signal_one();  return NULL; }

int main(void) {
    for (int i = 0; i < NFIBERS; ++i) {
        g_nodes[i].id = i + 1;
        atomic_init(&g_slots[i], NULL);
        atomic_init(&g_claimed[i + 1], 0);
    }
    for (int w = 0; w < NWORDS; ++w) atomic_init(&g_occupied[w], 0);

    pthread_t p0, p1, wa, wo;
    pthread_create(&p0, NULL, pusher0,   NULL);
    pthread_create(&p1, NULL, pusher1,   NULL);
    pthread_create(&wa, NULL, waker_all, NULL);
    pthread_create(&wo, NULL, waker_one, NULL);
    pthread_join(p0, NULL);
    pthread_join(p1, NULL);
    pthread_join(wa, NULL);
    pthread_join(wo, NULL);

    /* The concurrent signals may have run before either push, or between them. Drain again now that
       everything is quiescent, so every waiter must have been taken exactly once overall. */
    signal_all();

    /* PROPERTY 1 -- NO WAITER LOST. A fiber that parks and is never resumed hangs forever. */
    /* PROPERTY 2 -- NO WAITER CLAIMED TWICE. Resuming a completed task touches a recycled slab
       slot. Here the bitmap RMW is the ONLY thing preventing it -- see the header. */
    for (int i = 1; i <= NFIBERS; ++i)
        assert(atomic_load(&g_claimed[i]) == 1);

    return 0;
}

// RESULTS -- GenMC v0.17.0 (LLVM 15.0.7), 2026-08-24. Two pushers + a concurrent SignalAll + a
// concurrent SignalOne, then a quiescent SignalAll:
//
//   as shipped             no errors, 36 complete executions
//   -DNO_RELEASE           Error: Safety violation -- assert(t != NULL), line 102
//   -DCLAIM_NOT_RMW        Error: Safety violation -- assert(t != NULL), line 102
//   -DCLAIM_ALWAYS_WINS    Error: Safety violation -- assert(t != NULL), line 102
//
// So the publish-slot-then-set-bit ordering is correct, and a waiter is claimed exactly once across
// a concurrent SignalAll and SignalOne.
//
// READ THE CONTROLS CAREFULLY, because they did not fail the way they were predicted to and that is
// the most useful thing this file produced:
//
//   - NO_RELEASE was predicted to trip the publication assert, and does. As expected.
//
//   - CLAIM_NOT_RMW was predicted to double-claim. It does not. Its first reachable defect is a LOST
//     UPDATE: the load/store pair clobbers a bit that a concurrent AddWaiter set in between, so that
//     waiter is dropped from the bitmap and never woken. An earlier revision of this model caught it
//     at the end-of-main count instead. Same broken operation, different failure -- worth knowing,
//     because "it would wake someone twice" is the wrong thing to go looking for in a bug report.
//
//   - CLAIM_ALWAYS_WINS was written specifically to make assert(prev == 0) fire, and could not. It
//     lands on the publication assert too.
//
// THE UNDERLYING REASON, and the real finding: the slot exchange is a second, STRONGER arbiter than
// the bitmap claim. Only one thread can exchange a non-null Task* out of a slot, so every double
// claim degenerates into the loser reading NULL before it can act. The bit claim keeps the bitmap
// itself honest; the exchange is what actually guarantees wake-once. Both belong in the real code --
// SignalAll/SignalOne must keep the `if (t)` null check, and it is not defensive padding, it IS the
// arbitration.
//
// Consequently assert(prev == 0) is UNREACHABLE here. It is kept as a tripwire for a future model
// that re-parks a fiber onto the same slot, which is the only way two claimers could both hold a
// non-null task. Do not count it among the properties this file proves today.
//
// Keep all three controls and re-run them whenever this model changes. A control that stops failing
// is a broken control; a control that fails somewhere new is a finding.
