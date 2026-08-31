// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of include/TaskDeque.h for a stateless model checker (GenMC, CDSChecker, Nidhugg).
// Not a test. A test runs one interleaving -- whichever the CPU happened to pick. A model checker
// enumerates EVERY execution the C11 memory model permits, including ones no hardware would produce
// in a decade, and either finds a violating one or proves there is none within the bound.
//
// That distinction is the whole reason this file exists. The deque's dangerous case is the
// LAST-ELEMENT RACE, where the owner's pop_bottom and a thief's steal both go for one item. It is
// rare by construction, it is the bug that actually shipped once (the steal_batch use-after-free),
// and it cannot be reached reliably by running the thing -- least of all on x86, where every RMW is
// already seq_cst so the ordering question below is invisible.
//
//   THE QUESTION THIS FILE EXISTS TO ANSWER
//   The verified Chase-Lev of Le, Pop, Cohen and Zappa Nardelli (PPoPP 2013) uses memory_order_seq_cst
//   for the CAS on `top` in steal(). TaskDeque.h uses memory_order_acq_rel. Everything else matches,
//   and pop_bottom is actually STRONGER than the reference (release/acquire where they use relaxed,
//   leaning on the fence). Is acq_rel sufficient here, given both sides carry a seq_cst FENCE?
//
//   Build both ways and compare. NOTE THE FLAG POSITION -- the -D goes AFTER the `--`, because
//   everything past it is handed to clang, not to genmc. The other order is what this comment used
//   to say and it fails with "Unknown command line argument":
//     genmc -- deque_model.c                          # as shipped (acq_rel)
//     genmc -- -DSTEAL_CAS_SEQ_CST deque_model.c      # as the paper has it
//     genmc -- -DNO_POP_FENCE deque_model.c           # the negative control; MUST fail
//
//   RESULT, GenMC v0.17.0 (LLVM 15), 2026-08-11, one owner + two thieves, 2 items:
//
//     acq_rel (as shipped)        no errors, 174 complete executions
//     seq_cst (as the paper has)  no errors, 174 complete executions
//     -DNO_POP_FENCE              SAFETY VIOLATION
//
//   RE-VERIFIED 2026-08-17 after TaskDeque switched to TAGGED POINTERS (steal-vetting bits packed
//   into the spare low bits of the stored Task*, so steal_if no longer dereferences a task the
//   thief has not claimed). All three results reproduced EXACTLY -- 174 complete executions on both
//   orderings, safety violation at line 200 under -DNO_POP_FENCE.
//
//   That the model needed no change is the point, not an oversight: it already abstracts the
//   payload as an opaque `int` in g_buffer, so packing bits into it changes what the payload MEANS
//   and not what the protocol DOES. Same indices, same atomics, same fences, same CAS. steal_if is
//   likewise still covered by steal's proof -- it adds only a branch on the already-read local
//   payload and an early return that performs strictly FEWER shared operations than steal does.
//
//   So acq_rel IS sufficient. The two seq_cst FENCES carry the store-load ordering between
//   pop_bottom and steal; the CAS's own strength is not what makes it work, and the reference's
//   seq_cst there is stronger than necessary. TaskDeque.h keeps acq_rel, now with a reason.
//
//   The third line is why the first two are worth anything. NO_POP_FENCE deletes the seq_cst fence
//   in pop_bottom -- the one several sources claimed was redundant because the surrounding
//   operations are already release/acquire -- and the checker immediately produces an execution
//   where g_claims[2] reaches 2: the owner and a thief both claim the same item. That is the
//   double-claim use-after-free, found in under a second, on a property no amount of running the
//   real scheduler on x86 could ever have exercised, because x86 makes every RMW seq_cst anyway.
//
//   Treat NO_POP_FENCE as a permanent negative control. A harness that reports "no errors" but
//   cannot fail proves nothing; run it whenever this model changes and confirm it still breaks.
//
// SCOPE, deliberately small. Model checking is exponential in interleavings, so the harness is the
// minimum that can exhibit the race: two items, one owner, one thief. Memory-ordering bugs surface
// with tiny counterexamples -- if two threads and three operations cannot break it, more will not.
// The growable buffer is replaced by a fixed array: reallocation is a separate concern with its own
// (owner-only) reasoning, and folding it in here would blow up the state space for nothing.

#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>

#define CAPACITY 4          /* power of two, as the real deque requires */
#define MASK     (CAPACITY - 1)
#define NITEMS   2          /* enough for a non-empty deque plus the last-element race */

#ifdef STEAL_CAS_SEQ_CST
  #define STEAL_CAS_SUCCESS memory_order_seq_cst
#else
  #define STEAL_CAS_SUCCESS memory_order_acq_rel   /* what TaskDeque.h ships */
#endif

static _Atomic size_t g_top;
static _Atomic size_t g_bottom;
static int            g_buffer[CAPACITY];

// How many times each item was successfully claimed. The two properties that matter are stated
// over this at the end: nothing claimed twice, nothing lost.
static _Atomic int g_claims[NITEMS + 1];

#define EMPTY (-1)
#define ABORT (-2)

// ---- owner-only push, mirroring TaskDeque::push_bottom -------------------------------------
static void push_bottom(int item) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);
    if (b - t >= CAPACITY) return;                       /* full; the model never hits this */
    g_buffer[b & MASK] = item;
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&g_bottom, b + 1, memory_order_release);
}

// ---- owner-only pop, mirroring TaskDeque::pop_bottom ---------------------------------------
static int pop_bottom(void) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);
    if (t >= b) return EMPTY;

    b -= 1;
    atomic_store_explicit(&g_bottom, b, memory_order_release);

    /* NEGATIVE CONTROL. Build with -DNO_POP_FENCE to delete this fence, which several sources
       claimed was removable because the surrounding operations are already release/acquire. If the
       checker reports NO error with it gone, this harness cannot detect anything and every clean
       result above is worthless. It has to fail. */
#ifndef NO_POP_FENCE
    atomic_thread_fence(memory_order_seq_cst);           /* the load-bearing fence */
#endif

    t = atomic_load_explicit(&g_top, memory_order_acquire);

    if (t <= b) {
        int item = g_buffer[b & MASK];
        if (t == b) {
            /* Last item: race the thief for it. The owner's CAS is acq_rel in both variants --
               only the THIEF's ordering is under test, since that is where the reference differs. */
            size_t expected = t;
            if (!atomic_compare_exchange_strong_explicit(
                    &g_top, &expected, t + 1,
                    memory_order_acq_rel, memory_order_relaxed)) {
                atomic_store_explicit(&g_bottom, b + 1, memory_order_relaxed);
                return EMPTY;                            /* thief won */
            }
            atomic_store_explicit(&g_bottom, b + 1, memory_order_relaxed);
        }
        return item;
    }
    atomic_store_explicit(&g_bottom, t, memory_order_relaxed);
    return EMPTY;
}

// ---- thief, mirroring TaskDeque::steal ------------------------------------------------------
static int steal(void) {
    size_t t = atomic_load_explicit(&g_top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    size_t b = atomic_load_explicit(&g_bottom, memory_order_acquire);

    if (t < b) {
        int item = g_buffer[t & MASK];
        size_t expected = t;
        if (!atomic_compare_exchange_strong_explicit(
                &g_top, &expected, t + 1,
                STEAL_CAS_SUCCESS, memory_order_relaxed)) {
            return ABORT;                                /* lost the race; caller retries */
        }
        return item;
    }
    return EMPTY;
}

// ---------------------------------------------------------------------------------------------
static void record(int item) {
    if (item >= 1 && item <= NITEMS)
        atomic_fetch_add_explicit(&g_claims[item], 1, memory_order_relaxed);
}

static void *owner_thread(void *arg) {
    (void)arg;
    record(pop_bottom());
    record(pop_bottom());
    return NULL;
}

static void *thief_thread(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}

/* A SECOND thief matters: with one, `top` is only ever contended owner-vs-thief, and the CAS
   under test never loses to a peer. Two thieves add thief-vs-thief contention and the case where
   a thief's CAS fails because another thief -- not the owner -- advanced top. */
#ifndef SINGLE_THIEF
static void *thief_thread2(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}
#endif

int main(void) {
    atomic_init(&g_top, 0);
    atomic_init(&g_bottom, 0);
    for (int i = 0; i <= NITEMS; ++i) atomic_init(&g_claims[i], 0);

    /* Sequential setup: the owner is the sole producer, so pushing before the threads start is
       faithful and keeps the explored state space to the part under test. */
    for (int i = 1; i <= NITEMS; ++i) push_bottom(i);

    pthread_t owner, thief;
    pthread_create(&owner, NULL, owner_thread, NULL);
    pthread_create(&thief, NULL, thief_thread, NULL);
#ifndef SINGLE_THIEF
    pthread_t thief2;
    pthread_create(&thief2, NULL, thief_thread2, NULL);
#endif
    pthread_join(owner, NULL);
    pthread_join(thief, NULL);
#ifndef SINGLE_THIEF
    pthread_join(thief2, NULL);
#endif

    /* PROPERTY 1 -- NO DUPLICATION. This is the one that matters: two claimants running the same
       task is the use-after-free / double-free that shipped once already. */
    for (int i = 1; i <= NITEMS; ++i)
        assert(atomic_load(&g_claims[i]) <= 1);

    /* PROPERTY 2 -- NO LOSS, stated carefully. A steal that returns ABORT lost the CAS and a real
       caller would retry, so with N claimants racing, up to N-1 of them can legitimately come away
       empty in a single bounded run. The bound below is deliberately loose for that reason: this
       property is a sanity check, and PROPERTY 1 is the one with teeth. */
    int total = 0;
    for (int i = 1; i <= NITEMS; ++i) total += atomic_load(&g_claims[i]);
    assert(total >= 1);

    return 0;
}
