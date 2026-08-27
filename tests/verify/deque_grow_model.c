// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of the OWNER-ONLY RESIZE that replaces push_bottom's `return false` in TaskDeque.
// Companion to deque_model.c, which deliberately does NOT cover this -- its scope note says
// "reallocation is a separate concern with its own (owner-only) reasoning". This is that file.
//
// THE ALGORITHM UNDER TEST. Called by the owner from push_bottom, having already observed
// b - t >= capacity_ with the same b and t the push will use:
//
//     newBuf = new uintptr_t[oldCap * 2]
//     for i in [t, b):  newBuf[i & newMask] = old[i & oldMask]   // copy by LOGICAL index
//     publish buffer_ = newBuf                                    // release
//     retire(old)                                                 // EBR/HP, NOT delete[]
//
// top_ AND bottom_ ARE NOT TOUCHED. That is what keeps the existing Chase-Lev proof intact: the CAS
// on top_ remains the sole arbiter of who owns a slot, and grow only changes WHERE that slot lives.
// Copying by LOGICAL index is what makes the two buffers agree -- a thief reading old[t & oldMask]
// and a thief reading new[t & newMask] read the SAME VALUE, so a grow racing a steal cannot change
// which task is claimed, only which array it came out of.
//
//   WHAT THIS FILE HAS TO ANSWER
//   Growing under a live thief is three separate claims, and each gets its own negative control
//   because a model that cannot fail proves nothing:
//
//     1. PUBLICATION ORDERING. The copy must be visible to a thief that acquires the new pointer.
//        -DNO_PUBLISH_RELEASE drops the release/acquire pair to relaxed; a thief may then load the
//        new buffer and read a slot the copy has not landed in yet.
//
//     2. RETIRE, NOT FREE. A thief may already be reading the old buffer when it is replaced.
//        -DNO_RETIRE models delete[]-at-grow by poisoning the old array the instant the new one is
//        published -- the reuse EBR exists to defer.
//
//     3. THE POINTER AND ITS MASK MUST BE ONE ATOMIC OBJECT. -DSPLIT_PTR_MASK stores them as two
//        independent atomics, which is the shape a first implementation reaches for, and a thief can
//        then pair a new pointer with an old mask and index a slot belonging to neither.
//
//   NOT YET RUN UNDER GenMC AS OF THIS COMMIT, and that is stated here rather than left for someone
//   to infer from the absence of a results block. deque_model.c records real numbers with a date and
//   a version; this file deliberately records none, because none exist yet.
//
//   What HAS been done: all four variants compile clean and exit 0 when run natively. That proves
//   the harness is not broken and nothing asserts trivially. It proves NOTHING ELSE -- a single x86
//   execution cannot exhibit any of the three bugs below, since x86 gives every RMW seq_cst ordering
//   for free and one interleaving is not a proof. The negative controls exiting 0 natively is the
//   EXPECTED result, not a problem, and is exactly why a model checker is the instrument here.
//
//   The CI job runs all four. Until that has gone green once, treat the algorithm as UNVERIFIED.
//
//   RUN THEM. Flag position matters -- -unroll is a GenMC option and goes BEFORE the --, the -D
//   defines and the file are compiler flags and go after:
//
//     genmc -unroll=8 -- tests/verify/deque_grow_model.c                        # expect: no errors
//     genmc -unroll=8 -- -DNO_PUBLISH_RELEASE tests/verify/deque_grow_model.c   # MUST fail
//     genmc -unroll=8 -- -DNO_RETIRE          tests/verify/deque_grow_model.c   # MUST fail
//     genmc -unroll=8 -- -DSPLIT_PTR_MASK     tests/verify/deque_grow_model.c   # MUST fail
//
// SCOPE, deliberately small, for the same reason deque_model.c is: model checking is exponential in
// interleavings. Capacity 2 growing to 4, three items, one owner and two thieves. A resize bug two
// thieves cannot expose will not appear with thirty.
//
// WHAT IS ABSTRACTED, and why each is faithful:
//
//   THE ALLOCATION IS TWO STATIC ARRAYS. `new` either returns memory or throws, and on throw the
//   queue is still entirely on the old buffer with nothing published -- the pre-grow state, which
//   deque_model.c already covers. Modelling the allocator adds no reachable state.
//
//   THE MASK IS DERIVED FROM THE POINTER in the default build, and that IS the "one atomic object"
//   requirement rather than a convenience: it makes the pair impossible to tear. SPLIT_PTR_MASK
//   replaces it with two atomics to show what that requirement buys.
//
//   RETIRE IS MODELLED AS "the old array is never written again". That is exactly the guarantee EBR
//   provides here and no more: reclamation is deferred past every reader that could still hold the
//   pointer. NO_RETIRE removes it.

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <assert.h>

#define CAP0    2                 /* power of two, as the real deque requires */
#define CAP1    (CAP0 * 2)
#define NITEMS  3                 /* CAP0 items fill it; the third push is the one that grows */

#define EMPTY   (-1)
#define ABORT   (-2)
#define POISON  (-99)             /* what a reused block looks like; see NO_RETIRE */

static _Atomic size_t g_top;
static _Atomic size_t g_bottom;

static int g_bufA[CAP0];
static int g_bufB[CAP1];

#ifdef NO_PUBLISH_RELEASE
  #define PUBLISH_ORDER memory_order_relaxed
  #define ACQUIRE_ORDER memory_order_relaxed
#else
  #define PUBLISH_ORDER memory_order_release   /* what the algorithm requires */
  #define ACQUIRE_ORDER memory_order_acquire
#endif

static int *_Atomic g_buf;

#ifdef SPLIT_PTR_MASK
/* THE BUG THIS CONTROL EXISTS TO SHOW. Two independent atomics can be read in either order and from
   either generation, so a thief can pair the NEW pointer with the OLD mask -- or the reverse -- and
   index a slot that belongs to neither. */
static _Atomic size_t g_mask;
static size_t mask_of(int *buf) { (void)buf; return atomic_load_explicit(&g_mask, ACQUIRE_ORDER); }
#else
/* ONE ATOMIC OBJECT, expressed as "the mask is a function of the pointer". The real implementation
   gets the same property from an atomic struct {ptr, cap, mask}; what matters is that a reader
   cannot observe a pointer from one generation with a mask from another. */
static size_t mask_of(int *buf) { return (buf == g_bufA) ? (CAP0 - 1) : (CAP1 - 1); }
#endif

static _Atomic int g_claims[NITEMS + 1];

/* ---- owner-only grow ------------------------------------------------------------------------ */
static void grow(size_t t, size_t b) {
    int   *old     = atomic_load_explicit(&g_buf, memory_order_relaxed);   /* owner: sole writer */
    size_t oldMask = mask_of(old);
    int   *nbuf    = g_bufB;
    size_t newMask = CAP1 - 1;

    /* COPY BY LOGICAL INDEX. i is the absolute index, so the same i lands at a different physical
       slot under the new mask while holding the same value -- which is what lets a racing thief
       read either array and claim the same task. */
    for (size_t i = t; i != b; ++i)
        nbuf[i & newMask] = old[i & oldMask];

#ifdef SPLIT_PTR_MASK
    atomic_store_explicit(&g_mask, newMask, PUBLISH_ORDER);
#endif
    atomic_store_explicit(&g_buf, nbuf, PUBLISH_ORDER);

#ifdef NO_RETIRE
    /* delete[] AT GROW, modelled. A thief that loaded the old pointer before the publish is still
       reading through it; freeing here lets the allocator hand the block out again. */
    for (size_t i = 0; i < CAP0; ++i) old[i] = POISON;
#endif
    /* With retire: the old array is simply never written again. EBR defers the actual free past
       every reader that could still hold the pointer, which is the guarantee -- and all of it. */
}

/* ---- owner-only push, with the resize replacing `return false` ------------------------------- */
static void push_bottom(int item) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);

    int   *buf  = atomic_load_explicit(&g_buf, memory_order_relaxed);
    size_t mask = mask_of(buf);

    if (b - t >= mask + 1) {
        grow(t, b);                                        /* the algorithm under test */
        buf  = atomic_load_explicit(&g_buf, memory_order_relaxed);
        mask = mask_of(buf);
    }

    buf[b & mask] = item;
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&g_bottom, b + 1, memory_order_release);
}

/* ---- owner-only pop, unchanged from deque_model.c except for the buffer indirection ---------- */
static int pop_bottom(void) {
    size_t b = atomic_load_explicit(&g_bottom, memory_order_relaxed);
    size_t t = atomic_load_explicit(&g_top,    memory_order_acquire);
    if (t >= b) return EMPTY;

    b -= 1;
    atomic_store_explicit(&g_bottom, b, memory_order_release);
    atomic_thread_fence(memory_order_seq_cst);
    t = atomic_load_explicit(&g_top, memory_order_acquire);

    if (t <= b) {
        int   *buf  = atomic_load_explicit(&g_buf, ACQUIRE_ORDER);
        size_t mask = mask_of(buf);
        int    item = buf[b & mask];
        if (t == b) {
            size_t expected = t;
            if (!atomic_compare_exchange_strong_explicit(
                    &g_top, &expected, t + 1,
                    memory_order_acq_rel, memory_order_relaxed)) {
                atomic_store_explicit(&g_bottom, b + 1, memory_order_relaxed);
                return EMPTY;
            }
            atomic_store_explicit(&g_bottom, b + 1, memory_order_relaxed);
        }
        return item;
    }
    atomic_store_explicit(&g_bottom, t, memory_order_relaxed);
    return EMPTY;
}

/* ---- thief ----------------------------------------------------------------------------------- */
static int steal(void) {
    size_t t = atomic_load_explicit(&g_top, memory_order_acquire);
    atomic_thread_fence(memory_order_seq_cst);
    size_t b = atomic_load_explicit(&g_bottom, memory_order_acquire);

    if (t < b) {
        /* RELOAD THE BUFFER ON EVERY ATTEMPT -- it is not loop-invariant once the deque can grow,
           which is the one thing resizing forces on the steal path. The acquire pairs with the
           publishing release, so the copy is visible before anything read through this pointer. */
        int   *buf  = atomic_load_explicit(&g_buf, ACQUIRE_ORDER);
        size_t mask = mask_of(buf);
        int    item = buf[t & mask];

        size_t expected = t;
        if (!atomic_compare_exchange_strong_explicit(
                &g_top, &expected, t + 1,
                memory_order_acq_rel, memory_order_relaxed)) {
            return ABORT;                                  /* lost the race; caller retries */
        }
        return item;
    }
    return EMPTY;
}

/* --------------------------------------------------------------------------------------------- */
static void record(int item) {
    /* A POISONED READ IS A FAILURE IN ITSELF, asserted here rather than at the end so the
       counterexample points at the read. This is what NO_RETIRE trips. */
    assert(item != POISON);
    if (item >= 1 && item <= NITEMS)
        atomic_fetch_add_explicit(&g_claims[item], 1, memory_order_relaxed);
}

static void *owner_thread(void *arg) {
    (void)arg;
    push_bottom(NITEMS);        /* the deque is full, so THIS push grows */
    record(pop_bottom());
    return NULL;
}

static void *thief_thread(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}

/* Two thieves for the same reason deque_model.c has two: with one, top_ is only ever contended
   owner-vs-thief and a thief's CAS never loses to a peer. */
static void *thief_thread2(void *arg) {
    (void)arg;
    record(steal());
    return NULL;
}

int main(void) {
    atomic_init(&g_top, 0);
    atomic_init(&g_bottom, 0);
    atomic_init(&g_buf, g_bufA);
#ifdef SPLIT_PTR_MASK
    atomic_init(&g_mask, CAP0 - 1);
#endif
    for (int i = 0; i <= NITEMS; ++i) atomic_init(&g_claims[i], 0);
    for (int i = 0; i < CAP1; ++i) g_bufB[i] = POISON;   /* uncopied slots must never be claimed */

    /* Sequential setup: the owner is the sole producer, so filling to capacity before the threads
       start is faithful and keeps the explored space to the part under test. */
    for (int i = 1; i < NITEMS; ++i) push_bottom(i);

    pthread_t owner, thief, thief2;
    pthread_create(&owner,  NULL, owner_thread,  NULL);
    pthread_create(&thief,  NULL, thief_thread,  NULL);
    pthread_create(&thief2, NULL, thief_thread2, NULL);
    pthread_join(owner,  NULL);
    pthread_join(thief,  NULL);
    pthread_join(thief2, NULL);

    /* PROPERTY 1 -- NO DUPLICATION ACROSS A RESIZE. The item exists in BOTH arrays while the grow
       is in flight, so this is the property a naive copy breaks: an owner and a thief reading
       different buffers must still not both come away owning it. top_ is the arbiter, and grow
       must not disturb that. */
    for (int i = 1; i <= NITEMS; ++i)
        assert(atomic_load(&g_claims[i]) <= 1);

    /* PROPERTY 2 -- NO LOSS IN THE COPY. Everything in [t, b) must survive the move. Stated loosely
       for the same reason as deque_model.c: with three claimants racing, an ABORT is legitimate, so
       this is a sanity bound and PROPERTY 1 is the one with teeth. */
    int total = 0;
    for (int i = 1; i <= NITEMS; ++i) total += atomic_load(&g_claims[i]);
    assert(total >= 1);

    return 0;
}
