// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of COUNTED EPOCHS -- the reclamation scheme that would let a COROUTINE hold epoch
// protection across a suspension. Not implemented anywhere yet; this file exists to find out
// whether it can be, before any of it is written.
//
//   genmc -- counted_epoch_model.c                       # as designed; must pass
//   genmc -- -DNO_REVALIDATE counted_epoch_model.c       # negative control; MUST fail
//   genmc -- -DNO_ADVANCE_GATE counted_epoch_model.c     # negative control; MUST fail
//
// (Flags before the `--` go to genmc, everything after to clang. See deque_model.c's header.)
//
// == WHY THIS SCHEME AT ALL ==
//
// Today a reader ANNOUNCES ITS EPOCH IN A SLOT and reclamation scans every slot for the minimum.
// That needs a stable slot per reader, which fibers have (a fixed pool, one slot each, registered
// once) and COROUTINES CANNOT. A coroutine borrows the worker's slot, and a coroutine is not bound
// to a worker: park inside a guard and the worker's next guard overwrites the announcement. For a
// fiber, suspending inside a guard leaks; for a coroutine it CORRUPTS.
//
// Giving coroutines slots was tried and reverted. Any FIXED pool reintroduces the exact ceiling
// coroutines exist to escape -- a reactor's steady state is thousands of parked operations -- and a
// pool sized for the well-behaved case is empty precisely when the misbehaving case needs it
// (measured: 132 parked against 62 slots, 37 fell back).
//
// So change the question. Not "where is every reader" but "HOW MANY READERS ARE IN EACH EPOCH":
//
//     enter:   e = globalEpoch;  counters[e % N]++      -> the token is just `e`
//     leave:   counters[token % N]--
//     reclaim: free what was retired below the oldest epoch with a nonzero count
//
// Identity disappears. A coroutine holds `e` in its own frame, migrates freely, and shares nothing
// with any worker. Unbounded readers, no pool, no registration. A parked reader stalls advancement
// -- a leak bounded by the park, which is exactly the fiber behaviour -- and corruption is
// structurally impossible because there is no shared slot to clobber.
//
// This is SRCU in shape (sleepable RCU: readers may block, which classic RCU forbids).
//
// == THE TWO THINGS THIS FILE EXISTS TO CHECK ==
//
// 1. THE ACQUIRE/ADVANCE RACE. `enter` reads the epoch and then increments its counter, and those
//    are two operations. The interleaving that kills it:
//
//        reader: e = globalEpoch (42)
//                                      reclaimer: advance past 42
//                                      reclaimer: nothing is in 42, free it
//        reader: counters[42]++
//        reader: dereference -> USE AFTER FREE
//
//    The intended fix is optimistic re-validation: increment, RE-READ the epoch, and back out and
//    retry if it moved. Nothing is dereferenced before the re-check, so a stale acquire is
//    harmless. -DNO_REVALIDATE removes the re-check and must fail.
//
// 2. LAPPING. Counters are indexed `e % N`, so a reader parked at epoch 42 shares a slot with epoch
//    42+N and the reclaimer cannot tell which is held. The intended fix is to GATE ADVANCEMENT --
//    refuse to move into a slot that is still non-zero -- so a slot's epoch is never ambiguous. A
//    parked reader then stalls advancement after N steps: reclamation stops, nothing corrupts.
//    -DNO_ADVANCE_GATE removes the gate and must fail.
//
// N IS 2 HERE, deliberately. The real thing wants 8 or so; two is the smallest ring in which
// lapping is reachable inside a model checker's budget, and a scheme that is wrong at N=2 is wrong.
//
// WHAT IS NOT MODELLED, so nobody reads more into a pass than is there: the sharded per-worker
// counters a real implementation would need for contention, and the full SafeEpoch scan. The
// reclaim condition below is the narrowest one that can expose both races with one reader, which is
// the point -- a richer model would let a broken protocol pass by accident, which is the vacuous
// -test trap this project has hit before.

#include <stdatomic.h>
#include <stdint.h>
#include <pthread.h>
#include <assert.h>

#define NSLOTS   2                       /* ring size; see header */
#define SLOT(e)  ((e) & (NSLOTS - 1))    /* power of two, so the modulo is a mask */
#define MAX_TRY  2                        /* bound the retry loop -- a model checker needs finite */
#define MAX_ADV  2                        /* enough advances to lap a ring of 2 */

static _Atomic(unsigned) g_epoch = 0;
static _Atomic(int)      g_counters[NSLOTS];

/* The object under protection. `g_freed` standing in for "returned to the allocator": a reader
   touching it after that is the use-after-free this whole scheme exists to prevent. */
static _Atomic(int) g_freed = 0;
static _Atomic(int) g_retired_at = 0;

// ---- reader ------------------------------------------------------------------------------------
//
// Returns the epoch acquired, or (unsigned)-1 if it gave up. Giving up is not a correctness
// failure; it is how the bounded retry keeps the model finite.
static unsigned enter_epoch(void) {
    for (int attempt = 0; attempt < MAX_TRY; ++attempt) {
        unsigned e = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
        atomic_fetch_add_explicit(&g_counters[SLOT(e)], 1, memory_order_seq_cst);

#ifndef NO_REVALIDATE
        /* THE RE-CHECK. If the epoch moved between our load and our increment, a reclaimer may have
           already decided nothing was in `e` and freed accordingly -- so our count is worthless and
           we must not proceed on it. Back out and try again at the newer epoch.

           This is sound only because NOTHING HAS BEEN DEREFERENCED YET. The increment alone cannot
           hurt anyone; acting on it can. Same publish-then-validate shape as a hazard pointer, and
           the same store-buffer argument as the deque's pop_bottom fence. */
        unsigned again = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
        if (again != e) {
            atomic_fetch_sub_explicit(&g_counters[SLOT(e)], 1, memory_order_seq_cst);
            continue;
        }
#endif
        return e;
    }
    return (unsigned)-1;
}

static void leave_epoch(unsigned token) {
    atomic_fetch_sub_explicit(&g_counters[SLOT(token)], 1, memory_order_seq_cst);
}

static void *reader(void *arg) {
    (void)arg;
    unsigned tok = enter_epoch();
    if (tok != (unsigned)-1) {
        /* THE ACCESS. Everything above exists so that this is safe. */
        assert(atomic_load_explicit(&g_freed, memory_order_seq_cst) == 0);
        leave_epoch(tok);
    }
    return 0;
}

// ---- reclaimer ---------------------------------------------------------------------------------

static int try_advance(void) {
    unsigned e = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
    unsigned next = e + 1;

#ifndef NO_ADVANCE_GATE
    /* THE GATE. Moving into a slot that still has readers makes that slot ambiguous: a reader
       parked at `next - NSLOTS` becomes indistinguishable from one entering at `next`, and the
       reclaimer can no longer tell which epoch is actually held. Refusing to advance keeps every
       slot's epoch unique, at the cost of stalling reclamation while somebody is parked -- which
       is the leak we are willing to accept and exactly what a parked fiber already costs. */
    if (atomic_load_explicit(&g_counters[SLOT(next)], memory_order_seq_cst) != 0) return 0;
#endif

    return atomic_compare_exchange_strong_explicit(
        &g_epoch, &e, next, memory_order_seq_cst, memory_order_seq_cst);
}

static void *reclaimer(void *arg) {
    (void)arg;

    /* Retire at the current epoch: no reader entering AFTER this can reach the object, but readers
       already inside this epoch or earlier still can. */
    unsigned r = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
    atomic_store_explicit(&g_retired_at, (int)r, memory_order_seq_cst);

    for (int i = 0; i < MAX_ADV; ++i) try_advance();

    /* THE RECLAIM CONDITION, kept as narrow as it can be: free only once nobody is counted in the
       epoch the object was retired in. With one reader that is exactly the condition that must hold,
       and keeping it minimal is what stops a broken protocol passing by accident. */
    if (atomic_load_explicit(&g_counters[SLOT(r)], memory_order_seq_cst) == 0)
        atomic_store_explicit(&g_freed, 1, memory_order_seq_cst);

    return 0;
}

int main(void) {
    pthread_t tr, tc;
    pthread_create(&tr, 0, reader, 0);
    pthread_create(&tc, 0, reclaimer, 0);
    pthread_join(tr, 0);
    pthread_join(tc, 0);
    return 0;
}
