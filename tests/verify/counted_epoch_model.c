// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A MODEL of COUNTED EPOCHS -- the reclamation scheme that would let a COROUTINE hold epoch
// protection across a suspension. Not implemented anywhere yet; this file exists to find out
// whether it can be, before any of it is written.
//
//   genmc -- counted_epoch_model.c                        # as designed; passes
//   genmc -- -DNO_ADVANCE_GATE counted_epoch_model.c      # control; FAILS, as it must
//   genmc -- -DNO_REVALIDATE counted_epoch_model.c        # control; PASSES -- see RESULTS below
//   genmc -- -DTWO_READERS counted_epoch_model.c          # a second reader; 831 executions
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
//    The intended fix was optimistic re-validation: increment, RE-READ the epoch, and back out and
//    retry if it moved. -DNO_REVALIDATE removes it. IT TURNS OUT NOT TO BE NEEDED -- see RESULTS.
//
// 2. LAPPING. Counters are indexed `e % N`, so a reader parked at epoch 42 shares a slot with epoch
//    42+N and the reclaimer cannot tell which is held. The intended fix is to GATE ADVANCEMENT --
//    refuse to move into a slot that is still non-zero -- so a slot's epoch is never ambiguous. A
//    parked reader then stalls advancement after N steps: reclamation stops, nothing corrupts.
//    -DNO_ADVANCE_GATE removes the gate. IT IS NEEDED, and GenMC proves it -- see RESULTS.
//
// N IS 2 HERE, deliberately. The real thing wants 8 or so; two is the smallest ring in which
// lapping is reachable inside a model checker's budget, and a scheme that is wrong at N=2 is wrong.
//
// ================================================================================================
// == RESULTS (GenMC 0.17.0, 2026-08-25) ==
//
//   as designed                        PASS   18 executions (831 with -DTWO_READERS)
//   -DNO_ADVANCE_GATE                  FAIL   <- the gate is PROVEN necessary
//   -DNO_REVALIDATE                    PASS   <- the control does NOT fire. See below.
//   -DTWO_READERS -DNO_REVALIDATE      PASS   <- still does not fire with a second reader
//
// FINDING 1: THE GATE IS LOAD-BEARING AND PROVEN. Without it the reclaimer attributes a nonzero
// slot to the wrong epoch -- a reader parked NSLOTS epochs back is indistinguishable from a fresh
// one -- and GenMC produces the use-after-free. This is not an argument any more.
//
// FINDING 2: THE RE-VALIDATION IS REDUNDANT, and the reason is structural rather than a gap in the
// model. Two orderings conspire:
//
//   * the reclaimer UNLINKS BEFORE it checks the counters, and
//   * a reader ANNOUNCES BEFORE it loads the protected pointer.
//
// So a reader whose increment lands after the reclaimer's check has necessarily loaded the pointer
// after the unlink -- it reads null and touches nothing. The stale-acquire window closes itself.
//
// THAT IS A REAL SIMPLIFICATION: no retry loop, and with it goes the livelock question a retry loop
// would have raised (can a reader spin forever while advances keep succeeding?). But it converts an
// incidental ordering into a LOAD-BEARING REQUIREMENT, which is the thing to carry forward:
//
//     ANY code path that loads a protected pointer BEFORE announcing its epoch breaks this, and
//     breaks it silently. Re-validation is what would have made such a path safe. Dropping it means
//     announce-then-traverse is now part of the contract, not just the convention.
//
// If that ordering is ever hard to guarantee at a call site, put the re-validation back rather than
// reasoning about the particular case -- it costs one extra load on the fast path.
//
// A CAVEAT ON FINDING 2, stated because a passing control is exactly what a vacuous test looks
// like: this says the race is unreachable IN THIS MODEL, with these two orderings, at N=2, with up
// to two readers and two advances. It does not say re-validation is unnecessary in general, and the
// event-table model has already produced one "this assert is currently unreachable" that was honest
// only because it was labelled as such.
// ================================================================================================
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
#define NSHARDS  2                       /* see the sharding note in the header */
#define MAX_TRY  2                        /* bound the retry loop -- a model checker needs finite */
#define MAX_ADV  2                        /* enough advances to lap a ring of 2 */

static _Atomic(unsigned) g_epoch = 0;
static _Atomic(int)      g_counters[NSLOTS][NSHARDS];

/* Sum one ring slot across shards. The ONLY thing anybody reads -- individual shards may be
   negative, which is the mechanism rather than a bug. See enter/leave. */
static int ring_total(unsigned ring) {
    int t = 0;
    for (unsigned s = 0; s < NSHARDS; ++s)
        t += atomic_load_explicit(&g_counters[ring][s], memory_order_seq_cst);
    return t;
}

/* THE OBJECT AND ITS REACHABILITY. `g_ptr` is the link a reader traverses; `g_freed` stands in for
   "returned to the allocator". Modelling the link is not decoration -- the FIRST version of this
   file asserted on every reader access and GenMC immediately found a counterexample in which the
   reader entered AFTER the object was retired. That reader cannot reach the object in real EBR,
   because retiring means unlinking first, so the model was flagging a safe execution. A model that
   cannot express reachability cannot express what EBR protects. */
typedef struct { int v; } Obj;
static Obj                g_obj      = { 7 };
static _Atomic(Obj *)     g_ptr      = &g_obj;
static _Atomic(int)       g_freed    = 0;

// ---- reader ------------------------------------------------------------------------------------
//
// Returns the epoch acquired, or (unsigned)-1 if it gave up. Giving up is not a correctness
// failure; it is how the bounded retry keeps the model finite.
static unsigned enter_epoch(unsigned shard) {
    for (int attempt = 0; attempt < MAX_TRY; ++attempt) {
        unsigned e = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
        atomic_fetch_add_explicit(&g_counters[SLOT(e)][shard], 1, memory_order_seq_cst);

#ifndef NO_REVALIDATE
        /* THE RE-CHECK. If the epoch moved between our load and our increment, a reclaimer may have
           already decided nothing was in `e` and freed accordingly -- so our count is worthless and
           we must not proceed on it. Back out and try again at the newer epoch.

           This is sound only because NOTHING HAS BEEN DEREFERENCED YET. The increment alone cannot
           hurt anyone; acting on it can. Same publish-then-validate shape as a hazard pointer, and
           the same store-buffer argument as the deque's pop_bottom fence. */
        unsigned again = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
        if (again != e) {
            atomic_fetch_sub_explicit(&g_counters[SLOT(e)][shard], 1, memory_order_seq_cst);
            continue;
        }
#endif
        return e;
    }
    return (unsigned)-1;
}

/* LEAVE ON A DIFFERENT SHARD THAN WE ENTERED ON -- the migration case, and the whole reason this
   dimension exists. A coroutine enters on worker A and resumes on worker B, so the decrement lands
   elsewhere: A sits at +1, B at -1, and only the SUM is correct. If that is wrong, it is wrong
   here. */
static void leave_epoch(unsigned token, unsigned shard) {
    atomic_fetch_sub_explicit(&g_counters[SLOT(token)][shard], 1, memory_order_seq_cst);
}

static void *reader(void *arg) {
    (void)arg;
    const unsigned enter_shard = 0, leave_shard = NSHARDS - 1;   /* migrate between them */
    unsigned tok = enter_epoch(enter_shard);
    if (tok != (unsigned)-1) {
        /* Traverse the link UNDER protection. A null means the reclaimer unlinked before we looked,
           so there is nothing to reach and nothing to protect -- that reader is trivially safe and
           must not assert. Only a reader that got a live pointer is making a claim about EBR. */
        Obj *p = atomic_load_explicit(&g_ptr, memory_order_seq_cst);
        if (p) {
            /* THE ACCESS. Everything above exists so that this is safe. */
            assert(atomic_load_explicit(&g_freed, memory_order_seq_cst) == 0);
        }
        leave_epoch(tok, leave_shard);
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
    if (ring_total(SLOT(next)) != 0) return 0;
#endif

    return atomic_compare_exchange_strong_explicit(
        &g_epoch, &e, next, memory_order_seq_cst, memory_order_seq_cst);
}

static void *reclaimer(void *arg) {
    (void)arg;

    /* UNLINK FIRST, THEN RETIRE. The order is what makes "retired at epoch r" mean anything: after
       the unlink no reader can newly reach the object, so only readers already inside epoch r or
       earlier can still hold it. Retiring before unlinking would be a different (and wrong)
       protocol. */
    atomic_store_explicit(&g_ptr, (Obj *)0, memory_order_seq_cst);
    unsigned r = atomic_load_explicit(&g_epoch, memory_order_seq_cst);

    for (int i = 0; i < MAX_ADV; ++i) try_advance();

    /* THE RECLAIM CONDITION: free once no reader remains in any epoch <= r. Readers in LATER epochs
       are irrelevant -- they entered after the unlink and cannot hold this object -- and that is
       precisely the distinction the ring makes hard, because slot k alone does not say which epoch
       it stands for.
       .
       THE GATE IS WHAT MAKES IT ANSWERABLE. Because advancement refuses to enter an occupied slot,
       at most NSLOTS epochs are live at once, so each slot corresponds to exactly ONE epoch in
       [cur - NSLOTS + 1, cur]. Recovering that epoch is what lets a nonzero counter be attributed.
       Remove the gate (-DNO_ADVANCE_GATE) and this attribution is a lie: slot k may be held by a
       reader NSLOTS epochs older than the one computed here. */
    unsigned cur = atomic_load_explicit(&g_epoch, memory_order_seq_cst);
    int blocked = 0;
    for (unsigned k = 0; k < NSLOTS; ++k) {
        /* the unique live epoch e with SLOT(e) == k */
        unsigned e = cur - ((cur - k) & (NSLOTS - 1));
        if (e <= r && ring_total(k) != 0) blocked = 1;
    }
    if (!blocked)
        atomic_store_explicit(&g_freed, 1, memory_order_seq_cst);

    return 0;
}

int main(void) {
    pthread_t tr, tr2, tc;
    pthread_create(&tr,  0, reader,    0);
#ifdef TWO_READERS
    pthread_create(&tr2, 0, reader,    0);
#endif
    pthread_create(&tc,  0, reclaimer, 0);
    pthread_join(tr, 0);
#ifdef TWO_READERS
    pthread_join(tr2, 0);
#endif
    pthread_join(tc, 0);
    return 0;
}
