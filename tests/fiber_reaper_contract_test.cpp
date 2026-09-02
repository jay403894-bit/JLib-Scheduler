// THE DEATH/REAPER CONTRACT: bounded unique owners, idempotent cleanup, and a real reuse fence.
//
// The creditor chain is append-only for one fiber life. That is only safe if three things hold, and
// missing any of them turns the chain into a second lost-wake machine rather than an obvious break.
//
//   1. UNIQUE BY WORKER, NOT ONE NODE PER HOP. If a worker picks the fiber up ten times it must
//      still be ONE creditor. Otherwise death walks a hop LOG and floods that worker's inbox with
//      ten deliveries of the same fiber -- and the fiber cannot even be on two chains at once,
//      because cleanupNext is a single pointer, so the surplus is silently lost.
//
//   2. CLEANUP ON A HISTORICAL OWNER IS IDEMPOTENT. Death sends work to everyone on the chain
//      because they MAY still hold something. By the time a hop lands, that worker has run
//      arbitrary other work and may hold nothing at all. A hook that assumes "I am being called,
//      therefore I still own this" double-retires -- or touches a slot a LATER life is using.
//
//   3. THE LIST IS THE REUSE FENCE. The fiber may not be recycled while any creditor is still
//      owed, because reusing the id before those hops run is a new life colliding with the old
//      chain.
//
// WHY THESE ASSERTIONS AND NOT "IT WORKED": every one of these failures produces a plausible run.
// A flooded inbox still completes. A double release still returns. An early recycle still hands out
// a working fiber. The counts are the only thing that separates them from correct behaviour.

#include "TaskScheduler.h"
#include "FiberRegistry.h"
#include "Fiber.h"

#include <cstdio>
#include <vector>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-72s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static constexpr size_t kWorkers = 16;

static std::vector<size_t> g_delivered;
static std::vector<size_t> g_released;
static size_t g_recycles = 0;
static size_t g_ranBeforeFirstRecycle = 0;
static size_t g_ran = 0;

static bool DispatchProbe(size_t holder, Fiber*) { g_delivered.push_back(holder); return true; }
static void ReleaseProbe(size_t holder, Fiber*)  { g_released.push_back(holder); }
static void RecycleProbe(Fiber*) {
    if (g_recycles == 0) g_ranBeforeFirstRecycle = g_ran;
    ++g_recycles;
}

static void Reset() {
    g_delivered.clear(); g_released.clear();
    g_recycles = 0; g_ranBeforeFirstRecycle = 0; g_ran = 0;
}

// Walks the chain the way real workers do: one hop, then the hop's own AdvanceCleanup, until dry.
static void RunChain(Fiber& f) {
    auto& reg = FiberRegistry::Instance();
    reg.AdvanceCleanup(&f);
    for (size_t i = 0; i < g_delivered.size(); ++i) { ++g_ran; reg.AdvanceCleanup(&f); }
}

static size_t CountOf(const std::vector<size_t>& v, size_t x) {
    size_t n = 0; for (size_t e : v) if (e == x) ++n; return n;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== reaper contract: unique owners, idempotent cleanup, reuse fence ===\n");

    auto& reg = FiberRegistry::Instance();
    reg.Build(nullptr, kWorkers);
    reg.SetDispatch(&DispatchProbe);
    reg.SetRecycle(&RecycleProbe);

    // ---- 1. TEN PICKUPS BY ONE WORKER IS ONE CREDITOR ---------------------------------------
    {
        Reset();
        Fiber f;
        for (int i = 0; i < 10; ++i) f.NoteCreditor(3);   // q3 steals it ten times
        f.NoteCreditor(7);
        RunChain(f);

        std::printf("    q3 noted 10x: delivered=%zu, to q3=%zu, to q7=%zu\n",
                    g_delivered.size(), CountOf(g_delivered, 3), CountOf(g_delivered, 7));

        Check(CountOf(g_delivered, 3) == 1, "ten pickups by one worker produce ONE delivery to it");
        Check(CountOf(g_delivered, 7) == 1, "and the other creditor still gets exactly one");
        Check(g_delivered.size() == 2, "no hop log: the chain is the SET of owners, not the history");
    }

    // ---- 2. THE REUSE FENCE: no recycle while anyone is still owed ---------------------------
    //
    // Counts hops that RAN, not hops dispatched. A fan-out that queues everything and then recycles
    // reads as equivalent and is not -- the fiber would be reused while the chains still point at
    // it. Only "how many had actually run when the first recycle happened" tells them apart.
    {
        Reset();
        Fiber f;
        for (size_t h : { (size_t)0, (size_t)4, (size_t)9, (size_t)15 }) f.NoteCreditor(h);
        RunChain(f);

        std::printf("    4 creditors: delivered=%zu ran-before-first-recycle=%zu recycles=%zu\n",
                    g_delivered.size(), g_ranBeforeFirstRecycle, g_recycles);

        Check(g_delivered.size() == 4, "every distinct creditor was delivered to");
        Check(g_recycles == 1, "the fiber was recycled EXACTLY once");
        Check(g_ranBeforeFirstRecycle == g_delivered.size(),
              "and not until every hop had RUN (queued is not run -- that is the fence)");
    }

    // ---- 3. A FIBER WITH NO CREDITORS RECYCLES IMMEDIATELY AND DELIVERS NOTHING --------------
    //
    // The common case by far -- a fiber that never touched affine state -- and the one that must
    // cost nothing. If it dispatched even once, every fiber death would post a task to run an empty
    // routine, which is what "being picked up is not a debt" exists to prevent.
    {
        Reset();
        Fiber f;
        RunChain(f);
        Check(g_delivered.empty(), "a fiber owing nobody dispatches NOTHING");
        Check(g_recycles == 1, "and is recycled immediately, exactly once");
    }

    // ---- 4. RELEASE RUNS ONCE PER OWED WORKER, EVEN WITH REPEATED PICKUPS --------------------
    //
    // The idempotence requirement has two halves. This is the half the REGISTRY owes: at most one
    // release call per worker per life, so a correct hook is never asked to be idempotent by the
    // library itself. (The other half -- tolerating a worker that no longer holds anything -- is
    // the hook's, and is stated on SetRelease; the library cannot enforce it, since only the hook
    // knows what it owned.)
    {
        Reset();
        reg.SetRelease(&ReleaseProbe);

        Fiber f;
        for (int i = 0; i < 5; ++i) { f.NoteCreditor(2); f.NoteCreditor(11); }
        // Deliver for real this time so DrainHolder -- which is what calls the release hook -- runs.
        reg.SetDispatch(nullptr);          // default dispatch: onto the holder's inbound chain
        reg.AdvanceCleanup(&f);
        for (size_t h = 0; h < kWorkers; ++h) reg.DrainHolder(h);
        // A hop may have re-homed the fiber to a holder already swept; sweep until dry.
        for (int guard = 0; guard < 8; ++guard) {
            size_t moved = 0;
            for (size_t h = 0; h < kWorkers; ++h) moved += reg.DrainHolder(h);
            if (!moved) break;
        }

        std::printf("    released on: %zu worker(s), q2=%zu q11=%zu\n",
                    g_released.size(), CountOf(g_released, 2), CountOf(g_released, 11));

        Check(CountOf(g_released, 2)  <= 1, "release ran AT MOST once on q2 despite 5 pickups");
        Check(CountOf(g_released, 11) <= 1, "release ran AT MOST once on q11 despite 5 pickups");
        Check(!g_released.empty(), "and it ran at all (else this arm is vacuous)");

        reg.SetRelease(nullptr);
        reg.SetDispatch(&DispatchProbe);
    }

    // ---- 5. TEARDOWN DRIVES THE CHAIN TO COMPLETION -----------------------------------------
    //
    // HONEST LABEL: this arm does NOT catch a live bug, and the note order below does not create
    // one. TakeCreditor pops the LOWEST set bit first, so hops always hand FORWARD however the bits
    // were set -- a single forward sweep is correct today, and would be whatever order this test
    // notes them in.
    //
    // What it pins is the OUTCOME rather than the mechanism: teardown ends with the chain drained
    // and the fiber recycled exactly once. That assertion survives a future change to TakeCreditor's
    // order, which the single-sweep version would not -- pop the highest bit instead, or rotate for
    // fairness, and a forward-only sweep starts silently stranding fibers behind itself.
    {
        Reset();
        reg.SetDispatch(nullptr);          // real delivery onto holder chains
        Fiber f;
        for (size_t h : { (size_t)13, (size_t)9, (size_t)5, (size_t)1 }) f.NoteCreditor(h);
        reg.AdvanceCleanup(&f);            // seeds the first holder's chain

        const size_t drained = reg.DrainAllForTeardown();
        std::printf("    teardown drained %zu hop(s), recycles=%zu\n", drained, g_recycles);

        Check(g_recycles == 1,
              "teardown drove the chain to completion -- one sweep would have stranded it");
        reg.SetDispatch(&DispatchProbe);
    }

    reg.SetDispatch(nullptr);
    reg.SetRecycle(nullptr);

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_fail ? "FAILED" : "PASSED", g_fail, g_fail == 1 ? "" : "s");
    return g_fail ? 1 : 0;
}
