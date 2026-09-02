// THE HOLDER SIDE OF FiberRegistry -- THE STRUCTURE, IN ISOLATION.
//
// No pool, no scheduler, no workers. The registry's seams exist so this can drive the chain
// directly, and the reason is recorded rather than assumed: the last structure tested THROUGH live
// plumbing (deque_grow_test) deadlocked in the plumbing and never reached its assertion.
//
// WHAT THIS GUARDS IS THE TWO-SPACE SEPARATION. `table` is indexed by Fiber::poolIndex (debtors);
// `inbound` is indexed by holder (threads). Thread 3 is not fiber 3, and an aliasing bug between
// them would be silent -- a thread slotting itself in at a fiber's index and releasing that fiber's
// debts. So the accessors are named for their space, and this asserts the spaces stay disjoint.
//
// EVERY CLAIM HAS ITS OWN NEGATIVE CONTROL, and each control breaks ONLY the structure -- never the
// expectation it is compared against. An earlier control in this codebase "passed" because it
// skipped recording the expectation along with the thing being tested, so the census matched itself.
//
//   cmake -S . -B build-ctl -DJLIBSCHED_FIBERHOLDER_CTL=FANOUT
//
//   NO_EXHAUST_GUARD   external claim wraps instead of refusing  -> distinctness MUST fail
//   FANOUT             chain delivers to every creditor, then    -> "a hop RAN before the first
//                      recycles                                     recycle" MUST fail
//   DROP_CREDITOR      one NoteCreditor skipped (expectation      -> the creditor census MUST fail
//                      still recorded)

#include "../include/FiberRegistry.h"
#include <cstdio>
#include <atomic>
#include <thread>
#include <vector>
#include <set>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-70s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

static constexpr size_t kWorkers = 8;

// ---- 1. THE HOLDER SPACE ----------------------------------------------------------------------
static void TestHolderSpace() {
    auto& reg = FiberRegistry::Instance();
    reg.Build(nullptr, kWorkers);       // no pool: the holder side does not need one

    Check(reg.HolderCount() == kWorkers + FiberRegistry::kExternalReaders,
          "holder space: workers followed by the external ids, nothing else");

    size_t workerHolders = 0;
    for (size_t h = 0; h < reg.HolderCount(); ++h)
        if (reg.IsWorkerHolder(h)) ++workerHolders;
    Check(workerHolders == kWorkers, "holder space: exactly the workers are worker-holders");
    Check(reg.HolderOfWorker(0) == 0 && reg.HolderOfWorker(kWorkers - 1) == kWorkers - 1,
          "holder space: workers are the prefix, so a worker id IS its holder id");

    // THE CREDITOR MASK MUST REACH THE LAST EXTERNAL HOLDER. At 4 words this covered 256 and
    // silently refused every external id above it -- NoteCreditor drops an out-of-range holder
    // rather than wrapping, which is right, but the debt goes with it.
    Fiber f;
    const size_t last = reg.HolderCount() - 1;
    f.NoteCreditor(last);
    Check(f.HasCreditors(), "creditor mask reaches the LAST holder, not just the last worker");
    Check(f.TakeCreditor() == last, "and gives that exact holder back");
}

// ---- 2. LAZY EXTERNAL CLAIM -------------------------------------------------------------------
static void TestExternalClaim() {
    auto& reg = FiberRegistry::Instance();
    reg.Build(nullptr, kWorkers);

    constexpr int kThreads = 16, kPer = 8;          // 128 attempts against 64 slots
    std::vector<std::vector<size_t>> got(kThreads);
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&, t] { for (int i = 0; i < kPer; ++i) got[t].push_back(reg.ClaimExternal()); });
    for (auto& th : ts) th.join();

    std::set<size_t> distinct;
    size_t refused = 0, dupes = 0, aliasedWorker = 0;
    for (auto& v : got)
        for (size_t id : v) {
            if (id == FiberRegistry::kNoHolder) { ++refused; continue; }
            // THE ALIASING CHECK. An external id landing inside the worker range would give a bare
            // thread a worker's chain -- the exact space confusion this file exists to catch.
            if (reg.IsWorkerHolder(id)) ++aliasedWorker;
            if (!distinct.insert(id).second) ++dupes;
        }
    std::printf("    granted=%zu distinct=%zu refused=%zu dupes=%zu aliased-onto-worker=%zu\n",
                (size_t)(kThreads * kPer) - refused, distinct.size(), refused, dupes, aliasedWorker);

    Check(dupes == 0,         "external claim: no id handed out twice (a reused holder id is a UAF)");
    Check(aliasedWorker == 0, "external claim: no external id lands in the WORKER range");
    Check(distinct.size() == FiberRegistry::kExternalReaders,
          "external claim: exactly kExternalReaders granted");
    Check(refused == (size_t)(kThreads * kPer) - FiberRegistry::kExternalReaders,
          "external claim: every attempt past the last slot is REFUSED, not wrapped");
}

// ---- 3. THE CREDITOR SET ----------------------------------------------------------------------
static void TestCreditorSet() {
    Fiber f;
    std::set<size_t> expected;
    const size_t holders[] = { 0, 1, 7, 63, 64, 65, 127, 200, 320 };
    for (size_t h : holders) {
        expected.insert(h);                          // recorded FIRST, and never skipped
#if defined(JLIB_FIBERHOLDER_CTL_DROP_CREDITOR)
        if (h == 64) continue;                       // CONTROL: structure loses one, expectation keeps it
#endif
        f.NoteCreditor(h); f.NoteCreditor(h); f.NoteCreditor(h);   // idempotent
    }
    std::set<size_t> taken;
    size_t dupes = 0;
    for (;;) {
        const size_t h = f.TakeCreditor();
        if (h == SIZE_MAX) break;
        if (!taken.insert(h).second) ++dupes;
    }
    std::printf("    noted=%zu taken=%zu dupes=%zu\n", expected.size(), taken.size(), dupes);
    Check(taken == expected,  "creditor set: every holder noted comes back exactly once");
    Check(dupes == 0,         "creditor set: deduplicated by construction (3 notes -> 1 debt)");
    Check(!f.HasCreditors(),  "creditor set: drained empty after the last take");

    Fiber g;
    g.NoteCreditor(Fiber::kCreditorWords * 64);      // one past the end
    Check(!g.HasCreditors(),  "creditor set: an out-of-range holder is REFUSED, not wrapped");
}

// ---- 4. THE CHAIN -----------------------------------------------------------------------------
//
// THE ASSERTION IS ABOUT HOPS THAT *RAN*, NOT HOPS DISPATCHED. Fan-out dispatches everything before
// recycling, so a test counting dispatches would pass under the control it exists to catch. And the
// recycle hook LATCHES the first recycle -- overwriting on every call is what made this control pass
// while the structure was broken, because the recovery hops overwrote the 0 the bug produced.
static std::vector<size_t> g_delivered;
static size_t g_ran = 0, g_ranAtRecycle = 0, g_recycleCount = 0;

static bool TestDispatch(size_t holder, Fiber*) { g_delivered.push_back(holder); return true; }
static void TestRecycle(Fiber*) {
    if (g_recycleCount == 0) g_ranAtRecycle = g_ran;
    ++g_recycleCount;
}

static void RunChain(Fiber& f) {
    g_delivered.clear(); g_ran = 0; g_ranAtRecycle = 0; g_recycleCount = 0;
    auto& reg = FiberRegistry::Instance();
    reg.SetDispatch(&TestDispatch);
    reg.SetRecycle(&TestRecycle);
    reg.AdvanceCleanup(&f);
    for (size_t i = 0; i < g_delivered.size(); ++i) { ++g_ran; reg.AdvanceCleanup(&f); }
    reg.SetDispatch(nullptr);
    reg.SetRecycle(nullptr);
}

static void TestChain() {
    {
        Fiber f;
        for (size_t h : { 0u, 3u, 9u, 70u }) f.NoteCreditor(h);
        RunChain(f);
        std::printf("    many: delivered=%zu ran=%zu ran-at-first-recycle=%zu recycles=%zu\n",
                    g_delivered.size(), g_ran, g_ranAtRecycle, g_recycleCount);
        Check(g_delivered.size() == 4, "chain: one delivery per creditor, no more");
        Check(g_ranAtRecycle == 4,
              "chain: EVERY hop had RUN before the FIRST recycle (fan-out queues, it does not run)");
        Check(g_recycleCount == 1,
              "chain: recycled EXACTLY once (a second recycle hands back a referenced fiber)");
    }
    {
        Fiber f;
        f.NoteCreditor(2);
        RunChain(f);
        Check(g_delivered.size() == 1 && g_ranAtRecycle == 1 && g_recycleCount == 1,
              "chain: a single holder is a chain of length 1 (same path, no branch)");
    }
    {
        Fiber f;
        RunChain(f);
        Check(g_delivered.empty() && g_recycleCount == 1,
              "chain: a fiber owing nobody recycles without a single delivery");
    }
}

// ---- 5. DELIVERY ------------------------------------------------------------------------------
static void TestDelivery() {
    auto& reg = FiberRegistry::Instance();
    reg.Build(nullptr, kWorkers);

    constexpr int kProducers = 8, kEach = 256;
    std::vector<Fiber> fibers(kProducers * kEach);
    std::vector<std::thread> ts;
    for (int p = 0; p < kProducers; ++p)
        ts.emplace_back([&, p] {
            for (int i = 0; i < kEach; ++i) reg.Deliver(/*holder*/ 0, &fibers[p * kEach + i]);
        });
    for (auto& th : ts) th.join();

    Check(reg.HolderHasWork(0), "delivery: the holder reports work before the drain");

    std::set<Fiber*> seen;
    size_t walked = 0, dupes = 0;
    for (Fiber* f = reg.TakeAll(0); f; ) {
        Fiber* nxt = f->cleanupNext;               // read BEFORE the fiber can be reused
        ++walked;
        if (!seen.insert(f).second) ++dupes;
        f = nxt;
    }
    std::printf("    delivered=%d walked=%zu distinct=%zu dupes=%zu\n",
                kProducers * kEach, walked, seen.size(), dupes);
    Check(walked == (size_t)(kProducers * kEach), "delivery: every fiber pushed is drained");
    Check(dupes == 0,                             "delivery: no fiber appears twice in the chain");
    Check(!reg.HolderHasWork(0),   "delivery: one exchange takes the WHOLE chain, not one link");
    Check(reg.TakeAll(0) == nullptr, "delivery: draining an empty holder yields null, not garbage");
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== FiberRegistry holder side: the structure, in isolation ===\n");
#if defined(JLIB_FIBERHOLDER_CTL_NO_EXHAUST_GUARD)
    std::printf("  [CONTROL: NO_EXHAUST_GUARD -- the external-claim test MUST fail]\n");
#elif defined(JLIB_FIBERHOLDER_CTL_FANOUT)
    std::printf("  [CONTROL: FANOUT -- 'every hop RAN before recycle' MUST fail]\n");
#elif defined(JLIB_FIBERHOLDER_CTL_DROP_CREDITOR)
    std::printf("  [CONTROL: DROP_CREDITOR -- the creditor census MUST fail]\n");
#endif
    std::printf("\n  -- holder space --\n");        TestHolderSpace();
    std::printf("\n  -- lazy external claim --\n"); TestExternalClaim();
    std::printf("\n  -- creditor set --\n");        TestCreditorSet();
    std::printf("\n  -- the cleanup chain --\n");   TestChain();
    std::printf("\n  -- delivery --\n");            TestDelivery();

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
