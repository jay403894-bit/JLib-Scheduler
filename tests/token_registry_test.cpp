// TOKEN REGISTRY -- THE STRUCTURE, IN ISOLATION.
//
// No pool, no scheduler, no workers. The registry's two seams (SetDispatch/SetRecycle) exist so
// this can drive the chain directly, and the reason is recorded rather than assumed: the last
// structure in this codebase tested THROUGH live plumbing (deque_grow_test) deadlocked in the
// plumbing and never reached its assertion. A structure test that cannot start is not a weaker
// test, it is no test.
//
// EVERY CLAIM HERE HAS ITS OWN NEGATIVE CONTROL, and each control breaks ONLY the structure -- never
// the expectation the assertion is compared against. That distinction is the whole game: an earlier
// control in this codebase "passed" because it skipped recording the expectation along with the
// thing being tested, so the census matched itself.
//
// Build a control with, e.g.:  cmake .. -DJLIBSCHED_TOKENREG_CTL=FANOUT
//
//   OVERLAP            reader-space ranges collide          -> the layout test must FAIL
//   NO_EXHAUST_GUARD   external claim wraps instead of      -> the distinctness test must FAIL
//                      refusing
//   FANOUT             chain delivers to every creditor     -> "a hop RAN before recycle" must FAIL
//                      then recycles
//   DROP_CREDITOR      one NoteCreditor is skipped          -> the creditor census must FAIL
//                      (expectation still recorded)

#include "../include/TokenRegistry.h"
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

static constexpr std::size_t kFibers  = 128;
static constexpr std::size_t kWorkers = 8;

// ---- 1. THE READER SPACE ---------------------------------------------------------------------
//
// Three kinds, one contiguous index. The claim is not "the bases look right" -- it is that every id
// in [0, Capacity()) classifies as EXACTLY ONE kind. A layout bug that shifts a base shows up as an
// id claimed by two kinds or by none, and counting per-kind totals would miss both.
static void TestReaderSpace() {
    auto& reg = TokenRegistry::Instance();
    reg.Build(kFibers, kWorkers);

    std::size_t nf = 0, nw = 0, nx = 0, overlap = 0, orphan = 0;
    for (std::size_t r = 0; r < reg.Capacity(); ++r) {
        const int kinds = (reg.IsFiberReader(r)    ? 1 : 0)
                        + (reg.IsWorkerReader(r)   ? 1 : 0)
                        + (reg.IsExternalReader(r) ? 1 : 0);
        if (kinds > 1) ++overlap;
        if (kinds == 0) ++orphan;
        if (reg.IsFiberReader(r))    ++nf;
        if (reg.IsWorkerReader(r))   ++nw;
        if (reg.IsExternalReader(r)) ++nx;
    }
    std::printf("    fibers=%zu workers=%zu external=%zu  overlap=%zu orphan=%zu  capacity=%zu\n",
                nf, nw, nx, overlap, orphan, reg.Capacity());

    Check(overlap == 0, "reader space: no id belongs to two kinds");
    Check(orphan  == 0, "reader space: no id belongs to none");
    Check(nf == kFibers && nw == kWorkers && nx == TokenRegistry::kExternalReaders,
          "reader space: each range is exactly its size");
    // A FIBER'S poolIndex IS ITS READER ID. If this ever needs arithmetic, the hottest lookup in
    // the system stopped being a subscript.
    Check(reg.FiberBase() == 0, "reader space: fibers are the prefix (poolIndex == reader id)");

    // Only THREADS can be creditors, so fibers must map to no holder at all. Getting this wrong
    // would let a fiber be billed for thread-affine cleanup it cannot possibly hold.
    Check(reg.HolderOfReader(0) == kMaxHolders,
          "holders: a fiber is NOT a holder (it cannot own thread-affine state)");
    Check(reg.HolderOfReader(reg.WorkerBase()) == 0,
          "holders: workers rebase to 0 so the creditor mask stays small");
    Check(reg.HolderOfReader(reg.ExternalBase()) == kWorkers,
          "holders: externals follow the workers contiguously");
}

// ---- 2. LAZY EXTERNAL CLAIM ------------------------------------------------------------------
//
// Claimed once, never released, and EXHAUSTION REFUSES RATHER THAN WRAPS. A reused reader id is the
// one failure that turns a leak into a use-after-free, so the 65th caller must get kNoReader.
static void TestExternalClaim() {
    auto& reg = TokenRegistry::Instance();
    reg.Build(kFibers, kWorkers);

    constexpr int kThreads = 16;
    constexpr int kPer     = 8;                 // 128 attempts against 64 slots
    std::vector<std::vector<std::size_t>> got(kThreads);
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; ++t)
        ts.emplace_back([&, t] {
            for (int i = 0; i < kPer; ++i) got[t].push_back(reg.ClaimExternal());
        });
    for (auto& th : ts) th.join();

    std::set<std::size_t> distinct;
    std::size_t refused = 0, dupes = 0, outOfRange = 0;
    for (auto& v : got)
        for (std::size_t id : v) {
            if (id == TokenRegistry::kNoReader) { ++refused; continue; }
            if (!reg.IsExternalReader(id)) ++outOfRange;
            if (!distinct.insert(id).second) ++dupes;
        }
    std::printf("    granted=%zu distinct=%zu refused=%zu dupes=%zu out-of-range=%zu\n",
                (std::size_t)(kThreads * kPer) - refused, distinct.size(), refused, dupes, outOfRange);

    Check(dupes == 0,      "external claim: no id handed out twice (a reused reader id is a UAF)");
    Check(outOfRange == 0, "external claim: every granted id lands in the external range");
    Check(distinct.size() == TokenRegistry::kExternalReaders,
          "external claim: exactly kExternalReaders granted");
    Check(refused == (std::size_t)(kThreads * kPer) - TokenRegistry::kExternalReaders,
          "external claim: every attempt past the last slot is REFUSED, not wrapped");
}

// ---- 3. THE CREDITOR SET ---------------------------------------------------------------------
//
// Membership, not a sequence: dedup by construction, and every holder noted comes back exactly
// once. The expectation is recorded BEFORE the structure is touched, so the DROP_CREDITOR control
// breaks only the structure.
static void TestCreditorSet() {
    DebtToken tok;
    std::set<std::size_t> expected;

    const std::size_t holders[] = { 0, 1, 7, 63, 64, 65, 127, 200, 383 };
    for (std::size_t h : holders) {
        expected.insert(h);                     // <-- recorded FIRST, and never skipped
#if defined(JLIB_TOKENREG_CTL_DROP_CREDITOR)
        if (h == 64) continue;                  // CONTROL: structure loses one, expectation keeps it
#endif
        tok.NoteCreditor(h);
        tok.NoteCreditor(h);                    // idempotent: a second OR must not add a second debt
        tok.NoteCreditor(h);
    }

    std::set<std::size_t> taken;
    std::size_t dupes = 0;
    for (;;) {
        const std::size_t h = tok.TakeCreditor();
        if (h == kMaxHolders) break;
        if (!taken.insert(h).second) ++dupes;
    }
    std::printf("    noted=%zu taken=%zu dupes=%zu\n", expected.size(), taken.size(), dupes);

    Check(taken == expected, "creditor set: every holder noted comes back exactly once");
    Check(dupes == 0,        "creditor set: deduplicated by construction (3 notes -> 1 debt)");
    Check(!tok.HasCreditors(), "creditor set: drained empty after the last take");

    // REFUSE, DO NOT WRAP. An out-of-range holder billed modulo the mask width would silently
    // charge a real, innocent thread.
    DebtToken t2;
    t2.NoteCreditor(kMaxHolders);
    t2.NoteCreditor(kMaxHolders + 7);
    Check(!t2.HasCreditors(), "creditor set: an out-of-range holder is REFUSED, not wrapped");
}

// ---- 4. THE CHAIN ----------------------------------------------------------------------------
//
// One hop at a time, and the token is recycled only by the hop that finds the set empty.
//
// THE ASSERTION IS ABOUT HOPS THAT *RAN*, NOT HOPS DISPATCHED, and that is the entire point. Fan-out
// dispatches everything before recycling, so a test that counted dispatches would pass under the
// control it exists to catch. This one runs each delivery and counts how many had run by the time
// the recycle happened.
static std::vector<std::size_t> g_delivered;
static std::size_t g_ran = 0;
static std::size_t g_ranAtRecycle = 0;
static std::size_t g_recycleCount = 0;
static bool        g_recycled = false;

static bool TestDispatch(std::size_t holder, DebtToken*) {
    g_delivered.push_back(holder);
    return true;
}
static void TestRecycle(DebtToken*) {
    // LATCH THE **FIRST** RECYCLE. Overwriting on every call is not a detail -- it is what made this
    // control pass while the structure was broken. Under fan-out the first call delivers all four
    // and recycles with nothing yet run; the four follow-up hops then each find an empty set,
    // recycle AGAIN, and overwrite the 0 with a 4. The assertion then reads a number produced by
    // the recovery rather than by the bug.
    //
    // The recycle COUNT is the second half, and it is the sharper signal: a token must be recycled
    // exactly ONCE. Fan-out recycles it five times -- which in a live system means handing a token
    // back while chains still reference it.
    if (g_recycleCount == 0) g_ranAtRecycle = g_ran;
    ++g_recycleCount;
    g_recycled = true;
}

static void RunChain(DebtToken& tok) {
    g_delivered.clear();
    g_ran = 0; g_ranAtRecycle = 0; g_recycleCount = 0; g_recycled = false;

    auto& reg = TokenRegistry::Instance();
    reg.SetDispatch(&TestDispatch);
    reg.SetRecycle(&TestRecycle);

    reg.AdvanceCleanup(&tok);                   // first hop
    // Drive the chain the way a holder would: run what was delivered, and each run advances.
    for (std::size_t i = 0; i < g_delivered.size(); ++i) {
        ++g_ran;                                // this hop RUNS here
        reg.AdvanceCleanup(&tok);               // ... and hands on to the next creditor
    }
    reg.SetDispatch(nullptr);
    reg.SetRecycle(nullptr);
}

static void TestChain() {
    // MANY CREDITORS.
    {
        DebtToken tok;
        for (std::size_t h : { 0, 3, 9, 70 }) tok.NoteCreditor(h);
        RunChain(tok);
        std::printf("    many: delivered=%zu ran=%zu ran-at-first-recycle=%zu recycles=%zu\n",
                    g_delivered.size(), g_ran, g_ranAtRecycle, g_recycleCount);
        Check(g_delivered.size() == 4, "chain: one delivery per creditor, no more");
        Check(g_recycled,              "chain: the token is recycled once the set drains");
        Check(g_ranAtRecycle == 4,
              "chain: EVERY hop had RUN before the FIRST recycle (fan-out queues, it does not run)");
        Check(g_recycleCount == 1,
              "chain: recycled EXACTLY once (a second recycle hands back a referenced token)");
    }

    // A SINGLE HOLDER IS A CHAIN OF ONE -- the same code path, not a special case. This is what
    // makes pinned and migratable one mechanism instead of two.
    {
        DebtToken tok;
        tok.NoteCreditor(2);
        RunChain(tok);
        std::printf("    one:  delivered=%zu ran=%zu ran-at-recycle=%zu recycled=%d\n",
                    g_delivered.size(), g_ran, g_ranAtRecycle, (int)g_recycled);
        Check(g_delivered.size() == 1 && g_ranAtRecycle == 1,
              "chain: a single holder is a chain of length 1 (same path, no branch)");
    }

    // NO CREDITORS -> straight to recycle, nothing dispatched.
    {
        DebtToken tok;
        RunChain(tok);
        Check(g_delivered.empty() && g_recycled,
              "chain: a token owing nobody recycles without a single delivery");
    }
}

// ---- 5. DELIVERY -----------------------------------------------------------------------------
//
// Many producers, one consumer, and NOTHING MAY BE LOST. A dropped cleanup is not a dropped task --
// it is a handle that is never given back, and nothing downstream would report it.
static void TestDelivery() {
    auto& reg = TokenRegistry::Instance();
    reg.Build(kFibers, kWorkers);

    constexpr int kProducers = 8;
    constexpr int kEach      = 256;
    std::vector<DebtToken> tokens(kProducers * kEach);

    std::vector<std::thread> ts;
    for (int p = 0; p < kProducers; ++p)
        ts.emplace_back([&, p] {
            for (int i = 0; i < kEach; ++i)
                reg.Deliver(/*holder*/ 0, &tokens[p * kEach + i]);
        });
    for (auto& th : ts) th.join();

    Check(reg.HolderHasWork(0), "delivery: the holder reports work before the drain");

    std::set<DebtToken*> seen;
    std::size_t walked = 0, dupes = 0;
    for (DebtToken* t = reg.TakeAll(0); t; ) {
        DebtToken* nxt = t->next;               // read BEFORE the token can be reused
        ++walked;
        if (!seen.insert(t).second) ++dupes;
        t = nxt;
    }
    std::printf("    delivered=%d walked=%zu distinct=%zu dupes=%zu\n",
                kProducers * kEach, walked, seen.size(), dupes);

    Check(walked == (std::size_t)(kProducers * kEach), "delivery: every token pushed is drained");
    Check(dupes == 0,                                  "delivery: no token appears twice in the chain");
    Check(!reg.HolderHasWork(0),      "delivery: one exchange takes the WHOLE chain, not one link");
    Check(reg.TakeAll(0) == nullptr,  "delivery: draining an empty holder yields null, not garbage");
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== token registry: the structure, in isolation ===\n");
#if defined(JLIB_TOKENREG_CTL_OVERLAP)
    std::printf("  [CONTROL: OVERLAP -- the reader-space test MUST fail]\n");
#elif defined(JLIB_TOKENREG_CTL_NO_EXHAUST_GUARD)
    std::printf("  [CONTROL: NO_EXHAUST_GUARD -- the external-claim test MUST fail]\n");
#elif defined(JLIB_TOKENREG_CTL_FANOUT)
    std::printf("  [CONTROL: FANOUT -- 'every hop RAN before recycle' MUST fail]\n");
#elif defined(JLIB_TOKENREG_CTL_DROP_CREDITOR)
    std::printf("  [CONTROL: DROP_CREDITOR -- the creditor census MUST fail]\n");
#endif

    std::printf("\n  -- reader space --\n");        TestReaderSpace();
    std::printf("\n  -- lazy external claim --\n"); TestExternalClaim();
    std::printf("\n  -- creditor set --\n");        TestCreditorSet();
    std::printf("\n  -- the cleanup chain --\n");   TestChain();
    std::printf("\n  -- delivery --\n");            TestDelivery();

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
