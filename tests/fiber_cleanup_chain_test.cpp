// THE CLEANUP CHAIN -- IN ISOLATION, THROUGH FiberRegistry's SEAMS.
//
// AdvanceCleanup takes ONE creditor, dispatches a cleanup hop to it, and recycles the fiber only
// when nobody is owed. This drives that loop with a test dispatch and a test recycler, so the
// assertion is about the CHAIN and not about whether a pool came up.
//
// WHY NOT THROUGH A LIVE SCHEDULER. The last structure tested that way (deque_grow_test) deadlocked
// in the plumbing and never reached its assertion, reporting nothing wrong while doing it. A chain
// whose whole claim is "recycle happens after the last hop, not before" needs a test that can
// observe the ORDER, which a real dispatch cannot give you without a race.
//
// THE CLAIMS:
//   1. Every creditor gets exactly one hop.
//   2. Recycle happens ONCE, and AFTER every hop -- the property that separates chain from fan-out.
//   3. A fiber owing nobody recycles immediately, with no hop. This is pinned mode's degenerate
//      case when the binding worker owed nothing.
//   4. A pinned fiber (one creditor) is one hop then recycle -- not a second code path.
//   5. A FAILED dispatch puts the creditor BACK. A dropped cleanup is a resource never returned,
//      so it must not be silently consumed by the attempt.
//
// NEGATIVE CONTROLS via -DJLIB_FIBERCHAIN_CTL=<name>, each expected RED:
//   CTL_FANOUT          recycle after dispatching, without waiting for the hops. This is the wrong
//                       design the header warns about, and claim 2 must catch it.
//   CTL_SWALLOW_FAILED  drop the creditor on a failed dispatch instead of restoring it. Claim 5.
//   CTL_SKIP_ONE        skip one creditor's hop. Claim 1.

#include "../include/FiberRegistry.h"
#include "../include/Fiber.h"
#include <cstdio>
#include <vector>
#include <set>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
	std::printf("  %-70s %s\n", what, c ? "ok" : "FAIL");
	if (!c) ++g_failures;
}

// ---- test seams ------------------------------------------------------------------------------
static std::vector<size_t> g_hops;        // creditors DISPATCHED to, in order
static size_t              g_completed = 0;      // hops that have actually RUN
static size_t              g_recycles = 0;
static size_t              g_doneAtFirstRecycle = SIZE_MAX;
static bool                g_failDispatch = false;
static Fiber*              g_pending = nullptr;   // fiber whose hop is "in flight"

// DISPATCHED IS NOT RUN, AND THAT DISTINCTION IS THE WHOLE TEST. An earlier version of this file
// recorded only dispatches and asked how many had been dispatched when recycle fired -- which
// fan-out satisfies exactly as well as the chain does, because fan-out dispatches everything first.
// CTL_FANOUT passed against that, which is how the gap was found. What separates the two designs is
// how many hops have EXECUTED at the moment the fiber goes back to the pool: all of them, or none.

static bool TestDispatch(size_t worker, Fiber* f) {
	if (g_failDispatch) return false;
#ifdef CTL_SKIP_ONE
	if (worker == 5) return true;   // CONTROL: claim delivery, deliver nothing
#endif
	g_hops.push_back(worker);
	g_pending = f;                  // the hop has NOT run yet -- the test runs it explicitly
	return true;
}

static void TestRecycle(Fiber*) {
	if (g_recycles == 0) g_doneAtFirstRecycle = g_completed;
	++g_recycles;
}

// A dispatched hop actually running. In production this is CleanupHop executing on the creditor,
// before it calls AdvanceCleanup for the next one.
static void CompletePendingHop() {
	if (g_pending) { ++g_completed; g_pending = nullptr; }
}

static void ResetSeams() {
	g_hops.clear(); g_completed = 0; g_recycles = 0; g_doneAtFirstRecycle = SIZE_MAX;
	g_failDispatch = false; g_pending = nullptr;
}

// Drive the chain the way the real dispatch would: each hop, once it has "run", advances again.
static void RunChain(FiberRegistry& reg, Fiber& f) {
#ifdef CTL_FANOUT
	// CONTROL: the wrong shape. Take every creditor and dispatch them all, THEN recycle -- without
	// any hop having run. This is what "priority makes it safe" would buy you, and it is why the
	// design says chain instead.
	for (size_t c = f.TakeCreditor(); c != SIZE_MAX; c = f.TakeCreditor())
		TestDispatch(c, &f);
	TestRecycle(&f);
	return;
#else
	while (reg.AdvanceCleanup(&f)) {
		// AdvanceCleanup dispatched a hop. In production that hop RUNS on the creditor and only
		// then calls AdvanceCleanup again; here this loop body is that execution. Recycle can be
		// reached only by falling out, i.e. after the last hop has run.
		CompletePendingHop();
	}
#endif
}

int main() {
	std::printf("=== fiber cleanup chain: in isolation ===\n");
	FiberRegistry& reg = FiberRegistry::Instance();
	reg.SetDispatch(&TestDispatch);
	reg.SetRecycle(&TestRecycle);

	// ---- 1 + 2. Every creditor gets one hop; recycle is once, and last --------------------------
	{
		ResetSeams();
		Fiber f;
		std::set<size_t> owed;
		for (size_t w = 0; w < 40; w += 5) { f.NoteCreditor(w); owed.insert(w); }
		RunChain(reg, f);

		std::set<size_t> hopped(g_hops.begin(), g_hops.end());
		std::printf("owed %zu, dispatched %zu, RAN %zu, recycles %zu, ran-at-first-recycle %zu\n",
			owed.size(), g_hops.size(), g_completed, g_recycles, g_doneAtFirstRecycle);
		Check(g_hops.size() == owed.size(), "one hop per creditor, no more and no fewer");
		Check(hopped == owed, "and each hop goes to a creditor that was actually owed");
		Check(g_recycles == 1, "the fiber is recycled exactly once");
		Check(g_doneAtFirstRecycle == owed.size(),
			"recycle happens after every hop has RUN (chain, not fan-out)");
		Check(!f.HasCreditors(), "and the creditor set is empty afterwards");
	}

	// ---- 3. Owing nobody recycles immediately --------------------------------------------------
	{
		ResetSeams();
		Fiber f;
		const bool more = reg.AdvanceCleanup(&f);
		Check(!more, "a fiber owing nobody reports the chain finished");
		Check(g_hops.empty(), "with no hop dispatched");
		Check(g_recycles == 1, "and is recycled straight away");
	}

	// ---- 4. Pinned mode: one creditor, one hop, then recycle -----------------------------------
	{
		ResetSeams();
		Fiber f;
		f.NoteCreditor(11);                  // "bound to worker 11", nothing else ever added
		RunChain(reg, f);
		Check(g_hops.size() == 1 && g_hops[0] == 11, "a pinned fiber takes exactly one hop, to its worker");
		Check(g_recycles == 1 && g_doneAtFirstRecycle == 1, "then recycles -- same chain, length one");
	}

	// ---- 5. A failed dispatch puts the creditor back -------------------------------------------
	// The creditor is already off the set when dispatch is attempted (TakeCreditor removed it), so
	// a failure that does not restore it loses the debt silently.
	{
		ResetSeams();
		Fiber f;
		f.NoteCreditor(9);
		g_failDispatch = true;
		const bool more = reg.AdvanceCleanup(&f);
		Check(!more, "a failed dispatch reports the chain did not advance");
		Check(g_recycles == 0, "and does NOT recycle a fiber that still owes");
#ifdef CTL_SWALLOW_FAILED
		Check(!f.HasCreditors(), "CONTROL: the creditor was consumed by the failed attempt");
#else
		Check(f.HasCreditors(), "the creditor is restored, so the debt can be retried");
#endif
		// And the retry works.
		g_failDispatch = false;
		RunChain(reg, f);
		Check(g_hops.size() == 1 && g_hops[0] == 9, "the retry delivers the same creditor");
		Check(g_recycles == 1, "and the chain then completes");
	}

	// ---- 6. Null is refused, not crashed -------------------------------------------------------
	{
		ResetSeams();
		Check(!reg.AdvanceCleanup(nullptr), "a null fiber is refused");
		Check(g_recycles == 0, "and nothing is recycled for it");
	}

	reg.SetDispatch(nullptr);
	reg.SetRecycle(nullptr);

	std::printf("=== %s (%d failure%s) ===\n",
		g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
	return g_failures ? 1 : 0;
}
