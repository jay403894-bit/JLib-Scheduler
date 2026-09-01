// THE CREDITOR SET -- THE STRUCTURE, IN ISOLATION.
//
// Fiber::creditors records every worker that owes thread-affine cleanup for a fiber. This drives
// NoteCreditor/TakeCreditor/HasCreditors/ClearCreditors directly against fibers the test owns, and
// never routes a task through the scheduler to reach them.
//
// WHY IN ISOLATION. The last structure that got a through-the-scheduler test (deque_grow_test)
// deadlocked in the plumbing and never reached its assertion, reporting nothing wrong while doing
// it. A structural claim needs a test that cannot fail for a structural reason other than the one
// being claimed.
//
// THE CLAIMS, in the order asserted:
//   1. Recording is idempotent -- a worker that touches a fiber N times is ONE creditor. This is
//      the property that makes a bitmask better than the linked list of ids it replaced.
//   2. Every recorded creditor is returned exactly once, and no unrecorded one ever is.
//   3. Concurrent drainers never hand the SAME creditor to two of them. This is the one that would
//      double-release a COM apartment, and it is why TakeCreditor CASes rather than fetch_and.
//   4. An out-of-range worker is REFUSED, not wrapped. A wrapped index silently bills the wrong
//      worker, which is worse than dropping it because it looks like it worked.
//   5. Pinned mode is this set with one member -- not a second mechanism.
//
// NEGATIVE CONTROLS, each compiled in by -DJLIB_FIBERREG_CTL=<name> and each expected to go RED.
// A control that PASSES means the assertion it guards does no work:
//
//   CTL_DROP_ONE        skip one NoteCreditor. Proves the census counts rather than passing on
//                       whatever it happens to find.
//   CTL_NO_DEDUP        count duplicate touches as separate creditors. Proves claim 1 is real and
//                       not vacuously true because nothing ever duplicates.
//   CTL_ACCEPT_OOR      assert an out-of-range worker IS recorded. Proves claim 4's refusal is
//                       reached rather than dead.
//   CTL_RACE_UNSAFE     drain with fetch_and instead of TakeCreditor's CAS. Proves claim 3 can
//                       actually observe a double hand-out.
//
// Run all four and see four failures before believing the green.

#include "../include/TaskScheduler.h"
#include "../include/Fiber.h"
#include <cstdio>
#include <vector>
#include <thread>
#include <atomic>
#include <set>

using namespace JLib;

static int g_failures = 0;

static void Check(bool cond, const char* what) {
	std::printf("  %-68s %s\n", what, cond ? "ok" : "FAIL");
	if (!cond) ++g_failures;
}

int main() {
	std::printf("=== fiber creditor set: structure in isolation ===\n");
	std::printf("kCreditorWords=%zu -> %zu addressable workers\n",
		Fiber::kCreditorWords, Fiber::kCreditorWords * 64);

	// ---- 1. Idempotence: N touches by one worker is ONE creditor -------------------------------
	{
		Fiber f;
		for (int i = 0; i < 50; ++i) {
#ifdef CTL_NO_DEDUP
			// CONTROL: a list-of-ids would append every touch. Emulate that by spreading the 50
			// touches across 50 DISTINCT workers, which is what "no dedup" costs you.
			f.NoteCreditor((size_t)i);
#else
			f.NoteCreditor(7);
#endif
		}
		size_t n = 0;
		while (f.TakeCreditor() != SIZE_MAX) if (++n > 64) break;
		Check(n == 1, "50 touches by one worker yield exactly ONE creditor");
		Check(!f.HasCreditors(), "the set is empty once drained");
	}

	// ---- 2. Census: every recorded creditor comes back, exactly once ---------------------------
	{
		Fiber f;
		std::set<size_t> recorded;
		for (size_t w = 0; w < 200; w += 3) {
			// THE EXPECTATION IS RECORDED FIRST, AND THE CONTROL BREAKS ONLY THE STRUCTURE. An
			// earlier spelling put `continue` above both lines, which dropped the creditor from the
			// reference set as well -- so the census still matched itself and the control PASSED.
			// A control that removes the evidence along with the fact proves nothing.
			recorded.insert(w);
#ifdef CTL_DROP_ONE
			if (w == 99) continue;              // CONTROL: expected, but never told to the fiber
#endif
			f.NoteCreditor(w);
		}
		std::set<size_t> returned;
		size_t guard = 0;
		for (size_t c = f.TakeCreditor(); c != SIZE_MAX; c = f.TakeCreditor()) {
			returned.insert(c);
			if (++guard > 512) break;           // bound: a broken drain must FAIL, not hang
		}
		std::printf("recorded %zu, returned %zu\n", recorded.size(), returned.size());
		Check(returned.size() == recorded.size(), "every recorded creditor is returned");
		Check(returned == recorded, "and no creditor that was never recorded appears");
		Check(f.TakeCreditor() == SIZE_MAX, "a drained set reports SIZE_MAX, not a stale bit");
	}

	// ---- 3. Concurrent drain: no creditor is handed to two drainers ----------------------------
	// This is the assertion that matters most. A creditor handed out twice means the same COM
	// apartment released twice, or the same handle closed twice -- corruption, not a stall.
	{
		constexpr size_t kWorkers = 250;
		constexpr int    kRounds  = 200;
		std::atomic<size_t> collisions{ 0 };
		std::atomic<size_t> total{ 0 };

		for (int round = 0; round < kRounds; ++round) {
			Fiber f;
			for (size_t w = 0; w < kWorkers; ++w) f.NoteCreditor(w);

			std::vector<std::vector<size_t>> got(4);
			std::vector<std::thread> ts;
			for (int t = 0; t < 4; ++t) {
				ts.emplace_back([&, t] {
					for (;;) {
#ifdef CTL_RACE_UNSAFE
						// CONTROL: read-then-clear without a CAS. Two drainers can read the same
						// word, both see the same lowest bit, and both claim it.
						size_t c = SIZE_MAX;
						for (size_t w = 0; w < Fiber::kCreditorWords; ++w) {
							uint64_t cur = f.creditors[w].load(std::memory_order_acquire);
							if (!cur) continue;
							const unsigned b = platform::CountTrailingZeros64(cur);
							f.creditors[w].fetch_and(~(1ull << b), std::memory_order_acq_rel);
							c = w * 64 + b;
							break;
						}
						if (c == SIZE_MAX) break;
#else
						const size_t c = f.TakeCreditor();
						if (c == SIZE_MAX) break;
#endif
						got[t].push_back(c);
					}
				});
			}
			for (auto& th : ts) th.join();

			std::set<size_t> seen;
			for (auto& v : got)
				for (size_t c : v) {
					total.fetch_add(1, std::memory_order_relaxed);
					if (!seen.insert(c).second)
						collisions.fetch_add(1, std::memory_order_relaxed);
				}
			if (seen.size() != kWorkers) collisions.fetch_add(1, std::memory_order_relaxed);
		}
		std::printf("concurrent drain: %zu handed out over %d rounds, %zu collisions\n",
			total.load(), kRounds, collisions.load());
		Check(collisions.load() == 0,
			"4 concurrent drainers never receive the same creditor twice");
		Check(total.load() == kWorkers * kRounds,
			"and between them they receive every creditor exactly once");
	}

	// ---- 4. Out of range is refused, not wrapped ----------------------------------------------
	{
		Fiber f;
		const size_t oor = Fiber::kCreditorWords * 64;      // one past the end
		f.NoteCreditor(oor);
#ifdef CTL_ACCEPT_OOR
		Check(f.HasCreditors(), "CONTROL: an out-of-range worker is recorded");
#else
		Check(!f.HasCreditors(), "an out-of-range worker is refused, not wrapped to worker 0");
#endif
		f.NoteCreditor(oor + 12345);
		Check(f.TakeCreditor() == SIZE_MAX, "and a far out-of-range index records nothing either");
	}

	// ---- 5. Pinned mode is the same set with one member ----------------------------------------
	// Not a separate mechanism and not a separate code path: bind adds one creditor, nothing else
	// ever does, and the cleanup chain is one hop. Asserted so that a future change which makes
	// pinned mode structurally different has to break this on the way past.
	{
		Fiber f;
		f.NoteCreditor(3);                       // "bound to worker 3"
		Check(f.HasCreditors(), "a pinned fiber owes exactly its binding worker");
		Check(f.TakeCreditor() == 3, "the chain's first hop is that worker");
		Check(f.TakeCreditor() == SIZE_MAX, "and the chain ends immediately -- one hop, then recycle");
	}

	// ---- 6. ClearCreditors, for the recycle path ----------------------------------------------
	{
		Fiber f;
		for (size_t w = 0; w < 100; ++w) f.NoteCreditor(w);
		f.ClearCreditors();
		Check(!f.HasCreditors(), "ClearCreditors empties the set (a recycled fiber owes nobody)");
		Check(f.TakeCreditor() == SIZE_MAX, "and nothing is left to hand out");
	}

	std::printf("=== %s (%d failure%s) ===\n",
		g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
	return g_failures ? 1 : 0;
}
