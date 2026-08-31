// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// ParallelFor must work from a bare thread AND from inside a Fiber task, with Native leaves.
//
// WHAT THIS ORIGINALLY PINNED, and why it still earns its place. Mode::FiberOnly used to reject
// TaskType::Native at the push gate and std::abort(), which made ParallelFor unusable under it --
// not degraded, aborted -- because its split leaves are Native by design: an untaken split is taken
// back and run inline for ~11 ns, and a leaf that cheap cannot pay for a ContextSwitch plus a slab
// fiber. The mode is gone (4.0.2, once admitting Native left it identical to Default), but the
// property it was hiding is permanent and worth a test: **a suspendable task may publish
// non-suspendable work, and both run on the same pool.** Any future gate that decides what may be
// pushed has to keep that true.
//
// WHY THE ASSERTIONS ARE SHAPED THIS WAY. "ParallelFor returned and the sum is right" is a weak
// check: it also passes when ParallelFor quietly ran the whole range SERIALLY on the calling thread,
// which is exactly what a broken parallel path degrades to. So this counts per-index visits
// (catching both a skipped sub-range and a double-executed one) AND requires that the work was
// actually spread across more than one thread. Without the second assertion the test would pass on
// a scheduler that had stopped dispatching entirely.

#include <TaskScheduler.h>
#include <Thread.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

namespace {

	constexpr int kN = 200'000;

	int g_failures = 0;

	void Check(bool ok, const char* what) {
		if (!ok) {
			std::printf("  FAIL: %s\n", what);
			++g_failures;
		} else {
			std::printf("  ok:   %s\n", what);
		}
	}

	// Per-index visit counts. A plain sum cannot tell "ran twice" from "ran once" when the body is
	// idempotent, and cannot localise a hole; this can do both.
	std::vector<std::atomic<int>> g_visits(kN);

	std::mutex              g_threadsMu;
	std::set<std::size_t>   g_threadsSeen;

	void NoteThread() {
		// Which OS thread ran this leaf. Worker index would be tidier, but a leaf can also be run
		// inline by the CALLER -- that is the whole point of speculative splitting -- and the caller
		// may be a bare thread with no worker index at all. The thread id covers both.
		const std::size_t id =
			std::hash<std::thread::id>{}(std::this_thread::get_id());
		std::lock_guard<std::mutex> lock(g_threadsMu);
		g_threadsSeen.insert(id);
	}

	void ResetCounters() {
		for (auto& v : g_visits) v.store(0, std::memory_order_relaxed);
		std::lock_guard<std::mutex> lock(g_threadsMu);
		g_threadsSeen.clear();
	}

	// Returns the number of indices NOT visited exactly once.
	int BadIndices() {
		int bad = 0;
		for (int i = 0; i < kN; ++i)
			if (g_visits[i].load(std::memory_order_relaxed) != 1) ++bad;
		return bad;
	}

	std::size_t ThreadsSeen() {
		std::lock_guard<std::mutex> lock(g_threadsMu);
		return g_threadsSeen.size();
	}

}  // namespace

int main() {
	std::printf("ParallelFor with Native leaves, from a bare thread and from a Fiber task\n");

	JLib::TaskScheduler::Init(0);
	auto& sched = JLib::TaskScheduler::Instance();

	// ---- 1. ParallelFor from the BARE main thread ------------------------------------------
	// Main publishes Native splits and helps drain them. This is the path that used to abort.
	{
		ResetCounters();
		sched.ParallelFor(0, kN, 1024, [](int lo, int hi) {
			NoteThread();
			for (int i = lo; i < hi; ++i)
				g_visits[i].fetch_add(1, std::memory_order_relaxed);
		});
		Check(BadIndices() == 0, "bare-thread ParallelFor visited every index exactly once");
		Check(ThreadsSeen() > 1, "bare-thread ParallelFor actually spread across threads");
	}

	// ---- 2. ParallelFor from inside a FIBER task -------------------------------------------
	// The mixed case, and the one the mode exists for: a suspendable task publishing
	// non-suspendable leaves -- the property named at the top of this file.
	{
		ResetCounters();
		JLib::WaitGroup wg;
		wg.n.store(1, std::memory_order_relaxed);
		JLib::Task* t = sched.CreateTask([&sched]() {
			sched.ParallelFor(0, kN, 1024, [](int lo, int hi) {
				NoteThread();
				for (int i = lo; i < hi; ++i)
					g_visits[i].fetch_add(1, std::memory_order_relaxed);
			});
		}, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
		t->waitGroup = &wg;
		sched.Push(t);
		sched.WaitFor(wg);

		Check(BadIndices() == 0, "ParallelFor from a Fiber task visited every index exactly once");
		Check(ThreadsSeen() > 1, "ParallelFor from a Fiber task actually spread across threads");
	}
	// ---- 3. A bare Native task runs on the pool --------------------------------------------
	// ---- 3. A bare Native task still runs under FiberOnly ----------------------------------
	// The narrowest statement of the change, with nothing else in the way: push one Native task
	// and see it run. This is the exact push that used to abort.
	{
		std::atomic<int> ran{ 0 };
		JLib::WaitGroup wg;
		wg.n.store(1, std::memory_order_relaxed);
		JLib::Task* t = sched.CreateTask([&ran]() { ran.fetch_add(1, std::memory_order_relaxed); });
		t->waitGroup = &wg;
		sched.Push(t);
		sched.WaitFor(wg);
		Check(ran.load(std::memory_order_relaxed) == 1, "a plain Native task runs on the pool");
	}

	sched.Join();

	std::printf("%s (%d failure%s)\n", g_failures ? "FAILED" : "PASSED",
	            g_failures, g_failures == 1 ? "" : "s");
	return g_failures ? 1 : 0;
}
