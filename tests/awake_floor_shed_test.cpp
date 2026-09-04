// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// The awake floor must GROW under a wave and SHED back to its base when the pool goes idle.
//
// WHY THIS TEST EXISTS. Growth and shedding were built at different times and only growth was ever
// checked, so shedding failed silently for a whole benchmark run: a 16-task wave grew the floor
// 2 -> 8 and it was still 8 during the serial latency row that followed. Nothing failed. The row
// simply measured a pool with eight spinners in it and reported worse numbers -- 1p 10.0 -> 5.74
// M/s, p50 0.40 -> 0.90 us -- which reads as a latency regression rather than as a controller that
// never shed. A one-way controller looks exactly like a slow scheduler.
//
// THE TWO ASSERTIONS ARE A PAIR AND NEITHER IS SUFFICIENT ALONE:
//   - shed without growth passes trivially on a scheduler where growth is broken (the floor never
//     left the base, so of course it is at the base afterwards)
//   - growth without shed is the bug that shipped
// So this asserts the floor RISES above base under load, and RETURNS to base once idle. The first
// assertion is the negative control for the second.

#include <TaskScheduler.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

namespace {

	int g_failures = 0;

	void Check(bool ok, const char* what) {
		std::printf("  %-58s %s\n", what, ok ? "ok" : "FAIL");
		if (!ok) ++g_failures;
	}

	// Long enough that the floor workers are still inside a body while later pushes arrive -- that
	// overlap is the whole condition growth reacts to. A no-op wave would drain as fast as it is
	// pushed and would never crowd anything.
	void HeavyBody(void*) {
		volatile double x = 0.0;
		const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(3);
		while (std::chrono::steady_clock::now() < until) x += 1.0;
		(void)x;
	}

}  // namespace

int main() {
	std::printf("Awake floor: grows under a wave, sheds back to base when idle\n");

	JLib::TaskScheduler::SetAwakeFloor(2);
	JLib::TaskScheduler::Init(0);
	auto& sched = JLib::TaskScheduler::Instance();

	// ---- THE FLOOR CANNOT GROW PAST THE POOL, so a narrow machine cannot host this test -----
	//
	// The whole claim below is "the floor rises ABOVE its base of 2 under load and sheds back".
	// On a two-core GitHub runner the pool is two workers wide, so there is nowhere above 2 to
	// grow into and the growth assertion fails every run -- not because the controller is broken
	// but because the machine cannot express the thing being asserted.
	//
	// SKIPPED (77) rather than failed, for the reason io_tiny_stack_test.cpp states at length: a
	// check that is permanently red is a check everybody learns to skim past. Needs a worker for
	// the base floor plus at least one to grow into, plus the main thread's own core to not be
	// fighting them -- 4 is the smallest honest bar.
	const size_t nWorkers = sched.GetWorkerCount();
	if (nWorkers < 4) {
		std::printf("  pool is %zu workers; the floor base is 2, so there is no headroom to grow into.\n"
		            "  SKIPPED: needs a pool of at least 4.\n", nWorkers);
		return 77;
	}

	const size_t base = JLib::TaskScheduler::GetAwakeFloorBase();
	Check(base == 2, "base floor is what was configured (2)");

	// Let startup settle so we are not reading a floor that the warmup is still moving.
	std::this_thread::sleep_for(std::chrono::milliseconds(50));
	Check(JLib::TaskScheduler::GetAwakeFloor() == base, "floor sits at base on an idle pool");

	// ---- GROW: a wave of long tasks from an idle pool --------------------------------------
	size_t floorDuringWave = 0;
	{
		constexpr int kWave = 16;
		// THE PEAK, NOT A LIVE SAMPLE. The first version of this read GetAwakeFloor() straight
		// after the push loop and FLAKED on Linux -- growth needs a few pushes to build depth, and
		// the collapse can fire the instant the wave drains, so a live read can land before growth
		// or after the shed and fail either way. A high-water mark cannot be raced: it records that
		// growth happened regardless of when anyone looks.
		JLib::TaskScheduler::ResetAwakeFloorPeak();
		JLib::WaitGroup wg;
		wg.n.store(kWave, std::memory_order_relaxed);
		for (int i = 0; i < kWave; ++i) {
			JLib::Task* t = sched.CreateTask(HeavyBody, nullptr);
			t->waitGroup = &wg;
			sched.Push(t);
		}
		sched.WaitFor(wg);
		floorDuringWave = JLib::TaskScheduler::GetAwakeFloorPeak();
	}
	Check(floorDuringWave > base, "floor GREW above base during the wave (peak, not a live sample)");

	// ---- SHED: the pool goes idle ----------------------------------------------------------
	// The collapse is gated on a hold since the last growth, so this waits well past it. It is a
	// generous margin on purpose: this test should fail because the floor did not shed, never
	// because it had not shed YET.
	const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
	while (std::chrono::steady_clock::now() < deadline
	       && JLib::TaskScheduler::GetAwakeFloor() > base) {
		std::this_thread::sleep_for(std::chrono::milliseconds(5));
	}
	const size_t floorAfter = JLib::TaskScheduler::GetAwakeFloor();
	Check(floorAfter == base, "floor SHED back to base once the pool went idle");

	if (floorAfter != base)
		std::printf("     floor was %zu, base is %zu (grew to %zu during the wave)\n",
		            floorAfter, base, floorDuringWave);

	JLib::detail::TeardownForTesting(sched);
	std::printf("%s (%d failure%s)\n", g_failures ? "FAILED" : "PASSED",
	            g_failures, g_failures == 1 ? "" : "s");
	return g_failures ? 1 : 0;
}
