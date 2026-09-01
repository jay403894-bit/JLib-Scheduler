// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// CAS RETRY DISTRIBUTION -- MAX AND BUCKETS PER CALL, NOT RETRIES/CALLS.
//
// == WHY NOT AN AVERAGE ==
//
// "hit rate 72%" and "casLost=73 of 35,308,502" are averages, and an average cannot see the thing
// worth finding. A loop that retries twice on ten million calls and a loop that retries ten
// thousand times on one call report nearly the same ratio, and only one of them is a bug. The
// failure mode this exists for is an OUTLIER: one call that spins while everything else sails past.
//
// The same argument already applies to the timing rows -- SchedulerBench reports p50 0.60 us next
// to max 86.90 us, a 145x tail that a mean cannot express. This is that idea applied to contention
// instead of latency.
//
// == WHAT IT RECORDS ==
//
// Per site, per thread: call count, total retries, the MAXIMUM retries any single call took, and
// bucket counts at 8 / 64 / 512 / 4096. The buckets are what distinguish "steadily a bit contended"
// from "usually free, occasionally catastrophic" -- those want opposite fixes, and the max alone
// cannot tell you whether the peak was a one-off or a pattern.
//
// == COST ==
//
// OFF unless JLIBSCHED_RETRY_STATS is defined. This sits inside CAS loops, so a build carrying it
// is a DIAGNOSTIC BUILD and its timings are not quotable -- the same rule the steal counters carry.
// When on, the counters live in per-thread cache-line-padded cells and are only ever written by
// their owning thread, so the instrument cannot itself become the contention it is measuring. That
// matters more here than usual: a shared max would need its own CAS loop.

#pragma once
#include "platform.h"
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace JLib {

	// Sites are named rather than numbered at the call site so a report reads without a lookup.
	// Add one here and at kRetrySiteNames below -- the static_assert keeps them in step.
	enum class RetrySite : unsigned {
		Bands = 0,        // the global band word (K/F). ONE word, CASed by every floor change.
		WaitGroupPush,    // WaitGroup's direct-waiter stack push
		Count
	};

	// NO deque-steal SITE, deliberately. Chase-Lev's `top` CAS is a SINGLE ATTEMPT -- it returns
	// nullopt on failure and the caller moves to the next victim -- so there is no per-call retry
	// count to record. Failed steals are already counted as probes-minus-hits by the steal stats,
	// which is the right instrument for a one-shot. A site here that could never populate would
	// print "(never called)" forever and read as a gap in coverage rather than an absence of loop.
	inline const char* const kRetrySiteNames[] = { "bands", "wg-push" };
	static_assert(sizeof(kRetrySiteNames) / sizeof(kRetrySiteNames[0]) == (size_t)RetrySite::Count,
		"kRetrySiteNames must have one entry per RetrySite -- an unnamed site prints as garbage, "
		"which is worse than not measuring it.");

#ifdef JLIBSCHED_RETRY_STATS

	// One more than the worker cap so a BARE THREAD (main pushing to a WaitGroup, a test driver)
	// has somewhere to go that is not worker 0's cell. Aliasing it onto a real worker would make
	// main's contention look like that worker's.
	inline constexpr size_t kRetrySlots = 65;
	inline constexpr size_t kRetrySpillSlot = 64;

	struct alignas(platform::kCacheLine) RetryCell {
		std::atomic<unsigned long long> calls{ 0 };
		std::atomic<unsigned long long> retries{ 0 };
		std::atomic<unsigned>           maxRetries{ 0 };
		std::atomic<unsigned long long> ge8{ 0 }, ge64{ 0 }, ge512{ 0 }, ge4096{ 0 };
	};

	// [site][slot]. Relaxed throughout: each cell has exactly one writer, and a reader that sees a
	// slightly stale count is reading a diagnostic, not making a decision.
	inline RetryCell g_retry[(size_t)RetrySite::Count][kRetrySlots];

	// Defined in TaskScheduler.cpp -- needs Thread, which this header must not include (Thread.h
	// includes TaskScheduler.h, and the CAS loops being instrumented live in both).
	size_t RetrySlotForCurrentThread() noexcept;

	inline void RecordRetries(RetrySite s, unsigned n) noexcept {
		RetryCell& c = g_retry[(size_t)s][RetrySlotForCurrentThread()];
		c.calls.fetch_add(1, std::memory_order_relaxed);
		if (!n) return;
		c.retries.fetch_add(n, std::memory_order_relaxed);
		// PLAIN COMPARE-AND-STORE, NOT AN ATOMIC MAX. Only this thread writes this cell, so there
		// is no race to lose -- and an atomic max would be a CAS loop inside the instrument for
		// measuring CAS loops.
		if (n > c.maxRetries.load(std::memory_order_relaxed))
			c.maxRetries.store(n, std::memory_order_relaxed);
		if (n >= 8)    c.ge8.fetch_add(1, std::memory_order_relaxed);
		if (n >= 64)   c.ge64.fetch_add(1, std::memory_order_relaxed);
		if (n >= 512)  c.ge512.fetch_add(1, std::memory_order_relaxed);
		if (n >= 4096) c.ge4096.fetch_add(1, std::memory_order_relaxed);
	}

	// COUNTS IN THE LOOP, COMMITS ON SCOPE EXIT. One store per retry in the hot path; the fetch_adds
	// happen once per call, off the retry path entirely.
	struct RetryProbe {
		unsigned  n = 0;
		RetrySite site;
		explicit RetryProbe(RetrySite s) noexcept : site(s) {}
		RetryProbe(const RetryProbe&) = delete;
		RetryProbe& operator=(const RetryProbe&) = delete;
		inline void Miss() noexcept { ++n; }
		~RetryProbe() { RecordRetries(site, n); }
	};

	inline constexpr bool kRetryStatsEnabled = true;
	void RetryStatsReset() noexcept;
	// Prints one line per site. Writes nothing when a site was never called, so an untouched site
	// does not read as "zero contention measured" when it is really "never exercised".
	void RetryStatsReport();

#else

	// The whole thing compiles away. `Miss()` on an empty struct is a no-op the optimiser deletes,
	// so instrumented loops keep their exact shape in a shipping build.
	struct RetryProbe {
		explicit RetryProbe(RetrySite) noexcept {}
		RetryProbe(const RetryProbe&) = delete;
		RetryProbe& operator=(const RetryProbe&) = delete;
		inline void Miss() noexcept {}
	};
	inline constexpr bool kRetryStatsEnabled = false;
	inline void RetryStatsReset() noexcept {}
	inline void RetryStatsReport() {}

#endif

}
