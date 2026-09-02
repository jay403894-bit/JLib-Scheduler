// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// WHAT THIS IS FOR
//
// One question: is it worth allocating coroutine FRAMES from a pool instead of global new?
//
// A spawned coroutine costs two allocations -- the frame, which the compiler emits an `operator new`
// for, plus a Task from the slab. Pooling the frame is the obvious optimization, and it is exactly
// the kind of obvious that this project has been burned by: the bare-thread fast spin was just as
// plausible and measured as a 24x REGRESSION (see bench/lock_contention.cpp). So this measures the
// thing before anyone sizes a pool for it.
//
// The decisive row is FRAME ONLY. It creates coroutines and destroys them WITHOUT ever scheduling
// them, so it isolates frame allocation + promise construction + destruction from everything else.
// Whatever that costs is the ABSOLUTE CEILING on what pooling could save; if it is a small fraction
// of a spawn, the pool is not worth its complexity, its second arena, or the risk of inhibiting the
// compiler's own heap-allocation elision.
//
// The other rows exist to give that number a denominator:
//
//   native task     a plain Native task through the same push/steal/complete path. The baseline --
//                   the gap between this and `coroutine` is what the coroutine mode costs at all.
//   coroutine       spawn -> run -> complete, never suspending.
//   coro + suspend  one `co_await Reschedule{}`, so it pays a re-push and a second dispatch.
//   Lazy await      awaited inline by another coroutine: no scheduling, no dispatch, symmetric
//                   transfer only. This is the cheap composition path and should look nothing like
//                   the others.
//
// METHOD, per the lesson in bench/lock_contention.cpp: report the MEDIAN of several reps and the
// full min..max spread alongside it. A single number from a single run of a multi-binary benchmark
// is how the fast-spin harness produced a p90 of 52% noise and nearly shipped a regression. Reading
// the spread is not optional here -- if the arms overlap, there is no result.
//
// FRAME SIZES: build with -DJLIBSCHED_CORO_STATS=ON and this also prints a histogram of the real
// frame sizes the compiler chose, which is the other input a pool needs and the one that cannot be
// derived from the source.
//
// BUILD
//   cmake -B build-corobench -DJLIBSCHED_COROUTINES=ON -DJLIBSCHED_CORO_STATS=ON
//   cmake --build build-corobench --config Release --target SchedulerCoroBench
// RUN
//   SchedulerCoroBench.exe [count] [reps]
//
// RESULTS (2026-08-23, Windows, MSVC, 31 workers, 20,000 items, median of 7 reps)
//
// POOLING COROUTINE FRAMES IS NOT WORTH DOING. Not yet, and not for this reason.
//
//   native task      1192.5 ns   spread 1178.8 .. 1200.2
//   coroutine        1291.0 ns   spread 1273.8 .. 1348.7    1.08x native
//   coro + suspend   1821.3 ns   spread  367.0 .. 1851.0    1.53x native   <-- bimodal, see below
//   Lazy await         31.1 ns   spread   31.0 ..   33.4    0.03x native
//   FRAME ONLY         28.1 ns   spread   27.0 ..   29.1    0.02x native
//
// Frame allocation + promise construction + destruction is 28.1 ns, or 2.2% of a full
// spawn->complete. And pooling cannot remove that 2.2% -- it can only replace global new with a slab
// Alloc, so the realistic saving is some fraction of it, call it 1%. Against that: a second arena
// with its own sizing knob, a mandatory fallback path for oversized frames, and a plausible loss of
// the compiler's own heap-allocation elision. Not a good trade at this price.
//
// TWO OTHER THINGS WORTH KEEPING:
//
// The coroutine mode costs 8% over a plain Native task through the same push/steal/complete path.
// That is the price of the whole third execution mode, and it is small because resume is just
// fn(data) -- the same call the worker already makes.
//
// `Lazy` awaited inline is 31 ns against 1291 ns for a spawn: 41x cheaper. That is the lazy,
// run-on-the-awaiting-worker, symmetric-transfer design paying off exactly as intended, and it means
// Lazy is cheap enough for ordinary decomposition rather than only for coarse work. Anyone tempted
// to make `co_await Child()` fork instead of running inline should read that ratio first.
//
// FRAME SIZES (same run, -DJLIBSCHED_CORO_STATS=ON; note that flag inflates the timings above, so
// the two must be read from separate runs):
//   640,008 frames, peak 20,000 live, largest 224 bytes
//     <=64    75.0%
//     <=128   25.0%
//     <=256    0.0%  (8 frames)
// So they are small -- a 256-byte slot would have covered every frame here. That is USEFUL LATER
// even though pooling lost: it is the sizing input, already measured, if the decision is revisited.
//
// WHAT THIS DOES NOT MEASURE, and the reason to revisit rather than treat it as closed: it measures
// per-call LATENCY on one machine with one compiler, at a peak of 20,000 live frames. It does not
// measure allocator CONTENTION under sustained high concurrency across many threads, which is the
// regime an I/O runtime with tens of thousands of in-flight operations would actually create. If
// JLib::IO produces that regime, re-run this before concluding anything -- the answer there could
// legitimately differ, and the frame-size histogram above is the input for sizing it.
//
// Also unreliable as measured: the `coro + suspend` row spans 367..1851 ns. It is bimodal, the same
// way bench.cpp's throughput/mp row is, and its headline number should not be quoted without the
// spread next to it.
//
// ---- ALLOCATOR CONTENTION (2026-08-23, -DJLIBSCHED_CORO_POOL=ON, arms interleaved in-process) ----
//
// Contention is the RIGHT argument for a slab -- per-call latency was never going to justify one.
// So it was tested directly, scaling the number of concurrently allocating threads:
//
//   threads   pooled ns   global ns   speedup
//     1         1314.1      1310.7     1.00x
//     2          819.4       783.1     0.96x
//     4          528.0       539.0     1.02x
//     8          356.6       346.6     0.97x
//    16          420.2       360.8     0.86x
//
// STILL NO. And the decisive column is `global`, not the speedup: its per-item cost FALLS as threads
// are added (1310 -> 347) and then flattens. If the general allocator were the bottleneck it would
// RISE with thread count. It does not, so there is no contention here for a pool to remove -- the
// flattening at 8-16 is the scheduler path saturating. Consistent with the FRAME ONLY row above:
// frame allocation is ~2% of a spawn, and something that small cannot become the bottleneck by
// having more threads do it.
//
// WHERE THIS TEST IS WEAK, stated because the high-thread rows should not be over-read. The pool here
// is a bump allocator with a per-thread free list and NO cross-thread rebalancing, and this workload
// migrates one way: producer threads allocate, workers free. Freed slots therefore pile up on worker
// thread-locals where producers never see them, so producers keep bump-allocating until the 65,536
// slots run out and fall through to global new anyway. At 4 threads and above the run exceeds that
// budget, so those rows measure a partly-exhausted pool, which is most of why pooled degrades to
// 0.86x at 16. A production pool would flush and refill through a shared list -- which is exactly
// what TaskAllocator does and exactly why it cannot simply be instantiated a second time (see the
// note on CoroFramePool in Coroutine.h). Rows 1 and 2 are the clean ones, and they are a wash.
//
// So the conclusion does not rest on the pooled column being good. It rests on the global column
// showing nothing to fix.
//
// ---- RETESTED WITH THE CENTRAL SLAB (2026-08-23) --------------------------------------------
//
// The run above used a PRIVATE pool, and that was the wrong design to test. TaskAllocator's
// per-thread free-list cache is a `static thread_local` in a STATIC member function -- one cache per
// thread for the whole class -- which is both why a second instance corrupts the heap and why the
// central slab is the only version worth measuring: one cache already serves every allocation routed
// through it, with refill/flush rebalancing through a shared backing list. A private pool cannot
// share any of that, which is exactly why it collapsed under this workload's one-way migration.
//
// So frames were routed through the scheduler's OWN TaskAllocator instead. Speedup vs global new,
// four consecutive runs (arms still interleaved in-process):
//
//   threads     run1   run2   run3   run4
//     1         1.06   1.05   1.05   1.04
//     2         1.02   1.05   1.01   1.00
//     4         1.07   1.00   1.08   1.03
//     8         1.04   1.03   1.02   1.02
//    16         0.82   0.71   0.90   0.84
//
// The central slab IS better than the private pool -- a consistent 2-6% win up to 8 threads where the
// private one was a wash. STILL NOT WORTH SHIPPING, and the reason inverts the original argument:
// at 16 threads it loses by ~18%, reproducibly, in every run.
//
// WHY THE REVERSAL, and it is the interesting part. Pooling frames centrally does not add a new
// contention point, it DOUBLES THE TRAFFIC THROUGH AN EXISTING ONE -- every coroutine now takes two
// slots from the same allocator instead of one, and this workload migrates one way (producers
// allocate, workers free), so the shared refill/flush list carries all of it. Below 8 threads the
// per-thread cache absorbs that and the slab's cheaper fast path wins. At 16 the shared list becomes
// the bottleneck and malloc -- which scales across independent arenas -- simply handles it better.
//
// That matters more than the size of the loss: high thread count is precisely the regime an I/O
// runtime would live in, so the contention argument that motivated pooling in the first place ends up
// arguing AGAINST it. Add the coupling it introduces -- a coroutine-heavy phase consuming task slab
// slots -- and there is nothing left to recommend it.
//
// Cross-run caveat worth heeding: absolute numbers moved a lot between runs (global at 16 threads
// ranged 134..360 ns across sessions). Only the interleaved speedup column is comparable; the
// absolutes are not, which is the same reason the arms are interleaved at all.
//
// ---- DECISION: SHIPPED, ON BY DEFAULT (2.12.0) -----------------------------------------------
//
// The contention rows above are not the whole picture, and reading only them would have got this
// wrong. Checking the per-item rows WITH pooling on turned up the number that actually decided it:
//
//                        global new   task slab
//   Lazy await, inline      31.1 ns     16.3 ns    1.9x FASTER
//   frame alloc+free        28.1 ns      ~17 ns
//   coroutine spawn          1291 ns     ~1263 ns
//   16 producer threads          --          --    ~18% slower (the one regression)
//
// The inline-Lazy path nearly HALVED. That is the composition path -- `co_await Child()` running on
// the awaiting worker with no dispatch -- so it is the one likely to run at high frequency, and it
// also disposes of the worry that declaring `operator new` on the promise would inhibit the
// compiler's frame elision. It did not; the slab's fast path simply beat malloc's.
//
// AND THE DECIDING ARGUMENT IS NOT IN THIS FILE AT ALL: fragmentation. Fixed 256-byte slots in one
// contiguous prefaulted region cannot fragment -- no size classes, no splitting, no coalescing -- so
// a process running for hours has the layout it had at startup. A few-second benchmark cannot see
// that, and its silence is not evidence of absence. Routing frames through the same slab gives the
// application ONE source of memory truth: one arena to size, one place to observe, one failure mode,
// and no coroutine-shaped hole in the zero-allocation steady state this library advertises.
//
// The 16-thread regression is accepted knowingly, and the escape hatch is runtime rather than
// compile-time: SetCoroFramePooling(false). The cost that needs documenting louder than the
// regression is CAPACITY -- each spawned coroutine now takes TWO slab slots, so a slab sized for N
// tasks holds N/2 concurrent coroutines. See TaskScheduler::SetSlabSizes.

#include "TaskScheduler.h"
#include "Coroutine.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return n == 0 ? 0.0 : (n % 2 ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]));
}

static std::atomic<uint64_t> g_sink{ 0 };

static JLib::Coro TrivialCoro() {
    g_sink.fetch_add(1, std::memory_order_relaxed);
    co_return;
}

static JLib::Coro SuspendingCoro() {
    g_sink.fetch_add(1, std::memory_order_relaxed);
    co_await JLib::Reschedule{};
    co_return;
}

static JLib::Lazy<int> LazyLeaf(int n) { co_return n; }

// Awaits `count` Lazys inline. No scheduling at all past the one Coro that drives it.
static JLib::Coro LazyDriver(int count) {
    int acc = 0;
    for (int i = 0; i < count; ++i) acc += co_await LazyLeaf(i);
    g_sink.fetch_add((uint64_t)acc, std::memory_order_relaxed);
}

struct Row {
    const char* name;
    std::vector<double> nsPerItem;
};

static void Report(const Row& r, double baselineMedian) {
    const double med = Median(r.nsPerItem);
    const double lo = *std::min_element(r.nsPerItem.begin(), r.nsPerItem.end());
    const double hi = *std::max_element(r.nsPerItem.begin(), r.nsPerItem.end());
    std::printf("  %-16s %9.1f ns   spread %7.1f .. %-8.1f", r.name, med, lo, hi);
    if (baselineMedian > 0.0 && med > 0.0) std::printf("  %5.2fx native", med / baselineMedian);
    std::printf("\n");
}

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    const int count = (argc > 1) ? std::atoi(argv[1]) : 20000;
    const int reps  = (argc > 2) ? std::atoi(argv[2]) : 7;

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("JLib::Scheduler coroutine bench -- workers=%zu  count=%d  reps=%d\n",
                sched.GetWorkerCount(), count, reps);
#if defined(JLIBSCHED_CORO_STATS)
    std::printf("frame-size instrumentation: ON (adds a counter per allocation; timings are inflated)\n");
#else
    std::printf("frame-size instrumentation: off (-DJLIBSCHED_CORO_STATS=ON to see frame sizes)\n");
#endif
    std::printf("\n");

    Row native{ "native task", {} }, coro{ "coroutine", {} }, coroSusp{ "coro + suspend", {} };
    Row frameOnly{ "FRAME ONLY", {} }, lazyRow{ "Lazy await", {} };

    // One warm-up rep of everything, discarded: first touch pays page faults and frequency ramp,
    // and folding that into rep 0 would bias whichever row happened to run first.
    for (int rep = 0; rep < reps + 1; ++rep) {
        const bool warm = (rep == 0);

        {   // native task baseline -- same push/steal/complete path, no coroutine anywhere
            JLib::WaitGroup wg;
            wg.n.store(count, std::memory_order_relaxed);
            const auto t0 = Clock::now();
            for (int i = 0; i < count; ++i) {
                auto* t = sched.CreateTask([] { g_sink.fetch_add(1, std::memory_order_relaxed); });
                t->waitGroup = &wg;
                sched.Push(t);
            }
            sched.WaitFor(wg);
            const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
            if (!warm) native.nsPerItem.push_back(ns / count);
        }

        {   // spawn -> run -> complete, no suspension
            JLib::WaitGroup wg;
            const auto t0 = Clock::now();
            for (int i = 0; i < count; ++i) JLib::Spawn(TrivialCoro(), &wg);
            sched.WaitFor(wg);
            const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
            if (!warm) coro.nsPerItem.push_back(ns / count);
        }

        {   // one suspension: pays an extra re-push and dispatch
            JLib::WaitGroup wg;
            const auto t0 = Clock::now();
            for (int i = 0; i < count; ++i) JLib::Spawn(SuspendingCoro(), &wg);
            sched.WaitFor(wg);
            const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
            if (!warm) coroSusp.nsPerItem.push_back(ns / count);
        }

        {   // THE DECISIVE ROW: frame allocation + promise construction + destruction, nothing else.
            //
            // Held in a vector rather than created and destroyed inside the loop, and that is
            // deliberate: a contained create/destroy is exactly the shape the compiler is allowed to
            // elide entirely (HALO), which would measure zero and answer the wrong question. A
            // SPAWNED coroutine can never be elided -- its handle escapes into a Task -- so the
            // non-elidable case is the one that matters here.
            std::vector<JLib::Coro> held;
            held.reserve((size_t)count);
            const auto t0 = Clock::now();
            for (int i = 0; i < count; ++i) held.emplace_back(TrivialCoro());
            held.clear();     // ~Coro destroys each frame; bodies never ran (initial_suspend)
            const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
            if (!warm) frameOnly.nsPerItem.push_back(ns / count);
        }

        {   // Lazy awaited inline -- symmetric transfer, no dispatch
            JLib::WaitGroup wg;
            const auto t0 = Clock::now();
            JLib::Spawn(LazyDriver(count), &wg);
            sched.WaitFor(wg);
            const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
            if (!warm) lazyRow.nsPerItem.push_back(ns / count);
        }
    }

    const double base = Median(native.nsPerItem);
    std::printf("per-item cost (median of %d reps)\n", reps);
    Report(native,    0.0);
    Report(coro,      base);
    Report(coroSusp,  base);
    Report(lazyRow,   base);
    std::printf("\n");
    Report(frameOnly, base);
    std::printf("  ^ the ceiling on what pooling frames could save. Compare against 'coroutine':\n");
    const double medCoro = Median(coro.nsPerItem), medFrame = Median(frameOnly.nsPerItem);
    if (medCoro > 0.0)
        std::printf("    frame alloc+free is %.1f%% of a full spawn->complete.\n",
                    100.0 * medFrame / medCoro);
    std::printf("    Pooling can only ever remove part of THAT -- it replaces global new, it does\n"
                "    not remove the allocation. Weigh against a second arena and possible loss of\n"
                "    the compiler's own elision.\n\n");

#if defined(JLIBSCHED_CORO_POOL)
    // ---- ALLOCATOR CONTENTION: the case pooling actually exists for --------------------------
    //
    // Everything above allocates frames from ONE thread, which is the wrong axis for this question.
    // An allocator's cost is not linear in thread count: a general-purpose one falls back to shared
    // arenas once per-thread caches miss, and coroutine frames make that worse than usual because
    // they are routinely allocated on one thread and freed on another. This scales the number of
    // CONCURRENTLY ALLOCATING threads and compares pooled against global new at each width.
    //
    // The two arms are INTERLEAVED INSIDE THIS PROCESS, alternating per rep, because the pool is
    // switchable at runtime. Comparing them across two binaries is the method that produced a 52%
    // p90 noise floor in the fast-spin work and nearly shipped a regression.
    std::printf("allocator contention -- concurrently allocating threads\n");
    std::printf("  %-8s %14s %14s %10s\n", "threads", "pooled ns", "global ns", "speedup");

    const int kPerThread = 20000;
    for (int producers : { 1, 2, 4, 8, 16 }) {
        std::vector<double> pooled, global;

        for (int rep = 0; rep < reps + 1; ++rep) {
            for (int arm = 0; arm < 2; ++arm) {
                // Alternate which arm goes first each rep so neither keeps inheriting the other's
                // leftover allocator state.
                const bool usePool = ((arm + rep) % 2) == 0;
                JLib::SetCoroFramePooling(usePool);

                JLib::WaitGroup wg;
                std::vector<std::thread> producersV;
                producersV.reserve((size_t)producers);

                const auto t0 = Clock::now();
                for (int p = 0; p < producers; ++p) {
                    producersV.emplace_back([&wg, kPerThread] {
                        for (int i = 0; i < kPerThread; ++i) JLib::Spawn(TrivialCoro(), &wg);
                    });
                }
                for (auto& th : producersV) th.join();
                sched.WaitFor(wg);
                const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();

                if (rep > 0) {
                    const double per = ns / (producers * kPerThread);
                    (usePool ? pooled : global).push_back(per);
                }
            }
        }

        const double mp = Median(pooled), mg = Median(global);
        std::printf("  %-8d %14.1f %14.1f %9.2fx\n", producers, mp, mg, mg > 0 ? mg / mp : 0.0);
    }
    JLib::SetCoroFramePooling(true);
    std::printf("  (speedup > 1.00 means pooling won. Read it against the single-thread rows above:\n"
                "   if it only wins at high thread counts, contention is the whole story.)\n\n");
#endif

#if defined(JLIBSCHED_CORO_STATS)
    JLib::DumpCoroFrameStats();
#endif
    return 0;
}
