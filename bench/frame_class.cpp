// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Does the 64-byte coroutine frame class actually pay, and by how much?
//
// MEASURED THE ONLY WAY THIS PROJECT TRUSTS. The obvious way to answer this is to build twice and
// compare, and that is exactly the mistake: two processes differ in page placement, ASLR, turbo
// residency and scheduler luck, and the difference between them has been measured at 50-100% noise
// on this machine. So both arms run HERE, in one process, ALTERNATING per repetition rather than in
// blocks -- a block layout lets a thermal or frequency drift halfway through masquerade as the
// effect, and alternating cancels any monotonic drift into both arms equally.
//
// The arms are the same code path with one runtime flag flipped (SetCoroSmallFrameClass), which is
// safe mid-run because FrameFree routes by ADDRESS, not by the current mode -- frames allocated
// under either setting free correctly afterwards.
//
// An A/A CONTROL runs first: both arms with the flag ON. Whatever spread that reports is the floor
// below which an A/B difference means nothing. A harness that cannot show ~0 on A/A cannot be
// trusted to show a real effect on A/B, and the first version of the lock-contention harness here
// failed exactly that check.

#include <TaskScheduler.h>
#include <Coroutine.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>

using Clock = std::chrono::steady_clock;

// A frame small enough to land in the 64-byte class. Kept trivial on purpose: this measures the
// allocator, and any real body would bury it.
// MUST be Coro, not Lazy<int>. A Lazy carries the promise value storage and lands in the <=128
// bucket, so it is NOT eligible for the 64-byte class -- an earlier version of this bench used one
// and dutifully reported "no difference" while toggling a branch that never fired. A bench that
// cannot exercise the thing it measures reports the noise floor and looks like a result.
static JLib::Coro Tiny(int x) { (void)x; co_return; }

// One frame ALLOCATED AND FREED, with no scheduling in the way. A Lazy suspends at
// initial_suspend, so creating one and letting it go out of scope is exactly
// operator new + operator delete on the frame -- which is the thing being measured. Driving
// it to completion instead would add microseconds of dispatch and bury a ~30 ns signal.
//
// The allocation cannot be elided: promise_type DECLARES operator new, which inhibits HALO.
static volatile void* g_sink = nullptr;
static double OneRep(int iters) {
    const auto t0 = Clock::now();
    for (int i = 0; i < iters; ++i) {
        auto l = Tiny(i);
        g_sink = &l;                 // keep the object observably alive
    }
    const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
    return ns / double(iters);
}

struct Stat { double best, median, worst; };

static Stat Summarize(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return Stat{ v.front(), v[v.size() / 2], v.back() };
}

// reps alternate: even -> arm A, odd -> arm B.
static void RunPair(const char* label, int reps, int iters, bool armAsmall, bool armBsmall) {
    std::vector<double> a, b;
    for (int r = 0; r < reps; ++r) {
        const bool isA = (r % 2) == 0;
        JLib::SetCoroSmallFrameClass(isA ? armAsmall : armBsmall);
        const double ns = OneRep(iters);
        (isA ? a : b).push_back(ns);
    }
    const Stat sa = Summarize(a), sb = Summarize(b);
    const double delta = (sb.median - sa.median) / sa.median * 100.0;
    std::printf("%-22s A %7.1f ns (med, %6.1f..%6.1f)   B %7.1f ns (med, %6.1f..%6.1f)   B-A %+6.1f%%\n",
                label, sa.median, sa.best, sa.worst, sb.median, sb.best, sb.worst, delta);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("coroutine frame size-class A/B -- workers=%zu\n", sched.GetWorkerCount());
    std::printf("inline Lazy, one frame per iteration; arms alternate every rep\n\n");

    constexpr int kReps = 40;
    constexpr int kIters = 20000;

    // Warm-up: first touch of both pools should not land inside a timed rep.
    JLib::SetCoroSmallFrameClass(true);  OneRep(kIters);
    JLib::SetCoroSmallFrameClass(false); OneRep(kIters);

    // A/A first. Both arms identical -- anything but ~0 here means the harness is measuring itself.
    RunPair("A/A control (both on)", kReps, kIters, true, true);

    // A = 64-byte class, B = 256-byte slot (the behaviour before the class existed).
    RunPair("64-class vs 256-slot",  kReps, kIters, true, false);

    JLib::SetCoroSmallFrameClass(true);
    std::printf("\nB-A positive means the 256-byte slot is SLOWER, i.e. the 64-byte class wins.\n");
    std::printf("Read it only if the A/A row is near zero.\n");

    sched.Join();
    return 0;
}
