// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES THE STACK HIGH-WATER PROBE MEASURE DEPTH, OR JUST REPORT A NUMBER?
//
// StackClass::Tiny is 2 pages usable -- 8 KB on a 4K-page box -- and picking it is a bet about a
// body nobody has measured. The bet is settled by a guard-page fault: deterministic, which is the
// right design, and useless as a warning, because it tells you the answer by crashing.
//
// The probe fills a fiber's stack with a pattern at acquire and, at DEAD, scans upward from just
// above the guard for the first word that is no longer the pattern. This file is the check that
// the number it produces tracks REAL DEPTH rather than being an artefact of the fill.
//
// ---- THREE ARMS, AND THE THIRD IS WHAT MAKES THE FIRST TWO MEAN ANYTHING --------------------
//
//   SHALLOW  a body with no meaningful locals            -> some small high-water
//   DEEP     the same body plus a known-size local array -> high-water GREATER by about that array
//   OFF      the probe disabled                          -> nothing recorded at all
//
// Arm 2 alone proves nothing: a probe that returned the whole stack size every time would pass it.
// The claim is the DIFFERENCE tracking the array, and arm 3 is what says the instrument is not
// simply always-on reporting a constant.
//
// WHY THE ASSERTION IS A BAND AND NOT A NUMBER. The measured depth includes the entry wrapper, the
// body's own frame, whatever the compiler spilled, and the red zone -- none of which this file
// controls. What it DOES control is the difference between two bodies that differ by one array, so
// that difference is what is asserted, with slack for the frame around it.

#include "TaskScheduler.h"
#include "Thread.h"
#include "platform.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-66s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

// The array size the deep arm adds. Big enough to dominate the frame noise around it, small enough
// to fit a Standard fiber (60 KB usable) several times over.
static constexpr size_t kDeepBytes = 16 * 1024;

static std::atomic<int> g_ran{ 0 };
// The sink stops the compiler from proving the array dead and eliding it -- which would make this
// file measure the optimiser rather than the runtime, and pass while measuring nothing.
static std::atomic<unsigned char> g_sink{ 0 };

static void ShallowBody(void*) {
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

static void DeepBody(void*) {
    volatile unsigned char buf[kDeepBytes];
    // TOUCH BOTH ENDS. Writing only the top would leave the rest of the array unwritten, the fill
    // pattern intact beneath it, and the probe would honestly report a shallower depth than the
    // array implies -- which is the probe being right and the test being wrong.
    buf[0] = 1;
    buf[kDeepBytes - 1] = 2;
    for (size_t i = 0; i < kDeepBytes; i += 64) buf[i] = (unsigned char)i;
    g_sink.store(buf[kDeepBytes / 2], std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

// Runs one body as a FIBER task and returns the class high-water it produced.
static size_t RunAndMeasure(TaskScheduler& sched, void (*body)(void*), StackClass cls) {
    TaskScheduler::ResetStackHighWater();
    g_ran.store(0, std::memory_order_relaxed);

    WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);
    Task* t = sched.CreateTask(body, nullptr, Lane::Normal, TaskType::Fiber);
    if (!t) { Check(false, "CreateTask returned a task"); return 0; }
    t->stackClass = cls;
    t->waitGroup = &wg;
    sched.Push(t);
    sched.WaitFor(wg);

    // The measurement happens at FiberStatus::DEAD, which the worker reaches after the body returns
    // and after WaitFor is satisfied. Give it room rather than racing it.
    for (int i = 0; i < 200 && TaskScheduler::GetStackHighWater(cls) == 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    return TaskScheduler::GetStackHighWater(cls);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== the stack high-water probe measures depth ===\n");

    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();
    std::printf("  page=%zu, Standard usable=%zu bytes\n",
                platform::PageSize(), (size_t)(60 * 1024));

    // ---- ARM 3 FIRST: with the probe OFF, nothing may be recorded. ------------------------
    //
    // Run first deliberately. If the probe were always-on, this arm fails and the file reports
    // "records with the probe disabled" -- which is the more useful message than arm 2 quietly
    // succeeding for the wrong reason.
    TaskScheduler::SetStackProbe(false);
    const size_t off = RunAndMeasure(sched, &DeepBody, StackClass::Standard);
    Check(g_ran.load() == 1, "the body ran with the probe off");
    Check(off == 0, "with SetStackProbe(false) NOTHING is recorded (it is not always-on)");

    // ---- ARM 1: shallow ------------------------------------------------------------------
    TaskScheduler::SetStackProbe(true);
    const size_t shallow = RunAndMeasure(sched, &ShallowBody, StackClass::Standard);
    std::printf("  shallow body high-water: %zu bytes\n", shallow);
    Check(g_ran.load() == 1, "the shallow body ran");
    Check(shallow > 0, "the probe recorded SOMETHING for a body that ran at all");
    Check(shallow < 16 * 1024,
          "and a body with no locals is not reported as deep (the fill is not the measurement)");

    // ---- ARM 2: the same, plus a known 16 KB array ---------------------------------------
    const size_t deep = RunAndMeasure(sched, &DeepBody, StackClass::Standard);
    std::printf("  deep body high-water:    %zu bytes  (+%zd vs shallow, array is %zu)\n",
                deep, (ptrdiff_t)deep - (ptrdiff_t)shallow, kDeepBytes);
    Check(g_ran.load() == 1, "the deep body ran");

    // THE CLAIM: the difference tracks the array. A band, not a number -- the entry wrapper, the
    // body's frame and whatever the compiler spilled are all in there and none of them is this
    // file's business. Half the array is far more than frame noise; twice it would mean the probe
    // is measuring something other than what was added.
    const ptrdiff_t delta = (ptrdiff_t)deep - (ptrdiff_t)shallow;
    Check(delta > (ptrdiff_t)(kDeepBytes / 2),
          "adding a 16 KB local moved the high-water by at least half that");
    Check(delta < (ptrdiff_t)(kDeepBytes * 2),
          "and by no more than twice it -- the probe tracks the array, not something else");

    // ---- AND THE SIZING QUESTION IT EXISTS TO ANSWER --------------------------------------
    //
    // Stated as a line rather than an assertion, because what a Tiny stack can hold is a fact about
    // the reader's body, not about this runtime. The point is that the number is now available
    // BEFORE the guard page answers it.
    const size_t tinyUsable = 2 * platform::PageSize();
    std::printf("\n  Tiny is %zu bytes usable. The deep body above measured %zu -- it would fault.\n"
                "  That is the probe doing its job: the answer arrives as a number here rather than\n"
                "  as an access violation on whichever thread you put the body on.\n",
                tinyUsable, deep);

    TaskScheduler::SetStackProbe(false);
    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
