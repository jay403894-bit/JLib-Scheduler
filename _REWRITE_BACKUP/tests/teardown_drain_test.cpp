// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// TEARDOWN IS A DRAIN, NOT AN ABANDONMENT.
//
// A frame parked on a primitive nobody will signal again used to be simply left there: its stack
// never unwound, so NOTHING it held was released -- RAII objects, its WaitGroup slot, a hazard
// record. Join() now walks the registry of live primitives, releases every waiter with Cancelled,
// and only then joins the workers, so each parked frame resumes, observes the cancel, and UNWINDS.
//
// THE ASSERTION IS AN RAII DESTRUCTOR RUNNING, deliberately. Checking that "the wait returned"
// would not distinguish a drain from a lucky signal; a destructor on the parked frame's stack can
// only run if that stack was actually unwound.

#include "TaskScheduler.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int  g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-68s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

namespace {
std::atomic<int> g_unwound{ 0 };

// Lives on the parked fiber's stack. Its destructor is the whole assertion.
struct UnwindWitness {
    ~UnwindWitness() { g_unwound.fetch_add(1, std::memory_order_release); }
};
} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();
    std::printf("teardown drain -- workers=%zu\n\n", sched.GetWorkerCount());

    // CONSTRUCTED AFTER Init, so it registers. A file-scope primitive would not -- see WaitPrimitive.
    auto* gate = new SchedulerMutex();
    gate->Lock();                                  // never unlocked, on purpose

    std::atomic<bool> parked{ false };

    // Four fibers, each parked on a lock that will never be released by anyone.
    for (int i = 0; i < 4; ++i) {
        Task* t = sched.CreateTask([&]() {
            UnwindWitness witness;                 // must be destroyed for the test to pass
            parked.store(true, std::memory_order_release);
            (void)gate->LockCancellable();
            // Not unlocking: if this ever acquires, the test is measuring the wrong thing.
        }, false, FiberSize::Standard, TaskType::Fiber);
        sched.Push(t);
    }

    for (int i = 0; i < 1000 && !parked.load(std::memory_order_acquire); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    Check(g_unwound.load(std::memory_order_acquire) == 0,
          "nothing has unwound yet -- four fibers are parked on a lock nobody will release");

    // TEARDOWN. No unlock, no signal, no cancel from the app: Join() alone must get them out.
    sched.Join();

    Check(g_unwound.load(std::memory_order_acquire) == 4,
          "Join() drained every parked frame and their stacks UNWOUND (destructors ran)");

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
