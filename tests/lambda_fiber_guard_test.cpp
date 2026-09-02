// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A LAMBDA CANNOT BE GIVEN A FIBER ROW -- AND NOT ONLY AT THE CONSTRUCTOR.
//
// 5.0.1 removed the TaskType parameter from CreateTask's lambda overload, so the CONSTRUCTOR can no
// longer produce a lambda fiber. That closed the door people walk through. It did not close the
// window, and the window is this:
//
//     Task* t = sched.CreateTask([&]{ ...waits... });   // Native LambdaTask
//     t->type = TaskType::Fiber;                        // `type` is a public bitfield
//     sched.Push(t);                                    // a lambda fiber, no diagnostic
//
// `Task` is a struct, `type` is public, and the library itself reassigns it (IoShared.cpp,
// CreateTaskImpl). A static_assert guards a CALL; it cannot guard an OBJECT that stays mutable
// afterwards, and the guarantee the runtime needs is about the object at the moment a row is
// leased to it.
//
// So Task carries a `lambdaBody` bit, set inside LambdaTask's constructors where it cannot be
// forgotten, and Thread::AcquireFiber refuses. This file is the proof that the refusal happens,
// because a check nobody has watched fire is indistinguishable from one that is disconnected.
//
// ---- WHAT THIS FILE CAN AND CANNOT ASSERT ---------------------------------------------------
//
// The violation ABORTS -- deliberately, and it must, because letting the task run Native instead
// would fail at the first WaitOnEvent several frames away with a message about the wrong thing.
// An abort cannot be caught, so the illegal arm runs in a CHILD PROCESS and this file asserts on
// its exit code. The legal arms run in-process, where they are cheap and prove the check
// discriminates rather than fires at everything.
//
//   ARM 1 (in-process)  a lambda task left Native            -> runs, no abort
//   ARM 2 (in-process)  a RAW fn-pointer task as Fiber       -> runs, no abort   <- the supported form
//   ARM 3 (child)       a lambda task forced to Fiber        -> ABORTS
//
// Arm 2 is the one that stops this from passing against a check that refused every fiber.

#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>

using namespace JLib;

static int g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-66s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

static std::atomic<int> g_ran{ 0 };

// Named bodies with explicit contexts, like every task body in this tree.
static void RawBody(void*) { g_ran.fetch_add(1, std::memory_order_release); }

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- THE CHILD ARM. Runs first when asked, and is expected to die. ----------------------
    //
    // Argv-driven rather than a separate binary so the two arms cannot drift apart: the illegal
    // task is constructed by the same code, in the same process image, as the legal ones.
    const bool childMode = (argc > 1 && std::strcmp(argv[1], "--violate") == 0);

    if (!childMode) std::printf("=== a lambda task can never be bound to a fiber ===\n");

    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();

    if (childMode) {
        // The hole, exactly as a user could write it today.
        Task* t = sched.CreateTask([] { g_ran.fetch_add(1, std::memory_order_release); });
        if (!t) return 77;                       // distinguishable from the abort code
        t->type = TaskType::Fiber;               // <- public field; nothing stops this at compile time
        sched.Push(t);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        // Reaching here means AcquireFiber accepted a closure. That is the bug this file exists
        // for, so exit distinctly rather than 0 -- a silent success would look like a pass.
        std::printf("CHILD SURVIVED -- AcquireFiber accepted a lambda fiber\n");
        return 42;
    }

    // ---- ARM 1: a lambda task, left alone. Must run normally. ------------------------------
    {
        g_ran.store(0, std::memory_order_relaxed);
        WaitGroup wg; wg.n.store(1, std::memory_order_relaxed);
        Task* t = sched.CreateTask([] { g_ran.fetch_add(1, std::memory_order_release); });
        Check(t != nullptr, "a lambda task was created");
        if (t) { t->waitGroup = &wg; sched.Push(t); sched.WaitFor(wg); }
        Check(g_ran.load(std::memory_order_acquire) == 1,
              "an ordinary lambda task still runs (the guard does not touch Native)");
    }

    // ---- ARM 2: THE CONTROL. A raw fn-pointer task AS A FIBER must be unaffected. -----------
    //
    // Without this, a guard that refused every fiber would pass arm 3 and this file would be
    // asserting that the runtime is broken.
    {
        g_ran.store(0, std::memory_order_relaxed);
        WaitGroup wg; wg.n.store(1, std::memory_order_relaxed);
        Task* t = sched.CreateTask(&RawBody, nullptr, Lane::Normal, TaskType::Fiber);
        Check(t != nullptr, "a raw fn-pointer FIBER task was created");
        if (t) { t->waitGroup = &wg; sched.Push(t); sched.WaitFor(wg); }
        Check(g_ran.load(std::memory_order_acquire) == 1,
              "the supported fiber form still runs (the guard discriminates)");
    }

    // ---- ARM 3: re-run ourselves with the hole, and require that it dies. ------------------
    {
        char cmd[1024];
        std::snprintf(cmd, sizeof cmd, "\"%s\" --violate", argv[0]);
        const int rc = std::system(cmd);
        std::printf("  child (lambda forced to TaskType::Fiber) exit code: %d\n", rc);

        // NOT `rc != 0`, and the distinction matters. 42 means the child RAN THE TASK and
        // survived -- the guard is absent -- which is the failure this file is for, and it must
        // not be credited as "it exited nonzero, good". 77 means CreateTask returned null, which
        // says nothing either way. Anything else is the abort, which is the pass.
        Check(rc != 42, "the child did NOT survive -- AcquireFiber refused the lambda fiber");
        Check(rc != 77, "the child got a task (the arm was not skipped by an allocation failure)");
        Check(rc != 0,  "the child terminated abnormally, as an aborted invariant should");
    }

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
