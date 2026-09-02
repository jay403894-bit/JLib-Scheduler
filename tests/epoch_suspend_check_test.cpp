// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES THE EPOCH-GUARD SUSPEND CHECK ACTUALLY FIRE?
//
// `EpochGuardSuspendCheck` is wired into all eleven ContextSwitch call sites and enforced in
// RELEASE, and until this file existed it had never been observed to do anything. That is the
// weakest possible position for a safety check: an unfired tripwire and a disconnected one look
// identical from outside, and the whole suite passing is consistent with both.
//
// Epochs.h shipped a test seam -- SetEpochSuspendViolationHandlerForTest -- for exactly this, and
// nothing used it. This file uses it.
//
// ---- WHAT THE CHECK IS FOR, because the failure it prevents is not obvious ---------------------
//
// Every reader announces its THREAD's epoch slot. A guard taken on worker A announces A's slot; if
// the fiber suspends and resumes on worker B, the guard's destructor clears B's slot -- which was
// never set there. That un-announces whatever live traversal B was running and frees its nodes
// underneath it. Not a stall, not a leak: a use-after-free in an unrelated fiber on another worker.
//
// ---- TWO ARMS, AND THE SECOND IS THE ONE THAT MAKES THE FIRST MEAN ANYTHING -------------------
//
//   ARM 1  suspend INSIDE a guard          -> the check MUST fire
//   ARM 2  suspend with NO guard held      -> the check MUST NOT fire
//
// Arm 1 alone would pass against a check that fired unconditionally, which would be just as broken
// and considerably more annoying. Arm 2 is what says the check discriminates rather than shouts.

#include "TaskScheduler.h"
#include "Epochs.h"
#include "Thread.h"
#include "Event.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-68s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

// THE SEAM REPLACES WHAT HAPPENS AFTER DETECTION, NEVER WHETHER DETECTION RUNS. Without it the
// check aborts the process, and a test cannot assert on a process that is already gone.
static std::atomic<int> g_violations{ 0 };
static void OnViolation() { g_violations.fetch_add(1, std::memory_order_relaxed); }

// ---- THE BODIES. Named functions with explicit contexts, like every task body in this tree. ----
struct ArmCtx {
    TaskScheduler*     s;
    Event*             gate;
    std::atomic<bool>* released;
    std::atomic<int>*  ran;
};

// ARM 1: take a guard, then suspend while still holding it. This is the illegal shape.
static void SuspendInsideGuardBody(void* p) {
    auto& c = *static_cast<ArmCtx*>(p);
    {
        EpochGuard g;                         // announces this THREAD's slot
        // ARMED, so a release that already landed self-signals rather than parking forever. The
        // point of this arm is the suspension, not a hang.
        c.s->WaitOnEventArmed(*c.gate, [&c] {
            if (c.released->load(std::memory_order_acquire)) c.gate->SignalAll();
        });
    }                                          // ...and the guard destructs on WHOEVER resumed us
    c.ran->fetch_add(1, std::memory_order_release);
}

// ARM 2: the same suspension with no guard held. Identical in every other respect, which is what
// makes it a control rather than a different test.
static void SuspendNoGuardBody(void* p) {
    auto& c = *static_cast<ArmCtx*>(p);
    c.s->WaitOnEventArmed(*c.gate, [&c] {
        if (c.released->load(std::memory_order_acquire)) c.gate->SignalAll();
    });
    c.ran->fetch_add(1, std::memory_order_release);
}

static int RunArm(TaskScheduler& sched, const char* eventName, void (*body)(void*)) {
    Event& gate = sched.GetEvent(eventName);
    std::atomic<bool> released{ false };
    std::atomic<int>  ran{ 0 };
    ArmCtx ctx{ &sched, &gate, &released, &ran };   // this frame owns it, and outlives the wait

    WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);

    g_violations.store(0, std::memory_order_relaxed);

    Task* t = sched.CreateTask(body, &ctx, Lane::Normal, TaskType::Fiber);
    if (!t) { Check(false, "CreateTask returned a task"); return -1; }
    t->waitGroup = &wg;
    sched.Push(t);

    // Let it reach the wait before releasing, so the suspension really happens rather than the
    // body running straight through.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    released.store(true, std::memory_order_release);
    gate.SignalAll();
    sched.WaitFor(wg);

    return g_violations.load(std::memory_order_relaxed);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== the epoch-guard suspend check fires, and only when it should ===\n");

    SetEpochSuspendViolationHandlerForTest(&OnViolation);
    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();

    // ---- ARM 2 FIRST, deliberately. -----------------------------------------------------------
    //
    // If the check were firing unconditionally, running the legal arm first means this file reports
    // "fires when it should not" rather than "detected a violation" -- the more useful message, and
    // it stops arm 1 from being credited for a check that cannot tell the two apart.
    const int cleanHits = RunArm(sched, "epoch_check_clean", &SuspendNoGuardBody);
    std::printf("  suspending with NO guard held: %d violation(s) reported\n", cleanHits);
    Check(cleanHits == 0,
          "a suspension outside any guard is NOT reported (the check discriminates)");

    // ---- ARM 1: the illegal shape -------------------------------------------------------------
    const int guardedHits = RunArm(sched, "epoch_check_guarded", &SuspendInsideGuardBody);
    std::printf("  suspending INSIDE an EpochGuard: %d violation(s) reported\n", guardedHits);
    Check(guardedHits >= 1,
          "suspending inside an EpochGuard IS reported (the tripwire is connected)");

    // WHY >= 1 AND NOT == 1. WaitOnEventArmed reaches ContextSwitch once, but a resumption that
    // takes the requeue path can pass a checked switch again while the guard is still live. The
    // claim this file makes is that the violation is DETECTED, not how many times a given
    // scheduling happens to route through a check -- pinning that number would make the test fail
    // on a scheduling change that broke nothing.

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
