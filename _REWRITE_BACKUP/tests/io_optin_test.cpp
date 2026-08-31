// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// The service layers are OPT-IN, and this is the test that says so.
//
// This library is a JOB SYSTEM first. An app that wants only jobs must not silently acquire a timer
// thread and a completion thread it never asked for -- that is two threads over the machine, which
// costs a few percent forever and is attributed to the scheduler being slow.
//
// So this process enables NOTHING and checks two things: the pool is full size, and both layers
// refuse to run. The refusal is the important half. A warning was the first design and it was wrong:
// the failure it guards is invisible, so it has to be a hard stop at the first call.

#include "TaskScheduler.h"
#include "Timer.h"
#include "IoReactor.h"

#include <cstdio>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // Deliberately no EnableTimers, no EnableIoReactor. A plain job-system app.
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();

    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;

    std::printf("a job-system-only app pays for no service layer\n");
    Check(!JLib::TaskScheduler::TimersEnabled(), "timers are off by default");
    Check(!JLib::TaskScheduler::IoReactorEnabled(), "the I/O layer is off by default");

    char msg[128];
    std::snprintf(msg, sizeof msg, "the pool kept every core it could: %zu workers of %u hw",
                  sched.GetWorkerCount(), hw);
    Check(sched.GetWorkerCount() == static_cast<size_t>(hw - 1), msg);

    // Both layers must REFUSE rather than start a thread. A returned failure here is the whole
    // point: it is what turns an invisible few-percent regression into a two-minute fix.
    std::printf("and using a disabled layer fails rather than quietly working\n");
    {
        JLib::CancelScope s;
        const JLib::TimerHandle h = JLib::TimerQueue::Instance().Arm(10'000'000, s.Token());
        Check(!h.Valid(), "TimerQueue::Arm returned an invalid handle");

        std::this_thread::sleep_for(std::chrono::milliseconds(60));
        Check(!s.Cancelled(), "and nothing was scheduled behind it");
    }
#if defined(_WIN32)
    {
        // Register is the gate every submit path goes through, so one refusal covers the surface.
        //
        // WINDOWS ONLY, because the reactor itself is: src/win32/IoReactor.cpp is the only backend,
        // so IoReactor::Instance() does not link elsewhere. The OPT-IN property being tested is not
        // Windows-specific though -- the timer half above runs everywhere, and that is the half that
        // would regress silently. Gating the whole test out of the POSIX builds would have cost that
        // coverage to fix a link error.
        Check(!JLib::IoReactor::Instance().Register(reinterpret_cast<void*>(uintptr_t(1))),
              "IoReactor::Register refused");
        Check(!JLib::IoReactor::Instance().InitSockets(), "IoReactor::InitSockets refused");
        Check(JLib::IoReactor::Instance().InFlight() == 0, "nothing is in flight");
    }
#endif

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
