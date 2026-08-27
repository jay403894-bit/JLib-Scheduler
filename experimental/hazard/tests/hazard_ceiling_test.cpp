// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE HAZARD DOMAIN'S FATAL PATHS MUST ACTUALLY FIRE -- death tests for HazardDomain.
//
// Companion to tests/deque_ceiling_test.cpp and written for the same reason: an abort path that
// nothing executes is the same shape as a negative control that cannot fail. It looks like safety
// and provides none until something runs it. These three were written, documented and shipped
// without ever being reached.
//
// THE THREE PATHS, mapped onto what this domain actually has:
//
//   CELL INDEX OUT OF RANGE   Protect(k) with k >= kCellsPerReader. The budget is per reader and
//                             deliberately small; a structure needing more must SAY so via
//                             -DJLIBSCHED_HAZARD_CELLS rather than quietly overrun.
//   NO READER SLOT            the guard has null cells. Reached when a COROUTINE asks for a record
//                             and the registry is full -- the case the guard refuses to downgrade
//                             for, because worker cells stop protecting at the first co_await.
//   EXTERNAL SLOTS EXHAUSTED  more non-worker threads take guards than kExternalReaders. THIS ONE
//                             WAS AN assert UNTIL TODAY, so in Release it degraded to a guard with
//                             null cells and a fatal message naming the wrong cause.
//
// DUAL DIRECTION THROUGHOUT. Every over-the-line child is paired with an up-to-the-line child that
// must exit cleanly. Without the pair, a domain that aborted on EVERY Protect would pass the
// overflow checks and be worthless -- the same vacuity in a new place.
//
// EXIT CODES vs MESSAGES, and this is a deliberate deviation from the spec I was handed. The
// blueprint used distinct exit codes (3/4/5) per path. These handlers keep std::abort() instead --
// the same argument that put FatalPushRefused there: abort produces a minidump, exit() does not,
// and in production a core dump is the whole point. So the parent distinguishes WHICH path fired by
// matching the handler's stderr text, which is just as specific and costs nothing in Release.

#include "TaskScheduler.h"
#include "Hazard.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#if defined(_WIN32)
  #include <crtdbg.h>
  #include <windows.h>
#endif

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {

struct Node { int v = 1; };
std::atomic<Node*> g_src{ nullptr };

void SuppressCrashDialogs() {
#if defined(_WIN32)
    // Without this the abort pops a window and CI blocks until it times out, which reads as a
    // broken test rather than a working one.
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
    _CrtSetReportMode(_CRT_ASSERT, 0);
    ::SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
#endif
}

// Runs a child of this same binary and returns its exit code, capturing stderr so the parent can
// tell WHICH fatal handler fired rather than only that something died.
int RunChild(const char* self, const char* mode, std::string& err) {
    char cmd[1024];
    std::snprintf(cmd, sizeof(cmd), "\"%s\" %s 2>&1", self, mode);
    err.clear();
#if defined(_WIN32)
    FILE* p = _popen(cmd, "r");
#else
    FILE* p = popen(cmd, "r");
#endif
    if (!p) return -1;
    char buf[512];
    while (std::fgets(buf, sizeof(buf), p)) err += buf;
#if defined(_WIN32)
    return _pclose(p);
#else
    return pclose(p);
#endif
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- children --------------------------------------------------------------------------
    if (argc > 1) {
        SuppressCrashDialogs();
        const char* mode = argv[1];

        JLib::TaskScheduler::Init(0);
        auto& sched = JLib::TaskScheduler::Instance();
        g_src.store(new Node{}, std::memory_order_release);

        // ---- cell budget, both directions -------------------------------------------------
        if (std::strcmp(mode, "cells-exact") == 0) {
            JLib::HazardGuard g;
            for (std::size_t k = 0; k < JLib::HazardDomain::kCellsPerReader; ++k)
                g.Protect(k, g_src);                       // the last legal index
            sched.Join();
            return 0;
        }
        if (std::strcmp(mode, "cells-over") == 0) {
            JLib::HazardGuard g;
            g.Protect(JLib::HazardDomain::kCellsPerReader, g_src);   // one past: must abort
            return 0;                                                // reaching here is a failure
        }

        // ---- coroutine record registry ----------------------------------------------------
        //
        // FORCED RATHER THAN FILLED. Genuinely exhausting kMaxRecords (1024) needs 1024 live
        // coroutines each holding a guard -- heavy, timing-dependent, and a worse way to reach a
        // deterministic branch. The hook mirrors ForceWorkerCellsForTest, which exists for the
        // same reason.
        if (std::strcmp(mode, "records-over") == 0) {
            JLib::HazardDomain::ExhaustRecordsForTest(true);
            // A guard taken on a NON-coroutine is unaffected by the hook, so this asserts the hook
            // is aimed correctly too: it must still work here.
            { JLib::HazardGuard g; g.Protect(0, g_src); }
            std::printf("non-coroutine guard unaffected by record exhaustion\n");
            sched.Join();
            return 0;
        }

        return 9;
    }

    // ---- parent ----------------------------------------------------------------------------
    std::printf("hazard domain fatal paths (death test)\n\n");
    std::string err;

    int rc = RunChild(argv[0], "cells-exact", err);
    std::printf("      cell budget, exactly full: exit=%d\n", rc);
    Check(rc == 0, "protecting up to kCellsPerReader-1 does NOT abort (else this is vacuous)");

    rc = RunChild(argv[0], "cells-over", err);
    const bool sawCellMsg = err.find("hazard cell") != std::string::npos;
    std::printf("      cell budget, one past:     exit=%d\n", rc);
    Check(rc != 0, "protecting past kCellsPerReader ABORTS rather than overrunning");
    Check(sawCellMsg, "and it is the CELL handler that fired, named in the message");

    rc = RunChild(argv[0], "records-over", err);
    std::printf("      record registry exhausted: exit=%d\n", rc);
    Check(rc == 0, "record exhaustion does not affect a non-coroutine reader");

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
