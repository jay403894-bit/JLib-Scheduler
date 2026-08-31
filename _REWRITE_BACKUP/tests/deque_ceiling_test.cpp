// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE GROWTH CEILING MUST ACTUALLY ABORT -- a death test for TaskDeque::FatalGrow.
//
// WHY THIS FILE EXISTS. grow() replaced push_bottom's `return false` and the overflow lanes that
// used to catch it are gone, so the ceiling is now the ONLY thing standing between an infinitely
// self-spawning task and unbounded memory growth. It was written, documented, and shipped without
// anything ever executing it.
//
// AN ABORT PATH NOTHING CAN TRIGGER IS THE SAME SHAPE AS A NEGATIVE CONTROL THAT CANNOT FAIL. Both
// look like safety and provide none, and this session found four of the latter. The default ceiling
// is 65,536 slots, which a test has no business filling, so the deque is constructed with a tiny one
// -- that is exactly why maxCapacity is a constructor parameter rather than a constant.
//
// HOW A DEATH TEST WORKS WITHOUT A FRAMEWORK. std::abort() cannot be caught in-process, so the test
// re-executes ITSELF with an argument, and the parent checks how the child died. The child also
// suppresses the Windows abort dialog, or the run would block forever waiting for a human on CI.
//
// WHAT IS ASSERTED, and the second half matters as much as the first:
//
//   1. Pushing past the ceiling ABORTS. Not a silent drop, not a hang, not corruption.
//   2. Pushing UP TO the ceiling does NOT abort. Without this the test passes on a deque that
//      aborts on every push, which would be "safe" and useless -- the same vacuity in a new place.

#include "TaskDeque.h"
#include "Task.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <new>

#if defined(_WIN32)
  #include <crtdbg.h>
#endif

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {

// A DEQUE STORES TAGGED Task*, and tag() reads corePref and type off the task -- so these must be
// real Tasks at real alignment, not casts of small integers: the tag packs into the low bits that
// alignas(Task) guarantees are free.
//
// PLACEMENT NEW INTO ALIGNED STORAGE, because Task::operator new is DELETED -- tasks belong to the
// slab and the deleted operator is what stops anyone forgetting that. This test has no scheduler and
// therefore no slab, so it owns the storage itself. The tasks are never executed and never freed;
// the process is about to abort in the case under test anyway.
std::vector<JLib::Task*> MakeTasks(size_t n) {
    auto* mem = static_cast<JLib::Task*>(
        ::operator new[](sizeof(JLib::Task) * n, std::align_val_t(alignof(JLib::Task))));
    std::vector<JLib::Task*> v;
    v.reserve(n);
    for (size_t i = 0; i < n; ++i) v.push_back(::new (mem + i) JLib::Task());
    return v;
}

// Fill a deque whose ceiling is `maxCap`, starting from `initial`, with `count` pushes.
// Returns normally if every push was accepted; aborts inside grow() if the ceiling is exceeded.
void FillTo(size_t initial, size_t maxCap, size_t count) {
    JLib::TaskDeque d(initial, maxCap);
    auto tasks = MakeTasks(count);
    for (size_t i = 0; i < count; ++i) {
        if (!d.push_bottom(tasks[i])) {
            std::printf("push_bottom returned false at %zu -- unexpected\n", i);
            std::exit(2);
        }
    }
    // Reached only when nothing aborted.
    std::printf("filled %zu with ceiling %zu, no abort\n", count, maxCap);
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // ---- child modes ---------------------------------------------------------------------------
    if (argc > 1) {
#if defined(_WIN32)
        // NO DIALOG, NO WATSON. Without this the abort pops a window and CI hangs until it times
        // out -- which reads as a broken test rather than a working one.
        _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
        _CrtSetReportMode(_CRT_ASSERT, 0);
#endif
        if (std::strcmp(argv[1], "over") == 0) {
            // Ceiling 8, start at 4: one grow to 8 is allowed, the next is not. 9 pushes must abort.
            FillTo(/*initial*/ 4, /*maxCap*/ 8, /*count*/ 9);
            return 0;                       // reaching here means it did NOT abort
        }
        if (std::strcmp(argv[1], "under") == 0) {
            // Same deque, filled exactly to the ceiling. Must complete normally.
            FillTo(/*initial*/ 4, /*maxCap*/ 8, /*count*/ 8);
            return 0;
        }
        return 3;
    }

    // ---- parent --------------------------------------------------------------------------------
    std::printf("deque growth ceiling (death test)\n\n");

    char cmd[1024];

    // ABORT LOOKS DIFFERENT ON EVERY PLATFORM, so the test asks only "did it exit abnormally":
    // Windows reports 3 for abort(), POSIX shells report 134 (128 + SIGABRT). Anything non-zero and
    // not one of the test's own explicit codes means it died the way it should.
// NUL on Windows, /dev/null everywhere else -- redirecting to "nul" on POSIX silently creates a
    // FILE called nul in the working directory, which is not a failure but is litter in CI.
#if defined(_WIN32)
    const char* kDevNull = "nul";
#else
    const char* kDevNull = "/dev/null";
#endif
    std::snprintf(cmd, sizeof(cmd), "\"%s\" over > %s 2>&1", argv[0], kDevNull);
    int overRc = std::system(cmd);
    std::printf("      past the ceiling: child exit=%d\n", overRc);
    Check(overRc != 0, "pushing past the ceiling ABORTED rather than dropping or corrupting");

    // THE HALF THAT KEEPS THIS HONEST. A deque that aborted on every push would satisfy the check
    // above and be worthless; this says the ceiling is where it is claimed to be.
    std::snprintf(cmd, sizeof(cmd), "\"%s\" under > %s 2>&1", argv[0], kDevNull);
    int underRc = std::system(cmd);
    std::printf("      up to the ceiling: child exit=%d\n", underRc);
    Check(underRc == 0, "filling exactly TO the ceiling did not abort (else the test is vacuous)");

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
