// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// AN I/O COMPLETION ACTUALLY PARKS ON A TINY STACK. END TO END.
//
// io_c17_test already asserts that Submit STAMPS the resume task with StackClass::Tiny. That is half
// a claim. The stamp only matters if it survives all the way to a bound fiber, and in that test it
// does not even get the chance: its continuation is Lane::Normal, so it lands on the FLOOR, and a
// Native task on the floor runs FIBERLESS -- no fiber is bound, the tiny cache is never popped, and
// the whole per-class pool could be broken without that test noticing.
//
// THE PATH THIS FILE EXERCISES is the one a real completion takes when it matters:
//
//     Submit stamps Tiny -> completion steered to the RESERVED band (K) -> a reserved worker binds a
//     fiber to a Native task -> AcquireFiber reads task->stackClass -> pops the TINY ThreadLocalCache
//     -> the continuation runs on a 2-page stack
//
// Every link in that chain existed before the stamp did, and none of it had a consumer: the sizes,
// the guard-paged arena, the per-class caches and the batch refill were all built, provisioned and
// never drawn from. This is the test that says they are drawn from now.
//
// WHY Lane::LowLatency IS LOAD-BEARING HERE AND NOT A PREFERENCE. It is what steers the completion to
// K, and K is the only band that binds a fiber to a Native task. On the floor this file would measure
// nothing at all -- which is exactly why it reports VACUOUS rather than passing if that happens.
//
// THE DEEP ARM IS THE CONTROL, and it is what separates "the stamp works" from "everything on K gets
// a tiny fiber". Without it, a bug that handed every reserved-band task a tiny stack -- including the
// deeply-recursive ones the Deep class exists for -- would leave arm 1 green. That bug is a
// guard-page fault, not a slowdown.

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "Thread.h"
#include "Fiber.h"
#include "platform.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <windows.h>

static int g_fail = 0, g_vacuous = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-68s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

struct ReadState {
    JLib::IoRequest req{};
    JLib::IoResult  result{};
    char            buf[256] = {};
};

// What the continuation saw about ITSELF, recorded from inside it. Read after, never inferred.
struct Seen {
    std::atomic<int>    qIndex{ -1 };
    std::atomic<int>    fiberClass{ -1 };   // -1 = never ran, -2 = ran with NO fiber
    std::atomic<size_t> stackSize{ 0 };
    std::atomic<size_t> usable{ 0 };
    std::atomic<int>    done{ 0 };
};

static void RecordSelf(Seen& s) {
    JLib::Thread* w = JLib::Thread::Current();
    s.qIndex.store(w ? w->qIndex : -1, std::memory_order_relaxed);
    JLib::Fiber* f = w ? w->currentFiber : nullptr;
    if (!f) {
        s.fiberClass.store(-2, std::memory_order_relaxed);   // fiberless: the floor case
    } else {
        s.stackSize.store(f->stackSize, std::memory_order_relaxed);
        // DERIVED FROM THE PUBLIC stackSize, because GlobalFiberPool::UsableFor is private and a
        // test has no business widening an interface to look at itself. FiberStackArena leaves the
        // lowest page of every region unbacked, so usable is the region minus one guard page --
        // which is the same arithmetic Task.h states ("4 KB of stack means an 8 KB region").
        const size_t region = f->stackSize;
        const size_t pg     = JLib::platform::PageSize();
        s.usable.store(region > pg ? region - pg : 0, std::memory_order_relaxed);
        s.fiberClass.store((int)f->stackClass, std::memory_order_relaxed);
    }
    s.done.store(1, std::memory_order_release);
}

static bool WaitDone(Seen& s, int secs = 5) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(secs);
    while (s.done.load(std::memory_order_acquire) == 0
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return s.done.load(std::memory_order_acquire) == 1;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== an I/O completion parks on a TINY stack, end to end ===\n");

    // EnableIoReactor implies K=2 (see SetIoHotLane's note), which is what gives the lane a band to
    // steer into. Without a reserved worker there is nothing here to test.
    JLib::TaskScheduler::EnableIoReactor(true);

    // DEEP MUST BE PROVISIONED OR THE CONTROL CANNOT RUN, and finding that out is worth writing
    // down: deepPerComputeWorker DEFAULTS TO 0. A task that asks for StackClass::Deep under the
    // default budget does not fail and does not fall back -- AcquireFiber finds no deep fiber, the
    // task is requeued, and it spins forever without ever running. The first version of this file
    // hit exactly that: the control never ran, and the exhaustion warning that fired named the
    // STANDARD budget (64 x 29 = 1856), which points at the wrong lever entirely.
    //
    // That is a live trap for anyone taking the new StackClass parameter at face value, and it is
    // not this test's to fix -- but a test that asks for a class must provision it.
    JLib::TaskScheduler::SetFiberBudget(64, 64, /*deepPerComputeWorker*/ 1);

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io    = JLib::IoReactor::Instance();

    const size_t K = JLib::TaskScheduler::GetHotWorkers();
    const size_t page = JLib::platform::PageSize();
    std::printf("  K=%zu, page=%zu\n", K, page);
    if (K == 0) {
        std::printf("  K clamped to 0 -- nothing steers to a reserved worker, so this file is vacuous\n");
        return 1;
    }

    char path[MAX_PATH], dir[MAX_PATH];
    ::GetTempPathA(MAX_PATH, dir);
    std::snprintf(path, sizeof path, "%sjlib_io_tiny.bin", dir);
    { FILE* f = std::fopen(path, "wb"); for (int i = 0; i < 1024; ++i) std::fputc(i & 0xFF, f); std::fclose(f); }

    HANDLE h = ::CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, nullptr,
                             OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
    Check(h != INVALID_HANDLE_VALUE && io.Register(h), "opened and registered the handle");
    if (h == INVALID_HANDLE_VALUE) return 1;

    // Let the pool reach steady state -- steering reads the awake bitmap, and a cold pool takes the
    // tail fallback instead of the path under test.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // ---- ARM 1: A LANE COMPLETION BINDS A TINY FIBER ---------------------------------------
    Seen tiny;
    {
        ReadState* st = new ReadState();
        JLib::Task* cont = sched.CreateInternalTask([&tiny] { RecordSelf(tiny); },
                                                    JLib::Lane::LowLatency);
        Check(cont != nullptr && cont->stackClass == JLib::StackClass::Standard,
              "the continuation starts Standard (else the stamp below proves nothing)");

        io.SubmitRead(h, st->buf, 256, 0, &st->req, &st->result, cont, JLib::CancelToken{});
        Check(cont->stackClass == JLib::StackClass::Tiny, "Submit stamped it Tiny");

        Check(WaitDone(tiny), "the continuation ran");
    }

    const int q  = tiny.qIndex.load();
    const int fc = tiny.fiberClass.load();
    std::printf("  ran on worker q=%d (K=%zu), fiber class=%d, stack=%zu usable=%zu\n",
                q, K, fc, tiny.stackSize.load(), tiny.usable.load());

    // ---- THE VACUITY GUARD, BEFORE ANY ASSERTION ABOUT THE STACK -------------------------
    //
    // Steering is best-effort: a LowLatency completion CAN spill to the floor when K is busy, and a
    // floor Native task is fiberless. That is not a failure of the routing, but it means this run
    // observed nothing about tiny stacks -- so it must not be allowed to read as a pass.
    if (fc == -2 || (size_t)q >= K) {
        std::printf("  VACUOUS: the completion ran on the FLOOR (fiberless), so nothing here\n"
                    "           exercised the tiny cache. Re-run; if it persists, steering is the bug.\n");
        ++g_vacuous;
    }
    else {
        Check((size_t)q < K, "it ran on a RESERVED worker");
        Check(fc == (int)JLib::StackClass::Tiny,
              "and the fiber it bound is a TINY one, not a Standard one");
        Check(tiny.usable.load() >= 2 * page,
              "and that stack really has the promised 2 pages");
        Check(tiny.stackSize.load() < 64 * 1024,
              "and it is genuinely smaller than Standard (else 'tiny' is a lie)");
    }

    // ---- ARM 2 / CONTROL: AN EXPLICIT Deep CONTINUATION BINDS A DEEP FIBER ----------------
    //
    // Same lane, same reactor, same band -- the ONLY difference is the class the caller asked for.
    // If this comes back Tiny, the stamp is unconditional and a recursive continuation is one call
    // away from a guard-page fault.
    Seen deep;
    {
        ReadState* st = new ReadState();
        JLib::Task* cont = sched.CreateInternalTask([&deep] { RecordSelf(deep); },
                                                    JLib::Lane::LowLatency,
                                                    JLib::CorePref::Default,
                                                    JLib::StackClass::Deep);
        Check(cont != nullptr && cont->stackClass == JLib::StackClass::Deep,
              "CONTROL: the Deep continuation starts Deep");

        io.SubmitRead(h, st->buf, 256, 0, &st->req, &st->result, cont, JLib::CancelToken{});
        Check(cont->stackClass == JLib::StackClass::Deep,
              "CONTROL: Submit did NOT downgrade it to Tiny");

        Check(WaitDone(deep), "CONTROL: the Deep continuation ran");
    }

    const int dq = deep.qIndex.load(), dfc = deep.fiberClass.load();
    std::printf("  control ran on q=%d, fiber class=%d, stack=%zu\n",
                dq, dfc, deep.stackSize.load());

    if (dfc == -2 || (size_t)dq >= K) {
        std::printf("  VACUOUS: the control also ran on the floor -- it distinguishes nothing\n");
        ++g_vacuous;
    }
    else {
        Check(dfc == (int)JLib::StackClass::Deep,
              "CONTROL: a Deep task on K binds a DEEP fiber, so K does not simply hand out Tiny");
    }

    io.Stop();
    ::CloseHandle(h);
    ::DeleteFileA(path);

    if (g_vacuous) std::printf("\n%d arm(s) VACUOUS -- this run proved less than it looks like\n", g_vacuous);
    std::printf("\n%s\n", (g_fail || g_vacuous) ? "FAILED" : "PASSED");
    return (g_fail || g_vacuous) ? 1 : 0;
}
