// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// The reactor driven from PLAIN C++17 -- no coroutine, no fiber, no <coroutine> header.
//
// This exists to keep an easy claim honest. The engine takes a `Task*` to push on completion and is
// type-erased over what kind of task that is, so continuation-passing style works: heap your own
// state, embed the IoRequest in it, hand Submit a continuation task. That is the "older heap method",
// and if it ever stops compiling the claim in IoReactor.h has stopped being true.
//
// WHAT THIS IS NOT is a pleasant way to write I/O, and the difference is not cosmetic. The function
// has to be split by hand at every suspension point -- everything before the read in one place,
// everything after it in another, with the live state moved to the heap because no stack survives
// the gap. `co_await` is the compiler writing exactly that split. Two reads in sequence here means
// three functions and a state struct; in IoAsync.h it means two lines.

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "platform.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

// The hand-written frame. In the coroutine version the compiler generates this.
struct ReadState {
    JLib::IoRequest req{};
    JLib::IoResult  result{};
    char            buf[256] = {};
    std::atomic<int>* done = nullptr;
};

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::EnableIoReactor(true);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();

    std::printf("the reactor from plain C++17 -- continuation-passing, no coroutine\n");

    char path[MAX_PATH], dir[MAX_PATH];
    ::GetTempPathA(MAX_PATH, dir);
    std::snprintf(path, sizeof path, "%sjlib_io_c17.bin", dir);
    { FILE* f = std::fopen(path, "wb"); for (int i = 0; i < 1024; ++i) std::fputc(i & 0xFF, f); std::fclose(f); }

    HANDLE h = ::CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, nullptr,
                             OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
    Check(h != INVALID_HANDLE_VALUE && io.Register(h), "opened and registered the handle");

    std::atomic<int> done{ 0 };
    ReadState* st = new ReadState();     // heap: no stack survives the suspension
    st->done = &done;

    // The CONTINUATION -- the second half of a function that had to be split by hand.
    JLib::Task* cont = sched.CreateInternalTask([st] {
        st->done->store(st->result.Ok() && st->result.bytes == 256 ? 1 : 2,
                        std::memory_order_release);
    });
    // CreateInternalTask, NOT CreateTask -- a continuation like this is the internal type by intent.
    // It needs no fiber: it stores a result and returns, which is exactly the Native contract. The
    // test used to say "a plain Native continuation task" while calling CreateTask, and that stopped
    // being true silently when the everything-is-a-fiber change flipped CreateTask's default type to
    // Fiber. The COMMENT stayed right and the CODE drifted, which is the harder direction to notice.
    //
    // DEFAULT PRIORITY is the other load-bearing property, and it is unchanged by this: `hipri`
    // defaults to false either way, so this is a LOPRI task. A loPri task steered onto a reserved
    // worker lands in an inbox K never reads -- unstealable -- and hangs the pool. That is what this
    // test caught; the type was never the half that mattered.
    Check(cont != nullptr, "created a plain internal (Native) continuation at DEFAULT priority");

    // ---- THE REACTOR STAMPS AN I/O CONTINUATION AS StackClass::Tiny --------------------------
    //
    // READ BEFORE Submit, so the assertion after it is a statement about what SUBMIT DID rather
    // than about what CreateInternalTask happened to default to. Without this the check would pass
    // just as well if the constructor started returning Tiny, which is a different claim.
    const JLib::StackClass beforeSubmit = cont->stackClass;
    Check(beforeSubmit == JLib::StackClass::Standard,
          "a fresh continuation starts at Standard (else the check below proves nothing)");

    const bool immediate = io.SubmitRead(h, st->buf, 256, 0, &st->req, &st->result,
                                         cont, JLib::CancelToken{});
    Check(!immediate, "the read was queued rather than answered immediately");

    // WHY THIS MATTERS AND WHY IT IS ASSERTED RATHER THAN PRINTED: a parked I/O continuation holds
    // its stack for the whole operation. At Standard that is a 60 KB usable / 64 KB region each; at
    // Tiny it is 2 pages. StackClass::Tiny was built, sized, pooled and cached per worker for
    // exactly this caller and then went UNUSED -- nothing in the library ever asked for it, so
    // every completion parked on a Standard stack and the tiny budget was inert. This line is what
    // keeps that from silently becoming true again.
    Check(cont->stackClass == JLib::StackClass::Tiny,
          "Submit routed the continuation to a TINY stack");

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (done.load(std::memory_order_acquire) == 0 &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    Check(done.load() == 1, "the continuation ran on completion with the right bytes");

    // WHERE IT WENT, printed rather than asserted. This continuation is Lane::Normal (CreateTask's
    // default), so the FLOOR is the correct destination -- steering Normal work at a reserved worker
    // is what hung this very test until 9-02, because K never reads its Normal inbox. Reported so
    // "my I/O was handed to the pool" can be distinguished from "my I/O is broken" without guessing.
    {
        const JLib::IoRoutingStats r = JLib::ReadIoRoutingStats();
        std::printf("  routing: %llu completion(s) to the LANE, %llu to the FLOOR (K=%zu)\n",
                    (unsigned long long)r.toLane, (unsigned long long)r.toFloor,
                    JLib::TaskScheduler::GetHotWorkers());
        Check(r.toLane + r.toFloor > 0, "the routing counters saw this completion at all");
        Check(r.toLane == 0,
              "a Lane::Normal continuation went to the FLOOR, not the reserved band");
    }

    bool right = true;
    for (int i = 0; i < 256; ++i)
        if ((unsigned char)st->buf[i] != (unsigned char)(i & 0xFF)) right = false;
    Check(right, "and the buffer holds what the file holds");

    // ---- CONTROL: AN EXPLICIT StackClass MUST SURVIVE THE STAMP -----------------------------
    //
    // The check above passes just as well if Submit stamps Tiny UNCONDITIONALLY, and that would be
    // a real bug rather than a stricter version of the same rule: a caller asking for Deep is
    // saying its continuation recurses, and silently handing it two pages turns an informed choice
    // into a guard-page fault -- which is a crash, not a slowdown.
    //
    // A SECOND READ, NOT A SECOND FILE. This reuses the open handle and submits a read whose
    // completion nobody waits for; the assertion is about what Submit did to the task on the way
    // in, which has already happened by the time Submit returns. Stop() below drains it.
    {
        ReadState* ctl = new ReadState();
        ctl->done = &done;
        JLib::Task* deep = sched.CreateInternalTask([ctl] { (void)ctl; },
                                                    JLib::Lane::Normal,
                                                    JLib::CorePref::Default,
                                                    JLib::StackClass::Deep);
        Check(deep && deep->stackClass == JLib::StackClass::Deep,
              "CONTROL: CreateInternalTask honours an explicit StackClass::Deep");

        io.SubmitRead(h, ctl->buf, 256, 0, &ctl->req, &ctl->result, deep, JLib::CancelToken{});

        Check(deep->stackClass == JLib::StackClass::Deep,
              "CONTROL: Submit did NOT downgrade an explicit Deep to Tiny");
        // ctl is intentionally leaked -- its completion may still be in flight, and freeing the
        // state a queued IoRequest points at is the use-after-free this test exists near.
    }

    delete st;
    io.Stop();
    ::CloseHandle(h);
    ::DeleteFileA(path);

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
