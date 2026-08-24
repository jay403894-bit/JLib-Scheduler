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
    JLib::Task* cont = sched.CreateTask([st] {
        st->done->store(st->result.Ok() && st->result.bytes == 256 ? 1 : 2,
                        std::memory_order_release);
    });
    Check(cont != nullptr, "created a plain Native continuation task");

    const bool immediate = io.SubmitRead(h, st->buf, 256, 0, &st->req, &st->result,
                                         cont, JLib::CancelToken{});
    Check(!immediate, "the read was queued rather than answered immediately");

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (done.load(std::memory_order_acquire) == 0 &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    Check(done.load() == 1, "the continuation ran on completion with the right bytes");

    bool right = true;
    for (int i = 0; i < 256; ++i)
        if ((unsigned char)st->buf[i] != (unsigned char)(i & 0xFF)) right = false;
    Check(right, "and the buffer holds what the file holds");

    delete st;
    io.Stop();
    ::CloseHandle(h);
    ::DeleteFileA(path);

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
