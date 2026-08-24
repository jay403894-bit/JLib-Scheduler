// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// IoReactor through `co_await`. C++20.
//
// The interesting half is cancellation, and a FILE is useless for it -- a local file read completes
// before you could ask it to stop, so a passing test would prove nothing. The cancellation sections
// use a NAMED PIPE with no writer: that read is genuinely pending for as long as we like, which is
// the only way to exercise the path that matters.
//
// What is under test is not "does a read work" but the property the design rests on: AN OPERATION
// ENDS ONLY ON A COMPLETION. Cancelling does not resume the coroutine -- it asks the OS, the OS
// completes the operation, and the resume happens then. If that stopped being true the kernel would
// be writing into a frame that had already been destroyed, and no ordinary test would notice.

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "IoAsync.h"
#include "Timer.h"
#include "platform.h"      // windows.h lives here and nowhere else

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

template <typename F>
static bool WaitUntil(F pred, int budgetMs = 5000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budgetMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return pred();
}

static constexpr int64_t ms(int64_t n) { return n * 1'000'000; }

// Results are captured through file-scope atomics rather than lambda captures: a coroutine's frame
// outlives the statement that spawned it, so anything it refers to has to outlive that too.
static std::atomic<int> g_status{ -1 }, g_bytes{ -1 }, g_resumed{ 0 }, g_started{ 0 };
static char g_buf[4096];

static JLib::Coro ReadOnce(void* h, std::uint32_t len, std::uint64_t off, JLib::CancelToken tok) {
    g_started.fetch_add(1, std::memory_order_relaxed);
    const JLib::IoResult r = co_await JLib::ReadAsync(h, g_buf, len, off, tok);
    g_status.store(static_cast<int>(r.status), std::memory_order_release);
    g_bytes.store(static_cast<int>(r.bytes), std::memory_order_release);
    g_resumed.fetch_add(1, std::memory_order_release);
    co_return;
}

static void Reset() {
    g_status.store(-1); g_bytes.store(-1); g_resumed.store(0); g_started.store(0);
    std::memset(g_buf, 0, sizeof g_buf);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    JLib::TaskScheduler::SetReserveTimerCore(true);
    JLib::TaskScheduler::SetReserveIoCore(true);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();

    std::printf("IoReactor via co_await -- workers=%zu\n\n", sched.GetWorkerCount());
    Check(JLib::IoReactor::IsAvailable(), "the reactor is implemented on this platform");

    char path[MAX_PATH];
    {
        char dir[MAX_PATH];
        ::GetTempPathA(MAX_PATH, dir);
        std::snprintf(path, sizeof path, "%sjlib_io_async_test.bin", dir);
        FILE* f = std::fopen(path, "wb");
        for (int i = 0; i < 4096; ++i) std::fputc(i & 0xFF, f);
        std::fclose(f);
    }

    std::printf("co_await ReadAsync on a file\n");
    {
        HANDLE h = ::CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, nullptr,
                                 OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
        Check(h != INVALID_HANDLE_VALUE, "opened the file for overlapped I/O");
        Check(io.Register(h), "registered the handle");

        Reset();
        JLib::WaitGroup wg;
        JLib::Spawn(ReadOnce(h, 256, 0, JLib::CancelToken{}), &wg);
        sched.WaitFor(wg);

        Check(g_status.load() == static_cast<int>(JLib::IoStatus::Completed), "it completed");
        Check(g_bytes.load() == 256, "with the bytes asked for");
        bool right = true;
        for (int i = 0; i < 256; ++i)
            if ((unsigned char)g_buf[i] != (unsigned char)(i & 0xFF)) right = false;
        Check(right, "and the right bytes");

        // Offsets, on the same handle, with no seeking.
        Reset();
        JLib::WaitGroup wg2;
        JLib::Spawn(ReadOnce(h, 16, 1000, JLib::CancelToken{}), &wg2);
        sched.WaitFor(wg2);
        Check(g_bytes.load() == 16 && (unsigned char)g_buf[0] == (unsigned char)(1000 & 0xFF),
              "a read at an offset landed where it was asked to");

        // End of file is a zero-byte COMPLETION, not a failure -- otherwise every reader has to
        // special-case a normal outcome.
        Reset();
        JLib::WaitGroup wg3;
        JLib::Spawn(ReadOnce(h, 16, 1u << 20, JLib::CancelToken{}), &wg3);
        sched.WaitFor(wg3);
        Check(g_status.load() == static_cast<int>(JLib::IoStatus::Completed) && g_bytes.load() == 0,
              "reading past the end is a zero-byte completion");

        ::CloseHandle(h);
    }

    // A pipe with a connected client and nobody writing: the read never completes on its own, so
    // cancellation is the only thing that can end it.
    const char* pipeName = "\\\\.\\pipe\\jlib_io_async_test";
    HANDLE server = ::CreateNamedPipeA(pipeName, PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED,
                                       PIPE_TYPE_BYTE | PIPE_WAIT, 1, 4096, 4096, 0, nullptr);
    HANDLE client = ::CreateFileA(pipeName, GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                                  OPEN_EXISTING, 0, nullptr);
    ::ConnectNamedPipe(server, nullptr);      // client already there: ERROR_PIPE_CONNECTED

    std::printf("a pending co_await is ended by cancellation, through its COMPLETION\n");
    {
        Check(server != INVALID_HANDLE_VALUE && client != INVALID_HANDLE_VALUE, "pipe pair open");
        Check(io.Register(server), "registered the pipe");

        JLib::CancelScope conn;
        JLib::CancelScope op(conn.Token());        // nested, as a real operation would be
        Reset();
        JLib::WaitGroup wg;
        JLib::Spawn(ReadOnce(server, 64, 0, op.Token()), &wg, 0,
                    JLib::CorePref::Default, op.Token().Raw());

        Check(WaitUntil([&] { return io.InFlight() == 1; }), "the read is in flight");
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        Check(g_resumed.load() == 0, "and still suspended -- nothing has been written");

        // Cancel the OUTER scope. The operation carries the inner one and inherits, which only
        // works because RequestCancel asks IsWithin rather than comparing tokens.
        conn.Cancel();
        const size_t asked = io.RequestCancel(conn.Token());
        Check(asked == 1, "one operation was asked to cancel, via the nested scope");

        sched.WaitFor(wg);
        Check(g_status.load() == static_cast<int>(JLib::IoStatus::Cancelled), "it woke Cancelled");
        Check(g_bytes.load() == 0, "reporting no bytes transferred");
        Check(io.InFlight() == 0, "the kernel handed the request back before the frame went away");
    }

    // The shape a real I/O timeout takes. EjectIoReactor does NOT resume anything, unlike every
    // other eject in Timer.h -- it asks the OS, and the await ends on the completion.
    std::printf("a Deadline times out a pending co_await\n");
    {
        JLib::CancelScope op;
        Reset();
        JLib::WaitGroup wg;

        const auto t0 = std::chrono::steady_clock::now();
        JLib::Deadline d(ms(120), op.Token(), JLib::EjectIoReactor, &io);
        JLib::Spawn(ReadOnce(server, 64, 0, op.Token()), &wg, 0,
                    JLib::CorePref::Default, op.Token().Raw());
        sched.WaitFor(wg);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t0).count();

        Check(g_status.load() == static_cast<int>(JLib::IoStatus::Cancelled), "it timed out");
        char msg[128];
        std::snprintf(msg, sizeof msg, "and not before its deadline (%lldms)", (long long)elapsed);
        Check(elapsed >= 115, msg);
        Check(io.InFlight() == 0, "nothing left in flight");
    }

    // The ordinary case, and the one that breaks if an eject ever resumes eagerly: data arrives, the
    // read completes, and the RAII Deadline takes its own timer out of the wheel on the way past.
    std::printf("a read that beats its deadline completes normally\n");
    {
        JLib::CancelScope op;
        Reset();
        JLib::WaitGroup wg;

        std::thread writer([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            DWORD wrote = 0;
            ::WriteFile(client, "hello, reactor", 14, &wrote, nullptr);
        });

        {
            JLib::Deadline d(ms(3000), op.Token(), JLib::EjectIoReactor, &io);
            JLib::Spawn(ReadOnce(server, 64, 0, op.Token()), &wg, 0,
                        JLib::CorePref::Default, op.Token().Raw());
            sched.WaitFor(wg);
        }
        writer.join();

        Check(g_status.load() == static_cast<int>(JLib::IoStatus::Completed), "it completed");
        Check(g_bytes.load() == 14, "with the bytes that were written");
        Check(std::memcmp(g_buf, "hello, reactor", 14) == 0, "and the right ones");
        Check(!op.Cancelled(), "the scope was never cancelled");
    }

    // WriteAsync had NO coverage until this section -- it was implemented and shipped in the same
    // breath as the read path and never once exercised, which is exactly how a mirrored code path
    // rots. The pipe is read back synchronously from the client end, so the check is that the bytes
    // actually crossed rather than that the call returned.
    std::printf("co_await WriteAsync puts bytes on the wire\n");
    {
        JLib::WaitGroup wg;
        static std::atomic<int> wstatus{ -1 }, wbytes{ -1 };
        wstatus.store(-1); wbytes.store(-1);

        JLib::Spawn([](void* h) -> JLib::Coro {
            static const char payload[] = "written by the reactor";
            const JLib::IoResult r = co_await JLib::WriteAsync(h, payload, 22, 0);
            wstatus.store(static_cast<int>(r.status), std::memory_order_release);
            wbytes.store(static_cast<int>(r.bytes), std::memory_order_release);
            co_return;
        }(server), &wg);
        sched.WaitFor(wg);

        Check(wstatus.load() == static_cast<int>(JLib::IoStatus::Completed), "the write completed");
        Check(wbytes.load() == 22, "reporting the byte count it was given");

        char got[32] = {};
        DWORD read = 0;
        const BOOL ok = ::ReadFile(client, got, 22, &read, nullptr);
        Check(ok && read == 22, "the other end received them");
        Check(std::memcmp(got, "written by the reactor", 22) == 0, "and they are the right bytes");
    }

    // An already-cancelled scope must never reach the kernel: nothing is submitted, so there is no
    // completion to wait for. The ONE case where an I/O cancel is immediate.
    std::printf("an already-cancelled scope never submits\n");
    {
        JLib::CancelScope op;
        op.Cancel();
        Reset();
        const size_t before = io.InFlight();
        JLib::WaitGroup wg;
        JLib::Spawn(ReadOnce(server, 64, 0, op.Token()), &wg);
        sched.WaitFor(wg);

        Check(g_status.load() == static_cast<int>(JLib::IoStatus::Cancelled), "it returned Cancelled");
        Check(io.InFlight() == before, "and nothing was ever put in flight");
        Check(g_resumed.load() == 1, "the coroutine still ran to completion");
    }

    // MANY concurrent operations -- the thing fibers could not have done. Each is a coroutine frame
    // of a couple of hundred bytes, not a 64 KB stack from a capped pool.
    std::printf("hundreds of concurrent reads, all cancelled together\n");
    {
        constexpr int kN = 512;
        JLib::CancelScope scope;
        JLib::WaitGroup wg;
        Reset();

        for (int i = 0; i < kN; ++i)
            JLib::Spawn(ReadOnce(server, 8, 0, scope.Token()), &wg, 0,
                        JLib::CorePref::Default, scope.Token().Raw());

        Check(WaitUntil([&] { return io.InFlight() == kN; }, 8000), "all of them are in flight");

        char m[128];
        std::snprintf(m, sizeof m, "%d concurrent operations, which the fiber pool could not hold", kN);
        Check(io.InFlight() == kN, m);

        scope.Cancel();
        const size_t asked = io.RequestCancel(scope.Token());
        sched.WaitFor(wg);

        std::snprintf(m, sizeof m, "all %d were asked (got %zu) and every coroutine resumed (got %d)",
                      kN, asked, g_resumed.load());
        Check(asked == kN && g_resumed.load() == kN, m);
        Check(io.InFlight() == 0, "the in-flight list is empty again");
    }

    io.Stop();
    ::CloseHandle(client);
    ::CloseHandle(server);
    ::DeleteFileA(path);

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
