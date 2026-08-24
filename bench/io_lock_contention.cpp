// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES THE REACTOR'S MUTEX MATTER? Built to answer that with a number instead of an argument.
//
// The reactor takes one internal mutex TWICE PER OPERATION -- once to link the request when it is
// submitted, once to unlink it when the completion arrives. That is the only lock on the per-I/O
// path, so it is the only candidate for a lock-free rewrite. The acceptor's lock is per-CONNECTION
// and sits between four syscalls; it is not worth measuring.
//
// WHAT IS MEASURED: `contended` counts acquisitions where a try_lock FAILED -- ones that actually
// had to wait. Total acquisitions alone would say how busy the lock is, not whether anyone queued
// behind it, and only the second question decides whether lock-free would buy anything.
//
// Needs -DJLIBSCHED_IO_LOCK_STATS=ON. Without it the counters are not compiled in and read as zero,
// which the output says explicitly rather than reporting a flattering 0%.
//
// HOW TO READ IT. The workload is deliberately the worst case for this lock: the smallest possible
// transfers, so the per-operation syscall is as cheap as it will ever be relative to the lock. Real
// I/O moves more data per operation and looks better than this. If contention is low HERE, it is
// lower everywhere.
//
//   bench.exe [connections] [rounds] [completionThreads]

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "IoAsync.h"
#include "platform.h"

#include <winsock2.h>
#include <ws2tcpip.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <vector>

namespace {

constexpr int kMsg = 8;                 // smallest useful transfer: maximise lock pressure per byte

int  g_conns  = 64;
int  g_rounds = 200;

std::vector<JLib::IoSocket> g_client, g_server;
std::vector<std::array<char, kMsg>> g_txBuf, g_rxBuf;
std::atomic<long long> g_ops{ 0 };

// One connection's traffic: `rounds` ping-pongs, two reactor operations each.
JLib::Coro RunPair(int i, int rounds) {
    for (int r = 0; r < rounds; ++r) {
        const JLib::IoResult w = co_await JLib::SendAsync(g_client[i], g_txBuf[i].data(), kMsg);
        if (!w.Ok()) break;
        const JLib::IoResult rd = co_await JLib::RecvAsync(g_server[i], g_rxBuf[i].data(), kMsg);
        if (!rd.Ok()) break;
        g_ops.fetch_add(2, std::memory_order_relaxed);
    }
    co_return;
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    if (argc > 1) g_conns  = std::atoi(argv[1]);
    if (argc > 2) g_rounds = std::atoi(argv[2]);
    const unsigned threads = (argc > 3) ? static_cast<unsigned>(std::atoi(argv[3])) : 1u;

    WSADATA wsa{};
    ::WSAStartup(MAKEWORD(2, 2), &wsa);

    JLib::TaskScheduler::EnableIoReactor(true, threads);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();
    io.InitSockets();

    std::printf("io_lock_contention -- %d connections x %d rounds, %u completion thread(s), "
                "%zu workers\n", g_conns, g_rounds, threads, sched.GetWorkerCount());

    // ---- loopback socket pairs, built synchronously; the setup is not what is being measured ----
    SOCKET lis = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(lis, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
    ::listen(lis, SOMAXCONN);
    int alen = sizeof addr;
    ::getsockname(lis, reinterpret_cast<sockaddr*>(&addr), &alen);

    g_client.resize(g_conns);
    g_server.resize(g_conns);
    g_txBuf.resize(g_conns);
    g_rxBuf.resize(g_conns);

    for (int i = 0; i < g_conns; ++i) {
        SOCKET c = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        ::connect(c, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
        SOCKET s = ::accept(lis, nullptr, nullptr);
        if (c == INVALID_SOCKET || s == INVALID_SOCKET) {
            std::printf("  setup failed at connection %d\n", i);
            return 1;
        }
        io.RegisterSocket(c);
        io.RegisterSocket(s);
        g_client[i] = static_cast<JLib::IoSocket>(c);
        g_server[i] = static_cast<JLib::IoSocket>(s);
        g_txBuf[i].fill(static_cast<char>('a' + (i % 26)));
    }

    // Counters are reset AFTER setup so the measurement covers only the traffic. Registration and
    // the synchronous connects take the lock too, and counting them would flatter the result.
    JLib::ResetIoLockStats();

    JLib::WaitGroup wg;
    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < g_conns; ++i) JLib::Spawn(RunPair(i, g_rounds), &wg);
    sched.WaitFor(wg);
    const auto t1 = std::chrono::steady_clock::now();

    const double secs = std::chrono::duration<double>(t1 - t0).count();
    const long long ops = g_ops.load();
    const JLib::IoLockStats st = JLib::ReadIoLockStats();

    std::printf("\n  operations      %lld\n", ops);
    std::printf("  elapsed         %.3f s\n", secs);
    std::printf("  throughput      %.0f ops/sec\n", secs > 0 ? ops / secs : 0.0);

    if (st.acquires == 0) {
        std::printf("\n  LOCK STATS NOT COMPILED IN -- rebuild with -DJLIBSCHED_IO_LOCK_STATS=ON.\n"
                    "  (Reporting 0%% here would be a lie, not a result.)\n");
    } else {
        const double pct = 100.0 * double(st.contended) / double(st.acquires);
        std::printf("\n  lock acquires   %llu  (%.2f per operation)\n",
                    (unsigned long long)st.acquires,
                    ops ? double(st.acquires) / double(ops) : 0.0);
        std::printf("  contended       %llu\n", (unsigned long long)st.contended);
        std::printf("  CONTENTION      %.3f%%  <- the number that decides lock-free\n", pct);
        std::printf("\n  For scale: SlabPool's shared mutex measured 0.02%% and the lock-free free\n"
                    "  list was dropped on that evidence. Same question, same bar.\n");
    }

    io.Stop();
    for (int i = 0; i < g_conns; ++i) {
        ::closesocket(static_cast<SOCKET>(g_client[i]));
        ::closesocket(static_cast<SOCKET>(g_server[i]));
    }
    ::closesocket(lis);
    ::WSACleanup();
    return 0;
}
