// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE DISPATCH SEAM: how long between the reactor finishing with an operation and the coroutine
// that was waiting on it actually running.
//
//     completion thread -> Push(Task*) -> worker inbox -> worker wakes -> coroutine resumes
//
// This is the seam hybrid designs fail on, and it is INVISIBLE IN A THROUGHPUT NUMBER. The lock
// benchmark reports 2M ops/sec at 256 saturating connections -- which keeps every worker hot, and is
// therefore the load shape most likely to look good and be wrong.
//
// WHY THE COLD CASE IS THE REAL ONE. This scheduler has already measured a push->run round trip at
// ~4.3us with roughly 85% of it being the OS WAKE, and found pinned-vs-round-robin worth 5.9x. So a
// completion arriving when every worker is parked pays a full wake-up before any I/O work matters.
// Under saturation that cost disappears; under a request every few milliseconds it is the whole
// latency.
//
// So both are measured and reported side by side:
//
//   HOT   back-to-back operations, workers never park -- the flattering case
//   COLD  a pause between operations long enough for the pool to go idle -- the honest one
//
// PERCENTILES, NOT A MEAN. The failure mode is a tail: a mean hides a p99 that is ten times worse,
// and a p99 is what a request actually experiences.
//
// Needs -DJLIBSCHED_IO_LOCK_STATS=ON for the completion timestamp. Without it every sample reads
// zero and the output says so rather than printing a flattering number.
//
//   bench.exe [samples] [coldPauseMs] [completionThreads]

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "IoAsync.h"
#include "Timer.h"
#include "platform.h"

#include <winsock2.h>
#include <ws2tcpip.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <array>
#include <vector>

namespace {

JLib::IoSocket g_client = 0, g_server = 0;
char g_tx[8] = { 'p','i','n','g','!','!','!','!' };
char g_rx[8];

std::vector<long long> g_samples;
std::atomic<int> g_done{ 0 };

// One operation, timed across the dispatch seam only. `completedAtNs` is stamped by the reactor
// after the result is stored and before the resume is pushed, so the syscall and the kernel's own
// time are excluded -- what is left is Push plus the worker getting to it.
constexpr int kBurst = 32;
std::vector<JLib::IoSocket> g_bc, g_bs;
std::vector<std::array<char, 8>> g_brx;

// Burst variant: one per socket, so a single wave of sends produces kBurst completions close
// enough together that the reactor actually has something to coalesce. The one-at-a-time pass
// cannot exercise batching at all -- every batch there is size 1.
JLib::Coro BurstOp(int slot, int sample) {
    const JLib::IoResult r = co_await JLib::RecvAsync(g_bs[slot], g_brx[slot].data(), 8);
    const long long now = JLib::MonotonicNs();
    if (r.completedAtNs != 0) g_samples[sample * kBurst + slot] = now - r.completedAtNs;
    co_return;
}

JLib::Coro OneOp(int i) {
    const JLib::IoResult r = co_await JLib::RecvAsync(g_server, g_rx, sizeof g_rx);
    const long long now = JLib::MonotonicNs();
    if (r.completedAtNs != 0) g_samples[i] = now - r.completedAtNs;
    g_done.fetch_add(1, std::memory_order_release);
    co_return;
}

void Report(const char* label, std::vector<long long> s) {
    if (s.empty()) { std::printf("  %-6s no samples\n", label); return; }
    std::sort(s.begin(), s.end());
    const auto pick = [&](double p) { return s[std::min(s.size() - 1, size_t(s.size() * p)) ]; };
    std::printf("  %-6s n=%-5zu  p50 %7.2f us   p90 %7.2f us   p99 %7.2f us   max %8.2f us\n",
                label, s.size(), pick(0.50) / 1000.0, pick(0.90) / 1000.0,
                pick(0.99) / 1000.0, double(s.back()) / 1000.0);
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    const int samples = (argc > 1) ? std::atoi(argv[1]) : 400;
    const int coldMs  = (argc > 2) ? std::atoi(argv[2]) : 3;
    const unsigned th = (argc > 3) ? unsigned(std::atoi(argv[3])) : 1u;

    WSADATA wsa{};
    ::WSAStartup(MAKEWORD(2, 2), &wsa);
    JLib::TaskScheduler::EnableIoReactor(true, th);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();
    io.InitSockets();

    std::printf("io_dispatch_latency -- %d samples, cold pause %dms, %u completion thread(s), "
                "%zu workers\n\n", samples, coldMs, th, sched.GetWorkerCount());

    // One loopback pair. Concurrency is deliberately ONE: this measures the seam, not the pipe.
    SOCKET lis = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    a.sin_port = 0;
    ::bind(lis, reinterpret_cast<sockaddr*>(&a), sizeof a);
    ::listen(lis, SOMAXCONN);
    int alen = sizeof a;
    ::getsockname(lis, reinterpret_cast<sockaddr*>(&a), &alen);

    SOCKET c = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    ::connect(c, reinterpret_cast<sockaddr*>(&a), sizeof a);
    SOCKET s = ::accept(lis, nullptr, nullptr);
    io.RegisterSocket(c);
    io.RegisterSocket(s);
    g_client = static_cast<JLib::IoSocket>(c);
    g_server = static_cast<JLib::IoSocket>(s);

    for (int pass = 0; pass < 2; ++pass) {
        const bool cold = (pass == 1);
        g_samples.assign(samples, 0);

        for (int i = 0; i < samples; ++i) {
            g_done.store(0, std::memory_order_release);

            // COLD: let the pool go idle first, so the completion has to pay a real wake-up. This is
            // the whole point of the second pass -- without it every worker is already spinning and
            // the number flatters.
            if (cold) std::this_thread::sleep_for(std::chrono::milliseconds(coldMs));

            JLib::WaitGroup wg;
            JLib::Spawn(OneOp(i), &wg);

            // Give the receive time to be posted, then feed it.
            std::this_thread::sleep_for(std::chrono::microseconds(200));
            // ::send, NOT WriteFile. The socket is associated with a completion port, so a
            // synchronous WriteFile with a null OVERLAPPED on it never returns -- that hung the
            // first run of this bench.
            ::send(c, g_tx, sizeof g_tx, 0);
            sched.WaitFor(wg);
        }

        std::vector<long long> valid;
        for (long long v : g_samples) if (v > 0) valid.push_back(v);

        if (valid.empty()) {
            std::printf("  %-6s NO TIMESTAMPS -- rebuild with -DJLIBSCHED_IO_LOCK_STATS=ON.\n"
                        "         (Printing a number here would be a lie, not a result.)\n",
                        cold ? "COLD" : "HOT");
        } else {
            Report(cold ? "COLD" : "HOT", std::move(valid));
        }
    }

    std::printf("\n  HOT is workers already awake; COLD is the pool parked between operations.\n"
                "  The gap between them IS the OS wake, and COLD is what a real request pays.\n");

    // ---- BURST: kBurst completions arriving together, which is the case coalescing exists for ----
    // The passes above feed ONE operation at a time, so every batch is size 1 and batching can only
    // show its cost. This is the arrival pattern it was built for.
    {
        const int rounds = (samples / kBurst) > 0 ? (samples / kBurst) : 1;
        g_bc.resize(kBurst); g_bs.resize(kBurst); g_brx.resize(kBurst);
        for (int i = 0; i < kBurst; ++i) {
            SOCKET bc = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            ::connect(bc, reinterpret_cast<sockaddr*>(&a), sizeof a);
            SOCKET bs = ::accept(lis, nullptr, nullptr);
            io.RegisterSocket(bc); io.RegisterSocket(bs);
            g_bc[i] = static_cast<JLib::IoSocket>(bc);
            g_bs[i] = static_cast<JLib::IoSocket>(bs);
        }

        g_samples.assign(size_t(rounds) * kBurst, 0);
        for (int rd = 0; rd < rounds; ++rd) {
            std::this_thread::sleep_for(std::chrono::milliseconds(coldMs));
            JLib::WaitGroup wg;
            for (int i = 0; i < kBurst; ++i) JLib::Spawn(BurstOp(i, rd), &wg);
            std::this_thread::sleep_for(std::chrono::microseconds(400));
            // One wave, back to back, so the completions land close enough to coalesce.
            for (int i = 0; i < kBurst; ++i)
                ::send(static_cast<SOCKET>(g_bc[i]), g_tx, sizeof g_tx, 0);
            sched.WaitFor(wg);
        }

        std::vector<long long> valid;
        for (long long v : g_samples) if (v > 0) valid.push_back(v);
        if (valid.empty()) std::printf("  BURST  NO TIMESTAMPS -- needs -DJLIBSCHED_IO_LOCK_STATS=ON.\n");
        else Report("BURST", std::move(valid));

        for (int i = 0; i < kBurst; ++i) {
            ::closesocket(static_cast<SOCKET>(g_bc[i]));
            ::closesocket(static_cast<SOCKET>(g_bs[i]));
        }
    }

    io.Stop();
    ::closesocket(c); ::closesocket(s); ::closesocket(lis);
    ::WSACleanup();
    return 0;
}
