// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Sockets through the reactor: a real loopback TCP connection, accepted and connected
// asynchronously, with data crossing in both directions.
//
// WHY A REAL CONNECTION rather than a mock. Accept and connect are the two operations with no
// ReadFile analogue -- they exist only as Winsock EXTENSION functions fetched at runtime -- and both
// require a fixup applied to the socket after completion (SO_UPDATE_ACCEPT_CONTEXT /
// SO_UPDATE_CONNECT_CONTEXT). Skipping that fixup does not fail: the socket looks connected and then
// misbehaves. The only way to catch it is to use the socket afterwards, so every section here sends
// or receives on the socket it just obtained.
//
// The cancellation section uses a PENDING ACCEPT -- a listener nobody connects to -- which is the
// socket equivalent of the pipe with no writer: it never completes on its own, so cancellation is
// the only thing that can end it.

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "IoAsync.h"
#include "Timer.h"
#include "platform.h"

#include <winsock2.h>
#include <ws2tcpip.h>

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

static std::atomic<int> g_acceptStatus{ -1 }, g_connectStatus{ -1 };
static std::atomic<int> g_recvStatus{ -1 }, g_recvBytes{ -1 }, g_sendBytes{ -1 };
static char g_rxbuf[256];

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    WSADATA wsa{};
    const int wsaRc = ::WSAStartup(MAKEWORD(2, 2), &wsa);

    JLib::TaskScheduler::SetReserveTimerCore(true);
    JLib::TaskScheduler::SetReserveIoCore(true);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();

    std::printf("IoReactor sockets -- workers=%zu\n\n", sched.GetWorkerCount());
    Check(wsaRc == 0, "WSAStartup succeeded (the app's job, not the library's)");

    // The extension functions have no import library; without this, accept and connect cannot be
    // called at all. It needs Winsock already up, which is why it is a separate step.
    Check(io.InitSockets(), "AcceptEx and ConnectEx resolved through WSAIoctl");

    // ---- a listener on an ephemeral loopback port ----------------------------------------------
    SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;                       // let the OS pick, so the test cannot collide
    Check(listener != INVALID_SOCKET &&
          ::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof addr) == 0 &&
          ::listen(listener, SOMAXCONN) == 0, "listener bound and listening on loopback");

    int addrLen = sizeof addr;
    ::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &addrLen);
    Check(io.RegisterSocket(listener), "listener registered with the reactor");

    // ---- accept and connect, both asynchronous, at the same time -------------------------------
    std::printf("AcceptAsync and ConnectAsync complete a real connection\n");
    SOCKET accepted = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    SOCKET client   = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    {
        // ConnectEx requires the socket to be BOUND first. Not this library's rule -- an unbound
        // socket fails with WSAEINVAL rather than binding itself.
        sockaddr_in any{};
        any.sin_family = AF_INET;
        any.sin_addr.s_addr = ::htonl(INADDR_ANY);
        any.sin_port = 0;
        Check(::bind(client, reinterpret_cast<sockaddr*>(&any), sizeof any) == 0,
              "the connecting socket was bound, as ConnectEx requires");
        Check(io.RegisterSocket(accepted) && io.RegisterSocket(client),
              "both sockets registered");

        static JLib::IoAcceptBuffer addrs;   // must outlive the await
        JLib::WaitGroup wg;
        g_acceptStatus.store(-1); g_connectStatus.store(-1);

        JLib::Spawn([](JLib::IoSocket l, JLib::IoSocket a, JLib::IoAcceptBuffer* ab) -> JLib::Coro {
            const JLib::IoResult r = co_await JLib::AcceptAsync(l, a, ab);
            g_acceptStatus.store(static_cast<int>(r.status), std::memory_order_release);
            co_return;
        }(listener, accepted, &addrs), &wg);

        static sockaddr_in target;
        target = addr;
        JLib::Spawn([](JLib::IoSocket c) -> JLib::Coro {
            const JLib::IoResult r = co_await JLib::ConnectAsync(c, &target, sizeof target);
            g_connectStatus.store(static_cast<int>(r.status), std::memory_order_release);
            co_return;
        }(client), &wg);

        sched.WaitFor(wg);
        Check(g_acceptStatus.load() == static_cast<int>(JLib::IoStatus::Completed), "the accept completed");
        Check(g_connectStatus.load() == static_cast<int>(JLib::IoStatus::Completed), "the connect completed");

        // THE FIXUP CHECK. Without SO_UPDATE_ACCEPT_CONTEXT the accepted socket does not inherit the
        // listener's properties and getpeername fails on it -- the socket looks fine until you use
        // it, which is exactly what makes omitting the fixup such a bad bug.
        sockaddr_in peer{};
        int peerLen = sizeof peer;
        Check(::getpeername(accepted, reinterpret_cast<sockaddr*>(&peer), &peerLen) == 0,
              "getpeername works on the accepted socket (SO_UPDATE_ACCEPT_CONTEXT was applied)");

        sockaddr_in cpeer{};
        int cpeerLen = sizeof cpeer;
        Check(::getpeername(client, reinterpret_cast<sockaddr*>(&cpeer), &cpeerLen) == 0,
              "and on the connected socket (SO_UPDATE_CONNECT_CONTEXT was applied)");
    }

    // ---- data actually crosses ------------------------------------------------------------------
    std::printf("SendAsync and RecvAsync move bytes over the connection\n");
    {
        std::memset(g_rxbuf, 0, sizeof g_rxbuf);
        g_recvStatus.store(-1); g_recvBytes.store(-1); g_sendBytes.store(-1);
        JLib::WaitGroup wg;

        JLib::Spawn([](JLib::IoSocket s) -> JLib::Coro {
            const JLib::IoResult r = co_await JLib::RecvAsync(s, g_rxbuf, sizeof g_rxbuf);
            g_recvStatus.store(static_cast<int>(r.status), std::memory_order_release);
            g_recvBytes.store(static_cast<int>(r.bytes), std::memory_order_release);
            co_return;
        }(accepted), &wg);

        JLib::Spawn([](JLib::IoSocket s) -> JLib::Coro {
            static const char payload[] = "over the wire, asynchronously";
            const JLib::IoResult r = co_await JLib::SendAsync(s, payload, 29);
            g_sendBytes.store(static_cast<int>(r.bytes), std::memory_order_release);
            co_return;
        }(client), &wg);

        sched.WaitFor(wg);
        Check(g_sendBytes.load() == 29, "the send reported the bytes it was given");
        Check(g_recvStatus.load() == static_cast<int>(JLib::IoStatus::Completed), "the recv completed");
        Check(g_recvBytes.load() == 29, "with the same count");
        Check(std::memcmp(g_rxbuf, "over the wire, asynchronously", 29) == 0,
              "and the bytes arrived intact");
    }

    // ---- a graceful close is a zero-byte completion ---------------------------------------------
    // Not a failure. A reader that treated FIN as an error would have to special-case the most
    // ordinary end there is.
    std::printf("a peer closing is a zero-byte completion, not an error\n");
    {
        std::memset(g_rxbuf, 0, sizeof g_rxbuf);
        g_recvStatus.store(-1); g_recvBytes.store(-1);
        JLib::WaitGroup wg;

        JLib::Spawn([](JLib::IoSocket s) -> JLib::Coro {
            const JLib::IoResult r = co_await JLib::RecvAsync(s, g_rxbuf, sizeof g_rxbuf);
            g_recvStatus.store(static_cast<int>(r.status), std::memory_order_release);
            g_recvBytes.store(static_cast<int>(r.bytes), std::memory_order_release);
            co_return;
        }(accepted), &wg);

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        ::shutdown(client, SD_SEND);
        sched.WaitFor(wg);

        Check(g_recvStatus.load() == static_cast<int>(JLib::IoStatus::Completed) &&
              g_recvBytes.load() == 0, "the recv returned Completed with zero bytes");
    }

    ::closesocket(client);
    ::closesocket(accepted);

    // ---- a PENDING ACCEPT, cancelled ------------------------------------------------------------
    // Nobody connects, so this accept never completes on its own -- the socket equivalent of a pipe
    // with no writer, and the only honest way to test cancellation on a socket.
    std::printf("a pending accept is ended by cancellation, through its completion\n");
    {
        SOCKET pending = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        Check(io.RegisterSocket(pending), "a fresh accept socket registered");

        static JLib::IoAcceptBuffer addrs2;
        JLib::CancelScope conn;
        JLib::CancelScope op(conn.Token());       // nested, as a real server would nest it
        g_acceptStatus.store(-1);
        JLib::WaitGroup wg;

        JLib::Spawn([](JLib::IoSocket l, JLib::IoSocket a, JLib::IoAcceptBuffer* ab,
                       JLib::CancelToken tok) -> JLib::Coro {
            const JLib::IoResult r = co_await JLib::AcceptAsync(l, a, ab, tok);
            g_acceptStatus.store(static_cast<int>(r.status), std::memory_order_release);
            co_return;
        }(listener, pending, &addrs2, op.Token()), &wg, 0,
          JLib::CorePref::Default, op.Token().Raw());

        Check(WaitUntil([&] { return io.InFlight() == 1; }), "the accept is in flight");
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        Check(g_acceptStatus.load() == -1, "and still suspended -- nobody has connected");

        conn.Cancel();
        const size_t asked = io.RequestCancel(conn.Token());
        Check(asked == 1, "one operation was asked to cancel, via the nested scope");

        sched.WaitFor(wg);
        Check(g_acceptStatus.load() == static_cast<int>(JLib::IoStatus::Cancelled),
              "the accept woke Cancelled");
        Check(io.InFlight() == 0, "nothing left in flight");
        ::closesocket(pending);
    }

    // ---- a Deadline on an accept ----------------------------------------------------------------
    std::printf("a Deadline times out a pending accept\n");
    {
        SOCKET pending = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        io.RegisterSocket(pending);

        static JLib::IoAcceptBuffer addrs3;
        JLib::CancelScope op;
        g_acceptStatus.store(-1);
        JLib::WaitGroup wg;

        const auto t0 = std::chrono::steady_clock::now();
        JLib::Deadline d(ms(120), op.Token(), JLib::EjectIoReactor, &io);
        JLib::Spawn([](JLib::IoSocket l, JLib::IoSocket a, JLib::IoAcceptBuffer* ab,
                       JLib::CancelToken tok) -> JLib::Coro {
            const JLib::IoResult r = co_await JLib::AcceptAsync(l, a, ab, tok);
            g_acceptStatus.store(static_cast<int>(r.status), std::memory_order_release);
            co_return;
        }(listener, pending, &addrs3, op.Token()), &wg, 0,
          JLib::CorePref::Default, op.Token().Raw());

        sched.WaitFor(wg);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t0).count();

        Check(g_acceptStatus.load() == static_cast<int>(JLib::IoStatus::Cancelled), "it timed out");
        char msg[128];
        std::snprintf(msg, sizeof msg, "and not before its deadline (%lldms)", (long long)elapsed);
        Check(elapsed >= 115, msg);
        ::closesocket(pending);
    }

    io.Stop();
    ::closesocket(listener);
    ::WSACleanup();

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
