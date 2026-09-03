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
#include "IoFiber.h"
namespace Fib = JLib::Fib;
#include "Timer.h"
#include "platform.h"

#include "socket_compat.h"   // Winsock spelling on both platforms -- see the header

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>
#include <new>

static int g_fail = 0;

// FAILED CHECKS ARE REMEMBERED AND REPRINTED AT THE END, not just logged where they happen. A
// failure two hundred lines up scrolls away, so an intermittent one gets sampled by looking at the
// tail and comes back "no FAILED found" -- which is how a real failure got mislabelled as a
// mystery. The summary makes any failure catchable with `tail`.
static const char* g_failed[32];
static int g_failedCount = 0;

static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAILED");
    if (!c) {
        ++g_fail;
        if (g_failedCount < 32) g_failed[g_failedCount++] = what;
    }
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

// ================= FIBER BODIES: NAMED FUNCTIONS, EXPLICIT CONTEXTS =========================
//
// Every arm below used to be a lambda. They are not, and the reason is not style.
//
// A fiber's stack is a PLACE -- register state mapped to memory -- and preserving it across a
// suspension is what this runtime is for. A closure is a VALUE whose lifetime the worker loop owns:
// the task frame is freed as soon as the body returns. Give a fiber a closure to run and one of two
// things happens when it parks -- the frame is gone when it resumes, or the fiber never dies and
// the frame is never freed. Corruption or leak, and neither surfaces where it was caused, because
// SlabPool is append-only and a released slot stays mapped holding its old bytes.
//
// Passing the closure's ADDRESS was tried and is not a fix: it relocates the lifetime from
// something the compiler checks to something a reviewer has to notice, and in one pass over this
// file it produced seven sites where the body would have died while its fiber was still parked.
//
// SO: named bodies, and the state arrives in a struct the caller declares where its scope is
// visible. The pleasant side effect is that the bodies COLLAPSED -- three identical recv closures
// with different captures became one RecvBody, and six acceptor-pool closures became one.
//
// WHAT IS NOT IN THESE STRUCTS: IoRequest and IoResult. Those are locals inside each body, which
// puts them on the FIBER'S stack -- the storage that survives the wait by design, and the whole
// reason a fiber can be written to look synchronous.

struct AcceptCtx {
    JLib::TaskScheduler* s;
    JLib::IoSocket       listener, pending;
    JLib::IoAcceptBuffer* addrs;
    std::atomic<int>*    status;
    JLib::CancelToken    tok;
};
static void AcceptBody(void* p) {
    auto& c = *static_cast<AcceptCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    const JLib::IoResult r = Fib::Accept(*c.s, c.listener, c.pending, c.addrs, req, out, c.tok);
    c.status->store(static_cast<int>(r.status), std::memory_order_release);
}

struct ConnectCtx {
    JLib::TaskScheduler* s;
    JLib::IoSocket       sk;
    sockaddr_in*         target;
    std::atomic<int>*    status;
};
static void ConnectBody(void* p) {
    auto& c = *static_cast<ConnectCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    const JLib::IoResult r = Fib::Connect(*c.s, c.sk, c.target, sizeof(sockaddr_in), req, out);
    c.status->store(static_cast<int>(r.status), std::memory_order_release);
}

// Three arms shared this exact shape and differed only in the socket. `buf` is a POINTER rather
// than the body reaching for g_rxbuf directly: a body that names what it touches can be read
// without hunting for globals, and the caller decides which storage this run writes into.
struct RecvCtx {
    JLib::TaskScheduler* s;
    JLib::IoSocket       sk;
    char*                buf;
    std::uint32_t        len;
    std::atomic<int>*    status;
    std::atomic<int>*    bytes;
};
static void RecvBody(void* p) {
    auto& c = *static_cast<RecvCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    const JLib::IoResult r = Fib::Recv(*c.s, c.sk, c.buf, c.len, req, out);
    c.status->store(static_cast<int>(r.status), std::memory_order_release);
    c.bytes->store(static_cast<int>(r.bytes), std::memory_order_release);
}

struct SendCtx {
    JLib::TaskScheduler* s;
    JLib::IoSocket       sk;
    const void*          data;
    std::uint32_t        len;
    std::atomic<int>*    bytes;
};
static void SendBody(void* p) {
    auto& c = *static_cast<SendCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    const JLib::IoResult r = Fib::Send(*c.s, c.sk, c.data, c.len, req, out);
    c.bytes->store(static_cast<int>(r.bytes), std::memory_order_release);
}

// `status` and `bytes` are both OPTIONAL here -- the over-limit arm cares only about the status and
// the scatter arm only about the byte count. Null means "not asked for" rather than forcing a
// caller to supply a counter it will not read.
struct SendVCtx {
    JLib::TaskScheduler*  s;
    JLib::IoSocket        sk;
    const JLib::IoBuffer* v;
    std::uint32_t         n;
    std::atomic<int>*     bytes;
    std::atomic<int>*     status;
};
static void SendVBody(void* p) {
    auto& c = *static_cast<SendVCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    const JLib::IoResult r = Fib::SendV(*c.s, c.sk, c.v, c.n, req, out);
    if (c.bytes)  c.bytes->store(static_cast<int>(r.bytes), std::memory_order_release);
    if (c.status) c.status->store(static_cast<int>(r.status), std::memory_order_release);
}

struct RecvFromCtx {
    JLib::TaskScheduler* s;
    JLib::IoSocket       sk;
    char*                buf;
    std::uint32_t        len;
    JLib::IoAddress*     from;
    std::atomic<int>*    status;
    std::atomic<int>*    bytes;
};
static void RecvFromBody(void* p) {
    auto& c = *static_cast<RecvFromCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    const JLib::IoResult r = Fib::RecvFrom(*c.s, c.sk, c.buf, c.len, c.from, req, out);
    c.status->store(static_cast<int>(r.status), std::memory_order_release);
    c.bytes->store(static_cast<int>(r.bytes), std::memory_order_release);
}

struct SendToCtx {
    JLib::TaskScheduler* s;
    JLib::IoSocket       sk;
    const void*          data;
    std::uint32_t        len;
    sockaddr_in*         dest;
    std::atomic<int>*    bytes;
};
static void SendToBody(void* p) {
    auto& c = *static_cast<SendToCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    const JLib::IoResult r = Fib::SendTo(*c.s, c.sk, c.data, c.len, c.dest, sizeof(sockaddr_in),
                                         req, out);
    c.bytes->store(static_cast<int>(r.bytes), std::memory_order_release);
}

// SIX ACCEPTOR-POOL ARMS COLLAPSED INTO ONE. They differed in which counters they bumped and which
// cancellation scope they ran under -- all context, none of it structure. `woke` and `got` are
// optional for the same reason as SendVCtx's.
//
// THE WAITER STAYS A LOCAL, and that is the point of the whole design: IoAcceptWaiter lives on this
// FIBER'S stack, alive for exactly the wait, nothing allocated. The coroutine version put it in the
// frame; a fiber's stack is the same storage with no language feature involved.
struct AcceptPoolCtx {
    JLib::TaskScheduler* s;
    JLib::IoAcceptor*    acc;
    JLib::CancelToken    tok;
    std::atomic<int>*    woke;
    std::atomic<int>*    got;
};
static void AcceptPoolBody(void* p) {
    auto& c = *static_cast<AcceptPoolCtx*>(p);
    JLib::IoAcceptWaiter w{};
    const JLib::IoSocket s = Fib::Accept(*c.s, *c.acc, w, c.tok);
    if (c.woke) c.woke->fetch_add(1, std::memory_order_relaxed);
    if (s != 0) {
        if (c.got) c.got->fetch_add(1, std::memory_order_relaxed);
        ::closesocket(static_cast<SOCKET>(s));
    }
}

// THE TWO CONCURRENT-WRITER ARMS. These vary per fiber -- each writer owns a different payload --
// so the caller keeps an ARRAY of contexts rather than one shared struct. That array is a plain
// vector of PODs whose scope is the section, which is easier to reason about than the vector of
// closures it replaces: no reserve() subtlety, because nothing holds a pointer into it that a
// reallocation could invalidate... except that SpawnFiber does, so it still must be reserved. Said
// plainly at the call site rather than left to be rediscovered.
struct WriterCtx {
    JLib::TaskScheduler* s;
    JLib::IoStream*      stream;
    const char*          payload;
    std::uint32_t        half;
};
static void WriterBody(void* p) {
    auto& c = *static_cast<WriterCtx*>(p);
    // Both halves carry the SAME letter, so a frame whose halves disagree -- or whose bytes are not
    // all one letter -- is proof that two sends interleaved.
    const JLib::IoBuffer v[2] = {
        { (void*)c.payload,           c.half },
        { (void*)(c.payload + c.half), c.half },
    };
    JLib::IoRequest req{}; JLib::IoResult out{};
    Fib::SendV(*c.s, *c.stream, v, 2, req, out);
}

struct SenderCtx {
    JLib::TaskScheduler* s;
    JLib::IoStream*      stream;
    const char*          payload;
    std::uint32_t        len;
};
static void SenderBody(void* p) {
    auto& c = *static_cast<SenderCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};
    Fib::Send(*c.s, *c.stream, c.payload, c.len, req, out);
}

// The reuse-reconnect client: connect, disconnect with TF_REUSE_SOCKET, connect again. The longest
// body in the file and the one that most wants to be a named function -- it has three phases, three
// result slots and a comment per step.
struct ReuseClientCtx {
    JLib::TaskScheduler* s;
    JLib::IoSocket       sk;
    sockaddr_in*         tgt;
    sockaddr_in*         tgt2;
    std::atomic<int>*    phase1;
    std::atomic<int>*    disc;
    std::atomic<int>*    phase2;
};
static void ReuseClientBody(void* p) {
    auto& c = *static_cast<ReuseClientCtx*>(p);
    JLib::IoRequest req{}; JLib::IoResult out{};

    JLib::IoResult r = Fib::Connect(*c.s, c.sk, c.tgt, sizeof(sockaddr_in), req, out);
    std::printf("      [client] connect#1 status=%d err=%d\n", (int)r.status, (int)r.error);
    c.phase1->store(static_cast<int>(r.status), std::memory_order_release);
    if (r.status != JLib::IoStatus::Completed) return;

    // Reuse: the socket is NOT reusable until this completes. Waited on, not fired-and-forgotten --
    // issuing the next operation before the disconnect's completion is the classic version of this
    // race.
    //
    // A FRESH IoRequest PER OPERATION. The coroutine frame gave each await its own; on a fiber the
    // stack does the same, but only if it is re-initialised -- reusing a request the kernel has
    // already written into is the shape of bug this file exists to prevent.
    req = JLib::IoRequest{}; out = JLib::IoResult{};
    r = Fib::Disconnect(*c.s, c.sk, true, req, out);
    std::printf("      [client] disconnect status=%d err=%d\n", (int)r.status, (int)r.error);
    c.disc->store(static_cast<int>(r.status), std::memory_order_release);
    if (r.status != JLib::IoStatus::Completed) return;

    // DO NOT RE-BIND. TF_REUSE_SOCKET returns the socket to an unconnected state but leaves it
    // BOUND -- binding again is WSAEINVAL (10022), and the failed bind then takes the following
    // ConnectEx down with it. A re-bind was added here while guessing at an earlier hang and was
    // itself the bug; the sequence is disconnect, then connect.
    std::printf("      [client] connecting again\n");
    req = JLib::IoRequest{}; out = JLib::IoResult{};
    r = Fib::Connect(*c.s, c.sk, c.tgt2, sizeof(sockaddr_in), req, out);
    std::printf("      [client] connect#2 status=%d err=%d\n", (int)r.status, (int)r.error);
    c.phase2->store(static_cast<int>(r.status), std::memory_order_release);
}

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    WSADATA wsa{};
    const int wsaRc = ::WSAStartup(MAKEWORD(2, 2), &wsa);

    // ---- `pin` RUNS THE WHOLE SUITE UNDER FiberMode::Pin, AND IT IS A REGRESSION TEST --------
    //
    // A pinned fiber resumes ONLY on its home worker, which makes this the configuration where a
    // reserved worker stealing suspendable work would strand it -- if a pinned resume went to a
    // deque. It does not: Requeue sends it to resumedInboxes[home] with MarkQueuedWork and
    // NotifyWorker, and every worker drains its OWN resume inbox regardless of band. A reserved
    // worker reads it exactly like any other.
    //
    // A steal gate on the fiber mode was added here on the opposite theory and then removed once
    // the mechanism was actually checked. This arm is what keeps that decision honest: if a pinned
    // resume ever DOES start landing somewhere K cannot read, this hangs, and the watchdog below
    // dumps the band that is holding it.
    bool pinned = false;
    for (int a = 1; a < argc; ++a)
        if (std::strcmp(argv[a], "pin") == 0) pinned = true;
    if (pinned) {
        JLib::TaskScheduler::SetFiberMode(JLib::FiberMode::Pin);
        std::printf("FiberMode::Pin -- pinned resumes go to the home worker's resume inbox\n");
    }

    JLib::TaskScheduler::EnableTimers(true);
    JLib::TaskScheduler::EnableIoReactor(true);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();

    std::printf("IoReactor sockets -- workers=%zu\n\n", sched.GetWorkerCount());

    // SKIP CLEANLY WHERE THE REACTOR HAS NO BACKEND YET, rather than failing every assertion below.
    //
    // This test is built on Linux from the moment the socket-compat shim exists, which is EARLIER
    // than the Linux reactor's operations land -- deliberately, so the shim and this file's
    // portability are compiled and kept honest by CI while the backend is still being written. A
    // test that must fail until an unrelated change arrives teaches everyone to ignore it, and an
    // ignored red test is worse than no test.
    //
    // IsAvailable() is the reactor's own answer, not a platform check: it is false while the
    // operations are stubs and becomes true when they work, so this skip disappears on its own
    // without anybody remembering to remove it.
    if (!JLib::IoReactor::IsAvailable()) {
        std::printf("  SKIPPED -- IoReactor reports no backend on this platform.\n");
        std::printf("  The Linux reactor's lifecycle exists; its operations do not yet. This file\n");
        std::printf("  is compiled here so the socket shim and its portability stay verified.\n");
        JLib::detail::TeardownForTesting(sched);
        return 0;
    }

    // ---- WATCHDOG. A SUITE THIS LONG MUST NOT ANSWER A FAILURE WITH A HANG ------------------
    //
    // Every section here joins with an unbounded WaitFor, so any stall takes the whole file down
    // and reports nothing -- which is exactly what the port's first failure did: no output past
    // the section header, and no way to tell a stuck accept from a stuck scheduler. The dump is
    // the difference between "it hung" and "one task is sitting in a reserved worker's loPri
    // deque", which is what the last one turned out to be.
    static std::atomic<bool> g_finished{ false };
    std::thread watchdog([&sched] {
        const auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(90);
        while (!g_finished.load(std::memory_order_acquire)
               && std::chrono::steady_clock::now() < dl)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (!g_finished.load(std::memory_order_acquire)) {
            std::printf("\n  WATCHDOG: no progress in 90s -- dumping and aborting.\n");
            std::fflush(stdout);
            sched.DumpPoolState("io_socket_test watchdog");
            std::fflush(stdout);
            std::_Exit(3);
        }
    });
    watchdog.detach();

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

    socklen_t addrLen = sizeof addr;
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

        static JLib::IoAcceptBuffer addrs;   // must outlive the operation
        JLib::WaitGroup wg;
        g_acceptStatus.store(-1); g_connectStatus.store(-1);

        // req/out ON THE FIBER'S OWN STACK, which is the whole shape of the port. The coroutine
        // version put them in the frame; a fiber's stack does the same job and for the same reason
        // -- the kernel writes into both AFTER the submit returns, so they must outlive it. What
        // they must NOT be is a local of something that returns first.
        AcceptCtx ac{ &sched, listener, accepted, &addrs, &g_acceptStatus, JLib::CancelToken{} };
        Fib::SpawnFiber(sched, wg, &AcceptBody, &ac);

        static sockaddr_in target;
        target = addr;
        ConnectCtx cc{ &sched, client, &target, &g_connectStatus };
        Fib::SpawnFiber(sched, wg, &ConnectBody, &cc);

        sched.WaitFor(wg);
        Check(g_acceptStatus.load() == static_cast<int>(JLib::IoStatus::Completed), "the accept completed");
        Check(g_connectStatus.load() == static_cast<int>(JLib::IoStatus::Completed), "the connect completed");

        // THE FIXUP CHECK. Without SO_UPDATE_ACCEPT_CONTEXT the accepted socket does not inherit the
        // listener's properties and getpeername fails on it -- the socket looks fine until you use
        // it, which is exactly what makes omitting the fixup such a bad bug.
        sockaddr_in peer{};
        socklen_t peerLen = sizeof peer;
        Check(::getpeername(accepted, reinterpret_cast<sockaddr*>(&peer), &peerLen) == 0,
              "getpeername works on the accepted socket (SO_UPDATE_ACCEPT_CONTEXT was applied)");

        sockaddr_in cpeer{};
        socklen_t cpeerLen = sizeof cpeer;
        Check(::getpeername(client, reinterpret_cast<sockaddr*>(&cpeer), &cpeerLen) == 0,
              "and on the connected socket (SO_UPDATE_CONNECT_CONTEXT was applied)");
    }

    // ---- data actually crosses ------------------------------------------------------------------
    std::printf("SendAsync and RecvAsync move bytes over the connection\n");
    {
        std::memset(g_rxbuf, 0, sizeof g_rxbuf);
        g_recvStatus.store(-1); g_recvBytes.store(-1); g_sendBytes.store(-1);
        JLib::WaitGroup wg;

        RecvCtx rc{ &sched, accepted, g_rxbuf, sizeof g_rxbuf, &g_recvStatus, &g_recvBytes };
        Fib::SpawnFiber(sched, wg, &RecvBody, &rc);

        static const char kPayload[] = "over the wire, asynchronously";
        SendCtx sc{ &sched, client, kPayload, 29, &g_sendBytes };
        Fib::SpawnFiber(sched, wg, &SendBody, &sc);

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

        RecvCtx rc2{ &sched, accepted, g_rxbuf, sizeof g_rxbuf, &g_recvStatus, &g_recvBytes };
        Fib::SpawnFiber(sched, wg, &RecvBody, &rc2);

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        ::shutdown(client, SD_SEND);
        sched.WaitFor(wg);

        Check(g_recvStatus.load() == static_cast<int>(JLib::IoStatus::Completed) &&
              g_recvBytes.load() == 0, "the recv returned Completed with zero bytes");
    }

    ::closesocket(client);
    ::closesocket(accepted);

    // A SECOND connection, established synchronously. The async accept/connect path is already
    // proven above; this section is about what crosses the wire, so it uses the boring way to get a
    // socket pair and keeps the interesting part uncluttered.
    SOCKET accepted2 = INVALID_SOCKET, client2 = INVALID_SOCKET;
    {
        client2 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        ::connect(client2, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
        accepted2 = ::accept(listener, nullptr, nullptr);
        Check(accepted2 != INVALID_SOCKET && client2 != INVALID_SOCKET,
              "a second connection for the payload tests");
        Check(io.RegisterSocket(accepted2) && io.RegisterSocket(client2), "both registered");
    }

    // ---- scatter/gather -------------------------------------------------------------------------
    // A header and a body from separate allocations, sent in ONE syscall with no copy to join them.
    // That is most of what a protocol implementation does, and the reason vectored I/O exists.
    std::printf("SendVRawAsync writes a header and body without joining them\n");
    {
        std::memset(g_rxbuf, 0, sizeof g_rxbuf);
        g_recvStatus.store(-1); g_recvBytes.store(-1); g_sendBytes.store(-1);
        JLib::WaitGroup wg;

        RecvCtx rc3{ &sched, accepted2, g_rxbuf, sizeof g_rxbuf, &g_recvStatus, &g_recvBytes };
        Fib::SpawnFiber(sched, wg, &RecvBody, &rc3);

        // Separate allocations, never concatenated anywhere -- and declared HERE, in the caller,
        // so the vector array outlives the send the way the fiber needs it to.
        static const char hdr[]  = "LEN=5|";
        static const char body[] = "hello";
        const JLib::IoBuffer vec2[2] = { { (void*)hdr, 6 }, { (void*)body, 5 } };
        SendVCtx sv{ &sched, client2, vec2, 2, &g_sendBytes, nullptr };
        Fib::SpawnFiber(sched, wg, &SendVBody, &sv);

        sched.WaitFor(wg);
        Check(g_sendBytes.load() == 11, "the send reported both segments as one transfer");
        Check(g_recvBytes.load() == 11, "the receiver got both");
        Check(std::memcmp(g_rxbuf, "LEN=5|hello", 11) == 0,
              "and they arrived contiguous and in order");
    }

    // Refused rather than truncated: a short write that reports success is the worst outcome a
    // protocol can be handed.
    std::printf("too many segments is refused, not truncated\n");
    {
        JLib::WaitGroup wg;
        static std::atomic<int> st{ -1 };
        st.store(-1);
        char scratch[8] = {};
        JLib::IoBuffer over[JLib::IoRequest::kMaxVectors + 1];
        for (auto& b : over) { b.data = scratch; b.len = 1; }
        SendVCtx svOver{ &sched, client2, over, JLib::IoRequest::kMaxVectors + 1, nullptr, &st };
        Fib::SpawnFiber(sched, wg, &SendVBody, &svOver);
        sched.WaitFor(wg);
        Check(st.load() == static_cast<int>(JLib::IoStatus::Failed),
              "more than kMaxVectors segments failed cleanly");
    }

    ::closesocket(client2);
    ::closesocket(accepted2);

    // ---- IoStream serialises concurrent writers ------------------------------------------------
    // THE TEST THAT JUSTIFIES IoStream EXISTING. Many coroutines write framed messages to one socket
    // at the same time. Each message is sent as TWO segments -- a header and a body -- so a pair of
    // unserialised sends can interleave and produce a header immediately followed by somebody else's
    // body. The receiver reassembles and checks every frame, so interleaving shows up as a corrupt
    // frame rather than as a hang or an error code.
    //
    // Written deliberately in the shape that is WRONG without IoStream: N independent writers, no
    // ordering discipline anywhere in the caller.
    std::printf("IoStream serialises concurrent writers on one socket\n");
    {
        SOCKET c3 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        ::connect(c3, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
        SOCKET a3 = ::accept(listener, nullptr, nullptr);
        Check(a3 != INVALID_SOCKET && io.RegisterSocket(a3) && io.RegisterSocket(c3),
              "a third connection for the concurrency test");
        // Squeeze the send buffer so the provider has to break a large send up.
        int snd = 4096;
        ::setsockopt(c3, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&snd), sizeof snd);

        // WHAT THIS TEST DOES AND DOES NOT PROVE -- read before trusting it.
        //
        // It proves IoStream WORKS: 6 concurrent writers, 1.1 MB through one socket, every frame
        // delivered whole and in one piece.
        //
        // It does NOT prove IoStream is NECESSARY. The negative control -- the same test on the raw
        // unserialised overload -- was run repeatedly at 96 KB frames with SO_SNDBUF squeezed to
        // 4 KB to force the provider to split, and NEVER reproduced interleaving. An earlier version
        // used 16-byte frames and was outright vacuous; this one is merely inconclusive.
        //
        // So the justification for IoStream is MSDN.s documented warning and portability across
        // Winsock providers, NOT an observed failure here. That is a weaker case than "we saw it
        // corrupt", and it is the honest one. The read direction is the stronger argument anyway:
        // two outstanding receives on one stream socket are filled in completion order, and which
        // bytes land in which buffer is arbitrary regardless of provider.
        constexpr int kWriters = 6;
        constexpr int kHalf    = 96 * 1024;
        constexpr int kFrame   = kHalf * 2;

        // Heap, not the coroutine frame: frames come from the task slab in size classes measured in
        // bytes, and 96 KB of locals would not fit in one.
        static std::vector<char> payload[kWriters];
        for (int i = 0; i < kWriters; ++i)
            payload[i].assign(kFrame, static_cast<char>('A' + i));

        static JLib::IoStream stream{ 0 };
        stream.~IoStream();
        new (&stream) JLib::IoStream(c3);          // rebind to this connection

        // The receiver has to run CONCURRENTLY: with a 4 KB send buffer the senders block until
        // somebody drains, so receiving after WaitFor would simply deadlock.
        std::vector<char> got(static_cast<size_t>(kWriters) * kFrame);
        std::atomic<int> total{ 0 };
        std::thread drain([&] {
            int n = 0, want = kWriters * kFrame;
            while (n < want) {
                const int k = ::recv(a3, got.data() + n, want - n, 0);
                if (k <= 0) break;
                n += k;
            }
            total.store(n, std::memory_order_release);
        });

        JLib::WaitGroup wg;
        // THIS BODY VARIES PER ITERATION (it captures `i`), so one shared closure will not do -- and
        // one declared inside the loop would be destroyed while its fiber was still sending. The
        // bodies therefore get storage that outlives the loop AND the WaitFor.
        //
        // reserve() IS LOAD-BEARING, not a performance note: SpawnFiber is handed `&bodies.back()`,
        // and a reallocation would move every closure already spawned out from under the fibers
        // pointing at them. Reserving the final size up front means no reallocation happens.
        // ONE CONTEXT PER WRITER, in an array whose scope covers the WaitFor below. reserve() is
        // load-bearing: SpawnFiber is handed &writerCtx[i], and a reallocation would move contexts
        // already handed out. Sizing it up front means no reallocation happens.
        std::vector<WriterCtx> writerCtx;
        writerCtx.reserve(kWriters);
        for (int i = 0; i < kWriters; ++i) {
            writerCtx.push_back(WriterCtx{ &sched, &stream, payload[i].data(), (std::uint32_t)kHalf });
            Fib::SpawnFiber(sched, wg, &WriterBody, &writerCtx[i]);
        }
        sched.WaitFor(wg);
        drain.join();

        char m[128];
        std::snprintf(m, sizeof m, "received all %d bytes (%d)", kWriters * kFrame, total.load());
        Check(total.load() == kWriters * kFrame, m);

        // Every frame must be kFrame bytes of ONE letter. Interleaving shows up as a letter change
        // partway through a frame.
        int corrupt = 0;
        for (int f = 0; f + kFrame <= total.load(); f += kFrame) {
            const char letter = got[f];
            for (int k = 1; k < kFrame; ++k)
                if (got[f + k] != letter) { ++corrupt; break; }
        }
        std::snprintf(m, sizeof m, "every frame is intact -- no interleaving (%d corrupt)", corrupt);
        Check(corrupt == 0, m);

        ::closesocket(c3);
        ::closesocket(a3);
    }

    // The chain is OBSERVABLE, which is what distinguishes this from "the sends happened to not
    // overlap". Many writers are started at once against one stream; at least one of them has to
    // find the direction busy and be queued rather than submitted, or the serialisation is not
    // actually being exercised and the interleaving check above proves nothing.
    std::printf("concurrent sends are queued on the stream, not handed to the kernel together\n");
    {
        SOCKET c5 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        ::connect(c5, reinterpret_cast<sockaddr*>(&addr), sizeof addr);
        SOCKET a5 = ::accept(listener, nullptr, nullptr);
        Check(a5 != INVALID_SOCKET && io.RegisterSocket(a5) && io.RegisterSocket(c5),
              "a connection for the queue-depth check");

        int snd5 = 4096;
        ::setsockopt(c5, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&snd5), sizeof snd5);

        static JLib::IoStream st5{ 0 };
        st5.~IoStream();
        new (&st5) JLib::IoStream(c5);

        constexpr int kW = 8, kSz = 64 * 1024;
        static std::vector<char> pay[kW];
        for (int i = 0; i < kW; ++i) pay[i].assign(kSz, static_cast<char>('a' + i));

        static std::atomic<size_t> peakQueued{ 0 };
        peakQueued.store(0);
        // ---- THE WATCHER COUNTS ITS OWN SAMPLES, AND THAT IS NOT BOOKKEEPING ------------------
        //
        // This is a SAMPLING instrument: a bare thread polling QueuedSends() while the fibers send.
        // A peak of 0 therefore has two completely different meanings and the test could not tell
        // them apart:
        //
        //   THE REAL FINDING    the sends never queued -- the chain handed them all to the kernel
        //                       together, which is the regression this arm exists to catch.
        //   THE INSTRUMENT MISS the watcher was starved and never ran during the window. On a busy
        //                       box with 31 spinning workers this thread can go a whole millisecond
        //                       without a slice, and the sends are gone by then.
        //
        // Observed: this reported peak 6-7 on eight consecutive runs and 0 exactly once, during a
        // full-suite pass -- a distribution no real regression produces. Counting samples separates
        // the two: a watcher that took thousands of samples and saw zero every time is evidence;
        // one that took a handful is a thread that never got to look.
        std::atomic<long long> samples{ 0 };
        std::atomic<bool> stop{ false };
        std::thread watch([&] {
            while (!stop.load(std::memory_order_acquire)) {
                const size_t q = st5.QueuedSends();
                samples.fetch_add(1, std::memory_order_relaxed);
                size_t seen = peakQueued.load(std::memory_order_relaxed);
                while (q > seen && !peakQueued.compare_exchange_weak(seen, q)) {}
                std::this_thread::yield();
            }
        });

        std::vector<char> sink(static_cast<size_t>(kW) * kSz);
        std::atomic<int> got{ 0 };
        std::thread drain([&] {
            int n = 0, want = kW * kSz;
            while (n < want) {
                const int k = ::recv(a5, sink.data() + n, want - n, 0);
                if (k <= 0) break;
                n += k;
            }
            got.store(n, std::memory_order_release);
        });

        JLib::WaitGroup wg;
        // Per-iteration capture, so the same treatment as the writer loop above: storage that
        // outlives the WaitFor, reserved up front so `&back()` stays valid.
        std::vector<SenderCtx> senderCtx;
        senderCtx.reserve(kW);
        for (int i = 0; i < kW; ++i) {
            senderCtx.push_back(SenderCtx{ &sched, &st5, pay[i].data(), (std::uint32_t)kSz });
            Fib::SpawnFiber(sched, wg, &SenderBody, &senderCtx[i]);
        }
        sched.WaitFor(wg);
        stop.store(true, std::memory_order_release);
        watch.join();
        drain.join();

        // THE THRESHOLD IS NOT A TUNING KNOB. Below it the watcher provably did not observe the
        // window, so a peak of 0 carries no information and reporting it as a failure would be
        // asserting something this run did not measure. Above it, a zero peak IS the finding.
        // 200 is far below what an unstarved watcher takes (thousands in a few milliseconds) and
        // far above what a starved one manages.
        constexpr long long kMinSamples = 200;
        const long long took = samples.load();
        char m[200];
        if (peakQueued.load() == 0 && took < kMinSamples) {
            std::snprintf(m, sizeof m,
                          "  SKIPPED: the queue-depth watcher took only %lld samples and never saw\n"
                          "  the window. That is a starved sampler, not a scheduler result, so this\n"
                          "  run reports nothing about send queueing rather than a false failure.\n",
                          took);
            std::printf("%s", m);
        } else {
            std::snprintf(m, sizeof m,
                          "at least one send was queued behind another (peak depth %zu, %lld samples)",
                          peakQueued.load(), took);
            Check(peakQueued.load() >= 1, m);
        }
        Check(got.load() == kW * kSz, "and every byte still arrived");
        Check(st5.QueuedSends() == 0, "the chain drained empty");

        ::closesocket(c5);
        ::closesocket(a5);
    }

    // ---- UDP ------------------------------------------------------------------------------------
    // Separate calls because an unconnected datagram socket has no peer: every datagram carries one,
    // and WSARecv on such a socket fails outright.
    std::printf("SendToAsync and RecvFromAsync carry a datagram and its sender\n");
    {
        SOCKET a = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        SOCKET b = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

        sockaddr_in aAddr{}, bAddr{};
        aAddr.sin_family = bAddr.sin_family = AF_INET;
        aAddr.sin_addr.s_addr = bAddr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        aAddr.sin_port = bAddr.sin_port = 0;
        Check(::bind(a, reinterpret_cast<sockaddr*>(&aAddr), sizeof aAddr) == 0 &&
              ::bind(b, reinterpret_cast<sockaddr*>(&bAddr), sizeof bAddr) == 0,
              "both datagram sockets bound");

        socklen_t alen = sizeof aAddr, blen = sizeof bAddr;
        ::getsockname(a, reinterpret_cast<sockaddr*>(&aAddr), &alen);
        ::getsockname(b, reinterpret_cast<sockaddr*>(&bAddr), &blen);
        Check(io.RegisterSocket(a) && io.RegisterSocket(b), "both registered with the reactor");

        static JLib::IoAddress from;         // must outlive the await
        static sockaddr_in dest;
        dest = aAddr;
        std::memset(g_rxbuf, 0, sizeof g_rxbuf);
        g_recvBytes.store(-1);
        JLib::WaitGroup wg;

        RecvFromCtx rf{ &sched, a, g_rxbuf, sizeof g_rxbuf, &from, &g_recvStatus, &g_recvBytes };
        Fib::SpawnFiber(sched, wg, &RecvFromBody, &rf);

        std::this_thread::sleep_for(std::chrono::milliseconds(30));

        static const char kDgram[] = "one datagram";
        SendToCtx sd{ &sched, b, kDgram, 12, &dest, &g_sendBytes };
        Fib::SpawnFiber(sched, wg, &SendToBody, &sd);

        sched.WaitFor(wg);
        Check(g_recvStatus.load() == static_cast<int>(JLib::IoStatus::Completed), "the datagram arrived");
        Check(g_recvBytes.load() == 12, "whole, in one message");
        Check(std::memcmp(g_rxbuf, "one datagram", 12) == 0, "with its bytes intact");

        // The sender's address came back through IoAddress, which is the entire reason RecvFrom is
        // a separate call -- and its length was written by the kernel on completion, which is why
        // IoAddress carries its own rather than taking one by pointer.
        const sockaddr_in* peer = reinterpret_cast<const sockaddr_in*>(from.bytes);
        Check(from.len == static_cast<std::int32_t>(sizeof(sockaddr_in)),
              "the address length was filled in on completion");
        Check(peer->sin_port == bAddr.sin_port, "and it names the socket that actually sent it");

        ::closesocket(a);
        ::closesocket(b);
    }

    // ---- pre-posted accepts ---------------------------------------------------------------------
    // The point of AcceptEx is that the socket exists BEFORE the connection does. Using it
    // one-at-a-time throws that away: connections sit in the backlog while the server gets round to
    // asking, and each then waits on a socket() call.
    //
    // This connects a BURST with no acceptor coroutine running at all, then goes looking. If the
    // accepts were not already posted, those connections would still be sitting in the backlog.
    std::printf("IoAcceptor keeps accepts posted before connections arrive\n");
    {
        SOCKET lis2 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in a2{};
        a2.sin_family = AF_INET;
        a2.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        a2.sin_port = 0;
        ::bind(lis2, reinterpret_cast<sockaddr*>(&a2), sizeof a2);
        ::listen(lis2, SOMAXCONN);
        socklen_t l2 = sizeof a2;
        ::getsockname(lis2, reinterpret_cast<sockaddr*>(&a2), &l2);
        Check(io.RegisterSocket(lis2), "a second listener registered");

        constexpr unsigned kDepth = 8;
        constexpr int kConns = 6;
        static JLib::IoAcceptor acceptor;
        Check(acceptor.Start(static_cast<JLib::IoSocket>(lis2), kDepth), "acceptor started");

        char m[128];
        std::snprintf(m, sizeof m, "%u accepts are posted with nobody waiting (%zu)",
                      kDepth, acceptor.Outstanding());
        Check(acceptor.Outstanding() == kDepth, m);

        // Connect a burst. No coroutine is accepting -- the posted accepts do the work.
        std::vector<SOCKET> clients;
        for (int i = 0; i < kConns; ++i) {
            SOCKET c = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            ::connect(c, reinterpret_cast<sockaddr*>(&a2), sizeof a2);
            clients.push_back(c);
        }

        Check(WaitUntil([&] { return acceptor.Available() >= kConns; }, 5000),
              "every connection was accepted without an acceptor coroutine running");

        // Depth is restored: each completed slot re-posted itself.
        Check(WaitUntil([&] { return acceptor.Outstanding() == kDepth; }, 5000),
              "and the pool re-posted itself back to full depth");

        // Now take them through the awaitable, which is an ordinary cancellable wait.
        static std::atomic<int> taken{ 0 };
        taken.store(0);
        JLib::WaitGroup wg;
        // ONE CONTEXT SHARED BY ALL kConns FIBERS -- nothing here varies per waiter -- and declared
        // outside the loop so it outlives the WaitFor below.
        AcceptPoolCtx apc{ &sched, &acceptor, JLib::CancelToken{}, nullptr, &taken };
        for (int i = 0; i < kConns; ++i)
            Fib::SpawnFiber(sched, wg, &AcceptPoolBody, &apc);
        sched.WaitFor(wg);

        std::snprintf(m, sizeof m, "all %d were handed to waiters (%d)", kConns, taken.load());
        Check(taken.load() == kConns, m);

        for (SOCKET c : clients) ::closesocket(c);

        // Stop cancels every posted accept and DRAINS the completions before releasing the slots --
        // the kernel holds pointers into them, so anything else is the corruption this file exists
        // to prevent. It cancels by SCOPE, so it reaches this acceptor's accepts and nothing else.
        acceptor.Stop();
        Check(acceptor.Outstanding() == 0, "Stop drained every outstanding accept");
        ::closesocket(lis2);
    }

    // THE LOST WAKE THAT SHUTDOWN CAUSES. A coroutine parked in AcceptAsync when Stop() runs has to
    // be released, or it waits forever for a connection that can no longer arrive. No race is
    // involved -- there is simply no code path that wakes it -- which is what makes this half of the
    // lost-wake family easy to miss. It hung the suite before Stop learned to eject waiters.
    std::printf("Stop releases coroutines parked waiting for a connection\n");
    {
        SOCKET lis4 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in a4{};
        a4.sin_family = AF_INET;
        a4.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        a4.sin_port = 0;
        ::bind(lis4, reinterpret_cast<sockaddr*>(&a4), sizeof a4);
        ::listen(lis4, SOMAXCONN);
        io.RegisterSocket(lis4);

        static JLib::IoAcceptor acc2;
        Check(acc2.Start(static_cast<JLib::IoSocket>(lis4), 4), "an acceptor with nobody connecting");

        static std::atomic<int> woke{ 0 }, gotSocket{ 0 };
        woke.store(0); gotSocket.store(0);
        JLib::WaitGroup wg;

        constexpr int kWaiters = 3;
        // Shared by all kWaiters, and outside the loop so it outlives their parked fibers.
        AcceptPoolCtx apc2{ &sched, &acc2, JLib::CancelToken{}, &woke, &gotSocket };
        for (int i = 0; i < kWaiters; ++i)
            Fib::SpawnFiber(sched, wg, &AcceptPoolBody, &apc2);

        std::this_thread::sleep_for(std::chrono::milliseconds(80));
        Check(woke.load() == 0, "all three are parked -- nobody has connected");

        // If Stop does not eject them, WaitFor never returns and this test hangs rather than fails.
        acc2.Stop();
        sched.WaitFor(wg);

        char m[128];
        std::snprintf(m, sizeof m, "Stop woke all %d of them (%d)", kWaiters, woke.load());
        Check(woke.load() == kWaiters, m);
        Check(gotSocket.load() == 0, "and each was handed 0 rather than a socket");

        ::closesocket(lis4);
    }

    // A PARKED AcceptAsync UNDER A CANCELLED SCOPE. Two distinct things have to work, and the second
    // is the one that leaks if it does not.
    std::printf("a cancelled AcceptAsync is ejected, and never handed a live socket\n");
    {
        SOCKET lis5 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in a5b{};
        a5b.sin_family = AF_INET;
        a5b.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        a5b.sin_port = 0;
        ::bind(lis5, reinterpret_cast<sockaddr*>(&a5b), sizeof a5b);
        ::listen(lis5, SOMAXCONN);
        socklen_t l5 = sizeof a5b;
        ::getsockname(lis5, reinterpret_cast<sockaddr*>(&a5b), &l5);
        io.RegisterSocket(lis5);

        static JLib::IoAcceptor acc3;
        Check(acc3.Start(static_cast<JLib::IoSocket>(lis5), 4), "acceptor started");

        // (1) EAGER EJECTION. Cancelling the scope sets a flag; something must still eject, exactly
        // as with SchedulerSemaphore::CancelWaiters. Nothing connects here, so the eject is the only
        // thing that can end these waits.
        {
            JLib::CancelScope conn;
            JLib::CancelScope op(conn.Token());     // nested, to exercise IsWithin selection
            static std::atomic<int> woke{ 0 }, got{ 0 };
            woke.store(0); got.store(0);
            JLib::WaitGroup wg;

            // One context for all three: op.Token() does not vary, and it must outlive the fibers
            // parked on acc3.
            AcceptPoolCtx apc3{ &sched, &acc3, op.Token(), &woke, &got };
            for (int i = 0; i < 3; ++i)
                Fib::SpawnFiber(sched, wg, &AcceptPoolBody, &apc3, op.Token());
            std::this_thread::sleep_for(std::chrono::milliseconds(80));
            Check(woke.load() == 0, "three waiters parked under a nested scope");

            conn.Cancel();
            const size_t n = acc3.CancelWaiters(conn.Token());
            sched.WaitFor(wg);

            char m[128];
            std::snprintf(m, sizeof m, "cancelling the OUTER scope ejected all three (%zu)", n);
            Check(n == 3 && woke.load() == 3, m);
            Check(got.load() == 0, "and none was handed a socket");
        }

        // (2) SKIP-AT-RELEASE, which is the one that leaks. A waiter cancelled while parked must not
        // be handed a live connection when one arrives -- it is about to unwind and would drop the
        // socket. It has to be woken with 0 and the connection given to somebody who still wants it.
        {
            JLib::CancelScope dead, live;
            static std::atomic<int> deadGot{ 0 }, liveGot{ 0 }, deadWoke{ 0 };
            deadGot.store(0); liveGot.store(0); deadWoke.store(0);
            JLib::WaitGroup wg;

            AcceptPoolCtx apcDead{ &sched, &acc3, dead.Token(), &deadWoke, &deadGot };
            Fib::SpawnFiber(sched, wg, &AcceptPoolBody, &apcDead, dead.Token());

            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            AcceptPoolCtx apcLive{ &sched, &acc3, live.Token(), nullptr, &liveGot };
            Fib::SpawnFiber(sched, wg, &AcceptPoolBody, &apcLive, live.Token());

            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            // Cancel the FIRST waiter's scope but do NOT eject it -- leave it parked and cancelled,
            // at the head of the queue, so the next connection meets it first.
            dead.Cancel();

            SOCKET c = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            ::connect(c, reinterpret_cast<sockaddr*>(&a5b), sizeof a5b);

            sched.WaitFor(wg);
            ::closesocket(c);

            Check(deadWoke.load() == 1, "the cancelled waiter was woken");
            Check(deadGot.load() == 0, "but NOT given the connection -- it would have dropped it");
            Check(liveGot.load() == 1, "the live waiter behind it got it instead");
        }

        acc3.Stop();
        ::closesocket(lis5);
    }

    // A DEADLINE ON WAITING FOR A CONNECTION. Needs EjectIoAcceptor, not EjectIoReactor: the
    // reactor's eject cancels in-flight OPERATIONS, and a parked AcceptAsync is not one -- there is
    // nothing submitted to the kernel on its behalf. Pointed at the reactor this would fire, set the
    // flag, eject nobody, and leave the wait parked. Same lost-wake family as Stop forgetting its
    // waiters, arriving through the timer instead.
    std::printf("a Deadline times out a wait for a connection\n");
    {
        SOCKET lis6 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in a6{};
        a6.sin_family = AF_INET;
        a6.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        a6.sin_port = 0;
        ::bind(lis6, reinterpret_cast<sockaddr*>(&a6), sizeof a6);
        ::listen(lis6, SOMAXCONN);
        io.RegisterSocket(lis6);

        static JLib::IoAcceptor acc4;
        Check(acc4.Start(static_cast<JLib::IoSocket>(lis6), 4), "acceptor with nobody connecting");

        JLib::CancelScope op;
        static std::atomic<int> woke{ 0 }, got{ 0 };
        woke.store(0); got.store(0);
        JLib::WaitGroup wg;

        const auto t0 = std::chrono::steady_clock::now();
        JLib::Deadline d(ms(120), op.Token(), JLib::EjectIoAcceptor, &acc4);
        AcceptPoolCtx apc4{ &sched, &acc4, op.Token(), &woke, &got };
        Fib::SpawnFiber(sched, wg, &AcceptPoolBody, &apc4, op.Token());

        sched.WaitFor(wg);
        const auto el = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count();

        Check(woke.load() == 1, "the deadline ended the wait");
        Check(got.load() == 0, "with no connection, as expected");
        char m[128];
        std::snprintf(m, sizeof m, "and not before its deadline (%lldms)", (long long)el);
        Check(el >= 115, m);

        acc4.Stop();
        ::closesocket(lis6);
    }

    // ---- DisconnectEx and socket reuse ----------------------------------------------------------
    // Under a high connect rate the cost is not the transfer, it is the socket: every closesocket
    // tears down kernel structures and every socket() rebuilds them, and a connection that serves one
    // request pays both. TF_REUSE_SOCKET skips the pair.
    //
    // The check that matters is that the socket is genuinely reusable afterwards -- so it is handed
    // straight to a fresh ConnectEx, which fails outright on a socket that was merely closed.
    std::printf("DisconnectEx hands a socket back for reuse\n");
    if (!JLib::IoReactor::SupportsDisconnectReuse()) {
        // ---- SKIPPED, AND THE SKIP HAS TO COME FIRST ------------------------------------------
        //
        // Socket reuse after disconnect has no POSIX equivalent for TCP -- see
        // IoReactor::SupportsDisconnectReuse. The backend answers EOPNOTSUPP, which is correct, and
        // running the section anyway is what made this a FIVE-check failure rather than a two-check
        // one: the client connects, the disconnect is refused, and the connection is left sitting in
        // `listener`'s accept backlog. The NEXT section submits an accept expecting it to stay
        // pending, that accept completes instantly off the backlog, and all four of its checks fail
        // for a reason that has nothing to do with cancellation.
        //
        // Which is the argument for the capability query existing at all: a section that discovers
        // an unsupported operation by attempting it has already changed the state the rest of the
        // file runs in.
        std::printf("  SKIPPED -- this platform cannot return a connected socket for reuse.\n"
                    "  Not a gap: a connected TCP socket cannot be unconnected on POSIX, and the\n"
                    "  reactor reports EOPNOTSUPP rather than pretending.\n");
    } else {
        SOCKET c4 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in any{};
        any.sin_family = AF_INET;
        any.sin_addr.s_addr = ::htonl(INADDR_ANY);
        any.sin_port = 0;
        ::bind(c4, reinterpret_cast<sockaddr*>(&any), sizeof any);
        Check(io.RegisterSocket(c4), "a socket to connect, disconnect and connect again");

        // A SECOND LISTENER, on a different port, and this is the whole reason the test failed
        // before. TF_REUSE_SOCKET returns the socket unconnected but leaves it BOUND to the same
        // local port -- so reconnecting to the SAME remote endpoint recreates an identical 4-tuple
        // and collides with the previous connection sitting in TIME_WAIT. That reports as
        // ERROR_DUP_NAME (52), intermittently, depending on how fast the first one drains.
        //
        // Which is a real property of socket reuse and not a quirk of this test: a reused socket
        // must go to a DIFFERENT peer, or the local port must be released. A connection pool that
        // reuses sockets against one server has to keep that in mind.
        SOCKET lis3 = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in a3{};
        a3.sin_family = AF_INET;
        a3.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        a3.sin_port = 0;
        ::bind(lis3, reinterpret_cast<sockaddr*>(&a3), sizeof a3);
        ::listen(lis3, SOMAXCONN);
        socklen_t a3len = sizeof a3;
        ::getsockname(lis3, reinterpret_cast<sockaddr*>(&a3), &a3len);

        static sockaddr_in tgt, tgt2;
        tgt = addr;
        tgt2 = a3;
        static std::atomic<int> phase1{ -1 }, disc{ -1 }, phase2{ -1 };
        phase1.store(-1); disc.store(-1); phase2.store(-1);

        // EVERY STEP LOGGED, on the client side only, and labelled so it cannot be confused with
        // anything the server thread prints. The point is to know WHICH step stops -- a missing
        // print at the end is otherwise indistinguishable between "never got there" and "got there
        // and hung", and those have completely different causes.
        JLib::WaitGroup wg;
        // THE SEQUENTIAL ARM, and the one the port reads most naturally: three operations in a row
        // on one socket, each waiting for the last. `co_return` on a failed step becomes a plain
        // `return` -- the only structural change in the whole body.
        ReuseClientCtx rcc{ &sched, static_cast<JLib::IoSocket>(c4), &tgt, &tgt2,
                            &phase1, &disc, &phase2 };
        Fib::SpawnFiber(sched, wg, &ReuseClientBody, &rcc);

        // BOUNDED, with select. A bare blocking accept here hangs the whole suite if the second
        // connect never lands -- which is exactly what happened, and a test that hangs on failure
        // tells you nothing about what failed.
        // One accept per listener, each bounded. Logged SEPARATELY from the client so a stall can be
        // attributed: "client never asked" and "server never accepted" look identical otherwise.
        std::thread acc([&] {
            const SOCKET lis[2] = { listener, lis3 };
            for (int i = 0; i < 2; ++i) {
                fd_set r; FD_ZERO(&r); FD_SET(lis[i], &r);
                timeval tv{ 5, 0 };
                if (::select(0, &r, nullptr, nullptr, &tv) <= 0) {
                    std::printf("      [server] accept#%d timed out\n", i + 1);
                    break;
                }
                SOCKET s = ::accept(lis[i], nullptr, nullptr);
                std::printf("      [server] accept#%d %s\n", i + 1,
                            s == INVALID_SOCKET ? "FAILED" : "ok");
                if (s == INVALID_SOCKET) break;
                ::closesocket(s);
            }
        });

        sched.WaitFor(wg);
        acc.join();

        Check(phase1.load() == static_cast<int>(JLib::IoStatus::Completed), "the first connect completed");
        Check(disc.load() == static_cast<int>(JLib::IoStatus::Completed), "the disconnect completed");
        // ASSERTED, after the two bugs that made it look flaky were found and both were MINE:
        //
        //   1. A re-bind between the disconnect and the reconnect. TF_REUSE_SOCKET leaves the socket
        //      BOUND, so binding again is WSAEINVAL (10022) and takes the following ConnectEx down
        //      with it. That step was added while guessing at an earlier hang.
        //   2. Reconnecting to the SAME endpoint. Still bound to the same local port, so the second
        //      connection recreates an identical 4-tuple and collides with the first one in
        //      TIME_WAIT -- ERROR_DUP_NAME (52), intermittently, depending on how fast it drains.
        //
        // Neither was a reactor bug and neither was flakiness: with per-step logging it was 6 out of
        // 6 identical. "Flaky" was a conclusion drawn from a STALE BINARY whose diagnostic had never
        // compiled.
        Check(phase2.load() == static_cast<int>(JLib::IoStatus::Completed),
              "and the SAME socket connected again -- no closesocket, no socket()");

        ::closesocket(c4);
        ::closesocket(lis3);
    }

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

        AcceptCtx ac2{ &sched, listener, pending, &addrs2, &g_acceptStatus, op.Token() };
        Fib::SpawnFiber(sched, wg, &AcceptBody, &ac2, op.Token());

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
        AcceptCtx ac3{ &sched, listener, pending, &addrs3, &g_acceptStatus, op.Token() };
        Fib::SpawnFiber(sched, wg, &AcceptBody, &ac3, op.Token());

        sched.WaitFor(wg);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t0).count();

        Check(g_acceptStatus.load() == static_cast<int>(JLib::IoStatus::Cancelled), "it timed out");
        char msg[128];
        std::snprintf(msg, sizeof msg, "and not before its deadline (%lldms)", (long long)elapsed);
        Check(elapsed >= 115, msg);
        ::closesocket(pending);
    }

    g_finished.store(true, std::memory_order_release);   // stand the watchdog down
    io.Stop();
    ::closesocket(listener);
    ::WSACleanup();

    if (g_fail != 0) {
        std::printf("\n%d CHECK(S) FAILED:\n", g_fail);
        for (int i = 0; i < g_failedCount; ++i) std::printf("  * %s\n", g_failed[i]);
    }
    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
