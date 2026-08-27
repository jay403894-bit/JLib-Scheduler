// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// UDP RECT -- a rectangle you move over the network, both ends in one binary.
//
//   udp_rect            the receiver: opens a window, binds 127.0.0.1:45454, moves the rect
//   udp_rect --send     the sender: arrow keys, one datagram per press, q to quit
//
// Run the first, then the second in another console. Both ends are on loopback, so there is
// nothing to configure and no second machine to find.
//
// == WHY THIS SAMPLE EXISTS ==
//
// The socket test sends exactly ONE datagram and checks it arrived. That proves the plumbing and
// nothing about the shape real code has: a receive LOOP that outlives thousands of packets, cancels
// cleanly on shutdown, and hands its results to a thread that never touches the reactor. This is
// that shape, small enough to read in one sitting.
//
// == THE WIRE FORMAT IS ABSOLUTE POSITION, NOT A DELTA, AND THAT IS THE LESSON ==
//
// Arrow keys are deltas, so sending deltas is the obvious design and it is wrong. UDP does not
// promise delivery: drop one delta and the two ends disagree about where the rect is FOREVER,
// because nothing later ever corrects it. Absolute position is idempotent -- a dropped packet costs
// one frame of smoothness and the next packet repairs it. That is why real game netcode replicates
// STATE and not input events, and you can watch the difference here by dropping packets on purpose
// with --drop.
//
// The sequence number is the other half. UDP does not promise ORDER either, so a late packet can
// arrive after a newer one and drag the rect backwards. The receiver keeps the highest sequence it
// has seen and discards anything not newer, which is the smallest possible version of what every
// replication layer does. On loopback reordering is rare; --drop makes the gaps visible in the
// title bar so you can watch the counter move.
//
// == WHAT IT DEMONSTRATES ABOUT THE SCHEDULER ==
//
// The receive loop is a coroutine on the worker pool. It parks on the reactor between datagrams and
// occupies NO thread while it waits -- the window's message pump has the main thread to itself, and
// the workers stay free for whatever else the app is doing. The coroutine touches no UI state: it
// publishes to atomics and PostMessage()s the window, which is the one Win32 call documented to be
// safe from a thread that does not own it. Shutdown is a cancel scope, not a flag: cancelling makes
// the parked RecvFrom return Cancelled, the loop breaks, and the WaitGroup reaches zero before the
// scheduler shuts down. Nothing is left parked on a socket that is about to close.

#include <winsock2.h>       // before windows.h, or windows.h pulls in winsock 1
#include <ws2tcpip.h>
#include <windows.h>

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "IoAsync.h"
#include "Coroutine.h"
#include "CancelToken.h"

#include <atomic>
#include <conio.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

constexpr std::uint16_t kPort  = 45454;
constexpr int           kWinW  = 640;
constexpr int           kWinH  = 480;
constexpr int           kRect  = 48;
constexpr int           kStep  = 12;
constexpr UINT          kWmPkt = WM_APP + 1;    // "a packet landed, repaint"

// 12 bytes on the wire. Both ends are the same binary on the same machine, so there is no byte
// order or padding question to answer -- a real protocol would have to fix both explicitly.
#pragma pack(push, 1)
struct Packet {
    std::uint32_t seq;
    std::int32_t  x;
    std::int32_t  y;
};
#pragma pack(pop)
static_assert(sizeof(Packet) == 12, "the wire format is 12 bytes");

// Written by the receive coroutine on a worker, read by WM_PAINT on the main thread. Relaxed is
// enough: they are independent scalars and a torn frame would be a pixel, not a bug.
std::atomic<int>           g_x{ kWinW / 2 - kRect / 2 };
std::atomic<int>           g_y{ kWinH / 2 - kRect / 2 };
std::atomic<std::uint32_t> g_seen{ 0 };      // packets accepted
std::atomic<std::uint32_t> g_stale{ 0 };     // packets discarded as not-newer
std::atomic<std::uint32_t> g_gaps{ 0 };      // sequence numbers that never arrived

// ---- receiver ------------------------------------------------------------------------------------

JLib::Coro RecvLoop(JLib::IoSocket s, HWND hwnd, JLib::CancelToken tok) {
    // Both live in the COROUTINE FRAME, which is exactly what the reactor asks for: the kernel holds
    // pointers to `from` and to its length field across the await, and the frame is what survives
    // it. The socket test uses statics only because a lambda cannot hold them; a real loop should
    // not copy that.
    JLib::IoAddress from{};
    char            buf[64];
    std::uint32_t   highest = 0;
    bool            first   = true;

    for (;;) {
        const JLib::IoResult r = co_await JLib::RecvFromAsync(s, buf, sizeof buf, &from, 0, tok);

        // Cancelled is the shutdown path, Failed is a dead socket. Neither is a reason to spin.
        if (r.status != JLib::IoStatus::Completed) break;

        // A datagram is a MESSAGE. A short one is a malformed message, not a partial read to
        // accumulate -- there is no stream here to resume from.
        if (r.bytes != sizeof(Packet)) continue;

        Packet p{};
        std::memcpy(&p, buf, sizeof p);

        // Not-newer means late or duplicated. Discarding it is the whole ordering policy.
        if (!first && p.seq <= highest) {
            g_stale.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        if (!first && p.seq > highest + 1)
            g_gaps.fetch_add(p.seq - highest - 1, std::memory_order_relaxed);

        highest = p.seq;
        first   = false;

        g_x.store(p.x, std::memory_order_relaxed);
        g_y.store(p.y, std::memory_order_relaxed);
        g_seen.fetch_add(1, std::memory_order_relaxed);

        // The one Win32 call documented to be safe from a thread that does not own the window.
        // InvalidateRect from here would be a bug on a good day and a hang on a bad one.
        ::PostMessage(hwnd, kWmPkt, 0, 0);
    }
    co_return;
}

LRESULT CALLBACK WndProc(HWND h, UINT m, WPARAM w, LPARAM l) {
    switch (m) {
    case kWmPkt: {
        ::InvalidateRect(h, nullptr, TRUE);
        char title[160];
        std::snprintf(title, sizeof title,
                      "JLib UDP rect  --  port %u   accepted %u   stale %u   lost %u",
                      static_cast<unsigned>(kPort),
                      g_seen.load(std::memory_order_relaxed),
                      g_stale.load(std::memory_order_relaxed),
                      g_gaps.load(std::memory_order_relaxed));
        ::SetWindowTextA(h, title);
        return 0;
    }
    case WM_PAINT: {
        PAINTSTRUCT ps{};
        HDC dc = ::BeginPaint(h, &ps);
        RECT full{};
        ::GetClientRect(h, &full);
        ::FillRect(dc, &full, reinterpret_cast<HBRUSH>(::GetStockObject(BLACK_BRUSH)));

        const int x = g_x.load(std::memory_order_relaxed);
        const int y = g_y.load(std::memory_order_relaxed);
        RECT   box{ x, y, x + kRect, y + kRect };
        HBRUSH brush = ::CreateSolidBrush(RGB(90, 200, 250));
        ::FillRect(dc, &box, brush);
        ::DeleteObject(brush);

        ::EndPaint(h, &ps);
        return 0;
    }
    case WM_DESTROY:
        ::PostQuitMessage(0);
        return 0;
    }
    return ::DefWindowProc(h, m, w, l);
}

int RunReceiver() {
    JLib::TaskScheduler::EnableIoReactor(true);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io    = JLib::IoReactor::Instance();

    SOCKET s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == INVALID_SOCKET) {
        std::printf("socket() failed: %d\n", ::WSAGetLastError());
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port        = ::htons(kPort);
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) {
        std::printf("bind(127.0.0.1:%u) failed: %d -- is a receiver already running?\n",
                    static_cast<unsigned>(kPort), ::WSAGetLastError());
        return 1;
    }
    if (!io.RegisterSocket(s)) {
        std::printf("RegisterSocket failed\n");
        return 1;
    }

    WNDCLASSA wc{};
    wc.lpfnWndProc   = &WndProc;
    wc.hInstance     = ::GetModuleHandleA(nullptr);
    wc.lpszClassName = "JLibUdpRect";
    wc.hCursor       = ::LoadCursor(nullptr, IDC_ARROW);
    ::RegisterClassA(&wc);

    RECT want{ 0, 0, kWinW, kWinH };
    ::AdjustWindowRect(&want, WS_OVERLAPPEDWINDOW, FALSE);
    HWND hwnd = ::CreateWindowA("JLibUdpRect", "JLib UDP rect  --  waiting for packets",
                                WS_OVERLAPPEDWINDOW, CW_USEDEFAULT, CW_USEDEFAULT,
                                want.right - want.left, want.bottom - want.top,
                                nullptr, nullptr, wc.hInstance, nullptr);
    if (!hwnd) {
        std::printf("CreateWindow failed\n");
        return 1;
    }
    ::ShowWindow(hwnd, SW_SHOW);

    // The scope is what ends the loop. Cancelling it completes the PARKED RecvFrom with Cancelled,
    // which is the only way to get a coroutine off a socket that has received nothing.
    JLib::CancelScope scope;
    JLib::WaitGroup   wg;
    JLib::Spawn(RecvLoop(s, hwnd, scope.Token()), &wg);

    std::printf("listening on 127.0.0.1:%u -- run `udp_rect --send` in another console\n",
                static_cast<unsigned>(kPort));

    MSG msg{};
    while (::GetMessage(&msg, nullptr, 0, 0) > 0) {
        ::TranslateMessage(&msg);
        ::DispatchMessage(&msg);
    }

    // Order matters: cancel, then drain, THEN close. Closing first would leave the reactor holding a
    // request against a dead handle.
    // TWO CALLS, AND THE SECOND ONE IS NOT OPTIONAL. This is the part of the API most likely to be
    // got wrong, and it hangs rather than failing, so it is worth being explicit about.
    //
    // scope.Cancel() marks the scope. That is enough for every OTHER primitive -- an Event, a
    // semaphore, a condition variable all cancel by WAKING their waiter. It is NOT enough here,
    // because a parked RecvFrom is not waiting on anything the scheduler owns: it is sitting in the
    // kernel's completion queue with the kernel holding this frame's buffer. Marking a scope cannot
    // reach into that. The reactor is not implicitly wired to the scope either, so nothing else was
    // going to make the call on our behalf.
    //
    // RequestCancel is phase one of the two-phase teardown the header describes: it walks the
    // in-flight list and issues CancelIoEx against everything under this token. It resumes NOBODY.
    // Phase two is the OS delivering the completion -- Cancelled, or Completed if the datagram beat
    // the cancel -- which is what finally lets the coroutine's await return and the loop break. So
    // WaitFor below is waiting on the kernel, not on a flag.
    //
    // Get this wrong and the symptom is a process that will not exit: with nothing ever arriving on
    // the socket, that RecvFrom stays parked forever and the WaitGroup never reaches zero.
    scope.Cancel();
    io.RequestCancel(scope.Token());
    sched.WaitFor(wg);

    // Only now. closesocket while a request is still in flight would leave the reactor holding a
    // completion against a dead handle.
    ::closesocket(s);
    // No explicit shutdown call exists, and none is missing: teardown happens in the scheduler
    // destructor at process exit. The cancel-and-drain above is the part that matters -- it is what
    // guarantees nothing is still parked on `s` when closesocket lands.
    return 0;
}

// ---- sender --------------------------------------------------------------------------------------
//
// Deliberately NOT on the scheduler. It is a plain blocking console loop, because a sample that put
// both halves on the pool would suggest you have to, and you do not -- the interesting end is the
// receiver.

int RunSender(int dropOneIn) {
    SOCKET s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (s == INVALID_SOCKET) {
        std::printf("socket() failed: %d\n", ::WSAGetLastError());
        return 1;
    }

    sockaddr_in dest{};
    dest.sin_family      = AF_INET;
    dest.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    dest.sin_port        = ::htons(kPort);

    Packet p{ 0, kWinW / 2 - kRect / 2, kWinH / 2 - kRect / 2 };

    std::printf("arrow keys move the rect, q quits\n");
    if (dropOneIn > 1)
        std::printf("dropping 1 packet in %d -- watch `lost` climb in the window title\n", dropOneIn);

    for (;;) {
        const int c = ::_getch();
        if (c == 'q' || c == 'Q' || c == 27) break;

        // Arrow keys arrive as a two-byte sequence: a 0 or 0xE0 lead-in, then the code.
        if (c != 0 && c != 0xE0) continue;
        switch (::_getch()) {
        case 72: p.y -= kStep; break;   // up
        case 80: p.y += kStep; break;   // down
        case 75: p.x -= kStep; break;   // left
        case 77: p.x += kStep; break;   // right
        default: continue;
        }

        // Clamp on the SENDER so both ends agree on the rule. Clamping only on the receiver would
        // let the sender's idea of the position walk off past the wall and stick there.
        if (p.x < 0) p.x = 0;
        if (p.y < 0) p.y = 0;
        if (p.x > kWinW - kRect) p.x = kWinW - kRect;
        if (p.y > kWinH - kRect) p.y = kWinH - kRect;

        ++p.seq;

        // The sequence still ADVANCES on a dropped packet -- that is the point. A gap in the numbers
        // is exactly what loss looks like to the receiver, and it is how the counter finds it.
        if (dropOneIn > 1 && (p.seq % static_cast<std::uint32_t>(dropOneIn)) == 0) continue;

        ::sendto(s, reinterpret_cast<const char*>(&p), sizeof p, 0,
                 reinterpret_cast<sockaddr*>(&dest), sizeof dest);
    }

    ::closesocket(s);
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    bool send = false;
    int  drop = 0;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--send") == 0) {
            send = true;
        } else if (std::strcmp(argv[i], "--drop") == 0 && i + 1 < argc) {
            drop = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--help") == 0) {
            std::printf("udp_rect            receive on 127.0.0.1:%u and draw the rect\n"
                        "udp_rect --send     send arrow-key movement to it\n"
                        "         --drop N   (with --send) drop 1 packet in N\n",
                        static_cast<unsigned>(kPort));
            return 0;
        }
    }

    WSADATA wsa{};
    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::printf("WSAStartup failed\n");
        return 1;
    }

    const int rc = send ? RunSender(drop) : RunReceiver();

    ::WSACleanup();
    return rc;
}
