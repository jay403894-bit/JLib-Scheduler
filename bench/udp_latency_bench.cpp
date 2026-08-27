// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// UDP RECT LATENCY -- where the time goes between deciding to move and drawing the move.
//
//   udp_latency_bench [--seconds 60] [--rate 60] [--plain] [--load N] [--headless]
//
// Four timestamps per packet, all from QueryPerformanceCounter, all on one machine so they share a
// clock and need no correlation:
//
//   t_input    the input source decided to move            (stands in for "the key came back")
//   t_send     immediately before sendto()                 -- input->send is our sender-side cost
//   t_recv     the receive coroutine has the datagram      -- send->recv is the wire plus the reactor
//   t_paint    WM_PAINT for the frame that first shows it  -- recv->paint is the UI handoff
//
// == WHAT THIS DELIBERATELY DOES NOT MEASURE, BECAUSE IT CANNOT ==
//
// A real keypress reaches _getch() through the OS input stack, and a painted rect reaches the eye
// through DWM composition and monitor scanout. Both are milliseconds, neither is ours, and together
// they are far larger than everything measured here. Quoting a keypress-to-photons number would be
// reporting Windows' latency with our name on it. t_paint is when the paint call RAN, not when the
// pixels were seen; add roughly one to two refresh intervals for the display path.
//
// So the number this can honestly defend is input->paint: the span this process is responsible for.
//
// == WHY --plain EXISTS ==
//
// An unanchored latency figure is not a result. --plain runs the identical pipeline with the receive
// on a dedicated blocking-recvfrom thread instead of a reactor coroutine: same socket, same packet,
// same PostMessage, same paint. The difference between the two runs is what the scheduler and
// reactor cost against the naive thing, which is the only comparison that means anything.
//
// == THE SENDER SHARES THE PROCESS, AND THAT IS A REAL LIMITATION ==
//
// It is a separate thread, and the datagram does traverse the kernel's loopback path for real, but a
// two-process split would be more honest about cache and scheduling effects. This is noted rather
// than hidden; it makes send->recv slightly optimistic.

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "IoAsync.h"
#include "Coroutine.h"
#include "CancelToken.h"
#include "Timer.h"      // MonotonicNs -- the clock the reactor stamps its own timings in

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

namespace {

constexpr std::uint16_t kPort = 45455;      // not the sample's port, so both can run at once
constexpr int           kWinW = 640;
constexpr int           kWinH = 480;
constexpr int           kRect = 48;
constexpr UINT          kWmPkt = WM_APP + 1;

#pragma pack(push, 1)
struct Packet {
    std::uint32_t seq;
    std::int32_t  x;
    std::int32_t  y;
    std::int64_t  tInput;
    std::int64_t  tSend;
};
#pragma pack(pop)

// One row per packet, written by three different threads at three different times and read only
// after every one of them has stopped. No synchronisation beyond that ordering is needed or used.
struct Row {
    std::int64_t tInput = 0;
    std::int64_t tSend  = 0;
    std::int64_t tRecv  = 0;
    std::int64_t tPaint = 0;     // 0 if this packet was superseded before any frame showed it

    // Reactor-internal, in MonotonicNs rather than QPC, so these three only ever get compared with
    // EACH OTHER -- never subtracted from tSend/tRecv above. Zero without JLIBSCHED_IO_LOCK_STATS.
    std::int64_t tCompleted = 0;
    std::int64_t tFlushed   = 0;
    std::int64_t tResumeNs  = 0;
};

std::vector<Row>           g_rows;
std::atomic<std::uint32_t> g_lastSeq{ 0 };      // highest sequence the receiver has accepted
std::atomic<int>           g_x{ 0 };
std::atomic<int>           g_y{ 0 };
std::atomic<bool>          g_stop{ false };
std::atomic<std::uint32_t> g_recvCount{ 0 };

// Where the RESUMED COMPLETION actually ran, indexed by logical processor. Sampled inside the
// coroutine, which by definition executes on the hot worker that picked it up -- so this half of the
// placement question needs no scheduler instrumentation at all.
std::atomic<unsigned>      g_hotCore[64];

std::vector<std::int64_t>  g_paintGaps;         // interval between consecutive paints, for jitter
std::int64_t               g_lastPaint = 0;

std::int64_t g_qpf = 1;

inline std::int64_t Now() {
    LARGE_INTEGER t;
    ::QueryPerformanceCounter(&t);
    return t.QuadPart;
}
inline double Ms(std::int64_t ticks) { return (double)ticks * 1000.0 / (double)g_qpf; }
inline double Us(std::int64_t ticks) { return (double)ticks * 1000000.0 / (double)g_qpf; }

// ---- statistics ----------------------------------------------------------------------------------

struct Stats {
    std::size_t n = 0;
    double p50 = 0, p90 = 0, p95 = 0, p99 = 0, p999 = 0, max = 0, mean = 0;
};

Stats Summarise(std::vector<double>& v) {
    Stats s{};
    if (v.empty()) return s;
    std::sort(v.begin(), v.end());
    s.n = v.size();
    auto at = [&](double q) {
        // Nearest-rank. With n in the tens of thousands the interpolation choice is far below the
        // run-to-run spread, and nearest-rank never invents a value that was not observed.
        std::size_t i = (std::size_t)(q * (double)v.size());
        if (i >= v.size()) i = v.size() - 1;
        return v[i];
    };
    s.p50 = at(0.50); s.p90 = at(0.90); s.p95 = at(0.95);
    s.p99 = at(0.99); s.p999 = at(0.999); s.max = v.back();
    double sum = 0; for (double d : v) sum += d;
    s.mean = sum / (double)v.size();
    return s;
}

// EVERYTHING GOES TO A FILE AS WELL AS THE CONSOLE, because the console is not reliably there. Run
// this from Explorer and the console it opens dies with the process, taking the entire report with
// it -- the report is printed at the END, so it is exactly the part that is lost. Same failure as
// ReportSlabUsage in an app with no console.
FILE* g_log = nullptr;

void Out(const char* fmt, ...) {
    va_list a;
    va_start(a, fmt); std::vfprintf(stdout, fmt, a); va_end(a);
    if (g_log) { va_start(a, fmt); std::vfprintf(g_log, fmt, a); va_end(a); }
}

void PrintRow(const char* label, const Stats& s, const char* unit) {
    Out("  %-22s %8.3f %8.3f %8.3f %8.3f %8.3f %9.3f  %s\n",
                label, s.mean, s.p50, s.p90, s.p95, s.p99, s.max, unit);
}

// ---- the receiving end ---------------------------------------------------------------------------

void Accept(const Packet& p, std::int64_t tRecv) {
    if (p.seq >= g_rows.size()) return;
    if (p.seq <= g_lastSeq.load(std::memory_order_relaxed) && p.seq != 0) return;   // stale
    Row& r = g_rows[p.seq];
    r.tInput = p.tInput;
    r.tSend  = p.tSend;
    r.tRecv  = tRecv;
    g_x.store(p.x, std::memory_order_relaxed);
    g_y.store(p.y, std::memory_order_relaxed);
    g_lastSeq.store(p.seq, std::memory_order_release);
    g_recvCount.fetch_add(1, std::memory_order_relaxed);
}

JLib::Coro RecvLoop(JLib::IoSocket s, HWND hwnd, JLib::CancelToken tok) {
    JLib::IoAddress from{};
    char            buf[128];
    for (;;) {
        const JLib::IoResult r = co_await JLib::RecvFromAsync(s, buf, sizeof buf, &from, 0, tok);
        if (r.status != JLib::IoStatus::Completed) break;
        const std::int64_t t = Now();                 // stamp FIRST, before any of our own work

        // EVERY resume, not just the first few. Migration is the hypothesis being tested, so a
        // single early sample would be exactly the wrong instrument -- it would report where the
        // worker started and say nothing about where it spent the run.
        if (const DWORD cpu = ::GetCurrentProcessorNumber(); cpu < 64)
            g_hotCore[cpu].fetch_add(1, std::memory_order_relaxed);

        if (r.bytes != sizeof(Packet)) continue;
        Packet p{};
        std::memcpy(&p, buf, sizeof p);

        // WHERE INSIDE send->recv THE TIME WENT. Zero unless the library was built with
        // JLIBSCHED_IO_LOCK_STATS, so this costs nothing in a normal build.
        //
        // This is the split that decides whether the scheduler can do anything about the tail at
        // all: (flushed - completed) is time the REACTOR held the completion in a batch, and
        // (recv - flushed) is time the SCHEDULER took to get a worker onto it. KPolicy::WaitTime
        // can only ever see the second. If the tail lives in the first, no amount of K helps.
        if (r.completedAtNs != 0 && p.seq < g_rows.size()) {
            g_rows[p.seq].tCompleted = r.completedAtNs;
            g_rows[p.seq].tFlushed   = r.flushedAtNs;
            g_rows[p.seq].tResumeNs  = JLib::MonotonicNs();   // same clock as the two above
        }
        Accept(p, t);
        ::PostMessage(hwnd, kWmPkt, 0, 0);
    }
    co_return;
}

// The control. Identical work, no scheduler and no reactor: one thread blocked in recvfrom.
void PlainRecvThread(SOCKET s, HWND hwnd) {
    char buf[128];
    while (!g_stop.load(std::memory_order_relaxed)) {
        sockaddr_in from{};
        int         flen = sizeof from;
        const int n = ::recvfrom(s, buf, sizeof buf, 0, (sockaddr*)&from, &flen);
        if (n <= 0) break;
        const std::int64_t t = Now();
        if (n != sizeof(Packet)) continue;
        Packet p{};
        std::memcpy(&p, buf, sizeof p);
        Accept(p, t);
        ::PostMessage(hwnd, kWmPkt, 0, 0);
    }
}

// ---- the window ----------------------------------------------------------------------------------

bool g_headless = false;

LRESULT CALLBACK WndProc(HWND h, UINT m, WPARAM w, LPARAM l) {
    switch (m) {
    case kWmPkt:
        ::InvalidateRect(h, nullptr, FALSE);
        return 0;
    case WM_PAINT: {
        PAINTSTRUCT ps{};
        HDC dc = ::BeginPaint(h, &ps);

        const int x = g_x.load(std::memory_order_relaxed);
        const int y = g_y.load(std::memory_order_relaxed);
        if (!g_headless) {
            RECT full{}; ::GetClientRect(h, &full);
            ::FillRect(dc, &full, (HBRUSH)::GetStockObject(BLACK_BRUSH));
            RECT   box{ x, y, x + kRect, y + kRect };
            HBRUSH brush = ::CreateSolidBrush(RGB(90, 200, 250));
            ::FillRect(dc, &box, brush);
            ::DeleteObject(brush);
        }
        ::EndPaint(h, &ps);

        // Stamp AFTER the drawing, so recv->paint covers the work and not just the dispatch. The
        // sequence this frame showed is whatever the receiver had published when we read it.
        const std::int64_t t   = Now();
        const std::uint32_t sq = g_lastSeq.load(std::memory_order_acquire);
        if (sq < g_rows.size() && g_rows[sq].tPaint == 0 && g_rows[sq].tRecv != 0)
            g_rows[sq].tPaint = t;

        if (g_lastPaint != 0) g_paintGaps.push_back(t - g_lastPaint);
        g_lastPaint = t;
        return 0;
    }
    case WM_DESTROY:
        ::PostQuitMessage(0);
        return 0;
    }
    return ::DefWindowProc(h, m, w, l);
}

// ---- the input source ----------------------------------------------------------------------------
//
// Stands in for the key handler. A high-resolution waitable timer rather than Sleep, because the
// default timer granularity is 15.6 ms -- pacing a 60 Hz test with Sleep would measure the timer.
// It also does not spin, so it does not steal a core from the thing under test.

void InputThread(std::uint32_t packets, int rateHz) {
    SOCKET s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    sockaddr_in dest{};
    dest.sin_family      = AF_INET;
    dest.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    dest.sin_port        = ::htons(kPort);

    HANDLE timer = ::CreateWaitableTimerExW(nullptr, nullptr,
                                            CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
    if (!timer) timer = ::CreateWaitableTimerW(nullptr, FALSE, nullptr);

    LARGE_INTEGER due{};
    due.QuadPart = -10000000LL / (rateHz ? rateHz : 60);      // negative = relative, 100 ns units
    ::SetWaitableTimer(timer, &due, 1000 / (rateHz ? rateHz : 60), nullptr, nullptr, FALSE);

    int px = kWinW / 2, py = kWinH / 2, dx = 3, dy = 2;

    for (std::uint32_t seq = 1; seq < packets && !g_stop.load(std::memory_order_relaxed); ++seq) {
        ::WaitForSingleObject(timer, 1000);

        const std::int64_t tIn = Now();      // "the input event is now known to the app"

        px += dx; py += dy;
        if (px < 0 || px > kWinW - kRect) { dx = -dx; px += dx * 2; }
        if (py < 0 || py > kWinH - kRect) { dy = -dy; py += dy * 2; }

        Packet p{};
        p.seq = seq; p.x = px; p.y = py;
        p.tInput = tIn;
        p.tSend  = Now();
        ::sendto(s, (const char*)&p, sizeof p, 0, (sockaddr*)&dest, sizeof dest);
    }

    ::CloseHandle(timer);
    ::closesocket(s);
}

// ---- CPU topology, because a processor NUMBER answers nothing --------------------------------------
//
// The question is not "which index did this run on", it is "which efficiency class, and did the hot
// worker and the completion thread land in the same cache domain". Raw indices cannot be compared
// across machines or even reasoned about on one: on a hybrid part the P and E cores are interleaved
// in ways that are not documented and not stable.
//
// So everything below maps through GetLogicalProcessorInformationEx: EfficiencyClass from
// RelationProcessorCore (higher = more performant; 0 is the E class on current Intel hybrids), and
// an LLC id from RelationCache at the last level, which is what says "same cluster / same CCD".

struct CoreInfo {
    int efficiency = -1;    // -1 = unknown
    int llc        = -1;    // index of the last-level cache this processor belongs to
    int coreId     = -1;    // physical core, so SMT siblings can be spotted
};
CoreInfo g_topo[64];

void BuildTopology() {
    DWORD len = 0;
    ::GetLogicalProcessorInformationEx(RelationAll, nullptr, &len);
    if (!len) return;
    std::vector<char> buf(len);
    if (!::GetLogicalProcessorInformationEx(RelationAll,
            (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)buf.data(), &len)) return;

    int coreSeq = 0, llcSeq = 0;
    int maxCacheLevel = 0;
    // Two passes over caches: the last level is whatever the deepest level present is, and that is
    // not always 3 (some parts expose an L4/side cache, and small parts stop at 2).
    for (DWORD off = 0; off < len; ) {
        auto* p = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)(buf.data() + off);
        if (p->Relationship == RelationCache && p->Cache.Level > maxCacheLevel)
            maxCacheLevel = p->Cache.Level;
        off += p->Size;
    }

    for (DWORD off = 0; off < len; ) {
        auto* p = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)(buf.data() + off);
        if (p->Relationship == RelationProcessorCore) {
            const int eff = (int)p->Processor.EfficiencyClass;
            for (WORD g = 0; g < p->Processor.GroupCount; ++g) {
                KAFFINITY m = p->Processor.GroupMask[g].Mask;
                for (int b = 0; b < 64; ++b)
                    if (m & (1ull << b)) { g_topo[b].efficiency = eff; g_topo[b].coreId = coreSeq; }
            }
            ++coreSeq;
        } else if (p->Relationship == RelationCache && p->Cache.Level == maxCacheLevel) {
            for (WORD g = 0; g < p->Cache.GroupCount; ++g) {
                KAFFINITY m = p->Cache.GroupMasks[g].Mask;
                for (int b = 0; b < 64; ++b) if (m & (1ull << b)) g_topo[b].llc = llcSeq;
            }
            ++llcSeq;
        }
        off += p->Size;
    }
}

// ---- where does K actually sit? -------------------------------------------------------------------
//
// The controller's behaviour is not inferable from the latency numbers -- "between static K=1 and
// static K=2" is consistent with sitting at 1 most of the time, at 2 most of the time, or flapping.
// Sampling the effective K settles it, and it is the only way to tell which of the promote signals
// is or is not firing without instrumenting the scheduler itself.

std::atomic<unsigned> g_kHist[16];

// FOREGROUND STATE. Windows gives the foreground window's threads a priority boost and a longer
// quantum (Win32PrioritySeparation; on client defaults that is roughly 3x). Losing focus costs both
// -- which is a PREEMPTION change, and preemption is what the dispatch tail was already shown to be.
//
// Sampled rather than read once, because focus can change mid-run and the whole question is whether
// this tracks the fast/slow mode. Counting both halves so a run that changed focus is visible as a
// fraction rather than silently reported as one or the other.
HWND                  g_hwnd = nullptr;
std::atomic<unsigned> g_fgSamples{ 0 };
std::atomic<unsigned> g_fgForeground{ 0 };

void KSamplerThread() {
    while (!g_stop.load(std::memory_order_relaxed)) {
        const size_t k = JLib::TaskScheduler::GetHotWorkers();
        g_kHist[k < 16 ? k : 15].fetch_add(1, std::memory_order_relaxed);

        // Sampled alongside K so the two can be read against each other on one timeline.
        if (g_hwnd) {
            g_fgSamples.fetch_add(1, std::memory_order_relaxed);
            if (::GetForegroundWindow() == g_hwnd)
                g_fgForeground.fetch_add(1, std::memory_order_relaxed);
        }
        ::Sleep(1);
    }
}

// ---- background load -----------------------------------------------------------------------------
//
// Keeps the worker pool genuinely busy, which is the only way to ask whether the receive path and
// the message pump degrade when the machine is not idle. Ordinary priority, ordinary queue: this is
// the app's other work, not a competing benchmark.

void LoadThread(int tasks) {
    auto& sched = JLib::TaskScheduler::Instance();
    while (!g_stop.load(std::memory_order_relaxed)) {
        // ParallelFor rather than a hand-rolled push loop: it is the shape an app's frame work
        // actually has, and it saturates the pool without this thread having to keep it fed.
        sched.ParallelFor(0, tasks, 1, [](int lo, int hi) {
            for (int i = lo; i < hi; ++i) {
                volatile double acc = 0;
                for (int k = 0; k < 20000; ++k) acc += (double)k * 1.000001;
            }
        });
    }
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    int  seconds = 60, rate = 60, load = 0;
    bool plain = false, hipri = false, waittime = false, pinHot = false, elevate = false, noActivate = false;
    size_t    kmin = 1, kmax = 2;
    long long waitTargetNs = 0;                 // 0 = leave the library default alone
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--seconds") && i + 1 < argc)   seconds = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--rate") && i + 1 < argc) rate    = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--load") && i + 1 < argc) load    = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--plain"))                plain   = true;
        else if (!std::strcmp(argv[i], "--hipri"))                hipri   = true;
        else if (!std::strcmp(argv[i], "--k") && i + 1 < argc) { kmin = kmax = (size_t)std::atoi(argv[++i]); hipri = true; }
        // Range and policy are SEPARATE flags on purpose. Bundling them would make every
        // WaitTime-vs-QueueLoad comparison also a range comparison, and there would be no way to
        // say which one moved the number.
        else if (!std::strcmp(argv[i], "--kmin") && i + 1 < argc) { kmin = (size_t)std::atoi(argv[++i]); hipri = true; }
        else if (!std::strcmp(argv[i], "--kmax") && i + 1 < argc) { kmax = (size_t)std::atoi(argv[++i]); hipri = true; }
        else if (!std::strcmp(argv[i], "--waittime"))           { waittime = true; hipri = true; }
        else if (!std::strcmp(argv[i], "--pin"))                  pinHot   = true;
        else if (!std::strcmp(argv[i], "--prio"))                 elevate  = true;
        else if (!std::strcmp(argv[i], "--noactivate"))            noActivate = true;
        else if (!std::strcmp(argv[i], "--waittarget") && i + 1 < argc) waitTargetNs = std::atoll(argv[++i]);
        else if (!std::strcmp(argv[i], "--headless"))             g_headless = true;
    }

    LARGE_INTEGER f; ::QueryPerformanceFrequency(&f); g_qpf = f.QuadPart;
    // Beside the EXE, not the working directory: launched from Explorer those differ, and the whole
    // point is that this has to work when nobody is watching a console.
    {
        char path[MAX_PATH] = { 0 };
        if (::GetModuleFileNameA(nullptr, path, MAX_PATH)) {
            if (char* slash = std::strrchr(path, '\\')) slash[1] = '\0';
            std::strncat(path, "udp_latency_bench.log", MAX_PATH - std::strlen(path) - 1);
            g_log = std::fopen(path, "w");
        }
        if (!g_log) g_log = std::fopen("udp_latency_bench.log", "w");
        if (g_log) std::fprintf(g_log, "log: %s\n", path);
    }

    BuildTopology();
    {
        // PRINTED, not assumed. Every conclusion below rests on this mapping being right, and
        // "efficiency class 0" means nothing until you can see how many processors are in it.
        int byClass[8] = { 0 };
        int known = 0, lo = 99, hi = -1;
        for (int i = 0; i < 64; ++i) {
            if (g_topo[i].efficiency < 0) continue;
            ++known;
            if (g_topo[i].efficiency < 8) ++byClass[g_topo[i].efficiency];
            if (g_topo[i].efficiency == 0) { if (i < lo) lo = i; if (i > hi) hi = i; }
        }
        Out("topology: %d logical processors; ", known);
        for (int c = 0; c < 8; ++c) if (byClass[c]) Out("class%d=%d ", c, byClass[c]);
        if (hi >= 0) Out(" (class0 spans cpu%d..cpu%d)", lo, hi);
        Out("\n");
    }

    WSADATA wsa{};
    ::WSAStartup(MAKEWORD(2, 2), &wsa);

    const std::uint32_t packets = (std::uint32_t)(seconds * rate) + 16;
    g_rows.assign(packets, Row{});
    g_paintGaps.reserve(packets);

    Out("udp_latency_bench -- %s receive, %d s at %d Hz%s\n",
                plain ? "PLAIN blocking" : (hipri ? (waittime ? "JLib lane WaitTime" : "JLib lane QueueLoad") : "JLib reactor"), seconds, rate,
                load ? "  [pool under load]" : "");

    SOCKET s = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port        = ::htons(kPort);
    if (::bind(s, (sockaddr*)&addr, sizeof addr) != 0) {
        Out("bind failed: %d\n", ::WSAGetLastError());
        return 1;
    }

    WNDCLASSA wc{};
    wc.lpfnWndProc   = &WndProc;
    wc.hInstance     = ::GetModuleHandleA(nullptr);
    wc.lpszClassName = "JLibUdpBench";
    ::RegisterClassA(&wc);
    HWND hwnd = ::CreateWindowA("JLibUdpBench", "JLib UDP latency bench",
                                WS_OVERLAPPEDWINDOW, CW_USEDEFAULT, CW_USEDEFAULT,
                                kWinW, kWinH, nullptr, nullptr, wc.hInstance, nullptr);
    // --noactivate MAKES THE FOREGROUND QUESTION AN EXPERIMENT INSTEAD OF AN ACCIDENT.
    //
    // Shown normally, this window TAKES foreground on creation -- so a run is "focused" unless a
    // human happens to click elsewhere mid-run. That made focus an uncontrolled variable that
    // tracked whether anyone was using the machine, and it is the single largest term in every
    // latency number this bench has produced: dispatch p90 1.5 us focused against 433 us not.
    //
    // SW_SHOWNOACTIVATE plus WS_EX_NOACTIVATE means the window appears and never steals focus, so
    // whatever the user was already in keeps it. That gives a repeatable UNFOCUSED run with nobody
    // touching the mouse, which is the only way to A/B this honestly.
    if (noActivate) {
        ::SetWindowLongPtrA(hwnd, GWL_EXSTYLE,
                            ::GetWindowLongPtrA(hwnd, GWL_EXSTYLE) | WS_EX_NOACTIVATE);
        ::ShowWindow(hwnd, SW_SHOWNOACTIVATE);
    } else {
        ::ShowWindow(hwnd, g_headless ? SW_MINIMIZE : SW_SHOW);
    }
    g_hwnd = hwnd;

    JLib::CancelScope scope;
    JLib::WaitGroup   wg;
    std::thread       plainThread, loadThread;

    if (plain) {
        plainThread = std::thread(PlainRecvThread, s, hwnd);
        if (load) { JLib::TaskScheduler::Init(0); loadThread = std::thread(LoadThread, load); }
    } else {
        JLib::TaskScheduler::EnableIoReactor(true);
        // --hipri is the whole reason the lane exists, and this is the case it was built for: a
        // short, latency-critical body that must not queue behind a pool full of ordinary frame
        // work. Without it the receive coroutine is an ordinary task and waits its turn, which is
        // correct default behaviour and visibly wrong for this particular job.
        // kmax==kmin is STATIC K by construction -- no controller, no oscillation. --k lets the
        // bench separate "the lane helps" from "the controller is stable", which are different claims.
        // Both MUST precede Init: placement and priority are applied as each worker starts.
        if (pinHot)  JLib::TaskScheduler::SetHotWorkerPin(true);
        if (elevate) JLib::TaskScheduler::SetHotThreadPolicy(JLib::TaskScheduler::HotThreadPolicy::Elevated);
        if (hipri) JLib::TaskScheduler::SetHotWorkerRange(kmin, kmax);
        if (waittime) JLib::TaskScheduler::SetHotWorkerPolicy(JLib::TaskScheduler::KPolicy::WaitTime);
        if (waitTargetNs > 0) JLib::TaskScheduler::SetLaneWaitTargetNs(waitTargetNs);
        JLib::TaskScheduler::Init(0);
        auto& io = JLib::IoReactor::Instance();
        if (!io.RegisterSocket(s)) { Out("RegisterSocket failed\n"); return 1; }
        JLib::Spawn(RecvLoop(s, hwnd, scope.Token()), &wg, hipri ? 1 : 0);
        if (load) loadThread = std::thread(LoadThread, load);
    }

    std::thread ksampler;
    if (!plain) ksampler = std::thread(KSamplerThread);
    std::thread input(InputThread, packets, rate);

    // Run the pump for the requested wall time. A timer posts WM_QUIT when the window has had long
    // enough; the pump itself is the thing being measured, so it must not be bypassed.
    const std::int64_t deadline = Now() + (std::int64_t)seconds * g_qpf;
    MSG msg{};
    while (Now() < deadline) {
        while (::PeekMessage(&msg, nullptr, 0, 0, PM_REMOVE)) {
            if (msg.message == WM_QUIT) goto done;
            ::TranslateMessage(&msg);
            ::DispatchMessage(&msg);
        }
        ::MsgWaitForMultipleObjects(0, nullptr, FALSE, 5, QS_ALLINPUT);
    }
done:
    g_stop.store(true, std::memory_order_relaxed);
    if (input.joinable()) input.join();
    if (loadThread.joinable()) loadThread.join();
    if (ksampler.joinable()) ksampler.join();

    if (plain) {
        ::closesocket(s);                       // unblocks the recvfrom
        if (plainThread.joinable()) plainThread.join();
    } else {
        scope.Cancel();
        JLib::IoReactor::Instance().RequestCancel(scope.Token());
        JLib::TaskScheduler::Instance().WaitFor(wg);
        ::closesocket(s);
    }

    // ---- results ---------------------------------------------------------------------------------

    std::vector<double> inSend, sendRecv, recvPaint, inPaint;
    std::size_t sent = 0, received = 0, painted = 0;
    for (std::size_t i = 1; i < g_rows.size(); ++i) {
        const Row& r = g_rows[i];
        if (r.tSend == 0) continue;
        ++sent;
        if (r.tRecv == 0) continue;
        ++received;
        inSend.push_back(Us(r.tSend - r.tInput));
        sendRecv.push_back(Us(r.tRecv - r.tSend));
        if (r.tPaint == 0) continue;
        ++painted;
        recvPaint.push_back(Us(r.tPaint - r.tRecv));
        inPaint.push_back(Ms(r.tPaint - r.tInput));
    }

    Out("\npackets: %zu accepted of %u seq, %zu reached a frame (%zu superseded first)\n",
                received, g_recvCount.load(), painted, received - painted);
    Out("\n  %-22s %8s %8s %8s %8s %8s %9s\n",
                "stage", "mean", "p50", "p90", "p95", "p99", "max");
    Out("  ---------------------------------------------------------------------------\n");

    Stats a = Summarise(inSend);    PrintRow("input -> send", a, "us");
    Stats b = Summarise(sendRecv);  PrintRow("send -> recv", b, "us   <-- wire + reactor");
    Stats c = Summarise(recvPaint); PrintRow("recv -> paint", c, "us   <-- UI handoff");
    Stats d = Summarise(inPaint);   PrintRow("input -> paint", d, "ms   <-- ours, end to end");

    // Jitter: the spread of the interval between consecutive frames. Mean is uninteresting (it is
    // the send rate); the tail is the whole question.
    std::vector<double> gaps;
    gaps.reserve(g_paintGaps.size());
    for (std::int64_t g : g_paintGaps) gaps.push_back(Ms(g));
    Stats j = Summarise(gaps);
    double var = 0;
    for (double g : gaps) var += (g - j.mean) * (g - j.mean);
    const double sd = gaps.empty() ? 0.0 : std::sqrt(var / (double)gaps.size());

    Out("\n  %-22s %8.3f %8.3f %8.3f %8.3f %8.3f %9.3f  ms  <-- frame interval\n",
                "paint -> paint", j.mean, j.p50, j.p90, j.p95, j.p99, j.max);
    Out("  frame jitter (sd): %.3f ms over %zu frames; target interval %.3f ms\n",
                sd, gaps.size(), 1000.0 / (double)(rate ? rate : 60));

    // ---- inside send->recv, when the reactor was built with the stats define ---------------------
    {
        std::vector<double> batched, dispatch;
        for (std::size_t i = 1; i < g_rows.size(); ++i) {
            const Row& r = g_rows[i];
            if (r.tCompleted == 0 || r.tFlushed == 0 || r.tResumeNs == 0) continue;
            batched.push_back((double)(r.tFlushed - r.tCompleted) / 1000.0);   // ns -> us
            dispatch.push_back((double)(r.tResumeNs - r.tFlushed) / 1000.0);
        }
        if (!batched.empty()) {
            Out("\n  the dispatch seam, split (needs -DJLIBSCHED_IO_LOCK_STATS):\n");
            Out("  %-22s %8s %8s %8s %8s %8s %9s\n",
                        "stage", "mean", "p50", "p90", "p95", "p99", "max");
            Out("  ---------------------------------------------------------------------------\n");
            Stats b = Summarise(batched);
            Stats d = Summarise(dispatch);
            PrintRow("reactor held (batch)", b, "us   <-- K cannot touch this");
            PrintRow("scheduler dispatch",   d, "us   <-- what WaitTime sees");
        }
    }

    // ---- placement: where the lane and the reactor actually ran ---------------------------------
    {
        unsigned compl_[64] = { 0 };
        JLib::IoReactor::CompletionCoreHistogram(compl_, 64);

        auto summarise = [](const unsigned* h, const char* what) {
            unsigned total = 0, onE = 0, onP = 0;
            int      top = -1; unsigned topN = 0;
            int      llcOfTop = -1;
            for (int i = 0; i < 64; ++i) {
                total += h[i];
                if (h[i] > topN) { topN = h[i]; top = i; }
                if (g_topo[i].efficiency == 0) onE += h[i];
                else if (g_topo[i].efficiency > 0) onP += h[i];
            }
            if (top >= 0) llcOfTop = g_topo[top].llc;
            if (!total) { Out("  %-18s (no samples)\n", what); return std::make_pair(-1, -1); }
            Out("  %-18s cpu%-3d %4.0f%% of %-8u  eff=%d llc=%d   P=%.0f%% E=%.0f%%\n",
                        what, top, 100.0 * topN / total, total,
                        top >= 0 ? g_topo[top].efficiency : -1, llcOfTop,
                        100.0 * onP / total, 100.0 * onE / total);
            return std::make_pair(top >= 0 ? g_topo[top].efficiency : -1, llcOfTop);
        };

        unsigned hot_[64] = { 0 };
        for (int i = 0; i < 64; ++i) hot_[i] = g_hotCore[i].load(std::memory_order_relaxed);

        Out("\n  placement (efficiency class: 0 = E core, higher = P core):\n");
        const auto h = summarise(hot_,   "resumed on");
        const auto c = summarise(compl_, "reactor woke on");

        // THE COMPARISON THE TABLE IS FOR. Same efficiency class or not, same last-level cache or
        // not -- a completion handed across a cache domain costs a great deal more than one handed
        // within it, and that is invisible in any per-stage latency number.
        if (h.first >= 0 && c.first >= 0) {
            Out("  -> hot/reactor: eff %s, llc %s\n",
                        h.first == c.first ? "SAME" : "DIFFERENT",
                        h.second == c.second ? "SAME" : "DIFFERENT");
        }
    }

    unsigned ktot = 0;
    for (int i = 0; i < 16; ++i) ktot += g_kHist[i].load();
    if (ktot) {
        Out("\n  effective K over the run (1 ms samples):\n");
        for (int i = 0; i < 16; ++i) {
            const unsigned c = g_kHist[i].load();
            if (c) Out("    K=%d  %5.1f%%  (%u samples)\n",
                               i, 100.0 * (double)c / (double)ktot, c);
        }
    }

    // ---- the one line to read ---------------------------------------------------------------------
    //
    // Foreground share against the tail, on one line, because the hypothesis is that these two move
    // together and nothing else in this report pairs them. Windows gives the foreground window's
    // threads a priority boost and a longer quantum; losing focus costs both, and that is a
    // preemption change -- which is what the dispatch tail was already shown to be.
    {
        const unsigned fs = g_fgSamples.load(), fg = g_fgForeground.load();
        const double   pct = fs ? 100.0 * (double)fg / (double)fs : -1.0;
        Out("\n  ==> FOREGROUND %5.1f%% of the run   |   send->recv p90 %8.3f us  p99 %8.3f us\n",
                    pct, b.p90, b.p99);
        if (fs == 0)
            Out("      (no samples -- foreground is only tracked on the reactor path)\n");
    }

    const double frame = 1000.0 / 60.0;
    Out("\n  VERDICT: input->paint p99 = %.3f ms vs a %.2f ms frame -- %s\n",
                d.p99, frame, d.p99 < frame ? "UNDER" : "OVER");
    Out("  (excludes OS input latency before t_input and DWM compose + scanout after\n"
                "   t_paint; neither is this process's to control)\n");

    if (g_log) { std::fclose(g_log); g_log = nullptr; }

    // HOLD THE CONSOLE ONLY IF WE OWN IT. GetConsoleProcessList returning 1 means this process is
    // the console's only client, which is the Explorer double-click case -- the window dies with us
    // and takes the report with it. Launched from an existing shell the count is higher and pausing
    // would just block a script forever, which is why this is a check and not an unconditional wait.
    {
        DWORD pids[4] = { 0 };
        if (::GetConsoleProcessList(pids, 4) == 1) {
            std::printf("\n  [press Enter to close -- the same report is in udp_latency_bench.log]\n");
            (void)std::getchar();
        }
    }

    ::WSACleanup();
    return 0;
}
