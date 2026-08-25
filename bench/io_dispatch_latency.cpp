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
#include "Thread.h"      // StealStatsReset/Read -- remote deque touches, the ping-pong quantity

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
std::vector<long long> g_hold;   // flushed - completed: time the REACTOR held it in a batch
std::vector<long long> g_disp;   // resumed - flushed:   time the SCHEDULER took to run it
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
    if (r.completedAtNs != 0) {
        const int k = sample * kBurst + slot;
        g_samples[k] = now - r.completedAtNs;
        if (r.flushedAtNs != 0) { g_hold[k] = r.flushedAtNs - r.completedAtNs; g_disp[k] = now - r.flushedAtNs; }
    }
    co_return;
}

JLib::Coro OneOp(int i) {
    const JLib::IoResult r = co_await JLib::RecvAsync(g_server, g_rx, sizeof g_rx);
    const long long now = JLib::MonotonicNs();
    if (r.completedAtNs != 0) {
        g_samples[i] = now - r.completedAtNs;
        if (r.flushedAtNs != 0) { g_hold[i] = r.flushedAtNs - r.completedAtNs; g_disp[i] = now - r.flushedAtNs; }
    }
    g_done.fetch_add(1, std::memory_order_release);
    co_return;
}

// ---- CONCURRENT SOAK ------------------------------------------------------------------------
//
// THE ONE SHAPE NOTHING ELSE HERE MEASURES. Every other pass has ~ONE lane operation in flight:
// HOT/COLD are strictly one-at-a-time, and BURST is a synchronised WAVE -- fire kBurst, wait for
// all, repeat. So every number this file has produced describes a lane with no CONCURRENCY, and
// the two open questions are exactly about what happens when it has some:
//
//   * optimal K only matters once ONE hot worker can saturate. At one op in flight it never can,
//     which is why K=1 has trivially sufficed so far.
//   * hot->hot stealing (and a ring buffer to make it cheap) only matters if hot workers back up
//     UNEVENLY. With one op in flight there is nothing to rebalance -- which is why it measured as
//     20.6M probes of pure cost. That result is honest for that workload and says nothing about a
//     loaded lane.
//
// N INDEPENDENT COROUTINES, each looping on its own socket pair, never synchronised with each
// other. That is a real steady-state offered load rather than a wave, so the lane can genuinely
// back up. Sweeping N answers "at what arrival rate does one hot worker saturate" and "do the hot
// workers share the load or does one starve".
struct SoakPair { JLib::IoSocket tx{}, rx{}; sockaddr_in dst{}; };
std::vector<SoakPair> g_soak;
std::vector<std::array<char, 8>> g_soakBuf;
std::atomic<int> g_soakLive{ 0 };
std::vector<long long> g_soakLat;      // one slot per op, flat
std::atomic<size_t> g_soakNext{ 0 };

// THE SKEW. Every op in the symmetric soak is the same size, so it cannot separate the two
// explanations for a bad lane tail: every hot worker saturated (capacity -- raise K) versus one hot
// worker buried while a sibling spins (balance -- what stealing fixes). Only the second is possible
// when all work is uniform and steering round-robins it, so the symmetric soak structurally CANNOT
// produce the condition it is being asked about.
//
// So a minority of resumptions burn CPU for a while. That is not an artificial stressor: it is what
// a real completion handler looks like the moment anyone does work in one -- parse a packet, hash a
// block, decompress an asset chunk. The burn happens INSIDE the resumed coroutine, i.e. on the hot
// worker, occupying it exactly as a real handler would, and blocking every completion steered
// behind it. A running task cannot be preempted, so that is the whole mechanism.
//
// g_skewEveryNth = 0 disables it, which is the A/A control against the symmetric soak.
int  g_skewEveryNth = 0;
long long g_skewBurnNs = 0;
std::atomic<long long> g_skewBurns{ 0 };

// Spin, not sleep: sleeping releases the worker, which is the opposite of what a busy handler does.
// Volatile sink so the optimiser cannot delete the loop.
static void BurnNs(long long ns) {
    const long long end = JLib::MonotonicNs() + ns;
    volatile double sink = 0.0;
    while (JLib::MonotonicNs() < end) { for (int i = 0; i < 64; ++i) sink = sink + 1.0; }
    (void)sink;
}

JLib::Coro SoakOp(int slot, int iters) {
    char tx[8] = { 's','o','a','k','!','!','!','!' };
    for (int i = 0; i < iters; ++i) {
        const JLib::IoResult w = co_await JLib::SendToAsync(
            g_soak[slot].tx, tx, sizeof tx, &g_soak[slot].dst, sizeof(sockaddr_in));
        if (!w.Ok()) break;

        JLib::IoAddress from{};
        const JLib::IoResult r = co_await JLib::RecvFromAsync(
            g_soak[slot].rx, g_soakBuf[slot].data(), 8, &from);
        if (!r.Ok()) break;

        // Only the RECV half is timed: it is the completion that had to find a worker. The send
        // completes on the same path but its latency is not what the lane exists to protect.
        if (r.completedAtNs != 0) {
            const size_t k = g_soakNext.fetch_add(1, std::memory_order_relaxed);
            if (k < g_soakLat.size()) g_soakLat[k] = JLib::MonotonicNs() - r.completedAtNs;
        }

        // TIMED FIRST, BURNED SECOND, deliberately. This op's own latency must record when it was
        // resumed, not when it finished -- the burn is what this op does TO THE OTHERS, and folding
        // it into this op's own sample would bury the effect in the population that causes it. The
        // signal being hunted lives in the latency of the SHORT ops queued behind this one.
        if (g_skewEveryNth > 0 && ((slot + i) % g_skewEveryNth) == 0) {
            g_skewBurns.fetch_add(1, std::memory_order_relaxed);
            BurnNs(g_skewBurnNs);
        }
    }
    g_soakLive.fetch_sub(1, std::memory_order_release);
    co_return;
}

// One soak run: N coroutines, `iters` round trips each, sockets set up and torn down. Extracted so
// the skewed sweep and the symmetric sweep are literally the same code path -- two copies of this
// would drift, and a skew-vs-no-skew comparison across two drifted harnesses measures the drift.
static std::vector<long long> SoakRun(int n, int iters, double& secsOut) {
    auto& io = JLib::IoReactor::Instance();

    g_soak.assign(n, SoakPair{});
    g_soakBuf.assign(n, {});
    g_soakLat.assign((size_t)n * iters, 0);
    g_soakNext.store(0, std::memory_order_relaxed);
    g_soakLive.store(n, std::memory_order_relaxed);

    for (int i = 0; i < n; ++i) {
        SOCKET t = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        SOCKET r = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        sockaddr_in ra{};
        ra.sin_family = AF_INET;
        ra.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        ra.sin_port = 0;
        ::bind(r, reinterpret_cast<sockaddr*>(&ra), sizeof ra);
        int rl = sizeof ra;
        ::getsockname(r, reinterpret_cast<sockaddr*>(&ra), &rl);
        io.RegisterSocket(t);
        io.RegisterSocket(r);
        g_soak[i].tx = static_cast<JLib::IoSocket>(t);
        g_soak[i].rx = static_cast<JLib::IoSocket>(r);
        g_soak[i].dst = ra;
    }

    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < n; ++i)
        JLib::Spawn(SoakOp(i, iters), static_cast<JLib::WaitGroup*>(nullptr), (uint8_t)1);

    // No WaitGroup: these are long-lived loops, and a WaitFor from main would spin-help and perturb
    // the very dispatch being measured. Poll the live count instead.
    while (g_soakLive.load(std::memory_order_acquire) > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    secsOut = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();

    for (int i = 0; i < n; ++i) {
        ::closesocket(static_cast<SOCKET>(g_soak[i].tx));
        ::closesocket(static_cast<SOCKET>(g_soak[i].rx));
    }

    std::vector<long long> v;
    for (long long x : g_soakLat) if (x > 0) v.push_back(x);
    std::sort(v.begin(), v.end());
    return v;
}

void Report(const char* label, std::vector<long long> s) {
    if (s.empty()) { std::printf("  %-6s no samples\n", label); return; }
    std::sort(s.begin(), s.end());
    const auto pick = [&](double p) { return s[std::min(s.size() - 1, size_t(s.size() * p)) ]; };
    std::printf("  %-6s n=%-5zu  p50 %7.2f us   p90 %7.2f us   p99 %7.2f us   max %8.2f us\n",
                label, s.size(), pick(0.50) / 1000.0, pick(0.90) / 1000.0,
                pick(0.99) / 1000.0, double(s.back()) / 1000.0);
}

// The decomposition that decides whether a flush timer is worth having: HOLD is time the reactor
// kept the completion in a batch, DISPATCH is time the scheduler took to get a worker onto it. A
// flush timer can only ever bound HOLD.
void ReportSplit() {
    std::vector<long long> h, d;
    for (size_t i = 0; i < g_hold.size(); ++i) if (g_hold[i] || g_disp[i]) { h.push_back(g_hold[i]); d.push_back(g_disp[i]); }
    if (h.empty()) return;
    Report("  hold", std::move(h));
    Report("  disp", std::move(d));
}

} // namespace

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    const int samples = (argc > 1) ? std::atoi(argv[1]) : 400;
    const int coldMs  = (argc > 2) ? std::atoi(argv[2]) : 3;
    const unsigned th = (argc > 3) ? unsigned(std::atoi(argv[3])) : 1u;

    // 0 = Sleep (the default), 1 = NoSleep. SAME BINARY, selected at runtime, because comparing two
    // builds is the control failure that invalidated the first version of this harness.
    //
    // NoSleep is the UPPER BOUND on what this seam can be worth, not a proposal: it deletes the OS
    // wake entirely, so whatever tail survives it is NOT wake latency and no parking policy can
    // remove it. It costs a spinning core per worker and taxes every other thread in the process
    // (~3.5% on an idle pool against a memory-bound main thread), which is why the default is Sleep.
    const bool noSleep = (argc > 4) ? (std::atoi(argv[4]) != 0) : false;

    // K-hot: how many workers never park. 0 is the control. Sweeping this is what says whether a
    // small number of landing spots is enough, or whether completions have to be STEERED at them --
    // a curve that stays flat until K is a large fraction of the pool means steering, not K.
    const std::size_t hot = (argc > 5) ? std::size_t(std::atoi(argv[5])) : 0u;

    // Raise ONLY the hot workers and the completion threads to TIME_CRITICAL. Tests whether the
    // bistability is the OS descheduling one end of the completion handoff.
    const bool hotRt = (argc > 6) ? (std::atoi(argv[6]) != 0) : false;

    // Hard-pin the hot workers. The PORTABLE preemption mitigation -- no privilege needed, works
    // the same on Linux/macOS -- so the question is how close pin-without-RT gets to RT.
    const bool hotPin = (argc > 7) ? (std::atoi(argv[7]) != 0) : false;

    // EXCLUSIVE: hot workers own their cores AND everyone else is masked off them. The userspace
    // approximation of isolcpus, and the only affinity arrangement that has not been refuted.
    const bool hotExcl = (argc > 8) ? (std::atoi(argv[8]) != 0) : false;

    // Pool size, 0 = auto. EXISTS FOR ONE REASON: exclusive affinity reserves K cores without
    // shrinking the pool, so the remaining workers plus main plus the IOCP thread get crammed onto
    // fewer CPUs than before. Measuring that and calling it "exclusion" would be measuring
    // OVERSUBSCRIPTION. Sizing the pool to cores-K is the only way to tell them apart.
    const int workers = (argc > 9) ? std::atoi(argv[9]) : 0;

    WSADATA wsa{};
    ::WSAStartup(MAKEWORD(2, 2), &wsa);

    // BEFORE Init, and this ordering is load-bearing: a completion thread reads the flag once at
    // Run() entry, and Run() starts inside Init. Setting it afterwards reaches the workers (they
    // re-read it every idle pass) but NOT the completion threads -- which would silently measure
    // only half the intervention, the half that was already shown to be the wrong half alone.
    JLib::TaskScheduler::SetHotThreadPolicy(
        hotRt ? JLib::TaskScheduler::HotThreadPolicy::Elevated
              : JLib::TaskScheduler::HotThreadPolicy::Normal);

    // ALSO before Init, and for a second reason: StartWorker decides placement as each worker
    // starts, so the hot COUNT must already be known there for pinning to select the right ones.
    if (hot) JLib::TaskScheduler::SetHotWorkers(hot);
    JLib::TaskScheduler::SetHotWorkerPin(hotPin || hotExcl);   // exclusive implies pinning the hots
    JLib::TaskScheduler::SetHotWorkerExclusive(hotExcl);

    JLib::TaskScheduler::EnableIoReactor(true, th);
    JLib::TaskScheduler::Init(workers);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io = JLib::IoReactor::Instance();
    io.InitSockets();

    if (noSleep)
        JLib::TaskScheduler::SetIdlePolicy(JLib::TaskScheduler::IdlePolicy::NoSleep);

    std::printf("io_dispatch_latency -- %d samples, cold pause %dms, %u completion thread(s), "
                "%zu workers, idle=%s, hot=%zu, hotRT=%d, hotPin=%d, hotExcl=%d\n\n", samples,
                coldMs, th, sched.GetWorkerCount(), noSleep ? "NoSleep" : "Sleep", hot,
                (int)hotRt, (int)hotPin, (int)hotExcl);

    // Reset AFTER setup so the count covers only the measured traffic. Registration and the
    // synchronous connects do their own stealing, and counting those would flatter the result.
    JLib::StealStatsReset();

    // MAIN MUST GET OFF THE HOT CORES TOO -- it is the thread running ::send, so it is a producer
    // for exactly those workers. Leaving it eligible for a hot core would leave the arrangement
    // half-done, and half-done is the configuration that measured WORSE than doing nothing.
    JLib::TaskScheduler::ExcludeCurrentThreadFromHotCpus();

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
        g_samples.assign(samples, 0); g_hold.assign(samples, 0); g_disp.assign(samples, 0);

        for (int i = 0; i < samples; ++i) {
            g_done.store(0, std::memory_order_release);

            // COLD: let the pool go idle first, so the completion has to pay a real wake-up. This is
            // the whole point of the second pass -- without it every worker is already spinning and
            // the number flatters.
            if (cold) std::this_thread::sleep_for(std::chrono::milliseconds(coldMs));

            JLib::WaitGroup wg;
            // hiPri: with K-hot + hotRT this is what keeps the hot worker at TIME_CRITICAL while it
            // runs the completion. An ordinary task demotes it to Normal first -- see Thread.cpp.
            JLib::Spawn(OneOp(i), &wg, 1);

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
            ReportSplit();
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

        const size_t n = size_t(rounds) * kBurst;
        g_samples.assign(n, 0); g_hold.assign(n, 0); g_disp.assign(n, 0);
        for (int rd = 0; rd < rounds; ++rd) {
            std::this_thread::sleep_for(std::chrono::milliseconds(coldMs));
            JLib::WaitGroup wg;
            for (int i = 0; i < kBurst; ++i) JLib::Spawn(BurstOp(i, rd), &wg, 1);
            std::this_thread::sleep_for(std::chrono::microseconds(400));
            // One wave, back to back, so the completions land close enough to coalesce.
            for (int i = 0; i < kBurst; ++i)
                ::send(static_cast<SOCKET>(g_bc[i]), g_tx, sizeof g_tx, 0);
            sched.WaitFor(wg);
        }

        std::vector<long long> valid;
        for (long long v : g_samples) if (v > 0) valid.push_back(v);
        if (valid.empty()) std::printf("  BURST  NO TIMESTAMPS -- needs -DJLIBSCHED_IO_LOCK_STATS=ON.\n");
        else { Report("BURST", std::move(valid)); ReportSplit(); }

        for (int i = 0; i < kBurst; ++i) {
            ::closesocket(static_cast<SOCKET>(g_bc[i]));
            ::closesocket(static_cast<SOCKET>(g_bs[i]));
        }
    }

    // ---- CONCURRENT SOAK SWEEP ----------------------------------------------------------------
    // Sweeps the offered load. Look for the N where p50 starts climbing: below it the lane is
    // keeping up, at it one hot worker is saturating, above it operations are queueing. THAT is
    // the number that decides whether K>1 and hot-worker rebalancing are live questions.
    {
        std::printf("\n  concurrent soak -- N independent coroutines, steady state (not a wave)\n");
        std::printf("    recv-completion -> coroutine resumed, us\n");
        std::printf("      N     ops     p50      p90      p99      max     ops/sec\n");

        g_skewEveryNth = 0;
        for (int n : { 1, 2, 4, 8, 16, 32, 64 }) {
            const int iters = (n <= 8) ? 200 : 100;
            double secs = 0.0;
            std::vector<long long> v = SoakRun(n, iters, secs);
            if (v.empty()) {
                std::printf("    %3d  no samples (needs -DJLIBSCHED_IO_LOCK_STATS=ON)\n", n);
                continue;
            }
            const auto pk = [&](double p) {
                return v[std::min(v.size() - 1, size_t(v.size() * p))] / 1000.0;
            };
            std::printf("    %3d  %6zu  %7.2f  %7.2f  %7.2f  %7.2f  %10.0f\n",
                        n, v.size(), pk(0.50), pk(0.90), pk(0.99),
                        double(v.back()) / 1000.0, secs > 0 ? v.size() / secs : 0.0);
        }
    }

    // ---- SKEWED SOAK: is an idle hot worker sitting next to a backlogged one? ------------------
    //
    // THE QUESTION THIS EXISTS TO SETTLE. Hot->hot stealing was removed after measuring 20.6M probes
    // at K=2 and 232M at K=4 for flat latency. That result is honest but it was taken against a
    // UNIFORM load, where steering round-robins equal work across the hot set and there is nothing
    // left to rebalance by construction. It says nothing about a lane whose handlers differ in size.
    //
    // A bad tail here still would not settle it, because two opposite situations produce one:
    //
    //   CAPACITY  every hot worker saturated. Stealing moves work between equally-buried cores and
    //             changes nothing. The answer is a bigger K.
    //   BALANCE   one hot worker buried behind a long handler while a sibling spins on an empty
    //             deque. The answer is stealing, and only stealing.
    //
    // So the latency table is the SYMPTOM and the occupancy witness is the DIAGNOSIS. Read the
    // witness first; the percentiles only say how much it is costing.
    if (JLib::kHotOccStatsEnabled) {
        const int n = 32, iters = 100;
        std::printf("\n  skewed soak -- 1 in 8 resumptions burns `burn` us on the hot worker\n");
        std::printf("    idle%%  = sampled idle passes where a SIBLING hot worker had a backlog.\n");
        std::printf("    Near 0 -> steering balances, hot->hot is dead weight (keep it deleted).\n");
        std::printf("    Well above 0 -> work is sitting still beside an idle core (build it).\n");
        std::printf("      K    burn   skew   arm      p50      p90      p99   idle%%  meanDepth\n");

        // BURN SIZE IS SWEPT, and that is not thoroughness -- it is the objection. A 200us handler
        // arguably breaks the lane's own contract that lane work is short, so a result visible only
        // there would say "keep handlers short", not "build stealing". 20us is a packet parse; 5us
        // is barely more than the dispatch it rides on. If the witness only lights up at 200us the
        // honest conclusion is a documentation fix.
        // ARM IS THE INNERMOST LOOP, deliberately. Building three executables and measuring them in
        // three sessions moved the K=1 rows by 2x -- and K=1 has no sibling, so the mechanism under
        // test provably cannot act there. That was machine drift being read as a result. Adjacent
        // arms share thermal and background state; separated ones do not.
        for (long long burn : { 20000LL, 200000LL }) {
          for (int skew : { 0, 8 }) {
            for (std::size_t k : { (std::size_t)1, (std::size_t)2, (std::size_t)4 }) {
              for (int arm : { 0, 1, 4 }) {   // arm 2 dropped (== arm 1); arm 3 answered its question
                if (skew == 0 && burn != 200000LL) continue;   // the control does not vary with burn
                JLib::TaskScheduler::SetLaneHintMode(arm);
                JLib::TaskScheduler::SetHotWorkers(k);
                g_skewEveryNth = skew;
                g_skewBurnNs = burn;
                g_skewBurns.store(0, std::memory_order_relaxed);
                JLib::HotOccStatsReset();

                double secs = 0.0;
                std::vector<long long> v = SoakRun(n, iters, secs);

                long long idle = 0, withSib = 0, depth = 0;
                JLib::HotOccStatsRead(idle, withSib, depth);
                const double pct = idle ? 100.0 * (double)withSib / (double)idle : 0.0;
                const double meanD = withSib ? (double)depth / (double)withSib : 0.0;

                if (v.empty()) { std::printf("    %3zu  %5d  no samples\n", k, skew); continue; }
                const auto pk = [&](double p) {
                    return v[std::min(v.size() - 1, size_t(v.size() * p))] / 1000.0;
                };
                std::printf("    %3zu  %6.0f  %5d  %4s  %7.2f  %7.2f  %7.2f  %6.2f  %9.2f\n",
                            k, burn / 1000.0, skew,
                            (arm == 0 ? "off" : arm == 3 ? "hnt" : arm == 1 ? "hot" : arm == 4 ? "any" : "pop"),
                            pk(0.50), pk(0.90), pk(0.99), pct, meanD);
              }
            }
          }
        }
        g_skewEveryNth = 0;
        // This section moves K, and the steal-stat report below is per-configuration. Put back what
        // the command line asked for so that number describes the run the caller requested.
        JLib::TaskScheduler::SetHotWorkers(hot);
    }
    else {
        std::printf("\n  skewed soak SKIPPED -- needs -DJLIBSCHED_HOT_OCCUPANCY_STATS=ON.\n");
        std::printf("    The latency half would run, but without the witness it cannot tell\n");
        std::printf("    'hot workers saturated' from 'one buried, one idle' -- and those two\n");
        std::printf("    have opposite answers. Printing only the tail would invite the guess.\n");
    }

    // REMOTE DEQUE TOUCHES. A probe is one look at another core's deque endpoint -- the line that
    // ping-pongs when N idle workers scan each other. This is the quantity the lane split was meant
    // to reduce, and unlike the latency percentiles it is not noisy: it is a count, so it separates
    // the loop saving from steering, which the timing comparison could not.
    {
        long long probes = 0, hits = 0;
        JLib::StealStatsRead(probes, hits);
        if (!JLib::kStealStatsEnabled)
            std::printf("\n  steal stats not compiled in -- rebuild with -DJLIBSCHED_STEAL_STATS=ON\n");
        else
            std::printf("\n  remote deque touches: %lld probes, %lld hits (%.2f%% productive)\n",
                        probes, hits, probes ? 100.0 * double(hits) / double(probes) : 0.0);
    }

    io.Stop();
    ::closesocket(c); ::closesocket(s); ::closesocket(lis);
    ::WSACleanup();
    return 0;
}
