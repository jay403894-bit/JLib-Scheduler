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
    const JLib::IoResult r = co_await JLib::RecvRawAsync(g_bs[slot], g_brx[slot].data(), 8);
    const long long now = JLib::MonotonicNs();
    if (r.completedAtNs != 0) {
        const int k = sample * kBurst + slot;
        g_samples[k] = now - r.completedAtNs;
        if (r.flushedAtNs != 0) { g_hold[k] = r.flushedAtNs - r.completedAtNs; g_disp[k] = now - r.flushedAtNs; }
    }
    co_return;
}

JLib::Coro OneOp(int i) {
    const JLib::IoResult r = co_await JLib::RecvRawAsync(g_server, g_rx, sizeof g_rx);
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

            // THE REACTOR'S OWN WAKE: MEASURED, AND IT IS NOT THERE. Every number above starts at
            // completedAtNs, which the reactor stamps after it has already woken and dequeued -- so
            // none of it can see the cost of waking the REACTOR. That looked like a hole worth a
            // dedicated core, since the reactor blocks in an INFINITE GetQueuedCompletionStatus and
            // pays a thread wake on the first completion after idle, one layer above the worker wake
            // K-hot was built for.
            //
            // Instrumented 8-25 with a dequeue-time stamp and a no-sleep poll mode, arms alternating
            // per sample. (sleep - poll) at p50, six arms over three reps:
            //
            //     +0.20  +0.40  -1.10  -0.70  +1.20  -0.50   us
            //
            // It flips sign. A 2-5us wake would be consistently positive; this is scatter around
            // zero, so the wake is under a microsecond and there is nothing to buy. Both the stamp
            // and the poll mode were REMOVED afterwards rather than left in: the stamp added 8 bytes
            // to IoResult, which is a by-value member of every awaiter and therefore rides in every
            // I/O coroutine frame, and the poll flag was an atomic load in the completion loop.
            // Instrumentation for a settled question is pure cost.
            //
            // The signal-to-noise was poor and that bounds the claim: send->dequeue is 32-40us with
            // ~20us of p90 spread, so this says the wake is SMALL, not that it is zero. Worth
            // re-running if the transport ever gets faster than loopback TCP.
            //
            // Worth knowing separately: that 32-40us is the real end-to-end shape. The dispatch seam
            // this whole file measures is ~3.6us of it. The kernel is most of the path.
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

    // ---- LANE WAKES: can a parked worker be pulled up in time to matter? ------------------------
    //
    // laneHintMode 4 lets an ordinary worker drain a backlogged lane, and under the DEFAULT Sleep
    // policy it is inert -- the ordinary workers are parked exactly when the lane backs up. Every
    // number that made mode 4 look good came from NoSleep runs. So this arm asks whether the hint
    // can also be a reason to WAKE somebody, and get NoSleep's lane behaviour for the cost of a
    // wake per burial instead of N cores never sleeping.
    //
    // THE THING THAT DECIDES IT IS THE ~90us WAKE. A woken worker runs nothing for about that long
    // (measured 8-24), so this can only pay where the burial it is racing lasts LONGER than the
    // wake. That is the whole reason burn is swept rather than fixed:
    //
    //     burn = 20us    the backlog is gone before the sleeper is running. Expect nothing, or
    //                    worse than nothing -- a woken core that finds an empty lane.
    //     burn = 200us   the sleeper can arrive with 100us of queue still in front of it.
    //
    // A result at 200us only is not a disappointment, it is the mechanism working exactly as its
    // cost model says it must. A result at 20us would mean the wake is cheaper than 90us and the
    // README is wrong about something.
    //
    // n = how many parked workers one burial pulls up. 0 is off. Above 1 is the thundering-herd
    // question: they all steal from the SAME victim deque, so the second and third pay a wake to
    // lose a CAS. Swept rather than argued.
    //
    // == MEASURED 8-25, Sleep policy, 29 workers, median of 3, TWO independent runs ==
    //
    //   200us burials, 1-in-8            p50            p90             p99
    //     K=1  wake 0                284.3 / 273.8   757.5 / 756.2  1152.8 / 1089.6
    //          wake 1                190.4 / 183.3   476.4 / 499.3   866.4 /  810.0
    //          wake 2                131.2 / 134.2   415.8 / 359.6   683.2 /  598.5
    //          wake 4                145.9 / 135.5   442.7 / 383.5   724.8 /  638.4
    //     K=4  wake 0                 39.9 /  37.0   241.1 / 239.2   438.6 /  449.2
    //          wake 2                 46.5 /  53.3   244.2 / 243.8   454.8 /  436.7
    //
    // BOTH RUNS AGREE INSIDE A FEW PERCENT, which is what makes this readable at all -- the earlier
    // steering sweep found this row's noise floor at 5-9%, and the K=1 effect is five times that.
    //
    // AT K=1 IT IS THE LARGEST SINGLE WIN MEASURED ON THIS LANE: p50 -52%, p90 -49%, p99 -45% at
    // wake=2. At K=4 it is a pure LOSS -- p50 +30%, replicated, with the tail unchanged.
    //
    // The split is not a tuning accident, it is the mechanism's own precondition. At K=1 there IS no
    // hot sibling, so hot->hot stealing cannot exist and a parked ordinary worker is the only thing
    // in the process that can help. By K=4 the hot set absorbs the backlog itself, so the wake buys
    // nothing and still costs a notify on the hot worker plus a cold core arriving to contend for
    // tasks that already had an owner.
    //
    // wake=2 beats wake=4 in both runs, so the herd is real above two: the third and fourth wake
    // pay ~90us to lose a CAS on the same victim deque.
    //
    // NOTE WHAT THIS COMPLEMENTS. Hot->hot stealing needs K>=2 to mean anything and does nothing at
    // K=1; this does everything at K=1 and nothing at K=4. They cover disjoint configurations. And
    // K=1 is the configuration this project actually recommends -- one hot core carries NoSleep's
    // p50 win without 29 cores spinning -- so the case with no other repair available is the case
    // most likely to be running.
    //
    // SHIPS AT 0 ANYWAY, because a default that helps K=1 and costs K=4 is a policy decision and not
    // a measurement. The obvious follow-up is to gate the wake on GetHotWorkers() == 1 inside
    // WakeForLane, which is not a tuning knob but the precondition above written down.
    //
    // == 1 AND NOT <= 1. At K=0 nothing can enter a hiPri lane at all -- push routes hiPri to the
    // ordinary lane, isHotWorker is false for every worker, so the hint is never set and the wake
    // never fires. Writing <= would imply K=0 is a case being handled when it is a case that cannot
    // occur.
    {
        const int n = 32, iters = 100;
        std::printf("\n  lane wakes -- a buried hot worker pulls parked workers up to steal\n");
        std::printf("    Sleep policy. wake=0 is today's behaviour (mode 4 inert while parked).\n");
        std::printf("    K=1 is NOT a control here: one hot worker can bury itself, so wakes fire.\n");
        // probes/s = remote deque touches. A hint held LONGER is a hint thieves keep acting on, so
        // this is the one cost of widening the gap that the latency columns would not surface.
        if (JLib::kLaneWakeStatsEnabled)
            std::printf("      K    burn   skew   wake  clear      p50      p90      p99   edges/s  wakes/s   core%%%s\n",
                        JLib::kStealStatsEnabled ? "    probes/s" : "");
        else
            std::printf("      K    burn   skew   wake  clear      p50      p90      p99\n");

        constexpr int kReps = 3;
        // THE 2x3. `clear` is the lower edge of the Schmitt trigger; 3 == kLaneStealDepth-1 is a
        // single threshold, i.e. no hysteresis, i.e. the control.
        //
        // == MEASURED 8-26, and it refuted the reason the hysteresis was built ==
        //
        // THE HYPOTHESIS WAS CHATTER: that the bit oscillated across a single threshold, so widening
        // the gap would cut the edge rate. IT DID NOT. Edges/s barely moved and if anything rose:
        //
        //   K=1  20us  wake=2   clear 3 -> 16814/s   clear 1 -> 17275/s   clear 0 -> 17557/s
        //   K=1 200us  wake=2   clear 3 ->  5053/s   clear 1 ->  5159/s   clear 0 ->  5522/s
        //
        // So the deque is not hovering at the line. It swings from a full batch to EMPTY and back --
        // UpdateLaneHint is called from the inbox->deque drain with depth = count-1, the worker then
        // runs the deque dry, and the next batch re-arms it. A gap between 4 and 1 lives entirely
        // inside that swing. The edge rate is the BATCH ARRIVAL RATE of the workload, not an
        // artifact, and no threshold tuning will reduce it.
        //
        // BUT IT IS A LARGE WIN ANYWAY, for a different reason, with the wake COUNT unchanged:
        //
        //   K=1 200us wake=2      p50      p90      p99    wakes/s
        //     clear 3           214.40   437.10   653.60     9998
        //     clear 1           196.60   260.60   449.50    10210
        //     clear 0            67.70   243.50   443.90    10777
        //
        // Same number of wakes, a third of the p50. So each wake is doing more, and the reason is
        // the PERMISSION WINDOW: a wake takes ~90us to land, and an ordinary worker may only touch
        // the lane while LaneStealable says so. With clear=3 the owner often drains below 4 during
        // those 90us, the bit clears, and the worker that was woken specifically to help arrives to
        // find itself no longer allowed to -- it parks again, having cost a wake and delivered
        // nothing. Holding the bit until the deque is empty keeps the permission alive long enough
        // for the helper it summoned to actually arrive. (Inference from equal wakes + better
        // latency, not a direct count of productive wakes.)
        //
        // AND IT COSTS THIEVES NOTHING, which was the risk of widening a bit they also read. The
        // wake=0 rows vary the gap with the wake path off, and they are flat in all four
        // configurations -- 487/490/484 at K=1 200us, 25.7/25.6/25.1 at K=4 20us. No regression.
        //
        // == THE PROBE COST, added 8-26, and it decides the default ==
        //
        // A bit held longer is a bit thieves keep acting on, and that is the one cost latency alone
        // would not surface -- remote deque touches are exactly the quantity the lane split exists to
        // reduce. Counted per arm:
        //
        //   probes/s              clear 3    clear 1    clear 0
        //     K=4  20us  wake=0     90625     166347     183750     2.0x
        //     K=4 200us  wake=0     30312      62510      77837     2.6x
        //     K=1 200us  wake=2     77173      88904      95272     1.2x
        //
        // THE wake=0 ROWS SETTLE IT. There the gap doubles probes and buys NOTHING -- latency is
        // flat across all three (64.0/89.2/65.9 p50 at K=4 200us, inside its own noise). So in the
        // SHIPPED configuration, where lane wakes are off, widening the hint is pure cost.
        //
        // The two knobs are therefore COUPLED and should be set together, not independently:
        //
        //     wake = 0   ->  clear = 3    widening pays for probes and gets nothing back
        //     wake > 0   ->  clear = 0    the probe rise is 1.2-1.5x and the latency win is large
        //
        // which is why laneClearDepth's default stays 3: it is the correct partner for the wake
        // default of 0. Enabling one without the other is the misconfiguration to warn about.
        //
        // THE FULL STACK, K=1, 200us burials, against the shipped default:
        //
        //     wake=0 clear=3    p50 510.60   p90 942.60   p99 1334.70
        //     wake=2 clear=0    p50  57.70   p90 247.80   p99  449.00
        //                           8.8x         3.8x          3.0x
        //
        // for an upper-bound 100% of one core in wakes, against NoSleep's 29 cores spinning. That is
        // the original hypothesis -- NoSleep-like lane behaviour without the NoSleep tax -- and on
        // this workload it holds by a factor of about thirty.
        //
        // THE wake=0 ROWS ARE NOT PADDING. The hint bit is read by thieves as well as by the wake
        // path, so widening it changes hot->hot stealing too -- a mechanism that is default ON and
        // already measured. With the wake path switched off, those three rows vary ONLY what thieves
        // see, which is the one way to tell a stealing regression from a wake improvement.
        const int wakeArms[]  = { 0, 0, 0, 2, 2, 2 };
        const int clearArms[] = { 3, 1, 0, 3, 1, 0 };
        constexpr int kNW = 6;

        for (long long burn : { 20000LL, 200000LL }) {
          for (std::size_t k : { (std::size_t)1, (std::size_t)4 }) {
            std::vector<double> p50[kNW], p90[kNW], p99[kNW], eps[kNW], wps[kNW], prb[kNW];
            for (int rep = 0; rep < kReps; ++rep) {
              for (int a = 0; a < kNW; ++a) {
                JLib::TaskScheduler::SetLaneHintMode(4);      // the wake is a no-op under any other
                JLib::TaskScheduler::SetSteerSkip(false);
                JLib::TaskScheduler::SetLaneWake(wakeArms[a]);
                JLib::TaskScheduler::SetLaneClearDepth(clearArms[a]);
                JLib::TaskScheduler::SetHotWorkers(k);
                g_skewEveryNth = 8;
                g_skewBurnNs   = burn;
                JLib::LaneWakeStatsReset();
                JLib::StealStatsReset();   // a bit held longer means thieves PROBE longer -- the one cost
                                           // of widening the gap that latency alone would not surface

                double secs = 0.0;
                std::vector<long long> v = SoakRun(n, iters, secs);
                if (v.empty()) continue;
                const auto pk = [&](double p) {
                    return v[std::min(v.size() - 1, size_t(v.size() * p))] / 1000.0;
                };
                p50[a].push_back(pk(0.50)); p90[a].push_back(pk(0.90)); p99[a].push_back(pk(0.99));

                long long ed = 0, nf = 0;
                JLib::LaneWakeStatsRead(ed, nf);
                if (secs > 0.0) { eps[a].push_back(ed / secs); wps[a].push_back(nf / secs); }
                long long pr = 0, ht = 0;
                JLib::StealStatsRead(pr, ht);
                if (secs > 0.0) prb[a].push_back(pr / secs);
              }
            }
            const auto med = [](std::vector<double> x) {
                if (x.empty()) return -1.0;
                std::sort(x.begin(), x.end());
                return x[x.size() / 2];
            };
            for (int a = 0; a < kNW; ++a) {
                std::printf("    %3zu  %6.0f  %5d  %5d  %6d  %7.2f  %7.2f  %7.2f",
                            k, burn / 1000.0, 8, wakeArms[a], clearArms[a],
                            med(p50[a]), med(p90[a]), med(p99[a]));
                if (JLib::kLaneWakeStatsEnabled) {
                    // core% = wakes/sec x ~90us. An UPPER BOUND, not a measurement: 90us is the
                    // cold-wake LATENCY, not CPU burn, and the woken worker goes on to do real work.
                    // It is here for one comparison only -- against NoSleep's 29 spinning cores,
                    // i.e. 2900% -- because that is what decides whether this is a bargain.
                    const double w = med(wps[a]);
                    std::printf("  %8.0f %8.0f  %6.2f", med(eps[a]), w, w * 90e-6 * 100.0);
                    if (JLib::kStealStatsEnabled) std::printf("  %10.0f", med(prb[a]));
                }
                std::printf("\n");
            }
          }
        }
        g_skewEveryNth = 0;
        JLib::TaskScheduler::SetLaneWake(0);
        JLib::TaskScheduler::SetLaneClearDepth(3);
        JLib::TaskScheduler::SetHotWorkers(hot);
    }

    // ---- THE UPPER EDGE: can a LOWER set threshold harvest the residual imbalance? --------------
    //
    // THE LEAD COMES FROM THE OCCUPANCY WITNESS, not from a hunch. At K=4 with hot->hot stealing on
    // it measured idle% 73.19 and meanDepth 2.45: a hot worker idle on nearly three quarters of its
    // idle passes, beside a sibling holding ~2.45 tasks. kLaneStealDepth is 4, so those backlogs are
    // never advertised and never stolen. Measured imbalance, sitting below the line.
    //
    // WHY 4 WAS PICKED, and why the objection is now testable rather than decisive: a thief taking
    // the owner's warm task is the v1 steal-hint failure -- 22,000 probes against a 9,164 baseline,
    // and it also defeats the completion steering that put the task there. But 4 was chosen when set
    // and clear were THE SAME NUMBER. With a Schmitt trigger they are independent, and set=2/clear=0
    // is a shape that could not be expressed then: advertise sooner, but hold the advertisement
    // through the whole drain instead of flapping.
    //
    // K=2 AND K=4 ONLY. The hypothesis is about work sitting on a SIBLING, so K=1 cannot test it --
    // there is no sibling, and the witness reads idle% 0.00 there in every arm.
    //
    // WATCH probes/s AS HARD AS THE PERCENTILES. A probe explosion at set=2 IS the v1 failure
    // reproducing, and it would show there before it showed in latency.
    //
    // == RESULT 8-26: NO. kLaneStealDepth stays 4, and now it stands on a measurement. ==
    //
    // Stealing alone (wake=0), which is the pure test of the hypothesis:
    //
    //   K  burn   set/clear      p50      p90      p99   probes/s
    //   4    20      4/3        25.60    57.30    86.30     91595
    //   4    20      2/0        23.90    60.70    93.40    210528   2.3x
    //   4   200      4/3        60.60   266.30   444.90     44364
    //   4   200      2/0        72.10   245.70   442.00     84987   1.9x
    //   2   200      4/3       214.80   484.10   717.30     23437
    //   2   200      2/0       213.50   487.90   804.30     31551   1.3x
    //
    // p50 a wash, the TAIL WORSE in three rows of four, probes roughly doubled -- the v1 failure
    // reproducing in exactly the column it was predicted in.
    //
    // WHY THE WITNESS'S 73% IDLE DOES NOT CASH IN, which is the part worth keeping: a sibling
    // holding ~2.45 tasks is about to run them. Taking one costs a contended CAS against the owner
    // and discards the steering that placed it there, and those roughly cancel the wait it saves.
    // Shallow imbalance is VISIBLE WITHOUT BEING PROFITABLE. So the residual idle% is now closed
    // rather than merely explained -- it is not a missed opportunity, it is the correct amount of
    // imbalance to tolerate.
    //
    // With wakes on, set=2 is mildly better (K=4 200us p99 398.30 -> 357.70) and pays 24100 ->
    // 39996 wakes/s for it. Same conclusion by a different route.
    {
        const int n = 32, iters = 100;
        std::printf("\n  lane SET threshold -- does advertising sooner reach the shallow imbalance?\n");
        std::printf("    K=4 witness said: idle 73%%, sibling meanDepth 2.45, below the set depth of 4.\n");
        std::printf("    probes/s is the v1 steal-hint failure mode; watch it, not just the tail.\n");
        std::printf("      K    burn    set  clear   wake      p50      p90      p99   edges/s  wakes/s    probes/s\n");

        constexpr int kReps = 3;
        const int setArms[]   = { 4, 2, 4, 2 };
        const int clearArms[] = { 3, 0, 0, 0 };
        const int wakeArms[]  = { 0, 0, 2, 2 };
        constexpr int kNA = 4;

        for (long long burn : { 20000LL, 200000LL }) {
          for (std::size_t k : { (std::size_t)2, (std::size_t)4 }) {
            std::vector<double> p50[kNA], p90[kNA], p99[kNA], eps[kNA], wps[kNA], prb[kNA];
            for (int rep = 0; rep < kReps; ++rep) {
              for (int a = 0; a < kNA; ++a) {
                JLib::TaskScheduler::SetLaneHintMode(4);
                JLib::TaskScheduler::SetSteerSkip(false);
                JLib::TaskScheduler::SetLaneSetDepth(setArms[a]);
                JLib::TaskScheduler::SetLaneClearDepth(clearArms[a]);
                JLib::TaskScheduler::SetLaneWake(wakeArms[a]);
                JLib::TaskScheduler::SetHotWorkers(k);
                g_skewEveryNth = 8;
                g_skewBurnNs   = burn;
                JLib::LaneWakeStatsReset();
                JLib::StealStatsReset();

                double secs = 0.0;
                std::vector<long long> v = SoakRun(n, iters, secs);
                if (v.empty() || secs <= 0.0) continue;
                const auto pk = [&](double p) {
                    return v[std::min(v.size() - 1, size_t(v.size() * p))] / 1000.0;
                };
                p50[a].push_back(pk(0.50)); p90[a].push_back(pk(0.90)); p99[a].push_back(pk(0.99));
                long long ed = 0, nf = 0, pr = 0, ht = 0;
                JLib::LaneWakeStatsRead(ed, nf);
                JLib::StealStatsRead(pr, ht);
                eps[a].push_back(ed / secs); wps[a].push_back(nf / secs); prb[a].push_back(pr / secs);
              }
            }
            const auto med = [](std::vector<double> x) {
                if (x.empty()) return -1.0;
                std::sort(x.begin(), x.end());
                return x[x.size() / 2];
            };
            for (int a = 0; a < kNA; ++a)
                std::printf("    %3zu  %6.0f  %5d  %5d  %5d  %7.2f  %7.2f  %7.2f  %8.0f %8.0f  %10.0f\n",
                            k, burn / 1000.0, setArms[a], clearArms[a], wakeArms[a],
                            med(p50[a]), med(p90[a]), med(p99[a]),
                            med(eps[a]), med(wps[a]), med(prb[a]));
          }
        }
        g_skewEveryNth = 0;
        JLib::TaskScheduler::SetLaneSetDepth(4);
        JLib::TaskScheduler::SetLaneClearDepth(3);
        JLib::TaskScheduler::SetLaneWake(0);
        JLib::TaskScheduler::SetHotWorkers(hot);
    }

    // ---- STEERING: is it cheaper to AVOID a buried hot worker than to STEAL BACK OFF one? -------
    //
    // stealHintLane is published by the buried worker so an idle sibling can find it. The completion
    // thread is choosing placement at that exact moment and was ignoring it -- pushSteered rotates
    // blind. So the same backlog is discovered twice: once by the producer, who could simply not put
    // work there, and once by a thief, who pays a probe, a contended CAS against the owner, and a
    // lost cache line to take it back.
    //
    // FOUR ARMS, because "does skipping help" and "does skipping REPLACE stealing" are different
    // questions and only the 2x2 separates them:
    //
    //     lane=off steer=off   neither mechanism -- the floor
    //     lane=any steer=off   stealing repairs the placement (what ships today)
    //     lane=off steer=on    the producer avoids it, nobody repairs
    //     lane=any steer=on    both
    //
    // If (off,on) reaches (any,off), the repair was buying what a free load could have. If (any,on)
    // beats both, they cover different windows -- steering can only act on tasks it is placing NOW,
    // and nothing it does helps a task already sitting in a buried worker's deque.
    //
    // K=1 IS THE A/A CONTROL AND IT IS NOT DECORATION. With one hot worker there is nobody to skip
    // to, so pushSteered falls back to the plain rotation and the steer arm is bit-identical to the
    // control. Any K=1 separation is drift, and this project has already read drift as a result once
    // (2x on exactly these rows, from measuring three arms as three executables).
    //
    // == MEASURED 8-25, Sleep policy, 29 workers, median of 3 interleaved reps ==
    //
    // READ THE CONTROL FIRST AND IT DISQUALIFIES MOST OF THE TABLE. The 20us rows carry a 77% p50
    // spread across four identical K=1 arms; nothing under 2x is readable there. The 200us skewed
    // rows are tight -- 5% at p50, 6% at p90, 9% at p99 -- and those are the only rows below that
    // are worth reading. This is why the control is swept rather than assumed: the noise floor is
    // not a property of the harness, it is a property of the ROW.
    //
    //   200us, skew 1-in-8            p50      p90      p99
    //     K=2  neither               92.3    547.2   1120.8
    //          steer only            73.2    476.6    850.1     p99 -24%
    //          stealing only         97.1    395.8    689.2     p99 -38%
    //          both                  79.5    382.5    660.8     p99 -41%
    //     K=4  neither               33.6    392.4    868.1
    //          steer only            31.1    280.4    683.7     p90 -29%, p99 -21%
    //          stealing only         37.8    241.9    485.8     p90 -38%, p99 -44%
    //          both                  36.4    248.1    458.4     p99 -47%
    //
    // THE SKIP IS REAL AND IT IS REDUNDANT. Against nothing it is worth 21-29% at the tail, far
    // outside that 9% control -- so the producer genuinely was throwing work at a buried worker.
    // But hot->hot stealing beats it on the same rows, and once stealing is on the skip adds
    // 4-6%, which is INSIDE the control and therefore not a result.
    //
    // WHY THE REPAIR BEATS THE AVOIDANCE, which was not the expected direction: the producer can
    // only act on the batch in its hand. The tasks that actually make the tail are the ones ALREADY
    // in the buried worker's deque -- placed before it went dark, and by definition unreachable to
    // anyone but a thief. Skipping stops the queue growing; stealing empties it. Those are
    // different windows, and the second one is where the microseconds are.
    //
    // So it ships OFF: under the default laneHintMode 4 it buys nothing measurable, and a branch
    // that buys nothing does not belong on by default. It stays in the tree because it is worth
    // real time when hot->hot stealing is disabled (mode 0), and because deleting the measurement
    // along with the code is how the same question gets asked a third time.
    {
        const int n = 32, iters = 100;
        std::printf("\n  steering skip -- producer avoids hot workers advertising a backlog\n");
        std::printf("    K=1 rows are the A/A control: no sibling to skip to, so both arms are the\n");
        std::printf("    same code path. Separation there is noise and calibrates the rest.\n");
        std::printf("      K    burn   skew   lane  steer      p50      p90      p99\n");

        // REPEATED AND MEDIANED, and that is not caution -- a single interleaved pass ALREADY
        // produced a 2.6x p50 spread across the four K=1 arms, which are the same code path. Arms
        // adjacent in time is necessary and it is not sufficient; a run that lands on a background
        // process is still a run. Median of REPS, arms still innermost.
        constexpr int kReps = 3;
        constexpr int kArms = 4;                     // (lane off/any) x (steer off/on)
        const char* armLane[kArms] = { "off", "off", "any", "any" };
        const char* armSteer[kArms] = { "off", "on",  "off", "on"  };

        for (long long burn : { 20000LL, 200000LL }) {
          for (int skew : { 0, 8 }) {
            for (std::size_t k : { (std::size_t)1, (std::size_t)2, (std::size_t)4 }) {
                if (skew == 0 && burn != 200000LL) continue;
                std::vector<double> p50[kArms], p90[kArms], p99[kArms];

                for (int rep = 0; rep < kReps; ++rep) {
                  for (int a = 0; a < kArms; ++a) {
                    JLib::TaskScheduler::SetLaneHintMode(a < 2 ? 0 : 4);
                    JLib::TaskScheduler::SetSteerSkip((a & 1) != 0);
                    JLib::TaskScheduler::SetHotWorkers(k);
                    g_skewEveryNth = skew;
                    g_skewBurnNs   = burn;

                    double secs = 0.0;
                    std::vector<long long> v = SoakRun(n, iters, secs);
                    if (v.empty()) continue;
                    const auto pk = [&](double p) {
                        return v[std::min(v.size() - 1, size_t(v.size() * p))] / 1000.0;
                    };
                    p50[a].push_back(pk(0.50));
                    p90[a].push_back(pk(0.90));
                    p99[a].push_back(pk(0.99));
                  }
                }

                const auto med = [](std::vector<double> x) {
                    if (x.empty()) return -1.0;
                    std::sort(x.begin(), x.end());
                    return x[x.size() / 2];
                };
                for (int a = 0; a < kArms; ++a)
                    std::printf("    %3zu  %6.0f  %5d   %4s  %5s  %7.2f  %7.2f  %7.2f\n",
                                k, burn / 1000.0, skew, armLane[a], armSteer[a],
                                med(p50[a]), med(p90[a]), med(p99[a]));
            }
          }
        }
        g_skewEveryNth = 0;
        JLib::TaskScheduler::SetSteerSkip(false);
        JLib::TaskScheduler::SetLaneHintMode(4);
        JLib::TaskScheduler::SetHotWorkers(hot);
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
