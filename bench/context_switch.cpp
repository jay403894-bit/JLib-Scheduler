// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// WHAT DOES ONE FIBER CONTEXT SWITCH COST, AND DOES DIRTY YMM STATE MAKE IT WORSE?
//
// ContextSwitch.asm saves XMM6-15 with legacy-SSE `movdqa`. On an AVX machine a legacy-SSE
// instruction issued while the upper halves of YMM are live costs an SSE/AVX transition. A fiber
// that parks straight out of an AVX kernel is in exactly that state, so if the penalty is real it
// is paid on every single switch in a vectorised workload.
//
// This bench exists to decide whether that is worth doing anything about. It does NOT change the
// fiber frame: bench/win32/context_switch_variants.asm holds three routines with the identical
// 168-byte frame, the identical ten registers and the identical MXCSR/FCW slots, differing only in
// how the sixteen-byte moves are encoded. They are interchangeable on the same stacks mid-run,
// which is what lets every arm be measured inside ONE process.
//
// THE HARNESS, and why it is shaped this way:
//
//   * ONE PROCESS, ONE PAIR OF STACKS, ARMS SELECTED BY A FUNCTION POINTER. No cross-build
//     comparison anywhere. Two binaries measured minutes apart is the single easiest way to get a
//     confident wrong answer out of this machine.
//   * INTERLEAVED REPS (A,B,C,...,A,B,C,...), median per row. Frequency and thermal drift then
//     land on every row equally instead of on whichever one ran first.
//   * A SAME-VS-SAME CONTROL. Row 2 is row 1, same function pointer, same stub, different table
//     entry. If it does not read ~1.00x, nothing else in the table is a measurement.
//   * THE REAL SHIPPED ROUTINE, measured directly. Row 3 is the library's own `ContextSwitch`, not
//     a copy of it, so the table always shows what actually ships rather than what the bench
//     believes ships. It began life as a cross-module control against row 1 -- identical code in a
//     different object file, which is how you catch a difference that is really code placement --
//     and it read 0.992-1.000x against row 1 across six runs on two machines before the gated
//     vzeroupper landed in it. Row 3 against row 4 now reads the cost of the CPUID gate.
//   * A CORRECTNESS GATE BEFORE ANY TIMING. Each variant does a round trip with a known pattern in
//     XMM6-15 while the fiber deliberately clobbers all ten. A variant that saved nothing would be
//     the fastest row in the table, and without the clobber the check would pass anyway -- see the
//     note on BenchClobberXmm.
//
// PER-SWITCH COST INCLUDES the AVX stub and two indirect calls, identically in every row. The rows
// are meaningful against EACH OTHER; the absolute figure is an upper bound on the switch alone.
//
// SECTION 2 THEN ASKS THE SAME QUESTION OF THE REAL POOL. No pool exists during section 1 -- that
// is a bare fiber on one pinned thread, and 31 workers spinning up would have been noise in it.
//
// WHAT SECTION 2 MEASURES IS THE CLIENT PATH, and the name matters because "suspend/resume" would
// claim more isolation than it has. A round trip here is: park on an Event (registration and all),
// SignalAll, re-queue, a worker picks it up, resume. The Event machinery and the worker wake-up are
// IN the number -- which is why it is ~4.5 us rather than the tens of nanoseconds a switch costs.
// That is the right thing to measure for "what does this change do to my program", and the wrong
// thing to quote as the cost of a suspend.
//
// The measurement that WOULD isolate it -- drive a fiber's suspend and resume directly, no Event,
// no queue -- sits between this and section 1, and is deliberately not built. Section 1 already
// bounds the switch itself, so it would only re-derive a number we have.
//
// It can do that as a true A/B because `JLibCtxHasAvx` is a RUNTIME VARIABLE. Flipping it 0/1
// gives the pre-fix and post-fix behaviour of the shipped routine in one binary, interleaved --
// instead of comparing a build from before against a build from after, minutes apart, which is the
// comparison this project has already been burned by.
//
// EXPECT A SMALL RATIO THERE AND DO NOT READ IT AS THE EFFECT SHRINKING. That round trip is ~4.6 us
// and is roughly 85% OS wake-up under the default Sleep idle policy (a figure this project measured
// independently in 2.3.0). The saving is an ABSOLUTE ~130-150 ns -- two switches' worth -- so what
// fraction it represents is decided entirely by how hot the pool is, not by the fix.

#include <windows.h>

#include "Fiber.h"
#include "Context.h"
#include "TaskScheduler.h"   // section 2 only -- the raw table starts no pool

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#if defined(_MSC_VER)
  #include <intrin.h>
#endif

using CsFn  = void (*)(Context*, Context*);
using PreFn = void (*)();

extern "C" {
    // The three variants. See bench/win32/context_switch_variants.asm.
    void CsSse(Context*, Context*);
    void CsSseVzu(Context*, Context*);
    void CsAvx(Context*, Context*);

    // Upper-YMM state stubs: same arithmetic, differing by one vzeroupper.
    void BenchDirtyUpper();
    void BenchCleanUpper();

    // Verification only. Destroys its caller's XMM6-15 on purpose.
    void BenchClobberXmm();
    void BenchXmmRoundTrip(Context* from, Context* to, CsFn fn, void* out160);
}
// ContextSwitch itself is declared by Context.h -- that is the shipped symbol, measured as a row.

namespace {

// ---------------------------------------------------------------------------------------------
// The ping-pong.
//
// Both sides run the SAME two statements -- one stub call, one switch -- so a round trip is two
// symmetric halves and dividing by two is honest. The fiber re-dirties the upper state on its own
// side; if only the main side did, half the switches would run clean and the vzeroupper arm would
// flatter itself.
//
// The globals are volatile because the arm is swapped WHILE THE FIBER IS PARKED inside the switch.
// The call through them is opaque, so a reload would almost certainly happen anyway, but "almost
// certainly" is not worth the risk of silently measuring one arm six times. The extra load is in
// every row.
// ---------------------------------------------------------------------------------------------
Context      g_main;
JLib::Fiber  g_fiber;
CsFn volatile  g_fn  = nullptr;
PreFn volatile g_pre = nullptr;

void FiberLoop() {
    for (;;) {
        g_pre();
        g_fn(&g_fiber.ctx, &g_main);
    }
}

void StartFiber() {
    const size_t kStack = 64 * 1024;
    void* mem = ::VirtualAlloc(nullptr, kStack, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE);
    if (!mem) { std::fprintf(stderr, "VirtualAlloc failed\n"); std::exit(2); }
    g_fiber.stackBase = mem;
    g_fiber.stackSize = kStack;
    // The real Fiber::Init, not a copy of it: the frame it writes is the contract every variant
    // here restores, so borrowing it is what guarantees the bench is switching the way the
    // scheduler switches.
    g_fiber.Init(&FiberLoop);
}

// ---------------------------------------------------------------------------------------------
// Correctness gate. Runs before any timing; a failure here disqualifies the row, because the
// cheapest possible variant is one that does not actually save anything.
// ---------------------------------------------------------------------------------------------
bool VerifyVariant(CsFn fn, const char* label) {
    g_fn  = fn;
    g_pre = &BenchClobberXmm;          // the fiber stomps all ten while we are away

    alignas(16) unsigned char out[160];
    std::memset(out, 0, sizeof out);
    BenchXmmRoundTrip(&g_main, &g_fiber.ctx, fn, out);

    int bad = -1;
    for (int r = 0; r < 10 && bad < 0; ++r) {
        const unsigned char want = static_cast<unsigned char>(0xA6 + r);
        for (int b = 0; b < 16; ++b)
            if (out[r * 16 + b] != want) { bad = r; break; }
    }
    if (bad < 0) {
        std::printf("  %-34s XMM6-15 survived a clobbered round trip   ok\n", label);
        return true;
    }
    std::printf("  %-34s XMM%d CAME BACK AS 0x%02X            FAILED\n",
                label, 6 + bad, out[bad * 16]);
    return false;
}

// ---------------------------------------------------------------------------------------------
// Timing.
// ---------------------------------------------------------------------------------------------
struct Row {
    const char* label;
    CsFn        fn;
    PreFn       pre;
    const char* note;
};

double g_qpcFreq = 0.0;

// Returns nanoseconds per SWITCH (a round trip is two).
double TimeRow(const Row& row, long long iters) {
    g_fn  = row.fn;
    g_pre = row.pre;

    LARGE_INTEGER a, b;
    ::QueryPerformanceCounter(&a);
    for (long long i = 0; i < iters; ++i) {
        g_pre();
        g_fn(&g_main, &g_fiber.ctx);
    }
    ::QueryPerformanceCounter(&b);

    const double secs = double(b.QuadPart - a.QuadPart) / g_qpcFreq;
    return secs * 1e9 / double(iters * 2);
}

double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    const size_t n = v.size();
    return n & 1 ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

// ---------------------------------------------------------------------------------------------
// SECTION 2 -- the same question, through the real scheduler.
//
// The raw table above measures one instruction sequence. This measures what a fiber actually pays
// to park and be woken: suspend switch, SignalAll, the re-queue, worker pickup, resume switch.
//
// THE GATE IS A RUNTIME VARIABLE, which is what makes this honest. `JLibCtxHasAvx` decides whether
// the shipped ContextSwitch executes its vzeroupper, so flipping it 0/1 gives the OLD and NEW
// behaviour of the real routine in ONE process, one binary, interleaved -- rather than comparing a
// build from before the change against a build from after it, minutes apart, which is precisely
// the comparison that produces confident fiction on this machine.
//
// Flipping it live is safe: both values are correct at any instant. The flag only decides whether
// an ABI-legal instruction executes, never what gets saved or restored.
//
// MAIN DRIVES WITH A SIGNAL LOOP rather than a sleep-then-signal. JLib::Event is a STATELESS
// rendezvous, so a signal that lands before the fiber has parked is simply lost -- the classic
// lost wakeup. Spinning `while (wakes == seen) SignalAll()` is self-healing: extra signals on an
// empty event are one atomic exchange returning null, and the fiber cannot be missed once parked.
// The spin is in every arm equally.
// ---------------------------------------------------------------------------------------------
extern "C" unsigned char JLibCtxHasAvx;   // defined in src/win32/FiberInit.cpp

std::atomic<long long> g_wakes{ 0 };
PreFn volatile         g_schedPre = nullptr;

// Nanoseconds per suspend/resume round trip.
double TimeSuspendResume(bool gateOn, PreFn pre, long long iters) {
    JLibCtxHasAvx = gateOn ? 1u : 0u;
    g_schedPre    = pre;
    g_wakes.store(0, std::memory_order_relaxed);

    auto& sched = JLib::TaskScheduler::Instance();
    auto& ev    = sched.GetEvent("cs-bench-suspend-resume");

    JLib::WaitGroup wg;
    wg.n.fetch_add(1, std::memory_order_relaxed);

    auto* t = sched.CreateTask([&sched, &ev, iters] {
        for (long long i = 0; i < iters; ++i) {
            g_schedPre();                 // park with upper YMM live, or not
            sched.WaitOnEvent(ev);
            g_wakes.fetch_add(1, std::memory_order_release);
        }
    }, JLib::Lane::Normal, JLib::TaskType::Fiber);
    if (!t) { std::fprintf(stderr, "CreateTask returned null\n"); std::exit(2); }
    t->waitGroup = &wg;
    sched.Push(t);

    // Prime: pay the dispatch and the first park OUTSIDE the timed region, so the number is round
    // trips and not one-off submission cost.
    while (g_wakes.load(std::memory_order_acquire) == 0) ev.SignalAll();

    LARGE_INTEGER a, b;
    ::QueryPerformanceCounter(&a);
    long long seen = 1;
    while (seen < iters) {
        ev.SignalAll();
        // NO BACKOFF HERE, and that was tested rather than assumed. The obvious worry is that
        // SignalAll (head.exchange) and AddWaiter (a CAS on the same head) make main and the
        // parking fiber fight over one cacheline, turning the round trip into a measurement of
        // that fight. Inserting 64 pause instructions between attempts made it WORSE -- 5112 ns
        // against 4644 -- and loosened the same-vs-same control from 1.000x to 1.016x. The wait is
        // not contention-bound, so backing off only adds latency to noticing the wake.
        seen = g_wakes.load(std::memory_order_acquire);
    }
    ::QueryPerformanceCounter(&b);

    sched.WaitFor(wg);
    const double secs = double(b.QuadPart - a.QuadPart) / g_qpcFreq;
    return secs * 1e9 / double(iters - 1);
}

void PrintCpuBrand() {
#if defined(_MSC_VER)
    int r[4]{};
    char brand[49]{};
    __cpuid(r, 0x80000000);
    if (unsigned(r[0]) >= 0x80000004u) {
        for (int i = 0; i < 3; ++i) {
            __cpuid(r, 0x80000002 + i);
            std::memcpy(brand + i * 16, r, 16);
        }
        std::printf("cpu: %s\n", brand);
    }
#endif
}

} // namespace

int main(int argc, char** argv) {
    long long iters = 50000;   // 100,000 switches per row per rep
    int       reps  = 25;
    int       core  = 0;
    for (int i = 1; i < argc; ++i) {
        if (!std::strcmp(argv[i], "--iters") && i + 1 < argc) iters = std::atoll(argv[++i]);
        else if (!std::strcmp(argv[i], "--reps") && i + 1 < argc) reps = std::atoi(argv[++i]);
        else if (!std::strcmp(argv[i], "--core") && i + 1 < argc) core = std::atoi(argv[++i]);
        else { std::printf("usage: %s [--iters N] [--reps N] [--core N]\n", argv[0]); return 1; }
    }

    // CONDITIONS FIRST, and unconditionally. An agent-spawned or throttled process cannot produce a
    // comparable number, and a row quoted without these two facts beside it is not a result.
    {
        PROCESS_POWER_THROTTLING_STATE pt{};
        pt.Version = PROCESS_POWER_THROTTLING_CURRENT_VERSION;
        const BOOL  gotQos = ::GetProcessInformation(::GetCurrentProcess(), ProcessPowerThrottling,
                                                     &pt, sizeof pt);
        const DWORD pc  = ::GetPriorityClass(::GetCurrentProcess());
        const bool  eco = gotQos && (pt.ControlMask & PROCESS_POWER_THROTTLING_EXECUTION_SPEED)
                                 && (pt.StateMask   & PROCESS_POWER_THROTTLING_EXECUTION_SPEED);
        std::printf("conditions: priority class 0x%lX%s, EcoQoS %s\n",
                    (unsigned long)pc,
                    pc == NORMAL_PRIORITY_CLASS ? " (NORMAL)"
                      : pc == IDLE_PRIORITY_CLASS ? " (IDLE -- THROTTLED, do not report this run)"
                      : pc == HIGH_PRIORITY_CLASS ? " (HIGH)" : "",
                    eco ? "ON -- THROTTLED, do not report this run" : (gotQos ? "off" : "unknown"));
    }
    PrintCpuBrand();

#if defined(_MSC_VER)
    if (!::IsProcessorFeaturePresent(PF_AVX_INSTRUCTIONS_AVAILABLE)) {
        std::printf("this CPU has no AVX -- there is no SSE/AVX transition to measure here.\n");
        return 0;
    }
#endif

    // Pinned and prioritised: this is a single-threaded instruction-sequence measurement, so a
    // migration between a P and an E core mid-rep is pure noise. Priority is reported rather than
    // assumed -- a throttled process does not get what it asks for.
    const DWORD_PTR prev = ::SetThreadAffinityMask(::GetCurrentThread(), (DWORD_PTR)1 << core);
    ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
    std::printf("pinned to cpu %d (%s), thread priority %d, running on cpu %u\n",
                core, prev ? "ok" : "AFFINITY FAILED",
                ::GetThreadPriority(::GetCurrentThread()), ::GetCurrentProcessorNumber());

    LARGE_INTEGER f;
    ::QueryPerformanceFrequency(&f);
    g_qpcFreq = double(f.QuadPart);

    StartFiber();

    std::printf("\ncorrectness gate (each variant round-trips XMM6-15 past a deliberate clobber)\n");
    bool ok = true;
    ok &= VerifyVariant(&CsSse,        "movdqa (bench copy)");
    ok &= VerifyVariant(&ContextSwitch,"movdqa (shipped library symbol)");
    ok &= VerifyVariant(&CsSseVzu,     "vzeroupper + movdqa");
    ok &= VerifyVariant(&CsAvx,        "vmovdqa (VEX)");
    if (!ok) {
        std::printf("\nA VARIANT DOES NOT PRESERVE THE ABI. No timings printed -- they would be\n"
                    "measuring a routine that is not doing the job.\n");
        return 1;
    }

    const Row rows[] = {
        { "movdqa            dirty upper", &CsSse,         &BenchDirtyUpper, "the OLD shipped behaviour" },
        { "movdqa            dirty upper", &CsSse,         &BenchDirtyUpper, "SAME-VS-SAME CONTROL -- must read 1.00x" },
        { "SHIPPED [library] dirty upper", &ContextSwitch, &BenchDirtyUpper, "what ships NOW: CPUID-gated vzeroupper" },
        { "vzeroupper+movdqa dirty upper", &CsSseVzu,      &BenchDirtyUpper, "ungated: row 3 minus this is the gate's cost" },
        { "vmovdqa (VEX)     dirty upper", &CsAvx,         &BenchDirtyUpper, "the alternative that was not taken" },
        { "movdqa            CLEAN upper", &CsSse,         &BenchCleanUpper, "the floor -- no dirty state to transition out of" },
        // THE REGRESSION ROW, and the one most workloads actually sit on. A pool doing no vector
        // work parks with a clean upper state, so the shipped vzeroupper buys it nothing and it
        // pays the gate plus the instruction for free. This row against the one above it is that
        // cost, and it is the only row that could argue against the change.
        { "SHIPPED [library] CLEAN upper", &ContextSwitch, &BenchCleanUpper, "what the fix COSTS a non-AVX workload" },
    };
    const int kRows = int(sizeof rows / sizeof rows[0]);

    std::printf("\n%lld iterations/rep (%lld switches), %d reps, interleaved\n",
                iters, iters * 2, reps);

    // Warm-up rep, discarded: first-touch of both stacks, and the branch predictors and the
    // frequency governor all settling.
    for (int r = 0; r < kRows; ++r) (void)TimeRow(rows[r], iters / 10 + 1);

    std::vector<std::vector<double>> samples(kRows);
    int foregroundHits = 0, foregroundSamples = 0;

    // FOCUS IS NOT ALWAYS KNOWABLE, and saying so beats printing a confident 0.0%.
    // Under Windows Terminal (and anything else using ConPTY) GetConsoleWindow() returns a HIDDEN
    // pseudo-console window that can never be the foreground window, so the obvious comparison
    // reports "0% foreground" for a run the user is staring at. The visibility test is what
    // distinguishes a real conhost window from that. It matters less for this bench than for a
    // latency one -- a pinned, CPU-bound loop with no other thread to be scheduled against barely
    // moves with focus -- but a field that silently always reads 0.0% is worse than no field.
    const HWND self         = ::GetConsoleWindow();
    const bool focusKnowable = self && ::IsWindowVisible(self);

    for (int rep = 0; rep < reps; ++rep) {
        for (int r = 0; r < kRows; ++r)
            samples[r].push_back(TimeRow(rows[r], iters));
        ++foregroundSamples;
        if (focusKnowable && ::GetForegroundWindow() == self) ++foregroundHits;
    }

    const double base = Median(samples[0]);
    std::printf("\n%-32s %10s %10s %8s   %s\n", "", "median ns", "min ns", "ratio", "");
    for (int r = 0; r < kRows; ++r) {
        const double med = Median(samples[r]);
        const double mn  = *std::min_element(samples[r].begin(), samples[r].end());
        std::printf("%-32s %10.2f %10.2f %7.3fx   %s\n",
                    rows[r].label, med, mn, med / base, rows[r].note);
    }

    if (!focusKnowable) {
        std::printf("\n==> FOREGROUND unknown -- running under a pseudo-console (Windows Terminal),\n"
                    "    where this process owns no visible window to compare against.\n");
    } else {
        const double fgPct = foregroundSamples ? 100.0 * foregroundHits / foregroundSamples : 0.0;
        std::printf("\n==> FOREGROUND %.1f%% of the run\n", fgPct);
        if (fgPct > 2.0 && fgPct < 98.0)
            std::printf("    focus changed mid-run -- this run is uninterpretable, re-run it.\n");
    }

    // The decision rule, printed with the numbers so a pasted result carries it.
    const double ctrl = Median(samples[1]) / base;
    if (ctrl < 0.98 || ctrl > 1.02)
        std::printf("    SAME-VS-SAME CONTROL READS %.3fx -- the harness is not resolving this\n"
                    "    difference. Do not read anything into the other rows.\n", ctrl);

    // ---- SECTION 2: through the real scheduler ------------------------------------------------
    // The pool starts only now. Everything above is a bare fiber on one pinned thread, and 31
    // worker threads spinning up would have been noise in it.
    //
    // Main is unpinned first: it now has to share the machine with a real pool, and holding it on
    // cpu 0 against a worker placed there is an artefact this section does not want.
    ::SetThreadAffinityMask(::GetCurrentThread(), prev ? prev : ~(DWORD_PTR)0);
    ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_NORMAL);

    JLib::TaskScheduler::Init(0);   // 0 = default pool size

    struct SchedRow { const char* label; bool gate; PreFn pre; const char* note; };
    const SchedRow srows[] = {
        { "gate OFF (old)  dirty upper", false, &BenchDirtyUpper, "an AVX fiber BEFORE the change" },
        { "gate ON  (new)  dirty upper", true,  &BenchDirtyUpper, "the same fiber AFTER it" },
        { "gate OFF (old)  CLEAN upper", false, &BenchCleanUpper, "a non-AVX fiber before" },
        { "gate ON  (new)  CLEAN upper", true,  &BenchCleanUpper, "...and after -- the cost, if any" },
        { "gate OFF (old)  dirty upper", false, &BenchDirtyUpper, "SAME-VS-SAME CONTROL -- must match row 1" },
    };
    const int kSRows = int(sizeof srows / sizeof srows[0]);

    const long long srIters = 20000;
    const int       srReps  = 21;
    std::printf("\n\nEvent round trip through the pool (client path) -- %u workers, %lld round trips/rep, %d reps\n",
                (unsigned)JLib::TaskScheduler::Instance().GetWorkerCount(), srIters, srReps);
    std::printf("the gate is toggled at RUNTIME, so old and new are the same binary, interleaved\n");

    for (int r = 0; r < kSRows; ++r) (void)TimeSuspendResume(srows[r].gate, srows[r].pre, 2000);

    std::vector<std::vector<double>> ss(kSRows);
    for (int rep = 0; rep < srReps; ++rep)
        for (int r = 0; r < kSRows; ++r)
            ss[r].push_back(TimeSuspendResume(srows[r].gate, srows[r].pre, srIters));

    const double sbase = Median(ss[0]);
    std::printf("\n%-30s %10s %10s %8s   %s\n", "", "median ns", "min ns", "ratio", "");
    for (int r = 0; r < kSRows; ++r) {
        const double med = Median(ss[r]);
        const double mn  = *std::min_element(ss[r].begin(), ss[r].end());
        std::printf("%-30s %10.1f %10.1f %7.3fx   %s\n",
                    srows[r].label, med, mn, med / sbase, srows[r].note);
    }

    // THE ABSOLUTE DELTA IS THE TRANSFERABLE NUMBER, not the ratio. The ratio is a statement about
    // how much OS wake-up happened to surround the switches in THIS configuration; the nanoseconds
    // are a property of the switches themselves and travel to a hotter pool unchanged.
    std::printf("\n    saves %+.0f ns per Event round trip on an AVX fiber    (row 1 - row 2)\n"
                "    costs %+.0f ns per Event round trip on a non-AVX one  (row 4 - row 3)\n"
                "    two switches' worth either way; compare against %.0f ns of raw switch delta.\n",
                Median(ss[0]) - Median(ss[1]),
                Median(ss[3]) - Median(ss[2]),
                2.0 * (Median(samples[0]) - Median(samples[2])));

    const double sctrl = Median(ss[4]) / sbase;
    if (sctrl < 0.95 || sctrl > 1.05)
        std::printf("\n    SAME-VS-SAME CONTROL READS %.3fx -- this section is not resolving its\n"
                    "    own difference. Do not read anything into its other rows.\n", sctrl);

    JLibCtxHasAvx = 1;   // leave the process in the shipped configuration
    return 0;
}
