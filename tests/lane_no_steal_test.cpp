// A RESERVED (LANE) WORKER MUST NOT STEAL BULK WORK, AND REFUSING MUST NOT HANG.
//
// This is the negative control for a behaviour change, and it is built to FAIL ON THE OLD CODE
// rather than merely pass on the new. Before the change, TryRunStolenNativeTask let a blocked lane
// task's worker fall through to GetTask() and steal bulk -- the documented "bounded violation beats
// a deadlock" tradeoff. Now it refuses, drains only its own inbox, and the lane stalls until the
// rest of the pool clears what it waits on.
//
// TWO ASSERTIONS, and they fail for opposite reasons, which is the point:
//
//   1. THE RUN COMPLETES. This is the liveness half. The old comment claimed refusing to steal
//      "would trade a policy violation for a hang" -- if that were true of this shape, this test
//      would time out. It does not, because whatever the lane task waits on is still runnable by
//      the other N-K workers. What refusing costs is the K lane threads for the duration, not the
//      program. A regression here means the hang argument was right after all.
//
//   2. THE LANE WORKER RAN ZERO BULK TASKS. This is the policy half and the discriminating one.
//      On the old code q0 runs at least one bulk task while its lane task is blocked, so this
//      assertion is what actually distinguishes the two versions. Without it the test would pass
//      either way and prove nothing -- an outcome this project has shipped before.
//
// SHAPE. K=1, so q0 is the whole reserved band. A lane task is pushed hiPri (it lands in q0's
// reserved inbox) and blocks on a WaitGroup of ordinary bulk tasks that were pushed BEFORE it. The
// bulk tasks record which worker ran them. q0, blocked inside the lane task, reaches the spin-help
// path with an empty lane inbox -- exactly the situation the change governs.
#define NOMINMAX
#include <TaskScheduler.h>
#include "Thread.h"     // Thread::Current()/qIndex -- ground truth for which worker ran a task
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

static constexpr int kMaxW  = 128;
static constexpr int kBulk  = 4000;   // enough that stealing one is overwhelmingly likely if allowed

static std::atomic<int> g_ranOn[kMaxW];
static std::atomic<int> g_bulkDone{ 0 };
static int failures = 0;

static void Check(bool ok, const char* what) {
    std::printf("  %-64s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

static void BulkPayload(void*) {
    // Record the worker, so "did the lane run bulk?" is answered by ground truth rather than by a
    // band the scheduler already agrees with itself about.
    if (JLib::Thread* t = JLib::Thread::Current()) {
        const int q = t->qIndex;
        if (q >= 0 && q < kMaxW) g_ranOn[q].fetch_add(1, std::memory_order_relaxed);
    }
    // A little work, so the bulk does not all drain before the lane task even starts blocking.
    volatile double s = 0.0;
    for (int i = 0; i < 200; ++i) s += (double)i * 1.000001;
    (void)s;
    g_bulkDone.fetch_add(1, std::memory_order_relaxed);
}

static JLib::WaitGroup* g_bulkWg = nullptr;
static std::atomic<bool> g_laneRan{ false };
static std::atomic<bool> g_laneFinished{ false };

static void LaneBlockingPayload(void*) {
    g_laneRan.store(true, std::memory_order_release);
    // THE CONTRACT VIOLATION, ON PURPOSE. Lane work is supposed to be short and non-blocking; this
    // blocks, which is the whole scenario under test. In a debug build the scheduler prints a
    // one-shot LANE CONTRACT VIOLATED line here -- expected output, not a failure.
    JLib::TaskScheduler::Instance().WaitFor(*g_bulkWg);
    g_laneFinished.store(true, std::memory_order_release);
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("lane_no_steal: a reserved worker refuses bulk, and that does not hang\n");

    const unsigned hw = std::thread::hardware_concurrency();
    if (hw <= 4) {
        std::printf("  hardware_concurrency = %u -- too few workers for a meaningful band; skipping\n", hw);
        return 0;
    }

    // ORDER IS LOAD-BEARING: SetHotWorkers before Init, and Init is static and must precede
    // Instance(). K=1 makes q0 the entire reserved band, so "did the lane steal" is one counter.
    JLib::TaskScheduler::SetHotWorkers(1);
    JLib::TaskScheduler::Init(0);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    for (int i = 0; i < kMaxW; ++i) g_ranOn[i].store(0, std::memory_order_relaxed);

    JLib::WaitGroup bulkWg;
    bulkWg.n.store(kBulk, std::memory_order_relaxed);
    g_bulkWg = &bulkWg;

    // Bulk FIRST, so it is outstanding by the time the lane task blocks on it.
    for (int i = 0; i < kBulk; ++i) {
        JLib::Task* t = sched.CreateTask(BulkPayload, nullptr);
        if (!t) { std::printf("  CreateTask(bulk) returned null\n"); return 1; }
        t->waitGroup = &bulkWg;
        sched.Push(t);
    }

    JLib::WaitGroup laneWg;
    laneWg.n.store(1, std::memory_order_relaxed);
    JLib::Task* lane = sched.CreateTask(LaneBlockingPayload, nullptr);
    if (!lane) { std::printf("  CreateTask(lane) returned null\n"); return 1; }
    lane->waitGroup = &laneWg;
    lane->hiPri = true;             // routes to the reserved band -- this is the lane task
    sched.Push(lane);

    // BOUNDED WAIT, NOT WaitFor. If the change did introduce the hang the old comment feared, a
    // plain WaitFor would hang this process and the test would report nothing at all. Polling with
    // a deadline turns that into a FAILED line and an exit code.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool completed = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (g_laneFinished.load(std::memory_order_acquire)
            && g_bulkDone.load(std::memory_order_relaxed) >= kBulk) { completed = true; break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    Check(g_laneRan.load(std::memory_order_acquire), "the lane task actually ran (else nothing was tested)");
    Check(completed, "the run COMPLETED -- refusing to steal costs K threads, not the program");

    const int laneRanBulk = g_ranOn[0].load(std::memory_order_relaxed);
    int otherRanBulk = 0;
    for (int q = 1; q < kMaxW; ++q) otherRanBulk += g_ranOn[q].load(std::memory_order_relaxed);

    std::printf("  bulk run on q0 (the lane) = %d, on every other worker = %d\n",
                laneRanBulk, otherRanBulk);

    // THE DISCRIMINATING ASSERTION. Old code: non-zero. New code: zero.
    Check(laneRanBulk == 0, "the LANE worker ran ZERO bulk tasks while its lane task was blocked");
    // Guards the above from passing vacuously: if nothing ran anywhere, q0 == 0 proves nothing.
    Check(otherRanBulk > 0, "bulk actually ran somewhere (so the zero above is meaningful)");

    std::printf(failures ? "FAILURES: %d\n" : "ALL CHECKS PASSED\n", failures);
    return failures ? 1 : 0;
}
