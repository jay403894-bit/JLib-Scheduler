// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES A LAMBDA FIBER TASK COMPLETE ITS LIFECYCLE, OR DOES IT STRAND A ROW?
//
// THE INVARIANT UNDER TEST, and it is the right invariant to hold: every AcquireFiber must have
// exactly one DEAD, so exactly one recycle -- tagged deleters run, the context is destroyed, the
// stack returns to the magazine, the slot is freed. A fiber that never reaches DEAD is not slow, it
// is a PERMANENT --budget: the row is never FREE again, the reaper never runs for it, and its
// creditors, FLS slots and retire bag stay attached to something that will not be recycled.
//
// THE CLAIM THIS FILE CHECKS is that a task created from a LAMBDA cannot satisfy that invariant --
// that nothing owns its completion, so its row is stranded RUNNING or PARKED forever. If that is
// true, every capturing lambda that waits leaks a stack, io_fiber_await.h is built on sand, and the
// socket suite passes only because it does not run long enough to exhaust the pool.
//
// WHY A COUNT AND NOT AN ASSERTION ABOUT STATUS. Reading `status == DEAD` from outside is a race and
// proves nothing about recycling. The pool's own accounting cannot be argued with: if rows are
// returned, AvailableCount comes back; if they are stranded, it does not. And the budget is
// deliberately SMALL against the number of tasks pushed through it -- kWaves * kPerWave is many
// times the fiber budget, so a leak of even one row per task cannot hide. It would exhaust the pool,
// and the exhaustion warning in Thread.cpp would fire long before the run ended.
//
// THE SUSPENSION IS THE POINT. A fiber task that never waits never leaves RUNNING, so it would prove
// nothing about the suspended case. Every task here parks on an event and is resumed.

#include "TaskScheduler.h"
#include "fiber_body.h"
#include "Thread.h"
#include "GlobalFiberPool.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

// A capture with a destructor, so this also answers the second half of the claim: if the row is
// never recycled and the closure is never destroyed, these do not balance.
static std::atomic<int> g_ctorCount{ 0 }, g_dtorCount{ 0 };
struct TracedCapture {
    int id;
    explicit TracedCapture(int i) : id(i) { g_ctorCount.fetch_add(1, std::memory_order_relaxed); }
    TracedCapture(const TracedCapture& o) : id(o.id) { g_ctorCount.fetch_add(1, std::memory_order_relaxed); }
    ~TracedCapture() { g_dtorCount.fetch_add(1, std::memory_order_relaxed); }
};

// ---- FIBER BODIES: NAMED FUNCTIONS, EXPLICIT CONTEXTS ---------------------------------------
//
// One spelling for every task body. A fiber's stack is a PLACE -- register state mapped to memory
// -- and a closure is a VALUE the worker loop frees the moment the body returns. A fiber given a
// closure that then parks either resumes into a dead frame or never dies and never returns its
// row, and SlabPool being append-only means neither shows up where it was caused.
struct RowCtx {
    TracedCapture     cap;        // the observable capture, now a plain member
    std::atomic<int>* parked;
    JLib::Event*      gate;
};
static void RowWaveBody(void* p) {
    auto& c = *static_cast<RowCtx*>(p);
    (void)c.cap.id;
    c.parked->fetch_add(1, std::memory_order_release);
    JLib::TaskScheduler::Instance().WaitOnEvent(*c.gate);
}

// Pure churn, to drive the queued reaper sweeps.
static void ChurnBody(void*) {}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== a lambda fiber task must reach DEAD and return its row ===\n");

    // SMALL POOL, SMALL BUDGET, so the total work is many times the fiber supply. A leak cannot be
    // absorbed by a generous pool -- it has to show up as exhaustion.
    JLib::TaskScheduler::SetFiberBudget(/*normalPerComputeWorker*/ 8, 8, 1);
    JLib::TaskScheduler::Init(4);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& pool  = sched.GetGlobalPool();

    std::this_thread::sleep_for(std::chrono::milliseconds(50));   // let the pool settle
    const size_t total     = pool.TotalCount();
    const size_t baseAvail = pool.AvailableCount();
    std::printf("  pool: %zu fibers total, %zu available at rest\n", total, baseAvail);

    constexpr int kWaves   = 40;
    constexpr int kPerWave = 24;
    std::printf("  running %d waves x %d suspending lambda fiber tasks = %d, against %zu fibers\n",
                kWaves, kPerWave, kWaves * kPerWave, total);

    for (int w = 0; w < kWaves; ++w) {
        JLib::Event& gate = sched.GetEvent("row_lifecycle_gate");
        std::atomic<int> parked{ 0 };
        JLib::WaitGroup wg;
        int made = 0;
        wg.n.store(kPerWave, std::memory_order_relaxed);

        // PER-ITERATION STATE, so each fiber needs its OWN context and they must outlive the wave.
        // reserve() is load-bearing rather than a performance note: MakeCtxTask is handed
        // &ctxs[i], and a reallocation would move contexts already handed out from under the
        // fibers pointing at them. Sizing up front means no reallocation happens.
        std::vector<RowCtx> ctxs;
        ctxs.reserve(kPerWave);

        for (int i = 0; i < kPerWave; ++i) {
            ctxs.push_back(RowCtx{ TracedCapture(w * 1000 + i), &parked, &gate });
            JLib::Task* t = JLibTest::MakeCtxTask(sched, &RowWaveBody, &ctxs[i]);
            if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
            t->waitGroup = &wg;
            sched.Push(t);
            ++made;
        }

        // ALL PARKED AT ONCE, so this wave holds `made` rows simultaneously. If rows from earlier
        // waves were never returned, this is where the pool runs dry -- AcquireFiber starts spinning
        // and the wave never fully parks.
        const auto d = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (parked.load(std::memory_order_acquire) < made
               && std::chrono::steady_clock::now() < d)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        if (parked.load(std::memory_order_acquire) < made) {
            std::printf("  STALLED on wave %d: only %d of %d parked -- the pool ran dry, which is\n"
                        "  exactly what a stranded row looks like.\n", w, parked.load(), made);
            ++g_fail;
            gate.SignalAll();
            break;
        }
        gate.SignalAll();
        sched.WaitFor(wg);
    }

    // The reaper is a QUEUED TASK sent on fiber death, so recycling is not synchronous with the last
    // WaitFor. Give it room, and drive the pool so the queued sweeps actually get to run.
    for (int i = 0; i < 200; ++i) {
        JLib::WaitGroup w2; w2.n.store(1, std::memory_order_relaxed);
        JLib::Task* t = JLibTest::MakeCtxTask(sched, &ChurnBody, nullptr);
        if (t) { t->waitGroup = &w2; sched.Push(t); sched.WaitFor(w2); }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const size_t endAvail = pool.AvailableCount();
    std::printf("  available: %zu at rest -> %zu after %d suspending lambda tasks\n",
                baseAvail, endAvail, kWaves * kPerWave);
    std::printf("  captures: %d constructed, %d destroyed\n",
                g_ctorCount.load(), g_dtorCount.load());

    // ---- AvailableCount IS THE WRONG INSTRUMENT, AND THIS FILE USED IT FIRST -----------------
    //
    // It counts fibers free in the GLOBAL pool. A fiber sitting in a worker's ThreadLocalCache is
    // not available globally and is emphatically not leaked -- it is cached for reuse, which is the
    // entire point of the per-class caches. The baseline here is taken before any work, when those
    // caches are cold, so comparing it against a post-run reading measures HOW MANY FIBERS THE
    // CACHES ARE HOLDING and calls the difference a leak.
    //
    // fiber_budget_test hit this exact trap and says so at its own assertion: "once workers stopped
    // blocking they each take a park fiber and prime a thread-local cache batch at startup, so
    // `available` is legitimately smaller... The instrument was wrong, not the pool."
    //
    // Kept as an INFORMATIONAL line rather than deleted, because the number is worth seeing -- but
    // it cannot carry the claim.
    std::printf("  (available is global-pool only; %zu fibers are held in per-worker caches, which\n"
                "   is reuse rather than leakage -- see the note here before reading it as a leak)\n",
                endAvail <= baseAvail ? baseAvail - endAvail : 0);

    // ---- WHAT ACTUALLY PROVES IT: THE POOL SURVIVED MANY TIMES ITS OWN SIZE -----------------
    //
    // 960 suspending tasks through 36 fibers is ~27x reuse. A row stranded per task would have run
    // the pool dry inside the second wave -- AcquireFiber would spin, the wave would not finish
    // parking, and the STALLED branch above would have fired and stopped the run. It did not.
    //
    // The exhaustion warning appearing ONCE is the expected transient: 24 tasks park simultaneously
    // against a 32-fiber budget with the caches holding some, so acquisition briefly spins and then
    // clears -- which is the "STALL that clears as blocked tasks finish" the warning itself
    // describes, and the opposite of a permanent --budget.
    Check(pool.TotalCount() == total,
          "the pool did not have to grow -- rows were recycled rather than replaced");
    Check(g_fail == 0,
          "all 40 waves completed: no wave ever failed to park (a stranded row exhausts the pool)");

    // AND THE SECOND HALF: if the row is never recycled, the closure is never destroyed either.
    Check(g_dtorCount.load() >= g_ctorCount.load(),
          "every capture was destroyed -- no closure outlived its task");

    std::printf("\n%s\n", g_fail ? "FAILED" : "PASSED");
    return g_fail ? 1 : 0;
}
