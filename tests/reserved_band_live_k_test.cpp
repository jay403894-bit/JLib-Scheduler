// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES A WORKER NOTICE THAT K CHANGED UNDER IT?
//
// WHAT THIS EXISTS TO CATCH. Thread::Worker cached "am I in the reserved band [0,K)" ONCE, for the
// life of the thread, behind a comment that said "K is STATIC -- no controller moves it -- so this
// is read once rather than per pass". True while K is pinned, and the exact assumption adaptive K
// removes. The cached bool feeds five decisions: the stray-inbox handoff, popping the worker's own
// loPri deque, the thief's refusal to steal bulk, the loPri inbox drain gate, and the park decision.
// A stale answer is wrong in all five at once, SILENTLY: a worker that stopped being reserved goes
// on refusing ordinary work forever, and nothing crashes, hangs, or appears in a dump.
//
// WHY "DID THE TASK RUN" IS THE WRONG QUESTION, and this is the whole design of the test. Placement
// already reads K live, so after K drops to 0 ordinary work is PLACED on workers 0..K-1 either way.
// A worker with a stale `reserved` bool does not drop that work -- it hands the stray off to a
// compute worker (Thread.cpp's self-healing stray path). So every task still completes and a naive
// test passes with the bug present. The observable that actually separates them is WHICH WORKER RAN
// IT: with a stale bool, workers 0..K-1 run nothing themselves, forever.
//
// THE CONTROL IS PHASE 1 AND IT IS NOT DECORATION. Before changing K, the same workload must show
// the reserved workers running ZERO ordinary tasks. If they are already running some, reservation is
// not in effect in this build and phase 2 would pass for a reason that has nothing to do with the
// fix -- so the file reports itself VACUOUS instead of green.
#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-68s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static constexpr size_t kK       = 2;      // reserved band [0,2)
static constexpr int    kTasks   = 4000;   // enough that every worker gets a fair chance
static constexpr int    kMaxW    = 64;

static std::atomic<int> g_ranOn[kMaxW];

static void Payload(void*) {
    JLib::Thread* t = JLib::Thread::GetCurrent();
    const int q = t ? t->qIndex : -1;
    if (q >= 0 && q < kMaxW) g_ranOn[q].fetch_add(1, std::memory_order_relaxed);
}

// Submit a batch of ORDINARY (loPri, Native) tasks and wait for them, recording which worker ran
// each. Explicitly not hiPri: the question is whether a reserved worker will touch BULK work.
static void RunBatch(JLib::TaskScheduler& sched) {
    for (int i = 0; i < kMaxW; ++i) g_ranOn[i].store(0, std::memory_order_relaxed);
    JLib::WaitGroup wg;
    wg.n.store(kTasks, std::memory_order_relaxed);
    for (int i = 0; i < kTasks; ++i) {
        JLib::Task* t = sched.CreateTask(Payload, nullptr);
        if (!t) { std::printf("  CreateTask returned null\n"); ++g_fail; return; }
        t->waitGroup = &wg;
        sched.Push(t);
    }
    sched.WaitFor(wg);
}

static int RanInBand(size_t lo, size_t hi) {
    int n = 0;
    for (size_t q = lo; q < hi && q < kMaxW; ++q) n += g_ranOn[q].load(std::memory_order_relaxed);
    return n;
}
static int DistinctWorkers() {
    int n = 0;
    for (int q = 0; q < kMaxW; ++q) if (g_ranOn[q].load(std::memory_order_relaxed)) ++n;
    return n;
}

int main() {
    // ORDER IS LOAD-BEARING: SetHotWorkers before Init, and Init is STATIC and must precede
    // Instance(). Getting it the other way round (Instance() then sched.Init()) dies at
    // 0xC0000409 with no output at all, because printf's buffer never flushes.
    JLib::TaskScheduler::SetHotWorkers(kK);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();

    const size_t workers = sched.GetWorkerCount();
    std::printf("reserved band follows LIVE K -- workers=%zu, K=%zu\n\n", workers, kK);
    if (workers < kK + 2) {
        std::printf("  SKIPPED: needs at least %zu workers to have a band to move.\n", kK + 2);
        JLib::detail::TeardownForTesting(sched);
        return 0;
    }

    // ---- PHASE 1: THE CONTROL. K=2, so workers 0 and 1 must run no bulk at all ----------------
    RunBatch(sched);
    const int reservedRan1 = RanInBand(0, kK);
    const int computeRan1  = RanInBand(kK, workers);
    const int distinct1    = DistinctWorkers();
    std::printf("  phase 1 (K=%zu): reserved band ran %d, compute band ran %d, %d distinct workers\n",
                kK, reservedRan1, computeRan1, distinct1);

    if (distinct1 < 2) {
        std::printf("\n  VACUOUS: the whole batch ran on one worker, so \"reserved workers ran\n"
                    "  nothing\" is true for a reason unrelated to reservation. Fix the funnel\n"
                    "  before trusting anything below.\n");
        ++g_fail;
    }
    Check(computeRan1 > 0, "the compute band ran the work");
    Check(reservedRan1 == 0, "CONTROL: with K=2, workers 0-1 ran no ordinary task at all");

    // ---- PHASE 2: SHRINK K TO 0 AT RUNTIME -----------------------------------------------------
    //
    // Workers 0 and 1 are now ordinary compute and must start running bulk THEMSELVES. Against the
    // cached bool they stay reserved for the life of the process: the work still completes, because
    // a stale-reserved worker hands strays off rather than dropping them, but these two never run
    // any of it and the count below stays at zero.
    JLib::TaskScheduler::SetHotWorkers(0);

    // Give the pool a moment to observe it. Not a synchronisation primitive -- with the fix this is
    // visible on the very next pass; the sleep is here so a failure means "never noticed" rather
    // than "had not noticed yet", which is the difference between a bug and a race in the test.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    RunBatch(sched);
    const int reservedRan2 = RanInBand(0, kK);
    const int computeRan2  = RanInBand(kK, workers);
    std::printf("  phase 2 (K=0): former reserved band ran %d, rest ran %d\n",
                reservedRan2, computeRan2);
    Check(reservedRan2 > 0,
          "workers 0-1 took ordinary work once K dropped -- the band is read per pass");

    // ---- PHASE 3: BACK UP AGAIN, because a one-way test only proves half of it ------------------
    //
    // Reserved-ness has to be REGAINED too. A per-pass read gets this for free; anything that
    // latched "became unreserved" would pass phase 2 and fail here, which is the [[k-ratchet]]
    // shape -- keyed off a transition instead of the current state.
    // THE FLOOR MUST SHED FIRST, AND THAT IS THE CONTRACT, NOT A WORKAROUND. Phase 2 pushed 4,000
    // tasks, which grows F. K + F <= N is enforced inside the band CAS and K NEVER decrements F to
    // make room -- growing K by shrinking the floor in one step would turn a compute worker into a
    // reserved one while it may still hold a loPri leaf. So with F still grown, a request for K=2 is
    // legitimately capped; the floor sheds on a later pass and K may rise then.
    //
    // An earlier version of this test asserted straight after SetHotWorkers and "passed" only
    // because K was allowed to steal from F. It was testing the hazard, not the contract.
    JLib::TaskScheduler::ForceAwakeFloorToBase();
    JLib::TaskScheduler::SetHotWorkers(kK);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Prove the precondition rather than assuming it: if K did not come back, the assertion below
    // would be measuring a band that was never restored and would fail for the wrong reason.
    const size_t kNow = JLib::TaskScheduler::GetHotWorkers();
    Check(kNow == kK, "K was restorable once the floor was back at base");

    RunBatch(sched);
    const int reservedRan3 = RanInBand(0, kK);
    std::printf("  phase 3 (K=%zu again, live K=%zu): reserved band ran %d\n", kK, kNow, reservedRan3);
    Check(reservedRan3 == 0, "workers 0-1 went back to refusing ordinary work when K was restored");

    JLib::detail::TeardownForTesting(sched);
    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
