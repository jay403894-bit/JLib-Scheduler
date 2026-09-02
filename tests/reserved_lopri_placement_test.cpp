// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// ORDINARY WORK MUST NEVER BE PLACED IN A RESERVED WORKER'S LOPRI INBOX.
//
//   THE INVARIANT. K workers -- the always-spinning I/O lane -- never read a loPri inbox. Every
//   reader in Thread.cpp is gated on !reservedForHiPri: the batch drain, the deque publish, the
//   yield-skip pop, DrainOwnInboxesToDeques, and all three park predicates. So a loPri task placed
//   on a worker in [0, K) is not slow, it is LOST: inboxes are owner-drain-only, no thief may take
//   it, and the owner is forbidden to look.
//
//   THAT IS WHAT MAKES THIS TESTABLE AT ALL, and it is new. Until 2026-08-31 a reserved worker
//   POPPED such a task and handed it to the first compute worker -- a self-healing net. Every task
//   still ran, so a run-count test passed with the placement bug present, and the only symptom was
//   one line on stderr. With the net gone, a misplaced task simply never runs, and "did every task
//   run" becomes a direct assertion about placement.
//
//   WHY A RUNTIME TEST AND NOT JUST THE MODEL. tests/verify/workerspin_model.c has a
//   -DPLACE_ON_RESERVED build that is red, and it proves nothing about this codebase -- no model
//   reads PickNextWorker. The next person to nest the reserved-band mask inside a floor-base check
//   keeps every model red and the process green. THIS is the lock; the model is the explanation.
//
//   WHAT IT CAUGHT (2026-08-31). PickNextWorker masked [0, K) out of the awake bitmap INSIDE
//   `if (const size_t baseF = GetAwakeFloorBase())`, and the bitmap pick that consumes the mask
//   returns before that block ends. With the awake floor base at 0 the mask never ran and an
//   ordinary CorePref::Default task could be handed to a reserved index. EnableIoReactor() calls
//   SetIoHotLane(1) -> SetHotWorkers(1), so every reactor app runs K >= 1; the library default is
//   Fbase 2, which is what kept it hidden. ARM 1 IS THAT CONFIGURATION.
//
//   TWO ASSERTIONS, AND THE SECOND IS THE VACUITY GUARD. "Every task ran" is also true when
//   reservation is not in effect at all -- K clamped to 0, a build where the band does nothing --
//   and then arm 1 would be green for a reason unrelated to the mask. So the file also asserts that
//   the reserved workers ran ZERO ordinary tasks. If they ran some, there is no reservation to
//   violate and this file reports itself VACUOUS rather than green.
//
//   BOUNDED WAIT, NEVER WaitForIdle. If the invariant is broken the misplaced tasks never run, and
//   a blocking wait would turn a clear assertion failure into a hang that reads as a flaky test.
#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

static int g_fail = 0, g_vacuous = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-70s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static constexpr size_t kK     = 2;      // reserved band [0, 2)
static constexpr int    kTasks = 2000;   // round-robin visits every worker many times
static constexpr int    kMaxW  = 64;

static std::atomic<int> g_ran{ 0 };
static std::atomic<int> g_ranOn[kMaxW];

static void Payload(void*) {
    JLib::Thread* t = JLib::Thread::GetCurrent();
    const int q = t ? t->qIndex : -1;
    if (q >= 0 && q < kMaxW) g_ranOn[q].fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

static void Arm(const char* name, size_t floor) {
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();

    JLib::TaskScheduler::SetAwakeFloor(floor);
    JLib::TaskScheduler::SetIoHotLane(kK);   // K + ReservedNeverParks -- the reactor config

    // ---- STEALING OFF, BECAUSE THIS FILE IS ABOUT PLACEMENT ---------------------------------
    //
    // TWO DIFFERENT PROPOSITIONS, and this test can only see one of them. Since 9-02 a reserved
    // worker STEALS ordinary work once the lane has been quiet -- so ordinary tasks legitimately
    // run on [0,K), and this file's vacuity guard fired exactly as designed: "reserved workers ran
    // 1 ordinary tasks -- no reservation in effect".
    //
    // The guard was right and the assertion was still worth keeping, because the invariant that
    // matters is unchanged: NOTHING MAY PUSH ORDINARY WORK TO K. That is what makes a Normal task
    // reachable, and violating it is the unreachable-inbox hang. A reserved worker CHOOSING to take
    // ordinary work off a floor deque is a different thing entirely -- it is reachable by
    // definition, since the taker is the one running it.
    //
    // AND THE GUARD'S INFERENCE IS WHAT EXPIRED, not its arithmetic. "Reserved workers ran ordinary
    // tasks, therefore no reservation is in effect" was sound while K could only run what was PLACED
    // on it. It is false now: K running ordinary work is K earning its core back.
    //
    // This file measures where tasks RAN, so it cannot tell placement from theft. Rather than weaken
    // the assertion into something that would still pass under a real placement bug, stealing is
    // switched off here -- then "ran on [0,K)" means "was PLACED there" again and the inference
    // holds. The steal behaviour is a separate question and wants its own file.
    JLib::TaskScheduler::SetReservedStealing(false);

    const size_t n = sched.GetWorkerCount();
    const size_t k = JLib::TaskScheduler::GetHotWorkers();
    std::printf("\n%s (K=%zu, awake floor base=%zu, pool=%zu)\n", name, k, floor, n);

    if (n <= k + 1) { std::printf("  pool too small (n=%zu, k=%zu)\n", n, k); ++g_fail; return; }
    if (k == 0)     { std::printf("  K clamped to 0 -- nothing to reserve\n"); ++g_vacuous; return; }

    // Let the pool reach a steady state: the awake-bitmap branch is the one under test and is only
    // taken once some worker has published an awake bit. Pushing into a cold pool exercises the
    // tail fallback instead -- which always had the right `j < hotN` test -- and would make this
    // arm green for the wrong reason.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    JLib::TaskScheduler::ResetYieldCounters();
    g_ran.store(0, std::memory_order_relaxed);
    for (int i = 0; i < kMaxW; ++i) g_ranOn[i].store(0, std::memory_order_relaxed);

    int created = 0;
    for (int i = 0; i < kTasks; ++i) {
        // ORDINARY: loPri, Native, CorePref::Default. lane is routed INTO the reserved band on
        // purpose and would be a different question entirely.
        JLib::Task* t = sched.CreateTask(Payload, nullptr, /*lane*/ JLib::Lane::Normal,
                                         JLib::TaskType::Native, JLib::CorePref::Default);
        if (!t) break;
        sched.Push(t);
        ++created;
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (g_ran.load(std::memory_order_relaxed) < created
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

    const int ran = g_ran.load(std::memory_order_relaxed);
    int onReserved = 0, onCompute = 0;
    for (size_t q = 0; q < n && q < kMaxW; ++q) {
        const int c = g_ranOn[q].load(std::memory_order_relaxed);
        if (q < k) onReserved += c; else onCompute += c;
    }
    std::printf("  created %d, ran %d, on [0,%zu) = %d, on [%zu,%zu) = %d\n",
                created, ran, k, onReserved, k, n, onCompute);

    // INFORMATIONAL, NOT ASSERTED. The yield window is timing-dependent -- whether a push lands
    // while a floor worker is off the core depends on the machine, not on the code under test --
    // so asserting a count here would be a flake generator. It is printed because a mechanism whose
    // rate is never shown is a mechanism nobody can A/B: "no difference" and "it never ran" look
    // identical. ARM 2 runs with a floor, so this is where a non-zero count should appear.
    std::printf("  yield re-aim: %u push(es) aimed at a YIELDing worker, %u re-aimed\n",
                JLib::TaskScheduler::YieldAimCount(), JLib::TaskScheduler::YieldReaimCount());

    // THE INVARIANT. A task placed on a reserved worker's loPri inbox can never run: nobody may
    // drain it. So a short count IS a placement violation.
    Check(ran == created, "every ordinary task ran (none stranded on a reserved worker)");

    // THE VACUITY GUARD -- see the header.
    if (onReserved != 0) {
        std::printf("  VACUOUS: reserved workers ran %d ordinary tasks -- no reservation in "
                    "effect, so the assertion above proves nothing\n", onReserved);
        ++g_vacuous;
    }
    Check(onCompute > 0, "ordinary work reached compute workers (test is not vacuous)");
}

int main() {
    std::printf("reserved_lopri_placement_test\n");

    // Init ONCE -- there is no public Shutdown; ~TaskScheduler runs at exit. The band setters are
    // runtime setters, so both arms reconfigure the live pool.
    JLib::TaskScheduler::SetIoHotLane(kK);
    JLib::TaskScheduler::SetAwakeFloor(0);
    JLib::TaskScheduler::Init(8);            // <= 256 so PickNextWorker takes the bitmap path

    // ARM 1: the configuration the bug lived in. Floor base 0 used to skip the reserved-band mask.
    Arm("ARM 1: awake floor base 0 -- the path that used to skip the mask", 0);

    // ARM 2: floor base >= K, which was always masked and must STAY masked. Keeping only arm 1
    // would half-detect a future change that guards the mask on something else again.
    Arm("ARM 2: awake floor base >= K -- the path that was already masked", kK);

    if (g_vacuous) std::printf("\n%d arm(s) VACUOUS\n", g_vacuous);
    std::printf("\n%s\n", (g_fail || g_vacuous) ? "FAILED" : "PASSED");
    return (g_fail || g_vacuous) ? 1 : 0;
}

/* =================================================================================================
   NEGATIVE CONTROL -- RUN 2026-08-31, and the FIRST version of this file did not survive it.

   The control is the bug itself: put the reserved-band mask back inside the floor-base check in
   PickNextWorker, i.e. prefix the mask loop with `if (GetAwakeFloorBase())`, rebuild, run.

     WITH THE FIX          ARM 1 and ARM 2: created 2000, ran 2000, on [0,2) = 0.  PASSED.
     WITH THE REGRESSION   ARM 1 strands, the pool cannot go quiet, and Join() dumps:

         q  state       inbox(hi/lo/rs)
         0  NOTIFIED       0/1/0            <-- reserved, holding an ordinary task
         1  NOTIFIED       0/1/0            <-- reserved, holding an ordinary task
         ...
       exit=1

   That is the `1 AWAKE ... inbox 0/1/0` dump the old stray-net comment described, reproduced on
   demand instead of observed once.

   THE FIRST VERSION OF THIS FILE PASSED THE CONTROL, which means it tested nothing. It called
   SetHotWorkers(kK) rather than SetIoHotLane(kK). SetHotWorkers sets K and NOTHING ELSE, so the
   reserved workers PARKED, published no awake bit, and the awake bitmap never contained an index
   below K -- so PickNextWorker fell through to its tail fallback, which has always had the correct
   `j < hotN` test. The hole is only reachable when a reserved worker is AWAKE, which is what
   ReservedNeverParks buys and what SetIoHotLane turns on. Every reactor app is in that
   configuration; a test that is not, is testing the wrong function.

   IF THIS FILE IS EVER CHANGED, RE-RUN THE CONTROL. A green run here means "no ordinary task was
   stranded"; it does not mean the bitmap path was exercised. Those came apart once already.
   ============================================================================================== */
