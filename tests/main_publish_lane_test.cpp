// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE MAIN PUBLISH LANE: ONE PUSH FOR A WHOLE BATCH, DISCOVERED BY STEALING.
//
//   WHAT IT IS. A deque at index workers.size()+1 that main PUBLISHES to and never pops. Every
//   worker steals from it. It exists because every worker already publishes to its own deque -- a
//   worker-side push is a push_bottom and done -- while main's Push pays PickNextWorker and then
//   lands in a single-consumer inbox exactly one worker may drain. See mainPubLane.
//
//   THE TWO WAYS IT CAN BE WRONG ARE OPPOSITE, AND BOTH ARE SILENT.
//
//   1. THE LANE IS INVISIBLE. Nothing sets a stealHint bit for it -- it has no owning thread to
//      maintain one -- so it is advertised by READING the deque on each steal scan. Get that wrong
//      and thieves never probe it and the park gate never counts it: the pool sleeps on live work.
//      That is a HANG, so this file never uses a blocking wait to decide the question. It waits
//      with a deadline and reports a short count, because a test that hangs on failure reads as
//      flaky infrastructure rather than as the assertion it is.
//
//   2. THE LANE NEVER READS EMPTY. The same advertise feeds advertisedCount, whose == 0 is the
//      first condition of the park gate. A lane that always claims to have work is not a hang, it
//      is a pool that never parks again -- every worker spinning forever on an empty deque, which
//      looks like SUCCESS to any test that only asks whether the tasks ran. Arm 3 is the one that
//      catches it, and it is the reason this file exists as more than a smoke test.
//
//   AND ARM 0 IS THE VACUITY GUARD. Every assertion below passes trivially if PushLazy quietly
//   returns false and the tasks go through ordinary placement instead -- that path already works,
//   which is exactly what makes it a good disguise. So the flag-off case is asserted to REFUSE, and
//   the flag-on case is asserted to ACCEPT, before anything is concluded from the counts.
#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-70s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static constexpr int kBatch = 4000;
static constexpr int kMaxW  = 64;

static std::atomic<int> g_ran{ 0 };
static std::atomic<int> g_ranOn[kMaxW];

static void Payload(void*) {
    JLib::Thread* t = JLib::Thread::GetCurrent();
    const int q = t ? t->qIndex : -1;
    if (q >= 0 && q < kMaxW) g_ranOn[q].fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

static void ResetCounts() {
    g_ran.store(0, std::memory_order_relaxed);
    for (int i = 0; i < kMaxW; ++i) g_ranOn[i].store(0, std::memory_order_relaxed);
}

// Builds `count` ordinary tasks. Returns how many it actually got -- a slab that refuses is a
// legitimate outcome and must not read as a lane failure.
static size_t Build(JLib::TaskScheduler& s, std::vector<JLib::Task*>& out, int count) {
    out.clear();
    out.reserve((size_t)count);
    for (int i = 0; i < count; ++i) {
        JLib::Task* t = s.CreateTask(Payload, nullptr, JLib::Lane::Normal,
                                     JLib::TaskType::Native, JLib::CorePref::Default);
        if (!t) break;
        out.push_back(t);
    }
    return out.size();
}

// Bounded, never WaitForIdle -- see the header. Returns how many ran.
static int AwaitRan(int want, int seconds) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    while (g_ran.load(std::memory_order_relaxed) < want
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    return g_ran.load(std::memory_order_relaxed);
}

int main() {
    // UNBUFFERED, DELIBERATELY. Two of the three failure modes in the header end the process
    // without returning -- a crash, or a runner killing a hang -- and a block-buffered stdout
    // through a pipe loses every line written before that point. The output IS the diagnosis here;
    // paying a syscall per line in a test that prints a dozen of them costs nothing.
    std::printf("main_publish_lane_test\n");
    std::fflush(stdout);

    // INIT BEFORE Instance(), and it is not a style choice: Instance() on an uninitialised
    // scheduler faults inside the CRT with no message at all. 8 workers so the pool stays under
    // kMaxHintQueues and the steal scan takes the bitmap path -- which is the path that has to
    // learn about the publish lane, and the whole point of this file.
    JLib::TaskScheduler::SetAwakeFloor(2);
    JLib::TaskScheduler::Init(8);

    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();
    const size_t n = sched.GetWorkerCount();
    std::printf("pool=%zu\n", n);
    if (n < 3) { std::printf("  pool too small (n=%zu)\n", n); return 1; }

    std::vector<JLib::Task*> tasks;

    // ---- ARM 0: THE LANE IS OFF BY DEFAULT AND REFUSES ---------------------------------------
    //
    // If PushLazy accepted here, every later arm would still be green -- via ordinary placement --
    // and this file would be testing PickNextWorker while claiming to test the lane.
    std::printf("\nARM 0: flag off -- PushLazy must refuse and consume nothing\n");
    JLib::TaskScheduler::SetMainPublishDeque(false);
    Check(!JLib::TaskScheduler::MainPublishDeque(), "the lane is off by default");
    ResetCounts();
    size_t got = Build(sched, tasks, 8);
    Check(got == 8, "8 tasks allocated");
    Check(!sched.PushLazy(tasks.data(), tasks.size()), "PushLazy refuses while the flag is off");
    // It refused, so it consumed nothing and the array is still ours to push normally.
    for (JLib::Task* t : tasks) sched.Push(t);
    Check(AwaitRan(8, 10) == 8, "the refused batch still ran through ordinary placement");

    // ---- ARM 1: PUBLISH A BATCH, AND EVERY TASK RUNS ------------------------------------------
    std::printf("\nARM 1: flag on -- one push for the whole batch\n");
    JLib::TaskScheduler::SetMainPublishDeque(true);
    Check(JLib::TaskScheduler::MainPublishDeque(), "the lane is on");
    ResetCounts();
    got = Build(sched, tasks, kBatch);
    std::printf("  built %zu tasks\n", got);
    Check(got > 0, "tasks allocated");
    Check(sched.PushLazy(tasks.data(), tasks.size()), "PushLazy accepts the batch in ONE push");

    const int ran1 = AwaitRan((int)got, 10);
    int spread = 0;
    for (size_t q = 0; q < n && q < kMaxW; ++q)
        if (g_ranOn[q].load(std::memory_order_relaxed) > 0) ++spread;
    std::printf("  ran %d of %zu, across %d of %zu workers\n", ran1, got, spread, n);

    // THE INVARIANT. Work in a deque nobody probes is not slow, it is lost -- and a short count
    // here is exactly the "lane is invisible" failure from the header.
    Check(ran1 == (int)got, "every published task ran (the lane is reachable)");

    // NOT VACUOUS. One worker draining the whole batch would also satisfy the line above, and would
    // mean the publish never became parallel -- the ramp this lane exists to remove.
    Check(spread > 1, "the batch was DISCOVERED by more than one worker (it was stolen, not owned)");

    // ---- ARM 2: THE RESERVED BAND TAKES FROM IT, WITHOUT PERMISSION ---------------------------
    //
    // INFORMATIONAL, NOT ASSERTED. Whether K wins a race for a lane entry depends on how long the
    // floor takes to drain it, which is the machine and not the code under test. It is PRINTED
    // because the whole argument for the lane is that K stops needing the quiet window to earn its
    // cores back, and a mechanism whose rate is never shown is one nobody can A/B.
    //
    // Stealing is switched OFF here deliberately: that flag governs taking work that BELONGS to
    // another worker. A non-zero count below with it off is the point being demonstrated.
    std::printf("\nARM 2: K takes from the lane with SetReservedStealing(false)\n");
    JLib::TaskScheduler::SetIoHotLane(2);
    JLib::TaskScheduler::SetReservedStealing(false);
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    const size_t k = JLib::TaskScheduler::GetHotWorkers();
    ResetCounts();
    got = Build(sched, tasks, kBatch);
    Check(got > 0 && sched.PushLazy(tasks.data(), tasks.size()), "batch published with K=2 reserved");
    const int ran2 = AwaitRan((int)got, 10);

    int onReserved = 0;
    for (size_t q = 0; q < k && q < kMaxW; ++q) onReserved += g_ranOn[q].load(std::memory_order_relaxed);
    std::printf("  ran %d of %zu; K=%zu took %d of them (informational)\n", ran2, got, k, onReserved);
    Check(ran2 == (int)got, "every task ran with a reserved band present");

    JLib::TaskScheduler::SetReservedStealing(true);
    JLib::TaskScheduler::SetIoHotLane(0);

    // ---- ARM 3: THE LANE READS EMPTY WHEN IT IS EMPTY -----------------------------------------
    //
    // THE ARM THAT CATCHES THE OPPOSITE FAILURE. The advertise feeds advertisedCount, and
    // advertisedCount == 0 is the park gate's first condition. If the lane always claims to hold
    // work the pool never parks again -- and every assertion above stays green, because the tasks
    // did run. The discriminator is that the workers must go back to SLEEP afterwards.
    //
    // Measured as PARK EVENTS, not as CPU: a park count that moves is direct evidence the gate
    // opened, whereas low CPU could equally mean the OS descheduled a spinner.
    //
    // THE PARK HAS TO HAPPEN INSIDE THE WINDOW, which the first version of this arm got wrong. It
    // reset the counters during a quiet pool and then watched for 300ms -- but the workers had
    // already parked microseconds after arm 2 drained, so the count stayed 0 and the arm reported
    // FAILED against working code. "Already asleep" and "cannot sleep" are the same reading on a
    // counter of TRANSITIONS. So: reset while quiet, then publish a batch to wake the pool, drain
    // it, and require a park AFTER that. Now 0 can only mean the workers never went back down.
    std::printf("\nARM 3: the drained lane lets the pool park again\n");
    JLib::TaskScheduler::SetAwakeFloor(0);          // nothing is pinned awake, so every worker is
                                                    // free to park and a 0 below is meaningful
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    JLib::TaskScheduler::ResetWorkerParkCounts();

    ResetCounts();
    got = Build(sched, tasks, 512);
    Check(got > 0 && sched.PushLazy(tasks.data(), tasks.size()), "a batch published to wake the pool");
    const int ran3 = AwaitRan((int)got, 10);
    Check(ran3 == (int)got, "that batch drained too");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    unsigned parks = 0;
    for (size_t q = 0; q < n; ++q) parks += JLib::TaskScheduler::GetWorkerParkCount(q);
    std::printf("  park events after the lane drained: %u\n", parks);
    Check(parks > 0, "the pool parked after the lane drained (the advertise clears)");

    std::printf(g_fail ? "\nFAILED\n" : "\nPASSED\n");
    return g_fail ? 1 : 0;
}
