// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// CAN THE THIEF ACTUALLY REACH THE WORK? -- reachability, not throughput.
//
// WHY THIS FILE EXISTS, and it is a specific failure rather than a category. The steal block picks
// victims from the hint bitmaps. tryStealFrom then decides what may be taken from a victim ALREADY
// CHOSEN -- so a victim the selection step never names is unreachable no matter how correct the
// gate below it is. Selection is upstream of permission, and nothing about the code makes that
// obvious.
//
// It has already gone wrong twice in one sitting:
//
//   1. Ordinary thieves built their candidate set from backlog|parallel only. A hot worker
//      advertises a buried lane on stealHintLane and on NOTHING else, so at laneHintMode 4 -- the
//      DEFAULT, the mode whose entire purpose is letting an ordinary worker drain a backlogged
//      lane -- that victim was never selected. The feature was silently dead. 28 tests and three
//      full green passes did not notice, because nothing in the suite asked "did anyone else run
//      lane work?"
//   2. The same mistake in the other direction killed hot->hot stealing; SchedulerDynamicKTest
//      caught that one only because K's controller happens to sit downstream of it.
//
// Both were REACHABILITY bugs. Neither produces a wrong answer, a crash, or a hang -- the work
// still runs, just always on the thread that already had it. A benchmark reports that as "a bit
// slower" and a correctness suite reports it as PASS. This file is the only shape that fails.
//
// == WHAT MAKES THE ASSERTIONS DISCRIMINATING RATHER THAN DECORATIVE ==
//
// CASE A: children of a task go onto the SPAWNING WORKER'S OWN DEQUE, and a lazy split published
// by a non-worker thread goes onto the NON-WORKER LANE. Neither is push-routed anywhere. So a
// chunk observed on any thread other than the publisher's can only have arrived by a steal, and
// counting distinct executors is a direct measurement of whether the victim was reachable.
//
// CASE B carries its own negative control, which is the part that matters. With K=1 every hiPri
// task routes to worker 0 and to nowhere else -- PickNextWorker rotates the HOT set for hiPri, and
// that set is one worker. So a hiPri task seen on a second thread cannot have been pushed there;
// it was stolen off the lane. laneHintMode 4 permits exactly that and mode 0 forbids it, so the
// same workload run under both modes must give different answers. If mode 0 also spreads, the
// test is measuring something other than what it claims and should be believed about nothing.

#include "TaskScheduler.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <set>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-70s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {

std::mutex                 g_mx;
std::set<std::thread::id>  g_threads;
std::atomic<int>           g_ran{ 0 };

void Note() {
    std::lock_guard<std::mutex> lk(g_mx);
    g_threads.insert(std::this_thread::get_id());
}
void Reset() {
    std::lock_guard<std::mutex> lk(g_mx);
    g_threads.clear();
    g_ran.store(0, std::memory_order_relaxed);
}
size_t Distinct() {
    std::lock_guard<std::mutex> lk(g_mx);
    return g_threads.size();
}

// Enough arithmetic that a chunk is worth stealing and short enough that the suite stays quick.
void Burn(int n) {
    double acc = 0.0;
    for (int i = 0; i < n; ++i) acc += std::sqrt((double)(i + 1));
    if (acc < 0.0) std::printf("");     // keep it observable
}

void HiPriBody(void*) {
    Burn(4000);
    Note();
    g_ran.fetch_add(1, std::memory_order_release);
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // NoSleep PINS THE VARIABLE THIS TEST IS NOT ABOUT. Reachability ("can a thief name the victim
    // holding the work?") and wakeup ("is that thief awake to look?") are separate mechanisms, and
    // under the default Sleep policy the second hides the first completely: hiPri pushes notify
    // only the hot worker, lane wakes default to 0, so every ordinary worker stays parked and never
    // runs a sweep at all. Case B then reads 1 executor whether the selection step is correct or
    // broken -- which is exactly what it did before this line existed. Keeping the workers spinning
    // makes the question purely "was the victim selectable".
    JLib::TaskScheduler::SetIdlePolicy(JLib::TaskScheduler::IdlePolicy::NoSleep);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("steal reachability -- workers=%zu\n", sched.GetWorkerCount());

    if (sched.GetWorkerCount() < 4) {
        std::printf("  SKIPPED -- needs at least 4 workers to distinguish a steal from a push.\n");
        sched.Join();
        return 0;
    }

    // ---- CASE A: a lazy split on the NON-WORKER LANE is reachable ------------------------------
    //
    // The main thread is not a worker, so ParallelFor publishes its splits onto the shared
    // non-worker lane and advertises them with SetParallelHint under THAT lane's index. If the
    // selection step ever stops naming that index -- an easy thing to do when reasoning only about
    // workers -- main quietly runs the whole range alone and every existing test still passes.
    std::printf("\na lazy split published by a NON-WORKER is stealable\n");
    {
        Reset();
        std::vector<double> data(1u << 20, 2.0);
        sched.ParallelFor(0, (int)data.size(), [&data](int b, int e) {
            for (int i = b; i < e; ++i) data[i] = std::sqrt(data[i]) + 1.0;
            Note();
        });

        bool transformed = true;
        for (size_t i = 0; i < data.size(); i += 4096)
            if (data[i] < 2.0) { transformed = false; break; }

        std::printf("      distinct executors=%zu\n", Distinct());
        Check(transformed, "the range was actually computed");
        Check(Distinct() > 1,
              "workers reached the non-worker lane (1 would mean main ran it alone)");
    }

    // ---- CASE B: an ORDINARY worker drains a hot lane, at mode 4 and only at mode 4 ------------
    //
    // K=1, so every hiPri task routes to worker 0 and nowhere else. Anything observed on a second
    // thread was stolen off the lane -- there is no push path that could have put it there.
    constexpr int kHiPri = 600;

    // THE LANE BIT IS SAMPLED WHILE THE WORK IS IN FLIGHT, and it is not decoration. If the bit is
    // never set, an ordinary worker has nothing to select on and BOTH arms read 1 -- which looks
    // exactly like "the feature is broken" while actually meaning "the test never built the
    // condition". Those need telling apart before either number is worth anything.
    std::atomic<unsigned long long> laneSeen{ 0 };
    std::atomic<bool> sampling{ false };
    std::thread sampler([&] {
        while (!sampling.load(std::memory_order_acquire)) std::this_thread::yield();
        while (sampling.load(std::memory_order_acquire))
            laneSeen.fetch_or(JLib::TaskScheduler::LaneBacklogMask(), std::memory_order_relaxed);
    });

    auto runLane = [&](int laneMode) -> size_t {
        JLib::TaskScheduler::SetLaneHintMode(laneMode);
        Reset();
        laneSeen.store(0, std::memory_order_relaxed);
        sampling.store(true, std::memory_order_release);
        JLib::WaitGroup wg;
        wg.n.store(kHiPri, std::memory_order_relaxed);
        for (int i = 0; i < kHiPri; ++i) {
            JLib::Task* t = sched.CreateTask(HiPriBody, nullptr, /*hipri*/ 1);
            if (!t) { wg.n.fetch_sub(1, std::memory_order_release); continue; }
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        sampling.store(false, std::memory_order_release);
        std::printf("      lane bits observed while running: 0x%llx\n",
                    (unsigned long long)laneSeen.load(std::memory_order_relaxed));
        return Distinct();
    };

    JLib::TaskScheduler::SetHotWorkers(1);
    // Let the promotion land before measuring: a worker only observes hot status from its own loop.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::printf("\nan ORDINARY worker drains a backlogged hot lane (laneHintMode 4)\n");
    const size_t mode4 = runLane(4);
    std::printf("      distinct executors=%zu of %d hiPri tasks\n", mode4, kHiPri);
    Check(mode4 > 1, "lane work reached a thread it was never pushed to");

    // THE CONTROL. Mode 0 forbids ordinary workers from touching the lane, so the identical
    // workload must NOT spread. If this also comes back > 1, the assertion above proves nothing --
    // some other path is distributing hiPri work and the whole case is measuring the wrong thing.
    std::printf("\nand does NOT at laneHintMode 0 (negative control)\n");
    const size_t mode0 = runLane(0);
    std::printf("      distinct executors=%zu of %d hiPri tasks\n", mode0, kHiPri);
    Check(mode0 == 1, "mode 0 keeps lane work on the hot worker -- the control fires");

    JLib::TaskScheduler::SetLaneHintMode(4);      // leave it as it ships
    JLib::TaskScheduler::SetHotWorkers(0);

    // JOIN BEFORE THE THREAD OBJECT DIES. ~thread on a joinable thread calls std::terminate, which
    // on MSVC is a silent 0xC0000409 -- and it happens AFTER main's output, so the test prints ALL
    // CHECKS PASSED and then exits 127. A harness reading the exit code calls that a failure and a
    // human reading the log calls it a pass; they are both looking at the same run.
    //
    // The sampler must also stop BEFORE the scheduler goes away: it calls LaneBacklogMask(), which
    // reaches Instance(), which throws once the pool is gone -- from a noexcept context. Same fatal
    // shape, different cause, and this is the second time today that pairing has bitten.
    sampling.store(false, std::memory_order_release);
    if (sampler.joinable()) sampler.join();

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    sched.Join();
    return g_fail == 0 ? 0 : 1;
}
