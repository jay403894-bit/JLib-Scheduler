// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES A FIBER STAY ON ONE WORKER?
//
// WHAT PINNING IS FOR. Nothing about a fiber's saved context is thread-specific -- the switch is a
// register save/restore and the stack travels with the fiber. But everything reached through
// `thread_local` IS thread-specific, and TLS follows the THREAD while the fiber does not. So a
// fiber that suspends on worker 3 and resumes on worker 7 silently starts reading worker 7's
// `thread_id`, worker 7's `Thread::instance`, worker 7's per-thread caches -- and any value it
// derived from those BEFORE the suspend is now wrong. Pinning removes that class of bug rather
// than asking every future call site to remember the rule. marl takes the same position
// structurally: `switchToFiber()` is documented "the fiber must belong to this worker".
//
// WHAT THIS MEASURES. Each subject records its worker index, suspends on an Event, and records it
// again on the way out. A migration is the two disagreeing. That count must be
// zero -- not small, zero, because one migration is one fiber reading another thread's locals.
//
// THE NEGATIVE CONTROL IS THE POINT OF THE FILE. A pinning test that passes because the workload
// never migrated anything proves nothing at all, and would keep passing with the mechanism deleted.
// So the identical workload runs a second time in Mode::Default, where fibers are routed by
// PickNextWorker and MUST migrate. If the control reports zero migrations, this test is VACUOUS and
// says so instead of reporting a pass -- the harness is then too weak to have detected a failure.
//
// WHY AN EVENT AND NOT CoYield. A yielded fiber goes to its own worker's deque, so whether it
// migrates depends on a thief happening to take it -- real, but probabilistic, and a weak control.
// An Event wake goes through Requeue, which round-robins in Default mode, so the control is close
// to guaranteed rather than merely likely.
//
// READING THE WORKER INDEX AFTER THE WAIT, NOT BEFORE, IS DELIBERATE AND IS THE RULE THIS FILE
// EXISTS TO PROTECT. `Thread::GetCurrent()` is a thread_local. Caching it across the suspend would
// make the test report the parking worker's identity in both samples and pass unconditionally --
// which is the same mistake, in test form, that pinning exists to prevent in the library.

#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-68s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {

constexpr int kTasks  = 24;
constexpr int kRounds = 12;

constexpr int kMaxWorkers = 512;

std::atomic<int> g_migrations{ 0 };
std::atomic<int> g_samples{ 0 };
// Which workers the subjects were actually seen on. THIS IS THE NEGATIVE CONTROL now that the
// cross-mode one is gone: "no fiber changed worker" is trivially true if every fiber ran on worker
// 0 the whole time, and a broken pool that funnelled all work to one worker would sail through the
// migration assertion. Requiring the subjects to span several workers makes the pinning claim mean
// "they stayed on their OWN worker", which is what is actually being asserted.
std::atomic<int> g_seenWorker[kMaxWorkers];
std::atomic<int> g_doneRound[kRounds];

std::string RoundName(int r) { return "pin_round_" + std::to_string(r); }

void SubjectBody(void*) {
    auto& sched = JLib::TaskScheduler::Instance();
    for (int r = 0; r < kRounds; ++r) {
        JLib::Thread* before = JLib::Thread::GetCurrent();
        const int w0 = before ? before->qIndex : -1;

        // NESTED FORK-JOIN, and it is here to make the run non-vacuous rather than to add realism.
        // TaskScheduler::WaitFor takes a DIRECT path -- the waiting fiber
        // links itself into the WaitGroup and is resumed straight off that stack, with no
        // DirectEvent, no mutex and no allocation. Nothing else in the suite reaches it: every
        // other WaitFor in these tests is called from main, which is a bare thread and takes the
        // spin-help branch instead. Without this line the direct path would be dead code that all
        // 25 tests pass without ever executing.
        //
        // It is also the harshest possible check of it. This is a fiber suspending inside a fiber
        // task, twice per round, 24 wide -- so a lost wakeup in that path does not degrade
        // anything, it hangs the test, and the watchdog below names the worker it hung on.
        // THIS BLOCK HAS ALREADY EARNED ITS PLACE. Adding it turned up a use-after-free in the
        // direct WaitFor path on the first run -- WakeAll resumed the waiting fiber BEFORE it had
        // finished reading the WaitGroup, and the group is a stack local in the frame being
        // resumed, so it was destroyed underneath the wake. Crashed 5 runs in 6. Removing this is
        // removing the only thing in the suite that reaches that path.
        {
            JLib::WaitGroup child;
            child.n.store(1, std::memory_order_relaxed);
            JLib::Task* c = sched.CreateTask(+[](void*) {}, nullptr, false,
                                             JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            if (c) { c->waitGroup = &child; sched.Push(c); sched.WaitFor(child); }
            else   { child.n.fetch_sub(1, std::memory_order_release); }
        }

        sched.WaitOnEvent(RoundName(r));

        // RE-READ. Not cached across the suspend -- see the header.
        JLib::Thread* after = JLib::Thread::GetCurrent();
        const int w1 = after ? after->qIndex : -1;

        g_samples.fetch_add(1, std::memory_order_relaxed);
        if (w0 != w1) g_migrations.fetch_add(1, std::memory_order_relaxed);
        if (w1 >= 0 && w1 < kMaxWorkers) g_seenWorker[w1].store(1, std::memory_order_relaxed);

        g_doneRound[r].fetch_add(1, std::memory_order_release);
    }
}

// Returns the migration count for one full pass of the workload.
int RunPass(const char* label) {
    auto& sched = JLib::TaskScheduler::Instance();

    g_migrations.store(0, std::memory_order_relaxed);
    g_samples.store(0, std::memory_order_relaxed);
    for (int r = 0; r < kRounds; ++r) g_doneRound[r].store(0, std::memory_order_relaxed);

    JLib::WaitGroup wg;
    wg.n.store(kTasks, std::memory_order_relaxed);

    for (int i = 0; i < kTasks; ++i) {
        JLib::Task* t = sched.CreateTask(SubjectBody, nullptr, false,
                                         JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { wg.n.fetch_sub(1, std::memory_order_release); continue; }
        t->waitGroup = &wg;
        sched.Push(t);
    }

    // Signal each round until every subject has come out of it. Signalling repeatedly rather than
    // waiting for all of them to register first is the same shape event_smoke uses: a subject that
    // parks AFTER a SignalAll has already scanned would otherwise sit there forever.
    //
    // Advancing only once g_doneRound[r] == kTasks is what makes this terminate rather than race:
    // the counter is incremented after the wait RETURNS, so reaching kTasks proves nobody is left
    // to park on round r, and no subject can park on it again -- its loop has moved on.
    // WATCHDOG. A pinned resume that nobody drains is an unrecoverable hang, and a hung test that
    // prints nothing is the least useful failure there is -- the pool dump names the worker, its
    // sleep state and which of its three queues is non-empty, which is the whole diagnosis.
    for (int r = 0; r < kRounds; ++r) {
        auto& ev = sched.GetEvent(RoundName(r));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (g_doneRound[r].load(std::memory_order_acquire) < kTasks) {
            ev.SignalAll();
            std::this_thread::sleep_for(std::chrono::microseconds(200));
            if (std::chrono::steady_clock::now() > deadline) {
                std::printf("\n  STUCK in round %d (%s): %d/%d subjects came out.\n",
                            r, label, g_doneRound[r].load(std::memory_order_acquire), kTasks);
                sched.DumpPoolState("fiber_pinning_test watchdog");
                std::printf("\n  Look for SLEEPING WITH WORK, and for which queue is non-empty:\n"
                            "  rs != 0 means a resumed fiber is parked in a queue whose owner is\n"
                            "  asleep -- and no other worker is permitted to take it.\n");
                std::fflush(stdout);
                std::_Exit(2);
            }
        }
    }

    sched.WaitFor(wg);

    const int mig = g_migrations.load(std::memory_order_relaxed);
    std::printf("  %-12s %5d suspend/resume samples, %5d migrated\n",
                label, g_samples.load(std::memory_order_relaxed), mig);
    return mig;
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    std::printf("Fiber pinning -- workers=%zu, %d fibers x %d rounds\n\n",
                sched.GetWorkerCount(), kTasks, kRounds);

    if (sched.GetWorkerCount() < 2) {
        std::printf("  Only one worker: a fiber cannot migrate, so neither arm can distinguish\n");
        std::printf("  anything. SKIPPED rather than reported as a pass.\n");
        sched.Join();
        return 0;
    }

    // ONE ARM NOW, AND THE CONTROL HAD TO CHANGE WITH IT. This test used to run the same workload
    // in Mode::Default as its negative control, and that control was the strongest kind -- 288 of
    // 288 samples migrated. It is gone because PINNING IS NO LONGER A MODE: there is no
    // configuration left in which a resumed fiber can land on another worker, so the control arm
    // would report zero migrations too and the file would correctly call itself vacuous.
    //
    // Losing a control silently is how a test rots into an assertion that cannot fail, so it is
    // replaced rather than dropped. See the spread check below.
    const int pinned = RunPass("pinned");

    std::printf("\n");

    // THE CONTROL IS CHECKED FIRST, because it decides whether the other result means anything.
    int distinct = 0;
    for (int i = 0; i < kMaxWorkers; ++i)
        if (g_seenWorker[i].load(std::memory_order_relaxed)) ++distinct;
    std::printf("  subjects were seen on %d distinct workers\n\n", distinct);

    if (distinct < 2) {
        std::printf("  VACUOUS: every subject ran on one worker, so \"no fiber changed worker\" is\n");
        std::printf("  true for a reason that has nothing to do with pinning and this file would\n");
        std::printf("  pass with the mechanism deleted. Something funnelled all the work to one\n");
        std::printf("  worker -- fix that before trusting the result below.\n");
        ++g_fail;
    } else {
        Check(distinct >= 2, "subjects spanned several workers, so pinning is not trivially true");
    }

    Check(pinned == 0, "no fiber resumed on a worker other than its home");

    if (pinned != 0) {
        std::printf("\n  A fiber changed workers across a suspend. Every `thread_local` it reads\n");
        std::printf("  after that point belongs to a different thread than the one it parked on --\n");
        std::printf("  thread_id, Thread::instance, and every per-thread cache. Find the requeue\n");
        std::printf("  path that did not route through Fiber::homeWorker: the known ones are\n");
        std::printf("  TaskScheduler::Requeue, RequeueResumedBatch, and the WANTS_YIELD push.\n");
    }

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");

    // TEARDOWN WATCHDOG. The round-loop watchdog above cannot see Join(), and Join is exactly where
    // this hung once workers could actually park: every check passed, the banner printed, and the
    // process then sat forever. A hang with no output is the least actionable failure there is, so
    // teardown gets its own timer and its own dump.
    {
        std::thread guard([&sched] {
            std::this_thread::sleep_for(std::chrono::seconds(15));
            std::printf("\n  STUCK IN Join(). Every check above passed, so this is teardown alone.\n");
            sched.DumpPoolState("fiber_pinning_test Join watchdog");
            std::printf("\n  A worker SLEEPING with a non-empty queue means a wake was lost.\n"
                        "  A worker AWAKE with empty queues means the join itself is stuck.\n");
            std::fflush(stdout);
            std::_Exit(3);
        });
        guard.detach();
    }

    sched.Join();
    return g_fail == 0 ? 0 : 1;
}
