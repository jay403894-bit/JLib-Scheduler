// QUIESCE: PUSH, THEN STOP, THEN CHECK NOTHING WAS LEFT BEHIND.
//
// == WHY THIS TEST EXISTS AND WHY IT IS SHAPED LIKE THIS ==
//
// Every latency row in this project measures HOW LONG something took. None of them can answer the
// question that actually matters for a wake protocol, because a lost wakeup and a descheduled
// thread look identical from the outside -- both are "a task that has not finished yet".
//
// THEY ARE NOT THE SAME KIND OF EVENT. A stall RESOLVES: the OS eventually runs the thread and the
// task completes. A lost wakeup is TERMINAL: the system reaches a state where work exists and
// nobody will ever look at it again, and no amount of waiting fixes it. So the discriminator is not
// duration, it is "does this complete with NO NEW INPUT?" -- which is what this test asks and what
// p50/p99/max cannot ask in principle.
//
// AND IT IS THE CASE EVERY OTHER ROW STRUCTURALLY CANNOT SEE. The benches run under CONTINUOUS
// load, where a lost wakeup is covered up by the next push waking somebody. Stopping is the whole
// experiment.
//
// == THE ASSERTION IS ARITHMETIC, NOT JUDGEMENT ==
//
// `completed == pushed`. There is no classifier here, no poll-rate heuristic, no "was the pool
// late" attribution -- the instruments that carry those have been wrong six times, and an
// instrument we wrote cannot validate the understanding it was built from. A counter and a
// comparison can. The pool dump appears only AFTER a failure, as evidence about where it stuck; it
// is never the pass/fail criterion.
//
// == THE TEST MUST NOT HELP, AND THIS IS THE EASIEST WAY TO GET IT WRONG ==
//
// It deliberately does NOT use WaitFor. A bare thread inside WaitFor runs one stolen Native task
// per poll (TryRunStolenNativeTask) -- so waiting that way would make the CALLING THREAD execute
// the very work whose delivery is under test, and a lost wakeup would complete anyway. The wait
// here is a passive sleep-and-read: no helping, no stealing, no participation of any kind.

#include "../include/TaskScheduler.h"
#include "../include/Thread.h"
#include "../include/Event.h"
#include <cstdio>
#include <atomic>
#include <thread>
#include <chrono>

using namespace JLib;

static int g_failures = 0;
static std::atomic<int> g_done{ 0 };

// Long enough that "slow" is excluded by orders of magnitude: a healthy round trip on this pool is
// ~0.5 us, so a second is two million times that. Anything still outstanding here is not late.
static constexpr int kDeadlineMs = 1000;

// PASSIVE. Polls a counter and sleeps; never touches the scheduler, never helps, never steals.
// Returns as soon as the target is reached so a healthy round costs ~0, and only pays the full
// deadline on an actual failure.
static bool WaitPassively(int target) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(kDeadlineMs);
    while (g_done.load(std::memory_order_acquire) < target) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return true;
}

// LET THE PARKABLE BAND ACTUALLY PARK. The point of every round is that the push has to BUY a wake;
// if the workers are still spinning from the previous round, the wake is free and the protocol
// under test never runs.
static void Settle() { std::this_thread::sleep_for(std::chrono::milliseconds(6)); }

static void Report(const char* phase, int round, int pushed, int completed) {
    std::printf("  FAIL  %s round %d: pushed %d, completed %d after %d ms with NOTHING ELSE PUSHED\n",
                phase, round, pushed, completed, kDeadlineMs);
    std::printf("        That is not a stall. Nothing more was coming, so the work is queued\n"
                "        somewhere nobody will look at again.\n");
    ++g_failures;
    TaskScheduler::Instance().DumpPoolState("quiesce: work outstanding with no producer");
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== quiesce: push, STOP, and check nothing was left behind ===\n");
    std::printf("    assertion is completed == pushed. No classifier, no heuristic, no helping.\n\n");

    TaskScheduler::Init(0);
    TaskScheduler& sched = TaskScheduler::Instance();

    // ---- FLOOR 0, OR THIS TEST IS VACUOUS -----------------------------------------------------
    //
    // Placement steers at the AWAKE FLOOR, and floor workers NEVER PARK. So with the default floor
    // a single push lands on a worker that is already spinning and finds the task by polling --
    // NO WAKE IS EVER BOUGHT. The latency row prints exactly that: `kernel wakes this row: 0`.
    //
    // A test of wake delivery that never buys a wake passes whether or not delivery works. Dropping
    // the floor to zero makes every worker parkable, so a push to a settled pool MUST wake somebody
    // or the task never runs.
    TaskScheduler::SetAwakeFloor(0);
    TaskScheduler::SetFloorGrowthEnabled(false);   // growth would re-create never-park workers

    std::printf("  workers=%zu   floor=0 (every worker parkable)   deadline per round = %d ms\n\n",
                sched.GetWorkerCount(), kDeadlineMs);

    // ---- PHASE A: ONE task onto a fully settled pool ------------------------------------------
    //
    // THE SHARPEST CASE, and the reason it is first. One task, everyone else asleep, nothing else
    // coming. There is no second push to cover a lost wake and no other work to make a worker look
    // again. If the wake protocol drops this, the task simply never runs -- which is precisely the
    // failure the continuous-load rows cannot expose.
    {
        constexpr int kRounds = 600;
        int worst = 0;
        // THE VACUITY CHECK. Counted across the phase and asserted non-zero below: if no push ever
        // bought a kernel wake, then delivery was never exercised and a pass means nothing. This is
        // the negative control the phase carries with it -- not a claim that the code is right, but
        // a check that the test could have noticed if it were wrong.
        TaskScheduler::ResetWakeCount();
        for (int r = 0; r < kRounds && g_failures == 0; ++r) {
            Settle();
            g_done.store(0, std::memory_order_release);
            Task* t = sched.CreateTask(+[](void*) {
                g_done.fetch_add(1, std::memory_order_release);
            }, nullptr);
            if (!t) { std::printf("  FAIL  CreateTask returned null\n"); ++g_failures; break; }
            sched.Push(t);
            if (!WaitPassively(1)) { Report("single", r, 1, g_done.load()); break; }
            ++worst;
        }
        const unsigned long long wakes = TaskScheduler::GetWakeCount();
        if (g_failures == 0) {
            std::printf("  ok    single task onto a settled pool: %d/%d rounds delivered"
                        "   (kernel wakes bought: %llu)\n", worst, kRounds, wakes);
            if (wakes == 0) {
                std::printf("  FAIL  VACUOUS: not one push bought a wake, so nothing here tested\n"
                            "        delivery. Some worker was awake and found the task by polling.\n");
                ++g_failures;
            }
        }
    }

    // ---- PHASE B: a BURST onto a settled pool -------------------------------------------------
    //
    // Many wakes at once, which is where a band-skip or a lost promotion would show. Same rule:
    // push them all, then stop dead.
    {
        constexpr int kRounds = 120;
        constexpr int kBurst  = 64;
        int ok = 0;
        for (int r = 0; r < kRounds && g_failures == 0; ++r) {
            Settle();
            g_done.store(0, std::memory_order_release);
            for (int i = 0; i < kBurst; ++i) {
                Task* t = sched.CreateTask(+[](void*) {
                    g_done.fetch_add(1, std::memory_order_release);
                }, nullptr);
                if (!t) { std::printf("  FAIL  CreateTask returned null\n"); ++g_failures; break; }
                sched.Push(t);
            }
            if (g_failures) break;
            if (!WaitPassively(kBurst)) { Report("burst", r, kBurst, g_done.load()); break; }
            ++ok;
        }
        if (g_failures == 0)
            std::printf("  ok    burst of %d onto a settled pool: %d/%d rounds fully delivered\n",
                        kBurst, ok, kRounds);
    }

    // ---- PHASE C: SUSPEND, then signal once, then stop -----------------------------------------
    //
    // The RESUME path, which is a different delivery mechanism from a fresh push and has its own
    // wake. Fibers park on an event; one SignalAll releases them; then nothing else is pushed. A
    // resume that goes to a queue nobody drains hangs here and cannot be covered by later traffic.
    //
    // THE SIGNAL DELIBERATELY RACES THE PARK. Waiters are released as soon as the count says they
    // ARRIVED, not after they are safely asleep -- the arriving/parking window is exactly where a
    // wake gets lost, so the test aims at it rather than avoiding it.
    {
        constexpr int kRounds  = 120;
        constexpr int kWaiters = 32;
        static std::atomic<int> arrived{ 0 };
        static Event* gate = nullptr;
        static std::atomic<bool> released{ false };
        int ok = 0;
        for (int r = 0; r < kRounds && g_failures == 0; ++r) {
            Settle();
            g_done.store(0, std::memory_order_release);
            arrived.store(0, std::memory_order_release);
            released.store(false, std::memory_order_release);
            // A FRESH EVENT PER ROUND: a reused one can still be signalled from the last round, and
            // then the waiters never park and the round tests nothing.
            char name[64];
            std::snprintf(name, sizeof name, "quiesce_gate_%d", r);
            gate = &sched.GetEvent(name);

            for (int i = 0; i < kWaiters; ++i) {
                Task* t = sched.CreateTask(+[](void*) {
                    arrived.fetch_add(1, std::memory_order_release);
                    TaskScheduler::Instance().WaitOnEventArmed(*gate, [] {
                        if (released.load(std::memory_order_acquire)) gate->SignalAll();
                    });
                    g_done.fetch_add(1, std::memory_order_release);
                }, nullptr, JLib::Lane::Normal, TaskType::Fiber);
                if (!t) { std::printf("  FAIL  CreateTask returned null\n"); ++g_failures; break; }
                sched.Push(t);
            }
            if (g_failures) break;

            // Wait for ARRIVAL (passively), then signal into the park window on purpose.
            const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(kDeadlineMs);
            while (arrived.load(std::memory_order_acquire) < kWaiters
                   && std::chrono::steady_clock::now() < dl)
                std::this_thread::sleep_for(std::chrono::microseconds(200));

            released.store(true, std::memory_order_release);
            gate->SignalAll();
            if (!WaitPassively(kWaiters)) { Report("resume", r, kWaiters, g_done.load()); break; }
            ++ok;
        }
        if (g_failures == 0)
            std::printf("  ok    %d fibers suspended and resumed, signal racing the park: "
                        "%d/%d rounds\n", kWaiters, ok, kRounds);
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    std::printf("    A pass is not a proof -- it says no lost wakeup FIRED in this many rounds on\n"
                "    this machine. It is evidence of a different kind from the timing rows, which\n"
                "    cannot see this class at all.\n");
    return g_failures ? 1 : 0;
}
