// A PARKED RESERVED WORKER MUST BE WOKEN BY A LANE PUSH.
//
// The shared intake belongs to NO WORKER. Every other queue in this scheduler has an owner whose own
// pass will eventually look at it; this one is reachable only by a reserved worker that chooses to,
// and a parked band chooses nothing. So `PushLaneIntake` notifies, and that notify is the only thing
// standing between a full intake and a permanently asleep lane.
//
// WHAT THIS IS ACTUALLY TESTING, because it is not what it first looks like.
//
// `LaneIntakeIdle()` reads moodycamel's size_approx(), which can LAG a concurrent enqueue -- so a
// worker can read "idle" while an item is already in flight and commit to sleeping. That is not a
// bug on its own: the producer enqueues, THEN notifies, and the park is a permit handshake that
// cannot lose a wake, so the worker that parked on a stale reading is pulled straight back out.
//
// So the race is closed by the NOTIFY, not by the predicate. Which makes the notify the thing worth
// pinning: a future reader who sees `!LaneIntakeIdle()` in all three park predicates could
// reasonably conclude the notify in PushLaneIntake is redundant and delete it. It is not, and this
// file is what says so.
//
// IT ALSO EXISTS BECAUSE THIS PATH IS DORMANT BY DEFAULT. SetIoHotLane pins K awake, so a shipped
// pool never parks a reserved worker and never exercises any of this. Code that only runs in a
// configuration nobody selects is code that rots -- so this test selects it explicitly, with
// SetHotWorkers (reserve WITHOUT never-park) rather than SetIoHotLane.
//
// A DEADLINE, NOT WaitForIdle. The failure being tested for is a task that never runs, and a test
// that waits forever for it does not fail -- it hangs, spinning workers until something kills it.
// That has cost this project more than a thousand CPU-seconds in one sitting. A bounded wait turns
// the hang into a reported failure, which is the whole difference between a diagnosis and a mystery.

#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-72s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static constexpr size_t kK      = 2;
static constexpr int    kRounds = 400;   // enough attempts to land inside the enqueue/park window

static std::atomic<int> g_ran{ 0 };
static std::atomic<int> g_onReserved{ 0 };
static void LaneBody(void*) {
    Thread* t = Thread::GetCurrent();
    const size_t k = TaskScheduler::GetHotWorkers();
    if (t && (size_t)t->qIndex < k) g_onReserved.fetch_add(1, std::memory_order_relaxed);
    g_ran.fetch_add(1, std::memory_order_release);
}

// How long a healthy wake takes, with enormous headroom. A futex wake is ~5.5 us here; if a push
// has not produced a run in 2 SECONDS the wake was lost, not slow, and no amount of further waiting
// changes that answer.
static bool AwaitRun(int want, int ms = 2000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (g_ran.load(std::memory_order_acquire) < want) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== a parked reserved worker is woken by a lane push ===\n");

    TaskScheduler::Init(8);
    auto& sched = TaskScheduler::Instance();

    // SetHotWorkers, NOT SetIoHotLane. The latter implies SetReservedNeverParks, which pins K awake
    // -- and a worker that never sleeps cannot demonstrate anything about being woken. Reserving
    // without pinning is the one configuration where the notify is load-bearing.
    TaskScheduler::SetHotWorkers(kK);
    TaskScheduler::SetReservedNeverParks(false);
    // Stealing off: a reserved worker that wanders onto the floor is BUSY rather than parked, which
    // would make an arm pass without the band ever having slept.
    TaskScheduler::SetReservedStealing(false);
    TaskScheduler::SetLaneIntake(true);

    const size_t k = TaskScheduler::GetHotWorkers();
    std::printf("  pool=%zu, K=%zu, never-park=%d, intake=%d\n",
                sched.GetWorkerCount(), k,
                (int)TaskScheduler::ReservedNeverParks(), (int)TaskScheduler::LaneIntakeEnabled());

    Check(k == kK, "K reserved");
    Check(!TaskScheduler::ReservedNeverParks(),
          "and the band is PARKABLE (else nothing here can be woken, and every arm is vacuous)");
    if (k == 0) { std::printf("  K clamped to 0 -- vacuous\n"); return 1; }

    // ---- ARM 1: ONE PUSH ONTO A QUIESCED POOL --------------------------------------------------
    //
    // The simple case, and the one a lost notify fails outright: let everything go to sleep, then
    // push exactly one lane task. Nothing else is running that might stumble over it.
    {
        g_ran.store(0); g_onReserved.store(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(150));   // let the band park

        Task* t = sched.CreateTask(&LaneBody, nullptr, Lane::LowLatency, TaskType::Native);
        Check(t != nullptr, "created a LowLatency task");
        sched.Push(t);

        Check(AwaitRun(1), "a single lane push onto a QUIESCED pool ran (else the wake was lost)");
        Check(g_onReserved.load() == 1, "and it ran on a RESERVED worker, through the intake");
    }

    // ---- ARM 2: REPEATED PARK/PUSH, TO LAND IN THE WINDOW --------------------------------------
    //
    // Arm 1 pushes when the band is deeply asleep, which is the easy case -- the notify has all the
    // time in the world to arrive. The dangerous window is narrow: the worker has read the intake as
    // idle and is COMMITTING to sleep when the enqueue lands. Hitting it needs many attempts with
    // just enough settle time that the band is entering or has just entered a park.
    //
    // A LOST WAKE HERE IS PERMANENT, not slow: nothing else will look at that intake. So one failure
    // in four hundred is a real defect and not flakiness, which is why the assertion is on the exact
    // count rather than a proportion.
    {
        g_ran.store(0); g_onReserved.store(0);
        int pushed = 0;
        for (int i = 0; i < kRounds; ++i) {
            // Varied, deliberately: a fixed sleep would sample one phase of the park sequence over
            // and over. Cycling 0..3 ms walks the window instead of hammering one point in it.
            std::this_thread::sleep_for(std::chrono::milliseconds(i % 4));
            Task* t = sched.CreateTask(&LaneBody, nullptr, Lane::LowLatency, TaskType::Native);
            if (!t) break;
            sched.Push(t);
            ++pushed;
        }
        const bool all = AwaitRun(pushed, 5000);
        std::printf("  %d/%d lane pushes ran, %d on reserved workers\n",
                    g_ran.load(), pushed, g_onReserved.load());
        Check(all, "EVERY push ran -- one lost wake here would strand that task forever");
        Check(g_onReserved.load() == pushed, "and every one landed on the reserved band");
    }

    // ---- ARM 3: THE CONTROL. Never-park must also work. ----------------------------------------
    //
    // Not a formality. If arm 2 passed because the band was never actually parking -- a settle time
    // too short, a floor that grew over the reserved indices -- then this arm is indistinguishable
    // from it, and the pair tells you the test cannot see the difference. It is here so a future
    // failure is attributable: arm 2 red with arm 3 green is a WAKE problem, both red is the intake
    // itself.
    {
        TaskScheduler::SetReservedNeverParks(true);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        g_ran.store(0); g_onReserved.store(0);

        int pushed = 0;
        for (int i = 0; i < 100; ++i) {
            Task* t = sched.CreateTask(&LaneBody, nullptr, Lane::LowLatency, TaskType::Native);
            if (!t) break;
            sched.Push(t);
            ++pushed;
        }
        Check(AwaitRun(pushed), "CONTROL: the same pushes run with the band pinned awake");
        Check(g_onReserved.load() == pushed, "CONTROL: and still on the reserved band");
        TaskScheduler::SetReservedNeverParks(false);
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_fail ? "FAILED" : "PASSED", g_fail, g_fail == 1 ? "" : "s");
    return g_fail ? 1 : 0;
}
