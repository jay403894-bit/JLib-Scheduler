// A RESERVED WORKER RUNS FIBER JOBS. THE FLOOR STILL RUNS NATIVE FIBERLESS.
//
// The fiberless fast path runs a task straight on the worker's OS stack with no fiber bound. It is
// legal only because Native is a CONTRACT -- it may not suspend, because there is nothing to switch
// away to. On the floor that is the right trade: work that will never wait should not pay for a
// fiber.
//
// ON K IT IS THE WRONG TRADE. The reserved lane carries I/O completions, and a completion is exactly
// the work that WANTS to wait -- read, then wait for the next read. The reactor's continuations are
// Native (CreateInternalTask), so fiberless they cannot suspend at all, and they cannot block
// either: SchedulerMutex::Lock refuses a fiberless task rather than stranding the worker's
// unstealable inbox. So K binds a fiber and the lane gets the capability it exists to provide.
//
// KEYED ON WHERE A TASK ACTUALLY RAN, NOT WHERE IT WAS AIMED. Lane::LowLatency routes to the
// reserved band but can SPILL to another lane inbox when K is busy, so "I pushed it LowLatency" does
// not mean "it ran on K". Each run records its own (queue index, had a fiber) pair and the
// assertions are computed from that -- otherwise a spill would look like a violation.
//
// THE FLOOR ARM IS THE NEGATIVE CONTROL, and it is why this file cannot pass by accident. If the
// change had bound a fiber to EVERY Native task instead of only K's, arm 1 would still be green --
// it is arm 2 going red that distinguishes "K got fibers" from "everyone got fibers", which is a
// real regression (it would put a ContextSwitch on every floor task in the pool).

#include "TaskScheduler.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-72s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static constexpr size_t kK     = 1;     // the reactor configuration: EnableIoReactor implies K=1
static constexpr int    kTasks = 400;

static std::atomic<int> g_ran{ 0 };
static std::atomic<int> g_resvWithFiber{ 0 }, g_resvNoFiber{ 0 };
static std::atomic<int> g_floorWithFiber{ 0 }, g_floorNoFiber{ 0 };

static void Payload(void*) {
    JLib::Thread* t = JLib::Thread::GetCurrent();
    const int  q         = t ? t->qIndex : -1;
    const bool haveFiber = (t && t->currentFiber != nullptr);
    const size_t k = JLib::TaskScheduler::GetHotWorkers();

    if (q >= 0) {
        if ((size_t)q < k) (haveFiber ? g_resvWithFiber  : g_resvNoFiber ).fetch_add(1, std::memory_order_relaxed);
        else               (haveFiber ? g_floorWithFiber : g_floorNoFiber).fetch_add(1, std::memory_order_relaxed);
    }
    g_ran.fetch_add(1, std::memory_order_relaxed);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== K runs fiber jobs; the floor keeps the fiberless fast path ===\n");

    JLib::TaskScheduler::Init(8);
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();
    JLib::TaskScheduler::SetIoHotLane(kK);

    const size_t n = sched.GetWorkerCount();
    const size_t k = JLib::TaskScheduler::GetHotWorkers();
    std::printf("  pool=%zu, K=%zu\n", n, k);
    if (k == 0 || n <= k + 1) {
        std::printf("  K clamped to 0 or pool too small -- this file would be vacuous\n");
        return 1;
    }

    // Steady state first: placement reads the awake bitmap, and pushing into a cold pool exercises
    // the tail fallback instead of the path under test.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    int created = 0;
    for (int i = 0; i < kTasks; ++i) {
        // Alternate LowLatency (routed to the reserved band) and Normal (the floor). Both NATIVE on
        // purpose -- the type is held constant, so the only variable is WHICH BAND ran it.
        JLib::Task* t = sched.CreateTask(Payload, nullptr,
                                         ((i % 2) == 0) ? JLib::Lane::LowLatency : JLib::Lane::Normal,
                                         JLib::TaskType::Native, JLib::CorePref::Default);
        if (!t) break;
        sched.Push(t);
        ++created;
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (g_ran.load(std::memory_order_relaxed) < created
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

    const int rwf = g_resvWithFiber.load(),  rnf = g_resvNoFiber.load();
    const int fwf = g_floorWithFiber.load(), fnf = g_floorNoFiber.load();
    std::printf("  reserved: %d with a fiber, %d without\n", rwf, rnf);
    std::printf("  floor   : %d with a fiber, %d without\n", fwf, fnf);

    Check(g_ran.load() == created, "every task ran");

    // VACUITY GUARDS FIRST. Both bands must have actually run something, or the assertions below are
    // statements about an empty set.
    Check(rwf + rnf > 0, "the reserved band ran at least one task (else arm 1 is vacuous)");
    Check(fwf + fnf > 0, "the floor ran at least one task (else the control is vacuous)");

    Check(rnf == 0, "EVERY task on a reserved worker had a fiber bound");
    Check(fwf == 0, "CONTROL: every Native task on the floor stayed FIBERLESS");

    // ---- THE CAPABILITY, not just the binding. ---------------------------------------------
    //
    // A bound fiber is the mechanism; suspending is the point. A fiberless task cannot reach
    // WaitOnEvent at all -- its guard checks assignedFiber and fails loudly -- so this completing is
    // the whole claim, and before this change it was unreachable for a Native task.
    {
        std::atomic<bool> resumed{ false }, onReserved{ false }, started{ false };
        JLib::Event& ev = sched.GetEvent("reserved_fiber_job_gate");

        auto* t = sched.CreateInternalTask([&] {
            JLib::Thread* th = JLib::Thread::GetCurrent();
            onReserved.store(th && (size_t)th->qIndex < k, std::memory_order_release);
            started.store(true, std::memory_order_release);
            JLib::TaskScheduler::Instance().WaitOnEvent(ev);   // needs a fiber
            resumed.store(true, std::memory_order_release);
        }, JLib::Lane::LowLatency);
        sched.Push(t);

        const auto d2 = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!started.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < d2)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        Check(started.load(std::memory_order_acquire), "the suspending Native task started");
        Check(onReserved.load(std::memory_order_acquire),
              "and it ran on a RESERVED worker (else this arm proves nothing about K)");

        ev.SignalAll();
        const auto d3 = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!resumed.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < d3)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        Check(resumed.load(std::memory_order_acquire),
              "a NATIVE task on K SUSPENDED on an event and was resumed");
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_fail ? "FAILED" : "PASSED", g_fail, g_fail == 1 ? "" : "s");
    return g_fail ? 1 : 0;
}
