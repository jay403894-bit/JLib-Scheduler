// THE PUBLIC JOB IS A FIBER -- and the internal ones are still not.
//
// The whole suite went green the moment CreateTask's default flipped from Native to Fiber, which
// proves nothing: every one of those tests predates the change and most name their task type
// explicitly. A change nothing asserts is a change nothing protects.
//
// THE CLAIM IS BEHAVIOURAL, NOT A TYPE TAG. Before this change a plain CreateTask produced a task
// that COULD NOT WAIT ON ANYTHING -- WaitOnEvent from it throws "Native tasks must never suspend".
// So the test that matters is: does a default-created task actually suspend and resume? A test that
// only read back `t->type` would pass with the execution path still broken.
//
// AND THE OTHER HALF MATTERS AS MUCH. ParallelFor grains must have stayed Native. If they quietly
// became fibers, grain concurrency is now bounded by the fiber pool (coreCount *
// StandardFibersPerWorker, 64 KB of stack each) and nested ParallelFor is back on the path that
// deadlocked before -- AcquireFiber fails, Requeue, spin. That failure is invisible until a range
// is big enough, so it is asserted here rather than waited for.

#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include <cstdio>
#include <atomic>
#include <thread>
#include <chrono>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-70s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

static std::atomic<int> g_onFiber{ -1 };
static std::atomic<int> g_resumed{ 0 };
static std::atomic<int> g_grainsNotOnFiber{ 0 };
static std::atomic<int> g_grainsOnFiber{ 0 };
static std::atomic<bool> g_released{ false };

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== everything is a fiber: the public default, and what stayed native ===\n");

    TaskScheduler::Init(8);
    auto& sched = TaskScheduler::Instance();

    // ---- 1. A PLAIN CreateTask IS A FIBER -----------------------------------------------------
    {
        Task* t = sched.CreateTask([] {});
        Check(t != nullptr, "a plain CreateTask returns a task");
        Check(t && t->type == TaskType::Fiber, "a plain CreateTask is TaskType::Fiber");
        // Not pushed -- hand it back rather than leaking a slab slot.
        if (t) { sched.CleanupTaskMetadata(t); t->~Task(); sched.GetAllocator()->Free(t); }

        Task* n = sched.CreateInternalTask(+[](void*) {}, nullptr);
        Check(n && n->type == TaskType::Native, "CreateInternalTask is still TaskType::Native");
        if (n) { sched.CleanupTaskMetadata(n); n->~Task(); sched.GetAllocator()->Free(n); }
    }

    // ---- 2. AND IT CAN ACTUALLY SUSPEND -------------------------------------------------------
    //
    // THE POINT OF THE WHOLE CHANGE. Under the old default this body threw: a Native task has no
    // context to switch away to, so WaitOnEvent is not merely slow for it, it is illegal.
    {
        Event& gate = sched.GetEvent("eiaf_gate");
        WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);

        Task* t = sched.CreateTask([&gate] {
            g_onFiber.store(TaskScheduler::Instance().IsOnFiber() ? 1 : 0,
                            std::memory_order_relaxed);
            // ARMED: if the release already happened, self-signal rather than parking forever.
            TaskScheduler::Instance().WaitOnEventArmed(gate, [&gate] {
                if (g_released.load(std::memory_order_acquire)) gate.SignalAll();
            });
            g_resumed.fetch_add(1, std::memory_order_release);
        });
        Check(t != nullptr, "created a default task that intends to wait");
        if (t) { t->waitGroup = &wg; sched.Push(t); }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        Check(g_onFiber.load() == 1, "a default-created task RUNS ON A FIBER (IsOnFiber)");
        Check(g_resumed.load() == 0, "and it really parked -- it had not completed before the signal");

        g_released.store(true, std::memory_order_release);
        gate.SignalAll();
        sched.WaitFor(wg);
        Check(g_resumed.load() == 1,
              "a plain CreateTask SUSPENDED AND RESUMED (illegal under the old Native default)");
    }

    // ---- 3. PARALLELFOR GRAINS ARE STILL NATIVE -----------------------------------------------
    //
    // Asserted on the BODY, not on a task type read from outside: the body is what runs, and
    // IsOnFiber() there answers the question that actually costs something -- did this grain
    // consume a pooled 64 KB stack?
    {
        // The range form hands the body a [lo, hi) slice, not one index.
        sched.ParallelFor(0, 4096, [](int lo, int hi) {
            const bool onFiber = TaskScheduler::Instance().IsOnFiber();
            for (int i = lo; i < hi; ++i) {
                if (onFiber) g_grainsOnFiber.fetch_add(1, std::memory_order_relaxed);
                else         g_grainsNotOnFiber.fetch_add(1, std::memory_order_relaxed);
            }
        });
        const int on = g_grainsOnFiber.load(), off = g_grainsNotOnFiber.load();
        std::printf("    grain iterations: on-fiber=%d not-on-fiber=%d\n", on, off);
        Check(on + off == 4096, "every iteration ran (so the counts mean something)");
        // THE MAIN THREAD MAY RUN SOME ITERATIONS ITSELF -- ParallelFor helps rather than blocking,
        // and main is not on a fiber either, so both of those land in `off`. The claim is only that
        // NOTHING ran on a fiber.
        Check(on == 0, "NO ParallelFor grain took a fiber (grain count stays unbounded by the pool)");
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
