// WHICH TASKS GET A FIBER, AND WHICH MUST NOT.
//
// THIS FILE ONCE ASSERTED THE OPPOSITE OF WHAT IT ASSERTS NOW, and the reversal is worth reading
// before the code. It was written when CreateTask's default flipped from Native to Fiber, to prove
// that a plain lambda task could suspend. It can no longer be created at all: the lambda overload
// has no TaskType parameter, and a closure is always Native.
//
// THE REASON IS OWNERSHIP, NOT PERFORMANCE. A fiber ROW is a leased stack plus the FLS slots,
// creditor list and retire bag hanging off it, and it has exactly one teardown -- the recycle after
// FiberStatus::DEAD. Every AcquireFiber must reach that exactly once; a row that does not is a
// permanent --budget. A lambda body copied onto the task slab has TWO owners -- the worker loop,
// which frees the frame when the body returns, and whoever resumes the fiber -- with no destructor
// they share. That is invisible while the body merely runs and a contradiction the moment it
// suspends, and because SlabPool is append-only the corruption does not surface where it is caused.
// So the rule is enforced by the type system rather than by a test, and section 1 checks that the
// enforcement is real.
//
// THE CLAIM IS STILL BEHAVIOURAL, NOT A TYPE TAG. Section 2 takes the supported fiber form -- raw
// void(*)(void*) with the context on the caller's stack -- and makes it actually park and resume,
// because reading back `t->type` would pass with the execution path broken.
//
// AND THE THIRD PART MATTERS AS MUCH. ParallelFor grains must be Native. If they quietly became
// fibers, grain concurrency is bounded by the fiber pool (coreCount * StandardFibersPerWorker,
// 64 KB of stack each) and nested ParallelFor is back on the path that deadlocked before --
// AcquireFiber fails, Requeue, spin. That failure is invisible until a range is big enough, so it
// is asserted here rather than waited for.

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

// ---- THE FIBER BODIES, AS NAMED FUNCTIONS ---------------------------------------------------
//
// One spelling for every task body in the suite: a named `void(void*)` plus a context struct the
// caller declares. Not a lambda, and not a captureless lambda either -- a single form means there
// is never a judgement call at a call site about which kind is safe here, and a named body is
// greppable, reviewable, and cannot quietly acquire state later.
static void EmptyBody(void*) {}

struct GateCtx { Event* gate; };
static void ArmedWaitBody(void* p) {
    Event& g = *static_cast<GateCtx*>(p)->gate;
    g_onFiber.store(TaskScheduler::Instance().IsOnFiber() ? 1 : 0, std::memory_order_relaxed);
    // ARMED: if the release already happened, self-signal rather than parking forever.
    TaskScheduler::Instance().WaitOnEventArmed(g, [&g] {
        if (g_released.load(std::memory_order_acquire)) g.SignalAll();
    });
    g_resumed.fetch_add(1, std::memory_order_release);
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== everything is a fiber: the public default, and what stayed native ===\n");

    TaskScheduler::Init(8);
    auto& sched = TaskScheduler::Instance();

    // ---- 1. A LAMBDA TASK IS NATIVE. A FIBER TASK IS void(*)(void*). -------------------------
    //
    // THIS SECTION USED TO ASSERT THE OPPOSITE, and the reversal is the design rather than a
    // regression. A fiber ROW -- the leased stack plus its FLS slots, creditor list and retire bag
    // -- has exactly one teardown, the recycle after FiberStatus::DEAD, so every AcquireFiber must
    // reach DEAD exactly once. A closure copied onto the task slab cannot hold up that end: the
    // task frame belongs to the worker loop, which frees it as soon as the body returns, while the
    // fiber belongs to whoever resumes it. Two owners, no shared destructor. Harmless while the
    // body merely runs; a contradiction the moment it suspends.
    //
    // And it does not fail where it is wrong. SlabPool is append-only and frees no extent before
    // the pool dies, so a released slot stays mapped holding its old bytes and the round trip reads
    // back intact. The bill arrives as a live count that never returns to baseline, a slab that
    // cannot be cleared, or a completion writing into a reissued stack. So the rule is enforced by
    // the TYPE SYSTEM -- CreateTask's lambda overload has no TaskType parameter at all -- and this
    // section checks that the enforcement is real rather than conventional.
    {
        Task* t = sched.CreateTask([] {});
        Check(t != nullptr, "a plain CreateTask returns a task");
        Check(t && t->type == TaskType::Native,
              "a LAMBDA CreateTask is TaskType::Native -- a closure never owns a fiber row");
        // Not pushed -- hand it back rather than leaking a slab slot.
        if (t) { sched.CleanupTaskMetadata(t); t->~Task(); sched.GetAllocator()->Free(t); }

        Task* n = sched.CreateInternalTask(&EmptyBody, nullptr);
        Check(n && n->type == TaskType::Native, "CreateInternalTask is still TaskType::Native");
        if (n) { sched.CleanupTaskMetadata(n); n->~Task(); sched.GetAllocator()->Free(n); }

        // AND THE FIBER PATH IS STILL ONE CALL AWAY. The raw overload takes a function pointer and
        // a context the CALLER owns, which is the form that has a single owner and therefore a
        // DEAD transition.
        Task* f = sched.CreateTask(&EmptyBody, nullptr, Lane::Normal, TaskType::Fiber);
        Check(f && f->type == TaskType::Fiber,
              "the raw void(*)(void*) overload still produces a Fiber task");
        if (f) { sched.CleanupTaskMetadata(f); f->~Task(); sched.GetAllocator()->Free(f); }
    }

    // ---- 2. AND THAT FIBER TASK CAN ACTUALLY SUSPEND ------------------------------------------
    //
    // The behavioural half, which is the half that matters: reading back `t->type` would pass with
    // the execution path broken. The body's state lives in a struct on THIS frame -- main's stack,
    // which outlives the WaitFor below -- and the task carries a pointer to it. That is the whole
    // pattern, and it is what tests/fiber_body.h wraps for the rest of the suite.
    {
        Event& gate = sched.GetEvent("eiaf_gate");
        WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);

        GateCtx ctx{ &gate };                   // OWNED HERE, outlives the wait
        Task* t = sched.CreateTask(&ArmedWaitBody, &ctx, Lane::Normal, TaskType::Fiber);
        Check(t != nullptr, "created a fiber task that intends to wait");
        if (t) { t->waitGroup = &wg; sched.Push(t); }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        Check(g_onFiber.load() == 1, "a raw-overload Fiber task RUNS ON A FIBER (IsOnFiber)");
        Check(g_resumed.load() == 0, "and it really parked -- it had not completed before the signal");

        g_released.store(true, std::memory_order_release);
        gate.SignalAll();
        sched.WaitFor(wg);
        Check(g_resumed.load() == 1,
              "a fiber task SUSPENDED AND RESUMED with its context on the caller stack");
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
