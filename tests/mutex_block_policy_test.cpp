// WHO MAY BLOCK ON A CONTENDED SchedulerMutex, AND WHAT BLOCKING COSTS.
//
// Two rules, tested separately because they fail in opposite directions.
//
// 1. A BARE THREAD BLOCKS. It used to SpinThenHelp: spin on Try_Lock and, between spins, try to run
//    stolen Native work. On a fiber-backed pool that help can never succeed -- GetTask vets steal
//    candidates with `fiberlessRunnable`, which rejects TaskType::Fiber, and that is now every task
//    in the pool -- so it burned a core walking candidates it was not permitted to claim.
//
// 2. A TASK WITH NO FIBER MAY NOT BLOCK AT ALL. Native runs directly on the worker, so waiting
//    there pins the worker while its own INBOX may hold tasks nobody else may run. That is a
//    self-deadlock that reports as a lost wake, and it is refused rather than diagnosed later.
//
// THE FIRST RULE IS MEASURED, NOT ASSUMED. "Main acquired the lock eventually" passes just as well
// against the spin it replaced, so it proves nothing. The discriminator is CPU TIME: blocking burns
// none, spinning burns wall-clock. Hence arm A -- a deliberate spin whose CPU must come back HIGH.
// If the instrument is broken or the OS reports garbage, arm A goes RED and arm B's low reading is
// correctly disbelieved instead of being read as success.

#include "../include/TaskScheduler.h"
#include "fiber_body.h"
#include <cstdio>
#include <atomic>
#include <chrono>
#include <thread>

#ifdef _WIN32
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
#else
#  include <time.h>
#endif

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-70s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

// CPU time consumed by THIS THREAD, in milliseconds. Thread-scoped, not process-scoped: the workers
// are busy throughout and a process-wide clock would drown the signal entirely.
static double ThreadCpuMs() {
#ifdef _WIN32
    FILETIME c, e, k, u;
    if (!GetThreadTimes(GetCurrentThread(), &c, &e, &k, &u)) return -1.0;
    auto to100ns = [](const FILETIME& f) {
        return ((unsigned long long)f.dwHighDateTime << 32) | f.dwLowDateTime;
    };
    return double(to100ns(k) + to100ns(u)) / 10000.0;
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) return -1.0;
    return double(ts.tv_sec) * 1000.0 + double(ts.tv_nsec) / 1e6;
#endif
}

static std::atomic<bool> g_hookFired{ false };
static void BlockViolationHook() { g_hookFired.store(true, std::memory_order_release); }

static const int kHoldMs = 250;

// ---- FIBER BODIES: NAMED FUNCTIONS, EXPLICIT CONTEXTS ---------------------------------------
//
// One spelling for every task body: a named void(void*) plus a struct the caller declares. Not a
// lambda and not a captureless one, so there is never a per-site judgement about which form is
// safe. A fiber's stack is a PLACE -- register state mapped to memory -- while a closure is a
// VALUE the worker loop frees when the body returns; a fiber given a closure that then parks
// either resumes into a dead frame or never dies and never gives its row back.
struct SleepHoldCtx {
    SchedulerMutex*    m;
    std::atomic<bool>* held;
    std::atomic<bool>* done;     // optional
    int                holdMs;
};
static void SleepHoldBody(void* p) {
    auto& c = *static_cast<SleepHoldCtx*>(p);
    c.m->Lock();
    c.held->store(true, std::memory_order_release);
    std::this_thread::sleep_for(std::chrono::milliseconds(c.holdMs));
    c.m->Unlock();
    if (c.done) c.done->store(true, std::memory_order_release);
}

struct TakeLockCtx { SchedulerMutex* m; std::atomic<bool>* got; };
static void TakeLockBody(void* p) {
    auto& c = *static_cast<TakeLockCtx*>(p);
    c.m->Lock();
    c.got->store(true, std::memory_order_release);
    c.m->Unlock();
}

// Holds the mutex ACROSS an event wait, so the lock stays held while this fiber is suspended.
struct HoldOnEventCtx {
    TaskScheduler*     s;
    SchedulerMutex*    m;
    Event*             hold;
    std::atomic<bool>* held;
};
static void HoldOnEventBody(void* p) {
    auto& c = *static_cast<HoldOnEventCtx*>(p);
    c.m->Lock();
    c.held->store(true, std::memory_order_release);
    c.s->WaitOnEvent(*c.hold);
    c.m->Unlock();
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== SchedulerMutex: who may block, and what blocking costs ===\n");

    TaskScheduler::Init(4);
    auto& sched = TaskScheduler::Instance();

    Check(ThreadCpuMs() >= 0.0, "the per-thread CPU clock is available on this platform");

    // ---- ARM A: THE INSTRUMENT CONTROL. A deliberate spin MUST read as high CPU. ---------------
    double spinRatio = -1.0;
    {
        const double cpu0 = ThreadCpuMs();
        const auto   w0   = std::chrono::steady_clock::now();
        volatile unsigned sink = 0;
        while (std::chrono::steady_clock::now() - w0 < std::chrono::milliseconds(kHoldMs))
            sink = sink + 1u;
        const double wall = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - w0).count();
        spinRatio = (ThreadCpuMs() - cpu0) / wall;
        std::printf("    arm A (spin):  cpu/wall = %.2f\n", spinRatio);
        Check(spinRatio > 0.60,
              "CONTROL: a busy spin reads as HIGH cpu/wall (else this test cannot tell them apart)");
    }

    // ---- ARM B: a contended Lock() on main must read as near-zero CPU. -------------------------
    {
        SchedulerMutex m;
        std::atomic<bool> held{ false };
        std::atomic<bool> done{ false };

        // The holder is a FIBER, which is the only thing allowed to hold a lock across a sleep.
        // A NAMED body and a context on THIS stack. Not a lambda: a fiber's stack is a place, and a
        // closure is a value the worker loop frees when the body returns -- a fiber handed one that
        // then parks either resumes into a dead frame or never dies and never returns its row.
        SleepHoldCtx ctx{ &m, &held, &done, kHoldMs };
        auto* holder = JLibTest::MakeCtxTask(sched, &SleepHoldBody, &ctx);
        sched.Push(holder);

        while (!held.load(std::memory_order_acquire)) std::this_thread::yield();

        const double cpu0 = ThreadCpuMs();
        const auto   w0   = std::chrono::steady_clock::now();
        m.Lock();                                   // <-- contended: main must SLEEP here
        const double wall = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - w0).count();
        const double ratio = (ThreadCpuMs() - cpu0) / wall;
        m.Unlock();

        std::printf("    arm B (block): cpu/wall = %.2f over %.0f ms of waiting\n", ratio, wall);
        Check(wall > kHoldMs * 0.5,
              "main really did wait on a held lock (else it never contended and arm B is vacuous)");
        Check(ratio < 0.20,
              "main BLOCKED rather than spun -- near-zero cpu while waiting");
        Check(spinRatio < 0.0 || ratio < spinRatio * 0.5,
              "and it used dramatically less cpu than the measured spin in arm A");

        while (!done.load(std::memory_order_acquire)) std::this_thread::yield();
    }

    // ---- THE REFUSAL: a task with no fiber may not block. --------------------------------------
    //
    // Ordered controls-first. If the hook fired for a fiber or for an uncontended lock, the final
    // assertion would pass for the wrong reason.
    SchedulerMutex::s_blockViolationHook.store(&BlockViolationHook, std::memory_order_release);

    {   // CONTROL: an UNCONTENDED Native lock is fine. Native may not WAIT; it may still LOCK.
        SchedulerMutex m;
        std::atomic<bool> ran{ false };
        g_hookFired.store(false, std::memory_order_release);
        auto* t = sched.CreateTask([&] {
            m.Lock();
            m.Unlock();
            ran.store(true, std::memory_order_release);
        }, JLib::Lane::Normal);
        sched.Push(t);
        while (!ran.load(std::memory_order_acquire)) std::this_thread::yield();
        Check(!g_hookFired.load(std::memory_order_acquire),
              "CONTROL: an UNCONTENDED Native Lock() is permitted (the rule is about waiting)");
    }

    {   // CONTROL: a FIBER may block on a contended lock -- it suspends and frees its worker.
        SchedulerMutex m;
        std::atomic<bool> held{ false }, got{ false };
        g_hookFired.store(false, std::memory_order_release);

        SleepHoldCtx hctx{ &m, &held, nullptr, 80 };
        auto* holder = JLibTest::MakeCtxTask(sched, &SleepHoldBody, &hctx);
        sched.Push(holder);
        while (!held.load(std::memory_order_acquire)) std::this_thread::yield();

        TakeLockCtx wctx{ &m, &got };
        auto* waiter = JLibTest::MakeCtxTask(sched, &TakeLockBody, &wctx);
        sched.Push(waiter);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!got.load(std::memory_order_acquire)
               && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        Check(got.load(std::memory_order_acquire),
              "CONTROL: a FIBER blocks on a contended lock and gets it");
        Check(!g_hookFired.load(std::memory_order_acquire),
              "CONTROL: and the refusal did NOT fire for it");
    }

    {   // THE CASE: a NATIVE task on a worker hits contention. It must be refused, not spin.
        SchedulerMutex m;
        std::atomic<bool> held{ false }, attempted{ false }, gotLock{ false };
        g_hookFired.store(false, std::memory_order_release);

        // THE HOLDER SUSPENDS INSTEAD OF SLEEPING, and the first version of this test got it wrong
        // in a way worth keeping written down. With `sleep_for` the holder pinned its worker for the
        // whole hold, placement steered the offender to that worker because it was AWAKE, and the
        // offender sat in its unstealable INBOX until the hold ended -- so it measured an
        // UNCONTENDED lock at 158 ms and the refusal correctly never fired. That is the busy+inbox
        // strand this whole rule exists to prevent, reproduced by accident in the test for it.
        //
        // Suspending on an Event frees the worker while still holding the lock, which is the state
        // the offender actually has to meet.
        Event& hold = sched.GetEvent("mutex_block_policy_hold");
        HoldOnEventCtx ectx{ &sched, &m, &hold, &held };

        // COUNTED, BECAUSE THIS SCOPE OWNS EVERYTHING THE HOLDER TOUCHES. ectx, m, held and
        // gotLock are all locals here, and the holder reads ectx -- and therefore m -- AFTER it
        // resumes from WaitOnEvent. SignalAll only wakes it; it does not wait for it. Pushing this
        // uncounted let the scope close while the holder was still on its way back, so it read a
        // dead frame and called Unlock() on it.
        //
        // MakeCtxTask's own header states the rule: "the context's scope must cover the wait.
        // Declare-then-join in the same function satisfies that structurally." This site declared
        // and never joined.
        //
        // THE SYMPTOM WAS A ~2-IN-20 SEGFAULT AFTER "PASSED" ALREADY PRINTED, on Windows and Linux
        // both, with the crash landing in whatever reused main's stack -- usually TaskScheduler's
        // teardown Join(). Green test, dead process, exit 139. Found with ASan in WSL: a
        // stack-buffer-overflow READ at HoldOnEventBody, reported against a LIVE Join() frame,
        // which is what "this address used to be ours" looks like.
        WaitGroup holderWg;
        holderWg.n.store(1, std::memory_order_relaxed);
        auto* holder = JLibTest::MakeCtxTask(sched, &HoldOnEventBody, &ectx);
        holder->waitGroup = &holderWg;
        sched.Push(holder);
        while (!held.load(std::memory_order_acquire)) std::this_thread::yield();

        auto* offender = sched.CreateTask([&] {
            m.Lock();
            // The hook returns WITHOUT the lock -- there is no lock to return with. Unlocking here
            // would corrupt a mutex another task legitimately holds.
            if (!g_hookFired.load(std::memory_order_acquire)) {
                gotLock.store(true, std::memory_order_release);
                m.Unlock();
            }
            attempted.store(true, std::memory_order_release);
        }, JLib::Lane::Normal);
        sched.Push(offender);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!attempted.load(std::memory_order_acquire)
               && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        Check(attempted.load(std::memory_order_acquire),
              "the Native task reached the contended Lock()");
        Check(!gotLock.load(std::memory_order_acquire),
              "and it did NOT acquire (else the lock was free and this case is vacuous)");
        Check(g_hookFired.load(std::memory_order_acquire),
              "a NATIVE task blocking on a contended mutex is REFUSED (it would strand its worker)");

        hold.SignalAll();

        // THE JOIN. SignalAll wakes the holder; this is what waits for it to finish reading ectx
        // and to release m. Without it the closing brace below races the holder's Unlock().
        sched.WaitFor(holderWg);
    }

    SchedulerMutex::s_blockViolationHook.store(nullptr, std::memory_order_release);

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
