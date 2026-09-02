// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// TEARDOWN IS A DRAIN, NOT AN ABANDONMENT -- FOR THE WAITERS IT CAN REACH.
//
// A frame parked on a primitive nobody will signal again used to be simply left there: its stack
// never unwound, so NOTHING it held was released -- RAII objects, its WaitGroup slot, a hazard
// record. Teardown now walks the registry of live primitives, releases every waiter it is allowed
// to release, and only then joins the workers, so each of those frames resumes, observes the
// cancel, and UNWINDS.
//
// THE ASSERTION IS AN RAII DESTRUCTOR RUNNING, deliberately. Checking that "the wait returned"
// would not distinguish a drain from a lucky signal; a destructor on the parked frame's stack can
// only run if that stack was actually unwound.
//
// ---- WHICH WAITERS TEARDOWN CAN REACH, AND WHY IT IS NOT ALL OF THEM --------------------------
//
// A WAITER IS WOKEN AT TEARDOWN ONLY IF IT CAN BE TOLD IT DID NOT ACQUIRE. SchedulerMutex and
// SchedulerSemaphore both gate ejection on the waiter carrying a `result` pointer -- the mutex
// folds it into `matches`, the semaphore spells it `cancellable`, and they mean the same thing.
// A plain Lock() or Wait() has nowhere to report Cancelled, so waking one would return it to a
// caller that believes it now holds the lock or the permit. It would then run its whole
// post-acquire body -- touching the resource, unlocking, signalling -- on the way out of a pool
// that is being destroyed. Leaving that frame parked is the lesser harm, and it is a CHOICE.
//
// SchedulerConditionVariable ejects everyone and is not an exception to the rule. A condition
// variable is permitted to wake spuriously by contract and its Wait() re-acquires the mutex before
// returning, so a plain CV waiter resuming has been handed nothing it does not hold.
//
// ONE ARM PER (PRIMITIVE, WAIT-FLAVOUR), because the policy is invisible from any single one of
// them. A file that only ever parked on LockCancellable would pass forever while saying nothing
// about the frames teardown deliberately abandons.
//
// THE PLAIN ARMS ASSERT THAT NOTHING UNWOUND, which is the point: they are not softened reports of
// a known gap, they PIN the policy. If someone later makes CancelWaiters eject plain waiters, these
// fail and demand that the reasoning above be revisited rather than silently invalidated.

#include "TaskScheduler.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int  g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-70s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

namespace {
std::atomic<int> g_unwoundMutexCancellable{ 0 };
std::atomic<int> g_unwoundMutexPlain{ 0 };
std::atomic<int> g_unwoundSemCancellable{ 0 };
std::atomic<int> g_unwoundSemPlain{ 0 };
std::atomic<int> g_entered{ 0 };
std::atomic<bool> g_tornDown{ false };

// Lives on the parked fiber's stack. Its destructor is the whole assertion.
struct UnwindWitness {
    std::atomic<int>* into;
    explicit UnwindWitness(std::atomic<int>& c) : into(&c) {}
    ~UnwindWitness() { into->fetch_add(1, std::memory_order_release); }
};

// A drain bug has two shapes and only one of them is a failed assertion. The other is teardown
// HANGING on a frame it cannot wake -- and a hung test is a CI timeout with no evidence in it.
// Printing the per-arm counts before aborting makes the hang name the arm responsible.
void StartWatchdog(int seconds) {
    std::thread([seconds] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
        while (std::chrono::steady_clock::now() < deadline) {
            if (g_tornDown.load(std::memory_order_acquire)) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        std::fprintf(stderr, "\n[watchdog] teardown did not return within %ds -- entered=%d\n"
                             "  unwound: mutex/cancellable=%d mutex/plain=%d sem/cancellable=%d sem/plain=%d\n",
                     seconds, g_entered.load(std::memory_order_relaxed),
                     g_unwoundMutexCancellable.load(std::memory_order_relaxed),
                     g_unwoundMutexPlain.load(std::memory_order_relaxed),
                     g_unwoundSemCancellable.load(std::memory_order_relaxed),
                     g_unwoundSemPlain.load(std::memory_order_relaxed));
        std::fflush(stderr);
        std::abort();
    }).detach();
}

// Reached by the no-capture lambdas below, which have to convert to plain function pointers.
SchedulerMutex*     s_gate        = nullptr;
SchedulerMutex*     s_gatePlain   = nullptr;
SchedulerSemaphore* s_cancellable = nullptr;
SchedulerSemaphore* s_plain       = nullptr;
} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();
    std::printf("teardown drain -- workers=%zu\n\n", sched.GetWorkerCount());

    // CONSTRUCTED AFTER Init, so they register. A file-scope primitive would not -- see
    // WaitPrimitive. Leaked on purpose: teardown resumes frames parked on them, so they have to
    // outlive it, and the plain arms stay parked on them forever by design.
    s_gate        = new SchedulerMutex();
    s_gatePlain   = new SchedulerMutex();
    s_cancellable = new SchedulerSemaphore(0);   // zero permits, never signalled
    s_plain       = new SchedulerSemaphore(0);   // zero permits, never signalled
    s_gate->Lock();                              // never unlocked, on purpose
    s_gatePlain->Lock();                         // never unlocked, on purpose

    auto spawn = [&sched](void (*body)()) {
        Task* t = sched.CreateTask([body] {
            g_entered.fetch_add(1, std::memory_order_release);
            body();
        }, JLib::Lane::Normal, TaskType::Fiber);
        if (!t) { Check(false, "CreateTask returned a task"); return; }
        sched.Push(t);
    };

    // ---- REACHABLE: these carry a result pointer, so teardown may eject them ------------------
    for (int i = 0; i < 4; ++i)
        spawn([] {
            UnwindWitness witness(g_unwoundMutexCancellable);
            (void)s_gate->LockCancellable();
            // Not unlocking: if this ever acquires, the test is measuring the wrong thing.
        });
    for (int i = 0; i < 2; ++i)
        spawn([] {
            UnwindWitness witness(g_unwoundSemCancellable);
            (void)s_cancellable->WaitCancellable();
        });

    // ---- UNREACHABLE BY POLICY: no result pointer, so teardown must leave them parked ---------
    for (int i = 0; i < 2; ++i)
        spawn([] {
            UnwindWitness witness(g_unwoundMutexPlain);
            s_gatePlain->Lock();
        });
    for (int i = 0; i < 2; ++i)
        spawn([] {
            UnwindWitness witness(g_unwoundSemPlain);
            s_plain->Wait();
        });

    constexpr int kFibers = 10;
    for (int i = 0; i < 2000 && g_entered.load(std::memory_order_acquire) < kFibers; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    Check(g_entered.load(std::memory_order_acquire) == kFibers, "all ten fibers reached their wait");
    // `entered` is bumped BEFORE the wait call, so it means "running", not "parked". Draining a
    // primitive nobody has reached yet would test nothing.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    Check(g_unwoundMutexCancellable.load(std::memory_order_acquire) == 0
       && g_unwoundMutexPlain.load(std::memory_order_acquire) == 0
       && g_unwoundSemCancellable.load(std::memory_order_acquire) == 0
       && g_unwoundSemPlain.load(std::memory_order_acquire) == 0,
          "nothing has unwound yet -- every fiber is parked on something nobody will release");

    // TEARDOWN. No unlock, no signal, no cancel from the app: the drain alone must get them out.
    StartWatchdog(30);
    JLib::detail::TeardownForTesting(sched);
    g_tornDown.store(true, std::memory_order_release);

    Check(g_unwoundMutexCancellable.load(std::memory_order_acquire) == 4,
          "mutex/LockCancellable: every parked frame unwound (destructors ran)");
    Check(g_unwoundSemCancellable.load(std::memory_order_acquire) == 2,
          "semaphore/WaitCancellable: every parked frame unwound");

    // The other half of the policy. Asserting == 0 rather than reporting it means a future change
    // to CancelWaiters cannot quietly flip this: waking these frames would hand each one a lock or
    // a permit it never took, so it has to be an argued change, not a drive-by one.
    Check(g_unwoundMutexPlain.load(std::memory_order_acquire) == 0,
          "mutex/plain Lock(): still parked -- nowhere to report Cancelled, BY POLICY");
    Check(g_unwoundSemPlain.load(std::memory_order_acquire) == 0,
          "semaphore/plain Wait(): still parked -- nowhere to report Cancelled, BY POLICY");

    // WHAT THAT COSTS, said out loud because teardown itself cannot say it. The quiescence loop
    // reads `busy`, the three inboxes and the deques -- a frame parked on a primitive is in NONE of
    // them, so these two abandoned frames read as a quiet pool and teardown returns without
    // complaint. The leak has no runtime symptom beyond destructors that never ran.
    std::printf("\n  By design, %d frame(s) were left parked and never unwound. Teardown cannot\n"
                "  detect this: parked frames are invisible to the quiescence loop, so a pool\n"
                "  that abandoned them still reports a clean shutdown.\n", 4);

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
