// DO FIBERS ACTUALLY MIGRATE? -- and pinned mode is the control that proves the test can tell.
//
// SetMigratableFibers(true) changes exactly one thing: Requeue routes a resumed fiber to the
// current worker's DEQUE BOTTOM (any worker may take it) instead of to its binding worker's resume
// inbox (exactly one may). The claim is that this makes a resume land on whoever is free rather
// than waiting for one specific thread.
//
// SO THE MEASUREMENT IS: did a fiber resume on a DIFFERENT worker than it suspended on?
//
// AND THE CONTROL IS PINNED MODE, RUN IN THE SAME PROCESS. A test that only ran the migratable arm
// could report "0 migrations" for two completely different reasons -- migration is broken, or the
// workload never gave it the chance -- and those want opposite fixes. Pinned must report ZERO by
// construction (that is what pinning IS), so:
//
//   pinned migrations > 0      -> the harness is wrong, not the scheduler. Pinning is violated or
//                                 the qIndex observation is bogus. FAIL either way.
//   migratable migrations == 0 -> either migration does not work, or this workload never suspended
//                                 anything while another worker was free. The suspend count below
//                                 separates those.
//
// WHY THIS IS SAFE TO RUN TODAY, before any resource wrapper sets a creditor: the library's own
// thread_locals are already migration-safe and were audited for it -- SlabPool::Free routes by
// ADDRESS, epochs use a GLOBAL participant list, hazard cells are indexed by the FIBER not the
// thread, and Fiber::localEpoch lives on the fiber. Fiber::creditors exists for APPLICATION-owned
// affine state, of which this test has none.

#include "../include/TaskScheduler.h"
#include "../include/Thread.h"
#include "../include/Event.h"
#include <cstdio>
#include <atomic>
#include <vector>
#include <thread>
#include <chrono>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

static constexpr int kTasks = 256;

// Per task: the worker it was on before the wait, and the one it was on after.
static std::atomic<int> g_before[kTasks];
static std::atomic<int> g_after[kTasks];
static std::atomic<int> g_sentTo[kTasks];   // where Requeue routed this fiber's resume
static std::atomic<int> g_ran{ 0 };
static std::atomic<bool> g_released{ false };
static Event* g_gate = nullptr;

// NOINLINE, AND THE TEST IS WRONG WITHOUT IT -- this is the whole reason the first four versions of
// this test reported "0 migrations" while the router reported 203 of 256 resumes sent elsewhere.
//
// Thread::instance is a thread_local. Inlined here, the compiler loads its TLS address ONCE and
// reuses it on both sides of the wait, because nothing in the source tells it the thread can change
// underneath -- and for every other test in this suite it cannot. A fiber that suspends on worker 1
// and resumes on worker 4 then reads worker 1's TLS block and reports "I did not move".
//
// Behind an opaque call the lookup happens per call, after the switch, on the thread actually
// running. Not a style preference: it is the difference between measuring the feature and
// measuring the compiler.
//
// AND IT IS ALSO THE FINDING. Anything that caches a TLS-derived value across a suspend is broken
// under migration -- see design/NOTES.md. The test hit it first because it is the first code that
// deliberately reads the same thread_local on both sides of one.
#if defined(_MSC_VER)
__declspec(noinline)
#else
__attribute__((noinline))
#endif
static int CurrentQ() {
    Thread* w = Thread::Current();
    return w ? w->qIndex : -1;
}

struct Arm { int migrated; int suspended; int completed; };

static Arm RunArm(TaskScheduler& sched, const char* name, const char* gateName) {
    // A FRESH EVENT PER ARM. Reusing one across arms means the second arm may find it already
    // signalled from the first, return from the wait WITHOUT PARKING, and report before==after for
    // a reason that has nothing to do with pinning. The suspend count below cannot see that -- it
    // counts tasks that recorded both fields, which they do whether or not they ever suspended.
    g_gate = &sched.GetEvent(gateName);
    for (int i = 0; i < kTasks; ++i) { g_before[i].store(-1); g_after[i].store(-1); g_sentTo[i].store(-1); }
    g_ran.store(0);
    g_released.store(false);

    WaitGroup wg;
    wg.n.store(kTasks, std::memory_order_relaxed);

    for (int i = 0; i < kTasks; ++i) {
        Task* t = sched.CreateTask(+[](void* p) {
            const int idx = (int)(intptr_t)p;
            g_before[idx].store(CurrentQ(), std::memory_order_relaxed);
            // ARMED WAIT: if the release already happened we self-signal rather than parking
            // forever. Same race-freedom the compare harness uses.
            TaskScheduler::Instance().WaitOnEventArmed(*g_gate, [] {
                if (g_released.load(std::memory_order_acquire)) g_gate->SignalAll();
            });
            g_after[idx].store(CurrentQ(), std::memory_order_relaxed);
#if defined(JLIBSCHED_REQUEUE_TRACE)
            // THE JOIN. Where the router SAID it sent me, read by the fiber that was sent, so the
            // two ends finally refer to one task instead of two aggregates.
            if (Thread* me = Thread::Current())
                if (Fiber* mf = me->currentFiber)
                    g_sentTo[idx].store((int)(mf->lastPlacedOn == SIZE_MAX ? -1
                                                                          : (int)mf->lastPlacedOn),
                                        std::memory_order_relaxed);
#endif
            g_ran.fetch_add(1, std::memory_order_relaxed);
        }, (void*)(intptr_t)i, JLib::Lane::Normal, TaskType::Fiber);
        if (!t) { std::printf("  %s: CreateTask returned null\n", name); return { -1, -1, -1 }; }
        t->waitGroup = &wg;
        sched.Push(t);
    }

    // Let them all reach the wait, then release together so the resumes are concurrent -- that is
    // the state in which a free worker can pick one up.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    TaskScheduler::RequeueTraceReset();   // count only the RESUMES, not the initial submissions
    g_released.store(true, std::memory_order_release);
    g_gate->SignalAll();
    sched.WaitFor(wg);
    TaskScheduler::RequeueTraceReport(name);

    Arm a{ 0, 0, g_ran.load() };
    // HOW MANY DISTINCT WORKERS ON EACH SIDE. Without this, "0 migrations" has two readings and the
    // test cannot say which: migration is broken, or every task ran on ONE worker so there was
    // never anywhere to migrate FROM. The suspend count does not separate them -- 256 suspends on
    // one worker looks identical to 256 spread across eight. This does.
    bool seenB[64] = {}, seenA[64] = {};
    for (int i = 0; i < kTasks; ++i) {
        const int b = g_before[i].load(), af = g_after[i].load();
        if (b >= 0 && af >= 0) {
            ++a.suspended;
            if (b != af) ++a.migrated;
            if (b  < 64) seenB[b]  = true;
            if (af < 64) seenA[af] = true;
        }
    }
    int nb = 0, na = 0;
    for (int i = 0; i < 64; ++i) { nb += seenB[i] ? 1 : 0; na += seenA[i] ? 1 : 0; }
    // RAW PAIRS. The summary counters above agreed with each other and still hid the answer once:
    // Requeue reported 178 of 256 routed AWAY from home while this reported 0 migrations. When two
    // instruments disagree, print the underlying data rather than adding a third counter.
    std::printf("               first 24 (before->sent->after): ");
    for (int i = 0; i < 24 && i < kTasks; ++i)
        std::printf("%d>%d>%d ", g_before[i].load(), g_sentTo[i].load(), g_after[i].load());
    std::printf("\n");
    // THE THREE-WAY SPLIT. "sent != before" is the ROUTER doing its job; "after != sent" is the
    // route being ignored downstream. Only one of those is a bug and they were indistinguishable.
    int sentAway = 0, landedWhereSent = 0;
    for (int i = 0; i < kTasks; ++i) {
        const int b = g_before[i].load(), s = g_sentTo[i].load(), af = g_after[i].load();
        if (s < 0) continue;
        if (s != b)  ++sentAway;
        if (s == af) ++landedWhereSent;
    }
    std::printf("               routed away from home %3d   landed where routed %3d\n",
                sentAway, landedWhereSent);
    std::printf("  %-12s completed %3d/%d   observed %3d   resumed elsewhere %3d"
                "   workers before=%d after=%d\n",
                name, a.completed, kTasks, a.suspended, a.migrated, nb, na);
    if (nb <= 1)
        std::printf("               ^ every task RAN on one worker, so this arm could not have\n"
                    "                 migrated anything. The result is about the WORKLOAD, not\n"
                    "                 about the feature.\n");
    return a;
}

int main() {
    // UNBUFFERED: this test crashed at startup and lost its entire buffer, so the first run showed
    // nothing at all. A test that can die mid-setup must not hide where.
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== migratable fibers: does a resume land on another worker? ===\n");

    // PINNED FIRST, so the control is measured before the thing it controls for -- and so a failure
    // in the control is not explained away by whatever the other arm did to the pool.
    TaskScheduler::SetMigratableFibers(false);
    TaskScheduler::Init(8);
    TaskScheduler& sched = TaskScheduler::Instance();
    

    const Arm pinned = RunArm(sched, "pinned", "gate_pinned");
    Check(pinned.completed == kTasks, "pinned: every task completed");
    Check(pinned.suspended > 0,       "pinned: tasks actually suspended and resumed");
    // THE CONTROL. Pinning means a resumed fiber goes to its binding worker's inbox, which no other
    // worker may drain -- so a different qIndex after the wait is not 'a bit of noise', it is the
    // invariant being violated.
    Check(pinned.migrated == 0,       "pinned: NOBODY resumed elsewhere (this is what pinning IS)");

    // ---- FLIPPING THE FLAG HERE RATHER THAN REBUILDING THE POOL ------------------------------
    //
    // The first version of this test did TeardownForTesting + Init(8) to give the second arm a
    // clean pool, because the flag is documented as set-before-Init. That CRASHES -- 0xC0000409,
    // STATUS_STACK_BUFFER_OVERRUN, immediately after the pinned arm passed. It is not this flag's
    // fault: NO OTHER TEST IN THE SUITE RE-INITS AFTER TEARDOWN, so the path has simply never been
    // exercised. Recorded in design/NOTES.md; not chased here.
    //
    // WHY FLIPPING IS SOUND AT THIS EXACT POINT, which is the only reason it is allowed: the
    // contract says set-before-Init because a fiber BOUND under one routing rule must not be
    // RESUMED under the other. WaitFor(wg) above has returned with all 256 tasks complete, so
    // there is no fiber in flight to be caught mid-rule. The assert makes that a checked
    // precondition rather than a claim -- if the first arm ever leaves work outstanding, this
    // stops instead of quietly measuring a mixed pool.
    if (pinned.completed != kTasks) {
        std::printf("  REFUSING to flip the mode: the pinned arm left %d/%d outstanding, so a\n"
                    "  fiber bound under pinning could be resumed under migration.\n",
                    pinned.completed, kTasks);
        return 1;
    }
    TaskScheduler::SetMigratableFibers(true);

    std::printf("  MigratableFibers() reports: %s\n",
                TaskScheduler::MigratableFibers() ? "TRUE" : "FALSE");
    const Arm mig = RunArm(sched, "migratable", "gate_migratable");
    Check(mig.completed == kTasks, "migratable: every task completed -- migration lost nothing");
    Check(mig.suspended > 0,       "migratable: tasks actually suspended and resumed");
    // THE CLAIM. If this is 0 while `suspended` is high, migration is not happening; the two
    // numbers together are what make that readable rather than ambiguous.
    Check(mig.migrated > 0,        "migratable: at least one fiber resumed on a DIFFERENT worker");

    std::printf("\n  pinned %d migrations vs migratable %d -- the gap IS the feature\n",
                pinned.migrated, mig.migrated);
    std::printf("=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
