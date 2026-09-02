// THE THREE DRAIN POINTS, AGAINST A LIVE POOL.
//
// token_registry_test proves the STRUCTURE in isolation. It cannot prove the wiring: that a token
// delivered to holder N is drained BY WORKER N, that a parked worker is woken to do it, and that
// main discharges its own through ProcessMainThread. Those are three edits in three files and every
// one of them can be absent while the structure test stays green -- which is exactly how a wired-up
// mechanism ships doing nothing.
//
// SO EVERY ASSERTION HERE IS ABOUT *WHICH THREAD RAN THE RELEASE*, not about whether it ran. A
// release that happens on the wrong thread is the precise bug the routing exists to prevent, and a
// test that only counted releases would pass with the routing deleted.
//
// NEGATIVE CONTROL:
//   cmake -S . -B build-ctl -DJLIBSCHED_TOKENDRAIN_CTL=NO_NOTIFY
//     -> Deliver stops waking the holder. The PARKED-worker case must TIME OUT, because a parked
//        worker with tokens on its chain never drains them. If that case still passes, the wake is
//        not what is delivering the cleanup and this test is measuring something else.

#include "../include/TaskScheduler.h"
#include "../include/TokenRegistry.h"
#include "../include/Thread.h"
#include <cstdio>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-68s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

// Where each release actually ran. -1 == a thread that is not one of our workers (main).
static std::atomic<int>    g_ranOn[512];
static std::atomic<size_t> g_released{ 0 };

// NOINLINE, and for the reason this whole session turned on: the compiler may cache the
// thread_local base across a call, and this is read from a fiber-capable worker. Behind an opaque
// call the lookup happens on the thread actually running. See design/NOTES.md.
#if defined(_MSC_VER)
__declspec(noinline)
#else
__attribute__((noinline))
#endif
static int CurrentQ() {
    Thread* w = Thread::Current();
    return w ? (int)w->qIndex : -1;
}

static void RecordRelease(size_t /*holder*/, DebtToken* t) {
    const size_t idx = (size_t)(intptr_t)t->readerId;   // the test stamps readerId as its own index
    if (idx < 512) g_ranOn[idx].store(CurrentQ(), std::memory_order_relaxed);
    g_released.fetch_add(1, std::memory_order_release);
}

static bool WaitFor(size_t target, int ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (g_released.load(std::memory_order_acquire) < target) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== token drain: the three points, against a live pool ===\n");
#if defined(JLIB_TOKENDRAIN_CTL_NO_NOTIFY)
    std::printf("  [CONTROL: NO_NOTIFY -- the PARKED-worker case MUST time out]\n");
#endif

    TaskScheduler::Init(8);
    auto& sched = TaskScheduler::Instance();
    auto& reg   = TokenRegistry::Instance();
    reg.SetRelease(&RecordRelease);

    const size_t nWorkers = sched.GetWorkerCount();
    std::printf("  workers=%zu  holders=%zu  capacity=%zu\n",
                nWorkers, reg.HolderCount(), reg.Capacity());
    Check(reg.HolderCount() >= nWorkers, "registry was built by StartPool (holders cover the workers)");

    // ---- DRAIN POINT 1: a token delivered to worker N is released BY WORKER N -----------------
    //
    // THE POOL IS DELIBERATELY LEFT IDLE FIRST. A busy pool would drain on its way through the task
    // loop for reasons that have nothing to do with the wake, so an idle pool is what makes this a
    // test of the notify rather than of the traffic.
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(120));   // let everyone park
        g_released.store(0);

        const size_t n = nWorkers;
        std::vector<DebtToken> toks(n);
        for (size_t i = 0; i < n; ++i) {
            for (auto& c : g_ranOn) c.store(-2, std::memory_order_relaxed);
            toks[i].readerId = i;
        }
        for (size_t i = 0; i < n; ++i) reg.Deliver(i, &toks[i]);

        const bool arrived = WaitFor(n, 3000);
        Check(arrived, "PARKED workers were WOKEN to drain (this is what Deliver's notify buys)");

        size_t wrongThread = 0, unrun = 0;
        for (size_t i = 0; i < n; ++i) {
            const int on = g_ranOn[i].load();
            if (on == -2) { ++unrun; continue; }
            if (on != (int)i) ++wrongThread;
        }
        std::printf("    delivered=%zu released=%zu wrong-thread=%zu never-ran=%zu\n",
                    n, g_released.load(), wrongThread, unrun);
        Check(unrun == 0,       "every delivered token was released");
        Check(wrongThread == 0, "each release ran ON ITS OWN HOLDER (routing, not just delivery)");
    }

    // ---- DRAIN POINT 2: main discharges its own through ProcessMainThread ---------------------
    //
    // Nothing else can. A worker cannot run main's release -- that is the affinity the whole design
    // is about -- and the scheduler cannot preempt main to make it happen.
    {
        g_released.store(0);
        for (auto& c : g_ranOn) c.store(-2, std::memory_order_relaxed);

        const size_t mine = reg.CurrentHolder();
        Check(mine != kMaxHolders, "main claimed an external holder id");
        Check(mine >= nWorkers,    "main's holder is EXTERNAL, not a worker's (it is not a worker)");

        DebtToken tok;
        tok.readerId = 300;
        reg.Deliver(mine, &tok);

        // NOT drained by anyone else, and that is the assertion -- give the pool a generous window
        // to prove no worker touches it.
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        Check(g_released.load() == 0,
              "main's debt is NOT drained by a worker (affine work stays on its own thread)");

        sched.ProcessMainThread();
        Check(g_released.load() == 1, "ProcessMainThread drained main's own chain");
        Check(g_ranOn[300].load() == -1, "main's release ran on MAIN (qIndex -1), not on a worker");
    }

    // ---- DRAIN POINT 3: nothing exits still owing ---------------------------------------------
    //
    // Delivered and then deliberately NOT drained, so Join() is the only thing that can settle it.
    // A leak here is silent by nature: the release simply never runs and nothing reports it.
    {
        g_released.store(0);
        for (auto& c : g_ranOn) c.store(-2, std::memory_order_relaxed);

        DebtToken tok;
        tok.readerId = 301;
        // Holder 0 while the pool is ALIVE would just be drained by worker 0. The point is to leave
        // a debt outstanding at teardown, so this one is aimed at an external holder nobody polls.
        const size_t idle = nWorkers + 1;
        const bool ok = reg.Deliver(idle, &tok);
        Check(ok, "delivery to an unpolled external holder is accepted");
        Check(g_released.load() == 0, "and stays outstanding while the pool runs");

        // Join() is deliberately PRIVATE -- see the note on detail::TeardownForTesting. This is the
        // supported way in, and it is the same path AtExitDestroyer takes.
        JLib::detail::TeardownForTesting(sched);
        std::printf("    after teardown: released=%zu\n", g_released.load());
        Check(g_released.load() == 1, "Join() drained the holder nobody was left to drain");
    }

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
