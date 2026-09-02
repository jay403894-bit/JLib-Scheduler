// NEGATIVE CONTROL for the fiber-row-balance teardown check.
// Arm 1 reports the resting balance. Arm 2 STRANDS a row on purpose -- a fiber parked on an event
// nobody signals -- so the teardown check must report a leak. If it stays silent here, the check
// is not wired and its silence everywhere else means nothing.
#include "TaskScheduler.h"
#include <cstdio>
#include <atomic>
#include <thread>
#include <chrono>

static std::atomic<int> g_parked{ 0 };
struct Ctx { JLib::SchedulerMutex* m; };

// A NAMED body, like every other task body in the suite -- one spelling, no per-site judgement
// about which form is safe here.
static void StrandBody(void* p) {
    g_parked.fetch_add(1, std::memory_order_release);
    static_cast<Ctx*>(p)->m->Lock();       // parks forever, uncancellable
}

int main(int argc, char**) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(4);
    auto& sched = JLib::TaskScheduler::Instance();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::printf("resting OutstandingFiberRows = %llu (measured: 0 -- the park path does not lease a row)\n",
                (unsigned long long)JLib::TaskScheduler::OutstandingFiberRows());

    if (argc > 1) {                       // STRAND ARM
        // A PLAIN Lock() ON A MUTEX NOBODY UNLOCKS, not an Event. Teardown EJECTS event waiters
        // and cancellable waiters -- it resumes them so they unwind, so they reach DEAD and their
        // rows come back, which is correct and is why an event-parked fiber is NOT a leak. A plain
        // Lock() is told nothing and stays parked by design, so its row is genuinely stranded and
        // is what the teardown check has to catch.
        static JLib::SchedulerMutex* held = new JLib::SchedulerMutex();
        held->Lock();                      // never unlocked, on purpose
        static Ctx ctx; ctx.m = held;
        for (int i = 0; i < 3; ++i) {
            JLib::Task* t = sched.CreateTask(&StrandBody, &ctx,
                                             JLib::Lane::Normal, JLib::TaskType::Fiber);
            if (t) sched.Push(t);
        }
        while (g_parked.load(std::memory_order_acquire) < 3) std::this_thread::yield();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::printf("3 fibers parked on a mutex nobody will unlock -- teardown cannot eject a plain Lock().\n");
    }
    std::printf("after work OutstandingFiberRows = %llu\n",
                (unsigned long long)JLib::TaskScheduler::OutstandingFiberRows());
    std::printf("-- teardown follows --\n");
    return 0;
}
