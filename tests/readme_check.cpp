#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <functional>
#include <atomic>
// The README's suspending example as a NAMED body with an explicit context -- which is the form the
// README itself now shows, because that is the only form the runtime supports for a job that waits.
struct WaiterCtx { JLib::Event* gate; std::atomic<bool>* ready; };
static void WaiterBody(void* p) {
    auto& c = *static_cast<WaiterCtx*>(p);
    JLib::TaskScheduler::Instance().WaitOnEventArmed(*c.gate, [&c] {
        if (c.ready->load(std::memory_order_acquire)) c.gate->SignalAll();
    });
}

int main() {
    JLib::TaskScheduler::EnableIoReactor(true);
    JLib::TaskScheduler::Init();
    auto& sched = JLib::TaskScheduler::Instance();

    JLib::WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);
    JLib::Task* t = sched.CreateTask([] { });
    t->waitGroup = &wg;
    sched.Push(t);
    sched.WaitFor(wg);

    std::function<void(int,int)> body = [&](int lo, int hi) { for (int i = lo; i < hi; ++i) {} };
    sched.ParallelFor(0, 1000000, 4096, body);

    // The README's suspending example, in the form the README now shows: raw void(*)(void*) with
    // the context on THIS frame. The lambda spelling used to be here and it compiled, ran, and
    // aborted at the wait -- which is what made the README's advice wrong rather than merely stale.
    JLib::Event& gate = sched.GetEvent("frame_ready");
    JLib::WaitGroup done;
    done.n.store(1, std::memory_order_relaxed);

    // ARMED, and the README says why: Push-then-SignalAll is a lost-wake race. The plain
    // WaitOnEvent spelling was in the README all along and this file never reached it -- the
    // lambda-fiber abort fired first -- so the race shipped in the documentation unexercised.
    std::atomic<bool> ready{ false };
    WaiterCtx ctx{ &gate, &ready };

    JLib::Task* waiter = sched.CreateTask(&WaiterBody, &ctx,
                                          JLib::Lane::Normal, JLib::TaskType::Fiber);
    waiter->waitGroup = &done;
    sched.Push(waiter);

    ready.store(true, std::memory_order_release);
    gate.SignalAll();
    sched.WaitFor(done);

    JLib::TaskDAG dag(sched);
    auto* physics = dag.CreateNode(sched.CreateTask([]{ }));
    auto* anim    = dag.CreateNode(sched.CreateTask([]{ }));
    auto* render  = dag.CreateMainNode(
                        sched.CreateTask([]{ }, JLib::Lane::Normal));
    dag.AddDependency(render, physics);
    dag.AddDependency(render, anim);
    dag.Submit();
    return 0;
}

// The README's configuration and FLS snippets, compiled.
static const uint16_t kScratch = JLib::TaskScheduler::AllocFiberLocalSlot();
struct State { int x; };
void ReadmeConfigCheck() {
    JLib::TaskScheduler::SetFiberMode(JLib::FiberMode::Pin);
    JLib::TaskScheduler::SetFiberBudget(64, 64, 1);
    JLib::TaskScheduler::SetIoHotLane(2);
    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::Ideal);
    JLib::TaskScheduler::EnableTimers(true);
    JLib::TaskScheduler::SetAwakeFloor(2);
    JLib::TaskScheduler::SetReservedStealing(true);
    JLib::TaskScheduler::SetSubmitLimit(1024);
    JLib::TaskScheduler::SetStealHint(true);

    JLib::TaskScheduler::FiberLocal(kScratch) = nullptr;
    auto* s = JLib::TaskScheduler::FiberLocalAs<State>(kScratch);
    (void)s;
    (void)JLib::TaskScheduler::Fibers();
}
