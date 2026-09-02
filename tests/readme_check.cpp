#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <functional>
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

    JLib::Event& gate = sched.GetEvent("frame_ready");
    sched.Push(sched.CreateTask([&] { sched.WaitOnEvent(gate); }));
    gate.SignalAll();

    JLib::TaskDAG dag(sched);
    auto* physics = dag.CreateNode(sched.CreateTask([]{ }));
    auto* anim    = dag.CreateNode(sched.CreateTask([]{ }));
    auto* render  = dag.CreateMainNode(
                        sched.CreateTask([]{ }, JLib::Lane::Normal, JLib::TaskType::Native));
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
