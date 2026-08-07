// TSAN probe -- NOT a benchmark. Exercises each lock-free structure enough times for TSAN to
// observe conflicting accesses, then exits. Deliberately small: a race is reported the FIRST time
// two conflicting accesses occur without intervening synchronisation, so volume buys nothing and
// costs 5-15x runtime under instrumentation. The full bench is the wrong target for this.
//
//   g++ -std=c++20 -O1 -g -fno-omit-frame-pointer -fsanitize=thread -I include \
//       src/*.cpp src/posix/*.cpp src/posix/ContextSwitch.s bench/tsan_probe.cpp \
//       -o /tmp/tsan_probe -lpthread
//   setarch $(uname -m) -R ./tsan_probe        # -R works around TSAN's ASLR mapping error
//
// READING THE OUTPUT -- two known blind spots, do not chase either:
//   * TaskDeque uses std::atomic_thread_fence, which TSAN CANNOT model (gcc warns -Wtsan). Reports
//     naming pop_bottom/steal are almost certainly that blind spot, not a real race.
//   * Fibers move stacks between threads without calling __tsan_switch_to_fiber, so TSAN may
//     attribute one thread's history to another. Reports straddling a fiber task are suspect.
// What IS trustworthy: reports where both stacks sit in Epochs, LockFreeList, WaitGroup, the fiber
// STATUS transitions, or TaskAllocator. Those are the structures an ARM port would stress.
#include <TaskScheduler.h>
#include <TaskDAG.h>
#include <atomic>
#include <cstdio>

static std::atomic<int> g_counter{0};

int main() {
    JLib::TaskScheduler::Init();
    auto& sched = JLib::TaskScheduler::Instance();

    // 1. fastJob fan-out: worker deques, the task allocator, and WaitGroup completion. fastJob runs
    //    on the worker's own OS stack, so these paths involve NO fiber switch -- anything reported
    //    here is real, not a fiber-attribution artifact.
    {
        JLib::WaitGroup wg;
        constexpr int kN = 400;
        wg.n.store(kN, std::memory_order_relaxed);
        for (int i = 0; i < kN; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {
                g_counter.fetch_add(1, std::memory_order_relaxed);
            }, nullptr);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        printf("fan-out done      : counter=%d (expect 400)\n", g_counter.load());
    }

    // 2. DAGs with real dependencies: TaskDAG's node graph and the LockFreeList behind
    //    AddDependency, plus the epoch manager that guards its reclamation. Several small graphs
    //    rather than one big one, so nodes are added AND retired repeatedly.
    for (int r = 0; r < 8; ++r) {
        JLib::TaskDAG dag(sched);
        JLib::WaitGroup wg;
        wg.n.store(1, std::memory_order_relaxed);
        auto* a = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
        auto* b = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
        auto* c = dag.CreateNode(sched.CreateTask(+[](void*) {}, nullptr));
        JLib::Task* last = sched.CreateTask(+[](void*) {}, nullptr);
        last->waitGroup = &wg;
        auto* d = dag.CreateNode(last);
        dag.AddDependency(b, a);
        dag.AddDependency(c, a);
        dag.AddDependency(d, b);
        dag.AddDependency(d, c);
        dag.Submit();
        sched.WaitFor(wg);
    }
    printf("dag rounds done   : 8\n");

    // 3. Fiber-backed tasks that actually suspend: the fiber status state machine, the global fiber
    //    pool's acquire/release, and the context switch itself. This is the section most likely to
    //    produce fiber-attribution noise -- and also the only one that touches those paths at all.
    {
        JLib::WaitGroup wg;
        constexpr int kF = 32;
        wg.n.store(kF, std::memory_order_relaxed);
        for (int i = 0; i < kF; ++i) {
            JLib::Task* t = sched.CreateTask(+[](void*) {
                g_counter.fetch_add(1, std::memory_order_relaxed);
            }, nullptr, /*hipri*/0, JLib::FiberSize::Standard, /*fastJob*/0);
            t->waitGroup = &wg;
            sched.Push(t);
        }
        sched.WaitFor(wg);
        printf("fiber tasks done  : counter=%d (expect 432)\n", g_counter.load());
    }

    printf("probe complete\n");
    return 0;
}
