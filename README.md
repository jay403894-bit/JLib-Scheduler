# JLib::Scheduler

A fiber-based, work-stealing task scheduler for real-time engines. Hand-written context switching,
lock-free Chase-Lev deques, a slab-allocated task system, frame DAGs with logic gates, and
hybrid-core aware placement.

Windows x64 (MSVC) · Linux x86-64 · Linux/Android AArch64 · macOS Apple Silicon · C++17 · BSD

[![CI](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml)

I built this scheduler to solve the problem of scheduling for my custom 2d/3d engine  -- it was built to be the backbone of multithreaded simulation engines.
I needed tasks that could wait on a gpu fence without parking a worker thread, enkiTS and taskflow cannot do this, marl can but was archived in April.

---

## Measured

This was tested on my machine, third party tests have come back and some are faster than mine depending on hardware and platform.
Needs and welcomes more testing for research!

| | i9-13900K, Release |
|---|---|
| Task enqueue → dequeue latency | 6.3 µs |
| 5-node frame DAG (build, validate, execute) | 31.9 µs |
| `Task` struct size | 64 bytes, one cache line, `static_assert`-enforced |
| Fiber stacks | 64 KB standard / 512 KB heavy, contiguous arena, guard-paged |
| Steal protocol | single-item Chase-Lev CAS |

Run them yourself with `SchedulerBench` — it takes an affinity policy argument and defaults to the
same one the library does.
## Build


```
cmake -B build -DCMAKE_BUILD_TYPE=Development
cmake --build build -j
```

Or open `Scheduler.sln` in Visual Studio. Three build types: `Debug`, `Release`, and `Development`
(optimized, with symbols and assertions live — it deliberately does not define `NDEBUG`).

To consume an installed copy, `find_package(JLibScheduler)` and link `JLib::Scheduler`.

Adding the sources to your own build directly: take `src/*.cpp` plus exactly one platform directory.
That is either `src/win32/` with its `ContextSwitch.asm`, or `src/posix/` (or `src/darwin/` on
macOS) plus exactly one architecture subdirectory, `src/posix/x86_64/` or `src/posix/aarch64/`.
Never two of the same kind — they define the same symbols, and a static library will not diagnose
that. It silently links whichever one it reaches first.

## Using it

```cpp
#include <TaskScheduler.h>

int main() {
    JLib::TaskScheduler::Init();               // auto pool size (hw-1)
    auto& sched = JLib::TaskScheduler::Instance();

    // Fire and forget. noFiber is the default: runs inline on a worker, no fiber, no switch.
    sched.Push([] { HeavyMath(); });

    // Fork-join: create tasks against a WaitGroup, then wait.
    JLib::WaitGroup wg;
    for (int i = 0; i < 8; ++i) {
        auto* t = sched.CreateTask([i] { Chunk(i); });
        t->waitGroup = &wg;
        wg.n.fetch_add(1, std::memory_order_release);
        sched.Push(t);
    }
    sched.WaitFor(wg);

    // Data-parallel loop. Decides serial vs parallel by timing a prefix, not by element count.
    sched.ParallelFor(0, 1'000'000, 4096, [](int a, int b) {
        for (int i = a; i < b; ++i) out[i] = std::sqrt((float)i);
    });

    sched.Join();
}
```

Three things to know while writing against it. A task that will call `WaitFor` must be created with
`noFiber = false` — `noFiber` defaults to true, and a task with no fiber under it cannot suspend, so
it fail-fasts with no message. Tasks live in 256-byte slab slots, so a lambda capturing more than
about 192 bytes fails a `static_assert`: capture pointers, not payloads.

And `CorePref::P` / `CorePref::E` are Windows-only. Elsewhere the scheduler has no way to tell the
core classes apart, so those requests are silently ignored rather than rejected — leave tasks at
`Default` or `Any` on other platforms, and don't build a design that assumes the placement held.

[DESIGN.md](DESIGN.md) has the rest — the execution model, the integration contracts, and the
decisions that were tried and removed.

## How it compares

| | enkiTS | Taskflow | marl | this |
|---|---|---|---|---|
| Work-stealing | yes | yes | yes | yes |
| Suspend/resume inside a task | no | no | yes | yes |
| Dependency DAG with AND/OR gates | partial | yes | no | yes |
| Cache/SMT topology-aware stealing | no | no | no | yes |
| Maintained | yes | yes | archived Apr 2026 | yes |

marl and FiberTaskingLib run every task on a fiber. Here fibers are opt-in, so middleware written
for an ordinary thread pool works unchanged — Jolt Physics runs through a `JPH::JobSystem` adapter
and never learns fibers exist. [Why that matters](DESIGN.md#the-hybrid-is-a-correctness-boundary-not-a-performance-dial).

## Versioning

1.0.0. The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`; those follow semver and do
not break without a 2.0. Every header is installed because the supported ones need them to compile,
but the rest are implementation detail and may change in any release. If you need something only
reachable through one of those, that is a missing feature — open an issue rather than depend on it.

[CHANGELOG.md](CHANGELOG.md) has the release history and the negative results.

## License

BSD 3-Clause. Use it, fork it, ship it.
