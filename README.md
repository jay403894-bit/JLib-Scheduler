# JLib::Scheduler

A fiber-based, work-stealing task scheduler for real-time engines. Hand-written context switching,
lock-free Chase-Lev deques, a slab-allocated task system, frame DAGs with logic gates, and
hybrid-core aware placement.

Windows x64 (MSVC) · Linux x86-64 · Linux/Android AArch64 · macOS Apple Silicon · C++17 · BSD

[![CI](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml)

<!-- ============================================================================================
     GAP 1 -- WRITE THIS ONE YOURSELF, IN FIRST PERSON. It is the most important paragraph in the
     file and the one that should not sound like anybody else.

     Three or four sentences, plain: what you were building, what you needed the job system to do,
     what you looked at first and why it did not fit, and what you decided to do instead. Concrete
     beats impressive -- name the actual problem you hit.

     Something in this shape (do not use these words, they are mine):
       "I was writing a 2D/3D engine and needed tasks that could wait on a GPU fence without
        parking a worker thread. enkiTS and Taskflow do not suspend inside a task; marl does but
        was archived in April. So I wrote this, and it has been running my engine since <month>."

     Delete this whole comment block when you are done.
============================================================================================ -->

**TODO: replace this paragraph with GAP 1 above.**

---

## Measured

<!-- GAP 2 (small): one sentence saying which machine and build, and that these are your numbers
     rather than a spec. Third-party runs have already come in higher on some metrics, so this is
     worth being plain about. Delete this comment when done. -->

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

The one thing to get right: a task that will call `WaitFor` must be created with `noFiber = false`.
`noFiber` defaults to true, and a task with no fiber under it cannot suspend — it fail-fasts with no
message. See [DESIGN.md](DESIGN.md#integration-contracts).

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

## Limitations

- x86-64 or AArch64 only, 64-bit only, and Windows means MSVC. The context switch is hand-written
  per ABI, so a new architecture is real work rather than a compiler flag. Windows on ARM64 is
  refused explicitly.
- Processor group 0 only, so at most 64 logical CPUs. Dual-socket machines need work this does not
  do.
- Tasks are 256-byte slab slots. Lambda captures past ~192 bytes fail a `static_assert` — capture
  pointers, not payloads.
- `CorePref::P` / `CorePref::E` do nothing outside Windows. Safe, since preference is a hint that
  spills, but do not build a design around it.

The reasoning behind each of these, and the decisions that were tried and removed, is in
[DESIGN.md](DESIGN.md).

## Versioning

1.0.0. The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`; those follow semver and do
not break without a 2.0. Every header is installed because the supported ones need them to compile,
but the rest are implementation detail and may change in any release. If you need something only
reachable through one of those, that is a missing feature — open an issue rather than depend on it.

[CHANGELOG.md](CHANGELOG.md) has the release history and the negative results.

## License

BSD 3-Clause. Use it, fork it, ship it.
