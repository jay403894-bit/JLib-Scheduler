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

| | i9-13900K, Release, 1.0.1 |
|---|---|
| Task enqueue → dequeue latency | 4.7 µs |
| 6-node frame DAG (build, validate, execute) | 23.2 µs |
| 1M-element recursive fork-join (10k leaves) | 0.19 ms |
| `Task` struct size | 64 bytes, one cache line, `static_assert`-enforced |
| Fiber stacks | 64 KB standard / 512 KB heavy, contiguous arena, guard-paged |
| Steal protocol | single-item Chase-Lev CAS |

Medians on the default affinity policy. Run-to-run spread was 3% on latency, 7% on the DAG and about
25% on fork-join, so treat the last digit as noise and the fork-join figure as a rough one.
`SchedulerBench` prints the version it was built from, and these numbers move between versions:
quoting one without the other is how the previous table ended up 25% pessimistic.

Run them yourself with `SchedulerBench`. It takes an affinity policy argument and defaults to the
same one the library does.

## Model checked

The two lock-free structures are model checked with [GenMC](https://plv.mpi-sws.org/genmc/), not
only tested. A test runs whichever interleaving the CPU happens to produce; a model checker
enumerates every execution the C11 memory model permits, so for a bounded harness the result is
exhaustive rather than lucky. Models live in `tests/verify/`.

- **Chase-Lev deque** (`deque_model.c`) - one owner, two thieves, 174 executions, no errors. It also
  settled a real question: the published verified Chase-Lev uses `seq_cst` for the steal CAS and this
  uses `acq_rel`. Both check clean, so the weaker ordering is sufficient here and the `seq_cst`
  fences are what carry the ordering.
- **Event waiter stack** (`event_model.c`) - two pushers, one drainer, 24 executions, no errors. No
  waiter lost, none woken twice, no race on the plain `nextWaiter` field.

Each model ships a negative control, which is the part that makes a clean run mean anything. Build
the deque model with `-DNO_POP_FENCE` and it produces a double-claim in under a second: two threads
taking the same task, the use-after-free class. That fence had been called redundant more than once.

This is not a proof of the whole scheduler. It covers two data structures at small bounds, which is
where memory-ordering bugs live.

## Build


```
cmake -B build -DCMAKE_BUILD_TYPE=Development
cmake --build build -j
```

Or open `Scheduler.sln` in Visual Studio. Three build types: `Debug`, `Release`, and `Development`
(optimized, with symbols and assertions live -- it deliberately does not define `NDEBUG`).

To consume an installed copy, `find_package(JLibScheduler)` and link `JLib::Scheduler`.

Adding the sources to your own build directly: take `src/*.cpp` plus exactly one platform directory.
That is either `src/win32/` with its `ContextSwitch.asm`, or `src/posix/` (or `src/darwin/` on
macOS) plus exactly one architecture subdirectory, `src/posix/x86_64/` or `src/posix/aarch64/`.
Never two of the same kind -- they define the same symbols, and a static library will not diagnose
that. It silently links whichever one it reaches first.

### iOS and other Apple platforms

Not supported, but probably working. iOS, tvOS, watchOS and visionOS are arm64 Darwin, so they use
the same AAPCS64 context switch and the same `src/darwin/` OS layer that macOS arm64 uses, and that
configuration is verified in CI on every push. What is missing is that nobody has ever run the
result. I have no Apple hardware, so I cannot produce that run and will not claim the platform.

If you can, the build is opt-in:

```
cmake -B build-ios -G Xcode -DCMAKE_SYSTEM_NAME=iOS -DCMAKE_OSX_ARCHITECTURES=arm64 \
      -DJLIBSCHED_ALLOW_UNVERIFIED_PLATFORM=ON
```

Without that flag it refuses and tells you the flag exists. With it, it warns and proceeds.

Expect three things. Placement is a no-op on Apple platforms (see below). An iOS executable has to
be wrapped in an app bundle before it will run at all, since there is no console. And the third is
the one that might make this a poor fit regardless of whether it compiles: **a pool of persistent
worker threads does not map cleanly onto the iOS app lifecycle.** This scheduler is built on the
assumption that the application largely owns the machine, which is true of a game in the foreground
and false the instant it is backgrounded -- threads are frozen mid-execution, and anything waiting on
a fence or a lock stalls until the app resumes. `TaskScheduler::Pause()` and `Resume()` exist as the
hook for that, but nothing here wires them to `applicationDidEnterBackground`, and a `hardware_concurrency - 1`
pool is an awkward shape on a phone where the OS is actively managing power. Treat iOS as
foreground-only unless you do that work yourself.

Please open an issue either way -- a report that it works is as useful as one that it doesn't, and a
PR from someone with the hardware is very welcome.

Windows on ARM64 is refused outright with no flag, but not because anything about it is hard. The
Windows ARM64 calling convention matches AAPCS64 on everything a context switch touches, so the
existing register save/restore carries over; what is actually missing is an `armasm64` translation
of it, an ARM64 configuration in the MSBuild project, and somebody with a Snapdragon machine who
wants this. It is roughly a day of mechanical work that nobody has asked for yet.

If you are that somebody, open an issue. That is the signal that would move it, and it is the
difference between a fifth supported platform and a fifth platform I cannot run.

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

### Submit from inside the pool where you can

Tasks that spawn tasks are the path this is built around, and the gap is larger than it sounds. On a
32-thread i9, pushing 200,000 tasks from one thread runs at 3.4 M/s with 8 workers and **0.8 M/s
with 31**, going backwards as workers are added. Submitting the same work from four tasks already on
the pool holds 4 to 6 M/s across the whole range.

Two reasons, and they compound. A single submitting thread is a fixed serial cost, so once the pool
can drain faster than one thread can push, the surplus workers find empty deques and their steal
traffic slows the producer down further. And the deques are Chase-Lev: their owner pushes and pops
locally with no atomic in the common case, and only thieves pay for the CAS. That asymmetry assumes
the producer is a worker. A task spawning a task gets that fast path; something pushing from outside
the pool goes through the shared inbox instead.

None of which means one submitter is wrong. A main thread submitting a frame's work is a normal
shape and 3.4 M/s is plenty for it. It means that if you are feeding the pool from a loop and
wondering why more workers is not helping, the loop is the thing to look at, not the pool size.
`SchedulerBench` reports both as `throughput/1p` and `throughput/mp` so the difference is visible on
your own hardware.

Three things to know while writing against it. A task that will call `WaitFor` must be created with
`noFiber = false` -- `noFiber` defaults to true, and a task with no fiber under it cannot suspend, so
it fail-fasts with no message. Tasks live in 256-byte slab slots, so a lambda capturing more than
about 192 bytes fails a `static_assert`: capture pointers, not payloads.

And `CorePref::P` / `CorePref::E` are Windows-only. Elsewhere the scheduler has no way to tell the
core classes apart, so those requests are silently ignored rather than rejected -- leave tasks at
`Default` or `Any` on other platforms, and don't build a design that assumes the placement held.

Machines wider than 64 logical CPUs are handled across all processor groups, up to 256 CPUs. This is
worth stating because it is the thing most job systems get wrong on Windows: `SetThreadAffinityMask`
takes its processor group from the calling thread rather than from its argument, so on a 128-thread
box no mask value can name a CPU in the second group. Binding here goes through
`SetThreadGroupAffinity` and `SetThreadIdealProcessorEx`, which take the group as data.

That path is written and reasoned through but **has not yet run on a machine wide enough to exercise
it**, because I do not own one. If you do, that is the single most useful thing you could report.
Everything at 64 CPUs or below is unaffected and is what the tested numbers come from.

[DESIGN.md](DESIGN.md) has the rest -- the execution model, the integration contracts, and the
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
for an ordinary thread pool works unchanged -- Jolt Physics runs through a `JPH::JobSystem` adapter
and never learns fibers exist. [Why that matters](DESIGN.md#the-hybrid-is-a-correctness-boundary-not-a-performance-dial).

## Versioning

1.0.0. The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`; those follow semver and do
not break without a 2.0. Every header is installed because the supported ones need them to compile,
but the rest are implementation detail and may change in any release. If you need something only
reachable through one of those, that is a missing feature -- open an issue rather than depend on it.

[CHANGELOG.md](CHANGELOG.md) has the release history and the negative results.

## License

BSD 3-Clause. Use it, fork it, ship it.
