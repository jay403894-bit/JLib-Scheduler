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

| | i9-13900K at Intel spec power limits, Release, 1.3.0 |
|---|---|
| Task enqueue → dequeue latency | 4.7 µs |
| 6-node frame DAG (build, validate, execute) | 22.7 µs |
| 1M-element recursive fork-join (10k leaves) | 0.22 ms |
| Bulk submission via `PushBatch` | 12.1 M tasks/sec |
| Bulk submission, 4 producers | 8.4 M tasks/sec |
| Per-item cost via `PushArray` (chunk 128) | 1.0 ns |
| `Task` struct size | 64 bytes, one cache line, `static_assert`-enforced |
| Fiber stacks | 64 KB standard / 512 KB heavy, contiguous arena, guard-paged |
| Steal protocol | single-item Chase-Lev CAS |

Medians of five runs on the default affinity policy. Run-to-run spread was 2.4% on latency, 9% on
the DAG and 19% on fork-join, so treat the last digit as noise.

**Those are the DEFAULT numbers, where idle workers park.** `SetIdlePolicy(IdlePolicy::NoSleep)`
keeps them searching instead, and the wake path turns out to be the largest single cost in the
scheduler:

| | `Sleep` (default) | `NoSleep` | |
|---|---|---|---|
| Task enqueue → dequeue latency | 4.68 µs | **1.15 µs** | 4.1x |
| 6-node frame DAG | 22.50 µs | **7.76 µs** | 2.9x |
| 1M recursive fork-join | 0.22 ms | **0.07 ms** | 3.1x |
| Single-producer submission | 1.21 M/s | **6.30 M/s** | 5.2x |
| 16 heavy tasks from an idle pool | 11.6x of 16 | 12.6x of 16 | flat |

That last row is the control, and it is the reason to believe the others. Its shortfall was
predicted before measuring to be frequency scaling rather than wake latency -- 16 heavy tasks at
once settle toward base clock on a chip at Intel spec power limits, where one task alone boosts. It
stayed flat while everything else moved 3-5x.

The default stays `Sleep` and should, for a library: spinning workers are a battery and thermal
problem on Android, they starve whatever else the host process runs, and they make the
oversubscription policy incoherent. `NoSleep` is for an application that owns the machine, which a
fullscreen game does. There is deliberately no middle setting -- a spin-then-park mode was built,
measured worse than both extremes, and removed; the reasoning is in [CHANGELOG.md](CHANGELOG.md).

Run `SchedulerBench nosleep` to reproduce.

**This machine runs Intel's specified power limits with unlimited turbo disabled**, so it boosts
briefly and then settles near base clock under sustained load. That is the part behaving as Intel
specifies it, not a handicap, but it is worth stating because most enthusiast boards ship with PL1
and PL2 effectively unlimited and most published numbers are measured that way. A machine running
board defaults will beat every figure above.

Numbers that are conservative for the reader are the right kind to be wrong, and this is also why
the third-party results matter more than mine: some have come back faster, and the spread across
machines says more than any single row.

`SchedulerBench` prints the version it was built from. That exists because results get pasted into
issues while the suite changes underneath them, and a number nobody can attribute to a build is not
data. If a pasted run has no version line at all, it predates 1.0.1 and should be re-run.

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
the pool holds 3.5 to 5 M/s across the whole range.

If you must submit in bulk from one thread, `PushBatch` is the answer and it is not a small
difference: **around 12 M/s, and flat whether the pool has 8 workers or 31.** It links a run of
tasks locally and hands the chain to one worker with a single wake, instead of paying a worker
selection, a queue push and a condition-variable signal per task.

The cost is entirely in submission, not in getting the work done. Timing the two halves separately,
the pool has already finished by the time the loop stops pushing: drain-after-submit is 0.00 ms at
every pool size from 8 to 31. So this is not the pool struggling to keep up, it is one thread's
`Push` getting more expensive as the pool grows, and external submission round-robins across every
worker's inbox, so the producer's working set and its coherence traffic grow with the worker count.

The deques are also Chase-Lev, which is the structural half: their owner pushes and pops locally
with no atomic in the common case, and only thieves pay for the CAS. That asymmetry assumes the
producer is a worker. A task spawning a task gets that fast path; something pushing from outside the
pool goes through the shared inbox instead.

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

On **Android, placement does not work at all**, and that is the platform's decision rather than a
gap here. Its cgroups own thread placement, so the affinity calls either fail for an unprivileged
app or succeed and are immediately overridden. `Hard` and `Ideal` are effectively `None` there, and
the topology-aware steal ordering they enable is correspondingly approximate. Android is supported
for correctness, not for placement, and it is the one target where you should read nothing into
timings: thermal throttling is unconstrained and the cores are heterogeneous, so a benchmark run
on a phone describes the phone. It is also verified by hand on Termux rather than in CI, which
covers Linux AArch64 on glibc but not bionic.

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

### Measured against enkiTS

Same machine, same harness, same worker count, both libraries expressed the way their authors
intended. i9-13900K at Intel spec power limits (see the caveat under [Measured](#measured)), 31
workers, Release, 1.3.0. `--` is not measured yet.

**Every column below was measured with only that scheduler running.** The harness takes `--only=jlib`
/ `--only=enki` and starts nothing else, so no library's threads are alive while another is timed.

| | this (Sleep) | this (NoSleep) | enkiTS | Taskflow | marl |
|---|---|---|---|---|---|
| Round-trip submit→run→wait | 4.6 µs | 0.97 µs | 21.7 µs | 1.30 µs | **0.88 µs** |
| Independent tasks, per task | 74 ns | **69 ns** | 21.8 µs | 310 ns | 290 ns |
| Range work, per item | 36 ns | 24 ns | **15 ns** | -- | -- |
| Bulk parallel-for, 20k items | 0.39 ms | **0.29 ms** | 0.33 ms | 0.49 ms | -- |
| 25% of tasks blocked 600 µs | **8.2 ms** | 10.1 ms | 15.4 ms | -- | -- |

Blank cells are not measured yet, not zero. Versions: enkiTS at `main`, Taskflow 4.1.0, marl at `main`
(**archived**, last commit 2026-04-27 — its column calibrates the fiber path, it is not a
recommendation).

**The latency row is an idle-policy axis, not an architecture one.** Taskflow and marl both keep
their workers searching before parking — Taskflow tries ~64 steals then yields 150 more times before
sleeping — so their defaults sit where `NoSleep` sits, and all three land within a few hundred
nanoseconds of each other. enkiTS parks promptly, like the default here, and is far slower again
because it suspends on a *shared* completion semaphore. Comparing anyone's default to `Sleep` here
would be measuring configuration and calling it design.

Independent-task throughput is the row where the architecture actually shows: 69-74 ns against
290-310 ns for the two fiber/graph libraries, and enkiTS is not really in this race at all because
submitting N single-item task sets is the usage it tells you to avoid.

`Sleep` is the default and the mobile/embedded configuration; `NoSleep` holds every worker core and
is for an application that owns the machine. They are close to two different products, which is why
both are listed rather than only the flattering one.

Note that `NoSleep` is not uniformly better: it **loses** on the blocking row (10.1 ms against
8.2 ms). Parked fibers have nothing for the spinning workers to do there, so the spin is pure waste.
It wins where dispatch latency dominates and costs you where it does not.

**Isolation is not optional here, and it took a wrong result to learn that.** An earlier version ran
both libraries in one process. Under `Sleep` that is harmless, because the pool not being
benchmarked is parked -- enkiTS measured alone (15.4 ns/item, 21.7 µs, 0.334 ms) matches what it
measured beside a sleeping JLib (15.3, 21.4, 0.331). Under `NoSleep` it is not harmless at all:
JLib's 31 workers spin through enkiTS's benchmarks too, 63 threads on 32 CPUs, and enkiTS's own
numbers moved ~40% -- ranged per-item 15.3 → 8.7 ns, latency 21.4 → 18.2 µs -- purely because a JLib
setting changed. A library's numbers moving when you reconfigure a different library is the signal
that a harness is measuring the machine rather than the code.

**The first two rows are the same quantity measured on each library's own terms, and they disagree
by 200x in opposite directions -- which is the point.** enkiTS's scheduled entity is a 16-byte
`SubTaskSet` in a contiguous fixed ring: no allocation, no pointer chase, and the caller owns the
`ITaskSet` and keeps it alive. That is why its per-ITEM cost is so low, and why its per-TASK cost is
so high when you actually need n independent task objects -- every `AddTaskSetToPipe` wakes the
waiting pool so one thread can claim one task. A `Task` here is 64 bytes carrying its own callable,
so it costs more per entity and can be fire-and-forget with captures. The size difference IS the API
difference; neither is a missed optimisation.

Read the rows accordingly: use `PushArray`/`ParallelFor` for range work and compare against row 3,
use individual tasks for a heterogeneous frame graph and compare against row 2.

Blocking is the row the fiber hybrid exists for, and it is a crossover rather than a win: even at
25% blocked it needs blocks longer than ~50 µs before parking beats simply holding the thread, peaks
at **1.75x around 600 µs**, and tapers again by 2 ms. Below that the fiber costs more than it saves.

Reproduce with the opt-in harness -- it is not built by default and enkiTS is not vendored:

```
git clone https://github.com/dougbinks/enkiTS
cmake -B build-compare -DCMAKE_BUILD_TYPE=Release -DJLIBSCHED_ENKITS_DIR=/path/to/enkiTS
cmake --build build-compare --config Release --target CompareEnkiTS
./build-compare/bin/Release/CompareEnkiTS --only=jlib
./build-compare/bin/Release/CompareEnkiTS --only=jlib nosleep
./build-compare/bin/Release/CompareEnkiTS --only=enki
```

Three runs, one scheduler each. With no `--only` it measures both in one process, which is valid only
while every library present parks when idle -- so `nosleep` forces `--only=jlib` and says so, rather
than quietly emitting a confounded column.

`bench/compare/compare_enkits.cpp` records the predictions made before measuring and, next to each
number, the harness faults that corrupted it first. The initial draft reported enkiTS as 15x slower
and every one of those faults was the harness, not enkiTS -- worth reading before trusting any row
here, and before writing a comparison of your own.

## Versioning

1.0.0. The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`; those follow semver and do
not break without a 2.0. Every header is installed because the supported ones need them to compile,
but the rest are implementation detail and may change in any release. If you need something only
reachable through one of those, that is a missing feature -- open an issue rather than depend on it.

[CHANGELOG.md](CHANGELOG.md) has the release history and the negative results.

## License

BSD 3-Clause. Use it, fork it, ship it.
