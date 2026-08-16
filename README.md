# JLib::Scheduler

A fiber-based, work-stealing task scheduler for real-time engines. Hand-written context switching,
lock-free Chase-Lev deques, a slab-allocated task system, frame DAGs with logic gates, and
hybrid-core aware placement.

Windows x64 (MSVC) · Linux x86-64 · Linux/Android AArch64 · macOS Apple Silicon · C++17 · BSD

[![CI](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml)

I built this scheduler to solve the problem of scheduling for my custom 2d/3d engine  -- it was built to be the backbone of multithreaded simulation engines.
I needed tasks that could wait on a gpu fence without parking a worker thread, enkiTS and taskflow cannot do this, marl can but was archived in April.

**Maturity.** Test it in your own project before you depend on it. Run your workload, run your
tests, and if it holds up, use it. What it should not be is dropped into a commercial project
untested on the strength of a benchmark table -- including this one's.

Bugs do get fixed: four defects were found and released inside a day in 1.3.x, two of which could
hang a pool. But that was a day off. This is one person with a full-time job, so read it as evidence
that reports get acted on -- not as a response time. During a work week the same four fixes would
have taken weeks, and a report may sit for a while before anyone looks at it. Nothing here is
staffed.

---
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

## Measured

This was tested on my machine, third party tests have come back and some are faster than mine depending on hardware and platform.
Needs and welcomes more testing for research!

i9-13900K at Intel spec power limits, Release, 1.3.0. Medians of three runs on the default affinity
policy. **The two columns are one library under its two idle policies**, not two products -- `Sleep`
parks idle workers and is the default, `NoSleep` keeps them searching.

| | `Sleep` (default) | `NoSleep` | |
|---|---|---|---|
| Task enqueue → dequeue latency | 4.67 µs | **1.21 µs** | 3.9x |
| 6-node frame DAG (build, validate, execute) | 22.5 µs | **7.8 µs** | 2.9x |
| 1M-element recursive fork-join (10k leaves) | 0.23 ms | **0.06 ms** | 3.8x |
| Single-producer submission | 0.99 M/s | **6.4 M/s** | 6.5x |
| Bulk submission via `PushBatch` | **12.2 M/s** | 10.8 M/s | 0.9x |
| Bulk submission, 4 producers | 9.8 M/s | **12.7 M/s** | 1.3x |
| Per-item cost via `PushArray` (chunk 128) | 1.0 ns | 1.0 ns | -- |
| 16 heavy tasks from an idle pool | 11.2x of 16 | 12.6x of 16 | flat |

Run-to-run spread is a few percent on latency and the DAG, wider on single-producer submission --
treat the last digit as noise. Reproduce either column with `SchedulerBench`, or both side by side
with `SchedulerBench both`.

**The wake path turned out to be the largest single cost in the scheduler**, and nothing here had
measured it until 1.3.0. That is what the first four rows are: whether a worker had to be woken by
the kernel. `NoSleep` is not uniformly better, which is why both columns are shown -- it costs you
on `PushBatch`, and on the blocking workload further down.

That last row is the control, and it is the reason to believe the others. Its shortfall was
predicted before measuring to be frequency scaling rather than wake latency -- 16 heavy tasks at
once settle toward base clock on a chip at Intel spec power limits, where one task alone boosts. It
stayed flat while everything else moved 3-6x.

The default stays `Sleep` and should, for a library: spinning workers are a battery and thermal
problem on Android, they starve whatever else the host process runs, and they make the
oversubscription policy incoherent. `NoSleep` is for an application that owns the machine, which a
fullscreen game does. There is deliberately no middle setting -- a spin-then-park mode was built,
measured worse than both extremes, and removed; the reasoning is in [CHANGELOG.md](CHANGELOG.md).

Structural properties, which do not vary by policy:

| | |
|---|---|
| `Task` struct size | 64 bytes, one cache line, `static_assert`-enforced |
| Fiber stacks | 64 KB standard / 512 KB heavy, contiguous arena, guard-paged |
| Steal protocol | single-item Chase-Lev CAS |

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


### Measured against enkiTS, TaskFlow and marl

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
| 25% of tasks blocked 600 µs | **7.4 ms** | 10.1 ms | 15.4 ms | -- | 8.8 ms |

Blank cells are not measured yet, not zero. Versions: enkiTS at `main`, Taskflow 4.1.0, marl at `main`
(**archived**, last commit 2026-04-27 — its column calibrates the fiber path, it is not a
recommendation).

**The latency row is an idle-policy axis, not an architecture one.** Taskflow and marl both keep
their workers searching before parking — Taskflow tries ~64 steals then yields 150 more times before
sleeping — so their defaults sit where `NoSleep` sits, and all three land within a few hundred
nanoseconds of each other. enkiTS parks promptly, like the default here, and is far slower again
because it suspends on a *shared* completion semaphore. Comparing anyone's default to `Sleep` here
would be measuring configuration and calling it design.


**The blocking row is a crossover, not a verdict, and marl is the only honest peer for it.** enkiTS
and Taskflow have no fibers, so a blocked task there holds a thread and the comparison is
architectural rather than close. marl suspends a fiber exactly as this does, so it measures two
implementations of the same idea. Swept by how long each task blocks (10 batches of 256, 25%
blocking, ms for all 2560 tasks, each library measured alone):

| block for | this (`Sleep`) | marl |
|---|---|---|
| 50 µs | 8.0 | **5.0** |
| 150 µs | 7.4 | **4.9** |
| 300 µs | 6.6 | **5.6** |
| 600 µs | **7.4** | 8.8 |
| 2000 µs | **22.2** | 22.6 |

marl leads below roughly 400 µs and this leads above it. Two things worth knowing rather than
smoothing over. Its `Event` carries sticky signalled state and waits on a predicate, so an
already-satisfied wait costs it nothing, while `JLib::Event` is a stateless rendezvous and always
pays a suspend/resume round trip -- that is a capability difference, not a slower fiber. And the
fiber machinery itself is not where this row's cost lives: measured separately, attaching a fiber is
free within noise and a full suspend/resume round trip is ~166 ns, against a ~1.3 µs baseline cost
to submit a task at all. Three earlier explanations for this row -- the event registry lock, the
serial resume path, and fiber acquisition -- were each measured and each wrong.

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

### iOS and Windows on ARM64

**Untested, not supported.** iOS, tvOS, watchOS and visionOS are arm64 Darwin, so they use the same
context switch and `src/darwin/` layer that macOS arm64 uses and that CI verifies -- but nobody has
run the result, and I have no Apple hardware to do it. Opt in with:

```
cmake -B build-ios -G Xcode -DCMAKE_SYSTEM_NAME=iOS -DCMAKE_OSX_ARCHITECTURES=arm64 \
      -DJLIBSCHED_ALLOW_UNVERIFIED_PLATFORM=ON
```

Without that flag it refuses and names the flag. One caveat worth stating: a pool of persistent
worker threads does not fit the iOS app lifecycle -- backgrounding freezes threads mid-execution, so
treat it as foreground-only unless you wire `Pause()`/`Resume()` up yourself.

**Windows on ARM64** is refused outright. The calling convention matches AAPCS64 on everything a
context switch touches, so what is missing is only an `armasm64` translation, an MSBuild ARM64
configuration, and somebody with the hardware -- about a day of mechanical work nobody has asked for.

Open an issue either way. A report that it works is as useful as one that it doesn't.

## Using it
Synchronization primitives: JLib provides fiber-aware SchedulerMutex, SchedulerSemaphore, SchedulerConditionVariable, Event, DirectEvent, and WaitGroup primitives.
```cpp
#include <TaskScheduler.h>

int main() {
    JLib::TaskScheduler::Init();               // auto pool size (hw-1)
    auto& sched = JLib::TaskScheduler::Instance();

    // Fire and forget. noFiber is the default: runs inline on a worker, no fiber, no switch.
    sched.Push([] { HeavyMath(); });

    // Scatter-gather: N independent tasks against one WaitGroup, then wait for all of them.
    JLib::WaitGroup wg;
    for (int i = 0; i < 8; ++i) {
        auto* t = sched.CreateTask([i] { Chunk(i); });
        t->waitGroup = &wg;
        wg.n.fetch_add(1, std::memory_order_release);
        sched.Push(t);
    }
    sched.WaitFor(wg);

    // Data-parallel loop. Decides serial vs parallel by timing a prefix, not by element count.
    sched.ParallelFor(0, 1000000, 4096, [](int a, int b) {
        for (int i = a; i < b; ++i) out[i] = std::sqrt((float)i);
    });

    // Same shape, fire-and-forget: submits ceil(n/chunk) tasks rather than n, and returns
    // as soon as they are queued. ~1 ns per item at chunk 128.
    JLib::WaitGroup arr;
    sched.PushArray(0, 1000000, 4096, [](size_t i) { out[i] = std::sqrt((float)i); }, &arr);
    sched.WaitFor(arr);

    sched.Join();
}
```

### Submit from inside the pool where you can

Pushing 200,000 tasks from one thread runs at **1.0 M/s**; submitting the same work from four tasks
already on the pool runs at **9.8 M/s** -- and single-producer submission gets *worse* as workers are
added. The cost is all in submission, not in finishing: drain-after-submit is 0.00 ms at every pool
size, so the pool is done before the loop stops pushing.

The reason is not a privileged path for workers -- `Push` routes every task through `PickNextWorker`
into some worker's inbox regardless of who called it, so a task spawning a task pays the same
placement, inbox push and notify an external thread does. What changes is that four producers do
that work concurrently while one producer does it serially, and single-producer cost grows with the
pool because placement round-robins across every worker's inbox, so the producer's coherence traffic
tracks the worker count.

The Chase-Lev asymmetry does exist, but it is downstream of this: a worker drains its own inbox into
its deque and pops from it with no atomic in the common case, and only thieves pay the CAS. That is
what makes the work cheap to *consume*, not cheap to submit.

One submitter is still a normal shape -- a main thread queueing a frame's work is fine. It just means
that if adding workers stops helping, the loop is the thing to look at. Two ways to fix it in bulk:

- **`PushBatch`** -- 12.2 M/s, flat across pool sizes. Links the tasks and hands them over in
  segments, instead of paying a worker selection, a queue push and a condition-variable signal each.
- **`PushArray`** -- 1.0 ns per item, because it submits ceil(n/chunk) tasks rather than n.

`SchedulerBench` reports the two cases as `throughput/1p` and `throughput/mp`.

### Rules worth knowing

A task that will call `WaitFor` must be created with **`noFiber = false`**. It defaults to true, and
a task with no fiber under it cannot suspend -- it fail-fasts with no message.

Tasks live in 256-byte slab slots, so a lambda capturing more than about **192 bytes** fails a
`static_assert`. Capture pointers, not payloads.

**How many tasks may be BLOCKED at once is capped at 64 per core.** A suspended task holds its fiber
for as long as it stays suspended, so concurrent suspensions are bounded by the fiber pool. Past
that, workers re-queue the tasks they cannot start and retry: it still makes progress, but it looks
like a stall rather than an error, so one warning is printed and then the pool grinds. Tasks that
never suspend are unaffected, and so is the total number in flight -- it is specifically the number
blocked *simultaneously*. Raise `standardFiberCount` in `StartPool` if you need more, remembering
each standard fiber carries a 64 KB stack, so the cap is a memory decision rather than an arbitrary
one.

### Placement is a hint, and on some platforms it is nothing

`CorePref::P` / `CorePref::E` are **Windows-only**. Elsewhere the scheduler cannot tell the core
classes apart, so the request is silently ignored rather than rejected -- leave tasks at `Default`
and do not build a design that assumes placement held.

**Android ignores placement entirely**, by the platform's decision rather than a gap here: its
cgroups own thread placement, so the affinity calls either fail for an unprivileged app or succeed
and are immediately overridden. `Hard` and `Ideal` are effectively `None`, and topology-aware
stealing is correspondingly approximate. Read nothing into timings there -- unconstrained thermal
throttling and heterogeneous cores mean a phone benchmark describes the phone. Verified by hand on
Termux rather than in CI, which covers Linux AArch64 on glibc but not bionic.

**Above 64 logical CPUs**, binding goes through `SetThreadGroupAffinity`/`SetThreadIdealProcessorEx`,
which take the processor group as data -- `SetThreadAffinityMask` takes it from the calling thread
and so cannot name a CPU in a second group. Handled up to 256 CPUs, but **untested above 64**; if you
have such a machine, that is the most useful thing you could report.

[DESIGN.md](DESIGN.md) has the rest -- the execution model, the integration contracts, and the
decisions that were tried and removed.

## Versioning

1.3.0. The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`; those follow semver and do
not break without a 2.0. Every header is installed because the supported ones need them to compile,
but the rest are implementation detail and may change in any release. If you need something only
reachable through one of those, that is a missing feature -- open an issue rather than depend on it.

[CHANGELOG.md](CHANGELOG.md) has the release history and the negative results.

## License

BSD 3-Clause. Use it, fork it, ship it.
