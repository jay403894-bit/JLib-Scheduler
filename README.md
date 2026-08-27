# JLib::Scheduler
[![CI](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml)

A hybrid runtime whose **core is C++17**, with **C++20 coroutines as an optional header**. It gives
you:

- **Cilk-style work-stealing** for fast parallel loops (`ParallelFor`, `PushArray`)
- **No cost model in the range APIs** -- `ParallelFor` has no probe and no calibrated constant; steals decide how work is divided
- **TaskFlow-style dependency graphs** (`TaskDAG`) with AND/OR gates
- **Middleware-safe threads by default** -- no TLS surprises
- **Three execution modes on one pool** -- plain tasks, fibers, and C++20 coroutines, sharing one
  lock and one semaphore
- **Fiber-based blocking only when you actually suspend**, so you never pay fiber overhead unless you need it

**The C++17 core is a feature, not a floor we have not raised yet.** Console SDKs, older MSVC and
long-lived engine codebases are still on C++17, and the whole scheduler -- deques, stealing, fibers,
DAGs, the verified concurrency -- builds and runs there. Coroutines live in one opt-in header
(`Coroutine.h`) that nothing else includes, so including it is the only thing that requires C++20.
The boundary is enforced by the build rather than by discipline: the library target is compiled as
C++17, so anything that leaked out of that header would fail to compile.

**Practically: include the headers and build your project as C++20 for the full runtime (coroutines
plus the I/O reactor).** The job system itself -- threads, fibers, DAGs, adaptive K-hot -- does not
require C++20, and a C++17 translation unit can use that core in full. Coroutine *tasks* are
rejected unless the TU is C++20. That last check is a runtime `assert` rather than a compile error
because `TaskType::Coroutine` is an enumerator in `Task.h`, which is C++17: a C++17 TU can always
NAME it, so the compiler has nothing to object to. It cannot legally MAKE one, because `Spawn()`
lives in `Coroutine.h` and that header `#error`s below C++20 -- but passing the enumerator straight
to `CreateTask()` used to slip through both, and produced no crash to trace back. The type is read
as an ownership signal ("the frame frees itself, do not free the Task"), so a counterfeit coroutine
ran, leaked, and left its WaitGroup unsignalled. The assert now catches it at the call site.


Built for real-time engines that mix compute-heavy loops, dependency-aware pipelines, and I/O-bound
middleware in the same frame.

Under the hood: hand-written context switching, lock-free Chase-Lev deques, a slab-allocated task
system, frame DAGs with logic gates, and hybrid-core aware placement.

**Maturity.** Test it in your own project before you depend on it. Run your workload, run your
tests, and if it holds up, use it. What it should not be is dropped into a commercial project
untested on the strength of a benchmark table -- including this one's.

Bugs do get fixed: four defects were found and released inside a day in 1.3.x, two of which could
hang a pool. But that was a day off. This is one person with a full-time job, so read it as evidence
that reports get acted on -- not as a response time. During a work week the same four fixes would
have taken weeks, and a report may sit for a while before anyone looks at it. Nothing here is
staffed.

Windows x64 & ARM64 (MSVC) · Linux x86-64 · Linux/Android AArch64 · macOS Apple Silicon · C++17 (C++20 for optional coroutines) · BSD


I built this scheduler to solve the problem of scheduling for my custom 2d/3d engine  -- it was built to be the backbone of multithreaded simulation engines.
I needed tasks that could wait on a gpu fence without parking a worker thread, enkiTS and taskflow cannot do this, marl can but was archived in April.



---
## How it compares

| | enkiTS | Taskflow | marl | this |
|---|---|---|---|---|
| Work-stealing | yes | yes | yes | yes |
| Suspend/resume inside a task | no | no | yes | yes |
| Dependency DAG with AND/OR gates | partial | yes | no | yes |
| Cache/SMT topology-aware stealing | no | no | no | yes |
| Model-checked concurrency (not just tested) | no | no | no | yes |
| Maintained | yes | yes | archived Apr 2026 | yes |

marl and FiberTaskingLib run every task on a fiber. Here fibers are opt-in, so middleware written
for an ordinary thread pool works unchanged -- Jolt Physics runs through a `JPH::JobSystem` adapter
and never learns fibers exist. [Why that matters](DESIGN.md#the-hybrid-is-a-correctness-boundary-not-a-performance-dial).

**"Model-checked" checked, not assumed**: enkiTS has no dedicated test suite at all (two ad hoc files
sitting in its `example/` directory); Taskflow's 39-file `unittests/` suite is the most extensive of
the three by far; marl has a real 14-file suite matched by benchmarks for most of them. None of the
three have anything resembling exhaustive concurrency verification -- it's unit tests only, at every
point on that spectrum. See [Model checked](#model-checked) for what GenMC and TLA+/TLC actually
cover here and, as importantly, what they don't.

## Measured

This was tested on my machine, third party tests have come back and some are faster than mine depending on hardware and platform.
Needs and welcomes more testing for research!

i9-13900K at Intel spec power limits, Release, **1.4.0**. Medians of five runs on the default
affinity policy, with the observed range in brackets. **The two columns are one library under its
two idle policies**, not two products -- `Sleep` parks idle workers and is the default, `NoSleep`
keeps them searching.

| | `Sleep` (default) | `NoSleep` | |
|---|---|---|---|
| Task enqueue → dequeue latency | 4.6 µs <br><sub>[4.28-4.72]</sub> | **1.0 µs** <br><sub>[0.94-2.8]</sub> | 4.6x |
| 6-node frame DAG (build, validate, execute) | 21.1 µs <br><sub>[20.2-22.1]</sub> | **7.4 µs** <br><sub>[7.2-8.4]</sub> | 2.9x |
| 1M-element recursive fork-join (10k leaves) | 0.25 ms <br><sub>[0.17-0.29]</sub> | **0.06 ms** <br><sub>[0.06-0.13]</sub> | 4.2x |
| Single-producer submission | 1.2 M/s <br><sub>[0.90-1.48]</sub> | **7.0 M/s** <br><sub>[5.9-7.2]</sub> | 5.8x |
| Bulk submission via `PushBatch` | **14.7 M/s** <br><sub>[14.2-18.2]</sub> | 14.3 M/s <br><sub>[13.1-16.4]</sub> | 1.0x |
| Bulk submission, 4 producers | 2.9 M/s <br><sub>[2.3-3.9]</sub> | **12.5 M/s** <br><sub>[12.1-13.0]</sub> | 4.3x |
| Per-item cost via `PushArray` (chunk 128) | 0.55 ns | 0.59 ns | -- |
| 16 heavy tasks from an idle pool | 10.8x of 16 | 12.6x of 16 | flat |

Reproduce either column with `SchedulerBench`, or both side by side with `SchedulerBench both`.
The brackets matter: the last digit is not meaningful on several of these rows -- bulk submission
and fork-join move ~20% run to run on an otherwise idle machine, and the `NoSleep` latency row is
bimodal -- so read the order of magnitude and the ratio, not the number.

**A quiet machine means quiet.** Two rounds of these numbers were thrown away because a WSL2 VM
that had been shut down seconds earlier was still tearing down in the background, which pulled the
whole `NoSleep` column down by up to 40%. If you reproduce this, give the machine real idle time
first and take the run twice.

**Rows that moved against the 1.3.0 version of this table.** `PushBatch` went 12.2 → 14.7 M/s and
`PushArray` 1.0 → 0.55 ns/item; both follow from 1.4 cutting the task allocator's round trip from
9.3 ns to 2.1 ns. Everything else reproduces 1.3.0 within noise except one row: 4-producer
submission under `Sleep` reads 2.9 M/s where the 1.3.0 table said 9.8.

**That was assumed to be a regression. It is not -- the row is BISTABLE, and neither number is
wrong.** v1.3.0 was checked out, rebuilt and measured beside 1.4.0: it reads 2.5-4.4 M/s against
1.4.0's 3.0-3.7, the same distribution, so nothing regressed between them. Sweeping pool size then
showed what the row actually does:

| pool | 4 | 8 | 16 | 18 | 20 | 22 | 24 | 31 |
|---|---|---|---|---|---|---|---|---|
| 4-producer M/s | 11.08 | 11.39 | 10.13 | 10.93 | 10.50 | **2.37** | 5.36 | 3.60 |

A cliff at ~21 workers. Below it the four producers keep the pool saturated, so workers never park
and a `Push` costs the ~1 ns of an awake-worker notify. Above it the consumers drain faster than the
producers submit, workers run dry and park, and every `Push` buys a kernel wake instead. Those are
two regimes, ~10-11 M/s and ~3 M/s, and this 31-worker machine sits just past the edge -- so the row
lands in either depending on how the run settles.

**This is the same failure mode the single-producer row is already documented for**, one crossover
to the right: four producers feed more consumers than one, so the cliff moves from ~14 workers to
~21. `bench.cpp` claimed the multi-producer case was flat across that sweep and served as the
control for it. That claim was wrong and is now corrected -- fork-join and the frame DAG are the
flat ones.

So the 9.8 was a real reading from the saturated regime, and `best-of-5` on a bistable metric
publishes whichever regime got lucky -- which is also why the row was recorded as 8.4 M/s and then
9.8 M/s sixteen minutes later on identical code. **Read this row as a submission-rate measurement
with a crossover, not a capacity number, and expect either regime.** Published as measured rather than quietly dropped.

**Measure on your own hardware before relying on any of this.** These numbers come from one desktop
with a large L3 and no competing load; a different cache hierarchy or a machine the application does
not own will move them, in some cases below 1.0x.

**The wake path turned out to be the largest single cost in the scheduler**, and nothing here had
measured it until 1.3.0. That is what the first four rows are: whether a worker had to be woken by
the kernel. `NoSleep` is not uniformly better, which is why both columns are shown -- it ties on
`PushBatch` (where submission, not wake-up, is the cost) and loses on the blocking workload further
down.

That last row is the control, and it is the reason to believe the others. Its shortfall was
predicted before measuring to be frequency scaling rather than wake latency -- 16 heavy tasks at
once settle toward base clock on a chip at Intel spec power limits, where one task alone boosts. It
stayed flat while everything else moved 2.9-5.8x.

The default stays `Sleep`. Spinning workers are a battery and thermal problem on Android, they starve
whatever else the host process runs, and they make the oversubscription policy incoherent.

**A fullscreen game is NOT the case for `NoSleep`,** despite the table above -- an earlier version of
this README said it was, and measurement says otherwise. The benchmarks flatter `NoSleep` because in
a scheduler benchmark the pool *is* the workload: there is no render or audio thread for the spinning
to tax, so only the wake saving shows up. Measured with an idle pool against a memory-bound main
thread, `NoSleep` costs ~3.5% to every other thread in the process; measured inside a real 2D game it
cost **23%**. `NoSleep` is for batch and offline work where the task graph is the entire program, or
for pipelines whose idle gaps are under ~100 µs.

There is deliberately no middle setting -- a spin-then-park mode was built, measured worse than both
extremes, and removed; the reasoning is in [CHANGELOG.md](CHANGELOG.md).

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
workers, Release. **Our two columns are 1.4.0; the other three were last measured at 1.3.0** and are
carried over unchanged -- see the note under the table for why that is still a fair comparison.
`--` is not measured yet.

**Every column below was measured with only that scheduler running.** The harness takes `--only=jlib`
/ `--only=enki` and starts nothing else, so no library's threads are alive while another is timed.

| | this (Sleep) | this (NoSleep) | enkiTS | Taskflow | marl |
|---|---|---|---|---|---|
| Round-trip submit→run→wait | 4.3 µs | 1.70 µs | 21.7 µs | 1.30 µs | **0.88 µs** |
| Independent tasks, per task | 67 ns | **65 ns** | 21.8 µs | 310 ns | 290 ns |
| Range work, per item | 38 ns | 25 ns | **15 ns** | -- | -- |
| Bulk parallel-for, 20k items | 0.36 ms | 0.18 ms | 0.373 ms | 0.49 ms | -- |
| 25% of tasks blocked 600 µs | **7.2 ms** | 10.2 ms | 15.4 ms | -- | 8.8 ms |

**Our two columns were re-measured at 1.4.0; the other three were not.** enkiTS, Taskflow and marl
have not changed and their cells are carried over -- but they were taken on the same machine in the
same harness, so the comparison still holds. The `NoSleep` round-trip cell is the one to distrust:
it read 0.97 µs at 1.3.0 and 1.70 µs here with an **83% spread across repeats**, which is the
harness being noisy on that row rather than a real move.

**The bulk row was re-measured after `ParallelFor` moved to recursive splitting**, because the
earlier figure was taken against the shared-cursor path that was mechanism-matched to enkiTS. It did
not move: **0.355 ms against enkiTS 0.373 ms**, which is a tie either way — inside both harnesses'
spreads (8% ours, 9% theirs). Read it as neither library having an edge on a uniform bulk range, not
as a win. See [Parallel loops](#parallel-loops).

**The 4.3 µs Sleep round-trip is specifically the cost of waking a FULLY PARKED worker, not a fixed
floor on submission.** `BenchLatency` round-robins across every worker, so any given one only gets
touched roughly once every pool-size iterations -- long enough to fully park every time, paying a
real OS kernel wake on every hit. A breakdown built to answer where that time goes (build with
`-DJLIBSCHED_LATENCY_STATS=ON`) found the OS wake itself is ~85% of it (4.00 µs of 4.89 µs in an
instrumented build), with `Worker()`'s own loop order -- it checks the local deque and runs a full
steal scan before ever looking at the inbox a cold wake was actually for -- contributing a real but
small ~0.30 µs. Pin the same round trip to one worker instead (`Push(1, task)` every time) and it
drops to **0.83 µs**, a 5.9x difference, because that worker is usually still mid-backoff, not yet
parked, when the next task lands. Neither number is "wrong" -- they measure different things: 4.3 µs
is what a genuinely idle pool costs to wake (e.g. a game at a frame boundary), 0.83 µs is what a
worker that's still warm from the last task costs. `JLIBSCHED_LATENCY_STATS` is a permanent,
off-by-default diagnostic (same convention as `JLIBSCHED_STEAL_STATS`) for measuring this split on
your own hardware.

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
| 50 µs | 6.4 | **4.8** |
| 150 µs | 6.1 | **4.8** |
| 300 µs | 6.3 | **5.8** |
| 600 µs | **7.4** | 8.0 |
| 2000 µs | **21.2** | 22.5 |

Re-measured at 3.0.0. marl is the control here and it barely moved (5.0 -> 4.8, 4.9 -> 4.8,
5.6 -> 5.8, 8.8 -> 8.0, 22.6 -> 22.5), which is what says the machine and harness are
comparable across the gap and the movement in our column is signal rather than drift. Do not
read that movement as one change though: these were last taken at 1.4.0 and many releases sit
between.

marl leads below roughly 400 µs and this leads above it. Two things worth knowing rather than
smoothing over. Its `Event` carries sticky signalled state and waits on a predicate, so an
already-satisfied wait costs it nothing, while `JLib::Event` is a stateless rendezvous and always
pays a suspend/resume round trip -- that is a capability difference, not a slower fiber. And the
fiber machinery itself is not where this row's cost lives: measured separately, attaching a fiber is
free within noise and a full suspend/resume round trip is ~92 ns, against a ~1.24 µs baseline cost
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

The lock-free/lock-based hot paths are model checked with [GenMC](https://plv.mpi-sws.org/genmc/),
not only tested. A test runs whichever interleaving the CPU happens to produce; a model checker
enumerates every execution the C11 memory model permits, so for a bounded harness the result is
exhaustive rather than lucky. Models live in `tests/verify/`.

- **Chase-Lev deque** (`deque_model.c`) - one owner, two thieves, 174 executions, no errors. It also
  settled a real question: the published verified Chase-Lev uses `seq_cst` for the steal CAS and this
  uses `acq_rel`. Both check clean, so the weaker ordering is sufficient here and the `seq_cst`
  fences are what carry the ordering.
- **Event waiter stack** (`event_model.c`) - two pushers, one drainer, 24 executions, no errors. No
  waiter lost, none woken twice, no race on the plain `nextWaiter` field.
- **Worker sleep/wake predicate** (`sleepwake_model.c`) - the `workerState`/`hasQueuedWork`/
  `immediate` protocol that lets a push skip the mutex+notify when the target is already awake.
  32 executions clean as shipped. Its own history is the cautionary tale for this whole section: an
  earlier, single-flag version of this same model passed clean, the protocol was built from it, and
  1.2.0 shipped a lost wakeup that hung macOS arm64 in CI about one run in three -- the model was not
  wrong about what it modelled, it just modelled a decision with two fewer inputs than the real one.
- **Fiber wait/resume handshake** (`fiberwait_model.c`) - `SchedulerMutex`/`Semaphore`/`CondVar`'s
  queue-then-mark-parkable ordering. This is the exact handshake that shipped broken in 1.3.4: publish
  to the waiter queue before marking the fiber parkable, and an `Unlock()`/`Signal()` landing in that
  window pops the fiber and discards the wake, since `ResumeQueueless` doesn't treat `RUNNING` as
  resumable -- a mutex locked forever with no holder. `-DOLD_ORDERING` reproduces it as a genuine
  safety violation; the shipped 1.3.5 ordering and `-DSEQ_CST` both check clean, confirming this was
  an ordering defect, not a missing memory barrier.
- **Fiber resume CAS race** (`fiberresume_model.tla`) - a DIFFERENT tool, TLA+/PlusCal checked with
  TLC rather than GenMC: `Fiber::ResumeQueueless()`'s own CAS race (`WANTS_SUSPEND` racing between the
  worker's own CAS to `SUSPENDED` and any other thread's CAS to `SUSPEND_SIGNALED`), independent of
  whichever queue/ordering discipline sits on top of it in the four models above. Read this one as
  design-level evidence, not GenMC-grade proof -- it's a hand-written abstraction checked by explicit-
  state search over a small bound (828 distinct states at `MaxFibers=3`, clean again at 5), not a
  check of the compiled C++ itself. See the file's own header for the full caveat and how to
  reproduce it.

Each GenMC model ships a negative control, which is the part that makes a clean run mean anything.
Build the deque model with `-DNO_POP_FENCE` and it produces a double-claim in under a second: two
threads taking the same task, the use-after-free class. That fence had been called redundant more
than once.

This is not a proof of the whole scheduler. It covers the handful of places memory-ordering and
wakeup-race bugs have actually shipped, at small bounds, not the scheduler as a whole.

## Build

```
cmake -B build -DCMAKE_BUILD_TYPE=Development
cmake --build build -j
```

**On Windows this silently builds Debug, not Development, unless you say otherwise.** CMake's
default generator there is Visual Studio, which is a MULTI-CONFIG generator: all three build types
coexist in the same `build/` tree, chosen at BUILD time via `--config`, not at configure time --
so `-DCMAKE_BUILD_TYPE=...` above does nothing on Windows and is silently accepted, not rejected,
which is what makes this easy to miss. Left unspecified, `cmake --build` falls back to MSBuild's
own default, Debug -- unoptimized, `_ITERATOR_DEBUG_LEVEL=2`, and consequently much slower than the
CMAKE_BUILD_TYPE line looks like it asked for. `SchedulerBench`'s crossover sweeps are already the
slowest sections in Development or Release; run one in an accidental Debug build and it can outlast
the 180-second per-section watchdog before printing anything for the section you're waiting on.

The portable form that works on both generator kinds:

```
cmake --build build --config Release -j
```

`Debug`/`Development`/`Release` all exist in the tree at once on Windows; this just tells the build
step which one, and the binary lands at `build/Release/...` rather than `build/Debug/...`. On a
single-config generator (Ninja, Makefiles -- the default on Linux/macOS) `--config` is accepted and
ignored, since `CMAKE_BUILD_TYPE` already fixed the choice at configure time there.

Or open `Scheduler.sln` in Visual Studio. Three build types: `Debug`, `Release`, and `Development`
(optimized, with symbols and assertions live -- it deliberately does not define `NDEBUG`).

To consume an installed copy, `find_package(JLibScheduler)` and link `JLib::Scheduler`.

Adding the sources to your own build directly: take `src/*.cpp` plus exactly one platform directory.
That is either `src/win32/` with its `ContextSwitch.asm`, or `src/posix/` (or `src/darwin/` on
macOS) plus exactly one architecture subdirectory, `src/posix/x86_64/` or `src/posix/aarch64/`.
On Windows ARM64 it is `src/win32/` **plus** `src/win32/aarch64/ContextSwitch.asm` **instead of**
the flat `ContextSwitch.asm`, and `src/posix/aarch64/FiberInit.cpp` -- that last file is ABI-common
and shared with the POSIX AArch64 build rather than duplicated.
Never two of the same kind -- they define the same symbols, and a static library will not diagnose
that. It silently links whichever one it reaches first.

Every platform below runs the full test suite in CI on every push:

| OS | Arch | Toolchain |
| --- | --- | --- |
| Windows | x86-64 | MSVC |
| Windows | ARM64 | MSVC (`armasm64`) |
| Linux | x86-64 | GCC |
| Linux / Android | AArch64 | GCC / Clang |
| macOS | arm64 | AppleClang |

### iOS

**Untested, not supported.** iOS, tvOS, watchOS and visionOS are arm64 Darwin, so they use the same
context switch and `src/darwin/` layer that macOS arm64 uses and that CI verifies -- but nobody has
run the result, and I have no Apple hardware to do it. Opt in with:

```
cmake -B build-ios -G Xcode -DCMAKE_SYSTEM_NAME=iOS -DCMAKE_OSX_ARCHITECTURES=arm64 \
      -DJLIBSCHED_ALLOW_UNVERIFIED_PLATFORM=ON
```

Without that flag it refuses and names the flag. Two caveats worth stating. A pool of persistent
worker threads does not fit the iOS app lifecycle -- backgrounding freezes threads mid-execution, so
treat it as foreground-only unless you wire `Pause()`/`Resume()` up yourself. And the fiber stack
arena commits 64 fibers per core by default, which is sized for a desktop rather than for jetsam
limits; that is the first knob to turn down if you try it.

Open an issue either way. A report that it works is as useful as one that it doesn't.

## Using it
Synchronization primitives: JLib provides fiber-aware SchedulerMutex, SchedulerSemaphore, SchedulerConditionVariable, Event, DirectEvent, and WaitGroup primitives.
```cpp
#include <TaskScheduler.h>

int main() {
    JLib::TaskScheduler::Init();               // auto pool size (hw-1)
    auto& sched = JLib::TaskScheduler::Instance();

    // Fire and forget. TaskType::Native is the default: runs inline on a worker, no fiber, no switch.
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

Pushing 200,000 tasks from one thread runs at **1.2 M/s**; submitting the same work from four tasks
already on the pool runs at **2.9 M/s** on this 31-worker machine, and **10-11 M/s** on a pool below
~21 workers -- and single-producer submission gets *worse* as workers are added. The cost is all in
submission, not in finishing: drain-after-submit is 0.00 ms at every pool size, so the pool is done
before the loop stops pushing.

The two four-producer figures are not a contradiction and neither is the headline: that row is
**bistable with a crossover at ~21 workers**, and this section used to quote 9.8 M/s, which was a
real reading from the saturated regime published by `best-of-5` on a metric that has two of them.
See [the table notes](#measured) for the sweep. The advice survives the correction -- four producers
beat one in *both* regimes -- but the margin is 2.4x here, not 10x, so treat "submit from inside the
pool" as worth doing rather than as the difference between working and not.

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

- **`PushBatch`** -- 14.7 M/s, flat across pool sizes. Links the tasks and hands them over in
  segments, instead of paying a worker selection, a queue push and a condition-variable signal each.
- **`PushArray`** -- 0.55 ns per item, because it submits ceil(n/chunk) tasks rather than n.

`SchedulerBench` reports the two cases as `throughput/1p` and `throughput/mp`.

### Rules worth knowing

A task that will call `WaitFor` must be created with **`TaskType::Fiber`**. It defaults to
`TaskType::Native`, and a task with no fiber under it cannot suspend -- it fail-fasts with no message.

Tasks live in 256-byte slab slots, so a lambda capturing more than about **192 bytes** fails a
`static_assert`. Capture pointers, not payloads.

**How many tasks may be BLOCKED at once is capped at 64 per core.** A suspended task holds its fiber
for as long as it stays suspended, so concurrent suspensions are bounded by the fiber pool. Past
that, workers re-queue the tasks they cannot start and retry: it still makes progress, but it looks
like a stall rather than an error, so one warning is printed and then the pool grinds. Tasks that
never suspend are unaffected, and so is the total number in flight -- it is specifically the number
blocked *simultaneously*. Call `TaskScheduler::SetFiberBudget(standardPerWorker, heavyPerWorker)`
**before** `Init()` if you need more -- it cannot be changed after, since each fiber registers a
permanent slot with the epoch manager and the stack arena is one fixed allocation made at startup.
Remember each standard fiber carries a 64 KB stack, so the cap is a memory decision rather than an
arbitrary one.

### Parallel loops

`ParallelFor` takes a `(lo, hi)` callable, covers the range in disjoint subranges, and blocks until
every one has run. It **splits recursively and lets steals decide**: it publishes the right half of
the range onto the calling thread's own deque and carries on with the left. If nobody takes a split
it takes it straight back and runs it inline for ~11 ns — no dispatch, no notify. If somebody does,
the pool was hungry and the split was right. It predicts nothing.

It is the range entry point you should reach for. `ParallelRange` was added earlier in 1.4 as the
probe-free alternative and removed in the same release, before either shipped; its shared-cursor
mechanism survives as `RunCursorRange`, which `ParallelFor` falls back to when a second non-worker
thread is already splitting.

**Which of the range APIs to use:**

| | blocks? | how it divides | reach for it when |
| --- | --- | --- | --- |
| `ParallelFor(begin,end,func)` | yes | recursive split, steals decide; grain picked for you | **the default.** You have a range and want it done before the next line |
| `ParallelFor(begin,end,grain,func)` | yes | same, your grain floored to ≥64 leaves/worker | you know the body's cost and want a coarser or finer split than the default |
| `PushArray(begin,end,chunk,fn,wg)` | **no** | fixed `chunk`, one task per chunk, submitted up front | fire-and-forget: the caller has other work, or wants several arrays in flight before waiting on them together |
| `RunCursorRange(begin,end,grain,func)` | yes | one task per worker sharing an atomic cursor | see the caveat below — normally you do not call this |

`PushArray` is the one with a genuinely different contract: it **returns as soon as the work is
queued**, so it is the only one of these you can use to overlap submission with other work on the
calling thread. Pass a `WaitGroup` if you need to wait later, `nullptr` if you never will. It costs
0.55 ns/item because it submits ceil(n/chunk) tasks rather than n.

**`RunCursorRange` is public, and that is currently an accident of layering rather than a
recommendation.** It is the shared core `ParallelFor` falls back to, and nothing selects it by size.
There used to be a claim here that it wins reliably at very large N — 22.6x against the splitter's
18.5x at N=200,000 — and it does not hold up. That table is kept below as a record of what was
claimed, not as guidance: it came from a scratchpad harness that was never committed, so nothing
could re-run it until `SchedulerBench` gained a real splitter-vs-cursor sweep specifically to check
it. Checking it is what broke it.

| heavy body (original, unreproduced) | N=1000 | N=2000 | N=4000 | N=10000 | N=200000 |
| --- | --- | --- | --- | --- | --- |
| recursive splitter | 10.3x | 12.6x | 13.6x | 13.9x | 18.5x |
| shared cursor | 6.6x | 7.8x | 9.4x | 9.3x | 22.6x |

A first re-run on the original desktop looked like it confirmed this — heavy pulling ahead ~1.2x
from around N=20,000. A second run on the exact same machine, same binary, same command, did not:
heavy sat at 0.99–1.01x across the whole range, never clearing the win margin. Runs across two
machines since then found no pattern that holds: a medium body won for the cursor at N=1,000 on
one machine and lost to the splitter at that same N on the other; a heavy body won at N=400,000 on
one and did not on the other. **Neither N, body cost, nor worker count predicts which one wins.**

That is a stronger claim than "the old number was wrong" — it means no replacement threshold
belongs here either, worker-count-relative or otherwise. The original mistake was trusting a table
nobody could re-run; the fix is not a better table, it is not trusting a table at all. If you need
to know which mechanism wins for a specific workload, run `SchedulerBench`'s splitter-vs-cursor
sweep against it yourself — it interleaves both arms with a same-vs-same control and marks any cell
whose control moved more than 5% on its own, so a noisy reading says so instead of publishing a
number.

### How do I pick a grain?

Mostly you don't. **`ParallelFor(begin, end, func)` derives one for you** from the two things known
exactly — the range and the pool size — and never consults the body: `range / (workers * 8)`, the
same rule Cilk's `cilk_for` uses for its default.

Pass a grain only if you have measured. The number that matters is **wall-clock per slice, not
elements**: aim for a few microseconds of work in each. If you have timed the loop serially once —
a two-line experiment in a dev build — then `grain = range * (target_slice_time / total_serial_time)`
gets you there without knowing anything per-element.

The floor protects against guessing too *fine*. Nothing protects against guessing too *coarse*: a
grain larger than `range / workers` leaves workers with nothing to take.

**What none of this can do is decline.** Nothing probe-free can refuse to parallelize a body too
cheap to be worth it — TBB's `simple_partitioner`, Rayon and Cilk all share this. 1.3.x could,
because it probed. It was dropped anyway, and the root of it is that **iteration count is a useless
proxy for execution time**. Everything else follows from trying to recover time from a number that
does not carry it.

Static probing hits four walls, and they are independent — fixing one leaves the rest:

- **Probe overhead.** A timer read is 15–30 ns plus pipeline serialisation, against a ~69 ns task
  overhead. A probe fine-grained enough to see divergence costs more than the decision is worth.
- **Cache perturbation.** The prefix warms L1/L2 while the remaining 99% streams from DRAM, so the
  estimate is biased *low systematically* rather than noisily — always the same direction, so it
  never averages out and more sampling cannot help.
- **Data-dependent work.** In frustum culling, ray tracing and narrow-phase — what this library is
  for — iteration 1 early-outs at 2 ns and iteration 50 does full mesh collision at 4,000 ns. No
  sample predicts divergence.
- **Asymmetric topology.** Measuring on a P-core describes a profile that means nothing once an
  E-core steals the remainder. That is the normal case on the ARM targets, not an edge case.

Only the first of those did *not* apply here: our probe called the clock twice per `ParallelFor`,
outside the loop, so ~30–60 ns amortised over the whole range. That is worth stating because it cuts
the other way — this was the cheapest possible probe, calibrated to within measurement error of the
right constant on the machine it was tuned on, and the other three walls took it anyway.

There was also a fifth failure that is about people rather than hardware: the gate **looked
deterministic while being wrong**. Same input, same answer, every run — so it read as a measurement
rather than a guess, and nothing ever prompted a check. A noisy predictor advertises its own
unreliability. This one did not.

**This is not a novel position.** No mainstream work-stealing runtime gates parallelism on a timed
serial prefix. Cilk-5 let spawn/steal decide; oneTBB's `auto_partitioner` uses a depth budget with
steal feedback; Rayon resets a split budget on observed steals. All of them infer demand from
**steals — something that actually happened** — rather than from a cost model. 1.4 converges with
that rather than inventing against it.

The 20k cheap-body row above is what that costs: 0.08x with a guessed grain, 0.24x with the default.
If you need to know whether a loop should have been parallel at all, `SetParallelForSerial(true)`
answers it in one run without a rebuild.

### You can take garbage collection off the workers

Reclamation is epoch-based, and by default a **worker** performs it: once enough pointers are
retired, whichever worker notices next stops and runs a reclaim pass. That pass scans every epoch
participant, and the participant set scales with the pool -- roughly 2,300 slots on a 31-worker
machine. It is not much total work, but it lands on a thread that was supposed to be running your
frame, at a moment you do not choose.

If your application has a natural idle point, hand it over:

```cpp
JLib::EpochManager::Instance().SetSelfReclaim(false);   // BEFORE StartPool
JLib::TaskScheduler::Init(workerCount);

while (running) {
    RunFrame();
    JLib::EpochManager::Instance().Tick();   // reclaim here, where the pool is idle anyway
}
```

**What that actually buys, measured** -- frame-shaped DAG, 32 nodes per frame, 4,000 frames after
400 warm-up, three interleaved rounds, per-frame microseconds:

| | p50 | p90 | p99 |
| --- | --- | --- | --- |
| default (workers reclaim) | 58.1 / 58.9 / 60.2 | 67.7 / 66.8 / 69.0 | **331 / 331 / 336** |
| `Tick()` on your thread | 60.5 / 58.7 / 58.5 | 69.3 / 67.8 / 66.3 | **125 / 111 / 104** |

**Throughput does not change.** Median and p90 are a wash, and if that is what you care about there
is nothing here for you. What changes is the tail: **p99 improves about 3x**, because the same scan
now happens between frames instead of stalling a worker inside one. It is a frame-time *consistency*
feature.

Two things to know before you flip it. It must be set **before `StartPool`** and never again -- it
is a plain `bool` read by every worker, so changing it on a live pool is a data race and a worker may
have hoisted the read out of its loop anyway. And **if you disable it and then never call `Tick()`,
retired memory grows without bound and nothing warns you.** The default is on precisely because a
library cannot assume its embedder has a loop to tick from.

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

**`CorePref::P`/`::E`'s reliability is quietly coupled to `AffinityPolicy`, not just to whether the
CPU is hybrid.** `isPCore` is a label assigned ONCE per worker at pool startup, from real topology.
Under `Hard` that label stays true for the process's whole life -- a worker never leaves its
assigned core. Under `Ideal`, **the shipped default**, the OS is only given a preference and may
migrate the thread under contention or thermal pressure, so the label can go stale mid-run: a
`P`-preferring task can land on a worker that used to be on a P-core and no longer is. In other
words, the mode where the hint is maximally trustworthy (`Hard`) is the one already measured and
rejected as the default for its own cost (see [Worker binding](DESIGN.md#worker-binding) -- ~45%
worse wake latency, ~2x on the frame DAG). `Ideal` migration is the exception rather than the rule
in practice, which is why it is still treated as "meaningful" rather than as `None`, but this is a
real gap between "hint" and "guarantee," not just boilerplate hedging -- know it before building a
design that assumes a `P` task always lands on a P-core.

### Hot workers: how many, and when none

`SetHotWorkers(K)` reserves K workers that **never park**, and the reactor steers I/O completions at
them. It is **off by default (K = 0)** and it is the difference between async I/O being usable in a
hybrid pool and not.

**The problem it solves.** Compute wants the pool to park when idle -- 31 threads spinning between
frames is unacceptable. I/O wants the pool never to park, because a completion arrives *precisely*
when the pool is idle; that is what awaiting means. Those are mutually exclusive as a global policy,
and the measured cost of choosing wrong is not subtle:

| | I/O dispatch p50 | p99 | cost |
|---|---|---|---|
| `Sleep` (default) | 10.5 us | ~300 us | none |
| `NoSleep` (whole pool) | 2.7 us | ~25 us | every core spins |
| **`SetHotWorkers(1)`** | **1.2 us** | **~7 us** | one core |

Parking is ~300 us of that; the residue between NoSleep and K-hot is contention between 31 spinners.
K-hot makes the idle policy a property of the **worker** rather than the pool, which is what lets one
scheduler serve compute that must not spin and I/O that must not wait.

**Picking K.** Two different rules, and which applies depends on what you are building.

*For an application with deadlines* -- a game, an audio pipeline -- **K is the number of independent
deadline paths that must not block each other**, not a measure of I/O volume. One hot worker serves
its lane strictly in order, so an audio refill in progress delays a network packet that completes
behind it. That coupling is visible at a concurrency of two:

    N=2    K=1: p50 1.30 us      K=2: p50 0.80 us
    N=4    K=1: p50 9.10 us      K=2: p50 2.50 us

So: **K=1** for one deadline path (audio, or netcode). **K=2** when two can collide. Bulk work --
asset streaming, background loads -- does not need its own hot worker and should share: queueing is
free when nothing is waiting on it, and a level load is exactly when no frame deadline exists.

*For a server*, where throughput matters more than a frame budget, K tracks concurrent in-flight
depth -- roughly **one hot worker per 2-4 concurrent operations**, because past that the worker
saturates and latency grows linearly:

    N          1     2     4      8      16      32      64
    K=1 p50 1.20  1.30  9.10  19.20   65.20  144.40  133.10 us   (~59k ops/s ceiling)
    K=2 p50 0.60  0.80  2.50   5.10   13.10   31.20   69.50 us   (~220k)
    K=4 p50 0.60  0.80  1.20   2.70    6.80   13.80   26.20 us   (491k, still climbing)

Note those are 8-byte loopback datagrams that complete instantly, so N in flight means N completions
arriving at once -- a saturating source. Real I/O spends its life in the kernel, so in-flight depth
and completion *rate* decouple: 16 concurrent asset reads do not produce 16 simultaneous
completions. For scale, K=1's ceiling is ~980 completions per frame at 60 fps.

**What it costs.** Each hot worker is removed from general placement -- at least K/N of throughput,
3.2% per worker on a 31-worker pool, and more on an SMT machine where a spinning worker also degrades
its sibling core. On a 16-core machine K=4 is over a quarter of the pool; treat K>=2 as a server
configuration and K=1 as the game one.

**What may go in the lane.** The lane is a *sparse resource*, and that -- not speed -- is what
decides. Its capacity is K, so everything routed there competes for K workers. Route all your work
to it and you have not made anything fast; you have serialised the pool through one or two threads.
That argument holds even if every task there were provably short.

So the lane is for work with a **deadline someone else set**: an I/O completion, an audio buffer
refill, a netcode tick. You may put your own task there -- `Spawn(coro, wg, /*hiPri*/ 1)`, or
`CreateTask(..., hiPri)` -- and it is reasonable to, if the task genuinely needs to run promptly
*and* you have K to spare. But `hiPri` is a claim about the work rather than a request for service:
it says *this is short, and it is worth one of my K slots*. Both halves matter, because a running
task cannot be preempted. One-in-eight resumes doing 200 us of work took the lane to a **1.1 ms
p99**, with hot workers spending 62-65% of their idle passes beside a buried sibling.

Priority belongs to the task and is inherited by every resume of it, so setting `hiPri` on a
coroutine that suspends repeatedly puts *all* of its resumes on the lane, not just the first. Nothing
in the scheduler promotes a task to the lane behind your back -- see the block above `SetHotWorkers`
in `TaskScheduler.h`.

**Use K=0 -- the default -- if you have no latency-critical I/O.** Pure compute gains nothing from a
hot worker and pays a core for it. K=0 costs nothing: the lane collapses and the worker loop is
*cheaper* than a pool without the feature, because a worker that serves no lane checks one inbox, one
deque and one steal probe per victim instead of two.

**`SetHotThreadPolicy(Elevated)`** additionally raises the hot workers and the reactor's completion
threads to `THREAD_PRIORITY_TIME_CRITICAL` (Windows; a no-op elsewhere). It raises **only** those
threads, never the process -- raising the whole process measured *5x worse*, because the spinning
workers then preempt the completion thread feeding them. Untested beside a live audio or present
thread; if you see stutter rather than an I/O win, drop it and keep `SetHotWorkers` alone, which is
where most of the measured benefit is.

[DESIGN.md](DESIGN.md) has the rest -- the execution model, the integration contracts, and the
decisions that were tried and removed.

## Versioning

4.0.0-beta. The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`; those follow semver and do
not break without a 2.0. Every header is installed because the supported ones need them to compile,
but the rest are implementation detail and may change in any release. If you need something only
reachable through one of those, that is a missing feature -- open an issue rather than depend on it.

That promise has been broken exactly once, deliberately, and it is listed here rather than only in
the changelog: **`ParallelForNB` was removed in a minor release.** It offered no way to observe
completion, so it had no correct usage to break; it was undocumented, untested and uncalled, and it
predated the range APIs that superseded it. Use `PushArray`. If the count of exceptions in this
paragraph ever reaches two, the policy is the thing that is wrong, not the releases.

[CHANGELOG.md](CHANGELOG.md) has the release history and the negative results.

## License

BSD 3-Clause. Use it, fork it, ship it.
