# JLib::Scheduler

> **Development note.** This runtime was developed iteratively, using AI-assisted tooling for rapid
> prototyping and code generation under strict human specification and verification. Lots of work was done by hand,
> the workflow changed as I found output and testing became of superior quality and speed than I could produce by hand. All
> architectural decisions, correctness validation, and performance tuning were conducted manually.
> The commit history reflects this collaborative, tool-accelerated workflow.

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

Windows x64 & ARM64 (MSVC) · Linux x86-64 · Linux/Android AArch64 · C++17 (C++20 for optional coroutines) · BSD

macOS Apple Silicon builds and runs the **scheduler only** -- see [Build](#build) for what that
excludes and why it is stated that narrowly.


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

### Weight and scope

This is a **much larger library than enkiTS**, and the table above is not free:

| | enkiTS | this | |
|---|---|---|---|
| Code lines (comments and blanks stripped) | 2,329 | 13,862 | 6.0x |
| Total lines | 3,347 | 31,530 | 9.4x |
| Comment + blank share | 30% | 56% | -- |
| Public headers | 3 | 33 | 11x |
| Scheduler object file | 0.10 MB | 1.45 MB | 14.5x |
| Scheduled entity | 16-byte `SubTaskSet` | 64-byte `Task` | 4x |

**The independent-task row follows directly from the last line of that table.** enkiTS schedules a
range descriptor in a contiguous fixed ring -- no allocation, no pointer chase, nothing per item.
We mint a 64-byte `Task` carrying a WaitGroup pointer, a cancel token, a fiber assignment, a type
tag and a core preference. On "20,000 independent empty items" that is 4x the cache footprint per
item to do a job that needs none of it. enkiTS is not beating us on execution; it is beating us by
**not creating the object**, and making minting cheaper cannot close that -- only not minting would.

**What the weight buys** is the rest of the comparison table: suspending inside a task, the DAG with
AND/OR gates, cancellation scopes, topology-aware stealing, the I/O reactor and the timer wheel.
The blocking row is where it pays -- a blocked task frees its worker where a thread-pool task holds
one -- and that is a property of the architecture rather than of tuning.

**What is just weight**, and the two are worth separating: `TaskScheduler.cpp` is 6,400 lines,
startup allocates more than a thread pool needs, and 33 headers is a lot to hand someone who wanted
a work queue. None of that is required by the design; it is accumulated surface.

So these are not really competitors. **If your workload is N independent items with no dependencies
and no blocking, enkiTS is the smaller and faster tool** and the table says so. This is a fiber
scheduler with an I/O reactor attached, and it earns its size on suspension, latency bands and
dependency graphs -- or it does not earn it at all.

**"Model-checked" checked, not assumed**: enkiTS has no dedicated test suite at all (two ad hoc files
sitting in its `example/` directory); Taskflow's 39-file `unittests/` suite is the most extensive of
the three by far; marl has a real 14-file suite matched by benchmarks for most of them. None of the
three have anything resembling exhaustive concurrency verification -- it's unit tests only, at every
point on that spectrum. See [Model checked](#model-checked) for what GenMC and TLA+/TLC actually
cover here and, as importantly, what they don't.

## Measured

This was tested on my machine, third party tests have come back and some are faster than mine depending on hardware and platform.
Needs and welcomes more testing for research!

i9-13900K at Intel spec power limits, Release, **4.0.1**, 31 workers. Adaptive K armed
(`hot=2..4`), floor base 2, `park=wait`. Two affinity policies, one library -- `ideal` is the
default and `hard` pins each worker to a CPU.

> **The library is 5.0.0 and these rows are 4.0.1**, which is what the runs they came from printed.
> What 5.0.0 adds on top is correctness work -- two lost wakes, a yield starvation, the off-by-K
> family, and teardown (see [CHANGELOG.md](CHANGELOG.md)) -- none of which has been re-measured here.
> The away bit and the escape queue were **built and then reverted**: both cost throughput on the
> bench and neither bought back a measurable win, so nothing from them is in the dispatch path.
> Re-run `SchedulerBench` on your own machine, which has always been the advice.

> **On `hard` vs `ideal`.** Measured properly, interleaved A/B/A/B, four runs of four reps each on
> the machine above. Only one row separates them at all: the frame DAG, where `ideal` wins 4/4 with
> ranges that do not overlap (worst `ideal` 3.88us beats best `hard` 4.04us, ~5.8% on the mean).
> `hard` leads on `bt` and `mp` 3/4 with overlapping ranges and a wider spread within each arm,
> which is what pinning to a core you cannot leave should look like. Burst speedup is **4.00x in
> every cell of both arms** -- that row is saturated and currently discriminates nothing. An earlier
> non-interleaved comparison appeared to show `hard` winning burst 4.0x to 3.2x; interleaving showed
> that was the `ideal` arm degrading, not `hard` improving. `ideal` stays the default.

| | `ideal` (default) | `hard` |
|---|---|---|
| Serial round trip, p50 | **0.60 µs** | **0.60 µs** |
| ...p99 | 1.00 µs | 1.00 µs |
| I/O completion → job start, p50 | **0.50 µs** | 0.60 µs |
| ...p99 | 0.90 µs | 1.00 µs |
| Single-producer submission | 3.12 M/s | 3.22 M/s |
| Bulk submission via `PushBatch` | **14.61 M/s** | **15.11 M/s** |
| Bulk submission, 4 producers | 7.28 M/s | 7.25 M/s |
| 6-node frame DAG | 4.66 µs/graph | 4.70 µs/graph |
| 1M-element recursive fork-join (10k leaves) | **0.25 ms** | 0.33 ms |
| `ParallelFor`, compute-bound 256K | **10.95x** | 8.26x |
| `ParallelFor`, memory-bound 16M | 4.80x | 4.00x |
| Event resume, 1 waiter, no hold-off | 0.60 µs | 0.60 µs |

**The frame DAG went 21.1 µs → 4.66 µs and fork-join 0.25 ms → 0.25 ms against the 1.4.0 table**
below; the DAG is the row that moved most across the 2.x–4.x work (chunked edges, the awake floor,
and the steal hints, in that order of contribution).

**Read the tail, not the mean.** Both runs above printed one pathological round trip -- 72 µs and
53 µs against a ~0.6 µs median -- and the pool dump taken at that instant shows every queue empty,
nothing advertised, and no worker holding work. There is nothing for the scheduler to have done
faster: that is the OS preempting the measuring thread, and it is why `max` is reported next to
`p99` rather than folded into an average.

### The lane, and where it actually wins

`latency/hot` reads **1.12x of cold** (0.84 vs 0.75 µs) -- the hiPri lane is *slower* than an
ordinary push on a serial ping-pong, and that is reported rather than hidden. On the same run the
**io-pipe row is 0.50 µs p50 against cold's 0.75** -- the lane is 1.5x *faster* on the measurement
it exists for.

Both are true and they are not in tension. A serial ping-pong hands the lane nothing to win: the
pool is idle, an ordinary push already lands on a spinning floor worker, so there is no OS wake to
remove and no bulk body to avoid being buried behind. io-pipe measures a completion arriving at a
**busy** pool, which is the state a reactor actually lives in, and there the reservation is the
whole difference. A latency lane is worth what it saves you from, and on an idle pool it saves you
from nothing.

**The residual on `latency/hot` turned out to be the park primitive, not the lane.** It was
attributed here to the adaptive-K observer, which no longer exists; with `neverpark` the row reads
0.62 µs against cold's 0.65, and without it 1.09 against 0.53. Reserving a band and then letting it
*sleep* means every completion wakes a parked worker, which is the worst of both purchases -- the
bench says so directly (`K IS NOMINAL: 2 of 2 workers in [0,2) PARKED`). If you arm K, arm
`SetReservedNeverParks(true)` with it.

**What the reservation actually buys is a tail that ignores the workload.** From
`tests/io_overlap_test.cpp`, completion latency probed against a saturated pool, hiPri p99 in µs:

| bulk body | reserved K=2 | lane on the floor, no reservation | no lane |
|---|---|---|---|
| 5 µs | **4.20** | 8.80 | 164.90 |
| 20 µs | **7.70** | 25.60 | 423.50 |
| 400 µs | **1.90** | 332.00 | 5030.90 |

The reserved column is flat; the other two track the bulk body length. A lane on the unreserved floor
still beats no lane by 15-19x -- so if you want the cores back, that is a real option costing ~2x on
the tail at fine grain. What does *not* hold is the intuition that small bulk bodies make the lane
unnecessary: a completion waits behind a worker's whole **backlog**, not behind one body, so
shrinking the body shrinks each unit and not the queue.

### Bands, measured rather than declared

The bench reports what the pool **did**, not what was configured, and the two are checked against
each other:

| | `ideal` | `hard` |
|---|---|---|
| Reserved-band parks (must be 0 under an armed range) | **0** | **0** |
| Floor parks (must be 0, always) | **0** | **0** |
| Floor members caught mid-park by the last gate | 5 | 4 |
| Awake floor: base → peak → after a 25 ms settle | 2 → 11 → **2** | 2 → 14 → **2** |
| Adaptive K under 8.1M hiPri tasks | 2 → **4** | 2 → **4** |

The floor grows under a wave and collapses back to base within the settle, every run. The
reserved band never sleeps while a range is armed -- idleness there is expressed as the controller
shedding K, not as a reserved worker parking, which would hand the next completion exactly the OS
wake the band was bought to remove.

**K ramps and has not yet been observed shedding within a bench run.** Promotion is one window;
demotion is 200 ms plus three consecutive quiet windows, by design -- being late to shed costs a
spinning core, being late to add costs latency continuously. The run ends before that elapses. It
is reported as unverified rather than claimed as working.

**The burst row is the honest weak spot.** 16 heavy tasks (3.28 ms each) from an idle pool reach
4.0x of 16, with the floor peaking at 11-14 but only 7-11 workers actually running a task. The
gap is structural and named in the output: a busy worker's inbox has exactly one legal consumer,
so work becomes stealable only once that worker drains it. Growth wakes cores faster than the wave
becomes reachable to them.

<details>
<summary><b>The 1.4.0 table, kept for method</b> -- how these are measured, and two findings that
have not gone stale</summary>

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

</details>

Structural properties, which do not vary by policy:

| | |
|---|---|
| `Task` struct size | 64 bytes, one cache line, `static_assert`-enforced |
| Fiber stacks | 64 KB standard / 512 KB heavy, contiguous arena, guard-paged |
| Steal protocol | single-item Chase-Lev CAS, bulk queues only |
| Latency lane | intrusive MPSC inbox, one legal consumer, never stolen from |
| Band state | one 64-bit word: F, Fbase, K, Kmax, Kmin -- CAS per field, one load per decision |
| Escape queue | one MPMC queue, cold path only -- see [When nobody can reach a task](#when-nobody-can-reach-a-task) |


### Measured against enkiTS, TaskFlow and marl

Same machine, same harness, same worker count, each library expressed the way its authors intended.
i9-13900K at Intel spec power limits (see the caveat under [Measured](#measured)), 31 workers,
`affinity=none`, Release. **All four columns re-measured at 4.0.1** against enkiTS `ccd4e8c`,
Taskflow 4.1.0 (`83f90a2`) and marl `b8406ab`.

**Every column was measured with only that scheduler running.** The harness takes `--only=jlib` /
`--only=enki` / `--only=tf` and starts nothing else, so no library's threads are alive while another
is timed. Each library's own JLib cell comes from its own harness process, which is why the
round-trip reads 0.63 / 0.63 / 0.57 across the three -- that spread is the honest measurement noise
on this row.

| | this | enkiTS | Taskflow | marl |
|---|---|---|---|---|
| Round-trip submit→run→wait | **0.63 µs** | 21.03 µs | 1.05 µs | 0.82 µs |
| Bulk parallel-for, 20k items | **0.20 ms** | 0.34 ms | 0.48 ms | -- |
| Independent tasks, ns/task at 20k (best API each) | 27.9 ns | **16.2 ns** | 316 ns | -- |
| 25% of tasks blocked 600 µs | **8.85 ms** | 15.49 ms | -- | 9.68 ms |
| ...same, minus each library's own D=0 baseline | **2.23 ms** | 6.62 ms | -- | -- |

**Where we lose, and it is not close: bulk independent-task throughput at large N.** enkiTS pushes
20,000 empty tasks at **16.2 ns each against our best 27.9** (`PushArray`, chunk 32) -- ~1.7x, and
it holds from 8k upward. Its `TaskSet` splits one range across workers with no per-task object at
all, which is simply a better shape for that job than minting tasks, however cheap the minting gets.
We are ahead at 1024 (34.7 vs 85.8 ns) where the split has not amortised yet, and behind at 64 and
256 where our dispatch dominates. If your workload is "N independent items, no dependencies, no
blocking", enkiTS is the faster tool and this table says so.

**Where we win, the margins are large and the ranges do not overlap.** Round-trip is 33x enkiTS and
1.7x Taskflow. Bulk parallel-for is 1.7x enkiTS and 2.4x Taskflow -- and although our spread on that
row is a poor **50%** (0.181-0.282 ms), even our *worst* run beats enkiTS's *best* (0.331) and
Taskflow's *best* (0.475), so the ordering survives the noise even though the number should not be
quoted to three digits.

**The blocking row is the architectural one and deserves its delta column.** Absolute times include
each library's submission overhead, which differs; subtracting each library's own zero-block
baseline isolates what blocking *costs it*. On that measure we are **2.8x-5.9x better across
50-600 µs** (0.43 vs 1.20 at 50 µs, 0.64 vs 3.75 at 300, 2.23 vs 6.62 at 600), narrowing to 1.4x at
2000 µs. That is the fiber path: a blocked task suspends and frees its worker, where a thread-pool
task holds one. It is the reason the hybrid exists, and it is the one row where the difference is a
property of the design rather than of tuning.

**enkiTS's "N sets" column reads ~21,000 ns/task and is not a defect in enkiTS.** It is wake
amplification -- N separate `TaskSet`s each waking the pool -- and it is in the harness only because
it is the shape a naive port produces. The "1 set" column is enkiTS used correctly and is the number
quoted above.

> The marl column is from its own dedicated harness with interleaved arms; see
> [Against marl, re-measured at 4.0.1](#against-marl-re-measured-at-401) for the full round-trip and
> blocking sweeps, including the `f31` control that shows all-31-spinning is 5x *worse* than a floor
> of two.

**All three third-party libraries were re-cloned for this table**, so their columns move for reasons
that have nothing to do with us: enkiTS and Taskflow are both at 2026-08-06 HEADs, marl is archived
at 2026-04-27. A column is only comparable to another run of the same commit.

**The bulk parallel-for row moved from a tie to a win, and the mechanism changed underneath it.**
At 1.4.0 it read 0.355 ms against enkiTS 0.373 -- a tie inside both spreads -- measured against the
shared-cursor path that was mechanism-matched to enkiTS. It is now 0.20 ms against 0.34: recursive
lazy splitting with demand-driven recruitment, which is a different algorithm rather than a tuned
one. See [Parallel loops](#parallel-loops).

<details>
<summary><b>The 4.3 µs round-trip of the 1.4.0 table, and why it is gone</b></summary>

**It was specifically the cost of waking a FULLY PARKED worker, not a fixed
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

**The awake floor is what retired that number.** A floor of two keeps a steer target running, so the
round trip no longer buys a kernel wake at all -- 4.3 µs → 0.63 µs, which is the pinned-worker
figure above arrived at by policy instead of by hand-picking a worker. The analysis stands as the
explanation of *why* the floor exists.

</details>

Blank cells are not measured yet, not zero. marl's column (**archived**, last commit 2026-04-27)
calibrates the fiber path; it is not a recommendation.

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

### Against marl, re-measured at 4.0.1

Same process, arms **interleaved** rather than run back to back, all-normal worker priority to match
marl's. i9-13900K, 31 workers, Release. `f31` is our own floor pinned to 31 (every worker spinning)
and `sy7` is a spin-yield interval of 7 -- both are our configuration knobs, included as controls.

**Round trip (µs, lower is better)**

| | jlib | jlib `sy7` | jlib `f31` | jlib `f31s` | marl |
|---|---|---|---|---|---|
| min | 0.524 | **0.529** | 2.715 | 2.727 | 0.788 |
| median | 0.572 | **0.539** | 2.864 | 2.774 | 0.821 |

**1.43x faster than marl at the default, 1.52x with `spinyield=7`.** The `f31` columns are the
useful negative result: pinning all 31 workers awake is **5x worse than the default**, not better.
Spinning is not the mechanism -- steering a push at an already-running worker is, and a floor of 2
does that as well as a floor of 31 while leaving 29 cores alone.

**Blocking crossover (ms, min of reps)** -- 25% of tasks blocked for `block µs`:

| block µs | jlib | jlib `sy7` | jlib `f31` | jlib `f31s` | marl |
|---|---|---|---|---|---|
| 0 | 6.78 | 6.61 | 6.72 | 5.06 | **4.40** |
| 50 | 6.71 | 6.94 | 5.87 | 5.71 | **5.25** |
| 150 | 6.80 | 6.90 | 5.89 | 6.16 | **4.73** |
| 300 | 6.91 | 6.77 | 6.33 | 6.81 | **6.21** |
| 600 | 7.52 | **7.32** | 10.10 | 9.93 | 9.68 |
| 2000 | **21.39** | 21.15 | 24.27 | 23.87 | 24.46 |

**The crossover is at roughly 300-600 µs and it goes both ways.** marl is ahead below it by up to
1.5x; we are ahead above it, by 1.3x at 600 µs and 1.14x at 2000 µs. Published in full because the
half we lose is as informative as the half we win: marl never parks, so short blocks cost it
nothing and cost us a wake, while long blocks let our fiber path reuse the core that marl leaves
spinning.

Reproduce with `build-compare/marl_ab.ps1`. The arms must interleave -- measuring them in separate
sessions moved our own K=1 rows by 2x, which is machine drift being read as a result.

## Model checked

The lock-free/lock-based hot paths are model checked with [GenMC](https://plv.mpi-sws.org/genmc/),
not only tested. A test runs whichever interleaving the CPU happens to produce; a model checker
enumerates every execution the C11 memory model permits, so for a bounded harness the result is
exhaustive rather than lucky. Models live in `tests/verify/`.

- **Chase-Lev deque** (`deque_model.c`) - one owner, two thieves, 174 executions, no errors. It also
  settled a real question: the published verified Chase-Lev uses `seq_cst` for the steal CAS and this
  uses `acq_rel`. Both check clean, so the weaker ordering is sufficient here and the `seq_cst`
  fences are what carry the ordering.
- **Deque resize** (`deque_grow_model.c`) - the owner-only `grow()` that replaces `push_bottom`'s
  `return false`, which `deque_model.c` deliberately leaves out. One owner, two thieves, capacity
  2 - 4: **210 executions, no errors**. Three negative controls, because the algorithm makes three
  separate claims, and all three fail as they must: the copy must be published with **release**, the
  old buffer must be **retired, not freed**, and the pointer and its mask must be **one atomic
  object** (two independent atomics let a thief pair a new pointer with an old mask and index off
  the end). `top` and `bottom` are untouched by the resize, so the CAS stays the sole arbiter and
  the existing proof is undisturbed.
  It also found something about the *existing* model: with plain (non-atomic) slots the default
  build fails a data race that has nothing to do with resizing - an owner pushing at `b` writes the
  same physical slot a thief reads at a stale `t` whenever the two logical indices are congruent mod
  capacity. Benign in outcome, since the thief discards the value when its CAS fails, but a race on
  a plain object nonetheless, which is why the verified Chase-Lev stores the array as atomics.
  `deque_model.c` misses it only because its owner thread never *pushes*.
- **MPSC inbox** (`mpsc_model.c`) - Vyukov's intrusive queue, which every worker inbox is. Two
  producers, one consumer: **2,478 executions, no errors**, with three negative controls that all
  fail. Dropping the release on the link store or the acquire on the consumer's `next` load is a
  race on the node's payload; relaxing the head exchange **loses an item**.
  That third result is worth the space, because the first version of this model said the opposite.
  With a *bounded* consumer - pop a fixed number of times and stop - a `false` from `pop()` is
  ambiguous: it may mean "a producer is mid-append", which is correct and expected, or "this item is
  never coming out", which is the bug. The model could not tell them apart, reported no errors over
  14,840 executions, and the exchange's ordering looked unnecessary. Draining after the producers
  are joined makes "nothing is lost" a decidable safety property - the append window is closed by
  construction at that point, so it is safety and not liveness - and the same build then fails in 3
  executions. **An assertion too weak to fail makes the thing it was meant to test look
  unnecessary.**
- **Event waiter stack** (`event_model.c`) - two pushers, one drainer, 24 executions, no errors. No
  waiter lost, none woken twice, no race on the plain `nextWaiter` field. **This structure is
  retired** - `Event` has used a slot table since 2026-08-24 and `Task::nextWaiter` is gone. The
  model is kept and still run because the argument it verifies is *why* the replacement exists: the
  stack was correct and still could not support `SignalOne`, because its links were the tasks
  themselves, so a waiter could never leave early without the next drain walking a recycled slab
  slot. Do not read a green run of it as coverage of what ships.
- **Event waiter table** (`event_table_model.c`) - the wake path that *does* ship: a fiber-indexed
  slot array with an occupancy bitmap. Two pushers plus a concurrent `SignalAll` and `SignalOne`,
  36 executions clean. Verifies that the slot-then-bit publish is seen in that order, and that a
  waiter is claimed exactly once. Its three controls are the interesting part: every one of them
  fails at the *publication* assert rather than the double-claim it was aimed at, because the slot
  exchange turns out to be a stronger arbiter than the bitmap claim - so "it would wake someone
  twice" is the wrong thing to go looking for in a bug report about this code.
- **Counted epochs** (`counted_epoch_model.c`) - the counter-based reclamation a coroutine uses,
  having no epoch slot of its own; sharded, and modelled with the leave landing on a different shard
  than the enter, which is the migration case. 18 executions clean, 831 with a second reader.
  `-DNO_ADVANCE_GATE` **fails**, which is the proof the advance gate is load-bearing: without it a
  reader parked a full ring back is indistinguishable from a fresh one. Its other control,
  `-DNO_REVALIDATE`, **passes** - re-validation really is redundant here, because a reader announces
  before it loads. That is a simplification and a hazard at once, and the model says so: it makes
  announce-then-traverse a load-bearing requirement rather than a convention.
- **Worker sleep/wake predicate** (`sleepwake_model.c`) - the `workerState`/`hasQueuedWork`/
  `immediate` protocol that lets a push skip the mutex+notify when the target is already awake.
  32 executions clean as shipped. Its own history is the cautionary tale for this whole section: an
  earlier, single-flag version of this same model passed clean, the protocol was built from it, and
  1.2.0 shipped a lost wakeup that hung macOS arm64 about one run in three -- the model was not
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

The platforms below build and run the test suite:

> **The Linux tree did not build end to end until 5.0.0.** Two benches included `<windows.h>`
> unconditionally, and as unconditional CMake targets they aborted `cmake --build` partway through --
> so every target after them in build order was silently never produced. That is worth knowing when
> reading any historical claim about Linux coverage: the suite ran, but not all of it existed.

| OS | Arch | Toolchain | Scope |
| --- | --- | --- | --- |
| Windows | x86-64 | MSVC | Everything, including the I/O reactor. Primary development target. |
| Windows | ARM64 | MSVC (`armasm64`) | Everything. |
| Linux | x86-64 | GCC | Scheduler, plus the io_uring reactor (sockets). |
| Linux / Android | AArch64 | GCC / Clang | Scheduler. Android has no reactor -- io_uring is refused by policy. |
| macOS | arm64 | AppleClang | **Scheduler only.** See below. |

**macOS is the scheduler and nothing beyond it.** The context switch, the `src/darwin/` layer and the
job system are there and were written to work; the I/O reactor is not, and will not be claimed until
somebody can actually run it. `IsAvailable()` returns false, there is no kqueue backend, and I have
no Apple hardware to write or verify one on -- so anything I shipped there would be untested code
wearing a support claim. Treat macOS as: it runs tasks, fibers and the parallel algorithms; it does
no I/O.

### CPU features on x86-64

**Nothing above baseline x86-64 is required.** There is no `/arch:` flag on any shipped translation
unit and no SIMD intrinsic in the library, so it runs on any x86-64 CPU, including pre-AVX parts
(Core 2, Nehalem, pre-Bulldozer, and the Goldmont-family Atoms that shipped without AVX into ~2020).

| | |
| --- | --- |
| **AVX / AVX2 in your code** | Supported and tested. 256-bit vector state survives a suspend, a resume, and a migration to a different worker, with other fibers running AVX arithmetic in the meantime -- `tests/avx_suspend_test.cpp` asserts exactly that, adversarially. |
| **AVX used by the library** | One instruction, on Windows x86-64 only: a `vzeroupper` at the top of `ContextSwitch`. |
| **AVX-512** | Should behave as AVX2 does; **untested**, no hardware. See below. |

**Why the library touches AVX at all.** The Windows context switch saves XMM6-15 with legacy-SSE
`movdqa`, and legacy SSE executed while the upper halves of YMM are live pays an SSE/AVX transition.
A fiber that parks straight out of an AVX kernel is in exactly that state, so it paid it on every
switch: **85.8 ns against 9.2 ns**, measured on a 13900K by `bench/context_switch.cpp`. One
`vzeroupper` recovers all of it.

**It is gated, not assumed.** `vzeroupper` is itself an AVX instruction, so emitting it
unconditionally would have quietly narrowed the floor above. Instead AVX is detected once at startup
(`CPUID` + `OSXSAVE` + `XGETBV`, in `src/win32/FiberInit.cpp`) and the switch tests a byte. A CPU
without AVX skips the instruction and gets the previous behaviour -- correct, and slower, which it
would have been anyway since without AVX there is no dirty upper state to transition out of. The
gate costs ~0.35 ns per switch, under half a percent of a suspend/resume round trip.

Destroying upper vector state there is permitted rather than tolerated: those halves are volatile
across a call under the Win64 ABI, and a context switch is an opaque call.

**AVX-512 is the honest gap.** `VZEROUPPER` is documented to clear bits `[MAXVL-1:128]` of ZMM0-15,
so a fiber parking out of a ZMM kernel should recover the same way, and ZMM16-31 are unreachable
from legacy SSE and cannot be involved. Neither claim has been measured -- the machine this was
developed on has AVX-512 fused off. There is no correctness risk either way (AVX-512 implies AVX,
so detection is right and the instruction is legal); what is unverified is the speedup, not the
behaviour.

The POSIX x86-64 switch has none of this and needs none: the System V ABI makes every XMM register
caller-saved, so that routine has no vector block and executes no legacy-SSE instruction at all.

### iOS

**Untested, not supported.** iOS, tvOS, watchOS and visionOS are arm64 Darwin, so they use the same
context switch and `src/darwin/` layer that macOS arm64 uses -- but nobody has run the result, and I
have no Apple hardware to do it. Opt in with:

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

## Samples

`samples/udp_rect.cpp` -- a rectangle you move over the network, with **both ends in one binary**:

```
udp_rect            receiver: opens a window, binds 127.0.0.1:45454, draws the rect
udp_rect --send     sender: arrow keys, one datagram per press, q to quit
udp_rect --send --drop 5    drop 1 packet in 5, to watch loss happen
```

Run the first, then the second in another console. Loopback, so there is nothing to configure and
no second machine to find.

It is the receive-**loop** counterpart to `io_socket_test`, which sends a single datagram and checks
it arrived -- that proves the plumbing and nothing about the shape real code has. Here the loop
outlives thousands of packets, publishes to atomics, `PostMessage`s a window it never touches
directly, and cancels cleanly on close. The coroutine occupies no thread while parked, so the
message pump keeps the main thread to itself.

The wire format is **absolute position with a sequence number**, not a delta, and that is the point
of the sample. Deltas are the obvious design and they are wrong over UDP: drop one and the two ends
disagree about where the rect is permanently, because nothing later corrects it. Absolute state is
idempotent -- a lost packet costs one frame and the next one repairs it. The sequence number handles
the other half, since UDP does not promise order either; the receiver keeps the highest it has seen
and discards anything not newer. `--drop` makes both counters move in the title bar.

**The teardown is worth reading even if you never run it.** Cancelling the scope is enough for every
other primitive here -- Event, semaphore and condition variable all cancel by waking their waiter --
and it is *not* enough for a parked read, which is sitting in the kernel's completion queue with the
kernel holding your buffer. `IoReactor::RequestCancel(token)` is the call that issues `CancelIoEx`;
without it a receiver with no traffic will not exit. See the comment at the bottom of `RunReceiver`.

### I/O reactor: where it actually works

`IoReactor::IsAvailable()` is the honest answer and you should branch on it. It reports a property of
**this process on this kernel**, not of the build -- a container that refuses `io_uring` via seccomp
or the `io_uring_disabled` sysctl reports false on a binary that supports it.

| Platform | Backend | Status |
| --- | --- | --- |
| Windows x64 / ARM64 | IOCP | Complete. Sockets, files, named pipes. |
| Linux x86-64 / AArch64 | io_uring | **Live as of 5.0.0.** Sockets pass end to end. |
| Linux without io_uring | — | **Not implemented.** `IsAvailable()` is false. |
| Android | — | Same: io_uring is usually refused by policy, so no reactor. |
| macOS | — | `IsAvailable()` is false; no reactor, and no kqueue backend until there is hardware to verify one on. |

**What "sockets pass end to end" covers**, so it is not read as more than it is: accept, connect,
send, recv, vectored send, a peer close as a zero-byte completion, the acceptor pool, `Stop` drain,
cancellation through nested scopes, deadlines, and `IoStream`'s chained ordering. **Not** covered:
file I/O (`SubmitRead`/`SubmitWrite` on a regular fd) has no test on Linux, and nothing has run under
load for longer than the suite takes. Reporting available is a claim that the operations work, not
that the backend is seasoned.

**There is no epoll backend.** The source describes one at length as the intended floor beneath
io_uring, and it was never written -- `src/posix/IoReactor.cpp` mentions epoll eight times and calls
`epoll_create` zero times. So on any Linux where io_uring is refused (older kernels, seccomp, the
`io_uring_disabled` sysctl, and most Android devices by SELinux policy) there is **no reactor at
all** and `IsAvailable()` correctly says so. That is the honest behaviour; it is not a fallback.

The gap matters most on **Android**, where io_uring is usually unavailable by policy rather than by
kernel version -- so async I/O there needs epoll written, not tested. The job system itself is
unaffected and runs on Android normally; this is the reactor only.

**Two escape hatches, both environment variables** so nothing an application links against can trip
them: `JLIB_IO_URING_OFF=1` forces the synchronous path back without a rebuild, and
`JLIB_IO_URING_FORCE=1` reports available even where the default would not. `JLIB_IO_TRACE=1` prints
one line per submission and per completion, paired by request pointer, which is how the chained-path
bug was found -- a submission with no matching completion is then visible by absence.

**One POSIX difference worth knowing before you design around it:** `SubmitDisconnect(s, reuse=true)`
is `DisconnectEx(TF_REUSE_SOCKET)` and has no POSIX equivalent -- a connected TCP socket cannot be
returned to an unconnected state. Ask `IoReactor::SupportsDisconnectReuse()` rather than finding out
by submitting; a refused disconnect leaves a half-open connection behind, and in this library's own
test that stranded a peer in a listener's backlog and failed four unrelated checks in the next
section.


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

    // No teardown call. The pool is process-lifetime: it drains and joins itself at exit, so
    // there is nothing to shut down here and no way to start a second one. `Join()` was public
    // through 4.0.2 and is not any more -- see CHANGELOG.md for 5.0.0.
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

### Fibers resume anywhere by default

A suspended fiber comes back on **whichever worker is free**, not the one it left. That is the point
of a fiber task library -- it is the only reason to have a pool rather than a thread per job -- and
it is the default as of 5.0.

**The trade is `thread_local`, and it is the only one.** A value read from TLS before a suspension
point is not necessarily the same value after it, because "after" may be a different thread. Nothing
catches this: you get the resuming worker's copy, silently.

```cpp
// Migratable (default). Do NOT hold a thread_local across a suspension point.
auto* tls = &SomeThreadLocal();
co_something_that_suspends();
tls->field = 1;                       // WRONG: `tls` may belong to another worker now
```

**The fix, if the state is yours: fiber-local slots.** They live on the fiber, which is the object
that actually survives the wait, so they are correct in *both* modes:

```cpp
enum class Fls : uint16_t { Scratch = 0, LastError = 1, COUNT };
static_assert((size_t)Fls::COUNT <= JLib::Fiber::kLocalSlots, "too many FLS slots");

TaskScheduler::FiberLocal((size_t)Fls::Scratch) = p;          // void*&, one load and an index
auto* s = TaskScheduler::FiberLocalAs<Scratch>((size_t)Fls::Scratch);
```

Eight slots per fiber, indexed by a compile-time constant -- no map, no lock, no allocation. The
library never reads a slot and clears them when a fiber is recycled, so a fiber never hands its
successor stale state; it cannot *free* what a slot points at, having no type, so an owning pointer
must be released by the task that set it. Off a fiber -- a `Native` task, a bare thread, main --
`FiberLocal()` reads `nullptr` rather than crashing, and `HasFiberLocal()` is the explicit check.

If the state is *not* yours -- a library you cannot audit keeps its own `thread_local` across a wait
-- take **pinned mode** instead:

```cpp
JLib::TaskScheduler::SetMigratableFibers(false);   // BEFORE Init()
JLib::TaskScheduler::Init(0);
```

Pinned means a fiber resumes **only** on the worker it was bound to. TLS is then safe, and what you
give up is resume-anywhere: a fiber whose home worker is busy waits for that worker specifically
rather than taking the next free one. This is marl's contract, and it was this library's default
through 5.0.

It is one predicate, not two schedulers -- pinned is the migratable path with the creditor set forced
to a single member -- so both modes share the same mechanisms and the flag is read at the
resume-routing decision rather than branched on throughout. It must be set **before `Init()`**;
flipping it under a live pool would strand already-bound fibers under the old rule.

> **Migratable is the newer path.** Pinned is what shipped through 5.0 and has the mileage. Resume-
> anywhere is the default because it is the correct shape for this library, not because it has more
> hours on it -- if you hit something odd under load, flipping to pinned is a useful bisect.

### Rules worth knowing

A task that will call `WaitFor` needs a fiber under it. `CreateTask` gives you **`TaskType::Fiber`**
by default, so this is usually automatic; a task explicitly created `TaskType::Native` has no fiber,
cannot suspend, and fail-fasts with no message if it tries.

Tasks live in 256-byte slab slots, so a lambda capturing more than about **192 bytes** fails a
`static_assert`. Capture pointers, not payloads.

**How many tasks may be BLOCKED at once is capped at 64 per core.** A suspended task holds its fiber
for as long as it stays suspended, so concurrent suspensions are bounded by the fiber pool. Past
that, workers re-queue the tasks they cannot start and retry: it still makes progress, but it looks
like a stall rather than an error, so one warning is printed and then the pool grinds. Tasks that
never suspend are unaffected, and so is the total number in flight -- it is specifically the number
blocked *simultaneously*. Call `TaskScheduler::SetFiberBudget(standardPerWorker)`
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

**There is now a second reason, and it is the stronger one: the cursor's wins are confined to cells
that lose to serial anyway.** Line the two sweeps up at the same N and the cursor takes `trivial`
and `light` — which measure 0.04x and 0.19x against serial there, so "the cursor is 3.8x better" is
a way of losing to one thread by 6x instead of by 25x. Where parallelizing is worth doing at all,
the splitter takes every cell. A router between the two paths would therefore be optimising exactly
the ranges whose correct answer is "run it serially", which is a gate question, not a path question.
The tables and the four-run sign flip are in
[DESIGN.md](DESIGN.md#do-not-route-large-uniform-ranges-to-the-cursor).

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

### When nobody can reach a task

Every queue here has exactly one legal consumer. A worker's inbox is drained by that worker and
nobody else -- no locks, no contention, no reasoning about who pops next. That is most of why
dispatch is cheap, and it has one consequence worth understanding before you tune anything:

**a task placed in the inbox of a worker that is inside a task body is reachable by nobody until
that body returns.** Not slow -- closed. No thief may take it, because taking it would mean two
consumers on a queue built for one.

So placement tracks which workers are *away* (inside a body) and steers around them. That covers
the normal case and runs out in one specific situation: the away set is set by **any** worker
running **any** task, so on a small or busy pool every candidate can be away at once -- a worker
running a two-nanosecond task is away exactly as one running a two-millisecond task is. Placement
then has to commit the task to somebody, with no way to tell which body finishes first.

**That is what the escape queue is for.** Ordinary work whose chosen worker is away goes to a
single MPMC queue that *any* worker may take from, and the first one free takes it. The choice is
deferred rather than guessed.

It is deliberately **not** a global run queue, and the difference is the whole design:

| | global run queue | this |
|---|---|---|
| Pushes that touch it | all of them | only when the chosen worker is away |
| Workers polling it | every pass | after own queues *and* a full steal come up empty |
| Guard | none | a depth counter -- one acquire load in the common case |
| Affinity | lost: pulled work has no `corePref`, no band, no home worker | preserved: this is only for work placement could not express |

A general global queue would put a contended line on the two hottest paths and dissolve the
placement decisions the rest of the scheduler exists to make. This is the destination for work that
has nowhere better -- precisely the case those decisions cannot express.

Reserved workers never drain it (that band takes lane work and nothing else), and lane work is
never diverted into it, or a completion would leave the lane it was routed to.

**Nothing to configure.** It has no knob and needs none: if placement can always find an un-away
worker, nothing ever enters it. It shows up in `DumpPoolState` alongside the away map if you want
to see whether your workload reaches it.

### Garbage collection is a task, and you do not schedule it

**You do not have to call anything.** The reaper queues reclamation as an ordinary task on fiber
death, rate-limited to one sweep in flight. `SetSelfReclaim` and `SetSelfScan` are gone, and so is
the obligation they implied.

Three designs were tried and the first two were wrong:

| | what it did | why it lost |
|---|---|---|
| worker-inline | whichever worker crossed the threshold stopped and swept | **p99 killer** — walks every participant on a thread that was running your frame, at a moment nobody chose |
| app-driven | you call `Tick()` at a frame boundary | better tail, but a **library cannot require a loop its embedder may not have**, and forgetting leaks silently in release |
| **a task** | the sweep is queued like any other work | displaces nothing; goes out as `Lane::Normal` so it never lands on the reserved band |

The difference between the first and the third is not *who* runs it — a task runs on a worker too —
it is **when**. Inline, a worker abandons its pass mid-flight. Queued, the sweep waits its turn
behind work that was already there.

You can still call `Tick()` and `Scan()` yourself if you have a natural idle point and want the
sweeps to land there, but nothing requires it:

```cpp
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
| when workers reclaimed | 58.1 / 58.9 / 60.2 | 67.7 / 66.8 / 69.0 | **331 / 331 / 336** |
| `Tick()` on your thread | 60.5 / 58.7 / 58.5 | 69.3 / 67.8 / 66.3 | **125 / 111 / 104** |

**Throughput does not change.** Median and p90 are a wash. What changes is the tail: **p99 improves
about 3x**, because the same scan stops stalling a worker mid-pass. That 3x is why worker-inline
reclamation was removed rather than left as a default — a default that costs a third of your p99 is
not a default, it is a trap with a knob next to it.

**The second row was measured with `Tick()` on the app's thread**, which was the design at the time.
The task-queued version should land in the same place for the same reason — the sweep is off the
critical path either way — but **that has not been re-measured**, and the row is left as what it
actually was rather than relabelled to match the current design.

**A development build warns once after 100,000 retirements if no sweep has run.** That now indicates
a scheduler problem rather than something you forgot: it means fiber deaths are not producing reclaim
tasks — a pool that only runs `Native` tasks never recycles a fiber, so nothing triggers one. A
release build will not tell you.

Historically this section argued the opposite — that ticking was the embedder's job, because a
library cannot choose a good moment inside your frame. The first half of that is still true; the
conclusion was not. A queued task does not have to choose a moment at all, which is what makes it
better than either the worker doing it inline or you doing it by hand.

#### Hazard pointers, same mechanism

Epochs are not the only reclamation here. Structures that must stay readable across a *suspend* use
hazard pointers instead -- epochs cannot, because no fiber or coroutine may suspend inside an
`EpochGuard`. That sweep used to land on a worker too, on the way into idle, and was removed for the
same reason.

**Forgetting `Scan()` is less severe than forgetting `Tick()`**: the threshold-triggered scan inside
`Retire()` still runs, so it degrades to *reclaims late* rather than *grows without bound*.

```cpp

while (running) {
    RunFrame();
    JLib::EpochManager::Instance().Tick();     // epochs
    JLib::HazardDomain::Instance().Scan();     // hazards
}
```

**The two differ in what forgetting costs you:**

| | `Tick()` (epochs) | `Scan()` (hazards) |
|---|---|---|
| If you never call it | **unbounded growth**; dev build warns once, release does not | **reclaims late, does not leak** |
| Measured | p99 331 → 113 µs | **not measured** |

The first row is the one to act on. `Retire` still runs its own threshold-triggered scan, so
forgetting `Scan()` costs you timeliness rather than memory, and
`HazardDomain::Instance().OrphanedRetired()` is how you would notice if a thread exited holding a
bag. Forgetting `Tick()` is the one that actually leaks.

The second row is a caveat rather than a recommendation. **The 3x p99 figure above is the epoch
result and has not been reproduced for hazards** -- the hazard bag is smaller and sweeps less often,
so it is a reason to measure your own workload, not a number to expect. Measure both
halves before assuming either pays.

### `CorePref` is about breadth, not core class

```cpp
Default   // steered at the awake floor -- the cheap push, no kernel wake
Wide      // spread across the full pool, paying wakes to get capacity NOW
```

Ordinary placement narrows to the awake floor whenever a floor worker is awake -- which is always,
since the floor never parks. That is the right answer for **latency-shaped** work: a completion or a
frame-graph node lands on a thread that is already running and costs no OS wake. It is the wrong
answer for **throughput-shaped** work, where being steered at two workers means the rest of the pool
only arrives through steals.

`Wide` is for the second kind. Use it when the body is long enough that a ~3 µs wake is noise and
what you want is every core running now: a physics step, a large parallel loop, any burst that wants
the machine at once.

```cpp
auto* t = sched.CreateTask(SimulateChunk, ctx, /*hiPri*/ 0,
                           JLib::TaskType::Native, JLib::CorePref::Wide);
```

Measured on 16 heavy tasks (3.3 ms each) submitted to an idle pool at `floor=2`:

| | `Default` | `Wide` |
|---|---|---|
| wall | 10.11 ms | **5.19 ms** |
| speedup | 5.2x of 16 | **10.1x of 16** |
| workers that ran one | 12 | **29** |

**The rule for when it helps:** `Wide` matters where *placement is the only distribution mechanism*.
A burst of independent tasks is exactly that -- nothing splits them, so where they land is where
they run. A recursively-split parallel range is not: its halves are distributed by **stealing**, and
measurement confirms the breadth hint changes nothing there (same 28-31 workers reached either way).
`ParallelFor` already asks for `Wide` where it applies; you do not need to.

**`CorePref::P` and `::E` were removed in 5.0.0.** They asked for Performance or Efficiency cores,
were requested by nothing that shipped, and were unproven where they applied -- under the default
`Ideal` binding a worker is not pinned, so the request was a hint the OS then weighed against its own
hybrid policy. They bound only under `Hard`, which is measured ~45% worse on wake latency and is not
the default for that reason. `Any` remains as an alias of `Default`.

**Android ignores placement entirely**, by the platform's decision rather than a gap here: its
cgroups own thread placement, so the affinity calls either fail for an unprivileged app or succeed
and are immediately overridden. `Hard` and `Ideal` are effectively `None`, and topology-aware
stealing is correspondingly approximate. Read nothing into timings there -- unconstrained thermal
throttling and heterogeneous cores mean a phone benchmark describes the phone. Verified by hand on
Termux, which covers Linux AArch64 on glibc but not bionic.

**Above 64 logical CPUs**, binding goes through `SetThreadGroupAffinity`/`SetThreadIdealProcessorEx`,
which take the processor group as data -- `SetThreadAffinityMask` takes it from the calling thread
and so cannot name a CPU in a second group. Handled up to 256 CPUs, but **untested above 64**; if you
have such a machine, that is the most useful thing you could report.

**The reasoning that removed `CorePref::P`/`::E` is worth keeping, because it applies to any future
placement hint.** `isPCore` is a label assigned ONCE per worker at pool startup, from real topology.
Under `Hard` that label stays true for the process's whole life -- a worker never leaves its assigned
core. Under `Ideal`, **the shipped default**, the OS is only given a preference and may migrate the
thread under contention or thermal pressure, so the label goes stale mid-run: a `P`-preferring task
could land on a worker that used to be on a P-core and no longer was. The mode where the hint was
maximally trustworthy (`Hard`) is the one already measured and rejected as the default for its own
cost (see [Worker binding](DESIGN.md#worker-binding) -- ~45% worse wake latency, ~2x on the frame
DAG). So the hint was only real in a configuration nobody runs.

`isPCore` itself is **not** gone and is still doing work: it drives E-core QoS handling and the
same-class/other-class victim ordering for steals. Those are properties of the machine that the pool
reacts to on its own, as distinct from a per-task request an application has to make correctly.

### Hot workers: how many, and when none

`SetHotWorkers(K)` reserves K workers that take **lane work only** -- no bulk task from any source,
stolen or pushed -- and the reactor steers I/O completions at them. It is **off by default (K = 0)**
and it is the difference between async I/O being usable in a hybrid pool and not.

**Reservation and spinning are two purchases, and this API sells only the first.** `SetHotWorkers`
does *not* imply never-park: a reserved worker still sleeps when its lane empties. It used to imply
it, and that charged every caller who merely wanted "don't run bulk work on q0" a ~35% ordinary
latency tax for a property most of them never asked for.

| call | reserved? | never parks? | use |
|---|---|---|---|
| `SetHotWorkers(k)` | yes | no | a lane that must not be buried behind bulk work |
| `SetIoHotLane(k)` | yes | yes | the I/O reactor -- see the table below |
| `SetHotWorkers(k)` + `SetReservedNeverParks(true)` | yes | yes | the same thing, spelled out |

The dispatch numbers below are the **`SetIoHotLane`** configuration. A completion landing on a
*parked* reserved worker still pays the OS wake the band exists to remove, so reservation alone
buys the "never buried behind a 400 us leaf" guarantee and nothing about the wake.

**Adaptive K is the exception:** while a scaling range is armed (`SetHotWorkerRange(min, max)` with
`max > min`) the reserved band never parks regardless of the flag. Idleness there has one owner --
the controller sheds K, and a worker stops spinning by ceasing to be hot rather than by sleeping
while still reserved.

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
hot worker and pays a core for it. K=0 costs nothing: the lane collapses, every push routes to the
ordinary inboxes, and the lane check is one load of a queue that is always empty.

**The lane is an MPSC inbox, not a deque, and that is a guarantee rather than an implementation
note.** It has exactly one legal consumer -- its owner -- so lane work is never stolen and never
staged anywhere on its way to running: the owner pops one arrival and runs it. There is no second
structure behind it and no unload step between the producer's push and the body, which is the whole
point on the one path that exists for latency. Two consequences worth stating plainly:

* **Nothing can rescue a completion queued behind a busy owner.** That is what K is for -- a
  reserved worker is never inside a bulk body -- and it is why a lane on an *unreserved* worker is
  ordering, not a latency guarantee.
* **Lane depth is presence, not a count.** An intrusive linked queue has no `size()`, so everything
  that reads the lane asks "is there work" and never "how much".

Stealing therefore touches one deque per victim, always -- there is no second probe to skip.

**`SetHotThreadPolicy(Elevated)`** additionally raises the hot workers and the reactor's completion
threads, and it is implemented on every platform -- `THREAD_PRIORITY_TIME_CRITICAL` on Windows,
`QOS_CLASS_USER_INTERACTIVE` on macOS (which is also what steers P vs E there, since Apple Silicon
exposes no affinity API), and per-thread nice on Linux. It raises **only** those
threads, never the process -- raising the whole process measured *5x worse*, because the spinning
workers then preempt the completion thread feeding them. That is also why the Linux backend goes
through `SYS_setpriority` on a TID rather than glibc's `setpriority`, which follows POSIX and would
elevate the whole process: the same regression, reachable by accident through the portable spelling.
Off Windows the request can be **refused** -- lowering nice needs `CAP_SYS_NICE`, and Android's
cgroups arbitrate regardless -- and a refusal is swallowed, because the unprivileged answer is still
an answer. Untested beside a live audio or present
thread; if you see stutter rather than an I/O win, drop it and keep `SetHotWorkers` alone, which is
where most of the measured benefit is.

### The awake floor, and why growth is gated the way it is

`SetAwakeFloor(N)` keeps workers `0..N-1` from ever parking. **Default 2.** It is a different knob
from `SetHotWorkers` -- K picks *which queue* a hiPri push lands in; the floor decides whether
landing anywhere costs an OS wake. Measured on 31 workers, 20,000 serial round trips:

| floor | kernel wakes | p50 | p99 | max |
|---|---|---|---|---|
| 0 | ~19,400 | 4.00 µs | 5.40 µs | 90–330 µs |
| 2 | **0** | **0.40 µs** | **0.60 µs** | ~80 µs |

At floor 0 every push wakes a sleeper and the landing spread is flat (~640 per queue). At floor 2
placement steers at a worker that is already running, and the tail that is left is preemption of a
spinner, not a lost wake. Throughput rows are *higher* at floor 0 -- that is 29 surplus workers free
to spin on a no-op storm, and it is not an argument against the floor.

The floor **grows** when a wave arrives and **collapses** back to the base when it drains. Growth is
gated by four filters, and every one of them exists because removing it was measured:

| filter | rule | what it costs to remove |
|---|---|---|
| **who** | bare producer only (`Thread::GetCurrent() == nullptr`) | `TaskDAG::Fire()` releases 6 dependents back-to-back from a worker and looks exactly like a wave. Without this, every graph grew the floor and pinned it at 16: frame DAG 3.5 → 7.2 µs across 20,000 graphs. |
| **when** | 2 pushes within 50 µs, **producer time** | see below -- no other clock works |
| **how long** | streak ≤ 64 | a wave is bounded; a flood is not. Without a ceiling the no-op row is indistinguishable from a burst: 1p 8.35 → 4.90 M/s. |
| **how wide** | grow by the streak, capped at N−K−1 | growing by one step per push places the tail of the wave before the floor catches up: 16 workers → 10. The cap was a flat 16 and is now "the pool minus the reserved band, minus one left parkable" -- that last worker is what keeps the collapse callable. |

**Do not put a duration check on the push path.** It is the obvious idea and it cannot work:

> At t=0 of a burst from an idle pool there is nothing true about the pool to observe.

Sixteen pushes land in ~10 µs while the workers are still coming out of `WaitOnAddress`. So
"has this worker been busy 200 µs" is false sixteen times, `busy` is false because nobody has picked
anything up, and inbox depth is shallow because nothing has drained. Four separate gates were built
on those signals and all four declined the entire submit. The **producer's own** timing needs none of
it -- two pushes 50 µs apart is a fact about the submission, knowable on push two.

A serial round trip cannot reach the streak (it pushes once, then blocks until that task completes),
which is why the latency row is unaffected: it still lands 100% on the base floor with 0 wakes.

Growth also fires from a **completion** whose body ran longer than 200 µs with a queue still behind
it (`Thread::GrowFloorIfLongBody`). That is the complement to the push path rather than a duplicate:
it catches a wave published *by a worker*, which the `who` filter deliberately excludes, and it can
use the body's measured duration instead of guessing. It hands the backlog out as well as growing --
growing alone leaves the promoted workers with nothing they can reach, because a busy worker's inbox
has one legal consumer.

Shedding runs from the idle path, one step straight back to the base, gated only on a 6 ms hold
since the last growth. It is deliberately *not* gated on the pool being quiet: a busy pool then never
sheds, which is how the floor once sat at 16 for an entire benchmark row.

`floor=0` remains meaningful -- "every core may park", for a process that cares about background CPU
more than about its tail.

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

## Credit
**The shape of this scheduler is not original, and the honest history is neither "copied" nor
"invented."** It was started independently, before I knew the talks existed. Partway in I found
them, and from that point the design was informed by them — so what began as convergence became,
deliberately, the same design.

The reference is Christian Gyrling's *Parallelizing the Naughty Dog Engine Using Fibers* (GDC 2015):
a small pool of worker threads, fibers as the unit that blocks — so a wait costs a context switch
instead of a core — and counters as the sync primitive. It was published as a **talk**, with no
paper and no reference implementation, so nothing here is a port; but nothing here is unaware of it
either, and claiming pure independence would be as inaccurate as claiming to have followed a spec.

**Concretely, what was taken:** the fiber stack split — 64 KB standard, 512 KB heavy — is
[FiberTaskingLib](https://github.com/RichieSams/FiberTaskingLib)'s numbers, the open implementation
of that design. Where the two differ elsewhere, the difference came from a measurement, and
[CHANGELOG.md](CHANGELOG.md) says which one.

**What is new here is not the design — it is that an implementation of this scope is open.** Fiber
job systems, work-stealing deques, hazard pointers and async I/O all exist publicly in pieces. A
single runtime that assembles them — three execution modes on one pool, K-hot latency lanes,
completion-based I/O inside the scheduler, primitives whose waiters steal work instead of blocking a
core, reclamation that covers readers which *migrate*, main-thread nodes in the DAG, near-zero
runtime allocation — is the part that has generally lived inside studios and trading firms rather
than in public. That is the claim, and it is about availability, not invention.

Everyone knows nobody in this space invented the deque either, so the rest is named too:

| Piece | Original | Where it lives |
|---|---|---|
| Fiber-based job system | Christian Gyrling (GDC 2015) | the whole design |
| Chase-Lev work-stealing deque | David Chase, Yossi Lev (SPAA 2005); orderings from Lê, Pop, Cohen, Zappa Nardelli (PPoPP 2013) | `TaskDeque.h` |
| Intrusive MPSC queue | Dmitry Vyukov | `TaskMPSCQueue.h` |
| Epoch-based reclamation | Keir Fraser (2004); the counted variant is SRCU-shaped | `Epochs.h` |
| Hazard pointers | Maged M. Michael (IEEE TPDS, 2004) | `Hazard.h` |
| Work stealing as a discipline | Blumofe, Leiserson (Cilk) | `ParallelFor`, no cost model |
| Treiber stack | R. K. Treiber (1986) | `Event`'s waiter list until 2.15.0 |
| `ConcurrentQueue`, `LightweightSemaphore` | Cameron Desrochers (moodycamel) | vendored, [include/LICENSE.md](include/LICENSE.md) |

**What is actually ours** is the integration and the failure modes: three execution modes sharing
one pool, the K-hot lane and its controller, counted epochs so a coroutine reader is *safe* rather
than *forbidden*, hazard cells indexed by the thing that migrates instead of by thread, the
cancellation model, and the tripwires. Those are the parts with no prior art to copy — which is
also why they are the parts that have been wrong most often, and why the negative results are in
the changelog instead of quietly deleted.

## License

BSD 3-Clause. Use it, fork it, ship it.
