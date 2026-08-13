# Changelog

Correctness fixes are marked **[CRITICAL]** with a note on what breaks without them -
downstream users (forks/ports) should treat those as must-pull.

## 1.2.3 - 2026-08-13

**[CRITICAL] `SchedulerMutex` could deadlock a thread against itself.** Acquiring one from a bare
thread runs stolen noFiber tasks while it waits, which is work-conserving and is also the most
dangerous property in the file: it makes locking REENTRANT. User code executes inside the
acquisition loop and can take locks of its own, and the interleaving is chosen by the scheduler, so
no lock-ordering discipline in the caller's code can prevent what follows.

Three separate failures came out of that, now guarded separately in `ContendedSpinStep`:

**Self-deadlock by inversion, the one that actually hangs.** A thread holding mutex A waits on B,
helps, and the helped task asks for A. A is owned by that same thread, stuck inside the task, so
nothing can ever release it. No fiber involved. `t_heldMutexes` closes it: a bare thread that owns a
`SchedulerMutex` stops executing other people's tasks entirely. Counted in `Try_Lock`, which is the
one place a bare thread acquires, including when a caller uses `Try_Lock` directly.

**Unbounded nesting.** A helped task contending the same primitive helps again, and each level is a
real stack frame. `t_spinHelpDepth` permits exactly one level; inside a helped task it spins instead.

**Pointless CPU burn when the holder is a suspended fiber.** Helping cannot resume it, because only
noFiber tasks are stolen here, so if the resumer is a fiber task the waiting thread structurally
cannot make that progress. After 1000 unproductive passes it yields, giving the OS a chance to run a
worker that can. The count measures UNPRODUCTIVE passes rather than iterations: each pass may run a
whole task, so counting all of them would yield out a thread doing real work, and a run task resets
it.

`SchedulerSemaphore::Wait` and `SchedulerConditionVariable::Wait` use the same helper. Two
deliberate asymmetries. The semaphore RESPECTS `t_heldMutexes` but never adds to it, because a
permit has no owner -- the thread that takes one is frequently not the one that returns it, so
counting a `Wait` as an acquisition would make a consumer's count climb forever and permanently
disable helping on that thread. And the ownership count is maintained for bare threads only: a fiber
can acquire on one worker and resume on another, so a per-thread count would be corrupted by
migration, which is the same rule DESIGN.md already states about thread-derived state.

**New `SchedulerSemaphore::ScopedPermit`, which closes that gap for the case where it can be
closed.** RAII: acquire on construction, release on destruction, and while held it opts into the
same ownership guard the mutex uses. That works because the TYPE declares the usage pattern. Take a
permit lock-like, on one thread, and you get the protection; keep calling `Wait`/`Signal` directly
for producer/consumer and you pay nothing, correctly, because there the permit has no owner to
track. A fiber gets no counting and does not need it, since it suspends on contention and never
enters the helping path.

Verified rather than assumed, given how the last two changes went: permit held for exactly its
scope, released on destruction, balanced across 1000 acquire/release cycles, correct no-op when used
from inside a task, and the pool still drains afterwards (which is what a guard stuck above zero
would break).

What it does NOT do, and the changelog should say so rather than implying a stronger claim: make
deadlock impossible. A helped task can block on something the scheduler cannot see -- a plain
`std::mutex`, a file read, a GPU fence -- and no ownership tracking reaches those. DESIGN.md now
states the rule that actually covers it: **from a bare thread, hold nothing across a blocking call
into the scheduler.** The guards bound the damage; the rule is the protection.

**`WaitFor` gets the two guards but keeps its own yield policy.** It has the identical reentrancy
hazard -- a caller holding a `SchedulerMutex` across it runs tasks that can ask for that same mutex
-- and it was also helping without marking the depth, so a helped task's own `Lock()` could help
again and nest through it. It does NOT adopt the 1000-pass counter: that loop already yields on the
first unproductive pass, which is the right behaviour when there is genuinely nothing to steal, and
the counter would only make it spin longer for no gain.

Fork-join is unaffected, which was the thing worth checking rather than assuming. Normal callers
hold no `SchedulerMutex` at that point, so `t_heldMutexes` is zero and the loop behaves exactly as
before; the guard only bites on a pattern that already self-deadlocked. Measured across five runs:
fork-join 0.20 to 0.24 ms against a 0.21 to 0.25 ms baseline.

No measurable cost: latency 4.69 us, frame DAG 22.7 us, fork-join 0.21 ms, burst 11.4x, all
unchanged. Found by reading, not by a failure.

## 1.2.2 - 2026-08-13

**[CRITICAL] fixes an intermittent hang introduced in 1.2.0.** The notify optimisation could lose a
wakeup and park a worker forever on work only it can drain. It stalled CI on macOS arm64 roughly one
run in three, on identical code, after passing twice. Anyone on 1.2.0 should take this.

**The protocol was sound and the model was correct. The proof was too narrow, and it was applied
wider than it reached.** The original `tests/verify/sleepwake_model.c` contained ONE flag,
`hasQueuedWork`, seq_cst on both sides, and showed that a single total order leaves at most one
party stale. Worker()'s sleep predicate has THREE inputs. `immediate` and `paused` were left as
release stores read with acquire, and for those pairs no total order exists: the setter stores its
flag, loads `workerState`, sees AWAKE and skips the signal, while the worker stores GOING_TO_SLEEP,
loads the flag, sees the stale value and parks. Exactly the interleaving the model's own negative
control reproduces, on variables the model never contained.

The fix is that every input to the sleep decision now joins the same total order: `immediate` and
`paused` are seq_cst on both store and load, alongside `hasQueuedWork`. `running` is the exception
and does not need it, because `Join()` passes `force=true` and never takes the skip.

**The model now contains all of them, and its negative control reproduces the shipped bug.** Adding
that control exposed a second trap worth naming: with the correctly-ordered pusher present,
`-DWEAK_IMMEDIATE` PASSES. Any single notify wakes the worker whoever sent it, so while a seq_cst
pusher is racing, the worker can only reach SLEEPING in executions where that pusher already
signalled. **A correctly handled flag masks a broken one.** The real system has no such guarantee,
since a worker can park with only an immediate-setter racing it, so `-DIMMEDIATE_ONLY` removes the
pusher and makes the weak flag stand alone. It fails immediately there.

```
every flag seq_cst                  no errors, 32 executions
-DIMMEDIATE_ONLY (strong)           no errors, 5 executions
-DIMMEDIATE_ONLY -DWEAK_IMMEDIATE   Safety violation     <- the 1.2.0 bug
-DACQ_REL_ONLY                      Safety violation
```

Why macOS caught it and nothing else did. It is weakly ordered, which x86 is not, so the reordering
is real rather than hidden by TSO. And `macos-14` is a **3-core** runner, so `pool = hw-1` is two
workers: the most park-prone configuration in the matrix, hitting the window far more often than
Linux AArch64's four. A full run on a six-worker Android phone missed it too.

The measured gains are unchanged from 1.2.0: latency 4.7 us, frame DAG 22.0 us, burst 11.5x.

The rule this leaves behind, now written into the model: a fourth input to the sleep predicate must
be seq_cst on both sides AND must appear in that model with its own negative control. A proof covers
what it modelled and nothing else.

## 1.2.1 - 2026-08-13

**[CRITICAL] for macOS: the default build was broken, and had been since 1.1.1.** `SchedulerTopologyTest`
failed to link with an undefined `JLib::topology::detail::ParseCpuList`. The library itself compiled
fine, but `cmake --build build` builds every target, so a plain build on macOS failed outright.

The declaration in `Topology.h` was guarded on `!JLIB_PLATFORM_WINDOWS`, which is not the same set
as "platforms that have this function". `ParseCpuList` parses Linux sysfs text and lives in
`src/posix/Topology.cpp`; macOS does not build that file at all, it gets `src/darwin/Topology.cpp`
and sysctl. So Apple targets got the declaration without a definition and the test linked against a
symbol nobody emits. Both guards are now `JLIB_PLATFORM_LINUX`.

Worth naming how it slipped through: a Windows and a Linux build both pass, because on Windows the
guard excludes it and on Linux the definition exists. The only configuration that fails is the one
neither developer machine here can produce. CI on `macos-14` caught it, which is the entire argument
for keeping a runner for a platform nobody owns.

## 1.2.0 - 2026-08-13

> **DO NOT USE THE `v1.2.0` TAG.** It contains the notify optimisation described below in its broken
> form, which can lose a wakeup and park a worker forever. It hung macOS arm64 about one run in
> three. It was tagged but never published as a release, so nothing points at it by default; the tag
> is left in place only because rewriting a published one is worse. Use 1.2.2 or later.

`v1.1.1` is an intermediate tag inside this release rather than a release of its own. It was cut
partway through the work below, while this section still read "unreleased", and three commits landed
after it. Everything it contains is described here; it is left in place rather than deleted because
it is already pushed and rewriting a published tag is worse than an untidy one.

**A push no longer wakes a worker that is already running.** `NotifyWorker()` used to take the
target worker's mutex and signal its condition variable on every single push, whether or not anyone
was listening. It now checks the worker's state first and returns immediately if that worker is
awake, because an awake worker clears `hasQueuedWork` and re-searches on every loop pass and will
find the task by itself.

Measured on a 32-thread i9 at Intel spec, medians of five runs against five before:

```
                 before    after
latency          6.4 us    4.7 us     -26%
frame DAG       28.4 us   22.1 us     -22%
fork-join       0.27 ms   0.23 ms     -15%
burst           11.6x     11.4x       unchanged
throughput/1p   0.81 M/s  0.83 M/s    unchanged
```

The variance result is worth as much as the speed: latency run-to-run spread fell from **19% to
2.4%**. Removing a kernel thread wake from the hot path removes the largest single source of jitter,
which for a frame scheduler matters at least as much as the mean. And `burst` is untouched, which is
the whole point -- the fan-out cap described below bought similar submission gains and destroyed
burst parallelism to do it, while this leaves an idle pool free to recruit everybody.

1p and PushBatch are unchanged, as expected. In those the pool genuinely outruns the producer, so
the workers really are parked and really do need waking; there is no wake to skip.

**The protocol is three states, not a flag, and it was model checked before it was written.**
`AWAKE` / `GOING_TO_SLEEP` / `SLEEPING`, where the middle state publishes the worker's INTENT before
it commits, so a pusher arriving between "decided to park" and "actually inside cv.wait" still
signals. `tests/verify/sleepwake_model.c` runs it through GenMC: 25 executions, no errors.

Every transition, and the load in `NotifyWorker`, is `seq_cst`, and that is load bearing rather than
defensive. The model's `-DACQ_REL_ONLY` negative control reports a safety violation, with this trace:

```
worker: Racq (g_work,  0)   <- reads the initial value, misses the push
pusher: Racq (g_state, 0)   <- reads the initial value, misses GOING_TO_SLEEP
```

Both loads stale at once. The pusher concludes "awake, no signal needed" while the worker concludes
"no work, safe to sleep", and the task is stranded on an inbox only that worker can drain. Neither
thread is reordered against itself; acquire/release simply never promised one would observe the
other, because StoreLoad is the pair it leaves free. **The weaker version is correct on x86 anyway**,
since `lock cmpxchg` incidentally drains the store buffer, so testing it on a desktop would have
produced a clean run and shipped a deadlock to AArch64.

`Join()` deliberately does NOT get the optimisation and passes `force=true`. Shutdown flips
`running` rather than `hasQueuedWork`, so skipping there would be the same Dekker race on a
different pair of variables, and one the model never covered. A proof does not extend to the
operation it did not model.

**The throughput benchmark was measuring something narrower than its name, and the difference looked
like a scheduler defect.** Sweeping pool size on it showed 3.4 M tasks/sec at 8 workers falling to
0.8 M/s at 14 and staying flat, a 4.3x collapse that reads as a failure to scale. It is not. On the
same machine and the same sweep, fork-join runs 0.16 to 0.20 ms and the frame DAG 24.2 to 23.6 us,
both essentially unchanged. Only the one benchmark with a single submitting thread fell over.

The old bench is now `throughput/1p` and has a sibling, `throughput/mp`, which submits the same
200,000 tasks from four producer TASKS running on the pool instead of from one thread. Measured
across the same sweep, on 4 through 31 workers: 1p goes 3.24, 3.44, 2.37, 1.01, 0.78, 0.79, 0.80,
while mp goes 6.34, 5.14, 4.64, 4.51, 4.39, 4.03, 3.34. The cliff is entirely absent from the second
column. What remains there is a gentle decline across an eightfold increase in workers, which is
ordinary contention in a benchmark composed of nothing but scheduling overhead.

**The mechanism is submission cost, and it is not what was first assumed.** 1p now times the push
loop separately from the wait, and `drain-after-submit` is **0.00 ms at every pool size from 8 to
31**. The pool is never behind; it has finished before the producer stops pushing. The entire number
is one thread's submission rate, which falls from 3.36 M/s at 8 workers to 0.91 at 13 and then sits
flat near 0.80 through 31.

The first explanation offered for this, recorded because it was wrong and the disproof is the useful
part, was a steal storm: surplus workers finding empty deques and interfering with the producer. A
new opt-in build option, `JLIBSCHED_STEAL_STATS`, counts steal probes and hits per worker and shows
the opposite. At 31 workers 1p performs 8.79 M probes for **81 hits**, while mp performs 15.19 M
probes for 110,738 hits. Per unit of time mp probes roughly seven times harder than 1p and runs four
times faster, so probe volume cannot be what makes 1p slow.

**The cause turned out to be thread wakes, and it is fixable.** Every push ends in
`Thread::NotifyWorker()`, which unconditionally takes the target worker's mutex and signals its
condition variable. While the pool is saturated that signal is nearly free, because nobody is
waiting on it. Once the pool can drain faster than one thread can submit, workers genuinely park,
and each push starts paying a real thread wake. That is a threshold rather than a gradient, which is
why the measurement is a cliff followed by a plateau, and it explains why mp is immune: its
producers are workers, so nothing idles.

**An external-submitter fan-out cap was tried and REMOVED.** It is recorded here rather than shipped
because the experiment identified the mechanism above, and because the reasons it failed are worth
not rediscovering. The idea was to cap how many distinct workers an external submitter rotates
across. Worker-to-worker pushes are never capped, since narrowing those would concentrate a
fork-join's children onto a few deques; `Thread::GetCurrent()` is null exactly when the caller is
not one of ours. Measured at pool 31, sweeping the cap:

```
fanout   1p throughput   latency    frame DAG
all(31)  0.81 M/s        4.84 us    24.69 us     <- current default
1        8.96 M/s        0.79 us    22.02 us
2        4.55 M/s        0.71 us    22.40 us
4        3.27 M/s        4.69 us    23.48 us
8        3.20 M/s        4.82 us    23.53 us
```

Single-producer throughput improves 11x and drain stays near zero (0.17 ms), so stealing distributes
the concentrated work perfectly well -- which it should, that being what the deques are for, and it
was previously doing almost nothing here. **Latency improves 6x**, and that is the more interesting
number: the latency bench pushes one task and waits, 20,000 times, so with wide fan-out every
round-trip lands on a different PARKED worker and pays a full wake. 4.8 us is roughly what a thread
wake costs. The frame DAG gains 11%. `throughput/mp` and fork-join are unchanged, confirming the
blast radius.

**Two things killed it.** A new `burst` bench submits 16 EXPENSIVE tasks from a deliberately idled
pool and reports parallel speedup against a measured single-task time. That is the case every other
bench misses, and it is the one a frame loop actually contains:

```
fanout   burst speedup, MSVC   burst speedup, GCC
all(31)  7.8x                  11.6x
1        1.8x                   2.0x
2        2.5x                    -
```

Two compilers on two platforms, which matters here because the first version of this bench was
wrong. Its body was `acc += i * k`, which has a closed form that GCC finds and MSVC does not: the
identical task measured 0.66 ms under one and 0.02 ms under the other, quietly making it a
parallelism benchmark on Windows and a task-overhead benchmark on Linux. It is an xorshift chain
now, where each iteration depends on the last and there is nothing to fold. Worth remembering for
any future bench body: if a compiler can solve your busy-work, one platform will report a number
that means something different from the other's.

Narrowing the fan-out collapses it. All sixteen tasks land in one inbox, only that worker is
notified, and the other thirty stay parked with no one to wake them, so the pool never assembles.
Trading 7.8x down to 1.8x on burst parallelism to buy submission rate is a bad deal for an engine,
and no fixed cap can tell the two workloads apart.

Worse, it could HANG. `PushLocal` spins on `while (immediateCoresInUse[chosen]) chosen =
PickNextWorker();` with no yield and no widening. That is safe with thirty-one candidates, because
they are never all claimed at once, and is not safe with four: `PickNextWorker` keeps handing back
claimed workers and the loop never exits. A benchmark run sat livelocked on one core for hours
before anyone noticed. An attempted fix that widened to the full pool on exhaustion did not hold
either, so the whole thing came out rather than being patched twice.

The experiment still earned its place, because it identified the mechanism, and the fix it pointed
at is the awake-preference change at the top of this release. That one keeps choosing the hot worker
in the latency case while an idle pool still recruits everybody for a burst: no tuning constant, and
no workload where it is the wrong answer. It touches the notify path that produced the ParallelFor
lost-wakeup deadlock, which is why it got a model before it got code.

**`PushBatch` is benchmarked for the first time, as `throughput/bt`, and it is the actual answer for
bulk submission: 12.0 M tasks/sec against 0.78 for the per-task path.** It has been in the scheduler
all along, linking a run of tasks locally and handing the chain to one inbox with a single notify
instead of paying worker selection, an inbox push and a condition-variable signal per task. Nothing
measured it, which is a large part of why the per-push wake cost stayed invisible. It concentrates
work on one worker exactly as a fan-out cap does, but as an explicit call at a site where the caller
knows what they are submitting, rather than as a global policy applied to work it knows nothing
about. That difference is why one is a default and the other is not.

The instrumentation defaults OFF and the library is built without it, because counters in the steal
loop would tax exactly the path under investigation. Numbers from a `JLIBSCHED_STEAL_STATS` build
should not be compared against a normal one.

1p is kept rather than replaced, because a main thread submitting a frame's work is a real pattern
and its submission rate is worth knowing. It is now labelled as a producer measurement so nobody
reads it as capacity, the same reason ParallelFor was split into memory-bound and compute-bound.
Producers in 1b are tasks rather than extra OS threads, so a pool that already owns every core is
not measured against oversubscription, and the count is fixed at four so a pool-size sweep compares
one variable at a time.

**`TaskAllocator`'s live-slot counter is sharded per thread.** It was one `std::atomic<long long>`
incremented on every `Alloc` and decremented on every `Free`: a single cache line taken exclusively
twice per task by every worker. `memory_order_relaxed` does not help, since relaxed governs ordering
and the read-modify-write still needs the line. It is now one padded counter per thread, summed on
demand by `LiveCount()`, whose only two callers are an error string and a diagnostic print.

Honesty about why: this was done as a fix for the collapse above and **it was not the cause** -- the
measured difference was 0.76 to 0.81 M/s at 31 workers, which is noise. Kept because removing a
globally contended atomic from the hottest path in the scheduler is correct on its own terms, but it
earns no performance claim.

## 1.1.0 - 2026-08-12

Started as 1.0.1 and became a minor rather than a patch partway through, once the 64-CPU work turned
from refusing an unsupported machine into actually supporting it. Adding a capability is a minor
bump. The version is raised in the same commit as the change rather than at release, because a tree
that has changed while still reporting the last tag is precisely the problem the benchmark version
stamp below exists to solve.

**Machines wider than 64 logical CPUs are now addressed properly, across all processor groups.**
Previously the scheduler could not name a CPU outside the first group, and did not know it: worker
binding used `1ULL << cpu_affinity`, which is undefined past 63 and, because x86 masks the shift
count to six bits, quietly evaluated to `1ULL << 0` and pinned that worker to CPU 0. A 128-thread
machine would have stacked an entire processor group onto a single core, presenting as unexplained
slowness rather than as a bug. Both platforms built the same 64-bit mask, so this was never
Windows-only, though Windows is where it bit hardest.

The fix is in three parts. Masks are now a `CpuMask`, a 256-CPU fixed bitset, in place of a
`uint64_t` throughout `Topology`, `llcMaskOfWorker` and the binding calls. CPUs are named by a flat
`CpuId` defined as `group * 64 + bit`, so the group and the processor number fall back out by
shifting and nothing above the platform layer has to know groups exist. And binding goes through
`SetThreadGroupAffinity` and `SetThreadIdealProcessorEx` rather than `SetThreadAffinityMask` and
`SetThreadIdealProcessor`, which is the part that actually matters: the old calls take the processor
group *ambiently* from the calling thread, so no mask value whatsoever could have referred to a CPU
in another group. The new ones take it as data.

`src/win32/Topology.cpp` also stopped filtering its records on `Group == 0`. That filter meant a
machine with a second group received a topology map describing only the first 64 CPUs: no SMT
siblings, no cache clusters and no P/E labels for any of the rest. Not a degraded map, an absent
one, and it would have silently disabled locality-aware stealing for half the machine.

Pool sizing no longer trusts `std::thread::hardware_concurrency()` on Windows either, since it has
historically reported only the calling thread's group. `Info` now carries `logicalCount` and
`groupCount` from `GetActiveProcessorCount(ALL_PROCESSOR_GROUPS)`, which the platform layer already
had to walk every group to fill in anyway.

Nothing changes on a machine with 64 CPUs or fewer, which is every machine this has been tested on:
verified unchanged on a 32-thread i9 under all three policies, and building and passing on Linux.
**The multi-group path itself is written but not yet exercised on real hardware** -- that needs a
machine nobody here owns, and it is the next thing to validate. A `CpuId` past `CpuMask::kMaxCpus`
still degrades to unbound with one warning, because a mask that silently sets no bit reads as
unexplained slowness too. Raising the cap is `CpuMask::kWords` and nothing else.

**The worker-to-CPU assignment is enumerated from the topology rather than assumed to be `0..N-1`.**
This was a bug in the first cut of the above, caught before it shipped and worth recording because
the failure was silent. Workers were assigned `cpu i+1`, a DENSE walk, while the new flat `CpuId` is
SPARSE: Windows aligns processor groups to NUMA boundaries instead of packing them to 64, so a
96-CPU machine presented as two groups of 48 has live ids 0..47 and 64..111 with a hole between.
Dense assignment would have requested ids 48..63, which name no processor at all. The binds fail,
sixteen workers run unbound, and their sibling and cluster entries point at CPUs that do not exist.
Nothing crashes. `StartPool` now unions the reported core masks, lists the live ids ascending and
hands worker `i` the `(i+1)`'th, which is identical to `i+1` on any dense single-group machine.

**New `SchedulerTopologyTest`, and it is the reason the above was found.** The wide path cannot be
exercised on any machine here, so the parts of it that are pure logic are tested directly instead:
mask operations across all four words and past the cap, the `CpuId` to (group, bit) round trip, the
sysfs list parser including `"0-63,128-191"` which the old `uint64_t` silently truncated, and a
hand-built two-groups-of-48 topology asserting every worker lands on a CPU that exists, that none
collide, and that no worker asks for a processor number its group lacks.

That last case carries a negative control: it also asserts the OLD dense scheme would have missed
exactly sixteen CPUs. Without it the test would pass on a correct-looking enumeration purely because
the machine running it is dense, which would make it decoration. `detail::ParseCpuList` is declared
in `Topology.h` solely so the suite can reach it.

Found while costing out whether renting a multi-die machine was worthwhile. The answer was no: the
LLC-cluster stealing multi-die actually needs was already present, and the mapping bug that would
have justified the rental was found by reading the code and then pinned down by a test that runs on
a laptop. What hardware would still add is confirmation that Windows accepts these calls and lays
threads out as expected, which is worth having but is not what was broken.

**The published benchmark numbers were 25% pessimistic and the DAG row said the wrong node count.**
Latency is 4.7 µs rather than 6.3, the frame DAG is 23.2 µs/graph rather than 31.9, and that graph
has always had six nodes (start, update, sprites, text, parts, present) despite the table calling it
five. The old figures came from an older build and no policy reproduces them: `ideal` and `none` both
land at 4.7, and `hard` is far worse than 6.3, not better. Which change earned the improvement is not
recoverable without bisecting, and it was not worth it. Table now records medians of five runs, the
spread (3% on latency, 7% on the DAG) and the version measured.

**`fork-join` is best-of-3 rather than best-of-1.** It was the one unreportable number in the suite:
five runs spanned 0.34 to 0.49 ms, a 37% swing, while every other section held under 7%. A single run
of a sub-millisecond section mostly measures scheduler warm-up, which the halved result confirms, the
figure dropped to 0.19 ms once the first run stopped counting. It is still the noisiest section at
roughly 25% (0.18 to 0.23 ms over nine runs), so the README quotes it with that spread attached
rather than pretending the last digit means anything.

**The benchmark prints its version.** `JLIBSCHED_VERSION` in CMakeLists.txt is now the single source
of truth, feeding `project()`, the installed package config and a new define that `SchedulerBench`
puts in its header line. This exists because the first two third-party runs had to be discarded: the
ParallelFor test had been split into memory-bound and compute-bound, the default affinity policy had
moved from `hard` to `ideal`, and the crossover reporting had changed, but nothing in the pasted
output said which build produced the numbers. A result that cannot be attributed to a build is not
data, and asking people to re-run costs goodwill that is in short supply.

**Removed `JLib::current_task`.** It was an `inline thread_local Task*` in `Thread.h`, written on
four paths in the worker dispatch loop and read nowhere: not in the library, not in the bench, not
in the tests. `Thread.h` is an implementation-detail header rather than part of the supported API
(`TaskScheduler.h`, `Task.h`, `TaskDAG.h`), so removing it is within the versioning policy, but it
is still a removal and belongs here.

It went because it was a trap rather than merely dead weight. A header-inlined thread-local called
`current_task` is exactly what someone extending this would reach for, and the obvious way to use it
(read it, wait on something, use it afterwards) is precisely the stale-thread-local bug described
below. It also cost a thread-local store per dispatch to hold a value nothing consumed.

**DESIGN.md states the rule that makes the above matter: nothing thread-derived may be held across a
suspend point.** A suspended fiber resumes on whatever worker collects it, so a `Thread*`, a thread
index or the address of a `thread_local` is stale as soon as a suspend returns. The section is there
because the failure is asymmetric across architectures and therefore easy to ship: x86-64 reaches
thread-locals through `%fs:`-relative addressing that is re-evaluated at every access, while AArch64
must materialise the thread pointer from `TPIDR_EL0` into a general register that the compiler may
hoist into a callee-saved one, where a correct context switch then faithfully preserves it across
the migration. Invisible on the machine most people develop on, live on the one they ship to.

The two thread-local sites that remain are safe structurally rather than by discipline, and the
section documents which pattern each uses, since those are what an extension should copy. Epoch
slots travel with the fiber (`Epochs::ThreadSlot(tid)` is only the fallback for callers not on a
fiber, which therefore cannot migrate). `TaskAllocator`'s per-thread free-list cache is safe for the
other available reason: `Alloc` and `Free` contain no suspend point, so the window never opens.

**`CpuRelax()` compiles under MSVC on ARM64.** It used GNU inline assembly on the non-x86 path, and
MSVC has no inline assembly on ARM64 at all, so that branch now uses the `__yield()` intrinsic.
Unreachable today because CMake refuses Windows on ARM64, and no supported platform changes
behaviour, but it removes a landmine for anyone attempting that port by hand.

Also corrected the README's stated reason for refusing Windows on ARM64. It claimed the port needed
a TEB stack-bounds fixup, which is false: no such fixup exists anywhere in this codebase, including
the Windows x64 build that ships and works. It gets away without one because the arena pre-commits
its stacks and installs its own guard page, and because x64 SEH unwinds from `.pdata` tables rather
than reading TEB bounds. Both reasons carry to ARM64. The honest position is that the port is
roughly a day of mechanical work that nobody has asked for, not that it is blocked.

## 1.0.0 - 2026-08-11

First tagged release. The version number is a statement about API stability, not about the code
suddenly becoming ready: the scheduler has been running a real engine for months. What changed is
that it now builds and passes its suite on four verified platforms with CI enforcing that on every
push, which is the point at which promising not to break the API costs something and therefore
means something. Breaking changes from here need a 2.0.

Everything below this heading shipped in 1.0.0.

**BREAKING (the last break before the promise): `fastJob` is now `noFiber`.** Same meaning, same
polarity, same `true` default - only the name changed. `CreateTask`'s parameter rename is invisible
to callers (C++ has no named arguments), so this only affects code touching `Task::noFiber`
directly, which realistically means nobody. Renamed now because it was the last free moment: the
old name advertised a benefit while the flag's real content is a *constraint* - a task with no
fiber underneath cannot suspend, and getting that wrong fail-fasts with no message. `noFiber` names
something checkable and puts the constraint at the call site.

**BREAKING, same pass: `TaskScheduler::TryRunStolenFastJob()` is now `TryRunStolenNoFiberTask()`.**
A public method, but with no callers in this tree or in any consuming project - the earlier rename
was case-sensitive and left every capital-F identifier behind. Renamed at the same last-free moment
and for the same reason: the name should say the task runs without a fiber, which is what makes it
safe for a fiberless spin-waiter to call.

**[CRITICAL] for consumers: `Task`'s layout changed, though its size did not.** It gained a
`Task* nextWaiter` link in what was already tail padding, so `sizeof(Task) == 64` still holds and is
still asserted. But the layout moved, which means a consuming project must be CLEAN-rebuilt rather
than incrementally rebuilt. A stale object file compiled against the old layout links without
complaint and reads the wrong offsets at runtime.

**`Event` is now lock-free and allocation-free.** It was a `std::mutex` around an `unordered_set`,
so every suspend allocated a hash node and every signal took a lock that an external thread - a GPU
fence callback, say - could contend. It is now an intrusive push-only stack threaded through
`Task::nextWaiter`: `AddWaiter` is one CAS, `SignalAll` is one `exchange`.

It needs no epoch reclamation, hazard pointers or tagged pointers, and the reason is load-bearing
rather than incidental. ABA bites on *pop* - read head, read `head->next`, CAS, and in that window
the node can be freed and a recycled address put back. Nothing here pops: `SignalAll` takes the
entire list in one exchange, so the window does not exist. That holds **only** while there is no
remove-one-waiter operation, which is why `Signal(Task*)` and `RemoveWaiter` were deleted rather
than kept - both had zero callers, and either one reintroduces remove-from-middle and the whole
hazard-pointer problem with it. The header says so, because they are exactly what someone would
helpfully restore.

Covered by `tests/event_smoke.cpp`, which signals while registrations are still in flight rather
than waiting for them to settle - the interleaving that could actually lose a waiter. It runs on all
four CI platforms, two of them weakly-ordered ARM64, which is the only reason coverage of new atomic
code is worth much: x86 is TSO and hides a missing barrier.

**`WaitGroup` moved to its own header, and `src/Task.cpp` became `src/WaitGroup.cpp`.** That file
contained exactly one function - `WaitGroup::WakeAll()` - and nothing about `Task` at all, so anyone
opening it looking for task internals found a synchronisation primitive. `WaitGroup` is a sibling of
`Event` and `DirectEvent` and now has a header like they do.

No caller changes: `TaskScheduler.h` includes `WaitGroup.h`, exactly as it already included
`DirectEvent.h`. `Task.h` forward-declares it, since a `Task` only holds a `WaitGroup*` - which also
keeps `<mutex>` and `<unordered_set>` out of the seven headers that include `Task.h` without ever
naming a `WaitGroup`. CMake needed no change (it globs `src/*.cpp`); `Scheduler.vcxproj` lists
sources explicitly and was updated.

**`GetEvent()` is usable without a second include now.** `TaskScheduler.h` returns `Event&` but only
forward-declared `Event`, so callers had to include `Event.h` themselves or get an incomplete-type
error. The cause was a cycle - `Event.h` included `TaskScheduler.h` - and it was unnecessary: all
`Event.h` ever needed was `Task` and `Fiber`, so it now includes `Fiber.h` and `TaskScheduler.h`
includes `Event.h`. One header is enough for all three primitives.

**`SchedulerMutex::Lock`'s documentation was wrong.** It said the caller must be a fiber. It has
always had a non-fiber branch that spins while running stolen `noFiber` work. Corrected - along with
the consequence, which is that it is the wrong lock for a short critical section reachable from a
foreign thread, since a driver callback contending on it would start executing tasks from the graph.

**`ParallelForFJ` is not experimental** and the header no longer says it is. `ParallelFor`
dispatches to it automatically past ~2 tasks per worker; below that the flat path is ~14% faster,
above it flat is ~8x slower at ~15k tasks. The doc comment had been contradicting the code beside it.

**Named events: the contract is now written down.** `GetEvent`/`WaitOnEvent` keep one entry per
distinct name and never evict - correct and cheap for a bounded, static set of rendezvous points,
which is what they are for. Minting a name per operation (`"fence_" + counter`) instead grows the
map without bound and eventually convoys on `registryMtx` during a rehash, which presents in a
debugger as a deadlock after about an hour of uptime. `WaitOnEventDirectArmed` is the API for
per-operation waits: pooled, no name, no map, no global lock. No eviction policy is needed here
because the unbounded case has its own API - that was always the design, it just was not documented.
A debug/development build now warns once at 4096 named events, naming the last key inserted.

**The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`.** Every header is installed
because the supported ones need them to compile, but the rest are implementation detail and are not
covered by the version promise. Stated explicitly so that fixing internals later is not a breaking
change by accident.

### macOS / Apple Silicon support
Builds and runs on macOS arm64, verified in CI on `macos-14` (AppleClang). The AAPCS64 context
switch is unchanged - the calling convention belongs to the instruction set, not the kernel, so
Apple arm64 uses the same `src/posix/aarch64/ContextSwitch.S` as Linux; only the Mach-O directives
behind `#if defined(__APPLE__)` differ, and those now genuinely take effect (see the `.s`→`.S` note
below). The ABI harness passes at `-O0` and `-O2` on both ARM64 platforms.

New `src/darwin/` OS layer, selected by CMake alongside `src/win32/` and `src/posix/`, while the
ABI layer under `src/posix/<arch>/` is shared by every POSIX target. `Topology.cpp` there reads
`sysctl` rather than sysfs. `Thread.cpp`'s affinity helper now takes a plain 64-bit mask instead of
a `cpu_set_t`, which keeps the policy switch free of a type macOS does not have.

**Placement is a documented no-op on macOS.** There is no thread-affinity API on Apple arm64 -
`THREAD_AFFINITY_POLICY` still links but has done nothing since Apple Silicon; the kernel owns
placement and takes intent through QoS classes. Topology reports SMT honestly (Apple Silicon has
none) but leaves the P/E class table empty: macOS publishes per-performance-level CPU *counts*, not
a logical-CPU-index → level mapping, and a class table built on a guessed ordering could not be
acted on even if it were right.

### Benchmark
`--help` used to fall through to the default and start a multi-minute run under a policy you had not
chosen; it now prints usage and exits, and an unknown argument exits non-zero.

**It defaulted to the wrong affinity policy.** `hard`, while the library defaults to `Ideal` - so
every casual run, including the first third-party numbers that came back, measured a policy the
library does not use and which measured ~45% worse on wake latency. Now defaults to `ideal`, and the
help text carries the measurement so the old assumption does not get re-derived.

New `[poolSize]` and `nosweep` arguments. Pool size is for sweeping worker count against latency and
the frame DAG, which is a DIAGNOSTIC - do not ship a small pool, it starves everything that is not a
tiny graph.

**`ParallelFor` is now reported as two cases instead of one misleading number.** The old single line
measured a 64 MB, ~2-flop-per-element kernel, which is capped by the memory system rather than the
scheduler: it reads below 1.00x on machines with a small last-level cache (0.75x on a Ryzen laptop
APU, 1.09x on an M1 Air) and above it on a large one (~3.4x on a 36 MB L3). A reader saw that near
the top of the output and concluded the feature does not work, while the crossover sweep at the
bottom of the same run showed up to 16x. It now prints a labelled memory-bound line and a
cache-resident compute-bound line, so the difference reads as the workload rather than the library.

**The crossover sweep was reporting invented numbers.** It took the first cell above `1.00x` as the
crossover, and run-to-run noise supplies that immediately - an M1 Air run claimed `trivial` won at
one microsecond of total work. A crossover now has to clear 1.15x *and* be confirmed by the next
size up, and the header states the rule.

### The benchmark no longer requires C++20
`std::atomic<double>::fetch_add` (C++20, P0020R6) is absent from AppleClang's libc++, and it was the
harness's only C++20 dependency. Replaced with a compare-exchange loop - which is what `fetch_add`
lowers to anyway on hardware with no native atomic FP add - so the bench is C++17 like the library.
Verified equivalent: the crossover sweep's sink prints the same value on x86-64 and AArch64.

### AArch64 support - the scheduler now builds and runs on ARM64
Full benchmark suite passes on Android/Termux (clang, AArch64), including recursive fork-join -
fibers suspending and resuming through a new hand-written AAPCS64 context switch under the real
scheduler. Windows x64 and Linux x86-64 rebuilt and re-run, unchanged.

New `src/posix/aarch64/{ContextSwitch.S,FiberInit.cpp}`: a 176-byte frame (x19–x30, d8–d15, FPCR).
Three things differ from the x86 ports and are worth knowing if you port further - the return
address lives in x30 rather than on the stack, so the trampoline address is seeded into a *register*
slot; there is no `call` to misalign the stack, so the trampoline needs no alignment compensation;
and SP must be 16-byte aligned at all times, not merely at call boundaries, because misalignment
faults rather than merely violating convention.

**Source layout change (affects hand-rolled builds).** `src/posix/` is now split by architecture:
`src/posix/x86_64/` and `src/posix/aarch64/`, each holding `ContextSwitch.S` and `FiberInit.cpp`.
The OS layer (`Topology.cpp`) stays shared, since Linux/x86-64 and Linux/AArch64 differ only in the
switch. If you add sources by hand rather than via CMake, add **one** architecture directory.

**[CRITICAL] for hand-rolled and out-of-tree builds.** Two definitions of `Fiber::Init` in one
static library is *not* a link error - the linker takes whichever archive member it reaches first
and silently drops the other. A stale `src/posix/FiberInit.cpp` left beside the new arch directories
therefore produces an AArch64 build that seeds a 64-byte System V frame for a 176-byte AAPCS64
restore, which faults one instruction past the top of the fiber stack. CMake now refuses to
configure if such a file exists, and each `FiberInit.cpp` `#error`s when built for the wrong
architecture. If you sync this tree by copying files, delete removed ones.

Also in this release: `include/platform.h` gained an architecture axis alongside its OS axis and a
`platform::CpuRelax()` spin hint (`pause` on x86-64, `yield` on AArch64) replacing 17 direct
`_mm_pause()` calls; the stray `<immintrin.h>` in `TaskScheduler.h` is gone; and on bionic, worker
affinity goes through `sched_setaffinity(pthread_gettid_np(...))`, since `pthread_setaffinity_np`
did not reach Android until API 36 and the binding is applied by the parent thread.

**CI now builds and runs the suite on every push** across Windows x64 (MSVC), Linux x86-64 (GCC)
and Linux AArch64 (GCC), plus the standalone AAPCS64 ABI harness at `-O0` and `-O2`. The ARM64
results therefore hold across two toolchains and two libcs - GCC/glibc in CI and Clang/bionic on
Android - which is what makes the ABI claim more than one machine's anecdote. Raspberry Pi needs
nothing extra: it is the same Debian-family aarch64/glibc configuration as the CI runner.

**No AArch64 performance numbers are published, deliberately.** Android cgroups own thread
placement, so affinity requests from an unprivileged app are routinely ignored, and thermal
throttling moves results mid-run. The ARM claim here is correctness only.

## 2026-08-07

### Linux support - the scheduler now builds and runs on Linux x86-64
Every benchmark passes on Ubuntu 24.04 / GCC 13, including recursive fork-join, i.e. fibers
suspending and resuming through the hand-written context switch under the real scheduler. Windows
is unchanged and unaffected.

- **Hand-written System V AMD64 context switch** (`src/posix/ContextSwitch.s`, GAS, Intel syntax).
  **`ucontext` was rejected on a MEASUREMENT, not on its POSIX deprecation:** `swapcontext` saves
  and restores the signal mask, which is a `sigprocmask` **syscall on every switch** - measured at
  **120.3 ns vs 8.0 ns for this implementation** - roughly an order of magnitude. Treat that ratio
  as indicative rather than exact: it compares a pure user-mode register swap against a **syscall**,
  measured under WSL where kernel transitions are inflated, so bare metal would narrow the gap. The
  mechanism is the point and does not change. Boost.Context was declined to keep
  the dependency count where it is.
  The SysV switch is *shorter* than the Win64 one: callee-saved is `rbx/rbp/r12–r15` only (RDI and
  RSI are argument registers here), **every XMM register is caller-saved so the whole 160-byte
  `xmm6–15` block disappears**, there is no shadow space and no TEB stack-bounds fixup. MXCSR and
  the x87 control word *are* still preserved - they are one physical register pair shared by every
  fiber on a worker, so a fiber that sets a rounding mode and yields would otherwise leak it.
- **Platform split by DIRECTORY** - `src/win32/` and `src/posix/`, each holding `ContextSwitch`,
  `FiberInit` and `Topology`. `include/platform.h` is the single place that tests the OS
  (`JLIB_PLATFORM_WINDOWS` / `JLIB_PLATFORM_POSIX`) and wraps the virtual-memory primitives, so
  `FiberStackArena` - alignment rule, bounds check, guard-page reasoning - exists **once** for both
  platforms. `Fiber::Init` sits next to its platform's assembly because the two are one contract.
- **`Ideal` on Linux binds to the whole LLC domain, not one core.** Linux has no equivalent of
  `SetThreadIdealProcessor`, but `sched_setaffinity` takes a **mask**, so the same intent - keep
  locality true with minimum rigidity - is expressed at domain granularity. This is what keeps
  `clusterMates` honest: the mask and the mate list derive from the same cache group, so the
  topology map is true by construction. Unmeasured on real hardware.
- **CMake build**, valid both standalone and as a subdirectory of the JLib umbrella, with
  `find_package(JLibScheduler)` support. A classic `Scheduler.sln` ships alongside for Visual
  Studio versions that cannot open the newer `.slnx` format.

### [CRITICAL] Data race in `StartPool` - worker startup vs. the `workers` vector
`StartPool` created a worker, pushed it into `workers`, and **started its thread in the same loop
iteration**. The worker's own startup path reads `scheduler->workers.size()`, so worker 0 was
reading the vector while the main thread was still `push_back`ing workers 1…N−1 - a concurrent read
against a write that can **reallocate**, letting the reader walk a buffer being freed underneath it.

Fixed by populating the vector fully and starting the threads in a second pass. `reserve()` alone
would *not* have been sufficient: a concurrent read of `size()` against a concurrent write is a race
even when no reallocation occurs. Nothing required a running worker before the vector was complete,
so the split costs nothing.

**This is shared code - the bug was equally present on Windows**, where it survived on timing luck
(the reallocation window is narrow and x86's memory model is forgiving). It is exactly the class
that becomes intermittent corruption under weaker ordering, so **forks on any platform should pull
it**, and it is the single strongest argument for auditing before any ARM work.

Found by ThreadSanitizer via the new `bench/tsan_probe.cpp` - a small harness that exercises each
lock-free structure a few hundred times rather than the benchmark's millions, since a race is
reported on first observation and volume only buys instrumentation cost. Post-fix the probe reports
**zero races**. Its header documents two TSAN blind spots to expect: `atomic_thread_fence` (which
TSAN cannot model, so `TaskDeque` reports are suspect) and unannotated fiber switches.

### [CRITICAL] Four latent defects, all found by porting
Every one of these compiled on MSVC and is non-conforming C++ - the category that breaks on a
compiler *upgrade*, not only on a new platform. **Forks should pull these regardless of platform.**

- **`Task` / `LambdaTask` declared `operator delete` as `= delete` while having a virtual
  destructor.** Ill-formed: the vtable's *deleting* destructor requires an accessible
  `operator delete` whether or not any code calls it. Now defined with an assert, so the
  "slab-allocated, never heap-deleted" guarantee survives with runtime rather than compile-time
  enforcement.
- **`LockFreeList::slabDeleter` - a `static` function - referenced the instance member
  `allocator`.** It is used as an `EpochManager::RetirePtr` callback, so it must be static and
  genuinely could not reach it. It compiled only because nothing instantiated `remove()` on a
  slab-backed list; **the first caller would have broken the build.** Fixed by carrying a
  `TaskAllocator* owner` on `LNodeBase`.
- **`LockFreeList.h` included MSVC-only `<intrin.h>`** and used nothing from it.
- **`Thread.cpp` used `std::memset` without `<cstring>`**; `bench.cpp` used `_stricmp`.

### [CRITICAL] LLC-aware work stealing never actually ran on Windows
`GetGroupMasksForRelation` read `info->Processor` for *every* relation, but
`SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX` is a **union** - `RelationCache` must be read as
`info->Cache`, which has a completely different layout. Reading `.Processor.GroupCount` off a cache
record landed in reserved bytes, read 0, and the loop never executed. Every cache query returned an
empty mask list **while still reporting success**, so `clusterMates` stayed empty and the
locality-first steal phase silently fell through to random on every Windows machine.

Found by porting: the new Linux sysfs implementation reported 17 cache instances on the same box
where Windows reported 0. Two independent implementations of one query disagreeing is what made it
visible after it had gone unnoticed for as long as the feature existed.

**Measured impact on a single-LLC Intel: none** (4.73 → 4.81 µs latency, 23.04 → 22.75 µs/graph -
noise). That machine's last-level cache spans all 32 logical CPUs, so "cluster mates" means
everyone and locality-first picks what random would. **It matters on multi-L3 hardware** - Ryzen
with 2+ CCDs, Threadripper, EPYC, multi-socket - where `clusterMates` becomes a real subset. Do not
read the flat numbers as "the fix was pointless"; read them as "that box cannot show it."

## 2026-08-05
- **[BEHAVIOUR CHANGE] Worker affinity default is now `Ideal`, not hard pinning.** New
  `TaskScheduler::SetAffinityPolicy(AffinityPolicy)` - `Hard` (`SetThreadAffinityMask`),
  `Ideal` (`SetThreadIdealProcessor`, the new default), `None`, `PhysicalOnly` (one worker per
  physical core, SMT siblings left empty). Must be set **before** `Init()`; binding happens at
  thread creation.

  Measured (`bench.exe hard|physical|ideal|none`, 32-logical hybrid, idle):

  | policy | throughput | ParallelFor | latency | frame DAG |
  |---|---|---|---|---|
  | hard | 0.83 M/s | 3.28× | 6.93 µs | 41.94 µs/graph |
  | physical | 0.83 M/s | 3.44× | 8.32 µs | 36.56 µs/graph |
  | ideal | 0.79 M/s | 3.72× | **4.64 µs** | **21.59 µs/graph** |
  | none | 0.80 M/s | 2.95× | 4.58 µs | 21.73 µs/graph |

  Hard binding buys ~4% on bulk throughput and costs **~45% on wake latency**: a pinned worker can
  only be woken onto its own core, so every sync point waits for that core specifically. The frame
  DAG - a chain of small nodes with a `WaitFor` between each, i.e. the shape of an actual frame - is
  dominated by that and loses nearly 2×. This contradicts the usual "engines pin their worker pool"
  advice, which is why it's recorded with numbers rather than asserted.

  `PhysicalOnly` exists because the previous scheme pinned workers to logical CPUs 1…N, deliberately
  doubling up on physical cores. It confirmed SMT contention is real - 15 physically-pinned workers
  roughly match 31 logically-pinned ones - but did **not** close the gap to unpinned, so the dominant
  cost is binding itself rather than the sibling mapping.

  **Caveat, deliberately not buried:** one idle hybrid machine, synthetic benchmark. Re-measure under
  load and on a non-hybrid CPU before treating it as universal. `Hard` remains available and is still
  the right choice for an application that genuinely owns the machine.
- **`SetParallelForThresholdUs` / `GetParallelForThresholdUs`**: the ParallelFor work threshold is now
  settable at runtime (defaults 75 µs optimized / 750 µs unoptimized). Set it enormous to force every
  `ParallelFor` serial - the fastest way to answer "is ParallelFor causing this?" without a rebuild.
- The threshold now keys on `NDEBUG || JLIB_DEVELOPMENT` rather than `NDEBUG` alone, so an optimized
  build that deliberately keeps assertions live (RelWithDebInfo-style) doesn't get the unoptimized
  value.

## 2026-08-04
- **Renamed the build artifact `Threads` → `Scheduler`.** `Threads.lib` in `C:\libs\Threads` was the
  last leftover of the original "T_Threads" name and disagreed with both the repo (`jlib-scheduler`)
  and the class (`JLib::TaskScheduler`) - three names for one library. Now: project/solution are
  `Scheduler.vcxproj`/`Scheduler.sln`, output is `Scheduler.lib`, canonical install is
  `C:\libs\Scheduler`. The namespace and class names are **unchanged** (`JLib::TaskScheduler`); this
  is purely the artifact.
  **[BREAKING for downstream]** `Threads.lib` and `C:\libs\Threads` are **gone**, not deprecated. A
  compatibility shim emitting both names existed briefly during the migration and was removed the
  same day once every consumer here was moved and verified - carrying a dead path just to avoid a
  one-line edit isn't worth the confusion of two names meaning the same thing, and pre-1.0 is exactly
  when a clean break is cheapest. **Forks: change your linker input to `Scheduler.lib` and your
  include path to `C:\libs\Scheduler\include`.** That is the whole migration; nothing in the API
  moved.
- **`bench/` is now in the repo** (`bench.cpp` + `build_bench.bat`). It was previously local-only,
  which made the measurements below unreproducible by anyone else.
- **`ParallelFor` now decides serial-vs-parallel by MEASURING, not by element count.** The old gate
  was `totalItems > 10000` - a constant set before fork-join existed and never re-measured. A sweep
  of per-element cost (1/8/64/512 flops) against N (256…200,000) found the crossover *element count*
  moves **400×** with body cost (200,000 for a trivial body, ~400 for an expensive one) while the
  crossover *work* stays pinned at **70–92 µs**. Element count was never the right unit: what races
  dispatch overhead is count × cost-per-element. The fixed gate was therefore wrong in **both**
  directions - a trivial body at N=10,001 was parallelized and ran **8–11× slower**, while a heavy
  body at N=4,000 was forced serial when parallel was **12.6× faster**.
  `ParallelFor` now runs a small prefix inline, times it, extrapolates, and parallelizes the
  remainder only if the estimate clears `kParallelWorthwhileUs = 75.0`. The probe is not overhead -
  it is loop work that had to happen anyway, done before the split instead of inside a chunk.
  Measured after: a trivial body went from 0.01–0.12× to a flat ~1.00× across the range (correctly
  choosing serial), while a heavy body keeps 5–19× from N=512 up.
  API-neutral - the signature is unchanged and no caller needs editing, but **callers with cheap
  bodies over 10k elements were silently losing several times over and will speed up on relink.**
- **`chunkSize` is now floored** so a range can't be split into more than ~4 chunks per worker.
  Past that, extra pieces buy no more load balancing and cost a task each: 256 elements at
  `chunkSize=2` built 128 tasks and measured 0.49× (2× *slower*) on tree overhead alone.
- **Crossover sweep added to the bench** (`bench/bench.cpp` → `BenchParallelForCrossover`), so the
  next time the dispatch path changes this constant can be re-derived instead of re-guessed.
- **C++17 compatibility is now enforced by the build**, not merely asserted: `deploy_lib.bat`
  compiles with `/std:c++17` (was `/std:c++20`). The scheduler never needed C++20 - it started
  compiling as such only because a newer Visual Studio defaults that way. Verified clean.

## 2026-08-02
- **Stealing policy made P/E-core aware.** A thief now prefers victims of its own core class, and a
  task with an explicit P/E preference is only stolen by a matching-class thief - so placement
  survives work-stealing instead of being undone by the first steal.
- **Pool sizing policy documented and settled**: reserve one core per *shipped, persistent, busy*
  thread - `hw-2` when the audio device thread is present, `hw-1` otherwise. The rule is about
  measured busy time, not thread existence; a thread that exists but sleeps costs nothing to
  schedule around. Transient oversubscription is accepted deliberately and is not a bug to chase:
  driver and OS threads the process does not own will always exist, profilers count spin-waiting
  workers as running, and neither is actionable from inside a scheduler.

## 2026-08-01
- **`Task::corePref` - P-core / E-core placement.** `CorePref::{Default, P, E, Wide}`, living in what
  was tail padding on `Task`, so it costs no extra bytes. **Priority and placement are deliberately
  orthogonal**: priority controls queue *order* only, placement is decided solely by `corePref`.
  Enforced at push placement (`PickNextWorker`) and at steal time; an explicit core affinity
  overrides it, being the stronger and more explicit request. Class-based routing is **opt-in and
  dormant** - everything is `Default` until a profiled caller asks otherwise, so behaviour is
  unchanged for existing code.

## 2026-07-23
- **`ParallelFor` dispatch is now hybrid flat/fork-join.** The flat path has the caller spawn every
  chunk serially - fine, and ~14% faster, when there are few tasks - but its O(#tasks) serial
  `CreateTask`+`Push`+`NotifyWorker` on one thread collapses at fine grain (~8× slower at ~15k
  tasks, with each notify also taking the worker mutex from the lost-wakeup fix). Fork-join spreads
  task *creation* across the pool and wins decisively past a few dozen tasks. Crossover: ~2 tasks
  per worker.
- **`TaskAllocator` optimized.**

## 2026-07-22
- **Batch stealing removed** (`stealbatch`, plus a follow-up sweep for vestigial remnants) - stealing
  is single-item now. Downstream note: this is what makes the promotion removal below correct.
- **Age-based promotion REMOVED** - this supersedes the mechanism described in the 2026-07-15 entry
  below. (It was briefly re-enabled on 07-21, which is why the history shows it twice; removing batch
  stealing the next day is what settled the question.) It became vestigial once stealing went
  single-item: a steal un-starves a loPri task
  immediately, so the 50 ms promotion timer never had anything left to rescue. The steal-fairness
  window (a forced loPri scan after 8 consecutive hiPri steals) is the real anti-starvation
  mechanism and remains. Do not re-add promotion without a profile showing starvation the fairness
  window misses. Downstream forks: dropping it is safe; keeping it is merely dead weight.

## 2026-07-20
- **Fiber-aware synchronization toolkit** (SchedulerMutex, SchedulerSemaphore,
  SchedulerConditionVariable): three complementary primitives built on spinlocks and
  fiber suspend/resume. Core design: fibers suspend on contention (yielding thread to
  other work), fast jobs spin-wait with `TryRunStolenFastJob()` to remain productive
  during lock acquisition. SchedulerMutex adds priority inheritance (boosts held-lock
  owner, prevents inversion). SchedulerConditionVariable uses transient semaphores for
  zero-allocation waiter coordination. All three tested end-to-end; scheduler internals
  remain on std::mutex (low-contention admin locks, simpler init/shutdown).

## 2026-07-17
- **Fiber stack guard pages** (`FiberStackArena::AllocateStack`): the lowest 4KB page of
  every fiber stack is now `PAGE_NOACCESS`. Stacks are carved contiguously from one
  reservation, so an overflow previously wrote straight into the NEXT fiber's stack with
  no fault - silent cross-fiber corruption. It now raises an access violation at the
  faulting instruction. Costs 4KB of usable stack per fiber (standard 64KB → 60KB usable,
  heavy 512KB → 508KB) and one `VirtualProtect` per fiber at pool init; zero per-switch
  cost. No recovery/stack-growth - a guard hit is a deliberate hard fault. Porters note
  (Linux): equivalent is `mprotect(PROT_NONE)` on the lowest page.

## 2026-07-16
- **TaskDAG runtime is now genuinely zero-allocation**: the per-fire heap-allocated
  `TaskFinishedContext` (one `new`/`delete` per node per submission - the DAG's only heap
  traffic) is gone. The saved fn/data/owner now live embedded in `TaskNode` itself, which
  always outlives the completion trampoline (EBR-deferred retire). API-neutral; downstream
  code that referenced `TaskDAG::TaskFinishedContext` directly (it was private) is unaffected.
- **[CRITICAL] Fiber epoch ABA guard** (`581c25e`): `GlobalFiberPool::ReturnBatch` now
  clears each fiber's `localEpoch` to `SIZE_MAX` before re-enqueueing it. Without this, a
  fiber recycled while its EBR slot still held a stale epoch (e.g. an `EpochGuard` skipped
  by an exception or early exit) could pin `MinActiveEpoch()` or ambiguously alias a new
  epoch entry - corrupting epoch-based reclamation decisions (use-after-free class).

## 2026-07-15
- **Starvation prevention** (`8555cbd`): three complementary mechanisms -
  age-based promotion (loPri tasks waiting > 50 ms are promoted to hiPri), steal fairness
  (a forced loPri scan after 8 consecutive hiPri steals), and `SchedulerMutex`
  (priority-inheritance mutex: boosts a contended lock holder, restores on unlock).
  Without these, a sustained hiPri flood can starve loPri work indefinitely, and a hiPri
  task waiting on a loPri lock holder can deadlock-by-starvation (priority inversion).
- README rewritten: full documentation of execution modalities, integration contracts,
  starvation prevention, and TaskDAG (`a5b465e`).

## 2026-07-13
- **[CRITICAL] Deadlock fix in the WaitGroup wake path** (`c3ca50d`, `2103903`):
  WaitGroup now wakes ALL waiters. Before this, multiple tasks waiting on the same
  WaitGroup could leave some suspended forever after the counter hit zero.
- API rename: `PushFork` naming consolidated; `WaitGroup`/`WaitFor`/`Task` updated
  (`ffc1fb7`). Downstream code written against the old names needs the rename.

## 2026-07-12
- **Task shrunk 96 → 64 bytes** (`112de38`): exactly one cache line, enforced by
  `static_assert`. Completion callbacks moved out of Task into TaskDAG's transient
  `TaskFinishedContext` (trampoline via `OnTaskFinishedWrapper`). Measurable frame-rate
  gain under load. Note: the virtual `~Task` destructor is load-bearing (slab reclaim
  calls `t->~Task()` through the base pointer) - do not remove the vptr to save 8 bytes.
- Post-cleanup benchmark: task enqueue/dequeue latency at **6.3 µs** (see `bench/`).

## 2026-07-08
- **[CRITICAL] Fork codepath fix** (`b9489e3`): forked tasks could take the wrong
  execution path after an earlier refactor. Fork-join (`PushFork` + `WaitFor`) was
  unreliable without this.

## 2026-07-05 … 07-06
- `StealBatch` implemented (`009e35a`): bulk fiber steal from the global pool.
- Project renamed JGL-Scheduler → JLib (`ae0fe00` and related).

## 2026-07-04
- Steal heuristics now topology-aware (`f169f0c`): SMT-sibling → LLC-local → global
  random, from actual CPU topology detection.
- Fast path without context switch for `fastJob` tasks (`6e5faa3`), with stricter
  contract checks and real error messages on misuse.
- Fork tasks execute fastJobs inline and only dump fiber tasks for stealing (`01216fb`).

## 2026-07-03
- **[CRITICAL] LockFreeList destructor leak** (`fd59e89`): the destructor leaked every
  non-sentinel node. Long sessions exhausted the TaskAllocator after ~48 minutes of
  gameplay and died with "allocator exhausted" - looked like a clean exit(0) from the
  outside. Any long-running process on an older revision will eventually hit this.

## 2026-07-01
- TaskDAG: main-thread-affinity nodes (`CreateMainNode`) for renderer integration
  (`bd68ec2`); whoever awaits a graph containing one MUST use `WaitForMain`.
- `DirectEvent` replaces the old Event (`67bdd87`).
- Main thread is a pure waiter in `WaitFor`, steals the first chunk in `ParallelFor`
  (`413922f`).

## 2026-06-30
- Inbox (MPSC handoff) drains before forked tasks (`deceabe`) - ordering fix for
  cross-thread pushes.

## 2026-06-28
- **[CRITICAL] ThreadLocalCache fiber duplication** (`8369d3a`): a fiber could be handed
  to two workers simultaneously (the "ParallelFor heisenbug" - intermittent corruption
  under parallel loads). 
- **[CRITICAL] ContextSwitch saves full register state** (`355fa8b`): the assembly
  context switch previously saved a partial state; optimized builds could clobber
  callee-saved/XMM registers across a suspend.

## 2026-06-27
- TaskDAG logical OR gates (`8d1b23f`): `CreateGate` supports AND/OR dependency
  composition, nestable.
- EpochManager made lock-free (`c436c68`); fiber epoch participation fixed (`bf44d3a`).

## 2026-06-20
- **v1.0** (`1b1a4d0`): first tagged version of the fiber-based rewrite.

## 2025-10 … 2025-11 (pre-fiber era)
- Original thread-pool incarnation: core pinning, group scheduling, thread-safe RNG,
  periodic task cancellation, two major rewrites (`ad9422c`, `5d63e3c`, `9b2e309`).
  The fiber/work-stealing architecture above replaced this design entirely.
