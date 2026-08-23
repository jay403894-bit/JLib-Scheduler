# Changelog

Correctness fixes are marked **[CRITICAL]** with a note on what breaks without them -
downstream users (forks/ports) should treat those as must-pull.

## 2.11.1 - 2026-08-23

**[CRITICAL] 2.11.0 does not compile on Linux/GCC. Use this instead.**

`TaskAllocator.h` gained an unconditional second-instance guard in 2.11.0 that calls
`fprintf`/`fflush`/`abort` without including `<cstdio>` or `<cstdlib>`. MSVC and AppleClang pull them
in transitively, so Windows x64, Windows ARM64 and macOS arm64 all passed; both GCC legs failed to
build. The canary path has used `std::fprintf` the same way for a long time and never surfaced it,
because nothing in CI builds with `JLIBSCHED_ALLOC_CANARY` -- an unconditional use was all it took.

**Also: `build-corobench/` was committed by accident** -- 6.3 MB of `.obj`, `.lib` and `.exe` swept in
because the new diagnostic build tree had no matching `.gitignore` entry. Untracked here, and
`.gitignore` now carries a `build-*/` catch-all rather than one hand-maintained line per tree. The
explicit entries stay for documentation, but narrow-by-default is only worth it for the `x64/` folders
some JLib repos commit deliberately -- a CMake tree is never worth committing, so there is no reason
to keep re-learning this per directory. The files remain in history for 2.11.0; only future clones of
this commit onward avoid re-downloading them.

## 2.11.0 - 2026-08-23

**[CRITICAL] Constructing a second `TaskAllocator` now aborts with a diagnostic instead of corrupting
the heap.** `local()` -- the per-thread free-list cache every `Alloc` and `Free` routes through -- is
a function-local `static thread_local` inside a STATIC member function, so it is one cache per thread
for the entire CLASS rather than per instance. Two live allocators feed one free list and each hands
out the other's slots. Nothing documented that, nothing prevented it, and the symptom is heap
corruption surfacing somewhere unrelated.

Found the hard way: pooling coroutine frames from a second `TaskAllocator` died with `0xC0000374`
immediately. The guard is unconditional rather than behind `JLIBSCHED_ALLOC_CANARY` -- it runs once
per allocator for the life of the process, so it costs nothing measurable, and the failure it
prevents is undiagnosable from the symptom. `TaskAllocator` is also non-copyable now, which it should
always have been for the same reason. If a second slab is ever genuinely wanted, the fix is to make
the cache per-instance, not to delete the guard.

**New: `bench/coroutine_bench.cpp` (`SchedulerCoroBench`), and the answer it was built for is NO.**
The question was whether coroutine frames should come off a slab instead of global new. Measured
rather than argued, and the interesting numbers are not the verdict:

| | ns/item | vs native |
|---|---:|---:|
| native task | 1192.5 | -- |
| coroutine | 1291.0 | 1.08x |
| coro + suspend | 1821.3 | 1.53x *(bimodal, 367..1851 -- do not quote alone)* |
| Lazy await | 31.1 | 0.03x |
| **frame alloc+free** | **28.1** | 0.02x |

The whole third execution mode costs **8% over a plain Native task** on the same push/steal/complete
path, because resume is just the `fn(data)` call the worker already makes. And **`Lazy` awaited
inline is 41x cheaper than a spawn** (31 ns vs 1291 ns) -- the lazy, run-on-the-awaiting-worker,
symmetric-transfer design paying off, and the argument against ever making `co_await Child()` fork.

Frame allocation is ~2% of a spawn, so pooling was tested on the axis that could actually justify it
-- **allocator contention** -- with pooled and unpooled arms interleaved in one process (the pool is
runtime-switchable precisely so they can be, rather than compared across two binaries):

```
threads   pooled ns   global ns   speedup
  1         1314.1      1310.7     1.00x
  2          819.4       783.1     0.96x
  4          528.0       539.0     1.02x
  8          356.6       346.6     0.97x
 16          420.2       360.8     0.86x
```

Still no, and the decisive column is `global`: its per-item cost FALLS as threads are added
(1310 -> 347) then flattens. A contended allocator's cost RISES with thread count. There is nothing
to remove; the flattening is the scheduler path saturating. The test pool has no cross-thread
rebalancing and this workload migrates one way, so it exhausts past 2 threads and rows 4-16 measure a
partly-exhausted pool -- rows 1-2 are the clean ones and are a wash. The conclusion rests on the
global column showing nothing to fix, not on the pooled column being good.

**New diagnostics, both OFF by default:** `JLIBSCHED_CORO_STATS` records a histogram of real
coroutine frame sizes (640,008 frames measured: 75% <=64 bytes, largest 224 -- the sizing input if
pooling is ever revisited), and `JLIBSCHED_CORO_POOL` enables the experimental frame pool. Neither is
in a normal build; when both are off, the promise types declare no allocation functions at all, so
the compiler is left free to elide frames entirely.

**Also fixes the instrumentation itself**, which initially measured nothing: the `operator new` hook
was placed on the `Coro` class rather than on `Coro::promise_type`. A coroutine frame's allocation
functions are looked up in the PROMISE type's scope, so on the wrapper they are simply never called.
Caught because "peak 1 live" is impossible for a benchmark that holds 20,000 coroutines at once.

## 2.10.0 - 2026-08-23

**[CRITICAL for anyone using coroutines] The deque's steal tag was lying about `TaskType`, and
fiberless helpers could not steal coroutine work.** Two halves of one problem, both introduced by
2.8.0.

`TaskDeque` packs steal-vetting bits into the spare low bits of the stored `Task*` so a predicate can
vet a candidate without dereferencing memory the thief has not claimed. `TaskType` had **one** bit
there -- `type == Native ? 0x4 : 0` -- from when the enum had two values. 2.8.0 added a third, so
every coroutine task round-tripped out of the tag as **`TaskType::Fiber`**. Harmless purely by luck:
the single predicate reading it asked for `== Native`, so the lie produced a decline rather than a
wrong steal. It would have become a real bug the instant any predicate asked for `== Fiber`. Now two
bits, with a `static_assert` so a fourth `TaskType` cannot reintroduce it quietly.

**And the decline was itself wrong.** A coroutine is resumed by calling a function on whatever stack
is current -- exactly like a Native task -- so a fiberless caller can run one perfectly well; only a
Fiber-backed task genuinely cannot be claimed. The predicate is now "does not need a fiber" rather
than "is Native", so a blocked main thread helping through `WaitFor` or a `SchedulerMutex` spin no
longer idles next to coroutine work it is able to do.

**Those two changes are one change, and the second half is what makes the first safe.**
`TryRunStolenNativeTask` ran `Execute()` and then unconditionally decremented the WaitGroup,
`DestroyTask`d and `Free`d -- correct for a Native task, a **double free** for a coroutine, which is
owned and released by the C++20 side. It now reads the type BEFORE `Execute()` (a completing
coroutine frees its own Task, so `task` may be dangling the instant `Execute()` returns) and skips
completion for coroutines, matching `Worker()`'s fast path exactly.

Verified by negative control rather than by reasoning: with the guard removed, the coroutine suite
dies with `0xC0000374` (heap corruption) and three failed checks -- which also proves the path is
genuinely exercised, since `WaitFor` on the main thread is what steals those coroutine tasks.

`TryRunStolenNativeTask` keeps its name despite now stealing coroutines too; it is public API and not
worth churning. Read "Native" there as "a task that does not require a fiber".

## 2.9.0 - 2026-08-23

**The stale-library guard can now see `Task` layout changes. It could not, and 2.8.0 was exactly the
change it would have missed.**

Packing the six flag bytes into one moved every flag while leaving `sizeof(Task)` at 64 -- they had
been followed by padding either way -- and size is what `AbiComponents` compared. A translation unit
compiled against 2.8.0's headers and linked to a 2.7.0 library would have read `trivialDtor` from the
wrong bit, silently skipping `~Task` and leaking every lambda capture, or running a destructor it
should not have. The guard would have reported everything in agreement, which is worse than no guard
because it reports success.

`sizeTask` is now `taskLayout`, a FINGERPRINT rather than a size. **No field was added**, and that
constraint drove the design: the field count and types are the guard function's own ABI, so an older
library would return fewer bytes than the caller allocated and the guard would be comparing against
uninitialized memory to decide whether to fire. Widening an existing field's meaning costs nothing.

The fingerprint hashes size, alignment, the packed flag block's width, every named member's offset,
and -- the part that closes the actual hole -- **each flag's bit position, observed rather than
assumed**. `sizeof` cannot see two 1-bit flags swapping places, but that relocates bits every
consumer reads. Each field is set alone in a zeroed copy of the block and the resulting bytes hashed,
which encodes exactly which byte and which bits it occupies.

**A second hole, closed structurally rather than by discipline.** `detail::TaskFlagPacking` was a
hand-written mirror of Task's flags; repack the real fields and forget the mirror, and the
fingerprint misses it. Both now expand one `JLIB_TASK_FLAG_FIELDS` macro, so they cannot drift.

VERIFIED IN BOTH DIRECTIONS, not merely reasoned about. With the library built and saved, two flags
were reordered in the header -- `sizeof(Task)` still 64, `sizeof(TaskFlagPacking)` still 1, every
named member at an identical offset -- and a consumer compiled against it and linked to the stale
library aborted as it should:

```
[JLib::Scheduler] FATAL: this translation unit was compiled against DIFFERENT ...
  Task layout (size/align/offsets)     751614434 vs 2509107104
```

Restoring the header and recompiling against the SAME library exits 0, so it detects the change
without firing on agreement.

Cost, stated plainly: a mismatch now prints two opaque hashes instead of "64 vs 72". The only useful
response to any mismatch was always the one the message already gives -- rebuild -- but the
diagnostic is less readable than it was.

**Also fixes two comments that 2.8.0 left stale**, in the release-before-last's own style: the
`TaskType` block still described `Task::coroDone`, a field that was removed when the coroutine
ownership model changed and the worker stopped completing coroutine tasks at all; and the new
three-mode description had been stacked on top of the old two-mode one rather than merged, so the
enum carried two overlapping explanations. No code changed for either.

## 2.8.0 - 2026-08-23

**A THIRD EXECUTION MODE: C++20 COROUTINES, AS AN OPTIONAL HEADER. The core stays C++17.**

`TaskType::Coroutine` joins `Native` and `Fiber`. A coroutine is resumed through the same
`fn(data)` call the worker already makes for every task -- `fn` is a trampoline, `data` is
`coroutine_handle::address()` -- so `<coroutine>` and C++20 never enter the library. `Task` did not
grow by a byte. Everything C++20 lives in `include/Coroutine.h`, which nothing else includes, and the
boundary is checked by the build rather than by discipline: the `Scheduler` target is compiled as
C++17, so anything leaking out of that header fails to compile. Tests for it are behind
`-DJLIBSCHED_COROUTINES=ON`, off by default, so requiring C++20 to build the suite cannot quietly
make C++20 the project's real floor.

New in that header:

- **`Coro` + `Spawn()`** -- fire-and-forget coroutines scheduled on the pool, reporting completion
  through a `WaitGroup`.
- **`Reschedule`** -- `co_await` it to yield the worker and continue on whichever picks the task up.
  It is also the minimal awaiter: every I/O awaiter will have this exact shape, differing only in
  *when* the task is re-pushed.
- **`Lazy<T>`** -- a coroutine that returns a value, awaitable from another coroutine, with
  `SyncWait()` for non-coroutine callers. No future type is involved and none is needed: the promise
  *is* the shared state, and `SyncWait` reuses the existing `WaitGroup`. Exceptions are stored as an
  `exception_ptr` and rethrown at the awaiting `co_await`, so `try`/`catch` around an await works.
- **Superset lock and semaphore.** `SchedulerMutex` and `SchedulerSemaphore` were EXTENDED, not
  duplicated -- there is still one lock, not a third choice. Their waiter queues now hold a `Waiter`
  that is either a fiber (resume it) or a coroutine task (re-push it), so a fiber and a coroutine
  contending on the same object is well defined. Bare threads still never queue: they cannot suspend,
  so they keep spinning on `Try_Lock` and helping. None of this needed C++20 -- a coroutine waiter is
  a `Task*` and re-arming it is a `Push` -- so the superset logic is in the C++17 core and only the
  `co_await LockAsync(m)` spelling is in the optional header.

**The design decision worth knowing before touching any of it: THE WORKER NEVER COMPLETES A
COROUTINE TASK.** The obvious alternative -- a "finished" flag the worker checks after resuming --
was written first and is racy. A coroutine that suspends becomes re-pushable *inside* `resume()`, so
a second worker can pick it up, finish it and free it while the first is still deciding what it saw;
both then observe "finished" and both free. No flag read closes that. Sole ownership by the C++20
side removes the race instead of narrowing it, and removed the flag with it.

`Event` and `DirectEvent` deliberately gain NO coroutine support. They are arbitrary-point
suspension -- a fiber parks from any stack depth because `ContextSwitch` takes the whole stack with
it -- and a coroutine suspends only where a `co_await` is written. The coroutine analogue of an event
is an awaitable, a different construct, not a polymorphic `Event`.

Two bugs found by the tests rather than by reading, both recorded at their sites: a nested `Lazy`
that suspended re-pushed a task still pointing at the *root* handle, resuming the wrong coroutine and
producing wrong VALUES rather than a crash (fixed by `detail::ArmResume`, which also forced the
trampoline to use `coroutine_handle<>` rather than a typed handle, since a typed one makes every
nested resume undefined behaviour that happens to work); and a dangling-lambda-coroutine in the test
itself, where a temporary `[&]{...}()` closure passed to `Spawn` died at the end of the
full-expression while the coroutine was still suspended.

The deep-await-chain test exists for one reason: `final_suspend` returns the continuation's handle so
the compiler tail-calls into it, making a chain of N awaits cost O(1) stack. Resuming the
continuation directly instead is O(N) and shows up only as a stack overflow under depth. 100,000
nested awaits pass; nothing shallower would catch it.

**REBUILD REQUIRED, and the stale-library guard will NOT tell you.** `Task`'s layout changed while
`sizeof(Task)` stayed 64, and size is what the guard compares -- so a translation unit compiled
against these headers and linked to an older `Scheduler.lib` would read the flag bits from the wrong
offsets and say nothing. `trivialDtor` is the field that makes this dangerous rather than merely
wrong: read from the wrong bit, the completion path either skips `~Task()` and leaks every lambda
capture, or runs a destructor it should not have. See the note at the end of this entry.

**`Task`'s six flag bytes are now one packed byte.** `hiPri`, `requiredSize`, `type`,
`priorityBoost`, `corePref` and `trivialDtor` occupied one byte each at offsets 48-53, with two
bytes of tail padding. As bitfields they fit in a single byte at 48, which frees 49-55 -- including a
4-byte, 4-aligned slot at offset 52. `sizeof(Task)` is unchanged at 64 (still exactly one cache
line) and so is the 192-byte lambda capture budget, because those bytes were padding either way.

The point is the reclaimed slot, not the byte saved: a 32-bit field can now be added for the
cancellation-token index the I/O runtime work needs, without pushing `Task` onto a second cache line
or shrinking what callers may capture. **Nothing claims it yet, deliberately** -- an unused field is
what `stopFlag` was, and it was removed for that reason.

Two traps are commented at the site because neither is visible in the code:

- **Bitfields cannot carry default member initializers before C++20** (P0683R1), and this library is
  C++17. Both constructors now initialize all six explicitly; one that forgets leaves a field
  *indeterminate*, not zero.
- **Adjacent bitfields in one allocation unit are a single memory location**, so two threads writing
  logically independent flags here is a data race. Safe today only because all six are written before
  the task is published and never again. Re-wiring lock priority inheritance would break that -- it
  writes `hiPri` from a thread other than the one running the task, while push/steal read `hiPri` --
  at which point those two must leave the block.

Packing bitfields of *different declared types* into one unit is a compiler behaviour, not a standard
guarantee (MSVC does it when the types are the same size, which is why all six are 1-byte types). If
a compiler declines, the block silently grows back, the reclaimed slot stops existing, and
`sizeof(Task) == 64` still holds -- so `detail::TaskFlagPacking` exists purely to `static_assert` the
packing actually happened.

Measured against three baseline runs, medians: throughput/1p 0.88 -> 0.94 M/s, PushBatch 15.79 ->
15.60 M/s, latency 4.39 -> 4.48 us, frame DAG 21.24 -> 20.87 us/graph. Every delta is inside the
run-to-run spread and they point in opposite directions, which is noise, not effect: the added
mask-and-shift is below what a multi-binary comparison can resolve here (~10%).

**`SchedulerMutex` has not inherited priority since 2026-07-20, and its comments claimed it did.**
Boosting the holder on contention arrived in `8555cbd` ("implemented starvation prevention") and its
only call site was deleted five days later in `21719ac`, the rewrite that turned this from a spinlock
into the suspend-or-help lock it is now. `BoostTaskPriority` still has no callers anywhere in the
library or its consumers, so `priorityBoost` is permanently 0 and `UnboostTaskPriority` -- which
`Unlock()` does still call -- always takes its false branch.

The removal was correct and the class comment now says why, so the boost does not get re-added as a
fix for a hang it cannot cause: priority inversion needs a high-priority waiter to starve the holder
of CPU, and nothing here can. `hiPri` is queue order only, every worker runs at the same OS priority,
and a task that has STARTED owns its worker until it yields -- so hiPri work cannot deschedule a
running lock holder. What made inversion real before was that waiters SPUN and genuinely competed for
a core; suspending and helping both removed that. The residual case (holder is a suspended fiber
whose resume sits in a loPri queue under hiPri flood) is bounded by `kStealFairnessWindow`. Same
shape as the age-based promotion removed once single-item stealing made it redundant: a mitigation
outliving its premise. No code changed.

**Known gap, not fixed here:** `AbiComponents` compares `sizeof(Task)`, so it cannot see a `Task`
layout change that preserves size -- exactly this one. Folding `sizeof(detail::TaskFlagPacking)` into
the reported value would close it, but that changes the guard's own return value and wants doing
deliberately rather than as a side effect of this change.

## 2.7.0 - 2026-08-23

**A fast spin before the contended bare-thread lock path was built, measured, and REVERTED. Do not
re-propose it.** The idea: `SchedulerMutex::Lock` and `SchedulerSemaphore::Wait`, on a bare thread,
call `ContendedSpinStep()` on every failed `Try_Lock`/`Try_Wait`, and that step walks steal
candidates across every deque. Trying a handful of plain `CpuRelax` retries first "obviously" avoids
paying for a steal scan when the holder is about to release in a few cycles. It is intuitive, and it
is wrong: measured against the shipped behaviour, it is a **large regression**, monotonic in the spin
count.

The decisive numbers (tiny critical section, 8 bare contenders, saturated pool, 8 workers):

| spin | p50 | p99 | lock acq/s | pool tasks/s |
|-----:|----:|----:|-----------:|-------------:|
| **0 (shipped)** | **0 ns** | 38,900 ns | **17,789,590** | **304,750** |
| 64 | 4,400 ns | 61,900 ns | 715,976 | 195,384 |
| 1024 | 5,000 ns | 68,200 ns | 625,812 | 171,135 |

Reproduced at 8 and 31 workers. 0 wins lock latency, lock throughput *and* the pool's own throughput
simultaneously, so it never became the latency-versus-throughput trade it was built to arbitrate.
The likely mechanism: `ContendedSpinStep` is not merely a slower retry, it is **backoff**. A tight
`CpuRelax` loop re-runs `Try_Lock`'s `spinLock.test_and_set` at full rate, and every one of those is
a write to the same cache line the *holder* needs in order to finish and release -- so spinning
harder starves the thread being waited on. That also explains the rows with no background load,
where 0 still wins by ~2x despite its steal attempt always failing and running nothing.

Net effect on shipped behaviour versus 2.6.0: **none.** The bound is 0 and the comparison folds away
at compile time, so the generated loop is what it always was. What is new is that it is now a
measured 0 rather than an unexamined one.

**New: `bench/lock_contention.cpp` (`SchedulerLockBench`), the first benchmark of the lock
primitives in this repo** -- `bench.cpp` covers push/steal/`ParallelFor` and never takes a
`SchedulerMutex`, so the constant above had nothing to be tuned against even in principle. It sweeps
critical-section length (0 ns / 200 ns / 2 us / 50 us) x contender count (1/2/8/16) x caller kind
(bare thread vs fiber) x background load (off/on), and reports lock latency percentiles, lock
throughput and completed pool work together, because an arm that improves one at the expense of
another has not won.

Two controls are built into it, and the first version of the harness was **discarded because of
them**. That version built one binary per spin value; its A/A control (same source and flags, built
twice under two labels) showed process-to-process drift with a p90 of 52% and a max of 118% on lock
throughput, and its fiber control -- which cannot be affected by the spin count, since fibers suspend
and never enter that path -- moved up to 246%. No plausible effect survives a floor like that, and
more repetitions do not fix variance that lives *between* processes. The arms now rotate inside a
single process, round by round with a shifting start offset; the A/A gap fell to ~0.3% and the fiber
control to within +-1.3%.

**New diagnostic option `JLIBSCHED_TUNABLE_FAST_SPIN` (OFF).** Makes the bound settable at runtime so
that comparison stays reproducible, and gates the bench target -- which `#error`s without it, since
without a runtime-settable bound every arm would be the same code and the benchmark would silently
compare a value against itself. Same OFF-by-default discipline as `JLIBSCHED_STEAL_STATS` and
`JLIBSCHED_LATENCY_STATS`.

## 2.6.0 - 2026-08-21

**README: `CorePref::P`/`::E`'s reliability is now documented as coupled to `AffinityPolicy`, not
just to whether the CPU is hybrid.** `isPCore` is a label assigned once per worker at pool startup;
under `Hard` it stays true for the process's life, but under `Ideal` (the shipped default) the OS
may migrate the thread and the label can go stale mid-run. The mode where the hint is maximally
trustworthy is the one already measured and rejected as the default for its own cost (~45% wake
latency, ~2x frame DAG -- see `DESIGN.md#worker-binding`). A real gap between hint and guarantee,
not boilerplate hedging, and now written down where the other `CorePref` caveats already live.

**Also fixes `## Versioning`**, which had been silently stuck at "2.0.0" through five real releases
since (2.1.0 through 2.5.0) -- found while touching the section next to it.

No code changed. Closes out this stretch of work: the `TryRunStolenNoFiberTask` rename gap (2.1.0),
the measured `PushBatch` immediate-drain win (2.2.0), the latency-breakdown diagnostic that found
the cold-park-vs-warm-worker distinction (2.3.0), the `fiberresume_model.tla` verification (2.4.0),
and `PushBatch` finally preserving `corePref` (2.5.0) are all shipped, green, and documented
accurately as of this release.

## 2.5.0 - 2026-08-21

**`PushBatch` now takes an explicit `CorePref pref = CorePref::Default` parameter**, closing the
gap flagged (but deliberately deferred) in 2.2.0 and documented in the 2.3.0/2.4.0 README notes:
every task in a batch used to be placed at `CorePref::Default` regardless of what was asked for,
because `PickNextWorker()` was always called with no argument. Both call sites inside `PushBatch`
(the explicit-cpuaffinity fallback and the segment loop) now call `PickNextWorker(pref)` instead.
`PushBatch` still assumes a batch is homogeneous in class -- it does not scan `tasks[]` for mixed
`corePref` values itself, deliberately: most callers (`ParallelFor`, any already-homogeneous
submission) already know their batch is one class, and a scan-and-partition on every call would tax
that common case to serve the one caller that actually has mixed input. Purely additive to the
signature (new trailing defaulted parameter) -- every existing call site compiles unchanged.

**That one caller is `Thread.cpp`'s `drainInbox`** (the 2.2.0 immediate-task inbox drain), the
actual place a real, mixed-corePref backlog can land in practice. It now does an in-place 3-way
partition of the already-popped `batch[]` (Dutch-flag style: P / Default-Wide-Any / E, since the
latter three all route identically -- see `CorePref`'s own comment), then calls `PushBatch` once
per non-empty bucket with the matching `pref`. Zero extra allocation, and the realistic common case
(everything `Default`, which is 100% of shipped callers today) still costs exactly the one
`PushBatch` call it always did -- the partition only does extra work when there's actually a mix to
split.

`SchedulerImmediateDrainTest`'s 150-task backlog now cycles `Default`/`P`/`E` every third task
instead of being uniformly `Default`, specifically to exercise the new partition. What it verifies,
and the limit of what it CAN verify: every task still runs exactly once regardless of the mix, on
any hardware -- that's the honest, portable check. It does NOT (and cannot, without a real hybrid
CPU on the test runner) assert that a `P` task actually lands on a P-class worker, since on a
non-hybrid machine every worker reports as class P and the distinction is unobservable from the
outside. 10/10 stress runs clean; full suite green; bench sanity-checked with no regression (latency
4.36us, matching the pre-existing 4.3-4.9us range).

## 2.4.0 - 2026-08-21

**New model: `tests/verify/fiberresume_model.tla`**, checking `Fiber::ResumeQueueless()`'s own CAS
race -- a fiber publishing `WANTS_SUSPEND` racing between the worker's own CAS to `SUSPENDED` and
any other thread's CAS to `SUSPEND_SIGNALED` (a plain `Fiber::Resume()`, or `Event::SignalAll`'s
batched path via `RequeueResumedBatch`). This is genuinely new coverage, not a duplicate of the four
existing GenMC models: `sleepwake_model.c` checks the OS-thread park/wake predicate,
`fiberwait_model.c` checks `SchedulerMutex`/`Semaphore`/`CondVar`'s queue-then-mark-parkable
ordering (the 1.3.4 deadlock), `event_model.c` checks the waiter-stack data structure -- none of them
check the `FiberStatus` CAS race itself, which all three sit on top of.

**A different tool, not a stronger one.** The four GenMC models explore the actual compiled C under
the real C11 memory model; this is TLA+/PlusCal, a hand-written abstraction checked by TLC's
explicit-state search. Verifies the protocol is sound, not that the C++ correctly implements it, and
treats every transition as a clean atomic CAS rather than modeling real memory orderings. Documented
plainly as design-level evidence, not GenMC-grade proof, in both the file's own header and the
README. Clean at `MaxFibers=3` (828 distinct states) and re-run clean at `MaxFibers=5`.

Started from a first draft generated by DeepSeek. Two real PlusCal legality errors blocked
translation (a variable assigned twice in one atomic step; a label inside a `with` block, which
PlusCal never allows). More importantly, even once fixed superficially, the original draft's
`Worker` process had exactly one label covering its entire loop body -- meaning the race it was
built to check could never actually happen in the model, since PlusCal treats everything between
labels as one indivisible step. Rewritten with real interleaving points (`Steal` / `RunFiber` /
`Resolve`) before the first meaningful run.

**README's "Model checked" section was stale independent of this change** -- fixed while touching
it. It documented only 2 of the 4 GenMC models that actually exist in `tests/verify/`
(`fiberwait_model.c` and `sleepwake_model.c` were both missing, along with their real, already-
recorded results). All four GenMC models plus the new TLA+ one are now listed.

## 2.3.0 - 2026-08-20

**New diagnostic: `JLIBSCHED_LATENCY_STATS`, built to answer where the README's 4.3 µs Sleep
round-trip actually goes** -- the OS kernel wake, or `Worker()`'s own loop order (it checks the
local deque and runs a full steal scan before ever looking at the inbox a cold wake was actually
for; see 2.2.0's `PushBatch` change to the same drain). Same convention as `JLIBSCHED_STEAL_STATS`:
off by default, three global timestamp marks (Wake, PreSteal, Found) compiled to nothing unless
built with `-DJLIBSCHED_LATENCY_STATS=ON`, and `BenchLatency` prints a breakdown only when it's on.

**The answer: the loop order costs a real but small ~0.30 µs (~6% of the round trip); the OS wake
is ~85% of it (4.00 µs of 4.89 µs measured in an instrumented build).** Confirmed on a strict
monotonicity check across the marks (reject any sample where Wake/PreSteal/Found don't strictly
increase in order) -- 2,245 of 20,000 samples passed, which is itself the interesting finding: most
round-robin iterations DON'T reach a clean cold-wake-to-found sequence at all, because round-robin
spreads submissions across every worker, and any one worker is idle for the other (workers-1)
iterations -- long enough to fully park every time.

**Pinning the same round trip to one worker instead of round-robin dropped it 5.9x, from 4.89 µs to
0.83 µs**, with the clean-sample rate jumping from 11% to 50%. That worker is usually still
mid-backoff, not yet parked, when the next task lands, so most hits skip the kernel wake entirely.
Neither number is wrong; they measure different things -- 4.3 µs is a genuinely idle pool's wake
cost, 0.83 µs is a worker still warm from the last task. README footnoted with both, so the
headline figure isn't read as a hard floor on submission when it's specifically the cold-park cost.

No production code changed. `Worker()` gained four `JLIBSCHED_LATENCY_MARK` call sites (the macro
expands to `((void)0)` when the flag is off, so a normal build costs nothing) at the two park-exit
points, the start of the steal scan, and the two inbox-found points.
Wired into CI the same way `JLIBSCHED_STEAL_STATS` is: one build-and-run on the cheapest runner, so
the diagnostic can't silently bit-rot between the rare times someone actually needs it.

## 2.2.0 - 2026-08-20

**Worker()'s immediate/fork inbox drain now dispatches via one `PushBatch()` call per round
instead of a per-task `Requeue()` loop.** When a worker is about to pin to a persistent
`PushImmediate` task (an audio mixer, a network poll loop), it must first empty its own inbox --
nothing else can steal from an inbox, only its owner can drain it, and once pinned it never will.
That drain used to call `Requeue()` once per task, and each call pays its own `PickNextWorker` +
spin-check + single-item push + `NotifyWorker` (a mutex lock) -- the exact per-task notify cost
`PushBatch` was built to amortize for every OTHER large submission path in this library, just never
wired into this one.

Measured with a new `SchedulerBench` sweep ("requeue vs pushbatch"), interleaved with a same-vs-same
control (every same-vs-same cell came back clean, no `?` flags): **PushBatch dispatches 7.5-8.2x
faster than the Requeue loop at every size tested (N=8 to N=256)**, including N=64 -- the actual
`BATCH_SIZE` a single drain round processes -- and N=256, standing in for the multi-round backlog
case the drain's own header comment already worried about. `minPerSegment=8` is tuned for this call
site's small batch sizes specifically; reusing `ParallelFor`'s default would over-segment a 64-task
drain and pay more notifies than the spread is worth (see `PushBatch`'s own header on that
regression).

One behavior change, currently inert: `PushBatch` places every task at `CorePref::Default`
regardless of its individual `corePref`, where the old per-task `Requeue` honored it. Harmless today
-- no shipped caller sets `CorePref::P`/`E` (class routing is opt-in and dormant) -- but worth
revisiting if a future P/E-routed caller is ever `PushImmediate`-pinned. `Requeue()` itself is
untouched and still the right tool at its four remaining call sites (`Fiber.cpp`, two single-task
retry/race paths in `Thread.cpp`, and the rare deque-full overflow fallback) -- all single-task
contexts where `PushBatch`'s segment-building overhead would have nothing to amortize against.

New test: `SchedulerImmediateDrainTest` (`immediate_drain_test.cpp`), since `PushImmediate()` had
zero test coverage of any kind before this. Pins a 2-worker pool's worker 0 after seeding a
150-task backlog (>`BATCH_SIZE`, forcing the drain through multiple `PushBatch` rounds) and checks
every backlogged task ran exactly once -- verifying the null-compaction this change needed
(`PushBatch` links `tasks[i]->next` contiguously and cannot tolerate a hole the old per-task loop's
`if (!t) continue` could) neither drops nor duplicates a task -- and that the immediate task itself
still runs afterward. Passed 8/8 local runs before landing; wired into CI on all 5 targets. Full
existing suite (primitives under all three modes, event smoke, fiber budget, task slab, topology)
verified green with no regressions.

## 2.1.0 - 2026-08-20

**`TaskScheduler::TryRunStolenNoFiberTask()` is renamed `TryRunStolenNativeTask()`.** A leftover
from the `TaskType` rename in 2.0.0: that pass covered `noFiber`/`Task::type` and every comment
that named them, but this one public method's name itself still spelled out the old `NoFiber`
vocabulary it was supposed to retire, plus one stale abbreviation (`TRSFJ`, from `TryRunStolenFastJob`,
the name before that) in an adjacent comment that never got updated either. Same category of
change as 2.0.0 -- a public rename, technically source-breaking -- but treated as closing a gap in
that sweep rather than a new breaking decision, so it ships as a minor rather than a second major
bump days after the first.

Renamed at every call site: `TaskScheduler.h`'s declaration and two comments, `TaskScheduler.cpp`'s
definition, three call sites, and two comments (including the stale `TRSFJ` abbreviation), and the
two comment-only mentions in `bench/compare/compare_taskflow.cpp` and `compare_marl.cpp`. Historical
CHANGELOG entries that describe what this method was named at past releases are left untouched --
they are an accurate record of history, not documentation of the current API.

## 2.0.0 - 2026-08-20

**[BREAKING] `Task::noFiber` and `CreateTask`'s matching parameter are replaced by `TaskType`, an
enum with values `Native` and `Fiber`.** This is the reason for the major version: requesting the
capability most calls actually want -- a task that may suspend -- meant writing `noFiber = false`,
a double negative repeated at every call site that needed it, each one requiring its own comment to
stay readable. `TaskType::Fiber` is the identical request spelled as a direct, positive statement.

The reason this is an enum and not a renamed bool: a bool rename is not safe here. `noFiber` was a
positional parameter, and every existing call site passed it as a bare `true`/`false`/`0`/`1`
literal. Flipping the polarity of a same-shaped bool (`fiber = true`) would have let every one of
those call sites keep compiling while silently meaning the opposite of what they used to -- no
error, no warning, just inverted behaviour. An enum can't accept a bare literal positionally, so the
compiler refuses every call site until it names `TaskType::Native` or `TaskType::Fiber` explicitly.
Same breaking change, but it fails at compile time instead of at runtime.

`Native` is the default, unchanged from `noFiber`'s effective default -- nothing about ordinary
`CreateTask` calls that don't touch this parameter needs to change. One related bug fixed for free:
`Task::noFiber`'s own member default was `0`/false (fiber-capable), while `CreateTask`'s parameter
defaulted to `true` (native) -- a mismatch that only mattered for a `Task` constructed directly
rather than through `CreateTask`, and is gone now that both read `TaskType::Native`.

Every internal call site (41+ across this repository) was updated, along with `TaskDeque::StealBits`
(the internal steal-vetting tag, which mirrors this field but was never itself public), and every
mention in the README and DESIGN.md, including code examples that would otherwise no longer compile
against the API they were documenting.

Verified before committing: a full clean rebuild of every target with zero errors; the complete
`primitives_test` suite green under default/`nosleep`/`noreclaim`, which specifically exercises the
fiber suspend/resume path under real contention rather than merely compiling against it; the
fork-join section of `SchedulerBench`, the one path that specifically needs `TaskType::Fiber`; and a
re-disassembly of the `Worker()` dispatch check this touches, confirming identical codegen to before
the change -- one byte compare and one conditional jump, `Native` still the fall-through case.

```cpp
// before
Task* t = sched.CreateTask(fn, data, hipri, size, /*noFiber*/ false, corePref);
// after
Task* t = sched.CreateTask(fn, data, hipri, size, TaskType::Fiber, corePref);
```

**`TaskScheduler::SetTaskSlabSize(slots)`, called before `Init()`.** Total task slab capacity,
defaulting to 1024*1024 as always. Combine with `SetLazyTaskSlab` to control both the ceiling and
the commit strategy -- a small eager slab for a memory budget you want to hold for certain, or a
large lazy one for a ceiling you do not expect to reach.

There was no structural reason the size was fixed: not a power of two anything else depends on
(checked), not coupled to the ABI guard, which hashes type layout rather than a runtime count. It
just went unexposed, the same shape of gap `SetFiberBudget` closed last release, found this time
because there was an actual reason to want it -- experimenting with memory profile -- rather than
inferred from a warning message.

Verified the same way: a dedicated test (`SchedulerTaskSlabSizeTest`) checks not just that
`TaskSlabSize()` reflects the configured value but that `TaskAllocator::Capacity()` after `Init()`
matches it, and that `CreateTask` actually exhausts near that count rather than the number being
cosmetic. Confirmed as a real check by reverting the wiring to the old hardcoded `1024 * 1024` and
watching both fail -- reported capacity 1048576 instead of the configured 37, and none of 137
allocation attempts against that 37-slot slab ever returned `nullptr`. Standalone binary, wired
into CI on all four platforms plus Windows ARM64, for the same reason `SchedulerFiberBudgetTest`
is one: `primitives_test.cpp` creates tasks liberally and was never written expecting a slab this
small.

## 1.6.0 - 2026-08-18

**Fiber pool capacity is now configurable: `TaskScheduler::SetFiberBudget(standardPerWorker,
heavyPerWorker)`, called before `Init()`.** Defaults stay 64/8, unchanged. This closes a gap the
library's own exhaustion warning had been pointing at since before this API existed: it says,
verbatim, "raise `standardFiberCount` in `TaskScheduler::StartPool`" -- naming a local variable
inside a function body that nothing outside the library could reach. Anyone who hit the warning in
production and went looking for the lever it described would not have found one. `SetFiberBudget`
is that lever, following the same pre-Init-only contract `SetLazyTaskSlab` and
`SetParallelForSerial` already use -- it cannot be changed after `Init()`, since each fiber
registers a permanent slot with the epoch manager and the stack arena is one fixed allocation made
at startup.

A stale comment sitting next to the old computation (`// 3. Ensure a minimum to avoid "thrashing"`)
had no code under it and is removed rather than kept as a second unfulfilled promise.

Verified with a dedicated test (`SchedulerFiberBudgetTest`, new in CI on all four platforms plus
the Windows ARM64 job) rather than trusting the getters alone: it checks that `StartPool` actually
*consumes* the configured counts by reading `GlobalFiberPool::AvailableCount()` after `Init()`, not
just that the setter stores what it was given. Confirmed as a real check, not a vacuous one, by
reverting the `StartPool` wiring back to the old hardcoded 64/8 and watching the test correctly
fail with the wrong pool size (288 instead of the configured 80) rather than passing regardless.
Not folded into the existing primitives suite: `TestFiberCapOversubscribed` there hardcodes
`4 * 64 = 256` directly into its own logic, so changing the process-wide default anywhere in that
shared binary would have silently invalidated it.

**`SchedulerBench` gained a splitter-vs-cursor sweep, because the one that was already published
could not be reproduced.** The README's crossover table -- the recursive splitter measured at
10.3x-18.5x speedup over serial, the shared cursor (`RunCursorRange`, called directly) at
6.6x-22.6x, crossing over above roughly N=200,000 on a uniform body -- came from a one-off
scratchpad harness that was never committed. Nothing in this repository, on CI, or on a second
machine could rerun that comparison; the table was frozen the moment the harness was deleted.

`BenchSplitterVsCursorCrossover` is that comparison, built into the shipped bench this time. Six
sizes from 1,000 to 400,000 (two points past the README's largest, so a crossover found there is
confirmed by a following size rather than standing alone as the last, unconfirmed column), across
the same four body-cost classes (`trivial`/`light`/`medium`/`heavy`) the ParallelFor sweep already
uses. `nosweep` skips it along with the existing sweep, since three arms per rep makes it the
slower of the two.

**Interleaved with a same-vs-same control, and this is not decoration.** A block-measured
comparison between two range APIs is exactly what produced a fictional 15-47% gap the first time
this library compared `ParallelFor` against `ParallelRange` -- see that entry below. Every rep
re-runs the splitter a second time as the control; a cell whose control moved more than 5% on its
own is marked `?` rather than trusted, because the honest reading there is "the machine was not
quiet enough for this cell," not "the cursor tied the splitter."

**Built specifically to check the README's claim, and checking it broke the claim.** A first run on
the original desktop looked like it reproduced the old table -- heavy bodies pulling ahead ~1.2x
from around N=20,000, close to the README's implied 22.6/18.5. A second run on the SAME machine,
same code, same command, did not: heavy stayed at 0.99-1.01x across every size, never clearing the
win margin. A borrowed laptop and repeated runs on both machines since then show no pattern that
holds: a medium body won for the cursor at N=1,000 on one machine and lost to the splitter at that
same N on the other; a heavy body won at N=400,000 on one machine and did not on the other. Neither
N, body cost, nor worker count predicts it on their own.

The honest conclusion is not "the crossover moved" -- it is that the original 32-point table was
never a reproducible measurement in the first place. It survived as long as it did only because
nothing could re-run it. See the corrected `README.md` section for what replaced the claim.

## 1.5.0 - 2026-08-18

**`ParallelForNB` is REMOVED.** It was the non-blocking range loop: fixed `chunkSize`, one task per
chunk, returns as soon as they are queued.

This is a **breaking change to a shipped public API** -- it was present in v1.3.6 and v1.4.0 -- and
it is shipping in a MINOR release as a deliberate, one-off exception to the versioning policy below.
Saying so plainly is the point: the alternative was to carry a symbol nobody could use until a 2.0
existed to justify deleting it.

The reasoning, so it can be judged rather than taken on trust. It was never intentionally part of the
supported surface -- it predates the range APIs that replaced it, was superseded long before it was
removed, and survived on inertia rather than on a decision. It was undocumented, untested, and had
no callers anywhere. And it could not be used correctly even in principle, because it offered no way
to observe completion. An API that cannot tell you when it finished has no correct usage to break.

**If you are affected, you will find out at COMPILE time, not at run time** -- the symbol is gone, so
this surfaces as an unresolved name rather than as changed behaviour. The migration is mechanical and
is shown below.

**It worked.** It was tested before removal, not after: 10,000 elements over 40 chunks, every element
covered exactly once, none skipped and none run twice. What it lacked was a reason to exist. It had
**zero callers** in this repository, in any other JLib component, or in the application that drives
this library, and **zero tests** -- a public API nothing exercised, which is the combination most
likely to rot unnoticed.

`PushArray` is the replacement and is strictly better specified: it returns the number of chunks
submitted, takes an optional `WaitGroup` so the caller can wait later, is a template rather than a
`std::function` (so it does not allocate to hold the callable), and degrades gracefully by running
the remainder inline when the arena is exhausted.

    // before
    sched.ParallelForNB(0, n, 256, [&](int lo, int hi) { work(lo, hi); });
    // after
    sched.PushArray(0, n, 256, [&](size_t lo, size_t hi) { work((int)lo, (int)hi); });

One thing it took with it: its lambda captured **144 bytes**, which made it the single construct in
the entire codebase that could not fit a 128-byte slab slot -- everything else, including 216,323
task constructions measured in a real application, fits in 96. That made it the sole reason
`TaskAllocator::SLOT` had to stay at 256. Removing it unblocks a smaller slot, though measurement
says not to bother: halving the stride to 128 was tested against a same-vs-same control and the
entire effect fell inside run-to-run noise.

**Three published figures were wrong and are corrected.** The 1.4.0 table was re-measured but the
prose under it was not, so the same page quoted two numbers for the same benchmark: `PushBatch` read
12.2 M/s where the table said **14.7**, and `PushArray` 1.0 ns/item where the table said **0.55**.
Both follow from 1.4 cutting the allocator round trip 9.3 -> 2.1 ns.

The third mattered more than a stale number usually does. "Submit from inside the pool where you
can" recommended an architecture on the strength of **9.8 M/s** for four-producer submission -- which
is the saturated regime of a row 1.4 documents as bistable with a cliff at ~21 workers, published by
`best-of-5` picking whichever regime got lucky. The advice survives (four producers beat one in both
regimes) but the margin on that machine is **2.4x, not 10x**, and the section now says so and points
at the sweep. If you quoted 9.8 M/s from the 1.4 README, it was real and it was not representative.

**The range APIs are now documented against each other** -- both `ParallelFor` overloads, `PushArray`
and `RunCursorRange`, on the axes that decide between them: does it block, how does it divide, when
do you want it. Two 1.4 leftovers went with it: `PushArray`'s comment still described `ParallelFor`
as "which probes the work", and the README called the shared cursor an internal fallback when
`RunCursorRange` is public.

**`SchedulerBench` stopped printing a gate that no longer exists.** Its crossover sweep header
advertised "probe a prefix, extrapolate, parallelize when est. work >= 75us" on every run. It now
states there is no gate, and that cells below 1.00x are the known cost of being probe-free rather
than a bug -- which is the question a sub-1.00x cell actually raises.

## 1.4.0 - 2026-08-18

**`ParallelFor` NO LONGER PROBES. It is now demand-driven, and the probe is gone rather than
demoted.** This is the headline change of 1.4 and it is a behaviour change to the primary API.

It used to decide serial-vs-parallel by PREDICTING: run a serial prefix, time it, extrapolate, and
parallelize if the estimate clears ~75 µs. That works on a uniform body and cannot work on a
data-dependent one -- a prefix over items 0..311 says nothing about item 50,000 when the early ones
early-out and the later ones do full mesh collision. The prefix also warms the cache while the
remainder streams from DRAM, so the estimate is biased low *systematically* rather than noisily, and
on a hybrid part the probe runs on the caller's core class and the work runs on another.

Now the range is made splittable and STEALS DECIDE. `ParallelFor` publishes the right half onto the
calling thread's own deque and carries on with the left. Nobody took it → the splitter takes it
straight back and runs it inline for ~11 ns, no dispatch and no notify. Somebody took it → the pool
was hungry and the split was right. Idle pool parallelizes, busy pool runs serially at near zero
cost, ragged body keeps getting stolen from -- which is itself the signal to subdivide where the
work turned out to be. Nothing to calibrate per machine.

That is affordable because of what the speculative path actually costs, which had never been
measured: **17.8 ns** at the time, **10.8 ns** after the allocator fix below, for alloc → construct
→ `push_bottom` → `pop_bottom` → run → destroy → free. An unstolen split needs no notify (nobody has
to be woken to run something you are about to run yourself) and no epoch retirement (winning the pop
CAS proves no thief observed it). The "~85-105 ns per task" figure quoted elsewhere is the
*cross-thread dispatch* path -- MPSC inbox, round-robin, notify -- which this skips entirely.

Medians of 15, 31 workers, default Sleep, speedup over the same body run serially:

| body | 1.3.x `ParallelFor` (probe) | recursive splitter | **1.4 `ParallelFor`** |
| --- | --- | --- | --- |
| uniform, 1M | 5.7-6.9x | 7.7-8.1x | **8.4-9.8x** |
| data-dependent 20x, 200k | 10.3-16.0x | 20.1-21.7x | **15.3-22.1x** |
| back-loaded (all cost in the last 10%), 100k | 5.8-6.1x | 9.4-15.2x | **11.3-13.5x** |

**There is a new no-grain overload, and it is the one most callers want.** "How do I know what grain
to pass?" is the fair objection, and for most callers the honest answer is that they cannot know --
they know their body is "a collision check", not what it costs per element. `ParallelFor(begin, end,
func)` derives a grain from the two things known exactly, the range and the pool size, and never
consults the body: `range / (workers * 8)`, the same rule Cilk's `cilk_for` uses.

Eight leaves per worker, not the sixty-four the explicit overload *floors* at, and the gap is
deliberate -- those are two different decisions. A floor stops an explicit grain being absurdly
fine, where leaning aggressive costs nothing. A default is picked with no information, and there the
errors are wildly asymmetric: under-splitting an expensive body costs ~10% (200k ragged: ~19-21x
against a 21.5-23x plateau) while over-splitting a cheap one costs ~20x (0.4 ns/element: 1.00x with
a justified grain against 0.05x). Deriving the default from the floor was the first version and it
measured that 0.05x.

**Explicit grain is floored at 64 leaves per worker.** Grain has a cliff on the low side that a
caller guessing too fine falls straight off -- measured before the floor, **grain 1 gave 2.61x on a
ragged body against 21.5x at grain 32-64**, and 1.95x against 18.6x back-loaded, because a split
costs ~10.8 ns unstolen and a full dispatch when stolen, against a leaf holding one element. The
floor is a statement about the POOL, whose size is known exactly, not about the BODY -- so it does
not smuggle the probe back in.

**WHAT YOU GIVE UP, stated plainly.** Nothing probe-free can decline to parallelize a body too cheap
to be worth it, and the old probe could. Pass a grain smaller than the body justifies -- 32 elements
of a ~0.4 ns/element loop -- and this faithfully splits that fine and measures ~0.08x against
serial, where 1.3.x held 1.00x. The no-grain overload bounds the damage (0.24x on the same case, and
11.8x rather than 4.6x on a large cheap range) but cannot remove it. TBB's `simple_partitioner`,
Rayon and Cilk all share this property. If you do not know what an iteration costs, use the no-grain
overload; if you need to know whether a loop should have been parallel at all, `SetParallelForSerial`
answers it in one run.

**REMOVED: `SetParallelForThresholdUs` / `GetParallelForThresholdUs`**, which tuned the gate that no
longer exists. Their genuinely useful job was never tuning -- it was answering "is `ParallelFor`
responsible for this?" without a rebuild, by being set enormous. **`SetParallelForSerial(bool)`**
replaces that affordance and drops the pretence that the microsecond figure meant anything portable.

**`ParallelForLazy` does not exist.** It was the working name while this was being built as a second
entry point; shipping two names for one behaviour would have been worse than the probe. It never
appeared in a release.

name a sensible grain.

**Callable from the main thread**, which is the normal case. `loPri`/`hiPri` gained one extra deque
past the workers -- the non-worker lane -- claimed by one non-worker thread at a time and probed by
every worker as an ordinary steal victim. Without it a main-thread caller had nowhere to publish and
the tree would have had to fan out over a chain of steal hops. A second concurrent non-worker caller
loses the claim and degrades to the cursor path rather than blocking. The extra unconditional steal
probe measured no regression on the benchmark suite (throughput, latency and frame DAG all within
run-to-run variance).

**`TaskAllocator::Alloc`+`Free` is 4.5x cheaper: 9.3 ns → 2.1 ns.**

Profiling the split path turned up something that had nothing to do with splitting. `Alloc` and
`Free` each did one `fetch_add`/`fetch_sub` on the sharded live-slot counter, and those two atomic
read-modify-writes were **6.7 ns of the 9.3 ns round trip — 72%** — while the free-list work they
surrounded was 1.5 ns. `memory_order_relaxed` does not make an RMW cheap: relaxed governs ordering,
which is the free part, but the `lock xadd` still takes the cache line exclusively, ~3.3 ns each
even completely uncontended. That was being paid twice per task for a number whose only readers are
an error message in `TaskNode.h` and a diagnostic print in the benchmark.

A shard is only ever *written* by the thread that owns it, so where ownership is exclusive the
update can be a plain relaxed load + relaxed store with no lock prefix. **Exactness is not traded
away.** The exclusivity premise fails above `kLiveSlots` (128) threads — and it fails on *cumulative*
thread creations, not concurrent ones, since `s_liveNext` never decrements and an exiting thread
never returns its slot, so a process that spawns transient threads gets there far sooner than its
peak thread count suggests. Past that point two threads share a shard, and a non-atomic RMW would
not merely blur the reading, it would lose updates permanently and drift further from the truth
forever — precisely destroying the counter's one job, telling a real slab leak from normal churn.
So the fast path is taken only where it is provably exclusive (the first 128 threads, which is every
realistic case) and everything past it falls back to the original `fetch_add`. Verified exact under
balanced churn on 16 threads, cross-thread alloc/free, 200 cumulative threads past the shard limit,
and 24 concurrent threads sharing shards.

Downstream effect: the speculative split path is **17.8 ns → 10.8 ns**, and the allocator drops from
54% of it to 19% (the deque's last-item CAS is now the largest single item). On the benchmark suite
the frame DAG went from 21.29-21.39 µs to 20.34-20.81 µs across four runs each — non-overlapping,
~3.4% — with batch throughput and round-trip latency also improved.

**REMOVED: `ParallelForFJ`. Use `ParallelFor`.**

The fork-join variant -- split in half, spawn the right half, recurse left. `ParallelFor` stopped
dispatching to it when the slice-stealing cursor path replaced per-chunk tasks, which left it public
with no caller anywhere in the tree.

**`ParallelFor` is the drop-in**: same shape, same blocking behaviour -- and as of 1.4 it IS a
recursive splitter, so a caller moving off the fork-join entry point gets the same structure it
wanted. Use the no-grain overload if the old `grain` argument was a guess.

This is a breaking change to shipped public API, taken in a minor release rather than deprecated
through one, because the entry point had already been superseded internally for a release and had no
callers left to warn.

Its measurement table survives in `RunLazyRange`'s comments rather than in git history, because it
says something about the code that replaced it: the fork-join splitter carried a capitalised
instruction NOT to spawn a child on the calling worker, backed by an A/B showing self-spawn
saturating at ~8x where round-robin `Push` climbed past 17x. The new `ParallelFor` self-spawns and
does not saturate -- the difference is that it wakes a thief per unstolen split. That wake, and the
per-thread seeding of its round-robin cursor, are load-bearing rather than optimisations: remove
either and it becomes the 8x row.

**`TaskDeque` now stores TAGGED pointers, so a thief never dereferences a task it has not claimed.**

`steal_if` vets a candidate before claiming it, and it used to do that by passing the predicate the
`Task*` and letting it read `corePref`/`noFiber`. That was safe in outcome -- CAS success proves
`top_` never moved, so the vetted read was of the live task, and every other outcome discards it --
but it was still a read through a pointer whose object lifetime could already have ended. The owner
may pop that task, run it, free it and get the same slab slot straight back; because the free list
is thread-local LIFO, that recycle is the **common** case, not a rare one. ThreadSanitizer reported
it and was right to.

Both vetting fields now ride in the spare low bits of the stored pointer -- `Task` is `alignas(16)`,
so bits 0-3 are free, and a `static_assert` ties that to `alignof(Task)` so shrinking the alignment
cannot break it silently. The predicate receives `StealBits` decoded from the tag, reading memory
the deque owns rather than memory it does not. Sound only because both fields are written
exclusively by `CreateTask` before the task is ever pushed, so a tag cannot go stale; a future
mutator of either field must re-tag.

Two things that sound like fixes and are not, recorded so they are not retried: making
`Task::corePref`/`noFiber` **atomic** does nothing here, because the racing write is the
*constructor* of the next task in that slot and initialization is not an atomic operation whatever
the member's type is -- and the real problem is the ended lifetime, which no member type addresses.
And "those fields never change, so the read is harmless" is a non-argument: by the time of the
racing write they belong to a different object.

**The protocol is unchanged** -- same indices, same atomics, same fences, same CAS; only the
payload's spare bits are new. Re-verified with GenMC after the change and all three results
reproduce exactly: no errors and 174 complete executions on both the `acq_rel` and the paper's
`seq_cst` steal CAS, and the permanent `-DNO_POP_FENCE` negative control still produces a safety
violation. The TSan probe went from 4 reports to **0**.

**[CRITICAL] The stale-library guard could not see this release's own ABI break, and a partial
rebuild crashed at `0x0000000000100000`.** Anyone who upgrades to 1.4 without a *full* rebuild of
every translation unit that includes `TaskScheduler.h` hits this, so it is a must-pull for
downstream forks.

1.4 added `nonWorkerLane` and `nonWorkerLaneClaimed` and made `consecutiveHiPriSteals`
`thread_local`. Those moved `taskAllocator` from offset **304 to 312** -- while
`sizeof(TaskScheduler)` stayed at exactly **1664**, because the new members landed in padding that
already existed. `sizeof(Task)`, `sizeof(TaskAllocator)` and `sizeof(EpochManager)` did not move
either, and those four sizes were the whole signature. The guard therefore *matched* across a real
ABI break and reported success.

What that produced was not a corrupt heap. It was a correctly-formed read of the wrong member:
`CreateTask` is header-inline, so a TU compiled against the new headers reached +312 into an object
laid out for +304, landed on `TaskAllocator::memSlots` instead of `mem`, and dereferenced its value.
`memSlots` is `1024*1024`, so the access violation read address `0x0000000000100000` -- a number
that reads like a wild pointer and is really a slot count. It surfaced inside the allocator, several
subsystems away from anything that had changed, and free-list instrumentation found nothing because
the bad pointer never went through the free list.

Two fixes, and the second was a bug in the first attempt at the first:

- The signature now includes **offsets**, not just sizes. `TaskScheduler::abiCanary` is a public
  one-byte member sitting immediately after `taskAllocator`; its `offsetof` moves whenever anything
  above it is added, removed, resized or reordered. Sizes remain for the types inline code allocates
  or copies whole.
- The signature and the check that consumes it now have **internal linkage**. They were written
  first as an `inline` function and an `inline const bool`, which have *external* linkage -- so the
  linker folds the library's copy and the application's copy into one, both sides evaluate the same
  body, and the comparison is a value against itself. Measured: with the library built at offset 296
  and the application at 312, the folded guard compared equal and let the program run into the
  corruption. Per-TU copies are the mechanism, not an oversight.

The report also had to change, because the first version of it was useless in the place it fires.
It printed two hashed 64-bit numbers to `stderr`, and a GUI-subsystem process has no console -- so
the entire diagnostic was "Fatal program exit requested" at `__scrt_common_main_seh`. It now carries
the components field by field and names the ones that disagree:

```
[JLib::Scheduler] FATAL: this translation unit was compiled against DIFFERENT
Scheduler headers than the Scheduler library it is linked to.
  Fields that disagree (library vs this TU):
    offsetof(TaskScheduler,abiCanary)    408 vs 424
```

and it goes to `OutputDebugStringA` as well as `stderr`, so it lands in the debugger's Output
window. The message says to rebuild **every** library that includes `TaskScheduler.h`, because that
is the case that actually bit: `Sound`, `Renderer`, `Physics3D`, `Assets` and `PlatformerPhysics2D`
each carry their own inlined copy of `CreateTask`, so a stale one of those reaches the wrong offset
in a perfectly good scheduler object. Rebuilding only the Scheduler does not fix it.

Verified both ways: a deliberately shifted header now aborts naming the field, and a matched pair
still runs clean. A guard that reports success is worse than no guard, which is what this was.

**[CRITICAL] `~TaskMPSCQueue` corrupted the heap.** It ended with `::delete stub_`, but `init()`
allocates `stub_` from the TaskAllocator slab -- and the `::` forces the *global* deallocation
function, stepping past `Task`'s own. A slab slot went to the CRT heap: immediate
`STATUS_HEAP_CORRUPTION` (0xC0000374) for anyone who destroyed one. It stayed hidden because nothing
in the library destroys one (`Init()` does `instance = new TaskScheduler(...)` with no matching
delete, so `~TaskScheduler` and the inbox vectors never run) -- but any harness or embedder that
stack-allocates a `TaskMPSCQueue` hits it on the first run. The destructor now returns the stub to
the arena it came from, and the default constructor zero-initializes `head_`/`tail_`/`stub_` instead
of leaving them indeterminate.

**`ParallelFor` now dispatches by slice-stealing.**

`ParallelFor`'s large-range path creates one task PER WORKER instead of one per chunk. Each pulls
`[lo, lo+grain)` off a shared cursor until the range is consumed. The per-chunk paths it replaces
cost ~80-140 ns each -- a slab slot, a push and an epoch retirement -- and that cost is precisely why
chunk size had to be floored at 4 per worker. Measured against the fork-join path, 4M items, 31
workers, medians of 4 runs: **1.7-1.9x on a uniform body, 1.2-1.3x when cost varies ~20x** across the
range. No API or semantic change -- same callable, same blocking behaviour, same completion
guarantee, so every existing caller gets it without doing anything.

The crossover sweep moved with it. Against the same sweep before the change:

| body | before | after |
| --- | --- | --- |
| trivial @ 200k | 0.68x -- never cleared 1.15x | **1.86x -- now has a crossover at all** |
| light @ 200k | 1.92x | **4.76x** |
| medium @ 4000 | 1.44x | **2.55x** |
| heavy @ 200k | 16.60x | **20.86x** |

**The ~75 µs gate was then measured, and it stays.** The reasonable-sounding prediction was that it
must be conservative: it was calibrated before fork-join existed, and dispatch has since got cheaper
twice. Measured directly -- raw serial loop against the cursor path (no probe to contaminate the
timing), sweeping element counts so total serial work spans ~0.1 µs to ~9 ms, three body costs,
medians of 15, stable across three runs:

| body | parallel breaks even at |
| --- | --- |
| heavy, ~64 flop/item | **69 µs** |
| medium, ~8 flop/item | **88 µs** |
| cheap, ~1 flop/item | never in range -- still 0.52x at 64 µs of serial work |

75 µs sits almost exactly between the two compute-bound crossovers. **It is well placed and was not
changed.** The prediction was wrong, and the measurement is the only reason we know that rather than
having shipped a tuned-by-guess constant.

The cheap row looks alarming and is not. That 0.52x is at 64 µs of serial work, which is BELOW the
gate -- `ParallelFor` correctly declines to parallelise there, and the number is what you would get
if it did not. Checked what actually happens once such a body clears the threshold, using a
memory-bound loop over a buffer far past L3, forced-serial against the default gate:

| elements | buffer | forced serial | default gate | |
| --- | --- | --- | --- | --- |
| 1,048,576 | 4 MB | 0.093 ms | 0.093 ms | 1.00x |
| 4,194,304 | 16 MB | 0.416 ms | 0.115 ms | **3.62x** |
| 16,777,216 | 64 MB | 3.708 ms | 1.259 ms | **2.95x** |
| 67,108,864 | 256 MB | 16.200 ms | 8.279 ms | **1.96x** |

It ties at the boundary and wins 2-3.6x past it. So the gate declines below its threshold and pays
off above it, which is exactly the job.

What does remain true is that 75 µs is calibrated on ONE machine. The memory-bound `ParallelFor`
bench row already records 0.75x on a Ryzen laptop APU and 1.09x on an M1 Air against ~3.3x here, so
the crossover genuinely moves with core count and memory bandwidth. That is what
`SetParallelForThresholdUs` exists for, and why the constant is exposed rather than baked in.

**`ParallelRange(begin, end, grain, fn)`** was added here as the probe-free entry point, and then
**REMOVED later in this same release, before it ever shipped.** Once `ParallelFor` lost its probe,
the reason to have a second probe-free entry point went with it -- and briefly the two were literally
the same function, while `ParallelFor` was pointed at the same cursor. Use `ParallelFor`; the
no-grain overload covers the case this was reached for.

What it contributed survives. Its shared cursor is still `RunCursorRange`, used as the fallback when
a second non-worker thread is already splitting, and its grain floor -- up to 64 slices per worker,
against the 4 per worker the old per-chunk path capped at -- is what `ParallelFor` floors to now.

`ParallelFor` itself ended on RECURSIVE SPLITTING rather than the cursor. It was switched to the
cursor for one commit on the strength of three large-N samples that showed a tie, and switched back
when the crossover sweep -- 32 points over four body costs -- showed the two cross over instead:
the splitter is 1.4-1.6x ahead from N=1000 to N=10000 and gives up ~1.2x only at very large N.


**A grain floor bug, caught by the new API and worth recording.** The first version floored grain at
a flat 64, on the theory that a cursor makes grain a load-balancing knob only. It does not: the TASK
count stops varying with grain, but the `fetch_add` count is `range/grain` on one contended line. A
flat 64 measured **15x slower than `ParallelFor`** on a 4M range -- 65,536 atomics against ~124.
`ParallelFor` had been hiding it by flooring grain to ~33,800 before the cursor ever saw it. The
floor now scales to keep at most 64 slices per worker. Had the cursor only ever been reachable
through `ParallelFor`, this would have shipped invisible.

`ParallelForFJ` is no longer what `ParallelFor` selects, and stays public for callers who want the
fork-join tree directly -- same reason `ParallelForNB` is public.

**`PushArray` is not obsolete**, and the reason is structural. `ParallelFor`
BLOCKS: its cursor, wait group and callable live on the caller's stack frame, which is the only
thing making a zero-allocation slice-stealing loop possible. `PushArray` does not block -- it hands
back a WaitGroup so you can submit range work and carry on, or never wait. A non-blocking cursor
needs heap state and a refcount to outlive the caller's frame, which is a different design. It is
also the only one of the three taking a per-ITEM callable.

**The task slab is no longer memset at construction, and can optionally be lazy.**

`std::vector<Block>(n)` value-initializes, so building the default 1M x 256-byte slab memset 256 MB
that the free-list loop overwrote microseconds later. `new Block[n]` default-initializes a trivial
type and writes nothing: **`Init()` drops from 57 ms to 35 ms with identical resident memory and
identical semantics.** Pure waste removed, no decision required.

`TaskScheduler::SetLazyTaskSlab(true)` before `Init` additionally defers linking slots into the free
list until they are first needed, so resident memory tracks peak live tasks rather than capacity:

    default            277 MB resident, Init() 35 ms
    SetLazyTaskSlab     21 MB resident, Init() 5.5 ms

**Default is OFF deliberately.** Eager faults every page in before the first task runs, so the steady
state has no memory events at all -- that is the zero-allocation runtime this library advertises.
Lazy keeps heap allocation out but moves first-touch page faults into the run, and a fault mid-frame
is an unpredictable kernel transition on the critical path. Saving memory a desktop application does
not care about is not worth weakening the guarantee it does. Turn it on for Android and iOS, where a
whole app may have a few hundred MB. `PrefaultTaskSlots(n)` is the middle ground: lazy on, then
prefault the ceiling you actually expect during load.

### Also in this release (drafted as 1.3.7 before the 1.4 work landed)

**Epoch reclamation can be handed to the application: `EpochManager::SetSelfReclaim(false)`, then
call `Tick()` from your own idle point.** Default is unchanged.

By default a worker performs reclamation -- once enough pointers are retired, whichever worker
notices next stops and runs a pass. That pass calls `MinActiveEpoch()`, which scans every epoch
participant, and the participant set scales with the pool (~2,300 slots on a 31-worker machine). The
total work is small; the problem is that it lands on a thread that was supposed to be running a
frame, at an unpredictable moment.

**Measured** -- frame-shaped DAG, 32 nodes per frame, 4,000 frames after 400 warm-up, three
interleaved rounds, per-frame microseconds:

| | p50 | p90 | p99 |
| --- | --- | --- | --- |
| default (workers reclaim) | 58.1 / 58.9 / 60.2 | 67.7 / 66.8 / 69.0 | **331 / 331 / 336** |
| `Tick()` on the app's thread | 60.5 / 58.7 / 58.5 | 69.3 / 67.8 / 66.3 | **125 / 111 / 104** |

**Throughput is unchanged** -- median and p90 are a wash. **p99 improves ~3x**, consistently, with
the ranges nowhere near overlapping. The win is *where* the scan runs, not how much of it there is.

Worth recording that this is not the reason the flag was written. It was proposed to remove the one
atomic in the retire path (`retiredCount.fetch_add`, per retired pointer), and that part measures
**nothing** -- exactly as the comparable `nextWorker` experiment predicted, because the `fetch_add`
sits next to a lock-free MPSC enqueue that costs more and cannot be removed. The tail-latency win was
found by measuring and was not predicted by anyone.

The flag is a plain `bool`, not an atomic, and that is deliberate: it is documented as settable only
before `StartPool`, so a worker hoisting the read out of its loop is the point -- the branch folds
away and the disabled build pays nothing. That is the exact inverse of `SetIdlePolicy`, which is
atomic *because* it is documented as changeable at runtime. Different contract, different storage.

Default stays on because this is a library and the embedder may have no loop to tick from -- a
headless server, a batch job, a plugin inside someone else's engine. Disabled with no `Tick()`,
retired memory grows without bound and nothing warns.

Also in this change: the decrement in `TryReclaim` is now guarded symmetrically with the increment
(unguarded, with nothing ever incrementing, the first manual `Tick()` wrapped `size_t` and poisoned
any diagnostic read of `RetiredCount()`), and four hand-copied trigger predicates collapse onto one
`ShouldSelfReclaim()`.

Tested as a third whole-suite mode rather than a bespoke case: `noreclaim` sets the flag before
`Init` and runs everything, so every retire path in every test exercises the disabled branch. It runs
on all five platforms in CI. Flipping the flag inside a test would have raced the very readers it is
meant to exercise.

## 1.3.6 - 2026-08-17

**Windows on ARM64 is supported.** Snapdragon X and any other WoA machine. It needed a third
hand-written context switch -- `src/win32/aarch64/ContextSwitch.asm`, in armasm64 syntax -- because a
calling convention belongs to the **(OS, ARCH) pair**, not to the arch. That is the one place this
library's "the OS axis and the arch axis are orthogonal" claim does not hold, and it is now a
documented caveat rather than something designed around: `src/win32/aarch64/` sits beside
`src/posix/aarch64/`.

The real difference turned out to be much narrower than expected. Windows ARM64 and AAPCS64 agree on
the callee-saved set (x19-x28, x29, x30, and the low 64 bits of v8-v15) and therefore on the frame
layout, so **`src/posix/aarch64/FiberInit.cpp` is reused verbatim** rather than duplicated -- only
the assembler syntax and the unwind-data format actually differ.

`x18` is never touched, and that is load-bearing rather than incidental. It is the TEB pointer on
Windows, and fibers in this scheduler **migrate between worker threads**, so saving it into a fiber's
frame and restoring it after that fiber resumes on a different worker would install a stale TEB on a
thread that did nothing wrong -- corrupting thread-local state and surfacing later as
nondeterministic damage in unrelated code.

**Two things the previous documentation asserted were wrong**, found while doing this:

- The CMake guard claimed a Windows fiber switch "must update the TEB's `StackBase`/`StackLimit` --
  the fixup the x64 MASM does". **It does not.** There is no TEB access anywhere in `src/win32/`.
  The ARM64 port matches the shipped x64 behaviour instead of inventing a third one. The consequence
  is identical on both: a fiber-stack overflow arrives as an access violation on the arena's guard
  page rather than as a proper stack-overflow exception, and SEH across a fiber boundary is limited.
  That is the normal trade for lightweight fibers and has shipped on x64 for the life of the library.
- **Windows needs two assembler languages.** CMake's `ASM_MASM` is ml/ml64 and is x86-only; ARM64
  requires `ASM_MARMASM` (armasm64, CMake 3.26+). It fails loudly rather than degrading, and the
  choice has to be made at `project()` time -- before `CMAKE_SYSTEM_PROCESSOR` exists -- so it comes
  from the generator platform or the host environment.

`tests/fibertest_win_aarch64.cpp` is the Windows ABI harness, and it is a separate file rather than
`#ifdef`s over the POSIX one because **MSVC supports no inline assembly on ARM64 at all**. The POSIX
harness is built almost entirely from `register uint64_t v asm("x19")` pins, which have no MSVC
spelling, so the register work moved into `tests/win32/fibertest_probe_aarch64.asm`. That version is
arguably better: `AbiProbe` seeds all eighteen callee-saved registers, drives one switch, verifies
branchlessly, and returns a **bitmask naming exactly which registers moved**, where the POSIX harness
needs two passes of five because pinning ten GPRs at once starves the register allocator. The
guard-page check uses SEH instead of `fork()`.

Verified on a GitHub `windows-11-arm` runner: the isolated ABI harness (round trips, callee-saved
GPRs, d8-d15, FPCR isolation, guard page) plus the full scheduler suite under both idle policies.

**`SetIdlePolicy` is now safe to call on a running pool.** It was not, and the failure mode was not
tearing: `g_idlePolicy` was a plain global read by every worker on every idle pass, and a non-atomic
load of a global the compiler can prove is not modified inside the worker loop is free to be
**hoisted out of that loop** -- so a running worker could never observe the change at all. Nothing
documented the before-`StartPool`-only constraint, which made it a trap. Now `std::atomic`, relaxed:
the policy is a hint about how hard to look for work, never a correctness input, so a stale read
means a worker spins one pass longer or parks one pass early and both are safe states. No new
lost-wakeup surface -- `sleepwake_model.c` is unaffected.

**`tests/verify/fiberwait_model.c`** model-checks the fiber wait/resume handshake shared by
`SchedulerMutex`, `SchedulerSemaphore` and (transitively) `SchedulerConditionVariable`. This was the
last unmodelled synchronisation in the library, and it is the one that shipped the 1.3.5 deadlock.
Two negative controls, both of which fail as required: `-DOLD_ORDERING` reproduces that deadlock, and
`-DCLOBBER_SUSPEND` reproduces the `Thread::Suspend` trap an otherwise correct-looking reorder would
walk into. The notable result is `-DSEQ_CST` exploring the **identical** state space to the shipped
release/acquire version: the spinlock already supplies the happens-before edge, so this was never a
memory-ordering bug and no barrier would have fixed it. When a lost wakeup turns up, check program
order before reaching for stronger orderings.

**The `IdlePolicy` guidance was recommending the wrong policy, and is corrected.** It named a
fullscreen game as the obvious case *for* `NoSleep`; that is precisely where `NoSleep` loses. The
error came from reasoning off the scheduler benchmarks, where the pool IS the workload so there is no
render or audio thread for the spinning to tax and only the wake saving shows up. Measured with an
idle pool and a memory-bound main thread (31 workers):

| | median frame | vs no pool |
| --- | --- | --- |
| no pool at all | 14.65 ms | -- |
| `Sleep` (parked) | 14.65 ms | +0.0% |
| `NoSleep` (spinning) | 15.17 ms | +3.5% |

An idle `Sleep` pool is free. An idle `NoSleep` pool taxes every other thread in the process, and the
cost is core **occupancy**, not cache traffic -- a control pool of pure `CpuRelax` threads touching no
shared memory reproduced almost all of it, which also means there is no spin-loop optimisation
available (measured ceiling 0.8%).

**And 3.5% is a lower bound, not the number.** Re-measured inside a real 2D game (5-node frame DAG,
vsync off, 600 frames after 120 warm-up, two interleaved rounds, median frame time): `Sleep`
383.3/374.1 us against `NoSleep` 462.0/464.9 us -- **23% worse**. A game has the render thread, GPU
driver threads and audio all competing, and spinning workers land on them at exactly the
latency-sensitive moments. The synthetic figure understated the real cost by roughly 7x.

`SetIdlePolicy`'s comment now carries all of that, plus why an **adaptive** mode was rejected: a
controller switching on queue depth, steal rate, suspension rate, DAG pressure or idle ratio cannot
observe its own cost function (the tax lands on the render thread; every available signal is
scheduler-internal), and those signals describe the present while the decision needs the next idle
gap -- in a frame workload a heavy burst is exactly what precedes a long idle tail, so it would be
most confident right before it was most wrong. A scoped RAII wrapper for phase-switching was written,
measured in the same game, found to change nothing (374.8/377.5 us, indistinguishable from `Sleep`),
and removed before release.

## 1.3.5 - 2026-08-16

**[CRITICAL] Lost wakeup in the fiber wait path of `SchedulerMutex` and `SchedulerSemaphore`.**
Without this, two fibers contending for the same mutex can deadlock permanently: the mutex is left
locked with no holder and the waiter is parked with nothing able to wake it. Present in every
release up to and including 1.3.4. `SchedulerConditionVariable::Wait`'s fiber path inherited it
through its stack-local semaphore, so all three primitives were affected by one root cause. Anyone
on 1.3.4 or earlier who locks a `SchedulerMutex` from inside a fiber task should treat this as a
must-pull.

Both wait paths made the fiber **discoverable before making it parkable**:

```cpp
waitingFibers.push(current);   // discoverable...
spinLock.clear(release);       // ...but status is still RUNNING
Thread::Suspend(current);      // only NOW -> WANTS_SUSPEND
```

An `Unlock()`/`Signal()` landing between the clear and the status store pops the fiber and calls
`Resume()`. `ResumeQueueless` does not treat `RUNNING` as resumable -- it takes the "not resumable
right now" branch and returns false, **silently discarding the wake**. It does not crash, which is
why this presented as a silent hang rather than an access violation. The fiber then parks
`SUSPENDED` with no reference to it left anywhere, while the unlocker has already handed ownership
over: `Unlock` deliberately leaves `locked == true` whenever it pops a waiter, because that is a
direct handoff. The result is a mutex locked forever with no holder. In the semaphore's case the
pop also consumes the permit.

The fix is ordering, not new machinery. `Fiber::status`'s `WANTS_SUSPEND -> SUSPEND_SIGNALED`
transition already *is* the suspend-pending handshake, and `ResumeQueueless` already defers
correctly -- a resume that arrives too early flips the state and the worker's park step wakes the
fiber instead of parking it. `WaitOnEvent` has always used it correctly and states the rule:
*become parkable BEFORE registering.* These two callers simply set it too late.

**The direct `ContextSwitch` is load-bearing, not a micro-optimisation.** `Fiber::Suspend()` stores
`WANTS_SUSPEND` *unconditionally*, so calling it after the reorder would clobber a
`SUSPEND_SIGNALED` written by a racing `Resume` and reintroduce the identical lost wakeup. That is
why `WaitOnEvent` switches directly too.

**How it was verified.** The race did not reproduce locally in ~30 runs before OR after the change,
so a green run proved nothing on its own. Widening the window (a bounded spin between
`spinLock.clear()` and the suspend) made it deterministic, and the failure reproduced the CI output
exactly, `PREMISE ... ok` immediately followed by the watchdog:

| build | result |
| --- | --- |
| old ordering + widened window | **4/4 deadlock** |
| fix + widened window | **6/6 pass** |
| fix, unmodified | **24/24 pass** (12 `sleep`, 12 `nosleep`) |

Ordering is the only variable between the first two rows.

Found by the fiber-contention case added to the primitives suite in 1.3.4 -- the first test that
ever exercised this path. It hit macOS arm64 under `sleep` and Windows/MSVC under `nosleep`. The
`nosleep` failure is the informative one: workers never park there, so this is not the park/wake
handshake covered by `tests/verify/sleepwake_model.c`.

**Still owed:** this handshake has no GenMC model. The deque and the sleep predicate both have one;
this is now the last unmodelled synchronisation in the scheduler, and it is the one that shipped a
bug.

## 1.3.4 - 2026-08-16

Everything below is one batch: v1.3.3 was the last tag, so all of it ships as 1.3.4.

**Work-stealing deque: the three `top_` CAS sites are back to the paper's `seq_cst`.** They were
weakened to `acq_rel` on 2026-08-11 because GenMC found that sufficient. The finding is probably
correct -- the last-element race is a Dekker pattern resolved by the two `seq_cst` FENCES, so the
CAS only has to publish and observe -- but note the asymmetry in what a model checker proves. Its
other result, that deleting the `pop_bottom` fence double-claims in under a second, is a concrete
counterexample and therefore PROOF. "No counterexample found" is bounded evidence, and the bound
was literally **one owner and two thieves**; this pool runs 31. Same tool, very different
confidence.

What decided it: the weakening buys **nothing** where this runs. On x86-64 both orderings emit the
identical `lock cmpxchg` (verified on GCC and MSVC). It differs only on AArch64 -- the newest,
least-exercised port -- and what it would buy there is unmeasured, while what it risks is two
workers claiming one task: a double-free that would never be reproducible. To weaken it again,
measure the barrier on ARM first and widen the model beyond two thieves.

**`PickNextWorker`'s `nextWorker` was `seq_cst` by accident.** Bare `operator T()`/`operator=` on a
`std::atomic` default to sequential consistency, so on x86 every push emitted a locked `xchg` on one
globally shared counter -- and `PickNextWorker` is called from `PushLocal` (both branches),
`Requeue`, `PushBatch` and `PushToCore`. Now explicit `relaxed`, matching the P/E picker beside it
which always was. Safe by construction: it is a round-robin HINT, nothing reads it for correctness,
and the loop re-checks `immediateCoresInUse` on whatever it picks.

**Measured: zero.** `throughput/mp` over 6 runs each, mean 3.06 -> 3.07 M tasks/sec. Kept on
semantics, not performance -- and it is not a no-op, MSVC codegen confirms `xchg` -> `mov`. The
saving is invisible because every push also does `NotifyWorker`, which takes the target worker's
mutex (the lost-wakeup fix); a mutex acquire/release dwarfs one locked instruction. That is why
`PushBatch` batches notifies per segment, and why the awake-preference optimisation was worth 26% on
latency while this is worth 0%. **There is no third `pendingTasks`-shaped atomic left on the push
path** -- anything further has to come from the notify, not from counters.

**Removed the dead `nextId`** and `Thread::GenerateID()`: a process-wide atomic counter whose only
reader had no callers anywhere. Never executed, so it cost no time; it just implied task IDs existed.

**`CreateTask` now accepts a NAMED callable, not only a temporary.** This did not compile:

```cpp
auto body = [&] { /* ... */ };
sched.CreateTask(body);            // error: cannot bind rvalue reference to lvalue
std::thread t(body);               // ...while this always did
```

`CreateTask` decays `F` before instantiating `LambdaTask<std::decay_t<F>>`, so that class's
`LambdaTask(F&&)` is a plain **rvalue reference**, not a forwarding reference, and an lvalue had
nothing to bind to. Fixed by adding a `LambdaTask(const F&)` overload that copies. **No API change:**
`CreateTask`'s signature is untouched, an rvalue still selects `F&&` and still moves exactly as
before, and an lvalue can only select the new overload, so there is no ambiguity. Rebuilt against
the full JLib solution and Game01 to confirm no existing call site changed meaning.

Guarded by a new case in `SchedulerPrimitivesTest`, which is mostly a compile-time assertion -- if
the overload is removed the test file stops building.

**Sync-primitive test coverage.** `SchedulerConditionVariable` had **no tests at all**, and the
mutex was only exercised from bare threads -- never from a fiber, which takes an entirely different
path (suspend rather than spin-and-help). Added: fiber-vs-fiber contention, mixed fiber-vs-bare
contention, CV `Wait`/`Notify_One` with the mutex provably released while parked and re-acquired
before returning, `Notify_All` across 8 waiters, and a semaphore permit returned by a thread that
never took it.

Also pinned down, because it is a genuine footgun: **on a bare thread `SchedulerConditionVariable::
Wait()` does not wait for a notification.** It unlocks, takes one spin-help step, re-locks and
returns. So `if (!ready) cv.Wait(m);` is correct on a fiber and broken on a bare thread; only a
predicate LOOP is correct in both. There is now a test asserting exactly that.

One note on the tests themselves. The first fiber-contention test asserted "never two holders" and
passed -- and measured with the mutex REMOVED it still passed **1 run in 3**, because two fibers
frequently never overlap at all. A mutual-exclusion test that passes while the mutex is broken is
worse than no test. It now runs an unlocked probe first and asserts overlap was actually observed
before trusting the locked result, behind a rendezvous so both fibers are resident before either
starts.

**[CRITICAL] fixes a deterministic deadlock when a task waits from inside a worker.** Affects every
release. If any `noFiber` task calls `ParallelFor`, `WaitFor`, `SchedulerMutex::Lock` or
`SchedulerConditionVariable::Wait` while running on a worker, take this.

`noFiber` is the `CreateTask` default, so "push a task that runs a parallel-for" -- the obvious
thing to write -- hangs, and hangs 100% of the time:

1. `PushLocal` round-robins chunks across ALL workers, **including the calling one**.
2. One chunk lands in the calling worker's own **inbox**.
3. A `noFiber` task cannot suspend, so its `WaitFor` spins in place rather than parking, and the
   worker never returns to `Worker()`'s loop.
4. Inboxes are owner-drain-only and never stealable, so that chunk is now invisible to the entire
   pool -- and the only thread that could drain it is the one spinning on it.

The pool dump at the hang is unambiguous: the calling worker `AWAKE`, `busy=1`, one task in its lo
inbox, every deque empty and all 30 other workers `SLEEPING`.

`TryRunStolenNoFiberTask` now handles this. When a steal finds nothing AND the caller is a worker,
it moves its own inboxes onto its own deques (`Thread::DrainOwnInboxesToDeques`) and retries.
Deques are stealable, so the work becomes reachable by the whole pool and by the spinner's own
`GetTask`, which already scans every deque including its own. A `NotifyAll` follows a productive
drain, because a drained task may be fiber-backed and unrunnable by the fiberless spinner while
every other worker is parked.

**Non-workers skip the whole path** -- main and app threads have no inbox and are not part of the
hazard. The drain runs only after a steal has already failed, so it is off the hot path: `latency`,
`burst`, `fork-join`, `frame DAG` and both `ParallelFor` rows are unchanged. The single-`ParallelFor`
repro goes from hanging 3/3 to completing in ~25 ms, matching the fiber-backed control. Verified on
MSVC and on GCC/Linux, where the hang also reproduced.

**It is not really a `ParallelFor` bug, and a fiber-backed outer caller does not protect you.** An
8-case matrix, each case in a fresh process (see below), against a build with the drain disabled:

```
                                    drain OFF   drain ON
1 level  ParallelFor    [noFiber]      HANG        OK
2 levels ParallelFor    [noFiber]      HANG        OK
3 levels ParallelFor    [noFiber]      HANG        OK
manual WaitGroup fanout [noFiber]      HANG        OK     <- no ParallelFor involved at all
1 level  ParallelFor    [fiber]        OK          OK
2 levels ParallelFor    [fiber]        HANG        OK     <- outer caller is a FIBER
3 levels ParallelFor    [fiber]        OK          OK
manual WaitGroup fanout [fiber]        OK          OK
```

A hand-rolled `WaitGroup` fan-out hangs with no `ParallelFor` anywhere, so the trigger is the wait,
not the loop. And nesting under a fiber-backed outer caller still hangs, because `ParallelFor`'s
chunks are themselves `noFiber` tasks -- an inner `ParallelFor` therefore waits from inside one,
regardless of what the outermost caller was. The cases that pass with the drain off pass by
alignment, not by construction.

**Test-design note, because the first version of this matrix was wrong:** run ONE case per process.
`PickNextWorker`'s cursor is process-global, so whether a chunk ever lands back in the calling
worker's own inbox depends on scheduler state left by earlier cases. Run sequentially in one
process, all 8 cases passed with the drain disabled -- a clean false negative that would have
"confirmed" a fix that was not being exercised.

It pushes to the worker's OWN deque rather than going through `Requeue`, and that is about the
caller rather than about `Requeue`. `Worker()`'s pre-immediate drain uses `Requeue` correctly:
`PushToCore` stores `immediateCoresInUse` **before** `SetImmediateTask`, so that worker's core is
already flagged by the time it sees the task, `PickNextWorker` skips flagged workers, and the task
cannot come back to the inbox being emptied. A worker spinning in `WaitFor` carries no such flag --
it is running an ordinary task -- so `PickNextWorker` can select it and `Requeue` would return the
task to where it was found. `push_bottom` is monotone progress and needs no flag.

**`PushFork` is REMOVED.** Use `Push`. To place a task on a known worker, `Push(cpu_affinity, task)`
-- one-based, 0 meaning round-robin. Nothing in the library called it.

It placed a child on the CALLING worker, on the theory that the parent was about to `WaitFor` and
suspend, freeing that core, so the child would run there warm on the parent's data. "Fork" meant
fork-a-child, never split-and-join. Removing it took answering three separate questions, and the
answers are recorded here because each one looked like a reason to KEEP it at the time.

**1. It never worked.** It passed a **zero-based** worker index to `PushLocal`'s **one-based**
affinity parameter, in every release. The fiber path put the child on the NEIGHBOUR (`qIndex - 1`),
and from worker 0 it passed 0, which that parameter reads as "round-robin", so the child went
anywhere at all. The non-fiber path claimed `immediateCoresInUse[w]` and pushed the task to `w-1`,
so the claim was released by whatever unrelated task worker `w` ran next -- or, for a claim made
against worker 0, never released, silently removing that core from placement for the life of the
process. Every other caller of that parameter (`Push(cpu, task)`, `PushBatch`, `PushToCore`) already
used the one-based convention. **The one thing this function existed to do had never once happened**,
so no code has ever depended on it.

**2. Its fiber-only gate was load-bearing, and that is a mark against the design, not for it.** A
forked child lands in the target worker's inbox, and inboxes are owner-drain-only -- never
stealable. Only a parent that actually suspends can guarantee that worker returns to its loop to
drain it. Widening the gate to "any worker" makes a `noFiber` parent strand its own child in its own
inbox and spin on it forever. So the function was safe only for fiber parents, and a caller had no
way to see why.

**3. With the index fixed, it still lost every measurement.** In its own legal configuration -- fiber
parent, fiber children, recursive fork-join over 1M elements, best-of-7 -- self-spawning is slower at
every tree size and worst on small trees, where it serialises the top of the tree onto one worker
until steals redistribute:

```
leaves            2      4      8     16     32     64    128    256    512
PushFork / Push  1.01x  2.97x  4.63x  4.43x  3.58x  2.43x  1.82x  1.49x  1.48x
```

**And the locality premise itself was false.** Tested directly for the first time, with a parent that
writes a buffer and a child that immediately reduces THAT buffer (median us, spawn->join, 200 reps):

```
working set        Push   PushFork   ratio
16 KB  (L1)       10.40      6.70    0.64x
256 KB (L2)       33.50     29.60    0.88x
2 MB              203.90   199.20    0.98x
16 MB            1612.90  1611.30    1.00x
256 KB NO SHARING  61.10    55.10    0.90x   <- control saves MORE than the sharing case
```

The gain is a flat few microseconds that only looks large when the child is short, it does not grow
when the child reuses the parent's data, and it **disappears entirely under
`IdlePolicy::NoSleep`** (16 KB goes from 0.64x to 1.09x). It was never locality: it was one avoided
worker WAKE, worth about one `latency`-bench round-trip (~5 us), available only on an idle pool, and
paid for with a 1.5x-4.6x loss the moment the pool is busy -- which is exactly when a job system is
being used. A name that reads as "fork-join" attached to that trade had already misled this
project's own bench and its own docs.

`Task::isForked` goes with it -- nothing else set it -- shrinking `Task` by a byte inside the same
64-byte budget. `Thread::Worker`'s two `was_forked` core-releases are gone; `is_handling_fork`, which
serves `PushImmediate` and is sound because an immediate task cannot be stolen off the worker that
claimed the core, is untouched. `DESIGN.md`'s fork-join example now uses `Push`; it had been
teaching the slower path.

## 1.3.3 - 2026-08-14

**[CRITICAL] fixes tasks being stranded permanently in the inbox of a pinned worker.** Affects every
release before 1.3.3, and earlier ones worse. If you use `PushImmediate`/`PushFork` for a persistent
service -- an audio mixer, a network poll loop -- and submit in bulk, take this.

Before taking an immediate/fork task, a worker drains its inboxes and re-queues them. It has to:
inboxes are owner-drain-only and not stealable, and a pinned worker never returns to its loop to
drain its own. That drain stopped after ONE `BATCH_SIZE` pass, so anything past 64 entries stayed
behind, unreachable by anything.

The 64 cap was correct when it was written. `PushLocal` round-robins one task at a time, so an inbox
rarely held more than a handful. What changed underneath it is `PushBatch`: it now hands a large
contiguous RUN to a single inbox -- roughly 645 tasks for a 20k batch at the default
`minPerSegment` -- and before 1.3.0 it was worse still, putting the ENTIRE batch into one worker.
The overflow stopped being hypothetical without the drain ever being revisited.

For a short fork the leftovers are latency. For a **persistent** pinned service, which is what the
mechanism exists for, the pin never ends and those tasks are stranded for the life of the process,
hanging anything that waits on them. That is the particle-demo deadlock again, needing >64 queued
rather than >0.

The drain now runs until the inbox is actually empty. Termination rests on an invariant rather than
a count, so it is written down at the call site: `PushToCore` stores `immediateCoresInUse` BEFORE
`SetImmediateTask`, so by the time a worker observes `immediateTask` its own core is already marked
and `PickNextWorker` skips it -- `Requeue` cannot hand a task back to the inbox being drained. The
comment also says not to "fix" the loop by re-adding a cap, because the cap is the bug.

**This is the third defect this cycle of the same shape**, and the pattern is worth naming: code that
was correct when written, invalidated later by a change somewhere else that quietly rewrote its
preconditions. The `push_bottom` retry path was fine until fiber exhaustion made a worker re-pop the
same task forever. `task_to_run` surviving the loop's `continue` was harmless until the task stopped
going back to the same deque. This drain was sufficient until batches began arriving in runs. None
were wrong when written; each became wrong when something else moved, and nothing in the tests or
the type system noticed.

## 1.3.2 - 2026-08-14

**The AArch64 spin hint is now `isb` rather than `yield`.** Measured on an Android AArch64 device:
`throughput/mp` -- four producers, the most contended row in the benchmark -- went **3.43 to 5.15 M
tasks/sec, about 50%**.

`CpuRelax()` had always been `yield` on AArch64 because it is the architectural analogue of x86's
`PAUSE`. It is also, on nearly all AArch64 hardware, a no-op: `YIELD` is a hint asking an SMT
implementation to favour the sibling thread, and almost no ARM core is SMT. So every spin loop in
this scheduler -- the contended-lock path, the steal loop, and `IdlePolicy::NoSleep`'s idle search --
had been backing off *not at all* on ARM, running at full instruction throughput and re-reading the
contended line every iteration.

`ISB` is an instruction synchronisation barrier, not a pause. It flushes the pipeline, and that is
precisely why it works: it costs real cycles, so the loop actually slows down. Fewer iterations per
microsecond means fewer memory accesses and less coherence traffic, which is why the throughput win
and lower energy per spin are the same effect seen from two directions.

**How it was measured, because the obvious way to run this on a phone produces a fake result.**
Running the candidate first on a cool device and the control second on a throttled one manufactures
exactly this shape. Both builds were therefore run in BOTH orders, on a device left to cool first,
and `isb` came out ahead each time. `SchedulerBench` stamps the hint into its banner (`spin=isb`,
`spin=yield`, `spin=nop8`, `spin=pause` on x86) so a pasted result says which variant produced it --
the same reason it stamps the version.

`-DJLIBSCHED_ARM_SPIN_HINT=yield` restores the old behaviour; `=nop` selects a fixed sled, which is
the control that would distinguish "delay helps" from "`isb` specifically helps" and has not been
run. x86-64 is unaffected and always uses `PAUSE`.

**This does not change the mobile power story, and `Sleep` remains the default.** The two are
different layers: `IdlePolicy` decides whether a worker spins at all -- under `Sleep` it parks on a
condition variable, the kernel deschedules the thread, and the core is free to idle -- while
`CpuRelax` only decides how to spin during the short search window before parking. `ISB` is not a
sleep and does not pretend to be one. The instruction that does sleep a core is `WFE`, and it is
deliberately not used here: it suspends the core but leaves the thread scheduled, so for a wait of
unknown length -- which is what an idle worker faces -- descheduling via futex is strictly better,
and on mobile it is what lets the SoC drop to a low-power state at all.

Every ARM measurement in this project before now was taken with a spin loop that did not spin.

## 1.3.1 - 2026-08-14

**[CRITICAL] fixes a deadlock when more tasks block at once than the fiber pool holds.** A suspended
task keeps its fiber, so the number of tasks that may be blocked SIMULTANEOUSLY is capped at the
pool size. Past that cap the pool hung.

THIS AFFECTS EVERY RELEASE BEFORE 1.3.1, NOT JUST 1.3.0. The defect dates to at least 2026-07-06 --
it was FOUND while testing 1.3.0, not introduced by it -- so 1.2.3 and earlier are affected too.
Anyone on any earlier version who fans out more blocking tasks than `64 x workers` should take this;
the symptom is an unexplained stall, not a crash.

Three separate defects, all found by writing the test for a limit that had just been documented:

**The pool hung instead of degrading.** When `AcquireFiber` failed, the worker pushed the task onto
the BOTTOM of its own deque -- the LIFO end it pops from -- so the next pop returned the same task.
The worker span pop/no-fiber/push forever and never reached its inbox drain, which is exactly where
`SignalAll` deposits the resumed tasks whose completion would have freed the fibers it was waiting
for. Every worker doing that at once is a deadlock. The comment there called the condition
"transient -- fibers are in use and will free up", and that claim had already been copied into the
warning text and the README before anyone tested it. Now routed through `Requeue`.

**The retry path leaked `task_to_run`.** It is a `thread_local` that survives the loop's `continue`,
and the loop top tests it before searching, so a worker kept re-processing a task it had already
queued and added another copy each pass. The old `push_bottom` hid this, because the task it
re-popped WAS the duplicate; routing the task anywhere else turns it into one `Task*` live in two
queues and run by two workers. It surfaced as a segfault the moment the deadlock was fixed.

**The fiber pool was sized from the machine rather than the pool.** `coreCount` was
`hardware_concurrency() - 1` instead of the resolved worker count, so `Init(4)` on a 32-thread
machine allocated 1984 standard + 248 heavy fibers -- about 248 MB of commit for four workers -- and
`Init(8)` on a 128-thread machine allocated roughly 1 GB. That is backwards for the embedding case
this library exists for. It remains fully automatic and the default `Init()` is unchanged; only an
explicitly smaller pool costs less than it used to.

**The exhaustion warning now prints once per process.** It fired on every failed acquire, and since
the caller re-queues and retries, a short pool spun through that path millions of times -- so the
warning made the stall slower and buried the one line explaining it.

**Test.** `SchedulerPrimitivesTest` now pushes more blocking tasks than the pool has fibers and
requires completion. The watchdog is the assertion: there is no honest threshold for "too slow", but
"finishes at all" is the property that broke. It fails on any earlier version by timeout and on
the intermediate fix by segfault, so it tells the three states apart.

### Also

`Event::SignalAll` re-queues woken fibers in one batch instead of one at a time. Each individual
re-queue is a placement, an inbox push, a `seq_cst` flag and a condition-variable signal, and a
broadcast waking 64 fibers paid all of it 64 times, serially, on the signalling thread. Measured on
the marl blocking comparison: the broadcast-heavy case went 13.5 -> 8.5 ms. Cases with a single
broadcast per batch are unchanged, so this helps where wakes are frequent rather than large.

`PushBatch` gained a `hiPri` parameter and `PushArray` gained one too, both defaulting to low.
`PushBatch` routed every batch into the low-priority inboxes unconditionally, which was a silent
priority inversion for anyone batching high-priority work.

`WaitOnEvent` and `WaitOnEventArmed` gained `Event&` overloads; the name-taking versions forward
through `GetEvent`. A caller waiting on the same event repeatedly can hoist the lookup to startup and
keep a global mutex and a string hash off the hot path. The reference is stable for the process
lifetime -- the registry owns `unique_ptr<Event>`, so a rehash moves the pointer, not the object.

## 1.3.0 - 2026-08-14

**[CRITICAL] fixes a lost wakeup that could strand a task permanently.** A worker could park while
its own inbox held work, and inboxes are not stealable, so that task never ran and everything
waiting on it hung. Reproduced at roughly 25% of runs on a 4-thread CI runner and 3% on a 32-thread
desktop; anyone on 1.2.0 through 1.2.3 should take this.

**The captured evidence, which is what finally identified it.** A watchdog now dumps pool state when
a benchmark section overruns, and a hang printed:

```
pendingTasks=36572  workers=3
  q  state      queued busy imm run   inbox(hi/lo)
  1  SLEEPING     0      0    0   1     0/1        <-- SLEEPING WITH WORK
```

Not a missed notify -- a missed FLAG. The item was in the inbox and `hasQueuedWork` was 0.

**The mechanism, and it is a memory-ordering bug rather than a logic one.** The clear was
`hasQueuedWork.store(false, memory_order_relaxed)` while `MarkQueuedWork` sets it `seq_cst`. A
relaxed store is not ordered against the loads that follow it, so it may SINK PAST the inbox drain:

```
worker: drain inbox                 -> empty
push:   queue the item; set flag    -> seq_cst
worker: the stale relaxed clear lands, wiping the flag that was just set
worker: parks with the item in its own inbox and no signal for it
```

So the flag was set correctly and cleared LATE, not early. The comment above that line argues that
"a push landing after the clear re-arms this flag", which is sound only if the clear is genuinely
ordered before the search it is meant to precede -- and relaxed gave it no defined position at all.
It is `seq_cst` now.

That ordering fix alone is not the guarantee, which is why the park decision also consults the
inboxes. The flag and the queue are separate objects and no single operation observes both, so a
push whose queue write is not yet visible to the drain can still be missed. Ordering the clear
removes the reordering that made the window easy to hit; consulting the queue removes the window.

**Why the flag existed at all is the interesting part.** The park decision originally consulted the
queues directly and the flag was added later as an optimisation -- but it REPLACED the queue check
rather than short-circuiting it. That is the failure mode for this kind of change: once the cheap
signal is the only signal, losing it is not a slow path, it is a lost task. The fix restores what
the optimisation should have been: flag first, queue as the truth. Both park decision points get it,
the unlocked recheck and the `cv.wait` predicate. Being under the mutex does not save the second
one, because the mutex orders you against a NOTIFIER and the failure is a push that never notifies.

Cost is two `empty()` calls on the park path only, immediately before a thread would otherwise
block. Nothing is added to the hot loop, which already drains the inbox every iteration.

**The race PREDATES the awake-preference rework, and this was measured rather than argued.** The
commit timeline was suggestive in the wrong direction: `throughput/mp` -- the first bench to push
from several workers at once -- ran for sixteen hours clean, and CI hangs began 74 minutes after the
rework landed. That correlation is a coincidence.

Checking out the rework's parent commit in a worktree (mp present, neither the skip nor this fix)
and running the same loop gives **5 hangs in 60 runs**, against 2 in 60 with the rework in place. So
the flag/queue race was already there, and the rework made it slightly LESS frequent, not more.

Worth recording because it was nearly concluded twice from reasoning alone. An earlier experiment
that disabled only the notify skip also produced MORE hangs, which pointed the same way, and was set
aside because the timeline looked incriminating. It should not have been: a direct measurement beats
a commit-date correlation, and the worktree test that settles it takes ten minutes.

What the rework DID do is make the bug findable. `throughput/mp` is the only bench that pushes from
several workers at once, and the section watchdog plus `DumpPoolState` are what turned a silent
30-minute CI timeout into a named worker asleep on a non-empty inbox.

Verified: 80 consecutive clean runs on Windows and 40 on Linux at the pool size that reproduced it,
against a prior rate of roughly 2 per 60.

**`TaskScheduler::DumpPoolState()` is kept as a permanent diagnostic.** A stack trace of this bug
shows three threads sitting in `cv.wait`, which is what you already knew; the state dump names the
worker that is asleep holding work. It found in one run what three rounds of reasoning had not.

**`GetTask` still carried the old 64-CPU mask.** The helper-steal path asked "is the calling thread
on a P core or an E core" with `isPCpu[CurrentCpu() & 63]`. `isPCpu` is sized to `CpuMask::kMaxCpus`
now, so above 64 logical CPUs that mask silently folded the caller onto another core's entry -- CPU
100 read slot 36 -- and answered the question about the wrong core. It is bounds-checked against the
table's actual size, with out-of-range degrading to "P", matching what `isPCpu` already defaults to
on a non-hybrid or query-failed machine.

Never a crash, only a worse steal decision, which is why nothing surfaced it: the multi-group work
converted every mask it could find, and this one reads like arithmetic rather than a CPU bound. A
sweep for the same pattern found no others -- the remaining `1ULL << bit` sites are `GROUP_AFFINITY`
masks, which genuinely are 64 bits per group, and `CpuMask::BitOf`, where `& 63` is the definition.

**False-sharing padding was half the size it needed to be on Apple Silicon.** Seven places used
`alignas(64)`; M-series cores have **128-byte** cache lines, so on that platform two supposedly
separated objects shared a line and the padding bought nothing.

`TaskDeque` is the case that matters. Its `top_` and `bottom_` are padded apart specifically to keep
the owner's end off the thieves' end, which is the whole reason a Chase-Lev deque is cheap for its
owner. On a Mac that separation silently did not exist. The sharded `LiveCounter` and
`StealCounters` arrays had the same problem, at two counters per line instead of one.

All seven now use `platform::kCacheLine`, one constant set to 128. Over-aligning on 64-byte-line
machines costs a little memory and measured nothing: latency 4.67 to 4.73 us, frame DAG 21.5 to
21.8, fork-join and multi-producer throughput unchanged across three runs. It is arguably correct
there too, since Intel's adjacent-line prefetcher pulls lines in pairs, making 128 the effective
sharing granularity on much x86 hardware regardless of the architectural line size.

Deliberately not `std::hardware_destructive_interference_size`: it is the standard spelling, but
libc++ was late to implement it and AppleClang is precisely the platform this exists for.

Found because a question about mobile cache sizes prompted checking line sizes, which are a
different thing. The phone was fine -- 64-byte lines, `Task` still one line. The Mac was not.

**The per-worker fiber cache size is now actually computed.** `StartWorker` derived it from
`((workerThreads * 72) / workerThreads) * 0.5`, in which the worker count cancels out: it was the
constant 36 at every pool size, and the `< 16` floor beneath it could never fire.

Mostly harmless, because 36 sits near the 32 the intended formula gives at the default pool size.
But it diverged badly for an explicitly small pool. Fibers are allocated from HARDWARE thread count,
not pool size, so `Init(4)` on a 32-thread machine still creates 1984 standard fibers; those four
workers should cache roughly 248 each and instead cached 36, sending them to the global pool
constantly. A performance cost only, never a fault, which is why nothing ever surfaced it.

It is now computed in `StartPool` as half of each worker's fair share of the standard pool and
passed to `StartWorker`. That is the only scope that knows both the fiber budget and the worker
count, which is precisely why the old inline version could not express it. Half rather than the
whole share so an idle worker's hoard is not the reason a busy one has to go to the global pool.
`ThreadLocalCache::Initialize` still clamps above and floors below, so this only has to be sane.

Verified at pool 31 and pool 4 on Windows and Linux; no measurable change at the default, which is
expected given 36 versus 32.

**New `SchedulerPrimitivesTest`, covering the least-tested code in the library.** The fiber-aware
locks had no tests at all, which is uncomfortable given 1.2.3 added three behavioural guards to them
and 1.2.4 touched them again, both verified by little more than "the benchmark still completes" --
and the benchmark barely takes a lock. 18 checks: mutex basics, two-thread contention asserting a
second holder is never admitted, semaphore counting, a producer/consumer permit released by a
different thread than took it (the case that makes permits unownable, so worth proving still works),
and `ScopedPermit` scope, balance over 1000 cycles, and correct no-op from a fiber.

**The one that earns its place is a regression test for the 1.2.3 self-deadlock**, and it is
verified against the bug rather than merely passing: with `t_heldMutexes` removed from
`ContendedSpinStep`, it hangs and the watchdog exits nonzero in 30 seconds. Restore the guard and it
passes. A test that has never been shown to fail proves nothing, which is the same rule the GenMC
models follow.

Passing is deterministic there; failing is probabilistic, since the broken version only hangs if the
helper happens to steal one of the queued tasks that wants the held lock. Hence 256 of them and a
300 ms hold on the second mutex.

**Every blocking case runs under a watchdog** that `_Exit(1)`s after 30 seconds. A regression in this
code does not produce a wrong answer, it produces a hang, and a hang in CI burns the whole job
timeout before reporting anything -- which is precisely what the 1.2.0 macOS bug did for thirty
minutes a run. Wired into all four CI targets.

### New: `IdlePolicy` -- workers do not have to sleep

`SetIdlePolicy(IdlePolicy::NoSleep)` makes idle workers keep searching instead of parking on their
condition variable. Default stays `Sleep`. Measured on the reference machine, medians of three:

| | `Sleep` (default) | `NoSleep` | |
|---|---|---|---|
| Round-trip latency | 4.68 µs | **1.15 µs** | 4.1x |
| 6-node frame DAG | 22.50 µs | **7.76 µs** | 2.9x |
| 1M fork-join | 0.22 ms | **0.07 ms** | 3.1x |
| throughput/1p | 1.21 M/s | **6.30 M/s** | 5.2x |
| throughput/mp | 8.89 M/s | 12.40 M/s | 1.4x |
| burst from idle | 11.6x of 16 | 12.6x of 16 | flat |

The wake path was the largest single cost in the scheduler and nothing here had measured it. That is
the honest summary: 3-5x on everything latency-shaped, for the price of holding the cores.

**Burst barely moved, and that is the control.** Its shortfall was predicted BEFORE measuring to be
frequency scaling rather than wake latency -- 16 heavy tasks at once settle toward base clock on a
chip at Intel spec power limits, where one task alone boosts. It stayed flat while everything else
moved 3-5x, which is what makes the other rows a signal rather than drift.

**The default stays `Sleep`, deliberately.** Spinning workers are a battery and thermal problem on
the ARM64/Android target, where throttling costs more clock than the wake ever costs in dispatch;
they starve whatever else the host runs; and they make the oversubscription policy incoherent, since
it reserves a core per persistent busy thread and this would make every worker one. `NoSleep` is for
an application that owns the machine.

**Negative result: there is no middle setting, and not for lack of trying.** A `SpinBriefly` mode
was built and measured -- search for a configurable number of microseconds, then park -- on the
classic spin-then-block reasoning that spinning for the cost of a block is 2-competitive. It was
worse than BOTH neighbours, monotonically worse as the budget grew (frame DAG, µs per graph:
`Sleep` 22.5 | 2 µs 23.6 | 5 µs 23.6 | 20 µs 27.3 | 100 µs 34.4 | `NoSleep` 7.8). The 2-competitive
argument assumes spinning is free for everyone else on the machine, and with 31 workers it is not:
spinners burn memory bandwidth and contend on steal CASes against the workers that actually have
work, then park anyway and pay the wake plus continuous park/unpark cv churn. Both extremes avoid
one half of that; the middle collects both. Removed rather than shipped as a trap for anyone
reasonably expecting a compromise setting.

`Pause()` parks regardless of policy. This is NOT a timed wait: it changes whether a worker parks,
never how long it stays parked, so the park is still an unconditional `cv.wait` and a lost wakeup
still hangs visibly rather than being papered over by a timeout.

The primitives suite now takes a `nosleep` argument and CI runs it twice, once per policy. The
policy changes the park path, which is precisely where the 1.2.0 lost wakeup lived, so the existing
blocking cases are worth more there than a bespoke test would be.

### Removed: `WaitAll()` and the `pendingTasks` counter

**API REMOVAL, and the reason 1.2.4 became 1.3.0.** `TaskScheduler::WaitAll()` is gone. It spun on a
global `pendingTasks` atomic that every push and every completion had to maintain, and it had no
callers anywhere -- not in the library, the bench, or the tests. Wait on a `WaitGroup` instead, which
is scoped to the work you actually submitted; "all work everywhere" stops being an answerable
question as soon as two systems share the pool.

It was not removed for tidiness. That counter was **24 ns per task, 27% of the entire per-task
cost** -- one contended cache line bounced between every producer and all 31 consumers, twice per
task. Measured by deleting it and re-running, after a first estimate of "3-9 ns" from a
consumer-only proxy proved badly wrong:

| | before | after |
|---|---|---|
| `throughput/mp` (4 producers) | 3.24 M/s | **10.61 M/s** |
| `throughput/bt` (`PushBatch`) | 11.81 M/s | **12.42 M/s** |
| `throughput/1p` (1 producer) | 0.81 M/s | **0.89 M/s** |
| per-task total, 20k tasks | 88.9 ns | **67.2 ns** |

The 3.3x on multi-producer is the counter's real signature: four producers issuing `fetch_add` on
the same line that 31 workers are issuing `fetch_sub` on.

`DumpPoolState` now SUMS the per-worker deques instead of printing the counter. That is strictly
better diagnostics -- it says where the work is, not just how much -- and it costs nothing, because
it only runs when a watchdog has already given up. The per-worker `<-- SLEEPING WITH WORK` line,
which is what actually identified the lost wakeup above, is untouched.

### Submission throughput

**`PushBatch` now spreads a batch across workers instead of stacking it on one.** It picked a single
worker and pushed every task into that worker's inbox. Inboxes are owner-drain-only, so that one
worker then moved the whole batch into its local deque `BATCH_SIZE` at a time while every other
worker stole from it ONE ITEM AT A TIME -- a 20k-task batch became one drain loop plus ~20k
single-item steal CASes contending on a single deque, with the producer that had just paid to build
the batch sitting idle. Segmenting costs nothing (the `next` links were being written anyway, just
as one long chain instead of several short ones) and turns one hot deque into several warm ones.
Measured at 31 workers, 20k tasks: dispatch **90 ns -> ~56 ns per task**.

Batches are NOT split below 64 tasks per segment. Each segment costs a `NotifyWorker`, which takes
the target's mutex, and splitting a small batch every which way pays those wake-ups for parallelism
it is too small to use -- that was a measured regression on small batches before the floor existed.
Explicit `cpuaffinity` is still honoured exactly and never spread; it is an explicit request.

**`ParallelFor`'s flat path submits with one `PushBatch`** instead of `numTasks-1` individual
`Push` calls, collected into a stack buffer so nothing is allocated on a path that runs per frame.
It passes `minPerSegment=1`, not the default 64: this path only runs at <= 2 tasks per worker, so a
64-task floor would collapse the batch onto ONE worker -- strictly worse than the per-task `Push` it
replaces, which at least round-robined.

Reported as neutral, because that is what it measured. ParallelFor floors `chunkSize` so
`numTasks <= workers*4`, and dispatches to fork-join above `workers*2`, so the flat path only ever
sees small task counts and the bench moved no further than its run-to-run spread. It is kept for
being strictly fewer notifies at equal distribution, not for a number.

**Fork-join's dispatch threshold was re-tested and stays.** The comment justifying it blames flat's
"O(#tasks) serial CreateTask+Push+NotifyWorker on one thread", and `PushBatch` removes exactly that
notify storm -- so the threshold might have become obsolete. Measured instead of assumed: forcing
the flat path for a 79-task ParallelFor gives **0.485 ms against fork-join's 0.400 ms**, with 26%
spread against 7%. Fork-join still wins and the threshold is still earned. The likely mechanism,
untested: from an idle pool flat issues every notify serially to a SLEEPING worker, while fork-join's
cascade wakes a few, and the deeper levels' notifies then hit already-awake workers and are skipped
by the awake-preference fast path.

**New: `PushArray(begin, end, chunkSize, fn, wg)`.** Submits a range as `ceil(n/chunkSize)` tasks,
each looping its own chunk and calling `fn(i)` per index, instead of one task per item. Per-task
overhead is a per-TASK cost, not a per-item one -- it buys queueing, stealing, completion accounting
and reclamation, none of which n known-up-front items each need separately -- so chunking divides the
whole figure by `chunkSize`. Measured per item at 20k items: **80 ns at chunk 1 -> 14 ns at 8 ->
4.9 at 32 -> 1.3 at 128**. It is the fire-and-forget sibling of `ParallelFor`, which probes the work,
picks its own split, runs a chunk on the calling thread and blocks; use `PushArray` when the caller
has other work to do, wants to submit several ranges before waiting on them together, or already
knows its own grain.

**Task completion can skip the virtual destructor** when the concrete type provably has nothing to
destroy (`Task::trivialDtor`, set by both `CreateTask` overloads; the lambda overload keys off
`std::is_trivially_destructible_v<F>`). Reported for completeness rather than as a win: A/B measured
**~2.6 ns against a 49-61 ns run-to-run spread**, so it is not distinguishable from noise here. Kept
because it is free and correct, and the default is 0 -- "not known to be trivial" -- so a task built
anywhere other than `CreateTask` keeps paying the call and an oversight can only ever be slow,
never wrong.

Not done: batching the per-task `WaitGroup` decrement. A contended shared atomic measures ~3-9 ns of
the ~80 ns total here, and deferring a completion signal so it can be flushed in bulk reintroduces
exactly the lost-wakeup shape this release exists to fix. `PushArray` already collapses those
decrements to one per chunk, which is the same benefit with none of the risk.

Tests: `PushArray` index coverage (exactly once, no gap and no duplicate -- the failure mode of a
chunked API is an off-by-one at a chunk boundary, which no timing benchmark would catch), chunk
larger than the range, empty and inverted ranges, `chunkSize == 0`, and a 5000-task spread
`PushBatch` arrival check.

### Comparison harness

**New opt-in `bench/compare`** measuring against enkiTS -- the closest architectural peer, so a gap
localises to implementation rather than design. Requires `-DJLIBSCHED_ENKITS_DIR`; without it the
target does not exist, so normal builds and CI never see it, and enkiTS is not vendored.

It takes an optional `nosleep` argument, and the file says in capitals not to quote that run as a
comparison. Both schedulers live in the harness's one process, so under `NoSleep` JLib's workers spin
through enkiTS's benchmarks as well -- 63 threads on 32 CPUs. The confound is visible in enkiTS's own
column, which cannot legitimately move when a JLib setting changes and does: ranged per-item 15.3 →
8.7 ns, latency 21.4 → 18.2 µs. enkiTS measured ~40% FASTER because JLib stopped sleeping, almost
certainly core parking. Head-to-head numbers come from the default run; `NoSleep` figures come from
`SchedulerBench`, which runs JLib alone.

Five workloads, with predictions recorded in the file before measuring and every harness fault
recorded next to the number it corrupted -- the first draft reported enkiTS as 15x slower and all
four faults were the harness (N waits against one, `WaitforAll` not being a per-batch wait,
`sleep_for(20us)` costing a full 15.6 ms Windows quantum, and mismatched baselines). It also fixes
one pointed the other way: the JLib column now reports `PushBatch` as well as per-task `Push`.

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
