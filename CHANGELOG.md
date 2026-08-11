# Changelog

Correctness fixes are marked **[CRITICAL]** with a note on what breaks without them —
downstream users (forks/ports) should treat those as must-pull.

## 1.0.0 — 2026-08-09

First tagged release. The version number is a statement about API stability, not about the code
suddenly becoming ready: the scheduler has been running a real engine for months. What changed is
that it now builds and passes its suite on four verified platforms with CI enforcing that on every
push, which is the point at which promising not to break the API costs something and therefore
means something. Breaking changes from here need a 2.0.

Everything below this heading shipped in 1.0.0.

**BREAKING (the last break before the promise): `fastJob` is now `noFiber`.** Same meaning, same
polarity, same `true` default — only the name changed. `CreateTask`'s parameter rename is invisible
to callers (C++ has no named arguments), so this only affects code touching `Task::noFiber`
directly, which realistically means nobody. Renamed now because it was the last free moment: the
old name advertised a benefit while the flag's real content is a *constraint* — a task with no
fiber underneath cannot suspend, and getting that wrong fail-fasts with no message. `noFiber` names
something checkable and puts the constraint at the call site.

**BREAKING, same pass: `TaskScheduler::TryRunStolenFastJob()` is now `TryRunStolenNoFiberTask()`.**
A public method, but with no callers in this tree or in any consuming project — the earlier rename
was case-sensitive and left every capital-F identifier behind. Renamed at the same last-free moment
and for the same reason: the name should say the task runs without a fiber, which is what makes it
safe for a fiberless spin-waiter to call.

**[CRITICAL] for consumers: `Task`'s layout changed, though its size did not.** It gained a
`Task* nextWaiter` link in what was already tail padding, so `sizeof(Task) == 64` still holds and is
still asserted. But the layout moved, which means a consuming project must be CLEAN-rebuilt rather
than incrementally rebuilt. A stale object file compiled against the old layout links without
complaint and reads the wrong offsets at runtime.

**`Event` is now lock-free and allocation-free.** It was a `std::mutex` around an `unordered_set`,
so every suspend allocated a hash node and every signal took a lock that an external thread — a GPU
fence callback, say — could contend. It is now an intrusive push-only stack threaded through
`Task::nextWaiter`: `AddWaiter` is one CAS, `SignalAll` is one `exchange`.

It needs no epoch reclamation, hazard pointers or tagged pointers, and the reason is load-bearing
rather than incidental. ABA bites on *pop* — read head, read `head->next`, CAS, and in that window
the node can be freed and a recycled address put back. Nothing here pops: `SignalAll` takes the
entire list in one exchange, so the window does not exist. That holds **only** while there is no
remove-one-waiter operation, which is why `Signal(Task*)` and `RemoveWaiter` were deleted rather
than kept — both had zero callers, and either one reintroduces remove-from-middle and the whole
hazard-pointer problem with it. The header says so, because they are exactly what someone would
helpfully restore.

Covered by `tests/event_smoke.cpp`, which signals while registrations are still in flight rather
than waiting for them to settle — the interleaving that could actually lose a waiter. It runs on all
four CI platforms, two of them weakly-ordered ARM64, which is the only reason coverage of new atomic
code is worth much: x86 is TSO and hides a missing barrier.

**`WaitGroup` moved to its own header, and `src/Task.cpp` became `src/WaitGroup.cpp`.** That file
contained exactly one function — `WaitGroup::WakeAll()` — and nothing about `Task` at all, so anyone
opening it looking for task internals found a synchronisation primitive. `WaitGroup` is a sibling of
`Event` and `DirectEvent` and now has a header like they do.

No caller changes: `TaskScheduler.h` includes `WaitGroup.h`, exactly as it already included
`DirectEvent.h`. `Task.h` forward-declares it, since a `Task` only holds a `WaitGroup*` — which also
keeps `<mutex>` and `<unordered_set>` out of the seven headers that include `Task.h` without ever
naming a `WaitGroup`. CMake needed no change (it globs `src/*.cpp`); `Scheduler.vcxproj` lists
sources explicitly and was updated.

**`GetEvent()` is usable without a second include now.** `TaskScheduler.h` returns `Event&` but only
forward-declared `Event`, so callers had to include `Event.h` themselves or get an incomplete-type
error. The cause was a cycle — `Event.h` included `TaskScheduler.h` — and it was unnecessary: all
`Event.h` ever needed was `Task` and `Fiber`, so it now includes `Fiber.h` and `TaskScheduler.h`
includes `Event.h`. One header is enough for all three primitives.

**`SchedulerMutex::Lock`'s documentation was wrong.** It said the caller must be a fiber. It has
always had a non-fiber branch that spins while running stolen `noFiber` work. Corrected — along with
the consequence, which is that it is the wrong lock for a short critical section reachable from a
foreign thread, since a driver callback contending on it would start executing tasks from the graph.

**`ParallelForFJ` is not experimental** and the header no longer says it is. `ParallelFor`
dispatches to it automatically past ~2 tasks per worker; below that the flat path is ~14% faster,
above it flat is ~8x slower at ~15k tasks. The doc comment had been contradicting the code beside it.

**Named events: the contract is now written down.** `GetEvent`/`WaitOnEvent` keep one entry per
distinct name and never evict — correct and cheap for a bounded, static set of rendezvous points,
which is what they are for. Minting a name per operation (`"fence_" + counter`) instead grows the
map without bound and eventually convoys on `registryMtx` during a rehash, which presents in a
debugger as a deadlock after about an hour of uptime. `WaitOnEventDirectArmed` is the API for
per-operation waits: pooled, no name, no map, no global lock. No eviction policy is needed here
because the unbounded case has its own API — that was always the design, it just was not documented.
A debug/development build now warns once at 4096 named events, naming the last key inserted.

**The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`.** Every header is installed
because the supported ones need them to compile, but the rest are implementation detail and are not
covered by the version promise. Stated explicitly so that fixing internals later is not a breaking
change by accident.

### macOS / Apple Silicon support
Builds and runs on macOS arm64, verified in CI on `macos-14` (AppleClang). The AAPCS64 context
switch is unchanged — the calling convention belongs to the instruction set, not the kernel, so
Apple arm64 uses the same `src/posix/aarch64/ContextSwitch.S` as Linux; only the Mach-O directives
behind `#if defined(__APPLE__)` differ, and those now genuinely take effect (see the `.s`→`.S` note
below). The ABI harness passes at `-O0` and `-O2` on both ARM64 platforms.

New `src/darwin/` OS layer, selected by CMake alongside `src/win32/` and `src/posix/`, while the
ABI layer under `src/posix/<arch>/` is shared by every POSIX target. `Topology.cpp` there reads
`sysctl` rather than sysfs. `Thread.cpp`'s affinity helper now takes a plain 64-bit mask instead of
a `cpu_set_t`, which keeps the policy switch free of a type macOS does not have.

**Placement is a documented no-op on macOS.** There is no thread-affinity API on Apple arm64 —
`THREAD_AFFINITY_POLICY` still links but has done nothing since Apple Silicon; the kernel owns
placement and takes intent through QoS classes. Topology reports SMT honestly (Apple Silicon has
none) but leaves the P/E class table empty: macOS publishes per-performance-level CPU *counts*, not
a logical-CPU-index → level mapping, and a class table built on a guessed ordering could not be
acted on even if it were right.

### Benchmark
`--help` used to fall through to the default and start a multi-minute run under a policy you had not
chosen; it now prints usage and exits, and an unknown argument exits non-zero.

**It defaulted to the wrong affinity policy.** `hard`, while the library defaults to `Ideal` — so
every casual run, including the first third-party numbers that came back, measured a policy the
library does not use and which measured ~45% worse on wake latency. Now defaults to `ideal`, and the
help text carries the measurement so the old assumption does not get re-derived.

New `[poolSize]` and `nosweep` arguments. Pool size is for sweeping worker count against latency and
the frame DAG, which is a DIAGNOSTIC — do not ship a small pool, it starves everything that is not a
tiny graph.

**`ParallelFor` is now reported as two cases instead of one misleading number.** The old single line
measured a 64 MB, ~2-flop-per-element kernel, which is capped by the memory system rather than the
scheduler: it reads below 1.00x on machines with a small last-level cache (0.75x on a Ryzen laptop
APU, 1.09x on an M1 Air) and above it on a large one (~3.4x on a 36 MB L3). A reader saw that near
the top of the output and concluded the feature does not work, while the crossover sweep at the
bottom of the same run showed up to 16x. It now prints a labelled memory-bound line and a
cache-resident compute-bound line, so the difference reads as the workload rather than the library.

**The crossover sweep was reporting invented numbers.** It took the first cell above `1.00x` as the
crossover, and run-to-run noise supplies that immediately — an M1 Air run claimed `trivial` won at
one microsecond of total work. A crossover now has to clear 1.15x *and* be confirmed by the next
size up, and the header states the rule.

### The benchmark no longer requires C++20
`std::atomic<double>::fetch_add` (C++20, P0020R6) is absent from AppleClang's libc++, and it was the
harness's only C++20 dependency. Replaced with a compare-exchange loop — which is what `fetch_add`
lowers to anyway on hardware with no native atomic FP add — so the bench is C++17 like the library.
Verified equivalent: the crossover sweep's sink prints the same value on x86-64 and AArch64.

### AArch64 support — the scheduler now builds and runs on ARM64
Full benchmark suite passes on Android/Termux (clang, AArch64), including recursive fork-join —
fibers suspending and resuming through a new hand-written AAPCS64 context switch under the real
scheduler. Windows x64 and Linux x86-64 rebuilt and re-run, unchanged.

New `src/posix/aarch64/{ContextSwitch.S,FiberInit.cpp}`: a 176-byte frame (x19–x30, d8–d15, FPCR).
Three things differ from the x86 ports and are worth knowing if you port further — the return
address lives in x30 rather than on the stack, so the trampoline address is seeded into a *register*
slot; there is no `call` to misalign the stack, so the trampoline needs no alignment compensation;
and SP must be 16-byte aligned at all times, not merely at call boundaries, because misalignment
faults rather than merely violating convention.

**Source layout change (affects hand-rolled builds).** `src/posix/` is now split by architecture:
`src/posix/x86_64/` and `src/posix/aarch64/`, each holding `ContextSwitch.S` and `FiberInit.cpp`.
The OS layer (`Topology.cpp`) stays shared, since Linux/x86-64 and Linux/AArch64 differ only in the
switch. If you add sources by hand rather than via CMake, add **one** architecture directory.

**[CRITICAL] for hand-rolled and out-of-tree builds.** Two definitions of `Fiber::Init` in one
static library is *not* a link error — the linker takes whichever archive member it reaches first
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
results therefore hold across two toolchains and two libcs — GCC/glibc in CI and Clang/bionic on
Android — which is what makes the ABI claim more than one machine's anecdote. Raspberry Pi needs
nothing extra: it is the same Debian-family aarch64/glibc configuration as the CI runner.

**No AArch64 performance numbers are published, deliberately.** Android cgroups own thread
placement, so affinity requests from an unprivileged app are routinely ignored, and thermal
throttling moves results mid-run. The ARM claim here is correctness only.

## 2026-08-07

### Linux support — the scheduler now builds and runs on Linux x86-64
Every benchmark passes on Ubuntu 24.04 / GCC 13, including recursive fork-join, i.e. fibers
suspending and resuming through the hand-written context switch under the real scheduler. Windows
is unchanged and unaffected.

- **Hand-written System V AMD64 context switch** (`src/posix/ContextSwitch.s`, GAS, Intel syntax).
  **`ucontext` was rejected on a MEASUREMENT, not on its POSIX deprecation:** `swapcontext` saves
  and restores the signal mask, which is a `sigprocmask` **syscall on every switch** — measured at
  **120.3 ns vs 8.0 ns for this implementation** — roughly an order of magnitude. Treat that ratio
  as indicative rather than exact: it compares a pure user-mode register swap against a **syscall**,
  measured under WSL where kernel transitions are inflated, so bare metal would narrow the gap. The
  mechanism is the point and does not change. Boost.Context was declined to keep
  the dependency count where it is.
  The SysV switch is *shorter* than the Win64 one: callee-saved is `rbx/rbp/r12–r15` only (RDI and
  RSI are argument registers here), **every XMM register is caller-saved so the whole 160-byte
  `xmm6–15` block disappears**, there is no shadow space and no TEB stack-bounds fixup. MXCSR and
  the x87 control word *are* still preserved — they are one physical register pair shared by every
  fiber on a worker, so a fiber that sets a rounding mode and yields would otherwise leak it.
- **Platform split by DIRECTORY** — `src/win32/` and `src/posix/`, each holding `ContextSwitch`,
  `FiberInit` and `Topology`. `include/platform.h` is the single place that tests the OS
  (`JLIB_PLATFORM_WINDOWS` / `JLIB_PLATFORM_POSIX`) and wraps the virtual-memory primitives, so
  `FiberStackArena` — alignment rule, bounds check, guard-page reasoning — exists **once** for both
  platforms. `Fiber::Init` sits next to its platform's assembly because the two are one contract.
- **`Ideal` on Linux binds to the whole LLC domain, not one core.** Linux has no equivalent of
  `SetThreadIdealProcessor`, but `sched_setaffinity` takes a **mask**, so the same intent — keep
  locality true with minimum rigidity — is expressed at domain granularity. This is what keeps
  `clusterMates` honest: the mask and the mate list derive from the same cache group, so the
  topology map is true by construction. Unmeasured on real hardware.
- **CMake build**, valid both standalone and as a subdirectory of the JLib umbrella, with
  `find_package(JLibScheduler)` support. A classic `Scheduler.sln` ships alongside for Visual
  Studio versions that cannot open the newer `.slnx` format.

### [CRITICAL] Data race in `StartPool` — worker startup vs. the `workers` vector
`StartPool` created a worker, pushed it into `workers`, and **started its thread in the same loop
iteration**. The worker's own startup path reads `scheduler->workers.size()`, so worker 0 was
reading the vector while the main thread was still `push_back`ing workers 1…N−1 — a concurrent read
against a write that can **reallocate**, letting the reader walk a buffer being freed underneath it.

Fixed by populating the vector fully and starting the threads in a second pass. `reserve()` alone
would *not* have been sufficient: a concurrent read of `size()` against a concurrent write is a race
even when no reallocation occurs. Nothing required a running worker before the vector was complete,
so the split costs nothing.

**This is shared code — the bug was equally present on Windows**, where it survived on timing luck
(the reallocation window is narrow and x86's memory model is forgiving). It is exactly the class
that becomes intermittent corruption under weaker ordering, so **forks on any platform should pull
it**, and it is the single strongest argument for auditing before any ARM work.

Found by ThreadSanitizer via the new `bench/tsan_probe.cpp` — a small harness that exercises each
lock-free structure a few hundred times rather than the benchmark's millions, since a race is
reported on first observation and volume only buys instrumentation cost. Post-fix the probe reports
**zero races**. Its header documents two TSAN blind spots to expect: `atomic_thread_fence` (which
TSAN cannot model, so `TaskDeque` reports are suspect) and unannotated fiber switches.

### [CRITICAL] Four latent defects, all found by porting
Every one of these compiled on MSVC and is non-conforming C++ — the category that breaks on a
compiler *upgrade*, not only on a new platform. **Forks should pull these regardless of platform.**

- **`Task` / `LambdaTask` declared `operator delete` as `= delete` while having a virtual
  destructor.** Ill-formed: the vtable's *deleting* destructor requires an accessible
  `operator delete` whether or not any code calls it. Now defined with an assert, so the
  "slab-allocated, never heap-deleted" guarantee survives with runtime rather than compile-time
  enforcement.
- **`LockFreeList::slabDeleter` — a `static` function — referenced the instance member
  `allocator`.** It is used as an `EpochManager::RetirePtr` callback, so it must be static and
  genuinely could not reach it. It compiled only because nothing instantiated `remove()` on a
  slab-backed list; **the first caller would have broken the build.** Fixed by carrying a
  `TaskAllocator* owner` on `LNodeBase`.
- **`LockFreeList.h` included MSVC-only `<intrin.h>`** and used nothing from it.
- **`Thread.cpp` used `std::memset` without `<cstring>`**; `bench.cpp` used `_stricmp`.

### [CRITICAL] LLC-aware work stealing never actually ran on Windows
`GetGroupMasksForRelation` read `info->Processor` for *every* relation, but
`SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX` is a **union** — `RelationCache` must be read as
`info->Cache`, which has a completely different layout. Reading `.Processor.GroupCount` off a cache
record landed in reserved bytes, read 0, and the loop never executed. Every cache query returned an
empty mask list **while still reporting success**, so `clusterMates` stayed empty and the
locality-first steal phase silently fell through to random on every Windows machine.

Found by porting: the new Linux sysfs implementation reported 17 cache instances on the same box
where Windows reported 0. Two independent implementations of one query disagreeing is what made it
visible after it had gone unnoticed for as long as the feature existed.

**Measured impact on a single-LLC Intel: none** (4.73 → 4.81 µs latency, 23.04 → 22.75 µs/graph —
noise). That machine's last-level cache spans all 32 logical CPUs, so "cluster mates" means
everyone and locality-first picks what random would. **It matters on multi-L3 hardware** — Ryzen
with 2+ CCDs, Threadripper, EPYC, multi-socket — where `clusterMates` becomes a real subset. Do not
read the flat numbers as "the fix was pointless"; read them as "that box cannot show it."

## 2026-08-05
- **[BEHAVIOUR CHANGE] Worker affinity default is now `Ideal`, not hard pinning.** New
  `TaskScheduler::SetAffinityPolicy(AffinityPolicy)` — `Hard` (`SetThreadAffinityMask`),
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
  DAG — a chain of small nodes with a `WaitFor` between each, i.e. the shape of an actual frame — is
  dominated by that and loses nearly 2×. This contradicts the usual "engines pin their worker pool"
  advice, which is why it's recorded with numbers rather than asserted.

  `PhysicalOnly` exists because the previous scheme pinned workers to logical CPUs 1…N, deliberately
  doubling up on physical cores. It confirmed SMT contention is real — 15 physically-pinned workers
  roughly match 31 logically-pinned ones — but did **not** close the gap to unpinned, so the dominant
  cost is binding itself rather than the sibling mapping.

  **Caveat, deliberately not buried:** one idle hybrid machine, synthetic benchmark. Re-measure under
  load and on a non-hybrid CPU before treating it as universal. `Hard` remains available and is still
  the right choice for an application that genuinely owns the machine.
- **`SetParallelForThresholdUs` / `GetParallelForThresholdUs`**: the ParallelFor work threshold is now
  settable at runtime (defaults 75 µs optimized / 750 µs unoptimized). Set it enormous to force every
  `ParallelFor` serial — the fastest way to answer "is ParallelFor causing this?" without a rebuild.
- The threshold now keys on `NDEBUG || JLIB_DEVELOPMENT` rather than `NDEBUG` alone, so an optimized
  build that deliberately keeps assertions live (RelWithDebInfo-style) doesn't get the unoptimized
  value.

## 2026-08-04
- **Renamed the build artifact `Threads` → `Scheduler`.** `Threads.lib` in `C:\libs\Threads` was the
  last leftover of the original "T_Threads" name and disagreed with both the repo (`jlib-scheduler`)
  and the class (`JLib::TaskScheduler`) — three names for one library. Now: project/solution are
  `Scheduler.vcxproj`/`Scheduler.sln`, output is `Scheduler.lib`, canonical install is
  `C:\libs\Scheduler`. The namespace and class names are **unchanged** (`JLib::TaskScheduler`); this
  is purely the artifact.
  **[BREAKING for downstream]** `Threads.lib` and `C:\libs\Threads` are **gone**, not deprecated. A
  compatibility shim emitting both names existed briefly during the migration and was removed the
  same day once every consumer here was moved and verified — carrying a dead path just to avoid a
  one-line edit isn't worth the confusion of two names meaning the same thing, and pre-1.0 is exactly
  when a clean break is cheapest. **Forks: change your linker input to `Scheduler.lib` and your
  include path to `C:\libs\Scheduler\include`.** That is the whole migration; nothing in the API
  moved.
- **`bench/` is now in the repo** (`bench.cpp` + `build_bench.bat`). It was previously local-only,
  which made the measurements below unreproducible by anyone else.
- **`ParallelFor` now decides serial-vs-parallel by MEASURING, not by element count.** The old gate
  was `totalItems > 10000` — a constant set before fork-join existed and never re-measured. A sweep
  of per-element cost (1/8/64/512 flops) against N (256…200,000) found the crossover *element count*
  moves **400×** with body cost (200,000 for a trivial body, ~400 for an expensive one) while the
  crossover *work* stays pinned at **70–92 µs**. Element count was never the right unit: what races
  dispatch overhead is count × cost-per-element. The fixed gate was therefore wrong in **both**
  directions — a trivial body at N=10,001 was parallelized and ran **8–11× slower**, while a heavy
  body at N=4,000 was forced serial when parallel was **12.6× faster**.
  `ParallelFor` now runs a small prefix inline, times it, extrapolates, and parallelizes the
  remainder only if the estimate clears `kParallelWorthwhileUs = 75.0`. The probe is not overhead —
  it is loop work that had to happen anyway, done before the split instead of inside a chunk.
  Measured after: a trivial body went from 0.01–0.12× to a flat ~1.00× across the range (correctly
  choosing serial), while a heavy body keeps 5–19× from N=512 up.
  API-neutral — the signature is unchanged and no caller needs editing, but **callers with cheap
  bodies over 10k elements were silently losing several times over and will speed up on relink.**
- **`chunkSize` is now floored** so a range can't be split into more than ~4 chunks per worker.
  Past that, extra pieces buy no more load balancing and cost a task each: 256 elements at
  `chunkSize=2` built 128 tasks and measured 0.49× (2× *slower*) on tree overhead alone.
- **Crossover sweep added to the bench** (`bench/bench.cpp` → `BenchParallelForCrossover`), so the
  next time the dispatch path changes this constant can be re-derived instead of re-guessed.
- **C++17 compatibility is now enforced by the build**, not merely asserted: `deploy_lib.bat`
  compiles with `/std:c++17` (was `/std:c++20`). The scheduler never needed C++20 — it started
  compiling as such only because a newer Visual Studio defaults that way. Verified clean.

## 2026-08-02
- **Stealing policy made P/E-core aware.** A thief now prefers victims of its own core class, and a
  task with an explicit P/E preference is only stolen by a matching-class thief — so placement
  survives work-stealing instead of being undone by the first steal.
- **Pool sizing policy documented and settled**: reserve one core per *shipped, persistent, busy*
  thread — `hw-2` when the audio device thread is present, `hw-1` otherwise. The rule is about
  measured busy time, not thread existence; a thread that exists but sleeps costs nothing to
  schedule around. Transient oversubscription is accepted deliberately and is not a bug to chase:
  driver and OS threads the process does not own will always exist, profilers count spin-waiting
  workers as running, and neither is actionable from inside a scheduler.

## 2026-08-01
- **`Task::corePref` — P-core / E-core placement.** `CorePref::{Default, P, E, Wide}`, living in what
  was tail padding on `Task`, so it costs no extra bytes. **Priority and placement are deliberately
  orthogonal**: priority controls queue *order* only, placement is decided solely by `corePref`.
  Enforced at push placement (`PickNextWorker`) and at steal time; an explicit core affinity
  overrides it, being the stronger and more explicit request. Class-based routing is **opt-in and
  dormant** — everything is `Default` until a profiled caller asks otherwise, so behaviour is
  unchanged for existing code.

## 2026-07-23
- **`ParallelFor` dispatch is now hybrid flat/fork-join.** The flat path has the caller spawn every
  chunk serially — fine, and ~14% faster, when there are few tasks — but its O(#tasks) serial
  `CreateTask`+`Push`+`NotifyWorker` on one thread collapses at fine grain (~8× slower at ~15k
  tasks, with each notify also taking the worker mutex from the lost-wakeup fix). Fork-join spreads
  task *creation* across the pool and wins decisively past a few dozen tasks. Crossover: ~2 tasks
  per worker.
- **`TaskAllocator` optimized.**

## 2026-07-22
- **Batch stealing removed** (`stealbatch`, plus a follow-up sweep for vestigial remnants) — stealing
  is single-item now. Downstream note: this is what makes the promotion removal below correct.
- **Age-based promotion REMOVED** — this supersedes the mechanism described in the 2026-07-15 entry
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
  no fault — silent cross-fiber corruption. It now raises an access violation at the
  faulting instruction. Costs 4KB of usable stack per fiber (standard 64KB → 60KB usable,
  heavy 512KB → 508KB) and one `VirtualProtect` per fiber at pool init; zero per-switch
  cost. No recovery/stack-growth — a guard hit is a deliberate hard fault. Porters note
  (Linux): equivalent is `mprotect(PROT_NONE)` on the lowest page.

## 2026-07-16
- **TaskDAG runtime is now genuinely zero-allocation**: the per-fire heap-allocated
  `TaskFinishedContext` (one `new`/`delete` per node per submission — the DAG's only heap
  traffic) is gone. The saved fn/data/owner now live embedded in `TaskNode` itself, which
  always outlives the completion trampoline (EBR-deferred retire). API-neutral; downstream
  code that referenced `TaskDAG::TaskFinishedContext` directly (it was private) is unaffected.
- **[CRITICAL] Fiber epoch ABA guard** (`581c25e`): `GlobalFiberPool::ReturnBatch` now
  clears each fiber's `localEpoch` to `SIZE_MAX` before re-enqueueing it. Without this, a
  fiber recycled while its EBR slot still held a stale epoch (e.g. an `EpochGuard` skipped
  by an exception or early exit) could pin `MinActiveEpoch()` or ambiguously alias a new
  epoch entry — corrupting epoch-based reclamation decisions (use-after-free class).

## 2026-07-15
- **Starvation prevention** (`8555cbd`): three complementary mechanisms —
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
  calls `t->~Task()` through the base pointer) — do not remove the vptr to save 8 bytes.
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
  gameplay and died with "allocator exhausted" — looked like a clean exit(0) from the
  outside. Any long-running process on an older revision will eventually hit this.

## 2026-07-01
- TaskDAG: main-thread-affinity nodes (`CreateMainNode`) for renderer integration
  (`bd68ec2`); whoever awaits a graph containing one MUST use `WaitForMain`.
- `DirectEvent` replaces the old Event (`67bdd87`).
- Main thread is a pure waiter in `WaitFor`, steals the first chunk in `ParallelFor`
  (`413922f`).

## 2026-06-30
- Inbox (MPSC handoff) drains before forked tasks (`deceabe`) — ordering fix for
  cross-thread pushes.

## 2026-06-28
- **[CRITICAL] ThreadLocalCache fiber duplication** (`8369d3a`): a fiber could be handed
  to two workers simultaneously (the "ParallelFor heisenbug" — intermittent corruption
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
