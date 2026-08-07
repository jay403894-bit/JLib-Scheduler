# Changelog

Correctness fixes are marked **[CRITICAL]** with a note on what breaks without them —
downstream users (forks/ports) should treat those as must-pull.

## 2026-08-07

### Linux support — the scheduler now builds and runs on Linux x86-64
Every benchmark passes on Ubuntu 24.04 / GCC 13, including recursive fork-join, i.e. fibers
suspending and resuming through the hand-written context switch under the real scheduler. Windows
is unchanged and unaffected.

- **Hand-written System V AMD64 context switch** (`src/posix/ContextSwitch.s`, GAS, Intel syntax).
  **`ucontext` was rejected on a MEASUREMENT, not on its POSIX deprecation:** `swapcontext` saves
  and restores the signal mask, which is a `sigprocmask` **syscall on every switch** — measured at
  **120.3 ns vs 8.0 ns for this implementation, 15× slower**. Boost.Context was declined to keep
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
