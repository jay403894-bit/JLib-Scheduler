# Changelog

Correctness fixes are marked **[CRITICAL]** with a note on what breaks without them —
downstream users (forks/ports) should treat those as must-pull.

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
