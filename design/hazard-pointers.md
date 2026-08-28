# Hazard pointers — design note

**Status: ALL PHASES SHIPPED.** include/Hazard.h, src/Hazard.cpp, tests/hazard_test.cpp (5/5),
tests/coro_hazard_test.cpp (6/6).
Phase 3 diverged from its plan in two places -- see "Phase 3 as BUILT" at the end for what changed
and why.

## When this is the right tool

**The original framing here was wrong, and it is corrected rather than quietly softened.** This note
used to open with "the hole": `SchedulerMutex` and friends suspend the fiber, `Epochs.h` forbids
suspending inside an `EpochGuard`, therefore a structure behind a fiber-aware lock has *no* safe
reclamation scheme. That argument does not hold up.

If you hold the lock, mutual exclusion already establishes lifetime — nothing can free a node you
can reach. And if the traversal is lock-free, you should not be suspending inside it anyway; the
standard pattern is protect the lookup, take a refcount, drop the guard, *then* await. So the case
this was sold on is much thinner than claimed.

The honest version is three cases:

| workload | what it needs |
|---|---|
| **locked structure** | lock / ownership can often establish lifetime on its own |
| **lock-free structure, raw pointers** | a reclamation scheme *is* required -- EBR, hazard pointers or refcounting |
| **epoch-compatible workload** | epochs may simply be cheaper |

`std::shared_ptr` **is already such a scheme, not a reason to reach for this one.** If the nodes are
refcounted, lifetime is handled; what is left is a cost question — an atomic per copy against an
announce per traversal — not a safety one. Refcounting is also half of the coroutine pattern this
file enforces: hazard-protect the lookup, take a refcount, drop the guard, then await.

**And where epochs and hazard pointers overlap, the axis is pinning, not suspension.** An epoch
reader pins *everything* retired since it announced; a hazard reader pins *only the nodes it named*.
A reader does not have to suspend to be slow — a descheduled worker, a preempted thread, a long
traversal or a parked fiber all do it. That is why hazard pointers exist in the literature and it is
the honest reason they exist here.

Epochs remain this engine's reclamation and stay on the steal path: nothing there stalls for long,
and the cheaper announce (~0.40 ns, against a store plus a fence per protected pointer) wins. Reach
for hazard pointers when a reader can be slow **and** the retire rate is high enough that pinning
everything since the announcement would matter.

Nothing in-tree needs either from this file — no concurrent node-recycling structure sits behind a
`SchedulerMutex`. That is a statement about the library, not about whether the facility is useful:
the users are applications, and this exists for the one that writes such a structure.

## The partition

The boundary is **can the reader suspend**, not "lock-free vs locked".

| | Epochs (have) | Hazard pointers (proposed) |
|---|---|---|
| for | lock-free structures | structures behind fiber-aware locks |
| reader suspends? | **never** — tripwire enforces | **yes, by design** |
| cost | one announce per traversal | store + fence per protected pointer, validate-reload per hop |
| a stalled reader pins | **everything** retired since it announced | only the nodes it named |
| retire | epoch advance | scan of published cells, batched |

Epochs are unchanged and the invariant does not move. **Epochs stay on the steal path** — that is
what the cost column buys.

---

## Where a textbook port dies

Michael's HP is `slots[tid][i]`: publish, use, clear. Two independent bugs here, not one.

**Bug 1 — slot reuse on the publishing worker.**

1. Fiber A on worker W0 publishes `slots[W0][0] = node`.
2. A parks on `SchedulerMutex`.
3. W0 runs fiber B, which writes `slots[W0][0] = other` (or clears it).
4. Another core retires `node`; the scan sees W0's cell ≠ node → frees it.
5. A resumes on W1 holding a dangling pointer.

**Bug 2 — the scan set does not include parked fibers.**

Same outcome with *no* overwrite at all: A published on W0, parked, and resumes on W1. W1's cells
were never the publish site, and a `clear()` on W1 does not undo W0. A scan that walks only
*running* workers never sees A, because A is parked. **Free under a sleeper.**

Bug 1 is about *where* cells live. Bug 2 is about *what retire walks*. Fixing one does not fix the
other.

## Ownership rule

**The reader is the fiber / coro frame / native task — never the worker.**

| execution | where the cells live |
|---|---|
| `TaskType::Native` | worker/thread is **fine** — a native task does not change stack mid-section |
| `TaskType::Fiber` | the `Fiber` (or the `Task` owning it) |
| `TaskType::Coroutine` | the frame / promise, which is what survives the `co_await` |

**This is already precedent, not a new idea.** `Fiber` carries its EBR slot for exactly this reason:

```cpp
// EBR participation slot. SIZE_MAX == "not in an epoch". The fiber is the unit
// that migrates across workers, so the slot lives here (not on the thread).
std::atomic<size_t> localEpoch{ SIZE_MAX };
```

Hazard cells go beside it, under the same argument.

`Protect()` writes **the fiber's** cell, `std::atomic_thread_fence`, then **reloads and compares** —
publishing a pointer read a moment ago proves nothing if it was retired in between. `Unprotect()`
clears **that same cell**, even when the resuming worker is not the publishing worker.

## Scan set

`Retire()` must walk **all three**:

1. every running worker's **native** cells;
2. every allocated **fiber** that may be parked;
3. every live **coro frame** that published.

(2) is already enumerable and costs nothing to build: `Fiber::poolIndex` is a *dense, stable* index
into `GlobalFiberPool`'s `standardFibers` / `heavyFibers` vectors, which are `reserve()`d and never
reallocate, so the index is fixed for the life of the program. It is the same property that makes
Event's waiter index a perfect hash. **A parked fiber is still in that vector, so walking it includes
sleepers by construction** — which is precisely what Bug 2 requires.

(3) is the expensive one. Coro frames are **not** a bounded pool, so this list must not live on the
fiber pool.

## Do NOT

- **Pin the fiber to its publishing worker for the hazard lifetime.** That is "fix TLS by killing
  steal."
- **Keep cells on the worker and copy them to the fiber on suspend.** You will miss a path — mutex,
  semaphore, event, fence — and the miss is silent.
- **Share one cell array per worker across its fiber cache.** That is Bug 1 with extra steps.

## Cost

Scan width is **cells × live readers**, not cells × workers: a 64-per-worker fiber budget × N
workers × 2–4 hazards. That number is the reason epochs stay on the steal path and this does not.

## Fiber reuse

When a fiber returns to the cache its cells must be **nulled and a generation bumped**, or a stale
scan treats a recycled block as still protecting `node`.

Precedent again: `GlobalFiberPool.cpp` already clears `localEpoch` to `SIZE_MAX` on return —
`"Defensive: clear epoch state"` — after an ABA bug where a recycled fiber carried a stale epoch.
Hazard cells need the same discipline plus the generation, since a cell holds a *pointer* rather
than a monotonic number and cannot be sanity-checked by value.

## Coroutines — SUPPORTED (this section was the plan; see "Phase 3 as BUILT")

**A coroutine may hold a `HazardGuard` across a `co_await`.** That is the point of the record: it is
owned by the **guard**, not by the worker and not by the frame, so it survives the suspend and
travels with the logical reader to whichever worker resumes it.

The original plan below — cells on the **promise**, unregistered in `final_suspend` — was written
before `ResumeCoroutine` was understood to be generic, and did not survive contact with it. What
shipped instead is a registry of records the guard acquires on first `Protect`. The distinction
matters: frame-owned cells die with the frame, and a *deferred* grace period cannot protect memory
that is already freed.

**The epoch invariant is untouched: still no parking inside an `EpochGuard`.** That asymmetry is the
whole reason both schemes exist — a coroutine has no epoch slot of its own (it borrows the
worker's), so suspending inside an epoch guard is a contract violation with a tripwire on it, while
suspending inside a hazard guard is ordinary use.

## Implementation order

1. **Fiber-owned cells + a scan set that includes parked fibers.** Both bugs, first, before
   `Protect()` is written. — **DONE**
2. Native-task cells on the worker. — **DONE**
3. Coro registry. — **DONE**, though not as planned (see "Phase 3 as BUILT")
4. Retire batching (`R = k × live_cells`), deleter-based. — **DONE**

Two things fell out of building 1–2 that were not in this plan and are worth knowing before 3:

- **The retire bag must be flushed when a worker goes to sleep.** The bag is per-thread on purpose —
  protection follows the reader, but the deferred free list must never sleep — and the corollary is
  that an idle worker would otherwise sit on retired nodes until its own next `Retire()`, which may
  be never.
- **The table must be built in `TaskScheduler::Init`, not lazily.** Lazy racing `Init` is real: a
  worker reaching its sleep path flushes the bag, which builds the table, while the pool is still
  coming up — baking in zero fibers and putting every later `poolIndex` out of range.

**The first test must force migration and a park**, not just concurrency. A thread-local HP passes a
unit test in which nothing migrates — it will go green in CI and smash in the mutex test. This
codebase has shipped six tests that passed with the mechanism removed; a hazard-pointer test that
never parks a publisher is number seven.

Negative control for it: with cells worker-owned instead of fiber-owned, the test must **fail**.

## Constraints (owner's, load-bearing)

**Allocator-agnostic. NOT the slab.** The slab is *scheduler-owned* memory — it exists because the
scheduler allocates its own tasks and frames. A client structure behind `SchedulerMutex` lives in
client memory: the heap, or the app's per-frame arena cleared at end of frame. `Retire` takes a
**deleter**, default `delete`. The job system must never dictate an app's allocation strategy.

**General.** No target structure and no bound on readers, so no fixed per-reader cell budget —
dynamically acquired records, never freed, released back for reuse.

## Open questions

- **Cancellation.** A fiber suspended holding cells can be cancelled. RAII covers the unwind path;
  the discard-at-pickup path needs checking, having drifted three times before.
- **Long parks.** Retire latency is bounded by the longest *park*, not the longest traversal. Correct
  and the whole advantage over EBR — it pins those nodes, not everything — but document it.
- **Registry growth.** Records are never freed. Bounded by the fiber pool for fibers; a program
  churning short-lived non-fiber threads grows it monotonically. Cap plus a diagnostic.

## What this is not

- Not a replacement for epochs. Lock-free structures keep EBR; it is cheaper and its no-suspend
  invariant is enforceable.
- Not a smart pointer. It protects a traversal, not ownership.
- Not slab-backed.

---

# Phase 3 — coroutines (the PLAN; see "Phase 3 as BUILT" below for what shipped)

Layering first: **no promise types in the C++17 core.** The core exposes a hook; the C++20 header
owns everything coroutine-shaped.

## Core API

```
registerExternalCells(std::atomic<void*>* cells, uint32_t n) -> RecordId
unregisterExternalCells(RecordId)
```

`Scan()` walks the fiber table, then the registry. A record is `{ cells, n, gen, state }`.

**Records are NEVER deleted.** `FREE -> LIVE -> RETIRED -> FREE` with a bumped generation. That is
Michael's answer to the registry's own reclamation problem: you cannot protect the hazard registry
with the hazard domain it serves.

## Coro side

The promise owns `std::atomic<void*> cells[kCellsPerReader]` and holds the `RecordId`.

**Register only frames that actually `Protect()`** -- not every frame. The registry then sizes to
PEAK CONCURRENT HP-USING coroutines, not to total coroutines spawned. Registering in every promise
ctor makes the registry a peak-load leak of records for cells nobody ever published into.

## The lifetime that will bite

`final_suspend` does **not** run on every path that already exists here:

- discarded before start
- cancelled in the queue (the non-lazy waiter drop)
- an exception before the first suspend
- `destroy()` from the scheduler, bypassing the promise's normal done path

These are the same discard sites that have drifted three times in this codebase.

So: **unregister must be valid if EITHER `final_suspend` or the destructor runs, and safe if BOTH
do.** `RecordId` + generation + a single `compare_exchange` `LIVE -> RETIRED` makes it idempotent.

**Put it in the frame destructor, not only in a scheduler hook** -- a discarded frame can be
destroyed on a worker that never ran the coroutine, and the fifth discard path is the one nobody
remembers to call the hook from.

## Ordering against Scan()

- **Register must COMPLETE** -- record visible, cells zeroed -- **before** the first store that
  publishes a pointer into those cells.
- **Unregister must COMPLETE after** the last unprotect, and after the frame can never again load
  that pointer.

Invert either edge and this is the fiber bug again: a scan misses a live block, or a scan treats a
recycled block as protecting the wrong node.

## Record reuse needs a GRACE PERIOD, not just a generation

**A RETIRED record must stay out of FREE until a scan that STARTED AFTER its unregistration has
FINISHED.** Hand the same `cells*` straight back and a scanner still walking that block sees a new
coroutine's pointer -- or a `nullptr` -- and frees under the old frame. **A generation on the record
is not sufficient**, because the scanner is reading the CELLS, not the record header.

Mechanism: a global scan counter bumped at scan entry and exit. A record retired at counter `c`
becomes FREE only once a scan that began at `> c` has completed. That is a grace period, i.e. the
reclamation problem one level up, solved with a counter rather than with hazard pointers.

## Do NOT

- Give frames a fake `poolIndex` and punch them into the fiber table.
- Allocate a registry node per `Protect`/`Unprotect`.
- Free registry nodes with the same HP domain they serve.

## The minimum test IS the spec

> `Protect` on a coroutine, `co_await` the mutex, **cancel/destroy that frame from another worker**,
> retire the node from a **third**.

Unregister late -> smash immediately. Unregister early with the record reused -> smash later. Both
failure directions are covered by one scenario, which is why this is the acceptance test and not an
addition to it.

**Summary: the layering is sound; the work is making discard and `final_suspend` the SAME
unregister, once, with a generation, on every path already known to be forgettable.**

## Phase 3 as BUILT — where it diverged from the plan, and why

**Shipped.** `include/Hazard.h`, `src/Hazard.cpp`, `tests/coro_hazard_test.cpp` (6/6).

**Divergence 1 — the guard owns the record, not the promise.** The plan put a `RecordId` in the
promise and had the resume trampoline hand the core a pointer to that field. That was UNSOUND:
`ResumeCoroutine` is generic BY DESIGN, because `Task::data` holds whichever frame was last armed --
for a nested `Lazy` that is the parent, not the `Coro` -- so typing the handle to reach
`promise().hazardRecord` reads a different promise's storage. Built it, it hung, root-caused it.

A `HazardGuard` is a LOCAL IN THE FRAME, so it already survives the `co_await`, and unwinding runs
its destructor. Owning the record there gets "released on every path" from the language instead of
from a list of call sites -- the same class of bug as the missing `PushBatch` stamp, closed by
construction rather than by vigilance. No promise field, no trampoline change, no type pun.

**Divergence 2 — cells are domain-owned.** Frame-owned cells die with the frame, and a deferred
grace cannot protect memory that is already freed; honouring it would need a synchronous drain
inside the destructor. Domain-owned removes the hazard and costs 4 bytes instead of 32.

**The grace period waits on SCANS, not on readers.** A scan runs to completion and cannot suspend,
so waiting for one is bounded. Waiting for readers to leave would be the epoch-pin failure again.

## "Destroy the frame from another worker" is UNREACHABLE, deliberately

The acceptance spec's step 3 has no code path here, and the refusal is explicit:

```
DiscardIfCancelled:  if (!task || task->started || !IsTaskCancelled(task)) return false;
```

A STARTED task is never discarded -- "discarding one of those abandons a live stack or frame rather
than cancelling it". `Spawn()` takes the handle via `Release()`, so no caller retains one either.

**The reachable analogue, and what is tested:** suspend inside a protected section, cancel, the
scheduler re-pushes, the frame resumes, polls, unwinds, and `~HazardGuard` unregisters. A frame
discarded before it ever starts never took a guard and holds no record.

Writing a foreign-`destroy()` test would be a regression test against a policy this runtime does not
have. **The contract has no path to violate, which is stronger than a passing test for it.**

## Known limitation: shutdown leak (not a smash)

A coroutine parked holding a guard that is NEVER re-pushed -- teardown that stops resuming before
every started frame has unwound -- leaves its record LIVE forever. Nothing is freed under anyone;
reclamation of that one node stops and one registry slot is lost. Bounded by `kMaxRecords`.

If an abort-a-waiter API is ever added (destroy WITHOUT resume), `~HazardGuard` on `handle.destroy()`
becomes load-bearing and the note above stops being true.
