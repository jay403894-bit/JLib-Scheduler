# Hazard pointers — design note

**Status: PROPOSAL. Nothing is built.** Written to be shot at.

## The hole

`SchedulerMutex`, `SchedulerSemaphore` and the condition variable all **suspend the fiber** on
contention. That is the point of them — a suspended fiber releases its worker, where a `std::mutex`
would park the OS thread and cost a core.

`Epochs.h` carries a hard invariant with a debug tripwire: **a fiber may not suspend inside an
`EpochGuard`.**

Together those leave a gap with nothing in it:

> A data structure guarded by a fiber-aware lock has **no safe memory-reclamation scheme today.**
> Its critical section can suspend, so it cannot use EBR. The alternatives are `std::mutex` — which
> stalls a worker, the exact thing this library exists to avoid — or no reclamation at all.

Nothing in-tree puts a concurrent node-recycling structure behind `SchedulerMutex` yet, which is the
only reason this has not bitten. The first client that does hits it immediately.

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

## Coroutines

Same rule, worse list. Either the cells live on the **promise** and are unregistered in
`final_suspend`/destroy, or `Protect()` across a `co_await` is disallowed without that unregister.
The epoch invariant is untouched: **still no parking inside an epoch.** HP across an await is legal
only while the frame still owns the cells.

## Implementation order

1. **Fiber-owned cells + a scan set that includes parked fibers.** Both bugs, first, before
   `Protect()` is written.
2. Native-task cells on the worker.
3. Coro registry.
4. Retire batching (`R = k × live_cells`), deleter-based.

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
