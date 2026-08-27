# JLib::Scheduler — engineering notes

A running log of decisions and the reasoning behind them. **This is the place for the "worth
flagging" details** — the ones that are obvious while the code is being written and invisible six
months later. Newest at the top. Each entry says what was decided, what it rules out, and what would
make it wrong.

---

## 2026-08-27 — APPROVED PATTERN: pick the reclamation scheme from the task type

**In a lock-free section that coroutines can reach, implement BOTH guards and branch on the task
type.** Not one or the other.

```cpp
if (TaskScheduler::CurrentTaskType() == TaskType::Coroutine) {
    HazardGuard g;                                  // survives a suspend
    Node* n = g.Protect(0, head);
    ...
} else {
    EpochGuard g;                                   // cheaper, and cannot suspend here
    Node* n = head.load(std::memory_order_acquire);
    ...
}
```

`TaskScheduler::CurrentTaskType()` is static and reads the thread-local directly -- no `Instance()`,
because this sits on the hot path. It reads `currentRunningTask`, not `currentFiber`, since a
coroutine has no fiber and that is precisely the case being distinguished. No task at all (a bare
thread) reports `Native`, which is the right answer rather than a fallback: a bare thread does not
change stack mid-section either.

### Why both arms rather than just hazards everywhere

Epochs are cheaper for the contexts that can use them -- one announce per traversal against a store
plus a fence plus a validate-reload PER HOP. A fiber or native task pays nothing for the coroutine
case if the branch keeps them out of it.

### Why both arms rather than just epochs

A coroutine cannot hold an `EpochGuard` across a `co_await` -- the tripwire in `Epochs.h` says so --
and the counted-epoch machinery that makes coroutine EBR safe at all is **1.41x the cost of the slot
path** (measured; sharding the counter took it from 61.7x to 1.41x, and that sharding is why it is
usable). SRCU-shaped safety is not free.

### The deciding term is the PARK, not the per-guard cost

Do not reason about this from guard throughput. On that axis epochs win, and win by more per hop:

| | per guard / protect |
|---|---|
| slot epoch | 2.52 B/sec |
| counted epoch (coroutine) | 1.82 B/sec — the 1.41x, and ~0.55 ns |
| hazard `Protect` | a store + a **seq_cst fence** + a reload, PER POINTER — tens of ns |

**And it does not matter, because a parked reader is what the choice is actually about.**

Counted epochs let a coroutine hold protection across a suspend — 312 parked readers, 0 lost — but
the advance gate then **refuses to advance the ring while anyone is parked in it**. One parked
coroutine stalls reclamation FOR EVERYONE: nothing anywhere is freed until it leaves. A parked
reader holding hazards pins only the nodes it named.

That is not a 1.41x difference. It is bounded against unbounded.

**A COROUTINE CANNOT HAVE A SLOT, and that is why counted epochs exist -- not the suspend.** Slots
are indexed by a stable identity: a fiber has `poolIndex`, a coroutine frame has nothing, because
frames are not a bounded pool. Borrowing the worker's slot is not having one -- it is the unsafe
thing, since the worker's next guard overwrites the announcement. A fixed slot pool was built and
reverted: any bound reintroduces the exact ceiling coroutines exist to escape, and a reactor parks
thousands.

So counted epochs are THE epoch mechanism for coroutines, not a fallback for a narrow case. The
choice below is between two VALID schemes for a coroutine, and it is decided by the park.

An earlier version of this note hedged that "a single protect probably favours the counted epoch".
That compared guard costs and ignored what a park does to everyone else's reclamation — which is the
entire reason hazard pointers exist here.

### What this rules out

- **"Just use hazards everywhere"** -- taxes fibers and native tasks for a case they never hit.
- **"Just use epochs everywhere"** -- illegal the moment a coroutine suspends inside the guard.
- **Leaving the branch out and documenting a rule** -- the tripwire only fires in Debug, and the
  failure it catches is silent in Release.

---

## 2026-08-27 — Teardown drains parked work instead of abandoning it

### The chain, and why all three pieces were needed

1. **Eager mutex cancellation** — made every primitive ejectable.
2. **The primitive registry** — made them findable.
3. **The drain in `Join()`** — made teardown unwind rather than abandon.

Each was useless alone. The mutex could not be woken, so a drain had nothing to wake it with; the
drain had nothing to iterate, so ejectability did not help.

### Why it mattered: abandonment releases nothing

A frame parked on a primitive nobody signals again **never unwinds**, so nothing it holds is
released — RAII objects, its `WaitGroup` slot, a hazard record, a lock it owns. Hazard records were
just the most *visible* instance because they are a bounded pool with a fatal on exhaustion. The
others failed silently, which is worse.

### `SchedulerMutex` was the only primitive without eager cancel

Event, `SchedulerSemaphore` and `SchedulerConditionVariable` all had `CancelWaiters(token)`. The
mutex had **skip-at-release only**: a cancelled waiter stayed parked until the holder happened to
release. That cost twice —

- **Consistency.** A caller cancelling a scope has no way to know one of its waits is a mutex and
  will not end.
- **Teardown.** A frame parked on a mutex whose holder is itself abandoned cannot be woken by
  *anything*. That is the structural cause of the leak above.

Ejected waiters are resumed **without** the lock — the same rule as everywhere here: a cancelled
acquire took no permit, a cancelled wait holds no lock.

### Three details in the drain that are easy to get wrong

**Ordering: after the service threads stop, BEFORE the workers join.** Nothing new can arrive once
the reactor and timer are stopped, but *unwinding is work* and needs live workers to run it. Drain
after the join and frames get re-pushed onto a pool that is gone.

**Copy the list under the lock, walk it outside.** `DrainForShutdown` resumes frames that can run
immediately, destroy their own primitives, and re-enter `~WaitPrimitive` — which takes the same
mutex. Holding it across the walk self-deadlocks.

**`DrainForShutdown` is a separate virtual from `CancelWaiters`, and tokenless.** A token would
invite using it as a general-purpose cancel, and `Event::CancelWaiters` has no token at all — a
shared token-taking virtual would have had to silently ignore one. Silently ignoring a token is the
kind of mismatch that becomes a bug report about "cancel cancelled the wrong thing".

### Registry shape

Intrusive **doubly**-linked chain plus a head pointer on `TaskScheduler`. Doubly, not singly, so
unlink is O(1) — singly linked makes every primitive destruction an O(n) walk, which a program
creating and destroying locks in a loop feels as O(n²). It answers exactly one question, *which
primitives exist*; the wait lists stay on the primitives that own them.

`WaitPrimitive` lives in its own header because `Event.h` is included **by** `TaskScheduler.h` and
cannot reach back into it.

### LIMITATION — a primitive constructed before `Init` is not registered

There is no scheduler to register with, and a **file-scope `SchedulerMutex` is the common case**. It
works normally; `Join()` simply cannot find it, so anything parked on it is abandoned exactly as
before. Construct primitives after `Init` if you want them drained. Stated here because it is the
kind of thing that otherwise gets discovered as "the drain doesn't work".

---

## 2026-08-27 — Hazard pointers

### The hole they fill

Fiber-aware locks **suspend**. `EpochGuard` forbids suspending. So a structure behind a
`SchedulerMutex` had **no safe reclamation scheme at all** — `std::mutex` (stalls a worker, the thing
this library exists to avoid) or nothing.

The dividing line is **can the reader suspend**, not lock-free vs locked.

### Two independent bugs a textbook port has here

Michael's HP is `slots[tid][i]`, correct when a reader *is* a thread. Here a reader migrates:

1. **Slot reuse.** Fiber A on W0 publishes, parks; W0 runs fiber B which overwrites the cell; a
   retire elsewhere frees the node; A resumes on W1 holding a dangle.
2. **The scan misses parked fibers.** Same outcome with no overwrite: a scan walking *running*
   workers never sees a sleeper. Free under it.

Bug 1 is **where cells live**; bug 2 is **what retire walks**. Fixing one leaves the other. Both are
answered by indexing cells by **reader** — `Fiber::poolIndex`, the worker for Native tasks, a
reserved block for external threads.

### Protection follows the reader; the retire bag does NOT

- **Cells** follow the reader, because a protected pointer survives a park.
- **The retire bag is per-thread**, because *the deferred free list must never sleep*. Put the bag on
  the fiber and a park freezes reclamation exactly the way an epoch pin does — the stall hazard
  pointers exist to escape.

Corollary: **the bag is flushed when a worker goes to sleep.** Otherwise an idle worker sits on
retired nodes until its own next `Retire()`, which may be never.

### Coroutines: the GUARD owns the record

The first design put a `RecordId` in the promise and had the resume trampoline hand the core a
pointer to that field. **Unsound**: `ResumeCoroutine` is generic *by design*, because `Task::data`
holds whichever frame was last armed — for a nested `Lazy` that is the parent, not the `Coro`. Typing
the handle to reach `promise().hazardRecord` reads another promise's storage.

A `HazardGuard` is a **local in the frame**: it survives the `co_await`, and unwinding runs its
destructor. Owning the record there gets "released on every path" **from the language** rather than
from remembering every discard site.

### Cells are domain-owned

Frame-owned cells die with the frame, and a deferred grace cannot protect memory that is already
freed — honouring it would need a synchronous drain inside the destructor. Domain-owned costs the
frame 4 bytes (an id) instead of 32 (four cells).

### The grace period waits on SCANS, never on readers

A scan runs to completion and cannot suspend, so waiting for one is bounded. Waiting for *readers*
to leave would be the epoch-pin failure again. A record retired at scan `c` is reusable only once a
scan begun after `c` has finished — **a generation alone is not enough, because the scanner reads the
CELLS, not the record header.**

### `kCellsPerReader` is a BUDGET, not a derived number

Compile-time knob (`-DJLIBSCHED_HAZARD_CELLS`). What consumes them: one payload pointer (1), a
list walk with prev/curr/next (2–3), a mutex wait plus "I still name this node" (+1), a nested HP
section (+depth). **4 = list walk plus one extra protect, no nesting.** It does *not* cover two
nested walks or a tree descent. If a structure needs six it should say so.

`Protect` is **fatal** on overflow and never slides onto another cell — a silent slide is the
worker-cell bug relocated onto the fiber. Fatal in both configurations, because returning a pointer
that was never published hands the caller a use-after-free with a confident comment above it.

### The table is built in `Init`, not lazily

Lazy racing `Init` is real: a worker reaching its sleep path flushes the retire bag, which builds the
table, while the pool is still coming up — baking in zero fibers and putting every later `poolIndex`
out of range. Caught only because the test printed `readers=` in its banner.

### "Destroy the frame from another worker" is unreachable, deliberately

`DiscardIfCancelled` refuses a started task outright — discarding one abandons a live frame — and
`Spawn` takes the handle via `Release()`, so no caller retains one. The reachable analogue is:
suspend, cancel, **resume, unwind**. A foreign-`destroy()` test would be a regression test against a
policy this runtime does not have.

**If an abort-a-waiter API is ever added** (destroy *without* resume), `~HazardGuard` on
`handle.destroy()` becomes load-bearing and several notes above stop being true.

---

## Cross-cutting rules this session kept re-proving

**An invariant that must hold at N call sites will be added to N−1.** The missing `PushBatch` lane
stamp made `KPolicy::WaitTime` measure nothing on live I/O while its test stayed green. Task
disposal has drifted three times the same way. Prefer designs where the language enforces it — the
hazard guard's destructor over a list of release sites.

**Run the negative control, and run it *before* writing the conclusion.** Every mechanism added here
was verified by breaking it deliberately and watching the test fail: worker-owned cells free under a
sleeping reader; the drain commented out leaves frames parked; a waiter under a different scope is
not ejected.

**A test whose subject never parks proves nothing.** A worker-owned hazard pointer passes every test
in which nothing migrates — green in CI, smashes on the first contended mutex.

**A controller declining to act is not a defect until you show that acting would help.** `KPolicy::
WaitTime` was built on "dynamic K never promotes at 60 Hz", which turned out to be correct refusal:
QueueLoad asks *would another worker help?*, and the answer was no. Removed.

**Measurement conditions are part of the measurement.** Window focus and process QoS move dispatch
latency ~173× on this machine. Benchmarks now print their own conditions, and latency numbers are
only trusted from a run started by hand.
