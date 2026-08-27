# JLib::Scheduler — engineering notes

A running log of decisions and the reasoning behind them. **This is the place for the "worth
flagging" details** — the ones that are obvious while the code is being written and invisible six
months later. Newest at the top. Each entry says what was decided, what it rules out, and what would
make it wrong.

---

## 2026-08-27 — Choosing a reclamation scheme: epochs or hazards

**FIRST, THE CASE WHERE THERE IS NO CHOICE.** A structure behind a fiber-aware LOCK gets hazards,
full stop: the reader SUSPENDS on the acquire, and `EpochGuard` forbids suspending. Epochs are not
slower there, they are ILLEGAL. That is what hazard pointers were built for and the numbers below do
not bear on it at all.

**The rest of this section is about lock-free sections, where both schemes are legal.** There it is a
PERFORMANCE choice, not a safety one: both are safe for all three execution modes, and counted
epochs made coroutines safe under EBR -- the entire point of building them.

### The rule

> **Epochs by default, for every mode.** The coroutine penalty is 0.15 ns per guard.
>
> **Hazards when a reader can PARK inside the protected section** — that is the only thing the
> per-guard numbers do not capture.
>
> **One scheme per structure. Do not mix.**

### The numbers, absolute — and they invert the obvious reading of "1.41x"

| | per guard / protect |
|---|---|
| slot epoch (fiber, native, bare thread) | **0.40 ns** |
| counted epoch (coroutine) | **0.55 ns** — the 1.41x, i.e. **0.15 ns** absolute |
| hazard `Protect` | store + **seq_cst fence** + reload, **PER POINTER** |

A seq_cst fence is an `mfence` or a locked op: order 20-40 cycles, ~10 ns. That is roughly **18x a
whole counted-epoch guard**, and the epoch is paid ONCE PER TRAVERSAL where the hazard is paid PER
POINTER.

**So epochs beat hazards on guard cost for coroutines too, and not narrowly.** 1.41x reads like a
reason to switch; 0.15 ns is not one. (The hazard figure is an ESTIMATE from the instruction cost --
not measured in this tree. The epoch figures are measured.)

### The one thing guard cost does not capture: a parked reader

Counted epochs keep a parked coroutine SAFE -- 312 parked readers, 0 lost protection. But the advance
gate then refuses to advance the ring while anyone is parked in it, so **one parked reader stalls
reclamation for everyone**. A parked hazard reader pins only the nodes it named.

Unbounded against bounded, and no per-guard number reaches it. That is the whole case for hazards,
and it is why the rule keys on PARKING rather than on execution mode.

### Why a coroutine pays the counted path at all

It cannot have a SLOT. Slots need a stable identity -- a fiber has `poolIndex`, a frame has nothing,
and frames are not a bounded pool. A fixed slot pool was built and REVERTED: any bound reintroduces
the exact ceiling coroutines exist to escape, and a reactor parks thousands.

### Why the counted path was accepted despite the cost

**Memory safety, not performance.** Without it a coroutine had no correct epoch story at all, and the
owner took the slower mechanism rather than leave a footgun where a user gets memory corruption. Read
the 1.41x in that light: a price paid knowingly.

### Why not to mix, concretely

Neither scheme can see the other's readers, so a structure with both makes RETIRE unsound:

- a fiber takes an `EpochGuard`, announces, is traversing node N;
- a writer unlinks N and calls `HazardRetire(N)`;
- the scan finds no hazard CELL naming N and frees it under the fiber.

The mirror fails too: a parked coroutine holding a hazard on N does not stall epoch advancement, so
an epoch-based retire frees N underneath it.

Sound mixing needs a **dual-condition retire** -- free only when no cell names the node AND its
retire generation is epoch-safe. One conjunction in the hazard `Scan`, and **NOT BUILT**: one scheme
per structure is simpler and, given the numbers above, costs nothing worth measuring.

### Still not measured

Hazards versus counted epochs on a real coroutine workload with a parked reader. That is the case the
rule turns on, and the guard costs above do not decide it. `bench/epoch_mechanisms.cpp`.

### TaskDAG stays on epochs

No coroutine builds a DAG, so the question does not arise. (A coroutine can *complete* an external
node via `SignalExternalNode`, reaching `ForEachDependent` -- a bare edge walk under a plain
`EpochGuard`, with no `co_await` in it.)


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

### Four details in the drain that are easy to get wrong

**Ordering: after the service threads stop, BEFORE the workers join.** Nothing new can arrive once
the reactor and timer are stopped, but *unwinding is work* and needs live workers to run it. Drain
after the join and frames get re-pushed onto a pool that is gone.

**Pop the head under the lock; drain outside it.** The obvious version — copy the list under the
lock, walk the copy — gets the deadlock right and the lifetime wrong. `DrainForShutdown` resumes
frames that run *immediately*, and a resumed frame can destroy its own primitives, so a copy holds
raw pointers to objects resumption is free to free: drain N unwinding a stack that owns primitive
N+1 leaves a dangling entry that drain N+1 calls a virtual on. Unlinking each primitive *before*
draining it fixes both — the mutex is not held across the resume, and anything a resumed frame
destroys unlinks itself before the loop reaches it.

**Every derived destructor calls `LeaveRegistry()` FIRST.** Unlinking in `~WaitPrimitive` is too
late: by then the derived part is gone and the vtable has reverted, so a racing drain calls into a
half-destroyed object. That is why `DrainForShutdown` is *not* pure — it is an asserting default, so
a class that forgets the call degrades to a skipped drain and a loud test failure instead of a
pure-virtual crash. Four call sites for one invariant; three of them will be the ones someone
forgets.

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

### What a coroutine still must not do — the contract after the record

The record closes *migration*. Two things are left, and neither is about worker cells — worker cells
were never going to save either one, with or without a worker.

**1. Do not `Protect` without a record.** On `kNoRecord` the guard's cells stay null and the first
`Protect` is fatal, naming `kMaxRecords`. The constructor returns early rather than resolving a
reader, so there is no downgrade at all: every available downgrade stops protecting at the first
`co_await`.

**2. Do not stash a raw `T*` and drop the guard.** Protection is the **guard's lifetime**, not the
pointer's. `~HazardGuard` clears the cells; a pointer kept past that is a plain pointer with nothing
announcing it.

**Rule 2 is easier to get wrong in a coroutine than anywhere else, and that is the reason to write
it down.** In a fiber or native task the scope that drops the guard usually ends soon after, so the
mistake is short-lived. A coroutine frame *outlives its suspensions*, so a stashed pointer keeps
looking valid — the frame still holds it, it still compiles, and the read after resume is a
use-after-free with nothing nearby to suggest it.

**Rule 1 is enforced; rule 2 cannot be.** Exhaustion is a state the code can observe, so it aborts
on it. A raw pointer copied out of a protected read is invisible from inside the library — no
signature to check, no tripwire to arm. That asymmetry is why one is an abort and the other is a
paragraph, and why review of any coroutine taking a guard should look for it specifically.

### The refusal has to live in the constructor, not in `CurrentReader()`

The guard used to *say* it refused the worker-cell downgrade on exhaustion and then fall through to
`CurrentReader()`, which refused a second time. Two things wrong with that, one of them live:

- **The refusal lived in another function.** Both sites derived "is this a coroutine" from
  `currentRunningTask->type`, so they agreed — but the guarantee rested on two call sites continuing
  to agree, a bet this codebase has lost three times (discard sites, the `PushBatch` stamp,
  `LeaveRegistry`).
- **`CurrentReader()`'s fiber branch ran first**, returning `fb->poolIndex` before it ever looked at
  the task type.

**And the detector itself could lie**, which no amount of gating fixes: `Complete()` used to free
the Task while the worker loop still held `currentRunningTask`, so through the whole unwind window
that pointer named a recycled slab slot. A destructor constructing a guard would read `Native` for a
coroutine and take worker cells *through the front door*. Fixed by moving `Complete()` to
`final_suspend` — see the entry below.

**Shape, not frequency, is what made this worth fixing.** `kMaxRecords` is rarely hit and the task
type is normally set, so the frequency was low. But the failure mode is: every test that never
`co_await`s inside the guard passes, and the first real suspend is a use-after-free. A fallback
whose only failing case is the case it exists for is the wrong shape regardless of how rare it is.

### `Complete()` belongs in `final_suspend`, not `return_void`

C++ runs `return_void()`, **then** destroys the body's locals, **then** reaches `final_suspend()`.
Completing in `return_void()` announced "done" while the frame's own destructors had yet to run.
Three defects from one ordering:

- `WaitFor(wg)` was not a happens-before for anything the frame owned. Measured at up to a
  millisecond on the cancellation path — not a few instructions.
- Hazard records were held past completion, pressuring `kMaxRecords`, whose exhaustion is a fatal
  abort by design.
- `currentRunningTask` went stale while user code could still run (above).

The old comment objected that "the promise is on borrowed time" in `final_suspend`. Half right:
nothing may touch the promise *after* `final_suspend` returns, and nothing does — but
`final_suspend()` is a member call on a **live** frame, since `suspend_never` destroys the frame
after the call, not before it.

`coro_hazard_test` went 3–4/40 failing to 0/40. **That number alone is weak** — at the pooled rate,
0/40 happens ~2.8% of the time by chance. What carries it is that the mechanism predicted the result
before the run, and an instrumented build had already shown the record *was* released, just late.

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

**A bisect at the wrong sample size is not a bisect.** Five "fixes" and a full revert went into a
`dag_external` segfault that turned out to be pre-existing — at n=40 the *pre-hazard* build crashes
too. Every clean run along the way (0/15, 0/20) was underpowered: at a ~7% rate those happen 31% and
21% of the time by chance. Before a run of green is treated as evidence, work out what a run of
green is worth.

---

## Deferred — AArch64 PAC/BTI

`src/posix/aarch64/ContextSwitch.S` assumes classic AArch64: unsigned return addresses, unguarded
indirect branches. Correct for every target that builds today; the next `#ifdef` if iOS or a
hardened Linux/Android distribution matters. **Not a bug in what ships — a forward-looking gap.**

Three distinct things, because "add PAC support" is not one change:

**BTI landing pads.** `blr x19` is a guarded indirect *call*, so its target needs `bti c` — a
compiler-built entry point emits that in its own prologue, so the call is fine. The label
`FiberTrampoline` is entered by `ret`, and BTI does not guard returns (that is PAC's job), so it
works today. Anything that ever reaches that label with `br`/`blr` needs `bti c` first.

**The property note is the quiet one.** A hand-written `.S` with no
`GNU_PROPERTY_AARCH64_FEATURE_1_AND` note makes the linker mark the **whole binary** as not
BTI/PAC-compatible. Shipping this file into a hardened build does not fault — it silently disables
the protection for every other object too. A downgrade nobody sees is worse than a crash.

**PAC signs against SP.** `paciasp`/`autiasp` use the stack pointer as the modifier, so a return
address signed on one stack cannot be authenticated on another — and a fiber switch changes SP by
definition. This port is safe *only because it never signs*: `ContextSwitch` saves and restores x30
raw and returns with a bare `ret`, and `Fiber::Init` seeds a raw address into the x30 slot. Adding
`pac-ret` here means auditing that sign and authenticate never straddle a switch. Not a paste job.

Reasoned, not tested — there is no PAC/BTI target in CI. Starting point for that port, not a
verified account of it.
