# JLib::Scheduler — engineering notes

A running log of decisions and the reasoning behind them. **This is the place for the "worth
flagging" details** — the ones that are obvious while the code is being written and invisible six
months later. Newest at the top. Each entry says what was decided, what it rules out, and what would
make it wrong.

---

## 2026-08-28 — The context switch pays an SSE/AVX transition, and it is 9x

**A fiber that parks straight out of an AVX kernel was costing ~9x a normal switch, on every
switch.** `ContextSwitch.asm` saves XMM6-15 with legacy-SSE `movdqa`; executing legacy SSE while the
upper halves of YMM are live costs an SSE/AVX transition. A vectorised workload is in that state
every time it suspends, so it paid it every time.

Measured by `bench/context_switch.cpp` — all arms in ONE process against ONE pair of stacks, reps
interleaved, medians, on a 13900K. Reproduced by two people on two machines to within 1.5%:

| | ns/switch | ratio |
|---|---|---|
| `movdqa`, dirty upper state (what shipped) | 85.8 | 1.000x |
| same, again — **same-vs-same control** | 85.9 | **1.001x** |
| `vzeroupper` + `movdqa`, dirty | 9.2 | 0.107x |
| `vmovdqa` (VEX), dirty | 9.2 | 0.108x |
| `movdqa`, CLEAN upper state | 9.2 | 0.107x |

**Three independent ways of avoiding legacy-SSE-with-dirty-upper all land on the same 9.2 ns.** That
triangulation is what the result rests on, not on any account of the microarchitecture — the
magnitude is larger than a reading of Raptor Lake's documented blend behaviour predicts, and no
explanation offered here would be worth more than the three-way agreement.

### The rule

> **One `vzeroupper` at the top of `ContextSwitch`, gated on a CPUID'd byte.**
>
> It covers BOTH halves of the routine — the saves for the outgoing fiber and the loads for the
> incoming one — because nothing between them re-dirties the upper state.

**Why gated rather than unconditional: `vzeroupper` is ITSELF an AVX instruction**, and so is
`vmovdqa`. Either one taken unconditionally raises the whole library's floor from baseline x86-64 to
AVX (2011+) — a real change to what it claims to run on, in exchange for an optimisation that has
nothing to optimise on a pre-AVX CPU: no AVX means no dirty upper state means no transition. The
gate is one load from an always-hot line plus a perfectly-predicted branch, measured at **~0.35
ns/switch** against ~76 ns saved.

`JLibCtxHasAvx` is constant-initialised to 0 and only ever raised, so a switch that happens before
the initialiser runs behaves exactly as it did before this change: correct, and slower. There is no
ordering in which the gate fails dangerously.

**Destroying upper YMM state here is legal, not a liberty.** The upper halves are volatile across a
call under the Win64 ABI and a context switch is an opaque call, so anything live up there is
already spilled to the fiber's own stack. `tests/avx_suspend_test.cpp` is the standing check and
passes against the gated version — it is now load-bearing rather than reassuring, because the switch
actively zeroes the state that test cares about.

### What this rules out

- **Windows only.** The POSIX x86-64 switch has NO XMM block at all — SysV makes every XMM
  caller-saved — so it executes zero legacy-SSE instructions and has nothing to fix. AArch64 has no
  SSE/AVX domains. Do not go looking for this elsewhere.
- **`vmovdqa` (VEX) was measured, not dismissed** — identical at 9.2 ns. It was not taken because it
  carries the same AVX floor while being a twenty-instruction rewrite of a routine that was already
  correct, against a one-instruction addition.

### What would make it wrong

Absolute numbers moved ~1.3 ns between batches on the same binary, INCLUDING the unchanged floor
row. Ratios held. Anyone re-running this should read the ratios and check that the same-vs-same
control still reads ~1.00x before believing any other row.

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

---

## Deferred — trace zones (the spec, before anyone writes one)

Not built. Written down first because the obvious version of this is wrong in a way that is hard to
undo once a profiler capture exists and looks plausible.

**Zone the chunks a profiler user cares about**, and nothing else: `push`, `run task`, `park`,
`steal`, `overflow dequeue`, `epoch tick`.

**One zone around the whole worker loop is noise.** It reports "the worker was in the worker loop,"
which is true and useless, and it hides the transitions that actually explain a frame.

**One zone per SUCCESSFUL steal is enough. Do not zone the empty-victim scan.** A failed probe is
the common case by a wide margin, so zoning it buys a trace dominated by nothing happening and puts
instrumentation directly on the steal path — the one place in this scheduler where a few
instructions have been measured to matter.

**Nothing may land near the deque CAS.** Same rule the power-throttling work follows: a syscall or a
timestamp on the steal CAS taxes the hot path to observe a cold one.

**Behind a compile flag, so the bench binary has zero Tracy/PIX calls.** Not a runtime bool — the
existing `JLIBSCHED_*_STATS` options are the precedent, and they exist precisely because a timing
taken with instrumentation on must never be compared against one taken with it off.

**Name the threads `jlib-p0` / `jlib-e3`** so P/E shows up in the trace. The class is already known
(`isPCore`), and a trace that cannot distinguish a P worker from an E one cannot explain the
scheduling decisions this library makes.

## Power throttling — SHIPPED, and it follows topology

`SetWorkerPowerThrottling`, Windows `SetThreadInformation` + `THREAD_POWER_THROTTLING`. Default is
`Topology`: **P-core workers opt out of throttling, E-core workers are throttled.**

**Never EcoQoS a thread you then set as an ideal P-core.** That asks the OS for two opposite things
— put this work on your fastest core, and run it slowly — and delivers a clamped P-core. The first
version of this was a blanket opt-out for every worker; that avoids the contradiction but throws
away information already in hand, since an E-core worker draining background bulk has no reason to
burn turbo budget.

**Set once at worker entry, never per task.** Per-task QoS is a syscall per task. The call appears
on no per-task path, no steal path, and nothing near the deque CAS — two call sites total, the
definition and the thread entry.

**Linux is deliberately a no-op.** `SCHED_IDLE` and nice mean "background" far more strongly than
EcoQoS does, and most game loops should not ask for that; `cpu.uclamp` is an administrative cgroup
setting a library has no business writing, and Android's cgroups override it regardless.

**Darwin is flagged, not guessed.** QoS is the lever (`USER_INTERACTIVE` vs `UTILITY`) and
`HotThreadPolicy` already sets it for hot workers, but a class-following version cannot be tested in
CI — same treatment as the AArch64 PAC/BTI gap.

---

## Fatal paths — what is death-tested, and the two deliberately left open

**An abort path nothing executes is the same shape as a negative control that cannot fail.** Both
look like safety and provide none. Three were shipped in that state and are now covered
(`deque_ceiling_test.cpp`, `hazard_ceiling_test.cpp`), every one **dual-direction** — an
over-the-line child that must abort, paired with an up-to-the-line child that must exit cleanly.
Without the pair, a structure that aborted on *every* operation would pass.

| path | trigger | covered |
|---|---|---|
| `TaskDeque::FatalGrow` | push past `maxCapacity` | yes |
| `FatalCellOverflow` (cell index) | `Protect(k)`, `k >= kCellsPerReader` | yes |
| `FatalCellOverflow` (external slots) | more non-worker readers than `kExternalReaders` | **was an `assert`** — now fatal |
| `FatalPushRefused` | `push_bottom` refuses a non-null item | unreachable by argument; hard-stops anyway |
| record registry exhausted | coroutine + full `kMaxRecords` | hook verified from the non-coroutine side only |

**Abort, not `exit(code)`.** The spec these came from used distinct exit codes per handler. They
keep `std::abort()` instead: abort produces a minidump and `exit()` does not, and in production the
core dump is the entire point. Tests distinguish *which* handler fired by matching its stderr text,
which is equally specific and costs nothing in Release.

### Two left open on purpose

**The coroutine record-exhaustion abort itself.** Driving a real C++20 frame to that branch needs
careful frame-state setup so the child does not leak or hit UB *before* reaching the hook — a test
that crashes for the wrong reason is worse than no test. `ExhaustRecordsForTest` is wired and
verified from the non-coroutine side, which holds the line.

**`Retire(nullptr)` staying a silent no-op.** Making it fatal is a genuine API behaviour change:
`free(nullptr)` semantics is what a caller reasonably expects, and lenient cleanup paths may depend
on it. It deserves an audit of every call site before the trigger is pulled, not a quick edit.

## Park primitive: WaitOnAddress vs condition variable (4.0.2, open A/B)

`TaskScheduler::SetParkPrimitive` / `JLIB_PARK=cv` / bench `park=cv|park=wait` select what a worker
blocks on when it parks. Default `WaitAddress` = `WaitOnAddress` on Windows, `FUTEX_WAIT_PRIVATE` on
Linux. `CondVar` = a per-worker `std::mutex` + `std::condition_variable`, the pre-futex mechanism.

**Why the flag exists.** `bench/futex_variance.cpp` says condvar has the tighter wake distribution,
and it has now said so twice (stddev 0.71x, p99 0.96x, re-run at N=5000 on 2026-08-29). But that
harness is a two-thread ping-pong: it has no push path, and the push path is the only place the two
primitives differ in cost. **The isolated bench cannot decide this and should not be cited as if it
had.**

**What actually differs.**
- Condvar must publish `WS_AWAKE` *inside* the waiter's mutex. A condvar has no memory, so the lock
  is the only thing that closes the lost-wakeup window that `WaitOnAddress` closes with its value
  compare. That puts a mutex acquire on the notify path -- which is what got condvar replaced.
- The `WaitAddress` arm pays four seq_cst flag loads, a CAS and three inbox re-reads before it
  blocks. The condvar arm does that same work inside the predicate, under a lock it already holds.
- The reason the trade may have changed shape since it was first decided: `NotifyWorker` now skips
  entirely when the target is awake, so on a floored pool the notify mutex is only taken when a
  worker is genuinely parked.

**How to read the A/B** (`build/park_ab.ps1`, interleaved arms, min across reps):
- `latency` is a **control, not a result**. It reports `kernel wakes: 0` -- with a live floor nobody
  parks on that row, so neither primitive is called. Movement there is the noise floor.
- `burst` (idle pool, every wake is a kernel wake) and `throughput/1p` (one producer, 31 workers
  running dry) are the rows where the primitive is actually exercised.

**Linux is not part of this.** `FUTEX_WAIT` and `WaitOnAddress` are different primitives with
different costs; a Windows condvar win is not a reason to touch the futex park. Linux keeps
`WaitAddress` unless separately measured on WSL.

### Correction (same day): the first A/B row could not see the park at all

The A/B above was first run on `burst` and `throughput/1p` in the DEFAULT config. Both are blind to
the thing being tested, and the wake counters now printed on those rows are what proved it:

| row | kernel wakes | row time | park share |
|---|---|---|---|
| `burst` | 15 | 14,230 us | 0.3% |
| `throughput/1p` | 813 / 5 runs | 66 ms | 0.7% |
| `latency` | 0 | -- | 0% |

A wake is ~3 us. Under 1% of every row, against +/-40% run-to-run noise on `burst`. Seven interleaved
reps of that produced exactly what it had to produce -- noise with no direction -- and it would have
printed the same had the arms differed by 2x. **The awake floor is why: its whole purpose is to keep
workers off the park, and it succeeded.**

The row that CAN decide it is the latency row with the floor switched off:

```
SchedulerBench.exe park=wait|cv floor=0 nogrow resv=0 nosweep noev
```

`latency` then does ~19,400 kernel wakes for 20,000 round-trips and the mean moves 0.57 us -> 4.34 us,
so ~87% of the row is the park. The printed wake count is a VALIDITY CHECK on every rep: if it is not
~20,000 the row is not exercising the park and that rep says nothing.

**Generalise this.** Before A/B-ing a mechanism, print how much of the row the mechanism accounts for.
"No difference" and "the mechanism was never called" are the same output otherwise.

### Result (2026-08-29): `WaitAddress` stays. Measured, not assumed.

7 interleaved reps per arm at `floor=0 nogrow resv=0`, ~19,400 wakes / 20,000 round-trips:

```
wait  mean min 4.09 med 4.18 | p50 min 4.00 med 4.10 | p99 min 5.60 med 6.50 | max min 104.20
cv    mean min 4.32 med 4.37 | p50 min 4.20 med 4.30 | p99 min 5.90 med 6.60 | max min 103.10
```

Mean and p50 separate with **no overlap between arms** (7-vs-7, p ~ 1/3432): ~0.17 us/round-trip, ~4%.
p99 overlaps and `max` is 103-318 us in both -- that tail is preemption, not the park.

**`bench/futex_variance.cpp`'s stddev 0.71x does not transfer to the scheduler.** It measures a
two-thread ping-pong with no push path; treat it as characterising the primitives in isolation and
nothing more.

**Why condvar loses, and it is not mainly the notify mutex:** a condvar wake must reacquire the mutex
inside `SleepConditionVariableCS` before it can return and re-check its predicate, so the woken thread
takes a lock before it can run. `WaitOnAddress` returns straight to the work loop.

The `CondVar` arm stays in-tree as the standing negative control for this claim.

### Darwin: `CondVar` is the default there, and the Windows result does not apply

`ParkPrimitiveDefault()` returns `CondVar` on `__APPLE__`. Not a fallback -- the only option. Darwin
has no address wait (`WaitOnAddress` is Windows, `FUTEX_WAIT` is Linux, `__ulock_wait` is private
API), so the `#else` arm of the park loop is a bare `break`: **every idle worker spins on its core**,
and the awake floor is meaningless because no worker was ever going to park.

That is what the condvar buys on Darwin, and it is why the ~4% Windows loss is irrelevant there --
the comparison on Darwin is not condvar-vs-futex, it is condvar-vs-spin. `JLIB_PARK=wait` still
selects the spin, which makes it the negative control for exactly this claim.

Consequence: the awake floor is now a real feature on all three platforms, and `GetAwakeFloor()` is
no longer advisory anywhere. The stale "Darwin has no park" comments in `Thread.cpp` are gone.

Not verified on real hardware -- no macOS runner was used for this change. Both arms are green on
Linux (used as the POSIX proxy) and on Windows.

---

## Migration, mailboxes, and the lock split (design intent, not yet all implemented)

### Fibers migrate, and it decides four subsystems

A resumed fiber may continue on ANY worker. Full reasoning is at the top of `TaskScheduler.h`; the
short version is that pinning exists to protect a library from `thread_local` state it cannot audit,
and a game engine owns every job on its fibers -- so the hazard becomes a rule we enforce rather than
a structure we build. And stack warmth decays with suspend duration while waiting for one specific
worker does not, so under a frame deadline resuming NOW on a cold stack beats resuming later warm.

What pays for it: `SlabPool::Free` routes by address, epochs use a global participant list with CAS
rather than a per-shard scheme, and hazard cells are indexed by the fiber rather than the thread.

**A sharded reclamation design (Seastar-shaped: shared-nothing per core, cross-core frees as
messages) is cheaper and scales better -- and is unsound under migration**, because a read that
starts on worker A and finishes on B belongs to no single shard. That branch requires pinning. Pick
one and follow it; the expensive mistake is paying migration's costs while accepting pinning's
constraints.

### Mailboxes are faster round-trip; deques are faster parallel

No single structure is both. A mailbox is one hop with a single legal consumer -- which also makes it
*de facto* affinity, since a task returned to its owner's inbox resumes on that worker with the stack
warm. That is pinning's benefit as a PLACEMENT rather than a rule, and the drain to the deque is the
escape hatch for when the owner does not come back promptly.

Consequence, and it cost a day to rediscover: work in an inbox is invisible to the pool until its
owner passes through its loop. A 16-task wave of 3.3 ms bodies aimed at two never-parking workers
reached 9 of 31 workers; the same wave through `PushBatch` reached 18-30. That was the CALLER, not
placement -- `Push` means "start this now", `PushBatch` means "here is a pile".

### The lock split: spin first, then suspend -- and two types, not one branch

Parking immediately is the wrong half for a frame, where most holds are microseconds. The shape to
build is marl's: spin for the short case, and after a timer suspend the fiber/coroutine. Uncontended
cost stays spinlock-cheap; long holds still do not burn a core.

**Two lock types, chosen by call site, not one type branching at runtime.** The invariant that makes
suspension possible -- every running context is a fiber or coroutine -- holds INSIDE the pool and
nowhere else. Main and bare app threads can only block, and a worker-style lock that helps (runs
other tasks while waiting) must never be used from main, which would run arbitrary work while holding
frame state. One type serving both means an unpredictable branch on a hot path and two different
semantics wearing one name.

### Deferred work: epoch decides WHEN, shard routing decides WHERE

Keep them separate. A cross-thread free should be an intrusive push onto the owning shard's list
(the dead slot's own first word is the link -- no allocation, and single-consumer makes it ABA-free),
drained by the owner when its local cache runs dry.

Do NOT make freeing a task. It stacks two independent deferral mechanisms on one operation -- the
epoch's "safe at epoch N" and the mailbox's "whenever the owner gets round to it" -- so the actual
free happens at the max of two things that do not know about each other. And a free-task must be
allocated from the slab, so reclaiming memory would consume memory, which fails exactly under the
pressure that makes frees urgent.

---

## Slab: what to look at when memory management gets reworked (2026-08-31)

Raised while fixing a flaky test, and worth having written down before anyone touches the slab.

### Cross-shard deletion IS accounted for -- the question is answered

`SlabPool::LiveCount()` sums per-thread shards **on demand** rather than maintaining a total, and
its own comment states the invariant: *"Individual shards go NEGATIVE and that is correct, not a bug
to clamp: a slot allocated on one thread and freed on another leaves +1 on one shard and -1 on the
other. Only the total means anything."*

So the accounting survives cross-shard free. Two consequences that are easy to forget:

- **Never clamp a per-shard counter at zero.** Negative is the correct reading of "this thread freed
  more than it allocated", which is the normal state for a worker that consumes tasks produced
  elsewhere. Clamping would silently inflate the total.
- **`LiveCount()` is a SMEAR, not a snapshot** -- shards are read one at a time while other threads
  keep working. Anything asserting exact equality on it is asserting that no other thread touched a
  shared counter for the duration, which is not a property of anything being tested. That is exactly
  what made `tests/coroutine_test.cpp` flaky: 1 run in 10 read 199 of 200, with the 256-byte column
  at 0 every time, so the size class was never what failed.

### The real open question is LOCALITY, not correctness

`SlabPool::Free` pushes the slot to the **freeing** thread's cache, not the one that allocated it.
The count balances; the *slot* does not go home. A producer/consumer split -- one thread minting
tasks, workers freeing them -- drains the producer's cache and grows the consumers', and the only
thing bounding the drift is cache overflow spilling to the shared pool. Whether that matters is
unmeasured. It is the first thing to instrument if slab behaviour is ever suspected, and it is a
different question from `homeShardCtx`, which is about a slot knowing where it belongs at all.

### If a test needs an EXACT answer, the counter is the wrong instrument

Option 3 from the discussion, recorded because the cheap fix (settle + slack) was taken instead:

> Have the allocator report which size class a **specific allocation** came from, rather than
> inferring it from a delta on a process-wide counter.

That removes the shared-counter dependency entirely and would make the 64-byte-class control exact
instead of tolerant. It needs an allocator API addition, so it was not worth it for one flaky check
-- but if the slab is being reworked anyway, this is nearly free to add at the same time, and it is
the only way to assert "this frame came from that class" without a race.

## Deferred — bench report polish, before the repo goes public again (2026-08-31)

Jay's call: these are presentation, not correctness, and they wait for the public
push. Recorded because "when it goes public" is exactly the horizon items fall off.

**The ParallelFor crossover table has an ordering bug.** Not diagnosed here, and the
symptom was not written down precisely enough to reconstruct — re-read the table
against its own column headers before assuming which axis is wrong. The
`distinct workers that ran a leaf` line under each row is the one that looks
inconsistent with its columns first.

**One table has no real comparison and should be deleted rather than fixed.** A row
that reports `NO VERDICT -- every cell's own control moved >5%`, or a ratio with a
confidence interval like `[0.39 .. 1.33]`, is not a measurement anybody can act on;
printing it invites someone to read a number out of it anyway. Better to remove the
section than to keep shipping a table whose honest answer is "this harness cannot
resolve this".

WHY THESE ARE WORTH THE TRIP AT ALL. Three separate report defects were found in one
evening while chasing a stall that did not exist:

  - the `completion` segment blamed the WAITER for time the WORKER spent in task
    teardown (TaskDAG::OnTaskDiscarded, then the waitGroup fetch_sub);
  - the per-class exemplar suppressed the one SCHEDULER-IMPLICATED trip, because a
    non-implicated trip of the same class printed first;
  - `landed on the floor: 20000 of 20000 (100.0%)` — the best possible result on
    that row — was read as "100% of aim MISSED", because the line named neither the
    desired direction nor what it counted;
  - `steal/bt` read as "batch stealing", which cannot exist with a Chase-Lev deque.

None of those was a scheduler bug and every one of them cost real time. THE RULE
THIS KEEPS PRODUCING: a diagnostic whose best result can be mistaken for its worst
is broken regardless of the number underneath it, and a label that names a
mechanism the system does not have will send someone looking for it.
