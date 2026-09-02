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

## The I/O lane without an MPMC FIFO — Jay's design, 2026-08-31 (NOT BUILT)

Recorded because it dissolves the blocker this session concluded was structural, and because I
told him the opposite twice.

**WHAT I SAID AND WHY IT WAS WRONG.** First: "you are one well-chosen queue away", which assumed a
shared ordered MPMC could load-balance completions. Then, correctly, that it could not -- completions
resume PINNED fibers, so the destination is fixed by fiber ownership and no queue changes that. From
which I concluded the ~K-worker ceiling was structural. **THAT SECOND CONCLUSION ASSUMED YOU NEED
MULTIPLE CONSUMERS. You do not.**

**THE DESIGN.** Concurrency on the I/O lane comes from FIBERS ON ONE CONSUMER, not from many
consumers:

  - the reactor / completion thread ONLY ENQUEUES. It never runs user code.
  - ONE owner is the only pop -- K=1, or one chosen floor worker. MPSC stays MPSC.
  - handlers are Fiber/Coroutine. `co_await` suspends the frame and returns the worker to its drain
    loop, so the consumer is never blocked and the same consumer pops the next completion.
  - two producers into one inbox is still one consumer. Ordering is per-queue FIFO with a single
    reader, which is all a completion stream needs.

One thread drives thousands of in-flight operations exactly as a single-threaded async runtime does.
**The MPMC FIFO requirement disappears because the multi-consumer case was never needed.**

**MOST OF THE MACHINERY ALREADY EXISTS.** `hiPriInboxes[q]` is a TaskMPSCQueue; both pop sites
(`Worker()`'s HiPri phase and `TryTakeLaneTask`) are owner-only by qIndex; it is drained FIRST on
every pass, ahead of the deque and stealing; and it is named in all three park predicates. The delta
is CONTRACT AND POLICY, not plumbing.

**WHAT IS ACTUALLY NEW, and none of it is built or modelled:**

  - **NATIVE ON THIS PATH MUST BE REJECTED, NOT TOLERATED.** A Native handler runs to completion on
    the consumer thread and IS the stolen consumer. Nothing enforces fiber-only today. This is the
    [[tolerance-is-not-permission]] shape: code that survives a violation is defending against it,
    not permitting it.
  - **BOUNDED + BACKPRESSURE AT THE REACTOR.** The inbox links through Task::next and is unbounded.
    DO NOT BAIL A SINKING OWNER BY HANDING WORK TO F -- that is the stray-net mistake again. Late
    packets on one owner beat lost packets on two.
  - **K=1 AS THE RULE.** K=4 sharing one backlog with hand-off when behind is the shape to stay away
    from; it is what forced the MPMC question in the first place.
  - **NOT IN ANY MODEL.** The three verify files cover the permit word and the loPri queue. A
    fiber-only single-consumer completion queue with backpressure is unmodelled.

**K IS NOT BROKEN BY THIS SESSION'S WORK -- it is better suited to it.** "K never reads loPri"
(k-never-reads-lopri-invariant) leaves K's queue set as exactly {hiPri, resumed}, which IS the I/O
lane. The reactor stays off the shipping surface (branch `io-reactor`, tag `io-reactor-preserved`)
until someone wants to build this.

### Correction, same evening: the pinning argument was about FIBERS, not the reactor

I concluded twice that the I/O lane had a structural ceiling because "completions resume PINNED
fibers, so no queue can load-balance them". **THE REACTOR DOES NOT RESUME FIBERS.** IoAsync.h:
"`co_await` for asynchronous I/O. C++20 -- and the ONLY C++20 in the reactor", with
`await_suspend(std::coroutine_handle<P>)`. A coroutine frame is a heap allocation and `resume()` may
be called from ANY thread. `resumedInboxes` is the FIBER path -- pinned, unstealable, per-worker --
and it is a different mechanism that happens to sit next to this one.

So completions ARE freely routable, round-robin IS valid, and the MPMC FIFO question is closed for
good rather than merely side-stepped: K single-consumes the reactor queue, each kworker
single-consumes its own mailbox, and every edge is MPSC.

**JAY'S SHAPE.** K is a boss poll loop that pulls completions and round-robins them into a vector of
MPSCQueue, one per kworker; kworkers are std::threads with their own ids (kworker0, ...) outside the
F bands; K and each kworker are accounted into the thread budget the way EnableTimers and
EnableIoReactor already reserve cores. Handlers are coroutines, so v5 makes the runtime C++20.

**THREE THINGS THAT STILL NEED ANSWERING, none of them the queue:**

  - **A BOSS THAT NEVER PARKS BURNS A CORE.** "K never leaves" means one core in a poll loop with
    zero I/O in flight, permanently, in every app that enables the reactor -- the NoSleep tax,
    forever, on one core. K should PARK on an empty reactor queue and be woken by the enqueue. That
    is the full EMPTY/NOTIFIED/PARKED permit machine, not the YIELD state: YIELD is for a thread
    leaving the core briefly and still owed no syscall. Same for kworkers on an empty mailbox.
    Making them Thread objects past the bands -- the `nw` non-worker-lane shape, `deques.size()` is
    already `num_workers + 1` -- gets the verified handshake for free.
  - **EXACTLY ONE RESUME MAY BE IN FLIGHT PER FRAME.** Not-pinned is not the same as safe-to-resume-
    twice. A cancel racing a completion must not put the same handle in two mailboxes. See
    [[io-cancel-requires-requestcancel]]: scope.Cancel() alone already leaves a parked read parked,
    so the cancel path here is not hypothetical.
  - **A HANDLER MAY RESUME ON A DIFFERENT KWORKER AFTER EVERY AWAIT.** Correct for the frame, fatal
    for any thread_local or same-thread assumption inside it -- and the FIBER path guarantees the
    opposite, so the two contracts must be documented as opposites rather than assumed alike.

### The shape, as decided (2026-08-31). NOT BUILT.

**K IS A REAL THREAD AND IT PARKS.** Check inbox -> run immediately if there is a task -> otherwise
WaitOnAddress until there is one. That is the permit machine already built and verified, so K and the
kworkers inherit it by being Thread objects; there is no NoSleep tax and nothing new to model. My
"demote K to a function" suggestion does NOT apply here -- it assumed pinned fibers made routing
static, and Jay's model has a live decision per completion.

**FIBER PER COMPLETION, NOT PER OPERATION**, and this is the part that makes the whole thing cheap.
K round-robins completions across kworkers, hands each one a fiber, and the kworker returns it to the
moodycamel free list when the handler finishes.

    fiber per OPERATION      budget = max in-flight ops. 1000 connections x 64 KB = 64 MB, committed
    fiber per COMPLETION     budget = concurrent handlers, i.e. ~kworker count. Tens of fibers.

The 64 KB x connections problem disappears: the budget is bounded by concurrent HANDLERS rather than
open connections. The cost is handler style -- a handler needing a second I/O op stores state on the
connection and returns, rather than awaiting inline. State machine, not linear async. It is also why
round-robin is legal: no continuation state lives in the fiber across completions.

**TWO FIBER POOLS.** Completions get their own pool shared with K. Jay's call, and explicitly "the
easiest even if not the best" -- the alternative is one pool with a reservation, which shares
capacity but needs a policy. Two pools cannot starve each other and an exhausted I/O pool is a local,
sized, reportable condition instead of a mysterious compute stall.

**WHAT THE TWO-POOL DECISION MUST CARRY, and it is the only sharp edge in it.** GlobalFiberPool is
not just a free list, it is a SIZING AUTHORITY. Exactly two sites index flat arrays by a fiber's own
poolIndex:

    Event::EnsureTable()   slots = new std::atomic<Task*>[TotalCount()]
    Hazard.cpp:129         sizes by TotalCount()

With two pools, IO fiber poolIndex 5 and compute fiber poolIndex 5 write THE SAME Event waiter slot.
Silent corruption, not an error. The fix is small and must not be skipped: give the I/O pool an index
range starting at the compute pool's count so indices are globally unique, and size both tables by
the UNION. `GlobalFiberPool::Create()` is a factory returning a pointer, not a singleton, so a second
instance is already constructible -- it is the indexing that needs the work, not the plumbing.

**THE RECYCLE POINT IS THE OTHER HAZARD.** A fiber must not return to the free list while the reactor
could still deliver a completion for it: the next acquire hands out the same Fiber* and a stale
completion resumes the wrong frame. Same pointer, different owner -- ABA, and this codebase has been
bitten there once already (ReturnBatch clearing localEpoch = SIZE_MAX). Sequenced after the reactor
guarantees no further completion, which is what RequestCancel is for -- and cancellation on this path
already has a sharp edge: scope.Cancel() alone leaves a parked read parked. RECYCLING TURNS THAT FROM
A HANG INTO A CORRUPTION, which is why this is the piece to model before writing it.

Note the free list is moodycamel and that is CORRECT here even though it was the wrong answer for
ordered completions: a free list has no ordering requirement, any fiber is as good as any other.
Same library, opposite verdict, for a real reason.

### pushIO must be able to FAIL, and what that implies

**K IS NOT A THROUGHPUT BOTTLENECK -- a pop and a few atomics per completion, millions/sec on one
thread -- BUT IT IS A SERIALIZATION POINT.** Without a fallible push a full mailbox means K spins or
blocks, and the whole pipeline stops behind one slow kworker.

**THIS IS THE LESSON THE REST OF THE SESSION KEPT TEACHING.** The loPri strand existed because push
always succeeded, the queue was unbounded, and a self-healing net quietly rehomed the consequence --
so no caller ever had to own a policy. A bounded push that returns false forces one. Precedent
already exists: TaskDeque's push_bottom_batch refuses and the caller requeues rather than dropping.

    TaskMPSCQueue::push is `void push(Task*)` -- infallible and unbounded. pushIO is a NEW bounded
    variant; inboxDepth already exists per worker as the counter, so it is a depth test and a false
    return rather than new plumbing.

**JAY'S RULE:** strict round-robin, re-aim on false until it lands, WITHOUT advancing to the next
task -- so a completion is placed before another is considered -- and if every kworker refuses, park
it in K's own std::queue backlog, which is safe unsynchronised because K is single-threaded.

**TWO THINGS THAT RULE IMPLIES:**

  - **DRAIN THE BACKLOG BEFORE PULLING NEW COMPLETIONS.** Otherwise K keeps accepting from the
    reactor while its own queue grows -- unbounded memory, and the backlogged items starve behind an
    endless stream of fresh ones, serving the OLDEST completions LAST. That inverts the ordering the
    bounded mailbox was there to protect.
  - **ROUND-ROBIN REQUIRES "AT MOST ONE OUTSTANDING OPERATION PER CONNECTION" AS A REACTOR-LEVEL
    INVARIANT.** Two completions for the SAME socket landing on different kworkers can be handled
    concurrently, which breaks stream ordering -- and nothing in pushIO can fix that, because by then
    they are on separate threads. It is normally free (the next recv is not issued until the current
    one completes) but it is a CONTRACT the reactor must state, not a property that happens to hold.

### It is TWO SCHEDULERS SHARING ONE PARK MACHINE, and that fixes the naming

Jay's framing, and it resolves a confusion that ran through this whole design conversation: the
library is an ASYNC I/O SCHEDULER and an FTL AT THE SAME TIME, one opted into from the other. What
they share is the park machine -- the permit word, YIELD, the wake protocol. What they do NOT share
is queues, priorities, or placement policy. That is exactly why the verify files kept needing "this
file is about the loPri queue" as a caveat.

**hiPri NEVER MEANT "MORE IMPORTANT", IT MEANT "THE LANE"**, which is what the labelling argument was
actually about. F is the priority in the jobs system and it is the only one. Renaming hiPri* to io*
makes this session's invariant self-describing: "K never reads loPri" has to be held in the head,
"THE I/O LANE NEVER READS THE TASK INBOX" is obvious on sight. NOT ALL WORKERS ARE EQUAL is a
deliberate property here, not an accident of implementation.

**IOMPSCQueue IS A SEPARATE TYPE AND THE DUPLICATION IS THE POINT.** A bounded queue whose push can
FAIL is a different CONTRACT, not a configuration of the same one. Parameterising TaskMPSCQueue would
put a new failure edge inside the path mpsc_model.c already proves; copying leaves that subject
untouched. `bool push()` is also what stops the copy being code smell -- it is the reason the type
exists. CAVEAT: the copy needs ITS OWN MODEL. Full/empty and a failing producer are new states and
mpsc_model.c says nothing about them.

**REACTOR-OWNED BACKLOG ELIMINATES pushIO ENTIRELY.** The retry-and-spill logic collapses into one
single-threaded loop instead of a shared helper every caller could get wrong: pop a completion, try
push into the next kworker's queue round-robin, on false park it in a plain std::queue -- no mutex,
because the only producer and the only consumer of that backlog are the same thread -- and drain the
backlog before pulling anything new.

**OPEN, and asked rather than assumed:** is K the reactor's DISPATCH THREAD (pulls completions,
round-robins, owns the backlog -- collapsing a hop and a wake), or a separate thread the reactor
feeds? "A K worker is deployed as a subcontractor of the reactor" and "the backlog is handled by the
reactor itself" read as the former. The two differ in thread count and in how many wakes a cold
completion pays, so it is worth stating before anyone builds to it.

**UNMEASURED, and stated as such:** the sleep/wake cost of an exclusive sub-scheduler. The claim is
that a poll-then-park loop is cheap because the kworkers only wake when work exists, and every
existing check already works. Plausible -- a wake is ~3 us and an I/O completion is not free either
-- but nobody has run it.

### The I/O lane design has moved to design/IO_LANE.md

The sections above are the conversation that produced it, kept for the reasoning and the two
corrections in it. THE DESIGN ITSELF IS IN design/IO_LANE.md and that is the one to read: NOTES.md
grew eight appended sections in one evening and the decisions were no longer findable.

Superseded by that document, and NOT to be re-derived from the sections above:
  - "two fiber pools" -- it is ONE pool BISECTED, which is what removes the Event/Hazard poolIndex
    collision rather than requiring it to be managed.
  - "the backlog eliminates pushIO" -- PushIO IS needed, and private with the reactor a friend,
    because the scheduler's PLACEMENT path is not thread-safe even though the queue is. A reactor
    retrying inside the shared push would obstruct an application thread pushing to F.
  - "K may be demoted to a function" -- K stays 1-3 real dispatch threads.
  - "the backlog is a std::queue" (twice above) -- it is TWO INTRUSIVE STACKS. Task already carries
    `std::atomic<Task*> next`, so in-stack and out-stack are one member pointer each and the whole
    structure allocates NOTHING; a std::queue is std::deque-backed and allocates in chunks, on the
    one path whose entire job is absorbing a burst. Reverse into the out-stack ONLY when it is empty
    -- reversing early puts newer arrivals ahead of older ones and inverts the ordering silently.

---

## Global task registry for migratable fibers: the fields go on the FIBER, and one already exists

2026-09-01. Design sketched: a global registry plus two links per task, so a fiber can migrate while
its thread-affine debt stays payable at one home.

```
Task { Task* next; Task* homeNext; uint8_t home; }
```

**THE STRUCTURE IS RIGHT AND THE PLACEMENT IS ALREADY DECIDED — ON Fiber, NOT Task.** `Fiber.h` was
written against this exact question and answers all three parts of it.

**1. `home` ALREADY EXISTS.** `Fiber::homeWorker` (`include/Fiber.h:54`) is a `size_t`, `SIZE_MAX`
means unbound, it is "set once, where the fiber is bound to a task, and read by every resume path to
decide where the fiber goes back", and it has 9 call sites. That is the proposed `home` field,
already shipped and already wired.

**2. `homeNext` ON Task RE-ADDS A RETIRED BUG.** From the `nextWaiter` comment
(`include/Fiber.h:56-68`), verbatim:

> AN EARLIER DESIGN THREADED THE WAITER LIST THROUGH Task AND WAS RETIRED FOR A REAL BUG: the links
> WERE the tasks, so a task freed back to the slab while a list still held its address meant the next
> drain walked a recycled slot. Fibers are never freed -- the global pool reserves and leaks them --
> so a link through a fiber cannot dangle that way.

A per-home list is a list that outlives a single queue transit by construction — the whole point is
durable membership — so it is *more* exposed to that recycled-slot walk than the waiter list was, not
less.

**3. Task HAS NO ROOM ANYWAY, and the 8 free bytes are the expensive ones.** `sizeof(Task) == 64` is
a static_assert. `uint8_t home` is genuinely free (byte 51, in the padding the 2.9.0 flag packing
opened). `Task* homeNext` is not: it claims the 8 bytes of deliberate tail padding, and the site says
what that costs —

> LambdaTask<F> stores F as a member after this base, and BOTH MSVC and GCC reuse the base tail
> padding -- measured [...] Claim these bytes and every single-capture lambda jumps 64 -> 80, which
> moves it out of the 64-byte class into the 128-byte one. That is a 2x memory regression on the most
> common task in the system, paid for one field.

`Fiber` has **no size assert and is not size-classed**, so `Fiber* homeNext` beside `nextWaiter` is
free, dangle-proof, and argued for by a comment already in the file.

**The registry is then a flat array of heads indexed by worker**, not a map — same shape as Event's
fiber-indexed waiter table, which `Fiber.h` cites as the precedent for exactly this reasoning. And
there is no per-pickup lookup: membership is a link on the fiber you already hold, reachable from
`Task::assignedFiber` in the same cache line.

### What is actually being traded, stated plainly

`homeWorker`'s comment ends: "Pinning removes the class of bug rather than asking every future call
site to remember the rule." **The registry replaces a structural guarantee with a bookkeeping one.**
Pinning makes TLS misuse impossible; a registry makes the debt *payable* but leaves every future call
site that touches `thread_local` obliged to know it. That is a real cost and it should be chosen
knowingly, not discovered later. The mitigation is the same one §3.8 of IO_LANE landed on: the home
write belongs in the resource wrapper at acquisition, not in the handler remembering.

### Open, not answered here

- **Unlink.** Push-at-head is one CAS; removing one entry mid-list is the hard half. Owner-only
  unlink (H owns its own list) plus CAS at the head is workable; drain-and-relink-live avoids the
  problem entirely at O(live) per drain.
- **One creditor or many.** A fiber that runs on A, migrates, and allocates on B owes both. One
  `home` is sufficient only if affine allocation is *bound* to `home` rather than to the current
  thread — which puts cross-thread traffic on the allocation path. Free-by-address plus a remote-free
  list is the alternative and needs no home at all.
- `home` was also described as "owner index **or stream id**" — two namespaces in one byte needs a
  discriminator or a partitioned value space.

---

## The push path writes three fields on the consumer's Thread, and they were on two cache lines

2026-09-01. MEASURED AND FIXED. `PushLocal` writes `inboxDepth`, `hasQueuedWork` (MarkQueuedWork)
and `workerState` (NotifyWorker) on the worker it selected, once per task, while that worker is
concurrently reading or RMW-ing all three. Every distinct COHERENCE line among them is a transfer
the producer pays per task; `PushBatch` pays it once per batch.

```
before   inboxDepth 2460 (line 38)   hasQueuedWork 2472 (38)   workerState 2520 (39)
after    inboxDepth 2460 (line 38)   hasQueuedWork 2472 (38)   workerState 2476 (38)
```

**RESULT, on the rows that hold still:**

| | before (2 runs) | after |
|---|---|---|
| latency p50 | 0.60 us | **0.50 us** |
| latency p99 | 0.90 us | **0.70 us** |
| latency/cold | 0.55 us | **0.48 us** |

p50 read 0.60 on every run that day at 0-2% spread, so this is well outside noise. ~48 bytes of
lane and debug counters (`laneWake`, `dbgOnAwakeFloor`, `dbgSpinTick`, `laneCyclesTotal`...) had
grown between the two halves of a hot triple.

**READ THE 64-BYTE NUMBER, NOT `platform::kCacheLine`.** That constant is 128 -- the adjacent-line
PREFETCH pair, correct for padding a counter away from unrelated data and NOT the coherence unit.
At 128 the answer was "1 line, nothing to fix" and this investigation would have closed on it.
x86 invalidates in 64. `tests/thread_layout_test.cpp` prints both and guards against a future field
splitting them again.

**DO NOT MEASURE THIS ON THE THROUGHPUT ROW.** The effect is one line transfer -- tens of ns against
a 422 ns push -- and that column swung 320..662 ns WITHIN a single run. Only the latency row has the
resolution (p50 stable to 0-2%).

### What is still on that path, and what is NOT the answer

- **CAS retries are not it.** `JLIBSCHED_RETRY_STATS` measured `MAX 0` at both the bands word and
  the WaitGroup waiter push; the collapse gate's own counter agrees at `casLost=56` of 35,976,149.
- **`NotePush`/`NoteWakeMiss` were removed** -- two per-push atomics, plus a `GetWorkerState()` load
  of the target's permit word, feeding counters NOTHING ever loaded. The comment that kept them
  claimed "the bench reports them"; the bench reports `g_wakeCalls`, a different counter.
- **`inboxDepth`'s producer-side atomic is the next candidate, and estimating it is the wrong fix.**
  Its ONLY reader is the worker's own publish-to-stealable gate (`Thread.cpp`, `>= kStealHintDepth`).
  A push-local counter cannot feed it -- the reader is the WORKER, not the pusher. But since the
  worker is the sole reader it can observe depth from its own drain ("batch came back full"), which
  removes the producer's RMW entirely rather than giving it an error bar. Batching the producer
  instead would UNDERCOUNT between flushes and delay the publish-to-stealable, which is the
  reachability mechanism the burst rows show matters most.
- The growth gate already uses the push-local-counter idea, and rejected depth for it explicitly:
  "it cannot use depth. 200,000 no-ops pile up behind two workers exactly like sixteen 3.3 ms bodies
  do, and gating on depth grew the floor to 16 on the no-op row: 1p 10.0 -> 5.2 M/s."

### REJECTED: removing inboxDepth's producer-side write

Proposed after the cache-line win, on the reasoning that the worker is its ONLY reader (the
publish-to-stealable gate) and could therefore observe depth locally, deleting an atomic RMW from
every push. It cannot, for a reason that is specific and fatal:

**A WORKER CAN ONLY LEARN ITS DEPTH BY CONSUMING.** A worker-local "consecutive pops" counter
reaches the threshold of 8 only after the worker has RUN eight tasks. In `burst/dflt` those are
3.3 ms bodies, so the publish would fire ~26 ms into a 9.9 ms row -- long after the wave needed to
become stealable, and reachability is what that row is limited by. The producer's write is what
makes depth known BEFORE anything runs, which is the whole value of it.

Two other shapes were already tried and are recorded at the gate:
- drain whenever the inbox is non-empty: one producer walked its whole backlog into one deque and
  hit the 65,536-slot ceiling on 200,000 no-op tasks.
- an away-bit maintained on the dispatch path: throughput/1p 5.37 -> 2.93 M/s, frame DAG
  8.45 -> 35.82 us/graph.

So the push path's remaining atomics are all load-bearing, and after the colocation they share one
coherence line. Closing the rest of the gap to marl needs a profiler, not more reading.

### KNOWN FLAKY: SchedulerWaitGroupCancelTest, ~2%, PRE-EXISTING (root cause found -- see below)

Measured 2026-09-01 while landing the fiber-death cleanup hook, because an intermittent failure on a
WaitGroup test immediately after touching the fiber ACQUIRE path is exactly the thing not to wave
through. Baseline first, on a stashed tree:

    without the change   1 / 60 failed
    with the change      2 / 40 failed

Too close and both samples too small to distinguish; the point is that it fails WITHOUT the change.
It prints "ALL CHECKS PASSED" and then exits non-zero, so the failure is after the assertions -- on
the teardown path, not in what the test checks. Jay: "i was thinking of testing that and i knew it
would break."

**RECORDED SO THE BASELINE DOES NOT HAVE TO BE MEASURED AGAIN.** A ~2% flake is invisible in a single
run and looks like a regression the first time a change happens to hit it.

### ROOT CAUSE of the WaitGroup flake above: WaitFor(wg, token) HAS NO DELIVERY PATH

The note above records the flake as pre-existing with the mechanism unknown. It is known now, and
it is not a race -- it is a hole in the design. Jay named it: "you fell on an infinite lock, nothing
signals the event to let you out, and you expected the token to work."

**A TOKEN IS PASSIVE.** Storing one in `WaitGroup::cancellable` creates no path out. It is a FILTER
applied by whoever walks that list, and walking is what has to be caused.

**AND NOTHING CAUSES IT FOR WaitGroup.** Cancellation dispatches ejection BY TYPE, and the table is:

    Timer.cpp    EjectEvent               -> Event::CancelWaiters()
    Timer.cpp    EjectSemaphore           -> SchedulerSemaphore::CancelWaiters(tok)
    Timer.cpp    EjectConditionVariable   -> SchedulerConditionVariable::CancelWaiters(tok)
    IoShared.cpp / IoReactor_stub.cpp     -> IoAcceptor::CancelWaiters(tok)

There is no `EjectWaitGroup`. So a fiber parked in `WaitFor(wg, tok)` is released by exactly two
things: the group COMPLETING, or an explicit `wg.CancelWaiters(tok)`. Cancel a scope on a group that
will never complete -- which is what cancelling usually means -- and the fiber waits forever. A
parked fiber cannot poll a flag.

**THE TEST PASSES ONLY BECAUSE IT DOES THE DISPATCH BY HAND**, one line after `scope.Cancel()`:

    scope.Cancel();
    const std::size_t woken = inner.CancelWaiters(scope.Token());   // the system never does this

Delete that line and it hangs. The ~2% flake is the same hole seen through a different interleaving
-- whether the group happens to complete before the test gives up -- which is also why it produces no
failure output: it is a hang, not a failed assertion, and it is load-sensitive because load decides
the ordering (3/100 with builds running, 0/200 on a quiet box).

**A RETRACTION, RECORDED BECAUSE THE REASONING WAS WRONG IN A USEFUL WAY.** Before this was found, I
argued the bug was the raw `uint32_t token` stored beside a pooled `DirectEvent*` -- identity kept
next to the object rather than in it. That is wrong: CancelToken is a 16-bit slot + 16-bit
GENERATION precisely so a stale handle stops resolving ("that generation check is the whole reason
the handle is not a bare pointer"), and the list hygiene is correct and documented on both paths
("REMOVE BEFORE WAKE, never the reverse"). Both halves were already protected. The bug was never
identity; it was that nobody ever looks.

**THE FIX IS NOT ANOTHER Eject* ENTRY.** A dispatch table that must name every primitive is the same
bug waiting for the next primitive -- WaitGroup was simply the one that got forgotten. What removes
the class is making the WAITER responsible for its own exit rather than depending on another party
choosing to walk a list: a bounded deadline after which it wakes and re-checks. That is the point of
the timer in the primitives rewrite, and it is a different thing from the timed worker sleep that was
rejected: a worker's park is signal-driven BY CONTRACT, so a timeout there masks a lost wakeup, while
a cancellable wait's release depends on an external decision that may never arrive.

### FTL migratable fibers: the complete protocol (Jay's design, 2026-09-01)

Recorded whole because the pieces arrived across a long session and only make sense together.

**PICKUP REGISTERS.** Any worker that picks up a `TaskType::Fiber` -- a fresh bind OR a resume of an
existing context -- registers itself on that fiber. Both branches converge at one point in
`Thread::Worker()` and that is where it goes. It is the RETURN ADDRESS: the worker is about to
context-switch in and run whatever the fiber does next, which may incur state only that thread can
release. Registering only at bind would record the first worker and miss every one a migrating fiber
afterwards ran on -- exactly the set whose cleanup would then be orphaned. One `fetch_or`, idempotent,
so a fiber bouncing between two workers a hundred times records two creditors.

**DEATH WALKS THE REGISTRATIONS.** For each one carrying a marker, create the deletion task and mail
it to that worker's inbox; the last hop recycles the fiber. Chain, not fan-out -- see the note above
on why queueing N tasks and then recycling is wrong.

**THE MARKER GATES THE TASK, THE REGISTRATION DOES NOT.** `creditors` records WHO ran it; the marker
records WHAT is owed (slab / epoch / hazard). A fiber that never incurred affine state has creditors
and no markers, so the chain walks and dispatches nothing. That is what lets registration sit on the
pickup path at all.

**AND IN MIGRATABLE MODE, RECLAMATION STOPS BEING SELF-DRIVEN.** Auto-scan and the epoch tick are
DISABLED; `SetSelfReclaim` is false on both (hazards need that). Everyone frees their own memory
instead: whatever tick a worker would have done is NOTED IN ITS REGISTRY ENTRY, and the chain settles
it at fiber death. Jay: "their registry entry has their id and anything they owe, like a real little
computer city."

**WHY THE THREE KINDS ARE slab / epoch / hazard, and why this is bigger than COM handles.** Those are
exactly the three costs `TaskScheduler.h`'s architecture header says migration already paid:
`SlabPool::Free` routes by ADDRESS rather than by the freeing thread owning the block; epochs use a
GLOBAL participant list with CAS because sharding "is UNSOUND under migration"; hazard cells are
indexed by the FIBER, not the thread. The header says "pick one branch and follow it; the expensive
mistake is taking the cost of migration and the constraints of pinning at the same time." THIS IS A
THIRD BRANCH IT DOES NOT CONSIDER: take the CHEAP per-thread structures AND migrate, and settle up at
the end. That is the real prize, not apartment teardown.

**BOTH MODES SHIP.** Pinned is marl's contract, for middleware that cannot audit its host. Migratable
is for a process that owns every job and can enforce "do not cache a TLS-derived value across a
suspension point". Pinned is NOT a second code path -- it is this machinery with the creditor set
holding one member, so the cleanup walk is identical and only RESUME ROUTING reads the flag. Set once
before Init, so the branch predicts perfectly.

### STATE: what is built, and what is still not working

BUILT AND GREEN: `Fiber::creditors` + Note/Take/Has/Clear; `Fiber::ResetForReuse` called on ACQUIRE
(closes a real hole -- `ReleaseFiber` is `localCache.Push`, which scrubbed nothing; only the spill
path reached `ReturnBatch`); `FiberRegistry` with the address table, the chain, and a one-shot
CAS-claimed `ReturnToPool`; the death hook in `OnFiberReturned`; registration at pickup;
`RequeueResult` {Failed, Pinned, Stealable}; the migratable branch in `Requeue` plus a gate keeping
migratable mode out of `resumedInboxes` entirely.

NOT WORKING: `tests/migratable_fiber_test.cpp` reports ZERO migrations with the flag confirmed TRUE.
Its PINNED CONTROL IS SOUND -- 256/256 completed across 6 workers, 0 resumed elsewhere, which is what
pinning means and proves the test discriminates. The migratable arm shows every one of 256 tasks
resuming on the worker it left, which is a perfect correlation and therefore still pinning somewhere.
Candidates NOT yet eliminated: `ResumeQueueless` returns FALSE when the fiber is in WANTS_SUSPEND and
the WORKER then wakes it locally (`Thread.cpp`'s own-inbox push) without ever reaching `Requeue`;
and the Event path generally, which was a poor choice of instrument -- the property under test is
just "mailbox a task with a fiber attached and switch into it", and an Event drags in the waiter
table, SignalAll enumeration and batch resume, none of which was verified first.

### WIDTH vs REACHABILITY: which knob a row is actually limited by

Two different constraints, and each row is limited by exactly one. Getting them confused is why the
same controller looks brilliant on one row and useless on the next.

**F (the awake floor) FIXES WIDTH.** It is a HIT-RATE CONTROLLER: keep the next Push landing on a
worker that is already awake, so it costs no OS wake. The latency row is that law working -- `F 2->2`,
`placement HIT the floor 20000/20000`, `kernel wakes 0`.

**Wide FIXES REACHABILITY.** It skips the awake-map steer entirely and falls through to the full-pool
rotation, paying wakes to put work where anyone can run it.

**MEASURED, BOTH DIRECTIONS, SAME DAY:**

| row | limited by | what fixed it |
|---|---|---|
| blocking crossover | WIDTH -- 15 of 31 workers never ran a task because the ceiling was 16 | raising the F ceiling 16 -> 29: **9.50 -> 6.25 ms**, idle tax unchanged |
| burst | REACHABILITY -- 16 heavies land on two staged inboxes | `wide`: **~4.6x -> ~11.7x**, and the floor never grows at all |

Burst, four runs: `dflt` 9.92 / 10.90 / 11.52 / 13.21 ms at peak 9-14 with 7-12 participants, against
`wide` 4.40 / 4.41 / 4.64 / 5.07 ms at peak **2** with **31**. Stable, not a single reading.

**WHY MORE F CANNOT FIX THE BURST.** An inbox has exactly ONE legal consumer, so a wave staged on two
workers is not stealable however many pollers you wake -- the extra ones just spin. That is precisely
what `GROWTH OVERSHOOT: peak 13, participants 10` counts, and why the burst row's grown workers show
up as floorSpin rather than as throughput.

**THE RULE:** grow F when work is ALREADY STEALABLE; skip the steer when it is not. Do not ask the
hit-rate controller to also be Cilk.

### THE FORKING THESIS: mechanism is invariant, policy is not

What makes variants cheap is that the line is already drawn, and today's work sits entirely on one
side of it.

**INVARIANT ACROSS ANY POOL DESIGN** -- built, verified, and not re-litigated by a fork:
the PERMIT MACHINE (four states, every write an RMW, swap-to-wake, CAS-to-PARKED as the linearization
point, model-checked with the controls going RED); the FIBER STATE MACHINE (the WANTS_SUSPEND /
SUSPENDED / SUSPEND_SIGNALED CAS dance that makes a signal landing mid-park safe); the COROUTINE
library; the I/O reactor.

**POLICY, AND THEREFORE FORKABLE**: adaptive F vs adaptive K vs a fixed floor; steer-at-the-floor vs
wide; park promptly vs spin-then-park; pinned vs migratable fibers. A server build optimising
throughput would plausibly make K the important controller and take a different position on width
and parking -- none of which touches the four things above.

That split is why "fork it and try variants" is a real plan rather than a rewrite. The expensive,
provable parts are done once.

### FTL: corrections to the blueprint, and what is NOT built yet

**REGISTRATION IS NOT A DEBT, AND THE GATE MUST BE THE MARKER.** Wiring `NoteCreditor` at pickup
made `HasCreditors()` true for EVERY fiber, so every death dispatched a cleanup task per worker it
had run on -- to run an empty routine -- and recycled through the GLOBAL pool instead of the
thread-local cache. The death hook now gates on `Fiber::OwesCleanup()` (any kind set), not on having
creditors. Jay's rule, which the first wiring did not honour: registration records WHO, the marker
records WHAT, and no marker means no task.

**JAY'S TWO CORRECTIONS TO THE BLUEPRINT, both still OPEN:**

1. **The list needs a `RegisteredFiber` struct as its next pointer, not just a number.** What is
   built is a per-fiber BITMASK of creditor worker ids -- that answers "who does THIS FIBER owe?"
   in O(1) and is right for the cleanup chain. It does NOT answer the reverse question, "which
   fibers owe ME?", except by scanning every fiber in the pool and testing one bit. A struct with a
   next pointer gives a per-worker list and makes that enumeration O(owed).

2. **Dynamically joining/leaving threads need an early-collect-and-unlink.** A worker that is about
   to go away must settle its debts BEFORE it disappears, which means finding every fiber that owes
   it and unlinking itself -- a function taking the fiber as an argument. That is the reverse
   enumeration from (1), which is why (1) is its prerequisite.

   NOT NEEDED TODAY: the pool builds its workers once in StartPool and destroys them once in Join,
   and the floor sheds by PARKING a worker rather than retiring it. So no worker currently
   disappears while fibers are alive. This is a requirement of a FUTURE dynamic-pool variant, and
   worth recording now because the bitmask silently cannot serve it.

**STATE.** Built and green: creditor set, kinds/marker with the gate, FiberRegistry (address table,
chain, one-shot CAS recycle), death hook, registration at pickup, ResetForReuse on acquire.
NOT built: anything that SETS a kind (no resource wrapper exists), the reverse enumeration above,
and migratable resume still reports 0 migrations in its own test with the flag confirmed TRUE.

## Migration and thread_local: what is safe, what is enforced, and what was parked

Found while getting `migratable_fiber_test` from RED to GREEN. The test reported **0 migrations**
while the router reported **186-203 of 256 resumes sent away from home**. The routing was right the
whole time; the test could not see it.

`CurrentQ()` was inlined, so MSVC loaded the TLS address of `Thread::instance` ONCE and reused it on
both sides of the wait -- across `WaitOnEventArmed`, an opaque extern call that context-switches. A
fiber that suspended on worker 1 and resumed on worker 4 read worker 1's TLS block and reported "I
did not move." Marking `CurrentQ()` `noinline` took it from 0 to 206 with no scheduler change at all.

**The compiler caches TLS across an opaque call.** That is measured here, not assumed.

### The pattern the reclamation code already uses

`CurrentEpochSlot()` (Thread.h) is the precedent, and it is two halves, not one:

- **Storage on the fiber** -- the slot is `&f->localEpoch`, so it travels with the fiber.
- **Access through TLS** -- it is reached via `Thread::GetCurrent()->currentFiber`, which is exactly
  the read that can go stale.

Its comment says the fiber branch is "migration-proof." The SLOT is. The LOOKUP is not. What makes
it safe is a separate, ENFORCED invariant: `JLIB_EPOCH_CHECK_NO_GUARD` fires at all eleven suspend
points (`Suspend`, `CoYield`, `WaitOnEvent`, `WaitFor`, `WaitOnEventArmed`, `WaitOnEventDirectArmed`,
both `SchedulerMutex` locks, both `Semaphore` waits, fiber exit). A guard cannot span a switch, so
the cached TLS cannot go stale inside one.

So the rule for any new fiber-affine state is: **storage on the fiber, access via TLS, correct only
inside an enforced no-suspend window.** Do not hold a TLS-derived value across a suspend.

### Parked: registry identity (tail-1)

Considered and NOT built. The idea was that registration returns "are you first", and a caller that
needs the PREVIOUS worker reads tail-1 of a per-fiber pickup list (you register yourself at the tail).

It has no caller. Every candidate is answered by something already built:

- "Which workers do I owe?" -> the creditor bitmask, which dedups by construction.
- "Run this on that worker" -> the return-sender task. It does not NAME the thread, it STANDS on it:
  `AdvanceCleanup` takes a creditor out of the mask (already the worker id, and what selects the
  inbox), mails the work there, and `Thread::GetCurrent()` is correct when it runs.

Naming the previous thread would only be needed to decide something BEFORE dispatch, or to have
someone other than the owner release the state. Neither exists. Building it now would add a second
identity source drifting alongside the bitmask.

A stack-pointer anchor was also proposed (`(addr - arenaBase) >> 16`; the arena is one contiguous
reservation at a uniform 64 KB stride, and the bounds test doubles as `isOnFiber`). It is layout-
viable and also has no caller, for the same reason -- the enforced window makes the TLS lookup fine.
Note it does NOT survive the planned third stack size class unchanged: a second arena with a
different stride turns it into a small sorted `{base, end, log2stride, firstIndex}` table.

### User thread_local: pinned mode, and why a registry does not fix it

No FTL that migrates caches the user's `thread_local`, and we do not either. A
`vector<std::any>` registry was considered and rejected -- not mainly for type erasure and the
allocation per entry, but because it does not close the hole. What makes user TLS dangerous is that
it is IMPLICIT: someone writes `thread_local int depth;` and nothing tells us it exists. A registry
holds only what was DECLARED, so anything forgotten is still broken and still silent, and "forgot
one" is indistinguishable from "correct" until it corrupts.

**Pinned mode is sound for all user TLS, including the ones we were never told about.** That is the
contract, and it is a correctness contract, not a performance note:

> If your task bodies keep thread-affine state across a suspend, run pinned.

The constructive alternative, if one is ever wanted: a typed `FiberLocal<T>` anchored on the fiber,
so `thread_local X` ports to `FiberLocal<X>` and is correct under migration by construction. Not
built.

### The diagnostic that settled it

`JLIBSCHED_REQUEUE_TRACE` (CMake option, default OFF) counts Requeue's three exits -- lane / pinned
/ placed -- plus how many placements chose home, and stamps `Fiber::lastPlacedOn`.

THE STAMP IS THE POINT. Two aggregate counters each counted honestly and disagreed 203-vs-2, and
neither could check the other because they never counted the same task. Joining them on ONE task --
the router stamps where it sent the fiber, the fiber reads it back after it wakes -- is what made
the disagreement legible instead of just puzzling.

## A per-pass singleton must be LEAKED, not a function-local static

Wiring the TokenRegistry drain into the worker loop crashed **five tests that never touch the
registry** -- `DagCancelTest` 3/3, plus `MutexCancel`, `IoOptIn`, `ReservedLoPriPlacement` and
`WaitGroupCancel` intermittently. ACCESS_VIOLATION (0xC0000005) and heap corruption (0xC0000374),
faulting module `ntdll`. Every one of them passed when run ALONE, and the batch failure set changed
run to run -- which reads exactly like machine load or an orphaned process, and was neither.

**Cause: static destruction order.** `TokenRegistry::Instance()` was

```cpp
static TokenRegistry r;   // destroyed at exit, in reverse construction order
```

and drain point 1 reads it on EVERY pass of EVERY worker. `AtExitDestroyer` calls `Join()` from its
own destructor, so the registry is destroyed while the workers it serves are still spinning; each
pass then reads `inbound` through freed storage. The fix is the one the fiber pool already makes:

```cpp
static TokenRegistry* r = new TokenRegistry();   // leaked on purpose
```

A process about to stop existing gains nothing from freeing its address space, and everything from
not pulling a structure out from under threads that are still running.

**`FiberRegistry::Instance()` has the same shape and has not been changed.** It is only touched at
fiber death rather than every pass, so it is far less likely to be caught in the exit window -- but
that is a probability argument, not a safety one. Worth fixing the same way.

### How it was localised, which is the reusable part

Not by reading the crash. `ntdll` + a varying failure set + individual passes is a fingerprint for
"someone else's memory", and the stack pointed nowhere near the cause. What found it was a
**one-line negative control**: `JLIB_TOKENDRAIN_CTL_NO_WORKER_DRAIN` compiles drain point 1 out. With
it, all five went green 3/3. That turned "the suite is flaky" into "this exact statement" in one
build, and the control is worth keeping for the same reason.

## The drain point has to be where a WOKEN worker reaches it

Drain point 1 was first written next to the post-task epoch `Tick()` -- the natural-looking "between
tasks" seam, and wrong for the only case that matters. **A worker woken purely to do cleanup has no
task to finish**, so it never reaches that branch: it wakes, finds nothing in any queue, and parks
again still holding the debt. `token_drain_live_test` caught it immediately -- 8 tokens delivered to
parked workers, 8 wakes, **0 released**.

It belongs at the TOP OF THE PASS, before the worker decides there is no work.

The guard is `HolderHasWork()`, an acquire LOAD and not an RMW, because it now runs every pass on
every worker. `Event::SignalAll` paid for the other version: it exchanged every word of its occupied
table unconditionally, so waking ONE waiter cost 35 RMWs at 31 workers -- invisible to every test,
because correctness was unaffected.

## Delivery must notify, and why it is not just latency

`TokenRegistry::Deliver` links the token onto the holder's chain -- no task, no queue push -- but the
WAKE is still mandatory. That chain has exactly one legal consumer, so a parked worker never drains
it. The cost is not delayed cleanup: **a token cannot be recycled until its chain drains, and the
token is embedded in the fiber, so a parked creditor holds a fiber out of the pool.** Under pressure
that is starvation.

Controls, both verified RED: `NO_NOTIFY` -> 8 delivered, 1 released, 7 never ran (the one is a worker
that happened to be awake, which is why the assertion is on all 8 rather than on "any"). 
`NO_WORKER_DRAIN` -> 0 of 8 released.

## Harness note: three broken PowerShell harnesses in one session

1. `Start-Process` + `WaitForExit` left `ExitCode` blank, reporting **31 of 31 tests failed**.
2. A control loop reported all four token-registry controls PASSING; they were fine.
3. `cmake -DJLIBSCHED_TOKENDRAIN_CTL=$ctl` inside a `foreach` did not expand `$ctl`, so the compiler
   received the literal `JLIB_TOKENDRAIN_CTL_$ctl=1` and BOTH controls silently ran the unmodified
   build and "passed". Caught by grepping the generated `.vcxproj` for the define rather than
   trusting the run.

The pattern is the same every time: a harness that reports the reassuring answer. **When a control
passes, check that the define reached the compiler before concluding anything about the code.**

## Is FiberRegistry thread-safe? The audit, member by member

Asked directly, and worth writing down because the answer is "yes, but for a structural reason, and
three things rest on call ordering rather than on the types."

**No striped mutex is needed, and the striping is already there.** The only shared mutable state on
the hot path is `inbound[h]` -- ONE ATOMIC HEAD PER HOLDER. A Treiber push (CAS) and a drain
(exchange) on a per-holder head have no shared head to contend on, so partitioning by holder IS the
stripe. There is no cross-holder operation on any hot path.

| member | protection |
|---|---|
| `inbound[h]` | atomic head; Treiber push, one-exchange drain. Many producers, one legal consumer -- the same split the resume inboxes have. |
| `externalNext` | `fetch_add`, with the bounds check on the PRE-increment value so a counter that runs past the end still refuses. |
| `Fiber::creditors` | `fetch_or` to record, CAS-to-clear to take. Two threads taking concurrently get distinct creditors. |
| `CurrentHolder`'s cache | `thread_local`, and a bare thread never migrates -- the same argument `CurrentEpochSlot`'s thread fallback rests on. |
| `table`, `workers`, `pool` | written ONLY by `Build`, read everywhere. Safe because Build runs at Init before any worker exists -- **an ordering precondition, not a type guarantee**. |
| the seams | were plain pointers. **This was a real race** -- see below. |

### The one real race, and where it came from

`fiber_drain_live_test` installs its release hook AFTER `TaskScheduler::Init`, so the pool is already
up and workers are calling `DrainHolder` -- which read `release` as a plain `ReleaseFn`. That is a
data race that happens to work on x86, which is the worst combination: every run survives it and
TSan is right about it.

Now `std::atomic<DispatchFn/RecycleFn/ReleaseFn>`, relaxed -- the pointer is the whole payload, there
is nothing published alongside it to acquire. Resolved through `Dispatcher()` / `Recycler()`, which
load ONCE: the old `(dispatch ? dispatch : &Default)` reads the pointer twice, so a seam installed
between the test and the call would be invoked without ever having been checked.

### What is still ordering-based

`Build` swaps `inbound` (a vector of atomics can only be rebuilt and swapped, never resized), and
clears `table`/`workers`/`pool`. Doing that while anything reads them is UB. It is currently
prevented by StartPool calling it before any worker is constructed -- correct today, and enforced by
nothing. If dynamically joining threads ever land, this is the first thing that breaks.

## Sequencing note: what "everything is a fiber" actually unlocks

The tick was unsafe on a worker because it runs arbitrary deleters on a bare thread that CANNOT
suspend -- unbounded time, and a deadlock if a deleter ever waits on something whose resume is pinned
to that same worker. **A tick that runs on a fiber can just suspend**, and the worker moves on.

So the fiber default is not a cost paid for tidiness; it is what makes worker-side reclamation legal
again, which is what lets the epoch garbage lists be thread-local without needing a message to
discharge them. The order matters: the fiber flip had to land before the epoch rework, not after.

## A whole tier of tests was not being built

`JLIBSCHED_COROUTINES` is **OFF by default** and gates every C++20 target -- `SchedulerCoroutineTest`,
`SchedulerFutureTest`, `SchedulerCoroEpochTest`, `SchedulerHazardCoroSuspendTest`,
`SchedulerIoAsyncTest`, `SchedulerIoSocketTest`. The default is defensible (requiring C++20 to build
the suite would quietly make C++20 the project's real floor) and it has a sharp edge:

**Counted epochs were removed with zero coverage of the coroutine path they existed for.** The suite
was green at 35/1 the whole time, and green meant nothing -- the tests for the thing being deleted
were not compiled. Turning the option on immediately found two real problems.

**Rule: any change to coroutines, or to anything a coroutine reaches, is untested until
`-DJLIBSCHED_COROUTINES=ON` has been built and run.**

### What it found, neither of which was the epoch change

**1. The fiber default moved hazard reader identity.** `HazardDomain` indexes cells by
`Fiber::poolIndex` for fibers and **by worker** for native tasks. `hazard_coro_suspend_test`'s
nested-worker-row case built its task with a plain `CreateTask` and got Native BY DEFAULT; once the
public default became Fiber, the task moved to a fiber row, the "second guard on the same worker row"
collision stopped being possible, and the abort it asserts never fired.

Generalised: **every public task now takes a fiber row rather than a worker row for hazards.** That
is an improvement -- fiber rows are the migration-safe ones -- but it is a behaviour change, and any
code that depended on worker-row semantics has to ask for Native explicitly.

**2. `SchedulerIoAsyncTest` and `SchedulerIoSocketTest` HANG.** Bisected in a worktree at `14a6dbd`,
before the fiber flip: **both hang there too**, so this is pre-existing IO breakage and not caused by
making the public job a fiber. Not chased -- the reactor is being revised to own its backlog with a
stack and an aimed `pushIO` at an available worker, and that rework is where the real IO test comes
from. A stranded backlog is a known possibility in that design.

One of these hangs was found orphaned at **1187 CPU-seconds**, starving every other test in the run
and making unrelated tests look flaky. When a suite result changes shape run to run, check for a
leftover process BEFORE reading anything into the failures.

## Thread-only epoch slots: 2016 participants -> 32

`CurrentEpochSlot()` returned the running FIBER's slot, and `GlobalFiberPool` registered one EBR
participant per fiber. `MinActiveEpoch` scans every participant on every reclaim attempt, so it
walked `coreCount * StandardFibersPerWorker` of them. Measured on a 31-worker box:

    before   MinActiveEpoch: 0.629 us per call over 2016 participant slots
    after    MinActiveEpoch: 0.016 us per call over   32 participant slots

**The fiber slot was never protecting the fiber.** It made an UNCHECKED violation harmless:
suspending inside an EpochGuard is forbidden, and because the slot travelled with the fiber, doing
it anyway was merely slow (the slot stayed announced, reclamation stalled, and it surfaced later as
allocator exhaustion). A leak, not a crash -- which is exactly why nobody had to fix it.

On thread-only slots the same violation is a USE-AFTER-FREE: enter announces thread A's slot, the
fiber resumes on B, and the destructor clears B's slot -- which was never set -- un-announcing a
live traversal on B and freeing its nodes underneath it.

So this is the counted-epoch trade one level up, and it takes the same answer: **delete the
insurance, enforce the rule.** `JLIB_EPOCH_CHECK_NO_GUARD` is now live in RELEASE and reads the SLOT
rather than a depth counter -- `t_epochGuardDepth` and its ENTER/LEAVE macros are gone entirely, so
even dev builds stop paying two thread_local ops per guard. Fibers and coroutines collapse into one
check, because with one slot per thread it is one question.

### OPEN: is running a task inside an EpochGuard legal?

`TaskDAG::ForEachDependent` holds a guard while it FIRES dependents. A dependent that runs inline
therefore reaches "fiber exit (task returned)" inside someone else's guard, having done nothing
wrong -- `dag_external_test` hits this deterministically, 4/4.

That makes the EXIT site ambiguous in a way the SUSPEND sites are not, and one slot read cannot
separate them:

    LEAKED   this task took a guard and never dropped it -> reclamation stalls from here on
    NESTED   an outer traversal on this thread is still live -> nothing is wrong

So the exit site warns ONCE and does not abort (`JLIB_EPOCH_CHECK_NO_GUARD_AT_EXIT`), while the
suspend sites still abort, where the answer is unambiguous.

**The real question is not settled.** If a dependent fired inside `ForEachDependent` ever SUSPENDS,
the outer guard spans a migration and the ambiguity stops being benign -- it becomes the exact
use-after-free the suspend check exists to prevent. Either dependents must not be fired inside the
guard, or the walk must copy the edges first and drop the guard before firing. Not decided here.

## Thread-local epoch bags: four structures became one

    BEFORE                                   AFTER
    moodycamel MPMC `incoming`               -
    `pending` staging vector                 each thread's own carry-over, private
    `reclaiming` CAS gate                    - (nothing shared to arbitrate)
    retire = implicit-producer enqueue       vector::push_back

**THE GATE EXISTED BECAUSE `pending` WAS SHARED.** One global staging vector means exactly one thread
may drain into it. Per-thread bags give every reclaimer a private slice, so the gate is not optimised
away -- it is unnecessary. All three go together; one shape, not three fixes.

The retire path loses its atomic entirely. It was a moodycamel implicit-producer enqueue -- hash the
thread, maybe allocate a block, copy 24 bytes -- and is now a `push_back` into a vector only the
owner touches.

`ShouldSelfReclaim()` asks **this thread's** bag rather than a global counter. With private bags a
worker that retired nothing has nothing to reclaim however busy the pool has been; the global count
would send it scanning every participant to free none of its own.

Shape copied from `HazardDomain`'s `thread_local RetireBatch` -- orphan store, dead-bag flag and all
-- rather than invented. That one already handles thread exit correctly and has been in service.

### The behaviour change, and why it needed a test

Only the OWNER drains its own bag. The old global queue meant ANY reclaimer freed EVERY retirement,
so "the suite is green" used to imply memory came back. It no longer does, and **nothing in the suite
asserted reclamation at all** -- survivable before, not now.

`epoch_reclaim_test` covers four claims, and the middle two are the ones this rework introduced:

    1. retire + tick  -> freed
    2. NEGATIVE CONTROL: no tick -> NOTHING freed (else claim 1 passes on a build that frees eagerly)
    3. another thread's tick does NOT drain a LIVE thread's bag  <- the documented change
    4. a thread that retires and EXITS hands its bag off        <- the safety net for 3

Without 4, private bags would make a short-lived thread an unconditional leak. Without 3 asserted,
the change gets rediscovered later as a bug.

**The skew cost is real and unmeasured:** a worker doing most of the removes accumulates on itself
between its own ticks, where the global queue would have let an idle thread do the work. That is a
tail cost, invisible to a throughput mean, and it is the price of deleting the gate.

### Measured on a 31-worker box

    MinActiveEpoch   0.629 us / 2016 slots   ->   0.015 us / 32 slots
    guards/sec       1.65e9                  ->   3.60e9

## busy=1 + lo=1 is a SELF-DEADLOCK, and it looks exactly like a lost wake

`dag_cancel_test`'s 25% was not a cancellation defect, not a lost wake, and not a race on
`Task::started`. `DumpPoolState` at the moment of failure:

    23  NOTIFIED  queued=1  busy=1  inbox(hi/lo/rs) 0/1/0   <-- LAST PUSH TARGET
    awake bits = 3 (0x800003)      advertised queues = 0

**Someone running on worker 23 was blocked, while the work they were waiting for sat in worker 23's
own inbox.** An inbox has exactly one legal consumer and nothing may steal from it, so the only
thread permitted to pop it was the one that could not.

In this instance the blocking was a busy-wait in a test holder:

    m.Lock();
    while (!release.load()) std::this_thread::yield();   // occupies the worker forever
    m.Unlock();

**AND PLACEMENT PREFERS THAT WORKER.** Steering favours AWAKE workers, and a worker spinning in a
task body is awake precisely BECAUSE it is spinning -- so it is a preferred target for the very task
that will then be stranded on it. The 25% is the probability that placement picked the one worker
that could never drain it. Holder suspends on an Event instead: 20/20 green, from 6/2, 8/2, 14/6.

### THE GENERAL SHAPE, WHICH THE TEST FIX DOES NOT ADDRESS

    something blocks WITHOUT SUSPENDING on a worker
      + the work it waits for is in that worker's own inbox
      = deadlock, unstealable by construction, indistinguishable from a lost wake in a dump

A FIBER that blocks is fine -- it suspends and frees the worker; that is what fibers are for. The
dangerous callers are the ones that block while still occupying the thread:

  * a busy-wait in a task body (this bug)
  * `ContendedSpinStep` -- the lock's contended path spins ON a worker, so a contended
    SchedulerMutex has this exact shape
  * anything Native that waits, which is why Native must not

So the diagnostic is worth keeping: **`busy=1` with a non-empty own inbox means look for a blocked
non-suspending caller on that worker**, not for a missing NotifyWorker. The dump already says as
much for the SLEEPING case ("state=SLEEPING and queued=1"); this is its awake twin, and it is the
harder one to read because a busy worker looks healthy.

### Sequence of wrong answers, kept because the eliminations are the useful part

1. Missing `OnTaskDiscarded` on some discard path -- WRONG. One call site, every discard routes
   through it, guarded by `!task->started`.
2. Double terminal (dependents fired twice) -- WRONG. Instrumented every arrival at OnTaskFinished:
   2 failures in 10 runs, ZERO doubles.
3. Bare-thread wait helping -- WRONG. Turning it off left the rate identical (2/8 either way).
4. A visibility race on `Task::started` -- WRONG. The task never started at all.
5. The test's 120 ms sleep being too short -- WRONG, and the fix that disproved it is what found
   the real answer: replacing the sleep with an OBSERVATION made the failure say
   "the task never started", which is a dispatch statement rather than a cancel one.

Every one of those was eliminated by measurement rather than argument. The dump is what ended it.

## Who may block on a contended SchedulerMutex (9-02)

Two rules, and they point in opposite directions.

**A bare thread BLOCKS.** `Lock()` on a non-fiber used to call `SpinThenHelp`: spin on `Try_Lock` and,
between spins, run stolen Native work. On a fiber-backed pool that help can never succeed --
`GetTask` vets steal candidates with `fiberlessRunnable`, which rejects `TaskType::Fiber`, and that
is now every task in the pool. So it burned a core walking candidates it was not permitted to claim.
It is now a real condvar block. Measured cpu/wall while waiting: **1.00 spinning, 0.00 blocked.**

**A task with no fiber MAY NOT BLOCK AT ALL.** A Native or directly-resumed coroutine task runs on
the worker with `currentFiber == nullptr`, so it lands on the bare path rather than the suspend path.
Waiting there pins the worker while that worker's INBOX may hold tasks nobody else may run -- inbox
work is unstealable by construction. That is `busy=1 + queued=1` with zero advertised queues: a
self-deadlock that reports as a lost wake. It now aborts with an explanation instead.

This is not a new rule. It is the existing may-not-suspend contract stated over its other half: a
task that cannot yield the worker cannot WAIT on the worker either, and a contended `Lock()` is a
wait however it is spelled. Blocking would be strictly WORSE than spinning here -- same stranded
inbox, worker asleep instead of hot -- which is why the fallback is deliberately not applied to it.

Two things the controls caught that argument would not have:

1. **The refusal was placed before the uncontended try.** That branch had no fast path -- the first
   spin of `SpinThenHelp` was it -- so refusing on entry rejected every uncontended Native `Lock()`
   in the codebase. The prohibition is on WAITING, not on locking. `Try_Lock()` runs first now.
2. **The test reproduced the bug it was testing for.** The first holder used `sleep_for`, pinning its
   worker for the whole hold. Placement steers to AWAKE workers, that worker was awake *because* it
   was sleeping-but-busy, and the offending task sat in its unstealable inbox until the hold ended --
   so it measured an *uncontended* lock at 158 ms and the refusal correctly never fired. The holder
   now suspends on an Event, which frees the worker while still holding the lock.

`SpinThenHelp` is untouched and still used by the other primitives; only the mutex stopped calling it.

## The whole I/O reactor was one routing guard, not a K rebuild (9-02)

`IoC17Test` was red and `IoAsyncTest`/`IoSocketTest` hung. All three were filed as "pending the K
worker rebuild" and the hangs had been bisected to a pre-existing commit. **All three were the same
two-line routing bug, and the suite is now 52/52 with the reactor included.**

The chain, because no single link looks wrong on its own:

1. `CreateTask`'s `hipri` parameter defaults to **false**, so an ordinary continuation is LOPRI.
2. The reactor routes by the task's own flag -- `if (resume->hiPri) batchHi[...] else batchLo[...]`.
3. `EnableIoReactor(true)` implies `SetIoHotLane(1)`, so `hotN == 1`.
4. `pushSteered` builds its candidate list from `[0, hotN)` **regardless of priority** and pushes
   `PushBatch(..., cand + 1, 64, hiPri)` -- aiming the completion at worker 0, a RESERVED worker.
5. **K never reads its loPri inbox.** Thread.cpp:2286, "that is the invariant, not a preference",
   guarded in the readers and in all three park predicates.
6. Inbox work is unstealable. So the completion is not deprioritised, it is UNREACHABLE. Every
   worker parks with work outstanding: a permanent hang that reports as a lost wake.

The steering predates the 8-31 invariant. When "K never reads loPri" shipped, the reactor was still
aiming ordinary work at the lane, and nothing connected the two -- the invariant's own test
(`reserved_lopri_placement_test`) proves K does not READ loPri, which is true and was never the
question. Nothing tested that no one PUSHES loPri there.

**The fix is not to let K read loPri.** Isolation is what the lane is for. It is that ordinary work
never belonged on the lane: K-steering exists for LANE work, and a loPri completion is by definition
not that, so it goes to the floor -- where `PushBatch` would have put it anyway.

Two things worth keeping:

- **A stale label cost real time.** The test asserted "created a plain Native continuation task". The
  type was wrong (the default became Fiber) and, more importantly, the type was never the point --
  `hipri = false` was. The label pointed the eye away from the only word that explained the failure.
- **This is the third instance of one family.** `busy=1 + lo=1` (the DAG flake), `busy=1 + inbox`
  (reproduced by accident in the mutex test's own holder), and now `parked + lo=1`. Every one is
  work parked in an inbox nobody is permitted to drain, and every one presents as a lost wake.
  When the dump says every worker is idle and the queue depth is not zero, look at WHO MAY READ
  THAT QUEUE before looking at the wake path.

## K runs fiber jobs; the floor keeps the fiberless fast path (9-02)

The fiberless path runs a task straight on the worker's OS stack with no fiber bound. It is legal
only because Native is a CONTRACT -- it may not suspend, because there is nothing to switch away to.
On the floor that is the right trade: work that will never wait should not pay for a fiber.

**On K it is the wrong trade, and the reason is what K is for.** The reserved lane carries I/O
completions, and a completion is exactly the work that wants to wait -- read, then wait for the next
read. The reactor's continuations are Native (`CreateInternalTask`), so fiberless they cannot suspend
at all, and since 9-02 they cannot block either: `SchedulerMutex::Lock` refuses a fiberless task
rather than stranding the worker's unstealable inbox. A fiber costs one acquire and one
ContextSwitch and buys suspend/resume, which is the entire capability the lane exists to provide.

    const bool fiberless = type == Coroutine || (type == Native && !reservedForHiPri);

**Coroutines stay fiberless deliberately.** Resuming one is a plain `fn(data)` call that returns, and
a coroutine OWNS ITS OWN COMPLETION -- the worker must never complete or free it. The fiber path
completes whatever it runs, so routing a coroutine through it would free a task the coroutine also
frees. They have their own suspension mechanism and need no fiber for it.

Safe because the two completion paths are already identical for Native: same WaitGroup
`fetch_sub` + gated `WakeAll`, same `CleanupTaskMetadata`/`DestroyTask`/`Free`. Checked before
changing the branch, because a fiber path that skipped the WaitGroup decrement would have turned
every `ParallelFor` on the lane into a hang.

`tests/reserved_fiber_job_test.cpp` measures 200/200 reserved with a fiber and 200/200 floor without,
then asserts the CAPABILITY rather than the binding: a Native task on K suspends on an event and is
resumed, which was unreachable before. **The floor arm is the negative control** -- binding a fiber
to every Native task would leave arm 1 green while putting a ContextSwitch on every task in the pool,
and only the floor arm going red distinguishes those two.

Keyed on where each task ACTUALLY ran, not where it was aimed: hiPri is routed into the reserved band
but can spill to another hiPri inbox when K is busy, so "I pushed it hiPri" is not "it ran on K".

## The reactor owns its backlog, and PushIO is the one Push that may say no (9-02)

Lane completions no longer go straight to a worker. They queue in the completion thread's own FIFO
deque and drain into a reserved worker through `TaskScheduler::PushIO`, which **returns false** when
every K worker is buried -- and then the completion STAYS in the backlog instead of being dumped
somewhere it would sit behind a handler.

**The bool is target selection, not a queue failure.** `TaskMPSCQueue::push` cannot fail -- its own
note says so, "the item is committed". A try-push at the queue level would be a check that always
returns true, which is worse than no check because it reads like a guard. So `PushIO` lives on the
scheduler, where the question ("was there an available K worker") actually has two answers.
Availability is `LaneBacklogMask()` -- one atomic load, not K per-worker queries -- and it refuses a
non-hiPri task outright, because a loPri task on K is the unreachable-inbox hang all over again.

**FIFO, not LIFO**, and this was the one real design fork. An item in the backlog has ALREADY waited
on the wire; LIFO would serve the newest first and starve the oldest without bound under load, which
is the opposite of what a reserved lane is for. The usual LIFO argument is cache locality and it does
not apply here -- the completion goes to a DIFFERENT thread, so the buffer is cold either way.

Three things that are easy to get wrong and were:

- **The timeout has THREE cases, not two.** Batch collected -> poll at 0 (unchanged). Nothing
  collected but a backlog held -> a SHORT FINITE wait. Both empty -> INFINITE. Zero-timeout retry is
  wrong because the completion thread runs at TIME_CRITICAL: it would hard-spin at priority 15
  against the very workers that have to drain the lane. INFINITE is wrong because the held tasks
  would wait for an unrelated I/O event.
- **The WAIT_TIMEOUT path must drain.** Without it the short wait just wakes, finds nothing, and
  sleeps again with the tasks still held.
- **Both exit paths must flush to the FLOOR, not through PushIO.** K may be permanently buried or
  gone at shutdown; a completion left in the backlog is a task nobody runs and every waiter behind it
  hangs. Correctness at teardown outranks lane placement.

**INSTRUMENTED BECAUSE IT IS NOW THE GLOBAL QUEUE FOR I/O.** `ReadIoBacklogStats()` is unconditional
-- unlike `IoLockStats`, which is behind a define and defined only under `_WIN32`. Counters live in
`IoShared.cpp` so a test reading them links on a platform with no reactor, where they read zero.
**High water is the load-bearing one**; instantaneous depth sampled from another thread is almost
always zero even when the backlog spikes.

First run at K=1, 256 concurrent reads: `highWater=109 pushed=256 declined=241 drains=242`, ending
empty with all 256 correct. A non-zero `declined` is the mechanism WORKING, not a failure. But 241
declines against 256 pushes says K=1 cannot absorb that arrival rate -- which is the number that
makes K=2 the next thing to try. Depth depends on drain speed, so it is indicative, not a measurement.

## FROZEN 5.0: two lanes, K in {0,1,2}, never-park K, adaptive F (9-02)

**`Lane::Normal | Lane::LowLatency` replaces `bool hiPri`.** K is how many workers are bound to
LowLatency; everything else stays on Normal and rides the adaptive floor.

The bool lied in both directions, and both cost time on 9-02. It said PRIORITY, so a loPri task on a
reserved worker read as *deprioritised* when it was **unreachable** -- K never reads its loPri inbox,
inbox work is unstealable, and the pool deadlocked reporting a lost wake. It also reads as
"important", which invites marking bulk work with it and funnelling volume onto K workers while the
floor idles. `enum class`, so `if (lane)` -- which would read as "if it has a lane", true of every
task -- does not compile.

**K never parks, and that spin is the point.** Reservation alone measured p50 5.90 / p99 43.00 us;
reservation plus never-park measured p50 2.00 / p99 6.30. A futex park costs ~5.5 us to get the core
back, which is the entire budget a lane exists to protect. **`max(F, K)`, not `F + K`** -- K does not
grow the floor, and nothing raises F because K exists.

**Parking was never the hazard; being UNREADABLE was.** A parked reserved worker with LowLatency work
waiting gets notified and wakes. An awake one holding work in a queue it will never read does not.
Independent properties, and only the second is a correctness question.

**K is clamped to 2.** Not a limit of the mechanism -- the bitmap reaches 64 -- a policy. Every
reserved thread comes off the floor where throughput lives (a lane ON the floor already measured
15-19x over no lane; reservation buys a flat TAIL, not more work done), and main + timer + one core
per completion thread are already reserved before the pool is sized. K=2 leaves an 8-core box with
three floor workers. Clamped rather than refused, matching the K+F clamp beside it.

**Two adaptive radii on one pool is a new machine, not a knob.** A joint F/K controller is not
revisited until the DAG cancel wake is boring. Adaptive K was already removed once (it never ramped,
shed on quiet, and the policy acted as a ceiling); this is the same lesson written as a rule.

More `Lane::` values only if two reserved classes genuinely fight over the band -- a 6.0 decision
with evidence, not a speculative third enumerator now.

## Migratable fibers are the DEFAULT as of 5.0 (9-02)

`SetMigratableFibers` now defaults to **true**. A suspended fiber resumes on whichever worker is
free, not the one it left.

**Why the flip:** resume-anywhere is what a fiber task library is FOR -- it is the only reason to
have a pool instead of a thread per job. Defaulting to pinned meant every program bought the
migration machinery (address-routed frees, the global epoch participant list, fiber-indexed hazard
cells) and then declined to use it. Migration's costs were already paid; the pin was never a fix.

**The trade is `thread_local` and nothing else.** Pinned mode is what makes a TLS value read before a
suspension point still valid after it. Migratable gives that up SILENTLY -- you get the resuming
worker's copy, with nothing to catch it. An app that keeps state in TLS across a wait, or embeds a
library it cannot audit, wants `SetMigratableFibers(false)` before `Init()`.
*(Spelled `SetFiberMode(FiberMode::Pin)` since 5.0 -- the entry above is left as written, but the
call in it no longer compiles. `FiberLocal<T>` is the other answer, and usually the better one: it
keeps migration and moves the state instead of the policy.)*

Thread-affine cleanup is settled through Fiber's creditor set, one hop per creditor, and costs
nothing for a fiber that never touched affine state -- which today is all of them.

**HONESTY NOTE FOR THE README, AND IT IS IN THERE:** migratable is the LESS-TESTED path. Pinned is
what shipped through 5.0 and has the mileage. It is the default because it is the right shape, not
because it has more hours on it, and flipping to pinned is a useful bisect for anything odd under
load. Not public yet -- 5.0 is a substantially different product from what shipped before it
(everything-is-a-fiber, Lane, K frozen at 2, migratable default), and it wants a real test pass and
at least an epoll backend before any release claim.

## Fiber-local slots, and the park predicate that was not naming the cleanup chain (9-02)

**1. The cleanup chain was drained by the pass but not named by the park predicate.**
`FiberRegistry::Deliver` already notified correctly -- push first, `NotifyHolder` second, workers
only, with a negative-control define. But the three park predicates listed `hasQueuedWork`,
`laneWake`, the hiPri inbox, the loPri inbox and `resumedInboxes` and **not** the holder chain, while
the pass drains it at the top (Thread.cpp:1347).

It worked only because `NotifyHolder` also sets `hasQueuedWork` -- and that flag is EDGE TRIGGERED
and cleared every pass, so the chain's safety rested on winning a race rather than on the queue being
read. That is exactly the argument the predicate itself refuses two clauses earlier for
`resumedInboxes`: *one legal consumer, so parking on a non-empty one is a resource never given back,
not a delay.* All three predicates now name it, so the set the search covers and the set the recheck
asks about are the same set by construction.

**2. Fiber-local storage replaces `thread_local` for fibers.** `Fiber::local[kLocalSlots]`, eight
`void*` slots indexed by a compile-time constant, reached through `TaskScheduler::FiberLocal(slot)`.
One load and an index -- no map, no hash, no lock, no allocation, because it has to be cheap enough
to use where TLS would have been or nobody will.

- **Eight is a memory decision, not a guess at demand.** Every pooled fiber carries the array; at 64
  fibers per worker on 32 workers that is 2048 fibers, so each slot costs 16 KB across the pool.
  Eight is 128 KB against the 64 KB stack each of those fibers already owns.
- **Scrubbed in `ResetForReuse`.** The most forgettable of the reset fields, because nothing in the
  library ever reads a slot -- only the application would see it break, and a stale pointer to a
  still-allocated object reads back fine rather than faulting.
- **The library cannot free what a slot points at**, having no type. Clearing prevents a stale READ,
  not a leak.
- **Off a fiber it returns a per-thread scratch `void*`** rather than asserting: a Native task and a
  bare thread legitimately have none, and library code that may run in either context needs to be
  able to ASK. `HasFiberLocal()` is the explicit test.

`tests/fiber_local_test.cpp` measured 49/64 fibers resuming on a different worker: **FLS 64 held / 0
lost, TLS 0 of 49 migrated fibers still seeing their own value.** The TLS arm is the negative control
and it is load-bearing -- arm 1 passing alone is equally consistent with "nothing migrated". Its
accessor is `noinline` because MSVC caches a thread_local base across an opaque call, which is what
once made migratable_fiber_test report 0 migrations out of 206.

## The death/reaper contract, checked against the code (9-02)

Jay's three requirements for an append-only creditor chain, and where each actually stands.

**1. Unique by worker, not one node per hop -- ALREADY STRUCTURAL.** `creditors` is a bitmask
(`fetch_or` of one bit per worker, `kCreditorWords = 6` -> 384 holders). A worker that steals the
fiber ten times sets the same bit ten times, and `TakeCreditor` clears it before dispatching. There
is no hop log to flood an inbox with, and there could not be one anyway: `cleanupNext` is a SINGLE
pointer, so a fiber cannot be on two chains at once -- surplus deliveries would be silently lost
rather than merely wasteful. Pinned by `fiber_reaper_contract_test`: 10 pickups -> 1 delivery.

**2. Idempotent cleanup on a historical owner -- THE CONTRACT WAS UNWRITTEN.** `ReleaseFn` had two
lines of comment and said nothing about it. Now stated on `SetRelease`, both halves:

- What the REGISTRY guarantees: at most one dispatch per worker per fiber life. A correct hook is
  never asked to be idempotent by the library itself.
- What the HOOK owes: tolerating that it may no longer hold what it owed. By the time a hop lands
  the worker has run arbitrary other work. **If the epoch slot it would clear already reads
  SIZE_MAX, return** -- a hook written as "I am being called, therefore I still own this"
  double-retires, or touches a slot a LATER life is already using. The library cannot enforce this,
  because only the hook knows what it owned.
- Release runs BEFORE the hop advances, deliberately: advancing can recycle, and a recycled fiber is
  a different life.

**3. The list is the reuse fence -- ALREADY STRONGER THAN ASKED.** The requirement was "clear after
the cleanup tasks are queued". The chain does better: it recycles only when `TakeCreditor` returns
SIZE_MAX, which is after the last hop has RUN. The `JLIB_FIBERHOLDER_CTL_FANOUT` control exists
precisely to prove queued != run.

**A CORRECTION WORTH KEEPING.** I claimed `DrainAllForTeardown`'s single forward sweep could strand a
fiber behind itself and called it a bug. It cannot: `TakeCreditor` pops the LOWEST set bit first, so
every hop hands FORWARD and a forward sweep is always sufficient. The multi-sweep stayed anyway, but
relabelled as defence in depth -- the single-pass version is correct only because of an ordering
guarantee that lives in another file and is neither stated nor tested. Change TakeCreditor to pop the
highest bit, or rotate for fairness, and teardown starts leaking silently.

**No `Unregister` is needed for this model** -- bounded unique owners plus idempotent tagged cleanup
is the whole requirement.

**Fiber gets `new`/`delete` stubs**, same reasoning as Task's but deleted OUTRIGHT rather than
defined-and-asserting: Task has a virtual destructor, so the compiler emits a deleting destructor and
requires an accessible `operator delete`; Fiber has no virtuals, so the compile-time form is
available and cannot be reached at runtime to assert. A heap-allocated Fiber has no arena stack, no
poolIndex any table can name, and no epoch slot -- invisible to both the cleanup chain and hazard
reclamation. Placement new is unaffected because every site writes `::new (mem) T(...)`.

## Removed: the cancellable WaitGroup wait. A WaitGroup is not a wait primitive. (9-02)

`TaskScheduler::WaitFor(wg, token)`, `WaitGroup::CancelWaiters` and the `cancellable` vector are
gone. The header had carried its own obituary for weeks -- *"INCOMPLETE. A FIBER THAT PARKS HERE CAN
WAIT FOREVER."*

**Jay's framing, and it is the right one: the IDEA was wrong, not the delivery path.** A WaitGroup is
a CONCURRENCY COUNTER -- N outstanding, a join that ends at zero. Event, the semaphore and the
condition variable own a QUEUE OF WAITERS, which is what cancellation ejects from. A counter owns
none. That is why the old `CancelWaiters` had to promise in capitals that it did not touch `n`: it
could not, because cancelling a wait while the counted work keeps running leaves the count
outstanding and the tasks decrementing into a group nobody is joined to.

So there was nothing coherent to deliver. Adding an `EjectWaitGroup` -- which is what I had been
planning all day -- would have made an incoherent operation *reliable* rather than *correct*.

**What it cost.** `waitgroup_cancel_test` deadlocked 2-13% of runs, and a deadlocked test does not
fail: it spins every worker until something kills it. It was the suite's only recurring red for
weeks, and it burned cores every time.

**Its only user was itself**, and it stayed green by calling `inner.CancelWaiters(...)` by hand on
the line after `scope.Cancel()` -- working around the exact gap it existed to cover. Nothing in the
library, the benches or the samples ever called either function.

**The lesson worth keeping is about the measurement, not the code.** I wanted to characterise the
flake rate before acting. Jay: *"why do you need a measurement, 2% is enough to just fix or get rid
of a feature."* A deadlock is not a number to estimate -- one confirmed instance is the whole
argument, and a rate only tells you how long you can ignore it.

**If a join you can abandon is ever needed:** compose it from a primitive that HAS waiters -- wait on
an Event the last task signals, and cancel the Event. The counter stays a counter.

## A DAG frame loop must Tick on the main thread (2026-09-02)

The reaper made reclamation a queued task and the README started saying "you do not have to call
anything." That is true for most workloads and FALSE FOR TaskDAG, and the docs were one edit away
from dropping the obligation entirely.

Two facts combine:

  * `TaskDAG` retires a node per node per frame, through `EpochManager::RetirePtr`.
  * `RetirePtr` has NO threshold-triggered sweep. Epochs reclaim only when something calls `Tick()`.
    (Hazards differ -- `Retire()` does self-trigger, which is why forgetting `Scan()` only reclaims
    late instead of leaking.)

And the reaper's only trigger is a FIBER DEATH. A DAG need not produce one: `CreateMainNode` REQUIRES
`TaskType::Native` and fatals on `TaskType::Fiber`, and a Native task never binds a fiber to kill.

So the DAG is a rate of retirement with no corresponding rate of reclamation -- the exact shape that
grows without bound. The frame boundary is where it gets fixed, because that is a moment the app has
and the library does not.

WHY THIS IS WORTH WRITING DOWN. The reaper is a real improvement and it is easy to over-read: "the
sweep is automatic now" is the natural summary and it silently drops a case. Anything that retires on
a schedule but does not recycle fibers is in the same position as the DAG, so the question to ask of
a future subsystem is not "does it retire" but "does it produce fiber deaths at the same rate".

## Essentials must check Push's bool (2026-09-02)

`Push` has always returned `bool` and three call sites in the library discarded it. Two of them had
already incremented a WaitGroup:

    RunCounted:    wg.n.fetch_add(1); t->waitGroup = &wg; Push(t);
    ParallelFor:   (same shape, per lane)
    QueueReclaim:  latches g_reclaimQueued, then pushes the sweep

A dropped push in the first two does not lose a task -- it HANGS EVERY WaitFor ON THAT GROUP
FOREVER, because the decrement lives in the completion of a task that was never queued. In the third
it stops reclamation for the rest of the run, because the flag says a sweep is coming and none is.
All three now undo their accounting on a false and let the natural trigger retry.

HONEST STATUS: currently unreachable through a non-null task. `PushTarget` returns false only for
null -- the inboxes are unbounded intrusive Vyukov MPSC queues whose push() cannot refuse, and the
one fallible path (PushLaneIntake, moodycamel) already falls through to the floor on failure. So
these are defensive today.

WORTH WRITING ANYWAY. The cost is one cold predicted branch; the failure it prevents is a hang with
no diagnostic. And the reachability is a property of TODAY's queues, not of the API -- the moment
anything acquires a bounded queue or a teardown refusal, every one of these sites becomes live. A
bool nobody reads is indistinguishable from a void return, which is how it got discarded three times.

## Fibers-only: coroutines removed, and the tiny-stack path wired at last (2026-09-02)

V5 makes this a FIBERS-ONLY runtime, I/O included, so the C++20 coroutine layer is gone to
extras/abandoned/ and `TaskType` is `{Native, Fiber}`. The runtime is pure C++17: no optional header,
no split standard in the build, no `JLIBSCHED_COROUTINES`.

WHAT IT COST, stated plainly rather than buried: the I/O test surface was written entirely on
`co_await`, so io_socket_test (1003 lines -- accept/connect/send/recv/vectored/peer-close/acceptor
pool/Stop drain/cancellation/deadlines/IoStream chaining, and the ONLY exercise of the Linux io_uring
backend) went with it. What remains is io_c17_test at ~150 lines. Until that suite is ported to the
fiber path, `IoReactor::IsAvailable()` on Linux is asserted by nothing.

### The tiny stack was built and never used

`StackClass::Tiny` -- 2 usable pages, a 3-page region -- was created for I/O continuations. All of it
existed: sizes, the guard-paged arena, a per-worker ThreadLocalCache per class, batch refill through
`GlobalFiberPool::StealInto`, recycle back into the owning class's cache, and a `SetFiberBudget`
knob. **Nothing ever asked for it.** `stackClass` was reachable only by assigning the field after
creation, `StackClass` appeared nowhere in TaskScheduler.h, and the reactor never touched it -- so
every parked I/O continuation held a 60 KB Standard stack and the tiny pool was provisioned, cached
and never drawn from. The budget number was unobservable.

Fixed both ends:
  * `StackClass` is a trailing defaulted parameter on all five Create* entry points.
  * The reactor STAMPS `Tiny` where it takes the resume task -- one place per backend, since every
    Submit overload funnels through it. Only over `Standard`: an explicit `Deep` survives, because a
    caller asking for Deep is saying it recurses and downgrading that is a guard-page fault rather
    than a slowdown.

Asserted in io_c17_test with the class read BEFORE Submit (so it tests what Submit did, not what the
constructor defaults to) plus a Deep control. Control verified RED against an unconditional stamp.

### Budget: 64 tiny per reserved worker, 128 at K=2

SIZED FOR A GAME, NOT A SERVER, and that is the whole justification. IoReactor.h argued a
fiber-backed reactor was unaffordable because "a reactor's steady state IS thousands of parked
operations" -- a server's premise, never this project's. The real shape is a UDP socket or two and
some file-streaming connections: tens in flight. The 500x per-operation figure was right; it was
multiplied by a concurrency two orders of magnitude too large.

The model that makes 128 generous is ONE FIBER PER SOCKET/FILE, LOOPING ON COMPLETIONS -- not one per
packet or per 4K read. That model is wrong at 16 and still wrong at 4096, so if 128 ever waits, dump
the spawn sites before raising the number.

Errs high deliberately: over-budget costs ~1.5 MB, under-budget is a SPIN on acquisition that
deadlocks if the fiber holders are waiting on tasks that cannot get one (4-per-worker did exactly
that to dag_cancel_test).

STILL UNMEASURED: whether 2 pages is enough for a real continuation. It is the only page-denominated
class, so it is 8 KB usable on x64 and 32 KB on a 16 KB-page platform, and an overflow is a
guard-page fault rather than a spin. A stack-watermark probe would settle it.

### FOUND BY THE CONTROL: StackClass::Deep is unprovisioned by default, and asking for it HANGS

`SetFiberBudget(normalPerComputeWorker = 64, tinyPerKWorker = 64, deepPerComputeWorker = 0)`.
Deep is **zero**. A task created with `StackClass::Deep` under the default budget does not fail, does
not fall back, and does not run: `AcquireFiber` finds no deep fiber, the task is requeued, and it
spins forever. io_tiny_stack_test's control hit it on the first run -- the continuation simply never
executed.

TWO THINGS MAKE IT WORSE THAN A MISSING RESOURCE:

  * THE EXHAUSTION WARNING NAMES THE WRONG CLASS. It fired and reported the STANDARD budget
    ("64 standard per worker x 29 workers = 1856 total"), so the message points at a lever that is
    already generous while the actual shortage -- zero deep fibers -- goes unmentioned. Someone
    debugging this would raise the standard budget and see no change.
  * THE NEW StackClass PARAMETER MAKES IT REACHABLE. Deep used to be hard to ask for (you had to
    poke `task->stackClass` after creation). It is now a normal argument on all five Create* entry
    points, so the first person to pass `StackClass::Deep` gets a silent hang.

NOT FIXED HERE, deliberately -- it is a separate decision with at least three defensible answers:
provision a small deep count by default; fall back to Standard when the asked-for class is empty
(safe for Deep, NOT safe for Tiny, which would silently undo the I/O footprint win); or refuse at
CreateTask and return nullptr, which is loud and matches the slab's "grow rather than fail" lesson
only partly. Worth deciding before anyone ships against the new parameter.

## OPEN: a reserved worker that steals SUSPENDABLE work loses it forever (2026-09-02)

Reserved-worker stealing (added 9-02, so K does not waste two cores while the lane is quiet) is sound
for work that runs to completion and UNSOUND for work that suspends:

  1. a reserved worker steals a `Lane::Normal` fiber task
  2. the fiber parks on I/O
  3. the completion wakes it; the resumption is published on the worker that was running it
  4. that worker is in the reserved band, WHICH NEVER READS A LOPRI DEQUE
  5. nobody may take it, ever

MEASURED: io_socket_test after the fiber port passes 1 run in 5 with stealing on, 5 in 5 with it off.
The watchdog dump names it exactly -- every worker idle, advertised queues = 0, and q0/q1 (the
reserved band at K=2) each holding `deque(hi/lo) 0/1`.

THIS IS THE 8-31 REACTOR BREAK'S INVARIANT, REACHED FROM THE OTHER END. That one was PLACEMENT --
never push loPri at K -- and is guarded by reserved_lopri_placement_test. This is RESUMPTION, which
that file structurally cannot see: it disables stealing itself, and says so, because it measures
where tasks RAN and cannot tell placement from theft.

THE GENERALISATION WORTH KEEPING: every "K never reads X" invariant has TWO entrances -- where work
is PUSHED and where work is RESUMED. Guarding the push side is what we did; it leaves the other open.

NOT FIXED, because the fix is a policy choice with three defensible answers:
  * reserved workers decline work that can suspend (simplest; costs the steal win on fiber tasks)
  * a resumed task is never published to a reserved worker (narrowest; touches the resume path)
  * K reads its own loPri deque for work IT stole (keeps the win; weakens an invariant that is
    currently absolute and easy to reason about)
Worked around in tests/io_socket_test.cpp with a comment naming this note.
