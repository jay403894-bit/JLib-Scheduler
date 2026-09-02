# JLib::Scheduler — Architecture

**A fibers-only execution substrate in pure C++17.** No coroutines, no optional language tier, no
idle policy to select. A task that waits suspends a *fiber*; the worker underneath it keeps running.

This document describes the **shape** of the runtime — what the pieces are and why they are separate.
It deliberately contains almost no measurements: numbers age against hardware and belong in
`design/NOTES.md`, where each one is dated and attached to the experiment that produced it. When this
document says "measured", it means the number lives there.

---

## 1. The substrate

**Hand-rolled context switching.** Fiber switching is assembly per architecture (x86-64, ARM64,
Windows-on-ARM), not `ucontext` or Fibers API. The switch saves what the platform ABI requires and
nothing more.

**Call-graph transparency.** A fiber's stack is a real stack. A task that calls into a library, which
calls into another, which waits, suspends the whole call graph and resumes it intact — no `async`
colouring, no rewriting of the functions in between. This is the property coroutines could not
provide and the reason the runtime is fibers-only.

**Three task types became two.** `TaskType::Native` runs directly on the worker's OS stack and **must
not suspend**; `TaskType::Fiber` owns a stack and may. `Coroutine` was removed in 5.0 — a fiber and a
coroutine were two answers to one question, and carrying both meant every suspension point in the
scheduler had two shapes to be correct in.

---

## 2. Topology: two bands

Workers are partitioned by index into two bands sharing one packed 64-bit word.

**K — the reserved lane.** The first `K` workers (clamped at 2) serve `Lane::LowLatency` work.
They never park, so a completion reaches a running thread rather than paying a kernel wake.

**The invariant is about the INBOX, not about ordinary work.** A reserved worker does not read its
own ordinary inbox — an inbox has exactly one legal consumer, so work placed there that its owner is
forbidden to read is not slow, it is *unreachable*. Nothing may push ordinary work at K, and that is
an invariant with a test of its own.

**But K does run ordinary work, by STEALING it.** When the lane has been quiet for long enough, a
reserved worker takes from other workers' deques — because a deque exists to be stolen from, and a
band that never steals is two cores the pool paid for and does not get. Taking work is always safe:
the thief is the one running it, so nothing can be stranded by the act of claiming it.

**THE TWO FIBER MODES DO NOT HAVE THE SAME INVARIANT.** They are different promises, not two
implementations of one, and stealing is safe under each for a *different* reason:

| | `Migrate` | `Pin` |
|---|---|---|
| the promise | resumes **promptly**, on whichever worker is free | resumes on **its home worker**, whichever that costs |
| what it gives up | `thread_local` across a suspend | promptness — it waits for one specific worker |
| where a resumption goes | ordinary placement, which masks `[0,K)` | that worker's **resume inbox**, never a deque |
| why K may steal | the resumption is on the floor, reachable by anyone | **stealing is not resumes** — a steal takes fresh work off a deque; a resume is delivered to one consumer that reads it |

The `Pin` row is the one worth reading twice. A reserved worker stealing pinned work cannot strand
its resumption, because the resume never travels by the route stealing uses — the two paths do not
meet. That is why the fiber mode does not gate stealing, and a gate added on the opposite assumption
was removed once the drain was actually read.

A reserved worker therefore reads its lane inbox, its resume inbox, and other workers' deques. It
never reads its own ordinary inbox, and nothing ever puts ordinary work there.

**F — the awake floor.** A *bounded, adaptive* number of ordinary workers held unparked, grown by a
controller under load and shed when the burst ends. This is the bounded version of what a pool-wide
"never sleep" policy used to do: the same store-instead-of-futex wake path, for a few workers, given
back when it is not earning.

**The bands are one word.** K and F were separate mechanisms with separate ideas of where the
boundary was, which produced a family of bugs that were all correct at K=0. One atomic word, read
once per decision.

---

## 3. Dispatch and the permit machine

**A 64-bit atomic bitmap of awake workers**, so placement is a bit scan rather than a walk. A pusher
prefers a worker that is already awake, which costs the pusher nothing — an awake worker never
advertises that it is going to sleep, so the push takes a skip instead of a notify.

**The permit machine** is the park/wake handshake: a small lock-free state machine per worker
(`EMPTY / NOTIFIED / GOING_TO_SLEEP / YIELD`) with a latched permit, so a wake that arrives during
the commit to sleep is consumed rather than lost. `YIELD` is a fourth state rather than a flag: a
worker that is runnable but momentarily off-core is neither awake nor parked, and collapsing it into
either loses the distinction that makes re-aiming a push worthwhile.

**Park reads the queue, never the hint.** The hint is an optimisation; the queue is the truth. Every
lost-wake bug in this project's history came from a park predicate that trusted a summary.

---

## 4. Work distribution: two queue kinds, on purpose

**Inboxes are unstealable staging.** One legal consumer — the owning worker. Intrusive MPSC (Vyukov),
so a push is a store and a link. Work arrives here and is republished in batches into the deque.

**Deques are stealable execution queues.** Chase–Lev, owner pushes and pops one end, thieves take the
other. A deque exists *so that other threads can steal from it*; an inbox exists so that they cannot.

**Steal hints.** A thief that probes every victim's deque endpoint finds almost nothing and burns
cache lines doing it. A word of hint bits, published by the owner and read by thieves, turns a scan
into a test — and it is also the park gate: nothing advertised anywhere means nothing to steal.

**The lane intake.** A shared MPMC (moodycamel) that lets any producer hand lane work to the reserved
band without choosing a worker, so a burst does not have to be bound to a target at push time.

**One placement path.** Everything — new work and resumptions alike — goes through `PushTarget`,
which masks the reserved band, marks queued work and notifies. A second placement path that skipped
those rules is exactly how work became unreachable.

---

## 5. Memory

**Task allocator.** Size-classed slabs for `Task` objects, freeing routed by *address* so the caller
never has to remember which class it came from. It **grows on exhaustion rather than failing** — the
lesson that took it from a 176 MB pre-sized pool to a 4 MB growing one while removing a failure mode
rather than papering over it.

**Fiber stack arena.** One reservation of address space; each stack is a region committed on
allocation with its lowest page left unbacked as a **guard page**, so overflow faults instead of
corrupting a neighbour. Usable depth is therefore one page less than the region.

**Three stack classes**, because one size is either wasteful or unsafe:

| class | for |
|---|---|
| `Tiny` | I/O continuations — wake, read a completion, finish. Page-denominated (2 usable pages). |
| `Standard` | everything else. |
| `Deep` | work that recurses past a standard stack. Opt-in, provisioned with a small floor. |

**Stacks are never returned to the OS in steady state.** They circulate through the pools — mapped,
committed and cache-warm. Nothing calls `VirtualFree` or `munmap` on a stack during normal operation.

### Thread-local fiber caches

**Every worker holds its own cache of fibers, one per stack class.** Acquiring a fiber is a pop from
a structure only that worker touches; releasing one is a push back into the cache for the class the
fiber belongs to. The global pool is consulted only on a miss.

This is what keeps fiber acquisition off the contended path. A single global free-list would put
every worker on one cache line for an operation that happens on *every task that can suspend* — the
allocator would become the bottleneck the work-stealing exists to avoid.

**Refill is a batch, not an item.** A cache that runs dry takes a run of fibers from the global pool
in one operation, so the cost of touching shared state is amortised across many acquisitions rather
than paid per fiber. The batch path writes straight into the cache's storage; an earlier version
returned a `std::vector` and paid a heap allocation per refill.

**The class travels with the fiber.** A released fiber goes back to the cache for *its own* class, so
a `Tiny` stack cannot drift into the `Standard` pool and quietly shrink what a standard task gets.
The per-class split is what makes three stack sizes safe to mix in one pool.

**Caches are primed at startup** rather than filled lazily, so the first task on a cold worker does
not pay for the pool.

---

## 6. Safe Memory Reclamation

A lock-free structure cannot `free()` while another core might still be reading. Two mechanisms,
chosen by what the reader is:

**Hazard pointers — for readers with no stable identity, and for reads that span a suspend.** A
reader publishes the address it is about to dereference into a per-fiber cell; retirement checks the
cells before freeing. This is the mechanism that works when the reader may migrate mid-traversal.

**Epochs — for bulk retirement by readers that do have a stable identity.** A thread-keyed slot, a
global epoch, and thread-local retire bags. Nothing may suspend inside an `EpochGuard`; that is the
contract, and it is what makes the cheap mechanism sound. Bags orphaned by a thread exiting are
handed to a leaked global store rather than dropped.

**The reaper — a function of the fiber registry, and a *task*, not a thread.**

On fiber death the registry queues **one ordinary task**, rate-limited to a single sweep in flight,
that performs `Tick()` → `Scan()` → user-registered deletes, in that order. The deletes go last
because a user deleter may itself retire, and running them first would leave those retirements for
the following pass.

Three designs were tried and the first two were wrong:

| | why it lost |
|---|---|
| worker-inline | a p99 killer — it walks every participant on a thread that was running your frame, at a moment nobody chose |
| app-driven `Tick()` | better tail, but a library cannot require a loop its embedder may not have, and forgetting it leaks silently |
| **a queued task** | displaces nothing; it waits its turn behind work that was already there |

The difference between the first and the third is not *who* runs it — a task runs on a worker too —
it is **when**. There is no reaper thread and nothing polls.

**The exception the design owes you:** `TaskDAG` retires a node per node per frame, epoch retirement
has no threshold-triggered sweep of its own, and a DAG need not produce a single fiber death — main
nodes are *required* to be `Native`. That is a rate of retirement with no rate of reclamation, so a
DAG frame loop must call `Tick()` itself.

---

## 7. The fiber registry

Fibers are identified by slot, not by pointer, and the registry owns everything keyed on that
identity:

**`FiberLocal<T>` and `FlsAlloc`.** Fiber-local storage. `thread_local` does not survive a migration
and fails *silently* — you get the resuming worker's copy, a plausible value rather than a crash — so
a migratable runtime needs storage keyed to the fiber. Slots are scrubbed on recycle, because a
pooled fiber carrying a previous occupant's pointer reads perfectly well and corrupts quietly.

**Lease / creditor tracking.** A fiber that acquired thread-affine state owes a cleanup hop to each
creditor. The set is a bitmask on the fiber, delivery is one visit per creditor, and the nodes are
intrusive — a death path may not allocate.

**Debt lists.** Objects a fiber must have released before its stack is reused, with a type-erased
deleter each, so `new`, a custom allocator and a pool free can all be discharged by the same sweep.

**ABA-proofing.** Slots are reused; a stale index must never resolve to a live fiber. Generation
counters on the identity, and a returned batch clears its local epoch so a recycled fiber cannot be
mistaken for a participant.

---

## 8. Fiber mode: Migrate or Pin

**This is a choice of INVARIANT, not a tuning knob.** Each mode promises something the other does not,
and the thing it gives up is the other one's promise:

- **`Migrate`** (default) — *a suspended fiber resumes promptly, on whichever worker is free.* The
  runtime does **not** promise the same worker, so `thread_local` written before a suspension point
  is not the same object after it. It does not crash; you get the resuming worker's copy.
- **`Pin`** — *a suspended fiber resumes on the worker it was bound to, and nowhere else.* That makes
  `thread_local` valid across a wait, and gives up promptness: if the home worker is busy the fiber
  waits for **that** worker while others sit free. This is marl's contract.

Migration is the better default because the thing it gives up has a replacement and the thing it buys
does not — `FiberLocal<T>` moves the *state* off the thread, whereas nothing recovers a core you are
declining to use. Take `Pin` when the state crossing the wait belongs to a library you cannot audit,
which is the one case where moving the state is not available to you.

**In pinned mode the resumption goes to the home worker's resume inbox**, never to a deque — which is
why the reserved band may steal in either mode. A reserved worker *must* read its resume inbox
regardless: an I/O completion **is** a fiber returning from an await, so the band could not do its
primary job otherwise.

---

## 9. Synchronization

Fiber-aware primitives that **suspend the fiber and release the worker**, rather than blocking the
thread: `SchedulerMutex`, `SchedulerSemaphore` (with `ScopedPermit`), `SchedulerConditionVariable`,
`Event`, `WaitGroup`. A raw `std::mutex` inside a task blocks a worker and is a bug.

`Event` uses a perfect-hash table keyed by name, with waiters indexed from the waiter side. A
`WaitGroup` is a **concurrency counter**, not a wait primitive — it has no waiter queue, which is why
it is not cancellable and why cancellation belongs on the primitives that own one.

A bare thread that blocks on the scheduler **helps** — it runs stolen work while it waits — with the
exception that a thread owning a `SchedulerMutex` must not, or it can deadlock against a task that
wants the same lock.

---

## 10. Cancellation and time

**`CancelScope` / `CancelToken`** with parent chains: cancelling a scope reaches its children. Tokens
are generation-tagged so a recycled scope cannot cancel work that outlived it.

**Cancellation reaches dispatched work**, not merely queued work — a token is stamped on the task, so
a scope can end something already running.

**In-flight I/O is different and the API says so.** Cancelling a scope does not by itself end a
posted operation: the kernel holds a buffer. `IoReactor::RequestCancel` asks, and the operation ends
through its *completion* — which is the only correct shape, because the memory cannot be released
until the kernel is finished with it.

**Timer wheel** — hierarchical, backing `Deadline` and periodic work, with occupancy bitmaps so an
empty level costs nothing to skip.

---

## 11. Parallel algorithms

**`ParallelFor` has no calibrated constant, and its division is decided by stealing.** Work is handed
out through an atomic slice-stealing cursor: idle workers take slices, so how the range is split is
decided by *who is free* rather than by a guess about how long an item takes. There is no tuned
threshold to get wrong on hardware it has never seen — the old body-probe-plus-threshold design was
removed in 1.4 for exactly that reason.

**One estimate survives, and it decides FAN-OUT WIDTH rather than the split.** `SetMeasuredWidth`
(on by default) times the first chunk on the calling thread and picks a width of `sqrt(W/c)`, where
`c` is the wake cost — the *k* minimising `W/k + k·c`. It exists to fill a missing middle: without
it, fan-out has two states, serial or the whole pool, chosen by an iteration count that never looks
at the body. Measured, that meant trivial work at N=256 recruiting 23 workers for ~2 µs of work,
while heavy work at the same N was refused outright.

The probe is close to free — the first chunk is work the range must do anyway, so it costs two clock
reads — but it is **a lower bound by design, and it does not always hit.** A back-loaded body makes
the first chunk unrepresentative; so does measuring on a caller running at single-core boost before
the rest of the pool spins up and the clock drops. Range recruitment corrects upward as expensive
leaves complete, so the probe picks a defensible start rather than a final answer — and how quickly
recruitment catches up is a real source of run-to-run variance in wide, uniform ranges.

`SetMeasuredWidth(false)` restores the older behaviour exactly.

**`TaskDAG`** — dependency graphs with **AND/OR gates** (`CreateGate`, `TaskNode::LogicType`). A gate
carries no task: when its trigger fires it propagates instantly rather than scheduling work, so gates
compose into arbitrary boolean expressions — `(A && B) || C`. **External nodes** let a graph wait on
something outside it (`CreateExternalNode` / `SignalExternal`), which is how I/O or another thread
joins a frame. Node completion is defined as "the node's function returned". Any task whose function can return before its work is done would fire dependents early,
which is why a fiber node is the correct way to suspend inside a graph: `ContextSwitch` preserves the
wrapper's frame, so the function returns only on real completion.

**Main-thread nodes** are supported and required to be `Native`, for game and simulation shapes where
some work must run on the thread that owns a device or a window.

---

## 12. I/O

**Completion-first.** `Submit*` takes a buffer, a request, a result and a **resume task**, and returns
`true` when the answer is already final — meaning the caller must not suspend and still owns the
resume task. Everything else is a completion that pushes the task.

**Backends:** IOCP on Windows, io_uring on Linux. epoll is the intended Android path. macOS is
supported for the jobs system only.

**The reactor is C++17.** It was paired with a `co_await` awaiter until 5.0; on a fibers-only runtime
the wrapper is a `WaitGroup` the completion decrements, which needs no language feature because a
fiber already knows how to suspend.

**Completions are stamped `StackClass::Tiny`** at submit — one place per backend, since every
overload funnels through it — and only over `Standard`, so an explicit `Deep` survives.

**Enabling the reactor reserves K workers**, and the design states that cost rather than hiding it.

---

## 13. State machines

Four lock-free state machines, each with a model:

- **Fiber** — `READY / RUNNING / WANTS_YIELD / WANTS_SUSPEND / SUSPEND_SIGNALED / SUSPENDED / DEAD`.
  `WANTS_*` exist because a context is not safe to re-queue until it has actually been *saved*, and
  `SUSPEND_SIGNALED` is the race made explicit: a `Resume` that arrives during `WANTS_SUSPEND` must
  wake the fiber rather than let it park on a signal already delivered. The CAS on the suspend commit
  is what makes that decision atomic.
- **Thread** — the worker pass: which queue is consulted in which order, and what makes a pass
  unproductive.
- **Pool permit** — the park/wake handshake described in §3.
- **Adaptive F controller** — growth on crowding, shedding on quiet, with the shed collapsing to the
  base rather than to zero.

---

## 14. Formal verification

The concurrency is **model-checked**, not argued. Twelve GenMC models live in `tests/verify/`:

`deque_model`, `deque_grow_model`, `mpsc_model`, `event_model`, `event_table_model`,
`sleepwake_model`, `sleepwake_permit_model`, `workerpass_model`, `workerspin_model`,
`yieldstate_model`, `fiberwait_model`, `counted_epoch_model`.

These are not documentation. The Chase–Lev `pop_bottom` `seq_cst` fence is *proven* required — a
model with it removed is red. Several carry deliberate negative-control builds (`-DPLACE_ON_RESERVED`,
`-DTARGET_YIELDED`) so that "the model is green" can be distinguished from "the model cannot fail".

**A model proves a model.** Where an invariant depends on this codebase reading its own state — which
worker may drain which queue — the model explains and a runtime test locks it, because no model reads
`PickNextWorker`.

---

## 15. Platforms

| | |
|---|---|
| Windows | x64 and ARM64, full runtime with IOCP |
| Linux | x64 and ARM64, full runtime with io_uring |
| Android | jobs system; epoll reactor is the intended path |
| macOS | **jobs system only** — kqueue is not tested and is not claimed |

---

## 16. Contracts the caller owes

Short list, because each of these is a silent failure rather than a loud one:

1. **`TaskType::Native` must not suspend.** There is nothing to switch away from; it fail-fasts with
   no message.
2. **Use the fiber-aware primitives inside a task.** A raw `std::mutex` blocks a worker.
3. **Nothing may suspend inside an `EpochGuard`.** Use hazards for reads that span a wait.
4. **A `TaskDAG` frame loop must `Tick()`.** See §6.
5. **`IoRequest` and `IoResult` must outlive the operation.** The kernel writes into them after the
   call returns.
6. **Cancelling a scope does not cancel posted I/O** — `RequestCancel` does.

---

*Measurements, dated experiments, and the reasoning behind decisions that were reversed live in
`design/NOTES.md`. This document is the shape; that one is the history.*
