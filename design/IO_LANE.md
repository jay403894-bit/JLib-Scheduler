# The I/O lane

**Status: DESIGN, NOT BUILT.** 2026-09-01. The reactor it replaces is preserved on branch
`io-reactor` at tag `io-reactor-preserved` and is off the shipping surface.

Nothing in this document has been measured. Where a number appears it is either from the current
codebase (cited) or explicitly labelled as an assumption.

---

## 1. What this is

**The library is two schedulers sharing one park machine.** An async I/O scheduler and a fiber task
scheduler, the first opted into from the second. They share the permit word, the YIELD state and the
wake protocol. They share nothing else — not queues, not priorities, not placement.

That is the sentence the rest of this follows from, and it is why the existing verify files keep
needing "this file is about the loPri queue" as a caveat: they are modelling one of the two.

### The naming was wrong and it hid this

`hiPri` never meant *more important*. It meant *the lane*. F is the priority in the jobs system and
it is the only one. Renaming `hiPri*` to `io*` makes the invariant shipped on 2026-08-31
self-describing:

| today | after |
|---|---|
| "K never reads loPri" — has to be memorised | "**the I/O lane never reads the task inbox**" — obvious on sight |

**NOT ALL WORKERS ARE EQUAL is deliberate here**, not an artefact. A K thread is a subcontractor of
the reactor: it dispatches completions and does nothing else. It does not steal, is not a steal
target, and takes no part in placement.

---

## 2. Roles

```
  OS completion source (IOCP / io_uring / epoll)
        |
        |  PushIO  — private, round-robin, may return false
        v
  K[0..n)   dispatch threads, 1-3 (2 is probably right: one always on duty)
        |
        |  pop own IOMPSCQueue -> borrow fiber -> run handler -> return fiber
        v
  handler completes, or issues the next operation and returns
```

**The reactor** owns the backlog. It pops a completion, tries `PushIO` round-robin, and on `false`
parks the item in a plain `std::queue` — no mutex, because that queue's only producer and only
consumer are the same thread. It drains the backlog **before** pulling anything new.

**K threads** pop their own queue, borrow a fiber, run the handler, return the fiber. One to three of
them; two means one is always on duty while the other is between re-aims.

**F workers** never see any of this. The I/O lane never reads the task inbox and F never reads an I/O
queue.

---

## 3. Decisions

### 3.1 `PushIO` is separate, private, and `friend`-accessible

Rejected: a `bool io` default parameter on the normal `Push`. **The scheduler's push path is not
thread-safe in the way the queue is** — the queue is MPSC and fine, the placement code around it is
not. A reactor sitting in a retry loop inside the shared push would obstruct an application thread
pushing to F.

So `PushIO` is its own function, `private`, with the reactor a `friend`. The application cannot reach
it and cannot get it wrong; the path stays lock-free with no shared placement state.

### 3.2 `IOMPSCQueue` is a separate type, and `bool push` is why

A bounded queue whose push can **fail** is a different *contract*, not a configuration of the
existing one. Parameterising `TaskMPSCQueue` would put a new failure edge inside the path
`mpsc_model.c` already proves.

`TaskMPSCQueue::push` today is `void push(Task*)` — infallible and unbounded.

> **The copy needs its own model.** Full/empty and a failing producer are new states.
> `mpsc_model.c` says nothing about them.

**Why a fallible push at all**: this is the lesson the 2026-08-31 session kept re-teaching. The loPri
strand existed because push always succeeded, the queue was unbounded, and a self-healing net quietly
rehomed the consequence — so no caller ever had to own a policy. A push that can return `false`
forces one. Precedent exists: `TaskDeque::push_bottom_batch` refuses and makes its caller requeue.

### 3.3 One fiber pool, bisected — not two pools

A reserved slice of the existing `GlobalFiberPool` for the I/O lane, rather than a second pool.

**This is what makes it safe.** Two pools would mint colliding `poolIndex` values, and exactly two
sites index flat arrays by that:

- `Event::EnsureTable()` — `slots = new std::atomic<Task*>[TotalCount()]`
- `Hazard.cpp:129` — sizes by `TotalCount()`

Two pools means I/O fiber `poolIndex 5` and compute fiber `poolIndex 5` write **the same Event waiter
slot**. Silent corruption, not an error. One bisected pool keeps a single index space and the problem
does not exist.

### 3.4 Fiber per completion, not per operation

The budget is bounded by **concurrent handlers**, not open connections:

| | per operation | **per completion** |
|---|---|---|
| budget | max in-flight ops — 1000 conns × 64 KB = **64 MB committed** | ~K thread count — **tens of fibers** |
| routing | pinned; assign once at operation start | free round-robin |

`GlobalFiberPool::kStandardStackSize` is 64 KB and the arena is one fixed allocation at `StartPool`,
so per-operation would put a hard pre-`Init` ceiling on simultaneous connections. Per-completion does
not.

**The cost is handler style.** A handler needing a second I/O op stores state on the connection and
returns, rather than awaiting inline. State machine, not linear async. That is also *why*
round-robin is legal: no continuation state lives in a fiber between completions.

### 3.5 Fibers, not coroutines

Keeps the I/O path **C++17**, matching the project's stated constraint. It also dissolves two hazards
the coroutine version has: a frame resumed on two threads, and a handler observing a different thread
after every await.

> **Correction on record.** This document's author claimed twice that the lane had a structural
> ceiling because "completions resume pinned fibers". The *preserved* reactor is coroutines —
> `IoAsync.h`: "`co_await` for asynchronous I/O. C++20 — and the ONLY C++20 in the reactor", with
> `await_suspend(std::coroutine_handle<P>)`. `resumedInboxes` is the **fiber** path and a different
> mechanism. The conclusion drawn from that confusion — that an ordered MPMC FIFO was required — was
> wrong, and this design needs no such queue.

### 3.6 `IsThreadAvailable`, and it reuses the fourth state

Push only to a K thread that is actually on a core. That question is already answered by the permit
word shipped on 2026-08-31:

| state | on core? | push here? |
|---|---|---|
| `WS_EMPTY` | yes, scanning | **yes** |
| `WS_NOTIFIED` | yes, permit latched | **yes** |
| `WS_YIELD` | no — stepped off | prefer not; safe but a quantum late |
| `WS_PARKED` | no — asleep | costs a wake |

No new mechanism. `WS_YIELD` exists precisely because `WS_EMPTY` was claiming "on core, no syscall
needed" while a thread was off it.


### 3.7 The adaptive floor, and what the I/O lane does to it

The compute side is not a fixed set of hot cores. **F is elastic**: it grows under a wave and sheds
when the wave drains, which is the mechanism the I/O lane sits beside and must not disturb.

```
  [0, K)          I/O lane          RESERVED   fixed, opt-in, never grows or sheds
  [K, K+F)        awake floor       ELASTIC    grows under load, collapses on idle
  [K+F, n)        parkable          the rest
```

**K is reserved, F is a budget.** That is the whole distinction, and it is why they can share a
machine without a controller arbitrating between them:

| | K (I/O lane) | F (awake floor) |
|---|---|---|
| size | fixed at opt-in, 1–3 | `Fbase` at rest, grows to `Fmax` under load |
| changes at runtime | no | yes — `NoteFloorCrowding` grows, `CollapseAwakeFloorToBase` sheds |
| who decides | the application, once | the workload, continuously |

As shipped 2026-08-31:

```
Fbase = n <= 8 ? 1 : 2
Fmax  = clamp(n - 2, Fbase, 16)     then clamped to n - K - 2
```

**The floor already starts after K and already cannot grow into it.** `floorBase = pbands.k`, and the
growth ceiling is `structural = n - kNow - 2` — live K, re-read every time. So a larger I/O lane
narrows the compute floor's ceiling automatically. No new accounting, and nothing to keep in step.

The `- 2` is the other half: one or two logical CPUs stay outside the live floor so the application
thread is not sharing a fully packed box. **Peak is a budget, not a target** — an unbounded peak is
NoSleep for the length of the grow-hold, and every parkable worker becomes a spinner until the
collapse wins.

### Wide is the other lever, and the I/O lane does not touch it

`Wide` wakes the crowd once for one wave, keeps F at base, and everyone parks after — measured at 31
participants with `PEAK 2`. Growth keeps cores hot for the *next* push. They are separate mechanisms
and the I/O lane interacts with neither: it never grows, never sheds, and is never a steal target.

### An open consequence, stated rather than assumed

**If K threads live OUTSIDE the worker array** — as §2 describes them, subcontractors of the reactor
rather than pool workers — then `bands.k` is 0 for the compute scheduler and **the reserved band
disappears from it entirely**:

- `PickNextWorker`'s `kResv` mask has nothing to mask
- `reservedForHiPri` gates in `Worker()` are dead code
- `hiPriInboxes` becomes per-K-thread rather than per-worker

That would delete most of the machinery repaired on 2026-08-31 — the placement mask, the drain gates,
the park-predicate terms — because the invariant they enforce ("ordinary work never reaches a
reserved worker") becomes true by construction when no worker is reserved.

**This is a consequence to confirm, not a decision recorded.** It is a large deletion, it changes the
thread budget (K threads become additional cores rather than a slice of the pool, accounted like the
timer thread), and the reserved band was bought with a measurement on the *old* design — reservation
buys a flat completion tail worth 175x at 400 µs grain. Whether that number survives the lane owning
its own threads is unknown and untested.

---

## 4. Invariants

1. **The I/O lane never reads the task inbox.** Shipped 2026-08-31 as "K never reads loPri"; every
   reader in `Thread.cpp` is gated, and there is no self-healing net — a violation is reported and
   the task is lost, deliberately.
2. **At most one outstanding operation per connection.** Round-robin means two completions for the
   same socket can land on different K threads and run concurrently, which breaks stream ordering.
   `PushIO` cannot fix this; by then they are on separate threads. Normally free — the next `recv`
   is not issued until the current completes — but it is a **reactor contract**, not a happy
   accident.
3. **The backlog drains before new completions are pulled.** Otherwise the reactor keeps accepting
   while its own queue grows: unbounded memory, and the oldest completions served last, inverting
   the ordering the bounded mailbox exists to protect.
4. **A fiber returns to the free list only after the reactor guarantees no further completion.**
   Otherwise the next acquire hands out the same `Fiber*` and a stale completion resumes the wrong
   frame — ABA, same pointer, different owner.
5. **No Native handlers on this path.** A Native handler runs to completion on the dispatch thread
   and *is* a stolen consumer. Reject, do not tolerate: code that survives a violation is defending
   against it, not permitting it.

---

## 5. Open questions

**Sleep or spin for the K threads.** A wake is ~3–4 µs. Two threads means one is usually on duty
while the other re-aims, which may make the wake cost irrelevant — or the lane may need a spin.
**Unmeasured.** The measurement is completions/sec against kernel wakes, the same shape as the
existing `kernel wakes this row: 0` line for the floor.

**Invariant 4 is the one to model first.** Cancellation on this path already has a sharp edge —
`scope.Cancel()` alone leaves a parked read parked — and recycling turns that known **hang** into a
**corruption**, which is strictly worse. Model before writing.

**How many K threads, and does it scale with cores?** Probably not: I/O concurrency scales with
in-flight operations and per-completion CPU, not with the machine. `EnableIoReactor(bool, unsigned
completionThreads = 1)` already exists as the knob. The rule that keeps it true: **a handler needing
real work pushes it to F rather than doing it** — parse, decompress, anything with a body. The moment
a dispatch thread does real work, the stolen-consumer problem is rebuilt one layer down.

---

## 6. Work not yet done

- The `hiPri*` → `io*` rename. Touches `hiPriInboxes`, `reservedForHiPri`, `SetIoHotLane`,
  `HiPriLaneActive`, `SetHiPriHint`/`ClearHiPriHint`, `hiPriStray`, all three park predicates and the
  dump columns. Worth doing as its own commit with the suite green either side — not folded into a
  behaviour change.
- `IOMPSCQueue` and its model.
- The bisected pool and its index accounting.
- `PushIO`, the `friend` declaration, and the reactor-side backlog.
