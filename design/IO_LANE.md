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
        |  pop own IOMPSCQueue -> resume the coroutine handle -> it awaits again or completes
        v
  handler completes, or issues the next operation and returns
```

**The reactor** owns the backlog, and the backlog is **TWO STACKS, NOT A QUEUE**:

```
    OS completion source
            |
            v
       in-stack          push arrivals here (LIFO, one store)
            |
            |  reverse, and ONLY when out-stack is empty
            v
      out-stack  --->  PushIO()      pop from here (FIFO order restored)
```

**IT ALLOCATES NOTHING.** `Task` already carries `std::atomic<Task*> next` — the field
`TaskMPSCQueue` links through — so each stack is a single member pointer, push and pop are two
stores, and the reverse is a pointer walk. A `std::queue` is `std::deque`-backed and allocates in
chunks; on a path whose entire job is to absorb a burst that already failed to be pushed, an
allocation is the last thing wanted. No synchronisation either: both stacks are touched only by the
reactor thread.

**FIFO IS PRESERVED AND THE AMORTISED COST IS O(1).** Arrivals `a, b, c` sit on the in-stack as
`c, b, a`; reversing yields `a, b, c` on the out-stack, which pops oldest-first. Each item is pushed
once, moved once, popped once.

> **REVERSE ONLY WHEN THE OUT-STACK IS EMPTY.** This is the one way to get the structure wrong:
> reversing while the out-stack still holds items puts newer arrivals *ahead* of older ones that
> were already waiting, and the ordering the backlog exists to protect is silently inverted. It
> fails as a fairness bug, not a crash.

The loop is therefore: pop the out-stack; if it is empty and the in-stack is not, reverse; only if
both are empty accept new completions from the OS.
**K threads** pop their own queue and RESUME the completion's coroutine handle. The handler runs until
its next `co_await` or until it returns; the frame is heap-allocated and owned by the coroutine, so
there is nothing to borrow and nothing to give back. One to three threads; two means one is usually
on duty while the other is between re-aims.

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

### 3.3 Coroutines, opt-in, C++20 — and the fiber pool is not involved at all

**REVERSED 2026-09-01.** An earlier draft of this document chose fibers, for C++17 and for two safety
properties pinning gives away. Jay's call is coroutines, and it is the right one: it simplifies the
pool question out of existence and it keeps the cost where it belongs.

**THE LIBRARY CORE STAYS C++17.** Only the I/O path is C++20, and only for applications that opt in.
That is already the shape that shipped — `IoAsync.h`: "`co_await` for asynchronous I/O. C++20 — and
the ONLY C++20 in the reactor", over a C++17 engine. Not everyone wants an I/O reactor; nobody who
declines it should pay a language-version bump for it.

**AND THE FIBER POOL IS NOT INVOLVED.** A coroutine frame is a heap allocation sized to its own
locals, served by the existing coroutine frame pool. It does not come from `GlobalFiberPool`, so:

- no bisected pool, no reserved slice, no index accounting
- no `poolIndex` collision — `Event::EnsureTable()` and `Hazard.cpp` size flat arrays by
  `GlobalFiberPool::TotalCount()`, and an I/O frame never appears in that space at all
- no 64 KB stack per anything

An earlier draft spent two sections on how to divide the fiber pool safely. **With coroutines there
is nothing to divide.**

### 3.4 A frame per operation, and linear async comes back with it

With fibers, a frame costs a 64 KB stack from a fixed arena, so a frame per *operation* meant a
pre-`Init` ceiling on simultaneous connections (1000 × 64 KB = 64 MB committed) and forced a frame
per *completion* instead — which in turn forced handlers into a state machine: store state on the
connection and return, never `co_await` twice.

**A coroutine frame is small and heap-allocated, so per-operation is simply affordable**, and the
handler-style constraint lifts with it:

```cpp
    auto n = co_await RecvAsync(conn, buf, sizeof buf, 0, tok);
    // ...
    co_await SendAsync(conn, reply, n, 0, tok);      // just works; state lives in the frame
```

The frame holds the continuation across both awaits. No connection-side state machine, and the
budget is bounded by outstanding operations at their real cost rather than by a stack apiece.

### 3.5 What choosing coroutines costs

Pinning was doing real work in the fiber draft, and giving it up hands back two hazards. Both are
listed as invariants in §4 rather than left in prose:

| | fibers (rejected) | **coroutines (chosen)** |
|---|---|---|
| language | C++17 throughout | **C++20 on the opt-in I/O path only** |
| frame cost | 64 KB stack, fixed arena | small heap frame, existing frame pool |
| pool interaction | bisected `GlobalFiberPool`, index accounting | **none** |
| handler style | state machine (per-completion) | **linear `co_await`** |
| routing | pinned; assign once per operation | free round-robin, which is what §2 does |
| double resume | prevented by construction | **must be prevented — invariant 4** |
| `thread_local` | stable across suspends | **breaks after every await — invariant 5** |
| suspend depth | anywhere in the call stack | only at a `co_await` |

The last row is the one to keep in view: a handler cannot suspend from inside an ordinary function it
calls. In practice I/O handlers are shallow — await, process, await — so this is rarely felt, but it
is a real restriction and the fiber path does not have it.

> **Correction on record, and it is the mistake that cost the most.** This document's author claimed
> twice that the lane had a structural ceiling because "completions resume PINNED fibers", and
> concluded from it that an ordered MPMC FIFO was required. The reactor is coroutines;
> `resumedInboxes` is the fiber path and a **different mechanism** that happens to sit beside it.
> Coroutine handles are not pinned, round-robin is legal, and **no MPMC FIFO is needed anywhere in
> this design.**
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

### 3.8 Thread-affine teardown: one optional `home`, and nothing else migrates

Invariant 6 says a handler may resume on a different K thread after every await. Two exits were
obvious and both are expensive: pin the coroutine (gives up round-robin and drags the fiber path
back in), or make nothing thread-affine in the first place (write a lock-free allocator). Jay's is
a third:

- `task->home = qIndex` on **first thread_local touch**; `0xFF` means *any worker*, which is the
  common case
- resume still runs anywhere
- **only the final teardown that genuinely refuses to be remote** — COM uninitialize, a thread-owned
  handle, a magazine you will not remote-free — is pushed to the home thread's inbox
- everything else: free-by-address plus the remote-free list

One `uint8_t`. No chain, no affinity list, no per-object table.

**The insight is that migration was never the problem — teardown was.** The frame does not care
where it resumes; a `HANDLE` opened by `CoInitialize`'s apartment does. Pinning the coroutine to
protect the last few microseconds of its life pays for the whole life. This pins the teardown only,
which is the part that is actually affine, and leaves the hot path untouched.

Three things it has to carry.

**(a) IT IS A CONTRACT, NOT A MECHANISM.** Nothing can detect a `thread_local` touch. `home` is set
by an explicit call, so a handler that touches TLS without declaring it gets no protection *and no
diagnostic*. Therefore the write must live in the **resource wrapper, at acquisition** — not in the
handler remembering to call it. Anything that depends on the author remembering is the loPri strand
again: a rule with no enforcement point, self-healing until it isn't.

**(b) THE TEARDOWN PUSH MUST NOT BE ALLOWED TO FAIL — AND §3.2 SAYS PUSHES FAIL.** This is where
this decision collides with an earlier one, and it is the "teardown might get complicated" that Jay
flagged. `IOMPSCQueue::push` returns `false` when full and invariant 3 says overflow drops
deliberately. **A dropped teardown is not a dropped packet.** It is a leaked apartment, an
unreleased handle, a magazine that never returns — a loss that never resolves on its own. Three
ways out, none measured:

- **route to the F inbox**: `loPriInboxes[home]` is unbounded and infallible today. But then `home`
  is an F index, and this only works if the affine resource was acquired on an F worker.
- **reserve headroom**: teardown pushes bypass the bound. Cheap, and it makes the bound a lie for
  exactly one class of item, which is arguably what a bound is for.
- **retry until it lands**: the reactor must never block, but this is not the reactor.

The third is probably right, and the reason is a frequency argument worth stating explicitly:
**teardown is O(connections), not O(completions).** A retry loop on the completion path would be
unacceptable; on a path that runs once per connection close it costs nothing measurable.

**(c) WHICH POOL DOES `home` INDEX?** K threads and F workers are different arrays with different
queue types, so a bare `uint8_t` is ambiguous between them — and (b) may deliberately want to land
in the *other* one. It needs a pool bit or a documented rule that homes are always F. Separately:
`0xFF` as the sentinel caps the home space at **254** while `kMaxHintQueues` is 256
(`include/TaskScheduler.h:2884`). `uint8_t` is already the convention for worker indices
(`src/TaskScheduler.cpp:5614`), so this is consistent — but at a 256-worker pool, worker 255 cannot
be a home and nothing would say so. Cap it and state it, or spend the second byte.

**One correction: "epoch slot on the fiber" is fiber-draft language.** With coroutines the mechanism
is counted epochs (`include/Epochs.h:474`), which shipped for this exact reason. The invariant
counted epochs did **not** lift still applies: **nothing may suspend inside an `EpochGuard`**, so a
handler must not hold a guard across a `co_await`. That is a third thing invariant 6 makes fatal,
alongside `thread_local` and same-thread assumptions, and it belongs in the same list.

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
3. **The backlog is BOUNDED, and overflow drops DELIBERATELY.** An unbounded backlog converts a
   latency problem into an out-of-memory one. Drops belong at the backlog as a stated policy with a
   counter, not as an emergent consequence of the OS socket buffer overflowing because nobody
   drained it -- late packets on one owner beat lost packets on two, but silent loss beats neither.
4. **The backlog drains before new completions are pulled, and the out-stack is reversed into only
   when it is EMPTY.** Otherwise the reactor keeps accepting while its own backlog grows: unbounded
   memory, and the oldest completions served last, inverting the ordering the bounded mailbox exists
   to protect. The two-stack structure gives FIFO amortised O(1) with no allocation -- and reversing
   early is the one way to break it, silently, as a fairness bug rather than a crash.
5. **Exactly one resume in flight per frame.** A coroutine handle is not pinned, which is what makes
   round-robin legal — but not-pinned is not the same as safe-to-resume-twice. A cancel racing a
   completion must never put the same handle in two K queues. **This is the invariant that pinning
   was providing for free**, and choosing coroutines means it has to be enforced instead. It is also
   the one to model first: cancellation on this path already has a sharp edge — `scope.Cancel()`
   alone leaves a parked read parked — and a double resume is a corruption where that is a hang.
6. **A handler may resume on a different K thread after every await.** Correct for the frame, fatal
   for three things inside it: a `thread_local`, a same-thread assumption, and an `EpochGuard` held
   across a `co_await` — counted epochs did not lift "nothing may suspend inside a guard". **The
   fiber path guarantees the opposite**, so the two contracts must be documented as opposites rather
   than assumed alike — the confusion between those two mechanisms is what produced the pinning error
   recorded in §3.5. The escape hatch for the first two is §3.8: one optional `home` per task,
   consulted only for teardown that refuses to be remote. Migration was never the problem.
7. **No Native handlers on this path.** A Native handler runs to completion on the dispatch thread
   and *is* a stolen consumer. Reject, do not tolerate: code that survives a violation is defending
   against it, not permitting it.

## 5. Open questions

**Sleep or spin for the K threads — and the answer is probably neither, alone.**

The concern: if K parks, every completion pays a wake, and a K that spins-and-yields instead will
suffer the same yield misses the floor did, ending in dropped packets.

**THE WAKE IS PER IDLE PERIOD, NOT PER COMPLETION.** K parks only when its queue is empty. Under
sustained traffic it never parks: the next completion finds `WS_EMPTY` — on core, scanning — and
costs nothing. The ~3–4 µs is paid on the idle→busy edge, once per burst.

    60 Hz client   one packet per 16.7 ms      a 4 µs wake is 0.025% of the interval
    busy server    continuous arrivals         K never parks; no wake at all

The configuration where parking genuinely hurts is **sparse traffic with a sub-10 µs latency
requirement** — real, but not the shape this library is aimed at.

**THE YIELD MISS IS ALREADY HANDLED BY §2.** `PushIO` round-robins and re-aims on `false`,
`IsThreadAvailable` skips a K in `WS_YIELD`, and two threads mean one is on duty while the other is
off-core. A yield miss costs a re-aim, not a drop. This is the same fix the floor got: the miss is
not prevented, it is made cheap.

**DROPPED PACKETS COME FROM THE REACTOR STALLING, NOT FROM K.** The OS socket buffer overflows when
nobody drains it, and the reactor is a dedicated thread doing only pop / push / backlog — it sustains
far more than a NIC delivers. K falling behind grows the backlog; it does not stop the drain. Drops
must therefore be a **bounded, deliberate policy at the backlog**, not an emergent consequence:
*late packets on one owner beat lost packets on two.* An unbounded backlog converts a latency problem
into an out-of-memory one, which is worse.

**THE SHAPE TO REACH FOR IS SPIN-THEN-PARK**, which this codebase already uses twice:

- `BareWaitBackoff` — CpuRelax ×512, then `std::this_thread::yield()`
- marl, measured — spins ~1 ms, then parks; an adaptive floor with no config

So: spin briefly after the last completion, covering back-to-back arrivals with no wake at all, then
park. Sustained load never sleeps; idle load never burns a core. One knob, and the default should be
chosen the way `kYieldFloorMin` was — small, with the reasoning written down.

**ALL OF THE ABOVE IS REASONING, NOT MEASUREMENT.** The number that settles it is completions/sec
against kernel wakes on this lane, the same shape as `kernel wakes this row: 0` for the floor. Until
that exists, the spin duration is a guess and should be a knob rather than a constant.
**Invariant 5 is the one to model first.** It is the one choosing coroutines took ON, and it is the
only hazard in this design that fails silently rather than loudly. Cancellation here already has a
sharp edge -- `scope.Cancel()` alone leaves a parked read parked -- and a double resume turns that
known **hang** into a **corruption**, which is strictly worse. Model before writing.

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
- A model for invariant 5 -- one resume in flight per frame, against a cancel racing a completion.
- A spin-then-park policy for the K threads, with the spin duration as a knob rather than a constant
  until completions/sec against kernel wakes has been measured on this lane.
- `PushIO`, the `friend` declaration, and the reactor-side backlog.
