# JLib::Scheduler
![CI](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml/badge.svg)

**A C++17 fiber-task library — a user-mode M:N scheduler and jobs system, with optional
asynchronous I/O.** Built for game engines and real-time simulations, where the number that matters
is not throughput but *the worst frame*.

Pure C++17. No coroutines, no optional language tier, no idle policy to pick.

---

## What it does

A task that waits **suspends a fiber**; the worker underneath it picks up other work. Because a fiber
owns a real stack, the suspension is transparent to everything above it:

```cpp
// This function has no idea it is inside a scheduler.
Mesh LoadMesh(const char* path) {
    auto bytes = ReadFile(path);      // deep inside: an I/O wait that suspends the fiber
    return Decode(bytes);             // resumes here, on whichever worker was free
}
```

`LoadMesh`, `ReadFile` and every frame between them are ordinary functions. Nothing is `async`,
nothing is rewritten, and a third-party library in the middle of the call graph does not need to know
it can be suspended. **That is what the fibers buy**, and it is the reason this runtime dropped its
C++20 coroutine layer rather than keeping both.

The cost of that choice is a stack per suspended task, which is why a lot of this design is about
making stacks cheap: tiered sizes, per-worker caches, and stacks that circulate rather than being
returned to the OS.

---

## Architectural pillars

**Call-graph transparency.** A whole call graph suspends and resumes intact. No function colouring,
no `async` infection, no rewriting the middle of a stack you do not own.

**Bifurcated core topology.** Workers split into two bands sharing one packed 64-bit word:

- **Static K lanes** — a small reserved band (clamped at 2) that never parks, so an I/O completion or
  a latency-critical dispatch reaches a *running* thread rather than paying a kernel wake.
- **Adaptive F floor** — a bounded number of ordinary workers held awake, grown by a controller under
  load and **shed when the burst ends**. Everyone else parks on native address-wait primitives
  (`WaitOnAddress` on Windows, `futex` on Linux), so an idle pool costs the rest of your process
  nothing.

**O(1) lock-free dispatch.** A 64-bit atomic bitmap of awake workers makes placement a bit scan
rather than a walk, and a push to an already-awake worker takes a skip instead of a notify.

**Two queue kinds, deliberately.** Unstealable MPSC inboxes stage work for exactly one consumer;
Chase–Lev deques exist so other threads *can* steal. Steal hint words turn a thief's scan into a
test.

**Tiered fiber stacks**, each region carrying an unbacked guard page so overflow faults instead of
corrupting a neighbour:

| class | usable / region | for |
|---|---|---|
| `Tiny` | 2 pages (8 KB on x64) | I/O continuations — wake, read a completion, finish |
| `Standard` | 60 KB / 64 KB | general compute, physics, animation |
| `Deep` | 508 KB / 512 KB | asset decompression, deep call trees, recursive traversal |

`Tiny` is page-denominated, so it is 8 KB on a 4 KB-page platform and 32 KB on a 16 KB-page one.
Stacks are **committed** at allocation, not merely reserved.

**Thread-local fiber caches.** Every worker holds its own cache *per stack class*; the global pool is
consulted only on a miss, and refill is a batch. A single shared free-list would put every worker on
one cache line for an operation that happens on every task that can suspend.

**Hybrid Safe Memory Reclamation.** Hazard pointers for readers with no stable identity and for reads
that span a suspension; epoch-based reclamation with per-thread retire bags for bulk retirement; and
a **Reaper** — a queued task, not a thread and not a poll — sent by the fiber registry on fiber death
to run `Tick()`, `Scan()` and user-registered deletes in one pass. No stop-the-world, no reference
counting on the hot path.

**Migratable or pinned fibers — `FiberMode`.** A suspended fiber resumes on **whichever worker is
free** (`FiberMode::Migrate`, the default), rather than waiting for the one it started on. That is
the better trade almost always: a fiber whose home worker is busy would otherwise sit idle beside
free cores.

The cost is exact and worth stating: **`thread_local` read before a suspension point is not the same
object after it.** It does not crash — you get the resuming worker's copy, a plausible value — which
is why the runtime provides fiber-local storage and why the failure is worth naming.

```cpp
JLib::TaskScheduler::SetFiberMode(JLib::FiberMode::Pin);   // BEFORE Init()
```

`Pin` resumes a fiber only on its home worker, so `thread_local` is safe again — the contract marl
gives you. Take it when the state crossing a wait belongs to a library you cannot audit. Otherwise
prefer `Migrate` and move the *state* rather than the policy.

**Fiber registry.** Fiber-local storage that survives migration where `thread_local` silently does
not, lease/creditor tracking for thread-affine cleanup, type-erased debt lists, and generation-tagged
identities so a recycled slot can never be mistaken for a live fiber.

```cpp
static const uint16_t kScratch = JLib::TaskScheduler::AllocFiberLocalSlot();  // once, not per task

JLib::TaskScheduler::FiberLocal(kScratch) = state;   // survives the wait, in EITHER mode
sched.WaitOnEvent(gate);
auto* s = JLib::TaskScheduler::FiberLocalAs<State>(kScratch);
```

**Cancellation and time.** `CancelScope`/`CancelToken` with parent chains and generation tags,
reaching work that has *already been dispatched*; a hierarchical timer wheel behind `Deadline` and
periodic work.

**Modern completion engines.** Windows IOCP and Linux `io_uring` directly — no POSIX wrapper
indirection. Completions are stamped `StackClass::Tiny` at submit.

---

## Measured

**One machine, one day: Intel i9-13900K, Windows, Release, 2026-09-02.** Reproduce with
`runtime_bench`; every arm compares the runtime against *itself*, and the file documents its own
method.

| | |
|---|---|
| Dispatch latency, p50 | **0.40 µs** |
| Dispatch latency, p99 | ~2.4 µs (2000+ samples) |
| Context switch | **9.8 ns** (AVX-dirty), 9.4 ns (clean) |
| 64-fiber wake storm | ~1.7 µs per fiber, median |
| Idle tax, shipped floor (2 workers) | **indistinguishable from zero** (−0.05% … +0.13%) |
| Idle tax, whole pool held awake (31) | **~+20%** |

Read ratios and orders of magnitude, not last digits. The bench prints observed ranges and says
**INDISTINGUISHABLE** where two arms overlap rather than letting you infer a winner from two medians
a nanosecond apart.

**Where the context-switch number comes from.** Saving XMM6-15 with legacy SSE while the upper YMM
state is dirty costs an SSE/AVX transition on every switch — **85.4 ns**. A CPUID-gated `vzeroupper`
removes it, and that is what ships: **9.8 ns**, an **8.7× improvement** on an AVX workload.

Through a full `Event` round trip it **saves ~88 ns** on a fiber with dirty upper state. The cost to
a fiber *without* one measured **+1 ns** on a quiet machine — indistinguishable, on ranges that
overlap — and +32 ns on a noisier run, so treat it as free rather than as a tuned trade.

`SchedulerContextSwitchBench` prints both directions, a **correctness gate** that round-trips
XMM6-15 past a deliberate clobber for every variant, and a same-vs-same control that must read
1.00× (measured 0.998× and 1.011×). A speed number from a variant that does not preserve the
registers is not a number at all, which is why the gate runs first.

**The two idle-tax rows are the argument for the whole topology**, so they are worth reading
together. An idle pool is not free to the rest of your process: hold **every** worker awake and a
memory-bound main thread pays **~20%** — cores are occupied, and no amount of cheapening the spin
recovers that. Hold **two** awake and the same thread cannot tell the difference.

That is why the runtime has a *bounded, shedding* floor instead of a "never park" switch, and why
the switch was removed rather than left as an option. The +20% is what the design is buying its way
out of, measured — not the cost of using it.

Both figures survived everything we could think to attack them with: three workload lengths
(0.4 ms / 4 ms / 15 ms), a machine with every other application closed, and two different idle
strategies. The bench prints all of it, and prints which conclusion the data supports rather than
the one that would read best.

**There are no comparisons against other schedulers here, on purpose.** A head-to-head implies a
shared objective function, and schedulers are not built for the same things — this one trades
throughput for call-graph transparency and a bounded tail. A fork-join microbenchmark will report
that trade as a defect.

---

## Quick start

```cpp
#include <TaskScheduler.h>

// A job that WAITS is a named function plus a context struct -- see the suspending section below
// for why that is the only shape the runtime supports for one.
struct WaiterCtx { JLib::Event* gate; std::atomic<bool>* ready; };

static void WaiterBody(void* p) {
    auto& c = *static_cast<WaiterCtx*>(p);
    // ARMED, because Push-then-Signal is a race: the signal can arrive before this task has
    // parked, and a plain WaitOnEvent would then wait for a wake that already happened. The arm
    // callback runs after the task is registered as a waiter but before it sleeps, so re-checking
    // the condition there closes the window.
    JLib::TaskScheduler::Instance().WaitOnEventArmed(*c.gate, [&c] {
        if (c.ready->load(std::memory_order_acquire)) c.gate->SignalAll();
    });
    /* resumes here, possibly on another worker */
}

int main() {
    JLib::TaskScheduler::Init();                       // 0 / omitted = size to the machine
    auto& sched = JLib::TaskScheduler::Instance();

    // ---- a job, and a join -------------------------------------------------------------
    JLib::WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);

    JLib::Task* t = sched.CreateTask([] { /* work */ });   // a lambda job: runs, returns, done
    t->waitGroup = &wg;
    sched.Push(t);
    sched.WaitFor(wg);

    // ---- a parallel loop: no threshold to tune, steals decide the split ---------------
    std::function<void(int,int)> body = [&](int lo, int hi) {
        for (int i = lo; i < hi; ++i) { /* ... */ }
    };
    sched.ParallelFor(0, 1'000'000, 4096, body);       // grain optional; steals do the dividing

    // ---- suspending, from anywhere in the call graph -----------------------------------
    //
    // A JOB THAT WAITS IS NOT A LAMBDA. `CreateTask(lambda)` is always Native -- it runs and
    // returns -- and the compiler will tell you so if you ask for anything else. A job that
    // suspends takes the raw form: a `void(*)(void*)` plus a context that OUTLIVES THE WAIT.
    //
    // That is an ownership rule, not a style. A fiber leases a stack whose only teardown is the
    // recycle after it finishes, so the runtime needs exactly one owner for that job's state. A
    // closure on the task slab has two -- the worker loop frees the frame when the body returns,
    // while the fiber belongs to whoever resumes it -- and nothing destroys it once. Keeping the
    // state on the caller's stack, where you can see its scope, removes the second owner.
    JLib::Event& gate = sched.GetEvent("frame_ready");
    JLib::WaitGroup done;
    done.n.store(1, std::memory_order_relaxed);

    std::atomic<bool> ready{ false };
    WaiterCtx ctx{ &gate, &ready };             // lives in THIS frame, which outlives the wait

    JLib::Task* waiter = sched.CreateTask(&WaiterBody, &ctx,
                                          JLib::Lane::Normal, JLib::TaskType::Fiber);
    waiter->waitGroup = &done;
    sched.Push(waiter);

    ready.store(true, std::memory_order_release);
    gate.SignalAll();
    sched.WaitFor(done);       // never leave a suspended fiber behind at exit
}
```

**A dependency graph:**

```cpp
JLib::TaskDAG dag(sched);
auto* physics = dag.CreateNode(sched.CreateTask([]{ /* ... */ }));
auto* anim    = dag.CreateNode(sched.CreateTask([]{ /* ... */ }));
auto* render  = dag.CreateMainNode(                                  // must run on the main thread
                    sched.CreateTask([]{ /* ... */ }, JLib::Lane::Normal, JLib::TaskType::Native));

dag.AddDependency(render, physics);
dag.AddDependency(render, anim);
dag.Submit();                                                        // false if the graph has a cycle
```

Gates (`CreateGate`, `LogicType::AND` / `OR`) compose into arbitrary boolean expressions, and
external nodes (`CreateExternalNode` / `SignalExternal`) let a graph wait on something outside it.

**Asynchronous I/O** is opt-in and reserves the K band:

```cpp
JLib::TaskScheduler::EnableIoReactor(true);   // BEFORE Init(); reserves 2 workers
JLib::TaskScheduler::Init();
```

---

## Configuration

**The distinction that matters is *before `Init()`* versus *runtime*.** Anything that decides how the
pool is BUILT — how many workers, how many fibers, which services exist — is read once at `Init` and
ignored afterwards. Anything that steers a pool that already exists can be changed at any time.

### Before `Init()`

| | default | |
|---|---|---|
| `EnableIoReactor(bool)` | off | Turns on the I/O layer. **Reserves K = 2 workers**, so it is not free — a job-system-only app should not pay for it. |
| `EnableTimers(bool)` | off | Timer wheel, behind `Deadline` and periodic work. |
| `SetFiberMode(FiberMode)` | `Migrate` | `Pin` to keep `thread_local` valid across a wait. |
| `SetFiberBudget(normal, tinyPerK, deep)` | `64, 64, 1` | Fibers per worker, per class. **This is the cap on how many tasks may be SUSPENDED AT ONCE** — a waiting task holds its stack. |
| `SetIoHotLane(k)` / `SetHotWorkers(k)` | 0 (2 with the reactor) | Size of the reserved band. Clamped at 2. |
| `SetAffinityPolicy(...)` | `Ideal` | Hard pinning measured *worse* than an ideal-processor hint; `None` for a shared machine. |

### At runtime

| | |
|---|---|
| `SetAwakeFloor(n)` | Workers held unparked. **Clamps against the LIVE pool**, so calling it before `Init` silently resolves to 0. |
| `SetReservedStealing(bool)` | Whether the reserved band takes ordinary work while the lane is quiet. |
| `SetSubmitLimit(n)` | Ingress backpressure — slows a producer before it builds a backlog. |
| `SetStealHint(bool)` | Diagnostic A/B of the steal hint. Shipping code leaves it on. |

**On `SetFiberBudget`, because it is the one with a sharp edge:** exhaustion is a **spin**, not a
failure — `AcquireFiber` returns nothing and the task is requeued. That clears on its own if the
blocked tasks can finish, and *does not* if they are waiting on work that cannot get a fiber because
they are holding them all. A graph with more simultaneously-suspended nodes than the budget is the
shape to look for. The pool warns once, naming the class that ran out and the argument that fixes it.

There are around fifty other setters. They are tuning and diagnostic knobs — split policy, park
primitive, power throttling, slab growth — and they exist so a behaviour can be A/B'd inside one
process rather than across two builds. You do not need any of them to use the library, and the
header documents each one where it is declared.

---

## Contracts you owe the scheduler

Short list, because each of these fails **silently**:

1. **`TaskType::Native` must not suspend.** It runs on the worker's OS stack — there is nothing to
   switch away from. Use the default (`Fiber`) for anything that waits.
2. **Use the fiber-aware primitives inside a task.** `SchedulerMutex`, `SchedulerSemaphore`,
   `SchedulerConditionVariable`, `Event`, `WaitGroup`. A raw `std::mutex` blocks a *worker*.
3. **A `TaskDAG` frame loop must call `EpochManager::Instance().Tick()`.** A DAG retires a node per
   node per frame and need not produce a single fiber death, so nothing triggers the reaper for it.
4. **Nothing may suspend inside an `EpochGuard`.** Use hazard pointers for reads that span a wait.
5. **`IoRequest` and `IoResult` must outlive the operation.** The kernel writes into them after the
   call returns.
6. **Cancelling a scope does not cancel posted I/O** — `IoReactor::RequestCancel` does. The kernel
   holds your buffer until the completion arrives.

---

## What this is not built for

Said plainly, so nobody has to discover it with a profiler:

- **Not a general-purpose async runtime.** No work-conserving scheduler for millions of tiny
  connections. A suspended task holds a stack, so concurrency is bounded by the fiber budget by
  design — this is a game runtime with I/O, not a server.
- **Not the fastest fork-join.** Suspension capability costs a stack, and the reserved band costs
  cores. If your workload never waits, a scheduler that cannot suspend will beat it.
- **Not a distributed or cross-process system.** One process, one pool.
- **macOS is jobs-system only.** kqueue is not implemented and is not claimed.

---

## Platforms

| platform | support |
|---|---|
| **Windows** (x64, ARM64) | full runtime, IOCP reactor |
| **Linux** (x64, ARM64) | full runtime, `io_uring` reactor |
| **Android** | jobs system; epoll reactor is the intended path |
| **macOS** | **jobs system only** — no reactor |

---

## Verification

The concurrency is **model-checked**, not argued. Twelve GenMC models and a TLA+ model live in
`tests/verify/` — the deque, the MPSC inbox, the sleep/wake permit handshake, the worker pass, the
yield state, the event table, counted epochs, and fiber resume.

They are not decoration. The Chase–Lev `pop_bottom` `seq_cst` fence is *proven* required — a model
with it removed is red. Several carry deliberate negative-control builds so that "the model is green"
can be told apart from "the model cannot fail".

Where an invariant depends on the codebase reading its own state, the model explains and a **runtime
test** locks it, because no model reads `PickNextWorker`.

---

## Documentation

- **[Architecture & Technical Reference](DESIGN.md)** — the K/F topology, the queue
  kinds, hazard pointers and epochs, the reaper lifecycle, the fiber registry, the state machines,
  and the contracts a caller owes.
- **[Design notes](design/NOTES.md)** — dated experiments, measurements, and the reasoning behind
  decisions that were reversed. The history, including what did not work.
- **[The I/O lane](design/IO_LANE.md)** — the reserved band and completion routing.
- **[Hazard pointers](design/hazard-pointers.md)** — the fine-grained reclamation path.
- **[CHANGELOG](CHANGELOG.md)**

---

## License

BSD-3-Clause. Part of [JLib](https://github.com/jay403894-bit).
