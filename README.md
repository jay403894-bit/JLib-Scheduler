# JLib::Scheduler

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

**Fiber registry.** Fiber-local storage (`FiberLocal<T>`) that survives migration where
`thread_local` silently does not, lease/creditor tracking for thread-affine cleanup, type-erased debt
lists, and generation-tagged identities so a recycled slot can never be mistaken for a live fiber.

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
| Context switch | ~9.2 ns |
| 64-fiber wake storm | ~1.7 µs per fiber, median |
| Idle tax of the shipped floor | indistinguishable from zero |

Read ratios and orders of magnitude, not last digits. The bench prints observed ranges and says
**INDISTINGUISHABLE** where two arms overlap rather than letting you infer a winner from two medians
a nanosecond apart.

**There are no comparisons against other schedulers here, on purpose.** A head-to-head implies a
shared objective function, and schedulers are not built for the same things — this one trades
throughput for call-graph transparency and a bounded tail. A fork-join microbenchmark will report
that trade as a defect.

---

## Quick start

```cpp
#include <TaskScheduler.h>

int main() {
    JLib::TaskScheduler::Init();                       // 0 / omitted = size to the machine
    auto& sched = JLib::TaskScheduler::Instance();

    // ---- a job, and a join -------------------------------------------------------------
    JLib::WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);

    JLib::Task* t = sched.CreateTask([] { /* work */ });   // Fiber by default: it may wait
    t->waitGroup = &wg;
    sched.Push(t);
    sched.WaitFor(wg);

    // ---- a parallel loop, with no cost model to tune -----------------------------------
    std::function<void(int,int)> body = [&](int lo, int hi) {
        for (int i = lo; i < hi; ++i) { /* ... */ }
    };
    sched.ParallelFor(0, 1'000'000, 4096, body);       // grain optional; steals do the dividing

    // ---- suspending, from anywhere in the call graph -----------------------------------
    JLib::Event& gate = sched.GetEvent("frame_ready");
    sched.Push(sched.CreateTask([&] {
        sched.WaitOnEvent(gate);                       // suspends the FIBER, frees the worker
        /* resumes here, possibly on another worker */
    }));
    gate.SignalAll();
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

- **[Architecture & Technical Reference](design/ARCHITECTURE.md)** — the K/F topology, the queue
  kinds, hazard pointers and epochs, the reaper lifecycle, the fiber registry, the state machines.
- **[Design notes](design/NOTES.md)** — dated experiments, measurements, and the reasoning behind
  decisions that were reversed. The history, including what did not work.
- **[The I/O lane](design/IO_LANE.md)** — the reserved band and completion routing.
- **[Hazard pointers](design/hazard-pointers.md)** — the fine-grained reclamation path.
- **[CHANGELOG](CHANGELOG.md)**

---

## License

BSD-3-Clause. Part of [JLib](https://github.com/jay403894-bit).
