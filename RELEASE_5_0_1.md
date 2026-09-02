# JLib::Scheduler 5.0.1 — CRITICAL

**This release closes a memory-safety hole in the public task API. Upgrading is not optional if you
create tasks from lambdas that wait on anything.**

---

## The defect

`CreateTask(lambda, lane, TaskType::Fiber)` compiled, ran, and passed tests. It was unsound.

A fiber **row** is a leased stack plus the FLS slots, creditor list and retire bag hanging off it.
It has exactly one teardown — the recycle that follows `FiberStatus::DEAD` — so the runtime's
invariant is that every `AcquireFiber` reaches DEAD exactly once.

A lambda body is copied onto the **task slab**. That object has two owners with no destructor
between them:

- the **worker loop**, which frees the task frame the instant the body returns;
- whoever eventually **resumes the fiber**.

While the body merely runs, nothing is wrong. The moment it *suspends*, its continuation is a
promise made by something entitled to have gone away.

### Why no test caught it

`SlabPool` is append-only and releases no extent before the pool itself is destroyed. A freed slot
stays **mapped** and usually still holds its old bytes, so a suspend/resume round trip reads its
capture back intact and the test passes. The bill arrives somewhere else:

| outcome | what you see |
|---|---|
| **Arbitrary code execution** | the capture held a `this`, a `std::function`, a callback or anything with a vtable; the slot is reissued; the next indirect call goes through a pointer that now belongs to whatever was allocated over it |
| **Silent corruption** | an 8-byte hole punched through a live object, faulting minutes later in code that did nothing wrong |
| **Unbounded leak** | the row never reaches DEAD, the reaper never runs for it, the budget never recovers — a linear climb in commit size to an OOM |

The third is the *lucky* one, because it fails predictably.

---

## The fix

**A lambda task is always `TaskType::Native`.** `CreateTask`'s lambda overload no longer has a
`TaskType` parameter; passing one is a `static_assert` that explains the rule and points at the
supported form. This is enforced by the type system rather than by a test, because the failure mode
is one a test cannot see.

A job that waits takes the raw overload — a `void(*)(void*)` plus a context that **outlives the
wait**:

```cpp
struct WaiterCtx { JLib::Event* gate; std::atomic<bool>* ready; };

static void WaiterBody(void* p) {
    auto& c = *static_cast<WaiterCtx*>(p);
    JLib::TaskScheduler::Instance().WaitOnEventArmed(*c.gate, [&c] {
        if (c.ready->load(std::memory_order_acquire)) c.gate->SignalAll();
    });
}

WaiterCtx ctx{ &gate, &ready };                    // this frame owns it...
JLib::Task* t = sched.CreateTask(&WaiterBody, &ctx,
                                 JLib::Lane::Normal, JLib::TaskType::Fiber);
t->waitGroup = &done;
sched.Push(t);
sched.WaitFor(done);                               // ...and outlives the wait
```

Lambdas remain the default and correct spelling for the overwhelming majority of task bodies —
anything that computes and returns. Those never call `AcquireFiber` and this change costs them
nothing. A Native task that tries to wait anyway now aborts with a named diagnostic at the call
site that caused it, rather than stranding a row.

---

## Migrating

The compiler finds every affected site for you.

| before | after |
|---|---|
| `CreateTask(lambda, lane, TaskType::Fiber)` | named body + context struct, as above |
| `CreateTask(lambda, lane, TaskType::Native)` | drop the argument — it is the only thing a lambda can be |
| `CreateTask(lambda)` | unchanged |
| `CreateTask(&fn, ctx, lane, TaskType::Fiber)` | unchanged |

**The one rule at a call site:** the scope holding the context must not exit until the task has
finished. Spawn-then-join in the same function satisfies it structurally. A context declared
*inside a loop* whose join is *after* the loop does not — hoist it out, or give the contexts a
`reserve()`d vector that outlives the loop.

That case is not hypothetical. Converting this repo's own suite produced roughly thirty sites
needing lifetime-specific attention, and one — `teardown_drain_test`'s spawn helper — crashed
outright because its body died when the helper returned while its fibers stayed parked forever.

---

## Also in this release

**A fiber-row balance check.** Dev builds (`!NDEBUG` or `JLIB_DEVELOPMENT`) now count
`AcquireFiber` against recycle and report any imbalance at `Join()`:

```
[JLib::Scheduler] FIBER ROW LEAK at teardown: 10 rows acquired, 6 recycled, 4 outstanding (expected 0).
```

Public accessor: `TaskScheduler::OutstandingFiberRows()`. The sum is **pool-wide** — under
migration the worker that acquires a row is frequently not the one that runs it to DEAD, so a
per-worker difference is meaningless. Resting value is 0; read it when the pool is quiet and
compare deltas. A shipping build compiles this to nothing.

`tests/row_probe.cpp` is its negative control: a clean arm reports 0 and stays silent, a strand arm
parks three fibers on a mutex teardown cannot eject and reports exactly 3.

**Slab usage as data.** `TaskScheduler::SlabUsage()` returns the per-class profile
`ReportSlabUsage()` prints — `capacity`, `resident`, `peakLive`, `live`, `extents` — so a test or a
budgeting tool can read the numbers instead of parsing them back out of a string.

**Two documentation bugs fixed.** The README's suspending example taught the unsound pattern, and
under it a second bug that had never been exercised: `Push` followed immediately by `SignalAll` is
a lost wake. Both are corrected, and `readme_check` now compiles and runs the corrected form.

**`teardown_drain_test` no longer claims teardown cannot detect an abandoned frame.** It can: the
row balance reports the same 4 that test derives from its own arm structure, by an entirely
independent route.

---

## Compatibility

- Source-breaking **only** for `CreateTask(lambda, ..., TaskType::...)`. Every other signature is
  unchanged.
- No ABI change to `Task`, `Thread` or the scheduler layout beyond the two dev-gated counters,
  whose members are unconditional so `sizeof(Thread)` does not depend on the including translation
  unit's flags.
- C++17. No new dependencies.

## Verification

Clean rebuild from scratch, both configurations, 51 test binaries, **657 checks, 0 failures, 0
timeouts** in each. Zero `INVARIANT VIOLATED` aborts. One fiber-row-leak report, from
`teardown_drain_test`, which deliberately leaves four frames parked to test that policy and now
documents the report as expected.
