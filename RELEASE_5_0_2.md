# JLib::Scheduler 5.0.2 — CRITICAL

**Completes the 5.0.1 fix. A lambda task can no longer be given a fiber row by any route, including
the one 5.0.1 left open. Upgrade from 5.0.1.**

---

## What 5.0.1 missed

5.0.1 removed the `TaskType` parameter from `CreateTask`'s lambda overload, so the **constructor**
can no longer produce a lambda fiber. That closed the door people walk through. It did not close
the window.

`Task` is a struct, `type` is a **public bitfield**, and the library itself reassigns it. So this
compiled, ran, and nothing said a word:

```cpp
Task* t = sched.CreateTask([&]{ /* ...waits... */ });   // Native LambdaTask
t->type = TaskType::Fiber;                              // public field
sched.Push(t);                                          // a lambda fiber
```

A `static_assert` guards a **call**. It cannot guard an **object** that stays mutable afterwards —
and the guarantee the runtime needs is about the object at the moment a fiber row is leased to it,
not about how that object was constructed.

The consequences are the ones 5.0.1 described, unchanged: a closure on the task slab has two owners
with no destructor between them, so a body that suspends either resumes into a freed frame or never
reaches `FiberStatus::DEAD`. Neither surfaces where it was caused, because `SlabPool` is
append-only and a released slot stays mapped holding its old bytes.

## The fix

`Task` carries a `lambdaBody` bit, set inside `LambdaTask`'s two constructors — where it cannot be
forgotten, because every `LambdaTask` that exists went through one of them — and zero-initialised on
the raw path.

`Thread::AcquireFiber` refuses:

```
[JLib::Scheduler] INVARIANT VIOLATED: a LAMBDA task was marked TaskType::Fiber.
```

**Enforced in Release**, not dev-gated. `AcquireFiber` is the single point every fiber-backed task
passes exactly once, on a path already allocating a stack — the check is one bitfield test against
the branch predictor's favourite outcome.

**Fatal rather than a silent downgrade to Native.** The body asked to suspend. Running it Native
instead would abort at the first `WaitOnEvent` several frames away, with a message about the wrong
thing.

`sizeof(Task)` is unchanged at 64 — the bit went into the flag block's existing padding, so no
layout, ABI or packing assertion moves.

## Proof

`tests/lambda_fiber_guard_test.cpp`, three arms. The middle one is why the other two mean anything:

| arm | expected |
|---|---|
| lambda left Native | runs — the guard ignores Native |
| **raw fn-pointer as Fiber** | **runs — the guard discriminates** |
| lambda forced to Fiber | **aborts** — the guard is connected |

The illegal arm runs in a **child process**, because the violation aborts and an abort cannot be
caught. Its exit code is checked against 42 and 77 specifically, so "the child survived and ran the
task" — which is the bug — cannot be mistaken for a pass merely because the process exited nonzero.

---

## Also in this release

**The epoch-guard suspend check is now proven to fire.** `EpochGuardSuspendCheck` was wired at all
eleven `ContextSwitch` call sites and enforced in Release, and had never been observed doing
anything — an unfired tripwire and a disconnected one look identical from outside. `Epochs.h`
shipped `SetEpochSuspendViolationHandlerForTest` for exactly this and nothing used it.
`tests/epoch_suspend_check_test.cpp` does: 0 violations suspending with no guard held, 1 suspending
inside one.

**`bench/waitfor_participation.cpp` removed.** It crashed on startup — and had been crashing before
any 5.0.1 work touched it, verified by building and running the pre-change source. Nothing noticed
because it is a bench, so the suite never ran it, and compiling is not running. A target that
compiles and aborts is worse than one that is absent, because it reads as covered.

**`_REWRITE_BACKUP/` removed from the repo** — 133 files, 52,339 lines of stale pre-5.0 duplicate
source that nothing built, nothing ran, and no CI job referenced. It was riding along in every clone
and every source release.

**`io_socket_test`'s queue-depth arm no longer reports a starved sampler as a failure.** It measures
with a polling watcher thread, so a peak of 0 meant either "the sends never queued" (the regression
it exists to catch) or "the watcher never got a slice". It counts its samples now and reports
nothing rather than asserting something it did not measure.

---

## Compatibility

No API change from 5.0.1. No ABI change — `sizeof(Task)` is still 64. C++17, no new dependencies.

## Verification

Clean build, both configurations: **53 test binaries, 666 checks, 0 failures, 0 timeouts** in each.
All 7 benches exit 0. Every binary that builds also runs.
