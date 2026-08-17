# JLib::Scheduler design notes

Everything that does not belong in a README: how the scheduler works, the contracts you have to
honour, and the decisions that turned out to be wrong.

- [Execution model](#execution-model)
- [Core placement (CorePref)](#core-placement-corepref)
- [Starvation prevention](#starvation-prevention)
- [Task modes](#task-modes)
- [Integration contracts](#integration-contracts)
- [API patterns](#api-patterns)
- [Synchronization and memory](#synchronization-and-memory)
- [TaskDAG](#taskdag)
- [Platform notes](#platform-notes)
- [Design decisions, and the bugs that taught them](#design-decisions-and-the-bugs-that-taught-them)

---

## Execution model

The scheduler maps physical cores to logical workers and provisions everything from slabs, so a
steady-state frame makes no allocations and no syscalls.

### Topology

`Init()` queries the real CPU topology rather than assuming CPU numbering: LLC clusters, SMT
siblings, and per-core efficiency class on hybrid parts. Windows reads
`GetLogicalProcessorInformationEx`; Linux parses sysfs; macOS reads sysctl. Work-stealing prefers
victims in the same cache domain before reaching across hardware boundaries, and skips busy SMT
siblings, since stealing from a busy sibling recruits no new execution ports.

### Slab allocator

`new` and `delete` are deleted on `Task`. Tasks come from a fixed-slot slab with epoch-based
reclamation: no runtime heap traffic, and `sizeof(Task) == 64` is enforced by `static_assert`.

### Queues

Each worker has split high/low priority deques plus an MPSC inbox that only the owner drains.
Stealing prefers LLC-local peers, then an idle SMT sibling, then a random victim. Priority is queue
order only. It never decides which core class runs a task -- see below.

---

## Core placement (CorePref)

Hybrid CPUs (Intel 12th-gen and later) mix performance and efficiency cores. Under `Hard` pinning
the OS cannot place work by class, so the scheduler does it -- explicitly, and orthogonally to
priority.

```cpp
enum class CorePref : uint8_t {
    Default,   // no preference (full-pool round-robin) -- the default for every task
    P,         // prefer performance cores (latency-critical, chunky work)
    E,         // prefer efficiency cores (background/bulk; preserves P headroom)
    Wide,      // explicit no-preference: throughput bursts that want ALL cores
    Any = Wide // alias: "genuinely don't care" -- same mechanism, honest name
};

sched.CreateTask(fn, data, /*hiPri*/ false, FiberSize::Standard, /*noFiber*/ true, CorePref::E);
```

The rules:

- Priority and placement are orthogonal. `hiPri` orders queues, `corePref` places work, and they are
  never coupled. A coupled design (hiPri→P, loPri→E) creates a structural starvation gradient:
  sustained high-priority load spills into the efficiency cores' lanes and squeezes bulk work from
  both directions at once.
- Preference is a hint at push. Placement spills to the other class rather than waiting on an
  unavailable one.
- Preference is a rule at steal. Thieves vet a candidate's class before claiming it, via
  `TaskDeque::steal_if` -- the predicate runs between the buffer read and the CAS, so declining costs
  zero atomics and never claims an unvetted task. `Default`/`Any`/`Wide` tasks stay stealable by
  everyone.
- A declined steal is not a miss. Steal-backoff damps CAS storms; a class decline performs no CAS,
  so it neither shrinks the probe width nor resets it.
- Owners run what they own. Spill transfers ownership, and explicit pinning (`PushImmediate`, DAG
  fork nodes) overrides class preference entirely.
- On non-hybrid CPUs every worker labels P, the class checks disable, and behaviour is identical to
  a classic full-pool scheduler.

`CorePref` is opt-in and dormant by default. See [Platform notes](#platform-notes) for where it does
nothing at all.

---

## Starvation prevention

Two mechanisms, and one that was removed.

**Steal fairness.** After 8 consecutive hiPri steals a worker is forced to scan loPri before
resuming hiPri preference. Without it a hiPri stream keeps every thief busy while loPri work sits.

**Priority inheritance.** When a high-priority task contends a lock held by a low-priority task, the
holder is boosted for the duration and restored on unlock. Fiber-aware, via
`Thread::GetCurrent()->currentRunningTask`. Use `SchedulerMutex` to get it.

**Age-based promotion, removed.** An earlier version promoted loPri tasks after 50 ms in queue.
Measurement showed it was vestigial: single-item stealing already un-starves a task the moment any
thief takes it, so promotion only ever helped work that got re-queued. The fairness window above is
the real anti-starvation mechanism. It stays out until a profile says otherwise.

---

## Task modes

Two execution paths. Choosing the wrong one deadlocks or corrupts queue state, so this is the one
table worth memorising.

| Mode | `noFiber` | Stack | Model | Use for |
|---|---|---|---|---|
| Standard | `true` (default) | worker's own stack | run to completion, never suspends | bulk math, raycasts, data sweeps, physics jobs |
| Fiber | `false` | pooled fiber stack | cooperative, may suspend | fork-join, anything that waits |

`noFiber` tasks skip fiber acquisition and the context switch entirely, which is why per-job
overhead stays small enough to run an entire physics engine's job graph through the pool.

---

## Integration contracts

### Fork-join requires a fiber

A task that will call `WaitFor` must be created with `noFiber = false`.

If it is `true`, the scheduler runs it directly on a worker thread. When it then calls `WaitFor`,
suspension is attempted on a thread with no fiber under it -- that throws inside a `noexcept`
`Execute()` and fail-fasts immediately, with no message (`STATUS_STACK_BUFFER_OVERRUN` on Windows).

### Pinned services must not be fibers

`PushImmediate(cpu_affinity, task)` removes a worker from the general pool, offloads its queue to
its neighbours, and locks it to one task's loop. Service tasks launched this way -- audio mixers,
network listeners -- must be `noFiber = true`. Immediate-mode tasks sit outside the fiber pool, so a
suspend/resume from one corrupts worker-queue boundary tracking.

### Suspending never blocks a thread

A fiber task that waits on an event suspends the *fiber*; the worker immediately picks up other
work. That is the property that lets GPU-fence waits, physics barriers, and IO share one pool with
compute without stalling cores, and it is the reason fibers exist here at all.

### From a bare thread, hold nothing across a blocking call into the scheduler

A bare thread cannot suspend, so when it blocks on a `SchedulerMutex`, a `SchedulerSemaphore`, a
condition variable or `WaitFor`, it runs stolen noFiber tasks instead of burning the core. That is
work-conserving and it has a consequence worth stating plainly: **acquiring a lock executes user
code**. Whatever the caller holds can be demanded by the task it runs, and the interleaving is
chosen by the scheduler, so lock-ordering discipline in the caller's own code cannot prevent it.

The concrete failure is self-deadlock. A thread holding mutex A waits on B, helps, and the helped
task asks for A. A is owned by that same thread, which is stuck inside the task, so nothing can ever
release it. No fiber is involved.

Three guards bound this (see `ContendedSpinStep`): a thread owning a `SchedulerMutex` stops helping
entirely, helping never nests more than one level deep, and a thread that has made no progress for a
long time yields rather than spinning. `SchedulerSemaphore::ScopedPermit` opts a lock-like permit
into the first of those, which raw `Wait`/`Signal` cannot do because a permit has no owner.

**None of that makes deadlock impossible, which is why the rule above is the actual protection.** A
helped task can block on something the scheduler cannot see: a plain `std::mutex`, a file read, a
GPU fence, a user's own condition variable. Ownership tracking reaches none of those. Hold nothing,
and the question does not arise.

Fibers are exempt from all of it. A fiber suspends on contention and never enters the helping path,
which is the main reason to prefer running work as tasks rather than blocking a bare thread.

### Nothing thread-derived may be held across a suspend point

The direct consequence of the above: a suspended fiber resumes on whatever worker picks it up, which
is usually not the one it left. So any value that identifies or belongs to a thread, a `Thread*`, a
thread index, the address of a `thread_local`, is stale the moment a suspend returns. Re-fetch it,
never carry it across.

This is not a style rule, and it bites harder on AArch64 than on x86-64. Reaching a `thread_local`
on x86-64 goes through `%fs:`-relative addressing that is re-evaluated at every access, so a stale
one is hard to construct. AArch64 has to materialize the thread pointer from `TPIDR_EL0` into a
general register, which the compiler may hoist into a callee-saved one, and a correct context switch
then faithfully preserves it across the migration. Same for Windows on ARM64 via `x18`. The bug is
invisible on the platform most people develop on and live on the one they ship to.

The scheduler's own thread-local state is structured so this cannot arise rather than trusting the
rule. Epoch slots are the example worth copying: `Epochs::ThreadSlot(tid)` exists only as the
fallback for callers that are *not* on a fiber and therefore cannot migrate, while a fiber's epoch
slot is registered per-fiber in `GlobalFiberPool` and travels with it. `TaskAllocator`'s per-thread
free-list cache is safe for the other available reason, `Alloc` and `Free` contain no suspend point,
so the window does not exist.

---

## API patterns

### Fork-join

```cpp
JLib::WaitGroup wg;

// noFiber MUST be false: this task suspends while waiting on children
Task* parent = sched.CreateTask(ParentWork, data, /*hiPri*/ 1, FiberSize::Standard, /*noFiber*/ false);
parent->waitGroup = &wg;
wg.n.fetch_add(1, std::memory_order_release);   // count BEFORE push -- workers decrement on completion
sched.Push(parent);

sched.WaitFor(wg);   // fiber callers park; main spin-helps by stealing noFiber tasks
```

### Pinned service

```cpp
// Runs raw on the pinned thread, so noFiber MUST be true
Task* audioService = sched.CreateTask([](void*) {
    while (engineRunning) {
        UpdateAudioBuffers();   // OS waits or atomics -- never a fiber yield
    }
}, nullptr, /*hiPri*/ 1, FiberSize::Standard, /*noFiber*/ true);

sched.PushImmediate(/*coreID*/ 2, audioService);   // evicts core 2's queue, locks the loop to it
```

### ParallelFor

`ParallelFor(start, end, chunk, fn)` picks between flat dispatch, where the caller spawns every
chunk, and **slice-stealing**, where one task per worker pulls `[lo, lo+grain)` off a shared cursor
until the range is consumed. Flat wins up to about 2 tasks per worker; past that its O(#tasks)
serial spawn on one thread collapses.

That crossover used to hand off to recursive fork-join splitting, which built the spawn tree with
the pool and measured roughly 8x faster than flat at ~15k tasks. Slice-stealing replaced it in 1.4
because it removes the per-chunk task entirely -- fork-join distributes the *spawning* but still
creates one task per chunk, at ~80-140 ns each for a slab slot, a push and an epoch retirement.
Measured against it: 1.7-1.9x on a uniform body, 1.2-1.3x when cost varies ~20x across the range.
`ParallelForFJ` remains public for callers who want the fork-join tree directly; it is simply not
what `ParallelFor` selects any more.

Whether to parallelize at all is decided by measurement, not element count: it runs a small prefix
inline, times it, extrapolates, and only splits if the estimated work clears ~75 µs. **That probe is
the one thing `ParallelRange` does not do** -- on a 20,000-item job the prefix is ~312 items run
serially, about a third of the wall time, which is worth skipping when you already know the range is
large and worth keeping when you do not. See
[Choosing a range API](README.md#choosing-a-range-api-parallelfor-vs-parallelrange), and see
[the crossover note](#parallelfor-is-gated-on-measured-work-not-element-count) below for why the
element count was the wrong unit, and for the limitation that remains.

---

## Synchronization and memory

Use `SchedulerMutex` instead of `std::mutex` where a lock may be held by a low-priority task while a
high-priority task waits -- it carries the priority inheritance described above. Scheduler-internal
locks and locks used within a single priority level do not need it.

While spinning on a contended `SchedulerMutex` or `SchedulerConditionVariable`, the spinner helps
drain the pool by stealing `noFiber` tasks (class-vetted against the core it is actually standing on)
rather than burning cycles.

You never call `delete` on a task. On completion the scheduler returns the slot to the slab, guarded
by epoch-based reclamation. This is enforced rather than documented: `operator delete` is deleted on
`Task`.

---

## TaskDAG

`TaskDAG` expresses execution order -- physics before render submission -- without blocking
primitives.

### Memory

Completion hooks live in the `TaskNode` rather than the `Task`, because the node doubles as the
trampoline's context and outlives it. That is what keeps `Task` at exactly 64 bytes. Firing a node
performs no heap allocation; nodes come from the slab and retire through EBR, which is what makes
the lock-free dependents lists safe against concurrent readers.

Root discovery and cycle checking use a single-threaded build-phase vector that `Submit()` clears.
After submission the graph is fully decentralized: nodes are autonomous and free themselves.

### Node types

**Worker nodes** (`CreateNode`) distribute across the stealing pool, with optional priority and
explicit `cpu_id` pinning.

**Main-thread nodes** (`CreateMainNode`) route through `PushMain` and run only when the main thread
pumps `ProcessMainThread`. Anything awaiting a graph that contains one must use `WaitForMain` -- a
plain `WaitFor` hangs forever on a node nothing is servicing.

**Gates** (`CreateGate`) are structural nodes with no payload. AND fires when every dependency
completes; OR fires on the first and short-circuits the rest. Gates nest, so `(A && B) || C`
composes naturally.

### Lifecycle

```
[ Build ] ──► [ HasCycle ] ──► [ Submit() ] ──► [ trampoline loop ]
(AddDependency) (Kahn's)       (fire roots)     (OnTaskFinishedWrapper)
```

`Fire()` wraps the task's fn/data with the completion trampoline. A worker runs the payload.
`OnTaskFinished()` atomically decrements dependents' counters, and satisfied dependents fire
immediately -- work cascades through the pool with no coordinator.

```cpp
JLib::TaskDAG graph(sched);

Task* inputTask   = sched.CreateTask(UpdateInput,          nullptr, 1, FiberSize::Standard, true);
Task* physicsTask = sched.CreateTask(IntegratePhysics,     nullptr, 1, FiberSize::Standard, true);
Task* renderTask  = sched.CreateTask(SubmitRenderCommands, nullptr, 1, FiberSize::Standard, true);

TaskNode* inputNode   = graph.CreateNode(inputTask);
TaskNode* physicsNode = graph.CreateNode(physicsTask);
TaskNode* renderNode  = graph.CreateMainNode(renderTask);  // graphics context wants the main thread

graph.AddDependency(renderNode, inputNode);    // render waits on both (implicit AND)
graph.AddDependency(renderNode, physicsNode);

if (!graph.Submit()) { /* cycle detected -- graph refused safely */ }
```

---

## Platform notes

### Where the four targets come from

The context switch is hand-written assembly per ABI: MASM for Win64, GAS for System V and for
AAPCS64. A new *architecture* therefore needs a new `ContextSwitch` and a matching `Fiber::Init`,
not a compiler flag.

Verified in CI on every push: Windows x64 (MSVC), Linux x86-64 (GCC), Linux AArch64 (GCC), macOS
arm64 (AppleClang). AArch64 on Android/Termux (Clang) is verified by hand. The benchmark suite
passes on all of them, fibers suspending and resuming through the switch under the real scheduler,
and the AAPCS64 switch has a standalone ABI harness (`tests/fibertest_aarch64.cpp`) run at `-O0` and
`-O2` on both ARM64 platforms.

The ARM64 results agreeing across three toolchains, three libcs and two object formats
(GCC/glibc/ELF, Clang/bionic/ELF, AppleClang/libc++/Mach-O) is what makes the ABI claim worth
anything. Raspberry Pi is the same configuration as the CI ARM64 runner and needs nothing extra.
32-bit targets are not supported and are not planned.

Platform code lives in `src/win32/`, `src/posix/` (Linux, Android) and `src/darwin/` (macOS). The
ABI layer under `src/posix/<arch>/` is shared by every POSIX target, because the calling convention
belongs to the instruction set and not to the kernel. `include/platform.h` is the only place that
tests OS and architecture.

Windows on ARM64 is refused explicitly rather than attempted. MSVC's ARM64 assembler is `armasm64`,
whose syntax is unrelated to the GAS syntax the AArch64 switch is written in, and a Windows fiber
switch must also update the TEB's stack bounds -- the fixup the x64 MASM does and the Linux/macOS
switch deliberately omits.

### ucontext is not used, and that is a measurement

`swapcontext` saves and restores the signal mask, which is a `sigprocmask` syscall on every switch:
120 ns against 8 ns for the hand-written version. Its POSIX deprecation is the lesser reason.

### Worker binding

Under `Hard`/`PhysicalOnly`, worker *i* binds to logical CPU *i+1* with main on CPU 0. That is what
makes the topology maps true rather than guesses.

Under the default `Ideal`, Windows uses `SetThreadIdealProcessor` (a hint) and Linux binds to the
whole LLC domain -- a mask, which Windows has no equivalent of. That mask ends up exactly as tight as
the hardware warrants. On multi-L3 parts (Ryzen CCDs, Threadripper, EPYC) it genuinely binds, and
that is where it matters, because a worker migrating across cache domains pays inter-die latency on
every steal. On single-L3 parts the domain is every CPU, so it binds nothing -- correct rather than
missing, since there is no domain to protect. It does not keep `siblingQIndex` true on Linux; the
kernel can still migrate within the LLC.

`Ideal` is the default because `Hard` measured worse: about 45% on wake latency and about 2x on the
frame DAG, because a wake has to wait for one specific, possibly parked core instead of landing on
any awake core in the domain. That contradicts the usual "engines pin everything" advice, and it
contradicted this project's own earlier position.

### Pool size

Auto size is `hardware_concurrency − 1`: main on CPU 0, workers on the rest. That is only safe
because the JLib stack keeps *busy* foreign threads at zero by construction -- input is Raw Input
riding the app's existing message pump, and gamepad support is opt-in and dynamically loaded
precisely because XInput spawns its own threads.

The rule is to reserve one core per foreign thread with measured busy time, never per thread that
merely exists. The one library that earned a reservation was GameInput, whose always-polling worker
showed a dose-responsive one-core deficit. JLib audio's remaining foreign thread -- its backend's
device-IO thread, event-driven at ~100 wakes/s and microseconds of memcpy per wake -- measurably
costs nothing, so audio does not change the default. `Init(N)` honours explicit sizes up to full
`hardware_concurrency`.

### Transient oversubscription is accepted on purpose

Pinned workers cannot dodge threads no user-mode process controls: GPU driver workers, DXGI, DWM,
all waking for microseconds at unpredictable times in every process on the machine. Desktop Windows
has no core isolation -- that is a console feature -- so the only correct handling is the one the OS
already provides, which is brief preemption.

Profilers report this faithfully and it looks alarming. VTune's Thread Oversubscription metric
counts spin-waiting threads as running, and in a mostly-idle game nearly all CPU time *is* short
spin and wake bursts. So the metric reads high while sampled concurrency never approaches core count
and frame times do not move with pool size. The number is real by Intel's definition; it describes a
designed trade, not a defect.

### CorePref does nothing outside Windows

P/E classification reads each core's `EfficiencyClass` from `GetLogicalProcessorInformationEx`.
Linux has no single equivalent -- the available signals are a perf-driver artifact
(`/sys/devices/cpu_core` vs `cpu_atom`) or CPPC `highest_perf` -- so it reports every core as equal.

That includes big.LITTLE and DynamIQ AArch64, where the heterogeneity is realer than on any x86
hybrid part: a phone typically spans three capacity tiers rather than two. Android is nonetheless
the wrong place to add it, because the platform's cgroups own thread placement and affinity requests
from an unprivileged app are routinely ignored. It arguably matters less on Linux generally, since
the kernel does hybrid placement itself via ITMT -- a class table there second-guesses a scheduler
that already knows, where on Windows nothing else is making the call.

An explicit `P` or `E` request is therefore silently a no-op on those platforms. That is safe rather
than broken: preference is a hint, so an empty class set spills and the task runs full-pool. But do
not build a design around it and expect it to hold cross-platform.

**macOS and QoS.** macOS has no thread-affinity API on arm64, but Apple provides a different
mechanism for the same intent: `pthread_set_qos_class_self_np` with `QOS_CLASS_USER_INITIATED`
biases toward P-cores and `QOS_CLASS_UTILITY` toward the efficiency cluster. That is a hint needing
no index-to-level mapping, so the missing piece on macOS -- sysctl publishes per-level CPU counts but
not which CPU is which -- stops being a blocker.

The scheduler still sets no QoS at all, and that is a decision. Workers inherit the QoS of whatever
thread calls `Init()`, and that inheritance is the configuration mechanism: an app wanting a
particular class sets its own before initialising and the pool follows, with no API to learn. QoS is
a power and thermal choice as much as a scheduling one, and a host running at `UTILITY` because it
is a background exporter has decided something about battery and fan noise. A job system that
silently promoted its workers would be overriding that -- the same argument that justifies
`AffinityPolicy::None` elsewhere.

If explicit tiering is ever added it must be opt-in and gated more tightly than affinity, for two
reasons that have nothing to do with Apple. Stealing is preference-blind, so a `Default` task will
land on a `UTILITY` worker -- which on macOS is a deprioritised, throttleable thread rather than
merely a slower core. And tiering shrinks the usable pool: a class-preferred task can only be placed
on its subset, which today is survivable only because preference spills to the other class, and
under QoS the spill target is throttled rather than just slower.

### Other limits

Processor group 0 only, so at most 64 logical CPUs. Fine for desktops and workstations; dual-socket
machines need work this project does not do.

Tasks are 256-byte slab slots, so lambda captures beyond about 192 bytes fail a `static_assert`.
Capture pointers, not payloads.

---

## Design decisions, and the bugs that taught them

Negative results with receipts.

### Fibers rather than C++20 coroutines

Coroutines colour the call chain. A function can only suspend if it was written as a coroutine, and
so must every frame between it and the scheduler -- `co_await` propagates one frame at a time, and
any ordinary function in the middle stops it dead. A fiber suspends the stack, so the wait can sit at
the bottom of a call chain whose middle frames are plain functions, including ones you do not own.

It is easy to overstate this, so: the suspend call is still yours. A third-party function that blocks
internally on a mutex or a syscall blocks the worker thread, and no fiber rescues you -- the library
has to hand you a callback, a visitor, or a polling hook first. What fibers remove is the requirement
that everything *between* your suspension point and the scheduler be a coroutine. Given a physics
library that takes a visitor callback, your callback can wait on a `WaitGroup` with the library's
frames sitting untouched in the middle of the stack. Coroutines cannot express that without
rewriting the library.

The same applies to code you do own, because colouring is transitive: making one leaf function
suspend means converting every caller above it. Recursive algorithms are the sharpest case. This
scheduler's own fork-join benchmark calls `WaitFor` at every level of a recursive split, which is
ordinary code on a fiber and a whole-program refactor with coroutines.

Two smaller differences. A suspended fiber has a real stack, so debuggers walk it and profilers
attribute samples to it, where a suspended coroutine is a heap object whose call history is gone.
And fiber stacks are pooled at fixed cost from a guard-paged arena rather than allocated per
invocation and elided if the compiler manages it.

Where coroutines do fit they are cheaper -- no stack, no switch, a frame sized to its locals -- and
they are standard C++, whereas fibers cost a hand-written context switch per ABI. That is exactly
why this scheduler is a hybrid rather than fibers-everywhere.

### The hybrid is a correctness boundary, not a performance dial

A `noFiber` task runs on one OS thread from start to finish and cannot migrate, because it has no
suspension point to migrate across. Everything thread-affine is therefore correct inside it:
`thread_local` stays consistent, `std::this_thread::get_id()` is stable, thread-bound OS resources
(COM apartments, GL contexts, per-thread allocator arenas) behave, and a plain `std::mutex` can be
locked and unlocked normally. Unlocking from a different thread than locked it is undefined
behaviour, and that is exactly the trap a migrating task sets.

None of that holds on the fiber path, where a task may suspend on one worker and resume on another,
so TLS read before a wait and after it can belong to different threads. That is why the scheduler
ships fiber-aware synchronization and why the contracts say not to hold a raw `std::mutex` across a
suspend.

The payoff is that middleware written for an ordinary thread pool drops straight in. JLib's Physics3D
drives Jolt Physics through a `JPH::JobSystem` adapter over this scheduler, with every Jolt job
submitted as `noFiber`. Jolt is pure compute, never suspends, and keeps per-thread temp allocators,
so it needs exactly the guarantee the default path gives -- it runs as if it were on enkiTS and never
learns fibers exist. Under a fiber-everything scheduler that integration is a hazard instead.

That generalises to three shapes, which cover most of what an engine links:

- Libraries with a pluggable dispatcher -- Jolt's `JobSystem`, PhysX's `PxCpuDispatcher`, Bullet's
  `btITaskScheduler`, Box2D v3's task callbacks. A thin adapter submitting `noFiber` tasks is enough.
- Poll-driven libraries -- an ASIO `io_context::poll()`, an `enet_host_service` with a zero timeout.
  Pump it from one `noFiber` task per frame.
- Blocking service loops -- a network listener or audio mixer that wants to own a thread. Use
  `PushImmediate` with `noFiber`, which reserves a worker for it.

So you are not cut off from the ecosystem in exchange for having fibers, which is the trade a fiber
scheduler usually asks you to make. This is also where the design departs from marl and
FiberTaskingLib rather than from coroutines: those run every task on a fiber, imposing the migration
discipline on the overwhelming majority of tasks that never suspend.

### No batch steal

A lock-free batch steal claims `[t, t+n)` under one `top` CAS, but the owner's `pop_bottom` takes
from the other end without touching `top` for non-last items. A batch can therefore double-claim a
task the owner also popped. That was a real use-after-free heisenbug, not a thought experiment.
Single-item stealing is standard, fast, and correct. There is no `steal_batch`.

### Predicated steal, not peek-then-steal

Class-aware stealing has to vet candidates, but a separate peek and steal is a TOCTOU race: another
thief advances `top` in between and you claim a task you never inspected. `steal_if` evaluates the
predicate between the buffer read and the CAS, so declines cost zero atomics and claims are exactly
the vetted slot.

### Priority is orthogonal to placement

The first P/E design coupled hiPri→P and loPri→E. Analysis killed it before it shipped: under
sustained hiPri load, spill floods the E-workers' hiPri lanes, and since every worker drains hiPri
first, bulk work gets squeezed by placement and priority simultaneously. Worse, it silently
invalidated the reasoning that justified removing age-based promotion. Orthogonal axes, always.

### ParallelFor is gated on measured work, not element count

The old gate was `N > 10000`, chosen before fork-join dispatch existed and never re-measured. The
deeper problem was that element count cannot express the crossover at all: what races dispatch
overhead is total work, which is count multiplied by per-element cost, and that differs by orders of
magnitude between callers. Sweeping both axes showed the crossover element count moving roughly 400x
with body cost while the crossover *work* stayed pinned around 70–92 µs. So the gate now probes a
prefix, extrapolates, and parallelizes at ~75 µs of estimated work.

The remaining limitation, found in third-party benchmark data: the probe measures elapsed time, which
cannot distinguish 75 µs of CPU work from 75 µs of waiting on DRAM -- and only the first parallelizes.
On a bandwidth-constrained machine the compute-bound bodies crossed over at 78 µs and 95 µs, matching
the constant closely, while the cheap memory-bound bodies needed 164 µs and 272 µs. The constant is
well-calibrated for compute-bound work and optimistic for memory-bound work.

### Hard pinning, and the foreign-thread problem solved at the source

Pinning makes the topology maps true, since sibling, cluster and P/E stealing are only meaningful
when workers cannot migrate. The cost is that pinned threads cannot dodge foreign threads landing on
their core.

Profiling a real game process found the persistent offender: GameInput's internal workers,
undocumented in count and varying by machine, with dose-response testing showing a full core of
deficit. Manual-dispatch mode (`IGameInputDispatcher`) was tried first and was not the fix -- it
controls when queued async work runs, not whether the library keeps threads of its own. The actual
fix was replacing the dependency: keyboard and mouse moved to Raw Input on the app's existing message
pump, gamepads to opt-in, dynamically-loaded XInput. A library whose thread count you cannot know is
a library you cannot budget for.

The portable lesson is the reservation rule above: reserve for measured busy time, never for thread
existence, and never for transient wakers like drivers, DXGI or DWM -- those are unknowable,
universal, microsecond-scale, and already handled by preemption.

Note that hard pinning is no longer the default, for the reasons under [Worker
binding](#worker-binding). It remains available because it is the only mode where you decide where
oversubscription lands rather than discovering it.
