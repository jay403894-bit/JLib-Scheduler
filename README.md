# 🧵 JLib::TaskScheduler

A fiber-based, work-stealing task scheduler for real-time engines. Hand-written x64 context switching, lock-free Chase-Lev deques, a slab-allocated task system, frame DAGs with logic gates, and hybrid-core (P/E) aware placement — built for Windows, on purpose.

**Windows x64 · MSVC · C++17 · BSD licensed**

---

## 📈 Measured (i9-13900K, Release)

| Metric | Number |
|---|---|
| Task enqueue → dequeue latency | **6.3 µs** |
| 5-node frame DAG (build + validate + execute) | **31.9 µs** |
| `Task` struct size | **exactly 64 bytes** (one cache line, `static_assert`-enforced) |
| Fiber stacks | 64 KB standard / 512 KB heavy, contiguous arena, guard-paged |
| Steal protocol | single-item Chase-Lev CAS (see *Design Decisions* for why there is no batch steal) |

## 🎯 Why this exists

Public C++ job systems make you choose: task graphs **without** fibers ([Taskflow], enkiTS), fibers **without** maintenance (marl — archived), or GDC-talk fiber schedulers that never ship outside their studios. This is the missing combination, maintained and BSD-licensed:

|  | enkiTS | Taskflow | marl | **JLib::TaskScheduler** |
|---|---|---|---|---|
| Work-stealing | ✅ | ✅ | ✅ | ✅ |
| Fibers (suspend/resume *inside* tasks) | ❌ | ❌ | ✅ | ✅ |
| Dependency DAG w/ AND/OR gates | partial | ✅ | ❌ | ✅ |
| Cache/SMT topology-aware stealing | ❌ | ❌ | ❌ | ✅ |
| Hybrid P/E-core placement | ❌ | ❌ | ❌ | ✅ |
| Maintained | ✅ | ✅ | ❌ archived | ✅ |

The trade: **Windows x64 + MSVC only, modern hardware assumed.** Constraining the problem is what makes one-person excellence possible — see *Requirements*.

---

## 📋 Table of Contents

1. [Requirements & Honest Limitations](#1-requirements--honest-limitations)
2. [Quick Start](#2-quick-start)
3. [Execution Paradigm & Component Map](#3-execution-paradigm--component-map)
4. [Hybrid P/E-Core Placement (CorePref)](#4-hybrid-pe-core-placement-corepref)
5. [Starvation Prevention](#5-starvation-prevention)
6. [Task Execution Modalities](#6-task-execution-modalities)
7. [Critical Integration Contracts](#7-critical-integration-contracts)
8. [Core API & Workflow Architectures](#8-core-api--workflow-architectures)
9. [Synchronization & Memory Safety](#9-synchronization--memory-safety)
10. [JLib::TaskDAG](#10-jlibtaskdag)
11. [Design Decisions (and the bugs that taught them)](#11-design-decisions-and-the-bugs-that-taught-them)

---

## ⚙️ 1. Requirements & Honest Limitations

**Requirements:** Windows 10+ · x64 · MSVC (C++17 or later) · MASM (`ml64`, ships with VS) for the context-switch assembly.

**Deliberate limitations — read before adopting:**

- **Windows x64 / MSVC only.** The context switch is hand-written x64 MASM against the Windows ABI; fibers, affinity, and topology queries are Win32. A Linux port is a real port (community fork exists), not a flag.
- **Workers are hard-pinned** (worker *i* → logical CPU *i+1*, main on CPU 0). This is what makes the topology maps (SMT sibling, LLC cluster, P/E class) *true* rather than guesses — the OS cannot migrate workers. See *Design Decisions*.
- **Auto pool size is `hardware_concurrency − 1`** (main on CPU 0, workers on the rest) — and this is only safe because the JLib stack keeps *busy* foreign threads at zero by construction: input is Raw Input riding the app's message pump (zero threads; gamepad support is opt-in and dynamically loaded precisely because XInput spawns its own). **The rule: reserve one core per foreign thread with *measured busy time* — not per thread that merely exists.** The one time a library earned a reservation (GameInput's always-polling worker), dose-response testing showed exactly one core of deficit; JLib audio's remaining foreign thread (its backend's device-IO thread) is the opposite case — event-driven, ~100 wakes/s, microseconds of memcpy per wake — and measurably costs nothing, so audio does **not** change the default. `Init(N)` honors explicit sizes up to full `hardware_concurrency`.
- **Transient oversubscription is accepted, on purpose.** Pinned workers can't dodge the threads no user-mode process controls — GPU driver workers, DXGI, DWM — which wake for microseconds at unpredictable times in *every* process on the machine. Desktop Windows has no core isolation (that's a console feature), so the only correct handling is the one the OS already provides: brief preemption. Profilers will faithfully report this: VTune's *Thread Oversubscription* metric counts spin-waiting threads as running, and in a mostly-idle game nearly all CPU time **is** short spin/wake bursts — so the metric reads high while sampled concurrency never approaches core count and frame times don't move with pool size. The number is real by Intel's definition; it describes a designed trade (spin-waits buy the latency figures above), not a defect.
- **Processor group 0 only** (≤ 64 logical CPUs). Fine for desktops/workstations; dual-socket monsters need work this project doesn't do.
- **Tasks are 256-byte slab slots.** Lambda captures beyond ~192 bytes fail a `static_assert` — capture pointers, not payloads.

---

## 🚀 2. Quick Start

Build + deploy the static lib (both configs) with the included script, or just add `src/*` + `ContextSwitch.asm` to your project.

```cpp
#include <TaskScheduler.h>

int main() {
    JLib::TaskScheduler::Init();               // auto pool size (hw-1); Init(hw-2) if you ship a foreign thread with real busy time
    auto& sched = JLib::TaskScheduler::Instance();

    // Fire-and-forget fastJob (default): pure compute, runs inline on a worker
    sched.Push([] { HeavyMath(); });

    // Fork-join: create tasks against a WaitGroup, then wait (fiber-suspends if
    // called from a task; spin-helps by stealing fastJobs if called from main)
    JLib::WaitGroup wg;
    for (int i = 0; i < 8; ++i) {
        auto* t = sched.CreateTask([i] { Chunk(i); });   // hiPri=false, fastJob=true defaults
        t->waitGroup = &wg;
        wg.n.fetch_add(1, std::memory_order_release);
        sched.Push(t);
    }
    sched.WaitFor(wg);

    // Data-parallel loop: auto-selects flat vs fork-join dispatch by measured crossover
    sched.ParallelFor(0, 1'000'000, 4096, [](int a, int b) {
        for (int i = a; i < b; ++i) out[i] = std::sqrt((float)i);
    });

    sched.Join();
}
```

---

## 🏗️ 3. Execution Paradigm & Component Map

The scheduler maps physical hardware cores to logical workers, utilizing specialized allocation and queuing mechanisms to eliminate system call overhead.

### Topology Awareness
On `Init()`, the system queries the **real** CPU topology (`GetLogicalProcessorInformationEx`, not numbering assumptions): Last Level Cache (LLC) clusters, SMT siblings, and per-core **EfficiencyClass** (P-core vs E-core on hybrid parts). Work-stealing prioritizes victims in the same cache domain before reaching across hardware boundaries, skips busy SMT siblings (stealing from a busy sibling recruits no new execution ports), and respects core-class placement (see §4).

### Slab Allocator & Local Caches
Standard `new`/`delete` are explicitly deleted on `Task`. Tasks are provisioned from a fixed-slot slab allocator with epoch-based reclamation — zero runtime heap traffic, perfect alignment, and `sizeof(Task) == 64` enforced at compile time.

### Dual-Queue Priorities
Each worker maintains split high/low priority deques plus MPSC inboxes (owner-drained). Work is stolen with hierarchical preference: LLC-local peers → idle SMT sibling → global random. **Priority is queue order only** — it never determines which core class runs a task (§4).

---

## ⚡ 4. Hybrid P/E-Core Placement (CorePref)

Modern consumer CPUs are hybrid (Intel 12th-gen+: performance + efficiency cores). Because workers are hard-pinned, the OS cannot place work by class — so the scheduler does it, explicitly and orthogonally to priority:

```cpp
enum class CorePref : uint8_t {
    Default,   // no preference (full-pool round-robin) -- the default for every task
    P,         // prefer Performance cores (latency-critical, chunky work)
    E,         // prefer Efficiency cores (background/bulk; preserves P headroom)
    Wide,      // explicit no-preference: wide throughput bursts that want ALL cores
    Any = Wide // alias: "genuinely don't care" -- same mechanism, honest name
};

sched.CreateTask(fn, data, /*hiPri*/ false, FiberSize::Standard, /*fastJob*/ true, CorePref::E);
```

**The rules (deliberate, and worth copying):**

- **Priority ⊥ placement.** `hiPri` orders queues; `corePref` places work. They are never coupled — a coupled design (hiPri→P, loPri→E) creates a structural starvation gradient: sustained high-priority load spills into the efficiency cores' lanes and starves bulk work from *both* directions at once.
- **Preference is a hint at push** — placement spills to the other class rather than ever waiting on an unavailable one.
- **Preference is a rule at steal** — thieves vet a candidate's class *before* claiming it (`TaskDeque::steal_if`: predicate evaluated between the buffer read and the CAS, so declining costs **zero** atomics and never claims an unvetted task). `Default/Any/Wide` tasks remain stealable by everyone.
- **A declined steal is not a miss.** Steal-backoff exists to damp CAS storms; a class-decline performs no CAS, so it neither shrinks probe width nor resets it.
- **Owners run what they own.** Spill transfers ownership; explicit CPU pinning (`PushImmediate`, DAG fork nodes) overrides class preference entirely.
- **Non-hybrid CPUs:** every worker labels P, class checks disable, behavior is identical to the classic full-pool scheduler.

---

## 🛡️ 5. Starvation Prevention

Two complementary mechanisms guarantee no priority level starves:

### Steal Fairness
After 8 consecutive hiPri steals, the scheduler forces a loPri scan before resuming hiPri preference. Without this, a hiPri stream could keep every thief busy while loPri work sits untouched.

### Priority Inheritance (SchedulerMutex)
When a high-priority task contends a lock held by a low-priority task, the holder is temporarily boosted (and restored on unlock) — the classic priority-inversion fix, fiber-aware via `Thread::GetCurrent()->currentRunningTask`.

> **Where did age-based promotion go?** Removed, deliberately. An earlier version promoted loPri tasks after 50 ms in queue — measurement showed it was vestigial: single-item stealing *already* un-starves a task the moment any thief takes it (promotion only helps work that gets re-queued, and stolen work runs immediately). The fairness window above is the real anti-starvation. Kept out until a profile says otherwise.

---

## 🎮 6. Task Execution Modalities

The scheduler operates two distinct execution pathways. **Selecting the wrong pathway will result in immediate deadlocks or queue corruption.**

| Execution Mode | fastJob | Allocation | Thread Model | Use Case |
|---|---|---|---|---|
| **Standard Task** | `true` (Default) | Raw worker stack | Non-cooperative, run-to-completion | Bulk math, raycasts, data sweeps, physics jobs |
| **Fiber Task** | `false` | Custom ASM/C++ fiber stack | Cooperative (may `WaitOnEvent`/suspend) | Fork-join patterns, waitable work |

fastJobs skip fiber allocation and context switching entirely — this is why per-job overhead stays microscopic for pure-compute workloads (e.g., an entire physics engine's job graph).

---

## 🚦 7. Critical Integration Contracts

### Contract 1: The Fork-Join / PushFork Rule

When employing fork-join parallelism, tasks must cooperatively yield their execution contexts during wait cycles rather than blocking the physical thread.

**The Rule:** You **MUST** pass `fastJob = false` inside `CreateTask` when pushing to `PushFork`.

**The Trap:** If a task enters `PushFork` with `fastJob = true`, the scheduler runs it as a standard thread-bound job. When that job calls `WaitFor()`, it will attempt fiber suspension mechanics on a naked thread — immediate hard deadlock.

### Contract 2: Long-Running Services vs. Immediate Mode

`PushImmediate(cpu_affinity, task)` strips a worker from the general pool, offloads its queue to neighbors, and locks it to a dedicated loop.

**The Rule:** Service tasks (audio processing loops, network listeners) launched via `PushImmediate` must be `fastJob = true`.

**The Trap:** Immediate-mode tasks are structurally isolated from the fiber pool. If one triggers a fiber suspend/resume, the worker-queue boundary tracking corrupts.

### Contract 3: Fiber-Suspending Tasks Never Block Threads

A fiber task that calls `WaitOnEvent`/`WaitForFenceValue`-style primitives suspends the *fiber*; the worker thread immediately picks up other work. This is the property that lets GPU-fence waits, physics barriers, and IO ride the same pool as compute without stalling cores — and it's the reason fibers exist in this scheduler at all.

---

## 🔧 8. Core API & Workflow Architectures

### The Fork-Join Pattern (Fibers)

```cpp
JLib::WaitGroup wg;

// fastJob MUST be false: this task suspends while waiting on children
Task* parent = sched.CreateTask(ParentWork, data, /*hiPri*/ 1, FiberSize::Standard, /*fastJob*/ false);
parent->waitGroup = &wg;
wg.n.fetch_add(1, std::memory_order_release);   // count BEFORE push -- workers decrement on completion
sched.PushFork(parent);

sched.WaitFor(wg);   // fiber callers park; main spin-helps by stealing fastJobs
```

### The Immediate Mode Pattern (Pinned Services)

```cpp
// Service tasks run raw on the pinned thread (fastJob MUST be true)
Task* audioService = sched.CreateTask([](void*) {
    while (engineRunning) {
        UpdateAudioBuffers();   // uses OS waits or atomics -- NEVER fiber yields
    }
}, nullptr, /*hiPri*/ 1, FiberSize::Standard, /*fastJob*/ true);

sched.PushImmediate(/*coreID*/ 2, audioService);   // evicts core 2's queue, locks the loop to it
```

### ParallelFor (hybrid dispatch)

`ParallelFor(start, end, chunk, fn)` auto-selects between flat dispatch (caller spawns chunks — wins ≤ ~2 tasks/worker) and recursive fork-join splitting (the tree is built *by* the pool — measured 8× faster at fine grain, 15k+ tasks). Ranges ≤ 10k items run serially: dispatch overhead would dominate.

---

## 🛡️ 9. Synchronization & Memory Safety

### Priority Inheritance (SchedulerMutex)

Use `SchedulerMutex` instead of `std::mutex` when a lock might be held by a low-priority task while a high-priority task waits.

**When to use:**
- ✅ Locks shared between hiPri and loPri tasks
- ✅ Resource pools where hiPri work might wait on loPri holders
- ❌ Scheduler-internal locks (already fast, no fiber wait)
- ❌ Locks only used within one priority level (no inversion possible)

While spinning on a contended `SchedulerMutex` or `SchedulerConditionVariable`, the spinner **helps drain the pool** by stealing fastJobs (class-vetted against the core it's actually standing on) instead of burning cycles.

### Memory Lifecycle Ownership

You **never** call `delete` on a task. On completion the scheduler returns the slot to the slab (epoch-based reclamation guards the lock-free structures). This is enforced: `operator delete` is deleted on `Task`.

---

## 📊 10. JLib::TaskDAG

The TaskDAG manages multi-threaded task dependencies — explicit execution order (Physics before Render submission) without blocking primitives.

### 🧠 Memory Architecture

**The 64-Byte Cache Target:** Completion hooks live in the `TaskNode` (the node doubles as the trampoline's context and outlives it), keeping `Task` at exactly 64 bytes. Firing a node performs **zero heap allocations** — nodes come from the slab.

**Epoch-Based Reclamation:** Completed nodes retire through the EBR pipeline back to the slab — safe against concurrent readers in the lock-free dependents lists.

**Transient build vector:** Root discovery and cycle checking use a single-threaded build-phase vector, cleared inside `Submit()`. Post-submission the graph is fully decentralized; nodes are autonomous and self-free.

### 🔀 Node Typologies

**1. Standard Worker Nodes (`CreateNode`)** — distributed across the work-stealing pool; optional priority and explicit `cpu_id` pinning.

**2. Main-Thread Nodes (`CreateMainNode`)** — route exclusively through `PushMain`; execute only when the main thread pumps `ProcessMainThread`.
**Critical Rule:** whoever awaits a graph containing a main node **MUST** use `WaitForMain` — plain `WaitFor` hangs forever on the un-pumped node.

**3. Logic Gates (`CreateGate`)** — structural nodes with no payload:
- **AND** — fires when *every* dependency completes.
- **OR** — fires on the *first* dependency, short-circuiting the rest.
- Gates nest: `(A && B) || C` pipelines compose naturally.

### 🔄 Lifecycle

```
[ Build Phase ] ──► [ HasCycle Check ] ──► [ Submit() ] ──► [ Trampoline Core Loop ]
(AddDependency)     (Kahn's validation)    (fire roots)     (OnTaskFinishedWrapper)
```

1. `Fire()` wraps the task's fn/data with the completion trampoline
2. A worker executes the original payload
3. `OnTaskFinished()` atomically decrements dependents' counters
4. Satisfied dependents `Fire()` immediately — work cascades through the pool with no coordinator

### 💻 Integration Example

```cpp
JLib::TaskDAG graph(sched);

Task* inputTask   = sched.CreateTask(UpdateInput,         nullptr, 1, FiberSize::Standard, true);
Task* physicsTask = sched.CreateTask(IntegratePhysics,    nullptr, 1, FiberSize::Standard, true);
Task* renderTask  = sched.CreateTask(SubmitRenderCommands,nullptr, 1, FiberSize::Standard, true);

TaskNode* inputNode   = graph.CreateNode(inputTask);
TaskNode* physicsNode = graph.CreateNode(physicsTask);
TaskNode* renderNode  = graph.CreateMainNode(renderTask);  // graphics context wants the main thread

graph.AddDependency(renderNode, inputNode);    // render waits on BOTH (implicit AND)
graph.AddDependency(renderNode, physicsNode);

if (!graph.Submit()) { /* cycle detected -- graph refused safely */ }
```

---

## 🔬 11. Design Decisions (and the bugs that taught them)

Negative results with receipts — the section most libraries won't write.

**No batch steal.** A lock-free batch steal claims `[t, t+n)` under one `top` CAS, but the owner's `pop_bottom` takes from the *other* end without touching `top` for non-last items — a batch can double-claim a task the owner also popped. That was a real use-after-free heisenbug, not a thought experiment. Single-item stealing is standard, fast, and *correct*; there is no `steal_batch`.

**Age-based promotion removed.** See §5 — promotion was vestigial once stealing went single-item. Features earn their place with profiles here.

**Priority ⊥ placement.** The first P/E design coupled hiPri→P / loPri→E. Analysis killed it before it shipped: under sustained hiPri load, spill floods the E-workers' hiPri lanes, and since every worker drains hiPri first, bulk work gets squeezed by placement *and* priority simultaneously — a structural starvation gradient. Worse, it silently invalidated the reasoning that justified removing age-promotion. Orthogonal axes, always.

**Predicated steal, not peek-then-steal.** Class-aware stealing needs to vet candidates — but a separate peek + steal is a TOCTOU race (another thief advances `top` in between, and you claim a task you never inspected). `steal_if` evaluates the predicate between the buffer read and the CAS: declines cost zero atomics, claims are exactly the vetted slot.

**Hard pinning, with the foreign-thread problem solved at the source.** Pinning makes the topology maps true (sibling/cluster/P-E stealing is only meaningful when workers can't migrate) at one real cost: pinned threads can't dodge foreign threads landing on their core. Profiling a real game process found the persistent offender — GameInput's internal workers (undocumented count, varies by machine; dose-response testing showed a full core of deficit). Manual-dispatch mode (`IGameInputDispatcher`) was tried first and was **not** the fix — it controls *when* queued async work runs, not whether the library keeps threads of its own. The real fix was replacing the dependency: keyboard/mouse moved to Raw Input on the app's existing message pump, and gamepads to opt-in, dynamically-loaded XInput — because a library whose thread count you can't know is a library you can't budget. The resulting policy is the portable lesson: **reserve a core per foreign thread with measured busy time, never for thread existence** — GameInput's always-polling worker showed a dose-responsive one-core deficit and earned a reservation; an event-driven waker like an audio backend's device-IO thread (µs of work per ~10 ms wake, and typically TIME_CRITICAL so it wins its preemption instantly at any pool size) does not. **Never reserve for transient wakers** (drivers, DXGI, DWM) — unknowable, universal, microsecond-scale, already handled by preemption. Auto pool size stays `hw−1` for the full JLib stack, audio included.

---

## 📄 License

BSD. Use it, fork it, ship it. A community Linux fork exists — upstreaming is welcome.

**Built for real-time engines. Proven under concurrent load — and under profilers.**
