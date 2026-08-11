# JLib::Scheduler

A fiber-based, work-stealing task scheduler for real-time engines. Hand-written context switching,
lock-free Chase-Lev deques, a slab-allocated task system, frame DAGs with logic gates, and
hybrid-core aware placement.

Windows x64 (MSVC) · Linux x86-64 · Linux/Android AArch64 · macOS Apple Silicon · C++17 · BSD

[![CI](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/jay403894-bit/JLib-Scheduler/actions/workflows/ci.yml)

I built this scheduler to solve the problem of scheduling for my custom 2d/3d engine  -- it was built to be the backbone of multithreaded simulation engines.
I needed tasks that could wait on a gpu fence without parking a worker thread, enkiTS and taskflow cannot do this, mamrl can but was archived in April.

---

## Measured

this was tested on my machine, third party tests have come back and some are faster than mine depending on hardware and platform.

| | i9-13900K, Release |
|---|---|
| Task enqueue → dequeue latency | 6.3 µs |
| 5-node frame DAG (build, validate, execute) | 31.9 µs |
| `Task` struct size | 64 bytes, one cache line, `static_assert`-enforced |
| Fiber stacks | 64 KB standard / 512 KB heavy, contiguous arena, guard-paged |
| Steal protocol | single-item Chase-Lev CAS |

Run them yourself with `SchedulerBench` — it takes an affinity policy argument and defaults to the
same one the library does.

<<<<<<< HEAD
## Build
=======
Public C++ job systems make you choose: task graphs **without** fibers ([Taskflow], enkiTS), fibers **without** maintenance (marl — archived), or GDC-talk fiber schedulers that never ship outside their studios. This is the missing combination, maintained and BSD-licensed:

|  | enkiTS | Taskflow | marl | **JLib::TaskScheduler** |
|---|---|---|---|---|
| Work-stealing | ✅ | ✅ | ✅ | ✅ |
| Fibers (suspend/resume *inside* tasks) | ❌ | ❌ | ✅ | ✅ |
| Dependency DAG w/ AND/OR gates | partial | ✅ | ❌ | ✅ |
| Cache/SMT topology-aware stealing | ❌ | ❌ | ❌ | ✅ |
| Hybrid P/E-core placement | ❌ | ❌ | ❌ | ✅ |
| Maintained | ✅ | ✅ | ❌ archived | ✅ |

The trade: **modern 64-bit hardware assumed** — x86-64 or AArch64, Windows/MSVC or Linux/GCC-Clang, nothing else. Constraining the problem is what makes one-person excellence possible — see *Requirements*.

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

**Requirements:** x86-64 or AArch64, and one of **Windows 10+ with MSVC** (x86-64 only; C++17+, MASM `ml64` ships with VS), **Linux/Android with GCC/Clang**, or **macOS with AppleClang** (C++17+, GAS for the context switch). CMake 3.21+ optional everywhere. The benchmark is C++17 too — there is no longer a C++20 caveat attached to that claim.

**What "1.0" covers.** The supported API is **`TaskScheduler.h`, `Task.h` and `TaskDAG.h`** — those follow semver, and nothing in them breaks without a 2.0. The install ships the whole `include/` directory because the supported headers need those types to compile, but everything else (`TaskDeque.h`, `LockFreeList.h`, `ThreadLocalCache.h`, `Epochs.h`, `TaskAllocator.h`, `Fiber.h`, `Thread.h`, `Context.h`, `Topology.h`, `platform.h`, and the vendored moodycamel queues) is **implementation detail and may change in any release**. If you find yourself including one of those directly, that is a missing feature in the supported API — please open an issue rather than depending on it.

**Deliberate limitations — read before adopting:**

- **Four verified targets, and the ABI is the reason there are only four.** The context switch is hand-written assembly per ABI — MASM for Win64, GAS for System V and for AAPCS64 — so a new *architecture* needs a new `ContextSwitch` and a matching `Fiber::Init`, not a compiler flag. Verified in CI on every push — **Windows x64 (MSVC)**, **Linux x86-64 (GCC)**, **Linux AArch64 (GCC)** and **macOS arm64 / Apple Silicon (AppleClang)** — plus **AArch64 on Android/Termux (Clang)** by hand. The full benchmark suite passes on all of them, fibers suspending and resuming through the hand-written switch under the real scheduler; the AAPCS64 switch additionally has a standalone ABI harness (`tests/fibertest_aarch64.cpp`) run at `-O0` and `-O2` on both ARM64 platforms. That the ARM64 results agree across **three toolchains, three libcs and two object formats** — GCC/glibc/ELF, Clang/bionic/ELF and AppleClang/libc++/Mach-O — is what makes the ABI claim worth anything. Raspberry Pi is the same configuration as the CI ARM64 runner (Debian-family aarch64, glibc) and needs nothing extra. 32-bit targets are not supported and are not planned. Everything platform-specific lives in `src/win32/`, `src/posix/` (Linux, Android) and `src/darwin/` (macOS), with the ABI layer under `src/posix/<arch>/` shared by every POSIX target — the calling convention belongs to the instruction set, not the kernel. `include/platform.h` is the single place that tests OS *and* architecture.
- **`ucontext` is not used, and that is a measurement.** `swapcontext` saves and restores the signal mask — a `sigprocmask` **syscall per switch** — at **120 ns against 8 ns** for the hand-written version. Its POSIX deprecation is the lesser reason.
- **Workers are bound to their core** (worker *i* → logical CPU *i+1*, main on CPU 0) under `Hard`/`PhysicalOnly`. This is what makes the topology maps (SMT sibling, LLC cluster, P/E class) *true* rather than guesses. Under the default `Ideal`, Windows uses `SetThreadIdealProcessor` (a hint) and Linux binds to the whole **LLC domain** — a mask, which Windows has no equivalent of. That mask is as tight as the hardware warrants: on **multi-L3** parts (Ryzen CCDs, Threadripper, EPYC) it genuinely binds, which is where it matters, since migrating across cache domains costs inter-die latency on every steal; on **single-L3** parts the domain is every CPU, so it binds nothing — correct rather than missing, as there is no domain to protect and binding would only add the rigidity that measured ~45% worse. It does **not** keep `siblingQIndex` true on Linux; the kernel can still migrate within the LLC. See *Design Decisions*.
- **Auto pool size is `hardware_concurrency − 1`** (main on CPU 0, workers on the rest) — and this is only safe because the JLib stack keeps *busy* foreign threads at zero by construction: input is Raw Input riding the app's message pump (zero threads; gamepad support is opt-in and dynamically loaded precisely because XInput spawns its own). **The rule: reserve one core per foreign thread with *measured busy time* — not per thread that merely exists.**  JLib audio's remaining foreign thread (its backend's device-IO thread) is the opposite case — event-driven, ~100 wakes/s, microseconds of memcpy per wake — and measurably costs nothing, so audio does **not** change the default. `Init(N)` honors explicit sizes up to full `hardware_concurrency`.
- **Transient oversubscription is accepted, on purpose.** Pinned workers can't dodge the threads no user-mode process controls — GPU driver workers, DXGI, DWM — which wake for microseconds at unpredictable times in *every* process on the machine. Desktop Windows has no core isolation (that's a console feature), so the only correct handling is the one the OS already provides: brief preemption. Profilers will faithfully report this: VTune's *Thread Oversubscription* metric counts spin-waiting threads as running, and in a mostly-idle game nearly all CPU time **is** short spin/wake bursts — so the metric reads high while sampled concurrency never approaches core count and frame times don't move with pool size. The number is real by Intel's definition; it describes a designed trade (spin-waits buy the latency figures above), not a defect.
- **`CorePref::P` / `CorePref::E` do nothing on Linux or AARCH64 platforms — leave tasks on `CorePref::Default`.** P/E classification is Windows-only: it reads each core's `EfficiencyClass` from `GetLogicalProcessorInformationEx`, and Linux has no single equivalent (the signals that exist are a perf-driver artifact, `/sys/devices/cpu_core` vs `cpu_atom`, or CPPC `highest_perf`). Linux therefore reports every core as equal — including on **big.LITTLE / DynamIQ AArch64**, where the heterogeneity is realer than on any x86 hybrid part (a phone typically spans three capacity tiers, not two) and is correspondingly *more* of a missed opportunity. Note also that Android is the wrong place to add it: the platform's cgroups own thread placement, so affinity requests from an unprivileged app are routinely ignored. **macOS is the interesting exception and is the likely first mover.** It has no thread-affinity API on arm64, but that is because Apple provides a *different* mechanism for the same intent: QoS classes. `pthread_set_qos_class_self_np` with `QOS_CLASS_USER_INITIATED` biases a thread toward P-cores and `QOS_CLASS_UTILITY` toward the efficiency cluster. Crucially that is a hint which does **not** require knowing which logical CPU is which — so the missing piece on macOS (sysctl publishes per-performance-level CPU *counts*, `hw.perflevel0/1.logicalcpu`, but no index→level mapping) stops being a blocker: size the P/E worker sets from the counts and let QoS carry the intent, with the existing `CorePref` routing unchanged. **The scheduler nevertheless sets no QoS at all, and that is a decision rather than a gap.** Workers inherit the QoS of whatever thread calls `Init()`, and that inheritance *is* the configuration mechanism — an app wanting a particular class sets its own before initialising and the whole pool follows, with no API to learn. QoS is a power and thermal choice as much as a scheduling one: a host deliberately running at `UTILITY` because it is a background exporter has decided something about battery and fan noise, and a job system that silently promoted its workers would be overriding it. That is the same argument that justifies `AffinityPolicy::None` elsewhere — an embedded library does not get to make that call. If explicit tiering is ever added it has to be opt-in and gated *more* tightly than affinity is, because stealing is preference-blind and a `Default` task landing on a `UTILITY` worker is a deprioritised, throttleable thread rather than merely a slower core. This is **safe** rather than broken — placement preference is a hint, so an empty class set spills to the other class and the task runs full-pool — but it means an explicit `P`/`E` request is silently a no-op there. Do not build a design around it and expect it to hold cross-platform. Class routing is opt-in and off by default, so this affects nobody who has not asked for it.
  Arguably it matters less on Linux anyway: the kernel does hybrid placement itself (ITMT), so a class table there second-guesses a scheduler that already knows — whereas on Windows nothing else is making that call.
- **Processor group 0 only** (≤ 64 logical CPUs). Fine for desktops/workstations; dual-socket monsters need work this project doesn't do.
- **Tasks are 256-byte slab slots.** Lambda captures beyond ~192 bytes fail a `static_assert` — capture pointers, not payloads.

---

## 🚀 2. Quick Start

**CMake, either platform:**
-> 8e682e7f4c08ab7c52b2ae1a1e03e83ec883295a

```
cmake -B build -DCMAKE_BUILD_TYPE=Development
cmake --build build -j
```

Or open `Scheduler.sln` in Visual Studio. Three build types: `Debug`, `Release`, and `Development`
(optimized, with symbols and assertions live — it deliberately does not define `NDEBUG`).

To consume an installed copy, `find_package(JLibScheduler)` and link `JLib::Scheduler`.

Adding the sources to your own build directly: take `src/*.cpp` plus exactly one platform directory.
That is either `src/win32/` with its `ContextSwitch.asm`, or `src/posix/` (or `src/darwin/` on
macOS) plus exactly one architecture subdirectory, `src/posix/x86_64/` or `src/posix/aarch64/`.
Never two of the same kind — they define the same symbols, and a static library will not diagnose
that. It silently links whichever one it reaches first.

## Using it

```cpp
#include <TaskScheduler.h>

int main() {
    JLib::TaskScheduler::Init();               // auto pool size (hw-1)
    auto& sched = JLib::TaskScheduler::Instance();

    // Fire and forget. noFiber is the default: runs inline on a worker, no fiber, no switch.
    sched.Push([] { HeavyMath(); });

    // Fork-join: create tasks against a WaitGroup, then wait.
    JLib::WaitGroup wg;
    for (int i = 0; i < 8; ++i) {
        auto* t = sched.CreateTask([i] { Chunk(i); });
        t->waitGroup = &wg;
        wg.n.fetch_add(1, std::memory_order_release);
        sched.Push(t);
    }
    sched.WaitFor(wg);

    // Data-parallel loop. Decides serial vs parallel by timing a prefix, not by element count.
    sched.ParallelFor(0, 1'000'000, 4096, [](int a, int b) {
        for (int i = a; i < b; ++i) out[i] = std::sqrt((float)i);
    });

    sched.Join();
}
```

The one thing to get right: a task that will call `WaitFor` must be created with `noFiber = false`.
`noFiber` defaults to true, and a task with no fiber under it cannot suspend — it fail-fasts with no
message. See [DESIGN.md](DESIGN.md#integration-contracts).

## How it compares

| | enkiTS | Taskflow | marl | this |
|---|---|---|---|---|
| Work-stealing | yes | yes | yes | yes |
| Suspend/resume inside a task | no | no | yes | yes |
| Dependency DAG with AND/OR gates | partial | yes | no | yes |
| Cache/SMT topology-aware stealing | no | no | no | yes |
| Maintained | yes | yes | archived Apr 2026 | yes |

marl and FiberTaskingLib run every task on a fiber. Here fibers are opt-in, so middleware written
for an ordinary thread pool works unchanged — Jolt Physics runs through a `JPH::JobSystem` adapter
and never learns fibers exist. [Why that matters](DESIGN.md#the-hybrid-is-a-correctness-boundary-not-a-performance-dial).

## Limitations

- x86-64 or AArch64 only, 64-bit only, and Windows means MSVC. The context switch is hand-written
  per ABI, so a new architecture is real work rather than a compiler flag. Windows on ARM64 is
  refused explicitly.
- Processor group 0 only, so at most 64 logical CPUs. Dual-socket machines need work this does not
  do.
- Tasks are 256-byte slab slots. Lambda captures past ~192 bytes fail a `static_assert` — capture
  pointers, not payloads.
- `CorePref::P` / `CorePref::E` do nothing outside Windows. Safe, since preference is a hint that
  spills, but do not build a design around it.

The reasoning behind each of these, and the decisions that were tried and removed, is in
[DESIGN.md](DESIGN.md).

## Versioning

1.0.0. The supported API is `TaskScheduler.h`, `Task.h` and `TaskDAG.h`; those follow semver and do
not break without a 2.0. Every header is installed because the supported ones need them to compile,
but the rest are implementation detail and may change in any release. If you need something only
reachable through one of those, that is a missing feature — open an issue rather than depend on it.

[CHANGELOG.md](CHANGELOG.md) has the release history and the negative results.

## License

BSD 3-Clause. Use it, fork it, ship it.
