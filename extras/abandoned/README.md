# Abandoned: the C++20 coroutine layer

**JLib::Scheduler is a fibers-only runtime as of 5.0, and that includes I/O.** These files are the
C++20 coroutine layer they replaced. They are kept for reference and are **not built, not tested and
not supported** -- nothing in `CMakeLists.txt` refers to them, and they will bit-rot.

| file | what it was |
|---|---|
| `Coroutine.h` | `Spawn()`, the promise type, and the frame pooling that put coroutine frames on the task slab |
| `Future.h` | a `co_await`-able future |
| `IoAsync.h` | the awaiter over the reactor -- `co_await` sugar on the completion-first API |
| `coroutine_test.cpp`, `future_test.cpp`, `coro_epoch_contract_test.cpp`, `hazard_coro_suspend_test.cpp` | their tests |
| `coroutine_bench.cpp`, `frame_class.cpp`, `udp_latency_bench.cpp` | their benchmarks |

## Why they went

A fiber and a coroutine were two answers to one question -- how does a task wait without blocking a
worker -- and carrying both meant every suspension point in the scheduler had two shapes to be
correct in. `Thread.cpp`'s dispatch, the epoch contract, the hazard contract and the reactor each
paid for that twice.

**Once I/O ran on fibers, the coroutine path had nothing left that it alone could do.** It was the
awaiter that justified the C++20 dependency, and fiber I/O does the same job with no dependency at
all.

**The runtime is now pure C++17.** That was already the headline -- "C++17 core, C++20 coroutines as
an optional header" -- and the optional half is what made the C++20 tier, the `JLIBSCHED_COROUTINES`
option, the split standard in `CMakeLists.txt` and `TaskType::Coroutine` all necessary. None of them
are, now. `TaskType` is `Native` and `Fiber`.

## If you want them back

They were removed rather than broken, so the code is intact as of the commit that moved it here --
but they were written against a `TaskType` that had a third value and a scheduler that dispatched it.
Reviving them is a real port, not a `#include`.
