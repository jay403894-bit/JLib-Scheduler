// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// Asynchronous I/O -- the ENGINE. C++17, compiled into the core library.
//
// THERE IS NO BLOCKING CALL HERE, and that is the design rather than an omission. Submit returns
// immediately; the completion pushes a Task. The only user-facing spelling is `co_await` in
// IoAsync.h, which is what the coroutine mode was built for: I/O is work that is SUSPENDED
// essentially always, and the cost of being suspended is the whole game.
//
// WHY THERE IS NO FIBER API. One was written, measured against the design, and deleted. A parked
// fiber costs a 64 KB stack and one slot from a pool whose own header calls itself "the exact upper
// bound on tasks that can be parked" -- against a coroutine frame of 64 to 256 bytes from the task
// slab. Roughly 500x per in-flight operation, in the one dimension a reactor lives on, and the cap
// is worse than the memory: a reactor's steady state IS thousands of parked operations, so fibers
// would not degrade, they would stop.
//
// Keeping it as a "fallback" was considered and rejected for a sharper reason: it would not fail, it
// would get SLOW. A missing C++20 compiler is a build error fixed in a minute; a fiber-backed
// reactor is a throughput wall found with a profiler weeks later. The deleted version proved how
// easy the trap is -- it identified its caller with `thread->currentFiber`, which is NULL for a
// coroutine (coroutine tasks ride the Native path and never get a fiber), so a coroutine calling it
// fell into the bare-thread branch and SPUN A WORKER for the duration of the I/O. Silently, on
// exactly the caller it existed for.
//
// C++20 IS CONFINED TO THE AWAITER. Everything here -- the completion port, the in-flight list, the
// backends -- is ordinary C++17 in .cpp files. The core pushes a `Task*` and never learns it is a
// coroutine, the same type erasure Coroutine.h relies on to keep <coroutine> out of the core. An app
// needs C++20 only if it wants to `co_await`.
//
// == COMPLETION-FIRST, AND WHY CANCELLING I/O IS A REQUEST RATHER THAN A WAKE ==
//
// This is the one place the library's usual cancellation shape does not apply, and getting it wrong
// corrupts memory rather than merely misbehaving.
//
// Every other primitive cancels by WAKING the waiter: Event, semaphore and condition variable each
// eject a parked task and it returns Cancelled. Safe, because nothing outside the process holds a
// reference to anything.
//
// An in-flight read is different: THE KERNEL OWNS YOUR BUFFER until the operation completes. Asking
// it to stop -- CancelIoEx, io_uring's cancel -- does not end the operation; it asks for an earlier
// completion, which may or may not arrive before the transfer finishes anyway. Resuming the task at
// that moment hands it back a buffer the kernel is still writing into.
//
// So cancellation is two-phase, and the second phase is the OS's to deliver:
//
//   1. Somebody cancels the scope (a peer disconnects, a Deadline fires). The reactor asks the OS to
//      cancel every in-flight operation under that scope. NOTHING IS RESUMED.
//   2. The OS completes the operation -- cancelled, or successfully if it was too late to stop. The
//      reactor drains that completion and resumes the task THEN.
//
// The operation always ends in a completion. Cancellation changes how long that takes, not whether
// it happens -- which is why the buffer is safe to reuse the moment the result is final, whatever
// the result says.
//
// EXACTLY ONE COMPLETION MEANS EXACTLY ONE WAKER, which makes this simpler than the in-process
// primitives rather than harder. The Event slot table, the semaphore queue and the condition
// variable queue each need an arbiter so a cancel and a signal racing cannot both wake one waiter.
// Here the OS is the arbiter: one completion per operation, so there is no race to arbitrate.
//
// == THE THREAD ==
//
// One dedicated thread drains completions and pushes the woken tasks. Not a worker, for the same
// reason the timer is not: draining a completion port is a blocking wait, and a worker that blocks
// on something outside the scheduler can absorb a lost wakeup. Call
// TaskScheduler::SetReserveIoCore(true) before Init so the pool leaves it a core.
//
// == BACKENDS, AND THE ONE RULE THEY ALL HAVE TO OBEY ==
//
// Windows/IOCP is implemented. io_uring, epoll and kqueue are intended and not written. POSIX is a
// STUB that reports IsAvailable() == false and fails cleanly -- it does not pretend, and it does not
// silently do synchronous I/O behind your back.
//
// IOCP and io_uring are COMPLETION models: you hand the kernel a buffer, it transfers, it tells you.
// epoll and kqueue are READINESS models: the kernel says the fd is readable and YOU do the read, so
// it never owns your buffer at all.
//
// A readiness backend COULD therefore resume a cancelled waiter immediately. IT MUST NOT. The
// contract above is the strict one on purpose, so one piece of caller code is correct everywhere: a
// readiness backend stops watching the fd, transfers nothing, and delivers a Cancelled result
// through the same completion path. Written down because the temptation runs the wrong way -- the
// shortcut exists only on the readiness side, and taking it would make cancellation observably
// faster on Linux while leaving the same program racing on Windows.

#include "CancelToken.h"

#include <cstddef>
#include <cstdint>

namespace JLib {

    struct Task;

    enum class IoStatus : std::uint8_t {
        Pending,      // submitted, no completion yet
        Completed,    // the transfer finished; `bytes` is valid
        Cancelled,    // ended early because the scope was cancelled; NOTHING was transferred
        Failed        // the OS refused or the transfer errored; `error` says which
    };

    // NOTHING WAS TRANSFERRED on Cancelled, the same rule as every other Cancelled here: a cancelled
    // acquire took no permit, a cancelled wait holds no lock, a cancelled read moved no bytes. A
    // caller treating Cancelled as "some bytes, maybe" would read buffer the kernel never filled.
    struct IoResult {
        IoStatus      status = IoStatus::Pending;
        std::uint32_t bytes  = 0;
        std::int32_t  error  = 0;      // platform error code; 0 unless status == Failed

        bool Ok() const noexcept { return status == IoStatus::Completed; }
    };

    // One in-flight operation. THE CALLER OWNS THE STORAGE and it must outlive the operation --
    // which for a coroutine means making it a member of the awaiter, so it lives in the coroutine
    // frame and is alive for exactly as long as the frame is suspended.
    //
    // NOT ALLOCATED BY THE REACTOR, deliberately. The kernel holds a pointer into this object until
    // the completion arrives, so an owner that can disappear is the whole hazard; making the caller
    // provide it puts the lifetime somewhere it can be reasoned about, and costs zero allocations on
    // the I/O path. It is also why there is no "abandon this operation" call: the frame cannot go
    // away before the completion, so there is nothing to abandon it from.
    //
    // The native part is an opaque block so this header never needs windows.h. Its size is checked
    // against the real thing by a static_assert in the backend.
    struct IoRequest {
        static constexpr std::size_t kNativeBytes = 64;

        alignas(16) unsigned char native[kNativeBytes] = {};

        Task*         resume = nullptr;   // pushed when the completion arrives
        IoResult*     out    = nullptr;   // written before the resume; points into the caller
        std::uint32_t token  = 0xFFFFFFFFu;
        void*         handle = nullptr;

        IoRequest*    prev = nullptr;     // in-flight list; doubly linked so removal is O(1)
        IoRequest*    next = nullptr;
    };

    class IoReactor {
    public:
        static IoReactor& Instance();

        // False where this is not implemented, so a caller can choose another path up front instead
        // of discovering the gap one failed read at a time.
        static bool IsAvailable() noexcept;

        // Associate a handle. Once per handle, BEFORE any operation, and the handle must have been
        // opened for overlapped I/O. A handle cannot be un-associated -- an OS limitation, not a
        // choice -- so it stays associated until closed.
        bool Register(void* handle);

        // Submit. THE RETURN HAS THE SAME POLARITY AS SchedulerMutex::LockAsyncEnqueue, and matching
        // it is the point: TRUE means the caller ALREADY HAS ITS ANSWER and must NOT suspend --
        // `*out` is final. FALSE means the operation is queued, `req` and `*out` belong to the
        // reactor until the completion, and the caller must stay suspended and touch neither.
        //
        // So an awaiter reads `return !SubmitRead(...)`, exactly as the mutex and semaphore ones do.
        //
        // `resume` is pushed to the scheduler when the completion arrives; null means nobody is
        // waiting and the result is simply written.
        //
        // An already-cancelled token returns TRUE with Cancelled WITHOUT SUBMITTING -- the one case
        // where an I/O cancel is immediate, because the kernel never took the buffer.
        //
        // DO NOT SET FILE_SKIP_COMPLETION_PORT_ON_SUCCESS ON A REGISTERED HANDLE. Everything above
        // rests on "once queued, a completion is guaranteed" -- including when the kernel call
        // returned success synchronously, because Windows queues a packet anyway. That flag removes
        // the packet for synchronous success, which is a tempting throughput win and would silently
        // break the lifetime argument: the awaiter would stay suspended forever waiting for a
        // completion that is never coming, with `req` and the caller's buffer pinned behind it. If
        // that optimisation is ever wanted, Submit has to detect synchronous success and return TRUE
        // instead -- it cannot be enabled on its own.
        bool SubmitRead (void* handle, void* buf, std::uint32_t len, std::uint64_t offset,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitWrite(void* handle, const void* buf, std::uint32_t len, std::uint64_t offset,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        // Ask the OS to cancel every in-flight operation under `token`, INCLUDING those under scopes
        // nested inside it. Returns how many were asked about -- not how many were cancelled, which
        // is decided by whether the transfer had already finished and is not knowable here.
        //
        // NOTHING IS RESUMED BY THIS. Each of those operations still completes and its task is
        // pushed by the completion, exactly as it would have been.
        //
        // An invalid token means every in-flight operation, for teardown.
        std::size_t RequestCancel(CancelToken token) noexcept;

        // Submitted, not yet completed. Diagnostics and tests; racy by nature.
        std::size_t InFlight() const noexcept;

        // Idempotent. In-flight operations are cancelled and DRAINED before the thread goes --
        // leaving the kernel holding a buffer whose owner has gone is the corruption this file
        // exists to prevent.
        void Stop() noexcept;

        struct Impl;

    private:
        IoReactor();
        ~IoReactor();
        IoReactor(const IoReactor&) = delete;
        IoReactor& operator=(const IoReactor&) = delete;

        Impl* impl;
    };

    // The TimerEject for a deadline on I/O:
    //
    //     CancelScope op(conn.Token());
    //     Deadline d(2s, op.Token(), EjectIoReactor, &IoReactor::Instance());
    //
    // NOTE WHAT IT DOES NOT DO. Every other eject in Timer.h resumes a parked waiter; this one only
    // asks the OS, and the operation ends when its COMPLETION arrives. The eject contract is really
    // "make progress toward ending the wait", and for I/O that is a request rather than a wake --
    // which is exactly why TimerEject is a function pointer instead of the timer knowing primitives.
    void EjectIoReactor(void* ctx, CancelToken token);

} // namespace JLib
