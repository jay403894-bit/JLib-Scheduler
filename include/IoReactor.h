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
// == WHERE THIS STOPS ==
//
// IN SCOPE, and now complete for Windows: the completion thread(s), Register, read/write,
// accept/connect, UDP, vectored transfers, Cancel and Stop, plus the optional accept pool and
// DisconnectEx -- the two things you only want if you already paid for a listen server.
//
// NOT IN SCOPE, and deliberately never: HTTP, TLS, framing, reliability, NAT traversal, lobbies,
// replication. Nor "the networking driver" in the engine sense -- sockets plus protocol plus game
// tick is a different library with a different lifecycle.
//
// THE EXTENSION POINT IS Submit*, NOT MORE OPCODES HERE. Anything protocol-shaped belongs in a
// JLib::Net built ON this: it gets IoRequest, the completion protocol, cancellation and deadlines
// for free, and this file keeps the property that makes it maintainable -- it knows about handles
// and completions and nothing about what the bytes mean.
//
// The line matters because it is easy to drift across. Every one of the operations above is a
// mechanism the OS exposes; the moment something here needs to know what a message IS, it has
// stopped being a reactor and started being a server, and this stops looking like a scheduler.

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
#include "TaskScheduler.h"   // SchedulerSemaphore: IoAcceptor queues ready connections behind one

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
    // One segment of a scatter/gather transfer. Maps to WSABUF on Windows and iovec on POSIX; the
    // field ORDER differs between those two, so this is converted rather than cast -- a struct that
    // is ABI-identical to both does not exist.
    struct IoBuffer {
        void*         data = nullptr;
        std::uint32_t len  = 0;
    };

    // A socket address, for datagrams. Caller-owned and must outlive the operation, like everything
    // else the kernel is given a pointer to. Big enough for sockaddr_storage, checked in the backend.
    struct IoAddress {
        static constexpr std::size_t kBytes = 128;

        alignas(16) unsigned char bytes[kBytes] = {};
        // IN: capacity. OUT: the length actually written. The kernel needs the address of this for
        // the whole operation, which is why it lives here rather than being an out-parameter.
        std::int32_t len = static_cast<std::int32_t>(kBytes);
    };

    struct IoRequest {
        // Holds the platform's overlapped structure AND the converted segment descriptors, because
        // Windows requires the descriptor array to stay valid for the DURATION of the operation --
        // not just the call. Building it on the submitting stack would be a use-after-free the
        // moment the operation went pending, and it would usually work, which is worse.
        static constexpr std::size_t kMaxVectors = 8;
        static constexpr std::size_t kNativeBytes = 192;

        alignas(16) unsigned char native[kNativeBytes] = {};

        Task*         resume = nullptr;   // pushed when the completion arrives
        IoResult*     out    = nullptr;   // written before the resume; points into the caller
        std::uint32_t token  = 0xFFFFFFFFu;
        void*         handle = nullptr;

        // Accept and connect need a fixup applied to the socket AFTER the completion arrives
        // (SO_UPDATE_ACCEPT_CONTEXT / SO_UPDATE_CONNECT_CONTEXT), so the completion has to know
        // which kind of operation it is draining. `aux` carries the listener for an accept.
        //
        // Kept here rather than inferred from the handle because by completion time there is nothing
        // left to infer from -- and a socket that skipped its fixup does not fail, it misbehaves
        // later, which is the worst way to find out.
        enum class Kind : std::uint8_t { Generic, Accept, Connect };
        Kind          kind = Kind::Generic;
        std::uintptr_t aux = 0;

        // LIVES HERE BECAUSE THE KERNEL WRITES TO IT ON COMPLETION. WSARecv's flags parameter is
        // [in, out] -- it reports things like MSG_PARTIAL when the operation finishes -- so a stack
        // local in the submitting function is a write into a dead frame once the operation goes
        // pending. It usually appears to work, which is exactly the problem.
        std::uint32_t flags = 0;

        IoRequest*    prev = nullptr;     // in-flight list; doubly linked so removal is O(1)
        IoRequest*    next = nullptr;
    };

    // A socket, as an integer rather than a pointer, because that is what both platforms actually
    // use: Windows' SOCKET is a UINT_PTR and a POSIX fd is an int. uintptr_t holds either without a
    // cast at the call site and keeps winsock2.h out of this header.
    using IoSocket = std::uintptr_t;

    // Scratch space AcceptEx writes the local and remote addresses into. THE CALLER PROVIDES IT and
    // it must outlive the operation, same rule as IoRequest -- so it belongs beside the request, in
    // the coroutine frame.
    //
    // The size is not arbitrary: AcceptEx demands at least sizeof(sockaddr) + 16 for EACH address,
    // and the +16 is a Windows requirement rather than padding anyone chose. 128 covers IPv6 twice
    // over, and the backend static_asserts it against the real structures.
    struct IoAcceptBuffer {
        static constexpr std::size_t kBytes = 128;
        alignas(16) unsigned char bytes[kBytes] = {};
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

        // ---- sockets ---------------------------------------------------------------------------
        //
        // WHY THESE ARE NOT JUST Read/Write. On Windows ReadFile does work on a socket, so it was
        // tempting to stop at the file API -- but it cannot carry flags (MSG_PEEK, MSG_OOB), cannot
        // do vectored buffers, and, decisively, ACCEPT AND CONNECT HAVE NO ReadFile ANALOGUE AT ALL.
        // A reactor that cannot accept a connection is not a reactor.
        //
        // Sockets are the app's to create, bind and listen -- this is a reactor, not a networking
        // library. It takes a socket the same way it takes a handle.

        // Once per process, before any socket operation. Resolves AcceptEx and ConnectEx, which are
        // WINSOCK EXTENSIONS with no import library: they are fetched at runtime through WSAIoctl
        // and the pointers differ per provider, so there is no way to call them without this.
        //
        // Does NOT call WSAStartup -- that is process-global policy an application usually already
        // has an opinion about, and a library quietly initialising Winsock behind an app that
        // shuts it down is a hard bug to find. Call WSAStartup yourself first; this returns false
        // if Winsock is not up.
        bool InitSockets();

        bool RegisterSocket(IoSocket s);

        // Same return polarity as SubmitRead: TRUE = answer already final, do NOT suspend.
        bool SubmitRecv(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                        IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitSend(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                        IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        // SCATTER/GATHER. The point is a server writing a header and a body from separate
        // allocations in ONE syscall with no copy to join them -- which is most of what a protocol
        // implementation does.
        //
        // `bufs` is COPIED into the request during submit, so the array itself may be a local. THE
        // MEMORY IT POINTS AT MAY NOT: that is the transfer, and it belongs to the kernel until the
        // completion, same as every other buffer here.
        //
        // At most IoRequest::kMaxVectors segments; more returns Failed rather than silently
        // truncating. Header/body/trailer is three, so eight is generous -- and it is bounded
        // because the descriptors have to live in the request, which has to have a size.
        bool SubmitRecvV(IoSocket s, const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitSendV(IoSocket s, const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                         IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        // ---- datagrams ---------------------------------------------------------------------------
        //
        // Separate from Recv/Send because an unconnected UDP socket has no peer: every datagram
        // carries one. WSARecv on such a socket fails, so this is not an ergonomic wrapper -- it is
        // the only way to do it.
        //
        // `from` receives the sender's address and MUST OUTLIVE THE OPERATION -- the kernel holds a
        // pointer to it and to its length field, so it belongs beside the request in the frame.
        //
        // A DATAGRAM IS A MESSAGE, not a stream. `bytes` is the whole datagram; a buffer too small
        // loses the remainder and reports WSAEMSGSIZE rather than returning a partial read, which is
        // UDP's semantics and not something this layer papers over.
        bool SubmitRecvFrom(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                            IoAddress* from,
                            IoRequest* req, IoResult* out, Task* resume, CancelToken token);
        bool SubmitSendTo(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                          const void* addr, std::uint32_t addrLen,
                          IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        // AcceptEx. `accepted` must already be a created, UNBOUND socket of the same family and type
        // as `listener` -- Windows requires the socket to exist before the accept starts, which is
        // the point of the design: it is what removes the syscall from the completion path.
        //
        // On success the reactor applies SO_UPDATE_ACCEPT_CONTEXT for you. Without it the accepted
        // socket does not inherit the listener's properties and getpeername fails on it -- a
        // documented Windows requirement that is very easy to omit and produces a socket that looks
        // connected and misbehaves.
        bool SubmitAccept(IoSocket listener, IoSocket accepted, IoAcceptBuffer* addrs,
                          IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        // ConnectEx. `s` MUST ALREADY BE BOUND -- to INADDR_ANY:0 if you have no preference. That is
        // ConnectEx's rule, not this library's, and an unbound socket fails with WSAEINVAL rather
        // than binding itself.
        //
        // On success the reactor applies SO_UPDATE_CONNECT_CONTEXT, for the same reason as accept.
        bool SubmitConnect(IoSocket s, const void* sockaddr, std::uint32_t sockaddrLen,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token);

        // DisconnectEx. Closes the connection and, with `reuse`, hands the SOCKET back in a state
        // where it can be given to AcceptEx or ConnectEx again.
        //
        // WHAT THIS IS FOR. Under a high connect rate, socket creation and teardown become the cost:
        // every closesocket walks kernel structures and every socket() rebuilds them, and a
        // connection that lasts one request pays both. Reuse skips the pair entirely, which is why a
        // pre-posted acceptor and this belong together -- the acceptor needs a fresh socket per
        // accept, and this is where fresh ones come from without asking the kernel.
        //
        // `reuse` is TF_REUSE_SOCKET. A socket disconnected with it MUST NOT be closed and re-created
        // -- that would waste the whole point -- and must not be used for anything else until it is
        // handed to an accept or a connect.
        //
        // Like every other operation here, it ends in a COMPLETION: the socket is not reusable when
        // this returns, it is reusable when the result is in hand.
        //
        // TWO THINGS A REUSED SOCKET STILL IS, both learned the hard way and neither obvious:
        //
        //   IT IS STILL BOUND. Binding it again is WSAEINVAL, and the failed bind then takes the
        //   following ConnectEx down with it.
        //
        //   IT IS STILL BOUND TO THE SAME LOCAL PORT. Reconnecting to the SAME remote endpoint
        //   therefore recreates an identical 4-tuple and collides with the previous connection in
        //   TIME_WAIT -- ERROR_DUP_NAME, intermittently, depending on how fast that drains. A
        //   connection pool reusing sockets against ONE server has to account for this; reuse is
        //   straightforward when the next peer is a different one.
        bool SubmitDisconnect(IoSocket s, bool reuse,
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

    // ================================================================================================
    // IoAcceptor -- ACCEPTS THAT ARE ALREADY POSTED WHEN THE CONNECTION ARRIVES.
    //
    // WHAT IT FIXES. Bare AcceptAsync starts its AcceptEx only once somebody awaits it, and the
    // caller must have created the accept socket first. Under a burst, connections sit in the
    // listen backlog while the server gets round to asking for them -- and each one then waits on a
    // socket() call. The whole point of AcceptEx is that the socket exists BEFORE the connection
    // does; using it one-at-a-time throws that away.
    //
    // So this keeps `depth` accepts outstanding at all times. A connection arriving finds a posted
    // accept and a ready socket, and the slot re-posts itself immediately afterwards.
    //
    // THE OWNERSHIP INVERSION, and it is the one place this library breaks its own rule. Everywhere
    // else the CALLER owns the IoRequest, because the awaiting frame bounds its lifetime. A
    // pre-posted accept has no awaiting frame -- that is the entire idea -- so the acceptor owns the
    // requests, the sockets and the address buffers, and its destructor is what bounds them. Stop()
    // cancels every outstanding accept and DRAINS the completions before releasing anything, for
    // the same reason IoReactor::Stop does: the kernel is holding pointers into that storage.
    //
    // Ready connections queue behind a semaphore, one permit each. That makes waiting for a
    // connection an ordinary cancellable wait, with scopes and deadlines working on it unchanged.
    // ================================================================================================
    class IoAcceptor {
    public:
        IoAcceptor() noexcept = default;
        ~IoAcceptor();

        IoAcceptor(const IoAcceptor&) = delete;
        IoAcceptor& operator=(const IoAcceptor&) = delete;

        // `listener` must already be bound, listening and registered with the reactor. The accept
        // sockets are created to match its family and protocol, queried from the listener itself
        // rather than passed in -- a mismatch there fails inside AcceptEx with a bad-argument error
        // that says nothing about which argument.
        bool Start(IoSocket listener, unsigned depth = 8);

        // Cancels every outstanding accept, waits for their completions, and closes the unused
        // sockets. Idempotent. Ready-but-unclaimed connections are closed too -- nobody is coming
        // for them, and leaking a socket per unclaimed connection is worse than refusing it.
        void Stop() noexcept;

        // Non-blocking. Returns 0 when nothing is ready. Normally reached through AcceptAsync in
        // IoAsync.h, which waits on the semaphore below first.
        IoSocket TryTake() noexcept;

        // One permit per ready connection. Exposed so a waiter can use the ordinary cancellable
        // wait -- which is what makes deadlines and scopes work on "wait for a connection" without
        // this class knowing anything about either.
        SchedulerSemaphore& Ready() noexcept;

        std::size_t Outstanding() const noexcept;   // accepts posted and not yet completed
        std::size_t Available() const noexcept;     // accepted, waiting to be taken

    private:
        struct Impl;
        Impl* impl = nullptr;
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
