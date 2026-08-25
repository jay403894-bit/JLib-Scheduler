// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// POSIX asynchronous I/O -- NOT IMPLEMENTED YET.
//
// A DELIBERATE STUB, not something forgotten. Every entry point fails cleanly and IsAvailable()
// answers false, so a caller can choose another path up front instead of discovering the gap one
// failed read at a time.
//
// What it must NOT do, and the reason this file exists rather than an #error: quietly fall back to
// blocking read()/write(). That would appear to work, would block a worker inside a task, and would
// turn a missing backend into an intermittent stall that profiles as "the scheduler is slow".
//
// io_uring on Linux and kqueue on macOS/BSD are intended, with epoll as the portable fallback. See
// the BACKENDS note in IoReactor.h for the rule they have to obey: epoll and kqueue are READINESS
// models and could resume a cancelled waiter immediately, and must not -- an operation ends on a
// completion on every platform, even where the completion has to be synthesised.

// NOT under src/posix/ or src/darwin/, deliberately. Those two diverge wholesale, so a stub placed
// in one of them has to be duplicated into the other -- and the copies drift. That is exactly what
// happened: the src/posix/ copy kept an IoAcceptor::Ready() long after the acceptor stopped using a
// semaphore, and macOS had no copy at all, so Linux broke on a signature and macOS on missing
// symbols. ONE stub, guarded, compiled from the common glob on every platform.
#if !defined(_WIN32)

#include "../include/IoReactor.h"

#include <cerrno>

namespace JLib {

    struct IoReactor::Impl { int unused = 0; };

    IoReactor::IoReactor() : impl(new Impl()) {}
    IoReactor::~IoReactor() { delete impl; }

    IoReactor& IoReactor::Instance() { static IoReactor r; return r; }

    bool IoReactor::IsAvailable() noexcept { return false; }
    bool IoReactor::Register(void*) { return false; }

    // TRUE means "the caller already has its answer and must not suspend", which is exactly right
    // here: there is no backend, so the answer is always Failed and it is available immediately.
    bool IoReactor::SubmitRead(void*, void*, std::uint32_t, std::uint64_t,
                               IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitWrite(void*, const void*, std::uint32_t, std::uint64_t,
                                IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }

    // Sockets: same story as the rest of this file -- fail cleanly rather than pretend. When the
    // io_uring and kqueue backends land these become real; until then a caller gets ENOSYS at the
    // first call instead of a mysterious hang.
    bool IoReactor::InitSockets() { return false; }
    bool IoReactor::RegisterSocket(IoSocket) { return false; }

    bool IoReactor::SubmitRecv(IoSocket, void*, std::uint32_t, std::uint32_t,
                               IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitSend(IoSocket, const void*, std::uint32_t, std::uint32_t,
                               IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitAccept(IoSocket, IoSocket, IoAcceptBuffer*,
                                 IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitConnect(IoSocket, const void*, std::uint32_t,
                                  IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }

    bool IoReactor::SubmitRecvV(IoSocket, const IoBuffer*, std::uint32_t, std::uint32_t,
                                IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitSendV(IoSocket, const IoBuffer*, std::uint32_t, std::uint32_t,
                                IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitRecvFrom(IoSocket, void*, std::uint32_t, std::uint32_t, IoAddress*,
                                   IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitSendTo(IoSocket, const void*, std::uint32_t, std::uint32_t,
                                 const void*, std::uint32_t,
                                 IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    bool IoReactor::SubmitDisconnect(IoSocket, bool, IoRequest*, IoResult* out, Task*, CancelToken) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }

    std::size_t IoReactor::RequestCancel(CancelToken) noexcept { return 0; }
    std::size_t IoReactor::InFlight() const noexcept { return 0; }
    // Both no-ops: there is no backend, so there are no threads to stop and no latch to clear.
    // They exist because TaskScheduler::Join and StartPool call them unconditionally on every
    // platform -- Join must stop the service threads before it clears the pool they push into, and
    // StartPool must undo that so Init works after Join. A stub missing either is a LINK error on
    // POSIX only, which is exactly how this one was found: Windows built clean and all three POSIX
    // jobs failed with `undefined reference to JLib::IoReactor::Start()`.
    void IoReactor::Stop() noexcept {}
    void IoReactor::Start() noexcept {}

    // IoAcceptor: nothing to pre-post without a backend. Start fails, so a caller finds out at
    // startup rather than by waiting forever for a connection that can never be accepted.
    struct IoAcceptor::Impl { int unused = 0; };
    IoAcceptor::~IoAcceptor() { delete impl; }
    bool IoAcceptor::Start(IoSocket, unsigned) { return false; }
    void IoAcceptor::Stop() noexcept {}
    IoSocket IoAcceptor::TryTake() noexcept { return 0; }
    std::size_t IoAcceptor::Outstanding() const noexcept { return 0; }
    std::size_t IoAcceptor::Available() const noexcept { return 0; }
    // Nothing can ever be queued, so the waiter never parks -- TRUE means "your answer is ready",
        // and the answer is "no socket". Returning false here would suspend a caller forever.
    bool IoAcceptor::TakeOrQueue(IoAcceptWaiter*, IoSocket* out, Task*, CancelToken) {
        if (out) *out = 0;
        return true;
    }
    std::size_t IoAcceptor::CancelWaiters(CancelToken) noexcept { return 0; }

    void EjectIoReactor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoReactor*>(ctx)->RequestCancel(token);
    }

    void EjectIoAcceptor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoAcceptor*>(ctx)->CancelWaiters(token);
    }

} // namespace JLib

#endif // !_WIN32
