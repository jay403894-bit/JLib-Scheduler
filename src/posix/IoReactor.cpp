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

#include "../../include/IoReactor.h"

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

    std::size_t IoReactor::RequestCancel(CancelToken) noexcept { return 0; }
    std::size_t IoReactor::InFlight() const noexcept { return 0; }
    void IoReactor::Stop() noexcept {}

    void EjectIoReactor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoReactor*>(ctx)->RequestCancel(token);
    }

} // namespace JLib
