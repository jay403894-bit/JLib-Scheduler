// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// `co_await` for asynchronous I/O. C++20 -- and the ONLY C++20 in the reactor.
//
//     JLib::Spawn([](void* h, JLib::CancelToken tok) -> JLib::Coro {
//         char buf[4096];
//         JLib::IoResult r = co_await JLib::ReadAsync(h, buf, sizeof buf, 0, tok);
//         if (r.status == JLib::IoStatus::Cancelled) co_return;   // buffer is ours again
//         Consume(buf, r.bytes);
//     }(handle, scope.Token()), &wg, 0, JLib::CorePref::Default, scope.Token().Raw());
//
// EVERYTHING UNDERNEATH IS C++17. The engine -- completion port, in-flight list, backends -- is
// compiled into the core library and never sees <coroutine>; it pushes a `Task*` and does not know
// what kind of task it is. tests/io_c17_test.cpp drives the whole thing from plain C++17 with a
// Native continuation task, and exists so that claim cannot quietly stop being true.
//
// BUT THIS IS NOT SYNTACTIC SUGAR, and calling it that would be wrong in a way that matters. The
// C++17 spelling is not the same code with uglier syntax -- it is a DIFFERENT DECOMPOSITION. Without
// coroutines a function containing a suspension point has to be split in half by hand: everything
// before the read in one function, everything after it in another, with every live variable moved to
// a heap struct because no stack survives the gap. Two sequential reads means three functions and a
// state machine you maintain.
//
// `co_await` is the compiler generating that split and that state struct. The awaiter below is the
// small part; the transformation is the large part, and it is the reason the reactor is built on
// coroutines rather than merely offered through them.
//
// WHY THE REQUEST IS A MEMBER OF THE AWAITER, and this is the load-bearing detail of the file:
//
// The kernel holds a pointer into `req_` until the operation completes. The awaiter lives in the
// COROUTINE FRAME, and the frame stays alive for exactly as long as the coroutine is suspended --
// which, because the only thing that resumes it is the completion, is exactly as long as the kernel
// needs. Putting the request in a local of some enclosing function instead would let that function
// return while the kernel still owned the memory.
//
// This is the same rule that makes IoReactor's cancellation a REQUEST rather than a wake: the
// operation must always end in a completion, because that is what bounds the lifetime. Adding any
// path that resumes early -- a timeout, an "abandon it" -- reintroduces exactly the bug this shape
// exists to prevent. A deadline on I/O therefore cancels the SCOPE and lets the completion arrive;
// see EjectIoReactor in IoReactor.h.

#include "IoReactor.h"
#include "Coroutine.h"

#include <cstdint>

namespace JLib {

    namespace detail {

        // Read and write differ only in which Submit to call, so the awaiter is shared and the
        // direction is a template parameter -- no virtual, no function pointer, nothing to indirect
        // through on a path that already costs a syscall.
        template <bool kWrite>
        class IoAwaiter {
        public:
            IoAwaiter(IoReactor& r, void* handle, void* buf, std::uint32_t len,
                      std::uint64_t offset, CancelToken token) noexcept
                : r_(r), handle_(handle), buf_(buf), len_(len), offset_(offset), token_(token) {}

            IoAwaiter(const IoAwaiter&) = delete;
            IoAwaiter& operator=(const IoAwaiter&) = delete;

            // ALWAYS false, deliberately. The "already cancelled" short-circuit lives inside Submit
            // rather than here, so there is ONE place that decides whether an operation reaches the
            // kernel. Answering it in two places is how the two answers drift apart.
            bool await_ready() const noexcept { return false; }

            // Returns TRUE to suspend. Submit returns true when the answer is already final, so this
            // is its negation -- the same shape as LockAwaiterCancellable over LockAsyncEnqueue.
            //
            // ONCE THIS RETURNS FALSE, TOUCH NOTHING. The operation is queued, and the completion may
            // land and re-push this coroutine onto another worker before this function has returned.
            template <typename P>
            bool await_suspend(std::coroutine_handle<P> h) {
                Task* t = detail::ArmResume(h);
                if constexpr (kWrite) {
                    return !r_.SubmitWrite(handle_, buf_, len_, offset_,
                                           &req_, &result_, t, token_);
                } else {
                    return !r_.SubmitRead(handle_, buf_, len_, offset_,
                                          &req_, &result_, t, token_);
                }
            }

            // By value: small, and the caller usually wants to keep it past this frame.
            [[nodiscard]] IoResult await_resume() const noexcept { return result_; }

        private:
            IoReactor&    r_;
            void*         handle_;
            void*         buf_;
            std::uint32_t len_;
            std::uint64_t offset_;
            CancelToken   token_;

            // MEMBERS, so they live in the coroutine frame. See the note at the top -- this is the
            // whole reason the awaiter exists rather than a free function returning IoResult.
            IoRequest req_{};
            IoResult  result_{};
        };

    } // namespace detail

    // `IoResult r = co_await ReadAsync(handle, buf, len, offset, token);`
    //
    // `token` is the scope the operation belongs to; cancelling it (directly, or by cancelling any
    // scope it is nested inside) asks the OS to cancel, and the await resumes when the COMPLETION
    // arrives. The buffer is yours again the moment the result is in hand, whatever it says.
    //
    // Passing no token means the operation is unscoped and cannot be cancelled -- the same "unscoped
    // work is not cancelled work" rule as everywhere else in the library.
    //
    // `offset` is ignored by handles that do not have one (pipes, sockets); pass 0.
    [[nodiscard]] inline detail::IoAwaiter<false>
    ReadAsync(void* handle, void* buf, std::uint32_t len, std::uint64_t offset = 0,
              CancelToken token = CancelToken{}) noexcept {
        return detail::IoAwaiter<false>(IoReactor::Instance(), handle, buf, len, offset, token);
    }

    // The const_cast is confined here rather than pushed into the awaiter: WriteFile takes a const
    // buffer and the awaiter stores one `void*` for both directions, so exactly one place has to
    // launder it and the backend's signature stays honest about not writing to it.
    [[nodiscard]] inline detail::IoAwaiter<true>
    WriteAsync(void* handle, const void* buf, std::uint32_t len, std::uint64_t offset = 0,
               CancelToken token = CancelToken{}) noexcept {
        return detail::IoAwaiter<true>(IoReactor::Instance(), handle,
                                       const_cast<void*>(buf), len, offset, token);
    }

} // namespace JLib
