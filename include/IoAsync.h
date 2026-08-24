// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// `co_await` for asynchronous I/O. C++20 -- and the ONLY C++20 in the reactor.
//
//     JLib::Spawn([](JLib::IoSocket s, JLib::CancelToken tok) -> JLib::Coro {
//         char buf[4096];
//         JLib::IoResult r = co_await JLib::RecvAsync(s, buf, sizeof buf, 0, tok);
//         if (r.status == JLib::IoStatus::Cancelled) co_return;   // buffer is ours again
//         Consume(buf, r.bytes);
//     }(sock, scope.Token()), &wg, 0, JLib::CorePref::Default, scope.Token().Raw());
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
#include <utility>

namespace JLib {

    namespace detail {

        // ONE awaiter for all six operations. They differ only in which Submit to call and what to
        // pass it, so the difference is captured in a stored callable rather than in six near-copies
        // of the suspend protocol -- which is where a divergence would eventually hide.
        //
        // The callable is stored BY VALUE, so it lives in the coroutine frame alongside the request.
        // It is a few words of captured arguments; there is no allocation and no std::function.
        template <typename Fn>
        class IoOpAwaiter {
        public:
            explicit IoOpAwaiter(Fn fn) noexcept : fn_(std::move(fn)) {}

            IoOpAwaiter(const IoOpAwaiter&) = delete;
            IoOpAwaiter& operator=(const IoOpAwaiter&) = delete;

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
                return !fn_(&req_, &result_, t);
            }

            // By value: small, and the caller usually wants to keep it past this frame.
            [[nodiscard]] IoResult await_resume() const noexcept { return result_; }

        private:
            Fn fn_;

            // MEMBERS, so they live in the coroutine frame. See the note at the top -- this is the
            // whole reason the awaiter exists rather than a free function returning IoResult.
            IoRequest req_{};
            IoResult  result_{};
        };

        template <typename Fn>
        IoOpAwaiter<Fn> MakeIoAwaiter(Fn fn) noexcept { return IoOpAwaiter<Fn>(std::move(fn)); }

    } // namespace detail

    // ---- files, pipes, anything with a handle --------------------------------------------------
    //
    // `token` is the scope the operation belongs to; cancelling it -- directly, or by cancelling any
    // scope it is nested inside -- asks the OS to cancel, and the await resumes when the COMPLETION
    // arrives. The buffer is yours again the moment the result is in hand, whatever it says.
    //
    // Passing no token means the operation is unscoped and cannot be cancelled, the same "unscoped
    // work is not cancelled work" rule as everywhere else in the library.
    //
    // `offset` is ignored by handles that do not have one (pipes, sockets); pass 0.

    [[nodiscard]] inline auto ReadAsync(void* handle, void* buf, std::uint32_t len,
                                        std::uint64_t offset = 0,
                                        CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRead(handle, buf, len, offset, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto WriteAsync(void* handle, const void* buf, std::uint32_t len,
                                         std::uint64_t offset = 0,
                                         CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitWrite(handle, buf, len, offset, r, o, t, token);
        });
    }

    // ---- sockets --------------------------------------------------------------------------------

    [[nodiscard]] inline auto RecvAsync(IoSocket s, void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRecv(s, buf, len, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendAsync(IoSocket s, const void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitSend(s, buf, len, flags, r, o, t, token);
        });
    }

    // `accepted` must already exist, unbound, same family and type as the listener; `addrs` must
    // outlive the await, so declare it beside the co_await and not in a caller. Both are AcceptEx's
    // requirements rather than this library's -- see SubmitAccept.
    [[nodiscard]] inline auto AcceptAsync(IoSocket listener, IoSocket accepted,
                                          IoAcceptBuffer* addrs,
                                          CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitAccept(listener, accepted, addrs, r, o, t, token);
        });
    }

    // `s` must already be BOUND -- ConnectEx's rule; bind to INADDR_ANY:0 if you have no preference.
    [[nodiscard]] inline auto ConnectAsync(IoSocket s, const void* sockaddr,
                                           std::uint32_t sockaddrLen,
                                           CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitConnect(s, sockaddr, sockaddrLen, r, o, t, token);
        });
    }

} // namespace JLib
