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

    // Next ready connection from a pre-posted acceptor. Returns 0 if cancelled or shutting down.
    //
    // The wait is an ORDINARY cancellable semaphore acquire, which is what makes scopes and
    // deadlines work on "wait for a connection" without IoAcceptor knowing either exists.
    class AcceptAwaiter {
    public:
        AcceptAwaiter(IoAcceptor& a, CancelToken t) noexcept : acc_(a), token_(t) {}
        AcceptAwaiter(const AcceptAwaiter&) = delete;
        AcceptAwaiter& operator=(const AcceptAwaiter&) = delete;

        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h);
            return !acc_.TakeOrQueue(&w_, &sock_, t, token_);
        }

        [[nodiscard]] IoSocket await_resume() const noexcept { return sock_; }

    private:
        IoAcceptor&    acc_;
        CancelToken    token_;
        // MEMBERS, so the parked waiter lives in the coroutine frame -- alive for exactly as long as
        // the wait, with nothing allocated and no wait primitive involved.
        IoAcceptWaiter w_{};
        IoSocket       sock_ = 0;
    };

    [[nodiscard]] inline AcceptAwaiter AcceptAsync(IoAcceptor& acc,
                                                   CancelToken token = CancelToken{}) noexcept {
        return AcceptAwaiter(acc, token);
    }

    // ---- scatter/gather -------------------------------------------------------------------------
    //
    // A header and a body from separate allocations, in one syscall, with no copy to join them:
    //
    //     IoBuffer v[2] = { { hdr, hdrLen }, { body, bodyLen } };
    //     IoResult r = co_await SendVAsync(s, v, 2);
    //
    // The ARRAY is copied during submit and may be a local. THE MEMORY IT POINTS AT MAY NOT -- that
    // is the transfer, and it belongs to the kernel until the completion.
    [[nodiscard]] inline auto RecvVAsync(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                         std::uint32_t flags = 0,
                                         CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRecvV(s, bufs, count, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendVAsync(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                         std::uint32_t flags = 0,
                                         CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitSendV(s, bufs, count, flags, r, o, t, token);
        });
    }

    // Close a connection and, with `reuse`, hand the SOCKET back ready for another accept or connect
    // -- skipping a closesocket/socket pair, which under a high connect rate is the actual cost.
    //
    // THE SOCKET IS NOT REUSABLE WHEN THIS IS CALLED, only when the result is in hand: like every
    // operation here it ends in a completion. Handing it to an accept before then hands the kernel a
    // socket it is still tearing down.
    [[nodiscard]] inline auto DisconnectAsync(IoSocket s, bool reuse = true,
                                              CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitDisconnect(s, reuse, r, o, t, token);
        });
    }

    // ---- datagrams ------------------------------------------------------------------------------
    //
    // `from` receives the sender's address and must outlive the await -- declare it beside the
    // co_await, in the coroutine frame, never in a caller.
    //
    // A DATAGRAM IS A MESSAGE. `bytes` is the whole thing; a buffer too small loses the remainder
    // and reports WSAEMSGSIZE rather than returning a partial read. That is UDP, not a wart here.
    [[nodiscard]] inline auto RecvFromAsync(IoSocket s, void* buf, std::uint32_t len,
                                            IoAddress* from, std::uint32_t flags = 0,
                                            CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitRecvFrom(s, buf, len, flags, from, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendToAsync(IoSocket s, const void* buf, std::uint32_t len,
                                          const void* addr, std::uint32_t addrLen,
                                          std::uint32_t flags = 0,
                                          CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([=](IoRequest* r, IoResult* o, Task* t) {
            return IoReactor::Instance().SubmitSendTo(s, buf, len, flags, addr, addrLen,
                                                      r, o, t, token);
        });
    }


    // ---- IoStream: the same operations, serialised per direction --------------------------------
    //
    //     IoStream conn{ sock };
    //     IoResult r = co_await SendVAsync(conn, v, 2);
    //
    // At most one transfer per direction is ever in flight; a second arriving while one runs is
    // linked into the stream and started by the first one's completion. The full reasoning, and why
    // this replaced a pair of SchedulerMutexes, is on IoStream in IoReactor.h.
    //
    // These are PLAIN AWAITERS, not Lazy coroutines. The mutex version had to be a Lazy because it
    // was genuinely two suspension points -- take the lock, then transfer. Chaining removes the
    // first one: a queued transfer does not wait and then start, it simply starts later. So this is
    // one suspension, one frame, and no slab allocation per send.

    [[nodiscard]] inline auto SendAsync(IoStream& s, const void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([&s, buf, len, flags, token](IoRequest* r, IoResult* o, Task* t) {
            const IoBuffer one{ const_cast<void*>(buf), len };
            return s.SubmitSend(&one, 1, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto SendVAsync(IoStream& s, const IoBuffer* bufs, std::uint32_t count,
                                         std::uint32_t flags = 0,
                                         CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([&s, bufs, count, flags, token](IoRequest* r, IoResult* o, Task* t) {
            return s.SubmitSend(bufs, count, flags, r, o, t, token);
        });
    }

    [[nodiscard]] inline auto RecvAsync(IoStream& s, void* buf, std::uint32_t len,
                                        std::uint32_t flags = 0,
                                        CancelToken token = CancelToken{}) noexcept {
        return detail::MakeIoAwaiter([&s, buf, len, flags, token](IoRequest* r, IoResult* o, Task* t) {
            const IoBuffer one{ buf, len };
            return s.SubmitRecv(&one, 1, flags, r, o, t, token);
        });
    }

    // ================================================================================================
    // ONE OPERATION PER DIRECTION PER STREAM SOCKET -- WHICH IoStream ABOVE ENFORCES FOR YOU.
    //
    // This note is about the RAW IoSocket overloads, which do not serialise. Use IoStream unless you
    // have your own ordering guarantee; what follows is why the guarantee is needed at all.
    //
    // Two SendAsync calls in flight on the same TCP socket are two independent operations, and the
    // kernel does not promise to complete them in the order they were submitted. Their bytes can
    // INTERLEAVE, which for a stream protocol is silent corruption -- a header from one message
    // followed by the body of another, with nothing reporting an error anywhere.
    //
    // The RAW overloads do not serialise, so that a caller with its own ordering guarantee is not
    // charged for a second one. IoStream is the answer for everyone else, and it is cheap: the lock
    // lives in the connection state the caller already owns, and an uncontended acquire is a fast
    // path that suspends rather than blocks when it is not.
    //
    // Holding IoStream::WriteLock() across several sends is the spelling for a framed message written
    // as a burst, where the guarantee has to span more than one call.
    //
    // Reads have the same shape and the same rule. Concurrent recv on one stream socket delivers
    // arbitrary slices to arbitrary callers.
    //
    // DATAGRAMS ARE THE EXCEPTION: each SendTo is one message, so concurrent sends cannot corrupt
    // each other. Only their relative ORDER is unspecified, which UDP does not promise anyway.
    // ================================================================================================

} // namespace JLib
