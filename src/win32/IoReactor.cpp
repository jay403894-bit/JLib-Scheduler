// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Windows I/O completion ports. See include/IoReactor.h for the model; this is the mechanism.

#include "../../include/IoReactor.h"
#include "../../include/TaskScheduler.h"
#include "../../include/platform.h"      // the ONLY place windows.h comes from

// AFTER platform.h, and that ordering is safe only because platform.h defines WIN32_LEAN_AND_MEAN
// before windows.h -- which is what keeps the ancient winsock.h out. Including winsock2.h after a
// windows.h that HAD pulled in winsock.h is the classic redefinition wall; here there is nothing to
// collide with. Do not "tidy" this above the platform.h include.
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mswsock.h>
#pragma comment(lib, "ws2_32.lib")

#include <cstddef>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

namespace JLib {

    // The opaque block in IoRequest really does hold one of these. Checked rather than trusted,
    // because the header cannot see the type and a silent overflow would corrupt whatever follows.
    static_assert(sizeof(OVERLAPPED) <= IoRequest::kNativeBytes,
                  "IoRequest::kNativeBytes is too small for OVERLAPPED");
    static_assert(alignof(OVERLAPPED) <= 16,
                  "IoRequest::native is not aligned enough for OVERLAPPED");

    static_assert(IoAcceptBuffer::kBytes >= 2 * (sizeof(sockaddr_in6) + 16),
                  "IoAcceptBuffer is too small for AcceptEx: it needs sizeof(sockaddr)+16 per address");

    static_assert(IoAddress::kBytes >= sizeof(sockaddr_storage),
                  "IoAddress is too small for sockaddr_storage");
    static_assert(sizeof(OVERLAPPED) + IoRequest::kMaxVectors * sizeof(WSABUF)
                      <= IoRequest::kNativeBytes,
                  "IoRequest::kNativeBytes cannot hold OVERLAPPED plus kMaxVectors WSABUFs");

    static OVERLAPPED* Ov(IoRequest* r) { return reinterpret_cast<OVERLAPPED*>(r->native); }

    // The descriptor array lives immediately after the OVERLAPPED, inside the request, because
    // Windows requires it to stay valid for the DURATION of the operation and not merely the call.
    // OVERLAPPED is 32 bytes and `native` is 16-aligned, so this lands aligned for WSABUF.
    static WSABUF* Bufs(IoRequest* r) {
        return reinterpret_cast<WSABUF*>(r->native + sizeof(OVERLAPPED));
    }

    // Converts, rather than casting: WSABUF is { ULONG len; CHAR* buf; } and IoBuffer is
    // { void* data; uint32_t len; } -- the opposite order, and iovec on POSIX is different again.
    // A struct ABI-identical to both does not exist, and pretending otherwise would be a silent
    // reinterpretation of a length as a pointer.
    static bool FillBufs(IoRequest* r, const IoBuffer* bufs, std::uint32_t count) {
        if (count == 0 || count > IoRequest::kMaxVectors) return false;
        WSABUF* w = Bufs(r);
        for (std::uint32_t i = 0; i < count; ++i) {
            w[i].len = static_cast<ULONG>(bufs[i].len);
            w[i].buf = static_cast<CHAR*>(bufs[i].data);
        }
        return true;
    }

    // AcceptEx and ConnectEx have NO IMPORT LIBRARY. They are provider-specific and must be fetched
    // at runtime through WSAIoctl, which is why InitSockets exists at all and why a socket has to be
    // available to ask. Resolved once; the pointers are per-provider in theory and universal in
    // practice for TCP.
    static LPFN_ACCEPTEX      g_acceptEx  = nullptr;
    static LPFN_CONNECTEX     g_connectEx = nullptr;
    static std::mutex         g_extMutex;

    static bool ResolveExtensions() {
        std::lock_guard<std::mutex> lk(g_extMutex);
        if (g_acceptEx && g_connectEx) return true;

        // A throwaway socket purely to ask. It is closed before returning -- the function pointers
        // outlive it, which is the whole reason this is a one-time bootstrap rather than per-socket.
        SOCKET probe = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (probe == INVALID_SOCKET) return false;   // usually WSANOTINITIALISED: no WSAStartup

        GUID gAccept  = WSAID_ACCEPTEX;
        GUID gConnect = WSAID_CONNECTEX;
        DWORD got = 0;
        bool ok = ::WSAIoctl(probe, SIO_GET_EXTENSION_FUNCTION_POINTER, &gAccept, sizeof gAccept,
                             &g_acceptEx, sizeof g_acceptEx, &got, nullptr, nullptr) == 0;
        ok = ok && ::WSAIoctl(probe, SIO_GET_EXTENSION_FUNCTION_POINTER, &gConnect, sizeof gConnect,
                              &g_connectEx, sizeof g_connectEx, &got, nullptr, nullptr) == 0;
        ::closesocket(probe);
        return ok && g_acceptEx && g_connectEx;
    }

    struct IoReactor::Impl {
        HANDLE port = nullptr;

        mutable std::mutex m;
        IoRequest* head = nullptr;      // in-flight, newest first
        std::size_t count = 0;

        // MANY THREADS ON ONE PORT is what IOCP is built for, and the port's own concurrency limit
        // (set at creation) caps how many the kernel lets RUN at once -- so extra threads cost a
        // stack and nothing else while they are parked in GetQueuedCompletionStatus. The count comes
        // from TaskScheduler because the pool reserves one core for each, and the two must agree.
        std::vector<std::thread> workers;
        bool running  = false;
        bool stopping = false;

        // Both under `m`. Entries are owned by their CALLER -- a coroutine frame, suspended -- so the
        // same rule as the condition variable applies: a request is unlinked BEFORE its task is
        // pushed, never after, or a later cancel pass walks into a frame that has resumed and gone.
        void Link(IoRequest* r) {
            r->prev = nullptr;
            r->next = head;
            if (head) head->prev = r;
            head = r;
            ++count;
        }

        void Unlink(IoRequest* r) {
            if (r->prev) r->prev->next = r->next;
            else if (head == r) head = r->next;
            if (r->next) r->next->prev = r->prev;
            r->prev = r->next = nullptr;
            --count;
        }

        // Caller holds `m`.
        void EnsureThreads() {
            if (running) return;
            running = true;
            const unsigned n = TaskScheduler::IoCompletionThreads();
            workers.reserve(n);
            for (unsigned i = 0; i < n; ++i) workers.emplace_back([this] { Run(); });
        }

        static IoResult Classify(BOOL ok, DWORD err, DWORD bytes) {
            IoResult res;
            if (ok) {
                res.status = IoStatus::Completed;
                res.bytes  = static_cast<std::uint32_t>(bytes);
            } else if (err == ERROR_OPERATION_ABORTED) {
                // The cancel request won. `bytes` is deliberately left at 0 -- a caller trusting a
                // partial count on a cancelled read would be reading buffer the kernel never filled.
                res.status = IoStatus::Cancelled;
            } else if (err == ERROR_HANDLE_EOF || err == ERROR_BROKEN_PIPE) {
                // End of stream is a completion of zero bytes, not a failure. Reporting it as Failed
                // would make every reader special-case a normal outcome.
                res.status = IoStatus::Completed;
                res.bytes  = 0;
            } else {
                res.status = IoStatus::Failed;
                res.error  = static_cast<std::int32_t>(err);
            }
            return res;
        }

        void Run() {
            for (;;) {
                DWORD bytes = 0;
                ULONG_PTR key = 0;
                LPOVERLAPPED ov = nullptr;

                // A BLOCKING WAIT, and this thread is one of only two places in the library allowed
                // one -- the other being the timer -- for the same reason: it has no work to lose.
                const BOOL ok = GetQueuedCompletionStatus(port, &bytes, &key, &ov, INFINITE);

                if (ov == nullptr) {
                    // Not a completion: the port died, or Stop() posted the wake-up below.
                    if (!ok) return;
                    std::lock_guard<std::mutex> lk(m);
                    if (stopping && count == 0) {
                        // Cascade the exit: one more wake-up so the next thread also sees it. Stop
                        // posts one per thread, and this makes the shutdown robust if a thread
                        // consumed a wake-up while work was still draining.
                        ::PostQueuedCompletionStatus(port, 0, 0, nullptr);
                        return;
                    }
                    continue;
                }

                IoRequest* r = reinterpret_cast<IoRequest*>(
                    reinterpret_cast<unsigned char*>(ov) - offsetof(IoRequest, native));
                const IoResult res = Classify(ok, ok ? ERROR_SUCCESS : ::GetLastError(), bytes);

                // THE FIXUP WINDOWS REQUIRES, and it has to happen here rather than in the caller:
                // until it runs, an accepted socket has not inherited the listener's properties and
                // a connected one has no idea it is connected -- getpeername, shutdown and the
                // socket options all misbehave. Nothing FAILS, which is what makes omitting it such
                // a bad bug.
                if (res.status == IoStatus::Completed && r->kind != IoRequest::Kind::Generic) {
                    if (r->kind == IoRequest::Kind::Accept) {
                        // handle = listener (where the I/O lives), aux = accepted (what to fix up).
                        const SOCKET listener = reinterpret_cast<SOCKET>(r->handle);
                        const SOCKET accepted = static_cast<SOCKET>(r->aux);
                        ::setsockopt(accepted, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                                     reinterpret_cast<const char*>(&listener), sizeof listener);
                    } else {
                        const SOCKET s = reinterpret_cast<SOCKET>(r->handle);
                        ::setsockopt(s, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, nullptr, 0);
                    }
                }

                Task* resume = nullptr;
                bool  lastOne = false;
                {
                    std::lock_guard<std::mutex> lk(m);
                    Unlink(r);                     // BEFORE the push; see the note on Link
                    if (r->out) *r->out = res;     // still safe: the owner is still suspended
                    resume = r->resume;
                    lastOne = (stopping && count == 0);
                }

                // OUTSIDE THE LOCK. Pushing can run the task on another worker immediately, and its
                // frame -- which is where `r` lives -- may be gone before Push returns. NOTHING below
                // may touch `r`.
                if (resume && TaskScheduler::IsInitialized())
                    TaskScheduler::Instance().Push(resume);

                if (lastOne) {
                    ::PostQueuedCompletionStatus(port, 0, 0, nullptr);
                    return;
                }
            }
        }

        // Shared by SubmitRead and SubmitWrite: everything except which kernel call to make.
        //
        // Returns TRUE when the caller already has its answer and must not suspend, matching
        // SchedulerMutex::LockAsyncEnqueue so an awaiter reads `return !Submit...`.
        template <typename Call>
        bool Submit(HANDLE h, std::uint64_t offset, IoRequest* req, IoResult* out,
                    Task* resume, CancelToken token, Call&& call) {
            const std::uint32_t tok = token.Raw();

            // Already cancelled: never submit. The kernel takes no buffer, so there is nothing to
            // wait for -- the ONE case where an I/O cancel is immediate.
            if (CancelToken(tok).Cancelled()) {
                if (out) *out = IoResult{ IoStatus::Cancelled, 0, 0 };
                return true;
            }

            *Ov(req) = OVERLAPPED{};
            Ov(req)->Offset     = static_cast<DWORD>(offset & 0xFFFFFFFFull);
            Ov(req)->OffsetHigh = static_cast<DWORD>(offset >> 32);
            req->out    = out;
            req->resume = resume;
            req->token  = tok;
            req->handle = h;

            {
                std::lock_guard<std::mutex> lk(m);
                if (stopping) {
                    if (out) *out = IoResult{ IoStatus::Failed, 0, ERROR_SHUTDOWN_IN_PROGRESS };
                    return true;
                }
                EnsureThreads();
                // LINKED BEFORE SUBMITTED, the same ordering rule as every parking path here: a fast
                // operation can complete on the reactor thread while this one is still inside
                // ReadFile, and the completion must find the request already discoverable.
                Link(req);
            }

            if (!call(Ov(req))) {
                const DWORD err = ::GetLastError();
                if (err != ERROR_IO_PENDING) {
                    // Rejected outright: the kernel never took the buffer, so NO COMPLETION IS
                    // COMING and the caller must not suspend.
                    std::lock_guard<std::mutex> lk(m);
                    Unlink(req);
                    if (out) *out = IoResult{ IoStatus::Failed, 0, static_cast<std::int32_t>(err) };
                    return true;
                }
            }

            // Queued. A completion is now GUARANTEED -- including when the call returned success
            // synchronously, because the handle is associated with a port and Windows queues a packet
            // either way. From here `req` and `*out` belong to the reactor.
            return false;
        }
    };

    IoReactor::IoReactor() : impl(new Impl()) {
        // The last argument is the port.s CONCURRENCY LIMIT: how many threads the kernel lets run
        // at once, not how many may wait. Matched to the thread count so extra threads park in
        // GetQueuedCompletionStatus and cost a stack rather than a core.
        impl->port = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0,
                                              TaskScheduler::IoCompletionThreads());
    }

    IoReactor::~IoReactor() {
        Stop();
        if (impl->port) ::CloseHandle(impl->port);
        delete impl;
    }

    IoReactor& IoReactor::Instance() {
        // Function-local static, like the cancel table and the timer: no static-init-order dependency
        // on the scheduler, and no thread until something actually submits.
        static IoReactor r;
        return r;
    }

    bool IoReactor::IsAvailable() noexcept { return true; }

    // OPT-IN, ENFORCED. This library is a job system first; the reactor is a layer, and an app that
    // wanted only jobs should not be paying a thread and a core for one it never asked for. A pool
    // sized without a completion core therefore refuses to run the reactor at all -- at the first
    // call, loudly -- rather than working while running the machine one thread over for the life of
    // the process. That deficit measured 3-4% the last time it happened and took a VTune session to
    // find; this is a two-minute fix instead.
    //
    // Checked at REGISTER, which every path has to go through before it can submit anything, so one
    // check covers the whole surface rather than eight.
    //
    // Only when a POOL EXISTS -- using the reactor with no scheduler is legitimate, since there are
    // no workers to oversubscribe.
    static bool IoLayerUsable() {
        if (!TaskScheduler::IsInitialized() || TaskScheduler::IoReactorEnabled()) return true;
        std::fprintf(stderr,
            "[JLib::Scheduler] IoReactor used but the I/O layer is not enabled -- the pool was sized "
            "without a core for its completion thread. Call TaskScheduler::EnableIoReactor(true) "
            "before Init.\n");
        return false;
    }

    bool IoReactor::Register(void* handle) {
        if (!IoLayerUsable()) return false;
        if (!handle || handle == INVALID_HANDLE_VALUE || !impl->port) return false;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            if (impl->stopping) return false;
            impl->EnsureThreads();
        }
        // Associating with an existing port returns the port itself; the completion key is unused
        // because the OVERLAPPED already identifies the request.
        return ::CreateIoCompletionPort(static_cast<HANDLE>(handle), impl->port, 0, 0) != nullptr;
    }

    bool IoReactor::SubmitRead(void* handle, void* buf, std::uint32_t len, std::uint64_t offset,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        HANDLE h = static_cast<HANDLE>(handle);
        return impl->Submit(h, offset, req, out, resume, token, [&](OVERLAPPED* ov) {
            return ::ReadFile(h, buf, len, nullptr, ov) != FALSE;
        });
    }

    bool IoReactor::SubmitWrite(void* handle, const void* buf, std::uint32_t len,
                                std::uint64_t offset, IoRequest* req, IoResult* out,
                                Task* resume, CancelToken token) {
        HANDLE h = static_cast<HANDLE>(handle);
        return impl->Submit(h, offset, req, out, resume, token, [&](OVERLAPPED* ov) {
            return ::WriteFile(h, buf, len, nullptr, ov) != FALSE;
        });
    }

    // ---- sockets ---------------------------------------------------------------------------------

    bool IoReactor::InitSockets() { return IoLayerUsable() && ResolveExtensions(); }

    bool IoReactor::RegisterSocket(IoSocket s) {
        // A SOCKET associates with a completion port exactly like a file handle -- on Windows it IS
        // a kernel handle. Same call, so no separate path.
        return Register(reinterpret_cast<void*>(static_cast<SOCKET>(s)));
    }

    bool IoReactor::SubmitRecv(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const SOCKET sock = static_cast<SOCKET>(s);
        HANDLE h = reinterpret_cast<HANDLE>(sock);
        const IoBuffer one{ buf, len };
        return SubmitRecvV(s, &one, 1, flags, req, out, resume, token);
    }

    bool IoReactor::SubmitSend(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const SOCKET sock = static_cast<SOCKET>(s);
        HANDLE h = reinterpret_cast<HANDLE>(sock);
        const IoBuffer one{ const_cast<void*>(buf), len };
        return SubmitSendV(s, &one, 1, flags, req, out, resume, token);
    }

    bool IoReactor::SubmitRecvV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                std::uint32_t flags, IoRequest* req, IoResult* out,
                                Task* resume, CancelToken token) {
        if (!FillBufs(req, bufs, count)) {
            // Too many segments, or none. Refused rather than truncated: a short write that looks
            // successful is the worst possible outcome for a protocol.
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEMSGSIZE };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            // BOTH POINTERS BELOW MUST OUTLIVE THE CALL. `flags` is [in, out] -- the kernel reports
            // MSG_PARTIAL and friends through it on completion -- so it lives in the request, not on
            // this stack. And the byte count is passed as NULL deliberately: MSDN says to do that
            // whenever lpOverlapped is non-null, because the value is only meaningful for a
            // synchronous completion and reading it otherwise gives erroneous results. The real
            // count comes from the completion packet.
            return ::WSARecv(sock, Bufs(req), count, nullptr,
                             reinterpret_cast<LPDWORD>(&req->flags), ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitSendV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                                std::uint32_t flags, IoRequest* req, IoResult* out,
                                Task* resume, CancelToken token) {
        if (!FillBufs(req, bufs, count)) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEMSGSIZE };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            return ::WSASend(sock, Bufs(req), count, nullptr,
                             static_cast<DWORD>(flags), ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitRecvFrom(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                                   IoAddress* from, IoRequest* req, IoResult* out,
                                   Task* resume, CancelToken token) {
        const IoBuffer one{ buf, len };
        if (!FillBufs(req, &one, 1) || !from) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEINVAL };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        from->len = static_cast<std::int32_t>(IoAddress::kBytes);   // capacity going in

        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            // `from` and `from->len` are both written by the kernel ON COMPLETION, so both must
            // outlive this call -- which is why IoAddress carries its own length rather than taking
            // one by pointer. Passing a local int here is the classic version of this bug.
            return ::WSARecvFrom(sock, Bufs(req), 1, nullptr,
                                 reinterpret_cast<LPDWORD>(&req->flags),
                                 reinterpret_cast<sockaddr*>(from->bytes),
                                 reinterpret_cast<LPINT>(&from->len), ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitSendTo(IoSocket s, const void* buf, std::uint32_t len,
                                 std::uint32_t flags, const void* addr, std::uint32_t addrLen,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const IoBuffer one{ const_cast<void*>(buf), len };
        if (!FillBufs(req, &one, 1) || !addr) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEINVAL };
            return true;
        }
        const SOCKET sock = static_cast<SOCKET>(s);
        req->flags = flags;
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            // The DESTINATION address is read at call time and not retained, unlike RecvFrom's
            // source -- so this one may legitimately be a caller's local.
            return ::WSASendTo(sock, Bufs(req), 1, nullptr, static_cast<DWORD>(flags),
                               static_cast<const sockaddr*>(addr), static_cast<int>(addrLen),
                               ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitAccept(IoSocket listener, IoSocket accepted, IoAcceptBuffer* addrs,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        if (!g_acceptEx && !ResolveExtensions()) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSANOTINITIALISED };
            return true;
        }

        const SOCKET lis = static_cast<SOCKET>(listener);
        const SOCKET acc = static_cast<SOCKET>(accepted);

        // `handle` IS THE LISTENER, and getting this backwards is a hang rather than an error.
        // AcceptEx is issued ON the listening socket -- that is where the pending I/O lives, so that
        // is what CancelIoEx has to be aimed at. Aiming it at the accepted socket cancels nothing (it
        // has no I/O of its own yet), no completion is ever posted, and the awaiting coroutine stays
        // suspended forever holding its request. Found exactly that way.
        //
        // The ACCEPTED socket goes in `aux`, because it is the one the post-completion fixup applies
        // to. The two sockets have different jobs here and neither can stand in for the other.
        req->kind = IoRequest::Kind::Accept;
        req->aux  = static_cast<std::uintptr_t>(acc);

        return impl->Submit(reinterpret_cast<HANDLE>(lis), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            // Receive length ZERO, deliberately. AcceptEx can also wait for the first chunk of data
            // before completing, which sounds efficient and is a denial-of-service: a client that
            // connects and sends nothing holds the accept -- and the pre-created socket -- open
            // indefinitely. Accepting first and reading second is the safe order.
            constexpr DWORD kAddrLen = sizeof(sockaddr_in6) + 16;
            DWORD got = 0;
            return g_acceptEx(lis, acc, addrs->bytes, 0, kAddrLen, kAddrLen, &got, ov) != FALSE;
        });
    }

    bool IoReactor::SubmitConnect(IoSocket s, const void* addr, std::uint32_t addrLen,
                                  IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        if (!g_connectEx && !ResolveExtensions()) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSANOTINITIALISED };
            return true;
        }

        const SOCKET sock = static_cast<SOCKET>(s);
        req->kind = IoRequest::Kind::Connect;

        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            DWORD sent = 0;
            return g_connectEx(sock, static_cast<const struct sockaddr*>(addr),
                               static_cast<int>(addrLen), nullptr, 0, &sent, ov) != FALSE;
        });
    }

    std::size_t IoReactor::RequestCancel(CancelToken token) noexcept {
        // NESTED SCOPES COUNT. An operation registered under a request scope belongs to the
        // connection scope above it, so this asks IsWithin rather than comparing tokens -- matching
        // with == is the 3.4.1 bug, where cancelling a parent silently missed its children.
        //
        // The CancelIoEx calls happen UNDER the lock, which is the opposite of what every other
        // cancel path here does, and correct precisely because this one resumes nobody. CancelIoEx
        // only queues a request; the completion, and the push that could race this list, arrive later
        // on the reactor thread. There is no handoff to hold a lock across.
        //
        // O(n) in the in-flight count, knowingly. A per-scope index is the obvious next step and the
        // moment to build it is a profile showing this hot -- the timer wheel's O(1) removal was
        // justified by a measured workload and this one has none yet.
        std::lock_guard<std::mutex> lk(impl->m);
        std::size_t asked = 0;
        for (IoRequest* r = impl->head; r; r = r->next) {
            if (token.Valid() && !CancelToken(r->token).IsWithin(token)) continue;
            ::CancelIoEx(r->handle, Ov(r));
            ++asked;
        }
        return asked;
    }

    std::size_t IoReactor::InFlight() const noexcept {
        std::lock_guard<std::mutex> lk(impl->m);
        return impl->count;
    }

    void IoReactor::Stop() noexcept {
        bool needJoin = false;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            if (impl->stopping) return;
            impl->stopping = true;
            needJoin = impl->running;
        }

        // EVERY IN-FLIGHT OPERATION IS CANCELLED AND DRAINED BEFORE THE THREAD GOES. Stopping while
        // the kernel still holds a buffer whose owning frame is about to unwind is the corruption
        // this file exists to prevent -- and the owner cannot resume until its completion arrives,
        // so the only way out is to make those completions happen.
        RequestCancel(CancelToken{});

        if (needJoin) {
            std::vector<std::thread> ts;
            { std::lock_guard<std::mutex> lk(impl->m); ts.swap(impl->workers); }
            // ONE WAKE-UP PER THREAD. A null OVERLAPPED is how the drain loop tells a wake-up from a
            // completion; each thread also posts one more on its way out, so the exit cascades even
            // if a thread consumed a wake-up while work was still draining.
            for (std::size_t i = 0; i < ts.size(); ++i)
                ::PostQueuedCompletionStatus(impl->port, 0, 0, nullptr);
            for (auto& t : ts) if (t.joinable()) t.join();
        }
        impl->running = false;
    }

    void EjectIoReactor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoReactor*>(ctx)->RequestCancel(token);
    }

} // namespace JLib
