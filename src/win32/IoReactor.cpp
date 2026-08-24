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

namespace JLib {

    // The opaque block in IoRequest really does hold one of these. Checked rather than trusted,
    // because the header cannot see the type and a silent overflow would corrupt whatever follows.
    static_assert(sizeof(OVERLAPPED) <= IoRequest::kNativeBytes,
                  "IoRequest::kNativeBytes is too small for OVERLAPPED");
    static_assert(alignof(OVERLAPPED) <= 16,
                  "IoRequest::native is not aligned enough for OVERLAPPED");

    static_assert(IoAcceptBuffer::kBytes >= 2 * (sizeof(sockaddr_in6) + 16),
                  "IoAcceptBuffer is too small for AcceptEx: it needs sizeof(sockaddr)+16 per address");

    static OVERLAPPED* Ov(IoRequest* r) { return reinterpret_cast<OVERLAPPED*>(r->native); }

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

        std::thread worker;
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
        void EnsureThread() {
            if (running) return;
            running = true;
            worker = std::thread([this] { Run(); });

#if defined(JLIB_DEVELOPMENT) || !defined(NDEBUG)
            if (TaskScheduler::IsInitialized() && !TaskScheduler::ReserveIoCore()) {
                std::fprintf(stderr,
                    "[JLib::Scheduler] IoReactor started a thread, but the pool was sized without "
                    "reserving a core for it -- call TaskScheduler::SetReserveIoCore(true) before "
                    "Init, or pass an explicit poolSize that already accounts for it.\n");
            }
#endif
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
                    if (stopping && count == 0) return;
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

                if (lastOne) return;
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
                EnsureThread();
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
        impl->port = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, nullptr, 0, 1);
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

    bool IoReactor::Register(void* handle) {
        if (!handle || handle == INVALID_HANDLE_VALUE || !impl->port) return false;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            if (impl->stopping) return false;
            impl->EnsureThread();
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

    bool IoReactor::InitSockets() { return ResolveExtensions(); }

    bool IoReactor::RegisterSocket(IoSocket s) {
        // A SOCKET associates with a completion port exactly like a file handle -- on Windows it IS
        // a kernel handle. Same call, so no separate path.
        return Register(reinterpret_cast<void*>(static_cast<SOCKET>(s)));
    }

    bool IoReactor::SubmitRecv(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const SOCKET sock = static_cast<SOCKET>(s);
        HANDLE h = reinterpret_cast<HANDLE>(sock);
        return impl->Submit(h, 0, req, out, resume, token, [&](OVERLAPPED* ov) {
            // WSABUF built here rather than stored in the request: WSARecv COPIES the descriptor
            // array before returning, so it only has to survive the call, not the operation. The
            // BUFFER it points at must survive, and that is the caller's, as always.
            WSABUF b{ static_cast<ULONG>(len), static_cast<CHAR*>(buf) };
            DWORD f = flags, got = 0;
            return ::WSARecv(sock, &b, 1, &got, &f, ov, nullptr) == 0;
        });
    }

    bool IoReactor::SubmitSend(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                               IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        const SOCKET sock = static_cast<SOCKET>(s);
        HANDLE h = reinterpret_cast<HANDLE>(sock);
        return impl->Submit(h, 0, req, out, resume, token, [&](OVERLAPPED* ov) {
            WSABUF b{ static_cast<ULONG>(len), static_cast<CHAR*>(const_cast<void*>(buf)) };
            DWORD sent = 0;
            return ::WSASend(sock, &b, 1, &sent, static_cast<DWORD>(flags), ov, nullptr) == 0;
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
            std::thread t;
            { std::lock_guard<std::mutex> lk(impl->m); t.swap(impl->worker); }
            // Wake the drain loop so it notices `stopping` once the queue is empty. A null OVERLAPPED
            // is how it tells a wake-up from a completion.
            ::PostQueuedCompletionStatus(impl->port, 0, 0, nullptr);
            if (t.joinable()) t.join();
        }
        impl->running = false;
    }

    void EjectIoReactor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoReactor*>(ctx)->RequestCancel(token);
    }

} // namespace JLib
