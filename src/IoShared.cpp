// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE PART OF THE REACTOR THAT IS NOT A BACKEND.
//
// IoStream's chaining and IoAcceptor's backlog are queueing and ordering logic. They call the
// reactor's Submit* interface and are otherwise platform-neutral -- but they lived in
// src/win32/IoReactor.cpp, so a Linux build had no IoAcceptor at all and the port LOOKED like it
// required reimplementing cancellable waiter queues. It did not: across both classes the
// platform-specific surface was a type, a close call, one socket-creation call, one error constant
// and one descriptor conversion, all of which now sit behind src/IoPlatform.h.
//
// WHY ONE COPY MATTERS MORE THAN THE LINE COUNT. A second implementation of "accept backlog with
// cancellable waiters" would not stay equal to the first. The two platforms would then disagree
// about what the reactor promises -- and the disagreement would surface as a test that passes on
// Windows and fails on Linux for reasons in neither backend. This repository already refuses that
// trade elsewhere (one CMakeLists serving two repos, rather than two that must agree forever).
//
// WHAT IS STILL PER-BACKEND: everything an operation touches. Opcodes, submission, completion,
// cancellation. Those are win32/IoReactor.cpp and posix/IoReactor.cpp, and they should stay there --
// this file must never grow a #if for an operation.

#include "../include/IoReactor.h"
#include "../include/TaskScheduler.h"
#include "IoPlatform.h"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

namespace JLib {

    void EjectIoReactor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoReactor*>(ctx)->RequestCancel(token);
    }

    void EjectIoAcceptor(void* ctx, CancelToken token) {
        if (ctx) static_cast<IoAcceptor*>(ctx)->CancelWaiters(token);
    }

    // ---- IoStream: serialised by chaining, not by locking ---------------------------------------

    namespace {
        struct SpinGuard {
            std::atomic_flag& f;
            explicit SpinGuard(std::atomic_flag& x) : f(x) {
                while (f.test_and_set(std::memory_order_acquire)) platform::CpuRelax();
            }
            ~SpinGuard() { f.clear(std::memory_order_release); }
        };
    }

    // Submits `req` if the direction is idle, otherwise QUEUES it. Either way the caller suspends
    // unless the answer is already final.
    bool IoStream::SubmitChained(Chain& c, IoRequest::Kind kind,
                                 const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        // Descriptors are filled NOW, even when the transfer is only queued -- the caller's `bufs`
        // array is allowed to be a local and will be gone by the time this is submitted.
        if (!ioplat::FillBufs(req, bufs, count)) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, ioplat::kErrMsgSize };
            return true;
        }

        // Already cancelled: answer now, and do NOT take a place in the queue. A cancelled transfer
        // that queued would hold the direction until its turn came, delaying live work behind
        // something nobody wants.
        if (CancelToken(token.Raw()).Cancelled()) {
            if (out) *out = IoResult{ IoStatus::Cancelled, 0, 0 };
            return true;
        }

        req->kind      = kind;
        req->bufCount  = count;
        req->flags     = flags;
        // Reset per transfer: this request may be recycled, and a stale partial-send total would be
        // added to a completion that never had one. See IoRequest::xferred.
        req->xferred   = 0;
        req->out       = out;
        req->resume    = resume;
        req->token     = token.Raw();
        req->handle    = reinterpret_cast<void*>(static_cast<std::uintptr_t>(sock_));
        req->hookCtx   = reinterpret_cast<std::uintptr_t>(this);
        req->onComplete = (kind == IoRequest::Kind::Send) ? &IoStream::OnSendComplete
                                                          : &IoStream::OnRecvComplete;
        req->next = nullptr;

        {
            SpinGuard g(lock_);
            if (c.busy) {
                // QUEUED, NOT SUBMITTED. This is the whole mechanism: the direction stays
                // single-threaded because only one transfer is ever handed to the kernel.
                if (c.tail) c.tail->next = req; else c.head = req;
                c.tail = req;
                ++c.queued;
                return false;                  // suspend; the chain will start it
            }
            c.busy = true;
        }

        return SubmitOne(req);
    }

    // Hands one already-prepared request to the kernel. Shared by the first submit and by every
    // chained one, so a queued transfer takes exactly the same path as an immediate one.
    bool IoStream::SubmitOne(IoRequest* req) {
        const bool immediate = IoReactor::Instance().SubmitPrepared(req);

        if (immediate) {
            // Never queued with the kernel, so no completion is coming and the hook will not run.
            // The chain has to be advanced here instead or the direction stays busy forever.
            Chain& c = (req->kind == IoRequest::Kind::Send) ? send_ : recv_;
            Advance(c, req->kind);
        }
        return immediate;
    }

    // Called from the completion thread, outside every lock. Starts the next queued transfer.
    void IoStream::Advance(Chain& c, IoRequest::Kind kind) {
        IoRequest* next = nullptr;
        {
            SpinGuard g(lock_);
            if (c.head) {
                next = c.head;
                c.head = next->next;
                if (!c.head) c.tail = nullptr;
                --c.queued;
                next->next = nullptr;
                // busy STAYS true: the direction is handing off, not going idle.
            } else {
                c.busy = false;
            }
        }
        if (next) SubmitOne(next);
        (void)kind;
    }

    void IoStream::OnSendComplete(IoRequest* r) {
        IoStream* s = reinterpret_cast<IoStream*>(r->hookCtx);
        if (s) s->Advance(s->send_, IoRequest::Kind::Send);
    }

    void IoStream::OnRecvComplete(IoRequest* r) {
        IoStream* s = reinterpret_cast<IoStream*>(r->hookCtx);
        if (s) s->Advance(s->recv_, IoRequest::Kind::Recv);
    }

    bool IoStream::SubmitSend(const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                              IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        return SubmitChained(send_, IoRequest::Kind::Send, bufs, count, flags,
                             req, out, resume, token);
    }

    bool IoStream::SubmitRecv(const IoBuffer* bufs, std::uint32_t count, std::uint32_t flags,
                              IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
        return SubmitChained(recv_, IoRequest::Kind::Recv, bufs, count, flags,
                             req, out, resume, token);
    }

    std::size_t IoStream::QueuedSends() const noexcept {
        SpinGuard g(lock_);
        return send_.queued;
    }

    std::size_t IoStream::QueuedRecvs() const noexcept {
        SpinGuard g(lock_);
        return recv_.queued;
    }

    // ---- IoAcceptor ------------------------------------------------------------------------------

    struct IoAcceptor::Impl {
        // One pre-posted accept. The acceptor owns all of it -- see the ownership note in the header:
        // a pre-posted accept has no awaiting frame to bound its lifetime, so this class is the
        // bound, and Stop() is where it is enforced.
        struct Slot {
            IoRequest      req{};
            IoAcceptBuffer addrs{};
            IoResult       result{};
            IoSocket         sock = ioplat::kNoSocket;
            Impl*          owner = nullptr;
            unsigned       index = 0;
        };

        IoSocket listener = ioplat::kNoSocket;
        int    family = AF_INET, type = SOCK_STREAM, proto = IPPROTO_TCP;

        std::vector<Slot>   slots;
        mutable std::mutex  m;
        std::vector<IoSocket> ready;          // accepted, nobody waiting yet
        IoAcceptWaiter*     waitHead = nullptr;   // parked, no connection yet
        IoAcceptWaiter*     waitTail = nullptr;
        std::size_t         outstanding = 0;
        bool                stopping = false;

        CancelScope         scope;          // cancels every accept this acceptor posted, and only those

        // Creates the socket AcceptEx will fill. Matching the listener's family/protocol matters:
        // a mismatch fails inside AcceptEx with an error that does not say which argument was wrong.
        IoSocket MakeSocket() const {
            return ioplat::MakeSocket(family, type, proto);
        }

        // Caller must NOT hold `m`: this pushes a task and submits to the kernel.
        void Post(unsigned i) {
            Slot& s = slots[i];
            s.sock = MakeSocket();
            if (s.sock == ioplat::kNoSocket) return;

            if (!IoReactor::Instance().RegisterSocket(static_cast<IoSocket>(s.sock))) {
                ioplat::CloseSocket(s.sock);
                s.sock = ioplat::kNoSocket;
                return;
            }

            // NO TASK IS ALLOCATED PER ACCEPT. The slot's completion is delivered through
            // IoRequest::onComplete -- the same hook IoStream uses to chain transfers -- so `resume`
            // stays null and nothing is pushed for the harvest itself. Only a WAITING caller gets
            // pushed, and that Task is the caller's own.
            s.req.onComplete = &Impl::AcceptCompleted;
            s.req.hookCtx    = reinterpret_cast<std::uintptr_t>(&s);

            { std::lock_guard<std::mutex> lk(m); ++outstanding; }

            if (IoReactor::Instance().SubmitAccept(static_cast<IoSocket>(listener),
                                                   static_cast<IoSocket>(s.sock), &s.addrs,
                                                   &s.req, &s.result, nullptr, scope.Token())) {
                // Answered immediately -- refused, or the scope was already cancelled. No completion
                // is coming, so the hook will not run and this must clean up itself.
                { std::lock_guard<std::mutex> lk(m); --outstanding; }
                ioplat::CloseSocket(s.sock);
                s.sock = ioplat::kNoSocket;
            }
        }

        static void AcceptCompleted(IoRequest* r) {
            Slot* s = reinterpret_cast<Slot*>(r->hookCtx);
            if (s && s->owner) s->owner->OnComplete(s->index);
        }

        void OnComplete(unsigned i) {
            Slot& s = slots[i];
            const IoSocket accepted = s.sock;     // the connection this slot just produced
            s.sock = ioplat::kNoSocket;

            bool   repost = false;
            bool   keep   = false;              // did anyone take the socket?
            IoAcceptWaiter* handTo = nullptr;
            IoAcceptWaiter* skipped = nullptr;   // cancelled waiters, woken with 0 below
            {
                std::lock_guard<std::mutex> lk(m);
                --outstanding;
                if (s.result.status == IoStatus::Completed && !stopping) {
                    repost = true;
                    keep   = true;
                    // HAND IT STRAIGHT TO A PARKED WAITER if there is one, rather than queueing it
                    // and then waking somebody to come and fetch it. Dequeued BEFORE the resume, the
                    // same rule as everywhere here: the waiter lives in a frame that dies when
                    // pushed.
                    //
                    // SKIP-AT-RELEASE, exactly as SchedulerSemaphore::Signal does. A waiter whose
                    // scope was cancelled while it was parked must NOT be handed a live socket: it
                    // is about to unwind and would drop it, leaking a connection per cancelled
                    // acceptor. Those are woken with 0 and the socket goes to the next one that
                    // still wants it.
                    while (waitHead) {
                        IoAcceptWaiter* w = waitHead;
                        waitHead = w->next;
                        if (!waitHead) waitTail = nullptr;
                        w->next = nullptr;

                        if (CancelToken(w->token).Cancelled()) {
                            w->next = skipped;      // reuse `next` as the skipped-list link
                            skipped = w;
                            continue;
                        }
                        handTo = w;
                        break;
                    }
                    if (!handTo) ready.push_back(accepted);
                }
            }

            // Cancelled, failed, or shutting down: nobody took it.
            if (!keep && accepted != ioplat::kNoSocket) ioplat::CloseSocket(accepted);

            // OUTSIDE THE LOCK. Push can run the waiter on another worker immediately, and its frame
            // -- which is where the waiter lives -- may be gone before Push returns, so the socket is
            // written first and nothing below touches `handTo`.
            if (handTo) {
                if (handTo->out) *handTo->out = static_cast<IoSocket>(accepted);
                Task* t = handTo->resume;
                if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().Push(t);
            }

            // Cancelled waiters, also outside the lock and also touched-then-forgotten.
            while (skipped) {
                IoAcceptWaiter* w = skipped;
                skipped = w->next;
                if (w->out) *w->out = 0;
                Task* t = w->resume;
                if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().Push(t);
            }

            // RE-POST IMMEDIATELY, so the depth is restored before the next connection arrives. That
            // is the entire point of the class -- an accept that is posted only after the previous
            // one is consumed is just AcceptAsync with extra steps.
            if (repost) Post(i);
        }
    };

    IoAcceptor::~IoAcceptor() {
        Stop();
        delete impl;
        impl = nullptr;
    }

    bool IoAcceptor::Start(IoSocket listener, unsigned depth) {
        if (impl || depth == 0) return false;
        if (!IoReactor::IsAvailable()) return false;

        impl = new Impl();
        impl->listener = static_cast<IoSocket>(listener);

        // Ask the LISTENER what it is rather than making the caller repeat it -- one fewer thing to
        // get subtly wrong, and the failure mode for getting it wrong is opaque. Queried ONCE here
        // and cached; every slot creates its socket from the cached triple, so this is a startup
        // cost rather than a per-connection one.
        //
        // A FAILURE HERE IS NOT FATAL, deliberately: the defaults already in Impl (AF_INET,
        // SOCK_STREAM, IPPROTO_TCP) are right for every listener this class has ever been handed,
        // and refusing to start because an optional query failed would be worse than proceeding.
        int fam = 0, typ = 0, prot = 0;
        if (ioplat::QuerySocketTriple(impl->listener, fam, typ, prot)) {
            impl->family = fam;
            impl->type   = typ;
            impl->proto  = prot;
        }

        impl->slots.resize(depth);
        for (unsigned i = 0; i < depth; ++i) {
            impl->slots[i].owner = impl;
            impl->slots[i].index = i;
        }
        for (unsigned i = 0; i < depth; ++i) impl->Post(i);

        std::lock_guard<std::mutex> lk(impl->m);
        return impl->outstanding > 0;
    }

    void IoAcceptor::Stop() noexcept {
        if (!impl) return;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            if (impl->stopping) return;
            impl->stopping = true;
        }

        // RELEASE PARKED WAITERS FIRST, and this was missing: a coroutine sitting in AcceptAsync
        // when Stop ran was never resumed and hung forever. A LOST WAKE arriving through shutdown
        // rather than through a race, which is the easier half of that class to overlook because no
        // concurrency is involved at all -- there is simply no code path that wakes them.
        //
        // `stopping` is already set above, so TakeOrQueue will not admit a new waiter behind this.
        CancelWaiters(CancelToken{});

        // CANCEL, THEN DRAIN. The kernel holds pointers into every slot's request and address
        // buffer, so releasing them before the completions arrive is the corruption this whole file
        // is arranged to prevent. Cancelling by SCOPE reaches exactly this acceptor's accepts and
        // nothing else -- which is why it owns a CancelScope rather than cancelling globally.
        impl->scope.Cancel();
        IoReactor::Instance().RequestCancel(impl->scope.Token());

        for (int spins = 0; spins < 5000; ++spins) {
            { std::lock_guard<std::mutex> lk(impl->m); if (impl->outstanding == 0) break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Anything accepted but never claimed is closed: nobody is coming for it, and leaking a
        // socket per unclaimed connection is worse than refusing one.
        std::vector<IoSocket> leftovers;
        { std::lock_guard<std::mutex> lk(impl->m); leftovers.swap(impl->ready); }
        for (IoSocket s : leftovers) ioplat::CloseSocket(s);
    }

    IoSocket IoAcceptor::TryTake() noexcept {
        if (!impl) return 0;
        std::lock_guard<std::mutex> lk(impl->m);
        if (impl->ready.empty()) return 0;
        const IoSocket s = impl->ready.back();
        impl->ready.pop_back();
        return static_cast<IoSocket>(s);
    }

    bool IoAcceptor::TakeOrQueue(IoAcceptWaiter* w, IoSocket* out, Task* resume, CancelToken token) {
        if (!impl || !w || !out) { if (out) *out = 0; return true; }

        // Already cancelled: answer now rather than queueing. A cancelled waiter in the queue would
        // take the next connection and drop it.
        if (CancelToken(token.Raw()).Cancelled()) { *out = 0; return true; }

        std::lock_guard<std::mutex> lk(impl->m);
        if (!impl->ready.empty()) {
            *out = static_cast<IoSocket>(impl->ready.back());
            impl->ready.pop_back();
            return true;                       // answer is final; do not suspend
        }
        if (impl->stopping) { *out = 0; return true; }

        w->resume = resume;
        w->out    = out;
        w->token  = token.Raw();
        w->next   = nullptr;
        if (impl->waitTail) impl->waitTail->next = w; else impl->waitHead = w;
        impl->waitTail = w;
        return false;                          // parked; an accept completion will resume it
    }

    std::size_t IoAcceptor::CancelWaiters(CancelToken token) noexcept {
        if (!impl) return 0;

        // REMOVED UNDER THE LOCK, RESUMED OUTSIDE IT -- each waiter lives in a frame that dies the
        // moment it is pushed, so leaving one linked while waking it dangles the list.
        IoAcceptWaiter* taken = nullptr;
        IoAcceptWaiter* takenTail = nullptr;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            IoAcceptWaiter* keepHead = nullptr;
            IoAcceptWaiter* keepTail = nullptr;
            for (IoAcceptWaiter* w = impl->waitHead; w; ) {
                IoAcceptWaiter* next = w->next;
                w->next = nullptr;
                const bool match = !token.Valid() || CancelToken(w->token).IsWithin(token);
                if (match) {
                    if (takenTail) takenTail->next = w; else taken = w;
                    takenTail = w;
                } else {
                    if (keepTail) keepTail->next = w; else keepHead = w;
                    keepTail = w;
                }
                w = next;
            }
            impl->waitHead = keepHead;
            impl->waitTail = keepTail;
        }

        std::size_t n = 0;
        while (taken) {
            IoAcceptWaiter* next = taken->next;
            if (taken->out) *taken->out = 0;
            Task* t = taken->resume;
            if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().Push(t);
            ++n;
            taken = next;
        }
        return n;
    }

    std::size_t IoAcceptor::Outstanding() const noexcept {
        if (!impl) return 0;
        std::lock_guard<std::mutex> lk(impl->m);
        return impl->outstanding;
    }

    std::size_t IoAcceptor::Available() const noexcept {
        if (!impl) return 0;
        std::lock_guard<std::mutex> lk(impl->m);
        return impl->ready.size();
    }

    // ---- BACKLOG TELEMETRY, UNCONDITIONAL AND ON EVERY PLATFORM -------------------------------
    //
    // Deliberately NOT behind JLIBSCHED_IO_LOCK_STATS. Once lane completions queue in the reactor
    // instead of going straight out, that deque IS the global queue for I/O, and "how deep did it
    // get" stops being a profiling curiosity and becomes the answer to whether the lane is keeping
    // up. A number available only in a special build is a number nobody has when it matters.
    //
    // HERE RATHER THAN IN THE WINDOWS BACKEND THAT WRITES THEM, because a test that reads these has
    // to link on a platform with no reactor. There they simply stay zero, which is the truthful
    // answer -- no reactor, no backlog.
    //
    // Five relaxed counters on a path that already makes a syscall per wake, so the cost is not
    // measurable.
    namespace detail {
        std::atomic<std::uint64_t> g_blDepth{ 0 };
        std::atomic<std::uint64_t> g_blHigh{ 0 };
        std::atomic<std::uint64_t> g_blPushed{ 0 };
        std::atomic<std::uint64_t> g_blDeclined{ 0 };
        std::atomic<std::uint64_t> g_blDrains{ 0 };
    }

    IoBacklogStats ReadIoBacklogStats() noexcept {
        IoBacklogStats s;
        s.depth     = detail::g_blDepth.load(std::memory_order_relaxed);
        s.highWater = detail::g_blHigh.load(std::memory_order_relaxed);
        s.pushed    = detail::g_blPushed.load(std::memory_order_relaxed);
        s.declined  = detail::g_blDeclined.load(std::memory_order_relaxed);
        s.drains    = detail::g_blDrains.load(std::memory_order_relaxed);
        return s;
    }

    // DOES NOT CLEAR `depth`. Depth is a live reading owned by the completion thread, not an
    // accumulator -- zeroing it here would publish a lie until the next drain pass corrected it.
    // Everything else is a running total and is what a caller means by "reset".
    void ResetIoBacklogStats() noexcept {
        detail::g_blHigh.store(0, std::memory_order_relaxed);
        detail::g_blPushed.store(0, std::memory_order_relaxed);
        detail::g_blDeclined.store(0, std::memory_order_relaxed);
        detail::g_blDrains.store(0, std::memory_order_relaxed);
    }

} // namespace JLib
