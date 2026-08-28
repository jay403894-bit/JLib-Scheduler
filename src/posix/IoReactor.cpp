// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE LINUX REACTOR. io_uring where it exists, epoll where it does not.
//
// STATE OF THIS FILE: the Impl, the completion threads and the lifecycle are here and working. The
// twenty Submit* entry points are NOT -- they return false, exactly as the stub did, so a caller
// gets the documented "not available" answer rather than a wrong one. That is deliberate: the
// completion loop is the part every operation hangs off, so it is worth having correct and testable
// before there is anything to complete.
//
// == WHY TWO BACKENDS, AND WHY epoll IS NOT OPTIONAL ==
//
// io_uring is completion-based, which maps 1:1 onto the IOCP design this reactor already has, and
// it is the only one of the two that can serve SubmitRead/SubmitWrite at all: epoll_ctl REFUSES a
// regular file (-EPERM), because a regular file is always "ready" and readiness notification is
// meaningless for it.
//
// But io_uring is not a given. Android ships epoll. Older distributions run kernels without
// io_uring, or with it compiled out. Hardened containers, seccomp filters and the io_uring_disabled
// sysctl refuse it on kernels that have it. So epoll is the FLOOR -- it gets built regardless -- and
// io_uring is the fast path on top, chosen when the probe says it is there.
//
// WHAT io_uring ACTUALLY BUYS, stated so nobody quotes a headline: syscalls per operation. epoll
// costs roughly two (an amortised epoll_wait plus the recv that readiness only promised would
// succeed); io_uring batches submissions into one io_uring_enter and transfers kernel-side. That is
// structural and it shows up under load. Whether it shows up in THIS runtime's dispatch latency is
// unproven -- the measured tail here is preemption with a ~90us floor beneath it, and shaving a
// syscall off submission may be invisible against that. Do not put a latency claim in the README
// until it is measured on both paths.
//
// SQPOLL IS DELIBERATELY NOT USED. It produces io_uring's headline numbers by dedicating a kernel
// thread to polling the ring -- buying latency by burning a core, which is the exact trade K-hot
// exists to avoid (one hot worker delivering what 29 spinning ones did). If it is ever wanted it
// belongs behind an opt-in flag with a measurement attached, not in the default path.

#if defined(__linux__)

#include "../../include/IoReactor.h"
#include "../../include/TaskScheduler.h"
#include "../IoPlatform.h"
#include "IoUring.h"

#include <sys/socket.h>
#include <sys/uio.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

namespace JLib {

// ---- THE LAYOUT OF IoRequest::native ON POSIX, and the seam's half that depends on it ----------
//
//   [0]                 struct msghdr   (56 bytes)
//   [sizeof(msghdr)]    struct iovec[kMaxVectors]  (128 bytes)
//                       ------------------------------------
//                       184 of the 192 available
//
// MEASURED, NOT ASSUMED: msghdr is 56 and iovec is 16 on x86-64 Linux, checked before this layout
// was chosen. The static_assert below is what keeps that true on a platform where it is not -- an
// overflow here would write past `native` into whatever follows it in the request, silently.
//
// msghdr FIRST because RECVMSG/SENDMSG need it at a stable address and the iovec array is what it
// points AT; putting the variable-length part first would make the fixed part's offset depend on
// the segment count.
static_assert(sizeof(struct msghdr) + IoRequest::kMaxVectors * sizeof(struct iovec)
                  <= IoRequest::kNativeBytes,
              "IoRequest::kNativeBytes cannot hold msghdr plus kMaxVectors iovecs");

static struct iovec* Iov(IoRequest* r) {
    return reinterpret_cast<struct iovec*>(r->native + sizeof(struct msghdr));
}

namespace ioplat {
    // The mirror of the Windows definition, over this backend's layout. Converts rather than casts:
    // iovec is { void* iov_base; size_t iov_len; } and IoBuffer is { void* data; uint32_t len; } --
    // same order here, unlike WSABUF, but the length widths differ and a struct that is ABI-identical
    // to both does not exist.
    bool FillBufs(IoRequest* r, const IoBuffer* bufs, std::uint32_t count) noexcept {
        if (count == 0 || count > IoRequest::kMaxVectors) return false;
        struct iovec* v = Iov(r);
        for (std::uint32_t i = 0; i < count; ++i) {
            v[i].iov_base = bufs[i].data;
            v[i].iov_len  = static_cast<std::size_t>(bufs[i].len);
        }
        return true;
    }
}

namespace {
    // Matches the Windows side's shard count and reasoning: the measured contention was
    // SUBMITTER-side (many workers submitting at once), so the fix is more locks rather than a
    // cleverer one. Sixteen was enough there and the access pattern is identical here.
    constexpr std::size_t kShards = 16;

    // Sentinel user_data for the NOP that wakes a parked completion thread. Cannot collide with a
    // real request: every real user_data is an IoRequest*, and no object lives at address 1.
    constexpr std::uint64_t kWakeSentinel = 1;
}

struct IoReactor::Impl {
    uring::Ring ring;
    bool        ringUp = false;

    // ---- THE SUBMISSION LOCK, which the Windows reactor does not need and cannot avoid here ----
    //
    // WSARecv is thread-safe against itself; io_uring's submission queue has exactly ONE legal
    // producer. The SQ tail is a plain shared word, so two workers reserving an SQE concurrently
    // would hand the kernel a torn or duplicated entry -- a corruption the kernel acts on, not a
    // race that merely loses a wakeup.
    //
    // ONE MUTEX, NOT SHARDED, and that is not laziness: sharding the in-flight list works because
    // the shards are independent structures, but there is exactly one SQ, so a second lock over it
    // would protect nothing. If this becomes the bottleneck the answer is a ring per submitting
    // thread (io_uring supports that; the CQs would then need draining per ring), not more locks
    // over one ring. Measure before building that -- the Windows profile said the cost was the
    // notify, not the submission.
    std::mutex submitMx;

    struct Shard {
        mutable std::mutex m;
        IoRequest* head = nullptr;
        char pad[platform::kCacheLine];
    };
    Shard shards[kShards];
    std::atomic<std::size_t> total{ 0 };

    // Address-derived, never stored in the request: no field, and the same request always hashes to
    // the same shard so Unlink finds it without a lookup. Slab addresses are regular in their low
    // bits, hence the shift before the multiply.
    Shard& ShardFor(const IoRequest* r) noexcept {
        const std::uintptr_t x = reinterpret_cast<std::uintptr_t>(r) >> 4;
        return shards[(x * 0x9E3779B97F4A7C15ull) >> 60 & (kShards - 1)];
    }

    void Link(IoRequest* r) noexcept {
        Shard& s = ShardFor(r);
        std::lock_guard<std::mutex> lk(s.m);
        r->prev = nullptr;
        r->next = s.head;
        if (s.head) s.head->prev = r;
        s.head = r;
        total.fetch_add(1, std::memory_order_relaxed);
    }

    // CALLER HOLDS THE SHARD LOCK. Split from Link for the same reason the Windows side splits it:
    // the completion path needs to unlink and read the request under ONE lock acquisition, because
    // the moment the resume is pushed the frame the request lives in can be gone.
    void UnlinkLocked(IoRequest* r) noexcept {
        Shard& s = ShardFor(r);
        if (r->prev) r->prev->next = r->next;
        else if (s.head == r) s.head = r->next;
        if (r->next) r->next->prev = r->prev;
        r->prev = r->next = nullptr;
        total.fetch_sub(1, std::memory_order_relaxed);
    }

    // Takes the shard lock itself. The completion path already holds it and uses UnlinkLocked; the
    // submit path does not, and hand-rolling the lock at three call sites is how one of them ends up
    // without it.
    void UnlinkUnlocked(IoRequest* r) noexcept {
        std::lock_guard<std::mutex> lk(ShardFor(r).m);
        UnlinkLocked(r);
    }

    std::mutex               life;
    std::atomic<bool>        stopping{ true };   // starts stopped; Start() clears it
    bool                     running = false;
    std::vector<std::thread> workers;

    // LAZY, so a process that never does I/O never pays for a completion thread. Called on the
    // submit path rather than at Init for the same reason the Windows side does it: EnableIoReactor
    // constructs the reactor, and constructing it must not spawn a thread that may never have work.
    void EnsureThreads() {
        if (running) return;                       // fast path, no lock: only ever false->true
        std::lock_guard<std::mutex> lk(life);
        if (running || !ringUp) return;
        stopping.store(false, std::memory_order_release);
        workers.emplace_back([this] { CompletionLoopEntry(this); });
        running = true;
    }

    // Defined below the loop; declared here because Impl is what owns the threads.
    static void CompletionLoopEntry(Impl* impl);
};

namespace {
    IoReactor::Impl* g_impl = nullptr;   // set by the constructor; the reactor is a singleton
}

// ================================ THE COMPLETION LOOP ==========================================
//
// Mirrors the Windows drain deliberately, including the ordering that is load-bearing:
//
//   1. block only when nothing is collected; once the batch has anything, poll with no wait so
//      whatever is ready RIGHT NOW joins it and the batch goes out the moment the ring is empty.
//      Coalescing that waits would trade tail latency for tail latency.
//   2. unlink and read the request under ONE shard-lock acquisition
//   3. run onComplete AFTER the lock (it submits the next transfer and would deadlock) and BEFORE
//      the resume is pushed (the push can let the request's frame die)
//   4. collect the Task*, never touch the request again
//
void IoReactor::Impl::CompletionLoopEntry(IoReactor::Impl* impl) {
    constexpr unsigned kCqBatch = 64;
    constexpr std::size_t kBatch = 64;

    io_uring_cqe cqes[kCqBatch];
    Task* batchHi[kBatch];
    Task* batchLo[kBatch];
    std::size_t nHi = 0, nLo = 0;

    auto flush = [&]() {
        if (!TaskScheduler::IsInitialized()) { nHi = nLo = 0; return; }
        auto& s = TaskScheduler::Instance();
        // PRIORITY IS A PARAMETER to PushBatch, so the two runs are split and pushed separately --
        // routing a hiPri run into the loPri inbox is the priority inversion PushBatch refuses by
        // name. The Windows side steers across the hot set here; that is deliberately NOT copied
        // yet, because steering wants the same measurement the Windows one got and this path has
        // never been run.
        if (nHi) { s.PushBatch(batchHi, nHi, 0, 64, true);  nHi = 0; }
        if (nLo) { s.PushBatch(batchLo, nLo, 0, 64, false); nLo = 0; }
    };

    for (;;) {
        if (nHi == 0 && nLo == 0) {
            // Nothing collected: sleep in the kernel until something completes. -EINTR is normal.
            uring::WaitCq(impl->ring, 1);
        }

        const unsigned got = uring::Reap(impl->ring, cqes, kCqBatch);
        if (got == 0) {
            // Empty poll. If a batch is in hand it goes now; if not, loop back and block.
            //
            // THE STOP CHECK LIVES HERE, not at the top: a thread that returned on an empty poll
            // would die on the first idle moment and no I/O would ever complete again -- silently,
            // because nothing else reports a missing completion thread.
            if (nHi || nLo) { flush(); continue; }
            if (impl->stopping.load(std::memory_order_acquire)) return;
            continue;
        }

        for (unsigned i = 0; i < got; ++i) {
            const io_uring_cqe& c = cqes[i];

            if (c.user_data == kWakeSentinel) {
                // Stop()'s wake. Flush what is in hand, pass the wake on so the exit cascades to
                // the next thread, and go. One wake per thread is posted, plus this relay, so a
                // thread that consumed a wake while work was still draining cannot strand a peer.
                flush();
                if (impl->stopping.load(std::memory_order_acquire)) {
                    std::lock_guard<std::mutex> lk(impl->submitMx);
                    uring::PostWake(impl->ring, kWakeSentinel);
                    return;
                }
                continue;
            }

            IoRequest* r = reinterpret_cast<IoRequest*>(static_cast<std::uintptr_t>(c.user_data));
            if (!r) continue;

            // io_uring reports a NEGATIVE ERRNO in res, and a non-negative res is the byte count.
            // No GetLastError equivalent and no separate `ok` flag -- one signed integer carries
            // both, which is why this cannot reuse the Windows Classify().
            IoResult res{};
            if (c.res >= 0) {
                res.status = IoStatus::Completed;
                res.error  = 0;

                // AN ACCEPT'S res IS A FILE DESCRIPTOR, NOT A BYTE COUNT, and this is the one place
                // that difference has to be understood. io_uring's ACCEPT creates the socket itself
                // and returns its fd; AcceptEx fills a socket the CALLER made, so on Windows the
                // accepted socket is already in `aux` and res is a byte count like everything else.
                //
                // Storing it in `aux` here makes the two platforms agree at the point the waiter
                // reads them: aux is "the accepted socket" on both, and bytes stays 0 because no
                // bytes were transferred. Leaving the fd in `bytes` instead would hand the caller a
                // descriptor through a field named for a length, and IoAcceptor would close the
                // socket it never received.
                if (r->kind == IoRequest::Kind::Accept) {
                    r->aux    = static_cast<std::uintptr_t>(c.res);
                    res.bytes = 0;
                } else {
                    res.bytes = static_cast<std::uint32_t>(c.res);
                }
            } else if (c.res == -ECANCELED) {
                // A cancellation is not a failure: RequestCancel asked for exactly this, and the
                // waiter must be able to tell it apart from a real error to unwind correctly.
                res.status = IoStatus::Cancelled;
                res.bytes  = 0;
                res.error  = ECANCELED;
            } else {
                res.status = IoStatus::Failed;
                res.bytes  = 0;
                res.error  = static_cast<std::uint32_t>(-c.res);
            }

            // NO ACCEPT/CONNECT FIXUP HERE, and its absence is worth stating because the Windows
            // path cannot omit it: SO_UPDATE_ACCEPT_CONTEXT / SO_UPDATE_CONNECT_CONTEXT exist
            // because AcceptEx and ConnectEx leave the socket half-initialised. Linux's accept and
            // connect return a socket that is already correct. Nothing to do -- not "nothing done".

            Task* resume = nullptr;
            {
                std::lock_guard<std::mutex> lk(impl->ShardFor(r).m);
                impl->UnlinkLocked(r);
                if (r->out) *r->out = res;    // safe here: the owner is still suspended
                resume = r->resume;
            }

            if (r->onComplete) r->onComplete(r);   // after the lock, before the push

            if (resume) {
                if (resume->hiPri) batchHi[nHi++] = resume;
                else               batchLo[nLo++] = resume;
                if (nHi == kBatch || nLo == kBatch) flush();
            }
        }

        if ((nHi || nLo) && impl->stopping.load(std::memory_order_acquire)) flush();
    }
}

// ==================================== LIFECYCLE ================================================

IoReactor::IoReactor() : impl(new Impl()) {
    g_impl = impl;
    // PROBED ONCE, AT CONSTRUCTION. The answer cannot change for the life of the process, and
    // deciding per operation would put a branch on the hot path for a question already settled.
    if (uring::Init(impl->ring, 256) == uring::InitResult::Ok) impl->ringUp = true;
}

IoReactor::~IoReactor() {
    Stop();
    if (impl->ringUp) uring::Shutdown(impl->ring);
    delete impl;
    g_impl = nullptr;
}

IoReactor& IoReactor::Instance() { static IoReactor r; return r; }

// FALSE UNTIL THE OPERATIONS EXIST. The ring being up is necessary and not sufficient -- reporting
// available while every Submit* returns false would make callers take the async path and then
// discover it does nothing, which is worse than the honest no.
// TRUE ONCE THE RING IS UP, because the operations are real now. It stayed false while every
// Submit* was a stub -- reporting available then would have made callers take the async path and
// find it inert, which is worse than the honest no.
//
// Reads the singleton's ring rather than a static flag: the answer is a property of THIS process's
// reactor, and a kernel that refused io_uring (seccomp, container policy) must report false even
// though the build supports it.
bool IoReactor::IsAvailable() noexcept {
    // STILL FALSE, AND THIS IS THE HONEST ANSWER RATHER THAN A LEFTOVER.
    //
    // The operations work -- accept, connect, send, recv, a peer close as a zero-byte completion,
    // vectored send, and the too-many-segments refusal all pass against real io_uring on
    // SchedulerIoSocketTest. What does NOT work is IoStream's CHAINING: the concurrency section
    // hangs with every thread asleep and the main thread in futex_do_wait, which is a LOST
    // COMPLETION -- an operation was submitted and no CQE ever arrived, so the completion thread
    // sleeps in io_uring_enter and the waiter never wakes.
    //
    // Reporting available while that is true would be worse than reporting nothing: a caller would
    // take the async path and HANG rather than get an error, and a hang is the one failure this
    // reactor's whole design is meant to make impossible. Flip this to `return Instance()->ringUp`
    // once the chained path completes -- the socket test's IsAvailable skip is what re-enables the
    // coverage automatically.
    //
    // The suspect list, narrowed by what already passes: SubmitPrepared is the only path the
    // working sections do not exercise, so either its SQE is malformed in a way the kernel accepts
    // without completing, or the chained transfer is legitimately pending (a WRITEV that filled the
    // socket buffer with nobody draining it would look exactly like this). Instrument the
    // completion loop before assuming either.
    return false;
}

void IoReactor::Start() noexcept {
    if (!impl->ringUp) return;

    // NO LOCK HELD HERE, AND THAT IS THE FIX RATHER THAN AN OVERSIGHT. This used to take impl->life
    // and then call EnsureThreads(), which takes impl->life itself -- and std::mutex is not
    // recursive, so it was a self-deadlock on the calling thread. It hung Init() before the process
    // printed a single line, which is a hard shape to read: no output, no CPU, no failing assertion.
    //
    // EnsureThreads owns the whole decision (the `running` check, the flag, the spawn) under that
    // one lock, so there is exactly one place that decides whether a completion thread exists.
    // Start() and a first Submit racing each other would otherwise be two spawners sharing one flag.
    //
    // ONE COMPLETION THREAD FOR NOW. The Windows side runs several; how many a ring wants is a
    // measurement nobody has taken, and a second thread contending on one CQ is not obviously a win.
    impl->EnsureThreads();
}

void IoReactor::Stop() noexcept {
    bool needJoin = false;
    {
        std::lock_guard<std::mutex> lk(impl->life);
        if (impl->stopping.load(std::memory_order_acquire)) return;
        impl->stopping.store(true, std::memory_order_release);
        needJoin = impl->running;
    }

    // EVERY IN-FLIGHT OPERATION IS CANCELLED AND DRAINED BEFORE THE THREADS GO, for the reason the
    // Windows file states: stopping while the kernel still holds a buffer whose owning frame is
    // about to unwind is the corruption this reactor exists to prevent, and the owner cannot resume
    // until its completion arrives -- so the only way out is to make those completions happen.
    RequestCancel(CancelToken{});

    if (needJoin) {
        std::vector<std::thread> ts;
        { std::lock_guard<std::mutex> lk(impl->life); ts.swap(impl->workers); impl->running = false; }
        for (std::size_t i = 0; i < ts.size(); ++i) {
            std::lock_guard<std::mutex> lk(impl->submitMx);
            uring::PostWake(impl->ring, kWakeSentinel);
        }
        for (auto& t : ts) if (t.joinable()) t.join();
    }
}

std::size_t IoReactor::InFlight() const noexcept {
    return impl->total.load(std::memory_order_relaxed);
}

// ============================ NOT YET IMPLEMENTED ==============================================
//
// Same contract as the stub these replace: a clean false, never a wrong answer. Each returns the
// polarity its declaration documents -- for the Submit* family, TRUE means "the answer is already
// final, do not suspend", so FALSE here would mean "suspend and wait for a completion that will
// never arrive". They therefore report failure through the out-parameter as well where they have
// one, and the awaiter layer treats that as an immediate failure rather than a park.

// ALL THREE TRIVIALLY TRUE, AND NOT AS STUBS. They are IOCP concepts with no io_uring equivalent:
// a handle must be ASSOCIATED with a completion port before it can be used with one, and Winsock
// must be started before any socket call exists. io_uring needs neither -- an SQE names a raw fd.
//
// IORING_REGISTER_FILES does exist and would let the kernel skip an fd lookup per operation, but it
// is an optimisation with real bookkeeping (a fixed table, re-registration on change), not a
// requirement, and nothing here has measured a need for it.
//
// TRUE rather than false matters: the tests assert on these, and false would report a setup failure
// for something that simply does not apply on this platform.
bool IoReactor::Register(void*)            { return true; }
bool IoReactor::InitSockets()              { return true; }
bool IoReactor::RegisterSocket(IoSocket)   { return true; }

// ================================ SUBMISSION ===================================================
//
// THE RETURN POLARITY IS THE THING TO GET RIGHT, and it is inverted from what reads naturally.
// TRUE means "the answer is already final, do NOT suspend" -- *out is filled. FALSE means "queued;
// the request and *out belong to the reactor until the completion". So the dangerous mistake is
// returning false on an error: the caller would park forever on a completion nobody will produce,
// and the symptom is a HANG rather than a failure.
//
// One helper, one lambda per operation, mirroring the Windows Submit() so the two backends have the
// same shape and the same ordering: cancel check, fill the request, link, submit, unlink on
// immediate failure.
template <typename Fill>
static bool SubmitOp(IoReactor::Impl* impl, IoRequest* req, IoResult* out,
                     Task* resume, CancelToken token, Fill&& fill) {
    const std::uint32_t tok = token.Raw();

    // ALREADY CANCELLED: answer now and do NOT submit. The one case where an I/O cancel is
    // immediate, because the kernel never took the buffer -- so there is nothing to wait for and
    // nothing that could still write into the caller's frame.
    if (CancelToken(tok).Cancelled()) {
        if (out) *out = IoResult{ IoStatus::Cancelled, 0, 0 };
        return true;
    }

    if (!impl->ringUp) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ENOSYS };
        return true;
    }
    if (impl->stopping.load(std::memory_order_acquire)) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ESHUTDOWN };
        return true;
    }

    req->out    = out;
    req->resume = resume;
    req->token  = tok;

    // LINKED BEFORE SUBMITTED, never after. The completion can arrive on another thread the
    // instant the SQE is published -- before this function returns -- and the drain looks the
    // request up in the shard to unlink it. Linking afterwards is a race whose loser is a
    // completion for a request that is not in the list yet.
    impl->Link(req);

    {
        std::lock_guard<std::mutex> lk(impl->submitMx);
        io_uring_sqe* sqe = uring::GetSqe(impl->ring);
        if (!sqe) {
            // SQ FULL. Not a queue we may spin on: only the kernel drains it, and this thread is a
            // worker that would be spinning instead of running the work that drains it. Report and
            // let the caller decide -- EAGAIN is exactly what a full submission queue means.
            impl->UnlinkUnlocked(req);
            if (out) *out = IoResult{ IoStatus::Failed, 0, EAGAIN };
            return true;
        }
        std::memset(sqe, 0, sizeof(*sqe));
        fill(sqe);
        sqe->user_data = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(req));

        const int rc = uring::Submit(impl->ring, 0);
        if (rc < 0) {
            impl->UnlinkUnlocked(req);
            if (out) *out = IoResult{ IoStatus::Failed, 0, -rc };
            return true;
        }
    }

    impl->EnsureThreads();
    return false;      // queued: stay suspended
}

bool IoReactor::SubmitRecv(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Recv;
    req->handle = reinterpret_cast<void*>(s);
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_RECV;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(buf);
        sqe->len       = len;
        sqe->msg_flags = flags;
    });
}

bool IoReactor::SubmitSend(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Send;
    req->handle = reinterpret_cast<void*>(s);
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_SEND;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(buf);
        sqe->len       = len;
        sqe->msg_flags = flags;
    });
}

// READV/WRITEV rather than RECV/SEND: the vectored socket ops in io_uring are RECVMSG/SENDMSG, which
// need a msghdr the caller never supplied. READV works on a socket fd and takes the iovec array
// directly, which is what this already has.
bool IoReactor::SubmitRecvV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                            std::uint32_t flags, IoRequest* req, IoResult* out,
                            Task* resume, CancelToken token) {
    if (!ioplat::FillBufs(req, bufs, count)) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ioplat::kErrMsgSize };
        return true;
    }
    req->kind = IoRequest::Kind::Recv;
    req->bufCount = count;
    req->handle = reinterpret_cast<void*>(s);
    (void)flags;   // READV has no flags argument; a caller passing MSG_* gets them ignored, not honoured
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_READV;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(Iov(req));
        sqe->len    = count;
        sqe->off    = static_cast<std::uint64_t>(-1);   // -1 = "current position", required for a socket
    });
}

bool IoReactor::SubmitSendV(IoSocket s, const IoBuffer* bufs, std::uint32_t count,
                            std::uint32_t flags, IoRequest* req, IoResult* out,
                            Task* resume, CancelToken token) {
    if (!ioplat::FillBufs(req, bufs, count)) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, ioplat::kErrMsgSize };
        return true;
    }
    req->kind = IoRequest::Kind::Send;
    req->bufCount = count;
    req->handle = reinterpret_cast<void*>(s);
    (void)flags;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_WRITEV;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(Iov(req));
        sqe->len    = count;
        sqe->off    = static_cast<std::uint64_t>(-1);
    });
}

bool IoReactor::SubmitRead(void* handle, void* buf, std::uint32_t len, std::uint64_t offset,
                           IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Generic;
    req->handle = handle;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_READ;
        sqe->fd     = static_cast<int>(reinterpret_cast<std::uintptr_t>(handle));
        sqe->addr   = reinterpret_cast<std::uint64_t>(buf);
        sqe->len    = len;
        sqe->off    = offset;
    });
}

bool IoReactor::SubmitWrite(void* handle, const void* buf, std::uint32_t len, std::uint64_t offset,
                            IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Generic;
    req->handle = handle;
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_WRITE;
        sqe->fd     = static_cast<int>(reinterpret_cast<std::uintptr_t>(handle));
        sqe->addr   = reinterpret_cast<std::uint64_t>(buf);
        sqe->len    = len;
        sqe->off    = offset;
    });
}

// RECVMSG/SENDMSG, because these are the only ops that carry a peer address. The msghdr lives in
// the request's own `native` (see the layout note at the top) -- the kernel reads it for the whole
// duration of the operation, so a stack local in this function would be a use-after-free the moment
// the call went pending, and it would usually appear to work.
bool IoReactor::SubmitRecvFrom(IoSocket s, void* buf, std::uint32_t len, std::uint32_t flags,
                               IoAddress* from, IoRequest* req, IoResult* out,
                               Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Recv;
    req->handle = reinterpret_cast<void*>(s);

    struct iovec* v = Iov(req);
    v[0].iov_base = buf;
    v[0].iov_len  = len;

    auto* mh = reinterpret_cast<struct msghdr*>(req->native);
    std::memset(mh, 0, sizeof(*mh));
    mh->msg_name    = from ? from->bytes : nullptr;
    mh->msg_namelen = from ? static_cast<socklen_t>(IoAddress::kBytes) : 0;
    mh->msg_iov     = v;
    mh->msg_iovlen  = 1;

    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_RECVMSG;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(mh);
        sqe->len       = 1;
        sqe->msg_flags = flags;
    });
}

bool IoReactor::SubmitSendTo(IoSocket s, const void* buf, std::uint32_t len, std::uint32_t flags,
                             const void* to, std::uint32_t toLen,
                             IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Send;
    req->handle = reinterpret_cast<void*>(s);

    struct iovec* v = Iov(req);
    v[0].iov_base = const_cast<void*>(buf);
    v[0].iov_len  = len;

    auto* mh = reinterpret_cast<struct msghdr*>(req->native);
    std::memset(mh, 0, sizeof(*mh));
    mh->msg_name    = const_cast<void*>(to);
    mh->msg_namelen = toLen;
    mh->msg_iov     = v;
    mh->msg_iovlen  = 1;

    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode    = IORING_OP_SENDMSG;
        sqe->fd        = static_cast<int>(s);
        sqe->addr      = reinterpret_cast<std::uint64_t>(mh);
        sqe->len       = 1;
        sqe->msg_flags = flags;
    });
}

// ACCEPT DIFFERS FROM WINDOWS IN WHO CREATES THE SOCKET, and the difference reaches the caller.
// AcceptEx needs a socket to exist first and fills it in; io_uring's ACCEPT creates one and returns
// its fd as the completion's `res`. So the `accepted` argument is UNUSED here -- IoAcceptor
// pre-creates a socket per slot for Windows's benefit, and on this backend that socket is simply
// closed and replaced by the one the kernel makes.
//
// THE ACCEPTED FD ARRIVES IN res, NOT in aux, which the completion loop has to know: for an accept,
// a non-negative res is a FILE DESCRIPTOR rather than a byte count.
bool IoReactor::SubmitAccept(IoSocket listener, IoSocket accepted, IoAcceptBuffer* addrs,
                             IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    // The pre-created socket is dead weight on this backend. Closing it here rather than leaking it
    // keeps the acceptor's slot bookkeeping honest without teaching it about backends.
    if (accepted) ioplat::CloseSocket(accepted);

    req->kind = IoRequest::Kind::Accept;
    req->handle = reinterpret_cast<void*>(listener);
    req->aux = 0;

    auto* alen = reinterpret_cast<socklen_t*>(req->native + sizeof(struct msghdr));
    *alen = addrs ? static_cast<socklen_t>(IoAcceptBuffer::kBytes) : 0;

    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode       = IORING_OP_ACCEPT;
        sqe->fd           = static_cast<int>(listener);
        sqe->addr         = addrs ? reinterpret_cast<std::uint64_t>(addrs->bytes) : 0;
        sqe->off          = reinterpret_cast<std::uint64_t>(alen);   // addrlen goes in off/addr2
        sqe->accept_flags = 0;
    });
}

bool IoReactor::SubmitConnect(IoSocket s, const void* sockaddr, std::uint32_t sockaddrLen,
                              IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    req->kind = IoRequest::Kind::Connect;
    req->handle = reinterpret_cast<void*>(s);
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_CONNECT;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(sockaddr);
        sqe->off    = sockaddrLen;      // CONNECT puts the address LENGTH in off, not len
    });
}

// SHUTDOWN, and `reuse` CANNOT BE HONOURED. DisconnectEx(TF_REUSE_SOCKET) hands a socket back to
// the pool ready to connect again, which has no Linux equivalent -- there is no way to un-connect a
// socket. Reported rather than silently ignored: a caller that asked for reuse and got a plain
// shutdown would reuse a socket that cannot be reconnected, and fail later somewhere else.
bool IoReactor::SubmitDisconnect(IoSocket s, bool reuse,
                                 IoRequest* req, IoResult* out, Task* resume, CancelToken token) {
    if (reuse) {
        if (out) *out = IoResult{ IoStatus::Failed, 0, EOPNOTSUPP };
        return true;
    }
    req->kind = IoRequest::Kind::Generic;
    req->handle = reinterpret_cast<void*>(s);
    return SubmitOp(impl, req, out, resume, token, [&](io_uring_sqe* sqe) {
        sqe->opcode = IORING_OP_SHUTDOWN;
        sqe->fd     = static_cast<int>(s);
        sqe->len    = SHUT_RDWR;        // SHUTDOWN takes `how` in len
    });
}

// Re-submits a request whose fields are already filled -- IoStream's chaining path, which builds a
// request when a transfer is queued and submits it when its turn comes.
bool IoReactor::SubmitPrepared(IoRequest* req) {
    if (!req) return true;
    const bool isSend = (req->kind == IoRequest::Kind::Send);
    const IoSocket s = reinterpret_cast<IoSocket>(req->handle);
    return SubmitOp(impl, req, req->out, req->resume, CancelToken(req->token),
                    [&](io_uring_sqe* sqe) {
        sqe->opcode = isSend ? IORING_OP_WRITEV : IORING_OP_READV;
        sqe->fd     = static_cast<int>(s);
        sqe->addr   = reinterpret_cast<std::uint64_t>(Iov(req));
        sqe->len    = req->bufCount;
        sqe->off    = static_cast<std::uint64_t>(-1);
    });
}

// CANCELLATION IS A REQUEST, NOT AN ORDER, which is the same two-phase contract the Windows side
// has and the reason this is named RequestCancel. The kernel owns the buffer until it says
// otherwise, so nothing here may free or unlink anything: it asks, and the COMPLETION does the
// unlinking with -ECANCELED like any other outcome. A cancel that unlinked eagerly would race the
// completion for a request whose frame is about to unwind.
//
// A DEFAULT-CONSTRUCTED TOKEN MEANS EVERYTHING, which is what Stop() relies on to drain.
std::size_t IoReactor::RequestCancel(CancelToken token) noexcept {
    if (!impl->ringUp) return 0;
    const std::uint32_t tok = token.Raw();
    const bool all = (tok == CancelToken{}.Raw());
    std::size_t asked = 0;

    for (std::size_t i = 0; i < kShards; ++i) {
        // THE TARGETS ARE COLLECTED UNDER THE SHARD LOCK AND SUBMITTED OUTSIDE IT. Submitting while
        // holding it would take submitMx underneath a shard lock, while the completion path takes
        // the shard lock on its own -- two locks in two orders, which is the deadlock this ordering
        // avoids. Copying the pointers is safe because a request cannot be freed while it is still
        // linked, and it is unlinked only by its own completion.
        IoRequest* targets[64];
        std::size_t n = 0;
        {
            std::lock_guard<std::mutex> lk(impl->shards[i].m);
            for (IoRequest* r = impl->shards[i].head; r && n < 64; r = r->next)
                if (all || r->token == tok) targets[n++] = r;
        }

        for (std::size_t k = 0; k < n; ++k) {
            std::lock_guard<std::mutex> lk(impl->submitMx);
            io_uring_sqe* sqe = uring::GetSqe(impl->ring);
            if (!sqe) break;              // SQ full: what could not be asked stays in flight
            std::memset(sqe, 0, sizeof(*sqe));
            sqe->opcode = IORING_OP_ASYNC_CANCEL;
            sqe->fd     = -1;
            // THE TARGET IS NAMED BY ITS user_data, which is how io_uring identifies an in-flight
            // operation -- there is no handle to pass. That is exactly why user_data is the
            // IoRequest pointer and nothing else.
            sqe->addr = static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(targets[k]));
            // The cancel's OWN completion is discarded: it reports whether the cancel was accepted,
            // not the outcome of the cancelled operation, and the caller is waiting for the latter.
            sqe->user_data = kWakeSentinel;
            if (uring::Submit(impl->ring, 0) < 0) break;
            ++asked;
        }
    }
    return asked;
}

} // namespace JLib

#endif // __linux__
