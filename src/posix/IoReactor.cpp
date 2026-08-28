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
#include "IoUring.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

namespace JLib {

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

    std::mutex               life;
    std::atomic<bool>        stopping{ true };   // starts stopped; Start() clears it
    bool                     running = false;
    std::vector<std::thread> workers;
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
static void CompletionLoop(IoReactor::Impl* impl) {
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
                res.bytes  = static_cast<std::uint32_t>(c.res);
                res.error  = 0;
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
bool IoReactor::IsAvailable() noexcept { return false; }

void IoReactor::Start() noexcept {
    std::lock_guard<std::mutex> lk(impl->life);
    if (!impl->ringUp) return;
    impl->stopping.store(false, std::memory_order_release);
    if (impl->running) return;

    // ONE COMPLETION THREAD FOR NOW. The Windows side runs several; how many this wants is a
    // measurement nobody has taken on a ring, and a second thread contending on one CQ is not
    // obviously a win. Starting at one keeps the first version honest.
    impl->workers.emplace_back([this] { CompletionLoop(impl); });
    impl->running = true;
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

bool IoReactor::Register(void*)            { return false; }
bool IoReactor::InitSockets()              { return false; }
bool IoReactor::RegisterSocket(IoSocket)   { return false; }

std::size_t IoReactor::RequestCancel(CancelToken) noexcept {
    // Nothing can be in flight while no Submit* works, so there is nothing to cancel. This becomes
    // an IORING_OP_ASYNC_CANCEL walk over the shards once operations exist.
    return 0;
}

} // namespace JLib

#endif // __linux__
