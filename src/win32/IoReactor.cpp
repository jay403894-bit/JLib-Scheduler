// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Windows I/O completion ports. See include/IoReactor.h for the model; this is the mechanism.

#include "../../include/IoReactor.h"
#include "../../include/TaskScheduler.h"
#include "../../include/platform.h"      // the ONLY place windows.h comes from
#include "../../include/Timer.h"         // MonotonicNs, for the dispatch-latency stamp

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
#include <chrono>

namespace JLib {

#if defined(JLIBSCHED_IO_LOCK_STATS)
    namespace detail {
        // Per-logical-processor wake counts for the completion threads. Diagnostic: it exists to
        // answer whether the reactor and the hot worker share an efficiency class and a cache
        // domain, which no latency number can answer on its own.
        std::atomic<unsigned> g_complCore[64];
    }
#endif

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

    // A std::mutex that counts how often an acquisition actually had to WAIT. Satisfies Lockable, so
    // every std::lock_guard site is unchanged; only the type alias below moves.
    //
    // The try_lock-then-lock shape is what makes `contended` mean something: a plain counter would
    // tell you how busy the lock is, not whether anyone queued behind it.
#if defined(JLIBSCHED_IO_LOCK_STATS)
    static std::atomic<std::uint64_t> g_ioAcquires{ 0 };
    static std::atomic<std::uint64_t> g_ioContended{ 0 };

    struct CountingMutex {
        std::mutex m;
        void lock() {
            if (!m.try_lock()) {
                g_ioContended.fetch_add(1, std::memory_order_relaxed);
                m.lock();
            }
            g_ioAcquires.fetch_add(1, std::memory_order_relaxed);
        }
        void unlock() { m.unlock(); }
    };
    using IoMutex = CountingMutex;
#else
    using IoMutex = std::mutex;
#endif

    IoLockStats ReadIoLockStats() noexcept {
#if defined(JLIBSCHED_IO_LOCK_STATS)
        return IoLockStats{ g_ioAcquires.load(std::memory_order_relaxed),
                            g_ioContended.load(std::memory_order_relaxed) };
#else
        return IoLockStats{};   // zeros: the counters are not compiled in
#endif
    }

    void ResetIoLockStats() noexcept {
#if defined(JLIBSCHED_IO_LOCK_STATS)
        g_ioAcquires.store(0, std::memory_order_relaxed);
        g_ioContended.store(0, std::memory_order_relaxed);
#endif
    }

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
    static LPFN_DISCONNECTEX  g_disconnectEx = nullptr;
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

        // DisconnectEx is resolved on the same trip but NOT required: a provider without it is
        // perfectly usable, it just cannot reuse sockets. Failing InitSockets over an optional
        // optimisation would take accept and connect down with it.
        GUID gDisc = WSAID_DISCONNECTEX;
        ::WSAIoctl(probe, SIO_GET_EXTENSION_FUNCTION_POINTER, &gDisc, sizeof gDisc,
                   &g_disconnectEx, sizeof g_disconnectEx, &got, nullptr, nullptr);
        ::closesocket(probe);
        return ok && g_acceptEx && g_connectEx;
    }

    struct IoReactor::Impl {
        HANDLE port = nullptr;

        // SHARDED IN-FLIGHT LIST. The measured contention was submitter-side -- many workers calling
        // Submit at once -- so the fix is to give them different locks rather than a cleverer one.
        //
        // WHY THIS AND NOT LOCK-FREE. The structure needs removal BY IDENTITY from the middle and a
        // full traversal for cancel-by-scope, so no queue replaces it, and a lock-free doubly-linked
        // list would need safe reclamation over COROUTINE FRAMES -- a walker can hold a request whose
        // frame the resumed coroutine has already destroyed. Sharding leaves that invariant exactly
        // as it was (unlink and resume still happen under one lock) and just makes the lock narrower.
        //
        // The shard is derived from the request's ADDRESS, not stored in it: no field, and the same
        // request always lands in the same shard, so Unlink finds it without a lookup.
        static constexpr std::size_t kShards = 16;

        struct Shard {
            mutable IoMutex m;
            IoRequest* head = nullptr;      // in-flight, newest first
            // Padded: two shards sharing a cache line would trade false sharing for lock contention,
            // which is not the trade being made here.
            char pad[platform::kCacheLine];
        };

        Shard shards[kShards];
        std::atomic<std::size_t> total{ 0 };     // across all shards, for InFlight and the drain

        // Addresses come from the task slab in size classes, so the low bits are regular; a multiply
        // hash spreads them instead of clustering every request of one size onto one shard.
        Shard& ShardFor(const IoRequest* r) noexcept {
            const std::uintptr_t x = reinterpret_cast<std::uintptr_t>(r) >> 4;
            return shards[(x * 2654435761u) % kShards];
        }

        // MANY THREADS ON ONE PORT is what IOCP is built for, and the port's own concurrency limit
        // (set at creation) caps how many the kernel lets RUN at once -- so extra threads cost a
        // stack and nothing else while they are parked in GetQueuedCompletionStatus. The count comes
        // from TaskScheduler because the pool reserves one core for each, and the two must agree.
        std::mutex life;                          // thread start/stop only; never on the I/O path
        std::vector<std::thread> workers;
        bool running = false;
        std::atomic<bool> stopping{ false };      // read from every shard, so not under any one lock

        // Caller holds THE REQUEST'S SHARD LOCK. Entries are owned by their CALLER -- a coroutine
        // frame, suspended -- so the same rule as the condition variable applies: a request is
        // unlinked BEFORE its task is pushed, never after, or a later cancel pass walks into a frame
        // that has resumed and gone.
        void Link(IoRequest* r) {
            Shard& s = ShardFor(r);
            r->prev = nullptr;
            r->next = s.head;
            if (s.head) s.head->prev = r;
            s.head = r;
            total.fetch_add(1, std::memory_order_relaxed);
        }

        void Unlink(IoRequest* r) {
            Shard& s = ShardFor(r);
            if (r->prev) r->prev->next = r->next;
            else if (s.head == r) s.head = r->next;
            if (r->next) r->next->prev = r->prev;
            r->prev = r->next = nullptr;
            total.fetch_sub(1, std::memory_order_relaxed);
        }

        void EnsureThreads() {
            std::lock_guard<std::mutex> lk(life);
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

        // COALESCED PUSH. Completions that are already queued are drained without blocking and their
        // resumes handed to the scheduler in ONE PushBatch rather than one Push each.
        //
        // This is interrupt coalescing, in user space and for the same reason: the expensive part of
        // waking a parked worker is paid per SCHEDULER INTERACTION, not per completion, so a burst
        // of ready operations should cost one wake rather than N. PushBatch already exists and
        // replacing a requeue loop with it measured 7.5-8.2x in 2.2.0.
        //
        // IT CANNOT HELP A LONE COMPLETION, and that limit is worth stating: with nothing else
        // queued the batch is one entry and this is exactly the old path. It buys the BURST tail,
        // not the idle p50.
        static constexpr std::size_t kBatch = 32;

        void Run() {
            // THE PRODUCER HALF of the same knob the hot workers read. Elevating the consumer alone
            // would just move the preemption: a hot worker at priority 15 waiting on a completion
            // thread at 8 still waits for whatever preempts the completion thread. Both ends of the
            // handoff, or neither.
            //
            // Safe to leave raised for the thread's whole life because this thread is BLOCKED in
            // GetQueuedCompletionStatus whenever it is not draining -- it cannot spin at priority 15
            // and starve anything, which is the risk that makes this dangerous for a worker.
            if (TaskScheduler::GetHotThreadPolicy() != TaskScheduler::HotThreadPolicy::Normal)
                ::SetThreadPriority(::GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);

            // And in exclusive mode, off the hot cores. The completion thread is a PRODUCER for the
            // hot workers, so sharing a core with one is the single worst placement available: it
            // would preempt exactly the thread it is trying to hand work to.
            TaskScheduler::ExcludeCurrentThreadFromHotCpus();

            // SPLIT BY PRIORITY. Push honours task->hiPri; PushBatch takes it as a PARAMETER and
            // applies it to the whole batch -- so batching them together silently forced every
            // resumption to low priority and threw away what the app chose at Spawn. Two runs, two
            // flushes, priority preserved.
            //
            // The reactor deliberately does NOT impose a priority of its own. Whether an I/O
            // resumption outranks other work is an application question -- a server usually wants
            // it (finishing started work bounds latency and frees buffers and sockets), a game
            // usually does not (the frame deadline is the hard one; a read can wait 16ms). And if
            // everything is hiPri then nothing is.
            Task* batchHi[kBatch]; std::size_t nHi = 0;
            Task* batchLo[kBatch]; std::size_t nLo = 0;
#if defined(JLIBSCHED_IO_LOCK_STATS)
            IoResult* outHi[kBatch]; IoResult* outLo[kBatch];
#endif

            std::size_t steer = 0;   // rotates completions across the hot set; see Flush
            const auto Flush = [&](Task** hi, std::size_t& nh, Task** lo, std::size_t& nl) {
                if (!TaskScheduler::IsInitialized()) { nh = 0; nl = 0; return; }
#if defined(JLIBSCHED_IO_LOCK_STATS)
                // Stamp BEFORE the push: the push can let the coroutine frame -- and the IoResult
                // living in it -- die immediately.
                const std::int64_t now = MonotonicNs();
                for (std::size_t i = 0; i < nh; ++i) if (outHi[i]) outHi[i]->flushedAtNs = now;
                for (std::size_t i = 0; i < nl; ++i) if (outLo[i]) outLo[i]->flushedAtNs = now;
#endif
                // STEER COMPLETIONS AT THE HOT WORKERS, when there are any.
                //
                // Without this, K-hot measurably does nothing: PushBatch spreads round-robin, so
                // with K=2 of 29 workers only ~7% of completions land on a worker that is awake,
                // and the other 93% pay the full wake anyway. Measured 8-24 -- p50 stayed flat at
                // 9-11us for K = 0, 1, 2 and 8 alike, against 3us for NoSleep-all. Hot workers that
                // nothing is aimed at just burn cores.
                //
                // cpuaffinity is 1-BASED and selects the worker directly, so 1..K is exactly the
                // hot set. Rotated so a burst spreads across them instead of queueing behind one.
                // Zero when K is 0, which is the ordinary spread-across-the-pool behaviour.
                // SPLIT the batch across the hot set, do not hand it all to one of them.
                //
                // PushBatch treats an explicit affinity as "this worker, do not spread", so aiming a
                // whole flush at one hot worker leaves the other K-1 spinning with nothing to do:
                // all the contention of K hot cores and the throughput of one. That is what an
                // earlier measurement mistook for "spreading a burst is worse than pinning it" --
                // nothing was being spread.
                //
                // The starting worker still rotates per flush, so a stream of SINGLE completions
                // spreads across the hot set instead of always hammering worker 1.
                //
                // AND SKIP THE ONES THAT ARE ALREADY BURIED, when TaskScheduler::GetSteerSkip() is on.
                //
                // The rotation above is blind: it hands worker w a slice whether or not w is 200us
                // into a handler. Hot->hot stealing exists to repair exactly that, but repair is the
                // expensive path -- a probe, a contended CAS against the owner, a lost cache line --
                // and the producer is standing right there holding the task with the answer already
                // published in stealHintLane. Not aiming at a buried worker is free; taking the task
                // back off it is not.
                //
                // ONE load of the mask per flush, not one query per slice.
                //
                // MEASURED 8-25 AND IT SHIPS OFF, because the expected direction was wrong. Against
                // nothing this is worth 21-29% at p90/p99 on a skewed lane -- the producer really
                // was aiming at buried workers. But hot->hot STEALING beats it on the same rows
                // (44% at K=4), and with stealing on this adds 4-6%, inside the control's own noise.
                //
                // The reason is that the two cover different windows and stealing has the bigger
                // one: the producer can only act on the batch in its hand, while the tail is made by
                // tasks ALREADY sitting in the buried worker's deque, placed before it went dark.
                // Skipping stops the queue growing; only a thief can empty it.
                //
                // See bench/io_dispatch_latency.cpp's steering block for the table and the control.
                //
                // If EVERY hot worker is advertising, there is nothing to prefer and this falls back
                // to the plain rotation rather than spreading across the pool. Spreading would mean
                // waking a cold worker -- ~90us under the default Sleep policy -- to beat a hot one
                // that may be about to finish. Ordinary workers already drain a backlogged lane
                // opportunistically under laneHintMode 4, without anyone paying a wake for it, and
                // that is the right place for that decision.
                const std::size_t hotN = TaskScheduler::GetHotWorkers();
                auto pushSteered = [&](Task** arr, std::size_t n, bool hiPri) {
                    if (!n) return;
                    auto& s = TaskScheduler::Instance();
                    if (hotN == 0) { s.PushBatch(arr, n, 0, 64, hiPri); return; }

                    // Candidate hot workers, by queue index. Bounded by the hint's own width: past
                    // worker 64 no bit exists, so those are simply always candidates.
                    constexpr std::size_t kMaxSteer = 64;
                    std::uint8_t cand[kMaxSteer];
                    std::size_t  nc = 0;
                    const std::size_t scanN = (hotN < kMaxSteer) ? hotN : kMaxSteer;

                    if (TaskScheduler::GetSteerSkip()) {
                        const unsigned long long busy = TaskScheduler::LaneBacklogMask();
                        for (std::size_t i = 0; i < scanN; ++i)
                            if (!(busy & (1ull << i))) cand[nc++] = std::uint8_t(i);
                    }
                    if (nc == 0)                                  // skip disabled, or all of them buried
                        for (std::size_t i = 0; i < scanN; ++i) cand[nc++] = std::uint8_t(i);

                    // Slice across the workers actually taking work, not across the whole hot set --
                    // otherwise skipping one would leave its slice unsent.
                    const std::size_t per = (n + nc - 1) / nc;   // ceil, so the last slice is the short one
                    std::size_t off = 0, w = steer++;
                    while (off < n) {
                        const std::size_t len = (per < n - off) ? per : (n - off);
                        s.PushBatch(arr + off, len, std::uint8_t(1 + cand[w % nc]), 64, hiPri);
                        off += len;
                        ++w;
                    }
                };
                pushSteered(hi, nh, true);  nh = 0;
                pushSteered(lo, nl, false); nl = 0;
            };

            for (;;) {
                DWORD bytes = 0;
                ULONG_PTR key = 0;
                LPOVERLAPPED ov = nullptr;

                // Block only when nothing is already collected. Once the batch has something in it,
                // poll with a zero timeout: anything else ready RIGHT NOW joins this batch, and the
                // moment the port is empty the batch goes out. No timer, no delay -- coalescing that
                // waits would trade tail latency for tail latency.
                const BOOL ok = (nHi == 0 && nLo == 0)
                    ? GetQueuedCompletionStatus(port, &bytes, &key, &ov, INFINITE)
                    : GetQueuedCompletionStatus(port, &bytes, &key, &ov, 0);

#if defined(JLIBSCHED_IO_LOCK_STATS)
                // WHICH CORE THE COMPLETION THREAD IS ACTUALLY ON, sampled per wake rather than once
                // at thread start. Startup placement is not the answer to the question: the Ideal
                // processor is a HINT, and Windows migrates a thread that parks and wakes as often
                // as this one does. A single start-of-thread reading would report a core this
                // thread may not have touched since.
                //
                // Diagnostic only, and behind the stats define for the usual reason -- this is on
                // the completion path, and the thing being measured is microsecond-scale.
                if (const DWORD cpu = ::GetCurrentProcessorNumber(); cpu < 64)
                    detail::g_complCore[cpu].fetch_add(1, std::memory_order_relaxed);
#endif

                if ((nHi || nLo) && ov == nullptr && !ok) {
                    Flush(batchHi, nHi, batchLo, nLo);
                    continue;
                }

                if (ov == nullptr) {
                    // Not a completion: a poll that found nothing, the port died, or Stop() posted
                    // the wake-up below.
                    //
                    // THE TIMEOUT CASE HAS TO BE SEPARATED FROM THE DEAD-PORT CASE, and it did not
                    // used to matter: with an INFINITE wait this call never times out, so `!ok` with
                    // no OVERLAPPED could only mean the port was gone, and returning was right. The
                    // moment anything polls with a zero timeout, that same condition means "nothing
                    // ready right now" -- and returning KILLS THE COMPLETION THREAD on the first
                    // empty poll, after which no I/O ever completes again. Silently: the reactor is
                    // simply gone and every operation hangs forever.
                    if (!ok) {
                        if (::GetLastError() == WAIT_TIMEOUT) continue;
                        return;
                    }
                    if (stopping.load(std::memory_order_acquire) && total.load(std::memory_order_acquire) == 0) {
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
                    std::lock_guard<IoMutex> lk(ShardFor(r).m);
                    Unlink(r);                     // BEFORE the push; see the note on Link
                    if (r->out) *r->out = res;     // still safe: the owner is still suspended
                    resume = r->resume;
                    lastOne = false;
                }

                // THE HOOK RUNS HERE: after the lock is dropped, before the resume is pushed.
                // After the lock, because it submits the next queued transfer and Submit takes the
                // same mutex. Before the push, because the push can let this request.s frame die.
#if defined(JLIBSCHED_IO_LOCK_STATS)
                // Stamped AFTER the result is stored and BEFORE the push, so the measured interval
                // is exactly "reactor finished" to "coroutine running".
                if (r->out) r->out->completedAtNs = MonotonicNs();
#endif

                if (r->onComplete) r->onComplete(r);

                // OUTSIDE THE LOCK, and COLLECTED rather than pushed. The frame `r` lives in dies the
                // moment its task runs, so nothing below may touch `r` -- which is true whether the
                // push happens now or at the flush, because collecting only copies the Task*.
                if (resume) {
#if defined(JLIBSCHED_IO_LOCK_STATS)
                    if (resume->hiPri) outHi[nHi] = r->out; else outLo[nLo] = r->out;
#endif
                    if (resume->hiPri) batchHi[nHi++] = resume;
                    else               batchLo[nLo++] = resume;
                    if (nHi == kBatch || nLo == kBatch) Flush(batchHi, nHi, batchLo, nLo);
                }

                if (lastOne || ((nHi || nLo) && stopping.load(std::memory_order_acquire)))
                    Flush(batchHi, nHi, batchLo, nLo);
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
                if (stopping.load(std::memory_order_acquire)) {
                    if (out) *out = IoResult{ IoStatus::Failed, 0, ERROR_SHUTDOWN_IN_PROGRESS };
                    return true;
                }
                EnsureThreads();
                std::lock_guard<IoMutex> lk(ShardFor(req).m);
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
                    std::lock_guard<IoMutex> lk(ShardFor(req).m);
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
            if (impl->stopping.load(std::memory_order_acquire)) return false;
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

    bool IoReactor::SubmitDisconnect(IoSocket s, bool reuse, IoRequest* req, IoResult* out,
                                     Task* resume, CancelToken token) {
        if (!g_disconnectEx && !ResolveExtensions()) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSANOTINITIALISED };
            return true;
        }
        if (!g_disconnectEx) {
            // Provider has no DisconnectEx. Reported rather than silently degraded to closesocket:
            // a caller that thinks it reused a socket and did not would then use a closed one.
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEOPNOTSUPP };
            return true;
        }

        const SOCKET sock = static_cast<SOCKET>(s);
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, out, resume, token,
                            [&](OVERLAPPED* ov) {
            return g_disconnectEx(sock, ov, reuse ? TF_REUSE_SOCKET : 0, 0) != FALSE;
        });
    }

    bool IoReactor::SubmitPrepared(IoRequest* req) {
        const SOCKET sock = reinterpret_cast<SOCKET>(req->handle);

        // The cancellation pre-check inside Submit still applies and is wanted: a transfer that
        // waited its turn in a chain may have had its scope cancelled while queued, and submitting
        // it then would hand the kernel a buffer nobody wants.
        const CancelToken tok(req->token);

        if (req->kind == IoRequest::Kind::Send) {
            return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, req->out, req->resume, tok,
                                [&](OVERLAPPED* ov) {
                return ::WSASend(sock, Bufs(req), req->bufCount, nullptr,
                                 static_cast<DWORD>(req->flags), ov, nullptr) == 0;
            });
        }
        return impl->Submit(reinterpret_cast<HANDLE>(sock), 0, req, req->out, req->resume, tok,
                            [&](OVERLAPPED* ov) {
            return ::WSARecv(sock, Bufs(req), req->bufCount, nullptr,
                             reinterpret_cast<LPDWORD>(&req->flags), ov, nullptr) == 0;
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
        // EVERY SHARD, one at a time. Cancel-by-scope is the rare path -- it pays for the sharding
        // that makes submit and complete cheap, which is the right way round.
        std::size_t asked = 0;
        for (std::size_t i = 0; i < Impl::kShards; ++i) {
            std::lock_guard<IoMutex> lk(impl->shards[i].m);
            for (IoRequest* r = impl->shards[i].head; r; r = r->next) {
                if (token.Valid() && !CancelToken(r->token).IsWithin(token)) continue;
                ::CancelIoEx(r->handle, Ov(r));
                ++asked;
            }
        }
        return asked;
    }

    std::size_t IoReactor::CompletionCoreHistogram(unsigned* counts, std::size_t max) noexcept {
        if (!counts || max == 0) return 0;
        const std::size_t n = max < 64 ? max : 64;
#if defined(JLIBSCHED_IO_LOCK_STATS)
        for (std::size_t i = 0; i < n; ++i)
            counts[i] = detail::g_complCore[i].load(std::memory_order_relaxed);
#else
        for (std::size_t i = 0; i < n; ++i) counts[i] = 0;
#endif
        return n;
    }

    std::size_t IoReactor::InFlight() const noexcept {
        return impl->total.load(std::memory_order_acquire);
    }

    // The counterpart to Stop, so a pool can be Init-ed again after Join. Clearing the stop latch is
    // the whole job: the completion PORT outlives Stop -- it is closed only by the destructor -- so
    // every handle registered against it is still associated, and the drain threads come back
    // through the existing lazy EnsureThreads path on the next Register or Submit.
    //
    // Idempotent, and a no-op on a reactor that was never stopped.
    void IoReactor::Start() noexcept {
        std::lock_guard<std::mutex> lk(impl->life);
        impl->stopping.store(false, std::memory_order_release);
    }

    void IoReactor::Stop() noexcept {
        bool needJoin = false;
        {
            std::lock_guard<std::mutex> lk(impl->life);
            if (impl->stopping.load(std::memory_order_acquire)) return;
            impl->stopping.store(true, std::memory_order_release);
            needJoin = impl->running;
        }

        // EVERY IN-FLIGHT OPERATION IS CANCELLED AND DRAINED BEFORE THE THREAD GOES. Stopping while
        // the kernel still holds a buffer whose owning frame is about to unwind is the corruption
        // this file exists to prevent -- and the owner cannot resume until its completion arrives,
        // so the only way out is to make those completions happen.
        RequestCancel(CancelToken{});

        if (needJoin) {
            std::vector<std::thread> ts;
            { std::lock_guard<std::mutex> lk(impl->life); ts.swap(impl->workers); }
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
        if (!FillBufs(req, bufs, count)) {
            if (out) *out = IoResult{ IoStatus::Failed, 0, WSAEMSGSIZE };
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
        req->out       = out;
        req->resume    = resume;
        req->token     = token.Raw();
        req->handle    = reinterpret_cast<void*>(static_cast<SOCKET>(sock_));
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
            SOCKET         sock = INVALID_SOCKET;
            Impl*          owner = nullptr;
            unsigned       index = 0;
        };

        SOCKET listener = INVALID_SOCKET;
        int    family = AF_INET, type = SOCK_STREAM, proto = IPPROTO_TCP;

        std::vector<Slot>   slots;
        mutable std::mutex  m;
        std::vector<SOCKET> ready;          // accepted, nobody waiting yet
        IoAcceptWaiter*     waitHead = nullptr;   // parked, no connection yet
        IoAcceptWaiter*     waitTail = nullptr;
        std::size_t         outstanding = 0;
        bool                stopping = false;

        CancelScope         scope;          // cancels every accept this acceptor posted, and only those

        // Creates the socket AcceptEx will fill. Matching the listener's family/protocol matters:
        // a mismatch fails inside AcceptEx with an error that does not say which argument was wrong.
        SOCKET MakeSocket() const {
            return ::WSASocketW(family, type, proto, nullptr, 0, WSA_FLAG_OVERLAPPED);
        }

        // Caller must NOT hold `m`: this pushes a task and submits to the kernel.
        void Post(unsigned i) {
            Slot& s = slots[i];
            s.sock = MakeSocket();
            if (s.sock == INVALID_SOCKET) return;

            if (!IoReactor::Instance().RegisterSocket(static_cast<IoSocket>(s.sock))) {
                ::closesocket(s.sock);
                s.sock = INVALID_SOCKET;
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
                ::closesocket(s.sock);
                s.sock = INVALID_SOCKET;
            }
        }

        static void AcceptCompleted(IoRequest* r) {
            Slot* s = reinterpret_cast<Slot*>(r->hookCtx);
            if (s && s->owner) s->owner->OnComplete(s->index);
        }

        void OnComplete(unsigned i) {
            Slot& s = slots[i];
            const SOCKET accepted = s.sock;     // the connection this slot just produced
            s.sock = INVALID_SOCKET;

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
            if (!keep && accepted != INVALID_SOCKET) ::closesocket(accepted);

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
        impl->listener = static_cast<SOCKET>(listener);

        // Ask the LISTENER what it is rather than making the caller repeat it -- one fewer thing to
        // get subtly wrong, and the failure mode for getting it wrong is opaque.
        WSAPROTOCOL_INFOW info{};
        int len = sizeof info;
        if (::getsockopt(impl->listener, SOL_SOCKET, SO_PROTOCOL_INFOW,
                         reinterpret_cast<char*>(&info), &len) == 0) {
            impl->family = info.iAddressFamily;
            impl->type   = info.iSocketType;
            impl->proto  = info.iProtocol;
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
        std::vector<SOCKET> leftovers;
        { std::lock_guard<std::mutex> lk(impl->m); leftovers.swap(impl->ready); }
        for (SOCKET s : leftovers) ::closesocket(s);
    }

    IoSocket IoAcceptor::TryTake() noexcept {
        if (!impl) return 0;
        std::lock_guard<std::mutex> lk(impl->m);
        if (impl->ready.empty()) return 0;
        const SOCKET s = impl->ready.back();
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

} // namespace JLib
