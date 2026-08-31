// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Timer.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include "../include/platform.h"

#include <chrono>
#include <cstdio>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

namespace JLib {

    int64_t MonotonicNs() noexcept {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    }

    void EjectEvent(void* ctx, CancelToken token) {
        if (ctx) static_cast<Event*>(ctx)->CancelWaiters();
        (void)token;   // Event claims its waiters directly; it has no scope filter.
    }

    void EjectSemaphore(void* ctx, CancelToken token) {
        if (ctx) static_cast<SchedulerSemaphore*>(ctx)->CancelWaiters(token);
    }

    void EjectConditionVariable(void* ctx, CancelToken token) {
        if (ctx) static_cast<SchedulerConditionVariable*>(ctx)->CancelWaiters(token);
    }

    // =============================================================================================
    // HIERARCHICAL TIMER WHEEL, four levels of 256 slots at a 1ms tick.
    //
    // WHY A WHEEL AND NOT A HEAP. A heap is O(log n) to insert and, worse, O(n) to remove an
    // arbitrary entry unless you carry a position index. The workload here is the opposite of what a
    // heap is good at: an I/O server arms a timeout per request and CANCELS ALMOST ALL OF THEM,
    // because the request usually completes long before its deadline. So removal is the hot
    // operation, not expiry, and a wheel makes it O(1) -- unlink an intrusive node, touch nothing
    // else. Insert is O(1) too: the deadline's own bits pick the slot.
    //
    // LEVELS. Level 0 covers 256 ticks at 1 tick each, level 1 covers 256*256 at 256 ticks each, and
    // so on: 256^4 = 2^32 ticks, about 49.7 days. Entries start in the coarsest level that can hold
    // them and CASCADE down as time approaches, so a far deadline is touched a handful of times
    // total rather than sitting in a sorted structure being compared against.
    //
    // NO HEAP FALLBACK, and that is a deliberate omission rather than a missing piece. The sketch
    // this was built from called for one for deadlines past the wheel's range -- but the range is
    // 49.7 days. Nothing in an I/O reactor or a game waits that long, and a second data structure
    // maintained for a case that never occurs is a second data structure to get wrong. Beyond-range
    // deadlines are CLAMPED to the far edge of the wheel and the caller can see it happened.
    //
    // IT DOES NOT TICK WHEN IT IS IDLE. A thread waking 1,000 times a second to look at empty slots
    // costs real power and, on this scheduler, measurably taxes the main thread -- the same reason
    // the pool defaults to Sleep rather than NoSleep. So each level carries a 256-bit OCCUPANCY
    // BITMAP and the thread sleeps until the soonest occupied slot, not until the next tick. With
    // nothing armed it does an untimed wait and costs nothing at all. The bitmap scan is the same
    // CountTrailingZeros64 trick the Event waiter table uses.
    // =============================================================================================

    namespace {

        constexpr int      kLevels    = 4;
        constexpr int      kSlotBits  = 8;
        constexpr uint32_t kSlots     = 1u << kSlotBits;      // 256
        constexpr uint32_t kSlotMask  = kSlots - 1;
        constexpr int      kOccWords  = int(kSlots / 64);     // 4
        constexpr int64_t  kMaxTicks  = int64_t(1) << (kSlotBits * kLevels);   // 2^32

        // Ticks that level L spans per slot: 1, 256, 65536, 16777216.
        constexpr int64_t LevelGranularity(int level) {
            return int64_t(1) << (kSlotBits * level);
        }

    } // namespace

    struct TimerQueue::Impl {
        // An armed timer. Intrusive doubly-linked BY INDEX rather than by pointer, so the backing
        // vector can grow without invalidating anything -- a pointer-linked list would dangle the
        // moment a reallocation moved it.
        struct Entry {
            int64_t    deadlineTick = 0;
            uint32_t   token        = CancelToken::kNone;
            TimerEject eject        = nullptr;
            void*      ctx          = nullptr;

            uint32_t   prev = kNil;
            uint32_t   next = kNil;

            // Odd = armed, even = free. Parity IS the armed flag, so there is no separate bool that
            // could disagree with it, and every bump invalidates every outstanding handle.
            uint32_t   generation = 0;
            uint32_t   nextFree   = 0;   // 1-based; 0 ends the free list

            int8_t     level = -1;       // where it currently lives, for O(1) unlink
            uint32_t   slot  = 0;
        };

        static constexpr uint32_t kNil = 0xFFFFFFFFu;

        mutable std::mutex      m;
        std::condition_variable cv;

        std::vector<Entry> entries;
        uint32_t           freeHead = 0;         // 1-based

        uint32_t heads[kLevels][kSlots];         // kNil-terminated intrusive lists
        uint64_t occ[kLevels][kOccWords];        // which slots are non-empty

        int64_t  tickNs     = 1'000'000;         // 1ms
        int64_t  epochNs    = 0;                 // MonotonicNs() at construction
        int64_t  currentTick = 0;

        size_t   armedCount = 0;
        bool     running    = false;
        bool     stopping   = false;
        std::thread worker;

        Impl() {
            for (int l = 0; l < kLevels; ++l) {
                for (uint32_t s = 0; s < kSlots; ++s) heads[l][s] = kNil;
                for (int w = 0; w < kOccWords; ++w) occ[l][w] = 0;
            }
            epochNs = MonotonicNs();
        }

        int64_t NowTick() const { return (MonotonicNs() - epochNs) / tickNs; }

        // ---- occupancy -------------------------------------------------------------------------

        void MarkOccupied(int l, uint32_t s)  { occ[l][s >> 6] |= (uint64_t(1) << (s & 63)); }
        void MarkEmpty(int l, uint32_t s)     { occ[l][s >> 6] &= ~(uint64_t(1) << (s & 63)); }
        bool LevelEmpty(int l) const {
            for (int w = 0; w < kOccWords; ++w) if (occ[l][w]) return false;
            return true;
        }

        // Next occupied slot at `l` at or after `from`, wrapping once. kSlots if the level is empty.
        //
        // A WORD AT A TIME, not a bit at a time. This runs on every wake of the timer thread, and a
        // per-slot loop is up to 256 iterations per level -- 1,024 to answer "is there anything to
        // do", which is the question asked most often and usually answered "no". Four 64-bit words
        // cover a level, so the whole scan is at most five loads and a CountTrailingZeros64. Same
        // trick, and for the same reason, as the Event waiter table's occupancy bitmap.
        uint32_t NextOccupied(int l, uint32_t from) const {
            const uint32_t startWord = from >> 6;
            const unsigned startBit  = from & 63;

            // The starting word, masked to bits at or after `from`. Shifting by 0 leaves it whole,
            // which is what a scan starting on a word boundary should see.
            uint64_t w = occ[l][startWord] & (~uint64_t(0) << startBit);
            if (w) return (startWord << 6) + platform::CountTrailingZeros64(w);

            for (int i = 1; i < kOccWords; ++i) {
                const uint32_t wi = (startWord + uint32_t(i)) % uint32_t(kOccWords);
                w = occ[l][wi];
                if (w) return (wi << 6) + platform::CountTrailingZeros64(w);
            }

            // Wrapped all the way round: the bits of the starting word that come BEFORE `from`.
            w = occ[l][startWord] & ~(~uint64_t(0) << startBit);
            if (w) return (startWord << 6) + platform::CountTrailingZeros64(w);
            return kSlots;
        }

        // ---- list ------------------------------------------------------------------------------

        void Link(uint32_t idx, int l, uint32_t s) {
            Entry& e = entries[idx];
            e.level = int8_t(l);
            e.slot  = s;
            e.prev  = kNil;
            e.next  = heads[l][s];
            if (e.next != kNil) entries[e.next].prev = idx;
            heads[l][s] = idx;
            MarkOccupied(l, s);
        }

        void Unlink(uint32_t idx) {
            Entry& e = entries[idx];
            if (e.level < 0) return;
            const int l = e.level;
            const uint32_t s = e.slot;

            if (e.prev != kNil) entries[e.prev].next = e.next;
            else                heads[l][s] = e.next;
            if (e.next != kNil) entries[e.next].prev = e.prev;

            e.prev = e.next = kNil;
            e.level = -1;
            if (heads[l][s] == kNil) MarkEmpty(l, s);
        }

        // ---- placement -------------------------------------------------------------------------

        // The deadline's own bits choose the slot -- that is the whole trick, and why insert is O(1)
        // with no comparisons. An entry goes in the COARSEST level whose window still contains it,
        // and cascades down later as `currentTick` catches up.
        void Place(uint32_t idx, int64_t deadlineTick) {
            const int64_t delta = deadlineTick - currentTick;

            if (delta <= 0) { Link(idx, 0, uint32_t(currentTick & kSlotMask)); return; }

            for (int l = 0; l < kLevels; ++l) {
                if (delta < LevelGranularity(l + 1)) {
                    Link(idx, l, uint32_t((deadlineTick >> (kSlotBits * l)) & kSlotMask));
                    return;
                }
            }
            // Past the wheel's 49.7 days. Clamped rather than kept in a second structure -- see the
            // header. It still fires, just at the far edge.
            Link(idx, kLevels - 1, uint32_t(((currentTick + kMaxTicks - 1) >> (kSlotBits * (kLevels - 1))) & kSlotMask));
        }

        // Empty one higher-level bucket back down. Every entry in it is re-Placed against the new
        // currentTick, which lands it in a finer level -- this is the only work a far-future timer
        // costs between arming and firing, and it happens at most kLevels-1 times in its life.
        void Cascade(int level, uint32_t slot) {
            uint32_t idx = heads[level][slot];
            heads[level][slot] = kNil;
            MarkEmpty(level, slot);

            while (idx != kNil) {
                const uint32_t nextIdx = entries[idx].next;
                entries[idx].prev = entries[idx].next = kNil;
                entries[idx].level = -1;
                Place(idx, entries[idx].deadlineTick);
                idx = nextIdx;
            }
        }

        // ---- slots -----------------------------------------------------------------------------

        uint32_t AcquireEntry() {
            if (freeHead != 0) {
                const uint32_t i = freeHead - 1;
                freeHead = entries[i].nextFree;
                return i;
            }
            entries.push_back(Entry{});
            return uint32_t(entries.size() - 1);
        }

        void ReleaseEntry(uint32_t i) {
            Entry& e = entries[i];
            e.eject = nullptr;
            e.ctx = nullptr;
            e.token = CancelToken::kNone;
            e.level = -1;
            e.nextFree = freeHead;
            freeHead = i + 1;
        }

        // ---- the clock -------------------------------------------------------------------------

        // Soonest tick at which anything could possibly need doing, or -1 if the wheel is empty.
        //
        // A LOWER BOUND IS ENOUGH, and asking for more would cost more than it saves. A level-2 slot
        // says "something falls inside this 65,536-tick window" and not where; waking at the start
        // of the window, cascading, and looking again is correct and cheap. Waking EARLY is free --
        // the next pass just sleeps again. Waking LATE is the only error, and this cannot.
        int64_t NextEventTick() const {
            int64_t best = -1;

            for (int l = 0; l < kLevels; ++l) {
                if (LevelEmpty(l)) continue;

                const int64_t gran = LevelGranularity(l);
                const uint32_t cur = uint32_t((currentTick >> (kSlotBits * l)) & kSlotMask);

                // Level 0's current slot may still hold entries due right now; higher levels are
                // examined from the NEXT slot, because the current one is the window we are inside.
                const uint32_t from = (l == 0) ? cur : ((cur + 1) & kSlotMask);
                const uint32_t s = NextOccupied(l, from);
                if (s == kSlots) continue;

                const uint32_t ahead = (s - from) & kSlotMask;
                const int64_t base = (l == 0)
                    ? currentTick
                    : ((currentTick >> (kSlotBits * l)) + 1) << (kSlotBits * l);
                const int64_t when = base + int64_t(ahead) * gran;

                if (best < 0 || when < best) best = when;
            }
            return best;
        }

        // Move the wheel to `target`, cascading every higher-level bucket the jump passes through.
        //
        // JUMPS, rather than stepping one tick at a time. Stepping is what a wheel normally does
        // because it normally ticks; this one sleeps to the next event instead, so a jump can be
        // millions of ticks and a per-tick loop would be the whole cost of the design. Cascading is
        // driven by which SLOT INDICES changed, computed arithmetically -- and a jump longer than a
        // level's full span means that level rotated at least once, so every one of its buckets is
        // due and the bitmap skips the empty ones.
        void AdvanceTo(int64_t target) {
            if (target <= currentTick) return;

            // currentTick MOVES FIRST, before anything cascades. Cascade re-Places each entry, and
            // Place buckets it by its distance from currentTick -- so cascading against the OLD time
            // would compute a distance a whole window too large and drop the entry straight back
            // into the level it just came out of. That is not a slow path, it is a loop.
            const int64_t from0 = currentTick;
            currentTick = target;

            for (int l = 1; l < kLevels; ++l) {
                const int shift = kSlotBits * l;
                const int64_t from = from0 >> shift;
                const int64_t to   = target >> shift;
                if (from == to) break;              // this level did not move; nor did coarser ones

                const int64_t steps = to - from;
                if (steps >= int64_t(kSlots)) {
                    for (uint32_t s = 0; s < kSlots; ++s)
                        if (heads[l][s] != kNil) Cascade(l, s);
                } else {
                    for (int64_t k = 1; k <= steps; ++k) {
                        const uint32_t s = uint32_t((from + k) & kSlotMask);
                        if (heads[l][s] != kNil) Cascade(l, s);
                    }
                }
            }
        }

        void Run() {
            std::unique_lock<std::mutex> lk(m);
            for (;;) {
                if (stopping) return;

                const int64_t next = NextEventTick();
                if (next < 0) {
                    // Nothing armed. Catch the wheel up to real time while it is provably empty --
                    // free here, and it keeps the next Arm from bucketing against a stale clock.
                    currentTick = NowTick();
                    cv.wait(lk);                                // untimed, which is the safe kind
                    continue;
                }

                const int64_t now = NowTick();
                if (next > now) {
                    // THE ONLY TIMED WAIT IN THE LIBRARY, and it is on a thread with no work to
                    // lose. Waking early re-checks; waking late makes a deadline late. A WORKER
                    // doing this could absorb a lost wakeup and look merely slow, which is why the
                    // workers never do -- see the note in TaskScheduler on worker sleep.
                    cv.wait_for(lk, std::chrono::nanoseconds((next - now) * tickNs));
                    continue;
                }

                // ADVANCE TO `next`, NOT TO `now`. A wheel fires the slot it is standing on, so
                // jumping straight to the current time steps over every slot in between and their
                // entries are simply never found -- silently, because nothing is left behind to
                // notice. Landing on exactly the next event tick means no occupied slot is ever
                // skipped, while the gaps between events are still crossed in one jump rather than
                // one tick at a time.
                AdvanceTo(next);

                // Everything due in the current level-0 slot. Collected under the lock, fired
                // outside it.
                constexpr size_t kBatch = 64;
                CancelToken tokens[kBatch];
                TimerEject  ejects[kBatch];
                void*       ctxs[kBatch];
                size_t      n = 0;

                const uint32_t slot = uint32_t(currentTick & kSlotMask);
                uint32_t idx = heads[0][slot];
                while (idx != kNil && n < kBatch) {
                    const uint32_t nextIdx = entries[idx].next;
                    Entry& e = entries[idx];

                    if (e.deadlineTick <= currentTick) {
                        tokens[n] = CancelToken(e.token);
                        ejects[n] = e.eject;
                        ctxs[n]   = e.ctx;
                        ++n;

                        // Retired BEFORE the lock is dropped, so a Disarm arriving while the
                        // callbacks run is told the truth -- that it removed nothing -- instead of
                        // being handed a slot already committed to firing.
                        Unlink(idx);
                        ++e.generation;              // odd -> even: free
                        ReleaseEntry(idx);
                        --armedCount;
                    }
                    idx = nextIdx;
                }

                if (n == 0) {
                    // Nothing was due here after all -- the usual reason is that `next` came from a
                    // coarser level and the cascade above dropped its entries into a LATER level-0
                    // slot. Step one tick so the scan cannot return this same slot forever; a
                    // recomputed NextEventTick then jumps to wherever the work actually landed.
                    ++currentTick;
                    continue;
                }

                // NOTHING BELOW TOUCHES WHEEL STATE. CancelVia and an eject both reach into the
                // scheduler and can resume tasks, and a resumed task may arm a timer on another
                // worker -- under this lock that is a deadlock, not a wait. Same rule the primitives
                // follow when they resume outside their spinlock.
                lk.unlock();
                for (size_t i = 0; i < n; ++i) {
                    // ORDER MATTERS. The flag first, so a waiter woken by the eject -- and every
                    // check it makes afterwards -- agrees it was cancelled. Ejecting first leaves a
                    // window where a resumed task looks up its token and finds it live.
                    //
                    // A false return means the scope is already gone: the operation beat its
                    // deadline. Skip the eject as well -- there is nobody to wake, and the ctx it
                    // would touch may belong to a frame that has already returned.
                    if (CancelVia(tokens[i]) && ejects[i]) ejects[i](ctxs[i], tokens[i]);
                }
                lk.lock();
            }
        }
    };

    constexpr uint32_t TimerQueue::Impl::kNil;

    // ---------------------------------------------------------------------------------------------

    TimerQueue::TimerQueue() : impl(new Impl()) {}

    TimerQueue::~TimerQueue() {
        Stop();
        delete impl;
    }

    TimerQueue& TimerQueue::Instance() {
        // Function-local static, like the cancel table: no static-init-order dependency on the
        // scheduler, and no thread exists until something actually arms a timer.
        static TimerQueue q;
        return q;
    }

    TimerHandle TimerQueue::Arm(int64_t delayNs, CancelToken token, TimerEject eject, void* ctx) {
        // Nothing to cancel. Refusing here rather than queueing keeps a dead entry out of the wheel
        // and stops the thread waking for a token that can never resolve.
        if (!token.Valid()) return TimerHandle{};

        std::lock_guard<std::mutex> lk(impl->m);
        if (impl->stopping) return TimerHandle{};

        // OPT-IN, ENFORCED. A pool exists and nobody asked for deadlines, so the pool was sized
        // without a core for this thread. Refusing here is deterministic and obvious at the first
        // Arm; starting anyway would run the machine one thread over for the process's lifetime,
        // which is a few percent nobody attributes correctly. Without a pool there is nothing to
        // oversubscribe and the timer is free to run.
        if (TaskScheduler::IsInitialized() && !TaskScheduler::TimersEnabled()) {
            std::fprintf(stderr,
                "[JLib::Scheduler] TimerQueue::Arm called but the timer layer is not enabled -- the "
                "pool was sized without a core for it. Call TaskScheduler::EnableTimers(true) "
                "before Init.\n");
            return TimerHandle{};
        }

        if (!impl->running) {
            impl->running = true;
            impl->worker = std::thread([this] { impl->Run(); });

        }

        // Rounded UP: a 1ms deadline on a 1ms tick must not fire at 0. A timer is allowed to be
        // late and never early -- firing early would cancel work that still had time left.
        const int64_t nowTick = impl->NowTick();
        const int64_t delayTicks = (delayNs > 0) ? ((delayNs + impl->tickNs - 1) / impl->tickNs) : 0;
        const int64_t deadlineTick = nowTick + delayTicks;

        // The wheel advances lazily, so currentTick can lag well behind real time when the queue has
        // been idle. Catch it up before placing, or the delta this is bucketed against is wrong.
        impl->AdvanceTo(nowTick);

        const uint32_t i = impl->AcquireEntry();
        Impl::Entry& e = impl->entries[i];
        ++e.generation;                       // even -> odd: armed
        e.deadlineTick = deadlineTick;
        e.token = token.Raw();
        e.eject = eject;
        e.ctx = ctx;
        impl->Place(i, deadlineTick);
        ++impl->armedCount;

        // Wake the thread only if this is now the soonest thing in the wheel. Arming behind an
        // earlier deadline changes nothing it needs to know yet.
        const int64_t next = impl->NextEventTick();
        if (next < 0 || next >= deadlineTick) impl->cv.notify_one();

        return TimerHandle{ (uint64_t(e.generation) << 32) | uint64_t(i) };
    }

    bool TimerQueue::Disarm(TimerHandle h) noexcept {
        if (!h.Valid()) return false;
        const uint32_t i = uint32_t(h.raw & 0xFFFFFFFFu);
        const uint32_t g = uint32_t(h.raw >> 32);

        std::lock_guard<std::mutex> lk(impl->m);
        if (i >= impl->entries.size()) return false;

        Impl::Entry& e = impl->entries[i];
        // Generation mismatch means it already fired or was already disarmed; even parity means the
        // entry is free. Either way this call removed nothing, and says so.
        if (e.generation != g || (g & 1u) == 0) return false;

        // O(1) -- the reason this is a wheel. Unlink from wherever it sits and stop; no rebuild, no
        // search, no hole left behind for a firing pass to trip over.
        impl->Unlink(i);
        ++e.generation;                       // odd -> even: free
        impl->ReleaseEntry(i);
        --impl->armedCount;
        return true;
    }

    std::size_t TimerQueue::PendingCount() const noexcept {
        std::lock_guard<std::mutex> lk(impl->m);
        return impl->armedCount;
    }

    // Counterpart to Stop, for an Init-after-Join cycle. Only clears the latch: the wheel itself is
    // intact and the worker thread is spawned lazily on the next Arm, so nothing else needs undoing.
    void TimerQueue::Start() noexcept {
        std::lock_guard<std::mutex> lk(impl->m);
        impl->stopping = false;
    }

    void TimerQueue::Stop() noexcept {
        std::thread t;
        {
            std::lock_guard<std::mutex> lk(impl->m);
            if (impl->stopping) return;
            impl->stopping = true;
            impl->cv.notify_all();
            t.swap(impl->worker);
        }
        // Joined OUTSIDE the lock: the thread takes it on every pass, so holding it here would be a
        // deadlock rather than a wait.
        if (t.joinable()) t.join();
    }

} // namespace JLib
