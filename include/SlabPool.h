// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"   // platform::kCacheLine
#include <memory>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <cstdio>
#include <cstdlib>

// THE FREE-LIST CANARY SWITCH.
//
// These checks were `#ifdef _DEBUG`, which meant they were OFF in the Development configuration --
// the optimized-with-assertions build this project is actually run and debugged in (it uses /MD, so
// _DEBUG is not defined; it gets /DJLIB_DEVELOPMENT instead). A use-after-free detector that does
// not run in the build you debug in is not a detector.
//
// That gap cost real time: a slot written through after being freed corrupted the free list, and the
// first visible symptom was a garbage pointer several subsystems away -- once inside
// LockFreeList::add, once inside refill() -- with nothing pointing at the write that actually did
// it. The canary catches it at the next Alloc() of that exact slot.
#if defined(_DEBUG) || defined(JLIB_DEVELOPMENT)
    #define JLIBSCHED_ALLOC_CANARY 1
#endif

// PORTABLE, and it has to be: JLIB_DEVELOPMENT is set on Linux and macOS as well (see
// CMAKE_CXX_FLAGS_DEVELOPMENT), so gating this on Windows.h / OutputDebugStringA / __debugbreak
// would have broken every POSIX Development build the moment the canary stopped being _DEBUG-only.
#ifdef JLIBSCHED_ALLOC_CANARY
  #if JLIB_PLATFORM_WINDOWS
    #include <Windows.h>
    #define JLIBSCHED_CANARY_REPORT(msg) do { OutputDebugStringA(msg); std::fprintf(stderr, "%s", msg); __debugbreak(); } while (0)
  #else
    #include <csignal>
    #define JLIBSCHED_CANARY_REPORT(msg) do { std::fprintf(stderr, "%s", msg); std::fflush(stderr); std::raise(SIGTRAP); } while (0)
  #endif
#endif

namespace JLib {
    namespace detail {
        // One live-slot counter, padded to its own cache line. Lives OUT here rather than nested in
        // the pool because a default member initializer cannot be used by a static data member of
        // the same enclosing class: the initializer is not complete until the class is, which GCC
        // diagnoses and MSVC quietly accepts.
        struct alignas(platform::kCacheLine) LiveCounter { std::atomic<long long> v{ 0 }; };
    }

    // A fixed-slot-size slab with a per-thread free-list cache, a shared overflow pool, and lazy
    // bump-allocation of never-touched capacity.
    //
    // WHY THIS IS A TEMPLATE, which is the whole reason it exists as its own type. The allocator
    // used to be one hand-written class, and adding a second size class meant duplicating every
    // member by hand. That duplication is not merely ugly -- it is where the bugs come from, and
    // both of the ones it produced were measured, not theorised:
    //
    //   1. The duplicated live counter was written as ONE shared std::atomic instead of the sharded
    //      form. That cost 62% on the frame path (11.7 ns vs 7.2 ns per alloc+free) because an
    //      uncontended `lock xadd` is ~3.3 ns and there are two per allocation. The original class
    //      documented exactly this and sharded for exactly this reason; the copy did not inherit
    //      the reasoning, only the shape.
    //   2. The per-thread cache is a `static thread_local` in a static member function, so it is one
    //      cache PER TYPE. A hand-written second pool must remember to give it a differently-named
    //      function or the two pools share one cache and each hands out the other's slots -- which
    //      corrupts the heap immediately and diagnoses as nothing (0xC0000374, seen once already).
    //
    // As a template, BOTH of those become structural. `SlabPool<64>` and `SlabPool<256>` are
    // different types, so `local()` and `s_live[]` are automatically distinct per size class. There
    // is no "remember to..." step left, because there is no copy.
    template <std::size_t SLOTSZ>
    class SlabPool {
    public:
        static constexpr std::size_t SLOT  = SLOTSZ;
        static constexpr std::size_t BATCH = 32;    // refill/flush granularity

    private:
        struct alignas(16) Block { std::byte b[SLOTSZ]; };

        // a Free slot's first 8 bytes ARE the "next Free" link (the intrusive trick)
        static void*& next(void* slot) { return *reinterpret_cast<void**>(slot); }

        // ---- shared backing (touched rarely, in batches, under the lock) ----
        //
        // LAZY. `new Block[n]` DEFAULT-initializes a trivial type and therefore writes nothing, plus
        // a bump cursor so slots are linked in BATCH-sized groups the first time they are actually
        // needed. Resident cost becomes proportional to PEAK LIVE slots instead of capacity.
        std::unique_ptr<Block[]> mem;
        std::size_t memSlots = 0;
        // Index of the first slot never yet handed out. Everything below it has been through the
        // free list at least once; everything at or above it has never been touched. Guarded by mtx.
        std::size_t bumpNext = 0;
        void* sharedHead = nullptr;
        std::mutex mtx;

        // ---- live-slot accounting, SHARDED PER THREAD ----
        // This was one `std::atomic<long long>` incremented on every Alloc and decremented on every
        // Free. That is a single cache line hit twice per slot by every worker in the pool, and
        // `memory_order_relaxed` does NOT make it cheap: relaxed governs ORDERING, which is free on
        // x86 anyway; the cost is the cache-line ownership transfer, which relaxed does nothing
        // about. Measured at ~3.3 ns of pure latency per uncontended RMW.
        using LiveCounter = detail::LiveCounter;
        static constexpr std::size_t kLiveSlots = 128;
        inline static LiveCounter s_live[kLiveSlots];
        inline static std::atomic<std::size_t> s_liveNext{ 0 };

        struct LiveRef { LiveCounter* c; bool exclusive; };
        static const LiveRef& liveSlot() {
            static thread_local LiveRef r = [] {
                const std::size_t n = s_liveNext.fetch_add(1, std::memory_order_relaxed);
                // n < kLiveSlots  =>  n % kLiveSlots == n, and no earlier or later thread was or
                // will be handed the same index. That is the exclusivity the fast path needs.
                return LiveRef{ &s_live[n % kLiveSlots], n < kLiveSlots };
            }();
            return r;
        }
        static void liveAdd(long long d) {
            const LiveRef& r = liveSlot();
            if (r.exclusive) {
                // Sole writer: read it, adjust it, put it back. No lock prefix, no line handoff.
                r.c->v.store(r.c->v.load(std::memory_order_relaxed) + d, std::memory_order_relaxed);
            }
            else {
                r.c->v.fetch_add(d, std::memory_order_relaxed);
            }
        }


        // ---- contention instrumentation (opt-in) ------------------------------------------------
        //
        // ANSWERS ONE QUESTION: is the shared-tier mutex actually contended? The per-thread cache
        // serves BATCH-1 of every BATCH allocations with plain pointer writes, so the lock is only
        // touched on refill and flush. Whether that matters is measurable, and this project has been
        // wrong about exactly this kind of guess before -- relaxed atomics on the push path measured
        // ZERO because the real cost was a mutex somewhere else entirely.
        //
        // CONTENTION IS COUNTED WITH try_lock, NOT TIMED. A timer around the acquire would cost more
        // than the acquire and would perturb what it measures; a failed try_lock is a direct,
        // essentially free answer to "did anyone have to wait".
        //
        // THE COUNTERS ARE SHARDED PER THREAD for the same reason the live counters are. Shared
        // atomics here would manufacture the contention being measured -- an instrument that creates
        // its own signal. That is not a hypothetical caution: an unsharded counter added to this
        // allocator earlier cost 62% and was caught only by an A/B bench.
#ifdef JLIBSCHED_ALLOC_STATS
        struct alignas(platform::kCacheLine) StatCounter {
            std::atomic<std::uint64_t> allocs{ 0 };
            std::atomic<std::uint64_t> frees{ 0 };
            std::atomic<std::uint64_t> refills{ 0 };
            std::atomic<std::uint64_t> refillBlocked{ 0 };
            std::atomic<std::uint64_t> flushes{ 0 };
            std::atomic<std::uint64_t> flushBlocked{ 0 };
        };
        inline static StatCounter s_stats[kLiveSlots];
        inline static std::atomic<std::size_t> s_statNext{ 0 };
        static StatCounter& statSlot() {
            static thread_local StatCounter* c = [] {
                const std::size_t n = s_statNext.fetch_add(1, std::memory_order_relaxed);
                return &s_stats[n % kLiveSlots];
            }();
            return *c;
        }
        #define JLIBSCHED_STAT_BUMP(field) \
            statSlot().field.store(statSlot().field.load(std::memory_order_relaxed) + 1, \
                                   std::memory_order_relaxed)
#else
        #define JLIBSCHED_STAT_BUMP(field) ((void)0)
#endif

        // Acquire the shared-tier lock, counting whether anyone had to WAIT. try_lock first: a
        // failure means the section was genuinely contended, which is the only number that decides
        // whether a lock-free free list would be worth its ABA problem. Compiles to a plain
        // lock_guard when stats are off.
#ifdef JLIBSCHED_ALLOC_STATS
        #define JLIBSCHED_LOCK_SHARED(which)                                       \
            if (!mtx.try_lock()) { JLIBSCHED_STAT_BUMP(which); mtx.lock(); }       \
            std::lock_guard<std::mutex> lk(mtx, std::adopt_lock)
#else
        #define JLIBSCHED_LOCK_SHARED(which) std::lock_guard<std::mutex> lk(mtx)
#endif

        // ---- per-thread cache (the lock-free hot path) ----
        //
        // ONE CACHE PER THREAD PER SIZE CLASS -- `static thread_local` in a static member function of
        // a class TEMPLATE, so each instantiation has its own. That is the property that makes a
        // second size class safe, and the reason this is not a plain class: two pools sharing one
        // cache means a slot freed through pool A is handed out by pool B, with neither the size nor
        // the ownership matching, and nothing diagnoses it (the free list is intrusive and carries
        // no tag).
        //
        // Still one cache per thread per class rather than per INSTANCE, so two instances of the
        // SAME class would alias -- which the constructor guard below refuses outright.
        //
        // Freeing a slot on a DIFFERENT thread than allocated it is fine, and is normal here --
        // epoch reclamation runs the deleter on whichever thread happens to reclaim. Slots are
        // interchangeable within one pool, so a slot simply migrates to that thread's cache.
        struct Cache { void* head = nullptr; std::size_t count = 0; };
        static Cache& local() {
            static thread_local Cache c;
            return c;
        }

        // Instances of THIS size class alive right now. Exists solely for the constructor check.
        static std::atomic<int>& LiveInstances() {
            static std::atomic<int> n{ 0 };
            return n;
        }

#ifdef JLIBSCHED_ALLOC_CANARY
        // Bytes [8,16) of a slot hold the intrusive "next free" link's tail + are otherwise unused
        // while free (the link itself only needs the first 8 bytes). Free() stamps a canary there;
        // Alloc() verifies it is UNCHANGED before handing the slot back out. If something wrote
        // through this slot AFTER it was freed, or the same slot got Free()'d twice, the canary will
        // have been clobbered -- caught at the NEXT Alloc() of that exact slot, right at the point of
        // detection, instead of silently corrupting the chain until the pool appears "exhausted" far
        // downstream of the actual bug.
        static constexpr std::uint64_t kFreeCanary = 0xFEEDFACECAFEBEEFULL;
        static void StampCanary(void* slot) {
            *reinterpret_cast<std::uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8) = kFreeCanary;
        }
        static void CheckCanary(void* slot) {
            const std::uint64_t v = *reinterpret_cast<std::uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8);
            if (v != kFreeCanary) {
                JLIBSCHED_CANARY_REPORT("SlabPool: corrupted freed slot detected "
                    "(use-after-free or double-free) -- breaking at the Alloc() that noticed.\n");
            }
        }
        static_assert(SLOTSZ >= 16, "the canary needs bytes [8,16) of a free slot");
#endif
        static_assert(SLOTSZ >= sizeof(void*), "a free slot must hold the intrusive next pointer");

    public:
        // A second instance of the SAME size class shares the per-thread cache above and each hands
        // out the other's slots. That corrupts the heap immediately and the crash appears somewhere
        // unrelated -- it is not hypothetical, it is what happened the first time coroutine frames
        // were pooled from a second allocator (0xC0000374, see bench/coroutine_bench.cpp).
        //
        // Checked unconditionally rather than under the canary: it runs once per pool for the life
        // of the process, so it costs nothing measurable, and the failure it prevents is both
        // catastrophic and completely undiagnosable from the symptom.
        //
        // DIFFERENT size classes are fine and are the point -- they are different types, hence
        // different caches and different counters.
        explicit SlabPool(std::size_t slots, bool lazy = false)
            : mem(slots ? new Block[slots] : nullptr), memSlots(slots) {
            if (LiveInstances().fetch_add(1, std::memory_order_relaxed) != 0) {
                std::fprintf(stderr,
                    "[JLib::Scheduler] FATAL: a second SlabPool<%zu> was constructed.\n"
                    "  A pool's per-thread free-list cache is shared by all instances of that size\n"
                    "  class (it is a static thread_local in a static member function), so two slabs\n"
                    "  feed one free list and each hands out the other's slots. This corrupts the\n"
                    "  heap immediately and the crash appears somewhere unrelated.\n"
                    "  Use one pool per size class, or make local() per-instance first.\n",
                    SLOTSZ);
                std::fflush(stderr);
                std::abort();
            }
            if (!lazy) Prefault(slots);
        }

        ~SlabPool() { LiveInstances().fetch_sub(1, std::memory_order_relaxed); }

        SlabPool(const SlabPool&) = delete;
        SlabPool& operator=(const SlabPool&) = delete;

        // Is `p` the start of a real slot in this pool's slab?
        //
        // THE CANARY HAS A BLIND SPOT AND THIS COVERS IT. The canary lives in bytes [8,16) of a freed
        // slot, but the intrusive free-list LINK is bytes [0,8). A stray write that lands only on the
        // link leaves the canary intact, so Alloc() reports nothing -- and the first symptom is
        // refill()'s walk dereferencing garbage, an access violation with no clue attached. That is
        // how it presented in Game01 (Debug), where next() returned 0x100000.
        //
        // Also the basis for routing a pointer to the right size class on free: separate backing
        // allocations cannot overlap, so at most one pool claims any given address.
        bool SlotInSlab(const void* p) const {
            if (!mem) return false;
            const std::byte* base = reinterpret_cast<const std::byte*>(mem.get());
            const std::byte* q    = reinterpret_cast<const std::byte*>(p);
            if (q < base || q >= base + (std::size_t)memSlots * SLOT) return false;
            return ((std::size_t)(q - base) % SLOT) == 0;   // a slot START, not an interior byte
        }

        void* Alloc() {                        // lock-free unless the cache is empty
            Cache& c = local();
            JLIBSCHED_STAT_BUMP(allocs);
            if (!c.head) refill(c);
            if (!c.head) return nullptr;       // backing fully exhausted
            void* slot = c.head;
#ifdef JLIBSCHED_ALLOC_CANARY
            CheckCanary(slot);
#endif
            c.head = next(slot);
            c.count--;
            // Owning the line is necessary but was never sufficient -- an uncontended `lock xadd`
            // still cost ~3.3 ns of pure latency here. See liveAdd for what replaced it.
            liveAdd(+1);
            return slot;
        }

        void Free(void* slot) {                // lock-free unless the cache overflows
            JLIBSCHED_STAT_BUMP(frees);
            Cache& c = local();
#ifdef JLIBSCHED_ALLOC_CANARY
            // Catch a bad pointer AT THE FREE, which is far nearer the culprit than the refill()
            // that eventually trips over it. Anything not a slot start -- a stack or heap address, an
            // interior byte, a pointer mangled by a tag that was not masked off -- would be linked
            // into the free list here and handed out later.
            if (!SlotInSlab(slot)) {
                char msg[192];
                std::snprintf(msg, sizeof msg,
                    "SlabPool: Free(%p) is not a slot in this slab -- refusing to link it into the "
                    "free list. Freeing a non-slab pointer, or freeing twice through a mangled one.\n",
                    slot);
                JLIBSCHED_CANARY_REPORT(msg);
                return;                        // do NOT corrupt the list with it
            }
#endif
            next(slot) = c.head;
#ifdef JLIBSCHED_ALLOC_CANARY
            StampCanary(slot);
#endif
            c.head = slot;
            c.count++;
            liveAdd(-1);
            if (c.count > 2 * BATCH) flush(c);
        }

        // Diagnostic only: how many slots are currently checked out (Alloc'd but not yet Free'd),
        // across every thread's cache + the shared pool.
        long long LiveCount() const {
            // Summed on demand rather than maintained. Individual shards go NEGATIVE and that is
            // correct, not a bug to clamp: a slot allocated on one thread and freed on another
            // leaves +1 on one shard and -1 on the other. Only the total means anything.
            //
            // Deliberately not a synchronized snapshot: shards are read one at a time while other
            // threads keep working, so the result is a smear across a short window rather than the
            // state at an instant. That was already true of the single atomic, and it is fine for the
            // job -- watching whether usage climbs monotonically (a leak) or oscillates (churn).
            long long n = 0;
            for (std::size_t i = 0; i < kLiveSlots; ++i)
                n += s_live[i].v.load(std::memory_order_relaxed);
            return n;
        }
        std::size_t Capacity() const { return memSlots; }

        // Print what the counters saw. Call after the workload, not during -- it reads relaxed
        // shards while other threads may still be running, so it is a smear across a short window
        // rather than an instant, which is fine for the question it answers.
        //
        // READ refill-blocked / refills. That ratio IS the answer to "would a lock-free free list
        // help": near zero means the shared tier is essentially never contended and a CAS pop would
        // buy nothing while costing an ABA problem that needs tagged pointers or EBR to solve. High
        // means the lock convoys under burst, and the cheap fix is to SHARD the shared tier -- more
        // lists, chosen by thread -- not to make one list lock-free.
        static void ReportStats(const char* label) {
#ifdef JLIBSCHED_ALLOC_STATS
            std::uint64_t allocs = 0, frees = 0, refills = 0, rBlocked = 0, flushes = 0, fBlocked = 0;
            for (std::size_t i = 0; i < kLiveSlots; ++i) {
                allocs   += s_stats[i].allocs.load(std::memory_order_relaxed);
                frees    += s_stats[i].frees.load(std::memory_order_relaxed);
                refills  += s_stats[i].refills.load(std::memory_order_relaxed);
                rBlocked += s_stats[i].refillBlocked.load(std::memory_order_relaxed);
                flushes  += s_stats[i].flushes.load(std::memory_order_relaxed);
                fBlocked += s_stats[i].flushBlocked.load(std::memory_order_relaxed);
            }
            const double perRefill = refills ? double(allocs) / double(refills) : 0.0;
            std::printf("  %-14s allocs %9llu  frees %9llu  refills %7llu (1 per %.1f, %llu blocked %.2f%%)"
                        "   flushes %8llu (%llu blocked, %.2f%%)\n",
                        label,
                        (unsigned long long)allocs,
                        (unsigned long long)frees,
                        (unsigned long long)refills, perRefill,
                        (unsigned long long)rBlocked,
                        refills ? 100.0 * double(rBlocked) / double(refills) : 0.0,
                        (unsigned long long)flushes,
                        (unsigned long long)fBlocked,
                        flushes ? 100.0 * double(fBlocked) / double(flushes) : 0.0);
#else
            (void)label;
#endif
        }

        // Pay the lazy slab's page faults NOW instead of during the run.
        //
        // The lazy backing makes resident memory proportional to peak live slots, which is the whole
        // point on mobile -- but it moves the cost of first-touching a page from startup to whenever
        // that page is first needed. For a workload whose peak live count GROWS over time, those
        // faults land mid-run, and a fault mid-frame is the same kind of latency spike that made
        // worker-side epoch reclamation worth moving off the critical path.
        //
        // Links the slots into the free list in the same order refill would, so this is purely a
        // scheduling change: no slot is treated differently for having been prefaulted.
        void Prefault(std::size_t slots) {
            if (!mem) return;
            std::lock_guard<std::mutex> lk(mtx);
            if (slots > memSlots - bumpNext) slots = memSlots - bumpNext;
            for (std::size_t k = 0; k < slots; ++k) {
                void* slot = &mem[bumpNext + k];
                next(slot) = sharedHead;
                sharedHead = slot;
#ifdef JLIBSCHED_ALLOC_CANARY
                StampCanary(slot);
#endif
            }
            bumpNext += slots;
        }

    private:
        // Move up to BATCH slots shared -> local by SPLICING a sub-chain instead of relinking
        // node-by-node. The walk MUST stay under the lock (sharedHead is shared), so the critical
        // section is still O(BATCH) pointer chases -- but the relinking is two writes.
        void refill(Cache& c) {
            JLIBSCHED_STAT_BUMP(refills);
            if (!mem) return;
            void* batchHead = nullptr;
            void* batchTail = nullptr;
            std::size_t moved = 0;
            {
                JLIBSCHED_LOCK_SHARED(refillBlocked);

                // 1. RECYCLED slots first. Preferring these over never-touched ones is what keeps
                //    the resident set at peak-live rather than creeping toward capacity: a steady
                //    workload churns the same pages forever and never advances the bump cursor.
                if (sharedHead) {
                    batchHead = sharedHead;
                    void* curr = batchHead;
                    while (curr && moved < BATCH) {
                        batchTail = curr;
                        curr = next(curr);
#ifdef JLIBSCHED_ALLOC_CANARY
                        // Report a broken link HERE, naming both ends, instead of dereferencing it
                        // on the next pass and dying with no context. See SlotInSlab.
                        if (curr && !SlotInSlab(curr)) {
                            char msg[256];
                            std::snprintf(msg, sizeof msg,
                                "SlabPool: free-list link corrupted -- slot %p points to %p, which is "
                                "not a slot in the slab [%p, %p). Something wrote the first 8 bytes of "
                                "a FREED slot (the canary at [8,16) cannot see that).\n",
                                batchTail, curr, (void*)mem.get(),
                                (void*)(reinterpret_cast<std::byte*>(mem.get()) + (std::size_t)memSlots * SLOT));
                            JLIBSCHED_CANARY_REPORT(msg);
                            curr = nullptr;          // stop the walk rather than follow it
                        }
#endif
                        ++moved;
                    }
                    sharedHead = curr;              // detach [batchHead .. batchTail] in ONE store
                }

                // 2. Top up from the NEVER-USED region. This is the only place a fresh page is
                //    touched, and it happens BATCH slots at a time rather than all of them at
                //    construction.
                if (moved < BATCH && bumpNext < memSlots) {
                    std::size_t take = BATCH - moved;
                    if (take > memSlots - bumpNext) take = memSlots - bumpNext;
                    for (std::size_t k = 0; k < take; ++k) {
                        void* slot = &mem[bumpNext + k];
                        // Prepend, so the tail of the combined chain stays whatever step 1 found
                        // (or the first bump slot if step 1 found nothing).
                        next(slot) = batchHead;
                        if (!batchHead) batchTail = slot;
                        batchHead = slot;
#ifdef JLIBSCHED_ALLOC_CANARY
                        // Stamped HERE, not in the constructor. `new Block[n]` leaves these bytes
                        // indeterminate, so without this the first Alloc() of a never-used slot would
                        // read garbage at [8,16) and report corruption that never happened.
                        StampCanary(slot);
#endif
                    }
                    bumpNext += take;
                    moved += take;
                }
            }
            if (!batchHead) return;                 // recycled list empty AND capacity exhausted
            // Thread-local from here -- batchTail's chain is ours alone now.
            next(batchTail) = c.head;
            c.head = batchHead;
            c.count += moved;
        }

        // Move the excess (down to BATCH) local -> shared. The walk is over the THREAD-LOCAL cache,
        // which needs no lock, so the excess sub-chain is peeled off entirely outside the lock and
        // the critical section collapses to just the 2-write splice.
        //
        // This tier is what keeps a one-way workload alive: coroutine producers allocate on one
        // thread and workers free on another, and a pool without a shared tier drains on one side and
        // piles up on the other. An earlier per-thread frame pool failed exactly that way.
        void flush(Cache& c) {
            JLIBSCHED_STAT_BUMP(flushes);
            if (c.count <= BATCH) return;
            std::size_t toMove = c.count - BATCH;
            void* batchHead = c.head;               // peel the top `toMove` nodes (all thread-local)
            void* batchTail = batchHead;
            for (std::size_t i = 1; i < toMove; ++i)
                batchTail = next(batchTail);
            c.head = next(batchTail);               // local cache keeps the remaining BATCH nodes
            c.count = BATCH;
            {
                JLIBSCHED_LOCK_SHARED(flushBlocked);
                next(batchTail) = sharedHead;       // splice the whole sub-chain onto shared in
                sharedHead = batchHead;             // two writes -- the entire critical section
            }
        }
    };
}
