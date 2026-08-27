// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// HAZARD POINTERS -- safe reclamation for structures behind FIBER-AWARE LOCKS.
//
// == WHY THIS EXISTS AND EPOCHS DO NOT COVER IT ==
//
// SchedulerMutex, SchedulerSemaphore and the condition variable all SUSPEND the fiber on
// contention -- that is the point of them, since a suspended fiber releases its worker where a
// std::mutex would park the OS thread and cost a core.
//
// Epochs.h forbids exactly that: a fiber may not suspend inside an EpochGuard, enforced by a debug
// tripwire. So a structure guarded by a fiber-aware lock has NO reclamation scheme available -- its
// critical section can suspend, so EBR is illegal, leaving std::mutex (stalls a worker) or nothing.
//
// The dividing line is CAN THE READER SUSPEND, not lock-free versus locked:
//
//   Epochs            lock-free structures. Reader never suspends. One announce per traversal.
//                     A stalled reader pins EVERYTHING retired since it announced.
//   Hazard pointers   structures behind fiber-aware locks. Reader suspends by design. A store plus
//                     fence per protected pointer. A stalled reader pins ONLY the nodes it named.
//
// Epochs are unchanged and stay on the steal path; that is what this cost column buys.
//
// == THE TWO BUGS A TEXTBOOK PORT HAS HERE ==
//
// Michael's HP is slots[tid][i] -- publish, use, clear. That is correct when a reader IS a thread.
// Here a reader migrates, and there are two independent failures:
//
//   1. SLOT REUSE ON THE PUBLISHING WORKER. Fiber A on W0 publishes, parks on a SchedulerMutex; W0
//      then runs fiber B, which writes the same cell. A retire elsewhere scans, sees the cell no
//      longer names the node, frees it. A resumes on W1 holding a dangling pointer.
//
//   2. THE SCAN DOES NOT SEE PARKED FIBERS. Same outcome with no overwrite at all: A published on
//      W0 and resumes on W1, and a scan that walks only RUNNING workers never sees A because A is
//      parked. Free under a sleeper.
//
// Bug 1 is about WHERE CELLS LIVE. Bug 2 is about WHAT RETIRE WALKS. Fixing one leaves the other.
//
// Both are answered by indexing cells by READER rather than by worker, where "reader" is:
//
//   Fiber      Fiber::poolIndex -- the unit that migrates, which is why Fiber already carries its
//              EBR slot for the identical reason ("the fiber is the unit that migrates across
//              workers, so the slot lives here (not on the thread)").
//   Native     the worker. A native task does not change stack mid-section, so it cannot migrate
//              inside a protected span and worker-owned cells are correct for it.
//   External   a non-worker thread (main, an app thread) claims one of a small reserved block.
//
// The table is FLAT and indexed by that reader id, which is what makes the scan include parked
// fibers BY CONSTRUCTION: a parked fiber's index still addresses its cells, and the scan walks the
// table rather than walking "who is running". Same perfect-hash property Event's waiter index uses.
//
// == WHAT IS NOT SUPPORTED YET ==
//
// COROUTINES MUST NOT HOLD A GUARD ACROSS A co_await. A coroutine resumes as a call on whatever
// worker takes it, so it resolves to worker-owned cells -- correct while it does not suspend, wrong
// the moment it does. Cells for coroutines belong on the PROMISE, unregistered at final_suspend,
// and that registry is deliberately not built here: coro frames are not a bounded pool, so it
// cannot reuse this table. See design/hazard-pointers.md.
//
// == ALLOCATION ==
//
// Retire takes a DELETER and defaults to `delete`. This never touches the task slab. The slab is
// SCHEDULER-owned memory and exists because the scheduler allocates its own tasks and frames; a
// client structure behind a SchedulerMutex lives in client memory -- the heap, or the app's
// per-frame arena. A job system has no business dictating an app's allocation strategy.

#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace JLib {

    class HazardDomain {
    public:
        // Cells per reader. FIXED, because the scan is O(readers * this) and an unbounded budget
        // would make retire unbounded. Two is enough for hand-over-hand list traversal (current +
        // next); four leaves room for a skip-list style hop without another design pass. Exceeding
        // it asserts rather than silently failing to protect.
        static constexpr std::size_t kCellsPerReader = 4;

        // Reserved reader ids for threads that are neither a fiber nor a worker -- the main thread,
        // an app's own threads. Claimed lazily and never released, so a program churning thousands
        // of short-lived threads exhausts them; that asserts rather than corrupting, and is the
        // documented growth caveat.
        static constexpr std::size_t kExternalReaders = 64;

        static HazardDomain& Instance();

        // Resolves the CURRENT execution context to a reader id. See the header comment for the
        // three cases. Never returns kNoReader for a legal caller.
        static constexpr std::size_t kNoReader = ~std::size_t(0);
        std::size_t CurrentReader();

        std::atomic<void*>* Cells(std::size_t reader);

        // Hand `p` over for deletion once no reader names it. Appended to a per-THREAD batch, which
        // is sound where a per-thread PROTECT cell would not be: retiring is a point operation that
        // cannot suspend, while protection spans one.
        void Retire(void* p, void (*deleter)(void*));

        // Frees everything in this thread's batch that no live cell names. Called automatically
        // when a batch grows past its threshold; public so a test can force it.
        void Scan();

        // Diagnostics only.
        // Builds the table if needed -- reporting 0 because nobody has taken a guard yet reads as
        // "the mechanism is inert", which is the opposite of the truth.
        std::size_t ReaderCount() { EnsureTable(); return readerCount; }
        std::size_t PendingRetired() const;

        // TEST HOOK, and it exists to make a NEGATIVE CONTROL deterministic rather than lucky.
        //
        // Forces CurrentReader() to resolve a fiber to its WORKER's cells -- i.e. reintroduces bug
        // 1 on purpose. The migration test asserts it FAILS with this on and passes with it off,
        // which is the only way to show the test is testing the mechanism rather than passing
        // because nothing happened to migrate. Six tests in this codebase have passed with their
        // mechanism removed; a hazard test that never parks a publisher would be the seventh.
        //
        // Never set this outside a test.
        static void ForceWorkerCellsForTest(bool on);

    private:
        void EnsureTable();

        std::atomic<std::atomic<void*>*> cells{ nullptr };
        std::atomic<std::uint64_t>*      externalOwners = nullptr;   // thread id claims
        std::size_t fiberCount  = 0;
        std::size_t workerCount = 0;
        std::size_t readerCount = 0;
    };

    // RAII over one reader's cells. Construct inside the traversal, destroy when the last protected
    // pointer is dead. Safe to hold across a SchedulerMutex acquire -- that is the entire purpose.
    class HazardGuard {
    public:
        HazardGuard();
        ~HazardGuard();

        HazardGuard(const HazardGuard&) = delete;
        HazardGuard& operator=(const HazardGuard&) = delete;

        // PUBLISH, FENCE, RE-READ. The re-read is not optional and is the whole protocol: naming a
        // pointer you loaded a moment ago proves nothing, because it may have been retired between
        // the load and the store. Loops until the source still holds what was published, which is
        // the point at which a retire either saw the cell or had not yet freed the node.
        template <typename T>
        T* Protect(std::size_t k, const std::atomic<T*>& src) {
            for (;;) {
                T* p = src.load(std::memory_order_acquire);
                Set(k, static_cast<void*>(p));
                // SEQ_CST, not release. This has to order a STORE (our cell) against a later LOAD
                // (re-reading src) -- StoreLoad, the one ordering release does not give. The
                // retiring side pays the symmetric fence in Scan().
                std::atomic_thread_fence(std::memory_order_seq_cst);
                if (src.load(std::memory_order_acquire) == p) return p;
            }
        }

        void Set(std::size_t k, void* p);
        void Clear(std::size_t k);
        void Swap(std::size_t a, std::size_t b);

        std::size_t Reader() const { return reader; }

    private:
        std::atomic<void*>* cells  = nullptr;
        std::size_t         reader = HazardDomain::kNoReader;
    };

    // Typed convenience. Deletes through T*, so T's destructor runs.
    template <typename T>
    inline void HazardRetire(T* p) {
        if (!p) return;
        HazardDomain::Instance().Retire(
            static_cast<void*>(p),
            [](void* q) { delete static_cast<T*>(q); });
    }

    inline void HazardRetire(void* p, void (*deleter)(void*)) {
        if (p) HazardDomain::Instance().Retire(p, deleter);
    }

} // namespace JLib
