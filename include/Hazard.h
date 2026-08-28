// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// == CREDIT ==
//
// ORIGINAL DESIGN: Maged M. Michael, "Hazard Pointers: Safe Memory Reclamation for Lock-Free
// Objects" (IEEE TPDS 15(6), 2004). The core is his: publish a pointer, fence, reload and verify,
// and let a retiring thread scan the published set before it frees. Everything below that line is
// unchanged from the paper and should stay that way.
//
// ADDED HERE, and each one is a deviation you should read before trusting your intuition about HP:
//
//   FIBER-AS-THREAD. Michael's reader is a THREAD and his cells are slots[tid][i]. Here the reader
//   is whatever migrates, so cells are indexed by Fiber::poolIndex for fibers, by worker for native
//   tasks, and by a reserved block for non-worker threads. This is not a cosmetic renaming: it is
//   the fix for both of the bugs described above, and a textbook slots[tid][i] port has them.
//
//   COROUTINES, via a SCOPE-BOUNDARY RULE rather than a second reader type. A coroutine may hold a
//   guard; it may not carry one across a suspension point. The supported shape is the standard
//   production pattern -- hazard-protect the LOOKUP, hand off to a refcount, DROP the guard, then
//   co_await -- and the rule is enforced at detail::ArmResume, the one choke point every awaiter
//   passes through, so a non-suspending guarded span costs nothing and raises nothing.
//
//   THAT PATTERN IS NOT ORIGINAL HERE and is credited rather than presented as a discovery: it is
//   how concurrent runtimes conventionally combine hazard pointers with asynchronous code, and it
//   reached this file through an AI-assisted design pass rather than being derived from the code.
//   What IS this codebase's is the verification that ArmResume is the right place to enforce it
//   (eight call sites, all in-library) and the tests that show it works in both directions.
//
//   AN EARLIER ATTEMPT GAVE COROUTINES THEIR OWN READER -- an external record from a pool, with a
//   generation and a grace period keyed on a scan count. It was removed, not repaired: the grace
//   could not be stated in one testable sentence and nothing exercised record reuse. Every bug ever
//   found in this file lived in that adaptation. experimental/hazard/README.md has the postmortem
//   and the two designs that would do it properly if it is ever wanted.
//
//   FAILURE MODES AND TRIPWIRES. Michael's paper does not have to describe what a migrating reader
//   does, because migrating readers do not exist in it. The warnings in this file and in
//   design/hazard-pointers.md are ours, they were each written after finding the thing they warn
//   about, and the assert text is part of the interface -- do not soften it.
//
// WHY MIGRATION IS THE PROBLEM AT ALL is upstream of this file: in this runtime the unit that
// blocks is a FIBER rather than a thread, so Michael's readers cannot move and these can. That
// shape matches the fiber-based job system Christian Gyrling described in "Parallelizing the
// Naughty Dog Engine Using Fibers" (GDC 2015) -- started independently here and informed by it
// afterwards. See the credit block in TaskScheduler.h for the full history.
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
//   Coroutine  a RECORD from a registry, acquired by the guard on first Protect. A frame has no
//              dense stable index -- frames are not a bounded pool -- so it cannot be a row in the
//              flat table. The record is what makes coroutines work; see below.
//   External   a non-worker thread (main, an app thread) claims one of a small reserved block.
//
// The table is FLAT and indexed by that reader id, which is what makes the scan include parked
// fibers BY CONSTRUCTION: a parked fiber's index still addresses its cells, and the scan walks the
// table rather than walking "who is running". Same perfect-hash property Event's waiter index uses.
// The record registry is scanned alongside it, for the same reason and with the same property.
//
// == COROUTINES ARE SUPPORTED -- AND THIS IS WHERE THEY BEAT EPOCHS ==
//
// A coroutine may hold a HazardGuard across a co_await. That is the whole point of the record: it
// is owned by the GUARD, not by the worker and not by the frame, so it survives the suspend and
// travels with the logical reader to whichever worker resumes it.
//
// CONTRAST WITH Epochs.h, and this is the reason both schemes exist. A coroutine has no epoch slot
// of its own -- it borrows the WORKER's -- so suspending inside an EpochGuard is a contract
// violation with a debug tripwire on it, and counted epochs exist to make the release-build
// consequence a bounded leak instead of a smash. Hazard pointers have no such restriction.
//
// SO THE RULE FOR PICKING ONE IS PERFORMANCE, NOT SAFETY -- both are safe now:
//
//   Fiber / native reader, never suspends      epoch guard    ~0.40 ns    the optimal path
//   Coroutine reader                           counted epoch  ~0.55 ns    safe, SRCU-shaped
//   Coroutine reader, or a suspending one      hazard         pricier per protected pointer,
//                                                             but it does not pin everything
//                                                             retired since it announced
//
// If a lock-free path can be entered by BOTH a coroutine and a fiber, branch on
// TaskScheduler::CurrentTaskType() and take the guard that fits -- that dual-guard pattern is
// approved and documented in design/NOTES.md.
//
// The record is released by ~HazardGuard -- NOT by ~promise_type, which was the discarded plan --
// and is NEVER DELETED: FREE -> LIVE -> RETIRED -> FREE with a bumped generation, because a hazard
// registry cannot be protected by the hazard domain it serves.
// One known limitation remains and it is a LEAK, NOT A SMASH -- see the note on ReaderCount below.
//
// == COROUTINES: TASKS YES, GUARDS NO ==
//
// TWO DIFFERENT FEATURES, and conflating them is what produced the registry. Coroutine TASKS in the
// pool are fully supported and untouched by this file -- resume, TaskType::Coroutine, all of it. What
// is refused is a HazardGuard inside a coroutine frame.
//
//   A HazardGuard IN A COROUTINE IS FATAL AT Protect. The constructor leaves cells null and returns;
//   protecting through it aborts with a message naming counted epochs as the alternative.
//   Constructing one is inert, because construction is not the illegal act.
//
//   WHY REFUSAL RATHER THAN SUPPORT. Cells are indexed by reader, and a coroutine frame has no dense
//   stable index -- frames are not a bounded pool. Covering that took an external record registry
//   with its own FREE -> LIVE -> RETIRED -> FREE lifecycle and a grace period keyed on a scan count.
//   That grace could not be stated in one testable sentence, nothing exercised record reuse, and
//   every one of the six bugs found in this file lived in that adaptation rather than in Michael's
//   algorithm. It was removed. See experimental/hazard/README.md for what bringing it back requires.
//
//   AND NO DOWNGRADE, WHICH IS THE POINT. Worker or fiber cells are correct until the frame's first
//   co_await and a use-after-free after it -- so a fallback would pass every test that does not
//   suspend inside a protected section, which is exactly the case it would exist for. Refusal is the
//   feature.
//
//   USE COUNTED EPOCHS for a reader that suspends. That is what the counted scheme was built for and
//   it is verified; epochs remain this engine's reclamation, and hazard pointers are the extra.
//
// == THE ONE RULE THAT SURVIVES FOR EVERY READER ==
//
//   DO NOT STASH A RAW T* AND DROP THE GUARD. Protection is the GUARD'S LIFETIME, not the
//      pointer's. ~HazardGuard clears the cells; a pointer you kept past that point is a plain
//      pointer with nothing announcing it, and the next retire is free to reclaim it.
//
//      THIS IS EASIER TO GET WRONG IN A COROUTINE THAN ANYWHERE ELSE, which is why it is written
//      here rather than assumed. In a fiber or a native task the scope that drops the guard usually
//      ends soon after, so the mistake is short-lived and often harmless. A coroutine frame OUTLIVES
//      its suspensions, so a stashed pointer keeps LOOKING valid -- the frame still holds it, the
//      code still compiles, and the read after resume is a use-after-free with nothing nearby to
//      suggest it. Hold the guard across every use, including across the co_await.
//
// RULE 1 IS ENFORCED, RULE 2 CANNOT BE. Exhaustion is a state this code can observe, so it aborts
// on it. A raw pointer copied out of a protected read is invisible from here -- there is no
// signature to check and no tripwire to arm. That asymmetry is why rule 2 is documented instead,
// and why review of any coroutine that takes a guard should look for it specifically.
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
        // Cells per reader. A BUDGET, not a derived number, and a compile-time knob
        // (-DJLIBSCHED_HAZARD_CELLS=n) because a structure that needs six should SAY so rather than
        // hope four is lucky. What actually consumes them:
        //
        //     one payload pointer                                     1
        //     lock-free list walk (prev / curr, sometimes next)        2-3
        //     mutex wait plus "I still name this node"                 +1
        //     a nested HP section on the same fiber                    +depth
        //
        // So 4 = "list walk plus one extra protect, no nesting". That covers a SchedulerMutex wait
        // plus a list node. It does NOT cover two nested walks or a tree descent. If this library's
        // own mutex/semaphore/event paths ever nest HP walks, count them once and freeze this at
        // that count + 1 -- not at a round number.
        //
        // The scan is O(readers * this), so raising it is not free; readers is in the thousands.
#ifndef JLIBSCHED_HAZARD_CELLS
    #define JLIBSCHED_HAZARD_CELLS 4
#endif
        static constexpr std::size_t kCellsPerReader = JLIBSCHED_HAZARD_CELLS;
        static_assert(kCellsPerReader >= 1, "a reader needs at least one hazard cell");

        // Running out of cells, or failing to resolve a reader, is a PROGRAMMING ERROR and is
        // reported here rather than absorbed. Never returns.
        [[noreturn]] static void FatalCellOverflow(std::size_t k, const char* what);


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

        // ORPHANED RETIRES -- what a thread left behind when it exited with a non-empty bag.
        //
        // Retire takes a deleter over the CALLER'S memory, so a bag abandoned at thread exit is not
        // delayed reclamation, it is a leak with the destructor never running. ~RetireBatch hands
        // leftovers to a global store and any later Scan sweeps it under the same safety test.
        //
        // Both are ZERO in a healthy process. OrphanedRetired is what is waiting now; OrphanedTotal
        // is cumulative, because the first number goes back to zero and cannot answer whether the
        // handoff ever ran -- which is exactly what a test has to assert.
        std::size_t OrphanedRetired() const;
        std::size_t OrphanedTotal() const;

        // NO KNOWN LIMITATION HERE ANY MORE. There used to be one: a coroutine parked while holding
        // a guard and never re-pushed leaked its record forever. It went with the registry -- a
        // coroutine cannot hold a guard at all now, so there is nothing left to leak.

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


        // Builds the table. Called by TaskScheduler::Init once the workers and the fiber pool are
        // actually up -- the lazy path cannot be trusted alone, because a worker reaching the sleep
        // path flushes the retire bag DURING Init and would otherwise bake in an empty table.
        void Init();

    private:
        void EnsureTable();

    public:
        // TRUE if this reader id is a FIBER row rather than a worker or external one. The suspend
        // check needs it: a parked fiber legitimately holds a guard across its park -- that is the
        // entire point of reader-indexed cells -- so fiber guards must NOT count toward the
        // per-thread depth, or the next coroutine to suspend on that worker trips a false alarm.
        bool IsFiberReader(std::size_t reader) const { return reader < fiberCount; }

        // A COROUTINE SUSPENDED WHILE HOLDING A WORKER-CELL GUARD. See the definition.
        [[noreturn]] static void FatalSuspendWithGuard(std::size_t depth);

        // A SECOND worker-row guard while one is already live. See the definition.
        [[noreturn]] static void FatalNestedWorkerGuard();

        // Worker-cell guards live on THIS thread right now. Read by ArmResume at every coroutine
        // suspension point; incremented and decremented by HazardGuard.
        static std::size_t& SuspendUnsafeDepth() noexcept;

    private:

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
            // FAIL OUT LOUD, and never by overwriting cell 0. A silent slide onto another cell is
            // the same class of bug as worker-owned cells -- protection quietly moves off the node
            // it was guarding -- just relocated onto the fiber.
            //
            // Fatal in BOTH configurations, because the failure mode here is not "wrong on some
            // paths": returning a pointer this never published hands the caller something it
            // believes is protected and is not. A use-after-free with a confident comment above it
            // is strictly worse than a stop.
            //
            // THE COROUTINE CHECK COMES FIRST and is not an ordering nicety. cells is null for BOTH
            // a refused coroutine and an external thread that ran out of reserved slots; same
            // symptom, completely different fix, so they must not share a message. Set() makes the
            // same check for the same reason -- this is the second of the two entry points, not a
            // duplicate of the first.
            if (!cells || k >= HazardDomain::kCellsPerReader)
                HazardDomain::FatalCellOverflow(k, cells ? "cell index out of range"
                                                         : "no reader slot (external slots exhausted?)");
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

        // Set when this guard was refused because its task is a COROUTINE, which is a different
        // reason for null cells than "external reader slots exhausted" and gets a different message.
        // Set when this guard resolved to WORKER cells (native task, coroutine, or external thread)
        // rather than a fiber row, which is the case that must not span a suspension. See the ctor.
        bool countsForSuspend = false;
        // Set when this guard is using a coroutine RECORD rather than a table slot. RELEASED BY
        // ~HazardGuard: the guard is a local in the frame, so the language runs that on every path
        // the frame can end, including destroy() on a suspended one.
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
