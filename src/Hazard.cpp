// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Hazard.h"
#include "../include/TaskScheduler.h"
#include "../include/Thread.h"
#include "../include/GlobalFiberPool.h"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>

namespace JLib {

    namespace {
        // ONE BATCH PER THREAD, and that is sound where a per-thread PROTECT cell would not be.
        // Retiring is a point operation: it appends and maybe scans, and neither can suspend. It is
        // PROTECTION that spans a suspend, which is why cells are per-reader and this is not.
        struct RetireBatch {
            std::vector<std::pair<void*, void (*)(void*)>> items;
        };
        thread_local RetireBatch t_retire;

        std::uint64_t ThisThreadId() {
            // +1 so a valid id is never 0, which is the "unclaimed" marker for external readers.
            return static_cast<std::uint64_t>(
                       std::hash<std::thread::id>{}(std::this_thread::get_id())) | 1ull;
        }

        std::mutex g_tableInit;
        std::atomic<bool> g_forceWorkerCells{ false };

        // ---- the external-record registry ------------------------------------------------------
        //
        // Statically sized and NEVER deleted -- records cycle rather than being allocated and freed,
        // which is Michael's answer to the registry's own reclamation problem. Sizing to PEAK
        // CONCURRENT HP-using coroutines, not to total coroutines spawned, is what keeps that
        // bounded: a record is taken on first Protect and given back in ~promise_type.
        enum : std::uint32_t { REC_FREE = 0, REC_LIVE = 1, REC_RETIRED = 2 };

        struct alignas(64) ExtRecord {
            std::atomic<void*>         cells[HazardDomain::kCellsPerReader];
            std::atomic<std::uint32_t> state{ REC_FREE };
            std::atomic<std::uint32_t> gen{ 0 };
            std::atomic<std::uint64_t> retiredAtScan{ 0 };
        };
        ExtRecord g_records[HazardDomain::kMaxRecords];

        // Bumped at the START of every scan. A record retired at `c` may be reused only once a scan
        // that BEGAN after `c` has finished -- see the grace-period note in the header. This counts
        // SCANS, which run to completion; it never counts readers, which may park.
        std::atomic<std::uint64_t> g_scanSeq{ 1 };

    }

    HazardDomain& HazardDomain::Instance() {
        static HazardDomain d;
        return d;
    }

    void HazardDomain::EnsureTable() {
        if (cells.load(std::memory_order_acquire)) return;

        // REFUSE TO BUILD BEFORE THE POOL EXISTS. The table is sized from the fiber pool and the
        // worker count, and a worker reaches the sleep path -- which flushes the retire bag, which
        // lands here -- while Init is still bringing the pool up. Building then bakes in a table of
        // zero fibers and zero workers, and every later poolIndex is out of range.
        //
        // Left unbuilt instead: Scan() no-ops, Retire() keeps accumulating, and Init calls Init()
        // below once the workers are actually up.
        if (!TaskScheduler::IsInitialized()) return;

        std::lock_guard<std::mutex> lk(g_tableInit);
        if (cells.load(std::memory_order_relaxed)) return;

        // Sized from the fiber pool the same way Event's waiter table is, and for the same reason:
        // Fiber::poolIndex is dense and STABLE for the life of the program (the pool is leaked and
        // its vectors are reserve()d, so they never reallocate). That is what makes this a perfect
        // hash rather than a map, and what lets the scan reach a PARKED fiber -- its index still
        // addresses its cells whether or not it is running.
        const std::size_t f = TaskScheduler::IsInitialized()
                            ? TaskScheduler::Instance().GetGlobalPool().TotalCount()
                            : 0;
        const std::size_t w = TaskScheduler::IsInitialized()
                            ? TaskScheduler::Instance().GetWorkerCount()
                            : 0;

        fiberCount  = f;
        workerCount = w;
        readerCount = f + w + kExternalReaders;

        auto* fresh = new std::atomic<void*>[readerCount * kCellsPerReader];
        for (std::size_t i = 0; i < readerCount * kCellsPerReader; ++i)
            fresh[i].store(nullptr, std::memory_order_relaxed);

        auto* owners = new std::atomic<std::uint64_t>[kExternalReaders];
        for (std::size_t i = 0; i < kExternalReaders; ++i)
            owners[i].store(0, std::memory_order_relaxed);

        externalOwners = owners;
        cells.store(fresh, std::memory_order_release);
    }

    void HazardDomain::Init() { EnsureTable(); }

    std::size_t HazardDomain::CurrentReader() {
        EnsureTable();

        // Pre-Init, or Init still running. Cells stay null, and a HazardGuard built here will fatal
        // on its first Protect with a message naming the cause -- which is the right outcome, since
        // protecting a pointer before the domain exists cannot be honoured.
        if (!cells.load(std::memory_order_acquire)) return kNoReader;

        Thread* w = Thread::GetCurrent();
        if (w) {
            // A FIBER OWNS ITS CELLS, because the fiber is what migrates. This is the same argument
            // Fiber::localEpoch already records for EBR, and it is what closes bug 1.
            if (Fiber* fb = w->currentFiber; fb && !g_forceWorkerCells.load(std::memory_order_relaxed)) {
                assert(fb->poolIndex < fiberCount && "fiber poolIndex outside the hazard table");
                return fb->poolIndex;
            }
            // A COROUTINE ONLY REACHES HERE IF THE REGISTRY WAS EXHAUSTED. The guard takes a record
            // before ever calling this, so arriving with a Coroutine task means AcquireRecord
            // returned kNoRecord -- every one of kMaxRecords is LIVE or still inside its grace.
            //
            // Falling through to worker cells would be correct until this frame's first co_await and
            // catastrophically wrong after, so it does not: this asserts, and in Release the guard's
            // cells stay null and Protect is fatal. Raise kMaxRecords, or find the frames that take
            // a guard and never release it.
            assert((w->currentRunningTask == nullptr ||
                    w->currentRunningTask->type != TaskType::Coroutine) &&
                   "hazard: coroutine record registry exhausted (kMaxRecords). Worker cells are NOT "
                   "a fallback -- they stop protecting at the first co_await. See "
                   "design/hazard-pointers.md");
            if (w->currentRunningTask && w->currentRunningTask->type == TaskType::Coroutine)
                return kNoReader;

            // NATIVE TASK: worker-owned cells are correct here and only here. A native task runs to
            // completion on the stack it started on, so it cannot migrate inside a protected span.
            const std::size_t qi = static_cast<std::size_t>(w->qIndex);
            assert(qi < workerCount && "worker index outside the hazard table");
            return fiberCount + qi;
        }

        // EXTERNAL THREAD: main, or an app thread. Claim a reserved id and keep it -- never
        // released, which is the documented growth caveat.
        const std::uint64_t me = ThisThreadId();
        for (std::size_t i = 0; i < kExternalReaders; ++i) {
            std::uint64_t cur = externalOwners[i].load(std::memory_order_acquire);
            if (cur == me) return fiberCount + workerCount + i;
        }
        for (std::size_t i = 0; i < kExternalReaders; ++i) {
            std::uint64_t expected = 0;
            if (externalOwners[i].compare_exchange_strong(expected, me,
                                                          std::memory_order_acq_rel,
                                                          std::memory_order_acquire))
                return fiberCount + workerCount + i;
        }
        assert(false && "hazard: ran out of external reader slots (kExternalReaders)");
        return kNoReader;
    }

    void HazardDomain::FatalCellOverflow(std::size_t k, const char* what) {
        // Matches TaskDAG::CreateNode's handling of a Coroutine task: say what happened, name the
        // knob, then stop. A library that keeps going here is guessing on the caller's behalf about
        // memory it does not own.
        std::fprintf(stderr,
            "[JLib::Scheduler] FATAL: hazard cell %zu -- %s.\n"
            "  kCellsPerReader is %zu. Raise it with -DJLIBSCHED_HAZARD_CELLS=n, or restructure\n"
            "  the traversal to hold fewer pointers live at once. This is fatal rather than\n"
            "  ignored because continuing would return a pointer that was never published --\n"
            "  protected in the caller's belief and not in fact.\n",
            k, what, kCellsPerReader);
        std::fflush(stderr);
        std::abort();
    }

    std::atomic<void*>* HazardDomain::RecordCells(RecordId id) {
        return (id < kMaxRecords) ? g_records[id].cells : nullptr;
    }

    HazardDomain::RecordId HazardDomain::AcquireRecord() {
        const std::uint64_t now = g_scanSeq.load(std::memory_order_acquire);

        for (std::uint32_t i = 0; i < kMaxRecords; ++i) {
            ExtRecord& r = g_records[i];
            std::uint32_t st = r.state.load(std::memory_order_acquire);

            // RECLAIM ONLY PAST THE GRACE. `> retiredAt + 1` means a scan that began strictly after
            // the release has also finished, so no scanner can still be walking this block under
            // the previous owner's identity. Bumping the generation alone would not do: a scanner
            // reads the CELLS, not the record header.
            if (st == REC_RETIRED &&
                now > r.retiredAtScan.load(std::memory_order_acquire) + 1) {
                if (r.state.compare_exchange_strong(st, REC_FREE,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
                    r.gen.fetch_add(1, std::memory_order_relaxed);
                    st = REC_FREE;
                } else {
                    continue;
                }
            }

            if (st == REC_FREE) {
                std::uint32_t expected = REC_FREE;
                if (r.state.compare_exchange_strong(expected, REC_LIVE,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
                    // ZEROED BEFORE IT IS VISIBLE AS LIVE to anything that will publish into it --
                    // register must COMPLETE before the first store that names a pointer, or a scan
                    // reads a stale pointer from the previous owner and over-protects forever.
                    for (std::size_t k = 0; k < kCellsPerReader; ++k)
                        r.cells[k].store(nullptr, std::memory_order_relaxed);
                    std::atomic_thread_fence(std::memory_order_release);
                    return i;
                }
            }
        }
        return kNoRecord;   // registry exhausted; caller decides how loud to be
    }

    void HazardDomain::ReleaseRecord(RecordId id) {
        if (id >= kMaxRecords) return;
        ExtRecord& r = g_records[id];

        // ORDER: cells cleared BEFORE the state moves, so a scanner that still sees LIVE reads
        // nullptr rather than a pointer this frame no longer protects.
        for (std::size_t k = 0; k < kCellsPerReader; ++k)
            r.cells[k].store(nullptr, std::memory_order_release);

        // IDEMPOTENT BY CONSTRUCTION -- one CAS LIVE -> RETIRED. ~promise_type is the only caller
        // today, but the whole point of the state machine is that a second call from any discard
        // path is a no-op rather than a double release.
        std::uint32_t expected = REC_LIVE;
        if (r.state.compare_exchange_strong(expected, REC_RETIRED,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
            r.retiredAtScan.store(g_scanSeq.load(std::memory_order_acquire),
                                  std::memory_order_release);
        }
    }

    void HazardDomain::ForceWorkerCellsForTest(bool on) {
        g_forceWorkerCells.store(on, std::memory_order_relaxed);
    }

    std::atomic<void*>* HazardDomain::Cells(std::size_t reader) {
        EnsureTable();
        if (reader == kNoReader || reader >= readerCount) return nullptr;
        return cells.load(std::memory_order_acquire) + reader * kCellsPerReader;
    }

    void HazardDomain::Retire(void* p, void (*deleter)(void*)) {
        if (!p || !deleter) return;
        EnsureTable();
        t_retire.items.emplace_back(p, deleter);

        // R = 2 * total cells. The factor above one is what makes retire amortized O(1): each scan
        // is guaranteed to free at least half the batch, because at most `total cells` of them can
        // be named.
        const std::size_t threshold = 2 * readerCount * kCellsPerReader;
        if (t_retire.items.size() >= (threshold ? threshold : 64)) Scan();
    }

    void HazardDomain::Scan() {
        EnsureTable();
        if (t_retire.items.empty()) return;

        std::atomic<void*>* base = cells.load(std::memory_order_acquire);
        if (!base) return;

        // THE SYMMETRIC HALF OF Protect's FENCE. Protect stores its cell then re-reads the source;
        // this reads every cell after the retiring store that unlinked the node. Both sides need
        // seq_cst or the pair degenerates into "each saw the other's old value".
        std::atomic_thread_fence(std::memory_order_seq_cst);

        // THE SCAN SET IS THE WHOLE TABLE, NOT "WHO IS RUNNING". Walking running workers is bug 2 --
        // it frees under a parked fiber, whose cells are still live and still name their nodes.
        // Indexing by reader makes sleepers unavoidable rather than something to remember.
        // OPENS THE SCAN. Everything retired at or before this number must survive a full scan
        // boundary before its record can be reused -- see AcquireRecord. Counting scans and not
        // readers is what keeps a parked fiber from freezing record reuse.
        g_scanSeq.fetch_add(1, std::memory_order_acq_rel);

        std::vector<void*> named;
        named.reserve(readerCount * kCellsPerReader / 4 + 8);
        for (std::size_t i = 0; i < readerCount * kCellsPerReader; ++i)
            if (void* v = base[i].load(std::memory_order_acquire)) named.push_back(v);

        // THEN THE REGISTRY. RETIRED records are read too, not skipped: the release clears the cells
        // before moving the state, so a retired record contributes nothing -- but reading it costs
        // one load and removes any question about a release racing this walk. The cells are
        // domain-owned, so this can never touch memory a destroyed frame took with it.
        for (std::uint32_t i = 0; i < kMaxRecords; ++i) {
            if (g_records[i].state.load(std::memory_order_acquire) == REC_FREE) continue;
            for (std::size_t k = 0; k < kCellsPerReader; ++k)
                if (void* v = g_records[i].cells[k].load(std::memory_order_acquire))
                    named.push_back(v);
        }

        std::sort(named.begin(), named.end());

        std::vector<std::pair<void*, void (*)(void*)>> keep;
        keep.reserve(named.size());
        for (auto& it : t_retire.items) {
            if (std::binary_search(named.begin(), named.end(), it.first)) keep.push_back(it);
            else it.second(it.first);
        }
        t_retire.items.swap(keep);
    }

    std::size_t HazardDomain::PendingRetired() const { return t_retire.items.size(); }

    // ---- HazardGuard ---------------------------------------------------------------------------

    HazardGuard::HazardGuard() {
        HazardDomain& d = HazardDomain::Instance();

        // A COROUTINE TAKES A RECORD, NOT THE WORKER'S CELLS -- and THE GUARD OWNS IT.
        //
        // This is where the design got simpler than planned. The first attempt kept the RecordId in
        // the promise and had the C++20 resume trampoline hand the core a pointer to that field.
        // That was unsound: ResumeCoroutine is generic BY DESIGN, because Task::data holds whichever
        // frame was last armed -- for a nested Lazy that is the parent, not the Coro -- so typing
        // the handle to reach `promise().hazardRecord` reads a different promise's storage. It also
        // required remembering every discard path that skips final_suspend.
        //
        // A HazardGuard is a LOCAL IN THE FRAME, so it already survives the co_await, and destroying
        // a suspended coroutine runs the destructors of in-scope locals. Owning the record here
        // gets "released on every path" from the language rather than from a list of call sites --
        // which is the same class of bug as the PushBatch stamp, closed by construction instead of
        // by vigilance.
        //
        // Detected the same way the old tripwire detected it: a Coroutine task with no fiber.
        if (Thread* w = Thread::GetCurrent()) {
            if (TaskScheduler::CurrentTaskType() == TaskType::Coroutine) {
                record = d.AcquireRecord();
                if (record != HazardDomain::kNoRecord) {
                    cells  = d.RecordCells(record);
                    reader = HazardDomain::kNoReader;
                    return;
                }
                // Registry exhausted. Deliberately NOT falling back to worker cells -- that is the
                // exact bug this exists to prevent, and Protect's fatal beats a silent downgrade.
            }
        }

        reader = d.CurrentReader();
        cells  = d.Cells(reader);
    }

    HazardGuard::~HazardGuard() {
        // CLEARS THE CELLS IT PUBLISHED, wherever this is now running. The cells belong to the
        // reader, not to the worker that happens to be executing it, so a resume on a different
        // worker still releases the right ones -- which is exactly what a worker-owned design
        // cannot do.
        if (!cells) return;
        for (std::size_t k = 0; k < HazardDomain::kCellsPerReader; ++k)
            cells[k].store(nullptr, std::memory_order_release);

        // A coroutine's record goes back HERE, and this destructor runs on every path a frame can
        // end -- normal return, an exception, or destroy() on a suspended frame, which unwinds
        // in-scope locals. That is the "same unregister on every discard path" requirement, obtained
        // from the language instead of from remembering the fifth one.
        if (record != HazardDomain::kNoRecord) {
            HazardDomain::Instance().ReleaseRecord(record);
            record = HazardDomain::kNoRecord;
        }
    }

    void HazardGuard::Set(std::size_t k, void* p) {
        assert(k < HazardDomain::kCellsPerReader && "hazard cell index out of range");
        if (!cells || k >= HazardDomain::kCellsPerReader) return;
        cells[k].store(p, std::memory_order_release);
    }

    void HazardGuard::Clear(std::size_t k) { Set(k, nullptr); }

    void HazardGuard::Swap(std::size_t a, std::size_t b) {
        assert(a < HazardDomain::kCellsPerReader && b < HazardDomain::kCellsPerReader);
        if (!cells) return;
        void* va = cells[a].load(std::memory_order_relaxed);
        void* vb = cells[b].load(std::memory_order_relaxed);
        cells[a].store(vb, std::memory_order_release);
        cells[b].store(va, std::memory_order_release);
    }

} // namespace JLib
