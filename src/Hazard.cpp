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
        //
        // BUT A THREAD-LOCAL BAG DIES WITH ITS THREAD, and that is a leak with a deleter attached.
        // Retire takes a DELETER over the CALLER'S memory, so a bag abandoned at thread exit does
        // not merely delay reclamation -- those objects are never freed and their destructors never
        // run. Workers exit at Join and app threads come and go, so this is reachable, not
        // theoretical: retire a few nodes on a thread, let the thread end, and they are gone.
        //
        // THE HANDOFF IS A DESTRUCTOR, not a call somewhere in the thread's exit path. Same reason
        // HazardGuard owns its record: "released on every path a thread can end" is a property the
        // language will enforce and a list of call sites will not. Every leftover moves to a global
        // orphan store that any later Scan sweeps.
        struct RetireBatch {
            std::vector<std::pair<void*, void (*)(void*)>> items;
            ~RetireBatch();
        };
        thread_local RetireBatch t_retire;

        // SET BY ~RetireBatch, and it must outlive the batch. A bool with constant initialization
        // has no destructor, so it is still readable while OTHER thread_locals are being destroyed
        // -- and one of those may itself Retire, which would otherwise touch a dead vector. After
        // the flag is up, Retire goes straight to the orphan store.
        thread_local bool t_retireDead = false;

        // LEAKED ON PURPOSE. A thread_local destructor may run during
        // process teardown, and a function-local static with a destructor could already be gone by
        // then -- handing objects to a destroyed vector is a worse bug than the one being fixed.
        // Nothing here is large: it holds only what abandoned threads left behind, which is zero in
        // a healthy process.
        struct OrphanStore {
            std::mutex mtx;
            std::vector<std::pair<void*, void (*)(void*)>> items;
        };
        OrphanStore* const g_orphans = new OrphanStore();

        // THE GATE, and the reason Scan does not pay a mutex. Scan runs on the retire threshold and
        // again before every park, so it is warm; the orphan sweep is for a case that never happens
        // in a healthy run. One relaxed load of a line that stays 0 keeps the cost at zero until
        // something is actually stranded.
        std::atomic<std::size_t> g_orphanCount{ 0 };

        // Cumulative, so a test can ask "did anything EVER get stranded" after the sweep has
        // already emptied the store. g_orphanCount goes back to zero; this does not.
        std::atomic<std::size_t> g_orphanTotal{ 0 };

        void AdoptOrphans(std::vector<std::pair<void*, void (*)(void*)>>& from) {
            if (from.empty()) return;
            std::lock_guard<std::mutex> lk(g_orphans->mtx);
            for (auto& it : from) g_orphans->items.push_back(it);
            g_orphanTotal.fetch_add(from.size(), std::memory_order_relaxed);
            g_orphanCount.store(g_orphans->items.size(), std::memory_order_release);
            from.clear();
        }

        RetireBatch::~RetireBatch() {
            t_retireDead = true;
            AdoptOrphans(items);
        }


        std::uint64_t ThisThreadId() {
            // +1 so a valid id is never 0, which is the "unclaimed" marker for external readers.
            return static_cast<std::uint64_t>(
                       std::hash<std::thread::id>{}(std::this_thread::get_id())) | 1ull;
        }

        std::mutex g_tableInit;
        std::atomic<bool> g_forceWorkerCells{ false };

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
            // A COROUTINE LANDS HERE ON PURPOSE, and gets the worker's cells. That is correct
            // exactly as long as the frame does not suspend while holding the guard -- and it is
            // ArmResume, at the suspension point, that enforces it. See FatalSuspendWithGuard.
            //
            // The refusal that used to sit here was broader than the hazard: a coroutine that never
            // suspends inside the guarded span is indistinguishable from a synchronous call, and
            // forbidding it also forbade the standard pattern (hazptr the lookup, take a refcount,
            // drop the guard, then co_await).

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
        // NOT AN assert, WHICH IS WHAT THIS WAS. It evaporates under /O2, and returning kNoReader in
        // Release leaves the guard with null cells -- so the first Protect is fatal ANYWAY, just one
        // step later and with a message blaming the wrong thing ("no reader slot") instead of naming
        // kExternalReaders. Same failure, worse diagnosis, and only in the build nobody is watching.
        FatalCellOverflow(0, "ran out of external reader slots -- raise kExternalReaders");
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

    // PER-THREAD, NOT PER-READER, and that is correct because it is only ever read by the thread
    // that would suspend: ArmResume runs on the same worker that constructed the guard, since no
    // suspension has happened yet. Fiber rows are excluded at construction, so a parked fiber's
    // guard never appears here.
    std::size_t& HazardDomain::SuspendUnsafeDepth() noexcept {
        static thread_local std::size_t d = 0;
        return d;
    }

    // A COROUTINE TRIED TO SUSPEND WHILE HOLDING A WORKER-CELL GUARD. Caught at the transition,
    // before the frame can be re-pushed and picked up by a different worker -- which is the moment
    // the cells stop meaning what the guard thinks they mean.
    //
    // ALWAYS ON, NOT DEBUG-ONLY, and the cost is the reason. This is one thread-local load at a
    // point that already pays a re-push and a wake, so leaving it out of Release buys nothing and
    // would make a violation a use-after-free in exactly the build nobody is watching. Epochs had to
    // buy their way out of that with a whole counted scheme; here it is free, so it stays.
    void HazardDomain::FatalSuspendWithGuard(std::size_t depth) {
        std::fprintf(stderr,
            "[JLib::Scheduler] FATAL: a coroutine suspended while holding %zu hazard guard(s).\n"
            "  The guard resolved to the WORKER's cells, which are correct only while the frame\n"
            "  stays on that worker. Resuming elsewhere leaves the announcement behind and the\n"
            "  protection silently stops -- a use-after-free with a confident comment above it.\n"
            "  THE FIX IS THE STANDARD PATTERN. Hazard pointers protect the LOOKUP; a refcount\n"
            "  protects the await. Hand off inside the guarded span, then drop the guard:\n"
            "\n"
            "      Node* safe = nullptr;\n"
            "      {\n"
            "          JLib::HazardGuard g;                       // synchronous, no suspend inside\n"
            "          Node* raw = g.Protect(0, head);\n"
            "          if (raw && raw->TryAcquireRef()) safe = raw;\n"
            "      }                                              // guard released HERE\n"
            "      if (safe) {\n"
            "          co_await safe->AsyncWork();                // free to migrate now\n"
            "          safe->ReleaseRef();\n"
            "      }\n"
            "\n"
            "  A guard must not span a suspension point. Nothing else about coroutines is\n"
            "  restricted -- one that never suspends inside the guarded span is fine, and this\n"
            "  check never fires for it, because await_ready() == true never reaches ArmResume.\n"
            "  A FIBER may hold a guard across a park: its cells are indexed by the fiber, not by\n"
            "  the worker, so this rule is about coroutines only.\n", depth);
        std::abort();
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
        // THE BAG MAY ALREADY BE GONE. A thread_local destructor running earlier in this thread's exit
        // can Retire -- freeing a structure whose nodes were still protected is exactly the kind of
        // thing a destructor does -- and t_retire.items would then be a destroyed vector. Straight to
        // the orphan store instead, where the next Scan on any thread will reclaim it.
        if (t_retireDead) {
            std::vector<std::pair<void*, void (*)(void*)>> one{ { p, deleter } };
            AdoptOrphans(one);
            return;
        }
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
        // BOTH must be empty to skip. Checking only the local bag made the orphan sweep unreachable
        // from the very thread that needs to run it: a thread with nothing of its own to reclaim
        // returned before ever looking at what an exited thread left. Caught by the test, which
        // failed on "a Scan on ANOTHER thread reclaimed all of them".
        if (t_retire.items.empty() && g_orphanCount.load(std::memory_order_acquire) == 0) return;

        std::atomic<void*>* base = cells.load(std::memory_order_acquire);
        if (!base) return;

        // THE SYMMETRIC HALF OF Protect's FENCE. Protect stores its cell then re-reads the source;
        // this reads every cell after the retiring store that unlinked the node. Both sides need
        // seq_cst or the pair degenerates into "each saw the other's old value".
        std::atomic_thread_fence(std::memory_order_seq_cst);

        // THE SCAN SET IS THE WHOLE TABLE, NOT "WHO IS RUNNING". Walking running workers is bug 2 --
        // it frees under a parked fiber, whose cells are still live and still name their nodes.
        // Indexing by reader makes sleepers unavoidable rather than something to remember.
        // AND THE TABLE IS ALL OF IT. Every reader -- fiber, worker, external thread -- has a dense
        // stable index, so this one walk is the entire scan set and there is no second lifecycle to
        // reason about. See experimental/hazard/README.md for what used to be here.
        std::vector<void*> named;
        named.reserve(readerCount * kCellsPerReader / 4 + 8);
        for (std::size_t i = 0; i < readerCount * kCellsPerReader; ++i)
            if (void* v = base[i].load(std::memory_order_acquire)) named.push_back(v);

        std::sort(named.begin(), named.end());

        std::vector<std::pair<void*, void (*)(void*)>> keep;
        keep.reserve(named.size());
        for (auto& it : t_retire.items) {
            if (std::binary_search(named.begin(), named.end(), it.first)) keep.push_back(it);
            else it.second(it.first);
        }
        t_retire.items.swap(keep);

        // SWEEP WHAT ABANDONED THREADS LEFT, using the scan set already computed above -- the same
        // safety test, so an orphan is freed under exactly the conditions its own thread would have
        // required. Nothing is freed early: an object still named by a live reader stays in the
        // store and waits for the next scan.
        //
        // GATED, because Scan is warm -- retire threshold and every park -- while this case is
        // empty in every healthy run. One relaxed load, and the mutex is only ever taken when
        // something is genuinely stranded.
        if (g_orphanCount.load(std::memory_order_acquire) != 0) {
            std::lock_guard<std::mutex> lk(g_orphans->mtx);
            std::vector<std::pair<void*, void (*)(void*)>> stillNamed;
            stillNamed.reserve(g_orphans->items.size());
            for (auto& it : g_orphans->items) {
                if (std::binary_search(named.begin(), named.end(), it.first)) stillNamed.push_back(it);
                else it.second(it.first);
            }
            g_orphans->items.swap(stillNamed);
            g_orphanCount.store(g_orphans->items.size(), std::memory_order_release);
        }
    }

    std::size_t HazardDomain::PendingRetired() const { return t_retire.items.size(); }

    // Waiting in the orphan store right now -- zero in a healthy run, and back to zero once a Scan
    // sweeps it. Use OrphanedTotal to ask whether stranding ever HAPPENED.
    std::size_t HazardDomain::OrphanedRetired() const {
        return g_orphanCount.load(std::memory_order_acquire);
    }

    // Cumulative and never decremented, so it survives the sweep that empties the store. A test
    // that only checked OrphanedRetired would pass whether or not the handoff ever ran.
    std::size_t HazardDomain::OrphanedTotal() const {
        return g_orphanTotal.load(std::memory_order_relaxed);
    }

    // ---- HazardGuard ---------------------------------------------------------------------------

    HazardGuard::HazardGuard() {
        HazardDomain& d = HazardDomain::Instance();

        // A COROUTINE MAY TAKE A GUARD, AND MAY NOT SUSPEND WHILE HOLDING ONE. Those are different
        // rules, and separating them is what lets the standard production pattern work here:
        //
        //     hazptr the lookup -> take a refcount -> DROP the guard -> co_await freely
        //
        // The blanket refusal this replaced forbade that too. A coroutine that never suspends inside
        // the guarded span is indistinguishable from a synchronous call: it stays on the worker that
        // resolved its cells, and worker cells are exactly right for it.
        //
        // ENFORCED AT THE SUSPENSION, NOT AT CONSTRUCTION, because that is where the fatal condition
        // actually materialises. detail::ArmResume is the single choke point -- every awaiter in
        // Coroutine.h and IoAsync.h routes through it, since that call is how a suspended frame gets
        // the Task the scheduler resumes it by -- so a coroutine cannot suspend through this runtime
        // without passing the check. await_ready() == true never reaches it, which is why a
        // non-suspending coroutine costs nothing and raises no false alarm.
        reader = d.CurrentReader();
        cells  = d.Cells(reader);

        // COUNT ONLY WORKER-CELL GUARDS. A parked FIBER holding a guard is legal and is the whole
        // point of indexing cells by the thing that migrates, so a fiber row must not contribute --
        // otherwise the next coroutine to suspend on that worker trips an alarm it did not cause.
        countsForSuspend = (reader != HazardDomain::kNoReader) && !d.IsFiberReader(reader);
        if (countsForSuspend) ++HazardDomain::SuspendUnsafeDepth();
    }

    HazardGuard::~HazardGuard() {
        // CLEARS THE CELLS IT PUBLISHED, wherever this is now running. The cells belong to the
        // reader, not to the worker that happens to be executing it, so a resume on a different
        // worker still releases the right ones -- which is exactly what a worker-owned design
        // cannot do.
        // DECREMENT FIRST, and unconditionally -- before the early return below, or a guard with no
        // cells (external slots exhausted) would leak depth and every later suspension on this
        // thread would false-alarm.
        if (countsForSuspend) {
            countsForSuspend = false;
            --HazardDomain::SuspendUnsafeDepth();
        }

        if (!cells) return;
        for (std::size_t k = 0; k < HazardDomain::kCellsPerReader; ++k)
            cells[k].store(nullptr, std::memory_order_release);

    }

    void HazardGuard::Set(std::size_t k, void* p) {
        // WHY cells IS NULL DECIDES WHICH MESSAGE. A coroutine was refused a reader on purpose; an
        // external thread ran out of reserved slots. Same symptom, different fix, so they must not
        // share a diagnostic.
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
