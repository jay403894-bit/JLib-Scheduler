// FIBER-LOCAL STORAGE SURVIVES A MIGRATION. THREAD-LOCAL DOES NOT.
//
// That sentence is the entire reason the slots exist, and this file is the proof of both halves.
// Migratable fibers resume on whichever worker is free, so a `thread_local` written before a
// suspension point belongs to a thread the fiber may no longer be on. Nothing catches it -- you get
// the resuming worker's copy, which is a plausible-looking value rather than a crash.
//
// THE TLS ARM IS THE NEGATIVE CONTROL AND IT IS NOT DECORATION. Without it, arm 1 passing would be
// consistent with "nothing migrated at all", which is the most likely way for this file to lie. The
// two arms are written into the SAME fiber across the SAME suspension, so they cannot disagree about
// whether a migration happened -- only about what survived it.
//
// THE TLS ACCESSOR IS noinline ON PURPOSE. MSVC caches a thread_local's base address across an
// opaque call, so an inlined read after a context switch can return the OLD thread's slot from a
// register and make TLS look like it migrated correctly. That is not a hypothesis: it is what made
// migratable_fiber_test report 0 migrations when there were 206. A real call forces a real reload,
// so this measures the RUNTIME rather than the optimiser.
//
// ARM 3 IS THE RECYCLE SCRUB, which is the failure that would look like working code. A pooled fiber
// handed to a new task with a previous occupant's pointer still in a slot reads perfectly well --
// the pointee is usually still allocated -- so it corrupts quietly instead of faulting.

#include "TaskScheduler.h"
#include "Thread.h"
#include "Fiber.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-72s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

enum class Fls : uint16_t { Sentinel = 0, COUNT };
static_assert((size_t)Fls::COUNT <= JLib::Fiber::kLocalSlots, "too many FLS slots");

// The control's storage. noinline both ways -- see the header note.
static thread_local void* t_tls = nullptr;
#if defined(_MSC_VER)
#  define JLIB_NOINLINE __declspec(noinline)
#else
#  define JLIB_NOINLINE __attribute__((noinline))
#endif
JLIB_NOINLINE static void  TlsSet(void* v) { t_tls = v; }
JLIB_NOINLINE static void* TlsGet()        { return t_tls; }

static constexpr int kFibers = 64;

struct Rec {
    int   workerBefore = -1;
    int   workerAfter  = -1;
    void* flsAfter     = nullptr;
    void* tlsAfter     = nullptr;
    void* sentinel     = nullptr;
    bool  done         = false;
};

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== fiber-local storage survives migration; thread_local does not ===\n");

    // Explicit rather than relying on the default, so this file states the mode it is about.
    JLib::TaskScheduler::SetMigratableFibers(true);
    JLib::TaskScheduler::Init(8);
    auto& sched = JLib::TaskScheduler::Instance();

    Check(JLib::TaskScheduler::MigratableFibers(), "migratable mode is on (else nothing can migrate)");

    std::vector<Rec> recs(kFibers);
    std::atomic<int> started{ 0 }, finished{ 0 };
    JLib::Event& gate = sched.GetEvent("fiber_local_gate");

    for (int i = 0; i < kFibers; ++i) {
        Rec* r = &recs[i];
        r->sentinel = (void*)(uintptr_t)(0xF1BE0000u + (unsigned)i);

        auto* t = sched.CreateTask([r, &gate, &started, &finished] {
            JLib::Thread* th = JLib::Thread::GetCurrent();
            r->workerBefore = th ? th->qIndex : -1;

            // Both stores happen HERE, on the pre-suspend worker.
            JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) = r->sentinel;
            TlsSet(r->sentinel);

            started.fetch_add(1, std::memory_order_release);
            JLib::TaskScheduler::Instance().WaitOnEvent(gate);

            // ...and both loads happen here, wherever we resumed.
            JLib::Thread* th2 = JLib::Thread::GetCurrent();
            r->workerAfter = th2 ? th2->qIndex : -1;
            r->flsAfter    = JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel);
            r->tlsAfter    = TlsGet();
            r->done        = true;
            finished.fetch_add(1, std::memory_order_release);
        }, JLib::Lane::Normal, JLib::TaskType::Fiber);
        if (t) sched.Push(t);
    }

    // Everyone parked before anyone is released, so the resume storm has the whole pool to land on
    // and migration is likely rather than incidental.
    const auto d1 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (started.load(std::memory_order_acquire) < kFibers
           && std::chrono::steady_clock::now() < d1)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    Check(started.load(std::memory_order_acquire) == kFibers, "every fiber parked");

    gate.SignalAll();

    const auto d2 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (finished.load(std::memory_order_acquire) < kFibers
           && std::chrono::steady_clock::now() < d2)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    Check(finished.load(std::memory_order_acquire) == kFibers, "every fiber resumed");

    int migrated = 0, flsHeld = 0, flsLost = 0, tlsHeldAcrossMigration = 0;
    for (const Rec& r : recs) {
        if (!r.done) continue;
        const bool moved = (r.workerBefore != r.workerAfter);
        if (moved) ++migrated;
        if (r.flsAfter == r.sentinel) ++flsHeld; else ++flsLost;
        if (moved && r.tlsAfter == r.sentinel) ++tlsHeldAcrossMigration;
    }
    std::printf("  %d/%d fibers resumed on a DIFFERENT worker\n", migrated, kFibers);
    std::printf("  FLS: %d held the sentinel, %d lost it\n", flsHeld, flsLost);
    std::printf("  TLS: %d of %d migrated fibers still saw their own value\n",
                tlsHeldAcrossMigration, migrated);

    // VACUITY GUARD FIRST. If nothing migrated, arm 1 is a statement about an empty set and the
    // control cannot distinguish anything either.
    Check(migrated > 0,
          "at least one fiber MIGRATED (else this file proves nothing about either storage)");

    Check(flsLost == 0, "FIBER-LOCAL survived every resume, migrated or not");

    // The control. TLS belongs to the THREAD, so a fiber that moved should NOT still see the value
    // it wrote on the worker it left. Stated as "not all of them" rather than "none": a migrated
    // fiber can legitimately still read its own value if it happened to land on a worker that ran
    // one of ITS earlier siblings... which cannot happen here, since each fiber writes a sentinel
    // unique to itself. So any survivor at all means the read did not really cross a thread.
    Check(migrated == 0 || tlsHeldAcrossMigration == 0,
          "CONTROL: thread_local did NOT survive a migration (else the arms cannot be told apart)");

    // ---- ARM 3: A RECYCLED FIBER MUST NOT CARRY THE LAST OCCUPANT'S SLOTS ------------------
    //
    // The quiet one. A stale slot points at memory that is usually still allocated, so it reads back
    // as a plausible value instead of faulting.
    {
        std::atomic<int> ran{ 0 };
        std::atomic<int> dirty{ 0 };
        const int kRounds = 200;

        for (int i = 0; i < kRounds; ++i) {
            auto* t = sched.CreateTask([&ran, &dirty] {
                if (JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) != nullptr)
                    dirty.fetch_add(1, std::memory_order_relaxed);
                // Leave a value behind for whoever gets this fiber next.
                JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) = (void*)(uintptr_t)0xDEAD;
                ran.fetch_add(1, std::memory_order_release);
            }, JLib::Lane::Normal, JLib::TaskType::Fiber);
            if (t) sched.Push(t);
        }

        const auto d3 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (ran.load(std::memory_order_acquire) < kRounds
               && std::chrono::steady_clock::now() < d3)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        std::printf("  recycle: %d/%d tasks ran, %d saw a stale slot\n",
                    ran.load(), kRounds, dirty.load());
        Check(ran.load(std::memory_order_acquire) == kRounds, "every recycle-probe task ran");
        Check(dirty.load(std::memory_order_relaxed) == 0,
              "a recycled fiber NEVER carries the previous occupant's slot");
    }

    // ---- OFF A FIBER: answerable, not fatal ------------------------------------------------
    Check(!JLib::TaskScheduler::HasFiberLocal(), "main reports NO fiber-local storage");
    Check(JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) == nullptr,
          "and reading a slot off a fiber gives nullptr rather than crashing");

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_fail ? "FAILED" : "PASSED", g_fail, g_fail == 1 ? "" : "s");
    return g_fail ? 1 : 0;
}
