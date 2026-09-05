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
#include "fiber_body.h"
#include "Thread.h"
#include "Fiber.h"
#include "FiberRegistry.h"
#include <cstdlib>

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

// THE WORKER-INDEX READ IS noinline FOR THE SAME REASON THE TLS ACCESSOR ABOVE IS. The bodies read
// it either side of the wait and compare the two; with whole-program optimisation the two
// Thread::GetCurrent() calls fold into one, so workerAfter can only ever equal workerBefore and
// this file reports "0 of 64 migrated" on a run where the non-WPO build of the same source reports
// 45-52. A migration count that depends on the optimiser is measuring the compiler.
//
// THIS HID A REAL BUG BEHIND ITS OWN VACUITY GUARD. The zero tripped "at least one fiber MIGRATED"
// first, which reads as "the scenario did not happen" rather than "the instrument folded" -- while
// the genuine failure underneath it, fiber-local storage resolving through a stale Thread*, was a
// separate defect in the library that the same folding also caused.
JLIB_NOINLINE static int WorkerIndexNow() {
    JLib::Thread* t = JLib::Thread::GetCurrent();
    return t ? t->qIndex : -1;
}

static constexpr int kFibers = 64;

// Arm 4 slots. FILE SCOPE so the task lambdas can name them without an explicit capture, and
// deliberately slots NO EARLIER ARM TOUCHES -- see arm 4.
static constexpr size_t kOwning   = 2;
static constexpr size_t kBorrowed = 3;
static_assert(kOwning < JLib::Fiber::kLocalSlots && kBorrowed < JLib::Fiber::kLocalSlots, "");

struct Rec {
    int   workerBefore = -1;
    int   workerAfter  = -1;
    void* flsAfter     = nullptr;
    void* tlsAfter     = nullptr;
    void* sentinel     = nullptr;
    bool  done         = false;
};

// ---- FIBER BODIES: NAMED FUNCTIONS, EXPLICIT CONTEXTS ---------------------------------------
//
// One spelling for every task body -- not a lambda, and not a captureless one either, so there is
// never a per-site judgement about which form is safe here. A fiber's stack is a PLACE: register
// state mapped to memory, kept intact across a suspension. A closure is a VALUE the worker loop
// frees the instant the body returns. A fiber handed a closure that then parks either resumes into
// a dead frame or never dies and never returns its row, and because SlabPool is append-only and a
// released slot stays mapped holding its old bytes, neither shows up where it was caused.
// ---- STATE THE BODIES NEED, HOISTED TO FILE SCOPE ------------------------------------------
//
// Owned/Alloc/s_live and the typed FiberLocal handle were declared inside main. They are up here
// now for one reason: a named body cannot see a function-local type, and named bodies are the
// rule. They are still test-private (static / internal linkage) and nothing outside this file
// can reach them.
static std::atomic<int> s_freed{ 0 };
static std::atomic<int> s_borrowedFreed{ 0 };
struct Owned { int magic; };

// Deliberately not `new`/`delete`: the whole reason the hook takes a void* is that the allocator
// is the caller's business.
struct Alloc {
    static void* Get()                { return std::malloc(sizeof(Owned)); }
    static void  Free(void* p)        { s_freed.fetch_add(1, std::memory_order_relaxed); std::free(p); }
    static void  NeverCalled(void*)   { s_borrowedFreed.fetch_add(1, std::memory_order_relaxed); }
};
static Owned s_live{ 0xABCD };

struct Scratch { int magic; };
static JLib::FiberLocal<Scratch> g_tls = JLib::MakeFiberLocal<Scratch>();

struct MigrateCtx {
    Rec*              rec;
    JLib::Event*      gate;
    std::atomic<int>* started;
    std::atomic<int>* finished;
};
static void MigrateBody(void* p) {
    auto& c = *static_cast<MigrateCtx*>(p);
    Rec* r = c.rec;
    r->workerBefore = WorkerIndexNow();

    // Both stores happen HERE, on the pre-suspend worker.
    JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) = r->sentinel;
    TlsSet(r->sentinel);

    c.started->fetch_add(1, std::memory_order_release);
    JLib::TaskScheduler::Instance().WaitOnEvent(*c.gate);

    // ...and both loads happen here, wherever we resumed.
    r->workerAfter = WorkerIndexNow();
    r->flsAfter    = JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel);
    r->tlsAfter    = TlsGet();
    r->done        = true;
    c.finished->fetch_add(1, std::memory_order_release);
}

struct ScrubCtx { std::atomic<int>* ran; std::atomic<int>* flagged; };
// The BORROWED-slot arm. Writes an owning pointer into one slot and a pointer to a live object
// into another that has no deleter, then checks the next occupant of this fiber sees neither.
static void BorrowSlotBody(void* p) {
    auto& c = *static_cast<ScrubCtx*>(p);
    if (JLib::TaskScheduler::FiberLocal(kOwning) != nullptr ||
        JLib::TaskScheduler::FiberLocal(kBorrowed) != nullptr)
        c.flagged->fetch_add(1, std::memory_order_relaxed);
    JLib::TaskScheduler::FiberLocal(kOwning)   = Alloc::Get();   // owning
    JLib::TaskScheduler::FiberLocal(kBorrowed) = &s_live;        // BORROWED
    c.ran->fetch_add(1, std::memory_order_release);
}

// The typed FiberLocal<T> arm. Every slot it reports into arrives as a pointer, so the body names
// exactly what it touches.
struct TypedCtx {
    Scratch*          obj;
    int*              idBefore;
    int*              idAfter;
    int*              wBefore;
    int*              wAfter;
    int*              gotMagic;
    JLib::Event*      gate;
    std::atomic<int>* started;
    std::atomic<int>* done;
};
static void TypedFlsBody(void* p) {
    auto& c = *static_cast<TypedCtx*>(p);
    *c.wBefore  = WorkerIndexNow();
    *c.idBefore = (int)JLib::FiberRegistry::GetID();

    g_tls.set(c.obj);
    c.started->fetch_add(1, std::memory_order_release);
    JLib::TaskScheduler::Instance().WaitOnEvent(*c.gate);

    *c.wAfter  = WorkerIndexNow();
    *c.idAfter = (int)JLib::FiberRegistry::GetID();
    *c.gotMagic = g_tls ? g_tls->magic : -1;    // operator bool + operator->
    c.done->fetch_add(1, std::memory_order_release);
}

static void ScrubSlotBody(void* p) {
    auto& c = *static_cast<ScrubCtx*>(p);
    if (JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) != nullptr)
        c.flagged->fetch_add(1, std::memory_order_relaxed);
    // Leave a value behind for whoever gets this fiber next.
    JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) = (void*)(uintptr_t)0xDEAD;
    c.ran->fetch_add(1, std::memory_order_release);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== fiber-local storage survives migration; thread_local does not ===\n");

    // Explicit rather than relying on the default, so this file states the mode it is about.
    JLib::TaskScheduler::SetFiberMode(JLib::FiberMode::Migrate);
    JLib::TaskScheduler::Init(8);
    auto& sched = JLib::TaskScheduler::Instance();

    Check(JLib::TaskScheduler::FibersMigrate(), "migratable mode is on (else nothing can migrate)");

    std::vector<Rec> recs(kFibers);
    std::atomic<int> started{ 0 }, finished{ 0 };
    JLib::Event& gate = sched.GetEvent("fiber_local_gate");

    // PER-ITERATION CAPTURE (`r` differs each time), so the bodies get storage that outlives the
    // loop and the wait. reserve() is load-bearing: MakeCtxTask is handed &ctxs[i], and a
    // reallocation would move contexts already handed out from under their fibers.
    std::vector<MigrateCtx> ctxs;
    ctxs.reserve(kFibers);

    for (int i = 0; i < kFibers; ++i) {
        Rec* r = &recs[i];
        r->sentinel = (void*)(uintptr_t)(0xF1BE0000u + (unsigned)i);

        ctxs.push_back(MigrateCtx{ r, &gate, &started, &finished });
        auto* t = JLibTest::MakeCtxTask(sched, &MigrateBody, &ctxs[i]);
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

        // Outside the loop: the task points at this body, and nothing is joined per iteration.
        ScrubCtx sctx{ &ran, &dirty };
        for (int i = 0; i < kRounds; ++i) {
            auto* t = JLibTest::MakeCtxTask(sched, &ScrubSlotBody, &sctx);
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

    // ---- ARM 4 IS GONE: SLOT DELETERS WERE REMOVED ------------------------------------------
    //
    // It tested FiberRegistry::SetSlotDeleter -- an owning slot freed at fiber death, with a
    // BORROWED slot as the control that must never be touched. The control was the good part and
    // the feature was not: a slot deleter is a deferred free that fires at a moment when freeing
    // was already safe, which is a third reclamation scheme on top of epochs and hazards, earning
    // nothing either of those does not already do. The API went, so its arm goes with it.
    //
    // Nothing here replaces it, deliberately. The rule the library keeps is the one arm 1 checks:
    // a slot is a void*, the library never frees what one points at, and clearing on recycle
    // prevents a stale READ rather than a leak.

    // ---- ARM 5: FiberLocal<T> AND GetID(), THE TYPED FACE ----------------------------------
    //
    // The raw slot API is what the library uses; this is what an application would. The test that
    // matters is the same one as arm 1 -- a value set before a suspend must still be there after,
    // wherever it resumed -- because that is the entire reason to prefer this over thread_local.
    //
    // GetID IS CHECKED FOR STABILITY ACROSS THE SUSPEND, not just for being non-null. A fiber's
    // registry identity is what every table indexes by, so an id that changed under a migration
    // would silently mis-address the cleanup chain and the hazard cells both.
    {
        Check(g_tls.slot != JLib::FiberRegistry::kNoSlot, "FlsAlloc handed out a slot");

        // A SECOND ALLOCATION MUST DIFFER. One counter handing the same index to two callers is the
        // failure that would make two unrelated subsystems silently share storage.
        auto other = JLib::MakeFiberLocal<Scratch>();
        Check(other.slot != g_tls.slot, "a second FlsAlloc returned a DIFFERENT slot");

        constexpr int kN = 48;
        std::vector<Scratch> objs(kN);
        std::vector<int>  idBefore(kN, -1), idAfter(kN, -1);
        std::vector<int>  wBefore(kN, -1),  wAfter(kN, -1);
        std::vector<int>  gotMagic(kN, -1);
        std::atomic<int> started{ 0 }, done{ 0 };
        JLib::Event& gate2 = sched.GetEvent("fiber_local_typed_gate");

        // Per-iteration state again -- same pattern, same reason. See the first loop in this file:
        // one context per fiber, in a reserved vector that outlives the wait.
        std::vector<TypedCtx> tctxs;
        tctxs.reserve(kN);

        for (int i = 0; i < kN; ++i) {
            objs[i].magic = 0x5A00 + i;
            tctxs.push_back(TypedCtx{ &objs[i], &idBefore[i], &idAfter[i], &wBefore[i],
                                      &wAfter[i], &gotMagic[i], &gate2, &started, &done });
            auto* t = JLibTest::MakeCtxTask(sched, &TypedFlsBody, &tctxs[i]);
            if (t) sched.Push(t);
        }

        const auto d5 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (started.load(std::memory_order_acquire) < kN
               && std::chrono::steady_clock::now() < d5)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        gate2.SignalAll();
        const auto d6 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (done.load(std::memory_order_acquire) < kN
               && std::chrono::steady_clock::now() < d6)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

        int moved = 0, held = 0, idStable = 0, idValid = 0;
        for (int i = 0; i < kN; ++i) {
            if (wBefore[i] != wAfter[i]) ++moved;
            if (gotMagic[i] == 0x5A00 + i) ++held;
            if (idBefore[i] >= 0) ++idValid;
            if (idBefore[i] == idAfter[i]) ++idStable;
        }
        std::printf("  typed: %d/%d migrated, %d held their value, ids valid %d stable %d\n",
                    moved, kN, held, idValid, idStable);

        Check(done.load(std::memory_order_acquire) == kN, "every typed-FLS task finished");
        Check(idValid == kN, "GetID() returned a real id on every fiber");
        Check(held == kN, "FiberLocal<T> held its value across the suspend, migrated or not");
        Check(idStable == kN, "and GetID() was STABLE across the suspend (tables index by it)");
        Check(moved > 0, "at least one migrated (else this arm repeats arm 3, not arm 1)");
    }

    // ---- OFF A FIBER: answerable, not fatal ------------------------------------------------
    Check(!JLib::TaskScheduler::HasFiberLocal(), "main reports NO fiber-local storage");
    Check(JLib::TaskScheduler::FiberLocal((size_t)Fls::Sentinel) == nullptr,
          "and reading a slot off a fiber gives nullptr rather than crashing");

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_fail ? "FAILED" : "PASSED", g_fail, g_fail == 1 ? "" : "s");
    return g_fail ? 1 : 0;
}
