// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// HAZARD POINTERS: the reader must survive a PARK, and the scan must see it while it sleeps.
//
// THE ONLY TEST THAT MATTERS IS THE ONE WITH A SUSPEND IN THE MIDDLE. A hazard pointer that lives
// on the worker passes every test in which nothing migrates -- it goes green in CI and smashes the
// first time a reader blocks on a SchedulerMutex. This codebase has already shipped six tests that
// passed with their mechanism removed; a hazard test whose publisher never parks would be number
// seven. So every case here does the same three things:
//
//   1. a reader publishes a hazard on a node, ON A FIBER
//   2. the reader PARKS (SchedulerMutex held by someone else), which is where a worker-owned cell
//      is either overwritten by the next fiber or simply not looked at by a scan
//   3. a writer unlinks and retires that node, and scans, WHILE THE READER SLEEPS
//
// then the reader wakes and checks its node is still intact.
//
// AND THE NEGATIVE CONTROL IS RUN, not asserted in a comment: the same scenario with
// ForceWorkerCellsForTest(true) must FREE the node. If that ever stops failing, this test has
// stopped testing anything and should be treated as broken rather than as good news.

#include "TaskScheduler.h"
#include "Hazard.h"


#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>

using namespace JLib;

static int  g_failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-68s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_failures;
}

namespace {

    // A node whose destructor is observable. The poison is what tells the reader it was freed under
    // it -- checking a "freed" flag from outside would not prove the READER saw a live object.
    constexpr std::uint64_t kAlive = 0xA11FE0000A11FE00ull;
    constexpr std::uint64_t kDead  = 0xDEADDEADDEADDEADull;

    struct Node {
        std::uint64_t magic = kAlive;
        int           value = 0;
        ~Node() { magic = kDead; }
    };

    std::atomic<int> g_freed{ 0 };

    void RetireNode(Node* n) {
        HazardDomain::Instance().Retire(n, [](void* p) {
            g_freed.fetch_add(1, std::memory_order_relaxed);
            delete static_cast<Node*>(p);
        });
    }

    struct Shared {
        std::atomic<Node*> head{ nullptr };
        SchedulerMutex     gate;              // what the reader parks on
        std::atomic<bool>  readerPublished{ false };
        std::atomic<bool>  writerRetired{ false };
        std::atomic<bool>  readerSawAlive{ false };
        std::atomic<bool>  readerRan{ false };
    };

    // Returns true if the node survived the park.
    bool RunScenario(TaskScheduler& sched, bool forceWorkerCells, bool& outParked) {
        HazardDomain::ForceWorkerCellsForTest(forceWorkerCells);
        g_freed.store(0, std::memory_order_relaxed);

        Shared s;
        s.head.store(new Node{ kAlive, 42 }, std::memory_order_release);

        // The gate is taken FIRST, by the main flow, so the reader is guaranteed to block on it
        // rather than racing through. This is what makes the park deterministic instead of hoped for.
        s.gate.Lock();



        // ---- reader ---------------------------------------------------------------------------
        // Runs on a FIBER (FiberSize::Standard), which is the whole point: a fiber is the thing
        // that migrates, so this is the context where cell ownership is load-bearing.
        WaitGroup wgReader;
        wgReader.n.store(1, std::memory_order_relaxed);
        Task* reader = sched.CreateTask([&s]() {
            HazardGuard g;
            Node* n = g.Protect(0, s.head);
            s.readerPublished.store(true, std::memory_order_release);

            // THE PARK. The gate is held elsewhere, so this fiber suspends here and its worker goes
            // off to run other things -- including, with worker-owned cells, other guards.
            s.gate.Lock();
            s.gate.Unlock();

            // Resumed, possibly on a different worker. Was our node kept alive?
            s.readerSawAlive.store(n->magic == kAlive, std::memory_order_release);
            s.readerRan.store(true, std::memory_order_release);
        }, false, FiberSize::Standard, TaskType::Fiber);
        reader->waitGroup = &wgReader;
        sched.Push(reader);

        // Wait for the hazard to be published and the reader to be parked on the gate.
        for (int i = 0; i < 2000 && !s.readerPublished.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));   // let it reach the park

        // ---- noise: other fibers take guards on the same workers ------------------------------
        // With worker-owned cells this is what clobbers the parked reader's cell. With fiber-owned
        // cells it is harmless. Included so the negative control fails for the RIGHT reason rather
        // than by luck of the scan.
        WaitGroup noiseWg;
        noiseWg.n.store(64, std::memory_order_relaxed);
        for (int i = 0; i < 64; ++i) {
            Task* t = sched.CreateTask([]() {
                HazardGuard g;
                Node local{};
                g.Set(0, &local);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                g.Clear(0);
            }, false, FiberSize::Standard, TaskType::Fiber);
            t->waitGroup = &noiseWg;
            sched.Push(t);
        }
        sched.WaitFor(noiseWg);

        // ---- writer: unlink and retire WHILE THE READER SLEEPS --------------------------------
        Node* victim = s.head.exchange(nullptr, std::memory_order_acq_rel);
        RetireNode(victim);
        HazardDomain::Instance().Scan();
        s.writerRetired.store(true, std::memory_order_release);

        const bool freedWhileParked = (g_freed.load(std::memory_order_acquire) != 0);

        // DID THE READER ACTUALLY PARK? Everything here rests on it having suspended at the gate.
        // If it sailed through, this whole scenario degenerates into a single-threaded scan and
        // proves nothing -- so the absence of a park is a TEST failure, not a pass.
        outParked = !s.readerRan.load(std::memory_order_acquire);

        // Release the gate; the reader wakes and inspects its node.
        s.gate.Unlock();
        sched.WaitFor(wgReader);

        HazardDomain::ForceWorkerCellsForTest(false);

        // If it was freed while parked the reader is reading freed memory, and `readerSawAlive` is
        // whatever the allocator left behind -- so the authoritative signal is freedWhileParked.
        return !freedWhileParked && s.readerRan.load(std::memory_order_acquire)
                                 && s.readerSawAlive.load(std::memory_order_acquire);
    }

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();
    std::printf("hazard pointers -- workers=%zu, readers=%zu, cells/reader=%zu\n\n",
                sched.GetWorkerCount(),
                HazardDomain::Instance().ReaderCount(),
                HazardDomain::kCellsPerReader);

    // ---- the mechanism ------------------------------------------------------------------------
    bool parkedA = false;
    const bool survived = RunScenario(sched, /*forceWorkerCells*/ false, parkedA);
    Check(parkedA, "the reader genuinely PARKED at the gate (otherwise this proves nothing)");
    Check(survived,
          "a node protected before a PARK survives a retire+scan taken while the reader sleeps");

    // ---- the negative control, RUN --------------------------------------------------------------
    // Same scenario, cells resolved to the WORKER instead of the fiber -- i.e. bug 1 reintroduced
    // deliberately. This must NOT survive. A pass here means the test proves nothing.
    bool parkedB = false;
    const bool survivedWithWorkerCells = RunScenario(sched, /*forceWorkerCells*/ true, parkedB);
    Check(!survivedWithWorkerCells,
          "NEGATIVE CONTROL: with worker-owned cells the same node is freed under the reader");

    // ---- ordinary reclamation still happens ------------------------------------------------------
    // A hazard scheme that never frees anything would pass both assertions above. It must also let
    // go once nobody names the node.
    {
        g_freed.store(0, std::memory_order_relaxed);
        Node* n = new Node{ kAlive, 7 };
        {
            HazardGuard g;
            g.Set(0, n);
            RetireNode(n);
            HazardDomain::Instance().Scan();
            Check(g_freed.load() == 0, "a named node is NOT freed by a scan");
        }
        HazardDomain::Instance().Scan();
        Check(g_freed.load() == 1, "and IS freed once the guard is gone -- reclamation is not stalled");
    }

    std::printf("\n%s -- %d failure(s)\n", g_failures ? "FAILED" : "PASSED", g_failures);
    return g_failures ? 1 : 0;
}
