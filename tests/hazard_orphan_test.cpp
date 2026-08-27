// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// A THREAD THAT EXITS WITH A NON-EMPTY RETIRE BAG MUST NOT TAKE ITS CONTENTS WITH IT.
//
// The retire bag is `thread_local` and that is correct -- retiring is a point operation that cannot
// suspend, unlike PROTECTION, which is why cells are per-reader and this is not. The consequence is
// what had to be fixed: Retire takes a DELETER over the CALLER'S memory, so a bag abandoned at
// thread exit is not delayed reclamation. Those objects are never freed and their destructors never
// run. Workers exit at Join and app threads come and go, so this is reachable.
//
// WHAT MAKES THIS TESTABLE AT ALL is that the deleter is ours: a counting deleter says exactly how
// many objects were reclaimed, which a leak check on the process cannot.
//
// THE SHAPE OF THE TEST, and it is the shape the deque overflow test needed and did not have the
// first time: prove the PATH RAN, not just that the outcome looked right. A run where the thread
// happened to scan everything before exiting also ends with nothing leaked -- and would pass a
// naive assertion while testing nothing. So the bag is deliberately left BELOW the scan threshold
// (retire a handful, never enough to trigger an automatic Scan), and OrphanedTotal is asserted
// non-zero to confirm the handoff actually happened.

#include "TaskScheduler.h"
#include "Hazard.h"

#include <atomic>
#include <cstdio>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {
    std::atomic<int> g_freed{ 0 };
    struct Node { int v = 7; };
    void CountingDeleter(void* p) {
        g_freed.fetch_add(1, std::memory_order_relaxed);
        delete static_cast<Node*>(p);
    }
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& dom = JLib::HazardDomain::Instance();

    std::printf("hazard retire-bag orphaning -- workers=%zu\n\n", sched.GetWorkerCount());

    // A HANDFUL, deliberately. The automatic Scan fires at 2 * readerCount * kCellsPerReader, which
    // on any real machine is in the hundreds -- so a few retires are guaranteed to still be sitting
    // in the bag when the thread ends. That is the condition under test; retiring enough to trip a
    // scan would test the ordinary path instead.
    const int kN = 5;

    const size_t orphanedBefore = dom.OrphanedTotal();

    std::thread t([&] {
        for (int i = 0; i < kN; ++i)
            JLib::HazardDomain::Instance().Retire(new Node{}, &CountingDeleter);
        // NO Scan() here, and no Join: the thread simply ends with its bag full, which is the whole
        // scenario. Anything that drains on the way out would test a path this bug is not about.
    });
    t.join();

    Check(g_freed.load() == 0, "nothing was freed yet -- they were still in the exiting thread's bag");

    Check(dom.OrphanedTotal() - orphanedBefore == (size_t)kN,
          "the exiting thread's bag was handed to the orphan store, not dropped");
    Check(dom.OrphanedRetired() >= (size_t)kN, "and they are waiting there to be swept");

    // NOBODY IS PROTECTING THESE, so the first scan that looks must free every one. The scan runs on
    // this thread -- the point being that reclamation no longer depends on the thread that retired.
    dom.Scan();

    Check(g_freed.load() == kN, "a Scan on ANOTHER thread reclaimed all of them");
    if (g_freed.load() != kN) std::printf("      expected %d freed, got %d\n", kN, g_freed.load());
    Check(dom.OrphanedRetired() == 0, "the orphan store is empty again");

    // A PROTECTED ORPHAN MUST SURVIVE. The sweep uses the same scan set as an ordinary retire, so an
    // object still named by a live reader has to stay. Without this the fix could "pass" by freeing
    // orphans unconditionally, which trades a leak for a use-after-free.
    {
        g_freed.store(0, std::memory_order_relaxed);
        Node* pinned = new Node{};

        // Protect reads through an atomic source and re-verifies, which is Michael's protocol --
        // there is no "publish this raw pointer" overload, deliberately.
        std::atomic<Node*> src{ pinned };
        JLib::HazardGuard guard;          // this thread, held across the whole block
        guard.Protect(0, src);

        std::thread t2([&] { JLib::HazardDomain::Instance().Retire(pinned, &CountingDeleter); });
        t2.join();

        dom.Scan();
        Check(g_freed.load() == 0, "an orphan still named by a live reader was NOT freed");
    }
    // guard is gone here, so the pin is released
    dom.Scan();
    Check(g_freed.load() == 1, "and it IS freed once the reader lets go");

    sched.Join();
    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    return g_fail == 0 ? 0 : 1;
}
