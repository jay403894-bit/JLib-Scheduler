// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// DOES A LAMBDA TASK'S CAPTURE SURVIVE A SUSPENSION?
//
// The body of `sched.CreateTask([...]{ ... })` is copied into a LambdaTask allocated from the task
// slab. That object holds the captures. The question this file exists to answer is what happens to
// it when the fiber running it SUSPENDS:
//
//   IF IT IS FREED AT SUSPEND, then every capturing lambda that waits is a use-after-free the moment
//   it resumes, io_fiber_await.h is built on sand, and the socket suite passes by luck.
//
//   IF IT SURVIVES UNTIL THE BODY RETURNS, capturing by value is the correct idiom for a task that
//   waits, and the only hazard is capturing by REFERENCE into a caller's stack.
//
// READING THE CODE SAYS THE SECOND -- DestroyTask/Free appear only in the FiberStatus::DEAD branch.
// That is an argument. This file is the measurement, and it exists because a mechanism story that
// has only been traced is not the same as one that has been observed failing when it is wrong.
//
// HOW IT WOULD DETECT THE FAILURE. A small capture could survive a free by accident -- freed slab
// memory is still mapped and usually still holds its old bytes. So the capture here is LARGE and
// PATTERNED, it is verified BYTE BY BYTE rather than by a checksum that could collide, and the pool
// is churned across the suspension so that a freed slot has every chance to be handed to somebody
// else and overwritten. If the frame is freed at suspend, that churn is what turns a silent
// survival into a visible corruption.

#include "TaskScheduler.h"
#include "fiber_body.h"
#include "Thread.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-66s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

// Big enough that it cannot ride in a register or a small slot, and patterned so any overwrite is
// visible. 512 bytes puts it well past the small task size classes.
struct BigCapture {
    static constexpr size_t kN = 512;
    unsigned char b[kN];
    void fill(unsigned char seed) { for (size_t i = 0; i < kN; ++i) b[i] = (unsigned char)(seed + i * 7u); }
    int firstBadByte(unsigned char seed) const {
        for (size_t i = 0; i < kN; ++i)
            if (b[i] != (unsigned char)(seed + i * 7u)) return (int)i;
        return -1;
    }
};

// ---- THE FIBER BODY: A NAMED FUNCTION AND AN EXPLICIT CONTEXT --------------------------------
//
// Which is also what this file now demonstrates. The large patterned payload is a MEMBER of the
// context, so it lives in the caller's frame -- storage whose scope the author can see -- and the
// task carries only a function pointer and an address. One owner for the state, one owner for the
// fiber row, and the ordinary DEAD/recycle path applies.
struct CapCtx {
    BigCapture        cap;
    unsigned char     seed;
    std::atomic<int>* parked;
    std::atomic<int>* resumed;
    std::atomic<int>* corrupt;
    std::atomic<int>* firstBad;
    JLib::Event*      gate;
};
static void CaptureBody(void* p) {
    auto& c = *static_cast<CapCtx*>(p);
    c.parked->fetch_add(1, std::memory_order_release);
    JLib::TaskScheduler::Instance().WaitOnEvent(*c.gate);

    // AFTER the suspension: is the payload the task was created with still intact?
    const int bad = c.cap.firstBadByte(c.seed);
    if (bad >= 0) {
        c.corrupt->fetch_add(1, std::memory_order_relaxed);
        int expect = -1;
        c.firstBad->compare_exchange_strong(expect, bad, std::memory_order_relaxed);
    }
    c.resumed->fetch_add(1, std::memory_order_release);
}

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== does a lambda task's by-value capture survive a suspension? ===\n");

    JLib::TaskScheduler::Init(8);
    auto& sched = JLib::TaskScheduler::Instance();
    JLib::Event& gate = sched.GetEvent("lambda_capture_gate");

    constexpr int kTasks = 32;
    std::atomic<int> parked{ 0 }, resumed{ 0 }, corrupt{ 0 }, firstBad{ -1 };
    JLib::WaitGroup wg;
    wg.n.store(kTasks, std::memory_order_relaxed);

    // The frames whose lifetime is in question. Recorded at creation so the churn below can be
    // checked against them without the tasks having to report anything about themselves.
    std::vector<const void*> suspendedAddrs;
    suspendedAddrs.reserve(kTasks);

    // ---- THE PER-ITERATION CONTEXT PATTERN, WHICH IS WHAT THIS FILE NOW DEMONSTRATES ---------
    //
    // Each fiber carries a DIFFERENT payload, so each needs its own context, and they must outlive
    // the WaitFor below. reserve() is load-bearing rather than a performance note: MakeCtxTask is
    // handed &ctxs[i], and a reallocation would move contexts already handed out from under the
    // fibers pointing at them. Sizing up front means no reallocation occurs.
    std::vector<CapCtx> ctxs;
    ctxs.reserve(kTasks);

    for (int i = 0; i < kTasks; ++i) {
        BigCapture cap;
        cap.fill((unsigned char)(i * 3 + 1));
        const unsigned char seed = (unsigned char)(i * 3 + 1);

        // `cap` dies at the end of this iteration; the copy inside the context does not.
        ctxs.push_back(CapCtx{ cap, seed, &parked, &resumed, &corrupt, &firstBad, &gate });
        JLib::Task* t = JLibTest::MakeCtxTask(sched, &CaptureBody, &ctxs[i]);

        if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
        suspendedAddrs.push_back(t);
        t->waitGroup = &wg;
        sched.Push(t);
    }

    // Everyone parks first, so every frame is suspended at once rather than in a trickle.
    const auto d1 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (parked.load(std::memory_order_acquire) < kTasks
           && std::chrono::steady_clock::now() < d1)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    Check(parked.load(std::memory_order_acquire) == kTasks, "every task parked inside its lambda");

    // ---- CHURN THE SLAB WHILE THEY ARE SUSPENDED ------------------------------------------
    //
    // THIS IS WHAT MAKES A FAILURE VISIBLE RATHER THAN SILENT. Freed slab memory stays MAPPED and
    // usually still holds its old bytes, so a released frame can read back correct by accident. Ten
    // thousand same-sized allocations between the suspend and the resume give any released slot a
    // real chance of being reused and overwritten -- which is the difference between this file
    // proving something and merely not crashing.
    // ---- AND THE CHECK THAT CANNOT BE FOOLED: ADDRESS REUSE --------------------------------
    //
    // NEITHER THE BYTE PATTERN NOR ASan CAN SETTLE THIS ON ITS OWN, and it is worth saying why
    // before trusting either.
    //
    //   THE PATTERN is weak because freed slab memory stays MAPPED and usually still holds its old
    //   bytes. A released frame can read back perfectly correct.
    //
    //   ASan IS BLIND HERE. The slab is ONE BIG BLOCK that ASan saw allocated at startup; freeing a
    //   SLOT inside it is the allocator's own bookkeeping, which ASan knows nothing about. The
    //   memory stays valid from its point of view, so a clean ASan run says nothing about slot
    //   lifetime. (It would need __asan_poison_memory_region in the slab's free path to see this,
    //   which the allocator does not do.)
    //
    // SO ASK THE ALLOCATOR INSTEAD. If a suspended task's frame were released, the slab would
    // eventually hand that exact address to somebody else -- that is what a free MEANS. Ten thousand
    // same-size-class allocations while 32 frames are suspended is ample opportunity. If no churn
    // task is ever issued the address of a still-suspended task, the frames were never freed. This
    // does not depend on what the bytes say or on what a sanitizer can see.
    {
        std::atomic<int> churn{ 0 };
        std::vector<const void*> churnAddrs;
        churnAddrs.reserve(12000);
        JLib::WaitGroup cwg;
        constexpr int kChurn = 10000;
        cwg.n.store(kChurn, std::memory_order_relaxed);
        for (int i = 0; i < kChurn; ++i) {
            BigCapture filler;
            filler.fill(0xAB);
            JLib::Task* c = sched.CreateTask([filler, &churn] {
                churn.fetch_add((int)filler.b[0] & 1, std::memory_order_relaxed);
            }, JLib::Lane::Normal);
            if (!c) { cwg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
            churnAddrs.push_back(c);        // recorded BEFORE the push -- the address is what matters
            c->waitGroup = &cwg;
            sched.Push(c);
        }
        sched.WaitFor(cwg);
        std::printf("  churned %d same-sized task frames while %d fibers were suspended\n",
                    kChurn, kTasks);

        size_t collisions = 0;
        for (const void* a : churnAddrs)
            for (const void* s : suspendedAddrs)
                if (a == s) { ++collisions; break; }

        std::printf("  %zu of %zu churn frames landed on a SUSPENDED task's address\n",
                    collisions, churnAddrs.size());
        Check(collisions == 0,
              "no suspended task's frame was reissued -- the slab never freed it");
        if (collisions)
            std::printf("  ^ THAT IS THE PROOF OF THE BUG: a live suspended frame was handed out\n"
                        "    again. The capture check below may still pass by accident.\n");
    }

    gate.SignalAll();
    sched.WaitFor(wg);

    Check(resumed.load(std::memory_order_acquire) == kTasks, "every task resumed");
    std::printf("  %d of %d captures intact after the suspension\n",
                kTasks - corrupt.load(), kTasks);
    if (corrupt.load() > 0)
        std::printf("  first corrupted byte index: %d\n", firstBad.load());

    // THE CLAIM. If this fails, capturing by value across a wait is unsafe and io_fiber_await.h --
    // and every arm of io_socket_test -- is built on a use-after-free.
    Check(corrupt.load() == 0,
          "a BY-VALUE capture is intact after the fiber resumes (the frame outlived the suspend)");

    std::printf("\n%s\n", g_fail ? "FAILED" : "PASSED");
    return g_fail ? 1 : 0;
}
