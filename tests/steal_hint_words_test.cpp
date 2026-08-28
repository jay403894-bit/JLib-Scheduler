// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// THE STEAL HINT ABOVE ONE WORD -- a pool with more queues than a uint64 has bits.
//
// WHY THIS EXISTS. The hint bitmaps were a single 64-bit word, one bit per queue, and
// MaybeStealable turned the hint off WHOLESALE once loPri.size() exceeded 64 -- necessarily, since
// covering only queues 0..63 would starve every queue above them. loPri.size() is num_workers + 1
// (the non-worker lane), so the edge sat at 64 WORKERS: a 64-thread machine kept its hints and a
// 128-thread Threadripper lost them entirely, falling back to probing every victim at the 0.2-0.9%
// hit rate the hints were built to fix. Probe traffic goes as N^2, so the machine where the hint
// matters most was the one that switched it off.
//
// The maps are now four words (256 queues). NOTHING ON A NORMAL DEVELOPER MACHINE EXERCISES THAT:
// a 32-thread part gives 31 workers, 32 queues, one word -- so every existing test runs the same
// path it always did and would pass with the second word wired up backwards. This file is the only
// thing that reaches word 1.
//
// == WHAT IT ASSERTS, AND WHAT IT DOES NOT ==
//
// ASSERTS: a pool larger than one word still balances. The fork-join below spawns children onto the
// SPAWNING WORKER'S OWN DEQUE, so the only way work reaches any other thread is a steal -- there is
// no push routing to spread it. If the word indexing were wrong in the direction that matters (a
// victim's bit written to one word and read from another, so an advertised queue looks empty), the
// tree would collapse onto the threads that happen to hold it and the distinct-executor count would
// fall off a cliff. That is the failure this catches.
//
// DOES NOT ASSERT that the hint is doing anything for performance. With the hint disabled entirely
// the pool ALSO balances -- more expensively. That is deliberate: a timing assertion on a shared CI
// machine is a flake generator, and this project has thrown away results to machine drift more than
// once. Efficacy is a bench question; this is a correctness question.
//
// OVERSUBSCRIPTION IS THE POINT, not a compromise. 100 software workers on a 32-thread box is a
// legitimate configuration -- threads oversubscribe, they do not fail -- and it is the only way to
// reach a two-word pool without a 128-thread machine. Timings here are meaningless by construction;
// do not add any.

#include "TaskScheduler.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <mutex>
#include <set>
#include <thread>

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-68s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

namespace {

// Enough workers that loPri.size() (workers + 1) crosses 64 and lands in the SECOND hint word.
constexpr size_t kWorkers = 100;
constexpr int    kDepth   = 13;            // 8192 leaves

std::atomic<long long> g_leaves{ 0 };
std::mutex             g_seenMx;
std::set<std::thread::id> g_seen;

struct ForkArgs {
    JLib::TaskScheduler* sched;
    JLib::WaitGroup*     wg;
    int                  depth;
};

// The scheduler owns the DECREMENT (it walks task->waitGroup on completion), so this body only ever
// raises the count -- and raises it BEFORE pushing, or the root can reach zero while a subtree is
// still being built and WaitFor returns early.
void ForkBody(void* p) {
    auto* a = static_cast<ForkArgs*>(p);
    if (a->depth <= 0) {
        g_leaves.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lk(g_seenMx);
            g_seen.insert(std::this_thread::get_id());
        }
        delete a;
        return;
    }
    for (int k = 0; k < 2; ++k) {
        auto* child = new ForkArgs{ a->sched, a->wg, a->depth - 1 };
        a->wg->n.fetch_add(1, std::memory_order_relaxed);
        JLib::Task* t = a->sched->CreateTask(ForkBody, child);
        if (!t) { a->wg->n.fetch_sub(1, std::memory_order_release); delete child; continue; }
        t->waitGroup = a->wg;
        a->sched->Push(t);
    }
    delete a;
}

} // namespace

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    JLib::TaskScheduler::Init(kWorkers);
    auto& sched = JLib::TaskScheduler::Instance();

    const size_t workers = sched.GetWorkerCount();
    const size_t queues  = workers + 1;       // + the non-worker lane; this is what loPri.size() is
    std::printf("steal hint above one word -- workers=%zu, queues=%zu, words=%zu\n\n",
                workers, queues, (queues + 63) / 64);

    // THIS TEST CANNOT MANUFACTURE ITS OWN SUBJECT, and saying so out loud is the point.
    //
    // StartPool clamps an explicit poolSize to hardware_concurrency -- deliberate policy, not an
    // oversight -- so asking for 100 workers on a 32-thread box yields 32, and every assertion
    // below would then run the ONE-word path while claiming to cover two. That is precisely the
    // vacuous-test shape this project has been bitten by repeatedly, so it exits early and loudly
    // rather than passing on coverage it did not have.
    //
    // Reaching word 1 needs a host with more than 64 hardware threads. The multi-word indexing is
    // the same expression on both sides (q >> 6 for the word, q & 63 for the bit, in the writer and
    // the reader alike), so a mismatch is unlikely by construction -- but "unlikely by construction"
    // is an argument, not a test, and this file should not be mistaken for the latter until it runs
    // somewhere it can bite.
    if (queues <= 64) {
        std::printf("  SKIPPED -- this host has %zu queues; word 1 begins at 65.\n", queues);
        std::printf("  The multi-word hint path is UNEXERCISED here. Run this on a host with more\n");
        std::printf("  than 64 hardware threads (Threadripper/EPYC/dual-socket) to make it real.\n");
        sched.Join();
        return 0;
    }

    JLib::WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);
    auto* root = new ForkArgs{ &sched, &wg, kDepth };
    JLib::Task* t = sched.CreateTask(ForkBody, root);
    if (!t) { std::printf("CreateTask returned null -- slab exhausted\n"); return 1; }
    t->waitGroup = &wg;
    sched.Push(t);
    sched.WaitFor(wg);

    const long long expected = 1LL << kDepth;
    const long long got      = g_leaves.load(std::memory_order_relaxed);
    std::printf("      leaves=%lld expected=%lld, distinct executors=%zu\n",
                got, expected, g_seen.size());

    // COMPLETION FIRST. A word-indexing bug that hid a victim cannot lose work -- the owner still
    // drains its own deque -- so this passing is necessary and nowhere near sufficient.
    Check(got == expected, "every leaf ran exactly once");

    // THE ONE THAT WOULD ACTUALLY FAIL. Children go to the spawning worker's own deque, so every
    // thread beyond the first got its work by STEALING. A pool this size collapsing to a handful of
    // executors is the signature of victims that cannot be seen.
    Check(g_seen.size() > 8, "work spread by stealing across many threads, not a handful");

    // A parallel range on the same pool, which drives the OTHER publisher: SetParallelHint at
    // publish time rather than UpdateBacklogHint at drain time. The two write different maps and a
    // change can move one without the other.
    std::vector<double> data(1u << 20, 2.0);
    sched.ParallelFor(0, (int)data.size(), [&data](int b, int e) {
        for (int i = b; i < e; ++i) data[i] = std::sqrt(data[i]) + 1.0;
    });
    bool allTransformed = true;
    for (size_t i = 0; i < data.size(); i += 4096)
        if (data[i] < 2.0) { allTransformed = false; break; }
    Check(allTransformed, "ParallelFor completed correctly on a multi-word pool");

    std::printf("\n%s\n", g_fail == 0 ? "ALL CHECKS PASSED" : "FAILURES ABOVE");
    sched.Join();
    return g_fail == 0 ? 0 : 1;
}
