// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// WHAT DOES THE EPOCH MACHINERY ACTUALLY COST, against just taking a mutex?
//
// The library has a whole reclamation subsystem -- guards, a moodycamel queue for retirements, a
// CAS to elect one reclaimer, a scan over thousands of participants, a deferred free list -- and
// none of it had ever been measured against the boring alternative. That is a bad reason to keep
// something, however correct it is.
//
// AND THE ALTERNATIVE IS SIMPLER THAN IT LOOKS, which is the honest framing: under a mutex there is
// NO DEFERRED RECLAMATION AT ALL. If every reader holds the lock and the deleter takes the same
// lock, you can free immediately. No epochs, no retire queue, no CAS, no scan, no pending list. So
// this is not "EBR versus mutex-plus-EBR". It is:
//
//   EBR     cheap concurrent reads (two stores per guard)  + all of that machinery
//   mutex   expensive serialised reads (lock/unlock)       + free() and nothing else
//
// The whole question is whether concurrent reads are worth the subsystem, and that is a function of
// how many readers there are -- so the thread count is swept rather than assumed. At one thread the
// mutex should win outright: it is uncontended and EBR's machinery is pure overhead.
//
// WHAT IS MODELLED: the DAG's actual shape. Readers walk a short pointer chain under protection --
// that is ForEachDependent -- while a writer retires nodes and reclaims them. The traversal is
// deliberately tiny, because a large one would dilute exactly the difference under test.
//
// WHAT IS NOT: the mutex arm frees immediately and so never builds a backlog, which flatters its
// memory behaviour and is the correct comparison anyway -- immediate free is the whole reason
// somebody would choose it.

#include "TaskScheduler.h"
#include "Epochs.h"
#include "Thread.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

namespace {

struct Node { Node* next; int payload; };

constexpr int kChain = 8;          // nodes walked per read; short, like an edge list

Node*      g_head = nullptr;       // the structure readers walk
std::mutex g_mutex;                // the alternative being measured

std::atomic<long long> g_reads{ 0 };
std::atomic<long long> g_retires{ 0 };
std::atomic<bool>      g_stop{ false };

void BuildChain() {
    for (int i = 0; i < kChain; ++i) g_head = new Node{ g_head, i };
}

// A read: walk the chain and touch it. Identical work in both arms; only the protection differs.
inline int Walk() {
    int sum = 0;
    for (Node* n = g_head; n; n = n->next) sum += n->payload;
    return sum;
}

std::atomic<int> g_sink{ 0 };

void ReaderEBR() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        for (int i = 0; i < 32; ++i) { JLib::EpochGuard g; g_sink.store(Walk(), std::memory_order_relaxed); }
        n += 32;
    }
    g_reads.fetch_add(n, std::memory_order_relaxed);
}

void ReaderMutex() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        for (int i = 0; i < 32; ++i) {
            std::lock_guard<std::mutex> lk(g_mutex);
            g_sink.store(Walk(), std::memory_order_relaxed);
        }
        n += 32;
    }
    g_reads.fetch_add(n, std::memory_order_relaxed);
}

// The writer: retire a node and let it be reclaimed. This is the half the mutex arm gets for free.
void WriterEBR() {
    auto& em = JLib::EpochManager::Instance();
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        auto* victim = new Node{ nullptr, 0 };
        em.RetirePtr(victim, em.CurrentEpoch(), [](void* p) { delete static_cast<Node*>(p); });
        // TICK PERIODICALLY, NOT PER RETIRE. Tick is AdvanceEpoch + a full participant scan, and no
        // real caller runs it per retirement -- the DAG ticks per frame and self-reclaim fires on a
        // threshold in the low thousands. An earlier version of this file ticked every time and made
        // the EBR retire rate look 8x worse than the mutex, which measured the benchmark rather than
        // the design.
        if ((++n & 63) == 0) em.Tick();
    }
    g_retires.fetch_add(n, std::memory_order_relaxed);
}

void WriterMutex() {
    long long n = 0;
    while (!g_stop.load(std::memory_order_relaxed)) {
        auto* victim = new Node{ nullptr, 0 };
        {
            // THE WHOLE RECLAMATION STORY for this arm: hold the same lock readers hold, and the
            // free is immediately safe. No queue, no epoch, no scan, no deferred list.
            std::lock_guard<std::mutex> lk(g_mutex);
            delete victim;
        }
        ++n;
    }
    g_retires.fetch_add(n, std::memory_order_relaxed);
}

struct Result { double reads, retires; };

// READERS RUN AS SCHEDULER TASKS, NOT RAW std::threads, and that is not a detail. A raw thread
// never gets a thread_id, so CurrentEpochSlot hands every one of them ThreadSlot(0) -- they ALIAS
// A SINGLE SLOT and the EBR arm measures contention on it instead of EBR. The first version of this
// file did exactly that and reported 172M reads at one thread collapsing to 37M at two, which is a
// scaling curve no lock-free design produces. Scheduler workers each hold a real id.
Result Run(void (*reader)(), void (*writer)(), int nreaders, int ms) {
    g_reads.store(0); g_retires.store(0); g_stop.store(false);
    auto& sched = JLib::TaskScheduler::Instance();
    for (int i = 0; i < nreaders; ++i)
        if (auto* t = sched.CreateTask([reader] { reader(); })) sched.Push(t);
    if (auto* t = sched.CreateTask([writer] { writer(); })) sched.Push(t);
    const auto t0 = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    g_stop.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(80));   // let them drain and add in
    const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    return { g_reads.load() / s, g_retires.load() / s };
}

double Median(std::vector<double> v) { std::sort(v.begin(), v.end()); return v[v.size() / 2]; }

} // namespace

int main(int argc, char** argv) {
    const int reps = (argc > 1) ? std::atoi(argv[1]) : 3;
    const int ms   = (argc > 2) ? std::atoi(argv[2]) : 150;

    JLib::TaskScheduler::Init(0);
    BuildChain();

    std::printf("epochs vs a plain mutex -- %d-node chain per read, %d ms, %d interleaved reps\n",
                kChain, ms, reps);
    std::printf("(the mutex arm needs NO deferred reclamation at all -- it frees under the lock)\n\n");
    std::printf("  readers        EBR reads/s      mutex reads/s   ratio      EBR retires/s   mutex retires/s\n");

    for (int r : { 1, 2, 4, 8, 16 }) {
        std::vector<double> er, mr, ew, mw;
        for (int i = 0; i < reps; ++i) {
            // ARMS INTERLEAVED per rep: measuring one to completion then the other is how thermal
            // drift becomes a result.
            const Result e = Run(&ReaderEBR,   &WriterEBR,   r, ms);
            const Result m = Run(&ReaderMutex, &WriterMutex, r, ms);
            er.push_back(e.reads); ew.push_back(e.retires);
            mr.push_back(m.reads); mw.push_back(m.retires);
        }
        const double e = Median(er), m = Median(mr);
        std::printf("  %7d  %16.0f  %16.0f  %6.1fx  %14.0f  %14.0f\n",
                    r, e, m, (e > m ? e / m : -(m / e)), Median(ew), Median(mw));
    }

    std::printf("\n  A NEGATIVE ratio means the MUTEX won. Expect that at one reader: uncontended,\n"
                "  the lock is a couple of atomics and the epoch machinery is pure overhead.\n"
                "  The question is where it crosses, and whether the pool ever runs below it.\n");

    JLib::TaskScheduler::Instance().Join();
    return 0;
}
