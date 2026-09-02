// THE REACTOR'S BACKLOG IS THE GLOBAL QUEUE FOR I/O. THIS IS WHAT WATCHES IT.
//
// Lane completions no longer go straight to a worker. They queue in the completion thread's own FIFO
// backlog and drain into a reserved worker through PushIO -- the one Push that may REFUSE, when
// every K worker is buried. That makes this deque a real queue in the system, with the failure modes
// a queue has: items held and never retried, items lost at shutdown, unbounded growth.
//
// THE ASSERTIONS ARE CORRECTNESS, NOT SPEED. Depth and high-water are REPORTED, never asserted
// against a threshold -- a number that depends on how fast this machine drains a lane is not a pass
// condition, it is a measurement, and this box is not where measurements are taken. What must hold
// regardless of timing:
//
//   1. EVERY completion runs. A held backlog that is never retried loses work silently, and the
//      symptom is a hang somewhere else entirely.
//   2. The backlog ENDS EMPTY. Draining to zero is the difference between "it kept up" and "it
//      stopped holding because nothing more arrived".
//   3. The mechanism actually ENGAGED. If `pushed` is zero the completions never went through the
//      lane path at all and every assertion above is a statement about an empty set.
//
// A NON-ZERO `declined` IS NOT A FAILURE, and the test says so where it prints it. It means PushIO
// held a completion instead of dumping it on a buried worker, which is the entire point of the
// mechanism. It is reported so a run where the lane saturated is distinguishable from one where it
// never had to work -- those two look identical if you only assert on correctness.

#include "TaskScheduler.h"
#include "IoReactor.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

#ifndef WIN32_LEAN_AND_MEAN
#  define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

static int g_fail = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-72s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++g_fail;
}

static constexpr int kReads = 256;   // enough concurrent completions to make the lane work

struct Slot {
    JLib::IoRequest req{};
    JLib::IoResult  result{};
    char            buf[256]{};
};

static std::atomic<int> g_done{ 0 };
static std::atomic<int> g_wrong{ 0 };

int main() {
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("=== the reactor backlog: nothing held, nothing lost ===\n");

    JLib::TaskScheduler::EnableIoReactor(true);      // implies SetIoHotLane(1): K = 1
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io    = JLib::IoReactor::Instance();

    if (!io.IsAvailable()) {
        std::printf("  no reactor on this platform -- nothing to test\n");
        return 0;
    }

    char path[MAX_PATH], dir[MAX_PATH];
    ::GetTempPathA(MAX_PATH, dir);
    std::snprintf(path, sizeof path, "%sjlib_io_backlog.bin", dir);
    { FILE* f = std::fopen(path, "wb"); for (int i = 0; i < 4096; ++i) std::fputc(i & 0xFF, f); std::fclose(f); }

    HANDLE h = ::CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, nullptr,
                             OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
    Check(h != INVALID_HANDLE_VALUE && io.Register(h), "opened and registered the handle");
    if (h == INVALID_HANDLE_VALUE) return 1;

    JLib::ResetIoBacklogStats();

    // Heap, not stack: nothing on this frame survives the completions, which run on workers.
    std::vector<Slot*> slots;
    slots.reserve(kReads);

    int submitted = 0, immediate = 0;
    for (int i = 0; i < kReads; ++i) {
        Slot* s = new Slot();
        slots.push_back(s);

        // hiPri IS THE POINT. Only hiPri completions are lane work and only lane work goes through
        // the backlog -- a loPri continuation goes straight to the floor and would exercise nothing
        // here. PushIO refuses a non-hiPri task outright.
        JLib::Task* cont = sched.CreateInternalTask([s] {
            if (!(s->result.Ok() && s->result.bytes == 256)) g_wrong.fetch_add(1, std::memory_order_relaxed);
            g_done.fetch_add(1, std::memory_order_release);
        }, /*hiPri*/ true);
        if (!cont) break;

        const bool now = io.SubmitRead(h, s->buf, 256, (std::uint64_t)(i % 8) * 256,
                                       &s->req, &s->result, cont, JLib::CancelToken{});
        if (now) ++immediate;
        ++submitted;
    }
    std::printf("  submitted %d reads (%d answered immediately)\n", submitted, immediate);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (g_done.load(std::memory_order_acquire) < submitted
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(2));

    const int ran = g_done.load(std::memory_order_acquire);
    const JLib::IoBacklogStats st = JLib::ReadIoBacklogStats();

    std::printf("\n  backlog: depth=%llu highWater=%llu pushed=%llu declined=%llu drains=%llu\n",
                (unsigned long long)st.depth, (unsigned long long)st.highWater,
                (unsigned long long)st.pushed, (unsigned long long)st.declined,
                (unsigned long long)st.drains);
    std::printf("  (declined is NOT a failure -- it counts the lane being full and a completion\n"
                "   being HELD rather than dumped on a buried worker, which is the mechanism working)\n\n");

    Check(ran == submitted, "EVERY completion ran -- nothing was held and forgotten");
    Check(g_wrong.load(std::memory_order_relaxed) == 0, "and every one of them read the right bytes");
    Check(st.pushed > 0, "the LANE path engaged (else these assertions cover an empty set)");
    Check(st.drains > 0, "the backlog was drained at least once");
    Check(st.depth == 0, "the backlog ENDED EMPTY -- it drained rather than merely stopping");
    Check(st.highWater >= st.depth, "high water is consistent with depth");

    // Shutdown with the reactor still registered: the exit paths must flush anything still held to
    // the floor rather than dropping it. A leak here would not fail this test -- it would hang some
    // other one later -- so the value is in exercising the path at all.
    io.Stop();
    ::CloseHandle(h);
    for (Slot* s : slots) delete s;

    std::printf("\n=== %s (%d failure%s) ===\n",
        g_fail ? "FAILED" : "PASSED", g_fail, g_fail == 1 ? "" : "s");
    return g_fail ? 1 : 0;
}
