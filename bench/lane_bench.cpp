// THE LANE, AS IT ACTUALLY IS NOW. One file, four questions, no retired mechanisms.
//
// bench.cpp and io_dispatch_latency.cpp still carry arms for things that no longer exist -- adaptive
// K, the lane deque, the producer-side spill, K=4 rungs against a K clamped to 2. Rows that measure
// a mechanism the code no longer has are worse than no rows: they print, they look like results, and
// two arms of the same configuration read as a finding about the difference between them.
//
// SO THIS FILE MEASURES ONLY WHAT THE 9-02 LANE IS:
//
//   - a SHARED moodycamel intake, K consumers, nothing bound to a worker at push time
//   - K workers that NEVER PARK and STEAL floor work once the lane has been quiet
//   - Lane::LowLatency vs Lane::Normal as the only routing input
//
// FOUR QUESTIONS, and each one has an arm that can say no:
//
//   1. Does the intake beat push-time binding?      SetLaneIntake(true/false)
//   2. What does K cost, and is 2 better than 1?    K in {1, 2}
//   3. Does stealing give the floor its cores back? SetReservedStealing(true/false), floor throughput
//   4. Does stealing cost the lane its latency?     the same arm, lane latency
//
// Q3 AND Q4 ARE THE SAME SWITCH READ TWO WAYS, and that is the point: stealing is only worth having
// if it wins Q3 by more than it loses Q4. Reporting either alone would recommend the wrong default.
//
// ---- HOW THE ARMS ARE RUN, because getting this wrong is how a bench lies ----------------------
//
// INTERLEAVED, ALWAYS. Every arm runs once per rep, in the same process, and the reported number is
// the MEDIAN across reps. Separately built binaries once moved this project's K=1 rows by 2x, which
// was machine drift presented as a result; running arm A to completion and then arm B has the same
// failure in a smaller form, because the box is not the same box ten seconds later.
//
// A CONTROL THAT MUST MOVE. Every table carries one arm whose number is known in advance -- if it
// does not separate, the harness is broken and the interesting rows are not evidence of anything.
//
// WHERE TASKS ACTUALLY RAN, printed beside every latency row. A lane row measuring zero tasks on
// reserved workers is not a fast lane, it is a vacuous row, and the count is the only thing that
// tells those apart.

#include "TaskScheduler.h"
#include "Thread.h"
#include "Timer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using namespace JLib;

// ---- sampling ---------------------------------------------------------------------------------

static std::vector<long long> g_lat;      // ns, one per sample
static std::atomic<int>       g_next{ 0 };
static std::atomic<int>       g_onReserved{ 0 };
static std::atomic<int>       g_onFloor{ 0 };

struct Sample { long long pushedAt; };
static std::vector<Sample> g_slots;

static void LatencyBody(void* p) {
    const long long now = MonotonicNs();
    const int idx = (int)(intptr_t)p;
    g_lat[idx] = now - g_slots[idx].pushedAt;

    Thread* t = Thread::GetCurrent();
    const size_t k = TaskScheduler::GetHotWorkers();
    if (t && (size_t)t->qIndex < k) g_onReserved.fetch_add(1, std::memory_order_relaxed);
    else                            g_onFloor.fetch_add(1, std::memory_order_relaxed);
    g_next.fetch_add(1, std::memory_order_release);
}

static double Pct(std::vector<long long> v, double p) {
    if (v.empty()) return -1.0;
    std::sort(v.begin(), v.end());
    const size_t i = std::min(v.size() - 1, (size_t)(v.size() * p));
    return (double)v[i] / 1000.0;             // us
}

static double Median(std::vector<double> v) {
    if (v.empty()) return -1.0;
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

// ---- Q1/Q2/Q4: lane dispatch latency ----------------------------------------------------------
//
// ONE IN FLIGHT AT A TIME, deliberately. This measures DISPATCH -- push to first instruction -- and
// a wave of N would measure queueing behind the other N-1 instead, which is a different question
// with a different answer. The soak row below is where concurrency belongs.
static void LaneLatencyRun(TaskScheduler& sched, int samples, Lane lane,
                           std::vector<long long>& out) {
    g_lat.assign(samples, -1);
    g_slots.assign(samples, Sample{ 0 });
    g_next.store(0, std::memory_order_release);

    for (int i = 0; i < samples; ++i) {
        const int before = g_next.load(std::memory_order_acquire);
        g_slots[i].pushedAt = MonotonicNs();
        Task* t = sched.CreateTask(&LatencyBody, (void*)(intptr_t)i, lane, TaskType::Native);
        if (!t) break;
        sched.Push(t);
        // Wait for THIS one before pushing the next: one in flight, by construction.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (g_next.load(std::memory_order_acquire) == before
               && std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
    }
    out.clear();
    for (long long v : g_lat) if (v >= 0) out.push_back(v);
}

// ---- Q3: floor throughput while the lane is idle -----------------------------------------------
//
// THE NUMBER STEALING EXISTS TO MOVE. No I/O at all, so the lane is quiet the whole time and a
// stealing K should be indistinguishable from K extra floor workers. With stealing off those cores
// spin on nothing and the floor runs N-K wide.
static std::atomic<long long> g_spun{ 0 };
static void FloorBody(void*) {
    // A fixed slab of work, big enough that scheduling overhead is not what is being timed.
    volatile long long acc = 0;
    for (int i = 0; i < 4000; ++i) acc += i;
    g_spun.fetch_add((long long)acc, std::memory_order_relaxed);
}

static double FloorThroughput(TaskScheduler& sched, int ms) {
    g_spun.store(0, std::memory_order_relaxed);
    std::atomic<int> done{ 0 };
    const int batch = 4096;

    const auto t0 = std::chrono::steady_clock::now();
    const auto end = t0 + std::chrono::milliseconds(ms);
    long long pushed = 0;
    while (std::chrono::steady_clock::now() < end) {
        for (int i = 0; i < batch; ++i) {
            Task* t = sched.CreateTask(&FloorBody, nullptr, Lane::Normal, TaskType::Native);
            if (!t) break;
            t->waitGroup = nullptr;
            sched.Push(t);
            ++pushed;
        }
        std::this_thread::yield();
    }
    // Let the tail drain so the rate is not inflated by work still queued.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    (void)done;
    return (double)pushed / secs;
}

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    const int reps    = (argc > 1) ? std::atoi(argv[1]) : 5;
    const int samples = (argc > 2) ? std::atoi(argv[2]) : 300;

    TaskScheduler::Init(0);
    auto& sched = TaskScheduler::Instance();
    std::printf("lane_bench -- %zu workers, %d reps, %d samples/row\n",
                sched.GetWorkerCount(), reps, samples);
    std::printf("  every arm runs once per rep, interleaved, median reported.\n\n");

    // ---- TABLE 1: lane dispatch latency ------------------------------------------------------
    //
    // THE Lane::Normal ROW IS THE CONTROL THAT MUST MOVE. It never touches the intake or the
    // reserved band whatever the other switches say, so it should be flat across every arm. If it
    // tracks the LowLatency rows, the switches are not doing anything and nothing below is evidence.
    std::printf("  lane dispatch latency -- push to first instruction, one in flight\n");
    std::printf("    K   intake  steal  lane          p50      p90      p99   onK/onFloor\n");

    struct Arm { size_t k; bool intake; bool steal; Lane lane; const char* name; };
    const Arm arms[] = {
        { 1, true,  true,  Lane::LowLatency, "LowLatency" },
        { 1, false, true,  Lane::LowLatency, "LowLatency" },
        { 2, true,  true,  Lane::LowLatency, "LowLatency" },
        { 2, false, true,  Lane::LowLatency, "LowLatency" },
        { 2, true,  false, Lane::LowLatency, "LowLatency" },
        { 2, true,  true,  Lane::Normal,     "Normal    " },   // <- the control
    };
    constexpr int kArms = (int)(sizeof(arms) / sizeof(arms[0]));

    std::vector<double> p50[kArms], p90[kArms], p99[kArms];
    int onK[kArms] = {}, onF[kArms] = {};

    for (int r = 0; r < reps; ++r) {
        for (int a = 0; a < kArms; ++a) {
            TaskScheduler::SetIoHotLane(arms[a].k);
            TaskScheduler::SetLaneIntake(arms[a].intake);
            TaskScheduler::SetReservedStealing(arms[a].steal);
            std::this_thread::sleep_for(std::chrono::milliseconds(30));   // settle

            g_onReserved.store(0); g_onFloor.store(0);
            std::vector<long long> v;
            LaneLatencyRun(sched, samples, arms[a].lane, v);
            if (v.empty()) continue;
            p50[a].push_back(Pct(v, 0.50));
            p90[a].push_back(Pct(v, 0.90));
            p99[a].push_back(Pct(v, 0.99));
            onK[a] = g_onReserved.load(); onF[a] = g_onFloor.load();
        }
    }

    for (int a = 0; a < kArms; ++a) {
        std::printf("   %2zu   %-6s %-6s %s  %7.2f  %7.2f  %7.2f   %d/%d\n",
                    arms[a].k, arms[a].intake ? "on" : "off", arms[a].steal ? "on" : "off",
                    arms[a].name, Median(p50[a]), Median(p90[a]), Median(p99[a]), onK[a], onF[a]);
    }
    std::printf("    ^ onK/onFloor is the VACUITY CHECK. A LowLatency row with onK == 0 measured the\n"
                "      floor, not the lane, and its latency says nothing about K.\n\n");

    // ---- TABLE 2: does stealing give the floor its cores back? --------------------------------
    std::printf("  floor throughput with NO I/O -- the number stealing exists to move\n");
    std::printf("    K   steal      tasks/sec\n");

    struct FArm { size_t k; bool steal; };
    const FArm farms[] = { { 0, true }, { 2, true }, { 2, false } };
    constexpr int kF = 3;
    std::vector<double> tput[kF];

    for (int r = 0; r < reps; ++r) {
        for (int a = 0; a < kF; ++a) {
            TaskScheduler::SetIoHotLane(farms[a].k);
            TaskScheduler::SetReservedStealing(farms[a].steal);
            TaskScheduler::SetLaneIntake(true);
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
            tput[a].push_back(FloorThroughput(sched, 250));
        }
    }
    for (int a = 0; a < kF; ++a)
        std::printf("   %2zu   %-6s  %13.0f\n", farms[a].k, farms[a].steal ? "on" : "off",
                    Median(tput[a]));

    std::printf("    ^ K=0 IS THE CEILING: no core reserved, so nothing to give back. K=2/steal=on\n"
                "      should approach it; K=2/steal=off is what reservation costs when the lane is\n"
                "      idle. If on and off are equal, stealing is not firing -- check the quiet\n"
                "      window (%u us), not the throughput.\n", TaskScheduler::IoQuietWindowUs());

    std::printf("\n  NOT MEASURED HERE, deliberately: lane latency while the FLOOR is saturated.\n"
                "  That is the case stealing is supposed to cost something in, and it needs a\n"
                "  contended arm this file does not have yet. Do not read table 1 as covering it.\n");
    return 0;
}
