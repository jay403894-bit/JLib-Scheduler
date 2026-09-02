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
static std::atomic<int>       g_bgOutstanding{ 0 };
static void FloorBody(void*) {
    // A fixed slab of work, big enough that scheduling overhead is not what is being timed.
    volatile long long acc = 0;
    for (int i = 0; i < 4000; ++i) acc += i;
    g_spun.fetch_add((long long)acc, std::memory_order_relaxed);
}

// ---- THE SATURATING BODY, AND WHY IT HAD TO BE MUCH BIGGER ------------------------------------
//
// FloorBody is ~2 us. One producer thread pushing those CANNOT saturate 31 workers -- the pool
// drains a round faster than a single thread can enqueue the next, so the floor sits mostly idle and
// a Lane::Normal sample lands on an awake worker instantly. That is exactly what the control
// reported: the Normal row came out FASTER than the LowLatency rows, which is not a result about
// the lane, it is the load failing to arrive.
//
// ~100x longer, so 512 of these is ~40 ms of work spread over the pool rather than ~1 ms. The
// producer can now stay ahead of the consumers, which is the only condition under which "the floor
// is busy" is true and the lane has anything to win.
static void HeavyFloorBody(void*) {
    volatile long long acc = 0;
    for (int i = 0; i < 400000; ++i) acc += i;
    g_spun.fetch_add((long long)acc, std::memory_order_relaxed);
    g_bgOutstanding.fetch_sub(1, std::memory_order_relaxed);   // pairs with the loader's cap
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

    // ---- TABLE 3: THE ONLY TABLE THAT CAN ANSWER THE LATENCY QUESTION -------------------------
    //
    // TABLE 1 CANNOT, and its own control says so: on an idle pool a Lane::Normal push also lands on
    // an awake worker instantly, so every arm measures the same thing and the control sits on top of
    // the LowLatency rows. A latency lane has nothing to win when there is nothing to wait behind.
    //
    // So this one SATURATES THE FLOOR first and then measures dispatch. That is the case the lane
    // exists for, the case stealing can cost something in, and the only configuration where
    // LowLatency and Normal should diverge at all.
    std::printf("\n  lane dispatch latency WITH THE FLOOR SATURATED -- the case the lane exists for\n");
    std::printf("    K   steal  lane          p50      p90      p99   onK/onFloor\n");

    struct CArm { size_t k; bool steal; Lane lane; const char* name; };
    const CArm carms[] = {
        { 2, true,  Lane::LowLatency, "LowLatency" },
        { 2, false, Lane::LowLatency, "LowLatency" },
        { 2, true,  Lane::Normal,     "Normal    " },   // <- must be MUCH worse, or the load missed
    };
    constexpr int kC = 3;
    std::vector<double> c50[kC], c90[kC], c99[kC];
    int cK[kC] = {}, cF[kC] = {};

    for (int r = 0; r < reps; ++r) {
        for (int a = 0; a < kC; ++a) {
            TaskScheduler::SetIoHotLane(carms[a].k);
            TaskScheduler::SetLaneIntake(true);
            TaskScheduler::SetReservedStealing(carms[a].steal);
            std::this_thread::sleep_for(std::chrono::milliseconds(30));

            // BACKGROUND LOAD, pushed before the samples and left running. Enough tasks that every
            // floor worker is inside a body when a sample arrives -- which is the whole point: the
            // lane's value is being reachable when the floor is not.
            // BOUNDED IN FLIGHT, and the first version was not -- it pushed 512 heavy tasks per
            // round unconditionally, built a backlog of ~118,000 per worker, and the pool could not
            // drain before teardown. A load generator that outruns the pool does not measure a busy
            // floor, it measures an unbounded queue, and then hangs.
            //
            // The cap is the point: keep roughly a few tasks per worker outstanding, which is
            // "every worker is inside a body" without "the queue grows forever".
            std::atomic<bool> stop{ false };
            const int cap = (int)sched.GetWorkerCount() * 3;
            std::thread loader([&] {
                while (!stop.load(std::memory_order_relaxed)) {
                    while (g_bgOutstanding.load(std::memory_order_relaxed) < cap
                           && !stop.load(std::memory_order_relaxed)) {
                        Task* t = sched.CreateTask(&HeavyFloorBody, nullptr, Lane::Normal, TaskType::Native);
                        if (!t) break;
                        g_bgOutstanding.fetch_add(1, std::memory_order_relaxed);
                        sched.Push(t);
                    }
                    std::this_thread::yield();
                }
            });
            std::this_thread::sleep_for(std::chrono::milliseconds(40));   // let it bite

            g_onReserved.store(0); g_onFloor.store(0);
            std::vector<long long> v;
            LaneLatencyRun(sched, samples, carms[a].lane, v);

            stop.store(true, std::memory_order_relaxed);
            loader.join();
            // DRAIN TO ZERO, not for a fixed 60 ms. A timed sleep was what let the previous version
            // carry a backlog into the next arm and, eventually, into teardown -- where the pool
            // failed to quiesce and dumped its state instead of finishing. Waiting on the counter
            // means each arm starts from a genuinely idle floor, which is also the only way the
            // arms are comparable to each other.
            {
                const auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(20);
                while (g_bgOutstanding.load(std::memory_order_relaxed) > 0
                       && std::chrono::steady_clock::now() < dl)
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }

            if (v.empty()) continue;
            c50[a].push_back(Pct(v, 0.50));
            c90[a].push_back(Pct(v, 0.90));
            c99[a].push_back(Pct(v, 0.99));
            cK[a] = g_onReserved.load(); cF[a] = g_onFloor.load();
        }
    }
    for (int a = 0; a < kC; ++a)
        std::printf("   %2zu   %-6s %s  %7.2f  %7.2f  %7.2f   %d/%d\n",
                    carms[a].k, carms[a].steal ? "on" : "off", carms[a].name,
                    Median(c50[a]), Median(c90[a]), Median(c99[a]), cK[a], cF[a]);

    std::printf("    ^ THE Normal ROW IS THE CONTROL AND IT MUST BE MUCH WORSE. If it is close to the\n"
                "      LowLatency rows the background load did not saturate the floor, and this table\n"
                "      is table 1 again with extra steps.\n");
    std::printf("    ^ THE Normal NUMBER IS ARBITRARY AND THE RATIO IS MEANINGLESS. It is queueing\n"
                "      behind HeavyFloorBody, so it scales with the size of a body this file chose:\n"
                "      make the background 10 ms and Normal reads ~10 ms. What matters is that the\n"
                "      LowLatency row DOES NOT MOVE with it -- lane latency is independent of how\n"
                "      long floor work runs, which is the actual property. Quoting 'the lane is\n"
                "      1500x faster' is quoting this file's body size.\n");
    std::printf("    ^ THE STEAL ARMS ARE NOT COMPARABLE HERE, and the run above showed why: every\n"
                "      Lane::LowLatency push stamps the lane-activity clock, so 300 back-to-back\n"
                "      samples keep the lane 'busy' and K never steals in EITHER arm. That the two\n"
                "      arms match is evidence the gate WORKS, not that stealing is free. Measuring\n"
                "      its cost needs a sampler that goes quiet longer than the %u us window.\n",
                TaskScheduler::IoQuietWindowUs());
    return 0;
}
