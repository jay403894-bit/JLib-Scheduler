// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#define NOMINMAX
#include "Task.h"
#include "CancelToken.h"
#include "TaskMPSCQueue.h"
#include "Epochs.h"
#include "TaskDeque.h"
#include "TaskAllocator.h"
#include "Topology.h"   // topology::CpuMask -- llcMaskOfWorker is one per worker
#include <cstdio>   // the stale-library guard reports through stderr
#include <cstdlib>  // ...and aborts rather than corrupting the heap
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <unordered_map>
#include <thread>
// <immintrin.h> was here and is x86-only. Removed rather than guarded, for the same reason
// LockFreeList.h lost its <intrin.h>: nothing in this header uses an intrinsic. The spin hint the
// .cpp needs is platform::CpuRelax() in platform.h, which is arch-correct by construction.
#include <queue>
#include "GlobalFiberPool.h"
// The three synchronisation primitives, all pulled in here so a caller needs only this header --
// WaitFor takes a WaitGroup&, GetEvent returns an Event&, and WaitOnEventDirectArmed hands out a
// DirectEvent*. Event.h used to be excluded because it depended on this header; it now depends on
// Fiber.h instead, which is all it ever needed.
#include "DirectEvent.h"
#include "WaitGroup.h"
#include "Event.h"
namespace JLib {
	class Thread;

	// How a cancellable wait ended.
	namespace detail {
		// TASK SIZE HISTOGRAM -- opt-in, -DJLIBSCHED_TASK_STATS=ON.
		//
		// THE QUESTION. TaskAllocator::SLOT is 256 because it must fit the LARGEST LambdaTask<F>
		// (see the static_assert in CreateTask). But sizeof(Task) is 64, and a capture-free task is
		// exactly that -- so it occupies a quarter of the slot it is given. Coroutine frames had the
		// same shape and a 64-byte class turned out to be worth 4x the memory on 75% of them; the
		// task path is the same question on a far higher volume (292,565 tasks vs 0 frames in the
		// DAG scaling bench).
		//
		// WHY THIS CANNOT BE ANSWERED FROM THE BENCHES. Every task in bench/ is a capture-free
		// function pointer or an empty lambda, so a bench-derived histogram is maximally flattering
		// and would argue for a 64-byte class on evidence that says nothing about real code. The
		// distribution that matters comes from an APPLICATION -- Game01, the physics step, the
		// renderer's per-cell submits -- where lambdas actually capture. Run it there before
		// choosing anything.
		//
		// 32-byte buckets rather than the coroutine histogram's octaves, because the decision here
		// is exactly WHERE to put a class boundary, and 64/128/256 would beg that question.
		//
		// Sharded per thread for the same reason every other counter in this library is: a shared
		// atomic on the creation path would manufacture contention and perturb the thing being
		// measured. This one is on the HOT path -- every CreateTask -- so it matters more here than
		// anywhere else.
#if defined(JLIBSCHED_TASK_STATS)
		inline constexpr size_t kTaskSizeBuckets = 8;
		inline constexpr size_t kTaskSizeShards = 64;

		struct alignas(platform::kCacheLine) TaskSizeShard {
			std::atomic<uint64_t> bucket[kTaskSizeBuckets]{};
			std::atomic<uint64_t> maxSize{ 0 };
			std::atomic<uint64_t> totalBytes{ 0 };
		};
		inline TaskSizeShard g_taskSizeShards[kTaskSizeShards];
		inline std::atomic<size_t> g_taskSizeNext{ 0 };

		inline TaskSizeShard& TaskSizeSlot() {
			static thread_local TaskSizeShard* s = [] {
				const size_t n = g_taskSizeNext.fetch_add(1, std::memory_order_relaxed);
				return &g_taskSizeShards[n % kTaskSizeShards];
			}();
			return *s;
		}

		// Upper bound of each bucket, so bucket i covers (kTaskSizeEdge[i-1], kTaskSizeEdge[i]].
		// Edges at 80 and 96 because a REAL application turned out to be bimodal at exactly 64 and
		// 80 (Game01, 28,619 tasks), and 32-byte buckets hid that -- everything landed in one "<=96"
		// row that could have meant anything from 65 to 96. A histogram whose resolution is coarser
		// than the decision it informs is a histogram that has to be re-run.
		inline constexpr size_t kTaskSizeEdge[kTaskSizeBuckets] = { 64, 80, 96, 128, 160, 192, 256, 0 };

		inline void RecordTaskSize(size_t n) {
			TaskSizeShard& s = TaskSizeSlot();
			size_t b = kTaskSizeBuckets - 1;                       // last bucket is "> 256"
			for (size_t i = 0; i + 1 < kTaskSizeBuckets; ++i)
				if (n <= kTaskSizeEdge[i]) { b = i; break; }
			// Sole writer per shard in the common case, so load/store rather than fetch_add -- the
			// same trick TaskAllocator::liveAdd uses, and for the same measured reason.
			s.bucket[b].store(s.bucket[b].load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
			s.totalBytes.store(s.totalBytes.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
			uint64_t prev = s.maxSize.load(std::memory_order_relaxed);
			while (n > prev && !s.maxSize.compare_exchange_weak(prev, n, std::memory_order_relaxed)) {}
		}

		void ReportTaskSizes();
		// Explicit stream, so the shutdown auto-report can reach a file. A GUI app has no console.
		void ReportTaskSizesTo(std::FILE* out);
#else
		inline void RecordTaskSize(size_t) {}
		inline void ReportTaskSizes() {}
		inline void ReportTaskSizesTo(std::FILE*) {}
#endif
	}

	enum class WaitResult : uint8_t {
		Ok,          // acquired the lock / took the permit
		Cancelled,   // the scope was cancelled; NOTHING WAS ACQUIRED -- do not Unlock or Signal
	};

	class TaskScheduler {
		friend class Thread;
		friend class GlobalFiberPool;


	public:
		// Priority inheritance methods (public for SchedulerMutex access)
		Task* GetCurrentTask() const;
		void CleanupTaskMetadata(Task* task);

		// Cancellation, observed wherever a task is about to RUN. True means the task was cancelled
		// and has already been disposed of -- released its WaitGroup, destroyed, slot returned --
		// so the caller must not touch it again and must not execute it.
		//
		// THIS EXISTS BECAUSE THE CHECK WAS IN ONE PLACE AND A TASK CAN START IN FOUR. Worker() had
		// it; ProcessMainThread, WaitFor's own drain and TryRunStolenNativeTask did not, so a
		// cancelled task picked up by a HELPER ran its payload -- reproducibly, a handful out of 500
		// in about 7% of runs, because the helper is a race for the same queue the workers are
		// discarding from. Same shape as the 3.2.1 bug where the waiter queues bypassed
		// IsTaskCancelled: "one place cancellation is decided" only holds if every site calls it.
		//
		// ONLY AN UNSTARTED TASK IS DISCARDED, for the reason Worker() documents at length: a queued
		// entry may be a RESUME, and discarding one of those abandons a live stack or coroutine
		// frame instead of cancelling it.
		bool DiscardIfCancelled(Task* task);

		static TaskScheduler& Instance() {
			if (!instance)
				throw std::runtime_error("Call TaskScheduler::Init() before Instance()!");
			return *instance;
		}
		static void Init(size_t poolSize = 0); // 0 = auto-detect

		~TaskScheduler();
		bool PushMain(Task* task);
		void ProcessMainThread();
		// Waits on wg like WaitFor(), but ALSO drains mainQ (ProcessMainThread) each spin.
		// REQUIRED if the WaitGroup covers any TaskDAG main-affinity node (see TaskNode::isMain)
		// -- those tasks only ever run when something calls ProcessMainThread, so a plain
		// WaitFor() would spin forever waiting on a task nothing is servicing. Caller must BE
		// the main thread (only it should call ProcessMainThread).
		void WaitForMain(WaitGroup& wg);
		void Join();
		void NotifyAll();

		// Print every worker's sleep/queue state to stdout. For a watchdog that has decided the pool
		// is wedged: a stack trace only shows workers sitting in cv.wait, which is already known.
		// What identifies a lost wakeup is a worker with state=SLEEPING and queued=1 -- parked while
		// holding work that only it can drain, because inboxes are not stealable.
		// Unsynchronised by design; see Thread::GetDebugState.
		void DumpPoolState(const char* why) const;
		// Parallel range loop. Decides serial-vs-parallel by MEASURING, not by element count: it runs a
		// small prefix inline, times it, and parallelizes the rest only if the extrapolated work clears
		// ~75us. That constant is the fork-join dispatch overhead and is the only thing that
		// generalizes -- the crossover ELEMENT COUNT moves 400x with per-element cost (see
		// bench.exe's crossover sweep), which is why the old fixed `> 10000` gate simultaneously
		// parallelized cheap loops that then ran 11x slower and serialized expensive ones that would
		// have run 12x faster. `chunkSize` is the grain; it is floored so the range can't be split into
		// more than ~4 chunks per worker no matter how small a value is passed.
		// How workers are bound to logical CPUs. Set BEFORE Init(); changing it afterwards does
		// nothing, since binding happens at thread creation.
		//
		//   Hard  (default) -- SetThreadAffinityMask. A worker runs on its own core or nowhere. Best
		//                      cache locality, and the only mode where you DECIDE where
		//                      oversubscription lands instead of discovering it. This is what shipping
		//                      engines do, because a game largely owns the machine.
		//   Ideal           -- SetThreadIdealProcessor: a strong hint. Keeps locality and keeps the
		//                      topology map meaningful, but Windows may migrate a worker under
		//                      contention from ANOTHER PROCESS, or for thermal/core-parking reasons --
		//                      the cases where hard affinity's worst case bites. A good default if the
		//                      host application does not own the machine.
		//   None            -- placement entirely up to Windows. For embedding inside an application
		//                      that manages thread placement itself.
		//
		// Locality-aware stealing (SMT sibling, then cache cluster) only means anything under Hard or
		// Ideal: if threads migrate freely, "my sibling" stops describing where data is. Pinning and
		// topology-aware stealing are one decision, not two.
		//   PhysicalOnly    -- one worker per PHYSICAL core, pinned to that core's first logical CPU with
//                      every SMT sibling left empty. HALVES the pool on an SMT machine, so it varies
//                      placement AND worker count together -- that is unavoidable, and sizing a pool
//                      to physical cores is a normal configuration in its own right.
        enum class AffinityPolicy : uint8_t { Hard = 0, Ideal, None, PhysicalOnly };
		static void           SetAffinityPolicy(AffinityPolicy p);
		static AffinityPolicy GetAffinityPolicy();

		// What an idle worker does when it has no local work and stole nothing. Set BEFORE Init().
		//
		//   Sleep (default) -- park on the condition variable. Costs a kernel transition to wake,
		//                      which is the single largest item in dispatch latency, and gives the
		//                      core back to everything else on the machine.
		//   NoSleep         -- never park while running. Lowest possible dispatch latency, and it
		//                      holds every worker core permanently. Measured on the reference machine:
		//                      4.1x on round-trip latency, 2.9x on the frame DAG, 3.1x on fork-join,
		//                      5.2x on single-producer throughput. That is what a core each buys.
		//
		// THE DEFAULT IS Sleep AND SHOULD STAY THAT WAY for a library rather than an application.
		// Spinning workers are a battery and thermal problem on the ARM64/Android target, where the
		// throttling costs more clock than the wake latency ever costs in dispatch; they starve
		// whatever else the host runs (an audio device thread, a render thread, Jolt through the
		// JobSystem adapter); and they make the oversubscription policy incoherent, since that
		// policy reserves a core per persistent busy thread and this would make EVERY worker one.
		// WHICH ONE TO PICK, and do NOT decide this from the scheduler benchmarks. They make NoSleep
		// look free, because in a scheduler benchmark the pool IS the workload: there is no render
		// or audio thread for the spinning to tax, so the wake saving is all that shows up. Measured
		// with an IDLE pool and a memory-bound main thread (31 workers, 2026-08-16):
		//
		//     no pool at all   14.65 ms/frame        Sleep (parked)   14.65 ms   +0.0%
		//     NoSleep          15.17 ms   +3.5%
		//
		// An idle Sleep pool is FREE. Any spinning pool costs ~3.5% to every other thread in the
		// process. And it is not the steal sweep doing it -- a control pool of pure CpuRelax threads
		// touching NO shared memory reproduced almost all of it (15.15 ms), so the cost is core
		// OCCUPANCY: all-core boost, SMT siblings, power budget. That also means there is no
		// spin-loop optimisation to be had; the measured ceiling on cheapening the spin is 0.8%.
		//
		// The two effects scale with OPPOSITE things. NoSleep's benefit scales with how OFTEN the
		// pool idles (wakes avoided); its cost scales with how LONG it idles. Break-even is roughly
		// where 0.035 * idle_gap ~= one 4.6us wake, i.e. an idle gap near 130us. Order of magnitude
		// only -- it assumes a single memory-bound victim thread -- but enough to decide with.
		//
		//   Sleep (the default, and the right answer for interactive apps): anything with a busy
		//     main/render thread, anything sharing the machine, anything whose idle gaps are long.
		//     A GAME IS THIS CASE, not the NoSleep case: a 60 FPS frame is 16,600us and even a
		//     2000 FPS frame is 500us, both orders of magnitude past break-even. An earlier version
		//     of this comment recommended NoSleep for a fullscreen game; that was wrong, and it was
		//     wrong because it reasoned from the benchmark rather than from an embedded pool.
		//   NoSleep: batch/offline work where the task graph is the entire program (asset bakes,
		//     offline renders, simulation runs), tight pipelines with sub-100us idle gaps, or a
		//     reserved-core deployment where the pool really does own the hardware.
		//
		// Pause() parks regardless of policy -- pausing means "stop using the CPU", and a policy
		// that kept spinning through it would be ignoring the only thing Pause is for.
		//
		// NOT A TIMED WAIT, and the distinction matters: this changes how long a worker searches
		// BEFORE parking, and the park itself stays an unconditional cv.wait. A lost wakeup still
		// hangs, unconditionally and visibly, instead of being silently papered over by a timeout.
		// It does make such a race rarer, which is a real debugging cost -- the 1.2.0 lost wakeup
		// was only findable because it reproduced at ~25% on a slow runner.
		// THERE IS DELIBERATELY NO MIDDLE SETTING. A SpinBriefly mode existed here, searching for a
		// configurable number of microseconds before parking, on the classic spin-then-block
		// reasoning that spinning for the cost of a block is 2-competitive. It was measured and it
		// was WORSE THAN BOTH NEIGHBOURS, monotonically worse as the budget grew (frame DAG, µs per
		// graph: Sleep 22.5 | brief@2 23.6 | brief@5 23.6 | brief@20 27.3 | brief@100 34.4 |
		// NoSleep 7.8). The 2-competitive argument assumes spinning is free for everyone else, and
		// with 31 workers it is not: spinners burn memory bandwidth and contend on steal CASes
		// against the workers that actually have work, and then park anyway, paying the wake cost
		// plus continuous park/unpark cv churn on top. Both extremes avoid one half of that; the
		// middle gets both. Removed rather than kept as a trap for anyone expecting a compromise.
		enum class IdlePolicy : uint8_t { Sleep = 0, NoSleep };
		// SAFE TO CALL ON A RUNNING POOL as of 1.3.6 -- the backing store is atomic. It was not
		// before: a plain global read inside the worker loop could be hoisted out of it, so a
		// change might never be observed at all. That was undocumented, which made it a trap.
		//
		// IF YOU FLIP IT AT RUNTIME, RESTORE IT ON EVERY PATH. Leaking NoSleep on an exception or an
		// early return does not fail loudly -- it silently taxes every other thread in the process
		// for the rest of the run, at a cost measured below in real digits. A scoped RAII wrapper
		// for this was written and then removed before it ever shipped in a release: measurement
		// (also below) showed the phase-switching use case it was built for did not pay, and an
		// unused public class is worse than a one-line discipline. If you find yourself flipping
		// this in more than one place, write the guard locally rather than asking for it back.
		//
		// COST MODEL -- why the default is Sleep and why you should think hard before changing it.
		// Measured 2026-08-16, 31 workers, IDLE pool, memory-bound main thread:
		//     no pool at all   14.65 ms/frame       Sleep (parked)  14.65 ms   +0.0%
		//     NoSleep          15.17 ms  +3.5%
		// An idle Sleep pool is FREE. An idle NoSleep pool taxes every OTHER thread in the process.
		// The cost is core OCCUPANCY, not cache traffic: a control pool of pure CpuRelax threads
		// touching no shared memory reproduced almost all of it -- which also means there is no
		// spin-loop optimisation to be had (measured ceiling 0.8%). The tax is therefore
		// proportional to IDLE TIME, not to work done.
		//
		// AND THAT 3.5% IS A LOWER BOUND, NOT THE NUMBER. Re-measured inside a real 2D game
		// (5-node frame DAG, vsync off, 600 frames after 120 warm-up, two interleaved rounds,
		// median frame time):
		//     Sleep    383.3 / 374.1 us            NoSleep  462.0 / 464.9 us   <- 23% WORSE
		// A game has the render thread, GPU driver threads and audio all competing, and spinning
		// workers land on them at exactly the latency-sensitive moments. The synthetic figure
		// understated the real cost by ~7x.
		//
		// WHY THERE IS NO ADAPTIVE MODE, since it is the obvious next idea (proposed and rejected
		// 2026-08-17). Switching automatically on queue depth / steal rate / suspension rate / DAG
		// pressure / idle ratio fails for two reasons that no amount of tuning fixes. First, the
		// controller cannot observe its own cost function: the tax lands on the RENDER thread and
		// every available signal is scheduler-internal, so it would converge on NoSleep in exactly
		// the cases measured as losing. Second, the signals are anti-correlated with the decision --
		// they describe the PRESENT, the decision needs the NEXT idle gap, and in a frame workload a
		// heavy burst is precisely what precedes a long idle tail. It would be most confident right
		// before it was most wrong. Keeping a small number of workers hot instead was also dropped:
		// permanent cost, intermittent benefit, and the cost is proportional to the hot count while
		// the benefit is sub-proportional (the other workers still wake cold).
		static void       SetIdlePolicy(IdlePolicy p);
		// Runtime kill switch for the bulk steal hint, so its value can be measured against an A/A
		// control inside ONE process. Diagnostic: shipping code should leave it on.
		static inline std::atomic<bool> stealHintOn{ true };
		static void SetStealHint(bool on) noexcept { stealHintOn.store(on, std::memory_order_relaxed); }
		static IdlePolicy GetIdlePolicy();

		// K-HOT: keep the first K workers from ever parking, while the rest obey IdlePolicy.
		//
		// The bounded-cost middle between Sleep and NoSleep. NoSleep's penalty lands in p90 and
		// scales with how many cores spin, so a handful of hot workers is a different trade from
		// spinning the whole pool. A hot worker also costs a PUSHER nothing: it never advertises
		// WS_GOING_TO_SLEEP, so pushes to it take the awake-preference skip instead of a notify.
		//
		// DEFAULT 0 -- off. A spinning core taxes every other thread in the process, so this is
		// opt-in exactly like the I/O layer is; a job-system-only user must not pay for it.
		//
		// WHETHER IT HELPS DEPENDS ON WHERE WORK LANDS. If pushes are spread round-robin, K hot
		// workers only catch K/N of them, and the answer is to STEER work at them rather than to
		// raise K. Measure before choosing a number.
		static void   SetHotWorkers(size_t k);
		// Re-applies the K clamp once workers exist; called by StartPool. See SetHotWorkers.
		void ClampHotWorkersToPool();
		static size_t GetHotWorkers();
		// WHO MAY DRAIN A BACKLOGGED LANE.
		//
		//   0  nobody -- the lane is strictly private to its hot worker
		//   1  hot workers only, from a backlogged hot sibling
		//   4  (default) hot workers, plus any ORDINARY worker that is already awake and searching
		//   2, 3  diagnostic arms the dispatch bench interleaves; see Thread.cpp`s steal predicate
		//
		// Mode 4 NEVER WAKES ANYONE. It is purely opportunistic: it spends capacity that happens to
		// be spinning already. That is exactly why it is safe to default on -- under the default Sleep
		// policy an ordinary worker is PARKED when the lane backs up (it has no bulk work keeping it
		// awake), so the arm is inert and measured identical to mode 1 in every row. Under NoSleep it
		// is a capacity valve, and the valve matters most where K is smallest:
		//
		//     NoSleep, p99            off    hot-only   + ordinary
		//       K=1  200us uniform    229       173          53
		//       K=1  200us skewed    1190      1165         554
		//       K=2  200us skewed    1196       806         464
		//       K=4   20us            162       108          92
		//
		// At K=4 the hot set already absorbs the backlog and mode 4 is a wash. It is a safety valve
		// for an under-provisioned lane, NOT a substitute for setting K: under Sleep it cannot help
		// at all, and that is the default.
		static inline std::atomic<int> laneHintMode{ 4 };
		static void SetLaneHintMode(int m) noexcept { laneHintMode.store(m, std::memory_order_relaxed); }
		static int  GetLaneHintMode() noexcept { return laneHintMode.load(std::memory_order_relaxed); }

		// THE TWO PREDICATES THAT DEFINE THE LOW-LATENCY LANE. Everything -- push routing, inbox
		// draining, deque popping, steal probes, the sleep predicate -- asks one of these rather than
		// open-coding the condition. Nine sites read the hiPri lane; a copy of the rule at each is
		// how the pickup-discard invariant drifted three times, so there are no copies.
		//
		// THE LANE ONLY EXISTS WHEN SOMEONE SERVES IT. At K=0 a hiPri task routes to the ordinary
		// lane and NOBODY probes hiPri -- which makes the default pool's worker loop CHEAPER than it
		// was before any of this: one inbox, one deque, one steal probe per victim.
		//
		// hiPri was never a real priority queue -- per-worker queues plus stealing gave no global
		// ordering, so "picked up first" cost every worker a second inbox, a second deque and a
		// second probe per victim to buy something close to a coin flip. It has a job now, and only
		// where it has one.
		static bool HiPriLaneActive() { return GetHotWorkers() > 0; }

		// Does THIS worker serve the lane?
		//
		// K > 0: only the hot ones, so N-K workers halve their search -- half the inbox checks, half
		// the deque checks, half the steal probes. That is the structural win, and it is why
		// ordinary workers must NOT steal hiPri: letting them means they have to probe it.
		//
		// K == 0: EVERY worker serves it, exactly as before any of this existed. Without that clause
		// nobody drains a hiPri lane at the default setting, and anything that reaches one -- a task
		// queued before K changed, the shutdown drain, any push site the collapse missed -- strands
		// forever while the pool spins looking for work it refuses to take. Making correctness
		// depend on catching every push site was the fragile half of this design; this makes the
		// safe case the DEFAULT case and leaves the optimisation to the configuration that asked
		// for it.
		// K == 0 RETURNS FALSE FOR EVERYONE, and that is the dead-lane elimination: PickNextWorker
		// routes hiPri to the hot set only, so at K=0 nothing can enter a hiPri lane and scanning
		// one is provably wasted work at the DEFAULT setting.
		//
		// ================================================================================================
		// PRIORITY BELONGS TO THE TASK, AND NOTHING MAY OVERRIDE IT ON RESUME.
		//
		// A task's hiPri is set once, at CreateTask, and defaults to 0. Push, Requeue and PushLocal
		// all route on it; IoReactor splits its completion batch by it precisely so PushBatch taking
		// priority as a PARAMETER cannot quietly drop it. There is no path that promotes a task to
		// the lane behind the caller's back, and there must not be one.
		//
		// THE TEMPTING CHANGE IS "RESUMES SHOULD BE hiPri" -- a woken fiber or coroutine is latency
		// sensitive, the argument goes, so put it on the fast lane. It is wrong for a reason that has
		// nothing to do with how long the resumed body runs:
		//
		//     THE LANE'S CAPACITY IS K. Anything routed there competes for K workers.
		//
		// K is 0 or 1 by default. Routing every resume there does not make resumes fast -- it
		// SERIALISES a 31-worker pool through one worker. The lane is valuable because it is sparse;
		// fill it with everything and it is just a smaller pool with a longer queue. That argument
		// holds even if every resumed body were provably short.
		//
		// The body length is the SECOND problem, and it is measured: a resumed coroutine is arbitrary
		// code, a running task cannot be preempted, and the skewed soak found that one-in-eight
		// resumes doing 200us of work takes the lane to a 1.1ms p99 with hot workers spending 62-65%
		// of their idle passes beside a buried sibling. Lane stealing drains the queue behind such a
		// task but cannot touch the task itself.
		//
		// So hiPri stays OPT-IN, per task, defaulting off -- and setting it is a claim about the work
		// ("every resume of this is short, and it is worth one of my K slots"), not a request for
		// speed. Only the caller can make that claim; the scheduler cannot infer it.
		// ================================================================================================
		//
		// It is not the whole safety story, though -- lowering K while lane work is already queued
		// would strand it. The worker sites pair this with a cheap non-empty check on their OWN
		// queues (a local cache line, one load) so anything stranded is still drained. Remote probes
		// get no such fallback: those are the ping-pong, and there is nothing to rescue there.
		static bool WorkerServesHiPri(size_t qIndex) { return GetHotWorkers() > qIndex; }

		// HOW HARD the I/O critical path preempts. Applies to the K hot workers AND the reactor's
		// completion threads -- NEVER to the process, and never to the other workers.
		//
		// THE SCOPE IS IN THE CALL, NOT THE ENUM, and that is deliberate. Process-wide elevation
		// measured 5x WORSE alongside K-hot: it raises all N workers, and N spinning threads then
		// preempt the completion thread feeding them. An enum offering a process-wide tier as a peer
		// of a per-thread one would invite exactly that mistake, so it does not offer one.
		//
		//   Normal    leave scheduling alone. The default.
		//   Elevated  no privilege required. Windows: THREAD_PRIORITY_TIME_CRITICAL, the top of the
		//             non-realtime range (15) inside a normal-priority process -- this is the
		//             configuration measured best (HOT p99 4.5-10us against a 144-2416us baseline).
		//             Linux/BSD would be SCHED_RR per thread; macOS QOS_CLASS_USER_INTERACTIVE.
		//   Realtime  the privileged tier: Linux/BSD SCHED_FIFO (needs CAP_SYS_NICE), macOS
		//             THREAD_TIME_CONSTRAINT_POLICY (what CoreAudio uses).
		//
		// ON WINDOWS, Realtime BEHAVES AS Elevated, on purpose. The only step above TIME_CRITICAL is
		// REALTIME_PRIORITY_CLASS, which is PROCESS-wide, needs a privilege, and would let a spin
		// loop starve the OS itself. Refusing to escalate is the honest mapping; silently giving the
		// caller a process-wide change they did not ask for is not.
		//
		// POSIX IS NOT IMPLEMENTED -- there is no backend yet, so both tiers are a no-op there
		// rather than a lie. Note that affinity is NOT a substitute: exclusive pinning was measured
		// and is worse than doing nothing (see SetHotWorkerExclusive). A POSIX port genuinely needs
		// the privileged call or real core isolation.
		enum class HotThreadPolicy : uint8_t { Normal = 0, Elevated, Realtime };
		static void            SetHotThreadPolicy(HotThreadPolicy p);
		static HotThreadPolicy GetHotThreadPolicy();

		// Hard-pin ONLY the hot workers to their cores, leaving the rest on the global policy.
		// MUST be set before Init -- placement happens as each worker starts. OFF BY DEFAULT.
		//
		// Hard-pinning the whole pool measured ~45%% worse wake latency and ~2x on a frame DAG,
		// which is why the default policy is Ideal. A hot worker is the opposite case: it never
		// parks, so it has no wake latency to lose, and there are one or two rather than N. This is
		// also the PORTABLE half of the priority story -- it reduces preemption without needing
		// TIME_CRITICAL or CAP_SYS_NICE.
		static void SetHotWorkerPin(bool on);
		static bool GetHotWorkerPin();

		// EXCLUSIVE AFFINITY: the hot workers own their cores and EVERY OTHER THREAD masks those
		// bits off. Pinning ALONE measured worse than doing nothing, because it confines the hot
		// worker without excluding anyone else from its core -- so when another thread lands there
		// the hot worker cannot migrate away and waits. This is the other half. Set before Init,
		// implies pinning for the hot workers, OFF BY DEFAULT.
		//
		// The userspace approximation of isolcpus -- and it cannot exclude OTHER PROCESSES, which is
		// exactly where it stops being equivalent to the real thing.
		static void SetHotWorkerExclusive(bool on);
		static bool GetHotWorkerExclusive();
		static void SetHotCpuMask(unsigned long long m);
		static unsigned long long GetHotCpuMask();

		// Called BY a thread ON ITSELF to stay off the hot cores. Ordinary workers and the reactor's
		// completion threads do this automatically; an application thread that wants the same can
		// call it directly. No-op unless exclusive mode is on.
		static void ExcludeCurrentThreadFromHotCpus();

		// How much estimated SERIAL WORK (microseconds) a loop must represent before ParallelFor splits
		// it. Defaults to 75us in Release and 750us in Debug -- the constant is the fork-join
		// dispatch+join overhead, and an unoptimized build pays roughly an order of magnitude more of it.
		// Exposed because it is a property of the machine and build, not a universal truth; an app that
		// has profiled its own workload knows better. Set it enormous (1e12) to force every ParallelFor
		// serial, which is the fastest way to answer "is ParallelFor causing this?" without a rebuild.
		// Set once at startup; read-only thereafter.
		// Hand epoch reclamation to the caller: workers stop self-triggering, and you call
		// EpochManager::Instance().Tick() from your own idle point instead. MUST be called before
		// StartPool. See EpochManager::SetSelfReclaim in Epochs.h for the full contract, the
		// measured numbers, and the warning about what happens if you disable it and never Tick().
		//
		// A FORWARDER, and it exists because the real function was unfindable. Every other knob in
		// this library is a static on TaskScheduler, so that is where people look -- the first
		// person to go looking for this one searched for TaskScheduler::SetSelfReclaim, did not
		// find it, and reasonably concluded the feature did not exist. An implementation detail
		// being the only entry point to a tuning option is an API bug, not a naming preference.
		static void SetSelfReclaim(bool on);
		static bool SelfReclaimEnabled();

		// Force every ParallelFor to run its whole range inline, on the calling thread.
		//
		// Replaces `SetParallelForThresholdUs`, which was removed in 1.4 along with the probe it
		// tuned. That setter's genuinely useful job was not tuning: it was answering "is ParallelFor
		// responsible for this?" in one run, without a rebuild, by being set enormous. This keeps
		// that affordance and drops the pretence that the number meant anything portable.
		static void SetParallelForSerial(bool on);
		static bool ParallelForSerial();

		// Parallel range loop. Calls func(lo, hi) over disjoint subranges covering [begin, end),
		// blocks until every one has run, and the calling thread participates rather than idling.
		//
		// IT NEVER PREDICTS -- this is the 1.4 change, and it replaced the probe outright.
		//
		// Until 1.4 this ran a serial prefix, timed it, extrapolated, and parallelized only if the
		// estimate cleared a ~75us gate. That works on a uniform body and CANNOT work on a
		// data-dependent one, which is what this library is for. A prefix over items 0..311 says
		// nothing about item 50,000 when the early ones early-out at 2 ns and the later ones do full
		// mesh collision at 4,000 ns. Worse, the prefix WARMS THE CACHE while the remainder streams
		// from DRAM, so the estimate is biased low SYSTEMATICALLY rather than noisily; and on a
		// hybrid part the probe runs on the caller's core class and the work runs on another.
		//
		// THE ROOT OF IT: ITERATION COUNT IS A USELESS PROXY FOR EXECUTION TIME. Everything below is
		// a consequence of trying to recover time from a number that does not carry it.
		//
		// Static probing hits four walls. They are independent -- fixing one leaves the rest:
		//
		//   1. PROBE OVERHEAD. A timer read is 15-30 ns plus pipeline serialisation, against a
		//      ~69 ns task overhead, so a probe fine-grained enough to see divergence costs more
		//      than the thing it is deciding about.
		//      *** THIS ONE DID NOT BITE US, and the distinction is worth keeping: our probe called
		//      the clock exactly TWICE per ParallelFor, outside the loop, so ~30-60 ns amortised
		//      over the whole range. Do not cite it as a reason ours failed. It matters the other
		//      way -- ours was the CHEAPEST POSSIBLE probe and the other three walls still took it.
		//   2. CACHE PERTURBATION. The prefix warms L1/L2; the remaining 99% streams from DRAM. The
		//      estimate is therefore biased LOW systematically rather than noisily -- always the
		//      same direction, so it never averages out and more sampling cannot help.
		//   3. DATA-DEPENDENT WORK. In the workloads this library targets -- frustum culling, ray
		//      tracing, narrow-phase -- iteration 1 early-outs at 2 ns and iteration 50 does full
		//      mesh collision at 4,000 ns. No sample predicts divergence.
		//   4. ASYMMETRIC TOPOLOGY. Measuring on a P-core describes an execution profile that means
		//      nothing once an E-core steals the remainder. The normal case on the Android/ARM
		//      targets, not an edge case.
		//
		// And a fifth that is about people rather than hardware: the gate LOOKED DETERMINISTIC while
		// being wrong. Same input, same answer, every run -- so it read as a measurement rather than
		// a guess, and nothing ever prompted a check. A noisy predictor advertises its own
		// unreliability; this one did not.
		//
		// NOT A NOVEL POSITION. No mainstream work-stealing runtime gates parallelism on a timed
		// serial prefix: Cilk-5 let spawn/steal decide, oneTBB's auto_partitioner uses a depth
		// budget with steal feedback, and Rayon resets a split budget on observed steals. All of
		// them infer demand from STEALS -- something that actually happened -- rather than from a
		// cost model. This is converging with a settled answer, not inventing against it.
		//
		// WHAT REPLACED IT: RECURSIVE LAZY SPLITTING, and STEALS DECIDE. This publishes the right
		// half of the range onto the calling thread's own deque and carries on with the left. Nobody
		// took it -> the splitter takes it straight back and runs it inline for ~11 ns, no dispatch
		// and no notify. Somebody took it -> the pool was hungry and the split was right.
		//
		// Everything it needs lives on THIS stack frame, which is sound only because ParallelFor
		// BLOCKS: the wait group and callable outlive every task by construction. That is exactly
		// why PushArray, which does NOT block, cannot use it.
		//
		// A SHARED-CURSOR ALTERNATIVE EXISTS AND IS USED AS A FALLBACK (RunCursorRange): one task per
		// worker, each pulling [lo, lo+grain) off one atomic. ParallelFor was switched to it for a
		// single commit and switched back. The bench crossover sweep -- 32 points over four body
		// costs, which is the instrument for this question -- shows the two CROSS OVER rather than
		// tie, medians of 3 and non-overlapping below 200k:
		//
		//     heavy body   N=1000   2000    4000    10000   200000
		//       splitter   10.3x   12.6x   13.6x   13.9x    18.5x
		//       cursor      6.6x    7.8x    9.4x    9.3x    22.6x
		//
		// The splitter is 1.4-1.6x better across the whole mid-range -- hundreds to thousands of
		// items with real per-item work, i.e. the frame-graph shape this library is for -- and gives
		// up ~1.2x only at very large N. A "tied" verdict was published briefly on the strength of
		// three samples that were all large-N, which is the one region where they agree. Do not
		// re-run that comparison without the sweep.
		// GRAIN (was `chunkSize`) is the smallest subrange worth handing to another thread. It is
		// floored at 64 slices per worker with an absolute floor of 64 items -- a statement about
		// the POOL, whose size is known exactly, never about the BODY.
		//
		// WHAT YOU GIVE UP with the probe gone, stated plainly: nothing here can decline to
		// parallelize a body too cheap to be worth it. Pass a grain far smaller than the body
		// justifies and it will faithfully slice that fine and lose to a serial loop -- ~0.19x on a
		// 20k range of a ~0.4 ns/element body, where the old probe held 1.00x. Give it a grain worth
		// a few microseconds of work and the same case is ~1.00x. If you cannot say, use the
		// overload below; if you need to know whether a loop should be parallel at all,
		// SetParallelForSerial(true) answers it in one run without a rebuild.
		void ParallelFor(int begin, int end, int grain, std::function<void(int, int)> func);

		// The same thing WITHOUT having to name a grain, and this is the one most callers want.
		//
		// "How do I know what grain to pass?" is the fair objection to the overload above, and for
		// most callers the honest answer is that they cannot know: they know their body is "a
		// collision check" or "a matrix multiply", not what it costs in nanoseconds per element.
		// Requiring a number nobody has is how you get 32 passed in because it looked reasonable.
		//
		// So this derives one from the two things that ARE known exactly -- the range and the pool
		// size -- and never consults the body: `range / (workers * 8)`, the rule Cilk's `cilk_for`
		// uses for its own default.
		//
		// A TUNING CONSTANT IS PART OF THE ALGORITHM IT WAS TUNED FOR, and this number has now been
		// wrong in BOTH directions inside a single release. 8 per worker suits the recursive
		// splitter, which pays per split and has a serial spine and so wants fewer, larger leaves.
		// While ParallelFor was briefly pointed at the shared cursor -- which publishes every lane at
		// once and pays one fetch_add per slice, so it wants the finer division -- keeping 8
		// measured 6.23x against 16.55x at an explicit grain on a 64 MB memory-bound body. Changing
		// it to defer to the cursor's floor then measured 4.36x against 11.33x once ParallelFor went
		// back to splitting. If the algorithm underneath changes again, this is a measurement to
		// redo, not a constant to carry across.
		//
		// PASS A GRAIN ONLY IF YOU HAVE MEASURED. The number that matters is wall-clock per leaf,
		// not elements: aim for a few microseconds of work in each. If you have timed the loop
		// serially once -- which is a two-line experiment in a dev build -- then
		// `grain = range * (target_leaf_time / total_serial_time)` gets you there without knowing
		// anything per-element.
		//
		// This does NOT rescue a body too cheap to parallelize; nothing probe-free can, and the
		// explicit overload's comment says why. What it does is bound the damage: leaves are capped
		// at `workers * 64`, so the worst case is that many dispatches rather than one per element.
		void ParallelFor(int begin, int end, std::function<void(int, int)> func);
		// `ParallelForFJ` was REMOVED in 1.4. It was the fork-join variant: split in half, spawn the
		// right half, recurse left. ParallelFor stopped dispatching to it when the slice-stealing
		// cursor path replaced per-chunk tasks, which left it public with no caller anywhere.
		//
		// USE `ParallelFor`. It is the drop-in -- same shape, same blocking behaviour -- and it still
		// decides serial-vs-parallel for you.



		// Shared slice-stealing core behind ParallelFor. Takes func
		// by REFERENCE: both callers block, so the object outlives every task, and copying a
		// std::function per worker would reintroduce an allocation this exists to remove.
		void RunCursorRange(int start, int end, int grain, std::function<void(int, int)>& func);

		// Blocking range loop with ATOMIC SLICE-STEALING, and no probe.
		// `ParallelRange` was REMOVED in 1.4, before it ever shipped. It was added earlier in the
		// same release as the probe-free entry point, and once ParallelFor lost its probe and moved
		// to the same slice-stealing cursor the two were literally the same function. Two names for
		// one behaviour is worse than the problem either was solving. Use ParallelFor.

		bool Push(Task* task);
		void WaitFor(WaitGroup& wg);

		// Cancellable WaitFor: returns Cancelled if `tok` fires while still waiting, Ok if the group
		// completed. Cancelling ends THIS WAIT ONLY -- the count is untouched, the outstanding tasks
		// still run and still decrement, and any other waiter on the same group is unaffected. Use
		// WaitGroup::CancelWaiters(tok) to release such waiters eagerly. See the definition.
		WaitResult WaitFor(WaitGroup& wg, CancelToken tok);
		bool Push(uint8_t cpu_affinity, Task* task);
		bool Requeue(Task* task);
		// minPerSegment: the smallest run this is willing to hand to a single worker. The default of
		// 64 suits a big fire-and-forget batch, where the alternative is ONE push and the notifies
		// are pure added cost. A caller replacing N individual Push() calls -- which already notify
		// N times -- wants 1, because for it any segmenting strictly REDUCES notifies. Getting this
		// backwards is a real regression in both directions, so it is a parameter rather than a
		// constant: see ParallelFor's flat path.
		// pref: the batch is still assumed HOMOGENEOUS -- every task in one call is placed as this
		// ONE class, not read per-task. A genuinely mixed-corePref set of tasks is the CALLER's job
		// to split into homogeneous runs and call PushBatch once per run -- see Thread.cpp's
		// drainInbox for the pattern. Deliberately not scanned here: most callers (ParallelFor, any
		// already-homogeneous submission) already know their batch is one class, and a scan-and-
		// partition on every call would tax that common case to serve the one caller that needs it.
		void PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity=0, size_t minPerSegment=64,
		               bool hiPri=false, CorePref pref=CorePref::Default);

		// Submit [begin, end) as ceil(n/chunkSize) TASKS rather than n of them, each task looping
		// over its own chunk and calling fn(i) per index. Returns the number of tasks created.
		//
		// WHY THIS EXISTS. Per-task scheduler overhead is ~85-105 ns measured (create + dispatch +
		// execute + reclaim), and it is a per-TASK cost, not a per-item one: it buys queueing,
		// stealing, completion accounting and reclamation, none of which n independent items each
		// need separately when they are known up front. Chunking divides that whole figure by
		// chunkSize -- at 32 it is ~3 ns/item -- which is the same amortisation a range-based API
		// gets, without giving up a per-item callable.
		//
		// NOT MADE OBSOLETE BY ParallelFor, and the reason is structural rather than a
		// performance argument. ParallelFor BLOCKS: its cursor, wait group and
		// callable all live on the caller's stack frame, which is the only thing that makes a
		// zero-allocation slice-stealing loop possible. PushArray does not block -- it hands you a
		// WaitGroup and returns, so you can submit range work and go do something else, or never
		// wait at all. A non-blocking cursor would need heap state and a refcount to keep it alive
		// past the caller's frame, which is a different design with a different cost.
		//
		// So the split is: PushArray when you want the range submitted and control back, the other
		// two when you want it done before the next line runs. It is also the only one of the three
		// taking a PER-ITEM callable rather than fn(lo, hi).
		//
		// ONE SHARP EDGE, since nothing here floors chunkSize for you: this creates a task per
		// chunk, so a tiny chunkSize over a large range creates a task per few items and will
		// exhaust the slab (measured: chunk 1 over 4M items exhausted a 1M-slot arena and fell back
		// to running inline, at 32 ns/item against 0.06 at chunk 4096). Pick a chunk size.
		//
		// NOT A REPLACEMENT FOR ParallelFor, which picks its own split, runs a chunk on the calling
		// thread and blocks until done. (It no longer probes -- 1.4 removed the gate; splits are
		// published speculatively and steals decide.) PushArray is the fire-and-forget
		// sibling: it returns as soon as the work is queued, so use it when the caller has other
		// things to do, wants to submit several arrays before waiting on them together, or already
		// knows its own chunk size. Pass a WaitGroup to be able to wait; pass nullptr not to.
		//
		// fn is COPIED into every chunk task (so it must be copyable, and its captures must outlive
		// the work). If the arena is exhausted the remaining chunks run INLINE on the caller, which
		// is the same graceful degradation ParallelFor does rather than dropping work on the floor.
		template<typename F>
		// hiPri routes every chunk task to the high-priority queue; default is low, matching
		// CreateTask and PushBatch. Priority here is QUEUE ORDER only -- it never implies placement,
		// which is CorePref's job (see Task.h).
		size_t PushArray(size_t begin, size_t end, size_t chunkSize, F&& fn, WaitGroup* wg = nullptr,
		                 bool hiPri = false) {
			if (end <= begin) return 0;
			if (chunkSize == 0) chunkSize = 1;
			const size_t total  = end - begin;
			const size_t chunks = (total + chunkSize - 1) / chunkSize;

			std::vector<Task*> ts;
			ts.reserve(chunks);
			for (size_t c = 0; c < chunks; ++c) {
				const size_t lo = begin + c * chunkSize;
				const size_t hi = (lo + chunkSize > end) ? end : lo + chunkSize;
				Task* t = CreateTask([fn, lo, hi]() { for (size_t i = lo; i < hi; ++i) fn(i); }, hiPri);
				if (!t) {                                   // arena exhausted: run it here
					for (size_t i = lo; i < hi; ++i) fn(i);
					continue;
				}
				t->waitGroup = wg;
				ts.push_back(t);
			}

			// Count BEFORE the push, never after: the instant PushBatch publishes these, a worker
			// can run one and decrement. Adding to n afterwards races a WaitFor that already saw
			// zero and returned on half-submitted work.
			if (wg && !ts.empty())
				wg->n.fetch_add((int)ts.size(), std::memory_order_relaxed);
			if (!ts.empty())
				PushBatch(ts.data(), ts.size(), 0, /*minPerSegment*/64, hiPri);
			return ts.size();
		}
		// Hand a task to a SPECIFIC core via that worker's immediate slot, bypassing the queues
		// entirely. For a persistent service running inside the pool rather than beside it -- an
		// audio mixer, a network poll loop. Running such a thing as a plain std::thread would work,
		// but the scheduler could not see it and the pool would silently oversubscribe the machine;
		// a pinned task is visible to placement, an outside thread is a core that quietly went
		// missing. The core is marked in immediateCoresInUse and excluded from normal placement
		// until the task finishes.
		bool PushImmediate(uint8_t cpu_affinity, Task* task);

		// PushFork was REMOVED in 1.3.4 -- use Push. It placed a child on the CALLING worker, on the
		// theory that the parent was about to WaitFor and free that core, so the child would run
		// warm on the parent's data. Measurement killed both halves of that: the win was one
		// avoided worker WAKE (~5us, gone entirely under IdlePolicy::NoSleep) and not locality at
		// all -- a control child sharing NO data with its parent saved just as much. Against that,
		// it was 1.5x-4.6x SLOWER for any fork-join-shaped spawn, worst on small trees. To place a
		// task on a known worker, Push(cpu_affinity, task) -- one-based, 0 meaning round-robin.
		static bool IsInitialized() {
			return instance != nullptr;
		}

		// How many worker threads this pool ACTUALLY has. Use this to size per-worker resources --
		// command allocators, context pools, a third-party job system's concurrency hint -- rather
		// than recomputing it from std::thread::hardware_concurrency(), which is a guess about a
		// number this object already knows. The auto pool is hw-1, but Init(poolSize) accepts an
		// explicit size and an app with a persistent audio thread is told to pass hw-2, so any
		// hardware_concurrency-derived count is wrong on exactly the machines that were configured
		// deliberately.
		//
		// NOT the same question as GetSafeTC(), which answers "what size SHOULD the pool be" and is
		// only meaningful before Init(). This answers "what size is it".
		//
		// Fixed after Init() and safe to call from anywhere; the vector is built once in StartPool
		// and never resized while the pool is running.
		size_t GetWorkerCount() const;

		// Pay the task slab's page faults NOW rather than during the run.
		//
		// As of 1.4 the slab is LAZY: `Init()` touches almost nothing, and resident memory grows
		// with peak live tasks instead of with capacity (measured 277 MB -> 21 MB, 57 ms -> 5.6 ms
		// at 31 workers). The trade is that first-touching a page moves from startup into whenever
		// that page is first needed, so a workload whose live-task count GROWS faults pages
		// mid-run -- and a fault mid-frame is a latency spike.
		//
		// Call this during load if you would rather eat the cost up front. Prefaulting the full
		// capacity reproduces the pre-1.4 behaviour exactly. Latency-sensitive applications with a
		// known task ceiling probably want it; a phone, or anything that never approaches the
		// default million slots, should leave it alone and keep the memory.
		void PrefaultTaskSlots(size_t slots);

		// Build the task slab LAZILY instead of up front. Set BEFORE Init(); the slab is constructed
		// with the scheduler.
		//
		// DEFAULT IS OFF, and the reason is a property rather than a number. Eager construction
		// faults every page in before the first task runs, so the steady state has no memory events
		// at all -- allocation is a pointer pop off a free list. That is the zero-allocation runtime
		// this library advertises. Lazy keeps heap allocation out too, but moves first-touch page
		// faults INTO the run, and a fault mid-frame is an unpredictable kernel transition on the
		// critical path -- the same class of problem as worker-side epoch reclamation, which was
		// worth 3x on p99 to move off it.
		//
		// What lazy buys, measured at the default 1M slots x 256 bytes:
		//
		//     eager   277 MB resident, Init() 57 ms      lazy   21 MB resident, Init() 5.6 ms
		//
		// So: leave it off on desktop, where 256 MB is invisible and the guarantee is worth more.
		// Turn it on for Android and iOS, where a whole app may have a few hundred MB and a quarter
		// of a gigabyte of never-touched task slots is disqualifying. Resident cost then tracks peak
		// live tasks rather than capacity.
		//
		// PrefaultTaskSlots() is the middle ground: lazy on, then prefault the ceiling you actually
		// expect during load, and pay nothing for the rest.
		static void SetLazyTaskSlab(bool on);
		static bool LazyTaskSlabEnabled();

		// Total task slab capacity. Defaults to 1024*1024 (1M), same as always; call this before
		// Init() to change it. No structural reason it was fixed -- not a power of two anything else
		// depends on, not coupled to the ABI guard, which hashes type layout rather than a runtime
		// count -- it just went unexposed until something wanted it. Combine with SetLazyTaskSlab to
		// size AND choose the commit strategy: a small eager slab for a memory budget you want to
		// hold for certain, or a large lazy one for a ceiling you do not expect to reach.
		//
		// PRE-INIT ONLY, same contract as the setters above: taskAllocator is a member of
		// TaskScheduler, constructed once when Init() calls `new TaskScheduler(...)`, so this has to
		// land before that to be read by it.
		//
		// Exhaustion past whatever this is set to is not an error on its own -- CreateTask returns
		// nullptr and every caller in this codebase already falls back to running inline (see
		// RunLazyRange and ParallelFor's grain-floor comment) -- but a slab too small for a real
		// workload turns that fallback from an occasional safety net into the common case, which
		// costs the dispatch benefit this scheduler exists for. Measure before shrinking it.
		// WHAT ELSE DRAWS ON THIS SLAB, because "slots" is not the same as "tasks" and sizing it
		// from the task count alone will come up short:
		//
		//   a task                1 slot
		//   a TaskDAG node        2 slots  -- the TaskNode AND its dependents list; TaskNode's
		//                                    constructor allocates twice. Applies to gates and
		//                                    external nodes too, which have no task at all.
		//   a spawned coroutine   2 slots  -- its Task and its frame (frames come from here as of
		//                                    2.12.0, for one arena to size and observe rather than
		//                                    two, and so coroutines do not punch a hole in the
		//                                    zero-allocation steady state described above)
		//
		// THE FAILURE MODES DIFFER, which matters more than the counts. A coroutine frame FALLS BACK
		// to global new when the slab is full or the frame exceeds TaskAllocator::SLOT, so it never
		// fails -- frames borrow opportunistically and stop competing under pressure. A task returns
		// nullptr; a TaskNode's constructor THROWS. Size for the tasks and nodes; the frames will
		// look after themselves.
		//
		// JLib::SetCoroFramePooling(false) opts frames back out to global new entirely.

		// ---- per-class slab sizing ---------------------------------------------------------------
		//
		// The slab is THREE pools -- 256, 128 and 64 bytes -- and their sizes used to be derived from
		// the one task-slab number by fixed divisors (`slots / 8` each). That was defensible while
		// coroutine frames were the only user of the small classes, and it stops being defensible the
		// moment anything else uses them: a divisor chosen for one consumer silently starves the next.
		//
		// It is not hypothetical. Routing Tasks and TaskNodes into the 64-byte class was tried and
		// immediately evicted coroutine frames from it -- the pool sized `slots / 8` for frames could
		// not also hold the tasks, so frames quietly fell back to 256-byte slots and the class stopped
		// doing the job it was built for. Nothing failed; it just silently undid itself.
		//
		// So the sizes are EXPLICIT. One number cannot size three pools, because memory in one pool
		// cannot serve a request from another -- that is the price of size classes, and the right
		// response is to let the caller state the split rather than to guess a divisor.
		struct SlabSizes {
			// NAMED FOR THEIR SLOT SIZE, not big/mid/small. `small` is a Windows macro (rpcndr.h:
			// `#define small char`), so a member of that name breaks every consumer that includes
			// windows.h -- which shipped in 3.0.0. Caught by tests/windows_header_guard.cpp on its
			// first run, in the PUBLIC struct callers have to write, which is the worse place for it.
			// DEFAULTS DERIVED FROM MEASUREMENT, not from a divisor. Game01 instrumented with
			// JLIBSCHED_TASK_STATS over 37,480 real tasks: largest 80 bytes, mean 77.5, 15.4% at
			// exactly 64 and 84.6% at exactly 80, NOTHING above 80. So the 80-byte pool is the
			// primary one and the 256-byte pool is no longer where tasks live -- it now serves DAG
			// edge chunks (which want a full slot on purpose) and coroutine frames up to 224 bytes.
			//
			//     256K x 256 =  64 MB      1M x 80 = 80 MB
			//     128K x 128 =  16 MB    256K x 64 = 16 MB      total 176 MB
			//
			// That is DOWN from 256 MB pre-3.0, with 1.63M task capacity against 1M. An earlier cut
			// of this release added the 80-byte pool WITHOUT shrinking the 256-byte one and reserved
			// 360 MB -- a memory regression dressed as a memory optimisation.
			//
			// THE TRADE, stated because a default decides for everyone who never calls SetSlabSizes:
			// an application whose lambdas capture more than 128 bytes gets 256K task slots here
			// rather than 1M, because such a task goes straight to the 256-byte class and cannot
			// fall back. No measured workload looks like that -- but none has been measured, either,
			// and the honest version of "we have evidence for this shape and none for the other" is
			// to say so rather than to imply the case is closed.
			size_t slots256 = 1024 * 1024 / 4;   // DAG edge chunks, frames >128 B, oversized lambdas
			size_t slots128 = 1024 * 1024 / 8;   // larger coroutine frames
			size_t slots80  = 1024 * 1024;       // two-capture lambdas: the common frame-loop task
			size_t slots64  = 1024 * 1024 / 4;   // capture-free / single-capture tasks, TaskNodes
		};

		// PRE-INIT ONLY: the allocator is constructed with these
		// when the scheduler is, so a call after Init() does nothing.
		//
		// THE FAILURE MODES STILL DIFFER PER CLASS, and that is what to size against. A coroutine
		// frame FALLS BACK -- to a larger class, then to global new -- so an undersized `small`
		// costs memory and never fails. A Task returns nullptr and a TaskNode's constructor THROWS,
		// so `big` is the one whose exhaustion is a real error. Size `big` for tasks and nodes; size
		// `small` and `mid` for the peak number of coroutine frames live at once, which is bounded by
		// how many you actually spawn, not by anything the library controls.
		static void SetSlabSizes(const SlabSizes& sizes);
		static SlabSizes CurrentSlabSizes();

		// ---- OPTIONAL SERVICE LAYERS ------------------------------------------------------------
		//
		// This library is a JOB SYSTEM first. Deadlines and asynchronous I/O are layers on top, and
		// an app that wants neither should pay for neither -- not a thread, not a core, not a
		// surprise. So they are OFF until asked for, and asking has to happen before Init because
		// the pool is sized once and each enabled service takes a core.
		//
		//     TaskScheduler::EnableIoReactor(true);   // implies timers
		//     TaskScheduler::Init(0);                 // hw-3 workers: main, timer, completion
		//
		// ENFORCED, NOT ADVISED. Using a disabled layer fails at the first call, loudly and
		// deterministically, rather than working while quietly running the machine one thread over.
		// A warning was the first design and it was wrong: the failure it guards against is a few
		// percent of throughput, forever, which nobody notices and nobody attributes correctly. A
		// hard stop at the first Arm or the first Register is a two-minute fix.
		//
		// Enforcement only applies once a POOL EXISTS. Using the timer or the reactor without ever
		// calling Init is legitimate -- there are no workers to oversubscribe.
		//
		// Ignored when an EXPLICIT poolSize is given: that is the app's own arithmetic and this must
		// not silently second-guess it. The layers still have to be enabled to be used.
		static void EnableTimers(bool on) noexcept;
		static bool TimersEnabled() noexcept;

		// Implies EnableTimers: an I/O timeout is a deadline, and every non-trivial reactor user
		// wants one. Enabling I/O and then discovering timers are off would be a papercut with no
		// upside.
		//
		// `completionThreads` is HOW MANY THREADS DRAIN THE COMPLETION PORT, and it is a parameter of
		// this call rather than a knob on IoReactor for one reason: the pool reserves one core PER
		// completion thread, so the two numbers must agree. As separate settings they would drift,
		// and drifting means silent oversubscription -- the exact failure this opt-in exists to stop.
		//
		//     EnableIoReactor(true, 4);  Init(0);   // hw-6: main, timer, 4 completion threads
		//
		// DEFAULT 1, AND NOT MEASURED. IOCP is built for many threads on one port and a busy server
		// will want more than one -- but the crossover has not been benchmarked here, and the honest
		// default is the one whose cost is known. Raise it against a profile, not a hunch; the port's
		// concurrency limit is set to match, so the kernel runs at most this many at once regardless.
		static void EnableIoReactor(bool on, unsigned completionThreads = 1) noexcept;
		static bool IoReactorEnabled() noexcept;
		static unsigned IoCompletionThreads() noexcept;

		// Give the timer thread a core of its own, so an app that uses deadlines does not run one
		// thread over the machine.
		//
		// WHY THIS IS NOT AUTOMATIC. The auto pool size is hw-1, and GetSafeTC's census calls that an
		// EXACT FIT: workers + main, nothing spare. TimerQueue adds a thread, so an app using
		// deadlines is oversubscribed by exactly one -- the deficit that cost a measured 3-4% the
		// last time it happened (see GetSafeTC's history note on GameInput). But the timer thread
		// only exists once something arms a deadline, and that is long after Init has sized the pool,
		// so the scheduler cannot detect it -- and reserving unconditionally would take a worker away
		// from every app that never arms one.
		//
		// So it is the app's declaration, like an explicit poolSize. Call it before Init:
		//
		//     TaskScheduler::SetReserveTimerCore(true);
		//     TaskScheduler::Init(0);          // hw-2 workers; main and the timer take the rest
		//
		// Ignored when an EXPLICIT poolSize is given -- an explicit size is the app's own arithmetic
		// and this must not silently second-guess it.
		//
		// A Development build complains the first time a deadline is armed without this set, because
		// the alternative is the failure mode that is hardest to notice: everything works, slightly
		// worse, forever.
		static void SetReserveTimerCore(bool reserve) noexcept;
		static bool ReserveTimerCore() noexcept;

		// The same, for IoReactor's completion thread. Both may be set; the pool loses one core per
		// service thread in use, so an app doing timed async I/O runs hw-3 workers.
		//
		// TWO FLAGS RATHER THAN A COUNT, because the question an app can answer is "do I use
		// deadlines" and "do I use async I/O" -- not "how many service threads does the library run",
		// which is the library's business and has changed twice already.
		static void SetReserveIoCore(bool reserve) noexcept;
		static bool ReserveIoCore() noexcept;


		// Fibers PER WORKER available to be held by a SUSPENDED task at once -- not a cap on tasks
		// in flight, since only a task that actually suspends holds one. Defaults to 64 standard +
		// 8 heavy per worker, same as always; call this before Init() to change the multiplier.
		// Total capacity is this times the pool's actual worker count, so a multiplier is what gets
		// configured here rather than an absolute total the caller would have to keep in sync with
		// an "auto" pool size it may not know yet.
		//
		// PRE-INIT ONLY, same contract as SetLazyTaskSlab and SetParallelForSerial above. Not
		// resizable after Init(): each fiber registers a permanent slot with the epoch manager and
		// the stack arena is one fixed allocation made at StartPool, so there is no live pool to
		// grow into even if this were called later -- it would just be ignored.
		//
		// This is the actual lever the exhaustion warning in Thread.cpp means when it says "raise
		// standardFiberCount" -- that used to name a local variable inside StartPool with no way to
		// reach it from outside the library. Call this before Init() instead.
		static void SetFiberBudget(size_t standardPerWorker, size_t heavyPerWorker);
		static size_t StandardFibersPerWorker();
		static size_t HeavyFibersPerWorker();

		GlobalFiberPool& GetGlobalPool();
		// NAMED events are for a BOUNDED, STATIC set of rendezvous points -- "physics_done",
		// "level_loaded", the handful of names your app knows at compile time. The registry is
		// find-or-insert and never evicts, which is correct for that use: it reaches N entries and
		// stays there.
		//
		// DO NOT MINT A NAME PER OPERATION. A key like "fence_" + counter grows the map without
		// bound, and since every GetEvent holds registryMtx across the lookup (and occasionally a
		// rehash of a huge map), the result is a lock convoy that PRESENTS EXACTLY LIKE A DEADLOCK
		// in a debugger -- several workers piled on one mutex with no visible owner. It takes an
		// hour or so of uptime to show up, so it will not appear in any short test.
		//
		// For a per-operation wait -- a GPU fence value, an IO completion, anything with a fresh
		// identity each time -- use WaitOnEventDirectArmed below instead. It takes a pooled
		// DirectEvent, touches no map and no global lock, and is the reason no eviction policy is
		// needed here: the unbounded case has its own API.
		Event& GetEvent(const std::string& name);
		// Event& overloads: no registry lookup, no global mutex, no string hash. Prefer these on any
		// path that waits on the SAME event repeatedly -- hoist GetEvent() to startup and keep the
		// reference. It stays valid for the process lifetime (the registry owns unique_ptr<Event>, so
		// a rehash moves the pointer not the object, and nothing erases entries).
		void WaitOnEvent(Event& ev);
		void WaitOnEvent(const std::string& eventName);

		// WaitOnEvent, but reports whether this task was cancelled while parked.
		//
		// DELIVERY IS AT THE WAKE, not at the cancel. Event::CancelWaiters marks the task through
		// the waiter index and WAKES it -- an Event may be signalled by a condition that never occurs,
		// so nothing is removed from it and its no-pop invariant holds. The task therefore stays
		// parked until the event actually fires; SignalAll resumes it as usual, and this returns
		// Cancelled instead of Ok.
		//
		// So a cancelled waiter is NOT woken early. If the event never fires it never returns --
		// but an uncancelled waiter on that event does not return either, so cancellation was never
		// what would have rescued it. Waking early would mean removing one waiter from a lock-free
		// stack whose links ARE the tasks, which is the thing Event's header comment forbids.
		//
		// Returns Ok for a task with no cancellation of any kind, which is every existing caller.
		[[nodiscard]] WaitResult WaitOnEventCancellable(Event& ev);
		// Like WaitOnEvent, but runs 'arm' AFTER this fiber is registered as a waiter and
		// marked parkable (WANTS_SUSPEND), and BEFORE it actually suspends. Use it to arm an
		// external wakeup (e.g. a GPU-fence completion callback that will SignalAll this
		// event) with no lost-wakeup race: any signal that fires once 'arm' has run is
		// guaranteed to find a registered, resumable waiter. Must be called from a fiber.
		void WaitOnEventArmed(Event& ev, const std::function<void()>& arm);
		void WaitOnEventArmed(const std::string& eventName, const std::function<void()>& arm);
		// Direct/handle variant of WaitOnEventArmed: no name, no registry, no global lock. Takes
		// a pooled DirectEvent and hands its pointer to 'arm' so the external signaler can wake
		// this fiber via DirectEvent::Signal() with a direct pointer. Preferred for the common
		// "signaler shares context with the waiter" case (fences, IO). Must be called on a fiber.
		void WaitOnEventDirectArmed(const std::function<void(DirectEvent*)>& arm);
		// True if the caller is currently a worker running a task on a fiber (so it is safe
		// to WaitOnEvent / WaitOnEventArmed). False on the main thread or any non-worker.
		bool IsOnFiber();
		void Pause();
		void Resume();
		void Stop(Task* worker_task);
		TaskAllocator* GetAllocator();

		// WaitAll() was REMOVED in 1.3.0. It spun on a global `pendingTasks` atomic that every push
		// and every completion had to maintain -- 24 ns per task in contention, 27% of the whole
		// per-task cost, to serve one method with no callers anywhere. Wait on a WaitGroup instead,
		// which is scoped to the work you actually submitted; "all work everywhere" is not a
		// question a caller can ask meaningfully once more than one system shares the pool.

		// Lets a non-worker caller (e.g. main, while spinning on a WaitGroup/counter) safely
		// help drain the pool instead of pure-spinning. Steals ONE Native task via GetTask(), which
		// vets the task's TaskType AT THE DEQUE (TaskDeque::steal_if) -- a fiber-backed task is never claimed
		// by this fiberless caller at all (it could suspend, and there's no fiber to switch away
		// to), so it stays queued for a real worker. This replaced the old steal-then-Requeue
		// relocation, which was pure contention churn (claim CAS + re-push + notify, task moved
		// nowhere). On a successful steal: runs Execute() inline, then frees with the EXACT SAME
		// sequence Worker()'s fast path uses (DestroyTask(), Free(), EBR tick check) -- required so
		// the slab stays correct; skipping either of these leaks a slab slot.
		// Returns true if it ran a task, false if nothing stealable -- callers should yield()
		// on false to avoid a hot spin.
		bool TryRunStolenNativeTask();

		// Steal-time class compatibility (used with TaskDeque::steal_if -- vet BEFORE claiming, never
		// steal-then-Requeue, which is pure deque contention + worker thrash). Placement policy at steal
		// mirrors push: Default/Any/Wide tasks are stealable by EVERY worker (work-conserving for the
		// common case); explicit P/E tasks are only stolen by a matching-class thief -- corePref is the
		// sole placement authority, including across steals. degenerateTopology (non-hybrid CPU: one
		// class set empty) disables class checks entirely so nothing is ever unstealable.
		// Takes the PREFERENCE, not the task. Steal vetting runs before the thief has claimed
		// anything, so it must not dereference the candidate -- the value arrives in the deque's
		// pointer tag instead (see TaskDeque's StealBits).
		static bool StealClassCompatible(CorePref p, bool thiefIsP, bool degenerateTopology) {
			if (p == CorePref::Default || p == CorePref::Wide) return true;   // Any aliases Wide
			if (degenerateTopology) return true;
			return (p == CorePref::P) ? thiefIsP : !thiefIsP;
		}
		// Convenience for callers that legitimately HOLD the task (owner-side placement checks),
		// where dereferencing is fine. Steal paths must use the CorePref overload.
		static bool StealClassCompatible(const Task* t, bool thiefIsP, bool degenerateTopology) {
			return StealClassCompatible(t->corePref, thiefIsP, degenerateTopology);
		}

		Task* CreateTask(void(*fn)(void*), void* data, uint8_t hipri = false, FiberSize size = FiberSize::Standard, TaskType type = TaskType::Native, CorePref corePref = CorePref::Default);

		template<typename F>
		auto CreateTask(F&& f, uint8_t hipri = false, FiberSize size = FiberSize::Standard, TaskType type = TaskType::Native, CorePref corePref = CorePref::Default) {
			using L = LambdaTask<std::decay_t<F>>;
			static_assert(sizeof(L) <= TaskAllocator::SLOT, "lambda too big for a slot");
			// Concrete size is a compile-time constant here, which is also why size-classing the
			// task path would be nearly free: the class could be picked at compile time.
			detail::RecordTaskSize(sizeof(L));
			static_assert(alignof(L) <= 16, "lambda over-aligned for the slot");

			// sizeof(L) is a compile-time constant, so the class is chosen at compile time and is correct
			// for ANY capture size -- a big lambda still lands in the 256-byte class. Nothing here
			// depends on knowing the size distribution in advance.
			void* mem = taskAllocator.AllocSized(sizeof(L));
			if (!mem) return static_cast<L*>(nullptr);
			L* t = ::new (mem) L(std::forward<F>(f));
 			t->hiPri = hipri;
			t->requiredSize = size;
			t->type = type;
			t->corePref = corePref;
			// ~LambdaTask is empty and its only member is the functor, so the destructor has work
			// to do only when the CAPTURES do. Non-capturing lambdas and captures of scalars or
			// raw pointers -- the overwhelming majority of task bodies -- skip the virtual call.
			t->trivialDtor = std::is_trivially_destructible_v<std::decay_t<F>> ? 1 : 0;
			return t;
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(uint8_t cpu_affinity, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t, cpu_affinity);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void PushImmediate(size_t coreID, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushToCore(coreID, t);
		}

	private:
		explicit TaskScheduler(size_t poolSize);

		// ---------- former SharedQueues state ----------
		// (`nextId` removed in 1.3.4: a process-wide atomic counter whose only reader,
		//  Thread::GenerateID(), had no callers anywhere. Never executed, so it cost no time -- but
		//  it sat in the shared struct implying task IDs existed. Nothing assigns or reads one.)
		std::vector<std::unique_ptr<std::atomic<bool>>> immediateCoresInUse;
		std::atomic<bool> paused{ false };
		std::vector<std::unique_ptr<TaskDeque>> loPri;
		std::vector<std::unique_ptr<TaskDeque>> hiPri;

		// ---------- the NON-WORKER LANE ----------
		// loPri/hiPri carry ONE EXTRA deque pair past the workers, at index `nonWorkerLane`
		// (== workers.size(), fixed by StartPool). Everything else -- inboxes,
		// immediateCoresInUse, the P/E sets, PickNextWorker -- stays worker-indexed.
		//
		// WHY IT EXISTS. Demand-driven splitting (ParallelFor since 1.4) works by publishing a split onto
		// the SPLITTER'S OWN deque and taking it straight back if nobody stole it -- measured at
		// 17.8 ns, versus ~85-105 ns for a real cross-thread dispatch. A worker has a deque to do
		// that with. Main did not, so a ParallelFor called from the main thread -- which is the
		// normal case -- had nowhere to publish, and the alternative was to hand the whole range to
		// one worker and let the tree fan out over log2(N) STEAL HOPS before the pool filled up.
		// One deque removes that ramp entirely: every worker can steal directly from main's lane,
		// in parallel, starting from the first split.
		//
		// SINGLE OWNER, ENFORCED. A Chase-Lev deque has exactly one pusher/popper by construction,
		// so two non-worker threads splitting at once would corrupt it. `nonWorkerLaneClaimed` is
		// the claim; a caller that loses it falls back to the cursor path rather than waiting, so
		// a second non-worker thread degrades to the 1.4 behaviour instead of blocking.
		//
		// Thieves treat it as one more victim and nothing else: it is probed after the topology
		// phases (it has no cache locality to anybody) and before the global random fallback.
		size_t nonWorkerLane = 0;
		std::atomic<bool> nonWorkerLaneClaimed{ false };

		// Everything a lazy split needs that does NOT change as the recursion descends. Held on the
		// root caller's stack and passed down by pointer, which is sound for exactly the reason
		// the cursor path is: ParallelFor BLOCKS, so the frame outlives every task
		// spawned under it. A non-blocking variant would need this refcounted on the heap.
		struct LazyRangeState {
			std::function<void(int, int)>* func;
			WaitGroup* wg;
			int grain;
		};
		// The deque the CALLING thread may publish onto, or nullptr if it has none. Resolved per
		// invocation and never cached across one: a split that gets stolen resumes on a different
		// thread, and it must then publish onto THAT thread's deque, not the one it came from.
		TaskDeque* LaneForCurrentThread();
		size_t LaneIndexForCurrentThread();
		void RunLazyRange(int lo, int hi, LazyRangeState* st);
		std::vector<std::unique_ptr<TaskMPSCQueue>> loPriInboxes;
		std::vector<std::unique_ptr<TaskMPSCQueue>> hiPriInboxes;
		static GlobalFiberPool* globalPool;
		// -----------------------------------------------

		// ---- loPri starvation prevention: steal fairness ----
		// After kStealFairnessWindow consecutive hiPri steals, GetTask() forces a loPri scan so a
		// steady stream of hiPri work can't starve loPri tasks. (There used to also be age-based
		// promotion -- boost old loPri tasks to hiPri -- but it's redundant now that stealing is
		// single-item: a stolen task runs immediately, so the steal itself un-starves it.)
		//
		// PER THREAD, NOT PER SCHEDULER -- see the definition in TaskScheduler.cpp. This was a plain
		// `int` member on the singleton, read-modify-written by GetTask() from every thread that
		// spin-helps, which is a genuine data race and ThreadSanitizer says so (2026-08-17). It is
		// also the wrong shape for what the counter means: the window is a property of ONE
		// stealer's recent history, and sharing it made N concurrent helpers trip it N times faster
		// than intended. Thread.cpp's `consecutiveMisses` backoff counter is thread_local for
		// exactly these two reasons and this now matches it.
		static thread_local int consecutiveHiPriSteals;
		static constexpr int kStealFairnessWindow = 8; // after 8 hiPri steals, force a loPri scan
		uint64_t GetCurrentTimeMs() const;
		// ----

		// ---- Priority inheritance for locks (prevent inversion deadlock) ----
		// Priority boosts now stored directly on Task.priorityBoost (no lock needed)
		// ----


		void RunCounted(WaitGroup& wg, Task* t);
		static size_t GetSafeTC();
		// Steals ONE task (hiPri-then-loPri, with steal fairness) for a non-worker helper. nullptr
		// if nothing stealable. See definition.
		Task* GetTask();
		void StartPool(size_t poolSize);
		bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
		// `hiPri` selects WHICH SET is rotated, and that is what makes the lane invariant structural
		// rather than a convention every call site has to remember. A hiPri task rotates the hot
		// workers only; everything else rotates the ordinary ones only. One branch, one place, and
		// no caller can route a lane task somewhere nothing serves it.
		int PickNextWorker(CorePref pref = CorePref::Default, bool hiPri = false);
		bool PushToCore(size_t core_id, Task* task);
		// Picks a worker from the requested class set (P/E), SPILLING to the other class if unavailable;
		// Default/Any/Wide (and non-hybrid / all-pinned) use the original full-pool round-robin. Placement
		// is governed SOLELY by CorePref -- hiPri is queue order only, never consulted for placement.
		// Preference is a hint -- never a constraint.

		// NOTE: an external-submitter fan-out cap was tried here and REMOVED. See CHANGELOG 1.1.1.
		// It made single-producer submission much faster and burst parallelism much worse, and it
		// could livelock: PushLocal spins on `while (immediateCoresInUse[chosen])` with no yield and
		// no widening, which is safe with thirty-one candidates and is not with four. If you are
		// tempted to reintroduce it, the useful version is preferring workers that are already
		// AWAKE, not capping how many exist.

		// ---------- topology-aware steal biasing ----------
		// Queried ONCE at StartPool() time via GetLogicalProcessorInformationEx -- real
		// hardware topology, not an assumption from the sequential affinity scheme (worker
		// qIndex i is pinned to logical CPU i+1, main sits on logical CPU 0; that mapping alone
		// doesn't tell you which logical CPUs actually share a core or an LLC on THIS machine).
		void BuildTopology(unsigned int num_workers);
		// clusterMates[qIndex] -- other worker qIndexes sharing this worker's last-level cache
		// domain, EXCLUDING its direct SMT sibling (that's handled separately below, since it
		// needs the extra "only if idle" check). Tried first, in random order, before falling
		// back to the existing global-random steal.
		std::vector<std::vector<int>> clusterMates;
		// Class-split views of clusterMates (built in BuildTopology once isPCore is known): a thief
		// probes victims IT CAN STEAL FROM first -- same-class mates, then the idle SMT sibling (same
		// physical core = same class by construction), then foreign-class mates as the last local
		// phase, then a global random fallback (also same-class-first). With every task at Default
		// this only reorders identical coverage; with class-pinned work it kills futile decline
		// probes at the source (scan order matches steal legality) and raises steal hit rate.
		// Non-hybrid: matesOtherClass is empty everywhere -> behavior identical to the classic order.
		std::vector<std::vector<int>> matesSameClass, matesOtherClass;
		// siblingQIndex[qIndex] -- the OTHER worker qIndex sharing this worker's physical core
		// (SMT sibling), or -1 if none (no SMT, or the sibling logical CPU isn't a pool worker
		// -- e.g. it's main's). Only stolen from if idle (see Thread::busy) -- a busy SMT
		// sibling shares this worker's execution ports, so stealing its work doesn't recruit
		// any additional throughput, just adds queued work to an already-contended core.
		std::vector<int> siblingQIndex;

		// llcMaskOfWorker[qIndex] -- the set of logical CPUs sharing this worker's last-level cache,
		// as a bitmask. 0 if topology was unavailable. POSIX ONLY in effect: it exists so the Ideal
		// policy can bind a worker to its whole LLC DOMAIN rather than to one core.
		//
		// Windows expresses "keep locality without rigidity" through SetThreadIdealProcessor, a hint.
		// Linux has no hint -- affinity is all or nothing per CPU -- but sched_setaffinity takes a
		// MASK, so the same intent is expressed by binding to the domain instead of the core. The
		// kernel may then place the thread on any core in that domain, so a wake lands on one that
		// is already awake instead of waiting for a specific parked core, which is where hard
		// pinning's ~45% wake-latency cost came from.
		//
		// The mask is exactly as tight as the hardware warrants. On MULTI-L3 parts (Ryzen CCDs,
		// Threadripper, EPYC) it genuinely binds, which is where it matters -- migrating across
		// cache domains costs inter-die latency per steal and makes clusterMates describe a machine
		// that does not exist. On SINGLE-L3 parts the domain is every CPU, so it binds nothing:
		// correct rather than missing, since there is no domain to protect and binding would only
		// add the rigidity that measured ~45% worse. UNMEASURED on real Linux hardware -- WSL
		// virtualises topology and has no real core parking.
		std::vector<topology::CpuMask> llcMaskOfWorker;
		// isPCore[qIndex] -- 1 if this worker is pinned to a PERFORMANCE core, 0 for an EFFICIENCY core
		// (Intel hybrid, e.g. i9-13900K = 8 P + 16 E). Derived in BuildTopology from each core's
		// PROCESSOR_RELATIONSHIP.EfficiencyClass (highest class present = P). Non-hybrid CPU -> all 1
		// (harmless). DATA ONLY for now: nothing schedules on it yet. Foundation for P/E-aware routing
		// (hiPri->P, loPri/bulk->E) + a P/E-aware core reserve -- needed manually because the pool is
		// HARD-PINNED (Thread::StartWorker SetThreadAffinityMask), so the OS can't place work P/E for us.
		std::vector<char> isPCore;
		// isPCpu[logical CPU] -- P/E class of every logical processor, same EfficiencyClass
		// derivation as isPCore but indexed by CPU, not worker. Needed because TryRunStolenNativeTask's
		// callers include NON-worker, possibly UNPINNED threads (main, or any app thread hitting a
		// SchedulerMutex/SchedulerConditionVariable spin): their class can't be assumed -- it's looked
		// up via GetCurrentProcessorNumber() at steal time ("this Native task would run HERE, right now").
		// Workers keep the cheaper static isPCore[qIndex] lookup (hard-pinned, class never changes).
		std::vector<char> isPCpu;
		// -----------------------------------------------

		static TaskScheduler* instance;
		// 1M tasks by default; see SetSlabSizes to change it. Eager unless SetLazyTaskSlab(true)
		// was called before Init -- see that setter for why the default is the expensive one.
		// Built from the per-class sizes -- see SlabSizes.
		TaskAllocator taskAllocator{ CurrentSlabSizes().slots256, CurrentSlabSizes().slots128, CurrentSlabSizes().slots80,
		                             CurrentSlabSizes().slots64, LazyTaskSlabEnabled() };

	public:
		// ABI LAYOUT CANARY. Not used by anything at runtime; its OFFSET is the payload.
		//
		// It sits immediately after taskAllocator, so it moves whenever any member above it is
		// added, removed, resized or reordered -- which is exactly the change class that the
		// stale-library guard needs to see and that sizeof() cannot report. 1.4 shifted
		// taskAllocator by 8 bytes with sizeof(TaskScheduler) pinned at 1664, and the guard matched
		// straight through it.
		//
		// It is PUBLIC so the guard can take offsetof() from a free function with no access to this
		// class. The alternative -- a member function that reports its own offsets -- cannot work:
		// it would have external linkage, the linker would fold the copies from the library and the
		// application into one, and both sides would then read the SAME layout and always agree.
		// A guard that compares a value against itself is worse than no guard, because it reports
		// success.
		struct AbiCanary { char unused; };
		AbiCanary abiCanary{};

	private:
		std::unordered_map<std::string, std::unique_ptr<Event>> eventRegistry;
		std::mutex registryMtx;
		EventPool eventPool{ 1024 };   // pooled DirectEvents for WaitOnEventDirectArmed
		std::atomic<bool> poolActive{ false };
		// TWO STEAL HINTS, because the pool holds two kinds of queue content with OPPOSITE
		// requirements, and one bit cannot mean both.
		//
		//   BACKLOG      "this worker has substantial excess work." Stealing it is an OPTIMISATION
		//                and only pays when the owner is demonstrably behind, since migration costs
		//                a cache miss on the task's working set. Threshold-maintained by the owner,
		//                so it is written only on crossings -- almost never for a queue that stays
		//                deep or stays shallow.
		//   PARALLELISM  "this work was deliberately produced for parallel execution." Stealing it
		//                is the LOAD-BALANCING MECHANISM, not an optimisation -- ParallelFor has no
		//                cost model and steals are what divide the range. A single split must be
		//                visible the instant it exists.
		//
		//   stealable = backlog || parallelism
		//
		// WHY BOTH ARE NEEDED, measured: a backlog-only hint cut remote probes 46x (9,164 -> 200,
		// every probe productive) and simultaneously took ParallelFor from 7.49x to 0.93x -- SLOWER
		// THAN SERIAL -- because shallow splits never reach any threshold. Thresholds of 2, 4, 8 and
		// 16 all did it. Threshold 1 is not a fix either: that is "any non-empty queue", which
		// summons every idle thief onto one deque for ONE task and measured 22,000 probes against a
		// 9,164 baseline, with the owner losing races for work it was about to run warm.
		//
		// The herd is correct exactly when there is work for a herd. PARALLELISM says so; BACKLOG
		// says so; a bare non-empty queue does not.
		static constexpr size_t kStealHintDepth = 8;

		std::atomic<unsigned long long> stealHintBacklog{ 0 };
		std::atomic<unsigned long long> stealHintParallel{ 0 };

		// Owner-maintained, on push and pop. Writes only on a threshold crossing.
		void UpdateBacklogHint(size_t q, size_t depth) noexcept {
			if (q >= 64) return;
			const unsigned long long bit = 1ull << q;
			const bool want = depth >= kStealHintDepth;
			if (want == ((stealHintBacklog.load(std::memory_order_relaxed) & bit) != 0)) return;
			if (want) stealHintBacklog.fetch_or(bit, std::memory_order_release);
			else      stealHintBacklog.fetch_and(~bit, std::memory_order_relaxed);
		}
		// Set by the splitter when it publishes; cleared by the owner when its lane drains. Held
		// conservatively -- while ANY work remains the lane stays advertised, which costs a probe
		// and can never hide a split.
		void SetParallelHint(size_t q) noexcept {
			if (q < 64) stealHintParallel.fetch_or(1ull << q, std::memory_order_release);
		}
		void ClearParallelHintIfEmpty(size_t q, size_t depth) noexcept {
			if (q >= 64 || depth != 0) return;
			stealHintParallel.fetch_and(~(1ull << q), std::memory_order_relaxed);
		}

		// Runtime kill switch, so the hint`s value can be measured against an A/A control inside one
		// process. Three separately-built binaries measured in three sessions is how a 2x machine
		// drift once got read as a result.
		bool MaybeStealable(size_t q) const noexcept {
			if (q >= 64 || loPri.size() > 64) return true;
			if (!stealHintOn.load(std::memory_order_relaxed)) return true;
			const unsigned long long bit = 1ull << q;
			return ((stealHintBacklog.load(std::memory_order_acquire)
			       | stealHintParallel.load(std::memory_order_acquire)) & bit) != 0;
		}

		// ---- THE LANE'S OWN BACKLOG SIGNAL, and why it is not the one above -------------------
		//
		// The two hints above are OWNER-maintained: the worker updates them once per pass through
		// its loop. That works for bulk work and fails for the lane, in exactly the case the lane
		// cares about. MEASURED: with one in eight completions running a 200us handler, a hot worker
		// spends 62-65% of its idle passes next to a SIBLING sitting on a mean depth of 8-13 -- and
		// during those 200us the buried worker is not looping, so an owner-maintained bit would stay
		// unset precisely when it is needed. Whoever advertises the backlog must be running, and the
		// only party guaranteed to be running is the one doing the pushing.
		//
		// So: an EXACT count of lane tasks outstanding at each worker, incremented by the publisher
		// and decremented by whoever removes one (the owner popping, or a thief stealing). The
		// counter is per-worker and on its own line; the BIT derived from it is what thieves read,
		// so a spinning hot worker touches one shared word rather than K contended counters.
		//
		// THRESHOLD, not "non-empty". Steering placed that task on that worker deliberately, and a
		// hot worker about to run its own warm task must not lose it to a sibling -- that is the
		// v1 steal-hint failure (owner loses the race, 22,000 probes against a 9,164 baseline) with
		// higher stakes, because here it also defeats the placement. Four is above the depth a
		// keeping-up worker reaches and well below the 8-13 measured when one is buried.
		static constexpr int kLaneStealDepth = 4;

		std::atomic<unsigned long long> stealHintLane{ 0 };

		// MAINTAINED BY THE OWNER AT ITS DRAIN, and the distinction from "owner maintained per pass"
		// is the whole reason this works. The buried worker is not looping during its 200us handler
		// -- but it drained its inbox into its deque IMMEDIATELY BEFORE starting that handler, and
		// at that moment it knew the depth. So the backlog is advertised before the worker goes
		// dark, and stays advertised for exactly as long as it is buried.
		//
		// The publisher-side exact counter this replaces worked too, and cost p50 ~30% ACROSS THE
		// BOARD -- including the uniform control where stealing almost never fires, which is what
		// identified it as accounting rather than mechanism. One atomic RMW per lane push, on a line
		// shared between the completion thread and the worker it steers to, on the hot path. Same
		// shape as the unsharded coroutine counter that once cost 62%. This version writes only on
		// threshold crossings, and only from a thread that already owns the line.
		//
		// The gap it accepts: tasks arriving DURING the handler sit in the inbox unadvertised. Those
		// are the ones a publisher-side counter would have caught, and giving them up is what buys
		// back the p50.
		// DIAGNOSTIC ARM SELECTOR, so the three candidate designs run from ONE binary and can be
		// interleaved. Comparing them as three separately-built executables measured in three
		// sessions produced a 2x swing in the K=1 rows -- a configuration with no sibling, where
		// hot->hot stealing provably cannot act -- which is machine drift, not a result. The branch
		// costs the same in every arm, which is the property that matters here.
		//
		//   0  no lane hint, no hot->hot stealing (the shipped behaviour before this)
		//   1  hint maintained at the DRAIN only, from a local count
		//   2  hint maintained per pickup, from hiPri->size()  (touches the thief-written line)

		void UpdateLaneHint(size_t w, size_t depth) noexcept {
			if (w >= 64) return;
			const unsigned long long bit = 1ull << w;
			const bool want = depth >= (size_t)kLaneStealDepth;
			if (want == ((stealHintLane.load(std::memory_order_relaxed) & bit) != 0)) return;
			if (want) stealHintLane.fetch_or(bit, std::memory_order_release);
			else      stealHintLane.fetch_and(~bit, std::memory_order_relaxed);
		}
		bool LaneStealable(size_t w) const noexcept {
			if (w >= 64) return false;
			return (stealHintLane.load(std::memory_order_acquire) & (1ull << w)) != 0;
		}

		std::atomic<int> nextWorker{ 0 };
		// Separate cursor for the hot set, so lane traffic and ordinary traffic do not perturb each
		// other's rotation -- and so the hot rotation stays over 0..K-1 rather than the whole pool.
		std::atomic<size_t> nextHotWorker{ 0 };
		// P/E routing (see PickNextWorker): worker qIndices split by efficiency class (from isPCore),
		// each with its own round-robin cursor. Built in StartPool. Preference is a HINT -- PickNextWorker
		// spills to the other class if the preferred one has no available worker, and an empty set (non-
		// hybrid CPU) just falls through to the full pool. Nothing here is a hard constraint.
		std::vector<int> pWorkers, eWorkers;
		std::atomic<size_t> nextPWorker{ 0 }, nextEWorker{ 0 };
		std::atomic<bool> stopFlag{ false };
		std::vector<std::shared_ptr<Thread>> workers;
		TaskMPSCQueue mainQ;
		std::mutex poolMutex;
	};
#if defined(JLIBSCHED_TUNABLE_FAST_SPIN)
	// PRESENT ONLY IN A DIAGNOSTIC BUILD (-DJLIBSCHED_TUNABLE_FAST_SPIN=ON). Makes the bare-thread
	// fast-spin bound settable at runtime so bench/lock_contention.cpp can rotate through candidate
	// values inside ONE process. It has to be one process: as a pure compile-time constant each
	// value is a separate binary, and an A/A control showed process-to-process drift far larger than
	// any effect the constant could have. Not a scheduler setting and not available in a normal
	// build -- see kFastSpinTries in src/TaskScheduler.cpp.
	namespace detail {
		void SetFastSpinTries(int n);
		int  GetFastSpinTries();
	}
#endif

	// Fiber-aware mutex. A blocked fiber SUSPENDS (freeing its worker); a blocked bare thread runs
	// stolen Native work instead of spinning.
	//
	// IT NO LONGER INHERITS PRIORITY, and the comment here claimed it did for a month. Boosting the
	// holder on contention was added in 8555cbd ("implemented starvation prevention", 2026-07-15)
	// and its single call site was removed five days later in 21719ac, the rewrite that turned this
	// from a spinlock into the suspend-or-help lock described above. Both functions are now DELETED
	// (see the note where they lived in TaskScheduler.cpp): Boost had no callers, which made
	// `priorityBoost` permanently 0 and Unboost a permanent no-op -- and once hiPri became the
	// low-latency lane, a mechanism that promotes an aged ORDINARY task into it would have pushed
	// bulk work onto the hot workers, which is precisely what the lane excludes. The packed
	// `priorityBoost` bit is left in place so Task's layout fingerprint is unchanged.
	//
	// That removal was correct, and it is worth knowing WHY so nobody re-adds the boost as a fix for
	// a hang it cannot cause. Classic priority inversion needs a high-priority waiter to starve the
	// holder of CPU. Nothing here can: `hiPri` is QUEUE ORDER ONLY -- never OS thread priority,
	// never placement -- every worker runs at the same OS priority, and a task that has already
	// STARTED owns its worker until it yields or finishes, so no amount of hiPri work can deschedule
	// a running lock holder. What made inversion real in the old design was that waiters SPUN, so
	// they genuinely competed with the holder for a core; suspending and helping both removed that.
	// The one residual case -- the holder is a suspended fiber whose resume sits in a loPri queue
	// while hiPri work floods in -- is bounded by kStealFairnessWindow, which forces a loPri scan
	// every 8 consecutive hiPri steals. Same shape as the age-based promotion that was removed once
	// single-item stealing made it redundant: a mitigation outliving its premise.
	// One queued waiter on a SchedulerMutex or SchedulerSemaphore. EXACTLY ONE of the two pointers is
	// non-null, and which one decides how the waiter is released:
	//
	//   fiber != nullptr   a suspended fiber        -> Thread::Resume(fiber)
	//   coro  != nullptr   a suspended coroutine    -> TaskScheduler::Push(coro), which re-runs its
	//                                                  resume trampoline on some worker
	//
	// BARE THREADS ARE NOT HERE, deliberately. A blocked bare thread cannot suspend, so it never
	// queues at all -- it spins on Try_Lock/Try_Wait running stolen work (see ContendedSpinStep) and
	// finds the lock free on its own. Only contexts that can actually park need waking.
	//
	// THIS IS THE WHOLE COST OF SUPERSETTING. Everything else about these primitives is unchanged:
	// the release path pops a waiter as it always did and branches on which kind it is. Note that
	// nothing here needs C++20 -- a coroutine waiter is just a Task* and re-arming it is a Push --
	// so this stays in the C++17 core, and only the `co_await` spelling lives in Coroutine.h.
	//
	// LIFETIME, same invariant the condition variable already relies on: a Waiter is held by VALUE in
	// the queue, so it does not point into a suspended stack. The Task*/Fiber* it carries outlive the
	// wait because a waiter cannot leave before it is released, and every release path removes the
	// entry from the queue BEFORE resuming or pushing it.
	// THE one place cancellation is decided. A task is cancelled if EITHER its scope was cancelled
	// or it was cancelled individually (Event::CancelWaiters). Every observation point -- worker
	// pickup, CurrentTaskCancelled, anything added later -- must go through here, so a new way of
	// being cancelled reaches all of them rather than the one its author happened to be editing.
	inline bool IsTaskCancelled(const Task* t) {
		if (!t) return false;
		if (t->cancelledDirect) return true;
		return CancelToken(t->cancelToken).Cancelled();
	}

	// Has the currently-running task's scope been cancelled?
	//
	// THE ONLY CANCELLATION CHECK A NATIVE TASK GETS, and that is by necessity rather than design: a
	// Native task has no suspension points, so there is nowhere for the runtime to deliver the news.
	// Call it wherever a long body can usefully give up:
	//
	//     for (auto& chunk : work) {
	//         if (JLib::CurrentTaskCancelled()) return;   // RAII unwinds as usual
	//         Process(chunk);
	//     }
	//
	// Fibers and coroutines will also observe cancellation at their suspension points -- that half is
	// not built yet, see CancelToken.h -- but polling stays available to all three and is the right
	// tool for a compute loop that never suspends.
	//
	// Returns false off a task entirely, and false for a task whose scope has been destroyed:
	// unscoped work is not cancelled work. One load through a table; no lock.
	bool CurrentTaskCancelled();

	struct Waiter {
		Fiber* fiber = nullptr;
		Task*  coro  = nullptr;

		// SKIP-AT-RELEASE. A cancellable waiter carries a pointer to a result slot it owns -- a
		// local on the suspended fiber's stack, or a member of the awaiter inside a coroutine
		// frame. Both are stable precisely because a waiter cannot leave before it is resumed, and
		// resuming it is what lets it leave.
		//
		// A NULL SLOT MEANS "NOT CANCELLABLE", and that is the safety property, not an oversight.
		// Plain Lock()/Wait() pass null and are never skipped, so they cannot come back without the
		// thing they asked for. If cancellation applied to them, a caller who ignored the result
		// would proceed believing it held a lock it does not hold -- silent, and far worse than
		// waiting. Cancellation is opt-in per call site for exactly that reason.
		WaitResult* result = nullptr;
		uint32_t    token  = 0xFFFFFFFFu;   // CancelToken::kNone
	};

	class SchedulerMutex {
	private:
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;
		bool locked = false;
		Task* lockHolder = nullptr;
		std::queue<Waiter> waiters;   // fibers AND coroutines; see Waiter
		std::atomic_flag holderLock = ATOMIC_FLAG_INIT;

	public:
		SchedulerMutex() = default;
		~SchedulerMutex() = default;

		// Acquires the lock. (No priority boost happens on contention despite what this line used to
		// say -- see the class comment above for when that stopped being true and why it is fine.)
		//
		// Callable from EITHER context, and the two behave differently on contention:
		//   on a fiber      -- the fiber is queued and SUSPENDED, freeing the worker for other work.
		//   on a bare thread -- there is no fiber to suspend, so it spins, running one stolen
		//                       Native task per iteration (TryRunStolenNativeTask) instead of
		//                       burning the cycles.
		//
		// That second path is why this is NOT the right lock everywhere, and the reason is stronger
		// than latency. Running stolen tasks inside the acquisition loop makes locking REENTRANT:
		// user code executes while you are blocked, and it can take locks of its own. Three guards
		// bound that (see ContendedSpinStep in TaskScheduler.cpp), the important one being that a
		// bare thread ALREADY HOLDING a SchedulerMutex stops helping entirely -- otherwise a helped
		// task asking for the lock you hold deadlocks you against yourself, with no fiber involved.
		//
		// What is still true: a caller that must return promptly, or that is not one of ours at all
		// (a driver thread-pool callback, say), will run tasks from the graph while it waits. For a
		// short critical section reached from foreign threads, a plain std::mutex is both cheaper
		// and safer; see the note in Event.h.
		//
		// Also unguarded by design: holding a SEMAPHORE permit. Permits have no owner -- the thread
		// that takes one is often not the one that returns it -- so there is nothing sound to count.
		void Lock();

		void Unlock();

		// Non-blocking try_lock
		bool Try_Lock();

		// Lock(), but observes the calling task's cancellation scope. Returns Cancelled if the
		// scope was cancelled while waiting -- IN WHICH CASE THE LOCK IS NOT HELD and Unlock() must
		// not be called.
		//
		// DELIVERY IS SKIP-AT-RELEASE, and the delay is the deliberate part of the design. A
		// cancelled waiter is not plucked out of the queue -- removing one entry would need the
		// queue to support arbitrary removal and the canceller to know which lock to look in. It is
		// skipped when the lock is NEXT RELEASED: Unlock walks past it, resumes it with Cancelled so
		// it can unwind, and hands ownership to the first waiter that is still interested.
		//
		// So cancellation here is bounded by how long the current holder keeps the lock, not
		// immediate. That is acceptable because a lock nobody releases hangs an uncancelled waiter
		// just as thoroughly; it is not a case cancellation was going to rescue. Waking a parked
		// waiter the instant Cancel() is called needs a scope->waiter registry, which costs more
		// than it currently buys.
		//
		// NEVER PUT CLEANUP BEHIND THIS SPELLING. A mutex is the primitive most likely to guard the
		// very state an unwind has to touch -- a pool to return a block to, a registry to remove
		// yourself from -- and an unwind path runs BECAUSE the scope was cancelled. So the token is
		// already cancelled by the time it gets there, the pre-check above fires, and this returns
		// Cancelled WITHOUT EVEN LOOKING AT THE LOCK. Not a race that leaks sometimes: the lock can
		// be sitting free and it still returns Cancelled, every single time, and the release never
		// happens.
		//
		// The rule that follows: cancellable spellings are for the PRODUCTIVE work you are
		// abandoning. The unwind that abandoning it requires uses plain Lock(), which cannot fail --
		// cancellation is what put you on that path, so it must not also be able to block your exit
		// from it.
		//
		// AND IF YOU DO WANT AN EAGERLY CANCELLABLE LOCK, YOU ALREADY HAVE ONE: SchedulerSemaphore
		// sem(1, 1). WaitCancellable() acquires, Signal() releases, CancelWaiters(tok) ejects a
		// waiter immediately, and maxPermits=1 clamps a double release instead of inventing a second
		// permit. That is why there is no eager cancel here and no second lock type -- it would be a
		// worse copy of a primitive that already exists.
		//
		// The trade is OWNERSHIP, and it is the whole difference: a permit has no owner (see Lock's
		// note on why holding one is unguarded), so a binary semaphore gives up the holder record,
		// the deadlock diagnostics built on it, and the guarantee that whoever releases is whoever
		// acquired. Take the semaphore when the wait may be unbounded and abandonment is a real
		// outcome; take the mutex when you want the lock to be owned.
		//
		// A BARE THREAD checks the token between spins instead, since it never enqueues.
		[[nodiscard]] WaitResult LockCancellable();

		// COROUTINE PATH. Do not call this directly -- use `co_await JLib::LockAsync(m)` from
		// Coroutine.h, which is the only correct way to reach it.
		//
		// Acquires the lock if it is free, and otherwise queues `coroTask` to be re-pushed when the
		// lock is released. Returns TRUE if the lock was taken (the caller must NOT suspend) and
		// FALSE if the task was queued (the caller must stay suspended).
		//
		// The test and the enqueue happen under the SAME spinLock acquisition, and that is the entire
		// reason this is one call rather than a Try_Lock followed by an enqueue. Split them and a
		// release landing in the gap finds an empty queue, sets locked = false and wakes nobody --
		// after which this task queues onto a free lock and waits for an Unlock that will never come.
		// Same lost-wakeup shape as the 1.3.5 bug, reached a different way.
		//
		// Once this returns false the task is visible to Unlock, so the caller must touch neither the
		// task nor its coroutine handle again: it may already be running on another worker.
		bool LockAsyncEnqueue(Task* coroTask);

		// Cancellable form of the above, for `co_await LockAsyncCancellable(m)`. `result` is a slot
		// inside the awaiter (and therefore inside the coroutine frame), which survives the
		// suspension for the same reason a fiber's stack local does.
		bool LockAsyncEnqueue(Task* coroTask, WaitResult* result);
	};

	class SchedulerSemaphore {
	private:
		std::mutex mtx;
		std::queue<Waiter> waiters;        // suspended fibers AND coroutines; see Waiter
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT; // Must be here!
		int permits;
		const int maxPermits;

	public:

		explicit SchedulerSemaphore(int initialPermits, int maxPermits = INT_MAX)
			: permits(initialPermits), maxPermits(maxPermits) {}

		void Wait();

		// Cancellable Wait. Returns Cancelled if this task's scope was cancelled while parked, and
		// in that case NO PERMIT WAS TAKEN -- do not Signal() to "give it back", there is nothing to
		// give back. Separate from Wait() so a caller who ignores the result cannot proceed believing
		// it holds a permit it does not hold; same rule as SchedulerMutex::LockCancellable.
		[[nodiscard]] WaitResult WaitCancellable();

		// EAGER cancellation, for a semaphore used as an I/O THROTTLE rather than a critical section.
		// Ejects waiters and wakes them with Cancelled WITHOUT waiting for a Signal -- the point being
		// that the release you would wait for is the work you are trying to abandon.
		//
		// tok selects whose waits to abort (the usual case: one player, one zone, one request). A
		// default-constructed token means EVERYONE, for teardown.
		//
		// Waiters that used plain Wait() are never ejected: they have nowhere to report Cancelled, so
		// waking one would hand its caller a permit it does not hold.
		void CancelWaiters(CancelToken tok = CancelToken{});

		bool Try_Wait();

		void Signal();

		// COROUTINE PATH. Use `co_await JLib::AcquireAsync(sem)` from Coroutine.h rather than calling
		// this directly. Takes a permit if one is available (returns true, caller must NOT suspend),
		// otherwise queues the task to be re-pushed by Signal() (returns false, caller stays
		// suspended). Test and enqueue share one spinLock acquisition for the same lost-wakeup reason
		// spelled out on SchedulerMutex::LockAsyncEnqueue.
		//
		// The ownership asymmetry documented on ScopedPermit applies here too and more sharply: a
		// permit taken by a coroutine has no owner the scheduler can track, and a coroutine can be
		// resumed on any worker, so nothing counts this as an acquisition for the helping guard.
		// result: optional slot the releaser writes before resuming, so a coroutine can learn it was
		// cancelled. Null means not cancellable and never skipped -- see the note on Waiter. The slot
		// must outlive the suspension, which is why the awaiter holds it as a MEMBER: an awaiter lives
		// in the coroutine frame, so a member of it is stable for exactly as long as the wait is.
		bool WaitAsyncEnqueue(Task* coroTask, WaitResult* result = nullptr);

		// RAII permit, and the ONLY way to hold one safely across a blocking call on a bare thread.
		//
		// Why this exists rather than tracking it in Wait(). A bare thread that blocks on a
		// scheduler primitive runs stolen tasks while it waits, so anything it holds can be demanded
		// by the code it runs. SchedulerMutex closes that by counting ownership and refusing to help
		// while it owns something. A raw permit cannot be counted the same way: the thread that
		// takes one is frequently not the thread that returns it, which is the entire point of a
		// producer/consumer semaphore, so counting a Wait() as an acquisition would make a
		// consumer's tally climb forever and silently stop it ever helping again.
		//
		// This resolves that by making the USAGE PATTERN declare itself. Take a permit lock-like --
		// acquire, work, release on the same thread -- and you get the same guard the mutex has.
		// Keep using Wait()/Signal() directly for producer/consumer and you pay nothing, correctly,
		// because there the permit genuinely has no owner.
		//
		// A fiber deliberately gets no counting: it can acquire on one worker and resume on another,
		// so a per-thread count would be corrupted by migration. It also does not need it, since a
		// fiber SUSPENDS on contention and never enters the helping path at all.
		//
		// What this does NOT do: make deadlock impossible. A helped task can still block on
		// something the scheduler cannot see -- a std::mutex, a file read, a GPU fence -- and no
		// ownership tracking reaches those. The rule in DESIGN.md is the real protection; this makes
		// the safe spelling the convenient one.
		class ScopedPermit {
		public:
			explicit ScopedPermit(SchedulerSemaphore& s);
			~ScopedPermit();
			ScopedPermit(const ScopedPermit&) = delete;
			ScopedPermit& operator=(const ScopedPermit&) = delete;
		private:
			SchedulerSemaphore& sem;
		};
	};

	class SchedulerConditionVariable {
	private:
		// User-space spinlock protecting the internal CV queue
		std::atomic_flag spinLock = ATOMIC_FLAG_INIT;

		// One entry per waiting fiber: the transient semaphore it is parked on, plus the scope it
		// waits under so CancelWaiters can pick out whose waits to abort.
		//
		// `sem` POINTS INTO THE WAITING FIBER'S STACK FRAME -- see the invariant note in Wait().
		// Anything that lets a waiter return must remove its entry from this queue FIRST, under the
		// lock, exactly as the notifiers do. That is the entire reason CancelWaiters below removes
		// before it wakes rather than the other way round.
		struct CvWaiter {
			SchedulerSemaphore* sem   = nullptr;
			uint32_t            token = 0xFFFFFFFFu;   // CancelToken::kNone
		};
		std::queue<CvWaiter> waitingQueue;

		void LockQueue();
		void UnlockQueue();

	public:
		SchedulerConditionVariable() = default;
		~SchedulerConditionVariable() = default;

		// Fibers suspend here; Native tasks spin and steal work
		void Wait(SchedulerMutex& mutex);

		// Cancellable Wait.
		//
		// THE INVARIANT: this RE-ACQUIRES THE MUTEX BEFORE RETURNING, cancelled or not. A condition
		// variable's contract is that Wait returns holding the lock, and cancellation does not get
		// to break it -- otherwise every caller would need a conditional unlock and the first one to
		// forget would unlock a mutex it does not hold. So the caller's Unlock stays unconditional
		// and only the RESULT differs.
		//
		// That is why this is safe where a timed wait would not be: re-acquiring is a wait, and for
		// a fiber or a coroutine a wait is a suspension rather than a blocked thread.
		[[nodiscard]] WaitResult WaitCancellable(SchedulerMutex& mutex);

		// EAGER cancellation: wakes matching waiters without any Notify. A condition variable is the
		// clearest case for it -- the condition may simply never become true.
		//
		// tok selects whose waits to abort; a default-constructed token means everyone, for
		// teardown. Waiters that used plain Wait() are never woken: they have nowhere to report
		// Cancelled.
		void CancelWaiters(CancelToken tok = CancelToken{});

		// Unblocks one waiting fiber context
		void Notify_One();

		// Unblocks all waiting fiber contexts
		void Notify_All();
	};

	// ================================ STALE-LIBRARY GUARD ==========================================
	// Aborts at startup if the Scheduler library was compiled against a DIFFERENT version of these
	// headers than the code including them now.
	//
	// WHY THIS EARNS ITS KEEP. Several classes here are header-only with real data members --
	// EpochManager, Task, TaskAllocator -- so adding a single member changes their layout in the
	// consumer's translation unit while the already-built .lib still uses the old offsets. Nothing
	// diagnoses that. The symptoms are heap corruption at an unrelated address, and they are
	// spectacularly misleading: this has produced an access violation inside std::vector::resize
	// (iterator-debug machinery walking a garbage proxy), a garbage epoch-slot pointer surfacing
	// inside LockFreeList::add, and a corrupted free list surfacing inside TaskAllocator::refill --
	// none of which are anywhere near the actual mistake, which was simply "you did not rebuild".
	//
	// This project has now lost time to it TWICE. A one-time comparison at static init is a much
	// better trade than the diagnosis.
	//
	// _ITERATOR_DEBUG_LEVEL is in the signature deliberately: on MSVC it changes std::vector's
	// layout, so mixing /MDd (level 2) with /MD (level 0) breaks exactly the same way and is the
	// other half of this failure mode.
	// Declared here rather than by including <windows.h>, which this header deliberately does not
	// pull in. The declaration matches the one in winbase.h exactly, so including both is fine.
#if defined(_WIN32)
}
extern "C" __declspec(dllimport) void __stdcall OutputDebugStringA(const char*);
namespace JLib {
#endif

	namespace detail {
		// WHAT THIS MUST COVER: every type whose LAYOUT a header-inline function depends on.
		//
		// Until 1.4 this was sizeof() only, and it did not cover TaskScheduler at all. Both of those
		// were wrong, and the second hid the first: 1.4 added nonWorkerLane and nonWorkerLaneClaimed
		// and made consecutiveHiPriSteals thread_local, which moved `taskAllocator` from offset 304
		// to 312 -- while sizeof(TaskScheduler) stayed at 1664, because the new members landed in
		// padding that was already there. sizeof(Task), sizeof(TaskAllocator) and
		// sizeof(EpochManager) did not move either. So the signature MATCHED across a genuine ABI
		// break, and the guard stayed silent through precisely the failure it exists to catch.
		//
		// THE RULE, corrected: sizeof is not a proxy for layout. Anything header-inline code reaches
		// into contributes an OFFSET (TaskScheduler::abiCanary carries that for the scheduler
		// itself), not just a size. Sizes stay for the types inline code allocates or copies whole.
		//
		// WHY THE ANONYMOUS NAMESPACE, WHICH IS NORMALLY WRONG IN A HEADER: internal linkage is the
		// entire mechanism. This has to evaluate SEPARATELY in the library's translation unit and in
		// each application translation unit, because comparing those two evaluations is the test.
		// An `inline` function -- what this was first written as -- has external linkage, so the
		// linker folds the copies into one and both sides call the SAME body. Tested: with the
		// library built at offset 296 and the application at 312, the folded version compared equal
		// and the program ran into the corruption unchecked. Per-TU copies are the point, not a
		// smell, and the ODR is satisfied precisely because these are distinct entities.
		#if defined(__GNUC__) || defined(__clang__)
		#	pragma GCC diagnostic push
		#	pragma GCC diagnostic ignored "-Winvalid-offsetof"
		#endif
		// Carried FIELD BY FIELD rather than hashed down to one number, because "signature
		// 4417359680416 != 4417354841872" tells you that something is stale and nothing about what.
		// Naming the field that moved tells you which header changed, which usually tells you which
		// library was not rebuilt.
		struct AbiComponents {
			// taskLayout is a FINGERPRINT, not a size -- see TaskLayoutFingerprint below. The field
			// COUNT and their types are this function's ABI, so nothing may be added here without
			// breaking every already-built library: an older one would return fewer bytes than the
			// caller allocated and the guard would compare against uninitialized memory to decide
			// whether the guard should fire. Widening the MEANING of an existing field costs
			// nothing, which is why that is what was done when Task's layout needed covering.
			uint32_t sizeEpochManager, taskLayout, sizeTaskAllocator, sizeTaskDeque;
			uint32_t sizeTaskMPSCQueue, sizeWaitGroup, sizeTaskScheduler;
			uint32_t offsetAbiCanary, iteratorDebugLevel;
		};

		namespace {
			// A fingerprint of Task's LAYOUT rather than merely its size, because size alone is blind
			// to the change class that matters most here.
			//
			// 2.8.0 packed six one-byte flags into a single byte. Every one of them moved, and
			// sizeof(Task) stayed 64 -- they had been followed by padding either way. A translation
			// unit compiled against the new header and linked to the older library would have read
			// `trivialDtor` from the wrong bit: silently skipping ~Task and leaking every lambda
			// capture, or running a destructor it should not have. The guard would have reported
			// everything in agreement, which is worse than having no guard, because it reports
			// success.
			//
			// Hashed rather than reported directly because the facts that matter do not fit in one
			// number: size, alignment, the packed flag block's width, and the offset of every named
			// member. Any reorder, resize or repack changes it. The cost is that a mismatch prints
			// two opaque values instead of "64 vs 72" -- acceptable, since the only useful response
			// to ANY mismatch here is the one the message already gives: rebuild.
			//
			// offsetof on Task is conditionally-supported (a vtable makes it non-standard-layout) and
			// is used on exactly the same footing as the existing offsetof(TaskScheduler, abiCanary)
			// below. Every compiler this library targets supports it.
			inline uint32_t TaskLayoutFingerprint() {
				uint32_t h = 2166136261u;                       // FNV-1a
				auto mix = [&h](uint32_t v) { h ^= v; h *= 16777619u; };
				mix((uint32_t)sizeof(Task));
				mix((uint32_t)alignof(Task));
				mix((uint32_t)sizeof(TaskFlagPacking));         // width of the packed flag block
				mix(TaskFlagBitLayout());                       // and every flag's BIT POSITION in it
				mix((uint32_t)offsetof(Task, fn));
				mix((uint32_t)offsetof(Task, data));
				mix((uint32_t)offsetof(Task, assignedFiber));
				mix((uint32_t)offsetof(Task, next));
				mix((uint32_t)offsetof(Task, waitGroup));
				return h;
			}

			inline AbiComponents LocalAbiComponents() {
				AbiComponents c{};
				c.sizeEpochManager    = (uint32_t)sizeof(EpochManager);
				c.taskLayout          = TaskLayoutFingerprint();
				c.sizeTaskAllocator   = (uint32_t)sizeof(TaskAllocator);
				c.sizeTaskDeque       = (uint32_t)sizeof(TaskDeque);
				c.sizeTaskMPSCQueue   = (uint32_t)sizeof(TaskMPSCQueue);
				c.sizeWaitGroup       = (uint32_t)sizeof(WaitGroup);
				c.sizeTaskScheduler   = (uint32_t)sizeof(TaskScheduler);
				c.offsetAbiCanary     = (uint32_t)offsetof(TaskScheduler, abiCanary);
#if defined(_ITERATOR_DEBUG_LEVEL)
				c.iteratorDebugLevel  = (uint32_t)_ITERATOR_DEBUG_LEVEL;
#endif
				return c;
			}
		}
		#if defined(__GNUC__) || defined(__clang__)
		#	pragma GCC diagnostic pop
		#endif

		// Defined in TaskScheduler.cpp, so it carries the value as the LIBRARY saw it.
		//
		// NAMED THIS WAY ON PURPOSE. There are two distinct staleness failures and this covers both:
		//   - The library predates the guard entirely (or was not rebuilt at all). Then this symbol
		//     is MISSING and you get a LINK error -- and a linker cannot print an explanation, so
		//     the only place to put one is the symbol name itself. "unresolved external symbol
		//     LibraryAbiSignature" tells you nothing; this tells you what to do.
		//   - The library was rebuilt but from different headers. Then it links, and the runtime
		//     comparison below fires with a real message.
		AbiComponents JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration();

		// Runs once per TRANSLATION UNIT, not once per program -- and that distinction is the whole
		// guard. This was `inline const bool`, which has external linkage and therefore gets folded
		// to a single copy, so exactly one TU's view was ever checked and every other TU went
		// unexamined. Internal linkage means each TU tests ITS OWN layout against the library's.
		// (See LocalAbiComponents above for the same mistake and the measurement that caught it.)
		namespace {
		[[maybe_unused]] const bool g_abiChecked = [] {
			const AbiComponents lib = JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration();
			const AbiComponents hdr = LocalAbiComponents();

			char msg[1600];
			int n = std::snprintf(msg, sizeof msg,
				"[JLib::Scheduler] FATAL: this translation unit was compiled against DIFFERENT "
				"Scheduler headers than the Scheduler library it is linked to.\n"
				"  Fields that disagree (library vs this TU):\n");

			const struct { const char* name; uint32_t l, h; } fields[] = {
				{ "sizeof(EpochManager)",              lib.sizeEpochManager,   hdr.sizeEpochManager   },
				{ "Task layout (size/align/offsets)",  lib.taskLayout,         hdr.taskLayout         },
				{ "sizeof(TaskAllocator)",             lib.sizeTaskAllocator,  hdr.sizeTaskAllocator  },
				{ "sizeof(TaskDeque)",                 lib.sizeTaskDeque,      hdr.sizeTaskDeque      },
				{ "sizeof(TaskMPSCQueue)",             lib.sizeTaskMPSCQueue,  hdr.sizeTaskMPSCQueue  },
				{ "sizeof(WaitGroup)",                 lib.sizeWaitGroup,      hdr.sizeWaitGroup      },
				{ "sizeof(TaskScheduler)",             lib.sizeTaskScheduler,  hdr.sizeTaskScheduler  },
				{ "offsetof(TaskScheduler,abiCanary)", lib.offsetAbiCanary,    hdr.offsetAbiCanary    },
				{ "_ITERATOR_DEBUG_LEVEL",             lib.iteratorDebugLevel, hdr.iteratorDebugLevel },
			};
			bool differs = false;
			for (const auto& f : fields) {
				if (f.l == f.h) continue;
				differs = true;
				n += std::snprintf(msg + n, (n < (int)sizeof msg) ? sizeof msg - n : 0,
					"    %-36s %u vs %u\n", f.name, f.l, f.h);
			}
			if (!differs) return true;

			std::snprintf(msg + n, (n < (int)sizeof msg) ? sizeof msg - n : 0,
				"  Rebuild EVERY library that includes TaskScheduler.h, not just the Scheduler --\n"
				"  a stale Sound/Renderer/Physics library carries its own inlined copy of CreateTask\n"
				"  and reaches the wrong offset in a correctly-built scheduler object.\n"
				"  Note the Scheduler ships Debug, Development and Release; rebuilding only some of\n"
				"  them causes exactly this. _ITERATOR_DEBUG_LEVEL differing means /MDd vs /MD.\n"
				"  Continuing would fault at an unrelated address -- refusing instead.\n");

			std::fputs(msg, stderr);
			std::fflush(stderr);
			// A GUI-subsystem process has no console, so the text above goes nowhere and all the
			// user sees is "Fatal program exit requested" at __scrt_common_main_seh. This is the
			// one channel that works there, and it lands in the debugger's Output window.
#if defined(_WIN32)
			OutputDebugStringA(msg);
#endif
			std::abort();
			return false;
		}();
		}
	}
}

