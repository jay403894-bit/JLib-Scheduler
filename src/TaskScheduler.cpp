// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Thread.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include "../include/platform.h"
#include "../include/Topology.h"
#include <stdexcept>
#include <cstdio>      // fprintf -- the debug-only event-registry tripwire in GetEvent
#include <vector>
#include <chrono>
using namespace JLib;

// ================= CONTENDED-WAIT HELPER, shared by SchedulerMutex and SchedulerSemaphore =======
// A bare thread that blocks on one of these cannot suspend, so instead of burning the core it runs
// a stolen noFiber task per iteration. That is work-conserving and it is also the single most
// dangerous thing in this file, because it makes acquiring a lock REENTRANT: user code runs inside
// the acquisition loop, and it can do anything, including taking locks.
//
// Three distinct failures come out of that, and they need three different guards.
//
//  1. UNBOUNDED NESTING. The helped task contends the same primitive, spin-helps again, runs another
//     task, and so on. Each level is a real stack frame. `t_spinHelpDepth` allows exactly one level:
//     inside a helped task we spin rather than help.
//
//  2. SELF-DEADLOCK BY INVERSION, and this one is a genuine hang rather than a slowdown. A thread
//     holding mutex A waits on B, helps, and the helped task asks for A. A is owned by this very
//     thread, which is stuck inside the task, so nothing can ever release it. No fiber involved and
//     no lock-ordering discipline in the caller's own code can prevent it, because the interleaving
//     is chosen by the scheduler. `t_heldMutexes` closes it: a thread that owns a SchedulerMutex
//     stops executing other people's tasks entirely.
//
//  3. POINTLESS CPU BURN when the holder is a SUSPENDED FIBER. Helping cannot resume it -- only
//     noFiber tasks are stolen here, so if the resumer is a fiber task this thread structurally
//     cannot make that progress. Yielding gives the OS a chance to run a worker that can.
//
// The idle count deliberately measures UNPRODUCTIVE passes, not iterations. Each pass may run a
// whole task, so counting all of them would yield out a thread that is doing real work; a run task
// resets it.
namespace {
	thread_local uint32_t t_spinHelpDepth = 0;   // >0 -> we are executing inside a helped task
	thread_local uint32_t t_heldMutexes   = 0;   // >0 -> this BARE THREAD owns a SchedulerMutex

	constexpr uint32_t kIdleSpinsBeforeYield = 1000;

	// Thread-local rather than a parameter so it survives across calls. The condition variable calls
	// this once per predicate check and loops outside, so a local would reset to zero every time and
	// the yield would never fire.
	thread_local uint32_t t_idleSpins = 0;

	// One iteration of a bare-thread contended wait. The caller owns the predicate and the loop.
	inline void ContendedSpinStep() {
		if (t_spinHelpDepth == 0 && t_heldMutexes == 0 && TaskScheduler::IsInitialized()) {
			++t_spinHelpDepth;
			const bool ranSomething = TaskScheduler::Instance().TryRunStolenNoFiberTask();
			--t_spinHelpDepth;
			if (ranSomething) { t_idleSpins = 0; return; }   // progress: not an idle pass
		}
		if (++t_idleSpins >= kIdleSpinsBeforeYield) {
			t_idleSpins = 0;
			std::this_thread::yield();
		}
		else {
			platform::CpuRelax();
		}
	}

	// Ownership is tracked for BARE THREADS ONLY. A fiber can acquire on one worker and resume on
	// another, so a per-thread count would be corrupted by migration -- see DESIGN.md's rule that
	// nothing thread-derived survives a suspend. The count only ever decides whether to help, and
	// erring toward "do not help" is safe, so a fiber simply never sets it.
	inline bool OnBareThread() {
		Thread* t = Thread::GetCurrent();
		return t == nullptr || t->currentFiber == nullptr;
	}
}

static_assert(sizeof(Task) <= TaskAllocator::SLOT, "Task doesn't fit a slot");
static_assert(alignof(Task) <= 16, "Task over-aligned for a slot");

TaskScheduler* TaskScheduler::instance = nullptr;
GlobalFiberPool* TaskScheduler::globalPool = nullptr;

TaskScheduler::TaskScheduler(size_t poolSize) {
	StartPool(poolSize);
}
size_t TaskScheduler::GetSafeTC() {
	// The AUTO pool size (Init/StartPool with poolSize == 0): hw-1 -- main pins CPU 0, workers pin
	// CPUs 1..hw-1. HISTORY (don't relive it): this was briefly hw-2 (2026-07-31) because GameInput's
	// internal worker thread (ABOVE_NORMAL priority, opaque count) was one persistent foreign claimant
	// with nowhere unclaimed to land -> measured oversubscription-by-one (VTune + dose-response: -3/-4
	// reserves didn't help, so the deficit was exactly one). That claimant is now GONE: JLib::Input runs
	// GameInput in MANUAL DISPATCH mode (IGameInputDispatcher held; InputManager::Update pumps
	// Dispatch() per frame on main), so its async work costs ZERO threads. Census: workers + main + 0
	// = exact fit at hw-1. Remaining foreign threads (GPU driver, audio callback, OS pools) are
	// low-duty-cycle wakers that time-slice fine -- measured, not assumed. Apps whose profile disagrees
	// (or that DON'T use manual-dispatch input) pass an EXPLICIT poolSize to Init (e.g. hw-2) -- that's
	// the config surface, deliberately no env var; explicit sizes may claim up to full hw (StartPool).
	unsigned int cores = std::thread::hardware_concurrency();
	if (cores <= 1) return 1;
	return static_cast<size_t>(cores - 1);
}
void TaskScheduler::Init(size_t poolSize) {
	if (instance != nullptr)
		throw std::runtime_error("TaskScheduler already initialized!");
	instance = new TaskScheduler(poolSize);

}
GlobalFiberPool& JLib::TaskScheduler::GetGlobalPool()
{
	if (!instance->globalPool)
		throw std::runtime_error("GlobalFiberPool not initialized!");
	return *instance->globalPool;

}
TaskScheduler::~TaskScheduler() {
	if (!stopFlag)
		Join();
}
bool TaskScheduler::PushMain(Task* task) {
	if (!poolActive) return false;
	if (!task) return false;
	mainQ.push(task);
	return true;
}
void TaskScheduler::ProcessMainThread() {
	if (!poolActive) return;
	Task* t;
	while (mainQ.pop(t)) {
		if (!t) continue;
		t->Execute();
		if (t->waitGroup) {
			if (t->waitGroup) {
				int old = t->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
				if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
					t->waitGroup->WakeAll();   // only touches wg if someone registered
			}
		}
		// Worker() frees a DEAD task after running it (see its FiberStatus::DEAD branch) --
		// this path was missing the equivalent, so every PushMain'd task leaked its slab.
		// Latent/unnoticed while PushMain was barely used; would leak fast once frame tasks
		// (StartFrame/Submit-producers/PresentFrame) route through here every frame.
		t->~Task();
		taskAllocator.Free(t);
	}
}
void TaskScheduler::WaitForMain(WaitGroup& wg) {
	while (wg.n.load(std::memory_order_acquire) > 0) {
		ProcessMainThread();  // drain any ready main-affinity DAG nodes
		std::this_thread::yield();
	}
}
void TaskScheduler::Join() {
	if (!poolActive) return;
	
	stopFlag.store(true, std::memory_order_release);

	{
		registryMtx.lock();
		for (auto& pair : eventRegistry)
			pair.second->SignalAll();

		registryMtx.unlock();
	}
	NotifyAll();

	for (auto& worker : workers)
		worker->Join();

	{
		poolMutex.lock();
		workers.clear();
		mainQ.clear();
		immediateCoresInUse.clear();
		poolMutex.unlock();
	}

	poolActive.store(false, std::memory_order_release);
}
void TaskScheduler::NotifyAll() {
	for (auto& w : workers)
		w->NotifyWorker();
}
// How much SERIAL WORK a loop must represent before splitting it pays for itself. Measured, not
// guessed (bench.exe, BenchParallelForCrossover): sweeping per-element cost over four orders of
// magnitude, the crossover ELEMENT COUNT moves 400x (200,000 for a trivial body down to ~400 for an
// expensive one) while the crossover WORK stays pinned at 70-92us. That constant is the fork-join
// dispatch+join overhead, and it is the only thing that generalizes -- which is why the old
// `totalItems > 10000` gate was wrong in BOTH directions: it let a trivial body parallelize at 10k
// where it ran 11x SLOWER, and forced an expensive body serial at 4k where parallel was 12x faster.
// 75us sits in the measured cluster. The error is asymmetric -- being slightly too eager can cost
// multiples, being slightly too cautious costs a few percent near the boundary -- so when in doubt
// this leans serial.
// Keyed on OPTIMIZATION, not on NDEBUG alone. A "Development"/RelWithDebInfo build is optimized but
// deliberately keeps assertions live, so it does NOT define NDEBUG -- testing NDEBUG by itself would
// hand an optimized build the unoptimized threshold and serialize work that should be parallel.
#if defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
static constexpr double kDefaultParallelWorthwhileUs = 75.0;
#else
// DEBUG BUILDS PAY A MUCH HIGHER DISPATCH COST and the 75us figure was measured in RELEASE. Unoptimized
// std::function indirection, _ITERATOR_DEBUG_LEVEL on the containers, and un-inlined task allocation
// make building a fork-join tree roughly an order of magnitude more expensive, while the constant is
// *only* a measure of that overhead. Leaving it at 75 made Game01's physics loop -- hundreds of
// elements, previously always serial under the old >10000 gate -- start parallelizing in Debug and
// drop the game to 2 FPS, while Release stayed in the hundreds. Same threshold, wrong build.
// Not a guess at a ratio: it is deliberately conservative, because in a Debug build being slightly too
// serial costs nothing anyone measures, and being too eager costs an unusable frame rate.
static constexpr double kDefaultParallelWorthwhileUs = 750.0;
#endif

// Runtime-settable so this can be BISECTED without a rebuild: set it absurdly high and every
// ParallelFor runs serial, which answers "is ParallelFor responsible for this?" in one run instead of
// a recompile per guess. Also legitimately useful beyond debugging -- the right value is a property of
// the machine and the build, and an app that has profiled its own workload knows better than a
// hard-coded constant. Plain double, written once at startup and only read afterwards.
// Worker binding policy. Read once per worker at thread creation (Thread.cpp), so it must be set
// before Init(); a plain global is correct here -- no cross-thread mutation after startup.
// DEFAULT = Ideal, changed from Hard 2026-08-05 on measurement (bench.exe hard|physical|ideal|none,
// 32-logical hybrid, idle):
//
//   policy    throughput   ParallelFor   latency    frame DAG
//   hard        0.83 M/s      3.28x      6.93 us    41.94 us/graph
//   physical    0.83 M/s      3.44x      8.32 us    36.56 us/graph
//   ideal       0.79 M/s      3.72x      4.64 us    21.59 us/graph
//   none        0.80 M/s      2.95x      4.58 us    21.73 us/graph
//
// Hard binding buys ~4% on bulk throughput (cache locality) and costs ~45% on WAKE LATENCY: a pinned
// worker can only be woken onto its own core, so every sync point waits for that specific core to be
// free. The frame DAG -- a chain of tiny nodes with a WaitFor between each, i.e. the shape of a real
// frame -- is dominated by that, and it is where hard affinity loses nearly 2x.
//
// PhysicalOnly was added to test whether the loss was SMT contention (the old scheme pins workers to
// logical CPUs 1..N, deliberately doubling up on physical cores). It helped -- 15 physically-pinned
// workers roughly match 31 logically-pinned ones, so sibling contention IS real -- but it did not
// close the gap to unpinned. So the dominant cost is binding itself, not the sibling mapping.
//
// Ideal keeps locality intent and the topology map meaningful while letting Windows move a worker
// when its core is busy, which is the case that hurt. Hard remains available for a machine the app
// genuinely owns, where predictable placement beats latency.
// CAVEAT: measured on ONE idle hybrid machine with a synthetic bench. Re-measure under load and on
// a non-hybrid CPU before treating this as universal.
static TaskScheduler::AffinityPolicy g_affinityPolicy = TaskScheduler::AffinityPolicy::Ideal;
void TaskScheduler::SetAffinityPolicy(AffinityPolicy p) { g_affinityPolicy = p; }
TaskScheduler::AffinityPolicy TaskScheduler::GetAffinityPolicy() { return g_affinityPolicy; }

static double g_parallelWorthwhileUs = kDefaultParallelWorthwhileUs;
void TaskScheduler::SetParallelForThresholdUs(double us) { g_parallelWorthwhileUs = us; }
double TaskScheduler::GetParallelForThresholdUs() { return g_parallelWorthwhileUs; }

void TaskScheduler::ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	// PROBE: the scheduler cannot know what an element costs, and the caller usually cannot say in a
	// portable way either -- so measure it. Run a small prefix serially, time it, and extrapolate to
	// the rest. The probe is not overhead: it is loop work that had to happen regardless, just done
	// here instead of in a chunk. Sized as a fraction of the range so an expensive body cannot burn
	// the whole budget deciding (32 heavy elements is ~19us, well under the threshold it is testing).
	const int probeCount = std::min(totalItems, std::max(32, std::min(1024, totalItems / 64)));
	const auto probeT0 = std::chrono::steady_clock::now();
	func(start, start + probeCount);
	const double probeUs =
		std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - probeT0).count();

	start += probeCount;
	totalItems -= probeCount;
	if (totalItems <= 0) return;   // the probe was the whole range

	// Floor the chunk size so the range can't be shattered into more pieces than the pool can use.
	// A caller passing a small chunkSize is expressing "split this finely", but past ~4 chunks per
	// worker extra pieces buy no more load balancing and cost a task each -- 256 elements at
	// chunkSize 2 builds 128 tasks to do ~250us of work, and measured 0.49x (i.e. 2x SLOWER) purely
	// on tree overhead. 4 per worker keeps enough pieces to even out a ragged body.
	{
		const int maxUsefulTasks = (int)std::max<size_t>(1, workers.size() * 4);
		chunkSize = std::max(chunkSize, (totalItems + maxUsefulTasks - 1) / maxUsefulTasks);
	}

	const double estRemainingUs = probeUs * ((double)totalItems / (double)probeCount);
	if (estRemainingUs >= g_parallelWorthwhileUs) {
		int numTasks = (totalItems + chunkSize - 1) / chunkSize;

		// HYBRID dispatch (same spirit as the scheduler's other hybrids -- fiberless-vs-fiber,
		// main-spin-help, locality-then-random steal): the FLAT path below has the CALLER spawn every
		// chunk serially -- fine and ~14% faster when there are few tasks, but its O(#tasks) serial
		// CreateTask+Push+NotifyWorker on ONE thread blows up at fine grain (benchmarked ~8x slower at
		// ~15k tasks; each notify also takes the worker mutex from the lost-wakeup fix). Fork-join
		// distributes the spawn across the whole pool and wins decisively past a few dozen tasks. Cross
		// over at ~2 tasks/worker (below that flat wins/ties; above, FJ pulls ahead). See ParallelForFJ.
		if ((size_t)numTasks > 2 * workers.size()) {
			ParallelForFJ(start, end, chunkSize, func);
			return;
		}

		// Chunk 0 is MAIN'S OWN LANE: the calling thread computes it as a plain inline call
		// (NOT a scheduled task), so it can never suspend/resume and never touches a fiber or
		// task slab. Chunks 1..numTasks-1 go to workers. This keeps all hw lanes busy without
		// making the caller a task-runner -- the caller stays a pure waiter for scheduled work.
		// See WaitFor for why the caller must not run scheduled tasks.
		const int mainChunkStart = start;
		const int mainChunkEnd = std::min(start + chunkSize, end);

		// Capturing `&func` (a by-value parameter) is safe ONLY because WaitFor(wg) below
		// blocks until every task tied to `wg` has completed -- the tasks never outlive this
		// frame. The completion decrement is done exclusively by the worker's waitGroup path
		// (Thread::Worker / TryRunStolenNoFiberTask); the task body must NOT also decrement, or
		// each task counts down twice and the wait wakes early on half-finished work.
		WaitGroup wg;
		for (int i = 1; i < numTasks; ++i) {
			int chunkStart = start + i * chunkSize;
			int chunkEnd = std::min(chunkStart + chunkSize, end);

			Task* t = CreateTask([&func, chunkStart, chunkEnd]() {
				func(chunkStart, chunkEnd);
				});
			if (!t) {
				func(chunkStart, chunkEnd); // arena exhausted: graceful degradation, run it here
				continue;
			}
			t->waitGroup = &wg;
			wg.n.fetch_add(1, std::memory_order_relaxed);
			// MUST route through Push()/PushLocal, NOT a blind `loPriInboxes[i % n]->push()`
			// round-robin: PushLocal's PickNextWorker skips any core whose immediateCoresInUse
			// is set (a worker PINNED by a persistent PushImmediate/PushFork task -- e.g. the
			// audio subsystem's forever-running mixer). A pinned worker never returns to its
			// loop to drain its inbox, and inboxes are owner-drain-only (never stealable, so
			// TryRunStolenNoFiberTask can't rescue them) -- so a chunk shoved into a pinned worker's
			// inbox is stranded forever and WaitFor(wg) spins until the heat death of the app.
			// This was the particle-demo deadlock: it only bit once the sound thread was pinned.
			// Push() also handles pendingTasks++/MarkQueuedWork/NotifyWorker.
			Push(t);
		}

		// Main computes its own lane while the workers churn -- no wasted thread.
		func(mainChunkStart, mainChunkEnd);

		// Block until every dispatched chunk is done (fiber callers park; non-fiber callers
		// spin-and-help via TryRunStolenNoFiberTask inside WaitFor).
		WaitFor(wg);
	}
	else
	{
		// Not enough work left to pay for dispatch -- finish the remainder inline. One whole-range
		// call is equivalent to the old per-item func(i, i+1) loop for a range-processing func,
		// and cheaper.
		func(start, end);
	}
}
void TaskScheduler::ParallelForFJ(int start, int end, int grain, std::function<void(int, int)> func) {
	grain = std::max(1, grain);
	if (end - start <= 0) return;
	if (end - start <= grain) { func(start, end); return; }   // too small to bother splitting

	WaitGroup wg;
	// Recursive splitter. Runs on the CALLER inline (it does the leftmost spine of leaves) and on
	// WORKERS (each spawned task runs it on its own sub-range). Discipline: spawn the RIGHT half as
	// a task and continue on the LEFT inline. Each spawned task increments wg once (before Push) and
	// is decremented once when it completes -- by the worker's waitGroup path (Thread::Worker /
	// TryRunStolenNoFiberTask), NOT here. The caller's own inline work is not a task and never touches wg.
	// wg can't hit 0 prematurely: a task increments for all its children (inside rec) BEFORE it
	// returns (and gets decremented), so pending descendants are always counted.
	std::function<void(int, int)> rec = [&](int a, int b) {
		while (b - a > grain) {
			int mid = a + (b - a) / 2;
			Task* t = CreateTask([&rec, mid, b]() { rec(mid, b); });
			if (!t) {
				func(mid, b);          // arena exhausted: do the right half inline, no task/no wg
			} else {
				t->waitGroup = &wg;
				wg.n.fetch_add(1, std::memory_order_relaxed);   // count BEFORE it can run+decrement
				Push(t);
			}
			b = mid;                   // keep splitting the left half on THIS thread
		}
		func(a, b);                    // base case: do the leaf
	};

	rec(start, end);   // caller does the leftmost spine + spawns the rest of the tree
	WaitFor(wg);
}

void TaskScheduler::ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);
		Push([=]() { func(chunkStart, chunkEnd); });
	}
}
// Fills siblingQIndex/clusterMates from REAL hardware topology -- NOT from the sequential affinity
// scheme assumption (worker qIndex i is pinned to logical CPU i+1, main sits on 0). That mapping
// tells you what you ASKED the OS for, not what the hardware actually looks like (adjacent logical
// CPU numbers being SMT/cache neighbors is a common convention, never a guarantee).
//
// ACQUIRING the topology is platform-specific and lives in src/win32/Topology.cpp (a Win32 API)
// and src/posix/Topology.cpp (sysfs text). DERIVING relationships from it is not platform-specific
// and stays here -- the "exactly two workers share this core" rule, the widest-group-is-LLC
// heuristic and the P/E demotion rule are the subtle parts, and there is one copy of each.
//
// On ANY failure (API unsupported, sysfs absent, nothing returned) this falls back to safe
// defaults: no SMT sibling for anyone, and everyone in one big cluster -- equivalent to plain
// random stealing, just never wrong.
void TaskScheduler::BuildTopology(unsigned int num_workers) {
	siblingQIndex.assign(num_workers, -1);
	clusterMates.assign(num_workers, {});

	// logical CPU -> qIndex, per the known affinity scheme (0 = main, i+1 = worker i).
	auto qIndexOf = [num_workers](int logicalCpu) -> int {
		if (logicalCpu < 1) return -1;
		unsigned int q = (unsigned int)(logicalCpu - 1);
		return (q < num_workers) ? (int)q : -1;
	};

	// --- P/E-core labels (Intel hybrid). DATA ONLY -- nothing schedules on this yet. Read each physical
	// core's PROCESSOR_RELATIONSHIP.EfficiencyClass; the HIGHEST class present is a Performance core, any
	// lower class is an Efficiency core. Default everyone to P (1): correct for a non-hybrid CPU, and a safe
	// fallback if the query fails -- only demote workers we positively identify as lower-class. ---
	isPCore.assign(num_workers, 1);
	isPCpu.assign(topology::CpuMask::kMaxCpus, 1);   // per-LOGICAL-CPU class; default all-P (non-hybrid / query-fail safe)

	topology::Info topo;
	topology::Query(topo);

	{
		// E only if the class is KNOWN and strictly below the top class seen (workers AND raw CPUs
		// -- the CPU table serves unpinned/non-worker threads, see isPCpu in the header). Linux
		// reports all-unknown today, so every core stays P there, which is what dormant class
		// routing wants.
		for (int cpu = 0; cpu < (int)topology::CpuMask::kMaxCpus; ++cpu) {
			const int eff = topo.efficiencyClass[cpu];
			if (eff >= 0 && eff < topo.maxClass) {
				isPCpu[cpu] = 0;
				int q = qIndexOf(cpu);
				if (q >= 0) isPCore[q] = 0;
			}
		}
	}

	const std::vector<topology::CpuMask>& coreMasks  = topo.coreMasks;    // one per physical core -- SMT groups
	const std::vector<topology::CpuMask>& cacheMasks = topo.cacheMasks;   // one per cache instance -- clusters
	const bool haveCores = topo.haveCores;
	const bool haveCache = topo.haveCache;

	if (haveCores) {
		for (const topology::CpuMask& mask : coreMasks) {
			std::vector<int> qsInGroup;
			for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu) {
				if (mask.Test(cpu)) {
					int q = qIndexOf((int)cpu);
					if (q >= 0) qsInGroup.push_back(q);
				}
			}
			// Only meaningful if exactly two POOL WORKERS share this physical core -- if the
			// group is size 1 (its SMT sibling is main or an unused logical CPU) there's no
			// worker sibling to record.
			if (qsInGroup.size() == 2) {
				siblingQIndex[qsInGroup[0]] = qsInGroup[1];
				siblingQIndex[qsInGroup[1]] = qsInGroup[0];
			}
		}
	}

	if (haveCache) {
		for (const topology::CpuMask& mask : cacheMasks) {
			// RelationCache returns one record PER cache instance PER level (L1/L2/L3 all
			// come back through this same query) -- we only want the last-level one(s), which
			// in practice are the masks covering the MOST logical CPUs. Filtering by "biggest
			// masks seen" below (after the loop) is simpler than threading the cache Level
			// field through GetGroupMasksForRelation, so just collect qIndex groups for every
			// mask here and pick the highest-CPU-count group per worker afterward.
			std::vector<int> qsInGroup;
			for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu) {
				if (mask.Test(cpu)) {
					int q = qIndexOf((int)cpu);
					if (q >= 0) qsInGroup.push_back(q);
				}
			}
			if (qsInGroup.size() < 2) continue;
			for (int q : qsInGroup) {
				// Keep the LARGEST group seen for this worker -- that's the last-level
				// (widest-sharing) cache instance rather than a narrower L1/L2 one.
				if (qsInGroup.size() > clusterMates[q].size() + 1) {
					clusterMates[q].clear();
					for (int other : qsInGroup) {
						if (other != q && other != siblingQIndex[q]) clusterMates[q].push_back(other);
					}
				}
			}
		}
	}
	else {
		// Fallback: no cache topology available -- treat the whole pool as one cluster so
		// locality-first-random-fallback degrades to exactly the old plain-random behavior.
		for (unsigned int q = 0; q < num_workers; ++q) {
			for (unsigned int other = 0; other < num_workers; ++other) {
				if (other != q && (int)other != siblingQIndex[q]) clusterMates[q].push_back((int)other);
			}
		}
	}

	// Per-worker LLC domain mask, for the Ideal policy on platforms whose affinity API takes a mask
	// (see llcMaskOfWorker in the header). The widest cache group containing this worker's logical
	// CPU is its last-level cache -- the same "widest wins" rule the cluster derivation above uses,
	// so a worker's mask and its clusterMates always describe the same domain.
	llcMaskOfWorker.assign(num_workers, topology::CpuMask{});
	if (haveCache) {
		for (unsigned int q = 0; q < num_workers; ++q) {
			const unsigned cpu = q + 1;              // the sequential pinning scheme: main on 0
			if (cpu >= topology::CpuMask::kMaxCpus) continue;
			unsigned best = 0;
			for (const topology::CpuMask& mask : cacheMasks) {
				if (!mask.Test(cpu)) continue;
				const unsigned n = mask.Count();
				if (n > best) { best = n; llcMaskOfWorker[q] = mask; }
			}
		}
	}

	// Split each worker's mates by core class (see matesSameClass in the header): the steal scan probes
	// same-class victims FIRST because those are the only ones an explicit-class task can be taken from
	// -- scan order should match steal legality. Same total coverage, reordered; non-hybrid CPUs put
	// everyone in matesSameClass and the foreign phase disappears.
	matesSameClass.assign(num_workers, {});
	matesOtherClass.assign(num_workers, {});
	for (unsigned int q = 0; q < num_workers; ++q)
		for (int m : clusterMates[q])
			((isPCore[m] == isPCore[q]) ? matesSameClass[q] : matesOtherClass[q]).push_back(m);
}

void TaskScheduler::StartPool(size_t poolSize) {
	poolMutex.lock();
	thread_counter.store(0, std::memory_order_release);
	// poolSize == 0 -> auto (hw-1, see GetSafeTC). An EXPLICIT poolSize is honored as given -- the app's
	// declared choice wins -- clamped only to the physical ceiling (hw), so a caller may claim the whole
	// CPU. Note the pinning scheme (worker i -> logical CPU i+1, main on 0): at poolSize == hw the LAST
	// worker's SetThreadAffinityMask targets a CPU past the topology and fails -> that worker just floats
	// unpinned (OS places it); harmless, but P/E labeling won't cover it.
	if (poolSize == 0)
		poolSize = GetSafeTC();
	{
		unsigned int hw = std::thread::hardware_concurrency();
		if (hw == 0) hw = 4;
		if (poolSize > hw) poolSize = hw;
	}

	// PhysicalOnly: one worker per PHYSICAL core, pinned to that core's FIRST logical CPU, leaving
	// every SMT sibling empty.
	//
	// This exists because the default scheme (worker i -> logical CPU i+1) deliberately places two
	// workers on most physical cores as hyperthread siblings, where they contend for the same
	// execution units. Benchmarking showed hard affinity ~1.8x WORSE than unpinned on the frame DAG,
	// and this is the hypothesis for why: the problem may be the naive logical-index mapping rather
	// than pinning itself. Windows' own scheduler spreads across distinct physical cores first and
	// only doubles up when it has to.
	//
	// Note this necessarily HALVES the pool on an SMT machine -- so it changes two variables at once
	// (placement AND worker count). That is inherent: you cannot put 31 workers on 16 physical cores
	// without doubling up. Sizing a pool to physical rather than logical cores is a legitimate
	// configuration in its own right, which is why it is worth measuring rather than avoiding.
	std::vector<int> physicalCpus;   // empty unless PhysicalOnly; entry 0 is reserved for main
	std::vector<int> logicalCpus;    // every logical CPU that EXISTS, ascending; entry 0 -> main
	{
		topology::Info topo;
		topology::Query(topo);

		// WHY THIS IS NOT 0..N-1. The flat CpuId is `group * 64 + bit` (see Topology.h), and Windows
		// aligns processor groups to NUMA/socket boundaries rather than packing them to 64. A 96-CPU
		// machine presented as two groups of 48 therefore has LIVE ids 0..47 and 64..111, with a hole
		// between -- the id space is sparse, while a naive `worker i -> cpu i+1` is dense.
		//
		// Handing out i+1 there would request ids 48..63, which name no processor at all: the bind
		// call fails, sixteen workers silently run unbound, and their siblingQIndex/clusterMates
		// entries describe CPUs that do not exist. Nothing crashes, which is what makes it nasty.
		//
		// Enumerating the CPUs the topology actually reported makes the assignment correct on any
		// grouping, and collapses to exactly i+1 on a dense single-group machine, which is every
		// machine anyone currently runs this on.
		if (topo.haveCores) {
			topology::CpuMask all;
			for (const topology::CpuMask& m : topo.coreMasks)
				for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu)
					if (m.Test(cpu)) all.Set(cpu);
			for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu)
				if (all.Test(cpu)) logicalCpus.push_back((int)cpu);
		}

		if (GetAffinityPolicy() == AffinityPolicy::PhysicalOnly) {
			if (topo.haveCores) {
				// One representative logical CPU per physical core: the lowest-numbered member of each
				// SMT group. Deliberately the SAME masks BuildTopology consumes, so "physical core"
				// means one thing across the whole scheduler.
				for (const topology::CpuMask& m : topo.coreMasks)
					for (unsigned cpu = 0; cpu < topology::CpuMask::kMaxCpus; ++cpu)
						if (m.Test(cpu)) { physicalCpus.push_back((int)cpu); break; }
			}
			if (physicalCpus.size() >= 2)
				poolSize = physicalCpus.size() - 1;   // one physical core left to main
			else
				physicalCpus.clear();                 // topology unavailable -> fall back to the normal scheme
		}
	}

	unsigned int num_workers = static_cast<unsigned int>(poolSize);
	// +1 for the main/submitting thread: it takes thread_id == num_workers at the end
	// of StartPool and uses epochs too (e.g. DAG AddDependency -> EnterEpoch). Sizing to
	// just num_workers leaves that slot out of bounds -> AV in Enter/LeaveEpoch.
	EpochManager::Instance().Init(num_workers + 1);
	BuildTopology(num_workers);
	// Split workers into P/E sets for PickNextWorker's tier routing (isPCore was filled by BuildTopology).
	// Non-hybrid CPU -> every worker labeled P -> eWorkers empty -> routing falls through to the full pool.
	pWorkers.clear(); eWorkers.clear();
	nextPWorker.store(0); nextEWorker.store(0);
	for (unsigned int q = 0; q < num_workers; ++q)
		(isPCore[q] ? pWorkers : eWorkers).push_back((int)q);
	stopFlag.store(false, std::memory_order_release);
	nextWorker = 0;
	unsigned int coreCount = std::thread::hardware_concurrency()-1;
	if (coreCount == 0) coreCount = 4; // Fallback

	size_t standardFiberCount = coreCount * 64;
	size_t heavyFiberCount = coreCount * 8;

	// 3. Ensure a minimum to avoid "thrashing"
	
	// GlobalFiberPool now owns all fibers and stack allocation
	globalPool = GlobalFiberPool::Create(standardFiberCount, heavyFiberCount);
	workers.clear();
	loPri.clear();
	immediateCoresInUse.clear();
	loPriInboxes.clear();
	hiPri.clear();
	hiPriInboxes.clear();
	workers.reserve(num_workers);
	loPri.reserve(num_workers);
	hiPri.reserve(num_workers);
	immediateCoresInUse.reserve(num_workers);
	loPriInboxes.reserve(num_workers);
	hiPriInboxes.reserve(num_workers);

	// mainQ (used by PushMain/ProcessMainThread, e.g. TaskDAG main-affinity nodes) was NEVER
	// init()'d -- its default ctor leaves head_/tail_/stub_ uninitialized. Harmless as long as
	// nothing actually called PushMain, which nothing did until real work started routing
	// through it (TaskDAG::Fire's isMain branch): TaskMPSCQueue::append() then wrote through a
	// garbage head_/prev pointer -> write access violation. One-time init, same as every other
	// TaskMPSCQueue (see loPriInboxes/hiPriInboxes below).
	mainQ.init(&taskAllocator);

	for (unsigned int i = 0; i < num_workers; ++i) {
		immediateCoresInUse.push_back(std::make_unique<std::atomic<bool>>(false));
		loPri.push_back(std::make_unique<TaskDeque>());
		hiPri.push_back(std::make_unique<TaskDeque>());
		loPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		hiPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		loPriInboxes[i]->init(&taskAllocator);
		hiPriInboxes[i]->init(&taskAllocator);
	}
	// TWO PASSES, and the split is load-bearing. Creating a worker and STARTING it in the same
	// iteration meant worker 0 was running -- and reading `workers` in its own startup path
	// (Thread::StartWorker reads scheduler->workers.size()) -- while this loop was still
	// push_back()ing workers 1..N-1. That is a genuine data race on the vector: concurrent read
	// against a write that can REALLOCATE, so the reader can walk a buffer being freed underneath
	// it. reserve() alone does not fix it; a concurrent read of size() against a concurrent write
	// is still a race even when no reallocation occurs. Populating fully and starting afterwards
	// removes the overlap instead of narrowing it.
	//
	// Nothing needed a worker running before the vector was complete, so this costs nothing.
	// Found by ThreadSanitizer (bench/tsan_probe.cpp) on Linux; it is SHARED code, so the bug was
	// equally present on Windows -- it survived on x86 by timing luck, and is exactly the class
	// that turns into intermittent corruption under weaker memory ordering.
	workers.reserve(num_workers);
	for (unsigned int i = 0; i < num_workers; ++i) {
		auto worker = std::make_shared<Thread>(*this);
		worker->SetQueueIndex(i);
		workers.push_back(worker);
	}
	for (unsigned int i = 0; i < num_workers; ++i) {
		// Default scheme: worker i takes the (i+1)'th logical CPU that EXISTS, main keeps the first.
		// On the dense single-group machines everyone runs, that is literally i+1; on a machine with
		// unevenly sized processor groups it skips the holes in the flat id space (see above).
		// PhysicalOnly instead walks the list of distinct physical cores, so no two workers share a
		// core's execution units. The bare i+1 is the last resort for when topology is unavailable,
		// which is also the only case where no holes can be detected.
		size_t cpu;
		if (!physicalCpus.empty())                cpu = (size_t)physicalCpus[i + 1];
		else if (logicalCpus.size() > (size_t)i + 1) cpu = (size_t)logicalCpus[i + 1];
		else                                      cpu = i + 1;
		workers[i]->StartWorker(cpu);
	}
	for (auto& w : workers) {
		while (!w->Ready())
			std::this_thread::yield();
	}
	thread_id = thread_counter.fetch_add(1);
	poolActive.store(true, std::memory_order_release);
	poolMutex.unlock();
}

void TaskScheduler::WaitOnEvent(const std::string& eventName) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		// A noFiber task (see Task::noFiber) runs with no fiber underneath it -- there's
		// nothing to switch away to. This is a contract violation, not a transient failure.
		throw std::runtime_error("WaitOnEvent called from a task with no assigned fiber -- "
			"noFiber tasks must never suspend.");
	}

	auto& event = GetEvent(eventName);

	// Order matters. Become parkable (WANTS_SUSPEND) BEFORE registering, so any signal
	// that races in sees a resumable state (Resume() flips WANTS_SUSPEND->SUSPEND_SIGNALED
	// and the worker wakes us after the switch). AddWaiter only inserts -- it no longer
	// touches status. The event mutex serializes AddWaiter against Signal/SignalAll, so a
	// signal that lands after we register is guaranteed to find and wake us.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	event.AddWaiter(myTask);

	// Return via the fiber's homeCtx (the worker stamps it before each switch-in),
	// not thread_local schedulerCtx -- the waiter resumes on whatever worker the
	// event signal lands on, which may differ from this one.
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}
bool TaskScheduler::Push(Task* task) {
	return PushLocal(task);
}


void TaskScheduler::RunCounted(WaitGroup& wg, Task* t) {
	wg.n.fetch_add(1, std::memory_order_relaxed);
	t->waitGroup = &wg;
	Push(t);
}

void TaskScheduler::WaitFor(WaitGroup& wg) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		WaitOnEventDirectArmed([&wg](DirectEvent* ev) {
			std::lock_guard<std::mutex> lock(wg.mtx);
			wg.waiters.insert(ev);
			int old = wg.n.fetch_or(WaitGroup::WAITER_BIT, std::memory_order_acq_rel);
			if ((old & WaitGroup::COUNT_MASK) == 0) {
				wg.waiters.erase(ev);
				ev->Signal();   // already done -- wake ourselves so we don't park forever
			}
			});
		return;
	}
	else {
		// Same reentrancy hazard as SchedulerMutex::Lock, and the same two guards -- but NOT the
		// same yield policy. This loop already yields on the first unproductive pass, which is the
		// right behaviour when there is genuinely nothing to steal, so ContendedSpinStep's
		// 1000-pass counter would only make it spin longer for no gain.
		//
		// What it was missing: a caller holding a SchedulerMutex across WaitFor would run tasks that
		// could ask for that same mutex and deadlock against itself; and helping without marking the
		// depth let a helped task's own Lock() help AGAIN, nesting through WaitFor.
		//
		// Fork-join is unaffected. Normal callers hold no SchedulerMutex here, so t_heldMutexes is
		// zero and this behaves exactly as before -- the guard only bites on a pattern that already
		// self-deadlocked.
		while (wg.n.load(std::memory_order_acquire) > 0) {
			bool ranSomething = false;
			if (t_heldMutexes == 0) {
				++t_spinHelpDepth;
				ranSomething = TryRunStolenNoFiberTask();
				--t_spinHelpDepth;
			}
			if (!ranSomething)
				std::this_thread::yield();
		}
	}
}
void JLib::TaskScheduler::PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity)
{
	// 1. Manually link them locally: Task A -> Task B -> Task C
	for (size_t i = 0; i < count - 1; ++i) {
		tasks[i]->next.store(tasks[i + 1], std::memory_order_relaxed);
	}
	// The last task's next is already handled by the queue's exchange logic
	pendingTasks.fetch_add(count, std::memory_order_relaxed);
	// 2. Submit the pointers directly - NO wrappers, NO heap allocation
	// NOTE (corePref): the whole batch is placed at CorePref::Default (full-pool round-robin) onto ONE
	// worker regardless of members' individual corePrefs -- a batch is assumed homogeneous/Default.
	// Class-pinned tasks should go through Push() individually; once batched here, the receiving OWNER
	// runs them unvetted (see the enforcement-scope note in Task.h), though class-aware STEALING still
	// applies to whatever other workers try to take from this deque.
	if (cpuaffinity == 0)
	{
		int chosen = PickNextWorker();
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			std::this_thread::yield();
			chosen = PickNextWorker();
		}
		loPriInboxes[chosen]->push_batch(tasks[0], tasks[count - 1]);
		// FIX: this whole function previously never notified ANYONE after the push -- if
		// `chosen` happened to be genuinely asleep, the entire batch would sit undiscovered
		// until that worker was woken for some unrelated reason (a different push landing on
		// it, etc.). A worker's own cv is private; nothing wakes it without an explicit notify
		// targeting it specifically.
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();
	}
	else
	{
		int chosen = cpuaffinity - 1;
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			std::this_thread::yield();
			chosen = PickNextWorker();
		}
		loPriInboxes[chosen]->push_batch(tasks[0], tasks[count - 1]);
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();
	}
}

bool TaskScheduler::Push(uint8_t cpu_affinity, Task* task) {
	return PushLocal(task, cpu_affinity);
}

bool TaskScheduler::PushImmediate(uint8_t cpu_affinity, Task* task) {
	if (!task) return false;
	return PushToCore(cpu_affinity, task);
}

bool TaskScheduler::PushFork(Task* task) {
	if (!task) return false;
	if (!poolActive) return false;

	int worker_id;
	bool is_local_push = false;

	if (IsOnFiber()) {
		worker_id = Thread::GetCurrent()->qIndex;
		is_local_push = true;  // Pushing to current worker, skip busy check
	}
	else {
		worker_id = PickNextWorker(task->corePref);
		if (worker_id < 0) return false;
	}

	// Only check busy flag if NOT pushing to current worker
	if (!is_local_push && immediateCoresInUse[worker_id]->load(std::memory_order_acquire))
		return false;

	if (!is_local_push) {
		immediateCoresInUse[worker_id]->store(true, std::memory_order_release);
	}

	task->isForked = 1;
	pendingTasks.fetch_add(1, std::memory_order_relaxed);

	return PushLocal(task, worker_id);
}
void TaskScheduler::WaitOnEventArmed(const std::string& eventName, const std::function<void()>& arm) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventArmed called from a task with no assigned fiber -- "
			"noFiber tasks must never suspend.");
	}

	auto& event = GetEvent(eventName);

	// Same ordering as WaitOnEvent: become parkable, then register as a waiter, so a signal
	// that races in is not lost (Resume flips WANTS_SUSPEND->SUSPEND_SIGNALED and the worker
	// wakes us after the switch). Crucially, run 'arm' only AFTER both -- the arm callback
	// hooks the external wakeup (e.g. a GPU fence), and must not be able to fire SignalAll
	// before this fiber is a discoverable, resumable waiter.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	event.AddWaiter(myTask);

	if (arm) arm();

	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}

void TaskScheduler::WaitOnEventDirectArmed(const std::function<void(DirectEvent*)>& arm) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventDirectArmed called from a task with no assigned "
			"fiber -- noFiber tasks must never suspend.");
	}

	DirectEvent* e = eventPool.Acquire();   // pool sized for max concurrent waits (never null in practice)

	// Ordering is load-bearing and identical in spirit to WaitOnEventArmed:
	//  1. become parkable FIRST -- if we published the waiter first, a signal could Resume() us
	//     while still RUNNING, and Resume() is a no-op for unrecognized states -> LOST wakeup.
	//  2. publish the waiter, so a signal that lands now finds a resumable target.
	//  3. arm the external wakeup only AFTER both.
	//  4. suspend.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	e->waiter.store(myTask, std::memory_order_release);

	if (arm) arm(e);

	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);

	// Resumed: WE own release. Signal() already exchanged waiter->null and will not touch e again.
	eventPool.Release(e);
}

bool TaskScheduler::IsOnFiber() {
	auto* t = Thread::GetCurrent();
	// currentRunningTask alone isn't enough -- a noFiber task sets it too (see Worker()'s fast
	// path) but deliberately never gets a fiber. Callers use this to decide whether
	// WaitOnEvent*-style suspension is safe, so it must be false for a noFiber task.
	return t != nullptr && t->currentRunningTask != nullptr && t->currentFiber != nullptr;
}

Event& TaskScheduler::GetEvent(const std::string& name) {
	registryMtx.lock();
	if (eventRegistry.find(name) == eventRegistry.end())
		eventRegistry[name] = std::make_unique<Event>();
	Event& event = *eventRegistry[name];

	// Debug tripwire for the misuse documented on the declaration: a caller minting a fresh name
	// per operation. Left to grow, that ends as a registryMtx convoy after ~an hour of uptime that
	// looks like a deadlock and is miserable to diagnose from the symptom. One warning turns that
	// into a sentence naming the cause. Threshold is far above any plausible static name set.
	// Debug-only: this is a caller bug, not a condition the library should pay to check in release.
#if defined(_DEBUG) || defined(JLIB_DEVELOPMENT)
	if (eventRegistry.size() == 4096)
		fprintf(stderr,
			"[JLib::Scheduler] WARNING: event registry has reached 4096 named events. Named events "
			"are for a BOUNDED set of rendezvous points; a name minted per operation (e.g. "
			"\"fence_\" + counter) grows this map without bound and will convoy on registryMtx. "
			"Use WaitOnEventDirectArmed for per-operation waits. Last inserted: \"%s\"\n",
			name.c_str());
#endif

	registryMtx.unlock();
	return event;
}
void TaskScheduler::Pause() {
	paused.store(true, std::memory_order_seq_cst);
}
void TaskScheduler::Resume() {
	paused.store(false, std::memory_order_seq_cst);
	NotifyAll();
}
void TaskScheduler::Stop(Task* worker_task) {
	// Only the SCHEDULER's own stopFlag matters -- Task::stopFlag was removed (it never had a
	// single reader; workers only ever check this scheduler-level flag). The parameter is kept
	// for API compatibility with existing callers.
	(void)worker_task;
	stopFlag.store(true, std::memory_order_release);
}


// Steals ONE task for a NON-worker helper (e.g. main spinning in WaitFor, or a noFiber spinning
// on a SchedulerMutex). Random-start, hiPri-then-loPri scan with fairness: after
// kStealFairnessWindow consecutive hiPri steals it forces a loPri scan so loPri work can't starve
// behind a stream of hiPri steals. Single-item steal() is the only correct steal in the lock-free
// deque (see TaskDeque.h). Returns nullptr if nothing was stealable anywhere.
Task* TaskScheduler::GetTask() {
	bool forceLoPri = (consecutiveHiPriSteals >= kStealFairnessWindow);

	// NOFIBER- AND CLASS-VETTED at the deque (steal_if): GetTask's ONLY caller is TryRunStolenNoFiberTask,
	// whose fiberless caller can't run anything that might suspend. Previously this stole blind and
	// Requeued fiber-backed tasks -- a claim-CAS + full re-push + notify to move a task nowhere (deque
	// contention + thrash). Now a fiber-backed task is never claimed at all: it stays put for a real worker,
	// and the scan just moves to the next victim. Class matters too because TRSFJ's callers vary: the
	// SchedulerMutex spin path invokes it FROM WORKERS (thief class = that worker's), while main/WaitFor
	// helpers are non-workers pinned to CPU 0 -- a P-core -- so they vet as P. corePref is the sole
	// placement authority EVERYWHERE, including helper steals.
	// Thief class, NOT assumed: a worker (SchedulerMutex/CV spin inside a noFiber lands here from
	// workers too) uses its pinned class; any NON-worker thread (main, or an arbitrary app thread
	// hitting a scheduler primitive -- possibly unpinned and floating) asks the OS where it is RIGHT
	// NOW via GetCurrentProcessorNumber + the per-CPU class table. "Would this noFiber run on a P or
	// E core?" is answered by where the caller is actually standing.
	Thread* thief = Thread::GetCurrent();
	const bool thiefIsP = thief
		? (isPCore[thief->qIndex] != 0)
		: (isPCpu[JLib::platform::CurrentCpu() & 63] != 0);
	const bool degen = pWorkers.empty() || eWorkers.empty();
	auto fastOnly = [&](Task* t) {
		return t->noFiber != 0 && StealClassCompatible(t, thiefIsP, degen);
	};

	if (!forceLoPri) {
		size_t numThreads = hiPri.size();
		size_t start = rand() % numThreads;
		for (size_t i = 0; i < numThreads; ++i) {
			size_t target = (start + i) % numThreads;
			if (auto s = hiPri[target]->steal_if(fastOnly)) {
				consecutiveHiPriSteals++;
				return *s;
			}
		}
	}

	// loPri: forced by fairness, or the hiPri-miss fallback.
	consecutiveHiPriSteals = 0; // reset the fairness counter whenever we scan loPri
	size_t numThreads = loPri.size();
	size_t start = rand() % numThreads;
	for (size_t i = 0; i < numThreads; ++i) {
		size_t target = (start + i) % numThreads;
		if (auto s = loPri[target]->steal_if(fastOnly))
			return *s;
	}

	return nullptr;
}

bool TaskScheduler::TryRunStolenNoFiberTask() {
	// Steal ONE noFiber and run it to completion right here with the full completion bookkeeping
	// Worker()'s fast path does. GetTask vets the noFiber flag AT THE DEQUE (steal_if) -- a fiber-backed task
	// is never claimed by this fiberless caller in the first place, so the old steal-then-Requeue
	// relocation path (claim CAS + re-push + notify = contention/thrash) no longer exists.
	Task* task = GetTask();
	if (!task) return false;

	task->Execute();
	if (task->waitGroup) {
		int old = task->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
		if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
			task->waitGroup->WakeAll();   // only touches wg if someone registered
	}
	task->~Task();
	taskAllocator.Free(task);
	pendingTasks.fetch_sub(1, std::memory_order_acq_rel);

	if (EpochManager::Instance().RetiredCount() > 512) {
		EpochManager::Instance().Tick();
	}
	return true;
}

TaskAllocator* TaskScheduler::GetAllocator() {
	return &taskAllocator;
}
void TaskScheduler::WaitAll() {
	while (pendingTasks.load(std::memory_order_acquire) > 0)
		std::this_thread::yield();
}

Task* TaskScheduler::CreateTask(void(*fn)(void*), void* data, uint8_t hipri, FiberSize size, uint8_t noFiber, CorePref corePref) {
	void* mem = taskAllocator.Alloc();
	if (!mem) return nullptr;
	Task* t = ::new (mem) Task(fn, data, hipri, size);
	t->noFiber = noFiber;
	t->corePref = corePref;
	return t;
}

bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task) return false;

	size_t num_workers = workers.size();
	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			loPriInboxes[idx]->push(task);
			pendingTasks.fetch_add(1, std::memory_order_release);
			// Targeted at worker idx specifically, not NotifyAll() -- only that one worker's
			// inbox actually changed. MarkQueuedWork() (release-ordered, matching
			// Thread.h's hasQueuedWork comment) pairs with the worker's own acquire-load in its
			// sleep predicate, closing the same notify-loss race the old blanket approach
			// happened to also close, just without waking every other worker for nothing.
			workers[idx]->MarkQueuedWork();
			workers[idx]->NotifyWorker();
		}
		else
			return false;
	}
	else {
		uint8_t chosen = PickNextWorker(task->corePref);
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			chosen = PickNextWorker(task->corePref);
		}
		if(task->hiPri)
			hiPriInboxes[chosen]->push(task);
		else
			loPriInboxes[chosen]->push(task);
		pendingTasks.fetch_add(1, std::memory_order_release);
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();

	}
	return true;
}
bool TaskScheduler::Requeue(Task* task) {
	if (!task) return false;
	// Re-queue a paused task (resumed after Suspend). Unlike PushLocal this does NOT
	// bump pendingTasks -- the task was already counted at its original submission and
	// is only resuming, not newly created. (The yield path does the same, via the
	// worker's push_bottom.) Otherwise every suspend->resume cycle leaks +1.
	uint8_t chosen = PickNextWorker(task->corePref);
	while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
		chosen = PickNextWorker(task->corePref);
	}
	if(task->hiPri)
		hiPriInboxes[chosen]->push(task);
	else
		loPriInboxes[chosen]->push(task);
	workers[chosen]->MarkQueuedWork();
	workers[chosen]->NotifyWorker();
	return true;
}
bool TaskScheduler::PushToCore(size_t core_id, Task* task) {
	if (core_id < 1) return false;
	if (!poolActive) return false;
	if (!task) return false;

	size_t idx = (core_id - 1) % workers.size();
	if (immediateCoresInUse[idx]->load(std::memory_order_acquire)) return false;

	// Marks this core busy-with-a-fork until Thread::Worker() clears it on completion (see
	// the is_handling_fork cleanup in both the noFiber and fiber-DEAD paths). If the forked
	// task never returns (a long-running subsystem pinned here for the program's lifetime),
	// this correctly STAYS true forever -- which is what makes PickNextWorker()'s existing
	// skip-if-busy check actually mean something: without setting this, a never-returning fork
	// would leave this worker's INBOX (not its deque -- that's still fully stealable) silently
	// accepting new round-robin-dispatched work that nothing would ever drain again.
	immediateCoresInUse[idx]->store(true, std::memory_order_release);

	// Deliberately does NOT call MarkQueuedWork(): a forked/immediate task bypasses the shared
	// deques/inboxes entirely (goes straight into workers[idx]->immediateTask below) and wakes
	// ONLY that one targeted worker via SetImmediateTask's own `immediate` flag + notify --
	// hasQueuedWork is specifically for the deque/inbox case, which this isn't.
	pendingTasks.fetch_add(1, std::memory_order_relaxed);
	workers[idx]->SetImmediateTask(task);
	workers[idx]->NotifyWorker();
	return true;
}
int TaskScheduler::PickNextWorker(CorePref pref) {
	// Placement is governed SOLELY by CorePref (see Task.h) -- queue priority (hiPri) is never consulted
	// here; the two axes are fully orthogonal by design. Default/Any/Wide all mean "no class preference"
	// and fall through to the original full-pool round-robin below.

	// Round-robin a worker subset, returning the first NON-pinned worker (immediateCoresInUse = a
	// persistent PushImmediate/PushFork claim), or -1 if the set is empty or every worker in it is pinned
	// -- which tells the caller to SPILL to the other class rather than block on an unavailable core.
	auto pickFrom = [this](std::vector<int>& set, std::atomic<size_t>& cur) -> int {
		size_t m = set.size();
		if (m == 0) return -1;
		size_t start = cur.load(std::memory_order_relaxed);
		for (size_t i = 0; i < m; ++i) {
			int idx = set[(start + i) % m];
			if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
				cur.store((start + i + 1) % m, std::memory_order_relaxed);
				return idx;
			}
		}
		return -1;
	};

	// Preference is a HINT, not a constraint (per the "don't wait when another core is free" rule): try the
	// preferred class, then SPILL to the other. On top of this, work-conserving stealing (an idle worker
	// steals a hiPri task off a busy one) covers the class-backlog-while-other-idle case at run time -- so
	// a task never sits queued while any core is idle.
	if (pref == CorePref::P || pref == CorePref::E) {
		const bool preferP = (pref == CorePref::P);
		int idx = preferP ? pickFrom(pWorkers, nextPWorker) : pickFrom(eWorkers, nextEWorker);
		if (idx < 0) idx = preferP ? pickFrom(eWorkers, nextEWorker) : pickFrom(pWorkers, nextPWorker);
		if (idx >= 0) return idx;
	}

	// Default/Any/Wide land here directly (no class preference); for P/E it's the last resort -- sets
	// empty (not built) or EVERY worker pinned. The original full-pool round-robin, unchanged.
	size_t n = workers.size();
	for (size_t i = 0; i < n; ++i) {
		size_t j = (nextWorker + i) % n;
		if (!immediateCoresInUse[j]->load(std::memory_order_acquire)) {
			nextWorker = (j + 1) % n;
			return static_cast<int>(j);
		}
	}
	int fallback = static_cast<int>(nextWorker);
	nextWorker = (fallback + 1) % n;
	return fallback;
}

// ---- Starvation prevention implementation ----
uint64_t TaskScheduler::GetCurrentTimeMs() const {
	using namespace std::chrono;
	return duration_cast<milliseconds>(high_resolution_clock::now().time_since_epoch()).count();
}

// ---- Priority inheritance implementation ----
Task* TaskScheduler::GetCurrentTask() const {
	// Access the current worker thread via thread-local storage (Thread::GetCurrent() returns
	// the thread_local Thread::instance if this is a worker thread, nullptr otherwise).
	// If we're on a worker thread with a running task, return it; otherwise nullptr.
	Thread* currentThread = Thread::GetCurrent();
	if (currentThread && currentThread->currentRunningTask) {
		return currentThread->currentRunningTask;
	}
	return nullptr; // Not on a worker thread, or no task currently running
}

void TaskScheduler::BoostTaskPriority(Task* task) {
	if (!task) return;

	// Only boost if not already boosted (no lock needed: only one thread modifies this task)
	if (task->priorityBoost == 0) {
		task->priorityBoost = task->hiPri;
		task->hiPri = 1; // boost to high priority
	}
}

void TaskScheduler::UnboostTaskPriority(Task* task) {
	if (!task) return;
	// Restore original priority if boosted (no lock needed: only one thread modifies this task)
	if (task->priorityBoost != 0) {
		task->hiPri = task->priorityBoost; // restore original priority
		task->priorityBoost = 0;
	}
}

void TaskScheduler::CleanupTaskMetadata(Task* task) {
	if (!task) return;
	// Metadata is stored directly on task, no cleanup needed (task is about to be freed anyway)
	// Just restore priority if it was boosted
	UnboostTaskPriority(task);
}


void SchedulerMutex::Lock() {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		// Fiber context: suspend on contention instead of blocking thread
		Task* callerTask = (TaskScheduler::IsInitialized()) ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			if (!locked) {
				locked = true;
				spinLock.clear(std::memory_order_release);
				{
					while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
					lockHolder = callerTask;
					holderLock.clear(std::memory_order_release);
				}
				return;
			}
			waitingFibers.push(current);
			spinLock.clear(std::memory_order_release);
		}
		Thread::Suspend(current);
		// Resumed: we have the lock
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
	}
	else {
		// Bare thread: cannot suspend, so help with stolen noFiber work while waiting. See the
		// block comment above ContendedSpinStep for what that costs and what guards it.
		while (!Try_Lock()) ContendedSpinStep();
		// Ownership is counted inside Try_Lock, which is the single place a bare thread takes this
		// lock -- including when a caller uses Try_Lock directly.
	}
}

void SchedulerMutex::Unlock()
{
	// Guarded rather than unconditional: a fiber never incremented (it can migrate mid-hold, so a
	// per-thread count would be wrong), and an unbalanced Unlock must not wrap the counter to
	// SIZE_MAX and silently disable helping on this thread forever.
	if (OnBareThread() && t_heldMutexes > 0) --t_heldMutexes;

	Task* wasHolder;
	Fiber* nextFiber = nullptr;
	{
		while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
		wasHolder = lockHolder;
		lockHolder = nullptr;
		holderLock.clear(std::memory_order_release);
	}

	{
		while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
		if (!waitingFibers.empty()) {
			nextFiber = waitingFibers.front();
			waitingFibers.pop();
		}
		else {
			locked = false;
		}
		spinLock.clear(std::memory_order_release);
	}

	// Only unboos if scheduler is initialized AND it's not a recursive/shutdown path
	if (wasHolder && TaskScheduler::IsInitialized()) {
		TaskScheduler::Instance().UnboostTaskPriority(wasHolder);
	}

	if (nextFiber) {
		Thread::Resume(nextFiber);
	}
}

bool SchedulerMutex::Try_Lock()
{
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (!locked) {
		locked = true;
		spinLock.clear(std::memory_order_release);
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
		// Every bare-thread acquisition passes through here, whether via Lock()'s spin loop or a
		// direct Try_Lock() by the caller, which is why the count lives here and not in Lock().
		if (OnBareThread()) ++t_heldMutexes;
		return true;
	}
	spinLock.clear(std::memory_order_release);
	return false;
}

void SchedulerSemaphore::Wait() {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;
	if (current != nullptr) {
		{
			// Tight spin-lock to protect inner state variables in user-space
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }

			if (permits > 0) {
				--permits;
				spinLock.clear(std::memory_order_release);
				return;
			}
			waitingFibers.push(current);
			spinLock.clear(std::memory_order_release);
		}
		Thread::Suspend(current);
	}
	else {
		// Same contended-wait discipline as SchedulerMutex; see ContendedSpinStep.
		//
		// This RESPECTS t_heldMutexes but does not add to it, and the asymmetry is deliberate. A
		// semaphore permit has no owner: the thread that takes one is frequently not the thread that
		// returns it, which is the entire point of a producer/consumer semaphore. Counting a Wait as
		// an acquisition would make a consumer's count climb forever and permanently disable helping
		// on that thread. So the inversion guard covers mutexes, which have real ownership, and a
		// thread holding only a permit is a documented gap rather than a tracked one.
		while (!Try_Wait()) ContendedSpinStep();
	}
}

// Ownership is claimed AFTER Wait() returns, not before: while still acquiring, this thread holds
// nothing, so helping is legitimate and desirable. It only stops helping once it actually owns the
// permit. See the class comment in TaskScheduler.h for why Wait() itself cannot do this.
SchedulerSemaphore::ScopedPermit::ScopedPermit(SchedulerSemaphore& s) : sem(s) {
	sem.Wait();
	if (OnBareThread()) ++t_heldMutexes;
}

SchedulerSemaphore::ScopedPermit::~ScopedPermit() {
	// Drop ownership BEFORE returning the permit. Signal() can resume a waiting fiber, and this
	// thread should already be back to "owns nothing" by the time anything else runs.
	// Guarded on >0 because a fiber never incremented, and because an unbalanced count must not
	// wrap and disable helping on this thread forever.
	if (OnBareThread() && t_heldMutexes > 0) --t_heldMutexes;
	sem.Signal();
}

bool SchedulerSemaphore::Try_Wait() {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (permits > 0) {
		--permits;
		spinLock.clear(std::memory_order_release);
		return true;
	}
	spinLock.clear(std::memory_order_release);
	return false;
}

void SchedulerSemaphore::Signal()
{
	// 1. Acquire the user-space spinlock
	while (spinLock.test_and_set(std::memory_order_acquire)) {
		platform::CpuRelax();
	}

	// 2. Safely manipulate the queue and permits
	if (!waitingFibers.empty()) {
		Fiber* fiber = waitingFibers.front();
		waitingFibers.pop();

		// 3. Release lock BEFORE resuming the fiber to minimize contention overhead
		spinLock.clear(std::memory_order_release);

		Thread::Resume(fiber);
	}
	else {
		if (permits < maxPermits) {
			++permits;  // no one waiting, just increment
		}
		// 3. Release lock on this execution path
		spinLock.clear(std::memory_order_release);
	}
}

void SchedulerConditionVariable::LockQueue() {
	while (spinLock.test_and_set(std::memory_order_acquire)) {
		platform::CpuRelax();
	}
}

void SchedulerConditionVariable::UnlockQueue() {
	spinLock.clear(std::memory_order_release);
}

void SchedulerConditionVariable::Wait(SchedulerMutex& mutex) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		// 1. Create a transient, local semaphore initialized to 0 permits
		SchedulerSemaphore localWaitSemaphore(0, 1);

		// 2. Lock the CV internal queue and push our wait handle
		LockQueue();
		waitingQueue.push(&localWaitSemaphore);
		UnlockQueue();

		// 3. Release the outer engine mutex so other threads/fibers can work
		mutex.Unlock();

		// 4. Suspend the fiber by waiting on our local semaphore.
		// Your existing semaphore code handles fiber suspension seamlessly here!
		localWaitSemaphore.Wait();

		// 5. Re-acquire the outer engine lock before returning control to the task
		mutex.Lock();
	}
	else {
		// Bare-thread fallback. The unlock happens FIRST, which is what makes helping legitimate
		// here: by the time ContendedSpinStep runs, this thread owns nothing, so the inversion the
		// t_heldMutexes guard exists to prevent cannot arise. It still needs the recursion guard and
		// the yield, hence the shared helper rather than an inline copy.
		mutex.Unlock();
		ContendedSpinStep();
		mutex.Lock();
	}
}

void SchedulerConditionVariable::Notify_One() {
	SchedulerSemaphore* nextSemaphore = nullptr;

	LockQueue();
	if (!waitingQueue.empty()) {
		nextSemaphore = waitingQueue.front();
		waitingQueue.pop();
	}
	UnlockQueue();

	// Signal the semaphore out-of-lock to maximize throughput
	if (nextSemaphore) {
		nextSemaphore->Signal();
	}
}

void SchedulerConditionVariable::Notify_All() {
	std::queue<SchedulerSemaphore*> localQueue;

	// Flush the global wait list into a local thread-isolated stack instantly
	LockQueue();
	std::swap(waitingQueue, localQueue);
	UnlockQueue();

	// Signal all waiting contexts sequentially
	while (!localQueue.empty()) {
		SchedulerSemaphore* sem = localQueue.front();
		localQueue.pop();
		sem->Signal();
	}
}