// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Thread.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include "../include/TaskDAG.h"   // OnTaskDiscarded: a discarded DAG task still owes its dependents
#include "../include/IoReactor.h" // Join() stops the completion threads before clearing the pool
#include "../include/Timer.h"     // ...and the timer thread, for the same reason
#include "../include/platform.h"
#include "../include/Topology.h"
#include <stdexcept>
#include <cstdio>      // fprintf -- the debug-only event-registry tripwire in GetEvent
#include <vector>
#include <chrono>
using namespace JLib;

// ================= CONTENDED-WAIT HELPER, shared by SchedulerMutex and SchedulerSemaphore =======
// A bare thread that blocks on one of these cannot suspend, so instead of burning the core it runs
// a stolen Native task per iteration. That is work-conserving and it is also the single most
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
//     Native tasks are stolen here, so if the resumer is a fiber task this thread structurally
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
			const bool ranSomething = TaskScheduler::Instance().TryRunStolenNativeTask();
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

	// Plain CpuRelax retries a contended bare-thread Try_Lock/Try_Wait takes before escalating to
	// ContendedSpinStep. ZERO IS THE MEASURED OPTIMUM. Do not raise it.
	//
	// THE IDEA, AND WHY IT IS WRONG. TryRunStolenNativeTask() walks steal candidates across every
	// deque -- real work, not a cheap check -- so paying it on the very first failed try looks
	// wasteful when the holder is about to release within a handful of cycles. A brief plain spin
	// first "obviously" catches that fast-flip case for a few PAUSE instructions instead. That
	// argument is intuitive, it is what this code did for part of 2.7.0's development, and
	// bench/lock_contention.cpp says it is backwards.
	//
	// MEASURED 2026-08-23 (see that file's RESULTS for the full table). Sweeping 0/16/64/256/1024
	// with the arms rotating inside one process, against an A/A noise floor and a fiber control:
	//
	//   tiny critical section, 8 bare contenders, saturated pool, 8 workers
	//     spin=0     p50     0 ns   p99  38,900 ns   17.8 M acq/s   304 k pool tasks/s
	//     spin=64    p50 4,400 ns   p99  61,900 ns    0.7 M acq/s   195 k pool tasks/s
	//     spin=1024  p50 5,000 ns   p99  68,200 ns    0.6 M acq/s   171 k pool tasks/s
	//
	// Monotonic in the spin count, reproduced at 8 and 31 workers, and 0 wins on latency AND lock
	// throughput AND the pool's own throughput simultaneously -- so this is not the
	// latency-versus-throughput trade it was expected to be. There is no regime in the sweep where
	// spinning first pays: uncontended is unaffected (Try_Lock succeeds on the first attempt and
	// never reaches here) and a long critical section is unaffected (the spin always exhausts).
	//
	// WHY, most likely: ContendedSpinStep is not merely a slower retry, it is BACKOFF. A tight
	// CpuRelax loop re-runs Try_Lock's spinLock.test_and_set at full rate, and every one of those is
	// a write to the same cache line the HOLDER needs in order to finish and release. Spinning
	// harder starves the thread being waited on. The expensive-looking steal attempt spaces those
	// writes out, which is why it wins even in the bg=off rows where the steal always fails and
	// finds nothing to run.
	//
	// The knob survives only so the result stays reproducible; see JLIBSCHED_TUNABLE_FAST_SPIN in
	// CMakeLists.txt. At 0 the comparison below folds away and the loop compiles to exactly what
	// shipped before this experiment.
#ifndef JLIBSCHED_FAST_SPIN_TRIES
#define JLIBSCHED_FAST_SPIN_TRIES 0
#endif
#if defined(JLIBSCHED_TUNABLE_FAST_SPIN)
	// DIAGNOSTIC BUILDS ONLY (bench/lock_contention.cpp). A shipping build gets the constexpr below
	// and the compiler folds the comparison away entirely.
	//
	// This exists because the constant could not be measured any other way. As a compile-time
	// constant each candidate value is a separate BINARY, so the arms cannot be interleaved inside
	// one process -- and measured against an A/A control (the same binary built twice), the
	// process-to-process drift on this machine ran to a p90 of 52% and a max of 118% on lock
	// throughput. No plausible effect survives that floor. Making the value settable at runtime lets
	// one process rotate through every arm round by round, which puts all of them under the same
	// machine conditions instead of charging each arm whatever the OS was doing when it launched.
	//
	// Plain int, not std::atomic: it is written between measured rounds with no contention in
	// flight, and making it atomic would put an acquire load in the spin loop being measured.
	int g_fastSpinTries = JLIBSCHED_FAST_SPIN_TRIES;
#define JLIB_FAST_SPIN_LIMIT (g_fastSpinTries)
#else
	constexpr int kFastSpinTries = JLIBSCHED_FAST_SPIN_TRIES;
#define JLIB_FAST_SPIN_LIMIT (kFastSpinTries)
#endif

	// Shared shape of SchedulerMutex::Lock's and SchedulerSemaphore::Wait's bare-thread paths.
	// At the shipped bound of 0 this is exactly `while (!tryOp()) ContendedSpinStep();` -- the
	// comparison is against a compile-time constant and folds away entirely. `tryOp` is Try_Lock or
	// Try_Wait; true means acquired.
	template <typename TryOp>
	inline void SpinThenHelp(TryOp&& tryOp) {
		int fastSpins = 0;
		(void)fastSpins;
		while (!tryOp()) {
			if (fastSpins < JLIB_FAST_SPIN_LIMIT) {
				++fastSpins;
				platform::CpuRelax();
			}
			else {
				ContendedSpinStep();
			}
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

#if defined(JLIBSCHED_TUNABLE_FAST_SPIN)
// Diagnostic accessors for the tunable above. Deliberately NOT part of TaskScheduler: this is not a
// scheduler setting, it is a benchmark hook that only exists in a build configured for measurement.
// Set it only while no contended acquisition is in flight (bench/lock_contention.cpp changes it
// between rounds, never during one).
namespace JLib {
	namespace detail {
		void SetFastSpinTries(int n) { g_fastSpinTries = (n < 0) ? 0 : n; }
		int  GetFastSpinTries()      { return g_fastSpinTries; }
	}
}
#endif

static_assert(sizeof(Task) <= TaskAllocator::SLOT, "Task doesn't fit a slot");
static_assert(alignof(Task) <= 16, "Task over-aligned for a slot");

TaskScheduler* TaskScheduler::instance = nullptr;
// See the declaration for why this is per-thread. Zero-initialized, so no guard variable and no
// initialization check on the steal path.
thread_local int TaskScheduler::consecutiveHiPriSteals = 0;
GlobalFiberPool* TaskScheduler::globalPool = nullptr;

TaskScheduler::TaskScheduler(size_t poolSize) {
	StartPool(poolSize);
}
// Init-only, and a plain bool for the same reason as EpochManager's selfReclaim: written once before
// any thread exists, read while sizing the pool. Nothing races it.
static bool g_reserveTimerCore = false;

static bool g_reserveIoCore = false;
// Cores the APP has claimed for threads of its own. Read only by GetSafeTC, so it matters only
// before Init -- see SetReservedCores.
static std::atomic<unsigned> g_reservedUserCores{ 0 };

// EnableIoReactor implies EnableTimers, and the implication is applied HERE rather than checked at
// the point of use -- so there is one moment where the two are made consistent, and no path that can
// observe I/O on with timers off.
//
// Turning I/O back OFF deliberately leaves timers on: it cannot know whether they were enabled on
// their own account, and silently disabling a service the app asked for is worse than leaving one
// enabled that it no longer needs.
void TaskScheduler::EnableTimers(bool on) noexcept { g_reserveTimerCore = on; }
bool TaskScheduler::TimersEnabled() noexcept { return g_reserveTimerCore; }

static unsigned g_ioCompletionThreads = 1;

void TaskScheduler::EnableIoReactor(bool on, unsigned completionThreads) noexcept {
	g_reserveIoCore = on;
	// Clamped rather than trusted: zero threads would mean nothing ever drains the port and every
	// operation would hang, which is a worse failure than being told a number was ignored.
	g_ioCompletionThreads = (completionThreads == 0) ? 1 : completionThreads;
	if (on) g_reserveTimerCore = true;
}
bool TaskScheduler::IoReactorEnabled() noexcept { return g_reserveIoCore; }
unsigned TaskScheduler::IoCompletionThreads() noexcept { return g_ioCompletionThreads; }

void TaskScheduler::SetReserveTimerCore(bool reserve) noexcept { EnableTimers(reserve); }
bool TaskScheduler::ReserveTimerCore() noexcept { return TimersEnabled(); }
void TaskScheduler::SetReserveIoCore(bool reserve) noexcept { EnableIoReactor(reserve); }
bool TaskScheduler::ReserveIoCore() noexcept { return IoReactorEnabled(); }


// PRINTS WHAT THE SLAB ACTUALLY USED, and the line to paste because of it.
//
// HIGH-WATER, NOT LIVE, is the number to configure against: live answers "checked out right now",
// and by the time it is read the peak has been and gone. The high-water mark costs nothing to
// maintain -- refill() prefers recycled slots and advances the bump cursor only when the free list
// could not fill a batch, so that cursor already is the peak the pool had to make resident.
// BUILT AS TEXT, NOT PRINTED, because the application that most needs this has no stdout.
//
// HIGH-WATER vs PEAK-LIVE vs CONFIGURED are three different numbers and conflating them is what made
// the first version of this report useless. Size against PEAK-LIVE: `resident` equals capacity in a
// non-lazy build, because Prefault touches every slot at construction.
std::string TaskScheduler::SlabUsageString(const char* label) {
	char line[256];
	std::string out;

	if (!instance) {
		std::snprintf(line, sizeof(line), "[%s] scheduler not initialized\n", label);
		return std::string(line);
	}
	const auto u = instance->taskAllocator.UsageProfile();
	const TaskAllocator::ClassUsage* cs[4] = { &u.c64, &u.c80, &u.c128, &u.c256 };

	std::snprintf(line, sizeof(line), "\n=== %s ===\n", label);                      out += line;
	std::snprintf(line, sizeof(line),
		"  class  configured    resident   peak-live        live  grown\n");         out += line;

	std::size_t resBytes = 0, capBytes = 0;
	long long   peakBytes = 0;
	for (const auto* c : cs) {
		resBytes  += c->resident * c->slotBytes;
		capBytes  += c->capacity * c->slotBytes;
		peakBytes += c->peakLive * (long long)c->slotBytes;
		std::snprintf(line, sizeof(line), "  %4zuB  %10zu  %10zu  %10lld  %8lld  %5zu\n",
			c->slotBytes, c->capacity, c->resident, c->peakLive, c->live, c->extents);
		out += line;
	}
	std::snprintf(line, sizeof(line),
		"  reserved %.1f MB, resident %.1f MB, peak demand %.2f MB\n",
		(double)capBytes / (1024.0 * 1024.0),
		(double)resBytes / (1024.0 * 1024.0),
		(double)peakBytes / (1024.0 * 1024.0));                                      out += line;

	// THE ACTIONABLE LINE, built from PEAK-LIVE rather than residency -- residency in a non-lazy
	// build is the number you already configured, so suggesting from it would hand back the input.
	// Headroom because the peak is what THIS run needed, and sizing to the exact peak means growing
	// on the first busier frame, which is the hitch the number exists to avoid.
	std::snprintf(line, sizeof(line), "  suggested (measured peak +50%%):\n");        out += line;
	std::snprintf(line, sizeof(line), "    JLib::TaskScheduler::SlabSizes s;\n");     out += line;
	std::snprintf(line, sizeof(line),
		"    s.slots64 = %zu; s.slots80 = %zu; s.slots128 = %zu; s.slots256 = %zu;\n",
		(std::size_t)(u.c64.peakLive  + u.c64.peakLive  / 2) + 64,
		(std::size_t)(u.c80.peakLive  + u.c80.peakLive  / 2) + 64,
		(std::size_t)(u.c128.peakLive + u.c128.peakLive / 2) + 64,
		(std::size_t)(u.c256.peakLive + u.c256.peakLive / 2) + 64);                  out += line;
	std::snprintf(line, sizeof(line),
		"    JLib::TaskScheduler::SetSlabSizes(s);   // before Init()\n");            out += line;

	for (const auto* c : cs) {
		if (c->extents) {
			std::snprintf(line, sizeof(line),
				"  NOTE: the %zuB class GREW %zu time(s) -- under-configured for this run.\n",
				c->slotBytes, c->extents);
			out += line;
		}
	}
	return out;
}

// DELIVERY, not formatting -- the text comes from SlabUsageString above.
//
// OutputDebugStringA ON WINDOWS, and it is the point rather than a nicety: a windowed application
// has no stdout, and even with a console the process exits before the last numbers can be read.
// The debugger's Output window needs neither a console nor a pause, and it keeps the text after the
// process is gone. Same reason the free-list canary reports through it.
void TaskScheduler::ReportSlabUsage(const char* label) {
	const std::string s = SlabUsageString(label);
	std::printf("%s", s.c_str());
#if defined(_WIN32)
	::OutputDebugStringA(s.c_str());
#endif
	// Per-class alloc/free/refill RATES -- which class is hot, and whether the size-class split
	// matches the workload. Sharded per thread, and a no-op unless JLIBSCHED_ALLOC_STATS is on.
	TaskAllocator::ReportStats();
}


void TaskScheduler::SetReservedCores(unsigned n) noexcept {
	g_reservedUserCores.store(n, std::memory_order_relaxed);
}
unsigned TaskScheduler::GetReservedCores() noexcept {
	return g_reservedUserCores.load(std::memory_order_relaxed);
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
	//
	// AND ONE MORE when the app has declared it will use deadlines. TimerQueue runs a thread of its
	// own -- it has to, because it is the only place in the library a TIMED wait is allowed, and a
	// worker that can return unsignalled is how a lost wakeup turns into "occasionally slow" instead
	// of "hung". A thread is the cheap half of that trade: it sleeps untimed whenever nothing is
	// armed, so it costs a slot in the census and almost no CPU. See SetReserveTimerCore.
	unsigned int cores = std::thread::hardware_concurrency();
	if (cores <= 1) return 1;

	unsigned int reserved = 1;                       // main
	if (g_reserveTimerCore) reserved += 1;           // the timer thread
	// ONE CORE PER COMPLETION THREAD. Set together by EnableIoReactor precisely so these two
	// numbers cannot drift apart -- a pool sized for one thread while four drain the port is the
	// silent oversubscription the opt-in exists to prevent.
	if (g_reserveIoCore)    reserved += g_ioCompletionThreads;
	// AND WHATEVER THE APP RESERVED FOR ITS OWN THREADS. Same census argument as the two above, just
	// with the count coming from the caller instead of from a feature flag: a thread the scheduler
	// does not own still occupies a core, and one it does not KNOW about is a core that quietly went
	// missing. Declaring it here is what keeps workers + main + timer + io + yours an exact fit.
	//
	// This is the replacement for PushImmediate. Pinning a pool worker to a blocking subsystem took
	// a worker out of a WORK-STEALING pool and spilled its queue to everyone else; reserving a core
	// and running a plain std::thread gets the same accounting with none of the invariants.
	reserved += g_reservedUserCores.load(std::memory_order_relaxed);
	if (cores <= reserved) return 1;
	return static_cast<size_t>(cores - reserved);
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
// Disposal must match what the completion paths do, minus the payload: release the WaitGroup FIRST
// so nothing waiting on this work blocks forever on something abandoned, then clean up and free.
bool TaskScheduler::DiscardIfCancelled(Task* task) {
	if (!task || task->started || !IsTaskCancelled(task)) return false;

	// WHAT THIS TASK STILL OWES, before it is thrown away. A subsystem that wrapped fn/data gets its
	// completion hook run BY fn, and fn is exactly what discarding skips -- so without this a
	// cancelled TaskDAG node never fires its dependents and the graph, plus anyone in WaitFor on it,
	// stops forever. Runs BEFORE the waitGroup decrement, matching the order the executed path uses
	// (fn -> completion -> waitGroup), so nothing observes this task as finished ahead of the work it
	// releases.
	TaskDAG::OnTaskDiscarded(task);

	if (task->waitGroup) {
		const int old = task->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
		if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
			task->waitGroup->WakeAll();
	}
	CleanupTaskMetadata(task);
	DestroyTask(task);
	taskAllocator.Free(task);
	return true;
}

void TaskScheduler::ProcessMainThread() {
	if (!poolActive) return;
	Task* t;
	while (mainQ.pop(t)) {
		if (!t) continue;
		if (DiscardIfCancelled(t)) continue;
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

	// STOP THE SERVICE THREADS FIRST, BEFORE THE WORKERS THEY PUSH INTO.
	//
	// Join() used to take down `workers` and nothing else, so a reactor completion thread and the
	// timer thread outlived it. That is not merely untidy: both PUSH TASKS, and the block below
	// clears `workers` and `mainQ`. A completion landing in that window
	// indexes vectors that are being emptied. The window is small, which is the worst kind -- it
	// makes the failure rare enough to look like something else.
	//
	// ORDER IS THE WHOLE POINT. Producers stop, then consumers: stopping the workers first would
	// leave a completion thread pushing into a pool that can no longer drain, which is a hang rather
	// than a crash. Both stops are idempotent and both layers are opt-in, so this is a no-op for a
	// job-system-only user.
	//
	// Init() BRINGS THEM BACK -- StartPool calls the matching Start() on each, so a Join-then-Init
	// cycle works. Neither Stop destroys state: the completion port survives (closed only by the
	// reactor destructor) and the timer wheel is intact, so restarting is just clearing the latch.
	if (IoReactorEnabled() && IoReactor::IsAvailable())
		IoReactor::Instance().Stop();
	if (TimersEnabled())
		TimerQueue::Instance().Stop();

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
		poolMutex.unlock();
	}

	poolActive.store(false, std::memory_order_release);
}
void TaskScheduler::DumpPoolState(const char* why) const {
	printf("\n=== POOL STATE (%s) ===\n", why);
	// Queued work is SUMMED FROM THE QUEUES rather than read off a counter. There used to be a
	// global `pendingTasks` atomic maintained on every push and every completion, and it existed
	// only to serve this line and WaitAll(); it cost 24 ns per task in contention (27% of the whole
	// per-task cost, measured) to answer a question the queues already know the answer to. Summing
	// is O(workers) in a function that only runs when a watchdog has already given up.
	size_t queued = 0;
	// loPri/hiPri are one longer than `workers` -- the extra pair is the non-worker lane, summed
	// here too. A watchdog that omitted it would report zero queued work while main's lane held a
	// stranded lazy split, which is exactly the case this dump exists to make visible.
	for (size_t i = 0; i < loPri.size(); ++i) queued += hiPri[i]->size() + loPri[i]->size();
	printf("queuedTasks=%zu  paused=%d  poolActive=%d  workers=%zu\n",
		queued,
		(int)paused.load(std::memory_order_relaxed),
		(int)poolActive.load(std::memory_order_relaxed),
		workers.size());
	printf("  q  state           queued busy run   inbox(hi/lo)  deque(hi/lo)\n");
	for (size_t i = 0; i < workers.size(); ++i) {
		const auto s = workers[i]->GetDebugState();
		static const char* kNames[] = { "AWAKE", "GOING_TO_SLEEP", "SLEEPING" };
		const char* st = (s.workerState >= 0 && s.workerState <= 2) ? kNames[s.workerState] : "?";
		printf(" %2d  %-14s   %d      %d    %d   %d     %d/%d           %zu/%zu%s\n",
			s.qIndex, st, (int)s.hasQueuedWork, (int)s.busy, (int)s.running,
			(int)!hiPriInboxes[i]->empty(), (int)!loPriInboxes[i]->empty(),
			hiPri[i]->size(), loPri[i]->size(),
			// The signature: parked, but holding work nobody else can take.
			(s.workerState == 2 && (s.hasQueuedWork || !hiPriInboxes[i]->empty()
				|| !loPriInboxes[i]->empty())) ? "   <-- SLEEPING WITH WORK" : "");
	}
	if (nonWorkerLane < loPri.size()) {
		printf(" nw  %-14s   -      -    -   -     -/-           %zu/%zu%s\n",
			nonWorkerLaneClaimed.load(std::memory_order_relaxed) ? "CLAIMED" : "free",
			hiPri[nonWorkerLane]->size(), loPri[nonWorkerLane]->size(), "");
	}
	fflush(stdout);
}

void TaskScheduler::NotifyAll() {
	for (auto& w : workers)
		w->NotifyWorker();
}

// Pull parked ORDINARY workers up to come and steal a buried hot worker's lane backlog. Called from
// UpdateLaneHint on the 0->1 edge only; see the SetLaneWake block in TaskScheduler.h for why this
// needs a predicate input rather than just a notify.
void TaskScheduler::WakeForLane(size_t depth) noexcept {
	// EDGES, counted before any early-out, because the rate is the whole cost model.
	//
	// A wake is ~90us of core time. That number is known; what is NOT known is how often this fires,
	// and rate x known cost is the tax -- computable without an A/B against a noisy end-to-end
	// metric, and with no noise floor of its own.
	//
	// THE FAILURE MODE THIS EXISTS TO CATCH. By design this is one wake per burial: the hint sets at
	// depth >= kLaneStealDepth and stays set until the owner drains below it. But a lane oscillating
	// AROUND that threshold produces an edge per oscillation, i.e. a wake storm at the rate the pool
	// churns. A steady soak cannot show that -- there the hint sets once and stays set -- so the
	// harness that measured the win is structurally blind to this particular way of losing.
	JLIBSCHED_LANE_WAKE_STAT(edges);

	int budget = laneWakeCount.load(std::memory_order_relaxed);
	if (budget <= 0) return;

	// A woken ordinary worker can only help if it is ALLOWED to touch the lane, and that is exactly
	// what laneHintMode 4 grants. Under any other mode the wake would be pure waste: the worker
	// comes up, runs a search that structurally skips hiPri, finds nothing, and parks again having
	// spent a kernel wake to do it.
	if (GetLaneHintMode() != 4) return;

	// Scale with how buried the owner is, capped by the caller's budget. One wake is the right
	// answer for a queue of 4; it is not for a queue of 40, where the woken worker takes one task
	// and the rest still waits behind the handler.
	int want = (int)(depth / (size_t)(laneSetDepth.load(std::memory_order_relaxed) > 0
	                             ? laneSetDepth.load(std::memory_order_relaxed) : 1));
	if (want < 1)      want = 1;
	if (want > budget) want = budget;

	// ORDINARY WORKERS ONLY. Hot workers do not park (that is what makes them hot), so notifying one
	// is a guaranteed no-op that still pays NotifyWorker's seq_cst load.
	const size_t hot = GetHotWorkers();
	const size_t n   = workers.size();
	if (n <= hot) return;

	// Rotate the starting point so a run of burials spreads over the pool instead of repeatedly
	// waking the same worker -- which would keep one core hot and leave the rest cold, i.e. K-hot
	// again but chosen by accident.
	size_t i = nextLaneWake.fetch_add(1, std::memory_order_relaxed);
	for (size_t scanned = 0; scanned < n - hot && want > 0; ++scanned, ++i) {
		Thread* w = workers[hot + (i % (n - hot))].get();
		// Spend the budget on workers that are actually parked. NotifyWorker skips an awake worker
		// for free, but "wake 2" should mean two sleepers, not two attempts.
		if (!w->Parked()) continue;
		// ORDER IS THE PROOF: publish the flag, THEN notify. NotifyWorker's awake-skip loads
		// workerState seq_cst and pairs with this seq_cst store -- the same StoreLoad handshake as
		// MarkQueuedWork, and unsound in the other order. tests/verify/sleepwake_model.c.
		w->MarkLaneWake();
		w->NotifyWorker();
		JLIBSCHED_LANE_WAKE_STAT(notifies);   // wakes actually SENT, which is what costs
		--want;
	}
}
// ---- THE REMOVED GATE, and two findings from it worth keeping -------------------------------
//
// ParallelFor used to refuse to parallelize below ~75 us of estimated serial work. The threshold,
// its runtime setter, and the probe that fed it are all gone in 1.4 (see ParallelFor's header for
// the four walls static probing hits). Two measurements from that era survive the mechanism,
// because both bear on anything that might tempt someone to add a gate back:
//
// 1. AN ELEMENT COUNT IS NOT A GATE. Sweeping per-element cost over four orders of magnitude, the
//    crossover ELEMENT COUNT moved 400x -- 200,000 for a trivial body down to ~400 for an expensive
//    one -- while the crossover WORK stayed pinned at 70-92 us. That is why the `totalItems > 10000`
//    rule this predates was wrong in BOTH directions: it parallelized a trivial body at 10k where it
//    ran 11x SLOWER, and serialized an expensive one at 4k where parallel was 12x faster. Any future
//    gate expressed in ELEMENTS is this bug again.
//
// 2. DISPATCH COSTS ~10x MORE IN A DEBUG BUILD, and that is a live hazard now rather than history.
//    The 75 us figure was measured in Release; unoptimized std::function indirection,
//    _ITERATOR_DEBUG_LEVEL on the containers and un-inlined task allocation make dispatch roughly an
//    order of magnitude dearer. Leaving Release's threshold in place for Debug made Game01's physics
//    loop -- a few hundred elements -- start parallelizing and dropped the game to 2 FPS while
//    Release stayed in the hundreds. The fix then was a separate 750 us Debug threshold.
//
//    THERE IS NO THRESHOLD NOW, in any build. What replaced that protection is the slice cap: a few
//    hundred elements floors to grain 64 and caps at min(workers, slices), so such a loop dispatches
//    ~5 tasks rather than the ~124 the old per-chunk path built. That should keep it out of the 2 FPS
//    regime, but it is REASONING, not a measurement -- nobody has run Game01 in Debug against 1.4.
//    If a Debug build regresses on frame time, this is the first thing to suspect, and
//    SetParallelForSerial(true) tells you in one run whether ParallelFor is responsible.

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

// ATOMIC, and the reason is not tearing. This was a plain global read by every worker on every idle
// pass, which made SetIdlePolicy safe ONLY before StartPool -- a constraint nothing documented and
// nothing enforced. The practical failure of calling it on a running pool was not a torn read: a
// non-atomic load of a global the compiler can prove is not modified inside the worker loop is free
// to be HOISTED OUT of that loop, so a running worker could never observe the change at all.
//
// Relaxed is sufficient, deliberately. This is a HINT about how hard to look for work, never a
// correctness input: the sleep predicate and the whole wake handshake are untouched, so a worker
// reading a stale value spins one pass longer or parks one pass early, and BOTH are safe states.
// No new lost-wakeup surface -- see tests/verify/sleepwake_model.c, which this does not affect.
//
// Note the two transitions are NOT symmetric, which matters to callers (see ScopedIdlePolicy):
// NoSleep -> Sleep applies immediately, because spinning workers re-read this every pass. Sleep ->
// NoSleep applies LAZILY, because parked workers are blocked in cv.wait and cannot see it until
// something wakes them.
static std::atomic<TaskScheduler::IdlePolicy> g_idlePolicy{ TaskScheduler::IdlePolicy::Sleep };
void TaskScheduler::SetIdlePolicy(IdlePolicy p) { g_idlePolicy.store(p, std::memory_order_relaxed); }
TaskScheduler::IdlePolicy TaskScheduler::GetIdlePolicy() { return g_idlePolicy.load(std::memory_order_relaxed); }

// K-HOT. DEFAULT IS ZERO, and that is not timidity -- it is the same rule the I/O layer follows:
// a job-system-only user must not pay for something they never asked for. A spinning core is a real
// cost to every other thread in the process, so it is opt-in like the reactor is.
static std::atomic<size_t> g_hotWorkers{ 0 };
// CLAMPED SO AT LEAST ONE ORDINARY WORKER ALWAYS EXISTS. K >= pool size makes every worker hot,
// and ordinary work then has no legal destination at all: PickNextWorker skips every index, falls
// through to its fallback, and hands the task to a hot worker that will not serve it. That is a
// guaranteed hang, and it is reachable by a plausible configuration -- Init(2) with K=2, or any
// K chosen from a core count larger than the pool the app actually asked for.
//
// Clamped at BOTH ends because the setter is legal before and after Init: before, workers.size() is
// 0 and there is nothing to clamp against, so StartPool re-applies it once the pool exists.
static std::atomic<size_t> g_hotWorkersRequested{ 0 };


// NO POLICY ENUM. min == max IS static, by construction rather than by a flag that has to agree
// with the bounds. The default range is (0,0), which is exactly the pre-existing K=0 default, so
// scaling is opt-in by WIDENING the range and by nothing else -- and there is no state in which a
// Static flag and a (1,8) range disagree about what the app asked for.
static std::atomic<size_t> g_hotMin{ 0 };
static std::atomic<size_t> g_hotMax{ 0 };

// PINS THE RANGE TO [k, k]. "Set K to exactly k" is a request for a FIXED k, and leaving a wider
// range in place would mean the controller quietly moved K away from what was just asked for. So
// this is the static spelling, it is what every existing caller already means by it, and it is why
// no policy flag is needed: SetHotWorkers(k) IS SetHotWorkerRange(k, k).
void TaskScheduler::SetHotWorkers(size_t k) {
	g_hotMin.store(k, std::memory_order_relaxed);
	g_hotMax.store(k, std::memory_order_relaxed);
	SetHotWorkersEffective(k);
}

// The move itself, without touching the bounds -- used by SetHotWorkers, by the range clamp, and by
// the controller, which must not rewrite the bounds it is being steered by.
void TaskScheduler::SetHotWorkersEffective(size_t k) {
	g_hotWorkersRequested.store(k, std::memory_order_relaxed);
	size_t eff = k;
	if (instance) {
		const size_t n = instance->workers.size();
		if (n && eff >= n) eff = n - 1;
	}
	const size_t prev = g_hotWorkers.exchange(eff, std::memory_order_relaxed);
	// THE GENERATION IS BUMPED ONLY ON A REAL CHANGE, and this line is the reason the whole scheme
	// is cheap: every worker reads it every pass, so it must stay in shared state. K moves rarely
	// even under scaling (2 ms up, 200 ms down), so the line is written rarely and the reads do not
	// ping-pong -- unlike stealHintLane, which hot workers rewrite thousands of times a second and
	// which is therefore the wrong thing for an ordinary worker to poll.

	// A NEWLY PROMOTED WORKER MUST BE WOKEN, or the promotion does nothing at all.
	//
	// Hot status is read per pass from a worker's own loop -- so a worker that is PARKED cannot
	// observe it. It sits in cv.wait until something happens to land in its inbox, which for a lane
	// nobody is steering to yet may be never. The core is nominally hot, is not spinning, is not
	// serving the lane, and is not even counted as idle.
	//
	// FOUND BY INSTRUMENTATION, after two wrong diagnoses of the same symptom: the duty-cycle
	// counters for the top worker read exactly ZERO forever after its first window. Not "low", which
	// would have looked like an idle core -- zero, which means the worker was not executing its loop
	// at all. Same lazy-application shape as NoSleep, and for the same reason.
	// A BARE NOTIFY IS NOT ENOUGH, and this is the second time tonight that exact trap has appeared.
	// cv.wait(lock, pred) loops INTERNALLY: the notify wakes the condvar, the predicate still sees no
	// queued work, and it goes straight back to sleep WITHOUT RETURNING TO THE WORKER LOOP. The
	// worker therefore never re-reads its hot status, however many times it is signalled.
	//
	// So the promotion has to make the PREDICATE true, not just ring the bell -- and laneWake already
	// is a predicate input, added for lane wakes and carried through the GenMC model with its own
	// negative control. Reusing it costs no new proof: "come up and look again" is exactly what it
	// means, and a promoted worker that comes up and looks will find itself hot and spin.
	// GUARDED, because this setter is legal BEFORE Init -- which is the documented way to configure
	// K, and is what every bench and most apps actually do. There is no pool to notify then, and no
	// need for one: a worker created later reads its hot status on its first pass. Missing this
	// guard segfaulted the dispatch bench before its first line of output.
	if (!instance) return;

	// BOTH DIRECTIONS, and the demote half is not symmetry for its own sake.
	//
	// A promoted worker must wake or the promotion does nothing. A DEMOTED worker must wake for a
	// different reason: it owes one reconciliation pass -- clearing a lane hint bit that only it can
	// clear, since its threshold path stops running the moment it stops being hot. If it is parked
	// when the demotion lands it never takes that pass, and the bit stays set forever, so every
	// thief keeps probing an empty lane on it.
	//
	// FOUND BY A FLAKY TEST, 1 run in 3 -- which is exactly what it should look like, because
	// whether the bit leaks depends on whether that worker happened to be awake. A stale bit is a
	// performance leak rather than a correctness fault, so nothing else would have caught it.
	const size_t loEnd = (prev < eff) ? prev : eff;
	const size_t hiEnd = (prev < eff) ? eff  : prev;
	for (size_t i = loEnd; i < hiEnd && i < instance->workers.size(); ++i) {
		instance->workers[i]->MarkLaneWake();
		instance->workers[i]->NotifyWorker(/*force*/ true);
	}
}
size_t TaskScheduler::GetHotWorkers() { return g_hotWorkers.load(std::memory_order_relaxed); }

// ---- DYNAMIC K ---------------------------------------------------------------------------------
// See the SetHotScaling block in TaskScheduler.h for the design; this is only the mechanism.
static std::atomic<long long> g_lastHotChangeNs{ 0 };
// Duty-cycle window for the MARGINAL hot worker -- see the down branch of MaybeAdjustHotWorkers.
static std::atomic<long long> g_windowStartNs{ 0 };
static constexpr unsigned     kMinTopSamples = 16;
// Consecutive low-occupancy windows before a demote. Promote reacts in one; demote waits for three,
// because being late to shed costs a spinning core and being late to add costs latency continuously.
static constexpr unsigned     kLowWindowsToDemote = 3;
static std::atomic<unsigned>  g_lowWindows{ 0 };
static std::atomic<long long> g_lastDemoteNs{ 0 };
// Lane MISSES: completions that had to queue behind another. See NoteLaneMiss.
static std::atomic<unsigned>  g_laneMisses{ 0 };
static std::atomic<long long> g_missWindowNs{ 0 };
// Misses must cross this INSIDE kMissWindowNs, or they are forgotten. Two is the smallest number
// that distinguishes "a burst arrived" from "the lane is behind" -- see MaybeAdjustHotWorkers.
static constexpr unsigned     kMissesToPromote = 1;
static constexpr long long    kMissWindowNs   = 1000000;   // 1 ms

// Up fast, down slow, and the asymmetry is the anti-thrash. A saturated lane loses latency every
// microsecond it stays saturated; an idle hot worker loses only a spinning core, which is the cost
// K-hot already accepts by design. Cheap to be late going down, expensive to be wrong.
// ================================ THE CONTROL POLICY, LOCKED =====================================
//
// EAGER UP, PATIENT DOWN. Both halves were arrived at by measurement and both have been broken once
// by tuning the other half's number, so the reasoning is recorded rather than just the values.
//
//   promote  200 us. The binding constraint on the ramp, found by simply lowering it: at 2 ms a
//            1 -> 4 climb could not beat 6 ms, which was most of a burst. Dropping it took burn-20
//            p50 from 35.30 to 26.50 against static K=4's 26.40, with no new mechanism.
//   window   10 ms of evidence per DECISION. Not a sample rate -- the workers gather the evidence
//            themselves, which is what stopped it depending on whoever happened to be polling.
//   demote   200 ms, and three consecutive quiet windows on top.
//
// THE 20 ms DEMOTE WAS AN EXPERIMENT AGAINST A BROKEN METRIC, and it does not survive the metric
// being fixed. While occupancy was wrongly driving demotion, shortening the interval looked like it
// helped; with an honest signal it only buys chatter and a fatter p99. If someone lowers this again,
// check first whether the demote SIGNAL is right -- that was the actual fault both times.
//
// DO NOT CHASE burn-20 p99 TO EQUAL STATIC K=4. It cannot and should not:
//
//   Static K=4 never sheds, so it never pays ramp. Comparing dyn's tail to it is comparing against a
//   configuration that does not do the job dyn exists to do. Dyn's contract is "= static under load,
//   cores back on quiet", and a teens-of-percent p99 on the FIRST WAVE after a long quiet period is
//   the fee for the second half of that. The 200 ms floor is what keeps the fee rare.
//
// Tightening the tail by speeding demotion inverts the policy and gives back the p50 win. That has
// now happened once; it is written here so it does not happen twice.
//
// Two polish options exist if a real workload ever cares, and NEITHER is needed while interleaved
// runs stay within a few percent on p50/p90: refuse to demote below K=2 when a small trickle is
// always present, or hold min K for one extra window after a demote. Both cut the worst ramp without
// touching promote.
static constexpr long long kHotUpIntervalNs   =    200000;     // 200us between promotions
static constexpr long long kHotWindowNs       =  10000000;     // 10 ms of evidence per decision
static constexpr long long kHotDownIntervalNs = 200000000;     // 200 ms between demotions

void TaskScheduler::SetHotWorkerRange(size_t minK, size_t maxK) noexcept {
	if (maxK < minK) maxK = minK;
	// minK >= 1 ONLY WHEN THE RANGE CAN MOVE. At K=0 the lane does not exist -- hiPri routes to the
	// ordinary lane, no worker serves it, no hint bit is ever set -- so a controller starting there
	// has nothing to observe and could never ramp up. K=0 is absorbing under scaling, but perfectly
	// fine as a fixed point, which is what (0,0) asks for and what every existing user already has.
	//
	// ADAPTIVE-FROM-ZERO IS NOT A MISSING FEATURE. It was designed (a Mode enum, a special rule
	// letting worker 0 keep a hiPri inbox and self-promote on pickup) and then CANCELLED, because it
	// makes the case it exists to serve strictly worse than not opting into K at all:
	//
	//   the first hiPri push after a quiet period would have to WAKE worker 0 (~90 us), PROMOTE it,
	//   and only then run the task -- and worker 0 at K=0 is an ORDINARY worker, so it may already
	//   be mid-task, leaving the lane task queued behind arbitrary work of unbounded length.
	//
	// That is worse than the pre-K-hot behaviour it was meant to improve on. If you want a lane at
	// all you want it staffed, so opting in MUST mean at least one worker that is already spinning
	// and takes the completion immediately. minK = 1 is the whole point of having a lane.
	//
	// (0,0) covers "no lane, give me the whole pool as one queue" -- and the dead-lane optimisation
	// already makes that cheaper than before K-hot existed: one inbox, one deque, one steal probe
	// per victim. There is no third mode to add.
	if (maxK > minK && minK < 1) minK = 1;
	g_hotMin.store(minK, std::memory_order_relaxed);
	g_hotMax.store(maxK, std::memory_order_relaxed);

	size_t k = GetHotWorkers();
	if (k < minK) SetHotWorkersEffective(minK);        // carries the pool clamp
	else if (k > maxK) SetHotWorkersEffective(maxK);
}
void TaskScheduler::GetHotWorkerRange(size_t& minK, size_t& maxK) noexcept {
	minK = g_hotMin.load(std::memory_order_relaxed);
	maxK = g_hotMax.load(std::memory_order_relaxed);
}

// A LANE MISS: a completion that had to QUEUE behind another one on the same worker.
//
// WHY THIS AND NOT THE SATURATION EDGE. The edge trigger needs every hot worker past
// kLaneStealDepth before the first promotion fires -- so at the start of a burst the pool sits at
// K=1 accumulating four items before it even begins to climb. p50 hides that (most of the burst
// runs at full K once the climb finishes) but the TAIL is made of exactly those early completions:
// measured 22%% worse p90 and 30%% worse p99 than static K=4 while p50 matched to 7%%.
//
// A miss is detectable at depth ONE instead of four, and it is FREE: the inbox drain already
// computes how many items it moved, so `count > 1` means somebody waited. No new counter on the
// hot path, no remote reads -- the owner reports it from a line it already owns.
//
// Promote on the FIRST miss rather than on an accumulated rate. The cost model is asymmetric and
// says to: an early promote costs one core for as long as it takes the occupancy test to notice,
// while a late one costs every completion in the burst a full wait. The up interval is the only
// brake, and demotion is on its own clock now, so an overshoot is self-correcting.
void TaskScheduler::NoteLaneMiss(size_t waiting) noexcept {
	if (!instance || waiting == 0) return;
	g_laneMisses.fetch_add(1, std::memory_order_relaxed);
	MaybeAdjustHotWorkers();
}

bool TaskScheduler::HotScalingActive() noexcept {
	return g_hotMax.load(std::memory_order_relaxed) > g_hotMin.load(std::memory_order_relaxed);
}

void TaskScheduler::MaybeAdjustHotWorkers() noexcept {
	if (!instance) return;
	// SCALING IS ENABLED BY THE RANGE ITSELF -- max > min. No flag to fall out of step with it.
	const size_t lo = g_hotMin.load(std::memory_order_relaxed);
	const size_t hi = g_hotMax.load(std::memory_order_relaxed);
	if (hi <= lo) return;
	const size_t k = GetHotWorkers();
	if (k == 0) return;   // no lane exists, so there is nothing to observe -- see SetHotWorkerRange

	// ONE MEASUREMENT SERVES BOTH DIRECTIONS: the fraction of its own passes on which each hot
	// worker had lane work queued. A worker keeping up has an empty deque most of the time; a worker
	// that cannot keep up always has something in it. So the duty cycle IS the saturation signal,
	// and asking it per worker removes the thing that broke both earlier attempts.
	//
	// WHY NOT popcount(stealHintLane) == K, which is where this started. That is an INSTANT AND
	// across every hot worker, and both failure modes came from that shape:
	//   - taken instantaneously it fires on a BURST -- at K=1, one worker briefly holding four items
	//     is not a shortfall, and K oscillated (below its peak on 5 samples of 160).
	//   - required to hold CONTINUOUSLY it becomes unreachable as K grows, because K workers being
	//     simultaneously buried for milliseconds on end is rare under round-robin steering. Measured:
	//     the climb stalled at K=2 and the dispatch bench landed on the K=2 row instead of K=4.
	// A duty cycle is neither: it accumulates evidence rather than demanding coincidence.
	//
	// THE CLOCK BOUNDS HOW OFTEN WE DECIDE, not how often we sample -- the samples are gathered by
	// the workers themselves, which is the fix for a measurement whose density used to depend on
	// whoever happened to be polling.
	const long long now = MonotonicNs();

	// Masked to the live hot set: bits above k-1 survive a previous, higher K, because nothing
	// clears a hint for a worker that stopped being hot, and an unmasked read takes a stale bit as
	// live evidence.
	const unsigned long long mask = (k >= 64) ? ~0ull : ((1ull << k) - 1);
	const unsigned long long adv  = instance->stealHintLane.load(std::memory_order_acquire) & mask;

	// ================== PROMOTE IS EVENT-DRIVEN AND RUNS BEFORE THE WINDOW GATE ==================
	//
	// THE TWO DIRECTIONS DO NOT SHARE A WINDOW, and making them share one was the mistake. The
	// ratchet-to-maxK was a DEMOTE failure -- nothing ever gave a core back -- and it got "fixed" by
	// making PROMOTE wait for sustained evidence. Wrong lever, and the bench charged for it: p50
	// went from 41.50 to 173.10 against static K=4's 75.30, because the climb spent most of each
	// burst at K=1 or 2 waiting for windows that were themselves stretched.
	//
	// Once demotion is correct, promotion has no reason to be cautious. The costs are asymmetric and
	// they point opposite ways: a slightly early promote costs ONE CORE FOR A FEW MILLISECONDS, and
	// it is taken back automatically by the occupancy test below. A late promote costs every
	// completion in the burst a full park latency. So: instant evidence to TAKE a core, sustained
	// evidence to RETURN one.
	//
	// This fires from UpdateLaneHint's set edge -- the moment the last unadvertised hot worker
	// becomes buried -- which is precisely when adv == mask can first become true. The rate limit is
	// the only brake, and it is what keeps a burst from adding four cores at once while still
	// allowing 1 -> 4 in a few milliseconds.
	// A MISS PROMOTES ON ITS OWN, without waiting for adv == mask. This is the fast half of the fast
	// half: saturation says "every hot worker is four deep", a miss says "somebody just had to wait",
	// and the second is true far earlier in a burst than the first.
	// A MISS RATE, NOT A SINGLE MISS, and the difference decides whether cores ever come back.
	//
	// One burst of six completions produces exactly one miss. That is a real queue and promoting on
	// it is right for latency -- but it is NOT evidence of pressure, and treating it as such is what
	// made shedding useless: the demote path worked perfectly (measured lows climbing to 19 and a
	// clean demotion at 201 ms) and the very next burst, 5 ms later, took the core straight back.
	// The core was returned for one window out of every two hundred milliseconds.
	//
	// A trickle and a genuine burst are indistinguishable at the first miss but not at the second.
	// A 6%-duty trickle produces ~0.2 misses/ms; a loaded lane produces them continuously. So the
	// counter has to cross a threshold INSIDE a short window, and misses older than that window are
	// forgotten rather than accumulated -- otherwise any trickle eventually reaches any threshold.
	unsigned misses = g_laneMisses.load(std::memory_order_relaxed);
	const long long missWs = g_missWindowNs.load(std::memory_order_relaxed);
	if (missWs == 0 || now - missWs >= kMissWindowNs) {
		g_missWindowNs.store(now, std::memory_order_relaxed);
		g_laneMisses.store(0, std::memory_order_relaxed);
		misses = 0;
	}
	const bool missed = misses >= kMissesToPromote;
	if (missed) g_laneMisses.store(0, std::memory_order_relaxed);

	// DOES NOT RESET THE LOW-WINDOW COUNTER, and that asymmetry is the fix for a load that bursts
	// forever without ever being busy.
	//
	// A burst proves MOMENTARY demand. It does not prove the core is SUSTAINABLY needed, and those
	// are different claims that the counter was conflating: every promote zeroed the demote evidence,
	// so a trickle arriving every 5 ms could re-promote faster than three 10 ms windows could ever
	// accumulate. K ratcheted to max and stayed there under a load with a 6% duty cycle -- measured
	// at 0 of 160 samples below peak.
	//
	// Occupancy is the only thing that answers "sustainably needed", so only the OCCUPANCY promote
	// below clears this. Event-driven promotes take the core immediately, as they should, and then
	// have to survive the same time-based scrutiny as everything else to keep it.
	if (k < hi && (missed || adv == mask)
	    && now - g_lastHotChangeNs.load(std::memory_order_relaxed) >= kHotUpIntervalNs) {
		SetHotWorkersEffective(k + 1);
		g_lastHotChangeNs.store(now, std::memory_order_relaxed);
		return;
	}

	// ---- everything below is the SLOW path: occupancy over a full window, for demotion ----------
	const long long ws = g_windowStartNs.load(std::memory_order_relaxed);
	if (ws == 0) { g_windowStartNs.store(now, std::memory_order_relaxed); return; }
	if (now - ws < kHotWindowNs) return;
	g_windowStartNs.store(now, std::memory_order_relaxed);

	// Read and reset every hot worker's window. Cheap: K is small, and these are relaxed counters on
	// lines their owners write and nobody else reads between windows.
	// OCCUPANCY IS A FRACTION OF WALL TIME, and the denominator is the window itself -- no per-pass
	// counter is involved, which is what makes it invariant to how fast any worker happens to loop.
	const long long windowNs = now - ws;
	if (windowNs <= 0) return;

	unsigned minTotal = ~0u;
	long long minBusyNs = 0, topBusyNs = 0;
	unsigned topTasks = 0;
	for (size_t i = 0; i < k && i < instance->workers.size(); ++i) {
		Thread* w = instance->workers[i].get();
		const unsigned  t  = w->laneCyclesTotal.exchange(0, std::memory_order_relaxed);
		const long long ns = w->laneBusyNs.exchange(0, std::memory_order_relaxed);
		const unsigned  tr = w->laneTasksRun.exchange(0, std::memory_order_relaxed);
		if (t < minTotal) minTotal = t;
		if (i == k - 1) { topBusyNs = ns; topTasks = tr; }
		if (i == 0 || ns < minBusyNs) minBusyNs = ns;
	}

	// Not enough evidence. A window in which some hot worker barely looped says the pool was busy
	// elsewhere, not that the lane was saturated or idle, and deciding on it is reading noise.
	// NO SAMPLE-COUNT GUARD. There used to be one -- `minTotal < kMinTopSamples` -- and it was
	// vestigial from the era when occupancy was a COUNT of passes. Occupancy is nanoseconds now, so
	// a worker's busy time is valid however few passes it made.
	//
	// Worse, the guard was a MINIMUM across the hot set, which is the same starvation bug as the
	// central sampler and the pinned worker: the BUSIEST worker makes the fewest passes, one per
	// task. At six 50 us tasks per 5 ms that is ~12 passes per 10 ms window against a threshold of
	// 16, so every window was discarded and the demote path was never reached at all. K ratcheted
	// 2 -> 4 under a 6%-duty trickle and never came back.
	//
	// THIRD TIME A MINIMUM-ACROSS-WORKERS STATISTIC HAS BEEN STARVED BY THE THING IT MEASURES. If a
	// future guard here is a min over workers, check first whether being busy REDUCES the quantity.
	if (minTotal == ~0u) return;

	const long long last = g_lastHotChangeNs.load(std::memory_order_relaxed);

	// A SECOND, SLOWER WAY UP, for the load that never trips the edge above. adv == mask requires
	// every hot worker to be past kLaneStealDepth AT ONE INSTANT; a lane that is fully occupied but
	// shallow -- completions arriving exactly as fast as they are served -- is saturated without
	// ever looking backed up. Occupancy sees that, and it is the only signal that does.
	//
	// Deliberately AND-ed with adv != 0: occupancy alone reads high on a worker being handed work
	// steadily and keeping up perfectly, which needs no extra core.
	if (k < hi
	    && minBusyNs * 10 >= windowNs * 7              // every hot worker >= 70% lane-occupied
	    && adv != 0
	    && now - last >= kHotUpIntervalNs) {
		g_lowWindows.store(0, std::memory_order_relaxed);
		SetHotWorkersEffective(k + 1);
		g_lastHotChangeNs.store(now, std::memory_order_relaxed);
		return;
	}

	// DOWN: is the LAST core added earning its keep? Half, per the rule that a core idle more than
	// half the time is not paying for a core. Slower than up by an order of magnitude, because being
	// late to shed costs only a spinning core while being late to add costs latency continuously.
	// SEVERAL WINDOWS, not one. A single low window is the gap between two bursts; a core that is
	// genuinely surplus is low in all of them. Counting consecutive windows rather than widening the
	// window keeps the UP path's reaction time short -- one evidence stream serves both directions,
	// and only the down side has to be patient.
	// IDLE MEANS "RECEIVED NO LANE WORK", NOT "SPENT LITTLE TIME ON IT".
	//
	// Occupancy was the wrong question for a DEADLINE lane, and using it demoted during genuine load.
	// A hot worker resuming an I/O completion does a few microseconds of work and suspends again, so
	// a fully-loaded lane worker sits at single-digit occupancy -- it is paid to be AVAILABLE, not
	// busy. Measured: with occupancy driving demotion, dyn p50 on the 200us burn went 29.30 -> 40.80
	// against static K=4, because the controller kept shedding cores that were doing their job.
	//
	// Task COUNT answers the real question -- is this core taking a share of the arrival rate? -- and
	// it separates the two cases the occupancy metric conflated:
	//   trickle : steering aims everything at the low indices, so the marginal core runs ZERO. Shed.
	//   loaded  : steering spreads, so every hot worker runs something. Keep, at any occupancy.
	//
	// It also refuses to punish a lane for being fast, which occupancy structurally does.
	const bool low = (topTasks == 0);
	(void)topBusyNs;
	unsigned lows = 0;
	if (low) lows = g_lowWindows.fetch_add(1, std::memory_order_relaxed) + 1;
	else     g_lowWindows.store(0, std::memory_order_relaxed);

	// GATED ON ITS OWN TIMESTAMP, not on the shared one. `last` is stamped by PROMOTIONS as well, so
	// gating demotion on it means any load that keeps promoting can never shed -- which is exactly
	// the continuous-light-load case, and lowering the up interval to 200us made it strictly worse.
	// The patience already lives in kLowWindowsToDemote, and a promote RESETS that counter, so the
	// hysteresis survives without borrowing the promote clock.
	if (k > lo && lows >= kLowWindowsToDemote
	    && now - g_lastDemoteNs.load(std::memory_order_relaxed) >= kHotDownIntervalNs) {
		g_lastDemoteNs.store(now, std::memory_order_relaxed);
		g_lowWindows.store(0, std::memory_order_relaxed);
		SetHotWorkersEffective(k - 1);
		g_lastHotChangeNs.store(now, std::memory_order_relaxed);
	}
}

// Re-apply the clamp once the pool size is known -- see SetHotWorkers.
void TaskScheduler::ClampHotWorkersToPool() {
	const size_t k = g_hotWorkersRequested.load(std::memory_order_relaxed);
	const size_t n = workers.size();
	g_hotWorkers.store((n && k >= n) ? n - 1 : k, std::memory_order_relaxed);

	// THE BOUNDS NEED THE SAME CLAMP, and leaving them out is a live bug on a small pool.
	//
	// K itself was clamped here; the RANGE was not. So SetHotWorkerRange(1, 4) on Init(2) left
	// hotMax at 4 while K could never exceed n-1 == 1. The controller then sees hi > lo, decides to
	// promote, SetHotWorkersEffective silently clamps the result back to 1 -- and stamps
	// lastHotChange anyway. It promotes forever without ever moving K, and because that timestamp
	// keeps resetting, the DOWN interval never elapses either, so demotion can never fire.
	//
	// Clamped here rather than in the setter because the setter is legal BEFORE Init, when there is
	// no pool to clamp against. Same reason K is re-applied here.
	if (n) {
		const size_t cap = n - 1;
		if (g_hotMax.load(std::memory_order_relaxed) > cap)
			g_hotMax.store(cap, std::memory_order_relaxed);
		if (g_hotMin.load(std::memory_order_relaxed) > cap)
			g_hotMin.store(cap, std::memory_order_relaxed);
	}
}

// Read by the hot workers themselves and by the reactor's completion threads, each of which raises
// its OWN priority. Off by default -- see the header.
static std::atomic<TaskScheduler::HotThreadPolicy> g_hotPolicy{ TaskScheduler::HotThreadPolicy::Normal };
void TaskScheduler::SetHotThreadPolicy(HotThreadPolicy p) { g_hotPolicy.store(p, std::memory_order_relaxed); }
TaskScheduler::HotThreadPolicy TaskScheduler::GetHotThreadPolicy() { return g_hotPolicy.load(std::memory_order_relaxed); }

// Hard-pin the hot workers only. Read in StartWorker, so it must be set BEFORE Init.
static std::atomic<bool> g_hotPin{ false };
void TaskScheduler::SetHotWorkerPin(bool on) { g_hotPin.store(on, std::memory_order_relaxed); }
bool TaskScheduler::GetHotWorkerPin() { return g_hotPin.load(std::memory_order_relaxed); }

// EXCLUSIVE AFFINITY. Pinning alone measured WORSE than doing nothing, because it CONFINES the hot
// worker without EXCLUDING anyone else from its core -- so whenever another thread lands there, the
// hot worker cannot migrate away and just waits. Exclusive mode is the other half: the hot workers
// own cores 0..K-1, and every other thread in the process masks those bits off.
//
// This is the userspace approximation of isolcpus. It cannot exclude OTHER PROCESSES, which is
// exactly where it stops being equivalent.
static std::atomic<bool> g_hotExclusive{ false };
static std::atomic<unsigned long long> g_hotCpuMask{ 0 };
void TaskScheduler::SetHotWorkerExclusive(bool on) { g_hotExclusive.store(on, std::memory_order_relaxed); }
bool TaskScheduler::GetHotWorkerExclusive() { return g_hotExclusive.load(std::memory_order_relaxed); }
void TaskScheduler::SetHotCpuMask(unsigned long long m) { g_hotCpuMask.store(m, std::memory_order_relaxed); }
unsigned long long TaskScheduler::GetHotCpuMask() { return g_hotCpuMask.load(std::memory_order_relaxed); }

// Called BY a thread ON ITSELF -- ordinary workers at loop entry, the reactor's completion threads
// at Run() entry, and the application's own thread if it wants to stay off the hot cores. Each
// thread masking itself avoids plumbing native handles around, and means a thread the scheduler does
// not own can opt in with one call.
//
// Takes the PROCESS mask as the starting point rather than "all CPUs", so it composes with an
// application that has already restricted the process. No-op unless exclusive mode is on and a hot
// mask has been published, and never masks a thread down to nothing.
void TaskScheduler::ExcludeCurrentThreadFromHotCpus() {
#if JLIB_PLATFORM_WINDOWS
	if (!g_hotExclusive.load(std::memory_order_relaxed)) return;
	const unsigned long long hot = g_hotCpuMask.load(std::memory_order_relaxed);
	if (!hot) return;

	DWORD_PTR procMask = 0, sysMask = 0;
	if (!::GetProcessAffinityMask(::GetCurrentProcess(), &procMask, &sysMask)) return;
	const DWORD_PTR keep = (DWORD_PTR)(procMask & ~(DWORD_PTR)hot);
	if (keep) ::SetThreadAffinityMask(::GetCurrentThread(), keep);
#endif
}

// The stale-library guard's other half. Compiled INTO the library, so it reports the signature as
// the library's own build saw these headers. The inline check in TaskScheduler.h compares it against
// the including translation unit's view; a mismatch means somebody rebuilt one and not the other.
namespace JLib { namespace detail {
	AbiComponents JLibScheduler_STALE_LIBRARY_rebuild_the_Scheduler_for_this_configuration() { return LocalAbiComponents(); }
}}

// Forwarders. The state lives on EpochManager (it is the thing that reclaims); these exist so the
// option is discoverable beside every other tuning knob. See the header for why that mattered.
void TaskScheduler::SetSelfReclaim(bool on) { EpochManager::Instance().SetSelfReclaim(on); }
bool TaskScheduler::SelfReclaimEnabled()    { return EpochManager::Instance().SelfReclaimEnabled(); }

// The debugging kill switch that outlived the gate. Plain bool, not atomic, for the same reason
// the idle and affinity policies are: it is set once at startup (or from a debugger) and read on a
// hot path, and making it atomic would tax every ParallelFor to synchronise a value nothing races.
static bool g_parallelForSerial = false;
void TaskScheduler::SetParallelForSerial(bool on) { g_parallelForSerial = on; }
bool TaskScheduler::ParallelForSerial()           { return g_parallelForSerial; }


// THE SLICE-STEALING CORE behind ParallelFor.
//
// One task PER WORKER rather than per chunk. Each pulls [lo, lo+grain) off a shared atomic until the
// range is consumed, so the number of scheduled entities is the pool size no matter how fine the
// grain -- which is the whole point. The per-chunk alternatives (flat spawn, fork-join) pay ~80-140
// ns per chunk for a slab slot, a push and an epoch retirement, and that is why chunk size had to be
// floored at 4 per worker: finer pieces balance better but cost a task each. Here grain controls
// balancing ONLY.
//
// Measured against the fork-join path it replaces, 4M items, 31 workers, medians of 4 runs:
//     uniform body   1.7-1.9x        body whose cost varies 20x across the range   1.2-1.3x
//
// EVERYTHING LIVES ON THE CALLER'S STACK, which is sound only because both entry points BLOCK:
// cursor, wg and func all outlive every task by construction. A non-blocking version would need
// heap state and a refcount, which is why `PushArray` -- the non-blocking range API -- allocates a
// task per chunk instead of sharing this cursor.
void TaskScheduler::RunCursorRange(int start, int end, int grain, std::function<void(int, int)>& func) {
	if (end <= start) return;
	if (workers.empty()) { func(start, end); return; }   // no pool: just run it

	// EXCLUDES THE HOT WORKERS. W drives the grain floor and the split count, and a hot worker will
	// never run a slice -- it does not steal. Counting them over-splits the range into pieces that
	// have fewer runners than the arithmetic assumed. Clamped so this is at least 1.
	const int W = (int)(workers.size() - GetHotWorkers());

	// GRAIN FLOOR, and it must scale with the range -- a flat floor is a trap.
	//
	// The cursor makes the number of TASKS independent of grain, but not the number of fetch_adds:
	// that is (range / grain), all contending on one line. A flat floor of 64 looked fine and was
	// measured 15x SLOWER than ParallelFor on a 4M range, because 4M/64 is 65,536 contended atomics
	// against ParallelFor's ~124. Fine grain is cheaper here than with per-chunk tasks, but it is
	// not free, and pretending otherwise just moves the cost from the allocator to the cache line.
	//
	// So: at most 64 slices per worker. That is 16x finer than the 4-per-worker cap the per-chunk
	// paths need -- which is the real improvement, and an honest one -- while keeping the atomic
	// count in the low thousands. The absolute floor of 64 items then covers tiny ranges, where
	// range/(W*64) rounds to nothing.
	{
		const long long total = (long long)end - start;
		const long long maxSlices = (long long)W * 64;
		const int balanced = (int)((total + maxSlices - 1) / maxSlices);
		grain = std::max({ grain, balanced, 64 });
	}

	// LANES ARE CAPPED AT THE NUMBER OF SLICES THAT ACTUALLY EXIST.
	//
	// This used to spawn exactly W tasks unconditionally, which is fine when the range has slices to
	// spare and pure waste when it does not: a 20,000-element range at grain 8192 is THREE slices,
	// and it was dispatching 31 tasks so that 28 of them could fetch_add past `end` and return
	// having each paid a full dispatch. Measured, that is the whole reason this path read 0.18x on a
	// body too cheap to parallelize -- it was not the cursor being unsuited to small work, it was
	// paying for 31 lanes to do 3 slices' worth.
	//
	// A lane that cannot get a slice is not a smaller share of the work, it is a task whose entire
	// life is one atomic and a return. The cap costs a division.
	const long long sliceCount = (((long long)end - start) + grain - 1) / grain;
	const int lanes = (int)std::min<long long>(W, std::max<long long>(1, sliceCount));

	std::atomic<int> cursor{ start };
	WaitGroup wg;
	wg.n.store(lanes, std::memory_order_relaxed);

	for (int w = 0; w < lanes; ++w) {
		Task* t = CreateTask([cur = &cursor, f = &func, end, grain]() {
			for (;;) {
				const int lo = cur->fetch_add(grain, std::memory_order_relaxed);
				if (lo >= end) return;
				const int hi = (lo + grain > end) ? end : lo + grain;
				(*f)(lo, hi);
			}
			}, false, FiberSize::Standard, TaskType::Native);
		// Arena exhausted: drop this lane rather than the work. The cursor is self-balancing, so the
		// remaining workers simply take what this one would have.
		if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); continue; }
		t->waitGroup = &wg;
		Push(t);
	}

	// The caller pulls too, same as the flat path keeping chunk 0 for itself. It is blocked here
	// regardless, so leaving a whole lane idle would be waste -- and on a pool already busy with
	// other work, this is what keeps the range moving at all.
	for (;;) {
		const int lo = cursor.fetch_add(grain, std::memory_order_relaxed);
		if (lo >= end) break;
		func(lo, (lo + grain > end) ? end : lo + grain);
	}
	WaitFor(wg);
}


// ==================== THE RECURSIVE LAZY SPLITTER: what ParallelFor uses ====================
//
// ParallelFor moved to RunCursorRange for one commit and was moved back. The bench crossover
// sweep -- 32 points across four body costs, which is the instrument for this question and which
// I had not run -- shows the two CROSS OVER rather than tie:
//
//     heavy body   N=1000   2000    4000    10000   200000
//       splitter   10.3x   12.6x   13.6x   13.9x    18.5x
//       cursor      6.6x    7.8x    9.4x    9.3x    22.6x
//
// (medians of 3, non-overlapping at every N below 200k.) The splitter is 1.4-1.6x better across the
// whole mid-range -- hundreds to thousands of items with real per-item work, i.e. the frame-graph
// shape this library exists for -- and gives up ~1.2x only at very large N. The earlier "tied"
// verdict came from three samples that were ALL large-N, which is the one region where they agree.
//
// See ParallelFor's header comment for why this exists at all (short version: a predictive
// probe cannot work on a data-dependent body, and data-dependent bodies are the target). What
// follows is how it works.

// Set on a NON-WORKER thread while it holds the non-worker lane. thread_local rather than a member
// because the claim is a property of the calling thread, not of the scheduler, and a bare bool is
// enough: the atomic in `nonWorkerLaneClaimed` is what makes the claim exclusive across threads;
// this only records which side of it we are on. Workers never set it -- they have their own lane
// by qIndex and never contend for this one.
static thread_local bool t_ownsNonWorkerLane = false;

// RAII claim on the non-worker lane. Failure is NOT an error and not something to wait on: it means
// another non-worker thread is already splitting, and the caller degrades to the cursor path.
namespace {
	struct NonWorkerLaneClaim {
		std::atomic<bool>* flag = nullptr;
		explicit NonWorkerLaneClaim(std::atomic<bool>& f) {
			if (!f.exchange(true, std::memory_order_acquire)) {
				flag = &f;
				t_ownsNonWorkerLane = true;
			}
		}
		~NonWorkerLaneClaim() {
			if (flag) {
				t_ownsNonWorkerLane = false;
				flag->store(false, std::memory_order_release);
			}
		}
		bool held() const { return flag != nullptr; }
		NonWorkerLaneClaim(const NonWorkerLaneClaim&) = delete;
		NonWorkerLaneClaim& operator=(const NonWorkerLaneClaim&) = delete;
	};
}

TaskDeque* TaskScheduler::LaneForCurrentThread() {
	// A worker publishes onto its own deque -- it is the sole owner, and pushing to it from inside
	// a task it is currently executing is already what Worker() does when it re-homes a task.
	Thread* self = Thread::GetCurrent();
	if (self) {
		const int q = self->qIndex;
		if (q >= 0 && (size_t)q < loPri.size()) return loPri[q].get();
		return nullptr;
	}
	// A non-worker publishes onto the shared non-worker lane, but ONLY if it holds the claim.
	// Without that check two app threads would push to one Chase-Lev deque, which has exactly one
	// legal producer.
	if (t_ownsNonWorkerLane && nonWorkerLane < loPri.size()) return loPri[nonWorkerLane].get();
	return nullptr;
}

// Which lane index LaneForCurrentThread just returned. Mirrors it exactly; SIZE_MAX when there is
// none. Split for one reason: the PARALLELISM hint is per-lane and needs the index, and duplicating
// the resolution at the call site is how the two would drift.
size_t TaskScheduler::LaneIndexForCurrentThread() {
	Thread* self = Thread::GetCurrent();
	if (self) {
		const int q = self->qIndex;
		if (q >= 0 && (size_t)q < loPri.size()) return (size_t)q;
		return SIZE_MAX;
	}
	if (t_ownsNonWorkerLane && nonWorkerLane < loPri.size()) return nonWorkerLane;
	return SIZE_MAX;
}

// Publish-right, recurse-left, down to grain.
//
// THIS SELF-SPAWNS, AND THE OLD FORK-JOIN SPLITTER MEASURED THAT AS A DISASTER. Worth stating up
// front, because `ParallelForFJ` (removed in 1.4) carried the opposite instruction in capital
// letters -- "do NOT spawn the child on the calling worker" -- backed by a real A/B from
// 2026-08-14, a 31-worker pool, heavy body, speedup vs serial:
//
//     N                 256    512   1000   2000   10000   40000   200000
//     round-robin Push  1.09   3.57   6.33   9.50   12.04   14.20    17.80
//     self-spawn        2.33   4.14   5.65   7.00    7.40    8.00     8.70
//
// Self-spawn SATURATED at ~8x where Push climbed past 17x, and that is the signature of the tree
// never reaching most of the pool: everything lands on one deque and single-item stealing is the
// only way it can spread. That measurement is what retired PushFork.
//
// This splitter puts its work on the caller's own deque too, and does NOT saturate -- 9-13x on the
// same class of body, better than ParallelFor on all three shapes. **The difference is the wake.**
// The old splitter published to a deque and told nobody, so it depended entirely on thieves noticing
// on their own; a parked worker never notices, because the sleep predicate does not cover other
// people's deques. This one notifies a worker per split whose predecessor was not taken, and that
// notify -- plus seeding the round-robin cursor PER THREAD, which on its own took a back-loaded
// range from 7.6x to 14.6x -- is what turns self-spawn from an 8x ceiling into the fastest of the
// three. Neither is an optimisation to trim later: remove either and this becomes the 8x row above.
//
// WHAT IS DEMAND-DRIVEN HERE, PRECISELY -- because it is easy to overclaim. The SERIAL-VS-PARALLEL
// decision is: a split is published to this thread's own deque and, if nobody takes it, taken back
// and run inline for 17.8 ns (scratchpad/taskcost.cpp). Nobody free, and the whole range collapses
// to a serial run with a small constant per grain-chunk and no dispatch, no notify and no
// retirement. Somebody free, and it parallelizes. That decision is made by steals, never predicted,
// which is the entire point of replacing ParallelFor's probe.
//
// The GRANULARITY is not demand-driven, and deliberately so. It is the caller's `grain`, honoured
// literally. A budget-and-refill scheme was built first -- ceil(log2(pool)) + 2 halvings, refilled
// whenever a steal was observed -- and it measured WORSE, for a reason worth keeping: a budget that
// decrements with depth starves the deepest parts of the tree, and on a back-loaded range the
// deepest part is exactly where the work turned out to be. The diagnostic (scratchpad/lazydiag.cpp)
// showed the hot tail left in 782-element leaves costing ~1.17 ms each against a 1.78 ms total --
// one leaf WAS the critical path. That is the probe's mistake again, one level down: guessing where
// the work is. Splitting to grain unconditionally makes no such guess, and it is affordable
// precisely because the split got cheap: 17.8 ns * range/grain, which at grain 64 over 100k
// elements is ~28 us against an 11 ms body.
//
// Measured on the back-loaded body after that change: 7.6x -> 14.6x, against the cursor path's
// 10.9x and ParallelFor's 6.3x.
void TaskScheduler::RunLazyRange(int lo, int hi, LazyRangeState* st) {
	TaskDeque* myLane = LaneForCurrentThread();
	const size_t myLaneIndex = LaneIndexForCurrentThread();

	while (myLane && (hi - lo) > st->grain) {
		// The wake decision below needs to know whether our previous split is still sitting here.
		// Sampled before the push, used after it.
		const bool laneWasEmpty = (myLane->size() == 0);

		const int mid = lo + (hi - lo) / 2;

		Task* t = CreateTask([this, mid, hi, st]() { RunLazyRange(mid, hi, st); });
		if (!t) break;                      // slab exhausted: run the remainder inline, no error
		t->waitGroup = st->wg;

		// Counted BEFORE publishing, for the same reason ParallelFor's flat path counts before
		// PushBatch: the instant this is on the deque a thief can take it, finish it and decrement,
		// and a count added afterwards races a wait that already saw zero. Nothing can drive the
		// count to zero prematurely, because every task increments for its own children before its
		// own decrement happens (which is after its body returns).
		st->wg->n.fetch_add(1, std::memory_order_relaxed);

		// PARALLELISM hint, set BEFORE the push is visible, for the same reason the count is
		// incremented before it: a thief that can see the task must also see the advertisement, or
		// the first split of a range is stealable by nobody and the whole range runs serially.
		if (myLaneIndex != SIZE_MAX) SetParallelHint(myLaneIndex);

		if (!myLane->push_bottom(t)) {
			// Deque full. Unwind the count and the task, then fall through to running the whole
			// remainder here -- the same graceful degradation the exhausted-slab case takes.
			st->wg->n.fetch_sub(1, std::memory_order_relaxed);
			DestroyTask(t);
			taskAllocator.Free(t);
			break;
		}

		// WAKE SOMEBODY -- but only when the evidence says nobody is coming.
		//
		// This is what the "an unstolen split is free" argument left out, and it took a round of
		// measurement to find: a split published onto a deque is INVISIBLE to a PARKED worker. The
		// sleep predicate covers only a worker's own inbox and hasQueuedWork, deliberately --
		// stealable work on somebody else's deque is found by the steal phase that every AWAKE
		// worker runs, and a sleeping worker runs nothing. So under the default Sleep policy, main
		// published its splits to an audience of nobody and then ran every leaf itself: correct
		// results, one thread, zero speedup.
		//
		// `laneWasEmpty` is the filter. If our previous split had already been carried off, thieves
		// are awake and hungry and will be back on their own -- say nothing. If it is still sitting
		// there, either nobody is awake or everybody is busy, and one notify is what distinguishes
		// those. That keeps the steady state nearly free: NotifyWorker on an AWAKE worker is a
		// seq_cst load and a return, measured at 1.17 ns, and here it is skipped outright.
		//
		// MarkQueuedWork is required alongside it, and is a mild stretch of that flag's meaning
		// ("my own queue changed" vs "come and look around"). It is the safe direction: the worker
		// clears the flag and re-searches at the top of every pass, so a spurious set costs one
		// search and can never LOSE a wakeup, only ever add one.
		//
		// AFTER the push, never before: a worker woken while the deque is still empty can finish a
		// whole search, find nothing and park again before the push lands, spending a kernel wake
		// for nothing.
		if (!laneWasEmpty && !workers.empty()) {
			// THE CURSOR MUST START SOMEWHERE DIFFERENT ON EVERY THREAD, and this is the single
			// highest-value line in the file. It began as a plain `= 0`, so every splitting thread
			// walked workers 0,1,2,... from the same place: the low-numbered workers were woken
			// over and over and the high-numbered ones were never woken at all. Measured on the
			// back-loaded range, 13 of 32 threads ran a leaf and the other 19 stayed parked through
			// the entire run with 2048 leaves available to steal. Seeding it per thread took that
			// case from 7.6x to 14.6x on its own -- more than every other tuning here combined.
			//
			// Seeded once per THREAD from a shared counter, so the atomic is off the split path,
			// with a stride coprime to any plausible pool size so two threads that start close
			// together diverge immediately.
			static std::atomic<size_t> s_wakeSeed{ 0 };
			static thread_local size_t wakeCursor = s_wakeSeed.fetch_add(7919, std::memory_order_relaxed);
			// SKIP THE HOT WORKERS. They do not steal, so they can never take a lazy split -- a
			// wake aimed at one is not merely wasted, it dirties the state of the very threads the
			// lane exists to leave undisturbed. Hot workers are 0..K-1, and SetHotWorkers is clamped
			// so at least one ordinary worker always exists, which is what makes this modulus safe.
			const size_t hotN = GetHotWorkers();
			const size_t eligible = workers.size() - hotN;
			const size_t w = hotN + (wakeCursor++ % eligible);
			workers[w]->MarkQueuedWork();
			workers[w]->NotifyWorker();
		}

		hi = mid;
	}
	// The leaf. Everything not published above is ours.
	(*st->func)(lo, hi);
}

void TaskScheduler::ParallelFor(int begin, int end, int grain, std::function<void(int, int)> func) {
	if (end - begin <= 0) return;
	grain = std::max(1, grain);

	// No pool (or a pool of one): there is nobody to steal, so splitting could only ever cost. Run
	// it straight. Same answer RunCursorRange gives in the same situation. SetParallelForSerial
	// joins it here -- one branch, and it answers "is ParallelFor responsible for this?" without a
	// rebuild, which is the one job the removed threshold setter was actually good for.
	if (g_parallelForSerial || !poolActive.load(std::memory_order_acquire) || workers.empty()) {
		func(begin, end);
		return;
	}

	// GRAIN IS FLOORED SO THE TREE CANNOT PRODUCE MORE LEAVES THAN THE POOL CAN USE.
	//
	// This is the same guard the old chunked path applied to chunkSize (there, at most 4 chunks per
	// worker), and it is legitimate for the same reason: it is a statement about the POOL, whose
	// size is known exactly, and not about the BODY, whose cost is the thing that cannot be known.
	// Nothing is predicted, so this does not smuggle the probe back in.
	//
	// It exists because grain has a CLIFF on the low side, and a caller who guesses too fine falls
	// straight off it. Measured (scratchpad/grainsweep.cpp), 31 workers:
	//
	//     grain          1      2      8     32     64    256
    //     ragged      2.61x  3.90x 14.14x 21.50x 21.45x 20.25x
	//     back-loaded 1.95x  2.97x 10.86x 18.58x 18.61x 12.85x
	//
	// The optimum is a broad plateau and the penalty for being under it is severe -- 8x on ragged
	// work at grain 1 -- because a split costs ~10.8 ns unstolen and a full dispatch when stolen,
	// against a leaf holding one or two elements. 64 leaves per worker lands inside the plateau
	// from any input (200k/31/64 -> ~101; 100k/31/64 -> ~51) and matches the cap ParallelRange
	// already documents for its own grain.
	//
	// NOTE WHAT THIS DOES NOT FIX, and it is the cost of removing the probe: a grain too COARSE for
	// the pool is clamped here, but a body too cheap to be worth parallelizing AT ALL is not, and
	// cannot be. Nothing that refuses to measure the body can make that call. See the header.
	{
		const size_t maxLeaves = workers.size() * 64;
		const int floorGrain = (int)(((size_t)(end - begin) + maxLeaves - 1) / maxLeaves);
		grain = std::max(grain, floorGrain);
	}

	// A NON-WORKER needs the shared lane; a worker already has one. Losing the race means another
	// app thread is mid-split, so fall back to the cursor path rather than serialising behind it --
	// a perfectly good answer, just not this one.
	const bool isWorker = (Thread::GetCurrent() != nullptr);
	NonWorkerLaneClaim claim(nonWorkerLaneClaimed);
	if (!isWorker && !claim.held()) {
		RunCursorRange(begin, end, grain, func);
		return;
	}

	WaitGroup wg;
	LazyRangeState st{ &func, &wg, grain };
	RunLazyRange(begin, end, &st);

	// THE WAIT, and it is not WaitFor for a bare caller. Two different jobs are going on here.
	//
	// A FIBER caller parks and its worker goes back to its own loop, where pop_bottom picks up
	// whatever this fiber published -- the right behaviour already, and it is why the from-a-worker
	// case worked before any of this existed.
	//
	// A BARE caller (main, or a Native task) has nothing to park into, so WaitFor spin-helps via
	// TryRunStolenNativeTask -> GetTask. That is CORRECT but the wrong tool for our own splits:
	// GetTask does `rand() % 32` -- a libc call taking a lock -- and then scans up to 32 hiPri plus
	// 32 loPri deques with a steal CAS at each, to reach tasks that are sitting on THIS thread's
	// own lane where a 4.6 ns pop_bottom would have them. Measured, that scan was most of the cost
	// of a cheap ParallelFor from main.
	//
	// So: drain our own lane LIFO first, and only fall back to general helping once it is empty
	// (thieves took the rest, and the wait now genuinely has nothing local to do). LIFO is also the
	// right order -- the most recently published split is the one whose data is still warm.
	if (!OnBareThread()) {
		WaitFor(wg);
		return;
	}

	TaskDeque* myLane = LaneForCurrentThread();
	// The non-worker lane has no worker looping over it, so the per-pass maintenance in Worker()
	// never runs for it. This drain is the only consumer that reliably passes, so it clears here.
	const size_t myLaneIndex = LaneIndexForCurrentThread();
	while ((wg.n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK) > 0) {
		if (myLane) {
			if (auto opt = myLane->pop_bottom()) {
				Task* t = *opt;
				// A HELPER IS A TASK PICKUP AND OWES THE SAME CHECK. This drain races the workers
				// for the very queue they are discarding cancelled tasks from, so without this a
				// cancelled task caught here runs its payload.
				if (DiscardIfCancelled(t)) continue;
				// The same completion bookkeeping Worker()'s Native fast path does, in the same
				// order. Not factored out into a shared helper only because that path also handles
				// immediate-core release and epoch ticking, neither of which applies here.
				t->Execute();
				if (t->waitGroup) {
					int old = t->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						t->waitGroup->WakeAll();
				}
				DestroyTask(t);
				taskAllocator.Free(t);
				continue;
			}
			if (myLaneIndex != SIZE_MAX) ClearParallelHintIfEmpty(myLaneIndex, 0);
		}
		// Our lane is empty: everything we published is out with thieves. Help the pool generally
		// rather than spinning -- this is the same policy WaitFor's bare path takes.
		if (!TryRunStolenNativeTask())
			std::this_thread::yield();
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
	nextWorker.store(0, std::memory_order_relaxed);   // init, single-threaded; explicit to match PickNextWorker
	// Sized from THE POOL, not the machine. This used to be hardware_concurrency()-1, which meant a
	// caller asking for a small pool still paid for a machine-sized fiber arena: Init(4) on a
	// 32-thread box allocated 1984 standard + 248 heavy fibers, ~248 MB of commit for four workers,
	// and Init(8) on a 128-thread machine allocated ~1 GB. That is exactly backwards for the case
	// this library is built for -- embedding inside an application that owns thread placement and
	// hands the scheduler a deliberately small pool.
	//
	// Still fully automatic: for the default Init() (poolSize 0 -> hw-1) this is the same number it
	// always was. It only changes what an EXPLICIT smaller pool costs.
	//
	// 64 standard + 8 heavy per worker (the defaults; see SetFiberBudget to change them) is the
	// count of tasks that may be SUSPENDED AT ONCE per worker, since a suspended task holds its
	// fiber -- not the number of tasks in flight. See the README's rules section. Each standard
	// fiber carries a 64 KB stack and each heavy one 512 KB, so the default is ~8 MB of commit per
	// worker; the pages are reserved and only materialise on touch.
	unsigned int coreCount = num_workers;
	if (coreCount == 0) coreCount = 4; // Fallback

	size_t standardFiberCount = coreCount * StandardFibersPerWorker();
	size_t heavyFiberCount = coreCount * HeavyFibersPerWorker();

	// GlobalFiberPool now owns all fibers and stack allocation
	globalPool = GlobalFiberPool::Create(standardFiberCount, heavyFiberCount);
	workers.clear();
	loPri.clear();
	loPriInboxes.clear();
	hiPri.clear();
	stealHintLane.store(0, std::memory_order_relaxed);
	hiPriInboxes.clear();
	workers.reserve(num_workers);
	// +1 for the NON-WORKER LANE (see nonWorkerLane's declaration). Only the deques get it --
	// inboxes stay worker-indexed, because nothing ever pushes to a
	// non-worker's inbox or pins a core to it.
	loPri.reserve(num_workers + 1);
	hiPri.reserve(num_workers + 1);
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
		loPri.push_back(std::make_unique<TaskDeque>());
		hiPri.push_back(std::make_unique<TaskDeque>());
		loPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		hiPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		loPriInboxes[i]->init(&taskAllocator);
		hiPriInboxes[i]->init(&taskAllocator);
	}
	// THE NON-WORKER LANE. One extra deque pair past the workers, owned by whichever non-worker
	// thread has claimed it (see nonWorkerLane / TryClaimNonWorkerLane). Built here rather than
	// lazily so its index is fixed for the whole life of the pool: the steal loop reads it on
	// every sweep and must never see a vector being grown underneath it -- the same race the
	// two-pass worker construction below exists to avoid.
	nonWorkerLane = num_workers;
	loPri.push_back(std::make_unique<TaskDeque>());
	hiPri.push_back(std::make_unique<TaskDeque>());
	nonWorkerLaneClaimed.store(false, std::memory_order_relaxed);
	stealHintLane.store(0, std::memory_order_relaxed);

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
	// HALF of each worker's fair share of the standard fiber pool.
	//
	// Computed here because this is the only scope that knows both numbers. The old version lived
	// inside StartWorker and read `((workerThreads * 72) / workerThreads) * 0.5`, in which the
	// worker count cancels out -- it was the constant 36 on every pool size, and the `< 16` floor
	// under it could never fire. Harmless in practice, since 36 happens to sit near the 32 the
	// intended formula gives at the default pool size, but it diverged badly for an explicitly small
	// pool: Init(4) still allocates fibers from HARDWARE thread count, so four workers should cache
	// roughly 248 each and instead cached 36, sending them to the global pool constantly.
	//
	// Half rather than the whole share so an idle worker's hoard is not the reason a busy one has to
	// go to the global pool. ThreadLocalCache::Initialize clamps to its own MaxCapacity above and
	// floors at 2 below, so this only has to be sane, not exact.
	const size_t fairShare = standardFiberCount / (num_workers ? num_workers : 1);
	const size_t fiberCacheCapacity = (fairShare / 2 < 16) ? 16 : fairShare / 2;

	// K is clamped against the ACTUAL pool size here -- it may have been set before Init, when there
	// was nothing to clamp against. Must precede the hot-CPU publish below, which reads it.
	ClampHotWorkersToPool();

	// UNDO Join()'s service-layer shutdown, so Init works a second time. Join stops the reactor and
	// timer threads (it must -- they push into the pool it is about to clear), and both latch
	// `stopping`. Without this, a Join-then-Init cycle leaves EnableIoReactor/EnableTimers reporting
	// true with nothing behind them, and every submit into them fails or waits.
	//
	// Cheap because neither Stop destroys its state: the completion port survives (closed only by
	// the reactor's destructor, so registered handles stay associated) and the timer wheel is
	// untouched. Both thread sets respawn lazily -- the reactor's on the next Register/Submit, the
	// timer's on the next Arm -- so clearing the latch is the entire restart.
	if (IoReactorEnabled() && IoReactor::IsAvailable())
		IoReactor::Instance().Start();
	if (TimersEnabled())
		TimerQueue::Instance().Start();

	// PUBLISH THE HOT CPU SET BEFORE ANY WORKER STARTS. Every other thread masks these bits off as
	// it comes up, so the set has to be complete first -- a non-hot worker that started earlier
	// would otherwise exclude an incomplete set and land on a hot core anyway. Same CPU-selection
	// formula as the loop below, deliberately duplicated rather than reordered, because the loop's
	// order is load-bearing for the Ready() handshake underneath it.
	if (GetHotWorkerExclusive()) {
		const size_t hotN = GetHotWorkers();
		unsigned long long mask = 0;
		for (size_t i = 0; i < hotN && i < num_workers; ++i) {
			size_t cpu;
			if (!physicalCpus.empty())                   cpu = (size_t)physicalCpus[i + 1];
			else if (logicalCpus.size() > (size_t)i + 1) cpu = (size_t)logicalCpus[i + 1];
			else                                         cpu = i + 1;
			if (cpu < 64) mask |= (1ULL << cpu);
		}
		SetHotCpuMask(mask);
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
		workers[i]->StartWorker(cpu, fiberCacheCapacity);
	}
	for (auto& w : workers) {
		while (!w->Ready())
			std::this_thread::yield();
	}
	thread_id = thread_counter.fetch_add(1);
	poolActive.store(true, std::memory_order_release);
	poolMutex.unlock();
}

WaitResult TaskScheduler::WaitOnEventCancellable(Event& event) {
	// Already cancelled before parking: do not park at all. Otherwise this task would sit on the
	// stack until the event fires, for a cancellation it could have observed immediately.
	if (IsTaskCancelled(GetCurrentTask())) return WaitResult::Cancelled;

	WaitOnEvent(event);

	// Resumed. Whether that was an ordinary signal or a signal that arrived after CancelWaiters
	// marked us is answered by the flag, not by anything the wake path had to carry -- which is
	// what lets the Treiber stack stay exactly as it was.
	return IsTaskCancelled(GetCurrentTask()) ? WaitResult::Cancelled : WaitResult::Ok;
}

void TaskScheduler::WaitOnEvent(Event& event) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		// A Native task (see Task::type) runs with no fiber underneath it -- there's
		// nothing to switch away to. This is a contract violation, not a transient failure.
		throw std::runtime_error("WaitOnEvent called from a task with no assigned fiber -- "
			"Native tasks must never suspend.");
	}

	// Order matters. Become parkable (WANTS_SUSPEND) BEFORE registering, so any signal
	// that races in sees a resumable state (Resume() flips WANTS_SUSPEND->SUSPEND_SIGNALED
	// and the worker wakes us after the switch). AddWaiter only inserts -- it no longer
	// touches status. Registration is a single store plus a bit set in the fiber-indexed table, so a
	// signal that begins after we register is guaranteed to find and wake us.
	// RESERVE BEFORE BECOMING PARKABLE. Registering cannot be allowed to fail: once the
	// fiber is WANTS_SUSPEND it is committed to suspending, and a waiter that fails to
	// register is not merely uncancellable -- nothing holds it, so nothing ever wakes it and
	// the task hangs. The table allocation is the only failable step, so it happens here,
	// while failing is still an ordinary error.
	if (!event.Reserve()) {
		throw std::runtime_error("WaitOnEvent: could not allocate the event waiter table -- "
			"refusing to park a fiber that could never be woken.");
	}

	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	event.AddWaiter(myTask);

	// PARK-PUBLISH RE-CHECK. A cancel that landed between the caller's check and the AddWaiter above
	// scanned a table this waiter was not in yet, so nothing holds it and nothing would ever wake it.
	// Re-reading here and claiming our own slot closes that window; SelfRemove's return value decides
	// which side owns the resume -- see the long note on it in Event.h.
	if (IsTaskCancelled(myTask) && event.SelfRemove(myTask)) {
		// We are the exclusive owner of this waiter, so no wake is coming and none is needed.
		// Cancellation is delivered at the wake as always -- here the wake is simply not suspending.
		myFiber->status.store(FiberStatus::RUNNING, std::memory_order_release);
		return;
	}

	JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitOnEvent");
	// Return via the fiber's homeCtx (the worker stamps it before each switch-in),
	// not thread_local schedulerCtx -- the waiter resumes on whatever worker the
	// event signal lands on, which may differ from this one.
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}

// Name-taking convenience: one registry lookup, then the real wait. Callers that wait on the SAME
// event repeatedly should hoist GetEvent() to startup and use the Event& overload -- GetEvent takes
// a global mutex and hashes a string, and on a hot shared-wait path that serialises every waiter.
// The reference stays valid for the process lifetime: the registry owns unique_ptr<Event>, so a
// rehash moves the pointer and not the object, and entries are never erased.
void TaskScheduler::WaitOnEvent(const std::string& eventName) { WaitOnEvent(GetEvent(eventName)); }

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
				ranSomething = TryRunStolenNativeTask();
				--t_spinHelpDepth;
			}
			if (!ranSomething)
				std::this_thread::yield();
		}
	}
}
// WaitFor that a cancel can end early. Returns Cancelled when `tok` fires while still waiting.
//
// WHAT IT DOES NOT DO: touch n. Cancelling a wait is "I stopped waiting", not "the group finished".
// Every task still outstanding stays outstanding and still decrements when it completes, so a second
// waiter -- or a later WaitFor on the same group -- still sees the truth. This is why Cancel() must
// never smash the count to zero: that would strand every task still in flight with nothing left to
// release, and lie to everyone else looking at the same group.
//
// COMPLETION WINS OVER CANCELLATION when both are true. A group that genuinely finished reports Ok,
// because it did; reporting Cancelled there would tell the caller to discard results that exist.
WaitResult TaskScheduler::WaitFor(WaitGroup& wg, CancelToken tok) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		WaitOnEventDirectArmed([&wg, tok](DirectEvent* ev) {
			std::lock_guard<std::mutex> lock(wg.mtx);
			wg.cancellable.push_back(WaitGroup::CancelWaiter{ ev, tok.Raw() });
			const int old = wg.n.fetch_or(WaitGroup::WAITER_BIT, std::memory_order_acq_rel);

			// Two reasons to wake ourselves instead of parking, checked under the SAME lock that
			// published us -- which is what makes the second one sound. A cancel landing between the
			// caller's check and this push would otherwise walk a list we are not on yet, and we
			// would park with nobody holding us: the park-publish race, exactly as on the semaphore
			// and Event paths.
			if ((old & WaitGroup::COUNT_MASK) == 0 || tok.Cancelled()) {
				wg.cancellable.pop_back();   // we are last: we pushed under this same lock
				ev->Signal();
			}
			});

		if ((wg.n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK) == 0)
			return WaitResult::Ok;
		return tok.Cancelled() ? WaitResult::Cancelled : WaitResult::Ok;
	}

	// BARE THREAD: nothing to park, so cancellation is observed between helping passes rather than
	// delivered. Same helping policy and the same two reentrancy guards as the uncancellable path.
	while ((wg.n.load(std::memory_order_acquire) & WaitGroup::COUNT_MASK) > 0) {
		if (tok.Cancelled()) return WaitResult::Cancelled;
		bool ranSomething = false;
		if (t_heldMutexes == 0) {
			++t_spinHelpDepth;
			ranSomething = TryRunStolenNativeTask();
			--t_spinHelpDepth;
		}
		if (!ranSomething)
			std::this_thread::yield();
	}
	return WaitResult::Ok;
}

void JLib::TaskScheduler::PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity, size_t minPerSegment,
	bool hiPri, CorePref pref)
{
	if (!tasks || count == 0) return;

	// NOTE (corePref): `pref` places the WHOLE batch as one class -- still assumed homogeneous, but
	// now the caller states which class explicitly (CorePref::Default unless told otherwise) instead
	// of it being silently hardcoded to Default regardless of what was asked for. A receiving OWNER
	// still runs its tasks unvetted once claimed (see the enforcement-scope note in Task.h), but
	// class-aware STEALING and PLACEMENT now both agree, where before only stealing did.
	//
	// A links-and-pushes helper, because the segment loop below and the explicit-affinity path want
	// the same three steps. push_batch null-terminates the tail itself, so segments never bleed into
	// one another even though `tasks` is one contiguous array.
	auto submitRun = [&](size_t first, size_t len, int chosen) {
		for (size_t i = first; i + 1 < first + len; ++i)
			tasks[i]->next.store(tasks[i + 1], std::memory_order_relaxed);
		// Priority is a PARAMETER, not read from the tasks: a batch is documented as homogeneous, and
		// silently routing a hiPri run into the loPri inbox is a priority inversion no caller could
		// see. Before this existed, every batch went to loPri unconditionally.
		// COLLAPSE WHEN THE LANE IS INACTIVE: at K=0 nobody probes hiPri, so a batch routed there
		// would never run. Same rule as PushLocal and Requeue, asked of the same predicate.
		const bool useHi = hiPri && HiPriLaneActive();
		(useHi ? hiPriInboxes : loPriInboxes)[chosen]->push_batch(tasks[first], tasks[first + len - 1]);
		// Without this the batch sits undiscovered if `chosen` is genuinely asleep: a worker's cv
		// is private and nothing wakes it without a notify targeting it specifically.
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();
	};

	if (cpuaffinity != 0) {
		// Explicit affinity is an explicit request: honour it and do not spread. Falls back to
		// PickNextWorker(pref), not the unpinned target, if that exact core is stuck -- pre-existing
		// behaviour, now at least routed toward the right class instead of always Default.
		const int chosen = cpuaffinity - 1;
		submitRun(0, count, chosen);
		return;
	}

	// SPREAD ACROSS WORKERS rather than handing the whole batch to one.
	//
	// This function used to pick ONE worker and push all `count` tasks into its inbox. Everything
	// then funnelled through that worker: it alone drains its inbox (inboxes are owner-drain-only),
	// moving tasks BATCH_SIZE at a time into its local deque, from which the other N-1 workers steal
	// ONE ITEM AT A TIME. So a 20k-task batch became one drain loop plus ~20k single-item steal CASes
	// contending on a single deque, while the producer that just paid to build the batch sat idle.
	// Measured at 31 workers: it is most of the ~90 ns/task dispatch cost, and it is why the cost
	// barely improved with batch size.
	//
	// Segmenting is nearly free -- the links were being written anyway, just in one long chain
	// instead of W shorter ones -- and it turns one hot deque into W warm ones, giving every worker
	// local work to pop instead of remote work to steal.
	// Segment count is NOT simply min(count, workers): each segment costs a MarkQueuedWork +
	// NotifyWorker, and a notify takes the target worker's mutex (the lost-wakeup fix). Splitting a
	// 64-task batch 31 ways buys almost no parallelism and pays 31 notifies for it -- measured as a
	// real regression on the small rows before this floor existed. Keep segments big enough that the
	// spread is worth the wake-up it costs, and let small batches behave as they always did.
	if (minPerSegment == 0) minPerSegment = 1;
	const size_t nw = workers.size();

	// CAP AT THE WORKERS THIS BATCH CAN ACTUALLY REACH, not at the pool. A hiPri batch goes to the
	// hot set; everything else goes to the ordinary set. Capping at the whole pool cuts the batch
	// into more pieces than there are destinations for them, so segments pile up on the same
	// workers -- paying the per-segment notify without buying the spread it is meant to buy.
	// SetHotWorkers is clamped so both sets are non-empty.
	const size_t hotN = GetHotWorkers();
	const size_t reachable = (hiPri && hotN) ? hotN
	                       : (nw > hotN ? nw - hotN : nw);

	size_t segments = (nw == 0) ? 1 : (count / minPerSegment);
	if (segments < 1)         segments = 1;
	if (segments > reachable) segments = reachable;
	const size_t per = count / segments;
	const size_t rem = count % segments;

	size_t first = 0;
	for (size_t s = 0; s < segments; ++s) {
		const size_t len = per + (s < rem ? 1 : 0);
		if (len == 0) continue;
		// Same lane branch as the single-task path -- a hiPri BATCH rotates the hot set. The retry
		// applies to ordinary placement only; PickNextWorker settles a fully-claimed hot set itself,
		// and yielding in a loop here on K = 1 would never terminate.
		const bool useHiSeg = hiPri && HiPriLaneActive();
		const int chosen = PickNextWorker(pref, useHiSeg);
		submitRun(first, len, chosen);
		first += len;
	}
}

bool TaskScheduler::Push(uint8_t cpu_affinity, Task* task) {
	return PushLocal(task, cpu_affinity);
}

void TaskScheduler::WaitOnEventArmed(Event& event, const std::function<void()>& arm) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventArmed called from a task with no assigned fiber -- "
			"Native tasks must never suspend.");
	}

	// RESERVE FIRST, for the reason spelled out in WaitOnEvent: registering must not be able
	// to fail once this fiber is parkable, or the waiter is dropped and the task hangs.
	if (!event.Reserve()) {
		throw std::runtime_error("WaitOnEventArmed: could not allocate the event waiter table -- "
			"refusing to park a fiber that could never be woken.");
	}

	// Same ordering as WaitOnEvent: become parkable, then register as a waiter, so a signal
	// that races in is not lost (Resume flips WANTS_SUSPEND->SUSPEND_SIGNALED and the worker
	// wakes us after the switch). Crucially, run 'arm' only AFTER both -- the arm callback
	// hooks the external wakeup (e.g. a GPU fence), and must not be able to fire SignalAll
	// before this fiber is a discoverable, resumable waiter.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	event.AddWaiter(myTask);

	// Same park-publish re-check as WaitOnEvent, and it goes BEFORE arm() on purpose: arm hooks an
	// external wakeup, and there is no reason to hook one for a fiber that is not going to park. A
	// cancel arriving after arm() is the ordinary case -- the external signal wakes us normally.
	if (IsTaskCancelled(myTask) && event.SelfRemove(myTask)) {
		myFiber->status.store(FiberStatus::RUNNING, std::memory_order_release);
		return;
	}

	if (arm) arm();

	JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitOnEventArmed");
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}

// See WaitOnEvent above: the name overload costs one registry lookup per call.
void TaskScheduler::WaitOnEventArmed(const std::string& eventName, const std::function<void()>& arm) {
	WaitOnEventArmed(GetEvent(eventName), arm);
}

void TaskScheduler::WaitOnEventDirectArmed(const std::function<void(DirectEvent*)>& arm) {
	auto* thread = Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;
	if (!myFiber) {
		throw std::runtime_error("WaitOnEventDirectArmed called from a task with no assigned "
			"fiber -- Native tasks must never suspend.");
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

	JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitOnEventDirectArmed");
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);

	// Resumed: WE own release. Signal() already exchanged waiter->null and will not touch e again.
	eventPool.Release(e);
}

bool TaskScheduler::IsOnFiber() {
	auto* t = Thread::GetCurrent();
	// currentRunningTask alone isn't enough -- a Native task sets it too (see Worker()'s fast
	// path) but deliberately never gets a fiber. Callers use this to decide whether
	// WaitOnEvent*-style suspension is safe, so it must be false for a Native task.
	return t != nullptr && t->currentRunningTask != nullptr && t->currentFiber != nullptr;
}

// Builds the waiter table on first use. Lives here rather than in Event.h because it needs the
// global fiber pool, and Event.h deliberately depends only on Fiber.h -- see the include note at
// the top of that header.
//
// SIZED BY THE POOL, not by a guess: one slot per fiber that exists, which is the exact upper
// bound on tasks that can be parked at once, since a parked task holds a fiber. There is no load
// factor to tune and no way to overflow it.
//
// The table is fully built -- both arrays allocated and zeroed -- BEFORE the single pointer that
// publishes it, so a reader that acquires a non-null table always sees a complete one. The loser
// of a creation race deletes its own and takes the winner's.

#if defined(JLIBSCHED_TASK_STATS)
namespace {
	// FIRES AT PROCESS EXIT, not at TaskScheduler::Join(). Join was the obvious hook and it was the
	// wrong one: Game01 -- the application this flag exists to measure -- never calls it. The
	// scheduler is a singleton that simply outlives wWinMain, so hooking Join produced a run that
	// looked successful and wrote nothing.
	//
	// Safe as a static destructor because everything it reads is trivially destructible (atomic
	// counters in a plain array), so there is no cross-TU destruction-order hazard: those objects
	// have no destructor to run before this one.
	struct TaskSizeAtExit {
		~TaskSizeAtExit() {
			if (std::FILE* f = std::fopen("jlib-task-sizes.txt", "w")) {
				JLib::detail::ReportTaskSizesTo(f);
				std::fclose(f);
			}
			JLib::detail::ReportTaskSizes();   // and stdout, for console apps
		}
	};
	TaskSizeAtExit g_taskSizeAtExit;
}
#endif

Event::WaiterTable* Event::EnsureTable() {
	if (WaiterTable* tb = table.load(std::memory_order_acquire)) return tb;

	const size_t n = TaskScheduler::Instance().GetGlobalPool().TotalCount();
	if (n == 0) return nullptr;

	auto* fresh = new (std::nothrow) WaiterTable();
	if (!fresh) return nullptr;                      // uncancellable waiters, not a crash
	fresh->fiberCount = n;
	fresh->words = (n + 63) / 64;
	fresh->slots = new (std::nothrow) std::atomic<Task*>[n];
	fresh->occupied = new (std::nothrow) std::atomic<std::uint64_t>[fresh->words];
	if (!fresh->slots || !fresh->occupied) { delete fresh; return nullptr; }

	for (size_t i = 0; i < n; ++i)            fresh->slots[i].store(nullptr, std::memory_order_relaxed);
	for (size_t w = 0; w < fresh->words; ++w) fresh->occupied[w].store(0, std::memory_order_relaxed);

	WaiterTable* expected = nullptr;
	if (table.compare_exchange_strong(expected, fresh,
			std::memory_order_release, std::memory_order_acquire)) {
		return fresh;
	}
	delete fresh;          // lost the race; expected holds the winner
	return expected;
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


// Steals ONE task for a NON-worker helper (e.g. main spinning in WaitFor, or a Native task spinning
// on a SchedulerMutex). Random-start, hiPri-then-loPri scan with fairness: after
// kStealFairnessWindow consecutive hiPri steals it forces a loPri scan so loPri work can't starve
// behind a stream of hiPri steals. Single-item steal() is the only correct steal in the lock-free
// deque (see TaskDeque.h). Returns nullptr if nothing was stealable anywhere.
Task* TaskScheduler::GetTask() {
	bool forceLoPri = (consecutiveHiPriSteals >= kStealFairnessWindow);

	// NATIVE- AND CLASS-VETTED at the deque (steal_if): GetTask's ONLY caller is TryRunStolenNativeTask,
	// whose fiberless caller can't run anything that might suspend. Previously this stole blind and
	// Requeued fiber-backed tasks -- a claim-CAS + full re-push + notify to move a task nowhere (deque
	// contention + thrash). Now a fiber-backed task is never claimed at all: it stays put for a real worker,
	// and the scan just moves to the next victim. Class matters too because TryRunStolenNativeTask's callers vary: the
	// SchedulerMutex spin path invokes it FROM WORKERS (thief class = that worker's), while main/WaitFor
	// helpers are non-workers pinned to CPU 0 -- a P-core -- so they vet as P. corePref is the sole
	// placement authority EVERYWHERE, including helper steals.
	// Thief class, NOT assumed: a worker (SchedulerMutex/CV spin inside a Native task lands here from
	// workers too) uses its pinned class; any NON-worker thread (main, or an arbitrary app thread
	// hitting a scheduler primitive -- possibly unpinned and floating) asks the OS where it is RIGHT
	// NOW via GetCurrentProcessorNumber + the per-CPU class table. "Would this Native task run on a P or
	// E core?" is answered by where the caller is actually standing.
	Thread* thief = Thread::GetCurrent();
	// Bounds-check against the ACTUAL table size, not `& 63`. That mask was the old 64-CPU
	// assumption and it survived the multi-group work: isPCpu is sized to CpuMask::kMaxCpus now, so
	// on a machine wider than 64 CPUs the mask silently folded a caller's CPU onto another core's
	// entry (CPU 100 read slot 36) and answered the P/E question about the wrong core. Never a
	// crash, only a worse steal decision, which is exactly why nothing surfaced it.
	// Out of range degrades to "P", matching isPCpu's all-P default for a non-hybrid or query-failed
	// machine -- the same safe answer the table already gives when it knows nothing.
	const unsigned thiefCpu = JLib::platform::CurrentCpu();
	const bool thiefIsP = thief
		? (isPCore[thief->qIndex] != 0)
		: (thiefCpu < isPCpu.size() ? (isPCpu[thiefCpu] != 0) : true);
	const bool degen = pWorkers.empty() || eWorkers.empty();
	// Vets the deque's TAG, not the task -- see TaskDeque::StealBits. Both fields this needs
	// (type, corePref) ride in the stored pointer's spare low bits precisely so this predicate
	// never has to dereference a candidate the thief has not claimed.
	// NOT "== Native" -- "does not need a fiber". A coroutine is resumed by calling a function on
	// whatever stack is current, exactly like a Native task, so a fiberless caller can run one
	// perfectly well; only a Fiber-backed task genuinely cannot be claimed here. Excluding
	// coroutines would leave a blocked main thread spinning next to coroutine work it is able to do.
	//
	// Whoever widens this must also keep TryRunStolenNativeTask's completion path in step: a
	// coroutine task is owned and freed by the C++20 side, so running one here and then applying the
	// usual DestroyTask/Free would be a double free. The two changes are one change.
	auto fiberlessRunnable = [&](StealBits sb) {
		return sb.type != TaskType::Fiber && StealClassCompatible(sb.corePref, thiefIsP, degen);
	};

	// A BARE THREAD IS NOT A HOT WORKER, so it does not reach into the lane. This is the same
	// reservation the worker steal path enforces, and it was a hole: a caller sitting in WaitFor
	// helps through here, and without this it would pull a completion out of a hot worker's deque
	// onto a thread that is not spinning, is not elevated, and may be about to block again. Every
	// reason ordinary workers are kept out applies harder to main.
	//
	// K = 0 leaves this exactly as it was -- no lane exists, and skipping hiPri there would strand
	// whatever is in it.
	const size_t hotN_ = GetHotWorkers();
	if (!forceLoPri && hotN_ == 0) {
		size_t numThreads = hiPri.size();
		size_t start = rand() % numThreads;
		for (size_t i = 0; i < numThreads; ++i) {
			size_t target = (start + i) % numThreads;
			if (auto s = hiPri[target]->steal_if(fiberlessRunnable)) {
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
		if (auto s = loPri[target]->steal_if(fiberlessRunnable))
			return *s;
	}

	return nullptr;
}

bool TaskScheduler::TryRunStolenNativeTask() {
	// Steals ONE task this fiberless caller can actually run and runs it right here. GetTask vets the
	// type AT THE DEQUE (steal_if) -- a fiber-backed task is never claimed in the first place, so the
	// old steal-then-Requeue relocation path (claim CAS + re-push + notify = contention/thrash) no
	// longer exists.
	//
	// "Native" IN THE NAME IS NOW NARROWER THAN THE BEHAVIOUR: since 2.8.0 this also steals
	// TaskType::Coroutine, because resuming a coroutine is a function call on the current stack and
	// needs no fiber. Only Fiber-backed tasks are off limits. The name is kept rather than churned
	// because it is public API; read it as "a task that does not require a fiber".
	// A HOT WORKER LANDS HERE TOO, and that is a hole worth naming. OnBareThread() is true for a
	// worker running a NATIVE task (no fiber), so a hot worker whose lane task calls WaitFor,
	// SchedulerMutex or a condition variable helps through this function -- and GetTask() steals
	// BULK work, which is exactly what "hot workers never steal" exists to prevent.
	//
	// It is NOT fixed by refusing. The fallback below drains this worker's own inboxes, and that
	// drain is load-bearing: a Native task blocking on a worker makes its own inbox unreachable by
	// the whole pool, which is a documented deterministic deadlock. Refusing to help would trade a
	// policy violation for a hang.
	//
	// So: drain the LANE FIRST. A hot worker checks its own inbox before touching anyone else's
	// deque, which means lane work always wins over stolen bulk, and bulk is taken only when the
	// lane is genuinely empty. The residual violation -- a hot worker running one bulk task while a
	// lane task of its own is blocked -- is reachable only by breaking the lane contract (lane work
	// must be short and non-blocking), and a bounded violation beats a deadlock.
	Thread* selfHot = Thread::GetCurrent();
	if (selfHot && GetHotWorkers() > (size_t)selfHot->qIndex)
		selfHot->DrainOwnInboxesToDeques();

	Task* task = GetTask();
	if (!task) {
		// Nothing stealable anywhere -- but "anywhere" only covers DEQUES, and if this caller is a
		// WORKER it is here because it is blocked inside a task (a Native task spinning in
		// WaitFor / SchedulerMutex / SchedulerConditionVariable). It will not return to Worker()'s
		// loop while it spins, and inboxes are owner-drain-only, so anything in ITS OWN inbox is
		// unreachable by the entire pool -- including, possibly, the task it is waiting for. That
		// is a real deadlock, not a slow path: a Native task calling ParallelFor from a worker
		// hangs deterministically, because PushLocal round-robins chunks over every worker
		// INCLUDING the calling one, and the chunk that lands at home is then stranded.
		//
		// NON-WORKERS SKIP THIS ENTIRELY, which is the common case: main and any app thread hitting
		// a scheduler primitive have no inbox and no qIndex to index one with. Thread::GetCurrent()
		// is null for them, and they are also not part of the hazard -- main blocking in WaitFor
		// blocks nobody's queue but its own, and it has none.
		Thread* self = Thread::GetCurrent();
		if (!self) return false;
		if (!self->DrainOwnInboxesToDeques()) return false;

		// A drained task may be FIBER-BACKED, which this fiberless caller can never run itself --
		// it now needs some other worker to steal it, and they may all be parked (in the repro,
		// every one of the other 30 was SLEEPING). Only reached when a drain actually moved
		// something, so this is not on any hot path.
		NotifyAll();

		task = GetTask();
		if (!task) return false;
	}

	// Read BEFORE Execute(), for the same reason Worker() does: a coroutine can run to completion
	// inside resume(), and completing frees both its frame and this Task -- so `task` may be dangling
	// the moment Execute() returns, and reading ->type then to decide what to do about it is itself
	// a use-after-free.
	// Same pickup check the worker makes -- this is a task about to START, just on a helping thread
	// rather than a worker. Returning true because a task WAS consumed: the caller's contract is
	// "did I make progress", and removing a cancelled task from the queue is progress.
	if (DiscardIfCancelled(task)) return true;

	const bool isCoroutine = (task->type == TaskType::Coroutine);

	task->Execute();

	// Same ownership rule as the worker's fast path, and it has to be the same or this is a double
	// free: a coroutine signals its own WaitGroup and returns its own Task to the slab from inside
	// the coroutine. Everything below belongs to tasks this caller actually owns.
	if (!isCoroutine) {
		if (task->waitGroup) {
			int old = task->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
			if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
				task->waitGroup->WakeAll();   // only touches wg if someone registered
		}
		DestroyTask(task);
		taskAllocator.Free(task);
	}

	if (EpochManager::Instance().ShouldSelfReclaim()) {
		EpochManager::Instance().Tick();
	}
	return true;
}

TaskAllocator* TaskScheduler::GetAllocator() {
	return &taskAllocator;
}

size_t TaskScheduler::GetWorkerCount() const {
	return workers.size();
}
// Forwarder; the slab lives on the allocator. See the header for the trade this exists to offer.
void TaskScheduler::PrefaultTaskSlots(size_t slots) {
	taskAllocator.Prefault(slots);
}

// Plain bool, init-only, for the same reason as EpochManager's selfReclaim: it is read exactly once,
// while constructing the allocator, before any worker exists. Nothing races it, and making it atomic
// would imply a runtime flip that is meaningless -- the slab is built by then.
static bool g_lazyTaskSlab = false;
void TaskScheduler::SetLazyTaskSlab(bool on) { g_lazyTaskSlab = on; }
bool TaskScheduler::LazyTaskSlabEnabled()    { return g_lazyTaskSlab; }

// SetTaskSlabSize/TaskSlabSize were REMOVED in 3.0.0 rather than kept as shims. A setter named for
// "the task slab" that configures ONE of three pools is worse than no setter at all -- it looks
// like it worked. A removed symbol is a compile error at every call site, which is exactly the
// review this change wants; a shim would have silently left two pools at defaults.
static JLib::TaskScheduler::SlabSizes g_slabSizes{};
void TaskScheduler::SetSlabGrowth(bool on) noexcept {
	detail::SlabGrowthEnabled().store(on, std::memory_order_relaxed);
}
bool TaskScheduler::SlabGrowthEnabled() noexcept {
	return detail::SlabGrowthEnabled().load(std::memory_order_relaxed);
}

void TaskScheduler::SetSlabSizes(const SlabSizes& s) { g_slabSizes = s; }
JLib::TaskScheduler::SlabSizes TaskScheduler::CurrentSlabSizes() { return g_slabSizes; }

// Defaults match what StartPool always computed inline (coreCount * 64, coreCount * 8) before this
// existed. Not validated against zero or anything else -- same trust-the-caller stance every other
// setter here takes; a caller who sets 0 has asked for a pool that always reports exhausted, and
// that is theirs to have asked for.
static size_t g_standardFibersPerWorker = 64;
static size_t g_heavyFibersPerWorker    = 8;
void TaskScheduler::SetFiberBudget(size_t standardPerWorker, size_t heavyPerWorker) {
	g_standardFibersPerWorker = standardPerWorker;
	g_heavyFibersPerWorker    = heavyPerWorker;
}
size_t TaskScheduler::StandardFibersPerWorker() { return g_standardFibersPerWorker; }
size_t TaskScheduler::HeavyFibersPerWorker()    { return g_heavyFibersPerWorker; }
#if defined(JLIBSCHED_TASK_STATS)
// Prints the task-size histogram AND the class-boundary arithmetic.
//
// The arithmetic is here rather than left to the reader because doing it by hand is where the
// frame-class decision nearly went wrong: 128+256 LOOKS like the obvious "cover everything" split
// and is actually WORSE than 64+256, which is not visible until the numbers are lined up. Anything
// that has to be recomputed by hand every time will eventually be recomputed wrong.
//
// CONSERVATIVE BY CONSTRUCTION: each bucket is charged at its UPPER bound, so a task somewhere
// inside a bucket is treated as the largest it could be. That understates the saving rather than
// overstating it -- the reported win is a floor, not a hope.
void JLib::detail::ReportTaskSizes() { ReportTaskSizesTo(stdout); }

// Writes to an explicit stream so the auto-report at shutdown can go to a FILE. A GUI application
// (Game01 is wWinMain) has nowhere for stdout to land, and "run it and read the console" is not
// available -- which would make this flag useless on exactly the code it exists to measure.
void JLib::detail::ReportTaskSizesTo(std::FILE* out) {
	uint64_t bucket[kTaskSizeBuckets] = {};
	uint64_t total = 0, maxSize = 0, totalBytes = 0;
	for (size_t s = 0; s < kTaskSizeShards; ++s) {
		for (size_t b = 0; b < kTaskSizeBuckets; ++b) {
			const uint64_t v = g_taskSizeShards[s].bucket[b].load(std::memory_order_relaxed);
			bucket[b] += v;
			total += v;
		}
		totalBytes += g_taskSizeShards[s].totalBytes.load(std::memory_order_relaxed);
		const uint64_t m = g_taskSizeShards[s].maxSize.load(std::memory_order_relaxed);
		if (m > maxSize) maxSize = m;
	}
	if (!total) { std::fprintf(out, "  task sizes: nothing recorded\n"); return; }

	std::fprintf(out, "  tasks created: %llu, largest %llu bytes, mean %.1f bytes\n",
		(unsigned long long)total, (unsigned long long)maxSize, double(totalBytes) / double(total));
	static const char* kLabel[kTaskSizeBuckets] = {
		"  <=64", "  <=80", "  <=96", " <=128", " <=160", " <=192", " <=256", "  >256" };
	for (size_t b = 0; b < kTaskSizeBuckets; ++b) {
		if (!bucket[b]) continue;
		std::fprintf(out, "    %s  %10llu  (%5.1f%%)\n", kLabel[b],
			(unsigned long long)bucket[b], 100.0 * double(bucket[b]) / double(total));
	}

	// Candidate class sets. A task is charged the smallest class that fits its bucket's upper bound;
	// anything that fits no class is charged 256 (today's behaviour).
	struct Candidate { const char* name; size_t cls[4]; size_t n; };
	static const Candidate kCandidates[] = {
		{ "256 only (today)",   { 256, 0, 0, 0 },     1 },
		{ "64 + 256",           { 64, 256, 0, 0 },    2 },
		{ "128 + 256",          { 128, 256, 0, 0 },   2 },
		{ "64 + 128 + 256",     { 64, 128, 256, 0 },  3 },
		{ "64 + 96 + 128 + 256",{ 64, 96, 128, 256 }, 4 },
		{ "64 + 80 + 128 + 256",{ 64, 80, 128, 256 }, 4 },   // what 3.0.2 ships
	};
	std::fprintf(out, "  bytes per task, by class set (charged at each bucket's upper bound):\n");
	for (const Candidate& c : kCandidates) {
		double bytes = 0;
		for (size_t b = 0; b < kTaskSizeBuckets; ++b) {
			if (!bucket[b]) continue;
			const size_t sz = kTaskSizeEdge[b] ? kTaskSizeEdge[b] : 256;
			size_t charged = 256;
			for (size_t i = 0; i < c.n; ++i)
				if (sz <= c.cls[i]) { charged = c.cls[i]; break; }
			bytes += double(charged) * double(bucket[b]);
		}
		const double per = bytes / double(total);
		std::fprintf(out, "    %-22s %6.1f B/task   (%.2fx of today)\n", c.name, per, per / 256.0);
	}
	std::fprintf(out, "  NOTE: bench tasks are capture-free, so this is only meaningful run on a REAL app.\n");
}
#endif

Task* TaskScheduler::CreateTaskImpl(void(*fn)(void*), void* data, uint8_t hipri, FiberSize size, TaskType type, CorePref corePref) {
	void* mem = taskAllocator.AllocSized(sizeof(Task));   // a bare Task is exactly 64 B
	if (!mem) return nullptr;
	detail::RecordTaskSize(sizeof(Task));   // exactly 64 -- a quarter of the slot it just took
	Task* t = ::new (mem) Task(fn, data, hipri, size);
	t->type = type;
	t->corePref = corePref;
	// Concrete type is exactly Task, whose destructor is empty -- the completion path can skip the
	// virtual call entirely. See Task::trivialDtor.
	t->trivialDtor = 1;
	return t;
}

bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task) return false;

	size_t num_workers = workers.size();
	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);
		// EXPLICIT AFFINITY NOW ALWAYS SUCCEEDS. It used to be refusable -- if that core was pinned
		// by PushImmediate this returned false and the caller had to cope. With pinning gone
		// (4.0.1) there is no way for a worker to be unavailable, so the refusal path and the
		// retry loops that danced around it are unreachable and removed. See SetReservedCores.
		loPriInboxes[idx]->push(task);
		// Targeted at worker idx specifically, not NotifyAll() -- only that one worker's
		// inbox actually changed. MarkQueuedWork() (release-ordered, matching
		// Thread.h's hasQueuedWork comment) pairs with the worker's own acquire-load in its
		// sleep predicate, closing the same notify-loss race the old blanket approach
		// happened to also close, just without waking every other worker for nothing.
		workers[idx]->MarkQueuedWork();
		workers[idx]->NotifyWorker();
	}
	else {
		const size_t hotN = GetHotWorkers();
		const bool useHi = task->hiPri && hotN;
		uint8_t chosen = (uint8_t)PickNextWorker(task->corePref, useHi);
		if (useHi) hiPriInboxes[chosen]->push(task);
		else
			loPriInboxes[chosen]->push(task);   // collapsed: no lane, no server
		workers[chosen]->MarkQueuedWork();
		workers[chosen]->NotifyWorker();

	}
	return true;
}
bool TaskScheduler::Requeue(Task* task) {
	if (!task) return false;
	// Re-queue a paused task (resumed after Suspend). Unlike PushLocal this does NOT
	// re-count the task -- it was already accounted for at its original submission and
	// is only resuming, not newly created. (The yield path does the same, via the
	// worker's push_bottom.) Otherwise every suspend->resume cycle leaks +1.
	// Same routing as PushLocal -- a RESUMED hiPri task is still on the lane, and sending it back to
	// an ordinary worker would strand it just as surely as a fresh one.
	const size_t hotN = GetHotWorkers();
	const bool useHi = task->hiPri && hotN;
	const uint8_t chosen = (uint8_t)PickNextWorker(task->corePref, useHi);
	if (useHi) hiPriInboxes[chosen]->push(task);
	else
		loPriInboxes[chosen]->push(task);   // collapsed: no lane, no server
	workers[chosen]->MarkQueuedWork();
	workers[chosen]->NotifyWorker();
	return true;
}
int TaskScheduler::PickNextWorker(CorePref pref, bool hiPri) {
	// THE LANE INVARIANT, ENFORCED AT THE ONE PLACE PLACEMENT IS DECIDED. A hiPri task rotates the
	// HOT workers only. Nothing downstream then has to rescue a lane task from a queue nobody
	// drains, because none can ever be put there -- and a rescue would be bailing a sinking ship:
	// it costs a second push, and multiple hiPri producers can outrun one bailing worker.
	//
	// Ordinary work takes the branch below and skips 0..K-1, so the two sets are disjoint. At K = 0
	// there are no hot workers, `hiPri` cannot be true here (the push sites collapse it), and this
	// is the original function unchanged.
	const size_t hotN = GetHotWorkers();
	if (hiPri && hotN) {
		const size_t n = workers.size();
		const size_t m = (hotN < n) ? hotN : n;
		// One rotation step. This used to scan for the first UNPINNED hot worker and fall through to
		// the same expression when every one of them was claimed; with pinning gone (4.0.1) the scan
		// could only ever return on its first iteration, so it collapsed into its own fallback.
		return (int)(nextHotWorker.fetch_add(1, std::memory_order_relaxed) % m);
	}

	// Placement is governed SOLELY by CorePref (see Task.h) -- queue priority (hiPri) is never consulted
	// here; the two axes are fully orthogonal by design. Default/Any/Wide all mean "no class preference"
	// and fall through to the original full-pool round-robin below.

	// Round-robin a worker subset, returning the next worker in that subset (pinning is gone as of
	// or -1 if the set is empty
	// -- which tells the caller to SPILL to the other class rather than block on an unavailable core.
	// DEDICATED HOT WORKERS: ordinary work must never be ROUTED to one. A hot worker exists to have
	// nothing else to do -- that is the entire latency guarantee. Letting a bulk task land there
	// costs a completion the whole duration of that task, because a running task cannot be
	// preempted and no bounded "short work" class exists to steal from instead.
	//
	// Hot workers are indices 0..K-1 by construction, so skipping them is an index test. Zero when
	// K = 0, which is the untouched original behaviour. P/E routing is preserved for the rest.
	// (hotN is read once at the top of this function, above the lane branch.)

	auto pickFrom = [this, hotN](std::vector<int>& set, std::atomic<size_t>& cur) -> int {
		size_t m = set.size();
		if (m == 0) return -1;
		size_t start = cur.load(std::memory_order_relaxed);
		for (size_t i = 0; i < m; ++i) {
			int idx = set[(start + i) % m];
			if ((size_t)idx < hotN) continue;          // reserved for the low-latency lane
			cur.store((start + i + 1) % m, std::memory_order_relaxed);
			return idx;
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
	// SEQ_CST, explicitly, and deliberately NOT relaxed. These were bare `nextWorker + i` /
	// `nextWorker = ...`, which default to seq_cst; on 2026-08-16 they were made explicitly RELAXED
	// on the reasoning that this is only a round-robin HINT (true -- nothing reads it for
	// correctness).
	//
	// REVERTED the same day. That change measured EXACTLY ZERO on throughput/mp (mean 3.06 -> 3.07
	// M tasks/sec over 6 runs each) -- and the very next CI run hung the primitives test on macOS
	// arm64, on a test that had passed at the same iteration count one commit earlier. It is the
	// only ordering WEAKENING in that commit (the deque CAS went the other way, to seq_cst), so it
	// is the prime suspect for having removed a fence something else was accidentally leaning on --
	// which is exactly the class of bug that shows on ARM and never on x86, and this project has
	// already shipped one of those (the 1.2.0 sleep-predicate lost wakeup, hung macOS arm64 one run
	// in three, invisible on TSO -- see NotifyWorker).
	//
	// NOT PROVEN: one pass and one fail is weak evidence against a historically ~1-in-3 flake, and
	// no mechanism has been constructed -- this store happens BEFORE the inbox push, and the
	// push/MarkQueuedWork/NotifyWorker sequence carries its own ordering. It is reverted because the
	// benefit was measured at zero, so there is nothing to weigh against the suspicion. If the macOS
	// hang reproduces WITH this at seq_cst, this is exonerated and the search moves on.
	size_t n = workers.size();
	for (size_t i = 0; i < n; ++i) {
		size_t j = (nextWorker.load(std::memory_order_seq_cst) + i) % n;
		if (j < hotN) continue;                        // reserved; see the note on pickFrom
		nextWorker.store((int)((j + 1) % n), std::memory_order_seq_cst);
		return static_cast<int>(j);
	}
	// Every eligible worker is pinned. Rotate among the NON-hot ones -- returning a hot worker here
	// would put bulk work on the lane precisely when the pool is most loaded, which is the worst
	// moment for it. Falls back to 0 only when every worker is hot, which K < workers.size() makes
	// impossible for any sane K but is not worth crashing over.
	int fallback = nextWorker.load(std::memory_order_seq_cst);
	if (hotN < n) {
		const size_t m = n - hotN;
		fallback = (int)(hotN + ((size_t)fallback % m));
	}
	nextWorker.store((int)(((size_t)fallback + 1) % n), std::memory_order_seq_cst);
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

// BoostTaskPriority/UnboostTaskPriority REMOVED. They were the lock-priority half of the old
// spinlock mutex, and Boost had had no callers since 21719ac -- which made Unboost a permanent
// no-op, since priorityBoost could only ever be zero.
//
// Deleting them matters now rather than being tidiness: hiPri is THE LOW-LATENCY LANE, so a
// mechanism that promotes an AGED ORDINARY TASK to hiPri would push bulk work onto the hot
// workers -- exactly the long-running work the lane is defined to exclude, and unstealable once
// there. A dead function that would be actively wrong if revived is worse than no function.
//
// Task::priorityBoost is deliberately LEFT IN PLACE: the flag block is static_asserted and
// fingerprinted by the stale-library guard, so removing a bit is a separate, deliberate change.
// The bit is now free for a future user.

void TaskScheduler::CleanupTaskMetadata(Task* task) {
	if (!task) return;
	// Metadata is stored directly on task, no cleanup needed (task is about to be freed anyway).
	// The priority-restore that used to be here went with BoostTaskPriority -- see above.
	(void)task;
}


bool JLib::CurrentTaskCancelled() {
	if (!TaskScheduler::IsInitialized()) return false;
	Task* t = TaskScheduler::Instance().GetCurrentTask();
	// Off a task entirely -- main, an app thread -- is not cancelled. So is a task that was never
	// given a scope. Both answer the same way and for the same reason: nobody asked for this work
	// to stop.
	if (!t) return false;
	return IsTaskCancelled(t);
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
			// Become PARKABLE before becoming DISCOVERABLE. Same order as WaitOnEvent, same
			// reason, and this had it backwards until 1.3.5.
			//
			// With the push first, there is a window between clearing spinLock and the status
			// store inside Fiber::Suspend where this fiber sits in waitingFibers but is still
			// RUNNING. An Unlock landing there pops us and calls Resume() -- and ResumeQueueless
			// does not treat RUNNING as resumable, so it silently DROPS the wake. We then park as
			// SUSPENDED with no reference to us left anywhere, while that Unlock has already
			// handed ownership over (it leaves `locked` true whenever it pops a waiter). The mutex
			// stays locked forever with no holder: a deadlock, not a stall. This is what hung the
			// primitives test in CI on both Windows and macOS arm64.
			//
			// Storing WANTS_SUSPEND first closes it. A Resume racing in now sees a resumable state
			// and flips us to SUSPEND_SIGNALED, and the worker's park step wakes us instead of
			// parking.
			current->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
			waiters.push(Waiter{ current, nullptr, nullptr, CancelToken::kNone });
			spinLock.clear(std::memory_order_release);
		}
		// Deliberately NOT Thread::Suspend(). Fiber::Suspend() stores WANTS_SUSPEND
		// UNCONDITIONALLY, which would clobber a SUSPEND_SIGNALED written by a Resume that raced
		// in above -- reintroducing the exact lost wakeup this ordering just closed. WaitOnEvent
		// switches directly for the same reason.
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerMutex::Lock");
		ContextSwitch(&current->ctx, current->homeCtx);
		// Resumed: we have the lock
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
	}
	else {
		// Bare thread: cannot suspend, so help with stolen Native work while waiting -- but only
		// after a brief plain spin comes up empty. See SpinThenHelp for why the fast spin exists
		// and the block comment above ContendedSpinStep for what the escalated path costs.
		SpinThenHelp([this] { return Try_Lock(); });
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
	Waiter next{};
	{
		while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
		wasHolder = lockHolder;
		lockHolder = nullptr;
		holderLock.clear(std::memory_order_release);
	}

	// SKIP-AT-RELEASE. Walk past waiters whose scope has been cancelled, resuming each with
	// Cancelled so it can unwind, until one that still wants the lock is found. Ownership stays with
	// US the whole time -- `locked` is only cleared when the queue runs dry -- so no third party can
	// take the lock in front of the waiter we eventually hand it to.
	//
	// ONE WAITER PER SPINLOCK ACQUISITION, deliberately. Resuming inside the queue lock would nest
	// it under whatever Thread::Resume takes (the notify mutex, a deque push), which is how lock
	// cycles get built. Popping one, releasing, then resuming costs a little more contention when
	// several waiters are cancelled at once -- a rare case -- and cannot deadlock.
	for (;;) {
		bool haveWaiter = false;
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			if (!waiters.empty()) {
				next = waiters.front();
				waiters.pop();
				haveWaiter = true;
			}
			else {
				locked = false;   // nobody left to hand it to; genuinely unlocked now
			}
			spinLock.clear(std::memory_order_release);
		}
		if (!haveWaiter) { next = Waiter{}; break; }

		// A null result slot means the waiter is NOT cancellable and must not be skipped -- see the
		// note on Waiter. Only an opted-in waiter can be passed over.
		//
		// ASK THE TASK, not the token. Fiber::owningTask is the task suspended on this fiber, and a
		// parked waiter is mid-task by definition, so it is exactly the right task for the whole
		// window this queue cares about. Going through IsTaskCancelled is what makes the "one place
		// cancellation is decided" claim in TaskScheduler.h actually true here: a waiter cancelled
		// INDIVIDUALLY (cancelledDirect, no scope involved) is now skipped too. Reading the copied
		// token saw only the scope, so a directly-cancelled waiter parked here was uncancellable.
		if (next.result && IsTaskCancelled(next.fiber ? next.fiber->owningTask : next.coro)) {
			*next.result = WaitResult::Cancelled;
			// Resumed WITHOUT the lock. It returns Cancelled and unwinds; we keep ownership and
			// look for someone who still wants it.
			if (next.fiber) Thread::Resume(next.fiber);
			else if (next.coro && TaskScheduler::IsInitialized())
				TaskScheduler::Instance().Push(next.coro);
			continue;
		}

		if (next.result) *next.result = WaitResult::Ok;
		break;   // this one gets the lock; released below, outside the spinlock
	}

	// Release whichever kind of waiter was queued. This is the only place the two kinds diverge, and
	// both hand the waiter to somebody else -- so after either call the waiter may already be running
	// and neither `next` nor anything it points at may be touched again.
	if (next.fiber) {
		Thread::Resume(next.fiber);
	}
	else if (next.coro) {
		// The coroutine now HOLDS the lock (locked stayed true above). Record it as the holder before
		// it can possibly run, so a concurrent Unlock from it sees a consistent holder rather than a
		// stale one. Then re-push: a worker picks the task up and the resume trampoline continues the
		// coroutine at the point after its co_await.
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = next.coro;
			holderLock.clear(std::memory_order_release);
		}
		if (TaskScheduler::IsInitialized()) TaskScheduler::Instance().Push(next.coro);
	}
}

WaitResult SchedulerMutex::LockCancellable() {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	// The token is still carried in the Waiter, but ONLY so CancelWaiters can tell which SCOPE a
	// waiter belongs to. Whether it is cancelled is asked of the task itself at release, through
	// Fiber::owningTask -- this comment used to claim there was no fiber->task link to walk back
	// through, and there always was one.
	Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
	const uint32_t tok = callerTask ? callerTask->cancelToken : CancelToken::kNone;

	// Already cancelled before we even try: fail immediately rather than acquire a lock the caller
	// is about to be told to drop.
	if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;

	if (current != nullptr) {
		WaitResult result = WaitResult::Ok;   // stack local -- stable until we are resumed
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
				return WaitResult::Ok;
			}
			// Parkable BEFORE discoverable, exactly as Lock() does -- see the long note there; the
			// reverse order is the 1.3.5 lost wakeup.
			current->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
			waiters.push(Waiter{ current, nullptr, &result, tok });
			spinLock.clear(std::memory_order_release);
		}
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerMutex::LockCancellable");
		ContextSwitch(&current->ctx, current->homeCtx);

		// Resumed. `result` says whether that came with the lock or with a cancellation.
		if (result == WaitResult::Cancelled) return WaitResult::Cancelled;
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = callerTask;
			holderLock.clear(std::memory_order_release);
		}
		return WaitResult::Ok;
	}

	// Bare thread: never enqueues, so skip-at-release cannot reach it. It polls the token between
	// attempts instead -- which is also why its cancellation is prompt where a fiber's waits for the
	// next release.
	while (!Try_Lock()) {
		if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;
		ContendedSpinStep();
	}
	return WaitResult::Ok;
}

bool SchedulerMutex::LockAsyncEnqueue(Task* coroTask, WaitResult* result) {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (!locked) {
		locked = true;
		spinLock.clear(std::memory_order_release);
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = coroTask;
			holderLock.clear(std::memory_order_release);
		}
		if (result) *result = WaitResult::Ok;
		return true;
	}
	waiters.push(Waiter{ nullptr, coroTask, result,
	                     coroTask ? coroTask->cancelToken : CancelToken::kNone });
	spinLock.clear(std::memory_order_release);
	return false;
}

bool SchedulerMutex::LockAsyncEnqueue(Task* coroTask) {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (!locked) {
		locked = true;
		spinLock.clear(std::memory_order_release);
		{
			while (holderLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			lockHolder = coroTask;
			holderLock.clear(std::memory_order_release);
		}
		return true;    // acquired outright -- caller must not suspend
	}
	// Queued. The task becomes reachable by Unlock the instant spinLock is cleared, so nothing may
	// touch it after this point.
	waiters.push(Waiter{ nullptr, coroTask, nullptr, CancelToken::kNone });
	spinLock.clear(std::memory_order_release);
	return false;
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
			// Identical lost-wakeup window to SchedulerMutex::Lock -- see the long comment there.
			// Publishing to waitingFibers while still RUNNING lets a racing Signal() pop this
			// fiber and drop the resume on the floor; the permit is consumed by the pop and the
			// fiber parks forever. Become parkable first, and switch directly rather than through
			// Fiber::Suspend so a SUSPEND_SIGNALED set in between is not clobbered.
			current->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
			waiters.push(Waiter{ current, nullptr });
			spinLock.clear(std::memory_order_release);
		}
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerSemaphore::Wait");
		ContextSwitch(&current->ctx, current->homeCtx);
	}
	else {
		// Same contended-wait discipline as SchedulerMutex; see SpinThenHelp and ContendedSpinStep.
		//
		// This RESPECTS t_heldMutexes but does not add to it, and the asymmetry is deliberate. A
		// semaphore permit has no owner: the thread that takes one is frequently not the thread that
		// returns it, which is the entire point of a producer/consumer semaphore. Counting a Wait as
		// an acquisition would make a consumer's count climb forever and permanently disable helping
		// on that thread. So the inversion guard covers mutexes, which have real ownership, and a
		// thread holding only a permit is a documented gap rather than a tracked one.
		SpinThenHelp([this] { return Try_Wait(); });
	}
}


// Cancellable Wait. The release side of this already existed and could not be reached: Signal()
// has walked past cancelled waiters since 2.14.0, but Wait() pushed Waiter{current, nullptr}, and
// a null result slot means "not cancellable, never skipped" by the contract on Waiter. So the
// machinery was complete and had no entry point.
//
// WHY A SEPARATE FUNCTION rather than making Wait() cancellable, which is the same argument as
// SchedulerMutex::Lock vs LockCancellable and worth repeating because getting it wrong is silent:
// a caller who ignored the result would proceed believing it holds a permit it does not hold. Plain
// Wait() can therefore never come back empty-handed, and cancellation is opt-in per call site.
//
// A CANCELLED RETURN MEANS NO PERMIT WAS TAKEN. Do not call Signal() to "give it back" -- there is
// nothing to give back, and doing so manufactures a permit from nowhere.
WaitResult SchedulerSemaphore::WaitCancellable() {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	// The token is still carried in the Waiter so CancelWaiters can select a SCOPE. Whether a waiter
	// is cancelled is asked of its task at release, via Fiber::owningTask -- see SchedulerMutex.
	Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
	const uint32_t tok = callerTask ? callerTask->cancelToken : CancelToken::kNone;

	// Already cancelled before trying: fail immediately rather than consume a permit the caller is
	// about to be told to drop. Taking one here would strand it -- the caller returns Cancelled and
	// has no reason to Signal.
	if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;

	if (current != nullptr) {
		WaitResult result = WaitResult::Ok;   // stack local -- stable until we are resumed
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }

			if (permits > 0) {
				--permits;
				spinLock.clear(std::memory_order_release);
				return WaitResult::Ok;
			}
			// THE PARK-PUBLISH RACE. The check at the top of this function is not enough on its own:
			// a cancel can land between it and the push below, and CancelWaiters would then walk a
			// list this waiter is not on yet -- after which the waiter parks and nothing ever wakes
			// it. Windows hid this because SwitchToFiber usually finished inside the test's sleep;
			// on POSIX the ContextSwitch path made the window wide enough to hang 5 runs out of 5.
			//
			// Re-reading the flag HERE, under the same acquisition that publishes, closes it: the
			// spin lock orders this against CancelWaiters' walk, so either we observe the cancel and
			// never publish, or our push happens-before any later walk and the walk finds us. This is
			// the same rule Thread.cpp already applies to SUSPEND_SIGNALED -- a cancel arriving during
			// "about to park" must not depend on a later walk to be seen.
			if (IsTaskCancelled(callerTask)) {
				spinLock.clear(std::memory_order_release);
				return WaitResult::Cancelled;
			}

			// Parkable BEFORE discoverable, exactly as Wait() does -- see the long note there; the
			// reverse order is the 1.3.5 lost wakeup.
			current->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
			waiters.push(Waiter{ current, nullptr, &result, tok });
			spinLock.clear(std::memory_order_release);
		}
		JLIB_EPOCH_CHECK_NO_GUARD("SchedulerSemaphore::WaitCancellable");
		ContextSwitch(&current->ctx, current->homeCtx);

		// Signal() wrote through `result` before resuming us, so it is settled by the time we run.
		return result;
	}

	// BARE THREAD: no fiber to park, so there is nothing for Signal() to skip and the whole
	// skip-at-release mechanism does not apply. A bare thread cannot be cancelled mid-wait, but it
	// can observe cancellation between attempts, which is what the predicate does -- so this
	// returns Cancelled promptly rather than never.
	//
	// Try_Wait() rather than a blocking wait, so a cancellation that lands while spinning is seen.
	bool got = false;
	SpinThenHelp([&] {
		if (Try_Wait()) { got = true; return true; }
		return IsTaskCancelled(callerTask);
	});
	return got ? WaitResult::Ok : WaitResult::Cancelled;
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

bool SchedulerSemaphore::WaitAsyncEnqueue(Task* coroTask, WaitResult* result) {
	while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
	if (permits > 0) {
		--permits;
		spinLock.clear(std::memory_order_release);
		if (result) *result = WaitResult::Ok;   // acquired outright; nothing will resume us
		return true;    // got a permit -- caller must not suspend
	}
	// Same park-publish re-check as WaitCancellable -- see the long note there. A coroutine suspends
	// after this returns false, so a cancel landing between the caller's check and this push would
	// leave it suspended with nothing on any list to find it.
	if (IsTaskCancelled(coroTask)) {
		spinLock.clear(std::memory_order_release);
		if (result) *result = WaitResult::Cancelled;
		return true;    // answer is settled -- caller must NOT suspend
	}

	// Queued. Reachable by Signal() the instant spinLock clears; touch nothing after.
	// result AND token, or the waiter is by definition not cancellable -- a null result slot means
	// "never skipped" and Signal() will hand it the permit no matter what its scope says.
	waiters.push(Waiter{ nullptr, coroTask, result,
	                     coroTask ? coroTask->cancelToken : CancelToken::kNone });
	spinLock.clear(std::memory_order_release);
	return false;
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

	// 2. Safely manipulate the queue and permits.
	//
	// SKIP-AT-RELEASE, same rule as SchedulerMutex::Unlock: walk past waiters whose scope was
	// cancelled, waking each with Cancelled so it can unwind, and give the permit to the first that
	// still wants it. The permit is never parked in `permits` while a real waiter exists, so nobody
	// can take it in front of the waiter being released. One waiter per spinlock acquisition, so a
	// resume never happens underneath the queue lock.
	if (!waiters.empty()) {
		for (;;) {
			const Waiter next = waiters.front();
			waiters.pop();
			const bool empty = waiters.empty();

			// Release BEFORE resuming: after either call the waiter may already be running, so
			// nothing below may touch it.
			spinLock.clear(std::memory_order_release);

			// Null result slot => not cancellable => never skipped. See the note on Waiter.
			// Ask the TASK, via Fiber::owningTask -- see the matching note in SchedulerMutex::Unlock.
			// This is what lets a directly-cancelled waiter (cancelledDirect) be skipped here too.
			const bool skip = next.result && IsTaskCancelled(next.fiber ? next.fiber->owningTask : next.coro);
			if (skip) *next.result = WaitResult::Cancelled;
			else if (next.result) *next.result = WaitResult::Ok;

			if (next.fiber) Thread::Resume(next.fiber);
			else if (next.coro && TaskScheduler::IsInitialized())
				TaskScheduler::Instance().Push(next.coro);

			if (!skip) return;          // permit handed over

			// That one did not want it. Take the queue lock again and look for another; if there is
			// nobody left, the permit goes back to the counter.
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
			if (empty || waiters.empty()) {
				if (permits < maxPermits) ++permits;
				spinLock.clear(std::memory_order_release);
				return;
			}
		}
	}
	else {
		if (permits < maxPermits) {
			++permits;  // no one waiting, just increment
		}
		// 3. Release lock on this execution path
		spinLock.clear(std::memory_order_release);
	}
}


// EAGER cancellation for a semaphore used as an I/O gate.
//
// WHY THIS EXISTS WHEN Signal()'s SKIP-AT-RELEASE ALREADY HANDLES CANCELLATION. Skip-at-release
// only delivers when somebody releases. That is right for a semaphore guarding a memory pool or a
// short critical section, where a permit comes back promptly. It is wrong for a semaphore used as a
// THROTTLE -- "at most eight concurrent asset streams" -- where a waiter can sit for as long as the
// work in front of it takes, and where the reason to cancel is usually that nobody wants the result
// any more: the player disconnected, closed the menu, or left the zone. Waiting for a release there
// means waiting for exactly the work you are trying to abandon.
//
// So the primitive is unchanged and the CALLER chooses: Wait() cannot be cancelled at all,
// WaitCancellable() learns at the next release, and this ejects a waiter immediately.
//
// A NULL RESULT SLOT IS NEVER EJECTED, and that is the same safety rule as everywhere else: a plain
// Wait() has nowhere to report Cancelled, so waking it would return it to a caller that believes it
// holds a permit. Those waiters stay queued no matter what token they carry.
//
// RESUMES OUTSIDE THE LOCK. Signal() takes the queue lock once per waiter for exactly this reason --
// a resumed task can be running on another worker before the call returns, and touching the queue
// while it does would be a lock held across a handoff. Here the whole eject pass happens under the
// lock, the lock is dropped, and only then is anyone resumed.
//
// The kBuf pass limit is not a cap on how many can be cancelled: the outer loop repeats while a
// pass fills the buffer, so an arbitrarily long queue drains in batches without allocating on what
// is already an error/teardown path.
void SchedulerSemaphore::CancelWaiters(CancelToken tok) {
	constexpr size_t kBuf = 64;
	Waiter victims[kBuf];

	for (;;) {
		size_t n = 0;
		{
			while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }

			std::queue<Waiter> keep;
			while (!waiters.empty()) {
				const Waiter w = waiters.front();
				waiters.pop();

				// Cancellable, matches the scope (or no scope given == everyone), and there is room
				// in this pass. Anything else is put back in order.
				const bool cancellable = (w.result != nullptr);
				const bool matches = !tok.Valid() || CancelToken(w.token).IsWithin(tok);
				if (cancellable && matches && n < kBuf) victims[n++] = w;
				else                                    keep.push(w);
			}
			waiters.swap(keep);

			spinLock.clear(std::memory_order_release);
		}

		if (n == 0) return;

		for (size_t i = 0; i < n; ++i) {
			*victims[i].result = WaitResult::Cancelled;
			if (victims[i].fiber) Thread::Resume(victims[i].fiber);
			else if (victims[i].coro && TaskScheduler::IsInitialized())
				TaskScheduler::Instance().Push(victims[i].coro);
		}

		if (n < kBuf) return;   // the pass did not fill, so the queue held no more matches
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
		waitingQueue.push(CvWaiter{ &localWaitSemaphore, CancelToken::kNone });
		UnlockQueue();

		// 3. Release the outer engine mutex so other threads/fibers can work
		mutex.Unlock();

		// Why a SEMAPHORE and not a fiber queue, and why this is not the 1.3.5 bug.
		//
		// Between UnlockQueue() above and the Wait() below, this fiber is discoverable but not yet
		// waiting -- the same shape as the lost wakeup fixed in SchedulerMutex::Lock in 1.3.5. It is
		// safe here for a structural reason: a Notify landing in that window pops this semaphore and
		// Signals it, Signal finds waitingFibers EMPTY and so takes the ++permits branch, and the
		// Wait() below then sees permits > 0 and returns immediately. A COUNTING semaphore stores an
		// early signal; the mutex's waiter queue could not, which is exactly why it deadlocked.
		// (Verified by widening this window: 5/5 pass, where the same treatment on the mutex's old
		// ordering gave 4/4 deadlocks.)
		//
		// LOAD-BEARING INVARIANT: waitingQueue holds a RAW POINTER into this fiber's STACK FRAME.
		// That is sound only because a waiter cannot leave before it is signalled, and every
		// signaller (Notify_One, Notify_All) removes the pointer from the queue BEFORE calling
		// Signal on it. Break either half and this is a use-after-free on a live fiber stack.
		// In particular, DO NOT add a timed/predicate Wait that can return unsignalled -- it would
		// pop this frame while its address is still queued. A timeout here needs the wait handle to
		// outlive the frame (pooled or refcounted), not just a deadline.
		//
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


// Cancellable Wait. Structurally identical to Wait() with one difference that is the entire point:
// it RE-ACQUIRES THE MUTEX BEFORE RETURNING, cancelled or not.
//
// THE INVARIANT, and why it is not negotiable. A condition variable's contract is that Wait returns
// holding the lock. If a cancelled return skipped that, every caller would need a conditional
// unlock, and the first one to forget would unlock a mutex it does not hold -- corrupting the mutex
// for everyone, from a path that only runs when something is already going wrong. So the lock is
// re-acquired unconditionally and only the RESULT differs; the caller's Unlock stays plain.
//
// Re-acquiring on a cancelled path is a wait, which sounds like it defeats the purpose. It does not,
// for the reason the whole library is built on: for a fiber or a coroutine a wait is a SUSPENSION,
// not a blocked thread. And the mutex being re-acquired is a critical section, which is the one
// thing this design deliberately does not let you abandon -- see SchedulerMutex.
//
// THE STACK-FRAME INVARIANT IS PRESERVED. waitingQueue holds a pointer into this frame, so nothing
// may let this function return while that pointer is still queued. Cancellation cannot return early
// on its own: the only ways out are a notifier (which removes the entry before signalling) and
// CancelWaiters (which removes it before waking). Both remove first, under the lock. That is why
// this is safe where a timed wait would not be.
WaitResult SchedulerConditionVariable::WaitCancellable(SchedulerMutex& mutex) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	Task* callerTask = TaskScheduler::IsInitialized() ? TaskScheduler::Instance().GetCurrentTask() : nullptr;
	const uint32_t tok = callerTask ? callerTask->cancelToken : CancelToken::kNone;

	if (current != nullptr) {
		// Already cancelled: do not park at all. The mutex is still held here and stays held, so
		// this returns exactly as the contract promises -- with the lock, and with nothing waited on.
		if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;

		SchedulerSemaphore localWaitSemaphore(0, 1);

		LockQueue();
		// Park-publish re-check, under the queue lock that orders this against CancelWaiters' walk --
		// the same rule as SchedulerSemaphore::WaitCancellable. Returning here still holds the mutex,
		// which is what the cancelled-return contract requires.
		if (IsTaskCancelled(callerTask)) {
			UnlockQueue();
			return WaitResult::Cancelled;
		}
		waitingQueue.push(CvWaiter{ &localWaitSemaphore, tok });
		UnlockQueue();

		mutex.Unlock();

		// Cancellable, so a Notify that skips this waiter at release reports Cancelled rather than
		// handing it a permit -- and CancelWaiters can eject it outright.
		const WaitResult r = localWaitSemaphore.WaitCancellable();

		// THE ENTRY MAY STILL BE QUEUED. A cancel landing between the push above and the semaphore's
		// own re-check makes that re-check return Cancelled WITHOUT this waiter ever parking -- so
		// CancelWaiters never ejected it and the entry survives, pointing at a stack local that dies
		// the moment this frame returns. Remove it here. Only on the cancelled path, and the queue is
		// short by construction.
		if (r == WaitResult::Cancelled) {
			LockQueue();
			std::queue<CvWaiter> keep;
			while (!waitingQueue.empty()) {
				const CvWaiter w = waitingQueue.front();
				waitingQueue.pop();
				if (w.sem != &localWaitSemaphore) keep.push(w);
			}
			waitingQueue.swap(keep);
			UnlockQueue();
		}

		// UNCONDITIONAL. See the invariant above.
		mutex.Lock();
		return r;
	}

	// Bare thread: no fiber to park, so nothing to eject and no early return to make safe. The
	// cancellation it can observe is the one that is already true.
	if (IsTaskCancelled(callerTask)) return WaitResult::Cancelled;
	mutex.Unlock();
	ContendedSpinStep();
	mutex.Lock();
	return IsTaskCancelled(callerTask) ? WaitResult::Cancelled : WaitResult::Ok;
}

// EAGER cancellation. Removes matching waiters from the queue and then wakes them, in that order.
//
// REMOVE BEFORE WAKE, and never the reverse. Every entry is a pointer into a waiting fiber's stack
// frame; waking a waiter lets that frame die, so waking one whose entry is still queued leaves a
// dangling pointer for the next Notify to signal. The notifiers already obey this rule -- pop under
// the lock, Signal outside it -- and this is the same rule for the same reason.
//
// Waking happens through the local semaphore's own eager path, which refuses to eject a waiter with
// no result slot. So a plain Wait() is never woken here even if its scope is cancelled: it has
// nowhere to report Cancelled, and returning it into its critical section believing the condition
// held would be worse than leaving it parked.
void SchedulerConditionVariable::CancelWaiters(CancelToken tok) {
	constexpr size_t kBuf = 64;
	SchedulerSemaphore* victims[kBuf];

	for (;;) {
		size_t n = 0;
		{
			LockQueue();

			std::queue<CvWaiter> keep;
			while (!waitingQueue.empty()) {
				const CvWaiter w = waitingQueue.front();
				waitingQueue.pop();

				const bool matches = !tok.Valid() || CancelToken(w.token).IsWithin(tok);
				if (matches && n < kBuf) victims[n++] = w.sem;
				else                     keep.push(w);
			}
			waitingQueue.swap(keep);

			UnlockQueue();
		}

		if (n == 0) return;

		// Out of the lock, and only now: each of these can wake and let its frame go.
		for (size_t i = 0; i < n; ++i) victims[i]->CancelWaiters(tok);

		if (n < kBuf) return;
	}
}

void SchedulerConditionVariable::Notify_One() {
	SchedulerSemaphore* nextSemaphore = nullptr;

	LockQueue();
	if (!waitingQueue.empty()) {
		nextSemaphore = waitingQueue.front().sem;
		waitingQueue.pop();
	}
	UnlockQueue();

	// Signal the semaphore out-of-lock to maximize throughput
	if (nextSemaphore) {
		nextSemaphore->Signal();
	}
}

void SchedulerConditionVariable::Notify_All() {
	std::queue<CvWaiter> localQueue;

	// Flush the global wait list into a local thread-isolated stack instantly
	LockQueue();
	std::swap(waitingQueue, localQueue);
	UnlockQueue();

	// Signal all waiting contexts sequentially
	while (!localQueue.empty()) {
		SchedulerSemaphore* sem = localQueue.front().sem;
		localQueue.pop();
		sem->Signal();
	}
}

// Grain-free overload: derive it from the range and the pool, never from the body.
//
// EIGHT leaves per worker -- Cilk's `cilk_for` rule, derived from n and P alone and never from the
// body.
//
// A TUNING CONSTANT IS PART OF THE ALGORITHM IT WAS TUNED FOR, and this line has now demonstrated
// that in both directions within one release. It was 8/worker for the recursive splitter, which
// pays per split and has a serial spine and therefore wants fewer, larger leaves. When ParallelFor
// was briefly pointed at the shared cursor -- which publishes every lane at once and pays one
// fetch_add per slice, so it wants the finer division -- keeping 8 measured 6.23x against 16.55x at
// an explicit grain on a 64 MB memory-bound body. It was changed to defer to the cursor's floor.
// When ParallelFor went BACK to the splitter, that new default was wrong the other way: 4.36x
// against 11.33x on a 2M cheap range, and 0.05x against 1.01x on a small one.
//
// So it is back to 8/worker, matching the algorithm that is actually underneath it. If that
// algorithm changes again, this number is not a constant to preserve -- it is a measurement to redo.
void TaskScheduler::ParallelFor(int begin, int end, std::function<void(int, int)> func) {
	const int n = end - begin;
	if (n <= 0) return;
	const size_t leaves = std::max<size_t>(1, workers.size() * 8);
	const int grain = (int)std::max<size_t>(1, ((size_t)n + leaves - 1) / leaves);
	ParallelFor(begin, end, grain, std::move(func));
}
