// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Thread.h"
#include "../include/Hazard.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
#include "../include/TaskDAG.h"   // OnTaskDiscarded: a discarded DAG task still owes its dependents
#include "../include/IoReactor.h" // Join() stops the completion threads before clearing the pool
#include <cstdlib>            // getenv / _dupenv_s -- JLIB_PARK selects the park primitive
#include <cstring>            // strncpy -- the POSIX arm of the JLIB_WATCHDOG env read. MSVC pulls
                              // this in transitively through <windows.h>, so its absence was a
                              // Linux-only compile failure and the Windows build never saw it.
#include "../include/Timer.h"     // ...and the timer thread, for the same reason
#include "../include/platform.h"
#include "../include/Topology.h"
#include <stdexcept>
#include <cstdio>      // fprintf -- the debug-only event-registry tripwire in GetEvent
#include <vector>
#include <chrono>
using namespace JLib;

// Mode gate, defined next to SetMode further down. Declared here because PushMain -- the first
// submission entry point in this file -- is above that definition.

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

// DEFINED IN THIS TU, DELIBERATELY, and after `instance` above. Both are static storage in the same
// translation unit, so their order is fixed rather than unspecified: `instance` is a constant-
// initialized pointer and this object is trivially constructed, which means the only thing with a
// destructor here is this one, and it cannot run before the pointer it reads exists.
//
// It is the ONLY caller of ~TaskScheduler() and therefore the only path that reaches Join(). See
// the declaration for what that drain does to user code during static destruction.
TaskScheduler::AtExitDestroyer TaskScheduler::atExitDestroyer;

// See the declaration. Idempotent for the same reason Join() is: it early-returns on !poolActive,
// so a harness that tears down explicitly and then exits does not drain twice.
void detail::TeardownForTesting(TaskScheduler& scheduler) { scheduler.Join(); }

// See the header. Joins first -- destroying a pool whose workers are still running would be a
// different and much less interesting crash -- then does the delete that production deliberately
// does not, so that ~TaskScheduler and every member destructor beneath it execute at least once
// somewhere. Clears `instance` so a later Instance() faults loudly rather than handing out a
// dangling pool.
void detail::DestroyForTesting() {
	TaskScheduler* p = TaskScheduler::instance;
	if (!p) return;
	p->Join();
	TaskScheduler::instance = nullptr;
	delete p;
}

TaskScheduler::AtExitDestroyer::~AtExitDestroyer() {
	// DELETE FIRST, CLEAR AFTER -- and not the other way round, which is the tempting order and the
	// wrong one. The pool is genuinely LIVE for the whole of the drain: workers are still spinning,
	// GetBands() reads `instance` on their hot path, and SchedulerSemaphore::CancelWaiters re-pushes
	// parked COROUTINES through TaskScheduler::IsInitialized() / Instance().Push(). Clearing the
	// pointer up front to guard against re-entrant user code would make every one of those take the
	// not-initialized branch, and the coroutine waiters this drain exists to release would be
	// dropped on the floor instead -- the exact abandonment the drain is here to prevent.
	//
	// So the pointer stays valid until the object is actually gone, and clearing it afterwards is
	// what closes the window that genuinely matters: a call arriving AFTER teardown finished.
	// JOIN, THEN LEAK. Deliberately, and this is not laziness -- `delete instance` here was tried
	// and it CRASHES, reproducibly, with an access violation. Not in Join: the drain completes and
	// all 31 workers stop cleanly. It dies afterwards, destroying TaskScheduler's own members.
	//
	// That is not surprising once you notice the path had never run. `instance` was new'd in Init()
	// and never deleted by anything, in any program, ever -- so ~TaskScheduler()'s member teardown
	// is code that has never executed, now being asked to run during static destruction with
	// whatever other translation units' statics are already gone. tests/atexit_teardown_test.cpp
	// reproduces it in two lines: Init(0), return.
	//
	// Join() is the part with the value in it. It stops the service threads, drains every primitive
	// so parked frames unwind and release what they hold, and joins the workers. Destroying the
	// members after that buys nothing a process about to exit needs -- the OS reclaims the memory
	// either way -- so the object leaks on purpose and the crash has nowhere to happen.
	//
	// Clearing the pointer AFTER the join is the same rule as everywhere else: the pool must look
	// live for the whole drain (GetBands reads it, CancelWaiters re-pushes coroutines through
	// Instance()), and anything arriving after teardown finished should find nothing.
	if (instance) instance->Join();
	instance = nullptr;
}

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

	// ---- TURNING THE REACTOR ON RESERVES A LANE, BECAUSE OTHERWISE THERE ISN'T ONE ------------
	//
	// This wiring did not exist and its absence was invisible. `SetIoHotLane` has been the named
	// "I want a latency lane" API since K came back, and NOTHING in the tree ever called it -- so
	// every reactor application got K=0: hiPri routes to the ordinary lane, no worker is reserved,
	// and no worker is kept awake for it. What `EnableIoReactor` reserved was a CORE for the
	// completion thread, which is pool budgeting and a different question entirely.
	//
	// THE COST OF NOT DOING THIS IS PAID ON THE FIRST PACKET, AND THEN ON EVERY BURST. A completion
	// arriving at a pool that has gone quiet has to buy a kernel wake before anything looks at it.
	// That is not a once-at-startup cost: any idle gap re-arms it, so bursty traffic pays it per
	// burst, which is exactly the shape network traffic has.
	//
	// K=1, NOT MORE. One reserved worker was measured to give the whole of the p50 win -- for one
	// core, not the pool -- and a second is a second core that never sleeps. If an application
	// wants a wider lane it says so with SetIoHotLane(k), and the check below leaves it alone.
	//
	// A DEFAULT, NOT AN OVERRIDE. Only applied when nobody has asked for a lane already: an app
	// that called SetIoHotLane(4) or armed a range before enabling the reactor must not be quietly
	// reset to 1. Order-independent on purpose -- the failure it prevents is a setter silently
	// undoing an earlier one, which is the lesson `hotrange=` taught in the bench.
	if (on) {
		// K is static, so "did anyone ask for a lane" is one question, not two. This used to also
		// consult the armed RANGE, which no longer exists.
		if (GetHotWorkers() == 0)
			SetIoHotLane(1);
	}
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

	// ---- TEMP DIAG: JLIB_WATCHDOG=<seconds> --------------------------------------------------
	//
	// A hung test tells you nothing, because the only process that could describe the pool is the
	// one that stopped. This detaches a thread that waits, then prints the band word and PER LANE
	// the deque size against its ring capacity -- which is the number that says whether a deque
	// ever approached its 32,768 ceiling, and therefore whether grow() could have run at all.
	//
	// Off unless the env var is set. REMOVE with the rest of the band/shed instrumentation.
	{
		char buf[16] = {};
#if defined(_MSC_VER)
		size_t elen = 0; char* ev = nullptr;
		if (_dupenv_s(&ev, &elen, "JLIB_WATCHDOG") == 0 && ev) { strncpy_s(buf, sizeof(buf), ev, _TRUNCATE); free(ev); }
#else
		if (const char* ev = std::getenv("JLIB_WATCHDOG")) { std::strncpy(buf, ev, sizeof(buf) - 1); }
#endif
		const int secs = buf[0] ? std::atoi(buf) : 0;
		if (secs > 0) {
			std::thread([secs] {
				std::this_thread::sleep_for(std::chrono::seconds(secs));
				TaskScheduler* inst = instance;
				if (!inst) return;
				const Bands b = GetBands();
				std::fprintf(stderr, "\n[watchdog] after %ds -- bands K=%zu F=%zu Fbase=%zu, workers=%zu, grows=%zu\n",
					secs, b.k, b.f, b.fbase, inst->workers.size(), TaskDeque::GrowCount());
				for (size_t q = 0; q < inst->workers.size(); ++q) {
					std::fprintf(stderr,
						// THE hiPri DEQUE COLUMNS ARE GONE AND SO ARE THEIR SPECIFIERS. They outlived
						// their arguments when the lane deque was deleted, which is not a cosmetic
						// bug: two orphan %zu shifted the three %s onto pointers, so the watchdog
						// CRASHED the process it was called to diagnose (0xC0000005, mid-line).
						// A diagnostic that kills the run is worse than none -- it destroys the
						// state you invoked it to read.
						"[watchdog]  q%-2zu deque size=%-7zu cap=%-7zu | inbox lo=%s hi=%s rs=%s | parks=%u\n",
						q, inst->deques[q]->size(), inst->deques[q]->capacity(),
						inst->loPriInboxes[q]->empty()   ? "empty" : "HAS",
						inst->hiPriInboxes[q]->empty()   ? "empty" : "HAS",
						inst->resumedInboxes[q]->empty() ? "empty" : "HAS",
						GetWorkerParkCount(q));
					// The per-pass phase/pass/drain counters that used to print here are gone: they
					// were atomics on every iteration of the hottest loop in the pool, which is not
					// something to leave in for a diagnostic. What survives is the queue and park
					// state above, which costs nothing until the watchdog fires.
				}
				std::fflush(stderr);
				inst->DumpPoolState("watchdog");
			}).detach();
		}
	}
}
GlobalFiberPool& JLib::TaskScheduler::GetGlobalPool()
{
	if (!instance->globalPool)
		throw std::runtime_error("GlobalFiberPool not initialized!");
	return *instance->globalPool;

}
// NEVER RUNS, and has never run. `instance` is new'd in Init() and nothing deletes it -- the
// at-exit path deliberately joins and leaks rather than deleting, because destroying the members
// after Join CRASHES (see AtExitDestroyer for the measurement). Kept because it is the correct
// thing for a TaskScheduler that is ever destroyed properly, and because deleting it would make
// the leak look accidental instead of chosen.
TaskScheduler::~TaskScheduler() {
	if (!stopFlag)
		Join();

	// ---- RELEASE THE QUEUES WHILE THE ALLOCATOR THEY FREE INTO IS STILL ALIVE -----------------
	//
	// MEMBERS DESTRUCT IN REVERSE DECLARATION ORDER, and this class declares the queues before the
	// allocator:
	//
	//     deques, loPriInboxes, hiPriInboxes, resumedInboxes ...  then  taskAllocator
	//
	// so taskAllocator would be destroyed FIRST and the queue vectors after it. `~TaskMPSCQueue`
	// ends with `alloc_->Free(stub_)` and `~TaskDeque` releases its ring, so every one of those --
	// three queues per worker, ~90 on a 31-worker pool -- would free into an allocator that is
	// already gone. Access violation, 0xC0000005, which is precisely the crash that made
	// `delete instance` unusable and left the pool leaking at exit rather than destroyed.
	//
	// Clearing them HERE fixes it without reordering the declarations: the destructor BODY runs
	// before any member destructor, so the queues are gone while the allocator is still whole. The
	// declaration-order alternative (move taskAllocator to the top so it destructs last) is equally
	// correct and compiler-enforced, but it moves an initialiser in a 3,000-line class to fix
	// something that reads better stated than implied.
	//
	// mainQ NEEDS NOTHING: it is declared after taskAllocator, so it destructs before it, and it
	// frees into a live allocator already. Left alone rather than added for symmetry -- an
	// unnecessary clear here would suggest the ordering is arbitrary when it is not.
	resumedInboxes.clear();
	hiPriInboxes.clear();
	loPriInboxes.clear();
	deques.clear();
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
			int old = t->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
			if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
				t->waitGroup->WakeAll();   // only touches wg if someone registered
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

	// DRAIN EVERY REGISTERED PRIMITIVE, BEFORE THE WORKERS ARE JOINED.
	//
	// This is what makes teardown a drain rather than an abandonment. Each primitive releases its
	// waiters with Cancelled; every parked frame is re-pushed, resumes, observes the cancellation,
	// and UNWINDS -- which is the only way anything it holds gets released. RAII objects run, its
	// WaitGroup slot is decremented, a hazard record goes back. None of that happens to a frame that
	// is simply left parked.
	//
	// ORDER: after the service threads stop (nothing new arrives) and BEFORE the workers are joined,
	// because unwinding is work and needs workers alive to run it. Draining after the join would
	// re-push frames onto a pool that is gone.
	//
	// POP THE HEAD, one at a time, rather than copying the list and walking the copy. The copy is
	// what the re-entrancy comment above demands -- DrainForShutdown resumes frames, and a resumed
	// frame runs IMMEDIATELY, destroys its primitives, and re-enters LeaveRegistry(), which takes
	// this same mutex -- but a copy holds raw pointers to objects that resumption is free to
	// destroy. Drain N unwinding a stack that owns primitive N+1 leaves a dangling entry that drain
	// N+1 then calls a virtual on.
	//
	// Unlinking each primitive BEFORE draining it fixes both at once: the mutex is not held across
	// the resume, a re-entrant LeaveRegistry() on the same object finds it already out and returns,
	// and anything the resumed frame destroys unlinks itself so this loop never reaches it.
	{
		for (;;) {
			WaitPrimitive* p = nullptr;
			{
				std::lock_guard<std::mutex> lk(primitivesMtx);
				p = primitivesHead;
				if (!p) break;
				primitivesHead = p->nextPrimitive_;
				if (primitivesHead) primitivesHead->prevPrimitive_ = nullptr;
				p->nextPrimitive_ = p->prevPrimitive_ = nullptr;
			}
			p->DrainForShutdown();
		}
	}

	{
		registryMtx.lock();
		for (auto& pair : eventRegistry)
			pair.second->SignalAll();

		registryMtx.unlock();
	}
	NotifyAll();

	// ---- QUIESCE BEFORE STOPPING. The step between "cancel" and "stop". -----------------------
	//
	// WHY IT HAS TO EXIST. Everything above CANCELS parked frames -- DrainForShutdown on every
	// primitive, then SignalAll on the event registry. Cancelling makes a frame RUNNABLE; it does
	// not run it. Each of those frames then has to be picked up by a worker, resumed, and allowed
	// to unwind, because unwinding is the only thing that runs its destructors, releases whatever
	// it holds, decrements its WaitGroup slot and returns its hazard record. Stop the workers
	// before that happens and none of it does -- silently, with a fast clean-looking shutdown.
	//
	// The old per-worker Join() got this by accident and never said so: while worker 0 was being
	// stopped and waited on, workers 1..N were still alive running exactly this work. Stopping all
	// workers at once -- which is what fixes the pinned-fiber strand -- removes the accident, so
	// the wait has to become deliberate. Measured with it missing: cancelled frames unwound in
	// 2 runs out of 12.
	//
	// QUIESCENT MEANS: no worker executing, and every queue this pool can hold work in is empty.
	// Both halves are needed. Empty queues alone would pass while a worker is mid-unwind, and
	// nobody busy would pass in the instant before a resumed frame is picked up.
	//
	// TWO CONSECUTIVE CLEAN OBSERVATIONS, because unwinding CASCADES: a frame that finishes
	// unwinding completes its task, which decrements a WaitGroup, which wakes the next frame. A
	// single snapshot lands in the gap between one frame finishing and the next becoming runnable
	// and calls that done.
	//
	// A DEADLINE, AND IT REPORTS. Nothing here can promise the pool drains -- a frame may be
	// blocked on something outside the scheduler entirely. Hanging teardown forever is worse than
	// leaking, so it gives up and DUMPS, because "shutdown leaked" with no further information is
	// the least actionable failure there is.
	{
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
		int cleanPasses = 0;
		bool quiesced = false;

		while (std::chrono::steady_clock::now() < deadline) {
			bool idle = true;

			for (size_t i = 0; i < workers.size() && idle; ++i) {
				if (!workers[i]) continue;
				if (workers[i]->busy.load(std::memory_order_acquire)) idle = false;
				else if (!hiPriInboxes[i]->empty() || !loPriInboxes[i]->empty()
				         || !resumedInboxes[i]->empty()) idle = false;
			}
			// Every deque INCLUDING the non-worker lane, which is one past the workers and can hold
			// work published by a bare thread that was helping.
			for (size_t i = 0; i < deques.size() && idle; ++i)
				// BOUNDED BY deques.size(), WHICH IS ONE LARGER than the inbox vectors -- loPri carries
				// the extra non-worker lane and the inboxes do not. Indexing hiPriInboxes[i] here reads
				// one past the end on the last iteration: an access violation in Release that Debug
				// happened to survive, which is why every test crashed while the Debug build only
				// reported a fiber stall.
				// The lane term is the INBOX now -- there is no lane deque to ask. Its index guard
				// is load-bearing, not defensive: hiPriInboxes is sized to the WORKERS and loPri
				// has one extra entry for the non-worker lane, so the two run out at different i.
				if (deques[i]->size() != 0
				    || (i < hiPriInboxes.size() && !hiPriInboxes[i]->empty())) idle = false;

			if (idle) {
				if (++cleanPasses >= 2) { quiesced = true; break; }
			} else {
				cleanPasses = 0;
			}
			std::this_thread::sleep_for(std::chrono::microseconds(200));
		}

		if (!quiesced) {
			std::printf("[JLib::Scheduler] Join(): the pool did not go quiet within 2s. Cancelled\n"
			            "  frames that have not unwound will NOT run their destructors, release what\n"
			            "  they hold, or return their hazard records. State follows.\n");
			DumpPoolState("Join(): quiescence timeout");
		}
	}

	for (auto& worker : workers)
		worker->RequestStop();

	// THREE PASSES OVER threads[], AND THE ORDER IS THE FIX.
	//
	// This used to be one pass calling Thread::Join(), which flipped `running`, notified, and waited
	// for that worker -- all before touching the next. So worker 0 had exited while 1..N were still
	// draining parked frames, and any fiber PINNED to worker 0 resumed into a queue with no owner
	// left to pop it and no other worker permitted to. The frame was being resumed precisely so it
	// could unwind, so stranding it meant its destructors never ran, its WaitGroup slot never
	// decremented and its hazard record never came back. Silent, and only at shutdown: the teardown
	// drain test failed about one run in three.
	//
	//   1. RequestStop on every worker   -- above. Nobody has left yet.
	//   2. ResumeAll                     -- one direct fiber resume per worker, no kernel object.
	//                                       A parked worker cannot notice step 1 until it is awake.
	//   3. join every OS thread          -- the only step that waits, and by now every worker has
	//                                       already been told to stop, so they unwind concurrently
	//                                       instead of one at a time.
	//
	// ResumeAll wakes WORKERS. The frames parked on primitives were already cancelled further up by
	// the DrainForShutdown pass -- that is what makes them unwind rather than merely become
	// runnable. Both are needed and neither substitutes for the other.
	ResumeAll();

	for (auto& worker : workers)
		if (worker && worker->GetThread().joinable())
			worker->GetThread().join();

	{
		poolMutex.lock();
		// The vector holds RAW Thread*, so clearing it is not enough -- delete them. Safe here and
		// only here: every worker has been Join()ed above, so no thread is still running in one.
		for (Thread* w : workers) delete w;
		workers.clear();
		mainQ.clear();
		poolMutex.unlock();
	}

	poolActive.store(false, std::memory_order_release);
}
// The worker the most recent placement-chosen push was aimed at, -1 before any. Read only by
// DumpPoolState, to mark the row a stalled round trip is waiting on -- see the write site, which is
// after the hiPri spill redirect so it names where the task actually went.
//
// Unconditional rather than behind the stats define: it is one relaxed store on a path that already
// does several, and a stall dump is precisely when a normal Release build needs to know this.
static std::atomic<int> g_lastPushTarget{ -1 };

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
	// hiPri has no deque any more and an MPSC has no size(), so only the loPri deques are counted.
	for (size_t i = 0; i < deques.size(); ++i) queued += deques[i]->size();
	printf("queuedTasks=%zu  paused=%d  poolActive=%d  workers=%zu\n",
		queued,
		(int)paused.load(std::memory_order_relaxed),
		(int)poolActive.load(std::memory_order_relaxed),
		workers.size());
	// ---- THE STEAL HINTS, AND WHY THEY BELONG IN THIS DUMP ------------------------------------
	//
	// A worker only parks when NOTHING IS ADVERTISED POOL-WIDE -- the park condition is
	// popcount(hints) == 0. So a single bit left set for a queue that is actually empty keeps every
	// worker in the pool spinning forever, and from outside that is indistinguishable from a park
	// that does not work: all AWAKE, busy=0, every queue empty. Printing the bits separates those
	// two, which the worker rows alone cannot.
	//
	// BACKLOG AND PARALLEL SEPARATELY, because they are retired by different code. A backlog bit is
	// maintained by the queue's OWNER at the top of each pass; a parallel bit is set by a splitter
	// and cleared by ClearParallelHintIfEmpty. A bit set for an index with no owning worker -- the
	// non-worker lane is exactly that -- has nobody to retire it at all.
	//
	// READ IT AGAINST THE QUEUE COLUMNS: a set bit whose queue shows 0/0 is a LIE, and it is the
	// reason the pool cannot stop.
	{
		unsigned advertised = 0;
		printf("  steal hints (a set bit = 'this queue has work a thief may take'):\n");
		for (size_t w = 0; w < kHintWords; ++w) {
			const unsigned long long b = stealHintBacklog[w].load(std::memory_order_relaxed);
			const unsigned long long p = stealHintParallel[w].load(std::memory_order_relaxed);
			if (!b && !p) continue;
			printf("    word %zu  backlog=0x%016llx  parallel=0x%016llx\n", w, b, p);
			advertised += platform::PopCount64(b | p);
		}
		printf("    advertised queues = %u   -> workers %s park (park requires 0)\n",
		       advertised, advertised ? "CANNOT" : "may");
	}

	// ---- THE AWAKE MAP: who placement is allowed to steer at ---------------------------------
	//
	// PickNextWorker prefers a worker whose bit is set here, because pushing to a running worker
	// costs nothing while pushing to a parked one buys an OS wake. If this map is EMPTY the
	// preference silently does not apply and every push falls through to round-robin -- which lands
	// on parked workers and pays that wake. From the outside that is indistinguishable from the
	// floor not working, and `latency/cold` bouncing between 0.3 us and 4 us is exactly what it
	// looks like.
	//
	// WHAT TO CHECK: the count should be at least the awake floor, and those bits should be the
	// LOW indices (0..K-1), because the floor is workers 0..K-1 by construction. A count of zero
	// with workers plainly AWAKE in the table below means SetAwake is not being called and the
	// preference is dead code.
	{
		unsigned awake = 0;
		for (size_t w = 0; w < kHintWords; ++w) {
			const unsigned long long a = awakeHint[w].load(std::memory_order_relaxed);
			if (!a) continue;
			printf("  awake map   word %zu = 0x%016llx\n", w, a);
			awake += platform::PopCount64(a);
		}
		printf("  awake bits = %u  (floor = %zu)  -> placement %s steer at a running worker\n",
		       awake, GetAwakeFloor(), awake ? "CAN" : "CANNOT");
	}

	// THE YIELD WINDOW, IN THE DUMP RATHER THAN ONLY IN A BENCH, because this is the report someone
	// reads when a worker looks AWAKE and nothing is happening -- which is the exact symptom the
	// fourth state exists to explain. `aimed` counts pushes whose chosen candidate was off the core
	// in its yield window; `re-aimed` counts the ones that found somewhere else to go. aimed == 0
	// means the window is never in the way and the state is costing a load per push for nothing.
	{
		const unsigned aim = YieldAimCount(), re = YieldReaimCount();
		printf("  yield window: %u push(es) aimed at a YIELDing worker, %u re-aimed"
		       "  -> %s\n", aim, re,
		       aim == 0 ? "never hit (the state is buying nothing here)"
		                : (re == aim ? "always found an alternative"
		                             : "some kept a yielding target (safe, one quantum late)"));
	}

	// `phase` is the breadcrumb -- WHERE in Worker() that thread last was. Prints "-" unless the
	// build has JLIBSCHED_STEAL_STATS. It is the column this dump was missing: "AWAKE with empty
	// queues" is a protocol state, not a location, and for a multi-millisecond stall the question
	// is whether the pool was scanning, parked, or not executing at all.
	// lane / flr / tick EARN THEIR WIDTH. A row reading NOTIFIED with every queue empty, no kernel
	// wake and busy=0 looks like a healthy idle pool, and it is the exact signature of a core that
	// is in its yield window or walking back into parkGate after a grow-wake. Without `flr` there
	// is no way to see that this worker entered the pass as a guest while the floor was growing
	// underneath it, and without `tick` no way to see whether this pass took the yield arm at all.
	printf("  q  state           queued lane flr tick busy run   inbox(hi/lo/rs)  deque(hi/lo)  phase\n");
	for (size_t i = 0; i < workers.size(); ++i) {
		const auto s = workers[i]->GetDebugState();
		// THESE MUST TRACK Thread::WorkerState. They did not: after the permit machine landed, slot
		// 1 stopped meaning GOING_TO_SLEEP and started meaning NOTIFIED -- a LATCHED PERMIT, which
		// is a worker that is running or about to consume it, the opposite of one drifting toward
		// sleep. The dump kept printing the old word and read as evidence of the very state the
		// change had removed.
		// KEEP THIS IN STEP WITH Thread::WorkerState. It was left stale once already, after the
		// three-state rename, and the dump then printed the name of a state that no longer existed
		// to the person evaluating the change that removed it.
		static const char* kNames[] = { "EMPTY", "NOTIFIED", "PARKED", "YIELD" };
		// <= 3, AND IT WAS <= 2. "YIELD" was added to kNames above and this bound was not widened
		// with it, so the one state the fourth value exists to make visible printed as "?" and
		// could never appear in this table -- directly under the comment telling the next person to
		// keep the two in step. Count the names, not the states you remember.
		const char* st = (s.workerState >= 0
		                  && s.workerState < (int)(sizeof(kNames) / sizeof(kNames[0])))
		               ? kNames[s.workerState] : "?";
		// ELEVEN specifiers for ELEVEN arguments. It once had one too many: an extra %d ahead of the
		// inbox pair desynchronised everything after it -- the deque sizes (size_t) were read
		// through %d, the marker string through %zu, and the trailing %s consumed an argument that
		// was never passed. That is UB in the one dump you only ever run when something has already
		// gone wrong. Count them against the argument list before changing this line.
		// FOURTEEN specifiers for FOURTEEN arguments -- see the note below; it desynchronised once
		// and read a size_t through %d in the one dump you only run when something is wrong.
		printf(" %2d  %-14s   %d      %d    %d   %4u %d    %d       %d/%d/%d           %zu/%zu  %-9s%s\n",
			s.qIndex, st, (int)s.hasQueuedWork,
			(int)s.laneWake, (int)s.onAwakeFloor, s.spinTick,
			(int)s.busy, (int)s.running,
			(int)!hiPriInboxes[i]->empty(), (int)!loPriInboxes[i]->empty(),
			(int)!resumedInboxes[i]->empty(),
			(size_t)(hiPriInboxes[i]->empty() ? 0 : 1), deques[i]->size(),
			WorkerPhaseOf((size_t)s.qIndex),
			// The signature: parked, but holding work nobody else can take. `rs` belongs here more
			// than either of the others -- a resumed fiber is pinned, so "nobody else can take it"
			// is true of that queue by design rather than by accident of scheduling.
			(s.workerState == 2 && (s.hasQueuedWork || !hiPriInboxes[i]->empty()
				|| !loPriInboxes[i]->empty() || !resumedInboxes[i]->empty()))
					? "   <-- SLEEPING WITH WORK"
			: (g_lastPushTarget.load(std::memory_order_relaxed) == s.qIndex)
					? "   <-- LAST PUSH TARGET" : "");
	}
	if (nonWorkerLane < deques.size()) {
		printf(" nw  %-14s   -      -    -   -     -/-           %zu/%zu%s\n",
			nonWorkerLaneClaimed.load(std::memory_order_relaxed) ? "CLAIMED" : "free",
			(size_t)0, deques[nonWorkerLane]->size(), "");   // hiPriInboxes has no non-worker lane
	}
	fflush(stdout);
}

void TaskScheduler::NotifyAll() {
	for (auto& w : workers)
		w->NotifyWorker();
}

// Resume every worker's pinned park fiber. The one broadcast wake in the system, and it reaches the
// kernel nowhere: it walks the thread table and does a direct fiber resume per entry.
//
// WHAT IT REPLACES. Every "wake everybody" in the old design went through a condition variable --
// one mutex acquire and one kernel signal per worker, per broadcast. This is a load and a CAS per
// worker. A null fiber is skipped and is the ordinary case: a worker that has never gone idle has
// no park fiber, because it has never needed one, and it is by definition already awake and
// looking.
//
// IT WAKES WORKERS AND NOTHING ELSE, which is the distinction to keep hold of during the Join
// rewrite. A user frame parked on a SchedulerMutex, a semaphore or a condition variable is not
// reached by this -- those are cancelled through their primitive's DrainForShutdown, which is what
// makes them UNWIND rather than merely become runnable. Teardown needs both: cancel the waiters so
// stacks unwind, and have workers awake to run the unwinding. Substituting one for the other trades
// a visible hang for a silent leak of every destructor those frames were going to run.
void TaskScheduler::ResumeAll() {
	for (auto& w : workers) {
		if (!w) continue;
		// ONE THING TO WAKE. Wake() publishes the state change and signals the address the worker
		// is blocked on. There is no park fiber to resume first: resuming a fiber only makes work
		// RUNNABLE, and a worker that is genuinely parked is off the run queue and cannot run
		// anything until the OS puts it back. That asymmetry is why Join() hung -- every parked
		// worker had runnable work and no thread awake to take it.
		w->Wake();
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
// How many workers are never allowed to park. See SetAwakeFloor in the header for why this exists
// and what it costs at each value. Default 1: one worker stays warm so a dispatch into an otherwise
// idle pool finds somebody already running instead of buying an OS wake.
// THE SHIPPED DEFAULT IS 2, and it is a game default rather than a universal one.
//
// The floor is how many workers never park. Measured on a 31-worker box, serial round trip:
//     floor=0   p50 ~0.5 us, but every push wakes a sleeper -- ~20,000 kernel wakes in the row,
//               a nearly flat landing spread (~640 per queue), and a tail that reaches 52-105 us.
//     floor=2   0 kernel wakes, p99 in the same band as p50, tail bounded by preemption of the
//               spinner rather than by a lost or slow wake.
// Two cores' worth of spin buys a tail that a frame budget can actually be written against, which
// is the trade a game wants. It is NOT the trade a batch flood wants: at floor=0 the other 29
// workers are free to spin or wake on a no-op storm, and the throughput rows are higher for it.
// Do not read those rows as an argument against the floor -- they are measuring the surplus
// workers, not the dispatch path.
//
// floor=0 remains available and remains meaningful: "every core may park", for a process that
// cares about background CPU more than about its tail. 1 is the cheap middle.
// ---- THE FLOOR IS THE RESERVED SET, AND NOTHING ELSE ----------------------------------------
//
// Workers [0, R) are dedicated to hiPri AND never park. Ordinary work never targets them, so there
// is no second population to size, no clamp, and no way for the two to fight over the same indices.
// Default 1: one core held for I/O.
//
// WHY BULK DOES NOT GET A FLOOR. It was measured and it does not want one -- 1p on this box:
//
//     floor=0   8.4 M/s        floor=2   ~7.5 M/s        floor=3   ~3.7 M/s
//
// Every never-parking worker taxes the producer more than it returns as a receiver, which is the
// same reason IdlePolicy::NoSleep is not the default. The floor was originally introduced to remove
// the OS wake from the dispatch path, and that job now belongs to the reserved hiPri workers, who
// are awake for a reason rather than on the chance that a push arrives.
//
// THE FRAGILITY THIS REMOVES was not theoretical: with the floor and the reserved set sized
// independently, R=2 against floor=2 reserved BOTH never-parking workers, ordinary placement had no
// awake target, and the bench stalled during warmup. One population cannot outgrow the other when
// there is only one population.
//
// So: [0, R) hiPri only and always awake; [R, N) ordinary, parks when idle.
// ---- K, F AND Fbase LIVE IN ONE WORD -------------------------------------------------------------
//
// THE BANDS ARE [0,K) [K,K+F) [K+F,N), SO BAND MEMBERSHIP IS A QUESTION ABOUT THE PAIR, NOT ABOUT
// EITHER NUMBER. While these were three atomics, every site that asked "am I on the floor" did two
// loads and could be answered with a K from before a promotion and an F from after it. No amount of
// re-checking fixes that -- it only narrows the window.
//
// MEASURED, and it is not a theoretical race. K only started actually moving once the controller got
// a periodic caller, and the very first runs with a live K reported 91-108 parks by workers the floor
// claimed, 30-32 of them confirmed against a re-read taken a few instructions later: the check passed
// on one pair and the counter failed on another. Every earlier run looked clean only because K never
// moved.
//
// ONE 64-BIT WORD, ONE LOAD, ONE CONSISTENT ANSWER. 16 bits each is far past any real pool (65535
// workers) and leaves room to grow. Writers use CAS loops so a K change and an F change cannot lose
// each other; readers that need the pair call GetBands() and get a snapshot that was true at one
// instant, which is the only thing a band decision can be based on.
//
// This subsumes the "never move both controllers in the same window" interlock: they may move
// whenever they like, because nobody can observe a torn pair.
// ---- ONE WORD, TWO CONTROLLERS, TWO INPUTS -------------------------------------------------------
//
//     bits  0.. 7   F      -- live floor width      (F controller writes this and nothing else)
//     bits  8..15   Fbase  -- immutable after Init  (policy; the floor sheds back TO this)
//     bits 16..23   K      -- live reserved width   (K controller writes this and nothing else)
//     bits 24..31   Kmax   -- cap, set at Init      (K may never exceed it)
//
// PACKING IS FOR CONSISTENCY, NOT CONTROL. One load gives a worker a (K, F, Fbase) that was true at
// a single instant, so a pass cannot park on an old F and skip its notify on a new one. It does NOT
// license one controller driving both: K reads only reserved/IO depth, F reads only compute backlog,
// and they share no "should we be bigger" state.
//
// THE CAS REFUSES ANYTHING THAT BREAKS:   K + F <= N     F >= Fbase     K <= Kmax
//
// AND IT NEVER DOES F-- AND K++ IN ONE STEP. If K wants a slot and K+F == N, K is refused; F sheds on
// a later pass and K may grow then. Doing both at once turns a compute worker into a reserved one
// while it may still be holding a loPri leaf -- which then runs at TIME_CRITICAL on a core that is
// supposed to be answering completions.
//
// I GOT THAT BACKWARDS ONCE: an earlier version let an explicit SetHotWorkers shrink F to make room.
// It "worked" and it is the same hazard wearing a caller's authority.
static std::atomic<uint64_t> g_bands{ (2ull) | (2ull << 8) };   // F=2, Fbase=2, K=0, Kmax=0

static constexpr uint64_t kBandF     = 0;
static constexpr uint64_t kBandFbase = 8;
static constexpr uint64_t kBandK     = 16;
static constexpr uint64_t kBandKmax  = 24;
// ---- Kmin JOINS THE WORD, bits 32..39 --------------------------------------------------------
//
// It lived in a standalone g_hotMin beside a g_hotMax, which made the SCALING RANGE a second source
// of truth about K -- and the worker had to reach for GetHotWorkerRange() to ask whether the
// controller could move, a third spelling of K on a thread that already had the band word in hand.
// The spec's layout stops at bit 31 and the word is 64 wide, so the range fits with room to spare
// and every band question becomes one load.
//
// Kmax at 24..31 was already here, so this only had to move its partner.
static constexpr uint64_t kBandKmin  = 32;
static inline size_t BandField(uint64_t w, uint64_t sh) noexcept { return (size_t)((w >> sh) & 0xFFull); }
static inline uint64_t BandPut(uint64_t w, uint64_t sh, size_t v) noexcept {
	return (w & ~(0xFFull << sh)) | ((uint64_t)(v & 0xFFu) << sh);
}

// Acquire on the read side pairs with the release on every mutating CAS, so a worker that observes a
// new band also observes whatever the controller published before it.
static inline uint64_t BandsWord() noexcept { return g_bands.load(std::memory_order_acquire); }
static inline size_t BandsK()  noexcept { return BandField(BandsWord(), kBandK);     }
static inline size_t BandsF()  noexcept { return BandField(BandsWord(), kBandF);     }
static inline size_t BandsFb() noexcept { return BandField(BandsWord(), kBandFbase); }

static inline void BandsSetFb(size_t fb) noexcept {
	uint64_t cur = g_bands.load(std::memory_order_relaxed), next;
	do { next = BandPut(cur, kBandFbase, fb); }
	while (!g_bands.compare_exchange_weak(cur, next, std::memory_order_release, std::memory_order_relaxed));
}
static inline void BandsSetKmax(size_t km) noexcept {
	uint64_t cur = g_bands.load(std::memory_order_relaxed), next;
	do { next = BandPut(cur, kBandKmax, km); }
	while (!g_bands.compare_exchange_weak(cur, next, std::memory_order_release, std::memory_order_relaxed));
}
static inline void BandsSetKmin(size_t km) noexcept {
	uint64_t cur = g_bands.load(std::memory_order_relaxed), next;
	do { next = BandPut(cur, kBandKmin, km); }
	while (!g_bands.compare_exchange_weak(cur, next, std::memory_order_release, std::memory_order_relaxed));
}
// THE SCALING RANGE, BOTH ENDS FROM ONE LOAD. `max > min` is the ONLY predicate that means "the
// controller may move K" -- MaybeAdjustHotWorkers gates on it, and so does the worker's observer.
// Two readers asking it of two different loads is how the observer ends up off while the controller
// runs, which starves it and makes K shed every window.
static inline bool BandsScaling(uint64_t w) noexcept {
	return BandField(w, kBandKmax) > BandField(w, kBandKmin);
}

// F WRITER. Clamped to [Fbase, N-K] -- never below the configured base, never into the reserved band
// or past the pool. Returns the previous F.
static inline size_t BandsSetF(size_t f, size_t n) noexcept {
	uint64_t cur = g_bands.load(std::memory_order_relaxed), next;
	size_t prev;
	do {
		prev = BandField(cur, kBandF);
		const size_t k  = BandField(cur, kBandK);
		const size_t fb = BandField(cur, kBandFbase);
		size_t want = (f < fb) ? fb : f;                      // F >= Fbase
		// DEGENERATE POOL: with fewer than two workers there is nobody to keep a slot parkable FOR,
		// and forcing F to 0 there is not conservatism -- it flips the only worker from never-park to
		// parkable and hands it the single-worker lost-wakeup path. Leave the bands alone below 2.
		if (n >= 2) { const size_t room = (n > k + 1) ? n - k - 1 : 0;   // K + F <= N, one left parkable
		              if (want > room) want = room; }
		next = BandPut(cur, kBandF, want);
	} while (!g_bands.compare_exchange_weak(cur, next, std::memory_order_release, std::memory_order_relaxed));
	return prev;
}
// Conditional F write for the collapse: succeeds only if F is still `expected`, and preserves
// whatever K did meanwhile.
static inline bool BandsCasF(size_t expected, size_t desired) noexcept {
	uint64_t cur = g_bands.load(std::memory_order_relaxed);
	for (;;) {
		if (BandField(cur, kBandF) != expected) return false;
		const uint64_t next = BandPut(cur, kBandF, desired);
		if (g_bands.compare_exchange_weak(cur, next, std::memory_order_release, std::memory_order_relaxed))
			return true;
	}
}

// K WRITER. Refused -- not accommodated -- if it would break K+F <= N. F is NEVER decremented here.
// Returns the previous K. `k` is also capped at Kmax when one has been set.
static inline size_t BandsSetK(size_t k, size_t n) noexcept {
	uint64_t cur = g_bands.load(std::memory_order_relaxed), next;
	size_t prev;
	do {
		prev = BandField(cur, kBandK);
		const size_t f    = BandField(cur, kBandF);
		const size_t kmax = BandField(cur, kBandKmax);
		size_t want = k;
		if (kmax && want > kmax) want = kmax;                 // K <= Kmax
		if (n >= 2) { const size_t room = (n > f + 1) ? n - f - 1 : 0;   // K + F <= N, one left parkable
		              if (want > room) want = room; }
		next = BandPut(cur, kBandK, want);
	} while (!g_bands.compare_exchange_weak(cur, next, std::memory_order_release, std::memory_order_relaxed));
	return prev;
}

// ---- BASE vs CURRENT, AND WHY THERE HAS TO BE A BASE -----------------------------------------
//
// g_awakeFloorBase is what the PROCESS ASKED FOR. g_awakeFloor is what the floor is RIGHT NOW,
// which push-side growth may raise above the base for as long as a wave lasts.
//
// WITHOUT THE SPLIT, GROWTH IS ONE-WAY IN PRACTICE. Measured: a 16-task burst grew the floor 2 -> 8
// and it was still 8 during the serial latency row that ran after it. Eight spinners against one
// producer cost more than the wake tail they saved -- 1p 10.0 -> 5.74 M/s, serial p50 0.40 -> 0.90
// us, p99 0.60 -> 1.70, DAG 4.67 -> 5.57. Zero kernel wakes the whole time: the wake tail was
// traded for always-on contention, which is a worse deal at idle and a better one only mid-wave.
//
// The old demote could not undo it either, and not because its RULE was wrong: it steps down one
// worker per kFloorDownNs after kQuietToDemote quiet windows, so 8 -> 2 takes six gated steps and
// the row it needed to fix is 25 ms long. A controller that sheds slower than the workload changes
// is a controller the workload never sees. Collapsing straight to the base is the fix, and it is
// safe precisely BECAUSE there is a base to collapse to -- "shed one and see" is what needs care.
// Whether ordinary placement aims at the LIVE floor or the BASE floor. See PickNextWorker.
//
// DEFAULT ON since 4.0.2. Growth that widens only the AWAKE set and not the RECEIVING set promotes
// workers that then have to steal their way to the work, and the blocking crossover measured that
// directly: 256 pushes per batch landing on 2 receivers while 14 more sat awake and empty.
//
// It is safe to have on by default for the same reason the cap was safe to lift -- the wide landing
// zone exists only while the floor is grown, and the floor is grown only while a wave is in flight.
// A quiet pool sheds to base and placement narrows with it, so the serial round trip -- which is
// what the narrow steer was bought to protect -- never sees a wide zone. THAT is the claim to
// re-check if the serial row ever regresses; it is a property of the SHED, not of this flag.
static std::atomic<bool> g_placementFollowsGrownFloor{ true };
void TaskScheduler::SetPlacementFollowsGrownFloor(bool on) noexcept {
	g_placementFollowsGrownFloor.store(on, std::memory_order_relaxed);
}
bool TaskScheduler::GetPlacementFollowsGrownFloor() noexcept {
	return g_placementFollowsGrownFloor.load(std::memory_order_relaxed);
}

// ---- RESERVED FOR hiPri ----------------------------------------------------------------------
//
// Workers [0, R) take hiPri work ONLY. Ordinary placement skips them, so a completion steered there
// finds a worker that is awake AND not already inside a bulk body.
//
// WHY THIS AND NOT THE FLOOR ALONE. The floor makes a worker awake; it does not make it free.
// Measured with hiPri steered at the unreserved floor (tests/io_overlap_test.cpp, pool saturated
// with 400 us bodies): hiPri lost to ordinary placement at p99 -- 0.12x, 0.91x, 0.62x -- because
// steering concentrated every completion onto two workers that were busy, while an unsteered push
// dispersed across all 31. Concentrating only wins if the target is kept free.
//
// AND ORDERING CANNOT SUBSTITUTE. "Drain hiPri before your own deque" applies when a worker picks
// its NEXT task; it does nothing about the 400 us body it is already in, and a running task cannot
// be preempted. That is why the busy-pool max was 378-1160 us -- one to three whole bodies -- with
// a p50 of 0.8 us. Reservation is the only thing that addresses the max.
//
// 1 BY DEFAULT: enough to receive completions, cheap enough that a 31-worker pool gives up 3% of
// its compute. R must be <= the awake floor or the reserved workers would park and a completion
// would buy the OS wake this whole scheme exists to avoid -- see ReservedHiPri().
// Default WaitAddress. On Linux this is FUTEX_WAIT and stays -- the Windows condvar finding does not
// transfer, they are different primitives with different costs.
// ENV OVERRIDE, so the A/B is not limited to the one binary that parses flags. Every test in the
// suite parks, and a park primitive that hangs will hang them -- but only if they can be made to
// use it. JLIB_PARK=cv flips the default for a whole run without editing thirty mains, which is
// what makes "does the condvar arm survive the existing tests" a question that can be answered at
// all. Read once, on first use; the flag still wins if something calls SetParkPrimitive later.
static TaskScheduler::ParkPrimitive ParkPrimitiveDefault() noexcept {
	const char* e =
#if defined(_MSC_VER)
		nullptr;
	size_t elen = 0; char ebuf[16] = {};
	if (_dupenv_s((char**)&e, &elen, "JLIB_PARK") != 0) e = nullptr;
	if (e) { strncpy_s(ebuf, sizeof(ebuf), e, _TRUNCATE); free((void*)e); e = ebuf; }
#else
		std::getenv("JLIB_PARK");
#endif
	if (e && (e[0] == 'c' || e[0] == 'C')) return TaskScheduler::ParkPrimitive::CondVar;
	if (e && (e[0] == 'w' || e[0] == 'W')) return TaskScheduler::ParkPrimitive::WaitAddress;

	// ---- DARWIN PARKS ON THE CONDVAR, AND IT IS NOT A SECOND CHOICE ----------------------------
	//
	// It is the ONLY choice. There is no address-wait to fall back to: WaitOnAddress is Windows,
	// FUTEX_WAIT is Linux, and Darwin's equivalent is __ulock_wait, which is private API. Without
	// this the `#else` arm of the park loop is a bare `break` -- every idle worker spins on its core
	// forever, the awake floor means nothing because no worker was ever going to park anyway, and a
	// pool sized to the machine burns the machine.
	//
	// So the measurement that picked WaitAddress on Windows does not apply here and must not be
	// cited here: it compared two working primitives, and on Darwin there is one. Losing by ~4% to
	// something that does not exist on this platform is not a reason to spin instead.
	//
	// JLIB_PARK=wait still works here and still means "no park at all" -- useful precisely once, as
	// the negative control that shows what the condvar is buying.
#if defined(__APPLE__)
	return TaskScheduler::ParkPrimitive::CondVar;
#else
	return TaskScheduler::ParkPrimitive::WaitAddress;
#endif
}
static std::atomic<TaskScheduler::ParkPrimitive> g_parkPrimitive{ ParkPrimitiveDefault() };

void TaskScheduler::SetParkPrimitive(ParkPrimitive p) noexcept { g_parkPrimitive.store(p, std::memory_order_relaxed); }
TaskScheduler::ParkPrimitive TaskScheduler::GetParkPrimitive() noexcept { return g_parkPrimitive.load(std::memory_order_relaxed); }

// ---- A RESERVED BAND THAT PARKS IS NOT A RESERVATION ---------------------------------------------
//
// Default FALSE, and then SetHotWorkers/SetHotWorkerRange turn it ON whenever K > 0. It is not an
// independent policy: [0,K) exists so a completion lands on a thread that is awake and holding no
// bulk work, and a worker that slept satisfies neither half. With this off, K bought nothing except
// a name -- the bench banner reported `K IS NOMINAL: 2 of 2 workers in [0,2) PARKED`, i.e. two
// parkable cores wearing a reserved label, which is the state Jay called "how you debug the wrong
// bug for another night".
//
// AT K == 0 IT STAYS OFF, because there is no band to keep awake and spinning [0,0) is free but
// meaningless. That is also why this is driven from the K setters rather than being derived inside
// ReservedNeverParks(): an explicit SetReservedNeverParks AFTER the K call still wins, so the
// negative control (reserve, but let them sleep) remains available for A/B.
static std::atomic<bool> g_reservedNeverParks{ false };
bool TaskScheduler::ReservedNeverParks() noexcept { return g_reservedNeverParks.load(std::memory_order_relaxed); }
void TaskScheduler::SetReservedNeverParks(bool on) noexcept { g_reservedNeverParks.store(on, std::memory_order_relaxed); }

size_t TaskScheduler::ReservedHiPri() noexcept {
	// RESERVATION IS K. There is one reserved band, [0, K), and one name for it -- keeping a second
	// count alongside K is what let them disagree, and a disagreement here means ordinary work on an
	// I/O core or an I/O push onto a compute core.
	return GetHotWorkers();
}
void TaskScheduler::SetReservedHiPri(size_t r) noexcept {
	// Kept as a spelling of SetHotWorkers so existing callers and the bench flag keep working.
	SetHotWorkers(r);
}

size_t TaskScheduler::GetAwakeFloorBase() noexcept {
	return BandsFb();
}

// Power-of-two-minus-one is assumed by the & test; a caller passing 100 gets 127's behaviour on some
// values and not others, so round UP to the next all-ones value rather than trusting the input.
// 7 = yield every 8 idle passes. WAS 0x3FF (every 1024), which was a rate limit chosen when the only
// spinners were two floor workers and which stopped being one the moment the floor could grow wide.
//
// THIS IS COUPLED TO kFloorGrowCap AND MUST NOT BE RAISED WITHOUT LOWERING THAT. Lifting the cap to
// n-1 alone made the marl blocking row WORSE than the old capped default at d>=150 (15.5/17.6/13.5
// against 10.3/10.5/9.9) because 30 never-parking workers with a rude spin starve any other runnable
// thread -- the same pathology the permanent floor=31 arm showed. With this default the same rows
// read 7.1/7.1/7.7 and the round trip is unchanged. The cap was accidentally protecting against the
// spin, and the honest fix is the spin, not the cap.
//
// Measured free at the shipped floor on the user's machine across two independent runs: round-trip
// median 0.548 vs 0.596 and 0.570 vs 0.624 -- politer was slightly TIGHTER, not slower.
static std::atomic<unsigned> g_spinYieldMask{ 7 };
void TaskScheduler::SetSpinYieldMask(unsigned mask) noexcept {
	unsigned m = mask;
	m |= m >> 1; m |= m >> 2; m |= m >> 4; m |= m >> 8; m |= m >> 16;
	g_spinYieldMask.store(m, std::memory_order_relaxed);
}
unsigned TaskScheduler::GetSpinYieldMask() noexcept { return g_spinYieldMask.load(std::memory_order_relaxed); }

// THE FLOOR SIZE BELOW WHICH A FLOOR WORKER NEVER YIELDS. See the header for why it exists; the
// short version is that yield() is politeness aimed at a WIDE spinning set, and on a two-bit steer
// set it is a coin flip that a push lands on the core that just stepped off.
static std::atomic<size_t> g_yieldFloorMin{ TaskScheduler::kYieldFloorMinDefault };
void   TaskScheduler::SetYieldFloorMin(size_t f) noexcept { g_yieldFloorMin.store(f, std::memory_order_relaxed); }
size_t TaskScheduler::GetYieldFloorMin() noexcept { return g_yieldFloorMin.load(std::memory_order_relaxed); }

// CpuRelax iterations per idle pass. Kept SHORT deliberately -- it sits between two consecutive
// looks at the queues, so it is dispatch latency for anything that arrives during it. A worker that
// wants to back off further should be parking, not relaxing longer.
static std::atomic<unsigned> g_workerRelax{ TaskScheduler::kWorkerRelaxDefault };
void     TaskScheduler::SetWorkerRelax(unsigned n) noexcept { g_workerRelax.store(n, std::memory_order_relaxed); }
unsigned TaskScheduler::GetWorkerRelax() noexcept { return g_workerRelax.load(std::memory_order_relaxed); }

// AN EXPLICIT CALL PINS THE BASE. Without this flag the pool-size default below could not tell an
// app that asked for floor=2 on a 4-core box from one that never asked at all, and would quietly
// overrule the first. Policy the caller stated always wins.
static std::atomic<bool> g_awakeFloorBaseExplicit{ false };
void TaskScheduler::SetAwakeFloor(size_t k) noexcept {
	g_awakeFloorBaseExplicit.store(true, std::memory_order_relaxed);
	// The caller is stating policy, so this moves the BASE as well as the current value. Growth
	// (NoteFloorCrowding) and shedding (CollapseAwakeFloorToBase) only ever move the current one.
	BandsSetFb(k);
	const size_t prev = BandsSetF(k, instance ? instance->workers.size() : 0);

	// A PROMOTION MUST WAKE THE WORKERS IT JUST PROMOTED. Storing the number is not enough: a
	// worker at index prev..k-1 parked while it was still ABOVE the floor, and nothing in its own
	// loop will ever run again to notice the floor moved -- it is asleep. Without this the floor is
	// a claim rather than a fact.
	//
	// MEASURED, and it is what made the awake map disagree with the floor: the dump read
	// `awake bits = 2 (floor = 6)`. The controller had promoted four times, each promotion did
	// nothing, and it kept promoting because the wake-misses it was reacting to never stopped --
	// placement still had only two workers to steer at, so pushes kept landing on parked ones.
	//
	// The K-hot controller this was ported from ended SetHotWorkersEffective with exactly this
	// loop. I took its decision rules and left behind the part that makes a decision take effect.
	//
	// GROWTH ONLY. Shrinking needs no action: a worker that is no longer on the floor simply parks
	// the next time it finds nothing, through the ordinary path.
	if (!instance || k <= prev) return;
	// [K+prev, K+k), NOT [prev, k). The floor is [K, K+F), so F values are WIDTHS and the workers
	// this growth added are offset by live K -- the same floor-indices-as-absolute-indices bug
	// NoteFloorCrowding's wake already had and fixed. At K=0 the two spell the same range, which is
	// how this copy survived: SetAwakeFloor is usually called at init, before anyone sets K.
	const size_t kNow = BandsK();
	for (size_t i = kNow + prev; i < kNow + k && i < instance->workers.size(); ++i) {
		if (Thread* w = instance->workers[i])
			w->NotifyWorker(/*force*/ true);
	}
}
size_t TaskScheduler::GetAwakeFloor() noexcept         { return BandsF(); }

// When the last growth happened. Read by the collapse so a wave is not sheared off mid-flight by an
// overflow worker that happens to find one idle instant between two tasks.
static std::atomic<long long> g_lastFloorGrowNs{ 0 };

// High-water mark of the CURRENT floor since the last reset -- see NoteFloorCrowding for why a
// transient needs one. Not reset by the collapse: the whole point is that it outlives the shed.
static std::atomic<size_t> g_awakeFloorPeak{ 0 };

size_t TaskScheduler::GetAwakeFloorPeak() noexcept {
	return g_awakeFloorPeak.load(std::memory_order_relaxed);
}
// Wake exactly one PARKED worker so it can come and steal. Round-robin over the pool so repeated
// ranges do not all wake the same thread, and skip anyone already awake -- waking a running worker
// is a wasted syscall, and the whole point of the awake floor is that those workers need no wake.
//
// Returns immediately once one is woken: the caller wants ONE thief, and the cascade does the rest
// (see the call site in the splitter for why one is enough).
// Minimum spacing between steal wakes. "Once per range" is what the 0 -> 1 edge is supposed to buy,
// but ClearParallelHintIfEmpty re-arms that edge every time a lane momentarily drains -- so for a
// CHEAP body, whose lanes drain constantly, the edge fires over and over and the wake becomes
// per-drain. That is the wake-per-split shape that has cost this splitter before.
//
// 2 us is longer than the drain-and-refill cycle of a trivial body and far shorter than the time a
// woken thief needs to arrive, so it suppresses the storm without delaying the cascade: each wake
// still begets the next through its own lane.
static constexpr long long kStealWakeSpacingNs = 2'000;
static std::atomic<long long> g_lastStealWakeNs{ 0 };

void TaskScheduler::WakeOneForSteal() noexcept {
	const long long now = MonotonicNs();
	long long last = g_lastStealWakeNs.load(std::memory_order_relaxed);
	if (now - last < kStealWakeSpacingNs) return;
	if (!g_lastStealWakeNs.compare_exchange_strong(last, now, std::memory_order_relaxed)) return;
	const size_t n = workers.size();
	if (!n) return;
	const unsigned r = (unsigned)nextWorker.fetch_add(1, std::memory_order_relaxed);
	for (size_t i = 0; i < n; ++i) {
		const size_t idx = ((size_t)r + i) % n;
		Thread* w = workers[idx];
		if (w && w->GetWorkerState() == 2 /* WS_SLEEPING */) {
			w->NotifyWorker(/*force*/ true);
			return;
		}
	}
}

// ---- WAKE UP TO `n` SLEEPERS FOR A LIVE RANGE ------------------------------------------------
//
// NOT RATE-LIMITED, AND THE OMISSION IS THE POINT. WakeOneForSteal above is spaced by
// kStealWakeSpacingNs because its caller is the SPLITTER, which publishes repeatedly and would
// otherwise wake on every split -- the spacing is what makes "once per range" true. This caller is
// different: it fires once per COMPLETED LEAF, already gated on that leaf having cost more than a
// wake, so the evidence is per-call and throttling it would discard exactly the signal it is built
// on. Routing recruitment through the spaced path would cap the cascade at one worker per 2 us
// window and reproduce the ramp it exists to remove.
//
// ONE PASS, ROTATING START. The scan visits each index once from a rotating offset, so concurrent
// recruiters do not all target the same sleeper, and a pool with fewer sleepers than `n` simply
// wakes what it has instead of spinning looking for more.
void TaskScheduler::WakeForSteal(size_t n) noexcept {
	if (!n) return;
	const size_t sz = workers.size();
	if (!sz) return;
	const unsigned r = (unsigned)nextWorker.fetch_add(1, std::memory_order_relaxed);
	size_t woken = 0;
	for (size_t i = 0; i < sz && woken < n; ++i) {
		const size_t idx = ((size_t)r + i) % sz;
		Thread* w = workers[idx];
		if (w && w->GetWorkerState() == 2 /* WS_SLEEPING */) {
			w->NotifyWorker(/*force*/ true);
			++woken;
		}
	}
}

void TaskScheduler::RedistributeToOverflow(size_t ownerIdx, size_t count) {
	// ---- OFF BY K, SECOND INSTANCE. THE FLOOR ACCESSORS RETURN WIDTHS ------------------------
	//
	// The band is [K, K+F). BandsFb() and BandsF() are COUNTS, so the grown slice is at indices
	// [K+baseF, K+liveF) -- and `baseF + (i % span)` reads them as indices. Identical mistake to
	// the one in PickNextWorker's growth spill, in a different function, found the same way: the
	// runtime reporting "ordinary task placed on RESERVED worker 2 (K=3)" AFTER that one was fixed.
	//
	// SILENT AT K=0 AND K=2, LOUD AT K=3, exactly as before. With no reserved band the two spellings
	// coincide; at K=2 with base F=2 it starts at index 2, the first FLOOR worker -- the wrong
	// target for the stated purpose but still a legal one, so nothing complained. At K=3 index 2 is
	// reserved and bulk work lands on an I/O core.
	//
	// One bad expression is a slip; two identical ones in two functions is the accessor names
	// inviting it. BandsF/BandsFb read like indices at the call site and are not, and every future
	// use will have to remember that.
	const size_t k     = BandsK();
	const size_t baseF = BandsFb();
	const size_t liveF = BandsF();
	if (liveF <= baseF || ownerIdx >= workers.size()) return;

	const size_t n    = workers.size();
	const size_t span = liveF - baseF;

	for (size_t i = 0; i < count; ++i) {
		// pop_bottom, because this IS the bottom's owner. The top stays available to thieves the
		// whole time, so this competes with stealing rather than blocking it -- if a thief already
		// took the task, pop_bottom simply comes back empty and there is nothing to hand out.
		auto opt = deques[ownerIdx]->pop_bottom();
		if (!opt) break;
		Task* t = *opt;
		if (!t) break;

		const size_t target = k + baseF + (i % span);
		if (target >= n || !workers[target]) { Requeue(t); continue; }

		loPriInboxes[target]->push(t);
		workers[target]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
		NoteInboxPush(1);
		workers[target]->MarkQueuedWork();
		// Force, because these indices are inside the grown floor and NotifyWorker's floor-skip
		// would otherwise drop the wake on the assumption that a floor worker is already looking.
		// It usually is -- growth just woke it -- but "usually" is how a task strands in an inbox
		// nobody else may drain.
		workers[target]->NotifyWorker(/*force*/ true);
	}
}

void TaskScheduler::ForceAwakeFloorToBase() noexcept {
	(void)BandsSetF(BandsFb(), instance ? instance->workers.size() : 0);
}
void TaskScheduler::ResetAwakeFloorPeak() noexcept {
	g_awakeFloorPeak.store(BandsF(), std::memory_order_relaxed);
}

// Hold a grown floor at least this long. Long enough that the gaps inside a wave do not shed it,
// short enough that the floor is back at base well before a human-scale idle period.
// ---- HOW LONG A GROWN FLOOR IS HELD BEFORE IT MAY SHED ---------------------------------------
//
// WAS A CONSTANT AT 6 ms, "longer than one heavy body" -- which is the right scale for the case it
// was written for (protect a live submission wave) and the wrong one for a caller that produces
// work on a CADENCE. A 16 ms frame loop expires this between every frame and re-pays the ramp on
// each one; a batch job wants seconds. No single value serves both, so it is a knob, for the same
// reason MinItersPerWorker cannot serve trivial and heavy at once.
//
// SIZE IT TO THE GAP YOU WANT TO HOLD THROUGH, slightly longer -- ~20 ms for a 60 Hz frame loop.
// NOT arbitrarily large: the hold is refreshed by every growth, so a value longer than the interval
// between waves means the floor never sheds at all and the pool is permanently NoSleep, which is
// measured at ~3.5% stolen from the main thread in Game01 and is the thing the awake floor exists
// to avoid paying when idle.
static std::atomic<long long> g_floorHoldNs{ 6'000'000 };   // 6 ms, the historical default
void TaskScheduler::SetFloorHoldMs(unsigned ms) noexcept {
	g_floorHoldNs.store((long long)ms * 1'000'000LL, std::memory_order_relaxed);
}
unsigned TaskScheduler::GetFloorHoldMs() noexcept {
	return (unsigned)(g_floorHoldNs.load(std::memory_order_relaxed) / 1'000'000LL);
}

// SHED IN ONE STEP, BACK TO THE BASE. Called from the IDLE path, which is the mirror of growth
// being called from the PUSH path: the pusher is the only party awake during a burst, and an
// overflow worker spinning with nothing to steal is the only party that can see the burst is over.
// A task-completion controller sees neither -- that is what left the floor at 8.
//
// Returns true if it shed, purely so the caller can log it.
// TEMP DIAG: why does the collapse not fire? Counted at every exit so the answer is a breakdown
// rather than a theory. REMOVE once the shed is fixed.
std::atomic<unsigned long long> g_cCalls{0}, g_cNoGrow{0}, g_cHeld{0}, g_cCas{0}, g_cDone{0};
void TaskScheduler::GetFloorCollapseStats(unsigned long long& calls, unsigned long long& noGrow,
                                          unsigned long long& held, unsigned long long& cas,
                                          unsigned long long& done) noexcept {
	calls  = g_cCalls.load(std::memory_order_relaxed);
	noGrow = g_cNoGrow.load(std::memory_order_relaxed);
	held   = g_cHeld.load(std::memory_order_relaxed);
	cas    = g_cCas.load(std::memory_order_relaxed);
	done   = g_cDone.load(std::memory_order_relaxed);
}

bool TaskScheduler::CollapseAwakeFloorToBase() noexcept {
	g_cCalls.fetch_add(1, std::memory_order_relaxed);
	// ONE WORD, BOTH FIELDS. These were two loads of g_bands, so `base` and `cur` could come from
	// different instants -- and this function's entire decision is a comparison between them.
	const uint64_t w0 = BandsWord();
	const size_t base = BandField(w0, kBandFbase);
	const size_t cur  = BandField(w0, kBandF);
	if (cur <= base) { g_cNoGrow.fetch_add(1, std::memory_order_relaxed); return false; }

	// ---- ASK ABOUT THE OVERFLOW RANGE, NOT ABOUT THE POOL ------------------------------------
	//
	// This used to refuse while ANY queue anywhere advertised work, and that is unsheddable for any
	// workload that stays busy: a frame-graph loop always has something advertised somewhere, so
	// the floor grew to 16 on its first few graphs and then could never come back down -- 20,000
	// graphs measured against sixteen spinners, 3.51 -> 6.6 us. The row looked like a spill
	// regression and was really a collapse that could not fire.
	//
	// The right question is narrower: are the workers we would GIVE UP doing anything? Base workers
	// never park, so their queues have no bearing on whether the grown part is still earning its
	// cores. Everything below checks [base, cur) and nothing else.

	// ---- AND ADVERTISED IS NOT ENOUGH, BECAUSE INBOXES ARE NOT ADVERTISED ---------------------
	//
	// Steal hints describe DEQUES. A task spilled into an overflow worker's inbox is invisible to
	// them, so a pool holding a live wave can read `advertised == 0` and shed -- parking the very
	// workers the spill just handed work to, in the middle of 3.3 ms bodies. That is why the burst
	// showed peak 16 but only ~10 participants: it grew, spilled, and then sheared itself, and the
	// remaining pushes went back to the two base workers.
	//
	// The collapse is correct-but-fatal without this check. Every overflow worker's inbox has to be
	// empty too, and only the OVERFLOW range needs checking -- base workers never park, so what is
	// in their inboxes has no bearing on whether the grown part can be given up.
	// NO BUSY-CHECK ON THE OVERFLOW WORKERS. It was here, and it made the floor unsheddable for any
	// pool that stays busy: a frame-graph loop keeps every promoted worker holding stolen nodes, so
	// the floor grew to 16 on its first graphs and stayed there for all 20,000 -- 3.51 -> 6.7 us,
	// which reads as a spill regression and is really sixteen spinners the collapse could not give up.
	//
	// IT WAS ALSO UNNECESSARY. Collapsing lowers a NUMBER; it does not park anybody. A worker parks
	// through its own idle path, and that path already refuses while its own inbox is non-empty --
	// term for term the same predicate the wake protocol uses. So a promoted worker still holding
	// spilled work simply keeps running and parks when it is genuinely done, floor or no floor.
	//
	// The hold since the last GROWTH is what protects a live wave, and it is the honest test: while a
	// wave is still being submitted the floor keeps being raised, so the hold keeps being pushed out.
	// When submissions stop, the hold expires and the floor goes back. "Nobody is growing me any more"
	// is a statement about the wave; "everyone is idle" is a statement about the whole pool, and the
	// pool is not what is being given up.

	const long long now = MonotonicNs();
	if (now - g_lastFloorGrowNs.load(std::memory_order_relaxed) < g_floorHoldNs.load(std::memory_order_relaxed)) { g_cHeld.fetch_add(1, std::memory_order_relaxed); return false; }

	// Straight to base rather than one step: see the base/current comment. Stepping down cannot
	// keep up with a workload that changes in tens of microseconds, and there is nothing to
	// discover on the way down -- the base is a stated policy, not an estimate.
	// CAS ON THE PACKED WORD: succeeds only if F is still `cur`, and preserves whatever K did
	// meanwhile. The old bare CAS on a standalone F could win while K moved underneath it, which
	// is the torn pair this packing exists to make impossible.
	const bool ok = BandsCasF(cur, base);
	(ok ? g_cDone : g_cCas).fetch_add(1, std::memory_order_relaxed);
	return ok;
}

// ---- AWAKE-FLOOR CONTROLLER STATE ------------------------------------------------------------
// Rates are asymmetric by design -- see MaybeAdjustAwakeFloor in the header for why.
static std::atomic<unsigned>  g_wakeMisses{ 0 };      // pushes that landed on a SLEEPING worker
static std::atomic<unsigned>  g_floorPushes{ 0 };     // placement-chosen pushes -- the denominator
static std::atomic<long long> g_floorMissWindowNs{ 0 };
static std::atomic<long long> g_floorWindowNs{ 0 };   // demote observation window
static std::atomic<long long> g_lastFloorUpNs{ 0 };
static std::atomic<long long> g_lastFloorDownNs{ 0 };
static std::atomic<unsigned>  g_quietWindows{ 0 };

static constexpr long long kFloorMissWindowNs   = 1'000'000;    // 1 ms
static constexpr long long kFloorWindowNs  = 10'000'000;   // 10 ms
static constexpr long long kFloorUpNs      = 200'000;      // promote at most every 0.2 ms
static constexpr long long kFloorDownNs    = 20'000'000;   // demote at most every 20 ms
static constexpr unsigned  kQuietToDemote  = 3;
static constexpr unsigned  kMissRatio      = 4;    // promote above 1 miss per 4 pushes (25%)
static constexpr unsigned  kMinPushSamples = 8;    // enough to exclude noise, not enough to exclude a burst
static constexpr unsigned  kQuietBusyPct   = 20;   // marginal worker under 20% busy = sheddable

// ---- LANE STRAND ACCOUNTING: WOULD A SHARED MPMC LANE WIN ANYTHING? ---------------------------
//
// Called from the dispatch site in Worker(), at the instant a reserved worker takes a task and
// leaves a non-empty lane inbox behind it. See the comment there for what the two numbers mean and
// why the split is the whole question.
//
// THE SCAN ASKS `busy`, NOT `awake`, and that distinction is the point. An awake worker with an
// empty queue is available; an awake worker inside a body is not, and it is the second kind that a
// pull queue cannot help with either. Reading awake here would report a prize that does not exist.
//
// SLEEPING PEERS COUNT AS IDLE, deliberately. A pull queue's consumer set is whoever is asking, and
// a parked reserved worker would be woken by the same signal that publishes the work -- so for the
// purpose of "could this backlog have gone somewhere else", a sleeper is somewhere else. That makes
// this an UPPER bound on the MPMC's win, which is the right direction for a number being used to
// decide whether to build one.
// ---- TWO PEER SETS, BECAUSE THEY PRICE TWO DIFFERENT DESIGNS ----------------------------------
//
//   [0, K)     -- what the CURRENT lane could reach. The spill's search space, and the ceiling on
//                 what any producer-side fix can ever recover.
//   [0, K+F)   -- what a SHARED PULL LANE could reach: reserved plus the awake floor, i.e. every
//                 worker that can take a completion without a kernel wake.
//
// THE PARKABLE BAND IS EXCLUDED FROM BOTH, and that is what keeps the second number honest. A
// scan over the whole pool would report ~100% forever, because 27 parked workers are all "not
// busy" -- and handing a latency-critical completion to one costs the ~3 us wake that the entire
// lane exists to avoid. An asleep worker is not a consumer, it is a bill.
//
// THE LIVE FLOOR, NOT THE BASE. Growth wakes workers into [K, K+F) and those are genuinely
// available while they are up, so the set a pull queue could draw on is whatever is awake at that
// instant. It moves under the measurement; that is a property of the pool, not an error.
static std::atomic<unsigned long long> g_laneStrandEvents{ 0 };
static std::atomic<unsigned long long> g_laneStrandIdleK{ 0 };
static std::atomic<unsigned long long> g_laneStrandIdleKF{ 0 };

void TaskScheduler::NoteLaneStrand(size_t ownerIndex) noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return;
	g_laneStrandEvents.fetch_add(1, std::memory_order_relaxed);

	const size_t n = inst->workers.size();
	const Bands  b = GetBands();
	const size_t kEnd  = (b.k < n) ? b.k : n;
	const size_t kfEnd = ((b.k + b.f) < n) ? (b.k + b.f) : n;

	// ONE SWEEP, TWO ANSWERS. [0, kEnd) is a prefix of [0, kfEnd), so a hit below kEnd counts for
	// both and the loop can stop at the first idle worker it finds in each range -- but it must not
	// stop at the first hit overall, because a hit in the floor says nothing about K.
	bool idleInK = false, idleInKF = false;
	for (size_t q = 0; q < kfEnd; ++q) {
		if (q == ownerIndex || !inst->workers[q]) continue;
		if (inst->workers[q]->busy.load(std::memory_order_relaxed)) continue;
		idleInKF = true;
		if (q < kEnd) { idleInK = true; break; }   // a K hit implies the KF hit; nothing left to learn
	}
	if (idleInK)  g_laneStrandIdleK.fetch_add(1, std::memory_order_relaxed);
	if (idleInKF) g_laneStrandIdleKF.fetch_add(1, std::memory_order_relaxed);
}
unsigned long long TaskScheduler::GetLaneStrandCount() noexcept {
	return g_laneStrandEvents.load(std::memory_order_relaxed);
}
unsigned long long TaskScheduler::GetLaneStrandIdlePeerCount() noexcept {
	return g_laneStrandIdleK.load(std::memory_order_relaxed);
}
unsigned long long TaskScheduler::GetLaneStrandIdleWideCount() noexcept {
	return g_laneStrandIdleKF.load(std::memory_order_relaxed);
}

// The floor-lane experiment switch. Relaxed on both sides: it is set before Init in every intended
// use, and a push that reads a stale value routes one task by the other policy -- both destinations
// are legal, both are drained, nothing is lost. Making it seq_cst would put a fence on the push path
// to protect a flag nobody flips at runtime.
// Which breadth the LAZY SPLITTER asks for. Default false = CorePref::Default, the shipped
// behaviour; see the argument at the CreateTask in RunLazyRange. Exists because that argument is a
// prediction nobody has A/B'd on a quiet machine, and a cross-run comparison cannot settle it -- the
// crossover's serial baselines have been seen to move 2x between runs.
static std::atomic<bool> g_parallelSplitWide{ false };
void TaskScheduler::SetParallelSplitWide(bool on) noexcept {
	g_parallelSplitWide.store(on, std::memory_order_relaxed);
}
bool TaskScheduler::ParallelSplitWide() noexcept {
	return g_parallelSplitWide.load(std::memory_order_relaxed);
}

static std::atomic<bool> g_hiPriFloorLane{ false };
void TaskScheduler::SetHiPriFloorLane(bool on) noexcept {
	g_hiPriFloorLane.store(on, std::memory_order_relaxed);
}
bool TaskScheduler::HiPriFloorLane() noexcept {
	return g_hiPriFloorLane.load(std::memory_order_relaxed);
}

static std::atomic<unsigned long long> g_hiPriSpills{ 0 };
unsigned long long TaskScheduler::GetHiPriSpillCount() noexcept {
	return g_hiPriSpills.load(std::memory_order_relaxed);
}

// See the declaration for why this exists again after the old lane wake was removed.
void TaskScheduler::NotifyLaneHelper(size_t excludeQ) noexcept {
	const size_t n = workers.size();
	if (n <= 1) return;
	// Its own cursor, so a burst of stagings spreads over the pool instead of hammering one worker.
	const size_t start = nextLaneWake.fetch_add(1, std::memory_order_relaxed);
	for (size_t i = 0; i < n; ++i) {
		const size_t q = (start + i) % n;
		if (q == excludeQ || !workers[q]) continue;
		// BUSY IS NOT HELP. A worker inside a task body is "awake" but will not reach the idle
		// path until its body ends, so it can neither read the advertisement nor steal. Skipping
		// rather than returning here is the difference between waking somebody and being fooled
		// by a neighbour that is just as stuck as the worker that staged the work.
		if (workers[q]->busy.load(std::memory_order_acquire)) continue;
		// AWAKE AND IDLE: no wake needed. It reaches the idle path on its own and reads the
		// advertisement there, so a notify would be pure cost.
		if (workers[q]->GetWorkerState() != 2 /* WS_SLEEPING */) return;
		workers[q]->NotifyWorker();
		return;
	}
}

static std::atomic<unsigned long long> g_laneProbes{ 0 };
static std::atomic<unsigned long long> g_laneSteals{ 0 };
void TaskScheduler::NoteLaneProbe(bool hit) noexcept {
	g_laneProbes.fetch_add(1, std::memory_order_relaxed);
	if (hit) g_laneSteals.fetch_add(1, std::memory_order_relaxed);
}
unsigned long long TaskScheduler::GetLaneProbeCount() noexcept {
	return g_laneProbes.load(std::memory_order_relaxed);
}
unsigned long long TaskScheduler::GetLaneStealCount() noexcept {
	return g_laneSteals.load(std::memory_order_relaxed);
}

// g_hiPriStaged and its two accessors were here. See the header: the lane deque they measured is
// gone, so the counter had no writer left.

// THE SCAN, SHARED BY BOTH ENTRY POINTS. Returns a worker better placed to run a lane task than
// `from`, or `from` itself when nobody is. Says nothing about whether `from` is in trouble -- that
// judgement belongs to the caller, and the two callers judge it differently:
//
//   HiPriSpillTarget   (producer, at the push)      -- only when the owner is already inside a body
//   SpillLaneRemainder (owner, at dispatch)         -- unconditionally, because the owner is about
//                                                      to enter a body and knows it
//
// Folding the "is `from` busy" test in here is what made the second caller impossible to write: at
// dispatch the owner is NOT yet busy, so a shared gate would decline every time, on the one path
// where the answer is always yes.
//
// A MEMBER rather than a file-static because it reads `workers`, which is private.

// See the declaration for why the PRODUCER has to make this call and why the overflow goes to
// another inbox rather than into a deque.
size_t TaskScheduler::HiPriSpillTarget(size_t chosen) noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return chosen;
	const size_t n = inst->workers.size();
	if (chosen >= n || !inst->workers[chosen]) return chosen;

	// ---- THE FAST PATH IS ONE ACQUIRE LOAD, AND IT ASKS ABOUT THE OWNER, NOT THE QUEUE --------
	//
	// If the owner is between tasks it is in Worker(), one pass away from popping this lane. That
	// is the healthy case and it costs a single load: no scan, no redirect, no second worker's
	// cache touched for a task that was about to run anyway.
	//
	// THIS FIRST ASKED WHETHER THE INBOX WAS EMPTY, and an empty inbox is not the healthy case --
	// it is half of the WORST one. The moment a long body starts, the owner has just POPPED its
	// lane, so the inbox is empty and the owner is unreachable. The very first task queued behind
	// that body therefore saw "empty, nothing to worry about" and stranded, every run, while the
	// seven behind it were rescued. Measured exactly that way: 7 of 8.
	//
	// `busy` is the state that matters, and it means "inside a task body" -- the same flag Join's
	// quiescence loop reads to decide a worker is mid-task.
	if (!inst->workers[chosen]->busy.load(std::memory_order_acquire)) return chosen;

	// ---- A SCAN BOUNDED BY K, BECAUSE K IS THE ONLY PLACE A COMPLETION MAY GO ------------------
	//
	// A busy owner is not pathological on its own -- workers are busy most of the time in a loaded
	// pool -- so this runs often, and an unbounded sweep would put O(workers) atomic loads on the
	// hiPri push path at exactly the moment the pool is under pressure. The bound is no longer a
	// probe budget: it is K itself, which is 1, 2 or 4. The whole scan is at most three acquire
	// loads and it cannot leave the reserved band, so there is nothing left to budget.
	//
	// WHY SPILL OFF A MERELY-BUSY OWNER AT ALL, rather than waiting for a body to prove it is long:
	// nothing can know a body's length before running it. The choice is between an unknown wait and
	// a worker that is idle NOW, and for the lane -- whose entire purpose is that something is
	// waiting on this task's latency -- idle-now wins. The cost when the body turns out to be short
	// is one colder cache line; the cost of guessing the other way is unbounded.

	const Bands b = GetBands();

	// ---- WHAT DISQUALIFIES A TARGET IS `busy`, NOT A NON-EMPTY INBOX --------------------------
	//
	// This first required the target's lane to be EMPTY, on the reasoning that a worker which
	// already has a lane task is "no better off". That is wrong, and measurably: it rate-limits the
	// spill to one task per worker, so on a 4-worker pool 3 of 8 moved and the other 5 stranded
	// exactly as if the mechanism were not there.
	//
	// The comparison is not "queued vs not queued", it is queued behind WHOM. A task queued on an
	// idle worker is drained on that worker's next pass, in order, in microseconds. A task queued
	// behind a body that has not returned is drained when that body ends, which is unbounded and is
	// the entire failure. Depth on an idle consumer is not the same object as depth on a stopped
	// one.
	//
	// ROTATING START, so successive spills spread instead of stacking on whichever worker happens
	// to be scanned first. Relaxed: this is a load-spreading hint and two producers racing to the
	// same target costs an ordering, not a correctness property.
	static std::atomic<unsigned> s_spillCursor{ 0 };
	const unsigned start = s_spillCursor.fetch_add(1, std::memory_order_relaxed);

	// ---- THE ONLY PASS: ANOTHER RESERVED WORKER, [0, K) ---------------------------------------
	//
	// THE FLOOR IS NOT A LEGAL SPILL TARGET, and this used to have two more passes that made it
	// one: pass 1 walked the awake floor [K, K+F), pass 2 walked any awake worker in the pool. Both
	// were argued from "handing a running thread a task costs no kernel wake", which is true and is
	// not the question. The question this function is answering is who can START the task, and an
	// awake worker is not that -- it is a worker that will start it whenever its current body
	// returns, which is the exact condition the spill exists to escape. The owner was rejected for
	// being busy and then the task was handed to a DIFFERENT busy-eligible worker.
	//
	// It is the same mistake as steering ordinary work at the floor, one lane over: awake is being
	// used as a proxy for available, and it is only a proxy when the worker is also RESERVED. A
	// floor member runs bulk. A 400 us ParallelFor leaf is exactly what it is there for. Landing a
	// completion behind one is not a rescue.
	//
	// So the lane is closed: [0, K) or the owner, nothing else. If every reserved worker is inside
	// a body then the lane is genuinely saturated, which is a statement about K being too small for
	// the completion rate -- a configuration answer, not something placement can fix by leaking
	// into the pool. K is 1, 2 or 4; nothing here is worth a probe budget.
	//
	// WHAT THIS GIVES UP, stated plainly: at K == 1 there is no other reserved worker, so the spill
	// can only ever return the owner and the mechanism is inert. That is correct rather than
	// unfortunate -- with one reserved core, "the lane is busy" and "the pool is busy" are the same
	// sentence, and the fix is K = 2.
	// ---- A SLEEPING TARGET IS NOT A RESCUE, AND THIS GUARD WAS MISSING -----------------------
	//
	// SKIPPED, NOT PREFERRED, which is the opposite of ordinary placement. Waking a core is a real
	// answer for bulk work -- capacity is the point. Here the task is already queued behind
	// somebody; moving it to a thread that must first be woken changes WHICH microseconds it waits,
	// not how many, and a wake is ~3 us against a lane whose whole p50 is under 2. Leaving it on
	// the owner is no worse and keeps the core asleep.
	//
	// IT WAS DROPPED IN THE K-ONLY REWRITE. The three passes this replaced were floor, then whole
	// pool: the floor pass needed no sleep test because floor members never park, and the pool pass
	// had one. Collapsing them carried the FLOOR pass's shape onto [0, K) -- a band that parks
	// unless SetReservedNeverParks is on. `!busy` is true for a sleeper, so every spill was free to
	// pick one.
	//
	// The K sweep is what surfaced it: p50 tracks provisioning cleanly (3.60 / 1.90 / 1.40 us at
	// K = 2 / 3 / 4) while p99 did NOT follow -- K=4 measured WORSE than K=3, 9.30 against 7.40 --
	// and spills rose with K, 286 / 479 / 545. More reserved workers means more of them idle and
	// therefore parked, so a larger K bought more chances to spill into a kernel wake.
	//
	// Free under `neverpark`: K never sleeps, so this load is always false and the branch predicts.
	if (b.k > 1) {
		for (size_t i = 0; i < b.k; ++i) {
			const size_t q = (start + i) % b.k;
			if (q >= n || q == chosen || !inst->workers[q]) continue;
			if (inst->workers[q]->GetWorkerState() == 2 /* WS_SLEEPING */) continue;
			if (inst->workers[q]->busy.load(std::memory_order_acquire)) continue;
			g_hiPriSpills.fetch_add(1, std::memory_order_relaxed);
			return q;
		}
	}

	// NOBODY IN THE BAND IS BETTER OFF. Every reserved worker is busy, so the task waits either
	// way -- and the owner is the one place it is already cache-warm and correctly placed. Falling
	// back rather than forcing a move is what keeps this from thrashing under sustained load, which
	// is the condition it fires under.
	return chosen;
}

void TaskScheduler::NoteWakeMiss() noexcept {
	g_wakeMisses.fetch_add(1, std::memory_order_relaxed);
}

// Relaxed, and they are diagnostics rather than control inputs -- nothing branches on them, so they
// buy no ordering and must not pretend to. See the header for why a zero AIM count is the important
// reading and not a boring one.
static std::atomic<unsigned> g_yieldAimed{ 0 };    // chosen candidate read WS_YIELD
static std::atomic<unsigned> g_yieldReaimed{ 0 };  // ...and an alternative was found
void     TaskScheduler::NoteYieldAim(bool reaimed) noexcept {
	g_yieldAimed.fetch_add(1, std::memory_order_relaxed);
	if (reaimed) g_yieldReaimed.fetch_add(1, std::memory_order_relaxed);
}
unsigned TaskScheduler::YieldAimCount() noexcept   { return g_yieldAimed.load(std::memory_order_relaxed); }
unsigned TaskScheduler::YieldReaimCount() noexcept { return g_yieldReaimed.load(std::memory_order_relaxed); }
void     TaskScheduler::ResetYieldCounters() noexcept {
	g_yieldAimed.store(0, std::memory_order_relaxed);
	g_yieldReaimed.store(0, std::memory_order_relaxed);
}

// The ceiling on push-side growth. A burst wider than this still serialises above the cap, which is
// deliberate: an unbounded rule turns one 16-wide wave into a permanently 16-awake pool, and the
// demote path then has to claw back what a single burst took. Growth is cheap and reversible up to
// here; past here it stops being either.
// 16, not 8. At 8 the burst row read 7.7x of 16 and the banner said 2 -> 8 -- growth worked and the
// CAP was the ceiling: 16 tasks over 8 workers is 2 waves, so ~8x was the most that number could
// ever have been. The arithmetic, not the mechanism, was the limit.
//
// A cap this high is only safe because the floor now collapses to its base in one step when the
// pool goes idle (CollapseAwakeFloorToBase). Raising the ceiling without that is how a single wave
// leaves the pool permanently 16-wide, which measured worse at idle than never having grown.
// OFF. The splitter advertises its lane with SetParallelHint and that is ENOUGH -- a set hint keeps
// advertisedCount non-zero, which is precisely what stops a worker parking, so the range is found on
// the next scan without a syscall. origin/main never had this wake and its splitter beat the cursor
// at small N.
//
// I ADDED IT ON A BROKEN A/B AND IT COST THE ROW IT WAS MEANT TO FIX. The control was written as
// `kWakeOnRangePublish && ... && SetParallelHint(...)`, so turning the flag off ALSO stopped setting
// the hint -- the "without" arm was not "hint alone", it was "nothing advertised at all". A probe
// then showed ZERO workers touching a published range and the wake looked 4-14x load-bearing.
//
// Measured correctly, hint-only against hint-plus-wake:
//     participants, N=10000    19  vs  13     <- the wake brings FEWER workers
//     light crossover          N=10000 vs N=40000
// A wake per publish competes with the scan it is trying to trigger. Left in place, off, because the
// mechanism is sound for a pool with no floor at all -- and because the next person deserves the
// corrected A/B rather than the story that shipped with it.
static constexpr bool kWakeOnRangePublish = false;

// How wide the ramp may get. 0 = no fixed cap, i.e. the whole pool bar one worker (the clamp in
// NoteFloorCrowding).
//
// WAS 16, AND THAT WAS THE CEILING ON THE RAMP RATHER THAN A SAFETY MARGIN. On a 31-worker pool the
// blocking crossover reported `peakF 16` on every single duration -- growth was pinned at the
// constant, not at what the workload wanted -- and the row ran 10.0 ms against 5.1 ms for the same
// pool with every worker receiving. Half the machine was awake-eligible and never used.
//
// A PERMANENT WIDE FLOOR IS STILL BAD and the cap is not what protects against that: `floor=31` as a
// BASE measures a round trip of 3.3-17 us against 0.6 at base=2, because 31 never-parking workers
// starve the submitter. What makes raising this safe is that growth is TRANSIENT -- it raises the
// live floor only, and CollapseAwakeFloorToBase returns it to base when the wave drains. The cap was
// bounding the wrong thing: the danger is a floor that does not SHED, not a floor that grows.
// WAS A COMPILE-TIME CONSTANT PINNED AT 0. The value and its meaning are unchanged -- 0 is
// unlimited -- but an application could not reach it, so "grow, but not past N" was inexpressible.
// See SetAwakeFloorMax in the header for why a CEILING is a different and far cheaper thing than
// SetAwakeFloor's permanent base, and for why the old wide-floor objection was the spin rather than
// the width.
static std::atomic<size_t> g_floorGrowCap{ 0 };
void   TaskScheduler::SetAwakeFloorMax(size_t n) noexcept { g_floorGrowCap.store(n, std::memory_order_relaxed); }
size_t TaskScheduler::GetAwakeFloorMax() noexcept { return g_floorGrowCap.load(std::memory_order_relaxed); }

// How deep a floor worker's inbox must be before queueing behind it is worth waking a core for.
// 4 sits between the two workloads that have to be told apart: a 6-node frame graph puts ~3 behind
// each base-floor worker and must NOT grow, a 16-task wave puts ~8 and must. Both numbers are
// measured, not assumed -- see the growth gate in PickNextWorker.
static constexpr size_t kDepthToGrow = 4;

// How long a floor worker must ALREADY have been inside its current task before queueing behind it
// is worth waking a core to avoid. Well above any trivial body -- a no-op task is tens of
// nanoseconds and a 6-node frame graph's nodes are single-digit microseconds, so neither can reach
// this no matter how deep their queue gets. A 3.3 ms burst body passes it 20x over.
//
// This is the isolation that depth could not provide: it is a statement about the COST of waiting,
// which is what the decision is actually about, rather than about how many are waiting.
static constexpr long long kLongBodyNs = 200'000;   // 200 us

// Consecutive pushes THIS producer has aimed at a busy floor worker before it starts spilling.
//
// PRODUCER-LOCAL ON PURPOSE. It is a property of one submitter's behaviour, not of the pool, so it
// needs no atomics and no shared line -- it costs an increment on a thread-local int, which is what
// makes it affordable on a path that runs 10 M times a second.
//
// 3 IS THE WHOLE LATENCY GUARANTEE. A serial round trip never has a second push outstanding, so it
// can never reach 3 and always lands on the base floor. A small frame graph gets its first three
// nodes on q0/q1 before anything moves. Only a genuine run of pushes into an unbroken busy stretch
// crosses it.
static constexpr unsigned kStreakToSpill = 2;

// Upper end of the spill window. A wave is bounded; a flood is not, and the ceiling is what tells
// them apart -- see the gate. 64 is comfortably above any submit that is really one wave and far
// below the point where a sustained producer would care about the first few dozen tasks.
// Runtime, not constexpr, so ONE binary can A/B the whole growth controller on the machine that has
// the baseline. `nogrow` on the bench command line sets this to 0 and pins the floor at its base:
// no push-side spill, no completion-side growth, no redistribute. See SetFloorGrowthEnabled.
static std::atomic<unsigned> g_streakSpillMax{ 64 };
#define kStreakSpillMax (g_streakSpillMax.load(std::memory_order_relaxed))

// OFF: measured worse on the one row it was written for -- see the comment at the gate. Left in
// place because the mechanism is correct for a SUSTAINED producer (where `busy` is meaningful by
// the time the second push lands) and only fails for a cold-start wave. Flip to true and re-measure
// if a workload appears whose producer keeps pushing after the pool has spun up.
static constexpr bool kSpillOnStreak = false;

// Reset whenever the target is observed NOT busy, which is what keeps a no-op flood off this path:
// those workers keep finishing, so the run keeps breaking.
static thread_local unsigned t_floorStreak = 0;

// When this producer last pushed. Producer time rather than worker state -- see the gate. Three
// pushes inside kWaveGapNs is a wave, and unlike anything about the pool it is knowable on push
// three, before a single worker has picked anything up.
static thread_local long long t_lastPushNs = 0;

// Splits this thread has published. Paces the steal wake: first publish always, then one in four.
static thread_local unsigned t_splitsPublished = 0;

// Pushes since the last clock read. Lets a settled producer skip the read entirely -- see the gate.
static thread_local unsigned t_pushesSinceClock = 0;

// Inter-push gap that still counts as "the same submit". Far longer than the ~0.5 us it takes to
// build and push a task, and far shorter than a round trip through a worker -- which is exactly the
// separation that keeps a serial round trip out: it pushes once, then blocks until the task is done.
static constexpr long long kWaveGapNs = 50'000;   // 50 us

void TaskScheduler::SetFloorGrowthEnabled(bool on) noexcept {
	g_streakSpillMax.store(on ? 64u : 0u, std::memory_order_relaxed);
}
bool TaskScheduler::GetFloorGrowthEnabled() noexcept {
	return g_streakSpillMax.load(std::memory_order_relaxed) != 0u;
}

void TaskScheduler::NoteFloorCrowding(size_t submitted) noexcept {
	if (!GetFloorGrowthEnabled()) return;
	TaskScheduler* inst = instance;
	if (!inst) return;
	const size_t n = inst->workers.size();
	if (n < 2) return;

	// ---- ONE SNAPSHOT FOR THE WHOLE DECISION ------------------------------------------------
	//
	// This took three separate loads of g_bands -- GetAwakeFloor() for F here, GetHotWorkers() for
	// K below, BandsFb() for Fbase at the wave cap -- and then compared them against each other as
	// though they described one instant. They do not have to: this runs on the PUSH path, which is
	// the one path a K change is most likely to race, and the result of mixing them is a cap
	// computed from a new K against an F read before it. That is how the floor gets a target that
	// was never a legal band.
	const uint64_t bw    = BandsWord();
	const size_t   k     = BandField(bw, kBandF);       // live F -- the value being grown
	const size_t   kNow  = BandField(bw, kBandK);       // live K -- where the floor starts
	const size_t   fbase = BandField(bw, kBandFbase);   // policy base -- the wave cap's floor
	// ---- K + F <= N. A GROW THAT WOULD BREAK IT IS REFUSED, NOT CLAMPED LATE ------------------
	//
	// The bands are [0,K) reserved, [K,K+F) floor, [K+F,N) parkable. F is counted FROM K, so a cap
	// that only knows about N lets the floor run off the end of the pool: with K=2 and a cap of n-1
	// on 31 workers the band is [2,32) and the PARKABLE BAND IS EMPTY -- every worker is floor,
	// nothing can ever park, and the collapse gate (which asks for an index at or above the floor)
	// has nobody left to fire it.
	//
	// THIS WAS REACHABLE AS OF TODAY'S CAP LIFT and was not before: the old cap of 16 kept K+F under
	// N for any sane K by accident, so lifting the cap to n-1 removed the only thing enforcing the
	// invariant. The cap was doing two jobs and only one of them was the one it was named for.
	//
	// Minus one more so a parkable worker always exists -- the pool must retain somewhere to put a
	// worker that has genuinely run out of work, or "parkable" is a band with no members.
	if (n <= kNow + 1) return;              // no room for a floor at all: refuse rather than wrap
	// ---- NO "SEVERAL DEQUES MUST HAVE WORK" GATE HERE, AND IT WAS TRIED --------------------
	//
	// The idea was Grok's and it is right about the DISEASE: `submitted` counts things pushed, so
	// one long body in one owner's inbox could slide F over twenty idle cores, which is how
	// burst/dflt produced `PEAK 11, participants 8`. The proposed cure was to require two or more
	// steal hints -- "this queue has work a THIEF may take" -- before widening past the base.
	//
	// IT COST 2.7x ON THE ROW IT WAS MEANT TO FIX. burst/dflt went 9.93 ms -> 26.54 ms (5.3x ->
	// 2.0x of 16) with participants collapsing from 8 to 2, because THIS FUNCTION IS CALLED AT PUSH
	// TIME. The tasks are still in inboxes; nothing has been staged to a deque yet; the steal hints
	// advertise DEQUE work. So the gate reads "no wave" at exactly the moment a wave is arriving,
	// and growth only becomes legal after the owners have drained -- which is far too late to widen
	// for the burst that is already in flight.
	//
	// THE HINTS ARE STRUCTURALLY THE WRONG INSTRUMENT HERE, not merely early, and that is the part
	// worth remembering. THE STEAL HINTS ARE A STEAL SCHEDULER: a bit means "a thief may take work
	// from this deque". F IS A PARK POLICY: it means "this many cores stay off WaitOnAddress".
	// Those two questions agree most of the time and DIVERGE EXACTLY ON THE BURST THE FLOOR EXISTS
	// FOR -- 16 heavy tasks is 16 inbox items and ~0 hint bits, because nothing is stealable until
	// an owner drains and stages. Gating a park policy on a steal signal therefore says "do not
	// recruit until the wave is already in flight", which is the opposite of what recruitment is.
	//
	// GROW EXISTS FOR THE MOMENT BEFORE ANYTHING IS STEALABLE. So a replacement signal has to be
	// PUSH-VISIBLE (Jay's list): queued / inbox depth, a long body, or the target already being
	// busy. Not hint bits.
	//
	// SHED IS THE OTHER DIRECTION AND MAY USE THEM. By collapse time the wave has either staged or
	// finished, so hints and inboxes read together are a fair question there -- which is what
	// CollapseAwakeFloorToBase already does. The asymmetry is the design, not an oversight.
	//
	// Until such a signal exists, the ceiling below is what bounds the damage, and it bounds it by
	// WIDTH rather than by refusing to grow at all.

	// ---- THE CEILING: A BUDGET, NOT A TARGET -------------------------------------------------
	//
	// NOT UNLIMITED BY DEFAULT, and that is a deliberate reversal. Max peak is NoSleep for the
	// length of the grow-hold: every parkable worker becomes a spinner until the collapse wins --
	// leftover F on the next latency row, N pause loops, the submitter and the waiter fighting
	// elevated cores, and a collapse that has to walk the whole band back down. Jay: "Base 2 is the
	// product. Peak is a budget. Max is a stress row, not the policy."
	//
	// hw/2 AS THE DEFAULT, floored at fbase so policy is never undercut. Half the machine is what a
	// burst may spend without handing the box to the pool.
	//
	// n - 2, NOT n - 1: leave a logical CPU out so the application thread is not sharing a fully
	// packed box with the floor. The entire latency argument for a floor is that a push lands on a
	// worker that is ALREADY RUNNING, and that stops being true if the submitter cannot get a core.
	//
	// WIDE IS THE OTHER LEVER AND STAYS SEPARATE. Wide wakes the crowd once for one wave, keeps F
	// at base, and everyone parks after -- burst/wide reaches 31 participants at PEAK 2. Growth
	// keeps cores hot for the NEXT push, which is only worth it if another wave is coming and the
	// work is stealable. Fork-join reaching peak 30 is a case for ASKING for width, not for
	// teaching every burst to become NoSleep for 6 ms.
	const size_t structural = (n >= kNow + 2) ? (n - kNow - 2) : 0;
	if (structural == 0) return;
	// Fmax = clamp(n - 2, Fbase, 16)
	//
	// n/2 WAS THE FIRST ATTEMPT AND IT IS WRONG AT BOTH ENDS, which is the argument for this shape:
	// it OVER-BINDS exactly where a burst needs cores (8-16 threads -> a cap of 4-8) and does
	// NOTHING where the behaviour was actually measured (31 threads -> 15, above an observed peak of
	// 11-13). A rule that only binds on the machines you did not test is not a policy.
	//
	// n - 2 keeps one or two logical CPUs outside the live floor so the application thread is not
	// sharing a packed box with it. THE ABSOLUTE 16 is the other half: without it a 64-thread box
	// grows a 62-core pause-loop, and "peak is a budget" stops meaning anything at scale. Sixteen
	// sits above every peak observed here (11-13 on a 31-wide pool) so it does not bind on the
	// shape that was measured, and caps the shapes that were not.
	//
	// Floored at fbase so policy is never undercut, then clamped to the structural limit.
	const size_t userCap = GetAwakeFloorMax();
	size_t cap;
	if (userCap) cap = userCap;
	else {
		cap = (n >= 2) ? (n - 2) : 0;
		if (cap < fbase) cap = fbase;
		if (cap > 16)    cap = 16;
	}
	if (cap > structural) cap = structural;

	// THE EARLY-OUT IS THE STEADY-STATE COST: two relaxed loads once the floor has reached the cap.
	// That matters because this sits on the push path, and in a throughput workload the floor is
	// crowded on nearly every push -- the gate has to be cheap when it is saying no.
	if (k >= cap) return;

	// NOT GATED ON WAKE MISSES, and that was tried. NoteWakeMiss fires whenever a push lands on a
	// sleeping worker, which sounds like "the floor was too small" but is not: the PARKABLE band is
	// supposed to be asleep and woken on demand, so a healthy pool misses constantly. Gating growth
	// on it changed nothing measurable. If a crowding signal is wanted here it has to describe the
	// COMPUTE backlog -- deque depth, long bodies on q >= K -- not the idle policy working.

	// need vs live. `submitted` is this submit; the floor is the live capacity. One crowded Push
	// moves the floor by one, so no single task can jump it; a PushBatch of N moves it by up to N,
	// because a batch is the one caller that KNOWS a wave arrived at once.
	// ---- CAP THE PEAK AT WHAT THE WAVE ACTUALLY NEEDS ----------------------------------------
	//
	// `k + submitted` compounds: each crowded push adds the whole batch again, so a 16-task burst
	// walked the floor to 28-29 on a 31-worker pool. Sixteen tasks cannot use twenty-nine workers,
	// and the surplus is pure cost -- every one of them is a never-parking spinner that then has to
	// be shed again.
	//
	// The ceiling is the WAVE, not the accumulated request: `waiting + Fbase` -- enough workers for
	// the outstanding items plus the floor that was there before it. Still clamped by `cap`
	// (N - K - 1), so K + F <= N holds and one worker stays parkable.
	size_t want = k + (submitted ? submitted : 1);
	const size_t waveCap = (submitted ? submitted : 1) + fbase;
	if (want > waveCap) want = waveCap;
	if (want > cap)     want = cap;
	// THIS FUNCTION ONLY EVER GROWS. The wave cap is a ceiling on the ASK, not a new target: with
	// `submitted == 1` and F already at 5 it computes 3, and letting that through turns the grow
	// path into a shed path -- fighting CollapseAwakeFloorToBase for the same field and shrinking a
	// floor that a live wave is still using. Shedding has one owner and it is the collapse.
	if (want < k) return;

	// RAISE THE CURRENT FLOOR ONLY -- never the base. Growth is a response to a wave; the base is
	// policy, and a wave must not be able to rewrite it. This is the half of the split that keeps
	// the collapse honest: without it there is nothing to collapse back TO.
	const size_t prev = BandsSetF(want, n);

	// ---- THE HOLD MEANS "SINCE THE FLOOR GREW", NOT "SINCE SOMEBODY ASKED" --------------------
	//
	// This stamped unconditionally, so every call pushed the collapse's 6 ms hold out -- including
	// calls that changed nothing. MEASURED over one bench run: 5,543,329 collapse attempts, of which
	// 5,541,706 (99.97%) were refused by the hold and only 74 ever shed. The floor was above base for
	// all but 1,470 of those attempts. A hold that is re-armed by the act of asking is not a hold,
	// it is a ratchet with a timer on it.
	if (want > prev)
		g_lastFloorGrowNs.store(MonotonicNs(), std::memory_order_relaxed);

	// PEAK, BECAUSE THE CURRENT VALUE IS UNOBSERVABLE BY THE TIME ANYONE ASKS. The collapse fires
	// from an idle overflow worker the moment the wave drains, so a caller that measures a wave and
	// then reads GetAwakeFloor() reads the BASE -- it has already shed. That produced a burst line
	// claiming `1 -> 1` next to a 7.02 ms wall time, two numbers that cannot both be true: sixteen
	// 3.28 ms tasks on one worker is ~50 ms, and 7.02 ms is two waves. The banner was measuring the
	// shed, not the growth. A high-water mark is the only honest way to report a transient.
	for (size_t peak = g_awakeFloorPeak.load(std::memory_order_relaxed); want > peak; )
		if (g_awakeFloorPeak.compare_exchange_weak(peak, want, std::memory_order_relaxed)) break;

	// A promotion must WAKE what it promoted, or it is a number change and nothing else -- the bug
	// that made the dump read `awake bits = 2 (floor = 6)`.
	if (want <= prev) return;
	// ---- THE FLOOR IS [K, K+F), SO THE WORKERS THIS JUST PROMOTED ARE [K+prev, K+want) --------
	//
	// This loop woke [prev, want) -- floor INDICES used as absolute worker indices, a leftover from
	// the [0, F) layout. It is correct only at K == 0, which is why it survived: the library default
	// is K == 0 and so is the marl harness. At K == 2 it woke workers 2 and 3 -- already on the
	// floor, already awake, so the notify was a no-op -- and never woke the two it actually added at
	// the top of the band. Growth then reported a number that went up while the workers it promoted
	// stayed parked, which is the same shape as the `awake bits = 2 (floor = 6)` bug this loop was
	// added to fix, reintroduced by the band split rather than by the original mistake.
	// FLOOR BOUND -> GetFloorBase(). These are floor indices being turned into worker indices, so
	// they follow the floor's base, not whoever is serving the lane this instant.
	const size_t kResv = GetBands().k;
	for (size_t i = kResv + prev; i < kResv + want && i < n; ++i)
		if (Thread* w = inst->workers[i]) w->NotifyWorker(/*force*/ true);
}

// Every placement-chosen push, whether it hit a sleeping worker or not. The DENOMINATOR.
//
// WHY A RATIO AND NOT A COUNT. The rule inherited from the K-hot controller was "one miss in a 1 ms
// window promotes", and that was sane there because its miss signal was a LANE miss -- a hiPri task
// arriving with no hot worker free, which on a 60 Hz frame loop happens a handful of times a
// second. The same rule against THIS signal is not: a placement-chosen push is a candidate event
// millions of times a second in throughput/1p, so "one miss in 1 ms" is essentially always true and
// the floor ratchets to whatever the run length allows.
//
// A FRACTION IS SCALE-FREE. "More than 1 in 20 pushes hit a sleeping worker" means the same thing at
// 60 Hz and at 10 M/s; "one miss" does not.
void TaskScheduler::NotePush() noexcept {
	g_floorPushes.fetch_add(1, std::memory_order_relaxed);
}

static std::atomic<unsigned long long> g_wakeCalls{ 0 };
void TaskScheduler::NoteWakeCall() noexcept { g_wakeCalls.fetch_add(1, std::memory_order_relaxed); }
// TEMP DIAG -- parks that happened while the LIVE floor claimed the worker. REMOVE.
static std::atomic<unsigned long long> g_floorParks{ 0 };
void TaskScheduler::NoteFloorPark() noexcept { g_floorParks.fetch_add(1, std::memory_order_relaxed); }
unsigned long long TaskScheduler::GetFloorParkCount() noexcept { return g_floorParks.load(std::memory_order_relaxed); }
void TaskScheduler::GetStaleHintReport(unsigned& advertised, unsigned& stale) noexcept {
	advertised = 0; stale = 0;
	TaskScheduler* inst = instance;
	if (!inst) return;
	const size_t nw = inst->workers.size();
	for (size_t q = 0; q < nw && q < 64; ++q) {
		// BOTH WORDS. The first version of this read only the BACKLOG word and reported
		// "advertised=0 stale=0" on a run whose parallel word held a leaked bit -- an instrument
		// that looked at the wrong half and cleared the real culprit of suspicion.
		if (!(inst->StealHintWord(0) & (1ull << q))) continue;
		++advertised;
		if (inst->WorkerQueuesEmpty(q)) ++stale;
	}
}
unsigned long long TaskScheduler::GetWakeCount() noexcept { return g_wakeCalls.load(std::memory_order_relaxed); }
void TaskScheduler::ResetWakeCount() noexcept { g_wakeCalls.store(0, std::memory_order_relaxed); }

void TaskScheduler::MaybeAdjustAwakeFloor() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return;

	const size_t n = inst->workers.size();
	if (n < 2) return;                       // nothing to move between

	const size_t lo = 1;                     // never fully park: the last worker is the landing site
	const size_t hi = n;
	const size_t k  = GetAwakeFloor();
	const long long now = MonotonicNs();

	// ---- PROMOTE: somebody had to be woken, so the floor was too small ----------------------
	// The window is rolled first so a stale count from a quiet second cannot fire a promotion now.
	const long long missWs = g_floorMissWindowNs.load(std::memory_order_relaxed);
	if (missWs == 0 || now - missWs >= kFloorMissWindowNs) {
		g_floorMissWindowNs.store(now, std::memory_order_relaxed);
		g_wakeMisses.store(0, std::memory_order_relaxed);
		g_floorPushes.store(0, std::memory_order_relaxed);
	}
	// ---- NO PROMOTION HERE. GROWTH IS THE PUSH PATH'S JOB -- see NoteFloorCrowding ------------
	//
	// A miss-ratio promote used to live here and it was wrong twice over for the row it was meant to
	// serve. First, it cannot RUN during a burst: the floor workers are inside multi-millisecond task
	// bodies and reach no completion, and every other worker is blocked in the kernel, so this
	// function is not called at all until the burst is over. Second, even when it does run the signal
	// is blind to the case -- a push onto an AWAKE floor worker records no miss, so sixteen tasks
	// piling onto two live workers produce a miss ratio of zero while the queue builds.
	//
	// The miss counters are still maintained (NoteWakeMiss/NotePush) because they are the honest
	// measure of "pushes that had to buy a kernel wake", which the bench reports and which is the
	// floor's headline number. They just no longer steer anything.
	(void)hi;

	// ---- DEMOTE: the MARGINAL floor worker has had nothing to do -----------------------------
	// Index k-1 specifically: it is the one that would be given up, and asking about it rather than
	// about the average is what makes shedding possible. An average over a busy floor never falls,
	// so K would ratchet up and stay there for the life of the process.
	const long long ws = g_floorWindowNs.load(std::memory_order_relaxed);
	if (ws == 0) { g_floorWindowNs.store(now, std::memory_order_relaxed); return; }
	if (now - ws < kFloorWindowNs) return;
	g_floorWindowNs.store(now, std::memory_order_relaxed);

	const long long windowNs = now - ws;
	if (windowNs <= 0) return;

	// Drain every floor worker's counters, not just the marginal one -- leaving the others set would
	// let a stale count from several windows ago decide a later decision.
	unsigned  marginalRan = 0;
	long long marginalBusyNs = 0;
	long long minBusyNs   = 0;
	for (size_t i = 0; i < k && i < n; ++i) {
		const unsigned  ran  = inst->workers[i]->tasksRun.exchange(0, std::memory_order_relaxed);
		const long long busy = inst->workers[i]->busyNs.exchange(0, std::memory_order_relaxed);
		if (i == k - 1) marginalRan = ran;
		if (i == k - 1) marginalBusyNs = busy;
		if (i == 0 || busy < minBusyNs) minBusyNs = busy;
	}

	// ---- PROMOTE ON SATURATION: every floor worker is ~fully occupied ------------------------
	//
	// THE SIGNAL THE WAKE COUNTER CANNOT SEE. Work piling onto an ALREADY-AWAKE worker wakes
	// nobody, so no miss is recorded -- which is exactly `burst`: sixteen tasks onto the single
	// floor worker, running one after another at 1.0x of 16, with the wake counter reading zero
	// throughout. Occupancy sees it on the first window.
	//
	// EVERY floor worker (the minimum), not the average. One saturated worker beside an idle one
	// means the floor is big enough and the work is badly spread -- a different problem, and
	// growing the floor would not fix it.
	//
	// 70%, carried over from the K-hot controller unchanged: high enough that a floor worker doing
	// intermittent work does not ratchet K upward, low enough to fire before the queue is deep.
	// AND THIS PROMOTE IS GONE TOO, for the first of the two reasons above: a utilisation window
	// measured at task completion cannot observe a worker that has not completed anything yet. It
	// fires reliably under sustained medium-length work -- the case that was already fine -- and
	// never under the long-task burst that actually needs the floor to grow. Growth is
	// NoteFloorCrowding's job now; what remains here is shedding, which completions CAN see because
	// a quiet worker is precisely one that keeps reaching them.
	// (nothing here -- promotion moved to NoteFloorCrowding on the push path)

	// ---- DEMOTE ON UTILISATION, NOT ON A ZERO COUNT ------------------------------------------
	//
	// The inherited rule was "the marginal worker ran ZERO tasks this window". Under any real load
	// it runs something, so demotion never fired at all -- which is the other half of the 50:1
	// asymmetry that let the floor climb and never come back down. A worker that ran three tasks in
	// ten milliseconds is not busy; it is idle with a rounding error, and a zero-count cannot say so.
	//
	// busyNs is already accumulated for exactly these workers and was previously read only by the
	// promote path. Below kQuietBusyPct of the window, the marginal worker is not earning its core.
	const bool marginalQuiet =
		(marginalRan == 0) || (marginalBusyNs * 100 < windowNs * (long long)kQuietBusyPct);

	// RESET ON ANY ACTIVITY. This is the ratchet rule: keyed off "IS idle", never "became idle".
	if (!marginalQuiet) { g_quietWindows.store(0, std::memory_order_relaxed); return; }

	// ---- AND THE DEMOTE IS GONE TOO. IT ATE THE BASE. ----------------------------------------
	//
	// This called SetAwakeFloor(k - 1), which since the base/current split is the POLICY setter --
	// it writes the base as well as the current floor. So every quiet window walked the configured
	// floor down one step, and `lo` is 1, so a process configured for 2 ended up based at 1 and
	// stayed there. The bench caught it as a banner reading `base 1` under `awake-floor=2`, which
	// is a controller quietly overwriting the configuration it was supposed to be steering within.
	//
	// The rule itself was never the problem and neither was the ratchet guard above it -- the
	// mistake was reaching for the public setter to do an internal move. But there is nothing left
	// for it to do either: shedding a GROWN floor is CollapseAwakeFloorToBase's job, it goes
	// straight to the base in one step, and it fires from the idle path where a burst can actually
	// be seen to be over. Shedding BELOW the base is not shedding, it is policy drift.
	(void)lo;
}

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
// g_hotMin / g_hotMax ARE GONE -- the range lives in the band word, Kmin at bits 32..39 and Kmax at
// 24..31. Two standalone atomics beside the word meant K had a second home, and the worker had to
// call GetHotWorkerRange() to ask a question the word could already answer. See kBandKmin.

// PINS THE RANGE TO [k, k]. "Set K to exactly k" is a request for a FIXED k, and leaving a wider
// range in place would mean the controller quietly moved K away from what was just asked for. So
// this is the static spelling, it is what every existing caller already means by it, and it is why
// no policy flag is needed: SetHotWorkers(k) IS SetHotWorkerRange(k, k).
// ---- EXPERIMENTAL: execution mode ------------------------------------------------------------
// See the declaration for what the mode is and, more importantly, for what it is NOT covered by.



// Mode::FiberOnly and its admission gate were removed in 4.0.2 -- see the note in TaskScheduler.h.
// The gate admitted everything by then, so its three call sites were no-ops; what it once enforced
// (Native must not suspend) is still enforced where it always was, by assignedFiber being null.

// ---- THE FLOOR'S BASE: LIVE K, OR THE RESERVATION CAPACITY? --------------------------------------
//
// false (default) -- the floor is [K, K+M) with LIVE K. Correct only while K never moves.
// true            -- the floor is [Kmax, Kmax+M). The band has a fixed base and cannot slide.
//
// WHY THIS IS THE WHOLE BLOCKER ON ADAPTIVE K AND ADAPTIVE M AT ONCE. M is written as an offset OF K,
// so a K promotion RELOCATES the floor under running workers: every worker's answer to "am I on the
// floor" changes at once, and K and M are separate atomics read at different moments in Worker(),
// NotifyWorker(), PickNextWorker() and CollapseAwakeFloorToBase(), so no reader gets a consistent
// pair. That is not hypothetical -- it is the shape of the off-by-K promotion-wake bug fixed earlier
// today, which was survivable ONLY because K is currently static.
//
// AND IT DOES NOT COST IDLE CORES, which is the objection to reserving at all. Only [0, activeK) is
// excluded from ordinary placement; [activeK, Kmax) stays ORDINARY COMPUTE that parks like any other
// worker. Kmax is a ceiling on how far I/O may ever reach into the pool, not a set of cores held
// empty against the chance that I/O shows up.
//
// NEARLY A NO-OP UNTIL ADAPTIVE K IS ARMED: SetHotWorkers pins min==max, so Kmax == activeK and both
// arms compute the same band. It only diverges once SetHotWorkerRange(min,max) gives K room to move,
// which is exactly the configuration this exists to make safe.
// WHERE THE FLOOR STARTS: LIVE K, READ NOW. One function so that every band decision in the system
// -- placement, notify, park, grow, collapse -- is spelled the same way and cannot drift apart.
// That drift is not hypothetical: the floor has already been [0,F) at one site and [K,K+F) at
// another in the same build, which hung the pool.
//
// A FIXED BASE AT Kmax WAS TRIED HERE AND IS NOT THE DESIGN. It stops the band sliding when K moves,
// but it does so by permanently reserving index space, and the answer to a moving band is to have
// ONE formula read from live atomics plus a rule that the two controllers never move in the same
// window -- not to freeze one of them.
// DECODED THROUGH BandField, NOT BY HAND. This open-coded the old 16-bit layout (K in bits 0..15)
// and was left behind when the word was re-laid out to F/Fbase/K/Kmax at 8 bits each -- so it read
// K = (F | Fbase<<8) = 514, every worker believed it was reserved, all 31 refused ordinary work and
// handed strays to each other, and the pool livelocked with a "RESERVED worker 1 (K=0)" warning.
//
// A SECOND DECODE OF THE SAME WORD IS THE SAME BUG AS A SECOND RECIPE FOR THE BAND. There is one
// field extractor and everything goes through it.
TaskScheduler::Bands TaskScheduler::GetBands() noexcept {
	const uint64_t w = BandsWord();
	Bands b{ BandField(w, kBandK),    BandField(w, kBandF), BandField(w, kBandFbase),
	         BandField(w, kBandKmin), BandField(w, kBandKmax) };

	// ---- THE INVARIANT HOLDS IN RELEASE, NOT ONLY UNDER assert ------------------------------
	//
	// The asserts below are diagnosis. They are not protection, because they compile out in exactly
	// the build where the k=514 decode actually hung the pool -- and this file already states that
	// rule elsewhere: an assert guarding a silent hang is a guard in the one build nobody is
	// watching. So K+F <= N is ENFORCED here, on every read, in every build.
	//
	// CLAMPING RATHER THAN ABORTING because the failure this catches is a band that is merely
	// unusable, and a pool that keeps running with a sane band can still be debugged; one that is
	// livelocked on `q < 514` cannot. The debug asserts fire first and name the real cause.
	//
	// N <= 1 IS LEFT ALONE, deliberately -- see BandsSetF. With fewer than two workers there is
	// nobody to keep a slot parkable for, and forcing F to 0 flips the only worker from never-park
	// to parkable, which hands it the single-worker lost-wakeup path.
	const Bands raw = b;   // what the word ACTUALLY said -- the asserts below judge this, not the clamp
	{
		const size_t nw = instance ? instance->workers.size() : 0;
		if (nw > 1) {
			if (b.k > nw - 1) b.k = nw - 1;              // K alone can never swallow the pool
			if (b.k + b.f > nw) b.f = nw - b.k;          // K + F <= N
		}
		else if (nw == 1) {
			// ---- A ONE-WORKER POOL IS ALL FLOOR: K=0, F=N, NEVER PARKS --------------------
			//
			// The usual rule keeps one worker parkable so the pool always has somewhere to put a
			// thread that has genuinely run out of work. At N=1 that rule inverts: the ONLY worker
			// becomes parkable, and a single worker that parks is the lost-wakeup path -- there is
			// no second thread to notice the work it missed and no thief to drain its inbox.
			//
			// It also used to depend on luck. BandsSetF skips its clamp entirely below N=2, so the
			// worker was saved only by Fbase defaulting to 2, which put q0 inside [0,2) and made it
			// floor by accident. `floor=0` removed that accident and armed the hang.
			//
			// K=0 for the same reason from the other side: a reserved band on a one-worker pool
			// reserves the entire pool, so ordinary work has nowhere to run at all.
			b.k = 0;
			b.f = 1;
		}
	}
#ifndef NDEBUG
	// ---- THE DECODE MUST BE SANE, AND A WRONG ONE IS NOT A SUBTLE BUG -------------------------
	//
	// This function open-coded the old 16-bit layout after the word was re-laid out to 8-bit
	// fields, and returned k = (F | Fbase<<8) = 514. Consequences, none of which look like a
	// decode bug from the outside:
	//   q < k is true for EVERY worker  -> the whole pool believes it is reserved
	//   the notify skip uses that k     -> wakes are dropped
	//   the floor window [514, 514+F)   -> empty, so F membership is nonsense
	// A producer waiting on a slot plus a dropped wake is exactly a hang, at ANY pool size -- not
	// only at N=1. Assert it here rather than diagnosing the symptom three layers away.
	// JUDGED ON `raw`, NOT ON `b`. Asserting the clamped value would be asserting that the clamp
	// ran -- it always does -- and would never fire on the bug it was written for. The clamp keeps
	// Release alive; these say WHY it had to.
	const size_t nw = instance ? instance->workers.size() : 0;
	assert((!nw || raw.k + raw.f <= nw || nw <= 1) && "GetBands: K+F exceeds the pool -- bad decode or a clamp that did not hold");
	assert((!nw || raw.k <= nw) && "GetBands: K exceeds the pool -- bad decode");
	assert((!nw || raw.f <= nw || nw <= 1) && "GetBands: F exceeds the pool -- bad decode");
	assert((!raw.kmax || raw.k <= raw.kmax) && "GetBands: K exceeds Kmax");
	assert((raw.kmax >= raw.kmin || !raw.kmax) && "GetBands: scaling range inverted (Kmax < Kmin)");
#endif
	return b;
}
size_t TaskScheduler::GetFloorBase() noexcept { return GetHotWorkers(); }

unsigned TaskScheduler::GetWorkerParkCount(size_t q) noexcept {
	TaskScheduler* inst = instance;
	if (!inst || q >= inst->workers.size() || !inst->workers[q]) return 0;
	return inst->workers[q]->parkCount.load(std::memory_order_relaxed);
}
void TaskScheduler::ResetWorkerParkCounts() noexcept {
	TaskScheduler* inst = instance;
	if (!inst) return;
	for (auto& w : inst->workers)
		if (w) w->parkCount.store(0, std::memory_order_relaxed);
}

void TaskScheduler::SetHotWorkers(size_t k) {
	BandsSetKmax(k);   // pinned: the cap IS the value...
	BandsSetKmin(k);   // ...and the floor of the range, so max == min == static K
	SetHotWorkersEffective(k);
	// ---- ASKING FOR K IS ASKING FOR THE BAND TO BE AWAKE --------------------------------------
	//
	// K > 0 is already the deliberate opt-in: nobody reserves cores for fun, they do it to get a
	// completion onto a thread that is not buried in a bulk body. Letting that band then PARK throws
	// away most of what the cores were spent on -- the completion pays an OS wake anyway, which is
	// the cost K existed to remove.
	//
	// MEASURED, io_overlap hiPri completion latency on a BUSY pool:
	//     reservation only   p50 5.90  p99 43.00  max 66.90 us
	//     + never-park       p50 2.00  p99  6.30  max 13.00 us
	// Exclusion works either way (300 of 300 ran inside [0,K) in both arms) -- this is purely the
	// wake. 2.9x p50, 6.8x p99.
	//
	// AT K == 0 IT GOES OFF: there is no band, and spinning [0,0) is meaningless. The library
	// default is K = 0, so nobody who did not ask for reservation pays a spinning core.
	//
	// ---- RESERVATION IS NOT SPIN, AND THIS SETTER NO LONGER CONFLATES THEM -------------------
	//
	// This called SetReservedNeverParks(k > 0), so asking for "do not run loPri on q0" silently
	// also bought "burn a core forever". The numbers above are real, but they are the numbers for
	// the I/O lane -- and every other caller was paying the ~35% ordinary-latency tax for a
	// property it never asked for. A caller that wants a reserved core to be able to SLEEP when its
	// lane is empty was, until now, unable to say so through this API: the flag was re-forced on
	// every K change.
	//
	// The two knobs are now orthogonal, which is what the band design says they are:
	//     SetHotWorkers(k)           K only. never-park is left exactly as it was (default false).
	//     SetReservedNeverParks(on)  the spin promise, explicit and opt-in.
	//     SetIoHotLane(k)            the named combination, for the reactor -- see below.
}

// ---- THE ONE CALLER THAT GENUINELY WANTS BOTH ------------------------------------------------
//
// The I/O lane is the case the never-park measurement came from: a completion landing on a
// reserved worker that is parked pays the OS wake the reservation was bought to avoid, so for the
// reactor the two knobs really are one decision. Naming that combination keeps it available
// without making every other caller pay for it.
//
// Games and the bench call SetHotWorkers and leave the flag false, so an empty reserved core parks
// like anything else.
void TaskScheduler::SetIoHotLane(size_t k) {
	SetHotWorkers(k);
	SetReservedNeverParks(k > 0);
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
	// AND CLAMPED TO WHAT THE LANE BITMAP CAN NAME. stealHintLane is one 64-bit word, unlike the
	// backlog/parallel maps which are four -- and that is deliberate rather than an oversight, because
	// a hot worker is always one of the LOWEST indices (`victimIsHot` is literally `hotN > target`),
	// so one word covers K <= 64 and K is measured useful in the 1-4 range.
	//
	// But "implausible" is not "impossible", and unclamped it fails SILENTLY: on a 128-thread part
	// somebody could ask for K=100, and hot workers 64..99 would never set a lane bit, so no sibling
	// could ever find them buried -- hot->hot stealing simply absent for a third of the lane, with
	// nothing reporting it. Clamping is the honest failure: K stops where the bitmap does.
	if (eff > 64) eff = 64;
	// Clamped against the LIVE F inside the CAS -- see BandsSetKClamped. K may not grow into the
	// floor or past the end of the pool, mirroring the refusal NoteFloorCrowding already makes on
	// the F side. Whichever controller asks second is the one that is refused; K does NOT shrink F
	// to make room, because that would steal indices from under a running ParallelFor.
	// REFUSED, NOT ACCOMMODATED. BandsSetK never decrements F to make room -- see the note on the
	// packed word. If K+F would exceed the pool the request is capped, F sheds on a later pass, and
	// K may grow then. `eff` is re-read because what was COMMITTED is what the wake loop below must
	// walk; asking for 4 and getting 2 must wake two workers, not four.
	const size_t nWorkers = instance ? instance->workers.size() : 0;
	const size_t prev = BandsSetK(eff, nWorkers);
	eff = BandsK();
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

	// ---- A PROMOTED WORKER SHEDS ITS ORDINARY WORK BEFORE IT IS A LANE WORKER ----------------
	//
	// THE INVARIANT IS THAT K NEVER READS A LOPRI INBOX. Every drain site in Worker() is now
	// gated on !reservedForHiPri, with no exception -- the stray-pop net that used to rehome an
	// ordinary task found on a reserved worker is gone, because a lane worker stopping to service
	// bulk work is the thing the lane exists to prevent. It is already behind; that is why it is
	// reserved.
	//
	// Which makes THIS the one legal way work can end up there, and the only one: the task was
	// placed while worker i was still compute, and K then grew over an inbox that already held it.
	// Placement reads LIVE K, so after BandsSetK above no new ordinary push targets [0, eff) --
	// but what is already sitting there is this function's problem, and nobody else's. Shed it
	// here, to the first compute worker, before the promotion is allowed to mean anything.
	//
	// WITHOUT THIS THE TASK IS LOST, not delayed: loPri inboxes are owner-drain-only, the owner
	// has just been told never to look, and no thief may. Removing the net without adding this
	// would trade a papered-over placement bug for a silent strand on a legal path.
	//
	// K ONLY, NOT ON A DEMOTE. hiEnd > loEnd in both directions; a worker LEAVING the band keeps
	// its inbox and starts draining it again on its next pass, which needs nothing from here.
	if (prev < eff) {
		const size_t firstCompute = eff;
		if (firstCompute < instance->workers.size()) {
			size_t moved = 0;
			for (size_t i = loEnd; i < hiEnd && i < instance->workers.size(); ++i) {
				Task* t = nullptr;
				while (instance->loPriInboxes[i]->pop(t) && t) {
					instance->loPriInboxes[firstCompute]->push(t);
					instance->workers[firstCompute]->inboxDepth.fetch_add(
						1, std::memory_order_relaxed);
					instance->workers[i]->inboxDepth.fetch_sub(1, std::memory_order_relaxed);
					++moved;
					t = nullptr;
				}
			}
			if (moved) {
				instance->workers[firstCompute]->MarkQueuedWork();
				instance->workers[firstCompute]->NotifyWorker(/*force*/ true);
			}
		}
		// firstCompute >= size means K covers the pool. BandsSetK clamps to n-1 precisely so that
		// cannot happen -- see ClampHotWorkersToPool -- so there is no "nowhere to put it" case to
		// handle here, and inventing one would be inventing a policy for an unreachable state.
	}

	for (size_t i = loEnd; i < hiEnd && i < instance->workers.size(); ++i) {
		instance->workers[i]->MarkLaneWake();
		instance->workers[i]->NotifyWorker(/*force*/ true);
	}

	// ---- THE FLOOR SLIDES WITH K, AND ITS NEW TAIL MAY BE ASLEEP --------------------------
	//
	// The loop above wakes the indices whose RESERVED membership changed. But the floor is
	// [K, K+F), so moving K moves the floor window too, and on a K GROW its new tail --
	// [prev+F, eff+F) -- was parkable a moment ago and may be parked right now. Nothing else
	// wakes it: floor growth's promotion wake only fires when F itself changes, and the notify
	// skip that would at least have been HONEST about these workers is gone. A worker there
	// becomes a floor member in its sleep -- the bench counts it as a live-floor park violation,
	// and until a push happens to land on that index the floor is running one narrower than the
	// word claims.
	//
	// (The SHED direction needs nothing: the ex-reserved indices that slide into the floor are
	// exactly [eff, prev), which the loop above already woke, and the ex-floor tail is awake by
	// definition and parks itself on its next pass.)
	//
	// Wakes the whole slid range unconditionally rather than testing who is parked -- K changes
	// are hold-gated and rare, and a force-notify on a worker that is already awake is one
	// WakeByAddress on a value that will not match. Same one-CAS-visibility argument as the loop
	// above: F is read AFTER the K CAS, so the window woken is the window the new word defines.
	if (eff > prev) {
		const size_t fNow = BandsF();
		for (size_t i = prev + fNow; i < eff + fNow && i < instance->workers.size(); ++i) {
			instance->workers[i]->NotifyWorker(/*force*/ true);
		}
	}
}
size_t TaskScheduler::GetHotWorkers() {
	// K IS BEING REMOVED, AND THIS IS THE GATE THAT MAKES THE WHOLE LANE INERT FIRST.
	//
	// Returning 0 here turns off every consumer of K in one place, because they all reduce to this:
	//   HiPriLaneActive()  -> false, so every push routes to loPriInboxes and hiPri gets nothing
	//   PickNextWorker     -> the lane branch is `hiPri && hotN`, and the ordinary branch's
	//                         `idx < hotN` skip stops reserving workers 0..K-1
	//   isHotWorker        -> `GetHotWorkers() > qIndex` is false for every worker
	//   WorkerServesHiPri  -> false, so nobody drains or steals the lane
	//
	// DEAD BEFORE DELETED, deliberately. The alternative -- ripping ~300 references out across 13
	// files and then finding out whether it still works -- cannot tell a routing mistake from a
	// deletion mistake. With the lane provably unreachable and the suite still green, whatever
	// breaks during the deletion is the deletion.
	//
	// hiPriStray stays live through this phase ON PURPOSE: it is the path that drains a lane queue
	// nobody serves, so anything already sitting in hiPri when this flipped still gets run.
	//
	// ---- K IS BACK, STATIC, AND DISJOINT FROM THE FLOOR (4.0.2) ---------------------------
	//
	// The unconditional `return 0` that used to sit here is gone. K is a STATIC count chosen when
	// I/O is turned on -- 0, 2 or 4 -- with no occupancy controller and no promotion. The layout it
	// defines:
	//
	//     [0,     K)      reserved. I/O resumes only. No ordinary work from ANY source.
	//     [K,   K+F)      the awake floor. Compute, never parks.
	//     [K+F,   N)      compute, parks when idle.
	//
	// TWO KNOBS, DISJOINT SETS, and that is the whole point. Merging them is what produced the
	// failure that led here: with reservation carved OUT of the floor, R=2 and F=2 left zero awake
	// workers taking ordinary pushes and the bench stopped making progress. Floor growth also has
	// to stay above K, or a no-op flood turns a reserved core into a spinner that then "helps" by
	// stealing a ParallelFor leaf -- which is exactly the running-leaf problem K exists to prevent.
	//
	// K=0, F=2 is the ordinary jobs pool with a wake-free dispatch band. K=2, F=2 is two reserved
	// plus two always-awake compute workers.
	return BandsK();
}

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

// HotScalingActive() USED TO SIT HERE and is gone. It claimed to be the master off-switch for the
// controller -- `return false` ahead of its real body -- but NOTHING READ IT: the controller gates
// on max > min directly, three lines below, so the switch disabled nothing. A dead flag that reads
// like a live one is worse than no flag: it invites exactly the mistake of gating a NEW mechanism
// on it, which leaves that mechanism off while the controller it feeds runs on the other predicate.
// (That is how the lane counters would have gone unwritten again: observer off, controller reading
// zeros, `low = topTasks == 0` always true, K shedding every window.)
//
// THE CAVEAT IT CARRIED IS REAL AND IS KEPT, because it is about the controller, not the flag:
//
//   The demote path once steered K from LANE OCCUPANCY -- the fraction of wall time a hot worker
//   spent running lane tasks. That assumed an idle worker BLOCKS, so idle time was unambiguous.
//   With the blocking park gone an idle worker SPINS, and occupancy stopped meaning what the
//   thresholds were tuned against. Symptom: dynamic_k_test failed about one run in four while
//   printing "ALL CHECKS PASSED" -- a decision that went the wrong way on a threshold, not a crash.
//
//   The demote has since been moved to a TASK COUNT (`low = topTasks == 0`), which is spin-immune
//   by construction: a spinning worker runs no lane tasks and counts zero. Occupancy survives only
//   in the slow PROMOTE (minBusyNs >= 70% of the window), where reading low on a spinning-but-idle
//   lane can only decline to add a core -- the safe direction.
//
// RE-TUNE BEFORE TRUSTING THE PROMOTE SIDE. To disable the controller now, pin the range:
// SetHotWorkers(k) sets min == max, which is the default and is what everything ships with.

// Re-apply the clamp once the pool size is known -- see SetHotWorkers.
void TaskScheduler::ClampHotWorkersToPool() {
	const size_t k = g_hotWorkersRequested.load(std::memory_order_relaxed);
	const size_t n = workers.size();
	BandsSetK((n && k >= n) ? n - 1 : k, n);

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
		const uint64_t cw = BandsWord();
		if (BandField(cw, kBandKmax) > cap) BandsSetKmax(cap);
		if (BandField(cw, kBandKmin) > cap) BandsSetKmin(cap);
	}
}

// Read by the hot workers themselves and by the reactor's completion threads, each of which raises
// its OWN priority. Off by default -- see the header.
static std::atomic<TaskScheduler::HotThreadPolicy> g_hotPolicy{ TaskScheduler::HotThreadPolicy::Normal };

void TaskScheduler::SetHotThreadPolicy(HotThreadPolicy p) { g_hotPolicy.store(p, std::memory_order_relaxed); }

// Read ONCE per worker at thread entry, never on a per-task path -- relaxed is right and nothing
// downstream orders against it.
static std::atomic<TaskScheduler::PowerThrottling> g_powerThrottling{ TaskScheduler::PowerThrottling::OptOut };
void TaskScheduler::SetWorkerPowerThrottling(PowerThrottling p) { g_powerThrottling.store(p, std::memory_order_relaxed); }
TaskScheduler::PowerThrottling TaskScheduler::GetWorkerPowerThrottling() { return g_powerThrottling.load(std::memory_order_relaxed); }
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
			}, false, TaskType::Native, CorePref::Wide);
		// Wide: a cursor LANE is a worker's worth of participation in one range. Steering these at
		// the floor is self-defeating -- every lane lands on the same couple of threads and the
		// cursor they share has nobody else pulling on it, which is the fan-out failure this whole
		// path exists to provide.
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
		// `attempt` gates the exchange, and the gate is load-bearing: ParallelFor constructs this
		// unconditionally and only LOOKS at it for non-workers -- but the CONSTRUCTOR ran either
		// way, so a WORKER's ParallelFor was silently taking the non-worker lane claim and holding
		// it for the life of its range. Any main-thread ParallelFor overlapping it then found the
		// claim "taken" and degraded to the cursor path for no reason: the flag was recording who
		// constructed an object, not who owns the lane.
		explicit NonWorkerLaneClaim(std::atomic<bool>& f, bool attempt) {
			if (attempt && !f.exchange(true, std::memory_order_acquire)) {
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
		if (q >= 0 && (size_t)q < deques.size()) return deques[q].get();
		return nullptr;
	}
	// A non-worker publishes onto the shared non-worker lane, but ONLY if it holds the claim.
	// Without that check two app threads would push to one Chase-Lev deque, which has exactly one
	// legal producer.
	if (t_ownsNonWorkerLane && nonWorkerLane < deques.size()) return deques[nonWorkerLane].get();
	return nullptr;
}

// Which lane index LaneForCurrentThread just returned. Mirrors it exactly; SIZE_MAX when there is
// none. Split for one reason: the PARALLELISM hint is per-lane and needs the index, and duplicating
// the resolution at the call site is how the two would drift.
size_t TaskScheduler::LaneIndexForCurrentThread() {
	Thread* self = Thread::GetCurrent();
	if (self) {
		const int q = self->qIndex;
		if (q >= 0 && (size_t)q < deques.size()) return (size_t)q;
		return SIZE_MAX;
	}
	if (t_ownsNonWorkerLane && nonWorkerLane < deques.size()) return nonWorkerLane;
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
// How many unclaimed splits a range tolerates on its own lane before it stops publishing and runs
// the remainder inline. 0 = split unconditionally (the pre-4.0.2 behaviour). Runtime rather than
// constexpr so one binary can A/B it on the machine that holds the baseline.
// Leaves per worker the splitter will mint before it raises the grain. 64 is the shipped value and
// effectively never binds; smaller values trade load-balancing granularity for dispatch cost.
// Iterations per worker below which a range is not worth fanning out AT ALL. N-only -- no probe, no
// body estimate -- and deliberately conservative: being wrong here serialises a rare shape, being
// wrong the other way costs 100x on the shape a frame loop hits constantly.
// 8, measured. The gate then sits at N < workers*8 (248 on a 31-worker pool), which is small enough
// that heavy N=256 still fans out (5.86x) and large enough that a trivial range of that size does
// not mint 124 tasks for 0.12 us of work. 16 was tried and takes heavy N=256 with it.
// 64. The gate then sits at N < workers*64 (1,984 on a 31-worker pool), which covers N=256, 512 and
// 1000 -- the cells where a trivial range was minting tasks for a tenth of a microsecond. 8 was
// tried and leaves N=256 on the parallel path (248 < 256), which is how that row still showed 22
// workers. The cost is heavy N=256-1000 running serially; the row that has to survive is heavy
// N=2000 and above, which does.
static std::atomic<size_t> g_minItersPerWorker{ 64 };
#define kMinItersPerWorker (g_minItersPerWorker.load(std::memory_order_relaxed))
void   TaskScheduler::SetMinItersPerWorker(size_t n) noexcept { g_minItersPerWorker.store(n, std::memory_order_relaxed); }
size_t TaskScheduler::GetMinItersPerWorker() noexcept { return g_minItersPerWorker.load(std::memory_order_relaxed); }

// Absolute minimum iterations in a leaf. maxLeaves is proportional to N and so never prevents a
// small range from splitting to grain 1; this does.
static constexpr size_t kMinGrain = 16;

// The shortest reading the width probe will divide by. Above the clock's own noise, and low enough
// that an expensive body clears it on its FIRST item -- which is the whole point, since the probe
// is serial time on the critical path and that path is shortest exactly when the body is dear.
static constexpr long long kProbeFloorNs = 500;

// ---- REMEMBERED BODY COST, PER CALL SITE ------------------------------------------------------
//
// WHY THIS EXISTS. The width probe is one leaf late by construction: it cannot know what a body
// costs until it has run some of it, so the FIRST range of any burst pays the old behaviour and
// only later ones benefit. Remembering what the last call measured moves that knowledge to BEFORE
// the next range instead of after, which is the whole of the cold-start problem.
//
// KEYED BY THE CALLABLE'S TYPE, AND A GLOBAL AVERAGE WOULD BE WORSE THAN NOTHING. A single EWMA
// across all call sites mixes a 0.5 ns/element body with a 600 ns/element one and produces a number
// describing neither -- and the bench alternates exactly those two, so it would have been measured
// as "working" while being meaningless. std::function::target_type() is the identity actually
// available here: every lambda has a distinct type, so a call site keys to itself across calls
// without an API change or a caller-supplied token.
//
// COLLISIONS ARE SAFE, which is why open addressing with a short probe is enough. A wrong estimate
// costs a wrong initial width, and width is already a LOWER BOUND that recruitment corrects upward
// from measured leaves. Nothing here can produce an incorrect result, only a worse ramp.
//
// STORED AS ns PER MILLION ITEMS because per-item is routinely sub-nanosecond and integer division
// would floor a trivial body to zero -- the exact case that must not be confused with "unknown".
namespace {
	struct BodyCost {
		std::atomic<size_t>    key{ 0 };          // type_info hash, 0 = empty
		std::atomic<long long> nsPerMega{ 0 };    // ns per 1,000,000 items
		std::atomic<unsigned>  uses{ 0 };         // for the periodic re-probe
	};
	constexpr size_t kBodyCostSlots = 64;
	BodyCost g_bodyCost[kBodyCostSlots];

	// A REPROBE EVERY 64 USES, because a remembered cost is a claim about a body that may change --
	// same lambda, different data, different cache behaviour. Cheap insurance: 1 call in 64 pays
	// the probe and refreshes, the rest start hot.
	constexpr unsigned kReprobeInterval = 64;

	BodyCost* FindBodySlot(size_t key) noexcept {
		if (key == 0) key = 1;                     // 0 is the empty sentinel
		const size_t start = key % kBodyCostSlots;
		for (size_t i = 0; i < 8; ++i) {           // short probe; give up rather than scan 64
			BodyCost& e = g_bodyCost[(start + i) % kBodyCostSlots];
			const size_t k = e.key.load(std::memory_order_acquire);
			if (k == key) return &e;
			if (k == 0) {
				size_t expected = 0;
				if (e.key.compare_exchange_strong(expected, key, std::memory_order_acq_rel,
				                                  std::memory_order_acquire))
					return &e;
				if (expected == key) return &e;    // lost the race to the same key: fine
			}
		}
		return nullptr;                            // table full for this key: behave as uncached
	}
}

// How many thieves a range wakes on its OPENING publish. Not 1 (too slow to fill a parked pool) and
// not a cascade (which lands 29 workers on a range that wanted 4). After the first wave, a wake
// happens only when a previous split is still unclaimed -- demand recruits the rest.
static constexpr size_t kFirstWaveThieves = 4;

// 8, not 64. 64 only binds above N = workers*64, so below that the caller.s grain decided and a
// trivial 256-element range minted 124 tasks.
static std::atomic<size_t> g_leavesPerWorker{ 8 };
void   TaskScheduler::SetLeavesPerWorker(size_t n) noexcept { g_leavesPerWorker.store(n ? n : 1, std::memory_order_relaxed); }
size_t TaskScheduler::GetLeavesPerWorker() noexcept { return g_leavesPerWorker.load(std::memory_order_relaxed); }

static std::atomic<size_t> g_lazySplitCap{ 2 };
void   TaskScheduler::SetLazySplitCap(size_t n) noexcept { g_lazySplitCap.store(n, std::memory_order_relaxed); }
size_t TaskScheduler::GetLazySplitCap() noexcept { return g_lazySplitCap.load(std::memory_order_relaxed); }

// ON BY DEFAULT, because the thing it replaces is doing nothing: a range currently wakes ONE worker
// and waits to be discovered. The flag exists so `norecruit` can A/B the whole mechanism in one
// binary against the floor=31 ceiling, which is what bounds its payoff.
// ---- ON BY DEFAULT AS OF 2026-08-31 ----------------------------------------------------------
//
// It shipped opt-in while it was unproven. It is not unproven any more: against the iteration-count
// gate it replaces, on the same run,
//
//                 trivial2000  trivial10000  light2000  light200k  heavy256  heavy1000
//   minfan gate      0.05x        0.28x        0.22x     10.72x     1.00x     1.00x
//   measured         0.94x        0.96x        1.04x     20.28x     6.61x    14.63x
//
// The gate fans a trivial body out at N=2000 and loses 19x, loses 4.7x on light at the same N, and
// refuses heavy at N<=1000 where 6-15x is available. Those are not close calls and they are far
// outside the noise -- a 0.05x cell means the parallel arm took twenty times the serial one, which
// no measurement artefact produces.
//
// WHAT IT COSTS, stated plainly: ParallelFor now runs a few items on the CALLER before deciding
// anything, so a range that would have run serially pays a probe it did not before. Measured at
// trivial N<=512, that is about 0.06 us. It buys not throwing away 130 us on heavy N=256.
//
// THE CELLS BEHIND THE SMALL NUMBERS ARE QUANTISED and should not be read finely: steady_clock is
// 100 ns here, trivial N=256 is ~120 ns of work, and the sweep takes a MINIMUM over runs -- so
// those ratios snap to small integer fractions (1/2, 2/3, 4/5) and repeat exactly. The case above
// rests on the large-magnitude cells, which are hundreds of ticks apart.
//
// SetMeasuredWidth(false) restores the old gate exactly; `nomwidth` is the bench arm.
static std::atomic<bool> g_measuredWidth{ true };
void TaskScheduler::SetMeasuredWidth(bool on) noexcept { g_measuredWidth.store(on, std::memory_order_relaxed); }
bool TaskScheduler::GetMeasuredWidth() noexcept { return g_measuredWidth.load(std::memory_order_relaxed); }

// ---- OFF BY DEFAULT, AND THE REASON IS A MEASURED FAILURE, NOT CAUTION -----------------------
//
// The idea is right and the KEY IS WRONG. Keying on func.target_type() assumes one callable type
// per call site, and that assumption breaks on a completely ordinary pattern: a dispatcher that
// forwards every body through a single wrapper lambda. The scheduler's own grain sweep does exactly
// that -- one lambda at bench.cpp serves trivial through heavy, selecting the body from a captured
// variable -- so all four hash to the SAME slot and the EWMA averages a 0.5 ns/element body with a
// 600 ns/element one.
//
// MEASURED, mwidth with minfan=0, memory on vs off:
//
//                trivial256  light256  light1000
//   nomemory        0.33x      0.71x      0.38x
//   remembered      0.01x      0.07x      0.09x
//
// 33x worse on trivial, because the remembered "average" body looks expensive enough to fan out
// work that costs nanoseconds. This is the exact hazard written into the comment on the table above
// -- and it was shipped anyway on the assumption that call sites have distinct lambda types.
//
// SO IT NEEDS A KEY THE LIBRARY CANNOT DERIVE. A caller-supplied token would work and is an API
// change; a return-address key would work and is not portable. Left in, off, because the mechanism
// is correct once keyed correctly and the next attempt should start from this result rather than
// rediscover it.
static std::atomic<bool> g_rememberedCost{ false };
void TaskScheduler::SetRememberedCost(bool on) noexcept { g_rememberedCost.store(on, std::memory_order_relaxed); }
bool TaskScheduler::GetRememberedCost() noexcept { return g_rememberedCost.load(std::memory_order_relaxed); }

static std::atomic<bool> g_rangeRecruit{ true };
void TaskScheduler::SetRangeRecruit(bool on) noexcept { g_rangeRecruit.store(on, std::memory_order_relaxed); }
bool TaskScheduler::RangeRecruitEnabled() noexcept { return g_rangeRecruit.load(std::memory_order_relaxed); }

// 3000 ns: the measured OS wake on this machine (8.0 us resume with a 1 ms hold-off against 5.0 us
// with none). A knob and not a constant -- it is a property of the box, and every recruitment
// decision is a ratio against it, so a wrong value here biases the whole mechanism one way.
static std::atomic<unsigned> g_wakeCostNs{ 3000 };
void TaskScheduler::SetWakeCostNs(unsigned ns) noexcept {
	// ZERO WOULD DIVIDE, and worse it would mean "wakes are free", which recruits the whole pool for
	// any leaf at all. Clamp to something a wake cannot be cheaper than.
	g_wakeCostNs.store(ns < 100 ? 100 : ns, std::memory_order_relaxed);
}
unsigned TaskScheduler::GetWakeCostNs() noexcept { return g_wakeCostNs.load(std::memory_order_relaxed); }

void TaskScheduler::RunLazyRange(int lo, int hi, LazyRangeState* st) {
	TaskDeque* myLane = LaneForCurrentThread();
	const size_t myLaneIndex = LaneIndexForCurrentThread();

	const size_t pendingCap = GetLazySplitCap();
	while (myLane && (hi - lo) > st->grain) {
		// The wake decision below needs to know whether our previous split is still sitting here.
		// Sampled before the push, used after it.
		const bool laneWasEmpty = (myLane->size() == 0);

		// ---- SPLIT ONLY WHILE SOMEBODY IS ACTUALLY TAKING THE SPLITS ------------------------
		//
		// This loop used to split unconditionally down to `grain`, materialising a Task per leaf
		// whether or not the pool had any use for one -- 128 tasks at EVERY size in the bench sweep,
		// because the caller asks for 4 chunks per worker regardless of N. That is free when a leaf
		// is 150 us of work and ruinous when it is 2 ns: measured against the cursor, the splitter
		// loses 2-5x on trivial/light at N=1000-20000 and wins 5-7x on medium/heavy. One mechanism,
		// opposite sign, decided entirely by what a leaf costs.
		//
		// THE LANE'S OWN DEPTH IS THE DEMAND SIGNAL -- no probe, no clock, no estimate of the body.
		// If what we published is still sitting there, nobody wanted it and publishing more cannot
		// help; the remainder runs inline instead, which is the ~11 ns path. If the lane has drained,
		// thieves are consuming as fast as we produce and splitting further is exactly right. A heavy
		// range keeps splitting because its leaves are taken instantly; a trivial one stops after a
		// couple, because they are not.
		//
		// A CAP RATHER THAN "MUST BE EMPTY": a thief that is a microsecond late should not collapse a
		// genuinely parallel range into serial work. `pendingCap` is how many unclaimed splits we
		// tolerate before concluding the pool is not asking for more. 0 restores the old behaviour.
		//
		// This is what the design comment already claimed -- "the range is split speculatively and
		// STEALS decide" -- except steals decided nothing: every split was materialised up front and
		// the steal only chose who ran it.
		if (pendingCap && myLane->size() > pendingCap) break;

		const int mid = lo + (hi - lo) / 2;

		// EXPLICITLY NATIVE, exactly as RunCursorRange spells it. A range leaf never suspends, so it
		// must not take a fiber stack -- and, more sharply, it must not fall under the fiberless steal
		// filter: GetTask vets candidates with `fiberlessRunnable`, which rejects TaskType::Fiber
		// outright, so a Fiber-typed split is invisible to a BARE waiter. Main would publish splits and
		// then yield in a loop it could never make progress on, while its own work sat on the caller
		// lane -- and with a small awake floor the parked workers would not look at that deque either.
		//
		// The default is already Native, so this argument changes no behaviour today. It is written out
		// because the DEFAULT is the wrong thing to depend on here: the cursor path states it, this one
		// did not, and a future change to the default would silently break only the splitter -- as a
		// regression in bare-thread ParallelFor, which is the hardest shape to attribute.
		// ---- DEFAULT, NOT Wide, AND THAT WAS MEASURED ---------------------------------------
		//
		// `Wide` was tried here and lost. The argument for it looks identical to the cursor path's
		// -- a split half exists so another worker runs it -- and it is wrong for one reason the
		// cursor does not share: THIS SPLIT IS SPECULATIVE. The range is halved on the guess that a
		// thief will take it, and an untaken split is taken back and run inline for ~11 ns. That
		// cheapness is the whole design. Placing it Wide pushes it at a possibly-parked worker and
		// pays a kernel wake PER SPLIT, recursively, for a task that was never certain to be needed.
		//
		// SETTLED BY THE `splitpref` ROW: THE EFFECT IS BELOW WHAT CAN BE MEASURED HERE.
		//
		// The two arms alternate inside one measurement and reach the SAME NUMBER OF WORKERS --
		// 28-31 of 31, every case, both arms, across three runs on two machines. The timing does not
		// settle: paired per-rep ratios span [0.83 .. 1.42] INSIDE a single run, and the per-case
		// median has read 0.91x, 1.06x and 1.21x for the same cell on the same binary.
		//
		// Read carefully, that is not "no effect" -- 7 of 9 case-medians landed above 1.00, so there
		// may be a ~5% lean toward Wide. It is that a ~5% lean cannot be established under a +/-40%
		// per-rep spread, and no number of reruns of a three-rep row will change that.
		//
		// WHY, AND THIS IS THE PART WORTH KEEPING: placement barely reaches the splitter. A lazy
		// split is halved recursively and distributed by STEALING -- that is the design, and an
		// untaken split is taken straight back and run inline for ~11 ns. Placement decides only
		// where the first push lands; the recursion and the thieves do the rest. So the breadth hint
		// has almost nothing to act on here.
		//
		// THE RULE THAT FALLS OUT: Wide matters where PLACEMENT IS THE ONLY DISTRIBUTION MECHANISM,
		// and not otherwise.
		//   burst        16 independent tasks pushed at once, no recursion  -> 12 to 29 participants
		//   cursor lanes N lanes created up front, nothing splits them      -> same shape as burst
		//   splitter     recursive halving plus steals                      -> 29 either way
		//
		// Default therefore stays, on the cheaper-push argument rather than on Wide being worse:
		// when two options measure the same, take the one that does not pay a kernel wake.
		Task* t = CreateTask([this, mid, hi, st]() { RunLazyRange(mid, hi, st); },
		                     false, TaskType::Native,
		                     ParallelSplitWide() ? CorePref::Wide : CorePref::Default);
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
		// Advertise the split. The hint is what a thief looks for; see the note in
		// publishing does NOT also wake here -- a wake per split measured as pure cost
		// (1p 9.19 -> 7.47 M/s, latency 0.39 -> 0.71 us) against a ParallelFor problem that did
		// not exist.
		// ---- AND WAKE ONE THIEF ON THE 0 -> 1 EDGE -------------------------------------------
		//
		// SetParallelHint's return exists for exactly this and the value was being discarded, which
		// left the splitter advertising a lane that nobody was ever woken to read. Under an awake
		// floor that is close to fatal: only the F floor workers are looking, the other 29 are in
		// WaitOnAddress, and a range gets F-way parallelism no matter how wide it is. Measured on
		// the crossover sweep, same machine: light at N=200000 22.31x -> 8.70x, trivial 6.15x ->
		// 2.96x, and every crossover moved right by ~4x. It reads as "ParallelFor only kicks in for
		// heavy bodies", because heavy is the one case whose bodies last long enough for the floor
		// controller to grow the pool by another route.
		//
		// ONCE PER RANGE, NOT PER SPLIT, and that distinction is the whole reason the edge is
		// returned rather than the caller just always waking: a wake per split was measured as pure
		// cost (the splitter fell 91x behind the cursor) because a range publishes hundreds of them.
		//
		// ONE IS ENOUGH BECAUSE IT CASCADES. The woken thief steals a split, runs it, and publishes
		// its own half to its own lane -- which is another 0 -> 1 edge, which wakes another. The
		// pool fills geometrically from one wake rather than linearly from a wake per task, and the
		// cost stays proportional to the number of ranges instead of the number of splits.
		// THE HINT IS UNCONDITIONAL. Only the WAKE is behind the flag.
		//
		// This was written as `kWakeOnRangePublish && ... && SetParallelHint(...)` and the
		// short-circuit meant turning the flag off ALSO stopped advertising the lane -- so the A/B
		// it existed for compared "hint + wake" against "no hint at all", not against "hint alone".
		// That is why the wake measured as 4-14x load-bearing and why a probe with the wake off saw
		// ZERO workers touch a published range: nothing was advertised for them to find. An A/B
		// whose control silently disables a second mechanism measures the wrong difference.
		const bool edge = (myLaneIndex != SIZE_MAX) && SetParallelHint(myLaneIndex);
		if (kWakeOnRangePublish && edge)
			WakeOneForSteal();

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
		// ---- AND WAKE ON THE FIRST SPLIT TOO, NOT ONLY THE SECOND ---------------------------
		//
		// The condition below is `!laneWasEmpty`: wake somebody when our previous split is STILL
		// sitting here, i.e. supply is outrunning demand. Sensible on its own, and it skips exactly
		// one publish per range -- the first one, when the lane is empty because nothing has been
		// published yet. That is the publish with the most range left to give and the only one that
		// happens while the pool may be entirely parked.
		//
		// IT DID NOT MATTER WHEN A HINT COULD REACH A SLEEPER. It does now. A parked worker blocks
		// in WaitOnAddress on a predicate that reads its own inbox, hasQueuedWork and laneWake --
		// nothing pool-wide. `SetParallelHint` flipping a bit cannot wake it. So the audience for a
		// range is the awake floor plus whoever this block wakes, and skipping the first publish
		// means a range's opening move reaches nobody who was asleep.
		//
		// ONCE PER RANGE, via state shared by every split, so this adds exactly one wake to a whole
		// ParallelFor rather than one per split -- the per-split version is what put the splitter
		// 91x behind the cursor and is not what this is.
		// ---- WAKE A NON-FLOOR WORKER, AND FORCE IT --------------------------------------------
		//
		// TWO BUGS IN ONE BLOCK, both of which left the overflow pool asleep through an entire
		// range:
		//
		// 1. The victim was chosen from `hotN`, which is 0, so the wake could land on a FLOOR
		//    worker -- already spinning, nothing gained, and the one wake this range gets is spent.
		//    The eligible set starts past max(hotN, floor): those are the workers actually parked.
		//
		// 2. NotifyWorker was called WITHOUT force, so it returns early on `qIndex < awakeFloor` or
		//    because the floor check fires. WS_AWAKE no longer causes an early return --
		//    Wake() swaps a permit unconditionally and only skips the SYSCALL. A parked
		//    overflow worker still cannot be woken by a steal hint -- the wait predicate
		//    laneWake, nothing pool-wide -- so if this call skips, that worker stays in the kernel
		//    for the life of the range. force=true is what makes it a wake instead of a hint.
		//
		// RATE: the first publish always wakes (it is the moment the range has the most left to
		// give and the pool is most likely parked), then one in four after that. A wake per split
		// is what put the splitter 91x behind the cursor; one per range was too few to fill a pool
		// that cannot hear a hint.
		++t_splitsPublished;
		const bool firstPublish =
			laneWasEmpty && !st->wokeForRange.exchange(true, std::memory_order_relaxed);
		// ---- CAP THE FIRST WAVE; LET DEMAND RECRUIT THE REST ---------------------------------
		//
		// A range opens by waking a SMALL, FIXED number of thieves -- not one (too slow to fill a
		// parked pool) and not a cascade (which lands 29 workers on a range that needed 4).
		//
		// After that, a wake happens only when `!laneWasEmpty` -- our previous split is STILL
		// sitting on the lane, so the thieves we already have are not keeping up and another is
		// genuinely wanted. That is the difference between recruiting and broadcasting: a cheap
		// N=2000 range finishes on main plus the first wave, because its lane keeps draining and no
		// further wake is ever justified; a heavy N=2000 range keeps leaving splits unclaimed for
		// ~1 ms each and pulls in the whole pool, one worker at a time, exactly as it should.
		//
		// The grain floor is what makes this safe: leaves are now big enough that a thief arriving
		// late still finds something worth taking, so recruiting slowly costs little.
		// (A duplicate of the firstWave definition below was glued onto the end of this comment
		// line during the rewrite -- dead text that compiled because it sat inside the comment.
		// Removed; the live definition follows the pacing note.)
		// AFTER THE FIRST WAVE, PACE IT. `!laneWasEmpty` was tried as the demand signal and it does not
		// cap anything: a caller publishes faster than four thieves drain, so the lane is almost always
		// non-empty and the rule recruits one per split -- 29 workers on a range that wanted 4, and
		// trivial/light crossovers back at 200000/40000. One in four splits is slow enough that a cheap
		// range finishes before the pool fills, and fast enough that a heavy one still gets everybody.
		// Four thieves on the opening publish, one every fourth split after that. Not one (too slow to
		// fill a parked pool) and not one per split (that is what put the splitter 91x behind the cursor).
		//
		// TIME-PACED RECRUITMENT WAS TRIED HERE AND REVERTED. Rate-limiting wakes to one per 10 us was
		// meant to let a cheap range finish on the first wave while a heavy one filled the pool. It did
		// not cap: trivial N=2000 still drew 27 workers, because on a warm pool nobody needs waking --
		// awake workers steal whatever is advertised. Controlling wakes cannot control participation,
		// and reading a clock to decide is a probe wearing a different hat.
		const size_t firstWave = firstPublish ? kFirstWaveThieves : 0u;
		const bool   needThief = firstPublish || (t_splitsPublished & 3u) == 0u;
		if (needThief && !workers.empty()) {
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
			// PAST BOTH THE LANE AND THE FLOOR. Those workers are already running -- waking one is a
			// wasted syscall and spends the wake this range gets. The parked overflow workers are
			// the only ones a wake can do anything for, and `hotN` alone is 0, so the old bound
			// let the wake land on a floor worker.
			// [K+F, N) IS THE PARKED SET, and this computed max(K, F) -- floor WIDTH used as an
			// absolute index, the same off-by-K family as SetAwakeFloor's wake and the collapse
			// gate before it. Identical at K=0 (max(0,F) == 0+F), which is how it survived; at
			// K=2/F=2 it started the sweep at worker 2, so up to half of the opening wave's four
			// wakes were spent force-waking floor workers that were already spinning -- the two
			// wakes this range most needed, delivered to the two workers that least needed them.
			// It also read K and F as two separate loads, which is the torn pair everywhere else
			// in this file reads from one word.
			const TaskScheduler::Bands wb = GetBands();
			const size_t start = wb.k + wb.f;
			if (start < workers.size()) {
				// N at a time on the opening publish, one thereafter. Each is FORCED: without it
				// NotifyWorker returns early on the floor check and never reaches
				// Wake(), and a parked worker has no other way to learn a lane was published --
				// its WaitOnAddress predicate reads only its own inbox, hasQueuedWork and laneWake,
				// nothing pool-wide.
				const size_t span  = workers.size() - start;
				size_t       count = firstWave ? firstWave : 1u;
				if (count > span) count = span;
				for (size_t k = 0; k < count; ++k) {
					const size_t w = start + (wakeCursor++ % span);
					workers[w]->MarkQueuedWork();
					workers[w]->NotifyWorker(/*force=*/true);
				}
			}
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

	// ---- TOO SMALL TO FAN OUT AT ALL --------------------------------------------------------
	//
	// A range shorter than this cannot repay the dispatch, whatever its body costs. Note what this
	// is NOT: it is not a probe, not an estimate, and not a guess about the body. It uses only N and
	// the pool size -- both of which ParallelFor already has -- and it asks the one question those
	// two can actually answer: is there enough here that waking and entering 31 workers could
	// possibly be worth it?
	//
	// THE MEASUREMENT THAT FORCED IT: at N=256 with a trivial body, the sweep reported 31 DISTINCT
	// workers each running a leaf, for a range holding ~0.12 us of total work. Fan-out was working
	// perfectly; the decision to fan out was the bug. 124 tasks minted, stolen and destroyed to do
	// what one thread does in a tenth of a microsecond -- which is why that cell read 0.01x and why
	// no amount of fixing the WAKE could ever move it.
	//
	// N-only means it can be wrong in one direction: a range of 1,984 elements whose body costs a
	// millisecond each runs serially. That is the trade for having no probe, and it is the safe
	// direction -- being wrong here costs a bounded amount of parallelism on a rare shape, where
	// being wrong the other way costs 100x on the shape a game hits every frame.
	// ---- MEASURE ONE CHUNK, THEN LET IT ANSWER BOTH QUESTIONS ---------------------------------
	//
	// THE DEFECT THIS FIXES IS THE MISSING MIDDLE. Fan-out had exactly two states: serial, or
	// workers.size() wide. Nothing between, and the choice was made by an ITERATION COUNT that
	// never looks at the body -- so trivial work at N=256 pulled in 23 of 31 workers for about two
	// microseconds of total work, and heavy work at the same N was refused entirely. Measured, cap
	// off: heavy N=256 runs 6.9x and trivial N=256 runs 0.02x. Same N, same machine, same gate.
	// No value of minItersPerWorker serves both, because the difference is not in N.
	//
	// A LEAF IS NOT A PROBE IF YOU HAD TO RUN IT ANYWAY. The old body probe was removed in 1.4 for
	// costing more than it saved, and that reasoning does not reach this: the first chunk is work
	// the range is obliged to perform, so the measurement costs two clock reads and nothing else.
	// The result is W, and from W both answers follow -- whether to fan out at all, and how wide.
	//
	// WIDTH IS sqrt(W/c), NOT THE POOL. Time with k workers is about W/k plus k*c of ramp (the
	// wakes are issued serially by one thread), and that is minimised at k = sqrt(W/c). It gives
	// ~1 for trivial at N=256 -- correctly declining -- and ~7 for heavy at the same N, where the
	// old gate gave 0. Checked against the crossover rows: predicted k tracked observed speedup at
	// 70-75% efficiency across a 17x range of W.
	//
	// AN UNDER-ESTIMATE IS SAFE AND AN OVER-ESTIMATE IS NOT, so this is deliberately a LOWER BOUND.
	// A non-uniform body -- the back-loaded case -- makes the first chunk unrepresentative and W
	// too small, and the answer to that is not a better guess: range recruitment already widens on
	// evidence as expensive leaves actually complete. The probe starts the range at a defensible
	// width and recruitment corrects upward from measurements it did not have to predict. Neither
	// has to be right alone.
	//
	// minItersPerWorker IS NOW A FLOOR THIS CAN LIFT, not a veto. It still refuses ranges the probe
	// also declines, so nothing that was serial before becomes a 31-way fan-out by surprise; what
	// changes is that a range the probe proves expensive is no longer blocked by an iteration count.
	size_t fanWidth = workers.size();
	if (GetMeasuredWidth()) {
		const int len = end - begin;

		// ---- IF THIS BODY HAS BEEN MEASURED BEFORE, DECIDE NOW AND SKIP THE PROBE -------------
		//
		// The probe is serial time on the critical path. A call site that has already been measured
		// does not need to pay it again -- the previous call's leaves ARE the measurement, and they
		// were taken from work that had to happen either way. This is what turns "correct from the
		// second range" into "correct from the second range onward, for free".
		BodyCost* slot = GetRememberedCost() ? FindBodySlot(func.target_type().hash_code()) : nullptr;
		if (slot) {
			const long long nsPerMega = slot->nsPerMega.load(std::memory_order_relaxed);
			const unsigned  uses      = slot->uses.fetch_add(1, std::memory_order_relaxed);
			// Known, and not due for a refresh.
			if (nsPerMega > 0 && (uses % kReprobeInterval) != 0) {
				const long long W = (nsPerMega * (long long)len) / 1'000'000LL;
				const long long c = (long long)GetWakeCostNs();
				size_t k = (size_t)std::sqrt((double)W / (double)(c > 0 ? c : 1));
				if (k > workers.size()) k = workers.size();
				if (k < 2) { func(begin, end); return; }   // remembered as too cheap to fan out
				fanWidth = k;
				goto haveWidth;                            // no probe, no serial prologue at all
			}
		}
		// A BOUNDED FRACTION, NOT A FIXED COUNT, and getting this wrong cost heavy work 35%.
		// The probe runs SERIALLY on the caller before anything fans out, so it is Amdahl's serial
		// fraction and it must scale with the range. A flat kMinGrain=64 is 25% of a 256-item range
		// -- which caps speedup at 4x no matter how many workers arrive, and measured exactly that:
		// heavy N=256 fell 7.40x -> 4.89x purely to the probe. 1/32 keeps the serial share near 3%
		// at every size, capped at kMinGrain so a huge range does not probe a huge chunk.
		//
		// NOT sized by `grain`: that is a caller's guess and has not been floored yet, so the
		// measurement would depend on the number it exists to correct.
		// ESCALATE ONLY IF THE MEASUREMENT IS TOO SHORT TO TRUST. The probe is SERIAL and it runs
		// BEFORE anything fans out, so it sits directly on the critical path -- and that path is
		// short exactly when the body is expensive. Heavy at N=256 is ~154 us of work, so ideal
		// parallel time is ~5 us, against a flat len/32 probe of ~4.8 us: the measurement nearly
		// doubled the critical path, and cost 7.40x -> 6.36x.
		//
		// So start at ONE item and stop as soon as the reading clears the clock's noise. An
		// expensive body satisfies that on its first item and pays almost nothing; a cheap body
		// needs many items, but each is cheap, so the absolute cost stays bounded either way. The
		// len/32 cap remains the backstop for a body so cheap it never clears the floor -- at which
		// point W is tiny, k is below 2, and the range runs serially anyway.
		// len/32 IS THE ONLY CAP, and the absolute kMinGrain one that used to sit on top of it was
		// a mistake. The relative bound already holds the serial prologue near 3% at every size;
		// adding a 16-item ceiling meant a 0.5 ns/element body could never be measured at all --
		// the reading stayed under the resolution floor, the range was declared cheap, and trivial
		// at N=200000 lost a real 5.01x on 94.6 us of genuine work. Per-item cost being tiny does
		// not make W tiny, and W is what decides.
		//
		// A large range can therefore probe thousands of items and still spend ~2% doing it, which
		// is exactly what it takes to time a sub-nanosecond body. A small range caps out low, never
		// clears the floor, and is correctly judged too cheap.
		const int probeCap = (len / 32 < 1) ? 1 : (len / 32);
		int probed = 0;
		long long probeNs = 0;      // the LAST chunk only -- see below
		int probeLen = 0;           // ...and its length, which is what W is extrapolated from
		// ---- ONE RUNNING TIMESTAMP, AND PROPORTIONAL ESCALATION ------------------------------
		//
		// The probe's cost for a CHEAP body is not the items it runs, it is the CLOCK READS. At
		// trivial N=256 the range is ~120 ns of work and doubling took four chunks -- eight
		// MonotonicNs() calls -- which is why that cell sat at 0.50x. Two changes, neither of which
		// touches the expensive case:
		//
		//   ONE TIMESTAMP PER CHUNK, not two. Each chunk's duration is the delta since the previous
		//   reading, so N chunks cost N+1 reads instead of 2N. The gap now includes a few
		//   instructions of loop bookkeeping, which is more honest rather than less.
		//
		//   ESTIMATE THE NEXT SIZE INSTEAD OF DOUBLING. If a chunk read `d` for `take` items, then
		//   roughly take*floor/d items are needed to clear the floor -- so go there directly rather
		//   than through every power of two. A cheap body converges in about two measurements
		//   instead of four; an expensive one still stops after ONE item, because its first reading
		//   already clears the floor and the loop never escalates at all.
		//
		// NOT one-shot-when-probeCap-is-small, which was the obvious version and is wrong: probeCap
		// is 8 at N=256, and running 8 HEAVY items up front is the ~25% serial prologue that cost
		// 7.40x -> 4.89x before. Escalation from 1 is what protects the expensive case, and it has
		// to stay.
		long long tPrev = MonotonicNs();
		for (int chunk = 1; probed < probeCap; ) {
			int take = chunk;
			if (take > probeCap - probed) take = probeCap - probed;
			func(begin + probed, begin + probed + take);
			const long long tNow = MonotonicNs();
			const long long p0 = tPrev, p1 = tNow;
			tPrev = tNow;
			probed += take;
			// Next size: what this reading says it would take to clear the floor, clamped so a
			// pathological reading cannot collapse to zero or explode past the cap.
			const long long d = (p1 > p0) ? (p1 - p0) : 1;
			long long want = ((long long)take * kProbeFloorNs) / d;
			if (want < (long long)take * 2) want = (long long)take * 2;   // never shrink or stall
			if (want > (long long)probeCap)  want = probeCap;
			chunk = (int)want;
			// THE LAST CHUNK, NOT THE SUM, AND THIS IS NOT AN OPTIMISATION. Each step pays two
			// clock reads, so summing them accumulates ~6 readings' worth of timer overhead into a
			// figure that for a 0.5 ns/element body is almost entirely overhead -- which inflates W
			// and let trivial at N=2000 fan out to 28 workers for one microsecond of work (0.19x).
			// Each chunk is twice the last, so the final one has the best signal-to-overhead ratio
			// available and is the only reading worth dividing by.
			// THE LARGEST CHUNK, NOT THE LAST ONE. The escalation doubles until it hits probeCap
			// and then takes the REMAINDER, which is routinely SMALLER than the chunk before it --
			// so "last" threw away the best sample for a truncated one. Whether that remainder
			// landed large or small depends on where len/32 falls relative to a power of two,
			// which is arbitrary in N, and it made the width non-monotonic: light measured 0, 29,
			// 0, 0, 31 workers across increasing N, where more items can only mean more W.
			//
			// Largest take is deterministic and has the best signal-to-overhead ratio of any
			// reading taken, which is the whole reason for preferring one chunk over the sum.
			if (p1 > p0 && take >= probeLen) { probeNs = p1 - p0; probeLen = take; }
			if (probeNs >= kProbeFloorNs) break;   // enough signal to divide by
		}
		if (probeLen <= 0) { probeLen = probed > 0 ? probed : 1; }
		// ADVANCE BY `probed`, NOT `probeLen`. They diverged the moment probeLen became "the last
		// chunk" rather than "everything measured" -- the loop RAN `probed` items and advancing by
		// the smaller number would execute the difference a second time. A silent
		// wrong-answer bug, not a slow one: the body would see duplicate indices.
		begin += probed;
		if (begin >= end) return;                // the whole range was the probe

		// COULD NOT MEASURE IT => IT IS CHEAP. If the reading never cleared the resolution floor
		// even after exhausting the probe cap, the body costs less per item than this clock can
		// resolve -- and dividing a sub-resolution number produces a W made of timer noise. That is
		// what made trivial swing 0.03x .. 0.83x across identical runs and fan out to 24 workers at
		// N=1000, where the right answer is never to fan out at all.
		//
		// The inference is sound rather than a fallback: a body too cheap to time over kMinGrain
		// items is, by that fact, too cheap to be worth a wake. Refusing here is the same answer
		// the arithmetic would give if the arithmetic were trustworthy.
		if (probeNs < kProbeFloorNs) {
			func(begin, end);
			return;
		}
		const long long W = (probeNs * (long long)len) / (probeLen > 0 ? probeLen : 1);

		// REMEMBER IT, so the next range of this body starts hot rather than re-paying the probe.
		// EWMA weighted 3:1 toward history -- one unrepresentative reading (a cold cache, a
		// preempted measurement) should nudge the estimate, not replace it. The first sample seeds
		// outright, because averaging a correct first reading against 0 would halve it for nothing.
		if (slot) {
			const long long sample = (probeNs * 1'000'000LL) / (probeLen > 0 ? probeLen : 1);
			const long long prev = slot->nsPerMega.load(std::memory_order_relaxed);
			slot->nsPerMega.store(prev > 0 ? (prev * 3 + sample) / 4 : sample,
			                      std::memory_order_relaxed);
		}

		const long long c = (long long)GetWakeCostNs();
		size_t k = (size_t)std::sqrt((double)W / (double)(c > 0 ? c : 1));
		if (k > workers.size()) k = workers.size();
		if (k < 2) {
			// Not worth a single wake: run the remainder here. This is the trivial-body case, and
			// it is now refused because the BODY was measured, not because N was small.
			func(begin, end);
			return;
		}
		fanWidth = k;
	} else {
		const int minIters = (int)workers.size() * (int)kMinItersPerWorker;
		if (end - begin < minIters) {
			func(begin, end);
			return;
		}
	}
	// Reached directly by the remembered-cost path above, which has a width and no probe to run.
	// A forward jump over both branches, which is legal precisely because everything it skips is
	// scoped inside them -- fanWidth is declared before the branch and is the only value carried.
haveWidth:
	;

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
		// LEAVES PER WORKER. The splitter mints one Task per leaf, so this is the whole difference
		// between it and the cursor on a cheap body: at the bench.s grain of N/(workers*4) both paths
		// process 128 chunks, but the cursor pays 31 tasks + 128 atomics where the splitter pays 128
		// tasks. An atomic increment is ~1%% of a task. 64 never binds -- it only raises grain once N
		// exceeds workers*64 -- so the caller.s grain decides, and a caller cannot know what its own
		// body costs. Lowering this makes the splitter mint fewer, larger leaves: less dispatch, less
		// steal traffic, coarser load balancing. Runtime so it can be swept in one binary.
		// fanWidth, NOT workers.size(): this is where the measured width becomes a real breadth
		// rather than an opinion. Leaves are minted for the workers the range can justify, so a
		// range worth two workers is cut into two workers' worth of leaves instead of thirty-one.
		// With the measurement off, fanWidth IS workers.size() and this is the old expression.
		// ---- WIDTH AND GRANULARITY ARE DIFFERENT QUESTIONS -----------------------------------
		//
		// Sizing leaves by fanWidth conflated them and cost heavy work at small N (measured: N=512
		// 11.91x -> 8.46x). k answers "how many workers is this range worth waking", which is a
		// RECRUITMENT question. Leaf count answers "how finely must this be cut so whoever shows up
		// can balance", which is a LOAD-BALANCE question -- and stealing means the workers who show
		// up are not limited to k. Minting k*L leaves makes the grain coarser, so the tail balances
		// worse among the 31 workers that arrive anyway.
		//
		// TESTED, AND THE HYPOTHESIS WAS WRONG: sizing leaves by the pool instead of fanWidth did
		// NOT recover heavy at small N (6.21x vs 6.36x at N=256, against 7.40x uncapped), so leaf
		// granularity was not what cost it -- the serial probe on the critical path was. Leaves are
		// therefore sized by fanWidth after all, which is the version that also narrows cheap
		// ranges instead of minting 31 workers' worth of leaves for two microseconds of work.
		const size_t maxLeaves = fanWidth * GetLeavesPerWorker();
		const int floorGrain = (int)(((size_t)(end - begin) + maxLeaves - 1) / maxLeaves);
		grain = std::max(grain, floorGrain);

		// ---- AND AN ABSOLUTE FLOOR ON THE LEAF ---------------------------------------------
		//
		// maxLeaves bounds leaves RELATIVE to N, so it never stops a small range from splitting to
		// grain 1: at N=256 it permits 256 leaves, and the caller's grain decides. That is how a
		// 0.12 us range came to mint 124 tasks. A leaf below this many iterations cannot repay its
		// own CreateTask + push + steal + Execute + Destroy no matter what the body is, so the floor
		// is absolute rather than proportional.
		//
		// Callers keep the right to ask for MORE parallelism than this by passing a large range;
		// what they lose is the ability to ask for leaves too small to pay for themselves, which
		// was never a useful request.
		grain = std::max(grain, (int)kMinGrain);
	}

	// A NON-WORKER needs the shared lane; a worker already has one. Losing the race means another
	// app thread is mid-split, so fall back to the cursor path rather than serialising behind it --
	// a perfectly good answer, just not this one.
	const bool isWorker = (Thread::GetCurrent() != nullptr);
	NonWorkerLaneClaim claim(nonWorkerLaneClaimed, /*attempt=*/!isWorker);
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

	// ---- CLEAR ON THE WAY OUT, NOT ONLY ON AN EMPTY POP ---------------------------------------
	//
	// The clear inside the loop is unreachable on the path that matters. If WE pop the last task,
	// executing it drops the WaitGroup to zero and the `continue` re-tests the loop condition, which
	// is now false -- so we leave without ever reaching it. Only the case where a THIEF took the last
	// task falls through to the empty branch and clears. That is a coin flip, and it is the whole
	// intermittency.
	//
	// WHAT A LEAKED BIT COSTS, and it is not a stale diagnostic. The non-worker lane has no worker
	// looping over it, so nothing else ever retires the bit. It is counted in advertisedCount, and
	// advertisedCount == 0 gates BOTH the park and the CollapseAwakeFloorToBase call site -- so one
	// leaked bit means no worker in the pool may ever park again and the awake floor can never shed.
	// Captured: `parallel=0x0000000080000000` (bit 31, the non-worker lane, on a 31-worker pool),
	// `advertised queues = 1 -> workers CANNOT park`, floor stuck at 17 through a 25 ms idle settle
	// with ZERO collapse calls in that window.
	//
	// Unconditional and idempotent: ClearParallelHintIfEmpty already no-ops for an out-of-range index.
	if (myLaneIndex != SIZE_MAX) ClearParallelHintIfEmpty(myLaneIndex, 0);
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

	// GlobalFiberPool now owns all fibers and stack allocation
	globalPool = GlobalFiberPool::Create(standardFiberCount);
	// Raw Thread*: delete before clearing. Normally empty here -- StartPool runs before any worker
	// exists -- but an Init after a Join finds the leftovers of the previous pool.
	for (Thread* w : workers) delete w;
	workers.clear();
	deques.clear();
	loPriInboxes.clear();
	resumedInboxes.clear();
	stealHintLane.store(0, std::memory_order_relaxed);
	hiPriInboxes.clear();
	workers.reserve(num_workers);
	// +1 for the NON-WORKER LANE (see nonWorkerLane's declaration). Only the deques get it --
	// inboxes stay worker-indexed, because nothing ever pushes to a
	// non-worker's inbox or pins a core to it.
	deques.reserve(num_workers + 1);
	loPriInboxes.reserve(num_workers);
	hiPriInboxes.reserve(num_workers);
	resumedInboxes.reserve(num_workers);
	// No park table here: each Thread owns its own park fiber, and `workers` is already the flat
	// array indexed by worker id. See Thread::GetFiber.

	// mainQ (used by PushMain/ProcessMainThread, e.g. TaskDAG main-affinity nodes) was NEVER
	// init()'d -- its default ctor leaves head_/tail_/stub_ uninitialized. Harmless as long as
	// nothing actually called PushMain, which nothing did until real work started routing
	// through it (TaskDAG::Fire's isMain branch): TaskMPSCQueue::append() then wrote through a
	// garbage head_/prev pointer -> write access violation. One-time init, same as every other
	// TaskMPSCQueue (see loPriInboxes/hiPriInboxes below).
	mainQ.init(&taskAllocator);

	for (unsigned int i = 0; i < num_workers; ++i) {
		deques.push_back(std::make_unique<TaskDeque>());
		// ONE DEQUE PER WORKER, NOT TWO. The lane's Chase-Lev ring used to be allocated here
		// alongside it -- 32,768 slots per worker, for a structure whose only purpose is letting
		// OTHER threads steal, on a queue no other thread was ever allowed to touch.
		loPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		hiPriInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		resumedInboxes.push_back(std::make_unique<TaskMPSCQueue>());
		loPriInboxes[i]->init(&taskAllocator);
		hiPriInboxes[i]->init(&taskAllocator);
		// init() is not optional -- mainQ's missing one wrote through a garbage head_ pointer.
		resumedInboxes[i]->init(&taskAllocator);
		// Diagnostic identity, so a growth-ceiling abort names the queue instead of leaving the
		// reader to work out which of 60-odd deques it was.
		deques[i]->SetOwnerTag(i, "deque");
	}
	// THE NON-WORKER LANE. One extra deque pair past the workers, owned by whichever non-worker
	// thread has claimed it (see nonWorkerLane / TryClaimNonWorkerLane). Built here rather than
	// lazily so its index is fixed for the whole life of the pool: the steal loop reads it on
	// every sweep and must never see a vector being grown underneath it -- the same race the
	// two-pass worker construction below exists to avoid.
	nonWorkerLane = num_workers;
	deques.push_back(std::make_unique<TaskDeque>());
	deques[nonWorkerLane]->SetOwnerTag(nonWorkerLane, "loPri/non-worker");
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
		// RAW new, and the vector holds a RAW Thread*. A shared_ptr bought reference counting for
		// an object with exactly one owner and a process-length lifetime, and cost an atomic
		// refcount plus a control-block indirection on every access to the table -- including the
		// wake path. Deleted in the two places the table is cleared, both of which already hold
		// poolMutex or run before any worker exists.
		Thread* worker = new Thread(*this);
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

	// ---- THE FLOOR BASE SCALES WITH THE MACHINE ---------------------------------------------
	//
	// Fbase = n <= 8 ? 1 : 2, and only when the application did not state one.
	//
	// EIGHT, NOT SIX. On an 8-thread box a base of 2 is a QUARTER of the machine that never parks,
	// and on a hyperthreaded one those two are often SMT siblings -- too much reserved heat for a
	// machine that also has an application thread. On a 4-core laptop a base of 2 is half the box.
	//
	// THE COST OF Fbase=1 IS ACCEPTED, NOT OVERLOOKED: the steer set is ONE BIT, so a long body on
	// q0 makes the next aimed push either wait on that worker or fall off the floor and buy a wake.
	// That is the F=1 tax and it is the right tax at 8. Note it does NOT interact with the yield
	// re-aim, which needed two floor cores to have somewhere to go: at F=1 the floor never yields at
	// all (kYieldFloorMin). IF p50 FALLS APART ON A REAL 8-THREAD BOX THE FIX IS A PLACEMENT
	// FALLBACK when the single floor worker is busy -- not putting Fbase back to 2. WIDE IS HOW A SMALL POOL USES EVERYONE: it wakes the crowd once for one wave
	// and they all park after, which is far cheaper there than keeping cores off WaitOnAddress.
	// The band word is initialised statically, before the pool size exists, so this is the first
	// point where the question can even be asked.
	if (!g_awakeFloorBaseExplicit.load(std::memory_order_relaxed)) {
		const size_t n = workers.size();
		const size_t fb = (n <= 8) ? 1u : 2u;
		BandsSetFb(fb);
		BandsSetF(fb, n);
	}

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

	// AFTER the workers exist, because the cell table is sized from the fiber pool and the worker
	// count. Building it earlier bakes in a table of zero readers and every later poolIndex is out of
	// range -- see HazardDomain::EnsureTable, which refuses to build before the pool for that reason.
	HazardDomain::Instance().Init();
	for (auto& w : workers) {
		while (!w->Ready())
			std::this_thread::yield();
	}
	thread_id = thread_counter.fetch_add(1);
	poolActive.store(true, std::memory_order_release);

	// BUILT HERE, not lazily, because lazily is a race with this very function. The hazard table is
	// sized from the fiber pool and the worker count -- and a worker that reaches its sleep path
	// flushes its retire bag, which lands in the domain, WHILE this is still running. Left to build
	// on first touch it would bake in a table of zero fibers, and every poolIndex afterwards would
	// be out of range. Workers are up and Ready() by this point, so the sizes are final.

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

// SPIN A LITTLE BEFORE GIVING THE CORE AWAY. Used by every BARE-THREAD wait loop.
//
// THE MEASUREMENT THAT DEMANDED IT. A pool dump taken during a 664 us round trip -- against a
// healthy 2.2 us -- showed all 31 workers AWAKE, busy=0, and every queue in the pool empty. The
// task had already run. Nothing was stuck. The waiter was simply not being scheduled.
//
// WHY: the bare wait loop called std::this_thread::yield() on every unproductive pass, and on
// Windows that is SwitchToThread() -- it hands the core to another READY thread. With 31 spinning
// workers there are always 31 ready threads, so main gave its core away and went to the back of the
// queue, for a completion that had already happened. It was not waiting on the scheduler; it was
// waiting on the OS to give back the core it had just volunteered.
//
// So: pause-spin first, and only yield once the wait is long enough that holding a core is the
// worse deal. A completion that lands within the spin window is observed IMMEDIATELY -- which is
// the entire round trip in the common case.
//
// THIS IS NOT THE REJECTED "spin before park". That one was about the WORKER idle path, where the
// decision is deliberately binary because a worker holding a core is competing with the whole pool
// indefinitely. This is a waiter that has one specific event coming, usually within a microsecond,
// and the alternative is not parking -- it is yielding into a saturated run queue.
//
// THE COUNTER IS THE CALLER'S, so each wait starts fresh: a loop that spun 10,000 times on its last
// wait must not yield immediately on the next one.
// ---- WAS THE WAITER RUNNING? ---------------------------------------------------------------
//
// These three exist to answer ONE question, and it is a question no timing can answer: when a bare
// wait takes 50 us, was this thread spinning around the loop the whole time and repeatedly seeing
// `n > 0` -- or was it not executing at all?
//
// The two look identical from outside. Both produce a long wait with an idle pool afterwards, and
// a pool dump taken during one shows the same thing as a dump taken during the other, because
// workerState says what a worker has ADVERTISED, never whether its thread holds a core. Counting
// the loop separates them with no clock involved:
//
//     polls HIGH  -> the waiter ran, looked, and kept finding n > 0. The completion really was
//                    outstanding, and the pool owns the delay.
//     polls LOW   -> the waiter was descheduled. Nothing was missed and nothing was late; this
//                    thread simply was not on a CPU to notice. The pool is exonerated.
//
// A COUNT, NOT A CLOCK, and deliberately: reading a clock in here would put a serialising
// instruction in the loop whose scheduling behaviour is the thing under measurement. The counters
// are thread_local and non-atomic -- only this thread writes them and only this thread reads them,
// after its own wait has returned.
//
// `helped` is separate from `polls` because a bare waiter that runs a stolen task is not waiting at
// all for that stretch: it is the pool, briefly. A round trip where helped > 0 measured something
// other than dispatch, and reading it as dispatch latency is a mistake worth being able to catch.
static thread_local unsigned t_bareWaitPolls  = 0;
static thread_local unsigned t_bareWaitYields = 0;
static thread_local unsigned t_bareWaitHelped = 0;

static inline void BareWaitBackoff(unsigned& spins) noexcept {
	// ~1 us of pause at a few hundred picoseconds each, then hand the core over. Short enough that
	// a genuinely long wait still yields promptly, long enough to cover a dispatch round trip.
	constexpr unsigned kSpinBeforeYield = 512;
	if (spins < kSpinBeforeYield) { ++spins; platform::CpuRelax(); }
	else                          { spins = 0; ++t_bareWaitYields; std::this_thread::yield(); }
}

unsigned TaskScheduler::LastBareWaitPolls()  noexcept { return t_bareWaitPolls;  }
unsigned TaskScheduler::LastBareWaitYields() noexcept { return t_bareWaitYields; }
unsigned TaskScheduler::LastBareWaitHelped() noexcept { return t_bareWaitHelped; }
void TaskScheduler::WaitFor(WaitGroup& wg) {
	auto thread = Thread::GetCurrent();
	Fiber* current = (thread != nullptr) ? thread->currentFiber : nullptr;

	if (current != nullptr) {
		// ---- DIRECT WAIT: no DirectEvent, no mutex, no allocation ---------------------------
		//
		// Ordering copied from WaitOnEventDirectArmed, whose comment is the specification. Do not
		// reorder these four steps; each one is a lost wakeup if it moves.
		//
		//   1. become parkable FIRST. Publishing before this lets a completing task Resume() a
		//      fiber that is still RUNNING -- Resume() is a no-op for that state, so the wake is
		//      dropped and the fiber parks forever on a finished group.
		//   2. publish onto the stack, so a signal landing now has a resumable target.
		//   3. arm: set WAITER_BIT and re-read the count. The group may have finished between 1
		//      and here, in which case nobody else will ever call WakeAll for us.
		//   4. suspend by ContextSwitch, NOT Fiber::Suspend() -- Suspend() re-stores
		//      WANTS_SUSPEND and would erase a SUSPEND_SIGNALED that step 3 just produced.
		current->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);

		Fiber* head = wg.directWaiters.load(std::memory_order_relaxed);
		do {
			current->nextWaiter = head;
		} while (!wg.directWaiters.compare_exchange_weak(
					head, current, std::memory_order_acq_rel, std::memory_order_relaxed));

		const int old = wg.n.fetch_or(WaitGroup::WAITER_BIT, std::memory_order_acq_rel);
		if ((old & WaitGroup::COUNT_MASK) == 0) {
			// ALREADY FINISHED. Every task decremented before we published, so no completion is
			// left to wake us -- we have to drain the stack ourselves. Draining ALL of it rather
			// than just our own entry is correct and is not overreach: the group is done, so every
			// waiter on it is owed a wake, and the exchange makes double-draining harmless.
			wg.WakeAllDirect();
		}

		JLIB_EPOCH_CHECK_NO_GUARD("TaskScheduler::WaitFor");
		ContextSwitch(&current->ctx, current->homeCtx);
		return;
	}

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
		unsigned waitSpins = 0;   // per-wait, so a long previous wait cannot make this one yield early
		// RESET PER WAIT, like waitSpins and for the same reason: these describe THIS wait. A caller
		// reads them after the wait returns, so they must not carry the previous one's total.
		t_bareWaitPolls = t_bareWaitYields = t_bareWaitHelped = 0;
		while (wg.n.load(std::memory_order_acquire) > 0) {
			++t_bareWaitPolls;
			bool ranSomething = false;
			if (t_heldMutexes == 0) {
				++t_spinHelpDepth;
				ranSomething = TryRunStolenNativeTask();
				--t_spinHelpDepth;
			}
			if (!ranSomething)
				BareWaitBackoff(waitSpins);
			else
				++t_bareWaitHelped;
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
	unsigned waitSpins = 0;   // per-wait; see BareWaitBackoff
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
			BareWaitBackoff(waitSpins);
	}
	return WaitResult::Ok;
}

void JLib::TaskScheduler::PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity, size_t minPerSegment,
	bool hiPri, CorePref pref)
{
	if (!tasks || count == 0) return;

	// ONCE PER SUBMIT BATCH, BEFORE PLACEMENT. A batch is the one caller that KNOWS a wave of
	// `count` tasks arrived at the same instant -- a single Push cannot know that, which is why the
	// per-push path only moves the floor one step at a time. Growing here means the placement that
	// follows already has the wider awake set to spread across, instead of discovering it one task
	// too late and piling the whole wave onto the two workers that were awake when it started.
	NoteFloorCrowding(count);

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
		if (useHi) SetHiPriHint((size_t)chosen);   // so a thief knows to probe this hiPri deque
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
	// ---- AT K = 0 THE LANE MUST BE SCANNED HERE, OR IT STRANDS ------------------------------
	//
	// With no reserved band there is no owner obliged to drain a hiPri deque, so a helper that
	// skips the lane leaves whatever is in it unreachable. This loop had been reduced to a bare
	// rotation with an EMPTY BODY -- scanning nothing -- which was survivable only because the
	// owning worker still popped its own hiPri deque. Restored with a body.
	// NO LANE STEAL FROM HERE, and that is how it shipped: this loop existed with an EMPTY BODY,
	// rotating over the lanes and taking nothing. I gave it a body while "restoring" it, which is a
	// behaviour change invented during a revert -- helpers would then pull lane work at K=0 where
	// they never had. Put back as it was.
	size_t numThreads = deques.size();
	// ONE rand() FOR BOTH SCANS. It is a libc call that takes a lock -- measured as most of the
	// cost of a cheap ParallelFor from main, which is why the bare-caller wait avoids this function
	// entirely -- so a second draw for the hiPri pass would double the only expensive part of it.
	size_t start = rand() % numThreads;

	// ---- hiPri DEQUES FIRST. THIS IS WHAT MAKES LANE WORK RESCUABLE ---------------------------
	//
	// The lane inbox cannot be stolen from and never will be: an MPSC has one legal consumer. The
	// deque is the structure that lets anybody reach lane work whose owner has stopped coming back
	// for it, which is the entire reason it was restored -- see the member declaration.
	//
	// BEFORE loPri, because a task staged here is still lane work that something is waiting on.
	// Staging it did not demote it.
	//
	// BOUNDED BY THE FAIRNESS WINDOW that was left behind when the old lane steal was removed --
	// `forceLoPri` and `consecutiveHiPriSteals` were still here, computed and then discarded via
	// `(void)forceLoPri`. A thief that has taken kStealFairnessWindow lane tasks in a row skips
	// this pass, so a saturated lane cannot monopolise every thief and starve bulk work.
	// GATED ON THE LANE HINT, like the other deques. Blind probing is what made stealing useless
	// before hints existed -- hit rate 0.2-0.9%, against 72-90% once thieves only probed queues
	// that had advertised. A CAS per victim per pass, nearly all of them failing, is not a cheap
	// way to find nothing.
	//
	// The bitmap is one 64-bit word, so a worker at index >= 64 has no bit to set. Those are probed
	// UNCONDITIONALLY rather than skipped: an absent bit must mean "unknown", never "empty", or the
	// backlog on worker 64 would be advertised to nobody and stranded exactly the way this whole
	// change exists to prevent.
	// THE LANE SWEEP THAT WAS HERE IS GONE WITH THE LANE DEQUE. It walked hiPri[] under the lane
	// bitmap and took one with steal_if. There is no hiPri[] now: the lane is an MPSC inbox, which
	// has exactly one legal consumer, so nothing outside the owner can take from it at any price.
	//
	// The fairness counter it fed goes with it. `consecutiveHiPriSteals` existed so a saturated
	// lane could not monopolise every thief and starve bulk work -- a hazard that required lane
	// work to be stealable in the first place.
	(void)forceLoPri;
	consecutiveHiPriSteals = 0;
	for (size_t i = 0; i < numThreads; ++i) {
		size_t target = (start + i) % numThreads;
		if (auto s = deques[target]->steal_if(fiberlessRunnable))
			return *s;
	}

	return nullptr;
}

bool TaskScheduler::TryRunStolenNativeTask() {
	// ---- DISABLED FOR THE PURE-FIBER REFACTOR --------------------------------------------------
	//
	// IT CANNOT SUCCEED ANY MORE, AND FAILING IS EXPENSIVE. GetTask() vets candidates with
	// `fiberlessRunnable`, which rejects TaskType::Fiber outright. In a workload where every task
	// is fiber-backed there is nothing this is permitted to claim -- so every call walks steal
	// candidates across the whole pool, touches remote deque endpoints, and returns false. Callers
	// then yield and do it again.
	//
	// THAT IS THE 664 us ROUND TRIP. The bare-thread WaitFor loop is
	//     while (n > 0) { if (!TryRunStolenNativeTask()) yield(); }
	// so a main thread waiting on ONE task ran a full O(N) pool scan per iteration, found nothing
	// it could ever run, and yielded -- against 31 spinning workers competing for the same cores.
	// The pool dump was consistent with exactly this and not with a dispatch failure: every worker
	// AWAKE, every queue EMPTY, and the work not progressing. There was no work to find. The task
	// had already run; the waiter was busy failing to help.
	//
	// It is also why the primitives tests hang rather than fail: they wait on fiber tasks from a
	// bare thread, so the help loop can never terminate through the help path.
	//
	// A CHEAP EARLY-OUT, NOT A BLANKET DISABLE. This returned false unconditionally for a while and
	// that was too broad: it is true that under a FIBER-ONLY workload nothing here can ever be
	// claimed, but ParallelFor's split tasks are NATIVE, and its main-participation path is
	//     if (!TryRunStolenNativeTask()) std::this_thread::yield();
	// so disabling this left main publishing splits and then yielding in a loop while its own work
	// sat waiting for a worker to steal it. That is the ParallelFor regression.
	//
	// THE GUARD IS THE HINT BITMAP: at most kHintWords relaxed word loads to answer "is ANY queue
	// advertising work". When nothing is, this returns without touching a single deque -- which is
	// the whole cost that mattered. The expensive shape was a bare waiter walking steal candidates
	// across the pool on EVERY iteration of its wait loop, touching remote endpoints, to find
	// something it was not permitted to take.
	//
	// IT DOES NOT CHECK WHETHER THE ADVERTISED WORK IS RUNNABLE BY US, deliberately. That answer
	// lives in the deque tag and cannot be read without a steal attempt. A fiber-only workload
	// therefore still pays one probe per call rather than none -- bounded, and far from the O(N)
	// sweep it replaces.
	// ---- THE OWNER'S OWN LANE, BEFORE THE STEAL GUARD ----------------------------------------
	//
	// ABOVE the hint check on purpose, and this placement is the whole fix. The guard below asks
	// "is anything STEALABLE anywhere" -- and lane work never is: an MPSC inbox has exactly one
	// legal consumer, so it is not in the steal hints and never can be. A blocked owner whose own
	// inbox holds work therefore sees `advertised == 0`, returns immediately, and never looks at
	// the one queue only it may drain. Measured exactly that way: q0 AWAKE, busy=1, hi=1, every
	// other worker idle, `advertised queues = 0`, pool deadlocked
	// (tests/lane_reachability_test.cpp phase 1 -- it failed with this code sitting below the
	// guard, which is why the test was written first).
	//
	// This is NOT giving spin-help a queue. The owner is right here, spinning in WaitFor a few
	// frames up, and it is the only thread permitted to touch this inbox -- it looking at its own
	// queue is the one legal way that work can move. ONE task per call, so nothing moves in bulk
	// and lane order is preserved; the caller loops, so depth N clears in N iterations.
	//
	// Non-workers fall through untouched: GetCurrent() is null for them, they own no inbox, and
	// they are not part of the hazard.
	Thread* laneOwner = Thread::GetCurrent();
	Task*   laneTask  = nullptr;
	if (laneOwner) {
		bool laneRelocated = false;
		laneTask = laneOwner->TryTakeLaneTask(laneRelocated);
		// A relocated fiber task is in a deque now and every worker that could steal it may be
		// parked -- same argument as the drain's NotifyAll below. Only the rare fiber-on-lane case.
		if (laneRelocated) NotifyAll();
	}

	if (!laneTask) {
		const size_t nq = deques.size();
		if (nq <= kMaxHintQueues) {
			size_t nWords = (nq + 63) / 64;
			if (nWords > kHintWords) nWords = kHintWords;
			unsigned long long any = 0;
			for (size_t w = 0; w < nWords; ++w) any |= StealHintWord(w);
			if (!any) return false;
		}
	}

	// Steals ONE task this fiberless caller can actually run and runs it right here. GetTask vets the
	// type AT THE DEQUE (steal_if) -- a fiber-backed task is never claimed in the first place, so the
	// old steal-then-Requeue relocation path (claim CAS + re-push + notify = contention/thrash) no
	// longer exists.
	//
	// "Native" IN THE NAME IS NOW NARROWER THAN THE BEHAVIOUR: since 2.8.0 this also steals
	// TaskType::Coroutine, because resuming a coroutine is a function call on the current stack and
	// needs no fiber. Only Fiber-backed tasks are off limits. The name is kept rather than churned
	// because it is public API; read it as "a task that does not require a fiber".
	// ---- A LANE WORKER DOES NOT STEAL. IT DRAINS ITS OWN INBOX AND OTHERWISE WAITS. -----------
	//
	// A HOT WORKER REACHES THIS FUNCTION, because OnBareThread() is true for a worker running a
	// Native OR Coroutine task -- neither takes a fiber -- so a lane task that blocks in WaitFor,
	// SchedulerMutex or a condition variable helps through here. GetTask() steals BULK work, which
	// is exactly what "hot workers never steal" exists to prevent.
	//
	// THIS USED TO ALLOW THE STEAL, on the argument that refusing would "trade a policy violation
	// for a hang". THAT ARGUMENT WAS WRONG, and specifically it overstated the risk. Refusing to
	// steal does not hang: whatever a blocked lane task is waiting on is still runnable by the
	// other N-K workers, and they run it, and the lane resumes. What refusing costs is the K lane
	// threads, for as long as the rest of the pool takes to clear the dependency. A real hang needs
	// the REST OF THE POOL blocked too, and at that point the lane's steal policy is not what is
	// killing you. The old code paid a permanent policy violation to insure against a scenario a
	// narrower reading does not reach.
	//
	// THE OWN-INBOX DRAIN IS STILL LOAD-BEARING and is deliberately NOT gated: a task in this
	// worker's inbox has exactly one legal consumer, so nobody else can rescue it, and skipping
	// that drain IS a genuine deterministic deadlock. "Never steals" and "always drains its own
	// inbox" are compatible, and the split between them is the whole point.
	//
	// SO A BLOCKING LANE TASK NOW STALLS THE LANE, LOUDLY, and that is the intended failure. It is
	// a CONTRACT violation, not a workload condition -- lane work must be short and non-blocking --
	// and for a contract violation a visible stall beats silent degradation. The old behaviour
	// produced a program that worked, with a reserved core quietly running bulk and a lane whose
	// p99 was wrong for reasons nothing reported. That surfaces months later as "the I/O latency
	// got worse and we don't know when". A stalled lane is found in minutes. See the debug assert
	// in the blocking primitives, which names the offending call site instead of making you infer
	// it from a stack dump.
	//
	// The lane exists only because an app asked for it -- EnableIoReactor implies it -- so the
	// workload is I/O completions: bounded, characterised work with known durations. A contract is
	// enforceable when you can describe the workload it applies to.
	//
	// READING BANDS HERE IS SAFE, unlike on the notify side ([[band-skip-lost-wake]]). A stale read
	// is benign in both directions: stale "reserved" refuses a steal for one pass and spins again,
	// stale "not reserved" takes one bulk task, which is what the old code did unconditionally.
	// Nothing parks on this decision, so no wake can be lost by it.
	Task* task = laneTask;
	if (!task) {
		const bool laneWorker = laneOwner && laneOwner->qIndex >= 0
		                     && (size_t)laneOwner->qIndex < GetBands().k;
		if (!laneWorker) {
			task = GetTask();
		}
#ifndef NDEBUG
		else {
			// ---- THE CONTRACT VIOLATION, NAMED ONCE ------------------------------------------
			//
			// Reaching here means all of: this is a reserved worker, it is inside a blocking
			// primitive (that is the only way into this function), and its own lane inbox is
			// empty. So a lane task is blocked on something no lane drain can supply -- exactly
			// what "lane work is short and non-blocking" forbids. There is no legitimate path
			// that produces this combination, so it cannot false-positive.
			//
			// A MESSAGE, NOT AN abort(). The stall IS the intended failure and the program should
			// still reach it; aborting would replace the behaviour under test with a different
			// one. This only removes the need to infer the cause from a dump of parked threads.
			//
			// ONE-SHOT, because a blocked lane task calls its primitive in a spin loop and would
			// otherwise emit thousands of identical lines and bury the first.
			static std::atomic<bool> warned{ false };
			bool expected = false;
			if (warned.compare_exchange_strong(expected, true, std::memory_order_relaxed))
				fprintf(stderr,
				        "[JLib::Scheduler] LANE CONTRACT VIOLATED: reserved worker q%d is BLOCKED "
				        "inside a task.\n  Lane work must be short and non-blocking; a lane task "
				        "must not call WaitFor, SchedulerMutex\n  or a condition variable. The "
				        "lane will stall until the rest of the pool clears what it waits on.\n"
				        "  Break on this line to find the blocking call.\n",
				        laneOwner->qIndex);
		}
#endif
	}
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
// ONE PARAMETER, because there is one stack class. This took (standard, heavy); the heavy class is
// gone -- see GlobalFiberPool -- and a second argument that silently did nothing would be the same
// trap Task::requiredSize was.
void TaskScheduler::SetFiberBudget(size_t fibersPerWorker) {
	g_standardFibersPerWorker = fibersPerWorker;
}
size_t TaskScheduler::StandardFibersPerWorker() { return g_standardFibersPerWorker; }

// DEFAULT false -- pinned, which is what every release through 5.0 shipped. Migration is opt-in
// rather than the new default because it changes a contract users already build against, and a
// scheduler that silently starts resuming elsewhere would break exactly the code that was relying
// on it not doing so, with no diagnostic.
static bool g_migratableFibers = false;
void TaskScheduler::SetMigratableFibers(bool on) {
	// THE CREDITOR MASK MUST COVER EVERY ADDRESSABLE WORKER. If it does not, NoteCreditor refuses a
	// high-numbered worker -- correctly, since wrapping would bill the wrong one -- and that
	// worker's cleanup is dropped instead. Silent, and only on machines wide enough to reach the
	// gap, which is the worst combination.
	//
	// ASSERTED HERE, INSIDE A MEMBER BODY, and the two failed placements are worth recording: at
	// class scope it cannot see kMaxHintQueues (declared further down, and a class-scope
	// static_assert is not a complete-class context), and at namespace scope in this file it cannot
	// ACCESS it (private). A member function body is both.
	static_assert(Fiber::kCreditorWords * 64 >= kMaxHintQueues,
		"Fiber::kCreditorWords is too narrow for kMaxHintQueues workers.");
	g_migratableFibers = on;
}

bool TaskScheduler::PushResume(size_t worker, Task* task) {
	if (!task) return false;
	TaskScheduler* s = instance;
	// NOT Instance(): that throws, and a cleanup push racing teardown is an ordinary outcome, not
	// an exceptional one. The caller gets false and decides.
	if (!s || !s->poolActive) return false;
	if (worker >= s->resumedInboxes.size()) return false;
	s->resumedInboxes[worker]->push(task);
	// WAKE THE OWNER. Nothing else will: this queue has exactly one legal consumer, so a parked
	// worker with a cleanup job in its inbox and no notification stays parked and the resource is
	// never given back. Same lost-wake shape as the band-skip fix.
	//
	// PUSH FIRST, NOTIFY SECOND. The reverse loses the wake outright -- the target can observe an
	// empty inbox, eat the permit, and park with the task arriving just behind it.
	if (worker < s->workers.size() && s->workers[worker])
		s->workers[worker]->NotifyWorker();
	return true;
}
bool TaskScheduler::MigratableFibers() { return g_migratableFibers; }

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

Task* TaskScheduler::CreateTaskImpl(void(*fn)(void*), void* data, uint8_t hipri, TaskType type, CorePref corePref) {
	void* mem = taskAllocator.AllocSized(sizeof(Task));   // a bare Task is exactly 64 B
	if (!mem) return nullptr;
	detail::RecordTaskSize(sizeof(Task));   // exactly 64 -- a quarter of the slot it just took
	Task* t = ::new (mem) Task(fn, data, hipri);
	t->type = type;
	t->corePref = corePref;
	// Concrete type is exactly Task, whose destructor is empty -- the completion path can skip the
	// virtual call entirely. See Task::trivialDtor.
	t->trivialDtor = 1;
	return t;
}

bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task) return false;

	// BEFORE THE PUSH, not after: the point is to slow this producer down before it adds to a
	// backlog, and a no-op unless a submit limit is set. Non-worker callers only -- see the
	// declaration for why a worker can never be held here.
	ApplyIngressBackpressure();

	size_t num_workers = workers.size();
	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);
		// EXPLICIT AFFINITY NOW ALWAYS SUCCEEDS. It used to be refusable -- if that core was pinned
		// by PushImmediate this returned false and the caller had to cope. With pinning gone
		// (4.0.1) there is no way for a worker to be unavailable, so the refusal path and the
		// retry loops that danced around it are unreachable and removed. See SetReservedCores.

		// AFFINITY DOES NOT DEMOTE. This branch pushed to loPriInboxes unconditionally, so
		// Push(affinity, hiPriTask) silently landed lane work on the ordinary deque -- the exact
		// priority inversion PushBatch's submitRun refuses by name ("no caller could see" it), and a
		// direct contradiction of the contract on Task::hiPri, which lists PushLocal among the paths
		// that route on it. Nothing reported it: the task still ran, just never on the lane, so the
		// lane simply never filled and its hint bit never set.
		//
		// Found by a reachability test that pinned hiPri work to one worker with affinity and then
		// could not explain why stealHintLane stayed 0x0.
		//
		// SAME PREDICATE AS EVERY OTHER PUSH PATH, deliberately: at K=0 nobody probes hiPri, so a
		// task routed to the lane would never run at all -- collapsing to loPri is what makes hiPri
		// free rather than dangerous when the lane is off.
		const bool useHi = task->hiPri && HiPriLaneActive();
		(useHi ? hiPriInboxes : loPriInboxes)[idx]->push(task);
		if (useHi) SetHiPriHint((size_t)idx);
		// EVERY inbox enqueue accounts for depth, including this one. Missing it here is what made
		// the growth gate dead in a long-running process: the drain decrements whatever it pops, so
		// a push path that does not increment walks the counter negative one task at a time. After
		// the throughput rows the bench reached the burst with a deeply negative depth, `depth > 4`
		// could never be true, and growth never ran -- while the same code grew to the cap on a
		// freshly started pool. The gate was not wrong; its input had been drifting for 200,000
		// tasks. See the self-heal in Worker()'s drain for the other half of the fix.
		workers[idx]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
		NoteInboxPush(1);
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
		// HiPriLaneActive(), NOT hotN. This site still spelled the gate as "K > 0", so with K stubbed it
		// silently collapsed every hiPri push to loPri -- placement was never even asked for a reserved
		// worker. Measured: 0 of 300 hiPri completions ran inside the reserved range while this stood.
		// Two spellings of one predicate is one too many; there are none left.
		const bool useHi = task->hiPri && HiPriLaneActive();
		uint8_t chosen = (uint8_t)PickNextWorker(task->corePref, useHi);

		// ---- DO NOT COMMIT ORDINARY WORK TO A WORKER THAT CANNOT REACH IT -------------------
		//
		// PickNextWorker already prefers workers that are not away, and returns one anyway when
		// every candidate is: it has to return something. This is the "something" -- a destination
		// ANY worker can take it from, rather than an inbox whose sole consumer is inside a task
		// body of unknown length.
		//
		// WHY THE PUSH SIDE AND NOT A BETTER PICK. There is no better pick to make: when the
		// un-away set is empty the information placement needs does not exist, and the failure is
		// not "chose badly" but "committed at all". Deferring the choice until a worker is free is
		// strictly better than guessing which body finishes first -- and it also closes the race,
		// because a worker that goes away between the pick and this check is caught here.
		//
		// LANE WORK IS EXCLUDED. A hiPri task belongs to the reserved band by construction, and a
		// reserved worker never drains this queue -- diverting it here would take a completion off
		// the lane it was routed to, which is the one thing the lane exists to prevent.
		//
		// COLD BY CONSTRUCTION. In the common case some worker is not away, so this branch is one
		// predictable test of a word the pusher just used.
		// AN ESCAPE-QUEUE DIVERSION SAT HERE AND WAS REVERTED -- see UpdateBacklogHint in the
		// header for the numbers. Work whose chosen worker was inside a task body went to a shared
		// MPMC queue any worker could drain, which is correct and cost too much: the trigger has to
		// mean "busy for a LONG time", and neither available signal does. `busy` means busy at all,
		// so under load nearly every push diverted and the shared queue became the main path (frame
		// DAG 8.45 -> 40.80 us/graph); a dedicated away bitmap means two atomic RMWs per task on one
		// shared word (8.45 -> 35.82). Telling a short body from a long one needs a probe, which is
		// the thing this scheduler refuses on principle.

		// THE PROMOTE SIGNAL LIVES HERE, and nowhere else, because this is the only place a bigger
		// floor could have changed the outcome. PLACEMENT chose this worker; if it is parked, the
		// push buys an OS wake that one more awake worker would have avoided.
		//
		// IT WAS IN NotifyWorker AND THAT COUNTED TWO THINGS IT MUST NOT:
		//   - FORCED wakes, which are the scheduler's own (SetAwakeFloor waking
		//     the workers it just promoted). Each promotion manufactured the evidence for the next.
		//   - PINNED RESUMES. Requeue must send a resumed fiber to its homeWorker -- that is what
		//     pinning means -- so if that worker is asleep the miss is unavoidable, and a larger
		//     floor cannot prevent it. Counting it promoted forever against a cost it could not fix.
		//
		// Measured as a clean ratchet with both included: floor 2 -> 6 -> 12 -> 15 -> 21 across
		// consecutive runs, latency climbing with it, and the demote rule never once firing.
		// WS_PARKED SPECIFICALLY -- not "anything other than EMPTY". This once said the
		// latter and it was a false signal: under the OLD protocol a worker passed through
		// WS_GOING_TO_SLEEP on every idle pass, so a floor worker that never parked read as
		// "not awake" most times it was sampled. That state is GONE with the permit machine
		// -- the word now reads WS_EMPTY until an actual commit -- but the test stays
		// WS_PARKED-specific, because WS_NOTIFIED means a latched permit on a RUNNING
		// worker and counting that as a miss would reintroduce the same false signal.
		// back. So a floor worker that never parks at all reads as "not awake" most of the times it
		// is sampled, and every push to it counted a miss. The floor then promoted continuously
		// against a cost that was never paid -- floor 17 with the pool perfectly healthy.
		//
		// GOING_TO_SLEEP means "still running, deciding". Only SLEEPING costs a wake.

		// ---- LANE OVERFLOW, AND IT HAS TO HAPPEN HERE ----------------------------------------
		//
		// A hiPri push aimed at a worker that is inside a task body would queue behind it in an
		// MPSC that ONE thread may pop -- and that thread is not in Worker(), so it reaches neither
		// the lane pop nor any spill of its own. Nothing downstream can rescue that task; the push
		// is the last moment a definitely-running thread gets to choose. See HiPriSpillTarget.
		//
		// BEFORE THE ACCOUNTING BELOW, deliberately. Everything from here down -- the wake-miss
		// sample, inboxDepth, MarkQueuedWork, NotifyWorker -- is written in terms of `chosen`, so
		// redirecting after any of it would leave the counters describing one worker and the task
		// sitting on another. That is the same class of bug as a setter silently undoing an earlier
		// one, and it would show up as a floor controller promoting against wakes nobody paid.
		if (useHi) chosen = (uint8_t)HiPriSpillTarget((size_t)chosen);

		NotePush();
		// ---- REMEMBER WHERE THIS PUSH WENT, SO A STALL DUMP CAN POINT AT IT ------------------
		//
		// Mid-stall the watcher cannot tell WHICH worker the outstanding task is waiting on: the
		// task has not run, so nothing has recorded a landing index yet. That leaves the reader
		// correlating against thirty-one interleaved rows by hand -- and the row that mattered was
		// the one the paste happened to cut.
		//
		// AFTER the spill redirect, for the same reason NotePush sits here. Recording before it
		// would name a worker the task was moved off, which is the counters-describe-one-worker,
		// task-sits-on-another bug the comment above warns about.
		//
		// Relaxed, last-writer-wins: the serial latency row has one push in flight, which is the
		// only case this is read in. Under concurrent producers it names whichever pushed last, and
		// the dump labels it so rather than implying more than it knows.
		g_lastPushTarget.store((int)chosen, std::memory_order_relaxed);
		if (chosen < workers.size() && workers[chosen]
		    && workers[chosen]->GetWorkerState() == 2 /* WS_SLEEPING */)
			NoteWakeMiss();

		if (useHi) {
			// ---- A LANE MISS IS "THIS COMPLETION WILL QUEUE BEHIND ONE ALREADY LINKED" -------
			//
			// Asked against the MPSC we are about to push to, BEFORE the push, because there is no
			// second structure to consult -- the inbox IS the lane. It is not a statement about
			// deque-vs-inbox and it is not a depth: one waiter is the whole signal.
			//
			// UNDER-COUNTING IS THE SAFE DIRECTION AND IS DELIBERATE. A Vyukov queue can report
			// empty while a producer's `next` store is still in flight, so a genuine miss can read
			// as empty. That costs a promotion we do not make, which is the conservative failure --
			// the opposite error would grow K on a lane that was never behind.
			//
			// PUSH SIDE ONLY. Never from pop (the consumer draining is not evidence of pressure)
			// and never on loPri (this is the K controller's input; loPri belongs to F).
			// NoteLaneMiss(1) GOES HERE and is deliberately NOT wired yet -- the hook is
			// `if (!hiPriInboxes[chosen]->empty()) NoteLaneMiss(1);` immediately before the
			// push, asked of the MPSC being pushed to. It stays out until a bench with STATIC
			// K is green, because it is the FAST promote input and arming a controller input
			// while the static path is still being validated makes the two failures
			// indistinguishable.
			hiPriInboxes[chosen]->push(task);
			SetHiPriHint((size_t)chosen);
		}
		else
			loPriInboxes[chosen]->push(task);   // collapsed: no lane, no server
		// DEPTH, not just "is there anything". The growth rule needs to tell a 16-task wave queued
		// behind two workers from a 6-node graph doing the same thing -- see NoteFloorCrowding.
		workers[chosen]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
		NoteInboxPush(1);
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
	// PINNED RESUME (unconditional since 4.0.2). A task that already HOLDS a fiber goes back to the worker
	// that fiber was bound on, into a queue nobody else drains -- not through PickNextWorker.
	//
	// This is the whole of pinning at the routing level. Everything else about it is bookkeeping.
	// A fiber carries a saved context that is thread-agnostic, but every `thread_local` it will
	// touch after resuming is not: TLS follows the thread and the fiber does not. Sending it to
	// "whichever worker is next" is what makes `thread_id` and `Thread::instance` disagree across a
	// suspend. See Fiber::homeWorker and resumedInboxes.
	//
	// THE hiPri LANE IS DELIBERATELY NOT CONSULTED HERE, and that is a real trade. A resumed hiPri
	// task would normally be routed back onto the lane so a hot worker serves it; pinned, it goes
	// to its home worker whether or not that worker is hot. The alternative -- honour the lane and
	// migrate -- reintroduces exactly the hazard this exists to remove, and a lane task that has
	// already started is no longer waiting on dispatch latency: it is waiting on whatever it
	// suspended for. Fresh lane work is unaffected, because a task that has not run holds no fiber
	// and never reaches this branch.
	{
		if (Fiber* f = task->assignedFiber) {
			const size_t home = f->homeWorker;
			// SIZE_MAX is "not bound", which a task holding a fiber should never be. Falling through
			// to the ordinary path is the safe answer rather than indexing on it: unpinned routing
			// is what shipped for every release before this one.
			// AND THE HOME WORKER MUST STILL BE THERE. Join() stops workers one at a time, so
			// during teardown some are gone while others are still draining parked frames. A
			// resume pinned to a departed worker lands in a queue nobody will ever pop -- and
			// because that frame is being drained precisely so it can UNWIND, stranding it means
			// its destructors never run, its WaitGroup slot is never released and its hazard record
			// is never returned. Silent, and only at shutdown: 4 runs in 10 of the teardown test.
			//
			// Falling back to ordinary placement re-homes it to a live worker. That is a migration,
			// which pinning exists to prevent -- accepted here because the alternative is a frame
			// that never runs at all, and because it is exactly what every release before pinning
			// did with these frames anyway.
			// ROUTING IS NOT WHERE THE TEARDOWN PROBLEM IS FIXED -- see the flush at the end of
			// Thread::Worker(). Special-casing shutdown here was tried twice and both were worse:
			// an IsRunning() test is check-then-act and still stranded 2 runs in 10, and diverting
			// to the ordinary path during shutdown resumed frames onto other workers and crashed.
			// A departing worker handing its own leftovers to its (stealable) deque has no race in
			// it, because only that worker can be doing it.
			if (home < resumedInboxes.size()) {
				resumedInboxes[home]->push(task);
				NoteInboxPush(1);
				workers[home]->MarkQueuedWork();
				workers[home]->NotifyWorker();
				return true;
			}
		}
	}

	// Same routing as PushLocal -- a RESUMED hiPri task is still on the lane, and sending it back to
	// an ordinary worker would strand it just as surely as a fresh one.
	const size_t hotN = GetHotWorkers();
	// HiPriLaneActive(), NOT hotN -- see the identical fix above.

	const bool useHi = task->hiPri && HiPriLaneActive();
	const uint8_t chosen = (uint8_t)PickNextWorker(task->corePref, useHi);
	if (useHi) {
		// Same hook as PushLocal, and it has to be here too: a RESUMED completion queueing behind
		// another is the exact pressure the K controller is meant to see, and on an I/O workload
		// resumes are most of the lane's traffic. See the note at the PushLocal site.
		// Same deferred hook as the PushLocal site above -- see the note there.
		hiPriInboxes[chosen]->push(task);
		SetHiPriHint((size_t)chosen);
	}
	else
		loPriInboxes[chosen]->push(task);   // collapsed: no lane, no server
	workers[chosen]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
	NoteInboxPush(1);
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

	// ---- THE FLOOR-LANE VARIANT: SAME ROTATION, DIFFERENT BAND ------------------------------
	//
	// Reached only with K == 0 and the experiment armed (see SetHiPriFloorLane). Rotates the awake
	// floor [K, K+F) exactly as the branch above rotates the reserved band, so the two configurations
	// differ in ONE property -- whether the target is reserved -- and nothing else. That is what
	// makes them comparable: same routing shape, same inbox, same drain order, same rotation.
	//
	// THE BASE FLOOR, NOT THE LIVE ONE. Growth widens [K, K+F) transiently during a burst, and
	// steering completions onto workers that a controller happened to wake makes the landing zone a
	// function of unrelated bulk activity. The claim under test is "the floor is the landing zone",
	// and the floor a caller configured is the one that claim is about.
	//
	// FALLS THROUGH IF THE FLOOR IS EMPTY. floor=0 means there is no awake band to aim at, and
	// ordinary placement -- which prefers awake workers anyway -- is a better answer than picking
	// index 0 and calling it a lane.
	if (hiPri && HiPriFloorLane()) {
		const Bands  b     = GetBands();
		const size_t baseF = GetAwakeFloorBase();
		const size_t n     = workers.size();
		if (baseF > 0 && b.k < n) {
			const size_t span = (b.k + baseF <= n) ? baseF : (n - b.k);
			if (span > 0)
				return (int)(b.k + (nextHotWorker.fetch_add(1, std::memory_order_relaxed) % span));
		}
	}

	// ---- HIPRI GOES TO THE RESERVED RANGE -----------------------------------------------------
	//
	// [0, R) takes hiPri only -- ordinary placement skips it below -- so a completion steered here
	// finds a worker that is awake and NOT inside a bulk body. That second half is what the awake
	// floor could not give, and it is the whole reason steering hiPri at the bare floor measured
	// WORSE than not steering it at all (see ReservedHiPri).
	// (the hiPri branch above already routes over [0, K) when K > 0; with K == 0 there is no
	//  reserved band and a hiPri push takes ordinary placement, which is the K=0 contract)

	// ---- RE-MEASURED 2026-08-30: THE CLAIM BELOW DID NOT REPRODUCE -----------------------------
	//
	// tests/io_overlap_test.cpp was rewritten because it did not exist any more -- the numbers in
	// the paragraph below outlived the harness that produced them, so for a while the one result
	// standing between the reserved band and its deletion could not be checked. It can now:
	// SetHiPriFloorLane(true) at K=0 is the configuration that paragraph describes, and it is an arm
	// of that test.
	//
	// hiPri p99, completions probed against a saturated pool, us:
	//
	//     bulk body      5 us      20 us     400 us
	//     reserved K=2   4.20       7.70       1.90     <- FLAT. Does not care about the grain.
	//     floor lane     8.80      25.60     332.00     <- tracks the body length
	//     no lane      164.90     423.50    5030.90
	//
	// SO THE FLOOR LANE DOES NOT LOSE. It beats no lane by 15-19x at every grain, which is the
	// opposite of "hiPri never won". What it does is degrade with body length, exactly as the
	// reasoning below predicts and for the reason given -- awake is not available. The old result
	// most likely measured a build where the floor had no hiPri INBOX drained ahead of its deque,
	// so ordering was absent and only steering remained; that cannot be confirmed, which is the
	// point of not deleting a test.
	//
	// WHAT IT COSTS TO DROP THE RESERVATION, stated as a number rather than an argument: 2.1x on
	// the completion tail at 5 us grain, 3.3x at 20 us, 175x at 400 us. An application whose bulk
	// work is genuinely fine-grained can have its cores back for roughly a doubling of p99. One
	// that admits long bodies cannot.
	//
	// (The K=0-no-lane control reads 1.00x / 0.88x at 5 and 20 us -- both arms are the same code
	//  path there, so that is the harness checking itself. Its 400 us row is noise-dominated and
	//  should not be read.)
	//
	// ---- (history: hiPri was briefly steered at the unreserved floor, and it lost) ------------
	//
	// This block used to round-robin a hiPri push over [0, base) -- the awake floor -- on the
	// reasoning that a floor worker never parks, so a completion would land on a running thread and
	// pay no OS wake. Half of that is true and the other half is what matters.
	//
	// tests/io_overlap_test.cpp, pool saturated with 400 us bulk bodies, completion latency
	// push -> first instruction, hiPri and loPri interleaved so both see the same machine state:
	//
	//     loPri/hiPri at p99:  0.12x, 0.91x, 0.62x   (>1 would mean hiPri wins)
	//
	// hiPri never won and was 8x worse at the tail in the first run. The reason is that the floor is
	// AWAKE but not RESERVED: steering sends every completion to the same 2 workers, and under load
	// those two are inside bulk bodies, while an unsteered loPri push spreads across all ~31 awake
	// workers through the bitmap pick below. Concentrating on two busy threads beats dispersing
	// across thirty only if those two are kept free -- which is precisely what K did and the floor
	// does not.
	//
	// AND ORDERING CANNOT RESCUE IT. "Drain hiPri before your own deque" only applies when a worker
	// goes looking for its next task; it says nothing about the 400 us body it is already inside,
	// and a running task cannot be preempted.
	//
	// So hiPri now takes the same placement as everything else and keeps only the part that pays:
	// queue ORDER. A hiPri task still lands in hiPriInboxes, is still drained before the worker's
	// own deque, and is still stolen before loPri -- it just is not aimed at two threads that are
	// already busy.
	//
	// THE REAL FIX IS DISJOINT RANGES -- hiPri at [0, R), bulk steered at [R, base) -- which gives
	// back the reservation without K's lane bitmap or dynamic-K machinery. That needs base > R to
	// mean anything, so it is a decision about pool shape rather than a one-line change, and it is
	// not made here.

	// Placement is governed SOLELY by CorePref (see Task.h) -- queue priority (hiPri) is never consulted
	// here; the two axes are fully orthogonal by design. Default/Any/Wide all mean "no class preference"
	// and fall through to the original full-pool round-robin below.

	// ---- PREFER A WORKER THAT IS ALREADY AWAKE -----------------------------------------------
	//
	// Ahead of the round-robin below, and only for a CorePref that expresses no class preference --
	// a task that asked for P or E cores must still get them, so those fall through untouched.
	//
	// MEASURED, at 31 workers, against blind round-robin:
	//     throughput/1p   2.20 -> 10.89 M/s      (spread 2.28x -> 1.16x)
	//     throughput/mp   4.19 ->  9.24 M/s      (spread 1.99x -> 1.09x)
	// Five times the single-producer throughput, and the bimodality that has dogged every 31-worker
	// run all session goes with it.
	//
	// WHY: pushing to a PARKED worker costs an OS wake; pushing to a running one costs nothing,
	// because NotifyWorker's awake-skip fires. Round-robin is blind to that difference, so a single
	// producer on a wide pool lands on a sleeper nearly every push. It is why no-op throughput
	// SCALED BACKWARDS -- 10.97 M/s at 2 workers, 4.46 at 4, 2.20 at 31: fewer workers meant each
	// was saturated, therefore awake, therefore free to push to.
	//
	// THE KNOWN COST, and it is not small: concentrating work on the already-awake set is bad for
	// anything that needs to FAN OUT. ParallelFor's memory-bound row went to 0.77x and the
	// splitter-vs-cursor table degraded across the board, because a split range wants many workers
	// running at once and this steers them all at the few that already are. Throughput and fan-out
	// pull in opposite directions here; nothing in this block balances them yet.
	//
	// O(1) AND POOL-SIZE INDEPENDENT: at most kHintWords word loads and one CTZ on a randomly
	// rotated word. The rotation is load-bearing for the same reason it is in the steal pick --
	// scanning from bit 0 funnels every producer onto the lowest-numbered awake worker and rebuilds
	// the convoy this is meant to spread.
	//
	// ALL BITS CLEAR = the pool is genuinely asleep, so there is nothing to prefer: the fallback
	// runs and the wake that follows is necessary work rather than a missed optimisation.
	if (workers.size() <= kMaxHintQueues
	// ---- Wide DELIBERATELY DOES NOT ENTER HERE ----------------------------------------------
	//
	// This whole block narrows placement to the awake floor, which is the cheap push -- a running
	// worker costs no kernel wake -- and it is the wrong answer for bulk. A physics step or a
	// ParallelFor leaf wants CAPACITY now, not a cheap push: it is milliseconds of work against a
	// ~3 us wake, and being steered at two workers means the rest of the pool only arrives through
	// steals. The burst row measured exactly that -- growth woke 13 workers, 9 ever ran a task.
	//
	// So `Wide` skips this and falls through to the full-pool rotation at the end, paying the wakes
	// to have every worker running immediately. `Default` (and `Any`, which aliases it) keeps the
	// steered push. That is the entire meaning of the enum now.
	    && pref == CorePref::Default) {
		size_t nWords = (workers.size() + 63) / 64;
		if (nWords > kHintWords) nWords = kHintWords;

		unsigned long long awakeW[kHintWords] = {};
		unsigned long long anyAwake = 0;
		for (size_t w = 0; w < nWords; ++w)
			awakeW[w] = AwakeHintWord(w);

		// ---- NEVER THE RESERVED BAND, AND UNCONDITIONALLY ON F ------------------------------
		//
		// [0, K) takes hiPri only. A reserved worker does not drain its loPri inbox AT ALL --
		// drainOwnInbox() opens `if (task_to_run || reservedForHiPri) return false;` (Thread.cpp)
		// -- and that gate does not consult the floor. So this mask must not consult it either.
		//
		// IT USED TO LIVE INSIDE `if (const size_t baseF = GetAwakeFloorBase())` BELOW, which made
		// "who may own ordinary work" depend on "how many cores stay off the park path". Those are
		// different questions and GetAwakeFloorBase() only answers the second. With Fbase == 0 the
		// mask never ran, the bitmap pick below returned an index in [0, K), and a CorePref::Default
		// task was pushed to loPriInboxes[q < K] -- an inbox nobody may steal from and whose owner
		// will not drain it.
		//
		// AND THE RESULT IS A SPIN, NOT A STALL, which is why it does not look like the other bug.
		// The pre-park recheck still names that queue, so the worker never parks on it: it takes
		// the `continue`, which skips the backoff, the search declines to look, and the recheck
		// fires again. Hot, forever, with an unstealable task sitting there. Do NOT "fix" that by
		// teaching the recheck the same gate -- silencing it turns the spin into a permanent
		// strand. Fix the writer, which is here.
		//
		// REACHABLE BY CONFIGURATION, not by an exotic race: EnableIoReactor() calls
		// SetIoHotLane(1) -> SetHotWorkers(1), so every reactor app runs K >= 1, and any app that
		// also sets the awake floor to 0 is in it. The library default is Fbase 2, which is what
		// kept this hidden.
		//
		// THE `j < hotN` FALLBACK AT THE END OF THIS FUNCTION ALREADY IMPLEMENTS THIS SENTENCE. It
		// was simply unreachable whenever the bitmap path returned first, so the two halves of the
		// same rule disagreed. After this move they say the same thing.
		//
		// Preference may fail and fall back. RESERVATION MAY NOT.
		// Pinned by tests/verify/workerspin_model.c -- see -DPLACE_ON_RESERVED.
		//
		// ONE load, reused by the floor block below: kResv is the LIVE-K question ("who serves the
		// lane right now"), floorBase is where the floor starts. Same number today, and they must
		// still come from the same instant.
		const Bands  pbands = GetBands();
		const size_t kResv  = pbands.k;
		for (size_t w = 0; w < nWords; ++w) {
			const size_t lo = w * 64;
			if (kResv > lo) {
				const size_t b = kResv - lo;
				awakeW[w] &= (b >= 64) ? 0ull : ~((1ull << b) - 1ull);
			}
		}
		for (size_t w = 0; w < nWords; ++w) anyAwake |= awakeW[w];

		// ---- PREFER A WORKER THAT IS ACTUALLY THERE TO DRAIN ITS INBOX ------------------------
		//
		// AWAKE IS NOT THE SAME QUESTION AS AVAILABLE. A worker inside a task body is awake, and it
		// is the ONLY legal consumer of its own inbox -- nobody steals from an inbox -- so anything
		// placed there waits for that body to return. If the body is itself waiting on what was just
		// pushed to it, it never returns, and the work is stranded rather than merely late.
		//
		// A worker spin-helping inside WaitFor is NOT away: that loop drains its own inboxes on
		// every failed steal, so it is a legal consumer again and placement should keep choosing it.
		// See the away clear in WaitFor.
		//
		// PREFERENCE, NOT EXCLUSION, and the fallback is the whole safety argument: if every
		// candidate is away the mask is dropped and placement proceeds exactly as before. A rule
		// that can refuse to place at all is worse than a late pickup -- on a one-worker pool every
		// worker is away whenever it is working, and there is nowhere else for the task to go.
		{
			// AN AWAY MASK WAS HERE. It filtered out workers inside a task body, which is the right
			// thing to want and cost more than it was worth on both implementations tried -- see
			// UpdateBacklogHint for the numbers. The invariant it defended is now kept by publishing
			// a BACKED-UP inbox into the stealable deque at dispatch, which costs one relaxed load
			// of a counter the worker already owns.
		}

		// ---- STEER AT THE BASE FLOOR, NOT AT THE GROWN ONE ----------------------------------
		//
		// Growth exists so a WAVE has somewhere to be stolen to. It is not an invitation to spread
		// ordinary pushes across every worker it woke, and spreading them there measurably hurts:
		// with the floor left at 8 after a burst, the serial row put 2500 round trips on each of
		// q0..q7 and cost p50 0.40 -> 0.90 us, p99 0.60 -> 1.70, 1p 10.0 -> 5.74 M/s. Zero kernel
		// wakes throughout -- the loss is eight spinners contending with one producer, not wakes.
		//
		// So placement keeps aiming at [0, base) and the grown workers earn their tasks by
		// STEALING. That works because a floor worker drains its inbox into its own DEQUE
		// (push_bottom_batch in Worker()), so everything behind the one task it is running is
		// stealable -- which is what makes "wake overflow workers" mean something at all. Waking
		// workers that can only look at empty deques is the version of this that does nothing.
		//
		// FALL BACK IF THE BASE IS ASLEEP. At base=0, or when the base workers have parked, the
		// mask is empty and the full awake set is used instead. Without that this becomes an
		// unconditional "always push to [0, base)", which is a different and worse change -- it
		// stops a fan-out workload from ever reaching a wide pool.
		if (const size_t baseF = GetAwakeFloorBase()) {
			unsigned long long steerW[kHintWords] = {};
			unsigned long long anySteer = 0;
			// KEEP [reserved, baseF). The low end is excluded because those workers take hiPri
			// only -- steering bulk work there is exactly what made a completion queue behind a
			// 400 us body.
			// THE FLOOR IS [K, K+F), NOT [0, F). K reserved workers sit below it and ordinary work
			// must never land there -- that is the entire reservation. Offsetting rather than
			// carving out is what keeps the two knobs independent: F always means "F compute
			// workers that never park", whatever K is, so raising K cannot silently starve the
			// band that receives ordinary pushes.
			// TWO DIFFERENT QUESTIONS, and this variable used to answer both because the answers
			// were the same number. They are not the same question:
			//   kResv     -- "who is serving the lane RIGHT NOW". Ordinary work may never land here.
			//                Must follow LIVE K, or a shrinking K would keep excluding cores that
			//                are no longer serving anything.
			//   floorBase -- "where does the floor band start". Must follow the floor's base, or a
			//                growing K relocates the floor under running workers.
			// kResv AND ITS MASK HAVE MOVED OUT OF THIS BLOCK -- see the note above the bitmap
			// build. "Ordinary work may never land in [0, K)" is unconditional on F, so it cannot
			// be enforced inside a test on the floor base. `pbands` is the load hoisted with it;
			// floorBase reads from the same instant it always did.
			const size_t floorBase = pbands.k;
			// ---- DOES THE RAMP APPLY TO PLACEMENT, OR ONLY TO WAKEFULNESS? ------------------
			//
			// Default false = the behaviour described above: steer at BASE, grown workers steal.
			//
			// WHY THE OTHER ANSWER IS WORTH HAVING. The comment above prices this tradeoff on the
			// serial round trip (spreading cost p50 0.40 -> 0.90 us, 1p 10.0 -> 5.74 M/s) and calls
			// the fan-out side unbalanced. The marl blocking row is the fan-out side, measured:
			// 256 pushes per batch all land on the TWO base-floor workers and the other 29 have to
			// steal their way in, so the row runs at 10.0 ms against marl's 4.4 and against 5.1 ms
			// for the same pool with a wide landing zone. It is not the park -- 70 kernel wakes in
			// 7680 pushes -- it is the width of the landing zone.
			//
			// AND THE REGRESSION THAT ARGUES AGAINST IT WAS MEASURED WITH F LEFT GROWN WHILE THE
			// WORKLOAD WAS QUIET ("with the floor left at 8 after a burst"). That is the shed
			// failing, not growth succeeding. Keyed off the LIVE floor, the wide landing zone exists
			// only while the controller says a burst is in progress and collapses with it -- which
			// is the difference between "is hot" and "became hot" that the K ratchet turned on.
			//
			// UNPROVEN UNTIL SWEPT. Turning this on must be checked against the serial round trip
			// and 1p, not just the blocking row, because those are what the narrow steer bought.
			const size_t steerF = g_placementFollowsGrownFloor.load(std::memory_order_relaxed)
			                    ? GetAwakeFloor() : baseF;
			const size_t hiEnd = floorBase + steerF;     // one past the floor
			for (size_t w = 0; w < nWords; ++w) {
				const size_t lo = w * 64;
				unsigned long long keep;
				if (hiEnd <= lo)              keep = 0ull;
				else if (hiEnd - lo >= 64)    keep = ~0ull;
				else                          keep = (1ull << (hiEnd - lo)) - 1ull;
				// ...and clear everything below the floor's BASE. Workers in [activeK, floorBase)
				// are ordinary compute -- neither reserved nor floor -- so they belong in the
				// general fallback set below, not in the preferred steer set.
				if (floorBase > lo) {
					const size_t b = floorBase - lo;
					keep &= (b >= 64) ? 0ull : ~((1ull << b) - 1ull);
				}
				steerW[w] = awakeW[w] & keep;
				anySteer |= steerW[w];
			}
			// ---- THE RESERVED BAND IS CLEARED UNCONDITIONALLY -------------------------
			//
			// The preference below is "prefer the floor"; THIS is "never the reserved band", and
			// conflating them was a real bug. The code used to fall back to the UNMASKED awake set
			// whenever no floor worker's bit was set -- and that set includes [0, K), so ordinary
			// work landed on an I/O core.
			//
			// It fires exactly when you would least want it to: at startup, before any worker has
			// published an awake bit, every warmup push takes the fallback. The result was a loPri
			// task sitting in a reserved worker's inbox forever -- inboxes are not stealable and a
			// reserved worker will not run it -- which is the stall the dump caught as
			// `1 AWAKE ... inbox 0/1/0`.
			//
			// Preference may fail and fall back. Reservation may not -- WHICH IS WHY THE MASK IS
			// NO LONGER HERE. It ran only when this block ran, i.e. only when Fbase > 0, so the
			// fallback it was written to defend against was undefended in exactly the
			// configuration that needs it most. It is now unconditional, above.
			//
			// Now the preference: narrow to the floor if any of it is awake, otherwise use whatever
			// non-reserved awake workers there are -- and "non-reserved" is now true by
			// construction rather than by this block having run.
			if (anySteer) {
				for (size_t w = 0; w < nWords; ++w) awakeW[w] = steerW[w];
				anyAwake = anySteer;
			}
		}

		// ---- SELECT THE r-th SET BIT, NOT "THE FIRST SET BIT AFTER A RANDOM START" -----------
		//
		// This used to rotate the word by b0 = (r >> 3) & 63 and take CTZ. That finds the first set
		// bit at or after a random start, which weights every candidate by THE SIZE OF THE GAP
		// PRECEDING IT rather than choosing among them evenly.
		//
		// MEASURED, floor=2, 20,000 serial round trips: q0 took 19,688 and q1 took 312. With the
		// awake word equal to 0b11, bit 0 is preceded by a 62-bit wrapped gap and bit 1 by a 1-bit
		// gap, so bit 1 won only when b0 landed exactly on 1 -- one value in 64. 20000/64 = 312.5;
		// the run measured 312. The trick is fine on the wide sparse map it was written for and is
		// 63:1 on a small dense one, which is exactly what an awake floor is.
		//
		// WHY NOT JUST ROUND-ROBIN OVER [0, F): tried, and it cost more than it fixed. Returning an
		// index inside the floor before this block runs means default-pref work can NEVER land on a
		// worker outside the floor -- so a 6-node DAG goes to two workers and the other 29 have to
		// steal it back. Measured: frame DAG 4.74 -> 9.65 us, ParallelFor memory-bound 4.18 ->
		// 2.25x, latency/cold 0.78 -> 2.76 us. The bias was in HOW a destination is chosen; the fix
		// belongs there and not in shrinking the set chosen from. Fan-out needs every awake worker
		// to stay reachable.
		//
		// COST: one popcount per word plus at most popcount(word) iterations of a clear-lowest-bit
		// loop. On the common shape -- a floor of 1-4 -- that is a handful of instructions. It is
		// bounded by the number of AWAKE workers, so the expensive case is a fully-awake pool, which
		// is also the case where the pick matters least.
		if (anyAwake) {
			const unsigned r = (unsigned)nextWorker.fetch_add(1, std::memory_order_relaxed);

			unsigned total = 0;
			for (size_t w = 0; w < nWords; ++w) total += platform::PopCount64(awakeW[w]);

			unsigned j = (total > 1) ? (r % total) : 0u;
			for (size_t w = 0; w < nWords; ++w) {
				unsigned long long word = awakeW[w];
				if (!word) continue;
				const unsigned pc = platform::PopCount64(word);
				if (j >= pc) { j -= pc; continue; }
				// Clear the j lowest set bits; the CTZ then names the (j+1)-th.
				for (unsigned c = 0; c < j; ++c) word &= word - 1;
				size_t idx = w * 64 + platform::CountTrailingZeros64(word);

				// ---- RE-AIM OFF A YIELDING WORKER ------------------------------------------
				//
				// THE BITMAP CANNOT ANSWER THIS, which is why the check is here and not a second
				// hint word. A bit says awake or not-awake, and YIELD is neither: the worker owes
				// no syscall (it is runnable and returns on its own) but it is NOT on the core
				// right now, so a push aimed at it waits a scheduling quantum. Clearing the bit
				// during the yield would collapse YIELD into PARKED for placement and lose exactly
				// the distinction that makes YIELD worth having -- and it would be a second source
				// of truth to keep in step with the word. The word is the source; this reads it.
				//
				// ONE LOAD, ON A LINE THE PUSHER IS ABOUT TO TOUCH ANYWAY. The caller is about to
				// push to this worker's inbox, bump its inboxDepth, MarkQueuedWork and NotifyWorker
				// -- all on the same object. This does not add a cache miss to the push path, and
				// it is asked only of the ONE candidate already chosen, never of the pool.
				//
				// ONE STEP, NOT A LOOP, AND DELIBERATELY BEST-EFFORT. `word`'s lowest set bit is
				// the candidate, so clearing it hands us the next one for free -- no popcount, no
				// rescan. If there is no alternative in this word we keep the yielding worker, and
				// that is CORRECT rather than a compromise: yieldstate_model.c's -DTARGET_YIELDED
				// is GREEN. The swap still latches the permit, the worker's return CAS sees
				// NOTIFIED and rescans, and nothing is lost. Aiming elsewhere buys latency, so
				// best-effort is the right amount of effort to spend on it -- and the state can
				// change the instant after this load either way.
				if (idx < workers.size()
				    && workers[idx]->GetWorkerState() == Thread::WS_YIELD) {
					bool reaimed = false;
					const unsigned long long rest = word & (word - 1);
					if (rest) {
						const size_t alt = w * 64 + platform::CountTrailingZeros64(rest);
						if (alt < workers.size()) { idx = alt; reaimed = true; }
					}
					// COUNTED BOTH WAYS -- see YieldAimCount() in the header. A zero AIM count means
					// the yield window is never in the way of a push and this whole state is a load
					// per push for nothing; that is the reading worth having before any A/B of it.
					NoteYieldAim(reaimed);
				}
				// NO PER-PUSH SPILL HERE, and both attempts at one are worth recording so they are
				// not retried. The idea was: prefer an awake worker UNLESS it is already occupied,
				// so a burst fans out instead of piling onto the single floor worker.
				//
				//   gate on `busy`           -> frame DAG 2.12 -> 16.12 us, 1p 10.95 -> 4.15
				//   gate on `hasQueuedWork`  -> burst 1.0x -> 6.5x, but 1p 10.95 -> 1.93
				//
				// Both fail identically: in ANY throughput workload the floor worker is permanently
				// busy and permanently has queued work, so every push spills to a parked worker and
				// buys a wake -- which is the floor-less behaviour the floor exists to replace.
				// Queueing behind a running awake worker IS the win; a per-push test cannot tell
				// that from the burst case, because at the moment of a push they look the same.
				//
				// Burst needs a mechanism that knows N tasks arrived AT ONCE -- which a single Push
				// cannot know and PushBatch can -- or a floor that GROWS under sustained load.
				if (idx < workers.size()) {
					// CROWDING: the preferred worker already has work waiting, so the floor is too
					// small for the rate arriving. Note it and STILL PUSH HERE -- redirecting the
					// task was tried twice and both spills reverted the floor entirely (frame DAG
					// 2.12 -> 16.12 on `busy`, 1p 10.95 -> 1.93 on `hasQueuedWork`). Queueing behind
					// an awake worker is the right thing to do with THIS task; the wrong thing is
					// leaving the floor at one for the NEXT sixteen.
					//
					// THIS IS THE ONLY SIGNAL THAT REACHES `burst`. Sixteen tasks pushed to a
					// single awake worker wake nobody, so the miss counter stays zero; and with the
					// other thirty workers parked, nothing is looping to run the controller from the
					// worker side either. The pusher is the only party awake and the only one that
					// can see the queue building.
					if (workers[idx]->hasQueuedWork.load(std::memory_order_relaxed)) {
						NoteWakeMiss();

						// GROW ON DEPTH, NOT ON "ANYTHING IS QUEUED". This gate was
						// hasQueuedWork alone, and a bool is too coarse: a 6-node frame graph
						// briefly queues behind two workers exactly like a 16-task wave does, so
						// every graph grew the floor and the hold outlived the graph. Measured
						// cost of that: DAG 4.67 -> 7.11 us, paying for spinners that had a
						// microsecond of work to steal.
						//
						// Depth separates them on the only axis that matters here -- how much is
						// waiting behind the task actually running. Six nodes over the base floor
						// is ~3 each and must not grow; sixteen 3.3 ms tasks is ~8 each and must.
						//
						// AND IT NO LONGER SELF-CANCELS, which is what makes one-step-per-push
						// enough for a wave: placement steers at [0, base), so growth does not
						// redirect later pushes away from the crowded worker. Depth keeps rising
						// across the wave instead of being spread thin by the growth it caused.
						// ---- A PRODUCER-LOCAL STREAK, WHICH IS THE ONE SIGNAL A PUSH HAS -------
						//
						// Two things a push CANNOT do, both learned the expensive way:
						//   - it cannot ask how long the target has been busy. A wave of sixteen is
						//     submitted inside ~10 us, long before the worker it queues behind has
						//     been running for any length worth measuring, so a duration gate here
						//     declines sixteen times and the burst never grows.
						//   - it cannot use depth. 200,000 no-ops pile up behind two workers
						//     exactly like sixteen 3.3 ms bodies do, and gating on depth grew the
						//     floor to 16 on the no-op row: 1p 10.0 -> 5.2 M/s.
						//
						// What it CAN see is its own history: how many pushes in a row this
						// producer has aimed at a floor worker that was already busy. That
						// separates the two workloads without a clock and without a queue length,
						// because it is measuring PERSISTENCE rather than size.
						//
						// WHY IT DOES NOT FIRE ON A FLOOD. A no-op worker keeps finishing, so `busy`
						// is observed false constantly and the streak keeps resetting -- it takes
						// an uninterrupted run to reach the threshold. A worker inside a 3.3 ms body
						// is busy for every push of the wave, so the streak climbs monotonically and
						// crosses on push three.
						//
						// The first kStreakToSpill tasks still land on q0/q1, which is what keeps
						// the serial round trip and a small frame graph exactly where they were.
						// ---- MEASURED WORSE, AND THE REASON GENERALISES --------------------
						//
						// Streak-to-inbox: q0+q1 took 72 of 80 burst tasks, participants 11 -> 7,
						// wall 10.24 -> 15.34 ms. It is kept behind this flag rather than deleted
						// because the IDEA is sound for a sustained producer -- it is the arrival
						// pattern it cannot see.
						//
						// THE POOL HAS NOT REACTED YET. `busy` turns true only when a worker picks
						// a task up, and a wave of sixteen is fully submitted in ~10 us while q0
						// and q1 are still coming out of WaitOnAddress. So every push observes
						// busy == false, the streak resets, and it never reaches 3.
						//
						// This is the third gate to die of the same cause, which is what makes it a
						// property of the problem rather than three bad guesses: duration (the
						// worker has not been busy 200 us yet), depth-after-drain (nothing has been
						// drained yet), and now streak (nothing is busy yet). AT t=0 OF A BURST
						// FROM AN IDLE POOL THERE IS NOTHING TRUE ABOUT THE POOL TO OBSERVE. Every
						// push-time signal is reading state the pool has not produced.
						//
						// Which leaves the completion path, and not as a fallback: a completion is
						// the first moment anything is known -- the body's real duration, measured,
						// with the queue still behind it.
						// SO THE RULE IS: ANYTHING ALREADY WAITING HERE, SPILL. No clock, no streak,
						// no busy flag -- the queue is the one thing that IS true at t=0, because
						// the producer itself just made it true. Everything else is state the pool
						// has not had time to produce.
						//
						// This is what the "working" configuration was actually doing. A duration
						// gate appeared to fix the burst, and it was inert: taskStartNs was never
						// cleared on the Native completion path, so it read as a task that started
						// long ago and the gate always said yes. The measured win was real; the
						// stated reason for it was not. Written down because a stale-input gate that
						// always fires is indistinguishable from a gate that works, and the next
						// person to touch the duration check deserves to know it was never load
						// bearing.
						//
						// WHY THIS IS SAFE FOR THE ROWS THAT MATTER. A serial round trip never has a
						// second task outstanding, so `queued` is 0 and it always lands on the base
						// floor -- latency and its 100%-on-floor landing spread are untouched. The
						// spill costs no wake either: it only ever targets [base, live), which
						// growth has already woken.
						// ---- PRODUCER TIME, WHICH IS THE ONE CLOCK THAT WORKS AT t=0 ----------
						//
						// Every previous gate asked about the POOL and the pool has not reacted
						// yet: `busy` is false because nobody has picked up, depth is shallow
						// because nothing has drained, the target has not been running 200 us
						// because it started microseconds ago. Sixteen pushes land in ~10 us and
						// the answer to all three questions is "no" for the whole submit.
						//
						// The PRODUCER'S OWN timing needs none of that. Three pushes inside 50 us
						// from one thread is a wave, and it is true on push three -- before any
						// worker has done anything at all. That is the difference between a signal
						// about the pool and a signal about the submission.
						//
						// WHY A SERIAL ROUND TRIP CANNOT REACH IT: it pushes once and then blocks
						// in WaitFor until that task completes, so consecutive pushes are ~0.5 us
						// of work apart but separated by a whole round trip -- the streak resets to
						// 1 every time and it never leaves the base floor.
						// ---- ONLY A BARE PRODUCER MAY GROW THE FLOOR -------------------------
						//
						// A WORKER PUSHING IS NOT A SUBMISSION, IT IS A CONTINUATION. TaskDAG's
						// Fire() releases six dependents back-to-back from inside the node that
						// just completed, and by producer timing that is indistinguishable from a
						// burst: same thread, six pushes, microseconds apart. So every graph grew
						// the floor, the hold kept it there, and 20,000 graphs ran against sixteen
						// spinners -- 3.51 -> 7.2 us, with the row printing `floor 16 (base 2)`.
						//
						// The distinction is not in the timing, it is in WHO. Work published by a
						// worker is already inside the pool: the pool is by definition awake and
						// running, it has a deque a thief can reach, and there is nothing to wake
						// anybody up FOR. Growth exists for the other case -- an outside thread
						// handing work to a pool that may be entirely parked -- and only a bare
						// thread can be in that case.
						//
						// ParallelFor's leaves and every Fire() land on this branch too, which is
						// the same reasoning: a split is a continuation of work already running.
						if (Thread::GetCurrent() != nullptr) return (int)idx;

						// ---- READ THE CLOCK ONLY WHERE THE ANSWER CAN STILL CHANGE ------------
						//
						// MonotonicNs() is a QueryPerformanceCounter on Windows, tens of
						// nanoseconds, and this sat on EVERY bare-thread push. Against a no-op
						// dispatch of ~130 ns that is a double-digit percentage of the row, and it
						// is spent almost entirely on producers whose answer was settled long ago:
						// a 200,000-push flood leaves the spill window after 64 and can never
						// re-enter it without a gap.
						//
						// So once the streak is past the window, the only thing still worth
						// detecting is that gap -- and a gap is a property of TIME, not of push
						// count, so sampling one push in 64 finds it just as well. At flood rates
						// 64, NOT 256: the sampled gap is compared against kWaveGapNs, so the sampling
						// interval must be comfortably SHORTER than that gap or it reports one. At
						// ~130 ns a push, 256 pushes is ~33 us against a 50 us threshold -- close enough
						// to trip on jitter and drop a settled flood back into the spill window. 64 is
						// ~8 us and cannot.
						// that is microseconds of delay on a decision that only matters when the
						// producer has stopped anyway; inside the window every push still reads,
						// because there the next push genuinely changes the outcome.
						++t_pushesSinceClock;
						if (t_floorStreak > kStreakSpillMax
						    && (t_pushesSinceClock & 0x3Fu) != 0u)
							return (int)idx;
						t_pushesSinceClock = 0;

						const long long nowNs = MonotonicNs();
						if (nowNs - t_lastPushNs < kWaveGapNs) ++t_floorStreak;
						else                                   t_floorStreak = 1;
						t_lastPushNs = nowNs;
						// ---- AND A CEILING, WHICH IS WHAT SEPARATES A WAVE FROM A FLOOD -------
						//
						// The streak alone fires for anything that pushes quickly, which is not
						// just a burst: measured, it fixed the burst (15-16 participants) and cost
						// 1p 8.35 -> 4.90 M/s and frame DAG 3.51 -> 7.08 us, because a no-op flood
						// and a frame graph are also one thread pushing every few hundred
						// nanoseconds. The gap never opens, so the streak never resets.
						//
						// A BURST IS SHORT AND THEN STOPS. That is the whole difference, and it is
						// visible in the streak itself: a 16-task wave counts to 16 and then sits
						// idle long enough to reset, while a flood counts to 200,000 and a frame
						// loop never stops counting at all. So spill only inside a WINDOW. Past the
						// ceiling the producer is not submitting a wave, it is simply a fast
						// producer, and steering it wide is the thing that wrecked those two rows.
						//
						// A flood therefore spills its first ~60 tasks and nothing after -- noise
						// against 200,000 -- and a frame loop stops spilling within its first
						// couple of graphs and stays stopped, because its streak never resets.
						if (t_floorStreak >= kStreakToSpill
						    && t_floorStreak <= kStreakSpillMax) {
							// ONE STEP PER CROWDED PUSH, NOT `depth` STEPS. Growing by the depth was
							// what turned eight queued no-ops into a floor of 16 in a single push;
							// it makes the size of a transient queue set a persistent policy.
							// One-per-push was previously too slow only because the gate itself was
							// wrong -- now that a wave is the only thing that reaches here, each of
							// its pushes contributes a step and the wave is exactly the population
							// that should be paying for them.
							// GROW BY THE STREAK. It is the producer's own count of how wide this
							// submit already is, which is exactly the width the pool needs -- and
							// stepping by one instead costs the wave its tail: measured, +1 per
							// push ran the 16-task burst on 10-11 workers against 16 for the
							// streak, because the last tasks are placed before the floor has
							// finished catching up with the first.
							//
							// SAFE ONLY BECAUSE OF THE TWO FILTERS ABOVE IT. Growing by the streak
							// is what pinned the floor at 16 for the no-op row and the frame-graph
							// row; those are now excluded by the caller test (a worker never grows)
							// and by the streak ceiling (a flood stops spilling past 64). With the
							// population reduced to actual bare-thread waves, the width they ask
							// for is the width they should get.
							NoteFloorCrowding(t_floorStreak);

							// ---- AND PUT THIS TASK ON ONE OF THE WORKERS WE JUST WOKE --------
							//
							// GROWING WITHOUT SPILLING DOES NOTHING FOR A WAVE, which the probe
							// showed directly: the floor grew to 5-8 during a 16-task wave and the
							// wave still took 24 ms -- eight waves on two workers, exactly as if
							// nothing had been promoted.
							//
							// The reason is that a busy worker's INBOX is unreachable. While q0
							// runs a 3 ms body, the tasks behind it sit in its inbox, and an inbox
							// has one legal consumer. It only becomes stealable when q0 finishes
							// and drains it into its deque -- by which time the workers growth woke
							// have found nothing advertised, collapsed the floor and parked again.
							// Promotion and reachability were racing, and reachability lost every
							// time.
							//
							// THIS IS NOT THE SPILL THAT WAS TRIED AND REVERTED. That one gated on
							// `busy` or `hasQueuedWork` -- true on nearly every push in any
							// throughput workload -- and spilled to PARKED workers, so it bought a
							// kernel wake per push and reproduced floor-less behaviour (frame DAG
							// 2.12 -> 16.12 us, 1p 10.95 -> 1.93 M/s). Two differences make this
							// one safe: it fires only past a depth threshold, so a serial round
							// trip and a small graph never reach it; and it spills only to workers
							// in [base, live), which growth has just WOKEN -- never to a sleeper,
							// so it costs no wake at all.
							// ---- OFF BY K. THE FLOOR ACCESSORS RETURN WIDTHS, NOT INDICES --------
							//
							// The band is [K, K+F). GetAwakeFloorBase() and GetAwakeFloor() are both
							// COUNTS, so the grown slice is at indices [K+baseF, K+liveF) -- and this
							// read them as indices and aimed at [baseF, liveF).
							//
							// INVISIBLE AT K=0 AND AT K=2, WHICH IS WHY IT SURVIVED. With no reserved
							// band the two coincide exactly. At K=2 with base F=2 the expression
							// starts at index 2, which is the first FLOOR worker -- wrong for the
							// stated purpose (it aims at the base floor rather than the grown slice)
							// but still a legal ordinary target, so nothing complains.
							//
							// AT K=3 IT AIMS INTO THE RESERVED BAND. base F=2 gives index 2, and
							// [0,3) is reserved, so an ordinary task lands on an I/O core. The
							// runtime says so itself -- "ordinary task placed on RESERVED worker 2
							// (K=3)" -- and the stray-drain net then bounces it to the first compute
							// worker, which is one more push and one more notify per occurrence.
							const size_t k     = hotN;
							const size_t baseF = GetAwakeFloorBase();
							const size_t liveF = GetAwakeFloor();
							if (liveF > baseF) {
								const size_t span = liveF - baseF;
								const size_t alt  = k + baseF
									+ ((size_t)nextWorker.fetch_add(1, std::memory_order_relaxed) % span);
								if (alt < workers.size()) return (int)alt;
							}
						}
					}
					return (int)idx;
				}
				break;
			}
		}
	}

	// `pickFrom` WAS HERE: a round-robin over one class's worker subset, skipping [0,K). Its only
	// caller was the P/E branch below, so it went with it. A lambda assigned to `auto` raises no
	// unused warning, which is exactly how this kind of thing survives a deletion -- so it is
	// removed rather than left for the next reader to wonder about.
	//
	// The reserved-band skip it carried is not lost: the full-pool rotation at the end of this
	// function does the same `j < hotN` test, and for the same reason. Ordinary work must never be
	// ROUTED to a reserved worker -- one exists to have nothing else to do, and a bulk task landing
	// there costs a completion the whole duration of that task, because a running task cannot be
	// preempted.

	// ---- THE P/E CLASS ROUTING WAS HERE AND IS GONE ------------------------------------------
	//
	// It tried the preferred class set and spilled to the other. Removed because it was dormant --
	// `src/posix/Topology.cpp` said so in the tree, "no shipped caller requests CorePref::P or ::E",
	// and a repo-wide grep found none -- and because where it DID apply it was unproven: under the
	// default `Ideal` affinity a worker is not pinned, so aiming at a "P worker" is a preference the
	// OS then weighs against its own hybrid policy. It binds only under `hard`, which measured ~45%
	// worse on wake latency. The concept is also x86-hybrid-specific and does not port.
	//
	// The topology it read is NOT gone and must not be: `isPCore` still drives E-core QoS handling
	// in Thread.cpp and the same-class/other-class victim ordering for steals. Those are properties
	// of the MACHINE that the pool reacts to; this was a per-task request nobody made.

	// Every task lands here now -- Default arrives having declined the steered pick above, Wide by
	// skipping it deliberately. The original full-pool round-robin, unchanged.
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
TaskType TaskScheduler::CurrentTaskType() noexcept {
	// Straight to the thread-local: no Instance(), which throws when uninitialised and would put a
	// branch plus a global load in front of every lock-free section that asks.
	//
	// currentRunningTask is the right source rather than currentFiber, because it is set for ALL
	// three modes -- a coroutine has no fiber, which is exactly the case this must distinguish.
	if (Thread* w = Thread::GetCurrent())
		if (Task* t = w->currentRunningTask)
			return t->type;
	return TaskType::Native;
}

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

// EAGER CANCELLATION FOR THE MUTEX. Modelled directly on SchedulerConditionVariable::CancelWaiters,
// including the shape that matters: filter the queue UNDER the lock, collect victims, and resume
// them OUTSIDE it. Resuming under the spinlock would let a woken waiter re-enter this mutex on
// another worker while this thread still holds the flag.
//
// A NULL RESULT SLOT IS NOT CANCELLABLE, and that is deliberate rather than an oversight -- plain
// Lock() passes null, and a waiter with nowhere to report Cancelled cannot be told it did not get
// the lock. Those are left queued, exactly as skip-at-release leaves them.
// ---- the primitive registry ---------------------------------------------------------------------
//
// Registration is skipped entirely when there is no scheduler yet. That is the file-scope primitive
// case and it is a genuine limitation, not a bug to paper over: such a primitive works normally but
// Join() cannot find it, so anything parked on it at teardown is abandoned exactly as before.
WaitPrimitive::WaitPrimitive() {
	if (!TaskScheduler::IsInitialized()) return;
	TaskScheduler& s = TaskScheduler::Instance();
	std::lock_guard<std::mutex> lk(s.primitivesMtx);
	nextPrimitive_ = s.primitivesHead;
	if (s.primitivesHead) s.primitivesHead->prevPrimitive_ = this;
	s.primitivesHead = this;
}

WaitPrimitive::~WaitPrimitive() { LeaveRegistry(); }

void WaitPrimitive::LeaveRegistry() noexcept {
	// IDEMPOTENT BY DESIGN, because it is called twice on every primitive: once as the first
	// statement of the derived destructor (while the derived vtable is still installed, so a
	// concurrent drain that races us either sees a fully-formed object or does not see us at all)
	// and once from ~WaitPrimitive for anything that forgot.
	//
	// A primitive constructed before Init never entered the chain and one destroyed after Join
	// cleared it is already out -- both leave the pointers null, so this is a no-op rather than a
	// corrupt splice.
	if (!TaskScheduler::IsInitialized()) return;
	TaskScheduler& s = TaskScheduler::Instance();
	std::lock_guard<std::mutex> lk(s.primitivesMtx);
	if (!nextPrimitive_ && !prevPrimitive_ && s.primitivesHead != this) return;   // not linked
	if (prevPrimitive_)                prevPrimitive_->nextPrimitive_ = nextPrimitive_;
	else if (s.primitivesHead == this) s.primitivesHead = nextPrimitive_;
	if (nextPrimitive_) nextPrimitive_->prevPrimitive_ = prevPrimitive_;
	nextPrimitive_ = prevPrimitive_ = nullptr;
}

void SchedulerMutex::CancelWaiters(CancelToken tok) {
	constexpr size_t kBuf = 64;
	for (;;) {
		Waiter victims[kBuf];
		size_t n = 0;

		while (spinLock.test_and_set(std::memory_order_acquire)) { platform::CpuRelax(); }
		{
			std::queue<Waiter> keep;
			while (!waiters.empty()) {
				const Waiter w = waiters.front();
				waiters.pop();

				// An INVALID token means "all of them" -- what a teardown drain asks for. Otherwise
				// IsWithin, not ==, so cancelling a parent scope reaches waits registered under a
				// child. Matching with == is the 3.4.1 bug the reactor already had.
				const bool matches = w.result &&
				                     (!tok.Valid() || CancelToken(w.token).IsWithin(tok));
				if (matches && n < kBuf) victims[n++] = w;
				else                     keep.push(w);
			}
			waiters.swap(keep);
		}
		spinLock.clear(std::memory_order_release);

		if (n == 0) return;

		// Out of the lock. Each of these may already be running by the time the call returns, so
		// nothing here may touch the waiter again afterwards.
		for (size_t i = 0; i < n; ++i) {
			*victims[i].result = WaitResult::Cancelled;   // resumed WITHOUT the lock
			if (victims[i].fiber) {
				Thread::Resume(victims[i].fiber);
			} else if (victims[i].coro && TaskScheduler::IsInitialized()) {
				TaskScheduler::Instance().Push(victims[i].coro);
			}
		}

		if (n < kBuf) return;   // drained in one pass; otherwise go round for the rest
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

// ---- ingress backpressure -----------------------------------------------------------------------

// ZERO MEANS UNLIMITED, and that is the default -- this changes nothing until an application asks
// for it. Read on the submit path and on the drain, both gated on this being non-zero, so a process
// that never calls SetSubmitLimit pays one relaxed load of a line that is read-shared and never
// written.
static std::atomic<size_t> g_submitLimit{ 0 };

// QUEUED-BUT-NOT-YET-STARTED, across every inbox. Maintained ONLY while a limit is set, which is
// why the limit must be set before Init: enabling it mid-run would start counting from a base that
// already has tasks in flight, and the number would be wrong for the life of the process.
static std::atomic<size_t> g_inboxDepth{ 0 };

void TaskScheduler::SetSubmitLimit(size_t maxQueued) {
	g_submitLimit.store(maxQueued, std::memory_order_relaxed);
}
size_t TaskScheduler::GetSubmitLimit() { return g_submitLimit.load(std::memory_order_relaxed); }
size_t TaskScheduler::QueuedDepth()    { return g_inboxDepth.load(std::memory_order_relaxed); }

void TaskScheduler::NoteInboxPush(size_t n) {
	if (g_submitLimit.load(std::memory_order_relaxed) == 0) return;
	g_inboxDepth.fetch_add(n, std::memory_order_relaxed);
}
void TaskScheduler::NoteInboxDrain(size_t n) {
	if (g_submitLimit.load(std::memory_order_relaxed) == 0 || n == 0) return;
	g_inboxDepth.fetch_sub(n, std::memory_order_relaxed);
}

// APPLIED TO NON-WORKER SUBMITTERS ONLY, and that restriction is the whole safety argument.
//
// A WORKER MUST NEVER BE MADE TO WAIT HERE. Bounding a queue means somebody has to stop pushing,
// and if that somebody is a worker it may be the only thread that can drain the queue it is waiting
// on -- a Native task inside ParallelFor pushes chunks to every worker INCLUDING itself, so a
// blocking bound there deadlocks deterministically rather than occasionally. External threads
// (main, an app thread, a loader) are never consumers of a worker inbox, so they can be held.
//
// AND THEY HELP RATHER THAN SLEEP. A flooding producer that is told to wait is a thread doing
// nothing while the pool it saturated is behind; the same thread running one task is a thread
// helping. TryRunStolenNativeTask is the same mechanism WaitFor already uses on non-workers, so
// this adds no new execution path -- only a new reason to enter one.
//
// BOUNDED, NOT BLOCKING. One helped task per call, then the caller re-checks and pushes anyway if
// it still cannot help. A submit that never returns is worse than a deep queue: the point is to
// slow a producer down to the rate the pool drains at, not to give the runtime a veto on submission.
void TaskScheduler::ApplyIngressBackpressure() {
	const size_t limit = g_submitLimit.load(std::memory_order_relaxed);
	if (limit == 0) return;
	if (Thread::GetCurrent() != nullptr) return;   // a worker: never held, see above
	if (g_inboxDepth.load(std::memory_order_relaxed) <= limit) return;

	// Help once. If there was nothing to help with, the backlog is somebody else's to drain and
	// spinning here would burn a core to no purpose -- yield instead and let the caller proceed.
	// Instance() rather than a static call: the helper is a member because it steals from THIS
	// pool's deques. Safe here -- a submit limit can only be set before Init, so by the time any
	// submission reaches this line the scheduler exists.
	if (!Instance().TryRunStolenNativeTask()) std::this_thread::yield();
}
