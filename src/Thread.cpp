// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Hazard.h"  // retire-bag flush on the way into sleep
#include "../include/Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include "../include/Timer.h"   // MonotonicNs -- lane occupancy stamps
#include <cassert>
#include <chrono>
#include <iostream>
#include <cstring>   // std::memset -- MSVC pulls this in transitively, libstdc++ does not
#include <utility>   // std::swap, for drainInbox`s in-place corePref partition

// ApplyHotPriority needs these, and it is defined near the top of this file -- so they cannot live
// in the !JLIB_PLATFORM_WINDOWS block further down, which comes after it.
#if !JLIB_PLATFORM_WINDOWS
#include <sys/resource.h>       // PRIO_PROCESS, setpriority
#endif
#if JLIB_PLATFORM_LINUX
#include <sys/syscall.h>        // SYS_setpriority, SYS_gettid
#include <unistd.h>
#endif
#if JLIB_PLATFORM_DARWIN
#include <pthread.h>
#include <sys/qos.h>            // QOS_CLASS_*, pthread_set_qos_class_self_np
#endif
using namespace JLib;
thread_local Thread* Thread::instance = nullptr;

namespace {
// THE ONLY PLACE PLATFORM DIFFERENCES LIVE for hot-worker priority. The worker loop calls this and
// stays one protocol on every OS; without that the loop grows three copies of the same state
// machine and they drift, which is a failure this file has already had.
//
// Called on the CURRENT thread only, and only when HotThreadPolicy != Normal -- the callers gate on
// hotPriorityRaised, which is never set under Normal. So this is a no-op configuration by
// construction rather than by an extra branch on the per-task path.
//
// EVERY BACKEND SWALLOWS FAILURE. Elevation is privileged on POSIX and can be refused at runtime in
// a way the Win32 call never is; refusing is not an error, it is the unprivileged answer. The
// caller's flags then say "raised" while the OS says otherwise -- harmless, because the flags exist
// to suppress redundant syscalls, not to describe the kernel.
void ApplyHotPriority(bool wantElevated) noexcept {
#if JLIB_PLATFORM_WINDOWS
    // TIME_CRITICAL inside a NORMAL_PRIORITY_CLASS process is 15: the top of the non-realtime range,
    // above the pool and below the OS. True realtime (16-31) needs REALTIME_PRIORITY_CLASS and a
    // privilege, and would let a spin loop starve the machine, so it is deliberately not asked for.
    ::SetThreadPriority(::GetCurrentThread(),
                        wantElevated ? THREAD_PRIORITY_TIME_CRITICAL : THREAD_PRIORITY_NORMAL);

#elif JLIB_PLATFORM_DARWIN
    // QoS, NOT SCHED_FIFO and NOT affinity. Apple Silicon has no thread affinity API at all, and QoS
    // is the documented lever -- it is also what actually steers P vs E core selection there, which
    // is the same decision Windows makes off priority.
    (void)pthread_set_qos_class_self_np(
        wantElevated ? QOS_CLASS_USER_INTERACTIVE : QOS_CLASS_DEFAULT, 0);

#elif JLIB_PLATFORM_LINUX
    // PER-THREAD niceness, and the raw syscall is what makes it per-thread. Linux nice is a
    // thread attribute, but glibc's setpriority(PRIO_PROCESS, 0, n) follows POSIX and applies to the
    // whole PROCESS -- which would elevate all 31 workers, the exact configuration measured 5x WORSE
    // because N runnable threads then preempt the completion thread feeding them. Passing a TID
    // through the syscall keeps it to this thread.
    //
    // -10 rather than -20: enough to win a timeslice contest against ordinary workers without
    // approaching the range where a spinning thread starves kernel threads. Not SCHED_FIFO -- that
    // is above nearly everything and will not yield, far closer to the process-wide elevation this
    // file records as a regression. A FIFO tier stays opt-in and unbuilt; see HotThreadPolicy.
    //
    // EPERM IS EXPECTED AND IGNORED. Lowering nice needs CAP_SYS_NICE or a raised RLIMIT_NICE, and
    // ANDROID IGNORES IT REGARDLESS because cgroups arbitrate there -- so never quote a measured win
    // from Android on the strength of this call.
    (void)syscall(SYS_setpriority, PRIO_PROCESS,
                  (int)syscall(SYS_gettid), wantElevated ? -10 : 0);
#else
    (void)wantElevated;
#endif
}

// OPT THIS THREAD OUT OF (OR INTO) OS POWER THROTTLING. Windows only; every other platform is a
// deliberate no-op, explained below.
//
// THIS IS NOT PRIORITY, and it is the reason ApplyHotPriority does not cover it. Priority decides
// who wins a timeslice contest. EcoQoS decides whether the thread runs at reduced FREQUENCY and
// gets parked on efficiency cores -- a thread can be TIME_CRITICAL and still be throttled, because
// the scheduler happily gives full priority within a clamped budget. So this applies to EVERY
// worker, not only hot ones, and it applies whatever HotThreadPolicy says.
//
// WHY IT IS WORTH A SYSCALL PER WORKER AT STARTUP. Windows applies EcoQoS by inheritance and by
// heuristic: a process launched from a background context starts throttled, and a process that
// loses foreground can be demoted. Measured on this machine, those two levers together moved
// dispatch latency by ~173x -- large enough that a whole benchmarking session was invalidated by it
// before the cause was found. A compute pool that the application has explicitly created and is
// actively feeding is not the workload EcoQoS is for.
//
// THE POLICY IS RESOLVED BY THE CALLER, not here. Topology never reaches this function -- the worker
// entry point turns it into OptOut or Force using isPCore, because that is the only place that knows
// which worker this is. See SetWorkerPowerThrottling for the policy itself.
//
// NEVER ECOQoS A WORKER THIS SCHEDULER PINS TO A P-CORE: that asks the OS to put the work on its
// fastest core AND to run it slowly, and what you get is a clamped P-core.
//
// NOT A GUARANTEE, and nothing here checks otherwise. The call is a REQUEST; the OS may still
// throttle for thermal or battery reasons, and on a version older than Windows 10 1809 it simply
// fails with ERROR_INVALID_PARAMETER. Failure is ignored for the same reason ApplyHotPriority
// ignores EPERM: refusing is the system's answer, not an error in the caller.
//
// NO POSIX EQUIVALENT EXISTS, and the absence is not an oversight. Linux expresses the same idea
// through cgroup cpu.uclamp and per-task util_clamp, which are administrative settings a library
// has no business writing; on Android the cgroup arbitration overrides anything a thread asks for
// anyway. macOS folds it into QoS, which ApplyHotPriority already sets -- QOS_CLASS_USER_INTERACTIVE
// is both the priority and the "do not park me on an E-core" request there.
void ApplyPowerThrottling(TaskScheduler::PowerThrottling p) noexcept {
#if JLIB_PLATFORM_WINDOWS
#ifdef THREAD_POWER_THROTTLING_CURRENT_VERSION
    THREAD_POWER_THROTTLING_STATE s{};
    s.Version = THREAD_POWER_THROTTLING_CURRENT_VERSION;

    // THE TWO MASKS ARE NOT A BOOL, which is the easy thing to get wrong here. ControlMask says
    // which knob is being SET; StateMask says what to set it to. So:
    //   Control = EXECUTION_SPEED, State = 0                -> throttling OFF (full speed)
    //   Control = EXECUTION_SPEED, State = EXECUTION_SPEED  -> throttling ON  (EcoQoS)
    //   Control = 0,               State = 0                -> hands off, OS decides
    // Zeroing both is NOT "no throttling" -- it is "stop overriding", which is a third outcome.
    switch (p) {
    case TaskScheduler::PowerThrottling::OptOut:
        s.ControlMask = THREAD_POWER_THROTTLING_EXECUTION_SPEED;
        s.StateMask = 0;
        break;
    case TaskScheduler::PowerThrottling::Force:
        s.ControlMask = THREAD_POWER_THROTTLING_EXECUTION_SPEED;
        s.StateMask = THREAD_POWER_THROTTLING_EXECUTION_SPEED;
        break;
    // Topology cannot arrive here -- the worker entry resolves it to OptOut or Force. Falling into
    // SystemManaged rather than asserting is the conservative reading of "somebody added an
    // enumerator": stop overriding, do not guess at full speed.
    case TaskScheduler::PowerThrottling::Topology:
    case TaskScheduler::PowerThrottling::SystemManaged:
    default:
        s.ControlMask = 0;
        s.StateMask = 0;
        break;
    }
    (void)::SetThreadInformation(::GetCurrentThread(), ThreadPowerThrottling, &s, sizeof(s));
#else
    // SDK predates the API. Building against it is fine and running on a newer OS is fine; this
    // build just cannot ask.
    (void)p;
#endif
#else
    (void)p;
#endif
}
} // namespace

#if !JLIB_PLATFORM_WINDOWS
#include <sched.h>
#include <pthread.h>

// Bind another thread to a CPU set.
//
// glibc's pthread_setaffinity_np did not reach bionic until API 36, and Termux/NDK builds target
// far lower than that, so on Android this goes through the syscall wrapper directly. That takes a
// TID rather than a pthread_t -- and the caller here is the PARENT thread, which cannot pass 0
// ("me") because that would pin itself instead of the worker. pthread_gettid_np is the bionic
// extension that closes the gap; it has been there since API 21.
//
// Failure is deliberately swallowed on both paths, matching the Windows behaviour: a container or
// cgroup may legitimately forbid the CPU, and an unpinned worker is a performance question, not a
// correctness one. On Android that is not an edge case -- the platform's cgroups own thread
// placement, so these calls routinely fail (or succeed and are then overridden) for an
// unprivileged app. Treat Android placement as UNENFORCEABLE and never quote a number from it.
// Takes a plain 64-bit mask rather than a cpu_set_t so the CALL SITES stay portable: macOS has no
// cpu_set_t at all, and mentioning it in the policy switch would need the whole block #ifdef'd.
static inline void BindThreadToMask(pthread_t handle, const topology::CpuMask& mask)
{
#if JLIB_PLATFORM_DARWIN
	// macOS has NO thread-affinity API on arm64. THREAD_AFFINITY_POLICY still compiles but has been
	// a no-op since Apple Silicon -- the kernel owns placement and expresses intent through QoS
	// classes instead. So this is an honest no-op, not a stub awaiting an implementation: the
	// policy is unenforceable, which is why the docs say placement is Windows/Linux only.
	(void)handle; (void)mask;
#else
	// cpu_set_t is 1024 bits by default, so Linux never had the 64-CPU problem Windows has -- the
	// old ceiling here was purely the uint64_t this function used to take.
	cpu_set_t set;
	CPU_ZERO(&set);
	for (unsigned c = 0; c < topology::CpuMask::kMaxCpus; ++c)
		if (mask.Test(c) && c < CPU_SETSIZE) CPU_SET(c, &set);
  #if defined(__BIONIC__)
	(void)sched_setaffinity(pthread_gettid_np(handle), sizeof(cpu_set_t), &set);
  #else
	(void)pthread_setaffinity_np(handle, sizeof(cpu_set_t), &set);
  #endif
#endif
}
#endif

Thread::Thread(TaskScheduler& scheduler) : scheduler(&scheduler) {
	std::memset(&schedulerCtx, 0, sizeof(Context));
}
Thread::~Thread() {
}
void Thread::StartWorker(size_t cpu_affinity, size_t fiberCacheCapacity)
{
	auto ready = std::make_shared<std::atomic<bool>>(false);
	thread = std::thread([this, ready, fiberCacheCapacity]() {
		while (!ready->load(std::memory_order_acquire)) std::this_thread::yield();
		instance = this;
		thread_id = thread_counter.fetch_add(1);
		// ONCE, HERE, AT THREAD ENTRY. Deliberately not on any per-task or steal path -- it is a
		// syscall, and the deque CAS and the steal loop stay untouched by design.
		//
		// TOPOLOGY IS RESOLVED HERE rather than inside the helper, because this is the only place
		// that knows WHICH worker this is. isPCore is filled by BuildTopology, which runs before any
		// worker is released -- StartWorker stores `ready` only after it has placed this thread --
		// so the read is ordered, not hopeful.
		//
		// P-CORE -> OPT OUT, E-CORE -> THROTTLE. Never the other way: this scheduler sets an ideal
		// P-core for the workers it wants fast, and EcoQoS-ing one of those asks the OS for two
		// opposite things and delivers a clamped P-core. On a non-hybrid CPU every worker reports P,
		// so nothing is throttled, which is the right answer there.
		{
			TaskScheduler::PowerThrottling pt = TaskScheduler::GetWorkerPowerThrottling();
			if (pt == TaskScheduler::PowerThrottling::Topology) {
				const bool onECore = ((size_t)qIndex < scheduler->isPCore.size())
				                     && !scheduler->isPCore[(size_t)qIndex];
				pt = onECore ? TaskScheduler::PowerThrottling::Force
				             : TaskScheduler::PowerThrottling::OptOut;
			}
			ApplyPowerThrottling(pt);
		}
		// ThreadLocalCache::Initialize clamps this to its MaxCapacity and floors it at 2.
		localCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity);
		this->Worker();
		});
	nativeHandle = thread.native_handle();

	// ---- THE REMAINING CEILING: CpuMask::kMaxCpus, not 64 ----
	// The 64-CPU limit this guard originally enforced is gone. Windows binding now goes through
	// SetThreadGroupAffinity / SetThreadIdealProcessorEx, which take the processor group as DATA
	// rather than inheriting it from the calling thread, and every mask in the scheduler is a
	// CpuMask rather than a uint64_t. What is left is the CpuMask width itself.
	//
	// This is still worth guarding rather than trusting, because the failure it prevents is silent.
	// A CpuId past the mask width would index off the end of efficiencyClass and set no bit at all,
	// so the worker would appear bound while being bound to nothing, which reads as unexplained
	// slowness. Refusing and saying so costs one branch per worker at startup.
	//
	// Degrading to None is not a capacity loss: the worker is still created, still stealing, still
	// running tasks. Only its PLACEMENT is dropped, which is exactly AffinityPolicy::None, and its
	// locality becomes approximate. Raising the cap is CpuMask::kWords in Topology.h.
	auto affinityPolicy = scheduler->GetAffinityPolicy();

	// PIN THE HOT WORKERS ONLY. Hard-pinning the WHOLE pool was measured harmful -- ~45% worse wake
	// latency, ~2x on a frame DAG -- which is why the default is Ideal. That finding is about the
	// pool: a pinned worker cannot escape a core someone else is occupying, and with N workers the
	// chance that SOME core is occupied approaches certainty.
	//
	// A hot worker looked like the opposite case -- it never parks, so it has no wake latency to
	// lose, and there are one or two rather than N. IT MEASURED WORSE ANYWAY, and badly: burst p99
	// 27ms against a 150us baseline, HOT p99 up to 10ms, and pin+RT worse than RT alone.
	//
	// Because this is taskset-style pinning: the worker is confined to a core, but NOTHING ELSE IS
	// EXCLUDED FROM IT. When the completion thread or an OS thread lands on that CPU the hot worker
	// cannot migrate away and simply waits; unpinned it moves to whichever core is free, which with
	// 29 workers plus service threads on ~32 logical CPUs it constantly needs to.
	//
	// isolcpus-style isolation -- the core RESERVED so nothing else is scheduled there -- is the
	// opposite arrangement and is NOT refuted by this. It also cannot be tested without booting for
	// it. Kept as a knob for exactly that case, off by default, and do not enable it expecting a win
	// on an ordinary machine.
	//
	// Requires SetHotWorkers BEFORE Init -- qIndex is set by SetQueueIndex before StartWorker, but
	// the hot COUNT has to already be known here.
	if (TaskScheduler::GetHotWorkerPin() && TaskScheduler::GetHotWorkers() > (size_t)qIndex)
		affinityPolicy = TaskScheduler::AffinityPolicy::Hard;
	if (cpu_affinity >= topology::CpuMask::kMaxCpus &&
	    affinityPolicy != TaskScheduler::AffinityPolicy::None) {
		affinityPolicy = TaskScheduler::AffinityPolicy::None;
		static std::atomic<bool> warned{ false };
		if (!warned.exchange(true, std::memory_order_relaxed)) {
			std::cerr << "[JLib::Scheduler] logical CPU " << cpu_affinity
			          << " is past CpuMask::kMaxCpus (" << topology::CpuMask::kMaxCpus
			          << ") -- that worker runs unbound. Raise CpuMask::kWords in Topology.h.\n";
		}
	}

#if JLIB_PLATFORM_WINDOWS
	// HOW this worker is bound to its core, per TaskScheduler::SetAffinityPolicy.
	//
	// Hard  -- SetThreadAffinityMask: the thread runs on that logical CPU or nowhere. Best cache
	//          locality and the only mode where "where does oversubscription land" is a decision
	//          rather than a discovery. Worst case: another process pins something to the same core
	//          and this worker is stuck behind it with no escape.
	// Ideal -- SetThreadIdealProcessor: a strong HINT. Windows keeps the thread there when it can,
	//          so cache locality and the topology map still hold, but it may migrate under contention
	//          or thermal pressure. Notably it also leaves core parking and Thread Director able to
	//          do their jobs.
	// None  -- leave placement entirely to Windows. Only sensible if this library is embedded in an
	//          application that owns thread placement itself, which is a real consideration for a
	//          LIBRARY that would otherwise hard-pin N threads inside someone else's process.
	//
	// NOTE the topology-aware steal ordering (SMT sibling first, then cache cluster) is only
	// meaningful under Hard or Ideal: if threads migrate freely, "my sibling" no longer describes
	// where any data actually is. Pinning and locality-aware stealing are ONE decision, not two.
	// Both calls are the GROUP-AWARE variants, and that is the whole reason this scheduler can
	// address a machine wider than 64 logical CPUs. SetThreadAffinityMask takes a bare 64-bit mask
	// interpreted in the CALLING thread's group, so it cannot name a CPU in another group at all --
	// the group is not a parameter, it is ambient. SetThreadGroupAffinity takes the group as data.
	// Same story for SetThreadIdealProcessor versus its Ex form, which swaps a bare DWORD for a
	// PROCESSOR_NUMBER carrying {Group, Number}.
	//
	// On a machine with one group these are exactly equivalent to what they replaced, so nothing
	// changes for the overwhelming majority of hardware. The flat CpuId splits into its two halves
	// by construction (see Topology.h): group is the high bits, processor number is the low six.
	const WORD  cpuGroup  = (WORD)topology::CpuMask::GroupOf((topology::CpuId)cpu_affinity);
	const BYTE  cpuNumber = (BYTE)topology::CpuMask::BitOf((topology::CpuId)cpu_affinity);

	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:   // one worker per physical core -- still a hard bind
	case TaskScheduler::AffinityPolicy::Hard: {
		GROUP_AFFINITY ga{};
		ga.Mask  = (KAFFINITY)(1ULL << cpuNumber);
		ga.Group = cpuGroup;
		SetThreadGroupAffinity(nativeHandle, &ga, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		PROCESSOR_NUMBER pn{};
		pn.Group  = cpuGroup;
		pn.Number = cpuNumber;
		SetThreadIdealProcessorEx(nativeHandle, &pn, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::None:
		break;
	}
#else
	// Linux. Same four policies, but only ONE of them has anything to do.
	//
	// Hard/PhysicalOnly -> pthread_setaffinity_np with a single-CPU set: the direct analogue of
	//                      SetThreadAffinityMask, with the same tradeoff (best locality, no escape
	//                      if something else lands on that core).
	//
	// Ideal -> bind to this worker's WHOLE LLC DOMAIN, not to one core. Linux has no equivalent of
	//          SetThreadIdealProcessor: affinity here is all-or-nothing per CPU, with no "hint".
	//          But sched_setaffinity takes a MASK, so the same INTENT -- keep locality true with
	//          minimum rigidity -- is expressed at a coarser granularity than Windows can. The
	//          kernel may place the thread on any core in the domain, so a wake lands on one that
	//          is already awake rather than waiting for a specific parked core, which is where
	//          hard pinning's ~45% wake-latency cost came from.
	//
	//          The mask ends up EXACTLY AS TIGHT AS THE HARDWARE WARRANTS, which is the point:
	//            - Multi-L3 (Ryzen CCDs, Threadripper, EPYC, multi-socket): it genuinely binds, and
	//              that is where it matters. An unbound worker migrating across cache domains pays
	//              inter-die latency on every steal AND makes clusterMates describe a machine state
	//              that does not exist.
	//            - Single-L3 (most Intel desktop): the domain is every CPU, so the mask binds
	//              nothing. Correct, not a gap -- there is no cache domain to protect, and binding
	//              anyway would only add the rigidity that measured ~45% worse on wake latency.
	//
	//          What this does NOT protect is siblingQIndex: the kernel can still migrate between
	//          physical cores inside the LLC, so "my SMT sibling" stays approximate on Linux.
	//          Deliberate. Tightening to a 2-CPU SMT-pair mask would buy a steal-ORDERING
	//          preference at close to hard pinning's cost -- the wrong side of that trade.
	//
	//          So the POLICY means the same thing on both platforms and only the MECHANISM differs.
	//          UNMEASURED on real Linux hardware: WSL virtualises topology and has no real core
	//          parking, so the placement effect cannot be benchmarked there. Falls back to no
	//          binding if topology was unavailable (mask 0), which is the old behaviour.
	//
	// None -> nothing, same as Windows.
	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:
	case TaskScheduler::AffinityPolicy::Hard: {
		topology::CpuMask one;
		one.Set((topology::CpuId)cpu_affinity);
		BindThreadToMask(nativeHandle, one);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		const size_t qi = (size_t)qIndex;   // set by SetQueueIndex before StartWorker
		if (qi < scheduler->llcMaskOfWorker.size()) {
			const topology::CpuMask& llc = scheduler->llcMaskOfWorker[qi];
			if (llc.Any()) BindThreadToMask(nativeHandle, llc);
		}
		break;
	}
	case TaskScheduler::AffinityPolicy::None:
		break;
	}
#endif
	ready->store(true, std::memory_order_release);
};
std::thread::id Thread::GetID() {
	return thread.get_id();
}
void Thread::SetQueueIndex(size_t index)
{
	qIndex = index;
};

bool Thread::DrainOwnInboxesToDeques() {
	// Pushes to our OWN deque, and NOT via Requeue the way Worker()'s pre-immediate drain does. That
	// difference is about THIS caller, not about Requeue -- the other drain is correct:
	// an immediate task its own core is already flagged, and PickNextWorker skips flagged workers.
	// Requeue there provably cannot hand a task back into the inbox being emptied.
	//
	// A worker spinning in WaitFor carries NO such flag -- it is running an ordinary task -- so
	// PickNextWorker can and will select it, and Requeue would put the task straight back where we
	// found it. Taking the flag to borrow that guarantee is worse than it looks: it marks the core
	// pinned for ALL placement, makes targeted Push(cpu, task) start returning false against us, and
	// has to be cleared on every exit from the spin. push_bottom needs none of that and is monotone
	// progress -- once on the deque the task is stealable by every other worker AND visible to our
	// own GetTask, which scans every deque including this one.
	const size_t BATCH = 64;
	Task* batch[BATCH];
	bool moved = false;

	auto drain = [&](TaskMPSCQueue* inbox, TaskDeque* deque, bool hiPriLane) {
		for (;;) {
			size_t count = 0;
			while (count < BATCH && inbox->pop(batch[count])) count++;
			if (count == 0) break;
			TaskScheduler::NoteInboxDrain(count);   // no-op unless a submit limit is set
			for (size_t i = 0; i < count; ++i) {
				if (!batch[i]) continue;
				// THE POP ALREADY HAPPENED, so a refusal here could not be retried -- the inbox no
				// longer has it. Ignoring the bool (which this once did) LOST the task: its WaitGroup
				// never decremented and whoever waited on it hung.
				//
				// THE DEQUE NO LONGER REFUSES: it grows, and if it cannot grow it aborts with a
				// message rather than handing back a failure nobody can act on here. The only
				// remaining false is a null item, which the guard above has already excluded --
				// hence the assert rather than a fallback path that can never run.
				// NOT AN assert: that evaporates under /O2, and the failure it would have caught is a
				// SILENTLY DROPPED TASK in exactly the build where nobody is watching -- the fiber
				// stays suspended forever and the hang has no stack trace pointing here. A crash with
				// a minidump beats that every time.
				if (!deque->push_bottom(batch[i])) TaskDeque::FatalPushRefused();
				moved = true;
			}
		}
	};

	drain(scheduler->hiPriInboxes[qIndex].get(), scheduler->hiPri[qIndex].get(), true);
	drain(scheduler->loPriInboxes[qIndex].get(), scheduler->loPri[qIndex].get(), false);
	return moved;
}
void Thread::Join() {
	bool expected = false;
	if (!joining.compare_exchange_strong(expected, true)) return;

	running.store(false, std::memory_order_release);
	NotifyWorker(/*force*/ true);   // shutdown flips `running`, not hasQueuedWork -- see NotifyWorker

	std::unique_lock<std::mutex> lock(joinMutex);
	cvWorkerDone.wait(lock, [this] {
		return !running.load(std::memory_order_acquire);
		});

	if (thread.joinable())
		thread.join();

	joining.store(false, std::memory_order_release);
}
Thread* Thread::GetCurrent() { 
	return instance; 
}

void Thread::CoYield(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->CoYield();
	}
}
void Thread::Suspend(Fiber* targetFiber){
	if (targetFiber) {
		targetFiber->Suspend();
	}
}
 void Thread::Resume(Fiber* targetFiber) {
	 if (targetFiber) {
		 targetFiber->Resume(); 
	 }
}

 void JLib::Thread::CoYield()
 {
	 GetCurrent()->currentFiber->CoYield();
 }

 void JLib::Thread::Suspend()
 {
	 GetCurrent()->currentFiber->Suspend();
 }

void Thread::NotifyWorker(bool force){
	// THE OPTIMISATION: if this worker is running, say nothing. It clears hasQueuedWork and
	// re-searches every loop pass, so it will find the task on its own, and MarkQueuedWork has
	// already been stored by every caller that reaches here. Skipping saves a mutex acquire/release
	// plus a condition-variable signal, and when the worker is actually parked the signal is a
	// kernel thread wake costing microseconds.
	//
	// That cost is not theoretical. While the pool is saturated nobody is waiting on the condvar and
	// the signal is nearly free, but once the pool drains faster than one thread can submit, every
	// push buys a real wake -- which is why single-producer submission used to collapse from 3.4 M/s
	// at 8 workers to 0.8 at 14+, a threshold rather than a gradient.
	//
	// The load is seq_cst and pairs with the seq_cst STORES of every flag the sleep decision reads:
	// hasQueuedWork (MarkQueuedWork), laneWake (MarkLaneWake) and paused (Pause/Resume). Both
	// operations of each pair have to be in one total order or the skip is unsound -- see
	// tests/verify/sleepwake_model.c.
	//
	// THIS SHIPPED BROKEN ONCE, in 1.2.0, and the reason is worth keeping. The first model had ONE
	// flag and proved the handshake for it. The predicate had more. The others were left as release
	// stores read with acquire, so for those the total-order argument did not exist:
	// the setter stores its flag, loads workerState, sees AWAKE, skips -- while the worker stores
	// GOING_TO_SLEEP, loads the flag, sees the stale value, and parks forever. It hung macOS arm64
	// in CI about one run in three and never once reproduced on x86, because TSO hides it.
	//
	// The flag that bug was found on -- `immediate` -- is gone with PushImmediate (4.0.1), but the
	// control survives it: `-DLANE_ONLY -DWEAK_LANEWAKE` is the same shape on laneWake and fails the
	// same way. If ANOTHER input is ever added to the sleep predicate, it must be
	// seq_cst on both sides and it must go into that model. A proof covers what it modelled.
	if (!force && workerState.load(std::memory_order_seq_cst) == WS_AWAKE) return;

	// The empty lock is load-bearing: cv.notify_one() without synchronizing on workerMutex
	// can land in the window AFTER Worker()'s sleep predicate evaluated false but BEFORE it
	// actually blocks -- the notify is dropped, the flag store is never re-checked, and the
	// worker sleeps on a non-empty inbox forever. Inboxes are only drainable by their owner
	// (steals never scan them), so one lost wakeup = that task stranded permanently (this was
	// the ParallelFor heisen-deadlock). Acquiring the mutex forces the notify to land either
	// before the predicate runs (it sees the flag) or after the worker is blocked (it wakes).
	{ std::lock_guard<std::mutex> g(workerMutex); }
	cv.notify_one();
}

bool Thread::Ready(){
	return ready.load(std::memory_order_acquire);
}
       
Fiber* Thread::AcquireFiber(Task* task) {
	Fiber* f = localCache.Pop();
	if (f) return f;

	f = localCache.Pop();

	if (!f) {
		// ONCE PER PROCESS, not once per failure. Exhaustion is not a single event: the caller
		// re-queues the task and yields, so a pool that is genuinely short spins through this path
		// millions of times. Printing each one turned an eight-minute stall into an eight-minute
		// stall that also floods the console with synchronised writes, which is slower AND hides
		// the one line that explains it. The condition is a capacity problem, so one line naming
		// the cap and the cause is all a reader can act on.
		static std::atomic<bool> warned{ false };
		if (!warned.exchange(true, std::memory_order_relaxed)) {
			// Report the ACTUAL configured budget, not the default. This line used to say
			// "(64 per core by default)" unconditionally, so a reader who had already raised the
			// budget was told a number that was not theirs and could not tell whether their call
			// had taken effect -- the one thing the message exists to help them decide.
			const size_t perWorker = TaskScheduler::StandardFibersPerWorker();
			const size_t workers   = scheduler ? scheduler->GetWorkerCount() : 0;
			std::cerr << "[JLib::Scheduler] fiber pool exhausted. A SUSPENDED task holds its fiber, "
			             "so the number of tasks that may be blocked AT ONCE is capped by the pool: "
			          << perWorker << " standard per worker";
			if (workers) std::cerr << " x " << workers << " workers = " << (perWorker * workers);
			std::cerr << " total. Past that, workers re-queue and retry instead of running.\n"
			             "  Usually that is a STALL that clears as blocked tasks finish. INSIDE A "
			             "TaskDAG IT MAY NEVER CLEAR: if the tasks holding the fibers are waiting on "
			             "work that cannot get a fiber because they are holding them all, nothing "
			             "progresses again. A DAG whose concurrently-suspended nodes outnumber the "
			             "budget above is the shape to look for.\n"
			             "  Either block fewer tasks concurrently, or call "
			             "TaskScheduler::SetFiberBudget(standardPerWorker, heavyPerWorker) BEFORE "
			             "Init() to raise it. This warning prints once.\n";
		}
	}
	return f;
}

void Thread::ReleaseFiber(Fiber* f) {
	localCache.Push(f);
}

uint32_t Thread::FastRand() {
	static thread_local uint32_t x = []() {
		auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
		uint32_t seed = static_cast<uint32_t>(now);
		seed ^= (std::hash<std::thread::id>{}(std::this_thread::get_id()) << 1);
		return seed == 0 ? 1 : seed;
		}();	
	x ^= x << 13;
	x ^= x >> 17;
	x ^= x << 5;
	return x;
}
Task* JLib::Thread::AcquireWork(bool& isFork)
{
	return nullptr;
}
void JLib::Thread::RunTask(Task* task, bool isFork)
{}
void Thread::Worker() {
	running.store(true, std::memory_order_release);
	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	static thread_local Task* task_to_run = nullptr;
	// IdlePolicy spin state for THIS worker. Both are reset the moment work is found, not only on
	// the fall-through to park -- otherwise a worker that spun most of its budget and then got a
	// task would carry that count into its next idle episode and park early ever after.
	unsigned idleSpins = 0;
	// TRUE while this worker holds the elevated priority. Raised when it becomes hot, and DROPPED
	// again when it stops -- it used to be one-way, which was correct only while the hot set was
	// static. Dynamic K made "was hot once" permanent while K itself sheds; see the stand-down
	// branch in the idle section for why that mattered under NoSleep.
	// Cross-platform again as of the POSIX port -- ApplyHotPriority has a backend on every OS now.
	bool hotPriorityRaised = false;
	// Sample counter for the dynamic-K controller; only worker 0 ever looks at it.
	unsigned hotCtlTick = 0;
	unsigned laneDutyTick = 0;
	// Generation-driven lane reconciliation. Seeded from the current generation so a worker that
	// starts life after a K change does not treat its own startup as a transition.
	// "This worker may have raised its OWN lane bit." Set while hot, and by the stray drain; cleared
	// when the bit is retired. Sound because nobody else can raise this worker`s bit from zero.
	bool ownsLaneBit = TaskScheduler::GetHotWorkers() > (size_t)qIndex;
	// Did this worker execute a LANE task on the previous pass? The occupancy numerator. A worker
	// busy bit is the wrong thing here: general help and pure spin must read as not-lane-busy, or
	// demotion never fires.
	bool ranLaneTaskLastPass = false;
	long long laneStartNs = 0;
	// Tracks what this thread`s priority actually IS, so the per-task adjust below is a syscall only
	// on a genuine change. Meaningless unless hotPriorityRaised.
	bool atCriticalPriority = false;

	// EXCLUSIVE MODE: an ORDINARY worker gets off the hot cores. The hot workers themselves are
	// already pinned to them by StartWorker. Done here, at loop entry, because by now every worker's
	// CPU is assigned and the hot mask was published before any of them started.
	if (!(TaskScheduler::GetHotWorkers() > (size_t)qIndex))
		TaskScheduler::ExcludeCurrentThreadFromHotCpus();
	while (running.load(std::memory_order_acquire)) {
		// TWO DIFFERENT QUESTIONS, and conflating them deadlocks the pool.
		//
		//   servesHiPri  do I DRAIN the lane? Hot workers only. FALSE FOR EVERYONE AT K=0, where
		//                nothing can enter a hiPri lane and scanning one is dead work.
		//   isHotWorker  am I a DEDICATED lane server -- never steal, never park? False for everyone
		//                at K=0. These are NOT the same question, and an earlier version keyed the
		//                "does not steal" rule off the wrong one.
		//
		// Read once per pass; both are relaxed loads and re-read every pass, so SetHotWorkers stays
		// safe to call on a running pool.
		const bool servesHiPri = TaskScheduler::WorkerServesHiPri((size_t)qIndex);
		// INSURANCE, not a scan: one load of a line this worker already owns. Covers the only way a
		// task can sit in a lane nobody serves -- SetHotWorkers being LOWERED while work is queued.
		// Costs nothing in the normal case because the queue is empty and the check short-circuits.
		// BOTH structures, because the inbox drains INTO the deque -- work can sit in the deque with
		// the inbox already empty, and checking only the inbox would strand exactly that case.
		const bool hiPriStray = !servesHiPri &&
			(!scheduler->hiPriInboxes[qIndex]->empty() || scheduler->hiPri[qIndex]->size() != 0);
		const bool isHotWorker = TaskScheduler::GetHotWorkers() > (size_t)qIndex;
		// Read once per pass beside isHotWorker: one relaxed load of a line the app touches only when
		// it reconfigures. Gates every piece of adaptive-K bookkeeping, so static K -- the default --
		// runs none of it.
		const bool adaptiveK = TaskScheduler::HotScalingActive();

		// Steal hints, maintained here and nowhere else: this is the one place the owner of this
		// deque reliably passes, and size() reads a line it already owns. Both writes are
		// conditional on a state CHANGE, so a queue that stays deep, or stays empty, never writes at
		// all. The hint goes stale while a long task runs -- the worker is not looping then -- which
		// can only cost a wasted probe, never hide work: the PARALLELISM bit is set by the splitter
		// at push time, before the task it split off can be picked up by anyone.
		{
			const size_t depth = scheduler->loPri[qIndex]->size();
			scheduler->UpdateBacklogHint((size_t)qIndex, depth);
			scheduler->ClearParallelHintIfEmpty((size_t)qIndex, depth);
			// The lane's own retirement path, for the case no thief covers: a hot worker that
			// drained its own backlog by popping. ONCE PER PASS, not per pickup -- that frequency
			// difference is the entire cost argument, and a pass is exactly when this worker has
			// nothing better to do anyway.
			if (isHotWorker) {
				const size_t laneDepth = scheduler->hiPri[qIndex]->size();
				scheduler->UpdateLaneHint((size_t)qIndex, laneDepth);
				ownsLaneBit = true;   // while hot, this worker is the one maintaining its bit

				// ---- DUTY CYCLE, and both halves of it were wrong once ------------------------
				//
				// COUNTED BY THE WORKER BEING MEASURED. The central-sampler version polled the top
				// worker from ONE driver worker's loop and gathered one or two samples per window
				// under light load, because its density depended on a third party's loop rate.
				//
				// EVERY PASS, NOT 1-IN-16. The pass RATE is inversely related to how busy a worker
				// is -- one pass per task when working, millions per second when spinning -- so any
				// per-pass divisor starves the sample count of exactly the state being detected.
				// 256 tasks yielded 16 samples, under the minimum, so the controller never decided.
				//
				// BUSY MEANS "RAN A TASK", NOT "HAS A QUEUE", and that distinction is what the
				// dispatch bench caught. A hot worker that is perfectly keeping up drains each
				// completion the pass it arrives, so its deque is empty at almost every sample --
				// fully utilised and reading as idle. Queue depth measures BACKLOG, which is what
				// stealHintLane is already for; utilisation is what decides whether a core is
				// earning its keep. The flag is set after the search, so this counts the PREVIOUS
				// pass, which is the only point at which the answer is known.
				// adaptiveK-gated: under static K nobody reads this, so nobody should pay for it.
				if (adaptiveK) laneCyclesTotal.fetch_add(1, std::memory_order_relaxed);
			}
			// CLOSED OUTSIDE THE HOT GUARD, and that is not tidiness. Inside it, a worker demoted
			// between setting the flag and reading it keeps the flag set until it is promoted again
			// -- and then books the ENTIRE INTERVENING GAP as lane time. Observed directly: 8.1
			// SECONDS of occupancy inside a 200 ms window, which is how the bug announced itself
			// rather than quietly inflating K. The stamp is closed wherever the worker next passes.
			if (ranLaneTaskLastPass) {
				laneBusyNs.fetch_add(MonotonicNs() - laneStartNs, std::memory_order_relaxed);
				ranLaneTaskLastPass = false;
			}
			// ---- RETIRING A BIT LEFT BEHIND BY A DEMOTION -----------------------------------
			//
			// THE LEAK THIS CLOSES, and it only exists once K can move. The line above is a worker's
			// own bit, maintained while it is hot. The moment it stops being hot that line stops
			// running -- so a bit set during the hot era is never cleared by anyone, and every thief
			// keeps probing an empty lane on that worker forever.
			//
			// The drain path covers the case where the demoted worker still HOLDS lane work (it is
			// gated on `servesHiPri || hiPriStray` and calls UpdateLaneHint(count - 1)). What it
			// cannot cover is the end: once the queues are empty the drain stops running, and the
			// last value it published may still have been above the threshold.
			//
			// FOUND BY A TEST PRECONDITION, not by reading: SchedulerDynamicKTest asserts worker 3's
			// bit is clear before its push, and that assertion failed against a bit left over from
			// an earlier section of the same run.
			//
			// DRIVEN BY A GENERATION COUNTER, not by tracking whether we set the bit. The first
			// version kept a per-worker "my bit might be set" bool, which was cheaper -- but its
			// invariant had to be maintained across all four UpdateLaneHint call sites, two of which
			// are THIEVES writing a victim's bit. Tracing four sites to convince yourself a local
			// flag is still true is the shape that has drifted in this codebase before. The
			// generation says only "K moved, re-derive," which is self-contained.
			//
			// STICKY UNTIL RESOLVED, because the bump and the fix cannot always happen together: a
			// worker demoted while still holding lane work must NOT clear its bit yet -- the drain is
			// still maintaining it and helpers should still see it. So the pending flag survives
			// until the queues are actually empty, at which point no further generation bump would
			// ever come.
			else {
				// LEVEL-TRIGGERED ON MY OWN STATE, and the generation counter it replaces was the
				// wrong instrument -- it fires once per K CHANGE, and the bit can be set again
				// afterwards by something that is not a K change:
				//
				//   1. K drops. The generation bumps, this worker wakes and reconciles, bit clear.
				//   2. hiPri work now arrives at this NON-hot worker, and the hiPriStray drain
				//      publishes UpdateLaneHint(q, count - 1) -- count is the batch size, so the bit
				//      is SET again.
				//   3. The worker pops those items one at a time. Pops never touch the hint.
				//   4. The queues go empty and no further K change ever comes, so the one-shot
				//      reconcile never runs again. The bit is stale forever.
				//
				// That is a 1-in-3 flake, because it depends on whether work lands before or after
				// the reconcile. Caught by a pool dump showing workers SLEEPING with 0/0 queues and
				// their bits set -- which also disproved the "still holding work" reading.
				//
				// A LOCAL FLAG IS SOUND HERE, though it was rejected earlier for a reason that turns
				// out not to apply: worker q's bit is only ever set by q itself. A thief can write a
				// victim's hint, but only after PROBING it, and probing requires the bit to be set
				// already -- so no thief can raise a bit from zero. The invariant is two sites in
				// this worker's own loop, not four across the file.
				if (ownsLaneBit && !hiPriStray) {
					// Clears if set, and early-outs without a write if it is not.
					scheduler->UpdateLaneHint((size_t)qIndex, 0);
					ownsLaneBit = false;
				}
			}
			// ---- WHY A K TRANSITION NEEDS NO HAND-OFF, which is NOT obvious ------------------
			//
			// Demoting a worker that still holds lane work looks like it must strand or at least
			// hide it. It also looks like a transition-time check would fix that -- and such a
			// check would itself race the producer: hotN drops, the demoted worker looks at its
			// queues, finds them empty and advertises nothing, and only THEN does an in-flight push
			// land on it.
			//
			// NOTHING IS NEEDED, and this was established with a negative control rather than by
			// reading. The section-5 drain is gated on `servesHiPri || hiPriStray`, so it runs for a
			// STRAY worker too, and it already calls UpdateLaneHint(count - 1). A demoted worker
			// therefore advertises whatever it is left holding, no matter how or when that work
			// arrived. An explicit hand-off was written here, tested, and found REDUNDANT --
			// SchedulerDynamicKTest passes unchanged with it removed, which is exactly how it was
			// caught, after two earlier versions of that test passed with it removed for the wrong
			// reason.
			//
			// The hand-off only ever reached depths BELOW kLaneStealDepth. Chasing those was
			// measured on 8-26 and loses: at set=2 the tail got worse in three rows of four and
			// steal probes roughly doubled. So the one case not covered here is a case the design
			// declines on purpose.

			// ONE worker drives the controller, and it is SAMPLED. A clock read on every pass of
			// every worker would cost more than the mechanism saves. Worker 0 is always hot under
			// Dynamic (minK >= 1), so it never parks and the down-ramp still gets evaluated on a
			// completely quiet lane -- which is the case the whole down direction exists for.
			// ANY hot worker drives the controller, not worker 0.
			//
			// Pinning it to worker 0 starved it in exactly the case it exists for. Worker 0 is the
			// LOWEST hot index, so under a lane that is not yet saturated it is the one actually
			// running tasks -- one loop pass per task -- while the higher hot workers spin freely.
			// The driver was therefore slowest precisely when the surplus workers above it were most
			// idle. Measured: THIRTEEN controller evaluations in an entire test run, and none during
			// a 2.5 s trickle, so the demote path was never reached and K could not shed.
			//
			// Letting every hot worker drive it costs nothing extra per worker and makes evaluation
			// frequency scale with how idle the hot set is -- which is the right way round, since an
			// idle hot set is what the demote path is looking for. Concurrent evaluations are already
			// safe: both directions are rate-limited on their own clocks and the window gate admits
			// one decision at a time.
			if (isHotWorker && adaptiveK && ((++hotCtlTick & 0x3F) == 0))
				TaskScheduler::MaybeAdjustHotWorkers();
		}

		ready.store(true, std::memory_order_release);
		// Cleared once per iteration, unconditionally, BEFORE this iteration's own-queue/steal/
		// inbox-drain search (steps 3-5 below) -- so any push that landed before this clear gets
		// found directly by that same search (turning task_to_run non-null, never reaching the
		// sleep predicate at all this iteration), and any push landing AFTER the clear re-arms
		// this flag (via MarkQueuedWork(), called by whichever push targeted this worker) for
		// the sleep predicate to see fresh, later in this same iteration. See hasQueuedWork's
		// declaration comment in Thread.h for the full reasoning.
		// seq_cst, NOT relaxed, and the whole comment above depends on it. A relaxed store is not
		// ordered against the loads that follow, so it may SINK PAST the drain below: the worker
		// searches and finds nothing, a push then lands and sets this flag seq_cst, and only then
		// does the stale clear land and wipe it. The worker parks with a task in its own inbox and
		// the only signal for it destroyed. That is the lost wakeup, and it is why the reasoning
		// above ("a push landing AFTER the clear re-arms the flag") did not hold in practice: the
		// clear had no defined position relative to the search it is supposed to precede.
		//
		// This is a tightening, not the guarantee. Ordering the clear cannot make the flag alone
		// sufficient, because the flag and the queue are separate objects and no single operation
		// observes both -- a push whose queue write is not yet visible to the drain can still be
		// missed. That is why the park decision consults the inboxes directly; this just removes the
		// reordering that made the window easy to hit.
		hasQueuedWork.store(false, std::memory_order_seq_cst);
		// Same clear, same place, same reason -- and for laneWake the once-per-pass clear is also what
		// BOUNDS it: a lane wake buys exactly one search pass. See its declaration in Thread.h.
		laneWake.store(false, std::memory_order_seq_cst);
		// --- 1. Execute task if found ---
		if (task_to_run) {
			// LANE OCCUPANCY, read on the next pass. One site, and it is the one place the loop
			// knows the answer -- see the duty-cycle block above.
			//
			// task->hiPri, NOT "a task ran". A hot worker that helps with ordinary work is not
			// occupied by the LANE, and counting it as such is the "any work on that thread" metric
			// that never demotes: the core looks busy while the deadline path it was provisioned
			// for is empty. K exists for the deadline path, so the numerator has to be the deadline
			// path.
			// adaptiveK FIRST, so static K -- min == max, the DEFAULT and what SetHotWorkers(k)
			// pins -- pays nothing for a mechanism that cannot act. A clock read per lane task plus
			// three counters is not much, but it is charged on the latency path, to every user who
			// never asked for scaling, to be discarded by the controller's first early-out.
			if (adaptiveK && task_to_run->hiPri != 0 && isHotWorker) {
				laneStartNs = MonotonicNs();
				ranLaneTaskLastPass = true;
				laneTasksRun.fetch_add(1, std::memory_order_relaxed);
			}
			// CANCELLATION, OBSERVED AT PICKUP. One flag in the scope; every task carrying a token
			// to it reads the new value the next time it is touched, and this is one of those
			// points. Nothing was removed from any queue to make this work -- which is exactly why
			// it reaches a task parked anywhere, including an Event's lock-free waiter stack, with
			// no change to Event at all.
			//
			// ONLY AN UNSTARTED TASK MAY BE DISCARDED. A queued entry may be a RESUME: Thread::Resume
			// ends in Requeue(owningTask), so a suspended fiber returns through these same queues,
			// and a coroutine awaiter re-pushes its Task the same way. Discarding one of those
			// abandons a live stack or frame rather than cancelling it -- see Task::started. A
			// started task is let through to run, and observes the cancellation at its own next
			// suspend point or poll, where it can unwind properly.
			// THROUGH DiscardIfCancelled, NOT AN INLINE COPY. This site used to open-code the
			// disposal -- waitGroup down, cleanup, destroy, free -- which made it a FOURTH discard
			// path that the shared one knew nothing about. The copy was missing the step added when
			// the DAG stopped being notified by fn alone: a discarded graph node still has to tell
			// its dependents, or an AND countdown can never reach zero and the graph, plus anyone
			// in WaitFor on it, stops forever.
			//
			// That is exactly what hung dag_cancel_test on POSIX: the worker discarded a cancelled
			// node here, released only its own WaitGroup slot, and the sink was never fired.
			// Windows almost always ran the body before the cancel landed and never reached this
			// branch at all. "One place cancellation is decided" is a property of the CALL SITES,
			// and this is the third time a copy of it has drifted.
			if (scheduler->DiscardIfCancelled(task_to_run)) {
				task_to_run = nullptr;
				if (EpochManager::Instance().ShouldSelfReclaim()) EpochManager::Instance().Tick();
				ready.store(true, std::memory_order_release);
				continue;
			}
			task_to_run->started = 1;

			// DEMOTE BEFORE RUNNING ORDINARY WORK. A hot worker sits at TIME_CRITICAL so it can
			// take a completion the instant one lands -- but it also steals general work when its
			// inbox is empty, and running a long ordinary task at priority 15 is exactly the
			// starvation the elevated priority was supposed to be worth the risk of.
			//
			// hiPri IS the discriminator, and it needs no new state: the queue already exists, the
			// app already sets the flag at Spawn, and "this task is worth pre-empting others for"
			// is precisely what it means. So the worker runs hiPri work elevated and everything
			// else at Normal, per task.
			//
			// Cached, because SetThreadPriority is a syscall and this is the per-task path -- it
			// fires only when the level actually changes, which for a steady stream of one kind of
			// work is never.
			if (hotPriorityRaised) {
				const bool wantCritical = (task_to_run->hiPri != 0);
				if (wantCritical != atCriticalPriority) {
					ApplyHotPriority(wantCritical);
					atCriticalPriority = wantCritical;
				}
			}

			// Fast path: run directly on THIS worker's own OS-thread stack, no fiber acquired
			// or ContextSwitch paid at all. Only safe because Native tasks are a CONTRACT --
			// they must never call WaitOnEvent*/anything that suspends (there's no fiber here
			// to switch away to). assignedFiber is deliberately left nullptr for these tasks,
			// which is exactly what WaitOnEvent*'s guards check for -- a mismarked Native
			// task that tries to suspend anyway fails loudly there instead of corrupting the
			// worker's real call stack.
			// Coroutines ride this path too: resuming one is a plain fn(data) call -- fn is a
			// trampoline, data is the handle address -- that runs on this worker's stack and
			// returns, exactly like a Native task. That type erasure is what keeps <coroutine> and
			// C++20 out of the core entirely. They differ only in who completes them (below).
			if (task_to_run->type == TaskType::Native || task_to_run->type == TaskType::Coroutine) {
				// READ BEFORE Execute(), and this is load-bearing rather than tidy. A coroutine's
				// resume can run the body to completion, and completion frees both the frame and
				// this Task -- so `task_to_run` may be DANGLING the instant Execute() returns.
				// Touching ->type afterwards to decide what to do about it is a use-after-free.
				const bool isCoroutine = (task_to_run->type == TaskType::Coroutine);

				currentRunningTask = task_to_run;
				busy.store(true, std::memory_order_relaxed);
				task_to_run->Execute();

				// OWNERSHIP: the worker completes Native tasks and NEVER completes coroutine ones.
				//
				// The tempting design -- a "did it finish" flag the worker checks after resuming --
				// is racy and was written and discarded here. A coroutine that suspends is re-pushed
				// by whatever armed its resume, so a SECOND worker can pick it up, run it to
				// completion and free it while the first worker is still deciding; both then observe
				// "finished" and both free. There is no flag read that closes that, because the
				// window opens the moment the task becomes re-pushable, which is inside resume().
				//
				// Giving the C++20 side sole ownership removes the race instead of racing better:
				// the coroutine signals its own WaitGroup and frees its own Task exactly once, from
				// inside the coroutine, before its frame goes away. See JLib/Coroutine.h.
				if (!isCoroutine) {
					if (task_to_run->waitGroup) {
						int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
						if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
							task_to_run->waitGroup->WakeAll();   // only touches wg if someone registered
					}
					busy.store(false, std::memory_order_relaxed);
					currentRunningTask = nullptr;

					scheduler->CleanupTaskMetadata(task_to_run);
					DestroyTask(task_to_run);
					// Free(), not FreeSized(): FreeSized reports failure by RETURNING false, and a task
					// whose body did not fit a slot lives on the heap. Free() is the funnel that routes
					// both. Ignoring the bool here leaked such a task.
					scheduler->GetAllocator()->Free(task_to_run);
				}
				else {
					// Coroutine: the task may already be freed, so nothing below may touch it.
					// Clearing these two is still required and still safe -- neither reads the task.
					busy.store(false, std::memory_order_relaxed);
					currentRunningTask = nullptr;
					task_to_run = nullptr;
				}

				if (EpochManager::Instance().ShouldSelfReclaim()) {
					EpochManager::Instance().Tick();
				}
				task_to_run = nullptr;
				ready.store(true, std::memory_order_release);
				continue;
			}

			Fiber* existingFiber = task_to_run->assignedFiber;

			Fiber* f;
			if (existingFiber) {
				f = existingFiber;      // resume existing context
			}
			else {
				f = AcquireFiber(task_to_run);
				if (!f) {
					// No fiber available right now (transient -- fibers are in use and will free
					// up). Re-queue rather than lose it. The forked/immediate arm that used to sit
					// here went with PushImmediate in 4.0.1 -- there is only one kind of task now.
					{
						// NOT push_bottom on our own deque. That end is LIFO for the owner (see
						// TaskDeque's header), so the next pop_bottom takes THE SAME TASK back and
						// this worker spins pop -> no fiber -> push -> pop forever on one task. It
						// therefore never reaches its inbox drain -- which is exactly where a
						// SignalAll deposits the resumed tasks whose completion would FREE the
						// fibers this one is waiting for. Every worker doing that at once is a
						// deadlock, not the "transient, fibers will free up" this comment used to
						// claim: over-subscribing the pool hung the primitives suite until its
						// watchdog fired.
						//
						// Requeue routes it through PickNextWorker to a worker inbox, so it leaves
						// this worker's hands and this pass continues to the steal and inbox-drain
						// steps that can actually make progress.
						scheduler->Requeue(task_to_run);
					}
					// MUST clear it. task_to_run is a thread_local that survives `continue`, and the
					// loop top runs `if (task_to_run)` before searching -- so leaving it set makes
					// this worker keep re-processing a task it has already handed to a queue, adding
					// one more copy each pass. With the old push_bottom that was hidden, because the
					// worker re-popped the very same task and the duplicate was itself; routing the
					// task anywhere else turns it into the same Task* live in two queues and run by
					// two workers, which is a use-after-free.
					task_to_run = nullptr;
					std::this_thread::yield();
					continue;
				}
				task_to_run->assignedFiber = f;
				f->owningTask = task_to_run;
				f->Init(GlobalFiberPool::FiberEntryWrapper);
			}

			f->status.store(FiberStatus::RUNNING, std::memory_order_release);
			f->homeCtx = &this->schedulerCtx;   // where the fiber returns to: THIS worker
			currentRunningTask = task_to_run;
			currentFiber = f;
			busy.store(true, std::memory_order_relaxed);
			{
				ContextSwitch(&this->schedulerCtx, &f->ctx);
	
			}
			busy.store(false, std::memory_order_relaxed);

			FiberStatus fs = f->status.load(std::memory_order_acquire);
			if (fs == FiberStatus::DEAD) {
				// Completed for good
				if (task_to_run->waitGroup) {
					int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						task_to_run->waitGroup->WakeAll();   // only touches wg if someone registered
				}
				task_to_run->assignedFiber = nullptr;
				ReleaseFiber(f);

				scheduler->CleanupTaskMetadata(task_to_run);
				DestroyTask(task_to_run);
				// Free(), not FreeSized() -- see the Native path above.
				scheduler->GetAllocator()->Free(task_to_run);

				// PushFork in 1.3.4.

				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}
			else if (fs == FiberStatus::WANTS_YIELD) {
				// Yielded. The switch above already saved the fiber's context
				f->status.store(FiberStatus::READY, std::memory_order_release);
				// NEVER DROP. Ignoring this bool (which this once did) was the worse of the two
				// lost-task sites: a yielded fiber that is never requeued is never RESUMED, so its
				// stack never unwinds and nothing it holds is released -- RAII, its WaitGroup slot,
				// a hazard record. The deque grows now rather than refusing, so the only false left
				// is a null task, which cannot be one here.
				// Same reasoning as the inbox drain: a hard stop, not an assert. A yielded fiber that
				// fails to requeue is never resumed, so its stack never unwinds -- silence here costs
				// RAII, a WaitGroup slot and a hazard record, and reports none of it.
				if (!scheduler->loPri[qIndex]->push_bottom(task_to_run)) TaskDeque::FatalPushRefused();
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}
			else {
				// WANTS_SUSPEND: the context is now safely saved, so publish SUSPENDED.
				// But a Signal/Resume may have raced in during WANTS_SUSPEND and flipped us
				// to SUSPEND_SIGNALED -- in that case DON'T park (the wakeup would be lost);
				// resume immediately instead. The CAS makes the park-vs-signal decision atomic.
				FiberStatus exp = FiberStatus::WANTS_SUSPEND;
				if (f->status.compare_exchange_strong(exp, FiberStatus::SUSPENDED, std::memory_order_acq_rel)) {
					// parked; a later Signal/Resume re-queues it (SUSPENDED -> READY)
				}
				else if (exp == FiberStatus::SUSPEND_SIGNALED) {
					// signal beat us here: wake now instead of parking
					f->status.store(FiberStatus::READY, std::memory_order_release);
					scheduler->Requeue(task_to_run);
				}
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}

			if (EpochManager::Instance().ShouldSelfReclaim()) {
				EpochManager::Instance().Tick();
			}

			// CLOSE THE LANE STAMP WHERE THE TASK ENDS, which is the only place the interval is
			// actually over. Closing it at the top of the next pass instead banks everything between
			// task-end and next-pass-top as lane time -- and if the worker parks in that gap, its
			// entire sleep. Measured with that bug present: a 6%-duty trickle reported 78-99%
			// occupancy and windows stretched from 10 ms to ~300 ms, so nothing ever demoted.
			//
			// The top-of-pass and pre-park closes remain, because this branch has `continue`
			// escapes that leave without reaching here. Those are backstops; this is the normal
			// path, and after it the stamp is provably shut.
			if (ranLaneTaskLastPass) {
				laneBusyNs.fetch_add(MonotonicNs() - laneStartNs, std::memory_order_relaxed);
				ranLaneTaskLastPass = false;
			}

			task_to_run = nullptr;
		}
		// --- 2. (was: immediate task execution) ---
		//
		// REMOVED IN 4.0.1 along with PushImmediate. A worker had a single-slot bypass around the
		// queues, written only by PushToCore, plus a drain that emptied this worker's own inboxes
		// first so a pinned task could not strand them. With no writer left the whole section was
		// unreachable, and both sites that released the core claim went with it.
		//
		// See TaskScheduler::SetReservedCores for the replacement: reserve a core, run a std::thread.
		{
			// --- 3. Local queues ---
			//
			//   HOT worker (serves the hiPri lane): own hiPri deque -> loPri deque -> steal
			//   NORMAL worker:                      loPri deque -> steal
			//
			// ONE GATE PRODUCES BOTH: the hiPri block below is conditional on
			// servesHiPri || hiPriStray, so a hot worker takes its own lane first and a normal
			// worker skips straight past it.
			//
			// THE OVERFLOW LANES THAT USED TO SIT BETWEEN THESE ARE GONE. A full deque no longer
			// pushes sideways -- it GROWS, so the lanes were unreachable in every run short of the
			// ceiling, and a mechanism that only executes in the case it cannot handle is worse than
			// not having it. A deque that cannot grow now aborts with a message naming the cause;
			// see TaskDeque::grow.
			if (!task_to_run && (servesHiPri || hiPriStray)) {
				auto opt = scheduler->hiPri[qIndex]->pop_bottom();
				if (opt) {
					Task* task = *opt;
					if (!task) {
						std::cerr << "[worker " << qIndex << "] Null task from pop_bottom!" << std::endl;
					}
					else {
						// Arm 2 only: the per-pickup maintenance whose cost is under test. size() reads
						// the deque`s top, which thieves write -- a contended line touched once per lane
						// task rather than once per idle pass.
						if (TaskScheduler::GetLaneHintMode() == 2)
							scheduler->UpdateLaneHint((size_t)qIndex, scheduler->hiPri[qIndex]->size());
						task_to_run = task;
						continue;
					}
				}
			}
			if (!task_to_run) {
				auto opt = scheduler->loPri[qIndex]->pop_bottom();
				if (opt) {
					Task* task = *opt;
					if (!task) {
						std::cerr << "[worker " << qIndex << "] Null task from pop_bottom!" << std::endl;
					}
					else {
						task_to_run = task;
						continue;
					}
				}
			}
		}
		{
			// --- 4. Work stealing ---
			if (!task_to_run) {
				JLIBSCHED_LATENCY_MARK(PreSteal);
				// Non-blocking backoff: a per-worker (thread_local, no shared/contended state --
				// can't itself become a new source of contention) count of consecutive whole-
				// steal-block misses. Above the threshold, shrink how many clusterMates get
				// probed each iteration (down to 1) instead of the full set -- reduces redundant
				// CAS/cache-line traffic pool-wide during a steal storm (many idle workers all
				// hammering the same handful of targets) without ever sleeping, so this worker
				// stays fully responsive to a fresh immediate/forked task or inbox item. Resets
				// to 0 the instant ANY steal succeeds -- "the storm cleared" observed via a real
				// outcome, since TaskDeque::steal() is lock-free (CAS-based, no actual lock to
				// wait on).
				static thread_local int consecutiveMisses = 0;
				constexpr int kBackoffMissThreshold = 8;

				// Locality-first, random fallback -- built from REAL queried topology
				// (TaskScheduler::BuildTopology), not an assumption about the affinity scheme:
				//  1. Try same-last-level-cache mates (clusterMates), random order, EXCLUDING
				//     the direct SMT sibling (handled separately below).
				//  2. If the SMT sibling is currently IDLE, try it -- a BUSY sibling shares
				//     this core's execution ports, so stealing its work wouldn't recruit any
				//     new throughput, just pile more work onto an already-contended core.
				//  3. Fall back to the old global-random steal across everyone.
				// Steal ONE task from `target`: try its hiPri deque first, then loPri. Single-item
				// steal is the ONLY correct steal in this lock-free deque -- a batched range steal
				// double-claims tasks the owner concurrently pops (use-after-free; see TaskDeque.h).
				// So there is no batch and no leftovers to re-home: whatever we steal becomes
				// task_to_run and runs on THIS worker immediately. (No age-promotion here either --
				// promoting an aged loPri task only helps if it gets REQUEUED into the hiPri lane,
				// but a task stolen for immediate execution is already un-starved by the steal, so
				// flipping its priority would be a no-op.)
				// CLASS-AWARE via steal_if: vet corePref BEFORE claiming (see StealClassCompatible) --
				// an explicit P/E task is left in place for a matching-class thief instead of being
				// stolen-then-Requeued (requeue on mismatch = deque contention + thrash, by design
				// rejected). Default/Any/Wide tasks (the common case) remain stealable by everyone.
				const bool iAmP  = scheduler->isPCore[qIndex] != 0;
				const bool degen = scheduler->pWorkers.empty() || scheduler->eWorkers.empty();
				bool sawDecline  = false;   // saw a task this sweep that exists but isn't ours (class mismatch)
				// Takes the deque's tag bits rather than the Task*, so no candidate is
				// dereferenced before it is claimed (see TaskDeque::StealBits).
				auto classOK = [&](StealBits sb) {
					if (TaskScheduler::StealClassCompatible(sb.corePref, iAmP, degen)) return true;
					sawDecline = true;
					return false;
				};
				auto tryStealFrom = [&](int target) -> bool {
					// TWO DISJOINT STEAL WORLDS once hot workers exist: hot steals from hot, ordinary
					// steals from ordinary. Neither ever probes the other's lane.
					//
					//   HOT -> HOT, hiPri only. Stealing between hot workers is what makes K > 1
					//   mean anything: without it, one hot worker can be backed up while another
					//   sits idle and nothing rebalances the lane. It does NOT take loPri -- bulk
					//   work is unbounded and a running task cannot be preempted, so one stolen
					//   bulk task costs every completion behind it that task's whole duration.
					//   Dedicated is what the latency guarantee means, and an idle hot worker
					//   spinning is the price the caller opted into by setting K.
					//
					//   ORDINARY -> ORDINARY, loPri only. ONE probe per victim instead of two, and
					//   that halving is the point: under NoSleep every idle worker runs this search
					//   continuously, so the probe count IS the contention. Letting ordinary workers
					//   take hiPri "under pressure" would put the probe back and give the saving
					//   away -- and would land a lane task on a cold or contended core, which is the
					//   thing the arrangement exists to prevent. A hot worker that cannot keep up is
					//   a capacity question: raise K.
					//
					// AT K = 0 neither world exists, so this is the original both-lanes steal
					// against every victim, unchanged.
					const std::size_t hotN = TaskScheduler::GetHotWorkers();
					const bool victimIsHot = hotN > (std::size_t)target;

					// A HOT WORKER NEVER STEALS -- not bulk work, and not from another hot worker.
					//
					// Hot->hot stealing was added to give K>1 a rebalancing path, and MEASURED AS
					// PURE COST: a hot worker never parks, so it runs this search continuously, and
					// each pass touched the sibling's deque endpoint. Probe counts for an identical
					// workload:
					//
					//     K=1        0 probes      p50 2.4us  p90 3.0us
					//     K=2   20,592,209         p50 2.3us  p90 3.2us
					//     K=4  232,627,776         p50 2.3us  p90 3.4us
					//
					// Quadratic in K, flat in latency. Twenty million cache-line invalidations
					// bought nothing, because there is nothing to rebalance: steering round-robins
					// pushes across the hot set, so the lane is already balanced at PUSH time, and
					// lane work is short by contract. K>1 is for availability, not load sharing.
					// A HOT WORKER STEALS ONLY FROM A HOT SIBLING, ONLY LANE WORK, AND ONLY WHEN
					// THAT SIBLING IS ADVERTISING A BACKLOG.
					//
					// This was removed once, correctly, on a measurement of 20.6M probes at K=2 and
					// 232M at K=4 for flat latency. What made that true was the workload: uniform
					// completions, round-robin steering, so the lane was balanced at push time and
					// there was nothing to rebalance. Skewing the handler sizes changes the answer.
					// Sampled idle passes where a SIBLING held a backlog, medians of 3:
					//
					//     handler    uniform   5us    20us   200us
					//       K = 2      3.0%    8.6%   3.5%   62.4%
					//       K = 4      8.8%   11.5%  22.8%   65.1%
					//
					// and at 200us capacity stops working -- K=1 to K=4 moves p99 only 1142us to
					// 906us -- because the tail is not saturation, it is one worker buried behind a
					// long handler while siblings spin. That is what this steals.
					//
					// It is gated, not restored: the probe happens only when the victim has
					// advertised a real backlog, so the 232M-probe outcome cannot recur -- an idle
					// hot pool reads one shared word and stops.
					//
					// WHAT IT COSTS, isolated. Four arms interleaved inside one process (three
					// separately-built binaries measured in three sessions moved the K=1 rows -- a
					// configuration with no sibling, where this cannot act -- by 2x, which was
					// machine drift being read as a result). Arm `hnt` maintains the hint and never
					// steals, which is what separates accounting from mechanism. Medians of 3:
					//
					//     20us handler, K=4       p50     p90     p99    idle%
					//       off                  17.8    84.8   167.2     13.2
					//       hint, never steal    17.3    77.6   159.8     12.6
					//       hint + steal         24.7    55.3    88.6      2.6
					//
					// hnt is off, on every metric. THE HINT MACHINERY IS FREE -- the bitmap write,
					// the drain-side update and the predicate cost nothing measurable, and two
					// earlier suspects were wrong: it is not the atomic RMW (a publisher-side exact
					// counter measured the same) and it is not the contended cache line (maintaining
					// from a local `count` instead of size() measured the same).
					//
					// The whole difference is the STEAL, and it is a genuine trade, not overhead:
					// ~35-40% worse p50 for ~30-50% better p90/p99, consistently, at every K and
					// every handler size. Moving a completion moves it to a core where its coroutine
					// frame and its buffer are cold. That is the price of not queueing behind a busy
					// worker, and for a lane whose entire reason to exist is the tail, it is the
					// right side of the trade.
					//
					// IT IS ALSO SELF-GATING, which is why it is on by default. The threshold is
					// only crossed when a worker is actually behind, so an unloaded lane -- the
					// case K-hot was built for, where dispatch is ~1-2us -- never steals and never
					// pays. The p50 appears under load, which is exactly when the tail is worth
					// buying.
					if (isHotWorker) {
						if (!victimIsHot) return false;                 // never bulk work
						// Mode 3 maintains the hint but never acts on it: the isolator above.
						{ const int m = TaskScheduler::GetLaneHintMode(); if (m == 0 || m == 3) return false; }
						if (!scheduler->LaneStealable((std::size_t)target)) return false;
						JLIBSCHED_STEAL_STAT(qIndex, probes);
						auto h = scheduler->hiPri[target]->steal_if(classOK);
						// THE THIEF RETIRES THE ADVERTISEMENT, because the owner no longer can. The
						// owner sets the bit at its drain and then stops touching it -- deliberately,
						// since clearing it per pop costs the contended read this design just removed.
						// So the party that empties the deque is whoever is looking at it, and on a
						// failed steal that is this thread, which has already paid for the line.
						// Clearing on a merely-lost race is conservative in the safe direction: the
						// next drain re-advertises, and until then thieves stay off a deque that has
						// nothing for them.
						if (!h) { scheduler->UpdateLaneHint((std::size_t)target, 0); return false; }
						scheduler->UpdateLaneHint((std::size_t)target, scheduler->hiPri[target]->size());
						JLIBSCHED_STEAL_STAT(qIndex, hits);
						task_to_run = *h;
						return true;
					}
					// ORDINARY -> LANE, mode 4 only. The open question: once a backlog is advertised,
					// is an ordinary worker a useful place to put lane work, or only a cold one?
					//
					// It is not the same question as hot->hot, and the difference is not the probe.
					// A hot sibling is awake by construction, at raised priority, and running only
					// short lane work. An ordinary worker is none of those: under the default Sleep
					// policy it is PARKED precisely when the lane backs up -- it has no bulk work to
					// keep it awake -- so this arm can do nothing unless something wakes it, and
					// waking it costs an OS round trip of the same order as the tail being bought.
					// That is why this is measured under both idle policies: NoSleep answers "is an
					// ordinary worker a good landing site", Sleep answers "can one even be reached".
					if (hotN && victimIsHot) {
						if (TaskScheduler::GetLaneHintMode() != 4) return false;
						if (!scheduler->LaneStealable((std::size_t)target)) return false;
						JLIBSCHED_STEAL_STAT(qIndex, probes);
						auto h = scheduler->hiPri[target]->steal_if(classOK);
						if (!h) { scheduler->UpdateLaneHint((std::size_t)target, 0); return false; }
						scheduler->UpdateLaneHint((std::size_t)target, scheduler->hiPri[target]->size());
						JLIBSCHED_STEAL_STAT(qIndex, hits);
						task_to_run = *h;
						return true;
					}

					// THE HINT. Two bits, ORed, and the OR is the whole design -- see the comment on
					// stealHintBacklog. One shared pair of words read here in place of a remote
					// deque endpoint; the words are written only on state changes, so under a steady
					// workload they sit shared-clean in every thief's cache and this test is free.
					if (!scheduler->MaybeStealable((std::size_t)target)) return false;

					// COUNTED HERE, AFTER the early-out, on purpose: a probe is a TOUCH of another
					// core's deque endpoint, and the cross-divide rejection above touches nothing.
					// Counting attempts instead would report the same number for every K and hide
					// the exact quantity this design set out to reduce -- remote line traffic.
					JLIBSCHED_STEAL_STAT(qIndex, probes);

					// ONE REMOTE LANE. loPri, always -- everything else was filtered above, so this
					// is the only reachable case rather than a branch.
					//
					// The hiPri probe that used to be here was dead work in EVERY configuration.
					// At K=0 push routing collapses the lane, so hiPri deques are empty by
					// construction and the probe was a guaranteed-useless touch of another core's
					// line, on every victim, on every pass, by every idle worker -- at the DEFAULT
					// setting. At K>0 the lane belongs to workers nobody may steal from. Two-lane
					// stealing cost remote traffic and never bought a task.
					//
					// OWN queues are a different question and stay unconditional: a worker's own
					// inbox and deque are ITS cache lines, so checking them is free and is what
					// keeps a stray lane task from stranding. Remote is where the ping-pong is.
					auto s = scheduler->loPri[target]->steal_if(classOK);
					if (!s) return false;
					JLIBSCHED_STEAL_STAT(qIndex, hits);
					task_to_run = *s;
					return true;
				};

				// CLASS-FIRST victim ordering: probe the victims this worker can actually steal
				// class-pinned work from BEFORE the ones it might have to decline -- scan order
				// matches steal legality. Phases: same-class LLC mates -> idle SMT sibling (same
				// physical core = same class by construction) -> foreign-class mates -> global
				// random, same-class set first. Same total coverage as the old order (the two mate
				// lists ARE clusterMates, split); on non-hybrid CPUs matesOtherClass is empty and
				// this degrades to exactly the classic scan.
				auto probeList = [&](const std::vector<int>& list) {
					if (list.empty() || task_to_run) return;
					size_t probeLimit = (consecutiveMisses < kBackoffMissThreshold)
						? list.size() : (size_t)1;
					size_t s = FastRand() % list.size();
					for (size_t i = 0; i < probeLimit; ++i) {
						if (tryStealFrom(list[(s + i) % list.size()])) break;
					}
				};

				probeList(scheduler->matesSameClass[qIndex]);

				if (!task_to_run) {
					int sibling = scheduler->siblingQIndex[qIndex];
					if (sibling >= 0 && !scheduler->workers[sibling]->busy.load(std::memory_order_relaxed)) {
						tryStealFrom(sibling);
					}
				}

				probeList(scheduler->matesOtherClass[qIndex]);

				// THE NON-WORKER LANE (see TaskScheduler::nonWorkerLane). Probed HERE -- after both
				// topology phases, before the global random fallback -- and the position is a
				// judgement, not an accident. It has no cache locality to any worker, so it does
				// not belong among the LLC mates; but it is the one victim guaranteed to be a
				// SINGLE known index rather than a random draw out of a set, so putting it after
				// the random fallback would make a lazy split's fan-out depend on a dice roll.
				// Unlike the mate lists this is one deque pair, so the probe is unconditional and
				// costs two loads when it is empty, which is the overwhelmingly common case.
				if (!task_to_run) {
					tryStealFrom((int)scheduler->nonWorkerLane);
				}

				if (!task_to_run) {
					// Global fallback: one random probe from the same-class set, then one from the
					// other class -- covers workers outside this LLC cluster (multi-CCD parts).
					auto tryRandomFrom = [&](const std::vector<int>& set) {
						if (set.empty() || task_to_run) return;
						int t = set[FastRand() % set.size()];
						if (t != qIndex) tryStealFrom(t);
					};
					tryRandomFrom(iAmP ? scheduler->pWorkers : scheduler->eWorkers);
					tryRandomFrom(iAmP ? scheduler->eWorkers : scheduler->pWorkers);
				}

				if (task_to_run) {
					consecutiveMisses = 0;
					continue;
				}
				else {
					// A class-DECLINED sweep is not emptiness: work exists, it just isn't ours. The
					// backoff counter exists to damp CAS storms from probing EMPTY deques -- and a
					// steal_if decline performs NO CAS -- so declines must not shrink future probe
					// width (that would blind this worker to compatible stragglers next sweep).
					// Neither is it success, so no reset. Parking is unaffected either way: sleep
					// was always per-sweep (cv.wait below), and class-targeted push notifies cover
					// new compatible work while we're parked.
					if (!sawDecline)
						++consecutiveMisses;
				}
			}
		}
		

		// 5. --- Pull from inboxes before sleep (drain them so nothing gets stuck) ---
		// NO RESCUE HERE, deliberately. A lane task cannot reach an ordinary worker's inbox at all:
		// PickNextWorker rotates the hot set for hiPri and the ordinary set for everything else, so
		// the two are disjoint at the one place placement is decided. Bailing a mis-placed task out
		// afterwards would cost a second push per task and could be outrun by multiple producers --
		// enforcing the invariant is cheaper than repairing violations of it.
		// THE GATE COVERS THE hiPri DRAIN ONLY -- NOT THE WHOLE BLOCK.
		//
		// This is where the K=0 deadlock lived. The loPri drain further down is NESTED inside this
		// block, so gating the block on `servesHiPri` swallowed it too: at K=0, where nothing serves
		// the lane, a worker stopped draining its OWN loPri INBOX. The pool dump made it obvious and
		// two rounds of reading had not -- worker 1 AWAKE, loPri inbox non-empty, 150 tasks never
		// run, and every hiPri structure empty. I had been hunting stranded LANE work while the
		// stranded work was ordinary.
		//
		// The lesson is narrower than "check your braces": a condition added to an EXISTING `if` is
		// silently inherited by everything already inside it. The gate belongs on the hiPri pop, not
		// on the section.
		if (!task_to_run) {
			size_t count = 0;
			if (servesHiPri || hiPriStray) {
			while (count < BATCH_SIZE && scheduler->hiPriInboxes[qIndex]->pop(batch[count])) {
				count++;
			}
			TaskScheduler::NoteInboxDrain(count);   // no-op unless a submit limit is set
			if (count > 0) {
				if (scheduler->hiPri[qIndex]->push_bottom_batch(batch, count)) {
					auto opt = scheduler->hiPri[qIndex]->pop_bottom();
					if (opt) {
						// THE DRAIN, and the ONLY place the owner touches the hint. Depth is `count - 1`
						// -- what this drain just landed, less the one being taken -- and that is a LOCAL
						// number. Calling size() here instead would read the deque`s top, which thieves
						// write, adding a contended-line read to every lane pickup: measured as a ~30%
						// p50 regression in EVERY configuration including the uniform control, which is
						// how it was caught. The drain is reached only when the deque was already empty,
						// so `count - 1` is the depth, not an increment to it.
						const int _lhm = TaskScheduler::GetLaneHintMode();
						if (_lhm == 1 || _lhm == 3 || _lhm == 4) scheduler->UpdateLaneHint((size_t)qIndex, count - 1);
						else if (_lhm == 2) scheduler->UpdateLaneHint((size_t)qIndex, scheduler->hiPri[qIndex]->size());
						// This drain just published a depth, so the bit may now be up -- including on
						// a NON-hot worker draining strays, which is the case the generation-based
						// reconcile could not see.
						ownsLaneBit = true;
						// A MISS: the drain moved more than one lane task, so at least one completion
						// waited behind another. Free -- count is already here.
						if (count > 1) TaskScheduler::NoteLaneMiss(count - 1);
						task_to_run = *opt;
						JLIBSCHED_LATENCY_MARK(Found);
						continue;
					}
				}
				else {
					// THE DEQUE WAS FULL AND THESE ARE ALREADY OUT OF THE INBOX. Dropping them here
					// used to be silent: pop() had consumed them, batch[] is reused next iteration,
					// so the tasks never ran, their slab slots leaked, and every WaitFor on their
					// WaitGroup waited forever with nothing to show in a pool dump. Losing work is a
					// worse failure than stalling on it -- a stall is at least diagnosable.
					// Requeue instead: inboxes are unbounded MPSC queues, so it cannot fail the way
					// a fixed-capacity deque can. Reachable only past 32768 queued on ONE worker's
					// deque, which needs a deliberate single-core bulk push (PushBatch with an
					// explicit cpuaffinity does not spread), but the cost of handling it is one loop.
					for (size_t i = 0; i < count; ++i)
						if (batch[i]) scheduler->Requeue(batch[i]);
				}
			}
			}   // end of the hiPri-only gate -- the loPri drain below is NOT part of it

			if (!task_to_run) {
				count = 0;
				while (count < BATCH_SIZE && scheduler->loPriInboxes[qIndex]->pop(batch[count])) {
					count++;
				}
				TaskScheduler::NoteInboxDrain(count);   // no-op unless a submit limit is set
				if (count > 0) {
					if (scheduler->loPri[qIndex]->push_bottom_batch(batch, count)) {
						auto opt = scheduler->loPri[qIndex]->pop_bottom();
						if (opt) {
							task_to_run = *opt;
							JLIBSCHED_LATENCY_MARK(Found);
							continue;
						}
					}
					else {
						// Same as the hiPri branch above: these are already out of the inbox, so
						// dropping them loses them silently. Requeue rather than discard.
						for (size_t i = 0; i < count; ++i)
							if (batch[i]) scheduler->Requeue(batch[i]);
					}
				}
			}
		}

		if (task_to_run) {
			idleSpins = 0;   // work found: this idle episode is over, next one starts fresh
			continue;
		}
		else {
			// IDLE POLICY. Found nothing this pass -- decide whether to search again or park.
			//
			// This sits BEFORE the WS_GOING_TO_SLEEP publish on purpose. A worker that spins here
			// never advertises an intent to park, so it stays WS_AWAKE and pushers take the
			// awake-preference skip instead of paying a notify for it: spinning does not merely
			// avoid the wake, it removes the wake cost from the PRODUCER too.
			//
			// Deliberately NOT a timed wait. The park below stays an unconditional cv.wait, so a
			// lost wakeup still hangs visibly rather than being masked by a timeout -- see the
			// IdlePolicy comment in TaskScheduler.h for why that distinction is the whole point.
			{
				// Shutdown and Pause both override the policy. Pausing means "stop using the CPU",
				// and a policy that spun through it would defeat the only thing Pause exists for.
				// K-HOT: the same decision, taken per worker instead of pool-wide. Workers below
				// the hot count never park; everyone else obeys the global policy. It is the
				// bounded-cost version of NoSleep -- whose penalty lands in p90 and scales with how
				// many cores spin, so 2-of-31 is a different question from 31-of-31.
				//
				// The spin ALSO removes the producer's notify, because a spinning worker never
				// advertises WS_GOING_TO_SLEEP (see the note above). So a hot worker is a landing
				// spot that costs a pusher nothing, which is the actual hypothesis being tested.
				const bool hot = TaskScheduler::GetHotWorkers() > (size_t)qIndex;

				// ---- WINDOWS ONLY. There is no POSIX implementation of HotThreadPolicy. ----------
				//
				// PORTABLE PROTOCOL. Every platform difference lives in ApplyHotPriority; the state
				// machine below is one copy on every OS, because three copies of it would drift.
				//
				// TARGETED priority, and the targeting is the point. Raising the WHOLE PROCESS was
				// measured 5x WORSE with K-hot: it elevates all N workers, so 29 spinning threads
				// preempt the completion thread that feeds them. Raising only the hot workers (and,
				// separately, the reactor's completion threads) elevates exactly the critical path
				// and leaves everyone else at Normal.
				//
				// TIME_CRITICAL inside a NORMAL_PRIORITY_CLASS process is priority 15 -- the top of
				// the non-realtime range. True realtime (16-31) needs REALTIME_PRIORITY_CLASS and a
				// privilege, and would let a spin loop starve the OS, so it is deliberately not
				// asked for here.
				// Elevated and Realtime map to the SAME Windows call: TIME_CRITICAL is already the
				// top of the non-realtime range, and the only step above it is a process-wide class
				// this deliberately refuses to touch. See HotThreadPolicy.
				if (hot && !hotPriorityRaised &&
				    TaskScheduler::GetHotThreadPolicy() != TaskScheduler::HotThreadPolicy::Normal) {
					ApplyHotPriority(true);
					hotPriorityRaised = true;
					atCriticalPriority = true;
				}
				// AND BACK DOWN WHEN THIS WORKER STOPS BEING HOT. The raise used to be permanent --
				// "not reset: a worker that stops being hot keeps the priority until the pool shuts
				// down, which is acceptable for a knob that is off by default and set once at
				// startup". That was sound while the hot set was STATIC, because only ever K workers
				// could be raised and K never moved.
				//
				// DYNAMIC K BREAKS IT, and in the direction that matters. K ramps to maxK, every
				// worker that was ever hot keeps the capability, K sheds back to minK -- and the
				// priority does not shed with it. The idle branch below then puts those demoted
				// workers at 15 while they wait. Under the default Sleep policy they PARK, so 15
				// costs nothing; under NoSleep they SPIN at 15, which is exactly the "N spinning
				// threads preempt the completion thread feeding them" configuration this file
				// records as 5x worse. NoSleep is not a corner case -- it is what a server build
				// would choose.
				//
				// So K comes down and the priority comes down with it. The original objection was
				// thrashing the syscall in the idle loop; the flags make it fire only on a real
				// change, and K moves at 200us up / 200ms down, not per pass.
				else if (!hot && hotPriorityRaised) {
					ApplyHotPriority(false);
					hotPriorityRaised = false;
					atCriticalPriority = false;
				}
				// Back up on the way into the idle search. A worker that was demoted to run an
				// ordinary task must be elevated again BEFORE it starts waiting, or the very next
				// completion is taken at Normal and the whole point is lost. Safe to be at 15 here
				// only because this worker is still HOT -- the branch above has already stood down
				// anyone who is not.
				else if (hotPriorityRaised && !atCriticalPriority) {
					ApplyHotPriority(true);
					atCriticalPriority = true;
				}
#ifdef JLIBSCHED_HOT_OCCUPANCY_STATS
				// THE WITNESS. Reaching here means this worker's whole search came up empty, so for
				// a hot worker this is by definition an idle pass. Sampled 1-in-64 rather than every
				// pass: the scan reads deque endpoints the sibling OWNER is writing, so at spin rate
				// it would steal exclusive state from the very worker it is trying to catch being
				// late. Both populations are sampled by the same counter, so the ratio is unaffected.
				if (hot && ((idleSpins & 0x3F) == 0)) {
					const std::size_t hotN = TaskScheduler::GetHotWorkers();
					long long deepest = 0;
					for (std::size_t v = 0; v < hotN && v < scheduler->hiPri.size(); ++v) {
						if (v == (std::size_t)qIndex) continue;
						const long long d = (long long)scheduler->hiPri[v]->size();
						if (d > deepest) deepest = d;
					}
					auto& slot = g_hotOcc[(std::size_t)qIndex < kHotOccSlots ? (std::size_t)qIndex : 0];
					slot.idlePasses.fetch_add(1, std::memory_order_relaxed);
					if (deepest > 0) {
						slot.idleWithSib.fetch_add(1, std::memory_order_relaxed);
						slot.sibDepthSum.fetch_add(deepest, std::memory_order_relaxed);
					}
				}
#endif
				const bool mayspin = (hot || TaskScheduler::GetIdlePolicy() == TaskScheduler::IdlePolicy::NoSleep)
					&& running.load(std::memory_order_acquire)
					&& !scheduler->paused.load(std::memory_order_seq_cst);

				if (mayspin) {
					++idleSpins;
					// Yield periodically even under NoSleep. A pure CpuRelax loop is pathological
					// if the pool is oversubscribed or shares cores with the host's own threads,
					// and "I own the machine" is a claim the caller makes, not one this can verify.
					if ((idleSpins & 0xFF) == 0) std::this_thread::yield();
					else platform::CpuRelax();
					continue;   // search again rather than parking
				}
				idleSpins = 0;   // policy is Sleep, or shutting down/paused: fall through and park
			}

			// Per-worker signal, not a pool-wide counter: hasQueuedWork means "a task was pushed
			// specifically to ME since my last search," which is the only thing that should
			// keep THIS worker from actually sleeping. Stealable work on OTHER workers' deques
			// doesn't belong here at all -- it's found by the unconditional steal-attempt phase
			// every AWAKE worker already runs each loop pass (steps 3-4 above), with no
			// predicate gating it. A worker that's genuinely out of local work and found nothing
			// to steal has no reason to keep spinning just because some unrelated queue has a
			// backlog it structurally can't help with anyway (e.g. another worker's own inbox,
			// which nobody but that worker can ever drain).
			// Publish the INTENT to park before the final check. From here until this worker is
			// awake again, a pusher observing the state will signal rather than assume this worker
			// will notice on its own. Model checked: tests/verify/sleepwake_model.c.
			// FLUSH THE HAZARD RETIRE BAG BEFORE PARKING, and this is not an optimisation.
			//
			// The retire bag is per-THREAD, deliberately -- protection follows the reader because a
			// protected pointer survives a park, but the deferred FREE LIST must never sleep. Put
			// the bag on the fiber and a park freezes reclamation exactly the way an epoch pin does.
			//
			// So: drain it on the way IN, before the park is even advertised, because a worker that
			// retired some nodes and then went idle would otherwise sit on them until its OWN next
			// retire -- which on an idle worker may be never.
			HazardDomain::Instance().Scan();

			int expected = WS_AWAKE;
			workerState.compare_exchange_strong(expected, WS_GOING_TO_SLEEP,
				std::memory_order_seq_cst, std::memory_order_relaxed);

			// RECHECK after advertising, and mirror the wait predicate exactly so the two cannot
			// disagree about what counts as work. The hasQueuedWork load is seq_cst deliberately:
			// it and MarkQueuedWork's store are the StoreLoad pair the whole protocol turns on, and
			// acquire here is precisely the bug the model's negative control reproduces.
			// ALL of these are seq_cst, not just hasQueuedWork. Each one pairs with a seq_cst store
			// on the setter's side, and each pair needs its own place in the single total order --
			// promoting one flag and leaving the others at acquire is precisely the bug that hung
			// macOS arm64 in 1.2.0. `running` is the exception and does not need it, because Join()
			// passes force=true and never takes the skip.
			// The inbox checks are NOT redundant with hasQueuedWork, and the evidence says so. A
			// captured hang shows a worker SLEEPING with a non-empty loPri inbox and the flag at 0:
			// the flag is cleared blind at the top of each loop, so a push whose queue write is not
			// yet visible to this worker's drain, but whose MarkQueuedWork landed before the clear,
			// leaves the item present and the only signal wiped. Inboxes are not stealable, so that
			// task strands and every waiter on it hangs.
			// The flag stays as the cheap common case; the queue is the truth.
			if (!running.load(std::memory_order_acquire)
				|| (!scheduler->paused.load(std::memory_order_seq_cst)
					&& (hasQueuedWork.load(std::memory_order_seq_cst)
						|| laneWake.load(std::memory_order_seq_cst)
						|| !scheduler->hiPriInboxes[qIndex]->empty()
						|| !scheduler->loPriInboxes[qIndex]->empty()))) {
				workerState.store(WS_AWAKE, std::memory_order_seq_cst);
				JLIBSCHED_LATENCY_MARK(Wake);
				if (!running.load(std::memory_order_acquire)) break;
				continue;   // work landed while deciding: go search for it instead of parking
			}

			// CLOSE THE LANE STAMP BEFORE PARKING. Occupancy is closed at the top of the next pass,
			// and a worker that runs a lane task and then sleeps does not HAVE a next pass until
			// somebody wakes it -- so the delta would bank its entire sleep as lane time. Measured
			// before this line existed: a 6%-duty trickle reported 78% occupancy, with windows
			// stretched from 10 ms to ~300 ms, and K never came back down.
			//
			// Two close sites and no more: the top of a pass, and here. Both are points the worker
			// provably reaches with a stamp open.
			if (ranLaneTaskLastPass) {
				laneBusyNs.fetch_add(MonotonicNs() - laneStartNs, std::memory_order_relaxed);
				ranLaneTaskLastPass = false;
			}
			// THE INVARIANT, asserted rather than trusted: no lane stamp may be open across a park.
			// Every regression of this bug looks the same from outside -- occupancy reads far above
			// true duty and K stops demoting -- and that symptom is several inferential steps from
			// the cause. This turns it into a stop at the exact line.
			assert(!ranLaneTaskLastPass && "a lane occupancy stamp was left open across a park");

			std::unique_lock<std::mutex> lock(workerMutex);
			int expectedGoing = WS_GOING_TO_SLEEP;
			workerState.compare_exchange_strong(expectedGoing, WS_SLEEPING,
				std::memory_order_seq_cst, std::memory_order_relaxed);

			// Same inbox checks as the recheck above, and for the same reason: the flag can be wiped
			// while an item is present, so a predicate that trusts only the flag can decide to sleep
			// on a non-empty inbox. Being evaluated under the mutex does not help -- the mutex
			// orders this against a NOTIFIER, and the failure is a push that never notifies because
			// its flag write was already lost.
			cv.wait(lock, [this]() {
				return !running.load(std::memory_order_acquire)
					|| (!scheduler->paused.load(std::memory_order_acquire)
						&& (hasQueuedWork.load(std::memory_order_acquire)
							|| laneWake.load(std::memory_order_acquire)
							|| !scheduler->hiPriInboxes[qIndex]->empty()
							|| !scheduler->loPriInboxes[qIndex]->empty()));
				});

			// Back to AWAKE before releasing the mutex, so the very next push skips the signal.
			workerState.store(WS_AWAKE, std::memory_order_seq_cst);
			JLIBSCHED_LATENCY_MARK(Wake);

			if (!running.load(std::memory_order_acquire)) break;
		}
	}
	running.store(false, std::memory_order_release);
	cvWorkerDone.notify_all();
}

