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
#include <sys/syscall.h>        // SYS_setpriority, SYS_gettid, SYS_futex
#include <unistd.h>
#include <linux/futex.h>        // FUTEX_WAIT_PRIVATE / FUTEX_WAKE_PRIVATE -- the worker park
#include <cerrno>

// THE WORKER PARK ON LINUX. Windows has WaitOnAddress/WakeByAddressSingle; this is the same
// protocol with the same word, which is the part that matters -- a park and a wake that do not
// agree on the address are not a handshake, they are a hang.
//
// NO GLIBC WRAPPER EXISTS for futex, so it is a raw syscall. That is the normal way to call it and
// not a workaround.
//
// PRIVATE because every waiter and waker is a thread of THIS process. The _PRIVATE variants skip
// the shared-mapping lookup the generic ones need, which is most of the syscall's cost.
//
// std::atomic<int> IS THE ADDRESS. It is lock-free and standard-layout, so its storage is a plain
// int and the kernel's compare reads exactly the word Wake() stores to. This is the same assumption
// the Windows path already makes with &workerState.
namespace {
	inline void FutexWait(std::atomic<int>* addr, int expected) noexcept {
		// Returns immediately with EAGAIN if the value already changed -- which is the whole point:
		// the wake that raced us is not lost, it is observed by the compare. EINTR is equally fine;
		// both simply return to the caller's predicate loop, which re-tests the real condition.
		::syscall(SYS_futex, reinterpret_cast<int*>(addr), FUTEX_WAIT_PRIVATE, expected,
		          nullptr, nullptr, 0);
	}
	inline void FutexWakeOne(std::atomic<int>* addr) noexcept {
		// Waking an address nobody waits on is a no-op, so this is unconditional for the same
		// reason WakeByAddressSingle is: re-testing "is it really asleep" reopens the window the
		// preceding store just closed.
		::syscall(SYS_futex, reinterpret_cast<int*>(addr), FUTEX_WAKE_PRIVATE, 1,
		          nullptr, nullptr, 0);
	}
}
#endif
#if JLIB_PLATFORM_DARWIN
#include <pthread.h>
#include <sys/qos.h>            // QOS_CLASS_*, pthread_set_qos_class_self_np
#endif
// Mirror of kLongBodyNs in TaskScheduler.cpp -- the boundary between "a body worth waking a core to
// avoid queueing behind" and a trivial one. Duplicated rather than exported because both users are
// internal and a header constant for one number read in two .cpp files is a worse trade.
static constexpr long long kLongBodyNsThread = 200'000;   // 200 us -- keep in step with kLongBodyNs

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

	// THE cvWorkerDone HANDSHAKE THAT USED TO BE HERE WAS ALREADY A NO-OP, which is why deleting it
	// changes nothing. It waited on the predicate `!running` -- and the line above sets `running`
	// false, on this very thread, before the wait. The predicate was satisfied before the wait
	// began, so it returned immediately every time and never once blocked.
	//
	// std::thread::join() below is what actually waits, and it is a stronger guarantee than the
	// condition variable was reaching for: it returns when the OS thread has genuinely exited, not
	// when a flag was observed. So the mutex, the condition variable and the notify_all at the
	// bottom of Worker() all go, and Thread loses three non-movable members.
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
	// ---- THE FLOOR IS ALREADY SCHEDULED ------------------------------------------------------
	//
	// Workers 0..F-1 never park, so there is nothing to wake and nothing to signal: this worker is
	// spinning in its search loop and will find the inbox item on its next pass. Skipping here is
	// the same reasoning as the WS_AWAKE skip below, decided one step earlier and without loading a
	// shared word.
	//
	// AND THE ONLY WORKER WORTH WAKING IS THE OWNER. An inbox is drained by its owner alone --
	// steals never scan inboxes -- so waking anybody else leaves the task exactly where it was.
	// That is why every wake in the scheduler now targets the worker the push was routed to, and
	// why the random-victim wake that briefly existed here was wrong in principle rather than
	// merely wasteful: it woke a worker that was not permitted to run the work.
	if (!force && (size_t)qIndex < TaskScheduler::GetAwakeFloor()) return;

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

	// ---- NOTIFY IS A FIBER RESUME (unconditional since 4.0.2; was Mode::FiberOnly) ----------
	//
	// No kernel object on this path at all. `workers` is the flat array indexed by worker id and
	// each Thread owns its park fiber, so waking this one is a resume of a pointer we already hold.
	//
	// WHY IT IS WORTH THE TROUBLE, measured rather than assumed: event/resume with a 1 ms hold-off
	// -- long enough that the worker has genuinely parked -- costs 8.0 us against 5.0 us with no
	// hold-off. That ~3 us delta is the OS wake and nothing else, because the two rows differ in
	// nothing else. Two context switches are tens of nanoseconds.
	//
	// A NULL FIBER IS THE COMMON CASE AND IS NOT A MISS. It means this worker has never gone idle,
	// so it is running and will find the task on its own -- exactly what the WS_AWAKE skip above
	// decides for the same reason. Resuming a fiber that is RUNNING or READY is likewise a no-op:
	// ResumeQueueless only claims a fiber that is actually parked, and a worker whose park fiber is
	// not parked is by definition awake and looking.
	// ONE THING TO SIGNAL. Wake() publishes the state change and wakes the address; there is no
	// park fiber to resume any more, because an overflow worker with nothing to run has no other
	// fibers and a suspended fiber does not free a core.
	Wake();
}

void Thread::Wake() noexcept {
	// seq_cst to match every other transition of this word: NotifyWorker's awake-skip loads it
	// seq_cst, and the two are the StoreLoad pair that decides whether a push signals at all.
	// THE STORE FIRST, THEN THE SIGNAL, AND THE ORDER IS THE WHOLE PROTOCOL.
	//
	// WakeByAddress has NO MEMORY. A wake delivered between the waiter's last predicate check and
	// the instant it actually blocks reaches nobody and is gone -- and the waiter then sleeps on a
	// value nobody will ever change again. Publishing AWAKE first closes that window structurally:
	// WaitOnAddress compares the address against the value the waiter expected, sees it differ, and
	// returns without blocking at all.
	//
	// This is the futex rule, and skipping it is not a theoretical lost wakeup. It hung Join every
	// single time and the pool intermittently, until ResumeAll and RequestStop were routed through
	// here as well -- resuming a park fiber only makes work RUNNABLE; it does not bring a thread
	// back onto the run queue.
	workerState.store(WS_AWAKE, std::memory_order_seq_cst);
#if defined(JLIB_PLATFORM_WINDOWS)
	// Unconditional: signalling an address nobody waits on is a no-op, and re-testing "is it really
	// asleep" would only reopen the window the store above just closed.
	// COUNTED. "How many pushes had to go to the kernel" is the one number that says whether the
	// floor is receiving the work or placement is still round-robining onto sleepers. With F=2 and
	// a serial round trip this should be ~0; thousands means every push is waking somebody.
	TaskScheduler::NoteWakeCall();
	::WakeByAddressSingle(&workerState);
#elif JLIB_PLATFORM_LINUX
	// Same protocol, same word: store AWAKE above, then wake. See FutexWakeOne.
	TaskScheduler::NoteWakeCall();
	FutexWakeOne(&workerState);
#endif
	// OTHER POSIX (Darwin) HAS NO PARK YET and therefore no wake to pair with one: the idle path
	// there falls through to spinning, so the store above is the entire notification. __ulock_wait
	// is the Darwin equivalent but it is private API; a pthread condvar per worker would work and is
	// what the pre-futex code did. Until one is chosen, the floor is a Windows and Linux feature and
	// GetAwakeFloor() is advisory elsewhere.
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
// decides what happens to that fiber and its task.
//
// FACTORED OUT OF Worker() SO IT CAN HAVE A SECOND CALLER. A bound main thread -- one that runs pool
// work while waiting, the way marl's bound thread does -- switches into fibers exactly as a worker
// does and must then run precisely this. The alternative is a second copy of a three-state machine
// with a CAS race in the middle of it, which is the kind of duplication that drifts apart and then
// hangs months later. One copy, two callers.
//
// THE CALLER OWNS THE SWITCH; this owns everything after it. On entry the fiber's context is already
// saved -- that is what makes publishing SUSPENDED safe here and unsafe anywhere earlier.
//
// THREE OUTCOMES, and each has a way of going quietly wrong that is worth naming:
//
//   DEAD          the task finished. Its WaitGroup is decremented, the fiber returns to the pool and
//                 the task is destroyed. WakeAll only fires when a waiter actually registered.
//
//   WANTS_YIELD   the fiber gave up its turn and must be re-queued. NEVER DROP IT: a yielded fiber
//                 that is not re-queued is never resumed, so its stack never unwinds and nothing it
//                 holds is released -- RAII, its WaitGroup slot, its hazard record. It goes to the
//                 owner's resumed inbox rather than its deque, because the deque is STEALABLE and a
//                 yielded fiber has a live saved context: a thief taking it would migrate it, which
//                 is the pinning invariant leaking out through the one path that looks local.
//                 push_bottom reads local; the other end of that deque is public.
//
//   WANTS_SUSPEND publish SUSPENDED -- unless a Signal/Resume raced in and left SUSPEND_SIGNALED, in
//                 which case parking would lose the wakeup, so it is re-queued instead. The CAS is
//                 what makes park-versus-signal a single atomic decision rather than two racing
//                 tests.
void Thread::OnFiberReturned(Fiber* f, Task* task) noexcept {
	Task* task_to_run = task;   // keeps the extracted body identical to what Worker() ran
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
		//
		// PINNED (unconditional since 4.0.2): NOT the deque. A yielded fiber has a live saved context
		// exactly like a suspended one, so pushing it to this worker's deque -- which IS
		// stealable -- lets a thief migrate it, and pinning would leak through the one path
		// that looks local. It reads local: push_bottom is the owner's own LIFO end. It is
		// not; the other end is public.
		//
		// The cost is real and accepted: an MPSC push instead of a deque push_bottom, and
		// the loss of LIFO reuse of a hot stack. Single-producer here (only the owner
		// yields its own fiber), and correctness outranks the cache win.
		scheduler->resumedInboxes[qIndex]->push(task_to_run);
		MarkQueuedWork();
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
}


// ---- FLOOR GROWTH AFTER A LONG BODY, SHARED BY BOTH COMPLETION ARMS -------------------------
//
// THE COMPLEMENT TO THE PUSH-SIDE CONTROLLER, NOT A DUPLICATE OF IT. Placement grows the floor only
// for a BARE producer, because a worker publishing work is a continuation and the pool it is
// publishing into is by definition awake. That rule is right for the common case and blind to one:
// a worker that publishes a wave of genuinely LONG tasks into a pool sitting at its base floor. No
// bare thread is involved, so nothing on the push path will ever grow for it.
//
// This catches that case with PROOF rather than a heuristic -- the body's duration is measured, not
// predicted, and there is still a queue behind it, so everything in that queue is about to cost the
// same again. It is one body late by construction; nothing can do better without knowing how long a
// task will run before running it.
//
// SHARED BECAUSE IT ALREADY DRIFTED TWICE. First it existed only on the fiber arm while the workload
// that needed it was Native, so it never ran. Then the Native arm gained the redistribute and the
// fiber arm did not, so fiber waves grew the floor and fed nobody -- the exact 2-participants state
// this was written to fix. Two copies of a rule are two rules.
void Thread::GrowFloorIfLongBody(long long bodyNs) {
	// Master switch: `nogrow` pins the floor at base for an A/B of the whole controller.
	if (!TaskScheduler::GetFloorGrowthEnabled()) return;
	if (bodyNs <= kLongBodyNsThread) return;
	if ((size_t)qIndex >= TaskScheduler::GetAwakeFloorBase()) return;

	// The deque is what a promoted worker can actually reach, so it is what "backlog" means here.
	const size_t waiting = scheduler->loPri[qIndex]->size();
	if (waiting < 1) return;

	TaskScheduler::NoteFloorCrowding(waiting);
	// Keep one to run next; hand out the rest. Growing without this measured almost nothing --
	// the floor reached 16 and the woken workers never got the work, because it sat on this
	// worker's deque and stealing did not find it against multi-millisecond bodies.
	scheduler->RedistributeToOverflow((size_t)qIndex, waiting - 1);
}
void Thread::Worker() {
	unsigned spinTick = 0;   // idle-pass counter, used only to rate-limit the yield below
	unsigned floorCtlTick = 0;   // subsamples the awake-floor controller; see MaybeAdjustAwakeFloor
	running.store(true, std::memory_order_release);
	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	static thread_local Task* task_to_run = nullptr;
	// IdlePolicy spin state for THIS worker. Both are reset the moment work is found, not only on
	// the fall-through to park -- otherwise a worker that spun most of its budget and then got a
	// task would carry that count into its next idle episode and park early ever after.
	// TRUE while this worker holds the elevated priority. Raised when it becomes hot, and DROPPED
	// again when it stops -- it used to be one-way, which was correct only while the hot set was
	// static. Dynamic K made "was hot once" permanent while K itself sheds; see the stand-down
	// branch in the idle section for why that mattered under NoSleep.
	// Cross-platform again as of the POSIX port -- ApplyHotPriority has a backend on every OS now.
	// Sample counter for the dynamic-K controller; only worker 0 ever looks at it.
	unsigned laneDutyTick = 0;
	// Generation-driven lane reconciliation. Seeded from the current generation so a worker that
	// starts life after a K change does not treat its own startup as a transition.
	// "This worker may have raised its OWN lane bit." Set while hot, and by the stray drain; cleared
	// when the bit is retired. Sound because nobody else can raise this worker`s bit from zero.
	bool ownsLaneBit = TaskScheduler::GetHotWorkers() > (size_t)qIndex;
	// Did this worker execute a LANE task on the previous pass? The occupancy numerator. A worker
	// busy bit is the wrong thing here: general help and pure spin must read as not-lane-busy, or
	// demotion never fires.
	// Latch for the hazard-bag drain below: set when this worker scans on going idle, cleared the
	// moment it actually runs something again. Without it the scan runs every spin iteration.
	bool scannedSinceWork = false;
	// EXCLUSIVE MODE: an ORDINARY worker gets off the hot cores. The hot workers themselves are
	// already pinned to them by StartWorker. Done here, at loop entry, because by now every worker's
	// CPU is assigned and the hot mask was published before any of them started.
	if (!(TaskScheduler::GetHotWorkers() > (size_t)qIndex))
		TaskScheduler::ExcludeCurrentThreadFromHotCpus();
	// Publish this worker as AWAKE for placement. ONCE, before the loop -- doing it per pass would
	// put a fetch_or on a shared word in the hot path, which is the contention this avoids.
	//
	// WITH NO PARK NOTHING EVER CLEARS IT, and that is honest rather than broken: a worker that
	// never sleeps IS awake. Every bit stays set, so the placement below picks uniformly at random
	// among all workers rather than round-robin. The bit only begins to DISCRIMINATE when a park
	// exists to clear it -- see TaskScheduler::PickNextWorker.
	scheduler->SetAwake((size_t)qIndex, true);
	while (running.load(std::memory_order_acquire)) {
		// Advertised-queue count for THIS pass, filled by the steal block below and read by the
		// park decision at the bottom. Pass-scoped rather than loop-scoped: it is a snapshot, and a
		// stale one would park a worker while work is queued.
		unsigned advertisedCount = 0;
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
			scannedSinceWork = false;   // we are about to run something: the latch re-arms

			// ---- CONTROLLER INPUTS, and only a floor worker pays for them ----------------------
			// tasksRun feeds the demote test (did the marginal floor worker do anything?), busyNs
			// feeds the promote-on-saturation test. Both are read per window by
			// MaybeAdjustAwakeFloor. Gated on the floor because the clock reads are the expensive
			// part and only floor workers are ever asked about -- one or two, not thirty-one.
			const bool onFloor = (size_t)qIndex < TaskScheduler::GetAwakeFloor();
			long long busyStartNs = 0;
			if (onFloor) {
				tasksRun.fetch_add(1, std::memory_order_relaxed);
				busyStartNs = MonotonicNs();
				// Publish it: the growth gate reads this to tell a long body from a trivial one.
				taskStartNs.store(busyStartNs, std::memory_order_relaxed);
			}
			// AND THE SPIN RUN ENDS HERE. spinTick counts CONSECUTIVE idle passes, so finding work
			// resets it -- see the yield below. Without this reset it counts idle passes since the
			// worker started, and a worker that just finished a task would yield 256 passes later:
			// exactly the worker most likely to be handed the next one, taken off CPU at the worst
			// possible moment. Measured with it counting cumulatively: latency/cold 3.28 -> 9.28 us.
			spinTick = 0;
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

				// Same call the fiber arm makes, and deliberately the SAME FUNCTION -- these two
				// blocks were written separately and drifted within a day (the fiber copy grew the
				// floor but never redistributed, so a wave of fiber tasks fed nobody).
				if (busyStartNs != 0) GrowFloorIfLongBody(MonotonicNs() - busyStartNs);

				// CLEAR THE STAMP. This arm continues straight back to the search loop, so the
				// clear on the fiber path below is never reached from here -- and a stamp that is
				// set but never cleared reads as "inside a task that started long ago" forever.
				// That silently disabled a duration gate that read it: the age was always enormous,
				// so the gate always said yes and looked like it was working.
				taskStartNs.store(0, std::memory_order_relaxed);

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
				// PIN IT HERE, at the ONE place a fiber becomes bound to a task, rather than at
				// AcquireFiber's three return points. The resume path deliberately does not touch
				// this: a fiber that already has a context keeps the home it was bound with, which
				// is the entire point. See Fiber::homeWorker.
				f->homeWorker = (size_t)qIndex;
				// PUBLISH THE PARK FIBER BEFORE IT CAN SUSPEND. This is the pointer NotifyWorker
				// resumes, and it is written here -- by the owning worker, before the switch-in --
				// rather than from inside the fiber. A fiber cannot safely learn which worker it is
				// on and record itself: `Thread::GetCurrent()` is a thread_local, so anything the
				// fiber derives from it before a suspend is wrong after one. The loop never
				// migrates; the fiber does. Written once, since the park task is created once.
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

			// EVERYTHING AFTER THE SWITCH LIVES IN ONE PLACE -- see Thread::OnFiberReturned. It was
			// inline here until a bound main thread needed to run the same three-state machine.
			OnFiberReturned(f, task_to_run);

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

			// CLOSE THE BUSY STAMP. Only opened for a floor worker, so this is a no-op for everyone
			// else. The Native and Coroutine arms above `continue` out before reaching here, so
			// they are not measured -- acceptable, because the signal only has to distinguish
			// "saturated" from "idle" on the one or two workers the controller asks about.
			long long bodyNs = 0;
			if (busyStartNs != 0) {
				bodyNs = MonotonicNs() - busyStartNs;
				busyNs.fetch_add(bodyNs, std::memory_order_relaxed);
			}
			taskStartNs.store(0, std::memory_order_relaxed);   // no longer inside a task

			// ---- GROW THE FLOOR HERE TOO, AND THIS IS THE ONLY PLACE THE LENGTH IS KNOWN ------
			//
			// The push path gates growth on "this worker has ALREADY been busy longer than a
			// trivial body", which is the right question and unanswerable for a wave that arrives
			// all at once: sixteen pushes land inside ~10 us, long before the worker it is queueing
			// behind has been busy for 200. So the push gate saw a short-running worker sixteen
			// times and correctly declined, and the burst never grew.
			//
			// A COMPLETION KNOWS WHAT A PUSH CAN ONLY GUESS. Here the body's duration is measured,
			// not predicted, and if a queue is still waiting then everything in it is about to cost
			// that much again. That is exactly the condition worth waking cores for, and it is why
			// this is not the completion-driven controller that was removed: that one asked about
			// utilisation windows and could not run during a burst at all. This asks one question
			// about the task that just ended, on the worker that just ended it.
			//
			// It is one body late by construction. A 16-task wave therefore costs about two waves
			// rather than one -- the first body, then the rest spread across the grown floor.
			// Nothing can do better without predicting how long a task will run before running it.
			// THE SAME FUNCTION THE NATIVE ARM CALLS. This block used to be a second copy of the
			// rule and it had already lost the redistribute half, so a wave of fiber tasks grew the
			// floor to 16 and then handed the woken workers nothing -- reproducing, on the fiber
			// path, the exact 2-participants/26 ms state the redistribute was written to fix.
			GrowFloorIfLongBody(bodyNs);

			// RUN THE CONTROLLER on a subsample of task completions. Cheap where it is called from:
			// a task has just finished, so this is not on the dispatch path, and the body early-outs
			// on its own window checks. 1-in-64 keeps the atomic reads it does off the hot path.
			// 1-in-8, NOT 1-in-64. At 64 the controller never ran during a burst: `burst` completes
			// sixteen tasks total, so the subsample tripped zero times across the whole run and the
			// floor stayed at one. The body early-outs on its own window checks, so the cost of
			// calling it more often is a clock read on one completion in eight.
			if ((++floorCtlTick & 0x7) == 0)
				TaskScheduler::MaybeAdjustAwakeFloor();

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
			// --- 3a. RESUMED FIBERS PINNED TO THIS WORKER (unconditional since 4.0.2) ---
			//
			// FIRST, ahead of both deques and every steal. A resumed fiber is not speculative work:
			// something suspended, something else signalled it, and whatever was waiting on it is
			// still waiting. It is also the one queue in the pool that NO other worker may drain, so
			// deferring it defers it for everybody.
			//
			// STRAIGHT INTO task_to_run, NEVER INTO THE DEQUE. That is the entire mechanism -- the
			// deque is stealable and this must not be. See TaskScheduler::resumedInboxes for why the
			// separation cannot be a steal predicate instead.
			//
			// ONE PER PASS, not a batch drain. Batching would mean parking the surplus somewhere,
			// and the only somewhere a worker has is its deque -- which would hand these straight
			// back to thieves and undo the pinning. Popping one and going round the loop costs an
			// extra pass and keeps the invariant local to this block.
			if (!task_to_run) {
				Task* resumed = nullptr;
				if (scheduler->resumedInboxes[qIndex]->pop(resumed) && resumed) {
					task_to_run = resumed;
					continue;
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

				// ================= VICTIM SELECTION: ADVERTISED WORKERS ONLY ===================
				//
				// ONE POLICY. The two mechanisms answer different questions and neither substitutes
				// for the other:
				//
				//   THE FLAG   "is there anything to take?" -- a presence test. One shared word,
				//              written only on a state change, so under steady load it sits
				//              shared-clean in every thief's cache.
				//   TOPOLOGY   "if I take something, from whom is it cheapest?" -- an ORDERING over
				//              victims. Never a presence test.
				//
				// So: never probe a victim whose bit is clear; use topology only to ORDER the ones
				// whose bit is set.
				//
				// THIS DOES NOT CHANGE WHICH VICTIMS ARE PROBED. tryStealFrom was already
				// flag-first, so a clear-bit victim was never probed -- the old scan merely walked
				// to it and bounced off MaybeStealable. What is deleted is the WALK, not a steal:
				// the mate vectors, the sibling's remote `busy` load, the unconditional
				// non-worker-lane probe and the two random class picks all existed to REACH victims
				// the flag then rejected.
				//
				// HINTS ARE ALLOWED TO BE LATE, BY DESIGN, which is why "poke the deque anyway in
				// case the hint went stale" is the waste and not the safety net. A stale CLEAR
				// delays one steal until the owner's next pass. A stale SET costs one failed probe,
				// after which the thief clears the bit. Paying a failed probe on every idle core on
				// every pass to insure against the first is that trade run backwards -- and under
				// NoSleep this block IS the idle loop.
				//
				// WHAT IS ACTUALLY EXPENSIVE, stated so this does not get re-litigated: steal_if on
				// another worker's deque, because the owner's pop_bottom lives on that line. Walking
				// a vector and bouncing off a bitmap test is small -- not free, but small. The
				// disaster was never the walk; it was treating the walk as a reason to touch deques.
				//
				// ROLE PICKS THE WORD. Three advertisements exist: backlog and parallel describe
				// ordinary deques, stealHintLane describes a hot worker's lane. tryStealFrom keeps
				// hot and ordinary in disjoint worlds, so a candidate list built from the wrong word
				// yields nothing but rejected calls -- that mistake silently disabled hot->hot lane
				// stealing once already, and SchedulerDynamicKTest is what caught it.
				//
				// PUBLISHERS VERIFIED BY HAND, because this policy starves if one is missing:
				//   - ordinary deque -- UpdateBacklogHint at depth >= kStealHintDepth, DRAIN-set by
				//     the owner (its only call site). Goes stale-clear while a long task runs, since
				//     a buried worker is not looping. Identical to today: the old scan would not
				//     have probed a clear bit either. If that window ever needs closing, the fix is
				//     a publisher-side set like the lane already has -- NOT a wider scan.
				//   - lazy splits -- SetParallelHint at publish, before the split becomes visible.
				//   - the NON-WORKER lane -- covered, and this is the one that would have starved
				//     ParallelFor. LaneForCurrentThread() is reached only by the lazy-split publish
				//     and its own drain, and the publish sets the parallel bit with that lane's
				//     index, so bits == 0 really does mean the lane is empty too.
				//
				// POOLS ABOVE 64 DEQUES ARE THE EXCEPTION. MaybeStealable returns true there -- the
				// bitmap cannot name those victims at all -- so every worker stays a candidate and
				// tryStealFrom's own gate is the only filter, which miss-probes. That is the thing
				// this policy exists to stop, and the fix is a SECOND HINT WORD, not a wider scan.
				// Do not "solve" >64 by scanning mates harder.
				// THE ORDINARY MASK MUST INCLUDE THE LANE AT laneHintMode 4, and leaving it out is a
				// silently disabled feature rather than a slow path. There are TWO lane branches in
				// tryStealFrom: hot->hot (the isHotWorker block) and, at mode 4 ONLY, ordinary->hot
				// -- an ordinary worker draining a backlogged lane, which is the DEFAULT (laneHintMode
				// is 4). That branch only decides what to do with a victim already chosen, so an
				// ordinary mask of backlog|parallel never names a hot worker advertising on
				// stealHintLane and the path becomes unreachable. Selection is upstream of the gate.
				//
				// The OR is conditional on the mode for the same reason the mask exists at all: at
				// modes 0/1/2/3 an ordinary thief handed a hot victim just walks into
				// `GetLaneHintMode() != 4 -> return false`, which is a call bought and thrown away.
				// EVERY READ HERE GOES THROUGH `scheduler`, NEVER THROUGH A STATIC. The static
				// LaneBacklogMask() reaches the same word via Instance(), which THROWS before the
				// pool is up -- from a noexcept function, so it is std::terminate, not an exception.
				// Workers start inside StartPool, before `instance` is assigned, so an unconditional
				// static read here killed every pool-starting test at 0xC0000409: silent, no message,
				// invisible to ASan. It survived earlier revisions only because the call sat behind
				// `isHotWorker` and K defaults to 0, so nothing ever executed it.
				// ---- SWEEP EVERY ADVERTISED VICTIM. RESTORED, AND HERE IS WHY. ----------------
				//
				// This was replaced by a single hint-directed probe per pass, on the argument that
				// probing one victim per pass over N passes covers the same victims as probing N in
				// one pass, at a fraction of the branch and cache cost. The argument was wrong in a
				// way the reasoning could not reach, and the pool dump said so:
				//
				//     EVERY WORKER AWAKE, EVERY QUEUE EMPTY, AND WORK NOT PROGRESSING.
				//
				// Thirty-one workers spinning, finding nothing, while a round trip took 664 us
				// against a healthy 2.2 -- and the frame DAG row went 11 -> 486 us in the same run.
				// Searching and not finding is precisely what one probe per pass produces when the
				// hint is momentarily wrong: nobody looks at the queue that actually holds work.
				//
				// THE SWEEP'S REAL VALUE WAS NEVER ITS HIT RATE -- it is that it is not fooled by a
				// stale bit. It probes every advertised victim before giving up, so a single missed
				// or late-cleared hint costs one wasted probe rather than hiding a queue from the
				// entire pool until something unrelated shakes it loose.
				//
				// VERTICAL ITERATION IS NOT EQUIVALENT TO HORIZONTAL ITERATION, and that is the
				// lesson worth keeping. It would be, if each pass were an independent sample of the
				// same distribution. It is not: the hint state PERSISTS across passes, so a worker
				// that picks wrongly this pass is likely to pick wrongly the next one too. N passes
				// of one probe cover N *correlated* samples; one pass of N probes covers the set.
				//
				// The cheap parts of the rewrite are kept: the popcount for advertisedCount, and the
				// random rotation so thieves do not herd onto the lowest-numbered victim.
				const size_t nq0      = scheduler->loPri.size();
				const bool   bitsUsable = nq0 <= TaskScheduler::kMaxHintQueues;
				size_t       nWords   = bitsUsable ? ((nq0 + 63) / 64) : 0;
				if (nWords > TaskScheduler::kHintWords) nWords = TaskScheduler::kHintWords;

				unsigned long long bitsW[TaskScheduler::kHintWords] = {};
				unsigned long long bitsAny = 0;
				for (size_t w = 0; w < nWords; ++w) {
					bitsW[w] = scheduler->StealHintWord(w);
					bitsAny |= bitsW[w];
					advertisedCount += platform::PopCount64(bitsW[w]);
				}

				// A queue with no bit at all is waved through rather than excluded: past
				// kMaxHintQueues nobody HAS a bit, and filtering on a bit that cannot exist would
				// remove those victims from selection entirely, which is worse than probing them.
				auto advertised = [&](int t) -> bool {
					if (!bitsUsable) return true;
					const size_t ut = (size_t)t;
					if (ut >= TaskScheduler::kMaxHintQueues) return true;
					return ((bitsW[ut >> 6] >> (ut & 63)) & 1ull) != 0;
				};

				// Nothing advertised anywhere means nothing to steal -- pool-wide, not "nothing
				// nearby". One register test ends the search without touching a queue.
				if (!bitsUsable || bitsAny != 0) {
					const int lim = (int)nq0;

					// The backoff budget counts PROBES, never skips. A candidate rejected by its
					// bit touches nothing, so charging it would blind a backed-off worker to the
					// one advertised victim sitting behind two quiet ones.
					const size_t probeLimit = (consecutiveMisses < kBackoffMissThreshold)
						? (size_t)lim : (size_t)1;
					size_t probed = 0;

					// EVERY VICTIM AT MOST ONCE PER PASS. Without this, phase 2 re-probes the mate
					// phase 1 just lost a race on -- a second remote CAS against the owner that
					// already beat us, which is the most expensive thing this block can do.
					unsigned long long tried = 0;
					auto probeOnce = [&](int t) {
						if (t == qIndex || !advertised(t)) return;
						if (t < 64) {
							if (tried & (1ull << t)) return;
							tried |= 1ull << t;
						}
						++probed;
						tryStealFrom(t);
					};

					// PHASE 1 -- advertised same-class LLC mates first, from a ROTATING start.
					// Walking from slot 0 puts every idle thief on this cluster onto the same first
					// advertised mate, which is the herd the hint exists to break up -- and worse
					// here than in phase 2, because these are the threads most likely to be
					// searching simultaneously.
					const std::vector<int>& mates = scheduler->matesSameClass[qIndex];
					if (!mates.empty()) {
						const size_t ms = FastRand() % mates.size();
						for (size_t i = 0; i < mates.size() && !task_to_run && probed < probeLimit; ++i)
							probeOnce(mates[(ms + i) % mates.size()]);
					}

					// PHASE 2 -- every other advertised victim, including the non-worker lane and
					// anything outside this LLC cluster. Rotating start for the same reason.
					if (!task_to_run && lim > 0) {
						const int start = (int)(FastRand() % (unsigned)lim);
						for (int i = 0; i < lim && !task_to_run && probed < probeLimit; ++i)
							probeOnce((start + i) % lim);
					}
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

			if (!task_to_run) {
				count = 0;
				while (count < BATCH_SIZE && scheduler->loPriInboxes[qIndex]->pop(batch[count])) {
					count++;
				}
				TaskScheduler::NoteInboxDrain(count);   // no-op unless a submit limit is set

				// ---- SELF-HEALING, NOT JUST BALANCED ---------------------------------------
				//
				// The obvious version of this is fetch_sub(count) against a fetch_add on every
				// push, and it is one missed push site away from being permanently wrong -- which
				// is exactly what happened: an un-incremented enqueue path walked the counter
				// negative over 200,000 tasks, the growth gate went dead, and the symptom was a
				// burst row that behaved as if the controller did not exist while the same code
				// worked perfectly on a freshly started pool. Nothing announced it, because a
				// heuristic reading a drifting input still looks like a heuristic that decided no.
				//
				// So when the inbox is observed EMPTY, the depth is not decremented, it is SET.
				// The owner is the only legal consumer, so "empty" is authoritative here in a way
				// a delta never is, and any accumulated error is erased the next time this worker
				// runs dry -- which on any real workload is constantly. Drift is bounded by one
				// idle cycle instead of by the lifetime of the process.
				if (scheduler->loPriInboxes[qIndex]->empty())
					inboxDepth.store(0, std::memory_order_relaxed);
				else if (count)
					inboxDepth.fetch_sub((int)count, std::memory_order_relaxed);
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
			continue;
		}
		else {
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
			// ONCE PER IDLE TRANSITION, NOT ONCE PER PASS. This used to sit immediately before a
			// BLOCKING park, so "every time we reach here" and "every time this worker goes idle"
			// were the same statement and the latch was unnecessary. With the park gone the worker
			// spins, so the same line became a full hazard-record scan at spin frequency -- on
			// every idle core at once.
			//
			// It is not only a cost. It changes OBSERVABLE reclamation timing: a retired pointer
			// that used to sit in its bag until the owner next retired is now swept within
			// microseconds, which broke the orphan test's "nothing was freed yet" outright. That
			// test is right and the behaviour was wrong.
			//
			// The latch clears wherever this worker actually runs something, so the guarantee the
			// original comment cares about is intact: a worker that retired nodes and then went
			// idle still drains them once, promptly, on the way in.
			if (!scannedSinceWork) {
				HazardDomain::Instance().Scan();
				scannedSinceWork = true;
			}

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
						|| !scheduler->loPriInboxes[qIndex]->empty()
						// PINNED RESUMES. Not optional and not merely a latency question: nobody
						// else is permitted to drain this queue, so parking on a non-empty one is
						// a permanent hang rather than a delay. Same reasoning as the loPri inbox
						// above, one degree worse -- that one at least ends when its owner wakes
						// for another reason.
						|| !scheduler->resumedInboxes[qIndex]->empty()))) {
				workerState.store(WS_AWAKE, std::memory_order_seq_cst);
				JLIBSCHED_LATENCY_MARK(Wake);
				if (!running.load(std::memory_order_acquire)) break;

				// PAUSE BEFORE GOING ROUND, and this `continue` is the reason the pool could still
				// take the machine after the backoff at the bottom was restored: it jumps to the TOP
				// of the loop and skips that backoff completely. The only exit handled here was the
				// shutdown `break`; every other path through this branch was a tight loop.
				//
				// AND IT CAN FIRE WITH NOTHING TO FIND. The predicate above is a disjunction of
				// HINTS -- hasQueuedWork, laneWake, three queue-emptiness checks -- any one of which
				// sends this worker back to search. The search can then come back empty: the flag
				// was set for work a thief already took, or a queue drained between the test and the
				// drain. The predicate fires again on the next pass, and nothing in that cycle ever
				// pauses. Thirty-one workers doing that is the same "all AWAKE, busy=0, every queue
				// empty" dump, arrived at by a different route.
				//
				// CpuRelax AND NOT THE YIELD PATH, deliberately. This branch means work probably
				// EXISTS, so the worker should get back to looking immediately -- a pause is ~35
				// cycles and costs nothing measurable, while a yield here would hand the core away
				// exactly when this worker is the one about to run something. The rate limiting
				// belongs at the bottom, where the worker has already looked and found nothing.
				platform::CpuRelax();
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
			// THE INVARIANT, asserted rather than trusted: no lane stamp may be open across a park.
			// Every regression of this bug looks the same from outside -- occupancy reads far above
			// true duty and K stops demoting -- and that symptom is several inferential steps from
			// the cause. This turns it into a stop at the exact line.

			// ---- THE PARK: NO KERNEL OBJECT, EVER ---------------------------------------------
			//
			// TWO BLOCKING PARKS USED TO LIVE HERE AND BOTH ARE DELETED. Each blocked the THREAD on
			// something the OS owns, and each was the problem rather than the mechanism:
			//
			//   the condvar  cost a mutex acquire on every notify -- on the PUSH path, the hot one.
			//   the event    removed that mutex but left TWO live mechanisms, and a worker already
			//                blocked in one when the selector flipped was never signalled again.
			//                Not a latency bug: a permanent strand. 24 of 31 workers asleep on
			//                non-empty inboxes, 0 of 24 subjects ever started. Reproduced in
			//                tests/fiber_pinning_test.cpp.
			//
			// MEASURED, so the deletion is not on principle: event/resume with a 1 ms hold-off --
			// long enough that the worker had genuinely parked -- cost 8.0 us against 5.0 us with
			// no hold-off. With the kernel object gone those same rows read 2.1 to 3.0 us. Five to
			// six microseconds, and the two rows differ in nothing but whether a thread had to be
			// woken.
			//
			// So the worker blocks on nothing. It publishes AWAKE and goes round to search again,
			// and a waker never reaches into the kernel to find it -- NotifyWorker resumes this
			// worker's park fiber instead.
			//
			// THE POOL THEREFORE SPINS WHEN IDLE, and that is deliberate, not an oversight. It is
			// the bargain IdlePolicy::NoSleep makes, and an idle pool stops being free -- it taxes
			// every other thread in the process. It is taken because at least one K-hot worker is
			// awake at all times anyway, so nothing was ever going to be woken from cold. The park
			// fiber below is what makes the second half possible: an idle worker that can STOP
			// again later without a condvar or an event behind it.
			{
				// DO NOT PUBLISH AWAKE HERE, and the omission is the whole point of this comment.
				//
				// This block used to open with `workerState.store(WS_AWAKE)` -- harmless while it
				// only advertised and spun, and fatal the moment a park was added below it. The
				// recheck above leaves this worker in WS_GOING_TO_SLEEP, which is exactly the state
				// the park's CAS consumes. Storing AWAKE first made that CAS fail, made the wait
				// predicate `workerState == WS_SLEEPING` false on entry, and so WaitOnAddress was
				// NEVER CALLED. The park ran, parked nothing, and fell through to the spin -- every
				// pass, on every worker, for the entire life of the process.
				//
				// IT COST A SESSION OF MISDIAGNOSIS because it presents identically to half a dozen
				// unrelated bugs: all workers AWAKE, busy=0, every queue empty. What finally
				// separated them was printing the PARK CONDITION beside the states -- "advertised
				// queues = 0 -> workers may park" next to 31 AWAKE workers is a contradiction, and
				// only a park that cannot fire produces it. When a dump cannot distinguish two
				// causes, add the deciding value to the dump rather than reasoning harder about it.
				//
				// So AWAKE is published on the paths that do NOT park, and nowhere else.
				if (!running.load(std::memory_order_acquire)) {
					workerState.store(WS_AWAKE, std::memory_order_seq_cst);
					break;
				}

				// NO PARK FIBER HERE. An overflow worker with nothing to run has no OTHER fibers to
				// run either, so switching into one buys nothing: a suspended fiber does not take a
				// THREAD off the core, and taking the thread off the core is the entire point for a
				// surplus worker.
				//
				// IT ALSO STACKED TWO WAITERS BEHIND ONE NOTIFY -- park fiber and WaitOnAddress --
				// which is the condvar-plus-event shape that stranded workers earlier today. Pick
				// one off-core park per worker and have exactly one thing to signal.
				//
				// A park fiber is for a thread that still has other fibers to run while one of them
				// waits. That is a different situation and this scheduler does not have it yet.

				// YIELD PERIODICALLY, NOT EVERY PASS. A pure CpuRelax loop is pathological the
				// moment the pool is not alone on the machine -- and it never is: the SUBMITTING
				// thread is not a worker. On a 31-worker pool that is 31 spinners against one
				// producer for 32 logical cores, and the producer only runs when the OS preempts
				// somebody.
				//
				// MEASURED, and it is why this came back. Deleting this with the idle backoff cost
				// single-producer throughput 2.02 -> 1.36 M/s while FOUR producers went 3.50 -> 5.03
				// -- the exact signature of starving one thread: worse when the pool is contended by
				// one outsider, better when the outsiders own more of the machine. Trimming the loop
				// made the spinners tighter, which made the starvation worse rather than better.
				//
				// 1-in-256 because the yield is the expensive half. A yield is a syscall; CpuRelax
				// is a hint. This buys the producer a scheduling opportunity at a rate that costs
				// the spinner almost nothing.
				//
				// A COUNTER, not a clock: reading a clock here would put a serialising instruction
				// in the tightest loop in the library to decide whether to do nothing.
				//
				// CONSECUTIVE IDLE PASSES, AND THAT DISTINCTION IS THE WHOLE DESIGN. spinTick is
				// reset the moment this worker runs anything, so the yield fires only for a worker
				// that has found nothing for a sustained run -- one that is genuinely surplus right
				// now, not one that is between tasks. A recently-active worker is the likeliest
				// landing site for the next push, and taking it off CPU is what a yield does.
				//
				// The first version counted cumulatively and cost latency/cold 3.28 -> 9.28 us for
				// a single-producer throughput gain, because it yielded busy workers on a timer
				// rather than idle ones on evidence.
				//
				// 1024, not 256: the yield is a syscall, and once it fires only for sustained idle
				// runs the producer still gets its scheduling opportunity while a worker that goes
				// quiet for a fraction of a millisecond stays hot.
				// THIS WAS COMMENTED OUT AND IT COST 300x. Removing the futex park took the backoff
				// with it, leaving the loop with no CpuRelax and no yield at all -- 31 workers in a
				// completely tight spin, plus a seq_cst store per worker per pass. Nothing in the
				// pool was broken; there was simply no core left for anyone else. A pool dump during
				// a 664 us round trip showed all 31 AWAKE, busy=0 and every queue empty, which is
				// exactly what a pool that cannot stop spinning looks like from outside.
				//
				// A LOOP WITH NO BACKOFF IS NOT A FAST LOOP. It is one that has taken the machine.
				// ---- THE EXIT CONDITION, AND THE PARK BEHIND IT --------------------------------
				//
				// WITHOUT THIS THE LOOP HAS NO WAY OUT. Everything above either finds work and
				// `continue`s, or falls to here and spins. AWAKE was the only state a worker could
				// reach, which is why every pool dump showed 31 workers AWAKE with busy=0 and every
				// queue empty no matter when it was taken -- that was not a symptom, it was the only
				// picture this loop could produce. CpuRelax does not surrender a core; it is a
				// pipeline hint. Thirty-one threads held 31 of 32 logical cores continuously and
				// main ran only when the OS preempted one, which is the 664 us round trip.
				//
				// THE CONDITION IS EVIDENCE, NOT A SPIN BUDGET. advertisedCount is the popcount of
				// the steal hints -- how many queues in the POOL have work a thief could take. Zero
				// means there is nowhere for this worker to go, so stopping costs nothing. Non-zero
				// means it is one probe away from useful and parking would be paid back immediately
				// in a wake. A fixed spin count cannot tell those apart; this can.
				//
				// The queue predicate was already checked immediately above -- if anything had been
				// queued to this worker we would have taken the `continue` there.
				// THE AWAKE FLOOR. Workers 0..K-1 never park: they keep spinning so a dispatch into
				// an otherwise idle pool lands on somebody already running and pays no OS wake.
				//
				// It does not change WHEN a worker may stop -- advertisedCount == 0 is still the
				// evidence -- only WHICH workers are allowed to. Read per pass so it stays
				// adjustable on a live pool; one relaxed load.
				// ---- SHED A GROWN FLOOR FROM HERE, THE MIRROR OF GROWING IT FROM PUSH ---------
				//
				// Growth is driven by the pusher because during a burst the pusher is the only
				// party awake. Shedding is driven from here for the same kind of reason: an
				// overflow worker spinning with nothing advertised anywhere is the only party that
				// can see the wave is OVER. Task completion sees neither -- which is how the floor
				// came to sit at 8 through an entire 25 ms latency row, costing p50 0.40 -> 0.90 us
				// and 1p 10.0 -> 5.74 M/s with zero kernel wakes to show for it.
				//
				// Ordered BEFORE the floor is read, so a worker that sheds can act on the new value
				// and park in this same pass instead of spinning one more time around.
				// CollapseAwakeFloorToBase does the real gating: it refuses while anything is
				// advertised and while the last growth is younger than the hold, so this is a cheap
				// couple of loads in the common case.
				if ((size_t)qIndex >= TaskScheduler::GetAwakeFloorBase() && advertisedCount == 0)
					TaskScheduler::CollapseAwakeFloorToBase();

				const bool onAwakeFloor = (size_t)qIndex < TaskScheduler::GetAwakeFloor();

				if (advertisedCount == 0 && !onAwakeFloor) {
					// Clear the awake bit BEFORE blocking so a producer stops choosing this worker
					// and paying a wake for it. Setting it late is harmless; clearing it late is not.
					scheduler->SetAwake((size_t)qIndex, false);

					int sleeping = WS_SLEEPING;
					int expectedGoing = WS_GOING_TO_SLEEP;
					workerState.compare_exchange_strong(expectedGoing, WS_SLEEPING,
						std::memory_order_seq_cst, std::memory_order_relaxed);

					// Re-test AFTER publishing SLEEPING: this is the other half of the handshake
					// with NotifyWorker's awake-skip. A notifier that loaded AWAKE and skipped must
					// be caught here, or its wake is lost. Same inputs as the predicate above, so
					// the two cannot disagree about what counts as work.
					// THIS LIST MUST MATCH THE RECHECK ABOVE, TERM FOR TERM. It did not: the
					// recheck tested hiPriInboxes and laneWake and this loop did not, so a hiPri
					// push -- or a lane wake -- could leave a worker blocked in WaitOnAddress with
					// a NON-EMPTY inbox. Inboxes are not stealable, so nobody else can drain it and
					// the task strands until something unrelated wakes that exact worker.
					//
					// It is the 1.2.0 shape exactly: two predicates that are supposed to describe
					// the same question, one of them missing a term. Every load here is seq_cst for
					// the same reason it is there -- each pairs with a seq_cst store on the setter's
					// side, and promoting one while leaving the others acquire is the bug the
					// sleepwake model's negative control reproduces.
					while (workerState.load(std::memory_order_seq_cst) == WS_SLEEPING
					       && running.load(std::memory_order_acquire)
					       && !hasQueuedWork.load(std::memory_order_seq_cst)
					       && !laneWake.load(std::memory_order_seq_cst)
					       && scheduler->hiPriInboxes[qIndex]->empty()
					       && scheduler->loPriInboxes[qIndex]->empty()
					       && scheduler->resumedInboxes[qIndex]->empty()) {
#if defined(JLIB_PLATFORM_WINDOWS)
						// Address-based: no kernel object to create, own or leak, and no mutex on
						// the push path. The waker changes workerState and then wakes this address
						// -- see Thread::Wake, where the ORDER is the whole protocol.
						::WaitOnAddress(&workerState, &sleeping, sizeof(int), INFINITE);
#elif JLIB_PLATFORM_LINUX
						// FUTEX_WAIT on the SAME WORD Thread::Wake stores to. The kernel re-checks
						// workerState == sleeping under the futex bucket lock before parking, so a
						// wake that lands between this loop's predicate and the syscall is not
						// lost -- it is observed as EAGAIN and this returns to re-test.
						FutexWait(&workerState, sleeping);
#else
						// Darwin has no park yet, so this is a spin, not a wait: slow, not wrong.
						// The floor is meaningless here -- every worker stays on its core either
						// way -- which is why GetAwakeFloor() is advisory on this platform.
						break;
#endif
					}

					workerState.store(WS_AWAKE, std::memory_order_seq_cst);
					scheduler->SetAwake((size_t)qIndex, true);
					JLIBSCHED_LATENCY_MARK(Wake);
					if (!running.load(std::memory_order_acquire)) break;
					continue;
				}

				// THE SPIN PATH, reached only when something IS advertised -- so this worker is
				// about to go looking again and must not be left advertising an intent to park.
				// Published here rather than at the top of the block, which is what broke the park.
				workerState.store(WS_AWAKE, std::memory_order_seq_cst);
				JLIBSCHED_LATENCY_MARK(Wake);

				if ((++spinTick & 0x3FF) == 0) std::this_thread::yield();
				else                           platform::CpuRelax();
			}
		}
	}
	running.store(false, std::memory_order_release);
}

