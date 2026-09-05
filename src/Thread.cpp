// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/Hazard.h"  // retire-bag flush on the way into sleep
#include "../include/Thread.h"
#include "../include/platform.h"
#include "../include/TaskScheduler.h"
#include "../include/Timer.h"   // MonotonicNs -- lane occupancy stamps
#include "../include/FiberRegistry.h"   // the fiber-death cleanup chain -- see OnFiberReturned

#include <cassert>
#include <chrono>
#include <iostream>
#include <cstring>   // std::memset -- MSVC pulls this in transitively, libstdc++ does not
#include <utility>   // std::swap, for drainInbox`s in-place corePref partition

// ApplyWorkerPriority needs these, and it is defined near the top of this file -- so they cannot live
// in the !JLIB_PLATFORM_WINDOWS block further down, which comes after it.
#if !JLIB_PLATFORM_WINDOWS
#include <sys/resource.h>       // PRIO_PROCESS, setpriority
#endif
#if JLIB_PLATFORM_LINUX
#include <sys/syscall.h>        // SYS_setpriority, SYS_gettid, SYS_futex
#include <unistd.h>
#include <linux/futex.h>        // FUTEX_WAIT_PRIVATE / FUTEX_WAKE_PRIVATE -- the worker park
#include <cerrno>
std::vector<std::unique_ptr<TaskMPSCQueue>>* Thread::normalInboxes = nullptr;
std::vector<Thread*>* Thread::workers = nullptr;
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
	// Called on the CURRENT thread only, and only when HotThreadPolicy != Normal -- the caller tests
	// that policy directly, alongside a `bandPrio != curPrio` compare that makes the common pass a
	// register test. So this is a no-op configuration by construction rather than by an extra branch on
	// the per-task path, and a syscall happens only on a genuine band transition.
	//
	// EVERY BACKEND SWALLOWS FAILURE. Elevation is privileged on POSIX and can be refused at runtime in
	// a way the Win32 call never is; refusing is not an error, it is the unprivileged answer. The
	// caller's flags then say "raised" while the OS says otherwise -- harmless, because the flags exist
	// to suppress redundant syscalls, not to describe the kernel.
	// ---- THREE LEVELS, NOT TWO, AND THE MIDDLE ONE IS THE FLOOR --------------------------------------
	//
	// This was a bool: elevated meant TIME_CRITICAL. That is the right level for a RESERVED worker, which
	// exists to answer an I/O completion the instant it lands and is idle the rest of the time. It is the
	// wrong level for a FLOOR worker: the floor is compute capacity that happens never to park, there can
	// be many of them (F reaches 28 on this pool), and a crowd of TIME_CRITICAL threads is the
	// process-wide elevation this file already records as a 5x REGRESSION -- N runnable threads at 15
	// preempt the very thread feeding them.
	//
	// So the floor gets HIGH and the lane gets CRITICAL. High still wins a timeslice contest against
	// ordinary workers, which is what the floor is for, without joining the band that outranks the
	// producer.
	enum class WorkerPrio : int { Normal = 0, High = 1, Critical = 2 };

	void ApplyWorkerPriority(WorkerPrio p) noexcept {
#if JLIB_PLATFORM_WINDOWS
		// TIME_CRITICAL inside a NORMAL_PRIORITY_CLASS process is 15: the top of the non-realtime range,
		// above the pool and below the OS. True realtime (16-31) needs REALTIME_PRIORITY_CLASS and a
		// privilege, and would let a spin loop starve the machine, so it is deliberately not asked for.
		// HIGHEST is 2 -- above the pool, still below anything the OS runs at 15.
		::SetThreadPriority(::GetCurrentThread(),
			p == WorkerPrio::Critical ? THREAD_PRIORITY_TIME_CRITICAL
			: p == WorkerPrio::High ? THREAD_PRIORITY_HIGHEST
			: THREAD_PRIORITY_NORMAL);

#elif JLIB_PLATFORM_DARWIN
		// QoS, NOT SCHED_FIFO and NOT affinity. Apple Silicon has no thread affinity API at all, and QoS
		// is the documented lever -- it is also what actually steers P vs E core selection there, which
		// is the same decision Windows makes off priority. USER_INITIATED is the middle tier: work the
		// user is waiting on, but not the interactive one.
		(void)pthread_set_qos_class_self_np(
			p == WorkerPrio::Critical ? QOS_CLASS_USER_INTERACTIVE
			: p == WorkerPrio::High ? QOS_CLASS_USER_INITIATED
			: QOS_CLASS_DEFAULT, 0);

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
		// -5 for High, halfway to the lane's -10: a real advantage over ordinary workers without putting
		// a whole floor into the band that starves the producer.
		(void)syscall(SYS_setpriority, PRIO_PROCESS, (int)syscall(SYS_gettid),
			p == WorkerPrio::Critical ? -10 : p == WorkerPrio::High ? -5 : 0);
#else
		(void)p;
#endif
	}

	// OPT THIS THREAD OUT OF (OR INTO) OS POWER THROTTLING. Windows only; every other platform is a
	// deliberate no-op, explained below.
	//
	// THIS IS NOT PRIORITY, and it is the reason ApplyWorkerPriority does not cover it. Priority decides
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
	// fails with ERROR_INVALID_PARAMETER. Failure is ignored for the same reason ApplyWorkerPriority
	// ignores EPERM: refusing is the system's answer, not an error in the caller.
	//
	// NO POSIX EQUIVALENT EXISTS, and the absence is not an oversight. Linux expresses the same idea
	// through cgroup cpu.uclamp and per-task util_clamp, which are administrative settings a library
	// has no business writing; on Android the cgroup arbitration overrides anything a thread asks for
	// anyway. macOS folds it into QoS, which ApplyWorkerPriority already sets -- QOS_CLASS_USER_INTERACTIVE
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
Thread::~Thread() {}
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
		// ONE PER CLASS. The tiny and deep caches are harmless when their class count is 0 -- Pop
		// refills from an empty queue, gets nothing, and AcquireFiber reports exhaustion for that
		// class exactly as it would for any other shortage.
		localCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity, StackClass::Standard);
		tinyCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity, StackClass::Tiny);
		deepCache.Initialize(&scheduler->GetGlobalPool(), fiberCacheCapacity, StackClass::Deep);
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
	// ---- THAT NUMBER IS DATED 2026-08-05 AND THE POOL HAS CHANGED UNDER IT --------------------
	//
	// The AWAKE FLOOR shipped in 4.0.1 on 2026-08-27, three weeks AFTER that run. So the ~45% was
	// measured on a pool where EVERY WORKER PARKED WHEN IDLE: every dispatch paid a wake, and a
	// pinned worker that could not migrate paid it worse. That is not a side effect of the
	// experiment, it IS the mechanism the number measures -- and the floor removes it for the band
	// that matters, because those threads no longer park at all.
	//
	// So the figure still explains why the default is Ideal HISTORICALLY, and it should not be
	// quoted as a current property of this scheduler. What settles it is a re-run of `hard` against
	// `ideal` at today's floor, which nobody has done. Do not read this comment as either
	// "hard is fine now" or "hard is still 45% worse"; both are guesses.
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
	// PinnedBandWidth() is K, or K+Fbase when SetBandPinIncludesFloor is on -- one definition, shared
	// with the mask builder in StartPool, so the set of cores cleared and the set of workers pinned
	// cannot disagree. Extending it to the base floor is off by default and UNMEASURED; see the
	// header, and do not confuse it with the whole-pool result, which is an argument about N.
	if (TaskScheduler::GetHotWorkerPin() && TaskScheduler::PinnedBandWidth() > (size_t)qIndex)
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
	const WORD  cpuGroup = (WORD)topology::CpuMask::GroupOf((topology::CpuId)cpu_affinity);
	const BYTE  cpuNumber = (BYTE)topology::CpuMask::BitOf((topology::CpuId)cpu_affinity);

	switch (affinityPolicy) {
	case TaskScheduler::AffinityPolicy::PhysicalOnly:   // one worker per physical core -- still a hard bind
	case TaskScheduler::AffinityPolicy::Hard: {
		GROUP_AFFINITY ga{};
		ga.Mask = (KAFFINITY)(1ULL << cpuNumber);
		ga.Group = cpuGroup;
		SetThreadGroupAffinity(nativeHandle, &ga, nullptr);
		break;
	}
	case TaskScheduler::AffinityPolicy::Ideal: {
		PROCESSOR_NUMBER pn{};
		pn.Group = cpuGroup;
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

	// RESOLVE THIS WORKER'S OWN QUEUES ONCE. See the members in Thread.h for why: every
	// `myDeque` is three dependent loads the compiler cannot hoist, and this
	// worker's index never changes after this call.
	//
	// GUARDED, because SetQueueIndex runs during pool construction and the vectors are sized in
	// the same loop. A short vector here means the caller changed that order; leaving the pointers
	// null is better than indexing past the end, and the worker would fault immediately rather
	// than reading a neighbour's queue.
	if (scheduler) {
		const size_t i = index;
		if (i < scheduler->deques.size())         myDeque = scheduler->deques[i].get();
		myDequeArray = &scheduler->deques;
		if (i < scheduler->normalInboxes.size())  myInbox = scheduler->normalInboxes[i].get();
		if (i < scheduler->laneInboxes.size())    myLane = scheduler->laneInboxes[i].get();
		if (i < scheduler->resumedInboxes.size()) myResumed = scheduler->resumedInboxes[i].get();
		g_hotExclusive = &scheduler->g_hotExclusive;
		g_hotCpuMask = &scheduler->g_hotCpuMask;
		kMaxHintQueues = scheduler->kMaxHintQueues;
		stealHintBacklog = scheduler->stealHintBacklog;
		kStealHintDepth = scheduler->kStealHintDepth;
		stealHintLane = &scheduler->stealHintLane;
		laneClearDepth = &scheduler->laneClearDepth;
		stealHintParallel = scheduler->stealHintParallel;
		laneSetDepth = &scheduler->laneSetDepth;
		allocator = &scheduler->taskAllocator;
		g_streakSpillMax = &scheduler->g_streakSpillMax;
		workers = &scheduler->workers;
		g_floorGrowCap = &scheduler->g_floorGrowCap;
		g_lastFloorGrowNs = &scheduler->g_lastFloorGrowNs;
		g_awakeFloorPeak = &scheduler->g_awakeFloorPeak;
		normalInboxes = &scheduler->normalInboxes;
		g_submitLimit = &scheduler->g_submitLimit;
		g_inboxDepth = &scheduler->g_inboxDepth;
		g_rangeRecruit = &scheduler->g_rangeRecruit;
		g_wakeCostNs = &scheduler->g_wakeCostNs;
		nextWorker = &scheduler->nextWorker;
		stealHintOn = &scheduler->stealHintOn;
		g_ioLastPushNs = &TaskScheduler::g_ioLastPushNs;
		g_ioQuietUs = &TaskScheduler::g_ioQuietUs;
		g_reservedSteal = &TaskScheduler::g_reservedSteal;
		matesSameClass = &scheduler->matesSameClass;
		matesOtherClass = &scheduler->matesOtherClass;
		clusterMates = &scheduler->clusterMates;
		g_floorMissWindowNs = &TaskScheduler::g_floorMissWindowNs;
		g_floorWindowNs = &TaskScheduler::g_floorWindowNs;   // demote observation window
		g_lastFloorUpNs = &TaskScheduler::g_lastFloorUpNs;
		g_lastFloorDownNs = &TaskScheduler::g_lastFloorDownNs;
		g_quietWindows = &TaskScheduler::g_quietWindows;
		g_laneStrandEvents = &TaskScheduler::g_laneStrandEvents;
		g_laneStrandIdleK = &TaskScheduler::g_laneStrandIdleK;
		g_laneStrandIdleKF = &TaskScheduler::g_laneStrandIdleKF;
		laneIntake = &scheduler->laneIntake;
	}
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

			// ---- ONE PUBLISH FOR THE WHOLE BATCH, NOT ONE PER TASK -------------------------
			//
			// This was a push_bottom per item: N bottom_ release-stores and N release fences to
			// publish work that arrived together. push_bottom_batch does the identical slot writes
			// -- same order, same indices -- and then one fence and one bottom_ store. Worker()'s
			// main drain has always used it; this path did not, and it is the one that runs in a
			// SPIN LOOP while a worker is blocked inside a task, so it repeats the cost.
			//
			// A THIEF SEES LESS, NOT MORE, and that is the only behavioural change: previously a
			// stealer could take item 0 while item 5 was still being pushed, now the batch appears
			// at once. Bounded by BATCH (64) and strictly fewer partial states to reason about.
			//
			// COMPACT FIRST. push_bottom_batch stores what it is handed, and a null in the ring is
			// a task nobody can run and a crash for whoever pops it -- push_bottom's null check was
			// doing this implicitly. Skipped rather than fatal, exactly as the old `continue` did:
			// a null here has never been observed and this is not the place to start aborting.
			size_t n = 0;
			for (size_t i = 0; i < count; ++i)
				if (batch[i]) batch[n++] = batch[i];
			if (n) {
				// THE POP ALREADY HAPPENED, so a refusal here could not be retried -- the inbox no
				// longer has it. Ignoring the bool (which this once did) LOST the task: its
				// WaitGroup never decremented and whoever waited on it hung.
				//
				// THE DEQUE NO LONGER REFUSES: it grows, and if it cannot grow it aborts with a
				// message rather than handing back a failure nobody can act on here.
				// NOT AN assert: that evaporates under /O2, and the failure it would have caught is
				// a SILENTLY DROPPED TASK in exactly the build where nobody is watching -- the
				// fiber stays suspended forever and the hang has no stack trace pointing here.
				if (!deque->push_bottom_batch(batch, n)) TaskDeque::FatalPushRefused();
				moved = true;
			}
		}
		};

	// ---- THE LANE IS NOT STAGED ANYWHERE --------------------------------------------------
	//
	// This drained the lane inbox into the lane deque. Both the destination and the reason for it
	// are gone: there is no lane deque, and the unload it performed is precisely the cost the lane
	// exists to avoid -- an O(depth) move of latency work into a bulk structure.
	//
	// Nothing replaces it here. The only caller is spin-help, which runs NATIVE work and has no
	// queue of its own to fill; the lane belongs to Worker(), where the owner pops one arrival and
	// runs it. The loPri drain below stays, and that one IS load-bearing: a worker blocked inside a
	// task stops running Worker(), which makes its own loPri inbox unreachable by the entire pool.
	//
	// NOT FOR K, WHICH NEVER READS LOPRI. This was the last unguarded reader of a reserved worker's
	// loPri inbox and it is the least obviously wrong of them, which is why it is worth naming: the
	// worker here is BLOCKED inside a task rather than choosing bulk over the lane, so it does not
	// break the spirit of the reservation. It breaks the letter, and the letter is what makes the
	// contract checkable -- "K never touches loPri" is worth more as an invariant with no
	// exceptions than as a rule with one defensible one.
	//
	// COSTS NOTHING WHEN THE INVARIANT HOLDS: a reserved worker's loPri inbox is empty, so this is
	// a no-op for it either way. What the gate buys is that when the invariant is VIOLATED, the
	// violation is reported by the diagnostic in Worker() instead of being quietly rehomed into a
	// stealable deque by a path nobody would think to look at.
	if ((size_t)qIndex >= TaskScheduler::GetHotWorkers())
		drain(myInbox, myDeque, false);
	return moved;
}

// ---- THE OWNER DRAINS ITS OWN LANE WHILE IT IS BLOCKED --------------------------------------
//
// THE HOLE THIS CLOSES, reproduced by tests/lane_reachability_test.cpp phase 1. A worker blocked
// inside a task is not in Worker(), so it never reaches the ONE pop that services its lane inbox.
// No thief can cover for it: an MPSC has exactly one legal consumer, and a second thread popping it
// is a data race rather than a slower steal. If what the worker is waiting FOR is a lane task that
// placement homed to its own inbox, the pool deadlocks -- measured, with q0 AWAKE, busy=1, hi=1 and
// every other worker idle.
//
// K CONCENTRATES IT. At K=1 every lane push lands on one worker, so one blocked reserved worker
// strands the entire lane. At K=0 the lane is inactive and this cannot arise at all.
//
// THIS IS NOT "GIVING SPIN-HELP A QUEUE". The owner is right here -- it is spinning in WaitFor a
// few frames up -- and it is the only thread permitted to touch this queue. It looking at its own
// inbox is the one legal way the work can move. The old rescue staged the WHOLE inbox into a deque;
// this takes ONE task per call, so nothing is moved in bulk and lane order is preserved.
//
// THE CALLER IS FIBERLESS. TryRunStolenNativeTask ends in a bare task->Execute() on the current
// stack -- that is why GetTask vets the type at the deque with steal_if and never claims a Fiber
// task. An MPSC pop is DESTRUCTIVE and cannot decline after looking, so the type is checked after
// the pop and a fiber-backed lane task is relocated: ONE task, to the owner's own loPri deque,
// where a worker that does have fiber machinery can steal and run it.
//
// NOT BACK TO THE LANE TAIL, which looks tidier and re-creates the deadlock: this worker will not
// return to Worker() while it spins, so a task pushed back to its own inbox is picked up by nobody
// -- and the case that matters is precisely the helper waiting on THAT task.
Task* Thread::TryTakeLaneTask(bool& relocated) {
	relocated = false;
	if (!scheduler || qIndex < 0) return nullptr;
	if ((size_t)qIndex >= scheduler->laneInboxes.size()) return nullptr;

	Task* t = nullptr;
	if (!myLane->pop(t) || !t) {
		if (myLane->empty())
			scheduler->ClearHiPriHint((size_t)qIndex);
		return nullptr;
	}

	if (t->type == TaskType::Fiber) {
		if (!myDeque->push_bottom(t)) TaskDeque::FatalPushRefused();
		relocated = true;
		return nullptr;
	}
	return t;
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

void Thread::CoYield(Fiber* targetFiber) {
	if (targetFiber) {
		targetFiber->CoYield();
	}
}
void Thread::Suspend(Fiber* targetFiber) {
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


void Thread::NotifyWorker(bool force) {
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
	// ---- NO BAND-BASED SKIP. BAND MEMBERSHIP IS NOT A SUBSTITUTE FOR WS_AWAKE. --------------
	//
	// A band test stood here -- "q is on the floor / reserved-never-parks, so it is spinning, skip
	// the wake". It was rewritten twice to chase the moving bands and both versions carried the
	// same structural hole: it answers "is q in the never-park SET right now", when the question a
	// skip must answer is "has q COMMITTED not to block". Those disagree on exactly one pass -- the
	// pass after a shed -- and that pass is the only time the answer matters.
	//
	// THE RACE, CONCRETELY. CollapseAwakeFloorToBase (or a K demote) shrinks the word. The worker,
	// on its next pass, reads the NEW word, finds itself outside the floor, and parks -- correctly.
	// The pusher reads the OLD word -- or a fresh word between two sheds, it makes no difference,
	// any read that is not the worker's own commitment can be stale -- finds q inside the floor,
	// and skips the wake. The push lands in an inbox whose owner just blocked, inboxes are not
	// stealable, and the task is unreachable. It is the 1.2.0 lost-wake shape triggered by a band
	// change instead of a flag, and it is why the freeze only appears when F or K SHEDS: in steady
	// state the member set never shrinks, so the two readers never disagree.
	//
	// WS_AWAKE below is different in kind, not merely in freshness: it is the sleeper's own
	// seq_cst-published state, paired with the seq_cst re-test after WS_SLEEPING is stored, and
	// that handshake is modeled (tests/verify/sleepwake_model.c). A worker that parked cannot be
	// read as AWAKE, whatever the bands did. The band skip saved one shared load ahead of this
	// one; it was an optimization for the spinning steady state, evaluated on the one pass where
	// the spin set had just shrunk. Correctness first -- if a cheaper skip is ever wanted again it
	// must be re-derived from the worker's own published state, never from band arithmetic.

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
	// ---- NO SKIP HERE ANY MORE. Wake() DECIDES, AND IT DECIDES ON THE PREVIOUS STATE. --------
	//
	// This used to load workerState and return early on WS_AWAKE. Under the permit machine that
	// test is both unnecessary and harmful:
	//
	//   UNNECESSARY -- Wake() swaps to WS_NOTIFIED and only reaches the kernel when the PREVIOUS
	//   value was WS_PARKED. A running worker costs one RMW and no syscall, which is what the skip
	//   was buying, except now it is decided by the same operation that latches the permit instead
	//   of by a separate load that can go stale between the two.
	//
	//   HARMFUL -- the load and the wake were two steps, so a worker could commit to parking
	//   between them. The old three-state protocol survived that (the commit CAS failed), but only
	//   because GOING_TO_SLEEP existed to fail against. There is no uncommitted state now: the CAS
	//   to WS_PARKED IS the commitment, so a skip decided before it would be deciding on nothing.
	//
	// `force` is kept for the shutdown and teardown callers and is now a no-op on this path -- the
	// swap latches regardless, and the kernel is reached exactly when somebody is parked. Left in
	// place rather than churned through every call site for a parameter that costs nothing.
	(void)force;

	// ---- ONE THING TO SIGNAL ---------------------------------------------------------------
	//
	// Wake() publishes the state change and wakes the address. That is the whole notify path: no
	// kernel object owned per worker, no mutex on the push side, nothing else to keep in step.
	//
	// THERE WAS A PARK FIBER HERE AND IT IS GONE. The idea was that a parked worker suspends into a
	// fiber and a notify resumes it -- no OS wake at all. It does not hold up: an overflow worker
	// with nothing to run has no OTHER fibers to switch to, and a suspended fiber does not free a
	// core, so the thread was still occupying one while "parked". Worse, it left TWO waiters behind
	// one notify -- the park fiber and the address wait -- which is the same shape as the old
	// condvar-plus-event pairing that stranded workers: whichever one the notifier did not signal
	// kept sleeping.
	//
	// WHAT THE WAKE COSTS, measured rather than assumed: event/resume with a 1 ms hold-off -- long
	// enough that the worker has genuinely parked -- costs 8.0 us against 5.0 us with no hold-off.
	// That ~3 us delta is the OS wake and nothing else, since the two rows differ in nothing else.
	// It is also the entire reason the awake floor exists: a worker that never parks never pays it.
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
	// This is the futex rule, and skipping it is not a theoretical lost wakeup: it hung Join every
	// single time and the pool intermittently, until ResumeAll and RequestStop were routed through
	// here too. Anything that wants a parked worker RUNNING has to come through this function --
	// making its work merely reachable is not the same as putting its thread back on the run queue.
	// ---- CONDVAR ARM ---------------------------------------------------------------------
	//
	// THE SAME RULE, PAID FOR DIFFERENTLY. A condvar has no memory either: notify_one delivered
	// between the waiter's predicate check and its block reaches nobody. WaitOnAddress closes that
	// window with the value compare -- a condvar can only close it with the mutex, which means the
	// AWAKE store has to happen INSIDE the lock the waiter evaluates its predicate under. Storing
	// outside and locking only around notify_one does not close it: the waiter can read the old
	// state, then take the lock, then block, and the notify is already spent.
	//
	// So this arm puts a mutex acquire on the push path. That is not an implementation wart to be
	// optimised away -- it is inherent to the primitive, and it is part of why this arm MEASURED
	// slower on Windows: 7 interleaved reps at floor=0, ~19,400 wakes per 20,000 round-trips, every
	// wait rep beating every cv rep on mean and p50 (4.09-4.25 vs 4.32-4.42 us). The bigger half of
	// that gap is on the WAITER, not here -- a condvar wake reacquires this mutex inside the wait
	// before it can re-check its predicate, so the woken thread takes a lock before it can run.
	//
	// THAT RESULT DOES NOT DEMOTE THIS ARM ON DARWIN, where it is the only park there is: no
	// WaitOnAddress, no futex, and __ulock_wait is private API. ParkPrimitiveDefault() selects it
	// there. Losing by 4% to a primitive that does not exist on the platform is not a reason to
	// spin. See design/NOTES.md and memory/futex-variance-checked.md.
	// ---- SWAP TO NOTIFIED. NOT "CAS IF PARKED", AND NOT A STORE. --------------------------
	//
	// The permit is latched unconditionally; only the PREVIOUS value decides whether an OS wake is
	// owed. That is what makes a wake unloseable -- there is no state in which this write is
	// discarded, so a wake cannot arrive "too early".
	//
	// A CAS THAT BAILS ON SEEING NOTIFIED IS NOT EQUIVALENT: it drops the permit while the word is
	// EMPTY, which is exactly the window between the sleeper's fast-path check and its commit, and
	// it loses the release edge the sleeper's park path synchronises with.
	//
	// A PLAIN STORE IS WORSE, and is what shipped here. Storing AWAKE erased whatever the word held
	// -- and, separately, published "awake" while the worker was still mid-transition, which is the
	// lie placement reads when it targets on this value.
	//
	// Modelled: tests/verify/sleepwake_permit_model.c. Removing BOTH this swap and the post-commit
	// recheck is a safety violation; either alone is survivable, which is why neither may be
	// "simplified" without reproducing that control.
	const int prev = workerState.exchange(WS_NOTIFIED, std::memory_order_seq_cst);

	// prev EMPTY or NOTIFIED: the thread is running, or already holds a permit it has not consumed.
	// It will see this on its next pass. DO NOT touch the OS thread -- that syscall is the whole
	// cost this protocol exists to avoid paying on a running worker.
	if (prev != WS_PARKED) return;

	// prev PARKED: it committed and is inside the wait. Only now is a syscall owed.
	if (TaskScheduler::GetParkPrimitive() == TaskScheduler::ParkPrimitive::CondVar) {
		// The mutex is still required on this arm. A condvar has no value compare, so the lock is
		// the only thing closing the notify-between-predicate-and-block window; the swap above
		// closes it for the address-wait arms, which is why they need no lock.
		{ std::lock_guard<std::mutex> lk(parkMx); }
		TaskScheduler::NoteWakeCall();
		parkCv.notify_one();
		return;
	}
#if defined(JLIB_PLATFORM_WINDOWS)
	// Counted: "how many pushes had to reach the kernel". Under this protocol that is now exactly
	// the number of pushes that found a COMMITTED sleeper, rather than every push to a non-AWAKE
	// word -- so the number means something narrower and more useful than it did.
	TaskScheduler::NoteWakeCall();
	::WakeByAddressSingle(&workerState);
#elif JLIB_PLATFORM_LINUX
	TaskScheduler::NoteWakeCall();
	FutexWakeOne(&workerState);
#endif
	// REACHING HERE ON DARWIN MEANS JLIB_PARK=wait, and then the store above IS the entire
	// notification -- there is no address to signal, so the worker is spinning and will see it on
	// its next pass. A default Darwin build never gets here: ParkPrimitiveDefault() selects CondVar
	// and the branch at the top of this function returns before the platform arms, so the floor is
	// a real feature on all three platforms rather than advisory on one.
	//
	// __ulock_wait would be the direct Darwin analogue of the other two arms, and it stays off the
	// table while it is private API. The condvar is not a placeholder for it -- it is the answer.
}


bool Thread::Ready() {
	return ready.load(std::memory_order_acquire);
}

// ---- THE STACK HIGH-WATER PROBE. Off unless TaskScheduler::SetStackProbe(true). --------------
//
// LAYOUT, because getting it backwards would report nonsense confidently. AllocateStack returns the
// LOW end of the region and the guard page is the lowest page of it, unbacked. Fiber::Init sets the
// stack TOP to stackBase + stackSize, and the stack grows DOWN from there into the region until it
// meets the guard. So:
//
//     [stackBase, stackBase+page)            the guard -- never touch it, that is the whole point
//     [stackBase+page, stackBase+stackSize)  what the body may use, filled here
//
// The LOWEST address ever written is the deepest the body got, so `top - lowest` is what it used.
static constexpr std::uint64_t kStackPattern = 0xC5C5C5C5C5C5C5C5ull;

static void FillStackPatternIfProbing(Fiber* f) {
	if (!f || !f->stackBase || !TaskScheduler::StackProbeEnabled()) return;
	const size_t page = platform::PageSize();
	if (f->stackSize <= page) return;                       // nothing usable past the guard
	std::uint64_t* lo = (std::uint64_t*)((char*)f->stackBase + page);
	std::uint64_t* hi = (std::uint64_t*)((char*)f->stackBase + f->stackSize);
	for (std::uint64_t* p = lo; p < hi; ++p) *p = kStackPattern;
}

// Called once the fiber has switched back out and is DEAD, so nothing is running on this stack and
// reading it is safe. Scans UPWARD from just above the guard: the first word that is no longer the
// pattern is the deepest point reached.
static void MeasureStackHighWaterIfProbing(Fiber* f) {
	if (!f || !f->stackBase || !TaskScheduler::StackProbeEnabled()) return;
	const size_t page = platform::PageSize();
	if (f->stackSize <= page) return;
	const std::uint64_t* lo = (const std::uint64_t*)((char*)f->stackBase + page);
	const std::uint64_t* hi = (const std::uint64_t*)((char*)f->stackBase + f->stackSize);
	const std::uint64_t* p = lo;
	while (p < hi && *p == kStackPattern) ++p;
	// p now points at the deepest touched word (or hi if the body never ran / touched nothing).
	const size_t used = (size_t)((const char*)hi - (const char*)p);
	detail::NoteStackHighWater(f->stackClass, used);
}

Fiber* Thread::AcquireFiber(Task* task) {
	// ---- A CLOSURE MUST NEVER BE HANDED A FIBER ROW. CHECKED HERE, ON THE OBJECT. -------------
	//
	// CreateTask's lambda overload has no TaskType parameter, so the CONSTRUCTOR cannot make a
	// lambda fiber. That is not the whole rule, because `type` is a public bitfield on a struct and
	// the library itself reassigns it. This compiles, and until this check existed nothing said a
	// word about it:
	//
	//     Task* t = sched.CreateTask([&]{ ...waits... });   // Native LambdaTask
	//     t->type = TaskType::Fiber;                        // public field
	//     sched.Push(t);                                    // a lambda fiber
	//
	// A static_assert guards a CALL. It cannot guard an OBJECT that stays mutable afterwards, and
	// the guarantee we need is about the object at the moment a row is leased to it.
	//
	// THIS IS THE ONE PLACE THAT MATTERS, which is why the cost is acceptable in Release: every
	// fiber-backed task passes through here exactly once, on a path that is already allocating a
	// stack. One bitfield test against the branch predictor's favourite outcome.
	//
	// WHY IT IS FATAL RATHER THAN A FALLBACK TO NATIVE. Running it Native instead would abort at
	// the first WaitOnEvent anyway, several frames away from the mistake and with a message about
	// the wrong thing. The body asked to suspend; the honest answer is that this task can never be
	// allowed to, and to say so at the line that caused it.
	if (task && task->lambdaBody && task->type == TaskType::Fiber) {
		std::fprintf(stderr,
			"[JLib::Scheduler] INVARIANT VIOLATED: a LAMBDA task was marked TaskType::Fiber.\n"
			"  A lambda body is a closure on the task slab. The worker loop frees that frame the\n"
			"  instant the body returns, while the fiber belongs to whoever resumes it -- two\n"
			"  owners, no shared destructor. If it suspends, it either resumes into a freed frame\n"
			"  or never reaches FiberStatus::DEAD, and its row is gone for the life of the process.\n"
			"  It will not fail here: SlabPool is append-only, so a released slot stays mapped\n"
			"  holding its old bytes and the corruption surfaces nowhere near this line.\n"
			"  CreateTask(lambda) cannot produce this -- something assigned `type` afterwards.\n"
			"  Fix: use the raw overload -- CreateTask(void(*)(void*), void* ctx, lane,\n"
			"  TaskType::Fiber) -- with the context on the caller's stack. See tests/fiber_body.h.\n");
		std::fflush(stderr);
		std::abort();
	}

	// SCRUB ON ACQUIRE, NOT ON RELEASE, AND THAT CLOSES A PRE-EXISTING HOLE.
	//
	// Fiber::ResetForReuse is called by GlobalFiberPool::ReturnBatch -- but ReleaseFiber does not
	// go through ReturnBatch. It is `localCache.Push(f)`, and ThreadLocalCache::Push scrubs
	// NOTHING; only the SPILL path (the top half, when the cache overflows) reaches ReturnBatch.
	// So a fiber pushed and popped from the local cache without that cache ever overflowing was
	// never reset -- including the localEpoch scrub whose own comment says it exists because "a
	// fiber once went back to the pool still announced at an old epoch, which is an ABA on the
	// reclaimer".
	//
	// ACQUIRE IS THE ONE POINT BOTH PATHS PASS THROUGH. Release has two (local cache, global
	// batch) and only one of them scrubbed. Here the rule states simply: a fiber handed out is
	// clean, whatever it came from.
	//
	// AND IT IS WHAT MAKES Fiber::creditors SAFE. A recycled fiber carrying a stale creditor bit
	// would bill its next occupant's cleanup to a worker that never touched it.
	// PICK THE CACHE BY THE TASK'S CLASS. A subscript, not a branch on the hot path: Standard is
	// class 0 and is what every task asks for unless it says otherwise.
	ThreadLocalCache<>& cache = CacheFor(task ? task->stackClass : StackClass::Standard);
	Fiber* f = cache.Pop();
	if (f) {
		f->ResetForReuse();
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
		fiberAcquires.fetch_add(1, std::memory_order_relaxed);   // see OutstandingFiberRows
#endif
		FillStackPatternIfProbing(f);
		return f;
	}

	f = cache.Pop();
	if (f) f->ResetForReuse();

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
			// ---- AND NAME THE CLASS THAT ACTUALLY RAN OUT. ----------------------------------
			//
			// This reported the STANDARD budget unconditionally, whatever class the caller asked
			// for -- so a task exhausting Tiny or Deep was handed a number that had nothing to do
			// with its shortage. That is not a cosmetic mismatch: when deepPerComputeWorker
			// defaulted to 0, a Deep task spun forever and this line pointed at the standard budget,
			// which was already generous. Raising the lever it named would change nothing, and the
			// one that mattered went unmentioned.
			const StackClass cls = task ? task->stackClass : StackClass::Standard;
			const char* clsName = cls == StackClass::Tiny ? "tiny"
				: cls == StackClass::Deep ? "deep" : "standard";
			const char* clsArg = cls == StackClass::Tiny ? "tinyPerKWorker"
				: cls == StackClass::Deep ? "deepPerComputeWorker"
				: "normalPerComputeWorker";
			const size_t perWorker = cls == StackClass::Tiny ? TaskScheduler::TinyFibersPerKWorker()
				: cls == StackClass::Deep ? TaskScheduler::DeepFibersPerComputeWorker()
				: TaskScheduler::StandardFibersPerWorker();
			const size_t workers = scheduler ? scheduler->GetWorkerCount() : 0;
			std::cerr << "[JLib::Scheduler] fiber pool exhausted for StackClass::" << clsName
				<< ". A SUSPENDED task holds its fiber, "
				"so the number of tasks that may be blocked AT ONCE is capped by the pool: "
				<< perWorker << " " << clsName << " per worker";
			if (perWorker == 0)
				std::cerr << " -- WHICH IS ZERO, so this class cannot be bound AT ALL and the task "
				"will retry forever rather than fail. Set " << clsArg << " above 0";
			if (workers) std::cerr << " x " << workers << " workers = " << (perWorker * workers);
			std::cerr << " total. Past that, workers re-queue and retry instead of running.\n"
				"  Usually that is a STALL that clears as blocked tasks finish. INSIDE A "
				"TaskDAG IT MAY NEVER CLEAR: if the tasks holding the fibers are waiting on "
				"work that cannot get a fiber because they are holding them all, nothing "
				"progresses again. A DAG whose concurrently-suspended nodes outnumber the "
				"budget above is the shape to look for.\n"
				"  Either block fewer tasks concurrently, or call "
				"TaskScheduler::SetFiberBudget(...) BEFORE Init() to raise the "
				<< clsArg << " argument -- which is the one this shortage is about, not "
				"whichever budget you raised last. This warning prints once.\n";
		}
	}
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
	// THE SECOND EXIT, and it counts only when a row was actually handed out. `f` is null here on
	// exhaustion -- the task is requeued and will ask again -- so counting unconditionally would
	// charge an acquire for a fiber nobody got and make the balance drift on the one path where the
	// pool is already under stress.
	if (f) fiberAcquires.fetch_add(1, std::memory_order_relaxed);
#endif
	if (f) FillStackPatternIfProbing(f);
	return f;
}

void Thread::ReleaseFiber(Fiber* f) {
	// HOME BY THE FIBER'S OWN CLASS, not the current task's -- the fiber knows where it came from
	// and a released fiber may well be a different class from whatever runs next.
	CacheFor(f->stackClass).Push(f);
}


// ---- UNIMPLEMENTED HALVES OF THE Worker() EXTRACTION. THEY ABORT ON PURPOSE. ----------------
//
// These are the other two thirds of the split that produced OnFiberReturned and
// GrowFloorIfLongBody: Worker() broken into "get a task" and "run it" so a BOUND MAIN THREAD could
// run the identical state machine instead of a second copy of it. That thread does not exist yet,
// so these were left as empty bodies -- and an empty body is the worst possible placeholder here.
//
// WHAT THE STUBS DID. `AcquireWork` returned nullptr, which is indistinguishable from "the pool is
// empty", so a caller would spin forever on a full queue. `RunTask` did NOTHING AT ALL: the task it
// was handed is never run, never completed, never destroyed -- its WaitGroup is never decremented,
// so whoever waits on it hangs, and the task leaks. A silently dropped task with a hung waiter and
// no stack trace pointing anywhere near here is the single worst failure this scheduler has, and
// two functions that produce it on their first call were sitting behind headers that describe them
// as working ("inbox drain + localQ + pop_bottom + steal").
//
// SO THEY ABORT INSTEAD. Nothing calls them today, which is exactly why this costs nothing and why
// it has to be done before something does. Aborting is not a hard stop chosen over an error return:
// there is no error to return -- a caller asking for work has no way to proceed without it, and a
// caller handing over a task has already given up ownership.
//
// NOT DELETED, because the extraction is still the plan and the reasoning for it is on
// OnFiberReturned: one copy of a three-state machine with a CAS in the middle, not two that drift.
// Implementing these means lifting the corresponding blocks out of Worker(), which is a real piece
// of work and not a cleanup.
[[noreturn]] static void FatalUnimplemented(const char* fn) {
	std::fprintf(stderr,
		"[JLib::Scheduler] FATAL: Thread::%s is not implemented.\n"
		"  It is a placeholder for the Worker() extraction that OnFiberReturned came from, and the\n"
		"  bound main thread it exists for has not been built. Calling it would %s.\n"
		"  Implement it by lifting the matching block out of Thread::Worker().\n",
		fn,
		std::strcmp(fn, "RunTask") == 0
		? "SILENTLY DROP this task -- never run, WaitGroup never decremented, waiters hung"
		: "report an empty pool forever, spinning a caller against queued work");
	std::fflush(stderr);
	std::abort();
}

Task* JLib::Thread::AcquireWork(bool& isFork)
{
	(void)isFork;
	FatalUnimplemented("AcquireWork");
}
void JLib::Thread::RunTask(Task* task, bool isFork)
{
	(void)task; (void)isFork;
	FatalUnimplemented("RunTask");
}
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
		// ---- THE FIBER IS DEAD: DOES IT OWE ANYONE? ------------------------------------------
		//
		// A fiber that incurred thread-affine state -- a COM apartment, a thread-owned handle, a
		// magazine that refuses to be remote-freed -- recorded each worker it owes in
		// Fiber::creditors. Those releases have to run ON those workers, so the fiber cannot simply
		// go back to this worker's cache: FiberRegistry walks the creditors one hop at a time and
		// the LAST hop recycles it. See FiberRegistry::AdvanceCleanup.
		//
		// THE COMMON PATH IS UNCHANGED, and that is deliberate. HasCreditors() is four relaxed
		// loads of a word this thread just finished running on, and it is false for every fiber
		// that never touched affine state -- which today is all of them, since nothing sets a
		// creditor yet. So this is inert until the resource wrappers exist, and when they do it
		// costs the ordinary fiber nothing.
		//
		// NOT ReleaseFiber IN THE OWING CASE: that pushes to this worker's LOCAL cache, and the
		// fiber must not be handed out again until its debts are paid. The chain's final hop
		// returns it to the global pool instead.
		// GATED ON WHAT IS OWED, NOT ON WHO RAN IT. This said HasCreditors() until registration at
		// pickup was wired -- at which point every fiber had a creditor, so every death dispatched
		// a cleanup task per worker to run an empty routine, and recycled through the global pool
		// instead of this worker's local cache. Being picked up is not a debt.
		//
		// So the common path is unchanged and stays unchanged: one relaxed load of a word this
		// thread just finished running on, then ReleaseFiber exactly as before.
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
		// THE OTHER HALF OF THE ROW INVARIANT. Counted HERE, inside the DEAD branch, because this is
		// the only teardown a row has -- whether it goes home directly or through the creditor
		// chain, this is the point past which the row is no longer this task's. An acquire without
		// a matching arrival here is a stranded stack.
		fiberRecycles.fetch_add(1, std::memory_order_relaxed);
#endif
		// MEASURED HERE, BEFORE THE ROW GOES ANYWHERE. The fiber has switched back out and is DEAD,
		// so nothing is running on this stack -- but the next line may hand it to the creditor chain
		// or straight back to a cache, either of which can put another body on it before we look.
		MeasureStackHighWaterIfProbing(f);
		if (f->OwesCleanup()) FiberRegistry::Instance().AdvanceCleanup(f);
		else                  ReleaseFiber(f);

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
		myResumed->push(task_to_run);
		// ---- THE ONE UNBOUNDED SOURCE, MARKED AT ITS PRODUCER --------------------------------
		//
		// A yield re-arms the resumed inbox, and the search checks that inbox BEFORE it drains the
		// loPri inbox -- so a fiber that yields in a loop makes the drain unreachable and starves
		// everything queued to this worker, permanently. Reproduced by
		// tests/yield_starvation_test.cpp: one worker, one yielding fiber, one queued task that
		// sat untouched for two seconds and ran the instant the fiber stopped.
		//
		// The ordering is not the bug -- a pinned resume has no other consumer, so checking it
		// first is what keeps a skipped resume from being a permanent hang. The bug is that the
		// ordering assumed resumed work was BOUNDED, and a yield loop is the one case that is not.
		//
		// MARKED HERE RATHER THAN DETECTED AT THE CONSUMER. The question the search needs -- "is
		// the next resumed task a yield or a genuine resume?" -- is free at this end and expensive
		// at the other: reading it there would mean peeking the head of a Vyukov MPSC without
		// advancing tail_, which is new lock-free code to answer something the producer already
		// knows. This is the same thread that will consume it, so it is a plain member write on a
		// line this worker already owns.
		//
		// GENUINE RESUMES NEVER SET IT. A resume arriving from Signal or Requeue keeps exactly the
		// priority it has today, so only the unbounded source is bounded.
		yieldedLastPass = true;
		hasQueuedWork.store(true, std::memory_order_seq_cst);
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
// ---- RECRUIT FOR A LIVE RANGE, ON EVIDENCE THIS WORKER JUST MEASURED ------------------------
//
// Called from both completion arms beside GrowFloorIfLongBody, and deliberately SEPARATE from it
// even though they share a hook. That one asks "is this pool under-provisioned for a wave of long
// tasks" and answers by moving the floor; this one asks "is a published range still handing out
// leaves worth waking somebody for" and answers by waking sleepers directly. Different question,
// different signal, different action -- and the comment on GrowFloorIfLongBody records that
// merging two rules into one site has already cost this file twice.
//
// BOTH INPUTS ARE OBSERVED, NEITHER PREDICTED, and that is the entire design. `bodyNs` was
// measured by running the leaf. The hint bit was set by a splitter that still has work. Nothing
// here estimates the total size of the range, because nothing CAN: a leaf is only representative
// of the rest if the body is uniform, and the back-loaded case is precisely where that fails. An
// oracle is not needed to answer "should one more worker come and look".
//
// WHY bodyNs/c AND NOT ONE. A leaf that cost `bodyNs` proves there was at least that much work
// available a moment ago, and in the time it took, bodyNs/c wakes could have been paid for. Waking
// exactly one would make full width cost log2(P) LEAF DURATIONS -- which for the heavy row, whose
// leaves are hundreds of microseconds, is slower than the ramp this replaces. Waking bodyNs/c
// reaches full width after roughly one expensive leaf, and stays at zero for a cheap one.
//
// THE CHEAP-BODY CASE NEEDS NO SEPARATE RULE. A trivial leaf is nanoseconds, bodyNs/c truncates to
// 0, and nothing is woken -- the same arithmetic that ramps heavy work refuses to touch work that
// cannot pay for a wake. That is the behaviour MinItersPerWorker currently approximates with an
// iteration count, which cannot distinguish the two because it never looks at the body.
void Thread::RecruitForLiveRange(long long bodyNs) {
	if (!TaskScheduler::RangeRecruitEnabled()) return;

	const long long c = (long long)TaskScheduler::GetWakeCostNs();
	if (bodyNs <= c) return;                       // this leaf did not pay for a wake

	// SECOND, because it is the more expensive check -- a scan of the hint words against one
	// compare. Ordering matters: the overwhelmingly common caller is a task that is not range work
	// at all, and it should fall out on the arithmetic.
	if (!scheduler->AnyParallelHint()) return;     // nothing live to recruit for

	size_t n = (size_t)(bodyNs / c);
	const size_t sz = scheduler->workers.size();
	if (n > sz) n = sz;                            // cannot use more than the pool
	scheduler->WakeForSteal(n);

	// ---- AND KEEP THEM, or the wake is spent for one leaf and re-paid on the next range --------
	//
	// Waking a worker that parks again on its next idle pass buys one steal. The floor is what
	// makes a wake persist, and the growth controller already has the timer for it: the shed
	// refuses while the last growth is younger than the hold, and every further growth pushes that
	// out -- so a workload that keeps producing expensive leaves keeps the herd out, and one that
	// stops gets the floor back automatically. "Release the herd for x ms" with no new mechanism.
	//
	// HERE AND NOT AT RANGE PUBLISH, and the distinction is the whole reason this is not another
	// failed ParallelFor experiment. A publish-time trigger fires before any leaf has run, so it
	// would have to GUESS whether the range deserves the pool -- and growth is not free (measured:
	// 3x on single-producer throughput). Hanging it here means it cannot fire without a leaf that
	// already demonstrated it cost more than a wake. A range of cheap bodies never reaches this
	// line, so it never pays.
	//
	// THE SAME AUTHORITY, deliberately. Per-thread spin deadlines would be a SECOND answer to "may
	// I park" alongside the floor, and two mechanisms owning that question is the shape of the
	// event/condvar bug that left 24 of 31 workers asleep on non-empty inboxes. The floor already
	// owns it; this only moves it.
	TaskScheduler::NoteFloorCrowding(n);
}

void Thread::GrowFloorIfLongBody(long long bodyNs) {
	// Master switch: `nogrow` pins the floor at base for an A/B of the whole controller.
	if (!TaskScheduler::GetFloorGrowthEnabled()) return;
	if (bodyNs <= kLongBodyNsThread) return;
	// FLOOR WORKERS ONLY, AND THE FLOOR IS [K, K+F). This read `qIndex >= base` as if the floor
	// began at 0, which after the band split meant a RESERVED worker could grow the floor -- the
	// I/O cores driving a compute-capacity decision, and growth then waking more of themselves.
	{
		// ONE LOAD FOR BOTH FIELDS. This asked GetHotWorkers() for K and GetAwakeFloorBase() for
		// Fbase separately, so the two halves of one band test could come from different instants:
		// with K moving, a worker could pass "I am not reserved" against the new K and "I am within
		// the floor" against a base read before it, and grow the floor from inside the reserved
		// band -- the I/O cores making a compute-capacity decision, which is what this gate exists
		// to prevent.
		const TaskScheduler::Bands gb = TaskScheduler::GetBands();
		const size_t q = (size_t)qIndex;
		if (q < gb.k) return;                                  // reserved: not our call
		if (q - gb.k >= gb.fbase) return;                      // above the floor
	}

	// The deque is what a promoted worker can actually reach, so it is what "backlog" means here.
	const size_t waiting = myDeque->size();
	if (waiting < 1) return;

	TaskScheduler::NoteFloorCrowding(waiting);
	// Keep one to run next; hand out the rest. Growing without this measured almost nothing --
	// the floor reached 16 and the woken workers never got the work, because it sat on this
	// worker's deque and stealing did not find it against multi-millisecond bodies.
	scheduler->RedistributeToOverflow((size_t)qIndex, waiting - 1);
}
// Consecutive failed steal attempts by THIS worker. Read by the steal backoff and by the park
// decision -- see the park site for why "advertised" alone is not a reason to stay awake.
static thread_local int consecutiveMisses = 0;

// Consecutive failed steals after which a worker gives up and parks even though something is still
// advertised. Also bounds the per-pass probe count -- see both call sites.
static constexpr int kBackoffMissThreshold = 8;
using namespace std::chrono;

void Thread::Worker() {

	//	tsanSchedulerFiber = tsan::CurrentFiber();

//	WorkerPrio curPrio = WorkerPrio::Normal;
	unsigned spinTick = 0;     // FLOOR idle passes -- rate-limits the yield, nothing else
	unsigned idlePasses = 0;   // ALL idle passes, for Thread::SpinTick(); see the store site
	unsigned floorCtlTick = 0;   // subsamples the awake-floor controller; see MaybeAdjustAwakeFloor
	unsigned laneCtlTick = 0;   // subsamples the K controller from a reserved worker; see below
	running.store(true, std::memory_order_release);
	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	static thread_local Task* task_to_run = nullptr;
	static thread_local bool laneSourced = false;
	bool ownsLaneBit = false;
	bool hotCpuExcluded = false;
	std::atomic<unsigned long long>* const awakeWord =
		((size_t)qIndex < TaskScheduler::kMaxHintQueues)
		? &scheduler->awakeHint[(size_t)qIndex >> 6] : nullptr;
	const unsigned long long awakeMask = 1ull << (qIndex & 63);

	if (awakeWord) awakeWord->fetch_or(awakeMask, std::memory_order_relaxed);
	while (running.load(std::memory_order_acquire)) {

#if !defined(JLIB_FIBERHOLDER_CTL_NO_WORKER_DRAIN)
		if (FiberRegistry::Instance().HolderHasWork((size_t)qIndex))
			FiberRegistry::Instance().DrainHolder((size_t)qIndex);
#endif  // CONTROL: worker never drains its own chain -- the live test's point-1 case must fail.


		unsigned advertisedCount = 0;
		JLIBSCHED_PHASE(qIndex, Start);
		//const TaskScheduler::Bands bandsNow = TaskScheduler::GetBands();

		const bool isReservedWorker = (size_t)qIndex < BandField(BandsWord(), kBandK);

	/*	auto drainOwnInbox = [&]() -> bool {
			if (task_to_run || isReservedWorker) return false;
			JLIBSCHED_PHASE(qIndex, InboxDrain);
			size_t count = 0;
			while (count < BATCH_SIZE && myInbox->pop(batch[count]))
				++count;
			TaskScheduler::NoteInboxDrain(count);   // no-op unless a submit limit is set
			if (myInbox->empty())
				inboxDepth.store(0, std::memory_order_relaxed);
			else if (count)
				inboxDepth.fetch_sub((int)count, std::memory_order_relaxed);
			if (count == 0) return false;

			const int keep = (int)count - 1;
			if (keep == 0 || myDeque->push_bottom_batch(batch, (size_t)keep)) {
				for (int s = 0; s < keep; ++s) JLIBSCHED_STEAL_STAT(qIndex, stagedFromInbox);
				JLIBSCHED_STEAL_STAT(qIndex, ranDirect);
				task_to_run = batch[count - 1];
				JLIBSCHED_LATENCY_MARK(Found);
				return true;
			}
			};
/*
		const WorkerPrio bandPrio =
			isReservedWorker ? WorkerPrio::Critical
			: ((size_t)qIndex >= bandsNow.k && (size_t)qIndex < bandsNow.k + bandsNow.f)
			? WorkerPrio::High
			: WorkerPrio::Normal;

		// ONLY ON A GENUINE BAND TRANSITION. This is ApplyWorkerPriority() inlined, and the
		// `bandPrio != curPrio` compare the file-header comment specifies was dropped along with the
		// call -- so SetThreadPriority / pthread_set_qos_class_self_np / setpriority fired on EVERY
		// PASS. A syscall per pass, in the loop that was flattened to remove a CALL per pass.
		//
		// curPrio was already declared at the top of Worker() for exactly this and was never read.
		// It starts at Normal, which is accurate: nothing touches a worker's priority between thread
		// start and here (the only other callers of the ladder are inside ApplyWorkerPriority
		// itself), so a worker whose band is Normal correctly skips the first apply too.
		//
		// Common pass is now a register compare. NOTE the remaining half of the header's design is
		// still unwritten: it also says the caller should test HotThreadPolicy != Normal, which
		// would skip elevation entirely when the policy is off. That CHANGES behaviour rather than
		// just removing redundant work, so it is deliberately not done here.
		/*
		if (bandPrio != curPrio) {
			curPrio = bandPrio;
#if JLIB_PLATFORM_WINDOWS
			::SetThreadPriority(::GetCurrentThread(),
				bandPrio == WorkerPrio::Critical ? THREAD_PRIORITY_TIME_CRITICAL
				: bandPrio == WorkerPrio::High ? THREAD_PRIORITY_HIGHEST
				: THREAD_PRIORITY_NORMAL);

#elif JLIB_PLATFORM_DARWIN
			(void)pthread_set_qos_class_self_np(
				bandPrio == WorkerPrio::Critical ? QOS_CLASS_USER_INTERACTIVE
				: bandPrio == WorkerPrio::High ? QOS_CLASS_USER_INITIATED
				: QOS_CLASS_DEFAULT, 0);

#elif JLIB_PLATFORM_LINUX
			(void)syscall(SYS_setpriority, PRIO_PROCESS, (int)syscall(SYS_gettid),
				bandPrio == WorkerPrio::Critical ? -10 : bandPrio == WorkerPrio::High ? -5 : 0);
#else
			(void)bandPrio;
#endif
		} */

	if (!hotCpuExcluded && !isReservedWorker) {
			do {
#if JLIB_PLATFORM_WINDOWS
				if (!g_hotExclusive->load(std::memory_order_relaxed)) break;
				const unsigned long long hot = g_hotCpuMask->load(std::memory_order_relaxed);
				if (!hot) break;
				DWORD_PTR procMask = 0, sysMask = 0;
				if (!::GetProcessAffinityMask(::GetCurrentProcess(), &procMask, &sysMask)) break;
				const DWORD_PTR keep = (DWORD_PTR)(procMask & ~(DWORD_PTR)hot);
				if (keep) ::SetThreadAffinityMask(::GetCurrentThread(), keep);
#endif
			} while (0);
			hotCpuExcluded = true;
		} 
		// NOT part of the commented hot-CPU block above -- the `*/` used to sit after this line, so
		// commenting that block silently took this declaration with it, and the `&& !hiPriStray`
		// guard below had to be commented out to keep compiling.
		//const bool hiPriStray = !isReservedWorker && !myLane->empty();

		if (!task_to_run) {
			const size_t depth = myDeque->size();
			//scheduler->UpdateBacklogHint((size_t)qIndex, depth);
			if (qIndex < TaskScheduler::kMaxHintQueues) {
				auto& word = stealHintBacklog[qIndex >> 6];
				const unsigned long long bit = 1ull << (qIndex & 63);
				const bool want = depth >= TaskScheduler::kStealHintDepth;
				if (want != ((word.load(std::memory_order_relaxed) & bit) != 0)) {
					if (want) word.fetch_or(bit, std::memory_order_release);
					else      word.fetch_and(~bit, std::memory_order_relaxed);
				}
			}
			//scheduler->ClearParallelHintIfEmpty((size_t)qIndex, depth);
			if (qIndex >= TaskScheduler::kMaxHintQueues || depth != 0) {}
			else
				stealHintParallel[qIndex >> 6].fetch_and(~(1ull << (qIndex & 63)), std::memory_order_relaxed);

			if (isReservedWorker) {

				const size_t laneDepth = myLane->empty() ? 0u : 1u;
				scheduler->UpdateLaneHint((size_t)qIndex, laneDepth);
				ownsLaneBit = true;   // while hot, this worker is the one maintaining its bit

			}

			else {

				// !hiPriStray RESTORED. Without it a NON-reserved worker whose lane inbox still has
				// work retracted its own stealHintLane bit anyway: the work stays, but no thief sees
				// it advertised -- and stealHintLane feeds advertisedCount, whose == 0 is the park
				// gate's first condition. So the pool could park on lane work that exists.
				if (ownsLaneBit) {
					// UpdateLaneHint INLINED, IN ITS OWN SCOPE, and the original call REMOVED -- it was
					// still here above the copy, so the hint was being written twice.
					//
					// Both early-outs were `return`s meaning "nothing to change". As bare breaks the
					// nearest enclosing loop is the PASS LOOP, and this sits ABOVE ready.store, so a
					// reserved worker taking the common path never reported ready at all.
					//
					// ownsLaneBit = false MOVED OUT of the scope: it must clear whether or not the bit
					// needed writing, or the worker re-enters this every pass forever.
					do {
						if (qIndex >= 64) break;
						const unsigned long long bit = 1ull << qIndex;
						const bool isSet = (stealHintLane->load(std::memory_order_relaxed) & bit) != 0;
						const bool want = isSet ? (depth > (size_t)laneClearDepth->load(std::memory_order_relaxed))
							: (depth >= (size_t)laneSetDepth->load(std::memory_order_relaxed));
						if (want == isSet) break;
						if (want) stealHintLane->fetch_or(bit, std::memory_order_release);
						else      stealHintLane->fetch_and(~bit, std::memory_order_relaxed);
					} while (0);   // do/while(0): these breaks mean "nothing to change", not "exit Worker()"
					}
					// OUTSIDE the guard, deliberately -- see the note above: it must clear whether or
					// not the bit needed writing, or the worker re-enters this every pass forever.
					ownsLaneBit = false;
				}
			}


		ready.store(true, std::memory_order_release);

		hasQueuedWork.store(false, std::memory_order_seq_cst);

		laneWake.store(false, std::memory_order_seq_cst);
		// --- 1. Execute task if found ---
		if (task_to_run) {


			const bool onFloor = (size_t)qIndex >= BandField(BandsWord(), kBandK)
				&& (size_t)qIndex < BandField(BandsWord(), kBandK) + BandField(BandsWord(), kBandF);
			long long busyStartNs = 0;
			if (onFloor) {
				tasksRun.fetch_add(1, std::memory_order_relaxed);
				busyStartNs = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
				// Publish it: the growth gate reads this to tell a long body from a trivial one.
				taskStartNs.store(busyStartNs, std::memory_order_relaxed);
			}

			long long laneStartNs = 0;
			if (isReservedWorker && BandField(BandsWord(), kBandKmax) > BandField(BandsWord(), kBandKmin)) {
				laneCyclesTotal.fetch_add(1, std::memory_order_relaxed);   // sample count
				if (laneSourced) {
					laneTasksRun.fetch_add(1, std::memory_order_relaxed);
					laneStartNs = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
				}
			}
			laneSourced = false;   // describes exactly one task -- cleared as it is dispatched


			constexpr size_t kPublishAtMost = 32;   // <= BATCH_SIZE
			constexpr size_t kDequeHeadroom = 256;
			if (!isReservedWorker
				&& inboxDepth.load(std::memory_order_relaxed) >= (int)kStealHintDepth
				&& myDeque->size() < kDequeHeadroom) {
				size_t got = 0;
				while (got < kPublishAtMost && myInbox->pop(batch[got]))
					if (batch[got]) ++got;

				size_t moved = 0;
				if (got) {
					myDeque->push_bottom_batch(batch, got);

					moved = got;
#ifdef JLIBSCHED_STEAL_STATS
					for (size_t s = 0; s < got; ++s) JLIBSCHED_STEAL_STAT(qIndex, stagedFromInbox);
#endif

				}
				if (moved) {
					inboxDepth.fetch_sub((int)moved, std::memory_order_relaxed);

					//scheduler->UpdateBacklogHint((size_t)qIndex, myDeque->size());
					// UpdateBacklogHint INLINED, IN ITS OWN SCOPE. Both early-outs were `return`s meaning
					// "the bit already says the right thing" -- the COMMON case. As bare breaks the nearest
					// enclosing loop is the PASS LOOP, so a worker left Worker() the first time it drained an
					// inbox and found its hint already correct. Startup looked fine because this sits under
					// `if (moved)`, which cannot fire until real work arrives.
					do {
						if (qIndex >= TaskScheduler::kMaxHintQueues) break;
						auto& word = stealHintBacklog[qIndex >> 6];
						const unsigned long long bit = 1ull << (qIndex & 63);
						const bool want = myDeque->size() >= TaskScheduler::kStealHintDepth;
						if (want == ((word.load(std::memory_order_relaxed) & bit) != 0)) break;
						if (want) word.fetch_or(bit, std::memory_order_release);
						else      word.fetch_and(~bit, std::memory_order_relaxed);
					} while (0);   // do/while(0): "the bit already says the right thing" is the COMMON case
				}
			}


			//spinTick = 0;
			// THE PREDICATE IS NOT A FLAG, AND THAT IS WHY IT MUST STAY A CALL. The disposal below
			// is DiscardIfCancelled's body inlined -- fine, it is a fixed sequence of statements --
			// but its GUARD is `IsTaskCancelled`, which is two ways to be cancelled:
			// `cancelledDirect` and a live CancelToken. Transcribing only the flag half silently
			// stops discarding every task cancelled through its SCOPE, which is the common case:
			// dag_cancel_test cancels a scope, stamps 500 tasks with its token, and all 500 ran.
			// The predicate has grown a term once already; the flag is not a substitute for it.
			//
			// ONLY AN UNSTARTED TASK MAY BE DISCARDED. A queued entry may be a RESUME -- Resume ends
			// in Requeue(owningTask), and a coroutine awaiter re-pushes its Task the same way -- so
			// discarding a started one abandons a live stack instead of cancelling it. A started
			// task is let through and observes the cancellation at its own next suspend or poll.
			if (task_to_run && !task_to_run->started && IsTaskCancelled(task_to_run)) {
				do {
					if (task_to_run->fn != &OnTaskFinishedWrapper || !task_to_run->data) break;
					auto* node = static_cast<TaskNode*>(task_to_run->data);
					node->owner->OnTaskFinished(node, TaskNode::Outcome::Cancelled);
				} while (0);

				if (task_to_run->waitGroup) {
					const int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						task_to_run->waitGroup->WakeAll();
				}
				if (task_to_run)
					(void)task_to_run;;
				if (!task_to_run->trivialDtor) task_to_run->~Task();
				allocator->Free(task_to_run);
				task_to_run = nullptr;
				ready.store(true, std::memory_order_release);
				continue;
			}

			task_to_run->started = 1;


			const bool fiberless = (task_to_run->type == TaskType::Native && !isReservedWorker);

			if (fiberless) {
				currentRunningTask = task_to_run;
				JLIBSCHED_PHASE(qIndex, Running);
				busy.store(true, std::memory_order_relaxed);
				task_to_run->Execute();


				if (task_to_run->waitGroup) {
					int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						task_to_run->waitGroup->WakeAll();   // only touches wg if someone registered
				}
				busy.store(false, std::memory_order_relaxed);
				currentRunningTask = nullptr;

				if (task_to_run)
					(void)task_to_run;
				if (!task_to_run->trivialDtor) task_to_run->~Task();

				allocator->Free(task_to_run);


				if (busyStartNs != 0) {
					const long long bodyNsHere = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() - busyStartNs;
					// ---- GrowFloorIfLongBody INLINED, IN ITS OWN SCOPE ----------------------------
					// Without this do/while(0) all FOURTEEN of the early-outs below bound to
					// `while (running)` at the top of Worker() -- so the first body that was not long
					// enough to grow the floor ENDED THE THREAD. Symptom: workers sitting at
					// state=NOTIFIED with `run 0` and a task in their inbox, notified and never
					// drained, while main spun in WaitFor.
					do {
						if (g_streakSpillMax->load(std::memory_order_relaxed) == 0u) break;   // growth disabled
						if (bodyNsHere <= kLongBodyNsThread) break;
						const size_t q = (size_t)qIndex;
						if (q < BandField(BandsWord(), kBandK)) break;                          // reserved: not our call
						if (q - BandField(BandsWord(), kBandK) >= BandField(BandsWord(), kBandFbase)) break;        // above the floor
						const size_t waiting = myDeque->size();
						if (waiting < 1) break;
						//TaskScheduler::NoteFloorCrowding(waiting);
						if (!(g_streakSpillMax->load(std::memory_order_relaxed) != 0u)) break;
						const size_t workersize = workers->size();
						if (workersize < 2) break;


						const uint64_t bw = BandsWord();
						const size_t   k = BandField(bw, kBandF);       // live F -- the value being grown
						const size_t   kNow = BandField(bw, kBandK);       // live K -- where the floor starts
						const size_t   fbase = BandField(bw, kBandFbase);   // policy base -- the wave cap's floor

						if (workersize <= kNow + 1) break;              // no room for a floor at all: refuse rather than wrap

						const size_t structural = (workersize >= kNow + 2) ? (workersize - kNow - 2) : 0;
						if (structural == 0) break;
						const size_t userCap = g_floorGrowCap->load(std::memory_order_relaxed);
						size_t cap;
						if (userCap) cap = userCap;
						else {
							cap = (workersize >= 2) ? (workersize - 2) : 0;
							if (cap < fbase) cap = fbase;
						}
						if (cap > structural) cap = structural;
						if (k >= cap) break;

						size_t wantone = k + (waiting ? waiting : 1);
						const size_t waveCap = (waiting ? waiting : 1) + fbase;
						if (wantone > waveCap) wantone = waveCap;
						if (wantone > cap)     wantone = cap;

						if (wantone < k) break;


						// BandsSetF (TaskScheduler.h) -- ONE definition; this was a hand-copy of its body.
						const size_t prev = BandsSetF(wantone, workersize);
						// ---- THE HOLD MEANS "SINCE THE FLOOR GREW", NOT "SINCE SOMEBODY ASKED" --------------------
						//
						// This stamped unconditionally, so every call pushed the collapse's 6 ms hold out -- including
						// calls that changed nothing. MEASURED over one bench run: 5,543,329 collapse attempts, of which
						// 5,541,706 (99.97%) were refused by the hold and only 74 ever shed. The floor was above base for
						// all but 1,470 of those attempts. A hold that is re-armed by the act of asking is not a hold,
						// it is a ratchet with a timer on it.
						if (wantone > prev)
							g_lastFloorGrowNs->store(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);

						// PEAK, BECAUSE THE CURRENT VALUE IS UNOBSERVABLE BY THE TIME ANYONE ASKS. The collapse fires
						// from an idle overflow worker the moment the wave drains, so a caller that measures a wave and
						// then reads GetAwakeFloor() reads the BASE -- it has already shed. That produced a burst line
						// claiming `1 -> 1` next to a 7.02 ms wall time, two numbers that cannot both be true: sixteen
						// 3.28 ms tasks on one worker is ~50 ms, and 7.02 ms is two waves. The banner was measuring the
						// shed, not the growth. A high-water mark is the only honest way to report a transient.
						for (size_t peak = g_awakeFloorPeak->load(std::memory_order_relaxed); wantone > peak; )
							if (g_awakeFloorPeak->compare_exchange_weak(peak, wantone, std::memory_order_relaxed)) break;

						// A promotion must WAKE what it promoted, or it is a number change and nothing else -- the bug
						// that made the dump read `awake bits = 2 (floor = 6)`.
						if (wantone <= prev) break;

						const size_t kResv = BandField(BandsWord(), kBandK);
						for (size_t i = kResv + prev; i < kResv + wantone && i < workersize; ++i)
							if (Thread* w = (*workers)[i]) w->NotifyWorker(/*force*/ true);


						// ---- RedistributeToOverflow INLINED, IN ITS OWN SCOPE ----------------------------
						// do/while(0) for two reasons: its k/baseF/liveF/n shadow names already live in the
						// enclosing block, and its early-out was a `return` from a helper -- which inlined
						// here would leave Worker() entirely rather than skip the redistribute.
						do {
								const size_t k = BandField(BandsWord(), kBandK);
								const size_t baseF = BandField(BandsWord(), kBandFbase);
								const size_t liveF = BandField(BandsWord(), kBandF);
								if (liveF <= baseF || (size_t)qIndex >= workers->size()) break;

								const size_t workersize = workers->size();
								const size_t span = liveF - baseF;

								for (size_t i = 0; i < waiting - 1; ++i) {
									// pop_bottom, because this IS the bottom's owner. The top stays available to thieves the
									// whole time, so this competes with stealing rather than blocking it -- if a thief already
									// took the task, pop_bottom simply comes back empty and there is nothing to hand out.
									auto opt = (*myDequeArray)[qIndex]->pop_bottom();
									if (!opt) break;
									Task* t = *opt;
									if (!t) break;

									const size_t target = k + baseF + (i % span);
									if (target >= workersize || !(*workers)[target]) { 
										const size_t n = normalInboxes->size();
										const size_t k = BandField(BandsWord(), kBandK);     // ONE load, per your own header note
										size_t r = k + (n > k ? FastRand() % (n - k) : 0);   // draw directly from [k, n)
										(*normalInboxes)[r]->push(t);   // `t`, not task_to_run -- t is what pop_bottom just handed us
										(*workers)[r]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
										(*workers)[r]->MarkQueuedWork();
										(*workers)[r]->NotifyWorker(/*force*/ true);
										 continue; }

									(*normalInboxes)[target]->push(t);
									(*workers)[target]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
									// NoteInboxPush(1) INLINED. Its `return` meant "skip the depth accounting" -- as a `break`
									// it aborted the whole redistribute loop, and g_submitLimit defaults to 0 so it fired on
									// the FIRST iteration of every run. Worse, it broke BEFORE MarkQueuedWork and
									// NotifyWorker below, so the task just pushed sat in another worker's inbox with nobody
									// woken to drain it -- the exact stranding the force-wake comment below exists to stop.
									// The count is 1, not n: the argument was tasks pushed; `n` here is workers->size().
									if (g_submitLimit->load(std::memory_order_relaxed) != 0)
										g_inboxDepth->fetch_add(1, std::memory_order_relaxed);
									(*workers)[target]->MarkQueuedWork();
									// Force, because these indices are inside the grown floor and NotifyWorker's floor-skip
									// would otherwise drop the wake on the assumption that a floor worker is already looking.
									// It usually is -- growth just woke it -- but "usually" is how a task strands in an inbox
									// nobody else may drain.
									(*workers)[target]->NotifyWorker(/*force*/ true);
								}
						} while (0);   // closes the RedistributeToOverflow scope
					} while (0);   // closes the GrowFloorIfLongBody scope
					//	RecruitForLiveRange(bodyNsHere);
						// RecruitForLiveRange INLINED, IN ITS OWN SCOPE. Its early-outs were `return`s from a
						// helper; unwrapped here they were breaking the PASS LOOP, so a worker left Worker() the
						// first time a body was too short to pay for a wake -- the common case.
						// COSTS NOTHING: `while (0)` is a constant-false condition, so no test and no
						// back-edge is emitted -- verified against /O2 codegen, identical to the same
						// block written with gotos. The scope buys legal shadowing and correct `break`
						// binding for free.
						do {
							if (!g_rangeRecruit->load(std::memory_order_relaxed)) break;

							const long long c = (long long)g_wakeCostNs->load(std::memory_order_relaxed);
							if (bodyNsHere <= c) break;                       // this leaf did not pay for a wake

							// SECOND, because it is the more expensive check -- a scan of the hint words against one
							// compare. Ordering matters: the overwhelmingly common caller is a task that is not range work
							// at all, and it should fall out on the arithmetic.
							size_t res = -1;
							for (size_t w = 0; w < TaskScheduler::kHintWords; ++w)
								if (stealHintParallel[w].load(std::memory_order_acquire)) { res = w; break; }
							if (res == -1) break;
							size_t n = (size_t)(bodyNsHere / c);
							const size_t sz = workers->size();
							if (n > sz) n = sz;                            // cannot use more than the pool
							if (!n) break;
							const unsigned r = (unsigned)nextWorker->fetch_add(1, std::memory_order_relaxed);
							size_t woken = 0;
							for (size_t i = 0; i < sz && woken < n; ++i) {
								const size_t idx = ((size_t)r + i) % sz;
								Thread* w = (*workers)[idx];
								if (w && w->GetWorkerState() == 2 /* WS_SLEEPING */) {
									w->NotifyWorker(/*force*/ true);
									++woken;
								}
							}
							size_t submitted = n;

						
		
						//;
						//TaskScheduler::NoteFloorCrowding(n);
						// ---- NoteFloorCrowding INLINED, IN ITS OWN SCOPE --------------------------------
						// do/while(0) because its `n` shadows the recruit block's, and its early-outs were
						// `return`s from a helper -- inlined here they would leave Worker() entirely.
						do {
							if (!(g_streakSpillMax->load(std::memory_order_relaxed) != 0u)) break;
							const size_t workersize = workers->size();
							if (workersize < 2) break;
							const uint64_t bw = BandsWord();
							const size_t   k = BandField(bw, kBandF);       // live F -- the value being grown
							const size_t   kNow = BandField(bw, kBandK);       // live K -- where the floor starts
							const size_t   fbase = BandField(bw, kBandFbase);   // policy base -- the wave cap's floor
							if (workersize <= kNow + 1) break;              // no room for a floor at all: refuse rather than wrap
							const size_t structural = (workersize >= kNow + 2) ? (workersize - kNow - 2) : 0;
							if (structural == 0) break;
							const size_t userCap = g_floorGrowCap->load(std::memory_order_relaxed);
							size_t cap;
							if (userCap) cap = userCap;
							else {
								cap = (workersize >= 2) ? (workersize - 2) : 0;
								if (cap < fbase) cap = fbase;
							}
							if (cap > structural) cap = structural;
							if (k >= cap) break;
							size_t want = k + (submitted ? submitted : 1);
							const size_t waveCap = (submitted ? submitted : 1) + fbase;
							if (want > waveCap) want = waveCap;
							if (want > cap)     want = cap;
							if (want < k) break;
							size_t f = want;
							// BandsSetF (TaskScheduler.h) -- ONE definition; this was a hand-copy of its body.
							// `workersize`, NOT `n`. This copy renamed the block's worker count to
							// de-shadow it, and these two uses were left behind -- so `n` bound to the
							// RECRUIT block's `size_t n = bodyNsHere / c`, a WAKE COUNT, and it
							// compiled because that outer n exists. BandsSetF takes the POOL SIZE and
							// clamps F to `n - k - 1`, so a wake count of 2 produced F=1 at K=0 and
							// F=0 at K=2 -- both measured, on both arms, as F below its own base with
							// N=31. The mirrored copy below kept its `const size_t n = workers->size()`
							// and never had the bug.
							const size_t prev = BandsSetF(f, workersize);

							if (want > prev)
								g_lastFloorGrowNs->store(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);

							for (size_t peak = g_awakeFloorPeak->load(std::memory_order_relaxed); want > peak; )
								if (g_awakeFloorPeak->compare_exchange_weak(peak, want, std::memory_order_relaxed)) break;

							if (want <= prev) break;
							const size_t kResv = BandField(BandsWord(), kBandK);
							for (size_t i = kResv + prev; i < kResv + want && i < workersize; ++i)
								if (Thread* w = (*workers)[i]) w->NotifyWorker(/*force*/ true);
						} while (0);   // closes the NoteFloorCrowding scope
					} while (0);   // closes the RecruitForLiveRange scope opened below the GrowFloor block
					}
				

			
				if (laneStartNs != 0)
					laneBusyNs.fetch_add(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() - laneStartNs, std::memory_order_relaxed);


				taskStartNs.store(0, std::memory_order_relaxed);

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
					{
						const size_t n = normalInboxes->size();
						const size_t k = BandField(BandsWord(), kBandK);     // ONE load, per your own header note
						size_t r = k + (n > k ? FastRand() % (n - k) : 0);   // draw directly from [k, n)
						(*normalInboxes)[r]->push(task_to_run);
						(*workers)[r]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
						(*workers)[r]->MarkQueuedWork();
						(*workers)[r]->NotifyWorker(/*force*/ true);
					}
					task_to_run = nullptr;
					continue;
				}
				task_to_run->assignedFiber = f;
				f->owningTask = task_to_run;
				f->homeWorker = (size_t)qIndex;
				f->Init(GlobalFiberPool::FiberEntryWrapper);
			}
			f->NoteCreditor((size_t)qIndex);

			f->status.store(FiberStatus::RUNNING, std::memory_order_release);
			f->homeCtx = &this->schedulerCtx;   // where the fiber returns to: THIS worker
			currentRunningTask = task_to_run;
			currentFiber = f;
			busy.store(true, std::memory_order_relaxed);
			{
				tsan::SwitchTo(f->tsanFiber);
				ContextSwitch(&this->schedulerCtx, &f->ctx);
			}
			busy.store(false, std::memory_order_relaxed);

			//OnFiberReturned(f, task_to_run);
			// NO SHADOW HERE. This line read `Task* task_to_run = task;` -- it declared a SECOND
			// task_to_run hiding the one the whole fiber path above just used (assignedFiber,
			// AcquireFiber, currentRunningTask) and bound it to `task` instead, which is null by
			// this point. The inlined OnFiberReturned body then completed the WRONG task: the
			// first thing it does is task_to_run->waitGroup, and that was the access violation.
			// The argument the helper wanted IS the enclosing task_to_run, so there is nothing to
			// bind -- the body simply uses it.
			FiberStatus fs = f->status.load(std::memory_order_acquire);
			if (fs == FiberStatus::DEAD) {
				// Completed for good
				if (task_to_run->waitGroup) {
					int old = task_to_run->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
					if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
						task_to_run->waitGroup->WakeAll();   // only touches wg if someone registered
				}
				task_to_run->assignedFiber = nullptr;
				// ---- THE FIBER IS DEAD: DOES IT OWE ANYONE? ------------------------------------------
				//
				// A fiber that incurred thread-affine state -- a COM apartment, a thread-owned handle, a
				// magazine that refuses to be remote-freed -- recorded each worker it owes in
				// Fiber::creditors. Those releases have to run ON those workers, so the fiber cannot simply
				// go back to this worker's cache: FiberRegistry walks the creditors one hop at a time and
				// the LAST hop recycles it. See FiberRegistry::AdvanceCleanup.
				//
				// THE COMMON PATH IS UNCHANGED, and that is deliberate. HasCreditors() is four relaxed
				// loads of a word this thread just finished running on, and it is false for every fiber
				// that never touched affine state -- which today is all of them, since nothing sets a
				// creditor yet. So this is inert until the resource wrappers exist, and when they do it
				// costs the ordinary fiber nothing.
				//
				// NOT ReleaseFiber IN THE OWING CASE: that pushes to this worker's LOCAL cache, and the
				// fiber must not be handed out again until its debts are paid. The chain's final hop
				// returns it to the global pool instead.
				// GATED ON WHAT IS OWED, NOT ON WHO RAN IT. This said HasCreditors() until registration at
				// pickup was wired -- at which point every fiber had a creditor, so every death dispatched
				// a cleanup task per worker to run an empty routine, and recycled through the global pool
				// instead of this worker's local cache. Being picked up is not a debt.
				//
				// So the common path is unchanged and stays unchanged: one relaxed load of a word this
				// thread just finished running on, then ReleaseFiber exactly as before.
#if !defined(NDEBUG) || defined(JLIB_DEVELOPMENT)
		// THE OTHER HALF OF THE ROW INVARIANT. Counted HERE, inside the DEAD branch, because this is
		// the only teardown a row has -- whether it goes home directly or through the creditor
		// chain, this is the point past which the row is no longer this task's. An acquire without
		// a matching arrival here is a stranded stack.
				fiberRecycles.fetch_add(1, std::memory_order_relaxed);
#endif
				// MEASURED HERE, BEFORE THE ROW GOES ANYWHERE. The fiber has switched back out and is DEAD,
				// so nothing is running on this stack -- but the next line may hand it to the creditor chain
				// or straight back to a cache, either of which can put another body on it before we look.
				//MeasureStackHighWaterIfProbing(f);
				// ---- MeasureStackHighWaterIfProbing(f) INLINED, IN ITS OWN SCOPE -------------------
				// do/while(0): these two early-outs meant "no measurement to take". As bare `return`s
				// they left Worker() -- and took the recycle, the ~Task and the Free below with them --
				// the FIRST time a fiber died with probing off, which is the default. A dead worker
				// and a leaked stack per fiber death.
				do {
					if (!f || !f->stackBase || !TaskScheduler::StackProbeEnabled()) break;
					const size_t page = platform::PageSize();
					if (f->stackSize <= page) break;
					const std::uint64_t* lo = (const std::uint64_t*)((char*)f->stackBase + page);
					const std::uint64_t* hi = (const std::uint64_t*)((char*)f->stackBase + f->stackSize);
					const std::uint64_t* p = lo;
					while (p < hi && *p == kStackPattern) ++p;
					// p now points at the deepest touched word (or hi if the body never ran / touched nothing).
					const size_t used = (size_t)((const char*)hi - (const char*)p);
					detail::NoteStackHighWater(f->stackClass, used);
				} while (0);
				if (f->OwesCleanup()) FiberRegistry::Instance().AdvanceCleanup(f);
				else                  ReleaseFiber(f);

				if (task_to_run)
					(void)task_to_run;
				if (!task_to_run->trivialDtor) task_to_run->~Task();
				// Free(), not FreeSized() -- see the Native path above.
				allocator->Free(task_to_run);

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
				// PARENS. Without them this is the FUNCTION'S ADDRESS, which is never null, so the
				// test was unconditionally true, `myResumed->push` below was unreachable, and Pin
				// mode never reached the resume inbox at all. MSVC says so (C4551, "function call
				// missing argument list") but it is a warning, not an error, so it built and ran.
				if (TaskScheduler::FibersMigrate())
				{
					const size_t n = normalInboxes->size();
					const size_t k = BandField(BandsWord(), kBandK);     // ONE load, per your own header note
					size_t r = k + (n > k ? FastRand() % (n - k) : 0);   // draw directly from [k, n)
					(*normalInboxes)[r]->push(task_to_run);
					(*workers)[r]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
					(*workers)[r]->MarkQueuedWork();
					(*workers)[r]->NotifyWorker(/*force*/ true);
				}
				else
					myResumed->push(task_to_run);
				// ---- THE ONE UNBOUNDED SOURCE, MARKED AT ITS PRODUCER --------------------------------
				//
				// A yield re-arms the resumed inbox, and the search checks that inbox BEFORE it drains the
				// loPri inbox -- so a fiber that yields in a loop makes the drain unreachable and starves
				// everything queued to this worker, permanently. Reproduced by
				// tests/yield_starvation_test.cpp: one worker, one yielding fiber, one queued task that
				// sat untouched for two seconds and ran the instant the fiber stopped.
				//
				// The ordering is not the bug -- a pinned resume has no other consumer, so checking it
				// first is what keeps a skipped resume from being a permanent hang. The bug is that the
				// ordering assumed resumed work was BOUNDED, and a yield loop is the one case that is not.
				//
				// MARKED HERE RATHER THAN DETECTED AT THE CONSUMER. The question the search needs -- "is
				// the next resumed task a yield or a genuine resume?" -- is free at this end and expensive
				// at the other: reading it there would mean peeking the head of a Vyukov MPSC without
				// advancing tail_, which is new lock-free code to answer something the producer already
				// knows. This is the same thread that will consume it, so it is a plain member write on a
				// line this worker already owns.
				//
				// GENUINE RESUMES NEVER SET IT. A resume arriving from Signal or Requeue keeps exactly the
				// priority it has today, so only the unbounded source is bounded.
				yieldedLastPass = true;
				hasQueuedWork.store(true, std::memory_order_seq_cst);
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
					if (TaskScheduler::FibersMigrate())
					{
						const size_t n = normalInboxes->size();
						const size_t k = BandField(BandsWord(), kBandK);     // ONE load, per your own header note
						size_t r = k + (n > k ? FastRand() % (n - k) : 0);   // draw directly from [k, n)
						(*normalInboxes)[r]->push(task_to_run);
						(*workers)[r]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
						(*workers)[r]->MarkQueuedWork();
						(*workers)[r]->NotifyWorker(/*force*/ true);
					}
					else
						myResumed->push(task_to_run);
				}
				currentFiber = nullptr;
				currentRunningTask = nullptr;
			}
			long long bodyNs = 0;
			if (busyStartNs != 0) {
				bodyNs = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() - busyStartNs;
				busyNs.fetch_add(bodyNs, std::memory_order_relaxed);
			}
			if (laneStartNs != 0)
				laneBusyNs.fetch_add(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() - laneStartNs, std::memory_order_relaxed);
			taskStartNs.store(0, std::memory_order_relaxed);   // no longer inside a task
			//GrowFloorIfLongBody(bodyNs);
			//RecruitForLiveRange(bodyNs);
			if (busyStartNs != 0) {
				const long long bodyNsHere = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() - busyStartNs;
				do {
					if (g_streakSpillMax->load(std::memory_order_relaxed) == 0u) break;   // growth disabled
					if (bodyNsHere <= kLongBodyNsThread) break;
					const size_t q = (size_t)qIndex;
					if (q < BandField(BandsWord(), kBandK)) break;                          // reserved: not our call
					if (q - BandField(BandsWord(), kBandK) >= BandField(BandsWord(), kBandFbase)) break;        // above the floor
					const size_t waiting = myDeque->size();
					if (waiting < 1) break;
					//TaskScheduler::NoteFloorCrowding(waiting);
					if (!(g_streakSpillMax->load(std::memory_order_relaxed) != 0u)) break;
					const size_t n = workers->size();
					if (n < 2) break;


					const uint64_t bw = BandsWord();
					const size_t   k = BandField(bw, kBandF);       // live F -- the value being grown
					const size_t   kNow = BandField(bw, kBandK);       // live K -- where the floor starts
					const size_t   fbase = BandField(bw, kBandFbase);   // policy base -- the wave cap's floor

					if (n <= kNow + 1) break;              // no room for a floor at all: refuse rather than wrap

					const size_t structural = (n >= kNow + 2) ? (n - kNow - 2) : 0;
					if (structural == 0) break;
					const size_t userCap = g_floorGrowCap->load(std::memory_order_relaxed);
					size_t cap;
					if (userCap) cap = userCap;
					else {
						cap = (n >= 2) ? (n - 2) : 0;
						if (cap < fbase) cap = fbase;
					}
					if (cap > structural) cap = structural;
					if (k >= cap) break;

					size_t wantone = k + (waiting ? waiting : 1);
					const size_t waveCap = (waiting ? waiting : 1) + fbase;
					if (wantone > waveCap) wantone = waveCap;
					if (wantone > cap)     wantone = cap;

					if (wantone < k) break;


					// BandsSetF (TaskScheduler.h) -- ONE definition; this was a hand-copy of its body.
					const size_t prev = BandsSetF(wantone, n);
					// ---- THE HOLD MEANS "SINCE THE FLOOR GREW", NOT "SINCE SOMEBODY ASKED" --------------------
					//
					// This stamped unconditionally, so every call pushed the collapse's 6 ms hold out -- including
					// calls that changed nothing. MEASURED over one bench run: 5,543,329 collapse attempts, of which
					// 5,541,706 (99.97%) were refused by the hold and only 74 ever shed. The floor was above base for
					// all but 1,470 of those attempts. A hold that is re-armed by the act of asking is not a hold,
					// it is a ratchet with a timer on it.
					if (wantone > prev)
						g_lastFloorGrowNs->store(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);

					// PEAK, BECAUSE THE CURRENT VALUE IS UNOBSERVABLE BY THE TIME ANYONE ASKS. The collapse fires
					// from an idle overflow worker the moment the wave drains, so a caller that measures a wave and
					// then reads GetAwakeFloor() reads the BASE -- it has already shed. That produced a burst line
					// claiming `1 -> 1` next to a 7.02 ms wall time, two numbers that cannot both be true: sixteen
					// 3.28 ms tasks on one worker is ~50 ms, and 7.02 ms is two waves. The banner was measuring the
					// shed, not the growth. A high-water mark is the only honest way to report a transient.
					for (size_t peak = g_awakeFloorPeak->load(std::memory_order_relaxed); wantone > peak; )
						if (g_awakeFloorPeak->compare_exchange_weak(peak, wantone, std::memory_order_relaxed)) break;

					// A promotion must WAKE what it promoted, or it is a number change and nothing else -- the bug
					// that made the dump read `awake bits = 2 (floor = 6)`.
					if (wantone <= prev) break;

					const size_t kResv = BandField(BandsWord(), kBandK);
					for (size_t i = kResv + prev; i < kResv + wantone && i < n; ++i)
						if (Thread* w = (*workers)[i]) w->NotifyWorker(/*force*/ true);


					// ---- RedistributeToOverflow INLINED, IN ITS OWN SCOPE ----------------------------
					// do/while(0) for two reasons: its k/baseF/liveF/n shadow names already live in the
					// enclosing block, and its early-out was a `return` from a helper -- which inlined
					// here would leave Worker() entirely rather than skip the redistribute.
					do {
						const size_t k = BandField(BandsWord(), kBandK);
						const size_t baseF = BandField(BandsWord(), kBandFbase);
						const size_t liveF = BandField(BandsWord(), kBandF);
						if (liveF <= baseF || (size_t)qIndex >= workers->size()) break;

						const size_t n = workers->size();
						const size_t span = liveF - baseF;

						for (size_t i = 0; i < waiting - 1; ++i) {
							// pop_bottom, because this IS the bottom's owner. The top stays available to thieves the
							// whole time, so this competes with stealing rather than blocking it -- if a thief already
							// took the task, pop_bottom simply comes back empty and there is nothing to hand out.
							auto opt = (*myDequeArray)[qIndex]->pop_bottom();
							if (!opt) break;
							Task* t = *opt;
							if (!t) break;

							const size_t target = k + baseF + (i % span);
							if (target >= n || !(*workers)[target]) {
								const size_t n = normalInboxes->size();
								const size_t k = BandField(BandsWord(), kBandK);     // ONE load, per your own header note
								size_t r = k + (n > k ? FastRand() % (n - k) : 0);   // draw directly from [k, n)
								(*normalInboxes)[r]->push(t);   // `t`, not task_to_run -- t is what pop_bottom just handed us
								(*workers)[r]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
								(*workers)[r]->MarkQueuedWork();
								(*workers)[r]->NotifyWorker(/*force*/ true);
								continue;
							}

							(*normalInboxes)[target]->push(t);
							(*workers)[target]->inboxDepth.fetch_add(1, std::memory_order_relaxed);
							// NoteInboxPush(1) INLINED. Its `return` meant "skip the depth accounting" -- as a `break`
							// it aborted the whole redistribute loop, and g_submitLimit defaults to 0 so it fired on
							// the FIRST iteration of every run. Worse, it broke BEFORE MarkQueuedWork and
							// NotifyWorker below, so the task just pushed sat in another worker's inbox with nobody
							// woken to drain it -- the exact stranding the force-wake comment below exists to stop.
							// The count is 1, not n: the argument was tasks pushed; `n` here is workers->size().
							if (g_submitLimit->load(std::memory_order_relaxed) != 0)
								g_inboxDepth->fetch_add(1, std::memory_order_relaxed);
							(*workers)[target]->MarkQueuedWork();
							// Force, because these indices are inside the grown floor and NotifyWorker's floor-skip
							// would otherwise drop the wake on the assumption that a floor worker is already looking.
							// It usually is -- growth just woke it -- but "usually" is how a task strands in an inbox
							// nobody else may drain.
							(*workers)[target]->NotifyWorker(/*force*/ true);
						}
					} while (0);
				} while (0);
				//	RecruitForLiveRange(bodyNsHere);
					// RecruitForLiveRange INLINED, IN ITS OWN SCOPE. Its early-outs were `return`s from a
					// helper; unwrapped here they were breaking the PASS LOOP, so a worker left Worker() the
					// first time a body was too short to pay for a wake -- the common case.
				do {
					if (!g_rangeRecruit->load(std::memory_order_relaxed)) break;

					const long long c = (long long)g_wakeCostNs->load(std::memory_order_relaxed);
					if (bodyNsHere <= c) break;                       // this leaf did not pay for a wake

					// SECOND, because it is the more expensive check -- a scan of the hint words against one
					// compare. Ordering matters: the overwhelmingly common caller is a task that is not range work
					// at all, and it should fall out on the arithmetic.
					size_t res = -1;
					for (size_t w = 0; w < TaskScheduler::kHintWords; ++w)
						if (stealHintParallel[w].load(std::memory_order_acquire)) { res = w; break; }
					if (res == -1) break;
					size_t n = (size_t)(bodyNsHere / c);
					const size_t sz = workers->size();
					if (n > sz) n = sz;                            // cannot use more than the pool
					if (!n) break;
					const unsigned r = (unsigned)nextWorker->fetch_add(1, std::memory_order_relaxed);
					size_t woken = 0;
					for (size_t i = 0; i < sz && woken < n; ++i) {
						const size_t idx = ((size_t)r + i) % sz;
						Thread* w = (*workers)[idx];
						if (w && w->GetWorkerState() == 2 /* WS_SLEEPING */) {
							w->NotifyWorker(/*force*/ true);
							++woken;
						}
					}
					size_t submitted = n;
					//TaskScheduler::NoteFloorCrowding(n);
					// ---- NoteFloorCrowding INLINED, IN ITS OWN SCOPE --------------------------------
					// do/while(0) because its `n` shadows the recruit block's, and its early-outs were
					// `return`s from a helper -- inlined here they would leave Worker() entirely.
					do {
						if (!(g_streakSpillMax->load(std::memory_order_relaxed) != 0u)) break;
						const size_t n = workers->size();
						if (n < 2) break;
						const uint64_t bw = BandsWord();
						const size_t   k = BandField(bw, kBandF);       // live F -- the value being grown
						const size_t   kNow = BandField(bw, kBandK);       // live K -- where the floor starts
						const size_t   fbase = BandField(bw, kBandFbase);   // policy base -- the wave cap's floor
						if (n <= kNow + 1) break;              // no room for a floor at all: refuse rather than wrap
						const size_t structural = (n >= kNow + 2) ? (n - kNow - 2) : 0;
						if (structural == 0) break;
						const size_t userCap = g_floorGrowCap->load(std::memory_order_relaxed);
						size_t cap;
						if (userCap) cap = userCap;
						else {
							cap = (n >= 2) ? (n - 2) : 0;
							if (cap < fbase) cap = fbase;
						}
						if (cap > structural) cap = structural;
						if (k >= cap) break;
						size_t want = k + (submitted ? submitted : 1);
						const size_t waveCap = (submitted ? submitted : 1) + fbase;
						if (want > waveCap) want = waveCap;
						if (want > cap)     want = cap;
						if (want < k) break;
						size_t f = want;
						// BandsSetF (TaskScheduler.h) -- ONE definition, shared with TaskScheduler.cpp.
						// This was a hand-copy of its body; four of them had to agree on F >= Fbase and
						// K + F <= N. inline in the header, so the codegen is the same as the copy was.
						const size_t prev = BandsSetF(f, n);

						if (want > prev)
							g_lastFloorGrowNs->store(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count(), std::memory_order_relaxed);

						for (size_t peak = g_awakeFloorPeak->load(std::memory_order_relaxed); want > peak; )
							if (g_awakeFloorPeak->compare_exchange_weak(peak, want, std::memory_order_relaxed)) break;

						if (want <= prev) break;
						const size_t kResv = BandField(BandsWord(), kBandK);
						for (size_t i = kResv + prev; i < kResv + want && i < n; ++i)
							if (Thread* w = (*workers)[i]) w->NotifyWorker(/*force*/ true);
					} while (0);
				} while (0);   // closes the RecruitForLiveRange scope opened below the GrowFloor block


			if (laneStartNs != 0)
				laneBusyNs.fetch_add(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() - laneStartNs, std::memory_order_relaxed);


			taskStartNs.store(0, std::memory_order_relaxed);

			task_to_run = nullptr;
			ready.store(true, std::memory_order_release);
			continue;
			}
			if ((++floorCtlTick & 0x7) == 0) {
	
				do {

				const size_t n = workers->size();
				if (n < 2) break;                        // nothing to move between

				const size_t lo = 1;                     // never fully park: the last worker is the landing site
				const size_t hi = n;
				const size_t k = BandField(BandsWord(), kBandF);
				const long long now = duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();

				// ---- PROMOTE: somebody had to be woken, so the floor was too small ----------------------
				// The window is rolled first so a stale count from a quiet second cannot fire a promotion now.
				const long long missWs = g_floorMissWindowNs->load(std::memory_order_relaxed);
				if (missWs == 0 || now - missWs >= TaskScheduler::kFloorMissWindowNs) {
					g_floorMissWindowNs->store(now, std::memory_order_relaxed);
					// The two counters this window used to roll are gone -- see the NotePush note above. The
					// window itself stays: the demote path below still uses it.
				}

				(void)hi;

				const long long ws = g_floorWindowNs->load(std::memory_order_relaxed);
				if (ws == 0) { g_floorWindowNs->store(now, std::memory_order_relaxed); break; }
				if (now - ws < TaskScheduler::kFloorWindowNs) break;
				g_floorWindowNs->store(now, std::memory_order_relaxed);

				const long long windowNs = now - ws;
				if (windowNs <= 0) break;

				// Drain every floor worker's counters, not just the marginal one -- leaving the others set would
				// let a stale count from several windows ago decide a later decision.
				unsigned  marginalRan = 0;
				long long marginalBusyNs = 0;
				long long minBusyNs = 0;
				for (size_t i = 0; i < k && i < n; ++i) {
					const unsigned  ran = (*workers)[i]->tasksRun.exchange(0, std::memory_order_relaxed);
					const long long busy = (*workers)[i]->busyNs.exchange(0, std::memory_order_relaxed);
					if (i == k - 1) marginalRan = ran;
					if (i == k - 1) marginalBusyNs = busy;
					if (i == 0 || busy < minBusyNs) minBusyNs = busy;
				}

				const bool marginalQuiet =
					(marginalRan == 0) || (marginalBusyNs * 100 < windowNs * (long long)TaskScheduler::kQuietBusyPct);

				// RESET ON ANY ACTIVITY. This is the ratchet rule: keyed off "IS idle", never "became idle".
				if (!marginalQuiet) { g_quietWindows->store(0, std::memory_order_relaxed); break; }

				(void)lo;
				} while (0);   // closes the MaybeAdjustAwakeFloor scope
			}
			task_to_run = nullptr;
		}
		{
			if (isReservedWorker && !myInbox->quiescent()) {
				static std::atomic<unsigned> strayWarned{ 0 };
				if (strayWarned.fetch_add(1, std::memory_order_relaxed) == 0)
					std::fprintf(stderr,
						"[JLib::Scheduler] ordinary work in the loPri inbox of RESERVED worker %d"
						" (K=%zu). K never drains loPri, so this task will not run. This is a"
						" placement bug -- find the writer; there is no longer a net.\n",
						qIndex, BandField(BandsWord(), kBandK));
			}

			if (!task_to_run && isReservedWorker) {
				Task* shared;
				if (laneIntake->try_dequeue(shared)) {
					task_to_run = shared;
					laneSourced = true;
				}
				
			}

			if (!task_to_run) {
				JLIBSCHED_PHASE(qIndex, Lane);
				Task* hp = nullptr;
				if (myLane->pop(hp) && hp) {
					task_to_run = hp;
					JLIBSCHED_STEAL_STAT(qIndex, fromHiInbox);
					laneSourced = true;   // K controller input -- see laneSourced

					inboxDepth.fetch_sub(1, std::memory_order_relaxed);

					if (!myLane->empty()) {
						g_laneStrandEvents->fetch_add(1, std::memory_order_relaxed);


						const size_t n = workers->size();
						const TaskScheduler::Bands  b = TaskScheduler::GetBands();
						const size_t kEnd = (b.k < n) ? b.k : n;
						const size_t kfEnd = ((b.k + b.f) < n) ? (b.k + b.f) : n;

						// ONE SWEEP, TWO ANSWERS. [0, kEnd) is a prefix of [0, kfEnd), so a hit below kEnd counts for
						// both and the loop can stop at the first idle worker it finds in each range -- but it must not
						// stop at the first hit overall, because a hit in the floor says nothing about K.
						bool idleInK = false, idleInKF = false;
						for (size_t q = 0; q < kfEnd; ++q) {
							if (q == qIndex || !(*workers)[q]) continue;
							if ((*workers)[q]->busy.load(std::memory_order_relaxed)) continue;
							idleInKF = true;
							if (q < kEnd) { idleInK = true; break; }   // a K hit implies the KF hit; nothing left to learn
						}
						if (idleInK)  g_laneStrandIdleK->fetch_add(1, std::memory_order_relaxed);
						if (idleInKF) g_laneStrandIdleKF->fetch_add(1, std::memory_order_relaxed);
					}
					continue;
				}

				if (myLane->empty()) {
					if (qIndex < 64)
						stealHintLane->fetch_and(~(1ull << qIndex), std::memory_order_relaxed);

				}
			}

			if (yieldedLastPass) {
				yieldedLastPass = false;

			if (!isReservedWorker && !myInbox->quiescent()) {
					Task* fromInbox = nullptr;
					if (myInbox->pop(fromInbox) && fromInbox) {
						inboxDepth.fetch_sub(1, std::memory_order_relaxed);
						task_to_run = fromInbox;
						JLIBSCHED_STEAL_STAT(qIndex, fromLoInbox);
						continue;
					}
				} 

				if (auto opt = myDeque->pop_bottom()) {
					if (Task* t = *opt) { task_to_run = t; continue; }
				}
			}

			if (!task_to_run && !scheduler->FibersMigrate()) {
				JLIBSCHED_PHASE(qIndex, Resumed);
				Task* resumed = nullptr;
				if (myResumed->pop(resumed) && resumed) {
					task_to_run = resumed;
					JLIBSCHED_STEAL_STAT(qIndex, fromResumed);
					laneSourced = true;
					continue;
				}
			}
			if (!task_to_run && !isReservedWorker) {
				auto opt = myDeque->pop_bottom();
				if (opt) {
					Task* task = *opt;
					if (!task) {
						std::cerr << "[worker " << qIndex << "] Null task from pop_bottom!" << std::endl;
					}
					else {
						task_to_run = task;
						JLIBSCHED_STEAL_STAT(qIndex, fromDeque);
						continue;
					}
				}
			}
		}   // closes the work-search block opened above the stray-work check
		
		if (!task_to_run && !isReservedWorker) {
			JLIBSCHED_PHASE(qIndex, InboxDrain);
			do {
				size_t count = 0;
				while (count < BATCH_SIZE && myInbox->pop(batch[count]))
					++count;

				if (g_submitLimit->load(std::memory_order_relaxed) != 0 && count != 0)
					g_inboxDepth->fetch_sub(count, std::memory_order_relaxed);
				if (myInbox->empty())
					inboxDepth.store(0, std::memory_order_relaxed);
				else if (count)
					inboxDepth.fetch_sub((int)count, std::memory_order_relaxed);
				if (count == 0) break;

				const int keep = (int)count - 1;
				if (keep == 0 || myDeque->push_bottom_batch(batch, (size_t)keep)) {
					for (int s = 0; s < keep; ++s) JLIBSCHED_STEAL_STAT(qIndex, stagedFromInbox);
					JLIBSCHED_STEAL_STAT(qIndex, ranDirect);
					task_to_run = batch[count - 1];
					JLIBSCHED_LATENCY_MARK(Found);
					break;
				}
			} while (0);
		}
		{
			if (!task_to_run) {
				JLIBSCHED_PHASE(qIndex, StealScan);
				JLIBSCHED_LATENCY_MARK(PreSteal);


				auto tryStealFrom = [&](int target) -> bool {
					if (!scheduler->MaybeStealable((std::size_t)target)) return false;
				
					JLIBSCHED_STEAL_STAT(qIndex, probes);

					// INLINED IoLaneQuiet(), AND ITS EARLY RETURNS ARE LOAD-BEARING. The helper is
					// three answers, not one expression: "stealing off" is a HARD no that the later
					// two lines must not reach, and "never pushed" is a hard yes that must not fall
					// through to the window compare against last == 0. Written as straight-line
					// assignments each one overwrites the one above it -- which is how
					// SetReservedStealing(false) became inert and reserved workers stole ordinary
					// work in a test that had switched stealing off. Keep the else-chain.
					//
					// AND ONLY A RESERVED WORKER ASKS. res is consumed one line below under
					// isReservedWorker and nowhere else, so computing it unconditionally put a
					// steady_clock::now() on every ordinary steal probe for a value that was thrown
					// away. The clock read belongs inside the branch that reads it.
					// THE PUBLISH LANE NEEDS NO PERMISSION, and that is the point of it. Everything
					// below decides whether a reserved worker may go take work that BELONGS to
					// another worker -- the quiet window is a courtesy to the owner it is taken
					// from. Work on the publish lane is owned by nobody, so K takes it on the same
					// footing as any other thief. This is what stops the reserved band from idling
					// two good cores next to a full lane.
					const bool fromPublishLane =
						TaskScheduler::MainPublishDeque()
						&& (size_t)target == scheduler->MainPublishLane()
						&& scheduler->MainPublishLane() != 0;

					if (isReservedWorker && !fromPublishLane) {
						bool res;
						if (!g_reservedSteal->load(std::memory_order_relaxed)) {
							res = false;   // stealing off: never quiet
						} else {
							const long long last = g_ioLastPushNs->load(std::memory_order_relaxed);
							if (last == 0) {
								res = true;   // never pushed means never protected
							} else {
								const long long win = (long long)g_ioQuietUs->load(std::memory_order_relaxed) * 1000ll;
								res = (duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count() - last) > win;
							}
						}
						if (!res) return false;
					}
					auto s = (*myDequeArray)[target]->steal();
					if (!s) return false;

					JLIBSCHED_STEAL_STAT(qIndex, hits);
					JLIBSCHED_STEAL_STAT(qIndex, fromSteal);
					task_to_run = *s;
					return true;
					};

				const size_t nq0 = myDequeArray->size();
				const bool   bitsUsable = nq0 <= TaskScheduler::kMaxHintQueues;
				size_t       nWords = bitsUsable ? ((nq0 + 63) / 64) : 0;
				if (nWords > TaskScheduler::kHintWords) nWords = TaskScheduler::kHintWords;

				unsigned long long bitsW[TaskScheduler::kHintWords] = {};
				unsigned long long bitsAny = 0;

				for (size_t w = 0; w < nWords; ++w) {
					const unsigned long long loBits = stealHintBacklog[w].load(std::memory_order_acquire)
						| stealHintParallel[w].load(std::memory_order_acquire);
					const unsigned long long laneBits = (w == 0) ? stealHintLane->load(std::memory_order_acquire) : 0ull;

					bitsW[w] = loBits | laneBits;
					bitsAny |= bitsW[w];
					advertisedCount += platform::PopCount64(loBits)
						+ platform::PopCount64(laneBits);
				}

				// ---- THE MAIN PUBLISH LANE ADVERTISES ITSELF, BY BEING READ -----------------
				//
				// Every bit above is maintained by a thread running a pass loop: a worker sets its
				// own and clears its own. The publish lane has no such thread. Main sets a bit at
				// push time and then never learns when the lane drained, so the clear would have to
				// come from the thieves -- and that pair has a losing race in the direction that
				// matters. A lost CLEAR costs one wasted probe. A lost SET parks the pool on live
				// work, which is the failure this whole hint word feeds (advertisedCount == 0 is
				// the park gate).
				//
				// So read the queue instead of trusting a bit about it. One relaxed size compare
				// per steal scan, on a line only this lane's producer writes, and it is right by
				// construction -- there is no window in which the lane holds work and does not say
				// so. Same rule as the park predicates: READ THE QUEUE, NOT THE HINT.
				if (TaskScheduler::MainPublishDeque() && bitsUsable) {
					const size_t pub = scheduler->MainPublishLane();
					if (pub != 0 && pub < nq0 && pub < TaskScheduler::kMaxHintQueues
					    && !(*myDequeArray)[pub]->empty()) {
						const size_t     pw  = pub >> 6;
						const unsigned long long pb = 1ull << (pub & 63);
						if (pw < nWords && !(bitsW[pw] & pb)) {
							bitsW[pw] |= pb;
							bitsAny   |= pb;
							++advertisedCount;
						}
					}
				}


				auto advertised = [&](int t) -> bool {
					if (!bitsUsable) return true;
					const size_t ut = (size_t)t;
					if (ut >= TaskScheduler::kMaxHintQueues) return true;
					return ((bitsW[ut >> 6] >> (ut & 63)) & 1ull) != 0;
					};


				if (!bitsUsable || bitsAny != 0) {
					const int lim = (int)nq0;


					const size_t probeLimit = (consecutiveMisses < kBackoffMissThreshold)
						? (size_t)lim : (size_t)1;
					size_t probed = 0;


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


					const auto ownWorkArrived = [&]() -> bool {
						return hasQueuedWork.load(std::memory_order_relaxed);
						};
					const std::vector<int>& mates = (*clusterMates)[qIndex];
					if (!mates.empty()) {
						const size_t ms = FastRand() % mates.size();
						for (size_t i = 0; i < mates.size() && !task_to_run && probed < probeLimit
							&& !ownWorkArrived(); ++i)
							probeOnce(mates[(ms + i) % mates.size()]);
					}

					// PHASE 2 -- every other advertised victim, including the non-worker lane and
					// anything outside this LLC cluster. Rotating start for the same reason.
					if (!task_to_run && lim > 0 && !ownWorkArrived()) {
						const int start = (int)(FastRand() % (unsigned)lim);
						for (int i = 0; i < lim && !task_to_run && probed < probeLimit
							&& !ownWorkArrived(); ++i)
							probeOnce((start + i) % lim);
					}
				}

				if (task_to_run) {
					consecutiveMisses = 0;
					continue;
				}
				else {


					++consecutiveMisses;
				}
			}
		}



	
		if (!task_to_run && !isReservedWorker) {
			JLIBSCHED_PHASE(qIndex, InboxDrain);
		
			do {
				size_t count = 0;
				while (count < BATCH_SIZE && myInbox->pop(batch[count]))
					++count;
				
				if (g_submitLimit->load(std::memory_order_relaxed) != 0 && count != 0)
					g_inboxDepth->fetch_sub(count, std::memory_order_relaxed);
				if (myInbox->empty())
					inboxDepth.store(0, std::memory_order_relaxed);
				else if (count)
					inboxDepth.fetch_sub((int)count, std::memory_order_relaxed);
				if (count == 0) break;

				const int keep = (int)count - 1;
				if (keep == 0 || myDeque->push_bottom_batch(batch, (size_t)keep)) {
					for (int s = 0; s < keep; ++s) JLIBSCHED_STEAL_STAT(qIndex, stagedFromInbox);
					JLIBSCHED_STEAL_STAT(qIndex, ranDirect);
					task_to_run = batch[count - 1];
					JLIBSCHED_LATENCY_MARK(Found);
					break;
				}
			} while (0);
		}
		if (task_to_run) {
			continue;
		}
		else {

			JLIBSCHED_PHASE(qIndex, ParkGate);

			if (!running.load(std::memory_order_acquire)
				|| (!scheduler->paused.load(std::memory_order_seq_cst)
					&& (hasQueuedWork.load(std::memory_order_seq_cst)
						|| laneWake.load(std::memory_order_seq_cst)
						|| !myLane->quiescent()
						|| (!isReservedWorker && !myInbox->quiescent())
						|| !myResumed->quiescent()
						|| FiberRegistry::Instance().HolderHasWork((size_t)qIndex)
						|| (isReservedWorker && !TaskScheduler::LaneIntakeIdle())))) {
				JLIBSCHED_LATENCY_MARK(Wake);
				if (!running.load(std::memory_order_acquire)) break;

				platform::CpuRelax();
				continue;   // work landed while deciding: go search for it instead of parking
			}
			{

				if (!running.load(std::memory_order_acquire)) {
					break;
				}

				// ONE WORD, READ BEFORE THE COLLAPSE, USED FOR BOTH DECISIONS BELOW.
				//
				// The gate must NOT re-read F after the collapse. CollapseAwakeFloorToBase writes
				// F <- Fbase, and its contract is that it parks nobody: "a worker parks through its
				// own idle path... next pass". Re-reading here makes every index in
				// [K+Fbase, K+F_old) fail the membership test and park IN THE SAME PASS that shed
				// them -- it deletes that pass. That was my change and it was wrong.
				//
				// One load also keeps the arm SELECTOR and the arm BOUNDS on the same word. Using a
				// stale selector with fresh bounds was the other half of the same mistake: with K
				// growing 2 -> 4, a worker at index 2 is reserved by the new word but still takes
				// the floor arm, asks `2 >= 4`, gets false, and walks into WS_PARKED -- with
				// ReservedNeverParks() guarding only the arm it can no longer reach.
				const uint64_t gateWord = BandsWord();
				const size_t   gateK    = BandField(gateWord, kBandK);
				const size_t   gateF    = BandField(gateWord, kBandF);

				if (advertisedCount == 0
					&& (size_t)qIndex >= gateK
					&& (size_t)qIndex < gateK + gateF)
					TaskScheduler::CollapseAwakeFloorToBase();

				const size_t floorBase = gateK;
				const size_t floorNow = gateF;

				const bool onAwakeFloor =
					((size_t)qIndex < gateK)
					? (TaskScheduler::ReservedNeverParks()
						|| BandField(gateWord, kBandKmax) > BandField(gateWord, kBandKmin))
					: ((size_t)qIndex >= floorBase
						&& (size_t)qIndex < floorBase + floorNow);


				if (advertisedCount == 0 && !onAwakeFloor) {

					{
						const TaskScheduler::Bands pb = TaskScheduler::GetBands();  // ONE load
						if ((size_t)qIndex >= pb.k && (size_t)qIndex < pb.k + pb.f) {
							// Never advertised an intent, so there is nothing to retract here.
							continue;
						}
					}
					if (awakeWord) awakeWord->fetch_and(~awakeMask, std::memory_order_relaxed);

					{
						int e = WS_NOTIFIED;
						if (workerState.compare_exchange_strong(e, WS_EMPTY,
							std::memory_order_seq_cst, std::memory_order_relaxed)) {
							if (awakeWord) awakeWord->fetch_or(awakeMask, std::memory_order_relaxed);
							continue;
						}
					}


					int expectedEmpty = WS_EMPTY;
					if (!workerState.compare_exchange_strong(expectedEmpty, WS_PARKED,
						std::memory_order_seq_cst, std::memory_order_relaxed)) {

						if (expectedEmpty == WS_NOTIFIED) {
							int e2 = WS_NOTIFIED;
							workerState.compare_exchange_strong(e2, WS_EMPTY,
								std::memory_order_seq_cst, std::memory_order_relaxed);
						}
						if (awakeWord) awakeWord->fetch_or(awakeMask, std::memory_order_relaxed);
						continue;
					}
					int sleeping = WS_PARKED;

					JLIBSCHED_PHASE(qIndex, Parked);
					// HOISTED OUT OF THE SCOPE IT USED TO LIVE IN, so lastParkBands below can record
					// the SAME judgement the gate made. It must be GetBands() and not BandsWord():
					// GetBands CLAMPS (K+F <= N, and the N==1 rewrite), so the raw word can say one
					// thing while the gate acts on another -- which is one of the two candidates the
					// table exists to tell apart.
					const TaskScheduler::Bands db = TaskScheduler::GetBands();
					{
						if ((size_t)qIndex >= db.k && (size_t)qIndex < db.k + db.f) {
							TaskScheduler::NoteFloorPark();   // count the catch -- the bench reports it
							{
								int held = WS_PARKED;
								if (!workerState.compare_exchange_strong(held, WS_EMPTY,
									std::memory_order_seq_cst, std::memory_order_relaxed)) {
									int got = WS_NOTIFIED;
									workerState.compare_exchange_strong(got, WS_EMPTY,
										std::memory_order_seq_cst, std::memory_order_relaxed);
								}
							}
							if (awakeWord) awakeWord->fetch_or(awakeMask, std::memory_order_relaxed);
							continue;
						}
					}

					// COUNTED HERE, BELOW THE LAST GATE, BECAUSE ONLY NOW IS THE PARK REAL. This sat
					// above the db re-check, so a worker the gate CAUGHT and turned around had already
					// incremented it -- and the bench bins parkCount by band, so every successful catch
					// on a floor index also printed as a floor park and tripped the JUNK RUN banner.
					// The signature was the two numbers agreeing: `floor 12` beside `CAUGHT 13`.
					// A genuine violation is a floor park with NO matching catch, and that is what this
					// ordering makes visible again. Three earlier `continue`s back out above this too;
					// none of them should count either, and now none do.
					// The band the gate JUST judged, packed K<<32 | F, so the end-of-run table can ask
					// "was q inside [K, K+F) when it slept" rather than "is it now".
					// fbase<<48 | nw<<40 | k<<32 | f. Fbase is the discriminator: F=0 beside base=0
					// is legal and points at whoever moved the base; F=0 beside base=2 means a
					// writer bypassed the `want = max(f, fb)` guard, and BandsCasF is the only one
					// without it.
					lastParkBands.store(((std::uint64_t)db.fbase << 48)
					                    | ((std::uint64_t)db.nw << 40)
					                    | ((std::uint64_t)db.k << 32)
					                    | (std::uint64_t)db.f,
					                    std::memory_order_relaxed);
					parkCount.fetch_add(1, std::memory_order_relaxed);

					assert(currentFiber == nullptr &&
						"worker about to park while still owning a fiber -- that frame can never "
						"be resumed by anyone");
					assert(currentRunningTask == nullptr &&
						"worker about to park while still holding a running task");


					if (TaskScheduler::GetParkPrimitive() == TaskScheduler::ParkPrimitive::CondVar) {
						std::unique_lock<std::mutex> lk(parkMx);
						parkCv.wait(lk, [&] {
							return workerState.load(std::memory_order_seq_cst) != WS_PARKED
								|| !running.load(std::memory_order_acquire)
								|| hasQueuedWork.load(std::memory_order_seq_cst)
								|| laneWake.load(std::memory_order_seq_cst)
								|| !myLane->quiescent()
								|| (!isReservedWorker
									&& !myInbox->quiescent())
								|| !myResumed->quiescent()
								|| FiberRegistry::Instance().HolderHasWork((size_t)qIndex)
								|| (isReservedWorker && !TaskScheduler::LaneIntakeIdle());
							});
					}
					else
						while (workerState.load(std::memory_order_seq_cst) == WS_PARKED

							&& running.load(std::memory_order_acquire)
							&& !hasQueuedWork.load(std::memory_order_seq_cst)
							&& !laneWake.load(std::memory_order_seq_cst)
							&& myLane->quiescent()
							&& (isReservedWorker
								|| myInbox->quiescent())
							&& myResumed->quiescent()
							&& !FiberRegistry::Instance().HolderHasWork((size_t)qIndex)
							&& (!isReservedWorker || TaskScheduler::LaneIntakeIdle())) {
#if defined(JLIB_PLATFORM_WINDOWS)
							::WaitOnAddress(&workerState, &sleeping, sizeof(int), INFINITE);
#elif JLIB_PLATFORM_LINUX
							FutexWait(&workerState, sleeping);
#else
							break;
#endif
						}
					{
						int got = WS_NOTIFIED;
						workerState.compare_exchange_strong(got, WS_EMPTY,
							std::memory_order_seq_cst, std::memory_order_relaxed);
					}
					if (awakeWord) awakeWord->fetch_or(awakeMask, std::memory_order_relaxed);
					JLIBSCHED_LATENCY_MARK(Wake);
					if (!running.load(std::memory_order_acquire)) break;
					continue;
				}
				auto yieldWithHandshake = [&]() -> bool {
					int e = WS_EMPTY;
					if (!workerState.compare_exchange_strong(e, WS_YIELD,
						std::memory_order_seq_cst, std::memory_order_relaxed)) {
						if (e == WS_NOTIFIED) {
							int e2 = WS_NOTIFIED;
							workerState.compare_exchange_strong(e2, WS_EMPTY,
								std::memory_order_seq_cst, std::memory_order_relaxed);
						}
						return true;
					}
					std::this_thread::yield();

					int back = WS_YIELD;
					if (!workerState.compare_exchange_strong(back, WS_EMPTY,
						std::memory_order_seq_cst, std::memory_order_relaxed)) {
						int e3 = WS_NOTIFIED;
						workerState.compare_exchange_strong(e3, WS_EMPTY,
							std::memory_order_seq_cst, std::memory_order_relaxed);
					}
					return false;
					}; 

				dbgOnAwakeFloor.store(onAwakeFloor, std::memory_order_relaxed);

				dbgSpinTick.store(++idlePasses, std::memory_order_relaxed);

				if (onAwakeFloor) {
					JLIBSCHED_PHASE(qIndex, FloorSpin);

				if (isReservedWorker) {
						const size_t liveFK = TaskScheduler::GetAwakeFloor();
						const unsigned tick = ++spinTick;
						if (liveFK > TaskScheduler::GetYieldFloorMin() &&
							(((tick + (unsigned)qIndex) & TaskScheduler::GetSpinYieldMask()) == 0)) {
							TaskScheduler::NoteReservedYieldSuppressed();
						}
						const unsigned rm = TaskScheduler::GetReservedYieldMask();
						if (rm != TaskScheduler::kReservedYieldNever &&
							(((tick + (unsigned)qIndex) & rm) == 0)) {
							if (yieldWithHandshake()) continue;
						}
						else platform::CpuRelax();
						continue;
					}
					

					const size_t liveF = TaskScheduler::GetAwakeFloor();
					if (liveF > TaskScheduler::GetYieldFloorMin()) {
						if ((((++spinTick) + (unsigned)qIndex) & TaskScheduler::GetSpinYieldMask()) == 0) {
							if (yieldWithHandshake()) continue;
						}
						else platform::CpuRelax();
					}
					else platform::CpuRelax();
					continue;
				} 
				{
					const unsigned relaxN = TaskScheduler::GetWorkerRelax();
					for (unsigned i = 0; i < relaxN; ++i) platform::CpuRelax();
				}
				{
					const unsigned gm = TaskScheduler::GetGuestYieldMask();
					if (gm != TaskScheduler::kGuestYieldNever &&
						((((++spinTick) + (unsigned)qIndex) & gm) == 0)) {
						(void)yieldWithHandshake();
					}
				}
			}
		}
	}
	
	running.store(false, std::memory_order_release);
}


namespace JLib {
// RESTORED. This definition lived in a trailing `namespace JLib { ... }` block that went out with
// the worker-loop flattening -- the declaration in Thread.h stayed, so nothing in the library or the
// benches noticed. SchedulerThreadLayoutTest is the only consumer, and it fails at LINK, which is
// why a green bench run said nothing about it. See the declaration in Thread.h for why it exists.
//
// offsetof on a non-standard-layout type is conditionally supported; MSVC, GCC and Clang all accept
// it, and it is the only way to ask this without hand-computing the layout.
void Thread::PushPathFieldOffsets(size_t& inboxDepthOff,
                                  size_t& hasQueuedWorkOff,
                                  size_t& workerStateOff) noexcept {
	inboxDepthOff    = offsetof(Thread, inboxDepth);
	hasQueuedWorkOff = offsetof(Thread, hasQueuedWork);
	workerStateOff   = offsetof(Thread, workerState);
}
}
