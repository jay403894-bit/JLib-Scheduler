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
                      : p == WorkerPrio::High     ? THREAD_PRIORITY_HIGHEST
                                                  : THREAD_PRIORITY_NORMAL);

#elif JLIB_PLATFORM_DARWIN
    // QoS, NOT SCHED_FIFO and NOT affinity. Apple Silicon has no thread affinity API at all, and QoS
    // is the documented lever -- it is also what actually steers P vs E core selection there, which
    // is the same decision Windows makes off priority. USER_INITIATED is the middle tier: work the
    // user is waiting on, but not the interactive one.
    (void)pthread_set_qos_class_self_np(
        p == WorkerPrio::Critical ? QOS_CLASS_USER_INTERACTIVE
      : p == WorkerPrio::High     ? QOS_CLASS_USER_INITIATED
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
	drain(scheduler->loPriInboxes[qIndex].get(), scheduler->deques[qIndex].get(), false);
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
// K CONCENTRATES IT. At K=1 every hiPri push lands on one worker, so one blocked reserved worker
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
	if ((size_t)qIndex >= scheduler->hiPriInboxes.size()) return nullptr;

	Task* t = nullptr;
	if (!scheduler->hiPriInboxes[qIndex]->pop(t) || !t) {
		if (scheduler->hiPriInboxes[qIndex]->empty())
			scheduler->ClearHiPriHint((size_t)qIndex);
		return nullptr;
	}

	if (t->type == TaskType::Fiber) {
		if (!scheduler->deques[qIndex]->push_bottom(t)) TaskDeque::FatalPushRefused();
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
	if (!force && workerState.load(std::memory_order_seq_cst) == WS_AWAKE) return;

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
	if (TaskScheduler::GetParkPrimitive() == TaskScheduler::ParkPrimitive::CondVar) {
		{
			std::lock_guard<std::mutex> lk(parkMx);
			workerState.store(WS_AWAKE, std::memory_order_seq_cst);
		}
		// Counted on the same footing as the WaitOnAddress arm so the latency row's wake number
		// stays comparable across the A/B. Both count "pushes that had to reach the kernel".
		TaskScheduler::NoteWakeCall();
		parkCv.notify_one();
		return;
	}

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
	// REACHING HERE ON DARWIN MEANS JLIB_PARK=wait, and then the store above IS the entire
	// notification -- there is no address to signal, so the worker is spinning and will see it on
	// its next pass. A default Darwin build never gets here: ParkPrimitiveDefault() selects CondVar
	// and the branch at the top of this function returns before the platform arms, so the floor is
	// a real feature on all three platforms rather than advisory on one.
	//
	// __ulock_wait would be the direct Darwin analogue of the other two arms, and it stays off the
	// table while it is private API. The condvar is not a placeholder for it -- it is the answer.
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
	const size_t waiting = scheduler->deques[qIndex]->size();
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

void Thread::Worker() {
	// Is THIS worker reserved for hiPri? Workers [0, R) run hiPri ONLY -- ordinary placement skips
	// "Am I in the reserved band [0, K)?" USED TO BE CACHED HERE, for the whole life of the thread,
	// on the grounds that K is static. It is now read once per pass at the top of the loop below --
	// see the note there. A cached answer is the same bug as a site spelling the band differently,
	// only harder to see, because it is a stale bool rather than a wrong formula.

	// WHAT PRIORITY THIS THREAD IS CURRENTLY AT. Thread-lifetime, not per pass: it exists to
	// SUPPRESS REDUNDANT SYSCALLS, so it must remember across passes what was last asked for.
	// It describes the request, not the kernel -- POSIX may refuse an elevation, and that is the
	// unprivileged answer rather than a state to track.
	WorkerPrio curPrio = WorkerPrio::Normal;
	unsigned spinTick = 0;   // idle-pass counter, used only to rate-limit the yield below
	// How long a NON-floor worker pauses before handing the core back. Short on purpose: it is
	// awake on a transient reason, so the pause only has to cover a steal target appearing, not a
	// policy decision. See the spin path in Worker().
	constexpr unsigned kGuestSpin = 32;
	unsigned floorCtlTick = 0;   // subsamples the awake-floor controller; see MaybeAdjustAwakeFloor
	unsigned laneCtlTick  = 0;   // subsamples the K controller from a reserved worker; see below
	running.store(true, std::memory_order_release);
	const size_t BATCH_SIZE = 64;
	Task* batch[BATCH_SIZE];
	static thread_local Task* task_to_run = nullptr;
	// ---- DID THE TASK ABOUT TO RUN COME OFF THE LANE? ------------------------------------------
	//
	// The K controller's only honest input. Set at the two lane pickups (hiPri inbox, resume inbox)
	// and cleared the moment the task is dispatched, so it describes exactly one task. Both pickups
	// `continue` straight to the dispatch at the top of the loop, so there is no path that sets it
	// and then runs something else.
	//
	// Thread-local for the same reason task_to_run is: it has to survive that continue.
	static thread_local bool laneSourced = false;
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
	// SEEDED FALSE, NOT FROM A PRE-LOOP GetHotWorkers(). It used to read K here, once, for the life
	// of the thread -- a fourth spelling of K that could never track a change. It does not need one:
	// the first pass sets this true if the worker is hot (see `if (isHotWorker)` below), so seeding
	// false is self-healing and costs exactly one pass of not owning a bit nobody has raised yet.
	bool ownsLaneBit = false;
	// Did this worker execute a LANE task on the previous pass? The occupancy numerator. A worker
	// busy bit is the wrong thing here: general help and pure spin must read as not-lane-busy, or
	// demotion never fires.
	// Latch for the hazard-bag drain below: set when this worker scans on going idle, cleared the
	// moment it actually runs something again. Without it the scan runs every spin iteration.
	bool scannedSinceWork = false;
	// EXCLUSIVE MODE: an ORDINARY worker gets off the hot cores. The hot workers themselves are
	// already pinned to them by StartWorker.
	//
	// MOVED INTO THE LOOP, latched -- see `hotCpuExcluded` at the first pass. It read
	// GetHotWorkers() here, which is both a fifth spelling of K and a value fixed for the life of
	// the thread: a worker that becomes ordinary after a K change stayed on the hot cores forever.
	// Deciding it from the pass snapshot means it follows K the one time it can.
	//
	// IT IS STILL ONE-WAY, and that is a property of the API rather than a choice made here: there
	// is no ExcludeCurrentThreadFrom... inverse to re-admit a thread that becomes reserved later.
	// So this applies the exclusion when the worker is ordinary and never undoes it. Documented
	// rather than hidden, because the alternative -- re-pinning on every band change -- is affinity
	// churn on a path that runs per pass.
	bool hotCpuExcluded = false;
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

		// ---- AM I RESERVED? ASKED ONCE PER PASS, FROM THE LIVE K ------------------------------
		//
		// PASS-SCOPED FOR TWO REASONS, and only the first is about adaptive K.
		//
		// ONE READ, so every decision in this pass agrees. Five independent reads spread through the
		// loop body would let K move between them, and this worker would then drain the ordinary
		// inbox as a compute worker and park as a reserved one in the SAME iteration. A consistent
		// pair per pass is the whole point; five fresh reads is not more correct, it is less.
		//
		// NOT HOISTED OUT OF THE LOOP, which is where it lived until now, cached for the life of the
		// thread with a comment saying "K is STATIC so there is nothing to re-read". That is true
		// today and is exactly the assumption adaptive K removes -- and it fails SILENTLY: a worker
		// that becomes reserved keeps taking bulk work forever, and one that stops being reserved
		// refuses ordinary work forever, with no crash and nothing in a dump to point here. Its five
		// consumers are the stray-drain, the loPri pop, the steal refusal, the inbox drain gate and
		// the park decision, so a stale answer is wrong in five different ways at once.
		// ---- THE ONE SNAPSHOT -----------------------------------------------------------------
		//
		// Everything this pass decides about bands reads THIS value and no other: reserved, floor
		// membership, OS priority, the collapse call site, and the park gate.
		//
		// It was two loads -- GetHotWorkers() for K here, a separate GetBands() for (K, F) below.
		// They return the same fields, but not necessarily from the same INSTANT: K can move
		// between them, and then this worker answers "am I reserved" against one K and "am I on the
		// floor" against another. That is the torn pair, and the symptom is not a visibly wrong
		// band -- it is a worker parking on the old F while the notifier skips it on the new one.
		// A lost wakeup, with nothing in any dump pointing here.
		//
		// ONE re-read is still taken later, immediately before blocking, and only to catch F
		// GROWING OVER US between this load and the park. That one is not a second opinion about
		// the same question; it is a different question asked at the last safe moment.
		const TaskScheduler::Bands bandsNow = TaskScheduler::GetBands();   // ONE load, whole pass
		const bool reservedForHiPri = (size_t)qIndex < bandsNow.k;

		// ---- WHAT OS PRIORITY DOES THIS WORKER'S BAND DESERVE, THIS PASS? ---------------------
		//
		// LIVE F, NOT BASE. "Active floor" is the point: a worker the growth controller promoted is
		// doing never-park work right now and should be snappy while it is; when the floor sheds it
		// goes back to Normal with everyone else. Keying off the base instead would elevate two
		// workers permanently and leave the other twenty-six of a grown floor at Normal, which is
		// the wrong half of the band.
		//
		// RESERVED IS CRITICAL, FLOOR IS ONLY HIGH -- see ApplyWorkerPriority. A reserved worker is
		// one or two threads that are idle until a completion lands; a floor can be twenty-eight,
		// and twenty-eight TIME_CRITICAL threads is the process-wide elevation measured 5x worse.
		const WorkerPrio bandPrio =
			reservedForHiPri ? WorkerPrio::Critical
			: ((size_t)qIndex >= bandsNow.k && (size_t)qIndex < bandsNow.k + bandsNow.f)
				? WorkerPrio::High
				: WorkerPrio::Normal;

		// ---- THE K CONTROLLER NEEDS A CALLER WHOSE RATE RISES WITH LOAD --------------------
		//
		// MaybeAdjustHotWorkers had exactly one live caller: the lane-hint setter, AFTER its
		// `if (want == isSet) return;`. That makes it EDGE-triggered on clear->set, and under
		// sustained lane load the bit is set once and stays set -- so the harder the lane is
		// loaded, the LESS often the controller that would grow it is asked. Measured: 2,525,568
		// sustained hiPri tasks moved K off its minimum exactly zero times.
		//
		// Its promote test (`adv == mask`, every hot worker has lane work) is a LEVEL, so it only
		// ever needed to be evaluated periodically. This is that caller.
		//
		// ---- THE CALLER MUST BE A THREAD THAT CANNOT SLEEP ----------------------------------
		//
		// This was RESERVED-ONLY, resting on "K > 0 implies ReservedNeverParks, so [0,K) is
		// guaranteed to be looping". That implication is GONE: never-park is now opt-in, so with
		// K > 0 and the flag off, a reserved worker parks the moment its lane inbox empties -- and
		// the controller's only caller goes to sleep with it. K can then never shed, because
		// shedding requires a window of evidence that nobody is awake to gather.
		//
		// That is the floor-collapse bug in a second costume: a controller whose caller lives
		// inside the band the controller is allowed to switch off. Both times the fix is the same
		// -- drive it from a thread whose wakefulness does not depend on the decision being made.
		//
		// SO: RESERVED **OR** FLOOR. The floor is defined as never parking, so as long as F >= 1
		// there is always a caller, whatever never-park is set to and whatever K does.
		//
		// WHO CALLS IS NOT WHAT IS SAMPLED, and that distinction is what keeps this legal under the
		// band split. MaybeAdjustHotWorkers reads the lane counters of [0,K) and nothing else; a
		// floor worker calling it does not make the controller observe compute, any more than the
		// bench calling it would. The rule is about the controller's INPUTS, not its trigger.
		//
		// KNOWN EDGE: floor=0 with never-park off leaves no guaranteed-awake worker at all, so the
		// controller can still lose its caller. That configuration also has no wake-free dispatch
		// band, so it is already outside what K is for -- named here rather than papered over.
		//
		// The body early-outs on its own window and interval checks, so the cost of the subsample
		// is a couple of relaxed loads on one idle pass in 64.
		// ---- THE ADAPTIVE-K SAMPLE SAT HERE AND IS GONE. K IS STATIC. -----------------------
		//
		// This ticked the lane controller once every 64 passes from every K and F worker. It is
		// removed for one reason above all others: MEASURED, THE CONTROLLER NEVER ACTED. 2.5M hiPri
		// tasks moved K off 1 exactly zero times -- one of its two inputs had no callers at all, and
		// the other was edge-triggered in a way that made load fire it LESS, not more.
		//
		// So it was carrying two costs for no observed benefit. The first is this line: a walk of
		// every worker with clock reads, landing on the dispatch path of the band whose entire job
		// is latency. The second is the race surface -- a K that can move underneath a placement or
		// steal decision made against an earlier snapshot.
		//
		// STATIC K IS NOT A LESSER VERSION OF THIS. Nobody reserves cores by accident: setting K is
		// the statement, and a fixed band is exactly as much lane as was asked for. Bring the
		// controller back when a workload is shown to saturate a lane -- and if it comes back, it
		// wants identity-based membership first, so a promotion is a bit flip rather than a boundary
		// sliding under everyone else's arithmetic.

		// APPLIED HERE AS WELL AS AT DISPATCH, because the band changes while a worker is IDLE and
		// the dispatch site only runs when it found something. Without this an idle floor worker
		// never becomes snappy in the first place, and a worker the floor just shed keeps whatever
		// it last held -- so a burst would leave up to twenty-eight threads elevated for the rest of
		// the process, which is precisely the failure this is supposed to avoid.
		//
		// The curPrio compare makes the common pass a register test; a syscall happens only on a
		// genuine band transition or a hiPri task boundary.
		if (TaskScheduler::GetHotThreadPolicy() != TaskScheduler::HotThreadPolicy::Normal
		    && bandPrio != curPrio) {
			ApplyWorkerPriority(bandPrio);
			curPrio = bandPrio;
		}
		// ---- ONE NAME FOR K ON THIS THREAD ---------------------------------------------------
		//
		// There were THREE: reservedForHiPri from the pass snapshot, servesHiPri from
		// WorkerServesHiPri(q), and isHotWorker from GetHotWorkers(). The header defines
		// WorkerServesHiPri(q) as `q < GetHotWorkers()`, so all three asked the identical question
		// -- and answered it from up to three different loads of the band word. That family is what
		// produced k=514: not one wrong decode, but several places entitled to their own K.
		//
		// The old comment here claimed servesHiPri and isHotWorker were "TWO DIFFERENT QUESTIONS".
		// They are not, and the code proved it: both reduce to q < K. Keeping the distinction alive
		// in prose is how a future edit re-derives one of them from a different source.
		const bool isHotWorker = reservedForHiPri;   // q < bandsNow.k, and nothing else

		// The hot-core exclusion, decided from the snapshot instead of from a pre-loop K read. One
		// branch on a latched bool once this has settled; see the declaration for why it is one-way.
		if (!hotCpuExcluded && !reservedForHiPri) {
			TaskScheduler::ExcludeCurrentThreadFromHotCpus();
			hotCpuExcluded = true;
		}

		// INSURANCE, not a scan: one load of a line this worker already owns. "I am NOT reserved and
		// yet there is lane work sitting in my inbox" -- reachable when K is LOWERED while work is
		// queued. Costs nothing normally: the queue is empty and the check short-circuits.
		//
		// It does not mean the work is stranded. Every worker pops its own hiPri inbox in the search
		// below, reserved or not, so this task WILL run here; what it means is that this worker is
		// not the one maintaining that lane's hint bit, which is what the retirement below fixes.
		const bool hiPriStray = !reservedForHiPri && !scheduler->hiPriInboxes[qIndex]->empty();
		// Read once per pass beside isHotWorker: one relaxed load of a line the app touches only when
		// it reconfigures. Gates every piece of adaptive-K bookkeeping, so static K -- the default --
		// runs none of it.

		// Steal hints, maintained here and nowhere else: this is the one place the owner of this
		// deque reliably passes, and size() reads a line it already owns. Both writes are
		// conditional on a state CHANGE, so a queue that stays deep, or stays empty, never writes at
		// all. The hint goes stale while a long task runs -- the worker is not looping then -- which
		// can only cost a wasted probe, never hide work: the PARALLELISM bit is set by the splitter
		// at push time, before the task it split off can be picked up by anyone.
		//
		// ---- SKIPPED ON A PASS THAT ALREADY HOLDS A TASK -------------------------------------
		//
		// `task_to_run` is a thread_local that SURVIVES the `continue`, so the search sets it, the
		// loop restarts, and every line between here and the dispatch below is paid BETWEEN finding
		// work and running it. That is the round trip, and this block is the most expensive thing in
		// it: a deque size() plus two hint words that may each be an atomic read-modify-write.
		//
		// NOTHING HERE IS OWED TO THE TASK ABOUT TO RUN. These advertise THIS worker's backlog to
		// thieves. A backlog that goes unadvertised for one pass costs a wasted probe, never work:
		// the bit is re-evaluated on the very next idle pass, and the PARALLELISM bit is set by the
		// splitter at push time regardless. That is the same argument the paragraph above already
		// makes for the hint going stale inside a long task body -- holding a task is that same
		// state, one moment earlier.
		//
		// The dual is what makes it safe: the hints exist to tell OTHER workers where to steal, and
		// a worker with a task in hand is not the one who needs to be found.
		if (!task_to_run) {
			const size_t depth = scheduler->deques[qIndex]->size();
			scheduler->UpdateBacklogHint((size_t)qIndex, depth);
			scheduler->ClearParallelHintIfEmpty((size_t)qIndex, depth);
			// The lane's own retirement path, for the case no thief covers: a hot worker that
			// drained its own backlog by popping. ONCE PER PASS, not per pickup -- that frequency
			// difference is the entire cost argument, and a pass is exactly when this worker has
			// nothing better to do anyway.
			if (isHotWorker) {
				// PRESENCE, NOT DEPTH. An MPSC inbox has no size() -- it is a linked list and
				// counting it would mean walking it. That costs nothing here, because the lane
				// hint has only ever been a presence BIT: every reader asks "is there lane work
				// on q", never "how much". 0/1 answers that exactly.
				const size_t laneDepth = scheduler->hiPriInboxes[qIndex]->empty() ? 0u : 1u;
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
			// [K, K+F), not [0, F) -- see the band layout in GetHotWorkers. A reserved worker's
			// occupancy says nothing about whether COMPUTE needs more never-parking capacity.
			const bool onFloor  = (size_t)qIndex >= bandsNow.k
			                   && (size_t)qIndex < bandsNow.k + bandsNow.f;
			long long busyStartNs = 0;
			if (onFloor) {
				tasksRun.fetch_add(1, std::memory_order_relaxed);
				busyStartNs = MonotonicNs();
				// Publish it: the growth gate reads this to tell a long body from a trivial one.
				taskStartNs.store(busyStartNs, std::memory_order_relaxed);
			}

			// ---- THE K CONTROLLER'S OBSERVER -------------------------------------------------
			//
			// THESE THREE COUNTERS HAD NO WRITER. MaybeAdjustHotWorkers exchange()s all three every
			// window, and nothing in this file ever incremented them -- so the controller read zero
			// for laneBusyNs (its slow promote could never fire) and zero for laneTasksRun, which
			// is the DEMOTE numerator: `low = (topTasks == 0)` was unconditionally true, so a K
			// controller that was ever enabled would shed on every single window. That is why
			// adaptive K "never ramped": it had no eyes, and what it did see said "shed".
			//
			// TASK COUNT, NOT OCCUPANCY, and the distinction is the whole design. A lane worker
			// resuming an I/O completion does a few microseconds of work and suspends again, so a
			// fully-loaded lane sits at single-digit occupancy -- it is paid to be AVAILABLE, not
			// busy. Occupancy-driven demotion measured 29.30 -> 40.80 p50 on the 200us burn because
			// it kept shedding cores that were doing their job. laneBusyNs is still recorded, but
			// only the slow PROMOTE reads it; the shed decision is a count.
			//
			// STAMPED ONLY FOR LANE-SOURCED WORK. Timing every task a reserved worker touches would
			// feed the controller the one thing it must ignore -- and an empty reserved spin must
			// read as 0% duty, not 100%, or K ratchets to the cap on an idle pool (see §7 of the
			// design note). `laneSourced` is that filter.
			// GATED ON THE EXACT PREDICATE THE CONTROLLER ITSELF USES -- kmax > kmin -- AND READ FROM
			// THE SAME SNAPSHOT as every other band decision on this pass. Under STATIC K (SetHotWorkers
			// pins them equal, and that is what everything ships with) this costs one comparison of two
			// fields already in hand. It has to be cheap: what it guards is a MonotonicNs() on every
			// lane task, and the lane IS the latency path.
			//
			// THE SAME PREDICATE, NOT A SECOND SPELLING OF IT. There was a HotScalingActive() that
			// looked like the right thing to ask and was a trap -- hard-wired false, read by
			// nobody, while the controller gated on max > min. Gating the observer on one and the
			// controller on the other is how the counters end up unwritten while the controller
			// reads them: laneTasksRun stays 0, `low = (topTasks == 0)` is always true, and K sheds
			// every window -- the exact bug this observer exists to remove. That function is gone.
			long long laneStartNs = 0;
			if (reservedForHiPri && bandsNow.kmax > bandsNow.kmin) {
				laneCyclesTotal.fetch_add(1, std::memory_order_relaxed);   // sample count
				if (laneSourced) {
					laneTasksRun.fetch_add(1, std::memory_order_relaxed);
					laneStartNs = MonotonicNs();
				}
			}
			laneSourced = false;   // describes exactly one task -- cleared as it is dispatched

			// ---- THIS WORKER IS NOW INSIDE A TASK BODY, AND PLACEMENT NEEDS TO KNOW -----------
			//
			// THE HALF THAT WAS MISSING. `awayHint`, `SetAway` and `AwayHintWord` all exist, and
			// PickNextWorker already filters on them -- `hereW[w] = awakeW[w] & ~AwayHintWord(w)`.
			// Nothing in this file ever SET the bit, so placement was masking against a bitmap that
			// was permanently zero: every worker looked available, including one sitting inside a
			// task that will not return for milliseconds. The spin-help path's `~RestoreAway`,
			// which sets the bit back to TRUE on the way out, is the tell -- it restores a state
			// that nothing established.
			//
			// WHY IT MATTERS AND NOT JUST FOR LATENCY. An inbox has one legal consumer, so work
			// steered at a worker that is inside a task body is not merely late -- it is
			// unreachable until that body returns. tests/dag_cancel_test.cpp holds a mutex in a
			// fiber that busy-waits on a flag; with the bit never set, placement kept choosing that
			// worker, and 2 of 4 waiters queued behind it never started at all, deterministically.
			//
			// ---- AND IT IS PUBLISHED BY `busy`, NOT BY A SECOND BITMAP ------------------------
			//
			// A SetAway(true)/SetAway(false) pair sat here and around both completion arms, and it
			// was the most expensive thing added this version: awayHint is ONE shared word, so
			// every worker doing fetch_or at dispatch and fetch_and at completion is a cache line
			// ping-ponging across the whole pool, per task.
			//
			// MEASURED, same binary, one flag apart: throughput/1p 5.37 -> 2.93 M/s and frame DAG
			// 8.45 -> 35.82 us/graph. A 4.2x on the DAG row to publish a fact that `busy` -- set
			// four lines below with a RELAXED STORE to a line this worker already owns -- was
			// already publishing for free.
			//
			// So the escape-hatch check reads `busy` instead (see PushLocal). Same question, same
			// answer, no shared write. The lesson is not "away was wrong" -- it is that a second
			// publication of a fact you already have is not free just because the store is small.

			// ---- AND PUBLISH THE BACKLOG BEFORE DISAPPEARING INTO THE BODY --------------------
			//
			// The away bit stops placement steering NEW work here. It cannot help what is already
			// queued -- and that is the common case, not the rare one: a worker takes its next task
			// out of the very queue the rest of the batch is sitting in, so whatever arrived with it
			// is still there when it goes away.
			//
			// An inbox has exactly one legal consumer, so that backlog is now unreachable to its
			// owner (busy) and to everyone else (not allowed). The drain that would move it into the
			// stealable deque lives on the IDLE path -- `if (!task_to_run)` -- which a worker inside
			// a long body never reaches. Measured by tests/busy_owner_inbox_test.cpp: 3-5 of 8
			// trivial tasks ran while one hog held its worker and the other worker sat idle.
			//
			// So the moment this worker becomes unreachable, it hands its inbox to its deque, where
			// a thief may take it. This is the same drain the idle path performs, moved to the point
			// where it is actually needed.
			//
			// THE HINT MUST BE UPDATED HERE TOO, and forgetting it would make the whole thing
			// pointless: thieves probe advertised queues, the hint is normally written once per pass
			// at the top of the loop, and this worker is about to stop having passes. Work in an
			// unadvertised deque is invisible -- exactly the `advertised queues = 0` next to a
			// non-empty deque that the AVX hang's dump showed.
			//
			// THE HINT IS UPDATED UNCONDITIONALLY, and the first version of this gated it on having
			// drained something -- which fixed nothing, because by the time a worker picks up a
			// task its inbox is usually ALREADY EMPTY: the idle-path drain moved the whole batch
			// into the deque one pass earlier, and the task being run was popped from there.
			//
			// So the backlog is in the DEQUE, which is stealable, and the reason nobody takes it is
			// purely that it is not advertised: UpdateBacklogHint runs at the TOP of a pass and the
			// drain at the BOTTOM, so the hint written this pass describes the deque as it was
			// BEFORE the drain -- zero -- and the next pass, which would correct it, never happens
			// because this worker is about to disappear into a task body.
			//
			// One store of a word this worker owns, on a path that already costs a dispatch.
			// UNCONDITIONAL, NOT UpdateBacklogHint. That one only advertises at kStealHintDepth (8)
			// or more, which is correct for a worker that is still taking passes and wrong for one
			// that is not: below the threshold the backlog is in a deque nobody is told about, and
			// the owner who would have run it is gone. Measured at depth 1, with an idle worker
			// beside it, for the whole duration of the body.
			// ---- IF THIS WORKER IS BACKED UP, PUBLISH BEFORE DISAPPEARING INTO THE BODY --------
			//
			// An inbox is fast because it has one legal consumer -- one exchange and a store, no
			// CAS -- and unreachable for the same reason: while its owner is inside a task body,
			// nothing in it can be taken by anyone. A deque is the opposite trade. So the answer is
			// not to pick one, it is to move between them at the one moment the trade flips.
			//
			// GATED ON DEPTH, AND THAT GATE IS THE WHOLE DIFFERENCE. An earlier version drained
			// whenever the inbox was non-empty, which is nearly every dispatch: a single producer
			// then walked its entire backlog into one deque and hit the 65,536-slot ceiling on
			// 200,000 no-op tasks. Backed up is rare, and it is exactly when the work is worth
			// making stealable -- a worker holding one task will run it; a worker holding eight is
			// not keeping up and should not also be a wall.
			//
			// FREE WHEN IT DOES NOT FIRE: one relaxed load of `inboxDepth`, a counter this worker
			// owns and the pusher already writes on every push. No bitmap, no shared word, no
			// atomic RMW added to the dispatch path -- which is what the away-bit version cost
			// (throughput/1p 5.37 -> 2.93 M/s, frame DAG 8.45 -> 35.82 us/graph).
			//
			// BOUNDED BY DEQUE HEADROOM, NOT JUST BY INBOX DEPTH -- and that second bound is the
			// one this took two attempts to get right. An inbox is a linked list and has no
			// ceiling; a deque has one (65,536) and aborts at it rather than dropping a task. So
			// "the inbox is backed up" is NOT a licence to move the backlog: when a producer
			// outruns the pool, backed-up is true on nearly every dispatch, and relocating each
			// time walks an unbounded queue into a bounded one. Measured twice, both fatal at the
			// ceiling on 200,000 no-op tasks.
			//
			// The purpose is to make SOME work stealable, not to move all of it. A thief needs a
			// few tasks to find, and whatever is left stays where it is -- reachable by the owner
			// on its next pass exactly as before.
			//
			// BATCHED ON BOTH SIDES, because that is what the two structures are for: a Vyukov
			// inbox pops many cheaply and push_bottom_batch publishes them with ONE release fence
			// and ONE store to bottom_, instead of a top_ load and a capacity check per item. The
			// deque forbids batch STEAL -- a stealer does not touch top_ for non-last items, so a
			// batch could double-claim -- and that restriction is on the thief's side only.
			//
			// COMPACT BEFORE PUBLISHING. push_bottom_batch stores what it is handed, and a null in
			// the ring is a task nobody can run and a crash for whoever pops it.
			constexpr size_t kPublishAtMost = 32;   // <= BATCH_SIZE
			constexpr size_t kDequeHeadroom = 256;
			if (!reservedForHiPri
			    && inboxDepth.load(std::memory_order_relaxed) >= (int)TaskScheduler::kStealHintDepth
			    && scheduler->deques[qIndex]->size() < kDequeHeadroom) {
				size_t got = 0;
				while (got < kPublishAtMost && scheduler->loPriInboxes[qIndex]->pop(batch[got]))
					if (batch[got]) ++got;

				size_t moved = 0;
				if (got) {
					if (!scheduler->deques[qIndex]->push_bottom_batch(batch, got))
						TaskDeque::FatalPushRefused();
					moved = got;
				}
				if (moved) {
					inboxDepth.fetch_sub((int)moved, std::memory_order_relaxed);
					// Advertise on the ORDINARY rule -- we just moved at least kStealHintDepth
					// items, so the normal threshold fires on its own. No special-casing, and no
					// pool-wide insomnia from advertising shallow queues.
					scheduler->UpdateBacklogHint((size_t)qIndex, scheduler->deques[qIndex]->size());
				}
			}

			// A DRAIN-TO-DEQUE SAT HERE AND IS GONE. The idea was to publish this worker.s inbox into
			// its stealable deque before disappearing into a task body, so a thief could reach it.
			// It overflowed the deque: this runs on EVERY dispatch with a non-empty inbox, so a
			// single producer pushing faster than the pool drains moves the entire backlog into one
			// deque -- 200,000 no-op tasks hit the 65,536-slot ceiling and aborted. The idle-path
			// drain it duplicated is bounded both ways: BATCH_SIZE per pass, and only when the worker
			// has nothing else to do.
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
			// PRIORITY IS A PROPERTY OF THE BAND, NOT OF THE TASK -- applied once per pass at the
			// top of the loop, not here. origin/main additionally raised to CRITICAL for any hiPri
			// TASK, wherever it ran, and that half is deliberately NOT ported: it would put a floor
			// worker at 15 for the duration, which is exactly what giving the floor HIGHEST instead
			// of TIME_CRITICAL exists to prevent. A lane task gets its latency from being ROUTED to
			// a reserved worker, which is already at CRITICAL; it does not need to drag whatever
			// worker it landed on up with it.
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

				// CLOSE THE LANE STAMP ON THIS ARM TOO. Native and Coroutine tasks `continue` from
				// here and never reach the fiber arm's close below -- and I/O completions are
				// exactly Native and Coroutine, so closing only there would have measured zero busy
				// time for the entire workload the K controller exists to serve.
				if (laneStartNs != 0)
					laneBusyNs.fetch_add(MonotonicNs() - laneStartNs, std::memory_order_relaxed);

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
				// WRITTEN BY THE OWNING WORKER, BEFORE THE SWITCH-IN, and never from inside the
				// fiber. A fiber cannot safely learn which worker it is on and record itself:
				// Thread::GetCurrent() is a thread_local, so anything the fiber derives from it
				// before a suspend is wrong after one. The loop never migrates; the fiber does.
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
			// The fiber arm's half of the lane stamp -- see the Native/Coroutine close above.
			if (laneStartNs != 0)
				laneBusyNs.fetch_add(MonotonicNs() - laneStartNs, std::memory_order_relaxed);
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
			// ---- STRAY ORDINARY WORK ON A RESERVED CORE: HAND IT OFF, NEVER SIT ON IT ----
			//
			// A reserved worker refuses ordinary work, which makes its loPri inbox a BLACK HOLE if
			// anything ever lands there: inboxes are not stealable, so no thief can rescue it, and
			// the owner will not run it. Observed exactly once and it stopped the pool -- the dump
			// read `1 AWAKE ... inbox 0/1/0`, one loPri task on a reserved core, every other worker
			// asleep with nothing advertised.
			//
			// PLACEMENT IS SUPPOSED TO PREVENT THIS and mostly does, but "mostly" is not a property
			// a scheduler can rely on: Requeue, an explicit-affinity push, and any future path can
			// all name a worker directly and bypass the band logic entirely. So rather than trust
			// that every site was found, the reserved worker hands strays to the first compute
			// worker and carries on. Self-healing beats exhaustive, because the failure mode of
			// missing one site is a silent permanent stall.
			if (reservedForHiPri && !task_to_run) {
				Task* stray = nullptr;
				if (scheduler->loPriInboxes[qIndex]->pop(stray) && stray) {
					// SAY SO -- but say the RIGHT thing, and which thing is right depends on whether
					// K can move. Under STATIC K, reaching here means some placement path put
					// ordinary work on a reserved core: a bug, and the net makes it survivable
					// rather than correct. Under ADAPTIVE K there is a second, LEGAL way in: the
					// task was placed while this worker was still compute, and a promote then grew
					// K over an inbox that already held it. No placement path did anything wrong --
					// the hand-off below IS the design for that race (the spec's promote rule: the
					// newly reserved worker sheds the loPri work it was holding). Printing
					// "placement bug" for that case sends whoever reads it hunting a path that does
					// not exist.
					static std::atomic<unsigned> warned{ 0 };
					if (warned.fetch_add(1, std::memory_order_relaxed) == 0) {
						if (bandsNow.kmax > bandsNow.kmin) {
							std::fprintf(stderr,
								"[JLib::Scheduler] ordinary task found on RESERVED worker %d (K=%zu, adaptive) --"
								" handing it to the first compute worker. Expected when a promote grows K"
								" over an inbox that already held work; only a placement bug if it floods.\n",
								qIndex, bandsNow.k);
						} else {
							std::fprintf(stderr,
								"[JLib::Scheduler] ordinary task placed on RESERVED worker %d (K=%zu) --"
								" handing it to the first compute worker. This is a placement bug;"
								" the net keeps the pool alive but the path should be found.\n",
								qIndex, bandsNow.k);
						}
					}
					// THE SAME K THAT SAID THIS WORKER IS RESERVED. Re-asking would let K move
					// between the two, and then the "first compute worker" this hands the stray to
					// could itself be inside the reserved band -- the net re-creating the exact
					// placement bug it exists to absorb.
					const size_t firstCompute = bandsNow.k;
					if (firstCompute < scheduler->workers.size()) {
						scheduler->loPriInboxes[firstCompute]->push(stray);
						scheduler->workers[firstCompute]->inboxDepth.fetch_add(
							1, std::memory_order_relaxed);
						scheduler->workers[firstCompute]->MarkQueuedWork();
						scheduler->workers[firstCompute]->NotifyWorker(/*force*/ true);
					}
					else {
						// Nowhere to hand it to (K covers the pool). Run it rather than lose it --
						// the reservation is not worth stranding a task over.
						task_to_run = stray;
						continue;
					}
				}
			}

			// ---- THE LANE IS THE INBOX: POP ONE, RUN IT ----------------------------------
			//
			// Priority is expressed as ORDER, and this is where the order lives: every worker
			// looks at its lane before its own deque and before stealing.
			//
			// THERE IS NO LANE DEQUE. A Chase-Lev deque exists so OTHER threads can steal from
			// it, and nobody steals lane work -- the reserved worker is the one consumer, which
			// is exactly what an MPSC inbox already guarantees. Keeping a deque behind the inbox
			// only bought an unload step: pop from the inbox, push to the deque, pop it again,
			// all on the one thread, on the one path that exists for latency. The K worker wants
			// to take the next arrival and run it, and that is now the whole operation.
			//
			// It also costs nothing to grow. The inbox links through Task::next, so depth is
			// bounded by live tasks rather than by a 32,768-slot ring per worker that had to be
			// allocated up front and was never read from.
			//
			// CLEAR THE PRESENCE BIT WHEN THE INBOX IS EMPTY, and the race is benign in the one
			// direction it can go: a push landing between the emptiness check and the clear
			// leaves the bit down while work exists, so thieves will not probe -- but the OWNER
			// checks this inbox before anything else on every pass, so the work is found, just
			// not advertised. Nothing strands. The reverse (bit set, inbox empty) costs one
			// failed probe.
			if (!task_to_run) {
				Task* hp = nullptr;
				if (scheduler->hiPriInboxes[qIndex]->pop(hp) && hp) {
					task_to_run = hp;
					laneSourced = true;   // K controller input -- see laneSourced

					// ONE POP, ONE DECREMENT. The push incremented inboxDepth (see the hiPri
					// branch of the ordinary push path), so the lane has to pay it back or the
					// counter drifts upward and the loPri publish-at-dispatch below it fires on
					// a backlog that is not there.
					inboxDepth.fetch_sub(1, std::memory_order_relaxed);

					// ---- THE STRAND MEASUREMENT, AT THE INSTANT STRANDING HAPPENS -----------
					//
					// This is the moment: a task has been taken, the body has not started, and
					// anything still in this inbox is about to become unreachable -- an MPSC has
					// one legal consumer and that consumer is leaving Worker().
					//
					// IT MEASURES WHETHER A SHARED MPMC LANE WOULD WIN ANYTHING. A pull queue
					// helps in exactly one situation: work queued on a consumer that is busy WHILE
					// ANOTHER CONSUMER IS IDLE. If every reserved worker is inside a body, an MPMC
					// has nobody to hand the backlog to either and both designs wait the same. So
					// the split below is the whole question, and `withIdlePeer` is the prize:
					//
					//   strandEvents  -- dispatches that left a non-empty lane inbox behind
					//   withIdlePeer  -- of those, how many had another reserved worker NOT in a
					//                    body at that instant, i.e. somebody who could have taken it
					//
					// A `withIdlePeer` near zero means the lane is genuinely saturated when it
					// backs up, the MPMC's addressable set is empty, and the p99 gap is dispatch
					// cost rather than misallocation -- which a shared queue would ADD to.
					//
					// NOT THE SAME QUESTION AS `spilled`. That one is answered by the PRODUCER at
					// push time and can only see the owner's state then; this one is answered at
					// dispatch, so it catches the case the spill structurally cannot -- an owner
					// that was idle when the push landed and entered a body immediately after.
					//
					// COSTS ONE `empty()` LOAD when there is no backlog, which is the common case
					// and the same load the staging block it replaced opened with. The peer scan
					// runs only when a backlog exists and is at most K-1 relaxed loads, K <= 4.
					if (!scheduler->hiPriInboxes[qIndex]->empty())
						TaskScheduler::NoteLaneStrand((size_t)qIndex);

					continue;
				}

				// ---- WHAT WAS HERE: STAGE THE REMAINDER INTO A LANE DEQUE ---------------------
				//
				// A batch-pop of up to 32 followed by push_bottom_batch into hiPri[qIndex], so a
				// backlog behind this worker stayed stealable while it was inside a body. It ran on
				// 53% of completions in the overlapped io row, and it is gone with the deque.
				//
				// THE PROBLEM IT SOLVED ONLY EXISTS IF K RUNS A LONG BODY. An MPSC has one legal
				// consumer, so work left in this inbox waits for THIS worker -- which is a wait of
				// one body, and a lane body is a completion handler. The owner is back in Worker()
				// microseconds later and takes the next one. Staging bought reachability during a
				// long body, and paid for it on every completion with a pop, a deque push, a hint
				// update and a notify, on the one path that exists for latency.
				//
				// SO THE CONTRACT IS THE FIX, NOT THE STRUCTURE: do not put a long body on the
				// reserved band. A handler that blocks or grinds belongs on an ordinary worker, and
				// the way to get it there is to push it -- which is one line in the handler, not a
				// Chase-Lev ring and a steal protocol per reserved core.
				//
				// AND NEW ARRIVALS ARE STILL COVERED. HiPriSpillTarget checks the owner's `busy`
				// flag at PUSH time and redirects to another idle worker in [0, K), so a completion
				// arriving while this one is mid-body never joins the backlog in the first place.
				// That is the producer-side half, it is cheaper, and it survives.

				if (scheduler->hiPriInboxes[qIndex]->empty()) {
					scheduler->ClearHiPriHint((size_t)qIndex);
				}
			}

			// ---- A YIELD DOES NOT GET TO SKIP THE INBOX FOREVER --------------------------------
			//
			// The resumed inbox is checked before the loPri inbox drain at the bottom of this pass,
			// which is right for a genuine resume -- nobody else may drain it, so deferring one is
			// a permanent hang. A YIELD is different in exactly one way that matters: it re-arms
			// that check every pass, so "resumes first" silently becomes "resumes only" and the
			// drain is never reached.
			//
			// So when the last thing this worker re-queued was a yield, and its own loPri inbox has
			// something in it, that inbox gets this pass. One drain, then the yield resumes its
			// normal priority -- this bounds the source, it does not throttle it.
			//
			// quiescent(), NOT empty(): the same reason the park predicate uses it. empty() ignores
			// head_, so a push mid-flight reads as nothing there, and skipping the drain on that
			// answer is how a task waits for the next yield to notice it.
			//
			// THE FLAG IS CLEARED HERE WHATEVER HAPPENS, so it describes one pass and cannot latch
			// a worker into draining first forever.
			if (yieldedLastPass) {
				yieldedLastPass = false;

				// THE INBOX FIRST -- oldest arrival, and the queue nobody else may drain.
				if (!scheduler->loPriInboxes[qIndex]->quiescent()) {
					Task* fromInbox = nullptr;
					if (scheduler->loPriInboxes[qIndex]->pop(fromInbox) && fromInbox) {
						inboxDepth.fetch_sub(1, std::memory_order_relaxed);
						task_to_run = fromInbox;
						continue;
					}
				}

				// AND THIS WORKER'S OWN DEQUE, for the same reason. The deque pop also sits below
				// the resumed check, so a yield loop starves it too -- measured: with only the
				// inbox covered, the AVX suspend test went from 3 of 8 fibers started to 7 of 8,
				// and the last one sat in a worker's own deque while that worker yield-looped.
				//
				// A DEQUE IS STEALABLE, so this looks like it should resolve itself. It does not:
				// the steal hint is written from this worker's own pass, and a worker that never
				// finishes a pass never advertises -- `advertised queues = 0` with a task sitting
				// in q4's deque, in the dump that found this. Unreachable to its owner AND
				// invisible to thieves is the same dead end the inbox has, one queue over.
				if (auto opt = scheduler->deques[qIndex]->pop_bottom()) {
					if (Task* t = *opt) { task_to_run = t; continue; }
				}
			}

			if (!task_to_run) {
				Task* resumed = nullptr;
				if (scheduler->resumedInboxes[qIndex]->pop(resumed) && resumed) {
					task_to_run = resumed;
					// A RESUME IS LANE WORK for a reserved worker. It is the I/O completion path
					// itself -- a fiber coming back from an await -- and counting only hiPri pushes
					// would read a fully-loaded reactor as an idle lane and shed the core serving it.
					laneSourced = true;
					continue;
				}
			}

			// ---- A RESERVED WORKER TAKES NO ORDINARY WORK, FROM ANY SOURCE -------------
			//
			// Excluding [0, R) from PLACEMENT is not enough on its own, and the measurement said so
			// plainly: with placement fixed but this missing, the busy-pool max was still 1202 us --
			// a reserved worker had simply STOLEN a 400 us body and the completion queued behind it.
			// A reservation that only covers the push path reserves nothing.
			//
			// So a reserved worker runs hiPri and nothing else: its own hiPri inbox and deque above,
			// hiPri stolen from others, and otherwise it spins idle. Spinning idle is the cost of
			// the guarantee -- R cores that do no bulk work -- and it is why R is 1 by default.
			if (!task_to_run && !reservedForHiPri) {
				auto opt = scheduler->deques[qIndex]->pop_bottom();
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
				// File-scope rather than block-local: the PARK decision needs it too. A worker that keeps
				// losing the race for advertised work must be allowed to sleep, and that decision is made
				// several sections below this one.

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

					// ---- THERE IS NOTHING TO STEAL FROM A LANE ANY MORE ----------------------
					//
					// A LANE PROBE LIVED HERE, above MaybeStealable, because MaybeStealable tests
					// the loPri hint words only and would reject a worker advertising a staged lane
					// backlog before any lane probe could run. That ordering was correct for the
					// structure it served, and the structure is gone: with no hiPri deque there is
					// no endpoint to steal from. The lane is an MPSC inbox, and an inbox has exactly
					// one legal consumer -- steal_if on one is not a slower option, it is not an
					// option.
					//
					// SO LANE WORK IS NOW REACHED IN EXACTLY ONE WAY: its owner pops it. The two
					// things that used to be reachability -- staging plus this probe -- are replaced
					// by keeping the backlog from forming, which is the producer-side spill at push
					// time. That trade is the point of the change: a probe every thief pays on every
					// victim, for a rescue that only mattered while a reserved worker sat inside a
					// long body it should not have been running.

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
					// ---- THERE IS NOTHING TO STEAL ON THE LANE ------------------------------
					//
					// A gated hiPri steal used to sit here. It is not disabled, it is unreachable
					// by construction: the lane is an MPSC inbox, which has exactly ONE legal
					// consumer, and a second thread popping it is not a slower steal but a data
					// race. Stealing is a property of the Chase-Lev deque, and the lane no longer
					// has one.
					//
					// THAT IS THE RULE, NOT A REGRESSION. The reserved worker is the consumer of
					// its own lane and is not to be stolen from; at K=0 the owner still drains its
					// inbox before anything else on every pass, so lane work is never stranded --
					// only un-stealable.
					//
					// WHAT IT GIVES UP, PLAINLY: a completion queued behind a floor worker buried
					// in a long loPri leaf can no longer be relocated to an idle thief. K is what
					// buys that back -- a reserved worker is never inside a bulk body.

					// A RESERVED THIEF NOW STEALS NOTHING. It served hiPri only, and hiPri is no
					// longer stealable, so this is the whole answer for q < K rather than a filter
					// ahead of one. Taking a bulk task here is the path that left the max at 1202 us.
					// The lane probe that used to sit here has moved ABOVE MaybeStealable -- see
					// there for why its position is load-bearing rather than stylistic. What
					// remains below is the bulk rule, unchanged: a reserved thief takes none.
					if (reservedForHiPri) return false;
					auto s = scheduler->deques[target]->steal_if(classOK);
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
				//     a buried worker is not looping. Identical under the current scan: it would not
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
				const size_t nq0      = scheduler->deques.size();
				const bool   bitsUsable = nq0 <= TaskScheduler::kMaxHintQueues;
				size_t       nWords   = bitsUsable ? ((nq0 + 63) / 64) : 0;
				if (nWords > TaskScheduler::kHintWords) nWords = TaskScheduler::kHintWords;

				unsigned long long bitsW[TaskScheduler::kHintWords] = {};
				unsigned long long bitsAny = 0;
				// ---- STAGED LANE WORK IS AN ADVERTISEMENT, IN BOTH SENSES --------------------
				//
				// bitsW IS NOT ONLY A COUNT, IT IS THE VICTIM SET. Victim selection below probes a
				// worker if and only if its bit is set here -- "never probe a victim whose bit is
				// clear" -- so a worker advertising ONLY lane work would never be visited by any
				// thief, no matter how much was staged on its hiPri deque. OR-ing the lane word in
				// is what makes staged lane work reachable at all.
				//
				// THE COST IS THE ONE THIS BLOCK ALREADY ACCEPTS. A victim advertised for lane work
				// but not bulk is probed by tryStealFrom, which tries hiPri FIRST -- so it either
				// hits, or it costs exactly the single failed probe the comment below already
				// budgets for a stale SET, after which the thief lowers the bit.
				//
				// AND THE COUNT MATTERS SEPARATELY: advertisedCount == 0 is the park gate. A worker
				// stages its remainder precisely because it is about to vanish into a long body, so
				// the pool is usually otherwise quiet at that instant. Without the lane word here,
				// every other worker would see nothing advertised, park, and leave a stealable
				// backlog with nobody awake to steal it -- stranded again, in the very structure
				// added to stop stranding.
				for (size_t w = 0; w < nWords; ++w) {
					const unsigned long long loBits   = scheduler->StealHintWord(w);
					const unsigned long long laneBits = scheduler->LaneHintWord(w);
					bitsW[w] = loBits | laneBits;
					bitsAny |= bitsW[w];
					advertisedCount += platform::PopCount64(loBits)
					                 + platform::PopCount64(laneBits);
				}

				// A RESERVED WORKER IS COUNTED ADVERTISEMENTS FOR WORK IT MAY NOT TAKE, and this is
				// left alone deliberately. StealHintWord counts loPri advertisements from the whole
				// pool; a reserved worker can act on none of them, so in principle its idle decision
				// should read the hiPri hint word instead.
				//
				// TRIED AND REVERTED, because the problem it was written for did not exist: the
				// banner appeared to show reserved workers never parking, and that was an artifact of
				// resetting the park counters after startup. They park ONCE, immediately, and sleep
				// through the whole run -- nothing notifies them, so they never reach this decision
				// again. The gate is reached exactly once per reserved worker per run.
				//
				// It may still be worth doing for an I/O-heavy workload, where a reserved worker wakes
				// often and would then be held awake by compute backlog after every completion. That
				// is a real scenario and an unmeasured one; do not re-add it without a row that shows
				// the reserved worker staying awake.

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
				// Reserved workers do not drain the ordinary inbox either. Nothing should be placed
				// there for them, but a Requeue or an explicit-affinity push can still land one, and
				// draining it would put a bulk body on a reserved core.
				while (!reservedForHiPri
				       && count < BATCH_SIZE && scheduler->loPriInboxes[qIndex]->pop(batch[count])) {
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
					if (scheduler->deques[qIndex]->push_bottom_batch(batch, count)) {
						auto opt = scheduler->deques[qIndex]->pop_bottom();
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
			// GATED, so the app can move the sweep off the workers -- see
			// HazardDomain::SetSelfScan. Same trade EpochManager::SetSelfReclaim offers: the sweep
			// walks every hazard cell, and a worker doing that has stopped being available for the
			// completion that lands a microsecond later. An app with a natural idle point (a frame
			// boundary) can call Scan() there instead and keep its workers on work.
			//
			// THE LATCH IS SET EITHER WAY. It records "this worker has already looked since it last
			// ran something", which is true whether the sweep happened or was declined -- leaving it
			// clear would make a worker with self-scan off re-enter this branch on every idle pass
			// for the life of the process, which is the per-pass cost the latch exists to remove.
			if (!scannedSinceWork) {
				if (HazardDomain::Instance().SelfScanEnabled())
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
			// NO FLOOR TERM HERE, AND THAT IS DELIBERATE. Adding one looks right -- a floor worker
			// must not park -- but this predicate runs BEFORE the collapse call site further down,
			// so a floor worker that `continue`s here never reaches it. Measured: collapse calls
			// fell from ~5,000,000 a run to 162, and F then stuck above base on every single run
			// instead of intermittently. The floor check belongs inside the park block, after the
			// collapse has had its chance. See the CAS-to-SLEEPING below.
			// quiescent(), NOT empty(), ON ALL THREE QUEUES -- here and in both wait predicates
			// below, term for term. empty() ignores head_, so a push that has committed its
			// head_.exchange but not yet linked `next` reads as "nothing there"; one failed pop
			// lies the same way in the same window. That is survivable in the search loop (the
			// item is found next pass) and fatal here: these queues are owner-drain-only, so an
			// answer of "empty" that a producer is mid-way through falsifying puts this worker to
			// sleep on work nobody else may take. The K-demote pass is where it bites -- the
			// leaving worker asking "is the lane I am abandoning drained" must not accept a
			// single empty() as the answer. quiescent() also reads head_, which closes the window:
			// a committed push has already moved head_.
			if (!running.load(std::memory_order_acquire)
				|| (!scheduler->paused.load(std::memory_order_seq_cst)
					&& (hasQueuedWork.load(std::memory_order_seq_cst)
						|| laneWake.load(std::memory_order_seq_cst)
						|| !scheduler->hiPriInboxes[qIndex]->quiescent()
						|| !scheduler->loPriInboxes[qIndex]->quiescent()
						// PINNED RESUMES. Not optional and not merely a latency question: nobody
						// else is permitted to drain this queue, so parking on a non-empty one is
						// a permanent hang rather than a delay. Same reasoning as the loPri inbox
						// above, one degree worse -- that one at least ends when its owner wakes
						// for another reason.
						|| !scheduler->resumedInboxes[qIndex]->quiescent()))) {
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

			// ---- THE PARK: ONE ADDRESS, NO OWNED KERNEL OBJECT --------------------------------
			//
			// A worker with nothing advertised and no floor obligation blocks on its own
			// `workerState` word -- WaitOnAddress on Windows, FUTEX_WAIT on Linux. Wake() stores
			// AWAKE and signals that same address. One word, one waiter, one signal.
			//
			// TWO EARLIER PARKS LIVED HERE AND BOTH WERE DELETED, each for a reason that still
			// shapes this one:
			//
			//   the condvar  cost a mutex acquire on every notify -- on the PUSH path, the hot one.
			//   the event    removed that mutex but left TWO live mechanisms, and a worker already
			//                blocked in one when the selector flipped was never signalled again.
			//                Not a latency bug: a permanent strand. 24 of 31 workers asleep on
			//                non-empty inboxes, 0 of 24 subjects ever started. Reproduced in
			//                tests/fiber_pinning_test.cpp.
			//
			// Hence the rule this park is built to keep: ONE thing to signal. A park fiber was
			// tried here too and removed for exactly that -- it stacked a second waiter behind the
			// same notify, which is the event bug wearing different clothes.
			//
			// THE COST IT STILL PAYS, measured: event/resume with a 1 ms hold-off -- long enough
			// that the worker had genuinely parked -- costs 8.0 us against 5.0 us with none. That
			// ~3 us is the OS wake. Nothing here removes it; the AWAKE FLOOR is what avoids it, by
			// keeping workers [0, F) out of this block entirely so a push aimed at one of them
			// never has to buy a wake at all.
			//
			// THE PREDICATE BELOW MUST MATCH THE PRE-PARK RECHECK TERM FOR TERM. It did not once --
			// the recheck tested hiPri inboxes and laneWake and the wait loop did not -- so a hiPri
			// push could leave a worker blocked with a NON-EMPTY inbox. Inboxes are not stealable,
			// so nobody else could drain it and the task stranded until something unrelated woke
			// that exact worker.
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

				// (No park fiber here -- see the park block above for why one is the wrong shape
				// for a surplus worker.)

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
				// Offset by K for the same reason: "am I above the floor" is a question about
				// compute indices, and a reserved worker is not above the floor, it is beside it.
				// SAME SNAPSHOT AS THE PARK GATE BELOW. These two were separate loads sitting
				// twenty lines apart, deciding "may I call collapse" and "am I on the floor"
				// against potentially different bands -- the one pairing that must never disagree,
				// because a worker that answers "not above the floor" to the first and "not on the
				// floor" to the second parks without ever giving collapse its chance, and then
				// nobody above K+Fbase is awake to call it at all.
				// ---- SHED FROM A SPINNING FLOOR WORKER, NOT FROM ABOVE THE BASE ---------------
				//
				// This asked `q >= K + Fbase`, and that set can be entirely ASLEEP, which makes the
				// collapse uncallable exactly when it is needed. Measured, K=2 Fbase=2 F=6 on 31
				// workers: every worker in [4,31) had parked, so no thread satisfying the predicate
				// was running. The 25 ms idle settle recorded `collapse calls=0` -- not refused by
				// the hold, never CALLED -- and F sat at 6 for the rest of the run.
				//
				// The floor is the one band that CANNOT be asleep: it is defined as never parking.
				// So the shed has to be driven from inside it, [K, K+F), which includes the base
				// workers q=2 and q=3 that spin for the whole run. Under the old test those two
				// were the only threads guaranteed awake and the only ones excluded.
				//
				// A floor worker shedding the floor it is standing on is the intended direction,
				// not a hazard: collapse lowers a NUMBER and parks nobody. Workers above the new
				// F park through their own idle path on their next pass, which already refuses
				// while their own inbox is non-empty.
				//
				// STILL IN THE PARK BLOCK, not in the earlier recheck. Moving it there once made a
				// floor worker bail before ever reaching the call, dropping collapses from ~5M a
				// run to 162 and making the stuck floor happen every run instead of sometimes.
				if (advertisedCount == 0
				    && (size_t)qIndex >= bandsNow.k
				    && (size_t)qIndex <  bandsNow.k + bandsNow.f)
					TaskScheduler::CollapseAwakeFloorToBase();

				// A RESERVED hiPri WORKER NEVER PARKS EITHER, whatever the floor says. Reserving a
				// worker and then letting it sleep would give a completion the OS wake the whole
				// scheme exists to avoid -- it would be awake only when it happened to be busy.
				// ---- THE FLOOR IS [K, K+F). RESERVED WORKERS ARE NOT ON IT. ------------------
				//
				// F counts COMPUTE workers that never park, and it starts above the reserved band.
				// A reserved worker is governed by its own rule below, not by the floor -- treating
				// it as floor is what let floor growth walk into the I/O cores.
				//
				// A RESERVED WORKER PARKS when its own queues are empty. Its guarantee is "not
				// stuck inside a compute leaf", which reservation already provides; never sleeping
				// is a stronger and more expensive promise, so it is a separate flag, default off.
				// If the resume path ever measures a wake cost worth removing, turn it on then.
				// FLOOR MEMBERSHIP FOLLOWS LIVE K, and the sentence that used to sit here said the
				// opposite -- "follows the floor's BASE, not live K, see GetFloorBase". The code
				// below has always used live K, so the comment described a design that was
				// considered and not taken, and the next reader to trust it would have "fixed" the
				// code back to a fixed base.
				//
				// LIVE K IS RIGHT FOR A SLIDING RESERVED BAND: the floor is [K, K+F) by definition,
				// so when K moves the floor moves with it. A fixed base would leave the window
				// pinned while the band underneath it slid, which is how floor growth walked into
				// the I/O cores in the first place.
				const size_t floorBase = bandsNow.k;   // the pass snapshot -- see the collapse above
				const size_t floorNow  = bandsNow.f;
				// ---- A HOT WORKER DOES NOT PARK; IT STOPS BEING HOT -------------------------
				//
				// Under ADAPTIVE K (kmax > kmin) the reserved arm is unconditionally awake, and
				// the flag is not consulted. Two mechanisms were answering "the lane is idle" --
				// the worker by parking while still reserved, the controller by shedding K -- and
				// the worker's answer defeats the reservation the controller is maintaining: a
				// parked reserved core gives the next completion the OS wake the band exists to
				// remove, and it does so precisely because the lane went briefly quiet, which is
				// the band's steady state. Measured as the latency/hot inversion: hot 1.32x of
				// cold, every serial push landing mid-park-gauntlet. Idleness has ONE owner: the
				// controller sheds K, the leaving worker re-bands on its next pass, and only THEN
				// does that index park -- as a compute worker, not as a hot one.
				//
				// (Shedding "down to zero" -- kmin = 0 -- additionally needs a regrow signal at
				// K == 0, which is NoteLaneMiss, deliberately unwired until the static-K bench is
				// green. Until then SetHotWorkerRange floors kmin at 1 and the shed stops there.)
				//
				// Under STATIC K the flag still decides, exactly as the decoupling specified:
				// SetHotWorkers(k) buys reservation only, SetIoHotLane(k) buys the spin too, and
				// the parkable-reserved arm stays available as the A/B control.
				const bool onAwakeFloor =
					reservedForHiPri ? (TaskScheduler::ReservedNeverParks()
					                    || bandsNow.kmax > bandsNow.kmin)
					                 : ((size_t)qIndex >= floorBase
					                    && (size_t)qIndex < floorBase + floorNow);

				// ---- "SOMETHING IS ADVERTISED" IS NOT A REASON TO STAY AWAKE FOREVER ---------
				//
				// This used to be `advertisedCount == 0` alone, and that is what turned the steal
				// hint from a filter into an insomnia switch. During a ParallelFor the caller's lane
				// advertises for the life of the range, so advertisedCount is non-zero for EVERY
				// worker, none of them may park, and all 31 spin-probe every deque until the range
				// finishes. Counted, same work either side, machine-independent:
				//
				//     origin/main   2,480 probes   27.5% hit   3.6 probes/hit
				//     this loop   301,266 probes    0.36% hit   277 probes/hit
				//
				// 120x the probes for the same hits. Every one is a CAS on another deque's `top` --
				// the same line its owner touches in pop_bottom -- so the losers are not merely
				// wasting their own time, they are slowing down the workers that DID get work. It is
				// invisible on a heavy body and it is the entire cost of a cheap one.
				//
				// A hint means "there is work worth probing for". It cannot also mean "you may not
				// sleep": thirty workers racing for two splits will have twenty-eight losers, and a
				// loser has nothing useful left to do. So a worker that has missed
				// kBackoffMissThreshold steals in a row parks anyway -- which is what the old loop
				// did unconditionally, and why its hit rate was 27% instead of 0.4%.
				//
				// The floor is unaffected: a floor worker never parks either way, so the wake-free
				// dispatch path keeps its latency.
				// PARKING THE LOSERS WAS TRIED AND IT COST MORE THAN THE PROBES DID.
				// `(advertisedCount == 0 || consecutiveMisses >= kBackoffMissThreshold)` cut probes
				// 301,266 -> 8,740 and lifted the hit rate 0.36% -> 10.1% -- and made the rows
				// WORSE: heavy 20.4 -> 14.7, medium 14.7 -> 10.6, with the small-N crossovers
				// unmoved. A worker that has missed eight steals is not useless; it is one of thirty
				// racing for two splits, and it will win a later one. Parking it shrinks the
				// audience that heavy and medium actually use.
				//
				// The probe count is a real cost and a real difference from the old loop, but it is
				// a PROXY. Optimising it directly trades away the parallelism it was measuring.
				// ---- THE PARK MECHANISM IS A FLAG, AND THE NO-KERNEL ARM WAS DEAD ---------
				//
				// IdlePolicy::NoSleep had no callers left in this loop after the rewrite -- the
				// enum, the CLI flag and the docs all still existed, and the loop parked regardless.
				// So the one arm that avoids the kernel entirely was unreachable, which is also why
				// no measurement of it could be produced when it was asked for.
				//
				// WHAT THE TWO ARMS ACTUALLY DIFFER BY. A futex park blocks the THREAD: the core
				// goes back to the OS and getting it back is a kernel round trip, measured at ~5.5
				// us p50 on the reserved band (6.7 parked against 1.2 spinning). NoSleep never
				// enters the kernel: the worker keeps its core and a notify is a store the spinning
				// loop observes, which is nanoseconds.
				//
				// NEITHER IS FREE, which is why this is a policy and not a fix. A spinning worker
				// holds a core against every other thread in the process -- including the producer,
				// which is the bottleneck on a single-producer workload -- so the pool gets faster
				// at dispatch and slower at everything else.
				const bool noKernelPark =
					TaskScheduler::GetIdlePolicy() == TaskScheduler::IdlePolicy::NoSleep;
				if (advertisedCount == 0 && !onAwakeFloor && !noKernelPark) {
					// Clear the awake bit BEFORE blocking so a producer stops choosing this worker
					// and paying a wake for it. Setting it late is harmless; clearing it late is not.
					// ---- DID THE FLOOR GROW OVER ME SINCE THE GATE READ IT? ------------------
					//
					// The gate above tests !onAwakeFloor from live F, but F can rise between that
					// read and this block, and nothing re-asked. The worker then parks as a floor
					// worker, and the damage is not a slow thread: CollapseAwakeFloorToBase is only
					// ever called from an index at or above K+Fbase, so once the grown floor has
					// parked there is NOBODY AWAKE TO CALL IT and F can never shed. That is the
					// "F=12 after a 25 ms settle" report -- permanent, not slow.
					//
					// MEASURED before the check: 2, 12, 5 and 17 such parks in four consecutive
					// runs. After it: 0.
					//
					// HERE AND NOT IN THE PREDICATE ABOVE. The obvious place is the earlier recheck,
					// and it is wrong -- that runs before the collapse call site, so a floor worker
					// that bails there never gives the collapse its chance. Doing it there dropped
					// collapse calls from ~5,000,000 a run to 162 and made the stuck floor happen
					// every run instead of sometimes.
					{
						const TaskScheduler::Bands pb = TaskScheduler::GetBands();  // ONE load
						if ((size_t)qIndex >= pb.k && (size_t)qIndex < pb.k + pb.f) {
							workerState.store(WS_AWAKE, std::memory_order_seq_cst);
							continue;
						}
					}
					scheduler->SetAwake((size_t)qIndex, false);

					int sleeping = WS_SLEEPING;
					int expectedGoing = WS_GOING_TO_SLEEP;
					workerState.compare_exchange_strong(expectedGoing, WS_SLEEPING,
						std::memory_order_seq_cst, std::memory_order_relaxed);

					// ---- WHICH WORKERS ACTUALLY PARK -----------------------------------------
					//
					// Counted HERE, at the one place a worker commits to blocking, so the number
					// means "this index parked" and cannot be inferred from a band the caller
					// already believes in. A banner that prints K and F from the same atomics the
					// scheduler steers by proves nothing: it agrees with itself by construction.
					// Comparing DECLARED bands against OBSERVED parks is what makes the banner a
					// check rather than an echo -- a floor worker appearing here means the run is
					// junk no matter how good its numbers look.
					//
					// Relaxed and per-worker: no ordering is implied and nobody steers on it.
					parkCount.fetch_add(1, std::memory_order_relaxed);

					// ---- THE LAST GATE: A FLOOR MEMBER DOES NOT PROCEED INTO THE PARK -----------
					//
					// No longer a TEMP DIAG -- it graduated into the guard it was watching for.
					// This is the second grew-over-me re-read, one step later than `pb` above: pb
					// runs before the sleep commit, this runs after it, and the bands can move
					// between the two (with adaptive K they measurably do -- 1..6 counted per
					// bench run). The growth paths force-notify every index they slide the band
					// over, so a worker caught here WOULD be rescued microseconds after blocking
					// -- but recovering locally is cheaper than a kernel wake, and it makes the
					// counter mean something again: with this gate, a floor member simply never
					// completes a park, so any nonzero count the bench prints is a worker that
					// was caught HERE and turned around, and the "violation" a stuck run would
					// show is structurally impossible rather than merely improbable.
					//
					// ONE load, not GetFloorBase()+GetAwakeFloor() -- pairing a new K with an old
					// F once reported violations that never happened.
					{
						const TaskScheduler::Bands db = TaskScheduler::GetBands();
						if ((size_t)qIndex >= db.k && (size_t)qIndex < db.k + db.f) {
							TaskScheduler::NoteFloorPark();   // count the catch -- the bench reports it
							workerState.store(WS_AWAKE, std::memory_order_seq_cst);
							scheduler->SetAwake((size_t)qIndex, true);
							continue;
						}
					}

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
					// ---- NOTHING MAY BE PARKED WITH A FIBER IN HAND -------------------------
					//
					// A parked worker is off the run queue. If it still owned a fiber, that fiber
					// would be owned by a thread that cannot run it and unreachable to every thread
					// that could: its stack never unwinds, so its destructors never run, its
					// WaitGroup slot is never released and its hazard record is never returned. It
					// is the stranded-frame failure, arrived at from the idle path instead of the
					// teardown path.
					//
					// THE REASON THIS HOLDS IS NOT THE PARK, IT IS WHERE THE PARK SITS. Every exit
					// from OnFiberReturned clears currentFiber, and a fiber that waits does not
					// park the thread -- WaitFor switches it back to homeCtx and the worker returns
					// to this loop owning nothing. The park is only reachable from the search loop,
					// which runs on schedulerCtx.
					//
					// That was true of the removed park-fiber design too, but for a different and
					// weaker reason, so it is asserted rather than assumed: this is a structural
					// invariant of the hybrid, and the cost of it being wrong is silent.
					assert(currentFiber == nullptr &&
					       "worker about to park while still owning a fiber -- that frame can never "
					       "be resumed by anyone");
					assert(currentRunningTask == nullptr &&
					       "worker about to park while still holding a running task");

					// ---- CONDVAR ARM ---------------------------------------------------------
					//
					// The same predicate, evaluated under the worker's own lock. That is the point
					// of the comparison rather than an implementation detail: the WaitAddress arm
					// below re-reads four seq_cst flags and three inboxes on every pass BEFORE it
					// blocks, while this does the equivalent work inside the predicate the condvar
					// already has to evaluate under a lock it already has to take.
					//
					// The cost it pays back is on the NOTIFY side -- Wake() has to take this mutex
					// to close the lost-wakeup window -- and that mutex on the push path is exactly
					// what got condvar replaced in the first place. Which of the two dominates is
					// what this flag exists to measure, and it cannot be answered by an isolated
					// ping-pong: that harness has no push path.
					if (TaskScheduler::GetParkPrimitive() == TaskScheduler::ParkPrimitive::CondVar) {
						std::unique_lock<std::mutex> lk(parkMx);
						parkCv.wait(lk, [&] {
							return workerState.load(std::memory_order_seq_cst) != WS_SLEEPING
							    || !running.load(std::memory_order_acquire)
							    || hasQueuedWork.load(std::memory_order_seq_cst)
							    || laneWake.load(std::memory_order_seq_cst)
							    || !scheduler->hiPriInboxes[qIndex]->quiescent()
							    || !scheduler->loPriInboxes[qIndex]->quiescent()
							    || !scheduler->resumedInboxes[qIndex]->quiescent();
						});
					}
					else
					while (workerState.load(std::memory_order_seq_cst) == WS_SLEEPING

					       && running.load(std::memory_order_acquire)
					       && !hasQueuedWork.load(std::memory_order_seq_cst)
					       && !laneWake.load(std::memory_order_seq_cst)
					       && scheduler->hiPriInboxes[qIndex]->quiescent()
					       && scheduler->loPriInboxes[qIndex]->quiescent()
					       && scheduler->resumedInboxes[qIndex]->quiescent()) {
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
						// UNREACHABLE ON DARWIN IN A DEFAULT BUILD, because ParkPrimitiveDefault()
						// selects CondVar there and the condvar arm above returns before this loop.
						// This is what you get from JLIB_PARK=wait on a platform with no address
						// wait: a spin, not a wait -- slow, not wrong. Kept as the negative control
						// that shows what the condvar buys, and as the arm any future platform
						// lands in before it has a park of its own.
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

				// ---- HOW OFTEN A SPINNING WORKER GIVES THE CORE BACK ------------------------
				//
				// 1024 was a rate limit on a syscall, chosen when the only spinners were a couple of
				// floor workers. It is not a rate limit any more once the floor is large or the pool
				// is in NoSleep: each pass here scans three inboxes, the deque and the steal targets,
				// so 1024 passes is on the order of a hundred microseconds between yields, and a
				// pool of 31 of them will not let an unrelated runnable thread on.
				//
				// MEASURED, and it is not a small effect: the marl blocking row spawns a releaser
				// thread that busy-yields until its deadline, and with floor=31 or NoSleep it cannot
				// get scheduled to fire the signal -- 10 ms became 24-40 ms, WORSE than parking.
				// marl spins for a whole millisecond in the same benchmark and does not do this,
				// because its spin yields once per outer pass (~8192 nops + one steal), which is
				// microseconds apart, not hundreds of them. Its spin is polite; this one was not.
				//
				// NOT "spin before parking", which is rejected and stays rejected -- see
				// memory/spin-before-park-rejected.md and fast-spin-before-help-rejected.md. This
				// only changes how a worker that has ALREADY decided to stay awake behaves while it
				// waits, and it cannot make anyone park sooner or later.
				// ---- WHO OWNS THIS CORE DECIDES HOW LONG IT MAY BE HELD --------------------
				//
				// The two populations in this spin path are not the same kind of spinner, and until
				// now they were treated identically:
				//
				//   FLOOR and RESERVED workers are spinning BECAUSE THAT IS THE POLICY. Somebody
				//   asked for F workers that never park so work lands on a hot core. Their core is
				//   theirs; they go straight back to the search, rate-limited only by the periodic
				//   yield above.
				//
				//   EVERYONE ELSE is here only because something was advertised -- a transient
				//   reason, not a policy. That worker is a GUEST on the core: it should look, and
				//   then get out of the way promptly rather than holding it at the floor's cadence.
				//   It has not decided to park (nothing parks with work advertised); it goes around
				//   and re-decides, having given anything else runnable a chance to run first.
				//
				// This is what makes the yield interval safe to leave coarse for the floor: the
				// worker holding the core the longest is the one that was ASKED to.
				if (onAwakeFloor) {
					if ((++spinTick & TaskScheduler::GetSpinYieldMask()) == 0) std::this_thread::yield();
					else                                                       platform::CpuRelax();
					continue;
				}
				// A GUEST: brief pause, then hand the core back unconditionally.
				for (unsigned i = 0; i < kGuestSpin; ++i) platform::CpuRelax();
				std::this_thread::yield();
			}
		}
	}
	running.store(false, std::memory_order_release);
}

