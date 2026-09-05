// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Context.h"
#include "TsanFiber.h"   // tsanFiber below; no-ops without the sanitizer
#include "platform.h"
#include "Task.h"
#include "Epochs.h"    // JLIB_EPOCH_CHECK_NO_GUARD -- the suspend paths below assert on it
#include <atomic>
#include <cstdint>
// Defined in the platform ContextSwitch assembly (src/win32/ContextSwitch.asm, its aarch64 twin,
// or src/posix/<arch>/ContextSwitch.S). The restore lands on this at a 16-aligned SP; it calls the
// entry point Init stashed in the ABI's designated register (rbx on x86-64, x19 on AArch64),
// which re-establishes the ABI entry alignment. Declared at namespace scope with C linkage so
// Fiber::Init below can take its address without pulling in a platform header.
extern "C" void FiberTrampoline();
namespace JLib {

	// ---- WHAT Fiber REACHES UPWARD FOR, DECLARED SO THE BODIES CAN LIVE HERE -----------------
	//
	// Fiber.cpp IS GONE. Every definition is in this header so the switching path has no call in
	// it -- and a caller only needs to include Fiber.h, which is what WaitGroup.cpp and
	// DirectEvent.h already do. The two things Fiber cannot see from the bottom layer are
	// declared here and defined in TaskScheduler.cpp:
	//
	//   RequeueFromFiber      Resume() re-queues through the scheduler. This was ALWAYS a
	//                         cross-TU call (Requeue is a TaskScheduler member defined in the
	//                         .cpp), so nothing is lost by keeping it one -- what is gained is
	//                         that Fiber.h no longer needs TaskScheduler.h, so it stays the
	//                         bottom layer and the include direction is not inverted.
	//   TsanSwitchToScheduler ONLY under JLIB_TSAN. In an ordinary build the annotation compiles
	//                         to nothing, so CoYield and Suspend contain no call at all; a TSan
	//                         build takes one, where it cannot matter.
	// Task is declared by Task.h, included above -- no forward declaration needed here.
	namespace detail {
#if defined(JLIB_TSAN)
		void TsanSwitchToScheduler() noexcept;
#endif
	}
	enum class FiberStatus {
		READY,         // In a work queue, waiting to be run/stolen
		RUNNING,       // Currently executing on a worker
		WANTS_YIELD,   // Fiber asked to yield; worker re-queues it AFTER its ctx is saved
		WANTS_SUSPEND, // Fiber asked to suspend; worker marks SUSPENDED after its ctx is saved
		SUSPEND_SIGNALED, // A signal/Resume raced in during WANTS_SUSPEND; worker wakes it instead of parking
		SUSPENDED,     // Parked, not queued; only now may Resume() make it READY + re-queue
		DEAD           // Finished, pending cleanup/reclamation
	};

	struct FiberDebt {
		FiberDebt* next = nullptr;
		void*      obj  = nullptr;
		void     (*release)(void*) noexcept = nullptr;
		static constexpr size_t kAnyHolder = (size_t)-1;
		size_t holder = kAnyHolder;
	};

	namespace detail {
		void ReleaseFiberSlots(void** slots, size_t n) noexcept;
		void HandOffFiberDebts(FiberDebt* head) noexcept;
	}

	// ONE CACHE LINE PER FIBER BOUNDARY, NOT SIXTEEN BYTES.
	//
	// GlobalFiberPool holds `std::vector<Fiber> fibers[kClassCount]`, so fibers are CONTIGUOUS and
	// the stride is sizeof(Fiber). At alignas(16) that stride was 240 -- 3.75 cache lines -- and a
	// stride that is not a multiple of 64 guarantees neighbours share a line. The collision is not
	// theoretical and it is on the worst possible pair: fiber k's tail holds owedKinds, status and
	// localEpoch (offsets 216-232), and fiber k+1 begins with `ctx` at 240, all inside the single
	// 64-byte line [192, 256).
	//
	//   `status` is written by whichever worker suspends or resumes fiber k -- often not its own.
	//   `ctx` is the saved RSP, written by ContextSwitch on EVERY switch of fiber k+1.
	//
	// So two unrelated fibers ping-pong one line between two cores, on the hottest word the runtime
	// has, for no reason but the size of the struct.
	//
	// THE COST IS NOT THE 16 BYTES OF PADDING. 240 -> 256 is +6.7% on the struct and reads badly
	// until it is measured against what a fiber ACTUALLY costs: a Standard fiber carries a 64 KiB
	// stack region (60 KiB usable + one guard page), so the struct is 0.37% of it and the padding
	// is 0.024%. Two ten-thousandths of a fiber to stop a false-sharing pair.
	//
	// This also matches every other hot structure in the tree -- TaskDeque, SlabPool's counters,
	// RetryStats -- which already align on platform::kCacheLine. Fiber was the outlier.
	//
	// The 16 was never about the struct anyway: stack tops are aligned explicitly at the three
	// InitStack sites (`& ~(uintptr_t)0xF`), so nothing here depended on alignas(16).
	struct alignas(platform::kCacheLine) Fiber {
		Context ctx;
		uint64_t id;
		void* stackBase;
		size_t stackSize;
		StackClass stackClass = StackClass::Standard;
		size_t poolIndex = SIZE_MAX;
		Task* owningTask = nullptr; // The task currently running on this fiber
		Context* homeCtx = nullptr; // Scheduler ctx to return to; the worker sets this before each switch-in
		size_t homeWorker = SIZE_MAX;

		static constexpr size_t kLocalSlots = 8;
		void* local[kLocalSlots] = {};

		FiberDebt* debts = nullptr;

#if defined(JLIBSCHED_REQUEUE_TRACE)
		size_t lastPlacedOn = SIZE_MAX;
#endif

		Fiber* nextWaiter = nullptr;
		Fiber* cleanupNext = nullptr;
		
		void* tsanFiber = nullptr;

		
		static constexpr size_t kCreditorWords = 6;   // 384 holders
		std::atomic<uint64_t> creditors[kCreditorWords] = {};

		inline void NoteCreditor(size_t worker) {
			if (worker >= kCreditorWords * 64) return;   // refuse, do not wrap: a wrapped index
			                                             // would silently bill the wrong worker
			creditors[worker >> 6].fetch_or(1ull << (worker & 63), std::memory_order_release);
		}

		inline size_t TakeCreditor() {
			for (size_t w = 0; w < kCreditorWords; ++w) {
				uint64_t cur = creditors[w].load(std::memory_order_acquire);
				while (cur) {
					const unsigned b = platform::CountTrailingZeros64(cur);
					// CAS rather than fetch_and: two threads may drain concurrently during a
					// teardown sweep, and both must not be handed the same creditor.
					if (creditors[w].compare_exchange_weak(cur, cur & (cur - 1),
							std::memory_order_acq_rel, std::memory_order_acquire))
						return w * 64 + b;
					// cur was reloaded by the failed CAS; re-examine it rather than restarting.
				}
			}
			return SIZE_MAX;
		}

		
		enum OwedKind : uint32_t {
			kOwesNothing = 0,
			kOwesSlab    = 1u << 0,   // RESERVED, unused -- see above. Do not set it for memory.
			kOwesEpoch   = 1u << 1,
			kOwesHazard  = 1u << 2,
		};
		std::atomic<uint32_t> owedKinds{ kOwesNothing };

		inline void NoteOwed(uint32_t kinds) { owedKinds.fetch_or(kinds, std::memory_order_release); }
		inline uint32_t Owed() const { return owedKinds.load(std::memory_order_acquire); }

		// THE GATE ON THE WHOLE CLEANUP CHAIN. Creditors alone are not a reason to run it -- being
		// picked up is not a debt.
		inline bool OwesCleanup() const { return Owed() != kOwesNothing; }

		inline bool HasCreditors() const {
			for (size_t w = 0; w < kCreditorWords; ++w)
				if (creditors[w].load(std::memory_order_acquire)) return true;
			return false;
		}

		// Drop every creditor without running cleanup. FOR RECYCLE ONLY, and only once the chain has
		// drained -- a fiber returning to the pool is a NEW fiber and must owe nobody. Calling this
		// with debts outstanding does not lose a task, it loses a RELEASE: the COM apartment or
		// handle those creditors were holding is never given back, and nothing reports it.
		inline void ClearCreditors() {
			for (size_t w = 0; w < kCreditorWords; ++w)
				creditors[w].store(0, std::memory_order_release);
		}

		// ---- EVERYTHING A RECYCLED FIBER MUST NOT CARRY, IN ONE PLACE --------------------------
		//
		// A recycled fiber IS a new fiber. Anything left over from its last life is state the next
		// task inherits without asking for it.
		//
		// ONE FUNCTION, NEXT TO THE FIELDS, RATHER THAN SCRUBS AT THE RETURN SITE. GlobalFiberPool::
		// ReturnBatch used to clear localEpoch inline and nothing else -- and that line exists
		// BECAUSE a fiber once went back to the pool still announced at an old epoch, which is an
		// ABA on the reclaimer. A per-field list at the call site is correct exactly until someone
		// adds a field, and the person adding the field is not looking at the return path. Here,
		// adding a member and forgetting to reset it means the two are adjacent on screen.
		//
		// WHAT IS DELIBERATELY *NOT* RESET: poolIndex (dense, stable, and the identity every table
		// in the system is keyed by -- see Event's perfect hash), id, stackBase, stackSize, ctx and
		// the arena pointers. Those describe the SLOT, not the occupant, and clearing them would
		// unmake the fiber rather than free it.
		inline void ResetForReuse() {
			ClearCreditors();
			// AND THE KINDS WITH THEM. A recycled fiber carrying a stale kind would send its next
			// occupant's death down the cleanup chain to release something it never acquired. This
			// is the exact field the "one reset list, next to the members" note below exists for --
			// it was added after that rule, and the rule is why it is here rather than forgotten.
			owedKinds.store(kOwesNothing, std::memory_order_release);
			// SIZE_MAX is "not in an epoch". The original inline scrub, kept for its original
			// reason: a slot still announced pins the reclaimer at a dead epoch.
			localEpoch.store(SIZE_MAX, std::memory_order_release);
			// SIZE_MAX is "not bound". A stale value here names a worker the next occupant never
			// ran on, which under pinning is a resume aimed at the wrong thread.
			homeWorker = SIZE_MAX;
			owningTask = nullptr;
			homeCtx    = nullptr;
			// Owned by whichever primitive the fiber was parked on. A survivor means the next
			// occupant appears to be queued on a wait list it never joined.
			nextWaiter = nullptr;
			// FIBER-LOCAL SLOTS ARE THE OCCUPANT'S, NEVER THE SLOT'S. A recycled fiber carrying
			// live pointers would hand the next occupant another task's state -- and it would look
			// like working code, because a stale pointer to a still-allocated object reads fine.
			// This is the same rule owedKinds and nextWaiter above are here for, and it is the one
			// most likely to be forgotten, because unlike those two nothing in the library ever
			// reads these: only the application does, so only the application would see it break.
			//
			// THE LIBRARY DOES NOT FREE WHAT A SLOT POINTS AT. It cannot -- it has no type. A slot
			// holding an owning pointer must be released by the task that put it there, before the
			// task ends. Clearing here prevents a stale READ, not a leak.
			// A SLOT-DELETER PASS USED TO RUN HERE, freeing slots that had declared how via
			// FiberRegistry::SetSlotDeleter. It was removed with that API: it is a deferred free
			// running at a moment when freeing was already safe, which is a third reclamation
			// scheme earning nothing over the two that exist. The rule above is the whole contract
			// now -- the task that put an owning pointer in a slot releases it before the task
			// ends, and this only prevents a stale read.
			// ONE CLEAR, NOT TWO. ReleaseFiberSlots used to consult the deleter table and free the
			// owning slots, and this loop then zeroed the array regardless. With every slot borrowed
			// the two steps do the same thing, so only the seam remains.
			detail::ReleaseFiberSlots(local, kLocalSlots);

			// DEBTS BEFORE THE SLOTS ARE FORGOTTEN, and the head is cleared BEFORE the walk: a
			// release that somehow registers another debt on this fiber must not link onto a list
			// that is mid-walk. Everything registered after this point belongs to the next
			// occupant, which is the correct place for it to go.
			//
			// EVERY REMAINING DEBT IS DISCHARGED HERE, holder or not. By the time a fiber is being
			// recycled its creditor chain has drained -- AdvanceCleanup only recycles when
			// TakeCreditor returns SIZE_MAX -- so an affine debt still on this list is one nobody
			// claimed, and dropping it silently would leak the resource it names. Running it late on
			// the wrong thread is the lesser wrong of the two, and it is loud in a debugger; leaking
			// is neither.
			if (debts) {
				detail::HandOffFiberDebts(debts);
				debts = nullptr;
			}
			status.store(FiberStatus::READY, std::memory_order_release);
		}
		void* operator new(std::size_t) = delete;
		void* operator new[](std::size_t) = delete;
		void  operator delete(void*) = delete;
		void  operator delete[](void*) = delete;

		std::atomic<FiberStatus>  status;
		// EBR participation slot. SIZE_MAX == "not in an epoch". The fiber is the unit
		// that migrates across workers, so the slot lives here (not on the thread).
		// Default member init covers the move ctor too (a moved/fresh fiber is not in an
		// epoch), so the move ctor doesn't need to mention it.
		std::atomic<size_t> localEpoch{ SIZE_MAX };
		// INLINE STATIC (C++17): the out-of-line definition lived in Fiber.cpp, which is gone --
		// every Fiber definition is now in a header so the switching path has no call in it.
		inline static std::atomic<uint64_t> idGenerator{ 0 };
		void (*taskFunction)();
		Fiber() : stackBase(nullptr), stackSize(0), taskFunction(nullptr), status(FiberStatus::READY), id(idGenerator.fetch_add(1, std::memory_order_relaxed)) {

		}
		Fiber(Fiber&& other) noexcept
			: ctx(other.ctx), stackBase(other.stackBase), stackSize(other.stackSize),
			  taskFunction(other.taskFunction), status(other.status.load(std::memory_order_relaxed)), id(idGenerator.fetch_add(1, std::memory_order_relaxed)) {}
		Fiber& operator=(Fiber&&) = delete;
		Fiber(const Fiber&) = delete;
		Fiber& operator=(const Fiber&) = delete;
		// NOT inline: the body is in src/win32/FiberInit.cpp (and the posix twin), next to the
		// ContextSwitch assembly whose restore frame it has to match. An `inline` declaration needs
		// its definition in every TU that calls it, so marking it inline while the body sits in one
		// .cpp gives an unresolved external from Thread.obj -- which is exactly what it did.
		// ---- Fiber::Init -- THE FRAME ContextSwitch RESTORES. THREE ABIs, ONE DEFINITION SITE ----
		//
		// This was three separate FiberInit.cpp files. They are here now because keeping ONE
		// definition reachable was costing three separate safeguards in the build, all of them
		// guarding the same hazard: two definitions of Fiber::Init both land in Scheduler.lib and
		// static-archive linking picks whichever member comes FIRST, with no duplicate-symbol
		// error. That already happened -- an AArch64 build seeding an x86-64 stack frame, which
		// faults on the first switch-in with nothing pointing at the cause. The three guards were
		// a CMake REMOVE_ITEM, a cross-directory list(APPEND), and a configure-time FATAL_ERROR on
		// a stale file. A compile-time #if makes the whole class unrepresentable.
		//
		// THE SPLIT IS BY ABI, NOT BY OS, and that is why there are three arms and not four:
		// Windows-on-ARM64 and AAPCS64 agree on the callee-saved set (x19-x28, x29, x30, low 64
		// bits of v8-v15) and therefore on the frame, so ONE arm serves both. Only x86-64 splits,
		// because Win64 and SysV genuinely differ.
		//
		// EACH ARM IS ONE CONTRACT WITH ITS ContextSwitch. Init writes what the restore reads back,
		// register for register and slot for slot; disagree and the failure is a wild jump, not a
		// compile error. The assembly no longer sits in the same directory, so the per-slot
		// comments below are now the only thing holding the two halves in step -- they earn their
		// keep more here than they did next to the .asm, not less.
#if defined(__aarch64__) || defined(_M_ARM64)
		// ---- AAPCS64 (Linux AND Windows on ARM64): 176-byte frame -----------------------------
		void Init(void (*entryPoint)()) {
			uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
			uintptr_t* sp = (uintptr_t*)top;
			*(--sp) = (uintptr_t)&FiberTrampoline;
			*(--sp) = 0;                     // x29 (FP): zero terminates a backtrace cleanly at the fiber base
			*(--sp) = 0;                     // x28
			*(--sp) = 0;                     // x27
			*(--sp) = 0;                     // x26
			*(--sp) = 0;                     // x25
			*(--sp) = 0;                     // x24
			*(--sp) = 0;                     // x23
			*(--sp) = 0;                     // x22
			*(--sp) = 0;                     // x21
			*(--sp) = 0;                     // x20
			*(--sp) = (uintptr_t)entryPoint; // x19: the trampoline's 'blr x19' target
			*(--sp) = 0;
			*(--sp) = 0;
			for (int i = 0; i < 8; ++i)       // low 64 bits of v8-v15
				*(--sp) = 0;
			ctx.rsp = (void*)sp;
		}
#elif defined(_WIN32)
		// ---- Windows x64: 272-byte frame ------------------------------------------------------
		void Init(void (*entryPoint)()) {
			// 16-byte-align the very top of this fiber's stack.
			uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
			uintptr_t* sp = (uintptr_t*)top;

			// Windows x64 ABI: a called function gets 32 bytes of shadow space ABOVE its return
			// address for its callees to spill register params. The trampoline 'call's the C++
			// entry, so reserve that shadow at the very top, inside this fiber's own stack --
			// otherwise the entry function writes past stackTop (next fiber's base => silent
			// corruption, or unmapped memory => write AV at the stack-region boundary).
			// SysV has no shadow space, which is why the POSIX arm does not do this.
			sp -= 4;                                 // 32 bytes shadow space

			// Return address consumed by ContextSwitch's final 'ret': the trampoline. It runs at
			// a 16-aligned RSP and 'call's the real entry (in RBX) to land it at ABI 8-mod-16.
			*(--sp) = (uintptr_t)&FiberTrampoline;

			// 8 callee-saved GPR slots. ContextSwitch pops them r15..rbx, so rbx (popped last)
			// is the highest slot -- seeded with the entry point for the trampoline's `call rbx`.
			// RDI and RSI are callee-saved on Windows and NOT on SysV -- that difference is why
			// the POSIX frame has six GPR slots here rather than eight.
			*(--sp) = (uintptr_t)entryPoint; // rbx
			*(--sp) = 0;                     // rbp
			*(--sp) = 0;                     // rdi
			*(--sp) = 0;                     // rsi
			*(--sp) = 0;                     // r12
			*(--sp) = 0;                     // r13
			*(--sp) = 0;                     // r14
			*(--sp) = 0;                     // r15

			// 8-byte slot that realigns the XMM block to 16 -- mirrors ContextSwitch's
			// `sub rsp, 168` (= 160 XMM + 8). Without it ctx.rsp would be 8 mod 16 and the
			// restore's movdqa would #GP. ContextSwitch also stashes the FP control state here:
			// MXCSR at [base+160] (low 4 bytes), x87 FCW at [base+164] (next 2). Seed the ABI
			// defaults so the first switch-in's ldmxcsr/fldcw load a sane masked state instead
			// of garbage: MXCSR 0x1F80 (all FP exceptions masked, round-to-nearest), FCW 0x037F.
			*(--sp) = 0x0000037F00001F80ULL;

			// 160 bytes for non-volatile XMM6-15 (10 * 16). Restored with movdqa, so this block
			// -- and ctx.rsp -- must be 16-aligned. Zero-initialized; no incoming XMM state.
			// Every XMM register is CALLER-saved under SysV, so this block does not exist there.
			for (int k = 0; k < 20; ++k) *(--sp) = 0; // 20 * 8 = 160 bytes

			ctx.rsp = (void*)sp; // 16-aligned base of the XMM block; ContextSwitch loads RSP here
		}
#else
		// ---- System V AMD64 (Linux x86-64): 64-byte frame -------------------------------------
		//
		// Every difference from the Windows arm is an ABI difference:
		//   - no 32-byte shadow space (a Win64 requirement with no SysV equivalent)
		//   - six callee-saved GPRs, not eight: RDI and RSI are ARGUMENT registers here
		//   - no XMM block at all: every XMM register is caller-saved under SysV
		//
		// Layout at ctx.rsp, low to high -- must match ContextSwitch.s exactly:
		//   +0  MXCSR (4) + x87 CW (2) + 2 pad     +8  r15   +16 r14   +24 r13
		//   +32 r12                                +40 rbx   +48 rbp   +56 return address
		void Init(void (*entryPoint)()) {
			// 16-byte-align the very top of this fiber's stack.
			uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
			uintptr_t* sp = (uintptr_t*)top;

			// Return address consumed by ContextSwitch's final 'ret': the trampoline. It runs at a
			// 16-aligned RSP and 'call's the real entry (in RBX) to land it at ABI 8-mod-16.
			*(--sp) = (uintptr_t)&FiberTrampoline;

			// Six callee-saved GPR slots, in the order ContextSwitch pops them (r15 first, rbp
			// last). rbp is therefore the highest slot and rbx the second highest -- rbx carries
			// the entry point for the trampoline's `call rbx`.
			*(--sp) = 0;                     // rbp
			*(--sp) = (uintptr_t)entryPoint; // rbx
			*(--sp) = 0;                     // r12
			*(--sp) = 0;                     // r13
			*(--sp) = 0;                     // r14
			*(--sp) = 0;                     // r15

			// FP control state, and the 8 bytes that bring the frame to 16-byte alignment: MXCSR
			// in the low 4, x87 FCW in the next 2. ABI defaults, so the first switch-in's
			// ldmxcsr/fldcw load a sane masked state rather than garbage. Identical encoding to
			// the Windows slot.
			*(--sp) = 0x0000037F00001F80ULL;

			// 8 slots * 8 bytes = 64, so a 16-aligned top leaves ctx.rsp 16-aligned too.
			ctx.rsp = (void*)sp;
		}
#endif

		inline void CoYield() {
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::CoYield");
			// Record intent and switch out.
			this->status.store(FiberStatus::WANTS_YIELD, std::memory_order_release);
#if defined(JLIB_TSAN)
			detail::TsanSwitchToScheduler();
#endif
			ContextSwitch(&this->ctx, this->homeCtx);
		}

		inline void Suspend() {
			JLIB_EPOCH_CHECK_NO_GUARD("Fiber::Suspend");
			// Record intent and switch out.
			this->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
#if defined(JLIB_TSAN)
			detail::TsanSwitchToScheduler();
#endif
			ContextSwitch(&this->ctx, this->homeCtx);
		}
		inline bool ResumeQueueless() {
			while (true) {
				FiberStatus s = status.load(std::memory_order_acquire);
				if (s == FiberStatus::SUSPENDED) {
					FiberStatus exp = FiberStatus::SUSPENDED;
					return status.compare_exchange_strong(exp, FiberStatus::READY, std::memory_order_acq_rel);
				}
				else if (s == FiberStatus::WANTS_SUSPEND) {
					FiberStatus exp = FiberStatus::WANTS_SUSPEND;
					if (status.compare_exchange_strong(exp, FiberStatus::SUSPEND_SIGNALED, std::memory_order_acq_rel))
						return false;           // worker will wake it when it parks
					// CAS lost to the worker parking us (now SUSPENDED) -> loop and take that path
				}
				else {
					// RUNNING / READY / SUSPEND_SIGNALED / DEAD: not resumable right now (already
					// signaled, not waiting, or running). Nothing to do.
					return false;
				}
			}
		}
		// NOT `inline`. Both are DEFINED in TaskScheduler.cpp, where Thread is complete -- an inline
		// function must be defined in every TU that uses it, so `inline` on a declaration whose only
		// definition lives in one .cpp is an unresolved external. Fiber.h stays a LEAF: Event.h and
		// DirectEvent.h call these from their own inline bodies, so anything Fiber.h includes is
		// included by every consumer of the event headers -- which is why #include "Thread.h" here
		// closed a cycle (Thread.h includes Fiber.h) and left Fiber incomplete in both.
		void Resume();
		static void RequeueResumedBatch(Task** tasks, size_t n, Lane lane);

		// Safety check for the work-stealer
		inline bool IsReady() const { return status == FiberStatus::READY; }
	};
} // namespace JLib
