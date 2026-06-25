#include "../include/Fiber.h"
#include "../include/T_Thread.h"
#include "../include/TaskScheduler.h"
using namespace T_Threads;
std::atomic<uint64_t> T_Threads::Fiber::idGenerator{ 0 };
void Fiber::Init(void(*entryPoint)())
{
	// 16-byte-align the very top of this fiber's stack.
	uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
	uintptr_t* sp = (uintptr_t*)top;

	// Windows x64 ABI: the CALLER must leave 32 bytes of shadow space ABOVE the
	// return address for the callee to spill its register params. When
	// ContextSwitch 'ret's into entryPoint, that shadow space is whatever sits
	// above the entry RSP. Reserve it HERE, inside this fiber's own stack --
	// otherwise the entry function writes past stackTop, which is either the
	// next fiber's base (silent corruption) or, for the last fiber, unmapped
	// memory (0xC0000005 write AV at the stack-region boundary).
	sp -= 4;                              // 32 bytes shadow space (owned by this fiber)
	*(--sp) = 0;                          // landing slot: entry RSP points here (unused)
	*(--sp) = (uintptr_t)entryPoint;      // return address consumed by ContextSwitch 'ret'

	// 8 callee-saved registers consumed by ContextSwitch's pops (r15 is lowest).
	*(--sp) = 0; // rbx
	*(--sp) = 0; // rbp
	*(--sp) = 0; // rdi
	*(--sp) = 0; // rsi
	*(--sp) = 0; // r12
	*(--sp) = 0; // r13
	*(--sp) = 0; // r14
	*(--sp) = 0; // r15

	// 160 bytes for non-volatile XMM6-15 (10 * 16). ContextSwitch restores these
	// (movdqu) and then does `add rsp,160` BEFORE the pops, so this block must sit
	// below the GPR slots and ctx.rsp must point at its base. Zero-initialized;
	// a fresh fiber has no meaningful incoming XMM state.
	for (int k = 0; k < 20; ++k) *(--sp) = 0; // 20 * 8 = 160 bytes

	ctx.rsp = (void*)sp; // ContextSwitch loads RSP here (base of the XMM block)
}

void Fiber::CoYield() {
	// Record intent and switch out.
	this->status.store(FiberStatus::WANTS_YIELD, std::memory_order_release);
	ContextSwitch(&this->ctx, this->homeCtx);
}

void Fiber::Suspend() {
	// Record intent and switch out. 
	this->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	ContextSwitch(&this->ctx, this->homeCtx);
}
void Fiber::Resume() {
	// Robust wake that closes the lost-wakeup window. Two parkable states:
	//  - SUSPENDED: the worker already saved the context and parked us -> CAS to READY
	//    and re-queue (Requeue: no pendingTasks bump, since we were never decremented).
	//  - WANTS_SUSPEND: we asked to suspend but the worker hasn't published SUSPENDED
	//    yet (context maybe not saved) -> we must NOT resume now. Flip to SUSPEND_SIGNALED
	//    so the worker's park step wakes us once the context is safely saved.
	// Idempotent: a second Resume (or a state we don't recognize) is a no-op.
	while (true) {
		FiberStatus s = status.load(std::memory_order_acquire);
		if (s == FiberStatus::SUSPENDED) {
			FiberStatus exp = FiberStatus::SUSPENDED;
			if (status.compare_exchange_strong(exp, FiberStatus::READY, std::memory_order_acq_rel))
				TaskScheduler::Instance().Requeue(this->owningTask);
			return;
		}
		else if (s == FiberStatus::WANTS_SUSPEND) {
			FiberStatus exp = FiberStatus::WANTS_SUSPEND;
			if (status.compare_exchange_strong(exp, FiberStatus::SUSPEND_SIGNALED, std::memory_order_acq_rel))
				return;                 // worker will wake it when it parks
			// CAS lost to the worker parking us (now SUSPENDED) -> loop and take that path
		}
		else {
			// RUNNING / READY / SUSPEND_SIGNALED / DEAD: not resumable right now (already
			// signaled, not waiting, or running). Nothing to do.
			return;
		}
	}
}