#include "../include/Fiber.h"
#include "../include/T_Thread.h"
#include "../include/TaskScheduler.h"
using namespace T_Threads;
std::atomic<uint64_t> T_Threads::Fiber::idGenerator{ 0 };

// Defined in ContextSwitch.asm. The restore lands on this at a 16-aligned RSP; it
// 'call's the entry point we stash in RBX, which re-establishes the ABI 8-mod-16 entry.
extern "C" void FiberTrampoline();

void Fiber::Init(void(*entryPoint)())
{
	// 16-byte-align the very top of this fiber's stack.
	uintptr_t top = ((uintptr_t)((char*)stackBase + stackSize)) & ~(uintptr_t)0xF;
	uintptr_t* sp = (uintptr_t*)top;

	// Windows x64 ABI: a called function gets 32 bytes of shadow space ABOVE its return
	// address for its callees to spill register params. The trampoline 'call's the C++
	// entry, so reserve that shadow at the very top, inside this fiber's own stack --
	// otherwise the entry function writes past stackTop (next fiber's base => silent
	// corruption, or unmapped memory => write AV at the stack-region boundary).
	sp -= 4;                                 // 32 bytes shadow space

	// Return address consumed by ContextSwitch's final 'ret': the trampoline. It runs at
	// a 16-aligned RSP and 'call's the real entry (in RBX) to land it at ABI 8-mod-16.
	*(--sp) = (uintptr_t)&FiberTrampoline;

	// 8 callee-saved GPR slots. ContextSwitch pops them r15..rbx, so rbx (popped last)
	// is the highest slot -- we seed it with the entry point for the trampoline's
	// `call rbx`. The rest are zero; a fresh fiber has no meaningful GPR state.
	*(--sp) = (uintptr_t)entryPoint; // rbx
	*(--sp) = 0;                     // rbp
	*(--sp) = 0;                     // rdi
	*(--sp) = 0;                     // rsi
	*(--sp) = 0;                     // r12
	*(--sp) = 0;                     // r13
	*(--sp) = 0;                     // r14
	*(--sp) = 0;                     // r15

	// 8-byte dummy that realigns the XMM block to 16 -- mirrors ContextSwitch's
	// `sub rsp, 168` (= 160 XMM + 8 dummy). Without it ctx.rsp would be 8 mod 16 and
	// the restore's movdqa would #GP.
	*(--sp) = 0;

	// 160 bytes for non-volatile XMM6-15 (10 * 16). Restored with movdqa, so this block
	// -- and ctx.rsp -- must be 16-aligned. Zero-initialized; no incoming XMM state.
	for (int k = 0; k < 20; ++k) *(--sp) = 0; // 20 * 8 = 160 bytes

	ctx.rsp = (void*)sp; // 16-aligned base of the XMM block; ContextSwitch loads RSP here
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