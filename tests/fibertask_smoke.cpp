// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Minimal repro for the AArch64 fork-join segfault: the smallest program that runs a task ON A
// FIBER through the real scheduler.
//
//   clang++ -std=c++17 -O1 -g -I include tests/fibertask_smoke.cpp build/libScheduler.a -o smoke
//   ./smoke ; echo "exit $?"
//
// WHY THIS EXISTS: every other section of the bench uses CreateTask(+[](void*){}, nullptr), the
// function-pointer overload, which defaults to TaskType::Native and never touches a fiber. Only
// fork-join passes TaskType::Fiber, so it is the FIRST code in the whole bench to exercise
// Fiber::Init, the switch-in, FiberEntryWrapper, and the suspend path -- all of which are new on
// AArch64. The bench conflates all four; this separates them into two stages.
//
// STAGE 1 crashes  -> Fiber::Init / switch-in / FiberEntryWrapper. The fiber never even suspends.
// STAGE 1 passes,
// STAGE 2 crashes  -> the suspend/resume path (WaitFor from inside a fiber), not fiber startup.
// Both pass       -> fiber machinery is fine and the bug needs the bench's concurrency to show up;
//                    next lever is scale (many fibers at once), not the single-fiber path.
#include "../include/TaskScheduler.h"
#include "../include/GlobalFiberPool.h"
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <ucontext.h>

extern "C" void FiberTrampoline();

// Self-diagnosis instead of a debugger. lldb over ssh kept wedging the terminal, and everything
// worth knowing about this crash is in the register file at the moment of the fault -- so read it
// out of the signal's ucontext directly and print it next to the addresses it should match.
static void OnSegv(int, siginfo_t* si, void* ucv)
{
	ucontext_t* uc = (ucontext_t*)ucv;
	fprintf(stderr, "\n===================== SIGSEGV =====================\n");
	fprintf(stderr, "fault address      : %p\n", si->si_addr);
#if defined(__aarch64__)
	// bionic's aarch64 mcontext_t is struct sigcontext: regs[0..30], sp, pc, pstate.
	mcontext_t* m = &uc->uc_mcontext;
	fprintf(stderr, "pc                 : %016llx\n", (unsigned long long)m->pc);
	fprintf(stderr, "sp                 : %016llx\n", (unsigned long long)m->sp);
	fprintf(stderr, "x19 (entry slot)   : %016llx\n", (unsigned long long)m->regs[19]);
	fprintf(stderr, "x29 (frame ptr)    : %016llx\n", (unsigned long long)m->regs[29]);
	fprintf(stderr, "x30 (link reg)     : %016llx\n", (unsigned long long)m->regs[30]);
	fprintf(stderr, "x0                 : %016llx\n", (unsigned long long)m->regs[0]);
	fprintf(stderr, "x1                 : %016llx\n", (unsigned long long)m->regs[1]);
#endif
	// The comparison that actually answers the question: after the switch-in, x19 MUST equal
	// FiberEntryWrapper and pc should be inside it or inside FiberTrampoline. Printing the
	// expected values here means no addr2line and no symbol lookup is needed to read the result.
	void (*few)() = &JLib::GlobalFiberPool::FiberEntryWrapper;
	fprintf(stderr, "---- expected ----\n");
	fprintf(stderr, "&FiberTrampoline   : %p\n", (void*)&FiberTrampoline);
	fprintf(stderr, "&FiberEntryWrapper : %p\n", (void*)few);
	fprintf(stderr, "===================================================\n");
	fflush(stderr);
	_exit(139);
}

static void InstallCrashHandler()
{
	// SA_ONSTACK with an alternate stack is REQUIRED, not optional: if this fault turns out to be a
	// fiber stack overflow hitting the arena's guard page, there is no usable stack left to run a
	// handler on, and without this the handler would itself fault and we would learn nothing.
	// Was SIGSTKSZ*4. glibc 2.34 redefined SIGSTKSZ as a sysconf() call, so it is no longer a
	// constant expression and that line stopped compiling on every current distro -- which is
	// unfortunate for a file whose whole job is to be the AArch64 repro. The historical SIGSTKSZ is
	// 8-16 KiB, so a fixed 64 KiB is the same order and comfortably above MINSIGSTKSZ anywhere.
	static char altStack[64 * 1024];
	stack_t ss{};
	ss.ss_sp = altStack;
	ss.ss_size = sizeof(altStack);
	sigaltstack(&ss, nullptr);

	struct sigaction sa {};
	sa.sa_sigaction = OnSegv;
	sa.sa_flags = SA_SIGINFO | SA_ONSTACK;
	sigemptyset(&sa.sa_mask);
	sigaction(SIGSEGV, &sa, nullptr);
	sigaction(SIGBUS, &sa, nullptr);   // misaligned SP faults as SIGBUS on AArch64, not SIGSEGV
}

int main() {
	InstallCrashHandler();
    // None, not Ideal: keeps the bionic affinity shim out of the picture entirely so a crash here
    // cannot be blamed on it. Re-run with Ideal afterwards to test that separately.
    JLib::TaskScheduler::SetAffinityPolicy(JLib::TaskScheduler::AffinityPolicy::None);
    JLib::TaskScheduler::Init();
    JLib::TaskScheduler& sched = JLib::TaskScheduler::Instance();
    printf("scheduler up\n"); fflush(stdout);

    // ---------------------------------------------------------------- STAGE 1: one fiber, no suspend
    {
        JLib::WaitGroup wg;
        JLib::Task* t = sched.CreateTask([] {
            printf("  [stage 1: fiber task body ran]\n"); fflush(stdout);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { printf("STAGE 1 FAIL: CreateTask returned null\n"); return 1; }

        t->waitGroup = &wg;
        wg.n.fetch_add(1, std::memory_order_relaxed);
        printf("stage 1: pushing\n"); fflush(stdout);
        sched.Push(t);
        sched.WaitFor(wg);
        printf("STAGE 1 PASSED (fiber start, run, return)\n"); fflush(stdout);
    }

    // ---------------------------------------------------------------- STAGE 2: fiber that suspends
    // The task below calls WaitFor from INSIDE a fiber, which takes the WaitOnEventDirectArmed
    // suspend path rather than the main thread's spin loop. This is what fork-join does.
    {
        JLib::WaitGroup outer;
        JLib::Task* t = sched.CreateTask([&sched] {
            printf("  [stage 2: outer fiber running, creating child]\n"); fflush(stdout);
            JLib::WaitGroup inner;
            JLib::Task* child = sched.CreateTask([] {
                printf("    [stage 2: child ran]\n"); fflush(stdout);
            }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
            if (!child) { printf("STAGE 2 FAIL: child CreateTask null\n"); return; }

            child->waitGroup = &inner;
            inner.n.fetch_add(1, std::memory_order_relaxed);
            sched.Push(child);
            printf("  [stage 2: outer suspending in WaitFor]\n"); fflush(stdout);
            sched.WaitFor(inner);                 // <-- suspends THIS fiber
            printf("  [stage 2: outer resumed]\n"); fflush(stdout);
        }, false, JLib::FiberSize::Standard, JLib::TaskType::Fiber);
        if (!t) { printf("STAGE 2 FAIL: CreateTask returned null\n"); return 1; }

        t->waitGroup = &outer;
        outer.n.fetch_add(1, std::memory_order_relaxed);
        printf("stage 2: pushing\n"); fflush(stdout);
        sched.Push(t);
        sched.WaitFor(outer);
        printf("STAGE 2 PASSED (fiber suspend + resume)\n"); fflush(stdout);
    }

    printf("\nALL STAGES PASSED\n");
    return 0;
}
