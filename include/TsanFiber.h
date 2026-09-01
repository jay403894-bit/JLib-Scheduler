// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// TELLING THREAD SANITIZER THAT FIBERS EXIST.
//
// == WHY THIS IS NOT OPTIONAL FOR A FIBER SCHEDULER ==
//
// TSan tracks happens-before PER OS THREAD. A ContextSwitch swaps the stack out from under it, so
// without these annotations two fibers running on one worker are attributed to ONE thread. Both
// directions of error follow:
//
//   MISSED RACES   two fibers that genuinely race on the same worker look like sequential code to
//                  a per-thread model, so a real bug reports clean.
//   FALSE RACES    accesses correctly ordered by a fiber handoff look concurrent, because the
//                  handoff is invisible.
//
// A clean TSan run on an unannotated fiber scheduler therefore means NOTHING, in either direction.
// That is the same shape as the ASan-plus-fibers result recorded in the notes, where
// stack-use-after-return findings turned out to be shadow leftovers from fiber reuse -- the wrong
// instrument produces confident output about a question it cannot see.
//
// == THE MODEL ==
//
// TSan's fiber API is small and maps directly onto what this scheduler already does:
//
//   __tsan_get_current_fiber()   the fiber TSan thinks it is on -- for a worker, its SCHEDULER
//                                context, which is a fiber from TSan's point of view even though
//                                this codebase does not call it one.
//   __tsan_create_fiber()        a handle per JLib::Fiber, made once with the pool. Fibers are
//                                never destroyed here (the pool reserves and leaks), so there is
//                                no matching destroy on the normal path.
//   __tsan_switch_to_fiber(f,0)  called BEFORE the real ContextSwitch, so TSan's notion of "who is
//                                running" changes at the same instant the stack does.
//
// FLAG 0, NOT no_sync, AND THAT IS DELIBERATE. Passing __tsan_switch_to_fiber_no_sync would tell
// TSan the two fibers are NOT ordered by the switch. They are: a fiber handoff is a real
// synchronisation edge on this worker, and claiming otherwise would manufacture exactly the false
// races this header exists to prevent.
//
// == COST WHEN OFF ==
//
// Nothing. Every function below compiles to an empty body and the handles are null pointers, so a
// shipping build carries no field reads and no calls. `JLIB_TSAN` is defined by CMake only when
// -DJLIBSCHED_TSAN=ON, rather than sniffing __SANITIZE_THREAD__ / __has_feature, so GCC and Clang
// behave identically and the switch is in one place.

#pragma once

#if defined(JLIB_TSAN)
extern "C" {
	void* __tsan_get_current_fiber(void);
	void* __tsan_create_fiber(unsigned flags);
	void  __tsan_destroy_fiber(void* fiber);
	void  __tsan_switch_to_fiber(void* fiber, unsigned flags);
	void  __tsan_set_fiber_name(void* fiber, const char* name);
}
#endif

namespace JLib {
namespace tsan {

#if defined(JLIB_TSAN)
	inline constexpr bool kEnabled = true;

	inline void* CreateFiber()      { return __tsan_create_fiber(0); }
	inline void* CurrentFiber()     { return __tsan_get_current_fiber(); }
	inline void  Destroy(void* f)   { if (f) __tsan_destroy_fiber(f); }
	inline void  Name(void* f, const char* n) { if (f) __tsan_set_fiber_name(f, n); }

	// NULL-TOLERANT ON PURPOSE. A fiber can be entered before its handle exists (a pool built
	// before TSan state is set up) and a switch-back can happen on a thread that is not a worker.
	// Skipping the annotation loses fidelity for that one switch; passing null to the runtime is
	// undefined. Given the choice, lose the edge.
	inline void  SwitchTo(void* f)  { if (f) __tsan_switch_to_fiber(f, 0); }
#else
	inline constexpr bool kEnabled = false;

	inline void* CreateFiber()      { return nullptr; }
	inline void* CurrentFiber()     { return nullptr; }
	inline void  Destroy(void*)     {}
	inline void  Name(void*, const char*) {}
	inline void  SwitchTo(void*)    {}
#endif

}
}
