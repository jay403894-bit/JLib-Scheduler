// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <cassert>

// A HEADER OF ITS OWN because both TaskScheduler.h and Event.h need it, and Event.h is included BY
// TaskScheduler.h -- so it cannot reach back into it. Nothing here depends on either.

namespace JLib {

	// ============================ THE PRIMITIVE REGISTRY ======================================
	//
	// ANSWERS EXACTLY ONE QUESTION: which blocking primitives exist? Nothing else. The wait lists
	// stay on the primitives that own them; this is a doubly-linked intrusive chain and a head
	// pointer, and it exists so teardown can find them.
	//
	// WHY IT HAS TO EXIST. Every primitive here can now eject its waiters on demand -- Event,
	// SchedulerSemaphore, SchedulerConditionVariable and, as of the previous commit, SchedulerMutex.
	// But CancelWaiters is called by WHOEVER HOLDS THE OBJECT, so with no registry a global "release
	// everything parked" had nothing to iterate. A frame parked on a primitive nobody signals again
	// never unwinds, and nothing it holds is released: RAII, its WaitGroup slot, a hazard record.
	// That is the structural cause of parked-work leaking at shutdown.
	//
	// INTRUSIVE AND DOUBLY LINKED, not a container. Registration is a push-front under one mutex and
	// unlink is O(1) -- singly linked would make every primitive destruction an O(n) walk, which a
	// program creating and destroying locks in a loop would feel as O(n^2).
	//
	// A PRIMITIVE CONSTRUCTED BEFORE Init IS NOT REGISTERED, and that is a real limitation rather
	// than an oversight: there is no scheduler to register with. A file-scope SchedulerMutex is the
	// common case. Such a primitive is still fully functional; it simply will not be drained by
	// Join(), so anything parked on it at teardown is abandoned exactly as before. Construct
	// primitives after Init if you want them drained.
	class WaitPrimitive {
	public:
		virtual ~WaitPrimitive();
	protected:
		WaitPrimitive();
		// MUST BE THE FIRST STATEMENT OF EVERY DERIVED DESTRUCTOR. Unlinking in ~WaitPrimitive alone is
		// too late: by then the derived part is gone and the vtable has reverted, so a concurrent drain
		// calls into a half-destroyed object -- a PURE-VIRTUAL CALL if the hook were pure. Idempotent,
		// so calling it here AND in ~WaitPrimitive is free.
		//
		// FOUR CALL SITES -- SchedulerMutex, SchedulerSemaphore, SchedulerConditionVariable, Event.
		// An invariant spread over N sites gets added to N-1 here, which is why the base
		// DrainForShutdown asserts rather than being pure: a forgotten call is a loud test failure.
		void LeaveRegistry() noexcept;

		// CANCEL EVERY WAITER, unconditionally. Deliberately NOT named CancelWaiters and deliberately
		// tokenless: this is teardown's verb, and giving it a token would invite calling it as a
		// general-purpose cancel. Event's CancelWaiters has no token at all, so a shared token-taking
		// virtual would have had to silently ignore one, which is worse than not offering it.
		// NOT PURE, and the default is a deliberate backstop. Between a derived destructor finishing
		// and ~WaitPrimitive unlinking, the object is STILL IN THE LIST with the base vtable -- a
		// concurrent drain then makes a PURE-VIRTUAL CALL. LeaveRegistry() below is the real fix; this
		// default only ensures that forgetting it degrades to a skipped drain instead of a crash.
		//
		// Reached only if a derived class forgot LeaveRegistry(): with it, the object is out of the
		// list before its derived state dies and this is never called.
		virtual void DrainForShutdown() {
			assert(false && "WaitPrimitive: derived destructor must call LeaveRegistry() FIRST -- "
			                "this object was drained while being destroyed");
		}

	private:
		WaitPrimitive* nextPrimitive_ = nullptr;
		WaitPrimitive* prevPrimitive_ = nullptr;
		friend class TaskScheduler;
	};

}
