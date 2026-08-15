// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <functional>
#include <atomic>
#include <cassert>   // the slab-allocation guard in operator delete below

namespace JLib {
    struct Fiber;
    struct Task;
    struct DirectEvent;
    // Defined in WaitGroup.h. Forward-declared here because a Task only holds a WaitGroup*, and
    // keeping the definition out means <mutex> and <unordered_set> stay out of the seven headers
    // that include Task.h without ever naming a WaitGroup. Callers get it via TaskScheduler.h.
    struct WaitGroup;

    enum class FiberSize : uint8_t { Standard, Heavy };

    // Which CORE CLASS a task prefers on hybrid CPUs (P-cores vs E-cores). FULLY ORTHOGONAL to hiPri by
    // design: hiPri is QUEUE ORDER (drained/stolen first) and NOTHING ELSE; placement is governed SOLELY
    // by this field -- priority never implies a core class (a loPri P-core task and a hiPri E-core task
    // are both legitimate). Unset (Default) means no preference: the full-pool round-robin, the
    // scheduler's original placement behavior. Preference is a HINT, not a constraint: PickNextWorker
    // spills to the other class when the preferred one is unavailable, and stealing stays deliberately
    // preference-blind (work-conserving). Non-hybrid CPUs: all workers are P-labeled, so every value
    // routes to the full pool -- zero behavior difference.
    // SCOPE OF ENFORCEMENT (deliberate): corePref is vetted at PUSH placement (PickNextWorker) and at
    // STEAL (TaskDeque::steal_if via StealClassCompatible) -- but an OWNER always runs whatever is in
    // its own deque/inboxes unvetted: spill (preferred class unavailable at push) transfers ownership,
    // and explicit CPU affinity (Push(cpu,..)/PushImmediate/PushToCore/DAG isFork/isMain) OVERRIDES
    // corePref entirely -- pinning to a specific core is a stronger, more explicit request than a
    // class preference. Do not add owner-pop vetting: a task a worker owns must run, else it strands.
    enum class CorePref : uint8_t {
        Default = 0,   // unspecified -- behaves as Any (no class preference, full-pool round-robin)
        P       = 1,   // prefer Performance cores (latency-sensitive, chunky critical-path work)
        E       = 2,   // prefer Efficiency cores (background/bulk work; preserves P headroom)
        Wide    = 3,   // explicit no-preference, burst intent: round-robin the FULL pool (e.g. heavy
                       // physics -- wants all cores at once and isn't latency-tiered)
        Any     = 3,   // alias of Wide -- "I genuinely don't care which core class runs this." Same
                       // mechanism today; separate NAME so intent reads at call sites and the two can
                       // diverge later without API churn.
    };
    struct alignas(16) Task {
        using Func = void(*)(void*);

        // Exactly ONE cache line (see the static_assert below): vptr + 5 pointer fields + the
        // byte flags. Three former members were removed to get here, each with no functionality
        // loss: stopFlag (had zero readers anywhere -- cooperative cancellation passes a flag
        // through `data` instead), and onComplete/onCompleteData/callbackFlag (their ONLY user
        // was TaskDAG, which now wraps fn/data with its own trampoline -- see TaskDAG::Fire).
        // The vtable pointer stays: Thread.cpp/TaskScheduler.cpp destroy tasks via `t->~Task()`
        // through the BASE pointer, and that virtual dispatch is what runs ~LambdaTask (and any
        // captured objects' destructors) -- dropping it would silently leak lambda captures.
        Func fn;
        void* data = nullptr;
        Fiber* assignedFiber = nullptr;
        std::atomic<Task*> next{ nullptr };
        WaitGroup* waitGroup = nullptr;
        uint8_t hiPri = false;
        FiberSize requiredSize = FiberSize::Standard;
        // Run directly on the worker with NO fiber underneath: no fiber acquired, no context switch,
        // no 64KB stack. Cheaper, and the right default for the overwhelmingly common short task.
        //
        // THE CONSTRAINT: such a task MUST NOT SUSPEND -- no WaitFor, no WaitOnEvent, no CoYield.
        // There is no context to switch away from, so suspending throws inside a noexcept Execute()
        // and fail-fasts (STATUS_STACK_BUFFER_OVERRUN on Windows) with no message. CreateTask
        // defaults this to TRUE, so any task that waits on anything must opt out explicitly.
        //
        // (Named fastJob until 1.0. "noFiber" states what is checkable -- whether a fiber exists --
        // rather than advertising a benefit, and it puts the constraint in view at the call site.)
        uint8_t noFiber = 0;
        uint8_t priorityBoost = 0;  // Original priority before boost (0 = no boost, otherwise original hiPri)
        // P/E-core placement hint (see CorePref above). Lives in what was tail PADDING -- FiberSize is
        // uint8_t, so the byte block ends at offset 53 with 11 spare bytes under the 64-byte assert.
        CorePref corePref = CorePref::Default;

        // Set when this task's destructor provably has nothing to do, letting the completion path
        // skip `t->~Task()` -- a virtual call through the vtable on every single task.
        //
        // The vptr cannot go (see the note above: it is what runs ~LambdaTask and any captured
        // objects' destructors), but the CALL can, whenever the concrete type has no captures to
        // destroy: the (fn, data) overload builds a plain Task with an empty ~Task, and the lambda
        // overload builds a LambdaTask<F> whose only member is F, so it is a no-op exactly when F
        // is trivially destructible.
        //
        // DEFAULT IS 0 -- "not known to be trivial" -- on purpose. Tasks built anywhere other than
        // CreateTask (the MPSC queues' stub_, TaskDAG's nodes) never set it and keep paying the
        // call, which costs those cold paths nothing and means a missed set can only ever be slow,
        // never wrong. The inverse default would make an oversight leak captures silently.
        uint8_t trivialDtor = 0;

        // Intrusive link for Event's waiter stack, living in the same tail padding -- the byte
        // block above ends well short of 64, so this costs nothing and the one-cache-line assert
        // still holds.
        //
        // It is a SEPARATE field from `next` on purpose. `next` belongs to the queues, and a task
        // could in principle be reachable from one while the other is in use; aliasing them would
        // be exactly the sort of overlap that produced the fiber-duplication bugs. A fiber only
        // ever waits on one event at a time, so a single link is enough.
        //
        // Not atomic: it is published by the CAS in Event::AddWaiter (which is the release) and
        // read only after Event::SignalAll has taken the whole list with an acquiring exchange.
        Task* nextWaiter = nullptr;

        Task() : next(nullptr), fn(nullptr), data(nullptr), assignedFiber(nullptr) { ; }
        Task(Func f, void* d = nullptr, uint8_t hipri =false, FiberSize size = FiberSize::Standard)
            : fn(f), data(d), hiPri(hipri), requiredSize(size) {
        }
        virtual ~Task() {

        }

        // Tasks come from TaskAllocator's slabs and are returned there; they are never new'd or
        // deleted. `new` stays DELETED -- that is the half that matters, since it stops a Task
        // being heap-allocated in the first place.
        //
        // `operator delete` CANNOT be deleted alongside it, and that is not a style choice: Task
        // has a VIRTUAL destructor, so the compiler emits a *deleting* destructor into the vtable,
        // and that requires an accessible operator delete whether or not any code ever calls it.
        // MSVC tolerates the deleted form; GCC rejects it, and the standard is on GCC's side.
        // Found by compiling for Linux -- non-conforming code that happened to build, which is the
        // category that breaks on a compiler upgrade rather than only on a new platform.
        //
        // Defined and forbidden at runtime instead: same guarantee, enforcement moved from compile
        // time to a debug assert.
        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) noexcept {
            assert(false && "Task is slab-allocated by TaskAllocator; never delete one");
        }
        void operator delete[](void*) = delete;

        inline void Execute() noexcept {
            fn(data);
        }
    };

    // Completion-path destructor, skipping the virtual call when the task provably has nothing to
    // destroy (see Task::trivialDtor). Every site that finishes a task goes through here so the
    // rule lives in one place rather than being re-derived at each of them.
    inline void DestroyTask(Task* t) noexcept {
        if (!t->trivialDtor) t->~Task();
    }
    // One cache line, exactly -- if this fires, a new field pushed Task over 64 bytes and every
    // per-task access just started paying a second line. Grow deliberately or shrink elsewhere.
    static_assert(sizeof(Task) == 64, "Task must stay exactly one 64-byte cache line");

    template<typename F>
    class alignas(16) LambdaTask : public Task {
        F func;
    public:
        static_assert(sizeof(F) <= (256 - sizeof(Task)), "LambdaTask exceeds 256-byte slab capacity");
        // NOTE: F here is the CLASS template parameter, which CreateTask has already run through
        // std::decay_t. So this is a plain rvalue reference, NOT a forwarding reference, and it
        // accepts temporaries only. That is why a NAMED callable used to fail to compile:
        //     auto body = [&]{ ... };
        //     sched.CreateTask(body, ...);      // F deduced as lambda&, could not bind here
        // which is a perfectly reasonable thing to write, especially when the same body is also
        // handed to a std::thread. The const& overload below fixes it by copying.
        LambdaTask(F&& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(std::move(f))
        {
            this->data = this;
        }

        // Lvalue overload: copies the callable. No ambiguity with the one above -- an rvalue prefers
        // F&&, an lvalue can only bind here -- and no API change, since CreateTask's own signature is
        // untouched and callers that pass a temporary still move exactly as before.
        LambdaTask(const F& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(f)
        {
            this->data = this;
        }
		~LambdaTask() {
		}
        // Same reasoning as Task's, and for the same reason: this destructor is virtual by
        // inheritance, so its deleting destructor needs an accessible operator delete.
        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) noexcept {
            assert(false && "LambdaTask is slab-allocated by TaskAllocator; never delete one");
        }
        void operator delete[](void*) = delete;

    private:
        static void ExecuteWrapper(void* ptr) {
            LambdaTask* self = static_cast<LambdaTask*>(ptr);

            self->func();
        }
    };
 
};