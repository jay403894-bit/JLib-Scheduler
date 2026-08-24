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

    // Replaces the `noFiber` bool in 2.0: requesting suspend capability used to mean writing
    // `noFiber = false`, a double negative repeated at every call site that needed it, each
    // requiring its own explanatory comment to stay readable. `TaskType::Fiber` is the same request
    // spelled as a direct, positive statement, and -- unlike a renamed bool -- the compiler refuses
    // any call site still passing a bare `true`/`false` literal instead of naming a value, so a
    // change here cannot silently invert what an existing call site meant. 2.8 added a third value
    // to the same enum for the same reason.
    //
    // HOW a task suspends -- the only thing the worker needs to tell them apart by:
    //
    //   Native    runs to completion on the worker's own stack. The overwhelmingly common short
    //             task: no fiber, no context switch, no 64KB stack. MUST NOT suspend -- no WaitFor,
    //             no WaitOnEvent -- because there is no context to switch away from, so suspending
    //             throws inside a noexcept Execute() and fail-fasts (STATUS_STACK_BUFFER_OVERRUN on
    //             Windows) with no message. assignedFiber stays nullptr, which is exactly what the
    //             WaitOnEvent* guards check for. This is the default; ask for Fiber explicitly for
    //             anything that waits on something.
    //   Fiber     owns a stack; suspends by ContextSwitch and resumes later on any worker.
    //   Coroutine a C++20 coroutine handle, resumed through the SAME fn(data) call as everything
    //             else (fn is a trampoline, data is coroutine_handle::address()), so the worker
    //             needs no <coroutine> include and the core stays C++17. It differs from Native in
    //             ONE respect, and it is a lifetime rule rather than a dispatch rule: fn() returning
    //             means "suspended OR finished", and the worker cannot tell which -- so it NEVER
    //             completes a coroutine task at all. The C++20 side owns it start to finish and
    //             frees it from inside the coroutine. See JLib/Coroutine.h for why any flag-based
    //             alternative is racy.
    enum class TaskType : uint8_t { Native, Fiber, Coroutine };

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
    // Task's packed flag block, declared once and expanded in two places: Task itself, and
    // detail::TaskFlagPacking, which exists so the packing can be static_asserted and so the
    // stale-library guard can fingerprint it. Sharing the tokens is the point -- see the note at the
    // expansion site in Task.
    //
    //   hiPri         queue order only, never placement, never OS priority.
    //   requiredSize  Standard or Heavy fiber stack.
    //   type          Native / Fiber / Coroutine. TWO bits: three values today, room for a fourth.
    //                 Defaults to Native here as well as in CreateTask -- the predecessor field
    //                 (noFiber) defaulted the opposite way from CreateTask's parameter, which only
    //                 mattered for a Task constructed directly, and was fixed rather than carried.
    //   priorityBoost original hiPri before a boost, 0 meaning "not boosted". One bit is exact
    //                 because it only ever stores hiPri, which is itself one bit. Nothing writes it:
    //                 BoostTaskPriority has had no callers since 21719ac (see SchedulerMutex).
    //   corePref      P/E placement hint; two bits covers Default/P/E/Wide.
    //   trivialDtor   set when the destructor provably has nothing to do, letting the completion
    //                 path skip the virtual `t->~Task()` on every single task. The vptr itself
    //                 cannot go -- it is what runs ~LambdaTask and any captured objects' destructors
    //                 -- but the CALL can, whenever the concrete type has no captures to destroy.
    //                 DEFAULT 0 ("not known to be trivial") on purpose: tasks built anywhere other
    //                 than CreateTask (the MPSC queues' stub_, TaskDAG's nodes) never set it and
    //                 keep paying the call, so a missed set can only ever be slow, never wrong. The
    //                 inverse default would make an oversight leak captures silently.
#define JLIB_TASK_FLAG_FIELDS            \
        uint8_t   hiPri         : 1;     \
        FiberSize requiredSize  : 1;     \
        TaskType  type          : 2;     \
        uint8_t   priorityBoost : 1;     \
        CorePref  corePref      : 2;     \
        uint8_t   trivialDtor   : 1;

    struct alignas(16) Task {
        using Func = void(*)(void*);

        // Exactly ONE cache line (see the static_assert below): vptr + 5 pointer fields + a single
        // packed byte of flags. Three former members were removed to get here, each with no
        // functionality loss: stopFlag (had zero readers anywhere -- cooperative cancellation passes
        // a flag through `data` instead), and onComplete/onCompleteData/callbackFlag (their ONLY
        // user was TaskDAG, which now wraps fn/data with its own trampoline -- see TaskDAG::Fire).
        //
        // LAYOUT AS OF 2.8.0: vptr 0 | fn 8 | data 16 | assignedFiber 24 | next 32 | waitGroup 40 |
        // flags 48 | FREE 49-55 | nextWaiter 56. Bytes 52-55 are 4-aligned and unclaimed -- room for
        // one 32-bit field (a cancellation-token index is the intended tenant) at zero size cost.
        // The vtable pointer stays: Thread.cpp/TaskScheduler.cpp destroy tasks via `t->~Task()`
        // through the BASE pointer, and that virtual dispatch is what runs ~LambdaTask (and any
        // captured objects' destructors) -- dropping it would silently leak lambda captures.
        Func fn;
        void* data = nullptr;
        Fiber* assignedFiber = nullptr;
        std::atomic<Task*> next{ nullptr };
        WaitGroup* waitGroup = nullptr;
        // ---- PACKED FLAG BLOCK ------------------------------------------------------------------
        // These six were one byte each (offsets 48-53) with two bytes of tail padding after them.
        // Packed, they fit in a SINGLE byte at 48, which frees 49-55 -- in particular a 4-byte,
        // 4-aligned slot at offset 52 -- without growing Task past its one-cache-line budget. The
        // intended tenant is a cancellation-token index (see the I/O runtime work); nothing claims
        // it yet, and deliberately so: a field with no readers is what `stopFlag` was.
        //
        // C++17 CONSTRAINT, and it is a real trap: bitfields may not carry default member
        // initializers before C++20 (P0683R1). Every constructor below therefore has to set all six
        // EXPLICITLY. A constructor that forgets one leaves it indeterminate rather than zero, so
        // this is a use-of-uninitialized bug rather than a wrong-default bug. Do not add a
        // constructor here without extending its init list.
        //
        // CONCURRENCY, the other trap: adjacent bitfields in one allocation unit are a SINGLE
        // memory location in the C++ model, so two threads writing different fields here race even
        // though the fields are logically independent. That is safe today only because all six are
        // written before the task is published and never again -- including priorityBoost, whose
        // only writer (BoostTaskPriority) has had no callers since 21719ac. If lock priority
        // inheritance is ever re-wired it would write hiPri and priorityBoost from a thread other
        // than the one running the task, while the push and steal paths read hiPri -- at which
        // point those two must come back out of this block or become atomics. TSan would see it;
        // the compiler will not.
        // DECLARED VIA A MACRO so that detail::TaskFlagPacking below is the SAME TOKENS, not a
        // hand-copied mirror. The stale-library guard fingerprints that struct to detect exactly this
        // block being repacked or reordered -- a change that moves every flag while leaving
        // sizeof(Task) at 64. A mirror maintained by hand would be a correctness property enforced by
        // discipline, and it would fail silently and invisibly the first time someone forgot. Sharing
        // the tokens makes drift impossible instead of unlikely. Per-field notes are on the macro.
        JLIB_TASK_FLAG_FIELDS
        // ---- end packed flag block --------------------------------------------------------------

        // FIELD ORDER HERE IS LOAD-BEARING. `started` fills the single padding byte at 49 so the
        // 4-byte token still lands 4-aligned at 52 and nextWaiter stays at 56. Declare them the
        // other way round and `started` takes 56, pushing nextWaiter past the cache line --
        // sizeof(Task) goes 64 -> 80 under alignas(16). The static_assert below catches it, and did.
        //
        // Has this task's body begun? Set at first pickup and never cleared.
        //
        // LOAD-BEARING FOR CANCELLATION, and the reason is that a queued task is not always a task
        // waiting to START. Thread::Resume ends in TaskScheduler::Requeue(owningTask), so a fiber
        // that suspended comes back through the same queues as fresh work, and a coroutine awaiter
        // re-pushes its Task the same way. Those entries are handles to a LIVE 64KB stack or a live
        // coroutine frame.
        //
        // So a cancelled task can only be DISCARDED when this is 0. Discarding a started one does
        // not cancel it, it abandons it: the fiber never returns to GlobalFiberPool (leaked from a
        // budget of 64 per worker), every destructor on its stack is skipped -- including any
        // SchedulerMutex it still holds, which then stays locked forever -- and a coroutine's frame
        // and WaitGroup leak with it. A started task is cancelled by being RESUMED with Cancelled so
        // it unwinds normally. Same outcome for the caller; the only difference is which one is
        // safe. (Rust's "drop the future" runs the frame's destructors for exactly this reason.)
        //
        // Lives in the spare bytes the 2.9.0 flag packing freed (49-51), so sizeof(Task) stays 64.
        // Not a bitfield: written by the worker that picks the task up while other threads may read
        // the flags at 48, so it needs its own memory location.
        uint8_t started = 0;

        // The cancellation scope this task belongs to, or CancelToken::kNone. A 4-byte HANDLE, not
        // a flag: scopes are what get cancelled -- every node in a graph, every operation for a
        // connection -- and many tasks reference one. See CancelToken.h.
        //
        // THIS IS WHAT THE 2.9.0 FLAG PACKING WAS FOR. Packing six one-byte flags into one freed
        // bytes 49-55, including the 4-aligned slot at 52 this occupies: sizeof(Task) stays 64, one
        // cache line, and the lambda capture budget is untouched at 192 bytes. Deliberately a plain
        // uint32_t rather than a bitfield member -- it is written before the task is published and
        // read afterwards from other threads, so it needs its own memory location rather than
        // sharing an allocation unit with the flags above.
        uint32_t cancelToken = 0xFFFFFFFFu;   // CancelToken::kNone, spelled out to avoid the include


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

        // Both constructors initialize EVERY bitfield -- see the C++17 note on the flag block.
        // Members are listed in declaration order so the initialization order is the written one.
        Task()
            : fn(nullptr), data(nullptr), assignedFiber(nullptr), next(nullptr),
              hiPri(0), requiredSize(FiberSize::Standard), type(TaskType::Native),
              priorityBoost(0), corePref(CorePref::Default), trivialDtor(0) { ; }
        Task(Func f, void* d = nullptr, uint8_t hipri =false, FiberSize size = FiberSize::Standard)
            // hipri is a uint8_t taking any value; normalize rather than truncate into one bit.
            : fn(f), data(d), assignedFiber(nullptr), next(nullptr),
              hiPri(hipri ? 1 : 0), requiredSize(size), type(TaskType::Native),
              priorityBoost(0), corePref(CorePref::Default), trivialDtor(0) {
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

    namespace detail {
        // Mirrors Task's packed flag block, and exists because that block can fail SILENTLY.
        // Packing bitfields of DIFFERENT declared types into one allocation unit is not guaranteed
        // by the standard -- MSVC does it when the types are the same SIZE, which is why these are
        // all 1-byte types, but that is a compiler behaviour and not a promise. If some compiler
        // declines, the block grows back to six bytes, the 4-byte aligned slot reclaimed at offset
        // 52 quietly stops existing, and `sizeof(Task) == 64` STILL HOLDS -- because those bytes
        // were padding before the packing too. So the cache-line assert above cannot see this
        // failure and it needs its own.
        struct TaskFlagPacking { JLIB_TASK_FLAG_FIELDS };
        static_assert(sizeof(TaskFlagPacking) == 1,
                      "Task's six flags must pack into a single byte -- see the flag block in Task");

        // The bit layout of that block, as an actual observation rather than an assumption.
        //
        // sizeof alone is not enough for the stale-library guard: swapping two 1-bit flags, or moving
        // a flag between allocation units without changing the total width, leaves sizeof at 1 while
        // relocating the bits every consumer reads. So each field is set ALONE in a zeroed copy and
        // the resulting bytes are hashed -- which encodes, for every field, exactly which byte and
        // which bits it occupies. Any reorder, rewidth or repack changes the result.
        //
        // Safe to poke at raw bytes here in a way it would not be on Task: this is a trivial
        // aggregate with no vtable and no base, so zeroing it and reading it back is well defined.
        inline uint32_t TaskFlagBitLayout() {
            uint32_t h = 2166136261u;                      // FNV-1a
            auto mixByte = [&h](unsigned char b) { h ^= b; h *= 16777619u; };

            TaskFlagPacking p{};
            auto probe = [&](auto setter) {
                unsigned char* raw = reinterpret_cast<unsigned char*>(&p);
                for (size_t i = 0; i < sizeof p; ++i) raw[i] = 0;
                setter(p);
                for (size_t i = 0; i < sizeof p; ++i) mixByte(raw[i]);
            };

            probe([](TaskFlagPacking& f) { f.hiPri         = 1; });
            probe([](TaskFlagPacking& f) { f.requiredSize  = FiberSize::Heavy; });
            probe([](TaskFlagPacking& f) { f.type          = TaskType::Coroutine; });
            probe([](TaskFlagPacking& f) { f.priorityBoost = 1; });
            probe([](TaskFlagPacking& f) { f.corePref      = CorePref::Wide; });
            probe([](TaskFlagPacking& f) { f.trivialDtor   = 1; });
            return h;
        }
    }

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