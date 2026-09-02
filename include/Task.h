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

    // ---- WHICH STACK A FIBER TASK WANTS --------------------------------------------------------
    //
    // THREE CLASSES, AND THE REASON THERE ARE THREE IS NOT SYMMETRY:
    //
    //   Standard  64 KB. What everything gets unless it says otherwise.
    //
    //   Tiny      An I/O continuation does almost nothing: it wakes, reads a completion, and
    //             finishes. Giving it 64 KB is what makes a fiber-per-pending-operation
    //             unaffordable -- and that unaffordability is the whole reason the C++20 coroutine
    //             layer exists. Make the stack small enough and Event::Wait on a fiber replaces
    //             co_await, which keeps the project buildable as C++17.
    //
    //   Deep      512 KB. NEWLY NECESSARY, and it is a direct consequence of the public job
    //             becoming a fiber: work that recurses deeply used to run as a Native task on the
    //             OS thread's megabyte-plus stack. Now it gets 64 KB with a guard page under it,
    //             so what used to be slow-but-fine is a fault.
    //
    // GUARD PAGE ARITHMETIC MATTERS HERE. FiberStackArena leaves the lowest page of every region
    // unbacked, so USABLE depth is one page less than the class size and a class must be larger
    // than a page or AllocateStack refuses outright. "4 KB of stack" means an 8 KB region.
    //
    // Standard is 0 so a zero-initialised Task asks for what it used to get.
    enum class StackClass : uint8_t { Standard = 0, Tiny = 1, Deep = 2 };

    // ---- WHICH LANE A TASK RUNS IN. Replaces `bool hiPri`. --------------------------------------
    //
    // THE BOOL WAS A LIE IN BOTH DIRECTIONS and it cost real debugging time. "hiPri" says PRIORITY,
    // so a loPri task landing on a reserved worker read as merely deprioritised -- when in fact K
    // never reads its loPri inbox, inbox work is unstealable, and the task was UNREACHABLE. The pool
    // deadlocked and reported it as a lost wake. Nothing in the name "low priority" suggests
    // "will never run"; "not on the low-latency lane" does.
    //
    // It lied the other way too. A caller reads "hiPri" as "this work is IMPORTANT" and marks bulk
    // work with it -- which funnels volume onto K workers while the floor idles, because the lane's
    // capacity is K plus spill and nothing else. LowLatency does not invite that reading.
    //
    //   Normal       rides the floor and its adaptive awake set. This is where THROUGHPUT lives:
    //                the floor is most of the pool, and a lane on the floor already measured 15-19x
    //                over no lane at all. Correct for essentially all work.
    //   LowLatency   routed to the reserved band, which exists so this work meets a worker that is
    //                not competing for its place in a queue. Costs a core off the floor per reserved
    //                worker. For latency-critical, LOW-RATE work: I/O completions, subsystem
    //                messages. Never for volume.
    //
    // WHETHER THE BAND SPINS IS A POLICY, NOT PART OF THIS CONTRACT, and conflating the two is easy:
    // SetHotWorkers(k) reserves and lets them park, SetIoHotLane(k) reserves and pins them awake.
    // Parking costs ~5.5 us p50 to get the core back (6.7 parked vs 1.2 spinning, measured); the
    // spin costs a whole core held against every other thread in the process, producer included.
    //
    // PARKING IS NOT THE HAZARD -- being UNREADABLE is. The pool deadlock that motivated this enum
    // happened because K never READS its loPri inbox, not because K was asleep. A parked reserved
    // worker with a LowLatency push waiting for it gets notified and wakes; an awake one holding
    // work in a queue it will never look at does not. Those are independent properties and only the
    // second is a correctness question.
    //
    // TWO VALUES, FROZEN. Reservation is a tail-latency instrument, not a general resource to hand
    // out: every reserved thread comes off the floor, "reserve me one" is a request every subsystem
    // will make and none can price, and two of them on a small box with the timer and reactor
    // already reserved leaves almost no floor. More Lane:: values only if two reserved classes turn
    // out to genuinely fight over the band -- and then it is a 6.0 decision with evidence, not a
    // speculative third enumerator today.
    //
    // Normal is 0 so a zero-initialised Task stays off the lane, which is the safe default: the
    // floor always drains, and a task that should have been LowLatency is slow rather than stuck.
    enum class Lane : uint8_t { Normal = 0, LowLatency = 1 };

    // ---- WHERE A SUSPENDED FIBER MAY RESUME. Replaces `bool migratable`. --------------------
    //
    //   Migrate   (DEFAULT) a resumed fiber continues on WHICHEVER WORKER IS FREE. This is what a
    //             fiber task library is for -- it is the only reason to have a pool rather than a
    //             thread per job -- and it is what the library already paid for: address-routed
    //             frees, a global epoch participant list, fiber-indexed hazard cells.
    //   Pin       a fiber resumes ONLY on the worker it was bound to. marl's contract. TLS is safe
    //             because the fiber never moves; what you give up is resume-anywhere, so a fiber
    //             whose home worker is busy waits for that worker rather than taking the next free
    //             one.
    //
    // THE TRADE IS `thread_local` AND NOTHING ELSE. Under Migrate a TLS value read before a
    // suspension point is not necessarily the same value after it -- you get the resuming worker's
    // copy, SILENTLY. Use FiberLocal<T> (FiberRegistry.h) for anything that must survive a wait; it
    // is attached to the fiber and correct in both modes. Pin is for when the state is not yours --
    // a library you cannot audit keeping its own thread_local across a wait.
    //
    // AN ENUM RATHER THAN A BOOL because `SetMigratableFibers(false)` at a call site says nothing
    // about what false means, and the two modes are a real choice rather than a feature toggle.
    // Same reasoning that turned `bool hiPri` into Lane and `bool noFiber` into TaskType.
    //
    // Migrate is 0 so the default is the zero value, matching Lane::Normal and StackClass::Standard.
    enum class FiberMode : uint8_t { Migrate = 0, Pin = 1 };

    // Spelled out rather than left to `lane ? ... : ...`, because an enum class deliberately has no
    // conversion to bool and adding one would put the old ambiguity straight back: `if (lane)` reads
    // as "if it has a lane", which is true of every task. Both names say which lane they mean.
    constexpr bool IsLowLatency(Lane l) noexcept { return l == Lane::LowLatency; }
    constexpr bool IsNormalLane(Lane l) noexcept { return l == Lane::Normal; }

    // Which CORE CLASS a task prefers on hybrid CPUs (P-cores vs E-cores). Orthogonal to Lane in the
    // dimension THIS field governs: the lane never implies a core class, and a Normal P-core task and
    // a LowLatency E-core task are both legitimate.
    //
    // LANE DOES INFLUENCE WHICH WORKER, and the comment here claimed otherwise for several versions
    // after it stopped being true. LowLatency routes to the reserved band when one exists, because
    // queue order alone cannot deliver latency -- being first in a queue nobody is currently reading
    // is worth nothing, which is exactly how a completion aimed at a parked worker sat for a second.
    // Core CLASS remains this field's business alone; that part never changed. Unset (Default) means no preference: the full-pool round-robin, the
    // scheduler's original placement behavior. Preference is a HINT, not a constraint: PickNextWorker
    // spills to the other class when the preferred one is unavailable, and stealing stays deliberately
    // preference-blind (work-conserving). Non-hybrid CPUs: all workers are P-labeled, so every value
    // routes to the full pool -- zero behavior difference.
    // SCOPE OF ENFORCEMENT (deliberate): corePref is vetted at PUSH placement (PickNextWorker) and at
    // STEAL (TaskDeque::steal_if via StealClassCompatible) -- but an OWNER always runs whatever is in
    // its own deque/inboxes unvetted: spill (preferred class unavailable at push) transfers ownership,
    // and explicit CPU affinity (Push(cpu,..) / DAG isLocal / isFork / isMain) OVERRIDES
    // corePref entirely -- pinning to a specific core is a stronger, more explicit request than a
    // class preference. Do not add owner-pop vetting: a task a worker owns must run, else it strands.
    // ---- THIS IS A BREADTH AXIS NOW, NOT A CORE-CLASS ONE ------------------------------------
    //
    // P and E ARE GONE, and they were dormant: src/posix/Topology.cpp said so in the tree -- "no
    // shipped caller requests CorePref::P or ::E" -- and a grep of the whole repo found none. They
    // were also unproven where they did apply. Under the default `Ideal` affinity a worker is not
    // pinned, so routing a task to a "P worker" expresses a preference that the OS scheduler then
    // adjudicates against its own hybrid policy: a hint on top of a hint. They only bind under
    // `hard`, which measured ~45% worse on wake latency, so nothing runs there. And the concept does
    // not port -- P/E cores are an x86 hybrid notion the other platforms do not expose this way.
    //
    // WHAT PLACEMENT ACTUALLY CONSUMES is how WIDE to spread, and that question is real and
    // measured. Ordinary placement narrows to the awake floor whenever a floor worker is awake --
    // always, since the floor never parks -- which is right for latency-shaped work and wrong for
    // bulk. The burst row shows the cost directly: growth woke 13 workers and 9 ever ran a task,
    // because a busy worker's inbox has one legal consumer and the wave only became reachable as
    // each owner drained it. For a 3.3 ms physics body a kernel wake is ~3 us, 0.1%, and routing
    // around it costs most of the pool.
    //
    // `Any` IS GONE TOO, as a distinct idea: it meant "I do not care which core CLASS runs this",
    // and there are no classes any more. Kept as an alias so existing call sites compile.
    enum class CorePref : uint8_t {
        Default = 0,   // steered: prefer the awake floor. The cheap push -- no kernel wake -- and
                       // the right answer for latency-shaped work (completions, frame-graph nodes).
        Wide    = 1,   // spread across the FULL pool, paying wakes to get capacity NOW. For work
                       // that is throughput-shaped and long enough that a ~3 us wake is noise:
                       // physics steps, ParallelFor leaves, anything that wants every core at once
                       // rather than trickling out through steals.
        Any     = 0,   // alias of Default. Was "no class preference"; classes are gone.
                       // Two spare values remain in the 2-bit field.
    };
    // Task's packed flag block, declared once and expanded in two places: Task itself, and
    // detail::TaskFlagPacking, which exists so the packing can be static_asserted and so the
    // stale-library guard can fingerprint it. Sharing the tokens is the point -- see the note at the
    // expansion site in Task.
    //
    //   hiPri         queue order, AND routing to the reserved band when one exists. Never OS
    //                 priority. A LATENCY mechanism, not an importance one: it is bounded by K plus
    //                 lane spill, so bulk work marked hiPri funnels onto one or two threads while the
    //                 floor idles. Use it for latency-critical, low-rate work -- I/O completions,
    //                 subsystem messages -- and not for anything with volume.
    //   (a requiredSize bit sat here: Standard or Heavy fiber stack. Removed with the heavy
    //    stack class -- it was written by CreateTask and read by nothing.)
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
//   stackClass    which fiber stack this task needs. Two bits for three classes; see StackClass.
//                 THE FLAG BLOCK NOW SPILLS PAST ONE BYTE (7 bits -> 9) AND sizeof(Task) IS
//                 UNCHANGED, because the byte after it was padding to the 16-byte alignment. That
//                 is exactly the slack the header note below describes, so this is spending it
//                 rather than growing the task. A requiredSize bit lived here once and was removed
//                 with the heavy stack class; this is not that field returning by accident -- it
//                 has a reader (Thread::AcquireFiber) from the day it lands, which is precisely
//                 what the old one never had.
#define JLIB_TASK_FLAG_FIELDS            \
        Lane      lane          : 1;     \
        TaskType  type          : 2;     \
        uint8_t   priorityBoost : 1;     \
        CorePref  corePref      : 2;     \
        uint8_t   trivialDtor   : 1;     \
        StackClass stackClass   : 2;

    struct alignas(16) Task {
        using Func = void(*)(void*);

        // Exactly ONE cache line (see the static_assert below): vptr + 5 pointer fields + a single
        // packed byte of flags. Three former members were removed to get here, each with no
        // functionality loss: stopFlag (had zero readers anywhere -- cooperative cancellation passes
        // a flag through `data` instead), and onComplete/onCompleteData/callbackFlag (their ONLY
        // user was TaskDAG, which now wraps fn/data with its own trampoline -- see TaskDAG::Fire).
        //
        // LAYOUT: vptr 0 | fn 8 | data 16 | assignedFiber 24 | next 32 | waitGroup 40 |
        // flags 48 | started 49 | cancelledDirect 50 | pad 51 | cancelToken 52-55 | FREE 56-63.
        // Bytes 56-63 came free when Event stopped linking waiters through Task -- see the
        // tail-padding note further down. The vtable pointer stays: Thread.cpp/TaskScheduler.cpp
        // destroy tasks via `t->~Task()`
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
        // 4-byte token still lands 4-aligned at 52. Declare them the other way round and
        // `started` takes 52 while the token is pushed to 56 -- sizeof(Task) goes 64 -> 80 under
        // alignas(16). The static_assert below catches it, and did.
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

        // Cancelled DIRECTLY, independent of any scope. Set by whoever holds this exact task --
        // Event::CancelWaiters is the reason it exists.
        //
        // WHY NOT REUSE cancelToken. A token points at a SHARED scope; a task usually already
        // belongs to one (its graph's), and overwriting that to cancel this one task would silently
        // detach it from the scope it was in. A separate bit composes instead of replacing: a task
        // is cancelled if EITHER its scope was cancelled or it was cancelled individually.
        //
        // Lands at offset 50, in the padding the flag packing freed. Its own byte rather than a
        // bitfield for the same reason as `started`: written from another thread while the flags at
        // 48 are being read.
        uint8_t cancelledDirect = 0;

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


        // 8 BYTES OF DELIBERATE TAIL PADDING. Event's waiter list used to be an intrusive Treiber
        // stack linked through a Task* nextWaiter field here. It was retired when Event moved to
        // a fiber-indexed slot table: the intrusive link was what forbade removing one specific
        // waiter, and with it SignalOne. Nothing threads through a Task any more.
        //
        // THE SPACE IS LEFT UNCLAIMED ON PURPOSE, and since 3.0.0 that is a performance
        // decision rather than a tidiness one. LambdaTask<F> stores F as a member after this
        // base, and BOTH MSVC and GCC reuse the base tail padding -- measured, not assumed
        // (bench/dag_scaling.cpp prints it). So a lambda capturing up to 8 bytes, which is one
        // reference and by far the most common shape, costs NOTHING: sizeof(LambdaTask) stays
        // 64 and it lands in the 64-byte slab class.
        //
        // Claim these bytes and every single-capture lambda jumps 64 -> 80, which moves it out
        // of the 64-byte class into the 128-byte one. That is a 2x memory regression on the
        // most common task in the system, paid for one field. Measure before taking them.
        //
        // Anything put here must keep static_assert(sizeof(Task) == 64) true, must be added to the
        // ABI fingerprint in TaskScheduler.h, and must justify the size-class cost above.

        // Both constructors initialize EVERY bitfield -- see the C++17 note on the flag block.
        // Members are listed in declaration order so the initialization order is the written one.
        Task()
            : fn(nullptr), data(nullptr), assignedFiber(nullptr), next(nullptr),
              lane(Lane::Normal), type(TaskType::Native),
              priorityBoost(0), corePref(CorePref::Default), trivialDtor(0),
              stackClass(StackClass::Standard) { ; }
        Task(Func f, void* d = nullptr, Lane ln = Lane::Normal)
            : fn(f), data(d), assignedFiber(nullptr), next(nullptr),
              lane(ln), type(TaskType::Native),
              priorityBoost(0), corePref(CorePref::Default), trivialDtor(0),
              stackClass(StackClass::Standard) {
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
        // TWO BYTES NOW, NOT ONE, and the guard keeps its teeth. The block was 7 bits; stackClass
        // took it to 9, which genuinely needs a second byte -- and sizeof(Task) is unchanged because
        // the byte after it was already padding to the 16-byte alignment.
        //
        // WHAT THIS STILL CATCHES is the failure it was written for: a compiler that declines to pack
        // differently-typed bitfields into shared allocation units gives each field its own byte,
        // which is SEVEN here and still fails. Loosening it to <= would have thrown that away.
        static_assert(sizeof(TaskFlagPacking) == 2,
                      "Task's seven flags must pack into two bytes -- see the flag block in Task");

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

            probe([](TaskFlagPacking& f) { f.lane          = Lane::LowLatency; });
            // A requiredSize probe sat here. Removing the field CHANGES THIS FINGERPRINT, which is
            // correct and is the point: the flag block's layout really did change, so a library
            // built before the removal must not link against a header from after it. The guard will
            // say so by name instead of faulting at an unrelated address.
            probe([](TaskFlagPacking& f) { f.type          = TaskType::Coroutine; });
            probe([](TaskFlagPacking& f) { f.priorityBoost = 1; });
            probe([](TaskFlagPacking& f) { f.corePref      = CorePref::Wide; });
            probe([](TaskFlagPacking& f) { f.trivialDtor   = 1; });
            probe([](TaskFlagPacking& f) { f.stackClass    = StackClass::Deep; });
            return h;
        }
    }

    template<typename F>
    class alignas(16) LambdaTask : public Task {
        F func;
    public:
        // NO SIZE CEILING as of 4.0.1. This used to be a static_assert, and it was the SECOND copy
        // of the same rule -- CreateTask carried one too -- which is why removing that one alone did
        // not lift the limit. A body larger than the biggest slot now falls back to the global heap,
        // exactly as an oversized coroutine frame always has (detail::FrameAlloc), and disposal needs
        // no flag because TaskAllocator::Free routes by ADDRESS.
        static_assert(alignof(F) <= 16, "LambdaTask capture is over-aligned for a slab slot");
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
