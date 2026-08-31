// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "Task.h"
#include <stdexcept>
#include <string>

namespace JLib {
    class TaskDAG; // owner backpointer only -- no TaskDAG.h include (it includes this header)

    struct TaskNode;

    // One forward edge, dependency -> dependent. Nodes hold the HEAD of an intrusive
    // singly-linked list of these; the cells themselves live in TaskDAG chunk storage and
    // outlive every node, so walking them needs no reclamation of its own.
    //
    // The link is a POINTER, not an index into the chunk table, and that is deliberate: a
    // reader never touches the table, so growing it can never invalidate a walk in progress.
    // Chunks are individually stable, so the edges themselves never move.
    struct DagEdge {
        TaskNode* dep  = nullptr;
        DagEdge*  next = nullptr;
    };

    struct TaskNode {
        Task* task;                           // nullptr for a gate (see isGate)

        // Embedded completion context -- stamped by TaskDAG::Fire() when it repoints the
        // task's fn/data at OnTaskFinishedWrapper, so the node's REAL work survives the
        // overwrite. This used to be a separate heap-allocated TaskFinishedContext (a new+
        // delete per node fire, the DAG runtime's only heap traffic); the node itself always
        // outlives the trampoline (it's EBR-retired at the END of OnTaskFinished, and the
        // actual free is deferred past any reader), so the fields live here instead and the
        // DAG runtime is genuinely zero-allocation. Unused (defaults) on gates -- they never
        // schedule a task, so Fire() never stamps them.
        TaskDAG* owner = nullptr;
        Task::Func origFn = nullptr;
        void* origData = nullptr;

        // HOW a node finished. Cancellation is an OUTCOME, not a separate unwind: a cancelled node
        // does not run its payload, it just transitions and propagates, so the existing dependent
        // walk does all the work and there is no reverse traversal anywhere.
        //
        //   AND  any CANCELLED input  -> CANCELLED, immediately, without waiting for the countdown.
        //   OR   FIRST input wins     -> if the first to complete is CANCELLED the gate is
        //                                CANCELLED, and a later OK does not change it.
        //
        // Both fall out of Fire's existing `submitted` exchange, which already means "first caller
        // wins" -- it was written to dedup OR predecessors and AND races, and an outcome rides
        // through it without needing any new counter.
        enum class Outcome : uint8_t { Completed, Cancelled };

        // How this node decides it's ready, given its direct predecessors:
        //   AND -> fire once ALL predecessors finish (dependencies_left counts down to 0)
        //   OR  -> fire on the FIRST predecessor (the `submitted` exchange dedups the rest)
        enum LogicType : uint8_t { AND, OR };
        LogicType gateType   : 1;

        // A gate has no task: when its trigger fires it propagates INSTANTLY (runs its own
        // OnTaskFinished) instead of scheduling work. Compose gates to build arbitrary
        // boolean expressions, e.g. (A && B) || C.
        uint8_t   isGate     : 1;

        // An EXTERNAL node also has no task, but unlike a gate it does NOT complete when fired --
        // it waits to be told, by TaskDAG::SignalExternal, from whatever finishes the outside work
        // (an IOCP completion, a GPU fence callback, a coroutine's final suspend).
        //
        // WHY IT EXISTS: the alternative is a fiber node that blocks, and a suspended fiber holds a
        // 64KB stack for the whole wait. A thousand pending I/O operations that way is 64 MB of
        // stacks and a blown fiber budget; as external nodes it is a few hundred bytes and no
        // fibers at all. It also works from the MAIN thread, where suspension is impossible.
        uint8_t   isExternal : 1;

        // Arm/signal rendezvous for an external node. Fire() sets ARMED when the node's
        // dependencies are satisfied; SignalExternal sets SIGNALLED when the outside work
        // finishes. THE ORDER IS NOT KNOWN -- a completion can land before the dependencies do --
        // so whichever operation observes the OTHER's bit already set is the one that propagates.
        // Exactly one can, because fetch_or is atomic and the loser sees its own bit was unset.
        //
        // This is the same arm-then-publish hazard as WaitOnEventArmed and the 1.3.5 lost wakeup:
        // check-then-act would either drop the completion or fire the dependents twice.
        enum : int { EXT_ARMED = 1, EXT_SIGNALLED = 2 };
        std::atomic<int> extBits{ 0 };

        // Head of this node's forward-edge list. Was a LockFreeList<TaskNode*>* until
        // 2026-08-24: graph construction is single-threaded by contract (CreateNode appends
        // to a plain std::vector, so it cannot be otherwise) and nothing pushes after
        // Submit, so the list was paying for lock-freedom that was unreachable in practice
        // -- an EpochGuard per push, Harris marked pointers, remove/contains that no caller
        // used, and FOUR slab slots per node before a single edge existed (the node, the
        // list object, and its two sentinels). Now: one pointer, and edges come from the
        // owning DAG's chunk allocator.
        DagEdge* firstEdge = nullptr;
        std::atomic<int> dependencies_left;
        std::atomic<bool> submitted{ false };
        uint8_t cpuID    : 8;
        uint8_t priority : 8;
        uint8_t isLocal  : 1;
        uint8_t isFork   : 1;
        // Runs via TaskScheduler::PushMain (drained by ProcessMainThread) instead of the
        // worker pool. Whoever waits on a WaitGroup covering this node's completion MUST use
        // WaitForMain, not WaitFor -- see WaitForMain's declaration comment.
        uint8_t isMain   : 1;

        // A NODE ALLOCATES NOTHING. It used to make a SECOND slab allocation here for the
        // dependents list, with its own null check because an exhausted pool placement-new'd a
        // LockFreeList at address nullptr ("Access violation writing location 0x0"). That whole
        // failure mode went away with the allocation when edges became DAG-owned chunk storage.
        //
        // The allocator parameter went with it, once the change had settled: it was kept for a
        // day to hold the call sites still while the edge rework landed, and an unnamed parameter
        // three call sites pass for nothing is a hook for an allocation that no longer exists.
        // The node's own memory comes from AllocSized at the call site (56 bytes, the 64-byte
        // class) and edges from the DAG's chunk allocator, so there is no path left where a node
        // allocates anything itself.
        //
        // BITFIELDS ARE INITIALIZED HERE, NOT AT THE DECLARATION. C++17 does not allow a default
        // member initializer on a bitfield (that arrived in C++20), and the core of this library
        // is C++17 -- Task.h carries the same note on its flag block for the same reason.
        //
        // Listed in DECLARATION ORDER so the initialization order is the written one.
        explicit TaskNode(Task* t)
            : task(t)
            , gateType(AND), isGate(0), isExternal(0)
            , dependencies_left(0)
            , cpuID(0), priority(0), isLocal(1), isFork(0), isMain(0) {}

        // No destructor: nothing here owns memory any more. Edges belong to the DAG.
    };

    // Packed to fit the 64-byte class. It was 72 bytes -- a LogicType stored as a full int plus
    // five separate bools and two uint8_t members, scattered across the struct with padding
    // between them -- so it took a 256-byte slot and used 28% of it. The DAG scaling bench
    // allocates roughly as many nodes as tasks, so this was not a rounding error.
    //
    // Fitting the EXISTING class was preferred over adding a 96-byte one. Adding a size class is
    // cheap now that the pool is a template, but it is not free: each class is its own
    // reservation sized for its own peak, and memory in one cannot serve a request from another.
    // Shaping a struct to fit a class costs nothing at all.
    static_assert(sizeof(TaskNode) <= 64,
        "TaskNode must fit the 64-byte slab class -- see the packing note above");
}
