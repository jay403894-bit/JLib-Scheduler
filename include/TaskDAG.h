// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// TaskDAG.h  -- requires Task and TaskScheduler forward declarations
#pragma once
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include "Task.h"
#include "TaskScheduler.h"
#include "TaskNode.h"
#include "Epochs.h"
#include "TaskAllocator.h"

static constexpr uint8_t NONE = 255;

namespace JLib {

    class TaskDAG {
    public:
        TaskDAG(TaskScheduler& sched) : scheduler(sched) {};
        TaskNode* CreateNode(Task* t, uint8_t priority = NONE, uint8_t cpu_id = NONE);
        // Like CreateNode, but the task runs via TaskScheduler::PushMain (only progresses when
        // the main thread calls ProcessMainThread) instead of the worker pool. Use for anything
        // that must run on the main thread -- e.g. Submit() calls in this renderer, which push
        // into Renderer::m_WorkerLocalStorage/m_Buckets and are only safe single-threaded today.
        // Whoever waits on this DAG's completion MUST use TaskScheduler::WaitForMain, not
        // WaitFor, or a main-affinity node (and everything downstream of it) hangs forever.
        TaskNode* CreateMainNode(Task* t, uint8_t priority = NONE);
        // A gate has no task; it fires its dependents instantly when its trigger is met.
        // Compose gates to express arbitrary boolean readiness, e.g. (A && B) || C.
        TaskNode* CreateGate(TaskNode::LogicType type);

        // A node with no task that completes when SOMETHING OUTSIDE THE POOL says so, rather than
        // by running. Use it to make an external completion -- an overlapped I/O finishing, a GPU
        // fence signalling, a coroutine reaching its end -- a real dependency edge instead of
        // something a task sits and blocks on.
        //
        //     auto* io   = dag.CreateExternalNode();
        //     auto* next = dag.CreateNode(consumeTask);
        //     dag.AddDependency(next, io);          // next waits on the I/O
        //     ... issue the read, and from its completion callback:
        //     dag.SignalExternal(io);               // next fires; no fiber was ever held
        //
        // WHY NOT JUST BLOCK IN A NODE. A fiber node that suspends holds a 64KB stack for the whole
        // wait and one slot of a budget that defaults to 64 per worker; a thousand of them is 64 MB
        // and an exhausted pool, which inside a DAG can deadlock rather than merely stall (see the
        // fiber-exhaustion warning in Thread.cpp). An external node costs a few hundred bytes, no
        // fiber, and no worker. It also works when the waiting side is the MAIN thread, which is not
        // a fiber and cannot suspend at all.
        //
        // CONTRACT -- SIGNAL EXACTLY ONCE, AND ONLY FOR A NODE IN A SUBMITTED DAG. A completed node
        // is retired to the epoch manager, so signalling one twice is a use-after-free on the second
        // call, not a no-op. (The second call is ignored if the memory happens to still be live, but
        // do not rely on that.) A DAG that is never signalled simply never completes those nodes:
        // there is no timeout here by design, exactly as there is none on a worker's park.
        //
        // The signal may come from ANY thread, including one the scheduler knows nothing about.
        TaskNode* CreateExternalNode();
        void SignalExternal(TaskNode* node);
         void AddDependency(TaskNode* dependent, TaskNode* dependency);

         // Trampoline installed as the task's fn by Fire(): runs the node's real work, THEN
         // propagates completion to dependents. Same ordering the old Task::onComplete hook
         // gave (fn -> completion -> waitGroup decrement, since Execute() decrements after fn
         // returns), without Task itself carrying callback fields.
         // `data` IS the TaskNode -- the saved fn/data/owner live embedded in the node (see
         // TaskNode.h's embedded-context comment; this replaced a heap-allocated per-fire
         // TaskFinishedContext). Everything is read BEFORE OnTaskFinished runs: that call
         // retires the node via EBR, and while the free is epoch-deferred, nothing here may
         // rely on touching the node after handing it to its own completion path.
         static void OnTaskFinishedWrapper(void* data) {
             auto* node = static_cast<TaskNode*>(data);
             TaskDAG* owner = node->owner;
             node->origFn(node->origData);   // the node's actual task
             // Ran to the end, so the outcome is Completed. A node that is CANCELLED never reaches
             // here at all -- it is never dispatched, so its payload never runs.
             owner->OnTaskFinished(node, TaskNode::Outcome::Completed);
         }
        // Offline cycle check (Kahn's). MUST be called before any node is submitted --
        // it walks every tracked node, which self-free once running. Returns true if the
        // graph has a cycle (some node's dependencies_left can never reach 0).
        bool HasCycle();

        void Validate();

        // Validate then kick off the whole graph. Returns false (and reclaims the nodes)
        // if there's a cycle; otherwise submits all roots and returns true. This is the
        // intended entry point -- prefer it over calling SubmitIfReady per root, because
        // it also clears node tracking at the right moment (nodes self-free after this).
        bool Submit();

        void OnTaskFinished(TaskNode* node, TaskNode::Outcome outcome = TaskNode::Outcome::Completed);

        // ABORT THE GRAPH. Every node not yet dispatched completes as CANCELLED instead of running,
        // and that outcome propagates through the dependents exactly as a normal completion does --
        // AND cancels on any cancelled input, OR takes whichever result arrives first. Cancelled
        // nodes never run their payload; their tasks are destroyed and returned to the slab, and
        // their WaitGroups are decremented so anything blocked in WaitFor is released rather than
        // hanging on work that will never happen.
        //
        // WHAT IT DOES NOT DO, deliberately, in this first phase:
        //   * A task already RUNNING keeps running. Nothing can safely stop it mid-body -- that is
        //     cooperative everywhere (Go, C#, Rust land in the same place) because the alternative
        //     is asynchronous stack destruction. Poll a flag of your own if a long task needs to
        //     notice.
        //   * A task already QUEUED still runs. It has been dispatched; the cancel only stops what
        //     has not been.
        //   * An external node already ARMED is not reached. Nothing here knows how to cancel the
        //     outside operation it represents -- that needs a per-node cancel callback, which is
        //     the next phase. Until then, whoever armed it must still signal or cancel it, or the
        //     graph below it never completes.
        //
        // Idempotent and callable from any thread.
        void Cancel();
        bool Cancelled() const { return cancelled.load(std::memory_order_acquire); }

        // The cancellation scope every node's task is stamped with. Cancel() cancels it, so ONE call
        // reaches both halves of the problem: nodes not yet dispatched are converted by Fire()'s
        // flag check, and tasks already RUNNING see it through CurrentTaskCancelled() and can give
        // up at their own next check. Without this a running node had no way to hear about an abort
        // at all.
        //
        // Hand it to work this graph spawns but does not own -- coroutines, nested parallel loops --
        // so an abort reaches those too:
        //     t->cancelToken = dag.Token().Raw();
        CancelToken Token() const { return scope.Token(); }

        void EndFrame();
    private:
        TaskScheduler& scheduler;
        // Set by Cancel(). Read at the top of Fire(), which is the single funnel every node passes
        // through on its way to being dispatched -- so one flag converts the whole remaining graph
        // without needing a registry of nodes (Submit() clears that list; see the note there).
        std::atomic<bool> cancelled{ false };
        // One scope per graph, stamped onto every node's task by CreateNode. Lives as long as the
        // DAG object, which must outlive the work it submitted -- the same requirement the node
        // pointers already impose.
        CancelScope scope;
        // Completion for a node that will never run: decrement its WaitGroup, then destroy and free
        // its task. Mirrors Worker()'s fast path exactly, minus the Execute.
        void DisposeUnexecutedTask(TaskNode* node);
        // Tracks every node created this build, for cycle detection / root discovery.
        // Single-threaded build only; entries dangle after Submit() (nodes self-free),
        // so it is cleared there and never iterated post-submit.
        std::vector<TaskNode*> nodes;

        void Fire(TaskNode* node, TaskNode::Outcome outcome = TaskNode::Outcome::Completed);
        static void NodeDeleter(void* p);   // EBR deleter: ~TaskNode + return its slot to the slab
    };

    // SignalExternal reachable from a TaskNode* alone, without naming TaskDAG.
    //
    // This exists so Coroutine.h -- the optional C++20 header -- can complete a DAG node with only a
    // FORWARD DECLARATION of TaskNode and of this function, instead of including TaskDAG.h. Two
    // optional features should not drag each other into every translation unit that uses one of
    // them, and the direction has to stay C++20 -> C++17: the DAG must never learn what a coroutine
    // is, or using a DAG at all would start requiring C++20.
    //
    // Works because CreateExternalNode records the owning DAG on the node. Same contract as
    // TaskDAG::SignalExternal: exactly once, node must belong to a submitted DAG.
    void SignalExternalNode(TaskNode* node);
};