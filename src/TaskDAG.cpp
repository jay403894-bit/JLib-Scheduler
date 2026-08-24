// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/TaskDAG.h"
// Explicit, not left to whatever TaskDAG.h drags in: the node-type guards below use these in every
// build. MSVC would have supplied them transitively and GCC would not -- that exact difference
// shipped a Linux build break in 2.11.0.
#include <cstdio>    // fprintf, fflush, stderr
#include <cstdlib>   // abort
using namespace JLib;

TaskNode* TaskDAG::CreateNode(Task* t, uint8_t priority, uint8_t cpu_id) {
    // A COROUTINE CANNOT BE A DAG NODE, and this has to be a hard error rather than a comment.
    //
    // The DAG defines "node complete" as "origFn returned" -- Fire() swaps the task's fn for
    // OnTaskFinishedWrapper, which calls origFn and then fires the dependents. For a coroutine task
    // origFn is the resume trampoline, and THAT RETURNS ON EVERY SUSPENSION, not on completion. A
    // coroutine node would therefore fire its dependents at its first co_await, while its own body
    // is still running -- silently producing wrong results rather than crashing. There is also no
    // sound ownership story: the DAG retires the node as its last touch, while a coroutine frees its
    // own Task from inside itself.
    //
    // FIBER NODES ARE FINE AND ARE THE SUPPORTED WAY TO SUSPEND. A fiber's ContextSwitch preserves
    // the wrapper's stack frame, so origFn returns only on real completion. To drive coroutines from
    // a DAG, make the node a Fiber task that Spawn()s them and then WaitFor()s the group: the fiber
    // suspends until they finish, and the node completes exactly when they do.
    if (t && t->type == TaskType::Coroutine) {
        std::fprintf(stderr,
            "[JLib::Scheduler] FATAL: TaskDAG::CreateNode was given a TaskType::Coroutine task.\n"
            "  The DAG treats 'the task's function returned' as 'the node finished', and resuming a\n"
            "  coroutine returns at every suspension -- so dependents would fire at the first\n"
            "  co_await, while the node is still running.\n"
            "  Use a TaskType::Fiber node that Spawn()s the coroutines and WaitFor()s them: the\n"
            "  fiber suspends until they complete, and the node finishes when they do.\n");
        std::fflush(stderr);
        std::abort();
    }

    // Stamp the graph's cancellation scope onto the task, so a Cancel() reaches it even after it
    // has been dispatched and is running -- Fire()'s flag only governs what has NOT been dispatched.
    // A task that already carries a token keeps it: an explicitly-scoped task is not overridden by
    // being put in a graph.
    if (t && t->cancelToken == CancelToken::kNone) t->cancelToken = scope.Token().Raw();

    // Allocate the node memory from the scheduler's allocator


    // TaskNode is 56 bytes -- the 64-byte class, not a 256-byte slot. It falls through to a
    // larger class if the small pool is dry, so an undersized pool costs memory, not failure.
    // AllocEdge keeps plain Alloc(): a chunk is deliberately one FULL slot, 16 edges wide.
    void* mem = scheduler.GetAllocator()->AllocSized(sizeof(TaskNode));
    if (!mem) return nullptr;

    // Use placement new
    TaskNode* node = new (mem) TaskNode(t, *scheduler.GetAllocator());

    // Set properties
    node->isLocal = (priority == NONE);
    node->priority = (priority == NONE) ? 0 : priority;
    node->cpuID = cpu_id;

    nodes.push_back(node);   // track for cycle detection / root discovery (build-time only)
    return node;
}

TaskNode* TaskDAG::CreateMainNode(Task* t, uint8_t priority) {
    // A MAIN NODE CANNOT SUSPEND, so it must not be given a task that is allowed to.
    //
    // Main-thread nodes run via PushMain/ProcessMainThread -- on the main thread, which is not a
    // fiber. TaskScheduler::IsOnFiber() is false there, so anything that suspends (WaitOnEvent,
    // WaitFor from a fiber path, a contended SchedulerMutex on a fiber) has no context to switch
    // away to and fail-fasts with NO MESSAGE (STATUS_STACK_BUFFER_OVERRUN on Windows). Accepting a
    // Fiber-typed task here compiled cleanly and looked correct, which is exactly why it needs to be
    // rejected loudly instead.
    //
    // Coroutines are refused one level down, in CreateNode, for a different reason -- see there.
    //
    // If the work needs to suspend, it does not belong on a main node. Use a pool node
    // (CreateNode with TaskType::Fiber) and, if the result must land on the main thread, make a
    // main node that DEPENDS on it.
    if (t && t->type == TaskType::Fiber) {
        std::fprintf(stderr,
            "[JLib::Scheduler] FATAL: TaskDAG::CreateMainNode was given a TaskType::Fiber task.\n"
            "  Main-thread nodes run on the main thread, which is not a fiber -- there is nothing to\n"
            "  switch away to, so any suspension inside one fail-fasts with no message.\n"
            "  Put suspending work on a pool node (CreateNode, TaskType::Fiber) and give the main\n"
            "  node a dependency on it.\n");
        std::fflush(stderr);
        std::abort();
    }

    TaskNode* node = CreateNode(t, priority, NONE);
    if (node) node->isMain = true;
    return node;
}

TaskNode* TaskDAG::CreateGate(TaskNode::LogicType type) {


    // TaskNode is 56 bytes -- the 64-byte class, not a 256-byte slot. It falls through to a
    // larger class if the small pool is dry, so an undersized pool costs memory, not failure.
    // AllocEdge keeps plain Alloc(): a chunk is deliberately one FULL slot, 16 edges wide.
    void* mem = scheduler.GetAllocator()->AllocSized(sizeof(TaskNode));
    if (!mem) return nullptr;
    // A gate carries no task; it just propagates readiness. Same allocator/list as a node.
    TaskNode* node = new (mem) TaskNode(nullptr, *scheduler.GetAllocator());
    node->isGate = true;
    node->gateType = type;
    nodes.push_back(node);
    return node;
}

TaskNode* TaskDAG::CreateExternalNode() {


    // TaskNode is 56 bytes -- the 64-byte class, not a 256-byte slot. It falls through to a
    // larger class if the small pool is dry, so an undersized pool costs memory, not failure.
    // AllocEdge keeps plain Alloc(): a chunk is deliberately one FULL slot, 16 edges wide.
    void* mem = scheduler.GetAllocator()->AllocSized(sizeof(TaskNode));
    if (!mem) return nullptr;
    // Taskless like a gate, and allocated from the same slab -- but Fire() arms it instead of
    // completing it. See the declaration for the contract and TaskNode::extBits for the rendezvous.
    TaskNode* node = new (mem) TaskNode(nullptr, *scheduler.GetAllocator());
    node->isExternal = true;
    // Set HERE rather than in Fire(), which is where a task node gets it -- an external node returns
    // from Fire before that line. Having the owner on the node means a signaller only needs the node
    // pointer, which is what lets a coroutine's promise carry 8 bytes instead of 16 (see
    // Coro::promise_type::dagNode in Coroutine.h).
    node->owner = this;
    nodes.push_back(node);
    return node;
}

void TaskDAG::SignalExternal(TaskNode* node) {
    if (!node) return;

    const int prev = node->extBits.fetch_or(TaskNode::EXT_SIGNALLED, std::memory_order_acq_rel);

    // Already signalled. Ignored rather than propagated a second time -- but this is NOT a
    // supported way to use the API: a completed node has been retired to the epoch manager, so a
    // late second call is reading memory that is on its way to being reused. The check exists to
    // make the race between two nearly-simultaneous signals resolve to one completion, not to make
    // signalling twice safe. See the declaration.
    if (prev & TaskNode::EXT_SIGNALLED) return;

    // Complete only if Fire() has already armed this node. If it has not, the dependencies are
    // still outstanding and Fire() will see our bit and complete it when they land -- which is the
    // whole point of the rendezvous: an external completion is allowed to arrive first.
    if (prev & TaskNode::EXT_ARMED) {
        OnTaskFinished(node);
    }
}

void JLib::SignalExternalNode(TaskNode* node) {
    // The whole point of the indirection: a caller needs only the node, not the DAG's definition.
    if (node && node->owner) node->owner->SignalExternal(node);
}

bool TaskDAG::HasCycle() {
    // Kahn's topological sort on a COPY of the in-degrees (the real ones drive
    // execution). If we can't drain every node, the survivors are exactly the nodes
    // tangled in (or downstream of) a cycle.
    std::unordered_map<TaskNode*, int> indeg;
    indeg.reserve(nodes.size());
    for (auto* n : nodes)
        indeg[n] = n->dependencies_left.load(std::memory_order_relaxed);

    std::vector<TaskNode*> ready;
    for (auto* n : nodes)
        if (indeg[n] == 0) ready.push_back(n);

    size_t processed = 0;
    while (!ready.empty()) {
        TaskNode* n = ready.back(); ready.pop_back();
        ++processed;
        // Raw walk, no epoch guard: this runs before Submit, when nothing else can see the
        // graph and no node can be retired underneath us.
        for (DagEdge* e = n->firstEdge; e; e = e->next) {
            TaskNode* dep = e->dep;
            if (--indeg[dep] == 0) ready.push_back(dep);
        }
    }
    return processed != nodes.size();
}

bool TaskDAG::Submit() {
    if (HasCycle()) {
        // A cyclic node never runs, so it never self-frees via OnTaskFinished -- reclaim
        // here (node + its task) so a rejected DAG doesn't leak. Caller should fix the graph.
        //
        // AND RELEASE THE WAITGROUPS, which this did not do. A task that is destroyed here never
        // runs, so nothing else will ever decrement for it -- and anyone sitting in WaitFor on this
        // graph then blocks forever. Rejecting a bad graph turned into hanging the caller who asked
        // to wait for it, which is a worse outcome than the cycle. Same sequence Worker()'s fast
        // path and TaskDAG::DisposeUnexecutedTask use.
        for (auto* n : nodes) {
            Task* t = n->task;
            n->~TaskNode();
            // FreeSized, NOT Free: the node may have come from the 64-byte pool, and returning it
            // to the 256-byte pool's free list is immediate heap corruption. Routed by ADDRESS, so
            // it is correct wherever AllocSized actually placed it.
            scheduler.GetAllocator()->FreeSized(n);
            if (t) {
                if (t->waitGroup) {
                    const int old = t->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
                    if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
                        t->waitGroup->WakeAll();
                }
                t->~Task();
                scheduler.GetAllocator()->FreeSized(t);
            }
        }
        nodes.clear();
        return false;
    }

    // Collect roots BEFORE submitting anything: once a node is submitted it can complete
    // and self-free on a worker, so we must not touch the tracking vector afterward.
    std::vector<TaskNode*> roots;
    for (auto* n : nodes)
        if (n->dependencies_left.load(std::memory_order_acquire) == 0)
            roots.push_back(n);

    nodes.clear();
    for (auto* r : roots)
        Fire(r);
    return true;
}

void TaskDAG::AddDependency(TaskNode* dependent, TaskNode* dependency) {
    if (!dependent || !dependency) return;

    // APPEND, AND DO NOT DEDUPLICATE. This used to key the entry on the dependent's own pointer so a
    // repeated edge was dropped -- which cost an O(n) search per edge AND was wrong: the counter was
    // incremented unconditionally while the duplicate add returned false, so
    //   AddDependency(b, a); AddDependency(b, a);
    // left b with dependencies_left == 2 and a with ONE dependent. The countdown could then only
    // reach 1, and Kahn's -- which uses dependencies_left as the in-degree -- decremented once and
    // never drained b, so HasCycle reported a cycle in an acyclic graph and Submit silently rejected
    // it. Verified: that exact pair hung with ran=0.
    //
    // Duplicates are self-consistent when nothing dedups them: two entries, two decrements, zero
    // reached, and Fire's `submitted` exchange makes the second fire a no-op anyway -- which is what
    // the dedup was guarding against and why it was never needed.
    //
    // Increment ONLY after the edge is really recorded. A push that fails on an exhausted slab must
    // not leave a phantom dependency behind, or the dependent waits forever for an edge that does
    // not exist. (Build is single-threaded -- see the note on TaskDAG::nodes -- so there is no race
    // between the push and the increment.)
    DagEdge* e = AllocEdge();
    if (e) {
        e->dep  = dependent;
        e->next = dependency->firstEdge;   // prepend: order among dependents is irrelevant,
        dependency->firstEdge = e;         // every one of them is fired
        dependent->dependencies_left.fetch_add(1, std::memory_order_relaxed);
    }
}


// Bump-allocate one edge cell from the current chunk, cutting a new chunk from the SLAB when it is
// full. Single-threaded by the same contract as the rest of graph construction.
//
// A chunk is exactly one slab slot, so sizeof(DagEdge) divides into it evenly and no space is
// wasted at the tail.
JLib::DagEdge* JLib::TaskDAG::AllocEdge() {
    if (edgeCursor == kEdgeChunk) {
        void* mem = scheduler.GetAllocator()->Alloc();
        if (!mem) return nullptr;                 // slab dry; caller must not count the edge
        edgeChunks.push_back(static_cast<DagEdge*>(mem));
        edgeCursor = 0;
    }
    DagEdge* e = &edgeChunks.back()[edgeCursor++];
    e->dep = nullptr;
    e->next = nullptr;
    return e;
}

JLib::TaskDAG::~TaskDAG() {
    // Raw slots holding trivially-destructible DagEdge cells, so there is nothing to destroy --
    // just hand the memory back.
    TaskAllocator* a = scheduler.GetAllocator();
    for (DagEdge* chunk : edgeChunks) a->Free(chunk);
}


void TaskDAG::OnTaskFinished(TaskNode* node, TaskNode::Outcome outcome) {
    // Trigger each dependent. AND fires when its countdown reaches 0; OR fires on the
    // FIRST predecessor -- Fire's `submitted` exchange turns later predecessors into no-ops.
    //
    // CANCELLATION RIDES THE SAME WALK. A cancelled predecessor fires its dependents IMMEDIATELY as
    // cancelled, whatever the gate:
    //   AND -- an input that will never produce means the countdown can never legitimately reach 0,
    //          so waiting for it would strand the node forever. Fire now, cancelled.
    //   OR  -- first result wins, which is what `submitted` already enforces. If the first arrival
    //          is a cancellation the gate is cancelled, and a later OK is a no-op.
    // The countdown is still decremented on the normal path only; once a cancel has fired the node,
    // any later completion is deduped by `submitted` regardless of what the counter says.
    ForEachDependent(node, [this, outcome](TaskNode* dep) {
        if (outcome == TaskNode::Outcome::Cancelled) {
            Fire(dep, TaskNode::Outcome::Cancelled);
            return;
        }
        bool ready = (dep->gateType == TaskNode::OR)
            ? true
            : (dep->dependencies_left.fetch_sub(1, std::memory_order_acq_rel) - 1 == 0);
        if (ready) Fire(dep);
        });
    // Retire (don't immediately free): an OR dependent fires on its FIRST predecessor and
    // then runs + finishes, but LATER predecessors still hold this node in their
    // edge lists and dereference it inside their (epoch-guarded) ForEachDependent. EBR keeps
    // it alive until no such reader can still see it. (AND is safe either way, but uniform
    // retire is simplest.)
    EpochManager::Instance().RetirePtr(node, EpochManager::Instance().CurrentEpoch(), &TaskDAG::NodeDeleter);
}

void TaskDAG::NodeDeleter(void* p) {
    auto* n = static_cast<TaskNode*>(p);
    // The node no longer carries an allocator reference -- it stopped allocating when the
    // dependents list became DAG-owned chunk storage. This is the same allocator it was
    // built with: CreateNode/CreateGate/CreateExternalNode all pass *scheduler.GetAllocator().
    TaskAllocator* a = TaskScheduler::Instance().GetAllocator();
    n->~TaskNode();
    // FreeSized for the same reason as the cycle path above -- see there.
    a->FreeSized(n);
}


void TaskDAG::Cancel() {
    // One flag, read at the top of Fire(). Fire is the single funnel every node passes through on
    // its way to being dispatched, so this converts the entire remaining graph as it unrolls --
    // without a node registry, which Submit() deliberately does not keep (nodes self-free after it).
    // Already-dispatched work is untouched; see the declaration for exactly what that excludes.
    cancelled.store(true, std::memory_order_release);
    // The other half: tasks already dispatched cannot be un-dispatched, but they can be TOLD.
    // Cancelling the scope makes CurrentTaskCancelled() true inside every node's task, so a
    // long-running body that polls can give up instead of finishing work nobody wants.
    scope.Cancel();
}

void TaskDAG::DisposeUnexecutedTask(TaskNode* node) {
    Task* t = node->task;
    if (!t) return;                 // gate or external node: nothing was ever allocated to run
    node->task = nullptr;           // exactly once, even if something re-enters

    // THE WAITGROUP MUST STILL BE DECREMENTED. A cancelled task never runs, so nothing else will
    // ever decrement for it -- and a caller sitting in WaitFor would then block forever on work that
    // has been abandoned. Cancelling a graph would deadlock whoever asked to wait for it, which is
    // the opposite of the point. Same sequence Worker()'s fast path uses, minus the Execute.
    if (t->waitGroup) {
        const int old = t->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
        if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
            t->waitGroup->WakeAll();
    }
    scheduler.CleanupTaskMetadata(t);
    DestroyTask(t);
    scheduler.GetAllocator()->FreeSized(t);
}

void TaskDAG::Fire(TaskNode* node, TaskNode::Outcome outcome) {

    if (node->submitted.exchange(true, std::memory_order_acq_rel)) {
        return; // already fired by another predecessor (dedups OR, and AND races)
    }

    // A graph-wide cancel demotes anything not yet dispatched, and this is the only place it needs
    // checking: every node reaches dispatch through here exactly once, guarded by the exchange above.
    if (cancelled.load(std::memory_order_acquire)) {
        outcome = TaskNode::Outcome::Cancelled;
    }

    if (outcome == TaskNode::Outcome::Cancelled) {
        // A CANCELLED node is a pure state transition: its payload never runs, whatever kind of node
        // it is. Release the task it would have run -- nothing else ever will -- then propagate, so
        // the dependents see the cancellation and decide by their own gate type.
        //
        // For an EXTERNAL node this also means it is never armed, so a signal that arrives later
        // finds EXT_ARMED unset and does nothing. That is the correct outcome and not a lost wakeup:
        // the node has already completed, as cancelled.
        DisposeUnexecutedTask(node);
        OnTaskFinished(node, TaskNode::Outcome::Cancelled);
        return;
    }

    if (node->isGate) {
        // No task to schedule: the gate "completes" instantly, so propagate to its own
        // dependents right now. This recurses through chains of gates (depth = the depth
        // of the boolean expression), all on the firing thread.
        OnTaskFinished(node);
        return;
    }

    if (node->isExternal) {
        // Also taskless, but the opposite of a gate: firing does NOT complete it. Its
        // dependencies are now satisfied, so it becomes eligible to be completed by
        // SignalExternal -- and nothing else happens here. No task is pushed, no fiber is
        // taken, no worker is occupied while it waits.
        //
        // The signal may already have arrived (an I/O can complete before the nodes it depends
        // on do), so the arm has to be a rendezvous rather than a plain store: whoever sets the
        // second bit propagates. See TaskNode::extBits.
        const int prev = node->extBits.fetch_or(TaskNode::EXT_ARMED, std::memory_order_acq_rel);
        if (prev & TaskNode::EXT_SIGNALLED) {
            OnTaskFinished(node);   // signalled early -- we are the second arrival, so we complete
        }
        return;
    }

    // Wrap the task's own fn/data with the completion trampoline (see OnTaskFinishedWrapper) --
    // the originals are saved INTO THE NODE and invoked first, so behavior is identical to the
    // old Task::onComplete hook this replaced. Works for LambdaTask too: its origData is the
    // LambdaTask itself, carried through untouched. The node doubles as the trampoline's
    // context (task->data = node) -- no allocation; see TaskNode.h's embedded-context comment.
    node->owner = this;
    node->origFn = node->task->fn;
    node->origData = node->task->data;
    node->task->fn = &OnTaskFinishedWrapper;
    node->task->data = node;

    if (node->isMain) {
        scheduler.PushMain(node->task);
    }
    else if (node->isFork) {
        scheduler.PushImmediate(node->cpuID, node->task);
    }
    else if (node->isLocal) {
        if (node->cpuID == 0)
            scheduler.Push(node->task);
        else
            scheduler.Push(node->cpuID, node->task);
    }

}
