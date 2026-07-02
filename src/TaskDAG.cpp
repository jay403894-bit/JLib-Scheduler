#include "../include/TaskDAG.h"
using namespace T_Threads;

TaskNode* TaskDAG::CreateNode(Task* t, uint8_t priority, uint8_t cpu_id) {
    // Allocate the node memory from the scheduler's allocator
    void* mem = scheduler.GetAllocator()->Alloc();
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
    TaskNode* node = CreateNode(t, priority, NONE);
    if (node) node->isMain = true;
    return node;
}

TaskNode* TaskDAG::CreateGate(TaskNode::LogicType type) {
    void* mem = scheduler.GetAllocator()->Alloc();
    if (!mem) return nullptr;
    // A gate carries no task; it just propagates readiness. Same allocator/list as a node.
    TaskNode* node = new (mem) TaskNode(nullptr, *scheduler.GetAllocator());
    node->isGate = true;
    node->gateType = type;
    nodes.push_back(node);
    return node;
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
        n->dependents->for_each([&](TaskNode* dep) {   // forward edges
            if (--indeg[dep] == 0) ready.push_back(dep);
        });
    }
    return processed != nodes.size();
}

bool TaskDAG::Submit() {
    if (HasCycle()) {
        // A cyclic node never runs, so it never self-frees via OnTaskFinished -- reclaim
        // here (node + its task) so a rejected DAG doesn't leak. Caller should fix the graph.
        for (auto* n : nodes) {
            Task* t = n->task;
            n->~TaskNode();
            scheduler.GetAllocator()->Free(n);
            if (t) {
                t->~Task();
                scheduler.GetAllocator()->Free(t);
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
    dependent->dependencies_left.fetch_add(1, std::memory_order_relaxed);
    uint64_t key = reinterpret_cast<uintptr_t>(dependent);
    dependency->dependents->add(key, dependent);
}


void TaskDAG::OnTaskFinished(TaskNode* node) {
    // Trigger each dependent. AND fires when its countdown reaches 0; OR fires on the
    // FIRST predecessor -- Fire's `submitted` exchange turns later predecessors into no-ops.
    node->dependents->for_each([this](TaskNode* dep) {
        bool ready = (dep->gateType == TaskNode::OR)
            ? true
            : (dep->dependencies_left.fetch_sub(1, std::memory_order_acq_rel) - 1 == 0);
        if (ready) Fire(dep);
        });
    // Retire (don't immediately free): an OR dependent fires on its FIRST predecessor and
    // then runs + finishes, but LATER predecessors still hold this node in their dependents
    // lists and read it inside their (epoch-guarded) for_each. EBR keeps it alive until no
    // such reader can still see it. (AND is safe either way, but uniform retire is simplest.)
    EpochManager::Instance().RetirePtr(node, EpochManager::Instance().CurrentEpoch(), &TaskDAG::NodeDeleter);
}

void TaskDAG::NodeDeleter(void* p) {
    auto* n = static_cast<TaskNode*>(p);
    TaskAllocator* a = &n->alloc;   // grab the allocator before destroying the node
    n->~TaskNode();
    a->Free(n);
}


void TaskDAG::Fire(TaskNode* node) {

    if (node->submitted.exchange(true, std::memory_order_acq_rel)) {
        return; // already fired by another predecessor (dedups OR, and AND races)
    }

    if (node->isGate) {
        // No task to schedule: the gate "completes" instantly, so propagate to its own
        // dependents right now. This recurses through chains of gates (depth = the depth
        // of the boolean expression), all on the firing thread.
        OnTaskFinished(node);
        return;
    }

    auto* ctx = new TaskFinishedContext{ this, node };
    node->task->onComplete = &OnTaskFinishedWrapper;
    node->task->onCompleteData = ctx;

    if (node->isMain) {
        scheduler.PushMain(node->task);
    }
    else if (node->isFork) {
        scheduler.PushFork(node->cpuID, node->task);
    }
    else if (node->isLocal) {
        if (node->cpuID == 0)
            scheduler.Push(node->task);
        else
            scheduler.Push(node->cpuID, node->task);
    }

}