#include "../include/TaskDAG.h"
using namespace T_Threads;

TaskNode* TaskDAG::CreateNode(Task* t, uint8_t priority, uint8_t cpu_id) {
    // Allocate the node memory from the scheduler's allocator
    void* mem = scheduler.GetAllocator()->Alloc();
    if (!mem) return nullptr;

    // Use placement new
    TaskNode* node = new (mem) TaskNode(t);

    // Set properties
    node->isLocal = (priority == NONE);
    node->priority = (priority == NONE) ? 0 : priority;
    node->cpuID = cpu_id;

    nodes.push_back(node);   // track for cycle detection / root discovery (build-time only)
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
                bool slab = t->ownedBySlab;
                t->~Task();
                if (slab) scheduler.GetAllocator()->Free(t);
                else      ::operator delete(t);
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
        SubmitToScheduler(r);
    return true;
}

void TaskDAG::AddDependency(TaskNode* dependent, TaskNode* dependency) {
    dependent->dependencies_left.fetch_add(1, std::memory_order_relaxed);
    uint64_t key = reinterpret_cast<uintptr_t>(dependent);
    dependency->dependents->add(key, dependent);
}


void TaskDAG::OnTaskFinished(TaskNode* node) {
    // Traverse the lock-Free list and trigger dependents
    node->dependents->for_each([this](TaskNode* dep) {
        int val = dep->dependencies_left.fetch_sub(1, std::memory_order_acq_rel) - 1;
        if (val == 0) {
            SubmitToScheduler(dep);
        }
        });
    node->~TaskNode();
	scheduler.GetAllocator()->Free(node);
}


void TaskDAG::SubmitToScheduler(TaskNode* node) {

    if (node->submitted.exchange(true, std::memory_order_acq_rel)) {
        return; // Already submitted by another thread, do nothing!
    }
    node->task->onComplete = [node, this]() {
        this->OnTaskFinished(node);
        };

    if (node->isFork) {
        scheduler.PushFork(node->cpuID, node->task);
    }
    else if (node->isLocal) {
        if (node->cpuID == 0)
            scheduler.Push(node->task);
        else
            scheduler.Push(node->cpuID, node->task);
    }
  
}