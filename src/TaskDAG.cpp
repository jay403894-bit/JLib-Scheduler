#include "../include/TaskDAG.h"
using namespace T_Threads;

TaskNode* TaskDAG::CreateNode(Task* t, uint8_t priority, uint8_t cpu_id) {
    // Allocate the node memory from the scheduler's arena
    void* mem = scheduler.GetAllocator()->Alloc();
    if (!mem) return nullptr;

    // Use placement new
    TaskNode* node = new (mem) TaskNode(t);

    // Set properties
    node->isLocal = (priority == NONE);
    node->priority = (priority == NONE) ? 0 : priority;
    node->cpuID = cpu_id;

    // Optional: Keep a reference if you really need to, 
    // but you don't need unique_ptr anymore!
    return node;
}

void TaskDAG::AddDependency(TaskNode* dependent, TaskNode* dependency) {
    // Increment the dependency count
    dependent->dependencies_left.fetch_add(1, std::memory_order_relaxed);

    // Add the dependent node to the dependency's list
    // We use the pointer address as the key
    uint64_t key = reinterpret_cast<uintptr_t>(dependent);
    // Store the dependent NODE (not its Task): OnTaskFinished's for_each reads
    // dep->dependencies_left, so the list must hold TaskNode*, matching LockFreeList<TaskNode*>.
    dependency->dependents->add(key, dependent);
}

void TaskDAG::SubmitIfReady(TaskNode* node) {
    if (node->dependencies_left.load(std::memory_order_acquire) == 0) {
        SubmitToScheduler(node);
    }
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