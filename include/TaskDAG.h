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

namespace T_Threads {

    class TaskDAG {
        struct TaskFinishedContext {
            TaskDAG* parent;
            TaskNode* node;
        };
    public:
        TaskDAG(TaskScheduler& sched) : scheduler(sched) {};
        TaskNode* CreateNode(Task* t, uint8_t priority = NONE, uint8_t cpu_id = NONE);
        // A gate has no task; it fires its dependents instantly when its trigger is met.
        // Compose gates to express arbitrary boolean readiness, e.g. (A && B) || C.
        TaskNode* CreateGate(TaskNode::LogicType type);
         void AddDependency(TaskNode* dependent, TaskNode* dependency);

         static void OnTaskFinishedWrapper(void* data) {
             // You have to cast the void* back to whatever struct contains your context
             auto* context = static_cast<TaskFinishedContext*>(data);
             context->parent->OnTaskFinished(context->node);
             delete context; // Cleanup
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

        void OnTaskFinished(TaskNode* node);
        void EndFrame();
    private:
        TaskScheduler& scheduler;
        // Tracks every node created this build, for cycle detection / root discovery.
        // Single-threaded build only; entries dangle after Submit() (nodes self-free),
        // so it is cleared there and never iterated post-submit.
        std::vector<TaskNode*> nodes;

        void Fire(TaskNode* node);   // run the node (or, for a gate, propagate instantly)
        static void NodeDeleter(void* p);   // EBR deleter: ~TaskNode + return its slot to the slab
    };
};