#pragma once
#include <functional>
#include <atomic>
#include <mutex>
#include <unordered_set>

namespace JLib {
    struct Fiber;
    struct Task;
    struct DirectEvent;
    struct WaitGroup {
        static constexpr int WAITER_BIT = 0x40000000;
        static constexpr int COUNT_MASK = WAITER_BIT - 1;   // counts 
        std::atomic<int> n{ 0 };
        std::mutex mtx;
        std::unordered_set<DirectEvent*> waiters;  // Tasks suspended on this WaitGroup

        void WakeAll();
    };

    enum class FiberSize : uint8_t { Standard, Heavy };
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
        uint8_t fastJob = 0;
        uint8_t isForked = 0;  // Set by PushFork, cleared when task completes
        uint8_t priorityBoost = 0;  // Original priority before boost (0 = no boost, otherwise original hiPri)

        Task() : next(nullptr), fn(nullptr), data(nullptr), assignedFiber(nullptr) { ; }
        Task(Func f, void* d = nullptr, uint8_t hipri =false, FiberSize size = FiberSize::Standard)
            : fn(f), data(d), hiPri(hipri), requiredSize(size) {
        }
        virtual ~Task() {

        }

        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) = delete;
        void operator delete[](void*) = delete;

        inline void Execute() noexcept {
            fn(data);
        }
    };
    // One cache line, exactly -- if this fires, a new field pushed Task over 64 bytes and every
    // per-task access just started paying a second line. Grow deliberately or shrink elsewhere.
    static_assert(sizeof(Task) == 64, "Task must stay exactly one 64-byte cache line");

    template<typename F>
    class alignas(16) LambdaTask : public Task {
        F func;
    public:
        static_assert(sizeof(F) <= (256 - sizeof(Task)), "LambdaTask exceeds 256-byte slab capacity");
        LambdaTask(F&& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(std::forward<F>(f))
        {
            this->data = this;
        }
		~LambdaTask() {
		}
        void* operator new(std::size_t) = delete;
        void* operator new[](std::size_t) = delete;
        void operator delete(void*) = delete;
        void operator delete[](void*) = delete;

    private:
        static void ExecuteWrapper(void* ptr) {
            LambdaTask* self = static_cast<LambdaTask*>(ptr);

            self->func();
        }
    };
 
};