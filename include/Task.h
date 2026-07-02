#pragma once
#include <functional>
#include <atomic>

namespace T_Threads {
    struct Fiber;
    struct WaitGroup { std::atomic<int> n{ 0 }; };

    enum class FiberSize : uint8_t { Standard, Heavy };
    struct alignas(16) Task {
        using Func = void(*)(void*);

        Func fn;
        void* data = nullptr;
        Fiber* assignedFiber = nullptr;
        std::atomic<uint8_t> stopFlag{ false };
        // MUST default to nullptr: TaskAllocator recycles slab memory WITHOUT zeroing it (a
        // pure intrusive free-list), and neither constructor below sets these. Without an
        // explicit default, a task placement-new'd into a slot PREVIOUSLY used by a TaskDAG
        // node (which sets onComplete=&OnTaskFinishedWrapper) inherits that STALE pointer --
        // its Execute() then calls onComplete with a dangling onCompleteData (an already-
        // deleted TaskFinishedContext*), reading garbage -> crash. Only mattered once TaskDAG
        // started actually setting onComplete (2026-07-01); every other task type shares this
        // same allocator/slab pool and never explicitly clears it.
        Func onComplete = nullptr;
        void* onCompleteData = nullptr;
        std::atomic<uint8_t> callbackFlag{ false };
        std::atomic<Task*> next{ nullptr };
        WaitGroup* waitGroup = nullptr;
        uint8_t hiPri = false;
        FiberSize requiredSize = FiberSize::Standard;

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

            // Use exchange to ensure ONLY ONE thread ever fires onComplete()
            if (onComplete && !callbackFlag.exchange(true, std::memory_order_acq_rel)) {
                onComplete(onCompleteData);
            }

            if (waitGroup) waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
        }

        inline void SignalComplete() {
            // Only fire if we haven't already
            if (!callbackFlag.exchange(true, std::memory_order_acq_rel)) {
                if (onComplete) {
                    onComplete(onCompleteData);
                }
            }
        }
        inline void Stop() {
            stopFlag.store(true, std::memory_order_release);
        }
    };

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