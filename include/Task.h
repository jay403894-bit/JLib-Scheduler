#pragma once
#include <functional>
#include <atomic>
namespace T_Threads {
    struct Fiber;
    struct WaitGroup { std::atomic<int> n{ 0 }; };

    enum class FiberSize { Standard, Heavy };
    struct Task {
        using Func = void(*)(void*);

        Func fn;
        void* data = nullptr;
        Fiber* assignedFiber = nullptr; 
        std::atomic<bool> stopFlag{ false };
        std::function<void()> onComplete;
        std::atomic<bool> complete{ false };
        std::atomic<bool> callbackFlag{ false };
        std::atomic<Task*> next{ nullptr };
        WaitGroup* waitGroup = nullptr;
		bool ownedBySlab = false; // If true, the task is allocated from the slab and should be reclaimed there
        FiberSize requiredSize = FiberSize::Standard;
        Task() : next(nullptr), complete(false), fn(nullptr), data(nullptr), assignedFiber(nullptr) {}
        Task(Func f, void* d = nullptr, FiberSize size = FiberSize::Standard)
            : fn(f), data(d) {
        }
        virtual ~Task() = default;

        inline void Execute() noexcept {
             fn(data);
             if (onComplete && !callbackFlag.load(std::memory_order_acquire)) onComplete();
             if (waitGroup) waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
            complete.store(true, std::memory_order_release);
        }

        inline void SignalComplete() {
            // Only fire if we haven't already
            if (!callbackFlag.exchange(true, std::memory_order_acq_rel)) {
                if (onComplete) {
                    onComplete();
                }
            }
        }
        inline void Stop() {
            stopFlag.store(true, std::memory_order_release);
        }
    };

    template<typename F>
    class LambdaTask : public Task {
        F func;
    public:
        // Ensure we use perfect forwarding for the lambda
        LambdaTask(F&& f)
            : Task(LambdaTask::ExecuteWrapper, nullptr),
            func(std::forward<F>(f))
        {
            this->data = this;
        }

    private:
        static void ExecuteWrapper(void* ptr) {
            // Cast the void* back to the correct LambdaTask type
            LambdaTask* self = static_cast<LambdaTask*>(ptr);

            // Invoke the stored lambda directly.
            // We use () because a lambda IS a functor.
            self->func();
        }
    };
 
};