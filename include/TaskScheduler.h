#pragma once
#define NOMINMAX
#include "Task.h"
#include "TaskMPSCQueue.h"
#include "Epochs.h"
#include "TaskDeque.h"
#include "TaskAllocator.h"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <mutex>
#include <functional>
#include <string>
#include <unordered_map>
#include <thread>
#include <immintrin.h>
#include "GlobalFiberPool.h"
namespace T_Threads {
	class T_Thread;
	class Event;

	class TaskScheduler {
		friend class T_Thread;
		friend class GlobalFiberPool;


	public:

		static TaskScheduler& Instance() {
			if (!instance)
				throw std::runtime_error("Call TaskScheduler::Init() before Instance()!");
			return *instance;
		}
		static void Init(size_t poolSize = 0); // 0 = auto-detect

		~TaskScheduler();
		bool EnqueueToMain(Task* task);
		void ProcessMainThread();
		void Join();
		void NotifyAll();
		void ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func);
		void ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func);
		bool Push(Task* task);
		void WaitFor(WaitGroup& wg);
		bool Push(uint8_t cpu_affinity, Task* task);
		bool Requeue(Task* task);
		void PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity=0);
		bool PushFork(uint8_t cpu_affinity, Task* task);
		GlobalFiberPool& GetGlobalPool();
		Event& GetEvent(const std::string& name);
		void WaitOnEvent(const std::string& eventName);
		void Pause();
		void Resume();
		void Stop(Task* worker_task);
		TaskAllocator* GetAllocator();
		void WaitAll();
		 
		Task* CreateTask(void(*fn)(void*), void* data, uint8_t hipri = false, FiberSize size = FiberSize::Standard);

		template<typename F>
		auto CreateTask(F&& f, uint8_t hipri = false, FiberSize size = FiberSize::Standard) {
			using L = LambdaTask<std::decay_t<F>>;
			static_assert(sizeof(L) <= TaskAllocator::SLOT, "lambda too big for a slot");
			static_assert(alignof(L) <= 16, "lambda over-aligned for the slot");
			void* mem = taskAllocator.Alloc();
			if (!mem) return static_cast<L*>(nullptr);
			L* t = ::new (mem) L(std::forward<F>(f));
 			t->hiPri = hipri;
			t->requiredSize = size;
			return t;
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void Push(uint8_t cpu_affinity, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushLocal(t, cpu_affinity);
		}
		template <class F, std::enable_if_t<!std::is_base_of_v<Task, std::remove_pointer_t<std::decay_t<F>>>, int> = 0>
		void PushFork(size_t coreID, F&& f) {
			auto* t = CreateTask(std::forward<F>(f));
			PushToCore(coreID, t);
		}

	private:
		explicit TaskScheduler(size_t poolSize);

		// ---------- former SharedQueues state ----------
		std::atomic<uint64_t> nextId{ 0 };
		std::atomic<int> pendingTasks{ 0 };
		std::vector<std::unique_ptr<std::atomic<bool>>> immediateCoresInUse;
		std::atomic<bool> paused{ false };
		std::vector<std::unique_ptr<TaskDeque>> loPri;
		std::vector<std::unique_ptr<TaskDeque>> hiPri;
		std::vector<std::unique_ptr<TaskMPSCQueue>> loPriInboxes;
		std::vector<std::unique_ptr<TaskMPSCQueue>> hiPriInboxes;
		static GlobalFiberPool* globalPool;
		// -----------------------------------------------


		void RunCounted(WaitGroup& wg, Task* t);
		static size_t GetSafeTC();
		Task* GetTask();
		void StartPool(size_t poolSize);
		bool PushLocal(Task* task, uint8_t cpuaffinity = 0);
		bool PushToCore(size_t core_id, Task* task);
		int PickNextWorker();

		static TaskScheduler* instance;
		TaskAllocator taskAllocator{ 1024 * 1024 }; // 1M tasks
		std::unordered_map<std::string, std::unique_ptr<Event>> eventRegistry;
		std::mutex registryMtx;
		std::atomic<bool> poolActive{ false };
		std::atomic<int> nextWorker{ 0 };
		std::atomic<bool> stopFlag{ false };
		std::vector<std::shared_ptr<T_Thread>> workers;
		TaskMPSCQueue mainQ;
		std::mutex poolMutex;
	};
}
