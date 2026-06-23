#include "../include/T_Thread.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
using namespace T_Threads;
TaskScheduler* TaskScheduler::instance = nullptr;

TaskScheduler::TaskScheduler(size_t poolSize) {
	StartPool(poolSize);
}
size_t TaskScheduler::GetSafeTC() {
	unsigned int cores = std::thread::hardware_concurrency();
	if (cores == 0) return 1;
	if (cores == 1) return 1;
	return static_cast<size_t>(cores - 1);
}
void TaskScheduler::Init(size_t poolSize) {
	if (instance != nullptr)
		throw std::runtime_error("TaskScheduler already initialized!");
	instance = new TaskScheduler(poolSize);
}
TaskScheduler::~TaskScheduler() {
	if (!stopFlag)
		Join();
}
bool TaskScheduler::EnqueueToMain(Task* task) {
	if (!poolActive) return false;
	if (!task) return false;
	mainQ.push(task);
	return true;
}
void TaskScheduler::ProcessMainThread() {
	if (!poolActive) return;
	Task* t;
	while (mainQ.pop(t)) {
		if (!t) continue;
		t->Execute();
	}
}
void TaskScheduler::Join() {
	if (!poolActive) return;

	stopFlag.store(true, std::memory_order_release);

	{
		std::lock_guard<std::mutex> lock(registryMtx);
		for (auto& pair : eventRegistry)
			pair.second->SignalAll();
	}
	NotifyAll();

	for (auto& worker : workers)
		worker->Join();

	{
		std::lock_guard<std::mutex> lock(poolMutex);
		workers.clear();
		mainQ.clear();
		immediateCoresInUse.clear();
	}

	poolActive.store(false, std::memory_order_release);
}
void TaskScheduler::NotifyAll() {
	for (auto& w : workers)
		w->NotifyWorker();
}
void TaskScheduler::ParallelFor(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;

	// Holds only the tasks we successfully allocated.
	Task** taskPtrs = new Task * [numTasks];
	int created = 0;

	// 1. Create tasks. If the arena is exhausted (CreateTask returns nullptr),
	//    run that chunk inline on the caller rather than dereferencing null.
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);

		Task* t = CreateTask([=]() { func(chunkStart, chunkEnd); });
		if (!t) {
			func(chunkStart, chunkEnd); // graceful degradation: do it here
			continue;
		}
		taskPtrs[created++] = t;
	}

	if (created > 0) {
		runningTasks.fetch_add(created, std::memory_order_relaxed);

		// 2. Distribute across all workers' inboxes (round-robin). Dumping the
		//    whole batch on one worker pins it in the inbox->deque drain loop and
		//    hangs when created > deque capacity (large range / small chunkSize)
		//    or when there's only one worker. (push handles the next-link itself.)
		size_t n = inboxes.size();
		for (int i = 0; i < created; ++i) {
			inboxes[i % n]->push(taskPtrs[i]);
		}
		NotifyAll();

		// 3. Wait for completion, but HELP while waiting so the calling thread
		//    makes progress too. Pure spinning deadlocks when ParallelFor runs on
		//    a worker fiber (nested) or when there's only one worker. GetTask()
		//    steals a task out of a deque -- removed exactly once -- so running it
		//    here can't double-execute; we decrement runningTasks because the
		//    owning worker will never see it.
		for (int i = 0; i < created; ++i) {
			while (!taskPtrs[i]->complete.load(std::memory_order_acquire)) {
				Task* helped = GetTask();
				if (helped) {
					helped->Execute();
					runningTasks.fetch_sub(1, std::memory_order_acq_rel);
				}
				else {
					std::this_thread::yield();
				}
			}
		}
	}

	delete[] taskPtrs;

	// 4. Batch is done and no longer referenced by us. Recycle the arena it
	//    came from — but only if the whole system is quiescent (see RecycleArena).
	RecycleArena();
}
void TaskScheduler::ParallelForNB(int start, int end, int chunkSize, std::function<void(int, int)> func) {
	chunkSize = std::max(1, chunkSize);
	int totalItems = end - start;
	if (totalItems <= 0) return;

	int numTasks = (totalItems + chunkSize - 1) / chunkSize;
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);
		Push([=]() { func(chunkStart, chunkEnd); });
	}
}
void TaskScheduler::StartPool(size_t poolSize) {
	std::lock_guard<std::mutex> lock(poolMutex);

	if (poolSize == 0)
		poolSize = GetSafeTC();
	if (poolSize > GetSafeTC())
		poolSize = GetSafeTC();

	unsigned int num_workers = static_cast<unsigned int>(poolSize);
	EpochManager::Instance().Init(num_workers);
	stopFlag.store(false, std::memory_order_release);
	nextWorker = 0;

	const size_t fibersPerWorker = 32;
	globalPool = std::make_unique<FiberPool>(num_workers * fibersPerWorker);

	workers.clear();
	threadQs.clear();
	immediateCoresInUse.clear();
	inboxes.clear();
	workers.reserve(num_workers);
	threadQs.reserve(num_workers);
	immediateCoresInUse.reserve(num_workers);
	inboxes.reserve(num_workers);

	for (unsigned int i = 0; i < num_workers; ++i) {
		immediateCoresInUse.push_back(std::make_unique<std::atomic<bool>>(false));
		threadQs.push_back(std::make_unique<TaskDeque>());
		inboxes.push_back(std::make_unique<MPSCQueue<Task*>>());
	}
	for (unsigned int i = 0; i < num_workers; ++i) {
		auto worker = std::make_shared<T_Thread>(*this);
		worker->SetQueueIndex(i);
		workers.push_back(worker);
		worker->StartWorker(i + 1);
	}
	for (auto& w : workers) {
		while (!w->Ready())
			std::this_thread::yield();
	}
	poolActive.store(true, std::memory_order_release);
}

void TaskScheduler::WaitOnEvent(const std::string& eventName) {
	auto* thread = T_Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;

	auto& event = GetEvent(eventName);
	event.AddWaiter(myTask);

	ContextSwitch(&myFiber->ctx, &thread->schedulerCtx);
}
bool TaskScheduler::Push(Task* task) {
	return PushLocal(task);
}

bool T_Threads::TaskScheduler::PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity = 0)
{
	return PushBatchLocal(tasks, count, cpuaffinity);
}

bool TaskScheduler::Push(uint8_t cpu_affinity, Task* task) {
	return PushLocal(task, cpu_affinity);
}

bool TaskScheduler::PushFork(uint8_t cpu_affinity, Task* task) {
	if (!task) return false;
	return Push(cpu_affinity, task);
}
Event& TaskScheduler::GetEvent(const std::string& name) {
	std::lock_guard<std::mutex> lock(registryMtx);
	if (eventRegistry.find(name) == eventRegistry.end())
		eventRegistry[name] = std::make_unique<Event>();
	return *eventRegistry[name];
}
void TaskScheduler::Pause() {
	paused.store(true, std::memory_order_release);
}
void TaskScheduler::Resume() {
	paused.store(false, std::memory_order_release);
	NotifyAll();
}
void TaskScheduler::Stop(Task* worker_task) {
	stopFlag.store(true, std::memory_order_release);
	worker_task->Stop();
}
Task* TaskScheduler::GetTask() {
	Task* task_to_run = nullptr;
	Task* task;

	size_t numThreads = threadQs.size();
	size_t start = rand() % numThreads;
	for (size_t i = 0; i < numThreads; ++i) {
		size_t target = (start + i) % numThreads;
		auto opt = threadQs[target]->steal();
		if (opt) {
			task_to_run = *opt;
			current_task = task_to_run;
			break;
		}
	}

	return task_to_run;
}
void TaskScheduler::Wait(const std::vector<Task*>& tasks) {
	for (auto* t : tasks) {
		while (!t->complete.load(std::memory_order_acquire)) {
			Task* task = GetTask();
			if (task) {
				task->Execute();   // Execute() sets complete; decrement since the
				runningTasks.fetch_sub(1, std::memory_order_acq_rel); // worker won't
			}
			else
				std::this_thread::yield();
		}
	}
	// Tasks waited on are done; recycle if the system is quiescent.
	RecycleArena();
}
void TaskScheduler::WaitAll() {
	while (runningTasks.load(std::memory_order_acquire) > 0)
		std::this_thread::yield();
}
Arena* TaskScheduler::GetArena() {
	return taskArena.GetActive();
}
void TaskScheduler::RecycleArena() {
	for (int spins = 0; spins < 4096; ++spins) {
		if (runningTasks.load(std::memory_order_acquire) == 0) break;
		std::this_thread::yield();
	}
	if (runningTasks.load(std::memory_order_acquire) != 0) return;

	size_t epoch = EpochManager::Instance().CurrentEpoch();
	Arena* active = taskArena.GetActive();
	EpochManager::Instance().RetireArena(active, epoch);
	taskArena.Rotate();
	EpochManager::Instance().Tick();
}
Task* TaskScheduler::CreateTask(void(*fn)(void*), void* data) {
	void* mem = taskArena.GetActive()->allocate(sizeof(Task));
	if (!mem) return nullptr;
	return new (mem) Task(fn, data);
}
void* TaskScheduler::AllocateFromArena(size_t size) {
	return taskArena.GetActive()->allocate(size);
}

bool TaskScheduler::PushBatchLocal(Task* tasks[], size_t count, uint8_t cpuaffinity) {
	// 1. Manually link them locally: Task A -> Task B -> Task C
	for (size_t i = 0; i < count - 1; ++i) {
		tasks[i]->next.store(tasks[i + 1], std::memory_order_relaxed);
	}
	// The last task's next is already handled by the queue's exchange logic

	// 2. Submit the pointers directly - NO wrappers, NO heap allocation
	inboxes[cpuaffinity - 1]->push_batch(tasks[0], tasks[count - 1], count);
	return true;
}
bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task) return false;

	size_t num_workers = workers.size();
	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			runningTasks.fetch_add(1, std::memory_order_relaxed);
			inboxes[idx]->push(task);
			NotifyAll();
		}
		else
			return false;
	}
	else {
		runningTasks.fetch_add(1, std::memory_order_relaxed);
		uint8_t chosen = PickNextWorker();
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			chosen = PickNextWorker();
		}
		inboxes[chosen]->push(task);
		workers[chosen]->NotifyWorker();

	}
	return true;
}
bool TaskScheduler::PushToCore(size_t core_id, Task* task) {
	if (core_id < 1) return false;
	if (!poolActive) return false;
	if (!task) return false;

	size_t idx = (core_id - 1) % workers.size();
	if (immediateCoresInUse[idx]->load(std::memory_order_acquire)) return false;

	runningTasks.fetch_add(1, std::memory_order_relaxed);
	immediateCoresInUse[idx]->store(true, std::memory_order_release);
	workers[idx]->SetImmediateTask(task);
	workers[idx]->NotifyWorker();
	return true;
}
int TaskScheduler::PickNextWorker() {
	size_t n = workers.size();
	for (size_t i = 0; i < n; ++i) {
		size_t idx = (nextWorker + i) % n;
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			nextWorker = (idx + 1) % n;
			return static_cast<int>(idx);
		}
	}
	int fallback = static_cast<int>(nextWorker);
	nextWorker = (fallback + 1) % n;
	return fallback;
}
