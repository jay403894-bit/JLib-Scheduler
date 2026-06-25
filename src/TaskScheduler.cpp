#include "../include/T_Thread.h"
#include "../include/TaskScheduler.h"
#include "../include/Event.h"
using namespace T_Threads;

static_assert(sizeof(Task) <= TaskAllocator::SLOT, "Task doesn't fit a slot");
static_assert(alignof(Task) <= 16, "Task over-aligned for a slot");

TaskScheduler* TaskScheduler::instance = nullptr;
GlobalFiberPool* TaskScheduler::globalPool = nullptr;

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
GlobalFiberPool& T_Threads::TaskScheduler::GetGlobalPool()
{
	if (!instance->globalPool)
		throw std::runtime_error("GlobalFiberPool not initialized!");
	return *instance->globalPool;

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

	// Completion is tracked by this STACK-LOCAL counter -- NOT by polling the Task
	// objects. A worker destroys+frees each Task the instant its fiber goes DEAD
	// (T_Thread::Worker, the FiberStatus::DEAD branch), so the old code's
	// `taskPtrs[i]->complete` poll was a use-after-free: it read a slab slot that had
	// already been freed and frequently recycled into a brand-new Task (complete==false
	// again) -> spin forever (hang) or read unmapped memory (crash). This counter lives
	// on our stack frame, which outlives every chunk because we block below until it
	// hits zero. Each chunk decrements it as the LAST thing inside Execute(), before the
	// worker is allowed to free the Task. `remaining` and `func` are captured by
	// reference -- safe precisely because we don't return until remaining == 0.
	std::atomic<int> remaining{ 0 };

	// 1. Create tasks. If the arena is exhausted (CreateTask returns nullptr),
	//    run that chunk inline on the caller rather than dereferencing null.
	for (int i = 0; i < numTasks; ++i) {
		int chunkStart = start + i * chunkSize;
		int chunkEnd = std::min(chunkStart + chunkSize, end);

		Task* t = CreateTask([&remaining, &func, chunkStart, chunkEnd]() {
			func(chunkStart, chunkEnd);
			remaining.fetch_sub(1, std::memory_order_acq_rel);
		});
		if (!t) {
			func(chunkStart, chunkEnd); // graceful degradation: do it here (no counter touch)
			continue;
		}
		// Count it BEFORE it can possibly run (we push only after this loop).
		remaining.fetch_add(1, std::memory_order_relaxed);
		taskPtrs[created++] = t;
	}

	if (created > 0) {
		pendingTasks.fetch_add(created, std::memory_order_relaxed);

		// 2. Distribute across all workers' inboxes (round-robin). Dumping the
		//    whole batch on one worker pins it in the inbox->deque drain loop and
		//    hangs when created > deque capacity (large range / small chunkSize)
		//    or when there's only one worker. (push handles the next-link itself.)
		//    After this loop we never touch taskPtrs[i] again -- a worker may have
		//    already freed it.
		size_t n = inboxes.size();
		for (int i = 0; i < created; ++i) {
			inboxes[i % n]->push(taskPtrs[i]);
		}
		NotifyAll();

		// 3. Wait on the counter, HELPING while we wait so the calling thread makes
		//    progress too. Pure spinning deadlocks when ParallelFor runs on a worker
		//    fiber (nested) or when there's only one worker. GetTask() steals a task
		//    out of a deque -- removed exactly once -- so running it here can't
		//    double-execute; we decrement pendingTasks because the owning worker will
		//    never see it. (A helped chunk still decrements `remaining` from inside
		//    its own Execute(), so the wait terminates regardless of who runs it.)
		while (remaining.load(std::memory_order_acquire) > 0) {
			Task* helped = GetTask();
			if (helped) {
				helped->Execute();
				pendingTasks.fetch_sub(1, std::memory_order_acq_rel);
			}
			else {
				std::this_thread::yield();
			}
		}
	}

	delete[] taskPtrs;

	// 4. Batch is done and no longer referenced by us. Recycle the arena it
	//    came from — but only if the whole system is quiescent (see RecycleArena).
	//RecycleArena();
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
	thread_counter.store(0, std::memory_order_release);
	if (poolSize == 0)
		poolSize = GetSafeTC();
	if (poolSize > GetSafeTC())
		poolSize = GetSafeTC();

	unsigned int num_workers = static_cast<unsigned int>(poolSize);
	// +1 for the main/submitting thread: it takes thread_id == num_workers at the end
	// of StartPool and uses epochs too (e.g. DAG AddDependency -> EnterEpoch). Sizing to
	// just num_workers leaves that slot out of bounds -> AV in Enter/LeaveEpoch.
	EpochManager::Instance().Init(num_workers + 1);
	stopFlag.store(false, std::memory_order_release);
	nextWorker = 0;

	unsigned int coreCount = std::thread::hardware_concurrency()-1;
	if (coreCount == 0) coreCount = 4; // Fallback

	size_t standardFiberCount = coreCount * 64;
	size_t heavyFiberCount = coreCount * 8;

	// 3. Ensure a minimum to avoid "thrashing"
	
	// GlobalFiberPool now owns all fibers and stack allocation
	globalPool = GlobalFiberPool::Create(standardFiberCount, heavyFiberCount);
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
		inboxes.push_back(std::make_unique<TaskMPSCQueue>());
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
	thread_id = thread_counter.fetch_add(1);
	poolActive.store(true, std::memory_order_release);
}

void TaskScheduler::WaitOnEvent(const std::string& eventName) {
	auto* thread = T_Thread::GetCurrent();
	Task* myTask = thread->currentRunningTask;
	Fiber* myFiber = myTask->assignedFiber;

	auto& event = GetEvent(eventName);

	// Order matters. Become parkable (WANTS_SUSPEND) BEFORE registering, so any signal
	// that races in sees a resumable state (Resume() flips WANTS_SUSPEND->SUSPEND_SIGNALED
	// and the worker wakes us after the switch). AddWaiter only inserts -- it no longer
	// touches status. The event mutex serializes AddWaiter against Signal/SignalAll, so a
	// signal that lands after we register is guaranteed to find and wake us.
	myFiber->status.store(FiberStatus::WANTS_SUSPEND, std::memory_order_release);
	event.AddWaiter(myTask);

	// Return via the fiber's homeCtx (the worker stamps it before each switch-in),
	// not thread_local schedulerCtx -- the waiter resumes on whatever worker the
	// event signal lands on, which may differ from this one.
	ContextSwitch(&myFiber->ctx, myFiber->homeCtx);
}
bool TaskScheduler::Push(Task* task) {
	return PushLocal(task);
}
struct WaitGroup { std::atomic<int> n{ 0 }; };



void TaskScheduler::RunCounted(WaitGroup& wg, Task* t) {
	wg.n.fetch_add(1, std::memory_order_relaxed);
	t->waitGroup = &wg;
	Push(t);
}

void TaskScheduler::WaitFor(WaitGroup& wg) {
	while (wg.n.load(std::memory_order_acquire) > 0) {
		Task* h = GetTask();
		if (h) { h->Execute(); pendingTasks.fetch_sub(1, std::memory_order_acq_rel); }
		else std::this_thread::yield();
	}
}
void T_Threads::TaskScheduler::PushBatch(Task* tasks[], size_t count, uint8_t cpuaffinity)
{
	// 1. Manually link them locally: Task A -> Task B -> Task C
	for (size_t i = 0; i < count - 1; ++i) {
		tasks[i]->next.store(tasks[i + 1], std::memory_order_relaxed);
	}
	// The last task's next is already handled by the queue's exchange logic
	pendingTasks.fetch_add(count, std::memory_order_relaxed);
	// 2. Submit the pointers directly - NO wrappers, NO heap allocation
	if (cpuaffinity == 0)
	{
		int chosen = PickNextWorker();
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			_mm_pause();
			chosen = PickNextWorker();
		}
		inboxes[chosen]->push_batch(tasks[0], tasks[count - 1]);

	}
	else
	{
		int chosen = cpuaffinity - 1;
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			_mm_pause();
			chosen = PickNextWorker();
		}
		inboxes[chosen]->push_batch(tasks[0], tasks[count - 1]);
	}
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

TaskAllocator* TaskScheduler::GetAllocator() {
	return &taskAllocator;
}
void TaskScheduler::WaitAll() {
	while (pendingTasks.load(std::memory_order_acquire) > 0)
		std::this_thread::yield();
}

Task* TaskScheduler::CreateTask(void(*fn)(void*), void* data, FiberSize size) {
	void* mem = taskAllocator.Alloc();
	if (!mem) return nullptr;
	Task* t = new (mem) Task(fn, data, size);
	t->ownedBySlab = true;   // reclaimed via the slab on completion
	return t;
}

bool TaskScheduler::PushLocal(Task* task, uint8_t cpuaffinity) {
	if (!task) return false;

	size_t num_workers = workers.size();
	if (cpuaffinity > 0 && (size_t)(cpuaffinity - 1) < num_workers) {
		size_t idx = (size_t)(cpuaffinity - 1);
		if (!immediateCoresInUse[idx]->load(std::memory_order_acquire)) {
			pendingTasks.fetch_add(1, std::memory_order_relaxed);
			inboxes[idx]->push(task);
			NotifyAll();
		}
		else
			return false;
	}
	else {
		pendingTasks.fetch_add(1, std::memory_order_relaxed);
		uint8_t chosen = PickNextWorker();
		while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
			chosen = PickNextWorker();
		}
		inboxes[chosen]->push(task);
		workers[chosen]->NotifyWorker();

	}
	return true;
}
bool TaskScheduler::Requeue(Task* task) {
	if (!task) return false;
	// Re-queue a paused task (resumed after Suspend). Unlike PushLocal this does NOT
	// bump pendingTasks -- the task was already counted at its original submission and
	// is only resuming, not newly created. (The yield path does the same, via the
	// worker's push_bottom.) Otherwise every suspend->resume cycle leaks +1.
	uint8_t chosen = PickNextWorker();
	while (immediateCoresInUse[chosen]->load(std::memory_order_acquire)) {
		chosen = PickNextWorker();
	}
	inboxes[chosen]->push(task);
	workers[chosen]->NotifyWorker();
	return true;
}
bool TaskScheduler::PushToCore(size_t core_id, Task* task) {
	if (core_id < 1) return false;
	if (!poolActive) return false;
	if (!task) return false;

	size_t idx = (core_id - 1) % workers.size();
	if (immediateCoresInUse[idx]->load(std::memory_order_acquire)) return false;

	pendingTasks.fetch_add(1, std::memory_order_relaxed);
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
