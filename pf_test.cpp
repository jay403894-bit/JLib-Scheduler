// Reproduce the engine pattern: `new Task(fnptr, data)` + Push + Wait, then ParallelFor.
// Hypothesis: heap `new Task` (~140B) gets Freed into the 256B slab; ParallelFor's
// template CreateTask then placement-news a larger LambdaTask into that under-sized
// heap slot -> heap overflow -> read AV.
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <dbghelp.h>
#include "include/TaskScheduler.h"
#include "include/Event.h"
#include <atomic>
#include <vector>
#include <chrono>
#include <thread>
#include <cstdio>
#pragma comment(lib, "dbghelp.lib")

using namespace T_Threads;

static LONG WINAPI OnCrash(EXCEPTION_POINTERS* ep) {
    auto code = ep->ExceptionRecord->ExceptionCode;
    if (code != EXCEPTION_ACCESS_VIOLATION && code != 0xC0000374 /*heap corruption*/)
        return EXCEPTION_CONTINUE_SEARCH;
    fprintf(stderr, "\n*** CRASH code=0x%08lX addr=%p tid=%lu ***\n",
        (unsigned long)code, ep->ExceptionRecord->ExceptionAddress, GetCurrentThreadId());
    if (code == EXCEPTION_ACCESS_VIOLATION)
        fprintf(stderr, "    %s at 0x%p\n", ep->ExceptionRecord->ExceptionInformation[0] ? "WRITE" : "READ",
            (void*)ep->ExceptionRecord->ExceptionInformation[1]);
    HANDLE proc = GetCurrentProcess();
    SymSetOptions(SYMOPT_LOAD_LINES | SYMOPT_DEFERRED_LOADS);
    SymInitialize(proc, nullptr, TRUE);
    void* frames[40];
    USHORT n = CaptureStackBackTrace(0, 40, frames, nullptr);
    char b[sizeof(SYMBOL_INFO) + 256]; SYMBOL_INFO* sym = (SYMBOL_INFO*)b;
    sym->SizeOfStruct = sizeof(SYMBOL_INFO); sym->MaxNameLen = 255;
    for (USHORT i = 0; i < n; ++i) {
        DWORD64 disp = 0; const char* name = "?";
        if (SymFromAddr(proc, (DWORD64)frames[i], &disp, sym)) name = sym->Name;
        IMAGEHLP_LINE64 ln{}; ln.SizeOfStruct = sizeof(ln); DWORD ld = 0;
        if (SymGetLineFromAddr64(proc, (DWORD64)frames[i], &ld, &ln))
            fprintf(stderr, "  [%02u] %s  (%s:%lu)\n", i, name, ln.FileName, ln.LineNumber);
        else
            fprintf(stderr, "  [%02u] %s +0x%llx\n", i, name, (unsigned long long)disp);
    }
    fflush(stderr);
    TerminateProcess(GetCurrentProcess(), 99);
    return EXCEPTION_EXECUTE_HANDLER;
}

struct Params { int a, b; std::atomic<long long>* sum; };
static void Wrapper(void* p) {
    Params* pp = static_cast<Params*>(p);
    long long local = 0;
    for (int i = pp->a; i < pp->b; ++i) local += i;
    pp->sum->fetch_add(local, std::memory_order_relaxed);
}

static void EventWaiter(void* d) {
    auto* woken = static_cast<std::atomic<int>*>(d);
    TaskScheduler::Instance().WaitOnEvent("e");   // suspend until SignalAll
    woken->fetch_add(1, std::memory_order_relaxed);
}

int main(int argc, char** argv) {
    AddVectoredExceptionHandler(1, OnCrash);
    // mode: "A" = Wait-on-new-Task, "B" = ParallelFor, "E" = event suspend/resume, else both
    char mode = (argc > 1) ? argv[1][0] : '*';
    TaskScheduler::Init();
    auto& s = TaskScheduler::Instance();

    if (mode == 'E') {
        // Stress the park/signal race: many fibers WaitOnEvent, main hammers SignalAll.
        // A lost wakeup => some task stuck SUSPENDED => 'woken' plateaus => watchdog trips.
        const int N = 128, ROUNDS = 300;
        for (int round = 0; round < ROUNDS; ++round) {
            std::atomic<int> woken{ 0 };
            for (int i = 0; i < N; ++i) s.Push(s.CreateTask(EventWaiter, &woken));
            auto t0 = std::chrono::steady_clock::now();
            while (woken.load(std::memory_order_acquire) < N) {
                s.GetEvent("e").SignalAll();
                std::this_thread::yield();
                if (std::chrono::steady_clock::now() - t0 > std::chrono::seconds(10)) {
                    fprintf(stderr, "EVENT HANG round %d woken=%d/%d (lost wakeup)\n",
                        round, woken.load(), N);
                    return 3;
                }
            }
            if ((round % 50) == 0) { printf("[e] round %d ok\n", round); fflush(stdout); }
        }
        printf("EVENT_TEST_OK rounds=%d\n", ROUNDS);
        return 0;
    }

    const int RANGE = 100000, CHUNK = 64;
    long long expected = 0; for (long long i = 0; i < RANGE; ++i) expected += i;

    for (int iter = 0; iter < 300; ++iter) {
        std::atomic<long long> sumB{ 0 };
        s.ParallelFor(0, RANGE, CHUNK, [&sumB](int a, int b) {
            long long local = 0; for (int i = a; i < b; ++i) local += i;
            sumB.fetch_add(local, std::memory_order_relaxed);
            });
        if (sumB.load() != expected) {
            fprintf(stderr, "MISMATCH iter %d sumB=%lld exp=%lld\n", iter, sumB.load(), expected);
            return 2;
        }
        if ((iter % 25) == 0) { printf("[t] iter %d ok\n", iter); fflush(stdout); }
    }
    printf("PF_TEST_OK\n");
    return 0;
}
