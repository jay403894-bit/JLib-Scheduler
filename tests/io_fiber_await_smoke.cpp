// THE SMALLEST THING THE PORT DEPENDS ON: a FIBER suspends inside an I/O wait and is woken by the
// completion. io_socket_test hung on its first arm after the port, and that file is 1000 lines with
// two fibers, two sockets and a real connection in the first section alone -- far too much surface
// to reason about. This is the structure on its own, with one file read and one fiber.
//
// If this passes, the shim is sound and the hang is in the test's own shape. If it hangs, the shim
// is the bug and the 1000 lines are irrelevant.

#include "TaskScheduler.h"
#include "IoReactor.h"
#include "Thread.h"          // Thread::Current()->qIndex -- arm 4 asks WHICH worker resumed it
#include "io_fiber_await.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
// socket_compat.h, NOT a bare <windows.h>: it gets winsock2.h in FIRST. windows.h pulls the ancient
// winsock.h, and a winsock2.h after that is the classic redefinition wall -- the same ordering rule
// src/win32/IoReactor.cpp writes down at its own includes.
#include "socket_compat.h"

namespace Fib = JLib::testing;

static int g_fail = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-58s %s\n", what, c ? "ok" : "FAILED");
    if (!c) ++g_fail;
}

static std::atomic<int> g_status{ -1 }, g_bytes{ -1 }, g_ran{ 0 };

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);

    // `pin` runs everything under FiberMode::Pin and enables arm 4, which is the only arm that
    // asks whether a RESERVED worker actually drains its own resume inbox. Must precede Init().
    for (int a = 1; a < argc; ++a)
        if (std::strcmp(argv[a], "pin") == 0) JLib::TaskScheduler::SetFiberMode(JLib::FiberMode::Pin);

    std::printf("=== a fiber suspends on I/O and is woken by the completion ===\n");

    // TIMERS ON, matching io_socket_test. It is the one setup difference left between this file
    // and the suite that hangs, and it is not cosmetic: the timer wheel is another always-on
    // thread and another consumer of the same bands.
    JLib::TaskScheduler::EnableTimers(true);
    JLib::TaskScheduler::EnableIoReactor(true);
    JLib::TaskScheduler::Init(0);
    auto& sched = JLib::TaskScheduler::Instance();
    auto& io    = JLib::IoReactor::Instance();

    char path[MAX_PATH], dir[MAX_PATH];
    ::GetTempPathA(MAX_PATH, dir);
    std::snprintf(path, sizeof path, "%sjlib_fiber_await.bin", dir);
    { FILE* f = std::fopen(path, "wb"); for (int i = 0; i < 512; ++i) std::fputc(i & 0xFF, f); std::fclose(f); }

    HANDLE h = ::CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, nullptr,
                             OPEN_EXISTING, FILE_FLAG_OVERLAPPED, nullptr);
    Check(h != INVALID_HANDLE_VALUE && io.Register(h), "opened and registered");
    if (h == INVALID_HANDLE_VALUE) return 1;

    static char buf[256];
    JLib::WaitGroup wg;

    Fib::SpawnFiber(sched, wg, [&sched, h] {
        g_ran.store(1, std::memory_order_release);
        JLib::IoRequest req{}; JLib::IoResult out{};
        // The raw reactor call, through Await -- the same path every ported arm takes.
        const JLib::IoResult r = Fib::Await(sched, [&](JLib::Task* k) {
            return JLib::IoReactor::Instance().SubmitRead(h, buf, 256, 0, &req, &out, k,
                                                          JLib::CancelToken{});
        }, out);
        g_status.store(static_cast<int>(r.status), std::memory_order_release);
        g_bytes.store(static_cast<int>(r.bytes), std::memory_order_release);
        g_ran.store(2, std::memory_order_release);
    });

    // BOUNDED, never a bare WaitFor. If the shim is broken this must FAIL rather than hang -- a
    // hang here would be the same non-answer io_socket_test just gave.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (g_ran.load(std::memory_order_acquire) != 2
           && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

    Check(g_ran.load() >= 1, "the fiber started at all");
    Check(g_ran.load() == 2, "and RETURNED from the I/O wait (a hang fails here)");
    std::printf("  status=%d bytes=%d\n", g_status.load(), g_bytes.load());
    Check(g_status.load() == static_cast<int>(JLib::IoStatus::Completed), "the read completed");
    Check(g_bytes.load() == 256, "with the bytes asked for");

    ::CloseHandle(h);
    ::DeleteFileA(path);

    // ---- ARM 2: TWO FIBERS WHOSE I/O COMPLETES EACH OTHER ------------------------------------
    //
    // io_socket_test still hangs on its first section after arm 1 above passes, and that section is
    // an accept and a connect parked at the same time -- neither completes until the other runs.
    // That mutual dependency is the only structural difference, so it is what this reproduces.
    std::printf("\ntwo fibers, each parked on I/O the other completes\n");
    {
        ::WSADATA wsa{}; ::WSAStartup(MAKEWORD(2,2), &wsa);
        io.InitSockets();

        SOCKET lis = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK); a.sin_port = 0;
        ::bind(lis, (sockaddr*)&a, sizeof a); ::listen(lis, SOMAXCONN);
        int alen = sizeof a; ::getsockname(lis, (sockaddr*)&a, &alen);
        io.RegisterSocket(lis);

        SOCKET acc = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        SOCKET cli = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in any{}; any.sin_family = AF_INET; any.sin_addr.s_addr = ::htonl(INADDR_ANY); any.sin_port = 0;
        ::bind(cli, (sockaddr*)&any, sizeof any);
        io.RegisterSocket(acc); io.RegisterSocket(cli);

        static JLib::IoAcceptBuffer ab;
        static sockaddr_in tgt; tgt = a;
        static std::atomic<int> accDone{ -1 }, conDone{ -1 };

        JLib::WaitGroup wg2;
        Fib::SpawnFiber(sched, wg2, [&sched, lis, acc] {
            JLib::IoRequest req{}; JLib::IoResult out{};
            const JLib::IoResult r = Fib::Accept(sched, (JLib::IoSocket)lis, (JLib::IoSocket)acc, &ab, req, out);
            accDone.store((int)r.status, std::memory_order_release);
        });
        Fib::SpawnFiber(sched, wg2, [&sched, cli] {
            JLib::IoRequest req{}; JLib::IoResult out{};
            const JLib::IoResult r = Fib::Connect(sched, (JLib::IoSocket)cli, &tgt, sizeof tgt, req, out);
            conDone.store((int)r.status, std::memory_order_release);
        });

        // BOUNDED. A bare WaitFor here would reproduce the hang instead of reporting it.
        const auto d2 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while ((accDone.load() < 0 || conDone.load() < 0)
               && std::chrono::steady_clock::now() < d2)
            std::this_thread::sleep_for(std::chrono::milliseconds(5));

        std::printf("  accept=%d connect=%d\n", accDone.load(), conDone.load());
        Check(conDone.load() == (int)JLib::IoStatus::Completed, "the connect completed");
        Check(accDone.load() == (int)JLib::IoStatus::Completed, "the accept completed");

        // ---- THE LAST DIFFERENCE FROM io_socket_test: main JOINS the group ----------------
        //
        // Everything above waits on atomics with a deadline. The real test calls
        // sched.WaitFor(wg) from the MAIN thread, and that is the only structural difference
        // left -- a BARE THREAD blocking on the scheduler, which helps by running work while it
        // waits. Both fibers have already finished by now, so this must return immediately; if
        // it does not, joining is the hang and the I/O was never the problem.
        std::printf("  main is about to WaitFor the group (both fibers already finished)...\n");
        sched.WaitFor(wg2);
        std::printf("  ...WaitFor returned\n");
        Check(true, "main's WaitFor(wg) returned after the fibers completed");

        ::closesocket(cli); ::closesocket(acc); ::closesocket(lis);
    }

    // ---- ARM 3: MAIN JOINS WHILE THE FIBERS ARE STILL PARKED --------------------------------
    //
    // THIS IS THE REAL ORDERING, and the last one left untested. Arm 2 let the I/O finish before
    // main joined; io_socket_test spawns and joins IMMEDIATELY, so main is blocked in WaitFor
    // while both fibers are parked on completions that have not arrived.
    //
    // That matters because a bare thread blocking on the scheduler HELPS -- it runs work while it
    // waits -- so main is not merely asleep here, it is participating. If this hangs, the bug is
    // the join and not the I/O, and every arm above passing is exactly what made it look like the
    // I/O.
    std::printf("\nmain joins while both fibers are still parked\n");
    {
        SOCKET lis = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK); a.sin_port = 0;
        ::bind(lis, (sockaddr*)&a, sizeof a); ::listen(lis, SOMAXCONN);
        int alen = sizeof a; ::getsockname(lis, (sockaddr*)&a, &alen);
        io.RegisterSocket(lis);

        SOCKET acc = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        SOCKET cli = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        sockaddr_in any{}; any.sin_family = AF_INET; any.sin_addr.s_addr = ::htonl(INADDR_ANY); any.sin_port = 0;
        ::bind(cli, (sockaddr*)&any, sizeof any);
        io.RegisterSocket(acc); io.RegisterSocket(cli);

        static JLib::IoAcceptBuffer ab3;
        static sockaddr_in tgt3; tgt3 = a;
        static std::atomic<int> acc3{ -1 }, con3{ -1 }, joined{ 0 };

        JLib::WaitGroup wg3;
        Fib::SpawnFiber(sched, wg3, [&sched, lis, acc] {
            JLib::IoRequest req{}; JLib::IoResult out{};
            const JLib::IoResult r = Fib::Accept(sched, (JLib::IoSocket)lis, (JLib::IoSocket)acc, &ab3, req, out);
            acc3.store((int)r.status, std::memory_order_release);
        });
        Fib::SpawnFiber(sched, wg3, [&sched, cli] {
            JLib::IoRequest req{}; JLib::IoResult out{};
            const JLib::IoResult r = Fib::Connect(sched, (JLib::IoSocket)cli, &tgt3, sizeof tgt3, req, out);
            con3.store((int)r.status, std::memory_order_release);
        });

        // A WATCHDOG THREAD, because WaitFor has no deadline and this arm exists to survive its
        // failure. Without it a hang here is a hang of the whole file -- the same non-answer that
        // sent me looking at the I/O in the first place.
        std::thread watchdog([&] {
            const auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(15);
            while (joined.load(std::memory_order_acquire) == 0
                   && std::chrono::steady_clock::now() < dl)
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            if (joined.load(std::memory_order_acquire) == 0) {
                std::printf("  WATCHDOG: main is STUCK in WaitFor -- accept=%d connect=%d\n",
                            acc3.load(), con3.load());
                std::fflush(stdout);
                std::_Exit(3);   // escape rather than deadlock the run
            }
        });

        std::printf("  main entering WaitFor with both fibers parked...\n");
        sched.WaitFor(wg3);
        joined.store(1, std::memory_order_release);
        std::printf("  ...returned\n");
        watchdog.join();

        Check(con3.load() == (int)JLib::IoStatus::Completed, "the connect completed");
        Check(acc3.load() == (int)JLib::IoStatus::Completed, "the accept completed");
        ::closesocket(cli); ::closesocket(acc); ::closesocket(lis);
    }

    // ---- ARM 4: A RESERVED WORKER RESUMES A PINNED FIBER FROM ITS OWN INBOX ------------------
    //
    // THE SUITE PASSING ONLY PROVES NOTHING HUNG. This asserts the mechanism that makes it safe for
    // a reserved worker to steal in pinned mode: a pinned fiber resumes ONLY on its home worker, and
    // if that home is reserved the resumption goes to resumedInboxes[home] -- which every worker
    // drains regardless of band. A steal gate was added on the opposite theory and removed once the
    // drain was actually read; this is what keeps that decision from being re-litigated from memory.
    //
    // HOW A FIBER COMES TO BE HOMED ON K AT ALL: ordinary work is never PLACED there, so the only
    // route is a reserved worker STEALING it. So this arm is precisely "K steals a fiber, the fiber
    // suspends on I/O, and K resumes it" -- the sequence the gate was meant to prevent.
    if (JLib::TaskScheduler::GetFiberMode() == JLib::FiberMode::Pin) {
        std::printf("\na RESERVED worker resumes a pinned fiber from its own inbox\n");
        const size_t K = JLib::TaskScheduler::GetHotWorkers();
        if (K == 0) {
            std::printf("  K=0 -- no reserved band, so this arm is vacuous\n");
        } else {
            constexpr int kJobs = 96;
            static std::atomic<int> ranOnReserved{ 0 }, ranTotal{ 0 }, resumedOnReserved{ 0 };
            static std::atomic<int> parked4{ 0 };
            JLib::WaitGroup wg4;

            // ---- THE FIBERS SUSPEND ON AN EVENT, NOT ON I/O, AND THAT IS THE POINT ----------
            //
            // The first version of this arm did I/O, and its vacuity guard immediately caught the
            // problem: 0 of 96 jobs ever ran on a reserved worker. RESERVED STEALING IS GATED ON
            // THE LANE BEING QUIET -- a worker in the band will not take ordinary work while
            // completions are arriving, which is exactly the protection that makes the band worth
            // having. So an arm that generates I/O can never observe K stealing, by design.
            //
            // Suspending on an Event instead leaves the lane idle while ordinary suspendable work
            // is available, which is the configuration where K steals. The quiet window is dropped
            // to 1ms and waited out first, so the test does not depend on the default.
            JLib::TaskScheduler::SetIoQuietWindowUs(1000);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            JLib::Event& gate4 = sched.GetEvent("pin_arm4_gate");

            for (int i = 0; i < kJobs; ++i) {
                Fib::SpawnFiber(sched, wg4, [&sched, &gate4, K] {
                    JLib::Thread* pre = JLib::Thread::Current();
                    if (pre && (size_t)pre->qIndex < K)
                        ranOnReserved.fetch_add(1, std::memory_order_relaxed);

                    parked4.fetch_add(1, std::memory_order_release);
                    sched.WaitOnEvent(gate4);

                    // AFTER the suspension. In pinned mode this MUST be the same worker it started
                    // on -- that is what pinning means -- so a reserved start implies a reserved
                    // resume, and observing one is observing K draining its own resume inbox.
                    JLib::Thread* post = JLib::Thread::Current();
                    if (post && (size_t)post->qIndex < K)
                        resumedOnReserved.fetch_add(1, std::memory_order_relaxed);
                    ranTotal.fetch_add(1, std::memory_order_relaxed);
                });
            }

            // Let them all park before releasing, so the resumes are a storm the band participates
            // in rather than a trickle it never sees.
            const auto d4 = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (parked4.load(std::memory_order_acquire) < kJobs
                   && std::chrono::steady_clock::now() < d4)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            gate4.SignalAll();
            sched.WaitFor(wg4);

            std::printf("  %d jobs: %d started on a reserved worker, %d RESUMED on one\n",
                        ranTotal.load(), ranOnReserved.load(), resumedOnReserved.load());
            Check(ranTotal.load() == kJobs, "every pinned job finished (a stranded resume hangs here)");

            // THE VACUITY GUARD. If K never stole one, nothing here exercised the path and a green
            // arm would mean only that reserved stealing did not happen this run.
            if (ranOnReserved.load() == 0)
                std::printf("  VACUOUS: no job ever ran on a reserved worker, so K never stole one\n"
                            "           and this arm proves nothing. Reserved stealing is gated on\n"
                            "           the lane being QUIET -- re-run, or raise the I/O quiet window.\n");
            else
                Check(resumedOnReserved.load() > 0,
                      "a RESERVED worker resumed a pinned fiber -- it drains its own resume inbox");

        }
    }

    std::printf("\n%s\n", g_fail ? "FAILED" : "PASSED");
    return g_fail ? 1 : 0;
}
