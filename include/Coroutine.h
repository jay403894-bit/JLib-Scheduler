// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// C++20 coroutine support for JLib::Scheduler. OPTIONAL AND OPT-IN: nothing in the library includes
// this header, and the scheduler core stays C++17 whether or not you do.
//
// WHY THE CORE DOES NOT CHANGE. A coroutine is resumed by calling a function with a pointer, so the
// worker resumes one through the SAME `fn(data)` call it uses for every other task -- `fn` is the
// trampoline below, `data` is `coroutine_handle::address()`. `<coroutine>` and C++20 are therefore
// confined to this header and whichever translation units include it. `Task` carries a
// `TaskType::Coroutine` enumerator and nothing else; it did not grow by a byte.
//
// THE ONE THING THE CORE DOES KNOW is a lifetime rule, and it is the whole design:
//
//     THE WORKER NEVER COMPLETES A COROUTINE TASK. THIS SIDE OWNS IT, START TO FINISH.
//
// The obvious alternative -- a "finished" flag the worker checks after resuming -- is racy, was
// written first, and was discarded. A coroutine that suspends gets re-pushed by whatever armed its
// resume, so a second worker can pick it up, run it to completion and free it while the first worker
// is still deciding what it just saw. Both then observe "finished" and both free. No flag read
// closes that window, because the window opens *inside* resume(), the moment the task becomes
// re-pushable again. Sole ownership removes the race rather than narrowing it.
//
// The consequence that costs vigilance: after anything makes this coroutine re-pushable -- Push(),
// arming an I/O completion, signalling an awaiter -- NOTHING may touch the handle or the Task again
// on this thread. Both may already be gone. Every such site below says so.
//
// WHAT THIS IS NOT. `Event`/`DirectEvent` deliberately have no coroutine support, and that is not an
// omission to fill in later. They are ARBITRARY-POINT suspension: a fiber parks from any stack depth
// because ContextSwitch takes the whole stack with it. A coroutine has no such capability -- it
// suspends only where a `co_await` is written -- so the coroutine analogue of an event is an
// awaitable, a different construct, not a polymorphic Event. Locks and semaphores are different
// again: "wait until available" is expressible in both worlds, so those can become supersets.

#if !defined(__cpp_impl_coroutine) || __cpp_impl_coroutine < 201902L
    // Checked via the FEATURE-TEST macro rather than the language version, because /std:c++20 does
    // not by itself promise the coroutine implementation is present. The language check below only
    // runs when the feature macro is missing, to produce the more useful message for the common
    // cause (wrong /std:). _MSVC_LANG is consulted first since MSVC reports __cplusplus as 199711L
    // unless /Zc:__cplusplus is passed.
    #if !defined(_MSVC_LANG) || _MSVC_LANG < 202002L
        #if __cplusplus < 202002L
            #error "JLib/Coroutine.h requires C++20 (MSVC: /std:c++20, GCC/Clang: -std=c++20)"
        #endif
    #endif
    #error "JLib/Coroutine.h requires C++20 coroutines, but __cpp_impl_coroutine is not defined"
#endif

#include "TaskScheduler.h"
#include "Hazard.h"      // the suspension check in ArmResume
#include "Future.h"      // Future<T>/Promise<T>: the C++17 half; the awaiter for it is at the bottom

#include <coroutine>
#include <cstdint>
#include <exception>
#include <optional>
#include <type_traits>
#include <utility>

namespace JLib {

    // Declared, NOT included. Completing a DAG node from a coroutine needs only these two names, and
    // pulling in TaskDAG.h would make every coroutine translation unit compile the DAG -- coupling
    // two features that are independently optional. The direction matters more than the compile
    // time: this header may depend on the C++17 core, never the reverse, or using a TaskDAG would
    // start requiring C++20. See SignalExternalNode in TaskDAG.h.
    struct TaskNode;
    void SignalExternalNode(TaskNode* node);

    // ---- frame-size instrumentation (diagnostic builds only) -----------------------------------
    //
    // WHY THIS EXISTS. A spawned coroutine costs TWO allocations: the frame, which the compiler
    // emits an `operator new` for, and a Task from the slab. Pooling the frame is the obvious next
    // optimization -- and its two inputs, how BIG frames are and how MANY are live at once, are
    // chosen by the compiler from whichever locals live across suspend points. They cannot be
    // reasoned out from the source. Picking a pool slot size without them would be inventing a
    // constant, which is how kFastSpinTries got its value and how that turned into a 24x regression.
    //
    // So: measure first. This records a histogram of real frame sizes and the live/peak counts, and
    // is OFF by default like every other diagnostic here. It does NOT pool anything -- it hands
    // every allocation straight to global new -- because the point is to find out whether pooling is
    // worth doing and what to size it for.
#if defined(JLIBSCHED_CORO_STATS)
    namespace detail {
        struct CoroFrameStats {
            // Bucketed at the slot sizes a pool would plausibly use, not at powers of two for their
            // own sake: the question this answers is "what slot size covers most frames".
            static constexpr size_t kBuckets = 7;   // <=64, <=128, <=256, <=512, <=1024, <=2048, more
            std::atomic<uint64_t> bucket[kBuckets];
            std::atomic<uint64_t> allocations{ 0 };
            std::atomic<uint64_t> maxSize{ 0 };
            std::atomic<int64_t>  live{ 0 };
            std::atomic<int64_t>  peakLive{ 0 };

            void Record(size_t n) {
                allocations.fetch_add(1, std::memory_order_relaxed);
                size_t b = 0;
                for (size_t lim = 64; b < kBuckets - 1 && n > lim; ++b, lim *= 2) {}
                bucket[b].fetch_add(1, std::memory_order_relaxed);

                uint64_t prevMax = maxSize.load(std::memory_order_relaxed);
                while (n > prevMax && !maxSize.compare_exchange_weak(prevMax, n)) {}

                const int64_t now = live.fetch_add(1, std::memory_order_relaxed) + 1;
                int64_t peak = peakLive.load(std::memory_order_relaxed);
                while (now > peak && !peakLive.compare_exchange_weak(peak, now)) {}
            }
            void Release() { live.fetch_sub(1, std::memory_order_relaxed); }
        };

        inline CoroFrameStats& FrameStats() { static CoroFrameStats s{}; return s; }
    }

#endif // JLIBSCHED_CORO_STATS

    // ---- frame allocation -----------------------------------------------------------------------
    //
    // COROUTINE FRAMES COME FROM THE SCHEDULER'S TASK SLAB, not global new. On by default.
    //
    // ONE SOURCE OF MEMORY TRUTH is the main reason. The slab is one contiguous, prefaulted region
    // with fixed 256-byte slots: no size-class mismatch, no splitting, no coalescing, so external
    // fragmentation is impossible rather than merely reduced, and a process running for hours has the
    // same layout it had at startup. Routing frames through it means one arena to size, one place to
    // observe, one failure mode -- and it closes the coroutine-shaped hole in the zero-allocation
    // steady state this library advertises.
    //
    // THE MEASUREMENTS (bench/coroutine_bench.cpp, and note that the first two are what actually
    // decided it -- fragmentation is a long-run property no benchmark here can see):
    //   Lazy await, inline    31.1 -> 16.3 ns   1.9x faster; the composition path, used most often
    //   frame alloc+free      28.1 -> ~17 ns
    //   coroutine spawn       1291 -> ~1263 ns
    //   16 concurrent producer threads               ~18% SLOWER -- the one real regression
    //
    // The regression is specific in shape: with frames pooled, every coroutine takes TWO slots from
    // one allocator instead of one, and a workload that migrates one way (producers allocate, workers
    // free) pushes all of that through the shared refill/flush list. Below 8 threads the per-thread
    // cache absorbs it and the slab's cheap fast path wins; at 16 the shared list becomes the
    // bottleneck and malloc's independent arenas scale better. Call SetCoroFramePooling(false) if
    // your workload is that shape.
    //
    // CAPACITY, AND IT IS THE THING TO ACTUALLY WATCH: each spawned coroutine now consumes TWO slab
    // slots -- its Task and its frame. A slab sized for N tasks holds N/2 concurrent coroutines. See
    // TaskScheduler::SetSlabSizes. Oversized frames (>256 bytes) fall through to global new rather
    // than failing, so an unusually fat coroutine is slower, never broken.
    //
    // WHY IT IS RUNTIME-SWITCHABLE RATHER THAN A BUILD FLAG: SlotInSlab tells a pooled pointer from a
    // heap one, so `delete` stays correct no matter which mode was active at allocation. That is what
    // let both arms be interleaved inside one process to measure this -- comparing across two
    // binaries is the method that produced a 52% p90 noise floor in the fast-spin work and nearly
    // shipped a regression.
    //
    // LIFETIME CONTRACT: a coroutine must not outlive the scheduler. Its Task would dangle too, so
    // this adds no new constraint -- but a frame in the slab is unmapped with the slab, so a frame
    // destroyed after shutdown is the same bug, not a new one.
    namespace detail {
        // A DEDICATED pool, deliberately not a second TaskAllocator.
        //
        // TaskAllocator CANNOT BE INSTANTIATED TWICE. Its per-thread free-list cache is a
        // function-local `static thread_local` inside a STATIC member function, so it is one cache
        // per thread for the whole CLASS, shared by every instance. A second slab feeding the same
        // free list hands frame slots out as Tasks; the first attempt at this pooled frames from a
        // second TaskAllocator and died with 0xC0000374 immediately. Nothing in that class says so.
        //
        // Same shape otherwise: bump-allocate fresh slots, per-thread free list for reuse. A frame
        // freed on a different thread than it was allocated on lands on the FREEING thread's list,
        // which is the behaviour being tested -- that migration is the norm here, since Spawn runs
        // on the caller and the frame dies on whichever worker finished the coroutine.
        class CoroFramePool {
        public:
            static constexpr size_t kSlot = 256;      // covers every frame size measured (max 224)
            static constexpr size_t kSlots = 1u << 16; // 16 MB, committed on first use

            void* Alloc() {
                void*& head = tls();
                if (head) { void* p = head; head = *reinterpret_cast<void**>(p); return p; }
                const size_t i = bump_.fetch_add(1, std::memory_order_relaxed);
                if (i >= kSlots) return nullptr;      // exhausted -> caller falls back to global new
                return Base() + i * kSlot;
            }
            void Free(void* p) noexcept {
                void*& head = tls();
                *reinterpret_cast<void**>(p) = head;
                head = p;
            }
            bool Contains(const void* p) const noexcept {
                const std::byte* q = reinterpret_cast<const std::byte*>(p);
                const std::byte* b = Base();
                return q >= b && q < b + kSlots * kSlot;
            }
        private:
            static std::byte* Base() {
                static std::byte* m = new std::byte[kSlots * kSlot];
                return m;
            }
            static void*& tls() { static thread_local void* head = nullptr; return head; }
            std::atomic<size_t> bump_{ 0 };
        };

        inline CoroFramePool& FramePool() { static CoroFramePool p; return p; }
        inline std::atomic<bool>& FramePoolEnabled() { static std::atomic<bool> b{ true }; return b; }
        // Separate toggle for the 64-byte class so the two CLASSES can be interleaved as arms
        // in ONE process, which is the only comparison this project trusts -- a cross-binary
        // before/after is drift, not a measurement. Off means a small frame takes a 256-byte
        // slot exactly as it did before the class existed.
        //
        // Safe to flip mid-run for the same reason the pool toggle is: FrameFree routes by
        // ADDRESS, not by the current mode, so frames allocated under either setting free
        // correctly afterwards.
        inline std::atomic<bool>& SmallFrameClassEnabled() { static std::atomic<bool> b{ true }; return b; }
    }
    // Turn pooling on or off at runtime. Frames already allocated stay valid either way.
    inline void SetCoroFramePooling(bool on) {
        detail::FramePoolEnabled().store(on, std::memory_order_relaxed);
    }
    // Runtime switch for the 64-byte frame class; see SmallFrameClassEnabled.
    inline void SetCoroSmallFrameClass(bool on) {
        detail::SmallFrameClassEnabled().store(on, std::memory_order_relaxed);
    }
    inline bool CoroSmallFrameClass() {
        return detail::SmallFrameClassEnabled().load(std::memory_order_relaxed);
    }

    inline bool CoroFramePooling() {
        return detail::FramePoolEnabled().load(std::memory_order_relaxed);
    }

    namespace detail {
        // Frames come from THE SCHEDULER'S OWN TaskAllocator, not a second arena.
        //
        // That is not a shortcut, it is the whole point. TaskAllocator's per-thread free-list cache
        // is a `static thread_local` in a STATIC member function -- one cache per thread for the
        // entire class. That property is what makes a second instance corrupt the heap (see the
        // constructor guard) and it is equally what makes the central slab worth using: a single
        // per-thread cache already serves every allocation routed through it, and refill/flush
        // rebalance through a shared backing list. A separate pool cannot share any of that, which
        // is why the first attempt here -- a private bump allocator with a per-thread free list and
        // no rebalancing -- collapsed under this workload's one-way migration (producers allocate,
        // workers free) and exhausted its slots.
        //
        // Slot size is 256 and every frame measured so far fits (largest 224); anything larger falls
        // through to global new. Frames and Tasks now compete for the same slab, which is a real
        // coupling: a coroutine-heavy phase consumes slots a task push would otherwise get. That is
        // the trade being measured, not an oversight.
        inline void* FrameAlloc(std::size_t n) {
#if defined(JLIBSCHED_CORO_STATS)
            FrameStats().Record(n);
#endif
            if (FramePoolEnabled().load(std::memory_order_relaxed)
                && TaskScheduler::IsInitialized()) {
                auto* a = TaskScheduler::Instance().GetAllocator();
                // One call, and the allocator picks the smallest class that fits. The frame
                // sizes that justify the classes are recorded on TaskAllocator itself.
                //
                // SmallFrameClassEnabled() forces the old behaviour -- everything into a
                // 256-byte slot -- so the two can be interleaved as arms in ONE process.
                // A cross-binary before/after is drift, not a measurement; that is not
                // theoretical here, it produced a 46->28.8 ns "win" that was noise.
                const std::size_t want =
                    SmallFrameClassEnabled().load(std::memory_order_relaxed)
                        ? n
                        : (n <= TaskAllocator::SLOT ? TaskAllocator::SLOT : n);
                if (void* p = a->AllocSized(want)) return p;
            }
            return ::operator new(n);
        }
        inline void FrameFree(void* p) noexcept {
#if defined(JLIBSCHED_CORO_STATS)
            FrameStats().Release();
#endif
            // Discriminates by ADDRESS, not by the current mode -- which is what makes the runtime
            // switch safe, and what lets both arms be interleaved in one process.
            if (TaskScheduler::IsInitialized()) {
                auto* a = TaskScheduler::Instance().GetAllocator();
                // Address routes it to the right class; false means it was never ours.
                if (a->FreeSized(p)) return;
            }
            ::operator delete(p);
        }
    }

    // Expanded inside each promise_type. A macro rather than a base class because `operator new` for
    // a coroutine frame is looked up in the promise's own scope, and keeping the declaration literally
    // there avoids depending on how a given compiler handles the inherited form.
    //
    // When neither diagnostic is enabled this expands to NOTHING, so a normal build declares no
    // allocation functions at all and the compiler uses global new with no indirection -- and stays
    // free to elide the allocation entirely, which a declared operator new can inhibit.
#define JLIB_CORO_FRAME_ALLOC                                                                   \
        static void* operator new(std::size_t n) { return ::JLib::detail::FrameAlloc(n); }      \
        static void operator delete(void* p) noexcept { ::JLib::detail::FrameFree(p); }         \
        static void operator delete(void* p, std::size_t) noexcept { ::JLib::detail::FrameFree(p); }

#if defined(JLIBSCHED_CORO_STATS)
    // Prints the histogram. Call it after the workload, not during -- it reads relaxed counters and
    // makes no attempt at a consistent snapshot.
    inline void DumpCoroFrameStats() {
        auto& s = detail::FrameStats();
        static const char* kNames[] = { "<=64", "<=128", "<=256", "<=512", "<=1024", "<=2048", ">2048" };
        std::printf("coroutine frames: %llu allocated, peak %lld live, largest %llu bytes\n",
                    (unsigned long long)s.allocations.load(),
                    (long long)s.peakLive.load(),
                    (unsigned long long)s.maxSize.load());
        for (size_t i = 0; i < detail::CoroFrameStats::kBuckets; ++i) {
            const uint64_t c = s.bucket[i].load();
            if (!c) continue;
            std::printf("  %-8s %10llu  (%.1f%%)\n", kNames[i], (unsigned long long)c,
                        100.0 * (double)c / (double)(s.allocations.load() ? s.allocations.load() : 1));
        }
        const int64_t leaked = s.live.load();
        if (leaked != 0)
            std::printf("  WARNING: %lld frames still live -- a leak, or work still in flight\n",
                        (long long)leaked);
    }
#endif // JLIBSCHED_CORO_STATS

    namespace detail {
        // The bridge between the C++17 worker and a C++20 frame. Stored in Task::fn, so the worker
        // calls it without knowing what a coroutine is.
        //
        // DELIBERATELY NOT TEMPLATED ON THE PROMISE TYPE. `Task::data` does not always hold the
        // handle it started with -- see ArmResume -- so this must be able to resume whatever frame
        // is currently there. The void specialization of coroutine_handle is exactly that: it can
        // address any coroutine, and resuming through it is well defined regardless of promise type.
        // A typed handle would make every nested resume undefined behaviour that happens to work.
        //
        // IT MUST TOUCH NOTHING AFTER resume() RETURNS. By then the coroutine has either suspended
        // (and may already be running on another worker) or finished (and destroyed its own frame
        // and Task). Adding a `handle.done()` check here is the bug described at the top.
        inline void ResumeCoroutine(void* p) {
            std::coroutine_handle<>::from_address(p).resume();
        }

        // Points the Task at THIS coroutine before it is re-pushed, and hands back the Task.
        //
        // Every suspending awaiter must go through this. A Lazy runs inline on the awaiting
        // coroutine's Task, so `task` alone identifies the scheduled unit but NOT where to resume
        // it: `Task::data` still holds whichever frame was last armed, which for a nested Lazy is
        // its parent -- or the root Coro. Re-pushing without updating it resumes the parent, which
        // is parked at `co_await thisLazy` and will immediately read a result that does not exist.
        // That produced wrong VALUES rather than a crash, which is how it got caught.
        //
        // Ordering is carried by the task queue: this write happens-before the Push that publishes
        // the task, and the worker's read happens after it pops.
        template <typename P>
        inline Task* ArmResume(std::coroutine_handle<P> h) noexcept {
            // THE ONE CHOKE POINT for coroutine suspension: every parking await goes through here to
            // re-arm its resume, so this is where the epoch invariant is checkable exactly once. The
            // fiber tripwires sit on Fiber::Suspend and the wait primitives and cover none of this --
            // a co_await is not a fiber suspend. Debug-only; see CoroEpochGuardSuspendCheck for why
            // the coroutine case is a use-after-free rather than the fiber case.s leak.
            JLIB_EPOCH_CHECK_NO_GUARD_CORO();

            // AND THE HAZARD HALF, which is ALWAYS ON rather than debug-only. A coroutine may hold a
            // HazardGuard -- worker cells are right for a frame that stays put -- but it may not
            // carry one across this line, because resuming on another worker leaves the
            // announcement behind and the protection silently stops.
            //
            // THIS IS THE EXACT TRANSITION, which is why there are no false positives: a coroutine
            // whose await_ready() returns true never reaches this function at all, so a
            // non-suspending guarded span costs nothing and raises nothing. A parked FIBER holding a
            // guard is legal and is excluded at the guard, not here -- fiber rows never count.
            //
            // Compiled in for Release deliberately: one thread-local load, at a point already paying
            // a re-push and a wake. Leaving it out would make a violation a use-after-free in the
            // build nobody is watching. See HazardDomain::FatalSuspendWithGuard.
            if (const std::size_t d = HazardDomain::SuspendUnsafeDepth())
                HazardDomain::FatalSuspendWithGuard(d);

            Task* t = h.promise().task;
            t->data = h.address();
            return t;
        }

        // True if the task currently executing this coroutine has been cancelled. Uses the same
        // single decision point as everything else (IsTaskCancelled), so a coroutine, a fiber and
        // a worker at pickup all agree about what "cancelled" means.
        //
        // Off a task entirely -- a coroutine resumed on a bare thread -- this is false, matching
        // TaskScheduler::CurrentTaskCancelled. Unscoped work is not cancelled work.
        inline bool CurrentCoroTaskCancelled() noexcept {
            if (!TaskScheduler::IsInitialized()) return false;
            return IsTaskCancelled(TaskScheduler::Instance().GetCurrentTask());
        }
    }

    // A coroutine that runs on the scheduler pool. Fire-and-forget: it reports completion through a
    // WaitGroup rather than returning a value.
    //
    //     JLib::Coro Work(int n) {
    //         DoSomething(n);
    //         co_await JLib::Reschedule{};   // yields this worker, resumes on any worker
    //         DoMore(n);
    //     }
    //     JLib::WaitGroup wg;
    //     JLib::Spawn(Work(1), &wg);
    //     JLib::Spawn(Work(2), &wg);
    //     sched.WaitFor(wg);
    //
    // A value-returning Coro<T> is deliberately not here yet: it needs the frame to outlive the body
    // so the result can be read, which is a different lifetime shape from the self-destructing one
    // below and wants designing rather than bolting on.
    class Coro {
    public:
        struct promise_type;
        using Handle = std::coroutine_handle<promise_type>;

        struct promise_type {
            // Must live in the PROMISE, not in Coro. The compiler looks up `operator new` for a
            // coroutine frame in the promise type's scope; on the wrapper class it is simply never
            // called, and the instrumentation silently measures nothing. (It did, briefly.)
            JLIB_CORO_FRAME_ALLOC

            // Set by Spawn() before the task is ever pushed, and read only from inside this
            // coroutine. Null until spawned, which is what makes an un-spawned Coro safe to destroy.
            Task* task = nullptr;


            // Optional: a TaskDAG external node this coroutine completes when it finishes. Null for
            // an ordinary Spawn. Just the node, not the DAG -- TaskNode carries its owner, which is
            // why CreateExternalNode sets it.
            //
            // This is how a coroutine PARTICIPATES IN A GRAPH without being a node. It cannot be one:
            // the DAG defines completion as "the task's fn returned", and resuming a coroutine
            // returns at every suspension (see the guard in TaskDAG::CreateNode). Signalling from the
            // end of the body says the same thing accurately, and costs one pointer.
            //
            // What it buys: a DAG that waits on coroutine work no longer needs a fiber node to block
            // in. A suspended fiber node holds a 64KB stack; a suspended coroutine holds its frame
            // and its Task -- two slab slots, ~512 bytes. That is the difference between hundreds of
            // concurrent waits and tens of thousands.
            TaskNode* dagNode = nullptr;

            Coro get_return_object() noexcept { return Coro{ Handle::from_promise(*this) }; }

            // The coroutine must NOT begin on the thread that called the factory function -- it
            // begins when a worker picks up its Task. Without this, Spawn() would run the body
            // inline up to the first suspend, on the caller's thread, before it was scheduled at
            // all.
            std::suspend_always initial_suspend() noexcept { return {}; }

            // COMPLETION HAPPENS HERE, NOT IN return_void(), and the distinction is load-bearing.
            //
            // C++ runs return_void() and THEN destroys the body's automatic objects, and only then
            // reaches final_suspend(). Completing in return_void() therefore announced "this
            // coroutine is done" while its own locals were still alive and still running
            // destructors. Three things were wrong with that, and they were found in that order:
            //
            //   WaitFor(wg) WAS NOT A HAPPENS-BEFORE for anything the frame owned. A waiter woke
            //   while ~HazardGuard, ~SchedulerLock and every other RAII local had yet to run.
            //   Measured at up to a millisecond on the cancellation path, not a few instructions.
            //
            //   HAZARD RECORDS WERE HELD PAST COMPLETION, so kMaxRecords saw pressure from frames
            //   that were logically finished -- and record exhaustion is a fatal abort, by design.
            //
            //   currentRunningTask WENT STALE WHILE USER CODE COULD STILL RUN. Complete() returns
            //   the Task to the slab, but the worker loop does not clear its pointer until the
            //   resume returns -- so through the whole unwind window w->currentRunningTask named a
            //   slab slot that another thread may already have reallocated and re-tagged. Anything
            //   a destructor called that reads the task type (HazardGuard's constructor does) could
            //   read Native for a coroutine and silently take worker cells. That is the exact
            //   memory-safety downgrade Hazard.cpp refuses structurally, arrived at through a lying
            //   detector rather than through the fallthrough.
            //
            // SAFE HERE, which the old comment doubted: final_suspend() is a member call on a LIVE
            // frame. suspend_never means the frame is destroyed after this returns, not before it
            // is entered, so the promise is fully valid for the duration of the call. What was true
            // is that nothing may touch the promise AFTER final_suspend returns -- and nothing does.
            //
            // A frame destroyed without completing (destroy() on a suspended one, or an exception,
            // which terminates) skips this exactly as it previously skipped return_void(). No
            // change: both are the same set of paths.
            std::suspend_never final_suspend() noexcept { Complete(); return {}; }

            void return_void() noexcept {}

            // Matches what an exception escaping a Native task does: Task::Execute is noexcept, so
            // this propagates out of resume() and terminates. Deliberately not swallowed -- a
            // fire-and-forget coroutine has nobody to report to, and silently dropping the
            // exception would strand its WaitGroup instead.
            void unhandled_exception() { throw; }

            // Releases the Task, mirroring the worker's fast-path sequence exactly. Skipping any
            // step leaks a slab slot; this is the same sequence TryRunStolenNativeTask has to
            // repeat, for the same reason.
            void Complete() noexcept {
                Task* t = task;
                if (!t) return;          // never spawned, or already completed
                task = nullptr;

                // Signal BEFORE freeing: a waiter released here may look at nothing but the group,
                // but the task is dead the moment it is handed back to the slab.
                if (t->waitGroup) {
                    const int old = t->waitGroup->n.fetch_sub(1, std::memory_order_acq_rel);
                    if ((old & WaitGroup::COUNT_MASK) == 1 && (old & WaitGroup::WAITER_BIT))
                        t->waitGroup->WakeAll();
                }

                auto& sched = TaskScheduler::Instance();
                sched.CleanupTaskMetadata(t);
                DestroyTask(t);
                sched.GetAllocator()->Free(t);

                // LAST, and after the Task is back in the slab. Signalling fires the node's
                // dependents, which may start running on another worker before this call returns --
                // so everything this coroutine still owns is released first. (The frame outlives
                // this by a moment; final_suspend destroys it once Complete returns. Nothing the
                // dependents can reach points at it.)
                if (TaskNode* n = dagNode) {
                    dagNode = nullptr;                 // exactly once, like the task above
                    SignalExternalNode(n);
                }
            }
        };

        Coro() noexcept = default;
        explicit Coro(Handle h) noexcept : h_(h) {}

        Coro(Coro&& other) noexcept : h_(std::exchange(other.h_, {})) {}
        Coro& operator=(Coro&& other) noexcept {
            if (this != &other) {
                if (h_) h_.destroy();
                h_ = std::exchange(other.h_, {});
            }
            return *this;
        }
        Coro(const Coro&) = delete;
        Coro& operator=(const Coro&) = delete;

        // Only ever destroys a coroutine that was never spawned -- Spawn() takes the handle via
        // Release(). A spawned coroutine's frame is owned by the coroutine itself (final_suspend is
        // suspend_never) and destroying it from here would be a double free.
        ~Coro() { if (h_) h_.destroy(); }

        Handle Release() noexcept { return std::exchange(h_, {}); }
        explicit operator bool() const noexcept { return static_cast<bool>(h_); }

    private:
        Handle h_{};
    };


    // Schedules a coroutine on the pool. Returns false only if the task slab is exhausted, in which
    // case the coroutine is destroyed without running and the WaitGroup is left untouched.
    //
    // The WaitGroup is incremented HERE, before the push, and not inside the coroutine: incrementing
    // after the task is visible would let a waiter observe a count of zero and proceed while this
    // coroutine had not started.
    // cancelToken: hand a spawned coroutine a scope so an abort reaches it. Spawn CREATES the
    // Task, so a caller has no `t` to stamp afterwards -- without this parameter the
    // cancellable awaiters are unreachable for anything spawned this way, which is everything.
    //
    // Typically dag.Token().Raw(), so work a graph starts but does not own is cancelled with it.
    // hiPri DEFAULTS OFF AND SHOULD USUALLY STAY OFF. Setting it puts this coroutine AND EVERY
    // RESUME OF IT on the low-latency lane, which is served by K workers -- 0 or 1 by default. It is
    // a claim that the body is short and worth one of those slots, not a request for speed; a pool
    // whose resumes all land there is serialised through K workers rather than accelerated. See the
    // block above SetHotWorkers in TaskScheduler.h for the measurement behind that.
    inline bool Spawn(Coro&& c, WaitGroup* wg = nullptr,
                      uint8_t hiPri = 0, CorePref pref = CorePref::Default,
                      uint32_t cancelToken = CancelToken::kNone) {
        Coro::Handle h = c.Release();
        if (!h) return false;

        auto& sched = TaskScheduler::Instance();
        Task* t = sched.CreateTask(&detail::ResumeCoroutine,
                                   h.address(), hiPri, FiberSize::Standard,
                                   TaskType::Coroutine, pref);
        if (!t) { h.destroy(); return false; }

        h.promise().task = t;
        t->cancelToken = cancelToken;
        if (wg) {
            wg->n.fetch_add(1, std::memory_order_relaxed);
            t->waitGroup = wg;
        }

        // Last line for a reason: once pushed, a worker may resume, finish and free all of this
        // before Push() has even returned. Nothing may be read back afterwards.
        return sched.Push(t);
    }

    // Schedules a coroutine that COMPLETES A DAG EXTERNAL NODE when it finishes, making coroutine
    // work a dependency edge in a graph:
    //
    //     auto* n = dag.CreateExternalNode();
    //     dag.AddDependency(consume, n);          // consume waits for the coroutine
    //     JLib::Spawn(DoAsyncWork(...), n);
    //
    // A coroutine still cannot BE a node -- the DAG reads completion as "the task's fn returned",
    // and resuming a coroutine returns at every suspension (see TaskDAG::CreateNode's guard).
    // Signalling from the end of the body says the same thing accurately.
    //
    // The alternative is a fiber node that Spawns and WaitFors, which is correct but holds a 64KB
    // stack for the whole wait. This holds a frame and a Task -- two slab slots -- so a graph can
    // have tens of thousands of coroutine waits outstanding instead of hundreds.
    //
    // NOTHING HERE MAKES THE DAG REQUIRE C++20. Only this header does, and only a translation unit
    // that spawns a coroutine includes it; `SignalExternal` is plain C++17 and an external node is
    // equally happy being completed by an IOCP callback, a fence, or any thread.
    // See the note on the WaitGroup overload for why the token is a parameter here.
    inline bool Spawn(Coro&& c, TaskNode* node,
                      uint8_t hiPri = 0, CorePref pref = CorePref::Default,
                      uint32_t cancelToken = CancelToken::kNone) {
        Coro::Handle h = c.Release();
        if (!h) return false;

        auto& sched = TaskScheduler::Instance();
        Task* t = sched.CreateTask(&detail::ResumeCoroutine,
                                   h.address(), hiPri, FiberSize::Standard,
                                   TaskType::Coroutine, pref);
        if (!t) { h.destroy(); return false; }

        h.promise().task    = t;
        t->cancelToken = cancelToken;
        h.promise().dagNode = node;

        // Last line: once pushed, a worker may run this to completion -- signalling the node and
        // freeing all of it -- before Push() returns.
        return sched.Push(t);
    }

    // `co_await Reschedule{}` -- give up this worker and continue on whichever worker picks the
    // task up next. Useful on its own for fairness, and it is the minimal awaiter: every I/O
    // awaiter has this same shape, differing only in WHEN the task is re-pushed (on completion,
    // rather than immediately).
    struct Reschedule {
        bool await_ready() const noexcept { return false; }

        template <typename P>
        void await_suspend(std::coroutine_handle<P> h) const noexcept {
            // Read the task out FIRST. The coroutine is already suspended by the time this runs, so
            // the moment Push() makes it visible another worker may resume it, run it to completion
            // and free both the frame and the Task -- while this call is still on the stack.
            // Reading h.promise() after the push is a use-after-free.
            Task* t = detail::ArmResume(h);
            TaskScheduler::Instance().Push(t);
        }

        void await_resume() const noexcept {}
    };

    // ---- superset primitives: the co_await spelling ---------------------------------------------
    //
    // `SchedulerMutex` and `SchedulerSemaphore` serve all three contexts. A bare thread blocks and
    // helps, a fiber suspends, and a coroutine suspends here -- on the SAME object, so a fiber and a
    // coroutine contending for one lock is well defined rather than merely not crashing.
    //
    // Only the spelling differs, and it has to: a coroutine cannot suspend inside `Lock()` because
    // suspension is part of a coroutine's type, not something a called function can do to it. Hence
    // `co_await LockAsync(m)` rather than a third branch inside Lock().
    //
    //     co_await JLib::LockAsync(m);
    //     ... critical section ...
    //     m.Unlock();                       // plain call: releasing never suspends
    //
    // NOT RAII, deliberately. A guard whose destructor unlocks would have to run inside the
    // coroutine frame, and the natural spelling (`auto g = co_await ScopedLockAsync(m)`) hides a
    // suspension point inside what reads like a constructor. Explicit Unlock is uglier and truthful.
    // Note also that the fiber-side ScopedPermit's ownership counting deliberately does NOT apply to
    // coroutines -- see the note on WaitAsyncEnqueue.

    class LockAwaiter {
    public:
        explicit LockAwaiter(SchedulerMutex& m) noexcept : m_(m) {}

        // Always false: the acquire attempt happens in await_suspend, under the mutex's own
        // spinlock, together with the enqueue. Trying here instead would open the lost-wakeup gap
        // that LockAsyncEnqueue exists to close.
        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) const {
            Task* t = detail::ArmResume(h);
            // true  -> lock acquired, DO NOT suspend (returning false resumes immediately).
            // false -> queued; stay suspended and touch nothing further -- Unlock may already have
            //          re-pushed this task onto another worker.
            return !m_.LockAsyncEnqueue(t);
        }

        void await_resume() const noexcept {}

    private:
        SchedulerMutex& m_;
    };

    class AcquireAwaiter {
    public:
        explicit AcquireAwaiter(SchedulerSemaphore& s) noexcept : s_(s) {}
        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) const {
            Task* t = detail::ArmResume(h);
            return !s_.WaitAsyncEnqueue(t);
        }

        void await_resume() const noexcept {}

    private:
        SchedulerSemaphore& s_;
    };

    // Free functions rather than members, so TaskScheduler.h never has to name a coroutine type and
    // stays compilable as C++17.

    // ---- cancellable variants ------------------------------------------------------------------
    //
    // `co_await LockAsyncCancellable(m)` yields a WaitResult instead of nothing. Cancelled means
    // THE LOCK WAS NOT ACQUIRED -- do not Unlock, and do not proceed as though you hold it. That is
    // the same rule as the fiber-side LockCancellable, and it is the reason these are separate
    // types rather than a flag on the existing ones: a caller who ignores the result of the plain
    // awaiter cannot be wrong, because the plain awaiter cannot come back empty-handed.
    //
    // WHY THE RESULT IS A MEMBER and not a local in await_suspend. The releaser writes through this
    // slot from another thread, at an arbitrary time, and the write must land somewhere that is
    // still alive. An awaiter object lives IN THE COROUTINE FRAME for the whole co_await
    // expression, so a member of it is stable for exactly as long as the suspension -- which is the
    // property Waiter's contract requires and a stack local in await_suspend would not have.
    //
    // The awaiter is therefore NOT copyable and must be awaited where it is constructed. That is
    // the normal spelling anyway (`co_await LockAsyncCancellable(m)`), and holding one across a
    // suspension point would be a bug the type system now prevents.
    class LockAwaiterCancellable {
    public:
        explicit LockAwaiterCancellable(SchedulerMutex& m) noexcept : m_(m) {}
        LockAwaiterCancellable(const LockAwaiterCancellable&) = delete;
        LockAwaiterCancellable& operator=(const LockAwaiterCancellable&) = delete;

        // ALREADY CANCELLED: do not suspend and do not take the lock. Acquiring here would hand the
        // caller something it is immediately going to be told to drop, and it would have to know to
        // Unlock on a Cancelled return -- the exact confusion this API exists to avoid.
        bool await_ready() noexcept {
            if (detail::CurrentCoroTaskCancelled()) {
                result_ = WaitResult::Cancelled;
                return true;
            }
            return false;
        }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h);
            // true  -> acquired, DO NOT suspend. false -> queued; touch nothing further, Unlock may
            //          already have re-pushed this task onto another worker.
            return !m_.LockAsyncEnqueue(t, &result_);
        }

        [[nodiscard]] WaitResult await_resume() const noexcept { return result_; }

    private:
        SchedulerMutex& m_;
        WaitResult result_ = WaitResult::Ok;
    };

    // Same contract for a semaphore permit: Cancelled means NO PERMIT WAS TAKEN, so do not Signal()
    // to "give it back" -- there is nothing to give back.
    //
    class AcquireAwaiterCancellable {
    public:
        explicit AcquireAwaiterCancellable(SchedulerSemaphore& s) noexcept : s_(s) {}
        AcquireAwaiterCancellable(const AcquireAwaiterCancellable&) = delete;
        AcquireAwaiterCancellable& operator=(const AcquireAwaiterCancellable&) = delete;

        bool await_ready() noexcept {
            if (detail::CurrentCoroTaskCancelled()) {
                result_ = WaitResult::Cancelled;
                return true;
            }
            return false;
        }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            Task* t = detail::ArmResume(h);
            return !s_.WaitAsyncEnqueue(t, &result_);
        }

        [[nodiscard]] WaitResult await_resume() const noexcept { return result_; }

    private:
        SchedulerSemaphore& s_;
        WaitResult result_ = WaitResult::Ok;
    };


    inline LockAwaiter    LockAsync(SchedulerMutex& m) noexcept     { return LockAwaiter{ m }; }
    inline AcquireAwaiter AcquireAsync(SchedulerSemaphore& s) noexcept { return AcquireAwaiter{ s }; }

    // Cancellable spelling. `WaitResult r = co_await LockAsyncCancellable(m);` -- Cancelled means
    // NOTHING WAS ACQUIRED: do not Unlock, do not Signal, do not proceed as though you hold it.
    inline LockAwaiterCancellable    LockAsyncCancellable(SchedulerMutex& m) noexcept { return LockAwaiterCancellable{ m }; }

    inline AcquireAwaiterCancellable AcquireAsyncCancellable(SchedulerSemaphore& s) noexcept { return AcquireAwaiterCancellable{ s }; }

    // ---- Lazy<T>: a coroutine that RETURNS something -------------------------------------------
    //
    //     JLib::Lazy<int> Compute(int n) {
    //         co_await JLib::Reschedule{};
    //         co_return n * 2;
    //     }
    //     JLib::Lazy<int> Caller() { int x = co_await Compute(21); co_return x; }
    //     int result = JLib::SyncWait(Caller());     // from a non-coroutine
    //
    // NO FUTURE IS INVOLVED, and that is the point rather than an omission. A future exists to carry
    // a value from a producer's stack to a consumer's; a coroutine frame already holds the value and
    // already knows who is waiting, so the promise IS the shared state. That is also why nothing here
    // needs a lock: exactly one coroutine awaits a given Lazy, and the handoff is a resume.
    //
    // WHY THIS IS A SEPARATE TYPE FROM `Coro` rather than a template parameter on it. `Coro` is
    // fire-and-forget: final_suspend is suspend_never, so its frame self-destructs the moment the
    // body ends. A returned value has to outlive the body long enough to be read, so Lazy suspends at
    // final_suspend and its frame is owned and destroyed by the Lazy object. Two lifetimes, two
    // types.
    //
    // LAZY, AND INLINE ON AWAIT. The body does not start until something awaits it, and when it does
    // it runs on the AWAITING worker rather than being pushed. Pushing would buy a dispatch and lose
    // locality for no gain -- the awaiting coroutine cannot proceed regardless. Parallelism comes
    // from Spawn()ing several `Coro`s and joining a WaitGroup, not from making every await a fork.

    namespace detail {
        struct LazyPromiseBase {
            JLIB_CORO_FRAME_ALLOC

            // Who to resume when this coroutine finishes. Empty for a Lazy nobody has awaited.
            std::coroutine_handle<> continuation{};
            std::exception_ptr eptr;

            // The ROOT task this Lazy is running under, inherited from whoever awaited it.
            //
            // A Lazy has no Task of its own -- it is not scheduled, it runs inline on the awaiting
            // coroutine's worker, so the whole await chain is ONE scheduled unit. But suspending
            // awaiters need a Task to re-push, and `co_await Reschedule{}` or `co_await LockAsync(m)`
            // inside a nested Lazy has to re-push the root, not nothing. Awaiter::await_suspend
            // copies it down as the chain descends.
            Task* task = nullptr;

            std::suspend_always initial_suspend() noexcept { return {}; }

            // SYMMETRIC TRANSFER, and it is the one thing here that must not be simplified.
            // Returning the continuation's handle makes the compiler TAIL-CALL into it, so a chain
            // of N awaits costs O(1) stack. Calling `continuation.resume()` here instead and
            // returning void is O(N): every completed coroutine keeps a frame on the machine stack
            // while resuming the next. That difference is invisible in a small test and shows up as
            // a stack overflow under depth -- see the deep-chain case in the coroutine test, which
            // exists specifically to catch it.
            struct FinalAwaiter {
                bool await_ready() const noexcept { return false; }
                template <typename P>
                std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
                    std::coroutine_handle<> c = h.promise().continuation;
                    // noop_coroutine, not a null handle: returning null from await_suspend is
                    // undefined. This is the "finished but nobody was awaiting" case -- it happens
                    // for a Lazy that was started and abandoned.
                    return c ? c : std::noop_coroutine();
                }
                void await_resume() const noexcept {}
            };
            FinalAwaiter final_suspend() noexcept { return {}; }

            // Stored rather than thrown: rethrowing here would escape through resume() into a
            // noexcept Task::Execute and terminate. Held until await_resume, so it surfaces at the
            // `co_await` that asked for the value -- which is where a caller can actually catch it.
            // This is strictly better than `Coro`'s fire-and-forget behaviour, which has nobody to
            // report to and therefore terminates.
            void unhandled_exception() noexcept { eptr = std::current_exception(); }
        };

        template <typename T>
        struct LazyValue : LazyPromiseBase {
            std::optional<T> value;
            template <typename U = T>
            void return_value(U&& v) { value.emplace(std::forward<U>(v)); }
            T&& Take() {
                if (eptr) std::rethrow_exception(eptr);
                return std::move(*value);
            }
        };

        template <>
        struct LazyValue<void> : LazyPromiseBase {
            void return_void() noexcept {}
            void Take() { if (eptr) std::rethrow_exception(eptr); }
        };
    }

    template <typename T = void>
    class Lazy {
    public:
        struct promise_type : detail::LazyValue<T> {
            Lazy get_return_object() noexcept {
                return Lazy{ std::coroutine_handle<promise_type>::from_promise(*this) };
            }
        };
        using Handle = std::coroutine_handle<promise_type>;

        Lazy() noexcept = default;
        explicit Lazy(Handle h) noexcept : h_(h) {}
        Lazy(Lazy&& o) noexcept : h_(std::exchange(o.h_, {})) {}
        Lazy& operator=(Lazy&& o) noexcept {
            if (this != &o) { if (h_) h_.destroy(); h_ = std::exchange(o.h_, {}); }
            return *this;
        }
        Lazy(const Lazy&) = delete;
        Lazy& operator=(const Lazy&) = delete;

        // The Lazy owns its frame for its whole life -- unlike Coro, whose frame self-destructs.
        // Destroying an un-started or partially-run Lazy is well defined and runs the destructors of
        // whatever locals the body had constructed so far.
        ~Lazy() { if (h_) h_.destroy(); }

        struct Awaiter {
            Handle h;
            // Nothing to wait for if it already finished -- await_resume reads the stored value.
            bool await_ready() const noexcept { return !h || h.done(); }

            // Templated on the AWAITING promise so the root Task can be read off it. A type-erased
            // coroutine_handle<> would compile but leaves the inner promise with no task, and then
            // any suspending awaiter inside the inner Lazy has nothing to re-push.
            template <typename P>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<P> outer) noexcept {
                h.promise().continuation = outer;
                h.promise().task = outer.promise().task;   // inherit; see LazyPromiseBase::task
                // Returning the inner handle STARTS (or resumes) it by tail-call, on this worker.
                // That is both the lazy start and the reason an await costs no dispatch.
                return h;
            }

            decltype(auto) await_resume() { return h.promise().Take(); }
        };

        // Rvalue-only: awaiting a Lazy consumes it. Awaiting the same one twice would resume a
        // finished coroutine, which is undefined; the && qualifier makes that a compile error at the
        // obvious call sites rather than a runtime mystery.
        Awaiter operator co_await() && noexcept { return Awaiter{ h_ }; }

        bool Done() const noexcept { return !h_ || h_.done(); }
        explicit operator bool() const noexcept { return static_cast<bool>(h_); }

    private:
        Handle h_{};
    };

    namespace detail {
        // Named functions, not lambdas. A lambda-coroutine does NOT own its closure -- the frame
        // stores the captures by reference for a [&] lambda, and the temporary lambda object dies at
        // the end of the full-expression that spawned it, long before the coroutine finishes. That is
        // the classic dangling-coroutine-lambda bug. These take pointers by value into stack objects
        // that SyncWait keeps alive by blocking.
        template <typename T>
        Coro SyncWaitRunner(Lazy<T>* lazy, std::optional<T>* out) {
            out->emplace(co_await std::move(*lazy));
        }
        inline Coro SyncWaitRunnerVoid(Lazy<void>* lazy) {
            co_await std::move(*lazy);
        }
    }

    // Runs a Lazy to completion from a NON-coroutine context (main, a bare thread) and returns its
    // value. Blocks on a WaitGroup, so the caller must be somewhere blocking is acceptable -- from
    // inside a coroutine, `co_await` it instead.
    //
    // An exception thrown by the coroutine propagates out of the `co_await` inside the runner, where
    // nobody can catch it -- so it terminates, exactly as it would for any other spawned Coro. Catch
    // inside the Lazy if that matters, or await it from a coroutine that can handle it.
    template <typename T>
    T SyncWait(Lazy<T> lazy) {
        std::optional<T> out;
        WaitGroup wg;
        Spawn(detail::SyncWaitRunner<T>(&lazy, &out), &wg);
        TaskScheduler::Instance().WaitFor(wg);
        return std::move(*out);
    }

    inline void SyncWait(Lazy<void> lazy) {
        WaitGroup wg;
        Spawn(detail::SyncWaitRunnerVoid(&lazy), &wg);
        TaskScheduler::Instance().WaitFor(wg);
    }

    // ================================================================================================
    // `co_await` FOR Future<T>. The C++17 half -- Promise, the shared state, the waiter list -- is in
    // Future.h and never sees <coroutine>; this is the only part that needs C++20, exactly the split
    // IoReactor and IoAsync use.
    //
    //     const Texture& t = co_await fut;              // uncancellable
    //     auto r = co_await fut.Wait(scope.Token());    // cancellable; check r.status
    //
    // WHY THE WAITER IS A MEMBER, and it is the same load-bearing detail as IoOpAwaiter's request:
    // the node is linked into a list owned by the shared state, which may outlive this coroutine. It
    // lives in the FRAME, so it is alive for exactly as long as the suspension it represents, and the
    // unlink-before-resume rule in Future.h is what guarantees nothing touches it afterwards.
    //
    // ONCE await_suspend RETURNS FALSE, TOUCH NOTHING. The value may already have landed on another
    // thread and re-pushed this coroutine before this function has returned -- the same rule as
    // everywhere else in this header.
    template <class T>
    struct FutureResult {
        FutureStatus status = FutureStatus::Broken;
        const T*     value  = nullptr;      // null unless status == Ready

        [[nodiscard]] bool Ok() const noexcept { return status == FutureStatus::Ready; }
        const T& operator*() const noexcept { return *value; }
    };

    // No value to point at; the status IS the whole result.
    template <>
    struct FutureResult<void> {
        FutureStatus status = FutureStatus::Broken;
        [[nodiscard]] bool Ok() const noexcept { return status == FutureStatus::Ready; }
    };

    template <class T>
    class FutureAwaiter {
    public:
        FutureAwaiter(const Future<T>& f, CancelToken token) noexcept : f_(f), token_(token) {}
        FutureAwaiter(const FutureAwaiter&) = delete;
        FutureAwaiter& operator=(const FutureAwaiter&) = delete;

        // ALWAYS false, for the same reason IoOpAwaiter's is: the "is it already final" question is
        // answered in ONE place -- ReadyOrQueue, under the lock -- and asking it here as well is how
        // the two answers drift apart.
        bool await_ready() const noexcept { return false; }

        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) {
            auto* s = f_.State_();
            if (!s) { w_.status = FutureStatus::Broken; return false; }
            Task* t = detail::ArmResume(h);
            return !s->ReadyOrQueue(&w_, t, token_);
        }

        [[nodiscard]] FutureResult<T> await_resume() const noexcept {
            FutureResult<T> r;
            r.status = w_.status;
            if constexpr (!std::is_void_v<T>) {
                if (r.status == FutureStatus::Ready && f_.State_()) r.value = f_.State_()->Value();
            }
            return r;
        }

    private:
        const Future<T>&     f_;
        CancelToken          token_;
        detail::FutureWaiter w_{};      // MEMBER: lives in the frame, dies with the suspension
    };

    // Bare `co_await fut` -- uncancellable, and yields `const T&` directly. The uncancellable form
    // stays the simple one for the same reason WaitFor(wg) does: most waits are not scoped, and
    // making every caller unpack a status would be a tax on the common case. Use Wait(token) when the
    // wait belongs to a scope. Broken here is a programmer error (the producer was dropped), and the
    // assert says so rather than silently handing back a dangling reference.
    template <class T>
    class FutureRefAwaiter {
    public:
        explicit FutureRefAwaiter(const Future<T>& f) noexcept : inner_(f, CancelToken{}) {}

        bool await_ready() const noexcept { return inner_.await_ready(); }
        template <typename P>
        bool await_suspend(std::coroutine_handle<P> h) { return inner_.await_suspend(h); }

        [[nodiscard]] decltype(auto) await_resume() const noexcept {
            const FutureResult<T> r = inner_.await_resume();
            assert(r.Ok() && "co_await on a Future whose Promise was destroyed unset -- "
                             "use WaitFuture(fut, token) if the producer may legitimately go away");
            if constexpr (!std::is_void_v<T>) return (*r.value);
        }

    private:
        FutureAwaiter<T> inner_;
    };

    template <class T>
    [[nodiscard]] inline FutureRefAwaiter<T> operator co_await(const Future<T>& f) noexcept {
        return FutureRefAwaiter<T>(f);
    }

    // Cancellable form. Cancels THIS WAIT only -- the producer keeps producing and the other
    // consumers keep waiting. See the header note in Future.h.
    template <class T>
    [[nodiscard]] inline FutureAwaiter<T> WaitFuture(const Future<T>& f, CancelToken token) noexcept {
        return FutureAwaiter<T>(f, token);
    }

} // namespace JLib
