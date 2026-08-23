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

#include <coroutine>
#include <cstdint>
#include <exception>
#include <optional>
#include <utility>

namespace JLib {

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
            Task* t = h.promise().task;
            t->data = h.address();
            return t;
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
            // Set by Spawn() before the task is ever pushed, and read only from inside this
            // coroutine. Null until spawned, which is what makes an un-spawned Coro safe to destroy.
            Task* task = nullptr;

            Coro get_return_object() noexcept { return Coro{ Handle::from_promise(*this) }; }

            // The coroutine must NOT begin on the thread that called the factory function -- it
            // begins when a worker picks up its Task. Without this, Spawn() would run the body
            // inline up to the first suspend, on the caller's thread, before it was scheduled at
            // all.
            std::suspend_always initial_suspend() noexcept { return {}; }

            // suspend_never: the frame destroys itself once the body is done. Completion therefore
            // has to happen in return_void(), which runs BEFORE final_suspend -- by the time the
            // final awaiter is reached the promise is on borrowed time, and after it the frame (and
            // this object) is gone.
            std::suspend_never final_suspend() noexcept { return {}; }

            void return_void() noexcept { Complete(); }

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
    inline bool Spawn(Coro&& c, WaitGroup* wg = nullptr,
                      uint8_t hiPri = 0, CorePref pref = CorePref::Default) {
        Coro::Handle h = c.Release();
        if (!h) return false;

        auto& sched = TaskScheduler::Instance();
        Task* t = sched.CreateTask(&detail::ResumeCoroutine,
                                   h.address(), hiPri, FiberSize::Standard,
                                   TaskType::Coroutine, pref);
        if (!t) { h.destroy(); return false; }

        h.promise().task = t;
        if (wg) {
            wg->n.fetch_add(1, std::memory_order_relaxed);
            t->waitGroup = wg;
        }

        // Last line for a reason: once pushed, a worker may resume, finish and free all of this
        // before Push() has even returned. Nothing may be read back afterwards.
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
    inline LockAwaiter    LockAsync(SchedulerMutex& m) noexcept     { return LockAwaiter{ m }; }
    inline AcquireAwaiter AcquireAsync(SchedulerSemaphore& s) noexcept { return AcquireAwaiter{ s }; }

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

} // namespace JLib
