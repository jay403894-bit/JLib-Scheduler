// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
#pragma once

// A value produced by one party and read by MANY. C++17; the `co_await` spelling is in Coroutine.h.
//
//     JLib::Promise<Texture> p;
//     JLib::Future<Texture>  f = p.GetFuture();      // copy it to as many consumers as you like
//     ...
//     p.Set(LoadTexture(path));                      // from a worker, an I/O completion, any thread
//     const Texture& t = co_await f;                 // N coroutines may await the same f
//
// == WHY THIS EXISTS WHEN Lazy<T> ALREADY RETURNS A VALUE ==
//
// Lazy is single-consumer BY CONSTRUCTION and says so: the coroutine frame IS the shared state, the
// promise knows its one awaiter, and nothing needs a lock because there is exactly one of everything.
// That is a real advantage and it is not being replaced -- one reader, one result, zero allocation
// and zero synchronisation is the right tool and stays the right tool.
//
// This is the case that shape structurally cannot serve: a result MANY things wait on. An asset five
// systems want, a handshake several requests are queued behind. Making Lazy shareable would mean
// giving it refcounting, a waiter list and a mutex -- i.e. turning it into this and losing the reason
// it was good. So there are two types, and the choice between them is "how many readers", not taste.
//
// == THE VOCABULARY, WHICH IS THE POINT ==
//
// Between them these four cover "something will eventually become ready" without every suspension
// mechanism needing its own bespoke type:
//
//   Lazy<T>                    single-consumer OWNERSHIP. You own the frame, you destroy it, you
//                              are the only reader -- so nothing needs a refcount or a lock. Lazy:
//                              the body does not start until awaited, and runs INLINE on the
//                              awaiting worker. Move-only.
//   Future<T>                  copyable, refcounted, multi-consumer OBSERVATION. N readers observe
//                              one value. Nobody owns it; it dies when the last observer does.
//   Future<void>               copyable, refcounted, multi-consumer COMPLETION. N readers observe
//                              that a thing finished.
//   Promise<T>, Promise<void>  the producer side of both. Plain C++17.
//
// OWNERSHIP vs OBSERVATION is the axis, and it is why Lazy is not simply a degenerate Future: an
// owner may move the value out and destroy the frame, which an observer must never do. That is the
// same distinction Take() encodes -- it is legal only when you have become the sole owner.
//
// COMPLETION RATHER THAN SIGNAL, and the word is load-bearing: a completion happens exactly once and
// is terminal. There is no reset and no second firing, so a consumer that arrives LATE still
// observes it. An Event is the opposite -- arrive after the signal and you wait for the next one --
// which is why Future<void> is not "an Event for coroutines" but a different thing that happens to
// serve the same need better.
//
// == THE THREE DECISIONS, AND WHAT THEY COST ==
//
// SHAREABLE AND COPYABLE, REFCOUNTED. Copying a Future is another consumer. The refcount is touched
// on copy and on destruction -- NOT on await -- so the cost is per hand-off rather than per
// operation, and in exchange the state cleans itself up with no ownership rules for the caller to
// get wrong. That trade was made deliberately.
//
// `co_await` YIELDS `const T&`, NOT A COPY. With N consumers a by-value resume would copy the value
// N times for no reason; the value lives in the shared state and is valid for as long as any Future
// referring to it is alive. `Take()` exists for the case where you know you are the last reader and
// want to move it out -- it is checked, because doing that with other readers around would hand them
// a moved-from value.
//
// PRODUCED BY A SEPARATE Promise<T>, WHICH IS PLAIN C++17. The producer can be anything: a worker, a
// Native task, an I/O completion, a thread that has never heard of this scheduler. Only the awaiting
// side needs <coroutine>, which is the same split IoReactor and IoAsync already use -- the engine is
// C++17 and the C++20 is confined to the awaiter.
//
// == CANCELLATION CANCELS THE WAIT, NEVER THE WORK ==
//
// The same rule as WaitFor(wg, token): a cancelled waiter stops waiting, and NOTHING else changes.
// The producer keeps producing, the other consumers keep waiting, and the Future does not become
// broken. Cancelling your own interest in a shared result cannot be allowed to destroy it for
// everyone else -- there is no sense in which one consumer owns a value five others are waiting on.
//
// AND CANCELLATION TAKES THE SAME MUTEX AS Set(). It does not reach past the lock to pluck a waiter
// out. That is what makes "exactly one of {Set, Cancel} claims each waiter" true, and it is the same
// discipline as the cancelled condition-variable wait that must still return holding its mutex: a
// cancel that bypasses the synchronisation it was waiting on leaves the caller in a state its own
// code does not expect. Removal happens under the lock, before the resume, and a waiter is in the
// list exactly once -- so there is no double resume and no lost one.
//
// == WHY THE SHARED STATE IS `new` AND NOT A SLAB SLOT (measured 2026-08-25) ==
//
// It fits -- that was the first question and the answer is yes:
//
//     std::mutex 80    FutureState<void> 96    <int> 104    <std::string> 128
//
// so every ordinary T lands in the existing 128-byte class and a large one lands in 256. The waiter
// node is 32 bytes and costs nothing, because it lives in the awaiter's frame.
//
// WHAT SLABBING WOULD ACTUALLY BUY, and it is not the size class. The slab exists to avoid the CRT
// allocator's lock -- it is sharded, so concurrent creation from several workers does not serialise
// -- and to keep objects contiguous. Size classes are how it is organised, not why it exists. So
// the question is not "does it fit" (it does, at 128) but "are these created concurrently, often
// enough for the allocator lock to matter".
//
// NOT SLABBED FOR NOW, on the judgement that nothing has shown they are. Tasks are created at frame
// rate by every worker at once, which is what justified the slab; a Future is created once per
// asynchronous operation and outlives it. An asset load makes a handful. A per-request server might
// make thousands a second -- and if one ever does, slab it at 128 rather than adding a class.
//
// THE COUNTERWEIGHT, which is why this is a judgement rather than an obvious yes: the 128-byte
// class is shared with coroutine frames, and it is a FIXED capacity chosen by SlabSizes. Slabbing
// Futures spends frame budget on them, so an app that makes many would hit the cap sooner and the
// symptom would appear as frame-allocation failure somewhere unrelated. `new` has no cap. That
// coupling is worth taking on for something allocated at frame rate; it is not obviously worth it
// for something allocated once per operation.
//
// A SEPARATE AND LARGER OPTION, noted because it changes the arithmetic above: std::mutex is 80 of
// these 96 bytes. A spinlock would take the void form to about 24 and drop everything into the
// 64-byte class. The lock is held only for pointer surgery and two bools, never across a
// suspension, so it is a candidate -- but a spinlock trades a bounded wait for burning a core when
// the holder is preempted, which is a bigger decision than where the memory comes from.
//
// == WHAT THIS IS NOT ==
//
// No `.then()`, no `when_all`, no `when_any`, no executors. Those turn a primitive into a framework,
// and the combinators are all expressible in a coroutine already -- `when_all` is awaiting two
// futures in a row, `when_any` is what a CancelScope with a deadline does. The same boundary
// IoReactor draws at "not more opcodes": the extension point is a coroutine that awaits these, not
// more members here.

#include "CancelToken.h"
#include "TaskScheduler.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <new>
#include <utility>

namespace JLib {

    // Why a status rather than an exception: a waiter is resumed by a WORKER, and throwing out of a
    // resume unwinds through the scheduler rather than through the caller's own stack. Every other
    // wait in this library returns its outcome, and this matches.
    enum class FutureStatus : std::uint8_t {
        Ready,       // the value is set; Get() is valid
        Cancelled,   // this waiter's scope was cancelled. Says nothing about the value.
        Broken       // the Promise died without setting anything. Nobody is coming.
    };

    namespace detail {

        // ================================================================================================
        // PARKING A COROUTINE: THE FOUR RULES. Canonical statement -- IoAcceptor implements the same
        // discipline and points here rather than restating it.
        //
        // There are deliberately TWO implementations rather than one shared type, because the
        // SEMANTICS differ: IoAcceptor is a QUEUE (N ready sockets, each waiter takes a different
        // one) and this is a BROADCAST (one value, every waiter observes it). A shared list type
        // would invite the assumption that they behave alike. What is shared is the discipline
        // below, and it is shared because getting any of it wrong is a use-after-free rather than a
        // wrong answer.
        //
        //   1. THE NODE LIVES IN THE AWAITER'S FRAME. Never heap-allocated, never owned by the
        //      structure. Its lifetime is exactly the suspension it represents, which is what makes
        //      parking allocation-free and what makes rules 3 and 4 necessary.
        //
        //   2. LINK AND UNLINK UNDER THE SAME MUTEX THAT PUBLISHES. The take-or-park decision and
        //      the hand-off-or-queue decision must be made under one lock, so one of them is always
        //      second and sees the other. That mutex is doing the job a single successful CAS would
        //      have to do in a lock-free version -- which is why neither of these is lock-free. A
        //      cancel that reaches past the lock to pluck a waiter out breaks it: exactly one of
        //      {publish, cancel} must claim each waiter, and only the lock can arbitrate that.
        //
        //   3. UNLINK BEFORE RESUMING, and read `next` BEFORE pushing. The moment a waiter's Task is
        //      pushed, another worker may resume that coroutine, run it to completion and destroy
        //      the frame the node lives in. Reading node->next afterwards reads freed memory. This
        //      is the rule that looks redundant and is not.
        //
        //   4. WAKE OUTSIDE THE LOCK. The resumed coroutine can drop the last reference to the very
        //      state whose lock is held, re-entering its destructor while it is locked. Collect the
        //      claimed waiters under the lock, release it, then push.
        //
        // NOT the Event slot table, which cannot serve either of these: Event::AddWaiter indexes by
        // `assignedFiber->poolIndex`, and a coroutine task rides the Native path with no fiber, so
        // it silently drops every waiter. Event is a FIBER suspend point; these are the coroutine
        // equivalent, and the two cannot be merged.
        // ================================================================================================

        // LIVES IN THE AWAITER'S COROUTINE FRAME, exactly like IoAcceptWaiter -- so parking allocates
        // nothing and the node's lifetime is bounded by the suspension it represents. Deliberately
        // the same shape as the acceptor's rather than a new idiom; there are already two documented
        // cases in this project of parallel structures drifting apart.
        //
        // NOT the Event slot table, which cannot serve this at all: AddWaiter indexes by
        // `assignedFiber->poolIndex` and coroutine tasks ride the Native path with no fiber, so it
        // would silently drop every waiter.
        struct FutureWaiter {
            Task*          resume = nullptr;
            std::uint32_t  token  = CancelToken::kNone;
            FutureWaiter*  next   = nullptr;
            FutureStatus   status = FutureStatus::Ready;
        };

        // Everything that does not depend on T, so the list surgery and the refcount exist once.
        class FutureStateBase {
        public:
            void Retain() noexcept { refs_.fetch_add(1, std::memory_order_relaxed); }

            // Acquire on the last release pairs with the releases above, so the destructor that
            // follows sees every write any other holder made.
            bool Release() noexcept {
                if (refs_.fetch_sub(1, std::memory_order_release) != 1) return false;
                std::atomic_thread_fence(std::memory_order_acquire);
                return true;
            }

            int  UseCount() const noexcept { return refs_.load(std::memory_order_relaxed); }
            bool Ready()  const noexcept { std::lock_guard<std::mutex> lk(m_); return ready_; }
            bool Broken() const noexcept { std::lock_guard<std::mutex> lk(m_); return broken_ && !ready_; }

            // SAME RETURN POLARITY AS EVERY Submit AND AS IoAcceptor::TakeOrQueue: true means the
            // answer is already final and the caller MUST NOT suspend; false means queued.
            //
            // The already-cancelled check happens HERE, under the lock, rather than in await_ready --
            // one place decides, so a cancel landing between a check and a link cannot lose a waiter.
            bool ReadyOrQueue(FutureWaiter* w, Task* resume, CancelToken token) {
                std::lock_guard<std::mutex> lk(m_);
                if (ready_)  { w->status = FutureStatus::Ready;  return true; }
                if (broken_) { w->status = FutureStatus::Broken; return true; }
                if (token.Valid() && token.Cancelled()) {
                    w->status = FutureStatus::Cancelled;
                    return true;
                }
                w->resume = resume;
                w->token  = token.Raw();
                w->status = FutureStatus::Ready;
                w->next   = waiters_;
                waiters_  = w;
                return false;
            }

            // Eject waiters whose scope is `token` (or all, for an invalid one). The producer is NOT
            // touched and the state is NOT marked broken -- see the header note on why one consumer
            // cannot cancel a result five others want.
            std::size_t CancelWaiters(CancelToken token) noexcept {
                FutureWaiter* taken = nullptr;
                {
                    std::lock_guard<std::mutex> lk(m_);
                    FutureWaiter** link = &waiters_;
                    while (*link) {
                        FutureWaiter* w = *link;
                        const bool match = !token.Valid() || CancelToken(w->token).IsWithin(token);
                        if (match) {
                            *link = w->next;          // UNLINKED BEFORE RESUMED, under the lock: the
                            w->status = FutureStatus::Cancelled;   // node dies with the frame the
                            w->next = taken;          // moment its task is pushed.
                            taken = w;
                        } else {
                            link = &w->next;
                        }
                    }
                }
                return Wake(taken);
            }

            // Called with the value already stored, so any waiter this wakes sees a complete value.
            // Publishing after the wake would let a resumed reader observe a half-written result.
            std::size_t Publish(bool broken) noexcept {
                FutureWaiter* taken = nullptr;
                {
                    std::lock_guard<std::mutex> lk(m_);
                    if (ready_ || broken_) return 0;      // Set twice, or Set after a break
                    if (broken) broken_ = true; else ready_ = true;
                    taken = waiters_;
                    waiters_ = nullptr;
                    for (FutureWaiter* w = taken; w; w = w->next)
                        w->status = broken ? FutureStatus::Broken : FutureStatus::Ready;
                }
                return Wake(taken);
            }

        protected:
            ~FutureStateBase() = default;

            // OUTSIDE THE LOCK. Push can run the woken coroutine to completion on another worker
            // before this returns, and that coroutine may destroy the last Future -- which would
            // re-enter this object's destructor while the lock is held.
            static std::size_t Wake(FutureWaiter* list) noexcept {
                std::size_t n = 0;
                while (list) {
                    FutureWaiter* next = list->next;   // read before the frame can die
                    Task* t = list->resume;
                    if (t && TaskScheduler::IsInitialized()) TaskScheduler::Instance().Push(t);
                    ++n;
                    list = next;
                }
                return n;
            }

            // Short-held and never across a suspension -- it guards pointer surgery and two bools,
            // nothing more. That is the reason a plain std::mutex is right here despite the usual
            // "no raw mutexes inside a task" rule, and it is the same call IoReactor makes.
            mutable std::mutex m_;
            FutureWaiter*      waiters_ = nullptr;
            std::atomic<int>   refs_{ 1 };
            bool               ready_   = false;
            bool               broken_  = false;
        };

        template <class T>
        class FutureState final : public FutureStateBase {
        public:
            ~FutureState() { if (ready_) Value()->~T(); }

            template <class U>
            std::size_t SetValue(U&& v) {
                // Constructed BEFORE Publish takes the lock, so the value is complete before any
                // waiter can be woken to read it.
                {
                    std::lock_guard<std::mutex> lk(m_);
                    if (ready_ || broken_) return 0;
                }
                ::new (static_cast<void*>(storage_)) T(std::forward<U>(v));
                return Publish(/*broken*/ false);
            }

            T*       Value()       noexcept { return reinterpret_cast<T*>(storage_); }
            const T* Value() const noexcept { return reinterpret_cast<const T*>(storage_); }

        private:
            alignas(T) unsigned char storage_[sizeof(T)];
        };

        // A MULTI-CONSUMER COMPLETION, which is not merely Future<T> with the value taken out.
        // Event cannot serve a coroutine at all -- it is a fiber suspend point, indexed by
        // assignedFiber->poolIndex -- so before this there was no way for N coroutines to observe
        // that a thing finished. It carries no value; everything else, including cancellation and
        // the broken-producer rule, is identical.
        //
        // COMPLETION RATHER THAN SIGNAL, and the word is doing work: a completion happens exactly
        // once and is terminal. There is no reset and no second firing, so a consumer that arrives
        // late still observes it -- which is the opposite of an Event, where arriving after the
        // signal means waiting for the next one.
        template <>
        class FutureState<void> final : public FutureStateBase {
        public:
            std::size_t SetValue() { return Publish(/*broken*/ false); }
        };

        // ONE COPY OF THE OWNERSHIP LOGIC, shared by Future<T> and Future<void>. Writing the refcount
        // dance twice is exactly where the two would drift, and a refcount bug is a use-after-free
        // rather than a wrong answer.
        template <class State>
        class FutureHandle {
        public:
            FutureHandle() noexcept = default;
            FutureHandle(const FutureHandle& o) noexcept : s_(o.s_) { if (s_) s_->Retain(); }
            FutureHandle(FutureHandle&& o) noexcept : s_(std::exchange(o.s_, nullptr)) {}

            FutureHandle& operator=(const FutureHandle& o) noexcept {
                if (this != &o) { FutureHandle tmp(o); Swap(tmp); }
                return *this;
            }
            FutureHandle& operator=(FutureHandle&& o) noexcept {
                if (this != &o) { FutureHandle tmp(std::move(o)); Swap(tmp); }
                return *this;
            }
            ~FutureHandle() { Reset(); }

            void Swap(FutureHandle& o) noexcept { std::swap(s_, o.s_); }
            void Reset() noexcept {
                if (s_ && s_->Release()) delete s_;
                s_ = nullptr;
            }

            [[nodiscard]] bool Valid()  const noexcept { return s_ != nullptr; }
            [[nodiscard]] bool Ready()  const noexcept { return s_ && s_->Ready(); }
            [[nodiscard]] bool Broken() const noexcept { return s_ && s_->Broken(); }
            [[nodiscard]] int  UseCount() const noexcept { return s_ ? s_->UseCount() : 0; }

            // Ejects THIS scope's waiters. Does not touch the producer or anyone else -- see the
            // cancellation note at the top of this file.
            std::size_t CancelWaiters(CancelToken token) noexcept {
                return s_ ? s_->CancelWaiters(token) : 0;
            }

            // For Coroutine.h's awaiter; not part of the supported surface.
            State* State_() const noexcept { return s_; }

        protected:
            explicit FutureHandle(State* s) noexcept : s_(s) { if (s_) s_->Retain(); }
            State* s_ = nullptr;
        };

        // Same, for the producer half.
        template <class State>
        class PromiseHandle {
        public:
            PromiseHandle() : s_(new State()) {}
            PromiseHandle(const PromiseHandle&) = delete;
            PromiseHandle& operator=(const PromiseHandle&) = delete;
            PromiseHandle(PromiseHandle&& o) noexcept : s_(std::exchange(o.s_, nullptr)) {}
            PromiseHandle& operator=(PromiseHandle&& o) noexcept {
                if (this != &o) { Break(); s_ = std::exchange(o.s_, nullptr); }
                return *this;
            }

            // A DESTROYED-UNSET PROMISE MUST NOT HANG ITS WAITERS. Dropping the producer on an error
            // path is a normal thing to do, and the readers have to find out rather than park forever.
            ~PromiseHandle() { Break(); }

            [[nodiscard]] bool Valid() const noexcept { return s_ != nullptr; }

        protected:
            void Break() noexcept {
                if (!s_) return;
                s_->Publish(/*broken*/ true);      // no-op if a value already landed
                if (s_->Release()) delete s_;
                s_ = nullptr;
            }
            State* s_ = nullptr;
        };

    } // namespace detail

    template <class T> class Promise;
    // A HANDLE, not the value. Copy it to hand another consumer a read on the same result.
    template <class T>
    class Future : public detail::FutureHandle<detail::FutureState<T>> {
        using Base = detail::FutureHandle<detail::FutureState<T>>;
    public:
        using Base::Base;
        Future() noexcept = default;

        // Precondition: Ready(). The reference is valid while any Future on this result lives.
        [[nodiscard]] const T& Get() const noexcept {
            assert(this->s_ && this->s_->Ready() && "Future::Get() before the value is set");
            return *this->s_->Value();
        }

        // MOVE OUT, and it is checked because getting it wrong is silent. With another consumer still
        // holding a Future, moving the value out hands them something moved-from -- a bug that shows
        // up as an empty asset three frames later rather than as a crash here.
        [[nodiscard]] T Take() {
            assert(this->s_ && this->s_->Ready() && "Future::Take() before the value is set");
            assert(this->UseCount() == 1 &&
                   "Future::Take() with other consumers still holding this result");
            return std::move(*this->s_->Value());
        }

    private:
        friend class Promise<T>;
    };

    // NO Get() AND NO Take(), because there is nothing to get. Everything else -- copying,
    // cancellation, the broken-producer rule -- is the general behaviour, inherited unchanged.
    template <>
    class Future<void> : public detail::FutureHandle<detail::FutureState<void>> {
        using Base = detail::FutureHandle<detail::FutureState<void>>;
    public:
        using Base::Base;
        Future() noexcept = default;
    private:
        friend class Promise<void>;
    };

    // THE PRODUCER SIDE, AND IT IS PLAIN C++17 -- settable from a worker, a Native task, an I/O
    // completion, or a thread that knows nothing about this scheduler.
    template <class T>
    class Promise : public detail::PromiseHandle<detail::FutureState<T>> {
        using Base = detail::PromiseHandle<detail::FutureState<T>>;
    public:
        [[nodiscard]] Future<T> GetFuture() const noexcept { return Future<T>(this->s_); }

        // Returns how many waiters were woken. Setting twice is a no-op, not an error: a producer
        // racing its own cancellation path should not have to sequence them.
        template <class U>
        std::size_t Set(U&& v) { return this->s_ ? this->s_->SetValue(std::forward<U>(v)) : 0; }
    };

    // Set() takes nothing: this is a signal, not a value.
    template <>
    class Promise<void> : public detail::PromiseHandle<detail::FutureState<void>> {
        using Base = detail::PromiseHandle<detail::FutureState<void>>;
    public:
        [[nodiscard]] Future<void> GetFuture() const noexcept { return Future<void>(this->s_); }
        std::size_t Set() { return this->s_ ? this->s_->SetValue() : 0; }
    };

} // namespace JLib
