// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// SYNCHRONOUS-LOOKING I/O ON A FIBER. THE REPLACEMENT FOR co_await.
//
// The reactor's API is completion-first: you hand Submit a buffer, an IoRequest, an IoResult and a
// RESUME TASK, and it pushes that task when the completion arrives. The C++20 layer wrapped that in
// an awaiter so a coroutine could write `co_await RecvAsync(...)`. With the runtime fibers-only,
// this is the wrapper that does the same job for a fiber -- and it needs no language feature,
// because a fiber already knows how to suspend.
//
//     IoResult r = Await(sched, [&](JLib::Task* k) {
//         return io.SubmitRecv(s, buf, len, 0, &req, &out, k, tok);
//     }, req, out);
//
// HOW IT SUSPENDS: a WaitGroup, not an Event. The completion path already decrements a task's
// waitGroup and wakes its waiters -- that is the ordinary Native completion in Thread.cpp -- so
// giving the resume task a WaitGroup and calling WaitFor on it needs no new machinery and no name.
// An Event would need a distinct name per in-flight operation, which is exactly the wrong shape for
// something you do once per packet.
//
// THE `true` RETURN IS THE TRAP, and it is why this is a function rather than a two-line idiom.
// Submit returns TRUE when the answer is already final -- the operation was cancelled before it
// started, or it completed synchronously. In that case the resume task IS NEVER PUSHED AND NEVER
// FREED: the reactor did not take ownership, so the caller still holds it. Waiting would hang
// forever on a WaitGroup nobody will decrement, and dropping the pointer leaks a Task. Both are easy
// to write and neither fails loudly.
//
// WHAT ACTUALLY PARKS, which matters for sizing the pool: the CALLER's fiber. It is an ordinary
// Standard fiber holding its own stack for the duration of the I/O. StackClass::Tiny is for the
// short-lived RESUME task the reactor pushes, not for the waiter. So "one fiber per socket, loop on
// completions" is one STANDARD fiber parked per socket plus transient tiny ones -- which is the
// model the tiny budget was sized against.
//
// THIS BELONGS IN THE LIBRARY, not in a test. It is here because the port that needed it is a test,
// and putting a public header into the API is a decision worth making deliberately rather than as a
// side effect of restoring coverage.

#pragma once

#include "TaskScheduler.h"
#include "IoReactor.h"

#include <utility>

namespace JLib {
namespace testing {

// ---- THE RESUME TASK IS A RAW fn/data TASK, NOT A LAMBDA -----------------------------------
//
// It used to be `CreateInternalTask([] { }, Lane::Normal)` -- an EMPTY lambda whose only purpose was
// to exist so a WaitGroup could hang off it. That allocated a LambdaTask per I/O operation to hold
// nothing, and taught the wrong idiom in the one file people will copy from.
//
// A raw `void(*)(void*)` needs no capture, no closure type and no per-operation lambda frame. The
// body is genuinely empty: all the work is the WaitGroup decrement the worker performs on
// completion, which is the ordinary Native completion path and not something this function has to
// write.
//
// AND THE GENERAL RULE IT FOLLOWS, which is worth stating because it is easy to get backwards: state
// that must survive a suspension belongs on the FIBER'S STACK -- a local inside the task body -- or
// in storage you own and pass by pointer. Not in a capture. A capture lives in the task frame, which
// is a different object with a different lifetime; the fiber's stack is the thing the whole runtime
// exists to preserve across a wait. Note how every operation below declares its IoRequest and
// IoResult as locals in the CALLER'S fiber rather than capturing them.
inline void NoopResume(void*) noexcept {}

// `submit` is any callable taking the resume Task* and returning Submit's bool.
// Returns the IoResult the operation produced.
template <typename SubmitFn>
inline IoResult Await(TaskScheduler& sched, SubmitFn&& submit, IoResult& out) {
    WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);

    // ---- Lane::Normal, AND LowLatency HERE IS A HANG. MEASURED, NOT FEARED. -------------------
    //
    // The first version used Lane::LowLatency, reasoning that steering the completion to K is what
    // makes the reactor's Tiny stamp reach a bound fiber. That is true of a completion that IS the
    // work -- io_tiny_stack_test does exactly that and passes. It is false for a completion whose
    // job is to WAKE A WAITING FIBER, and the difference deadlocks:
    //
    //   the resume runs on a RESERVED worker -> it decrements the WaitGroup -> the woken fiber is
    //   Lane::Normal work, and it gets published on the worker that woke it -> that worker is in
    //   the reserved band, WHICH NEVER READS A LOPRI DEQUE -> nobody may take it, ever.
    //
    // Join's dump named it exactly: queuedTasks=1, every worker idle, advertised queues = 0, and
    // `q1 ... deque(hi/lo) 0/1` -- one task in a reserved worker's loPri deque. That is the
    // unreachable-inbox shape the band's own invariant exists to prevent, reached from the other
    // side. Nothing was lost or raced; the work was simply placed where its only legal reader is
    // forbidden to look.
    //
    // So the completion goes to the FLOOR, which can read loPri deques and can therefore resume an
    // ordinary fiber. The cost is that this path does not exercise StackClass::Tiny -- a floor
    // Native task is fiberless -- and that is the correct trade: the tiny stack is proven by
    // io_tiny_stack_test, where the completion is the whole work and has no fiber to wake.
    Task* resume = sched.CreateInternalTask(&NoopResume, nullptr, Lane::Normal);
    if (!resume) {
        out = IoResult{ IoStatus::Failed, 0, 0 };
        return out;
    }
    resume->waitGroup = &wg;

    if (submit(resume)) {
        // ALREADY FINAL. The reactor never took the task, so this owns it -- and must not wait,
        // because nothing will ever decrement wg. Destroy it here rather than leaking.
        //
        // The same two steps the worker's completion path takes, in the same order: run the
        // destructor, then release the storage. Free() rather than FreeSized() -- FreeSized reports
        // failure by returning false, and a task whose body did not fit a slot lives on the heap.
        DestroyTask(resume);
        sched.GetAllocator()->Free(resume);
        return out;
    }

    sched.WaitFor(wg);   // suspends THIS fiber until the completion runs the resume task
    return out;
}


// ---- THE TEN OPERATIONS, MIRRORING IoAsync.h's SIGNATURES EXACTLY ---------------------------
//
// Same names minus "Async", same parameters, same defaults. Each abandoned awaitable was itself a
// thin lambda over a Submit*, so these are that lambda with Await() in place of the awaiter -- which
// is why the port of the test body is a rename rather than a rewrite, and why a reviewer can check
// each one against the original in a single glance.
//
// EACH TAKES ITS OWN IoRequest AND IoResult BY REFERENCE, and the caller owns both. That is not
// ceremony: the kernel writes into them after the call returns, so they must outlive the operation.
// The coroutine version could hide this in the frame; a fiber's stack works the same way, but only
// if the objects are declared in the fiber and not in something that returns first.
namespace detail {
    inline IoReactor& R() { return IoReactor::Instance(); }
}

inline IoResult Recv(TaskScheduler& s, IoSocket sk, void* buf, std::uint32_t len,
                     IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                     CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitRecv(sk, buf, len, flags, &req, &out, k, tok); }, out);
}

inline IoResult Send(TaskScheduler& s, IoSocket sk, const void* buf, std::uint32_t len,
                     IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                     CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitSend(sk, buf, len, flags, &req, &out, k, tok); }, out);
}

inline IoResult RecvV(TaskScheduler& s, IoSocket sk, const IoBuffer* bufs, std::uint32_t count,
                      IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                      CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitRecvV(sk, bufs, count, flags, &req, &out, k, tok); }, out);
}

inline IoResult SendV(TaskScheduler& s, IoSocket sk, const IoBuffer* bufs, std::uint32_t count,
                      IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                      CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitSendV(sk, bufs, count, flags, &req, &out, k, tok); }, out);
}

// `accepted` must already exist, unbound, same family and type as the listener; `addrs` must outlive
// the operation. Both are AcceptEx's requirements rather than this library's.
inline IoResult Accept(TaskScheduler& s, IoSocket listener, IoSocket accepted, IoAcceptBuffer* addrs,
                       IoRequest& req, IoResult& out, CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitAccept(listener, accepted, addrs, &req, &out, k, tok); }, out);
}

// `sk` must already be BOUND -- ConnectEx's rule; bind to INADDR_ANY:0 if you have no preference.
inline IoResult Connect(TaskScheduler& s, IoSocket sk, const void* sa, std::uint32_t saLen,
                        IoRequest& req, IoResult& out, CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitConnect(sk, sa, saLen, &req, &out, k, tok); }, out);
}

inline IoResult Disconnect(TaskScheduler& s, IoSocket sk, bool reuse,
                           IoRequest& req, IoResult& out, CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitDisconnect(sk, reuse, &req, &out, k, tok); }, out);
}

// A DATAGRAM IS A MESSAGE: `bytes` is the whole thing, and a buffer too small loses the remainder
// and reports WSAEMSGSIZE rather than returning a partial read. That is UDP, not a wart here.
// `from` receives the sender's address and must outlive the operation.
inline IoResult RecvFrom(TaskScheduler& s, IoSocket sk, void* buf, std::uint32_t len,
                         IoAddress* from, IoRequest& req, IoResult& out,
                         std::uint32_t flags = 0, CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitRecvFrom(sk, buf, len, flags, from, &req, &out, k, tok); }, out);
}

inline IoResult SendTo(TaskScheduler& s, IoSocket sk, const void* buf, std::uint32_t len,
                       const void* addr, std::uint32_t addrLen, IoRequest& req, IoResult& out,
                       std::uint32_t flags = 0, CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return detail::R().SubmitSendTo(sk, buf, len, flags, addr, addrLen, &req, &out, k, tok); }, out);
}

// ---- RUN A BODY ON A FIBER, COUNTED BY A WaitGroup -------------------------------------------
//
// The replacement for `Spawn(coro(...), &wg)`. TaskType::Fiber is not optional here: the body
// SUSPENDS inside Await, and a Native task has no context to switch away from -- it would fail-fast
// with no message rather than wait.
//
// ---- IT TAKES A POINTER TO THE BODY, NOT THE BODY. THAT IS THE WHOLE DESIGN. ----------------
//
// This used to be `F&& body`, forwarded into CreateTask, which copied the closure onto the TASK
// SLAB and asked for TaskType::Fiber. That combination is exactly the thing the runtime cannot
// support, and it is worth being precise about why, because it works in a test every single time.
//
//   THE ROW. AcquireFiber leases a stack plus the FLS slots, creditor list and retire bag hanging
//   off it. The lease has one teardown -- the recycle after FiberStatus::DEAD -- so the invariant
//   is that every acquire reaches DEAD exactly once. A row that does not is a permanent --budget:
//   the stack never returns to the magazine and the reaper never runs for it.
//
//   TWO OWNERS. A slab-allocated closure is owned by the worker loop, which frees the task frame
//   as soon as the body returns. The fiber is owned by whoever resumes it. Those are different
//   parties with no shared destructor. While the body merely runs, nothing is wrong. The moment it
//   SUSPENDS, its continuation is a promise made by something entitled to have gone away.
//
//   AND THE FAILURE IS INVISIBLE. SlabPool is append-only and releases no extent before the pool
//   itself dies, so a freed slot stays mapped and usually still holds its bytes. The suspend/resume
//   round trip reads its capture back intact and the test passes -- while the bill accrues as a
//   live count that never returns to baseline, a slab that cannot be cleared, or a completion
//   writing into a stack the magazine has since reissued.
//
// SO THE CLOSURE LIVES ON THE CALLER'S STACK and the task carries a function pointer plus its
// address. One owner. The frame the runtime allocates holds no captures at all, so freeing it when
// the body returns is correct rather than merely survivable, and the row reaches DEAD on the
// ordinary path.
//
//     auto body = [&sched, sock] { ...Await(sched, ...)... };   // caller's frame owns this
//     Fib::SpawnFiber(sched, wg, &body);
//     sched.WaitFor(wg);                                        // ...and outlives the wait
//
// THE ONE RULE AT THE CALL SITE: the caller must not leave the scope holding `body` until the
// WaitGroup has completed. Every use here is the shape above -- spawn, then WaitFor in the same
// function -- which satisfies it structurally rather than by discipline. A `body` declared inside
// a loop must have the WaitFor inside that loop too.
//
// `tok` STAMPS THE TASK'S CANCELLATION SCOPE, which the coroutine version passed through Spawn's
// trailing parameters. It has to be on the TASK and not only on the operation: cancelling a scope
// must reach work that has already been dispatched and is running, and the token on the submit only
// governs the I/O.
inline void SpawnFiber(TaskScheduler& s, WaitGroup& wg, void (*body)(void*), void* ctx,
                       CancelToken tok = CancelToken{}) {
    wg.n.fetch_add(1, std::memory_order_relaxed);
    Task* t = s.CreateTask(body, ctx, Lane::Normal, TaskType::Fiber);
    if (!t) { wg.n.fetch_sub(1, std::memory_order_acq_rel); return; }
    if (tok.Raw() != CancelToken::kNone) t->cancelToken = tok.Raw();
    t->waitGroup = &wg;
    if (!s.Push(t)) { wg.n.fetch_sub(1, std::memory_order_acq_rel); DestroyTask(t); s.GetAllocator()->Free(t); }
}

// ---- THE ACCEPTOR POOL, whose output is a SOCKET rather than an IoResult --------------------
//
// IoAcceptor::TakeOrQueue has the same polarity as Submit -- TRUE means "you already have one, do
// not suspend" -- but it writes an IoSocket, not an IoResult. So it needs the WAIT without the
// result plumbing, which is what AwaitVoid is: the same WaitGroup handshake and the same ownership
// rule for the `true` case, with nothing said about what the operation produced.
//
// The waiter must OUTLIVE THE WAIT and belongs to the caller. In the coroutine version it was a
// member of the awaiter, living in the frame; here it is a local of the fiber, which is the same
// storage for the same reason.
template <typename SubmitFn>
inline void AwaitVoid(TaskScheduler& sched, SubmitFn&& submit) {
    WaitGroup wg;
    wg.n.store(1, std::memory_order_relaxed);

    Task* resume = sched.CreateInternalTask(&NoopResume, nullptr, Lane::Normal);   // FLOOR -- see Await
    if (!resume) return;
    resume->waitGroup = &wg;

    if (submit(resume)) {   // already final -- the callee never took the task, so this owns it
        DestroyTask(resume);
        sched.GetAllocator()->Free(resume);
        return;
    }
    sched.WaitFor(wg);
}

inline IoSocket Accept(TaskScheduler& s, IoAcceptor& acc, IoAcceptWaiter& w,
                       CancelToken tok = CancelToken{}) {
    IoSocket sock = 0;
    AwaitVoid(s, [&](Task* k) { return acc.TakeOrQueue(&w, &sock, k, tok); });
    return sock;
}

// ---- IoStream: the SERIALISED stream surface ------------------------------------------------
//
// IoStream exists so two sends on one socket cannot interleave on the wire. Its Submit* are members
// that queue behind each other, so these wrappers differ from the raw ones ONLY in which object the
// submit is called on -- which is exactly the distinction the "Raw" naming was introduced to make.
inline IoResult Send(TaskScheduler& s, IoStream& st, const void* buf, std::uint32_t len,
                     IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                     CancelToken tok = CancelToken{}) {
    const IoBuffer one{ const_cast<void*>(buf), len };
    return Await(s, [&](Task* k) { return st.SubmitSend(&one, 1, flags, &req, &out, k, tok); }, out);
}

inline IoResult SendV(TaskScheduler& s, IoStream& st, const IoBuffer* bufs, std::uint32_t count,
                      IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                      CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return st.SubmitSend(bufs, count, flags, &req, &out, k, tok); }, out);
}

inline IoResult Recv(TaskScheduler& s, IoStream& st, void* buf, std::uint32_t len,
                     IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                     CancelToken tok = CancelToken{}) {
    const IoBuffer one{ buf, len };
    return Await(s, [&](Task* k) { return st.SubmitRecv(&one, 1, flags, &req, &out, k, tok); }, out);
}

inline IoResult RecvV(TaskScheduler& s, IoStream& st, const IoBuffer* bufs, std::uint32_t count,
                      IoRequest& req, IoResult& out, std::uint32_t flags = 0,
                      CancelToken tok = CancelToken{}) {
    return Await(s, [&](Task* k) { return st.SubmitRecv(bufs, count, flags, &req, &out, k, tok); }, out);
}

} // namespace testing
} // namespace JLib
