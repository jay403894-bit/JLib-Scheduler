// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// See IoUring.h for why this is raw syscalls rather than liburing.

#include "IoUring.h"

#if defined(__linux__)

#include <cerrno>
#include <csignal>          // sigset_t, and _NSIG via <signal.h>
#include <cstring>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

// io_uring_enter's last argument is the SIGSET SIZE IN BYTES, and the kernel checks it exactly.
// _NSIG is glibc-internal and not guaranteed visible from <csignal> alone, so derive the size from
// the type instead of naming a macro that may not exist -- sizeof(sigset_t) is what the kernel is
// actually being told about, and it cannot drift from the type being passed.
#ifndef JLIB_SIGSET_BYTES
  #define JLIB_SIGSET_BYTES (sizeof(sigset_t))
#endif

namespace JLib { namespace uring {
namespace {

// NO GLIBC WRAPPERS EXIST for these on the versions this targets, so they are raw syscalls. That is
// not a workaround -- io_uring's three entry points have never been wrapped by glibc, and liburing
// makes exactly these calls.
int sys_io_uring_setup(unsigned entries, io_uring_params* p) noexcept {
    return (int)syscall(__NR_io_uring_setup, entries, p);
}
int sys_io_uring_enter(int fd, unsigned toSubmit, unsigned minComplete,
                       unsigned flags, sigset_t* sig) noexcept {
    return (int)syscall(__NR_io_uring_enter, fd, toSubmit, minComplete, flags, sig,
                        (std::size_t)JLIB_SIGSET_BYTES);
}

// The kernel's words are plain `unsigned*` in the uapi, but they are shared with another agent, so
// every access needs explicit ordering. Casting to a std::atomic reference is the least-lying way to
// say that in C++ -- the alternative is inline asm or volatile, and volatile does not order anything.
inline std::atomic<unsigned>& Atomic(unsigned* p) noexcept {
    return *reinterpret_cast<std::atomic<unsigned>*>(p);
}

InitResult Classify(int err) noexcept {
    switch (err) {
        case ENOSYS: return InitResult::Unsupported;
        case EPERM:
        case EACCES: return InitResult::Denied;
        default:     return InitResult::Failed;
    }
}

} // namespace

InitResult Init(Ring& out, unsigned entries) noexcept {
    Ring r;
    io_uring_params p{};

    const int fd = sys_io_uring_setup(entries, &p);
    if (fd < 0) return Classify(-fd == 0 ? errno : -fd);

    r.fd       = fd;
    r.entries  = p.sq_entries;
    r.features = p.features;

    // SIZES COME FROM THE KERNEL, never computed from `entries`. The ring is sized by the kernel
    // (rounded to a power of two, possibly clamped), and the array offsets are negotiated per ring.
    r.sqRingSz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    r.cqRingSz = p.cq_off.cqes  + p.cq_entries * sizeof(io_uring_cqe);

    // SINGLE MMAP when the kernel offers it (5.4+). Both rings then live in one mapping and the
    // larger size covers both -- unmapping twice would be an error, so cqRing aliases sqRing and
    // Shutdown checks for that.
    const bool single = (p.features & IORING_FEAT_SINGLE_MMAP) != 0;
    if (single) {
        if (r.cqRingSz > r.sqRingSz) r.sqRingSz = r.cqRingSz;
        r.cqRingSz = r.sqRingSz;
    }

    r.sqRing = mmap(nullptr, r.sqRingSz, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);
    if (r.sqRing == MAP_FAILED) { r.sqRing = nullptr; Shutdown(r); return InitResult::Failed; }

    if (single) {
        r.cqRing = r.sqRing;
    } else {
        r.cqRing = mmap(nullptr, r.cqRingSz, PROT_READ | PROT_WRITE,
                        MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
        if (r.cqRing == MAP_FAILED) { r.cqRing = nullptr; Shutdown(r); return InitResult::Failed; }
    }

    r.sqesSz = p.sq_entries * sizeof(io_uring_sqe);
    r.sqes = (io_uring_sqe*)mmap(nullptr, r.sqesSz, PROT_READ | PROT_WRITE,
                                 MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES);
    if (r.sqes == MAP_FAILED) { r.sqes = nullptr; Shutdown(r); return InitResult::Failed; }

    auto* sq = (unsigned char*)r.sqRing;
    auto* cq = (unsigned char*)r.cqRing;
    r.sqHead  = (unsigned*)(sq + p.sq_off.head);
    r.sqTail  = (unsigned*)(sq + p.sq_off.tail);
    r.sqMask  = (unsigned*)(sq + p.sq_off.ring_mask);
    r.sqArray = (unsigned*)(sq + p.sq_off.array);
    r.cqHead  = (unsigned*)(cq + p.cq_off.head);
    r.cqTail  = (unsigned*)(cq + p.cq_off.tail);
    r.cqMask  = (unsigned*)(cq + p.cq_off.ring_mask);
    r.cqes    = (io_uring_cqe*)(cq + p.cq_off.cqes);

    // THE INDIRECTION ARRAY IS FILLED ONCE. sqArray[i] = i makes the submission array an identity
    // map, so publishing an SQE is only a tail bump. The array exists so an application can submit
    // SQEs out of order; nothing here wants that, and pretending otherwise would mean maintaining a
    // second index for no benefit.
    for (unsigned i = 0; i < p.sq_entries; ++i) r.sqArray[i] = i;

    out = r;
    return InitResult::Ok;
}

void Shutdown(Ring& r) noexcept {
    if (r.sqes)   { munmap(r.sqes, r.sqesSz); r.sqes = nullptr; }
    // cqRing aliases sqRing under IORING_FEAT_SINGLE_MMAP; unmapping the same range twice would
    // fail, and on a future kernel where the second call happened to succeed against a NEW mapping
    // at the same address it would be far worse than an error.
    if (r.cqRing && r.cqRing != r.sqRing) { munmap(r.cqRing, r.cqRingSz); }
    r.cqRing = nullptr;
    if (r.sqRing) { munmap(r.sqRing, r.sqRingSz); r.sqRing = nullptr; }
    if (r.fd >= 0) { close(r.fd); r.fd = -1; }
    r.sqHead = r.sqTail = r.sqMask = r.sqArray = nullptr;
    r.cqHead = r.cqTail = r.cqMask = nullptr;
    r.cqes = nullptr;
    r.entries = 0;
}

InitResult Probe() noexcept {
    Ring r;
    const InitResult res = Init(r, 2);      // smallest ring the kernel will round up from
    if (res == InitResult::Ok) Shutdown(r);
    return res;
}

io_uring_sqe* GetSqe(Ring& r) noexcept {
    // Head is written by the KERNEL as it consumes entries, so it is an acquire load. Tail is ours
    // and can be read relaxed -- no other agent writes it.
    const unsigned tail = Atomic(r.sqTail).load(std::memory_order_relaxed);
    const unsigned head = Atomic(r.sqHead).load(std::memory_order_acquire);
    if ((tail - head) >= r.entries) return nullptr;      // full; caller must Submit and retry
    return &r.sqes[tail & *r.sqMask];
}

int Submit(Ring& r, unsigned waitFor) noexcept {
    // ONE ENTRY PER CALL, which is what GetSqe's contract implies: reserve, fill, submit. Batching
    // several SQEs before a single Submit is a later optimisation and would change this to track how
    // many were reserved -- deliberately not done yet, because the reactor above has no batching
    // call site and an unused generalisation is a place for a bug to hide.
    const unsigned tail = Atomic(r.sqTail).load(std::memory_order_relaxed);

    // RELEASE, and it is the important one in this file. The kernel must not be able to observe the
    // new tail before the SQE contents it points at. Get this wrong and the kernel reads a
    // half-written or stale SQE -- a data race with the other side of the ring, invisible to every
    // sanitizer we have.
    Atomic(r.sqTail).store(tail + 1, std::memory_order_release);

    unsigned flags = 0;
    if (waitFor > 0) flags |= IORING_ENTER_GETEVENTS;
    const int n = sys_io_uring_enter(r.fd, 1, waitFor, flags, nullptr);
    return (n < 0) ? -errno : n;
}

unsigned Reap(Ring& r, io_uring_cqe* out, unsigned max) noexcept {
    const unsigned head = Atomic(r.cqHead).load(std::memory_order_relaxed);
    // ACQUIRE, the mirror of Submit's release: the tail is published by the kernel AFTER it writes
    // the CQE, so reading it with acquire is what makes the CQE contents visible to us.
    const unsigned tail = Atomic(r.cqTail).load(std::memory_order_acquire);

    unsigned n = tail - head;
    if (n > max) n = max;
    for (unsigned i = 0; i < n; ++i)
        out[i] = r.cqes[(head + i) & *r.cqMask];

    // RELEASE on the way out: the kernel may reuse these slots the instant it sees the new head, so
    // the copies above must be complete first.
    if (n) Atomic(r.cqHead).store(head + n, std::memory_order_release);
    return n;
}

bool PostWake(Ring& r, std::uint64_t sentinel) noexcept {
    io_uring_sqe* sqe = GetSqe(r);
    if (!sqe) return false;
    std::memset(sqe, 0, sizeof(*sqe));
    sqe->opcode    = IORING_OP_NOP;
    sqe->user_data = sentinel;
    return Submit(r, 0) >= 0;
}

}} // namespace JLib::uring

#endif // __linux__
