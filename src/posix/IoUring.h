// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// RAW io_uring. No liburing.
//
// WHY NO liburing. This library vendors exactly one third-party header (concurrentqueue) and asks
// its consumers for nothing else. Adding liburing would make a system package a hard requirement of
// the Linux build -- for three syscalls and two mmaps. It also would not buy portability: the
// distro-shipped liburing version varies more than the kernel uapi does, and the uapi header
// (linux/io_uring.h) is the stable contract either way.
//
// It also makes the FALLBACK fall out for free. io_uring_setup returning -ENOSYS (kernel too old,
// io_uring compiled out) or -EPERM (a seccomp filter or a container policy refusing it -- common on
// Android and in hardened containers) is the probe. liburing would report the same thing behind an
// abstraction that then has to be un-abstracted to decide anything.
//
// WHAT THIS FILE IS NOT. It is the ring, and only the ring: setup, teardown, get an SQE, submit,
// drain CQEs. It knows nothing about IoRequest, tasks, or the scheduler. That separation is
// deliberate -- the ring can be exercised on its own (see the self-test at the bottom of the .cpp),
// so a failure here is distinguishable from a failure in the reactor above it, which is exactly the
// property that was missing when the steal work went wrong today.
//
// == THE MEMORY MODEL, because this is a lock-free ring shared with the KERNEL ==
//
// The kernel is the other thread. Submission: fill an SQE, then publish the tail with a RELEASE
// store -- the kernel must not observe the tail before the SQE contents. Completion: read the CQ
// tail with an ACQUIRE load, then read the CQE -- symmetric. Getting either backwards is a data
// race with the kernel and cannot be caught by any test in this repo, which is why both are spelled
// out at their call sites rather than left to a memory-model argument.

#pragma once

#if defined(__linux__)

#include <atomic>
#include <cstdint>
#include <linux/io_uring.h>

namespace JLib { namespace uring {

// One mapped ring. Trivially destructible on purpose: Shutdown is explicit, because tearing a ring
// down while the kernel still owns a buffer is the corruption the whole reactor exists to prevent,
// and a destructor makes that decision invisible at the call site.
struct Ring {
    int fd = -1;

    // The SQ and CQ rings are ONE mapping on every kernel this targets (IORING_FEAT_SINGLE_MMAP,
    // 5.4+), but the offsets are still read from the kernel rather than assumed -- they are
    // negotiated per ring, not part of the ABI.
    void*    sqRing     = nullptr;
    std::size_t sqRingSz = 0;
    void*    cqRing     = nullptr;      // == sqRing when single-mmap; only unmapped once
    std::size_t cqRingSz = 0;
    io_uring_sqe* sqes  = nullptr;
    std::size_t sqesSz  = 0;

    // Pointers INTO the mapping. The kernel owns these words; we read and write them with explicit
    // ordering. Not copies -- a copy would be a different variable than the one the kernel updates.
    unsigned* sqHead    = nullptr;
    unsigned* sqTail    = nullptr;
    unsigned* sqMask    = nullptr;
    unsigned* sqArray   = nullptr;

    unsigned* cqHead    = nullptr;
    unsigned* cqTail    = nullptr;
    unsigned* cqMask    = nullptr;
    io_uring_cqe* cqes  = nullptr;

    unsigned  entries   = 0;
    unsigned  features  = 0;
};

// Why a ring could not be created. Kept distinct from "it failed" because the CALLER's response
// differs: Unsupported and Denied mean fall back to epoll and say so once; Failed means something
// unexpected and is worth surfacing.
enum class InitResult : std::uint8_t {
    Ok,
    Unsupported,   // -ENOSYS: kernel too old, or io_uring not built in
    Denied,        // -EPERM / -EACCES: seccomp, container policy, or io_uring_disabled sysctl
    Failed,        // anything else -- mmap failure, ENOMEM, an unexpected errno
};

// entries is rounded UP to a power of two by the kernel. Returns the ring in `out` only on Ok.
InitResult Init(Ring& out, unsigned entries) noexcept;
void       Shutdown(Ring& r) noexcept;

// PROBE WITHOUT COMMITTING. Creates the smallest possible ring and tears it down, so the caller can
// decide a backend before building anything. Cheap enough to do once at Init and far more honest
// than a kernel-version check: a version string does not know about seccomp.
InitResult Probe() noexcept;

// Reserve one SQE. Returns nullptr when the submission queue is full -- the caller must submit and
// retry, never spin, since only the kernel drains it. The returned SQE is UNINITIALISED memory from
// a previous operation; callers zero what they use.
io_uring_sqe* GetSqe(Ring& r) noexcept;

// Publish everything reserved since the last Submit and optionally wait. Returns the number the
// kernel consumed, or -errno.
//
// waitFor > 0 blocks in the kernel until that many completions exist, which is what makes a
// dedicated completion thread cost nothing while idle -- the same property IOCP's
// GetQueuedCompletionStatusEx provides on the Windows side.
int Submit(Ring& r, unsigned waitFor) noexcept;

// BLOCK FOR COMPLETIONS WITHOUT SUBMITTING ANYTHING. Submit() publishes one reserved SQE, which a
// completion thread with nothing to send must not do. This is the analogue of a blocking
// GetQueuedCompletionStatus: enter the kernel, sleep until `minComplete` CQEs exist, return.
// Returns 0 on success or -errno; -EINTR is normal and means "woken, look again".
int WaitCq(Ring& r, unsigned minComplete) noexcept;

// THE SUBMISSION QUEUE HAS EXACTLY ONE LEGAL PRODUCER, which is the difference from IOCP that costs
// the POSIX reactor a lock IOCP never needed. WSARecv is thread-safe against itself; the io_uring SQ
// tail is a plain shared word, so two workers reserving at once would hand the kernel a torn or
// duplicated entry. The reactor therefore serialises submission -- see the note at the submit mutex
// in IoReactor.cpp -- and these two are the pieces it serialises.
//
// Kept as separate calls rather than one locked helper so the reactor can hold its lock across
// "reserve, fill, publish" without this file knowing what a reactor is.

// Drain up to `max` completions into `out`. Returns how many were written. Advances the CQ head, so
// the entries are consumed; copy anything needed out of them before the next call.
unsigned Reap(Ring& r, io_uring_cqe* out, unsigned max) noexcept;

// Wake a completion thread parked in Submit(waitFor>0) without a real completion. Posts a NOP whose
// user_data is `sentinel`, which the drain loop recognises and discards -- the direct analogue of
// posting a null OVERLAPPED to an IOCP, and the mechanism Stop() needs to retire its threads.
bool PostWake(Ring& r, std::uint64_t sentinel) noexcept;

}} // namespace JLib::uring

#endif // __linux__
