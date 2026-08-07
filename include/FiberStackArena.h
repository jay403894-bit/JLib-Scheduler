#pragma once
#include "platform.h"   // JLIB_PLATFORM_*, and the reserve/commit/release primitives below
#include <atomic>
#include <stdexcept>

// One arena, both platforms. Only the three virtual-memory calls differ (VirtualAlloc/VirtualFree
// vs mmap/mprotect/munmap) and those live behind JLib::platform in platform.h -- the alignment
// rule, the bounds check and the guard-page reasoning are identical everywhere and exist once.
class FiberStackArena {
    size_t pageSize;
    void* base;
    size_t totalSize;
    std::atomic<size_t> offset;

public:
    FiberStackArena(size_t capacity)
        : pageSize(JLib::platform::PageSize()) {
        // One large reservation with no backing; stacks are committed out of it as they are handed
        // out. Reserving up front is what makes the regions CONTIGUOUS, which is what makes the
        // guard-page argument below hold.
        base = JLib::platform::ReserveNoAccess(capacity);
        if (!base) throw std::runtime_error("FiberStackArena: reservation failed");
        totalSize = capacity;
        offset = 0;
    }
    ~FiberStackArena() {
        if (base) {
            JLib::platform::ReleaseReservation(base, totalSize);
        }
    }
    void* AllocateStack(size_t rawSize) {
        // Round up to a whole number of pages. If a caller ever passes a non-page-multiple size,
        // an unaligned offset makes the commit base round DOWN to the page boundary -- the region
        // would then overlap the previous stack, and the guard page below would land on the
        // previous fiber's live stack top. Aligning here makes it impossible for two stacks to
        // share a page. (VirtualAlloc and mprotect both round; the hazard is identical on Linux.)
        const size_t size = (rawSize + pageSize - 1) & ~(pageSize - 1);
        if (size <= pageSize) return nullptr;  // no room left for the guard page

        const size_t current = offset.fetch_add(size, std::memory_order_relaxed);
        // Bump-arena bounds check. This is NOT redundant with the commit failing below: a commit
        // past the end of THIS reservation can still SUCCEED if the address happens to fall inside
        // another reserved region (the standard and heavy arenas sit in the same address space),
        // which would hand out a stack carved from someone else's reserve. On Linux the same shape
        // of bug exists -- mprotect on an address belonging to an unrelated mapping succeeds.
        // Phrased to survive a wrapped offset rather than as current + size > totalSize.
        if (current > totalSize || size > totalSize - current) return nullptr;

        // Guard page: stacks grow DOWNWARD (Fiber::Init sets the context's rsp to base+size), so
        // the lowest page of this region is where an overflow lands. Leave it unbacked -- touching
        // it faults immediately, while costing no commit charge and no extra syscall. Without it
        // an overflow would silently clobber the NEXT fiber's stack (the regions are carved
        // contiguously from one reservation -- there is nothing else between them). Costs one page
        // of usable stack per fiber; zero cost per switch. MSVC's __chkstk touches pages downward
        // one at a time for >4KB frames, so it cannot step over the guard; gcc/clang's
        // -fstack-clash-protection does the same on Linux.
        char* region = (char*)base + current;
        if (!JLib::platform::CommitReadWrite(region + pageSize, size - pageSize))
            return nullptr;

        // Return the region base, NOT the committed address: Fiber::Init computes the stack top as
        // stackBase + stackSize, so it must see the bottom of the whole region.
        return region;
    }
};
