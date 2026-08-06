#pragma once
#include "platform.h"   // must precede memoryapi.h -- it pulls in windows.h, without which
#include <memoryapi.h>  // winnt.h errors out with "No Target Architecture"
#include <atomic>
#include <stdexcept>
class FiberStackArena {
    static constexpr size_t kPageSize = 4096;  // x64 Windows normal page

    void* base;
    size_t totalSize;
    std::atomic<size_t> offset;

public:
    FiberStackArena(size_t capacity) {
        // Allocate one massive chunk of memory
        base = VirtualAlloc(nullptr, capacity, MEM_RESERVE, PAGE_NOACCESS);
        if (!base) throw std::runtime_error("FiberStackArena: reservation failed");
        totalSize = capacity;
        offset = 0;
    }
    ~FiberStackArena() {
        if (base) {
            VirtualFree(base, 0, MEM_RELEASE);
        }
    }
    void* AllocateStack(size_t rawSize) {
        // Round up to a whole number of pages. If a caller ever passes a non-page-multiple
        // size, an unaligned offset makes VirtualAlloc round the commit base DOWN to the page
        // boundary -- the region would then overlap the previous stack, and the guard page
        // below would land on the previous fiber's live stack top. Aligning here makes it
        // impossible for two stacks to share a page.
        const size_t size = (rawSize + kPageSize - 1) & ~(kPageSize - 1);
        if (size <= kPageSize) return nullptr;  // no room left for the guard page

        const size_t current = offset.fetch_add(size, std::memory_order_relaxed);
        // Bump-arena bounds check. This is NOT redundant with the VirtualAlloc failure below:
        // MEM_COMMIT past the end of THIS reservation can still SUCCEED if the address happens
        // to fall inside another reserved region (the standard and heavy arenas sit in the same
        // address space), which would hand out a stack carved from someone else's reserve.
        // Phrased to survive a wrapped offset rather than as current + size > totalSize.
        if (current > totalSize || size > totalSize - current) return nullptr;

        // Guard page: stacks grow DOWNWARD (Fiber.cpp sets the context's rsp to base+size), so
        // the lowest page of this region is where an overflow lands. Leave it RESERVED but
        // uncommitted -- touching it faults immediately, exactly as a committed PAGE_NOACCESS
        // page would, but costs no commit charge and no VirtualProtect syscall. Without it an
        // overflow would silently clobber the NEXT fiber's stack (the regions are carved
        // contiguously from one reservation -- there is nothing else between them). Costs 4KB
        // of usable stack per fiber; zero cost per switch. MSVC's __chkstk touches pages
        // downward one at a time for >4KB frames, so it cannot step over the guard.
        char* region = (char*)base + current;
        if (!VirtualAlloc(region + kPageSize, size - kPageSize, MEM_COMMIT, PAGE_READWRITE))
            return nullptr;

        // Return the region base, NOT the VirtualAlloc result: Fiber::Init computes the stack
        // top as stackBase + stackSize, so it must see the bottom of the whole region.
        return region;
    }
};