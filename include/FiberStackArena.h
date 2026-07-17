#pragma once
#include <memoryapi.h>
#include <atomic>
#include "platform.h"
class FiberStackArena {
    void* base;
    size_t totalSize;
    std::atomic<size_t> offset;

public:
    FiberStackArena(size_t capacity) {
        // Allocate one massive chunk of memory
        base = VirtualAlloc(nullptr, capacity, MEM_RESERVE, PAGE_NOACCESS);
        totalSize = capacity;
        offset = 0;
    }
    ~FiberStackArena() {
        if (base) {
            VirtualFree(base, 0, MEM_RELEASE);
        }
    }
    void* AllocateStack(size_t size) {
        size_t current = offset.fetch_add(size);
        // Commit only the memory we actually need right now
        void* stack = VirtualAlloc((char*)base + current, size, MEM_COMMIT, PAGE_READWRITE);
        if (!stack) return nullptr;
        // Guard page: stacks grow DOWNWARD (Fiber.cpp sets the context's rsp to base+size), so
        // the lowest page of this region is where an overflow lands. PAGE_NOACCESS turns that
        // overflow into an immediate access violation at the faulting instruction, instead of
        // silently clobbering the NEXT fiber's stack (the regions are carved contiguously from
        // one reservation -- there is nothing else between them). Costs 4KB of usable stack per
        // fiber and one syscall here at pool init; zero cost per switch. MSVC's __chkstk touches
        // pages downward one at a time for >4KB frames, so it cannot step over the guard.
        DWORD oldProt;
        VirtualProtect(stack, 4096, PAGE_NOACCESS, &oldProt);
        return stack;
    }
};