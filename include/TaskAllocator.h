// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <vector>
#include <cstddef>
#include <mutex>
#include <atomic>
#ifdef _DEBUG
#include <Windows.h> // OutputDebugStringA/__debugbreak for the free-list canary check below
#endif
namespace JLib {
    class TaskAllocator {
    public:
        static constexpr size_t SLOT = 256;   // bytes per slot (>= your biggest task)
        static constexpr size_t BATCH = 32;    // refill/flush granularity
    private:
        struct alignas(16) Block { std::byte b[SLOT]; };

        // a Free slot's first 8 bytes ARE the "next Free" link (the intrusive trick)
        static void*& next(void* slot) { return *reinterpret_cast<void**>(slot); }

        // ---- shared backing (touched rarely, in batches, under the lock) ----
        std::vector<Block> mem;
        void* sharedHead = nullptr;
        std::mutex mtx;
        std::atomic<long long> liveCount{ 0 }; // see LiveCount() below

        // ---- per-thread cache (the lock-Free hot path) ----
        struct Cache { void* head = nullptr; size_t count = 0; };
        static Cache& local() {
            static thread_local Cache c;       // ONE per thread (see caveat below)
            return c;
        }

    public:
        explicit TaskAllocator(size_t slots) : mem(slots) {
            for (auto& blk : mem) {
                next(&blk) = sharedHead; sharedHead = &blk;
#ifdef _DEBUG
                // Every slot starts life "free" -- stamp the canary here too (not just in
                // Free()), or the FIRST-ever Alloc() of a never-before-freed slot reads
                // whatever value(std::byte)-initialization left at bytes[8,16) (zero), which
                // doesn't match the canary and looks exactly like corruption on startup.
                *reinterpret_cast<uint64_t*>(reinterpret_cast<std::byte*>(&blk) + 8) = 0xFEEDFACECAFEBEEFULL;
#endif
            }
        }

#ifdef _DEBUG
        // Bytes [8,16) of a slot hold the intrusive "next free" link's tail + are otherwise
        // unused while free (the link itself only needs the first 8 bytes). Free() stamps a
        // canary there; Alloc() verifies it's UNCHANGED before handing the slot back out. If
        // something wrote through this slot AFTER it was freed (use-after-free) or if the same
        // slot got Free()'d twice (corrupting the free-list into aliasing two live owners), the
        // canary will have been clobbered by whatever real data got written into the
        // still-technically-free slot -- this catches it at the NEXT Alloc() of that exact slot,
        // right at the point of detection, instead of silently corrupting the free-list chain
        // until the whole pool eventually appears "exhausted" (Alloc() returning nullptr) far
        // downstream of the actual bug.
        static constexpr uint64_t kFreeCanary = 0xFEEDFACECAFEBEEFULL;
        static void StampCanary(void* slot) {
            *reinterpret_cast<uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8) = kFreeCanary;
        }
        static void CheckCanary(void* slot) {
            uint64_t v = *reinterpret_cast<uint64_t*>(reinterpret_cast<std::byte*>(slot) + 8);
            if (v != kFreeCanary) {
                OutputDebugStringA("TaskAllocator: corrupted freed slot detected "
                    "(use-after-free or double-free) -- breaking at the Alloc() that noticed.\n");
                __debugbreak();
            }
        }
#endif

        void* Alloc() {                        // lock-Free unless the cache is empty
            Cache& c = local();
            if (!c.head) refill(c);
            if (!c.head) return nullptr;       // backing fully exhausted
            void* slot = c.head;
#ifdef _DEBUG
            CheckCanary(slot);
#endif
            c.head = next(slot);
            c.count--;
            liveCount.fetch_add(1, std::memory_order_relaxed);
            return slot;
        }

        void Free(void* slot) {                // lock-Free unless the cache overflows
            Cache& c = local();
            next(slot) = c.head;
#ifdef _DEBUG
            StampCanary(slot);
#endif
            c.head = slot;
            c.count++;
            liveCount.fetch_sub(1, std::memory_order_relaxed);
            if (c.count > 2 * BATCH) flush(c);
        }

        // Diagnostic only: how many slots are currently checked out (Alloc'd but not yet
        // Free'd), across every thread's cache + the shared pool. Not synchronized with
        // Alloc/Free beyond the atomic itself -- a momentary snapshot, good enough to watch
        // whether usage climbs monotonically (a real leak) or oscillates near a steady state
        // (normal churn) while chasing an exhaustion bug.
        long long LiveCount() const { return liveCount.load(std::memory_order_relaxed); }
        size_t Capacity() const { return mem.size(); }

    private:
        // Move up to BATCH slots shared -> local by SPLICING a sub-chain instead of relinking
        // node-by-node. The walk MUST stay under the lock (sharedHead is shared, other threads
        // mutate it), so refill's critical section is still O(BATCH) pointer-CHASES -- but the
        // per-node WRITES are gone: we detach the whole sub-chain in one store (sharedHead = curr)
        // and do the local attach (2 writes) after the lock drops. Free-list order is irrelevant
        // (slots are interchangeable), so keeping the sub-chain's original order is fine.
        void refill(Cache& c) {
            void* batchHead;
            void* batchTail = nullptr;
            size_t moved = 0;
            {
                std::lock_guard<std::mutex> lk(mtx);
                batchHead = sharedHead;
                if (!batchHead) return;             // pool exhausted
                void* curr = batchHead;
                while (curr && moved < BATCH) {
                    batchTail = curr;
                    curr = next(curr);
                    ++moved;
                }
                sharedHead = curr;                  // detach [batchHead .. batchTail] in ONE store
            }
            // Thread-local from here -- batchTail's chain is ours alone now.
            next(batchTail) = c.head;
            c.head = batchHead;
            c.count += moved;
        }

        // Move the excess (down to BATCH) local -> shared. This one is the big win: the walk is over
        // the THREAD-LOCAL cache, which needs no lock, so we peel the excess sub-chain off entirely
        // outside the lock and the critical section collapses to just the 2-write splice.
        void flush(Cache& c) {
            if (c.count <= BATCH) return;
            size_t toMove = c.count - BATCH;
            void* batchHead = c.head;               // peel the top `toMove` nodes (all thread-local)
            void* batchTail = batchHead;
            for (size_t i = 1; i < toMove; ++i)
                batchTail = next(batchTail);
            c.head = next(batchTail);               // local cache keeps the remaining BATCH nodes
            c.count = BATCH;
            {
                std::lock_guard<std::mutex> lk(mtx);
                next(batchTail) = sharedHead;       // splice the whole sub-chain onto shared in
                sharedHead = batchHead;             // two writes -- the entire critical section
            }
        }
    };
};