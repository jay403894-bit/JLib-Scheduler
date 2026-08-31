// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

//
// STATUS AS OF 2.15.0: NO PRODUCTION CALLER. Written for Event's waiter index and then replaced
// there within the day -- not because bucketing was wrong, but because the keys turned out not to
// be arbitrary (see LockFreeList.h's status note). Kept as a general-purpose container, and tested
// in tests/primitives_test.cpp.
//
// A LIKELY FUTURE CONSUMER is an index whose keys really are arbitrary and concurrent -- an I/O
// completion or cancellation table keyed by a handle. If that arrives, revisit the shape rather
// than assuming this one: chained buckets were chosen for a delete-heavy workload where open
// addressing would only tombstone, and a different consumer may not have that constraint.
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include "LockFreeList.h"

namespace JLib {

    // Fixed-bucket hash map, each bucket an ordinary LockFreeList. There is no map-level
    // synchronisation AT ALL: a key belongs to exactly one bucket, every operation is that bucket's
    // operation, and the bucket already takes its own EpochGuard. So this inherits the list's
    // lock-freedom and its reclamation for free, and adds nothing that could deadlock or block.
    //
    // DO NOT ADD A MAP-LEVEL OR BUCKET-LEVEL EpochGuard around the per-bucket ones, which is the
    // obvious-looking optimisation for for_each below (one guard instead of B). EpochGuard DOES NOT
    // NEST: its destructor stores SIZE_MAX unconditionally rather than restoring the previous
    // value, so the inner guard's exit un-announces a traversal the outer guard still has running,
    // and reclamation is then free to free nodes out from under it. Epochs.h says so at the
    // destructor. The guards here are strictly sequential -- one bucket finishes before the next
    // begins -- which is the only safe shape.
    //
    // WHAT BUCKETING BUYS. On a single LockFreeList every keyed operation is a Window::find walk,
    // so an index of N entries costs O(N) per insert and per remove -- filling or draining one is
    // O(N^2). Bucketing divides both by the bucket count. It does NOT make full iteration cheaper;
    // for_each visits every entry by definition.
    //
    // NO RESIZE, deliberately. Lock-free resize is the genuinely hard part of a lock-free hash map
    // (split-ordered lists, recursive bucket initialisation). This is built for indexes with a
    // KNOWN BOUND on live entries -- size the table against that bound via SuggestBuckets and a
    // fixed table has a known worst case and needs none of that machinery. If your key count is
    // genuinely unbounded, this is the wrong container.
    //
    // BUCKETS ARE LAZY. Each LockFreeList takes two sentinel slab slots in its constructor, so an
    // eager table costs 2B slots whether or not anything is ever put in it -- and a program holding
    // thousands of mostly-empty maps would spend the whole slab on them. Lazy construction makes an
    // untouched bucket cost 8 bytes and no slab slot, which is what lets the bucket count be sized
    // for the worst case instead of the common one.
    //
    // MIX YOUR KEYS -- the map does it for you, and this note is about why it must. Pointer keys
    // from a pool of fixed-size slots have their low bits ALWAYS ZERO and consecutive entries a
    // stride apart. A plain key & mask would send every one of them to the same bucket and quietly
    // rebuild the single list this class exists to replace, while every behavioural test still
    // passed. Guarded by a negative-controlled test in tests/primitives_test.cpp.
    template <typename T>
    class LockFreeHashMap {
        TaskAllocator& allocator;
        // One atomic pointer per bucket, published on first insert. Null means "no bucket yet",
        // which reads as empty -- lookups and removes on a null bucket are misses, not errors.
        std::unique_ptr<std::atomic<LockFreeList<T>*>[]> buckets;
        size_t bucketCount = 0;
        size_t mask = 0;

        // splitmix64's finalizer. Cheap (three shifts, two multiplies, no table) and it moves the
        // high bits of a pointer down into the low bits the mask actually reads, which is the whole
        // job -- see the POINTER KEYS note above.
        static uint64_t Mix(uint64_t k) {
            k ^= k >> 30; k *= 0xbf58476d1ce4e5b9ULL;
            k ^= k >> 27; k *= 0x94d049bb133111ebULL;
            k ^= k >> 31;
            return k;
        }

        // Returns the bucket, creating it when asked. The loser of a creation race destroys its own
        // list (returning both sentinels) and uses the winner's -- a one-time-per-bucket cost that
        // only a genuine tie pays. A list that could not get its sentinels is dropped and NEVER
        // published, so a dry slab degrades to "no bucket" rather than to a broken one.
        LockFreeList<T>* BucketFor(uint64_t key, bool create) {
            std::atomic<LockFreeList<T>*>& slot = buckets[Mix(key) & mask];
            LockFreeList<T>* b = slot.load(std::memory_order_acquire);
            if (b || !create) return b;

            auto* fresh = new (std::nothrow) LockFreeList<T>(allocator);
            if (!fresh) return nullptr;
            if (!fresh->ok()) { delete fresh; return nullptr; }

            if (slot.compare_exchange_strong(b, fresh,
                    std::memory_order_release, std::memory_order_acquire)) {
                return fresh;
            }
            delete fresh;   // lost the race; b now holds the winner
            return b;
        }

    public:
        // Bucket count for an index that may hold up to maxEntries, targeting chains of ~8.
        //
        // Size against the REAL bound, not a guess, and be careful what the bound is: the original
        // caller here nearly sized an index off a per-worker fiber budget when the actual ceiling
        // was the per-worker figure times the worker count -- a 16x undercount on a big machine,
        // and chains 64 deep instead of 4. Whatever your bound is, make sure it is the global one.
        static size_t SuggestBuckets(size_t maxEntries) {
            size_t want = maxEntries / 8;
            if (want < 16)  want = 16;     // floor: small events still get a real spread
            if (want > 512) want = 512;    // ceiling: past this the table costs more than it saves
            size_t n = 1;
            while (n < want) n <<= 1;
            return n;
        }

        // buckets_ is rounded UP to a power of two so the index is a mask instead of a modulo.
        explicit LockFreeHashMap(TaskAllocator& alloc, size_t buckets_ = 16) : allocator(alloc) {
            size_t n = 1;
            while (n < buckets_) n <<= 1;
            bucketCount = n;
            mask = n - 1;
            buckets.reset(new std::atomic<LockFreeList<T>*>[n]);
            for (size_t i = 0; i < n; ++i)
                buckets[i].store(nullptr, std::memory_order_relaxed);
        }

        ~LockFreeHashMap() {
            // Each surviving bucket's destructor frees its own sentinels AND any live entries.
            for (size_t i = 0; i < bucketCount; ++i)
                delete buckets[i].load(std::memory_order_acquire);
        }

        LockFreeHashMap(const LockFreeHashMap&) = delete;
        LockFreeHashMap& operator=(const LockFreeHashMap&) = delete;

        // False if the key was already present, or if the slab is too dry to hold it -- the same
        // contract as LockFreeList::add, which callers were already obliged to handle.
        bool add(uint64_t key, T item) {
            LockFreeList<T>* b = BucketFor(key, true);
            return b && b->add(key, item);
        }

        bool remove(uint64_t key) {
            LockFreeList<T>* b = BucketFor(key, false);
            return b && b->remove(key);
        }

        bool contains(uint64_t key) {
            LockFreeList<T>* b = BucketFor(key, false);
            return b && b->contains(key);
        }

        size_t buckets_count() const { return bucketCount; }

        // Exposed for tests. The failure this guards is silent: drop the Mix and every 64-byte slab
        // pointer lands in one bucket, so the map still WORKS and every behavioural check still
        // passes while the performance it exists for is entirely gone.
        size_t bucket_index(uint64_t key) const { return Mix(key) & mask; }

        // Visits every live entry. NOT A SNAPSHOT: an entry added to a bucket this walk has already
        // passed is missed, exactly as an entry added at the head of a single list mid-walk would
        // be. If that matters to a caller, it needs its own resolution -- there is no cheap way to
        // give a lock-free map a consistent full iteration, and pretending otherwise would be worse
        // than saying so here.
        template <typename F>
        void for_each(F func) {
            for (size_t i = 0; i < bucketCount; ++i) {
                if (LockFreeList<T>* b = buckets[i].load(std::memory_order_acquire))
                    b->for_each(func);
            }
        }
    };
}
