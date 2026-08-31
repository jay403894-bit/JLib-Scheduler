// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#include "../include/CancelToken.h"

namespace JLib {
    namespace detail {

        CancelSlot* CancelSlotTable() {
            // Function-local static: no static-init-order dependency on the scheduler, and the
            // zero-initialised array costs nothing until a scope is actually created. The lambda
            // threads every slot onto the free list once, under the magic-static guard.
            static CancelSlot* table = [] {
                static CancelSlot t[kCancelSlots];
                for (uint32_t i = 0; i < kCancelSlots; ++i) t[i].nextFree = i + 2;  // 1-based, next
                t[kCancelSlots - 1].nextFree = 0;                                   // end of list
                return t;
            }();
            return table;
        }

        // Head of the free list: (ABA tag << 32) | (1-based slot index), 0 index meaning empty.
        //
        // THE TAG IS NOT OPTIONAL. A plain index stack is the textbook ABA case: a popper reads
        // head=i and slots[i].nextFree=j, stalls, and by the time it CASes, i has been popped, used,
        // freed and re-pushed with a different successor -- the CAS still succeeds and publishes a
        // stale j, corrupting the list. Bumping a counter in the high half makes the CAS notice.
        //
        // SIXTY-FOUR BITS, and that is forced rather than luxurious. This was 32 bits split 16/16
        // while kCancelSlots was 4,096. The index here is ONE-BASED, so a table of 65,535 slots
        // needs to represent 65,535 -- which fits 16 bits only just, leaving the "empty" sentinel
        // and the tag nowhere to live, and a table of 65,536 would not fit at all. Widening to 64
        // gives the index a full 32 bits and the tag the other 32, so the slot count and the ABA
        // protection stop competing for the same word.
        static std::atomic<uint64_t>& FreeHead() {
            static std::atomic<uint64_t> head{ 1 };   // slot 0, 1-based
            return head;
        }

        // O(1). Replaces a linear CAS probe across the whole table, which every TaskDAG construction
        // paid -- once per frame for a per-frame graph, and degrading as more scopes stayed alive.
        static constexpr uint64_t kIndexMask = 0xFFFFFFFFull;

        static uint32_t AcquireSlot() {
            CancelSlot* table = CancelSlotTable();
            uint64_t head = FreeHead().load(std::memory_order_acquire);
            for (;;) {
                const uint32_t idx = uint32_t(head & kIndexMask);
                if (idx == 0) return 0xFFFFFFFFu;              // exhausted; caller fails open
                const uint64_t next = table[idx - 1].nextFree;
                const uint64_t bumped = (((head >> 32) + 1) << 32) | next;
                if (FreeHead().compare_exchange_weak(head, bumped,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                    return idx - 1;
            }
        }

        static void ReleaseSlot(uint32_t index) {
            CancelSlot* table = CancelSlotTable();
            uint64_t head = FreeHead().load(std::memory_order_acquire);
            for (;;) {
                // Safe to write unatomically: this slot is ours until the CAS publishes it.
                table[index].nextFree = uint32_t(head & kIndexMask);
                const uint64_t bumped = (((head >> 32) + 1) << 32) | uint64_t(index + 1);
                if (FreeHead().compare_exchange_weak(head, bumped,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                    return;
            }
        }

        CancelSlot* ResolveCancelSlot(uint32_t raw) {
            const uint32_t index = raw & 0xFFFFu;
            const uint32_t gen = raw >> 16;
            if (index >= kCancelSlots) return nullptr;

            CancelSlot* s = &CancelSlotTable()[index];
            // The generation is read AFTER the bounds check and compared before the caller reads
            // `cancelled`. A slot recycled since this handle was issued fails here, which is the
            // whole point: without it a stale token reports whichever scope inherited the slot.
            // MASK BEFORE COMPARING. The token carries only the low 16 bits (the ctor packs
            // `generation & 0xFFFF`), while this counter is a full 32-bit value that keeps
            // climbing. Comparing them unmasked worked until a slot's counter passed 65,535 and
            // then could NEVER match again: resolve returned null, Cancelled() read false, and the
            // slot FAILED OPEN PERMANENTLY -- cancellation silently stopped working with no error
            // anywhere. The free list is LIFO, so a create/destroy loop reuses one slot and drives
            // one counter; a per-frame scope reached this in about 18 minutes at 60fps, and from
            // then on every scope taking that slot was dead too.
            //
            // Wrapping is the CORRECT behaviour here, not a leftover: 16 bits of generation means a
            // handle stale by exactly 65,536 reuses aliases a live scope, and that is the documented
            // bound. Silently never matching is not a safer version of that -- it is the failure
            // mode the generation exists to prevent, applied to every token instead of a rare one.
            if (CancelSlot::GenOf(s->state.load(std::memory_order_acquire)) != gen) return nullptr;
            return s;
        }

    } // namespace detail

    CancelScope::CancelScope() noexcept {
        const uint32_t i = detail::AcquireSlot();
        if (i == 0xFFFFFFFFu) {
            // Free list empty. Stays invalid, so every token it hands out reports "not cancelled".
            // FAILING OPEN IS DELIBERATE: the opposite would abort live work because an unrelated
            // part of the process ran out of scopes.
            raw_ = CancelToken::kNone;
            return;
        }
        detail::CancelSlot* s = &detail::CancelSlotTable()[i];
        // The slot arrives with its flag already clear (the destructor guarantees it); this only
        // reads the generation the destructor left, to stamp into the handle.
        s->parent.store(CancelToken::kNone, std::memory_order_release);
        const uint32_t gen = detail::CancelSlot::GenOf(s->state.load(std::memory_order_acquire));
        raw_ = (gen << 16) | i;
    }

    // Nested. Identical to the root constructor except that the parent link is stored BEFORE the
    // token is published in raw_ -- so no reader can ever observe this scope with an unset or
    // half-written parent, and a walk that reaches it always sees the whole chain.
    //
    // The parent is resolved here rather than trusted: a stale handle stores kNone and this becomes
    // an ordinary root scope. Storing the raw handle unchecked would work too (the walk would just
    // fail to resolve it), but resolving now means the failure is one load at construction instead
    // of one failed resolve on every suspend point for the rest of the scope's life.
    CancelScope::CancelScope(CancelToken parent) noexcept {
        const uint32_t i = detail::AcquireSlot();
        if (i == 0xFFFFFFFFu) { raw_ = CancelToken::kNone; return; }

        detail::CancelSlot* s = &detail::CancelSlotTable()[i];
        // The slot arrives with its flag already clear (the destructor guarantees it); this only
        // reads the generation the destructor left, to stamp into the handle.
        s->parent.store(detail::ResolveCancelSlot(parent.Raw()) ? parent.Raw() : CancelToken::kNone,
                        std::memory_order_release);
        const uint32_t gen = detail::CancelSlot::GenOf(s->state.load(std::memory_order_acquire));
        raw_ = (gen << 16) | i;
    }

    CancelScope::~CancelScope() {
        if (raw_ == CancelToken::kNone) return;
        const uint32_t i = raw_ & 0xFFFFu;
        detail::CancelSlot* s = &detail::CancelSlotTable()[i];
        // Bump the generation BEFORE returning the slot, so any handle still holding the old one
        // starts failing its check the moment the scope is gone -- rather than at the point some
        // later scope happens to claim the slot and set its own flag.
        //
        // ONE step: bump the generation and clear the flag together. As two writes they could be
        // observed half-done -- a CancelVia landing between them sets the flag on the NEW
        // generation, and the slot is handed to the next scope already cancelled.
        uint64_t st = s->state.load(std::memory_order_acquire);
        while (!s->state.compare_exchange_weak(
                   st, uint64_t(uint32_t(st >> 32) + 1) << 32,
                   std::memory_order_acq_rel, std::memory_order_acquire)) {
            // Retries only because a Cancel raced in. That cancellation is dropped on purpose: the
            // scope is gone and there is nobody left who could observe it.
        }

        // Clear the parent too. A slot handed to a new root scope must not be born inheriting the
        // last tenant's ancestry -- the root constructor sets it as well, but a slot sitting free
        // with a live-looking parent link is the kind of state worth never having.
        s->parent.store(CancelToken::kNone, std::memory_order_release);
        detail::ReleaseSlot(i);
    }

    void CancelScope::Cancel() noexcept {
        if (raw_ == CancelToken::kNone) return;
        // fetch_or, not a store: the generation shares this word and must survive untouched.
        // No generation check needed -- this scope OWNS the slot for its whole lifetime, so it
        // cannot be writing to somebody else's. CancelVia, which holds only a token, must check.
        detail::CancelSlotTable()[raw_ & 0xFFFFu].state
            .fetch_or(detail::CancelSlot::kCancelledBit, std::memory_order_acq_rel);
    }

    bool CancelScope::Cancelled() const noexcept {
        return CancelToken(raw_).Cancelled();
    }

    // Generation-checked, which is the entire reason this is safe for a timer to call late. Cancel()
    // above can write unchecked because a CancelScope owns its slot for its lifetime; a bare token
    // owns nothing, so it must prove the slot is still the one it was issued against before writing.
    // Without that check a timer firing after its operation completed would cancel whatever scope
    // inherited the slot -- an unrelated connection, at random.
    // A CAS, NOT resolve-then-store. Resolving first and writing second leaves a window in which the
    // scope is destroyed and its slot re-issued, and the write then cancels whichever scope moved in
    // -- a live, unrelated connection, at random. That is the exact failure the generation exists to
    // prevent, so the check and the write must be the same atomic step. Both live in one word for
    // this reason; see CancelSlot.
    bool CancelVia(CancelToken token) noexcept {
        const uint32_t raw = token.Raw();
        if (raw == CancelToken::kNone) return false;
        const uint32_t index = raw & 0xFFFFu;
        if (index >= detail::kCancelSlots) return false;

        detail::CancelSlot* s = &detail::CancelSlotTable()[index];
        uint64_t st = s->state.load(std::memory_order_acquire);
        for (;;) {
            // Recycled since the handle was issued: the scope is gone. Say so rather than cancel.
            if (detail::CancelSlot::GenOf(st) != (raw >> 16)) return false;
            if (detail::CancelSlot::CancelledIn(st)) return true;   // idempotent
            if (s->state.compare_exchange_weak(st, st | detail::CancelSlot::kCancelledBit,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire))
                return true;
            // CAS failed: st was reloaded. If the destructor won the race the generation now
            // differs and the check above returns false on the next pass.
        }
    }

} // namespace JLib
