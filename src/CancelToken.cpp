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

        // Head of the free list: (ABA tag << 16) | (1-based slot index), 0 index meaning empty.
        //
        // THE TAG IS NOT OPTIONAL. A plain index stack is the textbook ABA case: a popper reads
        // head=i and slots[i].nextFree=j, stalls, and by the time it CASes, i has been popped, used,
        // freed and re-pushed with a different successor -- the CAS still succeeds and publishes a
        // stale j, corrupting the list. Bumping a counter in the high half makes the CAS notice.
        // kCancelSlots is 4096, so the index needs 13 bits and there are 16 to spare for the tag.
        static std::atomic<uint32_t>& FreeHead() {
            static std::atomic<uint32_t> head{ 1 };   // slot 0, 1-based
            return head;
        }

        // O(1). Replaces a linear CAS probe across the whole table, which every TaskDAG construction
        // paid -- once per frame for a per-frame graph, and degrading as more scopes stayed alive.
        static uint32_t AcquireSlot() {
            CancelSlot* table = CancelSlotTable();
            uint32_t head = FreeHead().load(std::memory_order_acquire);
            for (;;) {
                const uint32_t idx = head & 0xFFFFu;
                if (idx == 0) return 0xFFFFFFFFu;              // exhausted; caller fails open
                const uint32_t next = table[idx - 1].nextFree;
                const uint32_t bumped = (((head >> 16) + 1) << 16) | next;
                if (FreeHead().compare_exchange_weak(head, bumped,
                        std::memory_order_acq_rel, std::memory_order_acquire))
                    return idx - 1;
            }
        }

        static void ReleaseSlot(uint32_t index) {
            CancelSlot* table = CancelSlotTable();
            uint32_t head = FreeHead().load(std::memory_order_acquire);
            for (;;) {
                // Safe to write unatomically: this slot is ours until the CAS publishes it.
                table[index].nextFree = head & 0xFFFFu;
                const uint32_t bumped = (((head >> 16) + 1) << 16) | (index + 1);
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
            if (s->generation.load(std::memory_order_acquire) != gen) return nullptr;
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
        s->cancelled.store(0, std::memory_order_release);
        const uint32_t gen = s->generation.load(std::memory_order_acquire) & 0xFFFFu;
        raw_ = (gen << 16) | i;
    }

    CancelScope::~CancelScope() {
        if (raw_ == CancelToken::kNone) return;
        const uint32_t i = raw_ & 0xFFFFu;
        detail::CancelSlot* s = &detail::CancelSlotTable()[i];
        // Bump the generation BEFORE returning the slot, so any handle still holding the old one
        // starts failing its check the moment the scope is gone -- rather than at the point some
        // later scope happens to claim the slot and set its own flag.
        s->generation.fetch_add(1, std::memory_order_acq_rel);
        s->cancelled.store(0, std::memory_order_release);
        detail::ReleaseSlot(i);
    }

    void CancelScope::Cancel() noexcept {
        if (raw_ == CancelToken::kNone) return;
        detail::CancelSlotTable()[raw_ & 0xFFFFu].cancelled.store(1, std::memory_order_release);
    }

    bool CancelScope::Cancelled() const noexcept {
        return CancelToken(raw_).Cancelled();
    }

} // namespace JLib
