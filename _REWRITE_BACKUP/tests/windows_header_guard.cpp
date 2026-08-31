// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// COMPILE-ONLY GUARD: no public header may name anything after a common Windows macro.
//
// WHY THIS EXISTS. 3.0.0 and 3.0.1 shipped broken for every Windows application while this
// repository's own build stayed green throughout. TaskAllocator had a member named `small`, and
// rpcndr.h -- which arrives through windows.h in essentially any real Windows program -- contains:
//
//     #define small char
//
// So `SlabPool<64> small;` expanded to `SlabPool<64> char;`. The first error is C2628 in
// TaskAllocator.h and the next hundred are cascade errors in TaskScheduler.h pointing nowhere near
// the cause. It took building a game against the library to find a one-line problem.
//
// WHY IT DEFINES THE MACROS INSTEAD OF INCLUDING <windows.h>. This library includes windows.h in
// exactly one place -- platform.h, behind WIN32_LEAN_AND_MEAN and NOMINMAX -- and a test that
// included it directly would break that convention to test it, which is the wrong trade. Defining
// the macros is also STRICTLY BETTER coverage: it names the precise hazard, it covers more macros
// than any single Windows SDK version would happen to define, and it compiles everywhere, so a
// Linux or macOS CI leg catches a Windows-only break before a Windows machine ever sees it.
//
// WHAT TO DO WHEN THIS FAILS: something got named after one of the macros below. Rename it. Do NOT
// #undef the macro -- that changes what every other header in the consumer's translation unit sees,
// and a library has no business doing that to its user.

// The usual offenders. `small` is the one that actually bit; the rest are here because the next one
// will be a different name and this file should catch it the first time rather than the second.
#define small        char
#define near
#define far
#define interface    struct
#define IN
#define OUT
#define ERROR        0
#define DELETE       (0x00010000L)

// Every public header, in the order a consumer is most likely to reach them. TaskScheduler.h pulls
// in most of the tree, but the rest are listed explicitly so a collision in a header that only
// something else includes is still caught here.
#include "TaskScheduler.h"
#include "TaskAllocator.h"
#include "SlabPool.h"
#include "TaskDAG.h"
#include "TaskNode.h"
#include "Event.h"
#include "DirectEvent.h"
#include "LockFreeList.h"
#include "LockFreeHashMap.h"
#include "Epochs.h"
#include "Fiber.h"
#include "Thread.h"
#include "CancelToken.h"
#include "Timer.h"
#include "platform.h"

#include <cstdio>

int main() {
    // The slot constants are API, and a rename that satisfies the compiler while changing them
    // would be a silent break for anyone sizing against them.
    static_assert(JLib::TaskAllocator::SLOT       == 256, "slot constants are part of the API");
    static_assert(JLib::TaskAllocator::MID_SLOT   == 128, "slot constants are part of the API");
    static_assert(JLib::TaskAllocator::SLOT80     == 80,  "slot constants are part of the API");
    static_assert(JLib::TaskAllocator::SMALL_SLOT == 64,  "slot constants are part of the API");

    std::printf("Windows macro collision guard\n");
    std::printf("  every public header compiles with small/near/far/interface defined   ok\n");
    std::printf("\nALL CHECKS PASSED\n");
    return 0;
}
