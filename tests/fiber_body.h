// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// CREATING A FIBER TASK FROM A NAMED BODY AND AN EXPLICIT CONTEXT.
//
// This header used to hold `MakeFiberTask(sched, F* body)` -- a template that took a POINTER TO A
// CLOSURE. It is gone, and the reason is worth keeping, because it looked safe.
//
// A fiber's stack is a PLACE. It is register state mapped to memory, and preserving it across a
// suspension is the one thing the whole runtime exists to do. A closure is a VALUE with a lifetime
// somebody else controls. Say the two together out loud -- "I suspended my stack on the heap" -- and
// the category error is audible. There is no version of it that works: either the frame is gone when
// the fiber resumes, or the fiber never dies and the frame is never freed. In production those are a
// corruption and a leak, and neither surfaces where it was caused, because SlabPool is append-only
// and a released slot stays mapped holding its old bytes.
//
// PASSING THE CLOSURE'S ADDRESS DID NOT FIX THAT. It only moved the lifetime from something the
// compiler checks to something the author has to remember -- and the migration that introduced it
// produced roughly thirty sites needing lifetime-specific fixes, one of which (teardown_drain's
// spawn helper) crashed outright because the closure died when the helper returned while its fibers
// stayed parked forever. An affordance whose correct use requires that much vigilance is not an
// affordance.
//
// SO: the body is a NAMED FUNCTION and its state is a STRUCT THE CALLER DECLARES.
//
//     struct LoadCtx { WaitGroup* wg; Event* ev; int* n; };
//     static void LoadBody(void* p) {
//         auto* c = static_cast<LoadCtx*>(p);
//         ...
//     }
//
//     LoadCtx ctx{ &wg, &ev, &n };                       // scope visible at the declaration
//     Task* t = JLibTest::MakeCtxTask(sched, &LoadBody, &ctx);
//
// The context's scope must cover the wait. Declare-then-join in the same function satisfies that
// structurally; a context declared inside a loop whose join is after the loop does not.
//
// AND NOT A FILE-SCOPE STATIC INSTEAD, to save the struct. A static lies about lifetime: the next
// test in the translation unit, or a retry of this one, sees the last run's values. That is exactly
// how a green suite hides a leak. File-scope state is acceptable only where the file already owns a
// single sequential counter and the test cannot run twice overlapping -- never as a shortcut.
#pragma once

#include "TaskScheduler.h"

namespace JLibTest {

    // Sugar over the raw overload, and nothing more: it fixes TaskType::Fiber so the intent is
    // stated once per call rather than repeated as two positional arguments. It cannot take a
    // closure, which is the point -- `fn` is a function pointer and `ctx` is storage you named.
    inline JLib::Task* MakeCtxTask(JLib::TaskScheduler& s, void (*fn)(void*), void* ctx,
                                   JLib::Lane lane = JLib::Lane::Normal,
                                   JLib::CorePref pref = JLib::CorePref::Default,
                                   JLib::StackClass stack = JLib::StackClass::Standard) {
        return s.CreateTask(fn, ctx, lane, JLib::TaskType::Fiber, pref, stack);
    }

}   // namespace JLibTest
