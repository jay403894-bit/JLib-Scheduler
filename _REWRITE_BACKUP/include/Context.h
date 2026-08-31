// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include <stdint.h>

struct Context {
    void* rsp;      // 0
};

extern "C" void ContextSwitch(Context* from, Context* to);

