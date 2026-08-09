// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"
#include <vector>
#include <cstdint>

// CPU topology ACQUISITION -- the one part of BuildTopology that is genuinely platform-specific.
//
// Windows reads it from GetLogicalProcessorInformationEx; Linux parses sysfs text. Those share no
// code whatsoever, which is why they are separate FILES (src/win32/Topology.cpp, src/posix/
// Topology.cpp) rather than #ifdef branches.
//
// What is NOT here, deliberately: turning these masks into sibling/cluster relationships. That
// derivation holds the subtle parts -- the "only meaningful if exactly two POOL WORKERS share this
// core" rule, picking the widest cache group as the last-level one, the P/E demotion rule -- and it
// is identical on every platform. It stays in TaskScheduler::BuildTopology so there is exactly one
// copy of the reasoning to get right.
//
// Group 0 / first 64 logical CPUs only, matching the existing Windows limitation. A >64-logical-CPU
// multi-group machine would need GROUP_AFFINITY.Group handled on Windows and wider masks here.

namespace JLib { namespace topology {

struct Info {
    // One mask per PHYSICAL CORE: the logical CPUs sharing it, i.e. SMT sibling groups.
    std::vector<uint64_t> coreMasks;
    // One mask per CACHE INSTANCE, all levels mixed together. The caller filters for the
    // last-level one by preferring the widest group, so no cache-level field is needed.
    std::vector<uint64_t> cacheMasks;

    bool haveCores = false;   // false => caller records no SMT siblings
    bool haveCache = false;   // false => caller falls back to "whole pool is one cluster"

    // Per-logical-CPU efficiency class; -1 = unknown. Higher is more performant, and only the
    // RELATIVE order matters: a CPU is an E-core if its class is known and strictly below
    // maxClass. All -1 (the Linux default today) means "treat every core as equal", which is
    // exactly right for a non-hybrid CPU and a safe fallback otherwise.
    int  efficiencyClass[64];
    int  maxClass = -1;

    Info() { for (int i = 0; i < 64; ++i) efficiencyClass[i] = -1; }
};

// Fills `out` as far as the platform allows. Never throws; partial success is normal and is
// reported through the haveCores/haveCache flags rather than a single boolean.
void Query(Info& out);

}} // namespace JLib::topology
