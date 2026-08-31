// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

#pragma once
#include "platform.h"
#include <vector>
#include <cstdint>
#if JLIB_PLATFORM_LINUX
#include <string>   // detail::ParseCpuList, declared at the bottom for the test suite
#endif

// CPU topology ACQUISITION -- the one part of BuildTopology that is genuinely platform-specific.
//
// Windows reads it from GetLogicalProcessorInformationEx; Linux parses sysfs text; macOS asks
// sysctl. Those share no code whatsoever, which is why they are separate FILES (src/win32/,
// src/posix/, src/darwin/) rather than #ifdef branches.
//
// What is NOT here, deliberately: turning these masks into sibling/cluster relationships. That
// derivation holds the subtle parts -- the "only meaningful if exactly two POOL WORKERS share this
// core" rule, picking the widest cache group as the last-level one, the P/E demotion rule -- and it
// is identical on every platform. It stays in TaskScheduler::BuildTopology so there is exactly one
// copy of the reasoning to get right.

namespace JLib { namespace topology {

// ================================ THE FLAT LOGICAL-CPU ID ======================================
// A single number naming one logical CPU, defined as `group * 64 + bit-within-group`.
//
// Windows does not hand out a flat processor index. Above 64 logical CPUs it splits the machine
// into PROCESSOR GROUPS of at most 64 and numbers CPUs within a group, so "CPU 100" is not a thing
// you can say to the OS -- you say "group 1, bit 36". This library needs one integer to index
// tables by, so it defines one, and defines it so the group and bit fall back out by shifting.
//
// The id is SPARSE when groups are not full. Windows aligns groups to socket/NUMA boundaries rather
// than packing them, so an 80-CPU machine is typically two groups of 40 and the live ids are 0..39
// and 64..103, with a hole in between. That is intentional and costs only some unused table slots:
// the alternative, a dense index, would need a lookup table in both directions and would make
// GroupOf/BitOf a memory access instead of a shift. Nothing outside the platform layer mints these
// ids, so the holes are never observable to a caller.
//
// On Linux and macOS there are no groups, the kernel's own CPU numbering is already flat and dense,
// and the id is exactly that number. The group bits are simply always zero below 64 CPUs, and above
// it they are a word index with no OS meaning, which is harmless because those platforms bind with
// a wide mask rather than a (group, mask) pair.
using CpuId = std::uint32_t;

// A set of logical CPUs. Fixed width, no allocation: these are built during Init and then read.
//
// 256 rather than 64 because 64 is a Windows GROUP size, not a machine size, and a dual-socket
// 128-core EPYC is 256 threads exactly. Rather than a std::vector, or worse a (group, mask) PAIR
// that only Windows can express -- a Linux cache domain genuinely can straddle a 64-CPU boundary
// and would have to be split into fragments, which breaks the "widest mask is the last-level cache"
// rule that BuildTopology relies on -- this is one flat bitset and every consumer keeps the shape
// it already had. Raising the cap is the kWords constant and nothing else.
struct CpuMask {
    static constexpr unsigned kWords   = 4;
    static constexpr unsigned kMaxCpus = kWords * 64;   // 256

    std::uint64_t w[kWords] = {};

    static constexpr unsigned GroupOf(CpuId c) { return c >> 6; }
    static constexpr unsigned BitOf(CpuId c)   { return c & 63u; }
    static constexpr CpuId    Make(unsigned group, unsigned bit) { return CpuId(group) * 64u + bit; }

    void Set(CpuId c) {
        if (c < kMaxCpus) w[c >> 6] |= (std::uint64_t(1) << (c & 63u));
    }
    bool Test(CpuId c) const {
        return c < kMaxCpus && ((w[c >> 6] >> (c & 63u)) & 1u) != 0;
    }
    bool Any() const {
        for (unsigned i = 0; i < kWords; ++i) if (w[i]) return true;
        return false;
    }
    // Kernighan rather than an intrinsic or std::popcount: this is C++17, the intrinsic spelling
    // differs per compiler, and every call site here runs once during Init. Clarity wins.
    unsigned Count() const {
        unsigned n = 0;
        for (unsigned i = 0; i < kWords; ++i)
            for (std::uint64_t x = w[i]; x; x &= (x - 1)) ++n;
        return n;
    }
    bool operator==(const CpuMask& o) const {
        for (unsigned i = 0; i < kWords; ++i) if (w[i] != o.w[i]) return false;
        return true;
    }
    bool operator!=(const CpuMask& o) const { return !(*this == o); }
};

struct Info {
    // One mask per PHYSICAL CORE: the logical CPUs sharing it, i.e. SMT sibling groups.
    std::vector<CpuMask> coreMasks;
    // One mask per CACHE INSTANCE, all levels mixed together. The caller filters for the
    // last-level one by preferring the widest group, so no cache-level field is needed.
    std::vector<CpuMask> cacheMasks;

    bool haveCores = false;   // false => caller records no SMT siblings
    bool haveCache = false;   // false => caller falls back to "whole pool is one cluster"

    // How many logical CPUs the platform actually reported, and across how many groups.
    //
    // logicalCount exists because std::thread::hardware_concurrency() cannot be trusted on a
    // multi-group Windows machine: historically it returned only the CALLING thread's group, and
    // which MSVC versions changed that is not something to bet a pool size on. The platform layer
    // already has to walk every group to fill the masks above, so it can just say.
    //
    // groupCount is diagnostic. It is the one number that tells you at a glance whether a machine
    // is exercising the multi-group path at all, which matters because almost none are.
    unsigned logicalCount = 0;
    unsigned groupCount   = 0;

    // Per-logical-CPU efficiency class; -1 = unknown. Higher is more performant, and only the
    // RELATIVE order matters: a CPU is an E-core if its class is known and strictly below
    // maxClass. All -1 (the Linux default today) means "treat every core as equal", which is
    // exactly right for a non-hybrid CPU and a safe fallback otherwise.
    //
    // Indexed by the flat CpuId above, so it is sized for the cap and not for 64. The sparse-id
    // holes on an unevenly grouped Windows box simply stay at -1, which reads as "unknown" and is
    // the correct answer for a CPU that does not exist.
    int  efficiencyClass[CpuMask::kMaxCpus];
    int  maxClass = -1;

    Info() { for (unsigned i = 0; i < CpuMask::kMaxCpus; ++i) efficiencyClass[i] = -1; }
};

// Fills `out` as far as the platform allows. Never throws; partial success is normal and is
// reported through the haveCores/haveCache flags rather than a single boolean.
void Query(Info& out);

// LINUX ONLY, not merely non-Windows. This parses Linux sysfs text and is defined in
// src/posix/Topology.cpp, which macOS does not build -- it gets src/darwin/Topology.cpp and sysctl
// instead. Guarding this on !JLIB_PLATFORM_WINDOWS declared it on Apple platforms too, where the
// definition does not exist, and the test suite linked against a symbol that was never emitted.
// Caught by CI on macos-14; a Windows and Linux build cannot reproduce it.
#if JLIB_PLATFORM_LINUX
namespace detail {
// Parses a Linux sysfs CPU list ("0-3,8,12-15") into a mask. Declared here ONLY so the test suite
// can reach it: it is the one piece of the POSIX topology path that is pure logic, and therefore
// the one piece that can be got wrong without any hardware being involved. Not part of the API.
CpuMask ParseCpuList(const std::string& s);
}
#endif

}} // namespace JLib::topology
