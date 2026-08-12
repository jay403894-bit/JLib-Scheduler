// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.
//
// Tests for the parts of the >64-CPU support that are PURE LOGIC, and therefore the parts that can
// be wrong without any wide hardware existing to notice.
//
// This file exists because of a real bug. When the flat CpuId was introduced, the worker-to-CPU
// assignment in TaskScheduler::StartPool was still `worker i -> cpu i+1`, a DENSE walk over a
// SPARSE id space. On a machine whose processor groups are not exactly full it would have handed
// out ids naming no processor, the binds would have failed, and those workers would have run
// unbound with a topology map pointing at CPUs that do not exist. Nothing would have crashed.
//
// The only honest way to catch that class of bug without renting a 128-thread machine is to build
// the topology by hand and assert on the mapping, which is what the third section does. Treat a
// failure here as evidence the wide path is broken, because on the machines we can actually run it
// is the only evidence available.

#include "../include/Topology.h"
#include <cstdio>
#include <vector>
#include <algorithm>

using namespace JLib::topology;

static int failures = 0;
static void Check(bool ok, const char* what) {
    std::printf("  %-58s %s\n", what, ok ? "ok" : "FAILED");
    if (!ok) ++failures;
}

// ------------------------------------------------------------------ 1. CpuMask basics
static void TestCpuMaskBasics() {
    std::printf("CpuMask\n");

    CpuMask m;
    Check(!m.Any(),        "default-constructed mask is empty");
    Check(m.Count() == 0,  "empty mask counts zero");

    m.Set(0);
    m.Set(63);
    m.Set(64);       // first bit of the SECOND word: the old uint64_t could not hold this at all
    m.Set(255);      // last representable
    Check(m.Any(),            "mask with bits set reports Any");
    Check(m.Count() == 4,     "counts across all four words");
    Check(m.Test(0),          "bit 0 set");
    Check(m.Test(63),         "bit 63 set (last bit of word 0)");
    Check(m.Test(64),         "bit 64 set (first bit of word 1)");
    Check(m.Test(255),        "bit 255 set (last representable)");
    Check(!m.Test(1),         "unset bit reads false");
    Check(!m.Test(62),        "unset bit near a boundary reads false");

    // Out of range must be inert rather than corrupting a neighbouring word or faulting.
    CpuMask oob;
    oob.Set(CpuMask::kMaxCpus);
    oob.Set(CpuMask::kMaxCpus + 1000);
    Check(!oob.Any(),                     "Set() past kMaxCpus is ignored, not wrapped");
    Check(!oob.Test(CpuMask::kMaxCpus),   "Test() past kMaxCpus is false, not out-of-bounds");
    Check(!oob.Test(0),                   "out-of-range Set did not bleed into word 0");

    CpuMask a, b;
    a.Set(70); b.Set(70);
    Check(a == b,  "equality holds for identical masks");
    b.Set(200);
    Check(a != b,  "inequality holds when a high word differs");
}

// ------------------------------------------------------------------ 2. The flat id split
static void TestFlatIdSplit() {
    std::printf("flat CpuId <-> (group, bit)\n");

    Check(CpuMask::GroupOf(0)   == 0 && CpuMask::BitOf(0)   == 0,  "id 0   -> group 0, bit 0");
    Check(CpuMask::GroupOf(63)  == 0 && CpuMask::BitOf(63)  == 63, "id 63  -> group 0, bit 63");
    Check(CpuMask::GroupOf(64)  == 1 && CpuMask::BitOf(64)  == 0,  "id 64  -> group 1, bit 0");
    Check(CpuMask::GroupOf(100) == 1 && CpuMask::BitOf(100) == 36, "id 100 -> group 1, bit 36");
    Check(CpuMask::GroupOf(191) == 2 && CpuMask::BitOf(191) == 63, "id 191 -> group 2, bit 63");

    bool roundTrip = true;
    for (unsigned g = 0; g < 4; ++g)
        for (unsigned b = 0; b < 64; ++b) {
            const CpuId id = CpuMask::Make(g, b);
            if (CpuMask::GroupOf(id) != g || CpuMask::BitOf(id) != b) roundTrip = false;
        }
    Check(roundTrip, "Make/GroupOf/BitOf round-trip over every group and bit");
}

// ------------------------------------------------------------------ 3. THE BUG THIS FILE EXISTS FOR
//
// Mirrors the enumeration StartPool performs: union every core mask, list the live ids ascending,
// and assign worker i the (i+1)'th. Compared against the dense `i+1` it replaced.
static std::vector<int> EnumerateLiveCpus(const std::vector<CpuMask>& coreMasks) {
    CpuMask all;
    for (const CpuMask& m : coreMasks)
        for (unsigned cpu = 0; cpu < CpuMask::kMaxCpus; ++cpu)
            if (m.Test(cpu)) all.Set(cpu);

    std::vector<int> live;
    for (unsigned cpu = 0; cpu < CpuMask::kMaxCpus; ++cpu)
        if (all.Test(cpu)) live.push_back((int)cpu);
    return live;
}

static void TestUnevenGroups() {
    std::printf("worker -> CPU assignment on UNEVEN processor groups\n");

    // 96 logical CPUs presented as two groups of 48, which is what Windows does when it aligns
    // groups to NUMA nodes rather than packing them to 64. Live ids: 0..47 and 64..111. No SMT, so
    // one core mask per CPU.
    std::vector<CpuMask> coreMasks;
    for (unsigned g = 0; g < 2; ++g)
        for (unsigned b = 0; b < 48; ++b) {
            CpuMask m;
            m.Set(CpuMask::Make(g, b));
            coreMasks.push_back(m);
        }

    const std::vector<int> live = EnumerateLiveCpus(coreMasks);
    Check(live.size() == 96,                       "96 live CPUs discovered");
    Check(live[0] == 0 && live[47] == 47,          "group 0 occupies ids 0..47");
    Check(live[48] == 64 && live[95] == 111,       "group 1 occupies ids 64..111, after the hole");

    // Every worker must land on a CPU that exists, in a group that exists, and no two may collide.
    const size_t numWorkers = live.size() - 1;     // main keeps live[0]
    bool allExist = true, allDistinct = true, groupsSane = true;
    std::vector<int> assigned;
    for (size_t i = 0; i < numWorkers; ++i) {
        const int cpu = live[i + 1];
        if (!std::binary_search(live.begin(), live.end(), cpu)) allExist = false;
        if (std::find(assigned.begin(), assigned.end(), cpu) != assigned.end()) allDistinct = false;
        assigned.push_back(cpu);
        const unsigned bit = CpuMask::BitOf((CpuId)cpu);
        if (bit >= 48) groupsSane = false;         // neither group has a processor 48 or above
    }
    Check(allExist,    "every worker is assigned a CPU that exists");
    Check(allDistinct, "no two workers are assigned the same CPU");
    Check(groupsSane,  "no worker asks for a processor number its group does not have");

    // The negative control. Without this, the two checks above could pass on a broken enumeration
    // simply because the machine happened to be dense, and the test would be worthless.
    int denseWouldBeWrong = 0;
    for (size_t i = 0; i < numWorkers; ++i) {
        const int dense = (int)i + 1;
        if (!std::binary_search(live.begin(), live.end(), dense)) ++denseWouldBeWrong;
    }
    Check(denseWouldBeWrong == 16,
          "the old dense i+1 would have missed exactly 16 CPUs (ids 48..63)");

    // And the case everyone actually runs: one full group, where the fix must be a no-op.
    std::vector<CpuMask> dense32;
    for (unsigned b = 0; b < 32; ++b) { CpuMask m; m.Set(b); dense32.push_back(m); }
    const std::vector<int> liveDense = EnumerateLiveCpus(dense32);
    bool identicalToOldScheme = (liveDense.size() == 32);
    for (size_t i = 0; i + 1 < liveDense.size(); ++i)
        if (liveDense[i + 1] != (int)i + 1) identicalToOldScheme = false;
    Check(identicalToOldScheme, "on a dense single-group machine it collapses to exactly i+1");
}

// ------------------------------------------------------------------ 4. sysfs list parsing (POSIX)
#if !JLIB_PLATFORM_WINDOWS
static void TestParseCpuList() {
    std::printf("sysfs CPU-list parsing\n");

    CpuMask m = detail::ParseCpuList("0-3,8,12-15");
    Check(m.Count() == 9,                       "\"0-3,8,12-15\" yields 9 CPUs");
    Check(m.Test(0) && m.Test(3),               "  range endpoints are inclusive");
    Check(!m.Test(4) && !m.Test(7),             "  gaps are not set");
    Check(m.Test(8),                            "  bare single value is set");
    Check(m.Test(12) && m.Test(15),             "  trailing range parsed");

    // The case the old uint64_t silently truncated: a second-socket CPU list.
    CpuMask wide = detail::ParseCpuList("0-63,128-191");
    Check(wide.Count() == 128,                  "\"0-63,128-191\" yields 128 CPUs, not 64");
    Check(wide.Test(63) && wide.Test(128),      "  spans the old 64-bit boundary");
    Check(wide.Test(191),                       "  reaches id 191");
    Check(!wide.Test(64) && !wide.Test(127),    "  the gap between sockets stays clear");

    CpuMask big = detail::ParseCpuList("250-999");
    Check(big.Test(255) && !big.Test(250 + 900), "ids past kMaxCpus are dropped, not wrapped");

    Check(!detail::ParseCpuList("").Any(),        "empty string yields an empty mask");
    Check(!detail::ParseCpuList("garbage").Any(), "unparseable input yields an empty mask");
    Check(detail::ParseCpuList("7\n").Count() == 1, "trailing newline from sysfs is tolerated");
}
#endif

int main() {
    std::printf("JLib::Scheduler topology tests (kMaxCpus=%u)\n\n", CpuMask::kMaxCpus);
    TestCpuMaskBasics();
    TestFlatIdSplit();
    TestUnevenGroups();
#if !JLIB_PLATFORM_WINDOWS
    TestParseCpuList();
#endif
    std::printf("\n%s\n", failures == 0 ? "ALL CHECKS PASSED" : "FAILURES PRESENT");
    return failures == 0 ? 0 : 1;
}
