// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// CPU topology acquisition -- macOS.
//
// Third implementation of one query, sharing no code with the other two: Windows walks
// GetLogicalProcessorInformationEx, Linux parses sysfs text, and macOS answers sysctl. That is
// exactly why these are separate FILES rather than #ifdef branches inside one.
//
// WHAT THIS DELIBERATELY DOES NOT REPORT, and why that is correct rather than unfinished:
//
// macOS tells you HOW MANY CPUs are in each performance level (hw.perflevel0.* is the
// highest-performance level, hw.perflevel1.* the efficiency one) but it does NOT publish a
// logical-CPU-index -> perflevel mapping, nor which CPUs share an L2. You can find claims that the
// indices are simply ordered P-then-E; that is an observation about particular kernels, not a
// documented contract, and Info::efficiencyClass feeds task PLACEMENT.
//
// And placement is the part macOS does not offer at all: there is no thread-affinity API on Apple
// arm64 (THREAD_AFFINITY_POLICY still links but has been a no-op since Apple Silicon -- the kernel
// owns placement and takes intent through QoS classes instead). So a class table built on a guessed
// ordering could not be ACTED on even if it were right; it would only skew queue selection on the
// strength of an assumption. Reporting "all cores equal" is both honest and operationally
// identical, and it is the documented fallback Info was designed around.
//
// The counts are still worth reading for the SMT answer below, which is real information.
#include "../../include/Topology.h"
#include <sys/sysctl.h>
#include <cstdint>

namespace {

// Returns -1 if the key does not exist. Keys legitimately vary by chip: hw.perflevel1.* is absent
// on a non-hybrid Mac, and hw.nperflevels is absent on older Intel ones.
long long SysctlInt(const char* name)
{
    std::int64_t value = 0;
    size_t size = sizeof(value);
    if (::sysctlbyname(name, &value, &size, nullptr, 0) != 0) return -1;
    if (size == sizeof(std::int32_t)) return (long long)*(std::int32_t*)&value;
    return (long long)value;
}

} // namespace

void JLib::topology::Query(Info& out)
{
    const long long logical  = SysctlInt("hw.logicalcpu");
    const long long physical = SysctlInt("hw.physicalcpu");
    if (logical <= 0) return;                 // nothing usable; both have* flags stay false

    const int n = (int)(logical > 64 ? 64 : logical);   // group-0 / first-64 rule, as on Windows

    // SMT: the one thing sysctl answers unambiguously. Apple Silicon has no SMT, so logical ==
    // physical and every core is its own sibling group -- which is real information, not an
    // absence of it: it lets the caller correctly conclude there are no SMT pairs to reason about.
    if (physical > 0 && physical == logical) {
        out.coreMasks.reserve((size_t)n);
        for (int c = 0; c < n; ++c)
            out.coreMasks.push_back(std::uint64_t(1) << c);
        out.haveCores = true;
    }
    // Hyper-threaded Intel Mac: we know pairs EXIST (logical == 2 * physical) but not which CPU
    // indices form them, and a wrong pairing is worse than none -- it would make the steal order
    // prefer a core that shares nothing. Leave haveCores false so the caller records no siblings.

    // No cache masks: see the file header. haveCache stays false, and the caller falls back to
    // "the whole pool is one cluster", which on a single-package Mac is very nearly true anyway.
    // hw.perflevel0.cpusperl2 exists and would give real cluster SIZES on Apple Silicon if the
    // index mapping ever becomes available.

    // efficiencyClass left at -1 for every CPU -- see the file header for why that is deliberate.
}
