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
// And a class table built on a guessed ordering could not be ACTED on by the mechanism this struct
// was designed for: there is no thread-affinity API on Apple arm64 (THREAD_AFFINITY_POLICY still
// links but has been a no-op since Apple Silicon). Reporting "all cores equal" is therefore both
// honest and operationally identical today, and it is the documented fallback Info was built around.
//
// WHAT macOS OFFERS INSTEAD, AND WHY THIS LIBRARY DOES NOT REACH FOR IT. Apple's substitute for
// affinity is QoS: pthread_set_qos_class_self_np with QOS_CLASS_USER_INITIATED biases a thread to
// P-cores, QOS_CLASS_UTILITY to the efficiency cluster, and unlike affinity it needs no
// index->level mapping -- the counts above would be enough to tier the pool.
//
// The scheduler deliberately sets NO QoS AT ALL. Workers inherit it from the thread that calls
// Init(), and that inheritance IS the configuration mechanism: an application that wants a
// particular class sets its own QoS before initialising and the whole pool follows. Zero API
// surface, and the decision stays where it belongs.
//
// That last part is the real reason. QoS is a power and thermal decision as much as a scheduling
// one. A host that deliberately runs at UTILITY because it is a background exporter has made a
// choice about battery and fan noise, and a job system that silently promoted its workers would be
// overriding it -- the identical argument that justifies AffinityPolicy::None on the other
// platforms. A library embedded in someone else's process does not get to make that call.
//
// Tiering is also a poorer fit for THIS scheduler than it first looks, for reasons that have
// nothing to do with macOS and are worth understanding before anyone tries:
//
//   1. STEALING ERASES THE PREFERENCE, AND DOES SO EXACTLY WHEN IT WOULD MATTER. Stealing is
//      deliberately preference-blind (work-conserving), and steals happen when workers run dry.
//      The canonical E-tier use is background work that should not wake performance cores -- but
//      that is precisely the state where P-workers are idle and will pull the backlogged E-work
//      onto themselves. The hint is therefore weakest in the one scenario it exists for. Making it
//      stick would mean preference-RESPECTING stealing, i.e. surrendering work-conservation, which
//      is worth far more than the hint.
//
//   2. TIERING SHRINKS THE USABLE POOL. A class-preferred task can only be placed on its subset,
//      so the scheduler no longer has the whole CPU for it. The design survives that today because
//      preference is a HINT and PickNextWorker SPILLS to the other class when the preferred one is
//      busy -- a slower core is a fine fallback. Under QoS the spill target is a deprioritised,
//      throttleable scheduling class instead, so the graceful degradation that makes CorePref safe
//      on Windows and Linux turns into a cliff.
//
//   3. And CorePref routing is opt-in and dormant today (nothing sets it), so a startup-time split
//      would only make some workers slower at general work in exchange for nothing.
//
// If it is ever added anyway it must be opt-in, gated more tightly than the affinity path, and
// UTILITY-vs-BACKGROUND settled by measurement on real hardware rather than by guess.
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
