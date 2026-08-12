// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repository root.

// Windows topology acquisition. Moved verbatim (behaviour-wise) out of TaskScheduler.cpp so the
// Linux equivalent could exist beside it rather than inside an #ifdef.
//
// Reads REAL hardware topology rather than assuming the sequential affinity scheme (worker qIndex i
// pinned to logical CPU i+1, main on 0). That mapping tells you what was ASKED of the OS, not what
// the hardware looks like: adjacent logical CPU numbers being SMT or cache neighbours is a common
// convention and never a guarantee.
//
// ALL processor groups, not just group 0. Every GROUP_AFFINITY record is widened into the flat
// CpuId space (group * 64 + bit) defined in Topology.h, so nothing downstream has to know groups
// exist. This used to filter on `Group == 0`, which meant a machine wide enough to have a second
// group got a topology map describing only its first 64 CPUs.
#include "../../include/Topology.h"
#include <vector>
#include <cstddef>

namespace JLib { namespace topology {

// SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX is a UNION and the relation decides which member is
// valid. This was wrong for years and nobody noticed:
//
//   RelationProcessorCore -> info->Processor (PROCESSOR_RELATIONSHIP): Flags, EfficiencyClass,
//                            Reserved[20], GroupCount, GroupMask[GroupCount]
//   RelationCache         -> info->Cache     (CACHE_RELATIONSHIP):     Level, Associativity,
//                            LineSize, CacheSize, Type, Reserved[], GroupMask  (ONE affinity)
//
// Reading .Processor.GroupCount off a CACHE record lands inside CACHE_RELATIONSHIP::Reserved,
// which is zero -- so the loop ran zero times, every cache query returned an EMPTY mask list, and
// the caller's clusterMates stayed empty while haveCache reported success. Locality-aware stealing
// therefore degraded silently to plain random on every Windows machine.
//
// Found by porting: the Linux implementation reported 17 cache instances on the same box where
// this reported 0. Two implementations of the same query disagreeing is what made it visible.
static bool GetGroupMasksForRelation(LOGICAL_PROCESSOR_RELATIONSHIP relation,
                                     std::vector<CpuMask>& outMasks) {
    DWORD len = 0;
    GetLogicalProcessorInformationEx(relation, nullptr, &len);
    if (len == 0) return false;

    std::vector<std::byte> buffer(len);
    auto* base = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data());
    if (!GetLogicalProcessorInformationEx(relation, base, &len)) return false;

    DWORD offset = 0;
    while (offset < len) {
        auto* info = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buffer.data() + offset);
        if (info->Relationship == relation) {
            // GROUP_AFFINITY is inherently single-group: it carries one Group and one 64-bit Mask,
            // so a cache instance or a physical core can never span groups and each record maps to
            // exactly one CpuMask. Widen the record's bits into the flat id space (group * 64 + bit)
            // and every consumer downstream stays group-agnostic.
            //
            // These used to be filtered to `Group == 0`, which on a machine wide enough to HAVE a
            // second group meant the topology map silently described only the first 64 CPUs: no
            // SMT siblings, no cache clusters and no P/E labels for anything above that. Not a
            // degraded map, an absent one.
            auto widen = [&](const GROUP_AFFINITY& ga) {
                CpuMask m;
                for (unsigned bit = 0; bit < 64; ++bit)
                    if ((uint64_t)ga.Mask & (uint64_t(1) << bit))
                        m.Set(CpuMask::Make(ga.Group, bit));
                if (m.Any()) outMasks.push_back(m);
            };

            if (relation == RelationCache) {
                // One GROUP_AFFINITY per cache instance. Every level (L1/L2/L3) arrives through
                // this same query; the caller identifies the last-level one by preferring the
                // widest group, so no filtering on Cache.Level is needed here.
                widen(info->Cache.GroupMask);
            } else {
                const PROCESSOR_RELATIONSHIP& proc = info->Processor;
                for (WORD g = 0; g < proc.GroupCount; ++g) widen(proc.GroupMask[g]);
            }
        }
        offset += info->Size;
    }
    return true;
}

void Query(Info& out) {
    out.haveCores = GetGroupMasksForRelation(RelationProcessorCore, out.coreMasks);
    out.haveCache = GetGroupMasksForRelation(RelationCache,         out.cacheMasks);

    // Ask the OS directly rather than trusting std::thread::hardware_concurrency(), which on a
    // multi-group machine has historically reported only the CALLING thread's group. Passing
    // ALL_PROCESSOR_GROUPS is the whole point: it is the difference between seeing 128 CPUs and
    // seeing 64 of them twice as hard.
    out.groupCount   = (unsigned)GetActiveProcessorGroupCount();
    out.logicalCount = (unsigned)GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);

    // P/E-core labels (Intel hybrid). Each physical core's PROCESSOR_RELATIONSHIP carries an
    // EfficiencyClass; the HIGHEST class present is a Performance core and anything lower is an
    // Efficiency core. Left as all-unknown on failure, which the caller reads as "all cores equal"
    // -- correct for a non-hybrid CPU and safe everywhere else.
    DWORD len = 0;
    GetLogicalProcessorInformationEx(RelationProcessorCore, nullptr, &len);
    if (len == 0) return;

    std::vector<std::byte> buf(len);
    auto* base = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buf.data());
    if (!GetLogicalProcessorInformationEx(RelationProcessorCore, base, &len)) return;

    DWORD off = 0;
    while (off < len) {
        auto* info = reinterpret_cast<SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(buf.data() + off);
        if (info->Relationship == RelationProcessorCore) {
            const PROCESSOR_RELATIONSHIP& proc = info->Processor;
            const int eff = (int)proc.EfficiencyClass;
            if (eff > out.maxClass) out.maxClass = eff;
            for (WORD g = 0; g < proc.GroupCount; ++g) {
                const GROUP_AFFINITY& ga = proc.GroupMask[g];
                for (unsigned bit = 0; bit < 64; ++bit)
                    if ((uint64_t)ga.Mask & (uint64_t(1) << bit))
                        out.efficiencyClass[CpuMask::Make(ga.Group, bit)] = eff;
            }
        }
        off += info->Size;
    }
}

}} // namespace JLib::topology
