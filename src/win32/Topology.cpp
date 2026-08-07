// Windows topology acquisition. Moved verbatim (behaviour-wise) out of TaskScheduler.cpp so the
// Linux equivalent could exist beside it rather than inside an #ifdef.
//
// Reads REAL hardware topology rather than assuming the sequential affinity scheme (worker qIndex i
// pinned to logical CPU i+1, main on 0). That mapping tells you what was ASKED of the OS, not what
// the hardware looks like: adjacent logical CPU numbers being SMT or cache neighbours is a common
// convention and never a guarantee.
//
// Limitation, unchanged from the original: processor GROUP 0 only. Fine for the vast majority of
// desktop and workstation hardware (<=64 logical CPUs); a true multi-group machine would need
// GROUP_AFFINITY.Group handled here and wider masks in Info.
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
                                     std::vector<uint64_t>& outMasks) {
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
            if (relation == RelationCache) {
                // One GROUP_AFFINITY per cache instance. Every level (L1/L2/L3) arrives through
                // this same query; the caller identifies the last-level one by preferring the
                // widest group, so no filtering on Cache.Level is needed here.
                const CACHE_RELATIONSHIP& cache = info->Cache;
                if (cache.GroupMask.Group == 0)
                    outMasks.push_back((uint64_t)cache.GroupMask.Mask);
            } else {
                const PROCESSOR_RELATIONSHIP& proc = info->Processor;
                for (WORD g = 0; g < proc.GroupCount; ++g) {
                    if (proc.GroupMask[g].Group == 0)
                        outMasks.push_back((uint64_t)proc.GroupMask[g].Mask);
                }
            }
        }
        offset += info->Size;
    }
    return true;
}

void Query(Info& out) {
    out.haveCores = GetGroupMasksForRelation(RelationProcessorCore, out.coreMasks);
    out.haveCache = GetGroupMasksForRelation(RelationCache,         out.cacheMasks);

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
                if (proc.GroupMask[g].Group != 0) continue;   // group 0 only, see the note above
                const uint64_t mask = (uint64_t)proc.GroupMask[g].Mask;
                for (int cpu = 0; cpu < 64; ++cpu)
                    if (mask & (uint64_t(1) << cpu)) out.efficiencyClass[cpu] = eff;
            }
        }
        off += info->Size;
    }
}

}} // namespace JLib::topology
