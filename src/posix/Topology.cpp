// Linux topology acquisition, from sysfs. The Windows equivalent calls one API; here the same
// information is text under /sys/devices/system/cpu/, so the two share nothing and live in
// separate files rather than behind an #ifdef.
//
// What is read:
//   topology/thread_siblings_list   -> the logical CPUs on one physical core (SMT siblings)
//   cache/index*/shared_cpu_list    -> the logical CPUs sharing one cache instance
//
// Both are "CPU list" format: comma-separated singles and ranges, e.g. "0-3,8,12-15".
//
// No hwloc dependency on purpose. hwloc is the portable answer and would be the single heaviest
// dependency in the project, for about a hundred lines of text parsing -- off-brand for a library
// whose pitch is owning its own stack.
//
// Group 0 / first 64 logical CPUs only, matching the Windows implementation's limitation.
#include "../../include/Topology.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>

namespace JLib { namespace topology {

// Read a whole small sysfs file. Returns false if it does not exist -- normal, not an error:
// cache/index3 simply may not be present, and thread_siblings_list is absent in some containers.
static bool ReadFileText(const char* path, std::string& out) {
    FILE* f = std::fopen(path, "rb");
    if (!f) return false;
    char buf[512];
    size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
    std::fclose(f);
    buf[n] = '\0';
    out.assign(buf, n);
    return true;
}

// "0-3,8,12-15" -> bitmask. CPUs at or above 64 are ignored, matching the group-0 limitation.
static uint64_t ParseCpuList(const std::string& s) {
    uint64_t mask = 0;
    const char* p = s.c_str();
    while (*p) {
        while (*p == ',' || *p == ' ' || *p == '\n') ++p;
        if (!*p) break;
        char* end = nullptr;
        long lo = std::strtol(p, &end, 10);
        if (end == p) break;                       // not a number: stop rather than spin
        p = end;
        long hi = lo;
        if (*p == '-') {
            ++p;
            hi = std::strtol(p, &end, 10);
            if (end == p) break;
            p = end;
        }
        for (long c = lo; c <= hi && c < 64; ++c)
            if (c >= 0) mask |= (uint64_t(1) << c);
    }
    return mask;
}

static int OnlineCpuCount() {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) n = 1;
    return (int)std::min<long>(n, 64);
}

void Query(Info& out) {
    const int nCpu = OnlineCpuCount();
    char path[256];
    std::string text;

    // --- SMT sibling groups, one mask per physical core -----------------------------------------
    // Every CPU on a core reports the SAME sibling list, so the masks are deduplicated; the result
    // is one entry per physical core, which is exactly what RelationProcessorCore yields.
    for (int cpu = 0; cpu < nCpu; ++cpu) {
        std::snprintf(path, sizeof(path),
                      "/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list", cpu);
        if (!ReadFileText(path, text)) continue;
        const uint64_t m = ParseCpuList(text);
        if (m && std::find(out.coreMasks.begin(), out.coreMasks.end(), m) == out.coreMasks.end())
            out.coreMasks.push_back(m);
    }
    out.haveCores = !out.coreMasks.empty();

    // --- Cache instances, all levels ------------------------------------------------------------
    // Collected without filtering by level, exactly like the Windows RelationCache query: the
    // caller identifies the last-level cache by preferring the widest group, so threading a level
    // field through here would buy nothing.
    for (int cpu = 0; cpu < nCpu; ++cpu) {
        for (int idx = 0; idx < 8; ++idx) {
            std::snprintf(path, sizeof(path),
                          "/sys/devices/system/cpu/cpu%d/cache/index%d/shared_cpu_list", cpu, idx);
            if (!ReadFileText(path, text)) continue;
            const uint64_t m = ParseCpuList(text);
            if (m && std::find(out.cacheMasks.begin(), out.cacheMasks.end(), m) == out.cacheMasks.end())
                out.cacheMasks.push_back(m);
        }
    }
    out.haveCache = !out.cacheMasks.empty();

    // --- P/E classes: DELIBERATELY NOT IMPLEMENTED ---------------------------------------------
    // Left as all-unknown, which the caller reads as "every core is equal". That is correct for a
    // non-hybrid CPU and harmless on a hybrid one, because class-based routing is opt-in and
    // currently dormant -- no shipped caller requests CorePref::P or ::E.
    //
    // Doing it properly is genuinely messy and would be guesswork without hardware to verify on:
    // Intel hybrid exposes /sys/devices/cpu_core and /sys/devices/cpu_atom (a driver detail, not a
    // topology attribute), while ARM big.LITTLE uses cpu%d/cpu_capacity with a normalised scale.
    // Neither survives WSL's virtualised CPU view, so it cannot be tested here. Implement it when
    // there is a profiled caller AND real hardware to check against, not before.
}

}} // namespace JLib::topology
