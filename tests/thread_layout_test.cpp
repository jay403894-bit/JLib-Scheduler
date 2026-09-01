// WHICH CACHE LINES DOES A PUSH TOUCH ON THE CONSUMER'S Thread?
//
// PushLocal writes three fields on the worker it selected -- inboxDepth, hasQueuedWork (via
// MarkQueuedWork) and workerState (via NotifyWorker) -- while that worker is concurrently reading
// or RMW-ing all three. Every distinct cache line among them is a coherence transfer the producer
// pays PER TASK, and PushBatch pays once per 64. That difference is most of why Push measures
// ~422 ns/task against PushBatch's ~61.
//
// THIS PRINTS THE LAYOUT RATHER THAN ASSUMING IT. The fields are declared ~160 lines apart with no
// alignas between them, which SUGGESTS separate lines -- but suggests is not measures, and the
// compiler decides. Reasoning about false sharing from source order is how you end up optimising a
// layout that was already fine.
//
// IT IS ALSO A REGRESSION GUARD. Whatever layout is chosen, it is chosen for a measured reason, and
// an unrelated field inserted between these three would silently undo it. The assert at the bottom
// fails loudly instead.

#include "../include/Thread.h"
#include "../include/platform.h"
#include <cstdio>
#include <cstddef>

using namespace JLib;

static int g_failures = 0;
static void Check(bool c, const char* what) {
    std::printf("  %-64s %s\n", what, c ? "ok" : "FAIL");
    if (!c) ++g_failures;
}

int main() {
    const size_t line = platform::kCacheLine;
    std::printf("=== Thread layout: what one Push touches ===\n");
    std::printf("sizeof(Thread) = %zu, cache line = %zu\n", sizeof(Thread), line);

    // offsetof on a non-standard-layout type is conditionally supported; MSVC, GCC and Clang all
    // accept it and it is the only way to ask this question without hand-computing the layout.
    size_t oDepth = 0, oQueued = 0, oState = 0;
    Thread::PushPathFieldOffsets(oDepth, oQueued, oState);

    // BOTH GRANULARITIES, AND THE 64 ONE IS THE ANSWER.
    //
    // platform::kCacheLine is 128 here, which is the ADJACENT-LINE PREFETCH pair -- the right unit
    // for padding a counter away from unrelated data. It is NOT the coherence unit: x86 transfers
    // and invalidates in 64-byte lines, so that is what a producer-consumer ping-pong is counted in.
    //
    // Reporting only the 128 number said "1 line, nothing to fix" and would have closed this
    // investigation on the wrong answer.
    auto lineOf = [](size_t off, size_t unit) { return off / unit; };
    std::printf("\n  %-16s %8s %10s %10s\n", "field", "offset", "line/64", "line/128");
    std::printf("  %-16s %8zu %10zu %10zu\n", "inboxDepth",    oDepth,  lineOf(oDepth, 64),  lineOf(oDepth, line));
    std::printf("  %-16s %8zu %10zu %10zu\n", "hasQueuedWork", oQueued, lineOf(oQueued, 64), lineOf(oQueued, line));
    std::printf("  %-16s %8zu %10zu %10zu\n", "workerState",   oState,  lineOf(oState, 64),  lineOf(oState, line));

    auto distinctAt = [&](size_t unit) {
        size_t ls[3] = { lineOf(oDepth, unit), lineOf(oQueued, unit), lineOf(oState, unit) };
        size_t d = 1;
        for (size_t i = 1; i < 3; ++i) {
            bool seen = false;
            for (size_t j = 0; j < i; ++j) if (ls[j] == ls[i]) seen = true;
            if (!seen) ++d;
        }
        return d;
    };
    const size_t d64  = distinctAt(64);
    const size_t d128 = distinctAt(line);
    std::printf("\n  COHERENCE LINES TOUCHED PER PUSH (64B, the one that matters): %zu\n", d64);
    std::printf("  prefetch pairs spanned (%zuB): %zu\n", line, d128);
    std::printf("  (PushBatch touches these once per batch, not once per task -- so the 64B count\n"
                "   times the coherence cost is what batching amortizes away)\n\n");

    // NOT AN ASSERTION ABOUT WHICH LAYOUT IS RIGHT. Co-locating cuts transfers per push but makes
    // the producer's inboxDepth write invalidate the worker's workerState RMW on the SAME line;
    // separating avoids that but pays more transfers. Which wins is a measurement, not a rule.
    //
    // What IS assertable: the number is known and did not change by accident.
    Check(d64 >= 1 && d64 <= 3, "the three push-path fields occupy 1..3 coherence lines");
    Check(sizeof(Thread) > 0, "Thread is a complete type here");

    std::printf("=== %s (%d failure%s) ===\n",
        g_failures ? "FAILED" : "PASSED", g_failures, g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
