# `coro_hazard_test.cpp` — kept for the feature that was removed

This test covers **hazard protection across a `co_await`** — a `HazardGuard` held by a coroutine
frame through a suspend. That feature no longer exists, so the test does not build and is kept here
as the record of what it did, and as the starting point if it ever comes back.

**Coroutine *tasks* are unaffected and fully supported.** Two different features got conflated once
and that is what produced the registry; they are not the same thing:

- a coroutine **task** in the pool — resume, `TaskType::Coroutine`, all of it — is untouched;
- a **`HazardGuard` in a coroutine frame that outlives a suspend** is refused.

## Why the refusal, and why it is not a retreat

Cells are indexed by reader, and a coroutine frame has **no dense stable index** — frames are not a
bounded pool, unlike fibers (`poolIndex`), workers and the reserved external block. Covering that
took an external record registry with its own `FREE -> LIVE -> RETIRED -> FREE` lifecycle, a
generation counter, and a grace period keyed on a scan **count**: reuse was gated on
`now > retiredAt + 1` rather than on *no scan begun at or before `retiredAt` still being in flight*.

That grace could not be stated in one testable sentence, and **nothing exercised record reuse** — all
18 assertions covered protection, none covered a recycled `RecordId`. This codebase has already
shipped one generation-wraparound bug that a test looked straight past. Six of the six bugs found in
`Hazard.cpp` were in that adaptation layer; Michael's algorithm produced none.

**Paper readers do not come back from the dead mid-scan.** Refusing is what copying the paper
actually looks like — the mistake was inventing a second reader type to paper over the fact that
worker cells stop protecting at the first `co_await`.

**There is no downgrade, and that is the feature.** Worker or fiber cells are correct until the
frame's first suspend and a use-after-free after it, so a fallback would pass every test that does
not suspend inside a protected section — exactly the case it would exist for.

**Use counted epochs for a reader that suspends.** That is what the counted scheme was built for, it
is verified, and epochs remain this engine's reclamation. Hazard pointers are the extra.

## If it comes back

Three options were on the table; the shipped one is the first.

1. **Refuse** — what shipped. No registry.
2. **Copy Michael properly** — cells live in the `HazardGuard` object itself, the guard goes on a
   global list `Scan` walks, unlinked in `~HazardGuard` only after its cells are null. *The guard is
   the reader.* Capacity is live guards, not 1024 recycled ids, so there is no reuse to grace.
3. **Keep a pool** — then it is original work, not the paper, and it needs `completedScanSeq` (or an
   in-flight scan count), a reuse test, and a model of `FREE/LIVE/RETIRED` + scan start/end.

Option 2 is the honest small one. Option 3 is a project.
