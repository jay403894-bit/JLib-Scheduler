# Hazard pointers — PARKED, not shipped

**This directory is not built.** Nothing in `CMakeLists.txt` references it, the library does not
include it, and moving it here is the whole point: the scheme works and is well tested, but one part
of it is not, and it should not ship until that changes.

## Why it was parked

**The record lifecycle has no direct test.** Acquire -> LIVE -> RETIRED -> FREE with a bumped
generation, gated on `g_scanSeq` crossing a scan boundary, is the most intricate thing in the file.
Every existing assertion tests *protection*; none tests **record reuse** — no generation bump, no
ABA on a recycled `RecordId`, no grace-period boundary.

That is a specific worry rather than a general one, because this codebase has had exactly that bug
before: a cancel scope failed open after ~65k generation reuses (about 18 minutes at 60 fps), and
the test that should have caught it only checked `Valid()`, which proved nothing.

## What IS verified, so this is not being written off

18 assertions across four tests in `tests/`, all counting real frees, all dual-direction ("not freed
while named" AND "freed once released" — the first passes trivially if you never free anything):

- **Both textbook bugs a thread-indexed port has here**, each with a live kill switch:
  `ForceWorkerCellsForTest(true)` reintroduces worker-owned cells and the test asserts the node **is**
  freed under a sleeping reader, so the check cannot go quietly vacuous.
- **Coroutines**: protected across a `co_await`, record released on normal completion *and* on
  cancellation of a suspended frame.
- **Thread-exit orphans**: handed to the orphan store, swept by another thread, and an orphan still
  named by a live reader is **not** freed — so the sweep cannot have been made "safe" by freeing
  unconditionally.
- **Fatal paths** death-tested: cell-budget overflow aborts and names its handler; filling exactly to
  the budget does not.

Six real bugs were found and fixed in it, every one in the **adaptation** layer — cells indexed by
what migrates, the coroutine record, the `CurrentReader` fallthrough, a stale `currentRunningTask`,
the orphaned retire bag, `Scan`'s early return. Michael's core produced none.

## To bring it back

1. Write the record-reuse test: force many acquire/release cycles and prove the generation actually
   discriminates a recycled id from its previous owner.
2. Restore `Hazard.h` to `include/`, `Hazard.cpp` to `src/`, the four tests to `tests/`.
3. Re-add the three hooks: `HazardDomain::Instance().Init()` in `TaskScheduler::StartPool`, the
   retire-bag `Scan()` on the way into a worker's sleep, and the `Hazard.h` include in `Coroutine.h`.
4. Re-add the four test targets to `CMakeLists.txt`.

`DESIGN.md` here is the full design note. The dual-guard rule it describes — branch on
`TaskScheduler::CurrentTaskType()` between an `EpochGuard` and a `HazardGuard` — is not in effect
while this is parked; epochs are the only reclamation scheme the shipped library has.
