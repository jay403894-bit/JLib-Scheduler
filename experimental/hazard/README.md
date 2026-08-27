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

---

## If it comes back: the two designs, and the one mistake

**STABLE IS NOT DENSE, and conflating them is what killed the first attempt.** A C++20 frame does
not move — the promise address is stable from creation to `destroy`, and resume runs *that same
frame* on a possibly different worker. It is not densely *indexed*, which is what a flat table needs
and nothing else does. Ruling out a stable identity because it lacked density is the error that
produced the registry.

### The mistake, stated so it is not repeated

**The pin must not live on `Task::data`, or on whatever handle `ResumeCoroutine` was given.** That
object is not the frame. The trampoline is generic by design — for a nested `Lazy`, `Task::data`
holds the *parent's* frame — so typing the handle to reach `promise().something` reads a different
promise's storage. A nested `Lazy` is a different frame with its own promise.

### Design A — the guard is the reader

Cells live in the `HazardGuard` object itself, which is a local in the frame and therefore survives
the `co_await` for free. The guard goes on a global list `Scan` walks, and is unlinked in
`~HazardGuard` *after* its cells are null. Resume does not allocate a new reader; nothing hashes
"who resumed."

**There is no id to recycle**, so there is no reuse to grace — the property nobody could state
simply stops existing rather than getting a better statement. Capacity is live guards, not a fixed
pool of recycled ids.

**THE OPEN QUESTION, and it is the real one:** the guard list becomes the reclamation problem one
level up. Guard nodes are frame-local and die with the frame, so `Scan` can be walking a node that
is being destroyed. Two escapes are known: a lock around insert/remove/walk — cheap for `Scan`, but
`Protect` is hot — or nodes that are never freed, which is the pool again in a different hat.
Michael's registry is never-freed for precisely this reason. **Settle this before writing code.**

### Design B — cohort reclamation (do not mix it with A)

Membership is *"this job was born in generation G"*, not *"this stack is inside a section"*.

    +1  when the coro / fiber / native job is created in G
    -1  when it finishes (final_suspend, fiber returned, native returned)

**Suspend does nothing.** No frame pointer is ever read, and it does not matter who resumes. Objects
published in G are reclaimed once the counter reaches zero. A suspended coroutine is just an
outstanding job in G — the same as a parked fiber, the same as a thread blocked in a wait. *Count
it, do not map it.*

A stuck job means its generation never ends, so that generation leaks — not "reclamation frozen
forever", provided there are MANY generations (a frame index, a graph epoch, a counter of completed
cohorts). With one global G it degenerates into exactly the failure people attribute to epochs.

This is a reclamation scheme that sits beside epochs. It is not a hazard-pointer variant and should
not be built inside `Hazard.cpp`.

### Which, and when

Neither, until something actually needs hazard protection across a suspend. Nothing in-tree does:
no structure behind a `SchedulerMutex` recycles nodes yet, which is why the refusal costs nothing
today. **A is the smaller finish and the one that matches the paper's shape** — once its list
question is answered.
