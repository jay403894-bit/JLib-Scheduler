---------------------------- MODULE fiberresume_model ----------------------------
(* SPDX-License-Identifier: BSD-3-Clause                                            *)
(* Copyright (c) 2026 Joshua Makler. Part of JLib -- see LICENSE at the repo root.  *)
(*                                                                                    *)
(* A MODEL of the Fiber::ResumeQueueless() CAS race: a fiber publishes WANTS_SUSPEND *)
(* and switches away; concurrently, a worker's own dispatch code tries to CAS that  *)
(* WANTS_SUSPEND to SUSPENDED, while any other thread calling Fiber::Resume() (or,  *)
(* batched, Event::SignalAll -> RequeueResumedBatch) tries to CAS the SAME field to *)
(* SUSPEND_SIGNALED. Whichever CAS wins decides the outcome; the model checks that  *)
(* BOTH outcomes correctly return the fiber to READY -- neither the worker's own    *)
(* losing branch nor the racer's winning one can strand it in WANTS_SUSPEND.        *)
(*                                                                                    *)
(* NOT the same thing deque_model.c / event_model.c / fiberwait_model.c /           *)
(* sleepwake_model.c already check, despite the shared "fiber wait/resume" theme:   *)
(*   - sleepwake_model.c is the OS-thread park/wake predicate (workerState).        *)
(*   - fiberwait_model.c is SchedulerMutex/Semaphore/CondVar's queue-then-mark-     *)
(*     parkable ORDERING (the bug that shipped in 1.3.4 -- publish-before-parkable  *)
(*     let a Resume() land in the window and discard the wake).                     *)
(*   - event_model.c is the waiter-STACK data structure (lost/duplicate entries).   *)
(*   - THIS model is the FiberStatus CAS race itself, independent of any of the     *)
(*     above -- the thing all three of them ultimately have to get right underneath *)
(*     whatever queue/ordering discipline sits on top of it.                        *)
(*                                                                                    *)
(* A DIFFERENT TOOL, NOT A STRONGER ONE. The four models above use GenMC, which     *)
(* explores the actual compiled C code under the real C11 memory model -- there is  *)
(* no "did I translate this faithfully" gap. This is TLA+/PlusCal: a hand-written   *)
(* abstraction, checked by TLC's explicit-state search. It verifies the PROTOCOL is *)
(* sound (the CAS race resolves correctly under every interleaving TLC can reach at *)
(* the bounds below), not that the C++ correctly implements that protocol, and it   *)
(* does not model real memory orderings -- every transition here is treated as a    *)
(* clean atomic CAS. Read a clean run as design-level evidence, not as GenMC-grade  *)
(* proof of the shipped code.                                                       *)
(*                                                                                    *)
(* HOW TO REPRODUCE: TLA+ Toolbox (github.com/tlaplus/tlaplus/releases) or the      *)
(* standalone tla2tools.jar. Open this file, translate the PlusCal (Ctrl+T -- the   *)
(* translation below this comment is already current), then TLC Model Checker ->   *)
(* New Model. Behavior spec: "Temporal formula", value `Spec` (NOT the separate     *)
(* Init/Next fields -- Spec is what carries the WF_vars fairness both process types *)
(* declare, and ProgressInvariant is meaningless without it). Constants: MaxFibers, *)
(* MaxWorkers, MaxWakers. StateInvariant goes under Invariants; ProgressInvariant   *)
(* MUST go under Properties, not Invariants -- it uses ~>, a temporal operator, and *)
(* TLC rejects it with "not a state predicate" if it lands in the wrong list.       *)
(*                                                                                    *)
(* RESULTS: MaxFibers=3, MaxWorkers=2, MaxWakers=2 -- 828 distinct states, both      *)
(* StateInvariant and ProgressInvariant hold, no error. Re-run clean at MaxFibers=5. *)
(* Built and iterated 8-21 (DeepSeek's first draft could not translate -- two       *)
(* PlusCal legality errors -- and even once it translated, its single-label Worker  *)
(* process made the race it existed to check structurally unreachable; both fixed   *)
(* before the first real run).                                                      *)
EXTENDS Naturals, Sequences

CONSTANTS MaxFibers, MaxWorkers, MaxWakers

(*--algorithm fiber_state_machine
variables
    fibers = [i \in 1..MaxFibers |-> "READY"],
    woken_batch = << >>,
    pending_wakes = 0;

define
    IsReady(i)            == fibers[i] = "READY"
    IsRunning(i)           == fibers[i] = "RUNNING"
    IsWantsYield(i)        == fibers[i] = "WANTS_YIELD"
    IsWantsSuspend(i)      == fibers[i] = "WANTS_SUSPEND"
    IsSuspended(i)         == fibers[i] = "SUSPENDED"
    IsSuspendedSignaled(i) == fibers[i] = "SUSPEND_SIGNALED"
    IsDead(i)              == fibers[i] = "DEAD"

    (* Safety: every fiber is always in exactly one recognized state. *)
    StateInvariant ==
        \A i \in 1..MaxFibers:
            fibers[i] \in {"READY", "RUNNING", "WANTS_YIELD", "WANTS_SUSPEND",
                           "SUSPENDED", "SUSPEND_SIGNALED", "DEAD"}

    (* Liveness: a fiber that asked to suspend eventually leaves WANTS_SUSPEND (via the
       worker's own CAS to SUSPENDED, or a racing Waker's CAS to SUSPEND_SIGNALED). Needs
       fairness on both process types below or this reports a meaningless counterexample. *)
    ProgressInvariant ==
        \A i \in 1..MaxFibers:
            IsWantsSuspend(i) ~> ~IsWantsSuspend(i)
end define;

(* Fiber execution -- runs on a worker thread. THREE labels, not one: Steal picks up a READY
   fiber; RunFiber lets it declare WANTS_YIELD / WANTS_SUSPEND / DEAD; Resolve is a SEPARATE
   step that reacts to whatever fibers[current_fiber] holds BY THEN -- which may no longer be
   WANTS_SUSPEND, because the label boundary between RunFiber and Resolve is exactly the window
   a concurrent Waker can race into and flip it to SUSPEND_SIGNALED first. That window is the
   real race (see Fiber.cpp::ResumeQueueless) -- collapsing it into one atomic step, as the
   original draft did, makes the race structurally impossible to explore. *)
fair process Worker \in 1..MaxWorkers
variable current_fiber = 0;
begin
    Steal:
        while TRUE do
            either
                with i \in 1..MaxFibers do
                    await IsReady(i);
                    current_fiber := i;
                    fibers[i] := "RUNNING";
                end with;
            or
                skip;
            end either;
        RunFiber:
            if current_fiber > 0 then
                either
                    fibers[current_fiber] := "WANTS_YIELD";
                or
                    fibers[current_fiber] := "WANTS_SUSPEND";
                or
                    fibers[current_fiber] := "DEAD";
                end either;
            Resolve:
                if fibers[current_fiber] = "WANTS_YIELD" then
                    fibers[current_fiber] := "READY";
                    current_fiber := 0;
                elsif fibers[current_fiber] = "WANTS_SUSPEND" then
                    (* We still hold the CAS: nobody raced us. *)
                    fibers[current_fiber] := "SUSPENDED";
                    current_fiber := 0;
                elsif fibers[current_fiber] = "SUSPEND_SIGNALED" then
                    (* Lost the race: a Waker got here first while we were "saving context".
                       Real code: the worker itself flips this to READY and re-queues --
                       modeled here rather than in Waker, to match Thread.cpp exactly. *)
                    fibers[current_fiber] := "READY";
                    current_fiber := 0;
                else
                    current_fiber := 0;
                end if;
            end if;
        end while;
end process;

(* Models BOTH real wake paths through their shared primitive, ResumeQueueless(): a plain
   Fiber::Resume() (single wake) and Event::SignalAll's batching into RequeueResumedBatch. Kept
   as one process type rather than two, because the real code has one primitive underneath both
   -- splitting it was adding a distinction the C++ doesn't actually have. *)
fair process Waker \in 1..MaxWakers
variable current_wake = 0;
begin
    WakeLoop:
        while TRUE do
            (* No label may appear inside a `with` -- the pick has to resolve in one step.
               So the with-block only decides WHICH fiber (if any) got woken this pass, and
               stashes it in current_wake; the batching bookkeeping happens in Batch/FlushCheck
               below, as separate labeled steps, so pending_wakes is never assigned twice in
               the same step (that was the ORIGINAL error). *)
            with i \in 1..MaxFibers do
                if IsSuspended(i) then
                    fibers[i] := "READY";
                    current_wake := i;
                elsif IsWantsSuspend(i) then
                    (* The other half of the race: if this lands before the Worker's Resolve
                       step reads fibers[current_fiber], the worker sees SUSPEND_SIGNALED
                       instead of WANTS_SUSPEND and takes the "lost the race" branch above. *)
                    fibers[i] := "SUSPEND_SIGNALED";
                    current_wake := 0;
                else
                    current_wake := 0;
                end if;
            end with;
        Batch:
            if current_wake > 0 then
                woken_batch := Append(woken_batch, current_wake);
                pending_wakes := pending_wakes + 1;
            end if;
        FlushCheck:
            if pending_wakes >= 4 then
                pending_wakes := 0;
                woken_batch := << >>;
            end if;
        end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "1a28e397" /\ chksum(tla) = "e0881b45")
VARIABLES pc, fibers, woken_batch, pending_wakes

(* define statement *)
IsReady(i)            == fibers[i] = "READY"
IsRunning(i)           == fibers[i] = "RUNNING"
IsWantsYield(i)        == fibers[i] = "WANTS_YIELD"
IsWantsSuspend(i)      == fibers[i] = "WANTS_SUSPEND"
IsSuspended(i)         == fibers[i] = "SUSPENDED"
IsSuspendedSignaled(i) == fibers[i] = "SUSPEND_SIGNALED"
IsDead(i)              == fibers[i] = "DEAD"


StateInvariant ==
    \A i \in 1..MaxFibers:
        fibers[i] \in {"READY", "RUNNING", "WANTS_YIELD", "WANTS_SUSPEND",
                       "SUSPENDED", "SUSPEND_SIGNALED", "DEAD"}




ProgressInvariant ==
    \A i \in 1..MaxFibers:
        IsWantsSuspend(i) ~> ~IsWantsSuspend(i)

VARIABLES current_fiber, current_wake

vars == << pc, fibers, woken_batch, pending_wakes, current_fiber,
           current_wake >>

ProcSet == (1..MaxWorkers) \cup (1..MaxWakers)

Init == (* Global variables *)
        /\ fibers = [i \in 1..MaxFibers |-> "READY"]
        /\ woken_batch = << >>
        /\ pending_wakes = 0
        (* Process Worker *)
        /\ current_fiber = [self \in 1..MaxWorkers |-> 0]
        (* Process Waker *)
        /\ current_wake = [self \in 1..MaxWakers |-> 0]
        /\ pc = [self \in ProcSet |-> CASE self \in 1..MaxWorkers -> "Steal"
                                        [] self \in 1..MaxWakers -> "WakeLoop"]

Steal(self) == /\ pc[self] = "Steal"
               /\ \/ /\ \E i \in 1..MaxFibers:
                          /\ IsReady(i)
                          /\ current_fiber' = [current_fiber EXCEPT ![self] = i]
                          /\ fibers' = [fibers EXCEPT ![i] = "RUNNING"]
                  \/ /\ TRUE
                     /\ UNCHANGED <<fibers, current_fiber>>
               /\ pc' = [pc EXCEPT ![self] = "RunFiber"]
               /\ UNCHANGED << woken_batch, pending_wakes, current_wake >>

RunFiber(self) == /\ pc[self] = "RunFiber"
                  /\ IF current_fiber[self] > 0
                        THEN /\ \/ /\ fibers' = [fibers EXCEPT ![current_fiber[self]] = "WANTS_YIELD"]
                                \/ /\ fibers' = [fibers EXCEPT ![current_fiber[self]] = "WANTS_SUSPEND"]
                                \/ /\ fibers' = [fibers EXCEPT ![current_fiber[self]] = "DEAD"]
                             /\ pc' = [pc EXCEPT ![self] = "Resolve"]
                        ELSE /\ pc' = [pc EXCEPT ![self] = "Steal"]
                             /\ UNCHANGED fibers
                  /\ UNCHANGED << woken_batch, pending_wakes, current_fiber,
                                  current_wake >>

Resolve(self) == /\ pc[self] = "Resolve"
                 /\ IF fibers[current_fiber[self]] = "WANTS_YIELD"
                       THEN /\ fibers' = [fibers EXCEPT ![current_fiber[self]] = "READY"]
                            /\ current_fiber' = [current_fiber EXCEPT ![self] = 0]
                       ELSE /\ IF fibers[current_fiber[self]] = "WANTS_SUSPEND"
                                  THEN /\ fibers' = [fibers EXCEPT ![current_fiber[self]] = "SUSPENDED"]
                                       /\ current_fiber' = [current_fiber EXCEPT ![self] = 0]
                                  ELSE /\ IF fibers[current_fiber[self]] = "SUSPEND_SIGNALED"
                                             THEN /\ fibers' = [fibers EXCEPT ![current_fiber[self]] = "READY"]
                                                  /\ current_fiber' = [current_fiber EXCEPT ![self] = 0]
                                             ELSE /\ current_fiber' = [current_fiber EXCEPT ![self] = 0]
                                                  /\ UNCHANGED fibers
                 /\ pc' = [pc EXCEPT ![self] = "Steal"]
                 /\ UNCHANGED << woken_batch, pending_wakes, current_wake >>

Worker(self) == Steal(self) \/ RunFiber(self) \/ Resolve(self)

WakeLoop(self) == /\ pc[self] = "WakeLoop"
                  /\ \E i \in 1..MaxFibers:
                       IF IsSuspended(i)
                          THEN /\ fibers' = [fibers EXCEPT ![i] = "READY"]
                               /\ current_wake' = [current_wake EXCEPT ![self] = i]
                          ELSE /\ IF IsWantsSuspend(i)
                                     THEN /\ fibers' = [fibers EXCEPT ![i] = "SUSPEND_SIGNALED"]
                                          /\ current_wake' = [current_wake EXCEPT ![self] = 0]
                                     ELSE /\ current_wake' = [current_wake EXCEPT ![self] = 0]
                                          /\ UNCHANGED fibers
                  /\ pc' = [pc EXCEPT ![self] = "Batch"]
                  /\ UNCHANGED << woken_batch, pending_wakes, current_fiber >>

Batch(self) == /\ pc[self] = "Batch"
               /\ IF current_wake[self] > 0
                     THEN /\ woken_batch' = Append(woken_batch, current_wake[self])
                          /\ pending_wakes' = pending_wakes + 1
                     ELSE /\ TRUE
                          /\ UNCHANGED << woken_batch, pending_wakes >>
               /\ pc' = [pc EXCEPT ![self] = "FlushCheck"]
               /\ UNCHANGED << fibers, current_fiber, current_wake >>

FlushCheck(self) == /\ pc[self] = "FlushCheck"
                    /\ IF pending_wakes >= 4
                          THEN /\ pending_wakes' = 0
                               /\ woken_batch' = << >>
                          ELSE /\ TRUE
                               /\ UNCHANGED << woken_batch, pending_wakes >>
                    /\ pc' = [pc EXCEPT ![self] = "WakeLoop"]
                    /\ UNCHANGED << fibers, current_fiber, current_wake >>

Waker(self) == WakeLoop(self) \/ Batch(self) \/ FlushCheck(self)

Next == (\E self \in 1..MaxWorkers: Worker(self))
           \/ (\E self \in 1..MaxWakers: Waker(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in 1..MaxWorkers : WF_vars(Worker(self))
        /\ \A self \in 1..MaxWakers : WF_vars(Waker(self))

\* END TRANSLATION
=============================================================================
