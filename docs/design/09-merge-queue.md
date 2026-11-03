# 09 – Merge queue and conflict escalation

## Why a queue

The scheduler decides who may *work* at the same time. The queue decides what may *land*. Two changes
that each pass the tests can break together (both define `helper()` in different files; text merge accepts
it), so validation has to run on the merged result, and main must only move to states that passed.

`Repository.submit(validate=...)` is the hook: after merging and before the compare-and-set of main it hands
the candidate commit to a validator; a refusal returns `rejected` and main stays where it was. If main moves
meanwhile and the merge is redone, the validator runs again on the new candidate. What it approved is exactly
what lands. `MergeQueue` supplies the validator: a conflict-marker guard (always on) plus a pluggable
`Validator`, for example `CommandValidator([python3, -m, unittest])`, which writes the candidate tree to a
folder and runs the command there.

The lock inside `MergeQueue` only serializes validate-and-land inside one process, which keeps slow
validators from racing each other. Correctness does not depend on it: the last step is the CAS, so a second
queue or a direct submit costs a retry, never a corrupt main.

## The chain

Each step is cheaper and more automatic than the next; each failure is recorded as a `Step`.

1. **auto**: the ordinary three-way merge in `submit`. Clean merges, validated, land.
2. **merger agent** (on `conflict` or `rejected`): the branch is merged with main with conflict markers
   kept (a merge commit on the agent's own branch), and a merger worker is given the intent of both sides
   (the task description, the commit messages that landed meanwhile, the validator output if validation
   failed) and the conflicted paths. Whatever it writes goes back through the same validation, so a merger
   that leaves markers or breaks the tests is refused like anyone else.
3. **redo** (also the first resort on `stale`, because a merger cannot repair a decision made from old
   information): the task runs again on a fresh branch from the new main, with the upstream diff and the reason
   in its context. Up to `max_redos` times.
4. **human**: a durable record in the store (`mq/escalations`: task, branch, reason, conflicts, the steps
   taken) and no further automatic action. The branch is left as it is, markers included, for a person to open
   (`cli escalations`).

## What the tests establish

- main only moves to candidates a validator approved, under 8 threads landing at once (checked against the
  recorded CAS history), and no accepted update is lost (counter equals landed tasks);
- 15 random concurrent-edit runs through all stages (auto, merger, redo, human), with a deterministic union
  merger, check no markers reach main, original content is intact, each landed task's line appears exactly once and
  each escalated task's line not at all;
- each stage's record is exact; contention, a conflict that disappears before the merger runs, oversized upstream
  diffs and racing escalation appends are covered individually;
- on the real cluster: a conflict resolved by a merger, with a real command gating main.

Mutation sweep over the queue module: 140 of 146 mutants caught; the other six are a dataclass flag, JSON key
ordering, an `or ""` on process output, an `or tuple()` on a tuple and two `return None` statements that
mutate to themselves.

## Limits

- the queue is a library, not a service: it lives in the process that calls `land`. Several processes are safe
  but do not share a visible queue order.
- validation re-reads every file of the candidate; fine for repositories of thousands of files, wasteful beyond that.
- the merger sees only the conflicted paths it is told about; semantic conflicts that need other files are left
  to the validator to find.
- a redo replaces the agent's work; it does not try to salvage the partial result.
