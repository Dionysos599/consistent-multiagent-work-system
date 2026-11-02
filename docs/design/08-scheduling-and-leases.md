# 08 – Task graph, scheduler and path leases

## What decides whether two tasks may run together

Each task declares a *scope*: the files or folders it expects to change. A folder covers everything
below it; an empty scope means "unknown" and counts as the whole project, so it runs alone.

- Tasks with disjoint scopes and no dependency run in parallel.
- Tasks with overlapping scopes never run together. The one declared first goes first, and the later one
  forks from a main that already contains it, so it sees the earlier result instead of conflicting with it.
- `depends_on` adds explicit edges. If a dependency fails or is rejected, its dependents are skipped;
  tasks that merely overlapped it still run.

This is cheaper than resolving conflicts afterwards, and it is also what makes weak models usable:
the planner's job is to keep scopes narrow.

## Path leases

Scope in a plan is a prediction. A lease is the runtime claim: `(agent, paths, expires)`, all kept in
one store key and changed by compare-and-set. Properties:

- of racing overlapping claims exactly one is granted (the store serializes the CAS);
- a lease expires, so a crashed agent cannot block the project forever; `renew` extends a live one;
- claiming again as the same agent refreshes it, which also makes a retry after a lost reply harmless;
- leases are advisory. Nothing stops a write; the point is that the scheduler does not start a task whose
  files are claimed, including by another orchestrator process sharing the store.

After a task, `audit` compares the files it actually changed with its declared scope. Out-of-scope paths
are logged (bounded log in the store); a stray path that lies inside another agent's live lease is a
*collision*, the strongest early sign of a coming conflict. The merge queue uses this in the next stage.

## Scheduler

For each ready task: take the lease, fork a branch from current main, run the worker, audit, land, release
(the release is in a `finally`, so a crashing worker frees its files). A worker exception becomes a failed
outcome, never a stalled run. If a lease is held by someone outside the run, the task waits, and is
skipped after `patience` seconds.

Landing is the plain `Repository.submit` for now; the merge queue replaces it.

## Limits

- scopes are path prefixes; two tasks touching different functions in one file are serialized.
- the planner's scopes can be wrong; the audit reports it but does not stop the task.
- lease table is a single key: fine for tens of agents, a bottleneck for thousands.
- one process schedules; several schedulers on one store are protected by leases only.

## How this was tested

Beyond the functional tests: a reference-model test for the lease table (random acquire / renew / release / audit /
clock jumps compared step by step with a dict model, 40 seeds), a threaded exclusion test (no two threads ever
hold overlapping leases), injected CAS races for each lease operation, a scheduler chaos test (random crashes,
failed workers, rejected landings, leases expiring mid-task and an outsider competing for files; 25 seeds,
checking that exactly the landed tasks' edits are in main and no overlapping tasks ever ran together), fuzzing of
the tool layer, the agent loop, the planner parser and the response parser, and a run on the real cluster
with replicas killed. The fuzzers found three crashes (non-object tool arguments, non-string argument values,
malformed response bodies), now fixed. `workspace/tools/mutate.py` changes one operator at a time: 391 of
408 mutants in the agent and lease code are caught; the 17 survivors are dataclass flags, memoization,
unreachable guards and an optimization that skips redundant writes, all behaviour-preserving.
