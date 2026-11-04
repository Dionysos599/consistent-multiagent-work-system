# Reliability and the end-to-end orchestrator

Phase 4D. The earlier stages assume nothing dies. This one says what happens when a worker, an
orchestrator or a whole process is killed halfway through a run, and ties planner, scheduler and merge
queue into one resumable call.

## What is lost in a crash

An agent's edits live in the workspace overlay (memory) until committed. Its conversation lives in the
worker. Its claim on files is a lease in the store. A kill therefore loses the overlay and the
conversation, and leaves a lease behind. Everything else (branch commits, refs, main) is in the store and
is safe.

## Numbered tool calls and checkpoints (`agents/session.py`)

* Every tool call carries a sequence number. Call *n* runs once. Asking for *n* again returns the recorded
  result instead of running it a second time; a number that skips ahead is refused. This is the
  `(agent_id, seq)` at-most-once rule from the RPC layer applied to tool calls, and it is what lets a
  remote agent (the MCP stage) retry a request whose reply was lost without editing a file twice.
  `replace_in_file` is the case that matters: run twice, the second run fails; with the number, the
  retry gets the first run's answer.
* `checkpoint` commits the overlay to the agent's branch and writes one document
  (`session/<agent>`): sequence reached, commit, the caller's state (the conversation), and the last 20
  results. The commit happens first. A crash between the two leaves a commit newer than the record; on
  reopening, the branch is put back on the recorded commit, because the record says what the agent knew
  when it made that commit and the commit says more.
* Calls after the last checkpoint died with the overlay and are made again with the same numbers.
* If the record does not belong to the branch (its commit is not between the fork base and the tip, or is
  missing) it is ignored and the worker starts its conversation over on the branch as it is.
* `LoopWorker(checkpoint_every=n)` uses this: the conversation, step counter, nudge counter, token
  counts and the set of touched paths are recorded every *n* tool calls, and a finished task records
  its result, so running it again returns that result without asking the model anything.
  It is off by default; `cli orchestrate` turns it on.

## Task claims (`claims.py`)

One document per run holds, for every task: state (claimed, free, completed), owner, epoch, expiry and
the recorded outcome of a completed task.

* A claim is granted if the task is free, expired, or already ours; otherwise `busy`. A completed task
  answers `completed` with its outcome, to every claimant, forever.
* Takeover after expiry bumps the **epoch**. The epoch is a fencing token: `renew`, `complete` and
  `release` must present the epoch they were granted, so an owner that was merely slow, and has since been
  replaced, cannot complete or extend the claim. A claim that ran out but was not taken over can still be
  renewed by its owner.
* `release` keeps the epoch (the task becomes free), so epochs only go up.

The scheduler takes the claim before the path lease (the lease is keyed by task id, so taking it first
would refresh another owner's lease) and gives the claim back if the lease is denied. A heartbeat in the
scheduler loop renews the claim and lease of every running task every third of the claim lifetime. If a
renewal fails the task is marked lost: its worker is left to finish, but its branch is not landed and the
claim is not completed. The completion and the lease release are deliberately not in a `finally`:
a process that dies there leaves both behind, as a crash would, and the tests rely on that.

## The orchestrator (`agents/orchestrator.py`)

`Orchestrator.run(run_id, goal=… | graph=…)`:

1. **Plan once.** The plan is stored with put-if-absent at `runs/<id>/plan`. If it exists it is used and
   the planner is not asked; if two orchestrators plan at the same moment the store picks one.
2. **Schedule with claims**, landing through the merge queue.
3. **Record** the result at `runs/<id>/result`.

Resuming is just running it again with the same run id:

| State found | What happens |
|---|---|
| task completed (done, failed, rejected) | not run again; its recorded outcome is used |
| claimed by a live owner | left alone; after `patience` with nothing else to do it is reported as skipped |
| claimed by a dead owner, claim expired | taken over (epoch + 1); the worker resumes from its checkpoint |
| landed but completion not recorded | the durable worker returns its recorded result, `submit` finds the branch already in main (`noop`), the task is marked done |

Several orchestrators can serve one run at once; each task runs under exactly one of them.
`cli orchestrate RUN_ID --goal … --model …` and `cli run-status RUN_ID` expose this.

## How this was tested

* Claims: unit tests for every transition and rule above; 8-thread races (exactly one grant, 20 rounds); a
  randomized comparison with a reference model over claim/renew/complete/release/clock ticks.
* Sessions: replay, gaps, concurrent duplicates (8 threads, one execution), crash after a checkpoint
  (later calls lost, same numbers redone), commit made after the last record (rolled back), foreign
  record (ignored), result retention limit.
* Durable worker: a model that raises a `BaseException` after step 3 stands for the process dying
  (nothing in the code can catch it); the rerun continues from step 3 with the earlier conversation, no
  step is repeated, token counts add up; a finished task is not re-run; model errors keep progress.
* Orchestrator: a crash inside the first run leaves a live claim, a second orchestrator must not touch
  it, and after expiry a third finishes the run having run only the unfinished tasks; two orchestrators on
  one run execute each of ten tasks exactly once; a landed task with a lost completion is not landed twice;
  a failure is remembered across reruns.
* Real process kill on the real cluster: a subprocess orchestrator calls `os._exit(9)` in the middle of a
  task; a second process takes the claim over (epoch 2) and finishes the run from the checkpoint.

* Mutation sweep over `claims.py`, `kvdoc.py`, `session.py`, `orchestrator.py`, `scheduler.py` and
  `worker.py` against these tests plus the scheduling and worker suites: 339 of 360 mutants killed. The
  first pass killed fewer; the survivors led to tests for the heartbeat interval, lease renewal and
  re-acquisition, not releasing a replaced owner's lease, step counting on resume, and the exact error
  messages. The rest are equivalent (JSON key order, a redundant skip of already-lost tasks, an `or` that
  cannot differ because the heartbeat always has a default) or older survivors recorded in doc 08.

## Limits

* A `CliWorker` cannot restore its command's internal state. With `checkpoint_every=SECONDS` it pushes the
  files written so far at that interval, and a retry starts from them and is told in the prompt which files an
  earlier attempt already changed; without it a failed command leaves the branch as it was. Only files are
  saved, not what the agent was thinking.
* The fence is checked twice: before landing starts, and again inside the validate step of `submit`, after
  merging and just before the compare-and-set on main (`agents/fence.py`, a context variable set by the
  scheduler around the landing call). A replaced owner is refused at once: no merger, no redo, no escalation.
  The window between that check and the compare-and-set remains, because the store has no transaction across
  the claim and main. What lands in that window was still merged and validated against the main it replaced,
  so the cost of the race is duplicate work, not a wrong main.
* Within a run the orchestrator names every task `RUN-TASK` (branch, lease, claim and session), so two runs
  can reuse task names. Run ids are letters, digits and `_`, at most 32, and `RUN-TASK` must fit the 64
  character id limit. Calling `Scheduler` directly still uses the bare task id.
* Claim lifetime has to exceed the longest gap between heartbeats, or a slow machine loses its tasks
  needlessly. Clocks of orchestrators are assumed to agree to within a fraction of that lifetime.
* The scheduler decides the order, not the claims: a restarted run with a different `parallel` is fine, a
  different plan is not (the stored plan wins).
