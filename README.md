# consistent-multiagent-work-system

A strongly consistent foundation for multi-agent software work.

The long-term goal is a versioned workspace service in which several AI agents
(local or cloud) can fork a shared code repository, work in isolation, and merge
their results back through a linearizable commit point, with the orchestration
layer deciding how work is split and in what order results are merged.

The project is built bottom-up:

| Layer | Status | What it provides |
|---|---|---|
| 1. At-most-once RPC + key/value store | done | exactly-once command execution over an unreliable network |
| 2. Primary-backup replication | done | single-failure tolerance with a view service |
| 3. Multi-Paxos replicated state machine | done | majority-fault-tolerant, linearizable log |
| 4. Sharded store with live reconfiguration | done | multiple replica groups, shard migration via a shard master |
| 5. Versioned workspace (content-addressed objects + CAS refs) | done | fork / commit / three-way merge / stale-read detection on top of the store |
| 5b. Local sync | done | a real folder kept in sync with a workspace branch: pull / push / watch, conflict markers, offline edits |
| 6. Agent orchestration (workers, planner, path leases, merge queue) | in progress (workers, scheduling, merge queue done) | parallel sub-agents with serialized, validated merges |
| 7. MCP server | later | expose the workspace to external agents |

## Status

**Phase 0 (done)** – the replicated key/value stack is in this repository and its
protocol logic lives in pure state machines (`PaxosCore`, `ShardStoreCore`) that
return effects instead of performing I/O. Along the way an intermittent liveness
bug in shard reconfiguration (a retry suppressed by log de-duplication, which
deadlocked a reconfiguration whenever a replica was down) was found and fixed.

**Phase 1 (done)** – the store is usable from the outside:

- hardening: late commands from a client that moved on are never re-executed, heartbeats
  stay bounded while a replica is down, and a few edge cases were closed;
- `CompareAndSet` and `PutIfAbsent`, the two linearizable writes a versioned workspace needs;
- an HTTP gateway over an in-process sharded, replicated cluster, with replica fault injection;
- optional durability: a write-ahead log per replica, so a restarted cluster recovers its data.

**Phase 2 (done)** – the versioned workspace engine (`workspace/`, Python, standard library only):

- immutable content-addressed blobs, trees and commits; a fork is one ref, no copying;
- refs moved only by compare-and-set, so landed work forms a single history;
- workspaces with private overlays, read-your-writes, commit, rollback and resume after a crash;
- three-way merge of directory trees with line-level diff3 for text files; renames (recorded by
  the workspace or detected from content) carry other agents' edits to the new path;
- large files stored as chunks; a measure of unreachable (garbage) storage;
- `submit`: merge, then CAS main; retries on top of whatever landed in the meantime;
- stale-read detection: an agent whose reads were invalidated upstream is told so even when
  its edits do not overlap anyone else's. In a read-modify-write counter test with 8
  concurrent agents, skipping this check silently loses a large share of the increments; with it, none.

**Phase 3 (done)** – local checkouts (`cmws_ws.sync`, `python3 -m cmws_ws.cli`):

- `clone` a workspace branch into a folder, edit it with any tool, `push` / `pull` / `watch`;
- the same fetch-merge-CAS-retry as agent submission, so a folder and cloud agents can write one branch;
- conflicts become markers in the files and block pushing until resolved; edits made offline are
  merged and pushed on reconnect; local renames are detected and followed.

**Phase 4A (done)** – workers (`cmws_ws.agents`, `python3 -m cmws_ws.cli run-task`):

- a `Worker` takes a task and a branch forked from main, commits its result to that branch and never touches main;
- `LoopWorker` is a small tool-calling agent loop over any OpenAI-compatible chat endpoint (a local
  Ollama by default; no paid service involved) with six file tools and a `finish` call;
- `CliWorker` checks the branch out into a folder, runs an existing command-line agent there and pushes the result;
- tested with a scripted model, a fake chat endpoint over HTTP and real subprocesses.

**Phase 4B (done)** – planning and scheduling (`agents.graph`, `agents.planner`, `agents.scheduler`, `leases`):

- a task declares the files it expects to touch; tasks with disjoint scopes run in parallel, overlapping
  ones run one after another in declaration order and start from a main that already contains the earlier work;
- path leases are advisory, expiring claims kept in the store under CAS: of racing overlapping claims exactly
  one is granted, and a crashed agent's lease simply expires;
- after a task, what it really touched is audited against its declared scope; stray edits and collisions with
  another live lease are logged;
- `LLMPlanner` asks a chat model for a JSON task list, validates it and feeds errors back for a retry.

**Phase 4C (done)** – the merge queue (`agents.mergequeue`):

- finished branches land one at a time through a queue that validates the *merged* result (for example by
  running the test suite on it) before main moves, so main only ever holds approved states;
- a branch that cannot land escalates: automatic three-way merge, then a merger agent that sees both sides'
  intent and the conflict markers, then a redo of the task on top of what landed, then a durable record for a person;
- tested with semantic conflicts that text merge accepts, concurrent landings checked against the recorded
  history of main, random edit storms through every stage, and a run on the real cluster.

Next: reliability (call de-duplication, checkpoints, crash recovery) and the end-to-end orchestrator.

(Cross-group multi-key transactions are out of scope; their suite under
`ShardStorePart2Test` is not expected to pass.)

## Build and test

Requires Java 17+ and `make`.

```bash
make            # compile all modules
make clean
python3 run-tests.py -l 5        # this project's own tests (unit, integration, gateway, durability)
python3 run-tests.py -l 4 -p 2   # sharded store suite (also -l 1 .. -l 3)
make test-workspace              # workspace engine and sync tests, including runs against the real gateway
```

See `NOTICE` for where the test harness comes from.

## Running the gateway

```bash
make run-gateway ARGS="--port 8080 --groups 2 --replicas 3 --data-dir /tmp/cmws-data"
```

`--data-dir` is optional; with it, stopping and starting the gateway again keeps the data.

```bash
curl -X PUT  --data 'hello'  localhost:8080/kv/greeting        # write
curl                         localhost:8080/kv/greeting        # read (404 if missing)
curl -X POST --data 'v1'     localhost:8080/kv/head/cas        # create: key must not exist
curl -X POST --data 'v2'     'localhost:8080/kv/head/cas?expected=v1'   # advance if still v1
curl -X POST --data 'alice'  localhost:8080/kv/owner/put-if-absent
curl -X POST 'localhost:8080/admin/kill?node=server1-2'        # cut a replica off
curl -X POST 'localhost:8080/admin/revive?node=server1-2'
```

The conditional operations answer `{"success":true|false,"value":...}` where `value` is what the
key holds afterwards.

## Layout

```
modules/rpc-kvstore  at-most-once layer, key/value store, effect types (cmws.core)
modules/primary-backup, modules/paxos, modules/sharded-kv   the replication stack
modules/gateway      HTTP front end and in-process cluster
workspace/           versioned workspace engine (Python): objects, refs, merge, submit
modules/test-entries test discovery entry points (see NOTICE)
docs/design/         design documents
jars/                third-party and test-harness binaries
```

Protocol logic never performs I/O: a core returns `Send`, `SetTimer`, `Persist` and
protocol-specific effects, and a thin `Node` adapter carries them out.

## Origin

See [NOTICE](NOTICE).
