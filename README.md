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
| 5. Versioned workspace (content-addressed objects + CAS refs) | next | fork / commit / merge on top of the store |
| 6. Agent orchestration (planner, path leases, merge queue) | next | parallel sub-agents with serialized, validated merges |
| 7. MCP server | later | expose the workspace to external agents |

## Status

Phase 0 is complete: the replicated key/value stack is in this repository, its
protocol logic lives in pure state machines (`PaxosCore`, `ShardStoreCore`) that
return effects instead of performing I/O, and the test suites for layers 1–4
pass. (Cross-group multi-key transactions are out of scope; their suite under
`ShardStorePart2Test` is not expected to pass.) Along the
way an intermittent liveness bug in shard reconfiguration (a retry suppressed by
log de-duplication, which deadlocked a reconfiguration whenever a replica was
down) was found and fixed.

Next: hardening the store and adding compare-and-set, then the versioned
workspace layer.

## Build

Requires Java 17+ and `make`.

```bash
make            # compile all modules
make clean
```

Tests are run through `run-tests.py` (see `NOTICE` for where the harness comes from).

## Layout

```
modules/<name>/src   implementation (protocol cores + framework adapters)
modules/<name>/tst   tests
modules/test-entries test discovery entry points (see NOTICE)
docs/                design documents
jars/                third-party and test-harness binaries
```

## Origin

See [NOTICE](NOTICE).
