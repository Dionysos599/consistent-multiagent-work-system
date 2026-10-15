# consistent-multiagent-work-system

A strongly consistent foundation for multi-agent software work.

The long-term goal is a versioned workspace service in which several AI agents
(local or cloud) can fork a shared code repository, work in isolation, and merge
their results back through a linearizable commit point, with the orchestration
layer deciding how work is split and in what order results are merged.

The project is built bottom-up:

| Layer | Status | What it provides |
|---|---|---|
| 1. At-most-once RPC + key/value store | planned in Phase 0 | exactly-once command execution over an unreliable network |
| 2. Primary-backup replication | planned in Phase 0 | single-failure tolerance with a view service |
| 3. Multi-Paxos replicated state machine | planned in Phase 0 | majority-fault-tolerant, linearizable log |
| 4. Sharded store with live reconfiguration | planned in Phase 0 | multiple replica groups, shard migration via a shard master |
| 5. Versioned workspace (content-addressed objects + CAS refs) | next | fork / commit / merge on top of the store |
| 6. Agent orchestration (planner, path leases, merge queue) | next | parallel sub-agents with serialized, validated merges |
| 7. MCP server | later | expose the workspace to external agents |

## Phase 0 (current)

Bring the replicated key/value store into this repository as a sequence of
reviewable steps, then refactor it into a protocol core that is independent of
the test harness (state machines in, effects out).

## Build

Requires Java 17+ and `make`.

```bash
make            # compile all modules
make clean
```

Tests are run through `run-tests.py` (see `NOTICE` for where the harness comes from).

## Layout

```
modules/<name>/src   implementation
modules/<name>/tst   tests
docs/                design documents
jars/                third-party and test-harness binaries
```

## Origin

See [NOTICE](NOTICE).
