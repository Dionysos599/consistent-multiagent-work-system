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

Next: the versioned workspace layer (content-addressed objects plus compare-and-set references).

(Cross-group multi-key transactions are out of scope; their suite under
`ShardStorePart2Test` is not expected to pass.)

## Build and test

Requires Java 17+ and `make`.

```bash
make            # compile all modules
make clean
python3 run-tests.py -l 5        # this project's own tests (unit, integration, gateway, durability)
python3 run-tests.py -l 4 -p 2   # sharded store suite (also -l 1 .. -l 3)
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
modules/test-entries test discovery entry points (see NOTICE)
docs/design/         design documents
jars/                third-party and test-harness binaries
```

Protocol logic never performs I/O: a core returns `Send`, `SetTimer`, `Persist` and
protocol-specific effects, and a thin `Node` adapter carries them out.

## Origin

See [NOTICE](NOTICE).
