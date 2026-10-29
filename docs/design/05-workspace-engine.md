# Workspace engine

The engine lives in `workspace/src/cmws_ws` and uses the replicated store only through four
operations: `get`, `put`, `put_if_absent` and `cas` (`kv.py`). `MemoryKV` has the same semantics
in-process; `HttpKV` talks to the gateway.

## Data model

| Kind | Key | Mutable | Coordination needed |
|---|---|---|---|
| blob, tree, commit | `obj/<sha256>` | no | none: `put_if_absent`, same content gives the same id |
| ref | `ref/<name>` | yes | compare-and-set |
| workspace meta | `wsmeta/<agent>` | by its single owner | none |

Trees map names to `(file | dir, id)`; a commit is `(tree, parents, author, message, time)`. Ids are
SHA-256 over a kind-tagged canonical form and are verified on read. Because objects never change,
each client caches them forever, and forking costs one ref.

## Isolation and consistency

*Isolation:* a workspace is `(fork_base, head, overlay)`. Reads come from its own head plus the
overlay; nothing others land is visible until `sync`.

*Consistency:* `Repository.submit` loops: read main; merge the branch with it; CAS main from the value
read to the merge. The CAS is the linearization point. A failed CAS means someone landed first, so
the loop re-reads and merges again. Every landing therefore contains all earlier ones, and main's
first-parent history is one line (checked in `tests/test_properties.py` by recording every CAS in
the store's own order).

## Stale reads

The workspace records what it read from the fork snapshot: files, absent paths, and the names in
each directory it listed. Reads of paths the agent itself had changed are skipped (it saw its
own work). At submit time, if upstream moved, each recorded read is compared with upstream's
current value. A difference is a `StaleRead`, and `submit` refuses to land the work (`status =
"stale"`) unless `allow_stale` is set.

Why this is separate from merging: two agents each read `counter = 5` and write `6`. The text
merge sees identical edits and accepts them, losing an increment. With stale-read detection the
second agent is turned back and redoes its work on `6`. In the concurrent counter test (8 agents, 6 increments each) the final value is always 48 with the
check; with the check disabled it comes out well short (27 in one run).

## Merge

`textmerge.merge3` splits both sides into hunks against the base. Hunks from one side apply;
hunks that overlap or touch conflict (as in git) unless both sides produced identical lines.
`treemerge.merge_trees` walks the three trees by name, skipping equal subtrees by id, and merges
files line by line when all three versions are UTF-8. Conflict kinds: `content`, `binary`,
`modify-delete`, `add-add`, `type`, `rename-rename`.

### Renames

Without help, a rename is a delete plus an add, so "A renames `util.py`, B edits it" would be
reported as a modify/delete conflict even though the changes are compatible. Two sources of
rename information feed the merge (`renames.py`):

- **Recorded:** `Workspace.move` remembers (old, new) pairs and stores them in the commit
  (`Commit.renames`, left out of the hash input when empty). Chains collapse (a to b to c is
  a to c), moves back to the origin vanish, and a move of a file created in the same commit is
  an add. A record is trusted only if the trees agree (old gone, new present).
- **Detected:** for what is not recorded (shell `mv`, a file watcher that saw a delete and a
  create), a deleted and an added path with identical content, or text at least 60% similar
  by line, count as a move. Only tried for up to 2,500 delete x add pairs.

For each move on one side, the merge first applies it to the base and to the other side, then
does the normal three-way merge, so the other side's edits land on the new path. Not applied when
the other side already has something at the destination (that surfaces as a conflict instead of an
overwrite). Both sides moving one file to different places is `rename-rename`; to the same place
is fine. Directories are moved file by file.

## Large files

A blob over 256 KiB is split into fixed-size chunks; each chunk is its own object and the blob
becomes a small manifest. The blob id is still the hash of the whole content. Effects: no value
in the store is much larger than a chunk (replication messages stay small), editing one region
of a big file stores one chunk plus a manifest, and identical chunks are stored once. Chunks are
fixed-size, so inserting bytes near the start of a file shifts every later chunk; content-defined
chunking would fix that and was not needed for source code. Blob limit: 256 MiB.

## Garbage

Objects are never deleted. Abandoned workspaces, rollbacks and merge attempts that lost a CAS
leave unreferenced objects. Collecting them needs delete and key listing in the store (it has
neither), a grace period so an object written but not yet referenced is not collected, and log
compaction, because the write-ahead log keeps a deleted value's record anyway. Until then
`gc.storage_report` measures instead: bytes this process wrote versus bytes reachable from main
and the known workspace branches, reported as an overhead factor. Tree updates are batched so
that a commit writes only the trees on the way to its changes.

## Limits

- Directories exist only while they hold a file; empty directories are not represented.
- Rename detection is per file; a rename combined with a total rewrite is only followed when it
  was recorded. Similarity matching can pair an unrelated delete and add that happen to look alike.
- `sync` aborts on conflict instead of materialising conflict markers; that belongs with the
  conflict-resolution agents later.
- No garbage collection (see below).
- Ref and meta writes of a workspace branch assume a single writer; a second writer is detected
  on commit (`ConcurrentWorkspaceUpdate`) but not coordinated.
