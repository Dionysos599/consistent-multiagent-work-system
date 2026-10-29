"""Measuring what is still reachable.

Objects are immutable and never deleted. Abandoned workspaces, rolled-back commits and merge
attempts that lost a CAS leave unreferenced objects behind. Deleting them needs a store that can
delete and list keys, and a grace period so an object that was just written but not yet referenced
is not collected, and the write-ahead log does not shrink either until it can be compacted. So
this module only measures: how much of what this process wrote is reachable from main and the
known workspace branches, and how much is garbage. Evaluation reports that as storage overhead.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Set

from .objects import DIR, ObjectStore


@dataclass(frozen=True)
class StorageReport:
    total_objects: int
    total_bytes: int
    reachable_objects: int
    reachable_bytes: int

    @property
    def orphan_objects(self) -> int:
        return self.total_objects - self.reachable_objects

    @property
    def orphan_bytes(self) -> int:
        return self.total_bytes - self.reachable_bytes

    @property
    def overhead(self) -> float:
        """Bytes stored per byte that is still reachable (1.0 means no garbage)."""
        return self.total_bytes / self.reachable_bytes if self.reachable_bytes else 1.0


def reachable(store: ObjectStore, roots: Iterable[str]) -> Set[str]:
    """Ids of every commit, tree, blob and chunk reachable from the given commits."""
    seen: Set[str] = set()
    todo = list(roots)
    while todo:
        oid = todo.pop()
        if oid in seen:
            continue
        seen.add(oid)
        commit = store.get_commit(oid)
        todo.extend(commit.parents)
        trees = [commit.tree]
        while trees:
            tid = trees.pop()
            if tid in seen:
                continue
            seen.add(tid)
            for entry in store.get_tree(tid).entries.values():
                if entry.mode == DIR:
                    trees.append(entry.id)
                elif entry.id not in seen:
                    seen.add(entry.id)
                    seen.update(store.blob_chunks(entry.id))
    return seen


def storage_report(repo, extra_roots: Iterable[str] = ()) -> StorageReport:
    """Reachable versus garbage among the objects this process wrote."""
    roots = {repo.head(), *extra_roots}
    for agent in repo.workspaces:
        head = repo.refs.get(f"ws/{agent}")
        if head:
            roots.add(head)
    live = reachable(repo.store, roots)
    written = repo.store.written
    return StorageReport(
        total_objects=len(written),
        total_bytes=sum(written.values()),
        reachable_objects=sum(1 for o in written if o in live),
        reachable_bytes=sum(n for o, n in written.items() if o in live),
    )
