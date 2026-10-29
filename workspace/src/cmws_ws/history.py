"""Walking commit history: logs, ancestry and merge bases."""

from __future__ import annotations

from collections import deque
from typing import Dict, Iterator, List, Set

from .objects import Commit, ObjectStore


def ancestors(store: ObjectStore, commit_id: str) -> Set[str]:
    """``commit_id`` and every commit reachable through parents."""
    seen: Set[str] = set()
    todo = [commit_id]
    while todo:
        cid = todo.pop()
        if cid in seen:
            continue
        seen.add(cid)
        todo.extend(store.get_commit(cid).parents)
    return seen


def is_ancestor(store: ObjectStore, maybe_ancestor: str, descendant: str) -> bool:
    return maybe_ancestor in ancestors(store, descendant)


def log(store: ObjectStore, commit_id: str, limit: int | None = None) -> Iterator[tuple]:
    """Yield (id, commit) newest first along the first-parent line (main's linear history)."""
    n = 0
    cid: str | None = commit_id
    while cid is not None and (limit is None or n < limit):
        commit = store.get_commit(cid)
        yield cid, commit
        cid = commit.parents[0] if commit.parents else None
        n += 1


def merge_base(store: ObjectStore, a: str, b: str) -> str | None:
    """A best common ancestor of two commits, or None if the histories are unrelated.

    Best means: common to both and not an ancestor of another common commit. After criss-cross
    merges several such commits can exist; the smallest id is chosen so the result is deterministic.
    """
    common = ancestors(store, a) & ancestors(store, b)
    if not common:
        return None
    redundant: Set[str] = set()
    todo = [p for c in common for p in store.get_commit(c).parents]
    while todo:
        c = todo.pop()
        if c in redundant:
            continue
        redundant.add(c)
        todo.extend(store.get_commit(c).parents)
    best = sorted(common - redundant)
    return best[0]


def commits_between(store: ObjectStore, base: str, head: str) -> List[str]:
    """Commits reachable from ``head`` but not from ``base``, oldest first."""
    exclude = ancestors(store, base)
    order: List[str] = []
    seen: Set[str] = set()
    queue = deque([head])
    while queue:
        cid = queue.popleft()
        if cid in seen or cid in exclude:
            continue
        seen.add(cid)
        order.append(cid)
        queue.extend(store.get_commit(cid).parents)
    order.reverse()
    return order
