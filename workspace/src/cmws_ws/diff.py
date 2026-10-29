"""Comparing trees and rendering changes."""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import Dict, List, Optional

from .objects import DIR, ObjectStore

ADDED, DELETED, MODIFIED = "A", "D", "M"


@dataclass(frozen=True)
class Change:
    path: str
    kind: str  # ADDED, DELETED or MODIFIED
    old: Optional[str]  # blob ids
    new: Optional[str]


def diff_trees(store: ObjectStore, a: Optional[str], b: Optional[str]) -> List[Change]:
    """File-level changes turning tree ``a`` into tree ``b`` (None means the empty tree).

    Subtrees with equal ids are skipped without being read, so cost follows the size of the change.
    """
    changes: List[Change] = []
    _walk(store, a, b, "", changes)
    return sorted(changes, key=lambda c: c.path)


def _entries(store: ObjectStore, tid: Optional[str]):
    return store.get_tree(tid).entries if tid else {}


def _walk(store: ObjectStore, a: Optional[str], b: Optional[str], prefix: str,
          out: List[Change]) -> None:
    if a == b:
        return
    ea, eb = _entries(store, a), _entries(store, b)
    for name in set(ea) | set(eb):
        x, y = ea.get(name), eb.get(name)
        if x == y:
            continue
        path = f"{prefix}/{name}" if prefix else name
        x_dir = x is not None and x.mode == DIR
        y_dir = y is not None and y.mode == DIR
        if x_dir or y_dir:
            _walk(store, x.id if x_dir else None, y.id if y_dir else None, path, out)
        if x is not None and not x_dir:
            if y is not None and not y_dir:
                out.append(Change(path, MODIFIED, x.id, y.id))
            else:
                out.append(Change(path, DELETED, x.id, None))
        if y is not None and not y_dir and (x is None or x_dir):
            out.append(Change(path, ADDED, None, y.id))


def changed_files(store: ObjectStore, a: Optional[str], b: Optional[str]) -> Dict[str, Change]:
    return {c.path: c for c in diff_trees(store, a, b)}


def unified_bytes(path: str, old: Optional[bytes], new: Optional[bytes]) -> str:
    """A unified diff for one file (None: absent); binary files are summarised."""
    try:
        a, b = (old or b"").decode("utf-8"), (new or b"").decode("utf-8")
    except UnicodeDecodeError:
        return f"Binary files differ: {path} ({len(old or b'')} -> {len(new or b'')} bytes)\n"
    return "".join(difflib.unified_diff(
        a.splitlines(keepends=True), b.splitlines(keepends=True),
        fromfile="/dev/null" if old is None else f"a/{path}",
        tofile="/dev/null" if new is None else f"b/{path}"))


def unified(store: ObjectStore, change: Change) -> str:
    old = store.get_blob(change.old) if change.old else None
    new = store.get_blob(change.new) if change.new else None
    return unified_bytes(change.path, old, new)


def unified_all(store: ObjectStore, a: Optional[str], b: Optional[str]) -> str:
    return "".join(unified(store, c) for c in diff_trees(store, a, b))
