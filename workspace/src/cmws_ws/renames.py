"""Finding out which files a side of a merge moved.

Two sources, combined:

* **recorded**: ``Workspace.move`` stores (old, new) pairs in the commit. These are exact, and
  survive heavy edits to the moved file.
* **detected**: a deleted path and an added path with identical content, or text that is mostly the
  same (``SIMILARITY``), are treated as a move. This covers changes that did not go through
  ``move`` (a shell ``mv``, a file watcher that only saw a delete and a create).

Recorded renames win; detection only looks at paths they did not claim.
"""

from __future__ import annotations

import difflib
from typing import Dict, List, Optional, Tuple

from .diff import ADDED, DELETED, changed_files
from .history import commits_between
from .objects import ObjectStore
from .textmerge import split_lines

SIMILARITY = 0.6
MAX_DETECTION_PAIRS = 2500   # beyond this many delete x add pairs only exact matches are tried
MAX_SIMILAR_BYTES = 200_000


def recorded_renames(store: ObjectStore, base: str, head: str) -> Dict[str, str]:
    """Moves recorded on the commits between ``base`` and ``head``, with chains collapsed
    (a->b then b->c becomes a->c)."""
    chain: Dict[str, str] = {}
    for cid in commits_between(store, base, head):
        for src, dst in store.get_commit(cid).renames:
            origin = next((o for o, cur in chain.items() if cur == src), src)
            chain[origin] = dst
    return chain


def _similar(store: ObjectStore, a: str, b: str) -> float:
    if a == b:
        return 1.0
    x, y = store.get_blob(a), store.get_blob(b)
    if len(x) > MAX_SIMILAR_BYTES or len(y) > MAX_SIMILAR_BYTES:
        return 0.0
    try:
        xs, ys = x.decode("utf-8"), y.decode("utf-8")
    except UnicodeDecodeError:
        return 0.0
    return difflib.SequenceMatcher(None, split_lines(xs), split_lines(ys),
                                   autojunk=False).ratio()


def side_renames(store: ObjectStore, base_commit: Optional[str], side_commit: str) -> Dict[str, str]:
    """{old path: new path} for files that moved between the base and one side of a merge."""
    if base_commit is None:
        return {}
    return moved_between(store, store.get_commit(base_commit).tree,
                         store.get_commit(side_commit).tree,
                         recorded_renames(store, base_commit, side_commit))


def moved_between(store: ObjectStore, base_tree: str, side_tree: str,
                  recorded: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Moves between two trees: the recorded ones the trees confirm, then detected ones."""
    changes = changed_files(store, base_tree, side_tree)
    deleted = {p: c.old for p, c in changes.items() if c.kind == DELETED}
    added = {p: c.new for p, c in changes.items() if c.kind == ADDED}

    result: Dict[str, str] = {}
    for src, dst in (recorded or {}).items():
        # trust a record only if the trees agree that the move happened
        if src in deleted and dst in added:
            result[src] = dst
    free_src = {p: b for p, b in deleted.items() if p not in result}
    free_dst = {p: b for p, b in added.items() if p not in result.values()}

    # exact content matches first, then similar text
    by_content: Dict[str, List[str]] = {}
    for p, b in free_dst.items():
        by_content.setdefault(b, []).append(p)
    for src, b in sorted(free_src.items()):
        candidates = by_content.get(b)
        if candidates:
            dst = candidates.pop(0)
            result[src] = dst
            del free_dst[dst]
    free_src = {p: b for p, b in free_src.items() if p not in result}
    if len(free_src) * len(free_dst) <= MAX_DETECTION_PAIRS:
        scored: List[Tuple[float, str, str]] = []
        for src, sb in free_src.items():
            for dst, db in free_dst.items():
                score = _similar(store, sb, db)
                if score >= SIMILARITY:
                    scored.append((score, src, dst))
        used_src, used_dst = set(), set()
        for score, src, dst in sorted(scored, key=lambda t: (-t[0], t[1], t[2])):
            if src not in used_src and dst not in used_dst:
                result[src] = dst
                used_src.add(src)
                used_dst.add(dst)
    return result
