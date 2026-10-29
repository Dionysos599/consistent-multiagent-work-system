"""An agent's private, snapshot-isolated view of the repository.

Forking is free: a workspace is a commit id (its *fork base*) plus an in-memory overlay of pending
edits. Nothing the agent does is visible to others, and nothing others do is visible to it, until
it asks for ``sync`` or ``Repository.submit``. That isolation is what lets agents work
independently; the compare-and-set on ``main`` inside ``submit`` is what makes their combined
result consistent.

While the agent works, the workspace records what it *read* from the base snapshot (files, absent
paths and directory listings). At submit time the repository checks whether upstream has changed
any of those: an agent whose edits do not overlap anyone else's can still have been working from
facts that are no longer true, and that is reported as a stale read.

A Workspace is used by one thread; the shared state lives in the store.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .diff import Change, diff_trees, unified_bytes
from .history import ancestors, is_ancestor
from .objects import DIR, FILE, Commit, ObjectError, PathConflict, blob_id
from .paths import InvalidPath, normalize, split
from .treemerge import PathConflictInfo, merge_trees


class ConcurrentWorkspaceUpdate(Exception):
    """The workspace branch moved underneath this handle (another writer on the same branch)."""


class DirtyWorkspace(Exception):
    """The operation needs all edits committed first."""


@dataclass(frozen=True)
class StaleRead:
    path: str
    kind: str  # 'file' or 'listing'
    observed: object  # blob id / tuple of names at fork time (None: did not exist)
    current: object   # the same thing upstream now


@dataclass
class SyncResult:
    status: str  # 'up-to-date' | 'fast-forward' | 'merged' | 'conflict'
    commit: Optional[str] = None
    conflicts: List[PathConflictInfo] = field(default_factory=list)
    stale_reads: List[StaleRead] = field(default_factory=list)
    auto_merged: List[str] = field(default_factory=list)


class Workspace:
    def __init__(self, repo: "Repository", agent_id: str, fork_base: str, head: str,  # noqa: F821
                 reads: Optional[Dict[str, Optional[str]]] = None,
                 listings: Optional[Dict[str, Optional[Tuple[str, ...]]]] = None) -> None:
        self.repo = repo
        self.agent_id = agent_id
        self.fork_base = fork_base
        self.head = head
        self._overlay: Dict[str, Optional[bytes]] = {}  # path -> new content, None = deleted
        self._moves: Dict[str, str] = {}  # new path -> path it came from, for uncommitted moves
        self.reads: Dict[str, Optional[str]] = dict(reads or {})
        self.listings: Dict[str, Optional[Tuple[str, ...]]] = dict(listings or {})

    # ---- plumbing ------------------------------------------------------------------------

    @property
    def store(self):
        return self.repo.store

    @property
    def ref_name(self) -> str:
        return f"ws/{self.agent_id}"

    def _tree(self, commit_id: str) -> str:
        return self.store.get_commit(commit_id).tree

    @property
    def _head_tree(self) -> str:
        return self._tree(self.head)

    @property
    def _fork_tree(self) -> str:
        return self._tree(self.fork_base)

    # ---- the visible tree: head plus overlay ---------------------------------------------

    def _paths_under(self, directory: str) -> set:
        entry = self.store.lookup(self._head_tree, directory)
        paths = set()
        if entry is not None and entry.mode == DIR:
            paths = set(self.store.flatten(entry.id, directory))
        prefix = directory + "/" if directory else ""
        for p, data in self._overlay.items():
            if p.startswith(prefix):
                (paths.add if data is not None else paths.discard)(p)
        return paths

    def _kind(self, path: str) -> Optional[str]:
        if path in self._overlay and self._overlay[path] is not None:
            return FILE
        if self._paths_under(path):
            return DIR
        if path in self._overlay:  # deleted file
            return None
        entry = self.store.lookup(self._head_tree, path)
        return entry.mode if entry and entry.mode == FILE else None

    def _check_writable(self, path: str) -> None:
        parts = split(path)
        for i in range(1, len(parts)):
            if self._kind("/".join(parts[:i])) == FILE:
                raise PathConflict(f"{'/'.join(parts[:i])} is a file, cannot create {path}")
        if self._kind(path) == DIR:
            raise IsADirectoryError(path)

    # ---- recording what was read ---------------------------------------------------------

    def _file_id(self, tree_id: str, path: str) -> Optional[str]:
        e = self.store.lookup(tree_id, path)
        return e.id if e is not None and e.mode == FILE else None

    def _record_read(self, path: str) -> None:
        """Note that the agent depends on ``path`` as it was at the fork base — unless it has
        changed the path itself, in which case what it saw was its own work."""
        fork_id = self._file_id(self._fork_tree, path)
        if fork_id == self._file_id(self._head_tree, path):
            self.reads.setdefault(path, fork_id)

    def _names(self, tree_id: str, directory: str) -> Optional[Tuple[str, ...]]:
        entry = self.store.lookup(tree_id, directory)
        if entry is None or entry.mode != DIR:
            return None
        return tuple(self.store.get_tree(entry.id).entries)

    # ---- file operations -----------------------------------------------------------------

    def read(self, path: str) -> bytes:
        p = normalize(path)
        if p not in self._overlay:
            self._record_read(p)
        return self._peek(p)

    def _peek(self, p: str) -> bytes:
        """Content of a normalized path without noting it as something the agent depends on."""
        if p in self._overlay:
            data = self._overlay[p]
            if data is None:
                raise FileNotFoundError(p)
            return data
        entry = self.store.lookup(self._head_tree, p) if p else None
        if entry is None or (p and self._kind(p) is None):
            raise FileNotFoundError(p)
        if entry.mode == DIR:
            raise IsADirectoryError(p)
        return self.store.get_blob(entry.id)

    def read_text(self, path: str) -> str:
        return self.read(path).decode("utf-8")

    def exists(self, path: str) -> bool:
        p = normalize(path)
        if p not in self._overlay:
            self._record_read(p)
        return self._kind(p) is not None

    def write(self, path: str, data) -> None:
        p = normalize(path)
        if not p:
            raise InvalidPath("cannot write to the root")
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._check_writable(p)
        self._overlay[p] = bytes(data)

    def delete(self, path: str) -> None:
        """Delete a file, or a directory with everything below it."""
        p = normalize(path)
        if not p:
            raise InvalidPath("cannot delete the root")
        kind = self._kind(p)
        if kind is None:
            raise FileNotFoundError(p)
        if kind == FILE:
            self._overlay[p] = None
        else:
            for q in self._paths_under(p):
                self._overlay[q] = None

    def move(self, src: str, dst: str) -> None:
        """Rename a file or directory. Atomic: it is two edits in one overlay. The move is also
        recorded in the next commit so that merges can carry other agents' edits to the new path."""
        s, d = normalize(src), normalize(dst)
        kind = self._kind(s)
        if kind is None:
            raise FileNotFoundError(s)
        if d == s or d.startswith(s + "/"):
            raise InvalidPath(f"cannot move {s} into itself")
        moved = [(s, d)] if kind == FILE else [
            (q, d + q[len(s):]) for q in sorted(self._paths_under(s))]
        contents = {q: self._peek(q) for q, _ in moved}  # relocating is not depending
        self.delete(s)
        for q, target in moved:
            self.write(target, contents[q])
            origin = self._moves.pop(q, q)   # a file moved twice is one move from its origin
            if origin != target:
                self._moves[target] = origin

    def list(self, directory: str = "") -> List[Tuple[str, str]]:
        """Sorted (name, 'file'|'dir') pairs directly below ``directory``."""
        d = normalize(directory)
        if d and self._kind(d) != DIR:
            if self._kind(d) == FILE:
                raise NotADirectoryError(d)
            raise FileNotFoundError(d)
        self.listings.setdefault(d, self._names(self._fork_tree, d))
        prefix = d + "/" if d else ""
        found: Dict[str, str] = {}
        for p in self._paths_under(d):
            first, _, rest = p[len(prefix):].partition("/")
            found[first] = DIR if rest else FILE
        return sorted(found.items())

    def files(self) -> Dict[str, bytes]:
        """Every visible file with its content (does not count as reading, for tests and tools)."""
        out: Dict[str, bytes] = {}
        for p in self._paths_under(""):
            out[p] = self._overlay[p] if p in self._overlay else self.store.read_file(
                self._head_tree, p)
        return out

    # ---- state ---------------------------------------------------------------------------

    def _pending(self) -> Dict[str, Optional[bytes]]:
        """Uncommitted edits as {path: new content, None = deleted}, without the ones that leave
        a path as it already is in the head. Nothing is written to the store here."""
        out: Dict[str, Optional[bytes]] = {}
        for p, data in self._overlay.items():
            bid = blob_id(data) if data is not None else None
            if bid != self._file_id(self._head_tree, p):
                out[p] = data
        return out

    @property
    def dirty(self) -> bool:
        return bool(self._pending())

    def status(self) -> Dict[str, str]:
        """{path: 'A'|'M'|'D'} for uncommitted edits."""
        return {p: ("D" if data is None else "M" if self._file_id(self._head_tree, p) else "A")
                for p, data in sorted(self._pending().items())}

    def diff(self) -> str:
        """Unified diff of uncommitted edits against the head."""
        out = []
        for p, data in sorted(self._pending().items()):
            old = self.store.read_file(self._head_tree, p)
            out.append(unified_bytes(p, old, data))
        return "".join(out)

    def changes_since_fork(self) -> List[Change]:
        """File-level changes of committed work relative to the fork base."""
        return diff_trees(self.store, self._fork_tree, self._head_tree)

    # ---- committing and rolling back -----------------------------------------------------

    def commit(self, message: str) -> Optional[str]:
        """Turn the overlay into a commit on this workspace's branch; None if nothing changed."""
        pending = self._pending()
        if not pending:
            self._overlay.clear()
            return None
        changes = {p: (self.store.put_blob(d) if d is not None else None)
                   for p, d in pending.items()}
        tree = self.store.update(self._head_tree, changes)
        cid = self.store.put_commit(Commit(tree, (self.head,), self.agent_id, message,
                                           self.repo.clock(), self._recorded_moves(pending)))
        self._advance(cid)
        self._overlay.clear()
        self._moves.clear()
        return cid

    def _recorded_moves(self, pending: Dict[str, Optional[bytes]]) -> Tuple[Tuple[str, str], ...]:
        """Moves worth recording: the old path really went away, the new one really appeared,
        and the old path existed before this commit (moving a file you just created is an add)."""
        out = []
        for dst, src in sorted(self._moves.items()):
            if (pending.get(src, "x") is None and pending.get(dst) is not None
                    and self._file_id(self._head_tree, src) is not None
                    and self._file_id(self._head_tree, dst) is None):
                out.append((src, dst))
        return tuple(out)

    def _advance(self, new_head: str) -> None:
        self.repo.save_meta(self, new_head)
        if not self.repo.refs.cas(self.ref_name, self.head, new_head).ok:
            raise ConcurrentWorkspaceUpdate(self.ref_name)
        self.head = new_head

    def rollback(self, to: Optional[str] = None) -> None:
        """Drop uncommitted edits; with ``to``, also move back to an earlier commit of this branch."""
        self._overlay.clear()
        self._moves.clear()
        if to is None or to == self.head:
            return
        if not is_ancestor(self.store, self.fork_base, to) or not is_ancestor(
                self.store, to, self.head):
            raise ValueError("can only roll back to a commit between the fork base and the head")
        self._advance(to)

    # ---- catching up with upstream -------------------------------------------------------

    def sync(self) -> SyncResult:
        """Bring upstream (main) into this workspace: fetch + merge.

        On success the workspace's fork base moves to the upstream head, and the read record is
        reset (the agent is told which of its reads went stale in the result and is expected to
        act on that). On conflict nothing changes and the conflicts are returned.
        """
        if self.dirty:
            raise DirtyWorkspace("commit or roll back edits before syncing")
        upstream = self.repo.head()
        if upstream == self.fork_base or upstream in ancestors(self.store, self.head):
            return SyncResult("up-to-date")
        stale = self.repo.stale_reads(self, upstream)
        if is_ancestor(self.store, self.head, upstream):
            self._adopt(upstream, upstream)
            return SyncResult("fast-forward", upstream, stale_reads=stale)
        merged = self.repo.merge_into(self.head, upstream, f"ws/{self.agent_id}", "main")
        if not merged.clean:
            return SyncResult("conflict", conflicts=merged.conflicts, stale_reads=stale)
        cid = self.store.put_commit(Commit(
            merged.tree, (self.head, upstream), self.agent_id,
            f"Merge main into {self.ref_name}", self.repo.clock()))
        self._adopt(cid, upstream)
        return SyncResult("merged", cid, stale_reads=stale, auto_merged=merged.auto_merged)

    def _adopt(self, new_head: str, new_fork_base: str) -> None:
        self.repo.save_meta(self, new_head, fork_base=new_fork_base, reads={}, listings={})
        if not self.repo.refs.cas(self.ref_name, self.head, new_head).ok:
            raise ConcurrentWorkspaceUpdate(self.ref_name)
        self.head, self.fork_base = new_head, new_fork_base
        self.reads, self.listings = {}, {}
