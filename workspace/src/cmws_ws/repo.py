"""The repository: shared objects and refs, workspace lifecycle, and the submit protocol.

Submit is where isolation turns back into consistency. Agents work on private snapshots; to land
their work they call ``submit``, which loops:

    read main;  merge the agent's branch with it;  CAS main from what we read to the merge.

The CAS is the linearization point. If someone else moved main in the meantime the CAS fails and
the loop starts again from the new main, so every commit that ever lands has seen the complete
history before it, and main's first-parent history is a single line.

Before merging, the agent's *reads* are checked against what upstream changed since it forked
(stale-read detection). Independent edits can still be built on facts that stopped being true.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from .history import ancestors, is_ancestor, merge_base
from .kv import KV
from .objects import DIR, FILE, Commit, ObjectStore
from .refs import RefStore
from .renames import side_renames
from .treemerge import PathConflictInfo, TreeMergeResult, merge_trees
from .workspace import StaleRead, Workspace

MAIN = "main"


class WorkspaceExists(Exception):
    pass


class NoSuchWorkspace(Exception):
    pass


@dataclass
class SubmitResult:
    status: str  # 'fast-forward' | 'merged' | 'noop' | 'conflict' | 'stale' | 'contended'
    commit: Optional[str] = None  # the new main, when the work landed
    conflicts: List[PathConflictInfo] = field(default_factory=list)
    stale_reads: List[StaleRead] = field(default_factory=list)
    auto_merged: List[str] = field(default_factory=list)
    attempts: int = 0

    @property
    def landed(self) -> bool:
        return self.status in ("fast-forward", "merged")


class Repository:
    def __init__(self, kv: KV, main: str = MAIN, clock: Callable[[], float] = time.time,
                 max_attempts: int = 100) -> None:
        self.kv = kv
        self.store = ObjectStore(kv)
        self.refs = RefStore(kv)
        self.main = main
        self.clock = clock
        self.max_attempts = max_attempts
        self.workspaces: set = set()   # agent ids this process forked or resumed (see gc.py)

    # ---- setup ---------------------------------------------------------------------------

    @classmethod
    def init(cls, kv: KV, files: Optional[Dict[str, bytes]] = None, author: str = "system",
             **kwargs) -> "Repository":
        """Open the repository, creating main with an initial commit if it does not exist."""
        repo = cls(kv, **kwargs)
        if repo.refs.get(repo.main) is None:
            ids = {p: repo.store.put_blob(d if isinstance(d, bytes) else d.encode())
                   for p, d in (files or {}).items()}
            root = repo.store.put_commit(Commit(repo.store.build(ids), (), author,
                                                "Initial commit", repo.clock()))
            repo.refs.create(repo.main, root)  # losing this race is fine: someone else created it
        return repo

    def head(self) -> str:
        head = self.refs.get(self.main)
        if head is None:
            raise NoSuchWorkspace(f"{self.main} does not exist; call Repository.init")
        return head

    def files_at(self, commit_id: str) -> Dict[str, bytes]:
        tree = self.store.get_commit(commit_id).tree
        return {p: self.store.get_blob(b) for p, b in self.store.flatten(tree).items()}

    def main_files(self) -> Dict[str, bytes]:
        return self.files_at(self.head())

    # ---- workspace lifecycle -------------------------------------------------------------

    def _meta_key(self, agent_id: str) -> str:
        return f"wsmeta/{agent_id}"

    def fork(self, agent_id: str, base: Optional[str] = None) -> Workspace:
        """Create a workspace on the current main (or another commit). O(1): one ref is created."""
        base = base or self.head()
        ws = Workspace(self, agent_id, base, base)
        # The meta record is the claim on the name; it carries a nonce so that a retried claim
        # (lost reply) can tell its own earlier success from someone else's.
        claim = json.dumps({"fork_base": base, "head": base, "reads": {}, "listings": {},
                            "nonce": uuid.uuid4().hex}, sort_keys=True)
        stored, current = self.kv.put_if_absent(self._meta_key(agent_id), claim)
        if not stored and current != claim:
            raise WorkspaceExists(agent_id)
        self.refs.cas(ws.ref_name, None, base)
        self.workspaces.add(agent_id)
        return ws

    def resume(self, agent_id: str) -> Workspace:
        """Reopen a workspace from its last committed state (after a crash or restart)."""
        head = self.refs.get(f"ws/{agent_id}")
        raw = self.kv.get(self._meta_key(agent_id))
        if head is None or raw is None:
            raise NoSuchWorkspace(agent_id)
        meta = json.loads(raw)
        self.workspaces.add(agent_id)
        return Workspace(self, agent_id, meta["fork_base"], head, meta["reads"],
                         {d: (tuple(n) if n is not None else None)
                          for d, n in meta["listings"].items()})

    def save_meta(self, ws: Workspace, new_head: str, fork_base: Optional[str] = None,
                  reads: Optional[dict] = None, listings: Optional[dict] = None) -> None:
        self.kv.put(self._meta_key(ws.agent_id), json.dumps({
            "fork_base": fork_base or ws.fork_base,
            "head": new_head,
            "reads": ws.reads if reads is None else reads,
            "listings": {d: (list(n) if n is not None else None)
                         for d, n in (ws.listings if listings is None else listings).items()},
        }, sort_keys=True))

    # ---- stale reads ---------------------------------------------------------------------

    def stale_reads(self, ws: Workspace, upstream: str) -> List[StaleRead]:
        """Reads of ``ws`` that upstream has since invalidated."""
        tree = self.store.get_commit(upstream).tree
        out: List[StaleRead] = []
        for path, observed in sorted(ws.reads.items()):
            entry = self.store.lookup(tree, path)
            current = entry.id if entry is not None and entry.mode == FILE else None
            if current != observed:
                out.append(StaleRead(path, "file", observed, current))
        for directory, observed_names in sorted(ws.listings.items()):
            entry = self.store.lookup(tree, directory)
            current_names = (tuple(self.store.get_tree(entry.id).entries)
                             if entry is not None and entry.mode == DIR else None)
            if current_names != observed_names:
                out.append(StaleRead(directory, "listing", observed_names, current_names))
        return out

    # ---- merging -------------------------------------------------------------------------

    def merge_into(self, ours: str, theirs: str, ours_label: str = "ours",
                   theirs_label: str = "theirs") -> TreeMergeResult:
        """Three-way merge of two commits' trees around their merge base."""
        base = merge_base(self.store, ours, theirs)
        base_tree = self.store.get_commit(base).tree if base else None
        return merge_trees(self.store, base_tree, self.store.get_commit(ours).tree,
                           self.store.get_commit(theirs).tree, ours_label, theirs_label,
                           ours_renames=side_renames(self.store, base, ours),
                           theirs_renames=side_renames(self.store, base, theirs))

    # ---- submit --------------------------------------------------------------------------

    def submit(self, ws: Workspace, message: Optional[str] = None,
               allow_stale: bool = False) -> SubmitResult:
        """Land the workspace's work on main.

        Uncommitted edits are committed first (``message``). Returns ``stale`` or ``conflict``
        without touching main when the work cannot land as it stands; the caller (an
        orchestrator) then decides whether to sync and redo, merge by hand, or give up.
        With ``allow_stale`` the stale reads are reported but do not block the merge.
        """
        if ws.dirty:
            ws.commit(message or f"Work by {ws.agent_id}")
        ws.refresh()   # include whatever other writers of this branch (a local checkout) pushed
        result = SubmitResult("noop")
        for attempt in range(1, self.max_attempts + 1):
            result.attempts = attempt
            main = self.head()
            if is_ancestor(self.store, ws.head, main):
                result.status = "noop"  # nothing to land, or it is all on main already
                return result
            if main != ws.fork_base:
                # Upstream moved since the agent last looked: were its reads still true?
                result.stale_reads = self.stale_reads(ws, main)
                if result.stale_reads and not allow_stale:
                    result.status = "stale"
                    return result
            if is_ancestor(self.store, main, ws.head):
                # The branch already contains main, so landing it is just moving the ref.
                new_main, status, merged_files = ws.head, "fast-forward", []
            else:
                merged = self.merge_into(ws.head, main, f"ws/{ws.agent_id}", self.main)
                if not merged.clean:
                    result.status, result.conflicts = "conflict", merged.conflicts
                    return result
                new_main = self.store.put_commit(Commit(
                    merged.tree, (main, ws.head), ws.agent_id,
                    f"Merge {ws.ref_name}" + (f": {message}" if message else ""), self.clock()))
                status, merged_files = "merged", merged.auto_merged
            if self.refs.cas(self.main, main, new_main).ok:
                result.status, result.commit = status, new_main
                result.auto_merged = merged_files
                ws._adopt(new_main, new_main)
                return result
            # Someone else advanced main between our read and the CAS: start over on top of it.
        result.status = "contended"
        return result
