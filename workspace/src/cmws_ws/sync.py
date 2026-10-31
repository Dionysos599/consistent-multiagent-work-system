"""Local checkouts: a real directory kept in sync with a workspace branch.

A person (or a tool that works on files) edits a normal folder; the cloud side is the branch
``ws/<agent>`` in the store, which agents write to as well. Syncing is the same fetch / merge /
push that submitting agent work uses:

    pull   fetch the branch tip; three-way merge it with the edits sitting in the folder
           (base = the commit the folder last matched); apply the result to the folder.
    push   pull if the branch moved, commit the folder's changes on top of the tip, and
           compare-and-set the branch. If someone else moved it first, pull again and retry.

Conflicting text edits are written into the file with conflict markers, as git does; ``push`` is
blocked until the markers are gone (or ``resolve`` is called for conflicts that are not text).
The folder keeps its bookkeeping in ``.cmws/state.json``: the base commit and, per file, the blob
id (and file size and mtime) it had when last synced, which is how local changes are found without
hashing everything every time.
"""

from __future__ import annotations

import fnmatch
import json
import os
import re
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple

from .diff import ADDED, DELETED, MODIFIED, diff_trees
from .kv import KVUnavailable
from .objects import Commit, blob_id
from .paths import InvalidPath, normalize
from .renames import moved_between, side_renames
from .repo import Repository
from .treemerge import merge_trees

STATE_DIR = ".cmws"
DEFAULT_IGNORE = (".git", "__pycache__", ".DS_Store", ".cmwsignore")
MARKERS = re.compile(r"^<<<<<<< .*$(?:.|\n)*?^>>>>>>> ", re.M)
RACY_NS = 2_000_000_000          # a file modified this recently is hashed even if its stat matches
TEXT_CONFLICTS = ("content", "add-add")


class SyncError(Exception):
    pass


@dataclass
class PullResult:
    status: str                      # 'up-to-date' | 'updated' | 'conflict'
    updated: List[str] = field(default_factory=list)
    conflicts: Dict[str, str] = field(default_factory=dict)   # path -> kind


@dataclass
class PushResult:
    status: str                      # 'pushed' | 'nothing' | 'conflict' | 'blocked' | 'contended'
    commit: Optional[str] = None
    files: List[str] = field(default_factory=list)
    conflicts: Dict[str, str] = field(default_factory=dict)


class Checkout:
    def __init__(self, repo: Repository, root: Path, state: dict) -> None:
        self.repo = repo
        self.root = Path(root)
        self.state = state
        self._ignore_patterns: Optional[List[str]] = None

    # ---- creating and opening ------------------------------------------------------------

    @classmethod
    def clone(cls, repo: Repository, agent_id: str, root) -> "Checkout":
        """Create the folder from a workspace branch, forking the branch from main if it is new."""
        root = Path(root)
        if (root / STATE_DIR / "state.json").exists():
            raise SyncError(f"{root} is already a checkout")
        if root.exists() and any(root.iterdir()):
            raise SyncError(f"{root} is not empty")
        if repo.refs.get(f"ws/{agent_id}") is None:
            repo.fork(agent_id)
        tip = repo.refs.get(f"ws/{agent_id}")
        root.mkdir(parents=True, exist_ok=True)
        co = cls(repo, root, {"agent": agent_id, "base": tip, "files": {}, "conflicts": {}})
        tree = repo.store.get_commit(tip).tree
        co._apply(repo.store.empty_tree(), tree)
        co._rebuild_manifest(tree)
        co._save()
        return co

    @classmethod
    def open(cls, repo: Repository, root) -> "Checkout":
        path = Path(root) / STATE_DIR / "state.json"
        if not path.exists():
            raise SyncError(f"{root} is not a checkout (no {STATE_DIR}/state.json)")
        return cls(repo, Path(root), json.loads(path.read_text()))

    # ---- small helpers -------------------------------------------------------------------

    @property
    def agent_id(self) -> str:
        return self.state["agent"]

    @property
    def base(self) -> str:
        return self.state["base"]

    @property
    def ref(self) -> str:
        return f"ws/{self.agent_id}"

    @property
    def conflicts(self) -> Dict[str, str]:
        return self.state["conflicts"]

    def tip(self) -> Optional[str]:
        return self.repo.refs.get(self.ref)

    def _tree(self, commit_id: str) -> str:
        return self.repo.store.get_commit(commit_id).tree

    def _save(self) -> None:
        d = self.root / STATE_DIR
        d.mkdir(exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=d, prefix="state-")
        with os.fdopen(fd, "w") as f:
            json.dump(self.state, f, sort_keys=True)
        os.replace(tmp, d / "state.json")

    def _patterns(self) -> List[str]:
        if self._ignore_patterns is None:
            pats = list(DEFAULT_IGNORE)
            f = self.root / ".cmwsignore"
            if f.exists():
                pats += [ln.strip() for ln in f.read_text().splitlines()
                         if ln.strip() and not ln.startswith("#")]
            self._ignore_patterns = pats
        return self._ignore_patterns

    def ignored(self, rel: str) -> bool:
        parts = rel.split("/")
        if parts[0] == STATE_DIR:
            return True
        return any(fnmatch.fnmatch(part, pat) or fnmatch.fnmatch(rel, pat)
                   for pat in self._patterns() for part in parts)

    def _abs(self, rel: str) -> Path:
        p = self.root / rel
        real_root = os.path.realpath(self.root)
        if not os.path.realpath(p.parent).startswith(real_root):
            raise SyncError(f"{rel} would leave the checkout")
        return p

    def _walk(self) -> Iterator[Tuple[str, os.stat_result]]:
        stack = [""]
        while stack:
            rel_dir = stack.pop()
            try:
                entries = list(os.scandir(self.root / rel_dir if rel_dir else self.root))
            except FileNotFoundError:
                continue
            for e in entries:
                rel = f"{rel_dir}/{e.name}" if rel_dir else e.name
                if self.ignored(rel) or e.is_symlink():
                    continue
                if e.is_dir(follow_symlinks=False):
                    stack.append(rel)
                elif e.is_file(follow_symlinks=False):
                    try:
                        normalize(rel)
                    except InvalidPath:
                        continue
                    yield rel, e.stat(follow_symlinks=False)

    # ---- finding local changes -----------------------------------------------------------

    def scan(self) -> Dict[str, Optional[bytes]]:
        """Changes in the folder since it was last synced: {path: new content, None = deleted}."""
        files = self.state["files"]
        changes: Dict[str, Optional[bytes]] = {}
        seen = set()
        now = time.time_ns()
        for rel, st in self._walk():
            seen.add(rel)
            entry = files.get(rel)
            if (entry and entry[1] == st.st_size and entry[2] == st.st_mtime_ns
                    and now - st.st_mtime_ns > RACY_NS):
                continue
            data = (self.root / rel).read_bytes()
            bid = blob_id(data)
            if entry and entry[0] == bid:
                files[rel] = [bid, st.st_size, st.st_mtime_ns]    # touched but unchanged
                continue
            changes[rel] = data
        for rel in files:
            if rel not in seen and not self.ignored(rel):
                changes[rel] = None
        return changes

    def status(self) -> Dict[str, str]:
        """{path: 'A'|'M'|'D'} for local changes, like ``git status --short``."""
        files = self.state["files"]
        return {p: ("D" if d is None else "M" if p in files else "A")
                for p, d in sorted(self.scan().items())}

    # ---- writing the folder --------------------------------------------------------------

    def _apply(self, from_tree: str, to_tree: str) -> List[str]:
        """Make the folder go from ``from_tree`` to ``to_tree`` (the folder is believed to hold
        ``from_tree``). A file whose content is not what we think is left alone and returned."""
        store = self.repo.store
        raced: List[str] = []
        changes = diff_trees(store, from_tree, to_tree)

        def on_disk(rel: str) -> Optional[str]:
            p = self.root / rel
            return blob_id(p.read_bytes()) if p.is_file() and not p.is_symlink() else None

        for c in changes:
            if c.kind == DELETED:
                if on_disk(c.path) == c.old:
                    self._abs(c.path).unlink()
                    self._prune(c.path)
                    self.state["files"].pop(c.path, None)
                elif on_disk(c.path) is not None:
                    raced.append(c.path)
        for c in changes:
            if c.kind == DELETED:
                continue
            if on_disk(c.path) != c.old:
                raced.append(c.path)
                continue
            dest = self._abs(c.path)
            blocker = next((p for p in dest.parents if p != self.root and p.is_file()), None)
            if blocker is not None:
                raced.append(c.path)
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            data = store.get_blob(c.new)
            fd, tmp = tempfile.mkstemp(dir=dest.parent, prefix=".cmws-tmp-")
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, dest)
            st = dest.stat()
            self.state["files"][c.path] = [c.new, st.st_size, st.st_mtime_ns]
        return raced

    def _prune(self, rel: str) -> None:
        d = (self.root / rel).parent
        while d != self.root:
            try:
                d.rmdir()            # only succeeds on empty directories
            except OSError:
                return
            d = d.parent

    def _rebuild_manifest(self, tree: str) -> None:
        old = self.state["files"]
        self.state["files"] = {
            p: (old[p] if p in old and old[p][0] == b else [b, None, None])
            for p, b in self.repo.store.flatten(tree).items()}

    # ---- pull ----------------------------------------------------------------------------

    def pull(self) -> PullResult:
        tip = self.tip()
        if tip is None:
            raise SyncError(f"{self.ref} does not exist")
        if tip == self.base:
            return PullResult("up-to-date")
        store = self.repo.store
        base_tree, tip_tree = self._tree(self.base), self._tree(tip)
        local = self.scan()
        conflicts: Dict[str, str] = {}
        if local:
            ours_tree = store.update(base_tree, {p: (store.put_blob(d) if d is not None else None)
                                                 for p, d in local.items()})
            merged = merge_trees(
                store, base_tree, ours_tree, tip_tree, "local", "remote",
                ours_renames=moved_between(store, base_tree, ours_tree),
                theirs_renames=side_renames(store, self.base, tip), keep_going=True)
            target = merged.tree
            conflicts = {c.path: c.kind for c in merged.conflicts}
        else:
            ours_tree, target = base_tree, tip_tree
        before = set(self.state["files"])
        raced = self._apply(ours_tree, target)
        for p in raced:
            conflicts.setdefault(p, "raced")
        changed = [c.path for c in diff_trees(store, ours_tree, target)]
        self.state["base"] = tip
        self._rebuild_manifest(tip_tree)
        self.state["conflicts"].update(conflicts)
        self._save()
        return PullResult("conflict" if conflicts else "updated", sorted(changed), conflicts)

    # ---- conflicts -----------------------------------------------------------------------

    def unresolved(self) -> Dict[str, str]:
        """Conflicts still open. Text conflicts close themselves once the markers are gone."""
        open_: Dict[str, str] = {}
        for path, kind in list(self.conflicts.items()):
            if kind in TEXT_CONFLICTS:
                p = self.root / path
                if not p.exists():
                    del self.conflicts[path]
                    continue
                try:
                    has_markers = bool(MARKERS.search(p.read_text(encoding="utf-8")))
                except UnicodeDecodeError:
                    has_markers = False
                if not has_markers:
                    del self.conflicts[path]
                    continue
            open_[path] = kind
        return open_

    def resolve(self, path: str) -> None:
        """Declare a conflict handled (the file in the folder is what should be kept)."""
        self.conflicts.pop(normalize(path), None)
        self._save()

    # ---- push ----------------------------------------------------------------------------

    def push(self, message: Optional[str] = None, max_attempts: int = 20) -> PushResult:
        store = self.repo.store
        for _ in range(max_attempts):
            open_ = self.unresolved()
            if open_:
                self._save()
                return PushResult("blocked", conflicts=open_)
            if self.tip() != self.base:
                pulled = self.pull()
                if pulled.conflicts:
                    return PushResult("conflict", conflicts=pulled.conflicts)
            local = self.scan()
            if not local:
                self._save()
                return PushResult("nothing")
            base_tree = self._tree(self.base)
            tree = store.update(base_tree, {p: (store.put_blob(d) if d is not None else None)
                                            for p, d in local.items()})
            moves = moved_between(store, base_tree, tree)
            cid = store.put_commit(Commit(
                tree, (self.base,), self.agent_id, message or "Local changes",
                self.repo.clock(), tuple(sorted(moves.items()))))
            if self.repo.refs.cas(self.ref, self.base, cid).ok:
                files = self.state["files"]
                for p, d in local.items():
                    if d is None:
                        files.pop(p, None)
                    else:
                        files[p] = [blob_id(d), None, None]
                self.state["base"] = cid
                self._save()
                return PushResult("pushed", cid, sorted(local))
            # the branch moved between our read and the CAS: merge what arrived and try again
        return PushResult("contended")

    def submit(self, message: Optional[str] = None):
        """Push, then land the branch on main (``Repository.submit``) and catch up with it."""
        pushed = self.push(message)
        if pushed.status in ("blocked", "conflict", "contended"):
            return pushed
        result = self.repo.submit(self.repo.resume(self.agent_id), message)
        if result.landed:
            self.pull()
        return result


class Watcher:
    """Keeps a checkout in sync in the background by polling.

    Every ``interval`` seconds: if the branch moved remotely, pull; if the folder has changes that
    stayed the same for ``debounce`` seconds, push. When the store cannot be reached the watcher
    goes ``offline``: edits keep accumulating in the folder and are pushed (after merging whatever
    happened remotely) when the store is back. After a conflict it stops pushing until the
    markers are resolved.
    """

    def __init__(self, checkout: Checkout, interval: float = 0.5, debounce: float = 1.0,
                 message: str = "Local changes",
                 on_event: Optional[Callable[[str, object], None]] = None) -> None:
        self.checkout = checkout
        self.interval = interval
        self.debounce = debounce
        self.message = message
        self.on_event = on_event or (lambda kind, info: None)
        self.state = "idle"             # idle | offline | conflict
        self._sig: Optional[tuple] = None
        self._since = 0.0
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def run_once(self, now: Optional[float] = None) -> str:
        """One polling step; returns what happened. Separate from the thread so tests can drive it."""
        now = time.monotonic() if now is None else now
        co = self.checkout
        try:
            if co.unresolved():
                self.state = "conflict"
                return "conflict"
            if co.tip() != co.base:
                pulled = co.pull()
                self.on_event("pull", pulled)
                if pulled.conflicts:
                    self.state = "conflict"
                    return "conflict"
            local = co.scan()
            if not local:
                self._sig = None
                self.state = "idle"
                return "idle"
            sig = tuple(sorted((p, None if d is None else blob_id(d)) for p, d in local.items()))
            if sig != self._sig:
                self._sig, self._since = sig, now      # still changing: wait for it to settle
                self.state = "idle"
                return "waiting"
            if now - self._since < self.debounce:
                return "waiting"
            pushed = co.push(self.message)
            self.on_event("push", pushed)
            self._sig = None
            self.state = "conflict" if pushed.status in ("conflict", "blocked") else "idle"
            return pushed.status
        except KVUnavailable:
            self.state = "offline"
            self.on_event("offline", None)
            return "offline"

    def start(self) -> None:
        def loop() -> None:
            while not self._stop.is_set():
                self.run_once()
                self._stop.wait(self.interval * (4 if self.state == "offline" else 1))
        self._thread = threading.Thread(target=loop, name="cmws-watcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
