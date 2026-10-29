"""Content-addressed, immutable objects: blob, tree and commit.

Every object lives in the key/value store under ``obj/<sha256>``. The id is the hash of the
object's canonical bytes, so writing an object is idempotent (``put_if_absent``) and two agents
that produce the same content produce the same id without coordinating. Because objects never
change, they can be cached forever and a fork costs nothing: it is just a commit id.

Stored value format: a one-letter tag, a colon, then the body.

    u:<text>      blob that is valid UTF-8, stored verbatim
    b:<base64>    any other blob
    m:<json>      manifest of a large blob: {"size": n, "chunks": [chunk ids]}
    k:<base64>    one chunk of a large blob
    t:<json>      tree
    c:<json>      commit

Blobs larger than ``CHUNK_BYTES`` are split into fixed-size chunks, each stored as its own object
(identical chunks are stored once), and the blob id stays the hash of the whole content, so a
file has the same id whether or not it was chunked. No single value in the store exceeds roughly
a chunk, which keeps replication messages small.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Optional, Tuple

from .kv import KV
from .paths import InvalidPath, normalize, split

FILE = "file"
DIR = "dir"

CHUNK_BYTES = 256 * 1024
MAX_BLOB_BYTES = 256 * 1024 * 1024


class ObjectError(Exception):
    pass


class MissingObject(ObjectError):
    pass


class CorruptObject(ObjectError):
    pass


class PathConflict(ObjectError):
    """A path would need to be both a file and a directory."""


@dataclass(frozen=True)
class Entry:
    mode: str  # FILE or DIR
    id: str


@dataclass(frozen=True)
class Tree:
    entries: Mapping[str, Entry]

    def __post_init__(self) -> None:
        object.__setattr__(self, "entries", dict(sorted(self.entries.items())))

    def to_json(self) -> str:
        return json.dumps({n: [e.mode, e.id] for n, e in self.entries.items()},
                          sort_keys=True, separators=(",", ":"))

    @staticmethod
    def from_json(text: str) -> "Tree":
        raw = json.loads(text)
        return Tree({n: Entry(m, i) for n, (m, i) in raw.items()})


@dataclass(frozen=True)
class Commit:
    tree: str
    parents: Tuple[str, ...]
    author: str
    message: str
    timestamp: float
    # (old path, new path) pairs the author moved on purpose. A hint for merging; the trees are
    # the truth. Left out of the hash input when empty so commits without it keep their ids.
    renames: Tuple[Tuple[str, str], ...] = ()

    def to_json(self) -> str:
        d = {"tree": self.tree, "parents": list(self.parents), "author": self.author,
             "message": self.message, "timestamp": self.timestamp}
        if self.renames:
            d["renames"] = [list(r) for r in self.renames]
        return json.dumps(d, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def from_json(text: str) -> "Commit":
        d = json.loads(text)
        return Commit(d["tree"], tuple(d["parents"]), d["author"], d["message"], d["timestamp"],
                      tuple((a, b) for a, b in d.get("renames", ())))


def _hash(kind: str, payload: bytes) -> str:
    h = hashlib.sha256()
    h.update(kind.encode() + b" " + str(len(payload)).encode() + b"\0")
    h.update(payload)
    return h.hexdigest()


def blob_id(data: bytes) -> str:
    return _hash("blob", data)


class ObjectStore:
    """Reads and writes objects through a KV, with a process-local cache (objects are immutable)."""

    PREFIX = "obj/"

    def __init__(self, kv: KV) -> None:
        self._kv = kv
        self._cache: Dict[str, object] = {}
        self._manifests: Dict[str, Tuple[str, ...]] = {}
        # Every object this process stored, with the size of its stored value. Used to measure
        # how much of what was written is still reachable (see gc.py); not a global inventory.
        self.written: Dict[str, int] = {}

    # ---- writing -------------------------------------------------------------------------

    def _store(self, oid: str, value: str, cached: object) -> str:
        if oid not in self._cache:
            self._put(oid, value)
            self._cache[oid] = cached
        return oid

    def _put(self, oid: str, value: str) -> None:
        self._kv.put_if_absent(self.PREFIX + oid, value)
        self.written[oid] = len(value.encode("utf-8"))

    def put_blob(self, data: bytes) -> str:
        if len(data) > MAX_BLOB_BYTES:
            raise ObjectError(f"blob of {len(data)} bytes exceeds the {MAX_BLOB_BYTES} byte limit")
        oid = blob_id(data)
        if len(data) > CHUNK_BYTES:
            if oid not in self._cache:
                chunks = []
                for i in range(0, len(data), CHUNK_BYTES):
                    part = data[i:i + CHUNK_BYTES]
                    cid = _hash("chunk", part)
                    if cid not in self.written:
                        self._put(cid, "k:" + base64.b64encode(part).decode("ascii"))
                    chunks.append(cid)
                manifest = json.dumps({"size": len(data), "chunks": chunks},
                                      separators=(",", ":"))
                self._put(oid, "m:" + manifest)
                self._manifests[oid] = tuple(chunks)
                self._cache[oid] = data
            return oid
        try:
            value = "u:" + data.decode("utf-8")
        except UnicodeDecodeError:
            value = "b:" + base64.b64encode(data).decode("ascii")
        return self._store(oid, value, data)

    def put_tree(self, tree: Tree) -> str:
        body = tree.to_json()
        return self._store(_hash("tree", body.encode()), "t:" + body, tree)

    def put_commit(self, commit: Commit) -> str:
        body = commit.to_json()
        return self._store(_hash("commit", body.encode()), "c:" + body, commit)

    # ---- reading -------------------------------------------------------------------------

    def _load(self, oid: str) -> object:
        cached = self._cache.get(oid)
        if cached is not None:
            return cached
        raw = self._kv.get(self.PREFIX + oid)
        if raw is None:
            raise MissingObject(oid)
        tag, _, body = raw.partition(":")
        if tag == "u":
            obj: object = body.encode("utf-8")
            actual = blob_id(obj)  # type: ignore[arg-type]
        elif tag == "b":
            obj = base64.b64decode(body)
            actual = blob_id(obj)  # type: ignore[arg-type]
        elif tag == "m":
            manifest = json.loads(body)
            chunks = tuple(manifest["chunks"])
            obj = b"".join(self._chunk(c) for c in chunks)
            if len(obj) != manifest["size"]:
                raise CorruptObject(f"{oid}: manifest says {manifest['size']} bytes, got {len(obj)}")
            actual = blob_id(obj)  # type: ignore[arg-type]
            self._manifests[oid] = chunks
        elif tag == "t":
            obj = Tree.from_json(body)
            actual = _hash("tree", body.encode())
        elif tag == "c":
            obj = Commit.from_json(body)
            actual = _hash("commit", body.encode())
        else:
            raise CorruptObject(f"{oid}: unknown tag {tag!r}")
        if actual != oid:
            raise CorruptObject(f"{oid}: content hashes to {actual}")
        self._cache[oid] = obj
        return obj

    def _chunk(self, cid: str) -> bytes:
        raw = self._kv.get(self.PREFIX + cid)
        if raw is None:
            raise MissingObject(cid)
        tag, _, body = raw.partition(":")
        data = base64.b64decode(body)
        if tag != "k" or _hash("chunk", data) != cid:
            raise CorruptObject(f"{cid}: not a valid chunk")
        return data

    def blob_chunks(self, oid: str) -> Tuple[str, ...]:
        """Ids of the chunk objects behind a blob (empty for a blob stored in one piece)."""
        if oid in self._manifests:
            return self._manifests[oid]
        raw = self._kv.get(self.PREFIX + oid)
        if raw is not None and raw.startswith("m:"):
            self._manifests[oid] = tuple(json.loads(raw[2:])["chunks"])
            return self._manifests[oid]
        return ()

    def get_blob(self, oid: str) -> bytes:
        obj = self._load(oid)
        if not isinstance(obj, bytes):
            raise CorruptObject(f"{oid} is not a blob")
        return obj

    def get_tree(self, oid: str) -> Tree:
        obj = self._load(oid)
        if not isinstance(obj, Tree):
            raise CorruptObject(f"{oid} is not a tree")
        return obj

    def get_commit(self, oid: str) -> Commit:
        obj = self._load(oid)
        if not isinstance(obj, Commit):
            raise CorruptObject(f"{oid} is not a commit")
        return obj

    def empty_tree(self) -> str:
        return self.put_tree(Tree({}))

    # ---- tree operations -----------------------------------------------------------------

    def lookup(self, tree_id: str, path: str) -> Optional[Entry]:
        """The entry at ``path`` below ``tree_id`` (the root itself for ''), or None."""
        parts = split(path)
        if not parts:
            return Entry(DIR, tree_id)
        entry = Entry(DIR, tree_id)
        for part in parts:
            if entry.mode != DIR:
                return None
            nxt = self.get_tree(entry.id).entries.get(part)
            if nxt is None:
                return None
            entry = nxt
        return entry

    def read_file(self, tree_id: str, path: str) -> Optional[bytes]:
        entry = self.lookup(tree_id, path)
        if entry is None or entry.mode != FILE:
            return None
        return self.get_blob(entry.id)

    def flatten(self, tree_id: str, prefix: str = "") -> Dict[str, str]:
        """All files below a tree as {path: blob id}."""
        out: Dict[str, str] = {}
        for name, entry in self.get_tree(tree_id).entries.items():
            path = f"{prefix}/{name}" if prefix else name
            if entry.mode == FILE:
                out[path] = entry.id
            else:
                out.update(self.flatten(entry.id, path))
        return out

    def build(self, files: Mapping[str, str]) -> str:
        """Tree id for a {path: blob id} map. Directories exist only while they hold a file."""
        return self.update(None, files)

    def update(self, tree_id: Optional[str], changes: Mapping[str, Optional[str]]) -> str:
        """Apply {path: blob id, or None to delete} to a tree (None: the empty tree).

        Only the directories on the way to a change are rewritten, each once, so a batch leaves no
        intermediate trees behind. Deleting a directory path removes its whole subtree. Raises
        PathConflict if a change would make a path both a file and a directory.
        """
        root = _Node()
        for path, bid in changes.items():
            p = normalize(path)
            if not p:
                raise InvalidPath("cannot write to the root")
            node = root
            for part in split(p):
                node = node.children.setdefault(part, _Node())
            node.leaf, node.has_leaf = bid, True
        result = self._apply(tree_id, root, "")
        return result if result is not None else self.empty_tree()

    def _apply(self, tree_id: Optional[str], node: "_Node", prefix: str) -> Optional[str]:
        entries = dict(self.get_tree(tree_id).entries) if tree_id else {}
        for name, child in node.children.items():
            path = f"{prefix}/{name}" if prefix else name
            current = entries.get(name)
            if child.has_leaf:
                if child.leaf is None:                 # delete (a directory goes with its contents)
                    entries.pop(name, None)
                    current = None
                else:
                    if child.children:
                        # a file replacing a directory: legal only if the batch empties it
                        if not child.only_deletes(children_only=True):
                            raise PathConflict(f"{path} cannot be both a file and a directory")
                        if current is not None and current.mode == DIR:
                            if self._apply(current.id, child, path) is not None:
                                raise PathConflict(f"{path} is a directory")
                    elif current is not None and current.mode == DIR:
                        raise PathConflict(f"{path} is a directory")
                    entries[name] = Entry(FILE, child.leaf)
                    continue
            if child.children:
                if current is not None and current.mode == FILE:
                    if child.only_deletes():
                        continue                        # nothing below a file to delete
                    raise PathConflict(f"{path} is a file, cannot create anything below it")
                sub = self._apply(current.id if current is not None else None, child, path)
                if sub is None:
                    entries.pop(name, None)
                else:
                    entries[name] = Entry(DIR, sub)
        if not entries:
            return None
        if tree_id and entries == dict(self.get_tree(tree_id).entries):
            return tree_id
        return self.put_tree(Tree(entries))


class _Node:
    """One path component in a batch of tree changes."""

    def __init__(self) -> None:
        self.children: Dict[str, "_Node"] = {}
        self.leaf: Optional[str] = None
        self.has_leaf = False

    def only_deletes(self, children_only: bool = False) -> bool:
        if not children_only and self.has_leaf and self.leaf is not None:
            return False
        return all(c.only_deletes() for c in self.children.values())
