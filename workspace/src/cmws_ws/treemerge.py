"""Three-way merge of directory trees.

Per name in a directory, with b = base, o = ours, t = theirs:

* o == t: take it; o == b: take theirs; t == b: take ours (one side did nothing);
* both sides changed it: directories on both sides merge recursively; two files are merged line by
  line when all three are text; everything else is a conflict:

  ==============  ==========================================================
  ``content``     overlapping text edits (carries the regions)
  ``binary``      both edited a file that is not valid UTF-8
  ``modify-delete`` one side deleted a file the other changed
  ``add-add``     both created the path with different content
  ``type``        one side has a file where the other has a directory
  ``rename-rename`` the two sides moved one file to different places
  ==============  ==========================================================

Renames: when one side moved a file (``ours_renames`` / ``theirs_renames``, see renames.py), the
moved path is first applied to the base and to the other side as well, so edits made to the file
under its old name are merged into it under its new name instead of colliding with a deletion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Tuple

from .objects import DIR, FILE, Entry, ObjectStore, Tree
from .textmerge import ConflictRegion, merge_text


@dataclass(frozen=True)
class PathConflictInfo:
    path: str
    kind: str
    base: Optional[str] = None  # blob ids, when the sides are files
    ours: Optional[str] = None
    theirs: Optional[str] = None
    regions: tuple = ()  # ConflictRegion tuples for 'content'
    detail: str = ""


@dataclass
class TreeMergeResult:
    tree: Optional[str]  # merged tree id; None when there are conflicts
    conflicts: List[PathConflictInfo] = field(default_factory=list)
    auto_merged: List[str] = field(default_factory=list)  # files merged line by line
    renames_followed: List[Tuple[str, str]] = field(default_factory=list)  # edits carried across

    @property
    def clean(self) -> bool:
        return not self.conflicts


def merge_trees(store: ObjectStore, base: Optional[str], ours: str, theirs: str,
                ours_label: str = "ours", theirs_label: str = "theirs",
                ours_renames: Optional[Mapping[str, str]] = None,
                theirs_renames: Optional[Mapping[str, str]] = None) -> TreeMergeResult:
    result = TreeMergeResult(None)
    ours_renames, theirs_renames = ours_renames or {}, theirs_renames or {}
    skip = set()
    for src, dst in ours_renames.items():
        other = theirs_renames.get(src)
        if other is not None and other != dst:
            result.conflicts.append(PathConflictInfo(
                src, "rename-rename",
                detail=f"{ours_label} moved it to {dst}, {theirs_label} moved it to {other}"))
            skip.add(src)
    for renames, other_renames, which in ((ours_renames, theirs_renames, "ours"),
                                          (theirs_renames, ours_renames, "theirs")):
        for src, dst in sorted(renames.items()):
            if src in skip:
                continue
            other = theirs if which == "ours" else ours
            if other_renames.get(src) == dst:
                base, _ = _move(store, base, src, dst)      # both moved it to the same place
                continue
            if other is not None and store.lookup(other, dst) is not None:
                continue   # the other side already has something there: let it surface as a conflict
            base, _ = _move(store, base, src, dst)
            if which == "ours":
                theirs, moved = _move(store, theirs, src, dst)
            else:
                ours, moved = _move(store, ours, src, dst)
            if moved:
                result.renames_followed.append((src, dst))
    merged = _merge_dir(store, base, ours, theirs, "", result, ours_label, theirs_label)
    if result.clean:
        result.tree = merged
    return result


def _move(store: ObjectStore, tree: Optional[str], src: str, dst: str):
    """Move a file inside a tree when the source is there and the destination is free."""
    if tree is None:
        return tree, False
    s, d = store.lookup(tree, src), store.lookup(tree, dst)
    if s is None or s.mode != FILE or d is not None:
        return tree, False
    return store.update(tree, {src: None, dst: s.id}), True


def _is_dir(e: Optional[Entry]) -> bool:
    return e is not None and e.mode == DIR


def _is_file(e: Optional[Entry]) -> bool:
    return e is not None and e.mode == FILE


def _merge_dir(store: ObjectStore, base: Optional[str], ours: Optional[str],
               theirs: Optional[str], prefix: str, result: TreeMergeResult,
               ol: str, tl: str) -> Optional[str]:
    if ours == theirs:
        return ours
    if base == ours:
        return theirs
    if base == theirs:
        return ours
    eb = store.get_tree(base).entries if base else {}
    eo = store.get_tree(ours).entries if ours else {}
    et = store.get_tree(theirs).entries if theirs else {}
    out: Dict[str, Entry] = {}
    for name in sorted(set(eb) | set(eo) | set(et)):
        b, o, t = eb.get(name), eo.get(name), et.get(name)
        path = f"{prefix}/{name}" if prefix else name
        if o == t:
            chosen = o
        elif o == b:
            chosen = t
        elif t == b:
            chosen = o
        elif _is_dir(o) and _is_dir(t):
            sub = _merge_dir(store, b.id if _is_dir(b) else None, o.id, t.id, path, result, ol, tl)
            chosen = Entry(DIR, sub) if sub else None
        elif _is_file(o) and _is_file(t):
            chosen = _merge_file(store, b if _is_file(b) else None, o, t, path, result, ol, tl)
        else:
            chosen = None
            result.conflicts.append(_classify(b, o, t, path))
        if chosen is not None:
            out[name] = chosen
    if not out:
        return None
    return store.put_tree(Tree(out))


def _classify(b: Optional[Entry], o: Optional[Entry], t: Optional[Entry],
              path: str) -> PathConflictInfo:
    def fid(e: Optional[Entry]) -> Optional[str]:
        return e.id if _is_file(e) else None

    if (_is_dir(o) and _is_file(t)) or (_is_file(o) and _is_dir(t)):
        return PathConflictInfo(path, "type", fid(b), fid(o), fid(t))
    if o is None or t is None:
        return PathConflictInfo(path, "modify-delete", fid(b), fid(o), fid(t))
    return PathConflictInfo(path, "add-add", fid(b), fid(o), fid(t))


def _merge_file(store: ObjectStore, b: Optional[Entry], o: Entry, t: Entry, path: str,
                result: TreeMergeResult, ol: str, tl: str) -> Optional[Entry]:
    ob, tb = store.get_blob(o.id), store.get_blob(t.id)
    bb = store.get_blob(b.id) if b else b""
    try:
        base_s, ours_s, theirs_s = bb.decode("utf-8"), ob.decode("utf-8"), tb.decode("utf-8")
    except UnicodeDecodeError:
        result.conflicts.append(PathConflictInfo(path, "binary", b.id if b else None, o.id, t.id))
        return None
    merged = merge_text(base_s, ours_s, theirs_s, ours_label=ol, theirs_label=tl)
    if merged.clean:
        result.auto_merged.append(path)
        return Entry(FILE, store.put_blob(merged.text.encode("utf-8")))
    kind = "add-add" if b is None else "content"
    result.conflicts.append(PathConflictInfo(
        path, kind, b.id if b else None, o.id, t.id, tuple(merged.conflicts)))
    return None
