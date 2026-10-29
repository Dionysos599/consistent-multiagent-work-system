"""Line-based three-way merge (diff3).

``merge3(base, ours, theirs)`` splits each side into hunks relative to the base and combines them:

* a hunk made by only one side is applied;
* hunks that overlap or touch (like git, adjacent edits are not trusted to be independent) become
  a conflict unless both sides replaced the region with identical lines.
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class Hunk:
    start: int  # base[start:end] is replaced by ``lines``
    end: int
    lines: Tuple[str, ...]


@dataclass(frozen=True)
class ConflictRegion:
    base: Tuple[str, ...]
    ours: Tuple[str, ...]
    theirs: Tuple[str, ...]
    base_start: int  # 0-based line in the base where the region begins


@dataclass
class MergeResult:
    lines: List[str]  # merged text; conflicting regions are rendered with markers
    conflicts: List[ConflictRegion]

    @property
    def clean(self) -> bool:
        return not self.conflicts

    @property
    def text(self) -> str:
        return "".join(self.lines)


def split_lines(text: str) -> List[str]:
    return text.splitlines(keepends=True)


def hunks(base: Sequence[str], side: Sequence[str]) -> List[Hunk]:
    sm = difflib.SequenceMatcher(None, base, side, autojunk=False)
    return [Hunk(i1, i2, tuple(side[j1:j2]))
            for tag, i1, i2, j1, j2 in sm.get_opcodes() if tag != "equal"]


def _apply(base: Sequence[str], lo: int, hi: int, hs: Sequence[Hunk]) -> Tuple[str, ...]:
    out: List[str] = []
    pos = lo
    for h in hs:
        out.extend(base[pos:h.start])
        out.extend(h.lines)
        pos = h.end
    out.extend(base[pos:hi])
    return tuple(out)


def _marker_block(c: ConflictRegion, ours_label: str, theirs_label: str) -> List[str]:
    def terminated(lines):
        lines = list(lines)
        if lines and not lines[-1].endswith("\n"):
            lines[-1] += "\n"
        return lines

    return ([f"<<<<<<< {ours_label}\n"] + terminated(c.ours) + ["||||||| base\n"]
            + terminated(c.base) + ["=======\n"] + terminated(c.theirs)
            + [f">>>>>>> {theirs_label}\n"])


def merge3(base: Sequence[str], ours: Sequence[str], theirs: Sequence[str],
           ours_label: str = "ours", theirs_label: str = "theirs") -> MergeResult:
    a, b = hunks(base, ours), hunks(base, theirs)
    out: List[str] = []
    conflicts: List[ConflictRegion] = []
    pos = 0
    i = j = 0
    while i < len(a) or j < len(b):
        # Start a cluster with whichever hunk begins first, then absorb every hunk that overlaps
        # or touches the cluster, from either side.
        take_a = j >= len(b) or (i < len(a) and a[i].start <= b[j].start)
        first = a[i] if take_a else b[j]
        lo, hi = first.start, first.end
        ca: List[Hunk] = []
        cb: List[Hunk] = []
        if take_a:
            ca.append(a[i]); i += 1
        else:
            cb.append(b[j]); j += 1
        grew = True
        while grew:
            grew = False
            while i < len(a) and a[i].start <= hi:
                ca.append(a[i]); hi = max(hi, a[i].end); i += 1; grew = True
            while j < len(b) and b[j].start <= hi:
                cb.append(b[j]); hi = max(hi, b[j].end); j += 1; grew = True
        out.extend(base[pos:lo])
        pos = hi
        if not cb:
            out.extend(_apply(base, lo, hi, ca))
        elif not ca:
            out.extend(_apply(base, lo, hi, cb))
        else:
            mine, yours = _apply(base, lo, hi, ca), _apply(base, lo, hi, cb)
            if mine == yours:
                out.extend(mine)
            else:
                region = ConflictRegion(tuple(base[lo:hi]), mine, yours, lo)
                conflicts.append(region)
                out.extend(_marker_block(region, ours_label, theirs_label))
    out.extend(base[pos:])
    return MergeResult(out, conflicts)


def merge_text(base: Optional[str], ours: str, theirs: str, **labels: str) -> MergeResult:
    return merge3(split_lines(base or ""), split_lines(ours), split_lines(theirs), **labels)
