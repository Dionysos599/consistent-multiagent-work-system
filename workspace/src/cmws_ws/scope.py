"""Path scopes: the set of files and folders a task expects to touch.

A scope is a tuple of normalized paths; each path covers itself and everything below it. The empty
scope means "unknown", which is treated as the whole project so that nothing unsafe runs beside it.
"""

from __future__ import annotations

from typing import Iterable, Tuple

from .paths import normalize

Scope = Tuple[str, ...]


def make_scope(paths: Iterable[str]) -> Scope:
    """Normalized, de-duplicated, with paths already covered by a parent folder dropped."""
    norm = sorted({normalize(p) for p in paths})
    if not norm or "" in norm:
        return ("",)                      # unknown or explicitly everything
    kept = []
    for p in norm:
        if not any(covers(k, p) for k in kept):
            kept.append(p)
    return tuple(kept)


def covers(outer: str, inner: str) -> bool:
    """True when ``inner`` is ``outer`` or lies below it."""
    return outer == "" or inner == outer or inner.startswith(outer + "/")


def overlaps(a: Scope, b: Scope) -> bool:
    return any(covers(x, y) or covers(y, x) for x in a for y in b)


def inside(scope: Scope, path: str) -> bool:
    p = normalize(path)
    return any(covers(s, p) for s in scope)
