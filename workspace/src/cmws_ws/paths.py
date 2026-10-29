"""Workspace paths: relative, slash-separated, no '.', '..' or empty components."""

from __future__ import annotations

from typing import Tuple


class InvalidPath(ValueError):
    pass


def normalize(path: str) -> str:
    """Return the canonical form of ``path`` ('' is the root)."""
    if not isinstance(path, str):
        raise InvalidPath(f"path must be a string, got {type(path).__name__}")
    parts = [p for p in path.split("/") if p not in ("",)]
    for p in parts:
        if p in (".", "..") or "\0" in p:
            raise InvalidPath(f"invalid path component {p!r} in {path!r}")
    return "/".join(parts)


def split(path: str) -> Tuple[str, ...]:
    path = normalize(path)
    return tuple(path.split("/")) if path else ()


def join(*parts: str) -> str:
    return "/".join(p for p in parts if p)


def parent(path: str) -> str:
    path = normalize(path)
    return path.rpartition("/")[0]
