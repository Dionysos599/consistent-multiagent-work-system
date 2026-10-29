"""Refs: the only mutable, strongly consistent state in the system.

A ref is a name that points at a commit id. Moving it is a compare-and-set, so concurrent writers
are serialised by the store and the history of a ref is a single line. Everything else (objects)
is immutable and needs no coordination.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from .kv import KV

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-/]*$")


class InvalidRefName(ValueError):
    pass


@dataclass(frozen=True)
class CasResult:
    ok: bool
    current: Optional[str]  # the ref's value after the call


class RefStore:
    PREFIX = "ref/"

    def __init__(self, kv: KV) -> None:
        self._kv = kv

    def _key(self, name: str) -> str:
        if not _NAME.match(name) or ".." in name or name.endswith("/"):
            raise InvalidRefName(name)
        return self.PREFIX + name

    def get(self, name: str) -> Optional[str]:
        return self._kv.get(self._key(name))

    def cas(self, name: str, expected: Optional[str], new: str) -> CasResult:
        """Move ``name`` from ``expected`` (None: it must not exist) to ``new``.

        Ref values are commit ids, which are unique per commit, so if the store reports a failure
        but the ref already holds ``new``, an earlier attempt of this very call succeeded and its
        reply was lost (the HTTP client retries). That is reported as success.
        """
        if new is None:
            raise ValueError("a ref cannot be deleted, only moved")
        ok, current = self._kv.cas(self._key(name), expected, new)
        if not ok and current == new and expected != new:
            return CasResult(True, current)
        return CasResult(ok, current)

    def create(self, name: str, value: str) -> CasResult:
        return self.cas(name, None, value)
