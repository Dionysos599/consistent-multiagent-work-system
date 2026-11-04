"""Read-modify-write of one JSON document in the store, serialized by compare-and-set."""

from __future__ import annotations

import json
from typing import Any, Callable, Optional, Tuple

from .kv import KV


def load(kv: KV, key: str, default: Any) -> Any:
    raw = kv.get(key)
    return json.loads(raw) if raw else default


def update(kv: KV, key: str, change: Callable[[Any], Any], default: Any, max_attempts: int = 200) -> Any:
    """Apply ``change`` to the document and store the result if it changed.

    ``change`` receives a fresh copy of the current document (or ``default``), edits it in place and
    returns the value the caller wants back. It runs again, on newer data, whenever another writer got
    in first, so it must not have side effects outside the document.
    """
    for _ in range(max_attempts):
        raw = kv.get(key)
        doc = json.loads(raw) if raw else json.loads(json.dumps(default))
        before = json.dumps(doc, sort_keys=True)
        result = change(doc)
        after = json.dumps(doc, sort_keys=True)
        if after == before and raw is not None:
            return result
        ok, _ = kv.cas(key, raw, after) if raw is not None else kv.put_if_absent(key, after)
        if ok:
            return result
    raise RuntimeError(f"{key} is too contended")
