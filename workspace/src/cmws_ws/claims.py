"""Task claims: at most one live owner per task, with takeover after a crash.

A claim says "this orchestrator is running task T until time X". Claims expire, so a crashed
orchestrator stops blocking the task once its claim runs out; the next claimant gets a higher
``epoch``. The epoch is a fencing token: the old owner, should it turn out to be only slow, can no
longer renew or complete the claim, because its epoch is no longer the current one.

A finished task is recorded as completed together with its outcome, so a restarted run neither repeats
it nor loses what happened to it. All claims of a run share one key and change by compare-and-set.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from . import kvdoc
from .kv import KV

GRANTED, BUSY, COMPLETED = "granted", "busy", "completed"
_CLAIMED, _FREE, _COMPLETED = "claimed", "free", "completed"


@dataclass
class ClaimResult:
    status: str                          # granted | busy | completed
    epoch: int = 0
    holder: str = ""                     # for busy: the live owner
    outcome: Dict = field(default_factory=dict)   # for completed: what was recorded


class TaskClaims:
    def __init__(self, kv: KV, ttl: float = 300.0, clock: Callable[[], float] = time.time,
                 prefix: str = "", max_attempts: int = 200) -> None:
        self.kv, self.ttl, self.clock, self.max_attempts = kv, ttl, clock, max_attempts
        self.key = prefix + "claims"

    def _update(self, change):
        return kvdoc.update(self.kv, self.key, change, {}, self.max_attempts)

    def claim(self, task: str, owner: str, ttl: Optional[float] = None) -> ClaimResult:
        now = self.clock()
        expires = now + (self.ttl if ttl is None else ttl)

        def change(doc: dict) -> ClaimResult:
            e = doc.get(task)
            if e is None:
                doc[task] = {"state": _CLAIMED, "owner": owner, "epoch": 1, "expires": expires}
                return ClaimResult(GRANTED, 1)
            if e["state"] == _COMPLETED:
                return ClaimResult(COMPLETED, e["epoch"], e["owner"], e["outcome"])
            if e["state"] == _CLAIMED and e["expires"] > now and e["owner"] != owner:
                return ClaimResult(BUSY, e["epoch"], e["owner"])
            if e["state"] == _CLAIMED and e["owner"] == owner and e["expires"] > now:
                e["expires"] = expires                       # the same owner asking again keeps its claim
                return ClaimResult(GRANTED, e["epoch"])
            e.update(state=_CLAIMED, owner=owner, epoch=e["epoch"] + 1, expires=expires)
            return ClaimResult(GRANTED, e["epoch"])          # free, expired, or our own expired claim
        return self._update(change)

    def _mine(self, doc: dict, task: str, owner: str, epoch: int) -> Optional[dict]:
        e = doc.get(task)
        if e is not None and e["state"] == _CLAIMED and e["owner"] == owner and e["epoch"] == epoch:
            return e
        return None

    def renew(self, task: str, owner: str, epoch: int, ttl: Optional[float] = None) -> bool:
        """Extend our claim. Still possible after it ran out, as long as nobody took the task over."""
        expires = self.clock() + (self.ttl if ttl is None else ttl)

        def change(doc: dict) -> bool:
            e = self._mine(doc, task, owner, epoch)
            if e is None:
                return False
            e["expires"] = expires
            return True
        return self._update(change)

    def complete(self, task: str, owner: str, epoch: int, outcome: dict) -> bool:
        def change(doc: dict) -> bool:
            e = self._mine(doc, task, owner, epoch)
            if e is None:
                return False
            e.update(state=_COMPLETED, outcome=outcome)
            return True
        return self._update(change)

    def release(self, task: str, owner: str, epoch: int) -> bool:
        """Give the task back unfinished; the epoch is kept so that fencing stays monotonic."""
        def change(doc: dict) -> bool:
            e = self._mine(doc, task, owner, epoch)
            if e is None:
                return False
            e["state"] = _FREE
            return True
        return self._update(change)

    def snapshot(self) -> Dict[str, dict]:
        return kvdoc.load(self.kv, self.key, {})
