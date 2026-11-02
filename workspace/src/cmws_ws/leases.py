"""Path leases: advisory, expiring claims on parts of the project, kept in the shared store.

A lease says "this agent is working in these paths until time T". It is not enforced on writes
(the store still accepts any edit; the merge machinery still decides what lands). Its job is to keep
the planner and scheduler from starting two tasks on the same files, and to flag agents whose edits
wandered outside what they declared. A crashed agent's lease simply expires.

All leases live in one key and change by compare-and-set, so claims by different processes are
serialized by the store: of two overlapping claims racing for the same files exactly one is granted.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional

from .kv import KV
from .scope import Scope, inside, make_scope, overlaps

LEASES = "leases"
LOG = "lease-log"
MAX_LOG = 200


@dataclass(frozen=True)
class Lease:
    agent: str
    paths: Scope
    expires: float


@dataclass
class Acquisition:
    granted: bool
    lease: Optional[Lease] = None
    blocked_by: Dict[str, Scope] = field(default_factory=dict)      # holder -> its paths


@dataclass
class Audit:
    in_scope: List[str]
    out_of_scope: List[str]
    collisions: Dict[str, str]          # out-of-scope path -> other agent whose live lease covers it

    @property
    def clean(self) -> bool:
        return not self.out_of_scope


class LeaseTable:
    def __init__(self, kv: KV, ttl: float = 600.0, clock: Callable[[], float] = time.time,
                 max_attempts: int = 200, prefix: str = "") -> None:
        self.kv, self.ttl, self.clock, self.max_attempts = kv, ttl, clock, max_attempts
        self._leases_key, self._log_key = prefix + LEASES, prefix + LOG

    # ---- storage -----------------------------------------------------------------------

    def _load(self) -> tuple:
        raw = self.kv.get(self._leases_key)
        doc = json.loads(raw) if raw else {}
        now = self.clock()
        live = {a: Lease(a, tuple(v["paths"]), v["expires"]) for a, v in doc.items() if v["expires"] > now}
        return raw, live

    @staticmethod
    def _dump(live: Dict[str, Lease]) -> str:
        return json.dumps({a: {"paths": list(l.paths), "expires": l.expires} for a, l in sorted(live.items())},
                          sort_keys=True)

    def _update(self, change: Callable[[Dict[str, Lease]], Optional[object]]):
        """Apply ``change`` to the live leases with CAS; ``change`` returns the result, or raises Skip."""
        for _ in range(self.max_attempts):
            raw, live = self._load()
            before = dict(live)
            result = change(live)
            if live == before and raw is not None:
                return result                 # nothing to write (denied or already so)
            ok, _ = self.kv.cas(self._leases_key, raw, self._dump(live)) if raw is not None else \
                self.kv.put_if_absent(self._leases_key, self._dump(live))
            if ok:
                return result
        raise RuntimeError("lease table is too contended")

    # ---- operations --------------------------------------------------------------------

    def acquire(self, agent: str, paths: Iterable[str], ttl: Optional[float] = None) -> Acquisition:
        scope = make_scope(paths)
        expires = self.clock() + (ttl if ttl is not None else self.ttl)

        def change(live: Dict[str, Lease]) -> Acquisition:
            blockers = {a: l.paths for a, l in live.items() if a != agent and overlaps(scope, l.paths)}
            if blockers:
                return Acquisition(False, None, blockers)
            lease = Lease(agent, scope, expires)
            live[agent] = lease              # also refreshes an earlier claim by the same agent
            return Acquisition(True, lease)
        return self._update(change)

    def renew(self, agent: str, ttl: Optional[float] = None) -> bool:
        """Extend a lease that is still live; an expired one is gone and must be acquired again."""
        expires = self.clock() + (ttl if ttl is not None else self.ttl)

        def change(live: Dict[str, Lease]) -> bool:
            if agent not in live:
                return False
            live[agent] = Lease(agent, live[agent].paths, expires)
            return True
        return self._update(change)

    def release(self, agent: str) -> None:
        def change(live: Dict[str, Lease]) -> None:
            live.pop(agent, None)
        self._update(change)

    def holders(self) -> Dict[str, Lease]:
        return self._load()[1]

    # ---- auditing ----------------------------------------------------------------------

    def audit(self, agent: str, touched: Iterable[str], scope: Iterable[str], record: bool = True) -> Audit:
        """Compare what an agent changed with what it declared, and note the difference.

        An out-of-scope path inside another live lease is a collision: two agents are working on
        the same file, which the merge queue should treat as a likely conflict.
        """
        declared = make_scope(scope)
        holders = self.holders()
        ins, outs, collisions = [], [], {}
        for p in sorted(set(touched)):
            if inside(declared, p):
                ins.append(p)
                continue
            outs.append(p)
            for other, lease in holders.items():
                if other != agent and inside(lease.paths, p):
                    collisions[p] = other
        report = Audit(ins, outs, collisions)
        if outs and record:
            self._log({"agent": agent, "at": self.clock(), "out_of_scope": outs, "collisions": collisions})
        return report

    def _log(self, entry: dict) -> None:
        for _ in range(self.max_attempts):
            raw = self.kv.get(self._log_key)
            entries = json.loads(raw) if raw else []
            entries = (entries + [entry])[-MAX_LOG:]
            new = json.dumps(entries, sort_keys=True)
            ok, _ = self.kv.cas(self._log_key, raw, new) if raw is not None else self.kv.put_if_absent(self._log_key, new)
            if ok:
                return
        raise RuntimeError("lease log is too contended")

    def violations(self) -> List[dict]:
        raw = self.kv.get(self._log_key)
        return json.loads(raw) if raw else []
