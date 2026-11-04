"""A durable agent session: numbered tool calls that run at most once, and checkpoints to recover from.

An agent's edits live in memory (the workspace overlay) until they are committed, so a crash loses
everything since the last commit. A session makes that loss precise and harmless:

* every tool call carries a sequence number. Call ``n`` runs once; asking for it again (a retried
  request whose reply was lost) returns the recorded result instead of running it a second time, and a
  number that skips ahead is refused;
* ``checkpoint`` commits the overlay to the agent's branch and records, in one document in the store,
  the sequence reached, the commit and a caller-supplied state (the conversation so far);
* ``Session.open`` after a crash puts the branch back on the recorded commit (a commit made after the
  last record is dropped, since the record says what the agent knew when it made it) and carries on
  from the recorded sequence number. Calls made after the last checkpoint are lost with the overlay and
  are simply made again, with the same numbers.
"""

from __future__ import annotations

import json
import threading
from typing import Any, Dict, Optional

from ..kvdoc import load
from ..objects import MissingObject
from ..repo import Repository
from ..workspace import ConcurrentWorkspaceUpdate, Workspace
from .tools import ToolResult, Toolbox

KEEP_RESULTS = 20


def session_key(agent_id: str) -> str:
    return f"session/{agent_id}"


class Session:
    def __init__(self, repo: Repository, agent_id: str, ws: Workspace, doc: Optional[dict] = None) -> None:
        self.repo, self.agent_id, self.ws = repo, agent_id, ws
        self.tools = Toolbox(ws)
        doc = doc or {}
        self.seq: int = doc.get("seq", 0)
        self.state: Optional[dict] = doc.get("state")
        self.done: Optional[dict] = doc.get("done")
        self._results: Dict[int, ToolResult] = {
            int(k): ToolResult(*v) for k, v in doc.get("results", {}).items()}
        self._since_checkpoint = 0
        self._lock = threading.RLock()

    @classmethod
    def open(cls, repo: Repository, agent_id: str) -> "Session":
        ws = repo.resume(agent_id)
        doc = load(repo.kv, session_key(agent_id), None)
        if doc is not None and doc["head"] != ws.head:
            try:
                ws.rollback(to=doc["head"])
            except (ValueError, MissingObject, ConcurrentWorkspaceUpdate):
                doc = None            # the branch no longer goes through the record: start over on it
        return cls(repo, agent_id, ws, doc)

    @property
    def since_checkpoint(self) -> int:
        return self._since_checkpoint

    # ---- numbered calls ------------------------------------------------------------------

    def call(self, seq: int, name: str, arguments: dict) -> ToolResult:
        with self._lock:
            if seq <= self.seq:
                known = self._results.get(seq)
                if known is not None:
                    return known
                return ToolResult(f"error: call {seq} was already made and its result is no longer kept", False)
            if seq != self.seq + 1:
                return ToolResult(f"error: expected call {self.seq + 1}, got {seq}", False)
            out = self.tools.call(name, arguments)
            self._results[seq] = out
            self.seq = seq
            self._since_checkpoint += 1
            return out

    def next_seq(self) -> int:
        return self.seq + 1

    # ---- checkpoints ---------------------------------------------------------------------

    def checkpoint(self, state: Optional[dict] = None, done: Optional[dict] = None,
                   message: Optional[str] = None) -> Optional[str]:
        """Commit the overlay and record where the agent is. Returns the branch tip."""
        with self._lock:
            self.ws.commit(message or f"checkpoint {self.agent_id} at call {self.seq}")
            for old in [s for s in self._results if s <= self.seq - KEEP_RESULTS]:
                del self._results[old]
            self.state = state if state is not None else self.state
            self.done = done if done is not None else self.done
            doc = {"seq": self.seq, "head": self.ws.head, "state": self.state, "done": self.done,
                   "results": {str(s): [r.text, r.ok, r.finished] for s, r in sorted(self._results.items())}}
            self.repo.kv.put(session_key(self.agent_id), json.dumps(doc, sort_keys=True))
            self._since_checkpoint = 0
            return self.ws.head
