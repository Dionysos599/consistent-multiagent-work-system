"""The whole pipeline in one call: plan, schedule, run, validate, land, and survive a restart.

``Orchestrator.run(run_id, goal=...)`` asks the planner for a task graph (once: the plan is stored under
the run id), runs it with claims so that each task has one live owner, and lands every finished branch
through the merge queue. Running the same call again after a crash does not plan again: tasks already
completed are taken from the record, tasks whose owner died are taken over once their claim expires,
and a durable worker resumes from its last checkpoint instead of starting over.

Several orchestrators may work on one run at the same time (give each its own ``owner``); the claims
keep them off each other's tasks.
"""

from __future__ import annotations

import json
import socket
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from .. import kvdoc
from ..claims import TaskClaims
from ..kv import KV
from ..leases import LeaseTable
from ..repo import Repository
from .graph import TaskGraph
from .mergequeue import MergeQueue
from .planner import Planner
from .scheduler import DONE, RunReport, Scheduler
from .worker import Task, Worker


def task_to_dict(t: Task) -> dict:
    return {"id": t.id, "description": t.description, "paths": list(t.paths), "context": t.context,
            "depends_on": list(t.depends_on)}


def task_from_dict(d: dict) -> Task:
    return Task(d["id"], d["description"], tuple(d.get("paths", ())), d.get("context", ""),
                tuple(d.get("depends_on", ())))


def plan_key(run_id: str) -> str:
    return f"runs/{run_id}/plan"


def result_key(run_id: str) -> str:
    return f"runs/{run_id}/result"


@dataclass
class RunSummary:
    run_id: str
    report: RunReport
    resumed: bool                              # the plan was found in the store, not made now
    main: str = ""
    landed: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.report.ok


class Orchestrator:
    def __init__(self, repo: Repository, worker: Worker, planner: Optional[Planner] = None,
                 queue: Optional[MergeQueue] = None, owner: Optional[str] = None, parallel: int = 4,
                 claim_ttl: float = 300.0, lease_ttl: float = 900.0, poll: float = 0.05, patience: float = 60.0,
                 clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.repo, self.worker, self.planner = repo, worker, planner
        self.queue = queue or MergeQueue(repo)
        self.owner = owner or f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        self.parallel, self.claim_ttl, self.lease_ttl = parallel, claim_ttl, lease_ttl
        self.poll, self.patience, self.clock, self._sleep = poll, patience, clock, sleep

    @property
    def kv(self) -> KV:
        return self.repo.kv

    def plan(self, run_id: str, goal: Optional[str], graph: Optional[TaskGraph]) -> tuple:
        """Return (graph, resumed). The first plan stored under the run id wins, whoever made it."""
        raw = self.kv.get(plan_key(run_id))
        if raw is None:
            if graph is None:
                if self.planner is None or goal is None:
                    raise ValueError(f"run {run_id!r} has no stored plan; give a goal and a planner, or a graph")
                graph = self.planner.plan(goal, sorted(self.repo.main_files()))
            doc = json.dumps({"goal": goal, "created": self.clock(),
                              "tasks": [task_to_dict(t) for t in graph.tasks]}, sort_keys=True)
            stored, current = self.kv.put_if_absent(plan_key(run_id), doc)
            if stored:
                return graph, False
            raw = current                      # another orchestrator planned first: use its plan
        return TaskGraph([task_from_dict(d) for d in json.loads(raw)["tasks"]]), True

    def claims(self, run_id: str) -> TaskClaims:
        return TaskClaims(self.kv, ttl=self.claim_ttl, clock=self.clock, prefix=f"runs/{run_id}/")

    def run(self, run_id: str, goal: Optional[str] = None, graph: Optional[TaskGraph] = None) -> RunSummary:
        graph, resumed = self.plan(run_id, goal, graph)
        scheduler = Scheduler(self.repo, LeaseTable(self.kv, ttl=self.lease_ttl, clock=self.clock), self.worker,
                              parallel=self.parallel, land=self.queue.as_land(), lease_ttl=self.lease_ttl,
                              poll=self.poll, patience=self.patience, sleep=self._sleep,
                              claims=self.claims(run_id), owner=self.owner, claim_ttl=self.claim_ttl)
        report = scheduler.run(graph)
        summary = RunSummary(run_id, report, resumed, self.repo.head(), report.by_status(DONE),
                             [t for t, o in report.outcomes.items() if o.status in ("failed", "rejected")],
                             report.by_status("skipped"))
        kvdoc.update(self.kv, result_key(run_id), lambda d: d.update(
            owner=self.owner, finished=self.clock(), main=summary.main, landed=summary.landed,
            failed=summary.failed, skipped=summary.skipped), {})
        return summary


def run_status(kv: KV, run_id: str) -> Dict[str, object]:
    """What the store knows about a run: the plan, each task's claim state and the final result if any."""
    plan = kvdoc.load(kv, plan_key(run_id), None)
    return {"plan": plan, "claims": kvdoc.load(kv, f"runs/{run_id}/claims", {}),
            "result": kvdoc.load(kv, result_key(run_id), None)}
