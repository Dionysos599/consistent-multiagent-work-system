"""Running a task graph: as many tasks at once as the graph, the leases and the worker pool allow.

For each task that is ready the scheduler takes a path lease, forks a branch from the *current* main,
runs the worker, audits what the worker touched against its declared scope, then lands the branch
and releases the lease. Because a task starts only after everything it depends on or overlaps has
landed, it forks from a main that already contains that work. Landing here is the plain
``Repository.submit``; the validating merge queue replaces it in the next stage.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set

from ..claims import BUSY, COMPLETED, TaskClaims
from ..leases import Audit, LeaseTable
from ..repo import Repository, SubmitResult
from .graph import TaskGraph
from .worker import Task, Worker, WorkerResult, run_task

DONE, FAILED, REJECTED, SKIPPED = "done", "failed", "rejected", "skipped"

Land = Callable[[Repository, Task, WorkerResult], Optional[SubmitResult]]


def land_directly(repo: Repository, task: Task, result: WorkerResult) -> Optional[SubmitResult]:
    ws = repo.resume(task.id)
    return repo.submit(ws, f"{task.id}: {result.summary.strip().splitlines()[0][:80] if result.summary.strip() else task.description[:60]}")


@dataclass
class Outcome:
    task: Task
    status: str                                   # done | failed | rejected | skipped
    result: Optional[WorkerResult] = None
    landed: Optional[SubmitResult] = None
    audit: Optional[Audit] = None
    reason: str = ""
    started: float = 0.0
    finished: float = 0.0


@dataclass
class RunReport:
    outcomes: Dict[str, Outcome] = field(default_factory=dict)
    max_concurrency: int = 0

    @property
    def ok(self) -> bool:
        return all(o.status == DONE for o in self.outcomes.values())

    def by_status(self, status: str) -> List[str]:
        return [t for t, o in self.outcomes.items() if o.status == status]


class Scheduler:
    def __init__(self, repo: Repository, leases: LeaseTable, worker: Worker, parallel: int = 4,
                 land: Land = land_directly, lease_ttl: Optional[float] = None, poll: float = 0.05,
                 patience: float = 60.0, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic, claims: Optional[TaskClaims] = None,
                 owner: str = "scheduler", claim_ttl: Optional[float] = None,
                 heartbeat: Optional[float] = None) -> None:
        self.repo, self.leases, self.worker, self.parallel = repo, leases, worker, parallel
        self.land, self.lease_ttl, self.poll, self.patience = land, lease_ttl, poll, patience
        self._sleep, self._clock = sleep, clock
        self._last_beat = clock()
        self.claims, self.owner, self.claim_ttl = claims, owner, claim_ttl
        ttl = claim_ttl if claim_ttl is not None else (claims.ttl if claims is not None else 0)
        self.heartbeat = heartbeat if heartbeat is not None else (ttl / 3 if claims is not None else None)

    def run(self, graph: TaskGraph) -> RunReport:
        report = RunReport()
        finished: Set[str] = set()              # done, failed, rejected or skipped: no longer in the way
        landed_ok: Set[str] = set()
        started: Set[str] = set()
        running: Dict[Future, Task] = {}
        gauge = threading.Lock()
        live = [0]
        epochs: Dict[str, int] = {}             # task -> epoch of the claim we hold on it
        lost: Set[str] = set()                  # tasks whose claim another owner has taken over

        def work(task: Task) -> Outcome:
            out = Outcome(task, FAILED, started=self._clock())
            try:
                with gauge:
                    live[0] += 1
                    report.max_concurrency = max(report.max_concurrency, live[0])
                result = run_task(self.worker, self.repo, task, task.id)
                out.result = result
                out.audit = self.leases.audit(task.id, result.touched, task.paths)
                if not result.ok:
                    out.reason = result.error or result.status
                elif task.id in lost:
                    out.reason = "claim lost to another owner"
                else:
                    landed = self.land(self.repo, task, result)
                    out.landed = landed
                    if landed is None or landed.landed or landed.status == "noop":
                        out.status = DONE
                    else:
                        out.status, out.reason = REJECTED, landed.status
            except Exception as e:                               # a bug in a worker must not stall the run
                out.reason = f"{type(e).__name__}: {e}"
            finally:
                with gauge:
                    live[0] -= 1
            # Not in a finally clause on purpose: a process that dies here leaves its claim and lease
            # behind, to expire, exactly as a crash would.
            out.finished = self._clock()
            if self.claims is not None and task.id not in lost:
                self.claims.complete(task.id, self.owner, epochs[task.id], {
                    "status": out.status, "reason": out.reason,
                    "commit": out.landed.commit if out.landed is not None else None})
            if task.id not in lost:
                self.leases.release(task.id)
            return out

        with ThreadPoolExecutor(max_workers=self.parallel) as pool:
            idle_since: Optional[float] = None
            while len(finished) < len(graph.tasks):
                self._skip_dead(graph, finished, landed_ok, report)
                if len(finished) == len(graph.tasks):
                    break
                for task in graph.ready(finished, started):
                    if len(running) >= self.parallel:
                        break
                    if self.claims is not None:
                        claim = self.claims.claim(task.id, self.owner, self.claim_ttl)
                        if claim.status == COMPLETED:               # an earlier run already finished it
                            self._adopt(task, claim.outcome, report, finished, landed_ok)
                            continue
                        if claim.status == BUSY:
                            continue                                # another owner is running it
                        epochs[task.id] = claim.epoch
                    got = self.leases.acquire(task.id, task.paths, self.lease_ttl)
                    if not got.granted:
                        if self.claims is not None:
                            self.claims.release(task.id, self.owner, epochs[task.id])
                        continue                                  # someone outside this run holds the files
                    started.add(task.id)
                    running[pool.submit(work, task)] = task
                self._beat(running, epochs, lost)
                if running:
                    idle_since = None
                    done, _ = wait(list(running), timeout=self.poll, return_when=FIRST_COMPLETED)
                    for fut in done:
                        task = running.pop(fut)
                        out = fut.result()
                        report.outcomes[task.id] = out
                        finished.add(task.id)
                        if out.status == DONE:
                            landed_ok.add(task.id)
                else:
                    idle_since = idle_since if idle_since is not None else self._clock()
                    if self._clock() - idle_since > self.patience:
                        for t in graph.tasks:
                            if t.id not in finished:
                                report.outcomes[t.id] = Outcome(t, SKIPPED, reason="path lease held elsewhere")
                                finished.add(t.id)
                        break
                    self._sleep(self.poll)
        return report

    def _adopt(self, task: Task, outcome: dict, report: RunReport, finished: Set[str], landed_ok: Set[str]) -> None:
        status = outcome.get("status", FAILED)
        report.outcomes[task.id] = Outcome(task, status, reason=outcome.get("reason", "") or "completed earlier")
        finished.add(task.id)
        if status == DONE:
            landed_ok.add(task.id)

    def _beat(self, running: Dict[Future, Task], epochs: Dict[str, int], lost: Set[str]) -> None:
        """Keep the claims and leases of running tasks alive; note the claims that were taken over."""
        if self.claims is None or self.heartbeat is None:
            return
        now = self._clock()
        if now - self._last_beat < self.heartbeat:
            return
        self._last_beat = now
        for task in list(running.values()):
            if task.id in lost:
                continue
            if not self.claims.renew(task.id, self.owner, epochs[task.id], self.claim_ttl):
                lost.add(task.id)
                continue
            if not self.leases.renew(task.id, self.lease_ttl):
                self.leases.acquire(task.id, task.paths, self.lease_ttl)   # it ran out meanwhile

    @staticmethod
    def _skip_dead(graph: TaskGraph, finished: Set[str], landed_ok: Set[str], report: RunReport) -> None:
        """A task whose dependency failed can never run; mark it (and its dependents) skipped."""
        changed = True
        while changed:
            changed = False
            for t in graph.tasks:
                if t.id in finished:
                    continue
                bad = [d for d in t.depends_on if d in finished and d not in landed_ok]
                if bad:
                    report.outcomes[t.id] = Outcome(t, SKIPPED, reason=f"dependency {bad[0]} did not land")
                    finished.add(t.id)
                    changed = True
