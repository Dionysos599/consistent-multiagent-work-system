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
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.repo, self.leases, self.worker, self.parallel = repo, leases, worker, parallel
        self.land, self.lease_ttl, self.poll, self.patience = land, lease_ttl, poll, patience
        self._sleep, self._clock = sleep, clock

    def run(self, graph: TaskGraph) -> RunReport:
        report = RunReport()
        finished: Set[str] = set()              # done, failed, rejected or skipped: no longer in the way
        landed_ok: Set[str] = set()
        started: Set[str] = set()
        running: Dict[Future, Task] = {}
        gauge = threading.Lock()
        live = [0]

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
                    return out
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
                self.leases.release(task.id)
                out.finished = self._clock()
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
                    got = self.leases.acquire(task.id, task.paths, self.lease_ttl)
                    if not got.granted:
                        continue                                  # someone outside this run holds the files
                    started.add(task.id)
                    running[pool.submit(work, task)] = task
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
