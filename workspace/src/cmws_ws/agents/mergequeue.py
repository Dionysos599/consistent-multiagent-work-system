"""The merge queue: the only way work reaches main in an orchestrated run.

A finished branch is landed one at a time. Each landing merges the branch with the current main,
runs the validators on *the merged result* (not on the branch alone: two changes that each pass can
break together), and moves main only if they pass. Main therefore only ever holds states a validator
has approved, however many agents finish at once.

When a branch cannot land the queue escalates, cheapest first:

1. the automatic three-way merge (inside ``Repository.submit``);
2. a *merger* agent, which gets the branch merged with main, conflict markers in place, and the
   intent of both sides, and writes the resolution;
3. a *redo*: the task is run again from the new main, told what landed in the meantime;
4. a person: the branch and the reasons are recorded and nothing further happens automatically.

The queue's lock only serializes validate-and-land inside this process. Correctness never depends
on it: the final step is a compare-and-set of main, so several queues, or an agent landing directly,
cannot corrupt anything; they only cost each other a retry.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Protocol, Sequence, Set

from ..diff import changed_files, unified_all
from ..history import commits_between
from ..kv import KV
from ..objects import Commit
from ..repo import Repository, SubmitResult
from . import fence
from .worker import DONE, LoopWorker, Task, Worker, WorkerResult, run_task

MARKER = re.compile(r"^(<<<<<<< |>>>>>>> )", re.M)


@dataclass(frozen=True)
class Verdict:
    ok: bool
    output: str = ""


class Validator(Protocol):
    def check(self, files: Dict[str, bytes], changed: List[str]) -> Verdict: ...


class FnValidator:
    def __init__(self, fn: Callable[[Dict[str, bytes], List[str]], Verdict]) -> None:
        self.fn = fn

    def check(self, files, changed):
        return self.fn(files, changed)


class CommandValidator:
    """Writes the candidate tree to a folder and runs a command there (for example the test suite).
    Exit status 0 accepts."""

    def __init__(self, argv: Sequence[str], timeout: float = 600.0) -> None:
        self.argv, self.timeout = list(argv), timeout

    def check(self, files, changed):
        with tempfile.TemporaryDirectory(prefix="cmws-validate-") as tmp:
            for path, data in files.items():
                target = Path(tmp) / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
            try:
                proc = subprocess.run(self.argv, cwd=tmp, capture_output=True, text=True, timeout=self.timeout)
            except subprocess.TimeoutExpired:
                return Verdict(False, f"validation timed out after {self.timeout:.0f}s")
            except OSError as e:
                return Verdict(False, f"cannot run {self.argv[0]}: {e}")
            out = ((proc.stdout or "") + (proc.stderr or "")).strip()[-3000:]
            return Verdict(proc.returncode == 0, out)


@dataclass
class Step:
    stage: str          # auto | merger | redo | human
    status: str
    detail: str = ""


@dataclass
class Landing:
    status: str                              # landed | noop | escalated
    via: str = "auto"                        # which stage got it in (auto | merger | redo | human)
    commit: Optional[str] = None
    steps: List[Step] = field(default_factory=list)
    branch: str = ""                         # the branch that landed or was left for a person
    escalation: Optional[str] = None
    detail: str = ""

    @property
    def landed(self) -> bool:
        return self.status == "landed"


class Escalations:
    """Durable list of work left for a person (bounded, append by compare-and-set)."""

    KEY = "mq/escalations"
    MAX = 500

    def __init__(self, kv: KV, prefix: str = "", clock: Callable[[], float] = time.time) -> None:
        self.kv, self.key, self.clock = kv, prefix + self.KEY, clock

    def add(self, entry: dict) -> str:
        entry = dict(entry, at=self.clock())
        entry["id"] = f"esc-{int(entry['at'] * 1000)}-{entry['task']}"
        for _ in range(200):
            raw = self.kv.get(self.key)
            items = json.loads(raw) if raw else []
            new = json.dumps((items + [entry])[-self.MAX:], sort_keys=True)
            ok, _ = self.kv.cas(self.key, raw, new) if raw is not None else self.kv.put_if_absent(self.key, new)
            if ok:
                return entry["id"]
        raise RuntimeError("escalation list is too contended")

    def list(self) -> List[dict]:
        raw = self.kv.get(self.key)
        return json.loads(raw) if raw else []


MERGER_SYSTEM = """You resolve merge conflicts in a software project through file tools.
Two engineers changed the same files at the same time. One change is already on the main line; the other
is being merged in. In conflicted text files you will see blocks:
<<<<<<< ours  ...the incoming engineer's version...  ||||||| base ...original...  =======  ...main's version...  >>>>>>> main
Write each conflicted file so that BOTH changes take effect and the result is consistent code or text,
removing every marker line. For a file that one side deleted and the other edited, decide what the intent
of both implies and either keep the edited file or delete it. Do not touch files that have no conflict.
Call finish with one line describing how you combined the changes."""


def merger_worker(model, max_steps: int = 20) -> LoopWorker:
    return LoopWorker(model, name="merger", max_steps=max_steps, system=MERGER_SYSTEM)


def _has_new_markers(candidate: Dict[str, bytes], main: Dict[str, bytes]) -> List[str]:
    bad = []
    for path, data in candidate.items():
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if MARKER.search(text):
            before = main.get(path)
            try:
                if before is not None and MARKER.search(before.decode("utf-8")):
                    continue
            except UnicodeDecodeError:
                pass
            bad.append(path)
    return sorted(bad)


class MergeQueue:
    def __init__(self, repo: Repository, validator: Optional[Validator] = None, merger: Optional[Worker] = None,
                 redo_worker: Optional[Worker] = None, max_redos: int = 1,
                 escalations: Optional[Escalations] = None) -> None:
        self.repo, self.validator, self.merger = repo, validator, merger
        self.redo_worker, self.max_redos = redo_worker, max_redos
        self.escalations = escalations or Escalations(repo.kv)
        self._lock = threading.Lock()
        self.approved: List[str] = []                 # every candidate a validator accepted, for audit

    # ---- validating and landing one branch ----------------------------------------------

    def _validate(self, candidate: str) -> Optional[str]:
        lost = fence.check()
        if lost is not None:
            return lost                                  # the claim moved on: do not let this land
        store = self.repo.store
        main = self.repo.head()
        files = self.repo.files_at(candidate)
        before = self.repo.files_at(main)
        markers = _has_new_markers(files, before)
        if markers:
            return "unresolved conflict markers in " + ", ".join(markers)
        if self.validator is not None:
            changed = sorted(changed_files(store, store.get_commit(main).tree, store.get_commit(candidate).tree))
            verdict = self.validator.check(files, changed)
            if not verdict.ok:
                return verdict.output or "validation failed"
        self.approved.append(candidate)
        return None

    def _try(self, agent_id: str, message: Optional[str]) -> SubmitResult:
        with self._lock:
            ws = self.repo.resume(agent_id)
            r = self.repo.submit(ws, message, validate=self._validate)
        if r.status == "rejected" and r.detail == fence.LOST:
            raise fence.ClaimLost()
        return r

    # ---- the escalation chain -----------------------------------------------------------

    def land(self, task: Task, result: Optional[WorkerResult] = None, agent_id: Optional[str] = None) -> Landing:
        try:
            return self._land(task, result, agent_id)
        except fence.ClaimLost:                           # stop at once: no merger, no redo, no escalation
            return Landing("lost", branch=agent_id or task.id, detail=fence.LOST,
                           steps=[Step("fence", "lost", fence.LOST)])

    def _land(self, task: Task, result: Optional[WorkerResult], agent_id: Optional[str]) -> Landing:
        agent_id = agent_id or task.id
        summary = (result.summary.strip().splitlines()[0][:80] if result and result.summary.strip()
                   else task.description[:60])
        message = f"{task.id}: {summary}"
        landing = Landing("escalated", branch=agent_id)
        orig_base = self.repo.resume(agent_id).fork_base       # what the agent saw; the merger stage moves it
        r = self._try(agent_id, message)
        landing.steps.append(Step("auto", r.status, self._describe(r)))
        if r.landed or r.status == "noop":
            return self._done(landing, r, "auto", agent_id)
        if r.status == "contended":
            r = self._try(agent_id, message)                 # main was moving; one more go
            landing.steps.append(Step("auto", r.status, self._describe(r)))
            if r.landed or r.status == "noop":
                return self._done(landing, r, "auto", agent_id)

        if self.merger is not None and r.status in ("conflict", "rejected"):
            mr = self._merger_stage(task, agent_id, orig_base, r, landing, message)
            if mr is not None:
                if mr.landed or mr.status == "noop":
                    return self._done(landing, mr, "merger", agent_id)
                mr.conflicts = mr.conflicts or r.conflicts      # keep what the person needs to see
                r = mr

        if self.redo_worker is not None:
            for n in range(1, self.max_redos + 1):
                redo_id = self._redo_stage(task, n, orig_base, landing, r)
                if redo_id is None:
                    continue
                rr = self._try(redo_id, message)
                landing.steps.append(Step("redo", rr.status, self._describe(rr)))
                if rr.landed or rr.status == "noop":
                    return self._done(landing, rr, "redo", redo_id)
                r = rr
        return self._human(task, agent_id, landing, r)

    @staticmethod
    def _describe(r: SubmitResult) -> str:
        if r.status == "conflict":
            return "; ".join(f"{c.path} ({c.kind})" for c in r.conflicts)
        if r.status == "stale":
            return "; ".join(s.path for s in r.stale_reads)
        return r.detail

    @staticmethod
    def _done(landing: Landing, r: SubmitResult, via: str, branch: str) -> Landing:
        landing.status = "noop" if r.status == "noop" else "landed"
        landing.via, landing.commit, landing.branch = via, r.commit, branch
        return landing

    def _merger_stage(self, task: Task, agent_id: str, orig_base: str, r: SubmitResult,
                      landing: Landing, message: str) -> Optional[SubmitResult]:
        repo = self.repo
        ws = repo.resume(agent_id)
        main = repo.head()
        merged = repo.merge_into(ws.head, main, f"ws/{agent_id}", "main", keep_going=True)
        if merged.clean and r.status == "conflict":
            return self._try(agent_id, message)                # main moved and the conflict went away
        cid = repo.store.put_commit(Commit(merged.tree, (ws.head, main), agent_id,
                                           f"Merge main into ws/{agent_id} (conflicts marked)", repo.clock()))
        ws._adopt(cid, main)
        paths = tuple(sorted(c.path for c in merged.conflicts))
        upstream = self._upstream_summary(orig_base, main)
        problem = (f"Validation of the merged result failed:\n{r.detail}\n" if r.status == "rejected" else "")
        t = Task(f"{agent_id}-merge",
                 f"Resolve the merge of this change into main.\nThe change being merged: {task.description}\n"
                 f"What landed on main meanwhile:\n{upstream}\n{problem}"
                 f"Files with conflicts: {', '.join(paths) or '(none marked; fix the validation failure)'}.",
                 paths or tuple(), "")
        res = self.merger.run(t, repo, agent_id)
        landing.steps.append(Step("merger", res.status, res.summary or res.error))
        if not res.ok:
            return None
        rr = self._try(agent_id, message)
        landing.steps.append(Step("merger", f"land:{rr.status}", self._describe(rr)))
        return rr

    def _upstream_summary(self, since: str, main: str) -> str:
        store = self.repo.store
        lines = [f"- {store.get_commit(c).message}" for c in commits_between(store, since, main)][:20]
        return "\n".join(lines) or "(nothing recorded)"

    def _redo_stage(self, task: Task, n: int, orig_base: str, landing: Landing, last: SubmitResult) -> Optional[str]:
        repo = self.repo
        store = repo.store
        diff = unified_all(store, store.get_commit(orig_base).tree, store.get_commit(repo.head()).tree)
        if len(diff) > 8000:
            diff = diff[:8000] + "\n[... diff cut off]"
        redo_id = f"{task.id}-redo{n}"
        why = (f"Your earlier attempt could not be merged: {self._describe(last) or last.status}. "
               if last is not None else "")
        t = Task(redo_id, task.description, task.paths,
                 (task.context + "\n\n" if task.context else "") + why +
                 "Since then these changes landed on the main line; the project you see already includes them. "
                 "Do the task again on top of them:\n" + (diff or "(no textual changes)"),
                 task.depends_on)
        res = run_task(self.redo_worker, repo, t, redo_id)
        landing.steps.append(Step("redo", res.status, res.summary or res.error))
        return redo_id if res.ok else None

    def _human(self, task: Task, agent_id: str, landing: Landing, r: SubmitResult) -> Landing:
        conflicts = [[c.path, c.kind] for c in r.conflicts]
        eid = self.escalations.add({
            "task": task.id, "branch": f"ws/{agent_id}", "reason": r.status, "detail": self._describe(r),
            "conflicts": conflicts, "steps": [[s.stage, s.status, s.detail[:300]] for s in landing.steps]})
        landing.status, landing.via, landing.escalation = "escalated", "human", eid
        landing.detail = r.status
        landing.steps.append(Step("human", "escalated", eid))
        return landing

    # ---- scheduler integration ------------------------------------------------------------

    def as_land(self):
        """Adapter for ``Scheduler(land=...)``."""
        def land(repo, task, result):
            return self.land(task, result)
        return land
