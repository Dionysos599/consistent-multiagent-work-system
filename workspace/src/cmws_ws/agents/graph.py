"""The task graph: which tasks may run now.

Two things order tasks. Explicit ``depends_on`` edges say "needs the other's result". Overlapping
scopes say "would edit the same files": such tasks never run at the same time, and the one declared
first goes first, so a later task starts from a main that already contains the earlier one's work.
Tasks whose scopes do not overlap and that do not depend on each other run in parallel.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Set

from ..scope import Scope, make_scope, overlaps
from .worker import Task


ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


class InvalidGraph(ValueError):
    pass


class TaskGraph:
    def __init__(self, tasks: Iterable[Task]) -> None:
        self.tasks: List[Task] = list(tasks)
        self.by_id: Dict[str, Task] = {}
        for t in self.tasks:
            if not ID.match(t.id):
                raise InvalidGraph(f"bad task id {t.id!r}")
            if t.id in self.by_id:
                raise InvalidGraph(f"duplicate task id {t.id!r}")
            self.by_id[t.id] = t
        for t in self.tasks:
            for d in t.depends_on:
                if d not in self.by_id:
                    raise InvalidGraph(f"task {t.id!r} depends on unknown task {d!r}")
                if d == t.id:
                    raise InvalidGraph(f"task {t.id!r} depends on itself")
        self.scopes: Dict[str, Scope] = {t.id: make_scope(t.paths) for t in self.tasks}
        self._check_acyclic()
        self._index = {t.id: i for i, t in enumerate(self.tasks)}

    def _check_acyclic(self) -> None:
        state: Dict[str, int] = {}

        def visit(tid: str, trail: List[str]) -> None:
            if state.get(tid) == 2:
                return
            if state.get(tid) == 1:
                raise InvalidGraph("dependency cycle: " + " -> ".join(trail[trail.index(tid):] + [tid]))
            state[tid] = 1
            for d in self.by_id[tid].depends_on:
                visit(d, trail + [tid])
            state[tid] = 2
        for t in self.tasks:
            visit(t.id, [])

    def blockers(self, tid: str, finished: Set[str]) -> Set[str]:
        """Unfinished tasks that must go before ``tid``: dependencies, and earlier-declared overlaps."""
        t = self.by_id[tid]
        out = {d for d in t.depends_on if d not in finished}
        for other in self.tasks[:self._index[tid]]:
            if other.id not in finished and overlaps(self.scopes[tid], self.scopes[other.id]):
                out.add(other.id)
        return out

    def ready(self, finished: Set[str], started: Set[str]) -> List[Task]:
        """Tasks that can start now: not started or finished, and with nothing unfinished before them
        (an unfinished overlapping task declared earlier counts, so a running one always blocks)."""
        return [t for t in self.tasks
                if t.id not in started and t.id not in finished and not self.blockers(t.id, finished)]

    def waves(self) -> List[List[str]]:
        """The schedule if every task took one time unit: tasks in a wave could run together."""
        finished: Set[str] = set()
        waves = []
        while len(finished) < len(self.tasks):
            # ready() never returns two overlapping tasks: the later one is blocked by the earlier
            wave = [t.id for t in self.ready(finished, set(finished))]
            if not wave:
                raise InvalidGraph("no task can start")          # unreachable for a valid graph
            waves.append(wave)
            finished.update(wave)
        return waves

    def descendants(self, tid: str) -> Set[str]:
        """Tasks that (transitively) depend on ``tid`` through explicit edges."""
        out: Set[str] = set()
        frontier = [tid]
        while frontier:
            cur = frontier.pop()
            for t in self.tasks:
                if cur in t.depends_on and t.id not in out:
                    out.add(t.id)
                    frontier.append(t.id)
        return out
