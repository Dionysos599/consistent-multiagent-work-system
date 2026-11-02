"""Turning a goal into a task graph.

``StaticPlanner`` returns a fixed list (for tests and hand-written plans). ``LLMPlanner`` asks a chat
model for JSON, validates it with ``TaskGraph`` and, if the answer is unusable, sends the error back
and asks again, which small local models need more often than large ones.
"""

from __future__ import annotations

import json
import re
from typing import Dict, List, Optional, Protocol, Sequence

from .graph import InvalidGraph, TaskGraph
from .llm import ChatModel, ModelError
from .worker import Task


class PlanError(Exception):
    pass


class Planner(Protocol):
    def plan(self, goal: str, files: Sequence[str]) -> TaskGraph: ...


class StaticPlanner:
    def __init__(self, tasks: Sequence[Task]) -> None:
        self.tasks = list(tasks)

    def plan(self, goal: str, files: Sequence[str]) -> TaskGraph:
        return TaskGraph(self.tasks)


PROMPT = """You split a software goal into tasks for several engineers who work at the same time,
each in a private copy of the project.

Reply with JSON only: a list of objects with these fields
  "id":          short name using letters, digits, - and _
  "description": what to do, complete enough for someone who has not seen the other tasks
  "paths":       the files or folders this task will change (list of strings)
  "depends_on":  ids of tasks whose result this one needs (usually empty)

Rules: tasks whose paths do not overlap run in parallel, so keep paths narrow and give every task
its own files where you can. Tasks that must change the same file are run one after another.
Do not create a task for reading or reviewing. Use as few tasks as the goal needs.

Project files:
{files}

Goal: {goal}"""


def _json_block(text: str):
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, re.S)
    body = fenced.group(1) if fenced else text
    start = min([i for i in (body.find("["), body.find("{")) if i >= 0], default=-1)
    if start < 0:
        raise ValueError("no JSON found in the reply")
    value, _ = json.JSONDecoder().raw_decode(body[start:])
    return value


def parse_tasks(text: str) -> List[Task]:
    value = _json_block(text)
    if isinstance(value, dict):
        value = value.get("tasks", [value])
    if not isinstance(value, list) or not value:
        raise ValueError("expected a non-empty JSON list of tasks")
    tasks = []
    for i, item in enumerate(value):
        if not isinstance(item, dict) or "description" not in item:
            raise ValueError(f"task {i} must be an object with a description")
        paths = item.get("paths") or []
        deps = item.get("depends_on") or []
        if not all(isinstance(x, str) for x in list(paths) + list(deps)):
            raise ValueError(f"task {i}: paths and depends_on must be lists of strings")
        tasks.append(Task(str(item.get("id") or f"task-{i + 1}"), str(item["description"]),
                          tuple(paths), "", tuple(deps)))
    return tasks


class LLMPlanner:
    def __init__(self, model: ChatModel, max_attempts: int = 3, max_files: int = 200) -> None:
        self.model, self.max_attempts, self.max_files = model, max_attempts, max_files

    def plan(self, goal: str, files: Sequence[str]) -> TaskGraph:
        shown = list(files)[:self.max_files]
        listing = "\n".join(shown) + (f"\n... and {len(files) - len(shown)} more" if len(files) > len(shown) else "")
        messages: List[dict] = [{"role": "user", "content": PROMPT.format(files=listing or "(none yet)", goal=goal)}]
        error = "no attempt made"
        for _ in range(self.max_attempts):
            try:
                reply = self.model.chat(messages, [])
            except ModelError as e:
                raise PlanError(f"planner model failed: {e}") from e
            messages.append({"role": "assistant", "content": reply.content})
            try:
                return TaskGraph(parse_tasks(reply.content))
            except (ValueError, InvalidGraph) as e:
                error = str(e)
                messages.append({"role": "user", "content":
                                 f"That plan is not usable: {error}. Reply again with corrected JSON only."})
        raise PlanError(f"no usable plan after {self.max_attempts} attempts: {error}")
