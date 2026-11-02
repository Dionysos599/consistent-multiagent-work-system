"""Workers: things that carry out a task inside a workspace branch.

A worker is handed a task and the name of a branch that already exists (forked from main by
``run_task``). It does its work, commits it onto that branch, and reports. It never touches main:
landing the branch is the merge queue's job. Two kinds:

* ``LoopWorker`` drives a chat model through a small set of file tools, entirely in memory
  against the workspace overlay. This is the one that runs on a local Ollama model.
* ``CliWorker`` materializes the branch as a folder, runs an existing command-line agent in it
  and pushes whatever it left behind.
"""

from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Protocol, Sequence, Tuple

from ..repo import NoSuchWorkspace, Repository
from ..sync import Checkout
from .llm import ChatModel, ModelError
from .tools import Toolbox

DONE, GAVE_UP, STEP_LIMIT, ERROR = "done", "gave_up", "step_limit", "error"


@dataclass(frozen=True)
class Task:
    id: str
    description: str
    paths: Tuple[str, ...] = ()        # files or folders the task is expected to touch
    context: str = ""                  # extra material the planner chose to hand over
    depends_on: Tuple[str, ...] = ()   # tasks that must have landed before this one starts


@dataclass
class WorkerResult:
    status: str                        # done | gave_up | step_limit | error
    summary: str = ""
    commit: Optional[str] = None       # tip of the branch after the worker's commit (None: nothing changed)
    touched: Tuple[str, ...] = ()
    steps: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    error: str = ""
    transcript: List[dict] = field(default_factory=list, repr=False)

    @property
    def ok(self) -> bool:
        return self.status == DONE


class Worker(Protocol):
    name: str

    def run(self, task: Task, repo: Repository, agent_id: str) -> WorkerResult: ...


def run_task(worker: Worker, repo: Repository, task: Task, agent_id: Optional[str] = None) -> WorkerResult:
    """Give the task its own branch (forked from main now, or resumed after a restart) and run it."""
    agent_id = agent_id or task.id
    try:
        repo.resume(agent_id)
    except NoSuchWorkspace:
        repo.fork(agent_id)
    return worker.run(task, repo, agent_id)


def task_prompt(task: Task) -> str:
    parts = [f"Task: {task.description.strip()}"]
    if task.paths:
        parts.append("You are expected to change only: " + ", ".join(task.paths)
                     + ". Other agents are working on other parts of the project at the same time; "
                       "do not edit files outside this area.")
    if task.context.strip():
        parts.append("Background:\n" + task.context.strip())
    return "\n\n".join(parts)


SYSTEM = """You are a software engineer working on a project through file tools.
You can list, read, write, edit, move and delete files. You cannot run programs.
Work like this: look at the project with list_files, read what you need, make the change, then call finish
with a short summary. Make the smallest change that completes the task. Use replace_in_file for edits to
existing files and keep unrelated text exactly as it is. Other engineers edit other parts of the same
project at the same time, so stay inside the area you were given."""


class LoopWorker:
    def __init__(self, model: ChatModel, name: str = "loop", max_steps: int = 30, max_nudges: int = 2,
                 system: str = SYSTEM) -> None:
        self.model, self.name = model, name
        self.max_steps, self.max_nudges, self.system = max_steps, max_nudges, system

    def run(self, task: Task, repo: Repository, agent_id: str) -> WorkerResult:
        ws = repo.resume(agent_id)
        tools = Toolbox(ws)
        messages: List[dict] = [{"role": "system", "content": self.system},
                                {"role": "user", "content": task_prompt(task)}]
        result = WorkerResult(GAVE_UP)
        nudges = 0
        for step in range(1, self.max_steps + 1):
            try:
                reply = self.model.chat(messages, tools.specs())
            except ModelError as e:
                result.status, result.error = ERROR, str(e)
                break
            result.steps = step
            result.prompt_tokens += reply.usage.prompt_tokens
            result.completion_tokens += reply.usage.completion_tokens
            assistant: dict = {"role": "assistant", "content": reply.content}
            if reply.tool_calls:
                assistant["tool_calls"] = [
                    {"id": c.id, "type": "function",
                     "function": {"name": c.name, "arguments": _dump(c.arguments)}} for c in reply.tool_calls]
            messages.append(assistant)
            if not reply.tool_calls:
                nudges += 1
                if nudges > self.max_nudges:
                    result.status = GAVE_UP
                    result.summary = reply.content.strip()
                    break
                messages.append({"role": "user", "content":
                                 "Use the tools to make the change, and call finish when you are done."})
                continue
            nudges = 0
            finished = False
            for c in reply.tool_calls:
                out = tools.call(c.name, c.arguments)
                messages.append({"role": "tool", "tool_call_id": c.id, "content": out.text})
                if out.finished:
                    finished, result.summary = True, out.text
            if finished:
                result.status = DONE
                break
        else:
            result.status = STEP_LIMIT
        result.touched = tuple(sorted(tools.written))
        result.transcript = messages
        if result.status == DONE:
            result.commit = ws.commit(f"{task.id}: {_first_line(result.summary) or task.description[:60]}")
        return result


def _dump(arguments: dict) -> str:
    import json
    return json.dumps(arguments, ensure_ascii=False)


def _first_line(text: str) -> str:
    return text.strip().splitlines()[0][:100] if text.strip() else ""


class CliWorker:
    """Runs an existing command-line agent in a folder holding the branch's files.

    ``argv`` is the command; any element containing ``{prompt}`` gets the task text substituted
    (``["claude", "-p", "{prompt}"]``). With no placeholder the prompt is written to stdin. A
    non-zero exit leaves the branch untouched.
    """

    def __init__(self, argv: Sequence[str], name: str = "cli", timeout: float = 1800.0,
                 env: Optional[Dict[str, str]] = None, keep_dir: Optional[Path] = None) -> None:
        self.argv, self.name, self.timeout, self.env, self.keep_dir = list(argv), name, timeout, env, keep_dir

    def run(self, task: Task, repo: Repository, agent_id: str) -> WorkerResult:
        prompt = task_prompt(task)
        with tempfile.TemporaryDirectory(prefix="cmws-cli-") as tmp:
            root = Path(self.keep_dir) if self.keep_dir else Path(tmp) / "work"
            co = Checkout.clone(repo, agent_id, root)
            argv = [a.replace("{prompt}", prompt) for a in self.argv]
            stdin = None if any("{prompt}" in a for a in self.argv) else prompt
            try:
                proc = subprocess.run(argv, cwd=root, input=stdin, capture_output=True, text=True,
                                      timeout=self.timeout, env=self.env)
            except subprocess.TimeoutExpired:
                return WorkerResult(ERROR, error=f"timed out after {self.timeout:.0f}s")
            except OSError as e:
                return WorkerResult(ERROR, error=f"cannot run {argv[0]}: {e}")
            tail = (proc.stdout or "").strip()[-2000:]
            if proc.returncode != 0:
                return WorkerResult(ERROR, summary=tail, error=f"exit status {proc.returncode}: "
                                    + (proc.stderr or "").strip()[-500:])
            touched = tuple(sorted(co.status()))
            pushed = co.push(f"{task.id}: {_first_line(tail) or task.description[:60]}")
            if pushed.status in ("blocked", "conflict", "contended"):
                return WorkerResult(ERROR, summary=tail, touched=touched,
                                    error=f"push {pushed.status}: {sorted(pushed.conflicts)}")
            return WorkerResult(DONE, summary=tail, commit=pushed.commit, touched=touched)
