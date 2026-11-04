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
from .session import Session
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
    """A tool-calling loop over a chat model.

    With ``checkpoint_every`` (a number of tool calls) the run becomes durable: tool calls are numbered
    and executed once, the branch is committed and the conversation recorded every that many calls, and
    running the same task again after a crash picks up from the last record instead of starting over.
    """

    def __init__(self, model: ChatModel, name: str = "loop", max_steps: int = 30, max_nudges: int = 2,
                 system: str = SYSTEM, checkpoint_every: Optional[int] = None) -> None:
        self.model, self.name = model, name
        self.max_steps, self.max_nudges, self.system = max_steps, max_nudges, system
        self.checkpoint_every = checkpoint_every

    def run(self, task: Task, repo: Repository, agent_id: str) -> WorkerResult:
        session = Session.open(repo, agent_id) if self.checkpoint_every else None
        if session is not None and session.done is not None:
            d = dict(session.done)                            # finished before the crash: nothing to redo
            d["touched"] = tuple(d["touched"])
            return WorkerResult(**d)
        if session is not None:
            ws, tools = session.ws, session.tools
        else:
            ws = repo.resume(agent_id)
            tools = Toolbox(ws)
        messages: List[dict] = [{"role": "system", "content": self.system},
                                {"role": "user", "content": task_prompt(task)}]
        result = WorkerResult(GAVE_UP)
        nudges, first = 0, 1
        saved = session.state if session is not None else None
        if saved:
            messages, nudges, first = saved["messages"], saved["nudges"], saved["step"] + 1
            result.prompt_tokens, result.completion_tokens = saved["prompt_tokens"], saved["completion_tokens"]
            tools.written.update(saved["written"])
        result.steps = first - 1
        for step in range(first, self.max_steps + 1):
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
                out = session.call(session.next_seq(), c.name, c.arguments) if session else tools.call(c.name, c.arguments)
                messages.append({"role": "tool", "tool_call_id": c.id, "content": out.text})
                if out.finished:
                    finished, result.summary = True, out.text
            if finished:
                result.status = DONE
                break
            if session is not None and session.since_checkpoint >= self.checkpoint_every:
                session.checkpoint(self._state(messages, nudges, step, result, tools))
        else:
            result.status = STEP_LIMIT
        result.touched = tuple(sorted(tools.written))
        result.transcript = messages
        message = f"{task.id}: {_first_line(result.summary) or task.description[:60]}"
        if result.status == DONE:
            result.commit = ws.commit(message)
            if session is not None:
                result.commit = result.commit or (ws.head if ws.head != ws.fork_base else None)
                session.checkpoint(self._state(messages, nudges, result.steps, result, tools),
                                   done=_record(result), message=message)
        elif session is not None:
            session.checkpoint(self._state(messages, nudges, result.steps, result, tools))
        return result

    @staticmethod
    def _state(messages, nudges, step, result, tools) -> dict:
        return {"messages": messages, "nudges": nudges, "step": step, "written": sorted(tools.written),
                "prompt_tokens": result.prompt_tokens, "completion_tokens": result.completion_tokens}


def _record(r: WorkerResult) -> dict:
    return {"status": r.status, "summary": r.summary, "commit": r.commit, "touched": list(r.touched),
            "steps": r.steps, "prompt_tokens": r.prompt_tokens, "completion_tokens": r.completion_tokens,
            "error": r.error}


def _dump(arguments: dict) -> str:
    import json
    return json.dumps(arguments, ensure_ascii=False)


def _first_line(text: str) -> str:
    return text.strip().splitlines()[0][:100] if text.strip() else ""


class CliWorker:
    """Runs an existing command-line agent in a folder holding the branch's files.

    ``argv`` is the command; any element containing ``{prompt}`` gets the task text substituted
    (``["claude", "-p", "{prompt}"]``). With no placeholder the prompt is written to stdin. A
    non-zero exit leaves the branch as the last checkpoint left it.

    With ``checkpoint_every`` (seconds) whatever the command has written so far is pushed to the branch at
    that interval. If the machine dies, the next attempt starts from those files and is told what the
    branch already contains, instead of starting over.
    """

    def __init__(self, argv: Sequence[str], name: str = "cli", timeout: float = 1800.0,
                 env: Optional[Dict[str, str]] = None, keep_dir: Optional[Path] = None,
                 checkpoint_every: Optional[float] = None) -> None:
        self.argv, self.name, self.timeout, self.env, self.keep_dir = list(argv), name, timeout, env, keep_dir
        self.checkpoint_every = checkpoint_every

    def run(self, task: Task, repo: Repository, agent_id: str) -> WorkerResult:
        prior = _prior_work(repo, agent_id)
        prompt = task_prompt(task)
        if prior:
            prompt += ("\n\nAn earlier attempt at this task already changed these files on your branch: "
                       + ", ".join(prior) + ". They are in your working folder. Continue from there; "
                       "do not start over, and finish whatever is missing.")
        with tempfile.TemporaryDirectory(prefix="cmws-cli-") as tmp:
            root = Path(self.keep_dir) if self.keep_dir else Path(tmp) / "work"
            co = Checkout.clone(repo, agent_id, root)
            argv = [a.replace("{prompt}", prompt) for a in self.argv]
            stdin = None if any("{prompt}" in a for a in self.argv) else prompt
            try:
                proc = self._execute(argv, root, stdin, co, task)
            except subprocess.TimeoutExpired:
                return WorkerResult(ERROR, error=f"timed out after {self.timeout:.0f}s")
            except OSError as e:
                return WorkerResult(ERROR, error=f"cannot run {argv[0]}: {e}")
            tail = (proc.stdout or "").strip()[-2000:]
            if proc.returncode != 0:
                detail = (proc.stderr or "").strip()[-500:] or tail[-500:]     # some agents report errors on stdout
                return WorkerResult(ERROR, summary=tail, error=f"exit status {proc.returncode}: {detail}")
            pushed_before = {c.path for c in repo.resume(agent_id).changes_since_fork()}
            touched = tuple(sorted(set(co.status()) | pushed_before))
            pushed = co.push(f"{task.id}: {_first_line(tail) or task.description[:60]}")
            if pushed.status in ("blocked", "conflict", "contended"):
                return WorkerResult(ERROR, summary=tail, touched=touched,
                                    error=f"push {pushed.status}: {sorted(pushed.conflicts)}")
            ws = repo.resume(agent_id)
            commit = pushed.commit or (ws.head if ws.head != ws.fork_base else None)
            return WorkerResult(DONE, summary=tail, commit=commit,
                                touched=tuple(sorted({c.path for c in ws.changes_since_fork()})))

    def _execute(self, argv, root, stdin, co, task) -> "subprocess.CompletedProcess":
        if not self.checkpoint_every:
            return subprocess.run(argv, cwd=root, input=stdin, capture_output=True, text=True,
                                  timeout=self.timeout, env=self.env)
        proc = subprocess.Popen(argv, cwd=root, stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=self.env)
        waited, pending = 0.0, stdin
        while True:
            try:
                out, err = proc.communicate(input=pending, timeout=self.checkpoint_every)
                return subprocess.CompletedProcess(argv, proc.returncode, out, err)
            except subprocess.TimeoutExpired:
                pending = None
                waited += self.checkpoint_every
                if waited >= self.timeout:
                    proc.kill()
                    proc.communicate()
                    raise
                if co.status():
                    co.push(f"{task.id}: checkpoint")      # a refused push (conflict) is simply tried again later


def _prior_work(repo: Repository, agent_id: str) -> List[str]:
    """Files an earlier attempt left on the branch (empty if the branch is new or untouched)."""
    try:
        ws = repo.resume(agent_id)
    except NoSuchWorkspace:
        return []
    return sorted(c.path for c in ws.changes_since_fork())
