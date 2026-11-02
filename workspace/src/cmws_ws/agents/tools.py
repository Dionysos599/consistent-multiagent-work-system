"""The tools an agent gets over its workspace.

Small on purpose: weak models handle five or six well-described tools far better than twenty.
Every failure comes back as text the model can read and react to ("error: ..."); nothing here
raises into the loop.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Set

from ..objects import PathConflict
from ..paths import InvalidPath
from ..workspace import Workspace

MAX_OUTPUT = 20_000


@dataclass(frozen=True)
class ToolResult:
    text: str
    ok: bool = True
    finished: bool = False


def _spec(name: str, description: str, properties: Dict[str, dict], required: List[str]) -> dict:
    return {"type": "function", "function": {
        "name": name, "description": description,
        "parameters": {"type": "object", "properties": properties, "required": required}}}


_S = {"type": "string"}

SPECS: List[dict] = [
    _spec("list_files", "List the files and folders directly inside a folder ('' is the project root).",
          {"directory": {**_S, "description": "folder path, '' for the root"}}, []),
    _spec("read_file", "Read a text file. Long files are cut off; the result says so.",
          {"path": _S}, ["path"]),
    _spec("write_file", "Create a file or replace it completely with the given text.",
          {"path": _S, "content": _S}, ["path", "content"]),
    _spec("replace_in_file",
          "Replace one exact piece of text in a file with another. 'old' must occur exactly once; "
          "include enough surrounding lines to make it unique. Prefer this over rewriting a file.",
          {"path": _S, "old": _S, "new": _S}, ["path", "old", "new"]),
    _spec("delete_file", "Delete a file or a whole folder.", {"path": _S}, ["path"]),
    _spec("move_file", "Rename or move a file or folder.", {"src": _S, "dst": _S}, ["src", "dst"]),
    _spec("finish", "Call this once the task is complete, with a short summary of what you changed.",
          {"summary": _S}, ["summary"]),
]


class Toolbox:
    def __init__(self, ws: Workspace, max_output: int = MAX_OUTPUT) -> None:
        self.ws = ws
        self.max_output = max_output
        self.written: Set[str] = set()        # every path this agent modified, for scope checks
        self._tools: Dict[str, Callable[..., ToolResult]] = {
            "list_files": self._list, "read_file": self._read, "write_file": self._write,
            "replace_in_file": self._replace, "delete_file": self._delete,
            "move_file": self._move, "finish": self._finish}

    def specs(self) -> List[dict]:
        return SPECS

    def call(self, name: str, arguments: dict) -> ToolResult:
        fn = self._tools.get(name) if isinstance(name, str) else None
        if fn is None:
            return ToolResult(f"error: unknown tool {name!r}; available: {', '.join(self._tools)}", False)
        if not isinstance(arguments, dict):
            return ToolResult(f"error: the arguments of {name} must be a JSON object", False)
        if "_raw" in arguments:
            return ToolResult(f"error: the arguments of {name} are not valid JSON: {str(arguments['_raw'])[:200]}", False)
        for key, value in arguments.items():
            if not isinstance(value, str):
                return ToolResult(f"error: argument {key!r} of {name} must be a string", False)
        try:
            return fn(**arguments)
        except TypeError as e:
            return ToolResult(f"error: bad arguments for {name}: {e}", False)
        except FileNotFoundError as e:
            return ToolResult(f"error: no such file or folder: {e}", False)
        except (InvalidPath, PathConflict, NotADirectoryError, IsADirectoryError, UnicodeDecodeError, ValueError) as e:
            return ToolResult(f"error: {type(e).__name__}: {e}", False)

    # ---- tools ---------------------------------------------------------------------------

    def _list(self, directory: str = "") -> ToolResult:
        entries = self.ws.list(directory)
        if not entries:
            return ToolResult("(empty)")
        return ToolResult("\n".join(f"{name}/" if kind == "dir" else name for name, kind in entries))

    def _read(self, path: str) -> ToolResult:
        text = self.ws.read_text(path)
        if len(text) > self.max_output:
            return ToolResult(text[:self.max_output] + f"\n[... cut off after {self.max_output} of {len(text)} characters]")
        return ToolResult(text if text else "(empty file)")

    def _write(self, path: str, content: str) -> ToolResult:
        self.ws.write(path, content)
        self.written.add(path)
        return ToolResult(f"wrote {len(content)} characters to {path}")

    def _replace(self, path: str, old: str, new: str) -> ToolResult:
        if old == "":
            return ToolResult("error: 'old' must not be empty", False)
        text = self.ws.read_text(path)
        n = text.count(old)
        if n == 0:
            return ToolResult(f"error: the text to replace was not found in {path}; read the file again", False)
        if n > 1:
            return ToolResult(f"error: the text occurs {n} times in {path}; include more context to make it unique", False)
        self.ws.write(path, text.replace(old, new, 1))
        self.written.add(path)
        return ToolResult(f"replaced 1 occurrence in {path}")

    def _delete(self, path: str) -> ToolResult:
        self.ws.delete(path)
        self.written.add(path)
        return ToolResult(f"deleted {path}")

    def _move(self, src: str, dst: str) -> ToolResult:
        self.ws.move(src, dst)
        self.written.update((src, dst))
        return ToolResult(f"moved {src} to {dst}")

    def _finish(self, summary: str = "") -> ToolResult:
        return ToolResult(summary, True, finished=True)
