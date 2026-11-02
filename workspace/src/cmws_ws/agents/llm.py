"""The model interface the agent loop needs, one real implementation and one for tests.

``OpenAICompatModel`` speaks the chat-completions protocol with tool calling, which Ollama,
llama.cpp, vLLM and the hosted APIs all serve; the default URL is a local Ollama. Nothing here
needs a paid service.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Protocol, Sequence, Union


class ModelError(Exception):
    """The model could not be reached or answered with something unusable."""


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    arguments: dict            # {"_raw": text} when the model produced arguments that are not JSON


@dataclass(frozen=True)
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0


@dataclass(frozen=True)
class Reply:
    content: str = ""
    tool_calls: Sequence[ToolCall] = ()
    usage: Usage = field(default_factory=Usage)


class ChatModel(Protocol):
    def chat(self, messages: List[dict], tools: List[dict]) -> Reply: ...


def _arguments(raw) -> dict:
    if isinstance(raw, dict):              # Ollama's native shape
        return raw
    if raw in (None, ""):
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return {"_raw": str(raw)}
    return parsed if isinstance(parsed, dict) else {"_raw": str(raw)}


def parse_reply(body: dict) -> Reply:
    """Turn a chat-completions response into a Reply; anything malformed is a ModelError."""
    try:
        message = body["choices"][0]["message"]
        calls = []
        for i, c in enumerate(message.get("tool_calls") or []):
            fn = c.get("function") or {}
            calls.append(ToolCall(str(c.get("id") or f"call_{i}"), str(fn.get("name") or ""),
                                  _arguments(fn.get("arguments"))))
        usage = body.get("usage") or {}
        content = message.get("content")
        return Reply("" if content is None else str(content), tuple(calls),
                     Usage(int(usage.get("prompt_tokens") or 0), int(usage.get("completion_tokens") or 0)))
    except (KeyError, IndexError, TypeError, AttributeError, ValueError) as e:
        raise ModelError(f"unexpected response ({type(e).__name__}): {str(body)[:200]}") from e


class OpenAICompatModel:
    def __init__(self, model: str, base_url: str = "http://127.0.0.1:11434/v1",
                 api_key: Optional[str] = None, timeout: float = 300.0, temperature: float = 0.2,
                 retries: int = 2, backoff: float = 1.0, sleep: Callable[[float], None] = time.sleep) -> None:
        self.model, self.base_url = model, base_url.rstrip("/")
        self.api_key, self.timeout, self.temperature = api_key, timeout, temperature
        self.retries, self.backoff, self._sleep = retries, backoff, sleep

    def chat(self, messages: List[dict], tools: List[dict]) -> Reply:
        payload = {"model": self.model, "messages": messages, "temperature": self.temperature,
                   "stream": False}
        if tools:
            payload["tools"] = tools
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        data = json.dumps(payload).encode()
        last: Exception = ModelError("no attempt made")
        for attempt in range(self.retries + 1):
            req = urllib.request.Request(f"{self.base_url}/chat/completions", data=data, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return parse_reply(json.loads(r.read()))
            except urllib.error.HTTPError as e:
                last = ModelError(f"HTTP {e.code}: {e.read()[:200]!r}")
                if e.code < 500 and e.code != 429:
                    break                                    # the request itself is wrong; retrying will not help
            except (urllib.error.URLError, OSError, ValueError) as e:
                last = ModelError(f"cannot reach {self.base_url}: {e}")
            if attempt < self.retries:
                self._sleep(self.backoff * (2 ** attempt))
        raise last


class ScriptedModel:
    """Plays back prepared replies (or computes them from the conversation); records what it saw."""

    def __init__(self, script: Sequence[Union[Reply, Callable[[List[dict]], Reply]]]) -> None:
        self._script = list(script)
        self.calls: List[List[dict]] = []

    def chat(self, messages: List[dict], tools: List[dict]) -> Reply:
        self.calls.append([dict(m) for m in messages])
        if not self._script:
            raise ModelError("script exhausted")
        step = self._script.pop(0)
        return step(messages) if callable(step) else step


def call(name: str, _id: Optional[str] = None, **arguments) -> ToolCall:
    """Shorthand for scripts: ``Reply(tool_calls=[call('write_file', path='a', content='b')])``."""
    return ToolCall(_id or f"call_{name}", name, arguments)
