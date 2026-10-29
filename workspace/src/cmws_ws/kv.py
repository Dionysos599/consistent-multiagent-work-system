"""The narrow interface the workspace engine needs from the replicated store.

Everything above this module only ever calls four operations: get, put, put_if_absent and
compare-and-set. ``HttpKV`` speaks to the gateway of the Java store; ``MemoryKV`` is an in-process
stand-in with identical semantics, used by most tests so they run in milliseconds.
"""

from __future__ import annotations

import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional, Protocol, Tuple


class KVUnavailable(Exception):
    """The store gave no answer (no quorum, network down). The operation may or may not have run."""


class KV(Protocol):
    def get(self, key: str) -> Optional[str]: ...

    def put(self, key: str, value: str) -> None: ...

    def put_if_absent(self, key: str, value: str) -> Tuple[bool, Optional[str]]:
        """Store ``value`` unless the key exists. Returns (stored, value now held by the key)."""

    def cas(self, key: str, expected: Optional[str], value: str) -> Tuple[bool, Optional[str]]:
        """Set the key to ``value`` iff it currently holds ``expected`` (None: must not exist).

        Returns (success, value now held by the key).
        """


class MemoryKV:
    """Linearizable by construction: every operation runs under one lock."""

    def __init__(self) -> None:
        self._data: dict[str, str] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            return self._data.get(key)

    def put(self, key: str, value: str) -> None:
        with self._lock:
            self._data[key] = value

    def put_if_absent(self, key: str, value: str) -> Tuple[bool, Optional[str]]:
        with self._lock:
            if key in self._data:
                return False, self._data[key]
            self._data[key] = value
            return True, value

    def cas(self, key: str, expected: Optional[str], value: str) -> Tuple[bool, Optional[str]]:
        with self._lock:
            if self._data.get(key) == expected:
                self._data[key] = value
                return True, value
            return False, self._data.get(key)

    def keys(self) -> list[str]:
        with self._lock:
            return sorted(self._data)


class HttpKV:
    """Client for the HTTP gateway (``make run-gateway``).

    Transient failures (timeouts, 5xx, refused connections) are retried; when the retries run out
    ``KVUnavailable`` is raised. Retrying a conditional write can observe its own earlier effect,
    so callers that need exactly-once meaning must make their writes self-identifying: object
    keys are content hashes and ref values are unique commit ids, so both are.
    """

    def __init__(self, base_url: str, timeout: float = 30.0, retries: int = 3,
                 backoff: float = 0.2) -> None:
        self._base = base_url.rstrip("/")
        self._timeout = timeout
        self._retries = retries
        self._backoff = backoff

    def _url(self, key: str, op: str = "", query: str = "") -> str:
        url = f"{self._base}/kv/{urllib.parse.quote(key, safe='')}"
        if op:
            url += "/" + op
        if query:
            url += "?" + query
        return url

    def _call(self, method: str, url: str, body: Optional[str] = None) -> Tuple[int, str]:
        data = body.encode("utf-8") if body is not None else None
        last: Exception | None = None
        for attempt in range(self._retries + 1):
            req = urllib.request.Request(url, data=data, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    return resp.status, resp.read().decode("utf-8")
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return 404, ""
                if e.code < 500:
                    raise
                last = e
            except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
                last = e
            time.sleep(self._backoff * (attempt + 1))
        raise KVUnavailable(f"{method} {url}: {last}")

    def get(self, key: str) -> Optional[str]:
        status, body = self._call("GET", self._url(key))
        return None if status == 404 else body

    def put(self, key: str, value: str) -> None:
        self._call("PUT", self._url(key), value)

    def _conditional(self, url: str, value: str) -> Tuple[bool, Optional[str]]:
        import json

        _, body = self._call("POST", url, value)
        result = json.loads(body)
        return bool(result["success"]), result["value"]

    def put_if_absent(self, key: str, value: str) -> Tuple[bool, Optional[str]]:
        return self._conditional(self._url(key, "put-if-absent"), value)

    def cas(self, key: str, expected: Optional[str], value: str) -> Tuple[bool, Optional[str]]:
        query = "" if expected is None else "expected=" + urllib.parse.quote(expected, safe="")
        return self._conditional(self._url(key, "cas", query), value)
