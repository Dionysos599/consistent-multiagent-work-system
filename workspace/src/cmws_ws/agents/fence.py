"""A check that runs at the last moment before main moves, to stop an owner that has been replaced.

The scheduler installs it around the call that lands a branch (in the landing thread, via a context
variable, so the landing code needs no extra argument). ``Repository.submit`` and the merge queue call
``check()`` from their validate step, which runs after merging and just before the compare-and-set on main.
If the claim on the task now belongs to someone else the landing is refused with ``LOST``.

This narrows the window to the time between the check and the compare-and-set, it does not close it: the
store has no transaction across the claim and main. What is guaranteed regardless is that anything that
does land was merged and validated against the main it replaced.
"""

from __future__ import annotations

import contextvars
from contextlib import contextmanager
from typing import Callable, Optional

LOST = "claim lost to another owner"

_current: contextvars.ContextVar = contextvars.ContextVar("cmws_fence", default=None)


@contextmanager
def fenced(check: Callable[[], Optional[str]]):
    token = _current.set(check)
    try:
        yield
    finally:
        _current.reset(token)


def check() -> Optional[str]:
    f = _current.get()
    return f() if f is not None else None


class ClaimLost(Exception):
    pass
