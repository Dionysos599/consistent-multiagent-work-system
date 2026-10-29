"""Helpers shared by the property, concurrency and integration tests."""

import threading

from cmws_ws.history import ancestors
from cmws_ws.kv import MemoryKV


class RecordingKV(MemoryKV):
    """Records every successful CAS of one ref, in the order the store applied them."""

    def __init__(self, watched="ref/main"):
        super().__init__()
        self.watched = watched
        self.transitions = []
        self._rec = threading.Lock()

    def cas(self, key, expected, value):
        # record inside the store's critical section so the log is in linearization order
        with self._lock:
            if self._data.get(key) == expected:
                self._data[key] = value
                if key == self.watched:
                    self.transitions.append((expected, value))
                return True, value
            return False, self._data.get(key)


def check_linear_history(testcase, repo, transitions, root):
    """Every move of main starts where the previous one ended and builds on what it replaced."""
    first = transitions[0]
    testcase.assertEqual(first[0], None, "the first recorded move should be main's creation")
    testcase.assertEqual(first[1], root)
    prev = root
    for old, new in transitions[1:]:
        testcase.assertEqual(old, prev, "main moved from a value other than its predecessor's")
        testcase.assertIn(old, ancestors(repo.store, new), "a landing dropped earlier history")
        prev = new
    testcase.assertEqual(repo.head(), prev)
