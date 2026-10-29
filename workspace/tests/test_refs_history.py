import random
import threading
import unittest

from cmws_ws.history import ancestors, commits_between, is_ancestor, log, merge_base
from cmws_ws.kv import MemoryKV
from cmws_ws.objects import Commit, ObjectStore
from cmws_ws.refs import InvalidRefName, RefStore


class LostReplyKV(MemoryKV):
    """Applies every cas but sometimes reports failure, like a retried HTTP call would."""

    def __init__(self, rng):
        super().__init__()
        self.rng = rng

    def cas(self, key, expected, value):
        ok, cur = super().cas(key, expected, value)
        if ok and self.rng.random() < 0.5:
            # The reply was lost; the client retried and saw the already-applied state.
            return super().cas(key, expected, value)
        return ok, cur


class RefTest(unittest.TestCase):
    def test_create_and_cas(self):
        refs = RefStore(MemoryKV())
        self.assertIsNone(refs.get("main"))
        self.assertTrue(refs.create("main", "c1").ok)
        self.assertFalse(refs.create("main", "c2").ok)
        r = refs.cas("main", "wrong", "c2")
        self.assertFalse(r.ok)
        self.assertEqual(r.current, "c1")
        self.assertTrue(refs.cas("main", "c1", "c2").ok)
        self.assertEqual(refs.get("main"), "c2")

    def test_names(self):
        refs = RefStore(MemoryKV())
        for bad in ["", "/x", "a..b", "a/", "a b", "a?b"]:
            with self.assertRaises(InvalidRefName):
                refs.get(bad)
        refs.create("ws/agent-1.x", "c")

    def test_lost_reply_still_counts_as_success(self):
        refs = RefStore(LostReplyKV(random.Random(1)))
        cur = None
        for i in range(50):
            new = f"commit-{i}"
            self.assertTrue(refs.cas("main", cur, new).ok, i)
            cur = new
        self.assertEqual(refs.get("main"), cur)

    def test_exactly_one_winner_per_step(self):
        refs = RefStore(MemoryKV())
        refs.create("main", "c0")
        wins = []
        barrier = threading.Barrier(16)

        def worker(i):
            barrier.wait()
            if refs.cas("main", "c0", f"c-{i}").ok:
                wins.append(i)

        ts = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len(wins), 1)
        self.assertEqual(refs.get("main"), f"c-{wins[0]}")

    def test_cas_retry_loop_loses_no_increment(self):
        refs = RefStore(MemoryKV())
        refs.create("n", "0")
        per, nthreads = 200, 8

        def worker():
            for _ in range(per):
                while True:
                    cur = refs.get("n")
                    if refs.cas("n", cur, str(int(cur) + 1)).ok:
                        break

        ts = [threading.Thread(target=worker) for _ in range(nthreads)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(refs.get("n"), str(per * nthreads))


class HistoryTest(unittest.TestCase):
    def setUp(self):
        self.store = ObjectStore(MemoryKV())
        self.tree = self.store.empty_tree()
        self.n = 0

    def commit(self, *parents):
        self.n += 1
        return self.store.put_commit(Commit(self.tree, tuple(parents), "t", f"c{self.n}", self.n))

    def test_log_and_ancestors(self):
        a = self.commit()
        b = self.commit(a)
        c = self.commit(b)
        self.assertEqual([cid for cid, _ in log(self.store, c)], [c, b, a])
        self.assertEqual([cid for cid, _ in log(self.store, c, limit=2)], [c, b])
        self.assertEqual(ancestors(self.store, c), {a, b, c})
        self.assertTrue(is_ancestor(self.store, a, c))
        self.assertFalse(is_ancestor(self.store, c, a))

    def test_merge_base_fork_and_merge(self):
        root = self.commit()
        m1 = self.commit(root)
        w1 = self.commit(root)
        self.assertEqual(merge_base(self.store, m1, w1), root)
        merged = self.commit(m1, w1)
        m2 = self.commit(merged)
        w2 = self.commit(w1)
        # w1 is now the best common ancestor of m2 and w2
        self.assertEqual(merge_base(self.store, m2, w2), w1)
        self.assertEqual(merge_base(self.store, m1, m1), m1)

    def test_merge_base_of_ancestor_is_the_ancestor(self):
        a = self.commit()
        b = self.commit(a)
        self.assertEqual(merge_base(self.store, a, b), a)

    def test_unrelated_histories(self):
        self.assertIsNone(merge_base(self.store, self.commit(), self.commit()))

    def test_commits_between(self):
        a = self.commit()
        b = self.commit(a)
        c = self.commit(b)
        side = self.commit(a)
        m = self.commit(c, side)
        self.assertEqual(set(commits_between(self.store, b, m)), {c, side, m})
        self.assertEqual(commits_between(self.store, m, m), [])
        order = commits_between(self.store, a, m)
        self.assertLess(order.index(b), order.index(c))
        self.assertEqual(order[-1], m)


if __name__ == "__main__":
    unittest.main()
