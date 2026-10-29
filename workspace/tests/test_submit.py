import unittest

from cmws_ws.history import is_ancestor, log
from cmws_ws.kv import MemoryKV
from cmws_ws.repo import Repository
from cmws_ws.workspace import DirtyWorkspace

BASE = {"a.txt": "1\n2\n3\n4\n5\n6\n7\n8\n", "b.txt": "b\n", "cfg/app.json": '{"v": 1}\n',
        "src/main.py": "print('hi')\n"}


def repo():
    return Repository.init(MemoryKV(), BASE)


def text(r, path):
    return r.main_files()[path].decode()


class SubmitTest(unittest.TestCase):
    def test_fast_forward_when_main_did_not_move(self):
        r = repo()
        ws = r.fork("a")
        ws.write("new.txt", "x\n")
        res = r.submit(ws, "add new")
        self.assertEqual(res.status, "fast-forward")
        self.assertEqual(r.head(), res.commit)
        self.assertEqual(text(r, "new.txt"), "x\n")
        self.assertEqual((ws.fork_base, ws.head), (res.commit, res.commit))
        self.assertEqual(r.submit(ws).status, "noop")

    def test_independent_files_merge(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.write("a_only.txt", "a\n")
        b.write("b_only.txt", "b\n")
        self.assertTrue(r.submit(a, "a").landed)
        res = r.submit(b, "b")
        self.assertEqual(res.status, "merged")
        files = r.main_files()
        self.assertIn("a_only.txt", files)
        self.assertIn("b_only.txt", files)
        # the merge commit has main's previous head as first parent: history stays one line
        _, merge = next(log(r.store, r.head()))
        self.assertEqual(len(merge.parents), 2)

    def test_same_file_disjoint_regions_merge_automatically(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.write("a.txt", BASE["a.txt"].replace("1\n", "ONE\n"))
        b.write("a.txt", BASE["a.txt"].replace("8\n", "EIGHT\n"))
        self.assertTrue(r.submit(a).landed)
        res = r.submit(b)
        self.assertEqual(res.status, "merged")
        self.assertEqual(res.auto_merged, ["a.txt"])
        self.assertEqual(text(r, "a.txt"), "ONE\n2\n3\n4\n5\n6\n7\nEIGHT\n")

    def test_overlapping_edit_is_a_conflict_and_main_is_untouched(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.write("a.txt", BASE["a.txt"].replace("4\n", "A\n"))
        b.write("a.txt", BASE["a.txt"].replace("4\n", "B\n"))
        self.assertTrue(r.submit(a).landed)
        before = r.head()
        res = r.submit(b)
        self.assertEqual(res.status, "conflict")
        self.assertEqual([(c.path, c.kind) for c in res.conflicts], [("a.txt", "content")])
        self.assertEqual(r.head(), before)
        self.assertEqual(text(r, "a.txt"), BASE["a.txt"].replace("4\n", "A\n"))

    def test_stale_read_blocks_even_without_overlapping_edits(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        # b reads the config, then writes a different file that depends on it
        self.assertEqual(b.read_text("cfg/app.json"), '{"v": 1}\n')
        b.write("src/derived.py", "VERSION = 1\n")
        a.write("cfg/app.json", '{"v": 2}\n')
        self.assertTrue(r.submit(a).landed)
        before = r.head()
        res = r.submit(b)
        self.assertEqual(res.status, "stale")
        self.assertEqual([(s.path, s.kind) for s in res.stale_reads], [("cfg/app.json", "file")])
        self.assertEqual(r.head(), before)
        # an orchestrator that decides the dependency is harmless can override
        res = r.submit(b, allow_stale=True)
        self.assertEqual(res.status, "merged")
        self.assertEqual([s.path for s in res.stale_reads], ["cfg/app.json"])
        self.assertIn("src/derived.py", r.main_files())

    def test_reading_something_nobody_changed_is_not_stale(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        b.read("cfg/app.json")
        b.write("out.txt", "o\n")
        a.write("b.txt", "changed\n")
        self.assertTrue(r.submit(a).landed)
        self.assertEqual(r.submit(b).status, "merged")

    def test_absent_file_that_appears_and_listings_are_stale(self):
        r = repo()
        a, b, c = r.fork("a"), r.fork("b"), r.fork("c")
        self.assertFalse(b.exists("lock.txt"))        # b relies on the lock not existing
        b.write("x", "1")
        c.list("src")                                  # c relies on what src contains
        c.write("y", "1")
        a.write("lock.txt", "taken\n")
        a.write("src/extra.py", "e\n")
        self.assertTrue(r.submit(a).landed)
        rb, rc = r.submit(b), r.submit(c)
        self.assertEqual((rb.status, [s.path for s in rb.stale_reads]), ("stale", ["lock.txt"]))
        self.assertEqual((rc.status, [(s.path, s.kind) for s in rc.stale_reads]),
                         ("stale", [("src", "listing")]))

    def test_sync_then_resubmit_after_stale(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        b.read("cfg/app.json")
        b.write("src/derived.py", "VERSION = 1\n")
        a.write("cfg/app.json", '{"v": 2}\n')
        r.submit(a)
        self.assertEqual(r.submit(b).status, "stale")
        res = b.sync()
        self.assertEqual(res.status, "merged")
        self.assertEqual([s.path for s in res.stale_reads], ["cfg/app.json"])  # told what changed
        self.assertEqual(b.read_text("cfg/app.json"), '{"v": 2}\n')
        b.write("src/derived.py", "VERSION = 2\n")      # the agent redoes its work on new facts
        b.read("cfg/app.json")
        self.assertEqual(r.submit(b).status, "fast-forward")  # it already contains main
        self.assertEqual(text(r, "src/derived.py"), "VERSION = 2\n")
        self.assertEqual(text(r, "cfg/app.json"), '{"v": 2}\n')

    def test_sync_fast_forward_up_to_date_conflict_and_dirty(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        self.assertEqual(b.sync().status, "up-to-date")
        a.write("n.txt", "1\n")
        r.submit(a)
        self.assertEqual(b.sync().status, "fast-forward")
        self.assertEqual(b.read_text("n.txt"), "1\n")
        self.assertEqual(b.sync().status, "up-to-date")
        a.write("a.txt", BASE["a.txt"].replace("4\n", "A\n"))
        c = r.fork("c")
        c.write("a.txt", BASE["a.txt"].replace("4\n", "C\n"))
        c.commit("c")
        r.submit(a)
        res = c.sync()
        self.assertEqual((res.status, [x.path for x in res.conflicts]), ("conflict", ["a.txt"]))
        self.assertEqual(c.read_text("a.txt"), BASE["a.txt"].replace("4\n", "C\n"))  # untouched
        c.write("zzz", "dirty")
        with self.assertRaises(DirtyWorkspace):
            c.sync()

    def test_submit_commits_pending_edits_and_noop_for_empty_workspace(self):
        r = repo()
        ws = r.fork("a")
        self.assertEqual(r.submit(ws).status, "noop")
        ws.write("k", "v")
        self.assertTrue(r.submit(ws, "msg").landed)
        self.assertEqual(text(r, "k"), "v")

    def test_workspace_that_synced_lands_by_fast_forward(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.write("x", "1")
        r.submit(a)
        b.write("y", "2")
        b.commit("y")
        self.assertEqual(b.sync().status, "merged")
        res = r.submit(b)
        self.assertEqual(res.status, "fast-forward")
        self.assertEqual(sorted(set(r.main_files()) - set(BASE)), ["x", "y"])

    def test_main_history_is_one_line_through_every_landing(self):
        r = repo()
        roots = []
        for i in range(6):
            ws = r.fork(f"w{i}")
            ws.write(f"f{i}", str(i))
            res = r.submit(ws)
            roots.append(res.commit)
        chain = [cid for cid, _ in log(r.store, r.head())]
        for cid in roots:
            self.assertIn(cid, chain)   # every landed commit is on the first-parent line


if __name__ == "__main__":
    unittest.main()
