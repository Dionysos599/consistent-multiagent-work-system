import unittest

from cmws_ws.kv import MemoryKV
from cmws_ws.objects import MissingObject, PathConflict
from cmws_ws.repo import Repository, WorkspaceExists, NoSuchWorkspace
from cmws_ws.workspace import ConcurrentWorkspaceUpdate, DirtyWorkspace

BASE = {"README.md": "# demo\n", "src/app.py": "def main():\n    pass\n",
        "src/util.py": "x = 1\n", "docs/a.md": "a\n"}


def new_repo(files=None):
    return Repository.init(MemoryKV(), BASE if files is None else files)


class WorkspaceTest(unittest.TestCase):
    def test_fork_sees_main_and_is_isolated(self):
        repo = new_repo()
        a, b = repo.fork("a"), repo.fork("b")
        self.assertEqual(a.read_text("README.md"), "# demo\n")
        a.write("README.md", "changed by a\n")
        self.assertEqual(a.read_text("README.md"), "changed by a\n")   # read your writes
        self.assertEqual(b.read_text("README.md"), "# demo\n")         # not visible to b
        self.assertEqual(repo.main_files()["README.md"], b"# demo\n")  # nor on main
        a.commit("edit")
        self.assertEqual(b.read_text("README.md"), "# demo\n")         # not even after commit
        self.assertEqual(repo.main_files()["README.md"], b"# demo\n")

    def test_fork_creates_one_ref_and_no_objects(self):
        repo = new_repo()
        before = len(repo.kv.keys())
        repo.fork("a")
        self.assertEqual(len(repo.kv.keys()), before + 2)  # ref/ws/a + wsmeta/a
        with self.assertRaises(WorkspaceExists):
            repo.fork("a")

    def test_status_diff_commit(self):
        repo = new_repo()
        ws = repo.fork("a")
        ws.write("src/new.py", "n = 1\n")
        ws.write("src/util.py", "x = 2\n")
        ws.delete("docs/a.md")
        self.assertEqual(ws.status(), {"docs/a.md": "D", "src/new.py": "A", "src/util.py": "M"})
        self.assertIn("-x = 1", ws.diff())
        self.assertIn("+n = 1", ws.diff())
        cid = ws.commit("work")
        self.assertEqual(ws.status(), {})
        self.assertEqual(ws.diff(), "")
        self.assertEqual(repo.refs.get("ws/a"), cid)
        self.assertEqual(sorted(ws.files()), ["README.md", "src/app.py", "src/new.py", "src/util.py"])
        self.assertIsNone(ws.commit("nothing"))

    def test_looking_at_uncommitted_edits_writes_nothing_to_the_store(self):
        repo = new_repo()
        ws = repo.fork("a")
        ws.write("scratch.txt", "draft\n")
        ws.write("README.md", "draft\n")
        before = set(repo.kv.keys())
        ws.status(), ws.diff(), ws.dirty
        self.assertEqual(set(repo.kv.keys()), before)

    def test_writing_identical_content_is_not_a_change(self):
        ws = new_repo().fork("a")
        ws.write("README.md", "# demo\n")
        self.assertEqual(ws.status(), {})
        self.assertFalse(ws.dirty)

    def test_list_move_delete_directory(self):
        ws = new_repo().fork("a")
        self.assertEqual(ws.list(""), [("README.md", "file"), ("docs", "dir"), ("src", "dir")])
        ws.move("src", "lib")
        self.assertEqual(ws.list(""), [("README.md", "file"), ("docs", "dir"), ("lib", "dir")])
        self.assertEqual(ws.read_text("lib/util.py"), "x = 1\n")
        with self.assertRaises(FileNotFoundError):
            ws.read("src/util.py")
        ws.delete("lib")
        self.assertEqual(ws.list(""), [("README.md", "file"), ("docs", "dir")])
        ws.commit("m")
        self.assertEqual(sorted(ws.files()), ["README.md", "docs/a.md"])

    def test_move_is_atomic_in_one_commit(self):
        repo = new_repo()
        ws = repo.fork("a")
        ws.move("src/util.py", "src/helpers.py")
        cid = ws.commit("rename")
        files = repo.files_at(cid)
        self.assertIn("src/helpers.py", files)
        self.assertNotIn("src/util.py", files)
        with self.assertRaises(Exception):
            ws.move("src", "src/sub")

    def test_file_directory_errors(self):
        ws = new_repo().fork("a")
        with self.assertRaises(PathConflict):
            ws.write("README.md/x", "no")
        with self.assertRaises(IsADirectoryError):
            ws.write("src", "no")
        with self.assertRaises(IsADirectoryError):
            ws.read("src")
        with self.assertRaises(FileNotFoundError):
            ws.read("missing")
        with self.assertRaises(FileNotFoundError):
            ws.delete("missing")
        with self.assertRaises(NotADirectoryError):
            ws.list("README.md")
        ws.delete("README.md")                 # a deleted file can become a directory
        ws.write("README.md/inner", "ok")
        self.assertEqual(ws.list("README.md"), [("inner", "file")])

    def test_rollback_overlay_and_to_commit(self):
        repo = new_repo()
        ws = repo.fork("a")
        ws.write("a.txt", "1")
        c1 = ws.commit("c1")
        ws.write("a.txt", "2")
        c2 = ws.commit("c2")
        ws.write("a.txt", "3")
        ws.rollback()
        self.assertEqual(ws.read_text("a.txt"), "2")
        ws.rollback(to=c1)
        self.assertEqual(ws.head, c1)
        self.assertEqual(ws.read_text("a.txt"), "1")
        self.assertEqual(repo.refs.get("ws/a"), c1)
        with self.assertRaises(ValueError):
            ws.rollback(to=c2)       # c2 is no longer an ancestor of the head
        with self.assertRaises(MissingObject):
            ws.rollback(to=repo.head() + "x")

    def test_second_writer_on_the_same_branch_is_rejected(self):
        repo = new_repo()
        ws = repo.fork("a")
        twin = repo.resume("a")
        ws.write("x", "1")
        ws.commit("one")
        twin.write("y", "2")
        with self.assertRaises(ConcurrentWorkspaceUpdate):
            twin.commit("two")

    def test_resume_after_crash_keeps_commits_and_reads(self):
        repo = new_repo()
        ws = repo.fork("a")
        ws.read("src/util.py")
        ws.write("new", "1")
        ws.commit("checkpoint")
        ws.write("lost", "never committed")        # lost with the process
        back = Repository(repo.kv).resume("a")
        self.assertEqual(back.head, ws.head)
        self.assertEqual(back.fork_base, ws.fork_base)
        self.assertEqual(back.reads, ws.reads)
        self.assertEqual(back.read_text("new"), "1")
        self.assertFalse(back.exists("lost"))
        with self.assertRaises(NoSuchWorkspace):
            repo.resume("nobody")


class ReadSetTest(unittest.TestCase):
    def test_reads_of_untouched_files_are_recorded_own_writes_are_not(self):
        repo = new_repo()
        ws = repo.fork("a")
        ws.read("src/util.py")
        ws.exists("nope.txt")
        ws.write("src/app.py", "new\n")
        ws.read("src/app.py")                       # reads its own write
        ws.list("docs")
        self.assertEqual(set(ws.reads), {"src/util.py", "nope.txt"})
        self.assertIsNone(ws.reads["nope.txt"])
        self.assertIn("docs", ws.listings)

    def test_read_after_own_commit_is_not_an_upstream_dependency(self):
        ws = new_repo().fork("a")
        ws.write("src/util.py", "mine\n")
        ws.commit("c")
        ws.read("src/util.py")
        self.assertNotIn("src/util.py", ws.reads)
        ws.read("src/app.py")
        self.assertIn("src/app.py", ws.reads)


if __name__ == "__main__":
    unittest.main()
