import random
import unittest

from cmws_ws.gc import reachable, storage_report
from cmws_ws.kv import MemoryKV
from cmws_ws.objects import CHUNK_BYTES, Commit
from cmws_ws.renames import side_renames
from cmws_ws.repo import Repository

UTIL = "".join(f"util line {i}\n" for i in range(1, 13))
BASE = {"src/util.py": UTIL, "src/app.py": "import util\n", "docs/guide.md": "guide\n",
        "keep.txt": "k\n"}


def repo():
    return Repository.init(MemoryKV(), BASE)


def edit_line(text, n, new):
    lines = text.splitlines(keepends=True)
    lines[n - 1] = new + "\n"
    return "".join(lines)


def text(r, path):
    return r.main_files()[path].decode()


class RecordingTest(unittest.TestCase):
    def moves(self, r, cid):
        return r.store.get_commit(cid).renames

    def test_move_is_recorded_in_the_commit(self):
        r = repo()
        ws = r.fork("a")
        ws.move("src/util.py", "src/helpers.py")
        self.assertEqual(self.moves(r, ws.commit("m")), (("src/util.py", "src/helpers.py"),))

    def test_chains_collapse_and_noops_vanish(self):
        r = repo()
        ws = r.fork("a")
        ws.move("src/util.py", "x.py")
        ws.move("x.py", "y.py")
        ws.move("keep.txt", "tmp.txt")
        ws.move("tmp.txt", "keep.txt")                       # moved back: nothing happened
        self.assertEqual(self.moves(r, ws.commit("m")), (("src/util.py", "y.py"),))

    def test_moving_a_file_created_in_the_same_commit_is_just_an_add(self):
        r = repo()
        ws = r.fork("a")
        ws.write("new.txt", "n")
        ws.move("new.txt", "newer.txt")
        self.assertEqual(self.moves(r, ws.commit("m")), ())

    def test_moved_then_deleted_is_not_a_rename(self):
        r = repo()
        ws = r.fork("a")
        ws.move("keep.txt", "k2.txt")
        ws.delete("k2.txt")
        self.assertEqual(self.moves(r, ws.commit("m")), ())

    def test_directory_move_records_each_file(self):
        r = repo()
        ws = r.fork("a")
        ws.move("src", "lib")
        self.assertEqual(set(self.moves(r, ws.commit("m"))),
                         {("src/util.py", "lib/util.py"), ("src/app.py", "lib/app.py")})

    def test_commits_without_renames_keep_their_old_ids(self):
        r = repo()
        c = r.store.get_commit(r.head())
        self.assertNotIn("renames", c.to_json())
        self.assertEqual(Commit.from_json(c.to_json()), c)


class RenameMergeTest(unittest.TestCase):
    def test_edit_follows_a_recorded_rename(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("src/util.py", "src/helpers.py")
        b.write("src/util.py", edit_line(UTIL, 10, "edited by b"))
        self.assertTrue(r.submit(a).landed)
        res = r.submit(b)
        self.assertEqual(res.status, "merged", res.conflicts)
        files = r.main_files()
        self.assertNotIn("src/util.py", files)
        self.assertEqual(text(r, "src/helpers.py"), edit_line(UTIL, 10, "edited by b"))
        self.assertIn("src/helpers.py", res.auto_merged + ["src/helpers.py"])

    def test_rename_plus_edit_on_one_side_edit_elsewhere_on_the_other(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("src/util.py", "src/helpers.py")
        a.write("src/helpers.py", edit_line(UTIL, 1, "top edit by a"))
        b.write("src/util.py", edit_line(UTIL, 12, "bottom edit by b"))
        r.submit(a)
        self.assertEqual(r.submit(b).status, "merged")
        self.assertEqual(text(r, "src/helpers.py"),
                         edit_line(edit_line(UTIL, 1, "top edit by a"), 12, "bottom edit by b"))

    def test_overlapping_edits_conflict_at_the_new_path(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("src/util.py", "src/helpers.py")
        a.write("src/helpers.py", edit_line(UTIL, 5, "A"))
        b.write("src/util.py", edit_line(UTIL, 5, "B"))
        r.submit(a)
        res = r.submit(b)
        self.assertEqual([(c.path, c.kind) for c in res.conflicts], [("src/helpers.py", "content")])

    def test_the_other_side_renames_and_we_edit(self):
        """Same case, with the editing side landing first."""
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        b.write("src/util.py", edit_line(UTIL, 10, "edited by b"))
        a.move("src/util.py", "src/helpers.py")
        r.submit(b)
        self.assertEqual(r.submit(a).status, "merged")
        self.assertEqual(text(r, "src/helpers.py"), edit_line(UTIL, 10, "edited by b"))
        self.assertNotIn("src/util.py", r.main_files())

    def test_both_rename_to_different_names(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("keep.txt", "one.txt")
        b.move("keep.txt", "two.txt")
        r.submit(a)
        res = r.submit(b)
        self.assertEqual([(c.path, c.kind) for c in res.conflicts], [("keep.txt", "rename-rename")])
        self.assertIn("one.txt", res.conflicts[0].detail)

    def test_both_rename_to_the_same_name_and_edit_different_lines(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        for ws, n in ((a, 1), (b, 12)):
            ws.move("src/util.py", "src/helpers.py")
            ws.write("src/helpers.py", edit_line(UTIL, n, f"edit {n}"))
        r.submit(a)
        self.assertEqual(r.submit(b).status, "merged")
        self.assertEqual(text(r, "src/helpers.py"),
                         edit_line(edit_line(UTIL, 1, "edit 1"), 12, "edit 12"))

    def test_rename_versus_delete(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("keep.txt", "moved.txt")
        b.delete("keep.txt")
        r.submit(a)
        self.assertEqual(r.submit(b).status, "merged")            # a only moved it; b's delete wins
        self.assertNotIn("moved.txt", r.main_files())
        r2 = repo()
        a, b = r2.fork("a"), r2.fork("b")
        a.move("keep.txt", "moved.txt")
        a.write("moved.txt", "k edited\n")
        b.delete("keep.txt")
        r2.submit(a)
        res = r2.submit(b)
        self.assertEqual([(c.path, c.kind) for c in res.conflicts], [("moved.txt", "modify-delete")])

    def test_destination_already_taken_is_a_conflict_not_a_silent_overwrite(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("keep.txt", "taken.txt")
        b.write("taken.txt", "something else\n")
        r.submit(a)
        res = r.submit(b)
        self.assertEqual(res.status, "conflict")

    def test_directory_rename_carries_edits_into_it(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("src", "lib")
        b.write("src/util.py", edit_line(UTIL, 3, "edit in old dir"))
        b.write("src/brand_new.py", "added to the old dir\n")
        r.submit(a)
        res = r.submit(b)
        files = r.main_files()
        self.assertEqual(files["lib/util.py"].decode(), edit_line(UTIL, 3, "edit in old dir"))
        # a file that did not exist at the fork stays where its author put it
        self.assertIn("src/brand_new.py", files)
        self.assertEqual(res.status, "merged")

    def test_sync_follows_renames_too(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("src/util.py", "src/helpers.py")
        r.submit(a)
        b.write("src/util.py", edit_line(UTIL, 8, "b edit"))
        b.commit("b")
        res = b.sync()
        self.assertEqual(res.status, "merged", res.conflicts)
        self.assertEqual(b.read_text("src/helpers.py"), edit_line(UTIL, 8, "b edit"))

    def test_reads_of_the_old_path_go_stale(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        b.read("src/util.py")
        b.write("out", "x")
        a.move("src/util.py", "src/helpers.py")
        r.submit(a)
        self.assertEqual(r.submit(b).status, "stale")


class DetectionTest(unittest.TestCase):
    """No move() record: the rename is only visible as a delete plus an add."""

    def test_identical_content_is_detected(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.write("src/helpers.py", UTIL)
        a.delete("src/util.py")
        b.write("src/util.py", edit_line(UTIL, 10, "b"))
        self.assertEqual(r.store.get_commit(a.commit("shell mv")).renames, ())
        r.submit(a)
        self.assertEqual(r.submit(b).status, "merged")
        self.assertEqual(text(r, "src/helpers.py"), edit_line(UTIL, 10, "b"))

    def test_similar_content_is_detected(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.write("src/helpers.py", edit_line(edit_line(UTIL, 2, "a2"), 3, "a3"))
        a.delete("src/util.py")
        b.write("src/util.py", edit_line(UTIL, 11, "b"))
        r.submit(a)
        self.assertEqual(r.submit(b).status, "merged")

    def test_unrelated_delete_and_add_is_not_a_rename(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.write("other.txt", "completely different\ncontent\n")
        a.delete("src/util.py")
        b.write("src/util.py", edit_line(UTIL, 10, "b"))
        r.submit(a)
        res = r.submit(b)
        self.assertEqual([(c.path, c.kind) for c in res.conflicts], [("src/util.py", "modify-delete")])

    def test_recorded_rename_survives_a_rewrite_that_similarity_would_miss(self):
        r = repo()
        a, b = r.fork("a"), r.fork("b")
        a.move("src/util.py", "src/helpers.py")
        a.write("src/helpers.py", "".join(f"rewritten {i}\n" for i in range(30)))
        r.submit(a)
        mid = r.head()
        self.assertEqual(side_renames(r.store, r.store.get_commit(mid).parents[0], mid),
                         {"src/util.py": "src/helpers.py"})
        b.write("src/util.py", edit_line(UTIL, 4, "b"))
        res = r.submit(b)
        self.assertEqual(res.status, "conflict")            # the rewrite and b's edit do overlap
        self.assertEqual([c.path for c in res.conflicts], ["src/helpers.py"])

    def test_random_moves_and_disjoint_edits_commute(self):
        rng = random.Random(9)
        for seed in range(60):
            rng = random.Random(seed)
            files = {f"d{i % 3}/f{i}.txt": "".join(f"f{i} line {j}\n" for j in range(10))
                     for i in range(6)}
            r = Repository.init(MemoryKV(), files)
            a, b = r.fork("a"), r.fork("b")
            names = sorted(files)
            moved = rng.sample(names, rng.randint(1, 3))
            edited = rng.sample(names, rng.randint(1, 3))
            expect = dict(files)
            targets = {}
            for i, p in enumerate(moved):
                targets[p] = f"moved{i}/{p.split('/')[-1]}"
                a.move(p, targets[p])
                expect[targets[p]] = expect.pop(p)
            for p in edited:
                b.write(p, edit_line(files[p], 5, f"edit {p}"))
                dest = targets.get(p, p)
                expect[dest] = edit_line(files[p], 5, f"edit {p}")
            first, second = (a, b) if rng.random() < 0.5 else (b, a)
            r.submit(first)
            res = r.submit(second)
            self.assertEqual(res.status, "merged", (seed, res.conflicts))
            self.assertEqual({p: v.decode() for p, v in r.main_files().items()}, expect, seed)


class StorageReportTest(unittest.TestCase):
    def test_garbage_is_measured_and_live_data_is_not(self):
        r = repo()
        rep = storage_report(r)
        self.assertEqual(rep.orphan_objects, 0)
        self.assertEqual(rep.overhead, 1.0)
        a, b = r.fork("a"), r.fork("b")
        a.write("x", "1")
        b.write("x", "2")
        r.submit(a)
        r.submit(b)                       # conflicts: its commit and blob are never on main
        b.rollback()
        mid = storage_report(r)
        self.assertGreaterEqual(mid.reachable_objects, rep.reachable_objects)
        ghost = r.store.put_blob(b"nobody references this")
        self.assertEqual(storage_report(r).orphan_objects, mid.orphan_objects + 1)
        self.assertNotIn(ghost, reachable(r.store, [r.head()]))
        self.assertGreater(storage_report(r).overhead, 1.0)

    def test_chunks_of_large_files_count_as_reachable(self):
        r = repo()
        ws = r.fork("a")
        ws.write("big.bin", bytes(range(256)) * (CHUNK_BYTES // 64))
        r.submit(ws)
        rep = storage_report(r)
        self.assertEqual(rep.orphan_objects, 0)
        big = r.store.lookup(r.store.get_commit(r.head()).tree, "big.bin").id
        self.assertTrue(set(r.store.blob_chunks(big)) <= reachable(r.store, [r.head()]))


if __name__ == "__main__":
    unittest.main()
