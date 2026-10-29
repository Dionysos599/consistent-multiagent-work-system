import random
import unittest

from cmws_ws.diff import ADDED, DELETED, MODIFIED, diff_trees, unified_all
from cmws_ws.kv import MemoryKV
from cmws_ws.objects import ObjectStore
from cmws_ws.textmerge import merge3, merge_text, split_lines
from cmws_ws.treemerge import merge_trees


def L(text):
    return split_lines(text)


class TextMergeTest(unittest.TestCase):
    def test_disjoint_edits_merge(self):
        base = "a\nb\nc\nd\ne\n"
        r = merge_text(base, "A\nb\nc\nd\ne\n", "a\nb\nc\nd\nE\n")
        self.assertTrue(r.clean)
        self.assertEqual(r.text, "A\nb\nc\nd\nE\n")

    def test_same_change_on_both_sides(self):
        r = merge_text("a\nb\n", "a\nX\n", "a\nX\n")
        self.assertTrue(r.clean)
        self.assertEqual(r.text, "a\nX\n")

    def test_overlapping_edits_conflict_with_markers(self):
        r = merge_text("a\nb\nc\n", "a\nB1\nc\n", "a\nB2\nc\n", ours_label="me", theirs_label="up")
        self.assertFalse(r.clean)
        self.assertEqual(len(r.conflicts), 1)
        c = r.conflicts[0]
        self.assertEqual((c.base, c.ours, c.theirs), (("b\n",), ("B1\n",), ("B2\n",)))
        self.assertEqual(r.text, "a\n<<<<<<< me\nB1\n||||||| base\nb\n=======\nB2\n>>>>>>> up\nc\n")

    def test_adjacent_edits_are_conflicts_like_git(self):
        self.assertFalse(merge_text("a\nb\nc\n", "A\nb\nc\n", "a\nB\nc\n").clean)

    def test_inserts_at_different_places(self):
        r = merge_text("a\nb\nc\n", "new\na\nb\nc\n", "a\nb\nc\nend\n")
        self.assertEqual(r.text, "new\na\nb\nc\nend\n")

    def test_delete_vs_edit_conflicts_delete_vs_delete_ok(self):
        self.assertFalse(merge_text("a\nb\nc\n", "a\nc\n", "a\nBB\nc\n").clean)
        self.assertEqual(merge_text("a\nb\nc\n", "a\nc\n", "a\nc\n").text, "a\nc\n")

    def test_missing_trailing_newline(self):
        r = merge_text("a\nb\nc", "a\nb\nc\nd\n", "A\nb\nc")
        self.assertTrue(r.clean, r.text)
        self.assertEqual(r.text, "A\nb\nc\nd\n")

    def test_empty_base_both_add_same_vs_different(self):
        self.assertTrue(merge_text(None, "x\n", "x\n").clean)
        self.assertFalse(merge_text(None, "x\n", "y\n").clean)

    def test_random_disjoint_edits_equal_applying_both(self):
        """Edits made in separate, non-touching regions must always merge to 'both applied'."""
        rng = random.Random(3)
        for _ in range(300):
            n = rng.randint(8, 30)
            base = [f"line{i}\n" for i in range(n)]
            cut = rng.randint(3, n - 4)
            left, right = list(range(0, cut - 1)), list(range(cut + 1, n))
            ours, theirs, expect = list(base), list(base), list(base)
            for idx in rng.sample(left, min(len(left), rng.randint(0, 3))):
                ours[idx] = expect[idx] = f"ours{idx}\n"
            for idx in rng.sample(right, min(len(right), rng.randint(0, 3))):
                theirs[idx] = expect[idx] = f"theirs{idx}\n"
            r = merge3(base, ours, theirs)
            # edits can still touch when two picks are adjacent within one side; that is fine,
            # those are the same side. Across sides the regions are >= 2 lines apart.
            self.assertTrue(r.clean)
            self.assertEqual(r.lines, expect)

    def test_merge_is_symmetric(self):
        rng = random.Random(11)
        for _ in range(300):
            base = [f"{rng.randint(0, 5)}\n" for _ in range(rng.randint(0, 12))]

            def mutate():
                s = list(base)
                for _ in range(rng.randint(0, 3)):
                    op = rng.choice("ird")
                    k = rng.randint(0, len(s))
                    if op == "i":
                        s.insert(k, f"n{rng.randint(0, 3)}\n")
                    elif s and k < len(s):
                        if op == "r":
                            s[k] = f"m{rng.randint(0, 3)}\n"
                        else:
                            del s[k]
                return s

            o, t = mutate(), mutate()
            fwd, rev = merge3(base, o, t), merge3(base, t, o)
            self.assertEqual(fwd.clean, rev.clean)
            if fwd.clean:
                self.assertEqual(fwd.lines, rev.lines)
            # merging with an unchanged side returns the other side
            self.assertEqual(merge3(base, o, base).lines, o)
            self.assertEqual(merge3(base, base, t).lines, t)


class TreeMergeTest(unittest.TestCase):
    def setUp(self):
        self.store = ObjectStore(MemoryKV())

    def tree(self, files):
        return self.store.build({p: self.store.put_blob(d if isinstance(d, bytes) else d.encode())
                                 for p, d in files.items()})

    def files(self, tid):
        return {p: self.store.get_blob(b).decode("utf-8", "replace")
                for p, b in self.store.flatten(tid).items()}

    def test_independent_files_and_directories(self):
        base = self.tree({"a.txt": "1\n", "d/x": "x\n"})
        ours = self.tree({"a.txt": "1\n", "d/x": "x\n", "d/mine": "m\n"})
        theirs = self.tree({"a.txt": "2\n", "d/x": "x\n", "new/t": "t\n"})
        r = merge_trees(self.store, base, ours, theirs)
        self.assertTrue(r.clean)
        self.assertEqual(self.files(r.tree), {"a.txt": "2\n", "d/x": "x\n", "d/mine": "m\n",
                                              "new/t": "t\n"})

    def test_same_file_disjoint_lines_auto_merged(self):
        base = self.tree({"f": "1\n2\n3\n4\n5\n"})
        r = merge_trees(self.store, base, self.tree({"f": "A\n2\n3\n4\n5\n"}),
                        self.tree({"f": "1\n2\n3\n4\nE\n"}))
        self.assertTrue(r.clean)
        self.assertEqual(r.auto_merged, ["f"])
        self.assertEqual(self.files(r.tree)["f"], "A\n2\n3\n4\nE\n")

    def test_conflict_kinds(self):
        base = self.tree({"c": "1\n", "md": "1\n", "bin": b"\xff\x00a", "t": "x\n"})
        ours = self.tree({"c": "ours\n", "bin": b"\xff\x00b", "t/inner": "d\n", "aa": "x\n"})
        theirs = self.tree({"c": "theirs\n", "md": "2\n", "bin": b"\xff\x00c", "t": "y\n",
                            "aa": "y\n"})
        r = merge_trees(self.store, base, ours, theirs)
        self.assertIsNone(r.tree)
        kinds = {c.path: c.kind for c in r.conflicts}
        self.assertEqual(kinds, {"c": "content", "md": "modify-delete", "bin": "binary",
                                 "t": "type", "aa": "add-add"})
        content = next(c for c in r.conflicts if c.path == "c")
        self.assertEqual(content.regions[0].ours, ("ours\n",))

    def test_both_delete_and_unchanged_delete(self):
        base = self.tree({"a": "1\n", "b": "2\n", "c": "3\n"})
        ours = self.tree({"c": "3\n"})
        theirs = self.tree({"b": "2\n", "c": "3\n"})
        r = merge_trees(self.store, base, ours, theirs)
        self.assertTrue(r.clean)
        self.assertEqual(self.files(r.tree), {"c": "3\n"})

    def test_unrelated_histories_use_empty_base(self):
        r = merge_trees(self.store, None, self.tree({"a": "1\n"}), self.tree({"b": "2\n"}))
        self.assertEqual(self.files(r.tree), {"a": "1\n", "b": "2\n"})

    def test_emptied_directory_disappears(self):
        base = self.tree({"d/x": "1\n", "k": "k\n"})
        r = merge_trees(self.store, base, self.tree({"k": "k\n"}), self.tree({"d/x": "1\n", "k": "K\n"}))
        self.assertEqual(self.files(r.tree), {"k": "K\n"})


class DiffTest(unittest.TestCase):
    def test_diff_kinds_and_pruning(self):
        store = ObjectStore(MemoryKV())

        def tree(files):
            return store.build({p: store.put_blob(d.encode()) for p, d in files.items()})

        a = tree({"keep/x": "1\n", "mod": "1\n", "gone": "g\n", "dir/f": "f\n"})
        b = tree({"keep/x": "1\n", "mod": "2\n", "new": "n\n", "dir": "now a file\n"})
        got = {c.path: c.kind for c in diff_trees(store, a, b)}
        self.assertEqual(got, {"mod": MODIFIED, "gone": DELETED, "new": ADDED,
                               "dir/f": DELETED, "dir": ADDED})
        self.assertEqual(diff_trees(store, a, a), [])
        self.assertEqual(len(diff_trees(store, None, a)), 4)
        text = unified_all(store, a, b)
        self.assertIn("--- a/mod", text)
        self.assertIn("+2", text)


if __name__ == "__main__":
    unittest.main()
