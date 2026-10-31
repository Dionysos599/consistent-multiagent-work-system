import os
import random
import shutil
import tempfile
import time
import unittest
from pathlib import Path

from cmws_ws.kv import KVUnavailable, MemoryKV
from cmws_ws.repo import Repository
from cmws_ws.sync import Checkout, SyncError, Watcher

BASE = {"README.md": "# demo\n", "src/app.py": "".join(f"line {i}\n" for i in range(1, 13)),
        "src/util.py": "x = 1\n", "docs/a.md": "a\n"}


class FlakyKV(MemoryKV):
    down = False

    def _check(self):
        if self.down:
            raise KVUnavailable("store is down")

    def get(self, key):
        self._check()
        return super().get(key)

    def put(self, key, value):
        self._check()
        return super().put(key, value)

    def put_if_absent(self, key, value):
        self._check()
        return super().put_if_absent(key, value)

    def cas(self, key, expected, value):
        self._check()
        return super().cas(key, expected, value)


def tree_of(root):
    out = {}
    root = Path(root)
    for p in sorted(root.rglob("*")):
        if p.is_file() and ".cmws/" not in p.as_posix() + "/" and ".cmws" not in p.relative_to(root).parts:
            out[p.relative_to(root).as_posix()] = p.read_text()
    return out


def edit_line(text, n, new):
    lines = text.splitlines(keepends=True)
    lines[n - 1] = new + "\n"
    return "".join(lines)


class SyncCase(unittest.TestCase):
    kv_class = MemoryKV

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.repo = Repository.init(self.kv_class(), BASE)

    def folder(self, name):
        return Path(self.tmp) / name

    def clone(self, agent="alice", name=None):
        return Checkout.clone(self.repo, agent, self.folder(name or agent))

    def write(self, co, rel, text):
        p = co.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)

    def branch_files(self, agent):
        ws = self.repo.resume(agent)
        return {p: v.decode() for p, v in ws.files().items()}


class CheckoutBasicsTest(SyncCase):
    def test_clone_materializes_the_branch(self):
        co = self.clone()
        self.assertEqual(tree_of(co.root), BASE)
        self.assertEqual(co.status(), {})
        self.assertEqual(self.repo.refs.get("ws/alice"), co.base)

    def test_clone_refuses_a_non_empty_folder_and_double_clone(self):
        d = self.folder("x")
        d.mkdir()
        (d / "f").write_text("1")
        with self.assertRaises(SyncError):
            Checkout.clone(self.repo, "bob", d)
        co = self.clone()
        with self.assertRaises(SyncError):
            Checkout.clone(self.repo, "alice", co.root)

    def test_status_push_and_the_cloud_side_sees_it(self):
        co = self.clone()
        self.write(co, "src/util.py", "x = 2\n")
        self.write(co, "new/deep/file.txt", "n\n")
        (co.root / "docs/a.md").unlink()
        self.assertEqual(co.status(), {"docs/a.md": "D", "new/deep/file.txt": "A", "src/util.py": "M"})
        res = co.push("edits from my laptop")
        self.assertEqual(res.status, "pushed")
        self.assertEqual(res.files, ["docs/a.md", "new/deep/file.txt", "src/util.py"])
        self.assertEqual(co.status(), {})
        self.assertEqual(self.branch_files("alice"), tree_of(co.root))
        self.assertEqual(co.push().status, "nothing")
        self.assertEqual(self.repo.store.get_commit(res.commit).message, "edits from my laptop")

    def test_open_resumes_from_the_state_file(self):
        co = self.clone()
        self.write(co, "a.txt", "1")
        again = Checkout.open(self.repo, co.root)
        self.assertEqual(again.status(), {"a.txt": "A"})
        self.assertEqual(again.push().status, "pushed")

    def test_ignored_things_are_never_synced(self):
        co = self.clone()
        self.write(co, "__pycache__/x.pyc", "junk")
        self.write(co, ".DS_Store", "junk")
        self.write(co, "build/out.o", "obj")
        self.write(co, ".cmwsignore", "build\n*.log\n")
        self.write(co, "run.log", "log")
        co2 = Checkout.open(self.repo, co.root)     # re-read the ignore file
        self.assertEqual(co2.status(), {})
        self.write(co2, "keep.txt", "k")
        self.assertEqual(co2.status(), {"keep.txt": "A"})

    def test_symlinks_are_skipped(self):
        co = self.clone()
        os.symlink("/etc/hosts", co.root / "link")
        self.assertEqual(co.status(), {})

    def test_touching_a_file_is_not_a_change_but_a_same_size_edit_is(self):
        co = self.clone()
        p = co.root / "src/util.py"
        os.utime(p, None)
        self.assertEqual(co.status(), {})
        p.write_text("x = 9\n")                       # same size, different content, right away
        self.assertEqual(co.status(), {"src/util.py": "M"})

    def test_binary_and_large_files_round_trip(self):
        co = self.clone()
        data = os.urandom(700_000)
        (co.root / "blob.bin").write_bytes(data)
        self.assertEqual(co.push().status, "pushed")
        other = self.clone("alice", "second")
        # a second checkout of the same branch gets the same bytes
        self.assertEqual((other.root / "blob.bin").read_bytes(), data)


class PullTest(SyncCase):
    def test_remote_changes_arrive_and_deletions_prune_directories(self):
        co = self.clone()
        ws = self.repo.resume("alice")
        ws.write("src/new.py", "n\n")
        ws.delete("docs/a.md")
        ws.write("src/app.py", edit_line(BASE["src/app.py"], 3, "remote"))
        ws.commit("agent work")
        res = co.pull()
        self.assertEqual(res.status, "updated")
        self.assertEqual(res.updated, ["docs/a.md", "src/app.py", "src/new.py"])
        self.assertEqual(tree_of(co.root), {p: v.decode() for p, v in ws.files().items()})
        self.assertFalse((co.root / "docs").exists())
        self.assertEqual(co.pull().status, "up-to-date")
        self.assertEqual(co.status(), {})

    def test_disjoint_local_and_remote_edits_both_survive(self):
        co = self.clone()
        self.write(co, "mine.txt", "mine\n")
        self.write(co, "README.md", "# local\n")
        ws = self.repo.resume("alice")
        ws.write("theirs.txt", "theirs\n")
        ws.commit("agent")
        self.assertEqual(co.pull().status, "updated")
        self.assertEqual(co.status(), {"README.md": "M", "mine.txt": "A"})   # still pending
        self.assertEqual((co.root / "theirs.txt").read_text(), "theirs\n")
        self.assertEqual(co.push().status, "pushed")
        self.assertEqual(self.branch_files("alice"), tree_of(co.root))

    def test_same_file_different_lines_merge_into_the_folder(self):
        co = self.clone()
        self.write(co, "src/app.py", edit_line(BASE["src/app.py"], 2, "local"))
        ws = self.repo.resume("alice")
        ws.write("src/app.py", edit_line(BASE["src/app.py"], 11, "remote"))
        ws.commit("agent")
        self.assertEqual(co.pull().status, "updated")
        self.assertEqual((co.root / "src/app.py").read_text(),
                         edit_line(edit_line(BASE["src/app.py"], 2, "local"), 11, "remote"))
        self.assertEqual(co.push().status, "pushed")

    def test_a_local_rename_is_followed_by_a_remote_edit_of_the_old_name(self):
        co = self.clone()
        (co.root / "src/app.py").rename(co.root / "src/main.py")
        ws = self.repo.resume("alice")
        ws.write("src/app.py", edit_line(BASE["src/app.py"], 10, "remote edit"))
        ws.commit("agent")
        self.assertEqual(co.pull().status, "updated")
        self.assertFalse((co.root / "src/app.py").exists())
        self.assertEqual((co.root / "src/main.py").read_text(),
                         edit_line(BASE["src/app.py"], 10, "remote edit"))
        res = co.push()
        self.assertEqual(res.status, "pushed")
        self.assertEqual(self.repo.store.get_commit(res.commit).renames,
                         (("src/app.py", "src/main.py"),))


class ConflictTest(SyncCase):
    def conflicting(self):
        co = self.clone()
        self.write(co, "src/app.py", edit_line(BASE["src/app.py"], 5, "LOCAL"))
        ws = self.repo.resume("alice")
        ws.write("src/app.py", edit_line(BASE["src/app.py"], 5, "REMOTE"))
        ws.commit("agent")
        return co, ws

    def test_text_conflict_writes_markers_and_blocks_push(self):
        co, ws = self.conflicting()
        res = co.push()
        self.assertEqual((res.status, res.conflicts), ("conflict", {"src/app.py": "content"}))
        text = (co.root / "src/app.py").read_text()
        self.assertIn("<<<<<<< local\nLOCAL\n", text)
        self.assertIn("=======\nREMOTE\n>>>>>>> remote", text)
        self.assertEqual(co.push().status, "blocked")
        self.assertEqual(self.repo.refs.get("ws/alice"), ws.head)       # nothing was pushed

    def test_resolving_by_editing_unblocks_the_push(self):
        co, ws = self.conflicting()
        co.push()
        self.write(co, "src/app.py", edit_line(BASE["src/app.py"], 5, "BOTH"))
        res = co.push("resolve")
        self.assertEqual(res.status, "pushed")
        self.assertEqual(self.branch_files("alice")["src/app.py"], edit_line(BASE["src/app.py"], 5, "BOTH"))
        self.assertEqual(co.unresolved(), {})

    def test_other_files_in_a_conflicted_pull_still_arrive(self):
        co, ws = self.conflicting()
        ws.write("extra.txt", "from the agent\n")
        ws.commit("more")
        co.push()
        self.assertEqual((co.root / "extra.txt").read_text(), "from the agent\n")

    def test_modify_delete_keeps_the_file_and_needs_an_explicit_resolve(self):
        co = self.clone()
        self.write(co, "src/util.py", "x = 'local edit'\n")
        ws = self.repo.resume("alice")
        ws.delete("src/util.py")
        ws.commit("agent deleted it")
        res = co.push()
        self.assertEqual(res.conflicts, {"src/util.py": "modify-delete"})
        self.assertEqual((co.root / "src/util.py").read_text(), "x = 'local edit'\n")   # not lost
        self.assertEqual(co.push().status, "blocked")
        co.resolve("src/util.py")                                   # keep my version
        self.assertEqual(co.push().status, "pushed")
        self.assertEqual(self.branch_files("alice")["src/util.py"], "x = 'local edit'\n")

    def test_file_created_on_both_sides_with_different_content(self):
        co = self.clone()
        self.write(co, "notes.txt", "local notes\n")
        ws = self.repo.resume("alice")
        ws.write("notes.txt", "remote notes\n")
        ws.commit("agent")
        self.assertEqual(co.push().conflicts, {"notes.txt": "add-add"})
        self.assertIn("<<<<<<<", (co.root / "notes.txt").read_text())


class PushRaceTest(SyncCase):
    def test_branch_moves_between_reading_and_the_cas(self):
        co = self.clone()
        self.write(co, "mine.txt", "mine\n")
        ws = self.repo.resume("alice")
        real = self.repo.refs.cas
        fired = []

        def cas(name, expected, new):
            if name == "ws/alice" and not fired:
                fired.append(1)
                ws.write("theirs.txt", "theirs\n")       # someone gets in first
                ws.commit("agent")
            return real(name, expected, new)

        self.repo.refs.cas = cas
        res = co.push()
        self.assertEqual(res.status, "pushed")
        self.assertTrue(fired)
        final = self.branch_files("alice")
        self.assertEqual(final["mine.txt"], "mine\n")
        self.assertEqual(final["theirs.txt"], "theirs\n")
        self.assertEqual(tree_of(co.root), final)

    def test_endless_contention_gives_up_cleanly(self):
        co = self.clone()
        self.write(co, "mine.txt", "mine\n")
        ws = self.repo.resume("alice")
        real = self.repo.refs.cas
        n = [0]
        busy = [False]

        def cas(name, expected, new):
            if name == "ws/alice" and not busy[0]:
                busy[0] = True                       # the agent's own commit uses cas too
                n[0] += 1
                ws.refresh()
                ws.write(f"other{n[0]}.txt", "x")
                ws.commit("agent")
                busy[0] = False
            return real(name, expected, new)

        self.repo.refs.cas = cas
        self.assertEqual(co.push(max_attempts=3).status, "contended")


class SubmitFromFolderTest(SyncCase):
    def test_submit_lands_the_folder_on_main_and_pulls_back(self):
        co = self.clone()
        self.write(co, "feature.py", "print('feature')\n")
        res = co.submit("add feature")
        self.assertTrue(res.landed)
        self.assertEqual(self.repo.main_files()["feature.py"], b"print('feature')\n")
        self.assertEqual(co.status(), {})
        self.assertEqual(co.base, self.repo.refs.get("ws/alice"))


class WatcherTest(SyncCase):
    def test_push_waits_for_the_edit_to_settle(self):
        co = self.clone()
        w = Watcher(co, debounce=2.0)
        self.assertEqual(w.run_once(now=0), "idle")
        self.write(co, "a.txt", "1")
        self.assertEqual(w.run_once(now=1), "waiting")      # first sight
        self.assertEqual(w.run_once(now=2), "waiting")      # unchanged but not for long enough
        self.write(co, "a.txt", "12")                       # edited again: the clock restarts
        self.assertEqual(w.run_once(now=3), "waiting")
        self.assertEqual(w.run_once(now=4), "waiting")
        self.assertEqual(w.run_once(now=5.5), "pushed")
        self.assertEqual(self.branch_files("alice")["a.txt"], "12")

    def test_remote_changes_are_pulled(self):
        co = self.clone()
        w = Watcher(co)
        ws = self.repo.resume("alice")
        ws.write("r.txt", "remote\n")
        ws.commit("agent")
        w.run_once(now=0)
        self.assertEqual((co.root / "r.txt").read_text(), "remote\n")

    def test_conflict_stops_pushing_until_resolved(self):
        co = self.clone()
        w = Watcher(co, debounce=0)
        self.write(co, "src/app.py", edit_line(BASE["src/app.py"], 5, "LOCAL"))
        ws = self.repo.resume("alice")
        ws.write("src/app.py", edit_line(BASE["src/app.py"], 5, "REMOTE"))
        ws.commit("agent")
        self.assertEqual(w.run_once(now=0), "conflict")
        self.assertEqual(w.run_once(now=1), "conflict")
        self.write(co, "src/app.py", edit_line(BASE["src/app.py"], 5, "FIXED"))
        self.assertEqual(w.run_once(now=2), "waiting")
        self.assertEqual(w.run_once(now=3), "pushed")


class OfflineTest(SyncCase):
    kv_class = FlakyKV

    def test_edits_made_offline_are_merged_and_pushed_on_reconnect(self):
        co = self.clone()
        w = Watcher(co, debounce=0)
        kv = self.repo.kv
        kv.down = True
        self.write(co, "offline1.txt", "one\n")
        self.write(co, "src/util.py", "x = 'offline'\n")
        self.assertEqual(w.run_once(now=0), "offline")
        self.write(co, "offline2.txt", "two\n")
        self.assertEqual(w.run_once(now=1), "offline")
        self.assertEqual(w.state, "offline")
        # meanwhile the cloud agent keeps working on the same branch
        kv.down = False
        ws = self.repo.resume("alice")
        ws.write("cloud.txt", "cloud\n")
        ws.write("README.md", "# edited in the cloud\n")
        ws.commit("agent while laptop offline")
        kv.down = True
        self.assertEqual(w.run_once(now=2), "offline")
        kv.down = False
        self.assertEqual(w.run_once(now=3), "waiting")
        self.assertEqual(w.run_once(now=4), "pushed")
        final = self.branch_files("alice")
        self.assertEqual(final["offline1.txt"], "one\n")
        self.assertEqual(final["offline2.txt"], "two\n")
        self.assertEqual(final["cloud.txt"], "cloud\n")
        self.assertEqual(final["README.md"], "# edited in the cloud\n")
        self.assertEqual(final["src/util.py"], "x = 'offline'\n")
        self.assertEqual(tree_of(co.root), final)

    def test_a_failed_push_midway_changes_nothing(self):
        co = self.clone()
        self.write(co, "a.txt", "1")
        self.repo.kv.down = True
        with self.assertRaises(KVUnavailable):
            co.push()
        self.repo.kv.down = False
        self.assertEqual(co.status(), {"a.txt": "A"})
        self.assertEqual(co.push().status, "pushed")


class ThreadedWatcherTest(SyncCase):
    def wait_for(self, predicate, timeout=10):
        end = time.time() + timeout
        while time.time() < end:
            if predicate():
                return True
            time.sleep(0.05)
        return False

    def test_two_watched_folders_converge_through_the_store(self):
        a = self.clone("alice", "laptop")
        b = Checkout.clone(self.repo, "alice", self.folder("desktop"))
        wa = Watcher(a, interval=0.05, debounce=0.1)
        wb = Watcher(b, interval=0.05, debounce=0.1)
        wa.start(), wb.start()
        try:
            self.write(a, "from_laptop.txt", "hello\n")
            self.assertTrue(self.wait_for(lambda: (b.root / "from_laptop.txt").exists()))
            self.write(b, "from_desktop.txt", "hi\n")
            self.assertTrue(self.wait_for(lambda: (a.root / "from_desktop.txt").exists()))
            (a.root / "README.md").unlink()
            self.assertTrue(self.wait_for(lambda: not (b.root / "README.md").exists()))
        finally:
            wa.stop(), wb.stop()
        self.assertEqual(tree_of(a.root), tree_of(b.root))


class ConvergenceTest(SyncCase):
    def test_random_interleavings_of_two_folders_and_an_agent_converge(self):
        for seed in range(25):
            self.repo = Repository.init(MemoryKV(), BASE, main=f"main{seed}")
            rng = random.Random(seed)
            a = Checkout.clone(self.repo, f"u{seed}", self.folder(f"a{seed}"))
            b = Checkout.clone(self.repo, f"u{seed}", self.folder(f"b{seed}"))
            ws = self.repo.resume(f"u{seed}")
            last = {}      # file -> last content its owner wrote (None: deleted)
            for step in range(40):
                action = rng.choice(["edit_a", "edit_b", "agent", "push_a", "push_b",
                                     "pull_a", "pull_b", "del_a", "del_b"])
                n = rng.randint(0, 3)
                if action == "edit_a":
                    self.write(a, f"fa{n}.txt", f"a{step}\n"); last[f"fa{n}.txt"] = f"a{step}\n"
                elif action == "edit_b":
                    self.write(b, f"fb{n}.txt", f"b{step}\n"); last[f"fb{n}.txt"] = f"b{step}\n"
                elif action == "agent":
                    ws.refresh() if not ws.dirty else None
                    ws.write(f"fg{n}.txt", f"g{step}\n"); ws.commit("agent"); last[f"fg{n}.txt"] = f"g{step}\n"
                elif action == "del_a" and (a.root / f"fa{n}.txt").exists():
                    (a.root / f"fa{n}.txt").unlink(); last[f"fa{n}.txt"] = None
                elif action == "del_b" and (b.root / f"fb{n}.txt").exists():
                    (b.root / f"fb{n}.txt").unlink(); last[f"fb{n}.txt"] = None
                elif action == "push_a":
                    self.assertIn(a.push().status, ("pushed", "nothing"))
                elif action == "push_b":
                    self.assertIn(b.push().status, ("pushed", "nothing"))
                elif action == "pull_a":
                    a.pull()
                elif action == "pull_b":
                    b.pull()
            for co in (a, b):
                self.assertIn(co.push().status, ("pushed", "nothing"))
            for co in (a, b):
                co.pull()
            expected = {p: v for p, v in last.items() if v is not None}
            tip = self.branch_files(f"u{seed}")
            self.assertEqual(tree_of(a.root), tip, seed)
            self.assertEqual(tree_of(b.root), tip, seed)
            for p, v in expected.items():
                self.assertEqual(tip.get(p), v, (seed, p))
            for p, v in last.items():
                if v is None:
                    self.assertNotIn(p, tip, (seed, p))


if __name__ == "__main__":
    unittest.main()
