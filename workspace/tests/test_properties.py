"""W6: randomized checks of workspaces against a simple model, and of concurrent landings."""

import random
import sys
import threading
import unittest

from cmws_ws.kv import MemoryKV
from cmws_ws.objects import PathConflict
from cmws_ws.repo import Repository

from tests.support import RecordingKV, check_linear_history

NAMES = ["a", "b", "c"]


def model_kind(files, path):
    if path in files:
        return "file"
    if any(p.startswith(path + "/") for p in files):
        return "dir"
    return None


class WorkspaceModelTest(unittest.TestCase):
    """Random operations on one workspace must match a plain dict of path -> bytes."""

    def run_seed(self, seed):
        rng = random.Random(seed)
        initial = {"a/x": b"1", "b": b"2", "c/d/e": b"3"}
        repo = Repository.init(MemoryKV(), initial)
        ws = repo.fork("w")
        model = dict(initial)
        committed = dict(initial)
        commits = [(ws.head, dict(initial))]

        def rand_path():
            return "/".join(rng.choice(NAMES) for _ in range(rng.randint(1, 3)))

        for step in range(150):
            op = rng.choice(["write", "write", "delete", "move", "commit", "rollback", "undo"])
            p = rand_path()
            if op == "write":
                data = bytes([rng.randrange(256)])
                blocked_by_file = any(p.startswith(q + "/") for q in model)
                is_dir = model_kind(model, p) == "dir"
                try:
                    ws.write(p, data)
                except PathConflict:
                    self.assertTrue(blocked_by_file, (seed, step, p))
                except IsADirectoryError:
                    self.assertTrue(is_dir, (seed, step, p))
                else:
                    self.assertFalse(blocked_by_file or is_dir, (seed, step, p))
                    model[p] = data
            elif op == "delete":
                kind = model_kind(model, p)
                try:
                    ws.delete(p)
                except FileNotFoundError:
                    self.assertIsNone(kind)
                else:
                    self.assertIsNotNone(kind)
                    for q in [q for q in model if q == p or q.startswith(p + "/")]:
                        del model[q]
            elif op == "move":
                dst = rand_path()
                kind = model_kind(model, p)
                if kind is None or dst == p or dst.startswith(p + "/"):
                    continue
                # only attempt moves whose target is free, so the model stays simple
                moved = {q: v for q, v in model.items() if q == p or q.startswith(p + "/")}
                rest = {q: v for q, v in model.items() if q not in moved}
                targets = {dst + q[len(p):]: v for q, v in moved.items()}
                free = all(model_kind(rest, t) is None and
                           not any(t.startswith(r + "/") for r in rest) for t in targets)
                if not free:
                    continue
                ws.move(p, dst)
                model = {**rest, **targets}
            elif op == "commit":
                if ws.commit(f"step {step}"):
                    committed = dict(model)
                    commits.append((ws.head, dict(model)))
            elif op == "rollback":
                ws.rollback()
                model = dict(committed)
            else:  # undo to an earlier commit of this branch
                cid, snapshot = rng.choice(commits)
                ws.rollback(to=cid)
                model = dict(snapshot)
                committed = dict(snapshot)
                del commits[commits.index((cid, snapshot)) + 1:]
            self.assertEqual(ws.files(), model, (seed, step, op))
            self.assertEqual(ws.dirty, model != committed, (seed, step, op))

    def test_random_operation_sequences(self):
        for seed in range(40):
            self.run_seed(seed)


class SnapshotIsolationTest(unittest.TestCase):
    def test_a_workspace_never_sees_what_others_do_until_it_lands(self):
        rng = random.Random(5)
        repo = Repository.init(MemoryKV(), {"f0": b"0"})
        forks = []  # [workspace, the view it must have: fork snapshot + its own commits]
        n = 0
        for step in range(150):
            if rng.random() < 0.3 or not forks:
                forks.append([repo.fork(f"a{n}"), repo.main_files()])
                n += 1
            else:
                entry = rng.choice(forks)
                ws, view = entry
                if rng.random() < 0.6:
                    path = f"g{rng.randint(0, 8)}"
                    ws.write(path, str(step))
                    ws.commit("c")
                    view[path] = str(step).encode()
                else:
                    res = repo.submit(ws)  # merges, conflicts or goes stale; main moves either way
                    if res.landed:
                        entry[1] = repo.files_at(res.commit)
            for ws, view in forks:
                self.assertEqual(ws.files(), view)


class ConcurrentLandingTest(unittest.TestCase):
    def setUp(self):
        # force frequent thread switches so landings really race
        self._interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)

    def tearDown(self):
        sys.setswitchinterval(self._interval)

    def test_disjoint_work_from_many_threads_is_never_lost(self):
        kv = RecordingKV()
        base_lines = "".join(f"line{i}\n" for i in range(60))
        repo = Repository.init(kv, {"index.txt": base_lines})
        root = repo.head()
        agents, rounds = 12, 6
        attempts = []
        errors = []

        def agent(k):
            try:
                for r in range(rounds):
                    ws = repo.fork(f"agent{k}-round{r}")
                    lines = ws.read_text("index.txt").splitlines(keepends=True)
                    lines[k * 5] = f"agent{k} round{r}\n"      # lines 5 apart never touch
                    ws.write("index.txt", "".join(lines))
                    ws.write(f"out/agent{k}/r{r}.txt", f"{k}:{r}\n")
                    res = repo.submit(ws, f"agent {k} round {r}", allow_stale=True)
                    attempts.append(res.attempts)
                    if not res.landed:
                        errors.append((k, r, res))
            except Exception as e:  # pragma: no cover - surfaced below
                errors.append((k, "exception", repr(e)))

        threads = [threading.Thread(target=agent, args=(k,)) for k in range(agents)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(errors, [])
        files = repo.main_files()
        for k in range(agents):
            for r in range(rounds):
                self.assertEqual(files[f"out/agent{k}/r{r}.txt"], f"{k}:{r}\n".encode())
        lines = files["index.txt"].decode().splitlines()
        for k in range(agents):
            self.assertEqual(lines[k * 5], f"agent{k} round{rounds - 1}")
        check_linear_history(self, repo, kv.transitions, root)
        self.assertEqual(len(kv.transitions), 1 + agents * rounds)
        self.assertGreater(max(attempts), 1, "the landings never raced; the test proves nothing")

    def increment(self, repo, agent_id, allow_stale):
        ws = repo.fork(agent_id)
        n = int(ws.read_text("counter.txt"))
        ws.write("counter.txt", f"{n + 1}\n")
        return repo.submit(ws, "increment", allow_stale=allow_stale)

    def test_read_then_write_counter_loses_no_update_with_stale_read_checks(self):
        kv = RecordingKV()
        repo = Repository.init(kv, {"counter.txt": "0\n"})
        root = repo.head()
        threads_n, per = 8, 6
        landed = []
        redone = []

        def agent(k):
            for i in range(per):
                attempt = 0
                while True:
                    res = self.increment(repo, f"c{k}-{i}-{attempt}", allow_stale=False)
                    if res.landed:
                        landed.append(res.commit)
                        break
                    attempt += 1  # stale or conflicting: redo on the new main
                    redone.append(res.status)

        ts = [threading.Thread(target=agent, args=(k,)) for k in range(threads_n)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(repo.main_files()["counter.txt"], f"{threads_n * per}\n".encode())
        self.assertEqual(len(landed), threads_n * per)
        self.assertIn("stale", redone, "no agent ever saw a stale read; the test proves nothing")
        check_linear_history(self, repo, kv.transitions, root)

    def test_without_stale_read_checks_the_same_counter_loses_updates(self):
        """Both agents write '1': the textual merge sees identical edits and accepts them."""
        repo = Repository.init(MemoryKV(), {"counter.txt": "0\n"})
        a, b = repo.fork("a"), repo.fork("b")
        for ws in (a, b):
            ws.write("counter.txt", f"{int(ws.read_text('counter.txt')) + 1}\n")
        self.assertTrue(repo.submit(a, allow_stale=True).landed)
        res = repo.submit(b, allow_stale=True)
        self.assertTrue(res.landed)                     # merges cleanly ...
        self.assertEqual(repo.main_files()["counter.txt"], b"1\n")  # ... and one increment is gone
        # the default policy refuses exactly this case
        repo2 = Repository.init(MemoryKV(), {"counter.txt": "0\n"})
        a, b = repo2.fork("a"), repo2.fork("b")
        for ws in (a, b):
            ws.write("counter.txt", f"{int(ws.read_text('counter.txt')) + 1}\n")
        self.assertTrue(repo2.submit(a).landed)
        self.assertEqual(repo2.submit(b).status, "stale")

    def test_exactly_one_of_many_identical_races_wins_per_round(self):
        repo = Repository.init(MemoryKV(), {"lock": b""})
        results = []
        barrier = threading.Barrier(10)

        def agent(k):
            ws = repo.fork(f"racer{k}")
            ws.write("winner", str(k))
            barrier.wait()
            results.append(repo.submit(ws))

        ts = [threading.Thread(target=agent, args=(k,)) for k in range(10)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        landed = [r for r in results if r.landed]
        self.assertEqual(len(landed), 1)                 # add/add of different content conflicts
        self.assertEqual(sorted(r.status for r in results if not r.landed), ["conflict"] * 9)


if __name__ == "__main__":
    unittest.main()
