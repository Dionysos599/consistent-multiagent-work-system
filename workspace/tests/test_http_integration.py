"""The workspace engine on the real replicated store, through the HTTP gateway.

Starts ``make run-gateway`` (2 groups x 3 replicas) in a subprocess. Skipped when make or java is
not available. Set CMWS_SKIP_INTEGRATION=1 to skip explicitly.
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest
import urllib.request

from cmws_ws.kv import HttpKV
from cmws_ws.repo import Repository

from tests.support import check_linear_history

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC = os.path.join(ROOT, "workspace", "src")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipIf(os.environ.get("CMWS_SKIP_INTEGRATION") == "1", "integration tests disabled")
@unittest.skipUnless(shutil.which("make") and shutil.which("java"), "needs make and java")
class GatewayIntegrationTest(unittest.TestCase):
    proc = None

    @classmethod
    def setUpClass(cls):
        build = subprocess.run(["make"], cwd=ROOT, capture_output=True, text=True)
        if build.returncode != 0:
            raise unittest.SkipTest("cannot build the store: " + build.stderr[-300:])
        cls.port = free_port()
        cls.proc = subprocess.Popen(
            ["make", "run-gateway", f"ARGS=--port {cls.port} --groups 2 --replicas 3"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)
        cls.base = f"http://127.0.0.1:{cls.port}"
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(cls.base + "/health", timeout=1):
                    return
            except Exception:
                time.sleep(0.3)
        cls.tearDownClass()
        raise RuntimeError("gateway did not start")

    @classmethod
    def tearDownClass(cls):
        if cls.proc is not None:
            try:
                os.killpg(cls.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            cls.proc.wait(timeout=20)

    def kv(self):
        return HttpKV(self.base, timeout=30)

    def admin(self, action, node):
        req = urllib.request.Request(f"{self.base}/admin/{action}?node={node}", method="POST")
        urllib.request.urlopen(req, timeout=10).read()

    def test_kv_semantics_with_awkward_keys_and_values(self):
        kv = self.kv()
        prefix = f"it-{time.time_ns()}/"
        for key, value in [(prefix + "a b/c?d&e=f", "x\ny\r\n\t\"quoted\" \\ ☃ é"),
                           (prefix + "empty", ""), (prefix + "plus+sign", "1+1")]:
            kv.put(key, value)
            self.assertEqual(kv.get(key), value, key)
        self.assertIsNone(kv.get(prefix + "missing"))
        self.assertEqual(kv.put_if_absent(prefix + "pia", "one"), (True, "one"))
        self.assertEqual(kv.put_if_absent(prefix + "pia", "two"), (False, "one"))
        self.assertEqual(kv.cas(prefix + "c", None, "v1"), (True, "v1"))
        self.assertEqual(kv.cas(prefix + "c", None, "v2"), (False, "v1"))
        self.assertEqual(kv.cas(prefix + "c", "v1", "v2"), (True, "v2"))

    def test_agents_land_work_concurrently_through_the_cluster(self):
        kv = self.kv()
        repo = Repository.init(kv, {"counter.txt": "0\n", "notes/readme.md": "# notes\n"},
                               main=f"main-{time.time_ns()}")
        agents, per = 5, 3
        landed, redone = [], []

        def agent(k):
            for i in range(per):
                attempt = 0
                while True:
                    ws = repo.fork(f"it{time.time_ns()}-{k}-{i}-{attempt}")
                    n = int(ws.read_text("counter.txt"))
                    ws.write("counter.txt", f"{n + 1}\n")
                    ws.write(f"out/{k}-{i}.txt", f"{k}:{i}\n")
                    res = repo.submit(ws, f"agent {k} step {i}")
                    if res.landed:
                        landed.append(res.commit)
                        break
                    redone.append(res.status)
                    attempt += 1

        ts = [threading.Thread(target=agent, args=(k,)) for k in range(agents)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        files = repo.main_files()
        self.assertEqual(files["counter.txt"], f"{agents * per}\n".encode())
        for k in range(agents):
            for i in range(per):
                self.assertIn(f"out/{k}-{i}.txt", files)
        self.assertEqual(len(landed), agents * per)

    def test_a_large_binary_file_goes_through_in_chunks(self):
        import os as _os
        name = f"main-{time.time_ns()}"
        repo = Repository.init(self.kv(), {"a.txt": "1\n"}, main=name)
        data = _os.urandom(1_300_000)
        ws = repo.fork(f"big-{time.time_ns()}")
        ws.write("data/model.bin", data)
        self.assertTrue(repo.submit(ws).landed)
        fresh = Repository(self.kv(), main=name)
        self.assertEqual(fresh.main_files()["data/model.bin"], data)

    def test_folder_sync_and_cli_against_the_real_cluster(self):
        import tempfile
        from pathlib import Path
        name = f"main-{time.time_ns()}"
        agent = f"cli{time.time_ns()}"
        work = Path(tempfile.mkdtemp())
        project = work / "project"
        (project / "src").mkdir(parents=True)
        (project / "src/app.py").write_text("print('v1')\n")
        (project / "README.md").write_text("# demo\n")
        (project / "__pycache__").mkdir()
        (project / "__pycache__/junk.pyc").write_text("junk")

        def cli(*args):
            r = subprocess.run([sys.executable, "-m", "cmws_ws.cli", "--url", self.base,
                                "--main", name, *args], capture_output=True, text=True,
                               env={**os.environ, "PYTHONPATH": SRC}, cwd=work)
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
            return r.stdout

        cli("init", "--from", str(project))
        cli("clone", agent, str(work / "mine"))
        mine = work / "mine"
        self.assertEqual((mine / "src/app.py").read_text(), "print('v1')\n")
        self.assertFalse((mine / "__pycache__").exists())
        (mine / "src/app.py").write_text("print('v2')\n")
        (mine / "notes.txt").write_text("hello\n")
        self.assertIn("M src/app.py", cli("status", str(mine)))
        self.assertIn("pushed", cli("push", str(mine), "-m", "my edits"))
        out = cli("submit", str(mine))
        self.assertRegex(out, "fast-forward|merged")
        repo = Repository(self.kv(), main=name)
        self.assertEqual(repo.main_files()["src/app.py"], b"print('v2')\n")
        self.assertIn("my edits", cli("log"))        # the pushed commit is what landed on main
        shutil.rmtree(work, ignore_errors=True)

    def test_work_survives_losing_a_replica_per_group_and_a_fresh_client_sees_it(self):
        kv = self.kv()
        name = f"main-{time.time_ns()}"
        repo = Repository.init(kv, {"a.txt": "1\n"}, main=name)
        ws = repo.fork(f"k-{time.time_ns()}")
        ws.write("b.txt", "2\n")
        self.assertTrue(repo.submit(ws).landed)
        self.admin("kill", "server1-1")
        self.admin("kill", "server2-2")
        try:
            ws2 = repo.fork(f"k2-{time.time_ns()}")
            ws2.write("c.txt", "3\n")
            self.assertTrue(repo.submit(ws2).landed)
            # a client with a cold cache reads everything back from the degraded cluster
            fresh = Repository(self.kv(), main=name)
            self.assertEqual(sorted(fresh.main_files()), ["a.txt", "b.txt", "c.txt"])
        finally:
            self.admin("revive", "server1-1")
            self.admin("revive", "server2-2")

    def test_leases_and_a_scheduled_run_on_the_cluster_with_replicas_down(self):
        from cmws_ws.agents import LoopWorker, Reply, ScriptedModel, Task, TaskGraph, Scheduler
        from cmws_ws.agents.llm import call
        from cmws_ws.leases import LeaseTable

        tag = f"{time.time_ns()}"
        leases = LeaseTable(self.kv(), prefix=f"it-{tag}/", ttl=120)
        wins = []
        barrier = threading.Barrier(6)

        def claim(i):
            barrier.wait()
            if LeaseTable(self.kv(), prefix=f"it-{tag}/", ttl=120).acquire(f"w{i}", ["shared", f"own{i}"]).granted:
                wins.append(i)
        ts = [threading.Thread(target=claim, args=(i,)) for i in range(6)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len(wins), 1, "overlapping claims must be serialized by the replicated store")
        leases.release(f"w{wins[0]}")

        class ByTask:
            name = "by-task"

            def run(self, task, repo, agent_id):
                model = ScriptedModel([
                    Reply("", [call("write_file", "1", path=task.paths[0], content=f"{task.id}\n")]),
                    Reply("", [call("finish", "2", summary=f"wrote {task.paths[0]}")])])
                return LoopWorker(model).run(task, repo, agent_id)

        name = f"main-{tag}"
        repo = Repository.init(self.kv(), {"base.txt": "b\n"}, main=name)
        ids = [f"s{tag}-{i}" for i in range(5)]
        tasks = [Task(i, f"write {i}", (f"out/{i}.txt",)) for i in ids]
        self.admin("kill", "server1-1")
        self.admin("kill", "server2-2")
        try:
            rep = Scheduler(repo, leases, ByTask(), parallel=3, poll=0.01).run(TaskGraph(tasks))
            self.assertTrue(rep.ok, {k: v.reason for k, v in rep.outcomes.items()})
            fresh = Repository(self.kv(), main=name)
            files = fresh.main_files()
            for i in ids:
                self.assertEqual(files[f"out/{i}.txt"], f"{i}\n".encode())
            self.assertEqual(leases.holders(), {})
        finally:
            self.admin("revive", "server1-1")
            self.admin("revive", "server2-2")

    def test_merge_queue_on_the_cluster_resolves_a_conflict_and_gates_main_with_a_real_command(self):
        from cmws_ws.agents import CommandValidator, Escalations, MergeQueue, Reply, ScriptedModel, Task, merger_worker
        from cmws_ws.agents.llm import call

        tag = f"{time.time_ns()}"
        name = f"main-{tag}"
        pad = lambda *xs: "".join(f"{x}\n" for x in xs)
        base = pad("A = 1", "# p1", "# p2", "# p3", "B = 2", "# p4", "# p5", "# p6", "C = 3")
        repo = Repository.init(self.kv(), {"cfg.py": base, "check.py": "import cfg\nassert cfg.A < cfg.C\n"}, main=name)
        edits = {"one": pad("A = 10", "# p1", "# p2", "# p3", "B = 2", "# p4", "# p5", "# p6", "C = 30"),
                 "two": pad("A = 1", "# p1", "# p2", "D = 4", "# p3", "B = 2", "# p4", "# p5", "# p6", "C = 3"),
                 "three": pad("A = 1", "# p1", "# p2", "# p3", "B = 20", "# p4", "# p5", "# p6", "C = 31")}
        for who, text in edits.items():
            ws = repo.fork(f"{who}-{tag}")
            ws.write("cfg.py", text)
            ws.commit(who)
        resolved = pad("A = 10", "# p1", "# p2", "D = 4", "# p3", "B = 20", "# p4", "# p5", "# p6", "C = 30")
        merger = merger_worker(ScriptedModel([
            Reply("", [call("write_file", "1", path="cfg.py", content=resolved)]),
            Reply("", [call("finish", "2", summary="kept both")])]))
        q = MergeQueue(repo, CommandValidator([sys.executable, "check.py"]), merger=merger,
                       escalations=Escalations(self.kv(), prefix=f"it-{tag}/"))
        self.assertTrue(q.land(Task(f"one-{tag}", "x")).landed)             # A=10, C=30
        two = q.land(Task(f"two-{tag}", "x"))                              # adds D: merges cleanly
        self.assertEqual((two.landed, two.via), (True, "auto"), two.steps)
        three = q.land(Task(f"three-{tag}", "x"))                          # B=20 next to D=4: conflicts
        self.assertTrue(three.landed, three.steps)
        final = Repository(self.kv(), main=name).main_files()["cfg.py"].decode()
        self.assertEqual(final, resolved)
        self.assertEqual(three.via, "merger")


if __name__ == "__main__":
    unittest.main()
