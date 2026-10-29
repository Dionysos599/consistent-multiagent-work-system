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


if __name__ == "__main__":
    unittest.main()
