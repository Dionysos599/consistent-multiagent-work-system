import json
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from cmws_ws.agents import (CliWorker, LoopWorker, ModelError, OpenAICompatModel, Reply, ScriptedModel, Task,
                            Toolbox, Usage, run_task)
from cmws_ws.agents.llm import call, parse_reply
from cmws_ws.kv import MemoryKV
from cmws_ws.repo import Repository

PROJECT = {"app.py": b"def add(a, b):\n    return a - b\n", "util.py": b"X = 1\n", "docs/readme.md": b"hello\n"}


def new_repo():
    return Repository.init(MemoryKV(), dict(PROJECT))


class ToolboxTests(unittest.TestCase):
    def setUp(self):
        self.repo = new_repo()
        self.ws = self.repo.fork("t")
        self.tb = Toolbox(self.ws)

    def test_read_list_write_replace(self):
        self.assertEqual(self.tb.call("list_files", {}).text, "app.py\ndocs/\nutil.py")
        self.assertEqual(self.tb.call("list_files", {"directory": "docs"}).text, "readme.md")
        self.assertIn("return a - b", self.tb.call("read_file", {"path": "app.py"}).text)
        r = self.tb.call("replace_in_file", {"path": "app.py", "old": "a - b", "new": "a + b"})
        self.assertTrue(r.ok)
        self.assertIn("a + b", self.tb.call("read_file", {"path": "app.py"}).text)
        self.tb.call("write_file", {"path": "new/x.txt", "content": "hi"})
        self.assertEqual(self.tb.written, {"app.py", "new/x.txt"})

    def test_replace_requires_a_unique_match(self):
        self.tb.call("write_file", {"path": "d.txt", "content": "a a"})
        for old in ("a", "zzz", ""):
            r = self.tb.call("replace_in_file", {"path": "d.txt", "old": old, "new": "b"})
            self.assertFalse(r.ok, old)
        self.assertEqual(self.ws.read_text("d.txt"), "a a")

    def test_errors_come_back_as_text(self):
        for name, args in [("read_file", {"path": "missing"}), ("read_file", {"path": "../x"}),
                           ("nope", {}), ("write_file", {"path": "a"}), ("read_file", {"_raw": "{bad"}),
                           ("list_files", {"directory": "app.py"}), ("move_file", {"src": "x", "dst": "y"})]:
            r = self.tb.call(name, args)
            self.assertFalse(r.ok, (name, args))
            self.assertTrue(r.text.startswith("error"), r.text)

    def test_delete_move_finish_and_truncation(self):
        self.assertTrue(self.tb.call("move_file", {"src": "util.py", "dst": "lib/util.py"}).ok)
        self.assertTrue(self.tb.call("delete_file", {"path": "docs"}).ok)
        self.assertEqual(sorted(self.ws.files()), ["app.py", "lib/util.py"])
        self.assertTrue(self.tb.call("finish", {"summary": "ok"}).finished)
        small = Toolbox(self.ws, max_output=5)
        self.assertIn("cut off", small.call("read_file", {"path": "app.py"}).text)

    def test_binary_file_is_an_error_not_a_crash(self):
        self.ws.write("blob.bin", b"\xff\xfe\x00")
        self.assertFalse(self.tb.call("read_file", {"path": "blob.bin"}).ok)


class FakeServer:
    """A chat-completions endpoint that answers from a list of (status, body)."""

    def __init__(self, replies):
        self.replies, self.requests = list(replies), []
        outer = self

        class H(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                outer.requests.append((self.path, body, dict(self.headers)))
                status, payload = outer.replies.pop(0) if outer.replies else (500, {})
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}/v1"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def completion(content="", calls=(), usage=(3, 2)):
    msg = {"role": "assistant", "content": content}
    if calls:
        msg["tool_calls"] = list(calls)
    return 200, {"choices": [{"message": msg}], "usage": {"prompt_tokens": usage[0], "completion_tokens": usage[1]}}


def wire_call(i, name, args, as_string=True):
    return {"id": f"c{i}", "type": "function",
            "function": {"name": name, "arguments": json.dumps(args) if as_string else args}}


class ModelClientTests(unittest.TestCase):
    def test_parse_reply_shapes(self):
        r = parse_reply(completion("x", [wire_call(1, "a", {"p": 1}), wire_call(2, "b", {"q": 2}, False)])[1])
        self.assertEqual([(c.name, c.arguments) for c in r.tool_calls], [("a", {"p": 1}), ("b", {"q": 2})])
        self.assertEqual(r.usage, Usage(3, 2))
        bad = parse_reply({"choices": [{"message": {"tool_calls": [
            {"function": {"name": "a", "arguments": "{oops"}}]}}]})
        self.assertEqual(bad.tool_calls[0].arguments, {"_raw": "{oops"})
        with self.assertRaises(ModelError):
            parse_reply({"error": "x"})

    def test_request_shape_and_retry_on_server_error(self):
        srv = FakeServer([(503, {}), completion("hi")])
        self.addCleanup(srv.close)
        m = OpenAICompatModel("qwen", srv.url, api_key="k", sleep=lambda s: None)
        tools = Toolbox(new_repo().fork("t")).specs()
        reply = m.chat([{"role": "user", "content": "yo"}], tools)
        self.assertEqual(reply.content, "hi")
        self.assertEqual(len(srv.requests), 2)
        path, body, headers = srv.requests[1]
        self.assertEqual(path, "/v1/chat/completions")
        self.assertEqual(body["model"], "qwen")
        self.assertEqual(len(body["tools"]), len(tools))
        self.assertEqual(headers["Authorization"], "Bearer k")

    def test_client_errors_are_not_retried_and_unreachable_is_model_error(self):
        srv = FakeServer([(400, {"error": "bad"}), completion("late")])
        self.addCleanup(srv.close)
        with self.assertRaises(ModelError):
            OpenAICompatModel("m", srv.url, sleep=lambda s: None).chat([], [])
        self.assertEqual(len(srv.requests), 1)
        with self.assertRaises(ModelError):
            OpenAICompatModel("m", "http://127.0.0.1:9/v1", retries=1, sleep=lambda s: None, timeout=1).chat([], [])


def script_fix():
    return [
        Reply("", [call("list_files", "1")], Usage(10, 1)),
        Reply("", [call("read_file", "2", path="app.py")], Usage(12, 2)),
        Reply("", [call("replace_in_file", "3", path="app.py", old="a - b", new="a + b")], Usage(14, 3)),
        Reply("", [call("finish", "4", summary="fixed add\nmore detail")], Usage(16, 4)),
    ]


class LoopWorkerTests(unittest.TestCase):
    def setUp(self):
        self.repo = new_repo()
        self.main0 = self.repo.head()

    def test_task_lands_on_its_own_branch_and_main_is_untouched(self):
        model = ScriptedModel(script_fix())
        res = run_task(LoopWorker(model), self.repo, Task("fix-add", "make add add", ("app.py",)))
        self.assertTrue(res.ok, res)
        self.assertEqual((res.steps, res.prompt_tokens, res.completion_tokens), (4, 52, 10))
        self.assertEqual(res.touched, ("app.py",))
        self.assertEqual(self.repo.head(), self.main0)
        self.assertEqual(self.repo.files_at(self.repo.refs.get("ws/fix-add"))["app.py"],
                         b"def add(a, b):\n    return a + b\n")
        self.assertEqual(res.commit, self.repo.refs.get("ws/fix-add"))
        self.assertEqual(self.repo.store.get_commit(res.commit).message, "fix-add: fixed add")
        self.assertTrue(self.repo.submit(self.repo.resume("fix-add")).landed)

    def test_prompt_carries_task_scope_and_tool_results_are_paired(self):
        model = ScriptedModel(script_fix())
        res = run_task(LoopWorker(model), self.repo, Task("t", "do it", ("app.py",), "see docs"))
        first = model.calls[0]
        self.assertIn("app.py", first[1]["content"])
        self.assertIn("see docs", first[1]["content"])
        ids = [m["tool_call_id"] for m in res.transcript if m["role"] == "tool"]
        self.assertEqual(ids, ["1", "2", "3", "4"])

    def test_tool_errors_are_fed_back_and_the_model_recovers(self):
        model = ScriptedModel([
            Reply("", [call("read_file", "1", path="nope.py")]),
            lambda msgs: Reply("", [call("finish", "2", summary="nothing to do")])
            if msgs[-1]["content"].startswith("error") else Reply("", [call("finish", "x", summary="BAD")]),
        ])
        res = run_task(LoopWorker(model), self.repo, Task("t", "x"))
        self.assertEqual(res.summary, "nothing to do")
        self.assertIsNone(res.commit)                       # nothing changed, nothing committed

    def test_talking_without_tools_gives_up_after_nudges(self):
        model = ScriptedModel([Reply("I think it is fine")] * 5)
        res = run_task(LoopWorker(model, max_nudges=2), self.repo, Task("t", "x"))
        self.assertEqual(res.status, "gave_up")
        self.assertEqual(len(model.calls), 3)
        self.assertIn("Use the tools", model.calls[1][-1]["content"])

    def test_step_limit_and_model_error_leave_the_branch_alone(self):
        loop = [Reply("", [call("write_file", str(i), path="a.txt", content=str(i))]) for i in range(10)]
        res = run_task(LoopWorker(ScriptedModel(loop), max_steps=3), self.repo, Task("a", "x"))
        self.assertEqual(res.status, "step_limit")
        res = run_task(LoopWorker(ScriptedModel([Reply("", [call("write_file", "1", path="b.txt", content="x")])])),
                       self.repo, Task("b", "x"))
        self.assertEqual(res.status, "error")
        self.assertIn("exhausted", res.error)
        for t in ("a", "b"):
            self.assertEqual(self.repo.refs.get(f"ws/{t}"), self.main0)

    def test_run_task_resumes_an_existing_branch(self):
        self.repo.fork("r")
        res = run_task(LoopWorker(ScriptedModel([Reply("", [call("write_file", "1", path="n", content="1")]),
                                                  Reply("", [call("finish", "2", summary="s")])])),
                       self.repo, Task("r", "x"))
        self.assertTrue(res.ok)

    def test_parallel_workers_on_disjoint_files_both_land(self):
        def worker(path):
            return LoopWorker(ScriptedModel([Reply("", [call("write_file", "1", path=path, content=path)]),
                                             Reply("", [call("finish", "2", summary=path)])]))
        results = {}

        def go(name):
            results[name] = run_task(worker(f"{name}.txt"), self.repo, Task(name, "x"))
        threads = [threading.Thread(target=go, args=(n,)) for n in ("p", "q", "r")]
        [t.start() for t in threads]
        [t.join() for t in threads]
        for n in ("p", "q", "r"):
            self.assertTrue(results[n].ok)
            self.assertTrue(self.repo.submit(self.repo.resume(n)).landed)
        self.assertTrue({"p.txt", "q.txt", "r.txt"} <= set(self.repo.main_files()))

    def test_over_http_with_a_fake_ollama(self):
        srv = FakeServer([completion("", [wire_call(1, "write_file", {"path": "h.txt", "content": "yo"})]),
                          completion("", [wire_call(2, "finish", {"summary": "wrote h"})])])
        self.addCleanup(srv.close)
        res = run_task(LoopWorker(OpenAICompatModel("qwen", srv.url)), self.repo, Task("h", "write h"))
        self.assertTrue(res.ok)
        self.assertEqual(self.repo.files_at(res.commit)["h.txt"], b"yo")
        roles = [m["role"] for m in srv.requests[1][1]["messages"]]
        self.assertEqual(roles, ["system", "user", "assistant", "tool"])


class CliWorkerTests(unittest.TestCase):
    def setUp(self):
        self.repo = new_repo()
        self.main0 = self.repo.head()

    def py(self, code, **kw):
        return CliWorker([sys.executable, "-c", code], **kw)

    def test_changes_made_by_the_command_are_pushed(self):
        code = ("import sys,pathlib;p=pathlib.Path('app.py');p.write_text(p.read_text().replace('a - b','a + b'));"
                "pathlib.Path('new.txt').write_text(sys.argv[1] if len(sys.argv)>1 else 'n');print('fixed it')")
        res = run_task(self.py(code), self.repo, Task("c1", "fix add"))
        self.assertTrue(res.ok, res)
        self.assertEqual(res.touched, ("app.py", "new.txt"))
        files = self.repo.files_at(res.commit)
        self.assertIn(b"a + b", files["app.py"])
        self.assertEqual(self.repo.head(), self.main0)
        self.assertEqual(self.repo.store.get_commit(res.commit).message, "c1: fixed it")

    def test_prompt_substitution_and_stdin(self):
        w = CliWorker([sys.executable, "-c", "import sys,pathlib;pathlib.Path('p.txt').write_text(sys.argv[1])",
                       "{prompt}"])
        res = run_task(w, self.repo, Task("c2", "say hi", ("a.py",)))
        self.assertIn("say hi", self.repo.files_at(res.commit)["p.txt"].decode())
        self.assertIn("a.py", self.repo.files_at(res.commit)["p.txt"].decode())
        res = run_task(self.py("import sys,pathlib;pathlib.Path('s.txt').write_text(sys.stdin.read())"),
                       self.repo, Task("c3", "via stdin"))
        self.assertIn("via stdin", self.repo.files_at(res.commit)["s.txt"].decode())

    def test_failure_timeout_and_missing_command_leave_the_branch_alone(self):
        cases = {"f": self.py("import pathlib,sys;pathlib.Path('x').write_text('1');sys.exit(3)"),
                 "t": self.py("import time;time.sleep(30)", timeout=0.5),
                 "m": CliWorker(["/no/such/agent"])}
        for name, w in cases.items():
            res = run_task(w, self.repo, Task(name, "x"))
            self.assertEqual(res.status, "error", name)
            self.assertEqual(self.repo.refs.get(f"ws/{name}"), self.main0, name)
        self.assertIn("exit status 3", run_task(cases["f"], self.repo, Task("f2", "x")).error)

    def test_no_changes_means_no_commit(self):
        res = run_task(self.py("print('looked, nothing to do')"), self.repo, Task("n", "x"))
        self.assertTrue(res.ok)
        self.assertIsNone(res.commit)

    def test_cli_worker_and_loop_worker_merge_cleanly_together(self):
        a = run_task(self.py("import pathlib;pathlib.Path('util.py').write_text('X = 2\\n')"), self.repo, Task("ca", "x"))
        b = run_task(LoopWorker(ScriptedModel([Reply("", [call("write_file", "1", path="docs/readme.md", content="bye\n")]),
                                               Reply("", [call("finish", "2", summary="doc")])])),
                     self.repo, Task("lb", "x"))
        self.assertTrue(a.ok and b.ok)
        for n in ("ca", "lb"):
            self.assertTrue(self.repo.submit(self.repo.resume(n)).landed)
        files = self.repo.main_files()
        self.assertEqual((files["util.py"], files["docs/readme.md"]), (b"X = 2\n", b"bye\n"))


class RunTaskCommandTests(unittest.TestCase):
    def test_run_task_command_with_submit(self):
        import argparse
        import contextlib
        import io
        from cmws_ws import cli
        repo = new_repo()
        srv = FakeServer([completion("", [wire_call(1, "write_file", {"path": "z.txt", "content": "z"})]),
                          completion("", [wire_call(2, "finish", {"summary": "wrote z"})])])
        self.addCleanup(srv.close)
        args = argparse.Namespace(agent="z", description="write z", paths=["z.txt"], model="qwen",
                                  llm_url=srv.url, max_steps=5, argv=None, submit=True)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli._run_task(args, repo)
        self.assertEqual(code, 0, out.getvalue())
        self.assertIn("done: wrote z", out.getvalue())
        self.assertEqual(repo.main_files()["z.txt"], b"z")


if __name__ == "__main__":
    unittest.main()
