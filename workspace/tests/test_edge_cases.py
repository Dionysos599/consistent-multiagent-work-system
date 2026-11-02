"""Edge cases that the mutation sweep showed were not pinned down by the other tests."""

import argparse
import contextlib
import io
import json
import sys
import threading
import unittest
from unittest import mock

from cmws_ws.agents import (CliWorker, LoopWorker, ModelError, OpenAICompatModel, Reply, ScriptedModel, Task,
                            Toolbox, WorkerResult, run_task)
from cmws_ws.agents.graph import TaskGraph
from cmws_ws.agents.llm import _arguments, call, parse_reply
from cmws_ws.agents.planner import LLMPlanner
from cmws_ws.agents.scheduler import Scheduler
from cmws_ws.agents.tools import SPECS
from cmws_ws.agents.worker import task_prompt
from cmws_ws.kv import MemoryKV
from cmws_ws.leases import LOG, LeaseTable
from cmws_ws.repo import Repository
from cmws_ws.sync import PushResult

from tests.test_agents import FakeServer, completion, new_repo
from tests.test_scheduling import Clock, FnWorker, T


class RacyKV(MemoryKV):
    """Runs a hook right before the next CAS of a key, to model a competitor winning the race."""

    def __init__(self):
        super().__init__()
        self.hooks = {}

    def cas(self, key, expected, value):
        hook = self.hooks.pop(key, None)
        if hook:
            hook()
        return super().cas(key, expected, value)


class LeaseRaceTests(unittest.TestCase):
    def test_a_lost_cas_is_retried_against_the_new_state(self):
        kv = RacyKV()
        a, b = LeaseTable(kv), LeaseTable(kv)
        a.acquire("seed", ["z"])
        kv.hooks["leases"] = lambda: b.acquire("rival", ["x"])      # lands between a's read and a's write
        got = a.acquire("a", ["x/y"])
        self.assertFalse(got.granted)
        self.assertEqual(set(got.blocked_by), {"rival"})
        self.assertEqual(sorted(a.holders()), ["rival", "seed"])

    def test_a_lost_cas_on_renew_and_release_keeps_the_rivals_claim(self):
        kv = RacyKV()
        t = LeaseTable(kv)
        t.acquire("a", ["x"])
        kv.hooks["leases"] = lambda: LeaseTable(kv).acquire("rival", ["y"])
        self.assertTrue(t.renew("a"))
        kv.hooks["leases"] = lambda: LeaseTable(kv).acquire("rival2", ["w"])
        t.release("a")
        self.assertEqual(sorted(t.holders()), ["rival", "rival2"])

    def test_concurrent_log_entries_are_both_kept(self):
        kv = RacyKV()
        t = LeaseTable(kv)
        t.audit("a", ["p"], ["q"])
        kv.hooks[LOG] = lambda: LeaseTable(kv).audit("rival", ["r"], ["s"])
        t.audit("b", ["u"], ["v"])
        self.assertEqual([e["agent"] for e in t.violations()], ["a", "rival", "b"])

    def test_too_much_contention_is_reported_not_swallowed(self):
        class Hostile(MemoryKV):
            def cas(self, key, expected, value):
                return False, "other"

            def put_if_absent(self, key, value):
                return False, "other"
        t = LeaseTable(Hostile(), max_attempts=3)
        t.kv.put("leases", "{}")
        with self.assertRaises(RuntimeError):
            t.acquire("a", ["x"])
        with self.assertRaises(RuntimeError):
            t.audit("a", ["p"], ["q"])


class ToolEdgeTests(unittest.TestCase):
    def setUp(self):
        self.repo = new_repo()
        self.ws = self.repo.fork("t")
        self.tb = Toolbox(self.ws, max_output=50)

    def test_specs_match_the_tools_exactly(self):
        names = [s["function"]["name"] for s in SPECS]
        self.assertEqual(sorted(names), sorted(self.tb._tools))
        for s in SPECS:
            self.assertEqual(s["type"], "function")
            params = s["function"]["parameters"]
            self.assertTrue(s["function"]["description"])
            self.assertTrue(set(params["required"]) <= set(params["properties"]))
            json.dumps(s)

    def test_error_kinds_have_distinct_messages_and_flags(self):
        r = self.tb.call("nope", {})
        self.assertFalse(r.ok)
        self.assertIn("unknown tool", r.text)
        r = self.tb.call("read_file", {"_raw": "{bad"})
        self.assertFalse(r.ok)
        self.assertIn("not valid JSON", r.text)
        r = self.tb.call("read_file", {"path": 5})
        self.assertFalse(r.ok)
        self.assertIn("must be a string", r.text)
        r = self.tb.call("read_file", ["path"])
        self.assertFalse(r.ok)
        self.assertIn("JSON object", r.text)

    def test_empty_listing_empty_file_and_truncation_boundaries(self):
        self.tb.call("delete_file", {"path": "app.py"})
        self.tb.call("delete_file", {"path": "util.py"})
        self.tb.call("delete_file", {"path": "docs"})
        r = self.tb.call("list_files", {})
        self.assertEqual((r.ok, r.text), (True, "(empty)"))
        self.ws.write("e.txt", "")
        self.assertEqual(self.tb.call("read_file", {"path": "e.txt"}).text, "(empty file)")
        self.ws.write("exact.txt", "x" * 50)
        self.assertNotIn("cut off", self.tb.call("read_file", {"path": "exact.txt"}).text)
        self.ws.write("over.txt", "x" * 51)
        self.assertIn("cut off after 50 of 51", self.tb.call("read_file", {"path": "over.txt"}).text)
        self.assertNotIn("cut off", self.tb.call("read_file", {"path": "e.txt"}).text)

    def test_replace_with_empty_old_is_rejected_even_in_an_empty_file(self):
        self.ws.write("e.txt", "")
        r = self.tb.call("replace_in_file", {"path": "e.txt", "old": "", "new": "x"})
        self.assertFalse(r.ok)
        self.assertEqual(self.ws.read_text("e.txt"), "")

    def test_written_tracks_deletes_and_moves_on_both_ends(self):
        self.tb.call("delete_file", {"path": "util.py"})
        self.tb.call("move_file", {"src": "app.py", "dst": "lib/app.py"})
        self.assertEqual(self.tb.written, {"util.py", "app.py", "lib/app.py"})


class LlmEdgeTests(unittest.TestCase):
    def test_arguments_shapes(self):
        self.assertEqual(_arguments(None), {})
        self.assertEqual(_arguments(""), {})
        self.assertEqual(_arguments("[1]"), {"_raw": "[1]"})
        self.assertEqual(_arguments({"a": 1}), {"a": 1})

    def test_missing_ids_and_names_get_defaults(self):
        r = parse_reply({"choices": [{"message": {"tool_calls": [{"function": {}}, {"function": {"name": "x"}}]}}]})
        self.assertEqual([(c.id, c.name) for c in r.tool_calls], [("call_0", ""), ("call_1", "x")])

    def test_retry_count_and_exponential_backoff(self):
        srv = FakeServer([(503, {})] * 5)
        self.addCleanup(srv.close)
        sleeps = []
        m = OpenAICompatModel("m", srv.url, retries=2, backoff=1.5, sleep=sleeps.append)
        with self.assertRaises(ModelError):
            m.chat([], [])
        self.assertEqual(len(srv.requests), 3)
        self.assertEqual(sleeps, [1.5, 3.0])

    def test_rate_limit_is_retried_and_other_4xx_is_not(self):
        srv = FakeServer([(429, {}), completion("ok")])
        self.addCleanup(srv.close)
        self.assertEqual(OpenAICompatModel("m", srv.url, sleep=lambda s: None).chat([], []).content, "ok")

    def test_optional_request_fields_are_left_out_when_unused(self):
        srv = FakeServer([completion("a"), completion("b")])
        self.addCleanup(srv.close)
        m = OpenAICompatModel("m", srv.url)
        m.chat([{"role": "user", "content": "x"}], [])
        _, body, headers = srv.requests[0]
        self.assertNotIn("tools", body)
        self.assertNotIn("Authorization", headers)
        self.assertIs(body["stream"], False)

    def test_scripted_model_records_conversations_by_copy(self):
        m = ScriptedModel([Reply("a"), Reply("b")])
        msgs = [{"role": "user", "content": "1"}]
        m.chat(msgs, [])
        msgs[0]["content"] = "changed"
        m.chat(msgs, [])
        self.assertEqual(m.calls[0][0]["content"], "1")


class PromptAndWorkerEdgeTests(unittest.TestCase):
    def test_prompt_sections_appear_only_when_there_is_something_to_say(self):
        bare = task_prompt(Task("t", "do x"))
        self.assertNotIn("expected to change", bare)
        self.assertNotIn("Background", bare)
        full = task_prompt(Task("t", "do x", ("a.py", "b/"), "ctx"))
        self.assertIn("a.py, b/", full)
        self.assertIn("Background:\nctx", full)
        self.assertNotIn("Background", task_prompt(Task("t", "do x", (), "   ")))

    def test_step_limit_is_exact(self):
        repo = new_repo()
        model = ScriptedModel([Reply("", [call("list_files", str(i))]) for i in range(10)])
        res = run_task(LoopWorker(model, max_steps=3), repo, Task("s", "x"))
        self.assertEqual((res.status, res.steps, len(model.calls)), ("step_limit", 3, 3))
        model = ScriptedModel([Reply("", [call("list_files", "1")]), Reply("", [call("list_files", "2")]),
                               Reply("", [call("finish", "3", summary="ok")])])
        self.assertTrue(run_task(LoopWorker(model, max_steps=3), repo, Task("s2", "x")).ok)

    def test_transcript_shapes_and_unicode_arguments(self):
        repo = new_repo()
        model = ScriptedModel([Reply("thinking", [call("write_file", "1", path="u.txt", content="héllo 中文")]),
                               Reply("", [call("finish", "2", summary="done")])])
        res = run_task(LoopWorker(model), repo, Task("u", "x"))
        assistant = [m for m in res.transcript if m["role"] == "assistant"]
        self.assertIn("héllo 中文", assistant[0]["tool_calls"][0]["function"]["arguments"])
        self.assertEqual(json.loads(assistant[0]["tool_calls"][0]["function"]["arguments"])["path"], "u.txt")
        talk = run_task(LoopWorker(ScriptedModel([Reply("just words")] * 3), max_nudges=0), repo, Task("v", "x"))
        self.assertNotIn("tool_calls", [m for m in talk.transcript if m["role"] == "assistant"][0])

    def test_cli_worker_reports_a_blocked_push_as_an_error(self):
        repo = new_repo()
        w = CliWorker([sys.executable, "-c", "import pathlib;pathlib.Path('n.txt').write_text('1')"])
        for status in ("blocked", "conflict", "contended"):
            with mock.patch("cmws_ws.agents.worker.Checkout.push",
                            return_value=PushResult(status, conflicts={"n.txt": "content"})):
                res = run_task(w, repo, Task(f"p-{status}", "x"))
            self.assertEqual(res.status, "error")
            self.assertIn(status, res.error)
            self.assertEqual(res.touched, ("n.txt",))

    def test_cli_failure_message_carries_stderr_tail(self):
        repo = new_repo()
        w = CliWorker([sys.executable, "-c", "import sys;sys.stderr.write('bad thing');sys.exit(2)"])
        res = run_task(w, repo, Task("e", "x"))
        self.assertIn("exit status 2: bad thing", res.error)


class PlannerEdgeTests(unittest.TestCase):
    GOOD = '[{"id":"a","description":"x","paths":["p"]}]'

    def test_long_file_lists_are_truncated_with_a_count(self):
        model = ScriptedModel([Reply(self.GOOD)])
        LLMPlanner(model, max_files=3).plan("g", [f"f{i}" for i in range(10)])
        prompt = model.calls[0][0]["content"]
        self.assertIn("f2", prompt)
        self.assertNotIn("f3\n", prompt)
        self.assertIn("... and 7 more", prompt)
        model = ScriptedModel([Reply(self.GOOD)])
        LLMPlanner(model, max_files=3).plan("g", ["f0", "f1", "f2"])
        self.assertNotIn("more", model.calls[0][0]["content"])

    def test_the_model_sees_its_own_earlier_answer_when_asked_to_retry(self):
        model = ScriptedModel([Reply("first try, no json"), Reply(self.GOOD)])
        LLMPlanner(model).plan("g", [])
        second = model.calls[1]
        self.assertEqual([m["role"] for m in second], ["user", "assistant", "user"])
        self.assertEqual(second[1]["content"], "first try, no json")


class GraphEdgeTests(unittest.TestCase):
    def test_descendants_are_transitive(self):
        g = TaskGraph([T("a"), T("b", deps=["a"]), T("c", deps=["b"]), T("d", deps=["c"]), T("e")])
        self.assertEqual(g.descendants("a"), {"b", "c", "d"})
        self.assertEqual(g.descendants("e"), set())

    def test_a_task_depending_on_itself_is_named_in_the_error(self):
        from cmws_ws.agents.graph import InvalidGraph
        with self.assertRaisesRegex(InvalidGraph, "itself"):
            TaskGraph([T("a", deps=["a"])])


class SchedulerEdgeTests(unittest.TestCase):
    def setUp(self):
        self.kv = MemoryKV()
        self.repo = Repository.init(self.kv, {"a.txt": b"A\n"})

    def test_skipping_works_when_a_dependent_is_declared_before_its_dependency(self):
        def fn(task, ws):
            if task.id == "bad":
                raise RuntimeError("x")
            ws.write(task.paths[0], "ok")
        g = TaskGraph([T("grand", ["g.txt"], ["child"]), T("child", ["c.txt"], ["bad"]), T("bad", ["b.txt"])])
        rep = Scheduler(self.repo, LeaseTable(self.kv), FnWorker(fn), poll=0.002).run(g)
        self.assertEqual(sorted(rep.by_status("skipped")), ["child", "grand"])

    def test_failure_reason_falls_back_to_the_status(self):
        class GaveUp:
            name = "g"

            def run(self, task, repo, agent_id):
                return WorkerResult("gave_up")
        rep = Scheduler(self.repo, LeaseTable(self.kv), GaveUp(), poll=0.002).run(TaskGraph([T("t", ["x"])]))
        self.assertEqual((rep.outcomes["t"].status, rep.outcomes["t"].reason), ("failed", "gave_up"))

    def test_only_as_many_leases_are_taken_as_tasks_may_run(self):
        seen = []
        leases = LeaseTable(self.kv, ttl=60)

        def fn(task, ws):
            seen.append(len(leases.holders()))
            ws.write(task.paths[0], "x")
        g = TaskGraph([T(f"t{i}", [f"f{i}.txt"]) for i in range(5)])
        rep = Scheduler(self.repo, leases, FnWorker(fn, delay=0.01), parallel=2, poll=0.002).run(g)
        self.assertTrue(rep.ok)
        self.assertLessEqual(max(seen), 2)
        self.assertEqual(rep.max_concurrency, 2)

    def test_waiting_for_a_foreign_lease_sleeps_for_the_poll_interval(self):
        leases = LeaseTable(self.kv, ttl=3600)
        leases.acquire("outsider", ["x"])
        sleeps, clock = [], Clock()

        def fake_sleep(s):
            sleeps.append(s)
            clock.t += s
        rep = Scheduler(self.repo, leases, FnWorker(lambda t, ws: None), poll=0.5, patience=2.0,
                        sleep=fake_sleep, clock=clock).run(TaskGraph([T("t", ["x"])]))
        self.assertEqual(rep.by_status("skipped"), ["t"])
        self.assertEqual(sleeps, [0.5] * 5)                       # 0.0, .5, 1, 1.5, 2.0 -> over patience at 2.5

    def test_run_with_no_tasks_and_a_report_for_every_task(self):
        rep = Scheduler(self.repo, LeaseTable(self.kv), FnWorker(lambda t, ws: None)).run(TaskGraph([]))
        self.assertEqual((rep.outcomes, rep.ok), ({}, True))

    def test_a_worker_that_changes_nothing_is_done_and_lands_as_noop(self):
        rep = Scheduler(self.repo, LeaseTable(self.kv), FnWorker(lambda t, ws: None), poll=0.002).run(
            TaskGraph([T("t", ["x"])]))
        self.assertEqual(rep.outcomes["t"].status, "done")
        self.assertEqual(rep.outcomes["t"].landed.status, "noop")


if __name__ == "__main__":
    unittest.main()
