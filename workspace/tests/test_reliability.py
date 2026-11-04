"""Reliability: task claims, numbered tool calls with checkpoints, resumable workers and the orchestrator."""

import json
import random
import threading
import time
import unittest

from cmws_ws.agents import (LoopWorker, MergeQueue, ModelError, Orchestrator, Reply, ScriptedModel, Session, Task,
                            TaskGraph, WorkerResult, run_status)
from cmws_ws.agents.llm import call
from cmws_ws.agents.planner import PlanError, StaticPlanner
from cmws_ws.agents.scheduler import Scheduler
from cmws_ws.agents.session import KEEP_RESULTS, session_key
from cmws_ws import kvdoc
from cmws_ws.claims import BUSY, COMPLETED, GRANTED, TaskClaims
from cmws_ws.kv import MemoryKV
from cmws_ws.leases import LeaseTable
from cmws_ws.repo import Repository


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


class Crash(BaseException):
    """Stands for the process dying: not an Exception, so nothing in the code under test may catch it."""


# ---- claims -----------------------------------------------------------------------------------------

class ClaimTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.kv = MemoryKV()
        self.claims = TaskClaims(self.kv, ttl=100, clock=self.clock)

    def test_one_owner_at_a_time(self):
        a = self.claims.claim("t", "A")
        self.assertEqual((a.status, a.epoch), (GRANTED, 1))
        b = self.claims.claim("t", "B")
        self.assertEqual((b.status, b.holder), (BUSY, "A"))
        again = self.claims.claim("t", "A")                  # asking again keeps the claim and its epoch
        self.assertEqual((again.status, again.epoch), (GRANTED, 1))

    def test_takeover_after_expiry_fences_the_old_owner(self):
        self.claims.claim("t", "A")
        self.clock.t += 101
        b = self.claims.claim("t", "B")
        self.assertEqual((b.status, b.epoch), (GRANTED, 2))
        self.assertFalse(self.claims.renew("t", "A", 1))
        self.assertFalse(self.claims.complete("t", "A", 1, {"status": "done"}))
        self.assertTrue(self.claims.renew("t", "B", 2))
        self.assertTrue(self.claims.complete("t", "B", 2, {"status": "done"}))

    def test_claim_exactly_at_expiry_is_free(self):
        self.claims.claim("t", "A")
        self.clock.t += 100                                    # expires == now: no longer live
        self.assertEqual(self.claims.claim("t", "B").status, GRANTED)

    def test_renew_after_expiry_works_while_nobody_took_over(self):
        self.claims.claim("t", "A")
        self.clock.t += 500
        self.assertTrue(self.claims.renew("t", "A", 1))
        self.assertEqual(self.claims.claim("t", "B").status, BUSY)

    def test_same_owner_after_its_own_expiry_gets_a_new_epoch(self):
        self.claims.claim("t", "A")
        self.clock.t += 500
        self.assertEqual(self.claims.claim("t", "A").epoch, 2)

    def test_completed_is_final(self):
        e = self.claims.claim("t", "A").epoch
        self.assertTrue(self.claims.complete("t", "A", e, {"status": "done", "commit": "c1"}))
        r = self.claims.claim("t", "B")
        self.assertEqual((r.status, r.outcome["commit"], r.holder), (COMPLETED, "c1", "A"))
        self.clock.t += 10_000
        self.assertEqual(self.claims.claim("t", "B").status, COMPLETED)
        self.assertFalse(self.claims.renew("t", "A", e))
        self.assertFalse(self.claims.complete("t", "A", e, {"status": "failed"}))
        self.assertFalse(self.claims.release("t", "A", e))
        self.assertEqual(self.claims.snapshot()["t"]["outcome"]["status"], "done")

    def test_release_frees_the_task_but_keeps_epochs_rising(self):
        e = self.claims.claim("t", "A").epoch
        self.assertFalse(self.claims.release("t", "B", e))
        self.assertFalse(self.claims.release("t", "A", e + 1))
        self.assertTrue(self.claims.release("t", "A", e))
        self.assertFalse(self.claims.renew("t", "A", e))       # a released claim cannot be renewed
        b = self.claims.claim("t", "B")
        self.assertEqual((b.status, b.epoch), (GRANTED, 2))

    def test_claims_are_per_task_and_per_prefix(self):
        self.assertEqual(self.claims.claim("t", "A").status, GRANTED)
        self.assertEqual(self.claims.claim("u", "B").status, GRANTED)
        other = TaskClaims(self.kv, ttl=100, clock=self.clock, prefix="runs/x/")
        self.assertEqual(other.claim("t", "B").status, GRANTED)

    def test_explicit_ttl(self):
        self.claims.claim("t", "A", ttl=5)
        self.clock.t += 6
        self.assertEqual(self.claims.claim("t", "B").status, GRANTED)

    def test_racing_claims_grant_exactly_one(self):
        for round_ in range(20):
            kv = MemoryKV()
            granted, barrier = [], threading.Barrier(8)

            def go(i):
                barrier.wait()
                if TaskClaims(kv, ttl=100, clock=self.clock).claim("t", f"o{i}").status == GRANTED:
                    granted.append(i)
            ts = [threading.Thread(target=go, args=(i,)) for i in range(8)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            self.assertEqual(len(granted), 1, round_)

    def test_against_a_reference_model(self):
        rng = random.Random(7)
        for _ in range(40):
            kv, clock = MemoryKV(), Clock()
            claims = TaskClaims(kv, ttl=10, clock=clock)
            ref = {}                                           # task -> dict(state, owner, epoch, exp)
            held = {}                                          # (task, owner) -> epoch last granted
            for _ in range(60):
                op = rng.choice(["claim", "claim", "renew", "complete", "release", "tick"])
                task, owner = rng.choice("ab"), rng.choice(["X", "Y", "Z"])
                if op == "tick":
                    clock.t += rng.choice([1, 5, 11, 30])
                    continue
                e = ref.get(task)
                if op == "claim":
                    r = claims.claim(task, owner)
                    if e is None:
                        exp = (GRANTED, 1)
                        ref[task] = dict(state="claimed", owner=owner, epoch=1, exp=clock.t + 10)
                    elif e["state"] == "completed":
                        exp = (COMPLETED, e["epoch"])
                    elif e["state"] == "claimed" and e["exp"] > clock.t and e["owner"] != owner:
                        exp = (BUSY, e["epoch"])
                    elif e["state"] == "claimed" and e["exp"] > clock.t:
                        e["exp"] = clock.t + 10
                        exp = (GRANTED, e["epoch"])
                    else:
                        e.update(state="claimed", owner=owner, epoch=e["epoch"] + 1, exp=clock.t + 10)
                        exp = (GRANTED, e["epoch"])
                    self.assertEqual((r.status, r.epoch), exp)
                    if r.status == GRANTED:
                        held[(task, owner)] = r.epoch
                    continue
                ep = held.get((task, owner), 0)
                mine = e is not None and e["state"] == "claimed" and e["owner"] == owner and e["epoch"] == ep
                if op == "renew":
                    self.assertEqual(claims.renew(task, owner, ep), mine)
                    if mine:
                        e["exp"] = clock.t + 10
                elif op == "complete":
                    self.assertEqual(claims.complete(task, owner, ep, {"status": "done"}), mine)
                    if mine:
                        e["state"] = "completed"
                else:
                    self.assertEqual(claims.release(task, owner, ep), mine)
                    if mine:
                        e["state"] = "free"


class CountingKV(MemoryKV):
    def __init__(self):
        super().__init__()
        self.writes = 0
        self.before_cas = None

    def cas(self, key, expected, value):
        if self.before_cas:
            hook, self.before_cas = self.before_cas, None
            hook()
        self.writes += 1
        return super().cas(key, expected, value)

    def put_if_absent(self, key, value):
        self.writes += 1
        return super().put_if_absent(key, value)


class KvDocTests(unittest.TestCase):
    def test_update_creates_from_a_default_and_returns_the_result(self):
        kv, default = CountingKV(), {"n": 0}

        def bump(doc):
            doc["n"] += 1
            return doc["n"]
        self.assertEqual(kvdoc.update(kv, "k", bump, default), 1)
        self.assertEqual(kvdoc.update(kv, "k", bump, default), 2)
        self.assertEqual(default, {"n": 0})                       # the default is never edited
        self.assertEqual(kvdoc.load(kv, "k", None), {"n": 2})
        self.assertEqual(kvdoc.load(kv, "missing", "dflt"), "dflt")

    def test_a_change_that_changes_nothing_writes_nothing(self):
        kv = CountingKV()
        kvdoc.update(kv, "k", lambda d: d.update(a=1), {})
        kv.writes = 0
        self.assertEqual(kvdoc.update(kv, "k", lambda d: "same", {}), "same")
        self.assertEqual(kv.writes, 0)
        kv2 = CountingKV()
        kvdoc.update(kv2, "k", lambda d: None, {})                  # but a missing key is created
        self.assertEqual(kv2.get("k"), "{}")

    def test_a_lost_race_reruns_the_change_on_the_newer_document(self):
        kv, seen = CountingKV(), []
        kvdoc.update(kv, "k", lambda d: d.update(items=[]), {})
        kv.before_cas = lambda: kv.put("k", json.dumps({"items": ["theirs"]}))

        def add(doc):
            seen.append(list(doc["items"]))
            doc["items"].append("ours")
        kvdoc.update(kv, "k", add, {})
        self.assertEqual(seen, [[], ["theirs"]])
        self.assertEqual(kvdoc.load(kv, "k", None), {"items": ["theirs", "ours"]})

    def test_giving_up_when_too_contended(self):
        kv = CountingKV()
        kvdoc.update(kv, "k", lambda d: d.update(a=1), {})
        kv.cas = lambda key, expected, value: (False, "x")
        with self.assertRaises(RuntimeError):
            kvdoc.update(kv, "k", lambda d: d.update(a=2), {}, max_attempts=3)


# ---- sessions ---------------------------------------------------------------------------------------

def fresh_repo(files=None):
    return Repository.init(MemoryKV(), files or {"a.txt": "one\ntwo\nthree\n"})


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.repo = fresh_repo()
        self.repo.fork("w")
        self.s = Session.open(self.repo, "w")

    def test_a_replayed_call_does_not_run_twice(self):
        first = self.s.call(1, "replace_in_file", dict(path="a.txt", old="two", new="2"))
        self.assertTrue(first.ok)
        again = self.s.call(1, "replace_in_file", dict(path="a.txt", old="two", new="2"))   # would fail if run
        self.assertEqual(again, first)
        self.assertEqual(self.s.ws.read_text("a.txt"), "one\n2\nthree\n")
        self.assertEqual(self.s.seq, 1)

    def test_numbers_must_follow_on(self):
        gap = self.s.call(2, "list_files", {})
        self.assertEqual((gap.ok, gap.text), (False, "error: expected call 1, got 2"))   # skipping ahead is refused
        self.assertEqual(self.s.seq, 0)
        self.assertTrue(self.s.call(1, "list_files", {}).ok)
        self.assertFalse(self.s.call(3, "list_files", {}).ok)
        self.assertFalse(self.s.call(0, "list_files", {}).ok)           # never numbered
        self.assertEqual(self.s.next_seq(), 2)

    def test_a_failed_call_still_uses_its_number(self):
        bad = self.s.call(1, "replace_in_file", dict(path="a.txt", old="zzz", new="y"))
        self.assertFalse(bad.ok)
        self.assertEqual(self.s.next_seq(), 2)
        self.assertEqual(self.s.call(1, "list_files", {}), bad)         # and its result is what is replayed

    def test_concurrent_duplicates_run_once(self):
        outs, barrier = [], threading.Barrier(8)

        def go():
            barrier.wait()
            outs.append(self.s.call(1, "replace_in_file", dict(path="a.txt", old="one", new="1")))
        ts = [threading.Thread(target=go) for _ in range(8)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len({o.text for o in outs}), 1)
        self.assertTrue(all(o.ok for o in outs))
        self.assertEqual(self.s.ws.read_text("a.txt"), "1\ntwo\nthree\n")

    def test_checkpoint_survives_a_crash_and_later_calls_are_lost(self):
        self.s.call(1, "write_file", dict(path="b.txt", content="B"))
        head = self.s.checkpoint({"note": "after b"})
        self.assertNotEqual(head, self.repo.head())
        self.s.call(2, "write_file", dict(path="c.txt", content="C"))   # never checkpointed
        again = Session.open(self.repo, "w")                            # the process restarted
        self.assertEqual((again.seq, again.state), (1, {"note": "after b"}))
        self.assertEqual(again.ws.read_text("b.txt"), "B")
        self.assertFalse(again.ws.exists("c.txt"))
        self.assertTrue(again.call(2, "write_file", dict(path="c.txt", content="C2")).ok)   # same number, redone
        self.assertEqual(again.ws.read_text("c.txt"), "C2")

    def test_checkpointed_results_can_still_be_replayed(self):
        first = self.s.call(1, "list_files", {})
        self.s.checkpoint()
        again = Session.open(self.repo, "w")
        self.assertEqual(again.call(1, "write_file", dict(path="x", content="x")), first)
        self.assertFalse(again.ws.exists("x"))                          # replayed, not run

    def test_old_results_are_dropped_but_still_refused(self):
        for i in range(1, KEEP_RESULTS + 6):
            self.s.call(i, "list_files", {})
        self.s.checkpoint()
        again = Session.open(self.repo, "w")
        out = again.call(1, "write_file", dict(path="x", content="x"))
        self.assertFalse(out.ok)
        self.assertFalse(again.ws.exists("x"))
        n = KEEP_RESULTS + 5
        self.assertEqual(again.call(n, "write_file", dict(path="x", content="x")).text, self.s.call(n, "list_files", {}).text)
        self.assertEqual(len(json.loads(self.repo.kv.get(session_key("w")))["results"]), KEEP_RESULTS)

    def test_a_commit_made_after_the_last_record_is_undone(self):
        self.s.call(1, "write_file", dict(path="b.txt", content="B"))
        self.s.checkpoint()
        self.s.call(2, "write_file", dict(path="c.txt", content="C"))
        self.s.ws.commit("crash right after committing, before recording")
        self.assertEqual(Session.open(self.repo, "w").ws.exists("c.txt"), False)
        s = Session.open(self.repo, "w")
        self.assertEqual(s.seq, 1)
        self.assertEqual(s.ws.head, json.loads(self.repo.kv.get(session_key("w")))["head"])
        self.assertEqual(self.repo.refs.get("ws/w"), s.ws.head)

    def test_a_record_the_branch_no_longer_matches_is_ignored(self):
        self.s.call(1, "write_file", dict(path="b.txt", content="B"))
        self.s.checkpoint({"x": 1})
        doc = json.loads(self.repo.kv.get(session_key("w")))
        doc["head"] = self.repo.head()                                  # not between the fork base and the tip
        doc["head"] = "0" * 64
        self.repo.kv.put(session_key("w"), json.dumps(doc))
        s = Session.open(self.repo, "w")
        self.assertEqual((s.seq, s.state, s.done), (0, None, None))
        self.assertEqual(s.ws.read_text("b.txt"), "B")                  # the branch itself is untouched

    def test_checkpoint_counts_and_empty_checkpoint(self):
        self.assertEqual(self.s.since_checkpoint, 0)
        self.s.call(1, "list_files", {})
        self.s.call(2, "list_files", {})
        self.assertEqual(self.s.since_checkpoint, 2)
        head = self.s.checkpoint()
        self.assertEqual(self.s.since_checkpoint, 0)
        self.assertEqual(head, self.s.ws.fork_base)                     # nothing to commit: the tip stays
        self.assertEqual(Session.open(self.repo, "w").seq, 2)
        self.assertIsNotNone(self.repo.kv.get("session/w"))

    def test_the_default_checkpoint_message_names_the_agent_and_call(self):
        self.s.call(1, "write_file", dict(path="b.txt", content="B"))
        head = self.s.checkpoint()
        self.assertEqual(self.repo.store.get_commit(head).message, "checkpoint w at call 1")

    def test_open_without_a_workspace_fails(self):
        from cmws_ws.repo import NoSuchWorkspace
        with self.assertRaises(NoSuchWorkspace):
            Session.open(self.repo, "nobody")


# ---- durable loop worker ----------------------------------------------------------------------------

def steps(*calls):
    return [Reply("", [c]) if not isinstance(c, Reply) else c for c in calls]


TASK = Task("t1", "edit things", ("a.txt", "b.txt", "c.txt"))


class CrashingModel(ScriptedModel):
    """Plays its script, then dies as a process would."""

    def chat(self, messages, tools):
        if not self._script:
            raise Crash()
        return super().chat(messages, tools)


class DurableWorkerTests(unittest.TestCase):
    def script(self):
        return steps(call("write_file", "1", path="b.txt", content="B"),
                     call("replace_in_file", "2", path="a.txt", old="one", new="1"),
                     call("write_file", "3", path="c.txt", content="C"),
                     call("finish", "4", summary="edited three files"))

    def test_resumes_after_a_crash_without_repeating_anything(self):
        repo = fresh_repo()
        repo.fork("t1")
        first = CrashingModel(self.script()[:3])               # dies when asked for step 4
        with self.assertRaises(Crash):
            LoopWorker(first, checkpoint_every=2).run(TASK, repo, "t1")
        # steps 1-2 were checkpointed; step 3's write was only in memory
        second = ScriptedModel(self.script()[2:])
        res = LoopWorker(second, checkpoint_every=2).run(TASK, repo, "t1")
        self.assertEqual(res.status, "done")
        self.assertEqual(res.steps, 4)
        self.assertEqual(sorted(res.touched), ["a.txt", "b.txt", "c.txt"])
        ws = repo.resume("t1")
        self.assertEqual((ws.read_text("a.txt"), ws.read_text("b.txt"), ws.read_text("c.txt")),
                         ("1\ntwo\nthree\n", "B", "C"))
        history = second.calls[0]                              # the second run saw the first run's conversation
        self.assertEqual([m["role"] for m in history].count("assistant"), 2)
        self.assertEqual(res.commit, ws.head)

    def test_a_finished_task_is_not_run_again(self):
        repo = fresh_repo()
        repo.fork("t1")
        res = LoopWorker(ScriptedModel(self.script()), checkpoint_every=2).run(TASK, repo, "t1")
        self.assertTrue(res.ok)
        silent = ScriptedModel([])
        again = LoopWorker(silent, checkpoint_every=2).run(TASK, repo, "t1")
        self.assertEqual(silent.calls, [])
        self.assertEqual((again.status, again.commit, again.touched, again.steps, again.summary),
                         (res.status, res.commit, res.touched, res.steps, res.summary))
        self.assertEqual(again.prompt_tokens, res.prompt_tokens)

    def test_a_model_error_keeps_the_progress(self):
        repo = fresh_repo()
        repo.fork("t1")
        flaky = ScriptedModel(self.script()[:2])               # then "script exhausted" -> ModelError
        res = LoopWorker(flaky, checkpoint_every=1).run(TASK, repo, "t1")
        self.assertEqual((res.status, res.steps), ("error", 2))
        res2 = LoopWorker(ScriptedModel(self.script()[2:]), checkpoint_every=1).run(TASK, repo, "t1")
        self.assertEqual((res2.status, res2.steps), ("done", 4))
        self.assertEqual(sorted(res2.touched), ["a.txt", "b.txt", "c.txt"])

    def test_a_resumed_run_keeps_counting_steps_from_where_it_was(self):
        repo = fresh_repo()
        repo.fork("t1")
        LoopWorker(ScriptedModel(self.script()[:2]), checkpoint_every=1).run(TASK, repo, "t1")   # 2 steps, then an error
        broken = LoopWorker(ScriptedModel([]), checkpoint_every=1).run(TASK, repo, "t1")
        self.assertEqual((broken.status, broken.steps), ("error", 2))        # no step was made, 2 were already done
        capped = ScriptedModel(self.script()[2:])
        limited = LoopWorker(capped, max_steps=2, checkpoint_every=1).run(TASK, repo, "t1")
        self.assertEqual((limited.status, limited.steps, capped.calls), ("step_limit", 2, []))
        exact = LoopWorker(ScriptedModel(self.script()[2:]), max_steps=4, checkpoint_every=1).run(TASK, repo, "t1")
        self.assertEqual((exact.status, exact.steps), ("done", 4))              # the last allowed step is still taken

    def test_tokens_add_up_across_a_restart(self):
        from cmws_ws.agents import Usage
        repo = fresh_repo()
        repo.fork("t1")
        s = self.script()
        for r in s:
            object.__setattr__(r, "usage", Usage(10, 1))
        with self.assertRaises(Crash):
            LoopWorker(CrashingModel(s[:3]), checkpoint_every=2).run(TASK, repo, "t1")
        res = LoopWorker(ScriptedModel(s[2:]), checkpoint_every=2).run(TASK, repo, "t1")
        self.assertEqual((res.prompt_tokens, res.completion_tokens), (10 * 2 + 10 * 2, 2 + 2))

    def test_without_checkpoints_nothing_is_recorded(self):
        repo = fresh_repo()
        repo.fork("t1")
        res = LoopWorker(ScriptedModel(self.script())).run(TASK, repo, "t1")
        self.assertTrue(res.ok)
        self.assertIsNone(repo.kv.get(session_key("t1")))

    def test_a_task_that_changes_nothing_has_no_commit(self):
        repo = fresh_repo()
        repo.fork("t1")
        res = LoopWorker(ScriptedModel(steps(call("finish", "1", summary="nothing to do"))),
                         checkpoint_every=3).run(TASK, repo, "t1")
        self.assertEqual((res.status, res.commit), ("done", None))

    def test_a_giving_up_worker_is_checkpointed_too(self):
        repo = fresh_repo()
        repo.fork("t1")
        model = ScriptedModel([Reply("I cannot")] * 5)
        res = LoopWorker(model, checkpoint_every=3, max_nudges=1).run(TASK, repo, "t1")
        self.assertEqual(res.status, "gave_up")
        doc = json.loads(repo.kv.get(session_key("t1")))
        self.assertIsNone(doc["done"])
        self.assertEqual(doc["state"]["step"], 2)


# ---- orchestrator -----------------------------------------------------------------------------------

class FnWorker:
    name = "fn"

    def __init__(self, fn=None, fail=()):
        self.fn, self.fail, self.runs = fn, set(fail), []
        self.lock = threading.Lock()

    def run(self, task, repo, agent_id):
        with self.lock:
            self.runs.append(task.id)
        if task.id in self.fail:
            return WorkerResult("error", error="boom")
        ws = repo.resume(agent_id)
        if self.fn:
            self.fn(task, ws)
        else:
            ws.write(f"{task.id}.txt", task.id)
        return WorkerResult("done", f"did {task.id}", ws.commit(task.id), tuple(sorted(ws.status())))


def graph(*specs):
    return TaskGraph([Task(i, f"do {i}", tuple(p), "", tuple(d)) for i, p, d in specs])


G3 = (("a", ["a.txt"], []), ("b", ["b.txt"], []), ("c", ["c.txt"], ["a", "b"]))


class OrchestratorTests(unittest.TestCase):
    def make(self, worker, kv=None, owner="o1", clock=None, **kw):
        repo = Repository.init(kv or MemoryKV(), {"README": "x"})
        kw.setdefault("patience", 0.4)
        return repo, Orchestrator(repo, worker, owner=owner, clock=clock or time.time, poll=0.01, **kw)

    def test_plan_run_and_record(self):
        w = FnWorker()
        repo, orch = self.make(w)
        out = orch.run("r1", graph=graph(*G3))
        self.assertTrue(out.ok)
        self.assertFalse(out.resumed)
        self.assertEqual(sorted(out.landed), ["a", "b", "c"])
        self.assertEqual(sorted(repo.main_files()), ["README", "a.txt", "b.txt", "c.txt"])
        st = run_status(repo.kv, "r1")
        self.assertEqual([t["id"] for t in st["plan"]["tasks"]], ["a", "b", "c"])
        self.assertEqual({t: c["state"] for t, c in st["claims"].items()}, {"a": "completed", "b": "completed", "c": "completed"})
        self.assertEqual(st["result"]["landed"], ["a", "b", "c"])
        self.assertEqual(st["result"]["main"], repo.head())
        self.assertIsNotNone(repo.kv.get("runs/r1/plan"))
        self.assertIsNotNone(repo.kv.get("runs/r1/result"))
        self.assertEqual(out.main, repo.head())

    def test_the_planner_is_asked_once_and_the_plan_is_stored(self):
        asked = []

        class P:
            def plan(self, goal, files):
                asked.append((goal, list(files)))
                return graph(*G3)
        w = FnWorker()
        repo, orch = self.make(w, planner=P())
        orch.run("r1", goal="build it")
        self.assertEqual(asked, [("build it", ["README"])])
        again = Orchestrator(repo, FnWorker(), planner=P(), owner="o2", poll=0.01).run("r1", goal="build it")
        self.assertEqual(len(asked), 1)
        self.assertTrue(again.resumed)
        self.assertTrue(again.ok)

    def test_a_stored_plan_beats_a_different_graph(self):
        repo, orch = self.make(FnWorker())
        orch.run("r1", graph=graph(*G3))
        w2 = FnWorker()
        out = Orchestrator(repo, w2, owner="o2", poll=0.01).run("r1", graph=graph(("zzz", ["z"], [])))
        self.assertEqual(sorted(out.report.outcomes), ["a", "b", "c"])
        self.assertEqual(w2.runs, [])

    def test_no_plan_and_no_planner_is_an_error(self):
        repo, orch = self.make(FnWorker())
        with self.assertRaises(ValueError):
            orch.run("r1", goal="g")
        with self.assertRaises(ValueError):
            orch.run("r1")
        self.assertIsNone(repo.kv.get("runs/r1/plan"))

    def test_a_planner_failure_stores_nothing(self):
        class Bad:
            def plan(self, goal, files):
                raise PlanError("no")
        repo, orch = self.make(FnWorker(), planner=Bad())
        with self.assertRaises(PlanError):
            orch.run("r1", goal="g")
        self.assertIsNone(repo.kv.get("runs/r1/plan"))

    def test_two_orchestrators_planning_at_once_agree(self):
        kv = MemoryKV()
        Repository.init(kv, {"README": "x"})
        plans = []
        barrier = threading.Barrier(2)

        class P:
            def __init__(self, n):
                self.n = n

            def plan(self, goal, files):
                barrier.wait()
                return graph((f"only{self.n}", [f"f{self.n}"], []))

        def go(n):
            o = Orchestrator(Repository(kv), FnWorker(), planner=P(n), owner=f"o{n}", poll=0.01)
            plans.append(o.plan("r", "g", None)[0])
        ts = [threading.Thread(target=go, args=(n,)) for n in (1, 2)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len({tuple(t.id for t in g.tasks) for g in plans}), 1)

    def test_failed_tasks_are_remembered_and_their_dependents_skipped(self):
        w = FnWorker(fail={"a"})
        repo, orch = self.make(w)
        out = orch.run("r1", graph=graph(*G3))
        self.assertFalse(out.ok)
        self.assertEqual((out.failed, out.landed, out.skipped), (["a"], ["b"], ["c"]))
        w2 = FnWorker()
        out2 = Orchestrator(repo, w2, owner="o2", poll=0.01).run("r1")
        self.assertEqual(w2.runs, [])                       # a rerun does not retry a recorded failure
        self.assertEqual((out2.failed, out2.landed, out2.skipped), (["a"], ["b"], ["c"]))
        self.assertEqual(out2.report.outcomes["a"].status, "failed")
        self.assertEqual(out2.report.outcomes["a"].reason, "boom")
        st = run_status(repo.kv, "r1")
        self.assertEqual(st["result"]["failed"], ["a"])

    def test_restart_after_a_crash_repeats_no_finished_task(self):
        clock = Clock()
        calls = []

        def fn(task, ws):
            if task.id == "b" and not calls:
                calls.append(1)
                ws.write("b.txt", "half")
                raise Crash()
            ws.write(f"{task.id}.txt", task.id)
        w = FnWorker(fn)
        repo, orch = self.make(w, clock=clock, claim_ttl=100, parallel=1)
        with self.assertRaises(Crash):
            orch.run("r1", graph=graph(*G3))
        snap = TaskClaims(repo.kv, prefix="runs/r1/").snapshot()
        self.assertEqual((snap["a"]["state"], snap["b"]["state"]), ("completed", "claimed"))
        self.assertNotIn("c", snap)
        # the dead owner's claim is still live: a second orchestrator must not touch task b
        w2 = FnWorker(fn)
        busy = Orchestrator(repo, w2, owner="o2", clock=clock, claim_ttl=100, poll=0.01, patience=0.2).run("r1")
        self.assertEqual(w2.runs, [])
        self.assertEqual(busy.report.outcomes["b"].status, "skipped")
        self.assertEqual(busy.report.outcomes["a"].status, "done")
        # once the claim has expired it is taken over and the run completes
        clock.t += 101
        w3 = FnWorker(fn)
        done = Orchestrator(repo, w3, owner="o3", clock=clock, claim_ttl=100, poll=0.01, patience=0.5).run("r1")
        self.assertEqual(sorted(w3.runs), ["b", "c"])
        self.assertTrue(done.ok, done.report.outcomes)
        self.assertEqual(sorted(repo.main_files()), ["README", "a.txt", "b.txt", "c.txt"])
        self.assertEqual(repo.main_files()["b.txt"], b"b")
        self.assertEqual(TaskClaims(repo.kv, prefix="runs/r1/").snapshot()["b"]["epoch"], 2)

    def test_a_durable_worker_resumes_inside_the_orchestrator(self):
        clock = Clock()
        repo = Repository.init(MemoryKV(), {"README": "x"})
        script = [call("write_file", "1", path="f.txt", content="1\n"),
                  call("write_file", "2", path="g.txt", content="2\n"),
                  call("write_file", "3", path="h.txt", content="3\n"),
                  call("finish", "4", summary="three files")]
        crashing = LoopWorker(CrashingModel(steps(*script[:3])), checkpoint_every=1)
        g = graph(("t", [], []))
        o1 = Orchestrator(repo, crashing, owner="o1", clock=clock, claim_ttl=50, poll=0.01)
        with self.assertRaises(Crash):
            o1.run("r", graph=g)
        model = ScriptedModel(steps(*script[3:]))
        clock.t += 51
        o2 = Orchestrator(repo, LoopWorker(model, checkpoint_every=1), owner="o2", clock=clock, claim_ttl=50,
                          poll=0.01, patience=1)
        out = o2.run("r")
        self.assertTrue(out.ok, out.report.outcomes)
        self.assertEqual(len(model.calls), 1)                 # only the last step was asked of the model
        self.assertEqual(sorted(repo.main_files()), ["README", "f.txt", "g.txt", "h.txt"])

    def test_a_landed_task_whose_completion_was_lost_is_not_landed_twice(self):
        clock = Clock()
        repo = Repository.init(MemoryKV(), {"README": "x"})
        script = steps(call("write_file", "1", path="f.txt", content="1\n"), call("finish", "2", summary="ok"))
        o1 = Orchestrator(repo, LoopWorker(ScriptedModel(script), checkpoint_every=1), owner="o1", clock=clock,
                          claim_ttl=50, poll=0.01)
        self.assertTrue(o1.run("r", graph=graph(("t", ["f.txt"], []))).ok)
        main = repo.head()
        doc = TaskClaims(repo.kv, prefix="runs/r/").snapshot()          # forget that it completed
        doc["t"] = {"state": "claimed", "owner": "o1", "epoch": 1, "expires": clock.t + 1}
        repo.kv.put("runs/r/claims", json.dumps(doc))
        clock.t += 100
        silent = ScriptedModel([])
        out = Orchestrator(repo, LoopWorker(silent, checkpoint_every=1), owner="o2", clock=clock, claim_ttl=50,
                           poll=0.01).run("r")
        self.assertTrue(out.ok)
        self.assertEqual(silent.calls, [])
        self.assertEqual(repo.head(), main)                              # nothing was landed a second time
        self.assertEqual(out.report.outcomes["t"].landed.status, "noop")

    def test_two_orchestrators_share_one_run(self):
        kv = MemoryKV()
        Repository.init(kv, {"README": "x"})
        g = graph(*[(f"t{i}", [f"f{i}.txt"], []) for i in range(10)])
        counts, lock = {}, threading.Lock()

        def fn(task, ws):
            with lock:
                counts[task.id] = counts.get(task.id, 0) + 1
            time.sleep(0.01)
            ws.write(f"{task.id}.txt", task.id)
        results = []

        def go(name):
            repo = Repository(kv)
            o = Orchestrator(repo, FnWorker(fn), owner=name, poll=0.005, parallel=3, patience=5)
            results.append(o.run("r", graph=g))
        ts = [threading.Thread(target=go, args=(n,)) for n in ("p", "q")]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(counts, {f"t{i}": 1 for i in range(10)})
        self.assertTrue(all(r.ok for r in results))
        self.assertEqual(len(Repository(kv).main_files()), 11)
        owners = {c["owner"] for c in TaskClaims(kv, prefix="runs/r/").snapshot().values()}
        self.assertEqual(owners, {"p", "q"})                 # both did some of the work

    def test_it_lands_through_a_validating_queue(self):
        from cmws_ws.agents import FnValidator, Verdict
        queue = None
        w = FnWorker()
        repo = Repository.init(MemoryKV(), {"README": "x"})
        queue = MergeQueue(repo, validator=FnValidator(lambda files, changed: Verdict("b.txt" not in files, "no b")))
        o = Orchestrator(repo, w, queue=queue, owner="o", poll=0.01)
        out = o.run("r", graph=graph(*G3))
        self.assertEqual(out.landed, ["a"])
        self.assertEqual(sorted(out.failed), ["b"])
        self.assertEqual(out.skipped, ["c"])
        self.assertNotIn("b.txt", repo.main_files())


# ---- scheduler claims -------------------------------------------------------------------------------

class SchedulerClaimTests(unittest.TestCase):
    def test_heartbeat_keeps_a_slow_task_alive(self):
        repo = Repository.init(MemoryKV(), {"README": "x"})
        claims = TaskClaims(repo.kv, ttl=0.25, clock=time.time, prefix="p/")
        stolen = []
        stop = threading.Event()

        def thief():
            while not stop.is_set():
                if claims.claim("slow", "thief").status == GRANTED:
                    stolen.append(1)
                    return
                time.sleep(0.01)

        def fn(task, ws):
            time.sleep(0.8)                                   # more than three times the claim's lifetime
            ws.write("s.txt", "s")
        sch = Scheduler(repo, LeaseTable(repo.kv), FnWorker(fn), claims=claims, owner="me", poll=0.01,
                        heartbeat=0.05)
        result = []
        runner = threading.Thread(target=lambda: result.append(sch.run(graph(("slow", ["s.txt"], [])))))
        runner.start()
        while "slow" not in claims.snapshot():
            time.sleep(0.005)
        t = threading.Thread(target=thief)
        t.start()
        runner.join(10)
        stop.set()
        t.join()
        self.assertEqual(stolen, [])
        self.assertTrue(result[0].ok)

    def test_a_task_whose_claim_was_taken_over_does_not_land(self):
        repo = Repository.init(MemoryKV(), {"README": "x"})
        clock = Clock()
        claims = TaskClaims(repo.kv, ttl=10, clock=clock, prefix="p/")
        gate, started = threading.Event(), threading.Event()

        def fn(task, ws):
            ws.write("s.txt", "s")
            started.set()
            gate.wait(5)
        landed = []

        def land(repo_, task, result):
            landed.append(task.id)
        sch = Scheduler(repo, LeaseTable(repo.kv, clock=clock), FnWorker(fn), claims=claims, owner="old", poll=0.01,
                        heartbeat=0.01, land=land, lease_ttl=5, claim_ttl=10)
        result = []
        t = threading.Thread(target=lambda: result.append(sch.run(graph(("s", ["s.txt"], [])))))
        t.start()
        self.assertTrue(started.wait(5))
        clock.t += 11                                         # the old owner stalled; its claim ran out
        self.assertEqual(claims.claim("s", "new").epoch, 2)
        time.sleep(0.2)                                       # let the heartbeat notice
        gate.set()
        t.join(5)
        out = result[0].outcomes["s"]
        self.assertEqual((out.status, out.reason), ("failed", "claim lost to another owner"))
        self.assertEqual(landed, [])
        snap = claims.snapshot()["s"]
        self.assertEqual((snap["state"], snap["owner"]), ("claimed", "new"))   # the old owner did not complete it
        self.assertIn("s", LeaseTable(repo.kv, clock=clock).holders() or {"s": 1})

    def _slow_run(self, heartbeat, lease_ttl=5, claim_ttl=1000, sched_clock=None):
        """Start a scheduler on one blocked task; returns (everything the test needs, finish())."""
        repo = Repository.init(MemoryKV(), {"README": "x"})
        clock = Clock()
        calls = {"renew": 0}

        class Counting(TaskClaims):
            def renew(self, *a, **k):
                calls["renew"] += 1
                return super().renew(*a, **k)
        claims = Counting(repo.kv, ttl=claim_ttl, clock=clock, prefix="p/")
        leases = LeaseTable(repo.kv, clock=clock)
        gate, started = threading.Event(), threading.Event()

        def fn(task, ws):
            ws.write("s.txt", "s")
            started.set()
            gate.wait(10)
        kw = {"clock": sched_clock} if sched_clock else {}
        sch = Scheduler(repo, leases, FnWorker(fn), claims=claims, owner="me", poll=0.005, heartbeat=heartbeat,
                        lease_ttl=lease_ttl, claim_ttl=claim_ttl, **kw)
        result = []
        t = threading.Thread(target=lambda: result.append(sch.run(graph(("s", ["s.txt"], [])))))
        t.start()
        self.assertTrue(started.wait(5))

        def finish():
            gate.set()
            t.join(10)
            return result[0]
        return clock, claims, leases, calls, finish

    def test_the_heartbeat_waits_for_its_interval(self):
        sched = Clock(0.0)
        clock, claims, leases, calls, finish = self._slow_run(heartbeat=10, sched_clock=sched)
        time.sleep(0.15)
        self.assertEqual(calls["renew"], 0)                    # 0 s have passed on the scheduler's clock
        sched.t = 9.9
        time.sleep(0.15)
        self.assertEqual(calls["renew"], 0)
        sched.t = 10.0
        time.sleep(0.15)
        self.assertEqual(calls["renew"], 1)                    # once per interval, not once per loop
        time.sleep(0.15)
        self.assertEqual(calls["renew"], 1)
        sched.t = 20.0
        time.sleep(0.15)
        self.assertEqual(calls["renew"], 2)
        self.assertTrue(finish().ok)

    def test_the_heartbeat_extends_a_live_lease_and_renews_an_expired_one(self):
        clock, claims, leases, calls, finish = self._slow_run(heartbeat=0.01)
        time.sleep(0.1)
        first = leases.holders()["s"].expires
        clock.t += 3                                           # still live: expiry moves to now + ttl
        time.sleep(0.15)
        self.assertEqual(leases.holders()["s"].expires, first + 3)
        clock.t += 100                                         # ran out entirely: it is taken again
        time.sleep(0.15)
        self.assertIn("s", leases.holders())
        self.assertEqual(leases.holders()["s"].expires, clock.t + 5)
        self.assertTrue(finish().ok)
        self.assertEqual(leases.holders(), {})                 # and released at the end

    def test_a_default_heartbeat_is_a_third_of_the_claim_lifetime(self):
        repo = Repository.init(MemoryKV(), {"README": "x"})
        claims = TaskClaims(repo.kv, ttl=90)
        self.assertEqual(Scheduler(repo, LeaseTable(repo.kv), FnWorker(), claims=claims).heartbeat, 30)
        self.assertEqual(Scheduler(repo, LeaseTable(repo.kv), FnWorker(), claims=claims, claim_ttl=30).heartbeat, 10)
        self.assertEqual(Scheduler(repo, LeaseTable(repo.kv), FnWorker(), claims=claims, heartbeat=4).heartbeat, 4)
        self.assertIsNone(Scheduler(repo, LeaseTable(repo.kv), FnWorker()).heartbeat)

    def test_a_task_that_lost_its_claim_does_not_release_the_new_owners_lease(self):
        clock, claims, leases, calls, finish = self._slow_run(heartbeat=0.01)
        clock.t += 2000                                        # claim and lease both ran out
        self.assertEqual(claims.claim("s", "new").epoch, 2)
        leases.acquire("s", ["s.txt"])                         # the new owner's lease (same agent key)
        time.sleep(0.15)
        out = finish().outcomes["s"]
        self.assertEqual(out.reason, "claim lost to another owner")
        self.assertIn("s", leases.holders())                   # still there
        self.assertEqual(claims.snapshot()["s"]["owner"], "new")

    def test_a_task_blocked_by_a_foreign_lease_gives_its_claim_back(self):
        repo = Repository.init(MemoryKV(), {"README": "x"})
        leases = LeaseTable(repo.kv)
        leases.acquire("foreign", ["s.txt"])
        claims = TaskClaims(repo.kv, ttl=100, prefix="p/")
        sch = Scheduler(repo, leases, FnWorker(), claims=claims, owner="me", poll=0.01, patience=0.1)
        report = sch.run(graph(("s", ["s.txt"], [])))
        self.assertEqual(report.outcomes["s"].status, "skipped")
        self.assertEqual(claims.claim("s", "other").status, GRANTED)    # it was released, not left to expire

    def test_without_claims_nothing_is_recorded(self):
        repo = Repository.init(MemoryKV(), {"README": "x"})
        sch = Scheduler(repo, LeaseTable(repo.kv), FnWorker(), poll=0.01)
        self.assertTrue(sch.run(graph(("s", ["s.txt"], []))).ok)
        self.assertNotIn("p/claims", repo.kv.keys())
        self.assertFalse([k for k in repo.kv.keys() if k.endswith("claims")])


if __name__ == "__main__":
    unittest.main()
