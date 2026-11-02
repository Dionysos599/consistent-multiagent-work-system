"""Harder tests for the lease table, the scheduler, the tool layer and the parsers.

Model-based and randomized: each test states an invariant and tries to break it with many seeds.
"""

import json
import random
import string
import threading
import time
import unittest

from cmws_ws.agents import (LoopWorker, ModelError, Reply, ScriptedModel, Task, Toolbox, WorkerResult, run_task)
from cmws_ws.agents.graph import InvalidGraph, TaskGraph
from cmws_ws.agents.llm import ToolCall, Usage, parse_reply
from cmws_ws.agents.planner import LLMPlanner, PlanError, parse_tasks
from cmws_ws.agents.scheduler import Scheduler, land_directly
from cmws_ws.agents.tools import ToolResult
from cmws_ws.kv import MemoryKV
from cmws_ws.leases import LeaseTable
from cmws_ws.repo import Repository, SubmitResult
from cmws_ws.scope import inside, make_scope, overlaps

from tests.test_scheduling import Clock, FnWorker, T


class LeaseModelTests(unittest.TestCase):
    """Random operations against a plain-dict reference, one step at a time."""

    PATHS = ["a", "a/x", "a/y", "b", "b/z", "c", "c/d/e", "d"]

    def test_matches_the_reference_model(self):
        for seed in range(40):
            rng = random.Random(seed)
            clock = Clock()
            table = LeaseTable(MemoryKV(), ttl=20, clock=clock)
            model = {}                                    # agent -> (scope, expires)
            for step in range(120):
                now = clock.t
                model = {a: v for a, v in model.items() if v[1] > now}
                op = rng.choice(["acquire", "acquire", "renew", "release", "tick", "audit"])
                agent = f"w{rng.randrange(5)}"
                if op == "acquire":
                    paths = rng.sample(self.PATHS, rng.randint(0, 3))
                    ttl = rng.choice([None, 5, 30])
                    scope = make_scope(paths)
                    blockers = {a for a, (sc, _) in model.items() if a != agent and overlaps(scope, sc)}
                    got = table.acquire(agent, paths, ttl)
                    self.assertEqual(got.granted, not blockers, (seed, step, agent, paths))
                    self.assertEqual(set(got.blocked_by), blockers)
                    if got.granted:
                        model[agent] = (scope, now + (ttl if ttl is not None else 20))
                elif op == "renew":
                    ok = table.renew(agent)
                    self.assertEqual(ok, agent in model, (seed, step))
                    if ok:
                        model[agent] = (model[agent][0], now + 20)
                elif op == "release":
                    table.release(agent)
                    model.pop(agent, None)
                elif op == "audit":
                    touched = rng.sample(self.PATHS, rng.randint(0, 4))
                    declared = rng.sample(self.PATHS, rng.randint(0, 2))
                    rep = table.audit(agent, touched, declared, record=False)
                    dscope = make_scope(declared)
                    self.assertEqual(rep.out_of_scope, sorted(p for p in set(touched) if not inside(dscope, p)))
                    for path, other in rep.collisions.items():
                        self.assertNotEqual(other, agent)
                        self.assertTrue(inside(model[other][0], path))
                    for p in rep.out_of_scope:
                        expected = any(a != agent and inside(sc, p) for a, (sc, _) in model.items())
                        self.assertEqual(p in rep.collisions, expected, (seed, step, p))
                else:
                    clock.t += rng.choice([1, 7, 15, 25])
                now = clock.t
                live = table.holders()
                model = {a: v for a, v in model.items() if v[1] > now}
                self.assertEqual({a: (l.paths, l.expires) for a, l in live.items()}, model, (seed, step, op))
                agents = list(live.values())
                for i, x in enumerate(agents):
                    for y in agents[i + 1:]:
                        self.assertFalse(overlaps(x.paths, y.paths), (seed, step))

    def test_threads_never_hold_overlapping_leases_at_once(self):
        table = LeaseTable(MemoryKV(), ttl=60)
        guard = threading.Lock()
        inuse = {}
        violations = []
        stop = time.time() + 1.5

        def worker(i):
            rng = random.Random(i)
            n = 0
            while time.time() < stop:
                paths = rng.sample(LeaseModelTests.PATHS, rng.randint(1, 2))
                if not table.acquire(f"w{i}", paths).granted:
                    continue
                scope = make_scope(paths)
                with guard:
                    for other, osc in inuse.items():
                        if overlaps(scope, osc):
                            violations.append((i, other))
                    inuse[i] = scope
                n += 1
                time.sleep(0.0005)
                with guard:
                    del inuse[i]
                table.release(f"w{i}")
            counts[i] = n
        counts = {}
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertEqual(violations, [])
        self.assertGreater(sum(counts.values()), 50)             # it really did contend
        self.assertEqual(table.holders(), {})

    def test_violation_log_is_bounded_and_ordered(self):
        table = LeaseTable(MemoryKV())
        for i in range(250):
            table.audit("a", [f"x{i}"], ["y"])
        log = table.violations()
        self.assertEqual(len(log), 200)
        self.assertEqual(log[-1]["out_of_scope"], ["x249"])
        self.assertEqual(log[0]["out_of_scope"], ["x50"])

    def test_prefix_isolates_tables_on_one_store(self):
        kv = MemoryKV()
        a, b = LeaseTable(kv, prefix="a/"), LeaseTable(kv, prefix="b/")
        self.assertTrue(a.acquire("x", ["p"]).granted)
        self.assertTrue(b.acquire("y", ["p"]).granted)


class SchedulerChaosTests(unittest.TestCase):
    FILES = [f"d{d}/f{f}.txt" for d in range(3) for f in range(3)]

    def test_chaos_keeps_every_landed_edit_and_only_those(self):
        for seed in range(25):
            rng = random.Random(1000 + seed)
            kv = MemoryKV()
            repo = Repository.init(kv, {p: b"0\n" for p in self.FILES})
            tasks = []
            for i in range(rng.randint(5, 10)):
                scope = rng.choice([[rng.choice(self.FILES)], [f"d{rng.randrange(3)}"], rng.sample(self.FILES, 2), []])
                deps = [t.id for t in tasks if rng.random() < 0.2]
                tasks.append(T(f"t{i}", scope, deps))
            crash = {t.id: rng.random() < 0.2 for t in tasks}
            notok = {t.id: rng.random() < 0.1 for t in tasks}
            reject = {t.id: rng.random() < 0.1 for t in tasks}
            slow = {t.id: rng.choice([0, 0.002, 0.01]) for t in tasks}

            def edit(task, ws):
                if crash[task.id]:
                    ws.write("d0/f0.txt", "garbage")             # dirty overlay, then die
                    raise RuntimeError("crash")
                for p in sorted(f for f in self.FILES if inside(make_scope(task.paths), f)):
                    ws.write(p, ws.read_text(p) + task.id + "\n")

            class Worker(FnWorker):
                def run(self, task, repo_, agent_id):
                    res = super().run(task, repo_, agent_id)
                    time.sleep(slow[task.id])
                    if notok[task.id]:
                        return WorkerResult("gave_up", error="nope", touched=res.touched)
                    return res
            worker = Worker(edit)

            def land(repo_, task, result):
                if reject[task.id]:
                    return SubmitResult("conflict")
                return land_directly(repo_, task, result)

            leases = LeaseTable(kv, ttl=0.02)                      # leases keep expiring mid-task
            outsider_stop = threading.Event()

            def outsider():
                r = random.Random(seed)
                while not outsider_stop.is_set():
                    if leases.acquire("outsider", [r.choice(self.FILES)], 0.01).granted:
                        time.sleep(0.003)
                        leases.release("outsider")
                    time.sleep(0.001)
            th = threading.Thread(target=outsider)
            th.start()
            try:
                rep = Scheduler(repo, leases, worker, parallel=4, land=land, poll=0.002, patience=10,
                                lease_ttl=0.02).run(TaskGraph(tasks))
            finally:
                outsider_stop.set()
                th.join()
            ctx = (seed, {k: (v.status, v.reason) for k, v in rep.outcomes.items()})
            self.assertEqual(sorted(rep.outcomes), sorted(t.id for t in tasks), ctx)
            self.assertEqual(worker.overlap_seen, [], ctx)
            # status matches what we injected
            for t in tasks:
                o = rep.outcomes[t.id]
                dep_bad = any(rep.outcomes[d].status != "done" for d in t.depends_on)
                if o.status == "skipped":
                    self.assertTrue(dep_bad, ctx)
                elif crash[t.id]:
                    self.assertEqual(o.status, "failed", ctx)
                elif notok[t.id]:
                    self.assertEqual(o.status, "failed", ctx)
                elif reject[t.id]:
                    self.assertEqual(o.status, "rejected", ctx)
                else:
                    self.assertEqual(o.status, "done", ctx)
            # exactly the done tasks' edits are in main, each exactly once, nothing else
            final = repo.main_files()
            for p in self.FILES:
                lines = final[p].decode().split()
                self.assertEqual(lines[0], "0", ctx)
                landed = sorted(t.id for t in tasks
                                if rep.outcomes[t.id].status == "done" and inside(make_scope(t.paths), p))
                self.assertEqual(sorted(lines[1:]), landed, (p, ctx))
            self.assertEqual(leases.holders().keys() - {"outsider"}, set(), ctx)

    def test_two_tasks_with_wrong_scopes_that_edit_the_same_line_conflict_deterministically(self):
        repo = Repository.init(MemoryKV(), {"shared.txt": b"base\n"})
        both_forked = threading.Barrier(2, timeout=10)

        def fn(task, ws):
            ws.read_text("shared.txt")
            both_forked.wait()                                   # both are now working from the same main
            ws.write("shared.txt", task.id + "\n")
        g = TaskGraph([T("p", ["p.txt"]), T("q", ["q.txt"])])        # declared disjoint, actually not
        rep = Scheduler(repo, LeaseTable(repo.kv), FnWorker(fn), parallel=2, poll=0.002).run(g)
        self.assertEqual(sorted(o.status for o in rep.outcomes.values()), ["done", "rejected"])
        winner = rep.by_status("done")[0]
        self.assertEqual(repo.main_files()["shared.txt"], (winner + "\n").encode())
        loser = rep.outcomes[rep.by_status("rejected")[0]]
        self.assertIn(loser.landed.status, ("conflict", "stale"))
        self.assertEqual(LeaseTable(repo.kv).holders(), {})


class FuzzTests(unittest.TestCase):
    def junk(self, rng, depth=0):
        kinds = ["str", "int", "none", "float", "bool", "list", "dict", "long", "uni", "path"]
        k = rng.choice(kinds if depth < 2 else kinds[:5] + kinds[7:])
        if k == "str":
            return "".join(rng.choice(string.printable) for _ in range(rng.randint(0, 20)))
        if k == "int":
            return rng.randint(-10**9, 10**9)
        if k == "none":
            return None
        if k == "float":
            return rng.random() * 1e6
        if k == "bool":
            return rng.random() < 0.5
        if k == "list":
            return [self.junk(rng, depth + 1) for _ in range(rng.randint(0, 3))]
        if k == "dict":
            return {str(self.junk(rng, depth + 1)): self.junk(rng, depth + 1) for _ in range(rng.randint(0, 3))}
        if k == "long":
            return "x" * rng.choice([10_000, 200_000])
        if k == "uni":
            return "".join(chr(rng.choice([0x41, 0xe9, 0x4e2d, 0x1f600, 0x0, 0xd7ff, 0x202e])) for _ in range(rng.randint(1, 6)))
        return rng.choice(["../x", "a/../b", "/abs", "a//b", ".", "..", "a\0b", "docs", "docs/readme.md", "x/y/z.txt", ""])

    def test_toolbox_never_raises_whatever_it_is_called_with(self):
        names = ["list_files", "read_file", "write_file", "replace_in_file", "delete_file", "move_file", "finish",
                 "bogus", "", "rm -rf"]
        for seed in range(30):
            rng = random.Random(seed)
            repo = Repository.init(MemoryKV(), {"docs/readme.md": b"hi\n", "a.txt": b"A", "bin": b"\xff\x00"})
            ws = repo.fork("f")
            tb = Toolbox(ws, max_output=500)
            for _ in range(150):
                args = {k: self.junk(rng) for k in rng.sample(["path", "content", "old", "new", "src", "dst",
                                                               "directory", "summary", "extra"], rng.randint(0, 4))}
                if rng.random() < 0.1:
                    args = {"_raw": self.junk(rng)}
                out = tb.call(rng.choice(names), args)
                self.assertIsInstance(out, ToolResult)
                self.assertIsInstance(out.text, str)
                if not out.ok:
                    self.assertTrue(out.text.startswith("error"), out.text)
            # the workspace is still usable and consistent after the abuse
            ws.files()
            ws.status()
            ws.commit("fuzz")
            for path in ws.files():
                self.assertEqual(ws.read(path), repo.files_at(ws.head)[path])

    def test_loop_worker_survives_a_model_that_talks_nonsense(self):
        for seed in range(30):
            rng = random.Random(seed)
            replies = []
            for _ in range(rng.randint(1, 12)):
                calls = tuple(ToolCall(str(self.junk(rng)), rng.choice(["write_file", "read_file", "finish", "x", str(self.junk(rng))]),
                                       self.junk(rng) if rng.random() < 0.3 else {k: self.junk(rng) for k in ("path", "content", "summary")})
                              for _ in range(rng.randint(0, 3)))
                replies.append(Reply(str(self.junk(rng)), calls, Usage(rng.randint(0, 9), rng.randint(0, 9))))
            repo = Repository.init(MemoryKV(), {"a.txt": b"A"})
            main0 = repo.head()
            model = ScriptedModel([r if rng.random() > 0.05 else (lambda m: (_ for _ in ()).throw(ModelError("boom")))
                                   for r in replies])
            res = run_task(LoopWorker(model, max_steps=8), repo, Task("n", "x"))
            self.assertIn(res.status, ("done", "gave_up", "step_limit", "error"), seed)
            if res.status != "done":
                self.assertEqual(repo.refs.get("ws/n"), main0, (seed, res.status))
                self.assertIsNone(res.commit)
            else:
                self.assertEqual(res.commit, repo.refs.get("ws/n") if res.commit else None)
            ids = [m["tool_call_id"] for m in res.transcript if m["role"] == "tool"]
            self.assertEqual(len(ids), sum(len(m.get("tool_calls", [])) for m in res.transcript if m["role"] == "assistant"))

    def test_planner_parsing_never_crashes_on_garbage(self):
        good = json.dumps([{"id": "a", "description": "x", "paths": ["p"], "depends_on": []},
                           {"id": "b", "description": "y", "paths": ["q"], "depends_on": ["a"]}])
        for seed in range(300):
            rng = random.Random(seed)
            text = good
            for _ in range(rng.randint(0, 5)):
                i = rng.randrange(len(text))
                text = rng.choice([text[:i] + text[i + 1:], text[:i] + rng.choice('[]{}",: x\\') + text[i:], text[:i],
                                   text + text[:i], "```json\n" + text + "\n```", "prose " + text])
            try:
                tasks = parse_tasks(text)
            except ValueError:
                continue
            for t in tasks:
                self.assertIsInstance(t, Task)
            try:
                TaskGraph(tasks)
            except ValueError:
                pass
        for text in ["", "null", "123", '"str"', "[[]]", '[{"description": null}]', "{" * 500, "[" * 5000,
                     '{"tasks": 3}', '[{"description":"x","paths":"abc"}]', '[{"description":"x","depends_on":[["a"]]}]']:
            try:
                parse_tasks(text)
            except ValueError:
                pass

    def test_planner_recovers_or_fails_cleanly_with_random_replies(self):
        for seed in range(40):
            rng = random.Random(seed)
            replies = [Reply(rng.choice(["", "[]", "nope", '[{"id":"a","description":"x","paths":["../up"]}]',
                                         '[{"id":"a b","description":"x"}]',
                                         '[{"id":"a","description":"x","depends_on":["a"]}]',
                                         '[{"id":"a","description":"x","paths":["p"]}]'])) for _ in range(4)]
            try:
                g = LLMPlanner(ScriptedModel(replies), max_attempts=4).plan("g", [])
            except PlanError:
                continue
            self.assertIsInstance(g, TaskGraph)
            self.assertEqual([t.id for t in g.tasks], ["a"])

    def test_reply_parsing_on_malformed_bodies(self):
        for seed in range(300):
            rng = random.Random(seed)
            body = self.junk(rng)
            if rng.random() < 0.7:
                body = {"choices": [{"message": self.junk(rng) if rng.random() < 0.5 else
                                     {"content": self.junk(rng), "tool_calls": self.junk(rng)}}], "usage": self.junk(rng)}
            try:
                r = parse_reply(body)
            except ModelError:
                continue
            except (TypeError, AttributeError, ValueError) as e:
                self.fail(f"seed {seed}: {type(e).__name__}: {e} on {str(body)[:120]}")
            self.assertIsInstance(r.content, str)


if __name__ == "__main__":
    unittest.main()
