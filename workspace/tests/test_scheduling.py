import random
import threading
import time
import unittest

from cmws_ws.agents import ModelError, Reply, ScriptedModel, Task, WorkerResult
from cmws_ws.agents.graph import InvalidGraph, TaskGraph
from cmws_ws.agents.planner import LLMPlanner, PlanError, StaticPlanner, parse_tasks
from cmws_ws.agents.scheduler import Scheduler
from cmws_ws.kv import MemoryKV
from cmws_ws.leases import LeaseTable
from cmws_ws.repo import Repository
from cmws_ws.scope import covers, inside, make_scope, overlaps


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class ScopeTests(unittest.TestCase):
    def test_covers_and_overlap(self):
        self.assertTrue(covers("src", "src/a.py"))
        self.assertFalse(covers("src", "srcx/a.py"))
        self.assertTrue(overlaps(make_scope(["src"]), make_scope(["src/a.py"])))
        self.assertFalse(overlaps(make_scope(["src/a.py"]), make_scope(["src/b.py"])))
        self.assertFalse(overlaps(make_scope(["src"]), make_scope(["srcx"])))

    def test_unknown_scope_means_everything(self):
        self.assertEqual(make_scope([]), ("",))
        self.assertTrue(overlaps(make_scope([]), make_scope(["a"])))
        self.assertEqual(make_scope(["a", "a/b", "c//d/"]), ("a", "c/d"))
        self.assertTrue(inside(make_scope(["a"]), "a/x/y"))
        self.assertFalse(inside(make_scope(["a"]), "b"))


class LeaseTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.kv = MemoryKV()
        self.leases = LeaseTable(self.kv, ttl=10, clock=self.clock)

    def test_overlap_denied_disjoint_granted_same_agent_refreshes(self):
        self.assertTrue(self.leases.acquire("a", ["src"]).granted)
        r = self.leases.acquire("b", ["src/x.py"])
        self.assertFalse(r.granted)
        self.assertEqual(r.blocked_by, {"a": ("src",)})
        self.assertTrue(self.leases.acquire("b", ["docs"]).granted)
        self.assertTrue(self.leases.acquire("a", ["src"]).granted)
        self.assertFalse(self.leases.acquire("c", []).granted)          # unknown scope = everything

    def test_expiry_renew_release(self):
        self.leases.acquire("a", ["x"])
        self.clock.t += 9
        self.assertTrue(self.leases.renew("a"))
        self.clock.t += 9
        self.assertFalse(self.leases.acquire("b", ["x"]).granted)       # renewed, still live
        self.clock.t += 2
        self.assertFalse(self.leases.renew("a"))                        # expired: must acquire again
        self.assertTrue(self.leases.acquire("b", ["x"]).granted)
        self.leases.release("b")
        self.assertEqual(self.leases.holders(), {})
        self.assertTrue(self.leases.acquire("a", ["x"]).granted)

    def test_exactly_one_of_many_racing_claims_wins(self):
        for round_ in range(20):
            leases = LeaseTable(MemoryKV(), clock=self.clock)
            wins = []
            barrier = threading.Barrier(8)

            def go(i):
                barrier.wait()
                if leases.acquire(f"w{i}", ["shared", f"own{i}"]).granted:
                    wins.append(i)
            ts = [threading.Thread(target=go, args=(i,)) for i in range(8)]
            [t.start() for t in ts]
            [t.join() for t in ts]
            self.assertEqual(len(wins), 1, round_)

    def test_lost_reply_does_not_lose_or_duplicate_the_claim(self):
        class LostReply(MemoryKV):
            lose = True

            def cas(self, key, expected, value):
                ok, cur = super().cas(key, expected, value)
                if ok and self.lose:
                    self.lose = False
                    return False, cur            # the write happened, the reply says it did not
                return ok, cur
        leases = LeaseTable(LostReply(), clock=self.clock)
        leases.acquire("seed", ["z"])
        self.assertTrue(leases.acquire("a", ["x"]).granted)
        self.assertEqual(sorted(leases.holders()), ["a", "seed"])

    def test_audit_flags_out_of_scope_and_collisions_and_logs_them(self):
        self.leases.acquire("a", ["src/a.py"])
        self.leases.acquire("b", ["src/b.py"])
        report = self.leases.audit("a", ["src/a.py", "src/b.py", "README.md"], ["src/a.py"])
        self.assertEqual(report.in_scope, ["src/a.py"])
        self.assertEqual(report.out_of_scope, ["README.md", "src/b.py"])
        self.assertEqual(report.collisions, {"src/b.py": "b"})
        self.assertFalse(report.clean)
        log = self.leases.violations()
        self.assertEqual(len(log), 1)
        self.assertEqual(log[0]["agent"], "a")
        self.assertTrue(self.leases.audit("b", ["src/b.py"], ["src/b.py"]).clean)
        self.assertEqual(len(self.leases.violations()), 1)


def T(i, paths=(), deps=()):
    return Task(i, f"do {i}", tuple(paths), "", tuple(deps))


class GraphTests(unittest.TestCase):
    def test_validation(self):
        for tasks in ([T("a"), T("a")], [T("a", deps=["zz"])], [T("a", deps=["a"])],
                      [T("a", deps=["b"]), T("b", deps=["a"])], [T("bad id")], [T("")]):
            with self.assertRaises(InvalidGraph, msg=tasks):
                TaskGraph(tasks)

    def test_overlap_orders_by_declaration_and_disjoint_runs_together(self):
        g = TaskGraph([T("a", ["src/x.py"]), T("b", ["src/x.py"]), T("c", ["docs"]), T("d", ["src"], ["c"])])
        self.assertEqual([t.id for t in g.ready(set(), set())], ["a", "c"])
        self.assertEqual([t.id for t in g.ready(set(), {"a"})], ["c"])
        self.assertEqual([t.id for t in g.ready({"a"}, {"a"})], ["b", "c"])
        self.assertEqual(g.waves(), [["a", "c"], ["b"], ["d"]])
        self.assertEqual(g.descendants("c"), {"d"})
        self.assertNotIn("a", [t.id for t in g.ready({"a"}, set())])      # finished means never again

    def test_unknown_scope_runs_alone(self):
        g = TaskGraph([T("a", ["x"]), T("b", ["y"]), T("any")])
        self.assertEqual(g.waves(), [["a", "b"], ["any"]])
        late = TaskGraph([T("any"), T("a", ["x"])])
        self.assertEqual(late.waves(), [["any"], ["a"]])


class PlannerTests(unittest.TestCase):
    GOOD = '```json\n[{"id":"parser","description":"fix parser","paths":["src/parser.py"]},' \
           '{"id":"tests","description":"add tests","paths":["tests"],"depends_on":["parser"]}]\n```'

    def test_parse_variants(self):
        self.assertEqual([t.id for t in parse_tasks(self.GOOD)], ["parser", "tests"])
        self.assertEqual(parse_tasks('Sure! {"tasks":[{"description":"x"}]} done')[0].id, "task-1")
        for bad in ("no json", "[]", '[{"id":"a"}]', '[{"description":"x","paths":[1]}]'):
            with self.assertRaises(ValueError, msg=bad):
                parse_tasks(bad)

    def test_retries_with_the_error_and_then_succeeds(self):
        model = ScriptedModel([Reply("I would do it like this"), Reply(
            '[{"id":"a","description":"x","depends_on":["ghost"]}]'), Reply(self.GOOD)])
        g = LLMPlanner(model).plan("goal", ["src/parser.py"])
        self.assertEqual([t.id for t in g.tasks], ["parser", "tests"])
        self.assertIn("src/parser.py", model.calls[0][0]["content"])
        self.assertIn("not usable", model.calls[1][-1]["content"])
        self.assertIn("ghost", model.calls[2][-1]["content"])

    def test_gives_up_and_reports_model_failures(self):
        with self.assertRaises(PlanError):
            LLMPlanner(ScriptedModel([Reply("nope")] * 3), max_attempts=2).plan("g", [])
        with self.assertRaises(PlanError):
            LLMPlanner(ScriptedModel([])).plan("g", [])

    def test_static_planner(self):
        self.assertEqual(StaticPlanner([T("a")]).plan("g", []).tasks[0].id, "a")


class FnWorker:
    """A worker whose behaviour is a function over the workspace; records who ran when."""
    name = "fn"

    def __init__(self, fn, delay=0.0):
        self.fn, self.delay = fn, delay
        self.lock = threading.Lock()
        self.running = {}
        self.overlap_seen = []
        self.forks = {}

    def run(self, task, repo, agent_id):
        ws = repo.resume(agent_id)
        scope = make_scope(task.paths)
        with self.lock:
            for other, osc in self.running.items():
                if overlaps(scope, osc):
                    self.overlap_seen.append((task.id, other))
            self.running[task.id] = scope
            self.forks[task.id] = dict(repo.files_at(ws.head))
        try:
            self.fn(task, ws)
            time.sleep(self.delay)
            touched = tuple(sorted(ws.status()))
            commit = ws.commit(task.id)
        finally:
            with self.lock:
                del self.running[task.id]
        return WorkerResult("done", f"{task.id} done", commit, touched)


def write(path, text):
    def fn(task, ws):
        ws.write(path, text)
    return fn


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.kv = MemoryKV()
        self.repo = Repository.init(self.kv, {"a.txt": b"A\n", "b.txt": b"B\n", "docs/d.md": b"D\n"})
        self.leases = LeaseTable(self.kv, ttl=60)

    def sched(self, worker, **kw):
        kw.setdefault("poll", 0.005)
        return Scheduler(self.repo, self.leases, worker, **kw)

    def test_disjoint_tasks_really_run_in_parallel(self):
        barrier = threading.Barrier(3, timeout=5)

        def fn(task, ws):
            barrier.wait()                       # only passes if all three run at once
            ws.write(task.paths[0], task.id)
        w = FnWorker(fn)
        g = TaskGraph([T("x", ["x.txt"]), T("y", ["y.txt"]), T("z", ["z.txt"])])
        rep = self.sched(w, parallel=3).run(g)
        self.assertTrue(rep.ok, {k: v.reason for k, v in rep.outcomes.items()})
        self.assertEqual(rep.max_concurrency, 3)
        self.assertEqual({p: self.repo.main_files()[p] for p in ("x.txt", "y.txt", "z.txt")},
                         {"x.txt": b"x", "y.txt": b"y", "z.txt": b"z"})
        self.assertEqual(self.leases.holders(), {})

    def test_overlapping_tasks_run_one_after_another_and_the_second_sees_the_first(self):
        def append(task, ws):
            ws.write("a.txt", ws.read_text("a.txt") + task.id + "\n")
        w = FnWorker(append, delay=0.02)
        g = TaskGraph([T("one", ["a.txt"]), T("two", ["a.txt"]), T("three", ["a.txt"])])
        rep = self.sched(w, parallel=3).run(g)
        self.assertTrue(rep.ok)
        self.assertEqual(rep.max_concurrency, 1)
        self.assertEqual(self.repo.main_files()["a.txt"], b"A\none\ntwo\nthree\n")   # no lost update
        self.assertEqual(w.overlap_seen, [])

    def test_dependent_task_forks_from_main_containing_its_dependency(self):
        w = FnWorker(lambda t, ws: ws.write("gen.txt", "v1") if t.id == "gen" else ws.write("use.txt", ws.read_text("gen.txt")))
        g = TaskGraph([T("gen", ["gen.txt"]), T("use", ["use.txt"], ["gen"])])
        rep = self.sched(w).run(g)
        self.assertTrue(rep.ok)
        self.assertEqual(self.repo.main_files()["use.txt"], b"v1")
        self.assertIn("gen.txt", w.forks["use"])

    def test_failure_skips_dependents_only(self):
        def fn(task, ws):
            if task.id == "bad":
                raise RuntimeError("boom")
            ws.write(task.paths[0], "ok")
        g = TaskGraph([T("bad", ["p.txt"]), T("child", ["q.txt"], ["bad"]), T("grandchild", ["r.txt"], ["child"]),
                       T("fine", ["s.txt"])])
        rep = self.sched(FnWorker(fn)).run(g)
        self.assertEqual(rep.by_status("failed"), ["bad"])
        self.assertEqual(sorted(rep.by_status("skipped")), ["child", "grandchild"])
        self.assertEqual(rep.by_status("done"), ["fine"])
        self.assertIn("boom", rep.outcomes["bad"].reason)
        self.assertEqual(self.leases.holders(), {})

    def test_overlapping_later_task_still_runs_if_the_earlier_one_failed(self):
        def fn(task, ws):
            if task.id == "first":
                raise RuntimeError("x")
            ws.write("a.txt", "second")
        rep = self.sched(FnWorker(fn)).run(TaskGraph([T("first", ["a.txt"]), T("second", ["a.txt"])]))
        self.assertEqual((rep.by_status("failed"), rep.by_status("done")), (["first"], ["second"]))

    def test_out_of_scope_edits_are_audited(self):
        def fn(task, ws):
            ws.write("a.txt", "mine")
            ws.write("b.txt", "not mine")
        rep = self.sched(FnWorker(fn)).run(TaskGraph([T("t", ["a.txt"])]))
        self.assertEqual(rep.outcomes["t"].audit.out_of_scope, ["b.txt"])
        self.assertEqual(self.leases.violations()[0]["out_of_scope"], ["b.txt"])

    def test_waits_for_a_lease_held_elsewhere_then_runs(self):
        self.leases.acquire("outsider", ["a.txt"])
        threading.Timer(0.15, lambda: self.leases.release("outsider")).start()
        rep = self.sched(FnWorker(write("a.txt", "late"))).run(TaskGraph([T("t", ["a.txt"])]))
        self.assertTrue(rep.ok)
        self.assertGreaterEqual(rep.outcomes["t"].started, 0)
        self.assertEqual(self.repo.main_files()["a.txt"], b"late")

    def test_gives_up_on_a_lease_that_never_frees(self):
        self.leases.acquire("outsider", ["a.txt"])
        rep = self.sched(FnWorker(write("a.txt", "x")), patience=0.1).run(
            TaskGraph([T("t", ["a.txt"]), T("u", ["b.txt"])]))
        self.assertEqual(rep.by_status("skipped"), ["t"])
        self.assertEqual(rep.by_status("done"), ["u"])

    def test_rejected_landing_is_reported(self):
        def fn(task, ws):
            ws.write("a.txt", task.id)
        # two tasks that declare disjoint scopes but really edit the same line: a genuine conflict
        g = TaskGraph([T("p", ["p.txt"]), T("q", ["q.txt"])])
        rep = self.sched(FnWorker(fn, delay=0.05), parallel=2).run(g)
        self.assertEqual(len(rep.by_status("rejected")), 1)
        self.assertEqual(len(rep.by_status("done")), 1)

    def test_random_graphs_never_overlap_and_lose_nothing(self):
        files = [f"d{d}/f{f}.txt" for d in range(3) for f in range(3)]
        for seed in range(12):
            rng = random.Random(seed)
            repo = Repository.init(MemoryKV(), {p: b"0\n" for p in files})
            tasks = []
            for i in range(rng.randint(4, 9)):
                scope = rng.choice([[rng.choice(files)], [f"d{rng.randrange(3)}"], rng.sample(files, 2)])
                deps = [t.id for t in tasks if rng.random() < 0.15]
                tasks.append(T(f"t{i}", scope, deps))
            w = FnWorker(lambda task, ws: [ws.write(p, ws.read_text(p) + task.id + "\n")
                                           for p in sorted(f for f in files if inside(make_scope(task.paths), f))],
                         delay=0.003)
            rep = Scheduler(repo, LeaseTable(repo.kv), w, parallel=4, poll=0.002).run(TaskGraph(tasks))
            self.assertTrue(rep.ok, (seed, {k: v.reason for k, v in rep.outcomes.items() if v.reason}))
            self.assertEqual(w.overlap_seen, [], seed)
            final = repo.main_files()
            for p in files:
                lines = final[p].decode().split()[1:]
                expect = [t.id for t in tasks if inside(make_scope(t.paths), p)]
                self.assertEqual(sorted(lines), sorted(expect), (seed, p))     # every task's edit survived


if __name__ == "__main__":
    unittest.main()
