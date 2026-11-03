import sys
import threading
import time
import unittest

from cmws_ws.agents import (CommandValidator, Escalations, FnValidator, LoopWorker, MergeQueue, Reply, ScriptedModel,
                            Scheduler, Task, TaskGraph, Verdict, WorkerResult, merger_worker)
from cmws_ws.agents.llm import call
from cmws_ws.agents.mergequeue import _has_new_markers
from cmws_ws.kv import MemoryKV
from cmws_ws.leases import LeaseTable
from cmws_ws.repo import Repository

from tests.support import RecordingKV, check_linear_history
from tests.test_scheduling import FnWorker, T

BASE = {"a.txt": b"title\nvalue = 1\nend\n", "b.txt": b"B\n", "c.txt": b"C\n"}


def edit(repo, name, **files):
    """Fork a branch, change files, commit; return the Task describing it."""
    ws = repo.fork(name)
    for path, text in files.items():
        ws.write(path.replace("__", "/").replace("_dot_", "."), text)
    ws.commit(f"{name} work")
    return Task(name, f"task {name}")


class Doer:
    """A worker applying a function to a fresh branch (used as the redo worker)."""
    name = "doer"

    def __init__(self, fn):
        self.fn, self.runs = fn, []

    def run(self, task, repo, agent_id):
        self.runs.append(task)
        ws = repo.resume(agent_id)
        self.fn(task, ws)
        return WorkerResult("done", f"redid {task.id}", ws.commit(task.id))


def resolver(path, resolved):
    """A scripted merger: reads the file (must contain markers), writes the resolution, finishes."""
    seen = {}

    def write(msgs):
        seen["read"] = msgs[-1]["content"]
        return Reply("", [call("write_file", "2", path=path, content=resolved)])
    model = ScriptedModel([Reply("", [call("read_file", "1", path=path)]), write,
                           Reply("", [call("finish", "3", summary="kept both values")])])
    return merger_worker(model), model, seen


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repository.init(MemoryKV(), dict(BASE))

    def test_a_semantic_conflict_that_text_merge_accepts_is_caught_on_the_merged_result(self):
        def no_duplicate_defs(files, changed):
            names = [line for data in files.values() for line in data.decode().splitlines() if line.startswith("def ")]
            dup = sorted({n for n in names if names.count(n) > 1})
            return Verdict(not dup, f"duplicate definitions: {dup}")
        q = MergeQueue(self.repo, FnValidator(no_duplicate_defs))
        ta = edit(self.repo, "a", **{"x_dot_py": "def helper():\n    return 1\n"})
        tb = edit(self.repo, "b", **{"y_dot_py": "def helper():\n    return 2\n"})
        self.assertTrue(q.land(ta).landed)
        main = self.repo.head()
        lb = q.land(tb)
        self.assertEqual((lb.status, lb.via), ("escalated", "human"))
        self.assertEqual(self.repo.head(), main, "main must not move for a rejected candidate")
        self.assertIn("duplicate definitions", lb.steps[0].detail)
        self.assertEqual(q.escalations.list()[0]["task"], "b")

    def test_validator_sees_the_merged_tree_and_changed_paths(self):
        seen = []
        q = MergeQueue(self.repo, FnValidator(lambda files, changed: seen.append((dict(files), changed)) or Verdict(True)))
        edit(self.repo, "a", **{"b_dot_txt": "B2\n"})
        edit(self.repo, "c", **{"c_dot_txt": "C2\n"})
        q.land(Task("a", "x"))
        q.land(Task("c", "x"))
        self.assertEqual(seen[0][1], ["b.txt"])
        self.assertEqual(seen[1][1], ["c.txt"])
        self.assertEqual((seen[1][0]["b.txt"], seen[1][0]["c.txt"]), (b"B2\n", b"C2\n"))   # both changes present

    def test_nothing_to_land_is_a_noop_and_is_not_validated(self):
        calls = []
        q = MergeQueue(self.repo, FnValidator(lambda f, c: calls.append(1) or Verdict(True)))
        self.repo.fork("empty")
        self.assertEqual(q.land(Task("empty", "x")).status, "noop")
        self.assertEqual(calls, [])

    def test_validator_runs_again_when_main_moves_before_the_cas(self):
        from tests.test_edge_cases import RacyKV
        kv = RacyKV()
        repo = Repository.init(kv, dict(BASE))
        seen = []
        q = MergeQueue(repo, FnValidator(lambda files, changed: seen.append(dict(files)) or Verdict(True)))
        edit(repo, "a", **{"b_dot_txt": "B2\n"})
        rival = edit(repo, "rival", **{"c_dot_txt": "C2\n"})
        raced = []

        def rival_lands():
            if not raced:
                raced.append(1)
                repo.submit(repo.resume("rival"))
        kv.hooks["ref/main"] = rival_lands
        res = q.land(Task("a", "x"))
        self.assertTrue(res.landed)
        self.assertEqual(len(seen), 2)                          # validated on the old main, then on the new one
        self.assertEqual(seen[1]["c.txt"], b"C2\n")
        self.assertEqual(repo.main_files()["b.txt"], b"B2\n")
        self.assertEqual(repo.main_files()["c.txt"], b"C2\n")

    def test_command_validator(self):
        ok = CommandValidator([sys.executable, "-c", "import sys,pathlib;sys.exit(0 if pathlib.Path('a.txt').exists() else 1)"])
        self.assertTrue(ok.check({"a.txt": b"x"}, []).ok)
        self.assertFalse(ok.check({"z.txt": b"x"}, []).ok)
        out = CommandValidator([sys.executable, "-c", "print('boom');raise SystemExit(4)"]).check({}, [])
        self.assertEqual((out.ok, out.output), (False, "boom"))
        self.assertFalse(CommandValidator([sys.executable, "-c", "import time;time.sleep(30)"], timeout=0.3).check({}, []).ok)
        self.assertFalse(CommandValidator(["/no/such/tool"]).check({}, []).ok)
        nested = CommandValidator([sys.executable, "-c", "import pathlib;assert pathlib.Path('d/e/f.txt').read_text()=='hi'"])
        self.assertTrue(nested.check({"d/e/f.txt": b"hi"}, []).ok)

    def test_real_test_command_gates_main(self):
        repo = Repository.init(MemoryKV(), {"calc.py": b"def add(a, b):\n    return a + b\n",
                                            "test_calc.py": b"import calc\nassert calc.add(2, 2) == 4\n"})
        q = MergeQueue(repo, CommandValidator([sys.executable, "test_calc.py"]))
        edit(repo, "good", **{"calc_dot_py": "def add(a, b):\n    return b + a\n"})
        edit(repo, "bad", **{"calc_dot_py": "def add(a, b):\n    return a - b\n"})
        self.assertTrue(q.land(Task("good", "x")).landed)
        bad = q.land(Task("bad", "x"))
        self.assertEqual(bad.status, "escalated")
        self.assertIn(b"b + a", repo.main_files()["calc.py"])

    def test_marker_guard_ignores_markers_main_already_had(self):
        main = {"doc.md": b"<<<<<<< example\n"}
        self.assertEqual(_has_new_markers({"doc.md": b"<<<<<<< example\nmore\n"}, main), [])
        self.assertEqual(_has_new_markers({"doc.md": b"<<<<<<< x\n", "n.txt": b">>>>>>> y\n"}, main), ["n.txt"])
        self.assertEqual(_has_new_markers({"bin": b"\xff<<<<<<< "}, {}), [])
        self.assertEqual(_has_new_markers({"t": b"a <<<<<<< not at line start\n"}, {}), [])


class EscalationTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repository.init(MemoryKV(), dict(BASE))

    def conflicting(self):
        ta = edit(self.repo, "a", **{"a_dot_txt": "title\nvalue = 2\nend\n"})
        tb = edit(self.repo, "b", **{"a_dot_txt": "title\nvalue = 3\nend\n"})
        return ta, tb

    def test_without_helpers_a_conflict_goes_straight_to_a_person(self):
        q = MergeQueue(self.repo)
        ta, tb = self.conflicting()
        self.assertTrue(q.land(ta).landed)
        lb = q.land(tb)
        self.assertEqual((lb.status, lb.via), ("escalated", "human"))
        rec = q.escalations.list()[0]
        self.assertEqual((rec["task"], rec["branch"], rec["reason"]), ("b", "ws/b", "conflict"))
        self.assertEqual(rec["conflicts"], [["a.txt", "content"]])
        self.assertEqual(lb.escalation, rec["id"])

    def test_a_merger_agent_resolves_the_conflict(self):
        merger, model, seen = resolver("a.txt", "title\nvalue = 2 and 3\nend\n")
        q = MergeQueue(self.repo, merger=merger)
        ta, tb = self.conflicting()
        q.land(ta)
        lb = q.land(tb)
        self.assertEqual((lb.status, lb.via), ("landed", "merger"))
        self.assertEqual(self.repo.main_files()["a.txt"], b"title\nvalue = 2 and 3\nend\n")
        self.assertIn("<<<<<<<", seen["read"])                          # the merger really saw the markers
        self.assertIn("value = 2", seen["read"])
        self.assertIn("value = 3", seen["read"])
        prompt = model.calls[0][1]["content"]
        self.assertIn("task b", prompt)                                  # intent of the incoming change
        self.assertIn("a work", prompt)                                  # what landed on main meanwhile
        self.assertIn("a.txt", prompt)
        self.assertEqual([s.stage for s in lb.steps], ["auto", "merger", "merger"])

    def test_leftover_markers_are_refused_and_the_task_is_redone(self):
        merger = merger_worker(ScriptedModel([Reply("", [call("finish", "1", summary="looks fine to me")])]))
        redo = Doer(lambda task, ws: ws.write("a.txt", ws.read_text("a.txt").replace("value = 2", "value = 2\nextra = 3")))
        q = MergeQueue(self.repo, merger=merger, redo_worker=redo)
        ta, tb = self.conflicting()
        q.land(ta)
        lb = q.land(tb)
        self.assertEqual((lb.status, lb.via, lb.branch), ("landed", "redo", "b-redo1"))
        self.assertEqual(self.repo.main_files()["a.txt"], b"title\nvalue = 2\nextra = 3\nend\n")
        self.assertNotIn(b"<<<<<<<", self.repo.main_files()["a.txt"])
        self.assertTrue(any("conflict markers" in s.detail for s in lb.steps))
        self.assertIn("value = 2", redo.runs[0].context)                  # the redo is told what landed upstream
        self.assertIn("could not be merged", redo.runs[0].context)
        self.assertEqual([(x.stage, x.status) for x in lb.steps],
                         [("auto", "conflict"), ("merger", "done"), ("merger", "land:rejected"),
                          ("redo", "done"), ("redo", "fast-forward")])

    def test_everything_failing_leaves_a_record_and_the_marked_branch(self):
        merger = merger_worker(ScriptedModel([Reply("no tools, sorry")] * 6))
        class Broken:
            name = "broken"
            runs = []

            def run(self, task, repo, agent_id):
                self.runs.append(task)
                return WorkerResult("error", error="model unreachable")
        redo = Broken()
        redo.runs = []
        q = MergeQueue(self.repo, merger=merger, redo_worker=redo, max_redos=2)
        ta, tb = self.conflicting()
        q.land(ta)
        main = self.repo.head()
        lb = q.land(tb)
        self.assertEqual((lb.status, lb.via), ("escalated", "human"))
        self.assertEqual(self.repo.head(), main)
        self.assertEqual(len(redo.runs), 2)
        rec = q.escalations.list()[0]
        self.assertEqual(rec["task"], "b")
        self.assertEqual([s[0] for s in rec["steps"]].count("redo"), 2)
        self.assertIn(b"<<<<<<<", self.repo.files_at(self.repo.refs.get("ws/b"))["a.txt"])

    def test_a_stale_read_skips_the_merger_and_goes_to_redo(self):
        merger = merger_worker(ScriptedModel([]))
        redo = Doer(lambda task, ws: ws.write("b.txt", ws.read_text("a.txt").splitlines()[1] + "\n"))
        q = MergeQueue(self.repo, merger=merger, redo_worker=redo)
        ws = self.repo.fork("reader")
        ws.write("b.txt", ws.read_text("a.txt").splitlines()[1] + "\n")      # derived from a.txt
        ws.commit("reader work")
        edit(self.repo, "a", **{"a_dot_txt": "title\nvalue = 2\nend\n"})
        q.land(Task("a", "x"))
        lr = q.land(Task("reader", "copy the value line"))
        self.assertEqual((lr.status, lr.via), ("landed", "redo"))
        self.assertEqual(self.repo.main_files()["b.txt"], b"value = 2\n")     # recomputed from the new a.txt
        self.assertEqual(lr.steps[0].status, "stale")
        self.assertNotIn("merger", [s.stage for s in lr.steps])

    def test_modify_delete_conflict_goes_through_the_merger_too(self):
        model = ScriptedModel([Reply("", [call("delete_file", "1", path="c.txt")]),
                               Reply("", [call("finish", "2", summary="deleted, the edit was obsolete")])])
        q = MergeQueue(self.repo, merger=merger_worker(model))
        ws = self.repo.fork("del")
        ws.delete("c.txt")
        ws.commit("remove c")
        edit(self.repo, "mod", **{"c_dot_txt": "C changed\n"})
        q.land(Task("mod", "x"))
        res = q.land(Task("del", "remove c.txt"))
        self.assertEqual((res.status, res.via), ("landed", "merger"))
        self.assertNotIn("c.txt", self.repo.main_files())

    def test_escalations_command_output(self):
        import contextlib
        import io
        from cmws_ws import cli
        q = MergeQueue(self.repo)
        ta, tb = self.conflicting()
        q.land(ta)
        q.land(tb)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli._print_escalations(q.escalations.list())
        self.assertEqual(code, 0)
        text = out.getvalue()
        self.assertIn("ws/b  conflict  a.txt (content)", text)
        self.assertIn("auto    conflict", text)
        self.assertTrue(text.rstrip().endswith("1 escalation(s)"))

    def test_escalation_list_is_durable_and_ordered(self):
        e = Escalations(MemoryKV(), clock=iter(range(1, 10_000)).__next__)
        ids = [e.add({"task": f"t{i}"}) for i in range(3)]
        self.assertEqual([x["id"] for x in e.list()], ids)
        self.assertEqual(len(set(ids)), 3)


class UnionMerger:
    """A deterministic stand-in for a merger agent: keep the lines of both sides, drop the markers."""
    name = "union"

    def run(self, task, repo, agent_id):
        ws = repo.resume(agent_id)
        for path in task.paths:
            if not ws.exists(path):
                continue
            keep, seen = [], set()
            for line in ws.read_text(path).splitlines():
                if line.startswith(("<<<<<<< ", "||||||| ", "=======", ">>>>>>> ")):
                    continue
                if line not in seen:
                    seen.add(line)
                    keep.append(line)
            ws.write(path, "\n".join(keep) + "\n")
        return WorkerResult("done", "union", ws.commit(task.id))


class RandomChainTests(unittest.TestCase):
    def test_random_concurrent_edits_through_the_whole_chain(self):
        import random
        for seed in range(15):
            rng = random.Random(seed)
            kv = RecordingKV()
            base = "".join(f"row {i}\n" for i in range(6))
            repo = Repository.init(kv, {"t.txt": base, "u.txt": base})
            root = repo.head()

            def no_dupes(files, changed):
                for path, data in files.items():
                    lines = data.decode().splitlines()
                    if len(lines) != len(set(lines)):
                        return Verdict(False, f"duplicate lines in {path}")
                return Verdict(True)

            def add_line(task, ws, blind=False):
                path = task.paths[0]
                # a blind writer works from the rows it was given instead of reading (no read to go stale)
                lines = base.splitlines() if blind else ws.read_text(path).splitlines()
                who = task.id.split("-")[0]                      # a redo runs as "<name>-redoN"
                lines.insert(rng_pos[who] % (len(lines) + 1), f"new {who}")
                ws.write(path, "\n".join(lines) + "\n")
            n = rng.randint(4, 7)
            names = [f"w{i}" for i in range(n)]
            rng_pos = {x: rng.randrange(100) for x in names}
            target = {x: rng.choice(["t.txt", "u.txt"]) for x in names}
            redo = Doer(add_line)
            q = MergeQueue(repo, FnValidator(no_dupes), merger=UnionMerger(), redo_worker=redo, max_redos=3)
            tasks = {x: Task(x, "add a line", (target[x],)) for x in names}
            for x in names:
                ws = repo.fork(x)
                add_line(tasks[x], ws, blind=int(x[1:]) % 2 == 0)
                ws.commit(x)
            out = {}
            barrier = threading.Barrier(n)

            errors = []

            def go(x):
                barrier.wait()
                try:
                    out[x] = q.land(tasks[x])
                except Exception as e:                           # a crash must fail the test, not vanish
                    errors.append(repr(e))
            ts = [threading.Thread(target=go, args=(x,)) for x in names]
            [t.start() for t in ts]
            [t.join() for t in ts]
            self.assertEqual(errors, [], seed)
            files = repo.main_files()
            ctx = (seed, {x: (o.status, o.via) for x, o in out.items()})
            for x in names:
                present = sum(f"new {x}" in files[p].decode().splitlines() for p in files)
                self.assertEqual(present, 1 if out[x].landed else 0, (x, ctx))
            for p in files:
                text = files[p].decode()
                self.assertNotIn("<<<<<<<", text, ctx)
                self.assertEqual(sorted(l for l in text.splitlines() if l.startswith("row")),
                                 sorted(f"row {i}" for i in range(6)), ctx)              # original rows untouched
            check_linear_history(self, repo, kv.transitions, root)
            self.assertTrue({new for _, new in kv.transitions[1:]} <= set(q.approved), ctx)
            self.assertEqual(sum(1 for e in q.escalations.list()), sum(o.status == "escalated" for o in out.values()))


class ChainDetailTests(unittest.TestCase):
    """Pins down the exact record the queue keeps of each stage."""

    def setUp(self):
        self.repo = Repository.init(MemoryKV(), dict(BASE))

    def conflicting(self):
        return (edit(self.repo, "a", **{"a_dot_txt": "title\nvalue = 2\nend\n"}),
                edit(self.repo, "b", **{"a_dot_txt": "title\nvalue = 3\nend\n"}))

    @staticmethod
    def stages(landing):
        return [(s.stage, s.status) for s in landing.steps]

    def test_step_log_of_a_complete_failure(self):
        class Broken:
            name = "broken"

            def run(self, task, repo, agent_id):
                return WorkerResult("error", error="model unreachable")
        merger = merger_worker(ScriptedModel([Reply("no tools, sorry")] * 6))
        q = MergeQueue(self.repo, merger=merger, redo_worker=Broken(), max_redos=2)
        ta, tb = self.conflicting()
        q.land(ta)
        lb = q.land(tb)
        self.assertEqual(self.stages(lb), [("auto", "conflict"), ("merger", "gave_up"), ("redo", "error"),
                                           ("redo", "error"), ("human", "escalated")])
        self.assertEqual(lb.steps[0].detail, "a.txt (content)")
        self.assertEqual(lb.steps[1].detail, "no tools, sorry")
        self.assertEqual(lb.steps[2].detail, "model unreachable")
        self.assertEqual(lb.steps[4].detail, lb.escalation)

    def test_unresolved_markers_keep_the_original_conflicts_in_the_record(self):
        merger = merger_worker(ScriptedModel([Reply("", [call("finish", "1", summary="done")])]))
        q = MergeQueue(self.repo, merger=merger)
        ta, tb = self.conflicting()
        q.land(ta)
        lb = q.land(tb)
        self.assertEqual(self.stages(lb), [("auto", "conflict"), ("merger", "done"),
                                           ("merger", "land:rejected"), ("human", "escalated")])
        rec = q.escalations.list()[0]
        self.assertEqual(rec["conflicts"], [["a.txt", "content"]])
        self.assertEqual(rec["reason"], "rejected")
        self.assertIn("conflict markers", rec["detail"])

    def test_merger_gets_the_validation_failure_when_the_text_merged_cleanly(self):
        def no_dup(files, changed):
            names = [l for d in files.values() for l in d.decode().splitlines() if l.startswith("def ")]
            dup = sorted({n for n in names if names.count(n) > 1})
            return Verdict(not dup, f"duplicate definitions: {dup}")
        model = ScriptedModel([Reply("", [call("write_file", "1", path="y.py", content="def helper_two():\n    return 2\n")]),
                               Reply("", [call("finish", "2", summary="renamed")])])
        q = MergeQueue(self.repo, FnValidator(no_dup), merger=merger_worker(model))
        ta = edit(self.repo, "a", **{"x_dot_py": "def helper():\n    return 1\n"})
        tb = edit(self.repo, "b", **{"y_dot_py": "def helper():\n    return 2\n"})
        q.land(ta)
        lb = q.land(tb)
        self.assertEqual((lb.status, lb.via), ("landed", "merger"))
        prompt = model.calls[0][1]["content"]
        self.assertIn("Validation of the merged result failed", prompt)
        self.assertIn("duplicate definitions", prompt)
        self.assertIn("none marked", prompt)
        self.assertEqual(self.stages(lb), [("auto", "rejected"), ("merger", "done"), ("merger", "land:fast-forward")])

    def test_a_conflict_that_disappears_before_the_merger_runs_is_just_landed(self):
        class FlakyFirst(MergeQueue):
            first = True

            def _try(self, agent_id, message):
                if self.first:
                    self.first = False
                    from cmws_ws.repo import SubmitResult
                    return SubmitResult("conflict")
                return super()._try(agent_id, message)
        merger = merger_worker(ScriptedModel([]))
        q = FlakyFirst(self.repo, merger=merger)
        edit(self.repo, "z", **{"b_dot_txt": "B2\n"})
        res = q.land(Task("z", "x"))
        self.assertTrue(res.landed)
        self.assertEqual(self.repo.main_files()["b.txt"], b"B2\n")
        self.assertEqual([s.stage for s in res.steps], ["auto"])        # the merger agent never ran

    def test_contended_main_gets_one_more_try(self):
        from tests.test_edge_cases import RacyKV
        kv = RacyKV()
        repo = Repository.init(kv, dict(BASE))
        repo.max_attempts = 1
        q = MergeQueue(repo)
        edit(repo, "a", **{"b_dot_txt": "B2\n"})
        edit(repo, "rival", **{"c_dot_txt": "C2\n"})
        kv.hooks["ref/main"] = lambda: repo.submit(repo.resume("rival"))
        res = q.land(Task("a", "x"))
        self.assertTrue(res.landed)
        self.assertEqual(self.stages(res), [("auto", "contended"), ("auto", "merged")])
        self.assertEqual(repo.main_files()["b.txt"], b"B2\n")
        self.assertEqual(repo.main_files()["c.txt"], b"C2\n")

    def test_contended_twice_goes_to_a_person(self):
        class Always(MergeQueue):
            def _try(self, agent_id, message):
                from cmws_ws.repo import SubmitResult
                return SubmitResult("contended")
        q = Always(self.repo)
        edit(self.repo, "a", **{"b_dot_txt": "B2\n"})
        res = q.land(Task("a", "x"))
        self.assertEqual((res.status, res.via, len(res.steps)), ("escalated", "human", 3))

    def test_redo_context_keeps_the_task_context_and_the_stale_paths(self):
        redo = Doer(lambda task, ws: ws.write("b.txt", "v=" + ws.read_text("a.txt").splitlines()[1] + "\n"))
        q = MergeQueue(self.repo, redo_worker=redo)
        ws = self.repo.fork("r")
        ws.write("b.txt", "v=" + ws.read_text("a.txt").splitlines()[1] + "\n")
        ws.commit("r")
        edit(self.repo, "a", **{"a_dot_txt": "title\nvalue = 2\nend\n"})
        q.land(Task("a", "x"))
        res = q.land(Task("r", "copy the value", ("b.txt",), "ORIGINAL CONTEXT", ()))
        self.assertTrue(res.landed)
        ctx = redo.runs[0].context
        self.assertTrue(ctx.startswith("ORIGINAL CONTEXT\n\n"))
        self.assertIn("a.txt", ctx.split("Since then")[0])           # which read went stale
        self.assertEqual(redo.runs[0].paths, ("b.txt",))
        self.assertEqual(self.stages(res)[0], ("auto", "stale"))
        self.assertEqual(res.steps[0].detail, "a.txt")

    def test_a_large_upstream_diff_is_cut_down(self):
        redo = Doer(lambda task, ws: ws.write("b.txt", "redone\n"))
        q = MergeQueue(self.repo, redo_worker=redo)
        ws = self.repo.fork("r")
        ws.write("b.txt", ws.read_text("a.txt")[:5])
        ws.commit("r")
        edit(self.repo, "a", **{"a_dot_txt": "title\n" + "".join(f"line {i}\n" for i in range(3000)) + "end\n"})
        q.land(Task("a", "x"))
        q.land(Task("r", "x"))
        self.assertIn("[... diff cut off]", redo.runs[0].context)
        self.assertLess(len(redo.runs[0].context), 9000)
        small = Doer(lambda task, ws: ws.write("b.txt", "z\n"))
        q2 = MergeQueue(Repository.init(MemoryKV(), dict(BASE)), redo_worker=small)
        ws = q2.repo.fork("r")
        ws.write("b.txt", ws.read_text("a.txt")[:5])
        ws.commit("r")
        edit(q2.repo, "a", **{"a_dot_txt": "title\nvalue = 2\nend\n"})
        q2.land(Task("a", "x"))
        q2.land(Task("r", "x"))
        self.assertNotIn("cut off", small.runs[0].context)

    def test_escalations_survive_a_competing_append(self):
        from tests.test_edge_cases import RacyKV
        kv = RacyKV()
        e = Escalations(kv, clock=iter(range(1, 10_000)).__next__)
        e.add({"task": "first"})
        kv.hooks["mq/escalations"] = lambda: Escalations(kv, clock=iter(range(500, 600)).__next__).add({"task": "rival"})
        e.add({"task": "mine"})
        self.assertEqual([x["task"] for x in e.list()], ["first", "rival", "mine"])
        for i in range(Escalations.MAX + 5):
            e.add({"task": f"t{i}"})
        self.assertEqual(len(e.list()), Escalations.MAX)
        self.assertEqual(e.list()[-1]["task"], f"t{Escalations.MAX + 4}")


class QueueConcurrencyTests(unittest.TestCase):
    def test_main_only_ever_holds_validated_states_and_no_update_is_lost(self):
        for round_ in range(3):
            kv = RecordingKV()
            repo = Repository.init(kv, {"count.txt": b"0\n", "log.txt": b""})

            def invariant(files, changed):
                n = int(files["count.txt"].decode())
                lines = files["log.txt"].decode().splitlines()
                time.sleep(0.002)
                return Verdict(n == len(lines), f"count {n} but {len(lines)} log lines")

            def bump(task, ws):
                ws.write("count.txt", str(int(ws.read_text("count.txt")) + 1) + "\n")
                ws.write("log.txt", ws.read_text("log.txt") + task.id + "\n")
            redo = Doer(bump)
            q = MergeQueue(repo, FnValidator(invariant), redo_worker=redo, max_redos=12)
            root = repo.head()
            names = [f"w{i}" for i in range(8)]
            for n in names:
                ws = repo.fork(n)
                bump(Task(n, "x"), ws)
                ws.commit(n)
            results = {}
            barrier = threading.Barrier(len(names))

            def go(n):
                barrier.wait()
                results[n] = q.land(Task(n, "bump"))
            ts = [threading.Thread(target=go, args=(n,)) for n in names]
            [t.start() for t in ts]
            [t.join() for t in ts]
            landed = [r for r in results.values() if r.landed]
            self.assertEqual(len(landed) + sum(r.status == "escalated" for r in results.values()), 8)
            files = repo.main_files()
            self.assertEqual(int(files["count.txt"]), len(landed), "an update was lost or invented")
            self.assertEqual(len(files["log.txt"].decode().splitlines()), len(landed))
            check_linear_history(self, repo, kv.transitions, root)
            moved_to = {new for _, new in kv.transitions[1:]}
            self.assertTrue(moved_to <= set(q.approved), "main moved to a state no validator approved")

    def test_scheduler_lands_through_the_queue_and_reports_escalations(self):
        kv = MemoryKV()
        repo = Repository.init(kv, dict(BASE))

        def fn(task, ws):
            ws.write("a.txt", f"title\nvalue = {task.id}\nend\n")
        q = MergeQueue(repo)
        g = TaskGraph([T("p", ["p.txt"]), T("q", ["q.txt"])])        # declared disjoint but both rewrite a.txt
        barrier = threading.Barrier(2, timeout=10)

        def sync_fn(task, ws):
            ws.read_text("a.txt")
            barrier.wait()
            fn(task, ws)
        rep = Scheduler(repo, LeaseTable(kv), FnWorker(sync_fn), parallel=2, land=q.as_land(), poll=0.002).run(g)
        self.assertEqual(sorted(o.status for o in rep.outcomes.values()), ["done", "rejected"])
        self.assertEqual(len(q.escalations.list()), 1)


if __name__ == "__main__":
    unittest.main()
