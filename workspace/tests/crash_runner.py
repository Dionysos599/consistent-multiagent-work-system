"""Run an orchestrator against a gateway; with ``crash`` the process kills itself mid-task (used by test_http_integration).

    python3 crash_runner.py URL MAIN RUN_ID TAG crash|finish
"""

import os
import sys

from cmws_ws.agents import LoopWorker, Orchestrator, Reply, Task, TaskGraph
from cmws_ws.agents.llm import call
from cmws_ws.kv import HttpKV
from cmws_ws.repo import Repository


class Model:
    """Decides its next move from the conversation alone, so a resumed run continues where the last stopped."""

    def __init__(self, crash):
        self.crash = crash

    def chat(self, messages, tools):
        done = sum(1 for m in messages if m["role"] == "tool")
        task = messages[1]["content"].split("\n")[0].split()[-1]
        if done < 3:
            return Reply("", [call("write_file", f"c{done}", path=f"out/{task}/f{done}.txt", content=f"{task} {done}\n")])
        if self.crash and task.startswith("x"):
            os._exit(9)
        return Reply("", [call("finish", "fin", summary=f"wrote three files for {task}")])


class Worker:
    name = "scripted"

    def __init__(self, crash):
        self.inner = LoopWorker(Model(crash), checkpoint_every=1)

    def run(self, task, repo, agent_id):
        return self.inner.run(task, repo, agent_id)


def main(url, name, run_id, tag, mode):
    repo = Repository(HttpKV(url, timeout=30), main=name)
    tasks = [Task(f"x{tag}", f"write files for x{tag}", (f"out/x{tag}",)),
             Task(f"y{tag}", f"write files for y{tag}", (f"out/y{tag}",), "", (f"x{tag}",))]
    orch = Orchestrator(repo, Worker(mode == "crash"), owner=f"proc-{mode}", parallel=1, claim_ttl=2.0, poll=0.05)
    out = orch.run(run_id, graph=TaskGraph(tasks))
    print("ok" if out.ok else "failed", flush=True)
    return 0 if out.ok else 1


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:6]))
