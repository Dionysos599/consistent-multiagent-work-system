"""Command line for the workspace service.

    python3 -m cmws_ws.cli --url http://127.0.0.1:8080 init --from ./project
    python3 -m cmws_ws.cli --url ... clone alice ./alice-work
    python3 -m cmws_ws.cli status ./alice-work        # also: pull, push -m MSG, submit, resolve, watch
    python3 -m cmws_ws.cli --url ... log

The URL defaults to $CMWS_URL, then http://127.0.0.1:8080 (``make run-gateway``).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from .history import log
from .kv import HttpKV
from .repo import MAIN, Repository
from .sync import STATE_DIR, Checkout, Watcher


def _read_tree(root: Path, checkout_like: Checkout = None):
    files = {}
    ignore = Checkout.__new__(Checkout)
    ignore.root, ignore._ignore_patterns = root, None
    for dirpath, dirs, names in os.walk(root):
        rel_dir = Path(dirpath).relative_to(root).as_posix()
        rel_dir = "" if rel_dir == "." else rel_dir
        dirs[:] = [d for d in dirs if not ignore.ignored(f"{rel_dir}/{d}".lstrip("/"))]
        for n in names:
            rel = f"{rel_dir}/{n}".lstrip("/")
            p = root / rel
            if not ignore.ignored(rel) and p.is_file() and not p.is_symlink():
                files[rel] = p.read_bytes()
    return files


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="cmws", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default=os.environ.get("CMWS_URL", "http://127.0.0.1:8080"))
    ap.add_argument("--main", default=MAIN, help="name of the main ref")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("init", help="create the repository, optionally from a directory")
    p.add_argument("--from", dest="source", type=Path)
    p = sub.add_parser("clone", help="create a local folder for a workspace branch")
    p.add_argument("agent"), p.add_argument("dir", type=Path)
    for name in ("status", "pull", "push", "submit", "watch"):
        p = sub.add_parser(name)
        p.add_argument("dir", type=Path, nargs="?", default=Path("."))
        if name in ("push", "submit"):
            p.add_argument("-m", "--message")
        if name == "watch":
            p.add_argument("--interval", type=float, default=1.0)
            p.add_argument("--debounce", type=float, default=2.0)
    p = sub.add_parser("resolve", help="mark a non-text conflict as handled")
    p.add_argument("dir", type=Path), p.add_argument("path")
    p = sub.add_parser("log", help="history of main")
    p.add_argument("--limit", type=int, default=20)
    a = ap.parse_args(argv)

    repo_kwargs = {"main": a.main}
    kv = HttpKV(a.url)
    if a.cmd == "init":
        files = _read_tree(a.source) if a.source else None
        repo = Repository.init(kv, files, **repo_kwargs)
        print(f"main = {repo.head()[:12]} ({len(files or {})} files imported)")
        return 0
    repo = Repository(kv, **repo_kwargs)
    if a.cmd == "clone":
        co = Checkout.clone(repo, a.agent, a.dir)
        print(f"cloned ws/{a.agent} at {co.base[:12]} into {a.dir}")
        return 0
    if a.cmd == "log":
        for cid, c in log(repo.store, repo.head(), a.limit):
            print(f"{cid[:12]}  {c.author:12}  {c.message}")
        return 0

    co = Checkout.open(repo, a.dir)
    if a.cmd == "status":
        for path, code in co.status().items():
            print(code, path)
        for path, kind in co.unresolved().items():
            print("!", path, f"({kind} conflict)")
    elif a.cmd == "pull":
        r = co.pull()
        print(r.status, *r.updated)
        for path, kind in r.conflicts.items():
            print("!", path, f"({kind} conflict)")
        return 1 if r.conflicts else 0
    elif a.cmd == "push":
        r = co.push(a.message)
        print(r.status, *(r.files or []))
        for path, kind in r.conflicts.items():
            print("!", path, f"({kind} conflict)")
        return 0 if r.status in ("pushed", "nothing") else 1
    elif a.cmd == "submit":
        r = co.submit(a.message)
        status = getattr(r, "status", "?")
        print(status, *(getattr(r, "files", None) or []))
        for c in getattr(r, "conflicts", None) or []:
            print("!", getattr(c, "path", c), getattr(c, "kind", ""))
        for s in getattr(r, "stale_reads", None) or []:
            print("stale read:", s.path)
        return 0 if status in ("fast-forward", "merged", "noop", "pushed", "nothing") else 1
    elif a.cmd == "resolve":
        co.resolve(a.path)
    elif a.cmd == "watch":
        w = Watcher(co, interval=a.interval, debounce=a.debounce,
                    on_event=lambda kind, info: print(time.strftime("%H:%M:%S"), kind,
                                                      getattr(info, "status", ""), flush=True))
        print(f"watching {a.dir} (Ctrl-C to stop)")
        w.start()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            w.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
