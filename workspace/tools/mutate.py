"""Mutation sweep: change one operator at a time in the given modules and see whether the tests notice.

    python3 workspace/tools/mutate.py leases.py,agents/graph.py tests.test_scheduling,tests.test_robustness 8

Arguments: modules under src/cmws_ws (comma separated), test modules (comma separated), parallel jobs.
Survivors are listed with their source line; most are equivalent mutants (dataclass flags, memoization,
unreachable guards) and each one deserves a look before being dismissed.
"""
import ast, copy, os, shutil, subprocess, sys, tempfile, json
from concurrent.futures import ThreadPoolExecutor

WS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES = sys.argv[1].split(",")
TESTS = sys.argv[2].split(",")
JOBS = int(sys.argv[3]) if len(sys.argv) > 3 else 6

SWAP = {ast.Eq: ast.NotEq, ast.NotEq: ast.Eq, ast.Lt: ast.GtE, ast.GtE: ast.Lt, ast.Gt: ast.LtE, ast.LtE: ast.Gt,
        ast.In: ast.NotIn, ast.NotIn: ast.In, ast.Is: ast.IsNot, ast.IsNot: ast.Is}

def sites(tree):
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and type(node.ops[0]) in SWAP:
            out.append(("cmp", node))
        elif isinstance(node, ast.BoolOp):
            out.append(("boolop", node))
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            out.append(("not", node))
        elif isinstance(node, ast.Constant) and isinstance(node.value, bool):
            out.append(("bool", node))
        elif isinstance(node, ast.If):
            out.append(("if-false", node)); out.append(("if-true", node))
        elif isinstance(node, ast.Return) and node.value is not None:
            out.append(("return-none", node))
        elif isinstance(node, ast.Expr) and not isinstance(node.value, ast.Constant):
            out.append(("drop-stmt", node))
        elif isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub)):
            out.append(("arith", node))
    return out

def apply(tree, idx):
    t = copy.deepcopy(tree)
    kind, node = sites(t)[idx]
    if kind == "cmp":
        node.ops[0] = SWAP[type(node.ops[0])]()
    elif kind == "boolop":
        node.op = ast.Or() if isinstance(node.op, ast.And) else ast.And()
    elif kind == "not":
        # replace with operand: find parent
        for p in ast.walk(t):
            for f, v in ast.iter_fields(p):
                if v is node: setattr(p, f, node.operand)
                elif isinstance(v, list):
                    for i, x in enumerate(v):
                        if x is node: v[i] = node.operand
    elif kind == "bool":
        node.value = not node.value
    elif kind == "int":
        node.value = node.value + 1
    elif kind == "if-false":
        node.test = ast.Constant(False)
    elif kind == "if-true":
        node.test = ast.Constant(True)
    elif kind == "return-none":
        node.value = ast.Constant(None)
    elif kind == "drop-stmt":
        for p in ast.walk(t):
            for f, v in ast.iter_fields(p):
                if isinstance(v, list):
                    for i, x in enumerate(v):
                        if x is node:
                            v[i] = ast.Pass()
    elif kind == "arith":
        node.op = ast.Sub() if isinstance(node.op, ast.Add) else ast.Add()
    ast.fix_missing_locations(t)
    return kind, node.lineno, ast.unparse(t)

jobs = []
for f in FILES:
    src = open(os.path.join(WS, "src/cmws_ws", f)).read()
    tree = ast.parse(src)
    for i in range(len(sites(tree))):
        kind, line, new = apply(tree, i)
        jobs.append((f, kind, line, new))
print(len(jobs), "mutants", flush=True)

work = []
for j in range(JOBS):
    d = tempfile.mkdtemp(prefix="mut")
    shutil.copytree(os.path.join(WS, "src"), os.path.join(d, "src"), ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(os.path.join(WS, "tests"), os.path.join(d, "tests"), ignore=shutil.ignore_patterns("__pycache__"))
    work.append(d)
import queue
free = queue.Queue()
for d in work: free.put(d)

def run(job):
    f, kind, line, new = job
    d = free.get()
    try:
        path = os.path.join(d, "src/cmws_ws", f)
        orig = open(os.path.join(WS, "src/cmws_ws", f)).read()
        open(path, "w").write(new)
        try:
            r = subprocess.run([sys.executable, "-m", "unittest", "-f"] + TESTS, cwd=d, env=dict(os.environ, PYTHONPATH="src", CMWS_SKIP_INTEGRATION="1"),
                               capture_output=True, text=True, timeout=120)
            survived = r.returncode == 0
        except subprocess.TimeoutExpired:
            survived = False
        open(path, "w").write(orig)
        return (f, kind, line, survived)
    finally:
        free.put(d)

with ThreadPoolExecutor(JOBS) as ex:
    res = list(ex.map(run, jobs))
surv = [r for r in res if r[3]]
print(f"killed {len(res)-len(surv)} / {len(res)}")
for f, kind, line, _ in sorted(surv):
    srcl = open(os.path.join(WS, "src/cmws_ws", f)).read().splitlines()[line-1].strip()
    print("SURVIVED", f, line, kind, "|", srcl[:110])
for d in work: shutil.rmtree(d, ignore_errors=True)
