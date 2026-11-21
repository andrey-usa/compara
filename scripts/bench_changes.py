#!/usr/bin/env python3
"""compara's measured changes since a release: this build against the release's, every number from this run.

    bench_changes.py --head BIN --base BIN [--lookup-base BIN] --gen BIN
                     [--claims sortmerge,wide,names,escaped,stall,put] [--rows 10m]
                     [--memory 512M] [--runs 5] [--fault-runs 3] [--cap 150]
                     [--work DIR] [--delete] [--json OUT] [--notices]

Claims of a version's CHANGELOG, each as base (the last release) and head (this
commit) on the same inputs, in interleaved rounds, the order alternating from
round to round:

  sortmerge  a CSV pair of --rows rows a side (compara-gen-data, as bench-contenders), keyed
             on (account_id, txn_id), updated_at ignored, compared in a cgroup of --memory: under
             a heap limit its key indexes cannot fit, it goes to the sortmerge engine. Cold page
             cache: each input dropped (posix_fadvise DONTNEED) just before each run.
  wide       a sparse JSON table, 200,000 objects of id and 2 of 20,000 keys each (one row in
             twenty changed in B), compared with -c naming all 20,000 columns. The release
             refuses this table as "not one table" (its refusal is a bound on the shape, not
             the cells), so the cost of the name check is taken against --lookup-base, a build
             of the commit before the lookup: it runs the same compare with the refusal
             lifted. The release is run too, and its refusal recorded. Warm cache.
  names      300 lines of {"id":N,"<1 MB name>N":1} compared with itself, at --threads 1 and 4,
             and under --memory 64M: the file is refused for its key names, and what the run
             held before that is the peak RSS. Warm cache.
  escaped    the instructions (valgrind's cachegrind, scripts/icount.py, one thread) of a
             compare of compara-gen-data's 200k NDJSON pair with `note` spelled `not\u00e9`
             in every row, as Python's json.dumps writes noté, -k account_id,txn_id -i
             updated_at (updated_at differs in every row). One run a build: the count
             repeats to within hundredths of a percent.
  stall      a URL pair whose server (tests/object_server.py --fault stall, every request
             from the first) sends headers and then nothing, over plain HTTP and as S3
             (AWS_ENDPOINT_URL), at the default read timeout: the time to exit and how.
  put        `--export-dir` to a plain http:// URL (a presigned PUT) whose server fails
             every PUT (object_server.py --fault-puts): reads 19 KB and never answers;
             never reads 398 KB; reads 398 KB at 32 KB/s; reads it at 1 KB/s. The time to
             exit and how, at the default read timeout; at 32 KB/s the upload is checked
             to hold B's rows.

The stall and put runs wait on timers, not on work: --fault-runs rounds of them
(median and range), and a run still going after --cap seconds is killed and
recorded as no end; a build that did not end is not run again on that case.

Each run is a process of its own. Wall is the whole process, CPU is user + system
and peak RSS is that of the process alone (wait4); a cgroup, where there is one, is
the tool's own. A claim's number is the median of --runs (the median run's CPU, RSS
and major page faults beside it) with the fastest and slowest wall, and the cache
state is stated beside it with the share of the inputs found in the page cache
when the run started (mincore). A run's exit code and counts (or refusal) are
checked against its claim, and the claims' correctness gate fails the run when
base and head disagree on counts.

--json writes the results; --notices also prints them as `bench json i/N` notices
(3,500 characters each, as bench_contenders.py does), so the CHANGELOG can cite them
with no artifact download.
"""
import argparse
import datetime
import json
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_contenders import env, evict, notices, preread, resident  # noqa: E402

KEY = ["account_id", "txn_id"]
IGNORE = "updated_at"
SPARSE_ROWS, SPARSE_KEYS, SPARSE_PER, SPARSE_CHANGE = 200_000, 20_000, 2, 0.05
BIGKEY_LINES, BIGKEY_BYTES = 300, 1 << 20
ESC_ROWS = "200k"
SERVER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tests", "object_server.py")
ICOUNT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "icount.py")
WORK = ""  # the directory the inputs are in, kept out of the refusals printed


def size_bytes(text):
    n = text.strip().lower().rstrip("b").rstrip("i")
    for suffix, shift in (("k", 10), ("m", 20), ("g", 30)):
        if n.endswith(suffix):
            return int(n[:-1]) << shift
    return int(n)


class Cap:
    """A memory ceiling for one process, as a container has: a cgroup of its own with that
    limit, which compara reads (its heap limit is the cgroup's). On a GitHub runner that is
    `systemd-run --scope -p MemoryMax` (as bench_contenders.py); where there is no systemd
    but a writable cgroup v1 (a sandbox), a child of this process's own cgroup."""

    def __init__(self, limit):
        self.limit = limit
        self.bytes = size_bytes(limit)
        self.dir = None
        if shutil.which("systemd-run") and subprocess.run(
                ["sudo", "-n", "systemd-run", "--scope", "--quiet", "-p", f"MemoryMax={limit}", "true"],
                capture_output=True).returncode == 0:
            self.kind = "systemd-run MemoryMax"
            return
        for line in open("/proc/self/cgroup"):
            _, controllers, path = line.rstrip("\n").split(":", 2)
            if "memory" in controllers.split(","):
                root = "/sys/fs/cgroup/memory"
                self.dir = os.path.join(root, path.strip("/"), f"bench-changes-{os.getpid()}")
                os.makedirs(self.dir)
                self.write("memory.limit_in_bytes", self.bytes)
                if os.path.exists(os.path.join(self.dir, "memory.memsw.limit_in_bytes")):
                    self.write("memory.memsw.limit_in_bytes", self.bytes)
                self.kind = "cgroup v1 memory.limit_in_bytes"
                return
        sys.exit("no way to give a process a memory ceiling here (no systemd-run through sudo, no cgroup v1 memory)")

    def write(self, name, value):
        with open(os.path.join(self.dir, name), "w") as f:
            f.write(str(value))

    def wrap(self, cmd):
        if self.dir:
            return cmd
        return ["sudo", "-n", "systemd-run", "--scope", "--quiet", "-p", f"MemoryMax={self.limit}",
                "-p", "MemorySwapMax=0", "--"] + cmd

    def enter(self):
        """preexec_fn: the child moves itself into the cgroup before it execs."""
        if self.dir:
            path = os.path.join(self.dir, "cgroup.procs")
            return lambda: open(path, "w").write(str(os.getpid()))
        return None

    def close(self):
        if self.dir:
            try:
                os.rmdir(self.dir)
            except OSError:
                pass


def run_one(cmd, work, cap=None):
    """One run in a process of its own: exit code, wall, CPU (user + system), peak RSS in MB
    (wait4, that process and what it waited for), major page faults, stdout and stderr."""
    out_path, err_path = os.path.join(work, "run.out"), os.path.join(work, "run.err")
    if cap:
        pre, cmd = cap.enter(), cap.wrap(cmd)
    else:
        pre = None
    with open(out_path, "w") as out, open(err_path, "w") as err:
        t0 = time.perf_counter()
        p = subprocess.Popen(cmd, stdout=out, stderr=err, preexec_fn=pre)
        _, status, ru = os.wait4(p.pid, 0)
        wall = time.perf_counter() - t0
    code = os.waitstatus_to_exitcode(status)
    return {"exit": code, "wall_s": round(wall, 3), "cpu_s": round(ru.ru_utime + ru.ru_stime, 3),
            "peak_rss_mb": round(ru.ru_maxrss / 1024), "major_faults": ru.ru_majflt,
            "stdout": open(out_path).read(), "stderr": open(err_path).read()}


def set_cache(state, paths):
    evict(paths)
    if state == "warm":
        preread(paths)


def summary(runs):
    """One build's runs of one case: the median run's wall, CPU, peak RSS and faults (the lower
    of the two middle runs when their number is even, so every figure is from one real run),
    the fastest and slowest wall, the RSS range, and how many runs."""
    walls = sorted(r["wall_s"] for r in runs)
    mid = statistics.median_low(walls)
    run = next(r for r in runs if r["wall_s"] == mid)
    rss = sorted(r["peak_rss_mb"] for r in runs)
    return {"wall_s": mid, "wall_min_s": walls[0], "wall_max_s": walls[-1], "n": len(runs),
            "cpu_s": run["cpu_s"], "peak_rss_mb": run["peak_rss_mb"], "rss_min_mb": rss[0], "rss_max_mb": rss[-1],
            "major_faults": run["major_faults"], "exit": run["exit"],
            "resident": run["resident"], "result": run["result"], "engine": run.get("engine")}


def interleaved(builds, case, args, files, state, cap=None):
    """Every build once a round, --runs rounds, the order alternating so neither always goes
    first; its inputs put in the stated page-cache state just before each run. `case` makes the
    command for a build and reads what it printed into the checked result."""
    runs = {name: [] for name in builds}
    for i in range(args.runs):
        for name in (list(builds) if i % 2 == 0 else list(builds)[::-1]):
            set_cache(state, files)
            res = resident(files)
            cmd = case["cmd"](builds[name])
            r = run_one(cmd, args.work, cap if case.get("capped") else None)
            r["resident"] = res
            r["result"] = case["read"](name, r)
            if case["name"] == "sortmerge":
                # The note compara prints says which engine answered (sortmerge once auto fell back to it).
                r["engine"] = "sortmerge" if "sortmerge" in r["stdout"] + r["stderr"] else "turbo"
            runs[name].append(r)
            print(f"{case['name']} round {i + 1} {name}: exit {r['exit']}, {r['wall_s']:.2f} s wall, {r['cpu_s']:.2f} s CPU, "
                  f"{r['peak_rss_mb']} MB, {r['major_faults']} major faults, {res} of inputs cached, {r.get('engine') or ''} {r['result']}",
                  flush=True)
    return {name: {**summary(rs), "runs": [{k: v for k, v in r.items() if k not in ("stdout", "stderr", "result")}
                                          for r in rs]} for name, rs in runs.items()}


def counts_of(path):
    c = json.load(open(path))["counts"]
    return {k: c[k] for k in ("unchanged", "changed", "added", "removed")}


def refusal(r):
    """The first line of what a refused run said, as stderr gave it."""
    lines = [ln for ln in r["stderr"].splitlines() if ln.strip()]
    return lines[0].replace(WORK + "/", "")[:200] if lines else ""


def agree(case, results, reference, builds):
    """The correctness gate: the builds that finished a compare found the same counts."""
    seen = {n: r["result"] for n, r in results.items() if n in builds and isinstance(r["result"], dict)}
    for n, c in seen.items():
        if c != seen[reference]:
            sys.exit(f"correctness gate: {case} counts {c} on {n}, {seen[reference]} on {reference}")


def claim_sortmerge(args, builds):
    a, b = (os.path.join(args.work, f"{args.rows}_{s}.csv") for s in "ab")
    if not (os.path.exists(a) and os.path.exists(b)):
        subprocess.run([args.gen, "--rows", args.rows, "--out-dir", args.work, "--prefix", args.rows, "-f", "csv"],
                       check=True)
    cap = Cap(args.memory)
    # Each claim its own counts file: a capped run goes through sudo, so the
    # file it leaves is root's, and a later uncapped run could not write it.
    out_json = os.path.join(args.work, "counts-sortmerge.json")
    try:
        case = {"name": "sortmerge", "capped": True,
                "cmd": lambda bin_: [bin_, "compare", a, b, "-k", ",".join(KEY), "-i", IGNORE, "--json", out_json],
                "read": lambda n, r: (counts_of(out_json) if r["exit"] in (0, 1) else
                                      sys.exit(f"{n} exited {r['exit']}: {r['stderr'][-500:]}"))}
        res = interleaved({n: builds[n] for n in ("base", "head")}, case, args, [a, b], "cold", cap)
    finally:
        cap.close()
    agree("sortmerge", res, "base", ("base", "head"))
    out = {"claim": "sortmerge under a memory limit", "inputs": {"rows_a_side": args.rows, "format": "csv",
           "bytes": os.path.getsize(a) + os.path.getsize(b), "limit": args.memory, "limit_by": cap.kind},
           "cache": "cold", "builds": res}
    if args.delete:
        for f in (a, b):
            os.remove(f)
    return out


def gen_sparse(a, b):
    """200,000 objects of id and 2 of 20,000 keys (seed 7); one row in twenty differs in B.
    Returns the names the table has."""
    rng = random.Random(7)
    seen = set()
    with open(a, "w") as fa, open(b, "w") as fb:
        for i in range(SPARSE_ROWS):
            ks = rng.sample(range(SPARSE_KEYS), SPARSE_PER)
            seen.update(ks)
            row = '{"id":%d,%s}\n' % (i, ",".join('"f%d":"x"' % k for k in ks))
            fa.write(row)
            if rng.random() < SPARSE_CHANGE:
                ks2 = list(ks)
                ks2[0] = rng.randrange(SPARSE_KEYS)
                fb.write('{"id":%d,%s}\n' % (i, ",".join('"f%d":"%s"' % (k, "y" if j == 0 else "x")
                                                        for j, k in enumerate(ks2))))
            else:
                fb.write(row)
    return sorted(seen)


def claim_wide(args, builds):
    a, b = (os.path.join(args.work, f"sparse_{s}.ndjson") for s in "ab")
    names = [f"f{k}" for k in gen_sparse(a, b)]
    if len(names) != SPARSE_KEYS:
        sys.exit(f"the sparse table names {len(names)} columns, not {SPARSE_KEYS}")
    # An argument is capped at 128 KB; -c given again adds to the list.
    cols = []
    for i in range(0, len(names), 2000):
        cols += ["-c", ",".join(names[i:i + 2000])]
    out_json = os.path.join(args.work, "counts-wide.json")

    def read(n, r):
        if r["exit"] in (0, 1):
            return counts_of(out_json)
        return {"refused": refusal(r)}

    case = {"name": "wide", "cmd": lambda bin_: [bin_, "compare", a, b, "-k", "id", *cols, "--json", out_json],
            "read": read}
    res = interleaved({n: builds[n] for n in ("base", "lookup-base", "head") if n in builds}, case, args,
                      [a, b], "warm")
    agree("wide", res, "lookup-base" if "lookup-base" in res else "head", ("lookup-base", "head"))
    out = {"claim": "-c naming all 20,000 columns of a sparse JSON table",
           "inputs": {"objects": SPARSE_ROWS, "columns": SPARSE_KEYS, "keys_per_object": SPARSE_PER,
                      "rows_changed": SPARSE_CHANGE, "bytes": os.path.getsize(a), "columns_named": len(names)},
           "cache": "warm", "builds": res}
    for f in (a, b):
        os.remove(f)
    return out


def claim_names(args, builds):
    f = os.path.join(args.work, "bigkeys.ndjson")
    pad = "Q" * BIGKEY_BYTES
    with open(f, "w") as fh:
        for i in range(BIGKEY_LINES):
            fh.write('{"id":%d,"%s%d":1}\n' % (i, pad, i))
    res = {}
    for label, extra in (("threads 1", ["--threads", "1"]), ("threads 4", ["--threads", "4"]),
                         ("threads 1, --memory 64M", ["--threads", "1", "--memory", "64M"]),
                         ("threads 4, --memory 64M", ["--threads", "4", "--memory", "64M"])):
        case = {"name": f"names {label}",
                "cmd": lambda bin_, extra=extra: [bin_, "compare", f, f, "-k", "id", *extra],
                "read": lambda n, r: {"refused": refusal(r)} if r["exit"] == 2 else {"exit": r["exit"]}}
        res[label] = interleaved({n: builds[n] for n in ("base", "head")}, case, args, [f], "warm")
        for n in res[label]:
            if res[label][n]["exit"] != 2:
                sys.exit(f"names {label}: {n} exited {res[label][n]['exit']}, expected the refusal (2)")
    out = {"claim": "JSON key-name budget: peak RSS before the refusal",
           "inputs": {"lines": BIGKEY_LINES, "name_bytes": BIGKEY_BYTES, "bytes": os.path.getsize(f)},
           "cache": "warm", "cases": res}
    os.remove(f)
    return out


def claim_escaped(args, builds):
    a, b = (os.path.join(args.work, f"esc_{s}.ndjson") for s in "ab")
    subprocess.run([args.gen, "--rows", ESC_ROWS, "--out-dir", args.work, "--prefix", "esc", "-f", "ndjson"],
                   check=True, stdout=subprocess.DEVNULL)
    for f in (a, b):
        with open(f, "rb") as fh:
            text = fh.read()
        n = text.count(b'"note":')
        if n < text.count(b"\n"):
            sys.exit(f"escaped: {os.path.basename(f)} names note in {n} of its rows, not every one")
        with open(f, "wb") as fh:
            fh.write(text.replace(b'"note":', b'"not\\u00e9":'))
    out_json = os.path.join(args.work, "counts-escaped.json")
    res = {}
    for name in ("base", "head"):
        cmd = [sys.executable, ICOUNT, "--json", "--counter", "cachegrind", "--",
               builds[name], "compare", a, b, "-k", ",".join(KEY), "-i", IGNORE, "--threads", "1", "--json", out_json]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode == 77:
            sys.exit("escaped: no valgrind here to count instructions (apt-get install valgrind)")
        if p.returncode not in (0, 1):
            sys.exit(f"escaped: {name} exited {p.returncode}: {p.stderr[-500:]}")
        count = json.loads(p.stdout.strip().splitlines()[-1])
        res[name] = {"instructions": count["instructions"], "counter": count["counter"], "exit": p.returncode,
                     "result": counts_of(out_json)}
        print(f"escaped {name}: {count['instructions']:,} instructions ({count['counter']}), exit {p.returncode}, "
              f"{res[name]['result']}", flush=True)
    if res["base"]["result"] != res["head"]["result"]:
        sys.exit(f"correctness gate: escaped counts {res['head']['result']} on head, {res['base']['result']} on base")
    out = {"claim": "JSON names written with an escape: instructions, one thread",
           "inputs": {"rows": ESC_ROWS, "format": "ndjson", "name": "note written not\\u00e9 in every row",
                      "bytes": os.path.getsize(a) + os.path.getsize(b)},
           "args": f"-k {','.join(KEY)} -i {IGNORE} --threads 1", "builds": res}
    for f in (a, b):
        os.remove(f)
    return out


def serve(root, fault):
    """tests/object_server.py on `root` with `fault`, in a process group of its own; its URL."""
    srv = subprocess.Popen([sys.executable, SERVER, "--root", root, *fault], stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, text=True, start_new_session=True)
    line = srv.stdout.readline().split()
    if len(line) != 2 or line[0] != "LISTENING":
        srv.kill()
        sys.exit(f"the object server did not start: {line}")
    return srv, line[1]


def run_capped(cmd, env, cap):
    """One run, killed (its whole process group) when it is still going after `cap` seconds:
    exit code or None, wall, and the last line it wrote to stderr."""
    err_path = os.path.join(WORK, "run.err")
    with open(err_path, "w") as err:
        t0 = time.perf_counter()
        p = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL, stderr=err, start_new_session=True)
        code = None
        while time.perf_counter() - t0 < cap:
            pid, status = os.waitpid(p.pid, os.WNOHANG)
            if pid:
                code = os.waitstatus_to_exitcode(status)
                break
            time.sleep(0.05)
        wall = time.perf_counter() - t0
        if code is None:
            os.killpg(p.pid, 9)
            p.wait()
    lines = [ln for ln in open(err_path).read().splitlines() if ln.strip()]
    return code, round(wall, 2), (lines[-1] if lines else "")


def fault_env(extra=()):
    """The environment of a fault run: no read timeout or credentials of the caller's, no progress line."""
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("COMPARA_", "AWS_", "AZURE_", "GOOGLE_"))}
    env["COMPARA_PROGRESS"] = "0"
    env.update(extra)
    return env


def fault_rounds(name, builds, cases, args):
    """--fault-runs rounds of every case, the builds' order alternating. A case is (label, setup)
    where setup() starts the server and returns (server, cmd, env, check, root); check(code)
    says what the run did, or ends the job when head did not do what the claim says."""
    runs = {label: {n: [] for n in builds} for label, _ in cases}
    for i in range(args.fault_runs):
        for label, setup in cases:
            for n in (list(builds) if i % 2 == 0 else list(builds)[::-1]):
                if runs[label][n] and runs[label][n][-1]["exit"] is None:
                    continue
                srv, cmd, env, check, root = setup(builds[n])
                try:
                    code, wall, last = run_capped(cmd, env, args.cap)
                finally:
                    os.killpg(srv.pid, 9)
                    srv.wait()
                r = {"exit": code, "wall_s": wall, "said": re.sub(r"https?://127\.0\.0\.1:\d+", "URL", last)[:240],
                     "result": check(n, code, last)}
                if root:
                    shutil.rmtree(root, ignore_errors=True)
                runs[label][n].append(r)
                print(f"{name} {label} round {i + 1} {n}: "
                      f"{'no end, killed' if code is None else f'exit {code}'} at {wall:.1f} s; {r['result']}; {r['said']}",
                      flush=True)
    out = {}
    for label, by in runs.items():
        out[label] = {}
        for n, rs in by.items():
            walls = sorted(r["wall_s"] for r in rs)
            mid = statistics.median_low(walls)
            run = next(r for r in rs if r["wall_s"] == mid)
            out[label][n] = {"wall_s": mid, "wall_min_s": walls[0], "wall_max_s": walls[-1], "n": len(rs),
                             "exit": run["exit"], "said": run["said"], "result": run["result"]}
    return out


def claim_stall(args, builds):
    data = os.path.join(args.work, "stall")
    os.makedirs(os.path.join(data, "bkt"), exist_ok=True)
    subprocess.run([args.gen, "--rows", "20k", "--out-dir", os.path.join(data, "bkt"), "--prefix", "t", "-f", "csv"],
                   check=True, stdout=subprocess.DEVNULL)
    s3 = {"AWS_ENDPOINT_URL": None, "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
          "AWS_REGION": "us-east-1", "AWS_EC2_METADATA_DISABLED": "true"}

    def check(n, code, last):
        if n == "head" and (code != 2 or "stalled" not in last and "sent nothing" not in last):
            sys.exit(f"stall: head {'did not end' if code is None else f'exited {code}'}: {last}")
        return "no end" if code is None else ("error" if code == 2 else f"exit {code}")

    def setup(over_s3):
        def go(binary):
            srv, url = serve(data, ["--fault", "stall", "--fault-at", "0", "--fault-every", "1"])
            if over_s3:
                env = fault_env({**s3, "AWS_ENDPOINT_URL": url})
                a, b = "s3://bkt/t_a.csv", "s3://bkt/t_b.csv"
            else:
                env = fault_env()
                a, b = f"{url}/bkt/t_a.csv", f"{url}/bkt/t_b.csv"
            return srv, [binary, "compare", a, b, "-k", ",".join(KEY), "-i", IGNORE], env, check, None
        return go

    res = fault_rounds("stall", {n: builds[n] for n in ("base", "head")},
                       [("http", setup(False)), ("s3", setup(True))], args)
    shutil.rmtree(data, ignore_errors=True)
    return {"claim": "a URL whose server stalls every request, at the default read timeout",
            "inputs": {"rows_a_side": "20k", "format": "csv", "fault": "stall: headers, then nothing; every request"},
            "cap_s": args.cap, "cases": res}


PUT_SIZES = {"19 KB": 400, "398 KB": 8000}


def claim_put(args, builds):
    data = os.path.join(args.work, "put")
    pairs = {}
    for size, rows in PUT_SIZES.items():
        d = os.path.join(data, str(rows))
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "a.csv"), "w") as f:
            f.write("id,val\n")
        with open(os.path.join(d, "b.csv"), "w") as f:
            f.write("id,val\n")
            for i in range(rows):
                f.write(f"{i},{'v' * 40}{i}\n")
        pairs[size] = d

    def setup(size, fault, expect):
        pair = pairs[size]

        def go(binary):
            root = os.path.join(data, f"root-{os.getpid()}-{time.monotonic_ns()}")
            os.makedirs(os.path.join(root, "bkt"))
            srv, url = serve(root, [*fault, "--fault-puts", "--fault-every", "1"])
            up = os.path.join(root, "bkt", "out", "added.csv")

            def check(n, code, last):
                if code == 1:
                    with open(up, "rb") as f, open(os.path.join(pair, "b.csv"), "rb") as g:
                        if sorted(f.read().splitlines()) != sorted(g.read().splitlines()):
                            sys.exit(f"put: {n} exited 1 but the upload is not B's rows")
                    got = "uploaded"
                else:
                    got = "no end" if code is None else ("error" if code == 2 else f"exit {code}")
                if n == "head" and got != expect:
                    sys.exit(f"put: head {got}, the claim is {expect}: {last}")
                return got
            cmd = [binary, "compare", os.path.join(pair, "a.csv"), os.path.join(pair, "b.csv"), "-k", "id",
                   "--export-dir", f"{url}/bkt/out/"]
            return srv, cmd, fault_env(), check, root
        return go

    cases = [("19 KB read, never answered", setup("19 KB", ["--fault", "stall"], "error")),
             ("398 KB never read", setup("398 KB", ["--fault", "silent"], "error")),
             ("398 KB read at 32 KB/s", setup("398 KB", ["--fault", "dribble", "--rate", "32768"], "uploaded")),
             ("398 KB read at 1 KB/s", setup("398 KB", ["--fault", "dribble", "--rate", "1024"], "error"))]
    sizes = {s: f"{r} rows, {os.path.getsize(os.path.join(pairs[s], 'b.csv')):,} bytes" for s, r in PUT_SIZES.items()}
    res = fault_rounds("put", {n: builds[n] for n in ("base", "head")}, cases, args)
    shutil.rmtree(data, ignore_errors=True)
    return {"claim": "a plain http:// PUT (--export-dir to a URL) to a server that stops or reads slowly",
            "inputs": {"added.csv": sizes}, "cap_s": args.cap, "cases": res}


CLAIMS = {"sortmerge": claim_sortmerge, "wide": claim_wide, "names": claim_names,
          "escaped": claim_escaped, "stall": claim_stall, "put": claim_put}


def build_versions(builds):
    v = {}
    for name, binary in builds.items():
        v[name] = subprocess.run([binary, "--version"], capture_output=True, text=True).stdout.strip()
    return v


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--head", required=True, help="this commit's compara")
    ap.add_argument("--base", required=True, help="the last release's compara")
    ap.add_argument("--lookup-base", help="compara built at the commit before the column-name lookup (claim wide)")
    ap.add_argument("--gen", required=True, help="compara-gen-data (head's)")
    ap.add_argument("--claims", default=",".join(CLAIMS))
    ap.add_argument("--rows", default="10m", help="rows a side for sortmerge, as compara-gen-data takes them")
    ap.add_argument("--memory", default="512M", help="the cgroup's MemoryMax for sortmerge")
    ap.add_argument("--runs", type=int, default=5, help="rounds; each number is the median")
    ap.add_argument("--fault-runs", type=int, default=3, help="rounds of the stall and put runs")
    ap.add_argument("--cap", type=float, default=150, help="seconds a stall or put run is given before it is killed")
    ap.add_argument("--work", default="bench-changes-data")
    ap.add_argument("--delete", action="store_true", help="delete the CSV pair once measured")
    ap.add_argument("--json")
    ap.add_argument("--notices", action="store_true")
    args = ap.parse_args()
    global WORK
    WORK = os.path.abspath(args.work)
    args.work = WORK
    claims = args.claims.split(",")
    if set(claims) - set(CLAIMS):
        sys.exit(f"--claims takes {', '.join(CLAIMS)}, not {args.claims!r}")
    if "wide" in claims and not args.lookup_base:
        sys.exit("claim wide needs --lookup-base (the build the lookup is measured against)")
    builds = {"base": os.path.abspath(args.base), "head": os.path.abspath(args.head)}
    if args.lookup_base:
        builds["lookup-base"] = os.path.abspath(args.lookup_base)
    args.gen = os.path.abspath(args.gen)
    os.makedirs(args.work, exist_ok=True)
    doc = {"generated_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "run_id": os.environ.get("GITHUB_RUN_ID"), "commit": os.environ.get("GITHUB_SHA"),
           "runs": args.runs, "env": env(), "versions": build_versions(builds),
           "builds_note": {"base": "the last release", "head": "this commit",
                           "lookup-base": "the commit before the column-name lookup, the refusal of a sparse table already lifted"},
           "claims": {}}
    for name in claims:
        doc["claims"][name] = CLAIMS[name](args, builds)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(doc, f, indent=1)
    if args.notices:
        notices(doc)


if __name__ == "__main__":
    main()
