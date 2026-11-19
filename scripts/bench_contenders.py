#!/usr/bin/env python3
"""compara against DuckDB and Polars on one keyed comparison: every number from this run.

    bench_contenders.py --compara BIN --gen BIN [--qsv BIN] [--rows 1m,10m]
                        [--formats csv,parquet] [--runs 3] [--work DIR] [--delete]
                        [--cache warm,cold] [--memory-max 13G] [--json OUT] [--notices]

Two generated tables of 20 columns a side (compara-gen-data, seed 7), keyed
on (account_id, txn_id), updated_at ignored, in each format asked for. Every
contender reports how many keys are unchanged, changed, added and removed,
and the run fails unless they all agree: the correctness gate.

Each contender gets compara's semantics: values compared as text in CSV and
by value in Parquet, an empty field equal only to an empty field, and a key
that appears twice in one file compared by its first row. Each run is a
process of its own, and every run starts from a stated page-cache state of
the files that tool reads (--cache): warm, each input read to its end just
before; cold, each input dropped from the page cache (posix_fadvise
DONTNEED; no root needed). Without that, what one tool leaves in memory
decides how warm the next one starts: a 10M-row run where Polars was killed
at 13 GB left compara's 3.7 GB of inputs evicted, and compara took 9.3 s
there against 0.7 s the round before. Each tool's number in each state is
the median of --runs (the median run's CPU (user + system), peak RSS and
peak private memory (RssAnon, sampled) beside it) with the fastest and
slowest; the first state in --cache is the headline. Each run also records
how much of its inputs was in the page cache when it started (mincore) and
its major page faults. Each round also times reading both inputs from a cold
cache (`disk_read_s`), the floor for any tool on a cold run. DuckDB and Polars run from Python with
the fastest way found for each (every core; DuckDB reads Parquet where it
is and loads CSV once; Polars' lazy API, which its streaming engine did not
beat here); their wall is timed
inside the process from the first read to the counts, so the interpreter's
start and the import are not charged to them. compara's is the whole process.
--qsv adds qsv (dathere/qsv, its release binary) on the CSV inputs only: `qsv diff`
reads CSV alone, and has no way to leave a column out, so the ignored column is
cut from both files first with `qsv select`, outside the timing (the page says
so). Only the `qsv diff` process is timed, writing its diff to a file as compara
does not; its counts are read back from that output afterwards (qsv diff lists
the rows that differ, so unchanged is the left file's rows, from `qsv count`,
less the changed and removed). qsv requires unique keys and the generated tables
repeat a few, so the same untimed step keeps each key's first row with
`qsv extdedup --select`.
A tool the kernel kills for memory is recorded as out of memory, with what
it held, and the run goes on; --memory-max gives each tool a cgroup with
that ceiling, so it is the tool that is stopped and not the machine.

--json writes the results; --notices also prints them as `bench json i/N`
notices (3,500 characters each), so the docs can be written from a run with
no artifact download: `scripts/gen_bench_docs.py <run id>`.
"""
import argparse
import ctypes
import ctypes.util
import datetime
import json
import mmap
import os
import re
import platform
import statistics
import subprocess
import sys
import threading
import time

KEY = ["account_id", "txn_id"]
IGNORE = "updated_at"

DUCKDB = r"""
import json, sys, time
import duckdb
a, b, fmt, key, ignore = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(","), sys.argv[5]
t0 = time.perf_counter()
con = duckdb.connect()
# Its progress bar, drawn for a query that runs a few seconds, goes to stdout
# and ran into the result line the harness reads.
con.execute("SET enable_progress_bar = false")
if fmt == "csv":
    # A CSV row has no number in DuckDB until it is in a table: load each
    # file once, then take each key's first row by rowid, which is file order.
    for t, p in (("ra", a), ("rb", b)):
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_csv('{p}', all_varchar = true)")
    src, order = {"a": "ra", "b": "rb"}, "rowid"
else:
    # Parquet is read where it is, each row numbered from the file.
    src = {s: f"read_parquet('{p}', file_row_number = true)" for s, p in (("a", a), ("b", b))}
    order = "file_row_number"
cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src['a']}").fetchall()]
cmp = [c for c in cols if c not in key and c != ignore and c != "file_row_number"]
first = f"QUALIFY row_number() OVER (PARTITION BY {', '.join(key)} ORDER BY {order}) = 1"
eq = " AND ".join(f"ta.{c} IS NOT DISTINCT FROM tb.{c}" for c in cmp)
on = " AND ".join(f"ta.{k} = tb.{k}" for k in key)
row = con.execute(f'''
  WITH ta AS (SELECT * FROM {src['a']} {first}), tb AS (SELECT * FROM {src['b']} {first})
  SELECT
    count(*) FILTER (ta.{key[0]} IS NOT NULL AND tb.{key[0]} IS NOT NULL AND ({eq})),
    count(*) FILTER (ta.{key[0]} IS NOT NULL AND tb.{key[0]} IS NOT NULL AND NOT ({eq})),
    count(*) FILTER (ta.{key[0]} IS NULL),
    count(*) FILTER (tb.{key[0]} IS NULL)
  FROM ta FULL OUTER JOIN tb ON {on}''').fetchone()
wall = time.perf_counter() - t0
print(json.dumps({"wall_s": wall, "counts": dict(zip(("unchanged", "changed", "added", "removed"), row))}))
"""

POLARS = r"""
import json, sys, time
import polars as pl
a, b, fmt, key, ignore = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(","), sys.argv[5]
t0 = time.perf_counter()
scan = (lambda p: pl.scan_csv(p, infer_schema=False)) if fmt == "csv" else pl.scan_parquet
la, lb = scan(a), scan(b)
cmp = [c for c in la.collect_schema().names() if c not in key and c != ignore]
la = la.unique(subset=key, keep="first")
lb = lb.unique(subset=key, keep="first")
j = la.join(lb, on=key, how="full", suffix="_b", coalesce=False)
in_a, in_b = pl.col(key[0]).is_not_null(), pl.col(key[0] + "_b").is_not_null()
diff = pl.any_horizontal([pl.col(c).ne_missing(pl.col(c + "_b")) for c in cmp])
r = j.select(
    unchanged=(in_a & in_b & ~diff).sum(), changed=(in_a & in_b & diff).sum(),
    added=(~in_a).sum(), removed=(~in_b).sum()).collect().row(0, named=True)
wall = time.perf_counter() - t0
print(json.dumps({"wall_s": wall, "counts": {k: int(v) for k, v in r.items()}}))
"""


def tree(pid):
    """pid and every process below it (sudo and systemd-run sit above the tool under --memory-max)."""
    out, todo = [], [pid]
    while todo:
        p = todo.pop()
        out.append(p)
        try:
            for t in os.listdir(f"/proc/{p}/task"):
                todo += [int(c) for c in open(f"/proc/{p}/task/{t}/children").read().split()]
        except OSError:
            pass
    return out


def private_kb(pids):
    """RssAnon summed over pids: the pages a process holds that are its own (heap, stacks,
    anonymous maps). Peak RSS also counts the pages of files it maps, which the kernel can
    drop and read again: compara maps its inputs, so its RSS is mostly the files."""
    total = 0
    for p in pids:
        try:
            for line in open(f"/proc/{p}/status"):
                if line.startswith("RssAnon:"):
                    total += int(line.split()[1])
                    break
        except OSError:
            pass
    return total


def evict(paths):
    """Drops each file's pages from the page cache: the file is synced first,
    as the kernel keeps dirty pages, then advised away."""
    for path in paths:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)


def preread(paths):
    """Reads each file to its end, so the run after it finds it in the page cache."""
    for path in paths:
        with open(path, "rb", buffering=0) as f:
            while f.read(8 << 20):
                pass


def set_cache(state, paths):
    evict(paths)
    if state == "warm":
        preread(paths)


def resident(paths):
    """The share of the files' pages in the page cache (mincore over a map of
    each), or None where that cannot be asked (not Linux)."""
    if not sys.platform.startswith("linux"):
        return None
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
    libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte)]
    page, have, total = mmap.PAGESIZE, 0, 0
    for path in paths:
        size = os.path.getsize(path)
        if not size:
            continue
        fd = os.open(path, os.O_RDONLY)
        try:
            addr = libc.mmap(None, size, mmap.PROT_READ, mmap.MAP_SHARED, fd, 0)
            if addr in (None, ctypes.c_void_p(-1).value):
                return None
            n = (size + page - 1) // page
            vec = (ctypes.c_ubyte * n)()
            ok = libc.mincore(addr, size, vec) == 0
            libc.munmap(addr, size)
            if not ok:
                return None
            have += sum(b & 1 for b in vec)
            total += n
        finally:
            os.close(fd)
    return round(have / total, 3) if total else 1.0


def disk_read(paths):
    """Seconds to read the files from a cold page cache: the floor for a cold run."""
    evict(paths)
    t0 = time.perf_counter()
    preread(paths)
    return time.perf_counter() - t0


def run_one(cmd, work):
    """One run of cmd in a process of its own: exit code, wall, CPU (user +
    system) and peak RSS in MB of that process alone (wait4), peak private
    memory in MB (RssAnon of its process tree, sampled every 5 ms from a
    thread of this harness: a lower bound, since a peak shorter than a sample
    can be missed), its major page faults (reads it waited on the disk for:
    wait4, its process and the children it waited for) and its stdout."""
    out_path = os.path.join(work, "run.out")
    peak = [0]
    with open(out_path, "w") as out, open(os.path.join(work, "run.err"), "w") as err:
        t0 = time.perf_counter()
        p = subprocess.Popen(cmd, stdout=out, stderr=err)
        done = threading.Event()

        def sample():
            while not done.wait(0.005):
                peak[0] = max(peak[0], private_kb(tree(p.pid)))

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        _, status, ru = os.wait4(p.pid, 0)
        wall = time.perf_counter() - t0
        done.set()
        sampler.join()
    p.returncode = os.waitstatus_to_exitcode(status)
    text = open(out_path).read()
    return p.returncode, wall, ru.ru_utime + ru.ru_stime, ru.ru_maxrss / 1024, peak[0] / 1024, ru.ru_majflt, text


def compara_cmd(binary, a, b, out_json):
    return [binary, "compare", a, b, "-k", ",".join(KEY), "-i", IGNORE, "--json", out_json]


def qsv_diff_cmd(binary, a, b):
    """qsv diff on two files whose ignored column is already gone. Its options,
    from `qsv diff --help`: -k names the key columns (the same in both files),
    -j defaults to every CPU, and the diff goes to stdout (run_one's file)."""
    return [binary, "diff", "-k", ",".join(KEY), a, b]


def qsv_prepare(binary, a, b, work):
    """Not timed. qsv diff has no option to skip a column and says its keys MUST be
    unique (the generated tables repeat a few), so each file goes through
    `qsv select '!IGNORE'` and then `qsv extdedup --select KEY`, which keeps a
    key's first row, as compara compares it. Returns the two new paths, their
    row counts and the seconds spent."""
    t0 = time.perf_counter()
    outs = []
    for src in (a, b):
        base = os.path.join(work, "qsv_" + os.path.basename(src))
        # qsv reads a file's format from its extension, so the cut keeps `.csv`.
        cut = os.path.join(work, "qsvcut_" + os.path.basename(src))
        subprocess.run([binary, "select", f"!{IGNORE}", "-o", cut, src], check=True)
        subprocess.run([binary, "extdedup", "--select", ",".join(KEY), cut, base], check=True)
        os.remove(cut)
        outs.append(base)
    rows = [int(subprocess.run([binary, "count", p], check=True, capture_output=True, text=True).stdout)
            for p in outs]
    return outs[0], outs[1], rows, time.perf_counter() - t0


def qsv_counts(out_path, rows_a, rows_b):
    """Counts from qsv diff's output: a header `diffresult,<columns>`, then `-` (left)
    and `+` (right) rows. A key with both is changed, only `-` removed, only `+`
    added; unchanged is what is left of the left file's rows."""
    import csv
    seen = {}
    with open(out_path, newline="") as f:
        rd = csv.reader(f)
        head = next(rd, None)
        if head is None:
            kx = None
        else:
            kx = [head.index(k) for k in KEY]
        for row in rd:
            k = tuple(row[i] for i in kx)
            seen[k] = seen.get(k, 0) | (1 if row[0] == "-" else 2)
    changed = sum(1 for v in seen.values() if v == 3)
    removed = sum(1 for v in seen.values() if v == 1)
    added = sum(1 for v in seen.values() if v == 2)
    unchanged = rows_a - changed - removed
    if unchanged + changed + added != rows_b:
        sys.exit(f"qsv: {unchanged}+{changed}+{added} rows do not add up to the right file's {rows_b}: "
                 "duplicate keys left after extdedup?")
    return {"unchanged": unchanged, "changed": changed, "added": added, "removed": removed}


def contender(name, binary, a, b, fmt, work, memory_max=None):
    if name == "compara":
        out_json = os.path.join(work, "compara-counts.json")
        cmd = compara_cmd(binary, a, b, out_json)
    elif name == "qsv":
        cmd = qsv_diff_cmd(binary, a, b)
    else:
        code = DUCKDB if name == "duckdb" else POLARS
        cmd = [sys.executable, "-c", code, a, b, fmt, ",".join(KEY), IGNORE]
    if memory_max:
        # A cgroup of its own with a memory ceiling: a tool that needs more
        # is stopped by the kernel alone, and the machine (a CI runner) goes on.
        cmd = ["sudo", "-n", "systemd-run", "--scope", "--quiet", "-p", f"MemoryMax={memory_max}",
               "-p", "MemorySwapMax=0", "--"] + cmd
    return cmd, (out_json if name == "compara" else None)


def last_result(out, name, fmt):
    """The result a Python contender prints last. Text a library writes to
    stdout without a line break (DuckDB's progress bar) can come before it on
    the same line, so the result is read from where it starts."""
    at = out.rfind('{"wall_s"')
    if at < 0:
        sys.exit(f"{name} printed no result on {fmt}: {out[-500:]}")
    return json.loads(out[at:].splitlines()[0])


def summary(runs):
    """A tool's runs in one cache state: the median run's wall, CPU and memory (the
    lower of the two middle runs when their number is even, so every figure is
    from one real run), the fastest and slowest wall, and how many runs."""
    walls = sorted(r["wall_s"] for r in runs)
    mid = statistics.median_low(walls)
    run = next(r for r in runs if r["wall_s"] == mid)
    return {"wall_s": mid, "wall_min_s": walls[0], "wall_max_s": walls[-1], "n": len(runs),
            **{k: run[k] for k in ("cpu_s", "peak_rss_mb", "peak_private_mb", "major_faults", "resident")}}


def bench(args, fmt, a, b):
    """Every tool on one pair, in interleaved rounds; in each round every cache
    state of --cache, and in each state every tool once, its inputs put in
    that state just before its run. A tool the kernel kills for memory
    (SIGKILL) is recorded as out of memory, with how much it held, and left
    out of the later runs; any other failure stops the run."""
    names = ["compara", "duckdb", "polars"]
    states = args.cache.split(",")
    cmds = {n: contender(n, args.compara, a, b, fmt, args.work, args.memory_max) for n in names}
    files = {n: [a, b] for n in names}
    qsv_prep = None
    if args.qsv and fmt == "csv":
        names.append("qsv")
        qa, qb, qrows, prep_s = qsv_prepare(args.qsv, a, b, args.work)
        qsv_prep = {"prep_s": round(prep_s, 3)}
        cmds["qsv"] = contender("qsv", args.qsv, qa, qb, fmt, args.work, args.memory_max)
        files["qsv"] = [qa, qb]
    runs = {n: [] for n in names}
    counts, failed, floor = {}, {}, []
    for i in range(args.runs):  # interleaved: each round runs every tool once in each state
        floor.append(round(disk_read([a, b]), 3))
        print(f"{fmt} run {i + 1} disk: both inputs read from a cold cache in {floor[-1]:.2f} s", flush=True)
        for state in states:
            for n in names:
                if n in failed:
                    continue
                cmd, out_json = cmds[n]
                set_cache(state, files[n])
                res = resident(files[n])
                code, wall, cpu, rss, anon, majflt, out = run_one(cmd, args.work)
                err = open(os.path.join(args.work, "run.err")).read()[-2000:]
                if code in (-9, 137) and n != "compara":
                    failed[n] = {"failed": "out of memory", "peak_rss_mb": round(rss), "wall_s": round(wall, 3),
                                 "cache": state}
                    print(f"{fmt} run {i + 1} {state} {n}: killed for memory after {wall:.1f} s at {rss:.0f} MB",
                          flush=True)
                    continue
                if n == "compara":
                    if code not in (0, 1):
                        sys.exit(f"compara exited {code} on {fmt}: {err}")
                    c = json.load(open(out_json))["counts"]
                    counts[n] = {k: c[k] for k in ("unchanged", "changed", "added", "removed")}
                elif n == "qsv":
                    if code != 0:
                        sys.exit(f"qsv failed on {fmt} (exit {code}): {err}")
                    counts[n] = qsv_counts(os.path.join(args.work, "run.out"), qrows[0], qrows[1])
                else:
                    if code != 0:
                        sys.exit(f"{n} failed on {fmt} (exit {code}): {err}")
                    r = last_result(out, n, fmt)
                    wall = r["wall_s"]
                    counts[n] = r["counts"]
                runs[n].append({"cache": state, "resident": res, "wall_s": round(wall, 3), "cpu_s": round(cpu, 3),
                                "peak_rss_mb": round(rss), "peak_private_mb": round(anon), "major_faults": majflt})
                print(f"{fmt} run {i + 1} {state} {n}: {wall:.2f} s wall, {cpu:.2f} s CPU, {rss:.0f} MB "
                      f"({anon:.0f} private), {majflt} major faults, {res} of inputs cached, {counts[n]}", flush=True)
    results = {}
    for n in names:
        if n in failed:
            results[n] = failed[n]
            continue
        if counts[n] != counts["compara"]:
            sys.exit(f"correctness gate: {n} counts {counts[n]} on {fmt}, compara {counts['compara']}")
        by = {st: summary([r for r in runs[n] if r["cache"] == st]) for st in states}
        # The headline is the first state's median; every state's is kept under "cache".
        results[n] = {**by[states[0]], "cache": by, "runs": runs[n], "counts": counts[n]}
        if n == "qsv":
            results[n].update(qsv_prep)  # the select and count before the timing, for the record
    if qsv_prep:
        for p in (qa, qb):
            os.remove(p)
    return results, floor


def versions(args):
    v = {"compara": subprocess.run([args.compara, "--version"], capture_output=True, text=True).stdout.strip()}
    for mod in ("duckdb", "polars"):
        v[mod] = subprocess.run([sys.executable, "-c", f"import {mod}; print({mod}.__version__)"],
                                capture_output=True, text=True).stdout.strip()
    if args.qsv:
        v["qsv"] = subprocess.run([args.qsv, "--version"], capture_output=True, text=True).stdout.strip()
    v["python"] = platform.python_version()
    return v


def env():
    cpu = next((line.split(":", 1)[1].strip() for line in open("/proc/cpuinfo") if line.startswith("model name")),
               platform.processor())
    mem = next((int(line.split()[1]) // 1024 // 1024 for line in open("/proc/meminfo") if line.startswith("MemTotal")), 0)
    return {"cpu": cpu, "cores": os.cpu_count(), "memory_gb": mem, "os": platform.platform(terse=True),
            "runner": runner_image()}


def runner_image():
    """The runner image this run is on, as GitHub's image names it
    (ImageOS ubuntu24 -> ubuntu-24.04, plus ImageVersion), or None off
    GitHub. The docs print it: `ubuntu-latest` moves between images."""
    name, version = os.environ.get("ImageOS", ""), os.environ.get("ImageVersion", "")
    m = re.fullmatch(r"ubuntu(\d\d)", name)
    name = f"ubuntu-{m.group(1)}.04" if m else name
    return f"{name}, image {version}" if name and version else name or None


def notices(doc, title="bench json"):
    text = json.dumps(doc, separators=(",", ":"))
    # No piece starts or ends with a space, so a reader that trims each one
    # still joins them into the same document.
    parts, i = [], 0
    while i < len(text):
        j = min(i + 3500, len(text))
        while j < len(text) and j > i + 1 and (text[j - 1] == " " or text[j] == " "):
            j -= 1
        parts.append(text[i:j])
        i = j
    if len(parts) > 10:
        sys.exit(f"::error::{title}: {len(parts)} notices needed, GitHub keeps 10 per step")
    for n, p in enumerate(parts, 1):
        msg = p.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::notice title={title} {n}/{len(parts)}::{msg}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--compara", required=True)
    ap.add_argument("--gen", required=True, help="compara-gen-data")
    ap.add_argument("--qsv", help="qsv's release binary: adds qsv diff on the CSV inputs")
    ap.add_argument("--rows", default="1m,10m", help="sizes, smallest first: 1m,10m")
    ap.add_argument("--delete", action="store_true", help="delete each pair once measured")
    ap.add_argument("--memory-max", help="run every tool in a cgroup with this MemoryMax (e.g. 13G; needs sudo and systemd)")
    ap.add_argument("--formats", default="csv,parquet")
    ap.add_argument("--runs", type=int, default=3, help="rounds; each tool's number is its median")
    ap.add_argument("--cache", default="warm,cold",
                    help="page-cache states each tool runs in every round, the first the headline: warm,cold")
    ap.add_argument("--work", default="bench-data")
    ap.add_argument("--json")
    ap.add_argument("--notices", action="store_true")
    args = ap.parse_args()
    if not args.cache or set(args.cache.split(",")) - {"warm", "cold"}:
        sys.exit(f"--cache takes warm, cold or both, not {args.cache!r}")
    args.compara = os.path.abspath(args.compara)
    if args.qsv:
        args.qsv = os.path.abspath(args.qsv)
    os.makedirs(args.work, exist_ok=True)
    doc = {"generated_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "run_id": os.environ.get("GITHUB_RUN_ID"), "commit": os.environ.get("GITHUB_SHA"),
           "columns": 20, "key": KEY, "ignore": IGNORE, "runs": args.runs, "cache": args.cache.split(","),
           "env": env(), "versions": versions(args), "inputs": []}
    for rows in args.rows.split(","):
        for fmt in args.formats.split(","):
            a, b = (os.path.join(args.work, f"{rows}_{s}.{fmt}") for s in "ab")
            if not (os.path.exists(a) and os.path.exists(b)):
                subprocess.run([args.gen, "--rows", rows, "--out-dir", args.work, "--prefix", rows, "-f", fmt],
                               check=True)
            tools, floor = bench(args, fmt, a, b)
            doc["inputs"].append({"rows": rows, "format": fmt, "bytes": os.path.getsize(a) + os.path.getsize(b),
                                  "disk_read_s": statistics.median(floor), "disk_read_runs": floor,
                                  "tools": tools})
            for f in (a, b):  # the next pair needs the disk
                if args.delete:
                    os.remove(f)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(doc, f, indent=1)
    if args.notices:
        notices(doc)


if __name__ == "__main__":
    main()
