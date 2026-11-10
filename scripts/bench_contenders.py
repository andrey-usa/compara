#!/usr/bin/env python3
"""compara against DuckDB and Polars on one keyed comparison: every number from this run.

    bench_contenders.py --compara BIN --gen BIN [--rows 1m,10m] [--formats csv,parquet]
                        [--runs 3] [--work DIR] [--delete] [--memory-max 13G]
                        [--json OUT] [--notices]

Two generated tables of 20 columns a side (compara-gen-data, seed 7), keyed
on (account_id, txn_id), updated_at ignored, in each format asked for. Every
contender reports how many keys are unchanged, changed, added and removed,
and the run fails unless they all agree: the correctness gate.

Each contender gets compara's semantics: values compared as text in CSV and
by value in Parquet, an empty field equal only to an empty field, and a key
that appears twice in one file compared by its first row. Each run is a
process of its own; wall is the best of --runs, and that run's CPU (user +
system) and peak RSS go beside it. DuckDB and Polars run from Python with
the fastest way found for each (every core; DuckDB reads Parquet where it
is and loads CSV once; Polars' lazy API, which its streaming engine did not
beat here); their wall is timed
inside the process from the first read to the counts, so the interpreter's
start and the import are not charged to them. compara's is the whole process.
A tool the kernel kills for memory is recorded as out of memory, with what
it held, and the run goes on; --memory-max gives each tool a cgroup with
that ceiling, so it is the tool that is stopped and not the machine.

--json writes the results; --notices also prints them as `bench json i/N`
notices (3,500 characters each), so the docs can be written from a run with
no artifact download: `scripts/gen_bench_docs.py <run id>`.
"""
import argparse
import datetime
import json
import os
import platform
import subprocess
import sys
import time

KEY = ["account_id", "txn_id"]
IGNORE = "updated_at"

DUCKDB = r"""
import json, sys, time
import duckdb
a, b, fmt, key, ignore = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(","), sys.argv[5]
t0 = time.perf_counter()
con = duckdb.connect()
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


def run_one(cmd, work):
    """One run of cmd in a process of its own: exit code, wall, CPU (user +
    system) and peak RSS in MB of that process alone (wait4), and its stdout."""
    out_path = os.path.join(work, "run.out")
    with open(out_path, "w") as out, open(os.path.join(work, "run.err"), "w") as err:
        t0 = time.perf_counter()
        p = subprocess.Popen(cmd, stdout=out, stderr=err)
        _, status, ru = os.wait4(p.pid, 0)
        wall = time.perf_counter() - t0
    p.returncode = os.waitstatus_to_exitcode(status)
    text = open(out_path).read()
    return p.returncode, wall, ru.ru_utime + ru.ru_stime, ru.ru_maxrss / 1024, text


def compara_cmd(binary, a, b, out_json):
    return [binary, "compare", a, b, "-k", ",".join(KEY), "-i", IGNORE, "--json", out_json]


def contender(name, binary, a, b, fmt, work, memory_max=None):
    if name == "compara":
        out_json = os.path.join(work, "compara-counts.json")
        cmd = compara_cmd(binary, a, b, out_json)
    else:
        code = DUCKDB if name == "duckdb" else POLARS
        cmd = [sys.executable, "-c", code, a, b, fmt, ",".join(KEY), IGNORE]
    if memory_max:
        # A cgroup of its own with a memory ceiling: a tool that needs more
        # is stopped by the kernel alone, and the machine (a CI runner) goes on.
        cmd = ["sudo", "-n", "systemd-run", "--scope", "--quiet", "-p", f"MemoryMax={memory_max}",
               "-p", "MemorySwapMax=0", "--"] + cmd
    return cmd, (out_json if name == "compara" else None)


def bench(args, fmt, a, b):
    """Every tool on one pair, in interleaved rounds. A tool the kernel kills
    for memory (SIGKILL) is recorded as out of memory, with how much it held,
    and left out of the later rounds; any other failure stops the run."""
    names = ["compara", "duckdb", "polars"]
    cmds = {n: contender(n, args.compara, a, b, fmt, args.work, args.memory_max) for n in names}
    runs = {n: [] for n in names}
    counts, failed = {}, {}
    for i in range(args.runs):  # interleaved: each round runs every tool once
        for n in names:
            if n in failed:
                continue
            cmd, out_json = cmds[n]
            code, wall, cpu, rss, out = run_one(cmd, args.work)
            err = open(os.path.join(args.work, "run.err")).read()[-2000:]
            if code in (-9, 137) and n != "compara":
                failed[n] = {"failed": "out of memory", "peak_rss_mb": round(rss), "wall_s": round(wall, 3)}
                print(f"{fmt} run {i + 1} {n}: killed for memory after {wall:.1f} s at {rss:.0f} MB", flush=True)
                continue
            if n == "compara":
                if code not in (0, 1):
                    sys.exit(f"compara exited {code} on {fmt}: {err}")
                c = json.load(open(out_json))["counts"]
                counts[n] = {k: c[k] for k in ("unchanged", "changed", "added", "removed")}
            else:
                if code != 0:
                    sys.exit(f"{n} failed on {fmt} (exit {code}): {err}")
                r = json.loads(out.strip().splitlines()[-1])
                wall = r["wall_s"]
                counts[n] = r["counts"]
            runs[n].append({"wall_s": round(wall, 3), "cpu_s": round(cpu, 3), "peak_rss_mb": round(rss)})
            print(f"{fmt} run {i + 1} {n}: {wall:.2f} s wall, {cpu:.2f} s CPU, {rss:.0f} MB, {counts[n]}", flush=True)
    results = {}
    for n in names:
        if n in failed:
            results[n] = failed[n]
            continue
        if counts[n] != counts["compara"]:
            sys.exit(f"correctness gate: {n} counts {counts[n]} on {fmt}, compara {counts['compara']}")
        best = min(runs[n], key=lambda r: r["wall_s"])
        results[n] = {**best, "runs": runs[n], "counts": counts[n]}
    return results


def versions(args):
    v = {"compara": subprocess.run([args.compara, "--version"], capture_output=True, text=True).stdout.strip()}
    for mod in ("duckdb", "polars"):
        v[mod] = subprocess.run([sys.executable, "-c", f"import {mod}; print({mod}.__version__)"],
                                capture_output=True, text=True).stdout.strip()
    v["python"] = platform.python_version()
    return v


def env():
    cpu = next((line.split(":", 1)[1].strip() for line in open("/proc/cpuinfo") if line.startswith("model name")),
               platform.processor())
    mem = next((int(line.split()[1]) // 1024 // 1024 for line in open("/proc/meminfo") if line.startswith("MemTotal")), 0)
    return {"cpu": cpu, "cores": os.cpu_count(), "memory_gb": mem, "os": platform.platform(terse=True)}


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
    ap.add_argument("--rows", default="1m,10m", help="sizes, smallest first: 1m,10m")
    ap.add_argument("--delete", action="store_true", help="delete each pair once measured")
    ap.add_argument("--memory-max", help="run every tool in a cgroup with this MemoryMax (e.g. 13G; needs sudo and systemd)")
    ap.add_argument("--formats", default="csv,parquet")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--work", default="bench-data")
    ap.add_argument("--json")
    ap.add_argument("--notices", action="store_true")
    args = ap.parse_args()
    args.compara = os.path.abspath(args.compara)
    os.makedirs(args.work, exist_ok=True)
    doc = {"generated_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "run_id": os.environ.get("GITHUB_RUN_ID"), "commit": os.environ.get("GITHUB_SHA"),
           "columns": 20, "key": KEY, "ignore": IGNORE, "runs": args.runs,
           "env": env(), "versions": versions(args), "inputs": []}
    for rows in args.rows.split(","):
        for fmt in args.formats.split(","):
            a, b = (os.path.join(args.work, f"{rows}_{s}.{fmt}") for s in "ab")
            if not (os.path.exists(a) and os.path.exists(b)):
                subprocess.run([args.gen, "--rows", rows, "--out-dir", args.work, "--prefix", rows, "-f", fmt],
                               check=True)
            doc["inputs"].append({"rows": rows, "format": fmt, "bytes": os.path.getsize(a) + os.path.getsize(b),
                                  "tools": bench(args, fmt, a, b)})
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
