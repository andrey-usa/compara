#!/usr/bin/env python3
"""compara against DuckDB on zstd-compressed tables (issue #11).

Builds compara from this checkout (and, with --baseline <ref>, from that ref
too), installs DuckDB 1.x and 2.x into throwaway virtual environments, writes a
keyed pair with compara-gen-data, and compares it as `.csv.zst` and as zstd
Parquet (written by compara-gen-data and by DuckDB, whose row groups differ).
Every tool must report the same counts; the table is best of --runs walls.

    python3 scripts/bench_zstd.py                  # 10M rows, everything
    python3 scripts/bench_zstd.py --baseline main  # and compara at main
    python3 scripts/bench_zstd.py --rows 2m --runs 5

Needs a Rust toolchain and Python 3.11+, and about 6 GB of disk for 10M rows
(set --work to put it elsewhere). DuckDB runs the query bench-contenders runs:
both tables read, the first row of each key kept, a full outer join, and a
per-column comparison, values as text.
"""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
KEY = ["account_id", "txn_id"]
IGNORE = "updated_at"
COMPARED = ["posting_date", "value_date", "currency", "amount", "fee", "balance", "status", "channel",
            "region", "branch_code", "product_code", "counterparty", "quantity", "rate", "category",
            "risk_flag", "note"]
DUCKDBS = {"duckdb 1.x": "duckdb>=1,<2", "duckdb 2.x": "duckdb>=2,<3"}


def run(cmd, **kw):
    return subprocess.run(cmd, check=True, **kw)


def build(ref, target):
    """compara and compara-gen-data at `ref` (None: this checkout)."""
    src = ROOT
    if ref:
        src = target / "src"
        if not src.exists():
            run(["git", "-C", str(ROOT), "worktree", "add", "--detach", str(src), ref])
    run(["cargo", "build", "--release", "--locked", "--bin", "compara", "--bin", "compara-gen-data"],
        cwd=src, env={**os.environ, "CARGO_TARGET_DIR": str(target / "target")})
    return target / "target" / "release"


def venv(work, name, spec):
    d = work / ("venv-" + name.replace(" ", "-").replace(".", ""))
    py = d / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not py.exists():
        run([sys.executable, "-m", "venv", str(d)])
        run([str(py), "-m", "pip", "install", "--quiet", "--upgrade", spec, "zstandard"])
    version = subprocess.run([str(py), "-c", "import duckdb; print(duckdb.__version__)"],
                             capture_output=True, text=True, check=True).stdout.strip()
    return py, version


def data(work, rows, gen, py):
    """The pair as CSV, .csv.zst, compara-gen-data Parquet (zstd) and DuckDB Parquet (zstd)."""
    d = work / f"data-{rows}"
    if (d / "done").exists():
        return d
    d.mkdir(parents=True, exist_ok=True)
    run([str(gen / "compara-gen-data"), "--rows", rows, "--out-dir", str(d), "--prefix", "t"])
    run([str(gen / "compara-gen-data"), "--rows", rows, "--out-dir", str(d), "--prefix", "gen",
         "--format", "parquet", "--compression", "zstd"])
    script = f"""
import duckdb, zstandard
for s in "ab":
    with open(r"{d}/t_" + s + ".csv", "rb") as i, open(r"{d}/t_" + s + ".csv.zst", "wb") as o:
        zstandard.ZstdCompressor(level=3, threads=-1).copy_stream(i, o)
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    con.execute(f"COPY (SELECT * FROM read_csv('{d}/t_{{s}}.csv', all_varchar=true)) "
                f"TO '{d}/duck_{{s}}.parquet' (FORMAT parquet, COMPRESSION zstd)")
"""
    run([str(py), "-c", script])
    (d / "done").write_text("")
    return d


CASES = {
    ".csv.zst": ("t_a.csv.zst", "t_b.csv.zst"),
    "Parquet zstd (compara-gen-data)": ("gen_a.parquet", "gen_b.parquet"),
    "Parquet zstd (DuckDB-written)": ("duck_a.parquet", "duck_b.parquet"),
}


def time_compara(binary, a, b, runs):
    walls, counts = [], None
    out = Path(tempfile.gettempdir()) / f"compara-bench-{os.getpid()}.json"
    for _ in range(runs):
        t = time.perf_counter()
        r = subprocess.run([str(binary), "compare", str(a), str(b), "-k", ",".join(KEY), "-i", IGNORE,
                            "--json", str(out)], capture_output=True, text=True)
        walls.append(time.perf_counter() - t)
        if r.returncode not in (0, 1):
            raise SystemExit(f"compara failed: {r.stderr[-2000:]}")
        c = json.loads(out.read_text())["counts"]
        counts = (c["unchanged"], c["changed"], c["added"], c["removed"])
    return min(walls), counts


DUCK = r"""
import duckdb, json, sys, time
a, b, runs = sys.argv[1], sys.argv[2], int(sys.argv[3])
key = %(key)r
cmp = %(cmp)r
read = (lambda f: f"read_csv('{f}', all_varchar=true)") if ".csv" in a else (lambda f: f"read_parquet('{f}')")
cast = (lambda c: c) if ".csv" in a else (lambda c: f"CAST({c} AS VARCHAR)")
eq = " AND ".join(f"{cast('a.' + c)} IS NOT DISTINCT FROM {cast('b.' + c)}" for c in cmp)
first = f"QUALIFY row_number() OVER (PARTITION BY {', '.join(key)} ORDER BY rowid) = 1"
walls, row = [], None
for _ in range(runs):
    t = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    con.execute(f"CREATE TABLE ra AS SELECT * FROM {read(a)}")
    con.execute(f"CREATE TABLE rb AS SELECT * FROM {read(b)}")
    con.execute(f"CREATE TABLE ta AS SELECT * FROM ra {first}")
    con.execute(f"CREATE TABLE tb AS SELECT * FROM rb {first}")
    on = " AND ".join(f"a.{k} = b.{k}" for k in key)
    both = "a.account_id IS NOT NULL AND b.account_id IS NOT NULL"
    row = con.execute(f"SELECT count(*) FILTER ({both} AND ({eq})), count(*) FILTER ({both} AND NOT ({eq})), "
                      f"count(*) FILTER (a.account_id IS NULL), count(*) FILTER (b.account_id IS NULL) "
                      f"FROM ta a FULL OUTER JOIN tb b ON {on}").fetchone()
    walls.append(time.perf_counter() - t)
    con.close()
print(json.dumps([min(walls), list(row)]))
""" % {"key": KEY, "cmp": COMPARED}


def time_duckdb(py, a, b, runs):
    out = subprocess.run([str(py), "-c", DUCK, str(a), str(b), str(runs)], capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(f"duckdb failed: {out.stderr[-2000:]}")
    wall, counts = json.loads(out.stdout)
    return wall, tuple(counts)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", default="10m")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--baseline", help="also time compara built at this git ref (e.g. main)")
    p.add_argument("--work", type=Path, default=Path(tempfile.gettempdir()) / "compara-bench-zstd")
    args = p.parse_args()
    args.work.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(args.work).free < 6 << 30 and args.rows == "10m":
        print(f"warning: less than 6 GB free in {args.work}", file=sys.stderr)

    tools = {"compara": build(None, args.work / "this")}
    if args.baseline:
        tools[f"compara ({args.baseline})"] = build(args.baseline, args.work / "baseline")
    ducks = {name: venv(args.work, name, spec) for name, spec in DUCKDBS.items()}
    d = data(args.work, args.rows, tools["compara"], next(iter(ducks.values()))[0])

    cores = os.cpu_count()
    print(f"\n{platform.platform()}, {cores} cores, {args.rows} rows a side, best of {args.runs}\n")
    names = list(tools) + [f"{n} ({v})" for n, (_, v) in ducks.items()]
    print("| case | " + " | ".join(names) + " |")
    print("|---|" + "---|" * len(names))
    for case, (fa, fb) in CASES.items():
        a, b = d / fa, d / fb
        cells, counts = [], set()
        for name, release in tools.items():
            wall, c = time_compara(release / "compara", a, b, args.runs)
            cells.append(f"{wall:.2f} s")
            counts.add(c)
        for name, (py, _) in ducks.items():
            wall, c = time_duckdb(py, a, b, args.runs)
            cells.append(f"{wall:.2f} s")
            counts.add(c)
        flag = "" if len(counts) == 1 else f"  COUNTS DISAGREE: {sorted(counts)}"
        print(f"| {case} | " + " | ".join(cells) + f" |{flag}", flush=True)


if __name__ == "__main__":
    main()
