#!/usr/bin/env python3
"""compara against DuckDB, Polars, qsv and ClickHouse on one keyed comparison: every number from this run.

    bench_contenders.py --compara BIN --gen BIN [--qsv BIN] [--clickhouse BIN]
                        [--rows 1m,10m] [--formats csv,parquet] [--keys unique,repeated]
                        [--parquet-writers compara-gen-data,polars,duckdb] [--runs 3]
                        [--work DIR] [--delete] [--cache warm,cold] [--memory-max 13G]
                        [--json OUT] [--notices]
    bench_contenders.py --notices-of OUT [--notice-step K]

Two generated tables of 20 columns a side (compara-gen-data, seed 7), keyed
on (account_id, txn_id), updated_at ignored. Every contender reports how
many keys are unchanged, changed, added and removed, and the run fails
unless they all agree: the correctness gate.

The pairs. The headline pairs have no key twice in a file (--unique-keys),
the case every tool here is built for, so each runs its plainest query on
them. They come as CSV and as Parquet from three writers: compara-gen-data's
own (every column a string, uncompressed, a row group a million rows) and the
CSV pair as Polars and as DuckDB write it with their defaults (their own type
inference, compression and row groups), each written in a child process
before any timing; the page says what each file holds. The pairs the
generator writes by default repeat 0.01% of a file's keys, rows otherwise the
same, and stay as rows of their own (CSV, and compara-gen-data's Parquet):
there a key that appears twice in one file is compared by its first row, as
compara does, and each other tool has to be told so, which costs it time.

Each contender gets compara's semantics: values compared as text in CSV and
by value (in the file's types) in Parquet, and an empty field equal only to
an empty field. Each run is a process of its own, and every run starts from
a stated page-cache state of
the files that tool reads (--cache): warm, each input read to its end just
before; cold, each input dropped from the page cache (posix_fadvise
DONTNEED; no root needed). Without that, what one tool leaves in memory
decides how warm the next one starts: a 10M-row run where Polars was killed
at 13 GB left compara's 3.7 GB of inputs evicted, and compara took 9.3 s
there against 0.7 s the round before. The tools run in an order turned by
one each round, so none always goes first. Each tool's number in each state is
the median of --runs (the median run's CPU (user + system), peak RSS and
peak private memory (RssAnon, sampled) beside it) with the fastest and
slowest; the first state in --cache is the headline. Each run also records
how much of its inputs was in the page cache when it started (mincore) and
its major page faults. Each round also times reading both inputs from a cold
cache (`disk_read_s`), the floor for any tool on a cold run.

DuckDB and Polars run from Python with every core; their wall is timed
inside the process from the first read to the counts, so the interpreter's
start and the import are not charged to them. On unique keys DuckDB reads
each file where it is (read_csv with all_varchar, or read_parquet) into a
FULL OUTER JOIN, and Polars scans both into a full join (its lazy API, which
its streaming engine did not beat). On repeated keys DuckDB loads each CSV
into a table once for a row order and keeps each key's first row by rowid
(by file_row_number on Parquet, read where it is), and Polars adds
unique(keep="first"). compara's time is the whole process.
--qsv adds qsv (dathere/qsv, its release binary) on the CSV inputs only: `qsv diff`
reads CSV alone, and has no way to leave a column out, so the ignored column is
cut from both files first with `qsv select`, outside the timing (the page says
so). Only the `qsv diff` process is timed, writing its diff to a file as compara
does not; its counts are read back from that output afterwards (qsv diff lists
the rows that differ, so unchanged is the left file's rows, from `qsv count`,
less the changed and removed). qsv requires unique keys, so on the pairs that
repeat a few the same untimed step keeps each key's first row with
`qsv extdedup --select`.
--clickhouse adds clickhouse-local (ClickHouse's official release binary) on
the unique-key pairs, timed as a whole process: the `file` table function on
both inputs where they are (CSV as String columns), a FULL OUTER JOIN with
join_use_nulls.
A tool the kernel kills for memory is recorded as out of memory, with what
it held, and the run goes on; --memory-max gives each tool a cgroup with
that ceiling, so it is the tool that is stopped and not the machine.

--json writes the results; --notices also prints them as `bench json i/N`
notices (3,500 characters each; each run a row of fields, the summaries left
for the reader to work out again), so the docs can be written from a run with
no artifact download: `scripts/gen_bench_docs.py <run id>`. GitHub keeps 10
notices a step: results that need more are printed from several steps with
--notices-of OUT --notice-step 1, 2, ...
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
a, b, fmt, key, ignore, keys = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(","), sys.argv[5], sys.argv[6]
t0 = time.perf_counter()
con = duckdb.connect()
# Its progress bar, drawn for a query that runs a few seconds, goes to stdout
# and ran into the result line the harness reads.
con.execute("SET enable_progress_bar = false")
if keys == "unique":
    # No key twice in a file: each file is read where it is and joined as it is.
    src = {s: (f"read_csv('{p}', all_varchar = true)" if fmt == "csv" else f"read_parquet('{p}')")
           for s, p in (("a", a), ("b", b))}
    first = ""
else:
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
    first = f"QUALIFY row_number() OVER (PARTITION BY {', '.join(key)} ORDER BY {order}) = 1"
cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src['a']}").fetchall()]
cmp = [c for c in cols if c not in key and c != ignore and c != "file_row_number"]
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
a, b, fmt, key, ignore, keys = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4].split(","), sys.argv[5], sys.argv[6]
t0 = time.perf_counter()
scan = (lambda p: pl.scan_csv(p, infer_schema=False)) if fmt == "csv" else pl.scan_parquet
la, lb = scan(a), scan(b)
cmp = [c for c in la.collect_schema().names() if c not in key and c != ignore]
if keys != "unique":
    # A key twice in a file is compared by its first row.
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

# The Parquet pair as Polars and DuckDB write it, each with its defaults: the
# CSV read with its own type inference, written with its own compression and
# row groups. Each is written in a child process before the timing, so neither
# the writer's time nor its memory falls on any tool.
WRITERS = {
    "polars": r"""
import sys
import polars as pl
pl.read_csv(sys.argv[1]).write_parquet(sys.argv[2])
""",
    "duckdb": r"""
import sys
import duckdb
duckdb.sql(f"COPY (SELECT * FROM read_csv('{sys.argv[1]}')) TO '{sys.argv[2]}' (FORMAT parquet)")
""",
}

# What a Parquet file holds, for the page: its row groups, compression and column types.
DESCRIBE_PARQUET = r"""
import json, sys
import duckdb
p = sys.argv[1]
con = duckdb.connect()
groups, codecs = con.execute(f"SELECT count(DISTINCT row_group_id), list(DISTINCT compression ORDER BY compression) "
                             f"FROM parquet_metadata('{p}')").fetchone()
types = con.execute(f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{p}'))").fetchall()
print(json.dumps({"row_groups": groups, "compression": codecs, "types": dict(types)}))
"""


def clickhouse_query(a, b, fmt, columns):
    """clickhouse-local's query on a pair with unique keys: both files read where they
    are (the `file` table function) and joined as they are. CSV is read with every
    column a String, as compara compares CSV as text; Parquet with the file's types.
    join_use_nulls gives a row missing from one side NULLs there (ClickHouse otherwise
    fills in each type's default), and a NULL equals only a NULL."""
    def src(p):
        if fmt == "csv":
            return f"file('{p}', 'CSVWithNames', '{', '.join(f'{c} String' for c in columns)}')"
        return f"file('{p}', 'Parquet')"
    cmp = [c for c in columns if c not in KEY and c != IGNORE]
    eq = " AND ".join(f"ifNull(ta.{c} = tb.{c}, isNull(ta.{c}) AND isNull(tb.{c}))" for c in cmp)
    both = f"ta.{KEY[0]} IS NOT NULL AND tb.{KEY[0]} IS NOT NULL"
    on = " AND ".join(f"ta.{k} = tb.{k}" for k in KEY)
    return (f"SELECT countIf({both} AND ({eq})), countIf({both} AND NOT ({eq})), "
            f"countIf(ta.{KEY[0]} IS NULL), countIf(tb.{KEY[0]} IS NULL) "
            f"FROM {src(a)} AS ta FULL OUTER JOIN {src(b)} AS tb ON {on} "
            "SETTINGS join_use_nulls = 1 FORMAT TSV")


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


# qsv diff's options beyond the keys. `--drop-equal-fields` (blank the fields a
# changed row did not change) writes a third of the output but was no faster on
# the 1M-row unique-key pair (median of 7, local: 1.76 s against 1.74 s plain,
# the same counts), so qsv runs as its help shows it first.
QSV_DIFF_OPTS = []


def qsv_diff_cmd(binary, a, b):
    """qsv diff on two files whose ignored column is already gone. Its options,
    from `qsv diff --help`: -k names the key columns (the same in both files),
    -j defaults to every CPU, and the diff goes to stdout (run_one's file)."""
    return [binary, "diff", "-k", ",".join(KEY), *QSV_DIFF_OPTS, a, b]


def qsv_prepare(binary, a, b, work, dedup):
    """Not timed. qsv diff has no option to skip a column, so each file goes through
    `qsv select '!IGNORE'`. It also says its keys MUST be unique: on a pair that repeats
    keys (dedup), `qsv extdedup --select KEY` then keeps a key's first row, as compara
    compares it. Returns the two new paths, their row counts and the seconds spent."""
    t0 = time.perf_counter()
    outs = []
    for src in (a, b):
        base = os.path.join(work, "qsv_" + os.path.basename(src))
        # qsv reads a file's format from its extension, so the cut keeps `.csv`.
        cut = os.path.join(work, "qsvcut_" + os.path.basename(src))
        subprocess.run([binary, "select", f"!{IGNORE}", "-o", cut if dedup else base, src], check=True)
        if dedup:
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
                 "a key twice in a file?")
    return {"unchanged": unchanged, "changed": changed, "added": added, "removed": removed}


def contender(name, binary, pair, work, memory_max=None):
    a, b, fmt = pair["a"], pair["b"], pair["format"]
    if name == "compara":
        out_json = os.path.join(work, "compara-counts.json")
        cmd = compara_cmd(binary, a, b, out_json)
    elif name == "qsv":
        cmd = qsv_diff_cmd(binary, a, b)
    elif name == "clickhouse":
        cmd = [binary, "local", "--query", clickhouse_query(a, b, fmt, pair["columns"])]
    else:
        code = DUCKDB if name == "duckdb" else POLARS
        cmd = [sys.executable, "-c", code, a, b, fmt, ",".join(KEY), IGNORE, pair["keys"]]
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


def rotated(names, i):
    """The tools in round i's order: the list turned by one place a round, so no
    tool always runs first (or always right after another)."""
    k = i % len(names)
    return names[k:] + names[:k]


def bench(args, pair):
    """Every tool on one pair, in interleaved rounds; in each round every cache
    state of --cache, and in each state every tool once, its inputs put in
    that state just before its run, the order turned each round. A tool the
    kernel kills for memory (SIGKILL) is recorded as out of memory, with how
    much it held, and left out of the later runs; any other failure stops the run."""
    a, b, fmt, tag = pair["a"], pair["b"], pair["format"], pair["tag"]
    names = ["compara", "duckdb", "polars"]
    states = args.cache.split(",")
    cmds = {n: contender(n, args.compara, pair, args.work, args.memory_max) for n in names}
    files = {n: [a, b] for n in names}
    qsv_prep = None
    if args.qsv and fmt == "csv":
        names.append("qsv")
        qa, qb, qrows, prep_s = qsv_prepare(args.qsv, a, b, args.work, dedup=pair["keys"] != "unique")
        qsv_prep = {"prep_s": round(prep_s, 3)}
        cmds["qsv"] = contender("qsv", args.qsv, {**pair, "a": qa, "b": qb}, args.work, args.memory_max)
        files["qsv"] = [qa, qb]
    if args.clickhouse and pair["keys"] == "unique":
        names.append("clickhouse")
        cmds["clickhouse"] = contender("clickhouse", args.clickhouse, pair, args.work, args.memory_max)
        files["clickhouse"] = [a, b]
    runs = {n: [] for n in names}
    counts, failed, floor = {}, {}, []
    for i in range(args.runs):  # interleaved: each round runs every tool once in each state
        floor.append(round(disk_read([a, b]), 3))
        print(f"{tag} run {i + 1} disk: both inputs read from a cold cache in {floor[-1]:.2f} s", flush=True)
        for state in states:
            for n in rotated(names, i):
                if n in failed:
                    continue
                cmd, out_json = cmds[n]
                set_cache(state, files[n])
                res = resident(files[n])
                code, wall, cpu, rss, anon, majflt, out = run_one(cmd, args.work)
                err = open(os.path.join(args.work, "run.err")).read()[-2000:]
                # Killed by the kernel, or stopped by its own memory limit (ClickHouse's
                # MEMORY_LIMIT_EXCEEDED, DuckDB's Out of Memory Error, Python's MemoryError).
                if n != "compara" and (code in (-9, 137) or (code != 0 and any(
                        m in err for m in ("MEMORY_LIMIT_EXCEEDED", "Out of Memory Error", "MemoryError")))):
                    failed[n] = {"failed": "out of memory", "peak_rss_mb": round(rss), "wall_s": round(wall, 3),
                                 "cache": state}
                    print(f"{tag} run {i + 1} {state} {n}: killed for memory after {wall:.1f} s at {rss:.0f} MB",
                          flush=True)
                    continue
                if n == "compara":
                    if code not in (0, 1):
                        sys.exit(f"compara exited {code} on {tag}: {err}")
                    c = json.load(open(out_json))["counts"]
                    counts[n] = {k: c[k] for k in ("unchanged", "changed", "added", "removed")}
                elif n == "qsv":
                    if code != 0:
                        sys.exit(f"qsv failed on {tag} (exit {code}): {err}")
                    counts[n] = qsv_counts(os.path.join(args.work, "run.out"), qrows[0], qrows[1])
                elif n == "clickhouse":
                    if code != 0:
                        sys.exit(f"clickhouse failed on {tag} (exit {code}): {err}")
                    got = out.split()
                    if len(got) != 4:
                        sys.exit(f"clickhouse printed {out[-500:]!r} on {tag}, not four counts")
                    counts[n] = dict(zip(("unchanged", "changed", "added", "removed"), map(int, got)))
                else:
                    if code != 0:
                        sys.exit(f"{n} failed on {tag} (exit {code}): {err}")
                    r = last_result(out, n, tag)
                    wall = r["wall_s"]
                    counts[n] = r["counts"]
                runs[n].append({"cache": state, "resident": res, "wall_s": round(wall, 3), "cpu_s": round(cpu, 3),
                                "peak_rss_mb": round(rss), "peak_private_mb": round(anon), "major_faults": majflt})
                print(f"{tag} run {i + 1} {state} {n}: {wall:.2f} s wall, {cpu:.2f} s CPU, {rss:.0f} MB "
                      f"({anon:.0f} private), {majflt} major faults, {res} of inputs cached, {counts[n]}", flush=True)
    results = {}
    for n in names:
        if n in failed:
            results[n] = failed[n]
            continue
        if counts[n] != counts["compara"]:
            sys.exit(f"correctness gate: {n} counts {counts[n]} on {tag}, compara {counts['compara']}")
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
    if args.clickhouse:
        v["clickhouse"] = subprocess.run([args.clickhouse, "local", "--version"],
                                         capture_output=True, text=True).stdout.strip()
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


RUN_FIELDS = ["cache", "resident", "wall_s", "cpu_s", "peak_rss_mb", "peak_private_mb", "major_faults"]


def compact(doc):
    """The results with each run as a row of RUN_FIELDS and no summaries: what the
    notices carry. Every summary is the median run's, so the docs generator
    works them out again from the runs (gen_bench_docs.py expand)."""
    out = {**doc, "run_fields": RUN_FIELDS, "inputs": []}
    for inp in doc["inputs"]:
        tools = {}
        for n, r in inp["tools"].items():
            if r.get("failed"):
                tools[n] = r
                continue
            tools[n] = {"runs": [[x[k] for k in RUN_FIELDS] for x in r["runs"]], "counts": r["counts"],
                        **({"prep_s": r["prep_s"]} if "prep_s" in r else {})}
        out["inputs"].append({**inp, "tools": tools})
    return out


def notice_parts(doc):
    text = json.dumps(compact(doc), separators=(",", ":"))
    # No piece starts or ends with a space, so a reader that trims each one
    # still joins them into the same document.
    parts, i = [], 0
    while i < len(text):
        j = min(i + 3500, len(text))
        while j < len(text) and j > i + 1 and (text[j - 1] == " " or text[j] == " "):
            j -= 1
        parts.append(text[i:j])
        i = j
    return parts


def notices(doc, step=None, title="bench json"):
    """Prints the results as `bench json i/N` notices. GitHub keeps 10 notices a
    step (and 50 a job), so results longer than 10 are printed by several steps,
    each with --notice-step 1, 2, ...: step k prints parts 10k-9 to 10k."""
    parts = notice_parts(doc)
    if len(parts) > 50:
        sys.exit(f"::error::{title}: {len(parts)} notices needed, GitHub keeps 50 per job")
    if step is None and len(parts) > 10:
        sys.exit(f"::error::{title}: {len(parts)} notices needed, GitHub keeps 10 per step: "
                 "print them from several steps with --notices-of and --notice-step")
    first, last = (0, len(parts)) if step is None else (10 * (step - 1), min(10 * step, len(parts)))
    for n in range(first, last):
        msg = parts[n].replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::notice title={title} {n + 1}/{len(parts)}::{msg}")


def gen_paths(args, rows, fmt, keys):
    prefix = rows if keys == "repeated" else f"{rows}-unique"
    return tuple(os.path.join(args.work, f"{prefix}_{s}.{fmt}") for s in "ab")


def gen(args, rows, fmt, keys):
    """compara-gen-data's pair of this size, format and keys, written unless it is there."""
    a, b = gen_paths(args, rows, fmt, keys)
    prefix = os.path.basename(a)[:-len(f"_a.{fmt}")]
    if not (os.path.exists(a) and os.path.exists(b)):
        subprocess.run([args.gen, "--rows", rows, "--out-dir", args.work, "--prefix", prefix, "-f", fmt]
                       + (["--unique-keys"] if keys == "unique" else []), check=True)
    return a, b


def write_parquet(writer, csv_pair):
    """The CSV pair as `writer` (polars or duckdb) writes it to Parquet by default,
    each file in a child process of its own, before any timing."""
    out = []
    for src in csv_pair:
        dst = src[:-len(".csv")] + f".{writer}.parquet"
        if not os.path.exists(dst):
            subprocess.run([sys.executable, "-c", WRITERS[writer], src, dst], check=True)
        out.append(dst)
    return out


def describe(path, fmt):
    """The file's columns, and for Parquet its row groups, compression and types."""
    if fmt == "csv":
        with open(path) as f:
            return {"columns": f.readline().rstrip("\r\n").split(",")}
    d = json.loads(subprocess.run([sys.executable, "-c", DESCRIBE_PARQUET, path], check=True,
                                  capture_output=True, text=True).stdout)
    return {"columns": list(d["types"]), **d}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--compara")
    ap.add_argument("--gen", help="compara-gen-data")
    ap.add_argument("--qsv", help="qsv's release binary: adds qsv diff on the CSV inputs")
    ap.add_argument("--clickhouse", help="ClickHouse's release binary: adds clickhouse-local on the pairs with unique keys")
    ap.add_argument("--rows", default="1m,10m", help="sizes, smallest first: 1m,10m")
    ap.add_argument("--delete", action="store_true", help="delete each pair once measured")
    ap.add_argument("--memory-max", help="run every tool in a cgroup with this MemoryMax (e.g. 13G; needs sudo and systemd)")
    ap.add_argument("--formats", default="csv,parquet")
    ap.add_argument("--keys", default="unique,repeated",
                    help="pairs with no key twice in a file (unique, the headline), as the generator writes them "
                         "by default (repeated), or both")
    ap.add_argument("--parquet-writers", default="compara-gen-data,polars,duckdb",
                    help="who writes the unique-key Parquet pairs: compara-gen-data, and the CSV pair as Polars "
                         "and DuckDB write it with their defaults; the repeated-key pair is compara-gen-data's")
    ap.add_argument("--runs", type=int, default=3, help="rounds; each tool's number is its median")
    ap.add_argument("--cache", default="warm,cold",
                    help="page-cache states each tool runs in every round, the first the headline: warm,cold")
    ap.add_argument("--work", default="bench-data")
    ap.add_argument("--json")
    ap.add_argument("--notices", action="store_true")
    ap.add_argument("--notices-of", metavar="JSON", help="print the notices of results --json wrote, and nothing else")
    ap.add_argument("--notice-step", type=int, help="with --notices-of: the step's ten notices to print (1, 2, ...)")
    args = ap.parse_args()
    if args.notices_of:
        notices(json.load(open(args.notices_of)), args.notice_step)
        return
    if not (args.compara and args.gen):
        sys.exit("--compara and --gen are needed to run the benchmark")
    if not args.cache or set(args.cache.split(",")) - {"warm", "cold"}:
        sys.exit(f"--cache takes warm, cold or both, not {args.cache!r}")
    if set(args.keys.split(",")) - {"unique", "repeated"}:
        sys.exit(f"--keys takes unique, repeated or both, not {args.keys!r}")
    writers = args.parquet_writers.split(",")
    if not writers or set(writers) - {"compara-gen-data", "polars", "duckdb"}:
        sys.exit(f"--parquet-writers takes compara-gen-data, polars, duckdb, not {args.parquet_writers!r}")
    args.compara = os.path.abspath(args.compara)
    for tool in ("qsv", "clickhouse"):
        if getattr(args, tool):
            setattr(args, tool, os.path.abspath(getattr(args, tool)))
    os.makedirs(args.work, exist_ok=True)
    doc = {"generated_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "run_id": os.environ.get("GITHUB_RUN_ID"), "commit": os.environ.get("GITHUB_SHA"),
           "columns": 20, "key": KEY, "ignore": IGNORE, "runs": args.runs, "cache": args.cache.split(","),
           "env": env(), "versions": versions(args), "inputs": []}
    formats = args.formats.split(",")
    for rows in args.rows.split(","):
        for keys in args.keys.split(","):
            # Each pair: its format, who wrote it, and how it is made. The other writers'
            # Parquet is made from the generator's CSV pair of the same keys, written once.
            todo = []
            for fmt in formats:
                if fmt != "parquet":
                    todo.append((fmt, "compara-gen-data", lambda fmt=fmt: gen(args, rows, fmt, keys)))
                    continue
                for w in (writers if keys == "unique" else ["compara-gen-data"]):
                    make = ((lambda: gen(args, rows, "parquet", keys)) if w == "compara-gen-data"
                            else (lambda w=w: write_parquet(w, gen(args, rows, "csv", keys))))
                    todo.append(("parquet", w, make))
            made = set()
            for n, (fmt, writer, make) in enumerate(todo):
                a, b = make()
                made.update((a, b))
                info = describe(a, fmt)
                tag = f"{rows} {keys}-key {fmt}" + (f" ({writer})" if fmt == "parquet" else "")
                pair = {"a": a, "b": b, "format": fmt, "keys": keys, "tag": tag, "columns": info["columns"]}
                tools, floor = bench(args, pair)
                file_info = {k: v for k, v in info.items() if k != "columns"}
                doc["inputs"].append({"rows": rows, "format": fmt, "keys": keys, "writer": writer,
                                      **({"file": file_info} if file_info else {}),
                                      "bytes": os.path.getsize(a) + os.path.getsize(b),
                                      "disk_read_s": statistics.median(floor), "disk_read_runs": floor,
                                      "tools": tools})
                if args.delete:  # the next pair needs the disk; a CSV another writer still reads waits
                    later = any(f != "csv" and w != "compara-gen-data" for f, w, _ in todo[n + 1:])
                    for f in (a, b):
                        if not (fmt == "csv" and later):
                            os.remove(f)
                            made.discard(f)
            if args.delete:
                for f in made | set(gen_paths(args, rows, "csv", keys)):
                    if os.path.exists(f):
                        os.remove(f)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(doc, f, indent=1)
    if args.notices:
        notices(doc)


if __name__ == "__main__":
    main()
