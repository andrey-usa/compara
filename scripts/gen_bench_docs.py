#!/usr/bin/env python3
"""Write compara's benchmark docs from one bench-contenders run.

    gen_bench_docs.py RUN_ID | results.json [--readme README.md]
                      [--page docs/benchmarks.md] [--losses docs/losses.md]

RUN_ID: the run's `bench json i/N` notices are joined back into its
results through the checks API (no token, no artifact download). A path
reads a file scripts/bench_contenders.py --json wrote instead.

Writes README.md's `## Benchmarks` (one table and the measures another tool
won), the full page (every run, the machine, the versions and the method)
and the losses page. No number in them is typed by hand: rerun this on a
newer run to update them all.
"""
import json
import os
import re
import sys
import urllib.request

REPO = "andrey-usa/compara"
SITE = "https://andrey-usa.github.io/compara/"
NAMES = {"compara": "compara", "duckdb": "DuckDB", "polars": "Polars", "qsv": "qsv", "clickhouse": "ClickHouse"}
WRITERS = {"compara-gen-data": "compara-gen-data", "polars": "Polars", "duckdb": "DuckDB"}
FORMATS = {"csv": "CSV", "parquet": "Parquet", "ndjson": "NDJSON"}
TIE = 0.02  # closer than 2% is a tie, not a loss: about what one run varies by


def fetch(run):
    api = f"https://api.github.com/repos/{REPO}"

    def get(url):
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "gen_bench_docs"})) as r:
            return json.load(r)

    parts, total = {}, 0
    for job in get(f"{api}/actions/runs/{run}/jobs?per_page=100")["jobs"]:
        for n in get(f"{api}/check-runs/{job['id']}/annotations?per_page=100"):
            m = re.fullmatch(r"bench json (\d+)/(\d+)", n.get("title") or "")
            if m:
                parts[int(m.group(1))] = n["message"].strip()
                total = int(m.group(2))
    if not total or sorted(parts) != list(range(1, total + 1)):
        sys.exit(f"run {run}: no complete `bench json` notices ({sorted(parts)} of {total})")
    doc = json.loads("".join(parts[i] for i in range(1, total + 1)))
    doc["run_id"] = doc.get("run_id") or str(run)
    return doc


def median_run(runs):
    """The median run (the lower middle one when their number is even), so every
    figure beside the median wall is from one real run."""
    walls = sorted(r["wall_s"] for r in runs)
    mid = walls[(len(walls) - 1) // 2]
    run = next(r for r in runs if r["wall_s"] == mid)
    return {**run, "wall_s": mid, "wall_min_s": walls[0], "wall_max_s": walls[-1], "n": len(runs)}


def summary(runs):
    """bench_contenders.py's summary of a tool's runs in one cache state: the median
    run's figures (the lower middle one when their number is even), with the
    fastest and slowest wall and how many runs."""
    walls = sorted(r["wall_s"] for r in runs)
    mid = walls[(len(walls) - 1) // 2]
    run = next(r for r in runs if r["wall_s"] == mid)
    return {"wall_s": mid, "wall_min_s": walls[0], "wall_max_s": walls[-1], "n": len(runs),
            **{k: run.get(k) for k in ("cpu_s", "peak_rss_mb", "peak_private_mb", "major_faults", "resident")}}


def expand(doc):
    """Notices carry each run as a row of `run_fields` and no summaries (to fit
    GitHub's notices): the runs are read back into records and each state's
    summary worked out again, as bench_contenders.py wrote it."""
    fields = doc.pop("run_fields", None)
    if not fields:
        return doc
    for inp in doc["inputs"]:
        for n, r in inp["tools"].items():
            if r.get("failed"):
                continue
            runs = [dict(zip(fields, x)) for x in r["runs"]]
            by = {st: summary([x for x in runs if x["cache"] == st]) for st in doc["cache"]}
            inp["tools"][n] = {**by[doc["cache"][0]], "cache": by, **r, "runs": runs}
    return doc


def normalise(doc):
    """Results from before the runs had a stated cache state (and before the
    harness reported medians) are read the same way: each tool's median of its
    runs, under the state "unstated", which the docs then say."""
    if doc.get("cache"):
        return doc
    doc["cache"] = ["unstated"]
    for inp in doc["inputs"]:
        for n, r in inp["tools"].items():
            if not r.get("failed"):
                m = median_run(r["runs"])
                r.update({k: m[k] for k in ("wall_s", "cpu_s", "peak_rss_mb", "wall_min_s", "wall_max_s", "n")})
                r["peak_private_mb"] = m.get("peak_private_mb")
                r["cache"] = {"unstated": dict(r)}
    return doc


def mb(n):
    return f"{n / 1e6:,.0f} MB"


def size(rows):
    return rows.upper() if isinstance(rows, str) else f"{rows:,}"


def memory(r):
    """Peak RSS, and the private part of it where the run sampled it (results before it did not)."""
    own = r.get("peak_private_mb")
    return f"{r['peak_rss_mb']:,} MB" + (f" ({own:,} private)" if own is not None else "")


def spread(r):
    """A median wall with the fastest and slowest run beside it."""
    return f"{r['wall_s']:.2f} s ({r['wall_min_s']:.2f}-{r['wall_max_s']:.2f})"


def cell(r, best, states):
    if r.get("failed"):
        return f"{r['failed']} after {r['wall_s']:.0f} s, at {r['peak_rss_mb']:,} MB"
    head = spread(r)
    rest = "".join(f" · {st} {spread(r['cache'][st])}" for st in states[1:] if st in r.get("cache", {}))
    return f"{'**' + head + '**' if best else head}{rest} · {r['cpu_s']:.1f} s · {memory(r)}"


def label(inp):
    """An input as the tables name it: its size, format, who wrote a Parquet pair
    (results before there was more than one writer name none), whether its keys
    repeat, and its bytes."""
    fmt = FORMATS.get(inp["format"], inp["format"])
    if inp["format"] == "parquet" and inp.get("writer"):
        fmt += f" by {WRITERS.get(inp['writer'], inp['writer'])}"
    keys = ", repeated keys" if inp.get("keys") == "repeated" else ""
    return f"{size(inp['rows'])} rows, {fmt}{keys}, {mb(inp['bytes'])}"


def layout(inp):
    """What a Parquet file of the pair holds, as the run read it from file A."""
    f = inp.get("file")
    if not f:
        return ""
    by_type = {}
    for col, t in f["types"].items():
        by_type.setdefault(t, []).append(col)
    typed = "; ".join(f"{', '.join(cols)} {t}" for t, cols in by_type.items() if t != "VARCHAR")
    groups = f"{f['row_groups']} row group{'s' if f['row_groups'] != 1 else ''}"
    return (f"{', '.join(c.lower() for c in f['compression'])}, {groups}, "
            + (f"typed columns: {typed}, the rest strings" if typed else "every column a string"))


def main():
    args = sys.argv[1:]
    opts = {}
    for flag in ("--readme", "--page", "--losses"):
        if flag in args:
            i = args.index(flag)
            opts[flag] = args[i + 1]
            del args[i:i + 2]
    if not args:
        sys.exit(__doc__)
    src = args[0]
    doc = normalise(expand(json.load(open(src)) if os.path.exists(src) else fetch(src)))
    states = doc["cache"]
    run = doc.get("run_id")
    if not run:
        sys.exit("these results name no run: the docs cite one for every number")
    link = f"[bench-contenders run {run}](https://github.com/{REPO}/actions/runs/{run})"
    v, env, inputs = doc["versions"], doc["env"], doc["inputs"]
    # A tool that reads one format only (qsv: CSV) has a column and "-" where it did not run.
    names = [n for n in NAMES if any(n in inp["tools"] for inp in inputs)]

    def version(n):  # qsv's and ClickHouse's --version lines go on past the number
        m = re.search(r"\d+(\.\d+)+", v.get(n, ""))
        return m.group(0) if m else v.get(n, "")

    ver = {n: version(n) for n in names}
    ver["compara"] = v["compara"].split()[-1]
    date = doc["generated_utc"][:10]
    machine = f"{env['cpu']}, {env['cores']} cores, {env['memory_gb']} GB"
    # Runs from before the image was recorded ran on the moving label.
    runner = env.get("runner") or "ubuntu-latest"
    what = (f"Two tables of {doc['columns']} columns compared on a composite key "
            f"(`-k {','.join(doc['key'])} -i {doc['ignore']}`), every tool's unchanged, changed, added "
            f"and removed counts checked identical")
    # The headline: pairs with no key twice in a file. Pairs that repeat keys are a
    # table of their own. Results from before the split are all one table.
    headline = [inp for inp in inputs if inp.get("keys") != "repeated"]
    repeated = [inp for inp in inputs if inp.get("keys") == "repeated"]

    def table(inps):
        here = [n for n in names if any(n in inp["tools"] for inp in inps)]
        rows = ["| input, both files | " + " | ".join(f"{NAMES[n]} {ver[n]}".strip() for n in here) + " |",
                "|---|" + "---|" * len(here)]
        for inp in inps:
            rs = inp["tools"]
            done = [n for n in here if n in rs and not rs[n].get("failed")]
            fastest = min(rs[n]["wall_s"] for n in done)
            rows.append(f"| {label(inp)} | " + " | ".join(
                cell(rs[n], rs[n].get("wall_s") == fastest and n in done, states) if n in rs else "not run"
                for n in here) + " |")
        return rows

    losses = []
    for inp in inputs:
        rs = inp["tools"]
        done = [n for n in names if n in rs and not rs[n].get("failed")]
        head_wall = "wall time" if states == ["unstated"] else f"wall time from a {states[0]} page cache"
        for key, what_, fmt in (("wall_s", head_wall, lambda x: f"{x:.2f} s"),
                                ("cpu_s", "CPU time", lambda x: f"{x:.1f} s"),
                                ("peak_rss_mb", "peak memory", lambda x: f"{x:,} MB"),
                                ("peak_private_mb", "peak private memory", lambda x: f"{x:,} MB")):
            mine = rs["compara"].get(key)
            for n in done[1:] if done and done[0] == "compara" and mine is not None else []:
                if rs[n].get(key) is not None and rs[n][key] < mine * (1 - TIE):
                    losses.append(f"{what_} on {label(inp)}: {NAMES[n]} {fmt(rs[n][key])}, compara {fmt(mine)}")
        # Wall time is compared in every cache state the run measured, each by its median.
        for st in states[1:]:
            mine = rs["compara"].get("cache", {}).get(st, {}).get("wall_s")
            for n in done[1:] if done and done[0] == "compara" and mine is not None else []:
                theirs = rs[n].get("cache", {}).get(st, {}).get("wall_s")
                if theirs is not None and theirs < mine * (1 - TIE):
                    losses.append(f"wall time from a {st} page cache on {label(inp)}: {NAMES[n]} {theirs:.2f} s, "
                                  f"compara {mine:.2f} s")
    loss_line = ("Where another tool did better in this run (by more than 2%): " + "; ".join(losses) + "."
                 if losses else "No other tool did better than compara by more than 2% on any measure in this run.")
    if states == ["unstated"]:
        cache_note = ("These runs did not set the page cache before each run, so a run's wall time depends "
                      "on what the tool before it left in memory. ")
        first = "wall time"
    else:
        cache_note = (f"Every run starts from a stated page cache: {states[0]} (its inputs read to the end just "
                      "before) is the first figure" + (", cold (its inputs dropped from the page cache, so read "
                      "from the disk) the second" if "cold" in states[1:] else "") + ". ")
        first = f"wall time from a {states[0]} page cache"
    floors = "; ".join(f"{label(inp)} {inp['disk_read_s']:.2f} s" for inp in inputs if inp.get("disk_read_s"))
    floor_note = (f" Reading both inputs from a cold cache alone took (median): {floors}." if floors else "")
    split = bool(repeated)
    binaries = [n for n in ("qsv", "clickhouse") if n in names]
    note = ("Each cell: {first}, median of {runs} runs (fastest-slowest in brackets) · CPU time (user and "
            "system) · peak memory (RSS, and in brackets the private part of it), of the median run; fastest "
            "median in bold. {cache_note}DuckDB and Polars run from Python, and their time starts at their first "
            "read, after the interpreter's start and the import; {binaries}. {qsv}{clickhouse}{order}A tool the "
            "runner's kernel stopped for lack of memory says so.{floor_note}").format(
        runs=doc["runs"], first=first, cache_note=cache_note, floor_note=floor_note,
        binaries=(" and ".join(NAMES[n] for n in binaries) + (" are binaries" if len(binaries) > 1 else " is a binary")
                  + " timed as a whole process, like compara") if binaries else "compara is timed as a whole process",
        qsv=("qsv reads CSV only and is given the files with the ignored column"
             + (" already taken out (and, where keys repeat, each key's first row only)" if split else
                " and the repeated keys already taken out") + ", untimed (the page says how). ")
        if "qsv" in names else "",
        clickhouse=("ClickHouse (clickhouse-local) runs on the pairs with unique keys. " if "clickhouse" in names else ""),
        order=("The tools run in a different order each round. " if split else ""))

    readme = ["## Benchmarks", "",
              f"{what}, on GitHub's `{runner}` ({machine}), {link}, {date}"
              + (", on pairs with no key twice in a file:" if split else ":"), ""] + table(headline)
    if split:
        readme += ["", "The same comparison where a file repeats 0.01% of its keys, each compared by its first "
                   "row as compara does: DuckDB, Polars and qsv have to be told to keep the first row (the page "
                   "says how), which costs them time:", ""] + table(repeated)
    readme += ["", note, "", loss_line, "",
               f"The method, every run and object storage: [Benchmarks]({SITE}benchmarks/results.html).", ""]
    if "--readme" in opts:
        p = opts["--readme"]
        text = open(p).read()
        start = text.index("## Benchmarks")
        nxt = text.find("\n## ", start + 1)
        text = text[:start] + "\n".join(readme) + ("\n" + text[nxt + 1:] if nxt >= 0 else "")
        open(p, "w").write(text)

    def one(x):
        faults = f" · {x['major_faults']:,} major faults" if x.get("major_faults") is not None else ""
        return f"{x['wall_s']:.2f} s · {x['cpu_s']:.1f} s · {memory(x)}{faults}"

    runs = ["| input | tool | page cache | every run: wall · CPU · peak memory · major page faults |",
            "|---|---|---|---|"]
    for inp in inputs:
        for n in (n for n in names if n in inp["tools"]):
            r = inp["tools"][n]
            if r.get("failed"):
                runs.append(f"| {label(inp)} | {NAMES[n]} | {r.get('cache', '-')} | {cell(r, False, states)} |")
                continue
            for st in states:
                every = "; ".join(one(x) for x in r["runs"] if x.get("cache", "unstated") == st)
                runs.append(f"| {label(inp)} | {NAMES[n]} | {st} | {every} |")
    gate = "; ".join(
        f"{label(inp)}: " + ", ".join(f"{v_:,} {k}" for k, v_ in inp["tools"]["compara"]["counts"].items())
        for inp in inputs)
    key = ",".join(doc["key"])
    if split:
        files = [f"- {label(inp)}: {layout(inp)}." for inp in inputs if inp.get("file")]
        pairs = ("Each pair is written by `compara-gen-data` (seed 7). The headline pairs have no key twice in a "
                 "file (`--unique-keys`); the others are the generator's default, where 0.01% of a file's keys "
                 "appear twice (rows otherwise the same). Parquet comes from three writers, and every tool reads "
                 "the same files: compara-gen-data's own (every column a string, uncompressed), and, on the "
                 "unique-key pairs, the generator's CSV pair as Polars (`pl.read_csv(csv).write_parquet(out)`) "
                 "and DuckDB (`COPY (SELECT * FROM read_csv(csv)) TO out (FORMAT parquet)`) write it with their "
                 "defaults: their own type inference, compression and row groups, in a process of their own "
                 "before any timing. As file A of each pair holds it:\n\n" + "\n".join(files) + "\n\n")
        queries = ("Each tool computes what compara does: values compared as text in CSV and by value in "
                   "Parquet (in the file's types), and an empty field equal only to an empty field. Where no key "
                   "repeats, each runs its plainest query: DuckDB reads each file where it is "
                   "(`read_csv(..., all_varchar = true)` or `read_parquet`) and counts over a FULL OUTER JOIN on the "
                   "key; Polars scans both (`scan_csv(infer_schema=False)` or `scan_parquet`) and counts over a "
                   "full join, on its default engine, which its streaming engine did not beat; "
                   + (f"qsv runs `qsv diff -k {key}` on the files with the ignored column cut (`qsv select`, "
                      "untimed), its output written to a file and the counts read back from it, unchanged being "
                      "the left file's rows (`qsv count`) less the changed and removed; " if "qsv" in names else "")
                   + ("ClickHouse runs `clickhouse local` with the files read where they are (`file`, CSV as "
                      "String columns), a FULL OUTER JOIN on the key with `join_use_nulls = 1`, so a row missing "
                      "from one side has NULLs there, and a NULL counted equal only to a NULL; " if "clickhouse" in names
                      else "")
                   + "every tool on every core. Where keys repeat, a key that appears twice in a file is compared "
                   "by its first row, and the other tools have to be told so: DuckDB loads each CSV into a table "
                   "once for a row order and keeps each key's first row (`QUALIFY row_number()` by `rowid`; on "
                   "Parquet by `file_row_number`, read in place), Polars adds `unique(keep=\"first\")`, and qsv, "
                   "whose keys must be unique, is given each key's first row by `qsv extdedup`, untimed. "
                   + ("ClickHouse is not run there. " if "clickhouse" in names else ""))
    else:
        pairs = ("Each pair is written by `compara-gen-data` (seed 7) in each format, and every tool reads the "
                 "same files. ")
        queries = ("Each tool computes what compara does: values compared as text in CSV and "
                   "by value in Parquet, an empty field equal only to an empty field, and a key that appears "
                   "twice in a file compared by its first row. DuckDB reads Parquet where it is (`read_parquet`, "
                   "its row numbers from the file) and loads each CSV into a table once, for a row order to take "
                   "the first row by; Polars runs its lazy API, which its streaming engine did not beat. "
                   + ("qsv diff reads CSV only, has no option to leave a column out and says its keys must be "
                      "unique (these tables repeat a few): the ignored column is cut from both files with `qsv "
                      "select` and each key's first row is kept with `qsv extdedup`, both before the timing and "
                      "not charged to qsv, and the timed process is "
                      f"`qsv diff -k {key} LEFT RIGHT` on every core, its output written to a file; "
                      "the counts are read back from that output, and unchanged is the left file's rows "
                      "(`qsv count`) less the changed and removed. " if "qsv" in names else ""))
    page = ["# Benchmarks", "", f"Numbers: {link}, {date}.", "",
            f"{what}. Run on GitHub's `{runner}`: {machine}, {env['os']}.", ""] + table(headline)
    if split:
        page += ["", "Where a file repeats 0.01% of its keys, each compared by its first row:", ""] + table(repeated)
    page += ["", note, "",
             "## Every run", "",
             f"The runs go round the tools in turn ({doc['runs']} rounds"
             + (f", each tool once in each page-cache state per round: {', '.join(states)}" if states != ["unstated"] else "")
             + (", the order turned by one tool each round" if split else "")
             + "), so a slow minute on the runner falls on all of them.", ""] + runs + ["",
             "## Versions", "",
             f"compara built from the run's commit ({v['compara']}); DuckDB {v.get('duckdb')} and Polars "
             f"{v.get('polars')} installed from PyPI at their latest in the run, on Python {v.get('python')}."
             + (f" qsv {ver['qsv']} is its release binary, as downloaded in the run." if v.get("qsv") else "")
             + (f" ClickHouse {ver['clickhouse']} is its official release binary (`{v['clickhouse']}`), as "
                "downloaded in the run." if v.get("clickhouse") else ""), "",
             "## Method", "",
             pairs
             + ("Before each run the files that tool reads are put in a stated page-cache state: warm, read "
                "to the end just before the run; cold, dropped from the page cache (`posix_fadvise` "
                "DONTNEED), so the tool reads them from the runner's disk. The state is set for every tool "
                "alike, so no tool starts warmer because of what the one before it did (in runs before this "
                "was done, a tool killed for memory left the next one's inputs evicted: compara's 10M-row "
                "CSV took 0.70 s in one round and 9.33 s in the next, run 44629611468). Each run records the "
                "share of its inputs in the page cache as it starts and its major page faults. Each round "
                "also times reading both inputs from a cold cache, the least any tool's cold run can take. "
                if states != ["unstated"] else "")
             + "Each tool runs in a process of its own; CPU time and peak RSS are that "
             "process's (`wait4`). Peak RSS counts the pages of files a tool maps, which the kernel can drop "
             "and read again under memory pressure: compara maps both inputs, so most of its RSS is the "
             "files themselves. The private part is the tool's own memory (heap, stacks; `RssAnon` of its "
             "process tree, sampled every 5 ms, so a lower bound), what a container limit has to leave it. "
             + queries + "The run "
             f"fails unless every tool that finishes gives the same counts: {gate}.", "",
             "To run it yourself, from a checkout with compara built and `pip install duckdb polars`:", "",
             "```sh",
             "python3 scripts/bench_contenders.py --compara target/release/compara \\",
             "  --gen target/release/compara-gen-data --rows 1m,10m --json results.json",
             "python3 scripts/gen_bench_docs.py results.json --page bench.md",
             "```", ""]
    if "--page" in opts:
        open(opts["--page"], "w").write("\n".join(page))
    lose = ["# Where compara is not ahead", "", f"Numbers: {link}, {date}.", "",
            "Each measure of the benchmarks on which another tool beat compara in this run by more than "
            "2%, read from its results: wall time, CPU time, peak memory and, where the run sampled it, "
            "peak private memory, on each input, with unique keys or repeated. Closer than that is a tie. "
            "Peak memory counts the input files compara maps, which the kernel can drop; private memory is "
            "what the tool holds itself.", ""]
    lose += [f"- {x[0].upper() + x[1:]}." for x in losses] or ["- None in this run."]
    if "--losses" in opts:
        open(opts["--losses"], "w").write("\n".join(lose) + "\n")
    print(f"benchmark docs from run {run}: {len(inputs)} inputs, {len(names)} tools, {len(losses)} losses")


if __name__ == "__main__":
    main()
