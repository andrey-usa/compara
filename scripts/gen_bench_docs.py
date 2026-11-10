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
NAMES = {"compara": "compara", "duckdb": "DuckDB", "polars": "Polars"}
FORMATS = {"csv": "CSV", "parquet": "Parquet", "ndjson": "NDJSON"}


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


def mb(n):
    return f"{n / 1e6:,.0f} MB"


def size(rows):
    return rows.upper() if isinstance(rows, str) else f"{rows:,}"


def cell(r, best):
    if r.get("failed"):
        return f"{r['failed']} after {r['wall_s']:.0f} s, at {r['peak_rss_mb']:,} MB"
    wall = f"{r['wall_s']:.2f} s"
    return f"{'**' + wall + '**' if best else wall} · {r['cpu_s']:.1f} s · {r['peak_rss_mb']:,} MB"


def label(inp):
    return f"{size(inp['rows'])} rows, {FORMATS.get(inp['format'], inp['format'])}, {mb(inp['bytes'])}"


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
    doc = json.load(open(src)) if os.path.exists(src) else fetch(src)
    run = doc.get("run_id")
    if not run:
        sys.exit("these results name no run: the docs cite one for every number")
    link = f"[bench-contenders run {run}](https://github.com/{REPO}/actions/runs/{run})"
    v, env, inputs = doc["versions"], doc["env"], doc["inputs"]
    names = [n for n in NAMES if all(n in inp["tools"] for inp in inputs)]
    ver = {"compara": v["compara"].split()[-1], "duckdb": v.get("duckdb", ""), "polars": v.get("polars", "")}
    head = [f"{NAMES[n]} {ver[n]}".strip() for n in names]
    date = doc["generated_utc"][:10]
    machine = f"{env['cpu']}, {env['cores']} cores, {env['memory_gb']} GB"
    what = (f"Two tables of {doc['columns']} columns compared on a composite key "
            f"(`-k {','.join(doc['key'])} -i {doc['ignore']}`), every tool's unchanged, changed, added "
            f"and removed counts checked identical")

    table = ["| input, both files | " + " | ".join(head) + " |", "|---|" + "---|" * len(names)]
    losses = []
    for inp in inputs:
        rs = inp["tools"]
        done = [n for n in names if not rs[n].get("failed")]
        fastest = min(rs[n]["wall_s"] for n in done)
        table.append(f"| {label(inp)} | " + " | ".join(cell(rs[n], rs[n].get("wall_s") == fastest and n in done)
                                                         for n in names) + " |")
        for key, what_, fmt in (("wall_s", "wall time", lambda x: f"{x:.2f} s"),
                                ("cpu_s", "CPU time", lambda x: f"{x:.1f} s"),
                                ("peak_rss_mb", "peak memory", lambda x: f"{x:,} MB")):
            mine = rs["compara"][key]
            for n in done[1:] if done and done[0] == "compara" else []:
                if rs[n][key] < mine:
                    losses.append(f"{what_} on {label(inp)}: {NAMES[n]} {fmt(rs[n][key])}, compara {fmt(mine)}")
    loss_line = ("Where another tool did better in this run: " + "; ".join(losses) + "."
                 if losses else "No other tool did better than compara on any measure in this run.")
    note = ("Each cell: wall time, best of {runs} · CPU time (user and system) · peak memory, of the best "
            "run; fastest in bold. DuckDB and Polars run from Python, and their time starts at their first "
            "read, after the interpreter's start and the import. A tool the runner's kernel stopped for "
            "lack of memory says so.").format(runs=doc["runs"])

    readme = ["## Benchmarks", "",
              f"{what}, on GitHub's `ubuntu-latest` ({machine}), {link}, {date}:", ""] + table + [
              "", note, "", loss_line, "",
              f"The method, every run and object storage: [Benchmarks]({SITE}benchmarks/results.html).", ""]
    if "--readme" in opts:
        p = opts["--readme"]
        text = open(p).read()
        start = text.index("## Benchmarks")
        nxt = text.find("\n## ", start + 1)
        text = text[:start] + "\n".join(readme) + ("\n" + text[nxt + 1:] if nxt >= 0 else "")
        open(p, "w").write(text)

    runs = ["| input | tool | every run: wall · CPU · peak memory |", "|---|---|---|"]
    for inp in inputs:
        for n in names:
            r = inp["tools"][n]
            every = cell(r, False) if r.get("failed") else "; ".join(
                f"{x['wall_s']:.2f} s · {x['cpu_s']:.1f} s · {x['peak_rss_mb']:,} MB" for x in r["runs"])
            runs.append(f"| {label(inp)} | {NAMES[n]} | {every} |")
    gate = "; ".join(
        f"{label(inp)}: " + ", ".join(f"{v_:,} {k}" for k, v_ in inp["tools"]["compara"]["counts"].items())
        for inp in inputs)
    page = ["# Benchmarks", "", f"Numbers: {link}, {date}.", "",
            f"{what}. Run on GitHub's `ubuntu-latest`: {machine}, {env['os']}.", ""] + table + ["", note, "",
            "## Every run", "",
            f"The runs go round the tools in turn ({doc['runs']} rounds), so a slow minute on the runner "
            "falls on all of them.", ""] + runs + ["",
            "## Versions", "",
            f"compara built from the run's commit ({v['compara']}); DuckDB {v.get('duckdb')} and Polars "
            f"{v.get('polars')} installed from PyPI at their latest in the run, on Python {v.get('python')}.", "",
            "## Method", "",
            "Each pair is written by `compara-gen-data` (seed 7) in each format, and every tool reads the "
            "same files. Each tool runs in a process of its own; CPU time and peak memory are that "
            "process's (`wait4`). Each tool computes what compara does: values compared as text in CSV and "
            "by value in Parquet, an empty field equal only to an empty field, and a key that appears "
            "twice in a file compared by its first row. DuckDB reads Parquet where it is (`read_parquet`, "
            "its row numbers from the file) and loads each CSV into a table once, for a row order to take "
            "the first row by; Polars runs its lazy API, which its streaming engine did not beat. The run "
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
            "Each measure of the benchmarks on which another tool beat compara in this run, read from its "
            "results: wall time, CPU time and peak memory, on each input.", ""]
    lose += [f"- {x[0].upper() + x[1:]}." for x in losses] or ["- None in this run."]
    if "--losses" in opts:
        open(opts["--losses"], "w").write("\n".join(lose) + "\n")
    print(f"benchmark docs from run {run}: {len(inputs)} inputs, {len(names)} tools, {len(losses)} losses")


if __name__ == "__main__":
    main()
