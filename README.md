# compara

[![CI](https://github.com/andrey-usa/compara/actions/workflows/ci.yml/badge.svg)](https://github.com/andrey-usa/compara/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/compara.svg)](https://crates.io/crates/compara)
[![PyPI](https://img.shields.io/pypi/v/compara.svg)](https://pypi.org/project/compara/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/documentation-site-blue.svg)](https://andrey-usa.github.io/compara/)

Extremely fast table comparison: two tables on a composite key, with counts,
per-column statistics and a self-contained HTML report. One tool for every
recurring comparison: a nightly export against yesterday's, a CSV against the
Parquet a warehouse emits, a migration's source against its target.

> Faster software for a better planet — less energy, less compute, more time for life.

- Reads **CSV, TSV, JSON (newline-delimited or one array of objects) and
  Parquet**, and the two sides need not be in the same format.
- Ten million rows × 20 columns in a second or two on four cores, CSV or Parquet
  ([benchmarks](https://andrey-usa.github.io/compara/benchmarks/results.html)).
- Reads and writes **object storage and the web** directly (`s3://`,
  `gs://`, `az://`, `https://`), with each cloud's own credentials, and
  fetches only the Parquet columns it compares
  ([object storage](https://andrey-usa.github.io/compara/guides/cloud.html)).
- One static binary, and a distroless container image; the report is a
  single HTML file with nothing to host.
- Exit codes made for pipelines: `0` identical, `1` differences, `2` error,
  `3` duplicate keys (with `--fail-on-dups`).

## Install

Prebuilt binaries for Linux (x86_64, aarch64), macOS (arm64, x86_64) and
Windows (x64) are on the [releases page](https://github.com/andrey-usa/compara/releases),
and the same binaries through package managers:

| Channel | Command |
|---|---|
| Cargo | `cargo install --locked compara` |
| pip / uv | `pip install compara` · `uv tool install compara` |
| Homebrew | `brew install andrey-usa/tap/compara` |
| Scoop | `scoop bucket add andrey-usa https://github.com/andrey-usa/scoop-bucket` then `scoop install compara` |
| Docker | `docker run --rm -v "$PWD:/data" ghcr.io/andrey-usa/compara compare a.csv b.csv -k id` |

## Quick start

```console
$ compara compare examples/orders_2026-08.csv examples/orders_2026-09.csv -k order_id,line_no -i updated_at
A 800 rows | B 799 rows | matched 787 (changed 102) | added 10 | removed 12 | duplicate keys A 1, B 1 | engine turbo, 0.011s
note: column only in B, not compared: carrier
$ echo $?
1
$ compara compare examples/orders_2026-08.csv examples/orders_2026-09.csv -k order_id,line_no -i updated_at -o report.html
A 800 rows | B 799 rows | matched 787 (changed 102) | added 10 | removed 12 | duplicate keys A 1, B 1 | engine turbo, 0.001s
Report: report.html (20 KB)
note: column only in B, not compared: carrier
```

Two monthly order exports from the repository's `examples/`, keyed on
`(order_id, line_no)`, with `updated_at` left out. Exit code 1 says they
differ; the report holds the counts, per-column statistics and the changed,
added, removed and duplicate rows. The note names a column only one of the
files has: it is not compared, and it does not change the exit code. More: [Everyday comparisons](https://andrey-usa.github.io/compara/start/tutorial.html),
and every option of `compare`: [compare options](https://andrey-usa.github.io/compara/reference/options.html).

## For AI agents

- **Skill:** `compara skill` prints the guide an agent reads to use this
  version of compara.
- **Machine-readable result:** `--json summary.json` (or `--json -` for
  standard output) writes the counts and per-column statistics, in the shape
  [docs/contract.schema.json](https://github.com/andrey-usa/compara/blob/main/docs/contract.schema.json)
  gives.
- **Exit codes:** `0` identical, `1` differences, `2` error, `3` duplicate
  keys with `--fail-on-dups`; `--exit-zero` for a job whose output is the report.
- **Read-only by default:** `compare` without `-o`, `--json` or
  `--export-dir` writes nothing; `--summary` makes that a promise and refuses
  any flag that would write.
- **Docs as markdown:** [llms.txt](https://andrey-usa.github.io/compara/llms.txt) lists every page of the
  docs site, each one also served as `.md`.

The loop an agent runs: `compara columns` on each file to choose the key,
`compare --summary` for the counts, then `--json -` for the columns that
changed and `--export-dir` for the rows.

## Benchmarks

Two tables of 20 columns compared on a composite key (`-k account_id,txn_id -i updated_at`), every tool's unchanged, changed, added and removed counts checked identical, on GitHub's `ubuntu-24.04, image 20261116.376.1` (AMD EPYC 7763 64-Core Processor, 4 cores, 15 GB), [bench-contenders run 44635612282](https://github.com/andrey-usa/compara/actions/runs/44635612282), 2026-11-19:

| input, both files | compara 0.7.0 | DuckDB 2.0.0 | Polars 2.1.0 | qsv 29.0.0 |
|---|---|---|---|---|
| 1M rows, CSV, 368 MB | **0.13 s (0.12-0.13)** · cold 0.69 s (0.69-0.72) · 0.3 s · 411 MB (56 private) | 3.32 s (3.25-3.37) · cold 3.39 s (3.30-4.13) · 9.2 s · 1,185 MB (1,124 private) | 3.45 s (3.26-3.56) · cold 3.63 s (3.50-3.69) · 12.3 s · 2,154 MB (2,094 private) | 1.05 s (0.95-1.12) · cold 1.04 s (0.97-1.15) · 2.6 s · 299 MB (269 private) |
| 1M rows, Parquet, 161 MB | **0.17 s (0.17-0.17)** · cold 0.26 s (0.25-0.27) · 0.5 s · 339 MB (181 private) | 1.95 s (1.88-2.00) · cold 2.00 s (1.93-2.08) · 3.3 s · 1,161 MB (1,101 private) | 3.54 s (3.47-3.65) · cold 3.63 s (3.48-3.67) · 11.5 s · 2,207 MB (2,143 private) | not run |
| 10M rows, CSV, 3,679 MB | **0.99 s (0.98-1.03)** · cold 9.37 s (9.36-9.38) · 3.2 s · 4,031 MB (512 private) | 26.14 s (25.69-26.43) · cold 26.20 s (26.01-26.30) · 92.3 s · 8,813 MB (8,744 private) | out of memory after 17 s, at 13,344 MB | 12.13 s (9.98-12.72) · cold 12.24 s (10.68-12.33) · 28.4 s · 2,302 MB (2,273 private) |
| 10M rows, Parquet, 1,609 MB | **1.22 s (1.22-1.30)** · cold 4.08 s (4.06-4.11) · 4.0 s · 2,853 MB (1,338 private) | 10.98 s (10.74-11.45) · cold 12.67 s (12.28-13.04) · 37.9 s · 6,742 MB (6,675 private) | out of memory after 15 s, at 13,345 MB | not run |

Each cell: wall time from a warm page cache, median of 5 runs (fastest-slowest in brackets) · CPU time (user and system) · peak memory (RSS, and in brackets the private part of it), of the median run; fastest median in bold. Every run starts from a stated page cache: warm (its inputs read to the end just before) is the first figure, cold (its inputs dropped from the page cache, so read from the disk) the second. DuckDB and Polars run from Python, and their time starts at their first read, after the interpreter's start and the import; qsv is a binary and is timed as a whole process, like compara, runs on CSV only, and is given the files with the ignored column and the repeated keys already taken out (the page says how). A tool the runner's kernel stopped for lack of memory says so. Reading both inputs from a cold cache alone took (median): 1M rows, CSV, 368 MB 0.60 s; 1M rows, Parquet, 161 MB 0.09 s; 10M rows, CSV, 3,679 MB 8.62 s; 10M rows, Parquet, 1,609 MB 3.61 s.

Where another tool did better in this run (by more than 2%): peak memory on 1M rows, CSV, 368 MB: qsv 299 MB, compara 411 MB; peak memory on 10M rows, CSV, 3,679 MB: qsv 2,302 MB, compara 4,031 MB.

The method, every run and object storage: [Benchmarks](https://andrey-usa.github.io/compara/benchmarks/results.html).

## Documentation

<!-- Written by docs-site/build.py from docs-site/site.toml: edit that, then run it with --write. -->

The docs site, https://andrey-usa.github.io/compara/, has the same parts for every tool:

- **Start:** [Install](https://andrey-usa.github.io/compara/start/install.html) · [Quick start](https://andrey-usa.github.io/compara/start/quick-start.html) · [Everyday comparisons](https://andrey-usa.github.io/compara/start/tutorial.html)
- **Guides:** [Compare tables in object storage and on the web](https://andrey-usa.github.io/compara/guides/cloud.html) · [Run compara in a container, in CI or in Kubernetes](https://andrey-usa.github.io/compara/guides/containers.html) · [Name a recurring comparison](https://andrey-usa.github.io/compara/guides/profiles.html)
- **Concepts:** [Engines and formats](https://andrey-usa.github.io/compara/concepts/engines.html)
- **Reference:** [compare options](https://andrey-usa.github.io/compara/reference/options.html) · [The JSON summary](https://andrey-usa.github.io/compara/reference/json-summary.html) · [compara --help](https://andrey-usa.github.io/compara/reference/help.html)
- **Agents:** [For AI agents](https://andrey-usa.github.io/compara/agents/overview.html) · [Skill](https://andrey-usa.github.io/compara/agents/skill.html)
- **Benchmarks:** [Results](https://andrey-usa.github.io/compara/benchmarks/results.html) · [Where compara is not ahead](https://andrey-usa.github.io/compara/benchmarks/losses.html)
- **Project:** [Changelog](https://andrey-usa.github.io/compara/project/changelog.html) · [Contributing](https://andrey-usa.github.io/compara/project/contributing.html) · [Security](https://andrey-usa.github.io/compara/project/security.html)

For agents: [llms.txt](https://andrey-usa.github.io/compara/llms.txt) lists every page as markdown, and [llms-full.txt](https://andrey-usa.github.io/compara/llms-full.txt) is the whole site in one file.

## Contributing

Building, testing and the performance gate: [CONTRIBUTING.md](https://github.com/andrey-usa/compara/blob/main/CONTRIBUTING.md).

## License

MIT: [LICENSE](https://github.com/andrey-usa/compara/blob/main/LICENSE).
