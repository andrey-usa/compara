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
- Ten million rows × 20 columns of CSV in about a second on GitHub's
  4-core runner with the files cached, where the fastest of DuckDB,
  Polars, qsv and ClickHouse takes about 10 s; on Parquet as Polars or
  DuckDB write it, about twice as fast as the fastest of them (1.7–2.2 s
  vs 3.9–4.7 s); from a cold disk, little more than the time to read the
  files (CSV 9.3 s vs 8.6 s to read)
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

Two tables of 20 columns compared on a composite key (`-k account_id,txn_id -i updated_at`), every tool's unchanged, changed, added and removed counts checked identical, on GitHub's `ubuntu-24.04, image 20261116.376.1` (AMD EPYC 7763 64-Core Processor, 4 cores, 15 GB), [bench-contenders run 45112799027](https://github.com/andrey-usa/compara/actions/runs/45112799027), 2026-11-23, on pairs with no key twice in a file:

| input, both files | compara 0.8.0 | DuckDB 2.0.1 | Polars 2.1.0 | qsv 29.0.0 | ClickHouse 26.9.36.3 |
|---|---|---|---|---|---|
| 1M rows, CSV, 368 MB | **0.12 s (0.12-0.15)** · cold 0.69 s (0.68-0.71) · 0.3 s · 411 MB (56 private) | 1.27 s (1.26-1.29) · cold 1.30 s (1.28-1.67) · 4.1 s · 671 MB (608 private) | 0.93 s (0.92-0.93) · cold 1.38 s (1.34-2.15) · 3.6 s · 1,431 MB (1,370 private) | 0.91 s (0.90-1.11) · cold 1.12 s (1.08-1.13) · 2.4 s · 135 MB (105 private) | 1.49 s (1.46-1.89) · cold 1.59 s (1.58-1.73) · 4.5 s · 1,300 MB (948 private) |
| 1M rows, Parquet by compara-gen-data, 161 MB | **0.15 s (0.15-0.16)** · cold 0.25 s (0.24-0.25) · 0.4 s · 323 MB (165 private) | 1.16 s (1.11-1.18) · cold 1.20 s (1.16-1.21) · 1.4 s · 761 MB (701 private) | 0.62 s (0.61-0.64) · cold 0.67 s (0.66-0.70) · 2.2 s · 1,635 MB (1,568 private) | not run | 0.83 s (0.83-0.86) · cold 0.94 s (0.93-0.99) · 2.0 s · 1,036 MB (783 private) |
| 1M rows, Parquet by Polars, 61 MB | **0.26 s (0.25-0.26)** · cold 0.29 s (0.28-0.31) · 0.8 s · 362 MB (301 private) | 0.55 s (0.54-0.57) · cold 0.55 s (0.55-0.57) · 2.1 s · 568 MB (508 private) | 0.50 s (0.49-0.51) · cold 0.54 s (0.53-0.55) · 2.0 s · 1,115 MB (1,044 private) | not run | 0.89 s (0.86-0.91) · cold 0.89 s (0.88-0.91) · 2.2 s · 951 MB (724 private) |
| 1M rows, Parquet by DuckDB, 97 MB | **0.22 s (0.22-0.23)** · cold 0.28 s (0.27-0.28) · 0.7 s · 384 MB (288 private) | 0.47 s (0.46-0.47) · cold 0.48 s (0.48-0.49) · 1.7 s · 533 MB (474 private) | 0.43 s (0.41-0.44) · cold 0.47 s (0.47-0.48) · 1.8 s · 1,015 MB (944 private) | not run | 0.79 s (0.76-0.80) · cold 0.83 s (0.80-0.86) · 2.0 s · 990 MB (756 private) |
| 10M rows, CSV, 3,679 MB | **0.94 s (0.94-0.95)** · cold 9.32 s (9.30-9.33) · 3.1 s · 4,030 MB (511 private) | 9.87 s (9.81-9.89) · cold 10.19 s (10.16-10.23) · 37.1 s · 4,240 MB (4,178 private) | 9.75 s (9.67-9.99) · cold 13.37 s (13.25-13.56) · 35.7 s · 9,880 MB (9,816 private) | 11.72 s (10.07-11.96) · cold 10.76 s (10.38-12.29) · 27.8 s · 2,123 MB (2,096 private) | 30.04 s (29.88-30.26) · cold 29.97 s (29.77-30.30) · 75.8 s · 7,662 MB (5,986 private) |
| 10M rows, Parquet by compara-gen-data, 1,609 MB | **1.16 s (1.14-1.17)** · cold 4.03 s (4.00-4.05) · 3.8 s · 2,776 MB (1,261 private) | 4.93 s (4.87-5.13) · cold 5.91 s (5.79-6.04) · 17.3 s · 5,044 MB (4,997 private) | 5.97 s (5.87-5.99) · cold 7.67 s (7.50-7.77) · 19.2 s · 10,760 MB (10,723 private) | not run | 23.82 s (23.52-23.98) · cold 23.99 s (23.84-24.21) · 51.6 s · 7,102 MB (6,157 private) |
| 10M rows, Parquet by Polars, 609 MB | **2.24 s (2.22-2.43)** · cold 2.57 s (2.53-2.59) · 7.8 s · 3,138 MB (2,551 private) | 4.74 s (4.70-4.81) · cold 4.80 s (4.72-4.84) · 18.4 s · 3,572 MB (3,525 private) | 5.39 s (5.28-5.42) · cold 5.82 s (5.76-5.86) · 20.4 s · 9,084 MB (9,039 private) | not run | 23.39 s (23.27-23.56) · cold 23.47 s (23.33-23.82) · 49.3 s · 6,708 MB (6,212 private) |
| 10M rows, Parquet by DuckDB, 964 MB | **1.73 s (1.72-1.74)** · cold 2.69 s (2.67-2.73) · 5.9 s · 3,589 MB (2,664 private) | 3.93 s (3.91-3.95) · cold 3.95 s (3.92-4.00) · 15.2 s · 3,417 MB (3,370 private) | 4.66 s (4.63-4.79) · cold 5.47 s (5.39-5.51) · 17.8 s · 8,534 MB (8,491 private) | not run | 22.88 s (22.82-23.12) · cold 23.20 s (23.03-23.26) · 47.1 s · 6,504 MB (5,849 private) |

The same comparison where a file repeats 0.01% of its keys, each compared by its first row as compara does: DuckDB, Polars and qsv have to be told to keep the first row (the page says how), which costs them time:

| input, both files | compara 0.8.0 | DuckDB 2.0.1 | Polars 2.1.0 | qsv 29.0.0 |
|---|---|---|---|---|
| 1M rows, CSV, repeated keys, 368 MB | **0.12 s (0.12-0.12)** · cold 0.70 s (0.69-0.74) · 0.3 s · 411 MB (56 private) | 3.16 s (3.15-3.18) · cold 3.15 s (3.14-3.16) · 8.6 s · 1,111 MB (1,051 private) | 3.21 s (3.17-3.31) · cold 3.45 s (3.25-3.50) · 12.0 s · 2,096 MB (2,035 private) | 0.92 s (0.90-1.08) · cold 1.12 s (0.92-1.14) · 2.3 s · 222 MB (191 private) |
| 1M rows, Parquet by compara-gen-data, repeated keys, 161 MB | **0.15 s (0.15-0.17)** · cold 0.25 s (0.25-0.26) · 0.4 s · 339 MB (181 private) | 1.74 s (1.70-1.79) · cold 1.78 s (1.74-1.81) · 2.7 s · 1,226 MB (1,166 private) | 3.33 s (3.27-3.38) · cold 3.41 s (3.36-3.47) · 10.8 s · 2,206 MB (2,139 private) | not run |
| 10M rows, CSV, repeated keys, 3,679 MB | **0.94 s (0.94-0.98)** · cold 9.30 s (9.30-9.33) · 3.1 s · 4,031 MB (511 private) | 24.84 s (24.49-24.93) · cold 24.92 s (24.55-25.01) · 88.5 s · 8,742 MB (8,692 private) | out of memory after 17 s, at 13,319 MB | 10.66 s (9.87-12.55) · cold 11.05 s (10.85-12.41) · 26.3 s · 1,418 MB (1,391 private) |
| 10M rows, Parquet by compara-gen-data, repeated keys, 1,609 MB | **1.16 s (1.13-1.17)** · cold 4.04 s (4.02-4.05) · 3.7 s · 2,855 MB (1,340 private) | 9.87 s (9.66-9.93) · cold 10.39 s (10.22-10.76) · 34.3 s · 6,880 MB (6,827 private) | out of memory after 74 s, at 13,291 MB | not run |

Each cell: wall time from a warm page cache, median of 5 runs (fastest-slowest in brackets) · CPU time (user and system) · peak memory (RSS, and in brackets the private part of it), of the median run; fastest median in bold. Every run starts from a stated page cache: warm (its inputs read to the end just before) is the first figure, cold (its inputs dropped from the page cache, so read from the disk) the second. DuckDB and Polars run from Python, and their time starts at their first read, after the interpreter's start and the import; qsv and ClickHouse are binaries timed as a whole process, like compara. qsv reads CSV only and is given the files with the ignored column already taken out (and, where keys repeat, each key's first row only), untimed (the page says how). ClickHouse (clickhouse-local) runs on the pairs with unique keys. The tools run in a different order each round. A tool the runner's kernel stopped for lack of memory says so. Reading both inputs from a cold cache alone took (median): 1M rows, CSV, 368 MB 0.59 s; 1M rows, Parquet by compara-gen-data, 161 MB 0.11 s; 1M rows, Parquet by Polars, 61 MB 0.02 s; 1M rows, Parquet by DuckDB, 97 MB 0.03 s; 1M rows, CSV, repeated keys, 368 MB 0.60 s; 1M rows, Parquet by compara-gen-data, repeated keys, 161 MB 0.10 s; 10M rows, CSV, 3,679 MB 8.64 s; 10M rows, Parquet by compara-gen-data, 1,609 MB 3.60 s; 10M rows, Parquet by Polars, 609 MB 1.18 s; 10M rows, Parquet by DuckDB, 964 MB 2.05 s; 10M rows, CSV, repeated keys, 3,679 MB 8.63 s; 10M rows, Parquet by compara-gen-data, repeated keys, 1,609 MB 3.61 s.

Where another tool did better in this run (by more than 2%): peak memory on 1M rows, CSV, 368 MB: qsv 135 MB, compara 411 MB; peak memory on 1M rows, CSV, repeated keys, 368 MB: qsv 222 MB, compara 411 MB; peak memory on 10M rows, CSV, 3,679 MB: qsv 2,123 MB, compara 4,030 MB; peak memory on 10M rows, Parquet by DuckDB, 964 MB: DuckDB 3,417 MB, compara 3,589 MB; peak memory on 10M rows, CSV, repeated keys, 3,679 MB: qsv 1,418 MB, compara 4,031 MB.

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
