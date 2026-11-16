# Benchmarks

Numbers: [bench-contenders run 43794429051](https://github.com/andrey-usa/compara/actions/runs/43794429051), 2026-11-16.

Two tables of 20 columns compared on a composite key (`-k account_id,txn_id -i updated_at`), every tool's unchanged, changed, added and removed counts checked identical. Run on GitHub's `ubuntu-24.04, image 20261111.127.1`: Intel(R) Xeon(R) 6975P-C, 4 cores, 15 GB, Linux-7.0.0-1014-aws-x86_64-with-glibc2.39.

| input, both files | compara 0.6.1 | DuckDB 2.0.0 | Polars 2.1.0 | qsv 29.0.0 |
|---|---|---|---|---|
| 1M rows, CSV, 368 MB | **0.09 s** · 0.3 s · 411 MB | 2.29 s · 6.3 s · 1,080 MB | 2.25 s · 8.8 s · 2,364 MB | 0.78 s · 2.0 s · 145 MB |
| 1M rows, Parquet, 161 MB | **0.13 s** · 0.4 s · 322 MB | 1.26 s · 2.1 s · 1,167 MB | 2.27 s · 7.9 s · 2,382 MB | not run |
| 10M rows, CSV, 3,679 MB | **0.84 s** · 2.8 s · 4,030 MB | 17.16 s · 64.6 s · 8,793 MB | out of memory after 10 s, at 13,341 MB | 8.31 s · 22.1 s · 1,008 MB |
| 10M rows, Parquet, 1,609 MB | **1.06 s** · 3.5 s · 2,730 MB | 7.25 s · 26.8 s · 7,142 MB | out of memory after 9 s, at 13,342 MB | not run |

Each cell: wall time, best of 3 · CPU time (user and system) · peak memory, of the best run; fastest in bold. DuckDB and Polars run from Python, and their time starts at their first read, after the interpreter's start and the import; qsv is a binary and is timed as a whole process, like compara, runs on CSV only, and is given the files with the ignored column and the repeated keys already taken out (the page says how). A tool the runner's kernel stopped for lack of memory says so.

## Every run

The runs go round the tools in turn (3 rounds), so a slow minute on the runner falls on all of them.

| input | tool | every run: wall · CPU · peak memory |
|---|---|---|
| 1M rows, CSV, 368 MB | compara | 0.11 s · 0.3 s · 411 MB; 0.11 s · 0.3 s · 411 MB; 0.09 s · 0.3 s · 411 MB |
| 1M rows, CSV, 368 MB | DuckDB | 2.29 s · 6.3 s · 1,080 MB; 2.31 s · 6.4 s · 1,197 MB; 2.32 s · 6.4 s · 1,226 MB |
| 1M rows, CSV, 368 MB | Polars | 2.25 s · 8.8 s · 2,364 MB; 2.25 s · 8.8 s · 2,360 MB; 2.28 s · 8.9 s · 2,461 MB |
| 1M rows, CSV, 368 MB | qsv | 0.80 s · 2.1 s · 143 MB; 0.78 s · 2.0 s · 145 MB; 0.82 s · 2.2 s · 169 MB |
| 1M rows, Parquet, 161 MB | compara | 0.13 s · 0.4 s · 322 MB; 0.13 s · 0.4 s · 322 MB; 0.13 s · 0.4 s · 322 MB |
| 1M rows, Parquet, 161 MB | DuckDB | 1.29 s · 2.2 s · 1,323 MB; 1.26 s · 2.1 s · 1,167 MB; 1.29 s · 2.2 s · 1,423 MB |
| 1M rows, Parquet, 161 MB | Polars | 2.27 s · 7.9 s · 2,382 MB; 2.27 s · 7.9 s · 2,559 MB; 2.29 s · 7.9 s · 2,485 MB |
| 10M rows, CSV, 3,679 MB | compara | 0.84 s · 2.8 s · 4,030 MB; 7.31 s · 4.2 s · 4,030 MB; 0.92 s · 3.0 s · 4,030 MB |
| 10M rows, CSV, 3,679 MB | DuckDB | 17.21 s · 64.2 s · 8,823 MB; 17.23 s · 64.3 s · 8,789 MB; 17.16 s · 64.6 s · 8,793 MB |
| 10M rows, CSV, 3,679 MB | Polars | out of memory after 10 s, at 13,341 MB |
| 10M rows, CSV, 3,679 MB | qsv | 8.31 s · 22.1 s · 1,008 MB; 8.53 s · 22.6 s · 1,017 MB; 8.63 s · 22.7 s · 1,174 MB |
| 10M rows, Parquet, 1,609 MB | compara | 1.06 s · 3.5 s · 2,730 MB; 1.30 s · 3.8 s · 2,807 MB; 1.20 s · 3.6 s · 2,723 MB |
| 10M rows, Parquet, 1,609 MB | DuckDB | 7.32 s · 26.9 s · 7,096 MB; 7.25 s · 26.8 s · 7,142 MB; 7.65 s · 27.0 s · 7,102 MB |
| 10M rows, Parquet, 1,609 MB | Polars | out of memory after 9 s, at 13,342 MB |

## Versions

compara built from the run's commit (compara 0.6.1); DuckDB 2.0.0 and Polars 2.1.0 installed from PyPI at their latest in the run, on Python 3.13.18. qsv 29.0.0 is its release binary, as downloaded in the run.

## Method

Each pair is written by `compara-gen-data` (seed 7) in each format, and every tool reads the same files. Each tool runs in a process of its own; CPU time and peak memory are that process's (`wait4`). Each tool computes what compara does: values compared as text in CSV and by value in Parquet, an empty field equal only to an empty field, and a key that appears twice in a file compared by its first row. DuckDB reads Parquet where it is (`read_parquet`, its row numbers from the file) and loads each CSV into a table once, for a row order to take the first row by; Polars runs its lazy API, which its streaming engine did not beat. qsv diff reads CSV only, has no option to leave a column out and says its keys must be unique (these tables repeat a few): the ignored column is cut from both files with `qsv select` and each key's first row is kept with `qsv extdedup`, both before the timing and not charged to qsv, and the timed process is `qsv diff -k account_id,txn_id LEFT RIGHT` on every core, its output written to a file; the counts are read back from that output, and unchanged is the left file's rows (`qsv count`) less the changed and removed. The run fails unless every tool that finishes gives the same counts: 1M rows, CSV, 368 MB: 938,951 unchanged, 60,049 changed, 1,000 added, 1,000 removed; 1M rows, Parquet, 161 MB: 938,951 unchanged, 60,049 changed, 1,000 added, 1,000 removed; 10M rows, CSV, 3,679 MB: 9,390,680 unchanged, 599,320 changed, 10,000 added, 10,000 removed; 10M rows, Parquet, 1,609 MB: 9,390,680 unchanged, 599,320 changed, 10,000 added, 10,000 removed.

To run it yourself, from a checkout with compara built and `pip install duckdb polars`:

```sh
python3 scripts/bench_contenders.py --compara target/release/compara \
  --gen target/release/compara-gen-data --rows 1m,10m --json results.json
python3 scripts/gen_bench_docs.py results.json --page bench.md
```
