# Benchmarks

Numbers: [bench-contenders run 42782167828](https://github.com/andrey-usa/compara/actions/runs/42782167828), 2026-11-10.

Two tables of 20 columns compared on a composite key (`-k account_id,txn_id -i updated_at`), every tool's unchanged, changed, added and removed counts checked identical. Run on GitHub's `ubuntu-latest`: AMD EPYC 9V45 96-Core Processor, 4 cores, 15 GB, Linux-7.0.0-1019-azure-x86_64-with-glibc2.43.

| input, both files | compara 0.6.0 | DuckDB 2.0.0 | Polars 2.1.0 |
|---|---|---|---|
| 1M rows, CSV, 368 MB | **0.08 s** · 0.2 s · 411 MB | 1.90 s · 5.3 s · 1,213 MB | 2.10 s · 8.3 s · 2,262 MB |
| 1M rows, Parquet, 161 MB | **0.11 s** · 0.3 s · 317 MB | 1.10 s · 1.9 s · 1,358 MB | 2.11 s · 7.4 s · 2,430 MB |
| 10M rows, CSV, 3,679 MB | **0.74 s** · 2.2 s · 4,031 MB | 14.59 s · 53.7 s · 8,797 MB | out of memory after 12 s, at 13,345 MB |
| 10M rows, Parquet, 1,609 MB | **0.85 s** · 2.7 s · 2,739 MB | 6.40 s · 23.9 s · 7,001 MB | out of memory after 9 s, at 13,345 MB |

Each cell: wall time, best of 3 · CPU time (user and system) · peak memory, of the best run; fastest in bold. DuckDB and Polars run from Python, and their time starts at their first read, after the interpreter's start and the import. A tool the runner's kernel stopped for lack of memory says so.

## Every run

The runs go round the tools in turn (3 rounds), so a slow minute on the runner falls on all of them.

| input | tool | every run: wall · CPU · peak memory |
|---|---|---|
| 1M rows, CSV, 368 MB | compara | 0.09 s · 0.2 s · 411 MB; 0.08 s · 0.2 s · 411 MB; 0.09 s · 0.2 s · 411 MB |
| 1M rows, CSV, 368 MB | DuckDB | 1.97 s · 5.5 s · 1,218 MB; 1.90 s · 5.3 s · 1,213 MB; 1.94 s · 5.4 s · 1,203 MB |
| 1M rows, CSV, 368 MB | Polars | 2.21 s · 8.5 s · 2,272 MB; 2.10 s · 8.3 s · 2,262 MB; 2.22 s · 8.7 s · 2,378 MB |
| 1M rows, Parquet, 161 MB | compara | 0.12 s · 0.3 s · 318 MB; 0.11 s · 0.3 s · 317 MB; 0.11 s · 0.3 s · 318 MB |
| 1M rows, Parquet, 161 MB | DuckDB | 1.11 s · 1.9 s · 1,262 MB; 1.11 s · 1.9 s · 1,374 MB; 1.10 s · 1.9 s · 1,358 MB |
| 1M rows, Parquet, 161 MB | Polars | 2.21 s · 7.6 s · 2,502 MB; 2.11 s · 7.4 s · 2,430 MB; 2.13 s · 7.5 s · 2,567 MB |
| 10M rows, CSV, 3,679 MB | compara | 0.74 s · 2.2 s · 4,031 MB; 6.91 s · 2.7 s · 4,031 MB; 0.79 s · 2.4 s · 4,031 MB |
| 10M rows, CSV, 3,679 MB | DuckDB | 14.59 s · 53.7 s · 8,797 MB; 14.75 s · 54.0 s · 8,965 MB; 15.10 s · 55.0 s · 8,976 MB |
| 10M rows, CSV, 3,679 MB | Polars | out of memory after 12 s, at 13,345 MB |
| 10M rows, Parquet, 1,609 MB | compara | 0.85 s · 2.7 s · 2,739 MB; 2.34 s · 2.4 s · 2,885 MB; 0.96 s · 3.0 s · 2,742 MB |
| 10M rows, Parquet, 1,609 MB | DuckDB | 6.40 s · 23.9 s · 7,001 MB; 6.51 s · 24.7 s · 7,141 MB; 6.51 s · 24.6 s · 7,005 MB |
| 10M rows, Parquet, 1,609 MB | Polars | out of memory after 9 s, at 13,345 MB |

## Versions

compara built from the run's commit (compara 0.6.0); DuckDB 2.0.0 and Polars 2.1.0 installed from PyPI at their latest in the run, on Python 3.13.18.

## Method

Each pair is written by `compara-gen-data` (seed 7) in each format, and every tool reads the same files. Each tool runs in a process of its own; CPU time and peak memory are that process's (`wait4`). Each tool computes what compara does: values compared as text in CSV and by value in Parquet, an empty field equal only to an empty field, and a key that appears twice in a file compared by its first row. DuckDB reads Parquet where it is (`read_parquet`, its row numbers from the file) and loads each CSV into a table once, for a row order to take the first row by; Polars runs its lazy API, which its streaming engine did not beat. The run fails unless every tool that finishes gives the same counts: 1M rows, CSV, 368 MB: 938,951 unchanged, 60,049 changed, 1,000 added, 1,000 removed; 1M rows, Parquet, 161 MB: 938,951 unchanged, 60,049 changed, 1,000 added, 1,000 removed; 10M rows, CSV, 3,679 MB: 9,390,680 unchanged, 599,320 changed, 10,000 added, 10,000 removed; 10M rows, Parquet, 1,609 MB: 9,390,680 unchanged, 599,320 changed, 10,000 added, 10,000 removed.

To run it yourself, from a checkout with compara built and `pip install duckdb polars`:

```sh
python3 scripts/bench_contenders.py --compara target/release/compara \
  --gen target/release/compara-gen-data --rows 1m,10m --json results.json
python3 scripts/gen_bench_docs.py results.json --page bench.md
```
