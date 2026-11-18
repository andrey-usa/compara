# Benchmarks

Numbers: [bench-contenders run 44398184156](https://github.com/andrey-usa/compara/actions/runs/44398184156), 2026-11-18.

Two tables of 20 columns compared on a composite key (`-k account_id,txn_id -i updated_at`), every tool's unchanged, changed, added and removed counts checked identical. Run on GitHub's `ubuntu-24.04, image 20261111.370.1`: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz, 4 cores, 15 GB, Linux-7.0.0-1017-azure-x86_64-with-glibc2.39.

| input, both files | compara 0.7.0 | DuckDB 2.0.0 | Polars 2.1.0 | qsv 29.0.0 |
|---|---|---|---|---|
| 1M rows, CSV, 368 MB | **0.13 s** · 0.4 s · 411 MB (56 private) | 3.26 s · 9.2 s · 1,164 MB (1,103 private) | 3.22 s · 12.1 s · 2,208 MB (2,145 private) | 0.96 s · 2.6 s · 132 MB (102 private) |
| 1M rows, Parquet, 161 MB | **0.18 s** · 0.5 s · 320 MB (180 private) | 1.67 s · 2.8 s · 1,231 MB (1,171 private) | 3.00 s · 10.2 s · 2,234 MB (2,170 private) | not run |
| 10M rows, CSV, 3,679 MB | **1.15 s** · 3.9 s · 4,030 MB (511 private) | 25.67 s · 94.5 s · 8,832 MB (8,760 private) | out of memory after 14 s, at 13,335 MB | 11.46 s · 30.6 s · 982 MB (953 private) |
| 10M rows, Parquet, 1,609 MB | **1.55 s** · 5.2 s · 2,738 MB (1,340 private) | 10.27 s · 37.3 s · 7,057 MB (6,985 private) | out of memory after 11 s, at 13,336 MB | not run |

Each cell: wall time, best of 3 · CPU time (user and system) · peak memory (RSS, and in brackets the private part of it), of the best run; fastest in bold. DuckDB and Polars run from Python, and their time starts at their first read, after the interpreter's start and the import; qsv is a binary and is timed as a whole process, like compara, runs on CSV only, and is given the files with the ignored column and the repeated keys already taken out (the page says how). A tool the runner's kernel stopped for lack of memory says so.

## Every run

The runs go round the tools in turn (3 rounds), so a slow minute on the runner falls on all of them.

| input | tool | every run: wall · CPU · peak memory |
|---|---|---|
| 1M rows, CSV, 368 MB | compara | 0.14 s · 0.4 s · 411 MB (56 private); 0.13 s · 0.4 s · 411 MB (56 private); 0.13 s · 0.4 s · 411 MB (56 private) |
| 1M rows, CSV, 368 MB | DuckDB | 3.26 s · 9.2 s · 1,164 MB (1,103 private); 3.29 s · 9.4 s · 1,247 MB (1,186 private); 3.29 s · 9.3 s · 1,214 MB (1,153 private) |
| 1M rows, CSV, 368 MB | Polars | 3.25 s · 12.1 s · 2,182 MB (2,121 private); 3.22 s · 12.1 s · 2,157 MB (2,093 private); 3.22 s · 12.1 s · 2,208 MB (2,145 private) |
| 1M rows, CSV, 368 MB | qsv | 1.15 s · 3.0 s · 229 MB (200 private); 1.01 s · 2.6 s · 186 MB (155 private); 0.96 s · 2.6 s · 132 MB (102 private) |
| 1M rows, Parquet, 161 MB | compara | 0.18 s · 0.6 s · 320 MB (181 private); 0.18 s · 0.5 s · 320 MB (180 private); 0.18 s · 0.6 s · 320 MB (181 private) |
| 1M rows, Parquet, 161 MB | DuckDB | 1.67 s · 2.8 s · 1,231 MB (1,171 private); 1.68 s · 2.8 s · 1,260 MB (1,200 private); 1.73 s · 2.9 s · 1,383 MB (1,323 private) |
| 1M rows, Parquet, 161 MB | Polars | 3.00 s · 10.2 s · 2,234 MB (2,170 private); 3.12 s · 10.4 s · 2,279 MB (2,215 private); 3.11 s · 10.4 s · 2,306 MB (2,241 private) |
| 10M rows, CSV, 3,679 MB | compara | 1.15 s · 3.9 s · 4,030 MB (511 private); 9.38 s · 4.9 s · 4,030 MB (511 private); 4.22 s · 4.4 s · 4,031 MB (512 private) |
| 10M rows, CSV, 3,679 MB | DuckDB | 25.91 s · 94.4 s · 8,813 MB (8,750 private); 25.67 s · 94.5 s · 8,832 MB (8,760 private); 25.96 s · 94.7 s · 8,816 MB (8,746 private) |
| 10M rows, CSV, 3,679 MB | Polars | out of memory after 14 s, at 13,335 MB |
| 10M rows, CSV, 3,679 MB | qsv | 11.46 s · 30.6 s · 982 MB (953 private); 11.78 s · 31.0 s · 1,341 MB (1,311 private); 13.18 s · 32.5 s · 2,052 MB (2,023 private) |
| 10M rows, Parquet, 1,609 MB | compara | 1.55 s · 5.2 s · 2,738 MB (1,340 private); 3.14 s · 5.0 s · 2,830 MB (1,324 private); 1.64 s · 5.1 s · 2,724 MB (1,337 private) |
| 10M rows, Parquet, 1,609 MB | DuckDB | 10.30 s · 37.3 s · 6,998 MB (6,934 private); 10.45 s · 37.1 s · 7,112 MB (7,048 private); 10.27 s · 37.3 s · 7,057 MB (6,985 private) |
| 10M rows, Parquet, 1,609 MB | Polars | out of memory after 11 s, at 13,336 MB |

## Versions

compara built from the run's commit (compara 0.7.0); DuckDB 2.0.0 and Polars 2.1.0 installed from PyPI at their latest in the run, on Python 3.13.18. qsv 29.0.0 is its release binary, as downloaded in the run.

## Method

Each pair is written by `compara-gen-data` (seed 7) in each format, and every tool reads the same files. Each tool runs in a process of its own; CPU time and peak RSS are that process's (`wait4`). Peak RSS counts the pages of files a tool maps, which the kernel can drop and read again under memory pressure: compara maps both inputs, so most of its RSS is the files themselves. The private part is the tool's own memory (heap, stacks; `RssAnon` of its process tree, sampled every 5 ms, so a lower bound), what a container limit has to leave it. Each tool computes what compara does: values compared as text in CSV and by value in Parquet, an empty field equal only to an empty field, and a key that appears twice in a file compared by its first row. DuckDB reads Parquet where it is (`read_parquet`, its row numbers from the file) and loads each CSV into a table once, for a row order to take the first row by; Polars runs its lazy API, which its streaming engine did not beat. qsv diff reads CSV only, has no option to leave a column out and says its keys must be unique (these tables repeat a few): the ignored column is cut from both files with `qsv select` and each key's first row is kept with `qsv extdedup`, both before the timing and not charged to qsv, and the timed process is `qsv diff -k account_id,txn_id LEFT RIGHT` on every core, its output written to a file; the counts are read back from that output, and unchanged is the left file's rows (`qsv count`) less the changed and removed. The run fails unless every tool that finishes gives the same counts: 1M rows, CSV, 368 MB: 938,951 unchanged, 60,049 changed, 1,000 added, 1,000 removed; 1M rows, Parquet, 161 MB: 938,951 unchanged, 60,049 changed, 1,000 added, 1,000 removed; 10M rows, CSV, 3,679 MB: 9,390,680 unchanged, 599,320 changed, 10,000 added, 10,000 removed; 10M rows, Parquet, 1,609 MB: 9,390,680 unchanged, 599,320 changed, 10,000 added, 10,000 removed.

To run it yourself, from a checkout with compara built and `pip install duckdb polars`:

```sh
python3 scripts/bench_contenders.py --compara target/release/compara \
  --gen target/release/compara-gen-data --rows 1m,10m --json results.json
python3 scripts/gen_bench_docs.py results.json --page bench.md
```
