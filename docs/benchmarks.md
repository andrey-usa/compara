# Benchmarks

Numbers: [bench-contenders run 44629611468](https://github.com/andrey-usa/compara/actions/runs/44629611468), 2026-11-19.

Two tables of 20 columns compared on a composite key (`-k account_id,txn_id -i updated_at`), every tool's unchanged, changed, added and removed counts checked identical. Run on GitHub's `ubuntu-24.04, image 20261111.370.1`: AMD EPYC 9V45 96-Core Processor, 4 cores, 15 GB, Linux-7.0.0-1017-azure-x86_64-with-glibc2.39.

| input, both files | compara 0.7.0 | DuckDB 2.0.0 | Polars 2.1.0 | qsv 29.0.0 |
|---|---|---|---|---|
| 1M rows, CSV, 368 MB | **0.09 s** · 0.2 s · 411 MB (56 private) | 2.10 s · 5.7 s · 1,167 MB (1,107 private) | 2.31 s · 8.9 s · 2,294 MB (2,232 private) | 0.60 s · 1.5 s · 173 MB (143 private) |
| 1M rows, Parquet, 161 MB | **0.12 s** · 0.3 s · 319 MB (181 private) | 1.19 s · 2.1 s · 1,198 MB (1,139 private) | 2.34 s · 8.2 s · 2,457 MB (2,393 private) | not run |
| 10M rows, CSV, 3,679 MB | **0.70 s** · 2.2 s · 4,031 MB (511 private) | 16.46 s · 59.2 s · 8,832 MB (8,771 private) | out of memory after 13 s, at 13,343 MB | 8.68 s · 19.9 s · 1,430 MB (1,401 private) |
| 10M rows, Parquet, 1,609 MB | **0.87 s** · 2.7 s · 2,731 MB (1,337 private) | 7.27 s · 25.8 s · 6,950 MB (6,891 private) | out of memory after 11 s, at 13,345 MB | not run |

Each cell: wall time, best of 3 · CPU time (user and system) · peak memory (RSS, and in brackets the private part of it), of the best run; fastest in bold. DuckDB and Polars run from Python, and their time starts at their first read, after the interpreter's start and the import; qsv is a binary and is timed as a whole process, like compara, runs on CSV only, and is given the files with the ignored column and the repeated keys already taken out (the page says how). A tool the runner's kernel stopped for lack of memory says so.

## Every run

The runs go round the tools in turn (3 rounds), so a slow minute on the runner falls on all of them.

| input | tool | every run: wall · CPU · peak memory |
|---|---|---|
| 1M rows, CSV, 368 MB | compara | 0.09 s · 0.2 s · 411 MB (56 private); 0.14 s · 0.2 s · 411 MB (56 private); 0.09 s · 0.2 s · 411 MB (56 private) |
| 1M rows, CSV, 368 MB | DuckDB | 2.14 s · 5.8 s · 1,115 MB (1,054 private); 2.10 s · 5.7 s · 1,167 MB (1,107 private); 2.15 s · 5.8 s · 1,138 MB (1,078 private) |
| 1M rows, CSV, 368 MB | Polars | 2.42 s · 9.1 s · 2,249 MB (2,186 private); 2.43 s · 9.1 s · 2,391 MB (2,329 private); 2.31 s · 8.9 s · 2,294 MB (2,232 private) |
| 1M rows, CSV, 368 MB | qsv | 0.64 s · 1.6 s · 201 MB (170 private); 0.63 s · 1.5 s · 204 MB (180 private); 0.60 s · 1.5 s · 173 MB (143 private) |
| 1M rows, Parquet, 161 MB | compara | 0.13 s · 0.3 s · 322 MB (181 private); 0.12 s · 0.3 s · 319 MB (181 private); 0.12 s · 0.3 s · 322 MB (181 private) |
| 1M rows, Parquet, 161 MB | DuckDB | 1.23 s · 2.1 s · 1,359 MB (1,300 private); 1.23 s · 2.1 s · 1,382 MB (1,322 private); 1.19 s · 2.1 s · 1,198 MB (1,139 private) |
| 1M rows, Parquet, 161 MB | Polars | 2.34 s · 8.2 s · 2,457 MB (2,393 private); 2.46 s · 8.3 s · 2,527 MB (2,460 private); 2.51 s · 8.6 s · 2,527 MB (2,463 private) |
| 10M rows, CSV, 3,679 MB | compara | 0.70 s · 2.2 s · 4,031 MB (511 private); 9.33 s · 3.5 s · 4,030 MB (511 private); 2.90 s · 2.7 s · 4,031 MB (511 private) |
| 10M rows, CSV, 3,679 MB | DuckDB | 16.86 s · 59.6 s · 8,762 MB (8,701 private); 17.01 s · 59.7 s · 8,786 MB (8,724 private); 16.46 s · 59.2 s · 8,832 MB (8,771 private) |
| 10M rows, CSV, 3,679 MB | Polars | out of memory after 13 s, at 13,343 MB |
| 10M rows, CSV, 3,679 MB | qsv | 8.80 s · 19.2 s · 1,357 MB (1,328 private); 9.46 s · 21.2 s · 2,075 MB (2,046 private); 8.68 s · 19.9 s · 1,430 MB (1,401 private) |
| 10M rows, Parquet, 1,609 MB | compara | 0.87 s · 2.7 s · 2,731 MB (1,337 private); 2.04 s · 2.6 s · 2,876 MB (1,330 private); 0.92 s · 2.9 s · 2,709 MB (1,338 private) |
| 10M rows, Parquet, 1,609 MB | DuckDB | 7.27 s · 25.8 s · 6,950 MB (6,891 private); 7.38 s · 25.7 s · 7,119 MB (7,059 private); 7.53 s · 26.5 s · 7,156 MB (7,093 private) |
| 10M rows, Parquet, 1,609 MB | Polars | out of memory after 11 s, at 13,345 MB |

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
