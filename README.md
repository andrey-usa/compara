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
- Ten million rows × 20 columns in under a second on four cores, CSV or Parquet
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
$ echo $?
1
$ compara compare examples/orders_2026-08.csv examples/orders_2026-09.csv -k order_id,line_no -i updated_at -o report.html
A 800 rows | B 799 rows | matched 787 (changed 102) | added 10 | removed 12 | duplicate keys A 1, B 1 | engine turbo, 0.001s
Report: report.html (20 KB)
```

Two monthly order exports from the repository's `examples/`, keyed on
`(order_id, line_no)`, with `updated_at` left out. Exit code 1 says they
differ; the report holds the counts, per-column statistics and the changed,
added, removed and duplicate rows. More: [Everyday comparisons](https://andrey-usa.github.io/compara/start/tutorial.html),
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

Two tables of 20 columns compared on a composite key (`-k account_id,txn_id -i updated_at`), every tool's unchanged, changed, added and removed counts checked identical, on GitHub's `ubuntu-latest` (AMD EPYC 9V45 96-Core Processor, 4 cores, 15 GB), [bench-contenders run 42782167828](https://github.com/andrey-usa/compara/actions/runs/42782167828), 2026-11-10:

| input, both files | compara 0.6.0 | DuckDB 2.0.0 | Polars 2.1.0 |
|---|---|---|---|
| 1M rows, CSV, 368 MB | **0.08 s** · 0.2 s · 411 MB | 1.90 s · 5.3 s · 1,213 MB | 2.10 s · 8.3 s · 2,262 MB |
| 1M rows, Parquet, 161 MB | **0.11 s** · 0.3 s · 317 MB | 1.10 s · 1.9 s · 1,358 MB | 2.11 s · 7.4 s · 2,430 MB |
| 10M rows, CSV, 3,679 MB | **0.74 s** · 2.2 s · 4,031 MB | 14.59 s · 53.7 s · 8,797 MB | out of memory after 12 s, at 13,345 MB |
| 10M rows, Parquet, 1,609 MB | **0.85 s** · 2.7 s · 2,739 MB | 6.40 s · 23.9 s · 7,001 MB | out of memory after 9 s, at 13,345 MB |

Each cell: wall time, best of 3 · CPU time (user and system) · peak memory, of the best run; fastest in bold. DuckDB and Polars run from Python, and their time starts at their first read, after the interpreter's start and the import. A tool the runner's kernel stopped for lack of memory says so.

No other tool did better than compara by more than 2% on any measure in this run.

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
