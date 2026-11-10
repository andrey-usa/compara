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
- Ten million rows × 20 columns in about a second on four cores
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

Ten million rows × 20 columns, keyed on `(account_id, txn_id)`, `--ignore
updated_at`, four threads on a 4 vCPU / 16 GB GitHub Actions runner, best of
five. Each format ran on a runner of its own, named below (CI runs
[39728430053](https://github.com/andrey-usa/compara-polyglot-experiments/actions/runs/39728430053) for CSV and Parquet and
[39729034092](https://github.com/andrey-usa/compara-polyglot-experiments/actions/runs/39729034092) for ndjson, in
[compara-polyglot-experiments](https://github.com/andrey-usa/compara-polyglot-experiments)).
Each cell is wall time · CPU time · memory above the mapped input.

| Format | Input, both files | Runner CPU | compara |
|---|---:|---|---|
| CSV | 3,509 MB | AMD EPYC 7763 | 1.08 s · 3.5 s · 591 MB |
| ndjson | 8,488 MB | AMD EPYC 9V45 | 1.26 s · 3.9 s · 592 MB |
| Parquet | 1,535 MB | Intel Xeon 6973P-C | 1.17 s · 3.8 s · 1,174 MB |

Two Parquet files against DuckDB and Polars, on the same kind of runner
([CI run 39468615515](https://github.com/andrey-usa/compara-polyglot-experiments/actions/runs/39468615515): a 5M-row pair written by pyarrow, counts only, median
of three): compara 1.26 s at one thread and 0.56 s at four, DuckDB 6.73 s and
3.13 s, Polars 4.98 s and 2.73 s. With the changed, added and removed rows
written out, compara 0.72 s at four threads, DuckDB 4.00 s, Polars 2.74 s.

Contender benchmarks rerun weekly
([`bench-contenders.yml`](https://github.com/andrey-usa/compara/blob/main/.github/workflows/bench-contenders.yml)) against the
latest DuckDB.

Object storage, containers and every table: [Benchmarks](https://andrey-usa.github.io/compara/benchmarks/results.html).

## Documentation

<!-- Written by docs-site/build.py from docs-site/site.toml: edit that, then run it with --write. -->

The docs site, https://andrey-usa.github.io/compara/, has the same parts for every tool:

- **Start:** [Install](https://andrey-usa.github.io/compara/start/install.html) · [Quick start](https://andrey-usa.github.io/compara/start/quick-start.html) · [Everyday comparisons](https://andrey-usa.github.io/compara/start/tutorial.html)
- **Guides:** [Compare tables in object storage and on the web](https://andrey-usa.github.io/compara/guides/cloud.html) · [Run compara in a container, in CI or in Kubernetes](https://andrey-usa.github.io/compara/guides/containers.html) · [Name a recurring comparison](https://andrey-usa.github.io/compara/guides/profiles.html)
- **Concepts:** [Engines and formats](https://andrey-usa.github.io/compara/concepts/engines.html)
- **Reference:** [compare options](https://andrey-usa.github.io/compara/reference/options.html) · [The JSON summary](https://andrey-usa.github.io/compara/reference/json-summary.html) · [compara --help](https://andrey-usa.github.io/compara/reference/help.html)
- **Agents:** [For AI agents](https://andrey-usa.github.io/compara/agents/overview.html) · [Skill](https://andrey-usa.github.io/compara/agents/skill.html)
- **Benchmarks:** [Results](https://andrey-usa.github.io/compara/benchmarks/results.html)
- **Project:** [Changelog](https://andrey-usa.github.io/compara/project/changelog.html) · [Contributing](https://andrey-usa.github.io/compara/project/contributing.html) · [Security](https://andrey-usa.github.io/compara/project/security.html)

For agents: [llms.txt](https://andrey-usa.github.io/compara/llms.txt) lists every page as markdown, and [llms-full.txt](https://andrey-usa.github.io/compara/llms-full.txt) is the whole site in one file.

## Contributing

Building, testing and the performance gate: [CONTRIBUTING.md](https://github.com/andrey-usa/compara/blob/main/CONTRIBUTING.md).

## License

MIT: [LICENSE](https://github.com/andrey-usa/compara/blob/main/LICENSE).
