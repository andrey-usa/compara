# compara

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/andrey-usa/compara/actions/workflows/ci.yml/badge.svg)](https://github.com/andrey-usa/compara/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/compara.svg)](https://crates.io/crates/compara)
[![PyPI](https://img.shields.io/pypi/v/compara.svg)](https://pypi.org/project/compara/)

Extremely fast table comparison: two tables on a composite key, with counts,
per-column statistics and a self-contained HTML report. Key columns, compared
columns and normalisation rules are parameters, so one tool serves every
recurring comparison: a nightly export against yesterday's, a CSV against the
Parquet a warehouse emits, a migration's source against its target.

> Faster software for a better planet — less energy, less compute, more time for life.

- Reads **CSV, TSV, JSON (newline-delimited or one array of objects) and
  Parquet**, and the two sides need not be in the same format.
- Ten million rows × 20 columns in about a second on four cores
  ([numbers below](#speed)).
- Reads and writes **object storage and the web** directly -- `s3://`,
  `gs://`, `az://`, `https://` -- with each cloud's own credentials, and
  fetches only the Parquet columns it compares
  ([cloud and containers](#cloud-and-containers)).
- One static binary, and a distroless container image; the report is a
  single HTML file with nothing to host.
- Exit codes made for pipelines: `0` identical, `1` differences, `2` error,
  `3` duplicate keys (with `--fail-on-dups`).

## Install

Prebuilt binaries for Linux (x86_64, aarch64), macOS (arm64, x86_64) and
Windows (x64) are on the [releases page](https://github.com/andrey-usa/compara/releases),
and the same binaries through package managers:

| Channel | Install |
|---|---|
| Cargo | `cargo install --locked compara` |
| pip / uv | `pip install compara` · `uv tool install compara` |
| Homebrew | `brew install andrey-usa/tap/compara` |
| Scoop | `scoop bucket add andrey-usa https://github.com/andrey-usa/scoop-bucket` then `scoop install compara` |
| Docker | `docker run --rm -v "$PWD:/data" ghcr.io/andrey-usa/compara compare a.csv b.csv -k id` |

## Use

```sh
compara compare a.csv b.csv -k account_id,txn_id -i updated_at
compara compare a.csv b.csv -k account_id,txn_id -i updated_at -o report.html
compara compare a.csv b.csv -k account_id,txn_id --trim --tolerance 0.005
compara compare export.csv warehouse.parquet -k id
compara compare a.csv b.csv --profile orders --json summary.json --export-dir out/
compara columns data.parquet          # column names (reads only the footer)
compara head data.ndjson -n 5         # first rows, aligned
```

`compare` prints the counts and writes nothing else unless asked. `-o PATH`
writes the HTML report: counts, per-column statistics, and the changed,
added, removed and duplicate rows, each section capped at `--max-rows`
(counts are always exact). [`examples/report.html`](examples/report.html) is
one, made from the two files next to it:

```sh
compara compare examples/orders_2026-08.csv examples/orders_2026-09.csv -k order_id,line_no -i updated_at -o examples/report.html
```

| Option | Effect |
|---|---|
| `-k`, `--key` | composite key, required (or from a profile) |
| `-c`, `--compare` | columns to diff; default every column present in both files except the key |
| `-i`, `--ignore` | columns to skip (timestamps, run ids) |
| `--trim`, `--ignore-case`, `--empty-is-null` | normalisation before comparing (applies to key and values) |
| `--tolerance` | absolute numeric tolerance where both sides parse as numbers |
| `--delimiter` | override delimiter detection (CSV only); one that isn't in a file's header is noted |
| `--encoding` | UTF-8 (the default) is read as text; Latin-1 or a Windows code page compares as bytes, with the right counts; UTF-16 and UTF-32 are refused with how to convert |
| `--threads` | how wide the engines run; the default is every core |
| `--memory` | memory to plan for (e.g. `8G`); decides when large text files are joined in one pass instead of two, and how many columns of two Parquet files are read at once (a Parquet pair that needs more than a stated budget is refused) |
| `--max-rows` | rows embedded per report section (default 50 000) |
| `-o`, `--out` | write the HTML report here; there is no report without it |
| `--export-dir` | full, uncapped changed/added/removed CSVs |
| `--json` | write a JSON summary (counts and column stats; schema in `docs/contract.schema.json`) |
| `--summary` | print the counts and refuse any flag that writes a file (the default without `-o`, `--json` or `--export-dir`) |
| `--engine` | `auto` (default), `turbo`, `sortmerge` or `native`; `sortmerge` and `native` read CSV only |
| `--no-compress` | plain JSON payload in the report, for pre-2023 browsers (with `-o`) |
| `--fail-on-dups` | exit 3 when either file has duplicate keys |
| `--exit-zero` | exit 0 when the tables differ: for a scheduled job whose output is the report (a Kubernetes Job, an Airflow task) |

Duplicate keys are counted and listed per file; the first occurrence of each
key takes part in the join.

**Profiles.** A `compara.toml` in the working directory (or
`~/.config/compara/compara.toml`, or `--config PATH`) names recurring
comparisons: key, compared and ignored columns, normalisation, engine. See
[`compara.example.toml`](compara.example.toml). The command line overrides the
profile.

More: [cloud and containers](cloud.md), [speed and engines](speed.md),
the [CLI reference](cli.md) and [profiles and examples](examples.md).
