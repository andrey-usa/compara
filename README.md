# compara

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/andrey-usa/compara/actions/workflows/ci.yml/badge.svg)](https://github.com/andrey-usa/compara/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/compara.svg)](https://crates.io/crates/compara)
[![PyPI](https://img.shields.io/pypi/v/compara.svg)](https://pypi.org/project/compara/)
[![npm](https://img.shields.io/npm/v/compara.svg)](https://www.npmjs.com/package/compara)

Extremely fast table comparison: two tables on a composite key, with counts,
per-column statistics and a self-contained HTML report. Key columns, compared
columns and normalisation rules are parameters, so one tool serves every
recurring comparison: a nightly export against yesterday's, a CSV against the
Parquet a warehouse emits, a migration's source against its target.

> Faster software for a better planet — less energy, less compute, more time for life.

- Reads **CSV, newline-delimited JSON and Parquet**, and the two sides need not
  be in the same format.
- Ten million rows × 20 columns in about a second on four cores
  ([numbers below](#speed)).
- One static binary; the report is a single HTML file with nothing to host.
- Exit codes made for pipelines: `0` identical, `1` differences, `2` error,
  `3` duplicate keys (with `--fail-on-dups`).

## Install

Prebuilt binaries for Linux (x86_64, aarch64), macOS (arm64, x86_64) and
Windows (x64) are on the [releases page](https://github.com/andrey-usa/compara/releases),
and the same binaries through package managers:

| | |
|---|---|
| Cargo | `cargo install --locked compara` |
| npm | `npm install -g compara` (or `npx compara …`) |
| pip / uv | `pip install compara` · `uv tool install compara` |
| Homebrew | `brew install andrey-usa/tap/compara` |
| Scoop | `scoop bucket add andrey-usa https://github.com/andrey-usa/scoop-bucket` then `scoop install compara` |

## Use

```sh
compara compare a.csv b.csv -k account_id,txn_id -i updated_at
compara compare a.csv b.csv -k account_id,txn_id --trim --tolerance 0.005
compara compare export.csv warehouse.parquet -k id --summary
compara compare a.csv b.csv --profile orders --json summary.json --export-dir out/
compara columns data.parquet          # column names (reads only the footer)
compara head data.ndjson -n 5         # first rows, aligned
```

`compare` writes `<a>__vs__<b>.html` (or `--out PATH`): counts, per-column
statistics, and the changed, added, removed and duplicate rows, each section
capped at `--max-rows` (counts are always exact). [`examples/report.html`](examples/report.html)
is one, made from the two files next to it:

```sh
compara compare examples/orders_2026-08.csv examples/orders_2026-09.csv -k order_id,line_no -i updated_at
```

| Option | Effect |
|---|---|
| `-k`, `--key` | composite key, required (or from a profile) |
| `-c`, `--compare` | columns to diff; default every column present in both files except the key |
| `-i`, `--ignore` | columns to skip (timestamps, run ids) |
| `--trim`, `--ignore-case`, `--empty-is-null` | normalisation before comparing (applies to key and values) |
| `--tolerance` | absolute numeric tolerance where both sides parse as numbers |
| `--delimiter`, `--encoding` | override auto-detection (CSV only) |
| `--threads` | how wide the default engine runs; the default is every core |
| `--memory` | memory to plan for (e.g. `8G`); decides when large files are joined in one pass instead of two |
| `--max-rows` | rows embedded per report section (default 50 000) |
| `--export-dir` | full, uncapped changed/added/removed CSVs |
| `--json` | also write a JSON summary (counts and column stats; schema in `docs/contract.schema.json`) |
| `--summary` | print the counts and write nothing |
| `--engine` | `auto` (default), `turbo`, `sortmerge` or `native` |
| `--no-compress` | plain JSON payload in the report, for pre-2023 browsers |
| `--fail-on-dups` | exit 3 when either file has duplicate keys |

Duplicate keys are counted and listed per file; the first occurrence of each
key takes part in the join.

**Profiles.** A `compara.toml` in the working directory (or
`~/.config/compara/compara.toml`, or `--config PATH`) names recurring
comparisons: key, compared and ignored columns, normalisation, engine. See
[`compara.example.toml`](compara.example.toml). The command line overrides the
profile.

## Speed

Ten million rows × 20 columns, keyed on `(account_id, txn_id)`, `--ignore
updated_at`, one GitHub Actions runner (AMD EPYC 7763, 4 vCPU / 16 GB), best of
five. Each cell is wall time · CPU time · memory above the mapped input.

| Format | Input a side | compara |
|---|---:|---|
| CSV | 3,509 MB | 1.20 s · 4.0 s · 591 MB |
| ndjson | 8,487 MB | 2.79 s · 10.4 s · 592 MB |
| Parquet | 2,074 MB | 1.41 s · 4.7 s · 1,342 MB |

Contender benchmarks rerun weekly
([`bench-contenders.yml`](.github/workflows/bench-contenders.yml)) against the
latest DuckDB and pandas.

## Engines

Three backends, one result contract: every engine returns identical counts and
column statistics for the same input, and the test suite asserts it cell for
cell. `--engine auto` takes the first one that can load the input, in this
order.

| Engine | Implementation | Memory model | Use it for |
|---|---|---|---|
| `turbo` | mapped file, byte-level index; CSV, JSON and Parquet | off-heap bytes, index in memory | the default, and the fastest |
| `sortmerge` | external sort, merge join | bounded memory, spills to disk | files past what memory can index |
| `native` | over the `csv` crate | in-memory, row-oriented | the dependency-light baseline |

All three read values as text, with no type inference, so `1.0` and `1` stay
different unless a tolerance is set, and all three treat an empty field as
absent whether or not it is quoted.

### Input formats

`turbo` decides what a file is by its content rather than its name: a Parquet
magic number, a leading `{`, or a CSV header.

| Format | How it is read |
|---|---|
| CSV | mapped, scanned eight bytes at a time; a field is an offset and a length |
| newline-delimited JSON | the same, addressed by key rather than by column number; `\uXXXX` is decoded, so an escaped and a literal character compare equal |
| Parquet | pages decoded into an arena; dictionary-encoded columns point at the dictionary entry, so a repeated value costs eight bytes a row |

The Parquet reader is written here rather than taken from a crate, to keep that
representation instead of converting to Arrow arrays. It reads PLAIN,
dictionary (`PLAIN_DICTIONARY`, `RLE_DICTIONARY`), RLE booleans and the
version-2 delta encodings; uncompressed, snappy, gzip, zstd and LZ4-raw pages;
data pages v1 and v2; definition levels for optional columns. It refuses, by
name, nested or repeated schemas, `BYTE_STREAM_SPLIT`, LZO and Brotli.

A typed Parquet value is rendered to text by a fixed rule: byte arrays as their
bytes, booleans `true`/`false`, integers and decimals in full, floats in the
shortest round-trip form, dates `YYYY-MM-DD`, timestamps ISO 8601.
`tests/fixtures/formats/typed.csv` is that rule written down.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
