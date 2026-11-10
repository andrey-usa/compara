# compare options

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
[`compara.example.toml`](../compara.example.toml). The command line overrides the
profile.
