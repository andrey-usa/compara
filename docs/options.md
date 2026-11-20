# compare options

| Option | Effect |
|---|---|
| `-k`, `--key` | composite key, required (or from a profile) |
| `-c`, `--compare` | columns to diff; default every column present in both files except the key |
| `-i`, `--ignore` | columns to skip (timestamps, run ids) |
| `--trim`, `--ignore-case` | normalisation before comparing (applies to key and values); a value `--trim` leaves empty is absent, as an empty cell is, so `"  "` against an empty or missing value is no change |
| `--empty-is-null` | kept for old scripts and profiles: an empty value and a missing one are always equal, in every format |
| `--tolerance` | absolute numeric tolerance where both sides parse as numbers |
| `--delimiter` | override delimiter detection (CSV only): one character, or `'\t'` (or `tab`) for a tab; one that isn't in a file's header is noted |
| `--encoding` | UTF-8 (the default) is read as text; Latin-1 or a Windows code page compares as bytes, with the right counts; UTF-16 and UTF-32 are refused with how to convert |
| `--threads` | how wide the engines run; the default is every core |
| `--memory` | memory to plan for (e.g. `8G`); decides when large text files are joined in one pass instead of two, and how many columns of two Parquet files are read at once (a Parquet pair that needs more than a stated budget is refused) |
| `--max-rows` | rows embedded per report section (default 50 000) |
| `-o`, `--out` | write the HTML report here, or to standard output with `-o -`; there is no report without it |
| `--export-dir` | full, uncapped changed/added/removed CSVs |
| `--json` | write a JSON summary (counts and column stats; schema in `docs/contract.schema.json`) |
| `--summary` | print the counts and refuse any flag that writes a file (the default without `-o`, `--json` or `--export-dir`) |
| `--engine` | `auto` (default), `turbo`, `sortmerge` or `native`; `sortmerge` and `native` read CSV only |
| `--no-compress` | plain JSON payload in the report, for pre-2023 browsers (with `-o`) |
| `--fail-on-dups` | exit 3 when either file has duplicate keys |
| `--exit-zero` | exit 0 when the tables differ: for a scheduled job whose output is the report (a Kubernetes Job, an Airflow task) |

An option that names columns (`-k`, `-c`, `-i`) may be given more than once,
and the names add up: `-k order_id -k line_no` is `-k order_id,line_no`. Any
other option given twice with two values is refused.

A column that only one of the two files has is not compared, and the exit
code does not answer for it: a note after the counts names it (`note: column
only in B, not compared: carrier`), unless `-i` names the column or `-c`
names the ones to compare, and the JSON summary lists such columns as
`meta.only_in_a` and `meta.only_in_b`.

Duplicate keys are counted and listed per file; the first occurrence of each
key takes part in the join.

**Profiles.** A `compara.toml` in the working directory (or
`~/.config/compara/compara.toml`, or `--config PATH`) names recurring
comparisons: key, compared and ignored columns, normalisation, engine. See
[`compara.example.toml`](../compara.example.toml). The command line overrides the
profile. A list may run over several lines. A key a profile does not have,
or a value of the wrong kind (`trim = yes`), is refused with its line, and
a profile name that is not in the file is answered with the names that are.
