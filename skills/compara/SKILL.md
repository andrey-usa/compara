---
name: compara
description: Compare two tables (CSV, TSV, JSON, Parquet; local, s3://, gs://, az:// or https://) on a composite key with compara, and get the counts of unchanged, changed, added and removed rows, which columns changed, and the rows themselves. Use to check an export against the one before it, a migration's target against its source, or a CSV against the Parquet a warehouse wrote, instead of writing a join in SQL or pandas.
---

# compara

One native binary. It reads both tables, joins them on the key you name and
says what differs, in about a second for ten million rows.

## The loop

1. Find the key: `compara columns a.csv` prints the column names (on Parquet
   it reads only the footer); `compara head a.csv -n 5` shows the first rows.
2. Count: `compara compare a.csv b.csv -k id` prints one line of counts and
   writes nothing. Exit code `0`: identical; `1`: they differ; `2`: an
   error (its message says what to change); `3`: duplicate keys, with
   `--fail-on-dups`.
3. See which columns changed: `--json -` prints the summary as JSON on
   standard output (counts on standard error). `columns[].changed` counts the
   rows each column changed in; `counts` has `unchanged`, `changed`,
   `added`, `removed` and the duplicate keys of each side.
4. See the rows: `--export-dir out/` writes the changed, added and removed
   rows as CSVs, uncapped; `-o report.html` writes a report a person can open.

```sh
compara compare a.csv b.csv -k account_id,txn_id -i updated_at --json -
```

## Options that change the answer

| option | use it when |
|---|---|
| `-k a,b` | the key is several columns: always name all of them |
| `-i col,col` | a column always differs (load time, run id) |
| `-c col,col` | only some columns matter |
| `--tolerance 0.005` | numbers may differ by rounding; without it `1.0` and `1` differ in CSV |
| `--trim`, `--ignore-case`, `--empty-is-null` | whitespace, case, or empty against missing should not count |
| `--delimiter ';'` | the separator is not found from the header |
| `--encoding latin1` | the file is not UTF-8 |

Values compare as text in CSV and JSON; a Parquet column that holds numbers
or dates compares by value. A key that appears twice in a file is counted
under the duplicate keys, and its first row takes part in the join.

## Safe by default

`compare` writes only what `-o`, `--json` or `--export-dir` name;
`--summary` refuses those, for a run that must only read. `--exit-zero`
exits `0` even when the tables differ, for a job whose output is the report.

## Files that are not local

Any file argument can be a URL: `s3://bucket/key.parquet`, `gs://`, `az://`,
`https://`. Credentials come from each cloud's own environment variables or
profiles; with none, a public bucket is read unsigned. A directory, a quoted
glob or a prefix is its files read as one table; `s3://lake/orders@v41` is
version 41 of a Delta table.

## When something fails

Read the error: it names the file, the column or the option, and what to do
(a key column missing from one side lists the columns it has). `compara env`
prints the CPUs, the memory limit and the temporary directory it sees.
