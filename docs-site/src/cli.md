# CLI reference (`compara --help`)

Verbatim output of `compara --help` from a release build of this repository (main, post-0.6.0 — wording is slightly expanded vs the released 0.6.0 binary):

```text
usage: compara <command> [options]

commands:
  compare A B     Compare two tables on a composite key and print the counts;
                  with -o, also write an HTML report
  columns FILE    Print one file's column names, one per line
  head FILE       Print the first rows of one file (default 10)
  version         Print the version (also --version)
  env             Print what this process sees: CPUs, memory and its limits,
                  the temporary directory, and whether URLs can be read

A, B and FILE are paths, `-` for standard input, or with cloud support s3://,
gs://, az:// and https:// URLs. Gzip and zstd files are read as what they hold.
A directory, a quoted glob ('out/date=*/*.parquet', `**` for any depth) or an
object storage prefix (s3://bucket/orders/) is its files read as one table, in
path order; names starting with `.` or `_` in a directory are passed over, and
Hive partition directories (date=2026-10-01/) are columns. A Delta table is read
at its latest version, or at TABLE@vN (s3://lake/orders@v41).

columns and head read CSV, newline-delimited JSON and Parquet, and neither reads
more of the file than it has to: `columns` on Parquet reads the footer, and
`head` stops at the first page. Both take --delimiter.

head options:
  -n, --rows N            Rows to show (default 10)
      --csv               Print as CSV instead of an aligned table

compare options:
  -k, --key COLS          Comma-separated key column(s) (or from --profile)
  -c, --compare COLS      Columns to compare (default: every common non-key
                          column)
  -i, --ignore COLS       Columns to skip
  -p, --profile NAME      Profile name from compara.toml
      --config PATH       Path to compara.toml
      --trim              Strip whitespace before comparing
      --ignore-case       Compare text without regard to case
      --empty-is-null     Treat an empty string and null as equal
      --tolerance N       Absolute numeric tolerance
      --max-rows N        Rows embedded per report section (default 50000)
      --delimiter D       Force the delimiter (default: detected)
      --encoding ENC      UTF-8 (the default) is read as text; Latin-1 or a
                          Windows code page compares as bytes (right counts);
                          UTF-16 is refused
      --engine E          auto (default) picks one: turbo, in memory and the
                          fastest; sortmerge, which sorts on disk for files
                          larger than memory; native, the simple baseline.
                          sortmerge and native read CSV only
      --threads N         Threads to use (default: every core, or the
                          container's CPU quota)
      --memory SIZE       Memory to plan for, e.g. 8G (default: what the
                          machine or container has free); decides when large
                          text files are joined in one pass instead of two,
                          and how many columns of two Parquet files are read
                          at once; Parquet that needs more than SIZE one
                          column at a time is refused
      --export-dir DIR    Write full changed, added and removed CSVs here (a
                          directory, or a URL prefix such as s3://bucket/run/)
  -o, --out PATH          Write the HTML report here, a path or a URL (no
                          report without it)
      --json PATH         Write a JSON summary (counts and column statistics)
                          here, a path or a URL; `-` prints it, and the counts
                          line goes to stderr
      --no-compress       Report: embed plain JSON instead of gzip (for older
                          browsers)
      --summary           Print the counts, write nothing, and refuse any flag
                          that would (the default without -o, --json or
                          --export-dir)
      --fail-on-dups      Exit 3 when either file has duplicate keys
      --exit-zero         Exit 0 when the tables differ: for a scheduled job
                          whose output is the report, where a difference is
                          not a failure (a Kubernetes Job, an Airflow task)

Exit codes: 0 identical, 1 differences found, 2 error, 3 duplicate keys (with
--fail-on-dups).
```
