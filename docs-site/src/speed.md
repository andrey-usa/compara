# Speed, engines and formats

## Speed

Ten million rows × 20 columns, keyed on `(account_id, txn_id)`, `--ignore
updated_at`, four threads on a 4 vCPU / 16 GB GitHub Actions runner, best of
five. Each format ran on a runner of its own, named below (CI runs
39728430053 and 39729034092 in
[compara-polyglot-experiments](https://github.com/andrey-usa/compara-polyglot-experiments)).
Each cell is wall time · CPU time · memory above the mapped input.

| Format | Input, both files | Runner CPU | compara |
|---|---:|---|---|
| CSV | 3,509 MB | AMD EPYC 7763 | 1.08 s · 3.5 s · 591 MB |
| ndjson | 8,488 MB | AMD EPYC 9V45 | 1.26 s · 3.9 s · 592 MB |
| Parquet | 1,535 MB | Intel Xeon 6973P-C | 1.17 s · 3.8 s · 1,174 MB |

Two Parquet files against DuckDB and Polars, on the same kind of runner
(CI run 39468615515: a 5M-row pair written by pyarrow, counts only, median
of three): compara 1.26 s at one thread and 0.56 s at four, DuckDB 6.73 s and
3.13 s, Polars 4.98 s and 2.73 s. With the changed, added and removed rows
written out, compara 0.72 s at four threads, DuckDB 4.00 s, Polars 2.74 s.

Contender benchmarks rerun weekly
([`bench-contenders.yml`](.github/workflows/bench-contenders.yml)) against the
latest DuckDB.

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
absent whether or not it is quoted. A Parquet column is the exception, on
either side: it stores typed values, so a column it holds as numbers
compares by value with the other file's text (`31.40` against the DOUBLE
31.4 is no change), and a date, time or timestamp column by instant (`T` or
a space, a zero fraction or none, `Z` or nothing).

### Input formats

`turbo` decides what a file is by its content rather than its name: a Parquet
magic number, a leading `{` or `[`, or a CSV header.

| Format | How it is read |
|---|---|
| CSV | mapped, scanned eight bytes at a time; a field is an offset and a length; a UTF-8 byte order mark is skipped; the separator (comma, semicolon, tab or pipe) is read from the header line unless `--delimiter` names it |
| TSV | the CSV reader with a tab for the separator, found in the header line like any other; the same rows as TSV and as CSV give the same answer and the same export |
| JSON | one object per line (NDJSON), or one top-level array of objects; the columns are the keys of the first object, and a value is found by its key rather than its position; `\uXXXX` is decoded, so an escaped and a literal character compare equal; `null` and a missing key are both absent |
| Parquet | pages decoded into an arena; dictionary-encoded columns point at the dictionary entry, so a repeated value costs eight bytes a row |

**Nested JSON.** A value that is an object or an array is compared as its
JSON text, as written: any difference inside it is a change to that column,
and the report and export show the text. The same object written with its
keys in another order, or with other spacing, is a change too, so JSON
compares best flat, one scalar per key. For two files written by different
tools, flatten nested values into top-level keys first (`jq`, or the writer's
own option), and each nested field becomes a column compared on its own.

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

Two Parquet files take a columnar path instead, which never builds a row: it
joins on the key columns, then compares one column at a time, string columns
by dictionary id or bytes and booleans, integers, floats, dates, times,
timestamps and decimals of up to eight bytes as one word per value. A cell is
rendered by the rule above only when the report shows it or `--export-dir`
lists it. Added and removed rows are known from the join, so their cells are
written out during that pass; which rows changed is known only after the last
column, so an export reads the compared columns once more and writes out just
those rows. It costs that second pass and the text it writes, rather than
holding every column of both files at once. A column the two files store as
different types, or with an encoding or page outside PLAIN, dictionary and v1
pages (or LZO or Brotli compression), goes the row-by-row way instead, with
the same answer.
