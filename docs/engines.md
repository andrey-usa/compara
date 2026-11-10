# Engines and formats

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

## Input formats

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
