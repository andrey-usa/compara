# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- `sortmerge` read every spilled run through a 64 KB buffer of its own, so
  under a heap limit (`ulimit -d`, a container's) a large file's buffers grew
  with the file, not the limit, and the merge aborted (exit 134) or ran out
  of open files. Each run is now read through a share of the heap the merge
  may have, taken with a refusal rather than an abort, and runs beyond what
  fits, or beyond what `ulimit -n` allows, are merged in passes first; equal
  keys keep their order in the file. A row also asks its batch for room
  enough for lower case (`--ignore-case`) and for a column named twice.
- Under `--engine auto`, a CSV pair that does not fit `turbo` falls back to
  `sortmerge` from 24 MB of heap, the least a named `sortmerge` starts with,
  where it was refused below 48 MB.
- A run killed while `sortmerge` spilled (SIGKILL, the OOM killer, an
  abort) left its runs in the temporary directory for good, and enough such
  runs filled the disk. The next run that makes a temporary directory now
  removes what killed runs left there: only directories by compara's own
  names, this user's and closed to others, holding only the files compara
  writes in them, whose process is gone and whose run is over: each run now
  locks its directory and names it `...-locked`, and such a directory goes
  once nothing in it has changed for a minute. One an earlier version left
  (no lock, and its run may be going on in another pid namespace that
  shares the directory) waits until nothing in it has changed for a day.
  Anything else is left, and nothing is followed through a link. Unix only.
- A URL read near a heap limit (`ulimit -d`, or a container's) no longer
  ends the process on signal 6, "memory allocation of ... bytes failed":
  the HTTP client grows each connection's read buffer with no way to refuse,
  and sixteen requests in flight on each side took the last of the heap.
  A read now keeps no more requests in flight than the heap has room for,
  and a thread started meanwhile leaves that room; with none for one
  request it is refused, exit 2, with the limit and what to do.

## [0.8.0] - 2026-11-21

### Added

- `-o -` writes the HTML report to standard output, with the counts on
  standard error, as `--json -` does for the summary (both at once is
  refused). It replaces `-o /dev/stdout`, which is a link and now refused.

### Changed

- An `https://` file is read over HTTP/1.1, a connection to each part, also
  from a server that offers HTTP/2 (CloudFront and Google Cloud Storage
  public URLs among them), as `s3://`, `gs://` and `az://` already were:
  over one HTTP/2 connection every part's TLS and framing ran on one
  thread.
- An output that names one of the inputs is refused before anything is
  read (exit 2): `-o a.csv`, `--json ./a.csv`, a hard link to A, or an
  `--export-dir` whose `added.csv` is B would have replaced the input. Paths
  are compared as files, not as spellings.
- A JSON file that names columns with escapes, as Python's `json.dumps`
  does by default for any name outside ASCII, is compared in under half
  the instructions where an ignored value differs in every row: such a
  name is matched by the spelling the file gives it, and the check that a
  matched row is unchanged reads past it instead of parsing both rows.
  200,000 rows with one such name: 3,890.7M instructions to 1,772.9M
  ([run 44913596938](https://github.com/andrey-usa/compara/actions/runs/44913596938), cachegrind, one thread,
  against 0.7.0).
- In a container (or under `ulimit -d`), a pair whose key indexes
  cannot fit the heap goes to the sortmerge engine once its rows
  are counted, instead of after building indexes it then throws away:
  10M rows a side under a 512 MB limit, 52.8 s to 35.9 s ([run 44738688220](https://github.com/andrey-usa/compara/actions/runs/44738688220),
  cold cache, median of five interleaved rounds against 0.7.0). The note says how much the
  indexes need. Where they fit, nothing changes, also when one file's rows
  are much longer than the other's.
- A JSON file is refused for what comparing it would cost, not for its
  shape: when its objects times the columns compared pass 17.2 billion
  cells (2^34) and 256 for each byte of the file. 200,000 objects of 2 of
  20,000 keys each (7.7 MB) were refused as "not one table", even with
  `-c` naming every column; with it they compare in 0.26 s ([run 44738688220](https://github.com/andrey-usa/compara/actions/runs/44738688220),
  warm cache, median of five). The refusal says how many cells, and names
  `--compare`.
- `-c` (or `-i`, `-k`) naming many columns is checked against the files'
  headers by lookup, not a search per name: comparing a sparse JSON table
  with `-c` naming all 20,000 of its columns took 0.87 s and now takes
  0.26 s ([run 44738688220](https://github.com/andrey-usa/compara/actions/runs/44738688220), 200,000 objects of 2 keys each, warm cache,
  median of five, against the build before the lookup: 0.7.0 refuses that
  table).

### Fixed

- An `https://` file is read from a server that does not answer the
  request for its end (`Range: bytes=-n`, which brings the size with it):
  every GitHub release asset failed with `HTTP 501`, Fastly's
  "Unsupported client range", after three more tries, and a server that
  answered 416 without a size was read as an empty file. compara now asks
  for the file's start, which says the size, and reads on in ranges that
  name their first byte, as before in parallel and, for Parquet, only the
  footer and the columns compared. One that refuses every range is an
  error naming both requests and what to do.
- A server that answered the request for an `https://` file's last 64 KB
  with a longer span had all of it held in memory, however long: an
  answer longer than asked is now refused, naming it.
- A JSON file framed by its braces (one top-level array, or objects one
  after another as `jq .` writes them) with anything but space and a
  comma between two rows was read as some other rows. A stray `]` in a
  stream of objects ended the rows of the part of the file holding it, so
  the rows read moved with the thread count (100,000 read as 50,001 at
  one or two threads, 99,998 at four and up, exit 1); text after an
  array's closing `]` was dropped, and text after an object was skipped to
  the end of its line with any object on it (99,999 of 100,000). Such a
  file is now refused with the line and what to do, at every thread
  count, as is an object in it that cannot be followed to its closing
  brace (`[1, 2}`, a number running into a `{`).
- A line break inside a string of a later row of newline-delimited JSON
  (written raw where JSON writes `\n`) ended the row there for the row
  reader, and the rest of the line was a row with no key: two files
  differing past the break compared as equal, exit 0. The file is now
  refused with the line; `json.loads` and jq refuse it too.
- An object of newline-delimited JSON broken before its closing brace (a
  name with no colon, a missing quote, a line cut short at the end of the
  file) was read as far as the break, and the rest of its line was
  dropped: its later members compared as absent, or its key did and the
  row was added and removed. Such a file is now refused with the line, as
  `json.loads` refuses it.
- A JSON member name that is not UTF-8 (a Latin-1 `caf\xE9`, say) was
  listed with a replacement character but looked up as written, so the
  column had no values on either side and its changes went unreported
  with exit 0. Such a file is now refused with the line, the name's bytes
  and how to convert it (`iconv`); JSON text is UTF-8 (RFC 8259) and
  Python's `json.loads` refuses it too.
- A name repeated in one JSON object now takes its last value for every
  column, as `json.loads`, JavaScript and jq read it: a key column took
  its first, so `{"id":9,"id":10}` was row 9 for `-k id` and 10
  otherwise, and a later `null` did not clear an earlier value
  (`{"v":"a","v":null}` against `{"v":"b","v":null}` said changed). A file
  with no repeated name is read as before, at the same cost.
- A JSON key written with an escape was never read: Python's `json.dumps`
  writes every non-ASCII name that way by default (`"caf\u00e9"`), PHP
  writes `/` as `\/`. The column was listed but had no values on either
  side, so its changes went unreported with exit 0, and the same name
  written plainly on the other side made every row changed. A key is now
  the name its escapes spell, a key column's included.
- A nested JSON value (an object or an array) was compared as written, its
  spacing included, so the same records pretty-printed and compact compared
  as changed in every row with one: 133,212 rows changed of 298,486
  matched, where the records differ in 2,925 (local). The space between
  its tokens is no longer part of the value, and a string inside it is
  read however its characters are escaped (`\u00e9` or `é`, `\/` or
  `/`), as a top-level string always was. Key order and a number's
  spelling still count. The report and export show a nested value compact.
- The parts of a Hive-partitioned JSON table gave their partition values
  to every line that opened with a brace: in a pretty-printed part, the
  objects inside a row's arrays too, and in a part with several objects on
  a line, the first alone. A row's own object gets them now, after its
  members.
- A URL whose server stopped sending (an answer's headers and then
  nothing, or no answer at all) held the compare for good, over HTTP and
  from an object store alike, its progress line counting on. A read now
  waits 30 s for the answer and for each piece of it, what AWS's Java SDK
  waits and `curl --speed-time`'s default (`COMPARA_READ_TIMEOUT=<seconds>`
  waits longer), then asks again as for a dropped connection; a transfer
  ends, and says so, once its server has sent nothing on any request for
  twice that. One request stalled while others are answered, or every
  request in flight stalled once, still reads through. Object stores wait
  the same and ask again as they do after any failure, and their reads no
  longer stop at five minutes a request, which cut off an 8 MB part on a
  link under 28 KB/s. A server sending 1 KB a second is waited for. Every
  request stalled, at the default timeout: no end in 150 s to an error at
  60.1 s over HTTP and 60.1 s as S3 ([run 44913596938](https://github.com/andrey-usa/compara/actions/runs/44913596938),
  over loopback, median of three, against 0.7.0).
- A plain `http(s)://` PUT -- an output to a presigned URL -- had no time
  limit: a server that took the connection and then stopped, reading
  nothing, or reading it all and never answering, held compara for good;
  and one reading slowly was cut off after 30 s, by the HTTP client's
  TCP_USER_TIMEOUT, whose count runs on while a receiver's window opens and
  shuts again. The body is now read from the file as it goes (it was read
  into memory whole: 1 GiB more held during the upload), and the upload
  ends once the server has taken nothing for 30 s (`COMPARA_READ_TIMEOUT`)
  and the time what it may still be reading would take at 16 KB/s, the
  slowest pace waited for: compara sees only what the sockets take, and
  they hold megabytes, so a stopped server and a slow one look the same
  until then. The error says what happened, that compara gives up on a
  server taking less than 16 KB/s, and what to do. Against 0.7.0 ([run 44913596938](https://github.com/andrey-usa/compara/actions/runs/44913596938),
  over loopback, median of three): a 19 KB body to a server that reads it
  and never answers, no end in 150 s to an error at 32.1 s; 398 KB never
  read, 30.3 s to 55.1 s; 398 KB read at 32 KB/s, done in 12.2 s by both;
  at 1 KB/s, cut off at 30.3 s before and at 55.1 s now, the floor named.
- An output to a URL whose server reset the connection under a body of
  megabytes killed compara with SIGPIPE, exit 141 and no message (SIGPIPE's
  default, restored so that `compara head | head -1` ends quietly, caught
  the socket too). The upload's own error says what happened now, exit 2.
- A connection dropped or reset on a URL's first request, for the end of
  the object (its size, and a Parquet footer), failed the compare: exit 2,
  "error decoding response body". It is asked again, as every later range
  is.
- A URL's object overwritten part-way through the read, on a server that
  does not check `If-Match`, was read as one file stitched from two
  versions: 200,000 rows came out with 23,133 duplicate keys, exit 1. Each
  ranged answer's ETag, and the object size it gives, are now checked
  against those of the first, and a change is the same error as a refused
  `If-Match`: the object changed while it was being read. An answer for
  bytes other than those asked is asked for again, then an error.
- A ranged answer that ran past the range asked (from a server or proxy
  that ignores where a range ends) was written past it, over the next part
  where that had come in first: a wrong answer, no error. For the end of
  the object, whose bytes are placed by their count, all of them landed
  early: A 199,955 rows of 200,000, exit 1. Such an answer is asked for
  again, over HTTP and from the stores, and none of it is written.
- A JSON file of pretty-printed objects one after another, as `jq .`
  writes them, was read a line at a time: two objects were six rows, and
  two such files that differed compared as identical (exit 0). Its rows are
  now its objects, as a JSON array's are, and so are several objects on one
  line, where all but the first were dropped. A file read as
  newline-delimited JSON (its first object on one line of its own) whose
  later lines are not one object each is refused with the line and how to
  rewrite it (`jq -c .`); a line that was not an object had been a row with
  no key.
- A Parquet file compared under a heap limit (a container's, or
  `ulimit -d`) too small for its decoded columns stopped with exit 134
  ("memory allocation of ... failed") at most limits between too small
  to start and enough: 2M rows a side against a CSV, in memory, from
  300 to 500 MB. Its reader's allocations are checked now, so the run
  is refused with exit 2 and what did not fit. Two Parquet files
  compared by columns did the same where their dictionaries are
  interned and the report's changed cells are held.
- `--engine sortmerge` (and `auto`, where it falls back to it) near its
  heap limit (a container's, or `ulimit -d`) stopped with exit 134
  ("memory allocation of ... failed") at many limits just above the
  smallest it starts at, as its merge read ahead or its sort filled a
  batch. A block or batch that cannot grow is now handed on as it is and
  reused once read, so the run answers as without a limit, or is refused
  with exit 2 and what to raise. The merge's read buffers, one per
  spilled run, can still abort below that on files many times the limit.
- Where the system refuses a thread (a container's process limit, or a
  memory limit its stack counts against), the sortmerge and native engines
  and turbo's streaming join stopped with exit 101 ("failed to spawn
  thread"); they now do that work on the thread they have, with the same
  answer. A URL input or output is read and written on that thread too,
  where it was a panic and exit 101 ("OS can't spawn worker thread").
  Where the system gives some threads but not as many as two URL inputs
  could use, they no longer exit 101, 2 ("reading B panicked") or hang:
  each thread a transfer reads on is one compara starts itself, and one
  refused is a thread fewer, with the same answer. Where threads are
  plentiful, reads are as fast as before.
- The names of the JSON columns found past a file's first object are held
  to one budget for the file, shared by every thread reading it: 64 MB,
  or a quarter of `--memory` (or of the heap limit) when that is less.
  Each thread had 64 MB of its own and held each name twice, so a file of
  long keys took 200 MB of peak RSS at `--threads 1` and 780 MB at 4
  before it was refused, whatever `--memory` said; now 137 and 149 MB,
  and under `--memory 64M` it is refused at 41-52 MB ([run 44738688220](https://github.com/andrey-usa/compara/actions/runs/44738688220), 300
  names of 1 MB, warm cache, median of five against 0.7.0).
  Whether a file's names fit is the same at every thread count: names
  that every thread's piece meets are counted once, not once a piece.
- A JSON key with no value (`"zz":}`, `"q":,`) or no colon names no
  column, in the first object as in the later ones: the row parser reads
  no member there, and `compara columns` listed a column empty in every
  row.
- The report, the JSON summary and the export CSVs are written beside
  their names and moved into place once complete: a run that fails, is
  stopped or fills the disk leaves the file that was there before, not
  half of a new one, and a file that is replaced keeps its permissions.
  A killed run can leave its unfinished `.compara-<pid>-<n>.part` there,
  safe to delete. A file the user may write in a directory that takes no
  new file is written in place, as before.
- A file named with `__PAYLOAD__` (or `__MODE__`, `__TITLE__`) in it gave a
  blank report that said the browser could not decode it: the template's
  slots were looked for in text already put in, and the payload went into
  the page title.
- The report's "Download shown rows as CSV" quotes a value that holds a
  carriage return; such a value split its row in a spreadsheet.

### Security

- `-o`, `--json` and `--export-dir` refuse a symbolic link at the name of
  a file they would write (exit 2, saying so) and leave what it points to
  alone. They wrote through it: in a shared directory (`/tmp`, a CI
  workspace) a link left at `report.html` turned the run into an overwrite
  of the file it named. A link at the export directory's own name is
  refused too. `/dev/stdout` is such a link (`-o -` writes the report there);
  a FIFO or a device named directly (`/dev/null`) is still written.
- The HTML report carries a Content-Security-Policy that forbids it any
  connection, image, frame or form: it already loaded nothing and showed
  values as text, and now a value that ever got past the escaping could not
  fetch, post, beacon or load an image to send the report's data. A script
  could still navigate the page or open a window to an address that carries
  it (a policy cannot forbid that), so the escaping stays what keeps scripts
  out.

## [0.7.0] - 2026-11-17

### Added

- A column only one of the two files has is named in a note after the
  counts: `note: column only in B, not compared: carrier`. Such a column
  is not compared, and nothing on the terminal said so: a target that had
  lost a column compared as identical to its source, exit 0. The note is
  on standard error and changes no exit code; `-i` naming the column, or
  `-c` naming the columns to compare, says it is known. The JSON summary
  lists such columns as before (`meta.only_in_a`, `meta.only_in_b`).

### Changed

- Under `--trim`, a value of only spaces or tabs is the empty cell it
  looks like: `"  "` against an empty or missing value is no change. It was
  a change unless `--empty-is-null` was given too, which surprised most
  people who asked for `--trim`, since an empty cell is already the same as
  a missing one in CSV, JSON and Parquet alike. The answer changes for a
  comparison run with `--trim` alone on such values: they were counted as
  changed, blanked or filled and are now unchanged. `--empty-is-null` is
  still accepted, on the command line and in a profile, and changes
  nothing.

### Fixed

- A quote inside an unquoted field no longer joins rows. CSV gives a quote
  meaning only as the first byte of a field; anywhere else it is content,
  as in `5" nail`, and `native` and `sortmerge` have always read it so.
  The turbo engine found the end of a row by counting quotes, so one such
  quote took every row up to the next one like it as part of its own: a
  200,000-row file with an inch mark in every thousandth row was read as
  100,000 rows, and against a copy that differed in four of the swallowed
  rows it was reported identical, exit 0. A row with a quote that opens no
  field is now read field by field, as the other engines read it. The
  split of a large file across threads fell back on the same count: its
  bounds are now checked, and a file they are wrong for is read in one
  piece (one inch mark ahead of a quoted field of many lines gave 203,217
  rows of a 200,000-row file at four threads). Rows with no quote are
  scanned as before; a row with quotes takes a few more instructions for
  the check.
- Text between a closing quote and the delimiter is part of the value:
  `"ab"cd` is `abcd`, and `"John "Johnny" Smith"`, from an export that did
  not double its quotes, is `John Johnny" Smith"`, as `native` and
  `sortmerge` read them. turbo kept the quoted part and dropped the rest,
  so two files that differed only after a closing quote compared as
  identical. A quoted field the file ends in without closing keeps its
  last byte, which was dropped as if it were the closing quote.
- `--delimiter` refuses a quote or a line break, which no reader could
  split a row on.
- A file whose lines end in a carriage return alone is compared: the
  classic Mac line ending, which Excel still writes as "CSV (Macintosh)".
  The turbo engine ends a row at a line feed, so it read such a file as
  one long header line and no rows, and two of them that differed came
  out as `A 0 rows | B 0 rows`, exit 0. It now declines the file by its
  first line, and `auto` compares the pair with `sortmerge` (`native`
  where there is no room for it), which read either line ending, and
  says so in a note that names the input as it was given. `--engine
  turbo` says what it met and what reads it. `columns` and `head` read
  such a file as well.
- A JSON file's columns are the keys of every object in it, not of the
  first. A writer that leaves a null field out of its object (Spark's
  does, and Go's `omitempty`) leaves the column out of the first object
  whenever the first row has no value there; that column was then no
  column, and two files that differed only in it were reported identical,
  with exit code 0. Newline-delimited JSON and a top-level array are both
  read so, `compara columns` lists the same columns, in the order the file
  first shows them on any number of threads, and `compara head` lists
  those of the rows it shows. Every object's keys are now read once
  before the compare, which a JSON compare pays for in time; CSV and
  Parquet are untouched. A file in which no object has a key is refused
  as that.
  A file whose objects share almost no keys (every object naming a
  column of its own) is not one table and is refused as that, in under a
  second, unless `--compare` names the columns: its cells are the square
  of its rows.
- A table with tens of thousands of columns is read in the time its bytes
  take. Each column's name was searched for in the list of all of them,
  so the time grew with the square of the width.
- `--delimiter '\t'` and `--delimiter tab` are a tab, on the command line
  and as a profile's `delimiter`. Only the first character of the value was
  taken, so `'\t'` split every row on a backslash and `tab` on the letter
  t, and the run ended on a key column that was "not in A". A value longer
  than one character is now refused, with the tab's spelling.
- `--tolerance inf` and `--tolerance nan` are refused. Infinity made every
  pair of numbers equal and NaN was no tolerance at all, and neither said
  so.
- `compara head` keeps a row on one line when a value holds a line break
  or a tab: the aligned table shows it as `\n`, `\r` or `\t` (`--csv`
  prints the value as it is). A break used to start a new line in the
  middle of the table.
- A Parquet compare whose files fail in more than one column says the
  same error on every run and every machine: the first failing column in
  the order the columns are read, the one `--threads 1` reports. The
  columns are read on several threads, and the error was whichever thread
  failed first, so one fuzzed file gave "a snappy Parquet page declares
  11542724607 bytes" on one run and "unknown type in the parquet
  metadata" on another (15 of 25 runs at 8 threads; the test that holds
  it failed on a macOS runner). Columns after a known failure are no
  longer read.
- A profile whose list runs over several lines is read. `key = [` with
  one name to a line under it, as formatters write a list, was taken a
  line at a time: an empty key, and the comparison refused for having
  none.
- A profile says what it does not understand. A key it does not have
  (`ignore_cas = true`, `threads = 4`) and a flag that is not `true` or
  `false` (`trim = yes`) changed nothing and said nothing, so the
  comparison ran without the option; each is now refused with its line,
  and a key one or two letters off a real one with the real one. A
  profile name that is not in the file is answered with the names that
  are, and `--config` without `--profile`, which used nothing from the
  file, is refused.
- An option given more than once is no longer half ignored. The first
  value was the whole answer: `-k order_id -k line_no` compared on
  `order_id` alone, and the second of two `-o` was a report nobody
  wrote. The options that name columns (`-k`, `-c`, `-i`) now add up,
  and any other option given twice with two values is refused by name.
- `columns` and `head` look at one file and refuse a second, which was
  passed over without a word.
- The time on the counts line is written to the millisecond; a run
  under one printed `0s`.
- A name that matches a column but for its case, or for spaces around
  it, is answered with that column: `key column "id" not in A (its
  columns: "id ", " v"); a name is matched exactly, case and spaces
  included: there is "id " for "id"`. A header of `ID` or `id ` gave a
  list in which the name seemed to be there. The same for `--compare`
  and `--ignore`; a name with a space at either end is shown in quotes.
- `--engine native` and `--engine sortmerge` say that a file is not
  UTF-8 and which engine compares it by its bytes. They answered with a
  parse error from the CSV reader, after the note that `--encoding
  latin1` prints to say the counts will be right.

### Security

- chrono and humantime, which the cloud reader (object_store 0.14.2, its
  latest) depends on, are now marked unmaintained by RustSec
  (RUSTSEC-2026-0543, RUSTSEC-2026-0542). Neither is a vulnerability and
  neither has a fixed version; both are listed in `deny.toml`, with that
  reason, so the audits in CI still fail on any other advisory.
- Temporary files are the user's alone. compara spools standard input,
  downloads from object storage, decompressed copies and a table read
  from many files to the temporary directory, which most machines share
  between their users, and it made them with the default mode: under the
  usual umask every user of the machine could read them, a table fetched
  with the caller's cloud credentials included. The directories it made
  there were open the same way: the runs the `sortmerge` engine spills,
  which hold every row of both files, and an export or report on its way
  to object storage. Files are now made readable and writable by their
  owner only and directories closed to everyone else, each in one step
  and only where nothing is, under a name that cannot be guessed from the
  process id. Windows keeps its temporary directory per user and was not
  affected.

## [0.6.1] - 2026-11-10

### Added

- `compara skill` prints the agent skill this version carries: how an
  agent finds the key, reads the counts and the changed columns as JSON,
  and gets the rows, with the options that change the answer. A test
  holds every option it names to one `compara --help` lists.

### Security

- A snappy page in a Parquet file is held to the size its page header
  gives before any room is made for it. The stream opens with a length
  of its own, and the columnar reader took that length's word: a 2.8 KB
  file whose 11-byte page opened with "11.5 GB follow" had that much
  address space reserved, and was then refused as out of memory, or as
  a page that would not decompress, depending on the machine. It is now
  refused at once as what it is (`a snappy Parquet page declares
  11542724607 bytes where its header says 11`), and a length that the
  stream's bytes could not expand to (64 bytes from every three is the
  format's most) is refused before the header is asked. Found by the
  nightly fuzz job ([run
  42802471876](https://github.com/andrey-usa/compara/actions/runs/42802471876));
  the file is `tests/fixtures/hostile/snappy_length.parquet`.
- Keys built to share one hash made a compare quadratic: every engine
  hashed keys with a fixed FNV, and a file whose keys cancel each other's
  words put every row in one probe chain, so the work grew with the square of
  the rows (`tests/hostile.rs` builds such keys). Keys are now hashed by a full 64x64-bit multiply,
  folded, by a multiplier and from a start chosen at random for each run,
  so they can't be built to collide. Results are the same as before, byte
  for byte, and no slower: turbo takes 3.0% fewer instructions at 50k rows
  and 2.4% fewer at 200k than 0.6.0 does with this and the 32-bit hash
  below (cachegrind, [run 42703350843](https://github.com/andrey-usa/compara/actions/runs/42703350843)). `COMPARA_HASH_SEED` fixes the keys for a run that
  must repeat exactly (a benchmark).

### Changed

- On a CPU with AVX-512BW the turbo engine's CSV scan steps 64 bytes at a
  time instead of 32, chosen at run time, so the released binaries reach
  it without being built for it; AVX2, NEON and the portable scan stay as
  they were, and the output is the same byte for byte. Where memory
  bounds the scan it changes nothing: on a GitHub runner with a Xeon
  Platinum 8573C a 2M-row CSV pair compares in 0.215 s against 0.211 s
  before (median of 7, [run
  42764485535](https://github.com/andrey-usa/compara-polyglot-experiments/actions/runs/42764485535)).
  On two machines where the CPU bounded it, the two CSV sweeps took less
  than half their time before (indicative: measured outside CI).
- On arm64 the turbo engine's CSV scan packs its 32 compare bytes into a
  mask with three pairwise adds and one lane read, where it took four
  horizontal adds; same mask, same output. A 2M-row CSV pair compares in
  0.172 s against 0.196 s before on GitHub's arm64 runner (median of 7,
  interleaved), with 11.0% fewer instructions (cachegrind; [run
  42772693608](https://github.com/andrey-usa/compara-polyglot-experiments/actions/runs/42772693608)).
- A gzip or zstd input is expanded into memory on Linux while it fits a
  quarter of the memory free (inside a container, a sixth of the heap
  limit, which it then comes out of), and to `TMPDIR` past that. A 10M-row
  `.csv.zst` pair compares in 3.03 s on a 4-core runner, with the same
  counts as DuckDB (best of 3, the `zstd` job of bench-contenders, [run
  41494922719](https://github.com/andrey-usa/compara/actions/runs/41494922719)).
- The sortmerge engine sorts each batch by an eight-byte key prefix held
  beside the row number, reading the key text only on a tie; same output.
  With the spill record below, sortmerge takes 30.9% fewer instructions at
  50k rows and 30.1% fewer at 200k than 0.6.0 (cachegrind, [run
  42703350843](https://github.com/andrey-usa/compara/actions/runs/42703350843)).
- The turbo engine keeps each row's key hash in 32 bits rather than 64 (it
  only ever filtered candidates; the key bytes decide), and frees the
  duplicate-key counts once the duplicate sections are written: 1.9% fewer
  instructions at 50k rows and 0.9% fewer at 200k (cachegrind, [run
  40973415500](https://github.com/andrey-usa/compara/actions/runs/40973415500)); same output.
- The sortmerge engine's spilled runs hold each row as one record (its
  length, a length per cell, then its text), read back in two reads and
  checked once, where each cell was read and checked on its own: about a
  quarter fewer instructions: 24.3% fewer at 50k rows and 23.4% fewer at
  200k with runs forced to spill (cachegrind, [run
  40996649319](https://github.com/andrey-usa/compara/actions/runs/40996649319)); runs on disk about the same size; same
  output. A damaged run (cut short,
  lengths that don't add up, text that isn't UTF-8) is an error.

### Fixed

- A file with two columns of the same name was compared with one of them
  left out and no word about it; it is refused now, naming the side and the
  column (`A has two columns named "id"`), unless `-i` or `-c` leaves that
  column out. A name repeated inside one JSON object is still one column,
  its last value; unnamed columns (a header ending in commas) are not
  repeats.
- `--encoding` was accepted and ignored. Latin-1 and the Windows code pages
  still compare correctly as bytes and now say so in a note; UTF-16 and
  UTF-32, which can't, are refused with how to convert, as is a name that
  isn't an encoding. A UTF-16 file is recognized by its byte-order mark
  instead of failing on a header of noise.
- A key that isn't in a file says which side lacks it and what columns that
  side has, without "the turbo engine failed" before it: nothing failed.
- Delta-encoded Parquet integers whose deltas need more than 32 bits (an
  INT64 column with large jumps between rows) lost their high bits and were
  read as wrong values; they are read whole now.
- At a tight heap limit (a container's, or `ulimit -d`) a run could abort
  (SIGABRT) instead of refusing with exit 2: while one side's checked
  allocation met the ceiling, the other side's thread could meet it with a
  small one. The reserve kept for the refusal now covers that too; a test
  holds it (`the_band_where_it_aborts_does_not_grow`, green on Linux, ARM,
  macOS and Windows in [run
  40372192496](https://github.com/andrey-usa/compara/actions/runs/40372192496)).

### Changed

- Messages say what happened in plain words:
  - an option compara doesn't have is named as unknown (`--bogus` used to be
    reported as needing a value), and an unknown command names the
    commands there are in one line instead of printing the whole usage;
  - an empty file, a UTF-16 file and a binary file are each said as what
    they are, by every command and engine, instead of a strange column name
    and a missing key;
  - a cloud read with no credentials starts with what to set, and the
    store's retry log is left out; a URL that can't be reached says why (the
    host name does not resolve, nothing answers on that port, no answer
    within 30 s) and that a proxy is set when one is;
  - exit 3 says it came from `--fail-on-dups`;
  - a Parquet file that can't be read says so (`reading the Parquet files:
    …`) instead of "the turbo engine failed";
  - standard input and a decompressed file are named as given (`standard
    input is empty`, `e0.gz is empty`), not as the temporary copy read;
  - a pipe or process substitution given as a file (`<(…)`) says to pipe
    into `-` instead;
  - a `--delimiter` that isn't in a file's header says so, and what the
    header has, instead of quietly reading every row as one column.
- The counts line labels what it shows: `duplicate keys A 0, B 1 | engine
  turbo, 0.4s`, and `1 row` is singular. The HTML report says
  `engine turbo` and `1 row` too, and its tab title and heading agree.
- `--help` fits 80 columns, describes every option, and says what the three
  engines are for.

### Security

- Through a proxy (`HTTPS_PROXY`), a table at a name that leads to a cloud
  metadata address (a hosts entry or DNS name for 169.254.169.254), or at
  a metadata service's own name (`metadata.google.internal`), was read: the
  proxy looked the name up, past compara's check. The name is looked up
  before the request goes to the proxy, and refused like the address
  (`COMPARA_ALLOW_LINK_LOCAL=1` allows it); so is a redirect to one. A name
  only the proxy can look up still goes to it. Found by red-teaming the
  0.6 check.

- A Parquet page that claims more values, or more uncompressed bytes, than
  its stored bytes can hold by the decompression-bomb rule (past 1 GB, more
  than 1000 times) is refused before anything is sized from it
  (`COMPARA_MAX_EXPANSION`). Found by fuzzing: a 9.7 KB file had 2.8 GB
  reserved for a page that claimed 1.47 billion values.
- A Parquet page that claims a bit width wider than its values (found by
  fuzzing) is refused with a message; a debug build panicked on it and a
  release one read garbage.

- A gzip or zstd input that expands past 1 GB to more than 1000 times its
  size is refused as a decompression bomb; it was expanded whole into
  `TMPDIR` first (`COMPARA_MAX_EXPANSION=<times>`, 0 for no limit).
- `http(s)://` inputs and their redirects no longer reach link-local
  addresses (cloud metadata services: 169.254.169.254, fe80::/10,
  fd00:ec2::254), by address or by name (`COMPARA_ALLOW_LINK_LOCAL=1`
  allows it); redirects stop after 10, and a refused one says why.

## [0.6.0] - 2026-10-26

Tables compared where they are: in S3, Google Cloud Storage and Azure, on
the web, as directories and globs of parts, and as Delta Lake tables at a
version. A container image and a GitHub Action. Faster on every format,
and inside a container's memory limit a refusal that says why rather than
an OOM kill.

### Added

- Inputs from object storage and the web: `s3://`, `gs://`, `az://` (and
  `adl`, `azure`, `abfs`, `abfss`), `http://` and `https://`, wherever a
  file is accepted. Credentials come from each cloud's own environment --
  `AWS_*` and `~/.aws` profiles (keys, session tokens, `AWS_ENDPOINT_URL`
  for MinIO and the like, web identity for EKS, ECS task roles, instance
  metadata), `GOOGLE_*` and application default credentials, `AZURE_*`
  (account keys, SAS, service principals, managed and workload identity)
  -- and a presigned or SAS URL is read as it is. Objects are read in 8 MB
  ranges, sixteen at a time (`COMPARA_FETCH_CONCURRENCY`), into a temporary
  file. A Parquet object is read only as far as the comparison reads it:
  its footer and the column chunks of the key and compared columns, so
  `--compare` on two columns of a wide table fetches those two. `columns`
  on a remote Parquet file reads its last 64 KB. The `cloud` feature is on
  by default; without it the binary reads local files only. Every range is
  read from the version whose first bytes were read (`If-Match` on its
  ETag), so an object overwritten mid-read is an error rather than a file
  stitched from two versions; throttled and failed ranges are retried.
  A plain HTTP server that ignores ranges sends the object whole; it is
  written to disk as it arrives (the temporary directory's room checked
  first), never held in memory.
- A download that takes a while says how far it has got every ten seconds
  on stderr, so a CI job's or a pod's log shows it moving
  (`COMPARA_PROGRESS` sets the seconds; 0 turns it off).
- Outputs to the same places: `-o`, `--json` and `--export-dir` take an
  `s3://`, `gs://` or `az://` URL (or a presigned `https://` one), written
  in one request or as a multipart upload of 16 MB parts. `--json -` prints
  the summary on standard output, and the counts line goes to standard
  error, so a pipeline or a CI step reads one JSON document.
- A directory, a quoted glob (`'out/date=*/*.parquet'`, `**` for any depth)
  or an object storage prefix (`s3://bucket/orders/`, or the same without the
  slash when no object has that name) is a side: its files, read as one table
  in path order. CSV and TSV parts must share a header, Parquet parts a
  schema (names, order, types, nullability), and the error names the part
  and the difference; compressed parts are expanded on the way. Remote parts
  are fetched with sixteen ranges in flight across all of them, Parquet
  parts only as far as the comparison reads them. Hive partition
  directories (`date=2026-10-01/`) are columns of the table, as text, unless
  the files already have them: a Parquet part gets a column chunk of one
  dictionary value per row group, a CSV or JSON part the value in every row.
  `.`- and `_`-prefixed names are passed over; Iceberg tables are refused.
- Delta Lake tables, local or in object storage, at their latest version or
  at `@vN` (`s3://lake/orders@v41`): the files the log says make that
  version, from its last checkpoint and the commits after it, so two
  versions of a table compare like two tables. Deletion vectors, column
  mapping, V2 checkpoints and a log cleaned up past the version asked for
  are refused with what to do instead; a Delta table further down a
  directory is named rather than read as loose files.
- Credentials as each cloud's own tools find them, laptops included: an AWS
  profile that uses SSO, an assumed role or a `credential_process` is
  resolved as the AWS CLI resolves it; an `az login` is used when Azure has
  nothing else; and with no credentials at all a public bucket is read
  unsigned rather than refused.
- `--exit-zero`: exit 0 when the tables differ, for a scheduled job whose
  output is the report -- a Kubernetes Job, an Airflow task -- where exit 1
  would mark the run failed.
- `-` for either file reads standard input.
- Gzip and zstd files are read as what they hold, by their first bytes
  rather than their names (`.csv.gz`, `.ndjson.zst`); bzip2, xz and zip are
  named and refused rather than read as a CSV.
- `compara env`: the CPUs, memory, container limit, heap limit and
  temporary directory this process sees, with the room left in it, and
  where each cloud's credentials would come from (keys, a profile, web
  identity, a CLI sign-in, metadata) -- by what is set, nothing fetched. A
  download or a table of parts that the temporary directory has no room for
  is refused before it starts, with how much it needs and where.
- A container image, `ghcr.io/andrey-usa/compara`: the release binary on
  distroless for `linux/amd64` and `linux/arm64`, non-root, with build
  provenance and an SBOM.
- A GitHub Action (`uses: andrey-usa/compara@v0`): installs the release
  binary, compares, writes the counts and the most-changed columns to the job
  summary, and fails the step on differences; its outputs are the counts and
  the JSON summary's path.

### Changed

- A closed pipe ends the process quietly, as it ends `cat`: `compara head
  big.csv | head -1` used to print an internal error.
- Faster on every format and thread count. On 2M-row pairs, 4-core GitHub
  Actions runners, best of five (CI run 39728278163), the time against the
  C port of the same tool: CSV 0.75x at one thread and 0.83x at four, TSV
  0.74x and 0.81x, NDJSON 0.52x and 0.67x, Parquet 0.57x and 0.73x.
  - Quoted CSV: the row end and the join's register count follow quotes by
    parity, a register at a time, instead of a scan per quoted field and a
    field walk per quoted row. On the same 2M rows with their text fields
    quoted, 0.61x the C port's time at one thread and 0.63x at four (CI run
    39729954571).
  - Parquet: the join reads each key column through a form settled once per
    column, and compares short keys by a load or two instead of `memcmp`:
    the match sweep runs in half the instructions.
  - Buffers of 4 MB or more are advised onto transparent huge pages, so
    filling them takes a page fault per 2 MB rather than per 4 KB: on the
    2M-row Parquet pair 75,651 faults become 16,889.
  - NDJSON rows are proved equal before they are parsed, and the CSV join's
    register count, key hash and index insert do less per row.
- Inside a container with a memory limit, the heap is held under that limit
  (`RLIMIT_DATA`, a sixteenth below it), so running out of memory is a
  refusal that says what needed how much -- exit 2 -- rather than an OOM kill
  with no message (exit 137). Under `--engine auto` a CSV pair whose indexes
  do not fit is then compared by the `sortmerge` engine, which spills to
  disk, with the same answer. `COMPARA_CONTAIN=0` turns this off. In a
  container, files being written -- spilled runs, downloads, expanded gzip
  -- are handed to the disk as they go rather than left dirty in the page
  cache, where they count against the same limit: a 160 MB container whose
  heap stayed under 120 MB was OOM-killed by them.
- `sortmerge` sizes its batches from the heap it may have, and builds the
  report's rows only when there is a report: a summary run of a 2M-row CSV
  pair now completes in a 40 MB heap (`ulimit -d`), where it failed in
  100 MB. Named with `--engine sortmerge` in a heap under 24 MB, it is
  refused before it starts rather than aborted midway; under 48 MB `auto`
  doesn't fall back to it on its own, and its refusal says that
  `--engine sortmerge` tries it.
- The README has a table of comparisons read from S3 against DuckDB's
  `httpfs` (CI run 39793328982): at 10M rows a side, 2.7 times faster on a
  Parquet pair, 2.8 on CSV, 6.0 on a table of Parquet parts, in a third to
  a half of its memory.
- The README's speed table is rerun on this code (CI runs 39728430053 and
  39729034092): at ten million rows and four threads, CSV 1.20 s to 1.08 s,
  NDJSON 2.79 s to 1.26 s, Parquet 1.41 s to 1.17 s. Its input column was
  labelled "a side" but held both files; it says so now.

## [0.5.3] - 2026-10-22

Two Parquet files compared up to 2.5 times faster, JSON files that are one
top-level array, and two silent wrong answers on JSON input fixed.

### Added

- A JSON file can be one top-level array of objects, compact or
  pretty-printed, as well as one object per line: the rows are the array's
  objects and the columns the keys of the first one. Newline-delimited JSON
  reads as fast as before.

### Fixed

- A JSON value that is an object or an array was read as absent, so two
  different nested values compared as the same missing value and the change
  went unreported (0.5.2 and earlier). It is now compared as its JSON text,
  as written, and the report and export show that text. The README says what
  that means for key order and spacing, and when to flatten first.
- A pretty-printed top-level JSON array whose rows hold objects on lines of
  their own lost rows when read on several threads: a split guessed at the
  next line opening an object could land inside a row, and the rows read
  from there were dropped without a word (200,000 rows counted as 100,718
  at four threads). Splits are now checked as CSV splits are, and redone on
  bounds that follow the array's depth when a guess was wrong; the guess
  itself takes the column the first row opens at. Top-level arrays were new
  in 0.5.3-alpha.1; 0.5.2 did not read them.

### Changed

- Two Parquet files compare faster on the columnar path. Snappy pages are
  expanded with whole-word copies; a column's row groups are read in runs on
  threads of their own, both sides at once, so a file with fewer columns
  than cores no longer leaves cores idle; a slot of the key index holds its
  row, so a match costs one cache miss less, the cells a probe compares are
  prefetched, and integer, date and other word keys are hashed a column at a
  time; the compared columns go largest first and each is compared on
  several threads; the footer is parsed once per file. In CI (run
  39468615515, a 4-core runner, a 5M-row pair written by pyarrow, counts
  only) the comparison took 1.26 s at one thread, 0.79 s at two and 0.56 s
  at four, against 2.34 s, 1.53 s and 1.40 s for 0.5.3-alpha.1; DuckDB took
  6.73 s, 3.94 s and 3.13 s and Polars 4.98 s, 3.32 s and 2.73 s on the same
  runner. With the changed rows written out: 1.29 s, 1.09 s and 0.72 s,
  against 2.43 s, 2.19 s and 1.90 s. Answers are the same; a footer the
  row-group runs cannot trust is read in one pass, as before.
- `--export-dir` on two Parquet files keeps the decoded compared columns for
  the export when the memory budget has room for them, instead of reading
  each column a second time.
- The key index is built on one thread unless `COMPARA_PARALLEL_INSERT` is
  set: the parallel build, taken from 1M rows on, measured slower.
- Each occupied slot of the key index is ranked with one POPCNT per key where
  the CPU has it, chosen at run time.
- `COMPARA_PHASES=1` also prints each compared Parquet column's read and
  compare times.
- The README lists TSV and JSON as input formats of their own, with how each
  is read: TSV through the CSV reader, JSON as one object per line or one
  top-level array of objects, its columns the keys of the first object.

## [0.5.2] - 2026-10-16

### Added

- `compara-gen-data --format tsv` writes the CSV rows tab-separated, so a TSV
  pair can be benchmarked against the same rows in CSV. On a 1M-row pair TSV
  is within 5% of CSV on every engine (`turbo` 0.15 s against 0.14 s,
  `native` 1.33 s against 1.28 s, `sortmerge` 2.07 s against 1.98 s, four
  threads) and gives the same answer and export files, which
  `tests/formats.rs` now holds every engine to.

### Changed

- On x86-64 the CSV, TSV and NDJSON readers scan with AVX2 when the CPU has
  it, asked at run time, so the release binaries and `cargo install` get the
  vector scan that before needed a build made with `-C target-cpu=x86-64-v3`.
  On a 2M-row pair, four threads, CSV counts in 0.24 s against 0.28 s (paired
  1.16x), TSV 1.19x, NDJSON 1.02x, level with the x86-64-v3 build on all three;
  answers and export files are the same, byte for byte. A CPU without AVX2,
  and aarch64, scan eight bytes at a time as before.
- `--export-dir` on two Parquet files runs on the columnar path. Before, it
  stopped with "--export-dir is not supported on the parquet path" unless
  `--engine turbo` was given, which reads the files row by row: on a 1M-row
  pair with 60,000 changed rows that took 1.48 s and 766 MB, now 0.31 s and
  363 MB; at 5M rows 11.3 s and 3.8 GB, now 1.8 s and 1.7 GB. The files are
  the same, byte for byte.
- `--export-dir` without `-o` no longer gathers the report's rows, which
  nothing then reads.
- Two Parquet files compressed with gzip, zstd or LZ4 are compared on the
  columnar path, as snappy and uncompressed ones were. Before, any of them
  sent the pair through the row-by-row reader, and zstd is what polars writes
  by default: a 2M-row pair it wrote took 1.14 s at two threads, now 0.39 s,
  what the same pair written with snappy takes.

## [0.5.1] - 2026-10-14

### Changed

- `compare` writes the HTML report only with `-o`/`--out`. Without an output
  flag it prints the counts and writes nothing; before, it wrote
  `<a>__vs__<b>.html` into the working directory. `--json` and
  `--export-dir` write their files on their own, and `--no-compress` without
  `-o` is an error.
- `--engine sortmerge` or `--engine native` on a Parquet or JSON input exits 2
  with a message. Before, a Parquet pair took the columnar path anyway and
  the report said `parquet`.
- `--memory` and `COMPARA_MEMORY` apply to two Parquet files: they decide how
  many columns are read at once, and a pair that needs more than a stated
  budget is refused before anything is read. Before, the columnar path
  ignored them.
- Two Parquet files with boolean, integer, float, date, time, timestamp or
  decimal columns are compared on the columnar path, a word per value. Before,
  one such column sent the pair through the row-by-row reader: a 1M-row file
  written by DuckDB took 1.9 s and 4.5 s of CPU, now 0.4 s and 1.0 s.
- `native` holds rows packed and indexes keys without copying them: a 1M-row
  pair took 27 s and 2.9 GB, now 1.6 s and 0.7 GB; 10M rows take 33 s and
  6.7 GB, where 72 s and 25.7 GB were reported.
- `sortmerge` reads and sorts the two files at once and merges with a heap:
  a 1M-row pair took 9.8 s, now 2-3 s; 10M rows 209 s, now 35 s, in 260 MB.

### Fixed

- A CSV or JSON side compared with a Parquet side showed changed cells that
  held the same value: `31.40` against the DOUBLE 31.4, `12345` against the
  DECIMAL 12345.00, `2026-09-01 02:15:00` against the timestamp
  `2026-09-01T02:15:00`. A column Parquet stores as numbers now compares by
  exact value, and a temporal one by instant; a 100k-row CSV against its own
  Parquet export went from 100,000 changed rows to none.
- A UTF-8 byte order mark at the start of a CSV or NDJSON file became part of
  the first column's name in the default engine (`key column(s) missing`),
  and made an NDJSON file read as CSV.
- The weekly contender benchmark now holds DuckDB and pandas to compara's
  counts on the same question: it counted duplicate keys per row, read empty
  strings as NULL and did not map `unchanged`.

Hostile Parquet and NDJSON files are refused with exit 2 and a message,
where before a few bytes could crash or exhaust the process (found by
mutation fuzzing; each case is a test in `tests/hostile.rs`):

- a zstd page whose stream expands past its header's size was decompressed
  in full first: a 133 KB file used 8 GB of memory. Decompression now
  stops one byte past the declared size (zstd, gzip, snappy, LZ4).
- a footer nested 200,000 structs deep overflowed the stack; nesting past
  64 levels is refused.
- a list count larger than the footer could hold sized an allocation of
  up to 100 GB (abort) or, for lists of bools, looped for hours.
- a string length, page size or value count near 2^64 or negative wrapped
  the bounds checks and panicked.
- a `DELTA_BINARY_PACKED` header claiming a 10^16-value miniblock, and an
  RLE run of billions of values, were materialized up front (abort).
- an NDJSON file cut off right after a key's opening quote panicked
  `head` and `compare`.
- `--no-compress` reports: a value holding `<!--<script>` kept the
  report's script from running (a blank page). Every `<` in the embedded
  JSON is now written `\u003c`.

### Security

- Release workflow: read-only token by default, write access only for the
  job that creates the release; signed build provenance for the archives;
  every action pinned to a commit. CI runs `cargo audit`.

## [0.5.0] - 2026-10-09

First release as a standalone project.

### Added

- `compare`, `columns` and `head` over CSV, newline-delimited JSON and Parquet,
  with either side in any format.
- Three engines with one result contract: `turbo` (default), `sortmerge`
  (bounded memory, spills to disk) and `native`.
- A self-contained HTML report, a JSON summary (`--json`), uncapped CSV exports
  (`--export-dir`) and `--summary` for counts only.
- Profiles in `compara.toml` for recurring comparisons.
- `compara --version`.
- `compara-gen-data --row-group-size` and `--dict-limit` for the Parquet
  layout.
- Prebuilt binaries for Linux (x86_64, aarch64; glibc 2.28+), macOS (arm64,
  x86_64) and Windows (x64); packages for crates.io, npm, PyPI, Homebrew and
  Scoop.

### Changed

- The data generator and the benchmark harness are `compara-gen-data` and
  `compara-bench` (the latter behind the `bench` feature).

[0.6.0]: https://github.com/andrey-usa/compara/releases/tag/v0.6.0
[0.5.3]: https://github.com/andrey-usa/compara/releases/tag/v0.5.3
[0.5.2]: https://github.com/andrey-usa/compara/releases/tag/v0.5.2
[0.5.1]: https://github.com/andrey-usa/compara/releases/tag/v0.5.1
[0.5.0]: https://github.com/andrey-usa/compara/releases/tag/v0.5.0
