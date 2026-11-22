# Security policy

compara parses untrusted CSV, JSON and Parquet files, and its Parquet reader
decodes pages itself. Any input that makes it crash, hang, read out of bounds
or report a wrong count is a bug we want to hear about.

## What to expect

- A malformed or hostile file should end the run with exit 2 and a message;
  a panic, an abort or a hang is a bug. `tests/hostile.rs` holds one file per
  way this has failed before (deep or oversized footers, wrapping lengths,
  decompression bombs, truncated rows), and CI runs it on every push.
- No `unsafe` code depends on the file's contents: the `unsafe` blocks are
  SIMD loads behind bounds checks, prefetches, a `set_len` after the writes
  it covers, and the memory map.
- Memory grows with the row counts a file declares. A valid Parquet file can
  describe a billion equal values in a few kilobytes, so a small file is not
  a small comparison; run untrusted inputs under a memory limit (`ulimit -v`,
  a container) if that matters.
- A gzip or zstd input that expands past 1 GB to more than 1000 times its
  size is refused (`COMPARA_MAX_EXPANSION`), and so is a Parquet page that
  claims that many values or bytes from what it stores; an `http(s)://` input or
  redirect that leads to a link-local address (a cloud metadata service) is
  refused (`COMPARA_ALLOW_LINK_LOCAL=1`), through a proxy too: the name is
  looked up before the request goes to the proxy. Other private and loopback hosts
  stay readable: they are where many tables live.
- What compara writes to the temporary directory (`TMPDIR`) is for the user
  who ran it alone: a spooled standard input, a download, a decompressed
  copy, the runs `sortmerge` spills, an export on its way to object storage.
  Files are made with mode 0600 and directories 0700, never at a name that
  already exists, and removed when the run ends. A directory a killed run
  left there is removed by the next run that makes one, when it is provably
  compara's and over: compara's own name, this user's and closed to others,
  only compara's files in it and no link, not locked by a live run, its
  process gone, and nothing in it changed for a minute; a day for one an
  earlier version left, which no lock protects (Unix).
- The files a run is told to write (`-o`, `--json`, `--export-dir`) are
  written only at the names given: a symbolic link at one (or at the
  export directory) is refused, not followed, a name that is one of the
  inputs is refused before anything is read, and each file is written
  beside its name and moved onto it once complete. A device (`/dev/null`,
  `NUL`) and a file at a name that takes no new file or no rename (a
  directory the user may not add to, another user's file in a sticky
  directory, a file mounted at the name) are written in place instead: a
  run that fails there can leave the file half written, and its error
  names it.
- The HTML report shows cell values as text. It loads nothing from the
  network and runs no code from the data, and its Content-Security-Policy
  forbids it any connection, image or form, so a value that got past the
  escaping could not fetch, post or load anything. It does not stop a script
  from navigating the page or opening a window to an address that carries
  data: the escaping is what keeps scripts out, the policy a second layer.
- Exported CSVs (`--export-dir`, the report's download) keep values exactly
  as they were. A value beginning with `=`, `+`, `-` or `@` is a formula to a
  spreadsheet application: open untrusted exports as text.
- Writing a file you are comparing while compara has it mapped can end the
  run with SIGBUS; compare a copy of a file that is still being written.

## Reporting

Please report vulnerabilities privately through
[GitHub security advisories](https://github.com/andrey-usa/compara/security/advisories/new)
rather than a public issue. Only the latest release receives fixes.
