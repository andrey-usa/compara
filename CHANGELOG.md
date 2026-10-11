# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

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

[0.5.0]: https://github.com/andrey-usa/compara/releases/tag/v0.5.0
