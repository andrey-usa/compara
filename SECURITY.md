# Security policy

compara parses untrusted CSV, JSON and Parquet files, and its Parquet reader
decodes pages itself. Any input that makes it crash, hang, read out of bounds
or report a wrong count is a bug we want to hear about.

Please report vulnerabilities privately through
[GitHub security advisories](https://github.com/andrey-usa/compara/security/advisories/new)
rather than a public issue. Only the latest release receives fixes.
