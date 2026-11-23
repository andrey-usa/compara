# Where compara is not ahead

Numbers: [bench-contenders run 45112799027](https://github.com/andrey-usa/compara/actions/runs/45112799027), 2026-11-23.

Each measure of the benchmarks on which another tool beat compara in this run by more than 2%, read from its results: wall time, CPU time, peak memory and, where the run sampled it, peak private memory, on each input, with unique keys or repeated. Closer than that is a tie. Peak memory counts the input files compara maps, which the kernel can drop; private memory is what the tool holds itself.

- Peak memory on 1M rows, CSV, 368 MB: qsv 135 MB, compara 411 MB.
- Peak memory on 1M rows, CSV, repeated keys, 368 MB: qsv 222 MB, compara 411 MB.
- Peak memory on 10M rows, CSV, 3,679 MB: qsv 2,123 MB, compara 4,030 MB.
- Peak memory on 10M rows, Parquet by DuckDB, 964 MB: DuckDB 3,417 MB, compara 3,589 MB.
- Peak memory on 10M rows, CSV, repeated keys, 3,679 MB: qsv 1,418 MB, compara 4,031 MB.
