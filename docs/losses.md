# Where compara is not ahead

Numbers: [bench-contenders run 44398184156](https://github.com/andrey-usa/compara/actions/runs/44398184156), 2026-11-18.

Each measure of the benchmarks on which another tool beat compara in this run by more than 2%, read from its results: wall time, CPU time, peak memory and, where the run sampled it, peak private memory, on each input. Closer than that is a tie. Peak memory counts the input files compara maps, which the kernel can drop; private memory is what the tool holds itself.

- Peak memory on 1M rows, CSV, 368 MB: qsv 132 MB, compara 411 MB.
- Peak memory on 10M rows, CSV, 3,679 MB: qsv 982 MB, compara 4,030 MB.
