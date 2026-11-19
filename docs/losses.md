# Where compara is not ahead

Numbers: [bench-contenders run 44629611468](https://github.com/andrey-usa/compara/actions/runs/44629611468), 2026-11-19.

Each measure of the benchmarks on which another tool beat compara in this run by more than 2%, read from its results: wall time, CPU time, peak memory and, where the run sampled it, peak private memory, on each input. Closer than that is a tie. Peak memory counts the input files compara maps, which the kernel can drop; private memory is what the tool holds itself.

- Peak memory on 1M rows, CSV, 368 MB: qsv 173 MB, compara 411 MB.
- Peak memory on 10M rows, CSV, 3,679 MB: qsv 1,430 MB, compara 4,031 MB.
