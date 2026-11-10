# Benchmarks on object storage

Against DuckDB 2.0.0 reading the same objects over S3 (`httpfs`), on one
keyed comparison of 10M rows a side, both tools giving the same four counts,
an S3 server (SeaweedFS) on the same 4-core runner, best of three
([CI run 39793328982](https://github.com/andrey-usa/compara-polyglot-experiments/actions/runs/39793328982)):

| input on S3 | compara | DuckDB | peak RSS, compara / DuckDB |
|---|---:|---:|---:|
| Parquet, every column | 9.5 s | 25.3 s | 2.7 / 9.7 GB |
| Parquet, one compared column | 6.7 s | 10.3 s | 2.0 / 3.9 GB |
| Parquet, 9 parts under a prefix | 3.5 s | 20.8 s | 3.8 / 9.1 GB |
| CSV, 1.7 GB a side | 21.5 s | 60.2 s | 4.1 / 13.8 GB |

The single objects come off this server at about 90 MB/s each however many
ranges are in flight, which is most of compara's time on them; the parts,
fetched together, come at 340 MB/s.
