# Compare tables in object storage and on the web

Any file argument can be a URL, and so can `-o`, `--json` and
`--export-dir`:

```sh
compara compare s3://lake/orders/2026-09.parquet s3://lake/orders/2026-10.parquet -k order_id \
  --json s3://reports/orders/summary.json --export-dir s3://reports/orders/diff/
compara compare gs://bucket/export.csv.gz az://container/warehouse.parquet -k id
compara compare s3://lake/orders/run=41/ 's3://lake/orders/run=42/date=2026-10-*/*.parquet' -k order_id
compara compare "https://bucket.s3.amazonaws.com/a.csv?X-Amz-Signature=..." b.csv -k id
aws s3 cp s3://bucket/a.ndjson - | compara compare - b.ndjson -k id --json -
```

| Scheme | Credentials, as that cloud's own tools find them |
|---|---|
| `s3://`, `s3a://` | `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`, a `~/.aws` profile (`AWS_PROFILE`) -- static keys, or SSO, an assumed role or a `credential_process`, resolved the way the AWS CLI resolves them -- web identity (EKS), the ECS task role, instance metadata. `AWS_ENDPOINT_URL` for MinIO, R2, Ceph and the like. The bucket's region is looked up when none is set. |
| `gs://` | `GOOGLE_APPLICATION_CREDENTIALS`, application default credentials (`gcloud auth application-default login`), the metadata server (GKE, Cloud Run). |
| `az://`, `abfs(s)://`, `azure://`, `adl://` | `AZURE_STORAGE_ACCOUNT_NAME` with a key (`AZURE_STORAGE_ACCOUNT_KEY`), a SAS token (`AZURE_STORAGE_SAS_KEY`), a service principal, managed or workload identity, or an `az login` when there is nothing else. |
| `http://`, `https://` | none: the URL is fetched as given, so a presigned S3 URL or an Azure SAS URL works as it is. A refused one that names a bucket or a storage account is retried signed. |

- **No credentials, no setup**: with none to be found, a public bucket
  (open data on S3 or GCS) is read unsigned, and the note on stderr says so;
  a private one fails with what to set.
- **Parquet is read as far as the comparison reads it**: the footer, then
  only the column chunks of the key and compared columns. `--compare` on two
  columns of a 200-column table fetches those two; `compara columns` fetches
  the last 64 KB.
- **Everything else is read in parallel ranges** (8 MB, sixteen at a time,
  `COMPARA_FETCH_CONCURRENCY`) into a temporary file in `TMPDIR`. Gzip and
  zstd files, local or remote, are read as what they hold; one that expands
  past 1 GB to more than 1000 times its size is refused as a decompression
  bomb (`COMPARA_MAX_EXPANSION=<times>`, 0 for no limit).
- **Link-local addresses are not read**: an `http(s)://` table at
  169.254.0.0/16, fe80::/10 or fd00:ec2::254 (where cloud machines get
  their credentials), by address, by name or by a redirect, is refused;
  `COMPARA_ALLOW_LINK_LOCAL=1` allows it. Redirects stop after 10.
- **A table of many files is one side**: a directory, a quoted glob, or an
  object storage prefix is its files read as one table, in path order:
  Spark and Hive output (`date=2026-10-01/part-00000.snappy.parquet`), a
  warehouse unload (`unload/0000_part_00.csv.gz`). CSV parts must share a
  header and Parquet parts a schema, and the error names the part that does
  not. From object storage the parts are fetched together, sixteen ranges in
  flight across all of them, and Parquet parts only as far as the comparison
  reads them. Hive partitions are columns: under `orders/`, the parts of
  `date=2026-10-01/region=eu/` carry `date` and `region` (as text), so
  `-k date,order_id` keys a table whose ids repeat from day to day, and a
  partitioned table compares with a flat export that has those columns.
  Names starting with `.` or `_` (`_SUCCESS`, `.crc`) are passed over.
- **A Delta table is read at a version**: `s3://lake/orders/` is its latest
  version, `s3://lake/orders@v41` the forty-first: the files its log
  (`_delta_log/`, checkpoint and commits) says make that version, and no
  others, so two versions compare like two tables:
  `compara compare s3://lake/orders@v41 s3://lake/orders -k order_id`.
  Deletion vectors and column mapping are refused rather than read wrong; an
  Iceberg table is refused.
- **Every range is read from one version of the object** (`If-Match` on its
  ETag): an object overwritten mid-read is an error, not a file stitched
  from two versions. Throttled and dropped ranges are retried.
- **Outputs go up** in one request, or as a multipart upload for a large
  export. `--json -` prints the summary on stdout and the counts on stderr.
