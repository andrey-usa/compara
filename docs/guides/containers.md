# Run compara in a container, in CI or in Kubernetes

- `ghcr.io/andrey-usa/compara` is the release binary on
  `gcr.io/distroless/cc` for `linux/amd64` and `linux/arm64`, running as a
  non-root user with `WORKDIR /data`. Nothing else is in the image.
- It reads the container's limits: threads from the CPU quota, memory from
  the cgroup. **Out of memory is a message, not an OOM kill**: the heap is held
  a margin under the limit, so a pair that does not fit is refused with what
  needed how much (exit 2), and under `--engine auto` a CSV pair is compared
  by the engine that spills to disk instead, with the same answer.
- A read-only root filesystem works; standard input, compressed and remote
  inputs and the spilling engine need a writable `TMPDIR`; in Kubernetes an
  `emptyDir` (disk-backed: a `medium: Memory` one counts against the memory
  limit).
- `compara env` prints what the process sees: CPUs, memory, the container's
  limit, the heap limit it set, and whether the temporary directory is
  writable.

In GitHub Actions, the action installs the release binary, writes the
counts and the most-changed columns to the job summary, and fails the step
when the tables differ (`fail-on-differences: false` to only report):

```yaml
- uses: aws-actions/configure-aws-credentials@v5   # or any other credentials
  with: {role-to-assume: arn:aws:iam::123456789012:role/ci, aws-region: us-east-1}
- uses: andrey-usa/compara@v0
  with:
    a: s3://lake/orders/baseline.parquet
    b: out/orders.parquet
    key: order_id,line_no
    args: -i updated_at --tolerance 0.01
    report: out/orders-diff.html
```

Its outputs are `identical`, `changed`, `added`, `removed` and `summary`
(the JSON summary's path).

A Kubernetes Job comparing two Parquet snapshots with the pod's IAM role
(IRSA), its summary written next to the data; without `--exit-zero` the Job
fails when they differ:

```yaml
apiVersion: batch/v1
kind: Job
metadata: {name: orders-diff}
spec:
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: lake-reader          # annotated with the IAM role
      restartPolicy: Never
      containers:
        - name: compara
          image: ghcr.io/andrey-usa/compara:latest
          args: [compare, s3://lake/orders/v1.parquet, s3://lake/orders/v2.parquet,
                 -k, order_id, --json, s3://reports/orders/summary.json, --exit-zero]
          resources: {limits: {memory: 2Gi, cpu: "4"}}
          securityContext: {readOnlyRootFilesystem: true, runAsNonRoot: true}
          volumeMounts: [{name: tmp, mountPath: /tmp}]
      volumes: [{name: tmp, emptyDir: {}}]
```
