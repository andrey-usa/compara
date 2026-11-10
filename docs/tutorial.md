# Everyday comparisons

```sh
compara compare a.csv b.csv -k account_id,txn_id -i updated_at
compara compare a.csv b.csv -k account_id,txn_id -i updated_at -o report.html
compara compare a.csv b.csv -k account_id,txn_id --trim --tolerance 0.005
compara compare export.csv warehouse.parquet -k id
compara compare a.csv b.csv --profile orders --json summary.json --export-dir out/
compara columns data.parquet          # column names (reads only the footer)
compara head data.ndjson -n 5         # first rows, aligned
```

`compare` prints the counts and writes nothing else unless asked. `-o PATH`
writes the HTML report: counts, per-column statistics, and the changed,
added, removed and duplicate rows, each section capped at `--max-rows`
(counts are always exact). [`examples/report.html`](../examples/report.html) is
one, made from the two files next to it:

```sh
compara compare examples/orders_2026-08.csv examples/orders_2026-09.csv -k order_id,line_no -i updated_at -o examples/report.html
```
