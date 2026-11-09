# Profiles and examples

## A named profile (`compara.example.toml`)

A recurring comparison gets a name in `compara.toml`; every field is
optional and the command line overrides the profile.

```toml
{{#include ../../compara.example.toml}}
```

## The shipped example pair

`examples/` in the repo holds two monthly order exports and the report
made from them (`examples/report.html`):

```sh
compara compare examples/orders_2026-08.csv examples/orders_2026-09.csv \
  -k order_id,line_no -i updated_at -o report.html
```

First rows of `orders_2026-08.csv`:

```csv
{{#include ../../examples/orders_2026-08.csv:1:5}}
```

First rows of `orders_2026-09.csv` (note the extra `carrier` column and
the changed `status` values):

```csv
{{#include ../../examples/orders_2026-09.csv:1:5}}
```
