# Profiles and examples

## A named profile (`compara.example.toml`)

A recurring comparison gets a name in `compara.toml`; every field is
optional and the command line overrides the profile.

```toml
# Copy to compara.toml in the directory you run compara from (or to
# ~/.config/compara/compara.toml), or pass --config <path>, to give a recurring
# comparison a name:
#
#   compara compare data/p_a.csv data/p_b.csv --profile demo
#
# Every field is optional, and only the fields present override the defaults; the
# command line in turn overrides the profile.

[profiles.demo]
key    = ["account_id", "txn_id"]   # the key the generators write; required for any real pair
ignore = ["updated_at"]             # subtractive: only skip columns present in both files
trim   = true
tolerance = 0.005

[profiles.report]
key      = ["account_id", "txn_id"]
max_rows = 20000
engine   = "turbo"
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
order_id,line_no,sku,qty,unit_price,status,ship_date,region,updated_at
SO-00001,1,SKU109,13,197.03,open,2026-08-27,APAC,2026-08-31 10:00
SO-00001,2,SKU106,12,176.92,invoiced,2026-08-07,EMEA,2026-08-31 10:00
SO-00002,1,SKU127,14,25.61,open,2026-08-18,NA,2026-08-31 10:00
SO-00003,1,SKU136,4,284.50,invoiced,2026-08-21,APAC,2026-08-31 10:00
```

First rows of `orders_2026-09.csv` (note the extra `carrier` column and
the changed `status` values):

```csv
order_id,line_no,sku,qty,unit_price,status,ship_date,region,updated_at,carrier
SO-00001,1,SKU109,13,197.03,shipped,2026-08-27,APAC,2026-09-01 10:00,UPS
SO-00001,2,SKU106,12,176.92,invoiced,2026-08-07,EMEA,2026-09-01 10:00,UPS
SO-00002,1,SKU127,14,25.61,shipped,2026-08-18,NA,2026-09-01 10:00,UPS
SO-00003,1,SKU136,4,284.50,invoiced,2026-08-21,APAC,2026-09-01 10:00,UPS
```
