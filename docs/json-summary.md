# The JSON summary

`compare --json PATH` writes the counts and per-column statistics of a
comparison as one JSON document (`--json -` prints it on standard output and
the counts on standard error). `counts` and `columns` are the same from
every engine on the same input; `meta` says how this run went. The shape, as
a JSON Schema:

```json
{{#include contract.schema.json}}
```
