# Bug report: `INTERNAL Error: Calling GetValueInternal on a value that is NULL`

**File against `isaacbrodsky/h3-duckdb`, not `duckdb/duckdb`.** The assertion is
raised inside DuckDB, but only the H3 community extension triggers it: the same
queries over core DuckDB functions are fine, and only 4 of the ~16 H3 functions
tested are affected. See *Scope* below for the evidence.

## Versions

| | |
|---|---|
| DuckDB | v1.5.4 (Python client 1.5.4) |
| `h3` extension | `be9ee91`, installed from the community repository |
| Platform | Linux x86_64 |

Not yet re-verified against a newer `h3` build — worth doing before filing.

## Reproduction A — one statement, no files

```sql
INSTALL h3 FROM community; LOAD h3;

SELECT h3_cell_to_children(h0, 0)
FROM (VALUES (577199624117288959::BIGINT), (NULL)) t(h0);
```

```
INTERNAL Error: Calling GetValueInternal on a value that is NULL
```

This is the already-reported trigger — issue **#136** reports the same assertion
for `h3_cell_to_latlng` on a column containing NULL.

## Reproduction B — no NULL anywhere

This is the part that is not reported, and it is why #136's title
("crashes on encountering NULL") is an incomplete diagnosis. There is no NULL in
this data:

```sql
INSTALL h3 FROM community; LOAD h3;

COPY (SELECT 577199624117288959::BIGINT AS h0 FROM range(5))
  TO 'h0.parquet' (FORMAT PARQUET);

SELECT h3_cell_to_children(h0, 0) FROM read_parquet('h0.parquet');
```

```
INTERNAL Error: Calling GetValueInternal on a value that is NULL
```

The trigger is that the column arrives **dictionary-encoded**. `range(5)` of a
repeated value is where DuckDB's Parquet writer switches from `PLAIN` to
`PLAIN_DICTIONARY`; `range(3)` stays `PLAIN` and the same query succeeds.

Two things are required and are easy to lose while minimising:

- **Read the Parquet file directly.** `CREATE TABLE t AS SELECT * FROM
  read_parquet('h0.parquet')` and then querying `t` flattens the vector, and the
  bug disappears.
- **No NULLs are involved.** The value is the same non-NULL cell id in all 5 rows.

## Controls

Row count is not the variable; encoding is. Holding one fixed and varying the
other (files written with `pyarrow.parquet.write_table`):

| rows | `use_dictionary` | encodings in file | result |
|---:|---|---|---|
| 5 | `True` | `PLAIN, RLE, RLE_DICTIONARY` | **INTERNAL Error** |
| 5 | `False` | `RLE, PLAIN` | ok |
| 1 | `True` | `PLAIN, RLE, RLE_DICTIONARY` | **INTERNAL Error** |

A single dictionary-encoded row is enough. Five plain-encoded rows are fine.

Also ruled out, each by removing it from a failing query and seeing it still fail:
`UNNEST` (not required), a CTE (not required), sibling columns in the projection
(not required), the `spatial` extension (not required), the H3 resolution
argument (fails at 0, 1, 2 and 3 alike), and the input column's type
(`BIGINT` and `UBIGINT` both fail).

## Scope

Same three inputs across the H3 functions we could call on a scalar cell column.
`PLAIN` is a 3-row Parquet file, `DICT` a 5-row one, `NULL` an in-memory table
containing one NULL:

| function | returns | PLAIN | DICT | NULL |
|---|---|---|---|---|
| `h3_cell_to_latlng` | `DOUBLE[]` | ok | **INTERNAL** | **INTERNAL** |
| `h3_cell_to_children` | `BIGINT[]` | ok | **INTERNAL** | **INTERNAL** |
| `h3_grid_disk` | `BIGINT[]` | ok | **INTERNAL** | **INTERNAL** |
| `h3_grid_ring_unsafe` | `BIGINT[]` | ok | **INTERNAL** | **INTERNAL** |
| `h3_cell_to_parent` | `BIGINT` | ok | ok | ok |
| `h3_cell_to_center_child` | `BIGINT` | ok | ok | ok |
| `h3_cell_to_boundary_wkt` | `VARCHAR` | ok | ok | ok |
| `h3_cell_to_lat` / `h3_cell_to_lng` | `DOUBLE` | ok | ok | ok |
| `h3_is_valid_cell` | `BOOLEAN` | ok | ok | ok |
| `h3_get_resolution` | `INTEGER` | ok | ok | ok |
| `h3_h3_to_string` | `VARCHAR` | ok | ok | ok |
| `h3_cell_area` | `DOUBLE` | ok | ok | ok |
| `h3_latlng_to_cell` | `BIGINT` | ok | ok | ok |
| `h3_are_neighbor_cells` | `BOOLEAN` | ok | ok | ok |
| `h3_compact_cells` | `BIGINT[]` | ok | ok | ok |

The four affected functions are exactly those that return a **list from a scalar
argument**. Every scalar-returning function is fine, and so is `h3_compact_cells`,
which returns a list but takes one — a different code path.

Core DuckDB functions over the same dictionary-encoded column are unaffected
(`h0 + 1`, `abs(h0)`, `list_value(h0)`, and `UNNEST(range(n))` all succeed), which
is what points at the extension rather than at DuckDB's Parquet reader.

## Relationship to existing issues

- **#136** (open) — `h3_cell_to_latlng crashes on encountering NULL`. Same
  assertion. Reproduction B suggests the NULL is one symptom rather than the
  cause, and that a fix targeting NULL handling alone would leave the
  dictionary-encoded case broken.
- **#120** (closed) — `h3_grid_ring_unsafe breaks when running on data set`.
  `h3_grid_ring_unsafe` is one of the four affected functions, and "breaks when
  running on a data set, but not on literals" is exactly the shape of this bug.
  Possibly the same root cause, closed without it being found.

## Why this matters in practice

Dictionary encoding is the default in every common Parquet writer and kicks in on
any column with repeated values — which is most real data. The failure therefore
appears when moving from a literal or a small test fixture to an actual file,
which is the point at which it is most confusing. It also **invalidates the
connection**: every subsequent query in the process fails with
`FATAL Error: Failed: database has been invalidated because of a previous fatal
error`, so in a long-running process one such call takes down everything after it.

We hit this in `boettiger-lab/datasets` building an H3 index from a Parquet grid
of cell ids. The workaround is to avoid passing a Parquet-read column straight
into one of the four functions — either materialise it first, or pass the value as
a literal:

```sql
-- works: value as a literal, one statement per row
SELECT UNNEST(h3_cell_to_children(577199624117288959, 1));
```

## Suspected cause

Speculative — we have not read the extension source. The two triggers have in
common that the input vector is not a plain flat vector: `DICTIONARY_VECTOR` in
one case, and a flat vector with a NULL in its validity mask in the other. A
`Vector::GetValue(i)` on such a vector returns a NULL `Value`, and
`Value::GetValueInternal` asserts on NULL. If the four list-returning functions
share a helper that reads arguments with `GetValue` rather than going through
`UnaryExecutor`/`ListVector` with a flattened or unified format, that would
explain both triggers and why only those four are affected.
