"""Post-build invariants for H3-indexed (hex) datasets.

These are schema-only assertions run after the hex write step (vector
repartition and raster hex) to fail fast on a corrupt build rather than
silently shipping a dataset that breaks downstream consumers.
"""

from typing import Callable, List, Tuple


def assert_h3_columns_unsigned(
    fetch: Callable[[str], List[Tuple]],
    hex_glob: str,
) -> None:
    """Assert every physical H3 index column ``h{N>=1}`` reads back as ``UBIGINT``.

    Native-cell columns (``h3_polygon_wkt_to_cells`` / ``h3_latlng_to_cell``)
    are always ``UBIGINT``, but ``h3_cell_to_parent`` changed its return type
    from signed ``BIGINT`` to ``UBIGINT`` in a newer h3 community-extension
    release. Because both the pipeline and the MCP do *unpinned*
    ``INSTALL h3 FROM community``, a dataset's parent-column sign would
    otherwise be a fossil of the extension version on its build date. We keep
    tracking the latest h3, so this is an assertion (fail the build) rather
    than a version pin — it also catches a bespoke ingest that casts the native
    cell column (e.g. a ``BIGINT`` native ``h6``).

    ``h0`` is exempt: it is the Hive partition key, so DuckDB infers its type
    from the directory string and always reads it back as signed ``BIGINT``
    regardless of the physical type. The convention is therefore: physical
    h-cols ``UBIGINT``; ``h0`` the signed hive key.

    The check is schema-only (``DESCRIBE``, no data scan) and is evaluated
    through the same path the consumer reads (the hive glob), so it sees the
    types consumers actually get.

    Args:
        fetch: Callable that runs a SQL string and returns the result rows as a
            list of tuples. Pass ``lambda sql: con.raw_sql(sql).fetchall()`` for
            an ibis DuckDB connection, or ``lambda sql: con.execute(sql).fetchall()``
            for a raw ``duckdb`` connection.
        hex_glob: A ``read_parquet``-compatible path/glob for the written hex
            output (e.g. ``s3://bucket/foo/hex/h0=*/data_0.parquet``).

    Raises:
        RuntimeError: if any ``h{N>=1}`` column is not ``UBIGINT``. See issue #102.
    """
    sql = (
        "SELECT column_name, column_type FROM "
        f"(DESCRIBE SELECT * FROM read_parquet('{hex_glob}')) "
        "WHERE column_name SIMILAR TO 'h[1-9][0-9]*' "
        "AND column_type <> 'UBIGINT'"
    )
    offenders = fetch(sql)
    if offenders:
        cols = ", ".join(f"{name} ({typ})" for name, typ in offenders)
        raise RuntimeError(
            "H3 index columns must be UBIGINT after the hex build "
            "(h0 is exempt as the signed hive key), but found non-UBIGINT "
            f"columns: {cols}. This usually means the h3 community extension "
            "emitted signed BIGINT parents (an older h3_cell_to_parent) or an "
            "ingest cast the native cell column. Rebuild with a current h3 "
            "extension or recast the offending columns to UBIGINT. See issue #102."
        )
