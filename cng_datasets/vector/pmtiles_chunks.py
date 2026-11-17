"""Cut a converted GeoParquet into row ranges for a chunked PMTiles build (#83).

The single-pod PMTiles step writes the whole dataset as GeoJSONSeq to local
disk and tiles it with one tippecanoe. At tens of millions of features the
intermediate outgrows the pod's ephemeral storage and tippecanoe runs for hours.
The chunked backend splits the work three ways:

* **parts**: an indexed Job. Pod *i* takes the *i*-th of *n* contiguous row
  ranges and tiles it at the high zooms only (``-Z split_zoom``). Convert writes
  rows in Hilbert order (#276), so a contiguous range is a compact patch of the
  map and the parts barely overlap at those zooms.
* **overview**: one pod tiles the low zooms (below ``split_zoom``) from the
  whole dataset, simplified to the overview's resolution. Low zooms are where
  every part would land in the same few tiles; built in one pass, tippecanoe
  sees all the features and drops the densest consistently.
* **merge**: ``tile-join -pk`` of the overview and the parts.

This module holds the piece the parts and the overview share: reading one row
range of the GeoParquet, optionally simplified, into a local parquet that
``ogr2ogr`` turns into tippecanoe's input.
"""

from typing import Optional, Tuple

#: First zoom the parts build; the overview builds every zoom below it. At z7 a
#: tile is about 2.8 degrees across, so the parts of a national dataset cut into
#: ~100 Hilbert ranges seldom share a tile, and a global overview at z0-z6 stays
#: small enough for one pod.
DEFAULT_SPLIT_ZOOM = 7

#: tippecanoe's default tile detail: 2**12 = 4096 units across a tile.
_TILE_DETAIL = 4096


def chunk_bounds(n_rows: int, n_chunks: int, index: int) -> Tuple[int, int]:
    """Half-open row range ``[start, stop)`` of chunk *index* of *n_chunks*.

    Ranges are as even as integer division allows, cover every row exactly
    once, and are empty only when there are more chunks than rows.
    """
    if n_chunks < 1:
        raise ValueError(f"n_chunks must be at least 1, got {n_chunks}")
    if not 0 <= index < n_chunks:
        raise ValueError(f"index {index} is outside 0..{n_chunks - 1}")
    return n_rows * index // n_chunks, n_rows * (index + 1) // n_chunks


def overview_tolerance(split_zoom: int) -> float:
    """Simplification tolerance in degrees for an overview built below *split_zoom*.

    Half a tile pixel at the overview's highest zoom (``split_zoom - 1``) at the
    equator. tippecanoe simplifies to a full pixel when it tiles, so this
    removes only vertices tippecanoe would drop anyway, but it does so before
    the features reach tippecanoe's temporary files, which is what keeps the
    overview pod's disk bounded.
    """
    if split_zoom < 1:
        raise ValueError(f"split_zoom must be at least 1, got {split_zoom}")
    return 360.0 / (2 ** (split_zoom - 1)) / _TILE_DETAIL / 2


def extract_chunk(input_url: str, output_path: str, n_chunks: int = 1, index: int = 0,
                  simplify: Optional[float] = None, verbose: bool = True) -> int:
    """Write row range *index* of *n_chunks* of *input_url* to *output_path*.

    Rows are selected by their position in the file (``file_row_number``),
    which DuckDB pushes down to skip row groups outside the range, so a pod
    reads only its own share of a large file. Row order is kept.

    Args:
        input_url: The converted GeoParquet (``s3://`` or a local path).
        output_path: Local parquet to write. Not written when the range is empty.
        n_chunks: How many ranges the file is cut into.
        index: Which range to write.
        simplify: If given, a tolerance in the geometry's units (degrees for
            the converted EPSG:4326 output) for ``ST_SimplifyPreserveTopology``.

    Returns:
        The number of rows written (0 for an empty range, with no file written).
    """
    from .h3_tiling import setup_duckdb_connection, find_geometry_column
    from ..storage.s3 import configure_s3_credentials

    con = setup_duckdb_connection()
    configure_s3_credentials(con)
    n_rows = int(con.execute(
        f"SELECT sum(num_rows) FROM parquet_file_metadata('{input_url}')").fetchall()[0][0] or 0)
    start, stop = chunk_bounds(n_rows, n_chunks, index)
    if verbose:
        print(f"Chunk {index} of {n_chunks}: rows {start:,}..{stop:,} of {n_rows:,}")
    if stop <= start:
        if verbose:
            print("  Empty range: nothing to tile.")
        return 0

    con.execute(f"""CREATE VIEW chunk_source AS
        SELECT * FROM read_parquet('{input_url}', file_row_number = true)
        WHERE file_row_number >= {start} AND file_row_number < {stop}""")
    geom = find_geometry_column(con, "chunk_source")
    if simplify is not None:
        select = (f'SELECT * EXCLUDE (file_row_number, "{geom}"), '
                  f'ST_SimplifyPreserveTopology("{geom}", {float(simplify)!r}) AS "{geom}" '
                  f'FROM chunk_source')
    else:
        select = "SELECT * EXCLUDE (file_row_number) FROM chunk_source"
    # Row order is kept by DuckDB's default preserve_insertion_order, so the
    # chunk stays in the file's Hilbert order without a sort.
    con.execute("SET preserve_insertion_order = true")
    written = con.execute(f"COPY ({select}) TO '{output_path}' (FORMAT PARQUET)").fetchall()
    rows = int(written[0][0]) if written else 0
    if rows != stop - start:
        raise RuntimeError(f"Expected {stop - start:,} rows in chunk {index}, wrote {rows:,}.")
    if verbose:
        note = f", simplified to {simplify:g}" if simplify is not None else ""
        print(f"  Wrote {rows:,} rows to {output_path}{note}")
    return rows
