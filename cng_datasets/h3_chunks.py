"""
H3 chunking shared by the raster and multidimensional (`mdim`) hex paths.

A build's unit of work is a chunk cell: an h0 base cell, or one of its res-N
descendants (issue #173). Everything that decides *which* chunks exist, *where*
a chunk is on the globe, and *where its output and completion marker go* lives
here, so that `raster`, `mdim`, the workflow generators and `merge-chunks`
cannot drift apart about any of it (#181).
"""

import math
from typing import List, Optional

import duckdb

from cng_datasets.storage.s3 import configure_s3_credentials

DEFAULT_H0_GRID_PATH = "s3://public-grids/hex/h0-valid.parquet"

# How far an H3 descendant can protrude beyond its ancestor's boundary polygon,
# as a fraction of the ancestor's latitude extent. H3's hierarchy is only
# approximately containing, so a chunk's own polygon does not bound the pixels
# its native cells actually cover (issue #173).
#
# Measured over cells sampled across the globe at chunk resolutions 1-3, for
# descendants 1 to 4 levels down: the protrusion *converges* rather than
# compounding — 0.127 at one level, 0.150 by three, unchanged at four. 0.25
# carries a ~1.7x safety factor over that worst case.
#
# Two callers want different sides of the trade. Pruning a chunk uses a
# deliberately looser multiple: a false positive there costs one pod that exits
# in seconds, so generosity is nearly free. A read window pays for its margin in
# bytes on every chunk, so it uses the measured bound directly.
H3_PROTRUSION_MARGIN = 0.25


def cell_footprint(geom_wkt: str, margin_deg: float = 0.0):
    """(lat_min, lat_max, [(lon_lo, lon_hi), ...]) for a cell polygon.

    Shared by the overlap test and the enumeration prune so the two cannot
    disagree about where a cell is — which, on the antimeridian, is the
    difference between pruning nothing and pruning the strip that holds the
    data (issue #88).

    Cell polygons are planar lat/lon, so a cell with vertices on both sides of
    +/-180 has a bounding box ~360 deg wide that both fails to prune anywhere
    AND wrongly excludes the +/-180 strip where its data lives. Unwrap the
    longitudes (negatives +360); a span > 180 deg means the cell straddles, so
    its longitude footprint is two intervals on [-180, 180]. Latitude is never
    wrapped, so the polygon's lat bounds are used directly.
    """
    from shapely import wkt as shapely_wkt
    poly = shapely_wkt.loads(geom_wkt)
    if poly.geom_type == "MultiPolygon":
        xs = [x for g in poly.geoms for x, _ in g.exterior.coords]
    else:
        xs = [x for x, _ in poly.exterior.coords]
    minx, miny, maxx, maxy = poly.bounds

    if maxx - minx > 180:  # straddles the antimeridian
        uxs = [x + 360.0 if x < 0 else x for x in xs]
        umin, umax = min(uxs), max(uxs)
        lon_intervals = [(umin, 180.0)]
        if umax > 180.0:
            lon_intervals.append((-180.0, umax - 360.0))
    else:
        lon_intervals = [(minx, maxx)]

    if margin_deg:
        return widen_footprint(miny, maxy, lon_intervals, margin_deg)
    return miny, maxy, lon_intervals


def widen_footprint(miny: float, maxy: float, lon_intervals, margin_deg: float):
    """Widen a footprint by a margin in degrees of latitude.

    Split out of `cell_footprint` because the enumeration prune derives a
    cell's bounds in SQL rather than from parsed WKT, and the two must widen
    them identically or the prune and the overlap test disagree about the same
    cell.
    """
    miny -= margin_deg
    maxy += margin_deg
    # A degree of longitude shrinks with latitude, so a margin fixed in
    # degrees of latitude under-covers near the poles. Scale it by
    # 1/cos(lat) at the cell's furthest-from-equator edge, capped so a
    # near-polar cell widens to the whole globe rather than overflowing.
    lat = min(max(abs(miny), abs(maxy)), 89.0)
    lon_margin = min(margin_deg / max(math.cos(math.radians(lat)), 1e-6), 180.0)
    return miny, maxy, [(lo - lon_margin, hi + lon_margin) for lo, hi in lon_intervals]


def enumerate_chunk_cells(chunk_resolution: int,
                          h0_subset: Optional[List[int]] = None,
                          h0_grid_path: str = DEFAULT_H0_GRID_PATH,
                          con=None):
    """Ordered [(chunk_cell, h0_cell, h0_index)] — the units of work for a build.

    Shared by `RasterProcessor` and the workflow generator on purpose. The
    generator sizes a Job's completions from this list and a pod indexes into
    it; if the two computed it separately they could disagree, and a fan-out
    narrower than the list drops whole chunks with nothing to show for it.

    Ordering is (h0 index, chunk cell id) — deterministic and independent of the
    order h3_cell_to_children happens to return, so an index maps to the same
    cell across regenerations and DuckDB versions.

    At chunk_resolution 0 this is the h0 grid itself, in grid order, so the
    index -> cell mapping is identical to the historical --h0-index.
    """
    if chunk_resolution < 0:
        raise ValueError(f"chunk_resolution must be >= 0, got {chunk_resolution}")

    own_con = con is None
    if own_con:
        con = duckdb.connect(":memory:")
        try:
            con.execute("LOAD h3")
        except duckdb.Error:
            con.execute("INSTALL h3 FROM community")
            con.execute("LOAD h3")
        configure_s3_credentials(con)

    try:
        where_sql = ""
        if h0_subset:
            where_sql = f"WHERE i IN ({', '.join(str(int(h)) for h in sorted(set(h0_subset)))})"

        base = con.execute(f"""
            SELECT i, h0
            FROM read_parquet('{h0_grid_path}')
            {where_sql}
            ORDER BY i
        """).fetchall()

        if chunk_resolution == 0:
            return [(int(h0), int(h0), int(i)) for i, h0 in base]

        # Expanded one h0 at a time with the cell as a literal. The obvious
        # single query — UNNEST(h3_cell_to_children(h0, N)) selected alongside
        # i and h0 over a parquet scan — is not merely slow, it aborts DuckDB
        # with "INTERNAL Error: Calling GetValueInternal on a value that is
        # NULL" and invalidates the connection, so every later query in the
        # process fails too. The literal form is the one already proven in
        # _native_cells_for_h0, and there are at most 122 of these.
        cells = []
        for i, h0 in base:
            children = con.execute(
                f"SELECT UNNEST(h3_cell_to_children({int(h0)}, {chunk_resolution}))"
            ).fetchall()
            cells.extend((int(child), int(h0), int(i)) for (child,) in children)
        cells.sort(key=lambda row: (row[2], row[0]))
        return cells
    finally:
        if own_con:
            con.close()


def chunk_output_path(output_base: str, chunk_resolution: int, chunk_cell: int,
                      h0_cell: Optional[int] = None) -> str:
    """Where one chunk's parquet goes.

    At chunk_resolution 0 the historical `h0={cell}/data_0.parquet` is
    preserved exactly — that literal path is published in STAC READMEs that
    users copy from, so it is not ours to change. Sub-chunks cannot share
    one filename, so they are written as siblings named by their own cell
    and merged back into `data_0.parquet` by `merge_raster_chunks`, which
    keeps the published layout identical either way.
    """
    if h0_cell is None:
        h0_cell = chunk_cell
    base = f"{output_base.rstrip('/')}/h0={h0_cell}"
    if chunk_resolution == 0:
        return f"{base}/data_0.parquet"
    return f"{base}/part-{chunk_cell}.parquet"


def chunk_manifest_path(output_base: str, chunk_index: int) -> str:
    """Where one chunk records that it ran.

    Beside the parts rather than inside them, because the fact worth
    recording is that the chunk *completed* — which is exactly the case a
    chunk with no data cannot express by writing a part file. `_manifest`
    does not match the `h0=*` glob the merge reads parts through, so the two
    never collide.
    """
    return f"{output_base.rstrip('/')}/_manifest/chunk-{chunk_index}.parquet"


def record_chunk_completion(con, output_base: str, chunk_index: int, chunk_cell: int,
                            h0_cell: int, wrote_data: bool) -> None:
    """Write one chunk's completion marker, which `merge-chunks` requires."""
    import os
    path = chunk_manifest_path(output_base, chunk_index)
    if not path.startswith(("s3://", "http://", "https://")):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT {int(chunk_index)}::BIGINT AS chunk_index,
                   {int(chunk_cell)}::UBIGINT AS chunk_cell,
                   {int(h0_cell)}::UBIGINT AS h0,
                   {"true" if wrote_data else "false"}::BOOLEAN AS wrote_data
        ) TO '{path}' (FORMAT PARQUET)
    """)
