"""
H3 hexagonal tiling utilities for vector datasets.

This module provides functions to convert polygon, point, and line
geometries into H3 hexagonal cells at specified resolutions, with
support for chunked processing of large datasets.
"""

from typing import Optional, List, Dict, Tuple
import duckdb
import os
from cng_datasets.storage.s3 import configure_s3_credentials
from cng_datasets.duckdb_memory import to_duckdb_memory_limit


# Line geometries are polyfilled as the H3 cells they *intersect* (issue #239).
# h3_polygon_wkb_to_cells only accepts polygons, so a line is first given a
# negligible buffer (~1 cm; 2% of an edge even at res 15) purely to make it a
# polygon, then filled with the 'overlap' containment mode, which returns every
# cell whose boundary meets the polygon. Because the sliver contains the line,
# every cell the line passes through is included, at any latitude and whatever
# the local cell size. A coarse ring of quadrant segments is enough for a
# sliver this thin and keeps the buffered vertex count (and polyfill time) low.
#
# The old approach — buffer by one average edge length *in degrees*, then
# center-containment — lost cells two ways: a degree of longitude is only
# cos(lat) x 111 km, so the east-west buffer shrank with latitude (half an edge
# at 60N); and H3 cell size varies ~2x over the globe, so even an exact
# edge-length buffer misses cells larger than average.
_LINE_BUFFER_DEG = 1e-7
_LINE_BUFFER_QUAD_SEGS = 1

# Pass 1 writes one H3 cell list per polygon piece, and a large polygon is cut
# into pieces first (see _PIECE_MAX_CELLS), so no list approaches the ~268M-cell
# (2 GB) ceiling of one Arrow/parquet value that used to cap a feature (#107).
# What still bounds one feature is the hex pod: all of its cells go through one
# chunk's Pass 2, and a billion rows is about what 50Gi of scratch holds there.
# A feature estimated past that is refused with a clear error. The estimate
# (spheroid area / average hex area) undercounts the true polyfill by ~1.3x.
# Override via the CNG_MAX_CELLS_PER_FEATURE environment variable.
_DEFAULT_MAX_CELLS_PER_FEATURE = 1_000_000_000

# A large polygon part is cut into pieces before polyfill (issue #125), and H3
# fills each piece on its own. Center containment makes this exact: a cell is in
# the fill iff its centre is inside the polygon, so the pieces' fills partition
# the whole fill. The cuts run through the middle of a piece's bbox, offset by
# _PIECE_CUT_JITTER degrees so they do not land on round coordinates, where cell
# centres could sit exactly on a cut.
#
# Two things are bounded. H3 tests every candidate centre against every edge,
# so a fill costs about (cells x vertices): a 200k-vertex polygon with 156k
# cells at res 7 took 565 s as one piece. A piece above _PIECE_MAX_WORK is split
# again, which divides both terms, while a piece in the interior is just a
# 4-vertex box. And a piece above _PIECE_MAX_CELLS is split so that each cell
# list stays small, however large the feature.
_PIECE_MAX_CELLS = 1_000_000
_PIECE_MAX_WORK = 100_000_000
_PIECE_MAX_DEPTH = 16
_PIECE_CUT_JITTER = 3.14159e-9


def parse_resolution_by_area(spec: str) -> List[Tuple[Optional[float], int]]:
    """Parse a ``--resolution-by-area`` spec into sorted (threshold, resolution) bins.

    The spec stratifies polygons by their planar ``ST_Area`` (deg²; ~12,000 km²
    per deg² at the equator) so very large features are hexed at a coarser native
    resolution — avoiding the Pass-2 OOM / 2 GB parquet-page limit that uniform
    fine resolution hits (issue #98). This is the proven "Plan A" used to build the
    published ``iucn-ranges-2025/hex`` product.

    Format: comma-separated ``threshold:resolution`` pairs plus a single trailing
    bare ``resolution`` that is the catch-all for features larger than every
    threshold. Example ``"12:8,600:6,5"`` means::

        area <= 12   -> res 8
        area <= 600  -> res 6
        otherwise    -> res 5   (the catch-all)

    Args:
        spec: The raw ``--resolution-by-area`` string.

    Returns:
        Bins as ``(threshold, resolution)`` tuples sorted ascending by threshold,
        terminated by exactly one catch-all ``(None, resolution)``.

    Raises:
        ValueError: malformed token, resolution out of [0, 15], no catch-all, or
            more than one catch-all.
    """
    tokens = [t.strip() for t in spec.split(',') if t.strip()]
    if not tokens:
        raise ValueError(
            "Empty --resolution-by-area spec. Expected e.g. '12:8,600:6,5' "
            "(threshold:resolution pairs plus a trailing catch-all resolution)."
        )

    bins: List[Tuple[float, int]] = []
    catchall: Optional[int] = None
    for tok in tokens:
        if ':' in tok:
            thresh_str, res_str = tok.split(':', 1)
            try:
                threshold = float(thresh_str)
                res = int(res_str)
            except ValueError:
                raise ValueError(
                    f"Invalid --resolution-by-area bin '{tok}'. Expected "
                    f"'threshold:resolution' with numeric values."
                )
            bins.append((threshold, res))
        else:
            if catchall is not None:
                raise ValueError(
                    f"--resolution-by-area has more than one catch-all resolution "
                    f"(bare integer without a threshold). Found '{tok}' after a "
                    f"catch-all was already set; only the final entry may be bare."
                )
            try:
                catchall = int(tok)
            except ValueError:
                raise ValueError(
                    f"Invalid --resolution-by-area catch-all '{tok}'. Expected a "
                    f"bare integer resolution (the final entry)."
                )

    if catchall is None:
        raise ValueError(
            "--resolution-by-area requires a trailing catch-all resolution (a bare "
            "integer with no threshold) for features larger than every threshold, "
            "e.g. the '5' in '12:8,600:6,5'."
        )

    for _, res in bins:
        if not 0 <= res <= 15:
            raise ValueError(f"H3 resolution {res} out of range [0, 15] in --resolution-by-area.")
    if not 0 <= catchall <= 15:
        raise ValueError(f"H3 catch-all resolution {catchall} out of range [0, 15].")

    bins.sort(key=lambda b: b[0])
    for (t_prev, _), (t_cur, _) in zip(bins, bins[1:]):
        if t_cur == t_prev:
            raise ValueError(
                f"--resolution-by-area has a duplicate threshold {t_cur}; the later "
                f"bin would be unreachable. Use distinct, increasing thresholds."
            )
    return [(t, r) for t, r in bins] + [(None, catchall)]


def _native_res_case_sql(bins: List[Tuple[Optional[float], int]], geom_expr: str) -> str:
    """Build a SQL CASE expression mapping a geometry's planar area to its native
    H3 resolution, given parsed ``--resolution-by-area`` bins.

    The catch-all (``threshold is None``) becomes the ELSE branch. An
    antimeridian crosser is binned by its short-way area (issue #241).
    """
    area = _planar_area_deg2_sql(geom_expr)
    whens = [
        f"WHEN {area} <= {threshold} THEN {res}"
        for threshold, res in bins
        if threshold is not None
    ]
    catchall = next(res for threshold, res in bins if threshold is None)
    return "CASE " + " ".join(whens) + f" ELSE {catchall} END"


# H3 polygon_to_cells interprets a ring whose exterior spans more than 180 deg of
# longitude as the minimal-area (complement) side, so a circumpolar / transmeridian
# polygon fills ~nothing — e.g. a full -180..180 band yields ~1 cell instead of the
# millions its area implies (issue #145). Before polyfill we split any polygon whose
# longitude-bbox span exceeds 180 deg into sub-180-deg longitude bands, intersect
# the polygon with each, and polyfill the pieces. Each band must be strictly < 180
# deg wide (a 180-deg band is itself ambiguous and also fills ~nothing); four 90-deg
# strips tile the whole -180..180 range with margin to spare.
_TRANSMERIDIAN_MAX_SPAN_DEG = 180
_TRANSMERIDIAN_BANDS = [(-180, -90), (-90, 0), (0, 90), (90, 180)]
#
# A wide planar bbox alone does NOT mean the feature is circumpolar, though. A
# feature that merely *crosses* the antimeridian (an Aleutian island, a North
# Pacific RFMO area) also has a ~359-deg planar bbox, but there H3's own
# transmeridian handling is correct, and band-splitting is harmful: the pieces
# cover the wrong-way ~359-deg region, so the fill wraps the globe (issue #241).
# See _dateline_crosser_sql for how the two are told apart.

# Square of the equatorial metres-per-degree, for converting a planar ST_Area
# (deg²) to an approximate area in m². Over-estimates away from the equator
# (longitude degrees shrink with latitude), which is the safe direction for the
# oversized-feature guard below.
_DEG2_TO_M2 = 111_320.0 ** 2


def _lon_gap_sql(geom_expr: str) -> str:
    """SQL for the vertex-free longitude gap that spans the prime meridian.

    For longitudes in [-180, 180], any gap wider than 180 deg must contain 0:
    if the gap is (a, b) with b - a > 180 and -180 <= a < b <= 180, then
    a < b - 180 <= 0 and b > a + 180 >= 0. So min(lon >= 0) - max(lon < 0) is
    the only gap that can exceed 180, and it needs only list aggregates. NULL
    when every vertex is on one side of 0.

    Vertices exactly on +-180 are ignored. Read the short way (as H3 does), an
    edge between -180 and 180 has zero length, so they are no evidence of a
    crossing. They are how a full circumpolar band is usually written, e.g.
    POLYGON((-180 -78, 180 -78, 180 -50, -180 -50, -180 -78)), whose remaining
    vertex set is empty (NULL gap), so it is still band-split.
    """
    lons = f"list_transform(ST_Dump(ST_Points({geom_expr})), lambda s: ST_X(s.geom))"
    return (
        f"(list_min(list_filter({lons}, lambda x: x >= 0 AND x < 180)) "
        f"- list_max(list_filter({lons}, lambda x: x < 0 AND x > -180)))"
    )


def _dateline_crosser_sql(geom_expr: str) -> str:
    """SQL predicate: is this a narrow antimeridian *crosser* rather than a
    genuinely circumpolar feature? Never NULL.

    Both have a > 180-deg planar longitude bbox, so the bbox cannot separate
    them; the vertex distribution can. A crosser's vertices cluster near +-180
    and leave a gap wider than 180 deg the long way round (see _lon_gap_sql); a
    circumpolar band's vertices span all longitudes and leave no such gap.

    Checked against ``public-high-seas/rfmo/rfb`` (the #145 layer): CCAMLR, IWC,
    ACAP and CCSBT are circumpolar and still split; IPHC (288-deg gap) and APFIC
    (237-deg gap) are crossers and left to H3. The vertex list is built only for
    rows already wider than the span threshold, since DuckDB evaluates a CASE
    branch on just the rows that select it.
    """
    return f"""COALESCE(
        CASE WHEN ST_XMax({geom_expr}) - ST_XMin({geom_expr}) > {_TRANSMERIDIAN_MAX_SPAN_DEG}
             THEN {_lon_gap_sql(geom_expr)} > {_TRANSMERIDIAN_MAX_SPAN_DEG}
             ELSE FALSE
        END, FALSE)"""


def _planar_area_deg2_sql(geom_expr: str) -> str:
    """SQL for a feature's planar area in deg², measured the short way round
    for an antimeridian crosser.

    Planar ``ST_Area`` of a crossing ring is the wrong-way region, orders of
    magnitude too large. A multipolygon whose parts sit either side of the
    dateline without crossing it is a crosser too, but there ``ST_Area`` is
    exact. So a crosser takes the lesser of ``ST_Area`` and its short-way bbox
    (360 - gap deg wide), which bounds its true planar area from above.
    """
    short_way_bbox = (
        f"(360 - {_lon_gap_sql(geom_expr)}) * (ST_YMax({geom_expr}) - ST_YMin({geom_expr}))"
    )
    return (
        f"(CASE WHEN {_dateline_crosser_sql(geom_expr)} "
        f"THEN LEAST(ST_Area({geom_expr}), {short_way_bbox}) "
        f"ELSE ST_Area({geom_expr}) END)"
    )


def _spheroid_area_m2_sql(geom_expr: str) -> str:
    """Geodesic area in m² of a (lon, lat) geometry.

    Every geometry here is (lon, lat): convert writes it that way with
    ``always_xy``, and connections set ``geometry_always_xy`` so
    ``ST_Area_Spheroid`` reads it that way too. Before DuckDB 2.0 the function
    read (latitude, longitude), and the input was flipped here: unflipped, a
    feature beyond ±90° longitude has an out-of-range "latitude" and measures
    NaN, and NaN sorts above every number in DuckDB, so a single such feature
    hid the chunk's true worst feature from the #107 guard (issue #253).
    """
    return f"ST_Area_Spheroid({geom_expr})"


def _feature_area_m2_sql(geom_expr: str) -> str:
    """Area in m² that the polyfill of *geom_expr* walks.

    Geodesic normally; planar (the short way for antimeridian crossers) for a
    feature spanning more than 180 degrees of longitude, whose spheroid area is
    NaN or unrelated to what the fill covers (#167, #241).
    """
    return (
        f"CASE WHEN ST_XMax({geom_expr}) - ST_XMin({geom_expr}) > {_TRANSMERIDIAN_MAX_SPAN_DEG} "
        f"THEN {_planar_area_deg2_sql(geom_expr)} * {_DEG2_TO_M2} "
        f"ELSE {_spheroid_area_m2_sql(geom_expr)} END"
    )


def _native_res_sql(geom_expr: str, h3_resolution: int,
                    resolution_by_area: Optional[List[Tuple[Optional[float], int]]]) -> str:
    """The resolution *geom_expr* is hexed at: fixed, or by area (#98)."""
    if resolution_by_area is not None:
        return _native_res_case_sql(resolution_by_area, geom_expr)
    return str(int(h3_resolution))


def _polygon_cells_sql(geom_expr: str, h3_resolution: int,
                       resolution_by_area: Optional[List[Tuple[Optional[float], int]]]) -> str:
    """Estimated H3 cells in the polyfill of a polygon: area / average hex area.

    The estimate undercounts the true polyfill by ~1.3x (see
    _DEFAULT_MAX_CELLS_PER_FEATURE); it is shared by the #107 guard and the
    #124 chunk planner so the two cannot disagree about a feature's size.
    """
    res = _native_res_sql(geom_expr, h3_resolution, resolution_by_area)
    return f"({_feature_area_m2_sql(geom_expr)}) / h3_get_hexagon_area_avg({res}, 'm^2')"


def _representative_point_sql(geom_expr: str) -> str:
    """SQL for a point on the feature, for the sub-cell fallback (issue #104).

    ST_PointOnSurface reads an antimeridian crosser planarly, as the wrong-way
    band, and lands far from the feature; use one of its own vertices instead.
    """
    return (
        f"(CASE WHEN {_dateline_crosser_sql(geom_expr)} "
        f"THEN ST_Dump(ST_Points({geom_expr}))[1].geom "
        f"ELSE ST_PointOnSurface({geom_expr}) END)"
    )


def _transmeridian_split_sql(carry_cols: str, source: str) -> str:
    """SQL for a CTE body that splits >180-deg-longitude-span polygons into bands.

    Rows from ``source`` whose longitude bbox span is <= 180 deg pass through
    unchanged (the common fast path); wider rows are cross-joined with the
    longitude bands and intersected, so downstream ST_Dump + H3 polyfill only ever
    see sub-180-deg pieces. The intersection may yield a MULTIPOLYGON or
    GEOMETRYCOLLECTION, which the existing ST_Dump step already splits into single
    geometries before polyfill.

    Antimeridian crossers are not split but pass through unchanged, because H3
    polyfills them correctly and the split does not (issue #241; see
    _dateline_crosser_sql). NULL geometries are dropped, as before.

    Args:
        carry_cols: comma-separated non-geometry columns to propagate (e.g. the id
            column, plus ``native_res`` in variable-resolution mode).
        source: name of the upstream CTE exposing those columns and a ``geom`` column.
    """
    bands = ', '.join(
        f"(ST_GeomFromText('POLYGON(({lo} -90, {hi} -90, {hi} 90, {lo} 90, {lo} -90))'))"
        for lo, hi in _TRANSMERIDIAN_BANDS
    )
    needs_split = (
        f"ST_XMax(geom) - ST_XMin(geom) > {_TRANSMERIDIAN_MAX_SPAN_DEG} "
        f"AND NOT {_dateline_crosser_sql('geom')}"
    )
    return f'''
            SELECT {carry_cols}, geom
            FROM {source}
            WHERE NOT ({needs_split})
            UNION ALL
            SELECT {carry_cols}, ST_Intersection(s.geom, _bands.band) AS geom
            FROM (SELECT {carry_cols}, geom FROM {source} WHERE {needs_split}) AS s,
                 (VALUES {bands}) AS _bands(band)
            WHERE ST_Intersects(s.geom, _bands.band)
    '''


def _needs_split_sql(geom_expr: str, res: str) -> str:
    """SQL predicate: is this polygon too large or too costly to fill as one
    piece? Never true for an area that is not finite (see _PIECE_MAX_CELLS)."""
    cells = f"(({_feature_area_m2_sql(geom_expr)}) / h3_get_hexagon_area_avg({res}, 'm^2'))"
    return (
        f"(isfinite({cells}) AND ({cells} > {_PIECE_MAX_CELLS} "
        f"OR {cells} * ST_NPoints({geom_expr}) > {_PIECE_MAX_WORK}))"
    )


def _piece_split_sql(carry_cols: str, source: str, res: str) -> str:
    """SQL for CTE bodies that cut large polygon parts into pieces (issue #125).

    Returns the bodies of four CTEs, ``tbig``, ``tcross``, ``twrap`` and
    ``tpieces``, to be defined in that order under ``WITH RECURSIVE``; the
    leaves are the rows of ``tpieces`` with ``NOT _split``. Each row carries
    ``_depth``, which is 0 for a part that was not cut.

    Only valid polygon parts are cut, because clipping an invalid one can
    throw; lines (already ~1 cm slivers) and invalid parts are filled whole, as
    before. A large antimeridian crosser is first read in a 0..360 longitude
    frame and cut at 180, and its eastern half is moved back by -360, so that it
    becomes two ordinary parts that the bbox cuts below can handle. Its validity
    is judged in that frame, since the ring as stored crosses itself planarly.
    """
    split = _needs_split_sql("geom", res)
    j = _PIECE_CUT_JITTER
    mx = f"(ST_XMin(geom) + ST_XMax(geom)) / 2 + {j}"
    my = f"(ST_YMin(geom) + ST_YMax(geom)) / 2 + {j}"
    quadrants = (
        f"[ST_MakeEnvelope(ST_XMin(geom), ST_YMin(geom), {mx}, {my}), "
        f"ST_MakeEnvelope({mx}, ST_YMin(geom), ST_XMax(geom), {my}), "
        f"ST_MakeEnvelope(ST_XMin(geom), {my}, {mx}, ST_YMax(geom)), "
        f"ST_MakeEnvelope({mx}, {my}, ST_XMax(geom), ST_YMax(geom))]"
    )
    wgs84 = "+proj=longlat +datum=WGS84 +no_defs"
    crosser = _dateline_crosser_sql("geom")
    tbig = f"""
            SELECT {carry_cols}, geom,
                   CASE WHEN NOT _is_line AND ST_GeometryType(geom) = 'POLYGON'
                        THEN {split} ELSE FALSE END AS _split
            FROM {source}
    """
    tcross = f"""
            SELECT {carry_cols}, geom,
                   ST_Transform(geom, '{wgs84}', '{wgs84} +lon_wrap=180',
                                always_xy := true) AS _wrapped
            FROM tbig WHERE _split AND {crosser}
    """
    twrap = f"""
            SELECT {carry_cols}, geom,
                   CASE WHEN _split THEN ST_IsValid(geom) ELSE FALSE END AS _split
            FROM tbig WHERE NOT (_split AND {crosser})
            UNION ALL
            SELECT {carry_cols},
                   CASE WHEN ST_XMin(geom) >= 180 THEN ST_Translate(geom, -360, 0)
                        ELSE geom END AS geom,
                   TRUE AS _split
            FROM (
                SELECT {carry_cols},
                       UNNEST(ST_Dump(ST_Intersection(_wrapped, _half))).geom AS geom
                FROM tcross,
                     (VALUES (ST_MakeEnvelope(0, -90, 180, 90)),
                             (ST_MakeEnvelope(180, -90, 360, 90))) AS _halves(_half)
                WHERE ST_IsValid(_wrapped)
            )
            WHERE ST_GeometryType(geom) = 'POLYGON' AND NOT ST_IsEmpty(geom)
            UNION ALL
            SELECT {carry_cols}, geom, FALSE AS _split
            FROM tcross WHERE NOT ST_IsValid(_wrapped)
    """
    tpieces = f"""
            SELECT {carry_cols}, geom, 0 AS _depth,
                   CASE WHEN _split THEN {split} ELSE FALSE END AS _split
            FROM twrap
            UNION ALL
            SELECT {carry_cols}, geom, _depth,
                   _depth < {_PIECE_MAX_DEPTH} AND {split} AS _split
            FROM (
                SELECT {carry_cols}, _depth + 1 AS _depth,
                       UNNEST(ST_Dump(ST_Intersection(geom, _quad))).geom AS geom
                FROM (
                    SELECT {carry_cols}, _depth, geom, UNNEST({quadrants}) AS _quad
                    FROM tpieces WHERE _split
                )
            )
            WHERE ST_GeometryType(geom) = 'POLYGON' AND NOT ST_IsEmpty(geom)
    """
    return tbig, tcross, twrap, tpieces


def identify_id_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    specified_id_col: Optional[str] = None,
    check_uniqueness: bool = True,
) -> Tuple[str, bool]:
    """
    Identify or validate an ID column in a table.

    Uses case-insensitive matching to find common ID column names, or validates
    a user-specified column. Optionally checks for uniqueness.

    Prioritizes _cng_fid as the standard synthetic ID column created by convert_to_parquet.

    Args:
        con: DuckDB connection
        table_name: Name of the table or view to check
        specified_id_col: User-specified ID column name (if provided)
        check_uniqueness: Whether to validate that the ID column is unique

    Returns:
        Tuple of (column_name, is_unique) where:
        - column_name: Actual column name in the table (preserving case)
        - is_unique: True if column is unique (or check was skipped)

    Raises:
        ValueError: If specified column not found or uniqueness check fails
    """
    # Get all column names
    result = con.execute(f"SELECT * FROM {table_name} LIMIT 0").description
    all_columns = [col[0] for col in result]

    # If user specified a column, validate it exists
    if specified_id_col:
        # Case-insensitive search
        col_lower_map = {col.lower(): col for col in all_columns}
        actual_col = col_lower_map.get(specified_id_col.lower())

        if not actual_col:
            raise ValueError(f"Specified ID column '{specified_id_col}' not found in table. Available columns: {', '.join(all_columns)}")

        id_col = actual_col
    else:
        # Auto-detect common ID column names (case-insensitive)
        # _cng_fid is our standard synthetic ID from convert_to_parquet
        col_lower_map = {col.lower(): col for col in all_columns}
        common_id_names = ['_cng_fid', 'fid', 'objectid', 'id', 'uid', 'gid', 'ogc_fid']

        id_col = None
        for name in common_id_names:
            if name in col_lower_map:
                id_col = col_lower_map[name]
                break

        if not id_col:
            # No ID column found
            return None, False

    # Check uniqueness if requested
    is_unique = True
    if check_uniqueness:
        total_count = con.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]
        unique_count = con.execute(f'SELECT COUNT(DISTINCT "{id_col}") FROM {table_name}').fetchone()[0]

        is_unique = (total_count == unique_count)

        if not is_unique:
            warning_msg = f"Warning: ID column '{id_col}' has {total_count} rows but only {unique_count} unique values"
            if specified_id_col:
                # User specified it, so this is an error
                raise ValueError(warning_msg)
            else:
                # Auto-detected, so just warn and return
                print(f"  {warning_msg}")

    return id_col, is_unique


def geom_to_h3_cells(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int = 10,
    keep_cols: Optional[List[str]] = None,
    geom_col: str = "geom",
    resolution_by_area: Optional[List[Tuple[Optional[float], int]]] = None,
) -> str:
    """
    Convert geometries to H3 cells at specified resolution.

    Args:
        con: DuckDB connection
        table_name: Name of the table or view with geometry column
        zoom: H3 resolution level (0-15). Used when resolution_by_area is None.
        keep_cols: List of columns to keep from input. If None, keeps all except geom.
        geom_col: Name of the geometry column
        resolution_by_area: Optional parsed bins from parse_resolution_by_area. When
            provided, each feature is hexed at the native resolution its planar
            ST_Area maps to (issue #98), and a per-feature ``native_res`` column is
            emitted alongside ``h3id``. Points/lines have ~0 planar area and so fall
            in the finest (first) bin. When None, every feature is hexed at ``zoom``.

    Returns:
        SQL query string that generates H3 cells
    """
    # Get column names if not specified
    if keep_cols is None:
        cols_query = f"SELECT * FROM {table_name} LIMIT 0"
        keep_cols = [col for col in con.execute(cols_query).description if col[0] != geom_col]
        keep_cols = [col[0] for col in keep_cols]

    # Build column list for SELECT statements, quoting column names to handle spaces
    col_list = ', '.join([f'"{col}"' for col in keep_cols])

    # Detect geometry types present in the table
    type_counts = con.execute(f"""
        SELECT ST_GeometryType({geom_col}) AS gtype, COUNT(*) AS cnt
        FROM {table_name}
        GROUP BY gtype
    """).fetchall()
    type_map = {row[0]: row[1] for row in type_counts}

    point_count = sum(type_map.get(t, 0) for t in ('POINT', 'MULTIPOINT'))
    line_count = sum(type_map.get(t, 0) for t in ('LINESTRING', 'MULTILINESTRING'))

    if point_count > 0:
        print(
            f"  Warning: {point_count} point/multipoint geometries detected. "
            f"Each point will be resolved to a single H3 cell at resolution {zoom}. "
            "Sub-cell coordinate precision is lost, and nearby points at coarse "
            "resolutions may map to the same cell. Document this in the STAC metadata."
        )

    # Line geometries become a ~1 cm sliver polygon and are filled with 'overlap'
    # containment so every cell the line passes through is kept (issue #239; see
    # _LINE_BUFFER_DEG). `_is_line` carries that choice past the buffer, transmeridian
    # split and ST_Dump, after which the geometry type alone no longer tells.
    #
    # Polygon parts are filled from WKB, which skips a WKT text round trip. The
    # WKB functions raise on anything that is not a POLYGON, where the WKT ones
    # returned an empty list, so a part that is not a polygon (a line left in a
    # GEOMETRYCOLLECTION by ST_Dump) gets that empty list explicitly and falls
    # through to the representative-cell fallback exactly as before.
    line_buffer = f"{_LINE_BUFFER_DEG}, {_LINE_BUFFER_QUAD_SEGS}"
    is_line = "ST_GeometryType({g}) IN ('LINESTRING', 'MULTILINESTRING')"

    def polyfill(res: str) -> str:
        return f"""CASE
                       WHEN ST_GeometryType(geom) = 'POINT'
                       THEN [h3_latlng_to_cell(ST_Y(geom), ST_X(geom), {res})]
                       WHEN ST_GeometryType(geom) <> 'POLYGON'
                       THEN []::UBIGINT[]
                       WHEN _is_line
                       THEN h3_polygon_wkb_to_cells_experimental(
                                ST_AsWKB(ST_Force2D(geom)), {res}, 'overlap')
                       ELSE h3_polygon_wkb_to_cells(ST_AsWKB(ST_Force2D(geom)), {res})
                   END"""

    if line_count > 0:
        print(
            f"  Line geometries detected ({line_count} features). "
            f"Indexing every H3 cell at res {zoom} that each line passes through."
        )

    # Variable-resolution (issue #98): when resolution_by_area is provided, each
    # feature is hexed at the native resolution its planar ST_Area maps to, a
    # per-feature `native_res` column is emitted, and the H3 functions take that
    # column (not a literal zoom) as the resolution argument — DuckDB's H3
    # bindings accept a per-row resolution expression. Otherwise the
    # original single-`zoom` path below is used unchanged.
    if resolution_by_area is not None:
        native_res_case = _native_res_case_sql(resolution_by_area, geom_col)
        tbig_ba, tcross_ba, twrap_ba, tpieces_ba = _piece_split_sql(
            f'{col_list}, native_res, _is_line', 't1', 'native_res')
        return f'''
        WITH RECURSIVE tbase AS (
            SELECT {col_list},
                   {geom_col} AS _geom_orig,
                   {native_res_case} AS native_res
            FROM {table_name}
        ),
        t0 AS (
            SELECT {col_list}, native_res,
                   {is_line.format(g='_geom_orig')} AS _is_line,
                   CASE
                       WHEN {is_line.format(g='_geom_orig')}
                       THEN ST_Multi(ST_Buffer(ST_Force2D(_geom_orig), {line_buffer}))
                       WHEN ST_GeometryType(_geom_orig) = 'POLYGON'
                       THEN ST_Multi(_geom_orig)
                       ELSE _geom_orig
                   END AS geom
            FROM tbase
        ),
        tsplit AS ({_transmeridian_split_sql(f'{col_list}, native_res, _is_line', 't0')}),
        t1 AS (
            SELECT {col_list}, native_res, _is_line,
                   UNNEST(ST_Dump(geom)).geom AS geom
            FROM tsplit
        ),
        tbig AS ({tbig_ba}),
        tcross AS ({tcross_ba}),
        twrap AS ({twrap_ba}),
        tpieces AS ({tpieces_ba}),
        t2 AS (
            SELECT {col_list}, native_res, geom, _depth,
                   {polyfill('native_res')} AS h3id
            FROM tpieces WHERE NOT _split
        )
        SELECT {col_list}, native_res,
               CASE
                   WHEN h3id IS NOT NULL AND len(h3id) > 0 THEN h3id
                   WHEN _depth = 0 AND ST_YMin(geom) >= -90 AND ST_YMax(geom) <= 90
                   THEN [h3_latlng_to_cell(
                            ST_Y({_representative_point_sql('geom')}),
                            ST_X({_representative_point_sql('geom')}),
                            native_res)]
                   ELSE h3id
               END AS h3id
        FROM t2
        WHERE _depth = 0 OR len(h3id) > 0
        '''

    # Convert to multi-polygons and unnest, then generate H3 cells
    # The geometry is already GEOMETRY type in DuckDB spatial extension
    # Line geometries are buffered into sliver polygons before polyfill.
    #
    # Any polygon smaller than one H3 cell at this resolution contains no cell
    # centre, so h3_polygon_wkb_to_cells returns an empty array and the feature
    # would vanish from the output entirely; a chunk in which every feature is
    # sub-cell produces no rows at all (issue #104). When the polyfill is empty
    # we fall back to the single cell containing the feature's ST_PointOnSurface
    # — a guaranteed-interior representative point — so every feature yields
    # >= 1 cell regardless of its size. The fallback is gated on a valid latitude
    # range: a polygon with empty polyfill AND out-of-range Y is left empty on
    # purpose so the downstream swapped-(lat,lon) check still raises a clear
    # error rather than feeding an invalid latitude into h3_latlng_to_cell.
    #
    # A large polygon part is cut into pieces that are filled separately (issue
    # #125; see _PIECE_MAX_CELLS). The fallback above applies to a part as a
    # whole, so a piece of a cut part never takes it, and a piece that holds no
    # cell centre is dropped rather than kept as an empty list.
    tbig, tcross, twrap, tpieces = _piece_split_sql(f'{col_list}, _is_line', 't1', str(zoom))
    sql = f'''
        WITH RECURSIVE t0 AS (
            SELECT {col_list},
                   {is_line.format(g=geom_col)} AS _is_line,
                   CASE
                       WHEN {is_line.format(g=geom_col)}
                       THEN ST_Multi(ST_Buffer(ST_Force2D({geom_col}), {line_buffer}))
                       WHEN ST_GeometryType({geom_col}) = 'POLYGON'
                       THEN ST_Multi({geom_col})
                       ELSE {geom_col}
                   END AS geom
            FROM {table_name}
        ),
        tsplit AS ({_transmeridian_split_sql(f'{col_list}, _is_line', 't0')}),
        t1 AS (
            SELECT {col_list}, _is_line,
                   UNNEST(ST_Dump(geom)).geom AS geom
            FROM tsplit
        ),
        tbig AS ({tbig}),
        tcross AS ({tcross}),
        twrap AS ({twrap}),
        tpieces AS ({tpieces}),
        t2 AS (
            SELECT {col_list},
                   geom, _depth,
                   {polyfill(str(zoom))} AS h3id
            FROM tpieces WHERE NOT _split
        )
        SELECT {col_list},
               CASE
                   WHEN h3id IS NOT NULL AND len(h3id) > 0 THEN h3id
                   WHEN _depth = 0 AND ST_YMin(geom) >= -90 AND ST_YMax(geom) <= 90
                   THEN [h3_latlng_to_cell(
                            ST_Y({_representative_point_sql('geom')}),
                            ST_X({_representative_point_sql('geom')}),
                            {zoom})]
                   ELSE h3id
               END AS h3id
        FROM t2
        WHERE _depth = 0 OR len(h3id) > 0
    '''

    return sql


def find_geometry_column(con: duckdb.DuckDBPyConnection, table_name: str) -> str:
    """Find the geometry column in the table.

    Resolves the geometry column by DuckDB *type* first: a column whose type
    is GEOMETRY is the geometry, regardless of its name. This keeps an
    unrelated attribute column that merely happens to be named
    ``GEOMETRY``/``SHAPE``/``GEOM`` (e.g. a DOUBLE carried over from a source
    DBF, issue #171) from shadowing the real geometry column and aborting the
    hex job with an ``ST_GeometryType(DOUBLE)`` binder error. Name matching is
    used only as a fallback when no GEOMETRY-typed column exists (e.g. WKB
    stored as BLOB).
    """
    schema = con.execute(f"DESCRIBE {table_name}").fetchall()
    # DESCRIBE rows are (column_name, column_type, ...). Geometry columns are
    # reported as GEOMETRY (optionally with a CRS annotation, e.g.
    # "GEOMETRY('OGC:CRS84')"), so match on the type prefix.
    geom_typed = [
        row[0] for row in schema if str(row[1]).upper().startswith('GEOMETRY')
    ]
    if geom_typed:
        # If several geometry-typed columns exist, prefer conventional names;
        # otherwise take the first geometry-typed column.
        by_upper = {col.upper(): col for col in geom_typed}
        for name in ['GEOM', 'GEOMETRY', 'SHAPE']:
            if name in by_upper:
                return by_upper[name]
        return geom_typed[0]

    # Fallback: no GEOMETRY-typed column (e.g. WKB blobs) — match by name.
    columns = [row[0] for row in schema]
    for col in columns:
        if col.upper() in ['SHAPE', 'GEOMETRY', 'GEOM']:
            return col
    raise ValueError(f"No geometry column found. Available columns: {columns}")


def setup_duckdb_connection(
    extensions: Optional[List[str]] = None,
    http_retries: int = 20,
    http_retry_wait_ms: int = 5000,
) -> duckdb.DuckDBPyConnection:
    """
    Set up a DuckDB connection with required extensions.

    Args:
        extensions: List of DuckDB extensions to load. Defaults to ["spatial"]
        http_retries: Number of HTTP retries for remote files
        http_retry_wait_ms: Wait time between retries in milliseconds

    Returns:
        Configured DuckDB connection
    """
    if extensions is None:
        extensions = ["spatial"]

    con = duckdb.connect()

    # Install and load extensions
    for ext in extensions:
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    if "spatial" in extensions:
        # Geometries are (lon, lat) throughout; DuckDB 2.0 refuses axis-order
        # sensitive functions until this is set (#280).
        con.execute("SET geometry_always_xy = true")

    # Install h3 from community repository
    con.execute("INSTALL h3 FROM community")
    con.execute("LOAD h3")

    # Configure HTTP settings
    con.execute(f"SET http_retries={http_retries}")
    con.execute(f"SET http_retry_wait_ms={http_retry_wait_ms}")

    # Configure temp directory for spill-to-disk operations
    con.execute("SET temp_directory='/tmp'")

    # Enable large buffer size for complex geometries
    con.execute("SET arrow_large_buffer_size=true")

    # Bound DuckDB to the pod (issue #255). Unset, its buffer manager is sized
    # at 80% of the RAM it can see, which inside a pod is the *node's*: an 8Gi
    # hex pod on a 250 GiB node believes it has ~200 GiB, never spills, and is
    # OOM-killed by the cgroup instead. The generator sets this to 85% of the
    # pod's limit, as it does for the raster hex and repartition steps (#227).
    requested_limit = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if requested_limit:
        # Normalised: the value usually comes through a Kubernetes manifest,
        # and DuckDB rejects the k8s spelling ("8Gi") outright (issue #217).
        effective = to_duckdb_memory_limit(requested_limit)
        suffix = "" if effective == requested_limit else f" (from {requested_limit})"
        print(f"  Setting DuckDB memory_limit={effective}{suffix}")
        con.execute(f"SET memory_limit='{effective}'")

    return con


class H3VectorProcessor:
    """
    Process vector datasets into H3-indexed parquet files.

    Handles chunked processing of large vector datasets, converting
    geometries to H3 cells and adding parent cell hierarchies.
    """

    def __init__(
        self,
        input_url: str,
        output_url: str,
        h3_resolution: int = 10,
        parent_resolutions: Optional[List[int]] = None,
        chunk_size: int = 500,
        intermediate_chunk_size: int = 10,
        id_column: Optional[str] = None,
        resolution_by_area: Optional[List[Tuple[Optional[float], int]]] = None,
        read_credentials: Optional[Dict[str, str]] = None,
        write_credentials: Optional[Dict[str, str]] = None,
        plan_url: Optional[str] = None,
    ):
        """
        Initialize the H3 vector processor.

        Args:
            input_url: S3 URL or local path to input parquet/geoparquet file
            output_url: S3 URL or local path to output directory
            h3_resolution: Target H3 resolution for tiling. Ignored when
                resolution_by_area is set (the finest bin resolution is used).
            parent_resolutions: List of parent resolutions to include (e.g., [9, 8, 0])
            chunk_size: Number of rows to process in pass 1 (geometry to H3 arrays)
            intermediate_chunk_size: Number of rows to process in pass 2 (unnesting arrays)
            id_column: Name of ID column to use (auto-detected if not specified)
            resolution_by_area: Optional parsed bins from parse_resolution_by_area
                (issue #98). When set, each feature is hexed at the native resolution
                its planar ST_Area maps to, and the output carries a union schema
                (one column per resolution in the bins + parent_resolutions, finer
                columns NULL in coarser tiers) plus a native_res column.
            read_credentials: Dict with AWS credentials for reading (key, secret, region, endpoint)
            write_credentials: Dict with AWS credentials for writing (key, secret, region, endpoint)
            plan_url: Optional chunk plan from `cng-datasets vector-plan` (issue
                #124). When set, chunk K is the plan's row range K rather than
                rows [K * chunk_size, (K + 1) * chunk_size), and chunk_size is
                not used to select rows.
        """
        self.input_url = input_url
        self.output_url = output_url
        self.resolution_by_area = resolution_by_area
        # In variable-resolution mode the finest bin resolution stands in for
        # h3_resolution wherever a single "target" resolution is needed (output
        # column naming for the native cell, empty-polyfill latitude checks).
        if resolution_by_area is not None:
            self.h3_resolution = max(res for _, res in resolution_by_area)
        else:
            self.h3_resolution = h3_resolution
        self.parent_resolutions = parent_resolutions or [9, 8, 0]
        if resolution_by_area is not None:
            # The output must carry an h0 column: it is the hive partition key for
            # the repartition step. Fail fast and clearly rather than letting
            # repartition_by_h0 die on a missing-column binder error downstream.
            res_set = {res for _, res in resolution_by_area} | set(self.parent_resolutions)
            if 0 not in res_set:
                raise ValueError(
                    "--resolution-by-area output needs an h0 partition column; "
                    "include 0 in --parent-resolutions (e.g. '7,6,5,4,0')."
                )
        self.chunk_size = chunk_size
        self.intermediate_chunk_size = intermediate_chunk_size
        self.id_column = id_column
        self.plan_url = plan_url
        self.read_credentials = read_credentials
        self.write_credentials = write_credentials
        self.max_cells_per_feature = int(
            os.environ.get("CNG_MAX_CELLS_PER_FEATURE", _DEFAULT_MAX_CELLS_PER_FEATURE)
        )

        self.con = setup_duckdb_connection()
        self._configure_credentials()

    def _configure_credentials(self):
        """Configure S3 credentials for DuckDB using environment variables."""
        configure_s3_credentials(self.con)

    def _find_geometry_column(self, table_name: str) -> str:
        """The geometry column of *table_name*; see `find_geometry_column`."""
        return find_geometry_column(self.con, table_name)

    def _assert_no_oversized_feature(self, id_col: str, chunk_id: int) -> None:
        """Fail fast if any feature in `chunk_table` would produce more H3
        cells than one hex pod can handle (issues #107, #125).

        Estimates per-feature cell count as area / average hex area at the target
        resolution and raises a clear RuntimeError naming the worst offender (id,
        area, estimated cells) if it exceeds ``self.max_cells_per_feature``.
        Before polygons were cut into pieces (#125) the limit was one cell
        array's 2 GB ceiling and the failure a C++ page-size assertion; now it is
        what one chunk's Pass 2 can hold (see _DEFAULT_MAX_CELLS_PER_FEATURE).

        Area is measured geodesically (``ST_Area_Spheroid`` on (lon, lat) —
        see ``_spheroid_area_m2_sql``) for the common case.
        Features whose longitude bbox spans more than 180 deg need a planar
        estimate instead, because ``ST_Area_Spheroid`` returns NaN for them (or a
        figure unrelated to what the polyfill walks), and DuckDB orders NaN above
        every number:

        - Circumpolar features are band-split and polyfilled planarly, so their
          planar area is what the fill walks (#145, #167).
        - Antimeridian crossers are polyfilled the short way round by H3 (#241).
          They are measured by their short-way planar area
          (``_planar_area_deg2_sql``), not the wrong-way region.

        In variable-resolution mode (issue #98) the estimate uses each feature's
        own native resolution — large features map to a coarser resolution and far
        fewer cells, the per-feature back-off that complements this guardrail — and
        the reported resolution is the worst offender's native res.
        """
        area_expr = _feature_area_m2_sql("geom")
        est_cells_expr = _polygon_cells_sql(
            "geom", self.h3_resolution, self.resolution_by_area)
        res_select = f"{_native_res_sql('geom', self.h3_resolution, self.resolution_by_area)} AS native_res"

        worst = self.con.execute(f"""
            SELECT
                "{id_col}" AS fid,
                {area_expr} AS area_m2,
                {est_cells_expr} AS est_cells,
                {res_select}
            FROM chunk_table
            WHERE ST_GeometryType(geom) NOT IN ('POINT', 'MULTIPOINT')
            ORDER BY est_cells DESC
            LIMIT 1
        """).fetchone()

        if worst is None or worst[2] is None:
            return
        fid, area_m2, est_cells, native_res = worst
        if est_cells > self.max_cells_per_feature:
            raise RuntimeError(
                f"Chunk {chunk_id}: feature {id_col}={fid} is too large to hex at "
                f"resolution {native_res} — estimated {int(est_cells):,} H3 "
                f"cells ({area_m2 / 1e6:,.0f} km²) would exceed the per-feature "
                f"limit ({self.max_cells_per_feature:,}). All of a feature's "
                f"cells go through one hex pod, and this many would not fit its "
                f"scratch disk. Hex this feature at a coarser resolution (see "
                f"--resolution-by-area), or raise CNG_MAX_CELLS_PER_FEATURE if "
                f"your hex pods have the disk for it."
            )

    def process_chunk(
        self,
        chunk_id: int,
        keep_cols: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Process a single chunk using two-pass approach to avoid OOM.

        Pass 1: Convert geometries to H3 cell arrays (no unnesting)
        Pass 2: Read small batches, unnest arrays, write final output

        Args:
            chunk_id: Zero-based chunk index to process
            keep_cols: List of attribute columns to keep

        Returns:
            Output file path if successful, None if chunk_id out of range
        """
        # PASS 1: Geometry to H3 arrays (no unnesting)
        intermediate_file = self._process_pass1(chunk_id)
        if intermediate_file is None:
            return None

        # PASS 2: Unnest arrays in small batches
        output_file = self._process_pass2(chunk_id, intermediate_file)

        # Clean up intermediate file
        try:
            if intermediate_file.startswith('/tmp/'):
                os.remove(intermediate_file)
        except Exception as e:
            print(f"  Warning: Could not remove intermediate file: {e}")

        return output_file

    def _chunk_rows(self, chunk_id: int) -> Tuple[int, Optional[int]]:
        """(row offset, row count) for *chunk_id*; count None = nothing to do.

        Without a plan, chunks are fixed-size slices. With one (issue #124),
        they are the plan's ranges, and an index past its end is a surplus pod
        — the hex Job was generated with an upper bound on the chunk count —
        which exits cleanly rather than failing its Job index.
        """
        if self.plan_url is None:
            return chunk_id * self.chunk_size, self.chunk_size
        from .chunk_plan import read_plan_chunk
        chunk, n = read_plan_chunk(self.con, self.plan_url, chunk_id)
        if chunk is None:
            print(f"Chunk {chunk_id} is past the plan's {n:,} chunks; nothing to do")
            return 0, None
        print(f"Chunk {chunk_id} of {n:,}: rows {chunk.row_offset:,}+{chunk.row_count:,}, "
              f"~{chunk.est_cells:,.0f} estimated cells")
        return chunk.row_offset, chunk.row_count

    def _process_pass1(
        self,
        chunk_id: int,
    ) -> Optional[str]:
        """
        Pass 1: Convert geometries to H3 cell arrays without unnesting.
        Writes intermediate parquet with arrays to disk.
        """
        offset, limit = self._chunk_rows(chunk_id)
        if limit is None:
            return None

        self.con.execute(f"""
            CREATE OR REPLACE VIEW source_table AS
            SELECT * FROM read_parquet('{self.input_url}')
            LIMIT {limit} OFFSET {offset}
        """)

        chunk_rows = self.con.execute("SELECT COUNT(*) FROM source_table").fetchone()[0]
        if chunk_rows == 0:
            print(f"Chunk {chunk_id} is empty (offset {offset:,} beyond data)")
            return None
        if self.plan_url is not None and chunk_rows != limit:
            # The plan was cut from this file; a short range means the file
            # changed under it, and hexing the remainder would silently drop
            # rows at a chunk boundary.
            raise RuntimeError(
                f"Chunk {chunk_id}: the plan says {limit:,} rows from offset "
                f"{offset:,}, but {self.input_url} has only {chunk_rows:,} there. "
                f"The GeoParquet has changed since {self.plan_url} was written; "
                f"re-run the plan step.")

        print(f"\nPass 1 - Chunk {chunk_id} ({chunk_rows:,} rows): Converting geometries to H3 arrays...")

        geom_col = self._find_geometry_column('source_table')

        id_col, is_unique = identify_id_column(
            self.con,
            'source_table',
            specified_id_col=self.id_column,
            check_uniqueness=True
        )

        if id_col is None or not is_unique:
            print("  Creating synthetic ID as _fid")
            id_col = '_fid'
            self.con.execute(f"""
                CREATE OR REPLACE VIEW source_table_with_id AS
                SELECT row_number() OVER () - 1 + {offset} AS {id_col}, *
                FROM source_table
            """)
            chunk_view_name = 'source_table_with_id'
        else:
            print(f"  Using ID column: {id_col}")
            chunk_view_name = 'source_table'

        self.con.execute(f"""
            CREATE OR REPLACE VIEW chunk_table AS
            SELECT "{id_col}", {geom_col} AS geom
            FROM {chunk_view_name}
        """)

        # Guard against a single feature whose H3 cell array would exceed the
        # 2GB / parquet-page limit (issue #107). Estimate per-feature cell count
        # up front (spheroid area / average hex area) and fail with a clear,
        # actionable error naming the feature, rather than letting the COPY below
        # die on a C++ assertion. ST_Area_Spheroid needs no reprojection, so it
        # is robust on the pathological/antimeridian polygons that trigger this.
        self._assert_no_oversized_feature(id_col, chunk_id)

        # Generate H3 arrays WITHOUT unnesting
        h3_sql = geom_to_h3_cells(
            self.con,
            'chunk_table',
            zoom=self.h3_resolution,
            keep_cols=[id_col],
            resolution_by_area=self.resolution_by_area,
        )

        # Write arrays to intermediate file (NO UNNEST!)
        intermediate_file = f"/tmp/h3_intermediate_{chunk_id:06d}.parquet"

        self.con.execute(f"""
            COPY ({h3_sql})
            TO '{intermediate_file}'
            (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """)

        # Check for features that produced 0 H3 cells despite having polygon area.
        # With the ST_PointOnSurface fallback (issue #104) a valid sub-cell
        # polygon now always yields >= 1 cell, so the only remaining cause of an
        # empty array on an area>0, valid-latitude polygon is removed. What stays
        # detectable here is swapped (lat, lon) input: those have Y outside the
        # valid latitude range, the fallback is deliberately NOT applied (it
        # would feed an invalid latitude into h3_latlng_to_cell), the array stays
        # empty, and we raise a clear RuntimeError. Any other residual empty
        # array (degenerate geometry the fallback could not represent) is just
        # warned about. Join intermediate (id + h3id) back to chunk_table (id + geom).
        swapped, small = self.con.execute(f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE ST_YMin(c.geom) < -90 OR ST_YMax(c.geom) > 90
                ) AS swapped_coords,
                COUNT(*) FILTER (
                    WHERE ST_YMin(c.geom) >= -90 AND ST_YMax(c.geom) <= 90
                ) AS too_small
            FROM read_parquet('{intermediate_file}') AS h
            JOIN chunk_table AS c ON h."{id_col}" = c."{id_col}"
            WHERE len(h.h3id) = 0
              AND ST_GeometryType(c.geom) NOT IN ('POINT', 'MULTIPOINT')
              AND ST_Area(c.geom) > 0
        """).fetchone()

        if swapped > 0:
            raise RuntimeError(
                f"Chunk {chunk_id}: {swapped} polygon feature(s) produced 0 H3 cells "
                f"and have Y coordinates outside the valid latitude range [-90, 90]. "
                f"This indicates coordinates are in (lat, lon) order instead of (lon, lat). "
                f"Check input geometry coordinate order."
            )
        if small > 0:
            res_desc = (
                "their per-feature native resolution"
                if self.resolution_by_area is not None
                else f"resolution {self.h3_resolution}"
            )
            print(
                f"  Warning: {small} polygon feature(s) in chunk {chunk_id} produced 0 H3 cells "
                f"at {res_desc} even after the representative-point "
                f"fallback — likely degenerate geometry. These features are dropped."
            )

        print(f"  ✓ Pass 1 complete: {intermediate_file}")
        return intermediate_file

    def _process_pass2(
        self,
        chunk_id: int,
        intermediate_file: str,
    ) -> str:
        """
        Pass 2: Read H3 arrays in small batches, unnest, and write final output.
        """
        print(f"Pass 2 - Chunk {chunk_id}: Unnesting arrays in small batches...")

        # Get total rows in intermediate file
        total_rows = self.con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{intermediate_file}')"
        ).fetchone()[0]

        num_batches = (total_rows + self.intermediate_chunk_size - 1) // self.intermediate_chunk_size
        print(f"  Processing {total_rows} rows in {num_batches} batches of {self.intermediate_chunk_size}")

        # Get ID column name from intermediate file
        result = self.con.execute(f"SELECT * FROM read_parquet('{intermediate_file}') LIMIT 0").description
        id_col = result[0][0]  # First column is the ID

        # Build the per-batch SELECT that unnests each feature's H3 cell array.
        #
        # Variable-resolution mode (issue #98): every chunk emits the same union
        # schema — one h{r} column per resolution in {native resolutions} ∪
        # {parent_resolutions} — so the schema is uniform across chunks and the
        # repartition step is unaffected. For a row whose feature was hexed at
        # native_res, each column h{r} is the cell's ancestor at r when r <=
        # native_res (r == native_res returns the cell itself) and NULL otherwise.
        # A per-row native_res column lets consumers filter to a tier. The common
        # floor (coarsest native resolution) is non-null in every row, preserving
        # flat equality joins.
        if self.resolution_by_area is not None:
            native_resolutions = {res for _, res in self.resolution_by_area}
            finest_native = max(native_resolutions)
            # Union of native + parent resolutions, but never finer than the
            # finest native resolution: a parent res > finest_native could never
            # be a rollup of any cell, so it would be an all-NULL column for every
            # row (the fixed-resolution path likewise only emits parents < target).
            col_resolutions = sorted(
                {r for r in native_resolutions | set(self.parent_resolutions)
                 if r <= finest_native},
                reverse=True,
            )
            union_cols = [
                f"CASE WHEN {r} <= native_res THEN h3_cell_to_parent(cell, {r}) "
                f"ELSE CAST(NULL AS UBIGINT) END AS h{r}"
                for r in col_resolutions
            ]
            union_cols_str = ', '.join(union_cols)
        else:
            # Build parent resolution columns
            h3_col = f"h{self.h3_resolution}"
            parent_cols = []
            for parent_res in sorted(self.parent_resolutions):
                if parent_res < self.h3_resolution:
                    col_name = f"h{parent_res}"
                    parent_cols.append(f"h3_cell_to_parent({h3_col}, {parent_res}) AS {col_name}")

            parent_cols_str = ', ' + ', '.join(parent_cols) if parent_cols else ''

        # Batches are written to local /tmp, then merged into the final output
        final_output = f"{self.output_url.rstrip('/')}/chunk_{chunk_id:06d}.parquet"

        # A chunk whose features all produced zero H3 cells — most commonly a
        # contiguous block of null-geometry features (issue #169) — yields a
        # zero-row intermediate. The per-batch loop below would then write no
        # batch file, and the final COPY would abort with "No files found".
        # Instead, write an empty partition with the correct output schema so the
        # chunk completes cleanly (there is genuinely no data to hex). Selecting
        # the unnest expression over the empty intermediate reproduces exactly the
        # non-empty output schema with zero rows.
        if total_rows == 0:
            if self.resolution_by_area is not None:
                empty_sql = f"""
                    SELECT "{id_col}", native_res, {union_cols_str}
                    FROM (
                        SELECT "{id_col}", native_res, UNNEST(h3id) AS cell
                        FROM read_parquet('{intermediate_file}')
                    )
                """
            else:
                empty_sql = f"""
                    SELECT "{id_col}",
                           UNNEST(h3id) AS {h3_col}{parent_cols_str}
                    FROM read_parquet('{intermediate_file}')
                """
            print(f"  Writing empty output (0 hexable rows) to {final_output}...")
            self.con.execute(f"""
                COPY ({empty_sql})
                TO '{final_output}'
                (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
            """)
            print(f"  ✓ Pass 2 complete (empty): {final_output}")
            return final_output

        batch_files = []
        for batch_id in range(num_batches):
            batch_offset = batch_id * self.intermediate_chunk_size

            # Read small batch and unnest
            # IMPORTANT: Use subquery to apply LIMIT/OFFSET to input rows BEFORE unnest
            # Otherwise DuckDB applies LIMIT/OFFSET to the unnested output!
            if self.resolution_by_area is not None:
                # Unnest to one cell per row first, then derive the union columns
                # (which reference the per-row native_res alongside each cell).
                unnest_sql = f"""
                    SELECT "{id_col}", native_res, {union_cols_str}
                    FROM (
                        SELECT "{id_col}", native_res, UNNEST(h3id) AS cell
                        FROM (
                            SELECT * FROM read_parquet('{intermediate_file}')
                            LIMIT {self.intermediate_chunk_size} OFFSET {batch_offset}
                        )
                    )
                """
            else:
                unnest_sql = f"""
                    SELECT "{id_col}",
                           UNNEST(h3id) AS {h3_col}{parent_cols_str}
                    FROM (
                        SELECT * FROM read_parquet('{intermediate_file}')
                        LIMIT {self.intermediate_chunk_size} OFFSET {batch_offset}
                    )
                """

            # One file per batch, merged once below. Appending by rewriting a
            # single growing file cost O(batches²) I/O, which a feature cut
            # into many pieces (#125) would turn into hours.
            batch_file = f"/tmp/h3_batch_{chunk_id:06d}_{batch_id:06d}.parquet"
            self.con.execute(f"""
                COPY ({unnest_sql})
                TO '{batch_file}'
                (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
            """)
            batch_files.append(batch_file)

            if (batch_id + 1) % 10 == 0 or batch_id == num_batches - 1:
                print(f"  Progress: {batch_id + 1}/{num_batches} batches")

        # Final step: copy completed file to S3 (single write operation).
        #
        # De-duplicate (feature, cell) rows here (issue #150). Pass 1 dumps each
        # feature into one row PER PART (UNNEST(ST_Dump)), and Pass 2 unnests every
        # part's cell array independently, so a cell touched by >= 2 parts of the
        # same MultiPolygon (shared part boundaries; complex coastlines / island
        # fringes) is emitted once per part — byte-identical duplicate rows that
        # silently inflate every downstream SUM/area aggregate. Every column is a
        # deterministic function of (id, cell), so DISTINCT * removes exactly the
        # excess and keeps legitimately distinct rows (different features covering
        # the same cell keep their distinct id). This must run on the fully
        # assembled per-chunk file rather than per Pass-2 batch: one feature's
        # parts can span multiple batches, and each feature lives entirely within
        # one chunk, so a per-chunk DISTINCT is both complete and sufficient.
        print(f"  Writing final output to {final_output}...")
        self.con.execute(f"""
            COPY (SELECT DISTINCT * FROM read_parquet({batch_files!r}))
            TO '{final_output}'
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
        """)

        for batch_file in batch_files:
            try:
                os.remove(batch_file)
            except Exception as e:
                print(f"  Warning: Could not remove batch file: {e}")

        print(f"  ✓ Pass 2 complete: {final_output}")
        return final_output

    def process_all_chunks(self) -> List[str]:
        """
        Process all chunks in the dataset.

        Returns:
            List of output file paths
        """
        # Get total rows
        total_rows = self.con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.input_url}')"
        ).fetchone()[0]

        if self.plan_url is not None:
            from .chunk_plan import read_plan_chunk
            _, num_chunks = read_plan_chunk(self.con, self.plan_url, 0)
        else:
            num_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size

        print(f"Processing {total_rows} rows in {num_chunks} chunks...")

        output_files = []
        for chunk_id in range(num_chunks):
            output_file = self.process_chunk(chunk_id)
            if output_file:
                output_files.append(output_file)

        return output_files


def process_vector_chunks(
    input_url: str,
    output_url: str,
    chunk_id: Optional[int] = None,
    h3_resolution: int = 10,
    parent_resolutions: Optional[List[int]] = None,
    chunk_size: int = 500,
    intermediate_chunk_size: int = 10,
    **kwargs
) -> Optional[List[str]]:
    """
    Convenience function to process vector data into H3-indexed chunks.

    Args:
        input_url: S3 URL or local path to input file
        output_url: S3 URL or local path to output directory
        chunk_id: Specific chunk to process, or None to process all
        h3_resolution: Target H3 resolution
        parent_resolutions: List of parent resolutions to include
        chunk_size: Number of rows per chunk in pass 1
        intermediate_chunk_size: Number of rows per batch in pass 2 (unnesting)
        **kwargs: Additional arguments passed to H3VectorProcessor

    Returns:
        List of output file paths, or None if single chunk was out of range
    """
    processor = H3VectorProcessor(
        input_url=input_url,
        output_url=output_url,
        h3_resolution=h3_resolution,
        parent_resolutions=parent_resolutions,
        chunk_size=chunk_size,
        intermediate_chunk_size=intermediate_chunk_size,
        **kwargs
    )

    if chunk_id is not None:
        output_file = processor.process_chunk(chunk_id)
        return [output_file] if output_file else None
    else:
        return processor.process_all_chunks()
