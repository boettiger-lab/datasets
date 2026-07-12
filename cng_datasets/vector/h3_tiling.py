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


# Average H3 edge lengths in km (from h3geo.org/docs/core-library/restable)
_H3_EDGE_KM = {
    0: 1281.256011, 1: 483.0568391, 2: 182.5129565, 3: 68.97922179,
    4: 26.07175968, 5: 9.854090990, 6: 3.724532667, 7: 1.406475763,
    8: 0.531414010, 9: 0.200786148, 10: 0.075863783, 11: 0.028663897,
    12: 0.010830188, 13: 0.004092010, 14: 0.001546100, 15: 0.000584169,
}

# Pass 1 writes each feature's H3 cells as a single list value. DuckDB/Arrow
# cannot hold one list value larger than 2^31-1 bytes — i.e. ~268M UBIGINT
# cells — and the parquet page-size assertion (PrimitiveColumnWriter::NextPage)
# can fire below that. We refuse a feature whose estimated cell count exceeds a
# conservative fraction of that ceiling, raising a clear error instead of a C++
# assertion (issue #107). The estimate (spheroid area / average hex area)
# undercounts the true polyfill by ~1.3x, so the default leaves wide margin.
# Override via the CNG_MAX_CELLS_PER_FEATURE environment variable.
_DEFAULT_MAX_CELLS_PER_FEATURE = 134_000_000


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

    The catch-all (``threshold is None``) becomes the ELSE branch.
    """
    whens = [
        f"WHEN ST_Area({geom_expr}) <= {threshold} THEN {res}"
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


def _transmeridian_split_sql(carry_cols: str, source: str) -> str:
    """SQL for a CTE body that splits >180-deg-longitude-span polygons into bands.

    Rows from ``source`` whose longitude bbox span is <= 180 deg pass through
    unchanged (the common fast path); wider rows are cross-joined with the
    longitude bands and intersected, so downstream ST_Dump + H3 polyfill only ever
    see sub-180-deg pieces. The intersection may yield a MULTIPOLYGON or
    GEOMETRYCOLLECTION, which the existing ST_Dump step already splits into single
    geometries before polyfill.

    Args:
        carry_cols: comma-separated non-geometry columns to propagate (e.g. the id
            column, plus ``native_res`` in variable-resolution mode).
        source: name of the upstream CTE exposing those columns and a ``geom`` column.
    """
    bands = ', '.join(
        f"(ST_GeomFromText('POLYGON(({lo} -90, {hi} -90, {hi} 90, {lo} 90, {lo} -90))'))"
        for lo, hi in _TRANSMERIDIAN_BANDS
    )
    return f'''
            SELECT {carry_cols}, geom
            FROM {source}
            WHERE ST_XMax(geom) - ST_XMin(geom) <= {_TRANSMERIDIAN_MAX_SPAN_DEG}
            UNION ALL
            SELECT {carry_cols}, ST_Intersection(s.geom, _bands.band) AS geom
            FROM {source} AS s, (VALUES {bands}) AS _bands(band)
            WHERE ST_XMax(s.geom) - ST_XMin(s.geom) > {_TRANSMERIDIAN_MAX_SPAN_DEG}
              AND ST_Intersects(s.geom, _bands.band)
    '''


def _h3_edge_length_degrees(resolution: int) -> float:
    """Return the H3 edge length in degrees for a given resolution.

    Uses the equatorial approximation (1 deg ~ 111.32 km) which
    slightly over-buffers at higher latitudes — the safe direction
    for a spatial index.
    """
    return _H3_EDGE_KM[resolution] / 111.32


def _buffer_case_sql(native_res_col: str) -> str:
    """Build a SQL CASE mapping a per-row native H3 resolution column to the line
    buffer width in degrees (H3 edge length at that resolution).

    Used only on the variable-resolution path (issue #98), where the buffer for a
    line feature must track that feature's native_res rather than a fixed zoom.
    """
    whens = " ".join(
        f"WHEN {res} THEN {_h3_edge_length_degrees(res)}" for res in sorted(_H3_EDGE_KM)
    )
    # Fall back to the finest resolution's edge length if native_res is unexpected.
    return f"CASE {native_res_col} {whens} ELSE {_h3_edge_length_degrees(max(_H3_EDGE_KM))} END"


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

    # For line geometries, buffer to a thin polygon so h3_polygon_wkt_to_cells
    # can identify every H3 cell the line passes through.  Buffer width = H3
    # edge length in degrees (equatorial approximation; slightly over-buffers
    # at higher latitudes, which is the safe direction for a spatial index).
    buffer_deg = _h3_edge_length_degrees(zoom)

    if line_count > 0:
        print(
            f"  Line geometries detected ({line_count} features). "
            f"Buffering by {buffer_deg:.6f} deg (~H3 edge length at res {zoom}) "
            f"before H3 polyfill to ensure continuous cell coverage."
        )

    # Variable-resolution (issue #98): when resolution_by_area is provided, each
    # feature is hexed at the native resolution its planar ST_Area maps to, a
    # per-feature `native_res` column is emitted, and the H3 functions take that
    # column (not a literal zoom) as the resolution argument — DuckDB's H3
    # bindings accept a per-row resolution expression. The buffer width for line
    # geometries likewise tracks each feature's native_res. Otherwise the
    # original single-`zoom` path below is used unchanged.
    if resolution_by_area is not None:
        native_res_case = _native_res_case_sql(resolution_by_area, geom_col)
        buffer_case = _buffer_case_sql("native_res")
        return f'''
        WITH tbase AS (
            SELECT {col_list},
                   {geom_col} AS _geom_orig,
                   {native_res_case} AS native_res
            FROM {table_name}
        ),
        t0 AS (
            SELECT {col_list}, native_res,
                   CASE
                       WHEN ST_GeometryType(_geom_orig) IN ('LINESTRING', 'MULTILINESTRING')
                       THEN ST_Multi(ST_Buffer(ST_Force2D(_geom_orig), {buffer_case}))
                       WHEN ST_GeometryType(_geom_orig) = 'POLYGON'
                       THEN ST_Multi(_geom_orig)
                       ELSE _geom_orig
                   END AS geom
            FROM tbase
        ),
        tsplit AS ({_transmeridian_split_sql(f'{col_list}, native_res', 't0')}),
        t1 AS (
            SELECT {col_list}, native_res,
                   UNNEST(ST_Dump(geom)).geom AS geom
            FROM tsplit
        ),
        t2 AS (
            SELECT {col_list}, native_res, geom,
                   CASE
                       WHEN ST_GeometryType(geom) = 'POINT'
                       THEN [h3_latlng_to_cell(ST_Y(geom), ST_X(geom), native_res)]
                       ELSE h3_polygon_wkt_to_cells(ST_AsText(ST_Force2D(geom)), native_res)
                   END AS h3id
            FROM t1
        )
        SELECT {col_list}, native_res,
               CASE
                   WHEN h3id IS NOT NULL AND len(h3id) > 0 THEN h3id
                   WHEN ST_YMin(geom) >= -90 AND ST_YMax(geom) <= 90
                   THEN [h3_latlng_to_cell(
                            ST_Y(ST_PointOnSurface(geom)),
                            ST_X(ST_PointOnSurface(geom)),
                            native_res)]
                   ELSE h3id
               END AS h3id
        FROM t2
        '''

    # Convert to multi-polygons and unnest, then generate H3 cells
    # The geometry is already GEOMETRY type in DuckDB spatial extension
    # Line geometries are buffered into polygons before polyfill.
    #
    # Any polygon smaller than one H3 cell at this resolution contains no cell
    # centre, so h3_polygon_wkt_to_cells returns an empty array and the feature
    # would vanish from the output entirely; a chunk in which every feature is
    # sub-cell produces no rows at all (issue #104). When the polyfill is empty
    # we fall back to the single cell containing the feature's ST_PointOnSurface
    # — a guaranteed-interior representative point — so every feature yields
    # >= 1 cell regardless of its size. The fallback is gated on a valid latitude
    # range: a polygon with empty polyfill AND out-of-range Y is left empty on
    # purpose so the downstream swapped-(lat,lon) check still raises a clear
    # error rather than feeding an invalid latitude into h3_latlng_to_cell.
    sql = f'''
        WITH t0 AS (
            SELECT {col_list},
                   CASE
                       WHEN ST_GeometryType({geom_col}) IN ('LINESTRING', 'MULTILINESTRING')
                       THEN ST_Multi(ST_Buffer(ST_Force2D({geom_col}), {buffer_deg}))
                       WHEN ST_GeometryType({geom_col}) = 'POLYGON'
                       THEN ST_Multi({geom_col})
                       ELSE {geom_col}
                   END AS geom
            FROM {table_name}
        ),
        tsplit AS ({_transmeridian_split_sql(col_list, 't0')}),
        t1 AS (
            SELECT {col_list},
                   UNNEST(ST_Dump(geom)).geom AS geom
            FROM tsplit
        ),
        t2 AS (
            SELECT {col_list},
                   geom,
                   CASE
                       WHEN ST_GeometryType(geom) = 'POINT'
                       THEN [h3_latlng_to_cell(ST_Y(geom), ST_X(geom), {zoom})]
                       ELSE h3_polygon_wkt_to_cells(ST_AsText(ST_Force2D(geom)), {zoom})
                   END AS h3id
            FROM t1
        )
        SELECT {col_list},
               CASE
                   WHEN h3id IS NOT NULL AND len(h3id) > 0 THEN h3id
                   WHEN ST_YMin(geom) >= -90 AND ST_YMax(geom) <= 90
                   THEN [h3_latlng_to_cell(
                            ST_Y(ST_PointOnSurface(geom)),
                            ST_X(ST_PointOnSurface(geom)),
                            {zoom})]
                   ELSE h3id
               END AS h3id
        FROM t2
    '''

    return sql


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
        """Find the geometry column in the table."""
        result = self.con.execute(f"SELECT * FROM {table_name} LIMIT 0").description
        columns = [col[0] for col in result]
        for col in columns:
            if col.upper() in ['SHAPE', 'GEOMETRY', 'GEOM']:
                return col
        raise ValueError(f"No geometry column found. Available columns: {columns}")

    def _assert_no_oversized_feature(self, id_col: str, chunk_id: int) -> None:
        """Fail fast if any feature in `chunk_table` would produce an H3 cell
        array exceeding the single-array size limit (issue #107).

        Estimates per-feature cell count as spheroid area / average hex area at
        the target resolution and raises a clear RuntimeError naming the worst
        offender (id, area, estimated cells) if it exceeds
        ``self.max_cells_per_feature``. This converts an otherwise-fatal C++
        page-size assertion in the Pass-1 COPY into an actionable error.

        In variable-resolution mode (issue #98) the estimate uses each feature's
        own native resolution — large features map to a coarser resolution and far
        fewer cells, the per-feature back-off that complements this guardrail — and
        the reported resolution is the worst offender's native res.
        """
        if self.resolution_by_area is not None:
            native_res_case = _native_res_case_sql(self.resolution_by_area, "geom")
            est_cells_expr = (
                f"ST_Area_Spheroid(geom) / "
                f"h3_get_hexagon_area_avg({native_res_case}, 'm^2')"
            )
            res_select = f"{native_res_case} AS native_res"
        else:
            avg_hex_m2 = self.con.execute(
                f"SELECT h3_get_hexagon_area_avg({self.h3_resolution}, 'm^2')"
            ).fetchone()[0]
            est_cells_expr = f"ST_Area_Spheroid(geom) / {avg_hex_m2}"
            res_select = f"{self.h3_resolution} AS native_res"

        worst = self.con.execute(f"""
            SELECT
                "{id_col}" AS fid,
                ST_Area_Spheroid(geom) AS area_m2,
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
                f"cell-array limit ({self.max_cells_per_feature:,}). A single "
                f"feature's cells are written as one array value, which cannot "
                f"exceed the ~268M-element (2GB) Arrow/parquet-page ceiling. "
                f"Hex this feature at a coarser resolution (see #98 for adaptive "
                f"variable-resolution polyfill), or raise CNG_MAX_CELLS_PER_FEATURE "
                f"if your build tolerates a larger array."
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

    def _process_pass1(
        self,
        chunk_id: int,
    ) -> Optional[str]:
        """
        Pass 1: Convert geometries to H3 cell arrays without unnesting.
        Writes intermediate parquet with arrays to disk.
        """
        offset = chunk_id * self.chunk_size

        self.con.execute(f"""
            CREATE OR REPLACE VIEW source_table AS
            SELECT * FROM read_parquet('{self.input_url}')
            LIMIT {self.chunk_size} OFFSET {offset}
        """)

        chunk_rows = self.con.execute("SELECT COUNT(*) FROM source_table").fetchone()[0]
        if chunk_rows == 0:
            print(f"Chunk {chunk_id} is empty (offset {offset:,} beyond data)")
            return None

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

        # Use local /tmp for fast batch processing, then copy to final destination
        local_output = f"/tmp/h3_output_{chunk_id:06d}.parquet"
        final_output = f"{self.output_url.rstrip('/')}/chunk_{chunk_id:06d}.parquet"

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

            # Build up local file with fast local disk I/O
            if batch_id == 0:
                # First batch: create local file
                self.con.execute(f"""
                    COPY ({unnest_sql})
                    TO '{local_output}'
                    (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
                """)
            else:
                # Subsequent batches: append using local temp files
                temp_batch_file = f"/tmp/h3_batch_{chunk_id:06d}_{batch_id}.tmp"
                self.con.execute(f"""
                    COPY ({unnest_sql})
                    TO '{temp_batch_file}'
                    (FORMAT PARQUET, COMPRESSION 'ZSTD')
                """)

                # Merge into local output using fast local disk
                self.con.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{local_output}')
                        UNION ALL
                        SELECT * FROM read_parquet('{temp_batch_file}')
                    )
                    TO '{local_output}.new'
                    (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
                """)

                # Fast local file operations
                os.replace(f"{local_output}.new", local_output)
                os.remove(temp_batch_file)

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
            COPY (SELECT DISTINCT * FROM read_parquet('{local_output}'))
            TO '{final_output}'
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
        """)

        # Clean up local file
        try:
            os.remove(local_output)
        except Exception as e:
            print(f"  Warning: Could not remove local output file: {e}")

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
