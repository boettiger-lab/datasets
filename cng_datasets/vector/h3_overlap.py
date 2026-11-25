"""The 'overlap' polyfill: every H3 cell a feature touches, with its coverage.

The default vector hex keeps a cell when the polygon contains its centre. That
loses a feature smaller than a cell (it falls back to one cell, #104) and makes
a hex join miss overlaps narrower than a cell (#363). This module writes one row
per (feature, cell) whose areas intersect, with ``cover_frac``: the share of the
cell's area inside the feature, in (0, 1].

For each polygon piece (the same pieces the centre polyfill fills, see
``_piece_split_sql``):

1. H3 fills the piece in 'overlap' and in 'full' containment. A cell in 'full'
   lies wholly inside the piece and gets 1 with no geometry work.
2. The other cells (the boundary) are intersected with the piece. The piece is
   first clipped to the bounding box of each group of boundary cells sharing an
   h(R-4) ancestor, so a long coastline is never intersected whole per cell.
3. H3's 'overlap' mode omits some cells the piece overlaps, for example a cell
   whose centre a small hole covers (#363). So the neighbours of boundary cells
   that H3 did not return, and the cells under every hole vertex, are tested
   too, repeating from the cells found until none is added.
4. Fractions are summed per (feature, cell) over pieces, snapped to 1 within
   _SNAP, and cells touching only along an edge (zero area) are dropped.

Fractions are planar ratios in lon/lat, the convention H3 itself uses for cell
edges. Within one cell they differ from geodesic ratios by well under 0.1% at
res 9. Every piece reaching this step lies within -180..180: antimeridian
crossers are cut at 180 first, and a cell crossing 180 is unwrapped and also
meets the piece shifted by +360.

Points and lines have no area to share: a point keeps its cell and a line every
cell it passes through (#239), both with ``cover_frac`` NULL. A polygon feature
left with no cell (zero area) keeps the cell at a point on it, also NULL.

Ported from boettiger-lab/data-workflows ``hex_overlap.py`` (#1015), validated
there on all of WDPA: every parcel of 1 km² or more summed to its polygon area
within 4e-5.
"""

import concurrent.futures as cf
from typing import List

import duckdb

from .h3_tiling import (
    _LINE_BUFFER_DEG,
    _LINE_BUFFER_QUAD_SEGS,
    _piece_split_sql,
    _representative_point_sql,
    _transmeridian_split_sql,
)

# A piece whose estimated cells x vertices exceeds this is cut further before
# its fill. The overlap fill runs H3 twice and intersects every boundary cell,
# so it cuts much earlier than the centre polyfill's _PIECE_MAX_WORK: on WDPA a
# chunk filled in 304 s at 2e7 and 117 s at 2e6.
_OVERLAP_PIECE_MAX_WORK = 2_000_000

# Rounds of the repair sweep (step 3). On all of WDPA it added cells in round 1
# and none after.
_REPAIR_ROUNDS = 6

# Summed fractions within _SNAP of 1 are written as 1.
_SNAP = 1e-6

POLYFILL_MODES = ("centre", "overlap")


def _pieces_sql(table: str, id_col: str, res: int) -> str:
    """SQL for the pieces of every feature: ``fid, _kind, geom``.

    ``_kind`` is 'point', 'line' or 'polygon'. Polygons are made valid, split
    at 180 and cut like the centre polyfill's pieces, with a smaller budget.
    """
    line_buffer = f"{_LINE_BUFFER_DEG}, {_LINE_BUFFER_QUAD_SEGS}"
    is_line = "ST_GeometryType(geom) IN ('LINESTRING', 'MULTILINESTRING')"
    carry = "fid, _is_line"
    tbig, tcross, twrap, tpieces = _piece_split_sql(
        carry, "t1", str(res), max_work=_OVERLAP_PIECE_MAX_WORK, split_crossers=True)
    return f"""
        WITH RECURSIVE t00 AS (
            SELECT "{id_col}" AS fid, ST_Force2D(geom) AS geom
            FROM {table} WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
        ),
        t0 AS (
            SELECT fid, {is_line} AS _is_line,
                   CASE WHEN {is_line}
                        THEN ST_Multi(ST_Buffer(geom, {line_buffer}))
                        WHEN ST_GeometryType(geom) IN ('POLYGON', 'MULTIPOLYGON')
                             AND NOT ST_IsValid(geom)
                        THEN ST_MakeValid(geom)
                        ELSE geom END AS geom
            FROM t00
        ),
        tsplit AS ({_transmeridian_split_sql(carry, 't0')}),
        t1 AS (
            SELECT fid, _is_line, UNNEST(ST_Dump(geom)).geom AS geom FROM tsplit
        ),
        tbig AS ({tbig}),
        tcross AS ({tcross}),
        twrap AS ({twrap}),
        tpieces AS ({tpieces})
        SELECT fid,
               CASE WHEN _is_line THEN 'line'
                    WHEN ST_GeometryType(geom) = 'POINT' THEN 'point'
                    WHEN ST_GeometryType(geom) = 'POLYGON' THEN 'polygon'
                    ELSE 'other' END AS _kind,
               geom
        FROM tpieces WHERE NOT _split AND NOT ST_IsEmpty(geom)
    """


def _fractions(con, src: str, dst: str, grp_res: int) -> None:
    """Fractions of (qid, cell) pairs in *src* into *dst* (qid, cell, frac).

    Each cell polygon is unwrapped if it crosses the antimeridian; the piece is
    clipped to the bbox of each group of cells sharing an h<grp_res> ancestor,
    and each cell meets only its group's clip. A crossing cell also meets the
    piece shifted by +360.
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE _ov_fx AS
        WITH g AS (SELECT qid, cell, ST_GeomFromWKB(h3_cell_to_boundary_wkb(cell)) AS cg FROM {src})
        SELECT qid, cell, h3_cell_to_parent(cell, {grp_res}) AS grp,
               CASE WHEN ST_XMax(cg) - ST_XMin(cg) > 180 THEN
                 ST_MakePolygon(ST_MakeLine(list_transform(
                   range(1, ST_NPoints(ST_ExteriorRing(cg)) + 1),
                   lambda i: ST_Point(
                     CASE WHEN ST_X(ST_PointN(ST_ExteriorRing(cg), i::INTEGER)) < 0
                          THEN ST_X(ST_PointN(ST_ExteriorRing(cg), i::INTEGER)) + 360
                          ELSE ST_X(ST_PointN(ST_ExteriorRing(cg), i::INTEGER)) END,
                     ST_Y(ST_PointN(ST_ExteriorRing(cg), i::INTEGER))))))
               ELSE cg END AS cg,
               ST_XMax(cg) - ST_XMin(cg) > 180 AS am
        FROM g
    """)
    con.execute("""
        CREATE OR REPLACE TABLE _ov_clip AS
        WITH e AS (SELECT qid, grp, am, ST_GeomFromWKB(ST_AsWKB(ST_Extent_Agg(cg))) AS env
                   FROM _ov_fx GROUP BY qid, grp, am)
        SELECT e.qid, e.grp, e.am, ST_Intersection(p.geom, e.env) AS c0,
               CASE WHEN e.am THEN ST_Intersection(ST_Translate(p.geom, 360, 0), e.env) END AS c1
        FROM e JOIN _ov_pieces p USING (qid)
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE {dst} AS
        SELECT f.qid, f.cell,
               (ST_Area(ST_Intersection(f.cg, c.c0))
                + coalesce(ST_Area(ST_Intersection(f.cg, c.c1)), 0)) / ST_Area(f.cg) AS frac
        FROM _ov_fx f JOIN _ov_clip c USING (qid, grp, am)
    """)


def overlap_cells(con: duckdb.DuckDBPyConnection, table: str, id_col: str,
                  res: int, threads: int = 1, log=print) -> str:
    """Fill every feature of *table* (columns *id_col*, ``geom``) in overlap
    mode, into the table ``_ov_result (fid, cell UBIGINT, cover_frac
    FLOAT)``, one row per (feature, cell). Returns that table's name.

    Raises if a polygon piece still spans more than 180 degrees of longitude,
    since its planar fractions would be wrong (an antimeridian crosser that
    could not be cut, because it is invalid in a 0..360 frame).
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE _ov_pieces AS
        SELECT row_number() OVER (ORDER BY ST_NPoints(geom) DESC) AS qid, *
        FROM ({_pieces_sql(table, id_col, res)})
    """)
    wide = con.execute("""
        SELECT any_value(fid), count(*) FROM _ov_pieces
        WHERE _kind = 'polygon' AND ST_XMax(geom) - ST_XMin(geom) > 180
    """).fetchone()
    if wide[1]:
        raise RuntimeError(
            f"Overlap polyfill: {wide[1]} polygon piece(s) still span more than "
            f"180 degrees of longitude (first: {id_col}={wide[0]}), so their "
            f"coverage fractions would be measured the wrong way round. This is "
            f"an antimeridian crosser that is invalid even in a 0..360 frame; "
            f"repair or split it at 180 in the source.")
    n_pieces, n_pts = con.execute(
        "SELECT count(*), coalesce(sum(ST_NPoints(geom)), 0) FROM _ov_pieces").fetchone()
    log(f"  Overlap polyfill: {n_pieces:,} pieces, {n_pts:,} vertices")

    # 1. Fill. A scalar over a small table runs on one DuckDB thread, so the
    #    pieces are dealt to cursors (heaviest first, by qid) on threads.
    con.execute("CREATE OR REPLACE TABLE _ov_ov (qid BIGINT, cell UBIGINT)")
    con.execute("CREATE OR REPLACE TABLE _ov_fu (qid BIGINT, cell UBIGINT)")
    con.execute("CREATE OR REPLACE TABLE _ov_pt (qid BIGINT, cell UBIGINT)")
    threads = max(1, int(threads))

    def fill(k: int) -> None:
        cur = con.cursor()
        try:
            for tbl, kinds, expr in (
                ("_ov_ov", "('polygon', 'line')",
                 f"h3_polygon_wkb_to_cells_experimental(ST_AsWKB(geom), {res}, 'overlap')"),
                ("_ov_fu", "('polygon')",
                 f"h3_polygon_wkb_to_cells_experimental(ST_AsWKB(geom), {res}, 'full')"),
            ):
                cur.execute(f"""
                    INSERT INTO {tbl}
                    SELECT qid, UNNEST({expr}) FROM _ov_pieces
                    WHERE _kind IN {kinds} AND qid % {threads} = {k}
                """)
        finally:
            cur.close()

    if threads == 1:
        fill(0)
    else:
        with cf.ThreadPoolExecutor(threads) as ex:
            list(ex.map(fill, range(threads)))
    con.execute(f"""
        INSERT INTO _ov_pt
        SELECT qid, h3_latlng_to_cell(ST_Y(geom), ST_X(geom), {res})
        FROM _ov_pieces WHERE _kind = 'point'
    """)

    # 2. Boundary cells of polygon pieces: overlap minus full.
    grp_res = max(res - 4, 0)
    con.execute("""
        CREATE OR REPLACE TABLE _ov_bnd AS
        SELECT qid, cell FROM _ov_ov SEMI JOIN (SELECT qid FROM _ov_pieces WHERE _kind = 'polygon') USING (qid)
        ANTI JOIN _ov_fu USING (qid, cell)
    """)
    _fractions(con, "_ov_bnd", "_ov_bfrac", grp_res)

    # 3. Repair: test neighbours H3 did not return, and cells under hole
    #    vertices. The hole test sits in a CASE, not a WHERE: DuckDB 2.0.0
    #    pushed `WHERE ST_NumInteriorRings(g) > 0` into a table scan and
    #    returned no rows (data-workflows #1000).
    con.execute("""
        CREATE OR REPLACE TABLE _ov_seen AS
        SELECT qid, cell FROM _ov_ov SEMI JOIN (SELECT qid FROM _ov_pieces WHERE _kind = 'polygon') USING (qid)
    """)
    con.execute("CREATE OR REPLACE TABLE _ov_frontier AS SELECT qid, cell FROM _ov_bnd")
    con.execute(f"""
        CREATE OR REPLACE TABLE _ov_hole_seed AS
        WITH h AS (SELECT qid, CASE WHEN _kind = 'polygon' AND ST_NumInteriorRings(geom) > 0
                                    THEN ST_Difference(ST_MakePolygon(ST_ExteriorRing(geom)), geom) END AS holes
                   FROM _ov_pieces),
        pts AS (SELECT qid, UNNEST(ST_Dump(ST_Points(holes))).geom AS pt FROM h WHERE holes IS NOT NULL)
        SELECT DISTINCT qid, h3_latlng_to_cell(ST_Y(pt), ST_X(pt), {res}) AS cell FROM pts
    """)
    n_rep = 0
    for rnd in range(_REPAIR_ROUNDS):
        seed = "UNION SELECT qid, cell FROM _ov_hole_seed" if rnd == 0 else ""
        con.execute(f"""
            CREATE OR REPLACE TABLE _ov_cand AS
            SELECT * FROM (SELECT DISTINCT qid, UNNEST(h3_grid_disk(cell, 1)) AS cell FROM _ov_frontier
                           {seed})
            ANTI JOIN _ov_seen USING (qid, cell)
        """)
        if not con.execute("SELECT count(*) FROM _ov_cand").fetchone()[0]:
            break
        con.execute("INSERT INTO _ov_seen SELECT qid, cell FROM _ov_cand")
        _fractions(con, "_ov_cand", "_ov_rfrac", grp_res)
        con.execute("CREATE OR REPLACE TABLE _ov_frontier AS SELECT qid, cell FROM _ov_rfrac WHERE frac > 0")
        n_new = con.execute("SELECT count(*) FROM _ov_frontier").fetchone()[0]
        con.execute("INSERT INTO _ov_bfrac SELECT * FROM _ov_rfrac WHERE frac > 0")
        n_rep += n_new
        if not n_new:
            break
    log(f"  Overlap polyfill: repair added {n_rep:,} (piece, cell) pairs that H3 'overlap' omitted")

    # 4. Per feature: sum over pieces, snap to 1, drop zero area. Lines and
    #    points add their cells with NULL, unless a polygon part of the same
    #    feature already holds the cell.
    con.execute(f"""
        CREATE OR REPLACE TABLE _ov_result AS
        WITH u AS (SELECT qid, cell, 1.0::DOUBLE AS frac FROM _ov_fu
                   UNION ALL SELECT qid, cell, frac FROM _ov_bfrac),
        f AS (SELECT p.fid, u.cell, sum(u.frac) AS frac
              FROM u JOIN _ov_pieces p USING (qid) GROUP BY p.fid, u.cell),
        poly AS (SELECT fid, cell,
                        CASE WHEN frac >= 1 - {_SNAP} THEN 1.0 ELSE frac END::FLOAT AS cover_frac
                 FROM f WHERE frac > 0),
        other AS (SELECT DISTINCT p.fid, c.cell
                  FROM (SELECT qid, cell FROM _ov_ov UNION ALL SELECT qid, cell FROM _ov_pt) c
                  JOIN _ov_pieces p USING (qid) WHERE p._kind IN ('line', 'point'))
        SELECT * FROM poly
        UNION ALL
        SELECT fid, cell, NULL::FLOAT FROM other ANTI JOIN poly USING (fid, cell)
    """)

    # A feature left with no cell (zero-area polygon) keeps the cell at a point
    # on it, as in the centre polyfill (#104).
    pos = _representative_point_sql("geom")
    con.execute(f"""
        INSERT INTO _ov_result
        SELECT fid, h3_latlng_to_cell(ST_Y(pos), ST_X(pos), {res}), NULL::FLOAT
        FROM (SELECT "{id_col}" AS fid, {pos} AS pos FROM {table}
              WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom))
        ANTI JOIN (SELECT DISTINCT fid FROM _ov_result) USING (fid)
        WHERE ST_Y(pos) BETWEEN -90 AND 90
    """)
    # Ordinary tables, not TEMP: the fill's cursors are separate connections,
    # which do not see another connection's temp tables.
    for t in ("_ov_ov", "_ov_fu", "_ov_pt", "_ov_bnd", "_ov_bfrac", "_ov_seen", "_ov_frontier",
              "_ov_hole_seed", "_ov_cand", "_ov_rfrac", "_ov_fx", "_ov_clip", "_ov_pieces"):
        con.execute(f"DROP TABLE IF EXISTS {t}")
    return "_ov_result"


def overlap_output_sql(id_col: str, res: int, parent_resolutions: List[int]) -> str:
    """SELECT over ``_ov_result`` in the hex output schema: the id, ``h<res>``,
    ``cover_frac``, then each parent coarser than *res*, ascending."""
    parents = "".join(
        f", h3_cell_to_parent(cell, {p}) AS h{p}"
        for p in sorted(parent_resolutions) if p < res)
    return f'SELECT fid AS "{id_col}", cell AS h{res}, cover_frac{parents} FROM _ov_result'
