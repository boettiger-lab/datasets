"""Tests for the 'overlap' polyfill with cover_frac (issue #363)."""

import os
import tempfile

import pytest

from cng_datasets.vector import h3_overlap, h3_tiling
from cng_datasets.vector.h3_overlap import overlap_cells
from cng_datasets.vector.h3_tiling import (
    H3VectorProcessor,
    geom_to_h3_cells,
    parse_resolution_by_area,
    setup_duckdb_connection,
)
from tests.test_vector import _jagged_ring

# A ~1.2 ha square that holds no res-9 cell centre (the #363 MRE).
_TINY = "POLYGON((0.001 0.001, 0.002 0.001, 0.002 0.002, 0.001 0.002, 0.001 0.001))"
_HOLED = (f"POLYGON(({_jagged_ring(10, 45, 0.3, 400, 1)}), "
          f"(10.05 45.05, 9.95 45.05, 9.95 44.95, 10.05 44.95, 10.05 45.05))")
# Crosses the antimeridian as one ring, and the same area already split at 180.
_CROSSER = "POLYGON((179.9 10, -179.9 10, -179.9 10.2, 179.9 10.2, 179.9 10))"
_CROSSER_SPLIT = ("MULTIPOLYGON(((179.9 10, 180 10, 180 10.2, 179.9 10.2, 179.9 10)), "
                  "((-180 10, -179.9 10, -179.9 10.2, -180 10.2, -180 10)))")


def _overlap(features, res):
    """{fid: {cell: cover_frac}} for {fid: wkt}, and the open connection."""
    con = setup_duckdb_connection()
    con.execute("CREATE TABLE t (id INTEGER, geom GEOMETRY)")
    for fid, wkt in features.items():
        con.execute(f"INSERT INTO t VALUES ({fid}, ST_GeomFromText('{wkt}'))")
    overlap_cells(con, "t", "id", res, log=lambda *_: None)
    rows = con.execute("SELECT fid, cell, cover_frac FROM _ov_result").fetchall()
    out = {}
    for fid, cell, frac in rows:
        assert cell not in out.setdefault(fid, {}), "one row per (feature, cell)"
        out[fid][cell] = frac
    return out, con


def _planar_cover(con, cells):
    """Sum of cover_frac x planar cell area (deg²), for cells away from 180."""
    total = 0.0
    for cell, frac in cells.items():
        a = con.execute(
            f"SELECT ST_Area(ST_GeomFromWKB(h3_cell_to_boundary_wkb({cell}::UBIGINT)))").fetchone()[0]
        total += frac * a
    return total


def _area(con, wkt):
    return con.execute(f"SELECT ST_Area(ST_GeomFromText('{wkt}'))").fetchone()[0]


class TestOverlapPolyfill:

    @pytest.mark.timeout(60)
    def test_sub_cell_polygon_gets_every_cell_it_touches(self):
        """The #363 MRE: centre containment finds no cell; overlap finds the
        cells the square touches, and their fractions add up to its area."""
        out, con = _overlap({1: _TINY}, 9)
        cells = out[1]
        assert len(cells) >= 2
        assert all(0 < f <= 1 for f in cells.values())
        assert _planar_cover(con, cells) == pytest.approx(_area(con, _TINY), rel=1e-9)
        con.close()

    @pytest.mark.timeout(120)
    def test_fractions_add_up_to_the_polygon_area(self):
        out, con = _overlap({1: _HOLED}, 8)
        cells = out[1]
        assert 1.0 in cells.values() and any(f < 1 for f in cells.values())
        assert _planar_cover(con, cells) == pytest.approx(_area(con, _HOLED), rel=1e-9)
        con.close()

    @pytest.mark.timeout(120)
    def test_every_centre_cell_is_kept_whole_or_partly(self):
        out, con = _overlap({1: _HOLED}, 8)
        con.execute("CREATE TABLE c AS SELECT 1 AS id, ST_GeomFromText(?) AS geom", [_HOLED])
        sql = geom_to_h3_cells(con, "c", zoom=8, keep_cols=["id"])
        centre = {x for (lst,) in con.execute(f"SELECT h3id FROM ({sql})").fetchall() for x in lst}
        assert centre and centre <= set(out[1])
        con.close()

    @pytest.mark.timeout(180)
    def test_cutting_into_pieces_changes_no_fraction(self, monkeypatch):
        whole, con = _overlap({1: _HOLED}, 8)
        con.close()
        monkeypatch.setattr(h3_overlap, "_OVERLAP_PIECE_MAX_WORK", 2_000)
        monkeypatch.setattr(h3_tiling, "_PIECE_MAX_CELLS", 200)
        cut, con = _overlap({1: _HOLED}, 8)
        con.close()
        assert set(cut[1]) == set(whole[1])
        for cell, f in whole[1].items():
            assert cut[1][cell] == pytest.approx(f, abs=1e-6)

    @pytest.mark.timeout(60)
    def test_cell_under_a_small_hole_is_kept(self):
        """A hole that covers a cell's centre and lies inside the cell: H3's
        'overlap' mode can omit that cell (#363); the repair sweep keeps it,
        with the hole's share taken out."""
        con = setup_duckdb_connection()
        cell, lat, lng = con.execute("""
            SELECT c, h3_cell_to_lat(c), h3_cell_to_lng(c)
            FROM (SELECT h3_latlng_to_cell(0.5, 0.5, 9) AS c)""").fetchone()
        cell_area = con.execute(
            f"SELECT ST_Area(ST_GeomFromWKB(h3_cell_to_boundary_wkb({cell}::UBIGINT)))").fetchone()[0]
        con.close()
        d = 2e-4  # well inside a res-9 cell (~1.5e-3 deg across)
        hole = (f"{lng + d} {lat + d}, {lng - d} {lat + d}, {lng - d} {lat - d}, "
                f"{lng + d} {lat - d}, {lng + d} {lat + d}")
        wkt = f"POLYGON((0.45 0.45, 0.55 0.45, 0.55 0.55, 0.45 0.55, 0.45 0.45), ({hole}))"
        out, con = _overlap({1: wkt}, 9)
        assert cell in out[1]
        assert out[1][cell] == pytest.approx(1 - (2 * d) ** 2 / cell_area, rel=1e-6)
        assert _planar_cover(con, out[1]) == pytest.approx(_area(con, wkt), rel=1e-9)
        con.close()

    @pytest.mark.timeout(120)
    def test_antimeridian_crosser_matches_its_split_form(self):
        out, con = _overlap({1: _CROSSER, 2: _CROSSER_SPLIT}, 7)
        assert set(out[1]) == set(out[2])
        for cell, f in out[2].items():
            assert out[1][cell] == pytest.approx(f, abs=1e-6)
            assert 0 < f <= 1
        # Σ fraction x planar cell area = the polygon's planar area. Cells are
        # measured in a 0..360 frame, where one crossing 180 does not wrap.
        # (Not h3_cell_area against ST_Area_Spheroid: H3 measures on a sphere
        # and the spheroid area on WGS84, which differ by ~0.4% at 10N.)
        wgs84 = "+proj=longlat +datum=WGS84 +no_defs"
        covered = sum(
            f * con.execute(f"""SELECT ST_Area(ST_Transform(
                    ST_GeomFromWKB(h3_cell_to_boundary_wkb({c}::UBIGINT)),
                    '{wgs84}', '{wgs84} +lon_wrap=180', always_xy := true))""").fetchone()[0]
            for c, f in out[2].items())
        assert covered == pytest.approx(_area(con, _CROSSER_SPLIT), rel=1e-9)
        lngs = [con.execute(f"SELECT h3_cell_to_lng({c}::UBIGINT)").fetchone()[0] for c in out[2]]
        assert all(abs(x) > 179 for x in lngs), "cells must lie at the antimeridian"
        con.close()

    @pytest.mark.timeout(60)
    def test_points_and_lines_have_no_fraction(self):
        out, con = _overlap({1: "POINT(10.5 45.5)",
                             2: "LINESTRING(10 45, 10.2 45.1)"}, 8)
        con.close()
        assert len(out[1]) == 1 and list(out[1].values()) == [None]
        assert len(out[2]) > 3 and set(out[2].values()) == {None}


class TestOverlapChunk:

    @pytest.mark.timeout(240)
    def test_chunk_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = f"{tmpdir}/src.parquet"
            con = setup_duckdb_connection()
            con.execute(f"""
                COPY (
                    SELECT 1 AS _cng_fid, ST_GeomFromText('{_HOLED}') AS geom
                    UNION ALL SELECT 2, ST_GeomFromText('{_TINY}')
                    UNION ALL SELECT 3, ST_GeomFromText('POINT(20 20)')
                    UNION ALL SELECT 4, ST_GeomFromText('LINESTRING(30 30, 30.1 30.1)')
                    UNION ALL SELECT 5, ST_GeomFromText('POLYGON((40 40, 40.1 40.1, 40 40))')
                ) TO '{src}' (FORMAT PARQUET)
            """)
            con.close()
            out = f"{tmpdir}/out"
            os.makedirs(out)
            p = H3VectorProcessor(input_url=src, output_url=out, h3_resolution=8,
                                  parent_resolutions=[5, 0], chunk_size=10,
                                  polyfill="overlap")
            f = p.process_chunk(0)
            cols = [d[0] for d in p.con.execute(
                f"SELECT * FROM read_parquet('{f}') LIMIT 0").description]
            assert cols == ["_cng_fid", "h8", "cover_frac", "h0", "h5"]
            per_fid = dict(p.con.execute(f"""
                SELECT _cng_fid, count(*) FILTER (WHERE cover_frac IS NULL) = count(*)
                FROM read_parquet('{f}') GROUP BY 1""").fetchall())
            assert per_fid == {1: False, 2: False, 3: True, 4: True, 5: True}
            dup = p.con.execute(f"""
                SELECT count(*) - count(DISTINCT (_cng_fid, h8)) FROM read_parquet('{f}')""").fetchone()[0]
            assert dup == 0
            p.con.close()

    @pytest.mark.timeout(60)
    def test_swapped_coordinates_raise(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = f"{tmpdir}/src.parquet"
            con = setup_duckdb_connection()
            con.execute(f"""COPY (SELECT 1 AS _cng_fid,
                ST_GeomFromText('POLYGON((45 100, 45.1 100, 45.1 100.1, 45 100.1, 45 100))') AS geom)
                TO '{src}' (FORMAT PARQUET)""")
            con.close()
            p = H3VectorProcessor(input_url=src, output_url=tmpdir, h3_resolution=8,
                                  parent_resolutions=[0], chunk_size=10, polyfill="overlap")
            with pytest.raises(RuntimeError, match="lat, lon"):
                p.process_chunk(0)
            p.con.close()

    @pytest.mark.timeout(240)
    def test_resolution_by_area(self):
        """Each feature is filled at its native resolution, in the union schema
        of --resolution-by-area, with the same fractions a fixed-resolution
        overlap fill gives it at that resolution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = f"{tmpdir}/src.parquet"
            con = setup_duckdb_connection()
            con.execute(f"""
                COPY (
                    SELECT 1 AS _cng_fid, ST_GeomFromText('{_HOLED}') AS geom
                    UNION ALL SELECT 2, ST_GeomFromText('{_TINY}')
                    UNION ALL SELECT 3, ST_GeomFromText('POINT(20 20)')
                    UNION ALL SELECT 4, ST_GeomFromText('LINESTRING(30 30, 30.1 30.1)')
                ) TO '{src}' (FORMAT PARQUET)
            """)
            con.close()
            out = f"{tmpdir}/out"
            os.makedirs(out)
            # _HOLED (~0.28 deg²) is hexed at res 6; the rest at res 8.
            p = H3VectorProcessor(input_url=src, output_url=out,
                                  resolution_by_area=parse_resolution_by_area("0.01:8,6"),
                                  parent_resolutions=[5, 0], chunk_size=10,
                                  polyfill="overlap")
            f = p.process_chunk(0)
            cols = [d[0] for d in p.con.execute(
                f"SELECT * FROM read_parquet('{f}') LIMIT 0").description]
            assert cols == ["_cng_fid", "native_res", "h8", "h6", "h5", "h0", "cover_frac"]
            tiers = dict(p.con.execute(f"""
                SELECT _cng_fid, list(DISTINCT native_res) FROM read_parquet('{f}') GROUP BY 1
            """).fetchall())
            assert tiers == {1: [6], 2: [8], 3: [8], 4: [8]}
            assert p.con.execute(f"""
                SELECT count(*) FROM read_parquet('{f}')
                WHERE h0 IS NULL OR h5 IS NULL OR h6 IS NULL
                   OR (native_res = 6) <> (h8 IS NULL)""").fetchone()[0] == 0
            got = {fid: dict(p.con.execute(f"""
                       SELECT CASE WHEN native_res = 6 THEN h6 ELSE h8 END, cover_frac
                       FROM read_parquet('{f}') WHERE _cng_fid = {fid}""").fetchall())
                   for fid in (1, 2)}
            p.con.close()
        for fid, wkt, res in ((1, _HOLED, 6), (2, _TINY, 8)):
            want, con = _overlap({fid: wkt}, res)
            assert got[fid].keys() == want[fid].keys()
            assert got[fid] == pytest.approx(want[fid], abs=1e-6)
            assert _planar_cover(con, got[fid]) == pytest.approx(_area(con, wkt), rel=1e-6)
            con.close()
