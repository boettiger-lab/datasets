"""
Tests for cells-per-chunk planning of the vector hex fan-out (issue #124).
"""

import os
import tempfile

import duckdb
import pytest

from cng_datasets.vector.chunk_plan import (
    cut_chunks,
    feature_cells_sql,
    plan_chunks,
    read_plan_chunk,
    run_plan,
)
from cng_datasets.vector.h3_tiling import H3VectorProcessor, setup_duckdb_connection


def _contiguous(plan, n_rows):
    """Every row in exactly one chunk, in order."""
    expect = 0
    for chunk in plan.chunks:
        assert chunk.row_offset == expect
        assert chunk.row_count >= 1
        expect += chunk.row_count
    assert expect == n_rows


class TestCutChunks:
    def test_budget_starts_a_new_chunk(self):
        assert cut_chunks([4, 4, 4, 4], cells_budget=8, max_features=100) == [
            (0, 2, 8.0), (2, 2, 8.0)]

    def test_a_feature_over_budget_is_a_chunk_of_its_own(self):
        """A feature is the smallest unit a pod can be given."""
        assert cut_chunks([1, 50, 1], cells_budget=10, max_features=100) == [
            (0, 1, 1.0), (1, 1, 50.0), (2, 1, 1.0)]

    def test_feature_cap_binds_for_tiny_features(self):
        """Points are 1 cell each; without the cap they would all share a chunk
        and pass 2 would run one batch query per 10 of them."""
        chunks = cut_chunks([1] * 25, cells_budget=1e9, max_features=10)
        assert [c[1] for c in chunks] == [10, 10, 5]

    def test_empty(self):
        assert cut_chunks([], cells_budget=10, max_features=10) == []


class TestPlanChunks:
    def test_skewed_layer_is_split_where_it_is_heavy(self):
        cells = [100] * 1000 + [5_000_000] * 3 + [100] * 1000
        plan = plan_chunks(cells, cells_budget=1_000_000)
        _contiguous(plan, len(cells))
        # The fixed rule would give 2 chunks of 1000; each heavy feature is
        # isolated instead, and the light rows keep the fixed rule's grouping.
        assert len(plan.chunks) == 1 + 3 + 1
        assert plan.oversized == 3
        assert plan.cells_budget == plan.requested_budget

    def test_never_coarser_than_the_fixed_rule(self):
        cells = [1] * 5697
        plan = plan_chunks(cells, cells_budget=1e12, max_chunks=200)
        assert [c.row_count for c in plan.chunks][:5] == [1000] * 5
        assert len(plan.chunks) == 6  # the #144 repro, unchanged

    def test_budget_is_raised_to_fit_max_chunks_and_says_so(self):
        cells = [1_000_000] * 100
        plan = plan_chunks(cells, cells_budget=1_000_000, max_chunks=10)
        _contiguous(plan, 100)
        assert len(plan.chunks) <= 10
        assert plan.cells_budget > plan.requested_budget

    def test_feature_cap_is_raised_to_fit_max_chunks(self):
        plan = plan_chunks([1] * 5000, cells_budget=1e12, max_chunks=2)
        assert len(plan.chunks) == 2
        assert plan.max_features == 2500

    def test_rejects_nonpositive_inputs(self):
        with pytest.raises(ValueError):
            plan_chunks([1, 2], cells_budget=0)
        with pytest.raises(ValueError):
            plan_chunks([1, 2], max_chunks=0)


class TestFeatureCells:
    @pytest.fixture(scope="class")
    def con(self):
        con = setup_duckdb_connection()
        yield con
        con.close()

    def _cells(self, con, wkt, res=8):
        geom = f"ST_GeomFromText('{wkt}')"
        return con.execute(f"SELECT {feature_cells_sql(geom, res)}").fetchone()[0]

    def test_point_is_one_cell(self, con):
        assert self._cells(con, "POINT(120 10)") == 1

    def test_polygon_east_of_90_is_measured(self, con):
        """The #253 axis-order bug would make this NaN, then 1."""
        cells = self._cells(con, "POLYGON((100 0,102 0,102 2,100 2,100 0))")
        hex_m2 = con.execute("SELECT h3_get_hexagon_area_avg(8, 'm^2')").fetchone()[0]
        assert cells == pytest.approx(4 * 12_308e6 / hex_m2, rel=0.01)

    def test_line_scales_with_length(self, con):
        short = self._cells(con, "LINESTRING(120 10, 120.1 10)")
        long_ = self._cells(con, "LINESTRING(120 10, 121 10)")
        assert long_ == pytest.approx(10 * short, rel=0.01)
        assert short > 1


class TestRunPlan:
    def _source(self, tmpdir, n_small=30, n_big=2):
        src = os.path.join(tmpdir, "src.parquet")
        con = setup_duckdb_connection()
        con.execute(f"""
            COPY (
                SELECT i AS _cng_fid,
                       CASE WHEN i IN (10, 20)
                            THEN ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))')
                            ELSE ST_GeomFromText('POLYGON((10 10,10.01 10,10.01 10.01,10 10.01,10 10))')
                       END AS geom
                FROM range({n_small + n_big}) t(i)
            ) TO '{src}' (FORMAT PARQUET)
        """)
        con.close()
        return src

    @pytest.mark.timeout(60)
    def test_writes_the_plan_and_reports_its_chunk_count(self, tmp_path):
        src = self._source(str(tmp_path))
        out = str(tmp_path / "_hex_plan.parquet")
        log = tmp_path / "termination-log"
        log.write_text("")
        plan = run_plan(src, out, h3_resolution=7, cells_per_chunk=2_000,
                        max_chunks=200, termination_log=str(log))
        _contiguous(plan, 32)
        assert log.read_text() == f"chunks={len(plan.chunks)}\n"
        # The two 1-degree boxes (~2.3k res-7 cells each) are isolated.
        big = [c for c in plan.chunks if c.est_cells > 2_000]
        assert [(c.row_offset, c.row_count) for c in big] == [(10, 1), (20, 1)]

        con = duckdb.connect()
        assert con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0] == len(plan.chunks)
        meta = dict(con.execute(
            f"SELECT key::VARCHAR, value::VARCHAR FROM parquet_kv_metadata('{out}')"
        ).fetchall())
        assert meta["total_rows"] == "32"
        assert meta["cells_budget"] == "2000"

    @pytest.mark.timeout(120)
    def test_planned_hex_matches_fixed_chunking(self, tmp_path):
        """A plan changes where chunks start, never what is hexed or which id
        a row gets — including the synthetic _fid, which is offset-derived."""
        src = self._source(str(tmp_path))
        con = setup_duckdb_connection()
        # Drop the id column so _fid is synthesised from the row offset.
        nofid = str(tmp_path / "nofid.parquet")
        con.execute(f"COPY (SELECT geom FROM '{src}') TO '{nofid}' (FORMAT PARQUET)")
        con.close()
        plan_url = str(tmp_path / "_hex_plan.parquet")
        plan = run_plan(nofid, plan_url, h3_resolution=7, cells_per_chunk=2_000,
                        termination_log=None)
        assert len(plan.chunks) > 1

        def hex_rows(out, **kwargs):
            proc = H3VectorProcessor(input_url=nofid, output_url=out, h3_resolution=7,
                                     parent_resolutions=[0], **kwargs)
            proc.process_all_chunks()
            rows = proc.con.execute(
                f"SELECT * FROM read_parquet('{out}/*.parquet') ORDER BY ALL").fetchall()
            proc.con.close()
            return rows

        fixed = hex_rows(str(tmp_path / "fixed"), chunk_size=1000)
        planned = hex_rows(str(tmp_path / "planned"), plan_url=plan_url)
        assert fixed and planned == fixed

    @pytest.mark.timeout(60)
    def test_index_past_the_plan_is_a_clean_no_op(self, tmp_path):
        src = self._source(str(tmp_path))
        plan_url = str(tmp_path / "_hex_plan.parquet")
        plan = run_plan(src, plan_url, h3_resolution=7, termination_log=None)
        proc = H3VectorProcessor(input_url=src, output_url=str(tmp_path / "out"),
                                 h3_resolution=7, parent_resolutions=[0],
                                 plan_url=plan_url)
        assert proc.process_chunk(len(plan.chunks) + 5) is None
        chunk, n = read_plan_chunk(proc.con, plan_url, 0)
        assert chunk.row_offset == 0 and n == len(plan.chunks)
        proc.con.close()

    @pytest.mark.timeout(60)
    def test_a_plan_for_a_different_file_is_refused(self, tmp_path):
        """Rows the plan promises but the file lacks would be dropped silently."""
        src = self._source(str(tmp_path))
        plan_url = str(tmp_path / "_hex_plan.parquet")
        run_plan(src, plan_url, h3_resolution=7, cells_per_chunk=1e12,
                 termination_log=None)
        shorter = str(tmp_path / "shorter.parquet")
        con = duckdb.connect()
        con.execute(f"COPY (SELECT * FROM '{src}' LIMIT 5) TO '{shorter}' (FORMAT PARQUET)")
        con.close()
        proc = H3VectorProcessor(input_url=shorter, output_url=str(tmp_path / "out"),
                                 h3_resolution=7, parent_resolutions=[0],
                                 plan_url=plan_url)
        with pytest.raises(RuntimeError, match="re-run the plan step"):
            proc.process_chunk(0)
        proc.con.close()
