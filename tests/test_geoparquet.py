"""GeoParquet 1.1 output with a bbox covering column (issue #340).

Portolan requires GeoParquet 1.1 or 2.x and spatial statistics on every row
group (PORTO-FMT-007/012). These tests pin what convert writes and that the
covering column stays out of the hex and PMTiles outputs built from it.
"""

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from cng_datasets.vector.convert_to_parquet import write_with_duckdb
from cng_datasets.vector.geoparquet import (
    covering_columns,
    crs_projjson,
    plan_covering,
)


def _geo(path):
    return json.loads(pq.read_schema(str(path)).metadata[b"geo"])


def _con():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    return con


# 2,500 squares, reversed in id so a lost ORDER BY shows.
SQUARES = """
    SELECT i AS _cng_fid, 'f' || i AS name,
           ST_MakeEnvelope(-120 + (i % 50) * 0.1, 35 + (i // 50) * 0.1,
                           -120 + (i % 50) * 0.1 + 0.05, 35 + (i // 50) * 0.1 + 0.05) AS geom
    FROM range(2500) t(i) ORDER BY i DESC
"""


@pytest.fixture
def written(tmp_path):
    out = tmp_path / "squares.parquet"
    assert write_with_duckdb(SQUARES, str(out), row_group_size=500) == 2500
    return out


class TestConvertWritesGeoParquet11:
    @pytest.mark.timeout(60)
    def test_geo_key_is_1_1_with_a_bbox_covering(self, written):
        geo = _geo(written)
        assert geo["version"] == "1.1.0"
        assert geo["primary_column"] == "geom"
        col = geo["columns"]["geom"]
        assert col["encoding"] == "WKB"
        assert col["geometry_types"] == []
        assert "crs" not in col  # OGC:CRS84
        assert col["covering"] == {"bbox": {f: ["bbox", f] for f in ("xmin", "ymin", "xmax", "ymax")}}

    @pytest.mark.timeout(60)
    def test_one_geo_key_and_the_provenance_stamp_survives(self, written):
        kv = pq.read_metadata(str(written)).metadata
        assert [k for k in kv if k == b"geo"] == [b"geo"]
        assert b"cng_datasets_version" in kv

    @pytest.mark.timeout(60)
    def test_geometry_stays_plain_wkb(self, written):
        """No native Parquet GEOMETRY type, so older readers see what they always did."""
        col = pq.ParquetFile(str(written)).schema.column(3 - 1)
        assert col.name == "geom"
        assert col.physical_type == "BYTE_ARRAY"
        assert col.logical_type.type == "NONE"
        assert _con().execute(f"SELECT typeof(geom) FROM '{written}' LIMIT 1").fetchone()[0] \
            == "GEOMETRY('OGC:CRS84')"

    @pytest.mark.timeout(60)
    def test_every_row_group_has_bbox_statistics(self, written):
        meta = pq.read_metadata(str(written))
        assert meta.num_row_groups == 5
        names = [meta.schema.column(i).path for i in range(meta.num_columns)]
        for rg in range(meta.num_row_groups):
            for field in ("xmin", "ymin", "xmax", "ymax"):
                stats = meta.row_group(rg).column(names.index(f"bbox.{field}")).statistics
                assert stats is not None and stats.has_min_max

    @pytest.mark.timeout(60)
    def test_bbox_matches_each_geometry(self, written):
        bad = _con().execute(f"""
            SELECT count(*) FROM '{written}'
            WHERE bbox.xmin != ST_XMin(geom) OR bbox.ymin != ST_YMin(geom)
               OR bbox.xmax != ST_XMax(geom) OR bbox.ymax != ST_YMax(geom)""").fetchone()[0]
        assert bad == 0

    @pytest.mark.timeout(60)
    def test_row_order_of_the_query_is_kept(self, written):
        """The covering is a projection over the Hilbert-ordered query (#276)."""
        ids = [r[0] for r in _con().execute(f"SELECT _cng_fid FROM '{written}'").fetchall()]
        assert ids == list(range(2499, -1, -1))

    @pytest.mark.timeout(60)
    def test_an_existing_bbox_column_is_not_overwritten(self, tmp_path):
        out = tmp_path / "b.parquet"
        write_with_duckdb(
            "SELECT 'mine' AS bbox, ST_Point(1, 2) AS geom", str(out))
        geo = _geo(out)
        assert geo["columns"]["geom"]["covering"]["bbox"]["xmin"] == ["geom_bbox", "xmin"]
        row = _con().execute(f"SELECT bbox, geom_bbox.xmin FROM '{out}'").fetchone()
        assert row == ("mine", 1.0)

    @pytest.mark.timeout(60)
    def test_a_table_without_geometry_gets_no_geo_key(self, tmp_path):
        out = tmp_path / "plain.parquet"
        assert write_with_duckdb("SELECT * FROM range(7) t(i)", str(out)) == 7
        assert b"geo" not in (pq.read_metadata(str(out)).metadata or {})
        assert pq.read_schema(str(out)).names == ["i"]


class TestCrs:
    def test_crs84_and_epsg4326_are_written_as_no_crs(self):
        assert crs_projjson("GEOMETRY('OGC:CRS84')") is None
        assert crs_projjson("GEOMETRY('EPSG:4326')") is None
        assert crs_projjson("GEOMETRY") is None

    def test_a_projected_crs_is_written_as_projjson(self):
        crs = crs_projjson("GEOMETRY('EPSG:3310')")
        assert crs["id"] == {"authority": "EPSG", "code": 3310}

    def test_an_unreadable_crs_fails_the_write(self):
        with pytest.raises(ValueError):
            crs_projjson("GEOMETRY('not a crs at all')")

    def test_plan_puts_crs_on_the_column(self):
        plan = plan_covering([("id", "BIGINT"), ("SHAPE", "GEOMETRY('EPSG:3310')")])
        assert plan.primary == "SHAPE" and plan.column == "bbox"
        assert plan.geo["columns"]["SHAPE"]["crs"]["id"]["code"] == 3310


class TestCoveringStaysInTheGeoParquet:
    @pytest.mark.timeout(60)
    def test_covering_columns_reads_the_geo_key(self, written, tmp_path):
        con = _con()
        assert covering_columns(con, str(written)) == ["bbox"]
        legacy = tmp_path / "legacy.parquet"
        con.execute(f"COPY ({SQUARES}) TO '{legacy}' (FORMAT PARQUET)")
        assert covering_columns(con, str(legacy)) == []

    @pytest.mark.timeout(60)
    def test_pmtiles_chunk_drops_it(self, written, tmp_path):
        from cng_datasets.vector.pmtiles_chunks import extract_chunk
        out = tmp_path / "chunk.parquet"
        assert extract_chunk(str(written), str(out), n_chunks=2, index=0) == 1250
        assert "bbox" not in pq.read_schema(str(out)).names
        out = tmp_path / "chunk_simplified.parquet"
        extract_chunk(str(written), str(out), simplify=0.001)
        assert "bbox" not in pq.read_schema(str(out)).names

    @pytest.mark.timeout(120)
    def test_hex_output_drops_it(self, written, tmp_path):
        from cng_datasets.vector.h3_tiling import H3VectorProcessor
        from cng_datasets.vector.repartition import repartition_by_h0
        chunks = tmp_path / "chunks"
        chunks.mkdir()
        p = H3VectorProcessor(input_url=str(written), output_url=str(chunks),
                              h3_resolution=6, parent_resolutions=[0], chunk_size=5000)
        p.process_chunk(0)
        p.con.close()
        repartition_by_h0(chunks_dir=str(chunks), output_dir=str(tmp_path / "hex"),
                          source_parquet=str(written), cleanup=False)
        cols = _con().execute(
            f"DESCRIBE SELECT * FROM read_parquet('{tmp_path}/hex/**/*.parquet')"
        ).fetchall()
        names = [c[0] for c in cols]
        assert "name" in names and "_cng_fid" in names
        assert "bbox" not in names and "geom" not in names

    @pytest.mark.timeout(60)
    def test_ogr_does_not_expose_it_to_the_single_pod_pmtiles(self, written, tmp_path):
        """The single-pod PMTiles step runs ogr2ogr on the GeoParquet; GDAL must
        treat the covering as part of the geometry, not as an attribute."""
        import subprocess
        out = tmp_path / "f.geojsonl"
        subprocess.run(["ogr2ogr", "-f", "GeoJSONSeq", str(out), str(written)], check=True)
        first = json.loads(out.read_text().splitlines()[0])
        assert "bbox" not in first["properties"]
        assert set(first["properties"]) == {"_cng_fid", "name"}

    @pytest.mark.timeout(120)
    def test_reconverting_a_geoparquet_replaces_its_covering(self, written, tmp_path):
        """A GeoParquet source's own covering is dropped, not carried beside a new one."""
        from cng_datasets.vector.convert_to_parquet import convert_to_parquet
        out = tmp_path / "again.parquet"
        convert_to_parquet(str(written), str(out), verbose=False, progress=False)
        names = pq.read_schema(str(out)).names
        assert names.count("bbox") == 1 and "geom_bbox" not in names
        assert _geo(out)["columns"]["geom"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]
