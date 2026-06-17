"""Unit tests for convert_to_parquet with GeoParquet validation."""

import json
import pytest
import tempfile
import os
from pathlib import Path
import duckdb
from cng_datasets.vector.convert_to_parquet import (
    convert_to_parquet,
    build_read_reproject_query,
)


class TestReprojectQueryAxisOrder:
    """Axis-order handling in the read/reproject query (regression for #128).

    DuckDB's ST_Transform honours each CRS's authority axis order by default
    (EPSG:4326 is latitude-first), while GeoParquet always stores (lon, lat).
    The query passes always_xy := true so ST_Transform reads and writes (lon, lat)
    for every CRS, which removes the need for ST_FlipCoordinates and fixes the
    lat/lon swap that the old numeric is_geographic_crs() heuristic produced for
    geographic compound CRSs such as EPSG:5498 ("NAD83 + NAVD88 height").
    """

    @pytest.mark.parametrize("source_crs", [
        "EPSG:5498",   # NAD83 + NAVD88 height — compound geographic, code >= 5000 (#128)
        "EPSG:4269",   # NAD83 (Census TIGER), latitude-first geographic
        "OGC:CRS84",   # WGS84 longitude-first geographic
        "EPSG:4979",   # WGS84 geographic 3D
        "EPSG:3310",   # CA Albers, projected
    ])
    def test_reproject_uses_always_xy_and_never_flips(self, source_crs):
        sql = build_read_reproject_query(
            "dummy.gdb", source_crs=source_crs, target_crs="EPSG:4326", geom_col="geom"
        )
        assert "always_xy := true" in sql, "ST_Transform must force (lon, lat) axis order"
        assert "ST_FlipCoordinates" not in sql, (
            "No coordinate flip should be emitted; always_xy already yields (lon, lat)"
        )

    def test_no_reprojection_when_source_equals_target(self):
        sql = build_read_reproject_query(
            "dummy.gpkg", source_crs="EPSG:4326", target_crs="EPSG:4326", geom_col="geom"
        )
        assert "ST_Transform" not in sql

    def test_compound_crs_transform_preserves_lon_lat(self):
        """End-to-end ST_Transform check (no ogr2ogr needed): a point stored as
        (lon, lat) in EPSG:5498 must remain (lon, lat) after transform to EPSG:4326.

        This is the exact #128 scenario isolated to the transform DuckDB runs.
        """
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")
        try:
            # Compton Creek-ish point: lon=-118.2, lat=33.9 (stored x=lon, y=lat)
            x, y = con.execute("""
                SELECT ST_X(g), ST_Y(g) FROM (
                    SELECT ST_Transform(ST_Point(-118.2, 33.9),
                                        'EPSG:5498', 'EPSG:4326', always_xy := true) AS g
                )
            """).fetchone()
            assert -119 < x < -117, f"X must be longitude ~-118, got {x:.3f} (lat/lon swapped)"
            assert 33 < y < 35, f"Y must be latitude ~34, got {y:.3f} (lat/lon swapped)"
        finally:
            con.close()

    def test_compound_crs_to_crs84_target_is_valid_and_finite(self):
        """EPSG:5498 -> OGC:CRS84 must yield valid, finite geometry.

        #128 noted that --target-crs OGC:CRS84 produced corrupt geometry (xmax=inf,
        most features invalid). That is the same axis-order root cause: without
        always_xy, ST_Transform read the stored (lon, lat) as 5498's authority
        (lat, lon), feeding an out-of-range "latitude" into PROJ and blowing up to
        infinity. always_xy := true fixes the EPSG:4326 and OGC:CRS84 targets alike.
        """
        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")
        try:
            wkt = "POLYGON((-118.3 33.8,-118.1 33.8,-118.1 34.0,-118.3 34.0,-118.3 33.8))"
            valid, xmax, ymax = con.execute(f"""
                SELECT ST_IsValid(g), ST_XMax(g), ST_YMax(g) FROM (
                    SELECT ST_Transform(ST_GeomFromText('{wkt}'),
                                        'EPSG:5498', 'OGC:CRS84', always_xy := true) AS g
                )
            """).fetchone()
            assert valid, "transform to OGC:CRS84 produced invalid geometry"
            assert xmax < 0 and xmax > -119, f"xmax {xmax} not a finite CA longitude"
            assert 33 < ymax < 35, f"ymax {ymax} not a finite CA latitude"
        finally:
            con.close()


class TestConvertToParquet:
    """Test convert_to_parquet functionality."""
    
    @pytest.fixture
    def sample_geojson(self):
        """Create a temporary GeoJSON file for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            f.write("""{
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"name": "Feature 1", "value": 100},
                        "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]}
                    },
                    {
                        "type": "Feature",
                        "properties": {"name": "Feature 2", "value": 200},
                        "geometry": {"type": "Point", "coordinates": [-122.5, 37.9]}
                    },
                    {
                        "type": "Feature",
                        "properties": {"name": "Feature 3", "value": 300},
                        "geometry": {"type": "Point", "coordinates": [-122.6, 38.0]}
                    }
                ]
            }""")
            path = f.name
        
        yield path
        
        # Cleanup
        if os.path.exists(path):
            os.remove(path)
    
    @pytest.fixture
    def sample_geojson_with_id(self):
        """Create a GeoJSON file with an existing ID column."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            f.write("""{
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"fid": 1, "name": "Feature 1"},
                        "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]}
                    },
                    {
                        "type": "Feature",
                        "properties": {"fid": 2, "name": "Feature 2"},
                        "geometry": {"type": "Point", "coordinates": [-122.5, 37.9]}
                    }
                ]
            }""")
            path = f.name
        
        yield path
        
        if os.path.exists(path):
            os.remove(path)
    
    def test_convert_creates_id_column_when_missing(self, sample_geojson):
        """Test that conversion adds _cng_fid when no ID exists."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            convert_to_parquet(
                source_url=sample_geojson,
                destination=output_path,
                force_id=True,
                progress=False
            )
            
            # Verify file exists
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0
            
            # Verify _cng_fid column was added
            con = duckdb.connect()
            cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{output_path}')").fetchdf()['column_name'].tolist()
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
            ids = con.execute(f"SELECT _cng_fid FROM read_parquet('{output_path}') ORDER BY _cng_fid").fetchdf()['_cng_fid'].tolist()

            assert '_cng_fid' in cols, "Expected _cng_fid column"
            assert count == 3, "Expected 3 features"
            assert ids == [1, 2, 3], "Expected sequential IDs"

            con.close()
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
    def test_convert_preserves_source_id_and_adds_cng_fid(self, sample_geojson_with_id):
        """_cng_fid is always created even when source already has a 'fid' column.

        Source ID columns like 'fid' may identify features/geometries rather than rows
        (e.g. multiple rows per site sharing the same fid). _cng_fid is additive and
        the source column is preserved unchanged.
        """
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name

        try:
            convert_to_parquet(
                source_url=sample_geojson_with_id,
                destination=output_path,
                force_id=True,
                progress=False
            )

            con = duckdb.connect()
            cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{output_path}')").fetchdf()['column_name'].tolist()
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]

            assert 'fid' in cols, "Source fid column must be preserved"
            assert '_cng_fid' in cols, "_cng_fid must always be created as a row-unique key"
            assert count == 2, "Expected 2 features"

            # _cng_fid must be unique per row
            ids = con.execute(f"SELECT _cng_fid FROM read_parquet('{output_path}') ORDER BY _cng_fid").fetchdf()['_cng_fid'].tolist()
            assert ids == list(range(1, count + 1)), f"_cng_fid must be 1..N, got {ids}"

            con.close()

        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
    def test_parquet_input_with_nonunique_fid_gets_cng_fid(self):
        """Parquet input whose 'fid' is a feature key (not row-unique) still gets
        a row-unique _cng_fid (issue #43).

        Mirrors the TPL Conservation Almanac case: multiple rows share one fid
        (one row per funding program per site). The old parquet path picked 'fid'
        as the id and never synthesized _cng_fid, making the repartition join
        many-to-many. _cng_fid must be 1..N and additive (fid preserved).
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source.parquet")
            output_path = os.path.join(tmpdir, "out.parquet")

            con = duckdb.connect()
            con.install_extension("spatial")
            con.load_extension("spatial")
            # 3 sites, fid repeated (2 + 2 + 1 = 5 rows): fid is NOT row-unique.
            con.execute(f"""
                COPY (
                    SELECT * FROM (VALUES
                        (42453, 'grant',   ST_Point(-122.4, 37.8)),
                        (42453, 'bond',    ST_Point(-122.4, 37.8)),
                        (42454, 'grant',   ST_Point(-122.5, 37.9)),
                        (42454, 'private', ST_Point(-122.5, 37.9)),
                        (42455, 'grant',   ST_Point(-122.6, 38.0))
                    ) t(fid, program, geom)
                ) TO '{source}' (FORMAT PARQUET)
            """)
            con.close()

            convert_to_parquet(
                source_url=source,
                destination=output_path,
                force_id=True,
                progress=False,
            )

            con = duckdb.connect()
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}')"
            ).fetchdf()['column_name'].tolist()
            assert 'fid' in cols, "Source fid must be preserved"
            assert '_cng_fid' in cols, "_cng_fid must be synthesized even when fid exists"

            count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
            ).fetchone()[0]
            distinct = con.execute(
                f"SELECT COUNT(DISTINCT _cng_fid) FROM read_parquet('{output_path}')"
            ).fetchone()[0]
            assert count == 5 and distinct == 5, (
                f"_cng_fid must be row-unique: {distinct} distinct / {count} rows"
            )
            # fid stays non-unique (proves it was the wrong row key).
            fid_distinct = con.execute(
                f"SELECT COUNT(DISTINCT fid) FROM read_parquet('{output_path}')"
            ).fetchone()[0]
            assert fid_distinct == 3, "fid should remain a (non-unique) feature key"
            con.close()

    def test_parquet_input_preserves_existing_cng_fid(self):
        """A parquet source that already carries _cng_fid is not re-numbered."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "source.parquet")
            output_path = os.path.join(tmpdir, "out.parquet")

            con = duckdb.connect()
            con.install_extension("spatial")
            con.load_extension("spatial")
            con.execute(f"""
                COPY (
                    SELECT * FROM (VALUES
                        (100, ST_Point(-122.4, 37.8)),
                        (200, ST_Point(-122.5, 37.9))
                    ) t(_cng_fid, geom)
                ) TO '{source}' (FORMAT PARQUET)
            """)
            con.close()

            convert_to_parquet(
                source_url=source,
                destination=output_path,
                force_id=True,
                progress=False,
            )

            con = duckdb.connect()
            ids = con.execute(
                f"SELECT _cng_fid FROM read_parquet('{output_path}') ORDER BY _cng_fid"
            ).fetchdf()['_cng_fid'].tolist()
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}')"
            ).fetchdf()['column_name'].tolist()
            con.close()

            assert ids == [100, 200], f"existing _cng_fid must be preserved, got {ids}"
            assert cols.count('_cng_fid') == 1, "must not duplicate _cng_fid"

    def test_geoparquet_metadata_is_valid(self, sample_geojson):
        """Test that output has valid GeoParquet metadata."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            convert_to_parquet(
                source_url=sample_geojson,
                destination=output_path,
                force_id=True,
                progress=False
            )
            
            # Verify GeoParquet metadata exists
            import pyarrow.parquet as pq
            table = pq.read_table(output_path)
            metadata = table.schema.metadata
            
            # Check for geo metadata
            assert b'geo' in metadata, "Expected 'geo' metadata key"
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)


class TestGeoParquetCRS:
    """Test that DuckDB 1.5 writes correct CRS metadata in GeoParquet output."""

    def test_wgs84_geojson_has_geo_metadata_with_crs(self):
        """
        GeoJSON (WGS84) → GeoParquet should produce valid geo metadata.

        DuckDB 1.5 writes CRS as PROJJSON in the 'geo' key. For OGC:CRS84/WGS84,
        the CRS field may be omitted (it's the GeoParquet default), which is valid.
        """
        import json
        import pyarrow.parquet as pq

        geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"name": "test"},
                "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]}
            }]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            json.dump(geojson, f)
            src_path = f.name

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name

        try:
            convert_to_parquet(
                source_url=src_path,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False,
                progress=False,
            )

            metadata = pq.read_metadata(output_path)
            schema_metadata = metadata.schema.to_arrow_schema().metadata
            assert b'geo' in schema_metadata, "Missing 'geo' metadata key in GeoParquet output"

            geo = json.loads(schema_metadata[b'geo'])
            assert 'columns' in geo, "geo metadata missing 'columns' key"

            # Find the geometry column entry
            geom_cols = geo['columns']
            assert len(geom_cols) > 0, "No geometry columns in geo metadata"

            col_name = list(geom_cols.keys())[0]
            col_meta = geom_cols[col_name]
            assert col_meta.get('encoding') == 'WKB', f"Expected WKB encoding, got {col_meta.get('encoding')}"

            # CRS for WGS84 may be omitted (GeoParquet default) or present as PROJJSON
            crs = col_meta.get('crs')
            if crs is not None:
                assert isinstance(crs, dict), f"CRS should be PROJJSON dict, got {type(crs)}"
                # PROJJSON should have a type field
                assert 'type' in crs, f"PROJJSON CRS missing 'type': {crs}"

        finally:
            for p in [src_path, output_path]:
                if os.path.exists(p):
                    os.remove(p)

    def test_projected_crs_written_to_metadata(self):
        """
        EPSG:3310 → EPSG:4326 reprojection must write CRS metadata.

        After reprojection to WGS84, the output CRS should reflect EPSG:4326/OGC:CRS84.
        Since this is the GeoParquet default, CRS may be absent (valid) or present as PROJJSON.
        """
        import subprocess
        import json
        import shutil
        import pyarrow.parquet as pq

        wgs84_geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"name": "test"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-122.4, 37.8], [-122.4, 37.9],
                        [-122.3, 37.9], [-122.3, 37.8],
                        [-122.4, 37.8]
                    ]]
                }
            }]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='_src.geojson', delete=False) as f:
            json.dump(wgs84_geojson, f)
            src_path = f.name

        temp_dir = tempfile.mkdtemp()
        shp_path = os.path.join(temp_dir, 'test_3310.shp')

        try:
            result = subprocess.run(
                ['ogr2ogr', '-f', 'ESRI Shapefile', '-t_srs', 'EPSG:3310', shp_path, src_path],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                pytest.skip(f"Could not create projected test data: {result.stderr}")

            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
                output_path = f.name

            convert_to_parquet(
                source_url=shp_path,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False,
                progress=False,
            )

            metadata = pq.read_metadata(output_path)
            schema_metadata = metadata.schema.to_arrow_schema().metadata
            assert b'geo' in schema_metadata, "Missing 'geo' metadata after reprojection"

            geo = json.loads(schema_metadata[b'geo'])
            col_name = list(geo['columns'].keys())[0]
            col_meta = geo['columns'][col_name]

            # Geometry type should be present
            assert 'geometry_types' in col_meta, "Missing geometry_types in geo metadata"

            # CRS either absent (default WGS84) or valid PROJJSON
            crs = col_meta.get('crs')
            if crs is not None:
                assert isinstance(crs, dict), f"CRS should be PROJJSON dict, got {type(crs)}"

        finally:
            for p in [src_path]:
                if os.path.exists(p):
                    os.remove(p)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            if 'output_path' in locals() and os.path.exists(output_path):
                os.remove(output_path)

    def test_nad83_reprojection_preserves_geo_metadata(self):
        """
        EPSG:4269 (NAD83) → EPSG:4326 must produce geo metadata.

        This is the Census TIGER data scenario: geographic→geographic reprojection.
        """
        import subprocess
        import json
        import shutil
        import pyarrow.parquet as pq

        geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"name": "test"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-122.4, 37.8], [-122.4, 37.9],
                        [-122.3, 37.9], [-122.3, 37.8],
                        [-122.4, 37.8]
                    ]]
                }
            }]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='_src.geojson', delete=False) as f:
            json.dump(geojson, f)
            src_path = f.name

        temp_dir = tempfile.mkdtemp()
        shp_path = os.path.join(temp_dir, 'test_4269.shp')

        try:
            result = subprocess.run(
                ['ogr2ogr', '-f', 'ESRI Shapefile', '-t_srs', 'EPSG:4269', shp_path, src_path],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                pytest.skip(f"Could not create NAD83 test data: {result.stderr}")

            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
                output_path = f.name

            convert_to_parquet(
                source_url=shp_path,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False,
                progress=False,
            )

            metadata = pq.read_metadata(output_path)
            schema_metadata = metadata.schema.to_arrow_schema().metadata
            assert b'geo' in schema_metadata, "Missing 'geo' metadata for NAD83→WGS84"

            geo = json.loads(schema_metadata[b'geo'])
            assert 'columns' in geo
            assert len(geo['columns']) > 0, "No geometry columns in geo metadata"

        finally:
            for p in [src_path]:
                if os.path.exists(p):
                    os.remove(p)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            if 'output_path' in locals() and os.path.exists(output_path):
                os.remove(output_path)

    def test_no_bbox_struct_column_in_output(self):
        """
        Verify output does NOT contain a bbox struct column.

        DuckDB 1.5 uses native parquet row-group statistics for spatial filtering
        via the && operator, making the bbox struct column unnecessary. The bbox
        column causes friction in downstream tools (e.g., must be dropped for
        GeoJSON export).
        """
        import json

        geojson = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"name": "test"},
                "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]}
            }]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            json.dump(geojson, f)
            src_path = f.name

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name

        try:
            convert_to_parquet(
                source_url=src_path,
                destination=output_path,
                verbose=False,
                progress=False,
            )

            con = duckdb.connect()
            cols = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}')"
            ).fetchdf()['column_name'].tolist()
            con.close()

            assert 'bbox' not in cols, (
                "Output contains a 'bbox' struct column. DuckDB 1.5 uses native "
                "parquet row-group statistics instead — the bbox column should not be added."
            )

        finally:
            for p in [src_path, output_path]:
                if os.path.exists(p):
                    os.remove(p)


class TestReprojection:
    """Test reprojection functionality - critical for catching projection bugs."""
    
    @pytest.fixture
    def sample_projected_shapefile(self):
        """
        Create a shapefile in EPSG:3310 (NAD83/California Albers) for testing reprojection.
        
        Uses projected coordinates in meters that must be reprojected to WGS84 degrees.
        """
        import subprocess
        import json
        import shutil
        
        # Create a simple GeoJSON in WGS84 first (SF Bay Area)
        wgs84_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Area 1"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.4, 37.8], [-122.4, 37.9], 
                            [-122.3, 37.9], [-122.3, 37.8], 
                            [-122.4, 37.8]
                        ]]
                    }
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='_wgs84.geojson', delete=False) as f:
            json.dump(wgs84_geojson, f)
            wgs84_path = f.name
        
        # Create output directory for shapefile
        temp_dir = tempfile.mkdtemp()
        projected_path = os.path.join(temp_dir, 'test_3310.shp')
        
        try:
            # Use ogr2ogr to reproject to EPSG:3310
            result = subprocess.run(
                ['ogr2ogr', '-f', 'ESRI Shapefile', '-t_srs', 'EPSG:3310', 
                 projected_path, wgs84_path],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                pytest.skip(f"Could not create projected test data: {result.stderr}")
            
            yield projected_path
            
        finally:
            if os.path.exists(wgs84_path):
                os.remove(wgs84_path)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def sample_nad83_shapefile(self):
        """
        Create a shapefile in EPSG:4269 (NAD83) — the CRS used by all Census TIGER files.

        Source is geographic (EPSG:4269), target is EPSG:4326; reprojection is
        triggered because they differ. Exercises the always_xy := true transform
        path, which must keep coordinates in (lon, lat) order.
        """
        import subprocess
        import json
        import shutil

        wgs84_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Area 1"},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.4, 37.8], [-122.4, 37.9],
                            [-122.3, 37.9], [-122.3, 37.8],
                            [-122.4, 37.8]
                        ]]
                    }
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='_src.geojson', delete=False) as f:
            json.dump(wgs84_geojson, f)
            src_path = f.name

        temp_dir = tempfile.mkdtemp()
        shp_path = os.path.join(temp_dir, 'test_4269.shp')

        try:
            result = subprocess.run(
                ['ogr2ogr', '-f', 'ESRI Shapefile', '-t_srs', 'EPSG:4269', shp_path, src_path],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode != 0:
                pytest.skip(f"Could not create NAD83 test shapefile: {result.stderr}")
            yield shp_path
        finally:
            if os.path.exists(src_path):
                os.remove(src_path)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def test_nad83_shapefile_to_wgs84_axis_order(self, sample_nad83_shapefile):
        """
        EPSG:4269 (NAD83) shapefile → EPSG:4326 must produce (lon, lat) = (X, Y).

        Regression test for Census TIGER data. ST_Transform is now invoked with
        always_xy := true so it reads and writes (lon, lat) regardless of authority
        axis order; the output must therefore keep longitude in X.
        """
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name

        try:
            convert_to_parquet(
                source_url=sample_nad83_shapefile,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False,
                progress=False,
            )

            con = duckdb.connect()
            con.install_extension('spatial')
            con.load_extension('spatial')

            result = con.execute(f"""
                SELECT
                    MIN(ST_XMin(geom)) as min_x,
                    MAX(ST_XMax(geom)) as max_x,
                    MIN(ST_YMin(geom)) as min_y,
                    MAX(ST_YMax(geom)) as max_y
                FROM read_parquet('{output_path}')
            """).fetchone()
            con.close()

            min_x, max_x, min_y, max_y = result

            # X must be longitude (negative for US west coast), not latitude (~37)
            assert min_x < -100, (
                f"X min is {min_x:.2f} — looks like latitude, not longitude. "
                "Axis swap bug: ST_Transform output is not (lon, lat) — check that "
                "always_xy := true is set."
            )
            assert -123 <= min_x <= -122, f"X min {min_x:.2f} not in expected SF Bay Area lon range"
            assert -123 <= max_x <= -122, f"X max {max_x:.2f} not in expected SF Bay Area lon range"
            assert 37 <= min_y <= 39, f"Y min {min_y:.2f} not in expected SF Bay Area lat range"
            assert 37 <= max_y <= 39, f"Y max {max_y:.2f} not in expected SF Bay Area lat range"

        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

    def test_geojson_coordinate_order_preserved(self):
        """
        GeoJSON inputs must NOT have coordinates flipped.

        RFC 7946 mandates (lon, lat) order for GeoJSON, and GDAL's GeoJSON driver
        honours this. Applying ST_FlipCoordinates to GeoJSON would silently corrupt
        the output. This test guards against that regression.
        """
        import json

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.4, 37.8], [-122.4, 37.9],
                            [-122.3, 37.9], [-122.3, 37.8],
                            [-122.4, 37.8]
                        ]]
                    }
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            json.dump(geojson, f)
            src_path = f.name

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name

        try:
            convert_to_parquet(
                source_url=src_path,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False,
                progress=False,
            )

            con = duckdb.connect()
            con.install_extension('spatial')
            con.load_extension('spatial')

            result = con.execute(f"""
                SELECT MIN(ST_XMin(geom)) as min_x, MIN(ST_YMin(geom)) as min_y
                FROM read_parquet('{output_path}')
            """).fetchone()
            con.close()

            min_x, min_y = result
            assert min_x < -100, (
                f"GeoJSON X min is {min_x:.2f} — looks like latitude. "
                "GeoJSON inputs are being incorrectly flipped."
            )
            assert -123 <= min_x <= -122, f"GeoJSON X min {min_x:.2f} not in expected lon range"
            assert 37 <= min_y <= 39, f"GeoJSON Y min {min_y:.2f} not in expected lat range"

        finally:
            if os.path.exists(src_path):
                os.remove(src_path)
            if os.path.exists(output_path):
                os.remove(output_path)

    @pytest.fixture
    def sample_3d_point_shapefile(self):
        """
        Create a shapefile whose geometry type is reported as '3D Point' (has Z coords).

        DuckDB's spatial extension reads M/Z geometry as BLOB rather than GEOMETRY,
        which silently breaks reprojection, hex aggregation, and PMTiles export.
        This fixture reproduces that scenario so we can verify the flatten-to-2D
        pre-processing step fixes it end-to-end.
        """
        import json
        import shutil
        import subprocess

        # GeoJSON allows Z coordinates in the coordinate arrays (RFC 7946 §3.1.1).
        # ogr2ogr will produce a "3D Point" shapefile from this input.
        geojson_3d = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "A"},
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8, 100.0]},
                },
                {
                    "type": "Feature",
                    "properties": {"name": "B"},
                    "geometry": {"type": "Point", "coordinates": [-122.5, 37.9, 200.0]},
                },
                {
                    "type": "Feature",
                    "properties": {"name": "C"},
                    "geometry": {"type": "Point", "coordinates": [-122.6, 38.0, 300.0]},
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix="_3d.geojson", delete=False) as f:
            json.dump(geojson_3d, f)
            src_path = f.name

        temp_dir = tempfile.mkdtemp()
        shp_path = os.path.join(temp_dir, "test_3d_point.shp")

        try:
            result = subprocess.run(
                ["ogr2ogr", "-f", "ESRI Shapefile", shp_path, src_path],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0:
                pytest.skip(f"Could not create 3D test shapefile: {result.stderr}")
            yield shp_path
        finally:
            if os.path.exists(src_path):
                os.remove(src_path)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def test_3d_geometry_flattened_to_2d(self, sample_3d_point_shapefile):
        """
        Shapefiles with 3D (Z) geometry must convert successfully and produce a
        proper GEOMETRY column — not BLOB — in the output GeoParquet.

        Regression test for GitHub issue #50: DuckDB's spatial extension stores
        M/Z geometry types as BLOB rather than GEOMETRY, breaking reprojection,
        hex aggregation, and PMTiles export.  The fix is to pre-process with
        ogr2ogr -dim XY before passing the source to DuckDB.
        """
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            convert_to_parquet(
                source_url=sample_3d_point_shapefile,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False,
                progress=False,
            )

            con = duckdb.connect()
            con.install_extension("spatial")
            con.load_extension("spatial")

            # Verify row count — 0 rows would indicate the BLOB/reprojection bug
            row_count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
            ).fetchone()[0]
            assert row_count == 3, (
                f"Expected 3 rows, got {row_count}. "
                "0 rows typically means the geometry was stored as BLOB and "
                "ST_Transform silently produced no output."
            )

            # Verify the geometry column is typed as GEOMETRY, not BLOB
            schema = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}')"
            ).fetchall()
            geom_col_type = next(
                (row[1] for row in schema if row[0] == "geom"), None
            )
            assert geom_col_type is not None and geom_col_type.startswith("GEOMETRY"), (
                f"Expected geom column type GEOMETRY (or GEOMETRY(...)), got {geom_col_type!r}. "
                "BLOB means 3D/measured geometry was not flattened before writing."
            )

            # Verify coordinates are in valid lon/lat range (SF Bay Area)
            result = con.execute(f"""
                SELECT
                    MIN(ST_XMin(geom)) as min_x,
                    MAX(ST_XMax(geom)) as max_x,
                    MIN(ST_YMin(geom)) as min_y,
                    MAX(ST_YMax(geom)) as max_y
                FROM read_parquet('{output_path}')
            """).fetchone()
            min_x, max_x, min_y, max_y = result
            assert -123 <= min_x <= -122, f"X min {min_x:.2f} not in expected lon range"
            assert 37 <= min_y <= 39, f"Y min {min_y:.2f} not in expected lat range"

            con.close()

        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

    def test_projected_to_wgs84_reprojection(self, sample_projected_shapefile):
        """
        Test that EPSG:3310 (meters) is correctly reprojected to EPSG:4326 (degrees).
        
        This test MUST catch projection bugs where output stays in projected meters.
        """
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            # Convert with reprojection
            convert_to_parquet(
                source_url=sample_projected_shapefile,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False
            )
            
            # Verify with DuckDB
            con = duckdb.connect()
            con.install_extension('spatial')
            con.load_extension('spatial')
            
            # Check extent
            result = con.execute(f"""
                SELECT 
                    MIN(ST_XMin(geom)) as min_x,
                    MAX(ST_XMax(geom)) as max_x,
                    MIN(ST_YMin(geom)) as min_y,
                    MAX(ST_YMax(geom)) as max_y
                FROM read_parquet('{output_path}')
            """).fetchone()
            
            min_x, max_x, min_y, max_y = result
            
            print(f"\nReprojection test extent: X=[{min_x:.2f}, {max_x:.2f}], Y=[{min_y:.2f}, {max_y:.2f}]")
            
            # CRITICAL: Verify coordinates are in degrees, not meters
            # SF Bay Area should be around lon=-122, lat=37-38
            # If in meters (EPSG:3310), would be huge numbers like X=-200000, Y=180000
            
            # Test 1: Coordinates must be in valid lat/lon range
            assert -180 <= min_x <= 180, f"X min {min_x} not in lon range [-180, 180] - likely still in meters!"
            assert -180 <= max_x <= 180, f"X max {max_x} not in lon range [-180, 180] - likely still in meters!"
            assert -90 <= min_y <= 90, f"Y min {min_y} not in lat range [-90, 90] - likely still in meters!"
            assert -90 <= max_y <= 90, f"Y max {max_y} not in lat range [-90, 90] - likely still in meters!"
            
            # Test 2: Specific SF Bay Area check (our test data location)
            assert -123 <= min_x <= -122, f"X min {min_x} not in expected SF Bay Area lon range"
            assert -123 <= max_x <= -122, f"X max {max_x} not in expected SF Bay Area lon range"
            assert 37 <= min_y <= 39, f"Y min {min_y} not in expected SF Bay Area lat range"
            assert 37 <= max_y <= 39, f"Y max {max_y} not in expected SF Bay Area lat range"
            
            # Test 3: Verify geometries are valid
            invalid_count = con.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{output_path}')
                WHERE NOT ST_IsValid(geom)
            """).fetchone()[0]
            assert invalid_count == 0, f"Found {invalid_count} invalid geometries"

            con.close()

        finally:
            if os.path.exists(output_path):
                os.remove(output_path)


class TestMultipointGeometry:
    """Regression tests for MULTIPOINT sources producing BLOB instead of GEOMETRY (#61)."""

    @pytest.fixture
    def sample_multipoint_gpkg(self):
        """
        Create a GeoPackage with MULTIPOINT geometry.

        DuckDB's ST_Read returns BLOB (not GEOMETRY) for MULTIPOINT sources, which
        causes the output parquet to lack GeoParquet metadata and breaks PMTiles export.
        """
        import shutil
        import subprocess
        import json as _json

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "A", "value": 1},
                    "geometry": {
                        "type": "MultiPoint",
                        "coordinates": [[-122.4, 37.8], [-122.5, 37.9]],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {"name": "B", "value": 2},
                    "geometry": {
                        "type": "MultiPoint",
                        "coordinates": [[-122.6, 38.0], [-122.7, 38.1]],
                    },
                },
            ],
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
            _json.dump(geojson, f)
            src_path = f.name

        tmp_dir = tempfile.mkdtemp()
        gpkg_path = os.path.join(tmp_dir, "multipoint.gpkg")

        try:
            result = subprocess.run(
                ["ogr2ogr", "-f", "GPKG", gpkg_path, src_path],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0:
                pytest.skip(f"Could not create MULTIPOINT test GeoPackage: {result.stderr}")
            yield gpkg_path
        finally:
            if os.path.exists(src_path):
                os.remove(src_path)
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

    def test_multipoint_produces_geometry_column(self, sample_multipoint_gpkg):
        """
        MULTIPOINT sources must produce a GEOMETRY column (not BLOB) in output GeoParquet.

        Regression test for #61: DuckDB's ST_Read returns BLOB for MULTIPOINT geometry;
        the fix is to detect the BLOB column and cast via ST_GeomFromWKB() so that DuckDB
        writes proper GeoParquet column metadata.
        """
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            convert_to_parquet(
                source_url=sample_multipoint_gpkg,
                destination=output_path,
                target_crs="EPSG:4326",
                verbose=False,
                progress=False,
            )

            con = duckdb.connect()
            con.install_extension("spatial")
            con.load_extension("spatial")

            # geom must be GEOMETRY, not BLOB
            schema = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}')"
            ).fetchall()
            geom_col_type = next((row[1] for row in schema if row[0] == "geom"), None)
            assert geom_col_type is not None and geom_col_type.startswith("GEOMETRY"), (
                f"Expected GEOMETRY column, got {geom_col_type!r}. "
                "BLOB means GeoParquet metadata is missing and PMTiles export will fail."
            )

            # Correct feature count
            row_count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
            ).fetchone()[0]
            assert row_count == 2, f"Expected 2 rows, got {row_count}"

            # Geometry type round-trips correctly
            geom_types = {
                row[0]
                for row in con.execute(
                    f"SELECT DISTINCT ST_GeometryType(geom) FROM read_parquet('{output_path}')"
                ).fetchall()
            }
            assert geom_types == {"MULTIPOINT"}, f"Unexpected geometry types: {geom_types}"

            con.close()
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

    def test_parquet_input_blob_geometry_cast_to_geometry(self):
        """A parquet input whose geom is a BLOB-typed WKB column must come out as
        GEOMETRY with GeoParquet metadata (issue #61).

        The ST_Read path was fixed in #75, but the parquet-input path
        (process_parquet_input) passed BLOB geometry through unchanged, so a
        MULTIPOINT-derived parquet stayed BLOB and broke PMTiles. No ogr2ogr
        needed — the BLOB-geom source is built directly with DuckDB.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "blob_geom.parquet")
            output_path = os.path.join(tmpdir, "out.parquet")

            con = duckdb.connect()
            con.install_extension("spatial")
            con.load_extension("spatial")
            # ST_AsWKB yields a BLOB column — the exact shape ST_Read emits for
            # MULTIPOINT sources before the cast.
            con.execute(f"""
                COPY (
                    SELECT 'A' AS name,
                           ST_AsWKB(ST_GeomFromText('MULTIPOINT((-122.4 37.8),(-122.5 37.9))')) AS geom
                    UNION ALL
                    SELECT 'B' AS name,
                           ST_AsWKB(ST_GeomFromText('MULTIPOINT((-122.6 38.0),(-122.7 38.1))')) AS geom
                ) TO '{source}' (FORMAT PARQUET)
            """)
            assert con.execute(
                f"SELECT typeof(geom) FROM read_parquet('{source}') LIMIT 1"
            ).fetchone()[0] == "BLOB", "fixture precondition: input geom must be BLOB"
            con.close()

            convert_to_parquet(
                source_url=source,
                destination=output_path,
                force_id=True,
                progress=False,
            )

            con = duckdb.connect()
            con.install_extension("spatial")
            con.load_extension("spatial")
            schema = con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}')"
            ).fetchall()
            geom_col_type = next((row[1] for row in schema if row[0] == "geom"), None)
            assert geom_col_type is not None and geom_col_type.startswith("GEOMETRY"), (
                f"Expected GEOMETRY column, got {geom_col_type!r}"
            )
            geom_types = {
                row[0] for row in con.execute(
                    f"SELECT DISTINCT ST_GeometryType(geom) FROM read_parquet('{output_path}')"
                ).fetchall()
            }
            assert geom_types == {"MULTIPOINT"}, f"Unexpected geometry types: {geom_types}"
            con.close()

            # GeoParquet metadata (the 'geo' key) must be present.
            import pyarrow.parquet as pq
            metadata = pq.read_table(output_path).schema.metadata or {}
            assert b"geo" in metadata, "Output must carry GeoParquet 'geo' metadata"

    def test_geoarrow_native_input_raises_clear_error(self):
        """A geoarrow-native-encoded parquet (geometry as STRUCT, not WKB) must
        raise a clear, actionable error rather than silently passing the
        non-GEOMETRY column through with no GeoParquet metadata (issue #119).
        """
        import geopandas as gpd
        from shapely.geometry import Polygon

        with tempfile.TemporaryDirectory() as tmpdir:
            source = os.path.join(tmpdir, "geoarrow_native.parquet")
            output_path = os.path.join(tmpdir, "out.parquet")

            gdf = gpd.GeoDataFrame(
                {"name": ["a", "b"]},
                geometry=[
                    Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)]),
                    Polygon([(-121.5, 38.7), (-121.4, 38.7), (-121.4, 38.8), (-121.5, 38.8)]),
                ],
                crs="EPSG:4326",
            )
            gdf.to_parquet(source, geometry_encoding="geoarrow")

            # Precondition: DuckDB sees the geometry as a STRUCT, not GEOMETRY/BLOB.
            con = duckdb.connect()
            con.install_extension("spatial")
            con.load_extension("spatial")
            geom_type = next(
                row[1] for row in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{source}')"
                ).fetchall() if row[0] == "geometry"
            )
            con.close()
            assert geom_type.upper().startswith("STRUCT"), (
                f"fixture precondition: expected STRUCT geom, got {geom_type}"
            )

            with pytest.raises(ValueError, match="DuckDB cannot ingest directly"):
                convert_to_parquet(
                    source_url=source,
                    destination=output_path,
                    force_id=True,
                    progress=False,
                )
            # Nothing should have been written.
            assert not os.path.exists(output_path)
