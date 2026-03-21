"""Unit tests for convert_to_parquet with GeoParquet validation."""

import json
import pytest
import tempfile
import os
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from cng_datasets.vector.convert_to_parquet import convert_to_parquet, apply_geoparquet_optimizations


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
    
    def test_convert_preserves_existing_id_column(self, sample_geojson_with_id):
        """Test that conversion preserves existing fid column."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            convert_to_parquet(
                source_url=sample_geojson_with_id,
                destination=output_path,
                force_id=True,
                progress=False
            )
            
            # Verify file exists and has the existing fid column
            con = duckdb.connect()
            cols = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{output_path}')").fetchdf()['column_name'].tolist()
            count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]

            assert 'fid' in cols, "Expected existing fid column to be preserved"
            assert '_cng_fid' not in cols, "Should not add _cng_fid when fid exists"
            assert count == 2, "Expected 2 features"

            con.close()
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
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

        This is the real bug scenario: source is geographic (EPSG:4269), target is
        EPSG:4326. reprojection is triggered because they differ, and the previous code
        incorrectly applied ST_FlipCoordinates for all geographic targets — including
        geographic→geographic transforms where ST_Transform does NOT swap axes.
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

        This is the regression test for Census TIGER data. The bug: the old code applied
        ST_FlipCoordinates for any geographic target CRS, but geographic→geographic
        ST_Transform does NOT swap axes. The flip incorrectly put latitude values in X.
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
                "Axis swap bug: ST_FlipCoordinates is being applied to a "
                "geographic→geographic transform that doesn't need it."
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


class TestMGeometryBboxFix:
    """
    Regression tests for issue #50: bbox step fails when DuckDB omits GeoParquet
    metadata for Measured (M) geometry types (e.g. 3D Measured MultiPoint).

    When the parquet file has a geometry column named 'geom' but no GeoParquet
    metadata, find_primary_geometry_column() falls back to 'geometry', causing
    a Binder Error in the bbox SQL.  apply_geoparquet_optimizations(geom_col=...)
    must override this auto-detection.
    """

    def _make_parquet_with_wrong_primary_column(self, path: str) -> None:
        """
        Write a parquet file with a GEOMETRY column called 'geom' but GeoParquet
        metadata that incorrectly sets primary_column to 'geometry'.

        This simulates what happens for M-geometry shapefiles where DuckDB writes
        GeoParquet metadata but with the wrong primary_column value, causing
        find_primary_geometry_column() to return 'geometry' instead of 'geom'.
        """
        import struct

        con = duckdb.connect()
        con.install_extension("spatial")
        con.load_extension("spatial")

        # Use a tiny GeoJSON string to get a real GEOMETRY-typed column named 'geom'
        geojson = '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"id":1},"geometry":{"type":"Point","coordinates":[-122.4,37.8]}}]}'
        with tempfile.NamedTemporaryFile(mode="w", suffix=".geojson", delete=False) as f:
            f.write(geojson)
            gjson_path = f.name

        try:
            # Write via DuckDB — this produces correct GeoParquet metadata (primary_column='geom')
            con.execute(f"""
                COPY (SELECT * FROM ST_Read('{gjson_path}'))
                TO '{path}' (FORMAT PARQUET)
            """)
        finally:
            os.remove(gjson_path)
            con.close()

        # Patch the GeoParquet metadata to simulate the bug:
        # change primary_column from 'geom' to 'geometry' (wrong name)
        table = pq.read_table(path)
        existing_meta = table.schema.metadata or {}
        if b"geo" in existing_meta:
            geo = json.loads(existing_meta[b"geo"])
        else:
            geo = {"version": "1.1.0", "columns": {}}
        # Simulate wrong primary_column — this is what triggers the Binder Error
        geo["primary_column"] = "geometry"
        new_meta = dict(existing_meta)
        new_meta[b"geo"] = json.dumps(geo).encode()
        table = table.replace_schema_metadata(new_meta)
        pq.write_table(table, path)

    def test_apply_geoparquet_optimizations_with_geom_col_override(self):
        """
        apply_geoparquet_optimizations(geom_col='geom') must succeed on a parquet
        file whose geometry column is 'geom' but has no GeoParquet metadata.

        Without the fix, find_primary_geometry_column falls back to 'geometry' and
        the bbox SQL raises: Binder Error: Referenced column "geometry" not found.
        """
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            input_path = f.name
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            self._make_parquet_with_wrong_primary_column(input_path)

            # This must not raise — the geom_col override bypasses wrong auto-detection
            apply_geoparquet_optimizations(input_path, output_path, geom_col="geom")

            # Verify output has a bbox column (the critical result of the fix)
            out_schema = pq.read_schema(output_path)
            assert "bbox" in out_schema.names, (
                "Output parquet must have a 'bbox' column after optimization"
            )

            # Verify the geometry column is still present
            assert "geom" in out_schema.names, (
                "Output must still contain the 'geom' geometry column"
            )

        finally:
            for p in (input_path, output_path):
                if os.path.exists(p):
                    os.remove(p)

    def test_apply_geoparquet_optimizations_without_override_fails_on_wrong_metadata(self):
        """
        Without geom_col override, optimizations on a file whose GeoParquet metadata
        has primary_column='geometry' but the actual column is 'geom' should raise
        a Binder Error (pre-fix behavior documented here as regression guard).
        """
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            input_path = f.name
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            self._make_parquet_with_wrong_primary_column(input_path)

            # Without the override, geoparquet_io reads primary_column='geometry'
            # from the (patched) metadata and emits SQL referencing column 'geometry',
            # which does not exist → Binder Error.
            with pytest.raises(Exception, match="geometry|Binder|not found"):
                apply_geoparquet_optimizations(input_path, output_path)

        finally:
            for p in (input_path, output_path):
                if os.path.exists(p):
                    os.remove(p)
