"""Unit tests for convert_to_parquet with GeoParquet validation."""

import json
import pytest
import tempfile
import os
from pathlib import Path
import duckdb
from cng_datasets.vector.convert_to_parquet import convert_to_parquet


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
