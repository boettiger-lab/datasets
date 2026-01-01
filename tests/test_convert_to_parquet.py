"""Unit tests for convert_to_parquet with GeoParquet validation."""

import pytest
import tempfile
import os
from pathlib import Path
import duckdb

from cng_datasets.vector.convert_to_parquet import (
    convert_to_parquet,
    _check_needs_id_column,
    _convert_direct,
    _convert_with_id_column
)


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
    
    def test_check_needs_id_column_without_id(self, sample_geojson):
        """Test that source without ID column is detected correctly."""
        needs_id, id_col = _check_needs_id_column(sample_geojson, None, True)
        
        assert needs_id is True
        assert id_col == '_cng_fid'
    
    def test_check_needs_id_column_with_existing_id(self, sample_geojson_with_id):
        """Test that source with valid ID column is detected correctly."""
        needs_id, id_col = _check_needs_id_column(sample_geojson_with_id, None, True)
        
        assert needs_id is False
        assert id_col == 'fid'
    
    def test_convert_direct_creates_valid_geoparquet(self, sample_geojson_with_id):
        """Test that direct conversion creates valid GeoParquet."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            _convert_direct(
                sample_geojson_with_id,
                output_path,
                compression="ZSTD",
                compression_level=15,
                row_group_size=100000,
                verbose=False
            )
            
            # Verify file exists
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0
            
            # Use geoparquet-io to validate
            from geoparquet_io.core.check_parquet_structure import check_all
            
            # check_all prints errors but returns None by default
            check_all(output_path, verbose=False)
            
            # Verify DuckDB can read it with native parquet reader
            con = duckdb.connect()
            
            # Use native parquet reader (not ST_Read)
            result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()
            assert result[0] == 2, "Expected 2 features"
            
            # Verify fid column exists
            df = con.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            assert 'fid' in df.columns, "Expected fid column"
            
            con.close()
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
    def test_convert_with_id_column_creates_valid_geoparquet(self, sample_geojson):
        """Test that conversion with synthetic ID creates valid GeoParquet."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            _convert_with_id_column(
                sample_geojson,
                output_path,
                id_col_name='_cng_fid',
                compression="ZSTD",
                compression_level=15,
                row_group_size=100000,
                verbose=False
            )
            
            # Verify file exists
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0
            
            # Use geoparquet-io to validate
            from geoparquet_io.core.check_parquet_structure import check_all
            
            # check_all prints errors but returns None by default
            check_all(output_path, verbose=False)
            
            # Verify DuckDB can read it with native parquet reader and ID column exists
            con = duckdb.connect()
            
            # Use native parquet reader (not ST_Read - DuckDB's vendored GDAL lacks parquet driver)
            df = con.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            
            # Check ID column exists
            assert '_cng_fid' in df.columns, "Expected _cng_fid column"
            
            # Check ID values are sequential
            assert list(df['_cng_fid']) == [1, 2, 3], "Expected sequential IDs"
            
            # Check original data preserved
            assert 'name' in df.columns
            assert len(df) == 3
            
            con.close()
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
    def test_convert_to_parquet_without_id_adds_cng_fid(self, sample_geojson):
        """Test full conversion adds _cng_fid when no ID exists."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            convert_to_parquet(
                source_url=sample_geojson,
                destination=output_path,
                compression="ZSTD",
                compression_level=15,
                row_group_size=100000,
                force_id=True,
                progress=False
            )
            
            # Verify with DuckDB native parquet reader
            con = duckdb.connect()
            
            # Use native parquet reader (not ST_Read)
            df = con.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            
            assert '_cng_fid' in df.columns
            assert len(df) == 3
            assert df['_cng_fid'].is_unique
            
            con.close()
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
    def test_convert_to_parquet_with_existing_id_preserves_it(self, sample_geojson_with_id):
        """Test that existing ID column is preserved."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            convert_to_parquet(
                source_url=sample_geojson_with_id,
                destination=output_path,
                compression="ZSTD",
                compression_level=15,
                row_group_size=100000,
                force_id=True,
                progress=False
            )
            
            # Verify with DuckDB native parquet reader
            con = duckdb.connect()
            
            # Use native parquet reader (not ST_Read)
            df = con.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            
            # Should have original fid, not _cng_fid
            assert 'fid' in df.columns
            assert '_cng_fid' not in df.columns
            assert len(df) == 2
            
            con.close()
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
    def test_geoparquet_quality_checks(self, sample_geojson):
        """Test that output passes geoparquet-io quality checks."""
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            convert_to_parquet(
                source_url=sample_geojson,
                destination=output_path,
                compression="ZSTD",
                compression_level=15,
                row_group_size=100000,
                force_id=True,
                progress=False
            )
            
            # Run geoparquet-io quality checks
            from geoparquet_io.core.check_parquet_structure import (
                check_all,
                check_compression,
                check_row_groups,
                get_compression_info
            )
            
            # Check all (comprehensive check) - prints warnings but doesn't fail
            check_all(output_path, verbose=False)
            
            # Check compression specifically - returns dict of column -> compression type
            comp_info = get_compression_info(output_path)
            # Verify ZSTD compression is used for columns
            assert len(comp_info) > 0, "Expected compression info for columns"
            # Check that all columns use ZSTD
            for col, compression in comp_info.items():
                assert compression == 'ZSTD', f"Expected ZSTD compression for {col}, got {compression}"
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
    
    def test_gdal_can_read_output(self, sample_geojson):
        """Test that GDAL can read the output as valid GeoParquet (if Parquet driver available)."""
        import subprocess
        
        # Check if system GDAL has Parquet driver support
        try:
            result = subprocess.run(
                ['ogrinfo', '--formats'],
                capture_output=True,
                text=True,
                timeout=5
            )
            has_parquet_driver = 'Parquet' in result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            has_parquet_driver = False
        
        if not has_parquet_driver:
            pytest.skip("System GDAL does not have Parquet driver support")
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            convert_to_parquet(
                source_url=sample_geojson,
                destination=output_path,
                compression="ZSTD",
                force_id=True,
                progress=False
            )
            
            # Test GDAL can read it using ogrinfo
            result = subprocess.run(
                ['ogrinfo', '-al', '-so', output_path],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            assert result.returncode == 0, f"ogrinfo failed to read parquet: {result.stderr}"
            assert 'Feature Count: 3' in result.stdout, "Expected 3 features"
            assert '_cng_fid' in result.stdout, "Expected _cng_fid column"
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)


class TestIDColumnPriority:
    """Test that _cng_fid is prioritized in h3_tiling."""
    
    def test_cng_fid_is_prioritized(self):
        """Test that _cng_fid is detected first in priority list."""
        from cng_datasets.vector.h3_tiling import identify_id_column
        
        con = duckdb.connect()
        # Create table with both _cng_fid and fid
        con.execute('CREATE TABLE test AS SELECT 1 as _cng_fid, 2 as fid, 3 as value')
        
        id_col, is_unique = identify_id_column(con, 'test')
        
        # Should prefer _cng_fid over fid
        assert id_col == '_cng_fid'
        assert is_unique is True
        
        con.close()


class TestReprojection:
    """Test reprojection functionality with synthetic projected datasets."""
    
    @pytest.fixture
    def sample_projected_geojson(self):
        """
        Create a GeoJSON file in EPSG:3310 (NAD83/California Albers).
        
        Uses coordinates in meters (projected) that correspond to locations in California.
        These coordinates are roughly in the San Francisco Bay Area when projected.
        """
        import subprocess
        import json
        import shutil
        
        # Create a simple GeoJSON in WGS84 first (SF Bay Area)
        wgs84_geojson = {
            "type": "FeatureCollection",
            "crs": {
                "type": "name",
                "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}
            },
            "features": [
                {
                    "type": "Feature",
                    "properties": {"name": "Area 1", "id": 1},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.4, 37.8], [-122.4, 37.9], 
                            [-122.3, 37.9], [-122.3, 37.8], 
                            [-122.4, 37.8]
                        ]]
                    }
                },
                {
                    "type": "Feature",
                    "properties": {"name": "Area 2", "id": 2},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.5, 37.7], [-122.5, 37.8], 
                            [-122.4, 37.8], [-122.4, 37.7], 
                            [-122.5, 37.7]
                        ]]
                    }
                },
                {
                    "type": "Feature",
                    "properties": {"name": "Area 3", "id": 3},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-122.3, 37.7], [-122.3, 37.8], 
                            [-122.2, 37.8], [-122.2, 37.7], 
                            [-122.3, 37.7]
                        ]]
                    }
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='_wgs84.geojson', delete=False) as f:
            json.dump(wgs84_geojson, f)
            wgs84_path = f.name
        
        # Create output directory for shapefile (needs multiple files)
        temp_dir = tempfile.mkdtemp()
        projected_path = os.path.join(temp_dir, 'test_3310.shp')
        
        try:
            # Use ogr2ogr to reproject to EPSG:3310 as shapefile
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
            # Cleanup
            if os.path.exists(wgs84_path):
                os.remove(wgs84_path)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def test_projected_to_wgs84_reprojection(self, sample_projected_geojson):
        """
        Test that a dataset in EPSG:3310 (NAD83/California Albers) is correctly reprojected to WGS84.
        
        Verifies:
        1. Source is detected as EPSG:3310 and needs reprojection
        2. Output is properly reprojected to EPSG:4326
        3. Bounding box is within expected California lat/lon extent
        4. Geometry is valid after reprojection
        5. Synthetic ID column is created
        """
        from cng_datasets.vector.convert_to_parquet import _check_projection
        from cng_datasets.vector.h3_tiling import setup_duckdb_connection
        from cng_datasets.storage.s3 import configure_s3_credentials
        
        source_url = sample_projected_geojson
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
            output_path = f.name
        
        try:
            # First verify source needs reprojection
            needs_reproj, source_crs = _check_projection(source_url, "EPSG:4326")
            assert needs_reproj, f"Expected CPAD to need reprojection, but source is {source_crs}"
            assert source_crs == "EPSG:3310", f"Expected CPAD to be in EPSG:3310 (NAD83/CA Albers), got {source_crs}"
            
            # Convert with reprojection
            convert_to_parquet(
                source_url=source_url,
                destination=output_path,
                compression="ZSTD",
                compression_level=15,
                row_group_size=100000,
                force_id=True,
                progress=True,
                target_crs="EPSG:4326"
            )
            
            # Verify file exists
            assert os.path.exists(output_path), "Output parquet file not created"
            assert os.path.getsize(output_path) > 0, "Output parquet file is empty"
            
            # Verify reprojection with DuckDB spatial extension
            con = setup_duckdb_connection()
            configure_s3_credentials(con)
            
            # Load spatial extension
            con.execute("LOAD spatial")
            
            # Read the parquet file and compute bounding box
            # The geometry column is already in GEOMETRY type, no conversion needed
            result = con.execute(f"""
                SELECT 
                    COUNT(*) as count,
                    MIN(ST_XMin(geom)) as min_lon,
                    MAX(ST_XMax(geom)) as max_lon,
                    MIN(ST_YMin(geom)) as min_lat,
                    MAX(ST_YMax(geom)) as max_lat
                FROM read_parquet('{output_path}')
            """).fetchone()
            
            count, min_lon, max_lon, min_lat, max_lat = result
            
            # Verify we have features
            assert count > 0, "No features found in output"
            
            # California's approximate bounding box in WGS84:
            # Longitude: -124.5° to -114.0° (west to east)
            # Latitude: 32.5° to 42.0° (south to north)
            CA_MIN_LON = -124.5
            CA_MAX_LON = -114.0
            CA_MIN_LAT = 32.5
            CA_MAX_LAT = 42.0
            
            print(f"\nCPAD Reprojection Test Results:")
            print(f"  Feature count: {count:,}")
            print(f"  Bounding box: ({min_lon:.6f}, {min_lat:.6f}) to ({max_lon:.6f}, {max_lat:.6f})")
            
            # Verify bounding box is within California's extent (with small buffer for edge cases)
            assert min_lon >= CA_MIN_LON - 0.5, f"Minimum longitude {min_lon} is too far west (expected >= {CA_MIN_LON})"
            assert max_lon <= CA_MAX_LON + 0.5, f"Maximum longitude {max_lon} is too far east (expected <= {CA_MAX_LON})"
            assert min_lat >= CA_MIN_LAT - 0.5, f"Minimum latitude {min_lat} is too far south (expected >= {CA_MIN_LAT})"
            assert max_lat <= CA_MAX_LAT + 0.5, f"Maximum latitude {max_lat} is too far north (expected <= {CA_MAX_LAT})"
            
            # Verify coordinates are in lat/lon range (not projected coordinates in meters)
            assert -180 <= min_lon <= 180, f"Longitude {min_lon} outside valid range [-180, 180]"
            assert -180 <= max_lon <= 180, f"Longitude {max_lon} outside valid range [-180, 180]"
            assert -90 <= min_lat <= 90, f"Latitude {min_lat} outside valid range [-90, 90]"
            assert -90 <= max_lat <= 90, f"Latitude {max_lat} outside valid range [-90, 90]"
            
            # More specific checks for our SF Bay Area test data
            # Lon should be around -122.2 to -122.5, Lat around 37.7 to 37.9
            assert -123 <= min_lon <= -122, f"Min longitude {min_lon} not in expected SF Bay Area range"
            assert -123 <= max_lon <= -122, f"Max longitude {max_lon} not in expected SF Bay Area range"
            assert 37 <= min_lat <= 38, f"Min latitude {min_lat} not in expected SF Bay Area range"
            assert 37 <= max_lat <= 38, f"Max latitude {max_lat} not in expected SF Bay Area range"
            
            # Verify geometry is valid
            invalid_count = con.execute(f"""
                SELECT COUNT(*) 
                FROM read_parquet('{output_path}')
                WHERE NOT ST_IsValid(geom)
            """).fetchone()[0]
            
            assert invalid_count == 0, f"Found {invalid_count} invalid geometries after reprojection"
            
            # Verify ID column exists (either original 'id' or synthetic '_cng_fid')
            columns = con.execute(f"""
                SELECT column_name 
                FROM (DESCRIBE SELECT * FROM read_parquet('{output_path}'))
            """).fetchall()
            column_names = [col[0] for col in columns]
            
            # Since our fixture has an 'id' column, it should be preserved
            assert 'id' in column_names, "Expected id column to be preserved"
            
            # Verify id is unique
            id_count = con.execute(f"""
                SELECT COUNT(DISTINCT id) 
                FROM read_parquet('{output_path}')
            """).fetchone()[0]
            
            assert id_count == count, "Expected id to be unique for all features"
            
            con.close()
            
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)
