"""Unit tests for convert_to_parquet with GeoParquet validation."""

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
            df = con.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            
            assert '_cng_fid' in df.columns, "Expected _cng_fid column"
            assert len(df) == 3, "Expected 3 features"
            assert list(df['_cng_fid']) == [1, 2, 3], "Expected sequential IDs"
            
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
            df = con.execute(f"SELECT * FROM read_parquet('{output_path}')").df()
            
            assert 'fid' in df.columns, "Expected existing fid column to be preserved"
            assert '_cng_fid' not in df.columns, "Should not add _cng_fid when fid exists"
            assert len(df) == 2, "Expected 2 features"
            
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
