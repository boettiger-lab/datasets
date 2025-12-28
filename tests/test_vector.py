"""
Integration tests for vector processing.
"""

import pytest
import duckdb
import tempfile
from pathlib import Path
import os

from cng_datasets.vector.h3_tiling import geom_to_h3_cells, setup_duckdb_connection, H3VectorProcessor


class TestS3Connection:
    """Test S3 connection and credential configuration."""
    
    def test_s3_public_read(self):
        """Test reading from public S3 bucket without credentials."""
        # Clear any existing AWS env vars to test anonymous access
        old_key = os.environ.get("AWS_ACCESS_KEY_ID")
        old_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
        
        try:
            os.environ["AWS_ACCESS_KEY_ID"] = ""
            os.environ["AWS_SECRET_ACCESS_KEY"] = ""
            os.environ["AWS_S3_ENDPOINT"] = "s3-west.nrp-nautilus.io"
            os.environ["AWS_HTTPS"] = "TRUE"
            
            processor = H3VectorProcessor(
                input_url="s3://public-mappinginequality/mappinginequality.parquet",
                output_url="/tmp/test_output",
                h3_resolution=10,
                chunk_size=10
            )
            
            # Should be able to read metadata without errors
            result = processor.con.execute(
                "SELECT COUNT(*) as cnt FROM read_parquet('s3://public-mappinginequality/mappinginequality.parquet')"
            ).fetchone()
            
            assert result[0] > 0, "Should be able to read from public bucket"
            
        finally:
            # Restore original env vars
            if old_key is not None:
                os.environ["AWS_ACCESS_KEY_ID"] = old_key
            if old_secret is not None:
                os.environ["AWS_SECRET_ACCESS_KEY"] = old_secret


class TestH3Functions:
    """Test H3 utility functions."""
    
    def test_setup_duckdb_connection(self):
        """Test DuckDB connection setup with H3 extension."""
        con = setup_duckdb_connection()
        
        # Verify H3 extension is loaded
        result = con.execute("""
            SELECT h3_latlng_to_cell(37.7749, -122.4194, 10)::UBIGINT as h3_cell
        """).fetchone()
        
        assert result[0] > 0  # Should return a valid H3 cell ID
        con.close()
    
    def test_geom_to_h3_cells_simple(self):
        """Test converting geometry to H3 cells."""
        con = setup_duckdb_connection()
        
        # Create simple test data with a point geometry using ST_GeomFromText
        con.execute("""
            CREATE TABLE test_geom AS 
            SELECT 
                1 as id,
                ST_GeomFromText('POINT(-122.4194 37.7749)') as geom
        """)
        
        # Generate H3 cells
        sql = geom_to_h3_cells(con, "test_geom", zoom=10)
        
        # Execute and check result
        result = con.execute(f"SELECT * FROM ({sql})").fetchdf()
        
        assert len(result) > 0
        assert 'h3id' in result.columns
        assert 'id' in result.columns
        
        con.close()
    
    def test_geom_to_h3_cells_polygon(self):
        """Test converting polygon to H3 cells."""
        con = setup_duckdb_connection()
        
        # Create a small polygon
        con.execute("""
            CREATE TABLE test_poly AS 
            SELECT 
                1 as id,
                'test' as name,
                ST_GeomFromText('POLYGON((
                    -122.5 37.7, 
                    -122.4 37.7, 
                    -122.4 37.8, 
                    -122.5 37.8, 
                    -122.5 37.7
                ))') as geom
        """)
        
        sql = geom_to_h3_cells(con, "test_poly", zoom=8, keep_cols=['id', 'name'])
        result = con.execute(f"SELECT * FROM ({sql})").fetchdf()
        
        assert len(result) > 0
        assert 'h3id' in result.columns
        assert 'id' in result.columns
        assert 'name' in result.columns
        assert result['name'].iloc[0] == 'test'
        
        con.close()
    
    def test_h3_parent_resolution(self):
        """Test H3 parent cell calculation."""
        con = setup_duckdb_connection()
        
        # Get a cell and its parent
        result = con.execute("""
            WITH cell AS (
                SELECT h3_latlng_to_cell(37.7749, -122.4194, 10)::UBIGINT as h10
            )
            SELECT 
                h10,
                h3_cell_to_parent(h10, 9)::UBIGINT as h9,
                h3_cell_to_parent(h10, 8)::UBIGINT as h8,
                h3_cell_to_parent(h10, 0)::UBIGINT as h0
            FROM cell
        """).fetchdf()
        
        assert result['h10'].iloc[0] > 0
        assert result['h9'].iloc[0] > 0
        assert result['h8'].iloc[0] > 0
        assert result['h0'].iloc[0] > 0
        
        # Parent cells should be different from child
        assert result['h10'].iloc[0] != result['h9'].iloc[0]
        
        con.close()


class TestVectorProcessing:
    """Test complete vector processing workflows."""
    
    def test_h3_vector_processor_init(self):
        """Test H3VectorProcessor initialization without credentials."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test geoparquet file
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    1 as id,
                    'feature1' as name,
                    ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as geom
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            # Test initialization with empty credentials (for read-only public access)
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                chunk_size=100
            )
            
            assert processor.input_url == test_parquet
            assert processor.output_url == tmpdir
            assert processor.h3_resolution == 8
            assert processor.chunk_size == 100
            
            processor.con.close()
    
    def test_h3_vector_processor_process_chunk(self):
        """Test processing a single chunk of vector data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test geoparquet file with multiple features
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    ROW_NUMBER() OVER () as id,
                    'feature' || ROW_NUMBER() OVER () as name,
                    ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as geom
                FROM range(5)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            # Test processing with empty credentials
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=5
            )
            
            # Process chunk 0
            output_file = processor.process_chunk(0)
            
            assert output_file is not None
            assert Path(output_file).exists()
            assert output_file == f"{tmpdir}/chunk_000000.parquet"
            
            # Verify output has expected columns
            result_con = setup_duckdb_connection()
            result = result_con.execute(f"SELECT * FROM read_parquet('{output_file}')").fetchdf()
            
            assert 'h8' in result.columns
            assert 'h0' in result.columns
            assert 'id' in result.columns
            assert 'name' in result.columns
            assert len(result) > 0
            
            result_con.close()
            processor.con.close()
    
    def test_h3_vector_processor_out_of_range(self):
        """Test that out of range chunk_id returns None."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    1 as id,
                    ST_GeomFromText('POINT(-122.4194 37.7749)') as geom
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                chunk_size=5
            )
            
            # Try chunk 100 which doesn't exist
            output_file = processor.process_chunk(100)
            assert output_file is None
            
            processor.con.close()
    
    @pytest.mark.skip(reason="Requires S3 access and takes time")
    def test_process_vector_chunks_s3(self):
        """Test processing vector data from S3 (requires credentials)."""
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
