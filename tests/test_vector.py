"""
Integration tests for vector processing.
"""

import pytest
import duckdb
import tempfile
from pathlib import Path

from cng_datasets.vector.h3_tiling import geom_to_h3_cells, setup_duckdb_connection


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
    
    @pytest.mark.skip(reason="Requires S3 access and takes time")
    def test_process_vector_chunks_local(self):
        """Test processing vector data to chunks (requires test data)."""
        # This would need test data setup
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
