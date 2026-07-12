"""Unit tests for vector processing."""

import pytest
import duckdb
import tempfile
from pathlib import Path
import os
from unittest.mock import patch

from cng_datasets.vector.h3_tiling import geom_to_h3_cells, setup_duckdb_connection, H3VectorProcessor, identify_id_column, parse_resolution_by_area
from cng_datasets.vector.repartition import repartition_by_h0
from cng_datasets.hex_checks import assert_h3_columns_unsigned
from cng_datasets.storage.s3 import configure_s3_credentials


class TestIDColumnIdentification:
    """Test ID column identification logic."""
    
    def test_standard_fid_column(self):
        """Test detection of standard FID column."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as FID, 2 as value')
        
        id_col, is_unique = identify_id_column(con, 'test')
        assert id_col == 'FID'
        assert is_unique is True
        con.close()
    
    def test_case_insensitive_matching(self):
        """Test case-insensitive column matching."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as ObjectID, 2 as value')
        
        id_col, is_unique = identify_id_column(con, 'test')
        assert id_col == 'ObjectID'
        assert is_unique is True
        con.close()
    
    def test_no_standard_id_column(self):
        """Test handling when no standard ID column exists."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as MY_ID, 2 as value')
        
        id_col, is_unique = identify_id_column(con, 'test')
        assert id_col is None
        assert is_unique is False
        con.close()
    
    def test_non_unique_id_auto_detected(self):
        """Test warning for non-unique auto-detected ID column."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as fid, 2 as value UNION ALL SELECT 1 as fid, 3 as value')
        
        # Auto-detected non-unique ID should warn but not raise
        id_col, is_unique = identify_id_column(con, 'test')
        assert id_col == 'fid'
        assert is_unique is False
        con.close()
    
    def test_non_unique_id_user_specified(self):
        """Test error for non-unique user-specified ID column."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as my_id, 2 as value UNION ALL SELECT 1 as my_id, 3 as value')
        
        # User-specified non-unique ID should raise error
        with pytest.raises(ValueError, match="only.*unique values"):
            identify_id_column(con, 'test', specified_id_col='my_id')
        con.close()
    
    def test_user_specified_column(self):
        """Test using a user-specified ID column."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as MY_ID, 2 as value')
        
        id_col, is_unique = identify_id_column(con, 'test', specified_id_col='MY_ID')
        assert id_col == 'MY_ID'
        assert is_unique is True
        con.close()
    
    def test_user_specified_column_case_insensitive(self):
        """Test case-insensitive matching for user-specified column."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as MyColumn, 2 as value')
        
        # Should find 'MyColumn' even when user specifies 'mycolumn'
        id_col, is_unique = identify_id_column(con, 'test', specified_id_col='mycolumn')
        assert id_col == 'MyColumn'
        assert is_unique is True
        con.close()
    
    def test_user_specified_nonexistent_column(self):
        """Test error when user specifies a column that doesn't exist."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as id, 2 as value')
        
        with pytest.raises(ValueError, match="not found in table"):
            identify_id_column(con, 'test', specified_id_col='nonexistent')
        con.close()
    
    def test_skip_uniqueness_check(self):
        """Test skipping uniqueness validation for performance."""
        con = duckdb.connect()
        con.execute('CREATE TABLE test AS SELECT 1 as fid, 2 as value UNION ALL SELECT 1 as fid, 3 as value')
        
        # Should not check uniqueness when disabled
        id_col, is_unique = identify_id_column(con, 'test', check_uniqueness=False)
        assert id_col == 'fid'
        assert is_unique is True  # Returns True when check is skipped
        con.close()
    
    def test_common_id_column_priority(self):
        """Test that common ID columns are checked in priority order."""
        con = duckdb.connect()
        # Table with multiple possible ID columns - should pick 'fid' first
        con.execute('CREATE TABLE test AS SELECT 1 as fid, 2 as id, 3 as uid, 4 as value')
        
        id_col, is_unique = identify_id_column(con, 'test')
        assert id_col == 'fid'  # Should pick 'fid' first in priority list
        con.close()


class TestS3Connection:
    """Test S3 connection and credential configuration."""
    
    @pytest.mark.timeout(5)
    def test_s3_credential_configuration(self):
        """Test that S3 credentials are properly configured for anonymous public access (smoke test)."""
        with patch.dict(os.environ, {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_S3_ENDPOINT": "s3-west.nrp-nautilus.io",
            "AWS_HTTPS": "TRUE",
        }):
            con = setup_duckdb_connection()
            
            # Test the function we're actually testing
            configure_s3_credentials(con)
            
            # Verify the secret was created (this is just a smoke test, doesn't verify it works)
            result = con.execute("SELECT name, type FROM duckdb_secrets()").fetchall()
            secret_names = [row[0] for row in result]
            assert 's3_secret' in secret_names, "S3 secret should be created"
            
            con.close()
    
    @pytest.mark.timeout(30)
    @pytest.mark.integration
    def test_s3_network_connection(self):
        """Test that we can actually connect to S3 and read metadata (no processing)."""
        with patch.dict(os.environ, {
            "AWS_ACCESS_KEY_ID": "",
            "AWS_SECRET_ACCESS_KEY": "",
            "AWS_S3_ENDPOINT": "s3-west.nrp-nautilus.io",
            "AWS_HTTPS": "TRUE",
        }):
            con = setup_duckdb_connection()
            configure_s3_credentials(con)
            
            try:
                # Just read row count - tests network connection and S3 access
                # Using the same bucket as the working example
                result = con.execute(
                    "SELECT COUNT(*) as cnt FROM read_parquet('s3://public-mappinginequality/mappinginequality.parquet') LIMIT 1"
                ).fetchone()
                
                assert result is not None, "Should get result from S3"
                assert result[0] > 0, "Should have rows in the dataset"
                print(f"Successfully read S3 metadata: {result[0]} rows")
                con.close()
            except Exception as e:
                con.close()
                pytest.skip(f"S3 network connection failed: {e}")
    
    @pytest.mark.timeout(120)
    @pytest.mark.integration
    def test_s3_public_read_with_processing(self):
        """Test reading from public S3 bucket and processing (full integration test)."""
        # This test hits the network and processes a small chunk from S3
        # It's marked as integration and has a 120-second timeout
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {
                "AWS_ACCESS_KEY_ID": "",
                "AWS_SECRET_ACCESS_KEY": "",
                "AWS_S3_ENDPOINT": "s3-west.nrp-nautilus.io",
                "AWS_HTTPS": "TRUE",
            }):
                # Use source parquet with geometry from the bucket
                processor = H3VectorProcessor(
                    input_url="s3://public-mappinginequality/mappinginequality.parquet",
                    output_url=tmpdir,
                    h3_resolution=10,
                    chunk_size=10
                )
                
                try:
                    # Process just the first chunk (10 rows)
                    print("Starting process_chunk...")
                    output_file = processor.process_chunk(0)
                    
                    assert output_file is not None, "Chunk should be processed"
                    assert Path(output_file).exists(), "Output file should exist"
                    
                    # Verify the output has expected structure
                    result = processor.con.execute(f"SELECT * FROM read_parquet('{output_file}')").fetchdf()
                    assert len(result) > 0, "Should have processed some rows"
                    assert 'h10' in result.columns, "Should have h10 column"
                    print(f"Successfully processed {len(result)} rows")
                    
                    processor.con.close()
                except Exception as e:
                    processor.con.close()
                    # If this fails, it might be due to network issues or endpoint configuration
                    pytest.skip(f"S3 integration test failed (may be network/endpoint issue): {e}")


class TestH3Functions:
    """Test H3 utility functions."""
    
    @pytest.mark.timeout(5)
    def test_setup_duckdb_connection(self):
        """Test DuckDB connection setup with H3 extension."""
        con = setup_duckdb_connection()
        
        # Verify H3 extension is loaded
        result = con.execute("""
            SELECT h3_latlng_to_cell(37.7749, -122.4194, 10)::UBIGINT as h3_cell
        """).fetchone()
        
        assert result[0] > 0  # Should return a valid H3 cell ID
        con.close()
    
    @pytest.mark.timeout(5)
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
    
    @pytest.mark.timeout(5)
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

    @pytest.mark.timeout(10)
    def test_subcell_polygon_gets_representative_cell(self):
        """A polygon smaller than one H3 cell falls back to one representative cell (#104)."""
        con = setup_duckdb_connection()
        # ~50 m x 50 m polygon, far smaller than a res-8 cell (~0.74 km²).
        con.execute("""
            CREATE TABLE tiny AS
            SELECT 1::BIGINT AS _cng_fid,
                   ST_GeomFromText('POLYGON((10.00010 10.00010,10.00060 10.00010,10.00060 10.00060,10.00010 10.00060,10.00010 10.00010))') AS geom
        """)
        sql = geom_to_h3_cells(con, "tiny", zoom=8)
        n = con.execute(f"SELECT len(h3id) FROM ({sql})").fetchone()[0]
        con.close()
        assert n == 1, f"sub-cell polygon should yield one fallback cell, got {n}"

    @pytest.mark.timeout(10)
    def test_subcell_multipolygon_part_preserved(self):
        """A sub-cell MULTIPOLYGON part is preserved via the fallback, not dropped (#104)."""
        con = setup_duckdb_connection()
        con.execute("""
            CREATE TABLE tinymp AS
            SELECT 1::BIGINT AS _cng_fid,
                   ST_GeomFromText('MULTIPOLYGON(((10.0001 10.0001,10.0006 10.0001,10.0006 10.0006,10.0001 10.0006,10.0001 10.0001)))') AS geom
        """)
        sql = geom_to_h3_cells(con, "tinymp", zoom=8)
        n = con.execute(f"SELECT len(h3id) FROM ({sql})").fetchone()[0]
        con.close()
        assert n >= 1

    @pytest.mark.timeout(10)
    def test_swapped_coordinate_polygon_stays_empty(self):
        """The fallback is gated on valid latitude: swapped (lat,lon) input stays
        empty so the downstream swapped-coordinate check still raises (#104)."""
        con = setup_duckdb_connection()
        # Y values (~122) are outside H3's valid latitude range, so the fallback
        # must NOT fire (it would feed an invalid latitude into h3_latlng_to_cell).
        con.execute("""
            CREATE TABLE swapped AS
            SELECT 1::BIGINT AS _cng_fid,
                   ST_GeomFromText('POLYGON((10 122.0001,10.0005 122.0001,10.0005 122.0006,10 122.0006,10 122.0001))') AS geom
        """)
        sql = geom_to_h3_cells(con, "swapped", zoom=8)
        n = con.execute(f"SELECT len(h3id) FROM ({sql})").fetchone()[0]
        con.close()
        assert n == 0, "swapped-coordinate polygon must stay empty for the QC to catch it"

    @pytest.mark.timeout(5)
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

    @pytest.mark.timeout(20)
    def test_circumpolar_polygon_is_split_before_polyfill(self):
        """A polygon spanning the full 360 deg of longitude must fill its band,
        not collapse to ~1 cell. H3 polygon_to_cells reads a >180-deg ring as the
        minimal-area side, so the pipeline splits it into <180-deg bands (#145)."""
        con = setup_duckdb_connection()
        # Full -180..180 x -78..-50 band (CCAMLR-shaped) plus a normal box.
        con.execute("""
            CREATE TABLE feats AS
            SELECT 'ccamlr' AS id,
                   ST_GeomFromText('POLYGON((-180 -78,180 -78,180 -50,-180 -50,-180 -78))') AS geom
            UNION ALL
            SELECT 'normal' AS id,
                   ST_GeomFromText('POLYGON((-100 -70,-50 -70,-50 -50,-100 -50,-100 -70))') AS geom
        """)
        sql = geom_to_h3_cells(con, "feats", zoom=3, keep_cols=['id'])
        con.execute(f"CREATE TABLE arrs AS SELECT * FROM ({sql})")

        # Circumpolar feature now fills thousands of cells (was ~0 before the split).
        n_ccamlr = con.execute(
            "SELECT sum(len(h3id)) FROM arrs WHERE id = 'ccamlr'"
        ).fetchone()[0]
        assert n_ccamlr > 1000, f"circumpolar band should fill its band, got {n_ccamlr}"

        # Band boundaries must not double-count cells (each cell centre lands in one band).
        distinct, total = con.execute("""
            SELECT count(DISTINCT cell), count(*)
            FROM (SELECT UNNEST(h3id) AS cell FROM arrs WHERE id = 'ccamlr')
        """).fetchone()
        assert distinct == total, f"band split duplicated {total - distinct} boundary cells"

        # The normal polygon is untouched: single row via the fast path.
        n_rows_normal = con.execute(
            "SELECT count(*) FROM arrs WHERE id = 'normal'"
        ).fetchone()[0]
        assert n_rows_normal == 1, "sub-180-deg polygon should not be band-split"
        con.close()

    @pytest.mark.timeout(20)
    def test_circumpolar_polygon_split_variable_resolution(self):
        """The transmeridian split also applies on the variable-resolution path (#145)."""
        con = setup_duckdb_connection()
        con.execute("""
            CREATE TABLE feats AS
            SELECT 'ccamlr' AS id,
                   ST_GeomFromText('POLYGON((-180 -78,180 -78,180 -50,-180 -50,-180 -78))') AS geom
        """)
        rba = parse_resolution_by_area("12:5,3")  # large features hex at res 3
        sql = geom_to_h3_cells(con, "feats", zoom=8, keep_cols=['id'], resolution_by_area=rba)
        n = con.execute(f"SELECT sum(len(h3id)) FROM ({sql})").fetchone()[0]
        con.close()
        assert n > 1000, f"circumpolar band should fill its band on the var-res path, got {n}"


class TestLineGeometryH3:
    """Test H3 cell generation for LineString geometries via buffer."""

    @pytest.mark.timeout(10)
    def test_linestring_produces_h3_cells(self):
        """A simple LineString should produce H3 cells via buffer."""
        con = setup_duckdb_connection()
        con.execute("""
            CREATE TABLE test_line AS
            SELECT
                1 as id,
                ST_GeomFromText('LINESTRING(-122.5 37.7, -122.4 37.8)') as geom
        """)

        sql = geom_to_h3_cells(con, "test_line", zoom=8)
        result = con.execute(f"SELECT * FROM ({sql})").fetchdf()

        assert len(result) > 0, "LineString should produce at least one H3 cell"
        assert 'h3id' in result.columns
        assert 'id' in result.columns
        con.close()

    @pytest.mark.timeout(10)
    def test_linestring_cells_are_continuous(self):
        """H3 cells along a line should form a connected set (no gaps)."""
        con = setup_duckdb_connection()
        # A straight-ish line spanning several H3 res-8 cells
        con.execute("""
            CREATE TABLE test_line AS
            SELECT
                1 as id,
                ST_GeomFromText('LINESTRING(-122.5 37.7, -122.3 37.7)') as geom
        """)

        sql = geom_to_h3_cells(con, "test_line", zoom=8)
        result = con.execute(f"""
            SELECT DISTINCT UNNEST(h3id) AS cell FROM ({sql})
        """).fetchdf()

        cells = set(result['cell'].tolist())
        assert len(cells) >= 2, "Line spanning ~20 km should cover multiple H3 res-8 cells"

        # Check connectivity: every cell should have at least one neighbor in the set
        for cell in cells:
            neighbors = set(
                con.execute(f"SELECT UNNEST(h3_grid_disk({cell}::UBIGINT, 1))::UBIGINT").fetchdf().iloc[:, 0].tolist()
            )
            assert len(neighbors & cells) >= 2, (
                f"Cell {cell} has no neighbors in the set — gap in coverage"
            )
        con.close()

    @pytest.mark.timeout(10)
    def test_multilinestring_produces_h3_cells(self):
        """MultiLineString should also produce H3 cells."""
        con = setup_duckdb_connection()
        con.execute("""
            CREATE TABLE test_mline AS
            SELECT
                1 as id,
                ST_GeomFromText('MULTILINESTRING((-122.5 37.7, -122.4 37.8), (-122.3 37.75, -122.2 37.85))') as geom
        """)

        sql = geom_to_h3_cells(con, "test_mline", zoom=8)
        result = con.execute(f"SELECT * FROM ({sql})").fetchdf()

        assert len(result) > 0, "MultiLineString should produce H3 cells"
        con.close()

    @pytest.mark.timeout(15)
    def test_linestring_end_to_end_processing(self):
        """Test full H3VectorProcessor pipeline with LineString data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            con.execute(f"""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_GeomFromText(
                        'LINESTRING(-122.5 37.7, -122.4 37.75, -122.3 37.8)'
                    ) as geom
                FROM range(3) t(i)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()

            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''

            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=10,
            )
            output_file = processor.process_chunk(0)

            assert output_file is not None
            assert Path(output_file).exists()

            result_con = setup_duckdb_connection()
            result = result_con.execute(f"SELECT * FROM read_parquet('{output_file}')").fetchdf()

            assert 'h8' in result.columns
            assert 'h0' in result.columns
            assert 'id' in result.columns
            # Each of the 3 lines should produce cells
            assert len(result['id'].unique()) == 3
            assert len(result) >= 3, "Each line should produce at least one cell"

            result_con.close()
            processor.con.close()

    @pytest.mark.timeout(10)
    def test_mixed_geometry_types(self):
        """Mixed polygon + line geometries should both produce H3 cells."""
        con = setup_duckdb_connection()
        con.execute("""
            CREATE TABLE test_mixed AS
            SELECT 1 as id,
                   ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as geom
            UNION ALL
            SELECT 2 as id,
                   ST_GeomFromText('LINESTRING(-122.3 37.7, -122.2 37.8)') as geom
        """)

        sql = geom_to_h3_cells(con, "test_mixed", zoom=8)
        result = con.execute(f"SELECT * FROM ({sql})").fetchdf()

        ids_with_cells = set(result['id'].tolist())
        assert 1 in ids_with_cells, "Polygon should produce H3 cells"
        assert 2 in ids_with_cells, "LineString should produce H3 cells"
        con.close()


class TestVectorProcessing:
    """Test complete vector processing workflows."""
    
    @pytest.mark.timeout(5)
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
                chunk_size=100,
                intermediate_chunk_size=10
            )
            
            assert processor.input_url == test_parquet
            assert processor.output_url == tmpdir
            assert processor.h3_resolution == 8
            assert processor.chunk_size == 100
            assert processor.intermediate_chunk_size == 10
            
            processor.con.close()
    
    @pytest.mark.timeout(10)
    def test_two_pass_processing_intermediate_file(self):
        """Test that two-pass processing creates and cleans up intermediate file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test data with multiple features to ensure array creation
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    ROW_NUMBER() OVER () as id,
                    'feature' || ROW_NUMBER() OVER () as name,
                    ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as geom
                FROM range(3)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=5,
                intermediate_chunk_size=2
            )
            
            # Track intermediate file creation
            from pathlib import Path as PathLib
            intermediate_files_before = set(PathLib('/tmp').iterdir())
            
            # Process chunk
            output_file = processor.process_chunk(0)
            
            # Check intermediate file was created during processing
            # (It should be cleaned up after, so we can't check it now)
            assert output_file is not None
            assert Path(output_file).exists()
            
            # Verify output has expected structure
            result_con = setup_duckdb_connection()
            result = result_con.execute(f"SELECT * FROM read_parquet('{output_file}')").fetchdf()
            
            assert 'h8' in result.columns
            assert 'h0' in result.columns
            assert 'id' in result.columns
            assert len(result) > 0
            
            # Verify intermediate file was cleaned up
            intermediate_files_after = set(PathLib('/tmp').iterdir())
            new_intermediate_files = [f.name for f in (intermediate_files_after - intermediate_files_before) 
                                     if f.name.startswith('h3_intermediate')]
            assert len(new_intermediate_files) == 0, f"Intermediate files not cleaned up: {new_intermediate_files}"
            
            result_con.close()
            processor.con.close()
    
    @pytest.mark.timeout(10)
    def test_small_intermediate_chunk_size(self):
        """Test processing with very small intermediate_chunk_size to verify batching.
        
        IMPORTANT: Each polygon's ID should be preserved in ALL its H3 cells.
        So polygon ID=0 generates cells with ID=0, polygon ID=1 generates cells with ID=1, etc.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create 5 different polygons, each with unique ID
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            
            # First verify the input data has correct IDs
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    i as id,
                    ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as geom
                FROM range(5) t(i)
            """)
            
            # Verify input has 5 rows with IDs 0-4
            input_check = con.execute("SELECT id FROM test_data ORDER BY id").fetchdf()
            assert len(input_check) == 5, f"Expected 5 input rows, got {len(input_check)}"
            assert list(input_check['id']) == [0, 1, 2, 3, 4], f"Input IDs incorrect: {list(input_check['id'])}"
            
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            # Use very small intermediate_chunk_size to force multiple batches in pass 2
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=10,
                intermediate_chunk_size=1  # Process 1 row at a time in pass 2
            )
            
            output_file = processor.process_chunk(0)
            
            assert output_file is not None
            assert Path(output_file).exists()
            
            # Verify output is correct despite small batch size
            result_con = setup_duckdb_connection()
            result = result_con.execute(f"SELECT * FROM read_parquet('{output_file}') ORDER BY id, h8").fetchdf()
            
            assert 'h8' in result.columns
            assert 'h0' in result.columns
            assert 'id' in result.columns
            
            # Critical test: All 5 polygon IDs should be preserved in output
            # Each polygon generates one or more H3 cells, but ALL cells from that polygon keep its ID
            unique_ids = sorted(result['id'].unique())
            assert len(unique_ids) == 5, f"Expected 5 unique polygon IDs (0-4), got {len(unique_ids)}: {unique_ids}"
            assert unique_ids == [0, 1, 2, 3, 4], f"Expected IDs 0-4, got {unique_ids}"
            
            # Verify we have at least as many output rows as input polygons
            assert len(result) >= 5, f"Expected at least 5 output rows, got {len(result)}"
            
            # Verify each ID appears at least once (could appear multiple times if polygon generates multiple cells)
            for expected_id in [0, 1, 2, 3, 4]:
                id_count = len(result[result['id'] == expected_id])
                assert id_count > 0, f"ID {expected_id} missing from output!"
            
            result_con.close()
            processor.con.close()
    
    @pytest.mark.timeout(15)
    def test_multipolygon_no_duplicate_feature_cell_rows(self):
        """A MultiPolygon whose parts share cells must not emit duplicate
        (feature, cell) rows (issue #150).

        Two overlapping parts polyfill to overlapping cell sets. Before the fix,
        Pass 2 unnested each part independently and wrote a row per part per cell,
        byte-identically duplicating every shared cell and inflating downstream
        SUM/area aggregates. intermediate_chunk_size=1 forces the two parts into
        separate Pass-2 batches, so this also proves the dedup happens on the
        fully assembled per-chunk file, not merely within a batch.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            # fid=1: MultiPolygon with two overlapping parts (guaranteed shared
            #        cells). fid=2: a single-part Polygon control (never dup'd).
            con.execute(f"""
                CREATE TABLE test_data AS
                SELECT * FROM (VALUES
                    (1, ST_GeomFromText('MULTIPOLYGON(((-122.5 37.7,-122.4 37.7,-122.4 37.8,-122.5 37.8,-122.5 37.7)),((-122.45 37.72,-122.35 37.72,-122.35 37.82,-122.45 37.82,-122.45 37.72)))')),
                    (2, ST_GeomFromText('POLYGON((-121.5 36.7,-121.4 36.7,-121.4 36.8,-121.5 36.8,-121.5 36.7))'))
                ) t(id, geom)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()

            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''

            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=10,
                intermediate_chunk_size=1,  # force parts into separate pass-2 batches
            )
            output_file = processor.process_chunk(0)
            processor.con.close()

            rc = setup_duckdb_connection()
            try:
                total, distinct = rc.execute(f"""
                    SELECT COUNT(*), COUNT(DISTINCT (id, h8))
                    FROM read_parquet('{output_file}')
                """).fetchone()
                assert total == distinct, (
                    f"duplicate (feature, cell) rows: {total} rows vs {distinct} distinct"
                )

                # The multipart feature must still cover cells (fix removes dupes,
                # not the feature) and no single (id, cell) may repeat.
                worst = rc.execute(f"""
                    SELECT MAX(n) FROM (
                        SELECT COUNT(*) n FROM read_parquet('{output_file}')
                        GROUP BY id, h8
                    )
                """).fetchone()[0]
                assert worst == 1, f"a (feature, cell) pair repeats {worst} times"

                ids = {r[0] for r in rc.execute(
                    f"SELECT DISTINCT id FROM read_parquet('{output_file}')"
                ).fetchall()}
                assert ids == {1, 2}, f"expected both features present, got {ids}"
            finally:
                rc.close()

    @pytest.mark.timeout(5)
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
            # ID column preserves original name
            assert 'id' in result.columns
            # Original attributes are no longer kept (only ID kept for minimal RAM usage)
            assert 'name' not in result.columns
            assert len(result) > 0
            
            result_con.close()
            processor.con.close()
    
    @pytest.mark.timeout(5)
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


class TestOversizedFeatureGuard:
    """Issue #107: a feature whose H3 cell array would exceed the 2GB/page limit
    must fail with a clear, actionable error instead of a C++ assertion."""

    def _make_source(self, tmpdir):
        src = f"{tmpdir}/polys.parquet"
        con = setup_duckdb_connection()
        # Feature 1: ~2deg box (~67k cells at res 8); Feature 2: tiny box.
        con.execute(f"""
            COPY (
                SELECT 1 AS _cng_fid, ST_GeomFromText('POLYGON((0 0,2 0,2 2,0 2,0 0))') AS geom
                UNION ALL
                SELECT 2 AS _cng_fid, ST_GeomFromText('POLYGON((10 10,10.01 10,10.01 10.01,10 10.01,10 10))') AS geom
            ) TO '{src}' (FORMAT PARQUET)
        """)
        con.close()
        return src

    @pytest.mark.timeout(30)
    def test_oversized_feature_raises_clear_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = self._make_source(tmpdir)
            processor = H3VectorProcessor(
                input_url=src, output_url=f"{tmpdir}/out",
                h3_resolution=8, parent_resolutions=[0], chunk_size=500,
            )
            processor.max_cells_per_feature = 50_000  # below feature 1's ~67k estimate
            with pytest.raises(RuntimeError, match=r"_cng_fid=1 is too large"):
                processor._process_pass1(0)
            processor.con.close()

    @pytest.mark.timeout(30)
    def test_normal_features_pass_under_default_threshold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = self._make_source(tmpdir)
            processor = H3VectorProcessor(
                input_url=src, output_url=f"{tmpdir}/out",
                h3_resolution=8, parent_resolutions=[0], chunk_size=500,
            )
            # Default threshold (~134M) is far above these features.
            out = processor._process_pass1(0)
            n = processor.con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{out}')"
            ).fetchone()[0]
            assert n == 2
            processor.con.close()

    def test_threshold_from_env(self, monkeypatch):
        monkeypatch.setenv("CNG_MAX_CELLS_PER_FEATURE", "12345")
        with tempfile.TemporaryDirectory() as tmpdir:
            src = self._make_source(tmpdir)
            processor = H3VectorProcessor(
                input_url=src, output_url=f"{tmpdir}/out", h3_resolution=8,
            )
            assert processor.max_cells_per_feature == 12345
            processor.con.close()


class TestSwappedCoordinateDetection:
    """Test that swapped lat/lon coordinates are detected and raise an error."""

    @pytest.mark.timeout(60)
    def test_correct_coordinates_pass(self):
        """Polygons with correct (lon, lat) order should process without error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            # Alpine County CA bbox in correct (lon, lat) order
            con.execute(f"""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_GeomFromText('POLYGON((-120.073331 38.32688, -119.542332 38.32688, -119.542332 38.933324, -120.073331 38.933324, -120.073331 38.32688))') as geom
                FROM range(3) t(i)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()

            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''

            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                chunk_size=10,
            )
            output_file = processor.process_chunk(0)
            assert output_file is not None
            assert Path(output_file).exists()
            processor.con.close()

    @pytest.mark.timeout(60)
    def test_swapped_coordinates_raise_error(self):
        """Polygons with swapped (lat, lon) order should raise RuntimeError, not silently write 0 rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            # Alpine County CA bbox with swapped (lat, lon) order — longitudes in lat slot
            # are outside H3's valid lat range, so h3_polygon_wkt_to_cells returns []
            con.execute(f"""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_GeomFromText('POLYGON((38.32688 -120.073331, 38.933324 -120.073331, 38.933324 -119.542332, 38.32688 -119.542332, 38.32688 -120.073331))') as geom
                FROM range(3) t(i)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()

            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''

            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                chunk_size=10,
            )
            with pytest.raises(RuntimeError, match="outside the valid latitude range"):
                processor.process_chunk(0)
            processor.con.close()

    @pytest.mark.timeout(60)
    def test_small_polygon_preserved_via_representative_point(self):
        """
        Polygons smaller than one H3 cell must NOT be dropped (issue #104,
        superseding the earlier #51 skip-with-warning behavior). The H3 polyfill
        returns no cells for a sub-cell polygon, so each feature falls back to
        the single cell containing its ST_PointOnSurface — every feature yields
        >= 1 row, and an all-sub-cell chunk still writes output (no Pass 2 crash).
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            # Three tiny polygons (~0.0001 km²) in correct (lon, lat) order.
            # At H3 resolution 8 (cell area ~0.74 km²) the polyfill is empty.
            con.execute(f"""
                CREATE TABLE test_data AS
                SELECT
                    i as id,
                    ST_GeomFromText('POLYGON((-122.4 37.8, -122.4001 37.8, -122.4001 37.8001, -122.4 37.8001, -122.4 37.8))') as geom
                FROM range(3) t(i)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()

            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''

            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=tmpdir,
                h3_resolution=8,
                chunk_size=10,
            )
            # Must not raise, and the all-sub-cell chunk must still produce output.
            output_file = processor.process_chunk(0)
            assert output_file is not None
            # Every sub-cell feature is preserved with exactly one fallback cell.
            n_rows, n_features = processor.con.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT id) FROM read_parquet('{output_file}')"
            ).fetchone()
            processor.con.close()
            assert n_features == 3, "all three sub-cell features must survive"
            assert n_rows == 3, "each sub-cell feature maps to exactly one cell"


class TestRepartitionWithAttributeJoin:
    """Test repartition functionality with attribute join."""
    
    @pytest.mark.timeout(10)
    def test_id_column_preserved_in_chunks(self):
        """Test that ID column name is preserved in chunk output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/test.parquet"
            
            # Create data with OBJECTID (common in Esri formats)
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    ROW_NUMBER() OVER () as OBJECTID,
                    'feature' || ROW_NUMBER() OVER () as NAME,
                    ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as SHAPE
                FROM range(3)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            # Create output directory
            chunks_dir = f"{tmpdir}/chunks"
            os.makedirs(chunks_dir, exist_ok=True)
            
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=chunks_dir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=5
            )
            processor.process_chunk(0)
            processor.con.close()
            
            # Verify chunk has OBJECTID, not renamed to fid
            con = setup_duckdb_connection()
            chunk_df = con.execute(f"SELECT * FROM read_parquet('{chunks_dir}/*.parquet') LIMIT 1").fetchdf()
            
            assert 'OBJECTID' in chunk_df.columns, f"OBJECTID not preserved! Columns: {chunk_df.columns}"
            assert 'h8' in chunk_df.columns
            assert 'h0' in chunk_df.columns
            assert 'NAME' not in chunk_df.columns  # Attributes should not be in sparse chunks
            con.close()
    
    @pytest.mark.timeout(10)
    def test_repartition_identifies_chunk_id_column(self):
        """Test that repartition correctly identifies ID column from chunks."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            
            # Create fake chunks with different ID column name
            chunks_dir = f"{tmpdir}/chunks"
            os.makedirs(chunks_dir, exist_ok=True)
            
            con.execute(f"""
                CREATE TABLE chunk_data AS 
                SELECT 
                    1 as MyID,
                    613196575302221823::UBIGINT as h10,
                    613196575302221823::UBIGINT as h9,
                    613196575302221823::UBIGINT as h8,
                    577199624117288959 as h0
            """)
            con.execute(f"COPY chunk_data TO '{chunks_dir}/chunk_000000.parquet' (FORMAT PARQUET)")
            
            # Read back and verify ID detection logic
            import ibis
            ibis_con = ibis.duckdb.connect()
            chunks = ibis_con.read_parquet(f'{chunks_dir}/*.parquet')
            chunk_cols = chunks.columns
            
            # Find ID column (non-h3 column)
            chunk_id_col = None
            for col in chunk_cols:
                if not col.startswith('h') or not col[1:].isdigit():
                    chunk_id_col = col
                    break
            
            assert chunk_id_col == 'MyID', f"Expected MyID, got {chunk_id_col}"
            con.close()
    
    @pytest.mark.timeout(15)
    def test_end_to_end_with_attribute_join(self):
        """Test complete workflow: process chunks → repartition with attribute join."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/source.parquet"
            
            # Create source data with FID
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    ROW_NUMBER() OVER () as FID,
                    'feature' || ROW_NUMBER() OVER () as NAME,
                    'Type' || (ROW_NUMBER() OVER () % 2) as TYPE,
                    ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as geom
                FROM range(5)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            # Step 1: Process with H3VectorProcessor
            chunks_dir = f"{tmpdir}/chunks"
            os.makedirs(chunks_dir, exist_ok=True)
            
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=chunks_dir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=5
            )
            processor.process_chunk(0)
            processor.con.close()
            
            # Step 2: Repartition with attribute join
            repartition_by_h0(
                chunks_dir=chunks_dir,
                output_dir=f"{tmpdir}/output",
                source_parquet=test_parquet,
                cleanup=False
            )
            
            # Step 3: Verify output has ID and all attributes
            con = setup_duckdb_connection()
            output_df = con.execute(f"SELECT * FROM read_parquet('{tmpdir}/output/**/*.parquet') LIMIT 1").fetchdf()
            
            assert 'FID' in output_df.columns, f"FID not in output! Columns: {output_df.columns}"
            assert 'NAME' in output_df.columns, f"NAME not in output! Columns: {output_df.columns}"
            assert 'TYPE' in output_df.columns, f"TYPE not in output! Columns: {output_df.columns}"
            assert 'h8' in output_df.columns
            assert 'h0' in output_df.columns
            assert 'geom' not in output_df.columns  # Geometry should be excluded
            
            # Verify we have multiple rows (exploded by h3)
            total_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmpdir}/output/**/*.parquet')").fetchone()[0]
            assert total_rows > 5, f"Expected more than 5 rows after h3 explosion, got {total_rows}"
            
            con.close()
    
    @pytest.mark.timeout(15)
    def test_repartition_without_source_parquet(self):
        """Test repartition works without source parquet (no attribute join)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            
            # Create fake chunks directory
            chunks_dir = f"{tmpdir}/chunks"
            os.makedirs(chunks_dir, exist_ok=True)
            
            con.execute(f"""
                CREATE TABLE chunk_data AS 
                SELECT 
                    i as fid,
                    (613196575302221823 + i)::UBIGINT as h10,
                    613196575302221823::UBIGINT as h9,
                    613196575302221823::UBIGINT as h8,
                    577199624117288959 as h0
                FROM range(10) t(i)
            """)
            con.execute(f"COPY chunk_data TO '{chunks_dir}/chunk_000000.parquet' (FORMAT PARQUET)")
            con.close()
            
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            # Repartition without source parquet
            repartition_by_h0(
                chunks_dir=chunks_dir,
                output_dir=f"{tmpdir}/output",
                source_parquet=None,
                cleanup=False
            )
            
            # Verify output exists and has expected structure
            con = setup_duckdb_connection()
            output_df = con.execute(f"SELECT * FROM read_parquet('{tmpdir}/output/**/*.parquet')").fetchdf()
            
            assert 'fid' in output_df.columns
            assert 'h10' in output_df.columns
            assert 'h0' in output_df.columns
            assert len(output_df) == 10
            
            con.close()

    @pytest.mark.timeout(15)
    def test_repartition_orders_by_cng_fid_within_partition(self):
        """Rows within each h0 partition are sorted by _cng_fid (issue #103).

        The intra-partition ORDER BY tightens row-group [min,max] zonemaps so a
        `WHERE _cng_fid IN (...)` can prune. We assert the physical row order is
        non-decreasing in _cng_fid even when the chunk input is shuffled.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            chunks_dir = f"{tmpdir}/chunks"
            os.makedirs(chunks_dir, exist_ok=True)

            # Single h0 partition; _cng_fid deliberately out of order, multiple
            # rows per fid (the hex explosion) so ordering is observable.
            con.execute(f"""
                CREATE TABLE chunk_data AS
                SELECT
                    _cng_fid,
                    (613196575302221823 + row_number() OVER ())::UBIGINT as h10,
                    613196575302221823::UBIGINT as h8,
                    577199624117288959 as h0
                FROM (VALUES (5),(1),(3),(1),(5),(2),(3)) t(_cng_fid)
            """)
            con.execute(f"COPY chunk_data TO '{chunks_dir}/chunk_000000.parquet' (FORMAT PARQUET)")
            con.close()

            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''

            repartition_by_h0(
                chunks_dir=chunks_dir,
                output_dir=f"{tmpdir}/output",
                source_parquet=None,
                cleanup=False,
            )

            con = setup_duckdb_connection()
            part = f"{tmpdir}/output/h0=577199624117288959/data_0.parquet"
            # Read in physical (file) order — no ORDER BY in the query.
            fids = [r[0] for r in con.execute(
                f"SELECT _cng_fid FROM read_parquet('{part}')"
            ).fetchall()]
            con.close()

            assert fids == sorted(fids), f"rows not ordered by _cng_fid: {fids}"

    @pytest.mark.timeout(15)
    def test_repartition_with_mixed_case_columns(self):
        """Test that mixed case ID columns are handled correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            test_parquet = f"{tmpdir}/source.parquet"
            
            # Create source with mixed case ObjectID - use POLYGON not POINT
            con.execute(f"""
                CREATE TABLE test_data AS 
                SELECT 
                    ROW_NUMBER() OVER () as ObjectID,
                    'name' || ROW_NUMBER() OVER () as Name,
                    ST_GeomFromText('POLYGON((-122.5 37.7, -122.4 37.7, -122.4 37.8, -122.5 37.8, -122.5 37.7))') as Geometry
                FROM range(3)
            """)
            con.execute(f"COPY test_data TO '{test_parquet}' (FORMAT PARQUET)")
            con.close()
            
            os.environ['AWS_ACCESS_KEY_ID'] = ''
            os.environ['AWS_SECRET_ACCESS_KEY'] = ''
            
            # Process chunks
            chunks_dir = f"{tmpdir}/chunks"
            os.makedirs(chunks_dir, exist_ok=True)
            
            processor = H3VectorProcessor(
                input_url=test_parquet,
                output_url=chunks_dir,
                h3_resolution=8,
                parent_resolutions=[0],
                chunk_size=5
            )
            processor.process_chunk(0)
            processor.con.close()
            
            # Verify chunks have mixed case column
            con = setup_duckdb_connection()
            chunk_df = con.execute(f"SELECT * FROM read_parquet('{chunks_dir}/*.parquet') LIMIT 1").fetchdf()
            assert 'ObjectID' in chunk_df.columns, f"ObjectID not preserved in chunks! Columns: {chunk_df.columns}"
            
            # Test attribute join manually (since full repartition has issues with local paths)
            import ibis
            # Use raw duckdb to get column names — ibis cannot parse GEOMETRY(OGC:CRS84)
            # types written by DuckDB 1.5+.
            raw_con = duckdb.connect()
            desc = raw_con.execute(f"DESCRIBE SELECT * FROM read_parquet('{test_parquet}')").fetchdf()
            raw_con.close()
            source_cols = [c for c in desc['column_name'].tolist() if c != 'Geometry']

            ibis_con = ibis.duckdb.connect()
            chunks = ibis_con.read_parquet(f'{chunks_dir}/*.parquet')
            quoted = ', '.join(f'"{c}"' for c in source_cols)
            ibis_con.raw_sql(f"CREATE OR REPLACE VIEW _source_attrs AS SELECT {quoted} FROM read_parquet('{test_parquet}')")
            result = chunks.inner_join(ibis_con.table('_source_attrs'), 'ObjectID')
            result_df = result.execute()
            
            assert 'ObjectID' in result_df.columns, f"ObjectID not in joined result! Columns: {result_df.columns}"
            assert 'Name' in result_df.columns, f"Name not in joined result! Columns: {result_df.columns}"
            assert 'h8' in result_df.columns
            assert len(result_df) > 0

            con.close()

    @pytest.mark.timeout(10)
    def test_repartition_raises_on_empty_chunks_dir(self):
        """repartition_by_h0 should raise a clear RuntimeError when chunks dir has no parquet files (issue #10)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_chunks_dir = f"{tmpdir}/empty_chunks"
            os.makedirs(empty_chunks_dir)
            output_dir = f"{tmpdir}/output"

            with pytest.raises(RuntimeError, match="No parquet files found"):
                repartition_by_h0(
                    chunks_dir=empty_chunks_dir,
                    output_dir=output_dir,
                    cleanup=False,
                )

    @pytest.mark.timeout(30)
    def test_repartition_raises_on_empty_chunk_output(self):
        """repartition_by_h0 should raise a clear RuntimeError when chunks exist but write no rows (issue #10)."""
        import duckdb as _duckdb
        with tempfile.TemporaryDirectory() as tmpdir:
            chunks_dir = f"{tmpdir}/chunks"
            os.makedirs(chunks_dir)
            output_dir = f"{tmpdir}/output"

            # Write a parquet chunk with zero rows (schema only)
            _con = _duckdb.connect()
            _con.execute(
                f"COPY (SELECT 1 AS _cng_fid, 0 AS h10, 0 AS h0 WHERE false) "
                f"TO '{chunks_dir}/chunk.parquet' (FORMAT PARQUET)"
            )
            _con.close()

            with pytest.raises(RuntimeError, match="empty"):
                repartition_by_h0(
                    chunks_dir=chunks_dir,
                    output_dir=output_dir,
                    cleanup=False,
                )


class TestParseResolutionByArea:
    """Issue #98: parsing the --resolution-by-area spec into sorted bins."""

    def test_valid_spec_sorts_and_appends_catchall(self):
        # Intentionally out of order to confirm threshold sorting.
        bins = parse_resolution_by_area("600:6,12:8,5")
        assert bins == [(12.0, 8), (600.0, 6), (None, 5)]

    def test_single_catchall_only(self):
        assert parse_resolution_by_area("7") == [(None, 7)]

    def test_missing_catchall_raises(self):
        with pytest.raises(ValueError, match="catch-all"):
            parse_resolution_by_area("12:8,600:6")

    def test_multiple_catchalls_raise(self):
        with pytest.raises(ValueError, match="more than one catch-all"):
            parse_resolution_by_area("12:8,5,3")

    def test_empty_spec_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_resolution_by_area("")

    def test_non_numeric_bin_raises(self):
        with pytest.raises(ValueError, match="Invalid"):
            parse_resolution_by_area("12:8,9:bad,5")

    def test_resolution_out_of_range_raises(self):
        with pytest.raises(ValueError, match="out of range"):
            parse_resolution_by_area("12:99,5")

    def test_duplicate_threshold_raises(self):
        with pytest.raises(ValueError, match="duplicate threshold"):
            parse_resolution_by_area("12:8,12:6,5")


class TestResolutionByArea:
    """Issue #98: variable-resolution (size-stratified) H3 polyfill.

    Each feature is hexed at the native resolution its planar ST_Area maps to;
    output carries a union schema (finer columns NULL in coarser tiers) plus a
    native_res column, with the coarsest native resolution acting as a common
    floor present in every row.
    """

    # area 0.0001 deg² -> res 8; area 100 -> res 6; area 2250 -> res 5
    _SPEC = "1:8,600:6,5"
    _PARENTS = [7, 6, 5, 4, 0]

    def _make_source(self, tmpdir):
        src = f"{tmpdir}/polys.parquet"
        con = setup_duckdb_connection()
        con.execute(f"""
            COPY (SELECT * FROM (VALUES
                (1, 'small',  ST_GeomFromText('POLYGON((0 0,0 0.01,0.01 0.01,0.01 0,0 0))')),
                (2, 'medium', ST_GeomFromText('POLYGON((10 10,10 20,20 20,20 10,10 10))')),
                (3, 'large',  ST_GeomFromText('POLYGON((40 0,40 45,90 45,90 0,40 0))'))
            ) AS v(_cng_fid, name, geometry))
            TO '{src}' (FORMAT PARQUET)
        """)
        con.close()
        return src

    @pytest.mark.timeout(15)
    def test_geom_to_h3_cells_assigns_native_res(self):
        """A tiny polygon is hexed fine (res 8); a huge one coarse (res 5)."""
        con = setup_duckdb_connection()
        con.execute("""
            CREATE TABLE polys AS
            SELECT * FROM (VALUES
                (1, ST_GeomFromText('POLYGON((0 0,0 0.01,0.01 0.01,0.01 0,0 0))')),
                (3, ST_GeomFromText('POLYGON((40 0,40 45,90 45,90 0,40 0))'))
            ) AS v(_cng_fid, geom)
        """)
        bins = parse_resolution_by_area(self._SPEC)
        sql = geom_to_h3_cells(con, "polys", keep_cols=['_cng_fid'], resolution_by_area=bins)
        rows = con.execute(f"""
            SELECT _cng_fid, native_res,
                   h3_get_resolution(h3id[1]) AS cell_res
            FROM ({sql}) ORDER BY _cng_fid
        """).fetchall()
        con.close()
        by_fid = {r[0]: (r[1], r[2]) for r in rows}
        assert by_fid[1] == (8, 8), "tiny polygon should be native res 8"
        assert by_fid[3] == (5, 5), "huge polygon should be native res 5"

    @pytest.mark.timeout(60)
    def test_processor_emits_union_schema_and_native_res(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = self._make_source(tmpdir)
            out = f"{tmpdir}/chunks"
            os.makedirs(out)
            processor = H3VectorProcessor(
                input_url=src, output_url=out,
                parent_resolutions=self._PARENTS, chunk_size=10,
                intermediate_chunk_size=5,
                resolution_by_area=parse_resolution_by_area(self._SPEC),
            )
            # finest bin resolution stands in for h3_resolution
            assert processor.h3_resolution == 8
            chunk_file = processor.process_chunk(0)
            con = processor.con

            cols = [d[0] for d in con.execute(
                f"SELECT * FROM read_parquet('{chunk_file}') LIMIT 0").description]
            assert cols == ['_cng_fid', 'native_res', 'h8', 'h7', 'h6', 'h5', 'h4', 'h0']

            # native_res per feature
            native = dict(con.execute(
                f"SELECT DISTINCT _cng_fid, native_res FROM read_parquet('{chunk_file}')"
            ).fetchall())
            assert native == {1: 8, 2: 6, 3: 5}

            # coarse-tier rows have NULL finer columns; common floor never NULL
            nulls = con.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE native_res = 5 AND h8 IS NOT NULL),
                    COUNT(*) FILTER (WHERE native_res = 5 AND h6 IS NOT NULL),
                    COUNT(*) FILTER (WHERE native_res = 6 AND h7 IS NOT NULL),
                    COUNT(*) FILTER (WHERE h5 IS NULL),
                    COUNT(*) FILTER (WHERE h0 IS NULL)
                FROM read_parquet('{chunk_file}')
            """).fetchone()
            assert nulls == (0, 0, 0, 0, 0), (
                "coarse tiers must NULL their finer columns; the common floor (h5) "
                "and partition column (h0) must be non-null in every row"
            )

            # the native cell column equals native_res's resolution for every row
            bad = con.execute(f"""
                SELECT COUNT(*) FROM read_parquet('{chunk_file}')
                WHERE h3_get_resolution(
                    CASE native_res WHEN 8 THEN h8 WHEN 6 THEN h6 WHEN 5 THEN h5 END
                ) <> native_res
            """).fetchone()[0]
            assert bad == 0

            # all physical h{N>=1} columns are UBIGINT (issue #102)
            assert_h3_columns_unsigned(
                lambda q: con.execute(q).fetchall(), chunk_file)
            processor.con.close()

    def test_missing_h0_partition_column_raises(self):
        """By-area output must carry h0 (the hive partition key); omitting 0 from
        --parent-resolutions fails fast with a clear error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="h0 partition column"):
                H3VectorProcessor(
                    input_url=f"{tmpdir}/x.parquet", output_url=tmpdir,
                    parent_resolutions=[7, 6, 5],  # no 0
                    resolution_by_area=parse_resolution_by_area(self._SPEC),
                )

    @pytest.mark.timeout(60)
    def test_parent_finer_than_finest_native_is_dropped(self):
        """A parent resolution finer than the finest native bin would be an
        all-NULL column, so it is not emitted (matches the fixed-res path)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = self._make_source(tmpdir)
            out = f"{tmpdir}/chunks"
            os.makedirs(out)
            processor = H3VectorProcessor(
                input_url=src, output_url=out,
                parent_resolutions=[9, 8, 0],  # 9 > finest native (8)
                chunk_size=10, intermediate_chunk_size=5,
                resolution_by_area=parse_resolution_by_area(self._SPEC),
            )
            chunk_file = processor.process_chunk(0)
            cols = [d[0] for d in processor.con.execute(
                f"SELECT * FROM read_parquet('{chunk_file}') LIMIT 0").description]
            assert 'h9' not in cols, "all-null h9 (finer than finest native) must be dropped"
            # attributes (name) are rejoined later in repartition, not in the chunk
            assert cols == ['_cng_fid', 'native_res', 'h8', 'h6', 'h5', 'h0']
            processor.con.close()

    @pytest.mark.timeout(60)
    def test_oversized_feature_backs_off_to_native_res(self):
        """A feature that would exceed the #107 limit at the finest resolution
        passes when its area assigns it a coarse native resolution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            con = setup_duckdb_connection()
            src = f"{tmpdir}/big.parquet"
            con.execute(f"""
                COPY (SELECT 3 AS _cng_fid,
                             ST_GeomFromText('POLYGON((40 0,40 45,90 45,90 0,40 0))') AS geom)
                TO '{src}' (FORMAT PARQUET)
            """)
            con.close()

            # Fixed res 8: estimate far exceeds the (lowered) limit -> raises.
            fixed = H3VectorProcessor(
                input_url=src, output_url=f"{tmpdir}/o1",
                h3_resolution=8, parent_resolutions=[0], chunk_size=10,
            )
            fixed.max_cells_per_feature = 1_000_000
            with pytest.raises(RuntimeError, match=r"_cng_fid=3 is too large.*resolution 8"):
                fixed._process_pass1(0)
            fixed.con.close()

            # By-area assigns res 5: estimate is well under the same limit -> passes.
            os.makedirs(f"{tmpdir}/o2")
            byarea = H3VectorProcessor(
                input_url=src, output_url=f"{tmpdir}/o2",
                parent_resolutions=[5, 4, 0], chunk_size=10, intermediate_chunk_size=5,
                resolution_by_area=parse_resolution_by_area(self._SPEC),
            )
            byarea.max_cells_per_feature = 1_000_000
            chunk_file = byarea.process_chunk(0)
            assert chunk_file is not None
            n = byarea.con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{chunk_file}')").fetchone()[0]
            assert n > 0
            byarea.con.close()


