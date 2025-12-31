"""
H3 hexagonal tiling utilities for vector datasets.

This module provides functions to convert polygon and point geometries
into H3 hexagonal cells at specified resolutions, with support for
chunked processing of large datasets.
"""

from typing import Optional, List, Dict, Any, Tuple
import duckdb
import os
from cng_datasets.storage.s3 import configure_s3_credentials


def identify_id_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    specified_id_col: Optional[str] = None,
    check_uniqueness: bool = True,
) -> Tuple[str, bool]:
    """
    Identify or validate an ID column in a table.
    
    Uses case-insensitive matching to find common ID column names, or validates
    a user-specified column. Optionally checks for uniqueness.
    
    Prioritizes _cng_fid as the standard synthetic ID column created by convert_to_parquet.
    
    Args:
        con: DuckDB connection
        table_name: Name of the table or view to check
        specified_id_col: User-specified ID column name (if provided)
        check_uniqueness: Whether to validate that the ID column is unique
        
    Returns:
        Tuple of (column_name, is_unique) where:
        - column_name: Actual column name in the table (preserving case)
        - is_unique: True if column is unique (or check was skipped)
        
    Raises:
        ValueError: If specified column not found or uniqueness check fails
    """
    # Get all column names
    result = con.execute(f"SELECT * FROM {table_name} LIMIT 0").description
    all_columns = [col[0] for col in result]
    
    # If user specified a column, validate it exists
    if specified_id_col:
        # Case-insensitive search
        col_lower_map = {col.lower(): col for col in all_columns}
        actual_col = col_lower_map.get(specified_id_col.lower())
        
        if not actual_col:
            raise ValueError(f"Specified ID column '{specified_id_col}' not found in table. Available columns: {', '.join(all_columns)}")
        
        id_col = actual_col
    else:
        # Auto-detect common ID column names (case-insensitive)
        # _cng_fid is our standard synthetic ID from convert_to_parquet
        col_lower_map = {col.lower(): col for col in all_columns}
        common_id_names = ['_cng_fid', 'fid', 'objectid', 'id', 'uid', 'gid', 'ogc_fid']
        
        id_col = None
        for name in common_id_names:
            if name in col_lower_map:
                id_col = col_lower_map[name]
                break
        
        if not id_col:
            # No ID column found
            return None, False
    
    # Check uniqueness if requested
    is_unique = True
    if check_uniqueness:
        total_count = con.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]
        unique_count = con.execute(f'SELECT COUNT(DISTINCT "{id_col}") FROM {table_name}').fetchone()[0]
        
        is_unique = (total_count == unique_count)
        
        if not is_unique:
            warning_msg = f"Warning: ID column '{id_col}' has {total_count} rows but only {unique_count} unique values"
            if specified_id_col:
                # User specified it, so this is an error
                raise ValueError(warning_msg)
            else:
                # Auto-detected, so just warn and return
                print(f"  {warning_msg}")
    
    return id_col, is_unique


def geom_to_h3_cells(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    zoom: int = 10,
    keep_cols: Optional[List[str]] = None,
    geom_col: str = "geom",
) -> str:
    """
    Convert geometries to H3 cells at specified resolution.
    
    Args:
        con: DuckDB connection
        table_name: Name of the table or view with geometry column
        zoom: H3 resolution level (0-15)
        keep_cols: List of columns to keep from input. If None, keeps all except geom.
        geom_col: Name of the geometry column
        
    Returns:
        SQL query string that generates H3 cells
    """
    # Get column names if not specified
    if keep_cols is None:
        cols_query = f"SELECT * FROM {table_name} LIMIT 0"
        keep_cols = [col for col in con.execute(cols_query).description if col[0] != geom_col]
        keep_cols = [col[0] for col in keep_cols]
    
    # Build column list for SELECT statements, quoting column names to handle spaces
    col_list = ', '.join([f'"{col}"' for col in keep_cols])
    
    # Convert to multi-polygons and unnest, then generate H3 cells
    # The geometry is already GEOMETRY type in DuckDB spatial extension
    sql = f'''
        WITH t0 AS (
            SELECT {col_list},
                   CASE 
                       WHEN ST_GeometryType({geom_col}) = 'POLYGON' 
                       THEN ST_Multi({geom_col})
                       ELSE {geom_col}
                   END AS geom
            FROM {table_name}
        ),
        t1 AS (
            SELECT {col_list}, 
                   UNNEST(ST_Dump(geom)).geom AS geom 
            FROM t0
        ) 
        SELECT {col_list}, h3_polygon_wkt_to_cells(geom, {zoom}) AS h3id 
        FROM t1
    '''
    
    return sql


def setup_duckdb_connection(
    extensions: Optional[List[str]] = None,
    http_retries: int = 20,
    http_retry_wait_ms: int = 5000,
) -> duckdb.DuckDBPyConnection:
    """
    Set up a DuckDB connection with required extensions.
    
    Args:
        extensions: List of DuckDB extensions to load. Defaults to ["spatial"]
        http_retries: Number of HTTP retries for remote files
        http_retry_wait_ms: Wait time between retries in milliseconds
        
    Returns:
        Configured DuckDB connection
    """
    if extensions is None:
        extensions = ["spatial"]
    
    con = duckdb.connect()
    
    # Install and load extensions
    for ext in extensions:
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")
    
    # Install h3 from community repository
    con.execute("INSTALL h3 FROM community")
    con.execute("LOAD h3")
    
    # Configure HTTP settings
    con.execute(f"SET http_retries={http_retries}")
    con.execute(f"SET http_retry_wait_ms={http_retry_wait_ms}")
    
    # Configure temp directory for spill-to-disk operations
    con.execute("SET temp_directory='/tmp'")
    
    return con


class H3VectorProcessor:
    """
    Process vector datasets into H3-indexed parquet files.
    
    Handles chunked processing of large vector datasets, converting
    geometries to H3 cells and adding parent cell hierarchies.
    """
    
    def __init__(
        self,
        input_url: str,
        output_url: str,
        h3_resolution: int = 10,
        parent_resolutions: Optional[List[int]] = None,
        chunk_size: int = 500,
        id_column: Optional[str] = None,
        read_credentials: Optional[Dict[str, str]] = None,
        write_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the H3 vector processor.
        
        Args:
            input_url: S3 URL or local path to input parquet/geoparquet file
            output_url: S3 URL or local path to output directory
            h3_resolution: Target H3 resolution for tiling
            parent_resolutions: List of parent resolutions to include (e.g., [9, 8, 0])
            chunk_size: Number of rows to process per chunk
            id_column: Name of ID column to use (auto-detected if not specified)
            read_credentials: Dict with AWS credentials for reading (key, secret, region, endpoint)
            write_credentials: Dict with AWS credentials for writing (key, secret, region, endpoint)
        """
        self.input_url = input_url
        self.output_url = output_url
        self.h3_resolution = h3_resolution
        self.parent_resolutions = parent_resolutions or [9, 8, 0]
        self.chunk_size = chunk_size
        self.id_column = id_column
        self.read_credentials = read_credentials
        self.write_credentials = write_credentials
        
        self.con = setup_duckdb_connection()
        self._configure_credentials()
        
    def _configure_credentials(self):
        """Configure S3 credentials for DuckDB using environment variables."""
        configure_s3_credentials(self.con)
    
    def _find_geometry_column(self, table_name: str) -> str:
        """Find the geometry column in the table."""
        result = self.con.execute(f"SELECT * FROM {table_name} LIMIT 0").description
        columns = [col[0] for col in result]
        for col in columns:
            if col.upper() in ['SHAPE', 'GEOMETRY', 'GEOM']:
                return col
        raise ValueError(f"No geometry column found. Available columns: {columns}")
    
    def process_chunk(
        self,
        chunk_id: int,
        keep_cols: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Process a single chunk of the input dataset.
        
        Args:
            chunk_id: Zero-based chunk index to process
            keep_cols: List of attribute columns to keep
            
        Returns:
            Output file path if successful, None if chunk_id out of range
        """
        # Read only the chunk we need with column projection for speed
        # Use LIMIT/OFFSET with parquet metadata for efficient chunk selection
        offset = chunk_id * self.chunk_size
        
        self.con.execute(f"""
            CREATE OR REPLACE VIEW source_table AS 
            SELECT * FROM read_parquet('{self.input_url}')
            LIMIT {self.chunk_size} OFFSET {offset}
        """)
        
        # Get row count to validate chunk
        chunk_rows = self.con.execute("SELECT COUNT(*) FROM source_table").fetchone()[0]
        if chunk_rows == 0:
            print(f"Chunk {chunk_id} is empty (offset {offset:,} beyond data)")
            return None
        
        # Find and rename geometry column
        geom_col = self._find_geometry_column('source_table')
        
        # Identify or validate ID column
        id_col, is_unique = identify_id_column(
            self.con, 
            'source_table', 
            specified_id_col=self.id_column,
            check_uniqueness=True
        )
        
        # If no ID column found or not unique, create a synthetic one
        # But if _cng_fid already exists (from convert_to_parquet), use it!
        if id_col is None or not is_unique:
            print(f"  No valid ID column found, creating row_number as _fid")
            id_col = '_fid'
            self.con.execute(f"""
                CREATE OR REPLACE VIEW source_table_with_id AS 
                SELECT row_number() OVER () - 1 + {offset} AS {id_col}, *
                FROM source_table
            """)
            chunk_view_name = 'source_table_with_id'
        else:
            print(f"  Using ID column: {id_col} (unique: {is_unique})")
            chunk_view_name = 'source_table'
        
        print(f"\nProcessing chunk {chunk_id} ({chunk_rows:,} rows)")
        
        # Create optimized chunk view with ONLY ID and geometry (minimal memory for UNNEST)
        # Keep original ID column name to preserve data fidelity
        self.con.execute(f"""
            CREATE OR REPLACE VIEW chunk_table AS 
            SELECT "{id_col}", {geom_col} AS geom
            FROM {chunk_view_name}
        """)
        
        # Convert to H3 cells - only keeping ID column (with original name)
        h3_sql = geom_to_h3_cells(
            self.con, 
            'chunk_table', 
            zoom=self.h3_resolution, 
            keep_cols=[id_col]  # Keep original ID column name
        )
        
        # Build final query with unnested h3 cells and parent resolutions
        h3_col = f"h{self.h3_resolution}"
        parent_cols = []
        for parent_res in sorted(self.parent_resolutions):
            if parent_res < self.h3_resolution:
                col_name = f"h{parent_res}"
                parent_cols.append(f"h3_cell_to_parent({h3_col}, {parent_res}) AS {col_name}")
        
        parent_cols_str = ', ' + ', '.join(parent_cols) if parent_cols else ''
        
        # UNNEST with only ID + h3 cells (minimal RAM usage!)
        # Use original ID column name for output consistency
        final_sql = f"""
            SELECT "{id_col}", 
                   UNNEST(h3id) AS {h3_col}{parent_cols_str}
            FROM ({h3_sql})
        """
        
        # Generate output file path
        output_file = f"{self.output_url}/chunk_{chunk_id:06d}.parquet"
        
        # Write with compression and row group optimization
        self.con.execute(f"""
            COPY ({final_sql}) 
            TO '{output_file}' 
            (FORMAT PARQUET, 
             COMPRESSION 'ZSTD',
             ROW_GROUP_SIZE 100000)
        """)
        
        print(f"  ✓ Chunk {chunk_id} written to {output_file}")
        return output_file
    
    def process_all_chunks(self) -> List[str]:
        """
        Process all chunks in the dataset.
        
        Returns:
            List of output file paths
        """
        # Get total rows
        total_rows = self.con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{self.input_url}')"
        ).fetchone()[0]
        
        num_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
        
        print(f"Processing {total_rows} rows in {num_chunks} chunks...")
        
        output_files = []
        for chunk_id in range(num_chunks):
            output_file = self.process_chunk(chunk_id)
            if output_file:
                output_files.append(output_file)
        
        return output_files


def process_vector_chunks(
    input_url: str,
    output_url: str,
    chunk_id: Optional[int] = None,
    h3_resolution: int = 10,
    parent_resolutions: Optional[List[int]] = None,
    chunk_size: int = 500,
    **kwargs
) -> Optional[List[str]]:
    """
    Convenience function to process vector data into H3-indexed chunks.
    
    Args:
        input_url: S3 URL or local path to input file
        output_url: S3 URL or local path to output directory
        chunk_id: Specific chunk to process, or None to process all
        h3_resolution: Target H3 resolution
        parent_resolutions: List of parent resolutions to include
        chunk_size: Number of rows per chunk
        **kwargs: Additional arguments passed to H3VectorProcessor
        
    Returns:
        List of output file paths, or None if single chunk was out of range
    """
    processor = H3VectorProcessor(
        input_url=input_url,
        output_url=output_url,
        h3_resolution=h3_resolution,
        parent_resolutions=parent_resolutions,
        chunk_size=chunk_size,
        **kwargs
    )
    
    if chunk_id is not None:
        output_file = processor.process_chunk(chunk_id)
        return [output_file] if output_file else None
    else:
        return processor.process_all_chunks()
