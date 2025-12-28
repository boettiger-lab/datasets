"""
H3 hexagonal tiling utilities for vector datasets.

This module provides functions to convert polygon and point geometries
into H3 hexagonal cells at specified resolutions, with support for
chunked processing of large datasets.
"""

from typing import Optional, List, Dict, Any
import duckdb
import os


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
            read_credentials: Dict with AWS credentials for reading (key, secret, region, endpoint)
            write_credentials: Dict with AWS credentials for writing (key, secret, region, endpoint)
        """
        self.input_url = input_url
        self.output_url = output_url
        self.h3_resolution = h3_resolution
        self.parent_resolutions = parent_resolutions or [9, 8, 0]
        self.chunk_size = chunk_size
        self.read_credentials = read_credentials
        self.write_credentials = write_credentials
        
        self.con = setup_duckdb_connection()
        self._configure_credentials()
        
    def _configure_credentials(self):
        """Configure S3 credentials for DuckDB using environment variables."""
        key = os.getenv("AWS_ACCESS_KEY_ID", "")
        secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
        endpoint = os.getenv("AWS_S3_ENDPOINT", "s3.amazonaws.com")
        region = os.getenv("AWS_REGION", "us-east-1")
        use_ssl = os.getenv("AWS_HTTPS", "TRUE")
        
        # Determine URL style based on endpoint
        url_style = "vhost" if "amazonaws.com" in endpoint else "path"
        
        if key and secret:
            query = f"""
            CREATE OR REPLACE SECRET s3_secret (
                TYPE S3,
                KEY_ID '{key}',
                SECRET '{secret}',
                ENDPOINT '{endpoint}',
                REGION '{region}',
                URL_COMPATIBILITY_MODE true,
                USE_SSL {use_ssl},
                URL_STYLE '{url_style}'
            );
            """
            self.con.execute(query)
    
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
        # Create a view of the source table
        self.con.execute(f"CREATE OR REPLACE VIEW source_table AS SELECT * FROM read_parquet('{self.input_url}')")
        
        # Find and rename geometry column
        geom_col = self._find_geometry_column('source_table')
        
        # Get all columns
        result = self.con.execute("SELECT * FROM source_table LIMIT 0").description
        all_columns = [col[0] for col in result]
        
        # Get column names to keep (all except geom by default)
        if keep_cols is None:
            keep_cols = [col for col in all_columns if col != geom_col]
        
        # Calculate chunk boundaries
        total_rows = self.con.execute("SELECT COUNT(*) FROM source_table").fetchone()[0]
        num_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size
        
        print(f"Total rows: {total_rows:,}")
        print(f"Chunk size: {self.chunk_size:,}")
        print(f"Number of chunks: {num_chunks}")
        
        # Check if chunk_id is valid
        if chunk_id < 0 or chunk_id >= num_chunks:
            print(f"Index {chunk_id} out of range [0, {num_chunks - 1}].")
            return None
        
        offset = chunk_id * self.chunk_size
        print(f"\nProcessing chunk {chunk_id + 1}/{num_chunks} "
              f"(rows {offset:,} to {min(offset + self.chunk_size, total_rows):,})")
        
        # Create chunk view with renamed geometry column
        col_list = ', '.join([f'"{col}"' for col in keep_cols])
        self.con.execute(f"""
            CREATE OR REPLACE VIEW chunk_table AS 
            SELECT {col_list}, {geom_col} AS geom
            FROM source_table
            LIMIT {self.chunk_size} OFFSET {offset}
        """)
        
        # Convert to H3 cells
        h3_sql = geom_to_h3_cells(
            self.con, 
            'chunk_table', 
            zoom=self.h3_resolution, 
            keep_cols=keep_cols
        )
        
        # Build final query with unnested h3 cells and parent resolutions
        h3_col = f"h{self.h3_resolution}"
        parent_cols = []
        for parent_res in sorted(self.parent_resolutions):
            if parent_res < self.h3_resolution:
                col_name = f"h{parent_res}"
                parent_cols.append(f"h3_cell_to_parent({h3_col}, {parent_res}) AS {col_name}")
        
        parent_cols_str = ', ' + ', '.join(parent_cols) if parent_cols else ''
        
        final_sql = f"""
            SELECT {col_list}, 
                   UNNEST(h3id) AS {h3_col}
                   {parent_cols_str}
            FROM ({h3_sql})
        """
        
        self.con.execute(f"COPY ({final_sql}) TO '{output_file}' (FORMAT PARQUET)")
        
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
