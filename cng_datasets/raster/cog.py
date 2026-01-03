"""
Cloud-Optimized GeoTIFF (COG) creation and raster processing.

Tools for converting raster datasets to COG format and subsequently
to H3-indexed parquet files partitioned by h0 cells.
"""

from typing import Optional, Dict, Any, List
import os
import math
import duckdb
from osgeo import gdal, osr
from cng_datasets.storage.s3 import configure_s3_credentials

# Configure GDAL environment before any operations
if 'GDAL_DATA' in os.environ:
    gdal.SetConfigOption('GDAL_DATA', os.environ['GDAL_DATA'])
if 'PROJ_LIB' in os.environ:
    gdal.SetConfigOption('PROJ_LIB', os.environ['PROJ_LIB'])

# Set GDAL to use exceptions for better error handling
gdal.UseExceptions()


def _ensure_vsi_path(path: str, use_public_endpoint: bool = False) -> str:
    """Convert path to appropriate GDAL VSI notation.
    
    Args:
        path: Input path (s3://, https://, or local)
        use_public_endpoint: If True, convert s3:// to /vsicurl/ with public HTTPS URL
                            for single-file reads (faster for public data)
    
    Returns:
        Path in GDAL VSI notation
    """
    if path.startswith("s3://"):
        if use_public_endpoint:
            # Use public HTTPS endpoint with /vsicurl/ for single file reads
            bucket_path = path[5:]  # Remove s3://
            return f"/vsicurl/https://s3-west.nrp-nautilus.io/{bucket_path}"
        else:
            # Use /vsis3/ for writes and multi-file operations
            return f"/vsis3/{path[5:]}"
    return path


def detect_nodata_value(raster_path: str, verbose: bool = True) -> Optional[float]:
    """
    Detect NoData value from raster metadata.
    
    Args:
        raster_path: Path to raster file (can be /vsis3/ URL)
        verbose: Whether to print detection message (default: True)
        
    Returns:
        NoData value if found, None otherwise
    """
    # Use public endpoint for single-file reads
    raster_path = _ensure_vsi_path(raster_path, use_public_endpoint=True)
    ds = gdal.Open(raster_path)
    if ds is None:
        raise ValueError(f"Could not open raster: {raster_path}")
    
    # Get the first band
    band = ds.GetRasterBand(1)
    nodata_value = band.GetNoDataValue()
    
    ds = None
    
    if nodata_value is not None and verbose:
        print(f"✓ Auto-detected NoData value: {nodata_value}")
    elif verbose:
        print("ℹ No NoData value found in raster metadata")
    
    return nodata_value


def detect_optimal_h3_resolution(raster_path: str, verbose: bool = True) -> int:
    """
    Detect optimal H3 resolution based on raster resolution.
    
    Uses the finest pixel dimension to recommend an H3 resolution.
    H3 average edge lengths (from https://h3geo.org/docs/core-library/restable):
    - h15: 0.58m, h14: 1.5m, h13: 4.1m, h12: 10.8m, h11: 28.7m
    - h10: 75.9m, h9: 200.8m, h8: 531.4m, h7: 1.4km, h6: 3.7km
    - h5: 9.9km, h4: 26.1km, h3: 69.0km, h2: 182.5km, h1: 483.1km, h0: 1281.3km
    
    Args:
        raster_path: Path to raster file (can be /vsis3/ URL)
        verbose: Whether to print detection message (default: True)
        
    Returns:
        Recommended H3 resolution (0-15)
    """
    # Use public endpoint for single-file reads
    raster_path = _ensure_vsi_path(raster_path, use_public_endpoint=True)
    ds = gdal.Open(raster_path)
    if ds is None:
        raise ValueError(f"Could not open raster: {raster_path}")
    
    # Get geotransform to compute resolution
    gt = ds.GetGeoTransform()
    pixel_width = abs(gt[1])
    pixel_height = abs(gt[5])
    
    # Use finest resolution
    pixel_res_deg = min(pixel_width, pixel_height)
    
    # Convert to meters (approximate at equator: 1 degree ≈ 111km)
    pixel_res_m = pixel_res_deg * 111000
    
    ds = None
    
    # Map to H3 resolution
    # Use ~3x pixel resolution as target H3 edge length
    target_edge_m = pixel_res_m * 3
    
    # H3 average edge lengths in meters (from h3geo.org)
    h3_edge_lengths = {
        15: 0.584169, 14: 1.546100, 13: 4.092010, 12: 10.830188, 11: 28.663897,
        10: 75.863783, 9: 200.786148, 8: 531.414010, 7: 1406.475763, 6: 3724.532667,
        5: 9854.090990, 4: 26071.75968, 3: 68979.22179, 2: 182512.9565, 1: 483056.8391, 0: 1281256.011
    }
    
    # Find closest H3 resolution
    best_res = 8  # default
    min_diff = float('inf')
    
    for res, edge_m in h3_edge_lengths.items():
        diff = abs(math.log10(edge_m) - math.log10(target_edge_m))
        if diff < min_diff:
            min_diff = diff
            best_res = res
    
    if verbose:
        print(f"Raster resolution: {pixel_res_m:.1f}m → Recommended H3: {best_res}")
    return best_res


class RasterProcessor:
    """
    Process raster datasets into cloud-native formats.
    
    Converts raster data to COG format and H3-indexed parquet files
    partitioned by h0 cells, processing each h0 region separately
    for memory efficiency with global datasets.
    """
    
    def __init__(
        self,
        input_path: str,
        output_cog_path: Optional[str] = None,
        output_parquet_path: Optional[str] = None,
        h3_resolution: Optional[int] = None,
        parent_resolutions: Optional[List[int]] = None,
        h0_index: Optional[int] = None,
        value_column: str = "value",
        compression: str = "deflate",
        blocksize: int = 512,
        resampling: str = "nearest",
        nodata_value: Optional[float] = None,
        read_credentials: Optional[Dict[str, str]] = None,
        write_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the raster processor.
        
        Args:
            input_path: Path to input raster file (local or /vsis3/ URL)
            output_cog_path: Path for output COG file (optional)
            output_parquet_path: Base path for output parquet (e.g., s3://bucket/dataset/hex/)
            h3_resolution: Target H3 resolution (auto-detected if None)
            parent_resolutions: List of parent resolutions to include (e.g., [9, 8, 0])
            h0_index: Specific h0 cell index to process (0-121), or None for all
            value_column: Name for the raster value column in parquet
            compression: Compression method for COG (deflate, lzw, zstd, etc.)
            blocksize: Block size for COG tiling
            resampling: Resampling method (nearest, bilinear, cubic, etc.)
            nodata_value: NoData value to exclude from H3 conversion
            read_credentials: Dict with AWS credentials for reading
            write_credentials: Dict with AWS credentials for writing
        """
        # Use public endpoint for input reads
        self.input_path = _ensure_vsi_path(input_path, use_public_endpoint=True)
        self.output_cog_path = output_cog_path
        self.output_parquet_path = output_parquet_path
        self.h0_index = h0_index
        self.value_column = value_column
        self.compression = compression
        self.blocksize = blocksize
        self.resampling = resampling
        self.read_credentials = read_credentials
        self.write_credentials = write_credentials
        
        # Auto-detect NoData value if not specified
        if nodata_value is None:
            detected_nodata = detect_nodata_value(input_path, verbose=True)
            if detected_nodata is not None:
                self.nodata_value = detected_nodata
            else:
                self.nodata_value = None
                print("ℹ No NoData value specified or detected - all values will be included")
        else:
            self.nodata_value = nodata_value
            print(f"✓ Using user-specified NoData value: {nodata_value}")
        
        # Handle H3 resolution with informative messages
        detected_resolution = detect_optimal_h3_resolution(input_path, verbose=False)
        
        if h3_resolution is None:
            # Use auto-detected resolution
            self.h3_resolution = detected_resolution
            print(f"✓ Auto-detected H3 resolution: h{detected_resolution}")
        else:
            # User specified a resolution - compare with detection
            self.h3_resolution = h3_resolution
            
            if h3_resolution != detected_resolution:
                if h3_resolution < detected_resolution:
                    print(f"ℹ Using finer resolution h{h3_resolution} (user specified) instead of detected h{detected_resolution}")
                    print(f"  Note: Finer resolution will create more H3 cells and larger output files")
                else:
                    print(f"ℹ Using coarser resolution h{h3_resolution} (user specified) instead of detected h{detected_resolution}")
                    print(f"  Note: Coarser resolution will aggregate more pixels per H3 cell")
            else:
                print(f"✓ Using h{h3_resolution} (matches auto-detected resolution)")
        
        self.parent_resolutions = parent_resolutions or []
        
        # Set up DuckDB connection
        self.con = self._setup_duckdb()
    
    def _setup_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Set up DuckDB connection with extensions."""
        con = duckdb.connect()
        
        # Install and load extensions
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        con.execute("INSTALL h3 FROM community")
        con.execute("LOAD h3")
        
        # Configure HTTP settings
        con.execute("SET http_retries=20")
        con.execute("SET http_retry_wait_ms=5000")
        con.execute("SET temp_directory='/tmp'")
        
        # Configure S3 credentials
        configure_s3_credentials(con)
        
        return con
    
    def create_cog(
        self,
        output_path: Optional[str] = None,
        overviews: bool = True,
        overview_resampling: str = "average",
    ) -> str:
        """
        Create a Cloud-Optimized GeoTIFF from input raster.
        
        Optimized for cloud rendering in services like titiler with:
        - Internal tiling
        - Overview pyramids
        - Optimized compression
        - EPSG:4326 reprojection
        
        Args:
            output_path: Path for output COG (uses self.output_cog_path if None)
            overviews: Whether to create overview pyramids
            overview_resampling: Resampling method for overviews
            
        Returns:
            Path to created COG file
        """
        if output_path is None:
            output_path = self.output_cog_path
        
        if output_path is None:
            raise ValueError("output_path or output_cog_path must be specified")
        
        print(f"Creating COG: {output_path}")
        print(f"  Input: {self.input_path}")
        
        # Translate options for COG
        # COG driver is always tiled, so no TILED option needed
        translate_options = {
            'format': 'COG',
            'creationOptions': [
                f'COMPRESS={self.compression.upper()}',
                f'BLOCKSIZE={self.blocksize}',
                'BIGTIFF=IF_SAFER',
                'NUM_THREADS=ALL_CPUS',
            ]
        }
        
        # Add overview settings
        if overviews:
            translate_options['creationOptions'].append(
                f'RESAMPLING={overview_resampling}'
            )
        
        # Reproject to EPSG:4326 if needed
        ds = gdal.Open(self.input_path)
        if ds is None:
            raise ValueError(f"Could not open input raster: {self.input_path}")
        
        srs = osr.SpatialReference(wkt=ds.GetProjection())
        needs_reprojection = not srs.IsGeographic()
        ds = None
        
        if needs_reprojection:
            print("  Reprojecting to EPSG:4326...")
            # Use gdalwarp to reproject
            warp_options = gdal.WarpOptions(
                dstSRS='EPSG:4326',
                format='COG',
                creationOptions=translate_options['creationOptions'],
                resampleAlg=self.resampling,
                multithread=True,
            )
            result = gdal.Warp(output_path, self.input_path, options=warp_options)
        else:
            # Just translate to COG format
            result = gdal.Translate(
                output_path,
                self.input_path,
                **translate_options
            )
        
        if result is None:
            raise RuntimeError(f"Failed to create COG: {gdal.GetLastErrorMsg()}")
        
        result = None  # Close dataset
        
        print(f"  ✓ COG created: {output_path}")
        return output_path
    
    def process_h0_region(self, h0_index: Optional[int] = None) -> Optional[str]:
        """
        Process a single h0 region to H3-indexed parquet.
        
        Extracts the h0 region from the raster, converts to XYZ points,
        and generates H3 cells with parent resolutions.
        
        Args:
            h0_index: h0 cell index (0-121), uses self.h0_index if None
            
        Returns:
            Path to output parquet file, or None if region has no data
        """
        if h0_index is None:
            h0_index = self.h0_index
        
        if h0_index is None:
            raise ValueError("h0_index must be specified")
        
        print(f"\nProcessing h0 region {h0_index}...")
        
        # Load h0 polygons to get the geometry using SQL
        h0_result = self.con.execute(f"""
            SELECT h0, geom 
            FROM read_parquet('s3://public-grids/hex/h0-valid.parquet')
            WHERE i = {h0_index}
        """).fetchdf()
        
        if len(h0_result) == 0:
            print(f"  ⚠ No h0 region found for index {h0_index}")
            return None
            
        h0_geom_wkt = h0_result['geom'].iloc[0]
        h0_cell = h0_result['h0'].iloc[0]
        
        print(f"  h0 cell: {h0_cell}")
        
        # Extract region to XYZ using GDAL
        xyz_file = f"/tmp/raster_{h0_index}.xyz"
        
        print(f"  Extracting region with gdal.Warp...")
        warp_options = gdal.WarpOptions(
            dstSRS='EPSG:4326',
            cutlineWKT=h0_geom_wkt,
            cropToCutline=True,
            format='XYZ',
        )
        
        result = gdal.Warp(xyz_file, self.input_path, options=warp_options)
        
        if result is None:
            print(f"  ⚠ No data in region {h0_index}")
            return None
        
        result = None
        
        # Check if file exists and has data
        if not os.path.exists(xyz_file) or os.path.getsize(xyz_file) == 0:
            print(f"  ⚠ No data in region {h0_index}")
            return None
        
        print(f"  Converting XYZ to H3 cells...")
        
        # Read XYZ and convert to H3
        xyz_table = self.con.read_csv(
            xyz_file,
            delim=' ',
            columns={'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'FLOAT'}
        )
        
        # Filter nodata if specified
        if self.nodata_value is not None:
            xyz_table = xyz_table.filter(xyz_table.Z != self.nodata_value)
        
        # Build parent resolution columns
        h3_col = f"h{self.h3_resolution}"
        parent_cols = []
        parent_exprs = []
        
        for parent_res in sorted(self.parent_resolutions):
            if parent_res < self.h3_resolution:
                col_name = f"h{parent_res}"
                parent_cols.append(col_name)
                parent_exprs.append(f"h3_latlng_to_cell_string(Y, X, {parent_res}) AS {col_name}")
        
        parent_sql = ', ' + ', '.join(parent_exprs) if parent_exprs else ''
        
        # Generate H3 cells with parent resolutions
        output_path = f"{self.output_parquet_path}/h0={h0_cell}/data_0.parquet"
        
        query = f"""
            SELECT 
                Z AS {self.value_column},
                h3_latlng_to_cell_string(Y, X, {self.h3_resolution}) AS {h3_col}
                {parent_sql}
            FROM xyz_table
        """
        
        self.con.execute(query).write_parquet(
            output_path,
            compression='zstd'
        )
        
        # Clean up
        try:
            os.remove(xyz_file)
        except Exception as e:
            print(f"  Warning: Could not remove temp file: {e}")
        
        print(f"  ✓ Wrote: {output_path}")
        return output_path
    
    def process_all_h0_regions(self) -> List[str]:
        """
        Process all h0 regions (0-121) to H3-indexed parquet.
        
        Returns:
            List of output parquet file paths
        """
        output_files = []
        
        for h0_index in range(122):
            try:
                output_file = self.process_h0_region(h0_index)
                if output_file:
                    output_files.append(output_file)
            except Exception as e:
                print(f"  ✗ Error processing h0 {h0_index}: {e}")
        
        print(f"\n✓ Processed {len(output_files)} h0 regions")
        return output_files


def create_cog(
    input_path: str,
    output_path: str,
    compression: str = "deflate",
    blocksize: int = 512,
    overviews: bool = True,
    resampling: str = "nearest",
    **kwargs
) -> str:
    """
    Create a Cloud-Optimized GeoTIFF.
    
    Convenience function that wraps RasterProcessor.create_cog().
    
    Args:
        input_path: Path to input raster file
        output_path: Path for output COG file
        compression: Compression method (deflate, lzw, zstd, etc.)
        blocksize: Internal tile size
        overviews: Whether to create overview pyramids
        resampling: Resampling method
        **kwargs: Additional arguments passed to RasterProcessor
        
    Returns:
        Path to created COG file
    """
    processor = RasterProcessor(
        input_path=input_path,
        output_cog_path=output_path,
        compression=compression,
        blocksize=blocksize,
        resampling=resampling,
        **kwargs
    )
    
    return processor.create_cog()
