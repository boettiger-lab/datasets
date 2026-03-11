"""
Cloud-Optimized GeoTIFF (COG) creation and raster processing.

Tools for converting raster datasets to COG format and subsequently
to H3-indexed parquet files partitioned by h0 cells.
"""

from typing import Optional, Dict, Any, List, Union
import os
import math
import tempfile
import duckdb
from osgeo import gdal, osr
from cng_datasets.storage.s3 import configure_s3_credentials

# Set GDAL to use exceptions for better error handling
gdal.UseExceptions()

def _configure_proj():
    """Find a PROJ database with the correct schema version and configure GDAL to use it.

    The container may have multiple proj.db files from different PROJ installations.
    We use sqlite3 to check the schema minor version and pick one that is >=6,
    which is required by PROJ 9.x.
    """
    import sqlite3
    import subprocess

    try:
        result = subprocess.run(
            ["find", "/usr", "/opt", "/root", "-name", "proj.db"],
            capture_output=True, text=True, timeout=10
        )
        candidates = [p for p in result.stdout.strip().split("\n") if p]
        for path in candidates:
            try:
                conn = sqlite3.connect(path)
                row = conn.execute(
                    "SELECT value FROM metadata WHERE key='DATABASE.LAYOUT.VERSION.MINOR'"
                ).fetchone()
                conn.close()
                if row and int(row[0]) >= 6:
                    proj_dir = os.path.dirname(path)
                    os.environ["PROJ_DATA"] = proj_dir
                    os.environ["PROJ_LIB"] = proj_dir
                    gdal.SetConfigOption("PROJ_DATA", proj_dir)
                    return
            except Exception:
                continue
    except Exception:
        pass

_configure_proj()


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
            # Never hardwire endpoint - respect AWS_PUBLIC_ENDPOINT or AWS_S3_ENDPOINT env var
            endpoint = os.getenv('AWS_PUBLIC_ENDPOINT', os.getenv('AWS_S3_ENDPOINT', 's3-west.nrp-nautilus.io'))
            # Determine protocol from AWS_HTTPS env var (default TRUE for public endpoint)
            use_ssl = os.getenv('AWS_HTTPS', 'TRUE').upper() != 'FALSE'
            protocol = 'https' if use_ssl else 'http'
            return f"/vsicurl/{protocol}://{endpoint}/{bucket_path}"
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
    # Use internal endpoint so this works both inside and outside the cluster
    raster_path = _ensure_vsi_path(raster_path, use_public_endpoint=False)
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
    # Use internal endpoint so this works both inside and outside the cluster
    raster_path = _ensure_vsi_path(raster_path, use_public_endpoint=False)
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
    
    # H3 average edge lengths in Km (from https://h3geo.org/docs/core-library/restable/)
    # Converting to meters for comparison
    h3_edge_lengths_km = {
        0: 1281.256011, 1: 483.0568391, 2: 182.5129565, 3: 68.97922179,
        4: 26.07175968, 5: 9.854090990, 6: 3.724532667, 7: 1.406475763,
        8: 0.531414010, 9: 0.200786148, 10: 0.075863783, 11: 0.028663897,
        12: 0.010830188, 13: 0.004092010, 14: 0.001546100, 15: 0.000584169
    }
    h3_edge_lengths = {res: km * 1000 for res, km in h3_edge_lengths_km.items()}
    
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


def create_mosaic_cog(
    source_urls: List[str],
    output_path: str,
    target_crs: str = "EPSG:4326",
    target_extent: Optional[tuple] = None,
    target_resolution: Optional[float] = None,
    band: Optional[int] = None,
    nodata: Optional[float] = None,
    resampling: str = "bilinear",
    compression: str = "deflate",
) -> str:
    """
    Mosaic multiple raster tiles (potentially in different CRS) into a single COG.

    Handles the common case where source data is distributed as per-UTM-zone tiles
    (e.g. RAP 10m products across zones 12 and 13 for Wyoming). Groups tiles by CRS,
    warps each group to the target CRS, merges, then writes a Cloud-Optimized GeoTIFF.

    Args:
        source_urls: List of tile paths/URLs (local, /vsicurl/, s3://).
                     Tiles may be in mixed CRS (e.g. multiple UTM zones).
        output_path: Destination path for the COG (local path or s3://).
        target_crs: Output CRS (default: EPSG:4326).
        target_extent: Clip extent as (xmin, ymin, xmax, ymax) in target_crs.
                       If None, uses the union of all tile extents.
        target_resolution: Output pixel size in target_crs units (e.g. 0.0001 for ~10m
                           in degrees). If None, derived from the finest source tile.
        band: Extract a single band from multi-band sources (1-indexed). If None,
              all bands are preserved.
        nodata: NoData value for output. If None, inherited from source tiles.
        resampling: Resampling algorithm for warping (default: bilinear).
        compression: COG compression (deflate, lzw, zstd).

    Returns:
        output_path (echoed back for chaining)
    """
    if not source_urls:
        raise ValueError("source_urls must not be empty")

    print(f"Creating mosaic COG from {len(source_urls)} source tile(s)...")

    # Resolve VSI paths for all sources
    vsi_urls = [_ensure_vsi_path(u, use_public_endpoint=True) for u in source_urls]

    # Group tiles by their CRS authority string (e.g. "EPSG:32612")
    crs_groups: dict = {}
    for vsi_url in vsi_urls:
        ds = gdal.Open(vsi_url)
        if ds is None:
            print(f"  ⚠ Could not open {vsi_url}, skipping")
            continue
        srs = osr.SpatialReference(wkt=ds.GetProjection())
        srs.AutoIdentifyEPSG()
        epsg = srs.GetAuthorityCode(None)
        crs_key = f"EPSG:{epsg}" if epsg else srs.ExportToProj4()
        ds = None
        crs_groups.setdefault(crs_key, []).append(vsi_url)

    if not crs_groups:
        raise RuntimeError("No readable source tiles found")

    print(f"  CRS groups: { {k: len(v) for k, v in crs_groups.items()} }")

    workdir = tempfile.mkdtemp(prefix="mosaic_cog_")
    try:
        warped_paths = []

        warp_kwargs = dict(
            dstSRS=target_crs,
            resampleAlg=resampling,
            multithread=True,
            format="GTiff",
            creationOptions=["COMPRESS=NONE", "BIGTIFF=IF_SAFER"],
        )
        if target_extent is not None:
            xmin, ymin, xmax, ymax = target_extent
            warp_kwargs["outputBounds"] = (xmin, ymin, xmax, ymax)
            warp_kwargs["outputBoundsSRS"] = target_crs
        if target_resolution is not None:
            warp_kwargs["xRes"] = target_resolution
            warp_kwargs["yRes"] = target_resolution
        if nodata is not None:
            warp_kwargs["srcNodata"] = nodata
            warp_kwargs["dstNodata"] = nodata

        for i, (crs_key, tiles) in enumerate(crs_groups.items()):
            print(f"  Building VRT for {crs_key} ({len(tiles)} tiles)...")
            vrt_path = os.path.join(workdir, f"group_{i}.vrt")
            vrt_ds = gdal.BuildVRT(vrt_path, tiles, bandList=[band] if band else None)
            if vrt_ds is None:
                raise RuntimeError(f"gdal.BuildVRT failed for CRS group {crs_key}")
            vrt_ds = None  # flush

            warped_path = os.path.join(workdir, f"warped_{i}.tif")
            print(f"  Warping {crs_key} → {target_crs}...")
            result = gdal.Warp(warped_path, vrt_path, **warp_kwargs)
            if result is None:
                raise RuntimeError(f"gdal.Warp failed for CRS group {crs_key}: {gdal.GetLastErrorMsg()}")
            result = None
            warped_paths.append(warped_path)

        # Merge all warped groups into a final VRT
        print(f"  Merging {len(warped_paths)} warped group(s)...")
        merged_vrt = os.path.join(workdir, "merged.vrt")
        build_vrt_opts = {}
        if nodata is not None:
            build_vrt_opts["srcNodata"] = nodata
            build_vrt_opts["VRTNodata"] = nodata
        merged_ds = gdal.BuildVRT(merged_vrt, warped_paths, **build_vrt_opts)
        if merged_ds is None:
            raise RuntimeError(f"gdal.BuildVRT failed for merge: {gdal.GetLastErrorMsg()}")
        merged_ds = None

        # Write final COG
        print(f"  Writing COG → {output_path}...")
        cog_output = _ensure_vsi_path(output_path)
        translate_opts = gdal.TranslateOptions(
            format="COG",
            creationOptions=[
                f"COMPRESS={compression.upper()}",
                "PREDICTOR=YES",
                "OVERVIEW_RESAMPLING=AVERAGE",
                "BIGTIFF=IF_SAFER",
                "NUM_THREADS=ALL_CPUS",
            ],
        )
        result = gdal.Translate(cog_output, merged_vrt, options=translate_opts)
        if result is None:
            raise RuntimeError(f"gdal.Translate (COG) failed: {gdal.GetLastErrorMsg()}")
        result = None

        print(f"  ✓ Mosaic COG created: {output_path}")
        return output_path

    finally:
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)


class RasterProcessor:
    """
    Process raster datasets into cloud-native formats.
    
    Converts raster data to COG format and H3-indexed parquet files
    partitioned by h0 cells, processing each h0 region separately
    for memory efficiency with global datasets.
    """
    
    def __init__(
        self,
        input_path: Union[str, List[str]],
        output_cog_path: Optional[str] = None,
        output_parquet_path: Optional[str] = None,
        h3_resolution: Optional[int] = None,
        parent_resolutions: Optional[List[int]] = None,
        h0_index: Optional[int] = None,
        h0_grid_path: str = "s3://public-grids/hex/h0-valid.parquet",
        value_column: str = "value",
        compression: str = "deflate",
        blocksize: int = 512,
        resampling: str = "nearest",
        nodata_value: Optional[float] = None,
        target_crs: str = "EPSG:4326",
        target_extent: Optional[tuple] = None,
        target_resolution: Optional[float] = None,
        band: Optional[int] = None,
        read_credentials: Optional[Dict[str, str]] = None,
        write_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the raster processor.

        Args:
            input_path: Path(s) to input raster file(s). A single string or a list of
                        tile URLs/paths (possibly in mixed CRS — they will be mosaicked
                        and reprojected to target_crs before processing).
            output_cog_path: Path for output COG file (optional)
            output_parquet_path: Base path for output parquet (e.g., s3://bucket/dataset/hex/)
            h3_resolution: Target H3 resolution (auto-detected if None)
            parent_resolutions: List of parent resolutions to include (e.g., [9, 8, 0])
            h0_index: Specific h0 cell index to process (0-121), or None for all
            h0_grid_path: Path to h0 grid parquet file
            value_column: Name for the raster value column in parquet
            compression: Compression method for COG (deflate, lzw, zstd, etc.)
            blocksize: Block size for COG tiling
            resampling: Resampling method (nearest, bilinear, cubic, etc.)
            nodata_value: NoData value to exclude from H3 conversion
            target_crs: CRS for output (default: EPSG:4326); used when mosaicking
            target_extent: Clip extent (xmin, ymin, xmax, ymax) in target_crs; used when mosaicking
            target_resolution: Output pixel size in target_crs units; used when mosaicking
            band: Extract a single band from multi-band sources (1-indexed); used when mosaicking
            read_credentials: Dict with AWS credentials for reading
            write_credentials: Dict with AWS credentials for writing
        """
        # If a list of tiles is provided, mosaic them into a temp COG first
        self._mosaic_tmpdir = None
        if isinstance(input_path, list):
            if len(input_path) == 1:
                input_path = input_path[0]
            else:
                import tempfile
                self._mosaic_tmpdir = tempfile.mkdtemp(prefix="raster_processor_")
                mosaic_path = os.path.join(self._mosaic_tmpdir, "mosaic.tif")
                print(f"Multiple input tiles detected ({len(input_path)}), mosaicking to temp COG...")
                create_mosaic_cog(
                    source_urls=input_path,
                    output_path=mosaic_path,
                    target_crs=target_crs,
                    target_extent=target_extent,
                    target_resolution=target_resolution,
                    band=band,
                    nodata=nodata_value,
                    resampling=resampling,
                    compression=compression,
                )
                input_path = mosaic_path

        # Use public endpoint for input reads
        self.input_path = _ensure_vsi_path(input_path, use_public_endpoint=True)
        self.output_cog_path = output_cog_path
        self.output_parquet_path = output_parquet_path
        self.h0_index = h0_index
        self.h0_grid_path = h0_grid_path
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
        
        # Load h0 polygons to get the geometry using SQL with ST_AsText for WKT
        h0_result = self.con.execute(f"""
            SELECT h0, ST_AsText(geom) as geom_wkt
            FROM read_parquet('{self.h0_grid_path}')
            WHERE i = {h0_index}
        """).fetchdf()
        
        if len(h0_result) == 0:
            print(f"  ⚠ No h0 region found for index {h0_index}")
            return None
            
        h0_geom_wkt = h0_result['geom_wkt'].iloc[0]
        h0_cell = h0_result['h0'].iloc[0]
        
        print(f"  h0 cell: {h0_cell}")
        
        # Extract region to XYZ using GDAL
        xyz_file = f"/tmp/raster_{h0_index}.xyz"
        
        print(f"  Extracting region with gdal.Warp...")
        # Allow partial reprojection so h0 cutlines that extend outside the
        # source raster's projection domain (e.g. Albers-projected COGs) still
        # produce valid output for the overlap region.
        gdal.SetConfigOption('OGR_ENABLE_PARTIAL_REPROJECTION', 'TRUE')
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
        # Note: Raw DuckDB uses 'delimiter', Ibis uses 'delim'
        xyz_table = self.con.read_csv(
            xyz_file,
            delimiter=' ',
            columns={'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'FLOAT'}
        )
        
        # Build parent resolution columns
        h3_col = f"h{self.h3_resolution}"
        parent_cols = []
        parent_exprs = []
        
        for parent_res in sorted(self.parent_resolutions):
            if parent_res < self.h3_resolution:
                col_name = f"h{parent_res}"
                parent_cols.append(col_name)
                parent_exprs.append(f"h3_latlng_to_cell(Y, X, {parent_res}) AS {col_name}")
        
        parent_sql = ', ' + ', '.join(parent_exprs) if parent_exprs else ''
        
        # Add nodata filter to WHERE clause if specified
        where_clause = f"WHERE Z != {self.nodata_value}" if self.nodata_value is not None else ""
        
        # Generate H3 cells with parent resolutions
        output_path = f"{self.output_parquet_path.rstrip('/')}/h0={h0_cell}/data_0.parquet"
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        
        query = f"""
            COPY (
                SELECT 
                    Z AS {self.value_column},
                    h3_latlng_to_cell(Y, X, {self.h3_resolution}) AS {h3_col}
                    {parent_sql}
                FROM xyz_table
                {where_clause}
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """
        
        self.con.execute(query)
        
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
