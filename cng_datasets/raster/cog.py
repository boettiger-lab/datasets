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
from osgeo import gdal, ogr, osr
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


# Area-weighted reducers supported by the H3 hex aggregator (#84).
# Used both for runtime validation in RasterProcessor.__init__ and as
# argparse `choices=` in the CLI.
VALID_HEX_REDUCERS = ("sum", "mean", "mode")


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
    if path.startswith("https://") or path.startswith("http://"):
        return f"/vsicurl/{path}"
    return path


def is_cog(url: str) -> bool:
    """Check if a raster is a Cloud-Optimized GeoTIFF.

    Returns True if the raster has tiled blocks and internal overviews (COG structure).
    Returns True (fail-safe) if the file cannot be opened — avoids unnecessary preprocess
    steps when network access is unavailable at workflow-generation time.
    Returns False only when the file is confirmed to be non-COG.

    Args:
        url: Path to raster file (s3://, https://, or local path).

    Returns:
        True if COG (or unverifiable), False if confirmed non-COG.
    """
    try:
        vsi_path = _ensure_vsi_path(url, use_public_endpoint=True)
        ds = gdal.Open(vsi_path)
        if ds is None:
            return True  # Can't check — assume COG
        band = ds.GetRasterBand(1)
        # Must have internal overviews
        if band.GetOverviewCount() == 0:
            return False
        # Must have tiled (not stripped) blocks
        block_x, _ = band.GetBlockSize()
        if block_x == ds.RasterXSize:  # stripped layout
            return False
        return True
    except Exception:
        return True  # Can't check — assume COG


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

        # Write intermediate GTiff, build overviews, then write final COG.
        # Writing directly to COG via auto-overview generation is unreliable for S3
        # output and for large files; the explicit BuildOverviews + COPY_SRC_OVERVIEWS
        # pattern is the recommended approach for fast GDAL downsampling (issue #25).
        print(f"  Writing intermediate GTiff...")
        tmp_tif = os.path.join(workdir, "intermediate.tif")
        tmp_opts = gdal.TranslateOptions(
            format="GTiff",
            creationOptions=["COMPRESS=NONE", "BIGTIFF=IF_SAFER", "NUM_THREADS=ALL_CPUS"],
        )
        result = gdal.Translate(tmp_tif, merged_vrt, options=tmp_opts)
        if result is None:
            raise RuntimeError(f"gdal.Translate (GTiff) failed: {gdal.GetLastErrorMsg()}")
        result = None

        print(f"  Building overviews...")
        tmp_ds = gdal.Open(tmp_tif, gdal.GA_Update)
        tmp_ds.BuildOverviews("AVERAGE", [2, 4, 8, 16, 32, 64])
        tmp_ds = None

        print(f"  Writing COG → {output_path}...")
        cog_output = _ensure_vsi_path(output_path)
        # COG driver requires random-write access; /vsis3/ needs this config option.
        if cog_output.startswith("/vsis3/"):
            gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES")
        translate_opts = gdal.TranslateOptions(
            format="COG",
            creationOptions=[
                f"COMPRESS={compression.upper()}",
                "PREDICTOR=YES",
                "COPY_SRC_OVERVIEWS=YES",
                "OVERVIEW_RESAMPLING=AVERAGE",
                "BIGTIFF=IF_SAFER",
                "NUM_THREADS=ALL_CPUS",
            ],
        )
        result = gdal.Translate(cog_output, tmp_tif, options=translate_opts)
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
        hex_resampling: str = "mean",
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
            resampling: Resampling method for COG creation (default: "nearest")
            hex_resampling: Area-weighted reducer for aggregating source
                pixels into each H3 cell. One of: "sum" (counts/stocks like
                population), "mean" (intensities like NDVI), "mode" (categorical
                like land cover). Default: "mean". This replaces the older
                GDAL-Warp resampling enum (#84).
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

        # Warn if input is in a projected CRS — reprojection to EPSG:4326 will happen
        # internally, but a projected input can cause silent failures if PROJ is misconfigured.
        _ds = gdal.Open(self.input_path)
        if _ds is not None:
            _srs = osr.SpatialReference(wkt=_ds.GetProjection())
            _ds = None
            if not _srs.IsGeographic():
                _srs.AutoIdentifyEPSG()
                _epsg = _srs.GetAuthorityCode(None)
                _crs_name = f"EPSG:{_epsg}" if _epsg else _srs.GetName() or "unknown projected CRS"
                print(
                    f"⚠ Input raster is in a projected CRS ({_crs_name}), not WGS84/EPSG:4326.\n"
                    f"  It will be reprojected to EPSG:4326 during processing.\n"
                    f"  For best results, reproject first: "
                    f"gdalwarp -t_srs EPSG:4326 input.tif output-wgs84.tif"
                )

        self.output_cog_path = output_cog_path
        self.output_parquet_path = output_parquet_path
        self.h0_index = h0_index
        self.h0_grid_path = h0_grid_path
        self.value_column = value_column
        self.compression = compression
        self.blocksize = blocksize
        self.resampling = resampling
        # Area-weighted aggregation supports only these reducers. Old GDAL-Warp
        # values (average/near/bilinear/cubic) are rejected; #84 removed the
        # warp step entirely.
        if hex_resampling not in VALID_HEX_REDUCERS:
            raise ValueError(
                f"hex_resampling must be one of {list(VALID_HEX_REDUCERS)}, "
                f"got {hex_resampling!r}. The previous GDAL-Warp resampling "
                f"values (average, near, bilinear, cubic) are no longer supported "
                f"after #84 — see docs/raster_processing.md for migration."
            )
        self.hex_resampling = hex_resampling
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
                    print(f"ℹ Using coarser resolution h{h3_resolution} (user specified) instead of detected h{detected_resolution}")
                    print(f"  Note: Coarser resolution will aggregate more pixels per H3 cell")
                else:
                    print(f"ℹ Using finer resolution h{h3_resolution} (user specified) instead of detected h{detected_resolution}")
                    print(f"  Note: Finer resolution will create more H3 cells and larger output files")
            else:
                print(f"✓ Using h{h3_resolution} (matches auto-detected resolution)")
        
        self.parent_resolutions = parent_resolutions or []
        
        # Set up DuckDB connection
        self.con = self._setup_duckdb()

        # Pre-compute source raster bounds in EPSG:4326 for fast h0 intersection checks
        self._src_bounds_4326 = self._compute_src_bounds_4326()
    
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

    def _compute_src_bounds_4326(self) -> tuple:
        """Return (xmin, ymin, xmax, ymax) of the source raster in EPSG:4326."""
        ds = gdal.Open(self.input_path)
        if ds is None:
            raise ValueError(f"Could not open raster to compute bounds: {self.input_path}")
        gt = ds.GetGeoTransform()
        xmin = gt[0]
        xmax = gt[0] + gt[1] * ds.RasterXSize
        ymax = gt[3]
        ymin = gt[3] + gt[5] * ds.RasterYSize
        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(ds.GetProjection())
        src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        ds = None

        if src_srs.IsGeographic():
            return (min(xmin, xmax), min(ymin, ymax), max(xmin, xmax), max(ymin, ymax))

        tgt_srs = osr.SpatialReference()
        tgt_srs.ImportFromEPSG(4326)
        tgt_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        transform = osr.CoordinateTransformation(src_srs, tgt_srs)
        corners = [
            transform.TransformPoint(xmin, ymin),
            transform.TransformPoint(xmin, ymax),
            transform.TransformPoint(xmax, ymin),
            transform.TransformPoint(xmax, ymax),
        ]
        lons = [c[0] for c in corners]
        lats = [c[1] for c in corners]
        return (min(lons), min(lats), max(lons), max(lats))

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

        cog_creation_opts = [
            f'COMPRESS={self.compression.upper()}',
            f'BLOCKSIZE={self.blocksize}',
            'BIGTIFF=IF_SAFER',
            'NUM_THREADS=ALL_CPUS',
        ]

        # Reproject to EPSG:4326 if needed
        ds = gdal.Open(self.input_path)
        if ds is None:
            raise ValueError(f"Could not open input raster: {self.input_path}")

        srs = osr.SpatialReference(wkt=ds.GetProjection())
        needs_reprojection = not srs.IsGeographic()
        ds = None

        workdir = tempfile.mkdtemp(prefix="create_cog_")
        try:
            tmp_tif = os.path.join(workdir, "intermediate.tif")

            if needs_reprojection:
                print("  Reprojecting to EPSG:4326...")
                warp_options = gdal.WarpOptions(
                    dstSRS='EPSG:4326',
                    format='GTiff',
                    creationOptions=['COMPRESS=NONE', 'BIGTIFF=IF_SAFER'],
                    resampleAlg=self.resampling,
                    multithread=True,
                )
                result = gdal.Warp(tmp_tif, self.input_path, options=warp_options)
            else:
                result = gdal.Translate(
                    tmp_tif,
                    self.input_path,
                    format='GTiff',
                    creationOptions=['COMPRESS=NONE', 'BIGTIFF=IF_SAFER'],
                )

            if result is None:
                raise RuntimeError(f"Failed to create intermediate GTiff: {gdal.GetLastErrorMsg()}")
            result = None

            if overviews:
                print("  Building overviews...")
                tmp_ds = gdal.Open(tmp_tif, gdal.GA_Update)
                tmp_ds.BuildOverviews(overview_resampling.upper(), [2, 4, 8, 16, 32, 64])
                tmp_ds = None
                cog_creation_opts.append('COPY_SRC_OVERVIEWS=YES')
                cog_creation_opts.append(f'OVERVIEW_RESAMPLING={overview_resampling.upper()}')

            print(f"  Writing COG...")
            vsi_output = _ensure_vsi_path(output_path)
            # COG driver requires random-write access; /vsis3/ needs this config option.
            if vsi_output.startswith("/vsis3/"):
                gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES")
            result = gdal.Translate(
                vsi_output,
                tmp_tif,
                format='COG',
                creationOptions=cog_creation_opts,
            )
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

        if result is None:
            raise RuntimeError(f"Failed to create COG: {gdal.GetLastErrorMsg()}")

        result = None  # Close dataset
        
        print(f"  ✓ COG created: {output_path}")
        return output_path

    def _hex_aggregate_h0(self, h0_geom_wkt: str, h0_cell: int) -> Optional[str]:
        """Area-weighted aggregation of source raster into native H3 cells
        inside one h0 partition.

        Uses exactextract for fractional-pixel coverage so SUM/mean/mode
        are mass-conserving regardless of source-pixel vs hex-pitch ratio.
        Returns the output parquet path, or None if no cells produced values.
        """
        import geopandas as gpd
        import rasterio
        from shapely import wkt as shapely_wkt
        from exactextract import exact_extract

        h3_col = f"h{self.h3_resolution}"
        # Polyfill h0 -> native cells, then materialize each cell's boundary.
        # The polyfill is bounded by the h0 cell, which caps the number of cells
        # per call at ~5^(h3_resolution) - comfortably millions at h9 but
        # tens-of-millions at h11. If memory becomes a concern at fine
        # resolutions, this query can be chunked by an intermediate parent;
        # not done here because current workflows top out at h9-h10.
        cells_df = self.con.execute(f"""
            WITH native_cells AS (
                SELECT UNNEST(
                    h3_polygon_wkt_to_cells('{h0_geom_wkt}', {self.h3_resolution})
                ) AS cell
            )
            SELECT
                cell AS {h3_col},
                h3_cell_to_boundary_wkt(cell) AS boundary_wkt
            FROM native_cells
        """).fetchdf()

        if len(cells_df) == 0:
            print(f"  ℹ h0 {h0_cell}: no h{self.h3_resolution} cells in polyfill")
            return None

        cells_df["geometry"] = cells_df["boundary_wkt"].apply(shapely_wkt.loads)
        gdf = gpd.GeoDataFrame(
            cells_df.drop(columns=["boundary_wkt"]),
            geometry="geometry",
            crs="EPSG:4326",
        )

        # exactextract needs the raster opened with the right nodata.
        # Open via rasterio so we can override nodata if the user passed --nodata.
        with rasterio.open(self.input_path) as rast:
            needs_vrt = self.nodata_value is not None and rast.nodata != self.nodata_value

        vrt_path = None
        try:
            if needs_vrt:
                vrt_path = f"/tmp/raster_{h0_cell}_nodata.vrt"
                gdal.Translate(
                    vrt_path,
                    self.input_path,
                    format="VRT",
                    noData=self.nodata_value,
                )
                rast_arg = vrt_path
            else:
                rast_arg = self.input_path

            results = exact_extract(
                rast=rast_arg,
                vec=gdf,
                ops=[self.hex_resampling],
                output="pandas",
                include_cols=[h3_col],
            )
        finally:
            if vrt_path is not None and os.path.exists(vrt_path):
                os.remove(vrt_path)

        # exactextract names the output column "{band_label}_{op}". For a
        # single-band raster the label is "band_1". Normalize to value_column.
        op_col = [c for c in results.columns if c.endswith(f"_{self.hex_resampling}")]
        if not op_col:
            raise RuntimeError(
                f"exactextract returned no '{self.hex_resampling}' column; "
                f"got {list(results.columns)}"
            )
        results = results.rename(columns={op_col[0]: self.value_column})

        # Drop cells that produced no covered pixels (all-nodata under the cell).
        results = results[results[self.value_column].notna()]
        if len(results) == 0:
            print(f"  ℹ h0 {h0_cell}: no cells produced values (all nodata)")
            return None

        # Write to DuckDB to add parent columns and emit parquet.
        self.con.register("hex_values", results)

        parent_exprs = []
        for parent_res in sorted(self.parent_resolutions):
            if parent_res < self.h3_resolution:
                col_name = f"h{parent_res}"
                parent_exprs.append(
                    f"h3_cell_to_parent({h3_col}, {parent_res}) AS {col_name}"
                )
        parent_sql = ", " + ", ".join(parent_exprs) if parent_exprs else ""

        output_path = (
            f"{self.output_parquet_path.rstrip('/')}/h0={h0_cell}/data_0.parquet"
        )
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        self.con.execute(f"""
            COPY (
                SELECT {self.value_column}, {h3_col}{parent_sql}
                FROM hex_values
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """)
        self.con.unregister("hex_values")

        print(f"  ✓ Wrote: {output_path} ({len(results)} cells)")
        return output_path

    def process_h0_region(self, h0_index: Optional[int] = None) -> Optional[str]:
        """
        Process a single h0 region to H3-indexed parquet.

        Polyfills the h0 cell to its native-resolution H3 children and
        area-weighted-aggregates source-raster values into each cell via
        exactextract. Parent resolutions are added as decoration columns.
        
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

        # Skip h0 cells with no overlap with the source raster — avoids a
        # full-globe polyfill over a raster that contributes nothing.
        h0_geom = ogr.CreateGeometryFromWkt(h0_geom_wkt)
        h0_env = h0_geom.GetEnvelope()  # (xmin, xmax, ymin, ymax)
        src = self._src_bounds_4326   # (xmin, ymin, xmax, ymax)
        if src[2] < h0_env[0] or src[0] > h0_env[1] or src[3] < h0_env[2] or src[1] > h0_env[3]:
            print(f"  ℹ No overlap between source raster and h0 cell {h0_index}, skipping")
            return None

        # Area-weighted aggregation into native H3 cells (issue #84).
        # The previous gdal.Warp + XYZ + centroid-assignment path emitted
        # one row per warped pixel and lost mass at hex boundaries; replaced
        # with exact_extract over per-cell polygons.
        return self._hex_aggregate_h0(h0_geom_wkt, h0_cell)
    
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
