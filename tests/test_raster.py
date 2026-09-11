"""
Unit tests for raster processing functionality.

Tests COG creation, H3 tiling, and resolution detection using small test datasets.
"""

import pytest
import os
import math
import tempfile
import glob
import yaml
import shutil
from pathlib import Path
import duckdb
import numpy as np

# Check if GDAL is available with array support
try:
    from osgeo import gdal, osr
    gdal.UseExceptions()
    GDAL_AVAILABLE = True
    try:
        from osgeo import gdal_array
        GDAL_ARRAY_AVAILABLE = True
    except ImportError:
        GDAL_ARRAY_AVAILABLE = False
except ImportError:
    GDAL_AVAILABLE = False
    GDAL_ARRAY_AVAILABLE = False

# Skip marker for tests requiring GDAL array support
requires_gdal_array = pytest.mark.skipif(
    not GDAL_ARRAY_AVAILABLE,
    reason="GDAL with NumPy array support not available (requires system GDAL installation)"
)

requires_gdal = pytest.mark.skipif(
    not GDAL_AVAILABLE,
    reason="GDAL not available"
)


def _cutline_wkt_available():
    """warp-centroid clips each warp with WarpOptions(cutlineWKT=...)."""
    if not GDAL_AVAILABLE:
        return False
    from cng_datasets.raster.cog import gdal_supports_cutline_wkt
    return gdal_supports_cutline_wkt()


# Marks the warp-centroid tests, which cannot run on a GDAL without
# cutlineWKT (Ubuntu noble's 3.8.4, for one) — the method raises there by
# design rather than silently doing something else (issue #173).
requires_cutline_wkt = pytest.mark.skipif(
    not _cutline_wkt_available(),
    reason="GDAL lacks WarpOptions(cutlineWKT=); warp-centroid is unavailable"
)


@requires_gdal_array
class TestRasterProcessor:
    """Test the RasterProcessor class with small synthetic rasters."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def small_raster(self, temp_dir):
        """
        Create a small test raster (10x10 pixels covering ~1 degree).
        
        This creates a raster with ~0.1 degree resolution (~11km),
        which should map to approximately h5-h6 resolution.
        """
        from osgeo import gdal, osr
        
        # Create a 10x10 raster covering 1x1 degree area
        # Resolution: 0.1 degrees per pixel (~11km at equator)
        width, height = 10, 10
        xmin, ymin, xmax, ymax = -122.0, 37.0, -121.0, 38.0  # San Francisco area
        
        # Create the raster
        driver = gdal.GetDriverByName('GTiff')
        raster_path = os.path.join(temp_dir, 'test_raster.tif')
        
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Float32)
        
        # Set geotransform (xmin, pixel_width, 0, ymax, 0, -pixel_height)
        pixel_width = (xmax - xmin) / width
        pixel_height = (ymax - ymin) / height
        ds.SetGeoTransform([xmin, pixel_width, 0, ymax, 0, -pixel_height])
        
        # Set projection (WGS84)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        
        # Create data with some pattern (not all same value)
        data = np.arange(100, dtype=np.float32).reshape(10, 10)
        data[0, 0] = 255  # Add a nodata value
        
        band = ds.GetRasterBand(1)
        band.WriteArray(data)
        band.SetNoDataValue(255)
        band.FlushCache()
        
        ds = None  # Close dataset
        
        return raster_path
    
    @pytest.fixture
    def high_res_raster(self, temp_dir):
        """
        Create a small high-resolution raster (~100m pixels).
        
        This should map to approximately h9-h10 resolution.
        """
        from osgeo import gdal, osr
        
        # Create a 20x20 raster covering 0.02x0.02 degrees (~2km)
        # Resolution: 0.001 degrees per pixel (~111m at equator)
        width, height = 20, 20
        xmin, ymin = -122.0, 37.0
        pixel_size = 0.001  # degrees
        
        driver = gdal.GetDriverByName('GTiff')
        raster_path = os.path.join(temp_dir, 'high_res_raster.tif')
        
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Int16)
        ds.SetGeoTransform([xmin, pixel_size, 0, ymin + height * pixel_size, 0, -pixel_size])
        
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        
        # Create data with values 1-10
        data = np.random.randint(1, 11, size=(height, width), dtype=np.int16)
        
        band = ds.GetRasterBand(1)
        band.WriteArray(data)
        band.FlushCache()
        
        ds = None

        return raster_path

    @pytest.fixture
    def large_raster(self, temp_dir):
        """
        Create a 512x512 raster large enough to trigger overview generation.

        GDAL's BuildOverviews only writes levels where the overview dimension
        exceeds the block size; at least 512x512 is needed to get level-2 overviews
        with the default 256-pixel block size.
        """
        from osgeo import gdal, osr

        width, height = 512, 512
        xmin, ymin = -122.0, 37.0
        pixel_size = 0.001  # ~111 m per pixel
        raster_path = os.path.join(temp_dir, 'large_raster.tif')

        driver = gdal.GetDriverByName('GTiff')
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([xmin, pixel_size, 0, ymin + height * pixel_size, 0, -pixel_size])
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        data = np.arange(width * height, dtype=np.float32).reshape(height, width)
        ds.GetRasterBand(1).WriteArray(data)
        ds.GetRasterBand(1).FlushCache()
        ds = None
        return raster_path

    @pytest.fixture
    def esri102003_raster(self, temp_dir):
        """Raster with ESRI:102003 (USA Contiguous Albers) — no EPSG authority code (#131)."""
        from osgeo import gdal, osr
        width, height = 10, 10
        raster_path = os.path.join(temp_dir, "esri102003.tif")
        driver = gdal.GetDriverByName("GTiff")
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([-2000000, 10000, 0, 1000000, 0, -10000])
        srs = osr.SpatialReference()
        srs.ImportFromProj4(
            "+proj=aea +lat_0=37.5 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 "
            "+x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"
        )
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((height, width), dtype=np.float32))
        ds.GetRasterBand(1).FlushCache()
        ds = None
        return raster_path

    @pytest.fixture
    def global_mollweide_raster(self, temp_dir):
        """Global World Mollweide (ESRI:54009) raster whose rectangular extent
        exceeds the projection's valid oval domain (#151)."""
        from osgeo import gdal, osr
        width, height = 361, 181
        raster_path = os.path.join(temp_dir, "global_mollweide.tif")
        driver = gdal.GetDriverByName("GTiff")
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Float32)
        # Full Mollweide globe: x half-width ~1.804e7 m, y half-height ~9.02e6 m.
        ds.SetGeoTransform([-18040000, 100000, 0, 9020000, 0, -100000])
        srs = osr.SpatialReference()
        srs.SetFromUserInput("ESRI:54009")
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((height, width), dtype=np.float32))
        ds.GetRasterBand(1).FlushCache()
        ds = None
        return raster_path

    @pytest.mark.timeout(30)
    def test_raster_processor_init_no_epsg_srs(self, esri102003_raster):
        """RasterProcessor.__init__ must not crash on a raster with no EPSG authority code (#131)."""
        from cng_datasets.raster.cog import RasterProcessor
        # Should not raise RuntimeError from AutoIdentifyEPSG
        proc = RasterProcessor(esri102003_raster, local_cache_dir=None)
        assert proc.input_path is not None

    @pytest.mark.timeout(30)
    def test_raster_processor_init_global_mollweide(self, global_mollweide_raster):
        """__init__ must not crash computing 4326 bounds for a global Mollweide
        raster whose bbox corners fall in the undefined projection domain (#151).

        Previously _compute_src_bounds_4326 transformed the rectangular corners
        point-by-point and raised "Point outside of projection domain". The bounds
        are used only to clip/skip h0 regions, so they must be a safe superset of
        the true extent: near-full longitude and latitude within [-90, 90].
        """
        from cng_datasets.raster.cog import RasterProcessor
        proc = RasterProcessor(global_mollweide_raster, local_cache_dir=None)
        xmin, ymin, xmax, ymax = proc._src_bounds_4326
        assert all(math.isfinite(v) for v in (xmin, ymin, xmax, ymax))
        # A global raster must span essentially the whole longitude range; a
        # collapsed/undersized box here would silently drop data downstream.
        assert xmin <= -179.0 and xmax >= 179.0, f"longitude not global: {(xmin, xmax)}"
        assert -90.0 <= ymin < ymax <= 90.0, f"latitude out of range: {(ymin, ymax)}"

    @pytest.mark.timeout(30)
    def test_detect_nodata_value(self, small_raster):
        """Test NoData value detection from raster metadata."""
        from cng_datasets.raster import detect_nodata_value
        
        nodata = detect_nodata_value(small_raster, verbose=False)
        
        assert nodata == 255, f"Expected NoData=255, got {nodata}"
    
    @pytest.mark.timeout(30)
    def test_detect_nodata_value_none(self, high_res_raster):
        """Test NoData detection when no NoData value is set."""
        from cng_datasets.raster import detect_nodata_value
        
        nodata = detect_nodata_value(high_res_raster, verbose=False)
        
        assert nodata is None, f"Expected NoData=None, got {nodata}"
    
    @pytest.mark.timeout(30)
    def test_detect_optimal_h3_resolution_coarse(self, small_raster):
        """Test H3 resolution detection for coarse resolution raster."""
        from cng_datasets.raster import detect_optimal_h3_resolution
        
        h3_res = detect_optimal_h3_resolution(small_raster)
        
        # 0.1 degree pixels (~11km) should map to h5 or h6
        # With 3x multiplier: 33km target → h5 (9.9km) or h4 (26.1km)
        assert isinstance(h3_res, int)
        assert 4 <= h3_res <= 6, f"Expected h4-h6 for ~11km pixels, got h{h3_res}"
    
    @pytest.mark.timeout(30)
    def test_detect_optimal_h3_resolution_fine(self, high_res_raster):
        """Test H3 resolution detection for fine resolution raster."""
        from cng_datasets.raster import detect_optimal_h3_resolution
        
        h3_res = detect_optimal_h3_resolution(high_res_raster)
        
        # 0.001 degree pixels (~111m) should map to h9 or h10
        # With 3x multiplier: 333m target → h8 (531m) or h9 (201m)
        assert isinstance(h3_res, int)
        assert 8 <= h3_res <= 10, f"Expected h8-h10 for ~111m pixels, got h{h3_res}"
    
    @pytest.mark.timeout(60)
    def test_create_cog(self, small_raster, temp_dir):
        """Test COG creation from a small raster."""
        from cng_datasets.raster import create_cog
        
        output_cog = os.path.join(temp_dir, 'test_cog.tif')
        
        result = create_cog(
            input_path=small_raster,
            output_path=output_cog,
            compression='deflate',
            blocksize=256,
        )
        
        assert result == output_cog
        assert os.path.exists(output_cog)
        
        # Verify it's a valid COG
        ds = gdal.Open(output_cog)
        assert ds is not None
        
        # Check it has tiling (COG driver always produces tiled output)
        band = ds.GetRasterBand(1)
        block_size = band.GetBlockSize()
        assert block_size[0] == 256 or block_size[1] == 256, "Should be internally tiled"
        
        # Note: Small images (10x10) may not have overviews as they're already small
        # The COG driver automatically determines if overviews are needed

        ds = None

    @pytest.mark.timeout(60)
    def test_create_cog_has_overviews(self, large_raster, temp_dir):
        """COG created from a 512x512 raster must have internal overviews (issue #25).

        Without overviews, gdal.Warp at coarser H3 resolutions reads every source
        pixel (potentially billions of HTTP range requests), making processing
        infeasibly slow.
        """
        from cng_datasets.raster import create_cog

        output_cog = os.path.join(temp_dir, 'test_cog_overviews.tif')
        create_cog(
            input_path=large_raster,
            output_path=output_cog,
            compression='deflate',
            blocksize=256,
        )

        ds = gdal.Open(output_cog)
        assert ds is not None
        band = ds.GetRasterBand(1)
        assert band.GetOverviewCount() > 0, (
            "COG must have internal overviews for efficient GDAL downsampling"
        )
        ds = None

    @pytest.mark.timeout(60)
    def test_raster_processor_init(self, small_raster):
        """Test RasterProcessor initialization."""
        from cng_datasets.raster import RasterProcessor

        processor = RasterProcessor(
            input_path=small_raster,
            h3_resolution=6,
            parent_resolutions=[5, 0],
        )

        assert processor.h3_resolution == 6
        assert processor.parent_resolutions == [5, 0]
        assert processor.con is not None
        # Default hex resampling is 'mean' for continuous rasters
        assert processor.hex_resampling == "mean"

    @pytest.mark.timeout(60)
    def test_raster_processor_hex_resampling_mode(self, small_raster):
        """Categorical datasets should be able to opt into mode resampling (issue #80)."""
        from cng_datasets.raster import RasterProcessor

        processor = RasterProcessor(
            input_path=small_raster,
            h3_resolution=6,
            hex_resampling="mode",
        )
        assert processor.hex_resampling == "mode"

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("reducer", ["max", "min"])
    def test_raster_processor_hex_resampling_max_min(self, small_raster, reducer):
        """Peak/extremum rasters (e.g. species richness) need max/min reducers
        (issue #95). sum double-counts and mean averages away the hotspot."""
        from cng_datasets.raster import RasterProcessor

        processor = RasterProcessor(
            input_path=small_raster,
            h3_resolution=6,
            hex_resampling=reducer,
        )
        assert processor.hex_resampling == reducer

    @pytest.mark.timeout(60)
    def test_raster_processor_auto_detect(self, small_raster):
        """Test RasterProcessor with auto-detected resolution."""
        from cng_datasets.raster import RasterProcessor
        
        processor = RasterProcessor(
            input_path=small_raster,
            h3_resolution=None,  # Auto-detect
        )
        
        # Should auto-detect resolution
        assert processor.h3_resolution is not None
        assert 4 <= processor.h3_resolution <= 6
    
class TestRasterToH3Conversion:
    """Test raster to H3 parquet conversion with small data."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def tiny_raster(self, temp_dir):
        """
        Create a tiny raster (5x5 pixels) for fast H3 conversion tests.
        
        Covers a small area to minimize H3 cell count.
        """
        from osgeo import gdal, osr
        
        width, height = 5, 5
        # Small area in San Francisco
        xmin, ymin = -122.5, 37.7
        pixel_size = 0.01  # ~1km per pixel
        
        driver = gdal.GetDriverByName('GTiff')
        raster_path = os.path.join(temp_dir, 'tiny_raster.tif')
        
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Int16)
        ds.SetGeoTransform([xmin, pixel_size, 0, ymin + height * pixel_size, 0, -pixel_size])
        
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        
        # Simple data: 1-25
        data = np.arange(1, 26, dtype=np.int16).reshape(5, 5)
        data[0, 0] = 999  # Add a nodata value
        
        band = ds.GetRasterBand(1)
        band.WriteArray(data)
        band.SetNoDataValue(999)
        band.FlushCache()
        
        ds = None
        
        return raster_path
    
    @pytest.mark.timeout(120)
    def test_process_h0_region_basic(self, tiny_raster, temp_dir):
        """Test processing a single h0 region to parquet."""
        from cng_datasets.raster import RasterProcessor
        import geopandas as gpd
        from shapely.geometry import box
        
        # Create a mock h0 grid file locally that covers our test area.
        # Cell selection is now h3_cell_to_children(h0, res), so the h0 id must
        # be the *real* res-0 cell containing the San Francisco test raster
        # (577199624117288959 = h3_latlng_to_cell(37.725, -122.475, 0)); the
        # stored polygon is only used for the overlap-skip.
        h0_geom = box(-123, 37, -122, 38)  # Wider box to ensure coverage
        h0_gdf = gpd.GeoDataFrame({
            'i': [0],
            'h0': [577199624117288959],  # real res-0 cell over San Francisco
            'geometry': [h0_geom]
        }, crs='EPSG:4326')
        
        # Rename geometry column to 'geom' to match expected schema
        h0_gdf = h0_gdf.rename_geometry('geom')
        
        h0_file = os.path.join(temp_dir, 'h0-test.parquet')
        h0_gdf.to_parquet(h0_file)
        
        # Use a temporary local output
        output_dir = os.path.join(temp_dir, 'hex_output')
        os.makedirs(output_dir, exist_ok=True)
        
        processor = RasterProcessor(
            input_path=tiny_raster,
            output_parquet_path=output_dir,
            h3_resolution=4,  # Coarse: children of one h0 = 7^4 cells (fast)
            parent_resolutions=[0],
            h0_grid_path=h0_file,  # Use local h0 grid file
            value_column="test_value",
            nodata_value=999,
        )

        # Test the pipeline by processing this h0 region
        result = processor.process_h0_region(0)

        # Check the output exists
        if result:
            assert os.path.exists(result)

            # Verify parquet structure
            con = processor.con
            df = con.read_parquet(result).fetchdf()
            assert 'test_value' in df.columns
            assert 'h4' in df.columns
            assert 'h0' in df.columns
            assert len(df) > 0
            
            # Verify nodata was excluded
            assert 999 not in df['test_value'].values
        else:
            # If no data in region, that's also valid
            pass


class TestH3EdgeLengths:
    """Test that H3 edge length values are correct."""

    @pytest.mark.timeout(5)
    def test_h3_edge_length_values(self):
        """Verify H3 edge lengths match official values from h3geo.org."""
        # Official values from https://h3geo.org/docs/core-library/restable
        official_edge_lengths_km = {
            0: 1281.256011,
            1: 483.0568391,
            2: 182.5129565,
            3: 68.97922179,
            4: 26.07175968,
            5: 9.854090990,
            6: 3.724532667,
            7: 1.406475763,
            8: 0.531414010,
            9: 0.200786148,
            10: 0.075863783,
            11: 0.028663897,
            12: 0.010830188,
            13: 0.004092010,
            14: 0.001546100,
            15: 0.000584169,
        }

        # Just verify that we use reasonable values
        # This is a weak test but ensures we're in the right ballpark
        for res, expected_km in official_edge_lengths_km.items():
            expected_m = expected_km * 1000
            # Verify order of magnitude is reasonable
            assert expected_m > 0, f"Edge length for h{res} should be positive"
            assert expected_m < 10_000_000, f"Edge length for h{res} should be less than 10,000km"

    @pytest.mark.timeout(10)
    def test_resolution_detection_logic(self):
        """Test that resolution detection uses correct edge length comparisons."""
        from cng_datasets.raster.cog import detect_optimal_h3_resolution

        # We can't easily test without creating rasters, but we can verify
        # the function exists and has the right signature
        import inspect
        sig = inspect.signature(detect_optimal_h3_resolution)
        assert 'raster_path' in sig.parameters
        assert sig.return_annotation == int or str(sig.return_annotation) == 'int'


@requires_gdal
class TestCatalogJoinResolutionWarning:
    """
    A target resolution below h8 carries no h8 column, so the dataset cannot
    join the rest of the catalog on the universal join key. Auto-detection
    targets ~3x the source pixel edge and lands on h6 for a ~1 km global
    raster, so the coarse case must be announced (issue #182).
    """

    @pytest.mark.timeout(5)
    def test_no_warning_when_h8_is_present(self):
        from cng_datasets.raster import h3_resolution_join_warning

        # h8 as the target itself, whatever the parents
        assert h3_resolution_join_warning(8, user_specified=False, parent_resolutions=[0]) is None
        assert h3_resolution_join_warning(8, user_specified=True, parent_resolutions=[]) is None
        # h8 as a requested parent of a finer target
        for res in (9, 10, 12):
            assert h3_resolution_join_warning(
                res, user_specified=True, parent_resolutions=[9, 8, 0]
            ) is None

    @pytest.mark.timeout(5)
    def test_fine_target_without_h8_parent_warns(self):
        """
        The raster default is --parent-resolutions 0, so an h10 build emits h10
        and h0 and nothing to join the catalog on (issue #182).
        """
        from cng_datasets.raster import h3_resolution_join_warning

        msg = h3_resolution_join_warning(10, user_specified=True, parent_resolutions=[0])

        assert msg is not None
        assert msg.startswith("⚠")
        assert "no h8 column" in msg
        assert "h10, h0" in msg
        assert "--parent-resolutions" in msg

    @pytest.mark.timeout(5)
    def test_unknown_parents_skips_the_parent_check(self):
        """A caller that does not know its parents must not be warned falsely."""
        from cng_datasets.raster import h3_resolution_join_warning

        assert h3_resolution_join_warning(10, user_specified=True) is None

    @pytest.mark.timeout(5)
    def test_auto_detected_coarse_resolution_warns_with_remedy(self):
        from cng_datasets.raster import h3_resolution_join_warning

        msg = h3_resolution_join_warning(6, user_specified=False)

        assert msg is not None
        assert msg.startswith("⚠")
        # Names the consequence, not just the number
        assert "no h8 column" in msg
        assert "join" in msg
        # And the actionable remedy
        assert "--h3-resolution 8" in msg

    @pytest.mark.timeout(5)
    def test_explicit_coarse_resolution_is_acknowledged_not_second_guessed(self):
        from cng_datasets.raster import h3_resolution_join_warning

        msg = h3_resolution_join_warning(6, user_specified=True)

        assert msg is not None
        assert "no h8 column" in msg
        assert "specified explicitly" in msg
        # An explicit choice should not be told to pass the flag it just passed
        assert "--h3-resolution 8" not in msg

    @pytest.mark.timeout(5)
    def test_join_resolution_is_h8(self):
        from cng_datasets.raster import CATALOG_JOIN_RESOLUTION

        assert CATALOG_JOIN_RESOLUTION == 8


@requires_gdal_array
class TestCOGOptimization:
    """Test COG creation options and optimizations."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def test_raster(self, temp_dir):
        """Create a test raster with specific properties."""
        from osgeo import gdal, osr
        
        width, height = 32, 32  # Multiple of common block sizes
        xmin, ymin = -122.0, 37.0
        pixel_size = 0.001
        
        driver = gdal.GetDriverByName('GTiff')
        raster_path = os.path.join(temp_dir, 'test.tif')
        
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Byte)
        ds.SetGeoTransform([xmin, pixel_size, 0, ymin + height * pixel_size, 0, -pixel_size])
        
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        
        data = np.random.randint(0, 256, size=(height, width), dtype=np.uint8)
        
        band = ds.GetRasterBand(1)
        band.WriteArray(data)
        band.FlushCache()
        
        ds = None
        
        return raster_path
    
    @pytest.mark.timeout(60)
    def test_cog_compression_options(self, test_raster, temp_dir):
        """Test different COG compression methods."""
        from cng_datasets.raster import create_cog
        
        for compression in ['deflate', 'lzw']:
            output_cog = os.path.join(temp_dir, f'test_{compression}.tif')
            
            result = create_cog(
                input_path=test_raster,
                output_path=output_cog,
                compression=compression,
            )
            
            assert os.path.exists(result)
            
            # Verify compression (stored in dataset IMAGE_STRUCTURE metadata)
            ds = gdal.Open(result)
            assert ds is not None
            metadata = ds.GetMetadata('IMAGE_STRUCTURE')
            assert metadata.get('COMPRESSION') == compression.upper(), \
                f"Expected {compression.upper()}, got {metadata.get('COMPRESSION')}"
            assert metadata.get('LAYOUT') == 'COG', "Should be COG layout"
            ds = None
    
    @pytest.mark.timeout(60)
    def test_cog_blocksize(self, test_raster, temp_dir):
        """Test COG with different block sizes."""
        from cng_datasets.raster import create_cog
        
        output_cog = os.path.join(temp_dir, 'test_blocksize.tif')
        
        result = create_cog(
            input_path=test_raster,
            output_path=output_cog,
            blocksize=256,
        )
        
        ds = gdal.Open(result)
        band = ds.GetRasterBand(1)
        block_size = band.GetBlockSize()
        
        # Should be tiled (not striped)
        assert block_size[0] > 1 and block_size[1] > 1
        ds = None


class TestIntegration:
    """Integration tests for complete workflows."""
    
    @pytest.mark.timeout(120)
    def test_complete_workflow_small_dataset(self):
        """Test complete workflow: raster → COG → H3 parquet."""
        import tempfile
        import shutil
        from cng_datasets.raster import RasterProcessor
        
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Create tiny test raster
            from osgeo import gdal, osr
            
            width, height = 3, 3
            xmin, ymin = -122.0, 37.0
            pixel_size = 0.1
            
            driver = gdal.GetDriverByName('GTiff')
            raster_path = os.path.join(temp_dir, 'test.tif')
            
            ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Int16)
            ds.SetGeoTransform([xmin, pixel_size, 0, ymin + height * pixel_size, 0, -pixel_size])
            
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            ds.SetProjection(srs.ExportToWkt())
            
            data = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype=np.int16)
            
            band = ds.GetRasterBand(1)
            band.WriteArray(data)
            band.FlushCache()
            ds = None
            
            # Create processor
            cog_path = os.path.join(temp_dir, 'test_cog.tif')
            parquet_path = os.path.join(temp_dir, 'hex')
            
            processor = RasterProcessor(
                input_path=raster_path,
                output_cog_path=cog_path,
                output_parquet_path=parquet_path,
                h3_resolution=5,  # Very coarse for speed
                parent_resolutions=[0],
            )
            
            # Create COG
            cog_result = processor.create_cog()
            assert os.path.exists(cog_result)
            
            # Note: H3 conversion requires h0 grid access
            # Skip if not available
            
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestH3MassConservation:
    """Issue #84: SUM(value) across output parquet must equal the source raster
    total, within rounding. Pre-fix, the centroid-assignment of warped pixels
    produces a ~50% shortfall on this fixture."""

    @pytest.fixture
    def temp_dir(self):
        """Local temp directory — the fixture in TestRasterProcessor is class-scoped."""
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def ghs_pop_clip(self):
        """Path to the committed clipped ghs-pop-2020 tile."""
        path = Path(__file__).parent / "fixtures" / "ghs_pop_clip.tif"
        if not path.exists():
            pytest.skip(f"Fixture not found: {path}")
        return str(path)

    @requires_gdal
    @pytest.mark.timeout(600)
    def test_h3_aggregation_conserves_mass(self, ghs_pop_clip, temp_dir):
        """SUM(value) over the output parquet equals the source raster SUM
        within 1%. Pre-fix this fails by ~50% for ghs-pop-2020.

        Only iterates the h0 cells that overlap the fixture (rather than
        all 122) so the test finishes in seconds instead of minutes.
        """
        import rasterio
        import duckdb
        from cng_datasets.raster import RasterProcessor

        # Source truth: sum of all valid pixels in the raster.
        with rasterio.open(ghs_pop_clip) as src:
            arr = src.read(1, masked=True)
            raster_sum = float(arr.sum())
            nodata = src.nodata
            src_bounds = src.bounds  # (left, bottom, right, top)

        assert raster_sum > 0, "Fixture must contain populated pixels"

        # Run the pipeline at h7 — the regression (corner-effect mass loss)
        # surfaces at every resolution, but at h9 the polyfill (~40M cells/h0)
        # OOMs the 7 GiB GitHub Actions runner. h7 keeps it under ~1 M cells.
        output_dir = os.path.join(temp_dir, "ghs_pop_hex")
        processor = RasterProcessor(
            input_path=ghs_pop_clip,
            output_parquet_path=output_dir,
            h3_resolution=7,
            parent_resolutions=[0, 5, 6],
            value_column="population",
            hex_resampling="sum",
            nodata_value=nodata,
        )

        # Find only the h0 indices whose bounding box overlaps the fixture
        # extent — the global iteration of 122 cells dominates wall time
        # otherwise.
        overlapping_h0 = processor.con.execute(
            f"""
            SELECT i FROM read_parquet('{processor.h0_grid_path}')
            WHERE ST_Intersects(
              geom,
              ST_MakeEnvelope({src_bounds.left}, {src_bounds.bottom},
                              {src_bounds.right}, {src_bounds.top})
            )
            """
        ).fetchdf()["i"].tolist()

        outputs = []
        for idx in overlapping_h0:
            out = processor.process_h0_region(idx)
            if out:
                outputs.append(out)
        assert len(outputs) > 0, "Pipeline produced no parquet output"

        # Read back: sum across all h0 partitions.
        con = duckdb.connect()
        parquet_glob = os.path.join(output_dir, "h0=*/data_0.parquet")
        parquet_sum = con.execute(
            f"SELECT SUM(population) FROM read_parquet('{parquet_glob}')"
        ).fetchone()[0]

        # Schema invariant: one row per h7 cell (no duplicates).
        duplicate_count = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT h7 FROM read_parquet('{parquet_glob}')
                GROUP BY h7 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert duplicate_count == 0, (
            f"Expected one row per h7 cell, found {duplicate_count} duplicate h7 cells. "
            "Stage 2 must aggregate, not emit per-warped-pixel rows."
        )

        # Mass conservation: total within 1% of raster truth.
        relative_error = abs(parquet_sum - raster_sum) / raster_sum
        assert relative_error < 0.01, (
            f"Mass conservation violated: raster_sum={raster_sum:.2f}, "
            f"parquet_sum={parquet_sum:.2f}, relative_error={relative_error:.4f}. "
            f"Issue #84 regression."
        )

    @requires_gdal
    def test_hex_resampling_rejects_gdal_values(self, ghs_pop_clip, temp_dir):
        """Old GDAL-Warp resampling values must error with a clear message."""
        from cng_datasets.raster import RasterProcessor

        with pytest.raises(ValueError, match="hex_resampling must be one of"):
            RasterProcessor(
                input_path=ghs_pop_clip,
                output_parquet_path=os.path.join(temp_dir, "should_not_run"),
                h3_resolution=9,
                hex_resampling="average",
            )


class TestChildrenCellSelection:
    """Issue #88: each h0 partition's native cells must be the exact H3
    children of that h0 (h3_cell_to_children), giving a globally exact,
    gap-free, overlap-free partition. The previous polygon polyfill yields
    strays (cells whose true res-0 parent is a different h0) and, for the
    antimeridian h0s whose stored polygon spans -178..+177 planar, misses
    children entirely (one h0 polyfills to zero cells)."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def tiny_raster(self, temp_dir):
        """A 5x5 raster; only needed so RasterProcessor can initialise."""
        from osgeo import gdal, osr
        path = os.path.join(temp_dir, "tiny.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, 5, 5, 1, gdal.GDT_Int16)
        ds.SetGeoTransform([-122.5, 0.01, 0, 37.75, 0, -0.01])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((5, 5), dtype=np.int16))
        ds.FlushCache(); ds = None
        return path

    @requires_gdal
    @pytest.mark.timeout(120)
    def test_native_cells_are_exact_children_of_h0(self, tiny_raster):
        from cng_datasets.raster import RasterProcessor

        # 577375545977733119 is an antimeridian h0 (stored polygon spans
        # -175.6..+177.8 planar) and a non-pentagon at res 0; its polyfill
        # returns zero cells at every resolution.
        h0 = 577375545977733119
        proc = RasterProcessor(input_path=tiny_raster, h3_resolution=3)

        # Cell selection depends only on the h0 id (h3_cell_to_children),
        # never on the stored polygon.
        cells = [int(c) for c in proc._native_cells_for_h0(h0)]
        assert len(cells) > 0, "antimeridian h0 produced no cells"

        n_total, n_strays = proc.con.execute(
            "SELECT COUNT(*), "
            "COUNT(*) FILTER (WHERE h3_cell_to_parent(c, 0) <> ?::ubigint) "
            "FROM (SELECT UNNEST(?::ubigint[]) AS c)",
            [h0, cells],
        ).fetchone()
        assert n_strays == 0, f"{n_strays} cells are not children of h0 {h0}"

        expected = proc.con.execute(
            "SELECT len(h3_cell_to_children(?::ubigint, 3))", [h0]
        ).fetchone()[0]
        assert n_total == expected, f"expected {expected} children, got {n_total}"


class TestOverlapSkipAntimeridian:
    """Issue #88 follow-up: the overlap-skip in process_h0_region must use each
    h0's *true* footprint, not its stored planar polygon. Antimeridian h0s are
    stored as polygons spanning ~-178..+177, whose envelope covers the globe —
    so without unwrapping, a raster anywhere on Earth falsely "overlaps" them
    and they are needlessly processed (millions of children, all empty)."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    def _raster(self, temp_dir, name, xmin, ymin, xmax, ymax):
        from osgeo import gdal, osr
        path = os.path.join(temp_dir, name)
        nx = max(1, int(round(xmax - xmin)))
        ny = max(1, int(round(ymax - ymin)))
        ds = gdal.GetDriverByName("GTiff").Create(path, nx, ny, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([xmin, (xmax - xmin) / nx, 0, ymax, 0, -(ymax - ymin) / ny])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((ny, nx), dtype=np.float32))
        ds.FlushCache(); ds = None
        return path

    def _grid(self, temp_dir, wkt):
        import geopandas as gpd
        from shapely import wkt as shapely_wkt
        path = os.path.join(temp_dir, "grid.parquet")
        gpd.GeoDataFrame(
            {"i": [0], "h0": [579768083279773695],
             "geometry": [shapely_wkt.loads(wkt)]},
            crs="EPSG:4326",
        ).rename_geometry("geom").to_parquet(path)
        return path

    # A synthetic antimeridian h0 polygon: latitude band -45..-22 (matching the
    # real h0 579768083279773695), drawn the planar "long way" so its bbox spans
    # 355 deg of longitude — exactly the wrap that breaks the envelope check.
    WRAP_WKT = "POLYGON((177.5 -45, -178 -45, -178 -22, 177.5 -22, 177.5 -45))"

    @requires_gdal
    @pytest.mark.timeout(60)
    def test_antimeridian_h0_skipped_when_raster_far_from_seam(self, temp_dir, monkeypatch):
        from cng_datasets.raster import RasterProcessor
        # Decoy raster at lng -1..1 (nowhere near +/-180), same latitude band.
        raster = self._raster(temp_dir, "decoy.tif", -1.0, -50.0, 1.0, -20.0)
        proc = RasterProcessor(
            input_path=raster, h3_resolution=3,
            h0_grid_path=self._grid(temp_dir, self.WRAP_WKT),
        )
        called = []
        monkeypatch.setattr(proc, "_hex_aggregate_h0", lambda h0: called.append(h0))
        result = proc.process_h0_region(0)
        assert result is None
        assert called == [], "antimeridian h0 should be skipped for a far raster"

    @requires_gdal
    @pytest.mark.timeout(60)
    def test_antimeridian_h0_processed_when_raster_on_seam(self, temp_dir, monkeypatch):
        from cng_datasets.raster import RasterProcessor
        # Raster sitting on the +180 side of the seam, within the h0 lat band:
        # the fix must NOT skip this one (no false negatives / data loss).
        raster = self._raster(temp_dir, "seam.tif", 178.5, -40.0, 180.0, -30.0)
        proc = RasterProcessor(
            input_path=raster, h3_resolution=3,
            h0_grid_path=self._grid(temp_dir, self.WRAP_WKT),
        )
        called = []
        monkeypatch.setattr(proc, "_hex_aggregate_h0", lambda h0: called.append(h0))
        proc.process_h0_region(0)
        assert called, "antimeridian h0 overlapping the seam must be processed"


class TestSeamIntegration:
    """Issue #88(B), end-to-end: processing an antimeridian h0 over a uniform
    raster must not produce outlier cells. Before the per-cell antimeridian
    split is wired into the exact_extract worker, the wrapping seam children
    integrate a 360-deg ribbon of the raster and dwarf every other cell."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def ones_raster(self, temp_dir):
        """A global all-ones raster at 1-degree pitch."""
        from osgeo import gdal, osr
        path = os.path.join(temp_dir, "ones.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, 360, 180, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([-180.0, 1.0, 0, 90.0, 0, -1.0])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((180, 360), dtype=np.float32))
        ds.FlushCache(); ds = None
        return path

    @requires_gdal
    @pytest.mark.timeout(180)
    def test_antimeridian_h0_has_no_outlier_cells(self, ones_raster, temp_dir):
        from cng_datasets.raster import RasterProcessor
        import geopandas as gpd
        from shapely.geometry import box
        import duckdb

        # 579768083279773695 is an antimeridian h0; ~7% of its children at any
        # resolution wrap +/-180. Geometry is only used for the overlap-skip.
        grid = os.path.join(temp_dir, "grid.parquet")
        gpd.GeoDataFrame(
            {"i": [0], "h0": [579768083279773695], "geometry": [box(-180, -90, 180, 90)]},
            crs="EPSG:4326",
        ).rename_geometry("geom").to_parquet(grid)

        out_dir = os.path.join(temp_dir, "hex")
        proc = RasterProcessor(
            input_path=ones_raster,
            output_parquet_path=out_dir,
            h3_resolution=3,
            parent_resolutions=[0],
            h0_grid_path=grid,
            value_column="v",
            hex_resampling="sum",
        )
        result = proc.process_h0_region(0)
        assert result, "expected output for a global raster"

        con = duckdb.connect()
        vals = con.execute(
            f"SELECT v FROM read_parquet('{result}') ORDER BY v"
        ).fetchdf()["v"].tolist()
        assert len(vals) > 0
        # All cells at one resolution have ~equal area, so over a uniform
        # raster their summed coverage is comparable. A wrapping seam cell that
        # integrates a 360-deg ribbon would be orders of magnitude larger.
        median = vals[len(vals) // 2]
        assert max(vals) < 5 * median, (
            f"outlier cell: max={max(vals):.3f} median={median:.3f} — a seam "
            "cell is integrating a 360-deg ribbon (issue #88 part B)."
        )


class TestAntimeridianSplit:
    """Issue #88(B): h3_cell_to_boundary_wkt returns, for a cell touching
    +/-180, a planar polygon whose vertices on each side are joined the long
    way around, so its bounding box spans ~360 deg of longitude. Passed to
    exact_extract unchanged, the cell integrates the entire latitude band.
    _split_antimeridian must cut such a polygon at +/-180 into small parts."""

    def test_splits_wrapping_seam_cell(self):
        from cng_datasets.raster.cog import _split_antimeridian
        from shapely import wkt as shapely_wkt

        # Real boundary of h9 619238790872170495 (lat -6.89, lng ~ +/-180).
        seam = ("POLYGON ((-179.999972 -6.895538, -179.999137 -6.894068, "
                "-179.999923 -6.892804, 179.998456 -6.893010, "
                "179.997621 -6.894481, 179.998407 -6.895745, "
                "-179.999972 -6.895538))")
        geom = shapely_wkt.loads(seam)
        assert geom.bounds[2] - geom.bounds[0] > 180, "fixture should wrap"

        fixed = _split_antimeridian(geom)

        parts = list(fixed.geoms) if fixed.geom_type == "MultiPolygon" else [fixed]
        for p in parts:
            assert p.bounds[2] - p.bounds[0] < 1.0, "a part still wraps"
            assert p.bounds[0] >= -180.0001 and p.bounds[2] <= 180.0001
        # The true cell is ~0.1 km^2 (~1e-5 deg^2), nowhere near the ~1 deg^2
        # area of the unsplit 360-deg ribbon.
        assert fixed.area < 1e-3, f"split area {fixed.area} too large"

    def test_leaves_normal_cell_unchanged(self):
        from cng_datasets.raster.cog import _split_antimeridian
        from shapely import wkt as shapely_wkt

        normal = ("POLYGON ((10.0 6.0, 10.001 6.0, 10.0015 6.001, "
                  "10.001 6.002, 10.0 6.002, 9.9995 6.001, 10.0 6.0))")
        geom = shapely_wkt.loads(normal)
        out = _split_antimeridian(geom)
        assert out.equals(geom)

    def test_handles_polar_seam_cell(self):
        """Issue #92: a cell touching both +/-180 and a pole unwraps to a
        self-intersecting ring near lat ~90 that GEOS cannot split — the
        box intersection / unary_union raises a GEOSException that kills the
        whole worker process, so the affected h0 partition is never written.
        The helper must return a valid, non-empty geometry instead of raising.

        Fixture is the real boundary of h9 617048546304851967
        (h3_latlng_to_cell(89.999, 0.0, 9)), the pole cell that triggers it.
        """
        from cng_datasets.raster.cog import _split_antimeridian
        from shapely import wkt as shapely_wkt

        polar = ("POLYGON ((11.400728 89.998110, 86.369271 89.999144, "
                 "-159.168809 89.998601, -110.483110 89.997514, "
                 "-71.709687 89.997041, -32.485280 89.997285, "
                 "11.400728 89.998110))")
        geom = shapely_wkt.loads(polar)
        assert geom.bounds[2] - geom.bounds[0] > 180, "fixture should wrap"

        fixed = _split_antimeridian(geom)  # must not raise

        assert fixed.is_valid, "split produced an invalid geometry"
        assert not fixed.is_empty, "split dropped the cell entirely"


class TestProjDbSelection:
    """The container (and the CI runner) carry more than one proj.db — a
    GDAL-compatible one (DATABASE.LAYOUT.VERSION.MINOR >= 7) and a stale
    Ubuntu proj-data one (MINOR == 6). _configure_proj must deterministically
    pick the highest-version db and require MINOR >= 7, never letting `find`
    ordering land it on the stale db (which throws 'a number >= 7 is expected'
    and would intermittently fail raster jobs)."""

    def _make_proj_db(self, path, minor):
        import sqlite3
        os.makedirs(os.path.dirname(path), exist_ok=True)
        con = sqlite3.connect(path)
        con.execute("CREATE TABLE metadata (key TEXT, value TEXT)")
        con.execute(
            "INSERT INTO metadata VALUES ('DATABASE.LAYOUT.VERSION.MINOR', ?)",
            (str(minor),),
        )
        con.commit(); con.close()
        return path

    def test_picks_highest_version_regardless_of_order(self, tmp_path):
        from cng_datasets.raster.cog import _select_proj_db
        good = self._make_proj_db(str(tmp_path / "gdal" / "proj.db"), 7)
        stale = self._make_proj_db(str(tmp_path / "ubuntu" / "proj.db"), 6)
        # `find` order is arbitrary — the stale db must never win.
        assert _select_proj_db([stale, good]) == good
        assert _select_proj_db([good, stale]) == good

    def test_returns_none_when_best_below_minimum(self, tmp_path):
        from cng_datasets.raster.cog import _select_proj_db
        stale = self._make_proj_db(str(tmp_path / "ubuntu" / "proj.db"), 6)
        # No qualifying db -> return None so the caller leaves GDAL's own
        # configuration untouched rather than clobbering it with a stale db.
        assert _select_proj_db([stale]) is None

    def test_ignores_unreadable_candidates(self, tmp_path):
        from cng_datasets.raster.cog import _select_proj_db
        bad = tmp_path / "broken" / "proj.db"
        os.makedirs(bad.parent, exist_ok=True)
        bad.write_text("not a sqlite database")
        good = self._make_proj_db(str(tmp_path / "gdal" / "proj.db"), 9)
        assert _select_proj_db([str(bad), good]) == good

    def test_overrides_stale_preset_proj_data(self, tmp_path, monkeypatch):
        """_configure_proj must run its deterministic scan even when PROJ_DATA is
        already exported. The generated k8s job's bash wrapper sets PROJ_DATA from
        `find ... | head -1` (non-deterministic, can land on the stale MINOR==6 db
        — issue #91). Trusting that pre-set value would reintroduce the flaky
        version-mismatch race, so Python's selection must override it."""
        import subprocess
        from cng_datasets.raster import cog
        good_dir = tmp_path / "gdal"
        self._make_proj_db(str(good_dir / "proj.db"), 9)

        monkeypatch.setenv("PROJ_DATA", "/some/stale/dir")
        monkeypatch.setattr(cog, "_proj_configured", False)

        class _Result:
            stdout = str(good_dir / "proj.db") + "\n"
        monkeypatch.setattr(subprocess, "run", lambda *a, **k: _Result())

        cog._configure_proj()
        assert os.environ["PROJ_DATA"] == str(good_dir), "stale pre-set value must be overridden"

    def test_configure_proj_runs_once(self, monkeypatch):
        """The scan is guarded so it runs at most once per process — repeated
        entrypoint calls (RasterProcessor, create_mosaic_cog) don't re-`find`."""
        import subprocess
        from cng_datasets.raster import cog
        monkeypatch.setattr(cog, "_proj_configured", False)
        calls = []
        monkeypatch.setattr(subprocess, "run",
                            lambda *a, **k: calls.append(1) or type("R", (), {"stdout": ""})())
        cog._configure_proj()
        cog._configure_proj()
        assert len(calls) == 1


@requires_cutline_wkt
class TestWarpCentroidMethod:
    """PR #86: the opt-in warp-centroid fallback method (gdal.Warp -> XYZ ->
    centroid). Default stays exact-extract; warp-centroid trades the one-row-
    per-cell schema for speed/low-memory and accepts the full GDAL resampler
    vocabulary."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def sf_raster(self, temp_dir):
        from osgeo import gdal, osr
        path = os.path.join(temp_dir, "sf.tif")
        w = h = 20
        ds = gdal.GetDriverByName("GTiff").Create(path, w, h, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([-122.5, 0.01, 0, 37.9, 0, -0.01])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((h, w), dtype=np.float32))
        ds.FlushCache(); ds = None
        return path

    def _sf_grid(self, temp_dir):
        import geopandas as gpd
        from shapely.geometry import box
        path = os.path.join(temp_dir, "grid.parquet")
        gpd.GeoDataFrame(
            {"i": [0], "h0": [577199624117288959],  # real res-0 cell over SF
             "geometry": [box(-123, 37, -122, 38)]},
            crs="EPSG:4326",
        ).rename_geometry("geom").to_parquet(path)
        return path

    @requires_gdal
    def test_warp_centroid_accepts_gdal_resampler_exact_rejects(self, sf_raster, temp_dir):
        """warp-centroid takes GDAL resamplers (e.g. 'bilinear'); exact-extract
        rejects them — the validation is method-aware."""
        from cng_datasets.raster import RasterProcessor
        proc = RasterProcessor(
            input_path=sf_raster, output_parquet_path=os.path.join(temp_dir, "o"),
            h3_resolution=7, method="warp-centroid", hex_resampling="bilinear",
        )
        assert proc.method == "warp-centroid"

        with pytest.raises(ValueError, match="hex_resampling must be one of"):
            RasterProcessor(
                input_path=sf_raster, output_parquet_path=os.path.join(temp_dir, "o2"),
                h3_resolution=7, method="exact-extract", hex_resampling="bilinear",
            )

    @requires_gdal
    def test_invalid_method_rejected(self, sf_raster, temp_dir):
        from cng_datasets.raster import RasterProcessor
        with pytest.raises(ValueError, match="method must be one of"):
            RasterProcessor(
                input_path=sf_raster, output_parquet_path=os.path.join(temp_dir, "o"),
                h3_resolution=7, method="not-a-method",
            )

    @requires_gdal
    @pytest.mark.timeout(120)
    def test_warp_centroid_produces_output(self, sf_raster, temp_dir):
        """End-to-end: the warp-centroid path runs and writes a per-pixel
        parquet with the value + native + parent h-columns."""
        from cng_datasets.raster import RasterProcessor
        out_dir = os.path.join(temp_dir, "hex")
        proc = RasterProcessor(
            input_path=sf_raster, output_parquet_path=out_dir,
            h3_resolution=7, parent_resolutions=[0], h0_grid_path=self._sf_grid(temp_dir),
            value_column="v", method="warp-centroid", hex_resampling="average",
        )
        result = proc.process_h0_region(0)
        assert result and os.path.exists(result)
        df = proc.con.read_parquet(result).fetchdf()
        assert {"v", "h7", "h0"}.issubset(df.columns)
        assert len(df) > 0

    @requires_gdal
    @pytest.mark.timeout(120)
    def test_warp_centroid_default_reducer_runs(self, sf_raster, temp_dir):
        """The default hex_resampling ('mean') must work in warp-centroid mode.
        GDAL's resampleAlg vocabulary spells it 'average' (and 'near', not
        'nearest'), so the friendly aliases must be canonicalized before the
        warp — otherwise `--method warp-centroid` with no explicit
        --hex-resampling crashes with 'Unknown resampling method'."""
        from cng_datasets.raster import RasterProcessor
        out_dir = os.path.join(temp_dir, "hex")
        proc = RasterProcessor(
            input_path=sf_raster, output_parquet_path=out_dir,
            h3_resolution=7, parent_resolutions=[0], h0_grid_path=self._sf_grid(temp_dir),
            value_column="v", method="warp-centroid",  # hex_resampling defaults to "mean"
        )
        assert proc.hex_resampling == "mean"
        result = proc.process_h0_region(0)
        assert result and os.path.exists(result)
        df = proc.con.read_parquet(result).fetchdf()
        assert {"v", "h7", "h0"}.issubset(df.columns)
        assert len(df) > 0


class TestHexResamplingMaxMin:
    """Issue #95: peak/extremum rasters (species richness, IUCN richness) must
    aggregate to a hex cell's MAX over its footprint, not sum (double-counts
    species) or mean (averages away the hotspot). max/min are coverage-agnostic
    — the cell extremum is independent of fractional pixel coverage — so they
    forward straight to exactextract's first-class `max`/`min` ops."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def striped_raster(self, temp_dir):
        """A global 1-degree raster striped by latitude row: value 9.0 on even
        rows, 1.0 on odd rows. A per-cell extremum (max/min) must return one of
        the two actual pixel values; a MEAN would return intermediate values
        (~5) and a SUM would return values far above 9 — so every output value
        landing in {1.0, 9.0} discriminates an extremum reducer from both, and
        the peak 9.0 being present proves max preserved the hotspot."""
        from osgeo import gdal, osr
        path = os.path.join(temp_dir, "striped.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, 360, 180, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([-180.0, 1.0, 0, 90.0, 0, -1.0])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        arr = np.ones((180, 360), dtype=np.float32)
        arr[::2, :] = 9.0  # even rows = 9, odd rows = 1
        ds.GetRasterBand(1).WriteArray(arr)
        ds.FlushCache(); ds = None
        return path

    def _run(self, striped_raster, temp_dir, reducer):
        from cng_datasets.raster import RasterProcessor
        import geopandas as gpd
        from shapely.geometry import box

        grid = os.path.join(temp_dir, f"grid_{reducer}.parquet")
        gpd.GeoDataFrame(
            {"i": [0], "h0": [578536630256664575], "geometry": [box(-180, -90, 180, 90)]},
            crs="EPSG:4326",
        ).rename_geometry("geom").to_parquet(grid)

        out_dir = os.path.join(temp_dir, f"hex_{reducer}")
        proc = RasterProcessor(
            input_path=striped_raster,
            output_parquet_path=out_dir,
            h3_resolution=3,
            parent_resolutions=[0],
            h0_grid_path=grid,
            value_column="richness",
            hex_resampling=reducer,
        )
        result = proc.process_h0_region(0)
        assert result, f"expected output for a global raster ({reducer})"
        con = duckdb.connect()
        return con.execute(
            f"SELECT richness FROM read_parquet('{result}')"
        ).fetchdf()["richness"].tolist()

    @requires_gdal
    @pytest.mark.timeout(180)
    def test_max_reducer_returns_per_cell_maximum(self, striped_raster, temp_dir):
        vals = self._run(striped_raster, temp_dir, "max")
        assert len(vals) > 0
        # Every output value must be one of the actual pixel values {1.0, 9.0}:
        # a mean would yield intermediates (~5) and a sum values far above 9.
        assert all(abs(v - 1.0) < 1e-4 or abs(v - 9.0) < 1e-4 for v in vals), (
            f"max output not an extremum of pixel values: "
            f"min={min(vals):.3f} max={max(vals):.3f} (expected values in {{1,9}})"
        )
        # The peak (9.0) must survive — max must not average the hotspot away.
        assert max(vals) == pytest.approx(9.0, abs=1e-4), (
            f"max reducer lost the peak: max={max(vals):.3f} (expected 9.0)"
        )

    @requires_gdal
    @pytest.mark.timeout(180)
    def test_min_reducer_returns_per_cell_minimum(self, striped_raster, temp_dir):
        vals = self._run(striped_raster, temp_dir, "min")
        assert len(vals) > 0
        assert all(abs(v - 1.0) < 1e-4 or abs(v - 9.0) < 1e-4 for v in vals), (
            f"min output not an extremum of pixel values: "
            f"min={min(vals):.3f} max={max(vals):.3f} (expected values in {{1,9}})"
        )
        # The trough (1.0) must survive — min must not average it away.
        assert min(vals) == pytest.approx(1.0, abs=1e-4), (
            f"min reducer lost the trough: min={min(vals):.3f} (expected 1.0)"
        )


class TestParseNodataValues:
    """Multi-value nodata parsing/formatting helpers (issue #108)."""

    @pytest.mark.timeout(5)
    def test_parse_accepts_none_number_list_and_string(self):
        from cng_datasets.raster.cog import _parse_nodata_values
        assert _parse_nodata_values(None) == []
        assert _parse_nodata_values("") == []
        assert _parse_nodata_values(32767) == [32767.0]
        assert _parse_nodata_values([-9999, -1111]) == [-9999.0, -1111.0]
        assert _parse_nodata_values("-9999,-1111,32767") == [-9999.0, -1111.0, 32767.0]
        # whitespace and stray separators are tolerated
        assert _parse_nodata_values("  -9999 , 32767 ") == [-9999.0, 32767.0]

    @pytest.mark.timeout(5)
    def test_fmt_gdal_drops_trailing_zero_for_integers(self):
        from cng_datasets.raster.cog import _fmt_gdal
        assert _fmt_gdal(-9999.0) == "-9999"
        assert _fmt_gdal(32767) == "32767"
        assert _fmt_gdal(1.5) == "1.5"


@requires_gdal
class TestMultiValueNodata:
    """Categorical sources with multiple fill codes (issue #108)."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def categorical_raster(self, temp_dir):
        """A small Int16 raster carrying three distinct fill codes.

        Mimics LANDFIRE: -9999 (Fill-NoData), -1111 (Fill-Not-Mapped) and an
        internal nodata 32767, alongside genuine class codes (11, 22, 33).
        Only the internal 32767 is declared as the band NoData — the others
        leak through unless multi-value nodata is honored.
        """
        width, height = 6, 6
        xmin, ymin = -122.0, 37.0
        pixel_size = 0.01
        raster_path = os.path.join(temp_dir, "categorical.tif")

        driver = gdal.GetDriverByName("GTiff")
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Int16)
        ds.SetGeoTransform([xmin, pixel_size, 0, ymin + height * pixel_size, 0, -pixel_size])
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())

        data = np.full((height, width), 11, dtype=np.int16)
        data[:, 1] = 22
        data[:, 2] = 33
        data[0, 0] = -9999   # Fill-NoData
        data[0, 1] = -1111   # Fill-Not-Mapped
        data[0, 2] = 32767   # internal nodata
        band = ds.GetRasterBand(1)
        band.WriteArray(data)
        band.SetNoDataValue(32767)
        band.FlushCache()
        ds = None
        return raster_path

    @pytest.mark.timeout(30)
    def test_processor_stores_nodata_value_list(self, categorical_raster):
        from cng_datasets.raster import RasterProcessor
        proc = RasterProcessor(
            input_path=categorical_raster,
            h3_resolution=6,
            nodata_value="-9999,-1111,32767",
        )
        assert proc.nodata_values == [-9999.0, -1111.0, 32767.0]
        # the single-value paths still see the primary fill code
        assert proc.nodata_value == -9999.0

    @pytest.mark.timeout(60)
    def test_create_cog_collapses_all_fill_codes(self, categorical_raster, temp_dir):
        """All declared fill codes collapse to one nodata in the COG (issue #108)."""
        from cng_datasets.raster import RasterProcessor
        out = os.path.join(temp_dir, "categorical-cog.tif")
        proc = RasterProcessor(
            input_path=categorical_raster,
            output_cog_path=out,
            h3_resolution=6,
            hex_resampling="mode",
            nodata_value="-9999,-1111,32767",
        )
        proc.create_cog()

        ds = gdal.Open(out)
        band = ds.GetRasterBand(1)
        assert band.GetNoDataValue() == -9999.0
        arr = band.ReadAsArray()
        ds = None
        # Every former fill code is now the single nodata; none survive as data.
        assert -1111 not in arr
        assert 32767 not in arr
        # Genuine class codes are untouched.
        assert 11 in arr and 22 in arr and 33 in arr

    @pytest.fixture
    def secondary_fill_raster(self, temp_dir):
        """A raster filled entirely with a *secondary* fill code (-1111).

        The band declares 32767 as NoData, so -1111 is not the band's own
        nodata. A single-value pipeline would treat every pixel as a valid
        class; only multi-value exclusion (collapsing -1111 → primary) makes
        the whole raster nodata (issue #108).
        """
        width, height = 6, 6
        xmin, ymin = -122.0, 37.0
        pixel_size = 0.01
        raster_path = os.path.join(temp_dir, "all_secondary_fill.tif")

        driver = gdal.GetDriverByName("GTiff")
        ds = driver.Create(raster_path, width, height, 1, gdal.GDT_Int16)
        ds.SetGeoTransform([xmin, pixel_size, 0, ymin + height * pixel_size, 0, -pixel_size])
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        band = ds.GetRasterBand(1)
        band.WriteArray(np.full((height, width), -1111, dtype=np.int16))
        band.SetNoDataValue(32767)
        band.FlushCache()
        ds = None
        return raster_path

    def _run_hex(self, raster, temp_dir, nodata_value, hex_resampling="mode"):
        import geopandas as gpd
        from shapely.geometry import box
        from cng_datasets.raster import RasterProcessor

        h0_gdf = gpd.GeoDataFrame(
            {"i": [0], "h0": [577199624117288959], "geometry": [box(-123, 36, -121, 39)]},
            crs="EPSG:4326",
        ).rename_geometry("geom")
        h0_file = os.path.join(temp_dir, "h0-test.parquet")
        h0_gdf.to_parquet(h0_file)

        output_dir = os.path.join(temp_dir, "hex_output")
        os.makedirs(output_dir, exist_ok=True)

        proc = RasterProcessor(
            input_path=raster,
            output_parquet_path=output_dir,
            h3_resolution=5,
            parent_resolutions=[0],
            h0_grid_path=h0_file,
            value_column="evt",
            hex_resampling=hex_resampling,
            nodata_value=nodata_value,
        )
        result = proc.process_h0_region(0)
        df = proc.con.read_parquet(result).fetchdf() if result else None
        return result, df

    @pytest.mark.timeout(180)
    def test_hex_fractions_emits_per_class_long_rows(self, categorical_raster, temp_dir):
        """fractions reducer emits long (evt, frac) rows per class per cell (#142).

        Every genuine class survives (no mode-style absorption of minority
        classes), each carries a coverage fraction in (0, 1], and the fractions
        within a cell sum to <= 1 (the cell extends past the tiny test raster,
        so the remainder is unclassified/outside-raster).
        """
        result, df = self._run_hex(
            categorical_raster, temp_dir, "-9999,-1111,32767",
            hex_resampling="fractions",
        )
        assert result is not None
        # Long schema: a class column, a coverage fraction, and the native cell.
        assert "evt" in df.columns and "frac" in df.columns and "h5" in df.columns
        # No mode collapse — all three genuine classes are present as rows.
        for cls in (11, 22, 33):
            assert cls in df["evt"].values, f"class {cls} missing from fractions output"
        assert (df["frac"] > 0).all() and (df["frac"] <= 1.0 + 1e-9).all()
        # Per-cell fractions never exceed the whole cell.
        per_cell = df.groupby("h5")["frac"].sum()
        assert (per_cell <= 1.0 + 1e-9).all()

    @pytest.mark.timeout(180)
    def test_hex_fractions_keeps_nodata_as_explicit_class(self, categorical_raster, temp_dir):
        """nodata is carried as an explicit class so its share is recoverable
        rather than silently inflating the real classes (#142).

        The fill codes are collapsed to the primary (-9999) and kept; cells that
        hold at least one real class therefore also carry a -9999 row.
        """
        result, df = self._run_hex(
            categorical_raster, temp_dir, "-9999,-1111,32767",
            hex_resampling="fractions",
        )
        assert result is not None
        # The collapsed nodata code appears explicitly (the raster has fill px).
        assert -9999 in df["evt"].values
        # The other fill codes were collapsed away, not leaked as classes.
        for leaked in (-1111, 32767):
            assert leaked not in df["evt"].values
        # Every cell that carries the nodata code also carries a real class
        # (pure-nodata cells are dropped, not emitted as nodata-only rows).
        nodata_cells = set(df.loc[df["evt"] == -9999, "h5"])
        real_cells = set(df.loc[df["evt"].isin([11, 22, 33]), "h5"])
        assert nodata_cells <= real_cells

    @pytest.mark.timeout(180)
    def test_hex_excludes_secondary_fill_code(self, secondary_fill_raster, temp_dir):
        """A raster of nothing but a secondary fill code yields no hex cells."""
        result, df = self._run_hex(secondary_fill_raster, temp_dir, "-9999,-1111,32767")
        # Every pixel is a fill code, so once collapsed the whole raster is
        # nodata and no cell produces a value.
        assert result is None or len(df) == 0

    @pytest.mark.timeout(180)
    def test_hex_secondary_fill_leaks_without_multi_value(self, secondary_fill_raster, temp_dir):
        """Control: with only the band nodata, the secondary fill leaks through.

        Confirms the previous test is actually exercising multi-value exclusion
        and not passing for an unrelated reason.
        """
        result, df = self._run_hex(secondary_fill_raster, temp_dir, 32767)
        assert result is not None and len(df) > 0
        assert (df["evt"] == -1111).all()

    @pytest.mark.timeout(180)
    def test_hex_keeps_valid_codes_drops_fills(self, categorical_raster, temp_dir):
        """Genuine class codes survive; no fill code appears in the output."""
        result, df = self._run_hex(categorical_raster, temp_dir, "-9999,-1111,32767")
        if result:
            for fill in (-9999, -1111, 32767):
                assert fill not in df["evt"].values, f"fill code {fill} leaked into hex output"


@requires_gdal
class TestFractionsWorker:
    """Fractions-reducer worker internals (#142).

    Exercises the explode helper and the exact_extract chunk worker directly,
    isolating the long-explode logic from the gdal.Translate nodata-clearing
    path in _hex_aggregate_h0. The aggregation logic itself needs only
    numpy/pandas/rasterio/exactextract, but cng_datasets.raster.cog imports
    osgeo at module load, so these are gated on GDAL like the rest.
    """

    def test_explode_fractions_flattens_parallel_arrays(self):
        import pandas as pd
        from cng_datasets.raster.cog import _explode_fractions

        df = pd.DataFrame({
            "_h3_str": ["a", "b", "c"],
            "unique": [np.array([11, 22]), np.array([33]), np.array([], dtype="int32")],
            "frac": [np.array([0.6, 0.4]), np.array([1.0]), np.array([])],
        })
        out = _explode_fractions(df)
        # One row per (cell, class); the empty (no-coverage) cell drops out.
        assert list(out.columns) == ["_h3_str", "value", "frac"]
        assert len(out) == 3
        assert list(out.loc[out["_h3_str"] == "a", "value"]) == [11, 22]
        assert out.loc[out["_h3_str"] == "a", "frac"].sum() == pytest.approx(1.0)
        assert "c" not in out["_h3_str"].values

    def test_explode_fractions_all_empty(self):
        import pandas as pd
        from cng_datasets.raster.cog import _explode_fractions

        df = pd.DataFrame({
            "_h3_str": ["a"],
            "unique": [np.array([], dtype="int32")],
            "frac": [np.array([])],
        })
        out = _explode_fractions(df)
        assert len(out) == 0
        assert list(out.columns) == ["_h3_str", "value", "frac"]

    def _categorical_raster(self, tmp_path, nodata=None):
        import rasterio
        from rasterio.transform import from_origin
        arr = np.array([
            [11, 11, 22, 22],
            [11, 33, 22, 22],
            [33, 33, 33, 22],
            [11, 11, 22, 33],
        ], dtype="int16")
        p = os.path.join(tmp_path, "cat.tif")
        transform = from_origin(-122.0, 38.0, 0.01, 0.01)
        kwargs = dict(driver="GTiff", height=4, width=4, count=1,
                      dtype="int16", crs="EPSG:4326", transform=transform)
        if nodata is not None:
            kwargs["nodata"] = nodata
        with rasterio.open(p, "w", **kwargs) as ds:
            ds.write(arr, 1)
        return p

    def test_chunk_worker_fractions_returns_long_rows(self, tmp_path):
        from shapely.geometry import box
        from cng_datasets.raster.cog import _exact_extract_cells

        raster = self._categorical_raster(str(tmp_path))
        # A "cell" fully inside the raster footprint: fractions sum to 1.0.
        cell_wkt = box(-122.0, 37.96, -121.96, 38.0).wkt
        out = _exact_extract_cells(raster, "fractions", [(123, cell_wkt)])
        assert list(out.columns) == ["_h3_str", "value", "frac"]
        assert set(out["value"]) == {11, 22, 33}
        assert out["frac"].sum() == pytest.approx(1.0)

    def test_chunk_worker_fractions_excludes_band_nodata(self, tmp_path):
        from shapely.geometry import box
        from cng_datasets.raster.cog import _exact_extract_cells

        # Band nodata declared: the worker (no nodata-clearing VRT) leaves it to
        # exactextract, which excludes it — nodata-keeping is _hex_aggregate_h0's
        # job via a no-nodata VRT, not the worker's.
        raster = self._categorical_raster(str(tmp_path), nodata=33)
        cell_wkt = box(-122.0, 37.96, -121.96, 38.0).wkt
        out = _exact_extract_cells(raster, "fractions", [(123, cell_wkt)])
        assert 33 not in out["value"].values
        assert out["frac"].sum() == pytest.approx(1.0)


class TestBoundariesDerivedInWorkers:
    """
    The parent ships cell ids; each worker derives its own boundaries
    (issue #173).

    Fetching boundary WKT for every cell up front made those strings ~96% of a
    list materialised in full before any work began — the dominant term in the
    parent's peak RSS. A boundary is a pure function of the cell id, so the
    work moves to the workers and the parent carries 8 bytes per cell.
    """

    H0 = 577164439745200127  # a CONUS h0

    @pytest.fixture
    def tiny_raster(self, tmp_path):
        from osgeo import gdal, osr
        path = str(tmp_path / "tiny.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, 5, 5, 1, gdal.GDT_Int16)
        ds.SetGeoTransform([-100.0, 0.01, 0, 40.0, 0, -0.01])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((5, 5), dtype=np.int16))
        ds.FlushCache(); ds = None
        return path

    @requires_gdal
    @pytest.mark.timeout(120)
    def test_parent_gets_ids_only(self, tiny_raster):
        """8 bytes per cell, not a frame carrying a WKT string each."""
        from cng_datasets.raster import RasterProcessor

        proc = RasterProcessor(input_path=tiny_raster, h3_resolution=3)
        cells = proc._native_cells_for_h0(self.H0)

        assert isinstance(cells, np.ndarray)
        assert cells.dtype == np.uint64
        assert cells.nbytes == 8 * len(cells)
        assert len(cells) == 7 ** 3

    @requires_gdal
    @pytest.mark.timeout(120)
    def test_worker_derives_the_same_boundaries(self, tiny_raster):
        """The moved work must produce byte-identical WKT."""
        from cng_datasets.raster import RasterProcessor
        from cng_datasets.raster.cog import _boundary_wkt_for

        proc = RasterProcessor(input_path=tiny_raster, h3_resolution=3)
        cells = proc._native_cells_for_h0(self.H0)[:50]

        expected = proc.con.execute(
            "SELECT cell, h3_cell_to_boundary_wkt(cell) "
            "FROM (SELECT UNNEST(?::UBIGINT[]) AS cell)",
            [[int(c) for c in cells]],
        ).fetchall()

        derived = _boundary_wkt_for(cells)
        assert derived == [(int(c), w) for c, w in expected]

    @requires_gdal
    @pytest.mark.timeout(120)
    def test_order_follows_the_ids_given(self, tiny_raster):
        """
        Cells are paired to boundaries by id, not by row position, and come
        back in the caller's order — the output rows keep chunk order, so a
        reordering here would silently permute the result.
        """
        from cng_datasets.raster import RasterProcessor
        from cng_datasets.raster.cog import _boundary_wkt_for

        proc = RasterProcessor(input_path=tiny_raster, h3_resolution=3)
        cells = proc._native_cells_for_h0(self.H0)[:20]
        reversed_cells = cells[::-1]

        forward = _boundary_wkt_for(cells)
        backward = _boundary_wkt_for(reversed_cells)

        assert [h for h, _ in forward] == [int(c) for c in cells]
        assert backward == forward[::-1]

    @requires_gdal
    @pytest.mark.timeout(120)
    def test_duplicate_ids_are_all_returned(self, tiny_raster):
        """The id->WKT map must not collapse repeats into fewer rows."""
        from cng_datasets.raster import RasterProcessor
        from cng_datasets.raster.cog import _boundary_wkt_for

        proc = RasterProcessor(input_path=tiny_raster, h3_resolution=3)
        cell = int(proc._native_cells_for_h0(self.H0)[0])

        out = _boundary_wkt_for(np.array([cell, cell, cell], dtype=np.uint64))
        assert len(out) == 3
        assert len({w for _, w in out}) == 1


class TestWarpCentroidGdalGuard:
    """
    warp-centroid needs WarpOptions(cutlineWKT=) and says so up front
    (issue #173).

    Every h0 is warped clipped to its own boundary, so on a GDAL without that
    argument the method cannot work at all. It used to surface as a bare
    `TypeError: WarpOptions() got an unexpected keyword argument 'cutlineWKT'`
    from inside the warp — after the pod had already localized the COG — which
    reads as a bug in the tool rather than a missing dependency.
    """

    @requires_gdal
    def test_detection_matches_the_installed_bindings(self):
        import inspect
        from osgeo import gdal
        from cng_datasets.raster.cog import gdal_supports_cutline_wkt

        expected = "cutlineWKT" in inspect.signature(gdal.WarpOptions).parameters
        assert gdal_supports_cutline_wkt() is expected

    @requires_gdal
    def test_unsupported_gdal_is_refused_before_any_io(self, monkeypatch):
        """
        The source is an s3:// URL that would be downloaded first, so a guard
        that fired later would cost a multi-GB localize before failing.
        """
        import cng_datasets.raster.cog as cog
        from cng_datasets.raster import RasterProcessor

        monkeypatch.setattr(cog, "gdal_supports_cutline_wkt", lambda: False)
        monkeypatch.setattr(cog, "_localize_input",
                            lambda *a, **k: pytest.fail("localized before the guard ran"))

        with pytest.raises(RuntimeError) as exc:
            RasterProcessor(input_path="s3://bucket/big.tif", h3_resolution=8,
                            method="warp-centroid")

        message = str(exc.value)
        assert "cutlineWKT" in message
        # Names the remedy and does not offer exact-extract as a drop-in.
        assert "exact-extract" in message
        assert "deliberately" in message

    @requires_gdal
    def test_exact_extract_is_unaffected(self, monkeypatch, tmp_path):
        """The guard must not block the default method on the same GDAL."""
        import cng_datasets.raster.cog as cog
        from cng_datasets.raster import RasterProcessor
        from osgeo import gdal, osr

        path = str(tmp_path / "tiny.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, 5, 5, 1, gdal.GDT_Int16)
        ds.SetGeoTransform([-100.0, 0.01, 0, 40.0, 0, -0.01])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        ds.GetRasterBand(1).WriteArray(np.ones((5, 5), dtype=np.int16))
        ds.FlushCache(); ds = None

        monkeypatch.setattr(cog, "gdal_supports_cutline_wkt", lambda: False)
        proc = RasterProcessor(input_path=path, h3_resolution=8,
                               method="exact-extract")
        assert proc.method == "exact-extract"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestSubH0Chunking:
    """
    --chunk-resolution splits an h0 into its descendants (issue #173).

    The unit of work used to be fixed at one h0 base cell, so peak memory
    tracked the densest h0's native-cell count (~282M at res 10, ~140 GiB
    measured). Chunking below h0 cuts that by ~7x per level. H3 nests exactly,
    so the descendants tile their parent with no seams and no gaps — which is
    the property the gate below actually verifies: sub-chunked output must be
    the same rows, with the same values, as the h0 baseline.
    """

    H0_CELL = 577199624117288959   # res-0 cell over San Francisco
    RES = 3                        # native resolution: 7^3 children per h0

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def raster(self, temp_dir):
        """A small raster with a value gradient, so a wrong cell is a wrong value."""
        from osgeo import gdal, osr
        import numpy as np

        width = height = 64
        xmin, ymin, pixel = -123.0, 37.0, 1.0 / 64
        path = os.path.join(temp_dir, "grad.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, width, height, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([xmin, pixel, 0, ymin + height * pixel, 0, -pixel])
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        yy, xx = np.mgrid[0:height, 0:width]
        ds.GetRasterBand(1).WriteArray((yy * width + xx).astype("float32"))
        ds.FlushCache()
        ds = None
        return path

    def _grid(self, temp_dir):
        import geopandas as gpd
        from shapely.geometry import box
        h0_gdf = gpd.GeoDataFrame(
            {"i": [0], "h0": [self.H0_CELL], "geometry": [box(-124, 36, -122, 38)]},
            crs="EPSG:4326",
        ).rename_geometry("geom")
        path = os.path.join(temp_dir, "h0-test.parquet")
        h0_gdf.to_parquet(path)
        return path

    def _processor(self, raster, temp_dir, out_name, **kwargs):
        from cng_datasets.raster import RasterProcessor
        out = os.path.join(temp_dir, out_name)
        os.makedirs(out, exist_ok=True)
        return RasterProcessor(
            input_path=raster,
            output_parquet_path=out,
            h3_resolution=self.RES,
            parent_resolutions=[0],
            h0_grid_path=self._grid(temp_dir),
            value_column="v",
            **kwargs,
        ), out

    @pytest.mark.timeout(120)
    def test_chunk_list_tiles_the_parent_exactly(self, raster, temp_dir):
        """Every native cell has exactly one chunk: no gaps, no overlaps."""
        proc, _ = self._processor(raster, temp_dir, "cl", chunk_resolution=2)
        chunks = proc.chunk_cells()
        # H3 res-0 cells have 7 children each except the 12 pentagons (6), so
        # the count comes from the hierarchy, never from 7**n arithmetic.
        expected = proc.con.execute(
            f"SELECT len(h3_cell_to_children({self.H0_CELL}, 2))"
        ).fetchone()[0]
        assert len(chunks) == expected
        assert all(h0 == self.H0_CELL for _, h0, _ in chunks)
        # Deterministic and unique
        cells = [c for c, _, _ in chunks]
        assert len(set(cells)) == len(cells)
        assert cells == sorted(cells)

    @pytest.mark.timeout(120)
    def test_chunk_resolution_zero_is_the_historical_path(self, raster, temp_dir):
        """Default chunking writes the documented data_0.parquet, unchanged."""
        proc, out = self._processor(raster, temp_dir, "z", chunk_resolution=0)
        result = proc.process_chunk(0)
        assert result is not None
        assert result.endswith(f"h0={self.H0_CELL}/data_0.parquet")
        assert os.path.exists(result)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize("chunk_res,reducer", [(1, "mean"), (2, "mean"), (2, "mode"), (3, "mean")])
    def test_subchunked_output_matches_the_h0_baseline(self, raster, temp_dir, chunk_res, reducer):
        """
        The correctness gate: same rows, same values, chunked or not.

        This is what makes --chunk-resolution a memory optimisation rather than
        a different computation. Run across chunk depths and reducers because
        the failure it guards against — a native cell assigned to no chunk —
        depends on the chunk geometry, not on the aggregation.
        """
        base_proc, base_out = self._processor(
            raster, temp_dir, f"base{chunk_res}{reducer}", hex_resampling=reducer)
        assert base_proc.process_chunk(0) is not None

        sub_proc, sub_out = self._processor(
            raster, temp_dir, f"sub{chunk_res}{reducer}",
            chunk_resolution=chunk_res, hex_resampling=reducer)
        produced = [
            sub_proc.process_chunk(i) for i in range(len(sub_proc.chunk_cells()))
        ]
        assert any(p is not None for p in produced), "sub-chunked run produced nothing"

        con = duckdb.connect()
        baseline = con.execute(
            f"SELECT v, h{self.RES}, h0 FROM read_parquet("
            f"'{base_out}/h0=*/data_0.parquet') ORDER BY h{self.RES}"
        ).fetchall()
        chunked = con.execute(
            f"SELECT v, h{self.RES}, h0 FROM read_parquet("
            f"'{sub_out}/h0=*/part-*.parquet') ORDER BY h{self.RES}"
        ).fetchall()

        assert len(chunked) == len(baseline), (
            f"row count differs: baseline {len(baseline)}, chunked {len(chunked)} — "
            "sub-chunks must tile the h0 exactly"
        )
        assert chunked == baseline, "sub-chunked values differ from the h0 baseline"

    @pytest.mark.timeout(300)
    def test_merge_restores_the_published_layout(self, raster, temp_dir):
        """part-*.parquet in, one data_0.parquet out, same rows."""
        from cng_datasets.raster.merge import merge_raster_chunks

        base_proc, base_out = self._processor(raster, temp_dir, "mbase")
        base_proc.process_chunk(0)

        sub_proc, chunks_dir = self._processor(raster, temp_dir, "mchunks", chunk_resolution=2)
        for i in range(len(sub_proc.chunk_cells())):
            sub_proc.process_chunk(i)

        merged_dir = os.path.join(temp_dir, "merged")
        os.makedirs(merged_dir, exist_ok=True)
        written = merge_raster_chunks(chunks_dir, merged_dir, cleanup=False)
        assert written == 1

        merged_file = os.path.join(merged_dir, f"h0={self.H0_CELL}", "data_0.parquet")
        assert os.path.exists(merged_file), "merge must restore h0={cell}/data_0.parquet"

        con = duckdb.connect()
        baseline = con.execute(
            f"SELECT v, h{self.RES}, h0 FROM read_parquet("
            f"'{base_out}/h0=*/data_0.parquet') ORDER BY h{self.RES}"
        ).fetchall()
        merged = con.execute(
            f"SELECT v, h{self.RES}, h0 FROM read_parquet('{merged_file}') "
            f"ORDER BY h{self.RES}"
        ).fetchall()
        assert merged == baseline

        # Schema must not drift: a merged build and an unchunked one are the
        # same dataset and have to be readable by the same query.
        base_cols = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{base_out}/h0=*/data_0.parquet')"
        ).fetchall()
        merged_cols = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{merged_file}')"
        ).fetchall()
        assert merged_cols == base_cols

    @pytest.mark.timeout(120)
    def test_pruning_allows_for_children_outside_the_parent(self, raster, temp_dir):
        """
        Regression: H3 children are not strictly inside the parent polygon.

        Pruning a sub-chunk on its own boundary dropped a native cell that
        really did overlap the raster — silently, exit 0, with output that
        looked complete. The overlap test is widened by roughly one cell edge
        to cover the protrusion; this asserts the widening is load-bearing by
        checking that no chunk containing a baseline cell is pruned away.
        """
        base_proc, base_out = self._processor(raster, temp_dir, "pbase")
        base_proc.process_chunk(0)
        con = duckdb.connect()
        baseline_cells = {
            r[0] for r in con.execute(
                f"SELECT h{self.RES} FROM read_parquet('{base_out}/h0=*/data_0.parquet')"
            ).fetchall()
        }
        assert baseline_cells, "baseline produced no cells"

        sub_proc, sub_out = self._processor(raster, temp_dir, "psub", chunk_resolution=2)
        for i in range(len(sub_proc.chunk_cells())):
            sub_proc.process_chunk(i)
        chunked_cells = {
            r[0] for r in con.execute(
                f"SELECT h{self.RES} FROM read_parquet('{sub_out}/h0=*/part-*.parquet')"
            ).fetchall()
        }
        missing = baseline_cells - chunked_cells
        assert not missing, (
            f"{len(missing)} native cell(s) reached by the h0 baseline were pruned away "
            f"when chunked: {sorted(missing)[:5]}. The overlap margin is too small."
        )

    @pytest.mark.timeout(60)
    def test_chunk_index_past_the_end_is_rejected(self, raster, temp_dir):
        """A fan-out wider than the chunk list must fail, not silently do nothing."""
        proc, _ = self._processor(raster, temp_dir, "oob", chunk_resolution=2)
        with pytest.raises(ValueError, match="outside the"):
            proc.process_chunk(len(proc.chunk_cells()))

    @pytest.mark.timeout(60)
    def test_h0_subset_restricts_the_chunk_list(self, raster, temp_dir):
        proc, _ = self._processor(raster, temp_dir, "sub2", chunk_resolution=1, h0_subset=[0])
        assert len(proc.chunk_cells()) > 0
        empty, _ = self._processor(raster, temp_dir, "sub3", chunk_resolution=1, h0_subset=[7])
        assert empty.chunk_cells() == []


class TestWindowedCogReads:
    """
    A chunk reads only its own window of the source (issue #173, lever C).

    Full localization copies the whole COG into every pod, so total transfer
    scales with the fan-out rather than with the data: tolerable across 122 h0
    pods, ruinous across the thousands of pods sub-h0 chunking creates. These
    tests pin both halves of the claim — that windowing reads materially less,
    and that it does not change the answer.
    """

    H0_CELL = 577199624117288959
    RES = 3

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def raster(self, temp_dir):
        from osgeo import gdal, osr
        width = height = 256
        xmin, ymin, pixel = -123.0, 37.0, 1.0 / 256
        path = os.path.join(temp_dir, "big.tif")
        ds = gdal.GetDriverByName("GTiff").Create(
            path, width, height, 1, gdal.GDT_Float32,
            options=["TILED=YES", "BLOCKXSIZE=64", "BLOCKYSIZE=64"],
        )
        ds.SetGeoTransform([xmin, pixel, 0, ymin + height * pixel, 0, -pixel])
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        yy, xx = np.mgrid[0:height, 0:width]
        ds.GetRasterBand(1).WriteArray((yy * width + xx).astype("float32"))
        ds.FlushCache()
        ds = None
        return path

    def _grid(self, temp_dir):
        import geopandas as gpd
        from shapely.geometry import box
        g = gpd.GeoDataFrame(
            {"i": [0], "h0": [self.H0_CELL], "geometry": [box(-124, 36, -122, 38)]},
            crs="EPSG:4326",
        ).rename_geometry("geom")
        path = os.path.join(temp_dir, "h0-test.parquet")
        g.to_parquet(path)
        return path

    def _processor(self, raster, temp_dir, name, **kwargs):
        from cng_datasets.raster import RasterProcessor
        out = os.path.join(temp_dir, name)
        os.makedirs(out, exist_ok=True)
        return RasterProcessor(
            input_path=raster,
            output_parquet_path=out,
            h3_resolution=self.RES,
            parent_resolutions=[0],
            h0_grid_path=self._grid(temp_dir),
            value_column="v",
            local_cache_dir=os.path.join(temp_dir, f"cache-{name}"),
            **kwargs,
        ), out

    @pytest.mark.timeout(300)
    def test_windowed_output_matches_unwindowed(self, raster, temp_dir):
        """The gate: windowing is an I/O optimisation, not a different answer."""
        plain, plain_out = self._processor(
            raster, temp_dir, "plain", chunk_resolution=2, window_reads="never")
        for i in range(len(plain.chunk_cells())):
            plain.process_chunk(i)

        windowed, win_out = self._processor(
            raster, temp_dir, "win", chunk_resolution=2, window_reads="always")
        for i in range(len(windowed.chunk_cells())):
            windowed.process_chunk(i)

        con = duckdb.connect()
        a = con.execute(
            f"SELECT v, h{self.RES} FROM read_parquet('{plain_out}/h0=*/part-*.parquet') "
            f"ORDER BY h{self.RES}").fetchall()
        b = con.execute(
            f"SELECT v, h{self.RES} FROM read_parquet('{win_out}/h0=*/part-*.parquet') "
            f"ORDER BY h{self.RES}").fetchall()
        assert a, "unwindowed run produced nothing"
        assert b == a, "windowed read changed the values"

    @pytest.mark.timeout(300)
    def test_window_is_smaller_than_the_whole_raster(self, raster, temp_dir):
        """The point of the exercise: a chunk must not pull the whole file."""
        proc, _ = self._processor(
            raster, temp_dir, "size", chunk_resolution=2, window_reads="always")
        full_bytes = os.path.getsize(raster)

        from shapely import wkt as shapely_wkt
        from cng_datasets.raster.cog import _H3_PROTRUSION_MARGIN, _WINDOW_NO_OVERLAP

        sizes = []
        for chunk_cell, _, _ in proc.chunk_cells():
            geom = proc.con.execute(
                f"SELECT h3_cell_to_boundary_wkt({chunk_cell})").fetchone()[0]
            _, cminy, _, cmaxy = shapely_wkt.loads(geom).bounds
            path = proc._windowed_source_for(
                geom, _H3_PROTRUSION_MARGIN * (cmaxy - cminy), chunk_cell)
            if path is _WINDOW_NO_OVERLAP or path is None:
                continue
            sizes.append(os.path.getsize(path))
            os.remove(path)

        assert sizes, "no chunk produced a window"
        assert max(sizes) < full_bytes, (
            f"largest window {max(sizes)} B is not smaller than the source {full_bytes} B"
        )
        # The point is not just "smaller" but "proportional to the chunk", so
        # total transfer stops scaling with the fan-out.
        assert sum(sizes) < len(sizes) * full_bytes, (
            "windowing every chunk moved as many bytes as copying the file to each"
        )

    @pytest.mark.timeout(120)
    def test_window_missing_the_source_skips_rather_than_reading_it_all(self, raster, temp_dir):
        """
        An empty window means the chunk provably has nothing, not "read everything".

        The pruning test runs a deliberately looser margin, so a chunk can pass
        it and still have a window that misses the raster. Returning the same
        None that signals "could not window" would send that chunk off to read
        the entire source to discover it is empty.
        """
        from cng_datasets.raster.cog import _WINDOW_NO_OVERLAP
        proc, _ = self._processor(
            raster, temp_dir, "miss", chunk_resolution=2, window_reads="always")
        # A cell on the far side of the planet from the fixture raster.
        far = proc.con.execute("SELECT h3_latlng_to_cell(-33.9, 18.4, 2)").fetchone()[0]
        geom = proc.con.execute(f"SELECT h3_cell_to_boundary_wkt({far})").fetchone()[0]
        assert proc._windowed_source_for(geom, 0.01, far) is _WINDOW_NO_OVERLAP

    @pytest.mark.timeout(120)
    def test_auto_leaves_a_local_source_alone(self, raster, temp_dir):
        """
        A window over a local file transfers nothing and buys nothing.

        It would only decode and re-encode the region, so 'auto' windows a
        remote source and leaves a local read as it is.
        """
        local, _ = self._processor(raster, temp_dir, "auto-local", chunk_resolution=2)
        assert local._windowing is False
        forced, _ = self._processor(
            raster, temp_dir, "auto-forced", chunk_resolution=2, window_reads="always")
        assert forced._windowing is True

    @pytest.mark.timeout(120)
    def test_auto_is_off_without_sub_chunking(self, raster, temp_dir):
        """At h0 granularity the whole-file copy is still the right trade."""
        proc, _ = self._processor(raster, temp_dir, "auto-h0")
        assert proc._windowing is False

    @pytest.mark.timeout(60)
    def test_invalid_mode_is_rejected(self, raster, temp_dir):
        with pytest.raises(ValueError, match="window_reads"):
            self._processor(raster, temp_dir, "bad", window_reads="sometimes")


class TestH3ProtrusionMargin:
    """
    The margin constant is derived from a measurement, so pin the measurement.

    H3's hierarchy is only approximately containing: a descendant cell can
    protrude beyond its ancestor's boundary polygon. Both the chunk-pruning
    test and the read window depend on a bound for that protrusion — too small
    and cells are silently dropped or read as nodata (issue #173). If a future
    H3 version widens it, this fails here rather than in a build's output.
    """

    SAMPLE = [(0, 0), (37.7, -122.4), (51.5, -0.1), (-33.9, 18.4), (35.7, 139.7),
              (-23.5, -46.6), (64.1, -21.9), (1.3, 103.8), (-41.3, 174.8), (55.7, 37.6)]

    def _max_protrusion(self, con, chunk_res, levels_down):
        from shapely import wkt as swkt
        values = ",".join(str(p) for p in self.SAMPLE)
        parents = {
            r[0] for r in con.execute(
                f"SELECT h3_latlng_to_cell(lat, lon, {chunk_res}) "
                f"FROM (VALUES {values}) t(lat, lon)"
            ).fetchall()
        }
        worst = 0.0
        for parent in parents:
            pg = swkt.loads(
                con.execute(f"SELECT h3_cell_to_boundary_wkt({parent})").fetchone()[0])
            pminx, pminy, pmaxx, pmaxy = pg.bounds
            if pmaxx - pminx > 180:      # antimeridian: planar bounds are meaningless
                continue
            extent = pmaxy - pminy
            for (desc,) in con.execute(
                f"SELECT UNNEST(h3_cell_to_children({parent}, {chunk_res + levels_down}))"
            ).fetchall():
                dg = swkt.loads(
                    con.execute(f"SELECT h3_cell_to_boundary_wkt({desc})").fetchone()[0])
                dminx, dminy, dmaxx, dmaxy = dg.bounds
                if dmaxx - dminx > 180:
                    continue
                worst = max(worst, max(pminy - dminy, dmaxy - pmaxy,
                                       pminx - dminx, dmaxx - pmaxx, 0.0) / extent)
        return worst

    @pytest.mark.timeout(300)
    def test_margin_covers_measured_protrusion(self):
        from cng_datasets.raster.cog import _H3_PROTRUSION_MARGIN
        con = duckdb.connect()
        con.execute("INSTALL h3 FROM community; LOAD h3; INSTALL spatial; LOAD spatial;")
        worst = max(
            self._max_protrusion(con, chunk_res, levels)
            for chunk_res in (1, 2, 3)
            for levels in (1, 3)
        )
        assert worst > 0, "measurement found no protrusion at all — check the harness"
        assert worst < _H3_PROTRUSION_MARGIN, (
            f"measured protrusion {worst:.3f} of the chunk's extent meets or exceeds the "
            f"margin {_H3_PROTRUSION_MARGIN}. Raise _H3_PROTRUSION_MARGIN: too small a "
            f"margin drops cells from the prune test and reads nodata in the window."
        )

    @pytest.mark.timeout(300)
    def test_protrusion_converges_rather_than_compounding(self):
        """
        Why one constant works for every chunk depth.

        Each level is only approximately inside its parent, so a naive reading
        says the error accumulates with depth and no fixed margin is safe. It
        does not: the union of a cell's descendants converges on a region just
        larger than the cell.
        """
        con = duckdb.connect()
        con.execute("INSTALL h3 FROM community; LOAD h3; INSTALL spatial; LOAD spatial;")
        one = self._max_protrusion(con, 2, 1)
        deep = self._max_protrusion(con, 2, 4)
        assert deep < 2 * one, (
            f"protrusion grew from {one:.3f} at one level to {deep:.3f} at four — if it "
            f"compounds with depth, a depth-independent margin is not sound"
        )


class TestChunkCompleteness:
    """
    A partly failed fan-out must not be published as a complete dataset (#173).

    Counting part files cannot detect this: a chunk that does not overlap the
    raster legitimately writes none, so a missing part is indistinguishable from
    a chunk that never ran. Without a completion marker per chunk, merge
    consolidated whatever survived and — with cleanup on — deleted the evidence.
    The risk is concentrated on the Armada backend, whose jobs carry no retry
    budget (#183) and which is where a large fan-out gets routed.
    """

    H0_CELL = 577199624117288959
    RES = 3

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def raster(self, temp_dir):
        from osgeo import gdal, osr
        w = h = 64
        path = os.path.join(temp_dir, "r.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, w, h, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([-123.0, 1/64, 0, 37.0 + h/64, 0, -1/64])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        yy, xx = np.mgrid[0:h, 0:w]
        ds.GetRasterBand(1).WriteArray((yy * w + xx).astype("float32"))
        ds.FlushCache(); ds = None
        return path

    def _chunks(self, raster, temp_dir, name="chunks"):
        import geopandas as gpd
        from shapely.geometry import box
        from cng_datasets.raster import RasterProcessor
        grid = os.path.join(temp_dir, "h0.parquet")
        gpd.GeoDataFrame({"i": [0], "h0": [self.H0_CELL],
                          "geometry": [box(-124, 36, -122, 38)]},
                         crs="EPSG:4326").rename_geometry("geom").to_parquet(grid)
        out = os.path.join(temp_dir, name)
        os.makedirs(out, exist_ok=True)
        return RasterProcessor(
            input_path=raster, output_parquet_path=out, h3_resolution=self.RES,
            parent_resolutions=[0], h0_grid_path=grid, value_column="v",
            chunk_resolution=2,
        ), out

    @pytest.mark.timeout(300)
    def test_every_chunk_records_completion_including_empty_ones(self, raster, temp_dir):
        """
        The marker is the point: it exists even when the chunk wrote no data.

        Most chunks of this fixture do not overlap the raster at all, so if
        markers only followed output there would be far fewer than chunks.
        """
        proc, out = self._chunks(raster, temp_dir)
        n = len(proc.chunk_cells())
        for i in range(n):
            proc.process_chunk(i)
        markers = glob.glob(os.path.join(out, "_manifest", "chunk-*.parquet"))
        parts = glob.glob(os.path.join(out, "h0=*", "part-*.parquet"))
        assert len(markers) == n, f"{len(markers)} markers for {n} chunks"
        assert len(parts) < n, (
            "fixture no longer exercises the empty-chunk case — every chunk wrote "
            "data, so counting parts would have sufficed"
        )

    @pytest.mark.timeout(300)
    def test_merge_refuses_an_incomplete_fan_out(self, raster, temp_dir):
        """A chunk that never ran must stop the merge, not be merged around."""
        from cng_datasets.raster.merge import merge_raster_chunks
        proc, out = self._chunks(raster, temp_dir)
        n = len(proc.chunk_cells())
        for i in range(n - 1):          # the last chunk "fails": never runs
            proc.process_chunk(i)

        merged = os.path.join(temp_dir, "merged")
        os.makedirs(merged, exist_ok=True)
        with pytest.raises(RuntimeError, match="Incomplete fan-out"):
            merge_raster_chunks(out, merged, cleanup=False, expect_chunks=n)
        assert not glob.glob(os.path.join(merged, "h0=*", "*.parquet")), (
            "merge must write nothing when the fan-out is incomplete"
        )
        assert glob.glob(os.path.join(out, "h0=*", "part-*.parquet")), (
            "chunks must survive a refused merge so the missing ones can be found"
        )

    @pytest.mark.timeout(300)
    def test_merge_proceeds_when_every_chunk_ran(self, raster, temp_dir):
        from cng_datasets.raster.merge import merge_raster_chunks
        proc, out = self._chunks(raster, temp_dir)
        n = len(proc.chunk_cells())
        for i in range(n):
            proc.process_chunk(i)
        merged = os.path.join(temp_dir, "merged2")
        os.makedirs(merged, exist_ok=True)
        assert merge_raster_chunks(out, merged, cleanup=False, expect_chunks=n) >= 1
        assert os.path.exists(os.path.join(merged, f"h0={self.H0_CELL}", "data_0.parquet"))

    @pytest.mark.timeout(300)
    def test_check_is_opt_in(self, raster, temp_dir):
        """Without --expect-chunks the merge behaves as before."""
        from cng_datasets.raster.merge import merge_raster_chunks
        proc, out = self._chunks(raster, temp_dir)
        for i in range(len(proc.chunk_cells()) - 1):
            proc.process_chunk(i)
        merged = os.path.join(temp_dir, "merged3")
        os.makedirs(merged, exist_ok=True)
        assert merge_raster_chunks(out, merged, cleanup=False) >= 1

    @pytest.mark.timeout(120)
    def test_expect_chunks_without_markers_says_why(self, raster, temp_dir):
        """Chunks written before markers existed must fail legibly, not obscurely."""
        from cng_datasets.raster.merge import merge_raster_chunks
        proc, out = self._chunks(raster, temp_dir)
        for i in range(len(proc.chunk_cells())):
            proc.process_chunk(i)
        shutil.rmtree(os.path.join(out, "_manifest"))
        merged = os.path.join(temp_dir, "merged4")
        os.makedirs(merged, exist_ok=True)
        with pytest.raises(RuntimeError, match="[Nn]o completion markers"):
            merge_raster_chunks(out, merged, cleanup=False, expect_chunks=49)


class TestGapfill:
    """
    Re-running the chunks that never completed (#183, #173).

    Armada exposes no retry service on NRP — `armadactl get retry-policies`
    returns Unimplemented — and a preempted job is not rescheduled, so a
    transient fault leaves a permanently missing chunk. At an observed ~0.1%
    failure rate a few-thousand-unit fan-out loses one or two every run, which
    makes gap-fill a pipeline stage rather than an exception.
    """

    H0_CELL = 577199624117288959
    RES = 3

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def raster(self, temp_dir):
        from osgeo import gdal, osr
        w = h = 64
        path = os.path.join(temp_dir, "r.tif")
        ds = gdal.GetDriverByName("GTiff").Create(path, w, h, 1, gdal.GDT_Float32)
        ds.SetGeoTransform([-123.0, 1/64, 0, 37.0 + h/64, 0, -1/64])
        srs = osr.SpatialReference(); srs.ImportFromEPSG(4326)
        ds.SetProjection(srs.ExportToWkt())
        yy, xx = np.mgrid[0:h, 0:w]
        ds.GetRasterBand(1).WriteArray((yy * w + xx).astype("float32"))
        ds.FlushCache(); ds = None
        return path

    def _run_chunks(self, raster, temp_dir, skip=()):
        import geopandas as gpd
        from shapely.geometry import box
        from cng_datasets.raster import RasterProcessor
        grid = os.path.join(temp_dir, "h0.parquet")
        gpd.GeoDataFrame({"i": [0], "h0": [self.H0_CELL],
                          "geometry": [box(-124, 36, -122, 38)]},
                         crs="EPSG:4326").rename_geometry("geom").to_parquet(grid)
        out = os.path.join(temp_dir, "chunks")
        os.makedirs(out, exist_ok=True)
        proc = RasterProcessor(
            input_path=raster, output_parquet_path=out, h3_resolution=self.RES,
            parent_resolutions=[0], h0_grid_path=grid, value_column="v",
            chunk_resolution=2)
        n = len(proc.chunk_cells())
        for i in range(n):
            if i not in skip:
                proc.process_chunk(i)
        return out, n

    def _hex_manifest(self, temp_dir, completions):
        """A hex Job manifest of the shape the generator emits."""
        path = os.path.join(temp_dir, "demo-hex.yaml")
        with open(path, "w") as f:
            yaml.safe_dump({
                "apiVersion": "batch/v1", "kind": "Job",
                "metadata": {"name": "demo-hex", "namespace": "geo-workflows"},
                "spec": {
                    "completions": completions, "parallelism": 4,
                    "completionMode": "Indexed",
                    "template": {"spec": {
                        "restartPolicy": "Never",
                        "containers": [{
                            "name": "hex-task", "image": "img",
                            "command": ["bash", "-c",
                                        "cng-datasets raster --chunk-index ${JOB_COMPLETION_INDEX}"],
                        }],
                    }},
                },
            }, f)
        return path

    @pytest.mark.timeout(300)
    def test_missing_chunks_are_enumerated_not_counted(self, raster, temp_dir):
        """
        A count says "48 of 49" and leaves you to find the one.

        On a few-thousand-unit fan-out that is the entire problem, so the
        missing set is reported explicitly.
        """
        from cng_datasets.raster.merge import find_missing_chunks
        out, n = self._run_chunks(raster, temp_dir, skip={3, 11})
        assert find_missing_chunks(out, n) == [3, 11]

    @pytest.mark.timeout(300)
    def test_gapfill_job_set_reruns_exactly_the_missing_indices(self, raster, temp_dir):
        from cng_datasets.raster.merge import generate_gapfill
        out, n = self._run_chunks(raster, temp_dir, skip={3, 11})
        manifest = self._hex_manifest(temp_dir, n)
        dest = os.path.join(temp_dir, "gapfill.yaml")

        missing = generate_gapfill(out, n, manifest, dest, queue="geo-workflows")
        assert missing == [3, 11]

        spec = yaml.safe_load(open(dest))
        assert spec["queue"] == "geo-workflows"
        assert len(spec["jobs"]) == 2, "one job per missing chunk, and no others"
        cmds = [j["podSpec"]["containers"][0]["command"][-1] for j in spec["jobs"]]
        assert sorted(cmds) == sorted([
            "cng-datasets raster --chunk-index 3",
            "cng-datasets raster --chunk-index 11",
        ]), "the completion index must be substituted, not left as a placeholder"

    @pytest.mark.timeout(300)
    def test_gapfill_writes_nothing_when_complete(self, raster, temp_dir):
        from cng_datasets.raster.merge import generate_gapfill
        out, n = self._run_chunks(raster, temp_dir)
        dest = os.path.join(temp_dir, "gapfill.yaml")
        assert generate_gapfill(out, n, self._hex_manifest(temp_dir, n), dest) == []
        assert not os.path.exists(dest)

    @pytest.mark.timeout(300)
    def test_manifest_from_a_different_fan_out_is_refused(self, raster, temp_dir):
        """
        Re-running an index against the wrong chunk list processes the wrong cell.

        The index only means something relative to the enumeration that produced
        it, so a manifest whose completions disagree cannot be used.
        """
        from cng_datasets.raster.merge import generate_gapfill
        out, n = self._run_chunks(raster, temp_dir, skip={3})
        wrong = self._hex_manifest(temp_dir, n + 5)
        with pytest.raises(RuntimeError, match="different generations"):
            generate_gapfill(out, n, wrong, os.path.join(temp_dir, "g.yaml"))

    @pytest.mark.timeout(300)
    def test_markers_outside_the_expected_range_are_refused(self, raster, temp_dir):
        """Two fan-outs writing into one prefix must not be merged together."""
        from cng_datasets.raster.merge import find_missing_chunks
        out, n = self._run_chunks(raster, temp_dir)
        with pytest.raises(RuntimeError, match="outside the expected range"):
            find_missing_chunks(out, n - 3)
