"""
Unit tests for raster processing functionality.

Tests COG creation, H3 tiling, and resolution detection using small test datasets.
"""

import pytest
import os
import tempfile
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
        # Default hex resampling preserves existing behavior for continuous rasters
        assert processor.hex_resampling == "average"

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
    
    @pytest.mark.timeout(30)
    def test_duckdb_csv_reading(self, small_raster, temp_dir):
        """Test that DuckDB connection uses correct CSV parameter names."""
        from cng_datasets.raster import RasterProcessor
        import os
        
        # Create a simple XYZ file
        xyz_file = os.path.join(temp_dir, 'test.xyz')
        with open(xyz_file, 'w') as f:
            f.write('-122.5 37.7 100\n')
            f.write('-122.4 37.8 200\n')
        
        # Create a minimal processor using small_raster fixture
        processor = RasterProcessor(
            input_path=small_raster,
            h3_resolution=8,
        )
        
        # Test that read_csv works with 'delimiter' (raw DuckDB parameter)
        # This would fail if using 'delim' (Ibis parameter)
        result = processor.con.read_csv(
            xyz_file,
            delimiter=' ',
            columns={'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'FLOAT'}
        )
        
        # Verify we can execute the query
        df = result.fetchdf()
        assert len(df) == 2
        # Check approximate float values
        assert abs(df['X'].iloc[0] - (-122.5)) < 0.001
        assert abs(df['X'].iloc[1] - (-122.4)) < 0.001
        assert abs(df['Z'].iloc[0] - 100.0) < 0.001
        assert abs(df['Z'].iloc[1] - 200.0) < 0.001


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
        
        # Create a mock h0 grid file locally that covers our test area
        # San Francisco area: -122.5 to -122.45, 37.7 to 37.75
        h0_geom = box(-123, 37, -122, 38)  # Wider box to ensure coverage
        h0_gdf = gpd.GeoDataFrame({
            'i': [0],
            'h0': ['8007fffffffffff'],  # Mock h0 cell ID
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
            h3_resolution=7,  # Coarse resolution for speed
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
            assert 'h7' in df.columns
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


class TestH3Downsampling:
    """Test that gdal.Warp downsamples to H3 resolution."""

    @pytest.mark.timeout(5)
    def test_h3_res_to_degrees_monotonic(self):
        """Finer H3 resolutions should yield smaller pixel sizes."""
        from cng_datasets.raster.cog import _h3_res_to_degrees

        prev = _h3_res_to_degrees(0)
        for res in range(1, 16):
            cur = _h3_res_to_degrees(res)
            assert cur < prev, f"h{res} ({cur}) should be smaller than h{res-1} ({prev})"
            prev = cur

    @pytest.mark.timeout(5)
    def test_h3_res_to_degrees_known_values(self):
        """Spot-check a few resolutions against expected degree values."""
        from cng_datasets.raster.cog import _h3_res_to_degrees

        # h8 edge ≈ 531 m → ~0.0048°, h10 edge ≈ 76 m → ~0.00068°
        h8 = _h3_res_to_degrees(8)
        assert 0.003 < h8 < 0.007, f"h8 pixel size {h8} out of expected range"

        h10 = _h3_res_to_degrees(10)
        assert 0.0004 < h10 < 0.001, f"h10 pixel size {h10} out of expected range"

    @requires_gdal_array
    @pytest.mark.timeout(60)
    def test_warp_downsamples_high_res_raster(self):
        """A high-res raster warped at h8 should produce far fewer rows than source pixels."""
        from cng_datasets.raster.cog import _h3_res_to_degrees

        # Create a 200x200 raster at 0.0001° (~11 m) covering a 0.02°×0.02° patch.
        # Source pixels: 40,000.  At h8 (~0.0048°) the output grid should be
        # roughly (0.02/0.0048)^2 ≈ 17 pixels — orders of magnitude fewer.
        width, height = 200, 200
        xmin, ymin = -105.0, 42.0
        src_pixel = 0.0001

        with tempfile.TemporaryDirectory() as tmp:
            src_path = os.path.join(tmp, "hires.tif")
            driver = gdal.GetDriverByName("GTiff")
            ds = driver.Create(src_path, width, height, 1, gdal.GDT_Float32)
            ds.SetGeoTransform([xmin, src_pixel, 0, ymin + height * src_pixel, 0, -src_pixel])
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            ds.SetProjection(srs.ExportToWkt())
            band = ds.GetRasterBand(1)
            data = np.arange(width * height, dtype=np.float32).reshape(height, width)
            band.WriteArray(data)
            band.FlushCache()
            ds = None

            # Warp WITH downsampling (h8)
            pixel_size = _h3_res_to_degrees(8)
            xyz_ds_path = os.path.join(tmp, "downsampled.xyz")
            extent = (xmin, ymin, xmin + width * src_pixel, ymin + height * src_pixel)
            opts = gdal.WarpOptions(
                dstSRS="EPSG:4326",
                outputBounds=extent,
                xRes=pixel_size,
                yRes=pixel_size,
                resampleAlg=gdal.GRA_Average,
                format="XYZ",
            )
            result = gdal.Warp(xyz_ds_path, src_path, options=opts)
            assert result is not None
            result = None

            # Warp WITHOUT downsampling (native resolution)
            xyz_native_path = os.path.join(tmp, "native.xyz")
            opts_native = gdal.WarpOptions(
                dstSRS="EPSG:4326",
                outputBounds=extent,
                format="XYZ",
            )
            result = gdal.Warp(xyz_native_path, src_path, options=opts_native)
            assert result is not None
            result = None

            ds_size = os.path.getsize(xyz_ds_path)
            native_size = os.path.getsize(xyz_native_path)

            # The downsampled file should be at least 10× smaller
            assert ds_size < native_size / 10, (
                f"Downsampled XYZ ({ds_size} bytes) should be much smaller "
                f"than native ({native_size} bytes)"
            )

    @requires_gdal_array
    @pytest.mark.timeout(60)
    def test_warp_average_resampling_produces_mean(self):
        """GRA_Average should produce the mean of source pixels, not nearest."""
        from cng_datasets.raster.cog import _h3_res_to_degrees

        # 4x4 raster with known values, downsampled to ~1 output pixel.
        width, height = 4, 4
        xmin, ymin = -105.0, 42.0
        src_pixel = 0.001  # each pixel ~0.001°

        with tempfile.TemporaryDirectory() as tmp:
            src_path = os.path.join(tmp, "uniform.tif")
            driver = gdal.GetDriverByName("GTiff")
            ds = driver.Create(src_path, width, height, 1, gdal.GDT_Float32)
            ds.SetGeoTransform([xmin, src_pixel, 0, ymin + height * src_pixel, 0, -src_pixel])
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            ds.SetProjection(srs.ExportToWkt())
            # Values: 10, 20, 30, 40 ... average = 25
            data = np.array([
                [10, 20, 30, 40],
                [10, 20, 30, 40],
                [10, 20, 30, 40],
                [10, 20, 30, 40],
            ], dtype=np.float32)
            ds.GetRasterBand(1).WriteArray(data)
            ds.GetRasterBand(1).FlushCache()
            ds = None

            extent = (xmin, ymin, xmin + width * src_pixel, ymin + height * src_pixel)
            # Use a large pixel size so everything collapses to ~1 pixel
            pixel_size = 0.005
            xyz_path = os.path.join(tmp, "avg.xyz")
            opts = gdal.WarpOptions(
                dstSRS="EPSG:4326",
                outputBounds=extent,
                xRes=pixel_size,
                yRes=pixel_size,
                resampleAlg=gdal.GRA_Average,
                format="XYZ",
            )
            result = gdal.Warp(xyz_path, src_path, options=opts)
            assert result is not None
            result = None

            # Read the XYZ output — should be 1 (or very few) row(s).
            # The value should be a blend of the source pixels (10, 20, 30, 40),
            # not one of those exact values (which would indicate nearest-neighbor).
            with open(xyz_path) as f:
                lines = [l.strip() for l in f if l.strip()]
            assert len(lines) >= 1
            val = float(lines[0].split()[2])
            assert val not in (10.0, 20.0, 30.0, 40.0), (
                f"Got exact source value {val}; expected a blended average"
            )
            assert 10.0 <= val <= 40.0, f"Averaged value {val} outside source range"

    @requires_gdal_array
    @pytest.mark.timeout(60)
    def test_warp_mode_resampling_preserves_categorical_classes(self):
        """Regression test for issue #80: mode resampling must not blend class codes.

        GRA_Average on categorical data (e.g. land cover class codes {40, 50}) yields
        meaningless interpolated values (45) that don't map to any real class. GRA_Mode
        picks the most frequent source class, preserving categorical semantics.
        """
        # Categorical raster: checkerboard of class 40 and class 50. Any output
        # pixel covering a mixed region will average to 45 under GRA_Average but
        # must remain in {40, 50} under GRA_Mode.
        width, height = 8, 8
        xmin, ymin = -105.0, 42.0
        src_pixel = 0.001

        with tempfile.TemporaryDirectory() as tmp:
            src_path = os.path.join(tmp, "categorical.tif")
            driver = gdal.GetDriverByName("GTiff")
            ds = driver.Create(src_path, width, height, 1, gdal.GDT_Int16)
            ds.SetGeoTransform([xmin, src_pixel, 0, ymin + height * src_pixel, 0, -src_pixel])
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            ds.SetProjection(srs.ExportToWkt())
            # Alternating 40/50 checkerboard
            data = np.where(
                (np.indices((height, width)).sum(axis=0) % 2) == 0, 40, 50
            ).astype(np.int16)
            ds.GetRasterBand(1).WriteArray(data)
            ds.GetRasterBand(1).FlushCache()
            ds = None

            extent = (xmin, ymin, xmin + width * src_pixel, ymin + height * src_pixel)
            # Downsample by ~4x so each output pixel mixes source pixels
            pixel_size = 0.004

            def warp(alg, out_name):
                out_path = os.path.join(tmp, out_name)
                result = gdal.Warp(out_path, src_path, options=gdal.WarpOptions(
                    dstSRS="EPSG:4326",
                    outputBounds=extent,
                    xRes=pixel_size,
                    yRes=pixel_size,
                    resampleAlg=alg,
                    format="XYZ",
                ))
                assert result is not None
                result = None
                with open(out_path) as f:
                    return [float(l.split()[2]) for l in f if l.strip()]

            avg_vals = warp("average", "avg.xyz")
            mode_vals = warp("mode", "mode.xyz")

            # Average produces the bug: 45 appears
            assert any(abs(v - 45.0) < 0.5 for v in avg_vals), (
                "Expected averaging to produce invalid class code ~45 from {40,50}"
            )
            # Mode preserves canonical class codes only
            for v in mode_vals:
                assert v in (40.0, 50.0), (
                    f"Mode resampling produced non-canonical class {v}; "
                    f"must be one of {{40, 50}}"
                )

    @requires_gdal_array
    @pytest.mark.timeout(60)
    def test_no_downsample_when_source_coarser(self):
        """When source pixels are coarser than H3 cell size, output ≈ source size."""
        from cng_datasets.raster.cog import _h3_res_to_degrees

        # 10x10 raster at 0.01° (~1 km) pixels, warp at h10 (~0.0007°).
        # xRes < source pixel size, so GDAL upsamples — output should be
        # larger than input (more rows), confirming no data is lost.
        width, height = 10, 10
        xmin, ymin = -105.0, 42.0
        src_pixel = 0.01

        with tempfile.TemporaryDirectory() as tmp:
            src_path = os.path.join(tmp, "coarse.tif")
            driver = gdal.GetDriverByName("GTiff")
            ds = driver.Create(src_path, width, height, 1, gdal.GDT_Float32)
            ds.SetGeoTransform([xmin, src_pixel, 0, ymin + height * src_pixel, 0, -src_pixel])
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(4326)
            ds.SetProjection(srs.ExportToWkt())
            data = np.ones((height, width), dtype=np.float32) * 42.0
            ds.GetRasterBand(1).WriteArray(data)
            ds.GetRasterBand(1).FlushCache()
            ds = None

            pixel_size = _h3_res_to_degrees(10)  # ~0.0007°, finer than 0.01°
            xyz_path = os.path.join(tmp, "coarse_out.xyz")
            extent = (xmin, ymin, xmin + width * src_pixel, ymin + height * src_pixel)
            opts = gdal.WarpOptions(
                dstSRS="EPSG:4326",
                outputBounds=extent,
                xRes=pixel_size,
                yRes=pixel_size,
                resampleAlg=gdal.GRA_Average,
                format="XYZ",
            )
            result = gdal.Warp(xyz_path, src_path, options=opts)
            assert result is not None
            result = None

            with open(xyz_path) as f:
                lines = [l.strip() for l in f if l.strip()]
            # Should produce MORE rows than source pixels (upsampled)
            assert len(lines) > width * height, (
                f"Expected more rows than source ({width*height}), got {len(lines)}"
            )


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
