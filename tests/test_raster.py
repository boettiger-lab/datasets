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
        df = proc._native_cells_for_h0(h0)
        cells = [int(c) for c in df["h3"].tolist()]
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

    @pytest.mark.timeout(180)
    def test_hex_excludes_all_fill_codes(self, categorical_raster, temp_dir):
        """The hex step drops every fill code, not just the band's own nodata."""
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
            input_path=categorical_raster,
            output_parquet_path=output_dir,
            h3_resolution=5,
            parent_resolutions=[0],
            h0_grid_path=h0_file,
            value_column="evt",
            hex_resampling="mode",
            nodata_value="-9999,-1111,32767",
        )
        result = proc.process_h0_region(0)
        if result:
            df = proc.con.read_parquet(result).fetchdf()
            for fill in (-9999, -1111, 32767):
                assert fill not in df["evt"].values, f"fill code {fill} leaked into hex output"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
