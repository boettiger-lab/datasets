"""
Tests for N-D cube → H3 hex (`cng-datasets mdim`, issue #181).

Fixtures are small cubes written with GDAL's own multidimensional writer, as
zarr and as netCDF, so the reader under test sees what it sees in production.
Expected values are computed independently: brute-force pixel → cell in DuckDB
for `aggregate`, nearest-pixel lookup in numpy for `sample`.
"""

import os

import duckdb
import numpy as np
import pytest

gdal = pytest.importorskip("osgeo.gdal")

from cng_datasets.mdim.cftime import decode_cf_time, parse_units  # noqa: E402

H0 = 577199624117288959   # base cell 20: covers coastal California
LAT0, LON0 = 37.0, -122.5  # fixture cubes sit here


# -- fixtures -----------------------------------------------------------------

def make_cube(path, driver, lat, lon, time_vals, units="days since 2000-01-01",
              calendar="standard", data=None, block=None, nodata=None,
              order=("time", "lat", "lon"), typed_dims=True, scale=None, offset=None):
    """Write a (time, lat, lon) cube with GDAL's multidimensional API."""
    drv = gdal.GetDriverByName(driver)
    ds = drv.CreateMultiDimensional(path)
    rg = ds.GetRootGroup()
    dims = {}
    types = {"time": "TEMPORAL", "lat": "HORIZONTAL_Y", "lon": "HORIZONTAL_X"}
    for name, vals in (("time", time_vals), ("lat", lat), ("lon", lon)):
        d = rg.CreateDimension(name, types[name] if typed_dims else None, None, len(vals))
        v = rg.CreateMDArray(name, [d], gdal.ExtendedDataType.Create(gdal.GDT_Float64))
        # Zarr skips writing a chunk that equals its fill value, which would
        # read back a coordinate of 0 as missing; NaN cannot collide.
        v.SetNoDataValueDouble(float("nan"))
        v.Write(np.asarray(vals, dtype=np.float64))
        try:
            d.SetIndexingVariable(v)
        except RuntimeError:
            pass  # netCDF: a variable named after its dimension is its coordinate
        dims[name] = (d, v)
    tvar = dims["time"][1]
    tvar.SetUnit(units)
    tvar.CreateAttribute("calendar", [], gdal.ExtendedDataType.CreateString()).Write(calendar)
    perm = [("time", "lat", "lon").index(n) for n in order]
    for var, arr in (data or {}).items():
        opts = []
        if block:
            opts.append("BLOCKSIZE=" + ",".join(str(block[("time", "lat", "lon").index(n)]) for n in order))
        a = rg.CreateMDArray(var, [dims[n][0] for n in order],
                             gdal.ExtendedDataType.Create(gdal.GDT_Float32), opts)
        if nodata is not None:
            a.SetNoDataValueDouble(nodata)
        if scale is not None:
            a.SetScale(scale)
        if offset is not None:
            a.SetOffset(offset)
        a.Write(np.ascontiguousarray(np.transpose(arr, perm)).astype(np.float32))
    ds = None
    return path


def grid(n_lat, n_lon, step, lat0=LAT0, lon0=LON0):
    lat = lat0 + step * (np.arange(n_lat) - n_lat / 2 + 0.5)
    lon = lon0 + step * (np.arange(n_lon) - n_lon / 2 + 0.5)
    return lat, lon


def field(lat, lon, nt):
    """Distinct, smooth values: v = 10*lat + lon + 0.1*t."""
    t = np.arange(nt)[:, None, None]
    return 10.0 * lat[None, :, None] + lon[None, None, :] + 0.1 * t


@pytest.fixture
def h0_grid(tmp_path):
    path = str(tmp_path / "h0.parquet")
    duckdb.sql(f"COPY (SELECT 0 AS i, {H0}::UBIGINT AS h0) TO '{path}' (FORMAT PARQUET)")
    return path


def processor(cube, variables, out, h0_grid, **kwargs):
    from cng_datasets.mdim import MdimProcessor
    return MdimProcessor(inputs=cube if isinstance(cube, list) else [cube],
                         variables=variables, output_parquet_path=str(out),
                         h0_grid_path=h0_grid, **kwargs)


def rows(path, cols="*", order="1, 2"):
    return duckdb.sql(f"SELECT {cols} FROM read_parquet('{path}') ORDER BY {order}").fetchall()


def h3con():
    from cng_datasets.vector.h3_tiling import setup_duckdb_connection
    return setup_duckdb_connection()


# -- CF time ------------------------------------------------------------------

class TestCFTime:
    def test_gregorian_days_and_hours(self):
        t = decode_cf_time([0, 59, 365], "days since 2000-01-01", "standard")
        assert [str(d) for d in t.dates] == ["2000-01-01", "2000-02-29", "2000-12-31"]
        h = decode_cf_time([0, 36], "hours since 2000-01-01 00:00:00", "gregorian")
        assert [str(d) for d in h.dates] == ["2000-01-01", "2000-01-02"]

    def test_noon_reference_floors_to_the_day(self):
        """LOCA2: 'days since 2015-01-01 12:00:00', integer offsets."""
        t = decode_cf_time([0, 1], "days since 2015-01-01 12:00:00", "proleptic_gregorian")
        assert [str(d) for d in t.dates] == ["2015-01-01", "2015-01-02"]

    def test_noleap_skips_february_29(self):
        t = decode_cf_time([58, 59, 365], "days since 2000-01-01", "noleap")
        assert [str(d) for d in t.dates] == ["2000-02-28", "2000-03-01", "2001-01-01"]

    def test_360_day_has_february_30_and_no_dates(self):
        t = decode_cf_time([59, 360], "days since 2000-01-01", "360_day")
        assert (t.year.tolist(), t.month.tolist(), t.day.tolist()) == ([2000, 2001], [2, 1], [30, 1])
        assert t.dates is None

    def test_all_leap_has_no_dates(self):
        t = decode_cf_time([59], "days since 2001-01-01", "all_leap")
        assert (int(t.month[0]), int(t.day[0])) == (2, 29)   # 2001-02-29: no Gregorian DATE
        assert t.dates is None

    @pytest.mark.parametrize("units", ["months since 2000-01-01", "days after 2000-01-01", ""])
    def test_ambiguous_or_malformed_units_are_refused(self, units):
        with pytest.raises(ValueError):
            parse_units(units)

    def test_missing_time_values_are_refused(self):
        with pytest.raises(ValueError, match="missing value"):
            decode_cf_time([0, np.nan], "days since 2000-01-01", "standard")

    def test_julian_and_pre_1582_standard_are_refused(self):
        with pytest.raises(ValueError, match="unsupported CF calendar"):
            decode_cf_time([0], "days since 2000-01-01", "julian")
        with pytest.raises(ValueError, match="1582"):
            decode_cf_time([0], "days since 1500-01-01", "standard")


# -- reader -------------------------------------------------------------------

class TestReader:
    @pytest.mark.parametrize("driver, ext", [("Zarr", ".zarr"), ("netCDF", ".nc")])
    def test_storage_order_and_nodata(self, tmp_path, driver, ext):
        from cng_datasets.mdim import CubeSource
        lat, lon = grid(4, 5, 0.1)
        v = field(lat, lon, 3)
        v[1, 2, 3] = -999.0
        path = make_cube(str(tmp_path / f"c{ext}"), driver, lat, lon, [0, 1, 2],
                         data={"v": v}, nodata=-999.0, order=("lon", "time", "lat"))
        src = CubeSource([path], ["v"])
        got = src.read("v", 0, 3, 0, 4, 0, 5)
        assert got.shape == (3, 4, 5)
        assert np.isnan(got[1, 2, 3])
        v[1, 2, 3] = np.nan
        np.testing.assert_allclose(got, v, rtol=1e-6)

    def test_descending_lat_and_0_360_lon(self, tmp_path):
        from cng_datasets.mdim import CubeSource
        lat = np.array([38.0, 37.5, 37.0, 36.5])          # descending
        lon = np.array([237.0, 237.5, 238.0])             # 0-360
        path = make_cube(str(tmp_path / "d.zarr"), "Zarr", lat, lon, [0],
                         data={"v": np.zeros((1, 4, 3))})
        src = CubeSource([path], ["v"])
        iy, ix = src.nearest_pixel(np.array([37.1, 38.2, 30.0]), np.array([-122.4, -123.0, -122.0]))
        assert iy.tolist() == [2, 0, -1]
        assert ix.tolist() == [1, 0, 2]
        assert src.windows(36.9, 37.6, [(-123.1, -122.4)]) == [(1, 3, 0, 2)]

    def test_files_concatenate_along_time_and_must_share_the_grid(self, tmp_path):
        from cng_datasets.mdim import CubeSource
        lat, lon = grid(3, 3, 0.1)
        a = make_cube(str(tmp_path / "a.nc"), "netCDF", lat, lon, [0, 1], data={"v": field(lat, lon, 2)})
        b = make_cube(str(tmp_path / "b.nc"), "netCDF", lat, lon, [2, 3], data={"v": field(lat, lon, 2) + 1})
        src = CubeSource([a, b], ["v"])
        assert [str(d) for d in src.time.dates] == ["2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04"]
        assert [(s.start, s.size) for s in src.segments] == [(0, 2), (2, 2)]
        assert list(src.time_groups(0, 4, pixels=9, budget_bytes=1e9)) == [(0, 2), (2, 4)]
        bad = make_cube(str(tmp_path / "c.nc"), "netCDF", lat + 1, lon, [4], data={"v": field(lat, lon, 1)})
        with pytest.raises(ValueError, match="different lat/lon grid"):
            CubeSource([a, bad], ["v"])

    def test_scale_and_offset_are_applied(self, tmp_path):
        from cng_datasets.mdim import CubeSource
        lat, lon = grid(2, 2, 0.1)
        path = make_cube(str(tmp_path / "s.nc"), "netCDF", lat, lon, [0],
                         data={"v": np.full((1, 2, 2), 10.0)}, scale=0.5, offset=273.15)
        np.testing.assert_allclose(CubeSource([path], ["v"]).read("v", 0, 1, 0, 2, 0, 2), 278.15, rtol=1e-6)

    def test_untyped_dimensions_are_classified_by_name(self, tmp_path):
        from cng_datasets.mdim import CubeSource
        lat, lon = grid(2, 2, 0.1)
        path = make_cube(str(tmp_path / "u.zarr"), "Zarr", lat, lon, [0], data={"v": field(lat, lon, 1)},
                         typed_dims=False)
        assert CubeSource([path], ["v"]).axis == {"time": 0, "lat": 1, "lon": 2}


# -- processor ----------------------------------------------------------------

class TestAggregatePlacement:
    """Pixels finer than cells: every pixel centre lands in exactly one cell."""

    def _expected(self, lat, lon, v, res):
        con = h3con()
        glat, glon = np.meshgrid(lat, lon, indexing="ij")
        nt = v.shape[0]
        import pyarrow as pa
        con.register("px", pa.table({
            "lat": np.tile(glat.reshape(-1), nt), "lon": np.tile(glon.reshape(-1), nt),
            "t": np.repeat(np.arange(nt), glat.size), "v": v.reshape(-1)}))
        out = con.execute(f"""
            SELECT h3_latlng_to_cell(lat, lon, {res}) AS h, t, avg(v)
            FROM px GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()
        con.close()
        return out

    @pytest.mark.timeout(120)
    @pytest.mark.parametrize("driver, ext", [("Zarr", ".zarr"), ("netCDF", ".nc")])
    def test_matches_brute_force_pixel_assignment(self, tmp_path, h0_grid, driver, ext):
        lat, lon = grid(20, 24, 0.02)           # ~2 km pixels
        v = field(lat, lon, 4)
        cube = make_cube(str(tmp_path / f"c{ext}"), driver, lat, lon, [0, 1, 2, 3],
                         data={"v": v}, block=(2, 8, 8))
        proc = processor(cube, ["v"], tmp_path / "out", h0_grid, h3_resolution=6)
        assert proc.resolve_placement(LAT0) == "aggregate"
        path = proc.process_chunk(0)
        got = rows(path, "h6, time, v", order="h6, time")
        want = self._expected(lat, lon, v, 6)
        assert len(got) == len(want)
        for (h, d, x), (eh, et, ev) in zip(got, want):
            assert int(h) == int(eh)
            assert (np.datetime64(d, "D") - np.datetime64("2000-01-01", "D")).astype(int) == et
            assert x == pytest.approx(ev, rel=1e-6)

    @pytest.mark.timeout(120)
    def test_sub_chunks_partition_the_h0_without_gaps_or_duplicates(self, tmp_path, h0_grid):
        lat, lon = grid(40, 40, 0.05)
        cube = make_cube(str(tmp_path / "c.zarr"), "Zarr", lat, lon, [0], data={"v": field(lat, lon, 1)})
        whole = rows(processor(cube, ["v"], tmp_path / "h0", h0_grid, h3_resolution=6).process_chunk(0),
                     "h6, v", order="h6")
        sub = processor(cube, ["v"], tmp_path / "sub", h0_grid, h3_resolution=6, chunk_resolution=1)
        parts = []
        for i in range(len(sub.chunk_cells())):
            p = sub.process_chunk(i)
            if p:
                assert p.endswith(f"part-{sub.chunk_cells()[i][0]}.parquet")
                parts.extend(rows(p, "h6, v", order="h6"))
        assert sorted(parts) == whole
        manifest = duckdb.sql(
            f"SELECT count(*) FROM read_parquet('{tmp_path}/sub/_manifest/*.parquet')").fetchone()[0]
        assert manifest == len(sub.chunk_cells())


class TestSamplePlacement:
    """Pixels coarser than cells: each cell reads the pixel containing its centre."""

    @pytest.mark.timeout(120)
    def test_each_cell_reads_its_pixel(self, tmp_path, h0_grid):
        lat, lon = grid(4, 6, 0.25)             # NEX-GDDP-like 0.25 deg
        v = field(lat, lon, 2)
        cube = make_cube(str(tmp_path / "c.nc"), "netCDF", lat, lon, [0, 1], data={"tas": v})
        proc = processor(cube, ["tas"], tmp_path / "out", h0_grid, h3_resolution=6)
        assert proc.resolve_placement(LAT0) == "sample"
        got = rows(proc.process_chunk(0), "h6, time, tas", order="h6, time")
        con = h3con()
        centres = dict(((int(h), (la, lo)) for h, la, lo in con.execute(
            f"SELECT h, h3_cell_to_lat(h), h3_cell_to_lng(h) FROM "
            f"(SELECT DISTINCT h6 AS h FROM read_parquet('{tmp_path}/out/**/*.parquet'))").fetchall()))
        # Independently: the h0's res-6 cells whose centres fall on the grid
        # (pixel centres +/- half a pixel).
        expected_cells = con.execute(f"""
            SELECT count(*) FROM (SELECT UNNEST(h3_cell_to_children({H0}::UBIGINT, 6)) AS h)
            WHERE h3_cell_to_lat(h) BETWEEN {lat.min() - 0.125} AND {lat.max() + 0.125}
              AND h3_cell_to_lng(h) BETWEEN {lon.min() - 0.125} AND {lon.max() + 0.125}
        """).fetchone()[0]
        con.close()
        assert len(centres) == expected_cells
        assert got
        for h, d, x in got:
            la, lo = centres[int(h)]
            iy, ix = int(np.argmin(abs(lat - la))), int(np.argmin(abs(lon - lo)))
            t = int((np.datetime64(d, "D") - np.datetime64("2000-01-01", "D")).astype(int))
            assert x == pytest.approx(v[t, iy, ix], rel=1e-6)
        assert len(got) == 2 * expected_cells   # every cell, once per day


class TestTimeAndOptions:
    def _cube(self, tmp_path, calendar="noleap", nt=400):
        lat, lon = grid(2, 2, 0.25)
        v = np.broadcast_to(np.arange(nt, dtype=float)[:, None, None], (nt, 2, 2)).copy()
        return make_cube(str(tmp_path / "t.nc"), "netCDF", lat, lon, np.arange(nt),
                         calendar=calendar, data={"v": v})

    @pytest.mark.timeout(120)
    @pytest.mark.parametrize("agg, reducer, first", [
        ("year", "mean", (2000, 182.0)),           # days 0..364 of a noleap year
        ("year", "max", (2000, 364.0)),
        ("month", "min", (2000, 1, 0.0)),
    ])
    def test_time_aggregation(self, tmp_path, h0_grid, agg, reducer, first):
        proc = processor(self._cube(tmp_path), ["v"], tmp_path / "o", h0_grid, h3_resolution=6,
                         time_agg=agg, hex_resampling=reducer)
        cols = "year, v" if agg == "year" else "year, month, v"
        out = duckdb.sql(f"SELECT DISTINCT {cols} FROM read_parquet('{proc.process_chunk(0)}') "
                         f"ORDER BY ALL").fetchall()
        assert out[0] == pytest.approx(first)
        if agg == "month":
            assert out[1] == (2000, 2, 31.0)          # noleap: Feb starts at day 31

    @pytest.mark.timeout(120)
    def test_time_window(self, tmp_path, h0_grid):
        proc = processor(self._cube(tmp_path, calendar="standard"), ["v"], tmp_path / "o", h0_grid,
                         h3_resolution=6, time_start="2000-02-01", time_end="2000-02-03")
        times = duckdb.sql(f"SELECT DISTINCT time::VARCHAR FROM read_parquet('{proc.process_chunk(0)}') "
                           f"ORDER BY 1").fetchall()
        assert [t[0] for t in times] == ["2000-02-01", "2000-02-02", "2000-02-03"]

    def test_refusals(self, tmp_path, h0_grid):
        cube = self._cube(tmp_path, calendar="360_day", nt=3)
        with pytest.raises(ValueError, match="area-weighted"):
            processor(cube, ["v"], tmp_path / "o", h0_grid, h3_resolution=6, hex_resampling="sum")
        with pytest.raises(ValueError, match="--time-agg month or year"):
            processor(cube, ["v"], tmp_path / "o", h0_grid, h3_resolution=6)
        processor(cube, ["v"], tmp_path / "o", h0_grid, h3_resolution=6, time_agg="month")
        with pytest.raises(ValueError, match="not in"):
            processor(cube, ["nope"], tmp_path / "o", h0_grid, h3_resolution=6, time_agg="month")

    @pytest.mark.timeout(60)
    def test_chunk_off_the_source_writes_nothing_but_is_recorded(self, tmp_path):
        """A far-away h0 has no data; at chunk res > 0 its completion still counts."""
        far = str(tmp_path / "far.parquet")
        con = h3con()
        cell = con.execute("SELECT h3_cell_to_parent(h3_latlng_to_cell(-33.9, 18.4, 5), 0)").fetchone()[0]
        con.close()
        duckdb.sql(f"COPY (SELECT 0 AS i, {cell}::UBIGINT AS h0) TO '{far}' (FORMAT PARQUET)")
        proc = processor(self._cube(tmp_path, nt=2, calendar="standard"), ["v"], tmp_path / "o",
                         far, h3_resolution=3, chunk_resolution=1)
        assert all(proc.process_chunk(i) is None for i in range(len(proc.chunk_cells())))
        assert not any(p.startswith("h0=") for p in os.listdir(tmp_path / "o"))
        wrote = duckdb.sql(f"SELECT bool_or(wrote_data), count(*) FROM "
                           f"read_parquet('{tmp_path}/o/_manifest/*.parquet')").fetchone()
        assert wrote == (False, 7)
