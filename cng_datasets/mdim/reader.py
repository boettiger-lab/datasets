"""
GDAL multidimensional reader for (time, lat, lon) cubes (issue #181, reader B).

Reads zarr, netCDF, HDF5 — anything GDAL's multidimensional API opens —
through chunk-aligned slab reads, so a query touches only the chunks it
overlaps. Measured on the LOCA2 zarr store, a 1-day 1-degree read transferred
one compressed chunk (72 MiB); the DuckDB zarr extension v0.1.1 scanned the
store instead (#181). Everything downstream of the read — H3 indexing,
reduction, the parquet write — happens in DuckDB.

Several inputs are one time series split across files (NEX-GDDP ships a file
per year): they must share the spatial grid, and are concatenated along time in
the order given.
"""

from typing import Dict, Iterator, List, NamedTuple, Optional, Sequence, Tuple

import numpy as np
from osgeo import gdal

from .cftime import DecodedTime, decode_cf_time

gdal.UseExceptions()

_LAT_NAMES = ("lat", "latitude", "y", "nav_lat")
_LON_NAMES = ("lon", "longitude", "x", "nav_lon")
_TIME_NAMES = ("time", "t")


class Segment(NamedTuple):
    """One input file's share of the time axis."""
    path: str
    start: int     # first global time index
    size: int


def _attr(obj, name):
    try:
        at = obj.GetAttribute(name)
    except Exception:
        return None
    if at is None:
        return None
    try:
        return at.Read()
    except Exception:
        return None


def _classify(dim) -> Optional[str]:
    """'time', 'lat' or 'lon' for a dimension, from its GDAL type, CF
    attributes on its indexing variable, or its name."""
    dtype = (dim.GetType() or "").upper()
    if dtype == "TEMPORAL":
        return "time"
    if dtype == "HORIZONTAL_Y":
        return "lat"
    if dtype == "HORIZONTAL_X":
        return "lon"
    iv = dim.GetIndexingVariable()
    if iv is not None:
        std = str(_attr(iv, "standard_name") or "").lower()
        axis = str(_attr(iv, "axis") or "").upper()
        if std == "time" or axis == "T":
            return "time"
        if std == "latitude" or axis == "Y":
            return "lat"
        if std == "longitude" or axis == "X":
            return "lon"
    name = dim.GetName().lower()
    if name in _TIME_NAMES:
        return "time"
    if name in _LAT_NAMES:
        return "lat"
    if name in _LON_NAMES:
        return "lon"
    return None


def _open_array(path: str, variable: str):
    ds = gdal.OpenEx(path, gdal.OF_MULTIDIM_RASTER)
    if ds is None:
        raise ValueError(f"GDAL could not open {path} as a multidimensional dataset")
    rg = ds.GetRootGroup()
    full = variable if variable.startswith("/") else "/" + variable
    try:
        arr = rg.OpenMDArrayFromFullname(full)
    except RuntimeError:
        arr = None
    if arr is None:
        names = rg.GetMDArrayNames() or []
        raise ValueError(f"variable {variable!r} is not in {path}; arrays: {names}")
    return ds, arr


def _coordinate(dim) -> np.ndarray:
    iv = dim.GetIndexingVariable()
    if iv is None:
        raise ValueError(f"dimension {dim.GetName()!r} has no coordinate variable")
    return np.asarray(iv.ReadAsArray(), dtype=np.float64)


def _index_edges(centres: np.ndarray) -> np.ndarray:
    """Cell edges for 1-D ascending pixel centres, for nearest-pixel lookup."""
    if len(centres) == 1:
        return np.array([centres[0] - 0.5, centres[0] + 0.5])
    mid = (centres[1:] + centres[:-1]) / 2.0
    return np.concatenate([[2 * centres[0] - mid[0]], mid, [2 * centres[-1] - mid[-1]]])


class CubeSource:
    """A (time, lat, lon) variable — or several sharing a grid — over 1+ files."""

    def __init__(self, paths: Sequence[str], variables: Sequence[str]):
        if not paths:
            raise ValueError("at least one input is required")
        if not variables:
            raise ValueError("at least one --variable is required")
        self.paths = list(paths)
        self.variables = list(variables)
        self._handles: Dict[Tuple[str, str], tuple] = {}

        ds, arr = self._array(self.paths[0], self.variables[0])
        dims = arr.GetDimensions()
        kinds = [_classify(d) for d in dims]
        if sorted(k for k in kinds if k) != ["lat", "lon", "time"] or len(dims) != 3:
            raise ValueError(
                f"{self.variables[0]!r} has dimensions "
                f"{[(d.GetName(), k) for d, k in zip(dims, kinds)]}; mdim reads "
                f"(time, lat, lon) cubes. Other dimensions (depth, ensemble, ...) "
                f"are not supported yet.")
        self.axis = {k: i for i, k in enumerate(kinds)}       # storage axis of each
        self.lat = _coordinate(dims[self.axis["lat"]])
        self.lon = _coordinate(dims[self.axis["lon"]])
        self.block = [b if b else d.GetSize() for b, d in zip(arr.GetBlockSize(), dims)]
        self.lon_360 = bool(self.lon.max() > 180.0)
        # Ascending copies for index lookup; descending latitude is common.
        self._lat_desc = bool(len(self.lat) > 1 and self.lat[0] > self.lat[-1])
        self._lat_edges = _index_edges(self.lat[::-1] if self._lat_desc else self.lat)
        self._lon_edges = _index_edges(self.lon)

        for v in self.variables[1:]:
            self._check_same_grid(self.paths[0], v)
        segments, times = [], []
        start = 0
        for n, path in enumerate(self.paths):
            if n:
                for v in self.variables:
                    self._check_same_grid(path, v)
            _, a = self._array(path, self.variables[0])
            tdim = a.GetDimensions()[self.axis["time"]]
            iv = tdim.GetIndexingVariable()
            units = (iv.GetUnit() if iv is not None else "") or _attr(iv, "units")
            decoded = decode_cf_time(_coordinate(tdim), units, _attr(iv, "calendar"))
            segments.append(Segment(path, start, tdim.GetSize()))
            times.append(decoded)
            start += tdim.GetSize()
        self.segments = segments
        cals = {t.calendar for t in times}
        if len(cals) > 1:
            raise ValueError(f"inputs disagree on calendar: {sorted(cals)}")
        self.time = DecodedTime(
            np.concatenate([t.year for t in times]),
            np.concatenate([t.month for t in times]),
            np.concatenate([t.day for t in times]),
            None if any(t.dates is None for t in times) else np.concatenate([t.dates for t in times]),
            times[0].calendar,
        )

    # -- handles --------------------------------------------------------

    def _array(self, path, variable):
        key = (path, variable)
        if key not in self._handles:
            self._handles[key] = _open_array(path, variable)
        return self._handles[key]

    def _check_same_grid(self, path, variable):
        _, a = self._array(path, variable)
        dims = a.GetDimensions()
        kinds = [_classify(d) for d in dims]
        if {k: i for i, k in enumerate(kinds)} != self.axis:
            raise ValueError(f"{variable!r} in {path} has a different dimension order")
        if (not np.allclose(_coordinate(dims[self.axis["lat"]]), self.lat)
                or not np.allclose(_coordinate(dims[self.axis["lon"]]), self.lon)):
            raise ValueError(f"{variable!r} in {path} is on a different lat/lon grid")

    # -- geometry -------------------------------------------------------

    @property
    def pixel_deg(self) -> Tuple[float, float]:
        """(lat, lon) spacing in degrees, from the median step."""
        dy = float(np.median(np.abs(np.diff(self.lat)))) if len(self.lat) > 1 else 1.0
        dx = float(np.median(np.abs(np.diff(self.lon)))) if len(self.lon) > 1 else 1.0
        return dy, dx

    def _to_source_lon(self, lon_intervals) -> List[Tuple[float, float]]:
        """[-180, 180] intervals in the source's longitude convention."""
        if not self.lon_360:
            return [(max(lo, -180.0), min(hi, 180.0)) for lo, hi in lon_intervals]
        out = []
        for lo, hi in lon_intervals:
            lo, hi = max(lo, -180.0), min(hi, 180.0)
            if hi <= 0:
                out.append((lo + 360.0, hi + 360.0))
            elif lo >= 0:
                out.append((lo, hi))
            else:
                out.extend([(lo + 360.0, 360.0), (0.0, hi)])
        return out

    def _lat_index_range(self, lat_lo, lat_hi) -> Optional[Tuple[int, int]]:
        """Storage index range [i0, i1) of pixels whose centres are in range."""
        idx = np.where((self.lat >= lat_lo) & (self.lat <= lat_hi))[0]
        if len(idx) == 0:
            return None
        return int(idx.min()), int(idx.max()) + 1

    def windows(self, lat_lo, lat_hi, lon_intervals) -> List[Tuple[int, int, int, int]]:
        """Storage-index windows (y0, y1, x0, x1) whose pixel centres fall in
        the footprint. Usually one; two when the footprint crosses the source's
        longitude seam."""
        ys = self._lat_index_range(lat_lo, lat_hi)
        if ys is None:
            return []
        out = []
        for lo, hi in self._to_source_lon(lon_intervals):
            idx = np.where((self.lon >= lo) & (self.lon <= hi))[0]
            if len(idx):
                out.append((ys[0], ys[1], int(idx.min()), int(idx.max()) + 1))
        return out

    def tiles(self, window) -> Iterator[Tuple[int, int, int, int]]:
        """Split a window into pieces that each lie within one storage block,
        so every read decodes each chunk it touches exactly once."""
        y0, y1, x0, x1 = window
        by, bx = self.block[self.axis["lat"]], self.block[self.axis["lon"]]
        for ty in range(y0 - y0 % by, y1, by):
            for tx in range(x0 - x0 % bx, x1, bx):
                yield max(ty, y0), min(ty + by, y1), max(tx, x0), min(tx + bx, x1)

    def nearest_pixel(self, lat: np.ndarray, lon: np.ndarray):
        """(iy, ix) storage indices of the pixel containing each point, -1 if
        outside the grid. Points are in [-180, 180] longitude."""
        lon = np.where(lon < 0, lon + 360.0, lon) if self.lon_360 else lon
        iy = np.searchsorted(self._lat_edges, lat, side="right") - 1
        ny = len(self.lat)
        iy = np.where((iy < 0) | (iy >= ny), -1, iy)
        if self._lat_desc:
            iy = np.where(iy >= 0, ny - 1 - iy, -1)
        ix = np.searchsorted(self._lon_edges, lon, side="right") - 1
        ix = np.where((ix < 0) | (ix >= len(self.lon)), -1, ix)
        return iy.astype(np.int64), ix.astype(np.int64)

    # -- time -----------------------------------------------------------

    def time_groups(self, t_lo: int, t_hi: int, pixels: int,
                    budget_bytes: float) -> Iterator[Tuple[int, int]]:
        """Global time ranges [t0, t1) to read together: block-aligned within
        each file, never crossing a file, and grouped up to *budget_bytes* of
        float64 per variable."""
        bt = self.block[self.axis["time"]]
        per_step = max(1, pixels) * 8 * len(self.variables)
        steps = max(1, int(budget_bytes // per_step))
        span = max(bt, (steps // bt) * bt) if steps >= bt else steps
        for seg in self.segments:
            lo, hi = max(t_lo, seg.start), min(t_hi, seg.start + seg.size)
            if lo >= hi:
                continue
            local = lo - seg.start
            cursor = local - local % bt if span >= bt else local
            while cursor + seg.start < hi:
                a = max(cursor + seg.start, lo)
                b = min(cursor + seg.start + span, hi)
                yield a, b
                cursor += span

    # -- read -----------------------------------------------------------

    def read(self, variable: str, t0: int, t1: int, y0: int, y1: int,
             x0: int, x1: int) -> np.ndarray:
        """Values as float64 (time, lat, lon) in storage lat/lon order, with
        nodata as NaN and CF scale/offset applied. [t0, t1) must lie in one file."""
        seg = next(s for s in self.segments if s.start <= t0 < s.start + s.size)
        if t1 > seg.start + seg.size:
            raise ValueError("a read may not span files")
        _, arr = self._array(seg.path, variable)
        start = [0, 0, 0]
        count = [0, 0, 0]
        start[self.axis["time"]], count[self.axis["time"]] = t0 - seg.start, t1 - t0
        start[self.axis["lat"]], count[self.axis["lat"]] = y0, y1 - y0
        start[self.axis["lon"]], count[self.axis["lon"]] = x0, x1 - x0
        data = np.asarray(arr.ReadAsArray(array_start_idx=start, count=count), dtype=np.float64)
        data = np.transpose(data, (self.axis["time"], self.axis["lat"], self.axis["lon"]))
        nodata = None
        try:
            nodata = arr.GetNoDataValueAsDouble()
        except Exception:
            pass
        if nodata is not None and not np.isnan(nodata):
            data[data == nodata] = np.nan
        scale, offset = arr.GetScale(), arr.GetOffset()
        if scale not in (None, 1.0) or offset not in (None, 0.0):
            data = data * (scale if scale is not None else 1.0) + (offset or 0.0)
        return data
