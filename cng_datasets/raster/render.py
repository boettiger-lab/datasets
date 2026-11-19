"""Default renders and thumbnails for the COGs a workflow writes (issue #340).

A STAC collection with a COG needs a render (the STAC render extension: how a
map app draws it) and a thumbnail (PORTO-CORE-067). Both follow from the file
alone, so they are made here, at build time, rather than by the STAC author:

* A band with a color table is drawn with it: each entry becomes a colormap
  class, resampled nearest.
* Any other band is drawn with viridis stretched over its 2nd to 98th
  percentile, read from an overview. Minimum to maximum, although every COG
  carries it (#342), leaves skewed data such as population or carbon in the
  bottom of the ramp, which data-workflows' render check refuses. The stretch
  is in real units when the band has a scale or offset, because a map app
  unscales before it applies ``rescale``.
* A band with one value throughout has nothing to stretch and gets no render.

The automatic render is a default. Diverging data, a categorical raster with
no color table, or a log scale need a person's choice, which ``colormap``,
``rescale`` and ``colormap_json`` override.

The thumbnail draws the same render over the COG's overviews with GDAL, so it
needs no browser or tile server.

Output, keyed by object key like the checksums file, for the STAC author to
merge into ``renders`` (adding ``title`` and ``assets``)::

    {"bucket": "...", "generator": "cng-datasets x.y.z", "created": "...",
     "renders": {"lc/nlcd-2001-cog.tif": {"colormap_name": "viridis",
                                          "rescale": [[0.0, 95.0]], "nodata": 255,
                                          "thumbnail": "lc/nlcd-2001-cog.thumbnail.png"}}}
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

# matplotlib's colormaps at 0.0, 0.1, ... 1.0, as TiTiler names them. A
# thumbnail can be drawn for these, and their reverses (``_r``).
RAMPS = {
    "viridis": ["#440154", "#482475", "#414487", "#355f8d", "#2a788e", "#21918c",
                "#22a884", "#44bf70", "#7ad151", "#bddf26", "#fde725"],
    "rdbu": ["#67001f", "#b2182b", "#d6604d", "#f4a582", "#fddbc7", "#f7f7f7",
             "#d1e5f0", "#92c5de", "#4393c3", "#2166ac", "#053061"],
}
CYCLIC = {"twilight", "twilight_shifted", "hsv"}
PERCENTILES = (2, 98)
THUMBNAIL_SIZE = 512
STATS_SIZE = 1024  # pixels on the longer side of the overview the percentiles come from


def _open(path: str):
    from osgeo import gdal
    gdal.UseExceptions()
    return gdal.Open(_gdal_path(path))


def _gdal_path(path: str) -> str:
    return "/vsis3/" + path[len("s3://"):] if path.startswith("s3://") else path


def _hex(rgb) -> str:
    return "#" + "".join(f"{int(c):02x}" for c in rgb[:3])


def _real(x: float, scale: float, offset: float) -> float:
    return round(float(x) * scale + offset, 6)


def _sample(band, size: int = STATS_SIZE):
    """Valid values of a band, read at ``size`` pixels on the longer side (an overview)."""
    from osgeo import gdal
    w, h = band.XSize, band.YSize
    k = min(1.0, size / max(w, h))
    data = band.ReadAsArray(buf_xsize=max(1, round(w * k)), buf_ysize=max(1, round(h * k)),
                            resample_alg=gdal.GRIORA_NearestNeighbour).astype(float)
    nodata = band.GetNoDataValue()
    valid = np.isfinite(data)
    if nodata is not None:
        valid &= data != nodata
    return data[valid]


def cog_render(path: str, colormap: Optional[str] = None,
               rescale: Optional[List[float]] = None,
               colormap_json: Optional[Dict] = None) -> Optional[Dict]:
    """The render for band 1 of a COG, or None when it has nothing to show.

    Args:
        colormap: A TiTiler colormap name instead of viridis.
        rescale: ``[lo, hi]`` in real units instead of the 2nd-98th percentile.
        colormap_json: ``{value: "#rrggbb"}`` classes instead of the color table.
    """
    ds = _open(path)
    band = ds.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    table = band.GetColorTable()
    if colormap_json:
        out = {"colormap": {str(k): v for k, v in colormap_json.items()}, "resampling": "nearest"}
    elif table is not None and not colormap:
        classes = {}
        for i in range(table.GetCount()):
            entry = table.GetColorEntry(i)
            if (nodata is not None and i == nodata) or entry[3] == 0:
                continue
            classes[str(i)] = _hex(entry)
        if not classes:
            return None
        out = {"colormap": classes, "resampling": "nearest"}
    else:
        name = colormap or "viridis"
        out = {"colormap_name": name}
        if rescale is not None:
            lo, hi = sorted(float(x) for x in rescale)
        elif name in CYCLIC:
            lo = hi = None  # a cyclic map is not stretched
        else:
            values = _sample(band)
            if values.size == 0:
                return None
            scale, offset = band.GetScale() or 1.0, band.GetOffset() or 0.0
            lo, hi = (_real(np.percentile(values, q), scale, offset) for q in PERCENTILES)
            if not hi > lo:  # most values equal: widen to the full range
                lo = _real(values.min(), scale, offset)
                hi = _real(values.max(), scale, offset)
        if lo is not None:
            if not hi > lo:
                return None
            out["rescale"] = [[lo, hi]]
    if nodata is not None:
        out["nodata"] = int(nodata) if float(nodata).is_integer() else nodata
    return out


def ramp_stops(name: str) -> Optional[List[str]]:
    """The colour stops of a named colormap, or None if a thumbnail cannot draw it."""
    base = name.lower()
    reverse = base.endswith("_r")
    stops = RAMPS.get(base.removesuffix("_r"))
    if stops is None:
        return None
    return stops[::-1] if reverse else stops


def _ramp(values: np.ndarray, stops_hex: List[str]) -> np.ndarray:
    """RGB along ``stops_hex`` for values in 0..1."""
    stops = np.array([[int(c[i:i + 2], 16) for i in (1, 3, 5)] for c in stops_hex], dtype=float)
    pos = np.clip(values, 0, 1) * (len(stops) - 1)
    i = np.minimum(np.floor(pos).astype(int), len(stops) - 2)
    f = (pos - i)[..., None]
    return (stops[i] * (1 - f) + stops[i + 1] * f).round().astype(np.uint8)


def thumbnail_png(path: str, render: Dict, size: int = THUMBNAIL_SIZE) -> bytes:
    """A PNG of the COG drawn with ``render``, its longer side ``size`` pixels.

    Read through GDAL at the thumbnail's size, which picks the nearest
    overview, so a large COG costs a few reads. Nodata is transparent.
    """
    from osgeo import gdal
    ds = _open(path)
    w, h = ds.RasterXSize, ds.RasterYSize
    k = size / max(w, h)
    tw, th = max(1, round(w * k)), max(1, round(h * k))
    band = ds.GetRasterBand(1)
    resample = gdal.GRIORA_NearestNeighbour if "colormap" in render else gdal.GRIORA_Average
    data = band.ReadAsArray(buf_xsize=tw, buf_ysize=th, resample_alg=resample).astype(float)
    nodata = band.GetNoDataValue()
    valid = np.isfinite(data)
    if nodata is not None:
        valid &= data != nodata
    rgba = np.zeros((th, tw, 4), dtype=np.uint8)
    if "colormap" in render:
        for code, colour in render["colormap"].items():
            hit = valid & (data == float(code))
            rgba[hit, :3] = [int(colour[i:i + 2], 16) for i in (1, 3, 5)]
            rgba[hit, 3] = 255
    else:
        stops = ramp_stops(render["colormap_name"])
        if stops is None or "rescale" not in render:
            raise ValueError(f"No thumbnail ramp for colormap {render['colormap_name']!r}; "
                             f"one of {sorted(RAMPS)} (or _r) with a rescale can be drawn")
        scale, offset = band.GetScale() or 1.0, band.GetOffset() or 0.0
        lo, hi = render["rescale"][0]
        real = data * scale + offset
        rgba[..., :3] = _ramp((real - lo) / (hi - lo), stops)
        rgba[..., 3] = np.where(valid, 255, 0)
    mem = gdal.GetDriverByName("MEM").Create("", tw, th, 4, gdal.GDT_Byte)
    for i in range(4):
        mem.GetRasterBand(i + 1).WriteArray(rgba[..., i])
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "thumbnail.png")
        gdal.GetDriverByName("PNG").CreateCopy(out, mem)
        with open(out, "rb") as f:
            return f.read()


def thumbnail_key(cog_key: str) -> str:
    """``a/b-cog.tif`` -> ``a/b-cog.thumbnail.png``."""
    stem = cog_key[: -len(".tiff")] if cog_key.endswith(".tiff") else cog_key.removesuffix(".tif")
    return f"{stem}.thumbnail.png"


def _put(url: str, body: bytes, content_type: str, client=None) -> None:
    from ..storage.checksums import _split_s3, s3_client
    if url.startswith("s3://"):
        bucket, key = _split_s3(url)
        (client or s3_client()).put_object(Bucket=bucket, Key=key, Body=body,
                                           ContentType=content_type)
    else:
        with open(url, "wb") as f:
            f.write(body)


def write_renders(cogs: List[str], output: str, thumbnails: bool = True,
                  size: int = THUMBNAIL_SIZE, client=None, colormap: Optional[str] = None,
                  rescale: Optional[List[float]] = None,
                  colormap_json: Optional[Dict] = None) -> Dict:
    """Render (and thumbnail) each COG and write the JSON to ``output``.

    Args:
        cogs: ``s3://bucket/key`` URLs in one bucket, or local paths.
        output: Where to write the JSON (s3:// or local).
        thumbnails: Also write ``<cog stem>.thumbnail.png`` beside each COG.
        colormap, rescale, colormap_json: Overrides, as for :func:`cog_render`.
    """
    from .. import __version__
    from ..storage.checksums import _split_s3
    if not cogs:
        raise ValueError("No COGs to render")
    remote = [c.startswith("s3://") for c in cogs]
    if any(remote) and not all(remote):
        raise ValueError("COGs must be all s3:// URLs or all local paths")
    bucket = None
    if all(remote):
        buckets = {_split_s3(c)[0] for c in cogs}
        if len(buckets) != 1:
            raise ValueError(f"COGs span several buckets: {sorted(buckets)}")
        bucket = buckets.pop()

    renders = {}
    for cog in cogs:
        key = _split_s3(cog)[1] if bucket else cog
        render = cog_render(cog, colormap=colormap, rescale=rescale, colormap_json=colormap_json)
        if render is None:
            print(f"  {key}: one value throughout, so no render")
            continue
        drawable = "colormap" in render or (
            "rescale" in render and ramp_stops(render["colormap_name"]) is not None)
        if thumbnails and not drawable:
            print(f"  {key}: no thumbnail; colormap {render.get('colormap_name')!r} is not one "
                  f"this tool can draw ({', '.join(sorted(RAMPS))}, or _r)")
        elif thumbnails:
            png = thumbnail_png(cog, render, size)
            thumb = thumbnail_key(key)
            _put(f"s3://{bucket}/{thumb}" if bucket else thumb, png, "image/png", client)
            render["thumbnail"] = thumb
        renders[key] = render
        print(f"  {key}: {json.dumps({k: v for k, v in render.items() if k != 'colormap'})}")

    doc = {"generator": f"cng-datasets {__version__}",
           "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "renders": renders}
    if bucket:
        doc = {"bucket": bucket, **doc}
    _put(output, (json.dumps(doc, indent=2) + "\n").encode(), "application/json", client)
    return doc
