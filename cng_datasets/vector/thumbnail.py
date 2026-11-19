"""Thumbnails of a vector dataset drawn with its default style (issue #340).

A STAC collection needs a thumbnail (PORTO-CORE-067). For a PMTiles dataset it
shows the default style, so it is drawn here from the GeoParquet with GDAL,
rather than by a browser running MapLibre. It is a gallery preview: it matches
the style's classes, colours, opacities and filters, not MapLibre's
antialiasing or label placement.

What is drawn:

* Each ``fill``, ``line`` and ``circle`` layer of the style, in order. Symbol,
  raster and background layers are left out, as are hidden ones.
* Only the features a layer's ``filter`` keeps. A filter that hides
  placeholder features is what keeps a thumbnail from zooming out to a square
  in the ocean, so an expression this module cannot evaluate is an error, not
  something skipped.
* Over the extent of the features drawn, in Web Mercator like the map apps,
  4:3, with a margin, on a light background.

The expressions evaluated are the ones styles here use: ``get``, ``has``,
``!``, comparisons, ``all``, ``any``, ``in``, ``case``, ``match``, ``step``,
``interpolate`` (linear), ``coalesce``, ``to-string``, ``to-number``,
``to-boolean``, ``typeof``, ``literal`` and arithmetic, plus the legacy filter
form (``["==", "field", value]``).
"""

import math
import os
import re
import tempfile
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np

BACKGROUND = "#f4f4f2"
SIZE = 512
MARGIN = 12  # pixels
MERCATOR_LAT = 85.05112878


# ------------------------------------------------------------------ expressions

class UnsupportedExpression(ValueError):
    pass


def _typeof(v) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, (int, float)):
        return "number"
    if isinstance(v, str):
        return "string"
    if isinstance(v, list):
        return "array"
    return "object"


def _to_string(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


def _to_number(v):
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return None
    return None


def _compare(op, a, b) -> bool:
    if op == "==":
        return a == b
    if op == "!=":
        return a != b
    if a is None or b is None or _typeof(a) != _typeof(b):
        return False
    return {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}[op]


_LEGACY = {"==", "!=", "<", "<=", ">", ">=", "in", "!in", "has", "!has"}


def _legacy(expr) -> bool:
    """Whether a filter is in the legacy form, ``["==", "field", value]``."""
    return (isinstance(expr, list) and len(expr) >= 2 and expr[0] in _LEGACY
            and isinstance(expr[1], str) and not (expr[0] in ("in", "!in") and len(expr) == 3
                                                   and isinstance(expr[2], list)))


def evaluate(expr, props: Dict[str, Any]):
    """Evaluate a MapLibre expression (or legacy filter) on one feature's properties."""
    if not isinstance(expr, list):
        return expr
    if not expr:
        return expr
    op = expr[0]
    if not isinstance(op, str):  # a bare array of values
        return expr
    if _legacy(expr):
        key = expr[1]
        val = props.get(key)
        if op == "has":
            return key in props and props[key] is not None
        if op == "!has":
            return not (key in props and props[key] is not None)
        if op == "in":
            return val in expr[2:]
        if op == "!in":
            return val not in expr[2:]
        return _compare(op, val, expr[2])

    def ev(e):
        return evaluate(e, props)

    if op == "literal":
        return expr[1]
    if op == "get":
        return props.get(ev(expr[1]))
    if op == "has":
        k = ev(expr[1])
        return k in props and props[k] is not None
    if op == "!":
        return not ev(expr[1])
    if op in ("==", "!=", "<", "<=", ">", ">="):
        return _compare(op, ev(expr[1]), ev(expr[2]))
    if op == "all":
        return all(ev(e) for e in expr[1:])
    if op == "any":
        return any(ev(e) for e in expr[1:])
    if op == "in":
        needle, hay = ev(expr[1]), ev(expr[2])
        if isinstance(hay, str):
            return isinstance(needle, str) and needle in hay
        return needle in (hay or [])
    if op == "case":
        args = expr[1:]
        for i in range(0, len(args) - 1, 2):
            if ev(args[i]):
                return ev(args[i + 1])
        return ev(args[-1])
    if op == "match":
        value, args = ev(expr[1]), expr[2:]
        for i in range(0, len(args) - 1, 2):
            labels = args[i] if isinstance(args[i], list) else [args[i]]
            if value in labels:
                return ev(args[i + 1])
        return ev(args[-1])
    if op == "coalesce":
        for e in expr[1:]:
            v = ev(e)
            if v is not None:
                return v
        return None
    if op == "to-string":
        return _to_string(ev(expr[1]))
    if op == "to-number":
        for e in expr[1:]:
            n = _to_number(ev(e))
            if n is not None:
                return n
        return 0.0 if len(expr) == 2 and ev(expr[1]) is None else None
    if op == "to-boolean":
        v = ev(expr[1])
        return bool(v) and v != 0 and v != ""
    if op == "typeof":
        return _typeof(ev(expr[1]))
    if op in ("+", "*"):
        vals = [_to_number(ev(e)) for e in expr[1:]]
        if any(v is None for v in vals):
            return None
        return sum(vals) if op == "+" else math.prod(vals)
    if op in ("-", "/"):
        a = _to_number(ev(expr[1]))
        if len(expr) == 2:
            return None if a is None else -a
        b = _to_number(ev(expr[2]))
        if a is None or b is None:
            return None
        if op == "-":
            return a - b
        return a / b if b != 0 else None
    if op == "step":
        x = _to_number(ev(expr[1]))
        out = ev(expr[2])
        if x is None:
            return out
        for i in range(3, len(expr) - 1, 2):
            if x >= expr[i]:
                out = ev(expr[i + 1])
            else:
                break
        return out
    if op == "interpolate":
        kind = expr[1]
        if not (isinstance(kind, list) and kind[0] == "linear"):
            raise UnsupportedExpression(f"interpolate {kind!r}: only linear is drawn")
        x = _to_number(ev(expr[2]))
        stops = [(expr[i], ev(expr[i + 1])) for i in range(3, len(expr) - 1, 2)]
        if x is None or not stops:
            return None
        if x <= stops[0][0]:
            return stops[0][1]
        for (x0, c0), (x1, c1) in zip(stops, stops[1:]):
            if x <= x1:
                f = (x - x0) / (x1 - x0) if x1 != x0 else 0
                if isinstance(c0, (int, float)):
                    return c0 + (c1 - c0) * f
                r0, r1 = parse_color(c0), parse_color(c1)
                return tuple(a + (b - a) * f for a, b in zip(r0, r1))
        return stops[-1][1]
    raise UnsupportedExpression(f"expression operator {op!r} is not drawn by the thumbnail")


def referenced_properties(expr) -> List[str]:
    """Property names an expression or legacy filter reads."""
    out: List[str] = []

    def walk(e):
        if not isinstance(e, list) or not e:
            return
        if _legacy(e):
            out.append(e[1])
            return
        if e[0] in ("get", "has") and len(e) == 2 and isinstance(e[1], str):
            out.append(e[1])
            return
        if e[0] == "literal":
            return
        for x in e[1:]:
            walk(x)
    walk(expr)
    return list(dict.fromkeys(out))


_HEX = re.compile(r"^#([0-9a-f]{3}|[0-9a-f]{6}|[0-9a-f]{8})$", re.I)
_RGB = re.compile(r"^rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)\s*(?:,\s*([\d.]+)\s*)?\)$", re.I)
_NAMED = {"black": "#000000", "white": "#ffffff", "red": "#ff0000", "green": "#008000",
          "blue": "#0000ff", "gray": "#808080", "grey": "#808080", "transparent": "#00000000"}


def parse_color(c) -> Tuple[float, float, float, float]:
    """``(r, g, b, a)`` in 0..255 (a in 0..1) from a CSS colour string or tuple."""
    if isinstance(c, (tuple, list)) and len(c) in (3, 4) and all(isinstance(x, (int, float)) for x in c):
        return (float(c[0]), float(c[1]), float(c[2]), float(c[3]) if len(c) == 4 else 1.0)
    s = _NAMED.get(str(c).strip().lower(), str(c).strip())
    m = _HEX.match(s)
    if m:
        h = m.group(1)
        if len(h) == 3:
            h = "".join(ch * 2 for ch in h)
        a = int(h[6:8], 16) / 255 if len(h) == 8 else 1.0
        return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), a)
    m = _RGB.match(s)
    if m:
        a = float(m.group(4)) if m.group(4) is not None else 1.0
        return (float(m.group(1)), float(m.group(2)), float(m.group(3)), a)
    raise UnsupportedExpression(f"colour {c!r} is not one the thumbnail can read")


# ------------------------------------------------------------------ drawing

def _layers(style: dict) -> List[dict]:
    out = []
    for lay in style.get("layers", []):
        if lay.get("type") not in ("fill", "line", "circle"):
            continue
        if (lay.get("layout") or {}).get("visibility") == "none":
            continue
        out.append(lay)
    if not out:
        raise ValueError("The style has no fill, line or circle layer to draw")
    return out


def _paint(lay: dict) -> Tuple[Any, Any, float]:
    """(colour expression, opacity expression, width in pixels) for a layer."""
    t = lay["type"]
    p = lay.get("paint") or {}
    if t == "fill":
        return p.get("fill-color", "#000000"), p.get("fill-opacity", 1.0), 0.0
    if t == "line":
        return p.get("line-color", "#000000"), p.get("line-opacity", 1.0), p.get("line-width", 1.0)
    return p.get("circle-color", "#000000"), p.get("circle-opacity", 1.0), p.get("circle-radius", 5.0)


def _mercator(lon: float, lat: float) -> Tuple[float, float]:
    lat = max(-MERCATOR_LAT, min(MERCATOR_LAT, lat))
    x = math.radians(lon) * 6378137.0
    y = math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)) * 6378137.0
    return x, y


def _frame(bbox: Sequence[float], width: int, height: int) -> Tuple[float, float, float, float]:
    """Web Mercator bounds of a ``width`` x ``height`` image that fits ``bbox`` with a margin."""
    w, s, e, n = bbox
    if e < w:  # crosses the antimeridian: show the whole width
        w, e = -180.0, 180.0
    x0, y0 = _mercator(w, s)
    x1, y1 = _mercator(e, n)
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    span = max((x1 - x0) / max(1, width - 2 * MARGIN), (y1 - y0) / max(1, height - 2 * MARGIN))
    span = span if span > 0 else 1.0  # a single point
    return (cx - span * width / 2, cy - span * height / 2, cx + span * width / 2, cy + span * height / 2)


def _sql(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"


def _connect(parquet: str):
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    if parquet.startswith(("s3://", "http://", "https://")):
        from ..storage.s3 import configure_s3_credentials
        configure_s3_credentials(con)
    return con


def _geometry_column(con, parquet: str) -> str:
    from .geoparquet import geometry_columns, primary_geometry
    schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet({_sql(parquet)})").fetchall()
    cols = geometry_columns(schema)
    if not cols:
        raise ValueError(f"{parquet}: no geometry column")
    return primary_geometry(cols)


def render_thumbnail(style: dict, parquet: str, size: int = SIZE,
                     background: str = BACKGROUND) -> bytes:
    """A PNG of ``parquet`` drawn with ``style``, ``size`` x ``3/4 size`` pixels."""
    from osgeo import gdal, osr
    gdal.UseExceptions()
    width, height = size, round(size * 3 / 4)
    layers = _layers(style)
    con = _connect(parquet)
    geom = _geometry_column(con, parquet)

    # Which properties the layers read, and each feature's values for them.
    names = []
    for lay in layers:
        colour, opacity, _ = _paint(lay)
        for e in (lay.get("filter"), colour, opacity):
            names += referenced_properties(e)
    names = list(dict.fromkeys(names))
    present = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_sql(parquet)})").fetchall()}
    names = [n for n in names if n in present]
    g = '"' + geom.replace('"', '""') + '"'
    cols = ", ".join('"' + n.replace('"', '""') + '"' for n in names)
    # file_row_number ties these rows to the geometry read below.
    scan = f"read_parquet({_sql(parquet)}, file_row_number = true)"
    rows = con.execute(
        f"SELECT ST_XMin({g}), ST_YMin({g}), ST_XMax({g}), ST_YMax({g}), file_row_number"
        f"{', ' + cols if cols else ''} FROM {scan} "
        f"WHERE {g} IS NOT NULL ORDER BY file_row_number").fetchall()
    rids = [r[4] for r in rows]

    # Evaluate each layer's filter and colour per feature.
    per_layer = []
    drawn = np.zeros(len(rows), dtype=bool)
    for lay in layers:
        colour, opacity, width_px = _paint(lay)
        flt = lay.get("filter")
        keep = np.zeros(len(rows), dtype=bool)
        colours = {}
        index = np.zeros(len(rows), dtype=np.int32)
        for i, r in enumerate(rows):
            props = dict(zip(names, r[5:]))
            if flt is not None and not evaluate(flt, props):
                continue
            c = parse_color(evaluate(colour, props))
            a = c[3] * float(evaluate(opacity, props) if opacity is not None else 1.0)
            if lay["type"] == "line":  # a line under a pixel wide covers part of it
                a *= min(1.0, float(width_px))
            if a <= 0:
                continue
            key = (round(c[0]), round(c[1]), round(c[2]), round(a, 3))
            index[i] = colours.setdefault(key, len(colours) + 1)
            keep[i] = True
        drawn |= keep
        per_layer.append((lay, keep, index, colours, width_px))

    if not drawn.any():
        raise ValueError("The style's filters leave no feature to draw")
    ext = np.array([r[:4] for r in rows], dtype=float)[drawn]
    bounds = _frame([ext[:, 0].min(), ext[:, 1].min(), ext[:, 2].max(), ext[:, 3].max()], width, height)
    px = (bounds[2] - bounds[0]) / width

    bg = parse_color(background)
    canvas = np.empty((height, width, 3), dtype=float)
    canvas[...] = bg[:3]
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(3857)

    with tempfile.TemporaryDirectory() as tmp:
        # The geometry once, in Web Mercator and simplified to half a pixel.
        # Only a feature reaching past Mercator's latitude limit is clipped.
        src = os.path.join(tmp, "geom.parquet")
        clipped = (f"CASE WHEN ST_YMin({g}) < -{MERCATOR_LAT} OR ST_YMax({g}) > {MERCATOR_LAT} "
                   f"THEN ST_Intersection({g}, ST_MakeEnvelope(-180, -{MERCATOR_LAT}, 180, {MERCATOR_LAT})) "
                   f"ELSE {g} END")
        con.execute(
            f"COPY (SELECT file_row_number AS rid, ST_SimplifyPreserveTopology("
            f"ST_Transform({clipped}, 'EPSG:4326', 'EPSG:3857'), {px / 2}) AS geom "
            f"FROM {scan} WHERE {g} IS NOT NULL) TO {_sql(src)} (FORMAT PARQUET)")
        for n, (lay, keep, index, colours, width_px) in enumerate(per_layer):
            if not keep.any():
                continue
            con.execute("CREATE OR REPLACE TEMP TABLE cls AS SELECT unnest(?) AS rid, unnest(?) AS cls",
                        [[rids[i] for i in np.flatnonzero(keep)], index[keep].tolist()])
            t = lay["type"]
            half = max(float(width_px), 1.0) * px / 2
            if t == "fill":
                shape = "geom"
            elif t == "line":  # a line layer over polygons draws their boundaries
                shape = (f"ST_Buffer(CASE WHEN ST_Dimension(geom) = 2 THEN ST_Boundary(geom) "
                         f"ELSE geom END, {half})")
            else:
                shape = f"ST_Buffer(geom, {max(float(width_px), 1.0) * px})"
            fgb = os.path.join(tmp, f"layer{n}.fgb")
            con.execute(
                f"COPY (SELECT cls.cls, {shape} AS geom FROM read_parquet({_sql(src)}) p "
                f"JOIN cls USING (rid) WHERE NOT ST_IsEmpty(p.geom)) TO {_sql(fgb)} "
                f"(FORMAT GDAL, DRIVER 'FlatGeobuf', SRS 'EPSG:3857')")
            # One pass per colour, adding 1 per feature, so each pixel knows how
            # many features of that colour cover it. MapLibre blends every
            # overlapping feature, so n layers at opacity a cover 1-(1-a)^n.
            for (r, gr, b, a), k in colours.items():
                mem = gdal.GetDriverByName("MEM").Create("", width, height, 1, gdal.GDT_UInt16)
                mem.SetGeoTransform([bounds[0], px, 0, bounds[3], 0, -px])
                mem.SetProjection(srs.ExportToWkt())
                gdal.Rasterize(mem, fgb, burnValues=[1], where=f"cls = {k}", add=True, allTouched=True)
                count = mem.GetRasterBand(1).ReadAsArray().astype(float)
                mem = None
                hit = count > 0
                if hit.any():
                    cover = (1 - (1 - a) ** count[hit])[:, None]
                    canvas[hit] = canvas[hit] * (1 - cover) + np.array([r, gr, b]) * cover
    con.close()

    rgb = canvas.round().clip(0, 255).astype(np.uint8)
    mem = gdal.GetDriverByName("MEM").Create("", width, height, 3, gdal.GDT_Byte)
    for i in range(3):
        mem.GetRasterBand(i + 1).WriteArray(rgb[..., i])
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "thumbnail.png")
        gdal.GetDriverByName("PNG").CreateCopy(out, mem)
        with open(out, "rb") as f:
            return f.read()


def load_style(source: str) -> dict:
    """A style JSON from a local path, http(s):// URL or s3:// URL."""
    import json
    if source.startswith("s3://"):
        from ..storage.checksums import _split_s3, s3_client
        bucket, key = _split_s3(source)
        return json.loads(s3_client().get_object(Bucket=bucket, Key=key)["Body"].read())
    if source.startswith(("http://", "https://")):
        from urllib.request import urlopen
        with urlopen(source, timeout=60) as r:
            return json.loads(r.read())
    with open(source) as f:
        return json.load(f)


def write_thumbnail(style: str, parquet: str, output: str, size: int = SIZE) -> int:
    """Draw ``parquet`` with the style at ``style`` and write the PNG to ``output``."""
    from ..raster.render import _put
    png = render_thumbnail(load_style(style), parquet, size)
    _put(output, png, "image/png")
    return len(png)
