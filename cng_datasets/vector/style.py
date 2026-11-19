"""Default MapLibre styles for a PMTiles file, built from its data (issue #340).

Which column a default style shows, and whether as categories or a ramp, is a
judgement about what the dataset is for, made by whoever writes its STAC
(data-workflows). Everything after that choice follows from the data and is
done here: the most frequent values or the quantile breaks, the palette, the
paint, and the legend. It is the algorithm of data-workflows'
``scripts/default-style.py``, reading the GeoParquet with DuckDB directly, and
writes only the style document. The STAC author places it and adds the asset.

Two steps, as there:

* :func:`profile` lists each layer's drawable attributes, from the PMTiles
  tilestats, beside the column descriptions in a STAC collection if given.
* :func:`build_style` builds the style for one column, or a single colour with
  a stated reason.

The style is MapLibre v8, one source ``data`` (``pmtiles://`` relative to the
style's own location when both are in one place), one layer named after the
source layer, plus an outline for polygons, and ``metadata`` with
``description``, ``glen:legend``, ``glen:tooltip_fields`` and, for a single
colour, ``glen:single_colour_reason``.
"""

import json
import posixpath
import re
from typing import Dict, List, Optional, Sequence, Tuple

NO_DATA = "#cccccc"
OTHER = "#bdbdbd"
CATEGORICAL = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948",
               "#b07aa1", "#ff9da7", "#9c755f", "#86bcb6", "#d4a6c8", "#a0cbe8"]
SEQUENTIAL = {
    "viridis": ["#440154", "#414487", "#2a788e", "#22a884", "#7ad151", "#fde725"],
    "ylorrd": ["#ffffb2", "#fed976", "#feb24c", "#fd8d3c", "#f03b20", "#bd0026"],
    "blues": ["#eff3ff", "#c6dbef", "#9ecae1", "#6baed6", "#3182bd", "#08519c"],
    "greens": ["#edf8e9", "#c7e9c0", "#a1d99b", "#74c476", "#31a354", "#006d2c"],
    "purples": ["#f2f0f7", "#dadaeb", "#bcbddc", "#9e9ac8", "#756bb1", "#54278f"],
    "oranges": ["#feedde", "#fdd0a2", "#fdae6b", "#fd8d3c", "#e6550d", "#a63603"],
}
SINGLE = {"fill": "#4a7bb7", "line": "#2b4c7e", "circle": "#2b4c7e"}
GEOM = {"polygon": "fill", "multipolygon": "fill", "linestring": "line",
        "multilinestring": "line", "point": "circle", "multipoint": "circle"}
SKIP = re.compile(r"^(_cng_fid|ogc_fid|objectid|fid|shape_(area|len|length)|shape_leng|h\d+)$", re.I)
STYLE_TYPE = "application/vnd.mapbox.style+json"


# ------------------------------------------------------------------ inputs

def layer_stats(pmtiles: str) -> Dict[str, dict]:
    """``{layer: tilestats layer}`` from a PMTiles file's metadata."""
    from .pmtiles import read_pmtiles_metadata
    meta = read_pmtiles_metadata(pmtiles)
    return {lay["layer"]: lay for lay in (meta.get("tilestats") or {}).get("layers", [])}


def collection_columns(collection: Optional[dict], asset_key: Optional[str] = None) -> Dict[str, dict]:
    """``{name: column}`` from a collection's ``table:columns``, overlaid by its assets' own."""
    if not collection:
        return {}
    out = {c["name"]: c for c in collection.get("table:columns") or [] if isinstance(c, dict)}
    for k, a in (collection.get("assets") or {}).items():
        if asset_key is None or k == asset_key:
            for c in a.get("table:columns") or []:
                out.setdefault(c["name"], {}).update({x: y for x, y in c.items() if y})
    return out


def load_collection(source: Optional[str]) -> Optional[dict]:
    """A STAC collection from a local path or an http(s) URL."""
    if not source:
        return None
    if source.startswith(("http://", "https://")):
        from urllib.request import urlopen
        with urlopen(source, timeout=60) as r:
            return json.loads(r.read())
    with open(source) as f:
        return json.load(f)


def _q(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def _sql_value(column: str) -> str:
    """SQL for a column, or for a ratio ``A/B`` of two numeric columns."""
    if "/" in column:
        a, b = column.split("/", 1)
        return f"TRY_CAST({_q(a)} AS DOUBLE) / NULLIF(TRY_CAST({_q(b)} AS DOUBLE), 0)"
    return f"TRY_CAST({_q(column)} AS DOUBLE)"


def _sql_literal(path: str) -> str:
    return "'" + path.replace("'", "''") + "'"


class _Parquet:
    """Queries on the GeoParquet that holds the PMTiles file's features."""

    def __init__(self, url: str):
        import duckdb
        self.url = url
        self.con = duckdb.connect()
        if url.startswith(("s3://", "http://", "https://")):
            from ..storage.s3 import configure_s3_credentials
            configure_s3_credentials(self.con)

    def rows(self, sql: str):
        return self.con.execute(sql).fetchall()

    def categories(self, column: str, limit: int) -> Tuple[List[Tuple[str, int]], int]:
        rows = self.rows(
            f"SELECT CAST({_q(column)} AS VARCHAR) AS v, count(*) AS n, count(*) OVER () AS k "
            f"FROM read_parquet({_sql_literal(self.url)}) WHERE {_q(column)} IS NOT NULL "
            f"GROUP BY 1 ORDER BY n DESC, v LIMIT {int(limit) + 1}")
        k = int(rows[0][2]) if rows else 0
        return [(r[0], int(r[1])) for r in rows], k

    def quantiles(self, column: str, n: int, nodata=None) -> Tuple[List[float], float, float]:
        x = _sql_value(column)
        probs = [i / n for i in range(1, n)]
        where = f" WHERE {x} IS DISTINCT FROM {float(nodata)}" if nodata is not None else ""
        sel = ", ".join(f"quantile_cont({x}, {p})" for p in probs)
        r = self.rows(f"SELECT {sel}, min({x}), max({x}) FROM read_parquet({_sql_literal(self.url)}){where}")[0]
        if r[-2] is None:
            return [], float("nan"), float("nan")
        return [float(v) for v in r[:-2]], float(r[-2]), float(r[-1])


# ------------------------------------------------------------------ classes

def code_labels(col: dict, codes: Sequence = ()) -> Dict[str, str]:
    """``{code: definition}`` from a ``Values: A=Alpha, B=Beta.`` description."""
    desc = col.get("description") or ""
    pos = []
    for v in dict.fromkeys([str(x) for x in (col.get("values") or [])] + [str(x) for x in codes]):
        m = re.search(r"(?:^|[:;,]\s*|\(\s*)" + re.escape(v) + r"\s*=\s*", desc)
        if m:
            pos.append((m.end(), m.start(), v))
    pos.sort()
    out = {}
    for i, (end, _, v) in enumerate(pos):
        stop = pos[i + 1][1] if i + 1 < len(pos) else len(desc)
        text = re.split(r"\.\s+(?=[A-Z])", desc[end:stop])[0]
        out[v] = text.strip().rstrip(".;,").strip()
    return out


def nice_breaks(qs: Sequence[float], lo: float, hi: float) -> List[float]:
    """Round quantile breaks to the fewest significant figures that keeps them distinct."""
    raw = sorted({x for x in qs if lo < x < hi})
    for sig in range(1, 7):
        out = sorted({float(f"{x:.{sig}g}") for x in raw})
        out = [x for x in out if lo < x < hi]
        if len(out) == len(raw):
            return out
    return raw


def fmt(x: float) -> str:
    if abs(x) >= 10000:
        return f"{x:,.0f}"
    return f"{x:g}"


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def style_value(column: str, string: bool = False) -> Tuple[list, list]:
    """(numeric MapLibre expression, guard that it is defined) for a column or ``A/B``.

    A string column is coerced with a sentinel, so values that are not numbers
    fall to no data.
    """
    if string and "/" not in column:
        num = ["to-number", ["get", column], -1e30]
        return num, [">", num, -1e30]
    if "/" in column:
        a, b = column.split("/", 1)
        return (["/", ["to-number", ["get", a], 0], ["to-number", ["get", b], 1]],
                ["all", ["==", ["typeof", ["get", a]], "number"],
                 [">", ["to-number", ["get", b], 0], 0]])
    return ["to-number", ["get", column]], ["==", ["typeof", ["get", column]], "number"]


def paint_for(t: str, color) -> Tuple[dict, Optional[dict]]:
    if t == "fill":
        return ({"fill-color": color, "fill-opacity": 0.7},
                {"line-color": "#4d4d4d", "line-width": 0.4, "line-opacity": 0.6})
    if t == "line":
        return ({"line-color": color, "line-width": 1.5}, None)
    return ({"circle-color": color, "circle-radius": 4, "circle-stroke-color": "#ffffff",
             "circle-stroke-width": 0.6}, None)


def maplibre_style(t: str, paint: dict, outline: Optional[dict], name: str, pmtiles_href: str,
                   source_layer: str, description: str) -> dict:
    layers = [{"id": f"{source_layer}-{t}", "type": t, "source": "data",
               "source-layer": source_layer, "paint": paint}]
    if t == "fill" and outline:
        layers.append({"id": f"{source_layer}-outline", "type": "line", "source": "data",
                       "source-layer": source_layer, "paint": outline})
    return {"version": 8, "name": name,
            "sources": {"data": {"type": "vector", "url": f"pmtiles://{pmtiles_href}"}},
            "layers": layers, "metadata": {"description": description}}


def relative_href(pmtiles: str, output: Optional[str]) -> str:
    """The PMTiles file as the style should name it: relative to the style when
    both are in one bucket (or on one disk), else as given."""
    if not output:
        return pmtiles

    def split(u):
        if u.startswith("s3://"):
            bucket, _, key = u[5:].partition("/")
            return ("s3", bucket), key
        m = re.match(r"https?://[^/]+/([^/]+)/(.*)$", u)
        if m:
            return ("s3", m.group(1)), m.group(2)
        return ("local", ""), u

    (ps, pk), (os_, ok) = split(pmtiles), split(output)
    if ps != os_:
        return pmtiles
    return posixpath.relpath(pk, posixpath.dirname(ok) or ".")


# ------------------------------------------------------------------ profile

def profile(pmtiles: str, collection: Optional[dict] = None) -> dict:
    """Each layer's drawable attributes, for choosing the column a style shows."""
    cols = collection_columns(collection)
    out = {"pmtiles": pmtiles, "layers": []}
    for layer, st in layer_stats(pmtiles).items():
        attrs = []
        for at in st.get("attributes", []):
            name = at["attribute"]
            if SKIP.match(name):
                continue
            entry = {"name": name, "type": at.get("type"), "distinct": at.get("count")}
            if at.get("type") == "number" and at.get("min") is not None:
                entry["range"] = [at["min"], at["max"]]
            else:
                entry["sample"] = [str(v) for v in (at.get("values") or [])[:8]]
            col = cols.get(name) or {}
            if col.get("description"):
                entry["description"] = re.sub(r"\s+", " ", col["description"])
            if col.get("values"):
                entry["values"] = col["values"]
            attrs.append(entry)
        out["layers"].append({
            "layer": layer, "geometry": st.get("geometry"), "features": st.get("count"),
            "attributes_listed": len(st.get("attributes", [])),
            "attributes_total": st.get("attributeCount"), "attributes": attrs})
    return out


# ------------------------------------------------------------------ build

def build_style(pmtiles: str, *, parquet: Optional[str] = None, column: Optional[str] = None,
                kind: Optional[str] = None, single_colour: Optional[str] = None,
                layer: Optional[str] = None, collection: Optional[dict] = None,
                asset_key: Optional[str] = None, output: Optional[str] = None,
                label: Optional[str] = None, why: Optional[str] = None,
                palette: Optional[str] = None, reverse: bool = False,
                colors: Optional[Dict[str, str]] = None, labels: Optional[Dict[str, str]] = None,
                breaks: Optional[List[float]] = None, unit: Optional[str] = None,
                nodata: Optional[List] = None, color: Optional[str] = None,
                max_classes: int = 10, tooltip: Optional[List[str]] = None,
                title: Optional[str] = None) -> Tuple[dict, List[str]]:
    """Build a default style for one PMTiles file.

    Args:
        pmtiles: The PMTiles file (local, http(s):// or s3://).
        parquet: The GeoParquet with the same features. Classes and breaks are
            measured in it; without it they come from the PMTiles tilestats,
            which hold a sample of values.
        column: The column to show, or ``A/B`` for a ratio of two columns.
        kind: ``categorical`` or ``continuous``.
        single_colour: Instead of a column, one colour, for the reason given.
        layer: The PMTiles layer (default: the first).
        collection: The STAC collection, whose ``table:columns`` give labels.
        asset_key: The PMTiles asset in ``collection``, for its own columns.
        output: Where the style will be written, so the PMTiles href is
            relative to it.
        title: The data's name for the style title (default: the layer).

    Returns:
        ``(style, report lines)``.
    """
    if not single_colour and not (column and kind in ("categorical", "continuous")):
        raise ValueError("A style needs a column and a kind (categorical or continuous), "
                         "or a single colour with its reason")
    if palette and palette not in SEQUENTIAL:
        raise ValueError(f"Unknown palette {palette!r}; one of {sorted(SEQUENTIAL)}")
    stats = layer_stats(pmtiles)
    if not stats:
        raise ValueError(f"{pmtiles}: no tilestats layers in the PMTiles metadata")
    layer = layer if layer in stats else next(iter(stats))
    st = stats[layer]
    t = GEOM.get((st.get("geometry") or "").lower())
    if not t:
        raise ValueError(f"{pmtiles}: unknown geometry {st.get('geometry')!r}")
    attrs = {x["attribute"]: x for x in st.get("attributes", [])}
    cols = collection_columns(collection, asset_key)
    name = title or layer
    report: List[str] = []
    legend = None
    tooltip = list(tooltip or [])
    pq = _Parquet(parquet) if parquet else None
    nodata = [str(x) for x in (nodata or [])]

    if single_colour:
        fill = color or SINGLE[t]
        style_title, desc = name, single_colour.strip()
    else:
        truncated = (st.get("attributeCount") or 0) > len(attrs)
        for c in column.split("/"):
            if c not in attrs and not (truncated and c in cols):
                raise ValueError(f"`{c}` is not an attribute of the PMTiles layer `{layer}` "
                                 f"(has: {', '.join(attrs)})")
        label = label or column
        if kind == "categorical":
            colors = colors or {}
            limit = 500 if colors else max_classes + len(nodata)
            if pq:
                classes, k = pq.categories(column, limit)
            else:
                at = attrs[column]
                vals = [str(v) for v in at.get("values") or []]
                classes, k = [(v, None) for v in sorted(vals)[:limit + 1]], at.get("count", len(vals))
            k -= sum(1 for v, _ in classes if v in nodata)
            classes = [(v, n) for v, n in classes if v not in nodata]
            names = code_labels(cols.get(column) or {}, [v for v, _ in classes])
            names.update(labels or {})
            shown = classes[:max_classes] if k > max_classes else classes
            if colors:  # an explicit palette names the classes to show, in legend order
                present = dict(classes)
                shown = [(v, present[v]) for v in colors if v in present]
            if not colors and all(re.fullmatch(r"-?\d+(\.\d+)?", v) for v, _ in shown):
                shown = sorted(shown, key=lambda vn: float(vn[0]))  # codes read in order
            if palette and not colors:  # ordered classes: spread a sequential ramp
                ramp = SEQUENTIAL[palette][::-1 if reverse else 1]
                k1 = max(1, len(shown) - 1)
                pal = iter(ramp[round(i * (len(ramp) - 1) / k1)] for i in range(len(shown)))
            else:
                pal = iter(CATEGORICAL)
            need = sum(1 for v, _ in shown if v not in colors)
            if not (palette and not colors) and need > len(CATEGORICAL):
                raise ValueError(f"{need} classes but the categorical palette has "
                                 f"{len(CATEGORICAL)} colours; give colors, or lower max_classes")
            pairs = [(v, colors.get(v) or next(pal)) for v, _ in shown]
            other = k > len(pairs)
            expr = ["match", ["to-string", ["get", column]]]
            for v, c in pairs:
                expr += [v, c]
            expr.append(OTHER if other else NO_DATA)
            fill = ["case", ["!", ["has", column]], NO_DATA] + \
                sum(([["==", ["to-string", ["get", column]], x], NO_DATA] for x in nodata), []) + [expr]
            legend = {"type": "categorical", "label": label,
                      "classes": [{"label": names.get(v, v), "color": c} for v, c in pairs]
                      + ([{"label": "Other", "color": OTHER}] if other else [])}
            desc = (f"Features coloured by {label}" +
                    (f", the {len(pairs)} most common of {k} values, the rest grey" if other
                     else f", {k} values") + ".")
            report.append(f"categorical `{column}` {len(pairs)}/{k} classes "
                          f"({'GeoParquet' if pq else 'tilestats'}): "
                          + ", ".join(f"{v}={n}" for v, n in shown))
        else:
            ramp = list(SEQUENTIAL[palette or "viridis"])
            if reverse:
                ramp.reverse()
            n = len(ramp)
            if breaks:
                bks = [float(x) for x in breaks]
                lo = hi = None
                src = "given"
            else:
                nd = float(nodata[0]) if nodata else None
                if pq:
                    qs, lo, hi = pq.quantiles(column, n, nd)
                    src = "GeoParquet"
                else:
                    at = attrs[column]
                    vs = sorted(float(v) for v in at.get("values") or []
                                if isinstance(v, (int, float)) and v != nd)
                    probs = [i / n for i in range(1, n)]
                    qs = [vs[min(len(vs) - 1, int(p * len(vs)))] for p in probs] if vs else []
                    lo, hi = float(at.get("min", "nan")), float(at.get("max", "nan"))
                    src = "tilestats"
                bks = nice_breaks(qs, lo, hi)
            if not bks:
                raise ValueError(f"`{column}` has no spread to class ({lo}..{hi})")
            k = len(bks) + 1
            ramp = [ramp[round(i * (n - 1) / (k - 1))] for i in range(k)]
            num, guard = style_value(column, attrs.get(column, {}).get("type") == "string")
            step = ["step", num, ramp[0]]
            for b, c in zip(bks, ramp[1:]):
                step += [b, c]
            if nodata:
                guard = ["all", guard] + [["!=", num, float(x)] for x in nodata]
            fill = ["case", guard, step, NO_DATA]
            u = f" {unit}" if unit else ""
            labs = [f"under {fmt(bks[0])}{u}"] + \
                [f"{fmt(x)} – {fmt(y)}{u}" for x, y in zip(bks, bks[1:])] + \
                [f"{fmt(bks[-1])}{u} and over"]
            legend = {"type": "categorical", "label": label,
                      "classes": [{"label": lab, "color": c} for lab, c in zip(labs, ramp)]
                      + [{"label": "No data", "color": NO_DATA}]}
            desc = f"Features shaded by {label}, in {len(ramp)} classes at quantile breaks of the data."
            report.append(f"continuous `{column}` range {lo}..{hi}, breaks {bks} ({src})")
        style_title = f"{name} by {label}"
        for c in reversed(column.split("/")):
            if c not in tooltip:
                tooltip.insert(0, c)
    if why:
        desc = f"{desc} {why.strip()}"

    paint, outline = paint_for(t, fill)
    sty = maplibre_style(t, paint, outline, style_title, relative_href(pmtiles, output), layer, desc)
    if legend:
        sty["metadata"]["glen:legend"] = legend
    if single_colour:
        sty["metadata"]["glen:single_colour_reason"] = desc
    if tooltip:
        sty["metadata"]["glen:tooltip_fields"] = tooltip
    return sty, report


def style_filename(stem: str, column: Optional[str]) -> str:
    """``<stem>-<column>.json``, or ``<stem>.json`` for a single colour, as data-workflows names it."""
    return f"{stem}-{slug(column.replace('/', '-per-'))}.json" if column else f"{stem}.json"
