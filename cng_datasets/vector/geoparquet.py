"""GeoParquet 1.1 output with a bounding-box covering column (issue #340).

Portolan, the specification our catalog is moving to, requires GeoParquet 1.1
or 2.x and spatial statistics for every row group (PORTO-FMT-007/012). DuckDB
2.0 writes either GeoParquet 1.0.0, whose WKB column carries no spatial
statistics, or the native Parquet ``GEOMETRY`` type of 2.x. The native type
passes, but it changes the column's logical type for every downstream reader,
including the DuckDB-WASM builds in the browser apps, and DuckDB reads it back
without its CRS84 label.

So convert keeps plain WKB and adds a ``bbox`` struct column, the 1.1
"covering": its ``xmin``/``ymin``/``xmax``/``ymax`` fields get ordinary Parquet
min/max statistics in every row group, which is what a reader prunes on.
DuckDB has no option to declare a covering, so the ``geo`` key is written here
and handed to ``COPY`` through ``KV_METADATA``, which replaces DuckDB's own.

The ``geo`` key written here has no file ``bbox`` and an empty
``geometry_types`` list, which the specification reads as "unknown": both
would need a pass over the data before the write. The row-group statistics on
the covering column give a reader the extent instead.

The covering column belongs to the GeoParquet file only. The hex and PMTiles
steps read the converted file and copy its attributes, so they drop it with
:func:`covering_columns`; otherwise every hex row and every tile feature would
carry a struct of four doubles.
"""

import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

GEOPARQUET_VERSION = "1.1.0"
BBOX_FIELDS = ("xmin", "ymin", "xmax", "ymax")
BBOX_COLUMN = "bbox"

# The same preference as h3_tiling.find_geometry_column, so convert's primary
# column is the one the hex step reads.
_PREFERRED_GEOMETRY = ("GEOM", "GEOMETRY", "SHAPE")
_CRS_IN_TYPE = re.compile(r"^GEOMETRY\s*\(\s*'(.*)'\s*\)$", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class Covering:
    """How to add a covering column to a query, and the ``geo`` key that declares it."""

    primary: str
    column: str
    select: str
    geo: Dict


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def geometry_columns(schema: Sequence[Tuple[str, str]]) -> List[str]:
    """Names of the GEOMETRY-typed columns in a DuckDB ``DESCRIBE`` result."""
    return [str(row[0]) for row in schema if str(row[1]).upper().startswith("GEOMETRY")]


def primary_geometry(geom_cols: Sequence[str]) -> str:
    by_upper = {c.upper(): c for c in geom_cols}
    for name in _PREFERRED_GEOMETRY:
        if name in by_upper:
            return by_upper[name]
    return geom_cols[0]


def crs_projjson(column_type: str) -> Optional[Dict]:
    """PROJJSON for the CRS in a DuckDB geometry type, or None for OGC:CRS84.

    DuckDB carries a geometry's CRS in its type, as ``GEOMETRY('OGC:CRS84')``.
    GeoParquet reads a missing ``crs`` as OGC:CRS84, which is also what an
    unlabelled column has always been written as, so both get no ``crs``.
    EPSG:4326 is written as CRS84 too: convert's coordinates are (lon, lat)
    throughout (#280), which is CRS84's axis order and not EPSG:4326's.
    """
    match = _CRS_IN_TYPE.match(str(column_type).strip())
    if not match:
        return None
    label = match.group(1).replace("''", "'")
    from osgeo import osr
    srs = osr.SpatialReference()
    # An unreadable label: claiming CRS84 would be a guess, and a wrong one
    # moves every coordinate. Fail the write instead. osr raises or returns an
    # error code depending on whether GDAL exceptions are on.
    try:
        failed = srs.SetFromUserInput(label) != 0
    except RuntimeError:
        failed = True
    if failed:
        raise ValueError(f"Cannot write GeoParquet CRS for geometry type {column_type!r}")
    wgs84 = osr.SpatialReference()
    wgs84.SetFromUserInput("OGC:CRS84")
    if srs.IsSame(wgs84, ["CRITERION=EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS"]):
        return None
    return json.loads(srs.ExportToPROJJSON())


def covering_column_name(columns: Sequence[str], primary: str) -> str:
    """``bbox``, or ``<geometry>_bbox`` when the data already has a ``bbox``."""
    taken = {c.lower() for c in columns}
    name = BBOX_COLUMN if BBOX_COLUMN not in taken else f"{primary}_{BBOX_COLUMN}"
    n = 1
    base = name
    while name.lower() in taken:
        n += 1
        name = f"{base}_{n}"
    return name


def plan_covering(schema: Sequence[Tuple[str, str]]) -> Optional[Covering]:
    """The covering column and ``geo`` key for a query, or None if it has no geometry.

    Args:
        schema: ``(name, type)`` rows, as from ``DESCRIBE (<query>)``
    """
    geom_cols = geometry_columns(schema)
    if not geom_cols:
        return None
    types = {str(row[0]): str(row[1]) for row in schema}
    primary = primary_geometry(geom_cols)
    column = covering_column_name([str(row[0]) for row in schema], primary)
    g = _quote(primary)
    select =(f"struct_pack(xmin := ST_XMin({g}), ymin := ST_YMin({g}), "
              f"xmax := ST_XMax({g}), ymax := ST_YMax({g}))")
    columns = {}
    for name in geom_cols:
        meta: Dict = {"encoding": "WKB", "geometry_types": []}
        crs = crs_projjson(types[name])
        if crs is not None:
            meta["crs"] = crs
        if name == primary:
            meta["covering"] = {BBOX_COLUMN: {f: [column, f] for f in BBOX_FIELDS}}
        columns[name] = meta
    geo = {"version": GEOPARQUET_VERSION, "primary_column": primary, "columns": columns}
    return Covering(primary=primary, column=column, select=select, geo=geo)


def covering_columns(con, parquet_url: str) -> List[str]:
    """Covering columns a GeoParquet declares, which copies of its attributes drop.

    Returns an empty list for a file with no ``geo`` key or no covering,
    such as one converted before #340.
    """
    try:
        url = parquet_url.replace("'", "''")
        rows = con.execute(
            f"SELECT value FROM parquet_kv_metadata('{url}') WHERE decode(key) = 'geo' LIMIT 1"
        ).fetchall()
    except Exception:
        return []
    if not rows:
        return []
    value = rows[0][0]
    try:
        geo = json.loads(value.decode() if isinstance(value, (bytes, bytearray)) else value)
    except (ValueError, AttributeError):
        return []
    names = []
    for meta in (geo.get("columns") or {}).values():
        for spec in ((meta or {}).get("covering") or {}).values():
            for path in (spec or {}).values():
                if isinstance(path, list) and path and path[0] not in names:
                    names.append(path[0])
    return names
