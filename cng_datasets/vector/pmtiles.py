"""
Read tile-accurate field metadata from a PMTiles archive footer.

tippecanoe writes only a *subset* of the source columns into the vector tiles
(after any ``-y``/``-x`` includes, drop-densest, and attribute coercion), so the
PMTiles schema differs from the source GeoParquet schema. The authoritative tile
schema is ``vector_layers[].fields`` in the PMTiles metadata blob. This module
reads that blob from the archive footer — locally or over an HTTP/S3 byte range,
without downloading the whole tileset — and maps the tippecanoe field types to
STAC ``table:columns`` entries, so a STAC collection can advertise the fields
that are actually present in the tiles (issue #140).

The header layout is the PMTiles v3 spec: a fixed 127-byte header with the
metadata offset (uint64 LE at byte 24) and length (uint64 LE at byte 32), and the
internal-compression code at byte 97 (1 = none, 2 = gzip — what tippecanoe uses).
"""

import gzip
import json
import struct
from typing import Dict, List, Optional
from urllib.request import Request, urlopen


_PMTILES_HEADER_LEN = 127
_METADATA_OFFSET_POS = 24   # uint64 LE
_METADATA_LENGTH_POS = 32   # uint64 LE
_INTERNAL_COMPRESSION_POS = 97  # 1=none, 2=gzip, 3=brotli, 4=zstd

# tippecanoe emits these three field types in vector_layers[].fields; map each to
# the STAC table:columns / frictionless type vocabulary.
_TIPPECANOE_TYPE_TO_STAC = {
    "String": "string",
    "Number": "number",
    "Boolean": "boolean",
}


def _is_remote(source: str) -> bool:
    return source.startswith(("http://", "https://", "s3://"))


def _to_https(source: str) -> str:
    """Rewrite an s3:// NRP path to its public HTTPS endpoint (byte-range capable)."""
    if source.startswith("s3://"):
        return f"https://s3-west.nrp-nautilus.io/{source[len('s3://'):]}"
    return source


def _read_range(source: str, start: int, length: int) -> bytes:
    """Read ``length`` bytes starting at ``start`` from a local path or remote URL.

    Remote reads use an HTTP Range request so only the header and metadata blob
    are fetched, never the (potentially multi-GB) tile body.
    """
    if _is_remote(source):
        url = _to_https(source)
        req = Request(url, headers={"Range": f"bytes={start}-{start + length - 1}"})
        with urlopen(req) as resp:
            return resp.read()
    with open(source, "rb") as f:
        f.seek(start)
        return f.read(length)


def read_pmtiles_metadata(source: str) -> dict:
    """Read and decode the PMTiles metadata JSON from ``source``'s footer.

    Args:
        source: Local path, ``http(s)://`` URL, or ``s3://`` NRP path to a
            ``.pmtiles`` archive.

    Returns:
        The parsed metadata object (includes ``vector_layers`` for vector tiles).

    Raises:
        ValueError: not a PMTiles v3 archive, or an internal compression this
            reader does not support (only none/gzip — what tippecanoe writes).
    """
    header = _read_range(source, 0, _PMTILES_HEADER_LEN)
    if header[:7] != b"PMTiles":
        raise ValueError(f"{source!r} is not a PMTiles archive (bad magic).")
    version = header[7]
    if version != 3:
        raise ValueError(f"{source!r} is PMTiles v{version}; only v3 is supported.")

    metadata_offset = struct.unpack_from("<Q", header, _METADATA_OFFSET_POS)[0]
    metadata_length = struct.unpack_from("<Q", header, _METADATA_LENGTH_POS)[0]
    internal_compression = header[_INTERNAL_COMPRESSION_POS]

    blob = _read_range(source, metadata_offset, metadata_length)
    if internal_compression == 2:      # gzip (tippecanoe default)
        blob = gzip.decompress(blob)
    elif internal_compression == 1:    # none
        pass
    else:
        raise ValueError(
            f"{source!r} uses internal compression code {internal_compression} "
            f"(brotli/zstd); only none(1) and gzip(2) are supported."
        )
    return json.loads(blob.decode("utf-8"))


def pmtiles_vector_layers(source: str) -> List[dict]:
    """Return the ``vector_layers`` array from a PMTiles archive's metadata.

    Empty list for a non-vector (raster) archive.
    """
    meta = read_pmtiles_metadata(source)
    return meta.get("vector_layers", []) or []


def pmtiles_table_columns(source: str, layer: Optional[str] = None) -> List[Dict[str, str]]:
    """Extract tile-accurate STAC ``table:columns`` entries from a PMTiles footer.

    The tile fields (``vector_layers[].fields``) are the authoritative list of
    what a consumer can style/filter in the tiles — a subset of the source
    schema (issue #140). tippecanoe field types (String/Number/Boolean) are
    mapped to STAC ``table:columns`` types (string/number/boolean); an unknown
    type falls back to ``string``.

    Args:
        source: Local path / ``http(s)://`` / ``s3://`` to a ``.pmtiles`` archive.
        layer: Restrict to a single vector layer id; default merges all layers.

    Returns:
        A list of ``{"name": <field>, "type": <stac-type>}`` dicts, de-duplicated
        by field name (first occurrence wins), in first-seen order — ready to drop
        into a STAC collection's ``table:columns``.
    """
    columns: Dict[str, str] = {}
    for lyr in pmtiles_vector_layers(source):
        if layer is not None and lyr.get("id") != layer:
            continue
        for name, ttype in (lyr.get("fields") or {}).items():
            if name not in columns:
                columns[name] = _TIPPECANOE_TYPE_TO_STAC.get(ttype, "string")
    return [{"name": name, "type": stac_type} for name, stac_type in columns.items()]
