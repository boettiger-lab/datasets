"""Tests for reading tile-accurate field metadata from a PMTiles footer (#140)."""

import gzip
import json
import struct
import tempfile
import os

import pytest

from cng_datasets.vector.pmtiles import (
    read_pmtiles_metadata,
    pmtiles_vector_layers,
    pmtiles_table_columns,
)


def _make_pmtiles(metadata: dict, compression: int = 2, version: int = 3,
                  magic: bytes = b"PMTiles") -> bytes:
    """Build a minimal PMTiles v3 archive: 127-byte header + metadata blob.

    compression: 1=none, 2=gzip (matches the internal-compression byte at 97).
    """
    raw = json.dumps(metadata).encode("utf-8")
    blob = gzip.compress(raw) if compression == 2 else raw
    header = bytearray(127)
    header[0:7] = magic
    header[7] = version
    struct.pack_into("<Q", header, 24, 127)          # metadata offset
    struct.pack_into("<Q", header, 32, len(blob))    # metadata length
    header[97] = compression
    return bytes(header) + blob


def _write_tmp(data: bytes) -> str:
    with tempfile.NamedTemporaryFile(suffix=".pmtiles", delete=False) as f:
        f.write(data)
        return f.name


_SVI_META = {
    "vector_layers": [
        {"id": "svi", "fields": {
            "COUNTY": "String", "FIPS": "String",
            "RPL_THEMES": "Number", "FLAG": "Boolean",
        }}
    ]
}


class TestReadPmtilesMetadata:
    def test_gzip_roundtrip(self):
        path = _write_tmp(_make_pmtiles(_SVI_META, compression=2))
        try:
            assert read_pmtiles_metadata(path) == _SVI_META
        finally:
            os.unlink(path)

    def test_uncompressed_metadata(self):
        path = _write_tmp(_make_pmtiles(_SVI_META, compression=1))
        try:
            assert read_pmtiles_metadata(path) == _SVI_META
        finally:
            os.unlink(path)

    def test_bad_magic_raises(self):
        path = _write_tmp(_make_pmtiles(_SVI_META, magic=b"NOTPMT!"))
        try:
            with pytest.raises(ValueError, match="not a PMTiles archive"):
                read_pmtiles_metadata(path)
        finally:
            os.unlink(path)

    def test_unsupported_version_raises(self):
        path = _write_tmp(_make_pmtiles(_SVI_META, version=2))
        try:
            with pytest.raises(ValueError, match="only v3 is supported"):
                read_pmtiles_metadata(path)
        finally:
            os.unlink(path)

    def test_unsupported_compression_raises(self):
        path = _write_tmp(_make_pmtiles(_SVI_META, compression=3))  # brotli
        try:
            with pytest.raises(ValueError, match="only none.*and gzip"):
                read_pmtiles_metadata(path)
        finally:
            os.unlink(path)


class TestPmtilesTableColumns:
    def test_type_mapping(self):
        path = _write_tmp(_make_pmtiles(_SVI_META))
        try:
            cols = pmtiles_table_columns(path)
            assert cols == [
                {"name": "COUNTY", "type": "string"},
                {"name": "FIPS", "type": "string"},
                {"name": "RPL_THEMES", "type": "number"},
                {"name": "FLAG", "type": "boolean"},
            ]
        finally:
            os.unlink(path)

    def test_unknown_type_falls_back_to_string(self):
        meta = {"vector_layers": [{"id": "L", "fields": {"weird": "SomethingElse"}}]}
        path = _write_tmp(_make_pmtiles(meta))
        try:
            assert pmtiles_table_columns(path) == [{"name": "weird", "type": "string"}]
        finally:
            os.unlink(path)

    def test_multiple_layers_merged_and_deduped(self):
        meta = {"vector_layers": [
            {"id": "a", "fields": {"shared": "String", "only_a": "Number"}},
            {"id": "b", "fields": {"shared": "Number", "only_b": "String"}},  # shared: first wins
        ]}
        path = _write_tmp(_make_pmtiles(meta))
        try:
            cols = pmtiles_table_columns(path)
            names = [c["name"] for c in cols]
            assert names == ["shared", "only_a", "only_b"]
            # first occurrence (layer a, String) wins for the shared field
            assert next(c for c in cols if c["name"] == "shared")["type"] == "string"
        finally:
            os.unlink(path)

    def test_layer_filter(self):
        meta = {"vector_layers": [
            {"id": "a", "fields": {"fa": "String"}},
            {"id": "b", "fields": {"fb": "Number"}},
        ]}
        path = _write_tmp(_make_pmtiles(meta))
        try:
            assert pmtiles_table_columns(path, layer="b") == [{"name": "fb", "type": "number"}]
        finally:
            os.unlink(path)

    def test_raster_pmtiles_has_no_vector_layers(self):
        path = _write_tmp(_make_pmtiles({"type": "raster"}))
        try:
            assert pmtiles_vector_layers(path) == []
            assert pmtiles_table_columns(path) == []
        finally:
            os.unlink(path)
