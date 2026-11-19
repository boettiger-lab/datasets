"""Portolan conformance of the files cng-datasets writes (issue #340).

Builds a small vector dataset (GeoParquet and hex) and a COG with the same code
the workflows run, wraps them in the smallest catalog rashid will read, and
runs rashid's metadata and data passes over it. The test fails on any error or
warning from the rules about the files themselves: the data pass (PTL-DAT-*),
sizes and checksums (PTL-AST-003/004), and partitions (PTL-PRT-*). Rules about
what the STAC says, such as licence, providers or descriptions, belong to the
STAC author in data-workflows and are not asserted here.

rashid needs rasterio, pyproj and shapely, which the image builds against its
one system GDAL (#285); installing rashid beside them could replace them with
wheels. So it runs from its own virtualenv, named by ``RASHID``. CI installs
it there; without it the test is skipped:

    uv venv /opt/rashid && uv pip install --python /opt/rashid/bin/python rashid==0.4.0
    RASHID=/opt/rashid/bin/rashid pytest tests/test_portolan_conformance.py
"""

import json
import os
import shutil
import subprocess

import pytest

RASHID = os.environ.get("RASHID") or shutil.which("rashid")
pytestmark = pytest.mark.skipif(not RASHID, reason="rashid not installed (set RASHID)")

# The rule families that judge the bytes cng-datasets writes.
FILE_RULES = ("PTL-DAT-", "PTL-AST-003", "PTL-AST-004", "PTL-PRT-")

# Findings in those families that are known and accepted, with the reason.
# Keep this short: each entry is a rule the tool does not yet meet.
ACCEPTED = {
    # The fixture's glob is relative, so the data pass matches it on disk and
    # reads every hex file. A published collection uses s3://, which this
    # rule asks for; listing a bucket is not something a unit test can do.
    "PTL-PRT-002": "relative partition:glob in the local fixture",
}

FILE_EXT = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
PARTITION_EXT = "https://schemas.portolan-sdi.org/incubating/partition/v1.0.0/schema.json"
PARQUET = "application/vnd.apache.parquet"
COG = "image/tiff; application=geotiff; profile=cloud-optimized"


def _features(n=3000):
    """Squares over California, in a GeoJSON FeatureCollection, unsorted."""
    feats = []
    for i in range(n):
        x = -124 + ((i * 37) % 100) * 0.05
        y = 33 + ((i * 53) % 60) * 0.05
        ring = [[x, y], [x + 0.03, y], [x + 0.03, y + 0.03], [x, y + 0.03], [x, y]]
        feats.append({"type": "Feature", "properties": {"name": f"f{i}", "value": i % 17},
                      "geometry": {"type": "Polygon", "coordinates": [ring]}})
    return {"type": "FeatureCollection", "features": feats}


def _build_vector(root):
    from cng_datasets.vector.convert_to_parquet import convert_to_parquet
    from cng_datasets.vector.h3_tiling import H3VectorProcessor
    from cng_datasets.vector.repartition import repartition_by_h0

    src = root / "src.geojson"
    src.write_text(json.dumps(_features()))
    coll = root / "cat" / "vec"
    coll.mkdir(parents=True)
    parquet = coll / "vec.parquet"
    convert_to_parquet(str(src), str(parquet), row_group_size=500,
                       verbose=False, progress=False)

    chunks = root / "chunks"
    chunks.mkdir()
    p = H3VectorProcessor(input_url=str(parquet), output_url=str(chunks),
                          h3_resolution=6, parent_resolutions=[0], chunk_size=5000)
    p.process_chunk(0)
    p.con.close()
    repartition_by_h0(chunks_dir=str(chunks), output_dir=str(coll / "vec" / "hex"),
                      source_parquet=str(parquet), cleanup=False)
    return coll


def _build_raster(root):
    import numpy as np
    from osgeo import gdal, osr
    from cng_datasets.raster import create_cog

    coll = root / "cat" / "ras"
    coll.mkdir(parents=True)
    src = str(root / "src.tif")
    rng = np.random.default_rng(3)
    data = rng.uniform(0, 100, (600, 600)).astype(np.float32)
    data[:100, :] = -9999.0
    ds = gdal.GetDriverByName("GTiff").Create(src, 600, 600, 1, gdal.GDT_Float32)
    ds.SetGeoTransform([-121.0, 0.001, 0, 38.0, 0, -0.001])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    band.SetNoDataValue(-9999.0)
    band.WriteArray(data)
    ds = None
    create_cog(input_path=src, output_path=str(coll / "ras-cog.tif"), nodata_value=-9999.0)
    return coll


def _files(coll, targets):
    """file:size and file:checksum for each target, from the checksums step."""
    from cng_datasets.storage.checksums import write_checksums
    return write_checksums([str(coll / t) for t in targets], str(coll / "checksums.json"))


def _collection(cid, bbox, assets, **extra):
    return {
        "type": "Collection", "stac_version": "1.1.0", "id": cid,
        "stac_extensions": [FILE_EXT] + extra.pop("extensions", []),
        "title": cid, "description": f"{cid}: a conformance fixture.", "license": "CC-BY-4.0",
        "extent": {"spatial": {"bbox": [bbox]}, "temporal": {"interval": [[None, None]]}},
        "links": [{"rel": "root", "href": "../catalog.json", "type": "application/json"},
                  {"rel": "parent", "href": "../catalog.json", "type": "application/json"}],
        "assets": assets, **extra,
    }


def _table_columns(path):
    """name and type of each column, as a STAC author writes them from the file."""
    import pyarrow.parquet as pq
    # Descriptions are the STAC author's (PTL-DAT-020); the fixture has stand-ins.
    return [{"name": f.name, "type": str(f.type), "description": f"The {f.name} column."}
            for f in pq.read_schema(str(path))]


def _asset(href, media, title, entry):
    return {"href": href, "type": media, "roles": ["data"], "title": title,
            "description": title, "file:size": entry["file:size"],
            "file:checksum": entry["file:checksum"]}


@pytest.fixture(scope="module")
def catalog(tmp_path_factory):
    root = tmp_path_factory.mktemp("portolan")
    vec = _build_vector(root)
    ras = _build_raster(root)

    doc = _files(vec, ["vec.parquet"])
    flat = doc["files"][str(vec / "vec.parquet")]
    # The fixture reaches the hex files through partition:glob alone, which is
    # how rashid's data pass reads partitions. The published catalog also keeps
    # a hex asset that consumers look up by key; rashid reports its glob href
    # (PTL-AST-012), which is a catalog decision open on #340, not a file defect.
    hex_file = next((vec / "vec" / "hex").glob("h0=*/data_0.parquet"))
    vec_json = _collection(
        "vec", [-124.0, 33.0, -119.0, 36.0],
        {"vec-parquet": _asset("./vec.parquet", PARQUET, "GeoParquet", flat)},
        extensions=[PARTITION_EXT, "https://stac-extensions.github.io/table/v1.2.0/schema.json"],
        **{"partition:scheme": "hive", "partition:strategy": "h3",
           "partition:keys": [{"name": "h0", "type": "int64"}],
           "partition:glob": "./vec/hex/h0=*/data_0.parquet",
           "table:columns": _table_columns(vec / "vec.parquet") + [
               c for c in _table_columns(hex_file) if c["name"].startswith("h")]})
    (vec / "collection.json").write_text(json.dumps(vec_json, indent=2))

    doc = _files(ras, ["ras-cog.tif"])
    cog = doc["files"][str(ras / "ras-cog.tif")]
    ras_json = _collection("ras", [-121.0, 37.4, -120.4, 38.0],
                           {"ras-cog": _asset("./ras-cog.tif", COG, "COG", cog)})
    (ras / "collection.json").write_text(json.dumps(ras_json, indent=2))

    cat = {"type": "Catalog", "stac_version": "1.1.0", "id": "conformance",
           "description": "cng-datasets conformance fixtures",
           "links": [{"rel": "root", "href": "./catalog.json", "type": "application/json"},
                     {"rel": "child", "href": "./vec/collection.json", "type": "application/json"},
                     {"rel": "child", "href": "./ras/collection.json", "type": "application/json"}]}
    (root / "cat" / "catalog.json").write_text(json.dumps(cat, indent=2))
    return root / "cat"


def _rashid(catalog):
    run = subprocess.run([RASHID, "check", str(catalog), "--data-scope", "local", "--json", "--all"],
                         capture_output=True, text=True, timeout=600)
    try:
        report = json.loads(run.stdout)
    except ValueError:
        pytest.fail(f"rashid printed no JSON (exit {run.returncode}):\n{run.stderr[-2000:]}")
    for f in report["findings"]:  # in the log, for whoever reads a failure
        print(f"{f.get('rule_id')} [{f.get('severity')}] {f.get('message', '')[:200]}")
    return report["findings"]


@pytest.fixture(scope="module")
def findings(catalog):
    return _rashid(catalog)


@pytest.mark.timeout(900)
def test_the_data_pass_ran(findings):
    """A data pass that could not run enforces nothing (PTL-DAT-000)."""
    unavailable = [f for f in findings if f.get("rule_id") == "PTL-DAT-000"]
    assert not unavailable, unavailable


@pytest.mark.timeout(900)
def test_no_errors_or_warnings_on_the_files(findings):
    bad = [f for f in findings
           if (f.get("rule_id") or "").startswith(FILE_RULES)
           and f.get("severity") in ("error", "warning")
           and f.get("rule_id") not in ACCEPTED]
    assert not bad, "\n".join(
        f"{f['rule_id']} [{f['severity']}] {f.get('message')}" for f in bad)


@pytest.mark.timeout(900)
def test_a_broken_file_is_caught(catalog, tmp_path):
    """The check can fail: a wrong checksum on the flat file and an unreadable
    hex file each draw an error. Without this, a data pass that silently read
    nothing, such as a glob that matched no files, would pass."""
    broken = tmp_path / "cat"
    shutil.copytree(catalog, broken)
    coll = json.loads((broken / "vec" / "collection.json").read_text())
    coll["assets"]["vec-parquet"]["file:checksum"] = "1220" + "00" * 32
    (broken / "vec" / "collection.json").write_text(json.dumps(coll))
    next((broken / "vec" / "vec" / "hex").glob("h0=*/data_0.parquet")).write_bytes(b"not parquet")

    found = _rashid(broken)
    assert any(f.get("rule_id") == "PTL-DAT-001" and f.get("severity") == "error"
               for f in found), "the checksum mismatch was not caught"
    # rashid reports a file it cannot read as PTL-DAT-000 on that partition
    # file, which is what shows the glob was matched and each file opened.
    assert any(f.get("rule_id") == "PTL-DAT-000" and "partition file" in (f.get("message") or "")
               for f in found), "the unreadable hex file was not caught"
