"""Default MapLibre styles for a PMTiles file, built from its data (issue #340)."""

import json
import shutil
import subprocess
import sys

import duckdb
import pytest

from cng_datasets.vector.style import (
    CATEGORICAL,
    NO_DATA,
    OTHER,
    SEQUENTIAL,
    build_style,
    code_labels,
    nice_breaks,
    profile,
    relative_href,
    style_filename,
)

pytestmark = pytest.mark.skipif(not shutil.which("tippecanoe"), reason="tippecanoe not installed")


def _features(n=600):
    feats = []
    for i in range(n):
        x, y = -122 + (i % 30) * 0.05, 37 + (i // 30) * 0.05
        ring = [[x, y], [x + 0.04, y], [x + 0.04, y + 0.04], [x, y + 0.04], [x, y]]
        # zone: 14 values of unequal frequency, so four fall to Other.
        zone = "ABCDEFGHIJKLMN"[min(13, int((i % 100) ** 0.5 * 1.4))]
        feats.append({"type": "Feature",
                      "properties": {"name": f"f{i}", "zone": zone, "pop": float(i * i),
                                     "area": float(i % 7 + 1), "code": i % 3 + 1},
                      "geometry": {"type": "Polygon", "coordinates": [ring]}})
    return feats


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    d = tmp_path_factory.mktemp("style")
    gj = d / "parcels.geojson"
    gj.write_text(json.dumps({"type": "FeatureCollection", "features": _features()}))
    pm = d / "parcels.pmtiles"
    subprocess.run(["tippecanoe", "-q", "-o", str(pm), "-l", "parcels", "-z", "6", "--force", str(gj)],
                   check=True)
    pq = d / "parcels.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(f"COPY (SELECT * EXCLUDE (geom) FROM ST_Read('{gj}')) TO '{pq}' (FORMAT PARQUET)")
    return {"pmtiles": str(pm), "parquet": str(pq), "dir": d}


class TestHelpers:
    def test_nice_breaks(self):
        assert nice_breaks([12.3456, 123.456, 1234.56], 0, 10000) == [10, 100, 1000]
        assert nice_breaks([1.01, 1.02], 0, 2) == [1.01, 1.02]

    def test_code_labels(self):
        col = {"description": "Zone code. Values: A=Agricultural land; B=Built up, dense. C=Coast."}
        assert code_labels(col, ["A", "B", "C"]) == {
            "A": "Agricultural land", "B": "Built up, dense", "C": "Coast"}

    def test_relative_href(self):
        assert relative_href("s3://b/x/parcels.pmtiles", "s3://b/x/styles/parcels-zone.json") \
            == "../parcels.pmtiles"
        assert relative_href("https://s3-west.nrp-nautilus.io/b/x/p.pmtiles",
                             "s3://b/x/styles/p.json") == "../p.pmtiles"
        assert relative_href("s3://b/p.pmtiles", "s3://other/p.json") == "s3://b/p.pmtiles"

    def test_style_filename(self):
        assert style_filename("parcels", "zone") == "parcels-zone.json"
        assert style_filename("parcels", "pop/area") == "parcels-pop-per-area.json"
        assert style_filename("parcels", None) == "parcels.json"


class TestProfile:
    def test_lists_drawable_attributes_with_descriptions(self, data):
        coll = {"table:columns": [{"name": "zone", "description": "Zone. Values: A=Ag."}]}
        out = profile(data["pmtiles"], coll)
        (layer,) = out["layers"]
        assert layer["layer"] == "parcels" and layer["geometry"] == "Polygon"
        by = {a["name"]: a for a in layer["attributes"]}
        assert by["zone"]["type"] == "string" and by["zone"]["description"].startswith("Zone.")
        assert by["pop"]["range"][0] == 0


class TestCategorical:
    def test_most_common_classes_and_other(self, data):
        sty, report = build_style(data["pmtiles"], parquet=data["parquet"], column="zone",
                                  kind="categorical", output=str(data["dir"] / "styles" / "s.json"),
                                  title="Parcels")
        assert sty["version"] == 8 and sty["name"] == "Parcels by zone"
        assert sty["sources"]["data"]["url"] == "pmtiles://../parcels.pmtiles"
        fill, outline = sty["layers"]
        assert fill["id"] == "parcels-fill" and fill["source-layer"] == "parcels"
        assert outline["type"] == "line"
        legend = sty["metadata"]["glen:legend"]
        assert [c["color"] for c in legend["classes"][:10]] == CATEGORICAL[:10]
        assert legend["classes"][-1] == {"label": "Other", "color": OTHER}
        # Measured in the GeoParquet: the 10 most frequent zones, most frequent first.
        top = duckdb.sql(f"SELECT zone FROM '{data['parquet']}' GROUP BY 1 "
                         f"ORDER BY count(*) DESC, zone LIMIT 10").fetchall()
        assert [c["label"] for c in legend["classes"][:10]] == [r[0] for r in top]
        assert "the 10 most common of 14 values" in sty["metadata"]["description"]
        assert sty["metadata"]["glen:tooltip_fields"] == ["zone"]
        expr = fill["paint"]["fill-color"]
        assert expr[:3] == ["case", ["!", ["has", "zone"]], NO_DATA]
        assert expr[-1][-1] == OTHER
        assert "GeoParquet" in report[0]

    def test_labels_from_the_collection(self, data):
        coll = {"table:columns": [{"name": "code", "description": "Values: 1=One; 2=Two; 3=Three."}]}
        sty, _ = build_style(data["pmtiles"], parquet=data["parquet"], column="code",
                             kind="categorical", collection=coll)
        labels = [c["label"] for c in sty["metadata"]["glen:legend"]["classes"]]
        assert labels == ["One", "Two", "Three"]  # numeric codes in order

    def test_explicit_colors_name_the_classes(self, data):
        sty, _ = build_style(data["pmtiles"], parquet=data["parquet"], column="zone",
                             kind="categorical", colors={"B": "#000000", "A": "#ffffff"})
        cls = sty["metadata"]["glen:legend"]["classes"]
        assert [(c["label"], c["color"]) for c in cls[:2]] == [("B", "#000000"), ("A", "#ffffff")]


class TestContinuous:
    def test_quantile_breaks_from_the_geoparquet(self, data):
        sty, report = build_style(data["pmtiles"], parquet=data["parquet"], column="pop",
                                  kind="continuous", unit="people")
        legend = sty["metadata"]["glen:legend"]
        assert len(legend["classes"]) == 7  # six classes and No data
        assert legend["classes"][-1] == {"label": "No data", "color": NO_DATA}
        assert legend["classes"][0]["label"].startswith("under ") and "people" in legend["classes"][0]["label"]
        assert [c["color"] for c in legend["classes"][:6]] == SEQUENTIAL["viridis"]
        expr = sty["layers"][0]["paint"]["fill-color"]
        assert expr[0] == "case" and expr[2][0] == "step" and expr[-1] == NO_DATA
        # the breaks are the rounded quantiles of i*i over 600 features
        q = duckdb.sql(f"SELECT quantile_cont(pop, 0.5) FROM '{data['parquet']}'").fetchone()[0]
        assert any(abs(b - q) / q < 0.1 for b in expr[2][3::2])

    def test_ratio_of_two_columns(self, data):
        sty, _ = build_style(data["pmtiles"], parquet=data["parquet"], column="pop/area",
                             kind="continuous", palette="ylorrd", reverse=True)
        expr = sty["layers"][0]["paint"]["fill-color"]
        assert expr[2][1][0] == "/"
        assert sty["metadata"]["glen:legend"]["classes"][0]["color"] == SEQUENTIAL["ylorrd"][-1]
        assert sty["metadata"]["glen:tooltip_fields"] == ["pop", "area"]

    def test_given_breaks(self, data):
        sty, _ = build_style(data["pmtiles"], column="pop", kind="continuous", breaks=[10, 1000])
        assert len(sty["metadata"]["glen:legend"]["classes"]) == 4


class TestSingleColour:
    def test_reason_is_recorded(self, data):
        sty, _ = build_style(data["pmtiles"], single_colour="Footprints only; no attribute varies.")
        assert sty["metadata"]["glen:single_colour_reason"] == "Footprints only; no attribute varies."
        assert sty["layers"][0]["paint"]["fill-color"] == "#4a7bb7"
        assert "glen:legend" not in sty["metadata"]


class TestRefusals:
    def test_needs_a_column_and_kind(self, data):
        with pytest.raises(ValueError, match="column and a kind"):
            build_style(data["pmtiles"], column="zone")

    def test_unknown_column(self, data):
        with pytest.raises(ValueError, match="not an attribute"):
            build_style(data["pmtiles"], column="nope", kind="categorical")


def test_cli(data, monkeypatch, capsys):
    from cng_datasets import cli
    out = data["dir"] / "styles" / "parcels-zone.json"
    out.parent.mkdir(exist_ok=True)
    monkeypatch.setattr(sys, "argv", ["cng-datasets", "style", data["pmtiles"], "--parquet", data["parquet"],
                                      "--column", "zone", "--kind", "categorical", "--output", str(out)])
    cli.main()
    sty = json.loads(out.read_text())
    assert sty["sources"]["data"]["url"] == "pmtiles://../parcels.pmtiles"
    monkeypatch.setattr(sys, "argv", ["cng-datasets", "style-profile", data["pmtiles"]])
    capsys.readouterr()
    cli.main()
    assert json.loads(capsys.readouterr().out)["layers"][0]["layer"] == "parcels"
