"""Thumbnails of a vector dataset drawn with its default style (issue #340)."""

import json
import sys

import duckdb
import numpy as np
import pytest

gdal = pytest.importorskip("osgeo.gdal")

from cng_datasets.vector.thumbnail import (  # noqa: E402
    UnsupportedExpression,
    evaluate,
    parse_color,
    referenced_properties,
    render_thumbnail,
)


def _png(png):
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    gdal.FileFromMemBuffer("/vsimem/v.png", png)
    ds = gdal.Open("/vsimem/v.png")
    arr = np.stack([ds.GetRasterBand(i + 1).ReadAsArray() for i in range(ds.RasterCount)], -1)
    ds = None
    gdal.Unlink("/vsimem/v.png")
    return arr


@pytest.fixture(scope="module")
def parquet(tmp_path_factory):
    """A 10 x 10 grid of squares over California, A in the west half and B in the
    east, and one placeholder far out in the Pacific that a filter hides."""
    path = tmp_path_factory.mktemp("vt") / "grid.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute(f"""COPY (
        SELECT i AS id, CASE WHEN i % 10 < 5 THEN 'A' ELSE 'B' END AS kind, i * 1.0 AS v,
               ST_MakeEnvelope(-122 + (i % 10) * 0.2, 36 + (i // 10) * 0.2,
                               -122 + (i % 10) * 0.2 + 0.19, 36 + (i // 10) * 0.2 + 0.19) AS geom
        FROM range(100) t(i)
        UNION ALL SELECT 999, 'placeholder', NULL, ST_MakeEnvelope(-170, 0, -169, 1)
    ) TO '{path}' (FORMAT PARQUET)""")
    return str(path)


def _style(colour, flt=None, outline=False):
    fill = {"id": "grid-fill", "type": "fill", "source": "data", "source-layer": "grid",
            "paint": {"fill-color": colour, "fill-opacity": 1.0}}
    if flt is not None:
        fill["filter"] = flt
    layers = [fill]
    if outline:
        layers.append({"id": "grid-outline", "type": "line", "source": "data", "source-layer": "grid",
                       "paint": {"line-color": "#000000", "line-width": 1, "line-opacity": 1}})
    layers.append({"id": "labels", "type": "symbol", "source": "data", "source-layer": "grid",
                   "layout": {"text-field": ["get", "kind"]}})
    return {"version": 8, "sources": {"data": {"type": "vector", "url": "pmtiles://grid.pmtiles"}},
            "layers": layers}


MATCH = ["match", ["get", "kind"], "A", "#ff0000", "B", "#0000ff", "#00ff00"]
NOT_PLACEHOLDER = ["!=", ["get", "kind"], "placeholder"]


class TestExpressions:
    def test_match_case_step(self):
        p = {"kind": "A", "v": 7.0}
        assert evaluate(MATCH, p) == "#ff0000"
        assert evaluate(["case", ["!", ["has", "x"]], "#cccccc", "#000000"], p) == "#cccccc"
        assert evaluate(["step", ["get", "v"], "a", 5, "b", 10, "c"], p) == "b"
        assert evaluate(["match", ["to-string", ["get", "v"]], ["7", "8"], 1, 0], p) == 1

    def test_comparisons_and_legacy_filters(self):
        p = {"Range": "No Range", "n": 3}
        assert evaluate(["!=", ["get", "Range"], "No Range"], p) is False
        assert evaluate(["!=", "Range", "No Range"], p) is False      # legacy form
        assert evaluate(["in", "n", 1, 2, 3], p) is True
        assert evaluate(["all", [">", ["get", "n"], 2], ["<", ["get", "n"], 4]], p) is True
        assert evaluate([">", ["get", "missing"], 2], p) is False

    def test_numbers(self):
        p = {"a": "12", "b": 4}
        assert evaluate(["/", ["to-number", ["get", "a"], 0], ["to-number", ["get", "b"], 1]], p) == 3
        assert evaluate(["to-number", ["get", "x"], -1e30], {"x": "1930s"}) == -1e30
        assert evaluate(["interpolate", ["linear"], ["get", "b"], 0, 0, 8, 100], p) == 50

    def test_an_unknown_operator_is_an_error(self):
        with pytest.raises(UnsupportedExpression):
            evaluate(["within", {"type": "Polygon"}], {})

    def test_referenced_properties(self):
        assert referenced_properties(["case", NOT_PLACEHOLDER, MATCH, "#000"]) == ["kind"]
        assert referenced_properties(["==", "Range", "x"]) == ["Range"]

    def test_parse_color(self):
        assert parse_color("#abc") == (170, 187, 204, 1.0)
        assert parse_color("rgba(1, 2, 3, 0.5)") == (1, 2, 3, 0.5)


class TestRender:
    def test_classes_are_drawn_in_their_colours(self, parquet):
        img = _png(render_thumbnail(_style(MATCH, NOT_PLACEHOLDER), parquet))
        assert img.shape == (384, 512, 3)
        px = {tuple(p) for p in img.reshape(-1, 3)}
        assert (255, 0, 0) in px and (0, 0, 255) in px
        assert (0, 255, 0) not in px                      # the placeholder is filtered out
        assert tuple(img[0, 0]) == (244, 244, 242)        # background in the margin
        # West half red, east half blue, framed on the grid alone.
        row = img[192]
        red = np.flatnonzero((row == [255, 0, 0]).all(1))
        blue = np.flatnonzero((row == [0, 0, 255]).all(1))
        assert red.max() < blue.min()
        assert len(red) + len(blue) > 200                  # the grid fills the frame

    def test_without_the_filter_the_frame_zooms_out_to_the_placeholder(self, parquet):
        img = _png(render_thumbnail(_style(MATCH), parquet))
        filled = (img != [244, 244, 242]).any(-1)
        assert filled.mean() < 0.05                       # a speck, as data-workflows warned

    def test_opacity_blends_with_the_background(self, parquet):
        sty = _style("#000000", NOT_PLACEHOLDER)
        sty["layers"][0]["paint"]["fill-opacity"] = 0.5
        img = _png(render_thumbnail(sty, parquet))
        assert tuple(img[192, 256]) == (122, 122, 121)

    def test_outline_layer_draws_polygon_boundaries(self, parquet):
        img = _png(render_thumbnail(_style("#ffffff", NOT_PLACEHOLDER, outline=True), parquet))
        px = {tuple(p) for p in img.reshape(-1, 3)}
        assert (0, 0, 0) in px and (255, 255, 255) in px

    def test_a_style_from_build_style(self, parquet, tmp_path):
        """The styles cng-datasets builds evaluate here, as the GeoParquet's own columns."""
        from cng_datasets.vector.style import paint_for, maplibre_style
        colour = ["case", ["==", ["typeof", ["get", "v"]], "number"],
                  ["step", ["to-number", ["get", "v"]], "#440154", 50, "#fde725"], "#cccccc"]
        paint, outline = paint_for("fill", colour)
        sty = maplibre_style("fill", paint, outline, "Grid by v", "grid.pmtiles", "grid", "test")
        img = _png(render_thumbnail(sty, parquet))
        px = {tuple(p) for p in img.reshape(-1, 3)}
        assert len(px) > 3  # two classes, the no-data grey, blended at 0.7, and outlines

    def test_everything_filtered_is_an_error(self, parquet):
        with pytest.raises(ValueError, match="no feature"):
            render_thumbnail(_style(MATCH, ["==", ["get", "kind"], "nothing"]), parquet)


def test_cli(parquet, tmp_path, monkeypatch):
    from cng_datasets import cli
    style = tmp_path / "s.json"
    style.write_text(json.dumps(_style(MATCH, NOT_PLACEHOLDER)))
    out = tmp_path / "t.png"
    monkeypatch.setattr(sys, "argv", ["cng-datasets", "vector-thumbnail", "--style", str(style),
                                      "--parquet", parquet, "--output", str(out), "--size", "256"])
    cli.main()
    assert _png(out.read_bytes()).shape == (192, 256, 3)
