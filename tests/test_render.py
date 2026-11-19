"""Default renders and thumbnails for the COGs a workflow writes (issue #340)."""

import json
import tempfile
from pathlib import Path

import numpy as np
import pytest
import yaml

gdal = pytest.importorskip("osgeo.gdal")
from osgeo import osr  # noqa: E402

from cng_datasets.raster.render import (  # noqa: E402
    cog_render,
    thumbnail_key,
    thumbnail_png,
    write_renders,
)

NODATA = -9999.0


def _tif(path, data, nodata=NODATA, scale=None, offset=None, table=None, dtype=None):
    dtype = dtype or (gdal.GDT_Byte if data.dtype == np.uint8 else gdal.GDT_Float32)
    h, w = data.shape
    ds = gdal.GetDriverByName("GTiff").Create(str(path), w, h, 1, dtype)
    ds.SetGeoTransform([-121.0, 0.001, 0, 38.0, 0, -0.001])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    ds.SetProjection(srs.ExportToWkt())
    band = ds.GetRasterBand(1)
    if nodata is not None:
        band.SetNoDataValue(nodata)
    if scale is not None:
        band.SetScale(scale)
    if offset is not None:
        band.SetOffset(offset)
    if table is not None:
        ct = gdal.ColorTable()
        for value, rgba in table.items():
            ct.SetColorEntry(value, rgba)
        band.SetRasterColorTable(ct)
    band.WriteArray(data)
    ds = None
    return str(path)


def _skewed(n=400):
    rng = np.random.default_rng(11)
    data = rng.lognormal(0, 1.5, (n, n)).astype(np.float32)  # population-like skew
    data[: n // 5, :] = NODATA
    return data


def _png(png):
    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    gdal.FileFromMemBuffer("/vsimem/t.png", png)
    ds = gdal.Open("/vsimem/t.png")
    arr = np.stack([ds.GetRasterBand(i + 1).ReadAsArray() for i in range(ds.RasterCount)], -1)
    ds = None
    gdal.Unlink("/vsimem/t.png")
    return arr


class TestRender:
    def test_continuous_stretches_over_the_2nd_to_98th_percentile(self, tmp_path):
        data = _skewed()
        r = cog_render(_tif(tmp_path / "a.tif", data))
        valid = data[data != NODATA]
        lo, hi = r["rescale"][0]
        assert r["colormap_name"] == "viridis" and r["nodata"] == -9999
        # Read from the full raster here (it is under the overview size).
        assert lo == pytest.approx(np.percentile(valid, 2), rel=1e-4)
        assert hi == pytest.approx(np.percentile(valid, 98), rel=1e-4)
        assert hi < valid.max() / 2  # min-max would have put most data at the bottom

    def test_rescale_is_in_real_units(self, tmp_path):
        stored = np.tile(np.arange(100, dtype=np.float32), (100, 1))
        r = cog_render(_tif(tmp_path / "s.tif", stored, nodata=None, scale=0.5, offset=-10))
        lo, hi = r["rescale"][0]
        assert lo == pytest.approx(np.percentile(stored, 2) * 0.5 - 10, abs=1e-3)
        assert hi == pytest.approx(np.percentile(stored, 98) * 0.5 - 10, abs=1e-3)
        assert "nodata" not in r

    def test_a_color_table_becomes_the_colormap(self, tmp_path):
        data = np.array([[1, 2], [3, 0]], dtype=np.uint8).repeat(50, 0).repeat(50, 1)
        table = {0: (0, 0, 0, 0), 1: (255, 0, 0, 255), 2: (0, 128, 0, 255), 3: (0, 0, 255, 255)}
        r = cog_render(_tif(tmp_path / "c.tif", data, nodata=0, table=table))
        assert r == {"colormap": {"1": "#ff0000", "2": "#008000", "3": "#0000ff"},
                     "resampling": "nearest", "nodata": 0}

    def test_black_classes_in_the_data_are_kept(self, tmp_path):
        """Padding is opaque black; a real black class is too, and is kept when used."""
        data = np.array([[1, 4], [3, 0]], dtype=np.uint8).repeat(50, 0).repeat(50, 1)
        table = {1: (255, 0, 0, 255), 3: (0, 0, 255, 255), 4: (0, 0, 0, 255),
                 9: (10, 20, 30, 255)}  # 9: a class with a colour but no pixels
        r = cog_render(_tif(tmp_path / "c.tif", data, nodata=0, table=table))
        assert r["colormap"] == {"1": "#ff0000", "3": "#0000ff", "4": "#000000", "9": "#0a141e"}

    def test_one_value_throughout_has_no_render(self, tmp_path):
        assert cog_render(_tif(tmp_path / "k.tif", np.full((50, 50), 7, np.float32))) is None

    def test_overrides(self, tmp_path):
        path = _tif(tmp_path / "a.tif", _skewed())
        r = cog_render(path, colormap="rdbu_r", rescale=[5, -5])
        assert r["colormap_name"] == "rdbu_r" and r["rescale"] == [[-5.0, 5.0]]
        r = cog_render(path, colormap_json={1: "#123456"})
        assert r["colormap"] == {"1": "#123456"} and r["resampling"] == "nearest"
        r = cog_render(path, colormap="twilight")
        assert "rescale" not in r  # a cyclic map is not stretched


class TestThumbnail:
    def test_continuous_thumbnail(self, tmp_path):
        data = _skewed(1000)
        path = _tif(tmp_path / "a.tif", data)
        r = cog_render(path)
        img = _png(thumbnail_png(path, r, size=256))
        assert img.shape == (256, 256, 4)
        assert (img[:40, :, 3] == 0).all()           # the nodata rows are transparent
        assert (img[60:, :, 3] == 255).all()
        assert len({tuple(p) for p in img[60:, :, :3].reshape(-1, 3)}) > 20  # a ramp, not one colour

    def test_color_table_thumbnail_keeps_the_classes(self, tmp_path):
        data = np.array([[1, 2], [3, 0]], dtype=np.uint8).repeat(200, 0).repeat(200, 1)
        table = {1: (255, 0, 0, 255), 2: (0, 128, 0, 255), 3: (0, 0, 255, 255)}
        path = _tif(tmp_path / "c.tif", data, nodata=0, table=table)
        img = _png(thumbnail_png(path, cog_render(path), size=100))
        assert tuple(img[10, 10]) == (255, 0, 0, 255)
        assert tuple(img[10, 90]) == (0, 128, 0, 255)
        assert tuple(img[90, 10]) == (0, 0, 255, 255)
        assert img[90, 90, 3] == 0

    def test_thumbnail_key(self):
        assert thumbnail_key("lc/nlcd-2001-cog.tif") == "lc/nlcd-2001-cog.thumbnail.png"
        assert thumbnail_key("x.tiff") == "x.thumbnail.png"


class TestWriteRenders:
    def test_local(self, tmp_path):
        a = _tif(tmp_path / "a-cog.tif", _skewed())
        k = _tif(tmp_path / "k-cog.tif", np.full((50, 50), 7, np.float32))
        out = tmp_path / "ds.render.json"
        doc = write_renders([a, k], str(out), size=128)
        assert json.loads(out.read_text()) == doc
        assert list(doc["renders"]) == [a]          # the constant COG has none
        thumb = doc["renders"][a]["thumbnail"]
        assert thumb == str(tmp_path / "a-cog.thumbnail.png")
        assert _png(Path(thumb).read_bytes()).shape[:2] == (128, 128)

    def test_no_thumbnail_for_a_colormap_it_cannot_draw(self, tmp_path, capsys):
        a = _tif(tmp_path / "a-cog.tif", _skewed())
        doc = write_renders([a], str(tmp_path / "r.json"), colormap="magma")
        assert "thumbnail" not in doc["renders"][a]
        assert "no thumbnail" in capsys.readouterr().out
        assert not (tmp_path / "a-cog.thumbnail.png").exists()


class TestWorkflowStep:
    @pytest.mark.timeout(10)
    def test_raster_workflow_renders_each_cog_it_wrote(self, monkeypatch):
        from cng_datasets.k8s.workflows import generate_raster_workflow
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: False)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(dataset_name="lc/nlcd", source_urls="s3://src/n_{year}.tif",
                                     bucket="public-lc", output_dir=tmpdir, years="2000-2001")
            job = yaml.safe_load((Path(tmpdir) / "lc-nlcd-render.yaml").read_text())
            wf = yaml.safe_load((Path(tmpdir) / "workflow.yaml").read_text())
            cm = yaml.safe_load((Path(tmpdir) / "configmap.yaml").read_text())
        cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
        assert ("cng-datasets cog-render s3://public-lc/lc/nlcd-2000-cog.tif "
                "s3://public-lc/lc/nlcd-2001-cog.tif --output s3://public-lc/lc/nlcd.render.json") in cmd
        script = wf["spec"]["template"]["spec"]["containers"][0]["args"][0]
        assert (script.index("wait_job lc-nlcd-hex ") < script.index("-f /yamls/lc-nlcd-render.yaml")
                < script.index("-f /yamls/lc-nlcd-checksums.yaml"))
        assert "lc-nlcd-render.yaml" in cm["data"]

    @pytest.mark.timeout(10)
    def test_no_render_for_a_source_cog(self, monkeypatch):
        from cng_datasets.k8s.workflows import generate_raster_workflow
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: True)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(dataset_name="ras", source_urls="https://e.com/x.tif",
                                     bucket="b", output_dir=tmpdir)
            assert not (Path(tmpdir) / "ras-render.yaml").exists()
            assert "ras-render.yaml" not in (Path(tmpdir) / "workflow.yaml").read_text()
