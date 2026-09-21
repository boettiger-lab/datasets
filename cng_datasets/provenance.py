"""What built a dataset, recorded into the dataset.

Nothing used to record it. A published hex partition carried no trace of the
code that produced it, and because generated manifests pin
`ghcr.io/boettiger-lab/datasets:latest` — rebuilt on every push to `main` — the
version that ran was not even a released one: it was whatever `main` happened
to be when the pod pulled the image.

That matters when a build turns out to have been wrong. Reading band 1 of a
multi-band raster and labelling it with `--value-column` (issue #214) published
384,922,346 rows of one variable documented as another, and nothing in the
output distinguished it from a correct build. Answering "which datasets came
from a version with that bug" then means correlating S3 timestamps against git
history, for every dataset, by hand.

The stamp goes in the parquet's own key-value metadata rather than a sidecar
file, so it travels with the data through every copy, sync and re-publish:

    SELECT key, value FROM parquet_kv_metadata('s3://.../h0=*/data_0.parquet');

The library versions are there because two of this project's sharper bugs were
environment-dependent rather than code-dependent — a GDAL without
`WarpOptions(cutlineWKT=)` (#197) and one without OGR's Parquet driver — and
neither was answerable after the fact from the dataset alone.
"""

import os
from datetime import datetime, timezone
from typing import Dict


def build_metadata() -> Dict[str, str]:
    """The provenance recorded on every published parquet."""
    from . import __version__

    meta = {
        "cng_datasets_version": __version__,
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    # Set by the pod that runs the build, when whoever generated it knows the
    # image. Absent for a local run, which is itself worth knowing.
    image = os.environ.get("CNG_IMAGE")
    if image:
        meta["image"] = image
    for name, get in (("gdal_version", _gdal_version),
                      ("duckdb_version", _duckdb_version)):
        value = get()
        if value:
            meta[name] = value
    return meta


def _gdal_version():
    try:
        from osgeo import gdal
        return gdal.__version__
    except Exception:
        return None


def _duckdb_version():
    try:
        import duckdb
        return duckdb.__version__
    except Exception:
        return None


def kv_metadata_sql() -> str:
    """A `, KV_METADATA {...}` fragment for a DuckDB `COPY ... (FORMAT PARQUET)`.

    Returns an empty string if the metadata cannot be built, so a stamp that
    fails can never be the reason a dataset does not get written.
    """
    try:
        pairs = ", ".join(
            f"{key}: '{str(value).replace(chr(39), chr(39) * 2)}'"
            for key, value in build_metadata().items()
        )
    except Exception:
        return ""
    return f", KV_METADATA {{{pairs}}}" if pairs else ""
