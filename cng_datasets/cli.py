"""
Command-line interface for cng-datasets toolkit.
"""

import argparse
import sys

from cng_datasets.raster.cog import VALID_HEX_REDUCERS

# Repeated on every flag that takes h0 grid positions. The two numberings both
# run 0-121, so a base-cell list passed as positions is always in range and
# never errors — it just builds a different part of the world (issue #213).
POS_NOTE = (
    "These are **positions** in the h0 grid's own ordering, not H3 base cell "
    "numbers — position 12 is base cell 9. Both run 0-121, so a base-cell list "
    "passed here is always in range and silently builds a different part of the "
    "world; pass --h0-cells for base cell numbers instead (#213)."
)


def _resolve_h0_subset(args):
    """Grid positions from --h0-subset or --h0-cells, or None for a global run.

    The two flags mean the same restriction in different numberings, so taking
    both would leave which one won up to reading the code (issue #213).
    """
    subset = getattr(args, "h0_subset", None)
    cells = getattr(args, "h0_cells", None)
    if subset and cells:
        raise ValueError(
            "--h0-subset and --h0-cells are the same restriction in two "
            "numberings; pass one. --h0-subset takes h0 grid positions, "
            "--h0-cells takes H3 base cell numbers."
        )
    if subset:
        return [int(x.strip()) for x in subset.split(",") if x.strip()]
    if cells:
        from cng_datasets.raster.cog import h0_positions_for_base_cells
        base_cells = [int(x.strip()) for x in cells.split(",") if x.strip()]
        positions = h0_positions_for_base_cells(base_cells)
        print(f"✓ H3 base cells {base_cells} → h0 grid positions {positions}")
        return positions
    return None


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Cloud-native geospatial dataset processing toolkit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Vector processing command
    vector_parser = subparsers.add_parser("vector", help="Process vector datasets")
    vector_parser.add_argument("--input", required=True, help="Input file URL")
    vector_parser.add_argument("--output", required=True, help="Output directory URL")
    vector_res_group = vector_parser.add_mutually_exclusive_group()
    vector_res_group.add_argument("--resolution", type=int, default=10, help="H3 resolution")
    vector_res_group.add_argument(
        "--resolution-by-area", type=str, default=None,
        help="Variable resolution by polygon area (issue #98). Comma-separated "
             "'threshold:resolution' bins (planar deg2) plus a trailing catch-all "
             "resolution, e.g. '12:8,600:6,5' (area<=12 -> res 8, <=600 -> res 6, "
             "else res 5). Output carries a union schema + native_res column. "
             "Include 0 in --parent-resolutions so the h0 partition column exists.")
    vector_parser.add_argument("--chunk-size", type=int, default=500, help="Number of rows to process in pass 1 (geometry to H3 arrays)")
    vector_parser.add_argument("--intermediate-chunk-size", type=int, default=10, help="Number of rows to process in pass 2 (unnesting arrays) - reduce if hitting OOM")
    vector_parser.add_argument("--chunk-id", type=int, help="Process specific chunk")
    vector_parser.add_argument("--parent-resolutions", type=str, default="9,8,0", help="Comma-separated parent H3 resolutions (default: '9,8,0')")
    vector_parser.add_argument("--id-column", help="ID column name (auto-detected if not specified)")
    vector_parser.add_argument("--plan", default=None, metavar="URL",
                               help="Chunk plan from `vector-plan` (#124): chunk K is the plan's row "
                                    "range K instead of a fixed --chunk-size slice. An index past "
                                    "the end of the plan exits cleanly.")

    # Cells-per-chunk planning for the vector hex fan-out (issue #124)
    plan_parser = subparsers.add_parser(
        "vector-plan",
        help="Cut a GeoParquet into hex chunks by estimated H3 cells (#124)")
    plan_parser.add_argument("--input", required=True, help="GeoParquet the hex pods will read")
    plan_parser.add_argument("--output", required=True, help="Where to write the plan parquet")
    plan_res_group = plan_parser.add_mutually_exclusive_group()
    plan_res_group.add_argument("--resolution", type=int, default=10, help="H3 resolution")
    plan_res_group.add_argument("--resolution-by-area", type=str, default=None,
                                help="Same spec as `vector --resolution-by-area`")
    plan_parser.add_argument("--cells-per-chunk", type=float, default=None, metavar="N",
                             help="Estimated H3 cells per chunk (default 5,000,000)")
    plan_parser.add_argument("--max-chunks", type=int, default=None, metavar="N",
                             help="Upper bound on chunks: the hex Job's completions. The budget "
                                  "is raised, with a warning, to fit.")
    plan_parser.add_argument("--max-features-per-chunk", type=int, default=None, metavar="N",
                             help="Feature cap per chunk (default 1000, raised to fit --max-chunks)")


    # N-D cube (time, lat, lon) processing (issue #181)
    mdim_parser = subparsers.add_parser(
        "mdim", help="Hex a (time, lat, lon) cube (zarr, netCDF, ...) into H3 partitions")
    mdim_parser.add_argument("--input", dest="inputs", action="append", required=True,
                             metavar="PATH",
                             help="GDAL multidimensional source, e.g. 'ZARR:\"/vsicurl/https://…\"' "
                                  "or a netCDF path. Repeat for one time series split across files "
                                  "(e.g. a file per year), in time order.")
    mdim_parser.add_argument("--variable", dest="variables", action="append", required=True,
                             metavar="NAME", help="Array to hex. Repeatable; all must share the grid.")
    mdim_parser.add_argument("--output-parquet", required=True, help="Output hex directory")
    mdim_parser.add_argument("--resolution", type=int, required=True, help="H3 resolution")
    mdim_parser.add_argument("--parent-resolutions", type=str, default="0",
                             help="Comma-separated parent resolutions (default '0')")
    mdim_parser.add_argument("--h0-index", type=int, default=None,
                             help="h0 grid position to process (same as --chunk-index at --chunk-resolution 0)")
    mdim_parser.add_argument("--chunk-resolution", type=int, default=0)
    mdim_parser.add_argument("--chunk-index", type=int, default=None)
    mdim_parser.add_argument("--h0-subset", type=str, default=None, metavar="POSITIONS",
                             help="Restrict the chunk list to these h0 grid positions. " + POS_NOTE)
    mdim_parser.add_argument("--h0-cells", type=str, default=None, metavar="BASE_CELLS",
                             help="The same restriction as --h0-subset, given as H3 base cell numbers.")
    mdim_parser.add_argument("--hex-resampling", default="mean", choices=["mean", "min", "max"],
                             help="Reducer over pixels in a cell and over the --time-agg window. "
                                  "'sum' is refused: mdim placement is not area-weighted.")
    mdim_parser.add_argument("--time-agg", default="none", choices=["none", "month", "year"])
    mdim_parser.add_argument("--time-start", default=None, metavar="YYYY-MM-DD")
    mdim_parser.add_argument("--time-end", default=None, metavar="YYYY-MM-DD")
    mdim_parser.add_argument("--fan-out", default="space", choices=["space", "time"],
                             help="space: process one spatial chunk (--h0-index / --chunk-index) over "
                                  "all time. time: process every h0 (in --h0-subset) over this "
                                  "call's time range, and write per-h0 parts for merge-chunks; "
                                  "for sources whose chunks span the whole grid (#267).")
    mdim_parser.add_argument("--unit-index", type=int, default=None,
                             help="With --fan-out time: this unit's index, which names its parts "
                                  "and its completion marker.")
    mdim_parser.add_argument("--placement", default="auto", choices=["auto", "aggregate", "sample"],
                             help="aggregate: pixel centres into cells (pixels finer than cells); "
                                  "sample: cell centres read their pixel (pixels coarser). "
                                  "auto picks by comparing pixel and cell area.")

    mdim_wf = subparsers.add_parser(
        "mdim-workflow", help="Generate the k8s fan-out for a (time, lat, lon) cube (#181)")
    mdim_wf.add_argument("--dataset", required=True)
    mdim_wf.add_argument("--input", dest="inputs", action="append", required=True, metavar="PATH",
                         help="Repeat for a time series split across files, in time order")
    mdim_wf.add_argument("--variable", dest="variables", action="append", required=True)
    mdim_wf.add_argument("--bucket", required=True)
    mdim_wf.add_argument("--output-dir", default=".")
    mdim_wf.add_argument("--namespace", default=None)
    mdim_wf.add_argument("--h3-resolution", type=int, default=6)
    mdim_wf.add_argument("--parent-resolutions", default="0")
    mdim_wf.add_argument("--hex-resampling", default="mean", choices=["mean", "min", "max"])
    mdim_wf.add_argument("--time-agg", default="none", choices=["none", "month", "year"])
    mdim_wf.add_argument("--time-start", default=None)
    mdim_wf.add_argument("--time-end", default=None)
    mdim_wf.add_argument("--placement", default="auto", choices=["auto", "aggregate", "sample"])
    mdim_wf.add_argument("--hex-memory", default="16Gi")
    mdim_wf.add_argument("--hex-cpu", default="4")
    mdim_wf.add_argument("--hex-storage", default="10Gi")
    mdim_wf.add_argument("--max-parallelism", type=int, default=50)
    mdim_wf.add_argument("--h0-subset", default=None, metavar="POSITIONS", help=POS_NOTE)
    mdim_wf.add_argument("--h0-cells", default=None, metavar="BASE_CELLS")
    mdim_wf.add_argument("--chunk-resolution", type=int, default=0)
    mdim_wf.add_argument("--hex-retries", type=int, default=2)
    mdim_wf.add_argument("--max-failed-indexes", type=int, default=1)
    mdim_wf.add_argument("--merge-memory", default="16Gi")
    mdim_wf.add_argument("--merge-storage", default="50Gi")
    mdim_wf.add_argument("--fan-out", default="auto", choices=["auto", "space", "time"],
                         help="Axis the hex Job splits over (#267). auto: time when one chunk of "
                              "the source spans the whole grid (NEX-GDDP), space otherwise.")
    mdim_wf.add_argument("--time-unit-steps", type=int, default=365,
                         help="With --fan-out time on a single input: about this many time steps "
                              "per pod, cut only at --time-agg key boundaries")
    mdim_wf.add_argument("--no-validate-source", action="store_true",
                         help="Skip opening the first input at generation time")
    mdim_wf.add_argument("--backend", choices=["k8s", "armada", "auto"], default="k8s")
    mdim_wf.add_argument("--armada-queue", default=None)
    mdim_wf.add_argument("--armada-priority-class", default=None)
    mdim_wf.add_argument("--profile", default=None)

    # Raster processing command
    raster_parser = subparsers.add_parser("raster", help="Process raster datasets")
    raster_parser.add_argument("--input", required=True, action="append", dest="inputs",
                               help="Input raster file (local or /vsicurl/ URL). Repeat for multiple tiles to mosaic.")
    raster_parser.add_argument("--output-cog", help="Output COG file path")
    raster_parser.add_argument("--output-parquet", help="Output parquet directory (e.g., s3://bucket/dataset/hex/)")
    raster_parser.add_argument("--resolution", type=int, help="H3 resolution (auto-detected if not specified)")
    raster_parser.add_argument("--parent-resolutions", type=str, default="0", help="Comma-separated parent H3 resolutions (default: '0')")
    raster_parser.add_argument("--h0-index", type=int,
                               help="Process one h0 region by its **position** in the h0 grid's "
                                    "ordering (0-121), or omit to process all. A position is not "
                                    "an H3 base cell number — position 12 is base cell 9 — and "
                                    "both run 0-121, so a base cell number passed here is always "
                                    "in range and silently processes a different cell (#213). The "
                                    "resolved cell and its base cell are logged at start-up.")
    raster_parser.add_argument("--chunk-resolution", type=int, default=0, metavar="N",
                               help="H3 resolution of the unit of work (default: 0, one h0 base "
                                    "cell). A higher value splits each h0 into its res-N "
                                    "descendants, cutting peak memory ~7x per level, since RAM "
                                    "tracks the largest chunk's cell count (#173). Sub-chunks are "
                                    "written as part-{cell}.parquet and must be consolidated by "
                                    "`cng-datasets merge-chunks`.")
    raster_parser.add_argument("--chunk-index", type=int, default=None, metavar="K",
                               help="Which chunk to process. At --chunk-resolution 0 this is "
                                    "exactly --h0-index; give one or the other.")
    raster_parser.add_argument("--window-reads", choices=["auto", "always", "never"], default="auto",
                               help="Read only this chunk's window of the source COG instead of "
                                    "localizing the whole file. 'auto' (default) windows whenever "
                                    "the source is remote. Full localization is a per-pod cost, so "
                                    "total transfer and ephemeral disk scale with the fan-out rather "
                                    "than with the data (#209).")
    raster_parser.add_argument("--h0-subset", type=str, default=None, metavar="POSITIONS",
                               help="Restrict the chunk list to descendants of these h0 grid "
                                    "**positions**, e.g. '12,14,20,50,71,78' for CONUS, so a "
                                    "regional source never enumerates chunks it cannot overlap. "
                                    + POS_NOTE)
    raster_parser.add_argument("--h0-cells", type=str, default=None, metavar="BASECELLS",
                               help="The same restriction, given as H3 **base cell numbers** — "
                                    "'9,19,20,21,34,36' is the CONUS set and resolves to the "
                                    "positions above. Use this when the list came from the H3 "
                                    "library (h3_get_base_cell_number and friends), which is the "
                                    "obvious way to compute which cells a raster covers. Mutually "
                                    "exclusive with --h0-subset (#213).")
    raster_parser.add_argument("--value-column", default="value", help="Name for raster value column (default: 'value')")
    raster_parser.add_argument("--nodata", type=str,
                               help="NoData value(s) to exclude. Accepts a single value or a "
                                    "comma-separated list for categorical products with multiple "
                                    "fill codes, e.g. '-9999,-1111,32767' (all collapsed to the "
                                    "first in the COG and excluded from hex tiling).")
    raster_parser.add_argument("--compression", default="deflate", help="COG compression (deflate, lzw, zstd)")
    raster_parser.add_argument("--blocksize", type=int, default=512, help="COG block size (default: 512)")
    raster_parser.add_argument("--resampling", default="nearest", help="Resampling method for COG creation (default: nearest)")
    raster_parser.add_argument("--hex-resampling", default="mean",
                               help="Reducer for aggregating source pixels into each "
                                    "H3 cell. With --method=exact-extract (default), "
                                    "one of: sum/mean/mode/fractions/max/min (max/min for "
                                    "peak/richness rasters; 'fractions' emits per-class "
                                    "coverage rows for categorical area accounting, #142). "
                                    "With --method=warp-centroid, any GDAL resampleAlg "
                                    "(average, sum, mode, near, bilinear, cubic, ...). "
                                    "Default: mean.")
    raster_parser.add_argument("--method", default="exact-extract",
                               choices=("exact-extract", "warp-centroid"),
                               help="Raster→hex algorithm. 'exact-extract' (default): "
                                    "area-weighted per-cell, one row per cell, mass-conserving. "
                                    "'warp-centroid': older gdal.Warp→XYZ→centroid path; "
                                    "fast and low-memory but emits one row per warped pixel "
                                    "(consumers GROUP BY h<res>) and is mass-conserving only "
                                    "when hex pitch is finer than source pixel pitch (see #84).")
    raster_parser.add_argument("--target-crs", default="EPSG:4326", help="Output CRS for mosaic (default: EPSG:4326)")
    raster_parser.add_argument("--target-extent", help="Clip bbox 'xmin,ymin,xmax,ymax' in target CRS (mosaic only)")
    raster_parser.add_argument("--target-resolution", type=float, help="Output pixel size in target CRS units (mosaic only)")
    raster_parser.add_argument("--band", type=int,
                               help="Which band of a multi-band source to use, 1-indexed. Applies "
                                    "to the COG, the mosaic and the hex output alike — the band is "
                                    "selected once, at the source. Required when hexing a "
                                    "multi-band raster: without it the first band would be read "
                                    "silently and labelled with --value-column regardless (#214).")
    raster_parser.add_argument("--local-cache-dir", default="/tmp/cng-raster-cache",
                               help="Directory to copy remote input rasters into before processing "
                                    "(default: /tmp/cng-raster-cache). Reading a remote COG via "
                                    "/vsis3/ pays per-pixel HTTP latency that dominates wall time "
                                    "on dense h0 cells — local-cache gives ~12x speedup at the "
                                    "cost of one upfront copy. Use --no-local-cache to stream.")
    raster_parser.add_argument("--no-local-cache", dest="local_cache_dir", action="store_const",
                               const=None, help="Stream the input via /vsis3/ instead of "
                                                "copying to local disk first.")

    # Repartition command
    merge_parser = subparsers.add_parser(
        "merge-chunks",
        help="Merge sub-h0 raster hex chunks into one file per h0 partition")
    merge_parser.add_argument("--chunks-dir", required=True, help="Where the sub-chunked hex step wrote part-*.parquet")
    merge_parser.add_argument("--output-dir", required=True, help="Published hex tree to write h0=*/data_0.parquet into")
    merge_parser.add_argument("--expect-chunks", type=int, default=None, metavar="N",
                              help="The number of chunks the hex fan-out was sized for. The merge "
                                   "refuses to run unless that many chunks recorded completion, so a "
                                   "partly failed fan-out cannot be published as a complete dataset.")
    merge_parser.add_argument("--no-cleanup", dest="cleanup", action="store_false", default=True,
                              help="Keep the chunks prefix after a verified merge")
    merge_parser.add_argument("--memory-limit", type=str, default=None,
                              help="DuckDB memory limit (e.g. '8GiB'). Overrides DUCKDB_MEMORY_LIMIT.")

    gapfill_parser = subparsers.add_parser(
        "gapfill",
        help="Emit an Armada job set re-running the sub-h0 chunks that never completed. "
             "Exits 0 when nothing is missing and 1 when a job set was written, so a "
             "pipeline can branch on it the way it would on diff.")
    gapfill_parser.add_argument("--chunks-dir", required=True, help="Where the hex step wrote parts and markers")
    gapfill_parser.add_argument("--expect-chunks", type=int, required=True, metavar="N",
                                help="Size of the fan-out a complete build has")
    gapfill_parser.add_argument("--hex-manifest", required=True, metavar="YAML",
                                help="The generated <name>-hex.yaml the fan-out came from")
    gapfill_parser.add_argument("--output", required=True, metavar="YAML", help="Where to write the gap-fill job set")
    gapfill_parser.add_argument("--queue", default=None, help="Armada queue (default: the Job's namespace)")
    gapfill_parser.add_argument("--job-set-id", default=None, help="Armada job set id (default: <job name>-gapfill)")
    gapfill_parser.add_argument("--armada-priority-class", default=None, metavar="CLASS",
                                help="Armada priority class for the re-run; a shorthand ('default', "
                                     "'preemptible', 'high') or a literal class name")

    repartition_parser = subparsers.add_parser("repartition", help="Repartition chunks by h0")
    repartition_parser.add_argument("--chunks-dir", required=True, help="Input chunks directory URL")
    repartition_parser.add_argument("--output-dir", required=True, help="Output directory URL")
    repartition_parser.add_argument("--source-parquet", required=True, help="Source parquet with full attributes")
    repartition_parser.add_argument("--cleanup", action="store_true", default=True, help="Remove chunks after repartitioning")
    repartition_parser.add_argument("--memory-limit", type=str, default=None, help="DuckDB memory limit (e.g. '27GiB'). Overrides DUCKDB_MEMORY_LIMIT env var.")

    # K8s job generation command
    k8s_parser = subparsers.add_parser("k8s", help="Generate Kubernetes job")
    k8s_parser.add_argument("--job-name", required=True, help="Job name")
    k8s_parser.add_argument("--cmd", nargs="+", required=True, help="Container command", dest="container_command")
    k8s_parser.add_argument("--output", default="job.yaml", help="Output YAML file")
    k8s_parser.add_argument("--chunks", type=int, help="Number of chunks for indexed job")
    k8s_parser.add_argument("--namespace", default="biodiversity", help="Kubernetes namespace (default: biodiversity)")

    # Workflow generation command
    workflow_parser = subparsers.add_parser("workflow", help="Generate complete dataset workflow")
    workflow_parser.add_argument("--dataset", required=True, help="Dataset name (e.g., redlining)")
    workflow_parser.add_argument("--source-url", action="append", required=True, dest="source_urls", help="Source data URL (can be specified multiple times for multiple inputs)")
    workflow_parser.add_argument("--bucket", required=True, help="S3 bucket for outputs")
    workflow_parser.add_argument("--output-dir", default="k8s", help="Output directory for YAML files")
    workflow_parser.add_argument("--namespace", default=None, help="Kubernetes namespace the jobs run in, and the Armada queue unless --armada-queue overrides it (default from profile, or 'geo-workflows')")
    workflow_parser.add_argument("--h3-resolution", type=int, default=None, help="Target H3 resolution (default: auto — 10 for polygons/points, 8 for lines)")
    workflow_parser.add_argument(
        "--resolution-by-area", type=str, default=None,
        help="Variable resolution by polygon area (issue #98), e.g. '12:8,600:6,5'. "
             "Mutually exclusive with --h3-resolution; emits the same flag into the "
             "hex job. Include 0 in --parent-resolutions for the h0 partition column.")
    workflow_parser.add_argument("--parent-resolutions", type=str, default="9,8,0", help="Comma-separated parent H3 resolutions (default: '9,8,0')")
    workflow_parser.add_argument("--id-column", help="ID column name (auto-detected if not specified)")
    workflow_parser.add_argument("--layer", help="Layer name for multi-layer datasets (e.g., GDB files)")
    workflow_parser.add_argument("--hex-memory", type=str, default="8Gi", help="Memory per hex job pod (default: 8Gi)")
    workflow_parser.add_argument("--max-parallelism", type=int, default=50, help="Maximum parallel hex jobs (default: 50)")
    workflow_parser.add_argument("--max-completions", type=int, default=200, help="Maximum hex job completions (default: 200, increase to reduce chunk size/memory)")
    workflow_parser.add_argument("--chunk-size", type=int, default=None, metavar="N",
                                 help="Features per hex chunk (default 1000). Lower it for a dataset of few "
                                      "but very large features (ecoregions, countries, basins): hex memory "
                                      "follows the H3 cells of the features in a chunk, not their count, so "
                                      "847 continent-scale polygons need small chunks, not one pod (#237). "
                                      "Raised, with a warning, if it would exceed --max-completions. Giving "
                                      "it turns off cells-per-chunk planning.")
    workflow_parser.add_argument("--cells-per-chunk", type=float, default=None, metavar="N",
                                 help="Estimated H3 cells per hex chunk (default 5,000,000). A plan step "
                                      "after convert cuts the GeoParquet at this budget, never exceeding "
                                      "the default features per chunk, and the hex Job is sized to the "
                                      "plan (#124). Mutually exclusive with --chunk-size.")
    workflow_parser.add_argument("--intermediate-chunk-size", type=int, default=10, help="Number of rows to process in pass 2 (unnesting arrays) - reduce if hitting OOM")
    workflow_parser.add_argument("--row-group-size", type=int, default=100000, help="Number of rows per group in convert job (default: 100000)")
    workflow_parser.add_argument("--simplify-tolerance", type=float, default=None, help="Simplify geometry to this tolerance in target-CRS units (degrees for EPSG:4326; e.g. 0.0001 ~ 10m) during the convert step. Right-sizes high-vertex sources for tiling/hex (issue #132).")
    workflow_parser.add_argument("--trim-strings", action="store_true", help="Strip leading/trailing whitespace from every string column during the convert step. Off by default; opt in for sources whose categorical fields carry stray whitespace, which silently breaks equality filters (issue #180).")
    workflow_parser.add_argument("--hex-retries", type=int, default=2, metavar="N",
                                 help="Per-index retry budget for the hex fan-out (backoffLimitPerIndex, "
                                      "default 2). A preemption is already retried; this covers an OOM, a "
                                      "truncated read, or a node going away mid-run (#201).")
    workflow_parser.add_argument("--max-failed-indexes", type=int, default=1, metavar="N",
                                 help="How many hex indexes may exhaust their retries before the Job stops "
                                      "(default 1). Keeps a systematic failure from burning hours on the rest.")
    workflow_parser.add_argument("--hex-storage", type=str, default="10Gi", help="Ephemeral storage request/limit per hex job pod (default: 10Gi)")
    workflow_parser.add_argument("--repartition-storage", type=str, default="50Gi", help="Ephemeral storage request/limit for repartition job pod (default: 50Gi)")
    workflow_parser.add_argument("--repartition-memory", type=str, default="32Gi", help="Memory request/limit for repartition job pod (default: 32Gi)")
    workflow_parser.add_argument("--lat-column", default=None, metavar="NAME",
                                 help="Latitude column when the source is a CSV of points "
                                      "(auto-detected from e.g. Latitude/lat/y if not given)")
    workflow_parser.add_argument("--lon-column", default=None, metavar="NAME",
                                 help="Longitude column when the source is a CSV of points "
                                      "(auto-detected from e.g. Longitude/lon/x if not given)")
    workflow_parser.add_argument("--expect-features", type=int, default=None, metavar="N",
                                 help="Feature count the convert step must produce, known "
                                      "independently (e.g. from the source service's own "
                                      "count). The step exits non-zero on a mismatch, so a "
                                      "silently truncated source fails the workflow instead "
                                      "of flowing into the hex and PMTiles steps. Also sizes the "
                                      "hex job when the source can't be counted from here "
                                      "(e.g. not uploaded yet); without it, an uncountable "
                                      "source is an error")
    workflow_parser.add_argument("--backend", choices=["k8s", "armada"], default="k8s", help="Job backend: 'k8s' for standard Kubernetes Jobs (default), 'armada' for Armada queue submission")
    workflow_parser.add_argument("--armada-queue", default=None, metavar="QUEUE", help="Armada queue when --backend armada/auto. Defaults to --namespace: NRP maps queues one-to-one onto namespaces, but they are separate fields in a job set, so set this to submit to a queue that is not named after the namespace the pods land in.")
    workflow_parser.add_argument("--armada-priority-class", default=None, metavar="CLASS", help="Armada priority class when --backend armada: a shorthand ('default', 'preemptible', 'high') or a literal class name. Default is non-preemptible 'armada-default' — preempted Armada jobs are not rescheduled and k8s Job-level retry settings do not survive conversion")
    # Cluster/storage configuration flags
    workflow_parser.add_argument("--profile", default=None, metavar="NAME_OR_PATH", help="Cluster profile name (e.g. 'nrp') or path to a YAML profile file. Explicit flags below override profile values.")
    workflow_parser.add_argument("--s3-endpoint", default=None, metavar="HOST", help="Internal S3 endpoint for jobs (default from profile, or rook-ceph-rgw-nautiluss3.rook)")
    workflow_parser.add_argument("--s3-public-endpoint", default=None, metavar="HOST", help="Public S3 endpoint (default from profile, or s3-west.nrp-nautilus.io)")
    workflow_parser.add_argument("--s3-secret-name", default=None, metavar="SECRET", help="Kubernetes secret name for S3 credentials (default from profile, or 'aws')")
    workflow_parser.add_argument("--rclone-secret-name", default=None, metavar="SECRET", help="Kubernetes secret name for rclone config (default from profile, or 'rclone-config')")
    workflow_parser.add_argument("--rclone-remote", default=None, metavar="REMOTE", help="Rclone remote name for setup-bucket and pmtiles (default from profile, or 'nrp')")
    workflow_parser.add_argument("--priority-class", default=None, metavar="CLASS", help="Kubernetes priorityClassName; '' to omit (default: omitted, i.e. default priority 0). Pass 'opportunistic' for genuinely interruptible work, or to exceed a namespace quota — on NRP it is the lowest priority available and preemption exposure scales with pod runtime (#201)")
    workflow_parser.add_argument("--node-affinity", default=None, choices=["gpu-avoid", "none"], help="Node affinity: 'gpu-avoid' (NRP GPU avoidance) or 'none' to omit (default from profile)")

    # Raster workflow generation command
    raster_workflow_parser = subparsers.add_parser("raster-workflow", help="Generate complete raster dataset workflow")
    raster_workflow_parser.add_argument("--dataset", required=True, help="Dataset name")
    raster_workflow_parser.add_argument("--source-url", required=True, action="append", dest="source_urls",
                                        help="Source raster URL. Repeat for multiple tiles to mosaic.")
    raster_workflow_parser.add_argument("--bucket", required=True, help="S3 bucket for outputs")
    raster_workflow_parser.add_argument("--output-dir", default="k8s", help="Output directory for YAML files")
    raster_workflow_parser.add_argument("--namespace", default=None, help="Kubernetes namespace the jobs run in, and the Armada queue unless --armada-queue overrides it (default from profile, or 'geo-workflows')")
    raster_workflow_parser.add_argument("--h3-resolution", type=int, default=8, help="Target H3 resolution (default: 8)")
    raster_workflow_parser.add_argument("--parent-resolutions", type=str, default="0", help="Comma-separated parent H3 resolutions (default: '0')")
    raster_workflow_parser.add_argument("--value-column", default="value", help="Name for raster value column")
    raster_workflow_parser.add_argument("--nodata", type=str,
                                        help="NoData value(s) to exclude. Accepts a single value or "
                                             "a comma-separated list for categorical products with "
                                             "multiple fill codes, e.g. '-9999,-1111,32767'.")
    raster_workflow_parser.add_argument("--hex-resampling", default="mean",
                                        choices=VALID_HEX_REDUCERS,
                                        help="Reducer for H3 aggregation. 'sum' for "
                                             "counts (population, carbon); 'mean' for intensities; "
                                             "'mode' for categorical (single dominant class); "
                                             "'fractions' for categorical area accounting (one "
                                             "(value, frac) row per class per cell); 'max'/'min' "
                                             "for peak/extremum (species richness). Default: mean.")
    raster_workflow_parser.add_argument("--hex-memory", type=str, default="32Gi", help="Memory per hex job pod (default: 32Gi)")
    raster_workflow_parser.add_argument("--max-parallelism", type=int, default=61, help="Maximum parallel hex jobs (default: 61)")
    raster_workflow_parser.add_argument("--h0-subset", type=str, default=None, metavar="POSITIONS",
                                        help="Comma-separated h0 grid **positions** the source overlaps, e.g. "
                                             "'12,14,20,50,71,78' for CONUS. The hex job runs one completion "
                                             "per listed cell instead of all 122, so pods that could only find "
                                             "no overlap are never started. Omit for a global source. "
                                             + POS_NOTE)
    raster_workflow_parser.add_argument("--h0-cells", type=str, default=None, metavar="BASECELLS",
                                        help="The same subset, given as H3 **base cell numbers** — "
                                             "'9,19,20,21,34,36' is the CONUS set and resolves to the "
                                             "positions above. Use this when the list came from the H3 "
                                             "library, which is the obvious way to compute which cells a "
                                             "raster covers. Mutually exclusive with --h0-subset (#213).")
    raster_workflow_parser.add_argument("--hex-retries", type=int, default=2, metavar="N",
                                        help="Per-index retry budget for the hex fan-out (backoffLimitPerIndex, "
                                             "default 2). A preemption is already retried; this covers an OOM, a "
                                             "truncated read, or a node going away mid-run (#201).")
    raster_workflow_parser.add_argument("--max-failed-indexes", type=int, default=1, metavar="N",
                                        help="How many hex indexes may exhaust their retries before the Job stops "
                                             "(default 1). Keeps a systematic failure from burning hours on the rest.")
    raster_workflow_parser.add_argument("--hex-storage", type=str, default="20Gi", help="Ephemeral storage request/limit per hex job pod (default: 20Gi)")
    raster_workflow_parser.add_argument("--hex-cpu", type=str, default=None, metavar="N",
                                        help="CPU request/limit per hex job pod (default: 4)")
    raster_workflow_parser.add_argument("--hex-workers", type=int, default=None, metavar="N",
                                        help="Worker processes per hex job pod, emitted as "
                                             "CNG_HEX_WORKERS (default: one per --hex-cpu). This is "
                                             "the memory lever: peak RSS is roughly workers × "
                                             "--hex-chunk-size × bytes per cell. Lower it before "
                                             "raising --hex-memory, which on a scarce large-RAM node "
                                             "turns a retryable OOM into an unschedulable pod (#195).")
    raster_workflow_parser.add_argument("--hex-chunk-size", type=int, default=None, metavar="N",
                                        help="Cells per exact_extract call in the hex job, emitted as "
                                             "CNG_HEX_CHUNK_SIZE (default: 100000). The other half of "
                                             "the peak-memory product; lower it when fewer workers "
                                             "alone is too coarse a step.")
    raster_workflow_parser.add_argument("--cog-storage", type=str, default="50Gi", help="Ephemeral storage request/limit for COG preprocess job pod (default: 50Gi)")
    raster_workflow_parser.add_argument("--target-extent", help="Clip bbox 'xmin,ymin,xmax,ymax' in EPSG:4326 (multi-tile only)")
    raster_workflow_parser.add_argument("--target-resolution", type=float, help="Output pixel size in degrees (multi-tile only)")
    raster_workflow_parser.add_argument("--band", type=int,
                                        help="Which band of a multi-band source to use, 1-indexed. "
                                             "Giving it adds a preprocess-cog step that subsets the "
                                             "band, so the hex job is handed a single-band COG. "
                                             "Required for a multi-band source: the hex job refuses "
                                             "one it cannot disambiguate (#214).")
    raster_workflow_parser.add_argument("--output-cog-name", help="S3 key for intermediate COG (default: {dataset}-cog.tif)")
    raster_workflow_parser.add_argument("--backend", choices=["k8s", "armada", "auto"], default="k8s", help="Job backend: 'k8s' for standard Kubernetes Jobs (default), 'armada' for Armada queue submission, 'auto' to pick armada once the chunk count exceeds the ~200-pod namespace guideline")
    raster_workflow_parser.add_argument("--chunk-resolution", type=int, default=0, metavar="N",
                                        help="H3 resolution of one hex pod's unit of work (default: 0, "
                                             "one h0 base cell). A higher value splits each h0 into its "
                                             "res-N descendants, cutting peak memory ~7x per level since "
                                             "RAM tracks the largest chunk's cell count (#173). Adds a "
                                             "merge step that restores the published one-file-per-"
                                             "partition layout.")
    raster_workflow_parser.add_argument("--max-hex-memory", type=str, default=None, metavar="SIZE",
                                        help="Pick --chunk-resolution automatically as the coarsest that "
                                             "fits this budget, e.g. '8Gi'. Coarsest rather than finest "
                                             "because each extra level multiplies the pod count ~7x. "
                                             "Mutually exclusive with --chunk-resolution.")
    raster_workflow_parser.add_argument("--merge-memory", type=str, default="16Gi",
                                        help="Memory request/limit for the merge job pod (default: 16Gi). "
                                             "It streams one partition at a time, so this does not track "
                                             "the dataset's size.")
    raster_workflow_parser.add_argument("--merge-storage", type=str, default="100Gi",
                                        help="Ephemeral storage request/limit for the merge job pod (default: 50Gi, the house limit)")
    raster_workflow_parser.add_argument("--armada-queue", default=None, metavar="QUEUE", help="Armada queue when --backend armada/auto. Defaults to --namespace: NRP maps queues one-to-one onto namespaces, but they are separate fields in a job set, so set this to submit to a queue that is not named after the namespace the pods land in.")
    raster_workflow_parser.add_argument("--armada-priority-class", default=None, metavar="CLASS", help="Armada priority class when --backend armada: a shorthand ('default', 'preemptible', 'high') or a literal class name. Default is non-preemptible 'armada-default' — preempted Armada jobs are not rescheduled and k8s Job-level retry settings do not survive conversion")
    # Cluster/storage configuration flags
    raster_workflow_parser.add_argument("--profile", default=None, metavar="NAME_OR_PATH", help="Cluster profile name (e.g. 'nrp') or path to a YAML profile file. Explicit flags below override profile values.")
    raster_workflow_parser.add_argument("--s3-endpoint", default=None, metavar="HOST", help="Internal S3 endpoint for jobs (default from profile, or rook-ceph-rgw-nautiluss3.rook)")
    raster_workflow_parser.add_argument("--s3-public-endpoint", default=None, metavar="HOST", help="Public S3 endpoint (default from profile, or s3-west.nrp-nautilus.io)")
    raster_workflow_parser.add_argument("--s3-secret-name", default=None, metavar="SECRET", help="Kubernetes secret name for S3 credentials (default from profile, or 'aws')")
    raster_workflow_parser.add_argument("--rclone-secret-name", default=None, metavar="SECRET", help="Kubernetes secret name for rclone config (default from profile, or 'rclone-config')")
    raster_workflow_parser.add_argument("--rclone-remote", default=None, metavar="REMOTE", help="Rclone remote name for setup-bucket (default from profile, or 'nrp')")
    raster_workflow_parser.add_argument("--priority-class", default=None, metavar="CLASS", help="Kubernetes priorityClassName; '' to omit (default: omitted, i.e. default priority 0). Pass 'opportunistic' for genuinely interruptible work, or to exceed a namespace quota — on NRP it is the lowest priority available and preemption exposure scales with pod runtime (#201)")
    raster_workflow_parser.add_argument("--node-affinity", default=None, choices=["gpu-avoid", "none"], help="Node affinity: 'gpu-avoid' (NRP GPU avoidance) or 'none' to omit (default from profile)")

    # Sync job generation command
    sync_job_parser = subparsers.add_parser("sync-job", help="Generate Kubernetes job for syncing between S3 locations")
    sync_job_parser.add_argument("--job-name", required=True, help="Job name")
    sync_job_parser.add_argument("--source", required=True, help="Source path (e.g., 'remote1:bucket/path')")
    sync_job_parser.add_argument("--destination", required=True, help="Destination path (e.g., 'remote2:bucket/path')")
    sync_job_parser.add_argument("--output", default="sync-job.yaml", help="Output YAML file (default: sync-job.yaml)")
    sync_job_parser.add_argument("--namespace", default="biodiversity", help="Kubernetes namespace (default: biodiversity)")
    sync_job_parser.add_argument("--cpu", default="2", help="CPU request/limit (default: 2)")
    sync_job_parser.add_argument("--memory", default="4Gi", help="Memory request/limit (default: 4Gi)")
    sync_job_parser.add_argument("--dry-run", action="store_true", help="Dry run mode (show what would be synced)")

    # Storage management command
    storage_parser = subparsers.add_parser("storage", help="Manage cloud storage")
    storage_subparsers = storage_parser.add_subparsers(dest="storage_command")

    cors_parser = storage_subparsers.add_parser("cors", help="Configure bucket CORS")
    cors_parser.add_argument("--bucket", required=True, help="Bucket name")
    cors_parser.add_argument("--endpoint", help="S3 endpoint URL")

    sync_parser = storage_subparsers.add_parser("sync", help="Sync with rclone")
    sync_parser.add_argument("--source", required=True, help="Source path")
    sync_parser.add_argument("--destination", required=True, help="Destination path")
    sync_parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

    setup_bucket_parser = storage_subparsers.add_parser("setup-bucket", help="Setup public bucket with CORS")
    setup_bucket_parser.add_argument("--bucket", required=True, help="Bucket name")
    setup_bucket_parser.add_argument("--remote", default="nrp", help="Rclone remote name (default: nrp)")
    setup_bucket_parser.add_argument("--endpoint", help="S3 endpoint URL (defaults to AWS_PUBLIC_ENDPOINT env var)")
    setup_bucket_parser.add_argument("--no-cors", action="store_true", help="Skip CORS configuration")
    setup_bucket_parser.add_argument("--verify", action="store_true", help="Verify bucket configuration after setup")

    # PMTiles tile-accurate column inspection (issue #140)
    pmtiles_cols_parser = subparsers.add_parser(
        "pmtiles-columns",
        help="Read tile-accurate STAC table:columns from a PMTiles footer")
    pmtiles_cols_parser.add_argument("source", help="Path / http(s):// / s3:// to a .pmtiles archive")
    pmtiles_cols_parser.add_argument("--layer", default=None, help="Restrict to a single vector layer id")
    pmtiles_cols_parser.add_argument("--full-metadata", action="store_true",
                                     help="Print the full PMTiles metadata JSON instead of table:columns")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        _dispatch(args)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _dispatch(args):
    if args.command == "vector":
        from .vector import process_vector_chunks
        from .vector.h3_tiling import parse_resolution_by_area
        # Parse parent resolutions from comma-separated string
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]
        resolution_by_area = (
            parse_resolution_by_area(args.resolution_by_area)
            if args.resolution_by_area else None
        )
        process_vector_chunks(
            input_url=args.input,
            output_url=args.output,
            chunk_id=args.chunk_id,
            h3_resolution=args.resolution,
            parent_resolutions=parent_res,
            chunk_size=args.chunk_size,
            intermediate_chunk_size=args.intermediate_chunk_size,
            id_column=args.id_column,
            resolution_by_area=resolution_by_area,
            plan_url=args.plan,
        )

    elif args.command == "vector-plan":
        from .vector.chunk_plan import run_plan, DEFAULT_CELLS_PER_CHUNK
        from .vector.h3_tiling import parse_resolution_by_area
        run_plan(
            input_url=args.input,
            output_url=args.output,
            h3_resolution=args.resolution,
            resolution_by_area=(parse_resolution_by_area(args.resolution_by_area)
                                if args.resolution_by_area else None),
            cells_per_chunk=(args.cells_per_chunk if args.cells_per_chunk is not None
                             else DEFAULT_CELLS_PER_CHUNK),
            max_chunks=args.max_chunks,
            max_features=args.max_features_per_chunk,
        )

    elif args.command == "mdim":
        from .mdim import MdimProcessor
        if args.fan_out == "time":
            if args.unit_index is None:
                raise SystemExit("--fan-out time needs --unit-index")
            if args.h0_index is not None or args.chunk_index is not None:
                raise SystemExit("--fan-out time covers every h0; drop --h0-index/--chunk-index")
            index = args.unit_index
        else:
            if args.h0_index is not None and args.chunk_index is not None:
                raise SystemExit("give --h0-index or --chunk-index, not both")
            index = args.chunk_index if args.chunk_index is not None else args.h0_index
            if index is None:
                raise SystemExit("--h0-index or --chunk-index is required")
        processor = MdimProcessor(
            inputs=args.inputs,
            variables=args.variables,
            output_parquet_path=args.output_parquet,
            h3_resolution=args.resolution,
            parent_resolutions=[int(x) for x in args.parent_resolutions.split(",") if x.strip()],
            chunk_resolution=args.chunk_resolution,
            h0_subset=_resolve_h0_subset(args),
            hex_resampling=args.hex_resampling,
            time_agg=args.time_agg,
            time_start=args.time_start,
            time_end=args.time_end,
            placement=args.placement,
            allow_empty_window=args.fan_out == "time",
        )
        if args.fan_out == "time":
            processor.process_region(index)
        else:
            processor.process_chunk(index)

    elif args.command == "mdim-workflow":
        from .k8s import generate_mdim_workflow
        generate_mdim_workflow(
            dataset_name=args.dataset,
            inputs=args.inputs,
            variables=args.variables,
            bucket=args.bucket,
            output_dir=args.output_dir,
            namespace=args.namespace,
            h3_resolution=args.h3_resolution,
            parent_resolutions=[int(x) for x in args.parent_resolutions.split(",") if x.strip()],
            hex_resampling=args.hex_resampling,
            time_agg=args.time_agg,
            time_start=args.time_start,
            time_end=args.time_end,
            placement=args.placement,
            hex_memory=args.hex_memory,
            hex_cpu=args.hex_cpu,
            hex_storage=args.hex_storage,
            max_parallelism=args.max_parallelism,
            h0_subset=_resolve_h0_subset(args),
            chunk_resolution=args.chunk_resolution,
            hex_retries=args.hex_retries,
            max_failed_indexes=args.max_failed_indexes,
            merge_memory=args.merge_memory,
            merge_storage=args.merge_storage,
            validate_source=not args.no_validate_source,
            fan_out=args.fan_out,
            time_unit_steps=args.time_unit_steps,
            backend=args.backend,
            armada_queue=args.armada_queue,
            armada_priority_class=args.armada_priority_class,
            profile=args.profile,
        )

    elif args.command == "raster":
        from .raster import RasterProcessor, create_mosaic_cog

        # Parse parent resolutions
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]

        # Parse optional mosaic parameters
        target_extent = None
        if getattr(args, 'target_extent', None):
            parts = [float(x) for x in args.target_extent.split(',')]
            target_extent = tuple(parts)

        raster_h0_subset = _resolve_h0_subset(args)

        input_path = args.inputs if len(args.inputs) > 1 else args.inputs[0]

        # If multiple inputs and only --output-cog requested, use create_mosaic_cog directly
        if isinstance(input_path, list) and args.output_cog and not args.output_parquet:
            # Categorical sources (--hex-resampling mode/fractions) must not
            # average class codes in the COG overviews (issue #108).
            overview_resampling = (
                "mode" if args.hex_resampling in ("mode", "fractions") else "average"
            )
            create_mosaic_cog(
                source_urls=input_path,
                output_path=args.output_cog,
                target_crs=getattr(args, 'target_crs', 'EPSG:4326'),
                target_extent=target_extent,
                target_resolution=getattr(args, 'target_resolution', None),
                band=getattr(args, 'band', None),
                nodata=args.nodata,
                resampling=args.resampling,
                compression=args.compression,
                overview_resampling=overview_resampling,
            )
        else:
            processor = RasterProcessor(
                input_path=input_path,
                output_cog_path=args.output_cog,
                output_parquet_path=args.output_parquet,
                h3_resolution=args.resolution,
                parent_resolutions=parent_res,
                h0_index=args.h0_index,
                chunk_resolution=args.chunk_resolution,
                chunk_index=args.chunk_index,
                window_reads=args.window_reads,
                h0_subset=raster_h0_subset,
                value_column=args.value_column,
                nodata_value=args.nodata,
                compression=args.compression,
                blocksize=args.blocksize,
                resampling=args.resampling,
                hex_resampling=args.hex_resampling,
                method=getattr(args, 'method', 'exact-extract'),
                target_crs=getattr(args, 'target_crs', 'EPSG:4326'),
                target_extent=target_extent,
                target_resolution=getattr(args, 'target_resolution', None),
                band=getattr(args, 'band', None),
                local_cache_dir=getattr(args, 'local_cache_dir', '/tmp/cng-raster-cache'),
            )

            if args.output_cog:
                processor.create_cog()

            if args.output_parquet:
                if args.chunk_index is not None or args.h0_index is not None:
                    processor.process_chunk()
                elif args.chunk_resolution:
                    raise ValueError(
                        "--chunk-resolution needs a --chunk-index: there is no "
                        "process-everything mode for sub-h0 chunks, which exist "
                        "precisely so each unit runs in its own pod."
                    )
                else:
                    processor.process_all_h0_regions()

    elif args.command == "merge-chunks":
        from .raster.merge import merge_raster_chunks
        merge_raster_chunks(
            chunks_dir=args.chunks_dir,
            output_dir=args.output_dir,
            cleanup=args.cleanup,
            memory_limit=args.memory_limit,
            expect_chunks=args.expect_chunks,
        )

    elif args.command == "gapfill":
        from .raster.merge import generate_gapfill
        missing = generate_gapfill(
            chunks_dir=args.chunks_dir,
            expect_chunks=args.expect_chunks,
            hex_manifest=args.hex_manifest,
            output_path=args.output,
            queue=args.queue,
            job_set_id=args.job_set_id,
            priority_class=args.armada_priority_class,
        )
        # Non-zero when there were gaps, so a pipeline step can branch on it
        # without parsing the output.
        if missing:
            return 1

    elif args.command == "repartition":
        from .vector import repartition_by_h0
        repartition_by_h0(
            chunks_dir=args.chunks_dir,
            output_dir=args.output_dir,
            source_parquet=args.source_parquet,
            cleanup=args.cleanup,
            memory_limit=args.memory_limit,
        )

    elif args.command == "k8s":
        from .k8s import K8sJobManager
        manager = K8sJobManager(namespace=getattr(args, 'namespace', 'biodiversity'))
        if args.chunks:
            job_spec = manager.generate_chunked_job(
                job_name=args.job_name,
                script_path=args.container_command[0],
                num_chunks=args.chunks,
            )
        else:
            job_spec = manager.generate_job_yaml(
                job_name=args.job_name,
                command=args.container_command,
            )
        manager.save_job_yaml(job_spec, args.output)

    elif args.command == "sync-job":
        from .k8s import generate_sync_job
        generate_sync_job(
            job_name=args.job_name,
            source=args.source,
            destination=args.destination,
            output_file=args.output,
            namespace=args.namespace,
            cpu=args.cpu,
            memory=args.memory,
            dry_run=args.dry_run,
        )

    elif args.command == "workflow":
        from .k8s import generate_dataset_workflow
        from .vector.h3_tiling import parse_resolution_by_area
        # Parse parent resolutions from comma-separated string
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]
        if args.resolution_by_area and args.h3_resolution is not None:
            raise ValueError("--resolution-by-area and --h3-resolution are mutually exclusive")
        # Validate the spec early so workflow generation fails fast on a bad bin.
        if args.resolution_by_area:
            parse_resolution_by_area(args.resolution_by_area)
        generate_dataset_workflow(
            dataset_name=args.dataset,
            source_urls=args.source_urls,
            bucket=args.bucket,
            output_dir=args.output_dir,
            namespace=args.namespace,
            h3_resolution=args.h3_resolution,
            resolution_by_area=args.resolution_by_area,
            parent_resolutions=parent_res,
            id_column=args.id_column,
            layer=args.layer,
            hex_memory=args.hex_memory,
            max_parallelism=args.max_parallelism,
            max_completions=args.max_completions,
            chunk_size=args.chunk_size,
            cells_per_chunk=args.cells_per_chunk,
            hex_retries=args.hex_retries,
            max_failed_indexes=args.max_failed_indexes,
            intermediate_chunk_size=args.intermediate_chunk_size,
            row_group_size=args.row_group_size,
            simplify_tolerance=args.simplify_tolerance,
            trim_strings=args.trim_strings,
            lat_column=args.lat_column,
            lon_column=args.lon_column,
            expect_features=args.expect_features,
            backend=args.backend,
            armada_priority_class=args.armada_priority_class,
            armada_queue=args.armada_queue,
            hex_storage=args.hex_storage,
            repartition_storage=args.repartition_storage,
            repartition_memory=args.repartition_memory,
            profile=args.profile,
            s3_endpoint=args.s3_endpoint,
            s3_public_endpoint=args.s3_public_endpoint,
            s3_secret_name=args.s3_secret_name,
            rclone_secret_name=args.rclone_secret_name,
            rclone_remote=args.rclone_remote,
            priority_class=args.priority_class,
            node_affinity=args.node_affinity,
        )

    elif args.command == "raster-workflow":
        from .k8s import generate_raster_workflow
        # Parse parent resolutions
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]
        # Parse optional mosaic parameters
        target_extent = None
        if getattr(args, 'target_extent', None):
            parts = [float(x) for x in args.target_extent.split(',')]
            target_extent = tuple(parts)
        h0_subset = _resolve_h0_subset(args)
        # Only forward the sizing knobs that were actually given, so the
        # generator's own defaults stay the single source of truth for them.
        hex_sizing = {
            name: value
            for name, value in (
                ("hex_cpu", args.hex_cpu),
                ("hex_workers", args.hex_workers),
                ("hex_chunk_size", args.hex_chunk_size),
            )
            if value is not None
        }
        generate_raster_workflow(
            dataset_name=args.dataset,
            source_urls=args.source_urls,
            bucket=args.bucket,
            output_dir=args.output_dir,
            namespace=args.namespace,
            h3_resolution=args.h3_resolution,
            parent_resolutions=parent_res,
            value_column=args.value_column,
            nodata_value=args.nodata,
            hex_resampling=args.hex_resampling,
            hex_memory=args.hex_memory,
            max_parallelism=args.max_parallelism,
            h0_subset=h0_subset,
            hex_retries=args.hex_retries,
            max_failed_indexes=args.max_failed_indexes,
            hex_storage=args.hex_storage,
            **hex_sizing,
            chunk_resolution=args.chunk_resolution,
            max_hex_memory=args.max_hex_memory,
            merge_memory=args.merge_memory,
            merge_storage=args.merge_storage,
            cog_storage=args.cog_storage,
            target_extent=target_extent,
            target_resolution=getattr(args, 'target_resolution', None),
            band=getattr(args, 'band', None),
            output_cog_name=getattr(args, 'output_cog_name', None),
            backend=args.backend,
            armada_priority_class=args.armada_priority_class,
            armada_queue=args.armada_queue,
            profile=args.profile,
            s3_endpoint=args.s3_endpoint,
            s3_public_endpoint=args.s3_public_endpoint,
            s3_secret_name=args.s3_secret_name,
            rclone_secret_name=args.rclone_secret_name,
            rclone_remote=args.rclone_remote,
            priority_class=args.priority_class,
            node_affinity=args.node_affinity,
        )

    elif args.command == "storage":
        if args.storage_command == "cors":
            from .storage import configure_bucket_cors
            configure_bucket_cors(
                bucket_name=args.bucket,
                endpoint_url=args.endpoint,
            )
        elif args.storage_command == "sync":
            from .storage import RcloneSync
            syncer = RcloneSync(dry_run=args.dry_run)
            syncer.sync(args.source, args.destination)
        elif args.storage_command == "setup-bucket":
            from .storage import setup_public_bucket
            from .storage.setup_bucket import verify_bucket_config
            import json

            success = setup_public_bucket(
                bucket_name=args.bucket,
                remote=args.remote,
                endpoint=args.endpoint,
                set_cors=not args.no_cors,
                verbose=True
            )

            if success and args.verify:
                print("\nVerifying configuration...")
                results = verify_bucket_config(args.bucket, args.endpoint)
                print(json.dumps(results, indent=2))

            sys.exit(0 if success else 1)

    elif args.command == "pmtiles-columns":
        from .vector.pmtiles import read_pmtiles_metadata, pmtiles_table_columns
        import json
        if args.full_metadata:
            print(json.dumps(read_pmtiles_metadata(args.source), indent=2))
        else:
            print(json.dumps(pmtiles_table_columns(args.source, layer=args.layer), indent=2))


if __name__ == "__main__":
    sys.exit(main() or 0)
