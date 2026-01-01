import argparse
import os
import sys
import psutil
import time

# Add parent directory to path to import cng_datasets
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from cng_datasets.raster import RasterProcessor


def main():
    parser = argparse.ArgumentParser(
        description="Process raster tile for a given h0 hex index"
    )
    parser.add_argument(
        "--i", 
        type=int, 
        required=True, 
        help="H0 hex index to process (0-121, matches column i in h0-valid.parquet)"
    )
    parser.add_argument(
        "--zoom", 
        type=int, 
        default=None, 
        help="H3 resolution to aggregate to (default: auto-detect from raster)"
    )
    parser.add_argument(
        "--parent-resolutions",
        type=str,
        default="0",
        help="Comma-separated parent resolutions (e.g., '9,8,0')"
    )
    parser.add_argument(
        "--input-url", 
        default="https://minio.carlboettiger.info/public-wetlands/GLWD_v2_0/GLWD_v2_0_combined_classes/GLWD_v2_0_main_class.tif",
        help="Input raster URL (http/https or /vsis3/ path)"
    )
    parser.add_argument(
        "--output-url", 
        default="s3://public-wetlands/hex/",
        help="Output parquet base path (e.g., s3://bucket/dataset/hex/)"
    )
    parser.add_argument(
        "--value-column",
        default="value",
        help="Name for the raster value column (default: 'value')"
    )
    parser.add_argument(
        "--nodata",
        type=float,
        default=None,
        help="NoData value to exclude (e.g., 65535 for many rasters)"
    )
    parser.add_argument(
        "--profile", 
        action="store_true", 
        help="Enable memory and runtime profiling"
    )
    
    args = parser.parse_args()
    
    if args.profile:
        start_time = time.time()
    
    # Parse parent resolutions
    parent_resolutions = [int(r.strip()) for r in args.parent_resolutions.split(',') if r.strip()]
    
    print(f"=" * 60)
    print(f"Raster H3 Processing Job")
    print(f"=" * 60)
    print(f"H0 Index: {args.i}")
    print(f"Input: {args.input_url}")
    print(f"Output: {args.output_url}")
    
    if args.zoom is not None:
        print(f"H3 Resolution: {args.zoom} (specified)")
    else:
        print(f"H3 Resolution: auto-detect")
    
    print(f"Parent Resolutions: {parent_resolutions}")
    print(f"Value Column: {args.value_column}")
    
    if args.nodata is not None:
        print(f"NoData Value: {args.nodata}")
    
    print(f"=" * 60)
    print("", flush=True)
    
    # Create processor
    processor = RasterProcessor(
        input_path=args.input_url,
        output_parquet_path=args.output_url,
        h3_resolution=args.zoom,  # None = auto-detect
        parent_resolutions=parent_resolutions,
        h0_index=args.i,
        value_column=args.value_column,
        nodata_value=args.nodata,
    )
    
    # Process the h0 region
    output_file = processor.process_h0_region()
    
    if output_file:
        print(f"\n✓ Successfully processed h0 region {args.i}")
        print(f"  Output: {output_file}")
    else:
        print(f"\n⚠ No data in h0 region {args.i}")
    
    if args.profile:
        end_time = time.time()
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        print(f"\nProfiling:")
        print(f"  Maximum RAM: {mem_info.rss / 1024**2:.2f} MiB")
        print(f"  Runtime: {end_time - start_time:.2f} seconds")
    
    print(f"\n{'=' * 60}")
    print(flush=True)


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
