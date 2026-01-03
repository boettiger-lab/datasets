"""
Command-line interface for cng-datasets toolkit.
"""

import argparse
import sys


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
    vector_parser.add_argument("--resolution", type=int, default=10, help="H3 resolution")
    vector_parser.add_argument("--chunk-size", type=int, default=500, help="Number of rows to process in pass 1 (geometry to H3 arrays)")
    vector_parser.add_argument("--intermediate-chunk-size", type=int, default=10, help="Number of rows to process in pass 2 (unnesting arrays) - reduce if hitting OOM")
    vector_parser.add_argument("--chunk-id", type=int, help="Process specific chunk")
    vector_parser.add_argument("--parent-resolutions", type=str, default="9,8,0", help="Comma-separated parent H3 resolutions (default: '9,8,0')")
    vector_parser.add_argument("--id-column", help="ID column name (auto-detected if not specified)")

    
    # Raster processing command
    raster_parser = subparsers.add_parser("raster", help="Process raster datasets")
    raster_parser.add_argument("--input", required=True, help="Input raster file (local or /vsis3/ URL)")
    raster_parser.add_argument("--output-cog", help="Output COG file path")
    raster_parser.add_argument("--output-parquet", help="Output parquet directory (e.g., s3://bucket/dataset/hex/)")
    raster_parser.add_argument("--resolution", type=int, help="H3 resolution (auto-detected if not specified)")
    raster_parser.add_argument("--parent-resolutions", type=str, default="0", help="Comma-separated parent H3 resolutions (default: '0')")
    raster_parser.add_argument("--h0-index", type=int, help="Process specific h0 region (0-121), or omit to process all")
    raster_parser.add_argument("--value-column", default="value", help="Name for raster value column (default: 'value')")
    raster_parser.add_argument("--nodata", type=float, help="NoData value to exclude")
    raster_parser.add_argument("--compression", default="deflate", help="COG compression (deflate, lzw, zstd)")
    raster_parser.add_argument("--blocksize", type=int, default=512, help="COG block size (default: 512)")
    raster_parser.add_argument("--resampling", default="nearest", help="Resampling method (default: nearest)")
    
    # Repartition command
    repartition_parser = subparsers.add_parser("repartition", help="Repartition chunks by h0")
    repartition_parser.add_argument("--chunks-dir", required=True, help="Input chunks directory URL")
    repartition_parser.add_argument("--output-dir", required=True, help="Output directory URL")
    repartition_parser.add_argument("--source-parquet", required=True, help="Source parquet with full attributes")
    repartition_parser.add_argument("--cleanup", action="store_true", default=True, help="Remove chunks after repartitioning")
    
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
    workflow_parser.add_argument("--source-url", required=True, help="Source data URL")
    workflow_parser.add_argument("--bucket", required=True, help="S3 bucket for outputs")
    workflow_parser.add_argument("--output-dir", default="k8s", help="Output directory for YAML files")
    workflow_parser.add_argument("--namespace", default="biodiversity", help="Kubernetes namespace (default: biodiversity)")
    workflow_parser.add_argument("--h3-resolution", type=int, default=10, help="Target H3 resolution (default: 10)")
    workflow_parser.add_argument("--parent-resolutions", type=str, default="9,8,0", help="Comma-separated parent H3 resolutions (default: '9,8,0')")
    workflow_parser.add_argument("--id-column", help="ID column name (auto-detected if not specified)")
    workflow_parser.add_argument("--hex-memory", type=str, default="8Gi", help="Memory per hex job pod (default: 8Gi)")
    workflow_parser.add_argument("--max-parallelism", type=int, default=50, help="Maximum parallel hex jobs (default: 50)")
    workflow_parser.add_argument("--max-completions", type=int, default=200, help="Maximum hex job completions (default: 200, increase to reduce chunk size/memory)")
    workflow_parser.add_argument("--intermediate-chunk-size", type=int, default=10, help="Number of rows to process in pass 2 (unnesting arrays) - reduce if hitting OOM")
    
    # Raster workflow generation command
    raster_workflow_parser = subparsers.add_parser("raster-workflow", help="Generate complete raster dataset workflow")
    raster_workflow_parser.add_argument("--dataset", required=True, help="Dataset name")
    raster_workflow_parser.add_argument("--source-url", required=True, help="Source COG URL")
    raster_workflow_parser.add_argument("--bucket", required=True, help="S3 bucket for outputs")
    raster_workflow_parser.add_argument("--output-dir", default="k8s", help="Output directory for YAML files")
    raster_workflow_parser.add_argument("--namespace", default="biodiversity", help="Kubernetes namespace")
    raster_workflow_parser.add_argument("--h3-resolution", type=int, default=8, help="Target H3 resolution (default: 8)")
    raster_workflow_parser.add_argument("--parent-resolutions", type=str, default="0", help="Comma-separated parent H3 resolutions (default: '0')")
    raster_workflow_parser.add_argument("--value-column", default="value", help="Name for raster value column")
    raster_workflow_parser.add_argument("--nodata", type=float, help="NoData value to exclude")
    raster_workflow_parser.add_argument("--hex-memory", type=str, default="32Gi", help="Memory per hex job pod (default: 32Gi)")
    raster_workflow_parser.add_argument("--max-parallelism", type=int, default=61, help="Maximum parallel hex jobs (default: 61)")
    
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
    
    args = parser.parse_args()
    
    if args.command == "vector":
        from .vector import process_vector_chunks
        # Parse parent resolutions from comma-separated string
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]
        process_vector_chunks(
            input_url=args.input,
            output_url=args.output,
            chunk_id=args.chunk_id,
            h3_resolution=args.resolution,
            parent_resolutions=parent_res,
            chunk_size=args.chunk_size,
            intermediate_chunk_size=args.intermediate_chunk_size,
            id_column=args.id_column,
        )
    
    elif args.command == "raster":
        from .raster import RasterProcessor
        
        # Parse parent resolutions
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]
        
        processor = RasterProcessor(
            input_path=args.input,
            output_cog_path=args.output_cog,
            output_parquet_path=args.output_parquet,
            h3_resolution=args.resolution,
            parent_resolutions=parent_res,
            h0_index=args.h0_index,
            value_column=args.value_column,
            nodata_value=args.nodata,
            compression=args.compression,
            blocksize=args.blocksize,
            resampling=args.resampling,
        )
        
        # Create COG if output path specified
        if args.output_cog:
            processor.create_cog()
        
        # Process to H3 parquet if output path specified
        if args.output_parquet:
            if args.h0_index is not None:
                # Process single h0 region
                processor.process_h0_region()
            else:
                # Process all h0 regions
                processor.process_all_h0_regions()
    
    elif args.command == "repartition":
        from .vector import repartition_by_h0
        repartition_by_h0(
            chunks_dir=args.chunks_dir,
            output_dir=args.output_dir,
            source_parquet=args.source_parquet,
            cleanup=args.cleanup,
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
        # Parse parent resolutions from comma-separated string
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]
        generate_dataset_workflow(
            dataset_name=args.dataset,
            source_url=args.source_url,
            bucket=args.bucket,
            output_dir=args.output_dir,
            namespace=args.namespace,
            h3_resolution=args.h3_resolution,
            parent_resolutions=parent_res,
            id_column=args.id_column,
            hex_memory=args.hex_memory,
            max_parallelism=args.max_parallelism,
            max_completions=args.max_completions,
            intermediate_chunk_size=args.intermediate_chunk_size,
        )
    
    elif args.command == "raster-workflow":
        from .k8s import generate_raster_workflow
        # Parse parent resolutions
        parent_res = [int(x.strip()) for x in args.parent_resolutions.split(',') if x.strip()]
        generate_raster_workflow(
            dataset_name=args.dataset,
            source_url=args.source_url,
            bucket=args.bucket,
            output_dir=args.output_dir,
            namespace=args.namespace,
            h3_resolution=args.h3_resolution,
            parent_resolutions=parent_res,
            value_column=args.value_column,
            nodata_value=args.nodata,
            hex_memory=args.hex_memory,
            max_parallelism=args.max_parallelism,
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
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
