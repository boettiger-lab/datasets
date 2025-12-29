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
    vector_parser.add_argument("--chunk-size", type=int, default=500, help="Chunk size")
    vector_parser.add_argument("--chunk-id", type=int, help="Process specific chunk")
    vector_parser.add_argument("--parent-resolutions", type=str, default="9,8,0", help="Comma-separated parent H3 resolutions (default: '9,8,0')")
    
    # Raster processing command
    raster_parser = subparsers.add_parser("raster", help="Process raster datasets")
    raster_parser.add_argument("--input", required=True, help="Input raster file")
    raster_parser.add_argument("--output-cog", required=True, help="Output COG file")
    raster_parser.add_argument("--output-parquet", help="Output parquet directory")
    raster_parser.add_argument("--resolution", type=int, default=10, help="H3 resolution")
    
    # Repartition command
    repartition_parser = subparsers.add_parser("repartition", help="Repartition chunks by h0")
    repartition_parser.add_argument("--chunks-dir", required=True, help="Input chunks directory URL")
    repartition_parser.add_argument("--output-dir", required=True, help="Output directory URL")
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
    workflow_parser.add_argument("--output-dir", default=".", help="Output directory for YAML files")
    workflow_parser.add_argument("--namespace", default="biodiversity", help="Kubernetes namespace (default: biodiversity)")
    workflow_parser.add_argument("--h3-resolution", type=int, default=10, help="Target H3 resolution (default: 10)")
    workflow_parser.add_argument("--parent-resolutions", type=str, default="9,8,0", help="Comma-separated parent H3 resolutions (default: '9,8,0')")
    
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
        )
    
    elif args.command == "raster":
        from .raster import RasterProcessor
        processor = RasterProcessor(
            input_path=args.input,
            output_cog_path=args.output_cog,
            output_parquet_path=args.output_parquet or "",
            h3_resolution=args.resolution,
        )
        processor.create_cog()
        if args.output_parquet:
            processor.raster_to_h3_parquet()
    
    elif args.command == "repartition":
        from .vector import repartition_by_h0
        repartition_by_h0(
            chunks_dir=args.chunks_dir,
            output_dir=args.output_dir,
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
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
