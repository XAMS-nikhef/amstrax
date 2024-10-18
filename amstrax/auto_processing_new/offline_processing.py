import argparse
import logging
import os, sys, json
from datetime import datetime
from job_submission import submit_job

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments for offline job submission.
    """
    parser = argparse.ArgumentParser(description="Offline processing of XAMS data (job submission)")

    # Arguments for selecting runs
    parser.add_argument("--run_id", type=str, nargs="*", help="Specific run ID(s) to process.")
    parser.add_argument("--run_query", type=str, help="Query string to select runs from rundb (e.g., by date).")

    # Arguments for processing
    parser.add_argument(
        "--targets",
        nargs="+",
        default=["peaks", "events"],
        help="List of targets to process (e.g., raw_records, peaks, events).",
    )
    parser.add_argument(
        "--raw_records_folder",
        type=str,
        default="/data/xenon/xams_data/raw_records",
        help="Path to raw records folder.",
    )
    parser.add_argument(
        "--output_folder", type=str, default="/data/xenon/xams_v2/xams_processed", help="Path to output folder."
    )

    # Job submission and processing options
    parser.add_argument("--mem", default=8000, help="Memory per CPU")
    parser.add_argument("--logs_path", default="/data/xenon/xams_v2/logs/", help="Path where to save logs")
    parser.add_argument("--production", action="store_true", help="Run in production mode (update the rundb).")
    parser.add_argument(
        "--dry_run", action="store_true", help="Simulate job submission without actually submitting jobs."
    )
    parser.add_argument(
        "--amstrax_dir", default="/data/xenon/xams_v2/software/amstrax", help="Path to amstrax directory."
    )

    return parser.parse_args()


def main(args):
    """
    Main function for offline job submission of selected runs.
    """
    # Select runs either by run_id or using a custom query
    if args.run_id:
        run_docs = [{"number": int(run_id)} for run_id in args.run_id]
    elif args.run_query:
        raise NotImplementedError("Querying runs from rundb is not yet implemented.")
    else:
        log.error("Either --run_id or --run_query must be provided.")
        return

    # Submit jobs for each run
    for run_doc in run_docs:
        run_id = f'{int(run_doc["number"]):06}'


        # Build the job submission command
        # Also insert the amstrax dir on top of the PYTHONPATH
        job_command = f"""
        echo "Processing run {run_id} at $(date)"
        export PYTHONPATH={args.amstrax_dir}:$PYTHONPATH
        python {args.amstrax_dir}/amstrax/auto_processing_new/test_connection.py
        python {args.amstrax_dir}/amstrax/auto_processing_new/processing.py --run_id {run_id} \
                             --targets {' '.join(args.targets)} \
                             --raw_records_folder {args.raw_records_folder} \
                             --output_folder {args.output_folder} \
                             {'--production' if args.production else ''}
        """

        # Submit the job
        submit_job(
            jobstring=job_command,
            jobname=f"xams_{run_id}_offline",
            log_dir=args.logs_path,
            queue="short",
            mem_per_cpu=args.mem,
            cpus_per_task=1,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    args = parse_args()
    main(args)
