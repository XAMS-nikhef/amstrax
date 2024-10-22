import argparse
import logging
import os, sys, json
from datetime import datetime
from amstrax.auto_processing_new.job_submission import submit_job

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SETUP_FILE = '/data/xenon/xams_v2/setup.sh'

def parse_args():
    """
    Parse command-line arguments for offline job submission.
    """
    parser = argparse.ArgumentParser(description="Offline processing of XAMS data (job submission)")

    # Arguments for selecting runs
    # We should make it a group, so that only one of them can be used
    run_selection = parser.add_mutually_exclusive_group(required=True)
    run_selection.add_argument("--run_id", type=str, nargs="*", help="Specific run ID(s) to process.")
    run_selection.add_argument("--run_file", type=str, help="File with run IDs to process. It should contain one run ID per line.")

    # Arguments for processing
    parser.add_argument(
        "--targets",
        nargs="+",
        default=["peak_basics", "event_info"],
        help="List of targets to process (e.g., raw_records, peaks, events).",
    )
    parser.add_argument("--output_folder", type=str, default=None, help="Path to output folder.")
    parser.add_argument("--mem", default=8000, help="Memory per CPU")
    parser.add_argument("--logs_path", help="Path where to save logs")
    parser.add_argument("--amstrax_path", default=None, help="Version of amstrax to use.")
    parser.add_argument("--corrections_version", default=None, help="Version of corrections to use. Can be ONLINE, v0, v1..")
    parser.add_argument("--production", action="store_true", help="Run in production mode (update the rundb).")
    parser.add_argument("--dry_run", action="store_true", help="Simulate job submission without actually submitting jobs.")

    return parser.parse_args()


def check_for_production(args):
    """
    Check if the user really wants to run in production mode.
    """
    if args.production:

        # Some constraints for production mode
        # only if corrections_version is set
        # only if amstrax_path contains amstrax_versioned
        # only if output_folder is not set

        if not args.corrections_version:
            log.error("In production mode, you must specify a corrections version.")
            raise ValueError("Corrections version not specified in production mode.")

        if not args.amstrax_path or "amstrax_versioned" not in args.amstrax_path:
            log.error("In production mode, you must specify a versioned amstrax path.")
            raise ValueError("Amstrax path not specified in production mode.")

        if args.output_folder:
            log.error("In production mode, you must not specify an output folder. We take them from .xams_config")
            raise ValueError("Output folder specified in production mode.")

        log.warning("You are about to run in production mode. This will update the rundb and write to the official output folder.")
        log.warning("Are you sure you want to continue? (y/n)")
        answer = input()
        if answer.lower() != "y":
            log.error("Production mode was not confirmed. Exiting.")
            sys.exit(1)

    if not args.production:
        # if xams_processed in output_folder, not allowed
        if args.output_folder and "xams_processed" in args.output_folder:
            log.error("You are not allowed to write to the xams_processed folder.")
            raise ValueError("Output folder is xams_processed.")

        if not args.output_folder:
            log.error("You must specify an output folder.")
            raise ValueError("Output folder not specified.")

def main(args):
    """
    Main function for offline job submission of selected runs.
    """

    # Check the run selection method
    if args.run_id:
        run_docs = [{"number": int(run_id)} for run_id in args.run_id]
    elif args.run_file:
        # It should contain a list of run numbers, one per line
        with open(args.run_file, "r") as f:
            run_numbers = f.readlines()
        run_docs = [{"number": int(run_number)} for run_number in run_numbers]
    else:
        log.error("Either --run_id or --run_file must be provided.")
        return


    logs_path = args.logs_path
    if args.production:
        logs_path = amstrax.get_xams_config("logs_path")


    # Submit jobs for each run
    for run_doc in run_docs:
        run_id = f'{int(run_doc["number"]):06}'

        # Build the job submission command
        jobname = f"process_{run_id}"
        if args.corrections_version:
            jobname += f"_{args.corrections_version}"
        if args.production:
            jobname += os.basename(args.amsrtax_path)

        arguments = []
        arguments.append(f"--run_id {run_id}")
        arguments.append(f"--targets {' '.join(args.targets)}")
        arguments.append(f"--output_folder {args.output_folder}")
        if args.corrections_version:
            arguments.append(f"--corrections_version {args.corrections_version}")
        if args.amstrax_path:
            arguments.append(f"--amstrax_path {args.amstrax_path}")
        if args.production:
            arguments.append("--production")
        
        arguments = ' '.join(arguments)

        job_command = f"""
        echo "Processing run {run_id} at $(date)"
        source {SETUP_FILE}
        cd {os.path.dirname(os.path.realpath(__file__))}
        pwd
        python processing.py {arguments}
        echo "Finished run {run_id} at $(date)"
        """

        # Submit the job
        submit_job(
            jobstring=job_command,
            jobname=jobname,
            log_dir=logs_path,
            queue="short",
            mem_per_cpu=args.mem,
            cpus_per_task=1,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    args = parse_args()
    main(args)
