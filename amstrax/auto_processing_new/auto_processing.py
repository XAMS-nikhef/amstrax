import argparse
import time
import os
import logging
from datetime import datetime, timedelta

from job_submission import submit_job
from db_utils import query_runs, update_processing_status

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SETUP_FILE = "/data/xenon/xams_v2/setup.sh"

def parse_args():
    """
    Parse command-line arguments for online auto-processing.
    """
    parser = argparse.ArgumentParser(description="Autoprocess XAMS live data")
    parser.add_argument(
        "--target",
        nargs="*",
        default=["raw_records"],
        help="Target final data type to produce (e.g., raw_records, peaks, events).",
    )
    parser.add_argument("--output_folder", default="/data/xenon/xams_v2/xams_processed", help="Path where to save processed data")
    parser.add_argument("--timeout", default=20, type=int, help="Time (in seconds) between checks.")
    parser.add_argument("--max_jobs", default=5, type=int, help="Maximum number of jobs to submit simultaneously.")
    parser.add_argument("--run_id", default=None, help="Single run ID to process manually.")
    parser.add_argument("--mem", default=8000, type=int, help="Memory per CPU in MB.")
    parser.add_argument("--logs_path", default="/data/xenon/xams_v2/logs/", help="Path to save job logs.")
    parser.add_argument("--production", action="store_true", help="Run in production mode (will update the rundb).")
    parser.add_argument("--set_config_kwargs", default="{}", help="Dictionary of kwargs to pass to set_config.")
    parser.add_argument("--set_context_kwargs", default="{}", help="Dictionary of kwargs to pass to set_context.")
    parser.add_argument("--amstrax_path", default=None, help="Path to the amstrax directory.")
    parser.add_argument("--corrections_version", default=None, help="Version of corrections to use.")
    parser.add_argument("--queue", default="short", help="Queue to submit jobs to. See Nikhef docs for options.")

    return parser.parse_args()


def main(args):
    """
    Main function for continuous auto-processing of XAMS live data.
    """
    log.info("Starting XAMS auto-processing script...")
    amstrax_dir = amstrax.amstrax_dir  # Path to the amstrax directory
    nap_time = int(args.timeout)
    output_folder = args.output_folder
    targets = " ".join(args.target)
    runs_col = amstrax.get_mongo_collection()

    client = amstrax.get_mongo_client()
    processing_db = client["daq"]["processing"]

    while True:
        # Check if auto-processing is enabled
        auto_processing_on = processing_db.find_one({"name": "auto_processing"})["status"] == "on"

        # Update task list and check for new runs
        run_docs_to_do = update_task_list(args, runs_col, auto_processing_on)

        # Handle running jobs
        handle_running_jobs(runs_col, production=args.production)

        if not run_docs_to_do:
            log.info(f"No runs to process. Sleeping for {nap_time} seconds.")
            time.sleep(nap_time)
            continue

        # Submit new jobs if below max limit
        submit_new_jobs(args, runs_col, run_docs_to_do, amstrax_dir)

        if args.run_id:
            log.info("Finished processing run.")
            break

        log.info(f"Sleeping for {nap_time} seconds before next check...")
        time.sleep(nap_time)


def update_task_list(args, runs_col, auto_processing_on):
    """
    Update and return the list of tasks to be processed based on MongoDB queries.
    """
    query = {
        "data": {"$elemMatch": {"type": "live", "host": "stbc"}},
        "$or": [
            {
                "data": {"$not": {"$elemMatch": {"host": "stbc", "type": "raw_records"}}},
                "processing_failed": {"$not": {"$gt": 3}},
                "processing_status.status": {"$not": {"$in": ["running", "submitted"]}},
                "tags": {"$not": {"$elemMatch": {"name": "abandon"}}},
                "start": {"$gt": datetime.today() - timedelta(days=100)},
            },
            {"tags": {"$elemMatch": {"name": "process"}}},
        ],
    }

    if not auto_processing_on:
        query = {"data": {"$elemMatch": {"type": "live", "host": "stbc"}}, "tags": {"$elemMatch": {"name": "process"}}}

    projection = {"number": 1, "start": 1, "end": 1, "data": 1, "processing_status": 1, "processing_failed": 1}
    sort = [("number", -1)]
    run_docs_to_do = list(runs_col.find(query, projection).sort(sort))

    if args.run_id:
        run_docs_to_do = [runs_col.find_one({"number": int(args.run_id)}, projection)]

    if run_docs_to_do:
        log.info(f'Found {len(run_docs_to_do)} runs to process: {[run_doc["number"] for run_doc in run_docs_to_do]}')
    return run_docs_to_do


def handle_running_jobs(runs_col, production=False):
    """
    Check and update the status of running jobs. Mark jobs as failed if they've been running too long.
    """
    query = {"processing_status.status": {"$in": ["submitted", "running"]}}
    projection = {"number": 1, "processing_status": 1}
    run_docs_running = list(runs_col.find(query, projection))

    for run_doc in run_docs_running:
        processing_status = run_doc["processing_status"]
        run_number = run_doc["number"]

        # Mark jobs as failed if they’ve been running or submitted for over 30 minutes
        if processing_status["status"] in ["running", "submitted"]:
            if processing_status["time"] < datetime.now() - timedelta(minutes=30):
                new_status = "failed"
                log.info(
                    f'Run {run_number} has been {processing_status["status"]} for more than 30 minutes, marking as {new_status}'
                )

                if production:
                    update_processing_status(run_number, new_status, production=production, is_online=True)
                else:
                    log.info(f"Would have updated run {run_number} to {new_status}")


def submit_new_jobs(args, runs_col, run_docs_to_do, amstrax_dir):
    """
    Submit new jobs if the current number of running/submitted jobs is below the max_jobs limit.
    """
    query = {"processing_status.status": {"$in": ["submitted", "running"]}}
    projection = {"number": 1, "processing_status": 1}
    run_docs_running = list(runs_col.find(query, projection))

    num_running_jobs = len(run_docs_running)
    log.info(f"Found {num_running_jobs} running jobs.")

    if num_running_jobs >= args.max_jobs:
        log.info(f"Too many jobs running ({num_running_jobs}/{args.max_jobs}).")
        return

    max_jobs_to_submit = args.max_jobs - num_running_jobs
    will_do_run_ids = [int(run_doc["number"]) for run_doc in run_docs_to_do[:max_jobs_to_submit]]
    log.info(f"Submitting jobs for runs: {will_do_run_ids}")

    for run_doc in run_docs_to_do[:max_jobs_to_submit]:
        run_id = f'{int(run_doc["number"]):06}'
        job_name = f"process_{run_id}_online"

        production_flag = "--production" if args.production else ""
        targets = " ".join(args.target)


        arguments = []
        arguments.append(f"--run_id {run_id}")
        arguments.append(f"--targets {targets}")
        arguments.append(f"--output_folder {args.output_folder}")
        if args.corrections_version:
            arguments.append(f"--corrections_version {args.corrections_version}")
        if args.amstrax_path:
            arguments.append(f"--amstrax_path {args.amstrax_path}")
        if args.production:
            arguments.append("--production")
            arguments.append("--allow_raw_records")
            arguments.append("--is_online")
        arguments = " ".join(arguments)

        # Now using processing.py instead of make_raw_records.py
        jobstring = f"""
        echo "Processing run {run_id} at $(date)"
        source {SETUP_FILE}
        cd {os.path.dirname(os.path.realpath(__file__))}
        pwd
        python process.py {arguments}
        echo "Job complete!"
        echo `date`
        """

        if args.production:
            submit_job(
                jobstring=jobstring,
                jobname=job_name,
                log_dir=args.logs_path,
                queue=args.queue,
                mem_per_cpu=args.mem,
                cpus_per_task=1,
            )

            update_processing_status(
                run_id, "submitted", pull={"tags": {"name": "process"}}, production=args.production, is_online=True
            )
        else:
            log.info(f"Would have submitted job for run {run_id}")


if __name__ == "__main__":
    args = parse_args()

    log_name = "auto_processing_online"
    import amstrax

    versions = amstrax.print_versions(
        modules="strax amstrax numpy numba".split(), include_git=False, return_string=True
    )

    log = amstrax.get_daq_logger(
        log_name, log_name, level=logging.DEBUG, opening_message=f"Using versions: {versions}", logdir=args.logs_path
    )

    main(args)
