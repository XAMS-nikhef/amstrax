import argparse
import time
import os
import logging
import signal
import threading
from datetime import datetime, timedelta
import json

from job_submission import submit_job
from db_utils import update_processing_status, append_processing_history, set_provisional_gains


import subprocess
import shutil
import sys
from datetime import datetime, timedelta


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SETUP_FILE = "/data/xenon/xams_v2/setup.sh"

# CORRECTIONS VERSION FROM THE RUN DATABASE (amstrax_files docs/corrections_workflow.md, 2026-11-27).
# With --corrections_version db the loop reads the version in every iteration from daq.settings
# {name: "online_corrections_version", value: "ONLINE"}: changing it is one database write, no restart.
# Without that document the loop uses DEFAULT_ONLINE_VERSION; if the database cannot be read it keeps the version
# of the previous iteration. Any other value of --corrections_version is used as given (as before).
DB_VERSION = "db"
SETTING_ONLINE_VERSION = "online_corrections_version"
DEFAULT_ONLINE_VERSION = "ONLINE"

# CLEAN STOP. Ctrl-C (SIGINT) and SIGTERM (`kill`, `screen -X quit`, a reboot)
# set this event instead of killing the loop wherever it happens to be. The
# loop finishes the step it is in - never half a submission, where the condor
# job exists but the run document does not say so - and stops at the next
# check or during its sleep, within seconds. A second signal stops at once.
STOP = threading.Event()


def request_stop(signum, frame):
    if STOP.is_set():
        log.warning("Second stop request: exiting now.")
        raise KeyboardInterrupt
    log.info(f"Stop requested ({signal.Signals(signum).name}); finishing the current step.")
    STOP.set()


def install_stop_handlers():
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

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
    parser.add_argument("--set_config_kwargs", type=json.loads, default={}, help="Dictionary of kwargs to pass to set_config.")
    parser.add_argument("--set_context_kwargs", type=json.loads, default={}, help="Dictionary of kwargs to pass to set_context.")
    parser.add_argument("--amstrax_path", default=None, help="Path to the amstrax directory.")
    parser.add_argument("--corrections_version", default=None,
                        help="Version of corrections to use; 'db' = read it from the run database (daq.settings "
                             "online_corrections_version) in every iteration.")
    parser.add_argument("--queue", default="short", help="Queue to submit jobs to. See Nikhef docs for options.")
    parser.add_argument("--n_failures_max", type=int, help="Maximum number of failures before marking a run as failed.", default=3)
    parser.add_argument("--min_run_number", type=int, help="Minimum run number to process.", default=6000)

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
    settings_col = client["daq"]["settings"] if getattr(args, "corrections_version", None) == DB_VERSION else None
    corrections_version = None

    while not STOP.is_set():
        # Check if auto-processing is enabled
        auto_processing_on = processing_db.find_one({"name": "auto_processing"})["status"] == "on"

        new_version = resolve_corrections_version(args, settings_col, corrections_version)
        if new_version != corrections_version:
            log.info(f"Corrections version for new jobs: {new_version} (was {corrections_version})")
            corrections_version = new_version

        # Update task list and check for new runs
        run_docs_to_do = update_task_list(args, runs_col, auto_processing_on)

        # Handle running jobs
        handle_running_jobs(runs_col, production=args.production)

        if not run_docs_to_do:
            log.info(f"No runs to process. Sleeping for {nap_time} seconds.")
            STOP.wait(nap_time)
            continue

        if STOP.is_set():
            break

        # Submit new jobs if below max limit
        submit_new_jobs(args, runs_col, run_docs_to_do, amstrax_dir, corrections_version=corrections_version)

        if args.run_id:
            log.info("Finished processing run.")
            break

        log.info(f"Sleeping for {nap_time} seconds before next check...")
        STOP.wait(nap_time)

    if STOP.is_set():
        log.info("Auto-processing stopped cleanly.")


def resolve_corrections_version(args, settings_col, previous=None):
    """The corrections version for the jobs of this iteration (see DB_VERSION above)."""
    given = getattr(args, "corrections_version", None)
    if given != DB_VERSION:
        return given
    try:
        doc = settings_col.find_one({"name": SETTING_ONLINE_VERSION})
    except Exception as e:                                     # noqa: BLE001
        log.error(f"Cannot read daq.settings {SETTING_ONLINE_VERSION}: {e}; keeping {previous or DEFAULT_ONLINE_VERSION}")
        return previous or DEFAULT_ONLINE_VERSION
    value = (doc or {}).get("value")
    if not value:
        if previous is None:
            log.warning(f"No daq.settings {SETTING_ONLINE_VERSION}: using {DEFAULT_ONLINE_VERSION}")
        return DEFAULT_ONLINE_VERSION
    return str(value)


def provisional_gains(corrections_version, run_id, get_correction=None):
    """True if the run takes its gains from the open last range of an online gains file (`_online` / `_dev`, range
    ending in '*' or 999999): gains carried forward from the last calibration, not its own. Such runs are flagged in
    the run document and reprocessed when their own gains are in (corrections workflow, point 3). False when the
    version has fixed gains or the files cannot be read."""
    if not corrections_version:
        return False
    if get_correction is None:
        import amstrax
        get_correction = amstrax.get_correction
    try:
        name = get_correction(f"_global_{corrections_version}.json").get("gain_to_pe_array")
        if not isinstance(name, str) or not (name.endswith("_online.json") or "_dev" in name):
            return False
        ranges = get_correction(name)
    except Exception as e:                                     # noqa: BLE001
        log.warning(f"provisional_gains: cannot read the gains of {corrections_version}: {e}")
        return False
    n = int(run_id)
    for key in ranges:
        if "-" not in key:
            continue
        start, end = key.split("-", 1)
        open_end = end in ("*", "999999")
        if (0 if start == "*" else int(start)) <= n <= (999999 if open_end else int(end)):
            return open_end
    return False


def update_task_list(args, runs_col, auto_processing_on):
    """
    Update and return the list of tasks to be processed based on MongoDB queries.
    """
    query = {
        "data": {"$elemMatch": {"type": "live", "host": "stbc"}},
        "$or": [
            {
                "data": {"$not": {"$elemMatch": {
                    "host": {"$in": ["stbc", "wn"]}, 
                    "type": "raw_records"}}},
                "processing_failed": {"$not": {"$gt": args.n_failures_max}},
                "processing_status.status": {"$not": {"$in": ["running", "submitted"]}},
                "tags": {"$not": {"$elemMatch": {"name": "abandon"}}},
                "start": {"$gt": datetime.today() - timedelta(days=100)},
                "number": {"$gte": args.min_run_number},
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



def is_job_running_in_condor(run_number):
    """
    Check if a job with the given run_number exists in condor_q (not completed).
    """
    try:
        condor_q_bin = shutil.which("condor_q")
        if not condor_q_bin:
            raise RuntimeError("condor_q not found in PATH")
        # Own session: a Ctrl-C in the terminal goes to this loop only, which
        # then stops cleanly, and does not kill condor_q halfway.
        result = subprocess.run([condor_q_bin, '-nobatch'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                start_new_session=True)
        if result.returncode != 0:
            raise RuntimeError(f"condor_q command failed: {result.stderr}")

        job_name = f"process_{int(run_number):06d}_online"       # the online job script name (submit_new_jobs)
        return any(job_name in line for line in result.stdout.splitlines())

    except Exception as e:
        log.error(f"Error checking condor for run {run_number}: {e}")
        return True  # Fail-safe: assume running to avoid false marking as failed

def handle_running_jobs(runs_col, production=False):
    """
    Check and update the status of running jobs. A run is marked failed only if its status is 'submitted'/'running'
    for more than GRACE_MINUTES and no such job exists in condor. Jobs that wait in the queue or run long are left
    alone (with 100 jobs at once many wait longer than the old 30-minute limit, 28-10-2026); held jobs are condor's.
    """
    GRACE_MINUTES = 2                    # a job just submitted may not show up in condor_q yet
    query = {"processing_status.status": {"$in": ["submitted", "running"]}}
    projection = {"number": 1, "processing_status": 1}
    run_docs_running = list(runs_col.find(query, projection))

    for run_doc in run_docs_running:
        processing_status = run_doc["processing_status"]
        run_number = run_doc["number"]
        status_time = processing_status.get("time")

        should_fail = False
        if status_time and status_time > datetime.now() - timedelta(minutes=GRACE_MINUTES):
            continue

        # Check with condor if job is missing
        if not is_job_running_in_condor(run_number):
            log.info(f"No condor job found for run {run_number} marked as {processing_status['status']}.")
            should_fail = True

        if should_fail:
            new_status = "failed"
            if production:
                update_processing_status(run_number, new_status, production=production, is_online=True)
                append_processing_history(
                    run_number,
                    action="online_watchdog",
                    status="failed",
                    production=production,
                    host="stbc",
                    is_online=True,
                    reason="marked failed by watchdog (no condor job)",
                )
            else:
                log.info(f"Would have updated run {run_number} to {new_status}")


def submit_new_jobs(args, runs_col, run_docs_to_do, amstrax_dir, corrections_version=None):
    """
    Submit new jobs if the current number of running/submitted jobs is below the max_jobs limit.
    corrections_version: the version of this iteration (resolve_corrections_version); default args.corrections_version.
    """
    if corrections_version is None and args.corrections_version != DB_VERSION:
        corrections_version = args.corrections_version
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
        if STOP.is_set():
            log.info("Stop requested: no further submissions.")
            break
        run_id = f'{int(run_doc["number"]):06}'
        job_name = f"process_{run_id}_online"

        production_flag = "--production" if args.production else ""
        targets = " ".join(args.target)


        arguments = []
        arguments.append(f"--run_id {run_id}")
        arguments.append(f"--targets {targets}")
        arguments.append(f"--output_folder {args.output_folder}")
        if corrections_version:
            arguments.append(f"--corrections_version {corrections_version}")
        if args.amstrax_path:
            arguments.append(f"--amstrax_path {args.amstrax_path}")
        if args.production:
            arguments.append("--production")
            arguments.append("--allow_raw_records")
            arguments.append("--is_online")
        cfg_kwargs = args.set_config_kwargs
        if isinstance(cfg_kwargs, str):
            try:
                cfg_kwargs = json.loads(cfg_kwargs)
            except Exception:
                cfg_kwargs = {}
        if isinstance(cfg_kwargs, dict) and cfg_kwargs:
            config_kwargs = json.dumps(cfg_kwargs, separators=(',',':'))
            arguments.append(f"--set_config_kwargs {config_kwargs}")
        ctx_kwargs = args.set_context_kwargs
        if isinstance(ctx_kwargs, str):
            try:
                ctx_kwargs = json.loads(ctx_kwargs)
            except Exception:
                ctx_kwargs = {}
        if isinstance(ctx_kwargs, dict) and ctx_kwargs:
            context_kwargs = json.dumps(ctx_kwargs, separators=(',',':'))
            arguments.append(f"--set_context_kwargs {context_kwargs}")

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

        provisional = provisional_gains(corrections_version, run_id)
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
            if corrections_version:                    # without corrections there are no gains to flag
                set_provisional_gains(run_id, provisional, corrections_version, production=args.production)
            append_processing_history(
                run_id,
                action="online_submit",
                status="submitted",
                production=args.production,
                host="stbc",
                is_online=True,
                job_name=job_name,
                targets=list(args.target),
                corrections_version=corrections_version,
                provisional_gains=provisional,
                amstrax_path=args.amstrax_path,
                queue=args.queue,
                mem=args.mem,
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

    install_stop_handlers()
    main(args)
