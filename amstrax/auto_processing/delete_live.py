#!/usr/bin/env python
import argparse
import datetime
import logging
import os
import subprocess
import time
import pymongo
import logging


def parse_args():
    parser = argparse.ArgumentParser(description="Script to safely delete old data from DAQ and remote storage")
    parser.add_argument("--days_old", type=int, default=30, help="Delete data older than this many days")
    parser.add_argument("--logs_path", type=str, default="/home/xams/daq/logs", help="Logs storage location")
    parser.add_argument("--production", action="store_true", help="Perform deletion on production database")
    parser.add_argument("--we_are_really_sure", action="store_true", help="Perform deletion on production database")
    parser.add_argument("--ssh_host", type=str, default="xamsdata@nikhef", help="SSH host for remote deletion")
    parser.add_argument("--loop_infinite", action="store_true", help="Loop infinitely")
    parser.add_argument("--max_runs", type=int, default=10, help="Max number of runs to process")
    parser.add_argument("--sleep_time", type=int, default=60, help="Sleep time between runs")
    parser.add_argument("--min_run_number", type=int, default=2000, help="Minimum run number to consider")

    return parser.parse_args()


def get_old_runs(runsdb, days, args):
    """
    Retrieve old runs from both local and remote storage to check if they can be deleted.
    """
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
    query = {
        "end": {"$lt": datetime.datetime.now() - datetime.timedelta(seconds=30)},
        "number": {"$gt": args.min_run_number},
        "$or": [
            {
                "end": {"$lte": cutoff_date},
                "data": {
                    "$all": [
                        # {"$elemMatch": {"type": "live", "host": "daq"}},
                        {"$elemMatch": {"type": "live", "host": "stbc"}},
                        {"$elemMatch": {"type": "live", "host": "dcache"}},
                    ]
                },
            },
            {"tags": {"$elemMatch": {"name": "abandon"}}, "data": {"$elemMatch": {"type": "live", "host": "daq"}}},
        ],
    }
    projection = {"number": 1, "end": 1, "data": 1, "tags": 1}
    return list(runsdb.find(query, projection=projection))[0 : args.max_runs]


def check_data_safety(run_doc, ssh_host, args):
    """
    Perform safety checks to ensure data can be safely deleted from both local and remote storage.
    """
    run_id = str(run_doc["number"]).zfill(6)
    result = {}
    hosts_to_check = ["daq", "stbc", "dcache"]

    for host in hosts_to_check:
        path = next((d["location"] for d in run_doc["data"] if d["host"] == host), None)
        if not path:
            log.warning(f"Missing data path for run {run_id} on host {host}")
            return False

        num_files = count_files_in_directory(path, run_id, is_remote=(host != "daq"), ssh_host=ssh_host)
        log.info(f"Found {num_files} files in {path} on {host} for run {run_id}")
        result[host] = num_files

    if result["daq"] != result["stbc"] or result["stbc"] != result.get("dcache", -9):
        log.warning(f"Mismatch in file counts for run {run_id}")
        return False
    return True


def count_files_in_directory(path, run_id, is_remote=False, ssh_host=None):
    """
    Count the number of files in a given directory, locally or remotely via SSH.
    """
    full_path = os.path.join(path, run_id)

    if is_remote:
        # Construct SSH command for remote execution
        ssh_cmd = f"ssh {ssh_host} 'ls -1 {full_path}/**/* | wc -l'"
        result = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
    else:
        # Local execution
        cmd = f"ls -1 {full_path}/**/* | wc -l"
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)

    if result.returncode != 0:
        log.error(f"Command failed: {result.stderr}")
        return 0

    return int(result.stdout.strip())


def delete_data(run_doc, production, we_are_really_sure, ssh_host, is_remote=False):
    """
    Delete data from DAQ (local) or remote storage (stbc) if safety checks are passed.
    """
    run_id = str(run_doc["number"]).zfill(6)
    path = next((d["location"] for d in run_doc["data"] if d["host"] == ("stbc" if is_remote else "daq")), None)
    full_path = os.path.join(path, run_id)

    if not production:
        log.info(f"[Dry Run] Would delete data for run {run_id} at {full_path}")
    else:
        log.info(f"Deleting data for run {run_id} at {full_path}")
        if we_are_really_sure:
            if is_remote:
                cmd = ["ssh", ssh_host, f"rm -rf {full_path}"]
            else:
                cmd = f"rm -rf {full_path}"
            subprocess.run(cmd, shell=is_remote)
            log.info(f"Deleted data for run {run_id}")
        else:
            log.info(f"[Not Really Sure] Would delete data for run {run_id} at {full_path}")


def main(args):
    runsdb = amstrax.get_mongo_collection()
    old_runs = get_old_runs(runsdb, args.days_old, args)
    log.info(f"Found {len(old_runs)} runs with data older than {args.days_old} days")

    # Check local disk space
    log.info("Deleting local data.")
    for run_doc in old_runs:
        if check_data_safety(run_doc, args.ssh_host, args):
            delete_data(run_doc, args.production, args.we_are_really_sure, args.ssh_host, is_remote=False)

    # Check remote disk space via SSH
    log.info("Deleting remote data.")
    for run_doc in old_runs:
        if check_data_safety(run_doc, args.ssh_host, args):
            delete_data(run_doc, args.production, args.we_are_really_sure, args.ssh_host, is_remote=True)


if __name__ == "__main__":
    args = parse_args()
    log_name = "delete_live"

    import amstrax

    log = amstrax.get_daq_logger(log_name, log_name, level=logging.DEBUG, logdir=args.logs_path)

    if args.loop_infinite:
        while True:
            main(args)
            log.info(f"Sleeping for {args.sleep_time} seconds")
            time.sleep(args.sleep_time)
    else:
        main(args)
