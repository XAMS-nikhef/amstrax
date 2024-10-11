#!/usr/bin/env python
import argparse
import datetime
import logging
import os
import subprocess
import time
import pymongo
import logging
import os

def parse_args():
    parser = argparse.ArgumentParser(description='Script to safely delete live data from stbc to save space')
    parser.add_argument('--logs_path', type=str, default='/data/xenon/xams_v2/logs', help='Logs storage location')
    parser.add_argument('--live_data_path', type=str, default='/data/xenon/xams_v2/live_data', help='Live data storage location')
    parser.add_argument('--days_old', type=int, default=30, help='Delete data older than this many days')
    parser.add_argument('--production', action='store_true', help='Perform deletion on production database')
    parser.add_argument('--we_are_really_sure', action='store_true', help='Perform deletion on production database')
    parser.add_argument('--loop_infinite', action='store_true', help='Loop infinitely')
    parser.add_argument('--max_runs', type=int, default=5, help='Max number of runs to process at the time before reconsider')
    parser.add_argument('--sleep_time', type=int, default=60, help='Sleep time between checking')
    parser.add_argument('--min_free_diskspace', type=int, default=500e9, help='Minimum free disk space in GB. Default: 5GB')
    parser.add_argument('--min_run_number', type=int, default=2000, help='Minimum run number to consider')
    return parser.parse_args()

def check_diskspace(path):
    """
    Check the disk space on a given host.
    """
    log.info(f"Checking diskspace for {path}. Path exists: {os.path.exists(path)}")
    cmd = f"df -h {path}"
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
    if result.returncode != 0:
        log.error(f"Command failed: {result.stderr}")
        return None

    free_space = convert_to_bytes(result.stdout.split(' ')[-4])
    return free_space

def convert_to_bytes(size_str):
    size_value = int(size_str[:-1])  # Remove the last character (G/T) and convert to an integer
    size_unit = size_str[-1]         # Get the last character (the unit)

    if size_unit == 'T':
        return size_value * 10**12    # Convert terabytes to bytes
    elif size_unit == 'G':
        return size_value * 10**9     # Convert gigabytes to bytes
    elif size_unit == 'M':
        return size_value * 10**6     # Convert megabytes to bytes
    elif size_unit == 'K':
        return size_value * 10**3     # Convert kilobytes to bytes
    else:
        raise ValueError(f"Unknown size unit: {size_unit}")


def get_old_runs(runsdb, days, args):
    """
    Retrieve run documents where the data is older than specified days and exists in three locations
    or runs with an abandon tag. Returns a list of run documents.
    """
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)

    query = {
    # always make sure we do not mess with data currentrly being written
    'end': {'$lt': datetime.datetime.now() - datetime.timedelta(seconds=30)},
    'number': {'$gt': args.min_run_number},
    'data.attempted_deletion': {'$ne': 'True'},
    '$or': [
        {'end': {'$lte': cutoff_date},
        'data': {'$all': [
            {'$elemMatch': {'type': 'live', 'host': 'stbc'}},
            {'$elemMatch': {'type': 'live', 'host': 'dcache'}},
        ]}},
        {'tags': {'$elemMatch': {'name': 'abandon'}},
        'data': {'$elemMatch': {'type': 'live', 'host': 'daq'}}}
    ]
    }

    projection = {'number': 1, 'end': 1, 'data': 1, 'tags': 1}
    return list(runsdb.find(query, projection=projection))[0:args.max_runs]


def count_files_in_directory(path, run_id):
    """
    Count the number of files in a given directory, locally or remotely via SSH.
    """
    full_path = os.path.join(path, run_id)

    # get the number of files with ls | wc -l
    cmd = f"ls -1 {full_path}/* | wc -l"
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
    if result.returncode != 0:
        log.error(f"Command failed: {result.stderr}")
        return 0

    result = int(result.stdout.strip())
    return result


def check_data_safety(run_doc, args):
    """
    Perform checks to ensure that data can be safely deleted from DAQ.
    Returns True if safe to delete, False otherwise.
    """
    run_id = str(run_doc['number']).zfill(6)

    result = {}

    hosts_to_check = ['stbc', 'dcache']

    for host in hosts_to_check:
        path = next((d['location'] for d in run_doc['data'] if d['host'] == host), None)
        if not path:
            log.warning(f"Missing data path for run {run_id} on host {host}")
            return False

        # Get number of files in the directory
        num_files = count_files_in_directory(path, run_id)
        log.info(f"Found {num_files} files in {path} on {host} for run {run_id}")
        result[host] = num_files

    # Check if the file counts match
    if (result['stbc'] != result.get('dcache', -9)):
        log.warning(f"Mismatch in file count for run {run_id}")
        return False

    return True


def delete_data(runsdb, run_doc, production, we_are_really_sure):
    """
    Delete data from DAQ if safety checks are passed.
    """
    run_id = str(run_doc['number']).zfill(6)
    path = next((d['location'] for d in run_doc['data'] if d['host'] == 'stbc'), None)
    run_data_path = os.path.join(path, run_id)

    if not production:
        log.info(f"[Dry Run] Would delete data for run {run_id} at {run_data_path}")
    else:
        log.info(f"Deleting data for run {run_id} at {run_data_path}")

        if we_are_really_sure:
            # check that the path ends with the run number
            if not run_data_path.endswith(run_id):
                log.error(f"Path {run_data_path} does not end with run number {run_id}")
                return
            
            # check that the path exists
            if not os.path.exists(run_data_path):
                log.error(f"Path {run_data_path} does not exist?!")
                return

            else:
                try:
                    log.info(f"Deleting {run_data_path}")
                    cmd = [f"rm -rf {run_data_path}"]
                    log.info(f"Running command: {cmd}")
                    remove = subprocess.run(cmd, capture_output=True, shell=True, text=True)
                    if not os.path.exists(run_data_path):
                        log.info(f"Successfully deleted {run_data_path}")
                        # Move the stbc data entry from 'data' array to 'deleted_data' array in MongoDB
                        stbc_data_entry = next((d for d in run_doc['data'] if d['host'] == 'stbc'), None)
                        if stbc_data_entry:
                            runsdb.update_one(
                                {'number': int(run_id)},
                                {'$pull': {'data': stbc_data_entry}}
                            )
                            runsdb.update_one(
                                {'number': int(run_id)},
                                {'$push': {'deleted_data': dict(stbc_data_entry, **{
                                    'deleted_at': datetime.datetime.now(),
                                    'deleted_by': 'delete_live_stbc.py'})
                                    }
                                }
                            )
                            log.info(f"Moved stbc data entry for run {run_id} to 'deleted_data'")
                    else:
                        log.error(f"Path {run_data_path} does still exist?! Check the file permissions.")
                        log.info(f"Marking run {run_id} as attempted deletion")
                        runsdb.update_one(
                            {'number': int(run_id)},
                            {'$set': {'data.attempted_deletion': 'True'}}
                        )
                    
                except Exception as e:
                    log.error(f"Error in deleting data for run {run_id}: {e}")
                    return
        else:
            log.info(f"[Not Really Sure] Would delete data for run {run_id} at {run_data_path}")

    return


def main(args):
    runsdb = amstrax.get_mongo_collection()
    old_runs = get_old_runs(runsdb, args.days_old, args)
    log.info(f"Found {len(old_runs)} runs with data older than {args.days_old} days or with abandon tag")

    stbc_path = args.live_data_path
    free_space = check_diskspace(stbc_path)

    if free_space < args.min_free_diskspace:
        log.info(f"Free space in {stbc_path} is {free_space} bytes.\n"
                   "Deleting runs {old_runs['number']}, but first checking safety")

        for run_doc in old_runs:
            if check_data_safety(run_doc, args):
                log.info(f"Deleting live data for run {run_doc['number']}")
                delete_data(runsdb, run_doc, args.production, args.we_are_really_sure)
            else:
                log.warning(f"Skipping deletion for run {run_doc['number']} due to safety check failure")
                log.info(f"Marking run {run_doc['number']} as attempted deletion")
                runsdb.update_one(
                    {'number': run_doc['number'],'data.host': 'stbc'},
                    {'$set': {'data.$.attempted_deletion': 'True'}}
                )
    
    else:
        log.info(f"Free space in {stbc_path} is {free_space} bytes. Not deleting any runs.")

    return len(old_runs)


if __name__ == '__main__':
    args = parse_args()

    # Set up logging
    log_name = "delete_live_stbc"

    import amstrax

    versions = amstrax.print_versions(
        modules="strax amstrax numpy numba".split(),
        include_git=False,
        return_string=True,
    )

    log = amstrax.get_daq_logger(
        log_name,
        log_name,
        level=logging.DEBUG,
        opening_message=f"I am processing with these software versions: {versions}",
        logdir=args.logs_path,
    )

    if not args.production:
        log.info("Performing a dry run. No data will be deleted.")

    if not args.we_are_really_sure:
        log.info("We are not really sure. No data will be deleted.")

    if args.loop_infinite:
        while True:
            try:
                runs_deleted = main(args)
                sleep_time = 1 if runs_deleted else args.sleep_time
                log.info(f"Sleeping for {args.sleep_time} seconds...")
                time.sleep(args.sleep_time)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as fatal_error:
                log.error(
                    f"Fatal warning:\tran into {fatal_error}. Try "
                    "logging error and restart loop"
                )
                try:
                    log.warning(f"Fatal warning:\tran into {fatal_error}")
                except Exception as warning_error:
                    log.error(f"Fatal warning:\tcould not log {warning_error}")
                # This usually only takes a minute or two
                time.sleep(60)
                log.warning("Restarting run loop")
    else:
        main(args)
