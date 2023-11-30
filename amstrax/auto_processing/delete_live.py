#!/usr/bin/env python
import argparse
import datetime
import logging
import os
import subprocess
import time
import pymongo
import amstrax
import logging
from logging.handlers import TimedRotatingFileHandler
import os

def parse_args():
    parser = argparse.ArgumentParser(description='Script to safely delete old data from DAQ')
    parser.add_argument('--days_old', type=int, default=30, help='Delete data older than this many days')
    parser.add_argument('--logs_path', type=str, default='/home/xams/daq/logs', help='Logs storage location')
    parser.add_argument('--production', action='store_true', help='Perform deletion on production database')
    parser.add_argument('--we_are_really_sure', action='store_true', help='Perform deletion on production database')
    parser.add_argument('--ssh_host', type=str,
                        help='SSH host to use for remote file count. If not specified, will use local file count.',
                        default='stbc')
    parser.add_argument('--only_stoomboot', action='store_true', help='Only check stoomboot')
    parser.add_argument('--loop_infinite', action='store_true', help='Loop infinitely')
    parser.add_argument('--max_runs', type=int, default=10, help='Max number of runs to process')
    parser.add_argument('--sleep_time', type=int, default=60, help='Sleep time between runs')
    return parser.parse_args()

def setup_logging(logs_path):
    """
    Setup logging configuration with daily log rotation.
    """
    if not os.path.exists(logs_path):
        os.makedirs(logs_path)

    log_file = os.path.join(logs_path, 'delete_live.log')

    log_formatter = logging.Formatter("%(asctime)s | %(levelname)-5.5s | %(message)s")
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    # Setup file handler with daily rotation
    file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
    file_handler.setFormatter(log_formatter)
    file_handler.suffix = "%Y%m%d"
    logger.addHandler(file_handler)

    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

def get_old_runs(runsdb, days, args):
    """
    Retrieve run documents where the data is older than specified days and exists in three locations
    or runs with an abandon tag. Returns a list of run documents.
    """
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
    query = {
        '$or': [
            {'end': {'$lte': cutoff_date},
            'data': {'$all': [
                {'$elemMatch': {'type': 'live', 'host': 'daq'}},
                {'$elemMatch': {'type': 'live', 'host': 'stbc'}},
                {'$elemMatch': {'type': 'live', 'host': 'dcache'}}
            ]}},
            {'tags': {'$elemMatch': {'name': 'abandon'}},
            'data': {'$elemMatch': {'type': 'live', 'host': 'daq'}}}
        ]
    }

    if args.only_stoomboot and not args.production:
        query = {
        '$or': [
            {'end': {'$lte': cutoff_date},
            'data': {'$all': [
                {'$elemMatch': {'type': 'live', 'host': 'daq'}},
                {'$elemMatch': {'type': 'live', 'host': 'stbc'}},
            ]}},
            {'tags': {'$elemMatch': {'name': 'abandon'}},
            'data': {'$elemMatch': {'type': 'live', 'host': 'daq'}}}
        ]
    }

        

    projection = {'number': 1, 'end': 1, 'data': 1, 'tags': 1}
    return list(runsdb.find(query, projection=projection))[0:args.max_runs]

def check_data_safety(run_doc, ssh_host, args):
    """
    Perform checks to ensure that data can be safely deleted from DAQ.
    Returns True if safe to delete, False otherwise.
    """
    run_id = str(run_doc['number']).zfill(6)

    result = {}
    hosts_to_check = ['daq', 'stbc', 'dcache']

    if args.only_stoomboot and not args.production:
        hosts_to_check = ['daq', 'stbc']

    for host in hosts_to_check:
        path = next((d['location'] for d in run_doc['data'] if d['host'] == host), None)
        if not path:
            logging.warning(f"Missing data path for run {run_id} on host {host}")
            return False

        # Get number of files in the directory
        num_files = count_files_in_directory(path, run_id, is_remote=(host != 'daq'), ssh_host=ssh_host)
        logging.info(f"Found {num_files} files in {path} on {host} for run {run_id}")
        result[host] = num_files

    # Check if the file counts match
    if (result['stbc'] != result.get('dcache', -9) and not args.only_stoomboot) or result['daq'] != result['stbc']:
        logging.warning(f"Mismatch in file count for run {run_id}")
        return False


    num_files_daq = result['daq']
    logging.info(f"File count is {num_files_daq} for run {run_id} on all hosts, safe to delete")

    return True


def count_files_in_directory(path, run_id, is_remote=False, ssh_host=None):
    """
    Count the number of files in a given directory, locally or remotely via SSH.
    """
    full_path = os.path.join(path, run_id)
    
    if is_remote:
        ssh_cmd = ["ssh", ssh_host, f"ls -1 {full_path}/* | wc -l"]
        result = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            logging.error(f"SSH command failed: {result.stderr}")
            return 0
        result =  int(result.stdout.strip())
    else:        
        # get the number of files with ls | wc -l
        cmd = f"ls -1 {full_path}/* | wc -l"
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
        if result.returncode != 0:
            logging.error(f"Command failed: {result.stderr}")
            return 0
        result = int(result.stdout.strip())
    return result



def delete_data(runsdb, run_doc, production, we_are_really_sure):
    """
    Delete data from DAQ if safety checks are passed.
    """
    run_id = str(run_doc['number']).zfill(6)
    try:
        daq_path = next(d['location'] for d in run_doc['data'] if d['host'] == 'daq')
        daq_path = os.path.join(daq_path, run_id)
        if not production:
            logging.info(f"[Dry Run] Would delete data for run {run_id} at {daq_path}")
        else:
            # we actually do it
            logging.info(f"Deleting data for run {run_id} at {daq_path}")

            if we_are_really_sure:
                # check that the path ends with the run number
                if not daq_path.endswith(run_id):
                    logging.error(f"Path {daq_path} does not end with run number {run_id}")
                    return

                # check that the path exists
                if not os.path.exists(daq_path):
                    logging.error(f"Path {daq_path} does not exist, eliminating it from database")
                else:
                    logging.info(f"Deleting {daq_path}")
                    # delete the directory daq_path
                    os.system(f"rm -rf {daq_path}")
            

                # Move the DAQ data entry from 'data' array to 'deleted_data' array in MongoDB
                daq_data_entry = next((d for d in run_doc['data'] if d['host'] == 'daq'), None)
                if daq_data_entry:
                    runsdb.update_one(
                        {'number': int(run_id)},
                        {'$pull': {'data': daq_data_entry}}
                    )
                    runsdb.update_one(
                        {'number': int(run_id)},
                        {'$push': {'deleted_data': dict(daq_data_entry, **{
                            'deleted_at': datetime.datetime.now(),
                            'deleted_by': 'delete_live.py'})
                            }
                        }
                    )
                    logging.info(f"Moved DAQ data entry for run {run_id} to 'deleted_data'")
            else:
                logging.info(f"[Not Really Sure] Would delete data for run {run_id} at {daq_path}")

    except Exception as e:
        logging.error(f"Error in deleting data for run {run_id}: {e}")

def main(args):
    runsdb = amstrax.get_mongo_collection()
    old_runs = get_old_runs(runsdb, args.days_old, args)
    logging.info(f"Found {len(old_runs)} runs with data older than {args.days_old} days or with abandon tag")
    for run_doc in old_runs:
        # if abandoned, delete immediately (tags.name == 'abandon')
        if 'abandon' in [tag['name'] for tag in run_doc.get('tags', [])]:
            logging.info(f"Deleting abandoned run {run_doc['number']}")
            delete_data(runsdb, run_doc, args.production, args.we_are_really_sure)
            continue

        else:
            # do some checks
            logging.info(f"Checking safety for run {run_doc['number']}")
            if check_data_safety(run_doc, args.ssh_host, args):
                delete_data(runsdb, run_doc, args.production, args.we_are_really_sure)
            else:
                logging.warning(f"Skipping deletion for run {run_doc['number']} due to safety check failure")
        
    return len(old_runs)

if __name__ == '__main__':
    args = parse_args()
    setup_logging(args.logs_path)
    if not args.production:
        logging.info("Performing a dry run. No data will be deleted.")

    if not args.we_are_really_sure:
        logging.info("We are not really sure. No data will be deleted.")

    if args.loop_infinite:
        while True:
            runs_deleted = main(args)
            sleep_time = 1 if runs_deleted else args.sleep_time
            logging.info(f"Sleeping for {args.sleep_time} seconds...")
            time.sleep(args.sleep_time)
    else:
        main(args)
