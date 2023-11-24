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
    parser = argparse.ArgumentParser(
        description='Script that automatically copies new runs to stoomboot')
    parser.add_argument(
        '--detector',
        type=str,
        help='The detector that you are using',
        default='xams')
    parser.add_argument(
        '--max_runs',
        type=int,
        help='How many runs you want to copy every time',
        default=1)
    parser.add_argument(
        '--sleep_time',
        type=int,
        help='After how many seconds you want the script to check the database again',
        default=60)
    parser.add_argument(
        '--loop_infinite',
        action='store_true',
        help='If you want to run the script in an infinite loop',
        default=False)
    parser.add_argument(
        '--dest_location',
        type=str,
        help='The location where the data should be copied to',
        default='/data/xenon/xams_v2/live_data')
    parser.add_argument(
        '--dest_backup_location',
        type=str,
        help='The location where the data should be copied to',
        default='/dcache/archive/xenon/xams/xams_v2/live_data')
    parser.add_argument(
        '--logs_path',
        type=str,
        help='The location where the logs should be stored',
        default='/home/xams/daq/logs'
    )
    parser.add_argument(
        '--ssh_host',
        type=str,
        help='The host that you want to copy the data to',
        default='stbc'
    )
    parser.add_argument(
        '--production',
        action='store_true',
        help='If you want to run the script in production mode',
        default=False)


    return parser.parse_args()



def setup_logging(logs_path):
    """
    Setup logging configuration with daily log rotation.
    """
    if not os.path.exists(logs_path):
        os.makedirs(logs_path)

    log_file = os.path.join(logs_path, 'copying.log')

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


def get_rundocs(runsdb, args):
    """
    Retrieve run documents from MongoDB collection based on specific criteria.
    """

    # do two separate queries, to give priority to stoomboot over dcache
    query_not_on_stoomboot = {
        'end': {'$exists': True},
        'number': {'$gt': 2000},
        'data': {
            '$elemMatch': {
                'type': 'live',
                'host': 'daq'
            },
            '$not': {
                '$elemMatch': {'type': 'live', 'host': 'stoomboot'}
            }
        }
    }

    query_not_on_dcache = {
        'end': {'$exists': True},
        'number': {'$gt': 2000},
        'data': {
            '$elemMatch': {
                'type': 'live',
                'host': 'daq'
            },
            '$not': {
                '$elemMatch': {'type': 'live', 'host': 'dcache'}
            }
        }
    }


    projection = {'number': 1, 'end': 1, 'data': 1}
    sort = [('number', pymongo.DESCENDING)]

    # Perform queries and get results
    runs_not_on_stoomboot = list(runsdb.find(query_not_on_stoomboot, projection=projection, sort=sort))
    runs_not_on_dcache = list(runsdb.find(query_not_on_dcache, projection=projection, sort=sort))

    # combine the two lists
    rundocs = runs_not_on_stoomboot + runs_not_on_dcache
    rundocs = rundocs[:args.max_runs]

    return list(rundocs)

def copy_data(run_id, live_data_path, location, hostname, production, ssh_host):
    """
    Copy data to the specified location using rsync.
    """
    if not os.path.exists(live_data_path):
        logging.error(f"Could not find the data for run {run_id} at {live_data_path}")
        return

    logging.info(f"Copying run {run_id} to {location}")
    copy_cmd = ['rsync', '-av', live_data_path, f'{ssh_host}:{location}']
    copy = subprocess.run(copy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if copy.returncode != 0:
        logging.error(f"Something went wrong copying run {run_id} to {location}")
        logging.error(copy.stderr.decode())
    else:
        logging.info(f"Successfully copied run {run_id} to {location}")
        logging.info(copy.stdout.decode())

        if production:
            runsdb.update_one(
                {'number': int(run_id)},
                {'$push': {'data': {'type': 'live', 'host': hostname, 'path': location,
                                    'by': 'copy_live', 'time': datetime.datetime.now()}}}
            )
            logging.info(f"Successfully updated the database for run {run_id}")

def handle_runs(rundocs, args):
    """
    Handle the copying process for each run document.
    """
    for rd in rundocs:
        run_id = f"{rd['number']:06}"
        try:
            path = next(d['location'] for d in rd['data'] if d['type'] == 'live' and d['host'] == 'daq')
        except StopIteration:
            logging.error(f"Could not find the DB entry for live data of run {rd['number']}")
            continue

        live_data_path = os.path.join(path, run_id)

        if not any(d['type'] == 'live' and d['host'] == 'stoomboot' for d in rd['data']):
            copy_data(run_id, live_data_path, args.dest_location, 'stoomboot', args.production, args.ssh_host)

        if not any(d['type'] == 'live' and d['host'] == 'dcache' for d in rd['data']):
            copy_data(run_id, live_data_path, args.dest_backup_location, 'dcache', args.production, args.ssh_host)

def main(args):
    """
    Main function to automate copying of new runs.
    """
    logging.info('Starting to copy new runs...')
    rundocs = get_rundocs(runsdb, args)
    handle_runs(rundocs, args)
    logging.info('Finished copying new runs.')

if __name__ == '__main__':
    args = parse_args()
    setup_logging(args.logs_path)
    runsdb = amstrax.get_mongo_collection()

    if args.loop_infinite:
        while True:
            main(args)
            logging.info(f"Sleeping for {args.sleep_time} seconds...")
            time.sleep(args.sleep_time)
    else:
        main(args)
