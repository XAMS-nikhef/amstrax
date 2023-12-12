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
import os

# Define a dictionary for storage locations
STORAGE_PATHS = {
    'stbc': '/data/xenon/xams_v2/live_data',
    'dcache': '/dcache/archive/xenon/xams/xams_v2/live_data'
}

def parse_args():
    parser = argparse.ArgumentParser(
        description='Script that automatically copies new runs to stoomboot and optionally to dcache')
    parser.add_argument('--max_runs', type=int, default=1,
                        help='How many runs you want to copy every time')
    parser.add_argument('--sleep_time', type=int, default=60,
                        help='After how many seconds you want the script to check the database again')
    parser.add_argument('--loop_infinite', action='store_true', default=False,
                        help='If you want to run the script in an infinite loop')
    parser.add_argument('--only_stoomboot', action='store_true', default=False,
                        help='Only copy to stoomboot, skip dcache')
    parser.add_argument('--logs_path', type=str, default='/home/xams/daq/logs',
                        help='The location where the logs should be stored')
    parser.add_argument('--ssh_host', type=str, default='stbc',
                        help='The host that you want to copy the data to')
    parser.add_argument('--min_run_number', type=int, default=1555,
                        help='The minimum run number to start copying from')
    parser.add_argument('--production', action='store_true', default=False,
                        help='If you want to run the script in production mode')
    return parser.parse_args()


def get_rundocs(runsdb, args):
    """
    Retrieve run documents from MongoDB collection based on specific criteria.
    """

    base_query = {
        # end is at least 1 second ago
        'end': {'$lt': datetime.datetime.now() - datetime.timedelta(seconds=1)},
        'number': {'$gt': args.min_run_number},
        'data': {
            '$elemMatch': {
                'type': 'live',
                'host': 'daq'
            }
        },
        'tags': {
            '$not': {
                '$elemMatch': {'name': 'abandon'}
            }
        }
    }

    # do two separate queries, to give priority to stoomboot over dcache
    query_not_on_stoomboot = dict(base_query, **{
        'data': {
            '$not': {
                '$elemMatch': {'type': 'live', 'host': 'stbc'}
            }
        }
    })

    query_not_on_dcache = dict(base_query, **{
        'data': {
            '$not': {
                '$elemMatch': {'type': 'live', 'host': 'dcache'}
            }
        }
    })


    projection = {'number': 1, 'end': 1, 'data': 1}
    sort = [('number', pymongo.DESCENDING)]

    # Use a set to keep track of unique run numbers
    unique_run_numbers = set()

    # Function to process query and add unique runs
    def process_query(query):
        runs = runsdb.find(query, projection=projection, sort=sort)
        for run in runs:
            if run['number'] not in unique_run_numbers:
                unique_run_numbers.add(run['number'])
                yield run

    # Process both queries
    runs_not_on_stoomboot = list(process_query(query_not_on_stoomboot))

    if args.only_stoomboot:
        runs_not_on_dcache = []
    else:
        runs_not_on_dcache = list(process_query(query_not_on_dcache))

    # combine the two lists
    rundocs = runs_not_on_stoomboot + runs_not_on_dcache
    rundocs = rundocs[:args.max_runs]

    return list(rundocs)

def copy_data(run_id, live_data_path, location, hostname, production, ssh_host):
    """
    Copy data to the specified location using rsync.
    """
    if not os.path.exists(live_data_path):
        log.error(f"Could not find the data for run {run_id} at {live_data_path}, marking run as abandon")
        # add a tag to the tags array in the database, marking the run as abandon
        runsdb.update_one(
            {'number': int(run_id)},
            {'$push': {'tags': {'name': 'abandon', 'user': 'copy_live', 'time': datetime.datetime.now()}}}
        )

        return

    log.info(f"Copying run {run_id} to {location}")
    copy_cmd = ['rsync', '-av', live_data_path, f'{ssh_host}:{location}']
    copy = subprocess.run(copy_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if copy.returncode != 0:
        log.error(f"Something went wrong copying run {run_id} to {location}")
        log.error(copy.stderr.decode())
    else:
        log.info(f"Successfully copied run {run_id} to {location}")
        log.info(copy.stdout.decode())

        if production:
            runsdb.update_one(
                {'number': int(run_id)},
                {'$push': {'data': {'type': 'live', 'host': hostname, 'location': location,
                                    'by': 'copy_live', 'time': datetime.datetime.now()}}}
            )
            log.info(f"Successfully updated the database for run {run_id}")

    return copy.returncode


def handle_runs(rundocs, args):
    runs_copied = False
    for rd in rundocs:
        run_id = f"{rd['number']:06}"
        try:
            path = next(d['location'] for d in rd['data'] if d['type'] == 'live' and d['host'] == 'daq')
        except StopIteration:
            log.error(f"Could not find the DB entry for live data of run {rd['number']}")
            continue

        live_data_path = os.path.join(path, run_id)

        # Check if data is on stoomboot and copy if not
        copied_stomboot = False
        if not any(d['type'] == 'live' and d['host'] == 'stbc' for d in rd['data']):
            exit_code = copy_data(run_id, live_data_path, STORAGE_PATHS['stbc'], 'stbc', args.production, args.ssh_host)
            copied_stomboot = (exit_code == 0)
        else:
            # it was already on stoomboot
            copied_stomboot = True

        if copied_stomboot:
            # Check if data is on dcache and copy if not (and if not only_stoomboot)
            if not args.only_stoomboot and (not any(d['type'] == 'live' and d['host'] == 'dcache' for d in rd['data'])):
                copy_data(run_id, live_data_path, STORAGE_PATHS['dcache'], 'dcache', args.production, args.ssh_host)
                runs_copied = True
    
    return runs_copied

def main(args):
    """
    Main function to automate copying of new runs.
    """

    log.info('Starting to copy new runs...')
    rundocs = get_rundocs(runsdb, args)
    print(f"Found {len(rundocs)} runs to copy")
    runs_copied = handle_runs(rundocs, args)
    log.info('Finished copying new runs.')

if __name__ == '__main__':
    args = parse_args()

    log_name = "copy_live"

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

    runsdb = amstrax.get_mongo_collection()

    if args.loop_infinite:
        while True:
            runs_copied = main(args)
            sleep_time = 1 if runs_copied else args.sleep_time
            log.info(f"Sleeping for {args.sleep_time} seconds...")
            time.sleep(args.sleep_time)
    else:
        main(args)
