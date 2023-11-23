import subprocess
import pymongo
import amstrax
import os
import argparse
import configparser
import shutil
import logging
import time
from datetime import datetime, timedelta
import typing as ty


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
        default='/dcache/archive/xenon/xams/xams_v2/live_data'
    )
    parser.add_argument(
        '--logs_path',
        type=str,
        help='The location where the logs should be stored',
        default='/home/xams/daq/logs'
    )
    parser.add_argument(
        '--production',
        action='store_true',
        help='If you want to run the script in production mode',
        default=False)
    parser.add_argument(
        '--ssh_host',
        type=str,
        help='The host that you want to copy the data to',
        default='stbc'
    )


    return parser.parse_args()



def logfile():
    today = datetime.today()
    fileName = f"{today.year:04d}{today.month:02d}{today.day:02d}_copying"

    args = parse_args()

    logFormatter = logging.Formatter(
        f"{today.isoformat(sep=' ')} | %(levelname)-5.5s | %(message)s")
    logfile = logging.getLogger()

    logfile.setLevel(logging.INFO)

    fileHandler = logging.FileHandler("{0}/{1}.log".format(args.logs_path, fileName))
    fileHandler.setFormatter(logFormatter)
    logfile.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logfile.addHandler(consoleHandler)

    return logfile



def get_rundocs(runsdb: pymongo.collection.Collection, args: argparse.Namespace):
    
    # query runsdb for new runs
    # we need to find all the runs that have and end, and that have live_data only on this host (daq)

    query = {
        'end': {'$exists': True},
        'number': {'$gt': 2000},
        'data': {
            '$elemMatch': {
                'type': 'live_data',
                'host': 'daq'
            }
        },
        'data': {
            '$not': {
                '$all': [
                    {'$elemMatch': {'type': 'live_data', 'host': 'stoomboot'}},
                    {'$elemMatch': {'type': 'live_data', 'host': 'dcache'}}
                ]
            }
        }
    }


    projection = {'number': 1, 'end': 1, 'data': 1}

    # sort by number, so that we get the oldest runs first
    sort = [('number', pymongo.DESCENDING)]

    # get the runs
    runs = runsdb.find(query, projection=projection, sort=sort).limit(args.max_runs)

    # this is only a cursor, so we need to loop over it
    rundocs = list(runs)

    return rundocs

def handle_runs(rundocs: list, args: argparse.Namespace):

    for rd in rundocs:

        
        run_id = f"{rd['number']:06}"
        # we want to copy the data to stoomboot, a remote called stbc
        # find in the data array the entry with type=live_data and host=daq
        # and get the path, if there are missing entries raise an error
        try:
            path = [d['path'] for d in rd['data'] if d['type'] == 'live_data' and d['host'] == 'daq'][0]
        except IndexError:
            logs.error(f"Could not find the path for run {rd['number']}")
            continue

        live_data_path = os.path.join(path, run_id)

        # check if the data exists at the location
        if not os.path.exists(live_data_path):
            logs.error(f"Could not find the data for run {rd['number']} at {live_data_path}")
            continue

        # if production mode is on, copy the data to stoomboot, and update the database
        # stomboot and dcache are mounted on stbc, so we can use the same command, let's do an rsync stbc:..
        # let's use subprocess run and check if the copy goes well, in case, update the database

        # if no data entry exists for stoomboot, create one
        if not any([d['type'] == 'live_data' and d['host'] == 'stoomboot' for d in rd['data']]):
            copy_data(
                live_data_path=live_data_path,
                location=args.dest_location, 
                hostname='stoomboot', 
                run_id=run_id, 
                production=args.production,
                ssh_host=args.ssh_host
            )

        # if no data entry exists for dcache, create one
        if not any([d['type'] == 'live_data' and d['host'] == 'dcache' for d in rd['data']]):
            copy_data(
                live_data_path=live_data_path,
                location=args.dest_backup_location, 
                hostname='dcache', 
                run_id=run_id, 
                production=args.production,
                ssh_host=args.ssh_host
            )


    
def copy_data(rundsb: pymongo.collection.Collection,
    live_data_path: str, location: str, hostname: str, run_id: str, production=False, ssh_host='stbc'):

    # check if the data exists at the location
    if not os.path.exists(live_data_path):
        logs.error(f"Could not find the data for run {run_id} at {live_data_path}")
        return

    logs.info(f"Copying run {run_id} to {location}")
    if production:
        copy = subprocess.run(
            ['rsync', '-av', f'{live_data_path}/', f'{ssh_host}:{location}/'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        if copy.returncode != 0:
            logs.error(f"Something went wrong copying run {run_id} to {location}")
            logs.error(copy.stderr)
            return
        else:
            logs.info(f"Successfully copied run {run_id} to {location}")
            logs.info(copy.stdout)

            # add one entry to the data array
            rundb.update_one(
                {'number': int(run_id)},
                {'$push': 
                    {'data': 
                        {'type': 'live_data', 
                        'host': hostname, 
                        'path': location,
                        'by': 'copy_live',
                        'time': datetime.now()
                        }
                    }
                }
            )

            logs.info(f"Successfully updated the database for run {run_id}")
            
            return
    else:
        logs.info(f"Would have copied run {run_id} to {location}")

        return



    


def main(args):

    logs.info('I am ready to start copying data for!')

    # Initialize runsdatabase collection
    runsdb = amstrax.get_mongo_collection()

    # get the runs that we want to copy
    rundocs = get_rundocs(runsdb, args)

    # handle the runs
    handle_runs(rundocs, args)

    logs.info('I am done copying data for now!')

    return






if __name__ == '__main__':
    args = parse_args()
    logs = logfile()
    today = datetime.today()
    tomorrow = datetime.today() + timedelta(days=1)

    if not args.loop_infinite:
        main(args)
        exit()

    while True:
        if today < tomorrow:
            logs.info("I woke up! Let me check for new runs.")
            main(args)
            logs.info("I go to sleep for %d seconds. Cheers!" % args.sleep_time)
        else:
            logs = logfile()
            logs.info("I woke up! Let me check for new runs.")
            main(args)
            logs.info("I go to sleep for %d seconds. Cheers!" % args.sleep_time)
        time.sleep(args.sleep_time)
