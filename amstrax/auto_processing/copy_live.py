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
        type=bool,
        help='If True, the file checks every sleep_time seconds for new runs in the runs database',
        default=True)
    parser.add_argument(
        '--config',
        type=str,
        help='Path to your configuration file',
        default='/home/xams/daq/webinterface/web/config.ini')

    return parser.parse_args()


def parse_config(args) -> dict:
    config = configparser.ConfigParser()
    config.read(args.config)
    return config['DEFAULT']


def logfile():
    today = datetime.today()
    fileName = f"{today.year:04d}{today.month:02d}{today.day:02d}_copying"

    args = parse_args()
    config = parse_config(args)
    logs_path = config['logs_path']

    logFormatter = logging.Formatter(
        f"{today.isoformat(sep=' ')} | %(levelname)-5.5s | %(message)s")
    logfile = logging.getLogger()

    logfile.setLevel(logging.INFO)

    fileHandler = logging.FileHandler("{0}/{1}.log".format(logs_path, fileName))
    fileHandler.setFormatter(logFormatter)
    logfile.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logfile.addHandler(consoleHandler)

    return logfile


def get_locations_to_do(rundocs) -> ty.List[ty.Union[str, None]]:
    """For a bunch of rundocs, return which location needs to be moved (if any)"""
    locations = []

    for rd in rundocs:
        loc = None
        run = rd.get('number')
        data_fields = rd.get('data')

        # Check if in the 'data' field of the document if a location is stored
        for doc in data_fields:
            if doc['type'] == 'live':
                # Check if the data is already stored on stoomboot
                if doc['host'] == 'stoomboot':
                    logs.info(f'Run %s is already transferred according to the '
                              f'rundoc! I check later for new runs.'
                              % str(run))
                    loc = None
                    break

                location = doc['location']
                if not os.path.exists(location) or doc['host'] != 'daq':
                    raise ValueError(f'Something went wrong, {location} is not here?\n{doc}')
                # If the data is not yet on stoomboot, loc it to there
                loc = location
        locations.append(loc)
    return locations


def exec_commands_and_cleanup(runsdb: pymongo.collection.Collection,
                              rundocs: ty.List[dict],
                              locations: ty.List[ty.Union[str, None]],
                              target_location: str,
                              temporary_location: str,
                              ):
    """
    For each <rundoc>, copy each of the <locations> if it's not <None>
    and update the <rundoc> accordingly
    """
    if not os.path.exists(temporary_location):
        raise FileNotFoundError(f'{temporary_location} does not exist')
    for rd, location in zip(rundocs, locations):
        run = rd['number']
        if location is None:
            logs.debug(f'Nothing to transfer for {run}')
            continue

        cmd = f'rsync -a {location}/{run:06d} -e ssh stbc:{target_location}'
        logs.warning(f'Do {cmd} for {run}')
        copy_execute = subprocess.call(cmd, shell=True)

        # If copying was successful, update the runsdatabase
        # with the new location of the data
        if copy_execute == 0:
            logs.info('Success! Update database')
            # In stead of changing the old location, maybe better to add new location?
            runsdb.update_one(
                {'_id': rd['_id'],
                 'data': {
                     '$elemMatch': {
                         'location': f'{location}'
                     }}
                 },
                {'$set':
                     {'data.$.host': 'stoomboot',
                      'data.$.location': target_location,
                      'processing_status': 'pending'
                      }
                 }

            )

            # After updating the runsdatabase and checking if the data
            # is indeed on stoomboot, move the data on the DAQ machine
            # to a folder from which it can be removed (later)
            # After testing, let's change this to shutil.rmtree(location)
            shutil.move(f'{location}/{run:06d}', temporary_location)
            if os.path.exists(f'{temporary_location}/{run:06d}'):
                logs.info(f'I moved the data on the DAQ machine to its '
                          f'final destination before it gets removed')
            else:
                logs.error(f'Whut happened to {run}?!?!')

        else:
            logs.error(f'Copying did not succeed. Probably {run} is already copied?!')


def main(args):
    config = parse_config(args)
    detector = args.detector
    logs.info('I am ready to start copying data for %s!' % detector)

    max_runs = args.max_runs
    final_destination = config['final_destination']
    dest_loc = config['dest_location']

    # Initialize runsdatabase collection
    runs_database = config['RunsDatabaseName']
    runs_collection = config['RunsDatabaseCollection']
    runsdb = amstrax.get_mongo_collection(
        database_name=runs_database,
        database_col=runs_collection
    )

    # Make a list of the last 'max_runs' items in the runs database, 
    # only keeping the fields 'number', 'data' and '_id'.
    query = {}  # TODO for now, but we should query on the data field in the future
    rundocs = list(runsdb.find(query,
                               projection={'number': 1, 'data': 1, '_id': 1}
                               ).sort('number', pymongo.DESCENDING)[:max_runs])

    locations = get_locations_to_do(rundocs)
    exec_commands_and_cleanup(runsdb=runsdb,
                              rundocs=rundocs,
                              locations=locations,
                              target_location=dest_loc,
                              temporary_location=final_destination,
                              )

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
