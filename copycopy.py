import subprocess
import pymongo
import amstrax
import os
import argparse
import configparser
import shutil
import logging
import time
from datetime import datetime

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
    parser.add_argument('--config', 
        type=str, help='Path to your configuration file', 
        default='/home/xams/daq/webinterface/web/config.ini')

    return parser.parse_args()

def parse_config(args):
    config = configparser.ConfigParser()
    config.read(args.config)

    return config['DEFAULT']

def log(msg,level):
    """
    Function that returns log file with desired entry
    :param msg: (str) message to display in the logfile
    :param level: (level to display in the log file; can be critical, error, warning, info, debug (str) or 50, 40, 30, 20, 10 (int) respectively
    :return: file with log(msg,level) entry
    """
    today = datetime.today()
    fileName = f"{today.year:04d}{today.month:02d}{today.day:02d}_copying"
    
    args = parse_args()
    config = parse_config(args)
    logs_path = config['logs_path']

    logFormatter = logging.Formatter(f"{today.isoformat(sep=' ')} | %(levelname)-5.5s | %(message)s")
    log = logging.getLogger()

    fileHandler = logging.FileHandler("{0}/{1}.log".format(logs_path, fileName))
    fileHandler.setFormatter(logFormatter)
    log.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    log.addHandler(consoleHandler)

    if level == 'info' or level == 20:
        log.setLevel(logging.INFO)
        return log.info(msg)
    elif level == 'warning' or level == 30:
        log.setLevel(logging.WARNING)
        return log.warning(msg)
    elif level == 'error' or level == 40:
        log.setLevel(logging.ERROR)
        return log.error(msg)
    else:
        log.setLevel(logging.NOTSET)
        return log(msg)

def runsdb():
    args = parse_args()
    config = parse_config(args)

    max_runs = args.max_runs

    # Initialize runsdatabase collection
    runs_database = config['RunsDatabaseName']
    runs_collection = config['RunsDatabaseCollection']
    runsdb = amstrax.get_mongo_collection(database_name = runs_database,
                                            database_col= runs_collection)

    # Make a list of the last 'max_runs' items in the runs database, 
    # only keeping the fields 'number', 'data' and '_id'.
    query = {}  # TODO for now, but we should query on the data field in the future
    rundocs = list(runsdb.find(query, projection = {'number':1, 'data':1, '_id':1}).sort('number', pymongo.DESCENDING)[:max_runs])

    return rundocs

def copy():
    args = parse_args()
    config = parse_config(args)
    dest_loc = config['dest_location']                                                   
    for rd in rundocs:
        run = rd.get('number')
        data_fields = rd.get('data')
        ids = rd.get('_id')
        location = None

        # Check if in the 'data' field of the document if a location is stored                                            
        for doc in data_fields:
            location = doc['location']
            if run is None or location is None: 
                log('For %s we got no data? Rundoc: %s' %(str(run),str(rd)),'error')
            if doc['type'] == 'live':  
               # Check if the data is already stored on stoomboot 
               if doc['host']=='stoomboot' and location is not None:
                    log('Run %s is already transferred according to the rundoc! I check later for new runs.' %str(run),'info')
               else: 
                    # If the data is not yet on stoomboot, copy it to there
                    copy = f'rsync -a {location}/{run:06d} -e ssh stbc:{dest_loc}'
                    copy_execute = subprocess.call(copy, shell=True)

    return copy_execute

def update_runsdb():
    final_destination = config['final_destination']
    dest_loc = config['dest_location']

    runsdb = runsdb()

    if copy() == 0:
        log('I succesfully copied run %s from %s to %s' %(str(run),str(location),str(dest_loc)),'info')
        # In stead of changing the old location, maybe better to add new location?
        runsdb.update_one(
            {'_id': ids,
            'data': {
                '$elemMatch': {
                    'location': f'{location}'
                }}
            },
            {'$set':
                {'data.$.host': 'stoomboot',
                'data.$.location': f'{dest_loc}'
                }
            }
        )
        for doc in data_fields:
            if doc['location'] == f'{dest_loc}':  
                log('I updated the RunsDB with the new location for this run %s!' %str(run),'info')

        # After updating the runsdatabase and checking if the data is indeed on stoomboot,
        # move the data on the DAQ machine to a folder from which it can be removed (later)
        shutil.move(f'{location}/{run:06d}', f'{final_destination}')  # After testing, let's change this to shutil.rmtree(location)
        if os.path.exists(f'{final_destination}/{run:06d}'):
            log(f'I moved the data on the DAQ machine to its final destination before it gets removed','info')

    else:
        log('Copying did not succeed. Probably run %s is already copied.' %(str(run)),'error')

    return

def main():
    args = parse_args()
    config = parse_config(args)

    detector = args.detector

    log('I am ready to start copying data for %s!' %detector,'info')
    
    # max_runs = args.max_runs
    final_destination = config['final_destination']
    dest_loc = config['dest_location']
    
    # # Initialize runsdatabase collection
    # runs_database = config['RunsDatabaseName']
    # runs_collection = config['RunsDatabaseCollection']
    # runsdb = amstrax.get_mongo_collection(database_name = runs_database,
    #                                         database_col= runs_collection)

    # # Make a list of the last 'max_runs' items in the runs database, 
    # # only keeping the fields 'number', 'data' and '_id'.
    # query = {}  # TODO for now, but we should query on the data field in the future
    # rundocs = list(runsdb.find(query, projection = {'number':1, 'data':1, '_id':1}).sort('number', pymongo.DESCENDING)[:max_runs])
                                                    
    # for rd in rundocs:
    #     run = rd.get('number')
    #     data_fields = rd.get('data')
    #     ids = rd.get('_id')
    #     location = None

    #     # Check if in the 'data' field of the document if a location is stored                                            
    #     for doc in data_fields:
    #         location = doc['location']
    #         if run is None or location is None: 
    #             log('For %s we got no data? Rundoc: %s' %(str(run),str(rd)),'error')
    #         if doc['type'] == 'live':  
    #            # Check if the data is already stored on stoomboot 
    #            if doc['host']=='stoomboot' and location is not None:
    #                 log('Run %s is already transferred according to the rundoc! I check later for new runs.' %str(run),'info')
    #            else: 
    #                 # If the data is not yet on stoomboot, copy it to there
    #                 copy = f'rsync -a {location}/{run:06d} -e ssh stbc:{dest_loc}'
    #                 copy_execute = subprocess.call(copy, shell=True)

                    # If copying was succesful, update the runsdatabase with the new location of the data
                    if copy_execute == 0:
                        log('I succesfully copied run %s from %s to %s' %(str(run),str(location),str(dest_loc)),'info')
                        # In stead of changing the old location, maybe better to add new location?
                        runsdb.update_one(
                            {'_id': ids,
                            'data': {
                                '$elemMatch': {
                                    'location': f'{location}'
                                }}
                            },
                            {'$set':
                                {'data.$.host': 'stoomboot',
                                'data.$.location': f'{dest_loc}'
                                }
                            }
                        )
                        for doc in data_fields:
                            if doc['location'] == f'{dest_loc}':  
                                log('I updated the RunsDB with the new location for this run %s!' %str(run),'info')

                        # After updating the runsdatabase and checking if the data is indeed on stoomboot,
                        # move the data on the DAQ machine to a folder from which it can be removed (later)
                        shutil.move(f'{location}/{run:06d}', f'{final_destination}')  # After testing, let's change this to shutil.rmtree(location)
                        if os.path.exists(f'{final_destination}/{run:06d}'):
                            log(f'I moved the data on the DAQ machine to its final destination before it gets removed','info')

                    else:
                        log('Copying did not succeed. Probably run %s is already copied.' %(str(run)),'error')

    return


if __name__ == '__main__':
    args = parse_args()
    if not args.loop_infinite:
        main()
    else:
        while True:
            print("I woke up! Let me check for new runs")
            main()
            time.sleep(args.sleep_time)
