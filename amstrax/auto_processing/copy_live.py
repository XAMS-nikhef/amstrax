import subprocess
import pymongo
import amstrax
import os
import argparse
import configparser
import shutil
import logging
from datetime import datetime


def main():
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
    parser.add_argument('--config', 
        type=str, help='Path to your configuration file', 
        default='/home/xams/daq/webinterface/web/config.ini')
    args = parser.parse_args()

    # If we use a config file, it becomes super easy to set the database collections! <3 _ <3 (shall we then remove the if statements?)
    config = configparser.ConfigParser()
    config.read(args.config)
    config = config['DEFAULT']

    detector = args.detector
    max_runs = args.max_runs
    logs_path = config['logs_path']
    final_destination = config['final_destination']

    today = datetime.today()
    fileName = f"{today.year:04d}{today.month:02d}{today.day:02d}_copying"

    logFormatter = logging.Formatter(f"{today.isoformat(sep=' ')} | %(levelname)-5.5s | %(message)s")
    log = logging.getLogger()

    fileHandler = logging.FileHandler("{0}/{1}.log".format(logs_path, fileName))
    fileHandler.setFormatter(logFormatter)
    log.addHandler(fileHandler)
    log.setLevel(logging.ERROR)
    log.setLevel(logging.INFO)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    log.addHandler(consoleHandler)
    
    if detector == 'xams':
        runsdb = amstrax.get_mongo_collection(database_name='run',
                                              database_col='runs_gas') # or the new collection I still want to make (e.g runs_xams?)
    elif detector == 'xamsl':
        runsdb = amstrax.get_mongo_collection(database_name='run',
                                              database_col='runs_new')
    else: 
        log.error(f'Your detector {detector} does not exist. Use xams or xamsl.')
        raise TypeError(f'Your detector {detector} does not exist. Use xams or xamsl.')

    dest_loc = f'/data/xenon/{detector}/live_data'

    query = {}  # TODO for now, but we should query on the data field in the future
    rundocs = list(runsdb.find(query, projection = {'number':1, 'data':1, '_id':1}).sort('number', pymongo.DESCENDING)[:max_runs])
                                                    
    for rd in rundocs:
        run = rd.get('number')
        data_fields = rd.get('data')
        ids = rd.get('_id')
        location = None
                                                    
        for doc in data_fields:
            location = doc['location']
            if run is None or location is None: 
                log.error('For %s we got no data? Rundoc: %s' %(str(run),str(rd)))
            if doc['type'] == 'live':  
               if doc['host']=='stoomboot' and location is not None:
                    log.info('Run %s is already transferred according to the rundoc!' %str(run))
               else: 
                    copy = f'rsync -a {location}/{run:06d} -e ssh stbc:{dest_loc}'
                    copy_execute = subprocess.call(copy, shell=True)

                    if copy_execute == 0:
                        log.info('I succesfully copied run %s from %s to %s' %(str(run),str(location),str(dest_loc)))
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
                                log.info('I updated the RunsDB with the new location for this run %s!' %str(run))

                        shutil.move(f'{location}/{run:06d}', f'{final_destination}')  # After testing, let's change this to shutil.rmtree(location)
                        if os.path.exists(f'{final_destination}/{run:06d}'):
                            log.info(f'I moved the data on the DAQ machine to its final destination before it gets removed')

                    else:
                        log.error('Copying did not succeed. Probably run %s is already copied.' %(str(run)))

    return


if __name__ == '__main__':
    main()
