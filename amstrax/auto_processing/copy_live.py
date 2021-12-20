import subprocess
import pymongo
import amstrax
import argparse


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
    args = parser.parse_args()
    detector = args.detector
    max_runs = args.max_runs
    
    if detector == 'xams':
        runsdb = amstrax.get_mongo_collection(database_name='run',
                                              database_col='runs_gas') # or the new collection I still want to make (e.g runs_xams?)
    elif detector == 'xamsl':
        runsdb = amstrax.get_mongo_collection(database_name='run',
                                              database_col='runs_new')
    else: 
        raise TypeError(f'Your detector {detector} does not exist. Use xams or xamsl.') # TODO: find a way to show this error in the webinterface

    dest_loc = f'/data/xenon/{detector}/live_data'

    query = {}  # TODO for now, but we should query on the data field in the future
    rundocs = list(runsdb.find(query, projection = {'number':1, 'data':1, '_id':1}).sort('number', pymongo.DESCENDING)[:max_runs])
                                                    
    for rd in rundocs:
        run = rd.get('number')
        data_fields = rd.get('data')
        ids = rd.get('_id')
        location = None
                                                    
        for doc in data_fields:
            if doc['type'] == 'live':  
               location = doc['location']
        if run is None or location is None: 
             print(f'For {run:06d} we got no data? Rundoc: {rd}')

        copy = f'rsync -a {location}/{run:06d} -e ssh stbc:{dest_loc}'
        copy_execute = subprocess.call(copy, shell=True)

        if copy_execute == 0:
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

            print(f'I succesfully copied run {run:06d} from {location} to {dest_loc} and updated the RunsDB!')

        else:
            print(f'Copying did not succeed. Probably run {run:06d} is already copied.')

    return


if __name__ == '__main__':
    main()
