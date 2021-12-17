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

    runsdb = amstrax.get_mongo_collection()
    max_runs = 1
    dest_loc = f'/data/xenon/{detector}/live_data'

    # Take last 'max_runs' runs and sort them
    runs = list(run_id.get('number', 'no_number')
                for run_id in
                runsdb.find({},
                            projection={'number': 1, '_id': 0}
                            ).sort('number', pymongo.DESCENDING)[:max_runs])

    rundata = list(run_id.get('data', 'no_data')
                   for run_id in
                   runsdb.find({},
                               projection={'data': 1, '_id': 0}
                               ).sort('number', pymongo.DESCENDING)[:max_runs])

    live_location = []

    for i, j in enumerate(rundata):
        if rundata[i][0]['type'] == 'live':
            live_location.append(rundata[i][0]['location'])

    for (run, location) in zip(runs, live_location):
        try:
            copy = f'rsync -a {location}/00{run} -e ssh stbc:{dest_loc}'
            copy_execute = subprocess.call(copy, shell=True)

            rundoc = runsdb.find_one({'number': int(run)})

            if copy_execute == 0:
                # In stead of changing the old location, maybe better to add new location?
                runsdb.update_one(
                    {'number': int(run),
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

                print(
                    f'I succesfully copied run {run} from {location} to {dest_loc} and updated the RunsDB!')

            else:
                print(f'Copying did not succeed. Probably the run is already copied.')

        except:
            print(f"Run {run} is probably already copied. Just continue.")

    return


if __name__ == '__main__':
    main()
