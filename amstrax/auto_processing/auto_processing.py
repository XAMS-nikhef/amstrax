import argparse
import time
from datetime import datetime
import subprocess 

def parse_args():
    parser = argparse.ArgumentParser(
        description='Autoprocess xams data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--target',
        nargs="*",
        default=['peak_basics'],
        help="Target final data type to produce.")
    parser.add_argument(
        '--output_folder',
        default='./strax_data',
        help="Path where to save processed data")   
    parser.add_argument(
        '--timeout',
        default=20,
        type=int,
        help="Sleep this many seconds in between")
    parser.add_argument(
        '--process_stomboot',
        default=False,
        type=bool,
        help="False: process locally. True: submit and process on stomboot. Default: False.")
    parser.add_argument(
        '--max_jobs',
        default=150,
        type=int,
        help="Max number of jobs to submit, if you exceed this number, break submitting new jobs")
    parser.add_argument(
        '--context',
        default='xams_little',
        help="xams_little or xams")
    parser.add_argument(
        '--detector',
        default='xams',
        help="xamsl or xams")
    parser.add_argument(
        '--run_id', 
        default=None,
        help="Single run ID to process")
    return parser.parse_args()


if __name__ == '__main__':

    args = parse_args()
    version = '2.1.0'
    print('Starting autoprocess version %s...' % version)

    # Later import to prevent slow --help

    import sys, os

    import amstrax
    amstrax_dir = amstrax.amstrax_dir

    # settings
    nap_time = int(args.timeout)
    max_jobs = int(args.max_jobs) if args.max_jobs is not None else None
    context = args.context
    output_folder = args.output_folder
    process_stomboot = args.process_stomboot
    detector = args.detector
    target = args.target
    runs_col = amstrax.get_mongo_collection(detector)

    print('Correctly connected, starting loop')

    while 1:
        # Update task list
        # Probably want to add here some max retry if fail
        run_docs_to_do = list(runs_col.find({
            'processing_status':{'$ne': 'done'},
            'end':{"$ne":None},
            'start':{'$gt': datetime(2023,1,25)},
            'processing_failed':{'$not': {'$gt': 3}},
            }).sort('start', -1))

        if args.run_id is not None:
            run_docs_to_do = [runs_col.find_one({'number': int(args.run_id)})]
        
        if len(run_docs_to_do) > 0:
            print('I found %d runs to process, time to get to work!' % len(run_docs_to_do))
            print('These runs I will do:')
            print([run_doc['number'] for run_doc in run_docs_to_do])

        for run_doc in run_docs_to_do[:max_jobs]:
            run_name = f'{int(run_doc["number"]):06}'

            if process_stomboot:
                submit_stbc.submit_job(run_name, target=target, context=context, detector=detector,script='process_run')
                runs_col.find_one_and_update({'number': run_name},
                                            {'$set': {'processing_status': 'submitted_job' }})

            else: #process locally
                runs_col.find_one_and_update({'number': run_name},
                                            {'$set': {'processing_status': 'processing'}})
                target = ",".join(target)
                subprocess.run(f"process_run {run_name} --target {target} --output_folder {output_folder}", shell=True)
            time.sleep(2)

        if max_jobs is not None and len(run_docs_to_do) > max_jobs:
            print(f'Got {len(run_docs_to_do)} which is larger than {max_jobs}, I quit!')
            break
        print("Waiting %d seconds before rechecking, press Ctrl+C to quit..." % nap_time)
        time.sleep(nap_time)

        if args.run_id is not None:
            break

