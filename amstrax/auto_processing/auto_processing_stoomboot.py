import argparse
import time
from datetime import datetime, timedelta
import subprocess 
import logging

def parse_args():
    parser = argparse.ArgumentParser(
        description='Autoprocess xams data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--target',
        nargs="*",
        default=['raw_records',],
        help="Target final data type to produce.")
    parser.add_argument(
        '--output_folder',
        default='/data/xenon/xams_v2/xams_raw_records',
        help="Path where to save processed data")   
    parser.add_argument(
        '--timeout',
        default=20,
        type=int,
        help="Sleep this many seconds in between")
    parser.add_argument(
        '--max_jobs',
        default=5,
        type=int,
        help="Max number of jobs to submit, if you exceed this number, break submitting new jobs")
    parser.add_argument(
        '--run_id', 
        default=None,
        help="Single run ID to process")
    parser.add_argument(
        '--mem', 
        default=8000,
        help="Memory per CPU")
    parser.add_argument(
        '--logs_path',
        default='/data/xenon/xams_v2/logs/auto_processing',
        help="Path where to save logs")

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
    output_folder = args.output_folder
    
    targets = args.target
    targets = " ".join(targets)

    runs_col = amstrax.get_mongo_collection()

    print('Correctly connected, starting loop')

    infinite = True

    while infinite:
        
        # Update task list
        # Probably want to add here some max retry if fail
        query = {
            # live data is on stomboot (in the data array)
            'data': {
                # live data on stomboot
                '$elemMatch': { 'type': 'live','host': 'stoomboot'}, 
                # no raw_records on stoomboot
                '$not': {'$elemMatch': {'host': 'stoomboot', 'type': 'raw_records'}},
            },
            'processing_failed':{'$not': {'$gt': 3}},
            'processing_status': {'$not': {'$elemMatch': {'status': {'$in': ['submitted', 'running', 'done', 'testing']}}}},
            'start':{'$gt': datetime.today() - timedelta(days=30)},
            }

        projection = {'number': 1, 'start': 1, 'end': 1, 'data': 1, 'processing_status': 1, 'processing_failed': 1}

        sort = [('number', -1)]

        run_docs_to_do = list(runs_col.find(query, projection).sort(sort))

        if args.run_id is not None:
            infinite = False
            run_docs_to_do = [runs_col.find_one({'number': int(args.run_id)})]
        
        if len(run_docs_to_do) > 0:
            print('I found %d runs to process, time to get to work!' % len(run_docs_to_do))
            print('These runs I will do:')
            print([run_doc['number'] for run_doc in run_docs_to_do])
            for rd in run_docs_to_do[:500]:
                print(f"Run {rd['number']} has status {rd['processing_status']}")

        else:
            print('I found no runs to process, time to take a nap for %d seconds' % nap_time)
            time.sleep(nap_time)
            continue


        # check if there is any job running or submitted for more than 1 hour
        # if so, mark it as failed
        # this is to prevent jobs that are stuck to be stuck forever
        # and to prevent jobs that are stuck to be submitted again
        # this is a bit of a hack, but it works
        # we should probably do this in the runsdb, but this is easier for now

        query = {
            'processing_status': {
                '$elemMatch': {'status': {'$in': ['submitted', 'running']}}
            }
        }

        projection = {'number': 1, 'processing_status': 1}

        run_docs_running = list(runs_col.find(query, projection))

        for run_doc in run_docs_running:
            # check if there is a job running for more than 1 hour
            # if so, mark it as failed
            # this is to prevent jobs that are stuck to be stuck forever
            # and to prevent jobs that are stuck to be submitted again
            # this is a bit of a hack, but it works
            # we should probably do this in the runsdb, but this is easier for now
            processing_status = run_doc['processing_status']
            if processing_status['status'] == 'running':
                if processing_status['time'] < datetime.now() - timedelta(hours=1, minutes=5):
                    print(f'Run {run_doc["number"]} has a job running for more than 1 hour, marking as failed')
                    runs_col.update_one(
                        {'number': run_doc['number']},
                        {'$set': {'processing_status': {'status': 'failed', 'time': datetime.now()}}}
                    )
                    runs_col.update_one(
                        {'number': run_doc['number']},
                        {'$inc': {'processing_failed': 1}}
                    )
                    break

            if processing_status['status'] == 'submitted':
                if processing_status['time'] < datetime.now() - timedelta(hours=1, minutes=5):
                    print(f'Run {run_doc["number"]} has a job submitted for more than 1 hour, marking as failed')
                    runs_col.update_one(
                        {'number': run_doc['number']},
                        {'$set': {'processing_status': {'status': 'failed', 'time': datetime.now()}}}
                    )
                    runs_col.update_one(
                        {'number': run_doc['number']},
                        {'$inc': {'processing_failed': 1}}
                    )
                    break

        # check how many jobs are running (status submitted or running)
        # if more than max_jobs, wait
        # if less than max_jobs, submit new jobs
        # check it from the runsb 

        # get all runs that are submitted or running
        query = {'processing_status.status': {'$in': ['submitted', 'running']}}

        projection = {'number': 1, 'processing_status': 1}
        sort = [('number', -1)]

        run_docs_running = list(runs_col.find(query, projection).sort(sort))

        print(f'Found {len(run_docs_running)} runs that are running or submitted')

        if len(run_docs_running) >= args.max_jobs:
            # wait 1 minute and try again
            print(f'Found {len(run_docs_running)} runs that are running or submitted, waiting 20 sec')
            time.sleep(20)
            continue

        else:
            print(f'Found {len(run_docs_running)} runs that are running or submitted, submitting new jobs')

        max_jobs = args.max_jobs - len(run_docs_running)

        for run_doc in run_docs_to_do[:max_jobs]:
            run_id = f'{int(run_doc["number"]):06}'
                
            # import the file batch_stbc.py in the same folder as the current script
            from batch_stbc import submit_job

            jobstring = f"""
            echo `date`
            echo "Starting job for run {run_id}"
            export PATH=/data/xenon/miniconda3/bin:$PATH
            source activate /data/xenon/xams_v2/anaconda/xams
            cd /data/xenon/xams_v2/software/amstrax/amstrax/auto_processing/
            python make_raw_records.py --run_id {run_id} --output_folder {output_folder}
            echo "Done!"
            echo `date`
            """

            submit_job(
                jobstring=jobstring,
                jobname=f'xams_{run_id}_production',
                log=os.path.join(args.logs_path, f'xams_{run_id}.log'),
                queue='short',
                hours=1,
                mem_per_cpu=args.mem,
                cpus_per_task=1,
            )

            # update in the rundb that we submitted

            runs_col.update_one(
                {'number': run_doc['number']},
                {'$set': {'processing_status': {'status': 'submitted', 'time': datetime.now()}}}
            )

        print("Waiting %d seconds before rechecking, press Ctrl+C to quit..." % nap_time)
        time.sleep(nap_time)


print('Done!')
