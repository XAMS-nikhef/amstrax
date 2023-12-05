import argparse
import time
import os
import subprocess
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler

import sys

from batch_stbc import submit_job

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
    parser.add_argument(
        '--production',
        action='store_true',
        help="Set to production mode")
    parser.add_argument(
        '--only_manual',
        action='store_true',
        help="Set to only process runs with tag process")

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

def main(args):
    """
    Main function to handle auto-processing of xams data.
    """

    # Import custom modules
    import amstrax
    
    version = '2.1.0'
    logging.info(f'Starting autoprocess version {version}...')

    # Get configuration and setup
    amstrax_dir = amstrax.amstrax_dir
    nap_time = int(args.timeout)
    output_folder = args.output_folder
    targets = " ".join(args.target)
    runs_col = amstrax.get_mongo_collection()
    logging.info('Correctly connected, starting loop')
    amstrax_dir = amstrax.amstrax_dir
    
    client = amstrax.get_mongo_client()
    processing_db = client['daq']['processing']
        
    infinite = True
    while infinite:

        auto_processing_on = processing_db.find_one({'name': 'auto_processing'})['status'] == 'on'

        # Update task list
        run_docs_to_do = update_task_list(args, runs_col, auto_processing_on)
        
        # Check and handle running jobs
        handle_running_jobs(runs_col, production=args.production)

        if not run_docs_to_do:
            logging.info(f'I found no runs to process, time to take a nap for {nap_time} seconds')
            time.sleep(nap_time)
            continue

        # Submit new jobs if under max limit
        submit_new_jobs(args, runs_col, run_docs_to_do, amstrax_dir=amstrax_dir)

        logging.info(f"Waiting {nap_time} seconds before rechecking, press Ctrl+C to quit...")
        time.sleep(nap_time)

    logging.info('Done!')

# Define additional functions to modularize the script

def update_task_list(args, runs_col, auto_processing_on):
    """
    Update and return the list of tasks to be processed based on MongoDB queries.
    """
    # Your existing MongoDB query

    query = {
        'data': { '$elemMatch': {'type': 'live', 'host': 'stbc'}},
        # this or is just to allow a force process of a run, by adding the tag process
        '$or': [
                    {
                        'data': {'$not': {'$elemMatch': {'host': 'stbc', 'type': 'raw_records'}}},
                        'processing_failed': {'$not': {'$gt': 3}},
                        'processing_status.status': {'$not': {'$in': ['running', 'submitted']}},
                        'tags': {'$not': {'$elemMatch': {'name': 'abandon'}}},
                        'start': {'$gt': datetime.today() - timedelta(days=100)},            
                    },
                    {'tags': {'$elemMatch': {'name': 'process'}}}
        ]
    }

    if args.only_manual or not auto_processing_on:
        query = {
            'data': { '$elemMatch': {'type': 'live', 'host': 'stbc'}},
            'tags': {'$elemMatch': {'name': 'process'}}
        }

    # Projection for MongoDB query
    projection = {
        'number': 1, 
        'start': 1, 
        'end': 1, 
        'data': 1, 
        'processing_status': 1, 
        'processing_failed': 1
    }

    # Sorting order for the query results
    sort = [('number', -1)]

    # Execute the query and get the documents
    run_docs_to_do = list(runs_col.find(query, projection).sort(sort))

    # Check for a specific run ID if provided
    if args.run_id is not None:
        infinite = False
        run_docs_to_do = [runs_col.find_one({'number': int(args.run_id)}, projection)]

    # Log the found runs
    if run_docs_to_do:
        logging.info(f'I found {len(run_docs_to_do)} runs to process, time to get to work!')
        logging.info(f'Run numbers: {[run_doc["number"] for run_doc in run_docs_to_do]}')
    return run_docs_to_do



def handle_running_jobs(runs_col, production=False):
    """
    Check and update the status of running jobs.
    Mark jobs as failed if they've been running or submitted for over an hour.
    """
    query = {
        'processing_status.status': 
            {'$in': ['submitted', 'running']}
    }

    projection = {'number': 1, 'processing_status': 1}
    run_docs_running = list(runs_col.find(query, projection))

    for run_doc in run_docs_running:
        processing_status = run_doc['processing_status']
        run_number = run_doc['number']

        # Check for jobs running or submitted for more than 30 min
        if processing_status['status'] in ['running', 'submitted']:
            if processing_status['time'] < datetime.now() - timedelta(hours=0, minutes=30):
                new_status = 'failed'
                logging.info(f'Run {run_number} has a job {processing_status["status"]} for more than 1 hour, marking as {new_status}')

                if production:
                    runs_col.update_one(
                        {'number': run_number},
                        {'$set': {'processing_status': {'status': new_status, 'time': datetime.now()}},
                        '$inc': {'processing_failed': 1}}
                    )

                else:
                    logging.info(f'Would have updated run {run_number} to {new_status} in the database')


def submit_new_jobs(args, runs_col, run_docs_to_do, amstrax_dir):
    """
    Submit new jobs if the current number of running/submitted jobs is below the max_jobs limit.
    """
    # Check how many jobs are currently running or submitted
    query = {'processing_status.status': {'$in': ['submitted', 'running']}}
    projection = {'number': 1, 'processing_status': 1}
    sort = [('number', -1)]

    run_docs_running = list(runs_col.find(query, projection).sort(sort))
    num_running_jobs = len(run_docs_running)

    logging.info(f'Found {num_running_jobs} runs that are running or submitted')

    for run_doc in run_docs_running:
        logging.info(f'Run {run_doc["number"]} is in ststus {run_doc["processing_status"]["status"]}')

    if num_running_jobs >= args.max_jobs:
        logging.info(f'The number of running jobs ({num_running_jobs}) reached the limit ({args.max_jobs})')
        return

    # Submit new jobs
    max_jobs_to_submit = args.max_jobs - num_running_jobs

    will_do_run_ids = [int(run_doc['number']) for run_doc in run_docs_to_do[:max_jobs_to_submit]]
    logging.info(f'Will submit jobs for runs: {will_do_run_ids}')

    for run_doc in run_docs_to_do[:max_jobs_to_submit]:
        run_id = f'{int(run_doc["number"]):06}'
        job_name = f'xams_{run_id}_production'
        log_file = os.path.join(args.logs_path, f'{job_name}.log')

        production_flag = '--production' if args.production else ''
        targets = " ".join(args.target)

        jobstring = f"""
        echo `date`
        echo "Starting job for run {run_id}"
        export PATH=/data/xenon/cfuselli/miniconda-install/bin:$PATH
        source activate /data/xenon/xams_v2/anaconda/xams
        cd {amstrax_dir}/auto_processing/
        python make_raw_records.py --run_id {run_id} --target {targets} {production_flag}
        echo "Done!"
        echo `date`
        """

        if args.production:
            submit_job(
                jobstring=jobstring,
                jobname=job_name,
                log=log_file,
                queue='short',
                hours=1,
                mem_per_cpu=args.mem,
                cpus_per_task=1,
            )

            # Update the database with the submitted job info
            runs_col.update_one(
                {'number': run_doc['number']},
                {'$set': {'processing_status': {'status': 'submitted', 'time': datetime.now(), 'host': 'stbc'}},
                # remove tag process if it exists
                '$pull': {'tags': {'name': 'process'}}
                }
            )
        else:
            logging.info(f'Would have submitted job for run {run_id}')
            logging.info(f'Would have updated run {run_doc["number"]} to submitted in the database')


# Ensure the script is run as the main program
if __name__ == '__main__':
    args = parse_args()
    setup_logging(args.logs_path)
    main(args)
