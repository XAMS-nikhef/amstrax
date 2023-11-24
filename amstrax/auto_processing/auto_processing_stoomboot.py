import argparse
import time
import os
import subprocess
from datetime import datetime, timedelta
import logging
import sys

from batch_stbc import submit_job

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    return parser.parse_args()

def main():
    """
    Main function to handle auto-processing of xams data.
    """

    # Import custom modules
    import amstrax
    
    args = parse_args()
    version = '2.1.0'
    logger.info(f'Starting autoprocess version {version}...')

    # Get configuration and setup
    amstrax_dir = amstrax.amstrax_dir
    nap_time = int(args.timeout)
    output_folder = args.output_folder
    targets = " ".join(args.target)
    runs_col = amstrax.get_mongo_collection()
    logger.info('Correctly connected, starting loop')
    amstrax_dir = amstrax.amstrax_dir

    infinite = True
    while infinite:

        # Update task list
        run_docs_to_do = update_task_list(args, runs_col)
        
        if not run_docs_to_do:
            logger.info(f'I found no runs to process, time to take a nap for {nap_time} seconds')
            time.sleep(nap_time)
            continue

        # Check and handle running jobs
        handle_running_jobs(runs_col)

        # Submit new jobs if under max limit
        submit_new_jobs(args, runs_col, run_docs_to_do, amstrax_dir=amstrax_dir)

        logger.info(f"Waiting {nap_time} seconds before rechecking, press Ctrl+C to quit...")
        time.sleep(nap_time)

    logger.info('Done!')

# Define additional functions to modularize the script

def update_task_list(args, runs_col):
    """
    Update and return the list of tasks to be processed based on MongoDB queries.
    """
    # Your existing MongoDB query
    query = {
        'data': {
            '$elemMatch': {'type': 'live', 'host': 'stoomboot'}, 
            '$not': {'$elemMatch': {'host': 'stoomboot', 'type': 'raw_records'}},
        },
        'processing_failed': {'$not': {'$gt': 3}},
        'processing_status': {'$not': {'$elemMatch': {'status': {'$in': ['submitted', 'running', 'done', 'testing']}}}},
        'start': {'$gt': datetime.today() - timedelta(days=30)},
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
        logger.info(f'I found {len(run_docs_to_do)} runs to process, time to get to work!')
        logger.info(f'These runs I will do: {[run_doc["number"] for run_doc in run_docs_to_do]}')
    return run_docs_to_do



def handle_running_jobs(runs_col, production=False):
    """
    Check and update the status of running jobs.
    Mark jobs as failed if they've been running or submitted for over an hour.
    """
    query = {
        'processing_status': {
            '$elemMatch': {'status': {'$in': ['submitted', 'running']}}
        }
    }

    projection = {'number': 1, 'processing_status': 1}
    run_docs_running = list(runs_col.find(query, projection))

    for run_doc in run_docs_running:
        processing_status = run_doc['processing_status']
        run_number = run_doc['number']

        # Check for jobs running or submitted for more than 1 hour
        if processing_status['status'] in ['running', 'submitted']:
            if processing_status['time'] < datetime.now() - timedelta(hours=1, minutes=5):
                new_status = 'failed'
                logger.info(f'Run {run_number} has a job {processing_status["status"]} for more than 1 hour, marking as {new_status}')

                if production:
                    runs_col.update_one(
                        {'number': run_number},
                        {'$set': {'processing_status': {'status': new_status, 'time': datetime.now()}}},
                        {'$inc': {'processing_failed': 1}}
                    )
                else:
                    logger.info(f'Would have updated run {run_number} to {new_status} in the database')


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

    logger.info(f'Found {num_running_jobs} runs that are running or submitted')

    if num_running_jobs >= args.max_jobs:
        logger.info(f'Waiting 20 sec, as the number of running jobs ({num_running_jobs}) reached the limit ({args.max_jobs})')
        time.sleep(20)
        return

    # Submit new jobs
    max_jobs_to_submit = args.max_jobs - num_running_jobs

    will_do_run_ids = [int(run_doc['number']) for run_doc in run_docs_to_do[:max_jobs_to_submit]]
    logger.info(f'Will submit jobs for runs: {will_do_run_ids}')

    for run_doc in run_docs_to_do[:max_jobs_to_submit]:
        run_id = f'{int(run_doc["number"]):06}'
        job_name = f'xams_{run_id}_production'
        log_file = os.path.join(args.logs_path, f'{job_name}.log')

        jobstring = f"""
        echo `date`
        echo "Starting job for run {run_id}"
        export PATH=/data/xenon/cfuselli/miniconda-install/bin:$PATH
        source activate /data/xenon/xams_v2/anaconda/xams
        cd {amstrax_dir}/auto_processing/
        python make_raw_records.py --run_id {run_id} --output_folder {args.output_folder}
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
                {'$set': {'processing_status': {'status': 'submitted', 'time': datetime.now()}}}
            )
        else:
            logger.info(f'Would have submitted job for run {run_id}')
            logger.info(f'Would have updated run {run_doc["number"]} to submitted in the database')


# Ensure the script is run as the main program
if __name__ == '__main__':
    main()
