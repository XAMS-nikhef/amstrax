import argparse
import time


def parse_args():
    parser = argparse.ArgumentParser(
        description='Autoprocess xams(l) data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--target',
        default='raw_records_v1724',
        help="Target final data type to produce.")
    parser.add_argument(
        '--timeout',
        default=60,
        type=int,
        help="Sleep this many seconds in between")
    parser.add_argument(
        '--max_jobs',
        default=None,
        type=int,
        help="Max number of jobs to submit, if you exceed this number, break submitting new jobs")
    parser.add_argument(
        '--context',
        default='xams_little',
        help="xams_little or xams")
    parser.add_argument(
        '--detector',
        default='xamsl',
        help="xamsl or xams")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    version = '2.1.0'
    print('Starting autoprocess version %s...' % version)

    # Later import to prevent slow --help
    import amstrax
    from amstrax.auto_processing import submit_stbc

    # settings
    nap_time = int(args.timeout)
    max_jobs = int(args.max_jobs) if args.max_jobs is not None else None
    context = args.context
    detector = args.detector
    target = args.target
    runs_col = amstrax.get_mongo_collection()

    while 1:
        # Update task list
        run_docs_to_do = list(runs_col.find({'processing_status': 'pending'}))
        if len(run_docs_to_do) > 0:
            print('I found %d runs to process, time to get to work!' % len(run_docs_to_do))
            print('These runs I will do:')
            print([run_doc['number'] for run_doc in run_docs_to_do])

        for run_doc in run_docs_to_do[:max_jobs]:
            run_name = f'{int(run_doc["number"]):06}'
            submit_stbc.submit_job(run_name, target=target, context=context, detector=detector,script='process_run')
            runs_col.find_one_and_update({'number': run_name},
                                         {'$set': {'processing_status': 'submitted_job'
                                                  }})
            time.sleep(2)
        if max_jobs is not None and len(run_docs_to_do) > max_jobs:
            print(f'Got {len(run_docs_to_do)} which is larger than {max_jobs}, I quit!')
            break
        print("Waiting %d seconds before rechecking, press Ctrl+C to quit..." % nap_time)
        time.sleep(nap_time)
