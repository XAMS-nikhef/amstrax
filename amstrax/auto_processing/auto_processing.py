import time
import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description='Autoprocess xams(l) data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--target',
        default='raw_records',
        help="Name of context to use")
    parser.add_argument(
        '--timeout',
        default=60,
        type=int,
        help="Sleep this many seconds in between")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    version = '2.0.0'
    print('Starting autoprocess version %s...' % version)

    # Later import to prevent slow --help
    import amstrax
    from amstrax.auto_processing import submit_stbc

    # settings
    nap_time = int(args.timeout)

    runs_col = amstrax.get_mongo_collection()
    runs = runs_col['runs']

    while 1:
        # Update task list
        run_docs_to_do = list(runs.find({'processing_status': 'pending'}))
        if len(run_docs_to_do) > 0:
            print('I found %d runs to process, time to get to work!' % len(run_docs_to_do))
            print('These runs I will do:')
            for run_doc in run_docs_to_do:
                print(run_doc['name'])

        for run_doc in run_docs_to_do:
            run_name = run_doc['name']
            submit_stbc.submit(run_name, target='raw_records')
            runs.find_one_and_update({'name': run_name},
                                     {'$set': {'processing_status': 'submitted_job'
                                               }})
            time.sleep(2)

        print("Waiting %d seconds before rechecking, press Ctrl+C to quit..." % nap_time)
        time.sleep(nap_time)
