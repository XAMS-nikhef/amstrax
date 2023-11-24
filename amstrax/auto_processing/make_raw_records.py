#!/usr/bin/env python
import argparse
import datetime
import os
import sys
import strax

def parse_args():
    parser = argparse.ArgumentParser(
        description='Process a single run with amstrax',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--run_id',
        metavar='RUN_ID',
        type=str,
        help="ID of the run to process; usually the run name.")
    parser.add_argument(
        '--output_folder',
        default='./strax_data',
        help='Output folder for context')
    parser.add_argument(
        '--target',
        nargs='+',
        default=['raw_records'],
        help='Target data type(s) to process')
    parser.add_argument(
        '--live_data_dir',
        default='/data/xenon/xams_v2/live_data',
        help='Live data directory')
    parser.add_argument(
        '--production',
        action='store_true',
        help='Update the production database')

    return parser.parse_args()


def update_processing_status(runsdb, run_id, status, reason=None, production=False):
    """
    Update the processing status of a run in the database.
    """
    update = {'status': status, 'time': datetime.datetime.now()}
    if reason:
        update['reason'] = str(reason)

    if production:
        runsdb.update_one({'number': int(run_id)},
                        {'$set': {'processing_status': update}})
    else:
        print(f'Would update run {run_id} to status {status} with reason {reason}')


def add_data_entry(runsdb, run_id, data_type, location, host, by, user, production=False):
    """
    Add a data entry to the run document.
    """
    if production:
        runsdb.update_one({'number': int(run_id)},
                        {'$push': {'data': {'time': datetime.datetime.now(),
                                            'type': data_type,
                                            'location': location,
                                            'host': host,
                                            'by': by,
                                            'user': user}}})
    else:
        print(f'Would add data entry to run {run_id} of type {data_type} with location {location}')

def process_run(args):
    """
    Process a single run based on provided arguments.
    """

    import amstrax

    run_id = args.run_id
    output_folder = args.output_folder
    live_data = args.live_data_dir

    st = amstrax.contexts.xams(output_folder=output_folder, init_rundb=False)
    st.storage += [strax.DataDirectory(live_data, readonly=True)]
    st.set_config({'live_data_dir': live_data})

    run_col = amstrax.get_mongo_collection()
    run_doc = run_col.find_one({'number': int(run_id)})

    daqst = amstrax.contexts.context_for_daq_reader(st, run_id, 'xams', run_doc=run_doc, check_exists=False)

    for target in args.target:
        print(f'Processing {target}')
        daqst.make(run_id, target, progress_bar=True)

def main(args):
    """
    Main function to process a run.
    """

    import amstrax

    runsdb = amstrax.get_mongo_collection()
    rd = runsdb.find_one({'number': int(args.run_id)})

    if rd['processing_status'].get('status', None) == 'submitted':
        print(f'Run {args.run_id} was submitted for processing, lets process it.')

        # Update processing status to running
        update_processing_status(runsdb, args.run_id, 'running', production=args.production)

        try:
            process_run(args)  # Process the run

            update_processing_status(runsdb=runsdb,
                                     run_id=args.run_id,
                                     status='done',
                                     production=args.production)  # Update the processing status to done

            # Add the processed data to the run document
            add_data_entry(runsdb=runsdb,
                           run_id=args.run_id,
                           data_type='raw_records',
                           location=args.output_folder,
                           host='stoomboot',
                           by='make_raw_records.py',
                           user=os.environ['USER'],
                           production=args.production)

        except Exception as e:
            print(f'Processing of run {args.run_id} failed with error {e}')
            update_processing_status(runsdb, args.run_id, 'failed', reason=e)
            runsdb.update_one({'number': int(args.run_id)}, {'$inc': {'processing_failed': 1}})

        rd = runsdb.find_one({'number': int(args.run_id)})
        print(f"Run {rd['number']} has status {rd['processing_status']}")
    else:
        status = rd["processing_status"]["status"]
        e = f'Run {args.run_id} is not in submitted mode, but in {status}'
        update_processing_status(runsdb, args.run_id, 'failed', reason=e)



if __name__ == '__main__':
    args = parse_args()
    sys.exit(main(args))
