#!/usr/bin/env python
import argparse
import datetime
import os
import sys
import strax

HOST_DEFINITIONS = {
    'daq': {
        'output_folder': '/home/xams/data/xams_processed',
        # we will get the live data dir from the runsdb, so this is not used
        'live_data_dir': 'from_db',
        'hostname_regex': 'xams'
    },
    'stoomboot': {
        'output_folder': '/data/xenon/xams_v2/xams_raw_records',
        'live_data_dir': '/data/xenon/xams_v2/live_data',
        'hostname_regex': 'nikhef.nl'
    }
}

def parse_args():
    parser = argparse.ArgumentParser(
        description='Process a single run with amstrax',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--run_id',
        metavar='RUN_ID',
        type=str,
        required=True,
        help="ID of the run to process; usually the run name.")
    parser.add_argument(
        '--target',
        nargs='+',
        default=['raw_records'],
        help='Target data type(s) to process')
    parser.add_argument(
        '--production',
        action='store_true',
        help='Update the production database')

    return parser.parse_args()


def get_host():
    """
    Get the host name.
    """
    hostname = os.environ.get('HOSTNAME', None)
    if hostname is None:
        raise ValueError('Could not determine host name')

    # check in the host definitions if we know this host
    for host, host_def in HOST_DEFINITIONS.items():
        if host_def['hostname_regex'] in hostname:
            return host

    raise ValueError(f'Could not determine host name {hostname} from HOST_DEFINITIONS {HOST_DEFINITIONS}')
    

def update_processing_status(runsdb, run_id, status, reason=None, production=False):
    """
    Update the processing status of a run in the database.
    """
    update = {'status': status, 'time': datetime.datetime.now(), 'host': get_host()}
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

def process_run(args, runsdb):
    """
    Process a single run based on provided arguments.
    """

    import amstrax

    hostname = get_host()

    run_id = args.run_id

    run_doc = runsdb.find_one({'number': int(run_id)})

    output_folder = HOST_DEFINITIONS[hostname]['output_folder']

    if hostname == 'daq':
        live_data = run_doc['daq_config']['strax_output_path']
    else:
        live_data = HOST_DEFINITIONS[hostname]['live_data_dir']
    
    st = amstrax.contexts.xams(output_folder=output_folder, init_rundb=False)
    st.storage += [strax.DataDirectory(live_data, readonly=True)]

    daqst = amstrax.contexts.context_for_daq_reader(st, run_id, run_doc=run_doc, check_exists=False)

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

    processing_status = rd.get('processing_status', dict())
    # if procesing status is not a dict, we have a problem

    if args.production:
        status = processing_status.get('status', None)
        if status != 'submitted':
            e = f'Run {args.run_id} is not in submitted mode, but in {status}'
            update_processing_status(runsdb, args.run_id, 'failed', reason=e, production=args.production)
            return 

    print(f'Lets process run {args.run_id}')

    # Update processing status to running
    update_processing_status(runsdb, args.run_id, 'running', production=args.production)

    try:
        process_run(args, runsdb)  # Process the run

        update_processing_status(runsdb=runsdb,
                                    run_id=args.run_id,
                                    status='done',
                                    production=args.production)  # Update the processing status to done

        # Add the processed data to the run document
        add_data_entry(runsdb=runsdb,
                        run_id=args.run_id,
                        data_type='raw_records',
                        location=args.output_folder,
                        host=get_host(),
                        by='make_raw_records.py',
                        user=os.environ['USER'],
                        production=args.production)

    except Exception as e:
        print(f'Processing of run {args.run_id} failed with error {e}')
        update_processing_status(runsdb, args.run_id, 'failed', reason=e, production=args.production)
        runsdb.update_one({'number': int(args.run_id)}, {'$inc': {'processing_failed': 1}})

    rd = runsdb.find_one({'number': int(args.run_id)})
    print(f"Run {rd['number']} has status {rd['processing_status']}")
    

if __name__ == '__main__':
    args = parse_args()
    sys.exit(main(args))
