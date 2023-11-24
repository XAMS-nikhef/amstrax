#!/usr/bin/env python
import sys, os
import datetime
import argparse
import json
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


    return parser.parse_args()


def main(args):

    import amstrax

    runsdb = amstrax.get_mongo_collection()

    rd = runsdb.find_one({'number': int(args.run_id)})

    print(f'Run {args.run_id} has status {rd["processing_status"]["status"]}')

    if rd['processing_status'].get('status', None) == 'submitted':
        print(f'Run {args.run_id} was submitted for processing, but not yet processed.')

        # do a lot of stuff


        # update rundoc, for now set status to testing
        runsdb.update_one({'number': int(args.run_id)},
                            {'$set': {'processing_status': {'status': 'running', 'time': datetime.datetime.now()}}})

        # do the processing

        try:
            process(args)  # Call the process function with the args parameter
            
            runsdb.update_one({'number': int(args.run_id)},
                            {'$set': {'processing_status': {'status': 'done', 'time': datetime.datetime.now()}}})

            # add the processed data to the rundoc , append to array 
            runsdb.update_one({'number': int(args.run_id)},
                            {'$push': {'data': 
                            {'time': datetime.datetime.now(),
                            'type': 'raw_records',
                            'location': args.output_folder,
                            'host': 'stoomboot',
                            'by': 'make_raw_records.py',
                            'user': os.environ['USER']
                            }}})

        except Exception as e:
            
            print(f'Processing of run {args.run_id} failed with error {e}')
            # update rundoc, for now set status to testing
            runsdb.update_one({'number': int(args.run_id)},

                {'$set': {'processing_status': 
                    
                        {'status': 'failed',
                        'reason': str(e), 'time': datetime.datetime.now(),
                        'time': datetime.datetime.now()
                        }
                    }
                }
            )

            runsdb.update_one(
                        {'number': int(args.run_id)},
                        {'$inc': {'processing_failed': 1}}
                    )

        rd = runsdb.find_one({'number': int(args.run_id)})
        print(f"Run {rd['number']} has status {rd['processing_status']}")

        return 0
        

    else:
        print(f'Run {args.run_id} is not in submitted mode, but in {rd["processing_status"]["status"]}')

        raise ValueError(f'Run {args.run_id} is not in submitted mode, but in {rd["processing_status"]["status"]}')


def process(args):  # Add the args parameter to the process function

    import amstrax

    run_id = args.run_id
    run_col = amstrax.get_mongo_collection()
    run_doc = run_col.find_one({'number': int(run_id)})
    live_data = '/data/xenon/xams_v2/live_data'
    output_folder = args.output_folder

    st = amstrax.contexts.xams(output_folder=output_folder, init_rundb=False)
    st.storage += [strax.DataDirectory(live_data,
                                 provide_run_metadata=False,
                                 deep_scan=False,
                                 readonly=True)]

    st.set_config({'live_data_dir': f'{live_data}'})
    daqst = amstrax.contexts.context_for_daq_reader(st, run_id, 'xams', run_doc=run_doc, check_exists=False)

    for t in args.target:
        print(f'Processing {t} in amstraxer_easy')
        daqst.make(run_id, t, progress_bar=True)



if __name__ == '__main__':
    args = parse_args()
    sys.exit(main(args))
