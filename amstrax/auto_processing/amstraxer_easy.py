#!/usr/bin/env python
import sys, os
import amstrax
import strax
import numpy as np
import pandas as pd
import datetime
import argparse
import json

def parse_args():
    parser = argparse.ArgumentParser(
        description='Process a single run with amstrax',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        'run_id',
        metavar='RUN_ID',
        type=str,
        help="ID of the run to process; usually the run name.")
    parser.add_argument(
        '--context',
        default='xams',
        help="Name of context to use")
    parser.add_argument(
        '--target',
        default=['raw_records',],
        nargs="*",
        help='Target final data type to produce')
    parser.add_argument(
        '--output_folder',
        default='./strax_data',
        help='Output folder for context')
    parser.add_argument(
        '--detector',
        default='xams',
        help="xamsl or xams")
    parser.add_argument(
        '--from_scratch',
        action='store_true',
        help='Start processing at raw_records, regardless of what data is available. '
             'Saving will ONLY occur to ./strax_data! If you already have the target'
             'data in ./strax_data, you need to delete it there first.')
    parser.add_argument(
        '--config_kwargs',
        type=json.loads,
        help='Use a json-dict to set the context to. For example:'
             '--config_kwargs '
             '\'{'
             '"allow_multiprocess": true, '
             '"max_messages":4, '
             '"allow_shm": true, '
             '"allow_lazy": true}\''
    )
    parser.add_argument(
        '--testing_rundoc',
        type=json.loads,
        help='This is only used for testing, do not use!'
    )
    parser.add_argument(
        '--context_kwargs',
        type=json.loads,
        help='Use a json-file to load the context with (see config_kwargs for JSON-example)')
    parser.add_argument(
        '--timeout',
        default=None, type=int,
        help="Strax' internal mailbox timeout in seconds")
    parser.add_argument(
        '--workers',
        default=1, type=int,
        help=("Number of worker threads/processes. "
              "Strax will multithread (1/plugin) even if you set this to 1."))
    parser.add_argument(
        '--debug',
        action='store_true',
        help="Enable debug logging to stdout")
    parser.add_argument(
        '--build_lowlevel',
        action='store_true',
        help='Build low-level data even if the context forbids it.')
    return parser.parse_args()



def main(args):

    run_id = args.run_id
    client = amstrax.get_mongo_client()
    run_col = amstrax.get_mongo_collection('xams')
    run_doc = run_col.find_one({'number': int(run_id)})
    live_data = run_doc['daq_config']['strax_output_path']
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
