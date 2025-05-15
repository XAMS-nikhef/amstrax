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
        '--target',
        default=['raw_records',],
        nargs="*",
        help='Target data type(s) to build')
    parser.add_argument(
        '--output_folder',
        default='./strax_data',
        help='Output folder for context')
    parser.add_argument(
        '--config_kwargs',
        type=json.loads,
        help='Use a json-dict to set the context to. For example:'
             '--config_kwargs '
             '\'{'
             '"elfie": 2000, '
             '\'}\''
    )
    parser.add_argument(
        '--testing_rundoc',
        type=json.loads,
        help='This is only used for testing, do not use!'
    )
    parser.add_argument(
        '--context_kwargs',
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
        '--raw_records_folder',
        default='/data/xenon/xams_v2/xams_raw_records',
        help='Folder with raw records')
    parser.add_argument( 
        '--corrections_version',
        default='ONLINE',
        help='Corrections version to use, can be ONLINE or v0 v1 etc')


    return parser.parse_args()



def main(args):

    run_id = args.run_id
    client = amstrax.get_mongo_client()
    run_col = amstrax.get_mongo_collection('xams')
    run_doc = run_col.find_one({'number': int(run_id)})
    raw_records_folder = args.raw_records_folder
    output_folder = args.output_folder
    
    st = amstrax.contexts.xams(
        output_folder=output_folder,
        corrections_version=args.corrections_version,
    )

    st.set_config(args.config_kwargs)
    st.set_context_config(args.context_kwargs)

    st.storage += [strax.DataDirectory(raw_records_folder, readonly=True)]

    for t in args.target:
        print(f'Processing {t} in amstraxer_easy')
        daqst.make(run_id, t, progress_bar=True)


if __name__ == '__main__':
    args = parse_args()
    sys.exit(main(args))
