#!/usr/bin/env python
"""
Process a single run with amstrax
"""
import argparse
import datetime
import json
import logging
import os
import os.path as osp
import platform
import sys
import time

import psutil


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
        default='xams_little',
        help="Name of context to use")
    parser.add_argument(
        '--target',
        default='raw_records_v1724',
        help='Target final data type to produce')
    parser.add_argument(
        '--detector',
        default='xamsl',
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
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')

    print(f"Starting processing of run {args.run_id} until {args.target} for {args.detector} in context {args.context}")
    print(f"\tpython {platform.python_version()} at {sys.executable}")

    # These imports take a bit longer, so it's nicer
    # to do them after argparsing (so --help is fast)
    import strax
    print(f"\tstrax {strax.__version__} at {osp.dirname(strax.__file__)}")
    import amstrax
    print(f"\tamstrax {amstrax.__version__} at {osp.dirname(amstrax.__file__)}")

    if args.context_kwargs:
        logging.info(f'set context kwargs {args.context_kwargs}')
        st = getattr(amstrax.contexts, args.context)(**args.context_kwargs)
    else:
        st = getattr(amstrax.contexts, args.context)()

    if args.config_kwargs:
        logging.info(f'set context options to {args.config_kwargs}')
        st.set_config(to_dict_tuple(args.config_kwargs))

    if args.timeout is not None:
        st.context_config['timeout'] = args.timeout

    if args.build_lowlevel:
        st.context_config['forbid_creation_of'] = tuple()

    if 'raw_records' in args.target:
        # Only for testing!
        testing_rd = args.testing_rundoc
        if testing_rd is not None:
            testing_rd['start'] = datetime.datetime.now()
        st = amstrax.contexts.context_for_daq_reader(st,
                                                     args.run_id,
                                                     run_doc=testing_rd)

    if args.from_scratch:
        for q in st.storage:
            q.take_only = ('raw_records',)
        st.storage.append(
            strax.DataDirectory('./strax_data',
                                overwrite='always',
                                provide_run_metadata=False))
    if st.is_stored(args.run_id, args.target):
        print("This data is already available.")
        return 1
    try:
        md = st.run_metadata(args.run_id)
    except strax.RunMetadataNotAvailable:
        logging.warning('Using dummy timestamps')
        md = {}
        md['end'] = datetime.datetime.now()
        md['start'] = md['end'] - datetime.timedelta(seconds=360)
    t_start = md['start'].replace(tzinfo=datetime.timezone.utc).timestamp()
    t_end = md['end'].replace(tzinfo=datetime.timezone.utc).timestamp()
    run_duration = t_end - t_start

    st.config['run_start_time'] = md['start'].timestamp()
    st.context_config['free_options'] = tuple(
        list(st.context_config['free_options']) + ['run_start_time'])

    process = psutil.Process(os.getpid())
    peak_ram = 0

    def get_results():
        kwargs = dict(
            run_id=args.run_id,
            targets=args.target,
            max_workers=int(args.workers))
        yield from st.get_iter(**kwargs)

    clock_start = 0
    for i, d in enumerate(get_results()):
        mem_mb = process.memory_info().rss / 1e6
        peak_ram = max(mem_mb, peak_ram)

        if len(d) == 0:
            print(f"Got chunk {i}, but it is empty! Using {mem_mb:.1f} MB RAM.")
            continue

        # Compute detector/data time left
        t = d.end / 1e9
        dt = t - t_start
        time_left = t_end - t

        msg = (f"Got {len(d)} items. "
               f"Now {dt:.1f} sec / {100 * dt / run_duration:.1f}% into the run. "
               f"Using {mem_mb:.1f} MB RAM. ")
        if clock_start is not None:
            # Compute processing job clock time left
            d_clock = time.time() - clock_start
            clock_time_left = time_left / (dt / d_clock)
            msg += f"ETA {clock_time_left:.2f} sec."
        else:
            clock_start = time.time()

        print(msg, flush=True)

    print(f"\nAmstraxer is done! "
          f"We took {time.time() - clock_start:.1f} seconds, "
          f"peak RAM usage was around {peak_ram:.1f} MB.")


def to_dict_tuple(res: dict):
    """Convert list configs to tuple configs"""
    res = res.copy()
    for k, v in res.copy().items():
        if type(v) == list:
            # Remove lists to tuples
            res[k] = tuple(_v if type(_v) != list else tuple(_v) for _v in v)
    return res


if __name__ == '__main__':
    args = parse_args()
    sys.exit(main(args))
