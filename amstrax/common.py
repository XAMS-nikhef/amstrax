import socket
import sys
import os.path as osp
import os
import inspect
import urllib.request
import tarfile
import io
import numpy as np
import straxen
import strax

export, __all__ = strax.exporter()
__all__ += ['amstrax_dir', 'to_pe']

amstrax_dir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))

# Current values 
n_tpc_pmts = 8
to_pe = 1


def open_test_data(file_name
                   ):
    """Downloads amstrax test data to strax_test_data in the current directory"""
    with open(file_name, mode='rb') as f:
        result = f.read()
    f = io.BytesIO(result)
    tf = tarfile.open(fileobj=f)
    tf.extractall()


first_sr1_run = '1'

cache_dict = dict()


# Placeholder for resource management system in the future?
@export
def get_resource(x, fmt='text'):
    return straxen.get_resource(x, fmt=fmt)


@export
def get_elife(run_id):
    """Return electron lifetime for run_id in ns"""
    return 642e3


@export
def select_channels(arr, channel_list):
    """Select only the values in arr that have arr['channel'] in channel_list
    """
    sel = np.sum([arr['channel'] == channel for channel in channel_list], axis=0) > 0
    return arr[sel]
