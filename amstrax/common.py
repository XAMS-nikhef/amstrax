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
def get_secret(x):
    """Return secret key x. In order of priority, we search:

      * Environment variable: uppercase version of x
      * xenon_secrets.py (if included with your amstrax installation)
      * A standard xenon_secrets.py located on the midway analysis hub
        (if you are running on midway)
    """
    env_name = x.upper()
    if env_name in os.environ:
        return os.environ[env_name]

    message = (f"Secret {x} requested, but there is no environment "
               f"variable {env_name}, ")
    try:
        from . import xenon_secrets
    except ImportError:
        message += ("nor was there a valid xenon_secrets.py "
                    "included with your amstrax installation, ")

        # If on midway, try loading a standard secrets file instead
        if 'rcc' in socket.getfqdn():
            path_to_secrets = '/home/aalbers/xenon_secrets.py'
            if os.path.exists(path_to_secrets):
                sys.path.append(osp.dirname(path_to_secrets))
                import xenon_secrets
                sys.path.pop()
            else:
                raise ValueError(
                    message + ' nor could we load the secrets module from '
                              f'{path_to_secrets}, even though you seem '
                              'to be on the midway analysis hub.')

        else:
            raise ValueError(
                message + 'nor are you on the midway analysis hub.')

    if hasattr(xenon_secrets, x):
        return getattr(xenon_secrets, x)
    raise ValueError(message + " and the secret is not in xenon_secrets.py")


@export
def select_channels(arr, channel_list):
    """Select only the values in arr that have arr['channel'] in channel_list
    """
    sel = np.sum([arr['channel'] == channel for channel in channel_list], axis=0) > 0
    return arr[sel]
