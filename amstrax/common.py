import socket
import sys
import os.path as osp
import os
import inspect
import urllib.request
import tarfile
import io
import numpy as np

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


# Previous values
# to_pe = np.array([4.252e1,4.252e1,1.3e-4,4.252e1,4.252e1,4.252e1,4.252e1,4.252e1,4,4])
# to_pe = np.array([1.3e-4,1.3e-4])
# n_tpc_pmts = 16

# gain = np.array([0.324e6, 0.323e6, 1, 0.309e6, 0.312e6, 0.306e6, 0.319e6, 0.326e6])
# sample_duration*digitizer_voltage_range/(2**digitizer_bits*pmt_circuit_load_resistor*total_amplification*e)
# total_amplification = gain * factor
# to_pe = 2e-9 * 2 / (2**13 * 50 * gain * 10 * 1.602e-19)


first_sr1_run = '1'


@export
def pax_file(x):
    """Return URL to file hosted in the pax repository master branch"""
    return 'https://raw.githubusercontent.com/XENON1T/pax/master/pax/data/' + x


cache_dict = dict()


# Placeholder for resource management system in the future?
@export
def get_resource(x, fmt='text'):
    """Return contents of file or URL x
    :param binary: Resource is binary. Return bytes instead of a string.
    """
    is_binary = fmt != 'text'

    # Try to retrieve from in-memory cache
    if x in cache_dict:
        return cache_dict[x]

    if '://' in x:
        # Web resource; look first in on-disk cache
        # to prevent repeated downloads.
        cache_fn = strax.utils.deterministic_hash(x)
        cache_folders = ['./resource_cache',
                         '/tmp/amstrax_resource_cache',
                         ]
        for cache_folder in cache_folders:
            try:
                os.makedirs(cache_folder, exist_ok=True)
            except (PermissionError, OSError):
                continue
            cf = osp.join(cache_folder, cache_fn)
            if osp.exists(cf):
                return get_resource(cf, fmt=fmt)

        # Not found in any cache; download
        result = urllib.request.urlopen(x).read()
        if not is_binary:
            result = result.decode()

        # Store in as many caches as possible
        m = 'wb' if is_binary else 'w'
        available_cf = None
        for cache_folder in cache_folders:
            if not osp.exists(cache_folder):
                continue
            cf = osp.join(cache_folder, cache_fn)
            try:
                with open(cf, mode=m) as f:
                    f.write(result)
            except Exception:
                pass
            else:
                available_cf = cf
        if available_cf is None:
            raise RuntimeError(
                f"Could not load {x},"
                "none of the on-disk caches are writeable??")

        # Retrieve result from file-cache
        # (so we only need one format-parsing logic)
        return get_resource(available_cf, fmt=fmt)

    # File resource
    if fmt == 'npy':
        result = np.load(x)
    elif fmt == 'binary':
        with open(x, mode='rb') as f:
            result = f.read()
    elif fmt == 'text':
        with open(x, mode='r') as f:
            result = f.read()

    # Store in in-memory cache
    cache_dict[x] = result

    return result


@export
def get_elife(run_id):
    """Return electron lifetime for run_id in ns"""
    # TODO: Get/cache snapshot of needed values from run db valid for 1 hour
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
