import socket
import sys
import os.path as osp
import os
import inspect
import urllib.request

import numpy as np

import strax
export, __all__ = strax.exporter()
__all__ += ['straxen_dir', 'to_pe']

straxen_dir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))

# Current 
to_pe = np.ones(16, dtype=float)

@export
def pax_file(x):
    """Return URL to file hosted in the pax repository master branch"""
    return 'https://raw.githubusercontent.com/XENON1T/pax/master/pax/data/' + x


cache_folder = './resource_cache'


# Placeholder for resource management system in the future?
@export
def get_resource(x, binary=False):
    """Return contents of file or URL x
    :param binary: Resource is binary. Return bytes instead of a string.
    """
    if '://' in x:
        # Web resource
        cache_f = os.path.join(cache_folder,
                               strax.utils.deterministic_hash(x))
        if not os.path.exists(cache_folder):
            os.makedirs(cache_folder)
        if not os.path.exists(cache_f):
            y = urllib.request.urlopen(x).read()
            with open(cache_f, mode='wb' if binary else 'w') as f:
                if not binary:
                    y = y.decode()
                f.write(y)
        return get_resource(cache_f, binary=binary)
    else:
        # File resource
        with open(x, mode='rb' if binary else 'r') as f:
            return f.read()


@export
def get_elife(run_id):
    """Return electron lifetime for run_id in ns"""
    # TODO: Get/cache snapshot of needed values from run db valid for 1 hour
    return 642e3


@export
def get_secret(x):
    """Return secret key x. In order of priority, we search:

      * Environment variable: uppercase version of x
      * xenon_secrets.py (if included with your straxen installation)
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
                    "included with your straxen installation, ")

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
