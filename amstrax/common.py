from collections import defaultdict
from platform import python_version
from importlib import import_module
import socket

from collections import OrderedDict, deque
from importlib import import_module
from git import Repo, InvalidGitRepositoryError
from configparser import NoSectionError
import pandas as pd
import inspect
import io
import os
import tarfile
import sys
import numpy as np
import strax

export, __all__ = strax.exporter()
__all__ += ['amstrax_dir', 'to_pe']

# Current values 
n_tpc_pmts = 8
n_xamsl_channel = 4
to_pe = 1

_is_jupyter = any('jupyter' in arg for arg in sys.argv)

amstrax_dir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))



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
# @export
# def get_resource(x, fmt='text'):
#     return straxen.get_resource(x, fmt=fmt)

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

@export
def print_versions(
        modules=('strax', 'amstrax'),
        print_output=not _is_jupyter,
        include_python=True,
        return_string=False,
        include_git=True,
):
    """
    Print versions of modules installed.
    :param modules: Modules to print, should be str, tuple or list. E.g.
        print_versions(modules=('numpy', 'dddm',))
    :param return_string: optional. Instead of printing the message,
        return a string
    :param include_git: Include the current branch and latest
        commit hash
    :return: optional, the message that would have been printed
    """
    versions = defaultdict(list)
    if include_python:
        versions['module'] = ['python']
        versions['version'] = [python_version()]
        versions['path'] = [sys.executable]
        versions['git'] = [None]
    for m in strax.to_str_tuple(modules):
        result = _version_info_for_module(m, include_git=include_git)
        if result is None:
            continue
        version, path, git_info = result
        versions['module'].append(m)
        versions['version'].append(version)
        versions['path'].append(path)
        versions['git'].append(git_info)
    df = pd.DataFrame(versions)
    info = f'Host {socket.getfqdn()}\n{df.to_string(index=False,)}'
    if print_output:
        print(info)
    if return_string:
        return info
    return df


def _version_info_for_module(module_name, include_git):
    try:
        mod = import_module(module_name)
    except (ModuleNotFoundError, ImportError):
        print(f'{module_name} is not installed')
        return
    git = None
    version = mod.__dict__.get('__version__', None)
    module_path = mod.__dict__.get('__path__', [None])[0]
    if include_git:
        try:
            repo = Repo(module_path, search_parent_directories=True)
        except InvalidGitRepositoryError:
            # not a git repo
            pass
        else:
            try:
                branch = repo.active_branch
            except TypeError:
                branch = 'unknown'
            try:
                commit_hash = repo.head.object.hexsha
            except TypeError:
                commit_hash = 'unknown'
            git = f'branch:{branch} | {commit_hash[:7]}'
    return version, module_path, git
