import os
import requests
import strax

export, __all__ = strax.exporter()

GITHUB_RAW_URL = "https://raw.githubusercontent.com/XAMS-nikhef/amstrax_files/{branch}/corrections/"


@export
def get_correction(file_name, branch="master"):
    """
    Get correction file from GitHub or locally.
    It first tries to download from GitHub, and if that fails, it fetches from the local corrections directory.
    """

    # extract the part of the file that comes upto the last _ (e.g. 'gain_to_pe' from 'gain_to_pe_v1')
    dir_path = '_'.join(file_name.split('_')[:-1])
    # because we put correction files in the corrections directory ( gain_to_pe/gain_to_pe_v1.json )
    # we need to append the directory path to the file name
    file_name = os.path.join(dir_path, file_name)

    return _fetch_from_github(file_name, branch)


def _fetch_from_github(file_name, branch="master"):
    """Fetch correction file from GitHub raw URL"""
    url = GITHUB_RAW_URL.format(branch=branch) + file_name
    print(f"Fetching {file_name} from GitHub at {url}")
    response = requests.get(url)

    if response.status_code == 200:
        # Successfully fetched the file, return its contents
        print(f"Fetched {file_name} from GitHub")
        # return it as a json file (as a dictionary)
        return response.json()
    else:
        raise FileNotFoundError(f"File {file_name} not found in GitHub repository")
