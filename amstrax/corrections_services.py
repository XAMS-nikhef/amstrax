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
    # Try to get the file from GitHub
    return _fetch_from_github(file_name, branch)


def _fetch_from_github(file_name, branch="master"):
    """Fetch correction file from GitHub raw URL"""
    url = GITHUB_RAW_URL.format(branch=branch) + file_name
    
    response = requests.get(url)

    if response.status_code == 200:
        # Successfully fetched the file, return its contents
        print(f"Fetched {file_name} from GitHub")
        # return it as a json file (as a dictionary)
        return response.json()
    else:
        raise FileNotFoundError(f"File {file_name} not found in GitHub repository")
