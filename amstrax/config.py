import os
import configparser
from typing import Dict, Any
import strax

export, __all__ = strax.exporter()

@export
def get_xams_config(key) -> Dict[str, Any]:
    """Get the configuration dictionary."""



    config_file_path = os.getenv('XAMS_CONFIG_FILE')

    if not config_file_path:
        # Raise error that env XAMS_CONFIG_FILE is not set
        raise FileNotFoundError('XAMS_CONFIG_FILE is not set, did you run setup.sh?')

    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f'Could not find xams config file {config_file_path}, did you run setup.sh?')

    
    # we will check in the config file
    config_file = configparser.ConfigParser()
    config_file.read(config_file_path)

    return config_file['default'][key]
