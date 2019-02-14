version = '0.0.0' 
print(f'This is process_run version {version} initializing...')

import sys
import json
import strax
import amstrax
# import shutil
import os
from sshtunnel import SSHTunnelForwarder
import pymongo
import os

__version__ = '0.0.0'

print('Starting processing script version %s' % __version__)
# The number of arguments for this script
nargs = 1

# Parse arguments
if len(sys.argv) != 1 + nargs:
    print('ERROR: script %s expected to be called with %d arguments, but received %d' % (sys.argv[0], nargs,
                                                                                         len(sys.argv) - 1))
    sys.exit(1)
run_name = sys.argv[1]
print(f'I will start processing run {run_name}!')


# Initialize connection to Mongo database
MONGO_HOST = "145.102.133.174"
MONGO_USER = "xams"
if "MONGO_PASS" not in dict(os.environ).keys():
    raise RuntimeError("DAQ password not set. Please define in .bashrc file. (i.e. 'export MONGO_PASS = <secret password>')")
MONGO_PASS = os.environ['MONGO_PASS']

print('Initializing server...')
server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username=MONGO_USER,
    ssh_password=MONGO_PASS,
    remote_bind_address=('127.0.0.1', 27017)
)
server.start()
print('Server started.')

# We need to update runs db tags and read configuration, so connect to database
# TODO maybe read from a configuration file as fallback option?
print('Initializing runs db connection...')
client = pymongo.MongoClient()
runs_db = client['run']
runs = runs_db['runs']
print('Runs db connected.')

# Now read configuration
run_doc = runs.find_one({"name" : run_name})
runs.find_one_and_update({"name" : run_name}, {"$set": {"processing_status" : "building_records"}})

print(f"Writing to folder location {run_doc['ini']['data_folder']}")
st = strax.Context(storage=strax.DataDirectory(run_doc['ini']['data_folder']),
                   register_all=amstrax,
                   config=run_doc['ini']['strax_config'])
print('Initialized strax.')

print('Building records...')
st.make(run_name, 'records')
print('Record building done.')

if run_doc['ini']['delete_raw_records']:
    print('Deleting raw records...')
    print('WARNING not implemented yet...')
    # shutil.rmtree(os.path.join(data_folder, )
    # os.system("rm -rf {data_folder}/{something}")
      
if run_doc['delete_mongo_data']:
    print('Deleting mongo data...')
    print('WARNING not implemented yet...')
    # Probably better to do at the stage of reading from mongo
      
runs.find_one_and_update({"name" : run_name}, {"$set": {"processing_status" : "building_peaks"}})
print('Building peaks...')
st.make(run_name, 'peaks')
print('Building peaks done.')

runs.find_one_and_update({"name" : run_name}, {"$set": {"processing_status" : "done"}})




