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


nargs = 1

if len(sys.argv) != 1 + nargs:
    print('ERROR: script %s expected to be called with %d arguments, but received %d' % (sys.argv[0], nargs,
                                                                                         len(sys.argv) - 1))
    sys.exit(1)
run_name = sys.argv[1]

print(f'I will start processing run {run_name}!')

# read configuration from the json file
try:
    with open(f'/data/xenon/xams/processing_folder/config/{run_name}.json') as f:
        run_doc = json.load(f)
    print(f'Read configuration from /data/xenon/xams/processing_folder/config/{run_name}.json.')
except:
    print(f'ERROR: could not load configuration from data/xenon/xams/processing_folder/config/{run_name}.json.')
    sys.exit(2)

MONGO_HOST = "145.102.133.174"
MONGO_DB = "xamsdata0"
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
    
print(f'Writing to folder location {run_doc['data_folder']}')
st = strax.Context(storage=strax.DataDirectory(run_doc['data_folder']),
                   register_all=amstrax,
                   config=run_doc['strax_config'])
print('Initialized strax.')

print('Building records...')
st.make(run_name, 'records')
print('Record building done.')



if run_doc['delete_raw_records']:
    print('Deleting raw records...')
    print('WARNING not implemented yet...')
    # shutil.rmtree(os.path.join(data_folder, )
    # os.system("rm -rf {data_folder}/{something}")

if run_doc['delete_mongo_data']:
    print('Deleting mongo data...')
    print('WARNING not implemented yet...')
    # Probably better to do at the stage of reading from mongo
      
print('Building peaks...')
st.make(run_name, 'peaks')
print('Building peaks done.')

    


