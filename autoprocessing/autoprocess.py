import os
import time
import pymongo
import json
from sshtunnel import SSHTunnelForwarder
from getkey import getkey

version = '0.1.0' # STRAX version, Feb. 2019
print('Starting autoprocess version %s...' % version)

# settings
nap_time = 10 # Seconds

script_template = """#!/bin/bash
export PATH=/data/xenon/xams/anaconda3/bin:$PATH
source activate strax
python /data/xenon/xams/amstrax/autoprocessing/process_run.py {run_name}
echo "Script complete, bye!"
"""
    
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

print('Initializing runs db connection...')
client = pymongo.MongoClient()
runs_db = client['run']
runs = runs_db['runs']
print('Runs db connected.')



while 1:
    # Update task list
    run_docs_to_do = list(runs.find({'processing_status' : 'pending'}))
    if len(run_docs_to_do) > 0:
        print('I found %d runs to process, time to get to work!' % len(run_docs_to_do))
        print('These runs I will do:')
        for run_doc in run_docs_to_do:
            print(run_doc['name'])
    
    for run_doc in run_docs_to_do:
        run_name = run_doc['name']
        
        # Dump configuration, may be needed for fallback in processing script
        # Nice to have a backup anyway, right? Doesn't hurt.
        #with open(f'/data/xenon/xams/processing_folder/config/{run_name}.json', 'w') as outfile:
        #    json.dump(run_doc, outfile)
        
        # Build a script to submit to stoomboot cluster
        script_name = (f'p_{run_name}.sh')
        script_file = open(script_name, 'w')
        script_file_content = script_template.format(run_name = run_name)
        script_file.write(script_file_content)
        script_file.close()
        
        

        # Submit the job
        os.system('qsub %s' % script_name)
        print('Submitted job for run %s...' % run_name)
        runs.find_one_and_update({'name': run_name},
                                                {'$set': {'processing_status': 'submitted_job'
                                                         }})
        time.sleep(2)




    print("Waiting %d seconds before rechecking, press Ctrl+C to quit..."% nap_time)
    time.sleep(nap_time)

