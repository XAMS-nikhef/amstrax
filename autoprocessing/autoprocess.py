import os
import time
import pymongo
import json

version = '0.1.0' # STRAX version, Feb. 2019
print('Starting autoprocess version %s...' % version)

# settings
nap_time = 10 # Seconds
# cpus = 4

script_template = """#!/bin/bash
export PATH=/data/xenon/xams/anaconda3/bin:$PATH
source activate strax
python /data/xenon/xams/amstrax/autoprocessing/process_run.py {run_name}
touch /data/xenon/xams/processing_folder/done/{run_name}
"""
    

# connect to database
# client = pymongo.MongoClient('145.102.135.93')
# Newer address: 145.102.132.91 (Update Oct 2017)
# Use this one (localhost) if you have an ssh port forwarded
print('Trying to connect to Mongo client...')
client = pymongo.MongoClient()
#client = pymongo.MongoClient('145.102.132.91')
print('Connection started.')
runs_db = client['run']
runs = runs_db['runs']
print('Runs db connected.')

while 1:
    # Update task list
    run_docs_to_do = list(runs.find({'event_building_complete' : True, 'processing_status' : 'pending'}))
    # run_docs_to_do = list(runs.find({'name' : '170202_124836'}))
    print('This is the task list: ', run_docs_to_do)
    if len(run_docs_to_do) > 0:
        print('I found %d runs to process, time to get to work!' % len(run_docs_to_do))
        print('These runs I will do:')
        for run_doc in run_docs_to_do:
            print(run_doc['name'])
    
    for run_doc in run_docs_to_do:
        run_name = run_doc['name']
        
        with open(f'/data/xenon/xams/processing_folder/config/{run_name}.json', 'w') as outfile:
            json.dump(run_doc, outfile)
        
        script_name = (f'p_{run_name}.sh')
        script_file = open(script_name, 'w')
        script_file_content = script_template.format(run_name = run_name)

        script_file.write(script_file_content)
        script_file.close()
        
        runs.find_one_and_update({'name': run_doc['name']},
                                                {'$set': {
                    'processing_status': 'processing'
                                                         }})
        # Are you sure...?
        os.system('qsub %s' % script_name)
        print('Submitted job for run %s...' % run_doc['name'])
        time.sleep(2)




    print("Waiting %d seconds before rechecking for unprocessed runs..."% nap_time)
    time.sleep(nap_time)

