import os
import time
import amstrax.amstrax

version = '1.0.0'
print('Starting autoprocess version %s...' % version)

# settings
nap_time = 60  # Seconds

script_template = """#!/bin/bash
export PATH=/data/xenon/joranang/anaconda/bin:$PATH
source activate amstrax
python /data/xenon/xams/amstrax/autoprocessing/process_run.py {run_name}
echo "Script complete, bye!"
"""
runs_col = amstrax.amstrax.get_mongo_collection()
runs = runs_col['runs']
print('Runs db connected.')

while 1:
    # Update task list
    run_docs_to_do = list(runs.find({'processing_status': 'pending'}))
    if len(run_docs_to_do) > 0:
        print('I found %d runs to process, time to get to work!' % len(run_docs_to_do))
        print('These runs I will do:')
        for run_doc in run_docs_to_do:
            print(run_doc['name'])

    for run_doc in run_docs_to_do:
        run_name = run_doc['name']

        # Build a script to submit to stoomboot cluster
        script_name = (f'p_{run_name}.sh')
        script_file = open(script_name, 'w')
        script_file_content = script_template.format(run_name=run_name)
        script_file.write(script_file_content)
        script_file.close()

        # Submit the job
        os.system('qsub %s' % script_name)
        print('Submitted job for run %s...' % run_name)
        runs.find_one_and_update({'name': run_name},
                                 {'$set': {'processing_status': 'submitted_job'
                                           }})
        time.sleep(2)

    print("Waiting %d seconds before rechecking, press Ctrl+C to quit..." % nap_time)
    time.sleep(nap_time)
