import os
import time
import amstrax.amstrax
import submit_to_stoomboot

version = '1.0.0'
print('Starting autoprocess version %s...' % version)

# settings
nap_time = 60  # Seconds


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
        submit_to_stoomboot.submit(run_name, target='raw_records')
        runs.find_one_and_update({'name': run_name},
                                 {'$set': {'processing_status': 'submitted_job'
                                           }})
        time.sleep(2)

    print("Waiting %d seconds before rechecking, press Ctrl+C to quit..." % nap_time)
    time.sleep(nap_time)
