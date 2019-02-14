import os
import time
import pymongo
import json

version = '0.1.0' # STRAX version, Feb. 2019
print('Starting autoprocess_local version %s...' % version)

# settings
nap_time = 10 # Seconds


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
        
        
        
        
        
        
        time.sleep(2)




    print("Waiting %d seconds before rechecking, press Ctrl+C to quit..."% nap_time)
    time.sleep(nap_time)

