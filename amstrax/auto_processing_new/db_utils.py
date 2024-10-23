# db_interaction.py
from pymongo import MongoClient
import datetime
import logging
import amstrax

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Create a MongoDB client and connect to the rundb collection

def update_processing_status(run_id, status, reason=None, host='stbc', production=False, pull=dict(), is_online=False):
    """
    Update the processing status of a run in the MongoDB rundb.

    :param run_id: ID of the run to update.
    :param status: New status (e.g., 'running', 'done', 'failed').
    :param reason: Reason for failure, if applicable.
    :param host: Host where the processing is being done.
    :param production: If True, actually updates the database, else simulates the update.
    :return: None
    """
    
    runsdb = amstrax.get_mongo_collection()


    update = {
        'status': status,
        'time': datetime.datetime.now(),
        'host': host,
    }

    increase = {}

    if reason:
        update['reason'] = str(reason)
        increase['processing_failed'] = 1

        

    if production and is_online:
        runsdb.update_one(
            {'number': int(run_id)},
            {
                '$set': {'processing_status': update},
                '$inc': increase,
                '$pull': pull
            }
        )
        log.info(f"Run {run_id} updated to status {status} in production mode.")
    else:
        log.info(f"Would update run {run_id} to status {status} (dry run).")

def add_data_entry(
        run_id,
        data_type,
        location,
        host,
        n_chunks,
        size_mb,
        lineage_hash,
        user=None,
        amstrax_version=None,
        amstrax_path=None,
        corrections_version=None,
        production=False,
        **kwargs
    ):
    """
    Add a new data entry (e.g., raw records) for a run in the rundb.

    :param run_id: ID of the run to update.
    :param data_type: Type of data (e.g., 'raw_records').
    :param location: Where the data is stored.
    :param host: Host where the data was processed.
    :param by: Script or process responsible for creating the data.
    :param user: User who initiated the processing.
    :param production: If True, actually updates the database, else simulates the update.
    :return: None
    """

    runsdb = amstrax.get_mongo_collection()


    data_entry = {
        'time': datetime.datetime.now(),
        'type': data_type,
        'location': location,
        'host': host,
        'user': user,
        'amstrax_version': amstrax_version,
        'amstrax_path': amstrax_path,
        'corrections_version': corrections_version,
        'n_chunks': n_chunks,
        'size_mb': size_mb,
        'lineage_hash': lineage_hash,
    }

    data_entry.update(kwargs)

    # print them out nicely
    log.info(f"Data entry for run {run_id} with:")
    for key, value in data_entry.items():
        log.info(f"{key}: {value}")
        
    if production:
        run_doc = runsdb.find_one({'number': int(run_id)})
        for entry in run_doc.get('data', []):
            if entry.get('type', None) == data_type and \
                entry.get('location', None) == location and \
                    entry.get('lineage_hash', None) == lineage_hash:

                log.info(f"Entry for run {run_id} of type {data_type} and location {location} already exists.")
                return
        runsdb.update_one(
            {'number': int(run_id)},
            {'$push': {'data': data_entry}}
        )
        log.info(f"Data entry for run {run_id} added successfully in production mode.")
    else:
        log.info(f"Would add data entry to run {run_id} (dry run).")

def query_runs(query):
    """
    Query the rundb to retrieve runs based on a specific filter.
    
    :param query: MongoDB query object.
    :return: List of runs matching the query.
    """
    
    runsdb = amstrax.get_mongo_collection()

    results = list(runsdb.find(query))
    log.info(f"Found {len(results)} runs matching query.")
    return results
