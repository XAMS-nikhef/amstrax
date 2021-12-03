import amstrax
from . import amstraxer

if __name__ == '__main__':
    args = amstraxer.parse_args()

    run_collection = amstrax.get_mongo_collection()
    run_name = args.run_id
    target = args.target
    print(f'Start processing {run_name}: {target}')

    # Now read configuration
    print(f'Update database')
    run_doc = run_collection.find_one({"name": run_name})
    run_collection.find_one_and_update({"name": run_name},
                                       {"$set": {"processing_status": f"building_{target}"}})
    print(f'Start amstraxer')
    try:
        amstraxer.main(args)
    except Exception as e:
        run_collection.find_one_and_update({"name": run_name},
                                           {"$set": {
                                               "processing_status": f"failed due to {str(e)}"}})
        raise e

    run_collection.find_one_and_update({"name": run_name},
                                       {"$set": {"processing_status": "done"}})
