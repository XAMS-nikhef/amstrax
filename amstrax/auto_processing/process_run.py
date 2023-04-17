import sys
from amstrax.auto_processing import amstraxer
from amstrax.auto_processing import amstraxer_easy

if __name__ == '__main__':
    args = amstraxer.parse_args()

    # Do import later to get fast --help

    import sys
    import amstrax

    detector = args.detector
    run_collection = amstrax.get_mongo_collection(detector)
    run_name = args.run_id
    target = args.target
    print(f'Start processing {run_name}: {target}')

    # Now read configuration
    print(f'Update database')
    run_doc = run_collection.find_one({"number": int(run_name)})
    run_collection.find_one_and_update({"number": int(run_name)},
                                       {"$set": {"processing_status": f"building_{target}"}})
    print(f'Start amstraxer')
    try:
        amstraxer_easy.main(args)
    except Exception as e:
        run_collection.find_one_and_update({"number": int(run_name)},
                                           {"$set": {"processing_status": f"failed due to {str(e)}"},
                                           "$inc": { "processing_failed": 1}
                                           })
        raise e

    run_collection.find_one_and_update({"number": int(run_name)},
                                       {"$set": {"processing_status": "done"}})
