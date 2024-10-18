# processing.py
import os
import strax
import amstrax
import logging
from db_interaction import update_processing_status, add_data_entry

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def process_run(
    run_id,
    targets,
    live_folder,
    raw_records_folder,
    output_folder,
    allow_raw_records=False,
    corrections_version="ONLINE",
    production=False,
):
    """
    Process a single run based on the provided parameters.

    :param run_id: ID of the run to process.
    :param targets: List of data types to process (e.g., ['raw_records', 'peaks']).
    :param config: Configuration for the context.
    :param live_folder: Path to the live data folder for raw records processing.
    :param raw_records_folder: Path to the folder containing raw records (output from raw records processing).
    :param output_folder: Path to save the processed data (for peaks, events, etc.).
    :param rundb: MongoDB collection for retrieving the run document.
    :param allow_raw_records: Boolean flag to explicitly allow raw_records processing.
    :param corrections_version: Version of corrections to apply ('ONLINE' or specific).
    :param production: If True, updates the production database.
    :return: None
    """
    log.info(f"Processing run {run_id} with targets: {targets}")

    # Retrieve the run document from rundb
    rundb = amstrax.get_mongo_collection()
    run_doc = rundb.find_one({"number": int(run_id)})
    if not run_doc:
        log.error(f"Run document for {run_id} not found in rundb.")
        return

    # Separate raw_records if it's in the target list
    if "raw_records" in targets:
        if not allow_raw_records:
            log.error("Raw records processing is not allowed. Use the --allow_raw_records flag.")
            return

        log.info("Processing raw_records separately...")
        # Pop raw_records from the targets list
        targets.remove("raw_records")

        # Process raw_records using the live data folder
        log.info(f"Live data directory for run {run_id}: {live_folder}")

        # Setup the context specifically for raw_records
        raw_st = amstrax.contexts.xams(output_folder=raw_records_folder, init_rundb=False)
        raw_st.storage += [strax.DataDirectory(live_folder, readonly=True)]
        raw_st.set_config({"live_data_dir": live_folder})

        # Process raw_records using the special context
        try:
            log.info(f"Processing raw_records for run {run_id}")
            raw_st.make(run_id, "raw_records", progress_bar=True)
            update_processing_status(run_id, "done", production=production)
            add_data_entry(run_id, "raw_records", raw_records_folder, production=production)
        except Exception as e:
            log.error(f"Failed to process raw_records for run {run_id}: {e}")
            update_processing_status(run_id, "failed", reason=str(e), production=production)
            return

    # Process the remaining targets with the standard context
    if targets:
        log.info(f"Processing remaining targets: {targets}")

        # Setup the normal context for peaks/events/etc.
        st = amstrax.contexts.xams(output_folder=output_folder, corrections_version=corrections_version)
        st.storage += [strax.DataDirectory(raw_records_folder, readonly=True)]
        # st.set_config(set_config_kwargs)
        # st.set_context_config(set_context_kwargs)

        # Process each remaining target
        try:
            for target in targets:
                log.info(f"Processing {target} for run {run_id}")
                st.make(run_id, target, progress_bar=True)

            # Update processing status to 'done'
            update_processing_status(run_id, "done", production=production)
            add_data_entry(run_id, data_type=targets[-1], location=output_folder, production=production)

            log.info(f"Processing of run {run_id} completed successfully.")

        except Exception as e:
            log.error(f"Processing of remaining targets failed for run {run_id}: {e}")
            update_processing_status(run_id, "failed", reason=str(e), production=production)


def main():

    # Define the command line arguments
    import argparse

    parser = argparse.ArgumentParser(description="Process a single run using amstrax.")
    parser.add_argument("--run_id", type=str, help="Run ID to process.")
    parser.add_argument("--targets", nargs="+", help="List of data types to process (e.g., 'raw_records', 'peaks').")
    parser.add_argument(
        "--live_folder",
        type=str,
        help="Path to the live data folder for raw records processing.",
        default=None,
    )
    parser.add_argument(
        "--raw_records_folder",
        type=str,
        help="Path to the folder containing raw records (output from raw records processing).",
    )
    parser.add_argument(
        "--output_folder",
        type=str,
        help="Path to save the processed data (for peaks, events, etc.).",
    )
    parser.add_argument(
        "--allow_raw_records",
        action="store_true",
        help="Explicitly allow raw_records processing.",
    )
    parser.add_argument(
        "--corrections_version",
        type=str,
        default=None,
        help="Version of corrections to apply ('ONLINE' or v0,v1 or None).",
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="Update the production database.",
    )

    args = parser.parse_args()

    log.info(f"Starting processing.py with arguments: {args}")

    # Process the run based on the provided arguments
    process_run(
        args.run_id,
        args.targets,
        args.live_folder,
        args.raw_records_folder,
        args.output_folder,
        args.allow_raw_records,
        args.corrections_version,
        args.production,
    )


if __name__ == "__main__":
    main()
