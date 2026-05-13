# processing.py
import os
import sys
import logging
import argparse
import json
import strax
import socket
import getpass
import time, datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class RunProcessor:
    def __init__(self, args):
        self.run_id = f"{int(args.run_id):06d}"
        self.targets = args.targets
        self.output_folder = args.output_folder
        self.allow_raw_records = args.allow_raw_records
        self.corrections_version = args.corrections_version
        self.production = args.production
        self.amstrax_path = args.amstrax_path
        self.is_online = args.is_online
        self.fix_targets = args.fix_targets
        self.set_config_kwargs = args.set_config_kwargs or {}
        self.set_context_kwargs = args.set_context_kwargs or {}
        self.host = socket.gethostname().split("-")[0]
        
        if self.amstrax_path:
            self.amstrax_path = self.amstrax_path.rstrip("/")

        log.info(f"Processing run {self.run_id} with the following parameters:")
        log.info(f" --Targets: {self.targets}")
        log.info(f" --Output folder: {self.output_folder}")
        log.info(f" --Allow raw_records: {self.allow_raw_records}")
        log.info(f" --Corrections version: {self.corrections_version}")
        log.info(f" --Production: {self.production}")
        log.info(f" --Amstrax path: {self.amstrax_path}")
        log.info(f" --set_config_kwargs: {self.set_config_kwargs}")
        log.info(f" --set_context_kwargs: {self.set_context_kwargs}")
        log.info(f" --This file: {__file__}")
        log.info(f" --Is online: {self.is_online}")

        self.setup_amstrax()
        self.setup_production()
        self.run_doc = self.db_utils.get_run_doc(self.run_id)
        self.get_run_doc_info()
        self.db_utils.append_processing_history(
            self.run_id,
            action="process_run_start",
            status="running",
            production=self.production,
            host=self.host,
            is_online=self.is_online,
            targets=list(self.targets),
            corrections_version=self.corrections_version,
            amstrax_path=self.amstrax_path,
            amstrax_version=self.amstrax.__version__,
        )

    def setup_amstrax(self):

        if self.amstrax_path:
            if not os.path.exists(self.amstrax_path):
                raise FileNotFoundError(f"amstrax path {self.amstrax_path} does not exist.")
            log.info(f"Adding {self.amstrax_path} to sys.path.")
            sys.path.insert(0, self.amstrax_path)
        import amstrax
        self.amstrax = amstrax
        log.info(f"Using amstrax version: {amstrax.__version__} at {amstrax.__file__}")

        self.db_utils = self.amstrax.db_utils

    def get_run_doc_info(self):
        """ This one is just to extract the run document details and log them. """

        # Extract the run document details
        start_date = self.run_doc.get('start')
        end_date = self.run_doc.get('end')

        # Check if start and end dates are in the expected format
        if start_date and isinstance(start_date, dict) and '$date' in start_date:
            start_timestamp = start_date['$date'] / 1e3  # Convert from milliseconds to seconds
            start_datetime = datetime.datetime.fromtimestamp(start_timestamp)
        else:
            start_datetime = None

        if end_date and isinstance(end_date, dict) and '$date' in end_date:
            end_timestamp = end_date['$date'] / 1e3  # Convert from milliseconds to seconds
            end_datetime = datetime.datetime.fromtimestamp(end_timestamp)
        else:
            end_datetime = None

        # Calculate duration if both start and end times are available
        if start_datetime and end_datetime:
            duration = (end_timestamp - start_timestamp)
        else:
            duration = None

        # Log the run document details
        log.info(f"Run document for run {self.run_id}:")
        log.info(f" *** Mode: {self.run_doc.get('mode')}")
        if start_datetime:
            log.info(f" *** Start: {start_datetime}")
        if end_datetime:
            log.info(f" *** End: {end_datetime}")
        if duration is not None:
            log.info(f" *** Duration: {duration:.2f} seconds")
        log.info(f" *** User: {self.run_doc.get('user')}")
        log.info(f" *** Comments: {self.run_doc.get('comments')}")
        log.info(f" *** Tags: {self.run_doc.get('tags')}")


    def infer_special_modes(self, st):

        # Check if there is led in the run_doc
        if "ledcalibration" in self.run_doc.get('mode'):
            # Add the LEDCalibration plugin to the context
            log.info("Detected LED calibration run.")
            log.info("Adding LEDCalibration plugin to the context.")
            ax = self.amstrax
            st.register([
                ax.DAQReader,
                ax.RecordsLED,
                ax.LEDCalibration
            ])
        
            # override the targets to process only the LEDCalibration
            self.targets = ["raw_records", "records_led", "led_calibration"]
            log.info(f"Overriding targets to {self.targets}")
        

        if "_nai" in self.run_doc.get('mode'):
            # Add the NAI plugin to the context
            # All the _ext plugins should be already in the context
            log.info("Detected NaI run.")
            log.info("Adding peak_basics_ext to list of targets to process.")
            self.targets.append("peak_basics_ext")          

        return st

    def setup_production(self):

        if self.production:
            log.info("Setting up production configurations.")

            # Set the output folder to the production folder
            # if self.output_folder:
            #     raise ValueError("Output folder should not be set when processing production data.")

            self.output_folder = self.amstrax.get_xams_config("xams_processed_folder")
            log.info(f"Output folder set to {self.output_folder}")

            # Make sure we specified corrections version
            # if not self.corrections_version:
            #     raise ValueError("Corrections version should be specified for production processing.")

            # make sure that amstrax_path contains amstrax_versioned
            # if "amstrax_versioned" not in self.amstrax_path:
            #     raise ValueError("amstrax_path should be from amstrax_versioned for production processing.")

            # only xamsdata user can process production data
            if getpass.getuser() != "xamsdata":
                raise PermissionError("Only xamsdata user can process production data.")


    def add_data_entry(self, data_type, location, **info):
        """
        Add the data entry to rundb.
        """
        self.db_utils.add_data_entry(
            run_id=self.run_id,
            production=self.production,
            host=socket.gethostname().split("-")[0],
            location=location,
            user=getpass.getuser(),
            corrections_version=self.corrections_version,
            amstrax_path=self.amstrax_path,
            amstrax_version=self.amstrax.__version__,
            updated_at=datetime.datetime.now(),
            is_online=self.is_online,
            data_type=data_type,
            **info
        )


    def process(self):
        # Split targets into raw_records and others
        raw_ok = True
        if "raw_records" in self.targets:
            if not self.allow_raw_records:
                log.error("Raw records processing is not allowed. Use the --allow_raw_records flag.")
                self.db_utils.update_processing_status(
                    self.run_id,
                    "failed",
                    reason="raw_records requested without --allow_raw_records",
                    production=self.production,
                    is_online=self.is_online,
                )
                self.db_utils.append_processing_history(
                    self.run_id,
                    action="target_raw_records",
                    status="failed",
                    production=self.production,
                    host=self.host,
                    is_online=self.is_online,
                    reason="raw_records requested without --allow_raw_records",
                )
                return
            self.targets.remove("raw_records")
            raw_ok = self.process_raw_records()

        if self.targets and raw_ok:
            self.process_remaining_targets()
        elif self.targets and not raw_ok:
            reason = "raw_records stage failed; derived targets skipped"
            for target in self.targets:
                self.db_utils.update_target_processing_status(
                    self.run_id,
                    target,
                    "skipped",
                    reason=reason,
                    production=self.production,
                    is_online=self.is_online,
                )
            self.db_utils.append_processing_history(
                self.run_id,
                action="derived_targets_skipped",
                status="skipped",
                production=self.production,
                host=self.host,
                is_online=self.is_online,
                reason=reason,
                targets=list(self.targets),
            )
        elif raw_ok and not self.targets:
            self.db_utils.update_processing_status(
                self.run_id,
                "done",
                production=self.production,
                is_online=self.is_online,
            )
            self.db_utils.append_processing_history(
                self.run_id,
                action="process_run_end",
                status="done",
                production=self.production,
                host=self.host,
                is_online=self.is_online,
                targets=[],
            )

    def get_info_from_processed_data(self, folder, target, st):
        # Logic for getting info from processed data (similar to the existing one)
        # When the processing succeeds, the data is stored in the output folder
        # and the data entry is added to the database.
        # We want to know how many files are in the created folder
        # what is the lineage_hash 
        # and the total size of the data in MB

        key_for = str(st.key_for(self.run_id, target))
        log.info(f"Getting info from processed data in {folder} for {key_for}")
        data_folder = os.path.join(folder, key_for)
        lineage_hash = key_for.split("-")[-1]

        size_mb = 0
        n_files = 0

        for root, dirs, files in os.walk(data_folder):
            for file in files:
                size_mb += os.path.getsize(os.path.join(root, file)) / 1e6
                n_files += 1

        log.info(f"Processed data in {data_folder} contains {n_files} files with a total size of {size_mb:.2f} MB.")

        res = {
            "n_chunks": n_files,
            "lineage_hash": lineage_hash,
            "size_mb": size_mb
        }

        return res
        
    def process_raw_records(self):
        raw_records_folder = self.amstrax.get_xams_config("raw_records_folder")
        live_folder = self.amstrax.get_xams_config("live_folder")

        rundb = self.amstrax.get_mongo_collection()
        run_doc = rundb.find_one({"number": int(self.run_id)})
        if not run_doc:
            log.error(f"Run document for {self.run_id} not found in rundb.")
            return

        log.info("Processing raw_records separately...")
        log.info(f"Live data directory for run {self.run_id}: {live_folder}")

        raw_st = self.amstrax.contexts.xams(
            output_folder=raw_records_folder,
            init_rundb=False,
            corrections_version=self.corrections_version
        )
        if self.set_context_kwargs:
            raw_st.set_context_config(self.set_context_kwargs)
        if self.set_config_kwargs:
            raw_st.set_config(self.set_config_kwargs)

        self.raw_st = raw_st
        target = "raw_records"

        # Fast path: if raw_records already exists, skip before trying live-data setup.
        # This avoids failing reruns when live directory is gone or check_exists blocks.
        if self.raw_st.is_stored(self.run_id, target):
            log.info(f"Skipping {target} for run {self.run_id} as it is already processed.")
            self.db_utils.update_target_processing_status(
                self.run_id,
                target,
                "skipped",
                reason="already stored",
                production=self.production,
                is_online=self.is_online,
            )
            self.db_utils.append_processing_history(
                self.run_id,
                action="target_raw_records",
                status="skipped",
                production=self.production,
                host=self.host,
                is_online=self.is_online,
                reason="already stored",
            )
            return True

        raw_st.set_config({"live_data_dir": live_folder})
        raw_st = self.amstrax.contexts.context_for_daq_reader(
            raw_st, run_id=self.run_id, check_exists=False
        )
        raw_st.storage += [strax.DataDirectory(live_folder, readonly=True)]
        self.raw_st = raw_st

        try:
            self.db_utils.update_target_processing_status(
                self.run_id,
                target,
                "running",
                production=self.production,
                is_online=self.is_online,
            )
            log.info(f"Processing raw_records for run {self.run_id}")
            self.raw_st.make(self.run_id, target, progress_bar=True)
            info = self.get_info_from_processed_data(raw_records_folder, target, self.raw_st)
            self.add_data_entry(data_type=target, location=raw_records_folder, **info)
            self.db_utils.update_target_processing_status(
                self.run_id,
                target,
                "done",
                production=self.production,
                is_online=self.is_online,
            )
            self.db_utils.append_processing_history(
                self.run_id,
                action="target_raw_records",
                status="done",
                production=self.production,
                host=self.host,
                is_online=self.is_online,
                n_chunks=info.get("n_chunks"),
                size_mb=info.get("size_mb"),
                lineage_hash=info.get("lineage_hash"),
            )
            return True

        except Exception as e:
            log.error(f"Failed to process raw_records for run {self.run_id}: {e}")
            self.db_utils.update_target_processing_status(
                self.run_id,
                target,
                "failed",
                reason=str(e),
                production=self.production,
                is_online=self.is_online,
            )
            self.db_utils.update_processing_status(self.run_id, "failed", reason=str(e), production=self.production, is_online=self.is_online)
            self.db_utils.append_processing_history(
                self.run_id,
                action="target_raw_records",
                status="failed",
                production=self.production,
                host=self.host,
                is_online=self.is_online,
                reason=str(e),
            )
            return False


    def process_remaining_targets(self):
        raw_records_folder = self.amstrax.get_xams_config("raw_records_folder")

        log.info(f"Processing remaining targets: {self.targets}")
        log.info(f"Output folder: {self.output_folder}")

        st = self.amstrax.contexts.xams(
            output_folder=self.output_folder,
            corrections_version=self.corrections_version
        )
        if self.set_context_kwargs:
            st.set_context_config(self.set_context_kwargs)
        if self.set_config_kwargs:
            st.set_config(self.set_config_kwargs)
        st.storage += [strax.DataDirectory(raw_records_folder, readonly=True)]


        if not self.fix_targets:
            st = self.infer_special_modes(st)

        self.st = st

        try:
            for target in self.targets:
                if self.st.is_stored(self.run_id, target):
                    log.info(f"Skipping {target} for run {self.run_id} as it is already processed.")
                    self.db_utils.update_target_processing_status(
                        self.run_id,
                        target,
                        "skipped",
                        reason="already stored",
                        production=self.production,
                        is_online=self.is_online,
                    )
                    self.db_utils.append_processing_history(
                        self.run_id,
                        action=f"target_{target}",
                        status="skipped",
                        production=self.production,
                        host=self.host,
                        is_online=self.is_online,
                        reason="already stored",
                    )
                    continue

                t = time.time()
                log.info(f"Processing {target} for run {self.run_id}")
                self.db_utils.update_target_processing_status(
                    self.run_id,
                    target,
                    "running",
                    production=self.production,
                    is_online=self.is_online,
                )
                self.st.make(self.run_id, target, progress_bar=True)
                log.info(f"Processing of {target} completed successfully ({time.time() - t:.2f}s).")
                info = self.get_info_from_processed_data(self.output_folder, target, self.st)
                self.add_data_entry(data_type=target, location=self.output_folder, **info)
                self.db_utils.update_target_processing_status(
                    self.run_id,
                    target,
                    "done",
                    production=self.production,
                    is_online=self.is_online,
                )
                self.db_utils.append_processing_history(
                    self.run_id,
                    action=f"target_{target}",
                    status="done",
                    production=self.production,
                    host=self.host,
                    is_online=self.is_online,
                    n_chunks=info.get("n_chunks"),
                    size_mb=info.get("size_mb"),
                    lineage_hash=info.get("lineage_hash"),
                )

            self.db_utils.update_processing_status(self.run_id, "done", production=self.production, is_online=self.is_online)
            log.info(f"Processing of run {self.run_id} completed successfully.")
            self.db_utils.append_processing_history(
                self.run_id,
                action="process_run_end",
                status="done",
                production=self.production,
                host=self.host,
                is_online=self.is_online,
                targets=list(self.targets),
            )

        except Exception as e:
            log.error(f"Processing of targets failed for run {self.run_id}: {e}")
            self.db_utils.update_target_processing_status(
                self.run_id,
                target,
                "failed",
                reason=str(e),
                production=self.production,
                is_online=self.is_online,
            )
            self.db_utils.update_processing_status(self.run_id, "failed", reason=str(e), production=self.production, is_online=self.is_online)
            self.db_utils.append_processing_history(
                self.run_id,
                action=f"target_{target}",
                status="failed",
                production=self.production,
                host=self.host,
                is_online=self.is_online,
                reason=str(e),
            )


def parse_args():
    parser = argparse.ArgumentParser(description="Process a single run using amstrax.")
    parser.add_argument("--run_id", type=str, help="Run ID to process.")
    parser.add_argument("--targets", nargs="+", help="List of data types to process (e.g., 'raw_records', 'peaks').")
    parser.add_argument("--output_folder", type=str, help="Path to save the processed data.", default=None)
    parser.add_argument("--allow_raw_records", action="store_true", help="Explicitly allow raw_records processing.")
    parser.add_argument("--corrections_version", type=str, default=None, help="Version of corrections to apply.")
    parser.add_argument("--amstrax_path", type=str, default=None, help="Version of amstrax to use.")
    parser.add_argument("--production", action="store_true", help="Update the production database.")
    parser.add_argument("--is_online", action="store_true", help="Process online data.")
    parser.add_argument("--fix_targets", action="store_true", help="Fix the targets to process, do not allow special modes.")
    parser.add_argument(
        "--set_config_kwargs",
        type=json.loads,
        default={},
        help="JSON dict of kwargs passed to st.set_config (legacy-compatible).",
    )
    parser.add_argument(
        "--set_context_kwargs",
        type=json.loads,
        default={},
        help="JSON dict of kwargs passed to st.set_context_config (legacy-compatible).",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    processor = RunProcessor(args)
    processor.process()


if __name__ == "__main__":
    main()
